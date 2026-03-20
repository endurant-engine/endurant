defmodule Endurant.Integration.HardeningTest do
  use Endurant.TestSupport.IntegrationCase

  defmodule Probe do
    use Agent
    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      Agent.start_link(fn -> %{} end, name: name)
    end

    @spec reset() :: :ok
    def reset do
      Agent.update(__MODULE__, fn _ -> %{} end)
    end

    @spec fail_once_then_ok(term()) :: map() | no_return()
    def fail_once_then_ok(key) do
      attempt =
        Agent.get_and_update(__MODULE__, fn state ->
          current = Map.get(state, key, 0) + 1
          {current, Map.put(state, key, current)}
        end)

      if attempt == 1 do
        raise "fail once"
      else
        %{ok: true, key: key}
      end
    end

    @spec fail_twice_then_ok(term()) :: map() | no_return()
    def fail_twice_then_ok(key) do
      attempt =
        Agent.get_and_update(__MODULE__, fn state ->
          current = Map.get(state, key, 0) + 1
          {current, Map.put(state, key, current)}
        end)

      if attempt <= 2 do
        raise "fail twice"
      else
        %{ok: true, key: key}
      end
    end

    @spec fail_always(term()) :: no_return()
    def fail_always(_key) do
      raise "always fail"
    end
  end

  setup do
    {:ok, _pid} = start_supervised({Probe, name: Probe})
    :ok = Probe.reset()
    :ok
  end

  test("cancel during retry sleep finishes cancelled and does not retry task", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.CancelDuringRetryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-cancel-retry:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "unstable",
              fn _ -> Endurant.Integration.HardeningTest.Probe.fail_once_then_ok(input["id"]) end,
              retry: [max_attempts: 2, backoff: :constant, base_ms: 500]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.CancelDuringRetryWorkflow,
               %{id: "cdr-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :execution_waiting =
             wait_for_event(execution.id, :execution_waiting, 5000, runtime_opts)

    assert :ok = Endurant.cancel(execution.id, instance: Keyword.fetch!(runtime_opts, :instance))
    assert :cancelled = wait_for_status(execution.id, :cancelled, 5000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_started)) == 1
  end

  test("cancel near task completion yields single cancelled terminal", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.CancelNearFinishWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("manual")
            unique_id(fn %{id: id} -> "hardening-cancel-near-finish:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long", fn _ ->
              Process.sleep(400)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.CancelNearFinishWorkflow,
               %{id: "cnf-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    [claimed] =
      Endurant.Executions.claim_pending(:manual, 1, "test-worker:cnf", 30000, runtime_opts)

    Task.start(fn ->
      Endurant.Executor.run(
        claimed,
        runtime_opts ++ [worker_id: "test-worker:cnf", queue_manager: self()]
      )
    end)

    assert :task_started = wait_for_event(execution.id, :task_started, 5000, runtime_opts)
    Process.sleep(250)
    assert :ok = Endurant.cancel(execution.id, instance: Keyword.fetch!(runtime_opts, :instance))
    assert :cancelled = wait_for_status(execution.id, :cancelled, 5000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    refute Enum.any?(events, &(&1.type == :execution_completed))
    assert Enum.any?(events, &(&1.type == :execution_cancelled))
  end

  test("lock expiry during retry wait recovers and completes", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.RetryRecoveryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-retry-recovery:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "unstable",
              fn _ ->
                Endurant.Integration.HardeningTest.Probe.fail_twice_then_ok(input["id"])
              end,
              retry: [max_attempts: 3, backoff: :constant, base_ms: 500]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.RetryRecoveryWorkflow,
               %{id: "rr-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :execution_waiting =
             wait_for_event(execution.id, :execution_waiting, 5000, runtime_opts)

    force_wait_ready_and_lock_expired!(execution.id, runtime_opts)

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_resumed))

    if Enum.any?(events, &(&1.type == :execution_abandoned)) do
      assert_abandoned_has_timestamp!(events)
    end
  end

  test("duplicate recovery ticks do not duplicate abandoned event", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.RecoveryIdempotencyWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-recovery-idempotent:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "slow", fn _ ->
              Process.sleep(1500)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.RecoveryIdempotencyWorkflow,
               %{id: "ri-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :running = wait_for_status(execution.id, :running, 2000, runtime_opts)
    force_lock_expired!(execution.id, runtime_opts)

    tasks =
      for _ <- 1..5 do
        Task.async(fn -> Endurant.Executions.recover_expired_locks(100, runtime_opts) end)
      end

    _ = Enum.map(tasks, &Task.await(&1, 2000))
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :execution_abandoned)) <= 1
    assert_abandoned_has_timestamp!(events)
  end

  test("waiting lock expiry emits abandoned event and keeps waiting status", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  }) do
    reset_queue_manager_executors!(engine_name, 5000)

    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.WaitingOrphanWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-waiting-orphan:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            wait_signal("never")
            :ok
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.WaitingOrphanWorkflow,
               %{id: "wo-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :execution_waiting =
             wait_for_event(execution.id, :execution_waiting, 5000, runtime_opts)

    assert :waiting = wait_for_status(execution.id, :waiting, 2000, runtime_opts)
    kill_cached_executor!(execution.id, engine_name)
    force_lock_expired!(execution.id, runtime_opts)
    _ = Endurant.Executions.recover_expired_locks(100, runtime_opts)

    assert :execution_abandoned =
             wait_for_event(execution.id, :execution_abandoned, 5000, runtime_opts)

    assert :waiting = wait_for_status(execution.id, :waiting, 2000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_abandoned))
    assert_abandoned_has_timestamp!(events)
  end

  test("event append race keeps contiguous sequences", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.SignalRaceWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-signal-race:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go")
            wait_signal("go2")
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.SignalRaceWorkflow,
               %{id: "sr-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    _ =
      for i <- 1..20 do
        Task.start(fn ->
          Endurant.signal(execution.id, "go", %{n: i},
            instance: Keyword.fetch!(runtime_opts, :instance)
          )
        end)
      end

    _ =
      for i <- 1..20 do
        Task.start(fn ->
          Endurant.signal(execution.id, "go2", %{n: i},
            instance: Keyword.fetch!(runtime_opts, :instance)
          )
        end)
      end

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    events = Endurant.events(execution.id, instance: Keyword.fetch!(runtime_opts, :instance))
    sequences = Enum.map(events, & &1.sequence)
    assert sequences == Enum.to_list(1..length(sequences))
  end

  test("late cancel after retry exhausted returns not_active and emits no cancel events", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.ExhaustedThenCancelWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "hardening-exhausted-cancel:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "always_fail",
              fn _ -> Endurant.Integration.HardeningTest.Probe.fail_always(input["id"]) end,
              retry: [max_attempts: 2, backoff: :constant, base_ms: 20]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.ExhaustedThenCancelWorkflow,
               %{id: "ec-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :failed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    assert {:error, :not_active} =
             Endurant.cancel(execution.id, instance: Keyword.fetch!(runtime_opts, :instance))

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    refute Enum.any?(events, &(&1.type == :cancel_requested))
    refute Enum.any?(events, &(&1.type == :execution_cancelled))
  end

  test("task event guard halt does not mark execution failed", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HardeningTest.GuardHaltWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("manual")
            unique_id(fn %{id: id} -> "hardening-guard-halt:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long", fn _ ->
              Process.sleep(500)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.HardeningTest.GuardHaltWorkflow,
               %{id: "gh-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    [claimed] =
      Endurant.Executions.claim_pending(:manual, 1, "test-worker:gh", 30000, runtime_opts)

    Task.start(fn ->
      Endurant.Executor.run(
        claimed,
        runtime_opts ++ [worker_id: "test-worker:gh", queue_manager: self()]
      )
    end)

    assert :task_started = wait_for_event(execution.id, :task_started, 5000, runtime_opts)
    force_cancelled_state!(execution.id, runtime_opts)
    assert :cancelled = wait_for_status(execution.id, :cancelled, 5000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    refute Enum.any?(events, &(&1.type == :execution_failed))
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(execution_id, instance: Keyword.fetch!(runtime_opts, :instance)) do
      %{status: ^expected_status} ->
        expected_status

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("execution #{execution_id} did not reach status #{inspect(expected_status)}")
        else
          Process.sleep(25)
          do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
        end
    end
  end

  @spec wait_for_event(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_event(execution_id, expected_type, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_event(execution_id, expected_type, deadline, runtime_opts)
  end

  @spec do_wait_for_event(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_event(execution_id, expected_type, deadline, runtime_opts) do
    events = Endurant.events(execution_id, instance: Keyword.fetch!(runtime_opts, :instance))

    if Enum.any?(events, &(&1.type == expected_type)) do
      expected_type
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("execution #{execution_id} did not emit event #{inspect(expected_type)}")
      else
        Process.sleep(25)
        do_wait_for_event(execution_id, expected_type, deadline, runtime_opts)
      end
    end
  end

  @spec assert_abandoned_has_timestamp!([map()]) :: :ok
  defp assert_abandoned_has_timestamp!(events) do
    Enum.each(events, fn event ->
      if event.type == :execution_abandoned do
        abandoned_at =
          (event.payload && event.payload["abandoned_at"]) ||
            (event.payload && event.payload[:abandoned_at])

        assert is_binary(abandoned_at)
      end
    end)

    :ok
  end

  @spec force_lock_expired!(binary(), keyword()) :: :ok
  defp force_lock_expired!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions SET locked_until = timezone('UTC', now()) - interval '1 second', updated_at = timezone('UTC', now()) WHERE id = $1",
      [to_db_id(execution_id)]
    )

    :ok
  end

  @spec force_wait_ready_and_lock_expired!(binary(), keyword()) :: :ok
  defp force_wait_ready_and_lock_expired!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions
SET
  waiting_until = timezone('UTC', now()) - interval '1 second',
  locked_until = timezone('UTC', now()) - interval '1 second',
  updated_at = timezone('UTC', now())
WHERE id = $1
",
      [to_db_id(execution_id)]
    )

    :ok
  end

  @spec force_cancelled_state!(binary(), keyword()) :: :ok
  defp force_cancelled_state!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions
SET
  status = 'cancelled'::#{prefix}.endurant_execution_status,
  locked_by = NULL,
  locked_until = NULL,
  waiting_until = NULL,
  updated_at = timezone('UTC', now())
WHERE id = $1
",
      [to_db_id(execution_id)]
    )

    :ok
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec kill_cached_executor!(binary(), String.t(), pos_integer()) :: :ok
  defp kill_cached_executor!(execution_id, engine_name, timeout_ms \\ 2000) do
    manager_name = Endurant.Supervisor.queue_manager_name(engine_name, :orders)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    pid = find_cached_executor_pid!(manager_name, execution_id, deadline)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      timeout_ms -> raise "cached executor #{inspect(pid)} did not exit in #{timeout_ms}ms"
    end
  end

  @spec find_cached_executor_pid!(term(), binary(), integer()) :: pid()
  defp find_cached_executor_pid!(manager_name, execution_id, deadline_ms) do
    state = :sys.get_state(manager_name)
    match = Enum.find(state.cached, fn {_ref, info} -> info.execution_id == execution_id end)

    case match do
      {_ref, %{pid: pid}} when is_pid(pid) ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          raise "cached executor not found for #{execution_id}"
        else
          Process.sleep(20)
          find_cached_executor_pid!(manager_name, execution_id, deadline_ms)
        end
    end
  end

  @spec reset_queue_manager_executors!(String.t(), pos_integer()) :: :ok
  defp reset_queue_manager_executors!(engine_name, timeout_ms) do
    manager_name = Endurant.Supervisor.queue_manager_name(engine_name, :orders)
    state = :sys.get_state(manager_name)

    pids =
      (state.running |> Map.values() |> Enum.map(& &1.pid)) ++
        (state.cached |> Map.values() |> Enum.map(& &1.pid))

    pids
    |> Enum.uniq()
    |> Enum.each(fn pid ->
      if is_pid(pid) and Process.alive?(pid) do
        Process.exit(pid, :kill)
      end
    end)

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_queue_idle!(manager_name, deadline)
  end

  @spec do_wait_for_queue_idle!(term(), integer()) :: :ok
  defp do_wait_for_queue_idle!(manager_name, deadline_ms) do
    state = :sys.get_state(manager_name)

    if map_size(state.running) == 0 and map_size(state.cached) == 0 do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline_ms do
        raise "queue manager not idle within timeout"
      else
        Process.sleep(20)
        do_wait_for_queue_idle!(manager_name, deadline_ms)
      end
    end
  end
end
