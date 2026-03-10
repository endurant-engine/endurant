defmodule Endurant.Integration.QueueManagerTest do
  use Endurant.TestSupport.IntegrationCase

  test(
    "parked_limit saturation parks up to limit but additional executions still reach waiting state",
    %{runtime_opts: runtime_opts}
  ) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.QueueManagerTest.WaitingLimitWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "waiting-limit:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go_#{input["id"]}")
            %{id: input["id"], done: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, a} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.WaitingLimitWorkflow,
               %{id: "a"}
             )

    assert :waiting = wait_for_status(a.id, :waiting, 2000, runtime_opts)

    assert {:ok, b} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.WaitingLimitWorkflow,
               %{id: "b"}
             )

    assert :waiting = wait_for_status(b.id, :waiting, 2000, runtime_opts)
    assert :ok = Endurant.signal(Keyword.fetch!(runtime_opts, :instance), a.id, "go_a", %{})

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(a.id, 5000, runtime_opts)

    assert :waiting = wait_for_status(b.id, :waiting, 2000, runtime_opts)
    assert :ok = Endurant.signal(Keyword.fetch!(runtime_opts, :instance), b.id, "go_b", %{})

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(b.id, 5000, runtime_opts)
  end

  test("ready waiting executions resume before claiming new pending executions", %{
    runtime_opts: runtime_opts
  }) do
    waiting_workflow_module =
      quote do
        defmodule Endurant.Integration.QueueManagerTest.ResumePriorityWaitingWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "resume-priority-waiting:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go_#{input["id"]}")
            task(nil, "finish", fn _ -> %{id: input["id"], kind: :waiting} end)
          end
        end
      end

    pending_workflow_module =
      quote do
        defmodule Endurant.Integration.QueueManagerTest.ResumePriorityPendingWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "resume-priority-pending:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{id: input["id"], kind: :pending}
          end
        end
      end

    Code.compile_quoted(waiting_workflow_module)
    Code.compile_quoted(pending_workflow_module)

    assert {:ok, waiting_execution} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.ResumePriorityWaitingWorkflow,
               %{id: "w1"}
             )

    assert :waiting = wait_for_status(waiting_execution.id, :waiting, 2000, runtime_opts)

    assert {:ok, pending_execution} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.ResumePriorityPendingWorkflow,
               %{id: "p1"}
             )

    assert %{status: :pending} =
             Endurant.execution(Keyword.fetch!(runtime_opts, :instance), pending_execution.id)

    assert :ok =
             Endurant.signal(
               Keyword.fetch!(runtime_opts, :instance),
               waiting_execution.id,
               "go_w1",
               %{}
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(waiting_execution.id, 5000, runtime_opts)

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(pending_execution.id, 5000, runtime_opts)

    _ = event_inserted_at!(waiting_execution.id, :execution_completed, runtime_opts)
    _ = event_inserted_at!(pending_execution.id, :execution_started, runtime_opts)
  end

  test("queue manager recovers waiting/running execution when executor process dies", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.QueueManagerTest.RecoveryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "recovery:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long_step", fn _ ->
              Process.sleep(2000)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.RecoveryWorkflow,
               %{id: "r1"}
             )

    assert :running = wait_for_status(execution.id, :running, 2000, runtime_opts)
    queue_manager = queue_manager_pid!(engine_name)
    executor_pid = running_executor_pid!(queue_manager, execution.id)
    Process.exit(executor_pid, :kill)
    force_lock_expired!(execution.id, runtime_opts)

    assert {:ok, %{status: :completed, result: %{id: "r1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_abandoned))
    assert Enum.any?(events, &(&1.type == :execution_resumed))
  end

  test("expired waiting execution stays waiting until ready, then is abandoned and resumed", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.QueueManagerTest.WaitingRecoveryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "recovery-waiting:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go_#{input["id"]}")
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Keyword.fetch!(
                 runtime_opts,
                 :instance
               ),
               Endurant.Integration.QueueManagerTest.WaitingRecoveryWorkflow,
               %{id: "wr1"}
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2000, runtime_opts)
    queue_manager = queue_manager_pid!(engine_name)
    waiting_executor = parked_executor_pid!(queue_manager, execution.id)
    Process.exit(waiting_executor, :kill)
    force_lock_expired!(execution.id, runtime_opts)
    Process.sleep(150)

    assert %{status: :waiting} =
             Endurant.execution(Keyword.fetch!(runtime_opts, :instance), execution.id)

    assert :ok =
             Endurant.signal(Keyword.fetch!(runtime_opts, :instance), execution.id, "go_wr1", %{})

    assert {:ok, %{status: :completed, result: %{id: "wr1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_abandoned))
    assert Enum.any?(events, &(&1.type == :execution_resumed))
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(Keyword.fetch!(runtime_opts, :instance), execution_id) do
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

  @spec queue_manager_pid!(String.t()) :: pid()
  defp queue_manager_pid!(engine_name) do
    name = Endurant.Supervisor.queue_manager_name(engine_name, :orders)
    GenServer.whereis(name) || raise "queue manager not found for #{inspect(engine_name)}"
  end

  @spec running_executor_pid!(pid(), binary()) :: pid()
  defp running_executor_pid!(queue_manager, execution_id) when is_pid(queue_manager) do
    deadline = System.monotonic_time(:millisecond) + 2000
    do_find_running_executor_pid(queue_manager, execution_id, deadline)
  end

  @spec do_find_running_executor_pid(pid(), binary(), integer()) :: pid()
  defp do_find_running_executor_pid(queue_manager, execution_id, deadline) do
    state = :sys.get_state(queue_manager)

    case Enum.find(state.running, fn {_ref, info} -> info.execution_id == execution_id end) do
      {_ref, info} ->
        info.pid

      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("executor for #{execution_id} not found in running set")
        else
          Process.sleep(20)
          do_find_running_executor_pid(queue_manager, execution_id, deadline)
        end
    end
  end

  @spec parked_executor_pid!(pid(), binary()) :: pid()
  defp parked_executor_pid!(queue_manager, execution_id) when is_pid(queue_manager) do
    deadline = System.monotonic_time(:millisecond) + 2000
    do_find_parked_executor_pid(queue_manager, execution_id, deadline)
  end

  @spec do_find_parked_executor_pid(pid(), binary(), integer()) :: pid()
  defp do_find_parked_executor_pid(queue_manager, execution_id, deadline) do
    state = :sys.get_state(queue_manager)

    case Enum.find(state.parked, fn {_ref, info} -> info.execution_id == execution_id end) do
      {_ref, info} ->
        info.pid

      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("executor for #{execution_id} not found in parked set")
        else
          Process.sleep(20)
          do_find_parked_executor_pid(queue_manager, execution_id, deadline)
        end
    end
  end

  @spec force_lock_expired!(binary(), keyword()) :: :ok
  defp force_lock_expired!(execution_id, runtime_opts) do
    db_id =
      case Ecto.UUID.dump(execution_id) do
        {:ok, dumped} -> dumped
        :error -> execution_id
      end

    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions SET locked_until = timezone('UTC', now()) - interval '1 second', updated_at = timezone('UTC', now()) WHERE id = $1",
      [db_id]
    )

    :ok
  end

  @spec event_inserted_at!(binary(), atom(), keyword()) :: NaiveDateTime.t()
  defp event_inserted_at!(execution_id, event_type, runtime_opts) do
    {:ok, events} = PostgresHelper.history(execution_id, runtime_opts)

    case Enum.find(events, &(&1.type == event_type)) do
      %{inserted_at: %NaiveDateTime{} = inserted_at} -> inserted_at
      %{inserted_at: %DateTime{} = inserted_at} -> DateTime.to_naive(inserted_at)
      nil -> flunk("event #{inspect(event_type)} not found for execution #{execution_id}")
    end
  end
end