defmodule Endurant.Integration.CancelTest do
  use Endurant.TestSupport.IntegrationCase

  test("cancel pending execution marks it cancelled without starting", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.CancelTest.PendingBlockedWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("unmanaged")
            unique_id(fn %{id: id} -> "cancel-pending-blocked:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
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
               Endurant.Integration.CancelTest.PendingBlockedWorkflow,
               %{id: "p1"}
             )

    assert :pending = wait_for_status(execution.id, :pending, 2000, runtime_opts)
    assert :ok = Endurant.cancel(Keyword.fetch!(runtime_opts, :instance), execution.id)
    assert :cancelled = wait_for_status(execution.id, :cancelled, 3000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :cancel_requested))
    refute Enum.any?(events, &(&1.type == :execution_started))
    assert Enum.any?(events, &(&1.type == :execution_cancelled))
  end

  test("cancel waiting execution marks cancelled immediately and rejects signals", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.CancelTest.WaitingWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "cancel-waiting:#{id}" end)
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
               Endurant.Integration.CancelTest.WaitingWorkflow,
               %{id: "w1"}
             )

    assert :execution_waiting =
             wait_for_event(execution.id, :execution_waiting, 8000, runtime_opts)

    assert :waiting = wait_for_status(execution.id, :waiting, 3000, runtime_opts)
    assert :ok = Endurant.cancel(Keyword.fetch!(runtime_opts, :instance), execution.id)
    assert :cancelled = wait_for_status(execution.id, :cancelled, 3000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :cancel_requested))
    assert Enum.any?(events, &(&1.type == :execution_cancelled))

    assert {:error, :not_active} =
             Endurant.signal(Keyword.fetch!(runtime_opts, :instance), execution.id, "go_w1", %{})
  end

  test("cancel running execution transitions through cancelling and ends cancelled", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.CancelTest.RunningWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("manual")
            unique_id(fn %{id: id} -> "cancel-running:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long_task", fn _ ->
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
               Endurant.Integration.CancelTest.RunningWorkflow,
               %{id: "r1"}
             )

    worker_id = "test-worker:cancel-running"
    lease_ms = 30000
    [claimed] = Endurant.Executions.claim_pending(:manual, 1, worker_id, lease_ms, runtime_opts)
    assert claimed.id == execution.id

    Task.start(fn ->
      Endurant.Executor.run(
        claimed,
        runtime_opts ++ [worker_id: worker_id, queue_manager: self()]
      )
    end)

    assert :execution_started =
             wait_for_event(execution.id, :execution_started, 5000, runtime_opts)

    assert :running = wait_for_status(execution.id, :running, 2000, runtime_opts)
    assert :ok = Endurant.cancel(Keyword.fetch!(runtime_opts, :instance), execution.id)
    assert :cancel_requested = wait_for_event(execution.id, :cancel_requested, 2000, runtime_opts)
    assert :cancelled = wait_for_status(execution.id, :cancelled, 5000, runtime_opts)
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :cancel_requested))
    assert Enum.any?(events, &(&1.type == :execution_cancelled))
    refute Enum.any?(events, &(&1.type == :execution_completed))
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

  @spec wait_for_event(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_event(execution_id, expected_type, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_event(execution_id, expected_type, deadline, runtime_opts)
  end

  @spec do_wait_for_event(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_event(execution_id, expected_type, deadline, runtime_opts) do
    events = Endurant.events(Keyword.fetch!(runtime_opts, :instance), execution_id)

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
end