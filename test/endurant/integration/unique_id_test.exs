defmodule Endurant.Integration.UniqueIdTest do
  use Endurant.TestSupport.IntegrationCase

  test "insert conflicts while execution with same unique_id is open", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.UniqueIdTest.OpenConflictWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "unique-open:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long_running", fn _ ->
              Process.sleep(500)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, first} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.OpenConflictWorkflow,
               %{id: "u-1"},
               runtime_opts
             )

    assert :running = wait_for_status(first.id, :running, 2_000, runtime_opts)

    assert {:error, :unique_conflict} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.OpenConflictWorkflow,
               %{id: "u-1"},
               runtime_opts
             )
  end

  test "insert succeeds again after previous execution with same unique_id is completed", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.UniqueIdTest.ReinsertAfterTerminalWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "unique-terminal:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, first} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.ReinsertAfterTerminalWorkflow,
               %{id: "u-2"},
               runtime_opts
             )

    assert {:ok, %{status: :completed, result: %{id: "u-2", ok: true}}} =
             PostgresHelper.wait_for_execution!(first.id, 5_000, runtime_opts)

    assert {:ok, second} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.ReinsertAfterTerminalWorkflow,
               %{id: "u-2"},
               runtime_opts
             )

    assert is_binary(second.id)
    assert second.id != first.id
  end

  test "insert conflicts while execution with same unique_id is waiting", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.UniqueIdTest.WaitingConflictWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "unique-waiting:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            _ = wait_signal("go")
            :ok
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, first} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.WaitingConflictWorkflow,
               %{id: "u-3"},
               runtime_opts
             )

    assert :waiting = wait_for_status(first.id, :waiting, 2_000, runtime_opts)

    assert {:error, :unique_conflict} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.WaitingConflictWorkflow,
               %{id: "u-3"},
               runtime_opts
             )
  end

  test "insert conflicts while execution with same unique_id is continuable", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.UniqueIdTest.ContinuableConflictWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "unique-continuable:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            _ = wait_signal("go")
            :ok
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, first} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.ContinuableConflictWorkflow,
               %{id: "u-4"},
               runtime_opts
             )

    assert :waiting = wait_for_status(first.id, :waiting, 2_000, runtime_opts)

    assert :ok = Endurant.Executions.mark_continuable(first.id, runtime_opts)
    assert :continuable = wait_for_status(first.id, :continuable, 2_000, runtime_opts)

    assert {:error, :unique_conflict} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.ContinuableConflictWorkflow,
               %{id: "u-4"},
               runtime_opts
             )
  end

  test "insert conflicts while execution with same unique_id is cancelling", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.UniqueIdTest.CancellingConflictWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "unique-cancelling:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(nil, "long_running", fn _ ->
              Process.sleep(2_000)
              %{id: input["id"], ok: true}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, first} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.CancellingConflictWorkflow,
               %{id: "u-5"},
               runtime_opts
             )

    assert :running = wait_for_status(first.id, :running, 2_000, runtime_opts)
    assert {:ok, :running} = Endurant.Executions.request_cancel(first.id, runtime_opts)
    assert :cancelling = wait_for_status(first.id, :cancelling, 2_000, runtime_opts)

    assert {:error, :unique_conflict} =
             Endurant.insert(
               Endurant.Integration.UniqueIdTest.CancellingConflictWorkflow,
               %{id: "u-5"},
               runtime_opts
             )
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(execution_id, runtime_opts) do
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
end
