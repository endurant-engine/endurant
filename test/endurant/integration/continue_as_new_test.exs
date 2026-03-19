defmodule Endurant.Integration.ContinueAsNewTest do
  use Endurant.TestSupport.IntegrationCase

  test("continue_as_new carries unused signals and links the execution chain", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ContinueAsNewTest.Workflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "continue-as-new:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(version, input) do
            if input["continued"] do
              carried = wait_signal("carry")
              final = wait_signal("final")

              %{
                id: input["id"],
                version: version,
                first: input["first"],
                carried: carried,
                final: final
              }
            else
              first = wait_signal("first")

              continue_as_new(
                %{
                  "id" => input["id"],
                  "continued" => true,
                  "first" => first
                },
                version: "2",
                rollover_signals: true
              )
            end
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ContinueAsNewTest.Workflow,
               %{id: "c1"},
               instance: instance
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok = Endurant.signal(execution.id, "carry", %{value: "kept"}, instance: instance)
    assert :ok = Endurant.signal(execution.id, "first", %{value: "go"}, instance: instance)

    assert :continued_as_new =
             wait_for_status(execution.id, :continued_as_new, 5_000, runtime_opts)

    {:ok, old_events} = PostgresHelper.history(execution.id, runtime_opts)

    continued_event =
      Enum.find(old_events, &(&1.type == :execution_continued_as_new)) ||
        flunk("missing execution_continued_as_new event")

    assert payload_value(continued_event.payload, "first_execution_id") == execution.id

    new_execution_id =
      payload_value(continued_event.payload, "new_execution_id") ||
        flunk("missing new_execution_id in continue-as-new event")

    assert :waiting = wait_for_status(new_execution_id, :waiting, 5_000, runtime_opts)

    assert :ok =
             Endurant.signal("continue-as-new:c1", "final", %{value: "done"}, instance: instance)

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(new_execution_id, 8_000, runtime_opts)

    assert %{version: "2"} = Endurant.execution(new_execution_id, instance: instance)

    assert result == %{
             id: "c1",
             version: "2",
             first: %{value: :go},
             carried: %{value: "kept"},
             final: %{value: :done}
           }

    {:ok, new_events} = PostgresHelper.history(new_execution_id, runtime_opts)

    started_event =
      Enum.find(new_events, &(&1.type == :execution_started)) ||
        flunk("missing execution_started for continued execution")

    assert payload_value(started_event.payload, "first_execution_id") == execution.id
    assert payload_value(started_event.payload, "previous_execution_id") == execution.id
    assert payload_value(started_event.payload, "worker_id")

    assert Enum.any?(new_events, fn event ->
             event.type == :signal_received and payload_value(event.payload, "signal") == "carry"
           end)

    refute Enum.any?(old_events, fn event ->
             event.type == :signal_received and payload_value(event.payload, "signal") == "final"
           end)
  end

  test("continue_as_new does not roll over signals by default", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ContinueAsNewTest.NoSignalRolloverWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "continue-as-new:no-rollover:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            if input["continued"] do
              final = wait_signal("final")

              %{
                id: input["id"],
                final: final
              }
            else
              _first = wait_signal("first")

              continue_as_new(%{
                "id" => input["id"],
                "continued" => true
              })
            end
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ContinueAsNewTest.NoSignalRolloverWorkflow,
               %{id: "c2"},
               instance: instance
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok = Endurant.signal(execution.id, "carry", %{value: "old"}, instance: instance)
    assert :ok = Endurant.signal(execution.id, "first", %{value: "go"}, instance: instance)

    assert :continued_as_new =
             wait_for_status(execution.id, :continued_as_new, 5_000, runtime_opts)

    {:ok, old_events} = PostgresHelper.history(execution.id, runtime_opts)

    continued_event =
      Enum.find(old_events, &(&1.type == :execution_continued_as_new)) ||
        flunk("missing execution_continued_as_new event")

    new_execution_id =
      payload_value(continued_event.payload, "new_execution_id") ||
        flunk("missing new_execution_id in continue-as-new event")

    assert :waiting = wait_for_status(new_execution_id, :waiting, 5_000, runtime_opts)

    {:ok, new_events_before_final} = PostgresHelper.history(new_execution_id, runtime_opts)

    refute Enum.any?(new_events_before_final, fn event ->
             event.type == :signal_received and payload_value(event.payload, "signal") == "carry"
           end)

    assert :ok =
             Endurant.signal(
               "continue-as-new:no-rollover:c2",
               "final",
               %{value: "done"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(new_execution_id, 8_000, runtime_opts)

    assert result == %{
             id: "c2",
             final: %{value: :done}
           }
  end

  test("continue_as_new inside a task rethrows workflow control", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ContinueAsNewTest.TaskWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "continue-as-new:task:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            if input["continued"] do
              %{id: input["id"], continued: true}
            else
              task(input, "continue_task", fn i ->
                continue_as_new(%{
                  "id" => i["id"],
                  "continued" => true
                })
              end)
            end
          end
        end
      end

    Code.compile_quoted(workflow_module)

    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ContinueAsNewTest.TaskWorkflow,
               %{id: "ct-1"},
               instance: instance
             )

    assert :continued_as_new =
             wait_for_status(execution.id, :continued_as_new, 5_000, runtime_opts)

    {:ok, old_events} = PostgresHelper.history(execution.id, runtime_opts)

    continued_event =
      Enum.find(old_events, &(&1.type == :execution_continued_as_new)) ||
        flunk("missing execution_continued_as_new event")

    new_execution_id =
      payload_value(continued_event.payload, "new_execution_id") ||
        flunk("missing new_execution_id in continue-as-new event")

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(new_execution_id, 8_000, runtime_opts)

    assert result == %{id: "ct-1", continued: true}
    assert :task_started in Enum.map(old_events, & &1.type)
    refute :task_failed in Enum.map(old_events, & &1.type)
  end

  @spec wait_for_status(binary(), atom(), timeout(), keyword()) :: atom()
  defp wait_for_status(execution_id, status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    instance = Keyword.fetch!(runtime_opts, :instance)
    do_wait_for_status(instance, execution_id, status, deadline)
  end

  @spec do_wait_for_status(term(), binary(), atom(), integer()) :: atom()
  defp do_wait_for_status(instance, execution_id, status, deadline) do
    case Endurant.execution(execution_id, instance: instance) do
      %{status: ^status} ->
        status

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("execution #{execution_id} did not reach status #{inspect(status)}")
        else
          Process.sleep(25)
          do_wait_for_status(instance, execution_id, status, deadline)
        end
    end
  end

  @spec payload_value(map(), String.t()) :: term()
  defp payload_value(payload, key) do
    Map.get(payload, key) || Map.get(payload, String.to_atom(key))
  end
end
