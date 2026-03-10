defmodule Endurant.Integration.HistoryStatsTest do
  use Endurant.TestSupport.IntegrationCase

  test "workflow can read history_length/0 and history_size/0", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.HistoryStatsTest.HistoryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "history-stats:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            before_length = history_length()
            before_size = history_size()

            _ =
              task(input, "history_task", fn _ ->
                %{ok: true}
              end)

            after_length = history_length()
            after_size = history_size()

            %{
              before_length: before_length,
              before_size: before_size,
              after_length: after_length,
              after_size: after_size
            }
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               engine_name,
               Endurant.Integration.HistoryStatsTest.HistoryWorkflow,
               %{id: "hs-1"}
             )

    assert {:ok,
            %{
              status: :completed,
              result: %{
                before_length: before_length,
                before_size: before_size,
                after_length: after_length,
                after_size: after_size
              }
            }} = PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert before_length >= 2
    assert after_length >= before_length + 2
    assert after_size > before_size
  end
end
