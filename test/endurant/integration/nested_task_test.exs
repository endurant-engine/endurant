defmodule Endurant.Integration.NestedTaskTest do
  use Endurant.TestSupport.IntegrationCase

  test "task can be called from nested workflow functions", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.NestedTaskTest.NestedWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "nested:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            user = fetch_user(input)
            build_result(input, user)
          end

          defp fetch_user(input) do
            task(input, "fetch_user", fn i ->
              Endurant.TestSupport.WorkflowHelpers.Accounts.fetch_user(i["user_id"])
            end)
          end

          defp build_result(input, user) do
            task(%{id: input["id"], user: user}, "build_result", fn state ->
              %{id: state.id, user_id: state.user.id, premium: state.user.premium}
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.NestedTaskTest.NestedWorkflow,
               %{id: "n-1", user_id: "u-42"},
               runtime_opts
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)

    assert result == %{id: "n-1", user_id: "u-42", premium: true}

    assert {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    assert Enum.map(events, & &1.type) == [
             :execution_created,
             :execution_started,
             :task_started,
             :task_completed,
             :task_started,
             :task_completed,
             :execution_completed
           ]
  end
end
