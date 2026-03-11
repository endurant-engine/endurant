defmodule Endurant.Integration.PipeTaskTest do
  use Endurant.TestSupport.IntegrationCase

  test("workflow DX: pipe tasks with fn and capture forms", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.PipeTaskTest.PipeWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{user_id: user_id} -> "pipe:#{user_id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            input
            |> task("fetch_user", fn i ->
              Endurant.TestSupport.WorkflowHelpers.Accounts.fetch_user(i["user_id"])
            end)
            |> task("mark_premium", fn user -> Map.put(user, :kind, :pipe) end)
            |> task("persist", &Map.put(&1, :saved, true), retry: [max_attempts: 2])
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
               Endurant.Integration.PipeTaskTest.PipeWorkflow,
               %{user_id: "u-9"}
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert result == %{id: "u-9", premium: true, kind: :pipe, saved: true}
  end
end
