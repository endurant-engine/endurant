defmodule Endurant.Integration.DXTest do
  use Endurant.TestSupport.IntegrationCase

  test "workflow DX: define, insert, await, replay, and inspect history", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.DXTest.OrderWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{order_id: order_id} -> "order:#{order_id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            user =
              task(nil, "fetch_user", fn _ ->
                Endurant.TestSupport.WorkflowHelpers.Accounts.fetch_user(input["user_id"])
              end)

            task(nil, "send_email", fn _ ->
              if user.premium do
                Endurant.TestSupport.WorkflowHelpers.Mailer.send_priority(user.id)
              else
                Endurant.TestSupport.WorkflowHelpers.Mailer.send_regular(user.id)
              end
            end)

            processed =
              for {item, idx} <- Enum.with_index(input["items"]) do
                task(nil, "process_item:#{idx}", fn _ ->
                  Endurant.TestSupport.WorkflowHelpers.Orders.process_item(item)
                end)
              end

            approval = %{status: :pending}

            task(nil, "finalize", fn _ ->
              Endurant.TestSupport.WorkflowHelpers.Orders.finalize(
                input["order_id"],
                approval,
                processed
              )
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.DXTest.OrderWorkflow,
               %{
                 order_id: "o-123",
                 user_id: "u-7",
                 items: ["book", "pen"]
               },
               runtime_opts
             )

    assert execution.workflow_module == "Endurant.Integration.DXTest.OrderWorkflow"
    assert execution.unique_id == "order:o-123"
    assert execution.status == :pending
    assert execution.version == "1"

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)

    assert result == %{
             order_id: "o-123",
             approval: %{status: :pending},
             processed: [%{item: "book", status: :ok}, %{item: "pen", status: :ok}]
           }

    assert {:ok, replayed} = PostgresHelper.replay(execution.id, runtime_opts)
    assert replayed == result

    assert {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    assert Enum.map(events, & &1.type) == [
             :execution_created,
             :execution_started,
             :task_started,
             :task_completed,
             :task_started,
             :task_completed,
             :task_started,
             :task_completed,
             :task_started,
             :task_completed,
             :task_started,
             :task_completed,
             :execution_completed
           ]
  end
end
