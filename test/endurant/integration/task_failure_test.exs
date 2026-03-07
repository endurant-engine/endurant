defmodule Endurant.Integration.TaskFailureTest do
  use Endurant.TestSupport.IntegrationCase

  test "workflow DX: happy path with wait and in-workflow retry", %{runtime_opts: runtime_opts} do
    retry_workflow_module =
      quote do
        defmodule Endurant.Integration.TaskFailureTest.WaitRetryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "wait-retry:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            user =
              task(nil, "fetch_user", fn _ ->
                %{id: input["user_id"], premium: true}
              end)

            approval = wait_signal("approval_requested")

            invoice =
              task(
                nil,
                "issue_invoice",
                fn _ ->
                  Endurant.TestSupport.WorkflowHelpers.RetryGate.issue(input["id"])
                end,
                retry: [max_attempts: 2, backoff: :constant, base_ms: 100]
              )

            user_id = Map.get(user, :id) || Map.get(user, "id")

            task(nil, "finalize", fn _ ->
              %{
                order_id: input["id"],
                user_id: user_id,
                approval: approval,
                invoice: invoice
              }
            end)
          end
        end
      end

    Code.compile_quoted(retry_workflow_module)

    {:ok, _pid} =
      start_supervised(
        {Endurant.TestSupport.WorkflowHelpers.RetryGate,
         name: Endurant.TestSupport.WorkflowHelpers.RetryGate}
      )

    :ok = Endurant.TestSupport.WorkflowHelpers.RetryGate.reset()

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.TaskFailureTest.WaitRetryWorkflow,
               %{id: "wr-1", user_id: "u-1"},
               runtime_opts
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{approved: true},
               runtime_opts
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{
             order_id: "wr-1",
             user_id: "u-1",
             approval: %{approved: true},
             invoice: %{status: :issued, order_id: "wr-1"}
           }

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    assert :execution_waiting in Enum.map(events, & &1.type)
    assert Enum.count(events, &(&1.type == :task_failed)) >= 1
    assert Enum.count(events, &(&1.type == :task_completed)) >= 3
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
