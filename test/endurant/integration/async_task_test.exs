defmodule Endurant.Integration.AsyncTaskTest do
  use Endurant.TestSupport.IntegrationCase

  test("task_async + task_await_many runs and commits deterministically", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.AsyncTaskTest.ManyWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "async-many:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            slow =
              task_async(nil, "slow", fn _ ->
                Process.sleep(40)
                2
              end)

            fast =
              task_async(nil, "fast", fn _ ->
                Process.sleep(5)
                1
              end)

            results = task_await_many([slow, fast])

            task(nil, "finish", fn _ ->
              %{
                id: input["id"],
                fast: Map.fetch!(results, "fast"),
                slow: Map.fetch!(results, "slow")
              }
            end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.AsyncTaskTest.ManyWorkflow,
               %{id: "m1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert result == %{id: "m1", fast: 1, slow: 2}
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_completed)) == 3
  end

  test("task_await handles async failures with retry policy", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.AsyncTaskTest.RetryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "async-retry:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            handle =
              task_async(
                nil,
                "issue_invoice",
                fn _ -> Endurant.TestSupport.WorkflowHelpers.RetryGate.issue(input["id"]) end,
                retry: [max_attempts: 2, base_ms: 20]
              )

            task_await(handle)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    {:ok, _pid} =
      start_supervised(
        {Endurant.TestSupport.WorkflowHelpers.RetryGate,
         name: Endurant.TestSupport.WorkflowHelpers.RetryGate}
      )

    :ok = Endurant.TestSupport.WorkflowHelpers.RetryGate.reset()

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.AsyncTaskTest.RetryWorkflow,
               %{id: "r1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    assert result == %{status: :issued, order_id: "r1"}
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_failed)) >= 1
    assert Enum.count(events, &(&1.type == :task_completed)) >= 1
  end

  test("task_async_stream fails on duplicate task keys", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.AsyncTaskTest.StreamDuplicateWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "async-stream-dup:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            task_async_stream([1, 2], fn _item -> "same" end, fn item -> item * 10 end)
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.AsyncTaskTest.StreamDuplicateWorkflow,
               %{id: "s1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :failed, result: error}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert Map.get(error, "message", "") =~ "task_async_stream requires unique task keys"
  end

  test("task_await_many fails execution when an async task crashes", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.AsyncTaskTest.FailWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "async-fail:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, _input) do
            bad = task_async(nil, "bad", fn _ -> raise "boom" end)

            good =
              task_async(nil, "good", fn _ ->
                Process.sleep(60)
                :ok
              end)

            task_await_many([bad, good])
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.AsyncTaskTest.FailWorkflow,
               %{id: "f1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :failed, result: error}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert (Map.get(error, "message") || Map.get(error, "reason") || inspect(error)) =~ "boom"
    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_failed)) >= 1
  end
end
