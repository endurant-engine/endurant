defmodule Endurant.Integration.WaitingTest do
  use Endurant.TestSupport.IntegrationCase

  test("time-based waiting resumes execution after delay", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.WaitingTest.DelayWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "delay:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            sleep("delay:#{input["id"]}", 100)
            %{id: input["id"], done: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.WaitingTest.DelayWorkflow,
               %{id: "d-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: %{id: "d-1", done: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert :execution_waiting in Enum.map(events, & &1.type)
  end

  test("sleep inside a task rethrows workflow wait control", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.WaitingTest.TaskDelayWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "task-delay:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            delayed =
              task(input, "delay_task", fn i ->
                sleep("task-delay:#{i["id"]}", 100)
                %{id: i["id"], delayed: true}
              end)

            %{id: input["id"], delayed: delayed}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.WaitingTest.TaskDelayWorkflow,
               %{id: "td-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert result == %{id: "td-1", delayed: %{id: "td-1", delayed: true}}

    assert {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert :execution_waiting in Enum.map(events, & &1.type)
    assert :task_started in Enum.map(events, & &1.type)
    assert :task_completed in Enum.map(events, & &1.type)
    refute :task_failed in Enum.map(events, & &1.type)
  end

  test("signal-based waiting resumes when signal is written", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.WaitingTest.SignalWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            approval = wait_signal("approval_requested")
            %{id: input["id"], approval: approval}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.WaitingTest.SignalWorkflow,
               %{id: "s-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{approved: true},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert result == %{id: "s-1", approval: %{approved: true}}
  end

  test("wait_signal inside a task rethrows workflow wait control", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.WaitingTest.TaskSignalWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "task-signal:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            approval =
              task(nil, "await_signal", fn _ ->
                wait_signal("approval_requested")
              end)

            %{id: input["id"], approval: approval}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.WaitingTest.TaskSignalWorkflow,
               %{id: "ts-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{approved: true},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)

    assert result == %{id: "ts-1", approval: %{approved: true}}

    assert {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert :task_started in Enum.map(events, & &1.type)
    assert :task_completed in Enum.map(events, & &1.type)
    refute :task_failed in Enum.map(events, & &1.type)
  end

  test("multiple same signals sent before wait are consumed in order", %{
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.WaitingTest.SignalQueueWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal-queue:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            sleep("pre_wait:#{input["id"]}", 150)
            first = wait_signal("approval_requested")
            second = wait_signal("approval_requested")
            %{id: input["id"], approvals: [first, second]}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.WaitingTest.SignalQueueWorkflow,
               %{id: "sq-1"},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{n: 1},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{n: 2},
               instance: Keyword.fetch!(runtime_opts, :instance)
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8000, runtime_opts)

    assert result == %{id: "sq-1", approvals: [%{n: 1}, %{n: 2}]}
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(execution_id, instance: Keyword.fetch!(runtime_opts, :instance)) do
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
