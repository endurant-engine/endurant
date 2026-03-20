defmodule Endurant.Integration.TaskSourceTest do
  use Endurant.TestSupport.IntegrationCase

  alias Endurant.TestSupport.PostgresHelper

  test "task_source reports executed after a fresh task run", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.TaskSourceTest.ExecutedWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "task-source:executed:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _value =
              task(input, "fetch_user", fn i ->
                %{"id" => i["id"], "step" => "fetched"}
              end)

            %{"source" => task_source("fetch_user")}
          end
        end
      end

    Code.compile_quoted(workflow_module)
    workflow = Module.concat(__MODULE__, ExecutedWorkflow)
    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               workflow,
               %{"id" => "one"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{source: :executed}
  end

  test "task_source reports history after replay", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.TaskSourceTest.HistoryWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{"id" => id} -> "task-source:history:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            _value =
              task(input, "fetch_user", fn i ->
                %{"id" => i["id"], "step" => "fetched"}
              end)

            source = task_source("fetch_user")
            _ = wait_signal("continue")
            %{"source" => source}
          end
        end
      end

    Code.compile_quoted(workflow_module)
    workflow = Module.concat(__MODULE__, HistoryWorkflow)
    instance = Keyword.fetch!(runtime_opts, :instance)

    assert {:ok, execution} =
             Endurant.insert(
               workflow,
               %{"id" => "one"},
               instance: instance
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 8_000, runtime_opts)

    kill_cached_executor!(execution.id, engine_name)
    force_lock_expired!(execution.id, runtime_opts)
    Process.sleep(150)

    assert :ok =
             Endurant.signal(execution.id, "continue", %{"ok" => true}, instance: instance)

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{source: :history}

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert 1 == Enum.count(events, &(&1.type == :task_started))
    assert 1 == Enum.count(events, &(&1.type == :task_completed))
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

  @spec force_lock_expired!(binary(), keyword()) :: :ok
  defp force_lock_expired!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)

    PostgresHelper.Repo.query!(
      "UPDATE #{prefix}.endurant_executions SET locked_until = timezone('UTC', now()) - interval '1 second', updated_at = timezone('UTC', now()) WHERE id = $1",
      [to_db_id(execution_id)]
    )

    :ok
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec kill_cached_executor!(binary(), String.t(), pos_integer()) :: :ok
  defp kill_cached_executor!(execution_id, engine_name, timeout_ms \\ 2_000) do
    manager_name = Endurant.Supervisor.queue_manager_name(engine_name, :orders)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    pid = find_cached_executor_pid!(manager_name, execution_id, deadline)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      timeout_ms -> raise "cached executor #{inspect(pid)} did not exit in #{timeout_ms}ms"
    end
  end

  @spec find_cached_executor_pid!(term(), binary(), integer()) :: pid()
  defp find_cached_executor_pid!(manager_name, execution_id, deadline_ms) do
    state = :sys.get_state(manager_name)
    match = Enum.find(state.cached, fn {_ref, info} -> info.execution_id == execution_id end)

    case match do
      {_ref, %{pid: pid}} when is_pid(pid) ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          raise "cached executor not found for #{execution_id}"
        else
          Process.sleep(20)
          find_cached_executor_pid!(manager_name, execution_id, deadline_ms)
        end
    end
  end
end
