defmodule Endurant.Integration.ScheduledTest do
  use Endurant.TestSupport.IntegrationCase

  alias Endurant.Schedules

  defmodule FastScheduleWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      unique_id(fn %{"id" => id} -> "scheduled:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      %{id: input["id"], kind: "fast"}
    end
  end

  defmodule BlockingScheduleWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      unique_id(fn %{"id" => id} -> "scheduled:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      wait_signal("release_#{input["id"]}")
      %{id: input["id"], kind: "blocking"}
    end
  end

  test "scheduled row dispatches to execution", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    scheduled_at = DateTime.add(DateTime.utc_now(), 100, :millisecond)

    assert {:ok, scheduled} =
             Endurant.schedule(
               FastScheduleWorkflow,
               %{"id" => "dispatch"},
               scheduled_at,
               instance: engine_name
             )

    assert wait_until(fn ->
             case Schedules.get(scheduled.id, runtime_opts) do
               %{status: :dispatched, dispatched_execution_id: execution_id}
               when is_binary(execution_id) ->
                 true

               _ ->
                 false
             end
           end)

    %{dispatched_execution_id: execution_id} = Schedules.get(scheduled.id, runtime_opts)

    assert {:ok, %{status: :completed, result: %{id: id, kind: kind}}} =
             PostgresHelper.wait_for_execution!(execution_id, 5_000, runtime_opts)

    assert id in ["dispatch", :dispatch]
    assert kind in ["fast", :fast]
  end

  test "skip policy marks scheduled row skipped when same unique_id is already open", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    assert {:ok, running} =
             Endurant.insert(BlockingScheduleWorkflow, %{"id" => "skip"}, instance: engine_name)

    assert wait_for_execution_status(engine_name, running.id, [:running, :waiting])

    assert {:ok, scheduled} =
             Endurant.schedule(
               FastScheduleWorkflow,
               %{"id" => "skip"},
               DateTime.utc_now(),
               instance: engine_name
             )

    assert wait_until(fn ->
             case Schedules.get(scheduled.id, runtime_opts) do
               %{status: :skipped} -> true
               _ -> false
             end
           end)
  end

  test "public API lists and cancels scheduled rows", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    scheduled_at = DateTime.add(DateTime.utc_now(), 60, :second)

    assert {:ok, scheduled} =
             Endurant.schedule(
               FastScheduleWorkflow,
               %{"id" => "cancel-scheduled"},
               scheduled_at,
               instance: engine_name
             )

    listed = Endurant.scheduled(status: :pending, instance: engine_name)
    assert Enum.any?(listed, &(&1.id == scheduled.id))

    assert :ok = Endurant.cancel_scheduled(scheduled.id, instance: engine_name)

    assert wait_until(fn ->
             case Schedules.get(scheduled.id, runtime_opts) do
               %{status: :cancelled} -> true
               _ -> false
             end
           end)
  end

  @spec wait_for_execution_status(
          Endurant.Config.instance_name(),
          binary(),
          [atom()],
          non_neg_integer()
        ) ::
          boolean()
  defp wait_for_execution_status(instance, execution_id, statuses, timeout_ms \\ 2_000) do
    wait_until(
      fn ->
        case Endurant.execution(execution_id, instance: instance) do
          %{status: status} -> status in statuses
          _ -> false
        end
      end,
      timeout_ms
    )
  end

  @spec wait_until((-> boolean()), non_neg_integer(), non_neg_integer()) :: boolean()
  defp wait_until(fun, timeout_ms \\ 2_000, poll_ms \\ 25) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, poll_ms)
  end

  @spec do_wait_until((-> boolean()), integer(), non_neg_integer()) :: boolean()
  defp do_wait_until(fun, deadline, poll_ms) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(poll_ms)
        do_wait_until(fun, deadline, poll_ms)
      end
    end
  end
end
