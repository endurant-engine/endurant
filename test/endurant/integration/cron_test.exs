defmodule Endurant.Integration.CronTest do
  use Endurant.TestSupport.IntegrationCase

  alias Endurant.Crons

  defmodule FastCronWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      unique_id(fn %{"id" => id} -> "cron:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      %{id: input["id"], kind: "fast-cron"}
    end
  end

  defmodule BlockingCronWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      unique_id(fn %{"id" => id} -> "cron:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      wait_signal("release_#{input["id"]}")
      %{id: input["id"], kind: "blocking-cron"}
    end
  end

  defmodule QueueRequiredCronWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      unique_id(fn %{"id" => id} -> "cron-no-queue:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      %{id: input["id"], kind: "no-queue"}
    end
  end

  test "cron schedule dispatches fires and keeps status active at end_at boundary", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    start_at = DateTime.utc_now()
    end_at = DateTime.add(start_at, 2, :second)

    assert {:ok, cron} =
             Endurant.cron(
               FastCronWorkflow,
               %{"id" => "loop"},
               "*/1 * * * * *",
               instance: engine_name,
               start_at: start_at,
               end_at: end_at
             )

    assert wait_until(fn ->
             Endurant.cron_fires(cron.id, limit: 10, instance: engine_name)
             |> Enum.any?(&(&1.status == :dispatched))
           end)

    assert wait_until(
             fn ->
               case Crons.get(cron.id, runtime_opts) do
                 %{status: :active} -> true
                 _ -> false
               end
             end,
             5_000
           )

    # Past end_at, schedule remains active but no fires should exist beyond end_at.
    Process.sleep(1_500)
    fires = Endurant.cron_fires(cron.id, limit: 20, instance: engine_name)
    assert fires != []
    assert Enum.all?(fires, &(DateTime.compare(&1.scheduled_for, end_at) != :gt))
  end

  test "cron skip overlap when same unique_id is running", %{
    engine_name: engine_name
  } do
    assert {:ok, running} =
             Endurant.insert(BlockingCronWorkflow, %{"id" => "skip"}, instance: engine_name)

    assert wait_for_execution_status(engine_name, running.id, [:running, :waiting])

    start_at = DateTime.utc_now()
    end_at = DateTime.add(start_at, 2, :second)

    assert {:ok, cron} =
             Endurant.cron(
               FastCronWorkflow,
               %{"id" => "skip"},
               "*/1 * * * * *",
               instance: engine_name,
               start_at: start_at,
               end_at: end_at
             )

    assert wait_until(fn ->
             Endurant.cron_fires(cron.id, limit: 10, instance: engine_name)
             |> Enum.any?(&(&1.status == :skipped))
           end)
  end

  test "public API lists, pauses, resumes, and deletes cron schedules", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    start_at = DateTime.add(DateTime.utc_now(), 60, :second)

    assert {:ok, cron} =
             Endurant.cron(
               FastCronWorkflow,
               %{"id" => "control"},
               "*/1 * * * * *",
               instance: engine_name,
               start_at: start_at,
               end_at: DateTime.add(start_at, 60, :second)
             )

    listed = Endurant.crons(status: :active, instance: engine_name)
    assert Enum.any?(listed, &(&1.id == cron.id))

    assert :ok = Endurant.pause_cron(cron.id, instance: engine_name)

    assert wait_until(fn ->
             case Crons.get(cron.id, runtime_opts) do
               %{status: :paused} -> true
               _ -> false
             end
           end)

    assert :ok = Endurant.resume_cron(cron.id, instance: engine_name)

    assert wait_until(fn ->
             case Crons.get(cron.id, runtime_opts) do
               %{status: :active} -> true
               _ -> false
             end
           end)

    assert :ok = Endurant.delete_cron(cron.id, instance: engine_name)

    assert wait_until(fn ->
             case Crons.get(cron.id, runtime_opts) do
               nil -> true
               _ -> false
             end
           end)
  end

  test "cron requires workflows to declare a queue", %{engine_name: engine_name} do
    assert_raise ArgumentError, ~r/workflow must define queue/, fn ->
      Endurant.cron(
        QueueRequiredCronWorkflow,
        %{"id" => "missing-queue"},
        "*/1 * * * * *",
        instance: engine_name,
        start_at: DateTime.utc_now(),
        end_at: DateTime.add(DateTime.utc_now(), 60, :second)
      )
    end
  end

  test "startup sync upserts config cron and keeps manual cron rows", %{
    engine_name: engine_name,
    prefix: prefix
  } do
    sync_engine = "#{engine_name}.sync"
    runtime_opts = PostgresHelper.runtime_opts(prefix, sync_engine)
    now = DateTime.utc_now()
    start_at = DateTime.add(now, 3_600, :second)
    end_at = DateTime.add(start_at, 3_600, :second)

    cleanup_supervisor(sync_engine)

    on_exit(fn ->
      cleanup_supervisor(sync_engine)
    end)

    base_opts = [
      name: sync_engine,
      repo: PostgresHelper.Repo,
      prefix: prefix,
      queues: [orders: [concurrency: 1, cached_limit: 1, poll_interval: 25]]
    ]

    assert {:ok, pid1} =
             Endurant.Supervisor.start_link(
               base_opts ++
                 [
                   crons: [
                     %{
                       name: "cfg-sync",
                       expr: "*/10 * * * * *",
                       workflow: FastCronWorkflow,
                       input: %{"id" => "cfg-sync"},
                       start_at: start_at,
                       end_at: end_at,
                       status: :active
                     }
                   ]
                 ]
             )

    Process.unlink(pid1)

    config_cron =
      Endurant.crons(instance: sync_engine)
      |> Enum.find(&(&1.name == "cfg-sync"))

    assert config_cron != nil
    assert config_cron.status == :active
    assert config_cron.cron_expr == "*/10 * * * * *"

    assert {:ok, manual} =
             Endurant.cron(
               FastCronWorkflow,
               %{"id" => "manual-sync"},
               "*/15 * * * * *",
               instance: sync_engine,
               start_at: start_at,
               end_at: end_at
             )

    assert :ok = Endurant.pause_cron(config_cron.id, instance: sync_engine)

    assert wait_until(fn ->
             case Crons.get(config_cron.id, runtime_opts) do
               %{status: :paused} -> true
               _ -> false
             end
           end)

    Process.exit(pid1, :shutdown)

    assert wait_until(
             fn -> Endurant.Supervisor.supervisor_pid(sync_engine) == nil end,
             2_000
           )

    assert {:ok, _pid2} =
             Endurant.Supervisor.start_link(
               base_opts ++
                 [
                   crons: [
                     %{
                       name: "cfg-sync",
                       expr: "*/20 * * * * *",
                       workflow: FastCronWorkflow,
                       input: %{"id" => "cfg-sync-2"},
                       start_at: start_at,
                       end_at: end_at,
                       status: :active
                     }
                   ]
                 ]
             )

    case Endurant.Supervisor.supervisor_pid(sync_engine) do
      pid when is_pid(pid) -> Process.unlink(pid)
      _ -> :ok
    end

    assert wait_until(fn ->
             case Crons.get(config_cron.id, runtime_opts) do
               %{status: :active, cron_expr: "*/20 * * * * *", input: %{"id" => "cfg-sync-2"}} ->
                 true

               _ ->
                 false
             end
           end)

    assert Crons.get(manual.id, runtime_opts) != nil
    :ok = cleanup_supervisor(sync_engine)
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

  @spec cleanup_supervisor(Endurant.Config.instance_name()) :: :ok
  defp cleanup_supervisor(instance) do
    case Endurant.Supervisor.supervisor_pid(instance) do
      pid when is_pid(pid) ->
        Process.exit(pid, :shutdown)

        stopped? =
          wait_until(fn -> Endurant.Supervisor.supervisor_pid(instance) == nil end, 2_000)

        if not stopped? do
          Process.exit(pid, :kill)
          _ = wait_until(fn -> Endurant.Supervisor.supervisor_pid(instance) == nil end, 1_000)
        end

        :ok

      _ ->
        :ok
    end
  end
end
