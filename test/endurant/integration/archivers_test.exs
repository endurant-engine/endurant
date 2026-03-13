defmodule Endurant.Integration.ArchiversTest do
  use Endurant.TestSupport.IntegrationCase, async: false

  alias Endurant.ArchiveWorker
  alias Endurant.Archivers
  alias Endurant.Migrations.Postgres
  alias Endurant.Pruner
  alias Endurant.Settings

  defmodule TestArchiver do
    @behaviour Endurant.Archiver

    @impl true
    def init(version, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:archiver_init, version, opts[:archiver]})
      :ok
    end

    @impl true
    def archive(batch, version, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:archiver_archive, batch, version, opts[:archiver]})
      :ok
    end
  end

  test "archiver settings can be listed and filtered by enabled", %{runtime_opts: runtime_opts} do
    assert :ok =
             Archivers.put(
               "clickhouse",
               %{"enabled" => true, "cursor" => %{"completed_at" => nil, "execution_id" => nil}},
               runtime_opts
             )

    assert :ok = Archivers.put("s3", %{"enabled" => false}, runtime_opts)

    assert [
             %{"archiver" => "clickhouse", "enabled" => true},
             %{"archiver" => "s3", "enabled" => false}
           ] = Archivers.list(runtime_opts)

    assert [%{"archiver" => "clickhouse", "enabled" => true}] = Archivers.enabled(runtime_opts)
    assert ["clickhouse"] = Archivers.enabled_names(runtime_opts)
  end

  test "put_cursor updates cursor and preserves other setting fields", %{
    runtime_opts: runtime_opts
  } do
    assert :ok = Archivers.put("clickhouse", %{"enabled" => true}, runtime_opts)

    assert :ok =
             Archivers.put_cursor(
               "clickhouse",
               ~U[2026-03-12 09:45:00Z],
               "11111111-1111-1111-1111-111111111111",
               runtime_opts
             )

    assert %{
             "archiver" => "clickhouse",
             "enabled" => true,
             "cursor" => %{
               "completed_at" => "2026-03-12T09:45:00Z",
               "execution_id" => "11111111-1111-1111-1111-111111111111"
             }
           } = Archivers.get("clickhouse", runtime_opts)
  end

  test "archiver acquires a lease on its own settings row when enabled", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  } do
    archiver = "clickhouse"
    setting_id = Archivers.setting_id(archiver)

    assert :ok = Archivers.put(archiver, %{"enabled" => true}, runtime_opts)

    assert {:ok, _pid} =
             start_supervised(
               {ArchiveWorker,
                instance: engine_name,
                archiver: archiver,
                repo: Keyword.fetch!(runtime_opts, :repo),
                prefix: Keyword.fetch!(runtime_opts, :prefix),
                heartbeat_ms: 50,
                retry_ms: 25,
                scan_ms: 100}
             )

    assert wait_until(fn ->
             case Settings.get(setting_id, runtime_opts) do
               %{"owner" => owner, "lease_until_ms" => lease_until}
               when is_binary(owner) and owner != "" and is_integer(lease_until) ->
                 true

               _ ->
                 false
             end
           end)
  end

  test "archiver init receives the current migrated endurant version", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  } do
    archiver = "clickhouse"
    migrated_version = Postgres.migrated_version(runtime_opts)

    assert :ok = Archivers.put(archiver, %{"enabled" => true}, runtime_opts)

    assert {:ok, _pid} =
             start_supervised(
               {ArchiveWorker,
                instance: engine_name,
                archiver: archiver,
                archiver_module: TestArchiver,
                archiver_opts: [test_pid: self()],
                repo: Keyword.fetch!(runtime_opts, :repo),
                prefix: Keyword.fetch!(runtime_opts, :prefix),
                heartbeat_ms: 50,
                retry_ms: 25,
                scan_ms: 100}
             )

    assert_receive {:archiver_init, ^migrated_version, "clickhouse"}, 2_000
  end

  test "supervisor starts configured archivers", %{runtime_opts: runtime_opts, prefix: prefix} do
    archiver = "configured"
    instance = "#{__MODULE__}.configured"
    migrated_version = Postgres.migrated_version(runtime_opts)

    assert {:ok, _pid} =
             start_supervised(
               {Endurant.Supervisor,
                name: instance,
                repo: Keyword.fetch!(runtime_opts, :repo),
                prefix: prefix,
                archivers: [
                  configured: [
                    module: TestArchiver,
                    enabled: true,
                    url: "http://clickhouse.test:8123",
                    database: "archive_db",
                    test_pid: self(),
                    heartbeat_ms: 50,
                    retry_ms: 25,
                    scan_ms: 100
                  ]
                ]}
             )

    assert_receive {:archiver_init, ^migrated_version, "configured"}, 2_000

    assert %{
             "archiver" => "configured",
             "enabled" => true,
             "url" => "http://clickhouse.test:8123",
             "database" => "archive_db"
           } = Archivers.get(archiver, runtime_opts)
  end

  test "supervisor archiver config sync preserves cursor state", %{
    runtime_opts: runtime_opts,
    prefix: prefix
  } do
    archiver = "configured"
    instance = "#{__MODULE__}.configured_cursor"

    assert :ok =
             Archivers.put(
               archiver,
               %{
                 "enabled" => false,
                 "cursor" => %{
                   "completed_at" => "2026-03-12T09:05:00Z",
                   "execution_id" => "11111111-1111-1111-1111-111111111111"
                 }
               },
               runtime_opts
             )

    assert {:ok, _pid} =
             start_supervised(
               {Endurant.Supervisor,
                name: instance,
                repo: Keyword.fetch!(runtime_opts, :repo),
                prefix: prefix,
                archivers: [
                  configured: [
                    module: TestArchiver,
                    enabled: true,
                    url: "http://clickhouse.test:8123",
                    test_pid: self(),
                    heartbeat_ms: 50,
                    retry_ms: 25,
                    scan_ms: 100
                  ]
                ]}
             )

    assert %{
             "enabled" => true,
             "url" => "http://clickhouse.test:8123",
             "cursor" => %{
               "completed_at" => "2026-03-12T09:05:00Z",
               "execution_id" => "11111111-1111-1111-1111-111111111111"
             }
           } = Archivers.get(archiver, runtime_opts)
  end

  test "archive worker archives terminal execution batches, records deliveries, and advances cursor",
       %{runtime_opts: runtime_opts, engine_name: engine_name, prefix: prefix} do
    repo = Keyword.fetch!(runtime_opts, :repo)
    archiver = "clickhouse"

    execution_1 =
      insert_terminal_execution!(repo, prefix, %{
        unique_id: "archive-worker-1",
        completed_at: ~U[2026-03-12 09:00:00Z],
        inserted_at: ~U[2026-03-12 08:55:00Z],
        updated_at: ~U[2026-03-12 09:00:00Z]
      })

    insert_event!(
      repo,
      prefix,
      execution_1,
      1,
      "execution_started",
      %{},
      ~U[2026-03-12 08:56:00Z]
    )

    insert_event!(
      repo,
      prefix,
      execution_1,
      2,
      "execution_completed",
      %{"result" => %{"ok" => true}},
      ~U[2026-03-12 09:00:00Z]
    )

    execution_2 =
      insert_terminal_execution!(repo, prefix, %{
        unique_id: "archive-worker-2",
        completed_at: ~U[2026-03-12 09:05:00Z],
        inserted_at: ~U[2026-03-12 09:01:00Z],
        updated_at: ~U[2026-03-12 09:05:00Z],
        status: "failed"
      })

    insert_event!(
      repo,
      prefix,
      execution_2,
      1,
      "execution_failed",
      %{"error" => %{"message" => "boom"}},
      ~U[2026-03-12 09:05:00Z]
    )

    assert :ok = Archivers.put(archiver, %{"enabled" => true}, runtime_opts)

    assert {:ok, _pid} =
             start_supervised(
               {ArchiveWorker,
                instance: engine_name,
                archiver: archiver,
                archiver_module: TestArchiver,
                archiver_opts: [test_pid: self()],
                batch_size: 10,
                heartbeat_ms: 50,
                retry_ms: 25,
                scan_ms: 100,
                repo: repo,
                prefix: prefix}
             )

    assert_receive {:archiver_archive, batch, migrated_version, "clickhouse"}, 2_000
    assert migrated_version == Postgres.migrated_version(runtime_opts)
    assert Enum.map(batch, & &1.execution.id) == [execution_1, execution_2]
    assert Enum.map(batch, &length(&1.events)) == [2, 1]

    assert wait_until(fn ->
             delivered_ids = delivered_execution_ids(repo, prefix, archiver)
             Enum.sort(delivered_ids) == Enum.sort([execution_1, execution_2])
           end)

    assert wait_until(fn ->
             case Archivers.get(archiver, runtime_opts) do
               %{
                 "cursor" => %{
                   "execution_id" => ^execution_2,
                   "completed_at" => completed_at
                 }
               }
               when is_binary(completed_at) ->
                 String.starts_with?(completed_at, "2026-03-12T09:05:00")

               _ ->
                 false
             end
           end),
           "final archiver setting: #{inspect(Archivers.get(archiver, runtime_opts))}"
  end

  test "pruner fully deletes terminal executions only after all enabled archivers delivered",
       %{runtime_opts: runtime_opts, engine_name: engine_name, prefix: prefix} do
    repo = Keyword.fetch!(runtime_opts, :repo)

    old_completed_at =
      DateTime.utc_now()
      |> DateTime.add(-120_000, :millisecond)
      |> DateTime.truncate(:microsecond)

    older_inserted_at = DateTime.add(old_completed_at, -300, :second)
    recent_completed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    archived_execution =
      insert_terminal_execution!(repo, prefix, %{
        unique_id: "pruner-archived",
        completed_at: old_completed_at,
        inserted_at: older_inserted_at,
        updated_at: old_completed_at
      })

    partially_archived_execution =
      insert_terminal_execution!(repo, prefix, %{
        unique_id: "pruner-partial",
        completed_at: DateTime.add(old_completed_at, 1, :second),
        inserted_at: DateTime.add(older_inserted_at, 1, :second),
        updated_at: DateTime.add(old_completed_at, 1, :second)
      })

    recent_execution =
      insert_terminal_execution!(repo, prefix, %{
        unique_id: "pruner-recent",
        completed_at: recent_completed_at,
        inserted_at: DateTime.add(recent_completed_at, -1, :second),
        updated_at: recent_completed_at
      })

    insert_event!(
      repo,
      prefix,
      archived_execution,
      1,
      "execution_completed",
      %{},
      old_completed_at
    )

    insert_event!(
      repo,
      prefix,
      partially_archived_execution,
      1,
      "execution_completed",
      %{},
      DateTime.add(old_completed_at, 1, :second)
    )

    insert_event!(
      repo,
      prefix,
      recent_execution,
      1,
      "execution_completed",
      %{},
      recent_completed_at
    )

    assert :ok = Archivers.put("clickhouse", %{"enabled" => true}, runtime_opts)
    assert :ok = Archivers.put("s3", %{"enabled" => true}, runtime_opts)

    insert_delivery!(repo, prefix, "clickhouse", archived_execution)
    insert_delivery!(repo, prefix, "s3", archived_execution)
    insert_delivery!(repo, prefix, "clickhouse", partially_archived_execution)
    insert_delivery!(repo, prefix, "clickhouse", recent_execution)
    insert_delivery!(repo, prefix, "s3", recent_execution)

    assert {:ok, _pid} =
             start_supervised(
               {Pruner,
                instance: engine_name,
                repo: repo,
                prefix: prefix,
                retention_ms: 60_000,
                batch_size: 10,
                heartbeat_ms: 50,
                retry_ms: 25,
                scan_ms: 100}
             )

    assert wait_until(fn ->
             execution_ids(repo, prefix) |> Enum.member?(archived_execution) |> Kernel.not()
           end)

    refute archived_execution in execution_ids(repo, prefix)
    refute archived_execution in delivery_execution_ids(repo, prefix)
    assert event_count(repo, prefix, archived_execution) == 0

    assert partially_archived_execution in execution_ids(repo, prefix)
    assert partially_archived_execution in delivery_execution_ids(repo, prefix)
    assert event_count(repo, prefix, partially_archived_execution) == 1

    assert recent_execution in execution_ids(repo, prefix)
    assert recent_execution in delivery_execution_ids(repo, prefix)
    assert event_count(repo, prefix, recent_execution) == 1
  end

  defp wait_until(fun, timeout_ms \\ 2_000, poll_ms \\ 25) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, poll_ms)
  end

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

  defp insert_terminal_execution!(repo, prefix, attrs) do
    execution_id = Ecto.UUID.generate()
    unique_id = Map.fetch!(attrs, :unique_id)
    completed_at = Map.fetch!(attrs, :completed_at)
    inserted_at = Map.fetch!(attrs, :inserted_at)
    updated_at = Map.fetch!(attrs, :updated_at)
    status = Map.get(attrs, :status, "completed")

    sql = """
    INSERT INTO #{prefix}.endurant_executions (
      id, unique_id, queue, workflow_name, version, input, status, next_event_sequence,
      history_size_bytes, completed_at, inserted_at, updated_at
    )
    VALUES (
      $1, $2, 'archive', 'ArchiverTest.Workflow', '1', '{}'::jsonb,
      $3::#{prefix}.endurant_execution_status, 1, 0, $4, $5, $6
    )
    """

    repo.query!(
      sql,
      [to_db_id(execution_id), unique_id, status, completed_at, inserted_at, updated_at],
      log: false
    )

    execution_id
  end

  defp insert_event!(repo, prefix, execution_id, sequence, type, payload, inserted_at) do
    sql = """
    INSERT INTO #{prefix}.endurant_events (execution_id, sequence, type, payload, inserted_at)
    VALUES ($1, $2, $3::#{prefix}.endurant_event_type, $4::jsonb, $5)
    """

    repo.query!(sql, [to_db_id(execution_id), sequence, type, payload, inserted_at], log: false)
    :ok
  end

  defp insert_delivery!(repo, prefix, backend, execution_id) do
    sql = """
    INSERT INTO #{prefix}.endurant_archive_deliveries (backend, execution_id)
    VALUES ($1, $2)
    """

    repo.query!(sql, [backend, to_db_id(execution_id)], log: false)
    :ok
  end

  defp execution_ids(repo, prefix) do
    sql = """
    SELECT id
    FROM #{prefix}.endurant_executions
    ORDER BY id ASC
    """

    repo.query!(sql, [], log: false).rows
    |> Enum.map(fn [execution_id] -> to_app_id(execution_id) end)
  end

  defp delivered_execution_ids(repo, prefix, archiver) do
    sql = """
    SELECT execution_id
    FROM #{prefix}.endurant_archive_deliveries
    WHERE backend = $1
    ORDER BY execution_id ASC
    """

    repo.query!(sql, [archiver], log: false).rows
    |> Enum.map(fn [execution_id] -> to_app_id(execution_id) end)
  end

  defp delivery_execution_ids(repo, prefix) do
    sql = """
    SELECT DISTINCT execution_id
    FROM #{prefix}.endurant_archive_deliveries
    ORDER BY execution_id ASC
    """

    repo.query!(sql, [], log: false).rows
    |> Enum.map(fn [execution_id] -> to_app_id(execution_id) end)
  end

  defp event_count(repo, prefix, execution_id) do
    sql = """
    SELECT COUNT(*)
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    """

    case repo.query!(sql, [to_db_id(execution_id)], log: false).rows do
      [[count]] -> count
      _ -> 0
    end
  end

  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  defp to_app_id(id) do
    case Ecto.UUID.load(id) do
      {:ok, loaded} -> loaded
      :error -> id
    end
  end
end
