defmodule Endurant.Archivers.ClickHouseTest do
  use ExUnit.Case, async: true

  alias Endurant.Archivers.ClickHouse

  defmodule FakeHTTP do
    def request(url, body, headers, opts) do
      send(self(), {:clickhouse_request, url, body, headers, opts})
      {:ok, 200, "ok"}
    end
  end

  test "init creates the database, raw archive tables, and derived analytics tables" do
    opts = [url: "http://localhost:8123", http_client: FakeHTTP]

    assert :ok = ClickHouse.init(1, opts)

    assert_received {:clickhouse_request,
                     "http://localhost:8123?database=endurant&date_time_input_format=best_effort",
                     create_db_sql, _, _}

    assert create_db_sql == "CREATE DATABASE IF NOT EXISTS endurant"

    assert_received {:clickhouse_request, _, executions_sql, _, _}
    assert executions_sql =~ "CREATE TABLE IF NOT EXISTS endurant.endurant_archived_executions"
    assert executions_sql =~ "ENGINE = ReplacingMergeTree(archived_at)"

    assert_received {:clickhouse_request, _, events_sql, _, _}
    assert events_sql =~ "CREATE TABLE IF NOT EXISTS endurant.endurant_archived_events"
    assert events_sql =~ "ORDER BY (execution_id, sequence)"

    assert_received {:clickhouse_request, _, alter_events_sql, _, _}
    assert alter_events_sql =~ "ALTER TABLE endurant.endurant_archived_events"
    assert alter_events_sql =~ "ADD COLUMN IF NOT EXISTS task_run_id Nullable(String)"

    assert_received {:clickhouse_request, _, execution_facts_sql, _, _}
    assert execution_facts_sql =~ "CREATE TABLE IF NOT EXISTS endurant.endurant_execution_facts"
    assert execution_facts_sql =~ "task_interrupted_count UInt64"

    assert_received {:clickhouse_request, _, task_runs_sql, _, _}
    assert task_runs_sql =~ "CREATE TABLE IF NOT EXISTS endurant.endurant_task_runs"
    assert task_runs_sql =~ "task_run_id String"
  end

  test "archive inserts raw and derived batches as JSONEachRow" do
    batch = [
      %{
        execution: %{
          id: "exec-1",
          unique_id: "uniq-1",
          queue: "orders",
          workflow: "MyWorkflow",
          status: :completed,
          version: "3",
          next_event_sequence: 4,
          history_size_bytes: 128,
          completed_at: ~U[2026-03-12 12:00:00Z],
          inserted_at: ~U[2026-03-12 11:00:00Z],
          updated_at: ~U[2026-03-12 12:00:00Z]
        },
        events: [
          %{
            execution_id: "exec-1",
            sequence: 1,
            type: :task_started,
            payload: %{task: "fetch_user", task_run_id: "run-1"},
            inserted_at: ~U[2026-03-12 11:30:00Z]
          },
          %{
            execution_id: "exec-1",
            sequence: 2,
            type: :task_completed,
            payload: %{task: "fetch_user", task_run_id: "run-1", result: %{ok: true}},
            inserted_at: ~U[2026-03-12 11:30:01Z]
          }
        ]
      },
      %{
        execution: %{
          id: "exec-2",
          unique_id: "uniq-2",
          queue: "emails",
          workflow: "OtherWorkflow",
          status: :failed,
          version: "4",
          next_event_sequence: 2,
          history_size_bytes: 32,
          completed_at: ~U[2026-03-12 12:05:00Z],
          inserted_at: ~U[2026-03-12 12:00:00Z],
          updated_at: ~U[2026-03-12 12:05:00Z]
        },
        events: [
          %{
            execution_id: "exec-2",
            sequence: 1,
            type: :task_started,
            payload: %{task: "send_email", task_run_id: "run-2"},
            inserted_at: ~U[2026-03-12 12:04:58Z]
          },
          %{
            execution_id: "exec-2",
            sequence: 2,
            type: :task_interrupted,
            payload: %{task: "send_email", task_run_id: "run-2"},
            inserted_at: ~U[2026-03-12 12:04:59Z]
          }
        ]
      }
    ]

    assert :ok =
             ClickHouse.archive(
               batch,
               1,
               url: "http://localhost:8123",
               http_client: FakeHTTP
             )

    assert_received {:clickhouse_request, _, execution_insert, _, _}

    assert execution_insert =~
             "INSERT INTO endurant.endurant_archived_executions FORMAT JSONEachRow"

    assert execution_insert =~ "\"execution_id\":\"exec-1\""
    assert execution_insert =~ "\"execution_id\":\"exec-2\""
    assert execution_insert =~ "\"status\":\"completed\""
    assert execution_insert =~ "\"status\":\"failed\""
    assert execution_insert =~ "\"endurant_migration_version\":1"
    assert execution_insert =~ "\"raw_json\":\"{"

    assert_received {:clickhouse_request, _, event_insert, _, _}
    assert event_insert =~ "INSERT INTO endurant.endurant_archived_events FORMAT JSONEachRow"
    assert event_insert =~ "\"execution_id\":\"exec-1\""
    assert event_insert =~ "\"execution_id\":\"exec-2\""
    assert event_insert =~ "\"event_type\":\"task_started\""
    assert event_insert =~ "\"task\":\"fetch_user\""
    assert event_insert =~ "\"event_type\":\"task_completed\""
    assert event_insert =~ "\"event_type\":\"task_interrupted\""
    assert event_insert =~ "\"task_run_id\":\"run-1\""
    assert event_insert =~ "\"task\":\"send_email\""

    assert_received {:clickhouse_request, _, execution_facts_insert, _, _}

    assert execution_facts_insert =~
             "INSERT INTO endurant.endurant_execution_facts FORMAT JSONEachRow"

    assert execution_facts_insert =~ "\"workflow_name\":\"MyWorkflow\""
    assert execution_facts_insert =~ "\"event_count\":2"
    assert execution_facts_insert =~ "\"task_interrupted_count\":0"
    assert execution_facts_insert =~ "\"workflow_name\":\"OtherWorkflow\""
    assert execution_facts_insert =~ "\"task_interrupted_count\":1"

    assert_received {:clickhouse_request, _, task_runs_insert, _, _}
    assert task_runs_insert =~ "INSERT INTO endurant.endurant_task_runs FORMAT JSONEachRow"
    assert task_runs_insert =~ "\"task_run_id\":\"run-1\""
    assert task_runs_insert =~ "\"status\":\"completed\""
    assert task_runs_insert =~ "\"task_run_id\":\"run-2\""
    assert task_runs_insert =~ "\"status\":\"interrupted\""
  end

  test "archive skips event and task-run inserts when there are no events" do
    assert :ok =
             ClickHouse.archive(
               [%{execution: %{id: "exec-1"}, events: []}],
               1,
               url: "http://localhost:8123",
               http_client: FakeHTTP
             )

    assert_received {:clickhouse_request, _, execution_insert, _, _}
    assert execution_insert =~ "endurant_archived_executions"

    assert_received {:clickhouse_request, _, execution_facts_insert, _, _}
    assert execution_facts_insert =~ "endurant_execution_facts"

    refute_received {:clickhouse_request, _, _, _, _}
  end
end
