defmodule Endurant.EventsTest do
  use ExUnit.Case, async: true

  defmodule UnknownTypeRepo do
    def query!(_sql, [execution_id], _opts) do
      %{
        rows: [
          [1, execution_id, 1, "definitely_unknown_type", %{}, ~N[2026-01-01 00:00:00]]
        ]
      }
    end

    def query!(_sql, [execution_id, _sequence], _opts) do
      %{
        rows: [
          [1, execution_id, 2, "definitely_unknown_type", %{}, ~N[2026-01-01 00:00:00]]
        ]
      }
    end
  end

  defmodule PrefixCaptureRepo do
    def query!(sql, _params, _opts) do
      send(self(), {:captured_sql, sql})
      %{rows: []}
    end
  end

  defmodule HistoryLengthRepo do
    def query!(sql, _params, _opts) do
      send(self(), {:captured_sql, sql})
      %{rows: [[6]]}
    end
  end

  defmodule HistorySizeRepo do
    def query!(sql, _params, _opts) do
      send(self(), {:captured_sql, sql})
      %{rows: [[1024]]}
    end
  end

  test "list/2 fails fast for unknown event types" do
    execution_id = Ecto.UUID.generate()

    assert_raise CaseClauseError, fn ->
      Endurant.Events.list(execution_id, repo: UnknownTypeRepo, prefix: "public")
    end
  end

  test "list_after/3 fails fast for unknown event types" do
    execution_id = Ecto.UUID.generate()

    assert_raise CaseClauseError, fn ->
      Endurant.Events.list_after(execution_id, 1, repo: UnknownTypeRepo, prefix: "public")
    end
  end

  test "list/2 uses configured prefix in SQL" do
    execution_id = Ecto.UUID.generate()
    _ = Endurant.Events.list(execution_id, repo: PrefixCaptureRepo, prefix: "custom_schema")

    assert_received {:captured_sql, sql}
    assert sql =~ "FROM custom_schema.endurant_events"
  end

  test "history_length/2 returns counter from execution row" do
    execution_id = Ecto.UUID.generate()

    assert 5 ==
             Endurant.Events.history_length(
               execution_id,
               repo: HistoryLengthRepo,
               prefix: "custom_schema"
             )

    assert_received {:captured_sql, sql}
    assert sql =~ "SELECT next_event_sequence"
    assert sql =~ "FROM custom_schema.endurant_executions"
  end

  test "history_size/2 returns counter from execution row" do
    execution_id = Ecto.UUID.generate()

    assert 1024 ==
             Endurant.Events.history_size(
               execution_id,
               repo: HistorySizeRepo,
               prefix: "custom_schema"
             )

    assert_received {:captured_sql, sql}
    assert sql =~ "SELECT history_size_bytes"
    assert sql =~ "FROM custom_schema.endurant_executions"
  end
end
