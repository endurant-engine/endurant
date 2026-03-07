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
end
