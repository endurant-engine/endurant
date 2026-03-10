defmodule Endurant.EventsRetryTest do
  use ExUnit.Case, async: true

  defmodule NoRetryRepo do
    def transaction(fun, _opts), do: {:ok, fun.()}

    def query!(sql, params, opts) do
      send(self(), {:no_retry_repo_query, sql, params, opts})

      cond do
        String.contains?(sql, "SELECT next_event_sequence") ->
          %{rows: [[1]], num_rows: 1}

        String.contains?(sql, "INSERT INTO") ->
          Process.put(:insert_attempts, Process.get(:insert_attempts, 0) + 1)

          raise %Postgrex.Error{
            message: "duplicate key value violates unique constraint",
            postgres: %{
              code: :unique_violation,
              constraint: "endurant_events_execution_id_sequence_index",
              severity: "ERROR",
              pg_code: "23505"
            }
          }

        true ->
          %{num_rows: 1}
      end
    end
  end

  setup do
    on_exit(fn ->
      Process.delete(:insert_attempts)
    end)

    :ok
  end

  test "append does not retry unique violations from insert" do
    try do
      Endurant.Events.append(
        Ecto.UUID.generate(),
        :execution_created,
        %{},
        repo: NoRetryRepo,
        prefix: "public"
      )

      flunk("expected append/4 to raise Postgrex.Error")
    rescue
      error in Postgrex.Error ->
        assert error.postgres.code == :unique_violation
        assert error.postgres.constraint == "endurant_events_execution_id_sequence_index"
    end

    assert Process.get(:insert_attempts) == 1
  end
end
