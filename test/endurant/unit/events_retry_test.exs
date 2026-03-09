defmodule Endurant.EventsRetryTest do
  use ExUnit.Case, async: true

  defmodule RetryRepo do
    def transaction(fun, _opts), do: {:ok, fun.()}

    def query!(sql, params, opts) do
      send(self(), {:retry_repo_query, sql, params, opts})

      cond do
        String.starts_with?(sql, "SELECT id") ->
          %{num_rows: 1, rows: [[params |> hd()]]}

        String.starts_with?(sql, "INSERT INTO") and Process.get(:retry_repo_calls, 0) == 0 ->
          Process.put(:retry_repo_calls, 1)

          raise %Postgrex.Error{
            message: "duplicate key value violates unique constraint",
            postgres: %{
              code: :unique_violation,
              constraint: "endurant_events_execution_id_sequence_index",
              severity: "ERROR",
              pg_code: "23505"
            }
          }

        String.starts_with?(sql, "INSERT INTO") ->
          %{num_rows: 1}

        true ->
          %{num_rows: 1}
      end
    end
  end

  defmodule WrongConstraintRepo do
    def transaction(fun, _opts), do: {:ok, fun.()}

    def query!(sql, params, _opts) do
      if String.starts_with?(sql, "SELECT id") do
        %{num_rows: 1, rows: [[params |> hd()]]}
      else
        raise %Postgrex.Error{
          message: "duplicate key value violates unique constraint",
          postgres: %{
            code: :unique_violation,
            constraint: "some_other_unique_constraint",
            severity: "ERROR",
            pg_code: "23505"
          }
        }
      end
    end
  end

  setup do
    on_exit(fn ->
      Process.delete(:retry_repo_calls)
    end)

    :ok
  end

  test "append retries when unique violation matches sequence constraint" do
    assert :ok =
             Endurant.Events.append(
               Ecto.UUID.generate(),
               :execution_created,
               %{},
               repo: RetryRepo,
               prefix: "public"
             )

    assert Process.get(:retry_repo_calls) == 1
  end

  test "append does not retry for other unique constraints" do
    try do
      Endurant.Events.append(
        Ecto.UUID.generate(),
        :execution_created,
        %{},
        repo: WrongConstraintRepo,
        prefix: "public"
      )

      flunk("expected append/4 to raise Postgrex.Error")
    rescue
      error in Postgrex.Error ->
        assert error.postgres.code == :unique_violation
        assert error.postgres.constraint == "some_other_unique_constraint"
    end
  end
end
