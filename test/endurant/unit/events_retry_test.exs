defmodule Endurant.EventsRetryTest do
  use ExUnit.Case, async: true

  defmodule RetryRepo do
    def query!(sql, params, opts) do
      send(self(), {:retry_repo_query, sql, params, opts})

      case Process.get(:retry_repo_calls, 0) do
        0 ->
          Process.put(:retry_repo_calls, 1)

          raise %Postgrex.Error{
            postgres: %{
              code: :unique_violation,
              constraint: "endurant_events_execution_id_sequence_index"
            }
          }

        _ ->
          %{num_rows: 1}
      end
    end
  end

  defmodule WrongConstraintRepo do
    def query!(_sql, _params, _opts) do
      raise %Postgrex.Error{
        postgres: %{
          code: :unique_violation,
          constraint: "some_other_unique_constraint"
        }
      }
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
