defmodule Endurant.ExecutionsTest do
  use ExUnit.Case, async: true

  defmodule BrokenRepo do
    def query!(_sql, _params, _opts) do
      raise DBConnection.ConnectionError, "db unavailable"
    end
  end

  test "heartbeat/4 returns transient_db on connection error" do
    assert {:error, :transient_db} =
             Endurant.Executions.heartbeat(
               Ecto.UUID.generate(),
               "worker-1",
               30_000,
               repo: BrokenRepo,
               prefix: "public"
             )
  end
end
