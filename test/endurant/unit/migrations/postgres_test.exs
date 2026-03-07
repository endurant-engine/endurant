defmodule Endurant.Migrations.PostgresTest do
  use ExUnit.Case, async: true

  alias Endurant.Migrations.Postgres

  defmodule FakeRepo do
    def query(sql, params, opts) do
      send(self(), {:repo_query, sql, params, opts})
      Process.get(:repo_result, {:ok, %{rows: []}})
    end
  end

  setup do
    on_exit(fn ->
      Process.delete(:repo_result)
    end)

    :ok
  end

  test "migrated_version parses integer table comment" do
    Process.put(:repo_result, {:ok, %{rows: [["7"]]}})

    assert 7 == Postgres.migrated_version(repo: FakeRepo, prefix: "public")
  end

  test "migrated_version returns 0 for missing comment" do
    Process.put(:repo_result, {:ok, %{rows: [[nil]]}})

    assert 0 == Postgres.migrated_version(repo: FakeRepo, prefix: "public")
  end

  test "migrated_version returns 0 for malformed comment" do
    Process.put(:repo_result, {:ok, %{rows: [["not-an-int"]]}})

    assert 0 == Postgres.migrated_version(repo: FakeRepo, prefix: "public")
  end

  test "migrated_version uses parameterized prefix in lookup query" do
    Process.put(:repo_result, {:ok, %{rows: [["1"]]}})

    assert 1 == Postgres.migrated_version(repo: FakeRepo, prefix: "test")

    assert_received {:repo_query, query, ["test"], [log: false]}
    assert query =~ "n.nspname = $1"
  end

  test "migrated_version raises for invalid prefix" do
    assert_raise ArgumentError, ~r/invalid migration prefix/, fn ->
      Postgres.migrated_version(repo: FakeRepo, prefix: "te'st")
    end
  end

  test "up is a no-op when already at target version" do
    Process.put(:repo_result, {:ok, %{rows: [["1"]]}})

    assert :ok == Postgres.up(version: 1, repo: FakeRepo)
  end

  test "down raises for version above current_version" do
    Process.put(:repo_result, {:ok, %{rows: [["1"]]}})

    assert_raise ArgumentError, ~r/invalid migration version 2/, fn ->
      Postgres.down(version: 2, repo: FakeRepo)
    end
  end
end
