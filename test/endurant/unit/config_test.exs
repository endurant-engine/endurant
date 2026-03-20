defmodule Endurant.ConfigTest do
  use ExUnit.Case, async: true

  defmodule Repo do
  end

  test "db_log defaults to false and is propagated to runtime and queue opts" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 2]]
      )

    assert config.db_log == false
    assert Endurant.Config.runtime_opts(config)[:db_log] == false
    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :db_log) == false
  end

  test "cached_ttl_ms defaults to :infinity in queue opts" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 2]]
      )

    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :cached_ttl_ms) ==
             :infinity
  end

  test "cached_ttl_ms accepts positive integers" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [cached_ttl_ms: 5_000]]
      )

    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :cached_ttl_ms) == 5_000
  end

  test "cached_ttl_ms rejects invalid values" do
    assert_raise ArgumentError, ~r/:cached_ttl_ms/, fn ->
      Endurant.Config.new!(repo: Repo, queues: [default: [cached_ttl_ms: 0]])
    end
  end

  test "db_log true normalizes to :debug" do
    config = Endurant.Config.new!(repo: Repo, db_log: true)

    assert config.db_log == :debug
    assert Endurant.Config.runtime_opts(config)[:db_log] == :debug
  end

  test "db_log accepts explicit logger levels" do
    config = Endurant.Config.new!(repo: Repo, db_log: :info)

    assert config.db_log == :info
  end

  test "db_log rejects invalid values" do
    assert_raise ArgumentError, ~r/:db_log/, fn ->
      Endurant.Config.new!(repo: Repo, db_log: :verbose)
    end
  end
end
