defmodule Endurant.ConfigTest do
  use ExUnit.Case, async: true

  defmodule Repo do
  end

  test "db_log defaults to false and is propagated to runtime and queue opts" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 2, cached_limit: 2]]
      )

    assert config.db_log == false
    assert Endurant.Config.runtime_opts(config)[:db_log] == false
    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :db_log) == false
  end

  test "cached_ttl_ms defaults to :infinity in queue opts" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 2, cached_limit: 2]]
      )

    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :cached_ttl_ms) ==
             :infinity
  end

  test "cached_ttl_ms accepts positive integers" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 1, cached_limit: 1, cached_ttl_ms: 5_000]]
      )

    assert Keyword.fetch!(Keyword.fetch!(config.queues, :default), :cached_ttl_ms) == 5_000
  end

  test "cached_ttl_ms rejects invalid values" do
    assert_raise ArgumentError, ~r/:cached_ttl_ms/, fn ->
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 1, cached_limit: 1, cached_ttl_ms: 0]]
      )
    end
  end

  test "db_log true normalizes to :debug" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        db_log: true,
        queues: [default: [concurrency: 1, cached_limit: 1]]
      )

    assert config.db_log == :debug
    assert Endurant.Config.runtime_opts(config)[:db_log] == :debug
  end

  test "db_log accepts explicit logger levels" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        db_log: :info,
        queues: [default: [concurrency: 1, cached_limit: 1]]
      )

    assert config.db_log == :info
  end

  test "db_log rejects invalid values" do
    assert_raise ArgumentError, ~r/:db_log/, fn ->
      Endurant.Config.new!(
        repo: Repo,
        db_log: :verbose,
        queues: [default: [concurrency: 1, cached_limit: 1]]
      )
    end
  end

  test "queues are required" do
    assert_raise ArgumentError, ~r/:queues is required/, fn ->
      Endurant.Config.new!(repo: Repo)
    end
  end

  test "each queue must declare concurrency explicitly" do
    assert_raise ArgumentError, ~r/must declare :concurrency explicitly/, fn ->
      Endurant.Config.new!(repo: Repo, queues: [default: [cached_limit: 1, poll_interval: 100]])
    end
  end

  test "queue concurrency must be a positive integer" do
    assert_raise ArgumentError, ~r/:concurrency must be a positive integer/, fn ->
      Endurant.Config.new!(repo: Repo, queues: [default: [concurrency: 0, cached_limit: 1]])
    end
  end

  test "each queue must declare cached_limit explicitly" do
    assert_raise ArgumentError, ~r/must declare :cached_limit explicitly/, fn ->
      Endurant.Config.new!(repo: Repo, queues: [default: [concurrency: 1, poll_interval: 100]])
    end
  end

  test "queue cached_limit must be a non-negative integer" do
    assert_raise ArgumentError, ~r/:cached_limit must be a non-negative integer/, fn ->
      Endurant.Config.new!(repo: Repo, queues: [default: [concurrency: 1, cached_limit: -1]])
    end
  end

  test "queue_defaults cannot define concurrency" do
    assert_raise ArgumentError, ~r/:queue_defaults cannot set :concurrency/, fn ->
      Endurant.Config.new!(
        repo: Repo,
        queue_defaults: [concurrency: 2, poll_interval: 100],
        queues: [default: [concurrency: 1, cached_limit: 1]]
      )
    end
  end

  test "queue_defaults cannot define cached_limit" do
    assert_raise ArgumentError, ~r/:queue_defaults cannot set :cached_limit/, fn ->
      Endurant.Config.new!(
        repo: Repo,
        queue_defaults: [cached_limit: 2, poll_interval: 100],
        queues: [default: [concurrency: 1, cached_limit: 1]]
      )
    end
  end

  test "pruner requires retention_ms when enabled" do
    assert_raise ArgumentError, ~r/:pruner :retention_ms is required/, fn ->
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 1, cached_limit: 1]],
        pruner: [enabled: true]
      )
    end
  end

  test "pruner may omit retention_ms when disabled" do
    config =
      Endurant.Config.new!(
        repo: Repo,
        queues: [default: [concurrency: 1, cached_limit: 1]],
        pruner: [enabled: false]
      )

    assert config.pruner == [enabled: false]
  end
end
