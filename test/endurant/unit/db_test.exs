defmodule Endurant.DBTest do
  use ExUnit.Case, async: true

  test "merge_log_opt defaults Endurant queries to log false" do
    assert Endurant.DB.merge_log_opt([])[:log] == false
  end

  test "merge_log_opt normalizes db_log and preserves explicit query overrides" do
    assert Endurant.DB.merge_log_opt(db_log: true)[:log] == :debug
    assert Endurant.DB.merge_log_opt([db_log: :info], log: :warning)[:log] == :warning
  end

  test "merge_log_opt drops Endurant runtime keys before calling Ecto" do
    opts =
      Endurant.DB.merge_log_opt(
        [
          db_log: :info,
          repo: __MODULE__,
          prefix: "public",
          instance: :endurant,
          queue: :default
        ],
        timeout: 5_000
      )

    assert Keyword.get(opts, :timeout) == 5_000
    assert Keyword.get(opts, :log) == :info
    refute Keyword.has_key?(opts, :repo)
    refute Keyword.has_key?(opts, :prefix)
    refute Keyword.has_key?(opts, :instance)
    refute Keyword.has_key?(opts, :queue)
  end
end
