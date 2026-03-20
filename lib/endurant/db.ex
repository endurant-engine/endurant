defmodule Endurant.DB do
  @moduledoc false

  @type db_log_option :: false | :debug | :info | :notice | :warning | :error | :critical
  @ecto_query_opt_keys [
    :cache_statement,
    :caller,
    :log,
    :pool_timeout,
    :query_type,
    :telemetry_event,
    :telemetry_options,
    :timeout
  ]
  @ecto_transaction_opt_keys [
    :caller,
    :isolation,
    :log,
    :mode,
    :pool_timeout,
    :telemetry_event,
    :telemetry_options,
    :timeout
  ]

  @spec query!(module(), iodata(), list(), keyword()) :: term()
  def query!(repo, sql, params, opts \\ []) when is_list(params) and is_list(opts) do
    repo.query!(sql, params, merge_log_opt(opts))
  end

  @spec query(module(), iodata(), list(), keyword()) :: term()
  def query(repo, sql, params, opts \\ []) when is_list(params) and is_list(opts) do
    repo.query(sql, params, merge_log_opt(opts))
  end

  @spec transaction(module(), (-> term()), keyword(), keyword()) :: term()
  def transaction(repo, fun, runtime_opts, tx_opts \\ [])
      when is_function(fun, 0) and is_list(runtime_opts) and is_list(tx_opts) do
    repo.transaction(fun, merge_log_opt(runtime_opts, tx_opts, :transaction))
  end

  @spec db_log(keyword()) :: db_log_option()
  def db_log(opts) when is_list(opts) do
    case Keyword.get(opts, :db_log, false) do
      false -> false
      true -> :debug
      level when level in [:debug, :info, :notice, :warning, :error, :critical] -> level
      _ -> false
    end
  end

  @spec merge_log_opt(keyword(), keyword(), :query | :transaction) :: keyword()
  def merge_log_opt(runtime_opts, query_opts \\ [], kind \\ :query)
      when is_list(runtime_opts) and is_list(query_opts) do
    query_opts
    |> ecto_opts(kind)
    |> Keyword.put_new(:log, db_log(runtime_opts))
  end

  @spec ecto_opts(keyword(), :query | :transaction) :: keyword()
  defp ecto_opts(opts, :query) do
    Keyword.take(opts, @ecto_query_opt_keys)
  end

  defp ecto_opts(opts, :transaction) do
    Keyword.take(opts, @ecto_transaction_opt_keys)
  end
end
