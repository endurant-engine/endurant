defmodule Endurant.Registry do
  @moduledoc false

  @table :endurant_local_registry

  alias Endurant.Config

  @type instance_name :: Config.instance_name()

  @spec ensure_started() :: :ok
  def ensure_started do
    case :ets.whereis(@table) do
      :undefined ->
        _ =
          :ets.new(@table, [
            :named_table,
            :public,
            :set,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])

        :ok

      _tid ->
        :ok
    end
  rescue
    ArgumentError ->
      # Racy concurrent startup can attempt to create table twice.
      :ok
  end

  @spec put_config(Config.t()) :: :ok
  def put_config(%Config{name: instance} = config) do
    ensure_started()
    true = :ets.insert(@table, {{:config, instance}, config})
    :ok
  end

  @spec fetch_config(instance_name()) :: {:ok, Config.t()} | :error
  def fetch_config(instance) do
    ensure_started()

    case :ets.lookup(@table, {:config, instance}) do
      [{{:config, ^instance}, %Config{} = config}] -> {:ok, config}
      _ -> :error
    end
  end

  @spec register_supervisor(instance_name(), pid()) :: :ok | {:error, {:already_started, pid()}}
  def register_supervisor(instance, pid) when is_pid(pid) do
    ensure_started()
    key = {:supervisor, instance}

    case :ets.insert_new(@table, {key, pid}) do
      true ->
        :ok

      false ->
        case supervisor_pid(instance) do
          nil ->
            case :ets.insert_new(@table, {key, pid}) do
              true -> :ok
              false -> {:error, {:already_started, supervisor_pid(instance)}}
            end

          existing ->
            {:error, {:already_started, existing}}
        end
    end
  end

  @spec unregister_supervisor(instance_name(), pid()) :: :ok
  def unregister_supervisor(instance, pid) when is_pid(pid) do
    ensure_started()
    key = {:supervisor, instance}

    case :ets.lookup(@table, key) do
      [{^key, ^pid}] -> :ets.delete(@table, key)
      _ -> :ok
    end

    :ok
  end

  @spec supervisor_pid(instance_name()) :: pid() | nil
  def supervisor_pid(instance) do
    fetch_alive_pid({:supervisor, instance})
  end

  @spec put_queue_manager(instance_name(), atom(), pid()) :: :ok
  def put_queue_manager(instance, queue, pid) when is_atom(queue) and is_pid(pid) do
    ensure_started()
    true = :ets.insert(@table, {{:queue_manager, instance, queue}, pid})
    :ok
  end

  @spec delete_queue_manager(instance_name(), atom(), pid() | nil) :: :ok
  def delete_queue_manager(instance, queue, pid \\ nil) when is_atom(queue) do
    ensure_started()
    key = {:queue_manager, instance, queue}

    case {pid, :ets.lookup(@table, key)} do
      {nil, _} ->
        :ets.delete(@table, key)

      {expected_pid, [{^key, current_pid}]}
      when is_pid(expected_pid) and current_pid == expected_pid ->
        :ets.delete(@table, key)

      _ ->
        :ok
    end

    :ok
  end

  @spec queue_manager_pid(instance_name(), atom()) :: pid() | nil
  def queue_manager_pid(instance, queue) when is_atom(queue) do
    fetch_alive_pid({:queue_manager, instance, queue})
  end

  @spec clear_instance(instance_name()) :: :ok
  def clear_instance(instance) do
    ensure_started()
    :ets.match_delete(@table, {{:queue_manager, instance, :_}, :_})
    :ets.delete(@table, {:supervisor, instance})
    :ets.delete(@table, {:config, instance})
    :ok
  end

  @spec fetch_alive_pid(tuple()) :: pid() | nil
  defp fetch_alive_pid(key) do
    ensure_started()

    case :ets.lookup(@table, key) do
      [{^key, pid}] when is_pid(pid) ->
        if Process.alive?(pid) do
          pid
        else
          :ets.delete(@table, key)
          nil
        end

      _ ->
        nil
    end
  end
end
