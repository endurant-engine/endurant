defmodule Endurant.Supervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = require_name!(opts)
    validate_name!(name)
    opts = Keyword.put(opts, :name, name)

    Supervisor.start_link(__MODULE__, opts, name: supervisor_name(name))
  end

  @impl true
  def init(opts) do
    queues = Keyword.get(opts, :queues, default: [limit: 1])
    validate_unique_queues!(queues)
    instance = Keyword.fetch!(opts, :name)

    children =
      Enum.map(queues, fn {queue, queue_opts} ->
        %{
          id: {:queue_manager, queue},
          start:
            {Endurant.QueueManager, :start_link,
             [[name: queue_manager_name(instance, queue), queue: queue, opts: queue_opts]]}
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec supervisor_name(String.t()) :: {:global, {:endurant_supervisor, String.t()}}
  def supervisor_name(instance) when is_binary(instance) do
    {:global, {:endurant_supervisor, instance}}
  end

  @spec queue_manager_name(String.t(), atom()) ::
          {:global, {:endurant_queue_manager, String.t(), atom()}}
  def queue_manager_name(instance, queue) when is_binary(instance) and is_atom(queue) do
    {:global, {:endurant_queue_manager, instance, queue}}
  end

  @spec validate_unique_queues!(keyword()) :: :ok
  defp validate_unique_queues!(queues) do
    duplicates =
      queues
      |> Keyword.keys()
      |> Enum.group_by(& &1)
      |> Enum.filter(fn {_queue, entries} -> length(entries) > 1 end)
      |> Enum.map(&elem(&1, 0))

    case duplicates do
      [] ->
        :ok

      _ ->
        raise ArgumentError, "duplicate queues are not allowed: #{inspect(duplicates)}"
    end
  end

  @spec validate_name!(term()) :: :ok
  defp validate_name!(name) when is_binary(name) do
    if String.trim(name) != "" do
      :ok
    else
      raise ArgumentError, ":name must be a non-empty string, got: #{inspect(name)}"
    end
  end

  defp validate_name!(name) do
    raise ArgumentError, ":name must be a non-empty string, got: #{inspect(name)}"
  end

  @spec require_name!(keyword()) :: String.t()
  defp require_name!(opts) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} ->
        name

      :error ->
        raise ArgumentError, "missing required :name option (non-empty string)"
    end
  end
end
