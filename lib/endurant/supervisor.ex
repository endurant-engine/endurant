defmodule Endurant.Supervisor do
  @moduledoc false

  use Supervisor

  alias Endurant.Config
  alias Endurant.Registry

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    Registry.ensure_started()
    config = build_config!(opts)

    case Registry.supervisor_pid(config.name) do
      nil ->
        Supervisor.start_link(__MODULE__, config)

      pid ->
        {:error, {:already_started, pid}}
    end
  end

  @impl true
  def init(%Config{} = config) do
    case Registry.register_supervisor(config.name, self()) do
      :ok ->
        :ok

      {:error, {:already_started, pid}} ->
        raise "endurant instance #{inspect(config.name)} is already started at #{inspect(pid)}"
    end

    :ok = Registry.put_config(config)

    children = [
      %{
        id: :scheduler,
        start:
          {Endurant.Scheduler, :start_link,
           [[instance: config.name, repo: config.repo, prefix: config.prefix]]}
      }
      | Enum.map(config.queues, fn {queue, queue_opts} ->
          %{
            id: {:queue_manager, queue},
            start:
              {Endurant.QueueManager, :start_link,
               [[instance: config.name, queue: queue, opts: queue_opts]]}
          }
        end)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec supervisor_name(Config.instance_name()) :: pid() | nil
  def supervisor_name(instance) do
    supervisor_pid(instance)
  end

  @spec supervisor_pid(Config.instance_name()) :: pid() | nil
  def supervisor_pid(instance) do
    Registry.supervisor_pid(instance)
  end

  @spec queue_manager_name(Config.instance_name(), atom()) :: pid() | nil
  def queue_manager_name(instance, queue) when is_atom(queue) do
    queue_manager_pid(instance, queue)
  end

  @spec queue_manager_pid(Config.instance_name(), atom()) :: pid() | nil
  def queue_manager_pid(instance, queue) when is_atom(queue) do
    Registry.queue_manager_pid(instance, queue)
  end

  @spec build_config!(keyword()) :: Config.t()
  defp build_config!(opts) do
    case Keyword.fetch(opts, :config) do
      {:ok, %Config{} = config} ->
        config

      {:ok, other} ->
        raise ArgumentError, ":config must be an %Endurant.Config{}, got: #{inspect(other)}"

      :error ->
        Config.new!(opts)
    end
  end
end
