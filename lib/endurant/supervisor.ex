defmodule Endurant.Supervisor do
  @moduledoc false

  use Supervisor

  alias Endurant.ArchiveWorker
  alias Endurant.Archivers
  alias Endurant.Config
  alias Endurant.Crons
  alias Endurant.Pruner
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

    sync_config_crons!(config)
    sync_config_archivers!(config)
    :ok = Registry.put_config(config)

    children =
      [
        %{
          id: :scheduler,
          start:
            {Endurant.Scheduler, :start_link,
             [[instance: config.name, repo: config.repo, prefix: config.prefix]]}
        }
      ]
      |> Kernel.++(
        Enum.map(config.archivers, fn {archiver, archiver_opts} ->
          %{
            id: {:archiver, archiver},
            start:
              {ArchiveWorker, :start_link, [archive_worker_opts(config, archiver, archiver_opts)]}
          }
        end)
      )
      |> maybe_append_pruner(config)
      |> Kernel.++(
        Enum.map(config.queues, fn {queue, queue_opts} ->
          %{
            id: {:queue_manager, queue},
            start:
              {Endurant.QueueManager, :start_link,
               [[instance: config.name, queue: queue, opts: queue_opts]]}
          }
        end)
      )

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec sync_config_crons!(Config.t()) :: :ok
  defp sync_config_crons!(%Config{} = config) do
    case Crons.sync_from_config(config.crons, Config.runtime_opts(config)) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "failed to sync config crons: #{inspect(reason)}"
    end
  end

  @spec sync_config_archivers!(Config.t()) :: :ok
  defp sync_config_archivers!(%Config{} = config) do
    case Archivers.sync_from_config(config.archivers, Config.runtime_opts(config)) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "failed to sync config archivers: #{inspect(reason)}"
    end
  end

  @spec archive_worker_opts(Config.t(), String.t(), keyword()) :: keyword()
  defp archive_worker_opts(%Config{} = config, archiver, archiver_opts) do
    worker_keys = [:module, :batch_size, :scan_ms, :heartbeat_ms, :retry_ms, :lease_ms]
    module = Keyword.fetch!(archiver_opts, :module)

    [
      instance: config.name,
      archiver: archiver,
      archiver_module: module,
      archiver_opts: Keyword.drop(archiver_opts, worker_keys),
      db_log: config.db_log,
      repo: config.repo,
      prefix: config.prefix
    ]
    |> maybe_put_opt(:batch_size, Keyword.get(archiver_opts, :batch_size))
    |> maybe_put_opt(:scan_ms, Keyword.get(archiver_opts, :scan_ms))
    |> maybe_put_opt(:heartbeat_ms, Keyword.get(archiver_opts, :heartbeat_ms))
    |> maybe_put_opt(:retry_ms, Keyword.get(archiver_opts, :retry_ms))
    |> maybe_put_opt(:lease_ms, Keyword.get(archiver_opts, :lease_ms))
  end

  @spec maybe_append_pruner([map()], Config.t()) :: [map()]
  defp maybe_append_pruner(children, %Config{pruner: pruner_opts} = config) do
    if Keyword.get(pruner_opts, :enabled, false) do
      children ++
        [
          %{
            id: :pruner,
            start: {Pruner, :start_link, [pruner_opts(config)]}
          }
        ]
    else
      children
    end
  end

  @spec pruner_opts(Config.t()) :: keyword()
  defp pruner_opts(%Config{} = config) do
    [
      instance: config.name,
      db_log: config.db_log,
      repo: config.repo,
      prefix: config.prefix
    ]
    |> maybe_put_opt(:batch_size, Keyword.get(config.pruner, :batch_size))
    |> maybe_put_opt(:scan_ms, Keyword.get(config.pruner, :scan_ms))
    |> maybe_put_opt(:heartbeat_ms, Keyword.get(config.pruner, :heartbeat_ms))
    |> maybe_put_opt(:retry_ms, Keyword.get(config.pruner, :retry_ms))
    |> maybe_put_opt(:lease_ms, Keyword.get(config.pruner, :lease_ms))
    |> maybe_put_opt(:retention_ms, Keyword.get(config.pruner, :retention_ms))
  end

  @spec maybe_put_opt(keyword(), atom(), term()) :: keyword()
  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

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
