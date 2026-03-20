defmodule Endurant do
  @moduledoc """
  Public API for running Endurant and interacting with workflow executions.

  Endurant is instance-addressed. The default instance name is `Endurant`.
  Public operations can be called against the default instance, or with an
  explicit `:instance` option.

      Endurant.insert(MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"})
      Endurant.signal(execution_id, "approval_requested", %{approved: true})
      Endurant.execution(execution_id)
      Endurant.events(execution_id)

      Endurant.insert(MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"}, instance: :my_endurant)
      Endurant.signal(execution_id, "approval_requested", %{approved: true}, instance: :my_endurant)
      Endurant.execution(execution_id, instance: :my_endurant)
      Endurant.events(execution_id, instance: :my_endurant)
  """

  alias Ecto.Multi
  alias Endurant.Config
  alias Endurant.Crons
  alias Endurant.Registry

  @default_instance Endurant

  @typedoc "Name for the top-level Endurant instance."
  @type start_name :: atom() | String.t()

  @typedoc """
  Queue runtime options.

  Supported keys:
  - `:concurrency` maximum number of concurrently running executions for that queue.
  - `:cached_limit` maximum number of cached executions tracked in-memory.
  - `:poll_interval` queue polling interval in milliseconds.
  - `:lease_ms` execution lock lease in milliseconds.
  - `:recovery_limit` maximum number of expired locked executions recovered per poll.
  - `:heartbeat_interval` heartbeat interval used by long-running/cached executions.
  - `:repo` Ecto repo module used for persistence.
  - `:prefix` database schema prefix.
  """
  @type queue_options :: keyword()
  @type archiver_options ::
          [
            {:module, module()}
            | {:batch_size, pos_integer()}
            | {:scan_ms, pos_integer()}
            | {:heartbeat_ms, pos_integer()}
            | {:retry_ms, pos_integer()}
            | {:lease_ms, pos_integer()}
            | {atom(), term()}
          ]
  @type pruner_options ::
          [
            {:enabled, boolean()}
            | {:retention_ms, pos_integer()}
            | {:batch_size, pos_integer()}
            | {:scan_ms, pos_integer()}
            | {:heartbeat_ms, pos_integer()}
            | {:retry_ms, pos_integer()}
            | {:lease_ms, pos_integer()}
          ]

  @typedoc "Queue definitions passed to `start_link/1`."
  @type queues_option :: keyword(queue_options())
  @typedoc "Archiver definitions passed to `start_link/1`."
  @type archivers_option :: keyword(archiver_options())
  @typedoc "Pruner definition passed to `start_link/1`."
  @type pruner_option :: pruner_options()

  @type start_option ::
          {:name, start_name()}
          | {:repo, module()}
          | {:prefix, String.t()}
          | {:db_log, Config.db_log_option()}
          | {:queue_defaults, keyword()}
          | {:crons, [keyword() | map()]}
          | {:archivers, archivers_option()}
          | {:pruner, pruner_option()}
          | {:queues, queues_option()}
  @type start_options :: [start_option()]
  @typedoc "Name used to address a running Endurant instance."
  @type instance_name :: atom() | String.t()
  @type instance_option :: {:instance, instance_name()}
  @type schedule_options :: [{:id, binary()} | instance_option()]
  @type cron_options ::
          [
            {:id, binary()}
            | {:name, String.t()}
            | {:timezone, String.t()}
            | {:start_at, DateTime.t()}
            | {:end_at, DateTime.t()}
            | instance_option()
          ]
  @type insert_options :: [instance_option()]

  @type insert_result :: {:ok, map()} | {:error, :unique_conflict}
  @type insert_multi_input :: map() | (map() -> map())
  @type schedule_result :: {:ok, map()} | {:error, :id_conflict | :transient_db}
  @type scheduled_result :: [map()]
  @type cancel_scheduled_result :: :ok | {:error, :not_found | :not_pending}
  @type cron_result :: {:ok, map()} | {:error, term()}
  @type crons_result :: [map()]
  @type cron_fires_result :: [map()]
  @type pause_cron_result :: :ok | {:error, :not_found | :not_active | :transient_db}
  @type resume_cron_result :: :ok | {:error, :not_found | :not_paused | :ended | :transient_db}
  @type delete_cron_result :: :ok | {:error, :not_found | :transient_db}
  @type signal_result :: :ok | {:error, :not_found | :not_active}
  @type cancel_result :: :ok | {:error, :not_found | :not_active}
  @type execution_result :: map() | nil
  @type executions_result :: [map()]
  @typedoc "Workflow execution event returned by `events/1` and `events/2`."
  @type event :: %{
          id: integer(),
          execution_id: binary(),
          sequence: non_neg_integer(),
          type: atom(),
          payload: map(),
          inserted_at: NaiveDateTime.t() | DateTime.t() | nil
        }
  @type events_result :: [event()]

  @doc """
  Starts Endurant's supervision tree.

  ## Options

  - `:name` non-empty string or atom instance name (defaults to `Endurant`).
  - `:repo` Ecto repo module for this instance.
  - `:prefix` database schema prefix (default `"public"`).
  - `:db_log` DB query logging for Endurant-issued queries (default `false`).
  - `:queue_defaults` queue defaults merged into each queue config.
  - `:crons` config-managed cron schedules synced on startup.
  - `:archivers` archive worker definitions.
  - `:pruner` archive pruning worker definition.
  - `:queues` queue definitions and queue options.

  `:crons` entry options:

  - `:name` (required) unique cron name.
  - `:workflow` (required) workflow module.
  - `:expr` or `:cron_expr` (required) cron expression.
  - `:input` map payload (default `%{}`).
  - `:timezone` IANA timezone (default `"Etc/UTC"`).
  - `:start_at` UTC datetime lower bound (default `DateTime.utc_now/0`).
  - `:end_at` optional UTC datetime upper bound.
  - `:status` `:active | :paused` (default `:active`).

  Any option not passed directly can come from application config:

      config :endurant, :my_endurant,
        repo: MyApp.Repo,
        prefix: "public",
        queues: [default: [concurrency: 10]]

  `:archivers` entry options:

  - `:module` required user archiver implementation module.
  - `:enabled` runtime enabled state synced into `endurant_settings` (default `false`).
  - `:batch_size` terminal executions scanned per archive pass.
  - `:scan_ms` archive scan interval in milliseconds.
  - `:heartbeat_ms` lease heartbeat interval in milliseconds.
  - `:retry_ms` retry delay after acquisition/init failure in milliseconds.
  - `:lease_ms` worker lease duration in milliseconds.
  - any other JSON-compatible option is synced into the archiver settings row and passed
    through to the archiver callback `opts`.
  - any other non-JSON option is passed through to the archiver callback `opts` only.

  `:pruner` options:

  - `:enabled` enables the pruning worker (default `false`).
  - `:retention_ms` minimum age in milliseconds before archived terminal executions are pruned.
  - `:batch_size` terminal executions pruned per pass.
  - `:scan_ms` prune scan interval in milliseconds.
  - `:heartbeat_ms` lease heartbeat interval in milliseconds.
  - `:retry_ms` retry delay after acquisition failure in milliseconds.
  - `:lease_ms` worker lease duration in milliseconds.

  ## Examples

      Endurant.start_link(
        name: :my_endurant,
        repo: MyApp.Repo,
        prefix: "public",
        archivers: [
          clickhouse: [
            module: EndurantClickhouseArchiver,
            enabled: true,
            url: "http://localhost:8123",
            database: "endurant",
            batch_size: 1_000,
            scan_ms: 5_000
          ]
        ],
        pruner: [
          enabled: true,
          retention_ms: 86_400_000,
          batch_size: 500,
          scan_ms: 30_000
        ],
        queues: [
          default: [concurrency: 10, poll_interval: 200],
          emails: [concurrency: 5, poll_interval: 100]
        ]
      )
  """
  @spec start_link(start_options()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, @default_instance)

    name
    |> config_opts_from_env()
    |> Keyword.merge(opts)
    |> Keyword.put(:name, name)
    |> Endurant.Supervisor.start_link()
  end

  @doc """
  Inserts a workflow execution request targeting the default instance.
  """
  @spec insert(module(), map()) :: insert_result()
  def insert(workflow_module, input) when is_atom(workflow_module) and is_map(input) do
    Endurant.Executions.insert(workflow_module, input, instance_runtime_opts!(@default_instance))
  end

  @doc """
  Inserts a workflow execution request with options.

  Supported options:

  - `:instance` target Endurant instance. Defaults to `Endurant`.
  """
  @spec insert(module(), map(), insert_options()) :: insert_result()
  def insert(workflow_module, input, opts)
      when is_atom(workflow_module) and is_map(input) and is_list(opts) do
    runtime_opts = runtime_opts_from_public_opts!(opts)
    Endurant.Executions.insert(workflow_module, input, runtime_opts)
  end

  @doc """
  Inserts a workflow execution request as part of an `Ecto.Multi`.

  Supported options:

  - `:instance` target Endurant instance. Defaults to `Endurant`.

  The repo used for the insert comes from the surrounding multi transaction.
  The selected instance provides runtime configuration such as the prefix.
  """
  @spec insert(Multi.t(), Multi.name(), module(), insert_multi_input()) :: Multi.t()
  def insert(%Multi{} = multi, name, workflow_module, input_or_fun)
      when is_atom(workflow_module) and (is_map(input_or_fun) or is_function(input_or_fun, 1)) do
    insert(multi, name, workflow_module, input_or_fun, [])
  end

  @doc """
  Inserts a workflow execution request as part of an `Ecto.Multi`.

  Supported options:

  - `:instance` target Endurant instance. Defaults to `Endurant`.
  """
  @spec insert(Multi.t(), Multi.name(), module(), insert_multi_input(), insert_options()) ::
          Multi.t()
  def insert(%Multi{} = multi, name, workflow_module, input_or_fun, opts)
      when is_atom(workflow_module) and
             (is_map(input_or_fun) or is_function(input_or_fun, 1)) and
             is_list(opts) do
    runtime_opts = runtime_opts_from_public_opts!(opts)

    Multi.run(multi, name, fn repo, changes ->
      tx_opts = Keyword.put(runtime_opts, :repo, repo)
      input = resolve_insert_multi_input!(input_or_fun, changes)

      Endurant.Executions.insert_in_tx(workflow_module, input, tx_opts)
    end)
  end

  @doc """
  Schedules a workflow execution on the default instance.
  """
  @spec schedule(module(), map(), DateTime.t()) :: schedule_result()
  def schedule(workflow_module, input, scheduled_at)
      when is_atom(workflow_module) and is_map(input) and is_struct(scheduled_at, DateTime) do
    runtime_opts = instance_runtime_opts!(@default_instance)
    Endurant.Schedules.insert(workflow_module, input, scheduled_at, runtime_opts)
  end

  @doc """
  Schedules a workflow execution on the default instance with options.

  Supported options:

  - `:id` explicit schedule id (UUID string). Defaults to generated UUID.
  """
  @spec schedule(module(), map(), DateTime.t(), schedule_options()) :: schedule_result()
  def schedule(workflow_module, input, scheduled_at, opts)
      when is_atom(workflow_module) and is_map(input) and is_struct(scheduled_at, DateTime) and
             is_list(opts) do
    {runtime_opts, local_opts} = runtime_opts_and_local_opts!(opts)

    Endurant.Schedules.insert(
      workflow_module,
      input,
      scheduled_at,
      Keyword.merge(runtime_opts, local_opts)
    )
  end

  @doc """
  Lists scheduled rows from the default instance.

  Supported filters:

  - `:status` one of `:pending | :dispatched | :skipped | :failed | :cancelled`
  - `:cron_schedule_id` list only rows produced by one cron schedule id
  - `:limit` positive integer (default `100`)
  """
  @spec scheduled() :: scheduled_result()
  @spec scheduled(keyword()) :: scheduled_result()
  def scheduled(filters \\ [])

  def scheduled(filters) when is_list(filters) do
    {instance, resolved_filters} = split_instance_from_keyword(filters)
    Endurant.Schedules.list(resolved_filters, instance_runtime_opts!(instance))
  end

  @doc """
  Cancels one scheduled row on the default instance.

  A row can only be cancelled while `:pending`.
  """
  @spec cancel_scheduled(binary()) :: cancel_scheduled_result()
  def cancel_scheduled(schedule_id) when is_binary(schedule_id) do
    Endurant.Schedules.cancel(schedule_id, instance_runtime_opts!(@default_instance))
  end

  @spec cancel_scheduled(binary(), keyword()) :: cancel_scheduled_result()
  def cancel_scheduled(schedule_id, opts) when is_binary(schedule_id) and is_list(opts) do
    Endurant.Schedules.cancel(schedule_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Creates a cron schedule on the default instance.
  """
  @spec cron(module(), map(), String.t()) :: cron_result()
  @spec cron(module(), map(), String.t(), cron_options()) :: cron_result()
  def cron(workflow_module, input, cron_expr)
      when is_atom(workflow_module) and is_map(input) and is_binary(cron_expr) do
    cron(workflow_module, input, cron_expr, [])
  end

  @doc """
  Creates a cron schedule on the default instance with options.

  Supported options:

  - `:id` explicit cron id (UUID string), default generated UUID
  - `:name` optional unique name
  - `:timezone` IANA timezone (default `"Etc/UTC"`)
  - `:start_at` UTC datetime lower bound (default `DateTime.utc_now/0`)
  - `:end_at` optional UTC datetime upper bound
  """
  def cron(workflow_module, input, cron_expr, opts)
      when is_atom(workflow_module) and is_map(input) and is_binary(cron_expr) and
             is_list(opts) do
    {runtime_opts, local_opts} = runtime_opts_and_local_opts!(opts)
    Crons.insert(workflow_module, input, cron_expr, Keyword.merge(runtime_opts, local_opts))
  end

  @doc """
  Lists cron schedules from the default instance.

  Supported filters:

  - `:status` `:active | :paused`
  - `:limit` positive integer (default `100`)
  """
  @spec crons() :: crons_result()
  @spec crons(keyword()) :: crons_result()
  def crons(filters \\ [])

  def crons(filters) when is_list(filters) do
    {instance, resolved_filters} = split_instance_from_keyword(filters)
    Crons.list(resolved_filters, instance_runtime_opts!(instance))
  end

  @doc """
  Lists recent cron fires for one schedule.

  A fire is a scheduled row generated by cron dispatch.

  Supported filters:

  - `:limit` positive integer (default `100`)
  """
  @spec cron_fires(binary()) :: cron_fires_result()
  def cron_fires(cron_id) when is_binary(cron_id) do
    Crons.list_fires(cron_id, [], instance_runtime_opts!(@default_instance))
  end

  @spec cron_fires(binary(), keyword()) :: cron_fires_result()
  def cron_fires(cron_id, filters) when is_binary(cron_id) and is_list(filters) do
    {instance, resolved_filters} = split_instance_from_keyword(filters)
    Crons.list_fires(cron_id, resolved_filters, instance_runtime_opts!(instance))
  end

  @doc """
  Pauses one cron schedule on the default instance.
  """
  @spec pause_cron(binary()) :: pause_cron_result()
  def pause_cron(cron_id) when is_binary(cron_id) do
    Crons.pause(cron_id, instance_runtime_opts!(@default_instance))
  end

  @spec pause_cron(binary(), keyword()) :: pause_cron_result()
  def pause_cron(cron_id, opts) when is_binary(cron_id) and is_list(opts) do
    Crons.pause(cron_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Resumes one paused cron schedule on the default instance.
  """
  @spec resume_cron(binary()) :: resume_cron_result()
  def resume_cron(cron_id) when is_binary(cron_id) do
    Crons.resume(cron_id, instance_runtime_opts!(@default_instance))
  end

  @spec resume_cron(binary(), keyword()) :: resume_cron_result()
  def resume_cron(cron_id, opts) when is_binary(cron_id) and is_list(opts) do
    Crons.resume(cron_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Deletes one cron schedule on the default instance.
  """
  @spec delete_cron(binary()) :: delete_cron_result()
  def delete_cron(cron_id) when is_binary(cron_id) do
    Crons.delete(cron_id, instance_runtime_opts!(@default_instance))
  end

  @spec delete_cron(binary(), keyword()) :: delete_cron_result()
  def delete_cron(cron_id, opts) when is_binary(cron_id) and is_list(opts) do
    Crons.delete(cron_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Records a signal targeting an instance.
  """
  @spec signal(binary(), String.t()) :: signal_result()
  def signal(execution_id, signal) when is_binary(execution_id) and is_binary(signal) do
    Endurant.Executions.record_signal(
      execution_id,
      signal,
      %{},
      instance_runtime_opts!(@default_instance)
    )
  end

  @spec signal(binary(), String.t(), map()) :: signal_result()
  def signal(execution_id, signal, payload)
      when is_binary(execution_id) and is_binary(signal) and is_map(payload) do
    Endurant.Executions.record_signal(
      execution_id,
      signal,
      payload,
      instance_runtime_opts!(@default_instance)
    )
  end

  @spec signal(binary(), String.t(), keyword()) :: signal_result()
  def signal(execution_id, signal, opts)
      when is_binary(execution_id) and is_binary(signal) and is_list(opts) do
    Endurant.Executions.record_signal(
      execution_id,
      signal,
      %{},
      runtime_opts_from_public_opts!(opts)
    )
  end

  @spec signal(binary(), String.t(), map(), keyword()) :: signal_result()
  def signal(execution_id, signal, payload, opts)
      when is_binary(execution_id) and is_binary(signal) and is_map(payload) and is_list(opts) do
    Endurant.Executions.record_signal(
      execution_id,
      signal,
      payload,
      runtime_opts_from_public_opts!(opts)
    )
  end

  @doc """
  Requests cancellation of an execution targeting the default instance.
  """
  @spec cancel(binary()) :: cancel_result()
  def cancel(execution_id) when is_binary(execution_id) do
    Endurant.Executions.cancel(execution_id, instance_runtime_opts!(@default_instance))
  end

  @spec cancel(binary(), keyword()) :: cancel_result()
  def cancel(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Executions.cancel(execution_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Fetches one execution by id targeting the default instance.
  """
  @spec execution(binary()) :: execution_result()
  def execution(execution_id) when is_binary(execution_id) do
    Endurant.Executions.get(execution_id, instance_runtime_opts!(@default_instance))
  end

  @spec execution(binary(), keyword()) :: execution_result()
  def execution(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Executions.get(execution_id, runtime_opts_from_public_opts!(opts))
  end

  @doc """
  Lists executions from the default instance using optional filters.
  """
  @spec executions() :: executions_result()
  def executions do
    Endurant.Executions.list([], instance_runtime_opts!(@default_instance))
  end

  @spec executions(keyword()) :: executions_result()
  def executions(filters) when is_list(filters) do
    {instance, resolved_filters} = split_instance_from_keyword(filters)
    Endurant.Executions.list(resolved_filters, instance_runtime_opts!(instance))
  end

  @doc """
  Lists all events for an execution targeting the default instance.
  """
  @spec events(binary()) :: events_result()
  def events(execution_id) when is_binary(execution_id) do
    Endurant.Events.list(execution_id, instance_runtime_opts!(@default_instance))
  end

  @spec events(binary(), keyword()) :: events_result()
  def events(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Events.list(execution_id, runtime_opts_from_public_opts!(opts))
  end

  @spec split_instance_from_keyword(keyword()) :: {instance_name(), keyword()}
  defp split_instance_from_keyword(opts) when is_list(opts) do
    {Keyword.get(opts, :instance, @default_instance), Keyword.delete(opts, :instance)}
  end

  @spec runtime_opts_from_public_opts!(keyword()) :: keyword()
  defp runtime_opts_from_public_opts!(opts) when is_list(opts) do
    opts
    |> split_instance_from_keyword()
    |> elem(0)
    |> instance_runtime_opts!()
  end

  @spec runtime_opts_and_local_opts!(keyword()) :: {keyword(), keyword()}
  defp runtime_opts_and_local_opts!(opts) when is_list(opts) do
    {instance, local_opts} = split_instance_from_keyword(opts)
    {instance_runtime_opts!(instance), local_opts}
  end

  @spec instance_runtime_opts!(instance_name()) :: keyword()
  defp instance_runtime_opts!(instance) do
    case Registry.fetch_config(instance) do
      {:ok, %Config{} = config} ->
        Config.runtime_opts(config)

      :error ->
        raise ArgumentError,
              "endurant instance #{inspect(instance)} is not running on node #{inspect(node())}"
    end
  end

  @spec resolve_insert_multi_input!(insert_multi_input(), map()) :: map()
  defp resolve_insert_multi_input!(input, _changes) when is_map(input), do: input

  defp resolve_insert_multi_input!(input_fun, changes) when is_function(input_fun, 1),
    do: input_fun.(changes)

  @spec config_opts_from_env(instance_name()) :: keyword()
  defp config_opts_from_env(instance) when is_binary(instance), do: []

  defp config_opts_from_env(instance) do
    case Application.get_env(:endurant, instance, []) do
      env_opts when is_list(env_opts) ->
        if Keyword.keyword?(env_opts) do
          env_opts
        else
          raise ArgumentError,
                "application config for #{inspect(instance)} must be a keyword list, got: #{inspect(env_opts)}"
        end

      other ->
        raise ArgumentError,
              "application config for #{inspect(instance)} must be a keyword list, got: #{inspect(other)}"
    end
  end
end
