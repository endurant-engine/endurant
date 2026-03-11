defmodule Endurant do
  @moduledoc """
  Public API for running Endurant and interacting with workflow executions.

  Endurant is instance-addressed. The default instance name is `Endurant`.
  Public operations can be called against the default instance, or with an
  explicit instance as the first argument.

      Endurant.insert(MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"})
      Endurant.signal(execution_id, "approval_requested", %{approved: true})
      Endurant.execution(execution_id)
      Endurant.events(execution_id)

      Endurant.insert(MyEndurant, MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"})
      Endurant.signal(MyEndurant, execution_id, "approval_requested", %{approved: true})
  """

  alias Endurant.Config
  alias Endurant.Crons
  alias Endurant.Registry

  @default_instance Endurant

  @typedoc "Name for the top-level Endurant instance."
  @type start_name :: Config.instance_name()

  @typedoc """
  Queue runtime options.

  Supported keys:
  - `:limit` maximum number of concurrently running executions for that queue.
  - `:parked_limit` maximum number of parked executions tracked in-memory.
  - `:poll_interval` queue polling interval in milliseconds.
  - `:lease_ms` execution lock lease in milliseconds.
  - `:recovery_limit` maximum number of expired locked executions recovered per poll.
  - `:heartbeat_interval` heartbeat interval used by long-running/parked executions.
  - `:repo` Ecto repo module used for persistence.
  - `:prefix` database schema prefix.
  """
  @type queue_options :: keyword()

  @typedoc "Queue definitions passed to `start_link/1`."
  @type queues_option :: keyword(queue_options())

  @type start_option ::
          {:name, start_name()}
          | {:repo, module()}
          | {:prefix, String.t()}
          | {:queue_defaults, keyword()}
          | {:crons, [keyword() | map()]}
          | {:queues, queues_option()}
  @type start_options :: [start_option()]
  @type instance_name :: Config.instance_name()
  @type schedule_options :: [{:id, binary()}]
  @type cron_options ::
          [
            {:id, binary()}
            | {:name, String.t()}
            | {:timezone, String.t()}
            | {:start_at, DateTime.t()}
            | {:end_at, DateTime.t()}
          ]

  @type insert_result :: {:ok, map()} | {:error, :unique_conflict}
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
  @type events_result :: [Endurant.Events.event()]

  @doc """
  Starts Endurant's supervision tree.

  ## Options

  - `:name` non-empty string or atom instance name (defaults to `Endurant`).
  - `:repo` Ecto repo module for this instance.
  - `:prefix` database schema prefix (default `"public"`).
  - `:queue_defaults` queue defaults merged into each queue config.
  - `:crons` config-managed cron schedules synced on startup.
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

      config :endurant, Endurant,
        repo: MyApp.Repo,
        prefix: "public",
        queues: [default: [limit: 10]]

  ## Examples

      Endurant.start_link(
        repo: MyApp.Repo,
        prefix: "public",
        queues: [
          default: [limit: 10, poll_interval: 200],
          emails: [limit: 5, poll_interval: 100]
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
    insert(@default_instance, workflow_module, input)
  end

  @doc """
  Inserts a workflow execution request targeting an instance.
  """
  @spec insert(instance_name(), module(), map()) :: insert_result()
  def insert(instance, workflow_module, input)
      when (is_atom(instance) or is_binary(instance)) and is_atom(workflow_module) and
             is_map(input) do
    Endurant.Executions.insert(workflow_module, input, instance_runtime_opts!(instance))
  end

  @doc """
  Schedules a workflow execution on the default instance.
  """
  @spec schedule(module(), map(), DateTime.t()) :: schedule_result()
  def schedule(workflow_module, input, scheduled_at)
      when is_atom(workflow_module) and is_map(input) and is_struct(scheduled_at, DateTime) do
    schedule(@default_instance, workflow_module, input, scheduled_at, [])
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
    schedule(@default_instance, workflow_module, input, scheduled_at, opts)
  end

  @doc """
  Schedules a workflow execution to be dispatched at a future time.

  `scheduled_at` must be a UTC `DateTime`.

  Supported options:

  - `:id` explicit schedule id (UUID string). Defaults to generated UUID.

  Returns `{:error, :id_conflict}` when the provided id already exists.
  """
  @spec schedule(instance_name(), module(), map(), DateTime.t(), schedule_options()) ::
          schedule_result()
  def schedule(instance, workflow_module, input, scheduled_at, opts \\ [])
      when (is_atom(instance) or is_binary(instance)) and is_atom(workflow_module) and
             is_map(input) and is_struct(scheduled_at, DateTime) and is_list(opts) do
    runtime_opts = instance_runtime_opts!(instance)

    Endurant.Schedules.insert(
      workflow_module,
      input,
      scheduled_at,
      Keyword.merge(runtime_opts, opts)
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
  @spec scheduled(instance_name()) :: scheduled_result()
  @spec scheduled(keyword()) :: scheduled_result()
  @spec scheduled(instance_name(), keyword()) :: scheduled_result()
  def scheduled(instance_or_filters \\ @default_instance, filters \\ [])

  def scheduled(instance_or_filters, filters) when is_list(filters) do
    {instance, resolved_filters} =
      normalize_instance_and_filters!(instance_or_filters, filters, :scheduled)

    Endurant.Schedules.list(resolved_filters, instance_runtime_opts!(instance))
  end

  @doc """
  Cancels one scheduled row on the default instance.

  A row can only be cancelled while `:pending`.
  """
  @spec cancel_scheduled(binary()) :: cancel_scheduled_result()
  def cancel_scheduled(schedule_id) when is_binary(schedule_id) do
    cancel_scheduled(@default_instance, schedule_id)
  end

  @doc """
  Cancels one scheduled row on an instance.

  Returns:

  - `:ok` when cancelled.
  - `{:error, :not_found}` when id doesn't exist.
  - `{:error, :not_pending}` when already dispatched/skipped/failed/cancelled.
  """
  @spec cancel_scheduled(instance_name(), binary()) :: cancel_scheduled_result()
  def cancel_scheduled(instance, schedule_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(schedule_id) do
    Endurant.Schedules.cancel(schedule_id, instance_runtime_opts!(instance))
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
    cron(@default_instance, workflow_module, input, cron_expr, opts)
  end

  @doc """
  Creates a cron schedule on an instance.

  Cron schedule rows have `:active | :paused` status. Runtime dispatch creates
  fire rows in scheduled executions using overlap policy `:skip`.
  """
  @spec cron(instance_name(), module(), map(), String.t(), cron_options()) :: cron_result()
  def cron(instance, workflow_module, input, cron_expr, opts \\ [])
      when (is_atom(instance) or is_binary(instance)) and is_atom(workflow_module) and
             is_map(input) and is_binary(cron_expr) and is_list(opts) do
    runtime_opts = instance_runtime_opts!(instance)
    Crons.insert(workflow_module, input, cron_expr, Keyword.merge(runtime_opts, opts))
  end

  @doc """
  Lists cron schedules from the default instance.

  Supported filters:

  - `:status` `:active | :paused`
  - `:limit` positive integer (default `100`)
  """
  @spec crons() :: crons_result()
  @spec crons(instance_name()) :: crons_result()
  @spec crons(keyword()) :: crons_result()
  @spec crons(instance_name(), keyword()) :: crons_result()
  def crons(instance_or_filters \\ @default_instance, filters \\ [])

  def crons(instance_or_filters, filters) when is_list(filters) do
    {instance, resolved_filters} =
      normalize_instance_and_filters!(instance_or_filters, filters, :crons)

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

  @spec cron_fires(instance_name(), binary()) :: cron_fires_result()
  def cron_fires(instance, cron_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(cron_id) do
    Crons.list_fires(cron_id, [], instance_runtime_opts!(instance))
  end

  @spec cron_fires(binary(), keyword()) :: cron_fires_result()
  def cron_fires(cron_id, filters) when is_binary(cron_id) and is_list(filters) do
    Crons.list_fires(cron_id, filters, instance_runtime_opts!(@default_instance))
  end

  @spec cron_fires(instance_name(), binary(), keyword()) :: cron_fires_result()
  def cron_fires(instance, cron_id, filters)
      when (is_atom(instance) or is_binary(instance)) and is_binary(cron_id) and is_list(filters) do
    Crons.list_fires(cron_id, filters, instance_runtime_opts!(instance))
  end

  @doc """
  Pauses one cron schedule on the default instance.
  """
  @spec pause_cron(binary()) :: pause_cron_result()
  def pause_cron(cron_id) when is_binary(cron_id) do
    pause_cron(@default_instance, cron_id)
  end

  @doc """
  Pauses one cron schedule on an instance.

  Returns:

  - `:ok` when paused.
  - `{:error, :not_found}` when id doesn't exist.
  - `{:error, :not_active}` when already paused.
  """
  @spec pause_cron(instance_name(), binary()) :: pause_cron_result()
  def pause_cron(instance, cron_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(cron_id) do
    Crons.pause(cron_id, instance_runtime_opts!(instance))
  end

  @doc """
  Resumes one paused cron schedule on the default instance.
  """
  @spec resume_cron(binary()) :: resume_cron_result()
  def resume_cron(cron_id) when is_binary(cron_id) do
    resume_cron(@default_instance, cron_id)
  end

  @doc """
  Resumes one paused cron schedule on an instance.

  Returns:

  - `:ok` when resumed.
  - `{:error, :not_found}` when id doesn't exist.
  - `{:error, :not_paused}` when already active.
  - `{:error, :ended}` when no future run fits within `end_at`.
  """
  @spec resume_cron(instance_name(), binary()) :: resume_cron_result()
  def resume_cron(instance, cron_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(cron_id) do
    Crons.resume(cron_id, instance_runtime_opts!(instance))
  end

  @doc """
  Deletes one cron schedule on the default instance.
  """
  @spec delete_cron(binary()) :: delete_cron_result()
  def delete_cron(cron_id) when is_binary(cron_id) do
    delete_cron(@default_instance, cron_id)
  end

  @doc """
  Deletes one cron schedule on an instance.

  This deletes the cron schedule row. Existing generated fire rows are not
  deleted.
  """
  @spec delete_cron(instance_name(), binary()) :: delete_cron_result()
  def delete_cron(instance, cron_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(cron_id) do
    Crons.delete(cron_id, instance_runtime_opts!(instance))
  end

  @doc """
  Records a signal targeting an instance.
  """
  @spec signal(binary(), String.t()) :: signal_result()
  def signal(execution_id, signal) when is_binary(execution_id) and is_binary(signal) do
    signal(@default_instance, execution_id, signal, %{})
  end

  @spec signal(binary(), String.t(), map()) :: signal_result()
  def signal(execution_id, signal, payload)
      when is_binary(execution_id) and is_binary(signal) and is_map(payload) do
    signal(@default_instance, execution_id, signal, payload)
  end

  @doc """
  Records a signal targeting an instance.
  """
  @spec signal(instance_name(), binary(), String.t()) :: signal_result()
  def signal(instance, execution_id, signal)
      when (is_atom(instance) or is_binary(instance)) and is_binary(execution_id) and
             is_binary(signal) do
    signal(instance, execution_id, signal, %{})
  end

  @spec signal(instance_name(), binary(), String.t(), map()) :: signal_result()
  def signal(instance, execution_id, signal, payload)
      when (is_atom(instance) or is_binary(instance)) and is_binary(execution_id) and
             is_binary(signal) and is_map(payload) do
    Endurant.Executions.record_signal(
      execution_id,
      signal,
      payload,
      instance_runtime_opts!(instance)
    )
  end

  @doc """
  Requests cancellation of an execution targeting the default instance.
  """
  @spec cancel(binary()) :: cancel_result()
  def cancel(execution_id) when is_binary(execution_id) do
    cancel(@default_instance, execution_id)
  end

  @doc """
  Requests cancellation of an execution targeting an instance.
  """
  @spec cancel(instance_name(), binary()) :: cancel_result()
  def cancel(instance, execution_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(execution_id) do
    Endurant.Executions.cancel(execution_id, instance_runtime_opts!(instance))
  end

  @doc """
  Fetches one execution by id targeting the default instance.
  """
  @spec execution(binary()) :: execution_result()
  def execution(execution_id) when is_binary(execution_id) do
    execution(@default_instance, execution_id)
  end

  @doc """
  Fetches one execution by id targeting an instance.
  """
  @spec execution(instance_name(), binary()) :: execution_result()
  def execution(instance, execution_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(execution_id) do
    Endurant.Executions.get(execution_id, instance_runtime_opts!(instance))
  end

  @doc """
  Lists executions from the default instance using optional filters.
  """
  @spec executions() :: executions_result()
  def executions do
    executions(@default_instance, [])
  end

  @spec executions(keyword()) :: executions_result()
  def executions(filters) when is_list(filters) do
    executions(@default_instance, filters)
  end

  @doc """
  Lists executions from an instance using optional filters.
  """
  @spec executions(instance_name(), keyword()) :: executions_result()
  def executions(instance, filters)
      when (is_atom(instance) or is_binary(instance)) and is_list(filters) do
    Endurant.Executions.list(filters, instance_runtime_opts!(instance))
  end

  @doc """
  Lists all events for an execution targeting the default instance.
  """
  @spec events(binary()) :: events_result()
  def events(execution_id) when is_binary(execution_id) do
    events(@default_instance, execution_id)
  end

  @doc """
  Lists all events for an execution targeting an instance.
  """
  @spec events(instance_name(), binary()) :: events_result()
  def events(instance, execution_id)
      when (is_atom(instance) or is_binary(instance)) and is_binary(execution_id) do
    Endurant.Events.list(execution_id, instance_runtime_opts!(instance))
  end

  @spec normalize_instance_and_filters!(instance_name() | keyword(), keyword(), atom()) ::
          {instance_name(), keyword()}
  defp normalize_instance_and_filters!(instance, filters, _kind)
       when (is_atom(instance) or is_binary(instance)) and is_list(filters) do
    {instance, filters}
  end

  defp normalize_instance_and_filters!(filters, [], _kind) when is_list(filters) do
    {@default_instance, filters}
  end

  defp normalize_instance_and_filters!(instance_or_filters, filters, kind) do
    raise ArgumentError,
          "invalid #{kind} arguments: #{inspect(instance_or_filters)}, #{inspect(filters)}"
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
