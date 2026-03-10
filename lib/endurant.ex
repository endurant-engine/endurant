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
          | {:queues, queues_option()}
  @type start_options :: [start_option()]
  @type instance_name :: Config.instance_name()

  @type insert_result :: {:ok, map()} | {:error, :unique_conflict}
  @type signal_result :: :ok | {:error, :not_found | :not_active}
  @type cancel_result :: :ok | {:error, :not_found | :not_active}
  @type execution_result :: map() | nil
  @type events_result :: [Endurant.Events.event()]

  @doc """
  Starts Endurant's supervision tree.

  ## Options

  - `:name` non-empty string or atom instance name (defaults to `Endurant`).
  - `:repo` Ecto repo module for this instance.
  - `:prefix` database schema prefix (default `"public"`).
  - `:queue_defaults` queue defaults merged into each queue config.
  - `:queues` queue definitions and queue options.

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
