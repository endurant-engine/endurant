defmodule Endurant do
  @moduledoc """
  Public API for running Endurant and interacting with workflow executions.

  ## Overview

  Endurant is started as a supervisor with one or more queue managers.
  Workflow executions are inserted with `insert/3` and external events can be
  delivered with `signal/4`.

  ## Basic Usage

  Start Endurant with a queue:

      {:ok, _pid} =
        Endurant.start_link(
          name: "my_endurant",
          queues: [default: [concurrency: 10]]
        )

  Insert a workflow execution:

      {:ok, execution} =
        Endurant.insert(MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"})

  Deliver a signal to a running/waiting execution:

      :ok =
        Endurant.signal(execution.id, "approval_requested", %{approved: true})
  """

  @typedoc "Name for the top-level Endurant supervisor instance."
  @type start_name :: String.t()

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

  @typedoc "Queue definitions passed to `start_link/1`."
  @type queues_option :: keyword(queue_options())

  @type start_option :: {:name, start_name()} | {:queues, queues_option()}
  @type start_options :: [start_option()]

  @typedoc """
  Runtime options used for insert/signal operations.

  Supported keys:
  - `:repo` Ecto repo module used for persistence.
  - `:prefix` database schema prefix.
  """
  @type runtime_options :: keyword()

  @type insert_result :: {:ok, map()} | {:error, :unique_conflict}
  @type signal_result :: :ok | {:error, :not_found | :not_active}
  @type cancel_result :: :ok | {:error, :not_found | :not_active}
  @type execution_result :: map() | nil
  @type events_result :: [Endurant.Events.event()]

  @doc """
  Starts Endurant's supervision tree.

  ## Options

  - `:name` non-empty string name for the Endurant instance (required).
  - `:queues` queue definitions and queue options.

  ## Examples

      Endurant.start_link(
        name: "my_endurant",
        queues: [
          default: [concurrency: 10, poll_interval: 200],
          emails: [concurrency: 5, poll_interval: 100]
        ]
      )
  """
  @spec start_link(start_options()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    Endurant.Supervisor.start_link(opts)
  end

  @doc """
  Inserts a workflow execution request.

  Uses default runtime options.
  """
  @spec insert(module(), map()) :: insert_result()
  def insert(workflow_module, input) when is_atom(workflow_module) and is_map(input) do
    insert(workflow_module, input, [])
  end

  @doc """
  Inserts a workflow execution request with runtime options.

  ## Options

  - `:repo` Ecto repo module.
  - `:prefix` database schema prefix.

  ## Examples

      Endurant.insert(MyApp.Workflows.OrderWorkflow, %{order_id: "o-123"})

      Endurant.insert(
        MyApp.Workflows.OrderWorkflow,
        %{order_id: "o-123"},
        repo: MyApp.Repo,
        prefix: "public"
      )
  """
  @spec insert(module(), map(), runtime_options()) :: insert_result()
  def insert(workflow_module, input, opts)
      when is_atom(workflow_module) and is_map(input) and is_list(opts) do
    Endurant.Executions.insert(workflow_module, input, opts)
  end

  @doc """
  Records a signal and wakes a waiting execution, when applicable.

  Uses default runtime options.
  """
  @spec signal(binary(), String.t(), map()) :: signal_result()
  def signal(execution_id, signal, payload \\ %{})
      when is_binary(execution_id) and is_binary(signal) and is_map(payload) do
    signal(execution_id, signal, payload, [])
  end

  @doc """
  Records a signal for an execution with runtime options.

  ## Options

  - `:repo` Ecto repo module.
  - `:prefix` database schema prefix.

  ## Examples

      Endurant.signal(execution_id, "approval_requested", %{approved: true})

      Endurant.signal(
        execution_id,
        "approval_requested",
        %{approved: true},
        repo: MyApp.Repo,
        prefix: "public"
      )
  """
  @spec signal(binary(), String.t(), map(), runtime_options()) :: signal_result()
  def signal(execution_id, signal, payload, opts)
      when is_binary(execution_id) and is_binary(signal) and is_map(payload) and
             is_list(opts) do
    Endurant.Executions.record_signal(execution_id, signal, payload, opts)
  end

  @doc """
  Requests cancellation of an execution.

  Uses default runtime options.
  """
  @spec cancel(binary()) :: cancel_result()
  def cancel(execution_id) when is_binary(execution_id) do
    cancel(execution_id, [])
  end

  @doc """
  Requests cancellation of an execution with runtime options.

  ## Options

  - `:repo` Ecto repo module.
  - `:prefix` database schema prefix.
  """
  @spec cancel(binary(), runtime_options()) :: cancel_result()
  def cancel(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Executions.cancel(execution_id, opts)
  end

  @doc """
  Fetches one execution by id.

  Uses default runtime options.
  """
  @spec execution(binary()) :: execution_result()
  def execution(execution_id) when is_binary(execution_id) do
    execution(execution_id, [])
  end

  @doc """
  Fetches one execution by id with runtime options.

  ## Options

  - `:repo` Ecto repo module.
  - `:prefix` database schema prefix.
  """
  @spec execution(binary(), runtime_options()) :: execution_result()
  def execution(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Executions.get(execution_id, opts)
  end

  @doc """
  Lists all events for an execution in sequence order.

  Uses default runtime options.
  """
  @spec events(binary()) :: events_result()
  def events(execution_id) when is_binary(execution_id) do
    events(execution_id, [])
  end

  @doc """
  Lists all events for an execution with runtime options.

  ## Options

  - `:repo` Ecto repo module.
  - `:prefix` database schema prefix.
  """
  @spec events(binary(), runtime_options()) :: events_result()
  def events(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    Endurant.Events.list(execution_id, opts)
  end
end
