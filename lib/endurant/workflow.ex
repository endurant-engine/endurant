defmodule Endurant.Workflow do
  @moduledoc """
  Workflow definition DSL.

  Use this module to declare workflow metadata and implement `run/2`.

  `run/2` is successful as long as it returns a value. The workflow result is
  the function's return value (typically the last expression).

  ## Example

      defmodule MyApp.Workflows.OrderWorkflow do
        use Endurant.Workflow, version: "1"

        workflow do
          queue("orders")
          unique_id(fn %{"order_id" => id} -> "order:\#{id}" end)
        end

        @impl Endurant.Workflow
        def run(version, input) do
          _ = version
          %{"order_id" => input["order_id"], "accepted" => true}
        end
      end

  ## `use` options

    * `:version` - workflow version string (default `"1"`)
  """

  @runtime_key :endurant_workflow_runtime
  alias Endurant.Events
  alias Endurant.Executions

  defmodule AsyncHandle do
    @moduledoc """
    Handle returned by async workflow task helpers.

    Endurant uses this to track in-flight async work between `task_async/3,4`,
    `task_async_stream/3,4`, `task_await/1`, and `task_await_many/1`.
    """

    @enforce_keys [:name, :task_key, :fun, :input, :opts]
    defstruct [:name, :task_key, :fun, :input, :opts, :task]

    @opaque t :: %__MODULE__{
              name: String.t(),
              task_key: String.t(),
              fun: (term() -> term()),
              input: term(),
              opts: keyword(),
              task: Task.t() | nil
            }
  end

  @typedoc "Workflow version passed to `run/2`."
  @type version :: String.t()

  @typedoc "Workflow input payload."
  @type input :: map()

  @typedoc "Workflow return value."
  @type result :: term()

  @type async_handle :: AsyncHandle.t()

  @callback run(version(), input()) :: result()

  # DSL
  @doc false
  defmacro __using__(opts) do
    version = Keyword.get(opts, :version, "1")

    if not (is_binary(version) and byte_size(version) > 0) do
      raise ArgumentError,
            "use Endurant.Workflow expects :version to be a non-empty string, got: #{Macro.to_string(version)}"
    end

    quote do
      @behaviour Endurant.Workflow

      import Endurant.Workflow,
        only: [
          workflow: 1,
          queue: 1,
          unique_id: 1,
          sleep: 2,
          continue_as_new: 1,
          continue_as_new: 2,
          execution_id: 0,
          history_length: 0,
          history_size: 0
        ]

      import Endurant.Workflow.Tasks,
        only: [
          task: 3,
          task: 4,
          task_async: 3,
          task_async: 4,
          task_await: 1,
          task_await_many: 1,
          task_async_stream: 3,
          task_async_stream: 4
        ]

      import Endurant.Workflow.Signals, only: [wait_signal: 1]

      import Endurant.Workflow.Children,
        only: [
          child_workflow: 3,
          child_workflow: 4,
          child_workflow_async: 3,
          child_workflow_async: 4,
          child_workflow_await: 1
        ]

      Module.register_attribute(__MODULE__, :endurant_workflow_version, persist: true)

      @endurant_workflow_version unquote(version)

      @spec __workflow_queue__() :: String.t() | nil
      def __workflow_queue__, do: nil
      defoverridable __workflow_queue__: 0

      @spec __unique_id__(map()) :: String.t() | nil
      def __unique_id__(_input), do: nil
      defoverridable __unique_id__: 1

      @spec __workflow__() :: map()
      def __workflow__ do
        %{
          queue: __workflow_queue__(),
          unique_id: &__MODULE__.__unique_id__/1,
          version: @endurant_workflow_version
        }
      end
    end
  end

  @doc """
  Declares workflow metadata block.
  """
  defmacro workflow(do: block) do
    quote do
      unquote(block)
    end
  end

  defmacro workflow(_name, do: _block) do
    raise ArgumentError,
          "workflow/2 with a name is no longer supported; use `workflow do ... end`"
  end

  @doc """
  Sets the queue name for this workflow.

  `value` must be a string.
  """
  defmacro queue(value) when is_binary(value) do
    quote do
      @spec __workflow_queue__() :: String.t()
      def __workflow_queue__, do: unquote(value)
    end
  end

  defmacro queue(value) do
    raise ArgumentError, "queue/1 expects a string, got: #{Macro.to_string(value)}"
  end

  @doc """
  Sets the unique id resolver for this workflow.

  Accepted forms:

    * `unique_id("fixed-id")`
    * `unique_id(fn input -> ... end)`
    * `unique_id(resolver)` where `resolver` evaluates to a string or function/1
  """
  defmacro unique_id({:fn, _, _} = fun_ast) do
    quote do
      @spec __unique_id__(map()) :: String.t()
      def __unique_id__(input), do: unquote(fun_ast).(input)
    end
  end

  defmacro unique_id(value) when is_binary(value) do
    quote do
      @spec __unique_id__(map()) :: String.t()
      def __unique_id__(_input), do: unquote(value)
    end
  end

  defmacro unique_id(value) do
    quote do
      @spec __unique_id__(map()) :: String.t()
      def __unique_id__(input) do
        resolver = unquote(value)

        case resolver do
          fun when is_function(fun, 1) ->
            fun.(input)

          val when is_binary(val) ->
            val

          other ->
            raise ArgumentError,
                  "unique_id must be a function/1 or string, got: #{inspect(other)}"
        end
      end
    end
  end

  @doc """
  Suspends workflow execution for `delay_ms`.

  `wait_key` identifies this logical wait across replay. Use a stable key per
  wait site/iteration.

  ## Example

      sleep("retry:\#{attempt}", 1_000)
  """
  @spec sleep(String.t(), pos_integer()) :: :ok | no_return()
  def sleep(wait_key, delay_ms)
      when is_binary(wait_key) and is_integer(delay_ms) and delay_ms > 0 do
    runtime = runtime!()

    if MapSet.member?(runtime.waits, wait_key) do
      :ok
    else
      runtime = %{runtime | waits: MapSet.put(runtime.waits, wait_key)}
      put_runtime(runtime)
      wait_for_time_resume(runtime, delay_ms, wait_key)
    end
  end

  @doc """
  Ends the current execution and immediately continues as a new execution.

  The new execution inherits workflow metadata.
  """
  @spec continue_as_new(map()) :: no_return()
  def continue_as_new(next_input) when is_map(next_input) do
    continue_as_new(next_input, [])
  end

  @doc """
  Ends the current execution and immediately continues as a new execution with
  supported option overrides.

  Supported options:
  - `version: "..."` starts the continued execution with a different workflow version
  - `rollover_signals: true` carries unused observed signals into the new execution
  """
  @spec continue_as_new(map(), keyword()) :: no_return()
  def continue_as_new(next_input, opts) when is_map(next_input) and is_list(opts) do
    runtime = runtime!()
    version = Keyword.get(opts, :version)
    rollover_signals = Keyword.get(opts, :rollover_signals, false)

    if not (is_nil(version) or (is_binary(version) and byte_size(version) > 0)) do
      raise ArgumentError,
            "continue_as_new/2 expects :version to be a non-empty string, got: #{inspect(version)}"
    end

    if not is_boolean(rollover_signals) do
      raise ArgumentError,
            "continue_as_new/2 expects :rollover_signals to be a boolean, got: #{inspect(rollover_signals)}"
    end

    throw(
      {:endurant_continue_as_new,
       %{
         next_input: next_input,
         version: version,
         rollover_signals: rollover_signals,
         signal_queues: Map.get(runtime, :signal_queues, %{}),
         loaded_signal_seq: Map.get(runtime, :loaded_signal_seq, 0),
         first_execution_id: Map.get(runtime, :first_execution_id, runtime.execution_id)
       }}
    )
  end

  @spec wait_for_time_resume(map(), pos_integer(), String.t()) :: :ok | no_return()
  defp wait_for_time_resume(runtime, delay_ms, wait_key) do
    run_at = DateTime.add(DateTime.utc_now(), delay_ms, :millisecond)

    case Executions.mark_waiting_with_time_event_owned(
           runtime.execution_id,
           runtime.worker_id,
           run_at,
           delay_ms,
           wait_key,
           runtime.opts
         ) do
      :ok ->
        :ok

      {:error, :not_running} ->
        throw({:endurant_halt, :not_running})
    end

    case notify_waiting(runtime) do
      :cache ->
        case await_resume() do
          :ok -> :ok
          {:error, :cancelled} -> throw({:endurant_halt, :not_running})
        end

      :release ->
        case Executions.release_waiting_as_abandoned_owned(
               runtime.execution_id,
               runtime.worker_id,
               runtime.opts
             ) do
          :ok ->
            throw({:endurant_halt, :waiting_persisted})

          {:error, :not_running} ->
            throw({:endurant_halt, :not_running})
        end
    end
  end

  @spec notify_waiting(map()) :: :cache | :release
  defp notify_waiting(runtime) do
    manager = queue_manager!(runtime)
    GenServer.call(manager, {:executor_cached, self(), runtime.execution_id}, 5_000)
  end

  @spec queue_manager!(map()) :: pid() | atom() | {:via, module(), term()}
  defp queue_manager!(runtime) do
    case Keyword.fetch(runtime.opts, :queue_manager) do
      {:ok, manager} ->
        manager

      :error ->
        raise ArgumentError, "missing required :queue_manager option"
    end
  end

  @spec await_resume() :: :ok | {:error, :cancelled}
  defp await_resume do
    receive do
      :resume -> :ok
      :heartbeat_cancelled -> {:error, :cancelled}
      :heartbeat_lock_lost -> {:error, :cancelled}
      {:heartbeat_failed, reason} -> raise "heartbeat failed while waiting: #{inspect(reason)}"
    end
  end

  @doc """
  Returns the current execution id.

  Available only while running inside the workflow executor.
  """
  @spec execution_id() :: binary()
  def execution_id do
    runtime!().execution_id
  end

  @doc """
  Returns current workflow history event count.

  Available only while running inside the workflow executor.
  """
  @spec history_length() :: non_neg_integer()
  def history_length do
    runtime = runtime!()

    case Events.history_length(runtime.execution_id, runtime.opts) do
      event_count when is_integer(event_count) and event_count >= 0 ->
        event_count

      _ ->
        Map.get(runtime, :history_length, 0)
    end
  end

  @doc """
  Returns current workflow history size in bytes.

  Available only while running inside the workflow executor.
  """
  @spec history_size() :: non_neg_integer()
  def history_size do
    runtime = runtime!()

    case Events.history_size(runtime.execution_id, runtime.opts) do
      history_size_bytes when is_integer(history_size_bytes) and history_size_bytes >= 0 ->
        history_size_bytes

      _ ->
        Map.get(runtime, :history_size_bytes, 0)
    end
  end

  @doc false
  @spec put_runtime(map()) :: :ok
  def put_runtime(runtime) when is_map(runtime) do
    Process.put(@runtime_key, runtime)
    :ok
  end

  @doc false
  @spec delete_runtime() :: :ok
  def delete_runtime do
    Process.delete(@runtime_key)
    Endurant.Workflow.Tasks.delete_runtime_state()
    :ok
  end

  # Runtime access
  @doc false
  @spec runtime!() :: map()
  def runtime! do
    case Process.get(@runtime_key) do
      nil ->
        raise ArgumentError,
              "workflow runtime missing; tasks/signals can only run inside Endurant.Executor"

      runtime ->
        runtime
    end
  end
end
