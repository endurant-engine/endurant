defmodule Endurant.Workflow.Signals do
  @moduledoc """
  Signal waiting primitives for workflows.

  Use `wait_signal/1` or `wait_signal/2` inside `run/2` (or helper functions called from `run/2`)
  to block workflow progress until a matching signal payload is available.

  Signal consumption is deterministic:

    * queued signal events are loaded from history in sequence order
    * each `wait_signal/1` consumes exactly one payload (FIFO per signal key)
    * if no payload is available, execution transitions to waiting and resumes
      when new signal events arrive

  This module is imported by `use Endurant.Workflow`.
  """

  alias Endurant.Events
  alias Endurant.Executions
  alias Endurant.Telemetry
  alias Endurant.Workflow

  @doc """
  Waits for the next payload of `name` and returns it.

  `name` must be a string. When a queued payload already exists in runtime
  signal history, it is returned immediately. Otherwise the execution enters
  waiting state and resumes when a matching signal is recorded.

  ## Options

    * `:cached_ttl_ms` overrides the cached TTL for this wait only

  ## Example

      message = wait_signal("user_message")
  """
  @spec wait_signal(String.t()) :: term() | no_return()
  @spec wait_signal(String.t(), keyword()) :: term() | no_return()
  def wait_signal(name, opts \\ []) when is_binary(name) and is_list(opts) do
    signal_key = normalize_signal_name!(name)

    runtime =
      Workflow.runtime!()
      |> Workflow.apply_wait_opts(opts)

    runtime = ensure_signal_runtime(runtime)

    case pop_signal(runtime, signal_key) do
      {:ok, payload, next_runtime} ->
        Workflow.put_runtime(next_runtime)
        payload

      :empty ->
        refreshed_runtime = refresh_signal_runtime(runtime)

        case pop_signal(refreshed_runtime, signal_key) do
          {:ok, payload, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            payload

          :empty ->
            wait_for_signal_resume(refreshed_runtime, signal_key)
        end
    end
  end

  @spec wait_for_signal_resume(map(), String.t()) :: term() | no_return()
  defp wait_for_signal_resume(runtime, signal_key) do
    wait_started_at = Telemetry.monotonic_time()
    enter_signal_wait(runtime, signal_key)
    do_wait_for_signal_resume(runtime, signal_key, wait_started_at)
  end

  @spec enter_signal_wait(map(), String.t()) :: :ok | no_return()
  defp enter_signal_wait(runtime, signal_key) do
    case Executions.mark_waiting_with_event_owned(
           runtime.execution_id,
           runtime.worker_id,
           signal_key,
           runtime.opts
         ) do
      :ok ->
        :ok

      {:error, :not_running} ->
        throw({:endurant_halt, :not_running})
    end

    emit_signal(runtime, :wait_started, %{count: 1})

    case notify_waiting(runtime) do
      :cache ->
        emit_signal(runtime, :cache_decision, %{count: 1}, %{decision: :cache})
        :ok

      :release ->
        emit_signal(runtime, :cache_decision, %{count: 1}, %{decision: :release})

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

  @spec do_wait_for_signal_resume(map(), String.t(), integer()) :: term() | no_return()
  defp do_wait_for_signal_resume(runtime, signal_key, wait_started_at) do
    case await_resume() do
      :ok ->
        refreshed_runtime = refresh_signal_runtime(runtime)

        case pop_signal(refreshed_runtime, signal_key) do
          {:ok, payload, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            notify_ready(next_runtime)

            emit_signal(
              next_runtime,
              :resumed,
              %{count: 1, wait_duration_ms: Telemetry.duration_ms(wait_started_at)}
            )

            payload

          :empty ->
            do_wait_for_signal_resume(refreshed_runtime, signal_key, wait_started_at)
        end

      {:error, :cancelled} ->
        throw({:endurant_halt, :not_running})

      :released ->
        throw({:endurant_halt, :waiting_persisted})
    end
  end

  @spec notify_waiting(map()) :: :cache | :release
  defp notify_waiting(runtime) do
    manager = queue_manager!(runtime)

    GenServer.call(
      manager,
      {:executor_cached, self(), runtime.execution_id,
       Map.get(runtime, :cached_ttl_ms, :infinity)},
      5_000
    )
  end

  @spec notify_ready(map()) :: :ok
  defp notify_ready(runtime) do
    manager = queue_manager!(runtime)
    _ = GenServer.call(manager, {:executor_ready, self(), runtime.execution_id}, 5_000)

    :ok
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

  @spec await_resume() :: :ok | :released | {:error, :cancelled}
  defp await_resume do
    receive do
      :resume -> :ok
      :cached_ttl -> :released
      :heartbeat_cancelled -> {:error, :cancelled}
      :heartbeat_lock_lost -> {:error, :cancelled}
      {:heartbeat_failed, reason} -> raise "heartbeat failed while waiting: #{inspect(reason)}"
    end
  end

  @spec ensure_signal_runtime(map()) :: map()
  defp ensure_signal_runtime(runtime) do
    case Map.fetch(runtime, :signal_queues) do
      {:ok, _queues} ->
        runtime

      :error ->
        Map.merge(runtime, %{signal_queues: %{}, loaded_signal_seq: 0})
    end
  end

  @spec refresh_signal_runtime(map()) :: map()
  defp refresh_signal_runtime(runtime) do
    loaded_signal_seq = Map.get(runtime, :loaded_signal_seq, 0)
    events = Events.list_after(runtime.execution_id, loaded_signal_seq, runtime.opts)

    Enum.reduce(events, runtime, fn event, acc ->
      seq =
        case event do
          %{sequence: value} when is_integer(value) and value > 0 -> value
          _ -> Map.get(acc, :loaded_signal_seq, 0)
        end

      acc = %{acc | loaded_signal_seq: max(Map.get(acc, :loaded_signal_seq, 0), seq)}

      case event do
        %{type: :signal_received, payload: %{"signal" => signal, "payload" => payload}} ->
          enqueue_signal(acc, signal, payload)

        %{type: :signal_received, payload: %{signal: signal, payload: payload}} ->
          enqueue_signal(acc, signal, payload)

        _ ->
          acc
      end
    end)
  end

  @spec pop_signal(map(), String.t()) :: {:ok, term(), map()} | :empty
  defp pop_signal(runtime, signal_key) do
    signal_queues = Map.get(runtime, :signal_queues, %{})

    queue =
      case Map.get(signal_queues, signal_key, :queue.new()) do
        value when is_list(value) -> :queue.from_list(value)
        value -> value
      end

    case :queue.out(queue) do
      {{:value, payload}, rest_queue} ->
        {:ok, payload, %{runtime | signal_queues: Map.put(signal_queues, signal_key, rest_queue)}}

      {:empty, _queue} ->
        :empty
    end
  end

  @spec enqueue_signal(map(), String.t(), term()) :: map()
  defp enqueue_signal(runtime, signal, payload) do
    signal_queues = Map.get(runtime, :signal_queues, %{})

    signal_queues =
      signal
      |> signal_keys()
      |> Enum.reduce(signal_queues, fn key, acc -> enqueue_for_key(acc, key, payload) end)

    %{runtime | signal_queues: signal_queues}
  end

  @spec signal_keys(String.t()) :: [String.t()]
  defp signal_keys(signal) do
    [normalize_signal_name!(signal)]
  end

  @spec enqueue_for_key(map(), String.t(), term()) :: map()
  defp enqueue_for_key(signal_queues, key, payload) do
    Map.update(signal_queues, key, :queue.in(payload, :queue.new()), fn queue ->
      :queue.in(payload, queue)
    end)
  end

  @spec normalize_signal_name!(term()) :: String.t()
  defp normalize_signal_name!(name) when is_binary(name), do: name

  defp normalize_signal_name!(name) do
    raise ArgumentError, "signal name must be a string, got: #{inspect(name)}"
  end

  @spec emit_signal(map(), atom(), map(), map()) :: :ok
  defp emit_signal(runtime, event, measurements, extra_metadata \\ %{}) do
    Telemetry.emit(
      [:signal, event],
      measurements,
      Map.merge(
        %{
          instance: Map.get(runtime, :instance),
          node: node(),
          queue: Map.get(runtime, :queue),
          workflow: Map.get(runtime, :workflow),
          version: Map.get(runtime, :version)
        },
        extra_metadata
      )
    )
  end
end
