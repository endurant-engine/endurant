defmodule Endurant.Executor do
  @moduledoc false

  alias Endurant.Events
  alias Endurant.Executions
  alias Endurant.Telemetry
  alias Endurant.Workflow

  @type execution_ref :: map() | binary()
  @type execution_outcome ::
          :completed
          | :waiting
          | :cancelled
          | :failed
          | :lock_lost
          | :not_found
  @heartbeat_stop_timeout 1_000

  @spec run(execution_ref(), keyword()) ::
          execution_outcome()
  def run(execution_or_id, opts \\ []) do
    _ = queue_manager!(opts)
    worker_id = Keyword.get(opts, :worker_id, default_worker_id())

    with {:ok, execution} <- fetch_execution(execution_or_id, opts),
         {:ok, workflow_module} <- resolve_workflow_module(execution.workflow) do
      execute_execution(execution, workflow_module, worker_id, opts, false)
    else
      {:error, {:not_found, execution_id}} ->
        raise ArgumentError, "execution not found: #{inspect(execution_id)}"

      {:error, reason} ->
        raise RuntimeError, "failed to execute workflow: #{inspect(reason)}"
    end
  end

  @spec fetch_execution(execution_ref(), keyword()) ::
          {:ok, Executions.execution()} | {:error, term()}
  defp fetch_execution(%{id: id} = execution, opts) when is_binary(id) and is_list(opts) do
    input = Map.get(execution, :input, %{})
    queue = Map.get(execution, :queue, "default")
    workflow = Map.get(execution, :workflow)
    status = Map.get(execution, :status, :running)
    version = Map.get(execution, :version, "1")

    {:ok,
     %{id: id, queue: queue, workflow: workflow, input: input, status: status, version: version}}
  end

  defp fetch_execution(execution_id, opts) when is_binary(execution_id) and is_list(opts) do
    case Executions.get(execution_id, opts) do
      nil -> {:error, {:not_found, execution_id}}
      execution -> {:ok, execution}
    end
  end

  @spec resolve_workflow_module(module() | String.t() | nil) :: {:ok, module()} | {:error, term()}
  defp resolve_workflow_module(module) when is_atom(module) and not is_nil(module) do
    {:ok, module}
  end

  defp resolve_workflow_module("Elixir." <> _ = module_name) do
    try do
      {:ok, String.to_existing_atom(module_name)}
    rescue
      ArgumentError -> {:error, {:unknown_workflow_module, module_name}}
    end
  end

  defp resolve_workflow_module(module_name) when is_binary(module_name) do
    prefixed = "Elixir." <> module_name

    try do
      {:ok, String.to_existing_atom(prefixed)}
    rescue
      ArgumentError -> {:error, {:unknown_workflow_module, module_name}}
    end
  end

  defp resolve_workflow_module(other), do: {:error, {:invalid_workflow_module, other}}

  @spec load_history(Executions.execution(), keyword()) :: {:ok, [Events.event()]}
  defp load_history(execution, opts) do
    started_at = Telemetry.monotonic_time()
    history = Events.list(execution.id, opts)

    Telemetry.emit(
      [:executor, :history_loaded],
      %{duration_ms: Telemetry.duration_ms(started_at), event_count: length(history)},
      execution_metadata(opts, execution)
    )

    {:ok, history}
  end

  @spec run_workflow(
          module(),
          Executions.execution(),
          [Events.event()],
          String.t(),
          keyword()
        ) ::
          {:ok,
           {:completed, term()}
           | {:waiting, term()}
           | {:halt, :not_running | :waiting_persisted}
           | {:continue_as_new, map()}}
          | {:error, term(), binary()}
  defp run_workflow(workflow_module, execution, history, worker_id, opts) do
    task_results = task_results_from_history(history)
    task_sources = task_sources_from_history(history)
    {signal_queues, loaded_signal_seq} = signal_state_from_history(history)
    {child_states, loaded_child_seq} = child_state_from_history(history)

    {history_length, history_size_bytes, next_event_sequence} =
      initial_history_counters(execution, history)

    runtime = %{
      execution_id: execution.id,
      worker_id: worker_id,
      opts: opts,
      cached_ttl_ms: Keyword.get(opts, :cached_ttl_ms, :infinity),
      instance: Keyword.get(opts, :instance),
      queue: execution.queue,
      workflow: execution.workflow,
      version: execution.version,
      task_results: task_results,
      task_sources: task_sources,
      task_failures: task_failures_from_history(history),
      waits: waits_from_history(history),
      signal_queues: signal_queues,
      loaded_signal_seq: loaded_signal_seq,
      child_states: child_states,
      loaded_child_seq: loaded_child_seq,
      first_execution_id: first_execution_id_from_history(execution, history),
      history_length: history_length,
      history_size_bytes: history_size_bytes,
      next_event_sequence: next_event_sequence
    }

    Workflow.put_runtime(runtime)

    try do
      result = workflow_module.run(execution.version, execution.input)
      {:ok, {:completed, result}}
    rescue
      error ->
        {:error, {:exception, error, __STACKTRACE__}, execution.id}
    catch
      :throw, {:endurant_wait, wait_spec} ->
        {:ok, {:waiting, wait_spec}}

      :throw, {:endurant_halt, :not_running} ->
        {:ok, {:halt, :not_running}}

      :throw, {:endurant_halt, :waiting_persisted} ->
        {:ok, {:halt, :waiting_persisted}}

      :throw, {:endurant_continue_as_new, continue_as_new} ->
        {:ok, {:continue_as_new, continue_as_new}}

      kind, reason ->
        {:error, {:throw, kind, reason, __STACKTRACE__}, execution.id}
    after
      Workflow.delete_runtime()
    end
  end

  @spec execute_execution(Executions.execution(), module(), String.t(), keyword(), boolean()) ::
          execution_outcome()
  defp execute_execution(execution, workflow_module, worker_id, opts, already_started?) do
    opts = execution_runtime_opts(execution, workflow_module, opts)
    lease_ms = Keyword.get(opts, :lease_ms, 30_000)
    heartbeat_ms = heartbeat_interval_ms(opts, lease_ms)

    case maybe_start_execution(
           execution,
           worker_id,
           lease_ms,
           heartbeat_ms,
           opts,
           already_started?
         ) do
      {:ok, heartbeat_pid} ->
        try do
          case execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts) do
            {:continue_as_new, next_execution} ->
              notify_continued(execution.id, next_execution.id, opts)
              execute_execution(next_execution, workflow_module, worker_id, opts, true)

            outcome ->
              outcome
          end
        after
          stop_heartbeat_loop(heartbeat_pid)
        end

      {:error, :cancelled} ->
        cancel_execution(execution.id, opts)
        :cancelled

      {:error, :lock_lost} ->
        :lock_lost

      {:error, :not_found} ->
        :not_found
    end
  end

  @spec maybe_start_execution(
          Executions.execution(),
          String.t(),
          pos_integer(),
          pos_integer(),
          keyword(),
          boolean()
        ) ::
          {:ok, pid()} | {:error, :cancelled | :lock_lost | :not_found}
  defp maybe_start_execution(execution, worker_id, lease_ms, heartbeat_ms, opts, true) do
    start_heartbeat_for_execution(execution.id, worker_id, lease_ms, heartbeat_ms, opts)
  end

  defp maybe_start_execution(execution, worker_id, lease_ms, heartbeat_ms, opts, false) do
    case Executions.mark_started(execution.id, worker_id, opts) do
      :ok ->
        start_heartbeat_for_execution(execution.id, worker_id, lease_ms, heartbeat_ms, opts)

      {:error, :cancelled} ->
        {:error, :cancelled}

      {:error, :lock_lost} ->
        {:error, :lock_lost}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec start_heartbeat_for_execution(
          binary(),
          String.t(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) ::
          {:ok, pid()} | {:error, :cancelled | :lock_lost}
  defp start_heartbeat_for_execution(execution_id, worker_id, lease_ms, heartbeat_ms, opts) do
    case Executions.heartbeat(execution_id, worker_id, lease_ms, opts) do
      :ok ->
        {:ok, start_heartbeat_loop(execution_id, worker_id, lease_ms, heartbeat_ms, opts)}

      {:error, :cancelled} ->
        {:error, :cancelled}

      {:error, :lock_lost} ->
        {:error, :lock_lost}

      {:error, :transient_db} ->
        {:error, :lock_lost}
    end
  end

  @spec execute_loop(
          Executions.execution(),
          module(),
          String.t(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) :: execution_outcome() | {:continue_as_new, Executions.execution()}
  defp execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts) do
    case heartbeat_message() do
      :cancelled ->
        cancel_execution(execution.id, opts)
        :cancelled

      {:failed, reason} ->
        _ =
          Executions.mark_failed_owned(
            execution.id,
            worker_id,
            serialize_reason({:heartbeat_failed, reason}),
            opts
          )

        :failed

      :lock_lost ->
        :lock_lost

      :none ->
        execute_loop_continue(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts)
    end
  end

  @spec execute_loop_continue(
          Executions.execution(),
          module(),
          String.t(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) :: execution_outcome() | {:continue_as_new, Executions.execution()}
  defp execute_loop_continue(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts) do
    if Executions.cancellation_requested?(execution.id, opts) do
      cancel_execution(execution.id, opts)
      :cancelled
    else
      with {:ok, history} <- load_history(execution, opts),
           {:ok, outcome} <- run_workflow(workflow_module, execution, history, worker_id, opts) do
        case outcome do
          {:completed, result} ->
            if Executions.cancellation_requested?(execution.id, opts) do
              cancel_execution(execution.id, opts)
              :cancelled
            else
              _ = Executions.mark_completed_owned(execution.id, worker_id, result, opts)
              :completed
            end

          {:waiting, wait_spec} ->
            case handle_wait(execution.id, wait_spec, worker_id, heartbeat_ms, opts) do
              :ok ->
                execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts)

              :released ->
                :waiting

              {:error, :cancelled} ->
                cancel_execution(execution.id, opts)
                :cancelled

              {:error, {:heartbeat_failed, reason}} ->
                _ =
                  Executions.mark_failed_owned(
                    execution.id,
                    worker_id,
                    serialize_reason({:heartbeat_failed, reason}),
                    opts
                  )

                :failed

              {:error, :lock_lost} ->
                :lock_lost
            end

          {:halt, :not_running} ->
            if Executions.cancellation_requested?(execution.id, opts) do
              cancel_execution(execution.id, opts)
              :cancelled
            else
              :lock_lost
            end

          {:halt, :waiting_persisted} ->
            :waiting

          {:continue_as_new, continue_as_new} ->
            continue_as_new_execution(
              execution,
              worker_id,
              lease_ms,
              continue_as_new,
              opts
            )
        end
      else
        {:error, reason, execution_id} ->
          _ =
            Executions.mark_failed_owned(execution_id, worker_id, serialize_reason(reason), opts)

          :failed
      end
    end
  end

  @spec handle_wait(
          binary(),
          term(),
          String.t(),
          pos_integer(),
          keyword()
        ) ::
          :ok
          | :released
          | {:error, :cancelled}
          | {:error, :lock_lost}
          | {:error, {:heartbeat_failed, term()}}
  defp handle_wait(
         execution_id,
         {:time, delay_ms, wait_key},
         worker_id,
         heartbeat_ms,
         opts
       )
       when is_integer(delay_ms) and delay_ms > 0 and is_binary(wait_key) do
    run_at = DateTime.add(DateTime.utc_now(), delay_ms, :millisecond)

    case Executions.mark_waiting_with_time_event_owned(
           execution_id,
           worker_id,
           run_at,
           delay_ms,
           wait_key,
           opts
         ) do
      :ok -> :ok
      {:error, :not_running} -> {:error, :lock_lost}
    end
    |> case do
      :ok ->
        case notify_waiting(execution_id, opts) do
          :cache ->
            case wait_for_time(run_at, heartbeat_ms) do
              :ok ->
                notify_ready(execution_id, opts)
                await_resume_signal()

              :released ->
                :released

              {:error, _} = error ->
                error
            end

          :release ->
            case Executions.release_waiting_as_abandoned_owned(execution_id, worker_id, opts) do
              :ok -> :released
              {:error, :not_running} -> {:error, :lock_lost}
            end
        end

      {:error, :lock_lost} = error ->
        error
    end
  end

  @spec wait_for_time(DateTime.t(), pos_integer()) ::
          :ok
          | :released
          | {:error, :cancelled}
          | {:error, :lock_lost}
          | {:error, {:heartbeat_failed, term()}}
  defp wait_for_time(run_at, heartbeat_ms) do
    now = DateTime.utc_now()

    if DateTime.compare(now, run_at) == :lt do
      sleep_ms =
        run_at
        |> DateTime.diff(now, :millisecond)
        |> min(heartbeat_ms)
        |> max(1)

      receive do
        :cached_ttl ->
          :released

        :heartbeat_cancelled ->
          {:error, :cancelled}

        :heartbeat_lock_lost ->
          {:error, :lock_lost}

        {:heartbeat_failed, reason} ->
          {:error, {:heartbeat_failed, reason}}
      after
        sleep_ms ->
          wait_for_time(run_at, heartbeat_ms)
      end
    else
      :ok
    end
  end

  @spec notify_waiting(binary(), keyword()) :: :cache | :release
  defp notify_waiting(execution_id, opts) do
    manager = queue_manager!(opts)

    GenServer.call(
      manager,
      {:executor_cached, self(), execution_id, Keyword.get(opts, :cached_ttl_ms, :infinity)},
      5_000
    )
  end

  @spec notify_ready(binary(), keyword()) :: :ok
  defp notify_ready(execution_id, opts) do
    manager = queue_manager!(opts)
    _ = GenServer.call(manager, {:executor_ready, self(), execution_id}, 5_000)

    :ok
  end

  @spec queue_manager!(keyword()) :: pid() | atom() | {:via, module(), term()}
  defp queue_manager!(opts) do
    case Keyword.fetch(opts, :queue_manager) do
      {:ok, manager} ->
        manager

      :error ->
        raise ArgumentError, "missing required :queue_manager option"
    end
  end

  @spec await_resume_signal() ::
          :ok
          | :released
          | {:error, :cancelled}
          | {:error, :lock_lost}
          | {:error, {:heartbeat_failed, term()}}
  defp await_resume_signal do
    receive do
      :resume ->
        :ok

      :cached_ttl ->
        :released

      :heartbeat_cancelled ->
        {:error, :cancelled}

      :heartbeat_lock_lost ->
        {:error, :lock_lost}

      {:heartbeat_failed, reason} ->
        {:error, {:heartbeat_failed, reason}}
    end
  end

  @spec start_heartbeat_loop(binary(), String.t(), pos_integer(), pos_integer(), keyword()) ::
          pid()
  defp start_heartbeat_loop(execution_id, worker_id, lease_ms, heartbeat_ms, opts) do
    parent = self()

    spawn_link(fn ->
      heartbeat_loop(parent, execution_id, worker_id, lease_ms, heartbeat_ms, opts)
    end)
  end

  @spec stop_heartbeat_loop(pid()) :: :ok
  defp stop_heartbeat_loop(pid) when is_pid(pid) do
    ref = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      @heartbeat_stop_timeout ->
        Process.demonitor(ref, [:flush])
        :ok
    end
  end

  @spec heartbeat_loop(pid(), binary(), String.t(), pos_integer(), pos_integer(), keyword()) ::
          :ok
  defp heartbeat_loop(parent, execution_id, worker_id, lease_ms, heartbeat_ms, opts) do
    receive do
      :stop ->
        :ok
    after
      heartbeat_ms ->
        case Executions.heartbeat(execution_id, worker_id, lease_ms, opts) do
          :ok ->
            heartbeat_loop(parent, execution_id, worker_id, lease_ms, heartbeat_ms, opts)

          {:error, :cancelled} ->
            send(parent, :heartbeat_cancelled)
            :ok

          {:error, :lock_lost} ->
            send(parent, :heartbeat_lock_lost)
            :ok

          {:error, :transient_db} ->
            heartbeat_loop(parent, execution_id, worker_id, lease_ms, heartbeat_ms, opts)
        end
    end
  rescue
    error ->
      send(parent, {:heartbeat_failed, {:exception, error, __STACKTRACE__}})
      :ok
  catch
    kind, reason ->
      send(parent, {:heartbeat_failed, {kind, reason}})
      :ok
  end

  @spec heartbeat_message() :: :none | :cancelled | :lock_lost | {:failed, term()}
  defp heartbeat_message do
    receive do
      :heartbeat_cancelled -> :cancelled
      :heartbeat_lock_lost -> :lock_lost
      {:heartbeat_failed, reason} -> {:failed, reason}
    after
      0 -> :none
    end
  end

  @spec task_results_from_history([Events.event()]) :: %{optional(String.t()) => term()}
  defp task_results_from_history(events) do
    Enum.reduce(events, %{}, fn event, acc ->
      case event do
        %{type: :task_completed, payload: %{"task" => task, "result" => result}} ->
          Map.put(acc, task, result)

        %{type: :task_completed, payload: %{task: task, result: result}} ->
          Map.put(acc, task, result)

        _ ->
          acc
      end
    end)
  end

  @spec task_sources_from_history([Events.event()]) :: %{optional(String.t()) => :history}
  defp task_sources_from_history(events) do
    Enum.reduce(events, %{}, fn event, acc ->
      case event do
        %{type: :task_completed, payload: %{"task" => task}} when is_binary(task) ->
          Map.put(acc, task, :history)

        %{type: :task_completed, payload: %{task: task}} when is_binary(task) ->
          Map.put(acc, task, :history)

        _ ->
          acc
      end
    end)
  end

  @spec initial_history_counters(Executions.execution(), [Events.event()]) ::
          {non_neg_integer(), non_neg_integer(), pos_integer()}
  defp initial_history_counters(execution, history) do
    next_event_sequence =
      case Map.get(execution, :next_event_sequence) do
        value when is_integer(value) and value >= 1 -> value
        _ -> inferred_next_event_sequence(history)
      end

    history_size_bytes =
      case Map.get(execution, :history_size_bytes) do
        value when is_integer(value) and value >= 0 -> value
        _ -> 0
      end

    {next_event_sequence - 1, history_size_bytes, next_event_sequence}
  end

  @spec inferred_next_event_sequence([Events.event()]) :: pos_integer()
  defp inferred_next_event_sequence(events) do
    max_sequence =
      Enum.reduce(events, 0, fn
        %{sequence: sequence}, acc when is_integer(sequence) and sequence > acc -> sequence
        _, acc -> acc
      end)

    max_sequence + 1
  end

  @spec signal_state_from_history([Events.event()]) ::
          {%{optional(String.t()) => [term()]}, non_neg_integer()}
  defp signal_state_from_history(events) do
    Enum.reduce(events, {%{}, 0}, fn event, {queues, max_seq} ->
      seq =
        case event do
          %{sequence: value} when is_integer(value) and value > 0 -> value
          _ -> max_seq
        end

      updated_max_seq = max(max_seq, seq)

      case event do
        %{type: :signal_received, payload: %{"signal" => signal, "payload" => payload}} ->
          {enqueue_signal_payload(queues, signal, payload), updated_max_seq}

        %{type: :signal_received, payload: %{signal: signal, payload: payload}} ->
          {enqueue_signal_payload(queues, signal, payload), updated_max_seq}

        _ ->
          {queues, updated_max_seq}
      end
    end)
  end

  @spec waits_from_history([Events.event()]) :: MapSet.t(String.t())
  defp waits_from_history(events) do
    Enum.reduce(events, MapSet.new(), fn event, acc ->
      case event do
        %{type: :execution_waiting, payload: %{"mode" => mode, "wait_key" => wait_key}}
        when mode in [:time, "time"] and is_binary(wait_key) ->
          MapSet.put(acc, wait_key)

        %{type: :execution_waiting, payload: %{mode: :time, wait_key: wait_key}}
        when is_binary(wait_key) ->
          MapSet.put(acc, wait_key)

        _ ->
          acc
      end
    end)
  end

  @spec child_state_from_history([Events.event()]) ::
          {%{optional(String.t()) => map()}, non_neg_integer()}
  defp child_state_from_history(events) do
    Enum.reduce(events, {%{}, 0}, fn event, {states, max_seq} ->
      seq =
        case event do
          %{sequence: value} when is_integer(value) and value > 0 -> value
          _ -> max_seq
        end

      updated_max_seq = max(max_seq, seq)

      case event do
        %{type: :child_execution_started, payload: payload} ->
          case payload_value(payload, "child_key") do
            child_key when is_binary(child_key) ->
              state = %{
                status: :started,
                child_execution_id: payload_value(payload, "child_execution_id"),
                child_unique_id: payload_value(payload, "child_unique_id")
              }

              {Map.put(states, child_key, Map.merge(Map.get(states, child_key, %{}), state)),
               updated_max_seq}

            _ ->
              {states, updated_max_seq}
          end

        %{type: :child_execution_completed, payload: payload} ->
          case payload_value(payload, "child_key") do
            child_key when is_binary(child_key) ->
              state = %{
                status: :completed,
                result: payload_value(payload, "result"),
                child_execution_id: payload_value(payload, "child_execution_id"),
                child_unique_id: payload_value(payload, "child_unique_id")
              }

              {Map.put(states, child_key, Map.merge(Map.get(states, child_key, %{}), state)),
               updated_max_seq}

            _ ->
              {states, updated_max_seq}
          end

        %{type: :child_execution_failed, payload: payload} ->
          case payload_value(payload, "child_key") do
            child_key when is_binary(child_key) ->
              state = %{
                status: :failed,
                error: payload_value(payload, "error"),
                child_execution_id: payload_value(payload, "child_execution_id"),
                child_unique_id: payload_value(payload, "child_unique_id")
              }

              {Map.put(states, child_key, Map.merge(Map.get(states, child_key, %{}), state)),
               updated_max_seq}

            _ ->
              {states, updated_max_seq}
          end

        %{type: :child_execution_cancelled, payload: payload} ->
          case payload_value(payload, "child_key") do
            child_key when is_binary(child_key) ->
              state = %{
                status: :cancelled,
                child_execution_id: payload_value(payload, "child_execution_id"),
                child_unique_id: payload_value(payload, "child_unique_id")
              }

              {Map.put(states, child_key, Map.merge(Map.get(states, child_key, %{}), state)),
               updated_max_seq}

            _ ->
              {states, updated_max_seq}
          end

        _ ->
          {states, updated_max_seq}
      end
    end)
  end

  @spec task_failures_from_history([Events.event()]) :: %{
          optional(String.t()) => non_neg_integer()
        }
  defp task_failures_from_history(events) do
    Enum.reduce(events, %{}, fn event, acc ->
      case event do
        %{type: :task_failed, payload: %{"task" => task}} when is_binary(task) ->
          Map.update(acc, task, 1, &(&1 + 1))

        %{type: :task_failed, payload: %{task: task}} when is_binary(task) ->
          Map.update(acc, task, 1, &(&1 + 1))

        _ ->
          acc
      end
    end)
  end

  @spec payload_value(map(), String.t()) :: term()
  defp payload_value(payload, key) do
    Map.get(payload, key) || Map.get(payload, String.to_atom(key))
  end

  @spec enqueue_signal_payload(map(), term(), term()) :: map()
  defp enqueue_signal_payload(queues, signal, payload) do
    keys = signal_keys(signal)
    Enum.reduce(keys, queues, fn key, acc -> enqueue_for_key(acc, key, payload) end)
  end

  @spec signal_keys(String.t()) :: [String.t()]
  defp signal_keys(signal) when is_binary(signal), do: [signal]

  defp signal_keys(signal) do
    raise ArgumentError, "signal name must be a string, got: #{inspect(signal)}"
  end

  @spec enqueue_for_key(map(), String.t(), term()) :: map()
  defp enqueue_for_key(queues, key, payload) do
    Map.update(queues, key, :queue.in(payload, :queue.new()), fn queue ->
      :queue.in(payload, queue)
    end)
  end

  @spec first_execution_id_from_history(Executions.execution(), [Events.event()]) :: binary()
  defp first_execution_id_from_history(execution, events) do
    Enum.find_value(events, execution.id, fn event ->
      case event do
        %{type: :execution_started, payload: %{"first_execution_id" => first_execution_id}}
        when is_binary(first_execution_id) ->
          first_execution_id

        %{type: :execution_started, payload: %{first_execution_id: first_execution_id}}
        when is_binary(first_execution_id) ->
          first_execution_id

        _ ->
          nil
      end
    end)
  end

  @spec continue_as_new_execution(
          Executions.execution(),
          String.t(),
          pos_integer(),
          map(),
          keyword()
        ) :: {:continue_as_new, Executions.execution()} | :failed | :cancelled | :lock_lost
  defp continue_as_new_execution(
         execution,
         worker_id,
         lease_ms,
         continue_as_new,
         opts
       ) do
    case Executions.continue_as_new_owned(
           execution.id,
           worker_id,
           continue_as_new,
           lease_ms,
           opts
         ) do
      {:ok, next_execution} ->
        {:continue_as_new, next_execution}

      {:error, :cancelled} ->
        cancel_execution(execution.id, opts)
        :cancelled

      {:error, :not_running} ->
        :lock_lost

      {:error, reason} ->
        _ =
          Executions.mark_failed_owned(
            execution.id,
            worker_id,
            serialize_reason({:continue_as_new_failed, reason}),
            opts
          )

        :failed
    end
  end

  @spec serialize_reason(term()) :: map()
  defp serialize_reason({:exception, error, stacktrace}) do
    %{
      kind: :exception,
      module: inspect(error.__struct__),
      message: Exception.message(error),
      stacktrace: Exception.format_stacktrace(stacktrace)
    }
  end

  defp serialize_reason({:throw, kind, reason, stacktrace}) do
    %{
      kind: kind,
      reason: inspect(reason),
      stacktrace: Exception.format_stacktrace(stacktrace)
    }
  end

  defp serialize_reason(other), do: %{reason: inspect(other)}

  @spec cancel_execution(binary(), keyword()) :: :ok
  defp cancel_execution(execution_id, opts) do
    _ = Executions.request_cancel(execution_id, opts)
    _ = Executions.mark_cancelled(execution_id, opts)
    :ok
  end

  @spec notify_continued(binary(), binary(), keyword()) :: :ok
  defp notify_continued(old_execution_id, new_execution_id, opts) do
    manager = queue_manager!(opts)

    _ =
      GenServer.call(
        manager,
        {:executor_continued, self(), old_execution_id, new_execution_id},
        5_000
      )

    :ok
  end

  @spec execution_metadata(keyword(), Executions.execution(), map()) :: map()
  defp execution_metadata(opts, execution, extra \\ %{}) do
    Map.merge(
      %{
        instance: Keyword.get(opts, :instance),
        node: node(),
        queue: execution.queue,
        workflow: execution.workflow,
        version: execution.version
      },
      extra
    )
  end

  @spec default_worker_id() :: String.t()
  defp default_worker_id, do: "#{node()}:#{inspect(self())}"

  @spec heartbeat_interval_ms(keyword(), pos_integer()) :: pos_integer()
  defp heartbeat_interval_ms(opts, lease_ms) do
    Keyword.get(opts, :heartbeat_interval, max(div(lease_ms, 3), 1_000))
  end

  @spec execution_runtime_opts(Executions.execution(), module(), keyword()) :: keyword()
  defp execution_runtime_opts(execution, workflow_module, opts) do
    Keyword.put(
      opts,
      :cached_ttl_ms,
      resolve_cached_ttl_ms(execution, workflow_module, opts)
    )
  end

  @spec resolve_cached_ttl_ms(Executions.execution(), module(), keyword()) ::
          :infinity | pos_integer()
  defp resolve_cached_ttl_ms(execution, workflow_module, opts) do
    case cached_ttl_ms_from_metadata(Map.get(execution, :metadata, %{})) do
      nil ->
        workflow_module.__workflow__()
        |> Map.get(:cached_ttl_ms)
        |> case do
          nil -> Keyword.get(opts, :cached_ttl_ms, :infinity)
          value -> value
        end

      value ->
        value
    end
  end

  @spec cached_ttl_ms_from_metadata(map()) :: nil | :infinity | pos_integer()
  defp cached_ttl_ms_from_metadata(metadata) when is_map(metadata) do
    internal = metadata["endurant"] || metadata[:endurant]

    case internal do
      %{} = values ->
        case values["cached_ttl_ms"] || values[:cached_ttl_ms] do
          "infinity" -> :infinity
          value when is_integer(value) and value > 0 -> value
          _ -> nil
        end

      _ ->
        nil
    end
  end
end
