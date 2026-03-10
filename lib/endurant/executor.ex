defmodule Endurant.Executor do
  @moduledoc false

  alias Endurant.Events
  alias Endurant.Executions
  alias Endurant.Workflow

  @type execution_ref :: map() | binary()
  @heartbeat_stop_timeout 1_000

  @spec run(execution_ref(), keyword()) :: :ok
  def run(execution_or_id, opts \\ []) do
    _ = queue_manager!(opts)
    worker_id = Keyword.get(opts, :worker_id, default_worker_id())

    with {:ok, execution} <- fetch_execution(execution_or_id, opts),
         {:ok, workflow_module} <- resolve_workflow_module(execution.workflow) do
      lease_ms = Keyword.get(opts, :lease_ms, 30_000)
      heartbeat_ms = heartbeat_interval_ms(opts, lease_ms)

      case Executions.mark_started(execution.id, worker_id, opts) do
        :ok ->
          case Executions.heartbeat(execution.id, worker_id, lease_ms, opts) do
            :ok ->
              heartbeat_pid =
                start_heartbeat_loop(execution.id, worker_id, lease_ms, heartbeat_ms, opts)

              try do
                execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts)
              after
                stop_heartbeat_loop(heartbeat_pid)
              end

            {:error, :cancelled} ->
              cancel_execution(execution.id, opts)

            {:error, :lock_lost} ->
              :ok

            {:error, :transient_db} ->
              :ok
          end

        {:error, :cancelled} ->
          cancel_execution(execution.id, opts)

        {:error, :lock_lost} ->
          :ok

        {:error, :not_found} ->
          :ok
      end
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
    workflow = Map.get(execution, :workflow)
    status = Map.get(execution, :status, :running)
    version = Map.get(execution, :version, "1")

    {:ok, %{id: id, workflow: workflow, input: input, status: status, version: version}}
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

  @spec load_history(binary(), keyword()) :: {:ok, [Events.event()]}
  defp load_history(execution_id, opts) do
    {:ok, Events.list(execution_id, opts)}
  end

  @spec run_workflow(
          module(),
          Executions.execution(),
          [Events.event()],
          String.t(),
          keyword()
        ) ::
          {:ok,
           {:completed, term()} | {:waiting, term()} | {:halt, :not_running | :waiting_persisted}}
          | {:error, term(), binary()}
  defp run_workflow(workflow_module, execution, history, worker_id, opts) do
    task_results = task_results_from_history(history)
    {signal_queues, loaded_signal_seq} = signal_state_from_history(history)

    runtime = %{
      execution_id: execution.id,
      worker_id: worker_id,
      opts: opts,
      task_results: task_results,
      task_failures: task_failures_from_history(history),
      waits: waits_from_history(history),
      signal_queues: signal_queues,
      loaded_signal_seq: loaded_signal_seq
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

      kind, reason ->
        {:error, {:throw, kind, reason, __STACKTRACE__}, execution.id}
    after
      Workflow.delete_runtime()
    end
  end

  @spec execute_loop(
          Executions.execution(),
          module(),
          String.t(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) :: :ok
  defp execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts) do
    case heartbeat_message() do
      :cancelled ->
        cancel_execution(execution.id, opts)

      {:failed, reason} ->
        _ =
          Executions.mark_failed_owned(
            execution.id,
            worker_id,
            serialize_reason({:heartbeat_failed, reason}),
            opts
          )

      :lock_lost ->
        :ok

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
        ) :: :ok
  defp execute_loop_continue(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts) do
    if Executions.cancellation_requested?(execution.id, opts) do
      cancel_execution(execution.id, opts)
    else
      with {:ok, history} <- load_history(execution.id, opts),
           {:ok, outcome} <- run_workflow(workflow_module, execution, history, worker_id, opts) do
        case outcome do
          {:completed, result} ->
            if Executions.cancellation_requested?(execution.id, opts) do
              cancel_execution(execution.id, opts)
            else
              _ = Executions.mark_completed_owned(execution.id, worker_id, result, opts)
            end

          {:waiting, wait_spec} ->
            case handle_wait(execution.id, wait_spec, worker_id, heartbeat_ms, opts) do
              :ok ->
                execute_loop(execution, workflow_module, worker_id, lease_ms, heartbeat_ms, opts)

              :released ->
                :ok

              {:error, :cancelled} ->
                cancel_execution(execution.id, opts)

              {:error, {:heartbeat_failed, reason}} ->
                _ =
                  Executions.mark_failed_owned(
                    execution.id,
                    worker_id,
                    serialize_reason({:heartbeat_failed, reason}),
                    opts
                  )

              {:error, :lock_lost} ->
                :ok
            end

          {:halt, :not_running} ->
            if Executions.cancellation_requested?(execution.id, opts) do
              cancel_execution(execution.id, opts)
            else
              :ok
            end

          {:halt, :waiting_persisted} ->
            :ok
        end
      else
        {:error, reason, execution_id} ->
          _ =
            Executions.mark_failed_owned(execution_id, worker_id, serialize_reason(reason), opts)
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
          :park ->
            wait_for_time(run_at, heartbeat_ms)
            notify_ready(execution_id, opts)
            await_resume_signal()

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

  @spec wait_for_time(DateTime.t(), pos_integer()) :: :ok | {:error, :cancelled}
  defp wait_for_time(run_at, heartbeat_ms) do
    now = DateTime.utc_now()

    if DateTime.compare(now, run_at) == :lt do
      sleep_ms =
        run_at
        |> DateTime.diff(now, :millisecond)
        |> min(heartbeat_ms)
        |> max(1)

      Process.sleep(sleep_ms)
      wait_for_time(run_at, heartbeat_ms)
    else
      :ok
    end
  end

  @spec notify_waiting(binary(), keyword()) :: :park | :release
  defp notify_waiting(execution_id, opts) do
    manager = queue_manager!(opts)
    GenServer.call(manager, {:executor_parked, self(), execution_id}, 5_000)
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
          | {:error, :cancelled}
          | {:error, :lock_lost}
          | {:error, {:heartbeat_failed, term()}}
  defp await_resume_signal do
    receive do
      :resume ->
        :ok

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

  @spec default_worker_id() :: String.t()
  defp default_worker_id, do: "#{node()}:#{inspect(self())}"

  @spec heartbeat_interval_ms(keyword(), pos_integer()) :: pos_integer()
  defp heartbeat_interval_ms(opts, lease_ms) do
    Keyword.get(opts, :heartbeat_interval, max(div(lease_ms, 3), 1_000))
  end
end
