defmodule Endurant.QueueManager do
  @moduledoc false

  use GenServer

  defstruct [
    :queue,
    :opts,
    running: %{},
    cached: %{},
    ready: :queue.new()
  ]

  @type state :: %__MODULE__{
          queue: atom(),
          opts: keyword(),
          running: %{reference() => %{execution_id: term(), pid: pid()}},
          cached: %{reference() => %{execution_id: term(), pid: pid(), ready?: boolean()}},
          ready: :queue.queue(reference())
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      queue: Keyword.fetch!(opts, :queue),
      opts: Keyword.get(opts, :opts, [])
    }

    send(self(), :tick)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    recover_opts = Keyword.put(state.opts, :queue, state.queue)
    _ = Endurant.Executions.recover_expired_locks(recovery_limit(state.opts), recover_opts)
    state = promote_db_ready_waiters(state)
    worker_id = worker_id(state.queue)
    {state, _resumed} = resume_ready_waiters(state, worker_id)

    capacity_after_resume = max(concurrency(state.opts) - map_size(state.running), 0)

    ready_waiting =
      if capacity_after_resume > 0 do
        Endurant.Executions.claim_ready_waiting(
          state.queue,
          capacity_after_resume,
          worker_id,
          lease_ms(state.opts),
          state.opts
        )
      else
        []
      end

    running_after_waiting =
      spawn_executions(ready_waiting, state.running, worker_id, self(), state.opts)

    capacity = max(concurrency(state.opts) - map_size(running_after_waiting), 0)

    executions =
      if capacity > 0 do
        Endurant.Executions.claim_pending(
          state.queue,
          capacity,
          worker_id,
          lease_ms(state.opts),
          state.opts
        )
      else
        []
      end

    running = spawn_executions(executions, running_after_waiting, worker_id, self(), state.opts)

    state = Map.put(state, :running, running)

    Process.send_after(self(), :tick, poll_interval(state.opts))
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply,
     %{state | running: Map.delete(state.running, ref), cached: Map.delete(state.cached, ref)}}
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_call({:executor_cached, pid, execution_id}, _from, %__MODULE__{} = state) do
    if map_size(state.cached) < cached_limit(state.opts) do
      {:reply, :cache, move_running_to_cached(state, pid, execution_id)}
    else
      {:reply, :release, remove_from_running(state, pid, execution_id)}
    end
  end

  def handle_call({:executor_ready, pid, execution_id}, _from, %__MODULE__{} = state) do
    {:reply, :ok, mark_cached_ready(state, pid, execution_id)}
  end

  @spec spawn_executions(list(term()), map(), String.t(), pid(), keyword()) :: map()
  defp spawn_executions(executions, running, worker_id, manager, opts) do
    Enum.reduce(executions, running, fn execution, acc ->
      exec_opts =
        Keyword.merge(
          opts,
          worker_id: worker_id,
          lease_ms: lease_ms(opts),
          queue_manager: manager
        )

      {pid, ref} =
        :erlang.spawn_opt(
          fn -> Endurant.Executor.run(execution, exec_opts) end,
          [:link, :monitor]
        )

      Map.put(acc, ref, %{execution_id: Map.get(execution, :id), pid: pid})
    end)
  end

  @spec move_running_to_cached(state(), pid(), term()) :: state()
  defp move_running_to_cached(%__MODULE__{} = state, pid, execution_id) do
    match =
      Enum.find(state.running, fn {_ref, info} ->
        info.pid == pid
      end)

    case match do
      {ref, %{execution_id: ^execution_id} = info} ->
        running = Map.delete(state.running, ref)

        cached_info =
          info
          |> Map.put(:execution_id, execution_id)
          |> Map.put(:ready?, false)

        cached = Map.put(state.cached, ref, cached_info)
        %{state | running: running, cached: cached}

      {_ref, _info} ->
        state

      nil ->
        state
    end
  end

  @spec remove_from_running(state(), pid(), term()) :: state()
  defp remove_from_running(%__MODULE__{} = state, pid, execution_id) do
    match =
      Enum.find(state.running, fn {_ref, info} ->
        info.pid == pid
      end)

    case match do
      {ref, %{execution_id: ^execution_id}} ->
        %{state | running: Map.delete(state.running, ref)}

      {_ref, _info} ->
        state

      nil ->
        state
    end
  end

  @spec mark_cached_ready(state(), pid(), term()) :: state()
  defp mark_cached_ready(%__MODULE__{} = state, pid, execution_id) do
    match =
      Enum.find(state.cached, fn {_ref, info} ->
        info.pid == pid
      end)

    case match do
      {ref, %{execution_id: ^execution_id} = info} ->
        if info.ready? do
          state
        else
          cached = Map.put(state.cached, ref, %{info | ready?: true})
          ready = :queue.in(ref, state.ready)
          %{state | cached: cached, ready: ready}
        end

      {_ref, _info} ->
        state

      nil ->
        state
    end
  end

  @spec promote_db_ready_waiters(state()) :: state()
  defp promote_db_ready_waiters(%__MODULE__{} = state) do
    pending_refs =
      state.cached
      |> Enum.filter(fn {_ref, info} -> not info.ready? end)
      |> Enum.map(fn {ref, _info} -> ref end)

    execution_ids =
      pending_refs
      |> Enum.map(fn ref -> state.cached[ref].execution_id end)
      |> Enum.uniq()

    ready_ids = Endurant.Executions.ready_for_resume_many(execution_ids, state.opts)

    {cached, ready} =
      Enum.reduce(pending_refs, {state.cached, state.ready}, fn ref, {cached_acc, ready_acc} ->
        case Map.get(cached_acc, ref) do
          %{execution_id: execution_id} = current ->
            if MapSet.member?(ready_ids, execution_id) do
              {Map.put(cached_acc, ref, %{current | ready?: true}), :queue.in(ref, ready_acc)}
            else
              {cached_acc, ready_acc}
            end

          _ ->
            {cached_acc, ready_acc}
        end
      end)

    %{state | cached: cached, ready: ready}
  end

  @spec resume_ready_waiters(state(), String.t()) :: {state(), non_neg_integer()}
  defp resume_ready_waiters(%__MODULE__{} = state, worker_id) do
    capacity = max(concurrency(state.opts) - map_size(state.running), 0)
    do_resume(state, worker_id, capacity, 0)
  end

  @spec do_resume(state(), String.t(), non_neg_integer(), non_neg_integer()) ::
          {state(), non_neg_integer()}
  defp do_resume(state, _worker_id, 0, resumed), do: {state, resumed}

  defp do_resume(%__MODULE__{} = state, worker_id, remaining, resumed) do
    case :queue.out(state.ready) do
      {{:value, ref}, ready_queue} ->
        case Map.pop(state.cached, ref) do
          {nil, cached_after_pop} ->
            do_resume(
              %{state | cached: cached_after_pop, ready: ready_queue},
              worker_id,
              remaining,
              resumed
            )

          {info, cached_after_pop} ->
            case Endurant.Executions.mark_running(
                   info.execution_id,
                   worker_id,
                   lease_ms(state.opts),
                   state.opts
                 ) do
              :ok ->
                send(info.pid, :resume)
                running = Map.put(state.running, ref, Map.take(info, [:execution_id, :pid]))

                do_resume(
                  %{state | running: running, cached: cached_after_pop, ready: ready_queue},
                  worker_id,
                  remaining - 1,
                  resumed + 1
                )

              {:error, :cancelled} ->
                do_resume(
                  %{state | cached: cached_after_pop, ready: ready_queue},
                  worker_id,
                  remaining,
                  resumed
                )

              {:error, :lock_expired} ->
                do_resume(
                  %{state | cached: cached_after_pop, ready: ready_queue},
                  worker_id,
                  remaining,
                  resumed
                )

              {:error, :not_found} ->
                do_resume(
                  %{state | cached: cached_after_pop, ready: ready_queue},
                  worker_id,
                  remaining,
                  resumed
                )
            end
        end

      {:empty, _queue} ->
        {state, resumed}
    end
  end

  @spec poll_interval(keyword()) :: pos_integer()
  defp poll_interval(opts), do: Keyword.get(opts, :poll_interval, 1_000)

  @spec concurrency(keyword()) :: pos_integer()
  defp concurrency(opts), do: Keyword.get(opts, :concurrency, 1)

  @spec cached_limit(keyword()) :: pos_integer()
  defp cached_limit(opts), do: Keyword.get(opts, :cached_limit, 1_000)

  @spec lease_ms(keyword()) :: pos_integer()
  defp lease_ms(opts), do: Keyword.get(opts, :lease_ms, 30_000)

  @spec recovery_limit(keyword()) :: pos_integer()
  defp recovery_limit(opts), do: Keyword.get(opts, :recovery_limit, 100)

  @spec worker_id(atom()) :: String.t()
  defp worker_id(queue), do: "#{node()}:#{queue}"
end
