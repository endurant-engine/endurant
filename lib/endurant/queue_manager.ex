defmodule Endurant.QueueManager do
  @moduledoc false

  use GenServer

  alias Endurant.Registry
  alias Endurant.Telemetry

  defstruct [
    :instance,
    :queue,
    :opts,
    tick: 0,
    running: %{},
    parked: %{},
    ready: :queue.new()
  ]

  @type state :: %__MODULE__{
          instance: atom() | String.t(),
          queue: atom(),
          opts: keyword(),
          tick: non_neg_integer(),
          running: %{reference() => %{execution_id: term(), pid: pid()}},
          parked: %{reference() => %{execution_id: term(), pid: pid(), ready?: boolean()}},
          ready: :queue.queue(reference())
        }

  @type claim_branch :: :continuable | :waiting_ready
  @type recover_branch :: :running | :continuable | :waiting_ready

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    instance = Keyword.fetch!(opts, :instance)
    queue = Keyword.fetch!(opts, :queue)

    state = %__MODULE__{
      instance: instance,
      queue: queue,
      opts: Keyword.get(opts, :opts, [])
    }

    :ok = Registry.put_queue_manager(instance, queue, self())
    send(self(), :tick)
    {:ok, state}
  end

  @impl true
  def terminate(_reason, %__MODULE__{instance: instance, queue: queue}) do
    Registry.delete_queue_manager(instance, queue, self())
    :ok
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    started_at = Telemetry.monotonic_time()
    recover_opts =
      state.opts
      |> Keyword.put(:queue, state.queue)
      |> Keyword.put(:recover_order, recover_order_for_tick(state.tick))

    recovered = Endurant.Executions.recover_expired_locks(recovery_limit(state.opts), recover_opts)
    state = promote_db_ready_waiters(state)
    worker_id = worker_id(state.instance, state.queue)
    {state, resumed} = resume_ready_waiters(state, worker_id)

    capacity_after_resume = max(limit(state.opts) - map_size(state.running), 0)

    ready_waiting =
      if capacity_after_resume > 0 do
        claim_opts = Keyword.put(state.opts, :claim_order, claim_order_for_tick(state.tick))

        Endurant.Executions.claim_ready_waiting(
          state.queue,
          capacity_after_resume,
          worker_id,
          lease_ms(state.opts),
          claim_opts
        )
      else
        []
      end

    running_after_waiting =
      spawn_executions(ready_waiting, state.running, worker_id, self(), state.opts)

    capacity = max(limit(state.opts) - map_size(running_after_waiting), 0)

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

    state =
      state
      |> Map.put(:running, running)
      |> Map.update!(:tick, &(&1 + 1))

    Telemetry.emit(
      [:queue_manager, :tick],
      %{
        duration_ms: Telemetry.duration_ms(started_at),
        running: map_size(state.running),
        parked: map_size(state.parked),
        ready: :queue.len(state.ready),
        claimed_pending: length(executions),
        claimed_ready: length(ready_waiting),
        resumed: resumed,
        recovered: recovered
      },
      queue_manager_metadata(state)
    )

    Process.send_after(self(), :tick, poll_interval(state.opts))
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply,
     %{state | running: Map.delete(state.running, ref), parked: Map.delete(state.parked, ref)}}
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_call({:executor_parked, pid, execution_id}, _from, %__MODULE__{} = state) do
    if map_size(state.parked) < parked_limit(state.opts) do
      next_state = move_running_to_parked(state, pid, execution_id)

      Telemetry.emit(
        [:queue_manager, :parked],
        %{count: 1, running: map_size(next_state.running), parked: map_size(next_state.parked)},
        queue_manager_metadata(next_state, %{decision: :park})
      )

      {:reply, :park, next_state}
    else
      next_state = remove_from_running(state, pid, execution_id)

      Telemetry.emit(
        [:queue_manager, :parked],
        %{count: 1, running: map_size(next_state.running), parked: map_size(next_state.parked)},
        queue_manager_metadata(next_state, %{decision: :release})
      )

      {:reply, :release, next_state}
    end
  end

  def handle_call({:executor_ready, pid, execution_id}, _from, %__MODULE__{} = state) do
    next_state = mark_parked_ready(state, pid, execution_id)

    Telemetry.emit(
      [:queue_manager, :ready],
      %{count: 1, ready: :queue.len(next_state.ready)},
      queue_manager_metadata(next_state)
    )

    {:reply, :ok, next_state}
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

  @spec move_running_to_parked(state(), pid(), term()) :: state()
  defp move_running_to_parked(%__MODULE__{} = state, pid, execution_id) do
    match =
      Enum.find(state.running, fn {_ref, info} ->
        info.pid == pid
      end)

    case match do
      {ref, %{execution_id: ^execution_id} = info} ->
        running = Map.delete(state.running, ref)

        parked_info =
          info
          |> Map.put(:execution_id, execution_id)
          |> Map.put(:ready?, false)

        parked = Map.put(state.parked, ref, parked_info)
        %{state | running: running, parked: parked}

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

  @spec mark_parked_ready(state(), pid(), term()) :: state()
  defp mark_parked_ready(%__MODULE__{} = state, pid, execution_id) do
    match =
      Enum.find(state.parked, fn {_ref, info} ->
        info.pid == pid
      end)

    case match do
      {ref, %{execution_id: ^execution_id} = info} ->
        if info.ready? do
          state
        else
          parked = Map.put(state.parked, ref, %{info | ready?: true})
          ready = :queue.in(ref, state.ready)
          %{state | parked: parked, ready: ready}
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
      state.parked
      |> Enum.filter(fn {_ref, info} -> not info.ready? end)
      |> Enum.map(fn {ref, _info} -> ref end)

    execution_ids =
      pending_refs
      |> Enum.map(fn ref -> state.parked[ref].execution_id end)
      |> Enum.uniq()

    ready_ids = Endurant.Executions.ready_for_resume_many(execution_ids, state.opts)

    {parked, ready} =
      Enum.reduce(pending_refs, {state.parked, state.ready}, fn ref, {parked_acc, ready_acc} ->
        case Map.get(parked_acc, ref) do
          %{execution_id: execution_id} = current ->
            if MapSet.member?(ready_ids, execution_id) do
              {Map.put(parked_acc, ref, %{current | ready?: true}), :queue.in(ref, ready_acc)}
            else
              {parked_acc, ready_acc}
            end

          _ ->
            {parked_acc, ready_acc}
        end
      end)

    %{state | parked: parked, ready: ready}
  end

  @spec resume_ready_waiters(state(), String.t()) :: {state(), non_neg_integer()}
  defp resume_ready_waiters(%__MODULE__{} = state, worker_id) do
    capacity = max(limit(state.opts) - map_size(state.running), 0)
    do_resume(state, worker_id, capacity, 0)
  end

  @spec do_resume(state(), String.t(), non_neg_integer(), non_neg_integer()) ::
          {state(), non_neg_integer()}
  defp do_resume(state, _worker_id, 0, resumed), do: {state, resumed}

  defp do_resume(%__MODULE__{} = state, worker_id, remaining, resumed) do
    case :queue.out(state.ready) do
      {{:value, ref}, ready_queue} ->
        case Map.pop(state.parked, ref) do
          {nil, parked_after_pop} ->
            do_resume(
              %{state | parked: parked_after_pop, ready: ready_queue},
              worker_id,
              remaining,
              resumed
            )

          {info, parked_after_pop} ->
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
                  %{state | running: running, parked: parked_after_pop, ready: ready_queue},
                  worker_id,
                  remaining - 1,
                  resumed + 1
                )

              {:error, :cancelled} ->
                do_resume(
                  %{state | parked: parked_after_pop, ready: ready_queue},
                  worker_id,
                  remaining,
                  resumed
                )

              {:error, :lock_expired} ->
                do_resume(
                  %{state | parked: parked_after_pop, ready: ready_queue},
                  worker_id,
                  remaining,
                  resumed
                )

              {:error, :not_found} ->
                do_resume(
                  %{state | parked: parked_after_pop, ready: ready_queue},
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

  @spec limit(keyword()) :: pos_integer()
  defp limit(opts), do: Keyword.get(opts, :limit, 1)

  @spec parked_limit(keyword()) :: pos_integer()
  defp parked_limit(opts), do: Keyword.get(opts, :parked_limit, 1_000)

  @spec lease_ms(keyword()) :: pos_integer()
  defp lease_ms(opts), do: Keyword.get(opts, :lease_ms, 30_000)

  @spec recovery_limit(keyword()) :: pos_integer()
  defp recovery_limit(opts), do: Keyword.get(opts, :recovery_limit, 100)

  @spec claim_order_for_tick(non_neg_integer()) :: [claim_branch()]
  defp claim_order_for_tick(tick) do
    if rem(tick, 2) == 0 do
      [:continuable, :waiting_ready]
    else
      [:waiting_ready, :continuable]
    end
  end

  @spec recover_order_for_tick(non_neg_integer()) :: [recover_branch()]
  defp recover_order_for_tick(tick) do
    case rem(tick, 3) do
      0 -> [:running, :continuable, :waiting_ready]
      1 -> [:continuable, :waiting_ready, :running]
      _ -> [:waiting_ready, :running, :continuable]
    end
  end

  @spec worker_id(atom() | String.t(), atom()) :: String.t()
  defp worker_id(instance, queue), do: "#{instance_tag(instance)}:#{node()}:#{queue}"

  @spec instance_tag(atom() | String.t()) :: String.t()
  defp instance_tag(instance) when is_binary(instance), do: instance
  defp instance_tag(instance) when is_atom(instance), do: inspect(instance)

  @spec queue_manager_metadata(state(), map()) :: map()
  defp queue_manager_metadata(state, extra \\ %{}) do
    Map.merge(
      %{
        instance: state.instance,
        node: node(),
        queue: state.queue
      },
      extra
    )
  end
end
