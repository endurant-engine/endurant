defmodule Endurant.Pruner do
  @moduledoc false

  use GenServer

  alias Endurant.Archivers
  alias Endurant.Settings
  alias Endurant.Telemetry

  @default_prefix "public"
  @default_setting_id "pruner"
  @default_lease_ms 30_000
  @default_heartbeat_ms 10_000
  @default_retry_ms 2_000
  @default_scan_ms 30_000
  @default_batch_size 100
  @default_retention_ms 30 * 24 * 60 * 60 * 1_000

  defstruct [
    :instance,
    :setting_id,
    :owner,
    :lease_ms,
    :heartbeat_ms,
    :retry_ms,
    :scan_ms,
    :batch_size,
    :retention_ms,
    :runtime_opts,
    :fence,
    active?: false
  ]

  @type state :: %__MODULE__{
          instance: atom() | String.t(),
          setting_id: String.t(),
          owner: String.t(),
          lease_ms: pos_integer(),
          heartbeat_ms: pos_integer(),
          retry_ms: pos_integer(),
          scan_ms: pos_integer(),
          batch_size: pos_integer(),
          retention_ms: pos_integer(),
          runtime_opts: keyword(),
          fence: non_neg_integer() | nil,
          active?: boolean()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    state = %__MODULE__{
      instance: instance,
      setting_id: Keyword.get(opts, :setting_id, @default_setting_id),
      owner: owner_id(instance),
      lease_ms: positive_integer(Keyword.get(opts, :lease_ms, @default_lease_ms), :lease_ms),
      heartbeat_ms:
        positive_integer(Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms), :heartbeat_ms),
      retry_ms: positive_integer(Keyword.get(opts, :retry_ms, @default_retry_ms), :retry_ms),
      scan_ms: positive_integer(Keyword.get(opts, :scan_ms, @default_scan_ms), :scan_ms),
      batch_size:
        positive_integer(Keyword.get(opts, :batch_size, @default_batch_size), :batch_size),
      retention_ms:
        positive_integer(Keyword.get(opts, :retention_ms, @default_retention_ms), :retention_ms),
      runtime_opts: [repo: repo, prefix: prefix, db_log: Keyword.get(opts, :db_log, false)]
    }

    {:ok, state, {:continue, :acquire}}
  end

  @impl true
  def handle_continue(:acquire, %__MODULE__{} = state) do
    {:noreply, try_acquire(state)}
  end

  @impl true
  def handle_info(:retry_acquire, %__MODULE__{} = state) do
    {:noreply, try_acquire(state)}
  end

  def handle_info(:heartbeat, %__MODULE__{active?: true} = state) do
    case Settings.heartbeat_lease(
           state.setting_id,
           state.owner,
           state.lease_ms,
           state.runtime_opts
         ) do
      :ok ->
        schedule(:heartbeat, state.heartbeat_ms)
        {:noreply, state}

      {:error, :lock_lost} ->
        schedule(:retry_acquire, state.retry_ms)
        {:noreply, %{state | active?: false, fence: nil}}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        {:noreply, %{state | active?: false, fence: nil}}
    end
  end

  def handle_info(:heartbeat, %__MODULE__{} = state) do
    schedule(:retry_acquire, state.retry_ms)
    {:noreply, state}
  end

  def handle_info(:scan, %__MODULE__{active?: true} = state) do
    _ = process_scan(state)
    schedule(:scan, state.scan_ms)
    {:noreply, state}
  end

  def handle_info(:scan, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %__MODULE__{active?: true} = state) do
    _ = Settings.release_lease(state.setting_id, state.owner, state.runtime_opts)
    :ok
  end

  def terminate(_reason, %__MODULE__{}) do
    :ok
  end

  @spec try_acquire(state()) :: state()
  defp try_acquire(%__MODULE__{} = state) do
    case Settings.claim_lease(state.setting_id, state.owner, state.lease_ms, state.runtime_opts) do
      {:ok, fence} ->
        schedule(:heartbeat, state.heartbeat_ms)
        schedule(:scan, 0)
        %{state | active?: true, fence: fence}

      :busy ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec process_scan(state()) :: :ok
  defp process_scan(%__MODULE__{} = state) do
    started_at = Telemetry.monotonic_time()

    {outcome, enabled_archivers} =
      case Archivers.enabled_names(state.runtime_opts) do
        archivers when is_list(archivers) and archivers != [] ->
          prune_batch(state, archivers)
          {:ok, length(archivers)}

        archivers when is_list(archivers) ->
          {:ok, length(archivers)}

        _ ->
          {:error, 0}
      end

    Telemetry.emit(
      [:pruner, :scan],
      %{
        duration_ms: Telemetry.duration_ms(started_at),
        enabled_archivers: enabled_archivers
      },
      pruner_metadata(state, %{outcome: outcome})
    )
  end

  @spec prune_batch(state(), [String.t()]) :: :ok
  defp prune_batch(%__MODULE__{} = state, enabled_archivers) do
    repo = Keyword.fetch!(state.runtime_opts, :repo)
    prefix = Keyword.fetch!(state.runtime_opts, :prefix)
    started_at = Telemetry.monotonic_time()

    cutoff =
      DateTime.utc_now()
      |> DateTime.add(-state.retention_ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    {pruned, deleted_events, deleted_deliveries} =
      Endurant.DB.transaction(
        repo,
        fn ->
          execution_ids =
            select_prunable_execution_ids(
              repo,
              prefix,
              enabled_archivers,
              cutoff,
              state.batch_size,
              state.runtime_opts
            )

          if execution_ids != [] do
            deleted_events = delete_events(repo, prefix, execution_ids, state.runtime_opts)

            deleted_deliveries =
              delete_deliveries(repo, prefix, execution_ids, state.runtime_opts)

            pruned = delete_executions(repo, prefix, execution_ids, state.runtime_opts)
            {pruned, deleted_events, deleted_deliveries}
          else
            {0, 0, 0}
          end
        end,
        state.runtime_opts,
        timeout: :infinity
      )
      |> case do
        {:ok, counts} -> counts
        _ -> {0, 0, 0}
      end

    Telemetry.emit(
      [:pruner, :batch],
      %{
        duration_ms: Telemetry.duration_ms(started_at),
        pruned: pruned,
        retention_ms: state.retention_ms,
        batch_size: state.batch_size
      },
      pruner_metadata(state, %{outcome: :ok})
    )

    if pruned > 0 or deleted_events > 0 or deleted_deliveries > 0 do
      Telemetry.emit(
        [:pruner, :delete],
        %{executions: pruned, events: deleted_events, deliveries: deleted_deliveries},
        pruner_metadata(state)
      )
    end

    :ok
  rescue
    DBConnection.ConnectionError -> :ok
  end

  @spec select_prunable_execution_ids(
          module(),
          String.t(),
          [String.t()],
          DateTime.t(),
          pos_integer(),
          keyword()
        ) ::
          [Ecto.UUID.t() | binary()]
  defp select_prunable_execution_ids(
         repo,
         prefix,
         enabled_archivers,
         cutoff,
         batch_size,
         runtime_opts
       ) do
    sql = """
    SELECT e.id
    FROM #{prefix}.endurant_executions e
    WHERE e.status IN (
      'completed'::#{prefix}.endurant_execution_status,
      'failed'::#{prefix}.endurant_execution_status,
      'cancelled'::#{prefix}.endurant_execution_status,
      'continued_as_new'::#{prefix}.endurant_execution_status
    )
    AND e.completed_at IS NOT NULL
    AND e.completed_at < $2
    AND (
      SELECT COUNT(DISTINCT d.backend)
      FROM #{prefix}.endurant_archive_deliveries d
      WHERE d.execution_id = e.id
      AND d.backend = ANY($1)
    ) = $3
    ORDER BY e.completed_at ASC, e.id ASC
    LIMIT $4
    FOR UPDATE OF e SKIP LOCKED
    """

    Endurant.DB.query!(
      repo,
      sql,
      [enabled_archivers, cutoff, length(enabled_archivers), batch_size],
      runtime_opts
    ).rows
    |> Enum.map(fn [execution_id] -> execution_id end)
  end

  @spec delete_events(module(), String.t(), [Ecto.UUID.t() | binary()], keyword()) ::
          non_neg_integer()
  defp delete_events(repo, prefix, execution_ids, runtime_opts) do
    sql = "DELETE FROM #{prefix}.endurant_events WHERE execution_id = ANY($1)"
    Endurant.DB.query!(repo, sql, [execution_ids], runtime_opts).num_rows
  end

  @spec delete_deliveries(module(), String.t(), [Ecto.UUID.t() | binary()], keyword()) ::
          non_neg_integer()
  defp delete_deliveries(repo, prefix, execution_ids, runtime_opts) do
    sql = "DELETE FROM #{prefix}.endurant_archive_deliveries WHERE execution_id = ANY($1)"
    Endurant.DB.query!(repo, sql, [execution_ids], runtime_opts).num_rows
  end

  @spec delete_executions(module(), String.t(), [Ecto.UUID.t() | binary()], keyword()) ::
          non_neg_integer()
  defp delete_executions(repo, prefix, execution_ids, runtime_opts) do
    sql = "DELETE FROM #{prefix}.endurant_executions WHERE id = ANY($1)"
    Endurant.DB.query!(repo, sql, [execution_ids], runtime_opts).num_rows
  end

  @spec owner_id(atom() | String.t()) :: String.t()
  defp owner_id(instance) do
    "#{node()}:#{instance}:pruner:#{System.unique_integer([:positive])}"
  end

  @spec schedule(term(), non_neg_integer()) :: reference()
  defp schedule(message, delay_ms) do
    Process.send_after(self(), message, delay_ms)
  end

  @spec pruner_metadata(state(), map()) :: map()
  defp pruner_metadata(state, extra \\ %{}) do
    Map.merge(
      %{
        instance: state.instance,
        node: node()
      },
      extra
    )
  end

  @spec positive_integer(term(), atom()) :: pos_integer()
  defp positive_integer(value, _field) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, field) do
    raise ArgumentError,
          "#{inspect(field)} must be a positive integer, got: #{inspect(value)}"
  end
end
