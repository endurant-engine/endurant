defmodule Endurant.ArchiveWorker do
  @moduledoc false

  use GenServer

  alias Endurant.Archiver
  alias Endurant.Archivers
  alias Endurant.Migrations.Postgres
  alias Endurant.Settings
  alias Endurant.Telemetry

  @default_prefix "public"
  @default_lease_ms 30_000
  @default_heartbeat_ms 10_000
  @default_retry_ms 2_000
  @default_scan_ms 5_000
  @default_batch_size 100

  defstruct [
    :instance,
    :archiver,
    :setting_id,
    :owner,
    :lease_ms,
    :heartbeat_ms,
    :retry_ms,
    :scan_ms,
    :batch_size,
    :archiver_module,
    :archiver_opts,
    :runtime_opts,
    :fence,
    active?: false
  ]

  @type state :: %__MODULE__{
          instance: atom() | String.t(),
          archiver: String.t(),
          setting_id: String.t(),
          owner: String.t(),
          lease_ms: pos_integer(),
          heartbeat_ms: pos_integer(),
          retry_ms: pos_integer(),
          scan_ms: pos_integer(),
          batch_size: pos_integer(),
          archiver_module: module() | nil,
          archiver_opts: keyword(),
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
    archiver = opts |> Keyword.fetch!(:archiver) |> normalize_archiver!()
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    lease_ms = positive_integer(Keyword.get(opts, :lease_ms, @default_lease_ms), :lease_ms)

    heartbeat_ms =
      positive_integer(Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms), :heartbeat_ms)

    retry_ms = positive_integer(Keyword.get(opts, :retry_ms, @default_retry_ms), :retry_ms)
    scan_ms = positive_integer(Keyword.get(opts, :scan_ms, @default_scan_ms), :scan_ms)

    batch_size =
      positive_integer(Keyword.get(opts, :batch_size, @default_batch_size), :batch_size)

    archiver_module = validate_archiver_module!(Keyword.get(opts, :archiver_module))
    archiver_opts = Keyword.get(opts, :archiver_opts, [])

    state = %__MODULE__{
      instance: instance,
      archiver: archiver,
      setting_id: Keyword.get(opts, :setting_id, Archivers.setting_id(archiver)),
      owner: owner_id(instance, archiver),
      lease_ms: lease_ms,
      heartbeat_ms: heartbeat_ms,
      retry_ms: retry_ms,
      scan_ms: scan_ms,
      batch_size: batch_size,
      archiver_module: archiver_module,
      archiver_opts: archiver_opts,
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
    case Archivers.get(state.archiver, state.runtime_opts) do
      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}

      nil ->
        ensure_setting_and_acquire(state)

      %{"enabled" => true} ->
        acquire_after_ensure(state)

      %{"enabled" => "true"} ->
        acquire_after_ensure(state)

      %{} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec ensure_setting_and_acquire(state()) :: state()
  defp ensure_setting_and_acquire(%__MODULE__{} = state) do
    case Archivers.put(state.archiver, %{"enabled" => false}, state.runtime_opts) do
      :ok ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec acquire_after_ensure(state()) :: state()
  defp acquire_after_ensure(%__MODULE__{} = state) do
    case Settings.claim_lease(state.setting_id, state.owner, state.lease_ms, state.runtime_opts) do
      {:ok, fence} ->
        case initialize_archiver(state) do
          :ok ->
            schedule(:heartbeat, state.heartbeat_ms)
            schedule(:scan, 0)
            %{state | active?: true, fence: fence}

          {:error, _reason} ->
            _ = Settings.release_lease(state.setting_id, state.owner, state.runtime_opts)
            schedule(:retry_acquire, state.retry_ms)
            %{state | active?: false, fence: nil}
        end

      :busy ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec initialize_archiver(state()) :: :ok | {:error, term()}
  defp initialize_archiver(%__MODULE__{} = state) do
    with {:ok, module} <- archiver_module(state),
         {:ok, setting} <- archiver_setting(state) do
      module.init(
        Postgres.migrated_version(state.runtime_opts),
        archiver_callback_opts(state, setting)
      )
    end
  end

  @spec process_scan(state()) :: :ok
  defp process_scan(%__MODULE__{} = state) do
    started_at = Telemetry.monotonic_time()

    {outcome, fetched, pending, delivered} =
      with {:ok, module} <- archiver_module(state),
           {:ok, setting} <- archiver_setting(state),
           {:ok, rows} <- fetch_execution_batch(state) do
        last_row = List.last(rows)

        pending_rows =
          rows
          |> Enum.reject(& &1.delivered?)
          |> attach_events(state)

        fetched = length(rows)
        pending = length(pending_rows)

        result =
          case pending_rows do
            [] ->
              :ok

            pending_rows ->
              archive_started_at = Telemetry.monotonic_time()

              result =
                module.archive(
                  Enum.map(pending_rows, &%{execution: &1.execution, events: &1.events}),
                  Postgres.migrated_version(state.runtime_opts),
                  archiver_callback_opts(state, setting)
                )

              Telemetry.emit(
                [:archiver, :archive],
                %{
                  duration_ms: Telemetry.duration_ms(archive_started_at),
                  batch_size: pending,
                  event_count:
                    Enum.reduce(pending_rows, 0, fn row, acc -> length(row.events) + acc end),
                  history_size_bytes:
                    Enum.reduce(pending_rows, 0, fn row, acc ->
                      row.execution.history_size_bytes + acc
                    end)
                },
                archiver_metadata(state, %{outcome: archive_result_outcome(result)})
              )

              result
          end

        delivered =
          case result do
            :ok ->
              with :ok <- insert_deliveries(state, pending_rows),
                   :ok <- advance_cursor(state, last_row) do
                if pending > 0 do
                  Telemetry.emit(
                    [:archiver, :delivery_insert],
                    %{count: pending},
                    archiver_metadata(state, %{outcome: :ok})
                  )
                end

                if last_row do
                  Telemetry.emit(
                    [:archiver, :cursor_advanced],
                    %{count: 1},
                    archiver_metadata(state, %{outcome: :ok})
                  )
                end

                pending
              else
                {:error, _reason} -> 0
              end

            {:error, _reason} ->
              0
          end

        {archive_result_outcome(result), fetched, pending, delivered}
      else
        _ ->
          {:error, 0, 0, 0}
      end

    Telemetry.emit(
      [:archiver, :scan],
      %{
        duration_ms: Telemetry.duration_ms(started_at),
        batch_size: state.batch_size,
        fetched: fetched,
        pending: pending,
        delivered: delivered
      },
      archiver_metadata(state, %{outcome: outcome})
    )

    :ok
  end

  @spec schedule(term(), non_neg_integer()) :: reference()
  defp schedule(message, delay_ms) do
    Process.send_after(self(), message, delay_ms)
  end

  @spec fetch_execution_batch(state()) :: {:ok, [map()]}
  defp fetch_execution_batch(%__MODULE__{} = state) do
    repo = Keyword.fetch!(state.runtime_opts, :repo)
    prefix = Keyword.fetch!(state.runtime_opts, :prefix)
    {cursor_sql, cursor_params} = cursor_clause(state)

    sql = """
    SELECT
      e.id,
      e.unique_id,
      e.queue,
      e.workflow_name,
      e.version,
      e.input,
      e.status::text,
      e.next_event_sequence,
      e.history_size_bytes,
      e.waiting_until,
      e.locked_by,
      e.locked_until,
      e.completed_at,
      e.inserted_at,
      e.updated_at,
      EXISTS (
        SELECT 1
        FROM #{prefix}.endurant_archive_deliveries d
        WHERE d.backend = $1
        AND d.execution_id = e.id
      ) AS delivered
    FROM #{prefix}.endurant_executions e
    WHERE e.status IN (
      'completed'::#{prefix}.endurant_execution_status,
      'failed'::#{prefix}.endurant_execution_status,
      'cancelled'::#{prefix}.endurant_execution_status,
      'continued_as_new'::#{prefix}.endurant_execution_status
    )
    AND e.completed_at IS NOT NULL
    #{cursor_sql}
    ORDER BY e.completed_at ASC, e.id ASC
    LIMIT $#{length(cursor_params) + 2}
    """

    rows =
      Endurant.DB.query!(
        repo,
        sql,
        [state.archiver | cursor_params] ++ [state.batch_size],
        state.runtime_opts
      ).rows

    {:ok, Enum.map(rows, &execution_batch_row/1)}
  rescue
    DBConnection.ConnectionError -> {:ok, []}
  end

  @spec attach_events([map()], state()) :: [map()]
  defp attach_events([], _state), do: []

  defp attach_events(rows, %__MODULE__{} = state) do
    repo = Keyword.fetch!(state.runtime_opts, :repo)
    prefix = Keyword.fetch!(state.runtime_opts, :prefix)

    {placeholders, params} =
      rows
      |> Enum.map(& &1.execution.id)
      |> Enum.uniq()
      |> Enum.with_index(1)
      |> Enum.map_reduce([], fn {execution_id, idx}, acc ->
        {"$#{idx}", [to_db_id(execution_id) | acc]}
      end)

    sql = """
    SELECT id, execution_id, sequence, type::text, payload, inserted_at
    FROM #{prefix}.endurant_events
    WHERE execution_id IN (#{Enum.join(placeholders, ", ")})
    ORDER BY execution_id ASC, sequence ASC
    """

    events_by_execution =
      Endurant.DB.query!(repo, sql, Enum.reverse(params), state.runtime_opts).rows
      |> Enum.map(&event_row/1)
      |> Enum.group_by(& &1.execution_id)

    Enum.map(rows, fn row ->
      Map.put(row, :events, Map.get(events_by_execution, row.execution.id, []))
    end)
  end

  @spec insert_deliveries(state(), [map()]) :: :ok | {:error, :transient_db}
  defp insert_deliveries(_state, []), do: :ok

  defp insert_deliveries(%__MODULE__{} = state, rows) do
    repo = Keyword.fetch!(state.runtime_opts, :repo)
    prefix = Keyword.fetch!(state.runtime_opts, :prefix)

    {values_sql, params} =
      rows
      |> Enum.map(& &1.execution.id)
      |> Enum.uniq()
      |> Enum.with_index()
      |> Enum.map_reduce([], fn {execution_id, idx}, acc ->
        base = idx * 2 + 1
        {"($#{base}, $#{base + 1})", [to_db_id(execution_id), state.archiver | acc]}
      end)

    sql = """
    INSERT INTO #{prefix}.endurant_archive_deliveries (backend, execution_id)
    VALUES #{Enum.join(values_sql, ", ")}
    ON CONFLICT (backend, execution_id) DO NOTHING
    """

    _ = Endurant.DB.query!(repo, sql, Enum.reverse(params), state.runtime_opts)
    :ok
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec advance_cursor(state(), map() | nil) :: :ok | {:error, :transient_db}
  defp advance_cursor(_state, nil), do: :ok

  defp advance_cursor(%__MODULE__{} = state, row) do
    Archivers.put_cursor(
      state.archiver,
      row.execution.completed_at,
      row.execution.id,
      state.runtime_opts
    )
  end

  @spec execution_batch_row(list()) :: map()
  defp execution_batch_row([
         id,
         unique_id,
         queue,
         workflow_name,
         version,
         input,
         status,
         next_event_sequence,
         history_size_bytes,
         waiting_until,
         locked_by,
         locked_until,
         completed_at,
         inserted_at,
         updated_at,
         delivered?
       ]) do
    %{
      execution: %{
        id: to_app_id(id),
        unique_id: unique_id,
        queue: queue,
        workflow_name: workflow_name,
        version: version,
        input: input || %{},
        status: parse_status(status),
        next_event_sequence: next_event_sequence,
        history_size_bytes: history_size_bytes,
        waiting_until: waiting_until,
        locked_by: locked_by,
        locked_until: locked_until,
        completed_at: completed_at,
        inserted_at: inserted_at,
        updated_at: updated_at
      },
      delivered?: delivered?,
      events: []
    }
  end

  @spec event_row(list()) :: map()
  defp event_row([id, execution_id, sequence, type, payload, inserted_at]) do
    %{
      id: id,
      execution_id: to_app_id(execution_id),
      sequence: sequence,
      type: parse_event_type(type),
      payload: payload || %{},
      inserted_at: inserted_at
    }
  end

  @spec cursor_clause(state()) :: {String.t(), list()}
  defp cursor_clause(%__MODULE__{} = state) do
    case Archivers.cursor(state.archiver, state.runtime_opts) do
      %{"completed_at" => completed_at, "execution_id" => execution_id}
      when is_binary(completed_at) and is_binary(execution_id) ->
        {"AND (e.completed_at > $2 OR (e.completed_at = $2 AND e.id > $3))",
         [parse_cursor_time!(completed_at), to_db_id(execution_id)]}

      _ ->
        {"", []}
    end
  end

  @spec validate_archiver_module!(term()) :: module() | nil
  defp validate_archiver_module!(nil), do: nil

  defp validate_archiver_module!(module) when is_atom(module) do
    behaviours =
      module.module_info(:attributes)
      |> Keyword.get(:behaviour, [])

    if Archiver in behaviours do
      module
    else
      raise ArgumentError,
            ":archiver_module must implement Endurant.Archiver, got: #{inspect(module)}"
    end
  end

  defp validate_archiver_module!(module) do
    raise ArgumentError, ":archiver_module must be a module or nil, got: #{inspect(module)}"
  end

  @spec archiver_module(state()) :: {:ok, module()} | {:error, :not_found}
  defp archiver_module(%__MODULE__{archiver_module: module})
       when is_atom(module) and not is_nil(module) do
    {:ok, module}
  end

  defp archiver_module(%__MODULE__{} = state) do
    Archivers.resolve_module(state.archiver)
  end

  @spec archiver_setting(state()) :: {:ok, map()} | {:error, :not_found | :transient_db}
  defp archiver_setting(%__MODULE__{} = state) do
    case Archivers.get(state.archiver, state.runtime_opts) do
      nil -> {:error, :not_found}
      {:error, :transient_db} -> {:error, :transient_db}
      %{} = setting -> {:ok, setting}
    end
  end

  @spec archiver_callback_opts(state(), map()) :: keyword()
  defp archiver_callback_opts(%__MODULE__{} = state, setting) do
    [archiver: state.archiver, instance: state.instance, settings: setting] ++
      state.archiver_opts ++ state.runtime_opts
  end

  @spec parse_status(String.t()) :: atom()
  defp parse_status("pending"), do: :pending
  defp parse_status("running"), do: :running
  defp parse_status("waiting"), do: :waiting
  defp parse_status("continuable"), do: :continuable
  defp parse_status("abandoned"), do: :abandoned
  defp parse_status("cancelling"), do: :cancelling
  defp parse_status("completed"), do: :completed
  defp parse_status("failed"), do: :failed
  defp parse_status("cancelled"), do: :cancelled
  defp parse_status("continued_as_new"), do: :continued_as_new

  @spec parse_event_type(String.t()) :: atom()
  defp parse_event_type("execution_created"), do: :execution_created
  defp parse_event_type("execution_started"), do: :execution_started
  defp parse_event_type("execution_continued_as_new"), do: :execution_continued_as_new
  defp parse_event_type("execution_completed"), do: :execution_completed
  defp parse_event_type("execution_failed"), do: :execution_failed
  defp parse_event_type("execution_cancelled"), do: :execution_cancelled
  defp parse_event_type("execution_abandoned"), do: :execution_abandoned
  defp parse_event_type("execution_resumed"), do: :execution_resumed
  defp parse_event_type("execution_waiting"), do: :execution_waiting
  defp parse_event_type("child_execution_started"), do: :child_execution_started
  defp parse_event_type("child_execution_completed"), do: :child_execution_completed
  defp parse_event_type("child_execution_failed"), do: :child_execution_failed
  defp parse_event_type("child_execution_cancelled"), do: :child_execution_cancelled
  defp parse_event_type("task_started"), do: :task_started
  defp parse_event_type("task_completed"), do: :task_completed
  defp parse_event_type("task_failed"), do: :task_failed
  defp parse_event_type("task_interrupted"), do: :task_interrupted
  defp parse_event_type("signal_received"), do: :signal_received
  defp parse_event_type("cancel_requested"), do: :cancel_requested

  @spec parse_cursor_time!(String.t()) :: NaiveDateTime.t()
  defp parse_cursor_time!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        DateTime.to_naive(datetime)

      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive} -> naive
          _ -> raise ArgumentError, "invalid cursor completed_at: #{inspect(value)}"
        end
    end
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) when is_binary(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec to_app_id(binary()) :: binary()
  defp to_app_id(id) when is_binary(id) do
    case Ecto.UUID.load(id) do
      {:ok, loaded} -> loaded
      :error -> id
    end
  end

  @spec owner_id(atom() | String.t(), String.t()) :: String.t()
  defp owner_id(instance, archiver) do
    uniq = System.unique_integer([:positive, :monotonic])
    "#{node()}:#{inspect(instance)}:#{archiver}:#{uniq}"
  end

  @spec positive_integer(term(), atom()) :: pos_integer()
  defp positive_integer(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, name) do
    raise ArgumentError, "#{inspect(name)} must be a positive integer, got: #{inspect(value)}"
  end

  @spec archiver_metadata(state(), map()) :: map()
  defp archiver_metadata(state, extra) do
    Map.merge(
      %{
        instance: state.instance,
        node: node(),
        archiver: state.archiver
      },
      extra
    )
  end

  @spec archive_result_outcome(:ok | {:error, term()}) :: :ok | :error
  defp archive_result_outcome(:ok), do: :ok
  defp archive_result_outcome({:error, _reason}), do: :error

  @spec normalize_archiver!(atom() | String.t()) :: String.t()
  defp normalize_archiver!(archiver) when is_atom(archiver) do
    archiver
    |> Atom.to_string()
    |> normalize_archiver!()
  end

  defp normalize_archiver!(archiver) when is_binary(archiver) do
    normalized = String.trim(archiver)

    if normalized == "" do
      raise ArgumentError, ":archiver must be a non-empty string or atom"
    end

    normalized
  end

  defp normalize_archiver!(archiver) do
    raise ArgumentError, ":archiver must be a non-empty string or atom, got: #{inspect(archiver)}"
  end
end
