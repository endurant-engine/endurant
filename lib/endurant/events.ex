defmodule Endurant.Events do
  @moduledoc false

  @default_prefix "public"

  @type event :: %{
          id: integer(),
          execution_id: binary(),
          sequence: non_neg_integer(),
          type: atom(),
          payload: map(),
          inserted_at: NaiveDateTime.t() | DateTime.t() | nil
        }

  @doc """
  Appends an event for an existing execution.

  This function only requires that the execution row exists. It does not enforce
  executor ownership or running lease checks.

  Use this for system/state-transition events where correctness is enforced by
  the surrounding transaction/state machine, or for external events that may be
  recorded while an execution is not running (for example: `signal_received`,
  `cancel_requested`).

  If you need to guarantee that only the current lease owner can append the
  event while the execution is running, use `append_if_running_owned/5`.
  """
  @spec append(binary(), atom() | String.t(), map(), keyword()) :: :ok
  def append(execution_id, type, payload \\ %{}, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)
    type = normalize_type(type)
    payload = payload || %{}

    do_append(repo, prefix, execution_id, type, payload)
  end

  @doc """
  Appends an event only when the execution is currently running and owned by the
  provided worker lease.

  Conditions enforced by this function:
  - execution exists
  - status is `running`
  - `locked_by` matches `worker_id`
  - `locked_until` is in the future

  Returns `{:error, :not_running}` when those ownership/lease conditions are not met.

  Use this for executor-owned events (such as task lifecycle events) where stale
  or orphaned executors must not be allowed to write.
  """
  @spec append_if_running_owned(binary(), String.t(), atom() | String.t(), map(), keyword()) ::
          :ok | {:error, :not_running}
  def append_if_running_owned(execution_id, worker_id, type, payload \\ %{}, opts \\ [])
      when is_binary(worker_id) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)
    type = normalize_type(type)
    payload = payload || %{}

    do_append_if_running(repo, prefix, execution_id, worker_id, type, payload)
  end

  @doc """
  Lists the full event history for an execution in ascending sequence order.
  """
  @spec list(binary(), keyword()) :: [event()]
  def list(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT id, execution_id, sequence, type::text, payload, inserted_at
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    ORDER BY sequence ASC
    """

    rows =
      repo
      |> query!(sql, [execution_id])
      |> Map.fetch!(:rows)

    Enum.map(rows, fn [id, exec_id, sequence, type, payload, inserted_at] ->
      %{
        id: id,
        execution_id: to_app_id(exec_id),
        sequence: sequence,
        type: parse_type(type),
        payload: payload || %{},
        inserted_at: inserted_at
      }
    end)
  end

  @doc """
  Lists events with sequence strictly greater than the provided sequence number.

  Ordering is ascending by sequence. This is intended for incremental reads from
  a known checkpoint.
  """
  @spec list_after(binary(), non_neg_integer(), keyword()) :: [event()]
  def list_after(execution_id, sequence, opts \\ [])
      when is_integer(sequence) and sequence >= 0 do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT id, execution_id, sequence, type::text, payload, inserted_at
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND sequence > $2
    ORDER BY sequence ASC
    """

    rows =
      repo
      |> query!(sql, [execution_id, sequence])
      |> Map.fetch!(:rows)

    Enum.map(rows, fn [id, exec_id, sequence_value, type, payload, inserted_at] ->
      %{
        id: id,
        execution_id: to_app_id(exec_id),
        sequence: sequence_value,
        type: parse_type(type),
        payload: payload || %{},
        inserted_at: inserted_at
      }
    end)
  end

  @doc """
  Returns event history length for an execution.
  """
  @spec history_length(binary(), keyword()) :: non_neg_integer() | nil
  def history_length(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT next_event_sequence
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[next_event_sequence]]
      when is_integer(next_event_sequence) and next_event_sequence >= 1 ->
        next_event_sequence - 1

      _ ->
        nil
    end
  end

  @doc """
  Returns event history size in bytes for an execution.
  """
  @spec history_size(binary(), keyword()) :: non_neg_integer() | nil
  def history_size(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT history_size_bytes
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[history_size_bytes]]
      when is_integer(history_size_bytes) and history_size_bytes >= 0 ->
        history_size_bytes

      _ ->
        nil
    end
  end

  @spec normalize_type(atom() | String.t()) :: String.t()
  defp normalize_type(type) when is_atom(type), do: Atom.to_string(type)
  defp normalize_type(type) when is_binary(type), do: type

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    repo.query!(sql, params, log: false)
  end

  @spec do_append(module(), String.t(), binary(), String.t(), map()) :: :ok
  defp do_append(repo, prefix, execution_id, type, payload) do
    lock_sql = lock_execution_for_append_sql(prefix)
    insert_sql = insert_event_with_sequence_and_size_sql(prefix)
    update_sql = update_execution_event_stats_sql(prefix)

    case repo.transaction(
           fn ->
             case query!(repo, lock_sql, [execution_id]).rows do
               [[next_event_sequence]]
               when is_integer(next_event_sequence) and next_event_sequence >= 1 ->
                 case query!(repo, insert_sql, [execution_id, next_event_sequence, type, payload]).rows do
                   [[history_size_delta]]
                   when is_integer(history_size_delta) and history_size_delta >= 0 ->
                     case query!(repo, update_sql, [execution_id, history_size_delta]).num_rows do
                       1 -> :ok
                       _ -> repo.rollback(:execution_not_found)
                     end

                   _ ->
                     repo.rollback(:execution_not_found)
                 end

               _ ->
                 repo.rollback(:execution_not_found)
             end
           end,
           log: false
         ) do
      {:ok, :ok} ->
        :ok

      {:error, :execution_not_found} ->
        raise ArgumentError, "execution not found: #{inspect(to_app_id(execution_id))}"
    end
  end

  @spec do_append_if_running(
          module(),
          String.t(),
          binary(),
          String.t(),
          String.t(),
          map()
        ) ::
          :ok | {:error, :not_running}
  defp do_append_if_running(repo, prefix, execution_id, worker_id, type, payload) do
    lock_sql = lock_execution_for_owned_append_sql(prefix)
    insert_sql = insert_event_with_sequence_and_size_sql(prefix)
    update_sql = update_execution_event_stats_sql(prefix)

    case repo.transaction(
           fn ->
             case query!(repo, lock_sql, [execution_id, worker_id]).rows do
               [[next_event_sequence]]
               when is_integer(next_event_sequence) and next_event_sequence >= 1 ->
                 case query!(repo, insert_sql, [execution_id, next_event_sequence, type, payload]).rows do
                   [[history_size_delta]]
                   when is_integer(history_size_delta) and history_size_delta >= 0 ->
                     case query!(repo, update_sql, [execution_id, history_size_delta]).num_rows do
                       1 -> :ok
                       _ -> repo.rollback(:not_running)
                     end

                   _ ->
                     repo.rollback(:not_running)
                 end

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, :ok} -> :ok
      {:error, :not_running} -> {:error, :not_running}
    end
  end

  @spec lock_execution_for_append_sql(String.t()) :: String.t()
  defp lock_execution_for_append_sql(prefix) do
    """
    SELECT next_event_sequence
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    FOR UPDATE
    """
  end

  @spec lock_execution_for_owned_append_sql(String.t()) :: String.t()
  defp lock_execution_for_owned_append_sql(prefix) do
    """
    SELECT next_event_sequence
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    FOR UPDATE
    """
  end

  @spec insert_event_with_sequence_and_size_sql(String.t()) :: String.t()
  defp insert_event_with_sequence_and_size_sql(prefix) do
    """
    INSERT INTO #{prefix}.endurant_events (execution_id, sequence, type, payload, inserted_at)
    VALUES (
      $1,
      $2,
      $3::#{prefix}.endurant_event_type,
      $4,
      timezone('UTC', now())
    )
    RETURNING pg_column_size(ROW(id, execution_id, sequence, type, payload, inserted_at))::bigint
    """
  end

  @spec update_execution_event_stats_sql(String.t()) :: String.t()
  defp update_execution_event_stats_sql(prefix) do
    """
    UPDATE #{prefix}.endurant_executions
    SET next_event_sequence = next_event_sequence + 1,
        history_size_bytes = history_size_bytes + $2::bigint
    WHERE id = $1
    """
  end

  @spec parse_type(String.t()) :: atom()
  defp parse_type(type) do
    case type do
      "execution_created" -> :execution_created
      "execution_started" -> :execution_started
      "execution_completed" -> :execution_completed
      "execution_failed" -> :execution_failed
      "execution_cancelled" -> :execution_cancelled
      "execution_abandoned" -> :execution_abandoned
      "execution_resumed" -> :execution_resumed
      "execution_waiting" -> :execution_waiting
      "task_started" -> :task_started
      "task_completed" -> :task_completed
      "task_failed" -> :task_failed
      "signal_received" -> :signal_received
      "cancel_requested" -> :cancel_requested
    end
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
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
end
