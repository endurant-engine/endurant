defmodule Endurant.Executions do
  @moduledoc false

  alias Endurant.Events

  @default_prefix "public"

  @type execution :: %{
          id: binary(),
          workflow: module() | String.t(),
          input: map(),
          status: atom(),
          version: String.t()
        }
  @type claim_ready_branch :: :continuable | :waiting_ready
  @type recover_runnable_branch :: :running | :continuable | :waiting_ready

  @doc """
  Inserts a new execution in `pending` state and appends `:execution_created`.

  The insert enforces workflow-level uniqueness through the open-execution
  uniqueness constraint. When another open execution already exists for the same
  `unique_id`, this returns `{:error, :unique_conflict}`.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `{:ok, execution}` on success, where `execution` includes:
      `:id`, `:workflow_module`, `:unique_id`, `:version`, `:status`.
    * `{:error, :unique_conflict}` when an open execution with the same
      `unique_id` already exists.
  """
  @spec insert(module(), map(), keyword()) :: {:ok, map()} | {:error, :unique_conflict}
  def insert(workflow_module, input, opts \\ [])
      when is_atom(workflow_module) and is_map(input) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    workflow = workflow_module.__workflow__()
    execution_id = Ecto.UUID.generate()
    execution_db_id = to_db_id(execution_id)
    unique_id = resolve_unique_id(workflow, input)
    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = Map.get(workflow, :version, "1")

    sql = """
    INSERT INTO #{prefix}.endurant_executions
      (id, unique_id, queue, workflow_name, version, input, status, inserted_at, updated_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, 'pending'::#{prefix}.endurant_execution_status, timezone('UTC', now()), timezone('UTC', now()))
    ON CONFLICT (unique_id)
      WHERE status IN (
        'pending'::#{prefix}.endurant_execution_status,
        'running'::#{prefix}.endurant_execution_status,
        'waiting'::#{prefix}.endurant_execution_status,
        'continuable'::#{prefix}.endurant_execution_status,
        'abandoned'::#{prefix}.endurant_execution_status,
        'cancelling'::#{prefix}.endurant_execution_status
      )
      DO NOTHING
    RETURNING id
    """

    case repo.transaction(
           fn ->
             case query!(repo, sql, [
                    execution_db_id,
                    unique_id,
                    queue,
                    workflow_name,
                    version,
                    input
                  ]).rows do
               [[_]] ->
                 :ok =
                   Events.append(
                     execution_id,
                     :execution_created,
                     %{workflow: workflow_name, unique_id: unique_id, version: version},
                     opts
                   )

                 {:ok,
                  %{
                    id: execution_id,
                    workflow_module: workflow_name,
                    unique_id: unique_id,
                    version: version,
                    status: :pending
                  }}

               _ ->
                 {:error, :unique_conflict}
             end
           end,
           log: false
         ) do
      {:ok, result} -> result
      {:error, _op, reason, _changes} -> {:error, reason}
    end
  end

  @doc """
  Fetches a single execution by id.

  Returns normalized execution data when found:

    * `:id` - execution id in app UUID format
    * `:workflow` - stored workflow module name
    * `:input` - workflow input map
    * `:status` - execution status atom
    * `:version` - workflow version string

  Returns `nil` when no execution exists for the given id.

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.
  """
  @spec get(binary(), keyword()) :: execution() | nil
  def get(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT id, workflow_name, input, status::text, version
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[id, workflow_name, input, status, version]] ->
        %{
          id: to_app_id(id),
          workflow: workflow_name,
          input: input || %{},
          status: parse_status(status),
          version: version
        }

      _ ->
        nil
    end
  end

  @doc """
  Claims runnable executions from a queue and marks them as `running`.

  Candidates are selected from `pending` and `abandoned` executions in the
  target queue. `abandoned` executions are prioritized before `pending`, then
  ordered by `inserted_at` and `id`.

  For claimed executions that were previously `abandoned`, an
  `:execution_resumed` event is appended in the same transaction as the claim.

  ## Parameters

    * `queue` - queue name as atom or string.
    * `limit` - maximum number of executions to claim, must be `> 0`.
    * `worker_id` - worker lease owner id, must be a binary.
    * `lease_ms` - lease duration in milliseconds, must be `> 0`.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

  Returns a list of claimed executions. Each entry includes:

    * `:id` - execution id in app UUID format
    * `:workflow` - stored workflow module name
    * `:input` - workflow input map
    * `:status` - always `:running`
    * `:version` - workflow version string

  Returns `[]` when no rows are claimable.
  """
  @spec claim_pending(atom(), pos_integer(), String.t(), pos_integer(), keyword()) :: [map()]
  def claim_pending(queue, limit, worker_id, lease_ms, opts \\ [])
      when is_integer(limit) and limit > 0 and is_binary(worker_id) and
             is_integer(lease_ms) and lease_ms > 0 do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    queue = normalize_queue(queue)

    sql = """
    WITH candidate AS (
      SELECT id, status
      FROM #{prefix}.endurant_executions
      WHERE queue = $1
      AND status IN (
        'abandoned'::#{prefix}.endurant_execution_status,
        'pending'::#{prefix}.endurant_execution_status
      )
      ORDER BY
        CASE
          WHEN status = 'abandoned'::#{prefix}.endurant_execution_status THEN 0
          ELSE 1
        END,
        inserted_at ASC,
        id ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'running'::#{prefix}.endurant_execution_status,
      locked_by = $3,
      locked_until = timezone('UTC', now()) + ($4::int * interval '1 millisecond'),
      updated_at = timezone('UTC', now())
    FROM candidate
    WHERE e.id = candidate.id
    RETURNING e.id, e.workflow_name, e.input, e.version, candidate.status::text
    """

    case repo.transaction(
           fn ->
             rows =
               repo
               |> query!(sql, [queue, limit, worker_id, lease_ms])
               |> Map.fetch!(:rows)

             Enum.map(rows, fn [id, workflow_name, input, version, previous_status] ->
               execution = %{
                 id: to_app_id(id),
                 workflow: workflow_name,
                 input: input,
                 status: :running,
                 version: version
               }

               if previous_status == "abandoned" do
                 Events.append(execution.id, :execution_resumed, %{worker_id: worker_id}, opts)
               end

               execution
             end)
           end,
           log: false
         ) do
      {:ok, executions} -> executions
      {:error, reason} -> raise "claim_pending failed: #{inspect(reason)}"
    end
  end

  @doc """
  Confirms that a claimed execution has actually started on the current worker.

  This is a strict lease/ownership check at executor handoff time. It only
  succeeds when:

    * execution exists
    * status is `running`
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future

  On success, `waiting_until` is cleared and an `:execution_started` event is
  appended.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` - start confirmed for current owner/lease.
    * `{:error, :not_found}` - execution id does not exist.
    * `{:error, :cancelled}` - execution is in cancelling/cancelled path.
    * `{:error, :lock_lost}` - execution exists but lease ownership is not valid
      for this worker (wrong owner, expired/missing lease, or incompatible state).
  """
  @spec mark_started(binary(), String.t(), keyword()) ::
          :ok | {:error, :not_found | :lock_lost | :cancelled}
  def mark_started(execution_id, worker_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'running'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case query!(repo, sql, [execution_id, worker_id]).num_rows do
      1 ->
        Events.append(execution_id, :execution_started, %{worker_id: worker_id}, opts)
        :ok

      _ ->
        case lock_execution_status(repo, prefix, execution_id) do
          nil ->
            {:error, :not_found}

          status when status in [:cancelling, :cancelled] ->
            {:error, :cancelled}

          _ ->
            {:error, :lock_lost}
        end
    end
  end

  @doc """
  Marks a running execution as completed for the current lease owner.

  Completion is ownership-gated and lease-gated. It only succeeds when:

    * execution exists
    * status is `running`
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future

  On success, this writes the terminal state (`completed`, `completed_at`,
  cleared lock fields) and appends `:execution_completed` with the provided
  `result` payload in the same transaction.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `result` - completion payload to store in the completion event.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when completion and completion-event append succeed.
    * `{:error, :not_running}` when the execution is missing, not `running`, no
      longer owned by `worker_id`, or the lease is not valid.
  """
  @spec mark_completed_owned(binary(), String.t(), map(), keyword()) ::
          :ok | {:error, :not_running}
  def mark_completed_owned(execution_id, worker_id, result, opts \\ [])
      when is_binary(worker_id) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'completed'::#{prefix}.endurant_execution_status,
      completed_at = timezone('UTC', now()),
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case repo.transaction(
           fn ->
             case query!(repo, sql, [execution_id, worker_id]).num_rows do
               1 ->
                 Events.append(execution_id, :execution_completed, %{result: result}, opts)
                 :ok

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

  @doc """
  Marks a running execution as failed for the current lease owner.

  Failure is ownership-gated and lease-gated. It only succeeds when:

    * execution exists
    * status is `running`
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future

  On success, this writes terminal failure state (status `failed`,
  `completed_at`, cleared lock fields) and appends `:execution_failed` with the
  provided `error` payload in the same transaction.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `error` - serialized failure payload to store in the failure event.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when failure state and failure-event append succeed.
    * `{:error, :not_running}` when the execution is missing, not `running`, no
      longer owned by `worker_id`, or the lease is not valid.
  """
  @spec mark_failed_owned(binary(), String.t(), map(), keyword()) ::
          :ok | {:error, :not_running}
  def mark_failed_owned(execution_id, worker_id, error, opts \\ []) when is_binary(worker_id) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'failed'::#{prefix}.endurant_execution_status,
      completed_at = timezone('UTC', now()),
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case repo.transaction(
           fn ->
             case query!(repo, sql, [execution_id, worker_id]).num_rows do
               1 ->
                 Events.append(execution_id, :execution_failed, %{error: error}, opts)
                 :ok

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

  @doc """
  Marks a running execution as waiting for the current lease owner.

  This transition is ownership-gated and lease-gated. It only succeeds when:

    * execution exists
    * status is `running`
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future

  `waiting_until` may be `nil` (signal wait) or a timestamp (time wait).

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `waiting_until` - optional wake timestamp for time-based waits.

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when waiting state is persisted.
    * `{:error, :not_running}` when the execution is missing, not `running`, no
      longer owned by `worker_id`, or the lease is not valid.
  """
  @spec mark_waiting_owned(binary(), String.t(), DateTime.t() | nil, keyword()) ::
          :ok | {:error, :not_running}
  def mark_waiting_owned(execution_id, worker_id, waiting_until, opts \\ [])
      when is_binary(worker_id) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)
    waiting_until = maybe_naive(waiting_until)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'waiting'::#{prefix}.endurant_execution_status,
      waiting_until = $3,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case query!(repo, sql, [execution_id, worker_id, waiting_until]).num_rows do
      1 -> :ok
      _ -> {:error, :not_running}
    end
  end

  @doc """
  Atomically appends `:execution_waiting` (signal mode) and marks execution `waiting`.

  This is ownership-gated and lease-gated. Both the event append and state update
  happen in one transaction to avoid event/state divergence.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `signal_key` - normalized signal key being awaited.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when both event append and waiting state update succeed.
    * `{:error, :not_running}` when the execution is missing, not `running`, no
      longer owned by `worker_id`, or the lease is not valid.
  """
  @spec mark_waiting_with_event_owned(binary(), String.t(), String.t(), keyword()) ::
          :ok | {:error, :not_running}
  def mark_waiting_with_event_owned(execution_id, worker_id, signal_key, opts \\ [])
      when is_binary(worker_id) and is_binary(signal_key) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id_db = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'waiting'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case repo.transaction(
           fn ->
             case query!(repo, sql, [execution_id_db, worker_id]).num_rows do
               1 ->
                 Events.append(
                   execution_id,
                   :execution_waiting,
                   %{mode: :signal, signal: signal_key},
                   opts
                 )

                 :ok

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

  @doc """
  Atomically appends `:execution_waiting` (time mode) and marks execution `waiting`.

  This is ownership-gated and lease-gated. Both the event append and state update
  happen in one transaction to avoid event/state divergence.
  """
  @spec mark_waiting_with_time_event_owned(
          binary(),
          String.t(),
          DateTime.t(),
          pos_integer(),
          String.t(),
          keyword()
        ) :: :ok | {:error, :not_running}
  def mark_waiting_with_time_event_owned(
        execution_id,
        worker_id,
        run_at,
        delay_ms,
        wait_key,
        opts \\ []
      )
      when is_binary(worker_id) and is_binary(wait_key) and is_integer(delay_ms) and delay_ms > 0 do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id_db = to_db_id(execution_id)
    waiting_until = maybe_naive(run_at)
    waiting_until_iso = DateTime.to_iso8601(run_at)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'waiting'::#{prefix}.endurant_execution_status,
      waiting_until = $3,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case repo.transaction(
           fn ->
             case query!(repo, sql, [execution_id_db, worker_id, waiting_until]).num_rows do
               1 ->
                 Events.append(
                   execution_id,
                   :execution_waiting,
                   %{
                     mode: :time,
                     until: waiting_until_iso,
                     delay_ms: delay_ms,
                     wait_key: wait_key
                   },
                   opts
                 )

                 :ok

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

  @doc """
  Releases a waiting/continuable owned execution and appends `:execution_abandoned`.

  Lock release and event append are atomic in a single transaction.

  The `abandoned_at` payload is derived from the execution's previous
  `locked_until` value so timeline/debug views reflect when the lease expired.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when release + abandoned event append succeed.
    * `{:error, :not_running}` when the execution isn't in a releasable owned
      waiting/continuable state for `worker_id`.
  """
  @spec release_waiting_as_abandoned_owned(binary(), String.t(), keyword()) ::
          :ok | {:error, :not_running}
  def release_waiting_as_abandoned_owned(execution_id, worker_id, opts \\ [])
      when is_binary(worker_id) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id_db = to_db_id(execution_id)

    case repo.transaction(
           fn ->
             sql = """
             WITH locked_execution AS (
               SELECT id, locked_until
               FROM #{prefix}.endurant_executions
               WHERE id = $1
               AND status IN (
                 'waiting'::#{prefix}.endurant_execution_status,
                 'continuable'::#{prefix}.endurant_execution_status
               )
               AND locked_by = $2
               FOR UPDATE
             )
             UPDATE #{prefix}.endurant_executions e
             SET
                locked_by = NULL,
               locked_until = NULL,
               updated_at = timezone('UTC', now())
             FROM locked_execution
             WHERE e.id = locked_execution.id
             RETURNING locked_execution.locked_until
             """

             case query!(repo, sql, [execution_id_db, worker_id]).rows do
               [[locked_until]] ->
                 abandoned_at =
                   case locked_until do
                     %DateTime{} = dt ->
                       DateTime.to_iso8601(dt)

                     %NaiveDateTime{} = dt ->
                       dt
                       |> DateTime.from_naive!("Etc/UTC")
                       |> DateTime.to_iso8601()

                     other ->
                       raise "missing locked_until for waiting abandonment #{inspect(execution_id)}: #{inspect(other)}"
                   end

                 Events.append(
                   execution_id,
                   :execution_abandoned,
                   %{abandoned_at: abandoned_at},
                   opts
                 )

                 :ok

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

  @doc """
  Claims waiting executions that are ready to resume and marks them as `running`.

  Eligible executions are:

    * `continuable`, or
    * `waiting` with `waiting_until <= now()`

  Additional gating rules:

    * queue matches `queue`
    * execution is currently unowned (`locked_by IS NULL`)

  For each claimed row, `:execution_resumed` is appended in the same
  transaction as the state update.

  ## Parameters

    * `queue` - queue name as atom or string.
    * `limit` - maximum number of executions to claim, must be `> 0`.
    * `worker_id` - worker lease owner id, must be a binary.
    * `lease_ms` - lease duration in milliseconds, must be `> 0`.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

  Returns a list of claimed executions. Each entry includes:

    * `:id` - execution id in app UUID format
    * `:workflow` - stored workflow module name
    * `:input` - workflow input map
    * `:status` - always `:running`
    * `:version` - workflow version string

  Returns `[]` when no rows are claimable.
  """
  @spec claim_ready_waiting(atom(), pos_integer(), String.t(), pos_integer(), keyword()) :: [
          map()
        ]
  def claim_ready_waiting(queue, limit, worker_id, lease_ms, opts \\ [])
      when is_integer(limit) and limit > 0 and is_binary(worker_id) and
             is_integer(lease_ms) and lease_ms > 0 do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    queue = normalize_queue(queue)
    branch_order = claim_ready_branch_order(opts)

    case repo.transaction(
           fn ->
             {rows, _remaining} =
               Enum.reduce(branch_order, {[], limit}, fn branch, {acc, remaining} ->
                 if remaining <= 0 do
                   {acc, 0}
                 else
                   sql = claim_ready_waiting_sql(prefix, branch)
                   branch_rows = query!(repo, sql, [queue, remaining, worker_id, lease_ms]).rows
                   {acc ++ branch_rows, max(remaining - length(branch_rows), 0)}
                 end
               end)

             Enum.map(rows, fn [id, workflow_name, input, version] ->
               execution = %{
                 id: to_app_id(id),
                 workflow: workflow_name,
                 input: input,
                 status: :running,
                 version: version
               }

               Events.append(execution.id, :execution_resumed, %{worker_id: worker_id}, opts)

               execution
             end)
           end,
           log: false
         ) do
      {:ok, executions} -> executions
      {:error, reason} -> raise "claim_ready_waiting failed: #{inspect(reason)}"
    end
  end

  @doc """
  Transitions an owned waiting execution back to `running`.

  This is used by the queue manager when resuming a parked executor process.
  It only succeeds when:

    * execution exists
    * status is `waiting` or `continuable`
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future

  On success, it clears `waiting_until`, keeps ownership on `worker_id`, and
  extends the lease by `lease_ms`.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `lease_ms` - lease extension in milliseconds, must be `> 0`.

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when transition and lease extension succeed.
    * `{:error, :not_found}` when the execution id doesn't exist.
    * `{:error, :lock_expired}` when execution is still resumable in principle
      (`waiting`/`continuable`) but ownership/lease is no longer valid.
    * `{:error, :cancelled}` for non-resumable states.
  """
  @spec mark_running(binary(), String.t(), pos_integer(), keyword()) ::
          :ok | {:error, :not_found | :cancelled | :lock_expired}
  def mark_running(execution_id, worker_id, lease_ms, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'running'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = $2,
      locked_until = timezone('UTC', now()) + ($3::int * interval '1 millisecond'),
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status IN (
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status
    )
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    """

    case query!(repo, sql, [execution_id, worker_id, lease_ms]).num_rows do
      1 ->
        :ok

      _ ->
        case lock_execution_status(repo, prefix, execution_id) do
          nil -> {:error, :not_found}
          status when status in [:waiting, :continuable] -> {:error, :lock_expired}
          _ -> {:error, :cancelled}
        end
    end
  end

  @doc """
  Marks a waiting execution as `continuable`.

  This transition is intended for signal-driven wakeups, where a waiting
  execution should become resumable by the queue manager.

  Current behavior is intentionally best-effort:

    * updates only when status is `waiting`
    * returns `:ok` even if no row was updated

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.
  """
  @spec mark_continuable(binary(), keyword()) :: :ok
  def mark_continuable(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'continuable'::#{prefix}.endurant_execution_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'waiting'::#{prefix}.endurant_execution_status
    """

    _ = query!(repo, sql, [execution_id])
    :ok
  end

  @doc """
  Renews an execution lease heartbeat for the current worker.

  This is called by the executor heartbeat loop to keep ownership alive while
  work is in progress or parked waiting.

  Success requires:

    * execution exists
    * `locked_by` matches `worker_id`
    * `locked_until` is present and still in the future
    * status is one of `running`, `waiting`, `continuable`, or `cancelling`

  A cancelling execution is reported as cancelled in the same query result.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `worker_id` - current worker lease owner id.
    * `lease_ms` - lease extension in milliseconds.

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when lease renewal succeeds and execution is not cancelling.
    * `{:error, :cancelled}` when execution is in `cancelling`.
    * `{:error, :lock_lost}` when execution is missing, not owned by the worker,
      lease is missing/expired, or status is not heartbeat-eligible.
    * `{:error, :transient_db}` when the DB connection is temporarily unavailable.
  """
  @spec heartbeat(binary(), String.t(), pos_integer(), keyword()) ::
          :ok | {:error, :cancelled | :lock_lost | :transient_db}
  def heartbeat(execution_id, worker_id, lease_ms, opts \\ []) do
    do_heartbeat(execution_id, worker_id, lease_ms, opts)
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec do_heartbeat(binary(), String.t(), pos_integer(), keyword()) ::
          :ok | {:error, :cancelled | :lock_lost}
  defp do_heartbeat(execution_id, worker_id, lease_ms, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      locked_until =
        CASE
          WHEN status = 'cancelling'::#{prefix}.endurant_execution_status THEN locked_until
          ELSE timezone('UTC', now()) + ($3::int * interval '1 millisecond')
        END,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    AND status IN (
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'cancelling'::#{prefix}.endurant_execution_status
    )
    RETURNING status::text
    """

    case query!(repo, sql, [execution_id, worker_id, lease_ms]).rows do
      [["cancelling"]] -> {:error, :cancelled}
      [[_status]] -> :ok
      _ -> {:error, :lock_lost}
    end
  end

  @doc """
  Recovers executions whose leases expired.

  This function is typically called by the queue manager on each tick before
  claiming new work.

  Recovery work is performed in one transaction across three branches:

    * waiting executions with expired lease (not yet time-ready): clear lock and
      append `:execution_abandoned`
    * cancelling executions with expired lease: finalize to `cancelled` and
      append `:execution_cancelled`
    * runnable executions with expired lease: mark as `abandoned` and append
      `:execution_abandoned`

  ## Parameters

    * `limit` - maximum number of rows to process per recovery branch.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.
    * `:queue` - optional queue name (`atom` or `string`). When provided,
      recovery is scoped to that queue only.

  ## Returns

  Returns the number of rows processed by the runnable-expiration branch.
  """
  @spec recover_expired_locks(pos_integer(), keyword()) :: non_neg_integer()
  def recover_expired_locks(limit, opts \\ []) when is_integer(limit) and limit > 0 do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    queue = opts |> Keyword.get(:queue) |> normalize_queue_option()

    abandoned_rows =
      repo.transaction(
        fn ->
          clear_waiting_locks_sql = """
          WITH expired_waiting AS (
            SELECT e.id, e.locked_until
            FROM #{prefix}.endurant_executions e
            WHERE e.status = 'waiting'::#{prefix}.endurant_execution_status
            AND ($1::text IS NULL OR e.queue = $1)
            AND e.locked_until IS NOT NULL
            AND e.locked_until <= timezone('UTC', now())
            AND NOT (
              e.waiting_until IS NOT NULL
              AND e.waiting_until <= timezone('UTC', now())
            )
            ORDER BY e.locked_until ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
          )
          UPDATE #{prefix}.endurant_executions e
          SET
            locked_by = NULL,
            locked_until = NULL,
            updated_at = timezone('UTC', now())
          FROM expired_waiting
            WHERE e.id = expired_waiting.id
            RETURNING e.id, expired_waiting.locked_until
          """

          cleared_waiting_rows = query!(repo, clear_waiting_locks_sql, [queue, limit]).rows

          Enum.each(cleared_waiting_rows, fn [id, locked_until] ->
            abandoned_at =
              case locked_until do
                %DateTime{} = dt ->
                  DateTime.to_iso8601(dt)

                %NaiveDateTime{} = dt ->
                  dt
                  |> DateTime.from_naive!("Etc/UTC")
                  |> DateTime.to_iso8601()

                other ->
                  raise "missing locked_until for waiting lock recovery #{inspect(id)}: #{inspect(other)}"
              end

            Events.append(
              to_app_id(id),
              :execution_abandoned,
              %{abandoned_at: abandoned_at},
              opts
            )
          end)

          expire_cancelling_sql = """
          WITH expired_cancelling AS (
            SELECT e.id
            FROM #{prefix}.endurant_executions e
            WHERE e.status = 'cancelling'::#{prefix}.endurant_execution_status
            AND ($1::text IS NULL OR e.queue = $1)
            AND e.locked_until IS NOT NULL
            AND e.locked_until <= timezone('UTC', now())
            ORDER BY e.locked_until ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
          )
          UPDATE #{prefix}.endurant_executions e
          SET
            status = 'cancelled'::#{prefix}.endurant_execution_status,
            completed_at = timezone('UTC', now()),
            waiting_until = NULL,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = timezone('UTC', now())
          FROM expired_cancelling
          WHERE e.id = expired_cancelling.id
          RETURNING e.id
          """

          cancelled_rows = query!(repo, expire_cancelling_sql, [queue, limit]).rows

          Enum.each(cancelled_rows, fn [id] ->
            Events.append(to_app_id(id), :execution_cancelled, %{}, opts)
          end)

          recover_order = recover_runnable_branch_order(opts)

          {rows, _remaining} =
            Enum.reduce(recover_order, {[], limit}, fn branch, {acc, remaining} ->
              if remaining <= 0 do
                {acc, 0}
              else
                sql = recover_runnable_sql(prefix, branch)
                branch_rows = query!(repo, sql, [queue, remaining]).rows
                {acc ++ branch_rows, max(remaining - length(branch_rows), 0)}
              end
            end)

          Enum.each(rows, fn [id, locked_until, pre_orphaned_waiting, has_abandoned_event] ->
            # Rows that were already orphaned may already have an abandoned event.
            if pre_orphaned_waiting and has_abandoned_event do
              :ok
            else
              abandoned_at =
                case locked_until do
                  %DateTime{} = dt ->
                    DateTime.to_iso8601(dt)

                  %NaiveDateTime{} = dt ->
                    dt
                    |> DateTime.from_naive!("Etc/UTC")
                    |> DateTime.to_iso8601()

                  other ->
                    raise "missing locked_until for abandoned execution #{inspect(id)}: #{inspect(other)}"
                end

              Events.append(
                to_app_id(id),
                :execution_abandoned,
                %{abandoned_at: abandoned_at},
                opts
              )
            end
          end)

          rows
        end,
        log: false
      )
      |> case do
        {:ok, rows} -> rows
        {:error, reason} -> raise "recover_expired_locks failed: #{inspect(reason)}"
      end

    length(abandoned_rows)
  end

  @spec claim_ready_branch_order(keyword()) :: [claim_ready_branch()]
  defp claim_ready_branch_order(opts) do
    default_order = [:continuable, :waiting_ready]
    configured_order = Keyword.get(opts, :claim_order, default_order)

    case configured_order do
      [first, second] = order ->
        if first != second and MapSet.new(order) == MapSet.new(default_order) do
          order
        else
          default_order
        end

      _ ->
        default_order
    end
  end

  @spec claim_ready_waiting_sql(String.t(), claim_ready_branch()) :: String.t()
  defp claim_ready_waiting_sql(prefix, :continuable) do
    """
    WITH candidate AS (
      SELECT e.id
      FROM #{prefix}.endurant_executions e
      WHERE e.queue = $1
      AND e.status = 'continuable'::#{prefix}.endurant_execution_status
      AND e.locked_by IS NULL
      ORDER BY e.inserted_at ASC, e.id ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'running'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = $3,
      locked_until = timezone('UTC', now()) + ($4::int * interval '1 millisecond'),
      updated_at = timezone('UTC', now())
    FROM candidate
    WHERE e.id = candidate.id
    RETURNING e.id, e.workflow_name, e.input, e.version
    """
  end

  defp claim_ready_waiting_sql(prefix, :waiting_ready) do
    """
    WITH candidate AS (
      SELECT e.id
      FROM #{prefix}.endurant_executions e
      WHERE e.queue = $1
      AND e.status = 'waiting'::#{prefix}.endurant_execution_status
      AND e.waiting_until IS NOT NULL
      AND e.waiting_until <= timezone('UTC', now())
      AND e.locked_by IS NULL
      ORDER BY e.inserted_at ASC, e.id ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'running'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = $3,
      locked_until = timezone('UTC', now()) + ($4::int * interval '1 millisecond'),
      updated_at = timezone('UTC', now())
    FROM candidate
    WHERE e.id = candidate.id
    RETURNING e.id, e.workflow_name, e.input, e.version
    """
  end

  @spec recover_runnable_branch_order(keyword()) :: [recover_runnable_branch()]
  defp recover_runnable_branch_order(opts) do
    default_order = [:running, :continuable, :waiting_ready]
    configured_order = Keyword.get(opts, :recover_order, default_order)

    case configured_order do
      [a, b, c] = order ->
        if a != b and b != c and a != c and MapSet.new(order) == MapSet.new(default_order) do
          order
        else
          default_order
        end

      _ ->
        default_order
    end
  end

  @spec recover_runnable_sql(String.t(), recover_runnable_branch()) :: String.t()
  defp recover_runnable_sql(prefix, :running) do
    """
    WITH expired_running AS (
      SELECT
        e.id,
        e.locked_until,
        FALSE AS pre_orphaned_waiting,
        EXISTS (
          SELECT 1
          FROM #{prefix}.endurant_events ae
          WHERE ae.execution_id = e.id
          AND ae.type = 'execution_abandoned'::#{prefix}.endurant_event_type
        ) AS has_abandoned_event
      FROM #{prefix}.endurant_executions e
      WHERE ($1::text IS NULL OR e.queue = $1)
      AND e.status = 'running'::#{prefix}.endurant_execution_status
      AND e.locked_until IS NOT NULL
      AND e.locked_until <= timezone('UTC', now())
      ORDER BY e.locked_until ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'abandoned'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    FROM expired_running
    WHERE e.id = expired_running.id
    RETURNING
      e.id,
      expired_running.locked_until,
      expired_running.pre_orphaned_waiting,
      expired_running.has_abandoned_event
    """
  end

  defp recover_runnable_sql(prefix, :continuable) do
    """
    WITH expired_continuable AS (
      SELECT
        e.id,
        e.locked_until,
        FALSE AS pre_orphaned_waiting,
        EXISTS (
          SELECT 1
          FROM #{prefix}.endurant_events ae
          WHERE ae.execution_id = e.id
          AND ae.type = 'execution_abandoned'::#{prefix}.endurant_event_type
        ) AS has_abandoned_event
      FROM #{prefix}.endurant_executions e
      WHERE ($1::text IS NULL OR e.queue = $1)
      AND e.status = 'continuable'::#{prefix}.endurant_execution_status
      AND e.locked_by IS NOT NULL
      AND e.locked_until IS NOT NULL
      AND e.locked_until <= timezone('UTC', now())
      ORDER BY e.locked_until ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'abandoned'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    FROM expired_continuable
    WHERE e.id = expired_continuable.id
    RETURNING
      e.id,
      expired_continuable.locked_until,
      expired_continuable.pre_orphaned_waiting,
      expired_continuable.has_abandoned_event
    """
  end

  defp recover_runnable_sql(prefix, :waiting_ready) do
    """
    WITH expired_waiting_ready AS (
      SELECT
        e.id,
        e.locked_until,
        (e.locked_by IS NULL) AS pre_orphaned_waiting,
        EXISTS (
          SELECT 1
          FROM #{prefix}.endurant_events ae
          WHERE ae.execution_id = e.id
          AND ae.type = 'execution_abandoned'::#{prefix}.endurant_event_type
        ) AS has_abandoned_event
      FROM #{prefix}.endurant_executions e
      WHERE ($1::text IS NULL OR e.queue = $1)
      AND e.status = 'waiting'::#{prefix}.endurant_execution_status
      AND e.waiting_until IS NOT NULL
      AND e.waiting_until <= timezone('UTC', now())
      AND e.locked_until IS NOT NULL
      AND e.locked_until <= timezone('UTC', now())
      ORDER BY e.locked_until ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE #{prefix}.endurant_executions e
    SET
      status = 'abandoned'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    FROM expired_waiting_ready
    WHERE e.id = expired_waiting_ready.id
    RETURNING
      e.id,
      expired_waiting_ready.locked_until,
      expired_waiting_ready.pre_orphaned_waiting,
      expired_waiting_ready.has_abandoned_event
    """
  end

  @spec normalize_queue_option(nil | atom() | String.t()) :: nil | String.t()
  defp normalize_queue_option(nil), do: nil
  defp normalize_queue_option(queue) when is_atom(queue), do: Atom.to_string(queue)
  defp normalize_queue_option(queue) when is_binary(queue), do: queue

  @doc """
  Records a signal for an execution and optionally marks waiting execution as
  continuable when the signal matches the current wait.

  Behavior is transactional:

    * lock execution row
    * append `:signal_received` event
    * if execution is `waiting` and the latest wait expects this signal,
      transition `waiting -> continuable`

  Signals are accepted only for active execution states:
  `pending`, `running`, `waiting`, `continuable`, `abandoned`.

  ## Parameters

    * `execution_id` - execution id in app UUID format.
    * `signal` - signal name as string.
    * `payload` - signal payload map.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when signal is recorded.
    * `{:error, :not_found}` when execution id doesn't exist.
    * `{:error, :not_active}` when execution is in terminal/cancel path and
      should not accept new signals.
  """
  @spec record_signal(binary(), String.t(), map(), keyword()) ::
          :ok | {:error, :not_found | :not_active}
  def record_signal(execution_id, signal, payload, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)
    signal_name = normalize_signal(signal)
    wait_signal_key = signal_name

    case repo.transaction(
           fn ->
             case lock_execution_status(repo, prefix, execution_id) do
               nil ->
                 repo.rollback(:not_found)

               status when status in [:pending, :running, :waiting, :continuable, :abandoned] ->
                 Events.append(
                   execution_id,
                   :signal_received,
                   %{signal: signal_name, payload: payload || %{}},
                   opts
                 )

                 if status == :waiting and
                      latest_wait_signal_matches?(execution_id, wait_signal_key, repo, prefix) do
                   mark_continuable(execution_id, opts)
                 end

                 :ok

               _status ->
                 repo.rollback(:not_active)
             end
           end,
           log: false
         ) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Requests cancellation for an execution.

  High-level cancellation behavior:

    * running executions transition to `cancelling` and are finalized by the
      running executor (or recovery on lease expiry)
    * non-running active executions (`pending`, `waiting`, `continuable`,
      `abandoned`) transition to `cancelling` and are finalized immediately
    * repeated cancellation requests for `cancelling`/`cancelled` are idempotent

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when cancellation is accepted (or already in progress/already done).
    * `{:error, :not_found}` when execution id doesn't exist.
    * `{:error, :not_active}` when execution is already terminal
      (`completed`/`failed`).
  """
  @spec cancel(binary(), keyword()) :: :ok | {:error, :not_found | :not_active}
  def cancel(execution_id, opts \\ []) do
    case request_cancel(execution_id, opts) do
      {:ok, :running} ->
        :ok

      {:ok, :finalize_now} ->
        _ = mark_cancelled(execution_id, opts)
        :ok

      {:ok, :already_cancelling} ->
        :ok

      {:ok, :already_cancelled} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Performs the cancellation state transition decision and returns transition
  intent to the caller.

  This function is transactional and state-aware, returning whether the caller
  should wait for executor-driven finalization (`:running`) or can finalize
  immediately (`:finalize_now`).

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.
  """
  @spec request_cancel(binary(), keyword()) ::
          {:ok, :running | :finalize_now | :already_cancelling | :already_cancelled}
          | {:error, :not_found | :not_active}
  def request_cancel(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    case repo.transaction(
           fn ->
             case lock_execution_status(repo, prefix, execution_id) do
               nil ->
                 repo.rollback(:not_found)

               status when status in [:completed, :failed] ->
                 repo.rollback(:not_active)

               :cancelled ->
                 :already_cancelled

               :cancelling ->
                 :already_cancelling

               :running ->
                 mark_cancelling(execution_id, opts)
                 :running

               status when status in [:pending, :waiting, :continuable, :abandoned] ->
                 mark_cancelling(execution_id, opts)
                 :finalize_now

               _ ->
                 repo.rollback(:not_active)
             end
           end,
           log: false
         ) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Finalizes cancellation when execution is in `cancelling`.

  This function is idempotent for missing and already-cancelled executions, and
  strict for other states. On success it sets terminal state fields, including
  `completed_at`.

  ## Options

    * `:repo` - Ecto repo module to use for queries and transactions.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.

  ## Returns

    * `:ok` when cancelled (or already effectively cancelled/missing).
    * `{:error, :invalid_transition}` when execution exists but is not in
      `cancelling`/`cancelled`.
  """
  @spec mark_cancelled(binary(), keyword()) :: :ok | {:error, :invalid_transition}
  def mark_cancelled(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    case repo.transaction(
           fn ->
             case lock_execution_status(repo, prefix, execution_id) do
               nil ->
                 :ok

               :cancelled ->
                 :ok

               :cancelling ->
                 finalize_cancel(execution_id, opts)

               _ ->
                 repo.rollback(:invalid_transition)
             end
           end,
           log: false
         ) do
      {:ok, :ok} -> :ok
      {:error, :invalid_transition} -> {:error, :invalid_transition}
      {:error, _reason} -> {:error, :invalid_transition}
    end
  end

  @spec mark_cancelling(binary(), keyword()) :: :ok
  defp mark_cancelling(execution_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'cancelling'::#{prefix}.endurant_execution_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status IN (
      'pending'::#{prefix}.endurant_execution_status,
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'abandoned'::#{prefix}.endurant_execution_status
    )
    """

    case query!(repo, sql, [execution_id]).num_rows do
      1 -> Events.append(execution_id, :cancel_requested, %{}, opts)
      _ -> :ok
    end

    :ok
  end

  @spec finalize_cancel(binary(), keyword()) :: :ok
  defp finalize_cancel(execution_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'cancelled'::#{prefix}.endurant_execution_status,
      completed_at = timezone('UTC', now()),
      waiting_until = NULL,
      locked_by = NULL,
      locked_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'cancelling'::#{prefix}.endurant_execution_status
    """

    case query!(repo, sql, [execution_id]).num_rows do
      1 -> Events.append(execution_id, :execution_cancelled, %{}, opts)
      _ -> :ok
    end

    :ok
  end

  @doc """
  Returns whether cancellation has been requested or finalized for an execution.

  ## Options

    * `:repo` - Ecto repo module to use for queries.
      Defaults to `Application.fetch_env!(:endurant, :repo)`.
    * `:prefix` - database schema prefix that contains Endurant tables/types.
      Defaults to `"public"`.
  """
  @spec cancellation_requested?(binary(), keyword()) :: boolean()
  def cancellation_requested?(execution_id, opts \\ []) do
    case get(execution_id, opts) do
      %{status: status} when status in [:cancelling, :cancelled] -> true
      _ -> false
    end
  end

  @spec ready_for_resume?(binary(), keyword()) :: boolean()
  def ready_for_resume?(execution_id, opts \\ []) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)

    sql = """
    SELECT
      e.status::text,
      (
        e.status = 'continuable'::#{prefix}.endurant_execution_status
        OR (
          e.status = 'waiting'::#{prefix}.endurant_execution_status
          AND e.waiting_until IS NOT NULL
          AND e.waiting_until <= timezone('UTC', now())
        )
      ) AS ready
    FROM #{prefix}.endurant_executions e
    WHERE e.id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[status, true]] when status in ["waiting", "continuable"] -> true
      _ -> false
    end
  end

  @spec ready_for_resume_many([binary()], keyword()) :: MapSet.t(binary())
  def ready_for_resume_many(execution_ids, opts \\ []) when is_list(execution_ids) do
    ids =
      execution_ids
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    if ids == [] do
      MapSet.new()
    else
      repo = repo!(opts)
      prefix = Keyword.get(opts, :prefix, @default_prefix)

      {ids_sql, params} =
        ids
        |> Enum.with_index(1)
        |> Enum.reduce({[], []}, fn {id, idx}, {parts, acc} ->
          placeholder = "$#{idx}"
          {[placeholder | parts], [to_db_id(id) | acc]}
        end)

      sql = """
      SELECT e.id
      FROM #{prefix}.endurant_executions e
      WHERE e.id IN (#{ids_sql |> Enum.reverse() |> Enum.join(", ")})
      AND (
        e.status = 'continuable'::#{prefix}.endurant_execution_status
        OR (
          e.status = 'waiting'::#{prefix}.endurant_execution_status
          AND e.waiting_until IS NOT NULL
          AND e.waiting_until <= timezone('UTC', now())
        )
      )
      """

      query!(repo, sql, Enum.reverse(params)).rows
      |> Enum.reduce(MapSet.new(), fn [id], acc ->
        MapSet.put(acc, to_app_id(id))
      end)
    end
  end

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    repo.query!(sql, params, log: false)
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
  end

  @spec parse_status(String.t()) :: atom()
  defp parse_status(status) do
    case status do
      "pending" -> :pending
      "running" -> :running
      "waiting" -> :waiting
      "continuable" -> :continuable
      "abandoned" -> :abandoned
      "cancelling" -> :cancelling
      "completed" -> :completed
      "failed" -> :failed
      "cancelled" -> :cancelled
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

  @spec resolve_unique_id(map(), map()) :: String.t()
  defp resolve_unique_id(workflow, input) do
    case Map.get(workflow, :unique_id) do
      nil ->
        raise ArgumentError, "workflow must define unique_id"

      fun when is_function(fun, 1) ->
        case fun.(input) do
          value when is_binary(value) ->
            value

          other ->
            raise ArgumentError,
                  "invalid unique_id from function: expected binary, got #{inspect(other)}"
        end

      value when is_binary(value) ->
        value

      other ->
        raise ArgumentError, "invalid unique_id: #{inspect(other)}"
    end
  end

  @spec resolve_queue(map()) :: String.t()
  defp resolve_queue(workflow) do
    case Map.get(workflow, :queue) do
      queue when is_atom(queue) -> Atom.to_string(queue)
      queue when is_binary(queue) -> queue
      nil -> "default"
      other -> raise ArgumentError, "invalid queue: #{inspect(other)}"
    end
  end

  @spec maybe_naive(DateTime.t() | nil) :: NaiveDateTime.t() | nil
  defp maybe_naive(nil), do: nil
  defp maybe_naive(%DateTime{} = dt), do: DateTime.to_naive(dt)

  @spec lock_execution_status(module(), String.t(), binary()) :: atom() | nil
  defp lock_execution_status(repo, prefix, execution_id) do
    sql = """
    SELECT status::text
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    FOR UPDATE
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[status]] -> parse_status(status)
      _ -> nil
    end
  end

  @spec normalize_queue(atom() | String.t()) :: String.t()
  defp normalize_queue(queue) when is_atom(queue), do: Atom.to_string(queue)
  defp normalize_queue(queue) when is_binary(queue), do: queue

  @spec normalize_signal(String.t()) :: String.t()
  defp normalize_signal(signal) when is_binary(signal), do: signal

  @spec latest_wait_signal_matches?(binary(), String.t(), module(), String.t()) :: boolean()
  defp latest_wait_signal_matches?(execution_id, signal_key, repo, prefix) do
    sql = """
    SELECT payload
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type = 'execution_waiting'::#{prefix}.endurant_event_type
    ORDER BY sequence DESC
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[%{"mode" => "signal", "signal" => ^signal_key}]] -> true
      [[%{mode: :signal, signal: ^signal_key}]] -> true
      _ -> false
    end
  end
end
