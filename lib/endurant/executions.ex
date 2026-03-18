defmodule Endurant.Executions do
  @moduledoc false

  alias Endurant.Events
  alias Endurant.Telemetry

  @default_prefix "public"
  @default_list_limit 50
  @max_list_limit 1000

  @open_status_strings ["pending", "running", "waiting", "continuable", "abandoned", "cancelling"]
  @terminal_status_strings ["completed", "failed", "cancelled", "continued_as_new"]
  @all_status_strings @open_status_strings ++ @terminal_status_strings

  @type execution_status ::
          :pending
          | :running
          | :waiting
          | :continuable
          | :abandoned
          | :cancelling
          | :completed
          | :failed
          | :cancelled
          | :continued_as_new

  @type execution :: %{
          id: binary(),
          queue: String.t(),
          workflow: module() | String.t(),
          input: map(),
          status: atom(),
          version: String.t(),
          metadata: map(),
          next_event_sequence: pos_integer(),
          history_size_bytes: non_neg_integer()
        }

  @type execution_summary :: %{
          id: binary(),
          unique_id: String.t(),
          queue: String.t(),
          workflow: String.t(),
          input: map(),
          status: execution_status(),
          version: String.t(),
          next_event_sequence: pos_integer(),
          history_size_bytes: non_neg_integer(),
          waiting_until: NaiveDateTime.t() | DateTime.t() | nil,
          locked_by: String.t() | nil,
          locked_until: NaiveDateTime.t() | DateTime.t() | nil,
          completed_at: NaiveDateTime.t() | DateTime.t() | nil,
          inserted_at: NaiveDateTime.t() | DateTime.t(),
          updated_at: NaiveDateTime.t() | DateTime.t()
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
    workflow = workflow_module.__workflow__()
    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = Map.get(workflow, :version, "1")

    case repo.transaction(
           fn -> insert_in_tx(workflow_module, input, opts) end,
           log: false
         ) do
      {:ok, {:ok, _execution} = result} ->
        Telemetry.emit(
          [:execution, :inserted],
          %{count: 1},
          execution_metadata(opts, queue, workflow_name, version)
        )

        result

      {:ok, {:error, :unique_conflict} = result} ->
        Telemetry.emit(
          [:execution, :insert_conflict],
          %{count: 1},
          execution_metadata(opts, queue, workflow_name, version)
        )

        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @spec insert_in_tx(module(), map(), keyword()) :: {:ok, map()} | {:error, :unique_conflict}
  def insert_in_tx(workflow_module, input, opts \\ [])
      when is_atom(workflow_module) and is_map(input) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    workflow = workflow_module.__workflow__()

    execution_id =
      normalize_execution_id!(Keyword.get(opts, :execution_id, Ecto.UUID.generate()), :execution)

    execution_db_id = to_db_id(execution_id)

    unique_id =
      normalize_unique_id!(Keyword.get(opts, :unique_id, resolve_unique_id(workflow, input)))

    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = normalize_version!(Keyword.get(opts, :version, Map.get(workflow, :version, "1")))
    metadata = normalize_execution_row_metadata(Keyword.get(opts, :metadata, %{}))

    sql = """
    INSERT INTO #{prefix}.endurant_executions
      (id, unique_id, queue, workflow_name, version, input, metadata, status, inserted_at, updated_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, 'pending'::#{prefix}.endurant_execution_status, timezone('UTC', now()), timezone('UTC', now()))
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

    case query!(repo, sql, [
           execution_db_id,
           unique_id,
           queue,
           workflow_name,
           version,
           input,
           metadata
         ]).rows do
      [[_]] ->
        :ok =
          Events.append_in_tx(
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
           metadata: metadata,
           status: :pending
         }}

      _ ->
        {:error, :unique_conflict}
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
    SELECT id, queue, workflow_name, input, status::text, version, metadata, next_event_sequence, history_size_bytes
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [
        [
          id,
          queue,
          workflow_name,
          input,
          status,
          version,
          metadata,
          next_event_sequence,
          history_size_bytes
        ]
      ] ->
        %{
          id: to_app_id(id),
          queue: queue,
          workflow: workflow_name,
          input: input || %{},
          status: parse_status(status),
          version: version,
          metadata: metadata || %{},
          next_event_sequence: next_event_sequence,
          history_size_bytes: history_size_bytes
        }

      _ ->
        nil
    end
  end

  @doc """
  Lists executions with optional filters.

  Supported filters:
    * `:status` - status atom/string or list of statuses.
    * `:queue` - queue atom/string or list of queues.
    * `:workflow` - workflow module/string or list.
    * `:execution_id` - one execution id.
    * `:execution_ids` - list of execution ids.
    * `:unique_id` - exact unique id match.
    * `:inserted_after` / `:inserted_before` - timestamp filters.
    * `:updated_after` / `:updated_before` - timestamp filters.
    * `:open` - include open lifecycle statuses.
    * `:terminal` - include terminal lifecycle statuses.
    * `:cursor` - keyset cursor `%{inserted_at: ..., id: ...}` or `{inserted_at, id}`.
    * `:order` - `:asc | :desc` (default `:desc`) by `inserted_at, id`.
    * `:limit` - max rows (default `50`, max `1000`).
  """
  @spec list(keyword(), keyword()) :: [execution_summary()]
  def list(filters \\ [], opts \\ []) when is_list(filters) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    limit = normalize_list_limit!(filters)
    order_direction = normalize_order_direction!(filters)

    {clauses, params, next_index} = build_list_clauses(filters, prefix)
    where_sql = where_sql(clauses)

    sql = """
    SELECT
      id,
      unique_id,
      queue,
      workflow_name,
      version,
      input,
      status::text,
      next_event_sequence,
      history_size_bytes,
      waiting_until,
      locked_by,
      locked_until,
      completed_at,
      inserted_at,
      updated_at
    FROM #{prefix}.endurant_executions
    #{where_sql}
    ORDER BY inserted_at #{order_direction}, id #{order_direction}
    LIMIT $#{next_index}
    """

    query!(repo, sql, params ++ [limit]).rows
    |> Enum.map(fn [
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
                     updated_at
                   ] ->
      %{
        id: to_app_id(id),
        unique_id: unique_id,
        queue: queue,
        workflow: workflow_name,
        input: input || %{},
        status: parse_status(status),
        version: version,
        next_event_sequence: next_event_sequence,
        history_size_bytes: history_size_bytes,
        waiting_until: waiting_until,
        locked_by: locked_by,
        locked_until: locked_until,
        completed_at: completed_at,
        inserted_at: inserted_at,
        updated_at: updated_at
      }
    end)
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
               previous_status = parse_status(previous_status)

               execution = %{
                 id: to_app_id(id),
                 queue: queue,
                 workflow: workflow_name,
                 input: input,
                 status: :running,
                 version: version,
                 previous_status: previous_status
               }

               if previous_status == :abandoned do
                 Events.append(execution.id, :execution_resumed, %{worker_id: worker_id}, opts)
               end

               execution
             end)
           end,
           log: false
         ) do
      {:ok, executions} ->
        Enum.each(executions, fn execution ->
          Telemetry.emit(
            [:execution, :claimed],
            %{count: 1},
            execution_metadata(
              opts,
              execution.queue,
              execution.workflow,
              execution.version,
              %{previous_status: execution.previous_status}
            )
          )

          if execution.previous_status == :abandoned do
            Telemetry.emit(
              [:execution, :resumed],
              %{count: 1},
              execution_metadata(
                opts,
                execution.queue,
                execution.workflow,
                execution.version,
                %{previous_status: execution.previous_status}
              )
            )
          end
        end)

        Enum.map(executions, &Map.delete(&1, :previous_status))

      {:error, reason} ->
        raise "claim_pending failed: #{inspect(reason)}"
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
    RETURNING queue, workflow_name, version, inserted_at
    """

    case query!(repo, sql, [execution_id, worker_id]).rows do
      [[queue, workflow_name, version, inserted_at]] ->
        Events.append(execution_id, :execution_started, %{worker_id: worker_id}, opts)

        Telemetry.emit(
          [:execution, :started],
          %{
            count: 1,
            time_to_start_ms: Telemetry.datetime_diff_ms(inserted_at, NaiveDateTime.utc_now())
          },
          execution_metadata(opts, queue, workflow_name, version)
        )

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

                 apply_parent_close_policies_in_tx(repo, prefix, to_app_id(execution_id), opts)

                 {
                   terminal_execution_context(repo, prefix, execution_id),
                   maybe_record_child_terminal_event(
                     to_app_id(execution_id),
                     :child_execution_completed,
                     %{result: result},
                     opts
                   )
                 }

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, {execution_context, child_metric}} ->
        Telemetry.emit(
          [:execution, :completed],
          execution_terminal_measurements(execution_context),
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version
          )
        )

        emit_child_metric(child_metric)

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
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

                 apply_parent_close_policies_in_tx(repo, prefix, to_app_id(execution_id), opts)

                 {
                   terminal_execution_context(repo, prefix, execution_id),
                   maybe_record_child_terminal_event(
                     to_app_id(execution_id),
                     :child_execution_failed,
                     %{error: error},
                     opts
                   )
                 }

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, {execution_context, child_metric}} ->
        Telemetry.emit(
          [:execution, :failed],
          execution_terminal_measurements(execution_context),
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version,
            %{error_kind: Telemetry.error_kind(error)}
          )
        )

        emit_child_metric(child_metric)

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
    end
  end

  @spec continue_as_new_owned(binary(), String.t(), map(), pos_integer(), keyword()) ::
          {:ok, execution()} | {:error, :not_running | :cancelled | term()}
  def continue_as_new_owned(execution_id, worker_id, continue_as_new, lease_ms, opts \\ [])
      when is_binary(worker_id) and is_integer(lease_ms) and lease_ms > 0 and
             is_map(continue_as_new) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id = to_db_id(execution_id)
    next_input = Map.fetch!(continue_as_new, :next_input)
    next_version = Map.get(continue_as_new, :version)
    rollover_signals = Map.get(continue_as_new, :rollover_signals, false)
    loaded_signal_seq = Map.get(continue_as_new, :loaded_signal_seq, 0)
    signal_queues = Map.get(continue_as_new, :signal_queues, %{})
    first_execution_id = Map.get(continue_as_new, :first_execution_id)

    case repo.transaction(
           fn ->
             with {:ok, current} <-
                    fetch_running_execution_for_continue(repo, prefix, execution_id, worker_id) do
               first_execution_id =
                 if is_binary(first_execution_id) and first_execution_id != "" do
                   first_execution_id
                 else
                   to_app_id(execution_id)
                 end

               next_version =
                 if is_binary(next_version) and next_version != "" do
                   next_version
                 else
                   current.version
                 end

               merged_signal_queues =
                 if rollover_signals do
                   merge_late_signals(
                     signal_queues,
                     execution_id,
                     loaded_signal_seq,
                     repo,
                     prefix
                   )
                 else
                   %{}
                 end

               case update_execution_for_continue(repo, prefix, execution_id, worker_id).num_rows do
                 1 ->
                   next_execution_id = Ecto.UUID.generate()

                   case insert_continued_execution(
                          repo,
                          prefix,
                          current,
                          next_execution_id,
                          next_input,
                          next_version,
                          worker_id,
                          lease_ms
                        ) do
                     {:ok, next_execution} ->
                       Events.append(
                         to_app_id(execution_id),
                         :execution_continued_as_new,
                         %{
                           new_execution_id: next_execution.id,
                           first_execution_id: first_execution_id
                         },
                         opts
                       )

                       Events.append(
                         next_execution.id,
                         :execution_created,
                         %{
                           workflow: current.workflow,
                           unique_id: current.unique_id,
                           version: next_execution.version
                         },
                         opts
                       )

                       Events.append(
                         next_execution.id,
                         :execution_started,
                         %{
                           worker_id: worker_id,
                           first_execution_id: first_execution_id,
                           previous_execution_id: to_app_id(execution_id)
                         },
                         opts
                       )

                       Enum.each(Enum.sort(Map.keys(merged_signal_queues)), fn signal ->
                         queue =
                           case Map.fetch!(merged_signal_queues, signal) do
                             value when is_list(value) -> :queue.from_list(value)
                             value -> value
                           end

                         queue
                         |> :queue.to_list()
                         |> Enum.each(fn payload ->
                           Events.append(
                             next_execution.id,
                             :signal_received,
                             %{signal: signal, payload: payload},
                             opts
                           )
                         end)
                       end)

                       apply_parent_close_policies_in_tx(
                         repo,
                         prefix,
                         to_app_id(execution_id),
                         opts
                       )

                       next_execution

                     {:error, reason} ->
                       repo.rollback(reason)
                   end

                 _ ->
                   repo.rollback(:not_running)
               end
             else
               {:error, reason} ->
                 repo.rollback(reason)
             end
           end,
           log: false
         ) do
      {:ok, next_execution} ->
        Telemetry.emit(
          [:execution, :continued_as_new],
          %{count: 1},
          execution_metadata(
            opts,
            next_execution.queue,
            to_string(next_execution.workflow),
            next_execution.version
          )
        )

        {:ok, next_execution}

      {:error, :not_running} ->
        {:error, :not_running}

      {:error, :cancelled} ->
        {:error, :cancelled}

      {:error, reason} ->
        {:error, reason}
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

                 basic_execution_context(repo, prefix, execution_id_db)

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, execution_context} ->
        Telemetry.emit(
          [:execution, :waiting],
          %{count: 1},
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version,
            %{wait_kind: :signal}
          )
        )

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
    end
  end

  @doc false
  @spec mark_waiting_with_child_event_owned(
          binary(),
          String.t(),
          String.t(),
          String.t() | nil,
          binary() | nil,
          keyword()
        ) :: :ok | :already_resolved | {:error, :not_running}
  def mark_waiting_with_child_event_owned(
        execution_id,
        worker_id,
        child_key,
        child_unique_id,
        child_execution_id,
        opts \\ []
      )
      when is_binary(worker_id) and is_binary(child_key) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    execution_id_db = to_db_id(execution_id)

    lock_sql = """
    SELECT queue, workflow_name, version
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    FOR UPDATE
    """

    update_sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'waiting'::#{prefix}.endurant_execution_status,
      waiting_until = NULL,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    """

    case repo.transaction(
           fn ->
             case query!(repo, lock_sql, [execution_id_db, worker_id]).rows do
               [[queue, workflow_name, version]] ->
                 if child_terminal_event_recorded?(execution_id_db, child_key, repo, prefix) do
                   :already_resolved
                 else
                   case query!(repo, update_sql, [execution_id_db]).num_rows do
                     1 ->
                       Events.append(
                         execution_id,
                         :execution_waiting,
                         %{
                           mode: :child,
                           child_key: child_key,
                           child_unique_id: child_unique_id,
                           child_execution_id: child_execution_id
                         },
                         opts
                       )

                       %{queue: queue, workflow: workflow_name, version: version}

                     _ ->
                       repo.rollback(:not_running)
                   end
                 end

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, :already_resolved} ->
        :already_resolved

      {:ok, execution_context} ->
        Telemetry.emit(
          [:execution, :waiting],
          %{count: 1},
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version,
            %{wait_kind: :child}
          )
        )

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
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

                 basic_execution_context(repo, prefix, execution_id_db)

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, execution_context} ->
        Telemetry.emit(
          [:execution, :waiting],
          %{count: 1},
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version,
            %{wait_kind: :time}
          )
        )

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
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
               SELECT id, locked_until, status::text, queue, workflow_name, version
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
             RETURNING
               locked_execution.locked_until,
               locked_execution.status,
               locked_execution.queue,
               locked_execution.workflow_name,
               locked_execution.version
             """

             case query!(repo, sql, [execution_id_db, worker_id]).rows do
               [[locked_until, previous_status, queue, workflow_name, version]] ->
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

                 append_abandoned_execution_events(
                   repo,
                   prefix,
                   execution_id,
                   abandoned_at,
                   opts
                 )

                 %{
                   queue: queue,
                   workflow: workflow_name,
                   version: version,
                   previous_status: parse_status(previous_status)
                 }

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, execution_context} ->
        Telemetry.emit(
          [:execution, :released],
          %{count: 1},
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version,
            %{previous_status: execution_context.previous_status}
          )
        )

        :ok

      {:error, :not_running} ->
        {:error, :not_running}
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

                   branch_rows =
                     query!(repo, sql, [queue, remaining, worker_id, lease_ms]).rows
                     |> Enum.map(&{branch, &1})

                   {acc ++ branch_rows, max(remaining - length(branch_rows), 0)}
                 end
               end)

             Enum.map(rows, fn {branch, [id, workflow_name, input, version]} ->
               execution = %{
                 id: to_app_id(id),
                 queue: queue,
                 workflow: workflow_name,
                 input: input,
                 status: :running,
                 version: version,
                 claim_branch: branch,
                 previous_status: claim_branch_previous_status(branch)
               }

               Events.append(execution.id, :execution_resumed, %{worker_id: worker_id}, opts)

               execution
             end)
           end,
           log: false
         ) do
      {:ok, executions} ->
        Enum.each(executions, fn execution ->
          Telemetry.emit(
            [:execution, :resumed],
            %{count: 1},
            execution_metadata(
              opts,
              execution.queue,
              execution.workflow,
              execution.version,
              %{
                previous_status: execution.previous_status,
                claim_branch: execution.claim_branch
              }
            )
          )
        end)

        Enum.map(executions, &Map.drop(&1, [:claim_branch, :previous_status]))

      {:error, reason} ->
        raise "claim_ready_waiting failed: #{inspect(reason)}"
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
    RETURNING queue, workflow_name, version
    """

    case query!(repo, sql, [execution_id]).rows do
      [[queue, workflow_name, version]] ->
        Telemetry.emit(
          [:execution, :continuable],
          %{count: 1},
          execution_metadata(opts, queue, workflow_name, version)
        )

        :ok

      _ ->
        :ok
    end
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

    telemetry =
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

            append_abandoned_execution_events(repo, prefix, to_app_id(id), abandoned_at, opts)
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
          RETURNING e.id, e.queue, e.workflow_name, e.version, e.inserted_at
          """

          cancelled_rows = query!(repo, expire_cancelling_sql, [queue, limit]).rows

          cancelled_telemetry =
            Enum.map(cancelled_rows, fn [id, queue_name, workflow_name, version, inserted_at] ->
              Events.append(to_app_id(id), :execution_cancelled, %{}, opts)

              %{
                queue: queue_name,
                workflow: workflow_name,
                version: version,
                inserted_at: inserted_at
              }
            end)

          recover_order = recover_runnable_branch_order(opts)

          {rows, _remaining} =
            Enum.reduce(recover_order, {[], limit}, fn branch, {acc, remaining} ->
              if remaining <= 0 do
                {acc, 0}
              else
                sql = recover_runnable_sql(prefix, branch)

                branch_rows =
                  query!(repo, sql, [queue, remaining]).rows
                  |> Enum.map(&{branch, &1})

                {acc ++ branch_rows, max(remaining - length(branch_rows), 0)}
              end
            end)

          recovered_telemetry =
            Enum.reduce(rows, [], fn
              {_branch, [_id, _queue_name, _locked_until, true, true]}, acc ->
                acc

              {branch,
               [id, queue_name, locked_until, _pre_orphaned_waiting, _has_abandoned_event]},
              acc ->
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

                append_abandoned_execution_events(repo, prefix, to_app_id(id), abandoned_at, opts)

                [
                  %{queue: queue_name, status: :abandoned, recover_branch: branch}
                  | acc
                ]
            end)

          %{cancelled: cancelled_telemetry, recovered: Enum.reverse(recovered_telemetry)}
        end,
        log: false
      )
      |> case do
        {:ok, telemetry} -> telemetry
        {:error, reason} -> raise "recover_expired_locks failed: #{inspect(reason)}"
      end

    Enum.each(telemetry.cancelled, fn execution_context ->
      Telemetry.emit(
        [:execution, :cancelled],
        execution_cancelled_measurements(execution_context),
        execution_metadata(
          opts,
          execution_context.queue,
          execution_context.workflow,
          execution_context.version
        )
      )
    end)

    Enum.each(telemetry.recovered, fn recovered ->
      Telemetry.emit(
        [:execution, :recovered],
        %{count: 1},
        %{
          instance: Keyword.get(opts, :instance),
          node: node(),
          queue: recovered.queue,
          status: recovered.status,
          recover_branch: recovered.recover_branch
        }
      )
    end)

    length(telemetry.recovered)
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
        e.queue,
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
      e.queue,
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
        e.queue,
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
      e.queue,
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
        e.queue,
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
      e.queue,
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
    signal_target = execution_id
    signal_name = normalize_signal(signal)
    wait_signal_key = signal_name

    case repo.transaction(
           fn ->
             case resolve_signal_target(repo, prefix, signal_target) do
               nil ->
                 repo.rollback(signal_target_not_found_reason(repo, prefix, signal_target))

               execution
               when execution.status in [:pending, :running, :waiting, :continuable, :abandoned] ->
                 Events.append(
                   execution.id,
                   :signal_received,
                   %{signal: signal_name, payload: payload || %{}},
                   opts
                 )

                 wakeup =
                   execution.status == :waiting and
                     latest_wait_signal_matches?(
                       to_db_id(execution.id),
                       wait_signal_key,
                       repo,
                       prefix
                     )

                 if wakeup do
                   mark_continuable(execution.id, opts)
                 end

                 %{execution: execution, wakeup: wakeup}

               _status ->
                 repo.rollback(:not_active)
             end
           end,
           log: false
         ) do
      {:ok, %{execution: execution, wakeup: wakeup}} ->
        Telemetry.emit(
          [:execution, :signal_received],
          %{count: 1},
          execution_metadata(
            opts,
            execution.queue,
            execution.workflow,
            execution.version,
            %{wakeup: wakeup}
          )
        )

        :ok

      {:error, reason} ->
        {:error, reason}
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
             case lock_execution(repo, prefix, execution_id) do
               nil ->
                 repo.rollback(:not_found)

               execution when execution.status in [:completed, :failed] ->
                 repo.rollback(:not_active)

               %{status: :cancelled} ->
                 {:already_cancelled, nil}

               %{status: :cancelling} ->
                 {:already_cancelling, nil}

               %{status: :running} = execution ->
                 mark_cancelling_in_tx(repo, prefix, execution_id, opts)
                 {:running, execution}

               %{status: status} = execution
               when status in [:pending, :waiting, :continuable, :abandoned] ->
                 mark_cancelling_in_tx(repo, prefix, execution_id, opts)
                 {:finalize_now, execution}

               _ ->
                 repo.rollback(:not_active)
             end
           end,
           log: false
         ) do
      {:ok, {state, execution}} ->
        if state in [:running, :finalize_now] and execution do
          Telemetry.emit(
            [:execution, :cancel_requested],
            %{count: 1},
            execution_metadata(
              opts,
              execution.queue,
              execution.workflow,
              execution.version,
              %{status: execution.status}
            )
          )
        end

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
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
             case lock_execution(repo, prefix, execution_id) do
               nil ->
                 nil

               %{status: :cancelled} ->
                 nil

               %{status: :cancelling} ->
                 finalize_cancel_in_tx(repo, prefix, execution_id, opts)

               _ ->
                 repo.rollback(:invalid_transition)
             end
           end,
           log: false
         ) do
      {:ok, nil} ->
        :ok

      {:ok, {execution_context, child_metric}} ->
        Telemetry.emit(
          [:execution, :cancelled],
          execution_cancelled_measurements(execution_context),
          execution_metadata(
            opts,
            execution_context.queue,
            execution_context.workflow,
            execution_context.version
          )
        )

        emit_child_metric(child_metric)

        :ok

      {:error, :invalid_transition} ->
        {:error, :invalid_transition}

      {:error, _reason} ->
        {:error, :invalid_transition}
    end
  end

  @spec mark_cancelling_in_tx(module(), String.t(), binary(), keyword()) :: :ok
  defp mark_cancelling_in_tx(repo, prefix, execution_id, opts) do
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

  @spec finalize_cancel_in_tx(module(), String.t(), binary(), keyword()) :: map() | :ok
  defp finalize_cancel_in_tx(repo, prefix, execution_id, opts) do
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
    RETURNING id
    """

    case query!(repo, sql, [execution_id]).rows do
      [[_id]] ->
        Events.append(execution_id, :execution_cancelled, %{}, opts)

        apply_parent_close_policies_in_tx(repo, prefix, to_app_id(execution_id), opts)

        {
          terminal_execution_context(repo, prefix, execution_id),
          maybe_record_child_terminal_event(
            to_app_id(execution_id),
            :child_execution_cancelled,
            %{},
            opts
          )
        }

      _ ->
        :ok
    end
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

  @spec build_list_clauses(keyword(), String.t()) :: {[String.t()], list(), pos_integer()}
  defp build_list_clauses(filters, prefix) do
    {clauses, params, next_index} = {[], [], 1}
    statuses = normalize_status_filter!(filters)
    queues = normalize_value_filter!(filters, :queue, &normalize_queue_value!/1)
    workflows = normalize_value_filter!(filters, :workflow, &normalize_workflow_value!/1)
    execution_ids = normalize_execution_ids_filter!(filters)

    {clauses, params, next_index} =
      maybe_add_any_clause(
        clauses,
        params,
        next_index,
        "status",
        statuses,
        "#{prefix}.endurant_execution_status"
      )

    {clauses, params, next_index} =
      maybe_add_any_clause(clauses, params, next_index, "queue", queues, "text")

    {clauses, params, next_index} =
      maybe_add_any_clause(clauses, params, next_index, "workflow_name", workflows, "text")

    {clauses, params, next_index} =
      maybe_add_any_clause(
        clauses,
        params,
        next_index,
        "id",
        Enum.map(execution_ids, &to_db_id/1),
        "uuid"
      )

    {clauses, params, next_index} =
      maybe_add_unique_id_clause(clauses, params, next_index, Keyword.get(filters, :unique_id))

    {clauses, params, next_index} =
      maybe_add_time_clause(
        clauses,
        params,
        next_index,
        "inserted_at",
        ">=",
        Keyword.get(filters, :inserted_after),
        :inserted_after
      )

    {clauses, params, next_index} =
      maybe_add_time_clause(
        clauses,
        params,
        next_index,
        "inserted_at",
        "<",
        Keyword.get(filters, :inserted_before),
        :inserted_before
      )

    {clauses, params, next_index} =
      maybe_add_time_clause(
        clauses,
        params,
        next_index,
        "updated_at",
        ">=",
        Keyword.get(filters, :updated_after),
        :updated_after
      )

    {clauses, params, next_index} =
      maybe_add_time_clause(
        clauses,
        params,
        next_index,
        "updated_at",
        "<",
        Keyword.get(filters, :updated_before),
        :updated_before
      )

    maybe_add_cursor_clause(
      clauses,
      params,
      next_index,
      Keyword.get(filters, :cursor),
      normalize_order_direction!(filters)
    )
  end

  @spec maybe_add_any_clause(
          [String.t()],
          list(),
          pos_integer(),
          String.t(),
          [term()],
          String.t()
        ) ::
          {[String.t()], list(), pos_integer()}
  defp maybe_add_any_clause(clauses, params, next_index, _column, [], _type_name),
    do: {clauses, params, next_index}

  defp maybe_add_any_clause(clauses, params, next_index, column, values, type_name) do
    {
      clauses ++ ["#{column} = ANY($#{next_index}::#{type_name}[])"],
      params ++ [values],
      next_index + 1
    }
  end

  @spec maybe_add_unique_id_clause([String.t()], list(), pos_integer(), term()) ::
          {[String.t()], list(), pos_integer()}
  defp maybe_add_unique_id_clause(clauses, params, next_index, nil),
    do: {clauses, params, next_index}

  defp maybe_add_unique_id_clause(clauses, params, next_index, unique_id)
       when is_binary(unique_id) do
    {clauses ++ ["unique_id = $#{next_index}"], params ++ [unique_id], next_index + 1}
  end

  defp maybe_add_unique_id_clause(_clauses, _params, _next_index, other) do
    raise ArgumentError, ":unique_id must be a binary, got: #{inspect(other)}"
  end

  @spec maybe_add_time_clause(
          [String.t()],
          list(),
          pos_integer(),
          String.t(),
          String.t(),
          term(),
          atom()
        ) :: {[String.t()], list(), pos_integer()}
  defp maybe_add_time_clause(clauses, params, next_index, _column, _operator, nil, _filter_key),
    do: {clauses, params, next_index}

  defp maybe_add_time_clause(
         clauses,
         params,
         next_index,
         column,
         operator,
         filter_value,
         filter_key
       ) do
    timestamp = normalize_filter_timestamp!(filter_value, filter_key)

    {
      clauses ++ ["#{column} #{operator} $#{next_index}"],
      params ++ [timestamp],
      next_index + 1
    }
  end

  @spec maybe_add_cursor_clause([String.t()], list(), pos_integer(), term(), String.t()) ::
          {[String.t()], list(), pos_integer()}
  defp maybe_add_cursor_clause(clauses, params, next_index, cursor, order_direction) do
    case normalize_cursor_filter!(cursor) do
      nil ->
        {clauses, params, next_index}

      {inserted_at, execution_id} ->
        operator =
          if order_direction == "DESC" do
            "<"
          else
            ">"
          end

        clause = "(inserted_at, id) #{operator} ($#{next_index}, $#{next_index + 1})"

        {
          clauses ++ [clause],
          params ++ [inserted_at, to_db_id(execution_id)],
          next_index + 2
        }
    end
  end

  @spec where_sql([String.t()]) :: String.t()
  defp where_sql([]), do: ""

  defp where_sql(clauses) do
    "WHERE " <> Enum.join(clauses, " AND ")
  end

  @spec normalize_list_limit!(keyword()) :: pos_integer()
  defp normalize_list_limit!(filters) do
    case Keyword.get(filters, :limit, @default_list_limit) do
      value when is_integer(value) and value > 0 ->
        min(value, @max_list_limit)

      other ->
        raise ArgumentError, ":limit must be a positive integer, got: #{inspect(other)}"
    end
  end

  @spec normalize_order_direction!(keyword()) :: String.t()
  defp normalize_order_direction!(filters) do
    case Keyword.get(filters, :order, :desc) do
      :desc -> "DESC"
      "desc" -> "DESC"
      :asc -> "ASC"
      "asc" -> "ASC"
      other -> raise ArgumentError, ":order must be :asc or :desc, got: #{inspect(other)}"
    end
  end

  @spec normalize_status_filter!(keyword()) :: [String.t()]
  defp normalize_status_filter!(filters) do
    explicit_statuses = normalize_value_filter!(filters, :status, &normalize_status_string!/1)
    open? = normalize_boolean_filter!(filters, :open)
    terminal? = normalize_boolean_filter!(filters, :terminal)

    statuses =
      explicit_statuses ++
        if(open?, do: @open_status_strings, else: []) ++
        if(terminal?, do: @terminal_status_strings, else: [])

    statuses =
      statuses
      |> Enum.uniq()
      |> Enum.sort()

    if statuses == Enum.sort(@all_status_strings) do
      []
    else
      statuses
    end
  end

  @spec normalize_boolean_filter!(keyword(), atom()) :: boolean()
  defp normalize_boolean_filter!(filters, key) do
    case Keyword.get(filters, key, false) do
      true -> true
      false -> false
      nil -> false
      other -> raise ArgumentError, "#{inspect(key)} must be boolean, got: #{inspect(other)}"
    end
  end

  @spec normalize_value_filter!(keyword(), atom(), (term() -> term())) :: [term()]
  defp normalize_value_filter!(filters, key, normalizer) do
    case Keyword.get(filters, key) do
      nil ->
        []

      values when is_list(values) ->
        values
        |> Enum.map(normalizer)
        |> Enum.uniq()

      value ->
        [normalizer.(value)]
    end
  end

  @spec normalize_execution_ids_filter!(keyword()) :: [binary()]
  defp normalize_execution_ids_filter!(filters) do
    ids =
      [Keyword.get(filters, :execution_id)] ++
        case Keyword.get(filters, :execution_ids) do
          nil -> []
          values when is_list(values) -> values
          other -> raise ArgumentError, ":execution_ids must be a list, got: #{inspect(other)}"
        end

    ids
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn
      value when is_binary(value) ->
        value

      other ->
        raise ArgumentError, "execution id filters must be binaries, got: #{inspect(other)}"
    end)
    |> Enum.uniq()
  end

  @spec normalize_status_string!(term()) :: String.t()
  defp normalize_status_string!(status) when is_atom(status) do
    normalize_status_string!(Atom.to_string(status))
  end

  defp normalize_status_string!(status) when is_binary(status) do
    if status in @all_status_strings do
      status
    else
      raise ArgumentError, "unknown status filter: #{inspect(status)}"
    end
  end

  defp normalize_status_string!(other) do
    raise ArgumentError, "status filter must be atom or binary, got: #{inspect(other)}"
  end

  @spec normalize_queue_value!(term()) :: String.t()
  defp normalize_queue_value!(queue) when is_atom(queue), do: Atom.to_string(queue)
  defp normalize_queue_value!(queue) when is_binary(queue), do: queue

  defp normalize_queue_value!(other) do
    raise ArgumentError, "queue filter must be atom, binary, or list, got: #{inspect(other)}"
  end

  @spec normalize_workflow_value!(term()) :: String.t()
  defp normalize_workflow_value!(workflow) when is_atom(workflow), do: inspect(workflow)
  defp normalize_workflow_value!(workflow) when is_binary(workflow), do: workflow

  defp normalize_workflow_value!(other) do
    raise ArgumentError, "workflow filter must be module or binary, got: #{inspect(other)}"
  end

  @spec normalize_cursor_filter!(term()) :: {NaiveDateTime.t(), binary()} | nil
  defp normalize_cursor_filter!(nil), do: nil

  defp normalize_cursor_filter!({inserted_at, execution_id}) do
    {normalize_filter_timestamp!(inserted_at, :cursor),
     normalize_execution_id!(execution_id, :cursor)}
  end

  defp normalize_cursor_filter!(cursor) when is_map(cursor) do
    inserted_at = Map.get(cursor, :inserted_at) || Map.get(cursor, "inserted_at")

    execution_id =
      Map.get(cursor, :id) || Map.get(cursor, "id") || Map.get(cursor, :execution_id) ||
        Map.get(cursor, "execution_id")

    if is_nil(inserted_at) or is_nil(execution_id) do
      raise ArgumentError, ":cursor map must include :inserted_at and :id"
    end

    {normalize_filter_timestamp!(inserted_at, :cursor),
     normalize_execution_id!(execution_id, :cursor)}
  end

  defp normalize_cursor_filter!(other) do
    raise ArgumentError,
          ":cursor must be %{inserted_at: ..., id: ...} or {inserted_at, id}, got: #{inspect(other)}"
  end

  @spec normalize_execution_id!(term(), atom()) :: binary()
  defp normalize_execution_id!(execution_id, _filter_key) when is_binary(execution_id),
    do: execution_id

  defp normalize_execution_id!(other, filter_key) do
    raise ArgumentError,
          "#{inspect(filter_key)} execution id must be a binary, got: #{inspect(other)}"
  end

  @spec normalize_unique_id!(term()) :: String.t()
  defp normalize_unique_id!(unique_id) when is_binary(unique_id) and byte_size(unique_id) > 0,
    do: unique_id

  defp normalize_unique_id!(other) do
    raise ArgumentError, "unique_id must be a non-empty binary, got: #{inspect(other)}"
  end

  @spec normalize_version!(term()) :: String.t()
  defp normalize_version!(version) when is_binary(version) and byte_size(version) > 0, do: version

  defp normalize_version!(other) do
    raise ArgumentError, "version must be a non-empty binary, got: #{inspect(other)}"
  end

  @spec normalize_execution_row_metadata(term()) :: map()
  defp normalize_execution_row_metadata(metadata) when is_map(metadata), do: metadata
  defp normalize_execution_row_metadata(nil), do: %{}

  defp normalize_execution_row_metadata(other) do
    raise ArgumentError, "metadata must be a map, got: #{inspect(other)}"
  end

  @spec normalize_filter_timestamp!(term(), atom()) :: NaiveDateTime.t()
  defp normalize_filter_timestamp!(%NaiveDateTime{} = timestamp, _filter_key), do: timestamp

  defp normalize_filter_timestamp!(%DateTime{} = timestamp, _filter_key) do
    DateTime.to_naive(timestamp)
  end

  defp normalize_filter_timestamp!(timestamp, _filter_key) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, value, _offset} ->
        DateTime.to_naive(value)

      {:error, _reason} ->
        case NaiveDateTime.from_iso8601(timestamp) do
          {:ok, value} -> value
          {:error, _} -> raise ArgumentError, "invalid ISO8601 timestamp: #{inspect(timestamp)}"
        end
    end
  end

  defp normalize_filter_timestamp!(other, filter_key) do
    raise ArgumentError,
          "#{inspect(filter_key)} must be DateTime, NaiveDateTime, or ISO8601 string, got: #{inspect(other)}"
  end

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    repo.query!(sql, params, log: false)
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
  end

  @spec append_abandoned_execution_events(module(), String.t(), binary(), String.t(), keyword()) ::
          :ok
  defp append_abandoned_execution_events(repo, prefix, execution_id, abandoned_at, opts) do
    execution_id_db = to_db_id(execution_id)

    repo
    |> open_task_runs(prefix, execution_id_db)
    |> Enum.each(fn %{task: task, task_run_id: task_run_id} ->
      Events.append(
        execution_id,
        :task_interrupted,
        %{task: task, task_run_id: task_run_id},
        opts
      )
    end)

    Events.append(
      execution_id,
      :execution_abandoned,
      %{abandoned_at: abandoned_at},
      opts
    )
  end

  @spec open_task_runs(module(), String.t(), binary()) :: [
          %{task: String.t(), task_run_id: String.t()}
        ]
  defp open_task_runs(repo, prefix, execution_id) do
    sql = """
    SELECT type::text, payload
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type::text IN ('task_started', 'task_completed', 'task_failed', 'task_interrupted')
    ORDER BY sequence ASC
    """

    repo
    |> query!(sql, [execution_id])
    |> Map.fetch!(:rows)
    |> Enum.reduce(%{}, fn [type, payload], acc ->
      case {type, payload_task(payload), payload_task_run_id(payload)} do
        {"task_started", task, task_run_id}
        when is_binary(task) and is_binary(task_run_id) and task_run_id != "" ->
          Map.put(acc, task_run_id, %{task: task, task_run_id: task_run_id})

        {terminal_type, _task, task_run_id}
        when terminal_type in ["task_completed", "task_failed", "task_interrupted"] and
               is_binary(task_run_id) and task_run_id != "" ->
          Map.delete(acc, task_run_id)

        _ ->
          acc
      end
    end)
    |> Map.values()
  end

  @spec payload_task(map() | nil) :: String.t() | nil
  defp payload_task(payload) when is_map(payload) do
    Map.get(payload, "task") || Map.get(payload, :task)
  end

  defp payload_task(_payload), do: nil

  @spec payload_task_run_id(map() | nil) :: String.t() | nil
  defp payload_task_run_id(payload) when is_map(payload) do
    Map.get(payload, "task_run_id") || Map.get(payload, :task_run_id)
  end

  defp payload_task_run_id(_payload), do: nil

  @spec claim_branch_previous_status(claim_ready_branch()) :: :continuable | :waiting
  defp claim_branch_previous_status(:continuable), do: :continuable
  defp claim_branch_previous_status(:waiting_ready), do: :waiting

  @spec execution_metadata(keyword(), String.t(), String.t(), String.t(), map()) :: map()
  defp execution_metadata(opts, queue, workflow, version, extra \\ %{}) do
    Map.merge(
      %{
        instance: Keyword.get(opts, :instance),
        node: node(),
        queue: queue,
        workflow: workflow,
        version: version
      },
      extra
    )
  end

  @spec basic_execution_context(module(), String.t(), binary()) :: map()
  defp basic_execution_context(repo, prefix, execution_id) do
    sql = """
    SELECT queue, workflow_name, version
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[queue, workflow_name, version]] ->
        %{queue: queue, workflow: workflow_name, version: version}

      _ ->
        raise "missing execution telemetry context for #{inspect(to_app_id(execution_id))}"
    end
  end

  @spec terminal_execution_context(module(), String.t(), binary()) :: map()
  defp terminal_execution_context(repo, prefix, execution_id) do
    sql = """
    SELECT queue, workflow_name, version, inserted_at, next_event_sequence, history_size_bytes
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[queue, workflow_name, version, inserted_at, next_event_sequence, history_size_bytes]] ->
        %{
          queue: queue,
          workflow: workflow_name,
          version: version,
          inserted_at: inserted_at,
          next_event_sequence: next_event_sequence,
          history_size_bytes: history_size_bytes
        }

      _ ->
        raise "missing execution terminal context for #{inspect(to_app_id(execution_id))}"
    end
  end

  @spec execution_terminal_measurements(map()) :: map()
  defp execution_terminal_measurements(execution_context) do
    %{
      count: 1,
      duration_ms: execution_duration_ms(execution_context),
      history_length: max(execution_context.next_event_sequence - 1, 0),
      history_size_bytes: execution_context.history_size_bytes
    }
  end

  @spec execution_cancelled_measurements(map()) :: map()
  defp execution_cancelled_measurements(execution_context) do
    %{count: 1, duration_ms: execution_duration_ms(execution_context)}
  end

  @spec execution_duration_ms(map()) :: non_neg_integer()
  defp execution_duration_ms(%{inserted_at: inserted_at}) do
    Telemetry.datetime_diff_ms(inserted_at, NaiveDateTime.utc_now())
  end

  @spec emit_child_metric(map() | nil) :: :ok
  defp emit_child_metric(nil), do: :ok

  defp emit_child_metric(%{event: event, measurements: measurements, metadata: metadata}) do
    Telemetry.emit([:child, event], measurements, metadata)
  end

  @spec child_metric_context(module(), String.t(), map(), binary(), atom(), map(), keyword()) ::
          map()
  defp child_metric_context(
         repo,
         prefix,
         child_context,
         child_execution_id,
         event_type,
         payload,
         opts
       ) do
    parent_context =
      basic_execution_context(repo, prefix, to_db_id(child_context.parent_execution_id))

    child_execution_context = basic_execution_context(repo, prefix, to_db_id(child_execution_id))

    metadata =
      execution_metadata(
        opts,
        parent_context.queue,
        parent_context.workflow,
        parent_context.version,
        %{
          child_workflow: child_execution_context.workflow,
          child_version: child_execution_context.version,
          close_policy: child_context.parent_close_policy
        }
      )

    metadata =
      case event_type do
        :child_execution_failed ->
          Map.put(metadata, :error_kind, Telemetry.error_kind(payload_value(payload, "error")))

        _ ->
          metadata
      end

    %{
      event: child_metric_event_name(event_type),
      measurements: %{count: 1},
      metadata: metadata
    }
  end

  @spec child_metric_event_name(atom()) :: :completed | :failed | :cancelled
  defp child_metric_event_name(:child_execution_completed), do: :completed
  defp child_metric_event_name(:child_execution_failed), do: :failed
  defp child_metric_event_name(:child_execution_cancelled), do: :cancelled

  @spec payload_value(map(), String.t()) :: term()
  defp payload_value(payload, key) do
    Map.get(payload, key) || Map.get(payload, String.to_atom(key))
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
      "continued_as_new" -> :continued_as_new
    end
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) when is_binary(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec maybe_to_db_id(binary()) :: {:ok, binary()} | :error
  defp maybe_to_db_id(id) when is_binary(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> {:ok, dumped}
      :error when byte_size(id) == 16 -> {:ok, id}
      :error -> :error
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
    case lock_execution(repo, prefix, execution_id) do
      %{status: status} -> status
      nil -> nil
    end
  end

  @spec lock_execution(module(), String.t(), binary()) :: map() | nil
  defp lock_execution(repo, prefix, execution_id) do
    sql = """
    SELECT id, unique_id, status::text, queue, workflow_name, version, metadata
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    FOR UPDATE
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[id, unique_id, status, queue, workflow_name, version, metadata]] ->
        %{
          id: to_app_id(id),
          unique_id: unique_id,
          status: parse_status(status),
          queue: queue,
          workflow: workflow_name,
          version: version,
          metadata: metadata || %{}
        }

      _ ->
        nil
    end
  end

  @spec normalize_queue(atom() | String.t()) :: String.t()
  defp normalize_queue(queue) when is_atom(queue), do: Atom.to_string(queue)
  defp normalize_queue(queue) when is_binary(queue), do: queue

  @spec normalize_signal(String.t()) :: String.t()
  defp normalize_signal(signal) when is_binary(signal), do: signal

  @spec fetch_running_execution_for_continue(module(), String.t(), binary(), String.t()) ::
          {:ok, map()} | {:error, :not_running | :cancelled}
  defp fetch_running_execution_for_continue(repo, prefix, execution_id, worker_id) do
    sql = """
    SELECT id, unique_id, queue, workflow_name, version, metadata
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    FOR UPDATE
    LIMIT 1
    """

    case query!(repo, sql, [execution_id, worker_id]).rows do
      [[id, unique_id, queue, workflow_name, version, metadata]] ->
        {:ok,
         %{
           id: to_app_id(id),
           unique_id: unique_id,
           queue: queue,
           workflow: workflow_name,
           version: version,
           metadata: metadata || %{}
         }}

      _ ->
        case lock_execution_status(repo, prefix, execution_id) do
          status when status in [:cancelling, :cancelled] -> {:error, :cancelled}
          _ -> {:error, :not_running}
        end
    end
  end

  @spec insert_continued_execution(
          module(),
          String.t(),
          map(),
          binary(),
          map(),
          String.t(),
          String.t(),
          pos_integer()
        ) ::
          {:ok, execution()} | {:error, term()}
  defp insert_continued_execution(
         repo,
         prefix,
         current,
         next_execution_id,
         input,
         version,
         worker_id,
         lease_ms
       ) do
    execution_db_id = to_db_id(next_execution_id)

    sql = """
    INSERT INTO #{prefix}.endurant_executions
      (id, unique_id, queue, workflow_name, version, input, metadata, status, locked_by, locked_until, inserted_at, updated_at)
    VALUES
      (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        'running'::#{prefix}.endurant_execution_status,
        $8,
        timezone('UTC', now()) + ($9::int * interval '1 millisecond'),
        timezone('UTC', now()),
        timezone('UTC', now())
      )
    RETURNING id
    """

    case query!(repo, sql, [
           execution_db_id,
           current.unique_id,
           current.queue,
           current.workflow,
           version,
           input,
           Map.get(current, :metadata, %{}),
           worker_id,
           lease_ms
         ]).rows do
      [[_id]] ->
        {:ok,
         %{
           id: next_execution_id,
           queue: current.queue,
           workflow: current.workflow,
           input: input,
           status: :running,
           version: version,
           metadata: Map.get(current, :metadata, %{})
         }}

      _ ->
        {:error, :insert_failed}
    end
  end

  @spec update_execution_for_continue(module(), String.t(), binary(), String.t()) :: map()
  defp update_execution_for_continue(repo, prefix, execution_id, worker_id) do
    sql = """
    UPDATE #{prefix}.endurant_executions
    SET
      status = 'continued_as_new'::#{prefix}.endurant_execution_status,
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

    query!(repo, sql, [execution_id, worker_id])
  end

  @spec merge_late_signals(map(), binary(), non_neg_integer(), module(), String.t()) :: map()
  defp merge_late_signals(signal_queues, execution_id, loaded_signal_seq, repo, prefix) do
    sql = """
    SELECT payload
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND sequence > $2
    AND type = 'signal_received'::#{prefix}.endurant_event_type
    ORDER BY sequence ASC
    """

    Enum.reduce(query!(repo, sql, [execution_id, loaded_signal_seq]).rows, signal_queues, fn
      [%{"signal" => signal, "payload" => payload}], acc ->
        enqueue_signal_payload(acc, signal, payload)

      [%{signal: signal, payload: payload}], acc ->
        enqueue_signal_payload(acc, signal, payload)

      [_], acc ->
        acc
    end)
  end

  @spec enqueue_signal_payload(map(), String.t(), term()) :: map()
  defp enqueue_signal_payload(signal_queues, signal, payload) do
    Map.update(signal_queues, signal, :queue.in(payload, :queue.new()), fn queue ->
      queue =
        case queue do
          value when is_list(value) -> :queue.from_list(value)
          value -> value
        end

      :queue.in(payload, queue)
    end)
  end

  @spec resolve_signal_target(module(), String.t(), binary()) :: map() | nil
  defp resolve_signal_target(repo, prefix, execution_or_unique_id) do
    case maybe_to_db_id(execution_or_unique_id) do
      {:ok, execution_id} ->
        case lock_execution(repo, prefix, execution_id) do
          nil -> lock_open_execution_by_unique_id(repo, prefix, execution_or_unique_id)
          execution -> execution
        end

      :error ->
        lock_open_execution_by_unique_id(repo, prefix, execution_or_unique_id)
    end
  end

  @spec signal_target_not_found_reason(module(), String.t(), binary()) :: :not_found | :not_active
  defp signal_target_not_found_reason(repo, prefix, unique_id) do
    sql = """
    SELECT status::text
    FROM #{prefix}.endurant_executions
    WHERE unique_id = $1
    ORDER BY inserted_at DESC, id DESC
    LIMIT 1
    """

    case query!(repo, sql, [unique_id]).rows do
      [[status]] when status in ["completed", "failed", "cancelled", "continued_as_new"] ->
        :not_active

      _ ->
        :not_found
    end
  end

  @spec lock_open_execution_by_unique_id(module(), String.t(), binary()) :: map() | nil
  defp lock_open_execution_by_unique_id(repo, prefix, unique_id) do
    sql = """
    SELECT id, unique_id, status::text, queue, workflow_name, version
    FROM #{prefix}.endurant_executions
    WHERE unique_id = $1
    AND status IN (
      'pending'::#{prefix}.endurant_execution_status,
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'abandoned'::#{prefix}.endurant_execution_status,
      'cancelling'::#{prefix}.endurant_execution_status
    )
    ORDER BY inserted_at DESC, id DESC
    FOR UPDATE
    LIMIT 1
    """

    case query!(repo, sql, [unique_id]).rows do
      [[id, open_unique_id, status, queue, workflow_name, version]] ->
        %{
          id: to_app_id(id),
          unique_id: open_unique_id,
          status: parse_status(status),
          queue: queue,
          workflow: workflow_name,
          version: version
        }

      _ ->
        nil
    end
  end

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

  @spec latest_wait_child_matches?(binary(), String.t(), module(), String.t()) :: boolean()
  defp latest_wait_child_matches?(execution_id, child_key, repo, prefix) do
    sql = """
    SELECT payload
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type = 'execution_waiting'::#{prefix}.endurant_event_type
    ORDER BY sequence DESC
    LIMIT 1
    """

    case query!(repo, sql, [execution_id]).rows do
      [[%{"mode" => "child", "child_key" => ^child_key}]] -> true
      [[%{mode: :child, child_key: ^child_key}]] -> true
      _ -> false
    end
  end

  @spec child_terminal_event_recorded?(binary(), String.t(), module(), String.t()) :: boolean()
  defp child_terminal_event_recorded?(execution_id, child_key, repo, prefix) do
    sql = """
    SELECT 1
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type IN (
      'child_execution_completed'::#{prefix}.endurant_event_type,
      'child_execution_failed'::#{prefix}.endurant_event_type,
      'child_execution_cancelled'::#{prefix}.endurant_event_type
    )
    AND payload->>'child_key' = $2
    LIMIT 1
    """

    match?(%{rows: [[1]]}, query!(repo, sql, [execution_id, child_key]))
  end

  @spec maybe_record_child_terminal_event(binary(), atom(), map(), keyword()) :: map() | nil
  defp maybe_record_child_terminal_event(child_execution_id, event_type, payload, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    case child_parent_context(child_execution_id, repo, prefix) do
      nil ->
        nil

      child_context ->
        if lock_open_parent_execution(child_context.parent_execution_id, repo, prefix) do
          child_metric =
            child_metric_context(
              repo,
              prefix,
              child_context,
              child_execution_id,
              event_type,
              payload,
              opts
            )

          Events.append(
            child_context.parent_execution_id,
            event_type,
            Map.merge(payload, %{
              child_key: child_context.parent_child_key,
              child_execution_id: child_execution_id,
              child_first_execution_id: child_context.child_first_execution_id,
              child_unique_id: child_context.child_unique_id
            }),
            opts
          )

          if latest_wait_child_matches?(
               to_db_id(child_context.parent_execution_id),
               child_context.parent_child_key,
               repo,
               prefix
             ) do
            mark_continuable(child_context.parent_execution_id, opts)
          end

          child_metric
        end
    end
  end

  @spec apply_parent_close_policies_in_tx(module(), String.t(), binary(), keyword()) :: :ok
  defp apply_parent_close_policies_in_tx(repo, prefix, parent_execution_id, opts) do
    parent_execution_id
    |> list_open_child_executions_in_tx(repo, prefix)
    |> Enum.each(fn child ->
      if child.parent_close_policy == "request_cancel" do
        _ = request_cancel_in_tx(repo, prefix, child.execution_id, opts)
      end
    end)

    :ok
  end

  @spec lock_open_parent_execution(binary(), module(), String.t()) :: boolean()
  defp lock_open_parent_execution(parent_execution_id, repo, prefix) do
    sql = """
    SELECT 1
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    AND status IN (
      'pending'::#{prefix}.endurant_execution_status,
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'abandoned'::#{prefix}.endurant_execution_status,
      'cancelling'::#{prefix}.endurant_execution_status
    )
    FOR UPDATE
    LIMIT 1
    """

    case query!(repo, sql, [to_db_id(parent_execution_id)]).rows do
      [[1]] -> true
      _ -> false
    end
  end

  @spec child_parent_context(binary(), module(), String.t()) :: map() | nil
  defp child_parent_context(child_execution_id, repo, prefix) do
    sql = """
    SELECT unique_id, metadata
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [to_db_id(child_execution_id)]).rows do
      [[child_unique_id, metadata]] ->
        metadata = metadata || %{}

        with %{} = child_workflow <- metadata["child_workflow"] || metadata[:child_workflow],
             parent_execution_id when is_binary(parent_execution_id) <-
               child_workflow["parent_execution_id"] || child_workflow[:parent_execution_id],
             parent_child_key when is_binary(parent_child_key) <-
               child_workflow["parent_child_key"] || child_workflow[:parent_child_key] do
          %{
            child_unique_id: child_unique_id,
            child_first_execution_id:
              child_workflow["child_first_execution_id"] ||
                child_workflow[:child_first_execution_id] ||
                child_execution_id,
            parent_execution_id: parent_execution_id,
            parent_child_key: parent_child_key,
            parent_close_policy:
              child_workflow["parent_close_policy"] || child_workflow[:parent_close_policy]
          }
        else
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @spec list_open_child_executions_in_tx(binary(), module(), String.t()) :: [map()]
  defp list_open_child_executions_in_tx(parent_execution_id, repo, prefix) do
    sql = """
    SELECT
      id,
      status::text,
      metadata->'child_workflow'->>'parent_close_policy'
    FROM #{prefix}.endurant_executions
    WHERE metadata->'child_workflow'->>'parent_execution_id' = $1
    AND status IN (
      'pending'::#{prefix}.endurant_execution_status,
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'abandoned'::#{prefix}.endurant_execution_status,
      'cancelling'::#{prefix}.endurant_execution_status
    )
    FOR UPDATE
    """

    query!(repo, sql, [parent_execution_id]).rows
    |> Enum.map(fn [id, status, parent_close_policy] ->
      %{
        execution_id: to_app_id(id),
        status: parse_status(status),
        parent_close_policy: parent_close_policy
      }
    end)
  end

  @spec request_cancel_in_tx(module(), String.t(), binary(), keyword()) :: :ok
  defp request_cancel_in_tx(repo, prefix, execution_id, opts) do
    execution_id_db = to_db_id(execution_id)

    case lock_execution(repo, prefix, execution_id_db) do
      nil ->
        :ok

      execution when execution.status in [:completed, :failed, :cancelled] ->
        _ = execution
        :ok

      %{status: :cancelling} ->
        :ok

      %{status: :running} ->
        mark_cancelling_in_tx(repo, prefix, execution_id_db, opts)

      %{status: status} when status in [:pending, :waiting, :continuable, :abandoned] ->
        :ok = mark_cancelling_in_tx(repo, prefix, execution_id_db, opts)
        _ = finalize_cancel_in_tx(repo, prefix, execution_id_db, opts)
        :ok
    end
  end
end
