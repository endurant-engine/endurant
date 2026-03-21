defmodule Endurant.Schedules do
  @moduledoc false

  alias Endurant.Telemetry

  @default_prefix "public"

  @type overlap_policy :: :skip
  @type schedule_status ::
          :pending | :dispatched | :skipped | :failed | :cancelled

  @type schedule :: %{
          id: binary(),
          cron_schedule_id: binary() | nil,
          unique_id: String.t(),
          queue: String.t(),
          workflow: String.t(),
          version: String.t(),
          input: map(),
          scheduled_at: DateTime.t(),
          overlap_policy: overlap_policy(),
          status: schedule_status(),
          dispatched_execution_id: binary() | nil
        }

  @type insert_result :: {:ok, schedule()} | {:error, :id_conflict | :transient_db}

  @spec insert(module(), map(), DateTime.t(), keyword()) :: insert_result()
  def insert(workflow_module, input, scheduled_at, opts \\ [])
      when is_atom(workflow_module) and is_map(input) and is_struct(scheduled_at, DateTime) do
    do_insert(workflow_module, input, scheduled_at, opts)
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec get(binary(), keyword()) :: schedule() | nil
  def get(schedule_id, opts \\ []) when is_binary(schedule_id) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    schedule_id_db = to_db_id(schedule_id)

    sql = """
    SELECT
      id,
      cron_schedule_id,
      unique_id,
      queue,
      workflow_name,
      version,
      input,
      scheduled_at,
      overlap_policy::text,
      status::text,
      dispatched_execution_id
    FROM #{prefix}.endurant_scheduled_executions
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [schedule_id_db]).rows do
      [
        [
          id,
          cron_schedule_id,
          unique_id,
          queue,
          workflow_name,
          version,
          input,
          scheduled_at,
          overlap_policy,
          status,
          dispatched_execution_id
        ]
      ] ->
        %{
          id: to_app_id(id),
          cron_schedule_id: maybe_to_app_id(cron_schedule_id),
          unique_id: unique_id,
          queue: queue,
          workflow: workflow_name,
          version: version,
          input: input || %{},
          scheduled_at: to_datetime(scheduled_at),
          overlap_policy: parse_overlap_policy(overlap_policy),
          status: parse_schedule_status(status),
          dispatched_execution_id: maybe_to_app_id(dispatched_execution_id)
        }

      _ ->
        nil
    end
  end

  @spec list(keyword(), keyword()) :: [schedule()]
  def list(filters \\ [], opts \\ []) when is_list(filters) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    limit = positive_integer(Keyword.get(filters, :limit, 100), :limit)
    status_filter = Keyword.get(filters, :status)
    cron_schedule_id_filter = Keyword.get(filters, :cron_schedule_id)

    {clauses, params} =
      {[], [], 1}
      |> maybe_add_status_clause(status_filter, prefix)
      |> maybe_add_cron_schedule_id_clause(cron_schedule_id_filter)
      |> finalize_clauses()

    where_sql =
      case clauses do
        [] -> ""
        _ -> "WHERE " <> Enum.join(clauses, " AND ")
      end

    limit_placeholder = "$#{length(params) + 1}"

    sql = """
    SELECT
      s.id,
      s.cron_schedule_id,
      s.unique_id,
      s.queue,
      s.workflow_name,
      s.version,
      s.input,
      s.scheduled_at,
      s.overlap_policy::text,
      s.status::text,
      s.dispatched_execution_id
    FROM #{prefix}.endurant_scheduled_executions s
    #{where_sql}
    ORDER BY s.scheduled_at DESC, s.id DESC
    LIMIT #{limit_placeholder}
    """

    query!(repo, sql, params ++ [limit]).rows
    |> Enum.map(fn [
                     id,
                     cron_schedule_id,
                     unique_id,
                     queue,
                     workflow_name,
                     version,
                     input,
                     scheduled_at,
                     overlap_policy,
                     status,
                     dispatched_execution_id
                   ] ->
      %{
        id: to_app_id(id),
        cron_schedule_id: maybe_to_app_id(cron_schedule_id),
        unique_id: unique_id,
        queue: queue,
        workflow: workflow_name,
        version: version,
        input: input || %{},
        scheduled_at: to_datetime(scheduled_at),
        overlap_policy: parse_overlap_policy(overlap_policy),
        status: parse_schedule_status(status),
        dispatched_execution_id: maybe_to_app_id(dispatched_execution_id)
      }
    end)
  end

  @spec cancel(binary(), keyword()) :: :ok | {:error, :not_found | :not_pending}
  def cancel(schedule_id, opts \\ []) when is_binary(schedule_id) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    schedule_id_db = to_db_id(schedule_id)

    sql = """
    UPDATE #{prefix}.endurant_scheduled_executions
    SET
      status = 'cancelled'::#{prefix}.endurant_scheduled_execution_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'pending'::#{prefix}.endurant_scheduled_execution_status
    """

    case query!(repo, sql, [schedule_id_db]).num_rows do
      1 ->
        :ok

      _ ->
        case get(schedule_id, opts) do
          nil -> {:error, :not_found}
          _ -> {:error, :not_pending}
        end
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :not_pending}
  end

  @spec dispatch_due(pos_integer(), keyword()) :: non_neg_integer()
  def dispatch_due(limit, opts \\ [])
      when is_integer(limit) and limit > 0 and is_list(opts) do
    claim_due(limit, opts)
    |> Enum.reduce(0, fn schedule, count ->
      dispatch_one(schedule, opts)
      count + 1
    end)
  rescue
    DBConnection.ConnectionError ->
      0
  end

  @spec do_insert(module(), map(), DateTime.t(), keyword()) :: insert_result()
  defp do_insert(workflow_module, input, scheduled_at, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    workflow = workflow_module.__workflow__()
    schedule_id = Keyword.get(opts, :id, Ecto.UUID.generate())
    unique_id = resolve_unique_id(workflow, input)
    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = Map.get(workflow, :version, "1")
    scheduled_at_naive = DateTime.to_naive(scheduled_at)
    cron_schedule_id = maybe_to_db_id(Keyword.get(opts, :cron_schedule_id))

    sql = """
    INSERT INTO #{prefix}.endurant_scheduled_executions (
      id,
      cron_schedule_id,
      unique_id,
      queue,
      workflow_name,
      version,
      input,
      scheduled_at,
      overlap_policy,
      status,
      inserted_at,
      updated_at
    )
    VALUES (
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7,
      $8,
      'skip'::#{prefix}.endurant_schedule_overlap_policy,
      'pending'::#{prefix}.endurant_scheduled_execution_status,
      timezone('UTC', now()),
      timezone('UTC', now())
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id
    """

    case query!(repo, sql, [
           to_db_id(schedule_id),
           cron_schedule_id,
           unique_id,
           queue,
           workflow_name,
           version,
           input,
           scheduled_at_naive
         ]).rows do
      [[id]] ->
        schedule = %{
          id: to_app_id(id),
          cron_schedule_id: maybe_to_app_id(cron_schedule_id),
          unique_id: unique_id,
          queue: queue,
          workflow: workflow_name,
          version: version,
          input: input,
          scheduled_at: scheduled_at,
          overlap_policy: :skip,
          status: :pending,
          dispatched_execution_id: nil
        }

        Telemetry.emit([:schedule, :inserted], %{count: 1}, schedule_metadata(opts, schedule))
        {:ok, schedule}

      _ ->
        {:error, :id_conflict}
    end
  end

  @spec claim_due(pos_integer(), keyword()) :: [schedule()]
  defp claim_due(limit, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    SELECT
      s.id,
      s.cron_schedule_id,
      s.unique_id,
      s.queue,
      s.workflow_name,
      s.version,
      s.input,
      s.scheduled_at,
      s.overlap_policy::text,
      s.status::text,
      s.dispatched_execution_id
    FROM #{prefix}.endurant_scheduled_executions s
    WHERE s.status = 'pending'::#{prefix}.endurant_scheduled_execution_status
    AND s.scheduled_at <= timezone('UTC', now())
    ORDER BY s.scheduled_at ASC, s.id ASC
    LIMIT $1
    """

    query!(repo, sql, [limit]).rows
    |> Enum.map(fn [
                     id,
                     cron_schedule_id,
                     unique_id,
                     queue,
                     workflow_name,
                     version,
                     input,
                     scheduled_at,
                     overlap_policy,
                     status,
                     dispatched_execution_id
                   ] ->
      %{
        id: to_app_id(id),
        cron_schedule_id: maybe_to_app_id(cron_schedule_id),
        unique_id: unique_id,
        queue: queue,
        workflow: workflow_name,
        version: version,
        input: input || %{},
        scheduled_at: to_datetime(scheduled_at),
        overlap_policy: parse_overlap_policy(overlap_policy),
        status: parse_schedule_status(status),
        dispatched_execution_id: maybe_to_app_id(dispatched_execution_id)
      }
    end)
  end

  @spec dispatch_one(schedule(), keyword()) :: :ok
  defp dispatch_one(schedule, opts) do
    outcome =
      case find_open_execution_id(schedule.unique_id, opts) do
        nil ->
          dispatch_or_mark(schedule, opts)

        _execution_id ->
          mark_skipped(schedule.id, opts)
          :skipped
      end

    Telemetry.emit(
      [:schedule, :dispatch],
      %{count: 1, lag_ms: lag_ms(schedule.scheduled_at)},
      schedule_metadata(opts, schedule, %{outcome: outcome})
    )

    :ok
  rescue
    _error ->
      mark_failed(schedule.id, opts)

      Telemetry.emit(
        [:schedule, :dispatch],
        %{count: 1, lag_ms: lag_ms(schedule.scheduled_at)},
        schedule_metadata(opts, schedule, %{outcome: :failed})
      )

      :ok
  end

  @spec dispatch_or_mark(schedule(), keyword()) :: :dispatched | :skipped | :failed
  defp dispatch_or_mark(schedule, opts) do
    case dispatch_execution(schedule, opts) do
      {:ok, execution_id} ->
        mark_dispatched(schedule.id, execution_id, opts)
        :dispatched

      {:error, :unique_conflict} ->
        mark_skipped(schedule.id, opts)
        :skipped

      {:error, _reason} ->
        mark_failed(schedule.id, opts)
        :failed
    end
  end

  @spec dispatch_execution(schedule(), keyword()) :: {:ok, binary()} | {:error, term()}
  defp dispatch_execution(schedule, opts) do
    with {:ok, workflow_module} <- resolve_workflow_module(schedule.workflow),
         {:ok, execution} <- Endurant.Executions.insert(workflow_module, schedule.input, opts) do
      {:ok, execution.id}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec find_open_execution_id(String.t(), keyword()) :: binary() | nil
  defp find_open_execution_id(unique_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    SELECT e.id
    FROM #{prefix}.endurant_executions e
    WHERE e.unique_id = $1
    AND e.status IN (
      'pending'::#{prefix}.endurant_execution_status,
      'running'::#{prefix}.endurant_execution_status,
      'waiting'::#{prefix}.endurant_execution_status,
      'continuable'::#{prefix}.endurant_execution_status,
      'abandoned'::#{prefix}.endurant_execution_status,
      'cancelling'::#{prefix}.endurant_execution_status
    )
    ORDER BY e.inserted_at ASC, e.id ASC
    LIMIT 1
    """

    case query!(repo, sql, [unique_id]).rows do
      [[id]] -> to_app_id(id)
      _ -> nil
    end
  end

  @spec mark_dispatched(binary(), binary(), keyword()) :: :ok
  defp mark_dispatched(schedule_id, execution_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_scheduled_executions
    SET
      status = 'dispatched'::#{prefix}.endurant_scheduled_execution_status,
      dispatched_execution_id = $2,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'pending'::#{prefix}.endurant_scheduled_execution_status
    """

    _ = query!(repo, sql, [to_db_id(schedule_id), to_db_id(execution_id)])
    :ok
  end

  @spec mark_skipped(binary(), keyword()) :: :ok
  defp mark_skipped(schedule_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_scheduled_executions
    SET
      status = 'skipped'::#{prefix}.endurant_scheduled_execution_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'pending'::#{prefix}.endurant_scheduled_execution_status
    """

    _ = query!(repo, sql, [to_db_id(schedule_id)])
    :ok
  end

  @spec mark_failed(binary(), keyword()) :: :ok
  defp mark_failed(schedule_id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_scheduled_executions
    SET
      status = 'failed'::#{prefix}.endurant_scheduled_execution_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'pending'::#{prefix}.endurant_scheduled_execution_status
    """

    _ = query!(repo, sql, [to_db_id(schedule_id)])
    :ok
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

  @spec parse_overlap_policy(String.t()) :: overlap_policy()
  defp parse_overlap_policy("skip"), do: :skip

  defp parse_overlap_policy(other) do
    raise ArgumentError, "invalid overlap policy in db: #{inspect(other)}"
  end

  @spec parse_schedule_status(String.t()) :: schedule_status()
  defp parse_schedule_status("pending"), do: :pending
  defp parse_schedule_status("dispatched"), do: :dispatched
  defp parse_schedule_status("skipped"), do: :skipped
  defp parse_schedule_status("failed"), do: :failed
  defp parse_schedule_status("cancelled"), do: :cancelled

  @spec normalize_schedule_status_filter!(atom() | String.t()) :: String.t()
  defp normalize_schedule_status_filter!(status)
       when status in [:pending, :dispatched, :skipped, :failed, :cancelled] do
    Atom.to_string(status)
  end

  defp normalize_schedule_status_filter!(status) when is_binary(status) do
    normalized = String.trim(status)

    if normalized in ["pending", "dispatched", "skipped", "failed", "cancelled"] do
      normalized
    else
      raise ArgumentError, "invalid schedule status filter: #{inspect(status)}"
    end
  end

  defp normalize_schedule_status_filter!(status) do
    raise ArgumentError, "invalid schedule status filter: #{inspect(status)}"
  end

  @spec maybe_add_status_clause({[String.t()], list(), pos_integer()}, term(), String.t()) ::
          {[String.t()], list(), pos_integer()}
  defp maybe_add_status_clause({clauses, params, index}, nil, _prefix),
    do: {clauses, params, index}

  defp maybe_add_status_clause({clauses, params, index}, status_filter, prefix) do
    status = normalize_schedule_status_filter!(status_filter)
    clause = "s.status = $#{index}::#{prefix}.endurant_scheduled_execution_status"
    {[clause | clauses], params ++ [status], index + 1}
  end

  @spec maybe_add_cron_schedule_id_clause({[String.t()], list(), pos_integer()}, term()) ::
          {[String.t()], list(), pos_integer()}
  defp maybe_add_cron_schedule_id_clause({clauses, params, index}, nil),
    do: {clauses, params, index}

  defp maybe_add_cron_schedule_id_clause({clauses, params, index}, :manual) do
    {["s.cron_schedule_id IS NULL" | clauses], params, index}
  end

  defp maybe_add_cron_schedule_id_clause({clauses, params, index}, cron_schedule_id)
       when is_binary(cron_schedule_id) do
    clause = "s.cron_schedule_id = $#{index}"
    {[clause | clauses], params ++ [to_db_id(cron_schedule_id)], index + 1}
  end

  defp maybe_add_cron_schedule_id_clause({_clauses, _params, _index}, value) do
    raise ArgumentError, "invalid cron_schedule_id filter: #{inspect(value)}"
  end

  @spec finalize_clauses({[String.t()], list(), pos_integer()}) :: {[String.t()], list()}
  defp finalize_clauses({clauses, params, _index}) do
    {Enum.reverse(clauses), params}
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
      nil -> raise ArgumentError, "workflow must define queue"
      queue when is_atom(queue) -> Atom.to_string(queue)
      queue when is_binary(queue) -> queue
      other -> raise ArgumentError, "invalid queue: #{inspect(other)}"
    end
  end

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    Endurant.DB.query!(repo, sql, params)
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

  @spec maybe_to_app_id(binary() | nil) :: binary() | nil
  defp maybe_to_app_id(nil), do: nil
  defp maybe_to_app_id(id), do: to_app_id(id)

  @spec maybe_to_db_id(binary() | nil) :: binary() | nil
  defp maybe_to_db_id(nil), do: nil
  defp maybe_to_db_id(id), do: to_db_id(id)

  @spec to_datetime(DateTime.t() | NaiveDateTime.t() | nil) :: DateTime.t() | nil
  defp to_datetime(nil), do: nil
  defp to_datetime(%DateTime{} = dt), do: dt
  defp to_datetime(%NaiveDateTime{} = dt), do: DateTime.from_naive!(dt, "Etc/UTC")

  @spec schedule_metadata(keyword(), schedule(), map()) :: map()
  defp schedule_metadata(opts, schedule, extra \\ %{}) do
    Map.merge(
      %{
        instance: Keyword.get(opts, :instance),
        node: node(),
        queue: schedule.queue,
        workflow: schedule.workflow,
        version: schedule.version
      },
      extra
    )
  end

  @spec lag_ms(DateTime.t()) :: non_neg_integer()
  defp lag_ms(%DateTime{} = scheduled_at) do
    Telemetry.datetime_diff_ms(scheduled_at, DateTime.utc_now())
  end

  @spec positive_integer(term(), atom()) :: pos_integer()
  defp positive_integer(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, name) do
    raise ArgumentError, "#{inspect(name)} must be a positive integer, got: #{inspect(value)}"
  end
end
