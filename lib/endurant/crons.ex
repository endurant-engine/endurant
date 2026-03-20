defmodule Endurant.Crons do
  @moduledoc false

  alias Endurant.CronExpression
  alias Endurant.Telemetry

  @default_prefix "public"

  @type overlap_policy :: :skip
  @type cron_status :: :active | :paused
  @type fire_status :: :pending | :dispatched | :skipped | :failed | :cancelled

  @type cron_schedule :: %{
          id: binary(),
          name: String.t() | nil,
          unique_id: String.t(),
          queue: String.t(),
          workflow: String.t(),
          version: String.t(),
          input: map(),
          cron_expr: String.t(),
          timezone: String.t(),
          start_at: DateTime.t(),
          end_at: DateTime.t() | nil,
          next_run_at: DateTime.t(),
          overlap_policy: overlap_policy(),
          status: cron_status()
        }

  @type fire :: %{
          id: binary(),
          cron_schedule_id: binary(),
          scheduled_for: DateTime.t(),
          status: fire_status(),
          dispatched_execution_id: binary() | nil
        }

  @type insert_result ::
          {:ok, cron_schedule()}
          | {:error, :id_conflict | :name_conflict | :invalid_cron_expression | :invalid_timezone}
          | {:error, :invalid_window | :outside_window | :transient_db}

  @spec sync_from_config([map()], keyword()) :: :ok | {:error, term()}
  def sync_from_config(config_crons, opts \\ [])
      when is_list(config_crons) and is_list(opts) do
    repo = repo!(opts)

    case Endurant.DB.transaction(
           repo,
           fn ->
             Enum.each(config_crons, fn config_cron ->
               case upsert_config_cron(config_cron, opts) do
                 :ok -> :ok
                 {:error, reason} -> repo.rollback(reason)
               end
             end)

             :ok
           end,
           opts
         ) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec insert(module(), map(), String.t(), keyword()) :: insert_result()
  def insert(workflow_module, input, cron_expr, opts \\ [])
      when is_atom(workflow_module) and is_map(input) and is_binary(cron_expr) and is_list(opts) do
    do_insert(workflow_module, input, cron_expr, opts)
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec get(binary(), keyword()) :: cron_schedule() | nil
  def get(cron_id, opts \\ []) when is_binary(cron_id) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    SELECT
      id,
      name,
      unique_id,
      queue,
      workflow_name,
      version,
      input,
      cron_expr,
      timezone,
      start_at,
      end_at,
      next_run_at,
      overlap_policy::text,
      status::text
    FROM #{prefix}.endurant_cron_schedules
    WHERE id = $1
    LIMIT 1
    """

    case query!(repo, sql, [to_db_id(cron_id)]).rows do
      [row] -> row_to_cron(row)
      _ -> nil
    end
  end

  @spec list(keyword(), keyword()) :: [cron_schedule()]
  def list(filters \\ [], opts \\ []) when is_list(filters) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    limit = positive_integer(Keyword.get(filters, :limit, 100), :limit)
    status_filter = Keyword.get(filters, :status)

    {where_sql, params} =
      case status_filter do
        nil ->
          {"", []}

        value ->
          status = normalize_cron_status_filter!(value)
          {"WHERE c.status = $1::#{prefix}.endurant_cron_schedule_status", [status]}
      end

    limit_placeholder = "$#{length(params) + 1}"

    sql = """
    SELECT
      c.id,
      c.name,
      c.unique_id,
      c.queue,
      c.workflow_name,
      c.version,
      c.input,
      c.cron_expr,
      c.timezone,
      c.start_at,
      c.end_at,
      c.next_run_at,
      c.overlap_policy::text,
      c.status::text
    FROM #{prefix}.endurant_cron_schedules c
    #{where_sql}
    ORDER BY c.next_run_at DESC, c.id DESC
    LIMIT #{limit_placeholder}
    """

    query!(repo, sql, params ++ [limit]).rows
    |> Enum.map(&row_to_cron/1)
  end

  @spec list_fires(binary(), keyword(), keyword()) :: [fire()]
  def list_fires(cron_id, filters \\ [], opts \\ [])
      when is_binary(cron_id) and is_list(filters) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    limit = positive_integer(Keyword.get(filters, :limit, 100), :limit)

    sql = """
    SELECT
      s.id,
      s.cron_schedule_id,
      s.scheduled_at,
      s.status::text,
      s.dispatched_execution_id
    FROM #{prefix}.endurant_scheduled_executions s
    WHERE s.cron_schedule_id = $1
    ORDER BY s.scheduled_at DESC, s.id DESC
    LIMIT $2
    """

    query!(repo, sql, [to_db_id(cron_id), limit]).rows
    |> Enum.map(fn [id, cron_schedule_id, scheduled_for, status, dispatched_execution_id] ->
      %{
        id: to_app_id(id),
        cron_schedule_id: to_app_id(cron_schedule_id),
        scheduled_for: to_datetime(scheduled_for),
        status: parse_fire_status(status),
        dispatched_execution_id: maybe_to_app_id(dispatched_execution_id)
      }
    end)
  end

  @spec pause(binary(), keyword()) :: :ok | {:error, :not_found | :not_active | :transient_db}
  def pause(cron_id, opts \\ []) when is_binary(cron_id) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_cron_schedules
    SET
      status = 'paused'::#{prefix}.endurant_cron_schedule_status,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'active'::#{prefix}.endurant_cron_schedule_status
    """

    case query!(repo, sql, [to_db_id(cron_id)]).num_rows do
      1 ->
        :ok

      _ ->
        case get(cron_id, opts) do
          nil -> {:error, :not_found}
          _ -> {:error, :not_active}
        end
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec resume(binary(), keyword()) ::
          :ok | {:error, :not_found | :not_paused | :ended | :transient_db}
  def resume(cron_id, opts \\ []) when is_binary(cron_id) and is_list(opts) do
    case get(cron_id, opts) do
      nil ->
        {:error, :not_found}

      %{status: :paused} = schedule ->
        do_resume(schedule, opts)

      _ ->
        {:error, :not_paused}
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec delete(binary(), keyword()) ::
          :ok | {:error, :not_found | :transient_db}
  def delete(cron_id, opts \\ []) when is_binary(cron_id) and is_list(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    DELETE FROM #{prefix}.endurant_cron_schedules
    WHERE id = $1
    """

    case query!(repo, sql, [to_db_id(cron_id)]).num_rows do
      1 ->
        :ok

      _ ->
        {:error, :not_found}
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec dispatch_due(pos_integer(), keyword()) :: non_neg_integer()
  def dispatch_due(limit, opts \\ [])
      when is_integer(limit) and limit > 0 and is_list(opts) do
    1..limit
    |> Enum.reduce_while(0, fn _, dispatched ->
      case dispatch_one_due(opts) do
        :ok -> {:cont, dispatched + 1}
        :none -> {:halt, dispatched}
      end
    end)
  rescue
    DBConnection.ConnectionError ->
      0
  end

  @spec do_insert(module(), map(), String.t(), keyword()) :: insert_result()
  defp do_insert(workflow_module, input, cron_expr, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    workflow = workflow_module.__workflow__()
    cron_id = Keyword.get(opts, :id, Ecto.UUID.generate())
    name = Keyword.get(opts, :name)
    unique_id = resolve_unique_id(workflow, input)
    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = Map.get(workflow, :version, "1")
    timezone = normalize_timezone(Keyword.get(opts, :timezone, "Etc/UTC"))
    start_at = normalize_start_at(Keyword.get(opts, :start_at, DateTime.utc_now()))
    end_at = normalize_end_at(Keyword.get(opts, :end_at))

    if not valid_window?(start_at, end_at) do
      {:error, :invalid_window}
    else
      with {:ok, parsed_expr} <- CronExpression.parse(cron_expr),
           :ok <- validate_timezone(timezone),
           {:ok, next_run_at} <- first_run_at(parsed_expr, start_at, timezone),
           :ok <- validate_end_window(next_run_at, end_at) do
        sql = """
        INSERT INTO #{prefix}.endurant_cron_schedules (
          id,
          name,
          unique_id,
          queue,
          workflow_name,
          version,
          input,
          cron_expr,
          timezone,
          start_at,
          end_at,
          next_run_at,
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
          $9,
          $10,
          $11,
          $12,
          'skip'::#{prefix}.endurant_schedule_overlap_policy,
          'active'::#{prefix}.endurant_cron_schedule_status,
          timezone('UTC', now()),
          timezone('UTC', now())
        )
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """

        case query!(repo, sql, [
               to_db_id(cron_id),
               name,
               unique_id,
               queue,
               workflow_name,
               version,
               input,
               cron_expr,
               timezone,
               DateTime.to_naive(start_at),
               maybe_to_naive(end_at),
               DateTime.to_naive(next_run_at)
             ]).rows do
          [[id]] ->
            schedule = %{
              id: to_app_id(id),
              name: name,
              unique_id: unique_id,
              queue: queue,
              workflow: workflow_name,
              version: version,
              input: input,
              cron_expr: cron_expr,
              timezone: timezone,
              start_at: start_at,
              end_at: end_at,
              next_run_at: next_run_at,
              overlap_policy: :skip,
              status: :active
            }

            Telemetry.emit([:cron, :inserted], %{count: 1}, cron_metadata(opts, schedule))
            {:ok, schedule}

          _ ->
            if name_conflict?(name, opts),
              do: {:error, :name_conflict},
              else: {:error, :id_conflict}
        end
      else
        {:error, :invalid_cron_expression} -> {:error, :invalid_cron_expression}
        {:error, :invalid_timezone} -> {:error, :invalid_timezone}
        {:error, :outside_window} -> {:error, :outside_window}
        {:error, _reason} -> {:error, :outside_window}
      end
    end
  end

  @spec upsert_config_cron(map(), keyword()) :: :ok | {:error, term()}
  defp upsert_config_cron(config_cron, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    workflow_module = Map.fetch!(config_cron, :workflow)
    workflow = workflow_module.__workflow__()
    name = Map.fetch!(config_cron, :name)
    cron_expr = Map.fetch!(config_cron, :expr)
    input = Map.fetch!(config_cron, :input)
    timezone = Map.fetch!(config_cron, :timezone)
    start_at = Map.fetch!(config_cron, :start_at)
    end_at = Map.fetch!(config_cron, :end_at)
    status = Map.fetch!(config_cron, :status)
    unique_id = resolve_unique_id(workflow, input)
    queue = resolve_queue(workflow)
    workflow_name = inspect(workflow_module)
    version = Map.get(workflow, :version, "1")

    with {:ok, parsed_expr} <- CronExpression.parse(cron_expr),
         :ok <- validate_timezone(timezone),
         {:ok, next_run_at} <- first_run_at(parsed_expr, start_at, timezone),
         :ok <- validate_end_window(next_run_at, end_at) do
      sql = """
      INSERT INTO #{prefix}.endurant_cron_schedules (
        id,
        name,
        unique_id,
        queue,
        workflow_name,
        version,
        input,
        cron_expr,
        timezone,
        start_at,
        end_at,
        next_run_at,
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
        $9,
        $10,
        $11,
        $12,
        'skip'::#{prefix}.endurant_schedule_overlap_policy,
        $13::#{prefix}.endurant_cron_schedule_status,
        timezone('UTC', now()),
        timezone('UTC', now())
      )
      ON CONFLICT (name) WHERE name IS NOT NULL
      DO UPDATE SET
        unique_id = EXCLUDED.unique_id,
        queue = EXCLUDED.queue,
        workflow_name = EXCLUDED.workflow_name,
        version = EXCLUDED.version,
        input = EXCLUDED.input,
        cron_expr = EXCLUDED.cron_expr,
        timezone = EXCLUDED.timezone,
        start_at = EXCLUDED.start_at,
        end_at = EXCLUDED.end_at,
        next_run_at = EXCLUDED.next_run_at,
        overlap_policy = EXCLUDED.overlap_policy,
        status = EXCLUDED.status,
        updated_at = timezone('UTC', now())
      """

      _ =
        query!(repo, sql, [
          to_db_id(Ecto.UUID.generate()),
          name,
          unique_id,
          queue,
          workflow_name,
          version,
          input,
          cron_expr,
          timezone,
          DateTime.to_naive(start_at),
          maybe_to_naive(end_at),
          DateTime.to_naive(next_run_at),
          Atom.to_string(status)
        ])

      :ok
    else
      {:error, reason} ->
        {:error, {:invalid_config_cron, name, reason}}
    end
  end

  @spec do_resume(cron_schedule(), keyword()) ::
          :ok | {:error, :ended | :transient_db}
  defp do_resume(schedule, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    with {:ok, parsed_expr} <- CronExpression.parse(schedule.cron_expr),
         :ok <- validate_timezone(schedule.timezone),
         {:ok, next_run_at} <-
           first_run_at(
             parsed_expr,
             max_datetime(schedule.start_at, DateTime.utc_now()),
             schedule.timezone
           ) do
      if within_end_window?(next_run_at, schedule.end_at) do
        sql = """
        UPDATE #{prefix}.endurant_cron_schedules
        SET
          status = 'active'::#{prefix}.endurant_cron_schedule_status,
          next_run_at = $2,
          updated_at = timezone('UTC', now())
        WHERE id = $1
        AND status = 'paused'::#{prefix}.endurant_cron_schedule_status
        """

        _ = query!(repo, sql, [to_db_id(schedule.id), DateTime.to_naive(next_run_at)])
        :ok
      else
        {:error, :ended}
      end
    else
      {:error, :invalid_cron_expression} -> {:error, :ended}
      {:error, :invalid_timezone} -> {:error, :ended}
      {:error, _reason} -> {:error, :ended}
    end
  rescue
    DBConnection.ConnectionError ->
      {:error, :transient_db}
  end

  @spec dispatch_one_due(keyword()) :: :ok | :none
  defp dispatch_one_due(opts) do
    repo = repo!(opts)

    case Endurant.DB.transaction(repo, fn -> dispatch_one_due_tx(opts) end, opts) do
      {:ok, :none} ->
        :none

      {:ok, {:ok, schedule, outcome}} ->
        Telemetry.emit(
          [:cron, :dispatch],
          %{count: 1, lag_ms: lag_ms(schedule.next_run_at)},
          cron_metadata(opts, schedule, %{outcome: outcome})
        )

        :ok

      _ ->
        :none
    end
  end

  @spec dispatch_one_due_tx(keyword()) :: {:ok, cron_schedule(), :dispatched | :skipped} | :none
  defp dispatch_one_due_tx(opts) do
    case lock_next_due(opts) do
      nil ->
        :none

      schedule ->
        scheduled_for = schedule.next_run_at
        outcome = insert_scheduled_fire(schedule, scheduled_for, opts)
        :ok = advance_schedule_after_fire(schedule, scheduled_for, opts)
        {:ok, schedule, outcome}
    end
  end

  @spec lock_next_due(keyword()) :: cron_schedule() | nil
  defp lock_next_due(opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    SELECT
      c.id,
      c.name,
      c.unique_id,
      c.queue,
      c.workflow_name,
      c.version,
      c.input,
      c.cron_expr,
      c.timezone,
      c.start_at,
      c.end_at,
      c.next_run_at,
      c.overlap_policy::text,
      c.status::text
    FROM #{prefix}.endurant_cron_schedules c
    WHERE c.status = 'active'::#{prefix}.endurant_cron_schedule_status
    AND c.next_run_at <= timezone('UTC', now())
    AND (c.end_at IS NULL OR c.next_run_at <= c.end_at)
    ORDER BY c.next_run_at ASC, c.id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
    """

    case query!(repo, sql, []).rows do
      [row] -> row_to_cron(row)
      _ -> nil
    end
  end

  @spec insert_scheduled_fire(cron_schedule(), DateTime.t(), keyword()) :: :dispatched | :skipped
  defp insert_scheduled_fire(schedule, scheduled_for, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

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
    ON CONFLICT (cron_schedule_id, scheduled_at) DO NOTHING
    """

    case query!(repo, sql, [
           to_db_id(Ecto.UUID.generate()),
           to_db_id(schedule.id),
           schedule.unique_id,
           schedule.queue,
           schedule.workflow,
           schedule.version,
           schedule.input,
           DateTime.to_naive(scheduled_for)
         ]).num_rows do
      1 -> :dispatched
      _ -> :skipped
    end
  end

  @spec advance_schedule_after_fire(cron_schedule(), DateTime.t(), keyword()) :: :ok
  defp advance_schedule_after_fire(schedule, scheduled_for, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    next_run_at =
      case next_run(schedule, scheduled_for) do
        {:ok, next_run_at} ->
          next_run_at

        {:error, _reason} ->
          DateTime.add(scheduled_for, 1, :second)
      end

    sql = """
    UPDATE #{prefix}.endurant_cron_schedules
    SET
      next_run_at = $2,
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND status = 'active'::#{prefix}.endurant_cron_schedule_status
    """

    _ =
      query!(repo, sql, [
        to_db_id(schedule.id),
        DateTime.to_naive(next_run_at)
      ])

    :ok
  end

  @spec next_run(cron_schedule(), DateTime.t()) :: {:ok, DateTime.t()} | {:error, term()}
  defp next_run(schedule, scheduled_for) do
    with {:ok, parsed_expr} <- CronExpression.parse(schedule.cron_expr),
         {:ok, next_run_at} <-
           CronExpression.next_after(parsed_expr, scheduled_for, schedule.timezone) do
      {:ok, next_run_at}
    end
  end

  @spec row_to_cron(list()) :: cron_schedule()
  defp row_to_cron([
         id,
         name,
         unique_id,
         queue,
         workflow_name,
         version,
         input,
         cron_expr,
         timezone,
         start_at,
         end_at,
         next_run_at,
         overlap_policy,
         status
       ]) do
    %{
      id: to_app_id(id),
      name: name,
      unique_id: unique_id,
      queue: queue,
      workflow: workflow_name,
      version: version,
      input: input || %{},
      cron_expr: cron_expr,
      timezone: timezone,
      start_at: to_datetime(start_at),
      end_at: to_datetime(end_at),
      next_run_at: to_datetime(next_run_at),
      overlap_policy: parse_overlap_policy(overlap_policy),
      status: parse_cron_status(status)
    }
  end

  @spec name_conflict?(String.t() | nil, keyword()) :: boolean()
  defp name_conflict?(nil, _opts), do: false

  defp name_conflict?(name, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    sql = "SELECT id FROM #{prefix}.endurant_cron_schedules WHERE name = $1 LIMIT 1"
    query!(repo, sql, [name]).num_rows > 0
  end

  @spec valid_window?(DateTime.t(), DateTime.t() | nil) :: boolean()
  defp valid_window?(_start_at, nil), do: true
  defp valid_window?(start_at, end_at), do: DateTime.compare(end_at, start_at) != :lt

  @spec validate_end_window(DateTime.t(), DateTime.t() | nil) :: :ok | {:error, :outside_window}
  defp validate_end_window(next_run_at, end_at) do
    if within_end_window?(next_run_at, end_at), do: :ok, else: {:error, :outside_window}
  end

  @spec within_end_window?(DateTime.t(), DateTime.t() | nil) :: boolean()
  defp within_end_window?(_next_run_at, nil), do: true
  defp within_end_window?(next_run_at, end_at), do: DateTime.compare(next_run_at, end_at) != :gt

  @spec first_run_at(CronExpression.t(), DateTime.t(), String.t()) ::
          {:ok, DateTime.t()} | {:error, term()}
  defp first_run_at(parsed_expr, start_at, timezone) do
    base =
      start_at
      |> DateTime.truncate(:second)
      |> DateTime.add(-1, :second)

    CronExpression.next_after(parsed_expr, base, timezone)
  end

  @spec validate_timezone(String.t()) :: :ok | {:error, :invalid_timezone}
  defp validate_timezone(timezone) do
    case DateTime.now(timezone) do
      {:ok, _} -> :ok
      _ -> {:error, :invalid_timezone}
    end
  end

  @spec normalize_timezone(term()) :: String.t()
  defp normalize_timezone(timezone) when is_binary(timezone), do: timezone
  defp normalize_timezone(_timezone), do: "Etc/UTC"

  @spec normalize_start_at(term()) :: DateTime.t()
  defp normalize_start_at(%DateTime{} = start_at), do: start_at
  defp normalize_start_at(_start_at), do: DateTime.utc_now()

  @spec normalize_end_at(term()) :: DateTime.t() | nil
  defp normalize_end_at(nil), do: nil
  defp normalize_end_at(%DateTime{} = end_at), do: end_at
  defp normalize_end_at(_end_at), do: nil

  @spec max_datetime(DateTime.t(), DateTime.t()) :: DateTime.t()
  defp max_datetime(a, b) do
    case DateTime.compare(a, b) do
      :lt -> b
      _ -> a
    end
  end

  @spec parse_overlap_policy(String.t()) :: overlap_policy()
  defp parse_overlap_policy("skip"), do: :skip

  defp parse_overlap_policy(other) do
    raise ArgumentError, "invalid overlap policy in db: #{inspect(other)}"
  end

  @spec parse_cron_status(String.t()) :: cron_status()
  defp parse_cron_status("active"), do: :active
  defp parse_cron_status("paused"), do: :paused

  @spec parse_fire_status(String.t()) :: fire_status()
  defp parse_fire_status("pending"), do: :pending
  defp parse_fire_status("dispatched"), do: :dispatched
  defp parse_fire_status("skipped"), do: :skipped
  defp parse_fire_status("failed"), do: :failed
  defp parse_fire_status("cancelled"), do: :cancelled

  @spec normalize_cron_status_filter!(atom() | String.t()) :: String.t()
  defp normalize_cron_status_filter!(status) when status in [:active, :paused] do
    Atom.to_string(status)
  end

  defp normalize_cron_status_filter!(status) when is_binary(status) do
    normalized = String.trim(status)

    if normalized in ["active", "paused"] do
      normalized
    else
      raise ArgumentError, "invalid cron status filter: #{inspect(status)}"
    end
  end

  defp normalize_cron_status_filter!(status) do
    raise ArgumentError, "invalid cron status filter: #{inspect(status)}"
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

  @spec maybe_to_naive(DateTime.t() | nil) :: NaiveDateTime.t() | nil
  defp maybe_to_naive(nil), do: nil
  defp maybe_to_naive(%DateTime{} = dt), do: DateTime.to_naive(dt)

  @spec to_datetime(DateTime.t() | NaiveDateTime.t() | nil) :: DateTime.t() | nil
  defp to_datetime(nil), do: nil
  defp to_datetime(%DateTime{} = dt), do: dt
  defp to_datetime(%NaiveDateTime{} = dt), do: DateTime.from_naive!(dt, "Etc/UTC")

  @spec cron_metadata(keyword(), cron_schedule(), map()) :: map()
  defp cron_metadata(opts, schedule, extra \\ %{}) do
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
