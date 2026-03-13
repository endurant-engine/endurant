defmodule Endurant.Archivers.ClickHouse do
  @moduledoc false

  @behaviour Endurant.Archiver

  @default_database "endurant"
  @default_executions_table "endurant_archived_executions"
  @default_events_table "endurant_archived_events"
  @default_execution_facts_table "endurant_execution_facts"
  @default_task_runs_table "endurant_task_runs"

  @type http_result :: {:ok, pos_integer(), binary()} | {:error, term()}

  @impl true
  def init(endurant_migration_version, opts) when is_integer(endurant_migration_version) do
    database = database(opts)
    executions_table = executions_table(opts)
    events_table = events_table(opts)
    execution_facts_table = execution_facts_table(opts)
    task_runs_table = task_runs_table(opts)

    with :ok <- execute(opts, "CREATE DATABASE IF NOT EXISTS #{identifier(database)}"),
         :ok <-
           execute(
             opts,
             executions_table_sql(database, executions_table, endurant_migration_version)
           ),
         :ok <-
           execute(opts, events_table_sql(database, events_table, endurant_migration_version)),
         :ok <-
           execute(
             opts,
             alter_events_table_sql(database, events_table, endurant_migration_version)
           ),
         :ok <-
           execute(
             opts,
             execution_facts_table_sql(
               database,
               execution_facts_table,
               endurant_migration_version
             )
           ),
         :ok <-
           execute(
             opts,
             task_runs_table_sql(database, task_runs_table, endurant_migration_version)
           ) do
      :ok
    end
  end

  @impl true
  def archive(batch, endurant_migration_version, opts)
      when is_list(batch) and is_integer(endurant_migration_version) do
    database = database(opts)
    executions_table = executions_table(opts)
    events_table = events_table(opts)
    execution_facts_table = execution_facts_table(opts)
    task_runs_table = task_runs_table(opts)
    archived_at = DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

    execution_rows =
      batch
      |> Enum.map(fn %{execution: execution} ->
        execution_row(execution, endurant_migration_version, archived_at)
      end)

    event_rows =
      batch
      |> Enum.flat_map(fn %{execution: execution, events: events} ->
        Enum.map(events, &event_row(&1, execution, endurant_migration_version, archived_at))
      end)

    execution_fact_rows =
      batch
      |> Enum.map(fn %{execution: execution, events: events} ->
        execution_fact_row(execution, events, endurant_migration_version, archived_at)
      end)

    task_run_rows =
      batch
      |> Enum.flat_map(fn %{execution: execution, events: events} ->
        task_run_rows(execution, events, endurant_migration_version, archived_at)
      end)

    with :ok <-
           maybe_insert_rows(opts, database, executions_table, execution_rows),
         :ok <-
           maybe_insert_rows(opts, database, events_table, event_rows),
         :ok <-
           maybe_insert_rows(opts, database, execution_facts_table, execution_fact_rows),
         :ok <-
           maybe_insert_rows(opts, database, task_runs_table, task_run_rows) do
      :ok
    end
  end

  @spec maybe_insert_rows(keyword(), String.t(), String.t(), [map()]) :: :ok | {:error, term()}
  defp maybe_insert_rows(_opts, _database, _table, []), do: :ok

  defp maybe_insert_rows(opts, database, table, rows) when is_list(rows) do
    execute(opts, insert_sql(database, table, encode_json_each_row(rows)))
  end

  @spec execution_row(map(), non_neg_integer(), String.t()) :: map()
  defp execution_row(execution, endurant_migration_version, archived_at) do
    %{
      "execution_id" => fetch(execution, :id) || "",
      "unique_id" => fetch(execution, :unique_id) || "",
      "queue" => fetch(execution, :queue) || "",
      "workflow_name" => workflow_name(execution),
      "workflow_version" => stringify(fetch(execution, :version) || ""),
      "status" => stringify(fetch(execution, :status) || ""),
      "next_event_sequence" => integer(fetch(execution, :next_event_sequence)),
      "history_size_bytes" => integer(fetch(execution, :history_size_bytes)),
      "completed_at" => iso8601(fetch(execution, :completed_at)),
      "inserted_at" => iso8601(fetch(execution, :inserted_at)),
      "updated_at" => iso8601(fetch(execution, :updated_at)),
      "endurant_migration_version" => endurant_migration_version,
      "archived_at" => archived_at,
      "raw_json" => raw_json(execution)
    }
  end

  @spec event_row(map(), map(), non_neg_integer(), String.t()) :: map()
  defp event_row(event, execution, endurant_migration_version, archived_at) do
    payload = fetch(event, :payload) || %{}

    %{
      "execution_id" => fetch(event, :execution_id) || fetch(execution, :id) || "",
      "sequence" => integer(fetch(event, :sequence)),
      "event_type" => stringify(fetch(event, :type) || ""),
      "task" => nullable_string(fetch(payload, :task)),
      "task_run_id" => nullable_string(fetch(payload, :task_run_id)),
      "inserted_at" => iso8601(fetch(event, :inserted_at)),
      "endurant_migration_version" => endurant_migration_version,
      "archived_at" => archived_at,
      "raw_json" => raw_json(event)
    }
  end

  @spec execution_fact_row(map(), [map()], non_neg_integer(), String.t()) :: map()
  defp execution_fact_row(execution, events, endurant_migration_version, archived_at) do
    started_at =
      events
      |> Enum.find(fn event ->
        fetch(event, :type) in [:execution_started, "execution_started"]
      end)
      |> case do
        nil -> nil
        event -> fetch(event, :inserted_at)
      end

    completed_at = fetch(execution, :completed_at)
    duration_ms = duration_ms(started_at, completed_at)

    %{
      "execution_id" => fetch(execution, :id) || "",
      "unique_id" => fetch(execution, :unique_id) || "",
      "queue" => fetch(execution, :queue) || "",
      "workflow_name" => workflow_name(execution),
      "workflow_version" => stringify(fetch(execution, :version) || ""),
      "status" => stringify(fetch(execution, :status) || ""),
      "inserted_at" => iso8601(fetch(execution, :inserted_at)),
      "started_at" => iso8601(started_at),
      "completed_at" => iso8601(completed_at),
      "duration_ms" => nullable_integer(duration_ms),
      "event_count" => length(events),
      "history_size_bytes" => integer(fetch(execution, :history_size_bytes)),
      "abandoned_count" => count_events(events, :execution_abandoned),
      "resumed_count" => count_events(events, :execution_resumed),
      "task_started_count" => count_events(events, :task_started),
      "task_failed_count" => count_events(events, :task_failed),
      "task_interrupted_count" => count_events(events, :task_interrupted),
      "endurant_migration_version" => endurant_migration_version,
      "archived_at" => archived_at
    }
  end

  @spec task_run_rows(map(), [map()], non_neg_integer(), String.t()) :: [map()]
  defp task_run_rows(execution, events, endurant_migration_version, archived_at) do
    workflow_name = workflow_name(execution)
    workflow_version = stringify(fetch(execution, :version) || "")
    execution_id = fetch(execution, :id) || ""

    {rows, _open_runs, _attempts} =
      Enum.reduce(events, {[], %{}, %{}}, fn event, {rows, open_runs, attempts} ->
        payload = fetch(event, :payload) || %{}
        task = fetch(payload, :task)
        task_run_id = fetch(payload, :task_run_id)
        type = fetch(event, :type)

        case type do
          started when started in [:task_started, "task_started"] ->
            if is_binary(task) and task != "" and is_binary(task_run_id) and task_run_id != "" do
              attempt = Map.get(attempts, task, 0) + 1

              open_run = %{
                task: task,
                task_run_id: task_run_id,
                attempt: attempt,
                started_at: fetch(event, :inserted_at)
              }

              {rows, Map.put(open_runs, task_run_id, open_run), Map.put(attempts, task, attempt)}
            else
              {rows, open_runs, attempts}
            end

          terminal
          when terminal in [
                 :task_completed,
                 "task_completed",
                 :task_failed,
                 "task_failed",
                 :task_interrupted,
                 "task_interrupted"
               ] ->
            case Map.pop(open_runs, task_run_id) do
              {nil, remaining} ->
                {rows, remaining, attempts}

              {%{} = open_run, remaining} ->
                row = %{
                  "execution_id" => execution_id,
                  "workflow_name" => workflow_name,
                  "workflow_version" => workflow_version,
                  "task" => open_run.task,
                  "task_run_id" => open_run.task_run_id,
                  "attempt" => open_run.attempt,
                  "status" => task_run_status(terminal),
                  "started_at" => iso8601(open_run.started_at),
                  "finished_at" => iso8601(fetch(event, :inserted_at)),
                  "duration_ms" =>
                    nullable_integer(duration_ms(open_run.started_at, fetch(event, :inserted_at))),
                  "endurant_migration_version" => endurant_migration_version,
                  "archived_at" => archived_at
                }

                {[row | rows], remaining, attempts}
            end

          _ ->
            {rows, open_runs, attempts}
        end
      end)

    Enum.reverse(rows)
  end

  @spec workflow_name(map()) :: String.t()
  defp workflow_name(execution) do
    case fetch(execution, :workflow_name) || fetch(execution, :workflow) do
      nil -> ""
      value -> stringify(value)
    end
  end

  @spec raw_json(term()) :: binary()
  defp raw_json(value) do
    value
    |> normalize_json()
    |> Jason.encode!()
  end

  @spec encode_json_each_row([map()] | map()) :: binary()
  defp encode_json_each_row(rows) when is_map(rows), do: encode_json_each_row([rows])

  defp encode_json_each_row(rows) when is_list(rows) do
    rows
    |> Enum.map_join("\n", &Jason.encode!(&1))
    |> case do
      "" -> ""
      body -> body <> "\n"
    end
  end

  @spec insert_sql(String.t(), String.t(), binary()) :: binary()
  defp insert_sql(database, table, json_each_row_body) do
    "INSERT INTO #{identifier(database)}.#{identifier(table)} FORMAT JSONEachRow\n" <>
      json_each_row_body
  end

  @spec executions_table_sql(String.t(), String.t(), non_neg_integer()) :: binary()
  defp executions_table_sql(database, table, _version) do
    """
    CREATE TABLE IF NOT EXISTS #{identifier(database)}.#{identifier(table)} (
      execution_id String,
      unique_id String,
      queue String,
      workflow_name String,
      workflow_version String,
      status String,
      next_event_sequence UInt64,
      history_size_bytes UInt64,
      completed_at Nullable(DateTime64(6, 'UTC')),
      inserted_at Nullable(DateTime64(6, 'UTC')),
      updated_at Nullable(DateTime64(6, 'UTC')),
      endurant_migration_version UInt64,
      archived_at DateTime64(6, 'UTC'),
      raw_json String
    )
    ENGINE = ReplacingMergeTree(archived_at)
    ORDER BY execution_id
    """
    |> String.trim()
  end

  @spec events_table_sql(String.t(), String.t(), non_neg_integer()) :: binary()
  defp events_table_sql(database, table, _version) do
    """
    CREATE TABLE IF NOT EXISTS #{identifier(database)}.#{identifier(table)} (
      execution_id String,
      sequence UInt64,
      event_type String,
      task Nullable(String),
      task_run_id Nullable(String),
      inserted_at Nullable(DateTime64(6, 'UTC')),
      endurant_migration_version UInt64,
      archived_at DateTime64(6, 'UTC'),
      raw_json String
    )
    ENGINE = ReplacingMergeTree(archived_at)
    ORDER BY (execution_id, sequence)
    """
    |> String.trim()
  end

  @spec alter_events_table_sql(String.t(), String.t(), non_neg_integer()) :: binary()
  defp alter_events_table_sql(database, table, _version) do
    """
    ALTER TABLE #{identifier(database)}.#{identifier(table)}
    ADD COLUMN IF NOT EXISTS task_run_id Nullable(String)
    AFTER task
    """
    |> String.trim()
  end

  @spec execution_facts_table_sql(String.t(), String.t(), non_neg_integer()) :: binary()
  defp execution_facts_table_sql(database, table, _version) do
    """
    CREATE TABLE IF NOT EXISTS #{identifier(database)}.#{identifier(table)} (
      execution_id String,
      unique_id String,
      queue String,
      workflow_name String,
      workflow_version String,
      status String,
      inserted_at Nullable(DateTime64(6, 'UTC')),
      started_at Nullable(DateTime64(6, 'UTC')),
      completed_at Nullable(DateTime64(6, 'UTC')),
      duration_ms Nullable(Int64),
      event_count UInt64,
      history_size_bytes UInt64,
      abandoned_count UInt64,
      resumed_count UInt64,
      task_started_count UInt64,
      task_failed_count UInt64,
      task_interrupted_count UInt64,
      endurant_migration_version UInt64,
      archived_at DateTime64(6, 'UTC')
    )
    ENGINE = ReplacingMergeTree(archived_at)
    ORDER BY (workflow_name, execution_id)
    """
    |> String.trim()
  end

  @spec task_runs_table_sql(String.t(), String.t(), non_neg_integer()) :: binary()
  defp task_runs_table_sql(database, table, _version) do
    """
    CREATE TABLE IF NOT EXISTS #{identifier(database)}.#{identifier(table)} (
      execution_id String,
      workflow_name String,
      workflow_version String,
      task String,
      task_run_id String,
      attempt UInt64,
      status String,
      started_at Nullable(DateTime64(6, 'UTC')),
      finished_at Nullable(DateTime64(6, 'UTC')),
      duration_ms Nullable(Int64),
      endurant_migration_version UInt64,
      archived_at DateTime64(6, 'UTC')
    )
    ENGINE = ReplacingMergeTree(archived_at)
    ORDER BY (workflow_name, task, execution_id, attempt, task_run_id)
    """
    |> String.trim()
  end

  @spec execute(keyword(), binary()) :: :ok | {:error, term()}
  defp execute(opts, query) do
    case http_client(opts).request(base_url(opts), query, request_headers(opts), opts) do
      {:ok, status, _body} when status in [200, 201] ->
        :ok

      {:ok, status, body} ->
        {:error, {:clickhouse_request_failed, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec base_url(keyword()) :: String.t()
  defp base_url(opts) do
    url =
      opts
      |> option(:url, nil)
      |> case do
        value when is_binary(value) -> value
        other -> raise ArgumentError, ":url must be a non-empty string, got: #{inspect(other)}"
      end
      |> String.trim_trailing("/")

    uri = URI.parse(url)

    query =
      uri.query
      |> case do
        nil -> %{}
        existing -> URI.decode_query(existing)
      end
      |> Map.put_new("date_time_input_format", "best_effort")
      |> encode_auth(opts)
      |> URI.encode_query()

    %URI{uri | query: query}
    |> URI.to_string()
  end

  @spec encode_auth(map(), keyword()) :: map()
  defp encode_auth(query, opts) do
    query
    |> maybe_put("database", database(opts))
    |> maybe_put("user", option(opts, :username, nil))
    |> maybe_put("password", option(opts, :password, nil))
  end

  @spec maybe_put(map(), String.t(), term()) :: map()
  defp maybe_put(query, _key, nil), do: query
  defp maybe_put(query, _key, ""), do: query
  defp maybe_put(query, key, value), do: Map.put(query, key, to_string(value))

  @spec request_headers(keyword()) :: [{String.t(), String.t()}]
  defp request_headers(_opts) do
    [{"content-type", "text/plain; charset=utf-8"}]
  end

  @spec http_client(keyword()) :: module()
  defp http_client(opts) do
    option(opts, :http_client, __MODULE__.HTTP)
  end

  @spec database(keyword()) :: String.t()
  defp database(opts) do
    opts
    |> option(:database, @default_database)
    |> validate_identifier!(:database)
  end

  @spec executions_table(keyword()) :: String.t()
  defp executions_table(opts) do
    opts
    |> option(:executions_table, @default_executions_table)
    |> validate_identifier!(:executions_table)
  end

  @spec events_table(keyword()) :: String.t()
  defp events_table(opts) do
    opts
    |> option(:events_table, @default_events_table)
    |> validate_identifier!(:events_table)
  end

  @spec execution_facts_table(keyword()) :: String.t()
  defp execution_facts_table(opts) do
    opts
    |> option(:execution_facts_table, @default_execution_facts_table)
    |> validate_identifier!(:execution_facts_table)
  end

  @spec task_runs_table(keyword()) :: String.t()
  defp task_runs_table(opts) do
    opts
    |> option(:task_runs_table, @default_task_runs_table)
    |> validate_identifier!(:task_runs_table)
  end

  @spec option(keyword(), atom(), term()) :: term()
  defp option(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        case Keyword.get(opts, :settings) do
          %{} = settings ->
            Map.get(settings, Atom.to_string(key), Map.get(settings, key, default))

          _ ->
            default
        end
    end
  end

  @spec identifier(String.t()) :: String.t()
  defp identifier(value), do: value

  @spec validate_identifier!(term(), atom()) :: String.t()
  defp validate_identifier!(value, _name) when is_binary(value) do
    trimmed = String.trim(value)

    if trimmed =~ ~r/^[A-Za-z_][A-Za-z0-9_]*$/ do
      trimmed
    else
      raise ArgumentError, "invalid ClickHouse identifier: #{inspect(value)}"
    end
  end

  defp validate_identifier!(value, name) do
    raise ArgumentError, "#{inspect(name)} must be a non-empty identifier, got: #{inspect(value)}"
  end

  @spec fetch(map(), atom()) :: term()
  defp fetch(map, key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  @spec nullable_string(term()) :: String.t() | nil
  defp nullable_string(nil), do: nil
  defp nullable_string(value), do: stringify(value)

  @spec stringify(term()) :: String.t()
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value), do: to_string(value)

  @spec integer(term()) :: non_neg_integer()
  defp integer(value) when is_integer(value) and value >= 0, do: value
  defp integer(_value), do: 0

  @spec nullable_integer(term()) :: integer() | nil
  defp nullable_integer(nil), do: nil
  defp nullable_integer(value) when is_integer(value), do: value
  defp nullable_integer(_value), do: nil

  @spec iso8601(term()) :: String.t() | nil
  defp iso8601(nil), do: nil

  defp iso8601(%DateTime{} = value),
    do: DateTime.to_iso8601(DateTime.truncate(value, :microsecond))

  defp iso8601(%NaiveDateTime{} = value),
    do: NaiveDateTime.to_iso8601(NaiveDateTime.truncate(value, :microsecond))

  defp iso8601(value) when is_binary(value), do: value
  defp iso8601(_value), do: nil

  @spec duration_ms(term(), term()) :: integer() | nil
  defp duration_ms(nil, _finished_at), do: nil
  defp duration_ms(_started_at, nil), do: nil

  defp duration_ms(started_at, finished_at) do
    with {:ok, started_at, _} <- to_datetime(started_at),
         {:ok, finished_at, _} <- to_datetime(finished_at) do
      DateTime.diff(finished_at, started_at, :millisecond)
    else
      _ -> nil
    end
  end

  @spec to_datetime(term()) :: {:ok, DateTime.t(), integer()} | :error
  defp to_datetime(%DateTime{} = value), do: {:ok, DateTime.truncate(value, :microsecond), 0}

  defp to_datetime(%NaiveDateTime{} = value) do
    {:ok, DateTime.from_naive!(NaiveDateTime.truncate(value, :microsecond), "Etc/UTC"), 0}
  end

  defp to_datetime(value) when is_binary(value), do: DateTime.from_iso8601(value)
  defp to_datetime(_value), do: :error

  @spec count_events([map()], atom()) :: non_neg_integer()
  defp count_events(events, type) do
    Enum.count(events, fn event -> fetch(event, :type) in [type, Atom.to_string(type)] end)
  end

  @spec task_run_status(atom() | String.t()) :: String.t()
  defp task_run_status(:task_completed), do: "completed"
  defp task_run_status("task_completed"), do: "completed"
  defp task_run_status(:task_failed), do: "failed"
  defp task_run_status("task_failed"), do: "failed"
  defp task_run_status(:task_interrupted), do: "interrupted"
  defp task_run_status("task_interrupted"), do: "interrupted"

  @spec normalize_json(term()) :: term()
  defp normalize_json(%DateTime{} = value), do: iso8601(value)
  defp normalize_json(%NaiveDateTime{} = value), do: iso8601(value)

  defp normalize_json(%{} = value) do
    Map.new(value, fn {key, nested} -> {stringify(key), normalize_json(nested)} end)
  end

  defp normalize_json(value) when is_list(value) do
    Enum.map(value, &normalize_json/1)
  end

  defp normalize_json(value) when is_atom(value), do: stringify(value)
  defp normalize_json(value), do: value

  defmodule HTTP do
    @moduledoc false

    @spec request(String.t(), binary(), [{String.t(), String.t()}], keyword()) ::
            {:ok, pos_integer(), binary()} | {:error, term()}
    def request(url, body, headers, _opts) do
      _ = :inets.start()
      _ = :ssl.start()

      request =
        {String.to_charlist(url), normalize_headers(headers), ~c"text/plain", body}

      case :httpc.request(:post, request, [], body_format: :binary) do
        {:ok, {{_http_version, status, _reason_phrase}, _response_headers, response_body}} ->
          {:ok, status, response_body}

        {:error, reason} ->
          {:error, reason}
      end
    end

    @spec normalize_headers([{String.t(), String.t()}]) :: [{charlist(), charlist()}]
    defp normalize_headers(headers) do
      Enum.map(headers, fn {key, value} ->
        {String.to_charlist(String.downcase(key)), String.to_charlist(value)}
      end)
    end
  end
end
