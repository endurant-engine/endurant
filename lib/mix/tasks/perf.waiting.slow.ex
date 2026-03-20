defmodule Mix.Tasks.Perf.Waiting.Slow do
  @moduledoc false
  use Mix.Task
  require Logger
  @shortdoc "Run waiting benchmark with detailed slow-query output"
  @switches steps: :integer,
            batch: :integer,
            concurrency: :integer,
            queues: :integer,
            time_wait_percent: :integer,
            time_wait_delay_ms: :integer,
            cancel_sample: :integer,
            retry_percent: :integer,
            cached_limit: :integer,
            poll: :integer,
            lease: :integer,
            signal_sample: :integer,
            insert_concurrency: :integer,
            progress_every: :integer
  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    if Mix.env() != :test do
      Mix.raise("perf.waiting.slow must run with MIX_ENV=test")
    end

    _ = Mix.Task.run("app.start")
    previous_level = Logger.level()
    Logger.configure(level: :info)
    {opts, _, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      invalid_switches =
        invalid |> Enum.map(fn {switch, _value} -> to_string(switch) end) |> Enum.join(", ")

      Mix.raise(
        "invalid options: #{invalid_switches}. Use dashed names (for example --cached-limit, --signal-sample)."
      )
    end

    steps = positive(Keyword.get(opts, :steps, 5), 5)
    batch = positive(Keyword.get(opts, :batch, 10000), 10000)
    concurrency = positive(Keyword.get(opts, :concurrency, 8), 8)
    queue_count = positive(Keyword.get(opts, :queues, 1), 1)
    queues = build_queues(queue_count)
    time_wait_percent = percent(Keyword.get(opts, :time_wait_percent, 0), 0)
    time_wait_delay_ms = positive(Keyword.get(opts, :time_wait_delay_ms, 1000), 1000)
    cancel_sample = non_negative(Keyword.get(opts, :cancel_sample, 0), 0)
    retry_percent = percent(Keyword.get(opts, :retry_percent, 0), 0)
    cached_limit = non_negative(Keyword.get(opts, :cached_limit, 0), 0)
    poll_interval = positive(Keyword.get(opts, :poll, 50), 50)
    lease_ms = positive(Keyword.get(opts, :lease, 30000), 30000)
    signal_sample = positive(Keyword.get(opts, :signal_sample, 20), 20)
    insert_concurrency = positive(Keyword.get(opts, :insert_concurrency, 32), 32)
    progress_every = positive(Keyword.get(opts, :progress_every, 5000), 5000)
    prefix = "perf_waiting_slow_#{System.system_time(:millisecond)}"
    helper_call!(:start_repo!, [])
    {:ok, repo_pid} = helper_repo().start_link()
    pg_stats_enabled = ensure_pg_stat_statements(helper_repo())
    engine_name = "perf_waiting_slow"

    try do
      ensure_clean_prefix!(prefix)
      runtime_opts = helper_call!(:runtime_opts, [prefix]) |> Keyword.put(:instance, engine_name)
      repo = Keyword.fetch!(runtime_opts, :repo)
      schema_prefix = Keyword.fetch!(runtime_opts, :prefix)

      {:ok, supervisor_pid} =
        Endurant.start_link(
          name: engine_name,
          repo: repo,
          prefix: schema_prefix,
          queues:
            Enum.map(queues, fn queue ->
              {queue,
               concurrency: concurrency,
               cached_limit: cached_limit,
               poll_interval: poll_interval,
               lease_ms: lease_ms}
            end)
        )

      try do
        print_header(
          steps,
          batch,
          concurrency,
          cached_limit,
          poll_interval,
          signal_sample,
          insert_concurrency,
          queue_count,
          time_wait_percent,
          time_wait_delay_ms,
          cancel_sample,
          retry_percent
        )

        run_steps(
          steps,
          batch,
          signal_sample,
          cancel_sample,
          engine_name,
          runtime_opts,
          insert_concurrency,
          progress_every,
          queues,
          time_wait_percent,
          time_wait_delay_ms,
          retry_percent
        )

        if pg_stats_enabled do
          print_pg_stat_statements(helper_repo(), prefix, 15)
        end
      after
        _ = Supervisor.stop(supervisor_pid)
      end
    after
      helper_call!(:cleanup_prefix!, [prefix])
      Process.exit(repo_pid, :normal)
      Logger.configure(level: previous_level)
    end

    :ok
  end

  @spec run_steps(
          pos_integer(),
          pos_integer(),
          pos_integer(),
          non_neg_integer(),
          module(),
          keyword(),
          pos_integer(),
          pos_integer(),
          [atom()],
          non_neg_integer(),
          pos_integer(),
          non_neg_integer()
        ) :: :ok
  defp run_steps(
         steps,
         batch,
         signal_sample,
         cancel_sample,
         engine_name,
         runtime_opts,
         insert_concurrency,
         progress_every,
         queues,
         time_wait_percent,
         time_wait_delay_ms,
         retry_percent
       ) do
    _ =
      Enum.reduce(
        1..steps,
        %{inserted_total: 0, waiting_target: 0, printed_columns?: false},
        fn step, acc ->
          next_inserted_total = acc.inserted_total + batch
          next_waiting_target = acc.waiting_target + batch

          insert_batch!(
            acc.inserted_total + 1,
            next_inserted_total,
            runtime_opts,
            insert_concurrency,
            progress_every,
            queues,
            time_wait_percent,
            time_wait_delay_ms,
            retry_percent
          )

          cancelled = cancel_waiting_executions(cancel_sample, runtime_opts)
          waiting_rows = waiting_count(runtime_opts)
          signal_waiting_rows = signal_waiting_count(runtime_opts)
          sampled = min(signal_sample, signal_waiting_rows)
          signal_latencies = measure_signal_resume_latencies(sampled, runtime_opts)

          row = %{
            step: step,
            waiting_rows: waiting_rows,
            processes: :erlang.system_info(:process_count),
            memory_mb: :erlang.memory(:total) / 1_048_576.0,
            cached_count: cached_count(engine_name, queues),
            resume_p50: percentile(signal_latencies, 50),
            resume_p95: percentile(signal_latencies, 95),
            resume_p99: percentile(signal_latencies, 99)
          }

          {printed_columns?, _} =
            if acc.printed_columns? do
              {true, :ok}
            else
              Mix.shell().info("")
              print_columns()
              {true, :ok}
            end

          print_row(row)

          %{
            inserted_total: next_inserted_total,
            waiting_target: max(next_waiting_target - sampled - cancelled, 0),
            printed_columns?: printed_columns?
          }
        end
      )

    :ok
  end

  @spec insert_batch!(
          pos_integer(),
          pos_integer(),
          keyword(),
          pos_integer(),
          pos_integer(),
          [atom()],
          non_neg_integer(),
          pos_integer(),
          non_neg_integer()
        ) :: :ok
  defp insert_batch!(
         from_id,
         to_id,
         runtime_opts,
         insert_concurrency,
         _progress_every,
         queues,
         time_wait_percent,
         time_wait_delay_ms,
         retry_percent
       )
       when from_id <= to_id do
    _ = insert_concurrency
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    chunk_size = 1000
    queue_names = Enum.map(queues, &Atom.to_string/1)
    queue_count = length(queue_names)
    time_wait_run_at = DateTime.add(DateTime.utc_now(), time_wait_delay_ms, :millisecond)
    time_wait_until = DateTime.to_naive(time_wait_run_at)
    time_wait_until_iso = DateTime.to_iso8601(time_wait_run_at)

    seed_rows =
      Enum.map(from_id..to_id, fn id ->
        execution_id = Ecto.UUID.generate()
        {:ok, execution_db_id} = Ecto.UUID.dump(execution_id)
        queue = Enum.at(queue_names, rem(id - 1, queue_count), "perf")
        time_wait? = rem(id - 1, 100) < time_wait_percent
        retry_task? = rem(id * 17 + 13, 100) < retry_percent

        {workflow_name, waiting_until, waiting_payload} =
          if time_wait? do
            {"Endurant.Perf.WaitingSlowTimeWorkflow", time_wait_until,
             %{
               mode: :time,
               until: time_wait_until_iso,
               delay_ms: time_wait_delay_ms,
               wait_key: "bench:time"
             }}
          else
            {"Endurant.Perf.WaitingSlowWorkflow", nil, %{mode: :signal, signal: "go"}}
          end

        %{
          id: execution_db_id,
          unique_id: "perf-waiting:#{id}",
          queue: queue,
          workflow_name: workflow_name,
          version: "1",
          waiting_until: waiting_until,
          waiting_payload: waiting_payload,
          input: %{id: id, retry_task: retry_task?}
        }
      end)

    _processed =
      seed_rows
      |> Enum.chunk_every(chunk_size)
      |> Enum.reduce(0, fn chunk, processed ->
        seed_waiting_chunk!(repo, prefix, chunk)
        inserted = length(chunk)
        next = processed + inserted
        next
      end)

    :ok
  end

  @spec seed_waiting_chunk!(module(), String.t(), [map()]) :: :ok
  defp seed_waiting_chunk!(repo, prefix, rows) do
    _ =
      repo.transaction(
        fn ->
          insert_waiting_executions!(repo, prefix, rows)
          insert_seed_events!(repo, prefix, rows)
        end,
        log: false
      )

    :ok
  end

  @spec insert_waiting_executions!(module(), String.t(), [map()]) :: :ok
  defp insert_waiting_executions!(repo, prefix, rows) do
    {values_sql_rev, params_rev} =
      rows
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {row, idx}, {values_acc, params_acc} ->
        offset = idx * 7

        value_sql =
          "($" <>
            Integer.to_string(offset + 1) <>
            ", $" <>
            Integer.to_string(offset + 2) <>
            ", $" <>
            Integer.to_string(offset + 3) <>
            ", $" <>
            Integer.to_string(offset + 4) <>
            ", $" <>
            Integer.to_string(offset + 5) <>
            ", $" <>
            Integer.to_string(offset + 6) <>
            ", $" <>
            Integer.to_string(offset + 7) <>
            ", 'waiting'::" <>
            prefix <>
            ".endurant_execution_status, timezone('UTC', now()), timezone('UTC', now()))"

        params_row = [
          row.waiting_until,
          row.input,
          row.version,
          row.workflow_name,
          row.queue,
          row.unique_id,
          row.id
        ]

        {[value_sql | values_acc], params_row ++ params_acc}
      end)

    values_sql = Enum.reverse(values_sql_rev)
    params = Enum.reverse(params_rev)
    sql = "INSERT INTO #{prefix}.endurant_executions
  (id, unique_id, queue, workflow_name, version, input, waiting_until, status, inserted_at, updated_at)
VALUES #{Enum.join(values_sql, ",")}
ON CONFLICT DO NOTHING
"
    _ = repo.query!(sql, params, log: false)
    :ok
  end

  @spec insert_seed_events!(module(), String.t(), [map()]) :: :ok
  defp insert_seed_events!(repo, prefix, rows) do
    {values_sql_rev, params_rev} =
      rows
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {row, idx}, {values_acc, params_acc} ->
        offset = idx * 8

        value_sql =
          "($" <>
            Integer.to_string(offset + 1) <>
            ", $" <>
            Integer.to_string(offset + 2) <>
            ", $" <>
            Integer.to_string(offset + 3) <>
            "::" <>
            prefix <>
            ".endurant_event_type, $" <>
            Integer.to_string(offset + 4) <>
            "), ($" <>
            Integer.to_string(offset + 5) <>
            ", $" <>
            Integer.to_string(offset + 6) <>
            ", $" <>
            Integer.to_string(offset + 7) <>
            "::" <> prefix <> ".endurant_event_type, $" <> Integer.to_string(offset + 8) <> ")"

        created_payload = %{
          workflow: row.workflow_name,
          unique_id: row.unique_id,
          version: row.version
        }

        waiting_payload = row.waiting_payload

        params_row = [
          waiting_payload,
          "execution_waiting",
          2,
          row.id,
          created_payload,
          "execution_created",
          1,
          row.id
        ]

        {[value_sql | values_acc], params_row ++ params_acc}
      end)

    values_sql = Enum.reverse(values_sql_rev)
    params = Enum.reverse(params_rev)
    sql = "INSERT INTO #{prefix}.endurant_events
  (execution_id, sequence, type, payload)
VALUES #{Enum.join(values_sql, ",")}
ON CONFLICT DO NOTHING
"
    _ = repo.query!(sql, params, log: false)
    :ok
  end

  @spec waiting_count(keyword()) :: non_neg_integer()
  defp waiting_count(runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sql = "SELECT COUNT(*)
FROM #{prefix}.endurant_executions
WHERE status = 'waiting'::#{prefix}.endurant_execution_status
"

    case repo.query!(sql, [], log: false).rows do
      [[count]] when is_integer(count) -> count
      _ -> 0
    end
  end

  @spec signal_waiting_count(keyword()) :: non_neg_integer()
  defp signal_waiting_count(runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sql = "SELECT COUNT(*)
FROM #{prefix}.endurant_executions
WHERE status = 'waiting'::#{prefix}.endurant_execution_status
AND waiting_until IS NULL
"

    case repo.query!(sql, [], log: false).rows do
      [[count]] when is_integer(count) -> count
      _ -> 0
    end
  end

  @spec cancel_waiting_executions(non_neg_integer(), keyword()) :: non_neg_integer()
  defp cancel_waiting_executions(0, _runtime_opts) do
    0
  end

  defp cancel_waiting_executions(sample_size, runtime_opts) do
    sample_size
    |> waiting_ids_for_cancel(runtime_opts)
    |> Enum.reduce(0, fn id, acc ->
      case Endurant.cancel(id, instance: Keyword.fetch!(runtime_opts, :instance)) do
        :ok -> acc + 1
        _ -> acc
      end
    end)
  end

  @spec waiting_ids_for_cancel(non_neg_integer(), keyword()) :: [binary()]
  defp waiting_ids_for_cancel(0, _runtime_opts) do
    []
  end

  defp waiting_ids_for_cancel(limit, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sql = "SELECT id
FROM #{prefix}.endurant_executions
WHERE status = 'waiting'::#{prefix}.endurant_execution_status
ORDER BY inserted_at ASC
LIMIT $1
"
    repo.query!(sql, [limit], log: false).rows |> Enum.map(fn [id] -> to_app_id(id) end)
  end

  @spec cached_count(String.t(), [atom()]) :: non_neg_integer()
  defp cached_count(engine_name, queues) when is_list(queues) do
    Enum.reduce(queues, 0, fn queue, acc ->
      queue_name = Endurant.Supervisor.queue_manager_name(engine_name, queue)

      cached =
        case :sys.get_state(queue_name) do
          %{cached: cached} when is_map(cached) -> map_size(cached)
          _ -> 0
        end

      acc + cached
    end)
  end

  @spec measure_signal_resume_latencies(pos_integer(), keyword()) :: [float()]
  defp measure_signal_resume_latencies(0, _runtime_opts) do
    []
  end

  @spec measure_signal_resume_latencies(pos_integer(), keyword()) :: [float()]
  defp measure_signal_resume_latencies(sample_size, runtime_opts) do
    ids = waiting_ids(sample_size, runtime_opts)

    Enum.each(ids, fn id ->
      :ok =
        Endurant.signal(id, "go", %{bench: true},
          instance: Keyword.fetch!(runtime_opts, :instance)
        )
    end)

    signal_events = last_signal_events!(ids, runtime_opts)

    Enum.map(ids, fn id ->
      {signal_seq, signal_at} =
        case Map.fetch(signal_events, id) do
          {:ok, value} -> value
          :error -> Mix.raise("missing signal event for #{id}")
        end

      wait_resume_started_latency!(id, signal_seq, signal_at, runtime_opts, 30000)
    end)
  end

  @spec waiting_ids(pos_integer(), keyword()) :: [binary()]
  defp waiting_ids(limit, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sql = "SELECT id
FROM #{prefix}.endurant_executions
WHERE status = 'waiting'::#{prefix}.endurant_execution_status
AND waiting_until IS NULL
ORDER BY inserted_at ASC
LIMIT $1
"
    repo.query!(sql, [limit], log: false).rows |> Enum.map(fn [id] -> to_app_id(id) end)
  end

  @spec last_signal_events!([binary()], keyword()) :: %{
          binary() => {non_neg_integer(), NaiveDateTime.t()}
        }
  defp last_signal_events!([], _runtime_opts) do
    %{}
  end

  defp last_signal_events!(execution_ids, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    db_ids = Enum.map(execution_ids, &to_db_id/1)
    placeholders = db_ids |> Enum.with_index(1) |> Enum.map(fn {_id, idx} -> "$#{idx}" end)
    sql = "SELECT DISTINCT ON (execution_id) execution_id, sequence, inserted_at
FROM #{prefix}.endurant_events
WHERE execution_id IN (#{Enum.join(placeholders, ", ")})
AND type = 'signal_received'::#{prefix}.endurant_event_type
ORDER BY execution_id ASC, sequence DESC
"
    rows = repo.query!(sql, db_ids, log: false).rows

    rows_map =
      Enum.reduce(rows, %{}, fn
        [execution_id_db, seq, %NaiveDateTime{} = ts], acc when is_integer(seq) ->
          Map.put(acc, to_app_id(execution_id_db), {seq, ts})

        [execution_id_db, seq, %DateTime{} = ts], acc when is_integer(seq) ->
          Map.put(acc, to_app_id(execution_id_db), {seq, DateTime.to_naive(ts)})

        row, _acc ->
          Mix.raise("unexpected signal event row: #{inspect(row)}")
      end)

    missing_ids =
      execution_ids
      |> MapSet.new()
      |> MapSet.difference(MapSet.new(Map.keys(rows_map)))
      |> MapSet.to_list()

    if missing_ids != [] do
      Mix.raise("missing signal events for #{inspect(missing_ids)}")
    end

    rows_map
  end

  @spec wait_resume_started_latency!(
          binary(),
          non_neg_integer(),
          NaiveDateTime.t(),
          keyword(),
          pos_integer()
        ) :: float()
  defp wait_resume_started_latency!(execution_id, signal_seq, signal_at, runtime_opts, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    db_id = to_db_id(execution_id)
    do_wait_resume_started_latency(db_id, signal_seq, signal_at, runtime_opts, deadline)
  end

  @spec do_wait_resume_started_latency(
          binary(),
          non_neg_integer(),
          NaiveDateTime.t(),
          keyword(),
          integer()
        ) :: float()
  defp do_wait_resume_started_latency(db_id, signal_seq, signal_at, runtime_opts, deadline) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sql = "SELECT inserted_at
FROM #{prefix}.endurant_events
WHERE execution_id = $1
AND type = 'execution_started'::#{prefix}.endurant_event_type
AND sequence > $2
ORDER BY sequence ASC
LIMIT 1
"

    case repo.query!(sql, [db_id, signal_seq], log: false).rows do
      [[started_at]] when is_struct(started_at, NaiveDateTime) ->
        NaiveDateTime.diff(started_at, signal_at, :microsecond) / 1000.0

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          Mix.raise("timeout waiting for execution_started after signal")
        else
          Process.sleep(200)
          do_wait_resume_started_latency(db_id, signal_seq, signal_at, runtime_opts, deadline)
        end
    end
  end

  @spec ensure_clean_prefix!(String.t()) :: :ok
  defp ensure_clean_prefix!(prefix) do
    helper_call!(:cleanup_prefix!, [prefix])
    helper_call!(:prepare_prefix!, [prefix])
    :ok
  end

  @spec print_header(
          pos_integer(),
          pos_integer(),
          pos_integer(),
          non_neg_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  defp print_header(
         steps,
         batch,
         concurrency,
         cached_limit,
         poll_interval,
         signal_sample,
         insert_concurrency,
         queue_count,
         time_wait_percent,
         time_wait_delay_ms,
         cancel_sample,
         retry_percent
       ) do
    Mix.shell().info("")
    Mix.shell().info("Endurant Waiting Cardinality Benchmark")

    Mix.shell().info(
      "steps=#{steps} batch=#{batch} concurrency=#{concurrency} cached_limit=#{cached_limit} " <>
        "poll=#{poll_interval}ms signal_sample=#{signal_sample} insert_concurrency=#{insert_concurrency} " <>
        "queues=#{queue_count} time_wait_percent=#{time_wait_percent} " <>
        "time_wait_delay_ms=#{time_wait_delay_ms} cancel_sample=#{cancel_sample} " <>
        "retry_percent=#{retry_percent}"
    )

    Mix.shell().info("")
    :ok
  end

  @spec print_columns() :: :ok
  defp print_columns do
    Mix.shell().info(
      [
        pad("step", 5),
        pad("waiting_rows", 13),
        pad("processes", 10),
        pad("memory_mb", 10),
        pad("cached", 8),
        pad("resume_p95", 11),
        pad("resume_p99", 11)
      ]
      |> Enum.join(" ")
    )
  end

  @spec print_row(map()) :: :ok
  defp print_row(row) do
    Mix.shell().info(
      [
        pad("#{row.step}", 5),
        pad("#{row.waiting_rows}", 13),
        pad("#{row.processes}", 10),
        pad(fmt(row.memory_mb), 10),
        pad("#{row.cached_count}", 8),
        pad(fmt(row.resume_p95), 11),
        pad(fmt(row.resume_p99), 11)
      ]
      |> Enum.join(" ")
    )
  end

  @spec percentile([float()], pos_integer()) :: float()
  defp percentile([], _p) do
    0.0
  end

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    rank = max(ceil(length(sorted) * p / 100), 1)
    Enum.at(sorted, rank - 1, 0.0)
  end

  @spec fmt(number()) :: String.t()
  defp fmt(value) when is_number(value) do
    :erlang.float_to_binary(value * 1.0, decimals: 2)
  end

  @spec pad(String.t(), pos_integer()) :: String.t()
  defp pad(value, width) do
    String.pad_trailing(value, width)
  end

  @spec positive(term(), pos_integer()) :: pos_integer()
  defp positive(value, _default) when is_integer(value) and value > 0 do
    value
  end

  defp positive(_value, default) do
    default
  end

  @spec non_negative(term(), non_neg_integer()) :: non_neg_integer()
  defp non_negative(value, _default) when is_integer(value) and value >= 0 do
    value
  end

  defp non_negative(_value, default) do
    default
  end

  @spec percent(term(), non_neg_integer()) :: non_neg_integer()
  defp percent(value, _default) when is_integer(value) and value >= 0 and value <= 100 do
    value
  end

  defp percent(_value, default) do
    default
  end

  @spec build_queues(pos_integer()) :: [atom()]
  defp build_queues(1) do
    [:perf]
  end

  defp build_queues(count) when is_integer(count) and count > 1 do
    Enum.map(1..count, fn idx -> String.to_atom("perf_#{idx}") end)
  end

  @spec helper_module() :: module()
  defp helper_module do
    Module.concat([Endurant, TestSupport, PostgresHelper])
  end

  @spec helper_repo() :: module()
  defp helper_repo do
    Module.concat([helper_module(), Repo])
  end

  @spec helper_call!(atom(), [term()]) :: term()
  defp helper_call!(function, args) do
    apply(helper_module(), function, args)
  end

  @spec ensure_pg_stat_statements(module()) :: boolean()
  defp ensure_pg_stat_statements(repo) do
    case repo.query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", [], log: false) do
      {:ok, _} ->
        case repo.query("SELECT pg_stat_statements_reset()", [], log: false) do
          {:ok, _} ->
            Mix.shell().info("pg_stat_statements: enabled and reset")
            true

          {:error, error} ->
            Mix.shell().error("pg_stat_statements reset skipped: #{format_db_error(error)}")
            false
        end

      {:error, error} ->
        Mix.shell().error("pg_stat_statements setup skipped: #{format_db_error(error)}")
        false
    end
  end

  @spec print_pg_stat_statements(module(), String.t(), pos_integer()) :: :ok
  defp print_pg_stat_statements(repo, prefix, limit) do
    rows_total = fetch_top_pg_stat_statements(repo, prefix, limit, :total)
    rows_mean = fetch_top_pg_stat_statements(repo, prefix, limit, :mean)

    if rows_total == [] do
      Mix.shell().info("pg_stat_statements: no statements captured")
      :ok
    else
      print_pg_stat_rows("Top pg_stat_statements (total_exec_time ms)", rows_total, prefix)
      print_pg_stat_rows("Slowest pg_stat_statements (mean_exec_time ms)", rows_mean, prefix)
      :ok
    end
  end

  @spec print_pg_stat_rows(String.t(), [list()], String.t()) :: :ok
  defp print_pg_stat_rows(title, rows, prefix) do
    Mix.shell().info("")
    Mix.shell().info(title)

    Mix.shell().info(
      pad("#", 4) <>
        pad("operation", 23) <>
        pad("origin", 17) <>
        pad("calls", 10) <>
        pad("total_ms", 12) <> pad("mean_ms", 11) <> pad("queryid", 22) <> "query"
    )

    rows
    |> Enum.with_index(1)
    |> Enum.each(fn {[query_id, calls, row_total_ms, mean_ms, query], idx} ->
      operation = statement_label(query, prefix)
      origin = statement_origin(operation)

      Mix.shell().info(
        pad("#{idx}", 4) <>
          pad(operation, 23) <>
          pad(origin, 17) <>
          pad("#{calls}", 10) <>
          pad(fmt_number(row_total_ms), 12) <>
          pad(fmt_number(mean_ms), 11) <>
          pad("#{query_id}", 22) <> query_snippet(query, prefix, 90)
      )
    end)

    :ok
  end

  @spec fetch_top_pg_stat_statements(module(), String.t(), pos_integer(), :total | :mean) :: [
          list()
        ]
  defp fetch_top_pg_stat_statements(repo, prefix, limit, sort_by) do
    order_by =
      case sort_by do
        :mean -> "mean_exec_time DESC"
        _ -> "total_exec_time DESC"
      end

    filtered_sql = "SELECT queryid, calls, total_exec_time, mean_exec_time, query
FROM pg_stat_statements
WHERE query ILIKE $1
AND query NOT ILIKE '%pg_stat_statements%'
ORDER BY #{order_by}
LIMIT $2
"
    global_sql = "SELECT queryid, calls, total_exec_time, mean_exec_time, query
FROM pg_stat_statements
WHERE query NOT ILIKE '%pg_stat_statements%'
ORDER BY #{order_by}
LIMIT $1
"

    case repo.query(filtered_sql, ["%" <> prefix <> ".%", limit], log: false) do
      {:ok, %{rows: []}} ->
        case repo.query(global_sql, [limit], log: false) do
          {:ok, %{rows: rows}} ->
            rows

          {:error, error} ->
            Mix.shell().error("pg_stat_statements query skipped: #{format_db_error(error)}")
            []
        end

      {:ok, %{rows: rows}} ->
        rows

      {:error, error} ->
        Mix.shell().error("pg_stat_statements query skipped: #{format_db_error(error)}")
        []
    end
  end

  @spec statement_label(term(), String.t()) :: String.t()
  defp statement_label(query, prefix) when is_binary(query) do
    normalized = normalize_query(query, prefix)

    cond do
      String.starts_with?(normalized, "INSERT INTO <prefix>.endurant_events") and
          String.contains?(normalized, "COALESCE(MAX(sequence) + 1, 1)") ->
        "append event"

      String.starts_with?(normalized, "INSERT INTO <prefix>.endurant_events") ->
        "insert events"

      String.starts_with?(normalized, "INSERT INTO <prefix>.endurant_executions") ->
        "insert executions"

      String.starts_with?(normalized, "WITH candidate AS") and
          String.contains?(normalized, "candidate.status::text") ->
        "claim pending"

      String.starts_with?(normalized, "WITH candidate AS") ->
        "claim ready waiting"

      String.starts_with?(normalized, "WITH expired_waiting AS") ->
        "recover waiting"

      String.starts_with?(normalized, "WITH expired_running AS") ->
        "recover running"

      String.starts_with?(normalized, "WITH expired_continuable AS") ->
        "recover continuable"

      String.starts_with?(normalized, "WITH expired_waiting_ready AS") ->
        "recover waiting ready"

      String.starts_with?(normalized, "WITH expired_cancelling AS") ->
        "recover cancelling"

      String.starts_with?(normalized, "SELECT COUNT(*) FROM <prefix>.endurant_executions") ->
        "count executions"

      String.starts_with?(normalized, "SELECT id FROM <prefix>.endurant_executions") ->
        "list waiting ids"

      String.starts_with?(normalized, "UPDATE <prefix>.endurant_executions SET locked_until") ->
        "heartbeat update"

      String.starts_with?(normalized, "UPDATE <prefix>.endurant_executions SET status =") ->
        "status transition"

      String.starts_with?(normalized, "SELECT inserted_at FROM <prefix>.endurant_events") ->
        "resume latency read"

      true ->
        "other SQL"
    end
  end

  defp statement_label(_query, _prefix) do
    "other SQL"
  end

  @spec statement_origin(String.t()) :: String.t()
  defp statement_origin(operation) do
    if operation in [
         "insert events",
         "insert executions",
         "list waiting ids",
         "count executions",
         "resume latency read"
       ] do
      "benchmark_only"
    else
      "runtime_path"
    end
  end

  @spec normalize_query(String.t(), String.t()) :: String.t()
  defp normalize_query(query, prefix) do
    query
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> String.replace(~r/#{Regex.escape(prefix)}\./, "<prefix>.")
  end

  @spec query_snippet(term(), String.t(), pos_integer()) :: String.t()
  defp query_snippet(query, prefix, max_len) when is_binary(query) do
    normalized = normalize_query(query, prefix)

    if String.length(normalized) > max_len do
      String.slice(normalized, 0, max_len - 3) <> "..."
    else
      normalized
    end
  end

  defp query_snippet(query, _prefix, _max_len) do
    inspect(query)
  end

  @spec format_db_error(term()) :: String.t()
  defp format_db_error(%{__struct__: _} = error) do
    Exception.message(error)
  end

  defp format_db_error(error) do
    inspect(error)
  end

  @spec fmt_number(number() | term()) :: String.t()
  defp fmt_number(value) when is_number(value) do
    :erlang.float_to_binary(value * 1.0, decimals: 2)
  end

  defp fmt_number(value) do
    inspect(value)
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end

  @spec to_app_id(binary()) :: binary()
  defp to_app_id(id) do
    case Ecto.UUID.load(id) do
      {:ok, loaded} -> loaded
      :error -> id
    end
  end
end

defmodule Endurant.Perf.WaitingSlowWorkflow do
  @moduledoc false
  use Endurant.Workflow, version: "1"

  workflow do
    queue("perf")
    unique_id(fn input -> "perf-waiting:#{input.id}" end)
    @impl true
    @spec run(Endurant.Workflow.version(), Endurant.Workflow.input()) ::
            Endurant.Workflow.result()
    def run(_version, input) do
      _ = wait_signal("go")

      if retry_task_enabled?(input) do
        _ =
          task(
            input,
            "retry_probe",
            fn item ->
              id = workflow_id(item)

              if rem(id, 2) == 0 do
                raise "perf retry probe failure"
              else
                %{ok: true, id: id}
              end
            end,
            retry: [max_attempts: 2, backoff: :constant, base_ms: 1, max_ms: 1]
          )
      end

      %{id: input.id, done: true}
    end

    @spec retry_task_enabled?(map()) :: boolean()
    defp retry_task_enabled?(input) do
      Map.get(input, :retry_task, Map.get(input, "retry_task", false)) == true
    end

    @spec workflow_id(map()) :: integer()
    defp workflow_id(input) do
      Map.get(input, :id, Map.get(input, "id"))
    end
  end
end

defmodule Endurant.Perf.WaitingSlowTimeWorkflow do
  @moduledoc false
  use Endurant.Workflow, version: "1"

  workflow do
    queue("perf")
    unique_id(fn input -> "perf-waiting:#{input.id}" end)
    @impl true
    @spec run(Endurant.Workflow.version(), Endurant.Workflow.input()) ::
            Endurant.Workflow.result()
    def run(_version, input) do
      _ = sleep("bench:time", 1)
      %{id: input.id, done: true}
    end
  end
end
