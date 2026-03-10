defmodule Mix.Tasks.Perf.QueryFilter do
  @moduledoc false
  use Mix.Task
  require Logger

  @shortdoc "Run execution-list filter benchmark with pg_stat_statements output"

  @switches count: :integer,
            repeats: :integer,
            limit: :integer,
            id_sample: :integer,
            queue_count: :integer,
            workflow_count: :integer

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    if Mix.env() != :test do
      Mix.raise("perf.query_filter must run with MIX_ENV=test")
    end

    _ = Mix.Task.run("app.start")
    previous_level = Logger.level()
    Logger.configure(level: :info)
    {opts, _, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      invalid_switches =
        invalid
        |> Enum.map(fn {switch, _value} -> to_string(switch) end)
        |> Enum.join(", ")

      Mix.raise(
        "invalid options: #{invalid_switches}. Use dashed names (for example --id-sample, --queue-count)."
      )
    end

    count = positive(Keyword.get(opts, :count, 100_000), 100_000)
    repeats = positive(Keyword.get(opts, :repeats, 500), 500)
    limit = positive(Keyword.get(opts, :limit, 100), 100)
    id_sample = positive(Keyword.get(opts, :id_sample, 100), 100)
    queue_count = positive(Keyword.get(opts, :queue_count, 4), 4)
    workflow_count = positive(Keyword.get(opts, :workflow_count, 3), 3)
    prefix = "perf_query_filter_#{System.system_time(:millisecond)}"

    helper_call!(:start_repo!, [])
    {started_here?, repo_pid} = start_repo_link!()

    try do
      ensure_clean_prefix!(prefix)
      runtime_opts = helper_call!(:runtime_opts, [prefix])
      repo = Keyword.fetch!(runtime_opts, :repo)
      queue_names = build_queue_names(queue_count)
      workflow_names = build_workflow_names(workflow_count)

      print_header(count, repeats, limit, id_sample, queue_count, workflow_count)
      insert_seed_executions!(count, runtime_opts, queue_names, workflow_names)

      pg_stats_enabled = ensure_pg_stat_statements(repo)
      samples = load_samples!(runtime_opts, id_sample)
      scenarios = build_scenarios(limit, samples, queue_names, workflow_names)
      Mix.shell().info("scenarios=#{length(scenarios)}")
      results = run_scenarios(scenarios, repeats, runtime_opts)
      print_results(results)

      if pg_stats_enabled do
        print_pg_stat_statements(repo, prefix, 20)
      end
    after
      helper_call!(:cleanup_prefix!, [prefix])

      if started_here? do
        Process.exit(repo_pid, :normal)
      end

      Logger.configure(level: previous_level)
    end

    :ok
  end

  @spec run_scenarios([map()], pos_integer(), keyword()) :: [map()]
  defp run_scenarios(scenarios, repeats, runtime_opts) do
    Enum.map(scenarios, fn scenario ->
      {first_rows, latencies} =
        Enum.reduce(1..repeats, {nil, []}, fn _idx, {first_rows_acc, latencies_acc} ->
          started = System.monotonic_time(:microsecond)
          rows = Endurant.Executions.list(scenario.filters, runtime_opts)
          finished = System.monotonic_time(:microsecond)
          latency_ms = (finished - started) / 1000.0
          first_rows_next = if is_nil(first_rows_acc), do: rows, else: first_rows_acc
          {first_rows_next, [latency_ms | latencies_acc]}
        end)

      sorted = Enum.sort(latencies)

      %{
        name: scenario.name,
        rows: length(first_rows || []),
        p50_ms: percentile(sorted, 50),
        p95_ms: percentile(sorted, 95),
        p99_ms: percentile(sorted, 99)
      }
    end)
  end

  @spec print_header(
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: :ok
  defp print_header(count, repeats, limit, id_sample, queue_count, workflow_count) do
    Mix.shell().info("")
    Mix.shell().info("Endurant Execution Query Filter Benchmark")

    Mix.shell().info(
      "count=#{count} repeats=#{repeats} limit=#{limit} id_sample=#{id_sample} " <>
        "queues=#{queue_count} workflows=#{workflow_count}"
    )

    Mix.shell().info("")
    :ok
  end

  @spec print_results([map()]) :: :ok
  defp print_results(results) do
    Mix.shell().info("Filter results")

    Mix.shell().info(
      pad("#", 4) <>
        pad("scenario", 44) <>
        pad("rows", 8) <> pad("p50_ms", 10) <> pad("p95_ms", 10) <> "p99_ms"
    )

    results
    |> Enum.with_index(1)
    |> Enum.each(fn {row, idx} ->
      Mix.shell().info(
        pad("#{idx}", 4) <>
          pad(row.name, 44) <>
          pad("#{row.rows}", 8) <>
          pad(fmt(row.p50_ms), 10) <>
          pad(fmt(row.p95_ms), 10) <> fmt(row.p99_ms)
      )
    end)
  end

  @spec build_scenarios(pos_integer(), map(), [String.t()], [String.t()]) :: [map()]
  defp build_scenarios(limit, samples, queue_names, workflow_names) do
    queue_primary = List.first(queue_names)
    queue_secondary = Enum.at(queue_names, 1, queue_primary)
    queue_tertiary = Enum.at(queue_names, 2, queue_secondary)
    workflow_primary = List.first(workflow_names)
    workflow_secondary = Enum.at(workflow_names, 1, workflow_primary)
    workflow_tertiary = Enum.at(workflow_names, 2, workflow_secondary)

    queue_pair = Enum.uniq([queue_primary, queue_secondary])
    queue_triplet = Enum.uniq([queue_primary, queue_secondary, queue_tertiary])
    workflow_pair = Enum.uniq([workflow_primary, workflow_secondary])
    workflow_triplet = Enum.uniq([workflow_primary, workflow_secondary, workflow_tertiary])

    inserted_range = [inserted_after: samples.range_after, inserted_before: samples.range_before]
    updated_range = [updated_after: samples.updated_after, updated_before: samples.updated_before]

    cursor_filter = [
      order: :desc,
      cursor: %{inserted_at: samples.cursor_inserted_at, id: samples.cursor_id}
    ]

    base_scenarios = [
      %{name: "no filter", filters: [limit: limit]},
      %{name: "no filter (asc)", filters: [order: :asc, limit: limit]},
      %{name: "queue only", filters: [queue: queue_primary, limit: limit]},
      %{name: "workflow only", filters: [workflow: workflow_primary, limit: limit]},
      %{
        name: "queue + workflow",
        filters: [queue: queue_primary, workflow: workflow_primary, limit: limit]
      },
      %{name: "queue in [2]", filters: [queue: queue_pair, limit: limit]},
      %{name: "queue in [3]", filters: [queue: queue_triplet, limit: limit]},
      %{name: "workflow in [2]", filters: [workflow: workflow_pair, limit: limit]},
      %{name: "workflow in [3]", filters: [workflow: workflow_triplet, limit: limit]},
      %{name: "inserted range only", filters: inserted_range ++ [limit: limit]},
      %{name: "updated range only", filters: updated_range ++ [limit: limit]},
      %{name: "cursor only", filters: cursor_filter ++ [limit: limit]},
      %{
        name: "queue + inserted range",
        filters: [queue: queue_primary] ++ inserted_range ++ [limit: limit]
      },
      %{
        name: "workflow + inserted range",
        filters: [workflow: workflow_primary] ++ inserted_range ++ [limit: limit]
      },
      %{
        name: "queue + updated range",
        filters: [queue: queue_primary] ++ updated_range ++ [limit: limit]
      },
      %{
        name: "workflow + updated range",
        filters: [workflow: workflow_primary] ++ updated_range ++ [limit: limit]
      },
      %{
        name: "queue + cursor",
        filters: [queue: queue_primary] ++ cursor_filter ++ [limit: limit]
      },
      %{
        name: "workflow + cursor",
        filters: [workflow: workflow_primary] ++ cursor_filter ++ [limit: limit]
      },
      %{
        name: "queue + workflow + cursor",
        filters:
          [queue: queue_primary, workflow: workflow_primary] ++ cursor_filter ++ [limit: limit]
      }
    ]

    status_scenarios = [
      %{name: "open", filters: [open: true, limit: limit]},
      %{name: "terminal", filters: [terminal: true, limit: limit]},
      %{name: "status waiting", filters: [status: :waiting, limit: limit]},
      %{
        name: "status multi open",
        filters: [status: [:running, :waiting, :continuable], limit: limit]
      },
      %{name: "open + queue", filters: [open: true, queue: queue_primary, limit: limit]},
      %{name: "open + workflow", filters: [open: true, workflow: workflow_primary, limit: limit]},
      %{
        name: "open + queue + workflow",
        filters: [open: true, queue: queue_primary, workflow: workflow_primary, limit: limit]
      },
      %{name: "open + queue in [2]", filters: [open: true, queue: queue_pair, limit: limit]},
      %{
        name: "open + workflow in [2]",
        filters: [open: true, workflow: workflow_pair, limit: limit]
      },
      %{name: "open + inserted range", filters: [open: true] ++ inserted_range ++ [limit: limit]},
      %{name: "open + updated range", filters: [open: true] ++ updated_range ++ [limit: limit]},
      %{name: "open + cursor", filters: [open: true] ++ cursor_filter ++ [limit: limit]},
      %{
        name: "open + queue + cursor",
        filters: [open: true, queue: queue_primary] ++ cursor_filter ++ [limit: limit]
      },
      %{name: "terminal + queue", filters: [terminal: true, queue: queue_primary, limit: limit]},
      %{
        name: "terminal + workflow",
        filters: [terminal: true, workflow: workflow_primary, limit: limit]
      },
      %{
        name: "terminal + queue + workflow",
        filters: [terminal: true, queue: queue_primary, workflow: workflow_primary, limit: limit]
      },
      %{
        name: "terminal + inserted range",
        filters: [terminal: true] ++ inserted_range ++ [limit: limit]
      },
      %{name: "terminal + cursor", filters: [terminal: true] ++ cursor_filter ++ [limit: limit]}
    ]

    id_scenarios = [
      %{name: "by unique_id", filters: [unique_id: samples.unique_id, limit: limit]},
      %{name: "by execution_id", filters: [execution_id: samples.execution_id, limit: limit]},
      %{name: "by execution_ids", filters: [execution_ids: samples.execution_ids, limit: limit]}
    ]

    base_scenarios ++ status_scenarios ++ id_scenarios
  end

  @spec load_samples!(keyword(), pos_integer()) :: map()
  defp load_samples!(runtime_opts, id_sample) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    sample_size = max(id_sample, 200)

    rows =
      repo.query!(
        "SELECT id, unique_id, inserted_at, updated_at, queue, workflow_name
FROM #{prefix}.endurant_executions
ORDER BY inserted_at DESC, id DESC
LIMIT $1
",
        [sample_size],
        log: false
      ).rows

    if rows == [] do
      Mix.raise("no seed rows found")
    end

    top_row = hd(rows)
    first_row = hd(rows)
    unique_id = elem_from_row(top_row, 1)
    cursor_id = to_app_id(elem_from_row(top_row, 0))
    cursor_inserted_at = elem_from_row(top_row, 2)
    execution_id = to_app_id(elem_from_row(first_row, 0))

    execution_ids =
      rows
      |> Enum.take(id_sample)
      |> Enum.map(fn row -> to_app_id(elem_from_row(row, 0)) end)

    after_row = Enum.at(rows, div(length(rows) * 2, 3), List.last(rows))
    before_row = Enum.at(rows, div(length(rows), 3), hd(rows))

    {range_after, range_before} =
      ordered_time_range(elem_from_row(after_row, 2), elem_from_row(before_row, 2))

    {updated_after, updated_before} =
      ordered_time_range(elem_from_row(after_row, 3), elem_from_row(before_row, 3))

    %{
      unique_id: unique_id,
      execution_id: execution_id,
      execution_ids: execution_ids,
      range_after: range_after,
      range_before: range_before,
      updated_after: updated_after,
      updated_before: updated_before,
      cursor_inserted_at: cursor_inserted_at,
      cursor_id: cursor_id
    }
  end

  @spec ordered_time_range(NaiveDateTime.t(), NaiveDateTime.t()) ::
          {NaiveDateTime.t(), NaiveDateTime.t()}
  defp ordered_time_range(a, b) do
    case NaiveDateTime.compare(a, b) do
      :lt -> {a, b}
      :eq -> {NaiveDateTime.add(a, -1, :second), NaiveDateTime.add(b, 1, :second)}
      :gt -> {b, a}
    end
  end

  @spec elem_from_row(list(), non_neg_integer()) :: term()
  defp elem_from_row(row, idx) do
    case Enum.at(row, idx) do
      nil -> Mix.raise("unexpected sample row shape: #{inspect(row)}")
      value -> value
    end
  end

  @spec insert_seed_executions!(pos_integer(), keyword(), [String.t()], [String.t()]) :: :ok
  defp insert_seed_executions!(count, runtime_opts, queue_names, workflow_names) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    now = NaiveDateTime.utc_now() |> NaiveDateTime.add(-count * 2, :second)
    chunk_size = 1000

    Stream.iterate(1, fn value -> value + chunk_size end)
    |> Enum.take_while(&(&1 <= count))
    |> Enum.each(fn start_id ->
      end_id = min(start_id + chunk_size - 1, count)
      rows = build_seed_rows(start_id, end_id, now, queue_names, workflow_names)
      insert_seed_chunk!(repo, prefix, rows)
    end)

    :ok
  end

  @spec build_seed_rows(pos_integer(), pos_integer(), NaiveDateTime.t(), [String.t()], [
          String.t()
        ]) :: [map()]
  defp build_seed_rows(start_id, end_id, base_time, queue_names, workflow_names) do
    queue_count = length(queue_names)
    workflow_count = length(workflow_names)

    Enum.map(start_id..end_id, fn idx ->
      execution_id = Ecto.UUID.generate()
      status = status_for_index(idx)
      inserted_at = NaiveDateTime.add(base_time, idx, :millisecond)
      updated_at = NaiveDateTime.add(inserted_at, rem(idx, 7), :second)
      completed_at = if status in ["completed", "failed", "cancelled"], do: updated_at, else: nil

      waiting_until =
        if status == "waiting", do: NaiveDateTime.add(updated_at, 10, :second), else: nil

      locked_by =
        if status in ["running", "waiting", "continuable", "cancelling"],
          do: "worker-#{rem(idx, 32)}",
          else: nil

      locked_until =
        if is_binary(locked_by), do: NaiveDateTime.add(updated_at, 30, :second), else: nil

      %{
        id: to_db_id(execution_id),
        unique_id: "perf-filter:#{idx}",
        queue: Enum.at(queue_names, rem(idx - 1, queue_count), "queue_1"),
        workflow_name:
          Enum.at(workflow_names, rem(idx - 1, workflow_count), "Perf.QueryFilter.Workflow1"),
        version: "1",
        input: %{"id" => idx},
        status: status,
        waiting_until: waiting_until,
        locked_by: locked_by,
        locked_until: locked_until,
        completed_at: completed_at,
        inserted_at: inserted_at,
        updated_at: updated_at
      }
    end)
  end

  @spec insert_seed_chunk!(module(), String.t(), [map()]) :: :ok
  defp insert_seed_chunk!(repo, prefix, rows) do
    values_sql =
      rows
      |> Enum.with_index(0)
      |> Enum.map(fn {_row, row_idx} ->
        base = row_idx * 13

        "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}, $#{base + 5}, $#{base + 6}, " <>
          "$#{base + 7}::#{prefix}.endurant_execution_status, $#{base + 8}, $#{base + 9}, " <>
          "$#{base + 10}, $#{base + 11}, $#{base + 12}, $#{base + 13})"
      end)
      |> Enum.join(",\n")

    params =
      Enum.flat_map(rows, fn row ->
        [
          row.id,
          row.unique_id,
          row.queue,
          row.workflow_name,
          row.version,
          row.input,
          row.status,
          row.waiting_until,
          row.locked_by,
          row.locked_until,
          row.completed_at,
          row.inserted_at,
          row.updated_at
        ]
      end)

    sql = """
    INSERT INTO #{prefix}.endurant_executions (
      id,
      unique_id,
      queue,
      workflow_name,
      version,
      input,
      status,
      waiting_until,
      locked_by,
      locked_until,
      completed_at,
      inserted_at,
      updated_at
    )
    VALUES
    #{values_sql}
    """

    _ = repo.query!(sql, params, log: false)
    :ok
  end

  @spec status_for_index(pos_integer()) :: String.t()
  defp status_for_index(idx) do
    case rem(idx, 10) do
      0 -> "completed"
      1 -> "failed"
      2 -> "cancelled"
      3 -> "running"
      4 -> "waiting"
      5 -> "continuable"
      6 -> "abandoned"
      7 -> "cancelling"
      _ -> "pending"
    end
  end

  @spec build_queue_names(pos_integer()) :: [String.t()]
  defp build_queue_names(count) do
    Enum.map(1..count, fn idx -> "queue_#{idx}" end)
  end

  @spec build_workflow_names(pos_integer()) :: [String.t()]
  defp build_workflow_names(count) do
    Enum.map(1..count, fn idx -> "Perf.QueryFilter.Workflow#{idx}" end)
  end

  @spec percentile([float()], pos_integer()) :: float()
  defp percentile([], _p), do: 0.0

  defp percentile(values, p) do
    rank = max(ceil(length(values) * p / 100), 1)
    Enum.at(values, rank - 1, 0.0)
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

  @spec ensure_clean_prefix!(String.t()) :: :ok
  defp ensure_clean_prefix!(prefix) do
    helper_call!(:cleanup_prefix!, [prefix])
    helper_call!(:prepare_prefix!, [prefix])
    :ok
  end

  @spec start_repo_link!() :: {boolean(), pid()}
  defp start_repo_link! do
    case helper_repo().start_link() do
      {:ok, pid} ->
        {true, pid}

      {:error, {:already_started, pid}} ->
        {false, pid}
    end
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
    rows =
      repo.query!(
        "SELECT queryid::text, calls, total_exec_time, mean_exec_time, query
FROM pg_stat_statements
WHERE query LIKE $1
ORDER BY total_exec_time DESC
LIMIT $2
",
        ["%#{prefix}.endurant_executions%", limit],
        log: false
      ).rows

    case rows do
      [] ->
        Mix.shell().info("pg_stat_statements: no statements captured")

      _ ->
        Mix.shell().info("")
        Mix.shell().info("Top pg_stat_statements (total_exec_time ms)")

        Mix.shell().info(
          pad("#", 4) <>
            pad("calls", 10) <>
            pad("total_ms", 12) <> pad("mean_ms", 11) <> pad("queryid", 24) <> "query"
        )

        rows
        |> Enum.with_index(1)
        |> Enum.each(fn {[query_id, calls, total_ms, mean_ms, query], idx} ->
          normalized = normalize_query_text(query, prefix)

          Mix.shell().info(
            pad("#{idx}", 4) <>
              pad("#{calls}", 10) <>
              pad(fmt(total_ms), 12) <>
              pad(fmt(mean_ms), 11) <> pad("#{query_id}", 24) <> normalized
          )
        end)
    end
  end

  @spec normalize_query_text(iodata(), String.t()) :: String.t()
  defp normalize_query_text(query, prefix) do
    query
    |> IO.iodata_to_binary()
    |> String.replace(prefix, "<prefix>")
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> String.slice(0, 140)
  end

  @spec format_db_error(term()) :: String.t()
  defp format_db_error(%{postgres: %{code: code, message: message}}) do
    "#{code} #{message}"
  end

  defp format_db_error(error), do: inspect(error)

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
