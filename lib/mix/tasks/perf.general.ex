defmodule Mix.Tasks.Perf.General do
  @moduledoc """
  Runs a baseline end-to-end performance benchmark for Endurant.

  This benchmark measures:
  - Total wall time for insert + execute + complete
  - Throughput (executions/sec)
  - Latency percentiles (p50/p95/p99) from `execution_created` to `execution_completed`

  Run in test env so Postgres test deps/config are available:

      MIX_ENV=test mix perf.general

  Options:
  - `--count` number of executions per run (default: `1000`)
  - `--repeats` number of runs (default: `5`)
  - `--concurrency` queue concurrency (default: `8`)
  - `--poll` queue poll interval ms (default: `50`)
  - `--lease` lock lease ms (default: `30000`)
  """
  use Mix.Task
  require Logger

  @shortdoc "Run a general Endurant performance benchmark"

  @switches [
    count: :integer,
    repeats: :integer,
    concurrency: :integer,
    poll: :integer,
    lease: :integer
  ]

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    if Mix.env() != :test do
      Mix.raise("perf.general must run with MIX_ENV=test")
    end

    _ = Mix.Task.run("app.start")
    previous_level = Logger.level()
    Logger.configure(level: :info)

    {opts, _, _} = OptionParser.parse(args, strict: @switches)
    count = positive(Keyword.get(opts, :count, 1_000), 1_000)
    repeats = positive(Keyword.get(opts, :repeats, 5), 5)
    concurrency = positive(Keyword.get(opts, :concurrency, 8), 8)
    poll_interval = positive(Keyword.get(opts, :poll, 50), 50)
    lease_ms = positive(Keyword.get(opts, :lease, 30_000), 30_000)
    prefix = "perf_general_#{System.system_time(:millisecond)}"

    helper_call!(:start_repo!, [])

    {:ok, repo_pid} = helper_repo().start_link()

    try do
      runtime_opts = helper_call!(:runtime_opts, [prefix])
      workflow_module = Endurant.Perf.GeneralWorkflow

      results =
        Enum.map(1..repeats, fn run_idx ->
          ensure_clean_prefix!(prefix)

          queue_opts = [
            concurrency: concurrency,
            cached_limit: max(count * 2, 1_000),
            poll_interval: poll_interval,
            lease_ms: lease_ms
          ]

          supervisor_name = "perf_general_engine_#{run_idx}"

          {:ok, supervisor_pid} =
            Endurant.start_link(
              name: supervisor_name,
              queues: [perf: queue_opts ++ runtime_opts]
            )

          started_at = System.monotonic_time(:millisecond)

          insert_all!(workflow_module, count, runtime_opts)
          wait_for_completed!(count, workflow_module, runtime_opts, 120_000)

          finished_at = System.monotonic_time(:millisecond)
          duration_ms = max(finished_at - started_at, 1)
          throughput = count * 1_000 / duration_ms

          latencies_ms = completion_latencies_ms(workflow_module, runtime_opts)
          service_latencies_ms = service_latencies_ms(workflow_module, runtime_opts)
          p50_ms = percentile(latencies_ms, 50)
          p95_ms = percentile(latencies_ms, 95)
          p99_ms = percentile(latencies_ms, 99)
          svc_p50_ms = percentile(service_latencies_ms, 50)
          svc_p95_ms = percentile(service_latencies_ms, 95)
          svc_p99_ms = percentile(service_latencies_ms, 99)

          _ = Supervisor.stop(supervisor_pid)

          %{
            run: run_idx,
            duration_ms: duration_ms,
            throughput: throughput,
            p50_ms: p50_ms,
            p95_ms: p95_ms,
            p99_ms: p99_ms,
            svc_p50_ms: svc_p50_ms,
            svc_p95_ms: svc_p95_ms,
            svc_p99_ms: svc_p99_ms
          }
        end)

      print_summary(count, repeats, concurrency, poll_interval, results)
    after
      helper_call!(:cleanup_prefix!, [prefix])
      Process.exit(repo_pid, :normal)
      Logger.configure(level: previous_level)
    end

    :ok
  end

  @spec ensure_clean_prefix!(String.t()) :: :ok
  defp ensure_clean_prefix!(prefix) do
    if prefix_ready?(prefix) do
      helper_call!(:truncate_prefix!, [prefix])
    else
      # `prepare_prefix!` uses Ecto.Migrator with a fixed version.
      # If that version is already marked as up from an earlier interrupted run,
      # `prepare_prefix!` may no-op for a fresh prefix. Force a cleanup/down first.
      helper_call!(:cleanup_prefix!, [prefix])
      helper_call!(:prepare_prefix!, [prefix])
    end

    :ok
  end

  @spec prefix_ready?(String.t()) :: boolean()
  defp prefix_ready?(prefix) do
    repo = helper_repo()

    sql = """
    SELECT
      to_regclass($1) IS NOT NULL AS executions_exists,
      to_regclass($2) IS NOT NULL AS events_exists
    """

    executions_table = "#{prefix}.endurant_executions"
    events_table = "#{prefix}.endurant_events"

    case repo.query!(sql, [executions_table, events_table], log: false).rows do
      [[true, true]] -> true
      _ -> false
    end
  end

  @spec insert_all!(module(), pos_integer(), keyword()) :: :ok
  defp insert_all!(workflow_module, count, runtime_opts) do
    Enum.each(1..count, fn id ->
      case Endurant.insert(workflow_module, %{id: id}, runtime_opts) do
        {:ok, _execution} -> :ok
        {:error, reason} -> Mix.raise("insert failed for id=#{id}: #{inspect(reason)}")
      end
    end)
  end

  @spec wait_for_completed!(pos_integer(), module(), keyword(), pos_integer()) :: :ok
  defp wait_for_completed!(expected_count, workflow_module, runtime_opts, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_completed(expected_count, inspect(workflow_module), runtime_opts, deadline)
  end

  @spec do_wait_for_completed(pos_integer(), String.t(), keyword(), integer()) :: :ok
  defp do_wait_for_completed(expected_count, workflow_name, runtime_opts, deadline) do
    completed = completed_count(workflow_name, runtime_opts)

    cond do
      completed >= expected_count ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Mix.raise(
          "timeout waiting for completion: expected=#{expected_count} completed=#{completed}"
        )

      true ->
        Process.sleep(100)
        do_wait_for_completed(expected_count, workflow_name, runtime_opts, deadline)
    end
  end

  @spec completed_count(String.t(), keyword()) :: non_neg_integer()
  defp completed_count(workflow_name, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")

    sql = """
    SELECT COUNT(*)
    FROM #{prefix}.endurant_executions e
    WHERE e.workflow_name = $1
    AND e.status = 'completed'::#{prefix}.endurant_execution_status
    """

    case repo.query!(sql, [workflow_name], log: false).rows do
      [[count]] when is_integer(count) -> count
      _ -> 0
    end
  end

  @spec completion_latencies_ms(module(), keyword()) :: [float()]
  defp completion_latencies_ms(workflow_module, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    workflow_name = inspect(workflow_module)

    sql = """
    SELECT EXTRACT(EPOCH FROM (completed.inserted_at - created.inserted_at)) * 1000.0 AS latency_ms
    FROM #{prefix}.endurant_executions e
    JOIN #{prefix}.endurant_events created
      ON created.execution_id = e.id
      AND created.type = 'execution_created'::#{prefix}.endurant_event_type
    JOIN #{prefix}.endurant_events completed
      ON completed.execution_id = e.id
      AND completed.type = 'execution_completed'::#{prefix}.endurant_event_type
    WHERE e.workflow_name = $1
    """

    repo.query!(sql, [workflow_name], log: false).rows
    |> Enum.map(fn [latency_ms] -> as_float(latency_ms) end)
    |> Enum.sort()
  end

  @spec service_latencies_ms(module(), keyword()) :: [float()]
  defp service_latencies_ms(workflow_module, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    workflow_name = inspect(workflow_module)

    sql = """
    SELECT EXTRACT(EPOCH FROM (completed.inserted_at - started.inserted_at)) * 1000.0 AS latency_ms
    FROM #{prefix}.endurant_executions e
    JOIN #{prefix}.endurant_events started
      ON started.execution_id = e.id
      AND started.type = 'execution_started'::#{prefix}.endurant_event_type
    JOIN #{prefix}.endurant_events completed
      ON completed.execution_id = e.id
      AND completed.type = 'execution_completed'::#{prefix}.endurant_event_type
    WHERE e.workflow_name = $1
    """

    repo.query!(sql, [workflow_name], log: false).rows
    |> Enum.map(fn [latency_ms] -> as_float(latency_ms) end)
    |> Enum.sort()
  end

  @spec as_float(term()) :: float()
  defp as_float(value) when is_float(value), do: value
  defp as_float(%Decimal{} = value), do: Decimal.to_float(value)
  defp as_float(value) when is_integer(value), do: value * 1.0
  defp as_float(_), do: 0.0

  @spec percentile([float()], pos_integer()) :: float()
  defp percentile([], _p), do: 0.0

  defp percentile(values, p) when is_list(values) and is_integer(p) and p > 0 do
    size = length(values)
    rank = max(ceil(size * p / 100), 1)
    Enum.at(values, rank - 1, 0.0)
  end

  @spec print_summary(pos_integer(), pos_integer(), pos_integer(), pos_integer(), [map()]) :: :ok
  defp print_summary(count, repeats, concurrency, poll_interval, results) do
    Mix.shell().info("")
    Mix.shell().info("Endurant General Performance Benchmark")

    Mix.shell().info(
      "count=#{count} repeats=#{repeats} concurrency=#{concurrency} poll=#{poll_interval}ms"
    )

    Mix.shell().info("")

    Mix.shell().info(
      [
        pad("run", 4),
        pad("duration_ms", 13),
        pad("throughput/s", 14),
        pad("e2e_p50_ms", 12),
        pad("e2e_p95_ms", 12),
        pad("e2e_p99_ms", 12),
        pad("svc_p50_ms", 12),
        pad("svc_p95_ms", 12),
        pad("svc_p99_ms", 12)
      ]
      |> Enum.join(" ")
    )

    Enum.each(results, fn row ->
      Mix.shell().info(
        [
          pad("#{row.run}", 4),
          pad("#{row.duration_ms}", 13),
          pad(format_float(row.throughput), 14),
          pad(format_float(row.p50_ms), 12),
          pad(format_float(row.p95_ms), 12),
          pad(format_float(row.p99_ms), 12),
          pad(format_float(row.svc_p50_ms), 12),
          pad(format_float(row.svc_p95_ms), 12),
          pad(format_float(row.svc_p99_ms), 12)
        ]
        |> Enum.join(" ")
      )
    end)

    Mix.shell().info("")

    Mix.shell().info(
      "median throughput/s=#{format_float(median(results, & &1.throughput))} " <>
        "median e2e_p95_ms=#{format_float(median(results, & &1.p95_ms))} " <>
        "median svc_p95_ms=#{format_float(median(results, & &1.svc_p95_ms))}"
    )
  end

  @spec median([map()], (map() -> float())) :: float()
  defp median([], _extractor), do: 0.0

  defp median(rows, extractor) do
    values =
      rows
      |> Enum.map(extractor)
      |> Enum.sort()

    Enum.at(values, div(length(values), 2), 0.0)
  end

  @spec format_float(number()) :: String.t()
  defp format_float(value) when is_number(value),
    do: :erlang.float_to_binary(value * 1.0, decimals: 2)

  @spec pad(String.t(), pos_integer()) :: String.t()
  defp pad(value, width) when is_binary(value), do: String.pad_trailing(value, width)

  @spec positive(term(), pos_integer()) :: pos_integer()
  defp positive(value, _default) when is_integer(value) and value > 0, do: value
  defp positive(_value, default), do: default

  @spec helper_module() :: module()
  defp helper_module, do: Module.concat([Endurant, TestSupport, PostgresHelper])

  @spec helper_repo() :: module()
  defp helper_repo, do: Module.concat([helper_module(), Repo])

  @spec helper_call!(atom(), [term()]) :: term()
  defp helper_call!(function, args), do: apply(helper_module(), function, args)
end

defmodule Endurant.Perf.GeneralWorkflow do
  @moduledoc false
  use Endurant.Workflow, version: "1"

  workflow do
    queue("perf")
    unique_id(fn input -> "perf-general:#{input.id}" end)

    @impl true
    @spec run(Endurant.Workflow.version(), Endurant.Workflow.input()) ::
            Endurant.Workflow.result()
    def run(_version, input) do
      task(nil, "step_1", fn _ ->
        input.id + 1
      end)
    end
  end
end
