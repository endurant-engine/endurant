defmodule Mix.Tasks.Perf.Waiting do
  @moduledoc """
  Measures runtime performance as DB waiting cardinality grows.

  Metrics per step:
  - signal->resume latency (p50/p95/p99)
  - BEAM process count
  - BEAM memory
  - parked waiter count
  - DB waiting row count

  Run in test env:

      MIX_ENV=test mix perf.waiting
  """
  use Mix.Task
  require Logger

  @shortdoc "Run waiting-cardinality performance benchmark"

  @switches [
    steps: :integer,
    batch: :integer,
    limit: :integer,
    parked_limit: :integer,
    poll: :integer,
    lease: :integer,
    signal_sample: :integer,
    insert_concurrency: :integer,
    progress_every: :integer
  ]

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    if Mix.env() != :test do
      Mix.raise("perf.waiting must run with MIX_ENV=test")
    end

    _ = Mix.Task.run("app.start")
    previous_level = Logger.level()
    Logger.configure(level: :info)

    {opts, _, _} = OptionParser.parse(args, strict: @switches)
    steps = positive(Keyword.get(opts, :steps, 5), 5)
    batch = positive(Keyword.get(opts, :batch, 10_000), 10_000)
    limit = positive(Keyword.get(opts, :limit, 8), 8)
    parked_limit = non_negative(Keyword.get(opts, :parked_limit, 0), 0)
    poll_interval = positive(Keyword.get(opts, :poll, 50), 50)
    lease_ms = positive(Keyword.get(opts, :lease, 30_000), 30_000)
    signal_sample = positive(Keyword.get(opts, :signal_sample, 20), 20)
    insert_concurrency = positive(Keyword.get(opts, :insert_concurrency, 32), 32)
    progress_every = positive(Keyword.get(opts, :progress_every, 5_000), 5_000)
    prefix = "perf_waiting_#{System.system_time(:millisecond)}"

    helper_call!(:start_repo!, [])
    {:ok, repo_pid} = helper_repo().start_link()

    engine_name = "perf_waiting"

    try do
      ensure_clean_prefix!(prefix)
      runtime_opts = helper_call!(:runtime_opts, [prefix])

      {:ok, supervisor_pid} =
        Endurant.start_link(
          name: engine_name,
          queues: [
            perf:
              [
                limit: limit,
                parked_limit: parked_limit,
                poll_interval: poll_interval,
                lease_ms: lease_ms
              ] ++ runtime_opts
          ]
        )

      try do
        print_header(
          steps,
          batch,
          limit,
          parked_limit,
          poll_interval,
          signal_sample,
          insert_concurrency
        )

        run_steps(
          steps,
          batch,
          signal_sample,
          engine_name,
          runtime_opts,
          insert_concurrency,
          progress_every
        )
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
          module(),
          keyword(),
          pos_integer(),
          pos_integer()
        ) :: :ok
  defp run_steps(
         steps,
         batch,
         signal_sample,
         engine_name,
         runtime_opts,
         insert_concurrency,
         progress_every
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
            progress_every
          )

          waiting_rows = waiting_count(runtime_opts)
          sampled = min(signal_sample, waiting_rows)
          signal_latencies = measure_signal_resume_latencies(sampled, runtime_opts)

          row = %{
            step: step,
            waiting_rows: waiting_rows,
            processes: :erlang.system_info(:process_count),
            memory_mb: :erlang.memory(:total) / 1_048_576.0,
            parked_count: parked_count(engine_name, :perf),
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
            waiting_target: max(next_waiting_target - sampled, 0),
            printed_columns?: printed_columns?
          }
        end
      )

    :ok
  end

  @spec insert_batch!(pos_integer(), pos_integer(), keyword(), pos_integer(), pos_integer()) ::
          :ok
  defp insert_batch!(from_id, to_id, runtime_opts, insert_concurrency, _progress_every)
       when from_id <= to_id do
    _ = insert_concurrency
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    chunk_size = 1_000

    seed_rows =
      Enum.map(from_id..to_id, fn id ->
        execution_id = Ecto.UUID.generate()
        {:ok, execution_db_id} = Ecto.UUID.dump(execution_id)

        %{
          id: execution_db_id,
          unique_id: "perf-waiting:#{id}",
          queue: "perf",
          workflow_name: "Endurant.Perf.WaitingWorkflow",
          version: "1",
          input: %{id: id}
        }
      end)

    _processed =
      seed_rows
      |> Enum.chunk_every(chunk_size)
      |> Enum.reduce(0, fn
        chunk, processed ->
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
        offset = idx * 6

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
            ", 'waiting'::" <>
            prefix <>
            ".endurant_execution_status, timezone('UTC', now()), timezone('UTC', now()))"

        params_row = [row.input, row.version, row.workflow_name, row.queue, row.unique_id, row.id]
        {[value_sql | values_acc], params_row ++ params_acc}
      end)

    values_sql = Enum.reverse(values_sql_rev)
    params = Enum.reverse(params_rev)

    sql = """
    INSERT INTO #{prefix}.endurant_executions
      (id, unique_id, queue, workflow_name, version, input, status, inserted_at, updated_at)
    VALUES #{Enum.join(values_sql, ",")}
    ON CONFLICT DO NOTHING
    """

    _ = repo.query!(sql, params, log: false)
    :ok
  end

  @spec insert_seed_events!(module(), String.t(), [map()]) :: :ok
  defp insert_seed_events!(repo, prefix, rows) do
    {values_sql_rev, params_rev} =
      rows
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {row, idx}, {values_acc, params_acc} ->
        # 8 params per execution (2 events):
        # 1: exec_id, 2: seq1, 3:type1, 4:payload1, 5:exec_id, 6:seq2, 7:type2, 8:payload2
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
            "::" <>
            prefix <>
            ".endurant_event_type, $" <>
            Integer.to_string(offset + 8) <>
            ")"

        created_payload = %{
          workflow: row.workflow_name,
          unique_id: row.unique_id,
          version: row.version
        }

        waiting_payload = %{
          mode: :signal,
          signal: "go"
        }

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

    sql = """
    INSERT INTO #{prefix}.endurant_events
      (execution_id, sequence, type, payload)
    VALUES #{Enum.join(values_sql, ",")}
    ON CONFLICT DO NOTHING
    """

    _ = repo.query!(sql, params, log: false)
    :ok
  end

  @spec waiting_count(keyword()) :: non_neg_integer()
  defp waiting_count(runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")

    sql = """
    SELECT COUNT(*)
    FROM #{prefix}.endurant_executions
    WHERE status = 'waiting'::#{prefix}.endurant_execution_status
    """

    case repo.query!(sql, [], log: false).rows do
      [[count]] when is_integer(count) -> count
      _ -> 0
    end
  end

  @spec parked_count(String.t(), atom()) :: non_neg_integer()
  defp parked_count(engine_name, queue) do
    queue_name = Endurant.Supervisor.queue_manager_name(engine_name, queue)

    case :sys.get_state(queue_name) do
      %{parked: parked} when is_map(parked) -> map_size(parked)
      _ -> 0
    end
  end

  @spec measure_signal_resume_latencies(pos_integer(), keyword()) :: [float()]
  defp measure_signal_resume_latencies(0, _runtime_opts), do: []

  @spec measure_signal_resume_latencies(pos_integer(), keyword()) :: [float()]
  defp measure_signal_resume_latencies(sample_size, runtime_opts) do
    ids = waiting_ids(sample_size, runtime_opts)

    Enum.map(ids, fn id ->
      :ok = Endurant.signal(id, :go, %{bench: true}, runtime_opts)
      {signal_seq, signal_at} = last_signal_event!(id, runtime_opts)
      latency = wait_resume_started_latency!(id, signal_seq, signal_at, runtime_opts, 30_000)

      latency
    end)
  end

  @spec waiting_ids(pos_integer(), keyword()) :: [binary()]
  defp waiting_ids(limit, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")

    sql = """
    SELECT id
    FROM #{prefix}.endurant_executions
    WHERE status = 'waiting'::#{prefix}.endurant_execution_status
    ORDER BY inserted_at ASC
    LIMIT $1
    """

    repo.query!(sql, [limit], log: false).rows
    |> Enum.map(fn [id] -> to_app_id(id) end)
  end

  @spec last_signal_event!(binary(), keyword()) :: {non_neg_integer(), NaiveDateTime.t()}
  defp last_signal_event!(execution_id, runtime_opts) do
    repo = Keyword.fetch!(runtime_opts, :repo)
    prefix = Keyword.get(runtime_opts, :prefix, "public")
    db_id = to_db_id(execution_id)

    sql = """
    SELECT sequence, inserted_at
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type = 'signal_received'::#{prefix}.endurant_event_type
    ORDER BY sequence DESC
    LIMIT 1
    """

    case repo.query!(sql, [db_id], log: false).rows do
      [[seq, ts]] when is_integer(seq) and is_struct(ts, NaiveDateTime) -> {seq, ts}
      other -> Mix.raise("missing signal event for #{execution_id}: #{inspect(other)}")
    end
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

    sql = """
    SELECT inserted_at
    FROM #{prefix}.endurant_events
    WHERE execution_id = $1
    AND type = 'execution_started'::#{prefix}.endurant_event_type
    AND sequence > $2
    ORDER BY sequence ASC
    LIMIT 1
    """

    case repo.query!(sql, [db_id, signal_seq], log: false).rows do
      [[started_at]] when is_struct(started_at, NaiveDateTime) ->
        NaiveDateTime.diff(started_at, signal_at, :microsecond) / 1_000.0

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
          pos_integer()
        ) ::
          :ok
  defp print_header(
         steps,
         batch,
         limit,
         parked_limit,
         poll_interval,
         signal_sample,
         insert_concurrency
       ) do
    Mix.shell().info("")
    Mix.shell().info("Endurant Waiting Cardinality Benchmark")

    Mix.shell().info(
      "steps=#{steps} batch=#{batch} limit=#{limit} parked_limit=#{parked_limit} " <>
        "poll=#{poll_interval}ms signal_sample=#{signal_sample} insert_concurrency=#{insert_concurrency}"
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
        pad("parked", 8),
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
        pad("#{row.parked_count}", 8),
        pad(fmt(row.resume_p95), 11),
        pad(fmt(row.resume_p99), 11)
      ]
      |> Enum.join(" ")
    )
  end

  @spec percentile([float()], pos_integer()) :: float()
  defp percentile([], _p), do: 0.0

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    rank = max(ceil(length(sorted) * p / 100), 1)
    Enum.at(sorted, rank - 1, 0.0)
  end

  @spec fmt(number()) :: String.t()
  defp fmt(value) when is_number(value), do: :erlang.float_to_binary(value * 1.0, decimals: 2)

  @spec pad(String.t(), pos_integer()) :: String.t()
  defp pad(value, width), do: String.pad_trailing(value, width)

  @spec positive(term(), pos_integer()) :: pos_integer()
  defp positive(value, _default) when is_integer(value) and value > 0, do: value
  defp positive(_value, default), do: default

  @spec non_negative(term(), non_neg_integer()) :: non_neg_integer()
  defp non_negative(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative(_value, default), do: default

  @spec helper_module() :: module()
  defp helper_module, do: Module.concat([Endurant, TestSupport, PostgresHelper])

  @spec helper_repo() :: module()
  defp helper_repo, do: Module.concat([helper_module(), Repo])

  @spec helper_call!(atom(), [term()]) :: term()
  defp helper_call!(function, args), do: apply(helper_module(), function, args)

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

defmodule Endurant.Perf.WaitingWorkflow do
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
      %{id: input.id, done: true}
    end
  end
end
