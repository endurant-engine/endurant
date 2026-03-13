defmodule Endurant.Integration.EventsTest do
  use Endurant.TestSupport.IntegrationCase

  test "append/4 appends sequential events for an execution", %{runtime_opts: runtime_opts} do
    execution_id = insert_execution!(runtime_opts)

    assert :ok = Endurant.Events.append(execution_id, :task_started, %{task: "one"}, runtime_opts)

    assert :ok =
             Endurant.Events.append(execution_id, :task_completed, %{task: "one"}, runtime_opts)

    events = Endurant.Events.list(execution_id, runtime_opts)
    assert Enum.map(events, & &1.sequence) == [1, 2]
    assert Enum.map(events, & &1.type) == [:task_started, :task_completed]
  end

  test "append/4 raises for unknown execution id", %{runtime_opts: runtime_opts} do
    assert_raise ArgumentError, ~r/execution not found/, fn ->
      Endurant.Events.append(Ecto.UUID.generate(), :task_started, %{}, runtime_opts)
    end
  end

  test "append_if_running_owned/5 succeeds with matching lease", %{runtime_opts: runtime_opts} do
    execution_id = insert_execution!(runtime_opts, status: "running", locked_by: "worker-1")

    assert :ok =
             Endurant.Events.append_if_running_owned(
               execution_id,
               "worker-1",
               :task_started,
               %{task: "a"},
               runtime_opts
             )
  end

  test "append_if_running_owned/5 returns not_running for wrong worker", %{
    runtime_opts: runtime_opts
  } do
    execution_id = insert_execution!(runtime_opts, status: "running", locked_by: "worker-1")

    assert {:error, :not_running} =
             Endurant.Events.append_if_running_owned(
               execution_id,
               "worker-2",
               :task_started,
               %{},
               runtime_opts
             )
  end

  test "append_if_running_owned/5 returns not_running for expired lease", %{
    runtime_opts: runtime_opts
  } do
    execution_id =
      insert_execution!(runtime_opts,
        status: "running",
        locked_by: "worker-1",
        lease_offset_seconds: -1
      )

    assert {:error, :not_running} =
             Endurant.Events.append_if_running_owned(
               execution_id,
               "worker-1",
               :task_started,
               %{},
               runtime_opts
             )
  end

  test "append_if_running_owned/5 returns not_running for non-running states", %{
    runtime_opts: runtime_opts
  } do
    for status <- ["pending", "waiting", "cancelled"] do
      execution_id = insert_execution!(runtime_opts, status: status)

      assert {:error, :not_running} =
               Endurant.Events.append_if_running_owned(
                 execution_id,
                 "worker-1",
                 :task_started,
                 %{},
                 runtime_opts
               )
    end
  end

  test "list/2 returns full history ordered by sequence with atom types", %{
    runtime_opts: runtime_opts
  } do
    execution_id = insert_execution!(runtime_opts)

    :ok = Endurant.Events.append(execution_id, :signal_received, %{signal: "go-1"}, runtime_opts)

    :ok =
      Endurant.Events.append(execution_id, :execution_waiting, %{signal: "go-2"}, runtime_opts)

    :ok = Endurant.Events.append(execution_id, :task_completed, %{task: "t"}, runtime_opts)

    events = Endurant.Events.list(execution_id, runtime_opts)

    assert Enum.map(events, & &1.sequence) == [1, 2, 3]
    assert Enum.map(events, & &1.type) == [:signal_received, :execution_waiting, :task_completed]
  end

  test "list/2 parses task_interrupted events with payload", %{runtime_opts: runtime_opts} do
    execution_id = insert_execution!(runtime_opts)

    :ok =
      Endurant.Events.append(
        execution_id,
        :task_interrupted,
        %{task: "t", task_run_id: "run-1"},
        runtime_opts
      )

    assert [%{type: :task_interrupted, payload: %{"task" => "t", "task_run_id" => "run-1"}}] =
             Endurant.Events.list(execution_id, runtime_opts)
  end

  test "list_after/3 returns only events with sequence greater than checkpoint", %{
    runtime_opts: runtime_opts
  } do
    execution_id = insert_execution!(runtime_opts)

    :ok = Endurant.Events.append(execution_id, :signal_received, %{signal: "s1"}, runtime_opts)
    :ok = Endurant.Events.append(execution_id, :signal_received, %{signal: "s2"}, runtime_opts)
    :ok = Endurant.Events.append(execution_id, :signal_received, %{signal: "s3"}, runtime_opts)

    events = Endurant.Events.list_after(execution_id, 1, runtime_opts)
    assert Enum.map(events, & &1.sequence) == [2, 3]
  end

  test "list/2 returns execution_id in app uuid format", %{runtime_opts: runtime_opts} do
    execution_id = insert_execution!(runtime_opts)
    :ok = Endurant.Events.append(execution_id, :execution_started, %{}, runtime_opts)

    events = Endurant.Events.list(execution_id, runtime_opts)
    assert Enum.all?(events, &(&1.execution_id == execution_id))
  end

  test "concurrent appends keep contiguous sequence numbers", %{runtime_opts: runtime_opts} do
    execution_id = insert_execution!(runtime_opts)

    1..10
    |> Task.async_stream(
      fn index ->
        Endurant.Events.append(execution_id, :signal_received, %{index: index}, runtime_opts)
      end,
      max_concurrency: 2,
      timeout: 5_000
    )
    |> Enum.each(fn
      {:ok, :ok} -> :ok
      other -> flunk("unexpected append result: #{inspect(other)}")
    end)

    events = Endurant.Events.list(execution_id, runtime_opts)
    sequences = Enum.map(events, & &1.sequence)

    assert length(events) == 10
    assert sequences == Enum.to_list(1..10)
    assert MapSet.size(MapSet.new(sequences)) == 10
  end

  test "append allocates from next_event_sequence and advances counters", %{
    runtime_opts: runtime_opts
  } do
    execution_id = insert_execution!(runtime_opts)
    set_execution_counters!(execution_id, 5, 10, runtime_opts)

    assert :ok = Endurant.Events.append(execution_id, :task_started, %{task: "s"}, runtime_opts)

    events = Endurant.Events.list(execution_id, runtime_opts)
    assert Enum.map(events, & &1.sequence) == [5]

    assert %{next_event_sequence: 6, history_size_bytes: history_size_bytes} =
             execution_counters!(execution_id, runtime_opts)

    assert history_size_bytes > 10
  end

  @spec insert_execution!(keyword(), keyword()) :: binary()
  defp insert_execution!(opts, overrides \\ []) do
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.fetch!(opts, :prefix)
    execution_id = Ecto.UUID.generate()
    db_id = to_db_id(execution_id)
    unique_id = "events:#{execution_id}"
    status = Keyword.get(overrides, :status, "pending")
    locked_by = Keyword.get(overrides, :locked_by)
    lease_offset_seconds = Keyword.get(overrides, :lease_offset_seconds, 60)
    now = DateTime.utc_now()
    locked_until = DateTime.add(now, lease_offset_seconds, :second)

    sql = """
    INSERT INTO #{prefix}.endurant_executions (
      id, unique_id, queue, workflow_name, version, input, status,
      waiting_until, locked_by, locked_until, completed_at, inserted_at, updated_at
    )
    VALUES (
      $1, $2, 'manual', 'EventsTest.Workflow', '1', '{}'::jsonb, $3::#{prefix}.endurant_execution_status,
      NULL, $4, $5, NULL, timezone('UTC', now()), timezone('UTC', now())
    )
    """

    repo.query!(sql, [db_id, unique_id, status, locked_by, locked_until], log: false)
    execution_id
  end

  @spec set_execution_counters!(binary(), pos_integer(), non_neg_integer(), keyword()) :: :ok
  defp set_execution_counters!(execution_id, next_event_sequence, history_size_bytes, opts)
       when is_integer(next_event_sequence) and next_event_sequence >= 1 and
              is_integer(history_size_bytes) and history_size_bytes >= 0 do
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.fetch!(opts, :prefix)

    sql = """
    UPDATE #{prefix}.endurant_executions
    SET next_event_sequence = $2,
        history_size_bytes = $3
    WHERE id = $1
    """

    repo.query!(sql, [to_db_id(execution_id), next_event_sequence, history_size_bytes],
      log: false
    )

    :ok
  end

  @spec execution_counters!(binary(), keyword()) :: %{
          next_event_sequence: pos_integer(),
          history_size_bytes: non_neg_integer()
        }
  defp execution_counters!(execution_id, opts) do
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.fetch!(opts, :prefix)

    sql = """
    SELECT next_event_sequence, history_size_bytes
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    LIMIT 1
    """

    case repo.query!(sql, [to_db_id(execution_id)], log: false).rows do
      [[next_event_sequence, history_size_bytes]] ->
        %{
          next_event_sequence: next_event_sequence,
          history_size_bytes: history_size_bytes
        }

      _ ->
        flunk("execution counters not found for #{inspect(execution_id)}")
    end
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end
end
