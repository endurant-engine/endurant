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

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end
end
