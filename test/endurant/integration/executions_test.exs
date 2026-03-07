defmodule Endurant.Integration.ExecutionsTest do
  use Endurant.TestSupport.IntegrationCase

  test "mark_started/3 requires matching active lease owner", %{runtime_opts: runtime_opts} do
    execution_id =
      insert_execution!(runtime_opts,
        status: "running",
        locked_by: "worker-a",
        lease_offset_seconds: 60
      )

    assert {:error, :lock_lost} =
             Endurant.Executions.mark_started(execution_id, "worker-b", runtime_opts)

    prefix = Keyword.fetch!(runtime_opts, :prefix)

    [[locked_by]] =
      PostgresHelper.Repo.query!(
        "SELECT locked_by FROM #{prefix}.endurant_executions WHERE id = $1",
        [to_db_id(execution_id)],
        log: false
      ).rows

    assert locked_by == "worker-a"
  end

  test "claim_pending/5 rolls back state change when execution_resumed append fails", %{
    runtime_opts: runtime_opts
  } do
    prefix = Keyword.fetch!(runtime_opts, :prefix)
    install_resumed_failure_trigger!(prefix)

    on_exit(fn ->
      drop_resumed_failure_trigger!(prefix)
    end)

    execution_id =
      insert_execution!(runtime_opts,
        status: "abandoned",
        locked_by: nil,
        lease_offset_seconds: nil
      )

    assert_raise Postgrex.Error, ~r/forced resumed insert failure/, fn ->
      Endurant.Executions.claim_pending(:manual, 1, "worker-a", 30_000, runtime_opts)
    end

    assert %{status: :abandoned} = Endurant.Executions.get(execution_id, runtime_opts)

    rows =
      PostgresHelper.Repo.query!(
        "SELECT locked_by, locked_until FROM #{prefix}.endurant_executions WHERE id = $1",
        [to_db_id(execution_id)],
        log: false
      ).rows

    assert [[nil, nil]] = rows
  end

  test "claim_ready_waiting/5 rolls back state change when execution_resumed append fails", %{
    runtime_opts: runtime_opts
  } do
    prefix = Keyword.fetch!(runtime_opts, :prefix)
    install_resumed_failure_trigger!(prefix)

    on_exit(fn ->
      drop_resumed_failure_trigger!(prefix)
    end)

    execution_id =
      insert_execution!(runtime_opts,
        status: "continuable",
        locked_by: nil,
        lease_offset_seconds: nil
      )

    assert :ok =
             Endurant.Events.append(
               execution_id,
               :execution_abandoned,
               %{abandoned_at: "now"},
               runtime_opts
             )

    assert_raise Postgrex.Error, ~r/forced resumed insert failure/, fn ->
      Endurant.Executions.claim_ready_waiting(:manual, 1, "worker-a", 30_000, runtime_opts)
    end

    assert %{status: :continuable} = Endurant.Executions.get(execution_id, runtime_opts)

    rows =
      PostgresHelper.Repo.query!(
        "SELECT locked_by, locked_until, waiting_until FROM #{prefix}.endurant_executions WHERE id = $1",
        [to_db_id(execution_id)],
        log: false
      ).rows

    assert [[nil, nil, nil]] = rows
  end

  test "release_waiting_as_abandoned_owned/3 uses previous locked_until as abandoned_at", %{
    runtime_opts: runtime_opts
  } do
    execution_id =
      insert_execution!(runtime_opts,
        status: "waiting",
        locked_by: "worker-a",
        lease_offset_seconds: 120
      )

    prefix = Keyword.fetch!(runtime_opts, :prefix)

    [[locked_until]] =
      PostgresHelper.Repo.query!(
        "SELECT locked_until FROM #{prefix}.endurant_executions WHERE id = $1",
        [to_db_id(execution_id)],
        log: false
      ).rows

    assert %NaiveDateTime{} = locked_until

    assert :ok =
             Endurant.Executions.release_waiting_as_abandoned_owned(
               execution_id,
               "worker-a",
               runtime_opts
             )

    events = Endurant.Events.list(execution_id, runtime_opts)
    abandoned = Enum.find(events, &(&1.type == :execution_abandoned))
    assert abandoned

    expected_abandoned_at =
      locked_until
      |> DateTime.from_naive!("Etc/UTC")
      |> DateTime.to_iso8601()

    payload_abandoned_at =
      abandoned.payload["abandoned_at"] || abandoned.payload[:abandoned_at]

    assert payload_abandoned_at == expected_abandoned_at
  end

  @spec install_resumed_failure_trigger!(String.t()) :: :ok
  defp install_resumed_failure_trigger!(prefix) do
    PostgresHelper.Repo.query!(
      """
      CREATE OR REPLACE FUNCTION #{prefix}.endurant_fail_resumed_insert()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        IF NEW.type = 'execution_resumed'::#{prefix}.endurant_event_type THEN
          RAISE EXCEPTION 'forced resumed insert failure';
        END IF;
        RETURN NEW;
      END;
      $$;
      """,
      [],
      log: false
    )

    PostgresHelper.Repo.query!(
      """
      CREATE TRIGGER endurant_fail_resumed_insert_trigger
      BEFORE INSERT ON #{prefix}.endurant_events
      FOR EACH ROW
      EXECUTE FUNCTION #{prefix}.endurant_fail_resumed_insert();
      """,
      [],
      log: false
    )

    :ok
  end

  @spec drop_resumed_failure_trigger!(String.t()) :: :ok
  defp drop_resumed_failure_trigger!(prefix) do
    _ =
      PostgresHelper.Repo.query!(
        "DROP TRIGGER IF EXISTS endurant_fail_resumed_insert_trigger ON #{prefix}.endurant_events",
        [],
        log: false
      )

    _ =
      PostgresHelper.Repo.query!(
        "DROP FUNCTION IF EXISTS #{prefix}.endurant_fail_resumed_insert()",
        [],
        log: false
      )

    :ok
  end

  @spec insert_execution!(keyword(), keyword()) :: binary()
  defp insert_execution!(opts, overrides) do
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.fetch!(opts, :prefix)
    execution_id = Ecto.UUID.generate()
    db_id = to_db_id(execution_id)
    unique_id = "executions:#{execution_id}"
    status = Keyword.get(overrides, :status, "pending")
    waiting_until_offset_seconds = Keyword.get(overrides, :waiting_until_offset_seconds)
    locked_by = Keyword.get(overrides, :locked_by)
    lease_offset_seconds = Keyword.get(overrides, :lease_offset_seconds, 60)
    now = DateTime.utc_now()
    waiting_until = maybe_waiting_until(now, waiting_until_offset_seconds)
    locked_until = maybe_locked_until(now, lease_offset_seconds)

    sql = """
    INSERT INTO #{prefix}.endurant_executions (
      id, unique_id, queue, workflow_name, version, input, status,
      waiting_until, locked_by, locked_until, completed_at, inserted_at, updated_at
    )
    VALUES (
      $1, $2, 'manual', 'ExecutionsTest.Workflow', '1', '{}'::jsonb, $3::#{prefix}.endurant_execution_status,
      $4, $5, $6, NULL, timezone('UTC', now()), timezone('UTC', now())
    )
    """

    repo.query!(sql, [db_id, unique_id, status, waiting_until, locked_by, locked_until],
      log: false
    )

    execution_id
  end

  @spec maybe_waiting_until(DateTime.t(), nil | integer()) :: nil | DateTime.t()
  defp maybe_waiting_until(_now, nil), do: nil

  defp maybe_waiting_until(now, seconds) when is_integer(seconds),
    do: DateTime.add(now, seconds, :second)

  @spec maybe_locked_until(DateTime.t(), nil | integer()) :: nil | DateTime.t()
  defp maybe_locked_until(_now, nil), do: nil

  defp maybe_locked_until(now, seconds) when is_integer(seconds),
    do: DateTime.add(now, seconds, :second)

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> id
    end
  end
end
