defmodule Endurant.Migrations.Postgres.V01 do
  @moduledoc false

  use Ecto.Migration

  @spec up(map()) :: :ok
  def up(%{
        create_schema: create?,
        prefix: prefix,
        quoted_prefix: quoted
      }) do
    if create?, do: execute("CREATE SCHEMA IF NOT EXISTS #{quoted}")

    unless type_exists?(prefix, "endurant_execution_status") do
      execute("""
      CREATE TYPE #{quoted}.endurant_execution_status AS ENUM (
        'pending',
        'running',
        'waiting',
        'continuable',
        'abandoned',
        'cancelling',
        'completed',
        'failed',
        'cancelled',
        'continued_as_new'
      );
      """)
    end

    unless type_exists?(prefix, "endurant_event_type") do
      execute("""
      CREATE TYPE #{quoted}.endurant_event_type AS ENUM (
        'execution_created',
        'execution_started',
        'execution_completed',
        'execution_failed',
        'execution_cancelled',
        'execution_abandoned',
        'execution_resumed',
        'execution_waiting',
        'execution_continued_as_new',
        'child_execution_started',
        'child_execution_completed',
        'child_execution_failed',
        'child_execution_cancelled',
        'task_started',
        'task_completed',
        'task_failed',
        'task_interrupted',
        'signal_received',
        'cancel_requested'
      );
      """)
    end

    unless type_exists?(prefix, "endurant_schedule_overlap_policy") do
      execute("""
      CREATE TYPE #{quoted}.endurant_schedule_overlap_policy AS ENUM (
        'skip'
      );
      """)
    end

    unless type_exists?(prefix, "endurant_scheduled_execution_status") do
      execute("""
      CREATE TYPE #{quoted}.endurant_scheduled_execution_status AS ENUM (
        'pending',
        'dispatched',
        'skipped',
        'failed',
        'cancelled'
      );
      """)
    end

    unless type_exists?(prefix, "endurant_cron_schedule_status") do
      execute("""
      CREATE TYPE #{quoted}.endurant_cron_schedule_status AS ENUM (
        'active',
        'paused'
      );
      """)
    end

    create_if_not_exists table(:endurant_executions, primary_key: false, prefix: prefix) do
      add(:id, :binary_id, primary_key: true)
      add(:unique_id, :text, null: false)
      add(:queue, :text, null: false)
      add(:workflow_name, :text, null: false)
      add(:version, :text, null: false, default: "1")
      add(:input, :map, null: false)
      add(:metadata, :map, null: false, default: %{})
      add(:status, :"#{quoted}.endurant_execution_status", null: false)
      add(:next_event_sequence, :bigint, null: false, default: 1)
      add(:history_size_bytes, :bigint, null: false, default: 0)
      add(:waiting_until, :utc_datetime_usec)
      add(:locked_by, :text)
      add(:locked_until, :utc_datetime_usec)
      add(:completed_at, :utc_datetime_usec)

      add(:inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )

      add(:updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )
    end

    execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS endurant_executions_one_open_unique_idx
    ON #{quoted}.endurant_executions (unique_id)
    WHERE status IN (
      'pending'::#{quoted}.endurant_execution_status,
      'running'::#{quoted}.endurant_execution_status,
      'waiting'::#{quoted}.endurant_execution_status,
      'continuable'::#{quoted}.endurant_execution_status,
      'abandoned'::#{quoted}.endurant_execution_status,
      'cancelling'::#{quoted}.endurant_execution_status
    )
    """)

    # Indexes for execution list/filter queries.
    create_if_not_exists(
      index(:endurant_executions, [:unique_id],
        name: :endurant_executions_unique_id_lookup_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:inserted_at, :id],
        name: :endurant_executions_recent_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:status, :inserted_at, :id],
        name: :endurant_executions_status_recent_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_queue_recent_idx,
        prefix: prefix
      )
    )

    # Runtime claim/recovery indexes used by queue manager internals.
    create_if_not_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_claim_pending_idx,
        where: "status IN ('pending', 'abandoned')",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_claim_continuable_idx,
        where: "status = 'continuable' AND locked_by IS NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :waiting_until, :inserted_at, :id],
        name: :endurant_executions_claim_waiting_ready_idx,
        where: "status = 'waiting' AND waiting_until IS NOT NULL AND locked_by IS NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_waiting_idx,
        where: "status = 'waiting' AND locked_until IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_running_idx,
        where: "status = 'running' AND locked_until IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_continuable_idx,
        where: "status = 'continuable' AND locked_by IS NOT NULL AND locked_until IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_cancelling_idx,
        where: "status = 'cancelling' AND locked_until IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_executions, [:completed_at, :id],
        name: :endurant_executions_archive_scan_idx,
        where:
          "status IN ('completed', 'failed', 'cancelled', 'continued_as_new') AND completed_at IS NOT NULL",
        prefix: prefix
      )
    )

    execute("""
    CREATE INDEX IF NOT EXISTS endurant_executions_child_parent_execution_idx
    ON #{quoted}.endurant_executions ((metadata->'child_workflow'->>'parent_execution_id'))
    WHERE metadata ? 'child_workflow'
      AND status IN (
        'pending'::#{quoted}.endurant_execution_status,
        'running'::#{quoted}.endurant_execution_status,
        'waiting'::#{quoted}.endurant_execution_status,
        'continuable'::#{quoted}.endurant_execution_status,
        'abandoned'::#{quoted}.endurant_execution_status,
        'cancelling'::#{quoted}.endurant_execution_status
      )
    """)

    execute("""
    CREATE INDEX IF NOT EXISTS endurant_executions_child_parent_lookup_idx
    ON #{quoted}.endurant_executions ((metadata->'child_workflow'->>'parent_execution_id'))
    WHERE metadata ? 'child_workflow'
    """)

    execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS endurant_executions_open_child_key_unique_idx
    ON #{quoted}.endurant_executions (
      (metadata->'child_workflow'->>'parent_execution_id'),
      (metadata->'child_workflow'->>'parent_child_key')
    )
    WHERE metadata ? 'child_workflow'
      AND status IN (
        'pending'::#{quoted}.endurant_execution_status,
        'running'::#{quoted}.endurant_execution_status,
        'waiting'::#{quoted}.endurant_execution_status,
        'continuable'::#{quoted}.endurant_execution_status,
        'abandoned'::#{quoted}.endurant_execution_status,
        'cancelling'::#{quoted}.endurant_execution_status
      )
    """)

    create_if_not_exists table(:endurant_scheduled_executions, primary_key: false, prefix: prefix) do
      add(:id, :binary_id, primary_key: true)
      add(:unique_id, :text, null: false)
      add(:queue, :text, null: false)
      add(:workflow_name, :text, null: false)
      add(:version, :text, null: false, default: "1")
      add(:input, :map, null: false)
      add(:scheduled_at, :utc_datetime_usec, null: false)
      add(:overlap_policy, :"#{quoted}.endurant_schedule_overlap_policy", null: false)
      add(:status, :"#{quoted}.endurant_scheduled_execution_status", null: false)
      add(:cron_schedule_id, :binary_id)

      add(
        :dispatched_execution_id,
        references(:endurant_executions,
          type: :binary_id,
          on_delete: :nilify_all,
          prefix: prefix
        )
      )

      add(:inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )

      add(:updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )
    end

    create_if_not_exists(
      index(:endurant_scheduled_executions, [:status, :scheduled_at, :id],
        name: :endurant_scheduled_executions_claim_due_idx,
        where: "status = 'pending'",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_scheduled_executions, [:unique_id, :status],
        name: :endurant_scheduled_executions_unique_status_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_scheduled_executions, [:dispatched_execution_id],
        name: :endurant_scheduled_executions_dispatched_execution_idx,
        where: "dispatched_execution_id IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      unique_index(:endurant_scheduled_executions, [:cron_schedule_id, :scheduled_at],
        name: :endurant_scheduled_executions_cron_scheduled_unique_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_scheduled_executions, [:cron_schedule_id, :scheduled_at, :id],
        name: :endurant_scheduled_executions_cron_recent_idx,
        where: "cron_schedule_id IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists table(:endurant_cron_schedules, primary_key: false, prefix: prefix) do
      add(:id, :binary_id, primary_key: true)
      add(:name, :text)
      add(:unique_id, :text, null: false)
      add(:queue, :text, null: false)
      add(:workflow_name, :text, null: false)
      add(:version, :text, null: false, default: "1")
      add(:input, :map, null: false)
      add(:cron_expr, :text, null: false)
      add(:timezone, :text, null: false, default: "Etc/UTC")
      add(:start_at, :utc_datetime_usec, null: false)
      add(:end_at, :utc_datetime_usec)
      add(:next_run_at, :utc_datetime_usec, null: false)
      add(:overlap_policy, :"#{quoted}.endurant_schedule_overlap_policy", null: false)
      add(:status, :"#{quoted}.endurant_cron_schedule_status", null: false)

      add(:inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )

      add(:updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )
    end

    create_if_not_exists(
      index(:endurant_cron_schedules, [:status, :next_run_at, :id],
        name: :endurant_cron_schedules_claim_due_idx,
        where: "status = 'active'",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_cron_schedules, [:name],
        unique: true,
        name: :endurant_cron_schedules_name_unique_idx,
        where: "name IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_cron_schedules, [:unique_id, :status],
        name: :endurant_cron_schedules_unique_status_idx,
        prefix: prefix
      )
    )

    create_if_not_exists table(:endurant_settings, primary_key: false, prefix: prefix) do
      add(:id, :text, primary_key: true)
      add(:value, :map, null: false, default: %{})

      add(:inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )

      add(:updated_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )
    end

    create_if_not_exists(
      index(:endurant_settings, [:updated_at],
        name: :endurant_settings_updated_at_idx,
        prefix: prefix
      )
    )

    execute("""
    CREATE INDEX IF NOT EXISTS endurant_settings_archiver_enabled_idx
    ON #{quoted}.endurant_settings ((value->>'archiver'), (value->>'enabled'))
    WHERE value ? 'archiver' AND value ? 'enabled'
    """)

    create_if_not_exists table(:endurant_archive_deliveries, primary_key: false, prefix: prefix) do
      add(:backend, :text, null: false)

      add(
        :execution_id,
        references(:endurant_executions,
          type: :binary_id,
          on_delete: :delete_all,
          prefix: prefix
        ),
        null: false
      )
    end

    create_if_not_exists(
      unique_index(:endurant_archive_deliveries, [:backend, :execution_id],
        name: :endurant_archive_deliveries_backend_execution_unique_idx,
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:endurant_archive_deliveries, [:execution_id],
        name: :endurant_archive_deliveries_execution_idx,
        prefix: prefix
      )
    )

    create_if_not_exists table(:endurant_events, primary_key: false, prefix: prefix) do
      add(:id, :bigserial, primary_key: true)

      add(
        :execution_id,
        references(:endurant_executions,
          type: :binary_id,
          on_delete: :delete_all,
          prefix: prefix
        ),
        null: false
      )

      add(:sequence, :integer, null: false)
      add(:type, :"#{quoted}.endurant_event_type", null: false)
      add(:payload, :map, null: false, default: %{})

      add(:inserted_at, :utc_datetime_usec,
        null: false,
        default: fragment("timezone('UTC', now())")
      )
    end

    create_if_not_exists(
      unique_index(:endurant_events, [:execution_id, :sequence], prefix: prefix)
    )

    :ok
  end

  @spec down(map()) :: :ok
  def down(%{prefix: prefix, quoted_prefix: quoted}) do
    drop_if_exists(index(:endurant_events, [:execution_id, :sequence], prefix: prefix))
    drop_if_exists(table(:endurant_events, prefix: prefix))

    execute("DROP INDEX IF EXISTS #{quoted}.endurant_executions_open_child_key_unique_idx")
    execute("DROP INDEX IF EXISTS #{quoted}.endurant_executions_child_parent_lookup_idx")
    execute("DROP INDEX IF EXISTS #{quoted}.endurant_executions_child_parent_execution_idx")

    drop_if_exists(
      index(:endurant_archive_deliveries, [:execution_id],
        name: :endurant_archive_deliveries_execution_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_archive_deliveries, [:backend, :execution_id],
        name: :endurant_archive_deliveries_backend_execution_unique_idx,
        prefix: prefix
      )
    )

    drop_if_exists(table(:endurant_archive_deliveries, prefix: prefix))

    drop_if_exists(
      index(:endurant_cron_schedules, [:unique_id, :status],
        name: :endurant_cron_schedules_unique_status_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_cron_schedules, [:name],
        name: :endurant_cron_schedules_name_unique_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_cron_schedules, [:status, :next_run_at, :id],
        name: :endurant_cron_schedules_claim_due_idx,
        prefix: prefix
      )
    )

    drop_if_exists(table(:endurant_cron_schedules, prefix: prefix))

    drop_if_exists(
      index(:endurant_scheduled_executions, [:cron_schedule_id, :scheduled_at, :id],
        name: :endurant_scheduled_executions_cron_recent_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_scheduled_executions, [:cron_schedule_id, :scheduled_at],
        name: :endurant_scheduled_executions_cron_scheduled_unique_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_scheduled_executions, [:dispatched_execution_id],
        name: :endurant_scheduled_executions_dispatched_execution_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_scheduled_executions, [:unique_id, :status],
        name: :endurant_scheduled_executions_unique_status_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_scheduled_executions, [:status, :scheduled_at, :id],
        name: :endurant_scheduled_executions_claim_due_idx,
        prefix: prefix
      )
    )

    drop_if_exists(table(:endurant_scheduled_executions, prefix: prefix))

    drop_if_exists(
      index(:endurant_settings, [:updated_at],
        name: :endurant_settings_updated_at_idx,
        prefix: prefix
      )
    )

    execute("DROP INDEX IF EXISTS #{quoted}.endurant_settings_archiver_enabled_idx")

    drop_if_exists(table(:endurant_settings, prefix: prefix))

    drop_if_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_cancelling_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:completed_at, :id],
        name: :endurant_executions_archive_scan_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_continuable_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_running_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_waiting_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :waiting_until, :inserted_at, :id],
        name: :endurant_executions_claim_waiting_ready_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_claim_continuable_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_claim_pending_idx,
        prefix: prefix
      )
    )

    # Drop list/filter query indexes.
    drop_if_exists(
      index(:endurant_executions, [:queue, :inserted_at, :id],
        name: :endurant_executions_queue_recent_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:status, :inserted_at, :id],
        name: :endurant_executions_status_recent_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:inserted_at, :id],
        name: :endurant_executions_recent_idx,
        prefix: prefix
      )
    )

    drop_if_exists(
      index(:endurant_executions, [:unique_id],
        name: :endurant_executions_unique_id_lookup_idx,
        prefix: prefix
      )
    )

    execute("DROP INDEX IF EXISTS #{quoted}.endurant_executions_one_open_unique_idx")
    drop_if_exists(table(:endurant_executions, prefix: prefix))
    execute("DROP TYPE IF EXISTS #{quoted}.endurant_cron_schedule_status")
    execute("DROP TYPE IF EXISTS #{quoted}.endurant_scheduled_execution_status")
    execute("DROP TYPE IF EXISTS #{quoted}.endurant_schedule_overlap_policy")
    execute("DROP TYPE IF EXISTS #{quoted}.endurant_event_type")
    execute("DROP TYPE IF EXISTS #{quoted}.endurant_execution_status")

    :ok
  end

  @spec type_exists?(String.t(), String.t()) :: boolean()
  defp type_exists?(prefix, type_name) do
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM pg_type t
      JOIN pg_namespace n ON n.oid = t.typnamespace
      WHERE t.typname = $1
      AND n.nspname = $2
    )
    """

    case repo().query!(sql, [type_name, prefix], log: false).rows do
      [[exists?]] when is_boolean(exists?) -> exists?
      _ -> false
    end
  end
end
