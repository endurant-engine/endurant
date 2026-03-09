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
        'cancelled'
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
        'task_started',
        'task_completed',
        'task_failed',
        'signal_received',
        'cancel_requested'
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
      add(:status, :"#{quoted}.endurant_execution_status", null: false)
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

    drop_if_exists(
      index(:endurant_executions, [:queue, :locked_until, :id],
        name: :endurant_executions_recover_cancelling_idx,
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

    execute("DROP INDEX IF EXISTS #{quoted}.endurant_executions_one_open_unique_idx")
    drop_if_exists(table(:endurant_executions, prefix: prefix))
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
