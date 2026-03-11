defmodule Endurant.Migrations.PostgresIntegrationTest do
  use ExUnit.Case, async: false

  alias Endurant.Migrations.Postgres

  @base_version 20_260_000_000_000
  @prefix "migrating"

  defmodule MigrationRepo do
    use Ecto.Repo,
      otp_app: :endurant,
      adapter: Ecto.Adapters.Postgres

    def init(_, config) do
      db_config = Application.fetch_env!(:endurant, :postgres)

      {:ok,
       Keyword.merge(config,
         username: Keyword.fetch!(db_config, :username),
         password: Keyword.fetch!(db_config, :password),
         hostname: Keyword.fetch!(db_config, :hostname),
         database: Keyword.fetch!(db_config, :database),
         pool_size: 5,
         pool: Ecto.Adapters.SQL.Sandbox
       )}
    end
  end

  defmodule StepMigration do
    use Ecto.Migration

    @prefix "migrating"

    def up do
      Endurant.Migration.up(version: up_version(), prefix: @prefix)
    end

    def down do
      Endurant.Migration.down(version: down_version(), prefix: @prefix)
    end

    defp up_version do
      Application.fetch_env!(:endurant, :up_version)
    end

    defp down_version do
      Application.fetch_env!(:endurant, :down_version)
    end
  end

  setup_all do
    Application.put_env(:endurant, MigrationRepo, [])

    case MigrationRepo.__adapter__().storage_up(MigrationRepo.config()) do
      :ok -> :ok
      {:error, :already_up} -> :ok
    end

    {:ok, _pid} = start_supervised(MigrationRepo)

    on_exit(fn ->
      case MigrationRepo.__adapter__().storage_down(MigrationRepo.config()) do
        :ok -> :ok
        {:error, :already_down} -> :ok
      end
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      clear_migrated()
      Application.delete_env(:endurant, :up_version)
      Application.delete_env(:endurant, :down_version)
    end)

    :ok
  end

  test "migrating up and down between specific versions" do
    for up <- Postgres.initial_version()..Postgres.current_version() do
      Application.put_env(:endurant, :up_version, up)

      assert :ok = Ecto.Migrator.up(MigrationRepo, @base_version + up, StepMigration)
      assert migrated_version() == up
    end

    assert table_exists?("endurant_executions")
    assert table_exists?("endurant_events")
    assert table_exists?("endurant_settings")
    assert table_exists?("endurant_scheduled_executions")
    assert table_exists?("endurant_cron_schedules")

    Application.put_env(:endurant, :down_version, 1)
    assert :ok = Ecto.Migrator.down(MigrationRepo, @base_version + 1, StepMigration)

    refute table_exists?("endurant_executions")
    refute table_exists?("endurant_events")
    refute table_exists?("endurant_settings")
    refute table_exists?("endurant_scheduled_executions")
    refute table_exists?("endurant_cron_schedules")
  end

  defp migrated_version do
    Postgres.migrated_version(repo: MigrationRepo, prefix: @prefix)
  end

  defp table_exists?(table) do
    query = """
    SELECT EXISTS (
      SELECT 1
      FROM pg_tables
      WHERE schemaname = '#{@prefix}'
      AND tablename = '#{table}'
    )
    """

    {:ok, %{rows: [[exists]]}} = MigrationRepo.query(query)
    exists
  end

  defp clear_migrated do
    MigrationRepo.query("DELETE FROM schema_migrations WHERE version >= #{@base_version}")
    MigrationRepo.query("DROP SCHEMA IF EXISTS #{@prefix} CASCADE")
  end
end
