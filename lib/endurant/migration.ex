defmodule Endurant.Migration do
  @moduledoc """
  Entry point for Endurant database migrations.

  Host applications should call this module from their own `Ecto.Migration` files.

  ## Options

  - `:version` - Target migration version. Defaults to latest for `up/1` and `1` for `down/1`.
  - `:prefix` - Postgres schema prefix. Defaults to `"public"`. Must match
    `^[A-Za-z_][A-Za-z0-9_]*$`, otherwise `ArgumentError` is raised.
  - `:create_schema` - Whether to create the prefix schema when missing. Defaults to
    `true` for non-`public` prefixes.
  - `:repo` - Repo module used for migration queries. Inferred from
    `Ecto.Migration.repo/0` when called inside a host migration. Required when called
    outside an active migration context.

  ## Example

      defmodule MyApp.Repo.Migrations.AddEndurant do
        use Ecto.Migration

        def up do
          Endurant.Migration.up(version: 1)
        end

        def down do
          Endurant.Migration.down(version: 1)
        end
      end
  """

  use Ecto.Migration
  alias Endurant.Migrations.Postgres

  @doc """
  Migrates storage up to the latest (or requested) version.

  Accepts all options listed in module docs.
  """
  @spec up(keyword()) :: :ok
  def up(opts \\ []) when is_list(opts) do
    Postgres.up(opts)
  end

  @doc """
  Migrates storage down to the requested version.

  Accepts all options listed in module docs.
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) when is_list(opts) do
    Postgres.down(opts)
  end

  @doc """
  Returns the currently migrated version.

  Accepts all options listed in module docs.
  """
  @spec migrated_version(keyword()) :: non_neg_integer()
  def migrated_version(opts \\ []) when is_list(opts) do
    Postgres.migrated_version(opts)
  end
end
