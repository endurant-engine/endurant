defmodule Endurant.Migrations.Postgres do
  @moduledoc false

  use Ecto.Migration

  @initial_version 1
  @current_version 1
  @default_prefix "public"
  @version_table "endurant_executions"

  @spec initial_version() :: pos_integer()
  def initial_version, do: @initial_version

  @spec current_version() :: pos_integer()
  def current_version, do: @current_version

  @spec up(keyword()) :: :ok
  def up(opts) do
    opts = with_defaults(opts, @current_version)
    initial = migrated_version(opts)

    cond do
      initial == 0 ->
        change(@initial_version..opts.version, :up, opts)

      initial < opts.version ->
        change((initial + 1)..opts.version, :up, opts)

      true ->
        :ok
    end
  end

  @spec down(keyword()) :: :ok
  def down(opts) do
    opts = with_defaults(opts, @initial_version)
    initial = max(migrated_version(opts), @initial_version)

    if initial >= opts.version do
      change(initial..opts.version//-1, :down, opts)
    else
      :ok
    end
  end

  @spec migrated_version(keyword() | map()) :: non_neg_integer()
  def migrated_version(opts) do
    opts = with_defaults(opts, @initial_version)

    repo = Map.get_lazy(opts, :repo, fn -> repo() end)

    query = """
    SELECT pg_catalog.obj_description(c.oid, 'pg_class')
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = '#{@version_table}'
    AND n.nspname = $1
    """

    case repo.query(query, [opts.prefix], log: false) do
      {:ok, %{rows: [[version]]}} when is_binary(version) ->
        parse_version(version)

      _ ->
        0
    end
  end

  @spec parse_version(String.t()) :: non_neg_integer()
  defp parse_version(version) do
    case Integer.parse(version) do
      {value, ""} -> value
      _ -> 0
    end
  end

  @spec change(Range.t(), :up | :down, map()) :: :ok
  defp change(range, direction, opts) do
    Enum.each(range, fn index ->
      pad_idx = String.pad_leading(to_string(index), 2, "0")

      [Endurant.Migrations.Postgres, "V#{pad_idx}"]
      |> Module.safe_concat()
      |> apply(direction, [opts])
    end)

    case direction do
      :up -> record_version(opts, Enum.max(range))
      :down -> record_version(opts, Enum.min(range) - 1)
    end

    :ok
  end

  @spec record_version(map(), non_neg_integer()) :: :ok
  defp record_version(_opts, 0), do: :ok

  defp record_version(%{prefix: prefix}, version) do
    execute("COMMENT ON TABLE #{inspect(prefix)}.#{@version_table} IS '#{version}'")
    :ok
  end

  @spec with_defaults(keyword() | map(), pos_integer()) :: map()
  defp with_defaults(opts, version) do
    opts = Enum.into(opts, %{prefix: @default_prefix, version: version})
    validated_prefix = validate_prefix!(opts.prefix)
    validated_version = validate_version!(opts.version)

    opts
    |> Map.put(:prefix, validated_prefix)
    |> Map.put(:version, validated_version)
    |> Map.put(:quoted_prefix, inspect(validated_prefix))
    |> Map.put_new(:create_schema, validated_prefix != @default_prefix)
  end

  @spec validate_version!(term()) :: pos_integer()
  defp validate_version!(version) when is_integer(version) do
    if version in @initial_version..@current_version do
      version
    else
      raise ArgumentError,
            "invalid migration version #{inspect(version)}; expected #{@initial_version}..#{@current_version}"
    end
  end

  defp validate_version!(version) do
    raise ArgumentError,
          "invalid migration version #{inspect(version)}; expected integer in #{@initial_version}..#{@current_version}"
  end

  @spec validate_prefix!(term()) :: String.t()
  defp validate_prefix!(prefix) when is_binary(prefix) do
    if prefix =~ ~r/^[A-Za-z_][A-Za-z0-9_]*$/ do
      prefix
    else
      raise ArgumentError,
            "invalid migration prefix #{inspect(prefix)}; expected SQL identifier matching ^[A-Za-z_][A-Za-z0-9_]*$"
    end
  end

  defp validate_prefix!(prefix) do
    raise ArgumentError,
          "invalid migration prefix #{inspect(prefix)}; expected binary SQL identifier"
  end
end
