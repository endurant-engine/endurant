defmodule Endurant.Settings do
  @moduledoc false

  @default_prefix "public"

  @type lease_result :: {:ok, non_neg_integer()} | :busy | {:error, :transient_db}
  @type heartbeat_result :: :ok | {:error, :lock_lost | :transient_db}

  @spec put(String.t(), map(), keyword()) :: :ok | {:error, :transient_db}
  def put(id, value, opts \\ []) when is_binary(id) and is_map(value) and is_list(opts) do
    do_put(id, value, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec put_new(String.t(), map(), keyword()) :: :ok | {:error, :transient_db}
  def put_new(id, value, opts \\ []) when is_binary(id) and is_map(value) and is_list(opts) do
    do_put_new(id, value, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec get(String.t(), keyword()) :: map() | nil | {:error, :transient_db}
  def get(id, opts \\ []) when is_binary(id) and is_list(opts) do
    do_get(id, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec claim_lease(String.t(), String.t(), pos_integer(), keyword()) :: lease_result()
  def claim_lease(id, owner, lease_ms, opts \\ [])
      when is_binary(id) and is_binary(owner) and is_integer(lease_ms) and lease_ms > 0 and
             is_list(opts) do
    do_claim_lease(id, owner, lease_ms, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec heartbeat_lease(String.t(), String.t(), pos_integer(), keyword()) :: heartbeat_result()
  def heartbeat_lease(id, owner, lease_ms, opts \\ [])
      when is_binary(id) and is_binary(owner) and is_integer(lease_ms) and lease_ms > 0 and
             is_list(opts) do
    do_heartbeat_lease(id, owner, lease_ms, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec release_lease(String.t(), String.t(), keyword()) :: :ok | {:error, :transient_db}
  def release_lease(id, owner, opts \\ [])
      when is_binary(id) and is_binary(owner) and is_list(opts) do
    do_release_lease(id, owner, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec do_put(String.t(), map(), keyword()) :: :ok
  defp do_put(id, value, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    INSERT INTO #{prefix}.endurant_settings (id, value, inserted_at, updated_at)
    VALUES ($1, $2::jsonb, timezone('UTC', now()), timezone('UTC', now()))
    ON CONFLICT (id) DO UPDATE
      SET value = EXCLUDED.value,
          updated_at = timezone('UTC', now())
    """

    _ = query!(repo, sql, [id, value])
    :ok
  end

  @spec do_put_new(String.t(), map(), keyword()) :: :ok
  defp do_put_new(id, value, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    INSERT INTO #{prefix}.endurant_settings (id, value, inserted_at, updated_at)
    VALUES ($1, $2::jsonb, timezone('UTC', now()), timezone('UTC', now()))
    ON CONFLICT (id) DO NOTHING
    """

    _ = query!(repo, sql, [id, value])
    :ok
  end

  @spec do_get(String.t(), keyword()) :: map() | nil
  defp do_get(id, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    sql = "SELECT value FROM #{prefix}.endurant_settings WHERE id = $1"

    case query!(repo, sql, [id]).rows do
      [[value]] when is_map(value) -> value
      _ -> nil
    end
  end

  @spec do_claim_lease(String.t(), String.t(), pos_integer(), keyword()) :: lease_result()
  defp do_claim_lease(id, owner, lease_ms, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    _ = do_put_new(id, %{}, opts)

    sql = """
    WITH clock AS (
      SELECT FLOOR(EXTRACT(EPOCH FROM timezone('UTC', now())) * 1000)::bigint AS now_ms
    ),
    current AS (
      SELECT
        s.id,
        s.value,
        COALESCE((s.value->>'lease_until_ms')::bigint, 0) AS lease_until_ms,
        COALESCE(s.value->>'owner', '') AS owner,
        COALESCE((s.value->>'fence')::bigint, 0) AS fence,
        clock.now_ms
      FROM #{prefix}.endurant_settings s
      CROSS JOIN clock
      WHERE s.id = $1
      FOR UPDATE
    )
    UPDATE #{prefix}.endurant_settings s
    SET
      value = jsonb_set(
        jsonb_set(
          jsonb_set(s.value, '{owner}', to_jsonb($2::text), true),
          '{lease_until_ms}',
          to_jsonb(current.now_ms + $3::bigint),
          true
        ),
        '{fence}',
        to_jsonb(current.fence + 1),
        true
      ),
      updated_at = timezone('UTC', now())
    FROM current
    WHERE s.id = current.id
    AND (
      current.lease_until_ms <= current.now_ms
      OR current.owner = ''
      OR current.owner = $2
    )
    RETURNING current.fence + 1
    """

    case query!(repo, sql, [id, owner, lease_ms]).rows do
      [[fence]] when is_integer(fence) and fence >= 1 -> {:ok, fence}
      _ -> :busy
    end
  end

  @spec do_heartbeat_lease(String.t(), String.t(), pos_integer(), keyword()) :: heartbeat_result()
  defp do_heartbeat_lease(id, owner, lease_ms, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    WITH clock AS (
      SELECT FLOOR(EXTRACT(EPOCH FROM timezone('UTC', now())) * 1000)::bigint AS now_ms
    ),
    current AS (
      SELECT
        s.id,
        s.value,
        COALESCE((s.value->>'lease_until_ms')::bigint, 0) AS lease_until_ms,
        COALESCE(s.value->>'owner', '') AS owner,
        clock.now_ms
      FROM #{prefix}.endurant_settings s
      CROSS JOIN clock
      WHERE s.id = $1
      FOR UPDATE
    )
    UPDATE #{prefix}.endurant_settings s
    SET
      value = jsonb_set(
        s.value,
        '{lease_until_ms}',
        to_jsonb(current.now_ms + $3::bigint),
        true
      ),
      updated_at = timezone('UTC', now())
    FROM current
    WHERE s.id = current.id
    AND current.owner = $2
    AND current.lease_until_ms > current.now_ms
    """

    case query!(repo, sql, [id, owner, lease_ms]).num_rows do
      1 -> :ok
      _ -> {:error, :lock_lost}
    end
  end

  @spec do_release_lease(String.t(), String.t(), keyword()) :: :ok
  defp do_release_lease(id, owner, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    sql = """
    UPDATE #{prefix}.endurant_settings
    SET
      value = jsonb_set(
        jsonb_set(value, '{owner}', 'null'::jsonb, true),
        '{lease_until_ms}',
        to_jsonb(0),
        true
      ),
      updated_at = timezone('UTC', now())
    WHERE id = $1
    AND COALESCE(value->>'owner', '') = $2
    """

    _ = query!(repo, sql, [id, owner])
    :ok
  end

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    repo.query!(sql, params, log: false)
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
  end
end
