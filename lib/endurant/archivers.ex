defmodule Endurant.Archivers do
  @moduledoc false

  alias Endurant.Settings

  @default_prefix "public"

  @type archiver_name :: atom() | String.t()
  @type cursor_value :: %{
          optional(String.t()) => String.t() | nil
        }
  @type archiver_setting :: map()
  @worker_archiver_keys [:module, :batch_size, :scan_ms, :heartbeat_ms, :retry_ms, :lease_ms]

  @spec setting_id(archiver_name()) :: String.t()
  def setting_id(archiver) do
    "archiver:" <> normalize_archiver!(archiver)
  end

  @spec get(archiver_name(), keyword()) :: archiver_setting() | nil | {:error, :transient_db}
  def get(archiver, opts \\ []) do
    Settings.get(setting_id(archiver), opts)
  end

  @spec put(archiver_name(), map(), keyword()) :: :ok | {:error, :transient_db}
  def put(archiver, value, opts \\ []) when is_map(value) and is_list(opts) do
    archiver = normalize_archiver!(archiver)

    value
    |> stringify_keys()
    |> Map.put("archiver", archiver)
    |> then(&Settings.put(setting_id(archiver), &1, opts))
  end

  @spec sync_from_config([{archiver_name(), keyword()}], keyword()) ::
          :ok | {:error, :transient_db}
  def sync_from_config(archivers, opts \\ []) when is_list(archivers) and is_list(opts) do
    Enum.reduce_while(archivers, :ok, fn {archiver, archiver_opts}, :ok ->
      case sync_archiver_from_config(archiver, archiver_opts, opts) do
        :ok -> {:cont, :ok}
        {:error, :transient_db} = error -> {:halt, error}
      end
    end)
  end

  @spec list(keyword()) :: [archiver_setting()] | {:error, :transient_db}
  def list(opts \\ []) when is_list(opts) do
    do_list(nil, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec enabled(keyword()) :: [archiver_setting()] | {:error, :transient_db}
  def enabled(opts \\ []) when is_list(opts) do
    do_list(true, opts)
  rescue
    DBConnection.ConnectionError -> {:error, :transient_db}
  end

  @spec enabled_names(keyword()) :: [String.t()] | {:error, :transient_db}
  def enabled_names(opts \\ []) when is_list(opts) do
    case enabled(opts) do
      settings when is_list(settings) ->
        Enum.map(settings, &Map.get(&1, "archiver"))

      other ->
        other
    end
  end

  @spec resolve_module(archiver_name()) :: {:ok, module()} | {:error, :not_found}
  def resolve_module(archiver) do
    normalized = normalize_archiver!(archiver)

    case Enum.find(candidate_modules(), &module_matches_archiver?(&1, normalized)) do
      nil -> {:error, :not_found}
      module -> {:ok, module}
    end
  end

  @spec cursor(archiver_name(), keyword()) :: cursor_value() | nil | {:error, :transient_db}
  def cursor(archiver, opts \\ []) when is_list(opts) do
    case get(archiver, opts) do
      %{} = value ->
        case Map.get(value, "cursor") do
          %{} = cursor -> cursor
          _ -> nil
        end

      other ->
        other
    end
  end

  @spec put_cursor(
          archiver_name(),
          DateTime.t() | NaiveDateTime.t() | String.t() | nil,
          String.t() | nil,
          keyword()
        ) :: :ok | {:error, :transient_db}
  def put_cursor(archiver, completed_at, execution_id, opts \\ []) when is_list(opts) do
    case get(archiver, opts) do
      {:error, :transient_db} ->
        {:error, :transient_db}

      %{} = value ->
        put(
          archiver,
          Map.put(value, "cursor", %{
            "completed_at" => normalize_cursor_time(completed_at),
            "execution_id" => execution_id
          }),
          opts
        )

      nil ->
        put(
          archiver,
          %{
            "enabled" => false,
            "cursor" => %{
              "completed_at" => normalize_cursor_time(completed_at),
              "execution_id" => execution_id
            }
          },
          opts
        )
    end
  end

  @spec do_list(boolean() | nil, keyword()) :: [archiver_setting()]
  defp do_list(enabled?, opts) do
    repo = repo!(opts)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    {where_sql, params} =
      case enabled? do
        true ->
          {"AND value->>'enabled' = $1", ["true"]}

        nil ->
          {"", []}
      end

    sql = """
    SELECT value
    FROM #{prefix}.endurant_settings
    WHERE value ? 'archiver'
    #{where_sql}
    ORDER BY value->>'archiver' ASC
    """

    query!(repo, sql, params).rows
    |> Enum.map(fn [value] -> value end)
  end

  @spec sync_archiver_from_config(archiver_name(), keyword(), keyword()) ::
          :ok | {:error, :transient_db}
  defp sync_archiver_from_config(archiver, archiver_opts, opts) do
    archiver = normalize_archiver!(archiver)
    config_value = setting_from_config(archiver, archiver_opts)

    case get(archiver, opts) do
      {:error, :transient_db} ->
        {:error, :transient_db}

      %{} = current ->
        put(archiver, Map.merge(current, config_value), opts)

      nil ->
        put(archiver, config_value, opts)
    end
  end

  @spec setting_from_config(String.t(), keyword()) :: map()
  defp setting_from_config(archiver, archiver_opts) do
    archiver_opts
    |> Keyword.drop(@worker_archiver_keys)
    |> Enum.reject(fn {key, _value} -> key == :module end)
    |> Enum.reduce(%{"archiver" => archiver, "enabled" => false}, fn
      {:enabled, enabled}, acc when is_boolean(enabled) ->
        Map.put(acc, "enabled", enabled)

      {key, value}, acc ->
        case normalize_setting_value(value) do
          {:ok, normalized} -> Map.put(acc, to_string(key), normalized)
          :skip -> acc
        end
    end)
  end

  @spec normalize_setting_value(term()) :: {:ok, term()} | :skip
  defp normalize_setting_value(value) when is_binary(value), do: {:ok, value}
  defp normalize_setting_value(value) when is_boolean(value), do: {:ok, value}
  defp normalize_setting_value(value) when is_integer(value), do: {:ok, value}
  defp normalize_setting_value(value) when is_float(value), do: {:ok, value}
  defp normalize_setting_value(nil), do: {:ok, nil}

  defp normalize_setting_value(%DateTime{} = value),
    do: {:ok, DateTime.to_iso8601(value)}

  defp normalize_setting_value(%NaiveDateTime{} = value),
    do: {:ok, NaiveDateTime.to_iso8601(value)}

  defp normalize_setting_value(%{} = value) do
    {:ok,
     Map.new(value, fn {key, nested_value} ->
       normalized_key = to_string(key)

       case normalize_setting_value(nested_value) do
         {:ok, normalized_value} -> {normalized_key, normalized_value}
         :skip -> {normalized_key, nil}
       end
     end)}
  end

  defp normalize_setting_value(list) when is_list(list) do
    if Keyword.keyword?(list) do
      normalize_setting_value(Map.new(list))
    else
      {:ok,
       Enum.flat_map(list, fn item ->
         case normalize_setting_value(item) do
           {:ok, normalized_item} -> [normalized_item]
           :skip -> []
         end
       end)}
    end
  end

  defp normalize_setting_value(_value), do: :skip

  @spec candidate_modules() :: [module()]
  defp candidate_modules do
    :endurant
    |> Application.spec(:modules)
    |> Kernel.||([])
    |> Enum.filter(&archiver_module?/1)
  end

  @spec archiver_module?(module()) :: boolean()
  defp archiver_module?(module) do
    behaviours =
      module.module_info(:attributes)
      |> Keyword.get(:behaviour, [])

    Endurant.Archiver in behaviours
  end

  @spec module_matches_archiver?(module(), String.t()) :: boolean()
  defp module_matches_archiver?(module, normalized_archiver) do
    module
    |> Module.split()
    |> List.last()
    |> case do
      nil ->
        false

      name ->
        candidates = [Macro.underscore(name), String.replace(Macro.underscore(name), "_", "")]
        normalized_archiver in candidates
    end
  end

  @spec normalize_archiver!(archiver_name()) :: String.t()
  defp normalize_archiver!(archiver) when is_atom(archiver) do
    archiver
    |> Atom.to_string()
    |> normalize_archiver!()
  end

  defp normalize_archiver!(archiver) when is_binary(archiver) do
    normalized = String.trim(archiver)

    if normalized == "" do
      raise ArgumentError, "archiver must be a non-empty string or atom"
    end

    normalized
  end

  defp normalize_archiver!(archiver) do
    raise ArgumentError, "archiver must be a non-empty string or atom, got: #{inspect(archiver)}"
  end

  @spec normalize_cursor_time(DateTime.t() | NaiveDateTime.t() | String.t() | nil) ::
          String.t() | nil
  defp normalize_cursor_time(nil), do: nil
  defp normalize_cursor_time(%DateTime{} = completed_at), do: DateTime.to_iso8601(completed_at)

  defp normalize_cursor_time(%NaiveDateTime{} = completed_at),
    do: NaiveDateTime.to_iso8601(completed_at)

  defp normalize_cursor_time(completed_at) when is_binary(completed_at), do: completed_at

  @spec stringify_keys(term()) :: term()
  defp stringify_keys(%{} = map) do
    Map.new(map, fn {key, value} -> {to_string(key), stringify_keys(value)} end)
  end

  defp stringify_keys(list) when is_list(list) do
    Enum.map(list, &stringify_keys/1)
  end

  defp stringify_keys(value), do: value

  @spec query!(module(), iodata(), list()) :: map()
  defp query!(repo, sql, params) do
    repo.query!(sql, params, log: false)
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
  end
end
