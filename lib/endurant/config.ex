defmodule Endurant.Config do
  @moduledoc false

  @default_name Endurant
  @default_prefix "public"
  @default_queue_defaults [limit: 1]

  @type instance_name :: atom() | String.t()
  @type archiver_name :: String.t()
  @type archiver_options :: keyword()

  @type t :: %__MODULE__{
          name: instance_name(),
          repo: module(),
          prefix: String.t(),
          queues: keyword(keyword()),
          archivers: [{archiver_name(), archiver_options()}],
          pruner: keyword(),
          queue_defaults: keyword(),
          crons: [map()]
        }

  @enforce_keys [:name, :repo, :prefix, :queues, :archivers, :pruner, :queue_defaults, :crons]
  defstruct [:name, :repo, :prefix, :queues, :archivers, :pruner, :queue_defaults, :crons]

  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    name = require_name!(opts)
    queue_defaults = queue_defaults!(opts)
    queues = queues!(opts)
    archivers = archivers!(opts)
    pruner = pruner!(opts)
    crons = crons!(opts)
    repo = repo!(opts)
    prefix = prefix!(opts)

    normalized_queues =
      Enum.map(queues, fn {queue, queue_opts} ->
        merged =
          queue_defaults
          |> Keyword.merge(queue_opts)
          |> Keyword.put(:repo, repo)
          |> Keyword.put(:prefix, prefix)

        {queue, merged}
      end)

    %__MODULE__{
      name: name,
      repo: repo,
      prefix: prefix,
      queues: normalized_queues,
      archivers: archivers,
      pruner: pruner,
      queue_defaults: queue_defaults,
      crons: crons
    }
  end

  @spec runtime_opts(t()) :: keyword()
  def runtime_opts(%__MODULE__{} = config) do
    [repo: config.repo, prefix: config.prefix, instance: config.name]
  end

  @spec require_name!(keyword()) :: instance_name()
  defp require_name!(opts) do
    opts
    |> Keyword.get(:name, @default_name)
    |> validate_name!()
  end

  @spec validate_name!(term()) :: instance_name()
  defp validate_name!(name) when is_atom(name), do: name

  defp validate_name!(name) when is_binary(name) do
    if String.trim(name) == "" do
      raise ArgumentError, ":name must be a non-empty string or atom, got: #{inspect(name)}"
    end

    name
  end

  defp validate_name!(name) do
    raise ArgumentError, ":name must be a non-empty string or atom, got: #{inspect(name)}"
  end

  @spec queue_defaults!(keyword()) :: keyword()
  defp queue_defaults!(opts) do
    case Keyword.get(opts, :queue_defaults, @default_queue_defaults) do
      defaults when is_list(defaults) ->
        if Keyword.keyword?(defaults) do
          defaults
        else
          raise ArgumentError,
                ":queue_defaults must be a keyword list, got: #{inspect(defaults)}"
        end

      other ->
        raise ArgumentError,
              ":queue_defaults must be a keyword list, got: #{inspect(other)}"
    end
  end

  @spec crons!(keyword()) :: [map()]
  defp crons!(opts) do
    case Keyword.get(opts, :crons, []) do
      crons when is_list(crons) ->
        normalized = Enum.map(crons, &normalize_cron!/1)
        validate_unique_cron_names!(normalized)
        normalized

      other ->
        raise ArgumentError, ":crons must be a list, got: #{inspect(other)}"
    end
  end

  @spec normalize_cron!(term()) :: map()
  defp normalize_cron!(entry) when is_list(entry) do
    if Keyword.keyword?(entry) do
      normalize_cron_map!(Map.new(entry))
    else
      raise ArgumentError,
            "each cron config entry must be a map or keyword list, got: #{inspect(entry)}"
    end
  end

  defp normalize_cron!(%{} = entry), do: normalize_cron_map!(entry)

  defp normalize_cron!(other) do
    raise ArgumentError,
          "each cron config entry must be a map or keyword list, got: #{inspect(other)}"
  end

  @spec normalize_cron_map!(map()) :: map()
  defp normalize_cron_map!(entry) do
    name = require_non_empty_binary!(Map.get(entry, :name) || Map.get(entry, "name"), :name)

    cron_expr =
      require_non_empty_binary!(
        Map.get(entry, :expr) || Map.get(entry, "expr") || Map.get(entry, :cron_expr) ||
          Map.get(entry, "cron_expr"),
        :expr
      )

    workflow =
      case Map.get(entry, :workflow) || Map.get(entry, "workflow") do
        workflow when is_atom(workflow) ->
          workflow

        other ->
          raise ArgumentError, "cron :workflow must be a module atom, got: #{inspect(other)}"
      end

    input =
      case Map.get(entry, :input) || Map.get(entry, "input") || %{} do
        input when is_map(input) -> input
        other -> raise ArgumentError, "cron :input must be a map, got: #{inspect(other)}"
      end

    timezone =
      case Map.get(entry, :timezone) || Map.get(entry, "timezone") || "Etc/UTC" do
        timezone when is_binary(timezone) and timezone != "" ->
          timezone

        other ->
          raise ArgumentError, "cron :timezone must be a non-empty string, got: #{inspect(other)}"
      end

    start_at =
      case Map.get(entry, :start_at) || Map.get(entry, "start_at") || DateTime.utc_now() do
        %DateTime{} = dt -> dt
        other -> raise ArgumentError, "cron :start_at must be DateTime, got: #{inspect(other)}"
      end

    end_at =
      case Map.get(entry, :end_at) || Map.get(entry, "end_at") do
        nil ->
          nil

        %DateTime{} = dt ->
          dt

        other ->
          raise ArgumentError, "cron :end_at must be DateTime or nil, got: #{inspect(other)}"
      end

    status =
      normalize_cron_status!(Map.get(entry, :status) || Map.get(entry, "status") || :active)

    %{
      name: name,
      expr: cron_expr,
      workflow: workflow,
      input: input,
      timezone: timezone,
      start_at: start_at,
      end_at: end_at,
      status: status
    }
  end

  @spec normalize_cron_status!(term()) :: :active | :paused
  defp normalize_cron_status!(:active), do: :active
  defp normalize_cron_status!(:paused), do: :paused
  defp normalize_cron_status!("active"), do: :active
  defp normalize_cron_status!("paused"), do: :paused

  defp normalize_cron_status!(other) do
    raise ArgumentError, "cron :status must be :active or :paused, got: #{inspect(other)}"
  end

  @spec validate_unique_cron_names!([map()]) :: :ok
  defp validate_unique_cron_names!(crons) do
    duplicates =
      crons
      |> Enum.map(&Map.fetch!(&1, :name))
      |> Enum.group_by(& &1)
      |> Enum.filter(fn {_name, names} -> length(names) > 1 end)
      |> Enum.map(&elem(&1, 0))

    if duplicates != [] do
      raise ArgumentError, "duplicate cron names are not allowed: #{inspect(duplicates)}"
    end

    :ok
  end

  @spec require_non_empty_binary!(term(), atom()) :: String.t()
  defp require_non_empty_binary!(value, field) when is_binary(value) do
    if String.trim(value) == "" do
      raise ArgumentError, "cron #{inspect(field)} must be a non-empty string"
    end

    value
  end

  defp require_non_empty_binary!(value, field) do
    raise ArgumentError,
          "cron #{inspect(field)} must be a non-empty string, got: #{inspect(value)}"
  end

  @spec queues!(keyword()) :: keyword(keyword())
  defp queues!(opts) do
    queues = Keyword.get(opts, :queues, default: [])

    if not Keyword.keyword?(queues) do
      raise ArgumentError, ":queues must be a keyword list, got: #{inspect(queues)}"
    end

    duplicates =
      queues
      |> Keyword.keys()
      |> Enum.group_by(& &1)
      |> Enum.filter(fn {_queue, entries} -> length(entries) > 1 end)
      |> Enum.map(&elem(&1, 0))

    if duplicates != [] do
      raise ArgumentError, "duplicate queues are not allowed: #{inspect(duplicates)}"
    end

    Enum.map(queues, fn {queue, queue_opts} ->
      normalized_queue = normalize_queue!(queue)

      if not (is_list(queue_opts) and Keyword.keyword?(queue_opts)) do
        raise ArgumentError,
              "queue options for #{inspect(normalized_queue)} must be keyword list, got: #{inspect(queue_opts)}"
      end

      {normalized_queue, queue_opts}
    end)
  end

  @spec normalize_queue!(term()) :: atom()
  defp normalize_queue!(queue) when is_atom(queue), do: queue

  defp normalize_queue!(queue) do
    raise ArgumentError,
          "queue names must be atoms to avoid dynamic atom creation, got: #{inspect(queue)}"
  end

  @spec archivers!(keyword()) :: [{archiver_name(), archiver_options()}]
  defp archivers!(opts) do
    archivers = Keyword.get(opts, :archivers, [])

    if not Keyword.keyword?(archivers) do
      raise ArgumentError, ":archivers must be a keyword list, got: #{inspect(archivers)}"
    end

    normalized =
      Enum.map(archivers, fn {archiver, archiver_opts} ->
        normalized_archiver = normalize_archiver_name!(archiver)

        if not (is_list(archiver_opts) and Keyword.keyword?(archiver_opts)) do
          raise ArgumentError,
                "archiver options for #{inspect(normalized_archiver)} must be keyword list, got: #{inspect(archiver_opts)}"
        end

        module = Keyword.get(archiver_opts, :module)

        if not is_atom(module) do
          raise ArgumentError,
                "archiver #{inspect(normalized_archiver)} requires :module, got: #{inspect(module)}"
        end

        validate_archiver_config!(normalized_archiver, archiver_opts)

        {normalized_archiver, archiver_opts}
      end)

    duplicates =
      normalized
      |> Enum.map(&elem(&1, 0))
      |> Enum.group_by(& &1)
      |> Enum.filter(fn {_archiver, entries} -> length(entries) > 1 end)
      |> Enum.map(&elem(&1, 0))

    if duplicates != [] do
      raise ArgumentError, "duplicate archivers are not allowed: #{inspect(duplicates)}"
    end

    normalized
  end

  @spec normalize_archiver_name!(term()) :: String.t()
  defp normalize_archiver_name!(archiver) when is_atom(archiver) do
    archiver
    |> Atom.to_string()
    |> normalize_archiver_name!()
  end

  defp normalize_archiver_name!(archiver) when is_binary(archiver) do
    normalized = String.trim(archiver)

    if normalized == "" do
      raise ArgumentError, "archiver names must be non-empty strings or atoms"
    end

    normalized
  end

  defp normalize_archiver_name!(archiver) do
    raise ArgumentError,
          "archiver names must be non-empty strings or atoms, got: #{inspect(archiver)}"
  end

  @spec validate_archiver_config!(String.t(), keyword()) :: :ok
  defp validate_archiver_config!(archiver, archiver_opts) do
    case Keyword.fetch(archiver_opts, :enabled) do
      {:ok, enabled} when is_boolean(enabled) ->
        :ok

      {:ok, other} ->
        raise ArgumentError,
              "archiver #{inspect(archiver)} :enabled must be a boolean, got: #{inspect(other)}"

      :error ->
        :ok
    end

    :ok
  end

  @spec pruner!(keyword()) :: keyword()
  defp pruner!(opts) do
    case Keyword.get(opts, :pruner, enabled: false) do
      pruner_opts when is_list(pruner_opts) ->
        if Keyword.keyword?(pruner_opts) do
          validate_pruner_config!(pruner_opts)
          pruner_opts
        else
          raise ArgumentError, ":pruner must be a keyword list, got: #{inspect(pruner_opts)}"
        end

      other ->
        raise ArgumentError, ":pruner must be a keyword list, got: #{inspect(other)}"
    end
  end

  @spec validate_pruner_config!(keyword()) :: :ok
  defp validate_pruner_config!(pruner_opts) do
    case Keyword.fetch(pruner_opts, :enabled) do
      {:ok, enabled} when is_boolean(enabled) ->
        :ok

      {:ok, other} ->
        raise ArgumentError, ":pruner :enabled must be a boolean, got: #{inspect(other)}"

      :error ->
        :ok
    end

    :ok
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} ->
        repo

      :error ->
        raise ArgumentError, "missing required :repo option"
    end
  end

  @spec prefix!(keyword()) :: String.t()
  defp prefix!(opts) do
    case Keyword.fetch(opts, :prefix) do
      {:ok, prefix} when is_binary(prefix) ->
        prefix

      {:ok, other} ->
        raise ArgumentError, ":prefix must be a binary, got: #{inspect(other)}"

      :error ->
        @default_prefix
    end
  end
end
