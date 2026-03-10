defmodule Endurant.Config do
  @moduledoc false

  @default_name Endurant
  @default_prefix "public"
  @default_queue_defaults [limit: 1]

  @type instance_name :: atom() | String.t()

  @type t :: %__MODULE__{
          name: instance_name(),
          repo: module(),
          prefix: String.t(),
          queues: keyword(keyword()),
          queue_defaults: keyword()
        }

  @enforce_keys [:name, :repo, :prefix, :queues, :queue_defaults]
  defstruct [:name, :repo, :prefix, :queues, :queue_defaults]

  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    name = require_name!(opts)
    queue_defaults = queue_defaults!(opts)
    queues = queues!(opts)
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
      queue_defaults: queue_defaults
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
