defmodule Endurant.TestSupport.PostgresHelper do
  @moduledoc false

  @base_version 20_260_000_100_000

  defmodule Repo do
    use Ecto.Repo,
      otp_app: :endurant,
      adapter: Ecto.Adapters.Postgres

    @spec init(term(), keyword()) :: {:ok, keyword()}
    def init(_, config) do
      db_config = Application.fetch_env!(:endurant, :postgres)
      {:ok, Keyword.merge(config, db_config)}
    end
  end

  defmodule StepMigration do
    use Ecto.Migration

    @spec up() :: :ok
    def up do
      Endurant.Migration.up(prefix: migration_prefix())
    end

    @spec down() :: :ok
    def down do
      Endurant.Migration.down(version: 1, prefix: migration_prefix())
    end

    @spec migration_prefix() :: String.t()
    defp migration_prefix do
      Application.fetch_env!(:endurant, :test_prefix)
    end
  end

  @spec start_repo!() :: :ok
  def start_repo! do
    Application.put_env(:endurant, Repo, [])

    case Repo.__adapter__().storage_up(Repo.config()) do
      :ok -> :ok
      {:error, :already_up} -> :ok
    end

    :ok
  end

  @spec prepare_prefix!(String.t()) :: :ok
  def prepare_prefix!(prefix) do
    Application.put_env(:endurant, :test_prefix, prefix)

    _ = Ecto.Migrator.up(Repo, @base_version + 1, StepMigration, log: false)
    :ok
  end

  @spec cleanup_prefix!(String.t()) :: :ok
  def cleanup_prefix!(prefix) do
    Application.put_env(:endurant, :test_prefix, prefix)

    {started_here?, pid} =
      case Process.whereis(Repo) do
        nil ->
          {:ok, pid} = Repo.start_link()
          {true, pid}

        pid ->
          {false, pid}
      end

    try do
      case Ecto.Migrator.down(Repo, @base_version + 1, StepMigration, log: false) do
        :ok -> :ok
        :already_down -> :ok
      end
    rescue
      _ -> :ok
    end

    Repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")

    if started_here? do
      Process.exit(pid, :normal)
    end

    :ok
  end

  @spec truncate_prefix!(String.t()) :: :ok
  def truncate_prefix!(prefix) do
    Repo.query!("DELETE FROM #{prefix}.endurant_events")
    Repo.query!("DELETE FROM #{prefix}.endurant_archive_deliveries")
    Repo.query!("DELETE FROM #{prefix}.endurant_cron_schedules")
    Repo.query!("DELETE FROM #{prefix}.endurant_scheduled_executions")
    Repo.query!("DELETE FROM #{prefix}.endurant_executions")
    Repo.query!("DELETE FROM #{prefix}.endurant_settings")
    :ok
  end

  @spec runtime_opts(String.t(), term() | nil) :: keyword()
  def runtime_opts(prefix, instance \\ nil) do
    base = [repo: Repo, prefix: prefix]

    case instance do
      nil -> base
      _ -> Keyword.put(base, :instance, instance)
    end
  end

  @spec wait_for_execution!(binary(), timeout(), keyword()) ::
          {:ok, %{status: :completed | :failed, result: term()}} | no_return()
  def wait_for_execution!(execution_id, timeout_ms, opts)
      when is_binary(execution_id) and is_integer(timeout_ms) and timeout_ms >= 0 and
             is_list(opts) do
    poll_ms = Keyword.get(opts, :poll_interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait(execution_id, deadline, poll_ms, timeout_ms, opts)
  end

  @spec do_wait(binary(), integer(), non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, %{status: :completed | :failed, result: term()}} | no_return()
  defp do_wait(execution_id, deadline, poll_ms, timeout_ms, opts) do
    case Endurant.execution(execution_id, instance: instance_from_opts!(opts)) do
      %{status: :completed} ->
        case replay(execution_id, opts) do
          {:ok, result} ->
            {:ok, %{status: :completed, result: result}}

          {:error, :not_completed} ->
            if System.monotonic_time(:millisecond) >= deadline do
              raise ExUnit.AssertionError,
                    "execution #{execution_id} reached completed status but missing completion event within #{timeout_ms}ms"
            else
              Process.sleep(poll_ms)
              do_wait(execution_id, deadline, poll_ms, timeout_ms, opts)
            end
        end

      %{status: :failed} ->
        {:ok, %{status: :failed, result: failure_result(execution_id, opts)}}

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise ExUnit.AssertionError,
                "execution #{execution_id} did not reach terminal state within #{timeout_ms}ms"
        else
          Process.sleep(poll_ms)
          do_wait(execution_id, deadline, poll_ms, timeout_ms, opts)
        end
    end
  end

  @spec history(binary(), keyword()) :: {:ok, [Endurant.Events.event()]}
  def history(execution_id, opts \\ []) do
    {:ok, Endurant.events(execution_id, instance: instance_from_opts!(opts))}
  end

  @spec replay(binary(), keyword()) :: {:ok, term()} | {:error, :not_completed}
  def replay(execution_id, opts \\ []) do
    completion_event =
      execution_id
      |> then(&Endurant.events(&1, instance: instance_from_opts!(opts)))
      |> Enum.reverse()
      |> Enum.find(&(&1.type == :execution_completed))

    case completion_event do
      nil ->
        {:error, :not_completed}

      %{payload: payload} ->
        {:ok, payload_result(payload)}
    end
  end

  @spec failure_result(binary(), keyword()) :: term()
  defp failure_result(execution_id, opts) do
    failure =
      execution_id
      |> then(&Endurant.events(&1, instance: instance_from_opts!(opts)))
      |> Enum.reverse()
      |> Enum.find(&(&1.type == :execution_failed))

    case failure do
      %{payload: %{"error" => error}} -> error
      %{payload: %{error: error}} -> error
      _ -> nil
    end
  end

  @spec payload_result(map()) :: term()
  defp payload_result(%{"result" => result}), do: atomize(result)
  defp payload_result(%{result: result}), do: atomize(result)
  defp payload_result(payload), do: atomize(payload)

  @spec instance_from_opts!(keyword()) :: term()
  defp instance_from_opts!(opts) do
    Keyword.fetch!(opts, :instance)
  end

  @spec atomize(term()) :: term()
  defp atomize(%{} = map) do
    Map.new(map, fn
      {key, value} when is_binary(key) -> {String.to_atom(key), atomize(value)}
      {key, value} -> {key, atomize(value)}
    end)
  end

  defp atomize(value) when is_binary(value) do
    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> value
    end
  end

  defp atomize(list) when is_list(list), do: Enum.map(list, &atomize/1)
  defp atomize(other), do: other
end
