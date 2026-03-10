defmodule Endurant.Scheduler do
  @moduledoc false

  use GenServer

  alias Endurant.Settings

  @default_prefix "public"
  @default_lease_ms 30_000
  @default_heartbeat_ms 10_000
  @default_retry_ms 2_000

  defstruct [
    :instance,
    :setting_id,
    :owner,
    :lease_ms,
    :heartbeat_ms,
    :retry_ms,
    :runtime_opts,
    :fence,
    active?: false
  ]

  @type state :: %__MODULE__{
          instance: atom() | String.t(),
          setting_id: String.t(),
          owner: String.t(),
          lease_ms: pos_integer(),
          heartbeat_ms: pos_integer(),
          retry_ms: pos_integer(),
          runtime_opts: keyword(),
          fence: non_neg_integer() | nil,
          active?: boolean()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @spec setting_id(atom() | String.t()) :: String.t()
  def setting_id(instance) when is_atom(instance) do
    "scheduler:" <> Atom.to_string(instance)
  end

  def setting_id(instance) when is_binary(instance) do
    "scheduler:" <> instance
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    repo = Keyword.fetch!(opts, :repo)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    lease_ms = positive_integer(Keyword.get(opts, :lease_ms, @default_lease_ms), :lease_ms)

    heartbeat_ms =
      positive_integer(Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms), :heartbeat_ms)

    retry_ms = positive_integer(Keyword.get(opts, :retry_ms, @default_retry_ms), :retry_ms)

    state = %__MODULE__{
      instance: instance,
      setting_id: Keyword.get(opts, :setting_id, setting_id(instance)),
      owner: owner_id(instance),
      lease_ms: lease_ms,
      heartbeat_ms: heartbeat_ms,
      retry_ms: retry_ms,
      runtime_opts: [repo: repo, prefix: prefix]
    }

    {:ok, state, {:continue, :acquire}}
  end

  @impl true
  def handle_continue(:acquire, %__MODULE__{} = state) do
    {:noreply, try_acquire(state)}
  end

  @impl true
  def handle_info(:retry_acquire, %__MODULE__{} = state) do
    {:noreply, try_acquire(state)}
  end

  @impl true
  def handle_info(:heartbeat, %__MODULE__{active?: true} = state) do
    case Settings.heartbeat_lease(
           state.setting_id,
           state.owner,
           state.lease_ms,
           state.runtime_opts
         ) do
      :ok ->
        schedule(:heartbeat, state.heartbeat_ms)
        {:noreply, state}

      {:error, :lock_lost} ->
        schedule(:retry_acquire, state.retry_ms)
        {:noreply, %{state | active?: false, fence: nil}}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        {:noreply, %{state | active?: false, fence: nil}}
    end
  end

  def handle_info(:heartbeat, %__MODULE__{} = state) do
    schedule(:retry_acquire, state.retry_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %__MODULE__{active?: true} = state) do
    _ = Settings.release_lease(state.setting_id, state.owner, state.runtime_opts)
    :ok
  end

  def terminate(_reason, %__MODULE__{}) do
    :ok
  end

  @spec try_acquire(state()) :: state()
  defp try_acquire(%__MODULE__{} = state) do
    case Settings.put_new(state.setting_id, %{}, state.runtime_opts) do
      :ok ->
        acquire_after_ensure(state)

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec acquire_after_ensure(state()) :: state()
  defp acquire_after_ensure(%__MODULE__{} = state) do
    case Settings.claim_lease(state.setting_id, state.owner, state.lease_ms, state.runtime_opts) do
      {:ok, fence} ->
        schedule(:heartbeat, state.heartbeat_ms)
        %{state | active?: true, fence: fence}

      :busy ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}

      {:error, :transient_db} ->
        schedule(:retry_acquire, state.retry_ms)
        %{state | active?: false, fence: nil}
    end
  end

  @spec schedule(term(), non_neg_integer()) :: reference()
  defp schedule(message, delay_ms) do
    Process.send_after(self(), message, delay_ms)
  end

  @spec owner_id(atom() | String.t()) :: String.t()
  defp owner_id(instance) do
    uniq = System.unique_integer([:positive, :monotonic])
    "#{node()}:#{inspect(instance)}:#{uniq}"
  end

  @spec positive_integer(term(), atom()) :: pos_integer()
  defp positive_integer(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, name) do
    raise ArgumentError, "#{inspect(name)} must be a positive integer, got: #{inspect(value)}"
  end
end
