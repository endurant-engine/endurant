defmodule Endurant.Workflow.Children do
  @moduledoc false

  alias Endurant.Events
  alias Endurant.Executions
  alias Endurant.Telemetry
  alias Endurant.Workflow

  defmodule Handle do
    @moduledoc false

    @enforce_keys [:child_key]
    defstruct [
      :child_key,
      :child_execution_id,
      :child_unique_id,
      :child_workflow_name,
      :child_version,
      :parent_close_policy
    ]

    @type t :: %__MODULE__{
            child_key: String.t(),
            child_execution_id: binary() | nil,
            child_unique_id: String.t() | nil,
            child_workflow_name: String.t() | nil,
            child_version: String.t() | nil,
            parent_close_policy: String.t() | nil
          }
  end

  @doc """
  Starts a child workflow and waits for its terminal result.

  `name` must be stable across replay for that workflow path.

  ## Options

    * `:version` - child workflow version override
    * `:unique_id` - child workflow unique id override
    * `:close_policy` - behavior when the parent execution closes:
      `:abandon` or `:request_cancel` (default `:abandon`)
  """
  @spec child_workflow(String.t(), module(), map()) :: term() | no_return()
  @spec child_workflow(String.t(), module(), map(), keyword()) :: term() | no_return()
  def child_workflow(name, workflow_module, input, opts \\ [])
      when is_binary(name) and is_atom(workflow_module) and is_map(input) and is_list(opts) do
    name
    |> child_workflow_async(workflow_module, input, opts)
    |> child_workflow_await()
  end

  @doc """
  Starts a child workflow and returns a deterministic handle.

  On replay, this returns the same handle information reconstructed from parent
  history instead of starting a new child.

  `name` must be stable across replay for that workflow path.

  ## Options

    * `:version` - child workflow version override
    * `:unique_id` - child workflow unique id override
    * `:close_policy` - behavior when the parent execution closes:
      `:abandon` or `:request_cancel` (default `:abandon`)
  """
  @spec child_workflow_async(String.t(), module(), map()) :: Handle.t() | no_return()
  @spec child_workflow_async(String.t(), module(), map(), keyword()) :: Handle.t() | no_return()
  def child_workflow_async(name, workflow_module, input, opts \\ [])
      when is_binary(name) and is_atom(workflow_module) and is_map(input) and is_list(opts) do
    child_key = normalize_child_key!(name)
    runtime = ensure_child_runtime(Workflow.runtime!())

    case child_state(runtime, child_key) do
      {:ok, state, next_runtime} ->
        Workflow.put_runtime(next_runtime)
        child_handle(child_key, state)

      :missing ->
        refreshed_runtime = refresh_child_runtime(runtime)

        case child_state(refreshed_runtime, child_key) do
          {:ok, state, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            child_handle(child_key, state)

          :missing ->
            start_child_execution(refreshed_runtime, child_key, workflow_module, input, opts)
        end
    end
  end

  @doc """
  Waits for a child workflow handle to reach a terminal state.

  Returns the child workflow result on success.

  Raises when the child fails or is cancelled.
  """
  @spec child_workflow_await(Handle.t()) :: term() | no_return()
  def child_workflow_await(%Handle{} = handle) do
    runtime = ensure_child_runtime(Workflow.runtime!())

    case child_state(runtime, handle.child_key) do
      {:ok, state, next_runtime} ->
        Workflow.put_runtime(next_runtime)
        resolve_or_wait!(next_runtime, handle, state)

      :missing ->
        refreshed_runtime = refresh_child_runtime(runtime)

        case child_state(refreshed_runtime, handle.child_key) do
          {:ok, state, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            resolve_or_wait!(next_runtime, handle, state)

          :missing ->
            wait_for_child_resume(refreshed_runtime, handle)
        end
    end
  end

  @spec wait_for_child_resume(map(), Handle.t()) :: term() | no_return()
  defp wait_for_child_resume(runtime, %Handle{} = handle) do
    wait_started_at = Telemetry.monotonic_time()

    case enter_child_wait(runtime, handle) do
      :ok ->
        do_wait_for_child_resume(runtime, handle, wait_started_at)

      :already_resolved ->
        refreshed_runtime = refresh_child_runtime(runtime)

        case child_state(refreshed_runtime, handle.child_key) do
          {:ok, state, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            resolve_or_wait!(next_runtime, handle, state)

          :missing ->
            raise RuntimeError,
                  "child workflow #{inspect(handle.child_key)} resolved before wait but no terminal event was found"
        end
    end
  end

  @spec enter_child_wait(map(), Handle.t()) :: :ok | :already_resolved | no_return()
  defp enter_child_wait(runtime, %Handle{} = handle) do
    case Executions.mark_waiting_with_child_event_owned(
           runtime.execution_id,
           runtime.worker_id,
           handle.child_key,
           handle.child_unique_id,
           handle.child_execution_id,
           runtime.opts
         ) do
      :ok ->
        emit_child(runtime, handle, :wait_started, %{count: 1})

        case notify_waiting(runtime) do
          :park ->
            emit_child(runtime, handle, :park_decision, %{count: 1}, %{decision: :park})
            :ok

          :release ->
            emit_child(runtime, handle, :park_decision, %{count: 1}, %{decision: :release})

            case Executions.release_waiting_as_abandoned_owned(
                   runtime.execution_id,
                   runtime.worker_id,
                   runtime.opts
                 ) do
              :ok ->
                throw({:endurant_halt, :waiting_persisted})

              {:error, :not_running} ->
                throw({:endurant_halt, :not_running})
            end
        end

      :already_resolved ->
        :already_resolved

      {:error, :not_running} ->
        throw({:endurant_halt, :not_running})
    end
  end

  @spec do_wait_for_child_resume(map(), Handle.t(), integer()) :: term() | no_return()
  defp do_wait_for_child_resume(runtime, %Handle{} = handle, wait_started_at) do
    case await_resume() do
      :ok ->
        refreshed_runtime = refresh_child_runtime(runtime)

        case child_state(refreshed_runtime, handle.child_key) do
          {:ok, state, next_runtime} ->
            Workflow.put_runtime(next_runtime)
            notify_ready(next_runtime)

            emit_child(next_runtime, merge_handle(handle, state), :resumed, %{
              count: 1,
              wait_duration_ms: Telemetry.duration_ms(wait_started_at)
            })

            resolve_or_wait!(next_runtime, handle, state)

          :missing ->
            do_wait_for_child_resume(refreshed_runtime, handle, wait_started_at)
        end

      {:error, :cancelled} ->
        throw({:endurant_halt, :not_running})
    end
  end

  @spec child_state(map(), String.t()) :: {:ok, map(), map()} | :missing
  defp child_state(runtime, child_key) do
    case Map.get(runtime, :child_states, %{}) |> Map.fetch(child_key) do
      {:ok, state} -> {:ok, state, runtime}
      :error -> :missing
    end
  end

  @spec resolve_or_wait!(map(), Handle.t(), map()) :: term() | no_return()
  defp resolve_or_wait!(_runtime, _handle, %{status: :completed, result: result}), do: result

  defp resolve_or_wait!(_runtime, handle, %{status: :failed, error: error}) do
    raise RuntimeError, "child workflow #{inspect(handle.child_key)} failed: #{inspect(error)}"
  end

  defp resolve_or_wait!(_runtime, handle, %{status: :cancelled}) do
    raise RuntimeError, "child workflow #{inspect(handle.child_key)} was cancelled"
  end

  defp resolve_or_wait!(runtime, handle, %{status: :started} = state) do
    wait_for_child_resume(runtime, merge_handle(handle, state))
  end

  @spec ensure_child_runtime(map()) :: map()
  defp ensure_child_runtime(runtime) do
    runtime
    |> Map.put_new(:child_states, %{})
    |> Map.put_new(:loaded_child_seq, 0)
  end

  @spec refresh_child_runtime(map()) :: map()
  defp refresh_child_runtime(runtime) do
    loaded_child_seq = Map.get(runtime, :loaded_child_seq, 0)
    events = Events.list_after(runtime.execution_id, loaded_child_seq, runtime.opts)

    Enum.reduce(events, runtime, fn event, acc ->
      seq =
        case event do
          %{sequence: value} when is_integer(value) and value > 0 -> value
          _ -> Map.get(acc, :loaded_child_seq, 0)
        end

      acc = %{acc | loaded_child_seq: max(Map.get(acc, :loaded_child_seq, 0), seq)}

      case parse_child_event(event) do
        nil ->
          acc

        {child_key, state} ->
          existing = Map.get(Map.get(acc, :child_states, %{}), child_key, %{})
          merged = Map.merge(existing, state)
          %{acc | child_states: Map.put(Map.get(acc, :child_states, %{}), child_key, merged)}
      end
    end)
  end

  @spec parse_child_event(Events.event()) :: {String.t(), map()} | nil
  defp parse_child_event(%{type: :child_execution_started, payload: payload}) do
    with child_key when is_binary(child_key) <- payload_value(payload, "child_key") do
      {child_key,
       %{
         status: :started,
         child_execution_id: payload_value(payload, "child_execution_id"),
         child_unique_id: payload_value(payload, "child_unique_id"),
         child_workflow_name: payload_value(payload, "child_workflow_name"),
         child_version: payload_value(payload, "child_version"),
         parent_close_policy: payload_value(payload, "parent_close_policy")
       }}
    else
      _ -> nil
    end
  end

  defp parse_child_event(%{type: :child_execution_completed, payload: payload}) do
    with child_key when is_binary(child_key) <- payload_value(payload, "child_key") do
      {child_key,
       %{
         status: :completed,
         result: payload_value(payload, "result"),
         child_execution_id: payload_value(payload, "child_execution_id"),
         child_unique_id: payload_value(payload, "child_unique_id")
       }}
    else
      _ -> nil
    end
  end

  defp parse_child_event(%{type: :child_execution_failed, payload: payload}) do
    with child_key when is_binary(child_key) <- payload_value(payload, "child_key") do
      {child_key,
       %{
         status: :failed,
         error: payload_value(payload, "error"),
         child_execution_id: payload_value(payload, "child_execution_id"),
         child_unique_id: payload_value(payload, "child_unique_id")
       }}
    else
      _ -> nil
    end
  end

  defp parse_child_event(%{type: :child_execution_cancelled, payload: payload}) do
    with child_key when is_binary(child_key) <- payload_value(payload, "child_key") do
      {child_key,
       %{
         status: :cancelled,
         child_execution_id: payload_value(payload, "child_execution_id"),
         child_unique_id: payload_value(payload, "child_unique_id")
       }}
    else
      _ -> nil
    end
  end

  defp parse_child_event(_event), do: nil

  @spec start_child_execution(map(), String.t(), module(), map(), keyword()) :: Handle.t()
  defp start_child_execution(runtime, child_key, workflow_module, input, opts) do
    repo = repo!(runtime.opts)
    prefix = Keyword.get(runtime.opts, :prefix, "public")
    workflow = workflow_module.__workflow__()
    close_policy = normalize_close_policy!(Keyword.get(opts, :close_policy, :abandon))
    child_unique_id = resolve_child_unique_id!(workflow, input, opts)
    child_version = resolve_child_version!(workflow, opts)
    child_workflow_name = inspect(workflow_module)
    child_execution_id = Ecto.UUID.generate()

    metadata = %{
      "child_workflow" => %{
        "parent_execution_id" => runtime.execution_id,
        "parent_child_key" => child_key,
        "parent_close_policy" => close_policy,
        "child_first_execution_id" => child_execution_id
      }
    }

    lock_sql = """
    SELECT 1
    FROM #{prefix}.endurant_executions
    WHERE id = $1
    AND status = 'running'::#{prefix}.endurant_execution_status
    AND locked_by = $2
    AND locked_until IS NOT NULL
    AND locked_until > timezone('UTC', now())
    FOR UPDATE
    """

    case repo.transaction(
           fn ->
             case repo.query!(lock_sql, [to_db_id(runtime.execution_id), runtime.worker_id],
                    log: false
                  ).rows do
               [[1]] ->
                 case Executions.insert_in_tx(
                        workflow_module,
                        input,
                        [
                          execution_id: child_execution_id,
                          unique_id: child_unique_id,
                          version: child_version,
                          metadata: metadata
                        ] ++ runtime.opts
                      ) do
                   {:ok, child_execution} ->
                     Events.append(
                       runtime.execution_id,
                       :child_execution_started,
                       %{
                         child_key: child_key,
                         child_workflow_name: child_workflow_name,
                         child_unique_id: child_unique_id,
                         child_execution_id: child_execution.id,
                         child_first_execution_id: child_execution_id,
                         child_version: child_version,
                         parent_close_policy: close_policy
                       },
                       runtime.opts
                     )

                     %Handle{
                       child_key: child_key,
                       child_execution_id: child_execution.id,
                       child_unique_id: child_unique_id,
                       child_workflow_name: child_workflow_name,
                       child_version: child_version,
                       parent_close_policy: close_policy
                     }

                   {:error, :unique_conflict} ->
                     repo.rollback({:child_unique_conflict, child_unique_id})
                 end

               _ ->
                 repo.rollback(:not_running)
             end
           end,
           log: false
         ) do
      {:ok, handle} ->
        emit_child(runtime, handle, :started, %{count: 1})
        handle

      {:error, :not_running} ->
        throw({:endurant_halt, :not_running})

      {:error, reason} ->
        raise RuntimeError,
              "failed to start child workflow #{inspect(child_key)}: #{inspect(reason)}"
    end
  end

  @spec child_handle(String.t(), map()) :: Handle.t()
  defp child_handle(child_key, state) do
    %Handle{
      child_key: child_key,
      child_execution_id: Map.get(state, :child_execution_id),
      child_unique_id: Map.get(state, :child_unique_id),
      child_workflow_name: Map.get(state, :child_workflow_name),
      child_version: Map.get(state, :child_version),
      parent_close_policy: Map.get(state, :parent_close_policy)
    }
  end

  @spec merge_handle(Handle.t(), map()) :: Handle.t()
  defp merge_handle(%Handle{} = handle, state) do
    %Handle{
      handle
      | child_execution_id: Map.get(state, :child_execution_id, handle.child_execution_id),
        child_unique_id: Map.get(state, :child_unique_id, handle.child_unique_id),
        child_workflow_name: Map.get(state, :child_workflow_name, handle.child_workflow_name),
        child_version: Map.get(state, :child_version, handle.child_version),
        parent_close_policy: Map.get(state, :parent_close_policy, handle.parent_close_policy)
    }
  end

  @spec normalize_child_key!(term()) :: String.t()
  defp normalize_child_key!(name) when is_binary(name), do: name

  defp normalize_child_key!(name) do
    raise ArgumentError, "child workflow name must be a string, got: #{inspect(name)}"
  end

  @spec normalize_close_policy!(term()) :: String.t()
  defp normalize_close_policy!(:abandon), do: "abandon"
  defp normalize_close_policy!("abandon"), do: "abandon"
  defp normalize_close_policy!(:request_cancel), do: "request_cancel"
  defp normalize_close_policy!("request_cancel"), do: "request_cancel"

  defp normalize_close_policy!(other) do
    raise ArgumentError,
          "child workflow :close_policy must be :abandon or :request_cancel, got: #{inspect(other)}"
  end

  @spec resolve_child_unique_id!(map(), map(), keyword()) :: String.t()
  defp resolve_child_unique_id!(workflow, input, opts) do
    case Keyword.get(opts, :unique_id) do
      nil -> resolve_unique_id(workflow, input)
      unique_id -> normalize_child_option_string!(:unique_id, unique_id)
    end
  end

  @spec resolve_child_version!(map(), keyword()) :: String.t()
  defp resolve_child_version!(workflow, opts) do
    case Keyword.get(opts, :version) do
      nil -> Map.get(workflow, :version, "1")
      version -> normalize_child_option_string!(:version, version)
    end
  end

  @spec normalize_child_option_string!(atom(), term()) :: String.t()
  defp normalize_child_option_string!(_name, value)
       when is_binary(value) and byte_size(value) > 0 do
    value
  end

  defp normalize_child_option_string!(name, value) do
    raise ArgumentError,
          "child workflow #{inspect(name)} must be a non-empty string, got: #{inspect(value)}"
  end

  @spec resolve_unique_id(map(), map()) :: String.t()
  defp resolve_unique_id(workflow, input) do
    case Map.get(workflow, :unique_id) do
      fun when is_function(fun, 1) ->
        case fun.(input) do
          value when is_binary(value) -> value
          other -> raise ArgumentError, "invalid child workflow unique_id: #{inspect(other)}"
        end

      value when is_binary(value) ->
        value

      other ->
        raise ArgumentError, "invalid child workflow unique_id: #{inspect(other)}"
    end
  end

  @spec payload_value(map(), String.t()) :: term()
  defp payload_value(payload, key) do
    Map.get(payload, key) || Map.get(payload, String.to_atom(key))
  end

  @spec notify_waiting(map()) :: :park | :release
  defp notify_waiting(runtime) do
    manager = queue_manager!(runtime)
    GenServer.call(manager, {:executor_parked, self(), runtime.execution_id}, 5_000)
  end

  @spec notify_ready(map()) :: :ok
  defp notify_ready(runtime) do
    manager = queue_manager!(runtime)
    _ = GenServer.call(manager, {:executor_ready, self(), runtime.execution_id}, 5_000)
    :ok
  end

  @spec queue_manager!(map()) :: pid() | atom() | {:via, module(), term()}
  defp queue_manager!(runtime) do
    case Keyword.fetch(runtime.opts, :queue_manager) do
      {:ok, manager} -> manager
      :error -> raise ArgumentError, "missing required :queue_manager option"
    end
  end

  @spec await_resume() :: :ok | {:error, :cancelled}
  defp await_resume do
    receive do
      :resume -> :ok
      :heartbeat_cancelled -> {:error, :cancelled}
      :heartbeat_lock_lost -> {:error, :cancelled}
      {:heartbeat_failed, reason} -> raise "heartbeat failed while waiting: #{inspect(reason)}"
    end
  end

  @spec emit_child(map(), Handle.t(), atom(), map(), map()) :: :ok
  defp emit_child(runtime, %Handle{} = handle, event, measurements, extra_metadata \\ %{}) do
    Telemetry.emit(
      [:child, event],
      measurements,
      child_telemetry_metadata(runtime, handle, extra_metadata)
    )
  end

  @spec child_telemetry_metadata(map(), Handle.t(), map()) :: map()
  defp child_telemetry_metadata(runtime, %Handle{} = handle, extra_metadata) do
    Map.merge(
      %{
        instance: Map.get(runtime, :instance),
        node: node(),
        queue: Map.get(runtime, :queue),
        workflow: Map.get(runtime, :workflow),
        version: Map.get(runtime, :version),
        child_workflow: handle.child_workflow_name,
        child_version: handle.child_version,
        close_policy: handle.parent_close_policy
      },
      extra_metadata
    )
  end

  @spec repo!(keyword()) :: module()
  defp repo!(opts) do
    Keyword.get_lazy(opts, :repo, fn -> Application.fetch_env!(:endurant, :repo) end)
  end

  @spec to_db_id(binary()) :: binary()
  defp to_db_id(id) do
    case Ecto.UUID.dump(id) do
      {:ok, dumped} -> dumped
      :error -> raise ArgumentError, "invalid UUID: #{inspect(id)}"
    end
  end
end
