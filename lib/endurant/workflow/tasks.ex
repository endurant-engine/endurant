defmodule Endurant.Workflow.Tasks do
  @moduledoc false

  alias Endurant.Events
  alias Endurant.Workflow
  alias Endurant.Workflow.AsyncHandle

  @async_handles_key :endurant_workflow_async_handles
  @async_outcomes_key :endurant_workflow_async_outcomes

  # Sync
  @doc """
  Runs a deterministic task and returns its result.

  If a result for `name` already exists in workflow runtime state, it is returned without
  re-running `fun`.

  ## Options

    * `:retry` - keyword options:
      * `:max_attempts` (default `1`)
      * `:backoff` (`:constant` or `:exponential`, default `:constant`)
      * `:base_ms` (default `100`)
      * `:max_ms` (default `30_000`)

  ## Example

      user = task(input, "fetch_user", fn i -> Accounts.get_user!(i.user_id) end)
  """
  @spec task(term(), String.t(), (term() -> term())) :: term()
  @spec task(term(), String.t(), (term() -> term()), keyword()) :: term()
  def task(input, name, fun, opts \\ []) when is_function(fun, 1) and is_list(opts) do
    do_task(name, fn -> fun.(input) end, opts)
  end

  @spec do_task(String.t(), (-> term()), keyword()) :: term()
  defp do_task(name, fun, opts) when is_function(fun, 0) and is_list(opts) do
    runtime = Workflow.runtime!()
    task_key = normalize_task_name!(name)
    task_failures = runtime.task_failures || %{}
    attempts = Map.get(task_failures, task_key, 0)
    max_attempts = max_attempts(opts)

    case Map.fetch(runtime.task_results, task_key) do
      {:ok, result} ->
        result

      :error ->
        if attempts >= max_attempts do
          raise RuntimeError,
                "task #{inspect(task_key)} exceeded max attempts (#{attempts}/#{max_attempts})"
        end

        :ok = append_task_event(runtime, :task_started, %{task: task_key})

        try do
          result = fun.()

          :ok =
            append_task_event(runtime, :task_completed, %{task: task_key, result: result})

          current_runtime = Workflow.runtime!()

          Workflow.put_runtime(%{
            current_runtime
            | task_results: Map.put(current_runtime.task_results, task_key, result)
          })

          result
        rescue
          error ->
            :ok =
              append_task_event(runtime, :task_failed, %{
                task: task_key,
                error: format_error(error)
              })

            current_runtime = Workflow.runtime!()
            current_failures = current_runtime.task_failures || %{}
            attempt = Map.get(current_failures, task_key, 0) + 1

            Workflow.put_runtime(%{
              current_runtime
              | task_failures: Map.put(current_failures, task_key, attempt)
            })

            retry_or_reraise(name, task_key, attempt, opts, error, __STACKTRACE__, fun)
        catch
          :throw, {:endurant_halt, :not_running} ->
            throw({:endurant_halt, :not_running})

          kind, reason ->
            :ok =
              append_task_event(runtime, :task_failed, %{
                task: task_key,
                error: %{kind: kind, reason: inspect(reason)}
              })

            current_runtime = Workflow.runtime!()
            current_failures = current_runtime.task_failures || %{}
            attempt = Map.get(current_failures, task_key, 0) + 1

            Workflow.put_runtime(%{
              current_runtime
              | task_failures: Map.put(current_failures, task_key, attempt)
            })

            retry_or_rethrow(name, task_key, attempt, opts, kind, reason, __STACKTRACE__, fun)
        end
    end
  end

  @spec append_task_event(map(), atom(), map()) :: :ok | no_return()
  defp append_task_event(runtime, type, payload) do
    case Events.append_if_running_owned(
           runtime.execution_id,
           runtime.worker_id,
           type,
           payload,
           runtime.opts
         ) do
      :ok ->
        :ok

      {:error, :not_running} ->
        throw({:endurant_halt, :not_running})
    end
  end

  @spec retry_or_reraise(
          term(),
          String.t(),
          pos_integer(),
          keyword(),
          Exception.t(),
          list(),
          (-> term())
        ) ::
          no_return() | term()
  defp retry_or_reraise(name, task_key, attempt, opts, error, stacktrace, fun) do
    if retry?(attempt, opts, error) do
      retry_delay = retry_delay_ms(attempt, opts)
      retry_wait_key = "#{task_key}:#{attempt}"
      Workflow.sleep(retry_wait_key, retry_delay)
      do_task(name, fun, opts)
    else
      reraise(error, stacktrace)
    end
  end

  @spec retry_or_rethrow(
          term(),
          String.t(),
          pos_integer(),
          keyword(),
          atom(),
          term(),
          list(),
          (-> term())
        ) ::
          no_return() | term()
  defp retry_or_rethrow(name, task_key, attempt, opts, kind, reason, stacktrace, fun) do
    if retry?(attempt, opts, reason) do
      retry_delay = retry_delay_ms(attempt, opts)
      retry_wait_key = "#{task_key}:#{attempt}"
      Workflow.sleep(retry_wait_key, retry_delay)
      do_task(name, fun, opts)
    else
      :erlang.raise(kind, reason, stacktrace)
    end
  end

  @spec retry?(pos_integer(), keyword(), term()) :: boolean()
  defp retry?(attempt, opts, _error_or_reason) do
    max_attempts = max_attempts(opts)
    attempt < max_attempts
  end

  # Async
  @doc """
  Starts an asynchronous task and returns a handle.

  Use this to run work in parallel with other tasks. The returned handle is later resolved
  with `task_await/1` or `task_await_many/1`.

  ## Options

    * `:retry` - same retry options as `task/4`

  ## Example

      user_h =
        task_async(input, "fetch_user", fn i ->
          Accounts.get_user!(i.user_id)
        end)

      cart_h =
        task_async(input, "fetch_cart", fn i ->
          Carts.get_cart!(i.cart_id)
        end)

      results = task_await_many([user_h, cart_h])
      user = results["fetch_user"]
      cart = results["fetch_cart"]
  """

  @spec task_async(term(), String.t(), (term() -> term())) :: Workflow.async_handle()
  @spec task_async(term(), String.t(), (term() -> term()), keyword()) :: Workflow.async_handle()
  def task_async(input, name, fun, opts \\ []) when is_function(fun, 1) and is_list(opts) do
    drain_async_messages()
    runtime = Workflow.runtime!()
    task_key = normalize_task_name!(name)
    handles = async_handles()

    case Map.fetch(runtime.task_results, task_key) do
      {:ok, _result} ->
        %AsyncHandle{
          name: name,
          task_key: task_key,
          fun: fun,
          input: input,
          opts: opts,
          task: nil
        }

      :error ->
        case Map.get(handles, task_key) do
          %AsyncHandle{} = handle ->
            handle

          nil ->
            parent = self()
            initial_attempts = Map.get(runtime.task_failures || %{}, task_key, 0)

            task =
              Task.async(fn ->
                run_async_task(
                  parent,
                  runtime.execution_id,
                  runtime.worker_id,
                  task_key,
                  input,
                  fun,
                  opts,
                  initial_attempts,
                  runtime.opts
                )
              end)

            Process.unlink(task.pid)

            handle = %AsyncHandle{
              name: name,
              task_key: task_key,
              fun: fun,
              input: input,
              opts: opts,
              task: task
            }

            put_async_outcomes(Map.delete(async_outcomes(), task_key))
            put_async_handles(Map.put(handles, task_key, handle))
            handle
        end
    end
  end

  @doc """
  Waits for one async handle and returns its result.

  Use this after `task_async/4` when you only need one result.

  ## Example

      user = task_await(user_h)
  """
  @spec task_await(Workflow.async_handle()) :: term()
  def task_await(%AsyncHandle{} = handle) do
    drain_async_messages()
    runtime = Workflow.runtime!()

    case Map.fetch(runtime.task_results, handle.task_key) do
      {:ok, result} ->
        cleanup_handle(handle.task_key)
        result

      :error ->
        await_async_task_completion(handle)
        drain_async_messages()
        resolve_async_outcome!(handle.task_key)
    end
  end

  @doc """
  Waits for multiple async handles and returns `%{task_key => result}`.

  Use this after starting multiple tasks with `task_async/4`.

  ## Example

      results = task_await_many([user_h, cart_h])
      user = results["fetch_user"]
      cart = results["fetch_cart"]
  """
  @spec task_await_many([Workflow.async_handle()]) :: %{String.t() => term()}
  def task_await_many(handles) when is_list(handles) do
    validate_unique_handle_keys!(handles)
    drain_async_messages()

    sorted = Enum.sort_by(handles, & &1.task_key)
    pending = Enum.map(sorted, & &1.task_key) |> MapSet.new()

    wait_for_async_keys(pending)
    drain_async_messages()

    Enum.reduce(sorted, %{}, fn handle, acc ->
      Map.put(acc, handle.task_key, resolve_async_outcome!(handle.task_key))
    end)
  end

  @doc false
  @spec delete_runtime_state() :: :ok
  def delete_runtime_state do
    Process.delete(@async_handles_key)
    Process.delete(@async_outcomes_key)
    :ok
  end

  @spec async_handles() :: %{String.t() => Workflow.async_handle()}
  defp async_handles do
    Process.get(@async_handles_key, %{})
  end

  @spec async_outcomes() :: %{String.t() => {:ok, term()} | {:error, map()}}
  defp async_outcomes do
    Process.get(@async_outcomes_key, %{})
  end

  @spec put_async_outcomes(%{String.t() => {:ok, term()} | {:error, map()}}) :: :ok
  defp put_async_outcomes(outcomes) when is_map(outcomes) do
    Process.put(@async_outcomes_key, outcomes)
    :ok
  end

  @spec put_async_handles(%{String.t() => Workflow.async_handle()}) :: :ok
  defp put_async_handles(handles) when is_map(handles) do
    Process.put(@async_handles_key, handles)
    :ok
  end

  @spec cleanup_handle(String.t()) :: :ok
  defp cleanup_handle(task_key) when is_binary(task_key) do
    handles = async_handles()

    case Map.pop(handles, task_key) do
      {%AsyncHandle{task: %Task{} = task}, rest} ->
        _ = Task.shutdown(task, :brutal_kill)
        put_async_handles(rest)

      {_existing, rest} ->
        put_async_handles(rest)
    end

    :ok
  end

  @spec await_async_task_completion(Workflow.async_handle()) :: :ok
  defp await_async_task_completion(%AsyncHandle{task: nil}), do: :ok

  defp await_async_task_completion(%AsyncHandle{} = handle) do
    _ = Task.await(handle.task, :infinity)
    :ok
  end

  @spec drain_async_messages() :: :ok
  defp drain_async_messages do
    receive do
      message when is_tuple(message) ->
        handle_async_message(message)
        drain_async_messages()
    after
      0 ->
        :ok
    end
  end

  @spec wait_for_async_keys(MapSet.t(String.t())) :: :ok
  defp wait_for_async_keys(pending) do
    pending =
      Enum.reject(pending, &async_resolved?/1)
      |> MapSet.new()

    if MapSet.size(pending) == 0 do
      :ok
    else
      receive do
        message when is_tuple(message) ->
          handle_async_message(message)
          wait_for_async_keys(pending)
      end
    end
  end

  @spec async_resolved?(String.t()) :: boolean()
  defp async_resolved?(task_key) do
    runtime = Workflow.runtime!()

    Map.has_key?(runtime.task_results, task_key) or
      Map.has_key?(async_outcomes(), task_key)
  end

  @spec handle_async_message(tuple()) :: :ok
  defp handle_async_message({:endurant_async_attempt_failed, task_key}) do
    runtime = Workflow.runtime!()
    task_failures = runtime.task_failures || %{}

    Workflow.put_runtime(%{
      runtime
      | task_failures: Map.update(task_failures, task_key, 1, &(&1 + 1))
    })

    :ok
  end

  defp handle_async_message({:endurant_async_done, task_key, {:ok, result}}) do
    runtime = Workflow.runtime!()

    Workflow.put_runtime(%{
      runtime
      | task_results: Map.put(runtime.task_results, task_key, result)
    })

    put_async_outcomes(Map.put(async_outcomes(), task_key, {:ok, result}))
    :ok
  end

  defp handle_async_message({:endurant_async_done, task_key, {:error, reason}}) do
    put_async_outcomes(Map.put(async_outcomes(), task_key, {:error, reason}))
    :ok
  end

  defp handle_async_message(_message), do: :ok

  @spec resolve_async_outcome!(String.t()) :: term() | no_return()
  defp resolve_async_outcome!(task_key) do
    runtime = Workflow.runtime!()

    case Map.fetch(runtime.task_results, task_key) do
      {:ok, result} ->
        cleanup_handle(task_key)
        result

      :error ->
        case Map.get(async_outcomes(), task_key) do
          {:ok, result} ->
            cleanup_handle(task_key)
            result

          {:error, %{kind: :not_running}} ->
            cleanup_handle(task_key)
            throw({:endurant_halt, :not_running})

          {:error, reason} ->
            cleanup_handle(task_key)
            raise_async_reason(reason)

          nil ->
            raise RuntimeError, "missing async outcome for task #{inspect(task_key)}"
        end
    end
  end

  @spec raise_async_reason(map()) :: no_return()
  defp raise_async_reason(%{kind: :exception, module: module, message: message})
       when is_atom(module) and is_binary(message) do
    raise module, message: message
  end

  defp raise_async_reason(%{kind: kind, reason: reason}) do
    raise RuntimeError, "async task failed (#{inspect(kind)}): #{inspect(reason)}"
  end

  defp raise_async_reason(other) do
    raise RuntimeError, "async task failed: #{inspect(other)}"
  end

  @spec run_async_task(
          pid(),
          binary(),
          String.t(),
          String.t(),
          term(),
          (term() -> term()),
          keyword(),
          non_neg_integer(),
          keyword()
        ) :: :ok
  defp run_async_task(
         owner,
         execution_id,
         worker_id,
         task_key,
         input,
         fun,
         task_opts,
         attempts,
         runtime_opts
       ) do
    max = max_attempts(task_opts)

    if attempts >= max do
      send(owner, {:endurant_async_done, task_key, {:error, %{kind: :retry_exhausted}}})
      :ok
    else
      run_async_task_attempt(
        owner,
        execution_id,
        worker_id,
        task_key,
        input,
        fun,
        task_opts,
        attempts,
        runtime_opts
      )
    end
  end

  @spec run_async_task_attempt(
          pid(),
          binary(),
          String.t(),
          String.t(),
          term(),
          (term() -> term()),
          keyword(),
          non_neg_integer(),
          keyword()
        ) :: :ok
  defp run_async_task_attempt(
         owner,
         execution_id,
         worker_id,
         task_key,
         input,
         fun,
         task_opts,
         attempts,
         runtime_opts
       ) do
    case Events.append_if_running_owned(
           execution_id,
           worker_id,
           :task_started,
           %{task: task_key},
           runtime_opts
         ) do
      {:error, :not_running} ->
        send(owner, {:endurant_async_done, task_key, {:error, %{kind: :not_running}}})
        :ok

      :ok ->
        try do
          result = fun.(input)

          case Events.append_if_running_owned(
                 execution_id,
                 worker_id,
                 :task_completed,
                 %{task: task_key, result: result},
                 runtime_opts
               ) do
            :ok ->
              send(owner, {:endurant_async_done, task_key, {:ok, result}})
              :ok

            {:error, :not_running} ->
              send(owner, {:endurant_async_done, task_key, {:error, %{kind: :not_running}}})
              :ok
          end
        rescue
          error ->
            handle_async_failure(
              owner,
              execution_id,
              worker_id,
              task_key,
              input,
              fun,
              task_opts,
              attempts,
              runtime_opts,
              {:exception, error}
            )
        catch
          kind, reason ->
            handle_async_failure(
              owner,
              execution_id,
              worker_id,
              task_key,
              input,
              fun,
              task_opts,
              attempts,
              runtime_opts,
              {:throw, kind, reason}
            )
        end
    end
  end

  @spec handle_async_failure(
          pid(),
          binary(),
          String.t(),
          String.t(),
          term(),
          (term() -> term()),
          keyword(),
          non_neg_integer(),
          keyword(),
          {:exception, Exception.t()} | {:throw, atom(), term()}
        ) :: :ok
  defp handle_async_failure(
         owner,
         execution_id,
         worker_id,
         task_key,
         input,
         fun,
         task_opts,
         attempts,
         runtime_opts,
         failure
       ) do
    payload =
      case failure do
        {:exception, error} ->
          %{task: task_key, error: format_error(error)}

        {:throw, kind, reason} ->
          %{task: task_key, error: %{kind: kind, reason: inspect(reason)}}
      end

    case Events.append_if_running_owned(
           execution_id,
           worker_id,
           :task_failed,
           payload,
           runtime_opts
         ) do
      {:error, :not_running} ->
        send(owner, {:endurant_async_done, task_key, {:error, %{kind: :not_running}}})
        :ok

      :ok ->
        send(owner, {:endurant_async_attempt_failed, task_key})
        next_attempt = attempts + 1

        if retry?(next_attempt, task_opts, failure) do
          Process.sleep(retry_delay_ms(next_attempt, task_opts))

          run_async_task(
            owner,
            execution_id,
            worker_id,
            task_key,
            input,
            fun,
            task_opts,
            next_attempt,
            runtime_opts
          )
        else
          send(owner, {:endurant_async_done, task_key, {:error, async_failure_reason(failure)}})
          :ok
        end
    end
  end

  @spec async_failure_reason({:exception, Exception.t()} | {:throw, atom(), term()}) :: map()
  defp async_failure_reason({:exception, error}) do
    %{kind: :exception, module: error.__struct__, message: Exception.message(error)}
  end

  defp async_failure_reason({:throw, kind, reason}) do
    %{kind: kind, reason: inspect(reason)}
  end

  # Stream
  @doc """
  Returns a lazy stream of `{task_key, result}`.

  Use this to process many items with bounded parallelism.
  Consume with `Enum.to_list/1`, `Enum.reduce/3`, or similar.

  ## Options

    * `:max_concurrency` - how many tasks may run at once (default: scheduler count)

  ## Warning

  Workflows need to be Idempotence, the order of the Stream can change between runs.
  A changed order should not change the execution of the function.

  ## Example

      task_async_stream(items, fn item -> "item:\#{item.id}" end, fn item ->
        MyApp.process(item)
      end, max_concurrency: 8)
      |> Enum.to_list()
  """
  @spec task_async_stream(Enumerable.t(), (term() -> String.t()), (term() -> term())) ::
          Enumerable.t()
  @spec task_async_stream(Enumerable.t(), (term() -> String.t()), (term() -> term()), keyword()) ::
          Enumerable.t()
  def task_async_stream(items, key_fun, fun, opts \\ [])
      when is_function(key_fun, 1) and is_function(fun, 1) and is_list(opts) do
    max_concurrency =
      positive_integer(Keyword.get(opts, :max_concurrency, System.schedulers_online()))

    task_opts = Keyword.drop(opts, [:max_concurrency])

    entries =
      items
      |> Enum.map(fn item -> {normalize_task_name!(key_fun.(item)), item} end)

    validate_unique_stream_keys!(entries)

    Stream.resource(
      fn ->
        %{
          remaining: entries,
          in_flight: %{},
          max_concurrency: max_concurrency,
          task_opts: task_opts,
          fun: fun
        }
      end,
      &task_async_stream_next/1,
      &task_async_stream_after/1
    )
  end

  @spec validate_unique_handle_keys!([Workflow.async_handle()]) :: :ok
  defp validate_unique_handle_keys!(handles) do
    handles
    |> Enum.map(& &1.task_key)
    |> validate_unique_keys!("task_await_many")
  end

  @spec validate_unique_stream_keys!([{String.t(), term()}]) :: :ok
  defp validate_unique_stream_keys!(entries) do
    entries
    |> Enum.map(fn {key, _item} -> normalize_task_name!(key) end)
    |> validate_unique_keys!("task_async_stream")
  end

  @spec validate_unique_keys!([String.t()], String.t()) :: :ok | no_return()
  defp validate_unique_keys!(keys, context) do
    duplicates =
      keys
      |> Enum.group_by(& &1)
      |> Enum.filter(fn {_key, values} -> length(values) > 1 end)
      |> Enum.map(&elem(&1, 0))

    case duplicates do
      [] ->
        :ok

      _ ->
        raise ArgumentError,
              "#{context} requires unique task keys, duplicates: #{inspect(duplicates)}"
    end
  end

  @spec task_async_stream_next(map()) :: {[{String.t(), term()}], map()} | {:halt, map()}
  defp task_async_stream_next(state) do
    %{
      remaining: remaining,
      in_flight: in_flight,
      max_concurrency: max,
      task_opts: task_opts,
      fun: fun
    } =
      state

    {remaining, in_flight} =
      fill_stream_slots(remaining, in_flight, max, task_opts, fun)

    if map_size(in_flight) == 0 do
      {:halt, %{state | remaining: remaining, in_flight: in_flight}}
    else
      case wait_for_any_async_key(Map.keys(in_flight)) do
        {:ok, task_key} ->
          result = resolve_async_outcome!(task_key)

          {[{task_key, result}],
           %{state | remaining: remaining, in_flight: Map.delete(in_flight, task_key)}}

        {:error, :empty} ->
          {:halt, %{state | remaining: remaining, in_flight: in_flight}}
      end
    end
  end

  @spec task_async_stream_after(map()) :: :ok
  defp task_async_stream_after(%{in_flight: in_flight}) do
    Enum.each(Map.keys(in_flight), &cleanup_handle/1)
    :ok
  end

  @spec fill_stream_slots(
          [{String.t(), term()}],
          %{String.t() => Workflow.async_handle()},
          pos_integer(),
          keyword(),
          (term() -> term())
        ) :: {[{String.t(), term()}], %{String.t() => Workflow.async_handle()}}
  defp fill_stream_slots(entries, in_flight, max_concurrency, task_opts, fun) do
    available = max(max_concurrency - map_size(in_flight), 0)
    {to_start, rest} = Enum.split(entries, available)

    in_flight =
      Enum.reduce(to_start, in_flight, fn {key, item}, acc ->
        Map.put(acc, key, task_async(item, key, fun, task_opts))
      end)

    {rest, in_flight}
  end

  @spec wait_for_any_async_key([String.t()]) :: {:ok, String.t()} | {:error, :empty}
  defp wait_for_any_async_key([]), do: {:error, :empty}

  defp wait_for_any_async_key(task_keys) do
    drain_async_messages()

    case Enum.find(task_keys, &async_resolved?/1) do
      nil ->
        receive do
          message when is_tuple(message) ->
            handle_async_message(message)
            wait_for_any_async_key(task_keys)
        end

      key ->
        {:ok, key}
    end
  end

  # Shared helpers
  @spec max_attempts(keyword()) :: pos_integer()
  defp max_attempts(opts) do
    retry_opts = Keyword.get(opts, :retry, [])

    case Keyword.get(retry_opts, :max_attempts, 1) do
      value when is_integer(value) and value > 0 -> value
      _ -> 1
    end
  end

  @spec retry_delay_ms(pos_integer(), keyword()) :: pos_integer()
  defp retry_delay_ms(attempt, opts) do
    retry_opts = Keyword.get(opts, :retry, [])
    base = positive_integer(Keyword.get(retry_opts, :base_ms, 100))
    max_delay = positive_integer(Keyword.get(retry_opts, :max_ms, 30_000))
    backoff = Keyword.get(retry_opts, :backoff, :constant)

    delay =
      case backoff do
        :exponential ->
          base * trunc(:math.pow(2, max(attempt - 1, 0)))

        _ ->
          base
      end

    min(delay, max_delay)
  end

  @spec positive_integer(term()) :: pos_integer()
  defp positive_integer(value) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value), do: 1

  @spec format_error(Exception.t()) :: map()
  defp format_error(error) do
    %{module: inspect(error.__struct__), message: Exception.message(error)}
  end

  @spec normalize_task_name!(term()) :: String.t()
  defp normalize_task_name!(name) when is_binary(name), do: name

  defp normalize_task_name!(name) do
    raise ArgumentError, "task name must be a string, got: #{inspect(name)}"
  end
end
