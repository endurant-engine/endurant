defmodule Endurant.Integration.WorkflowErrorTest do
  use Endurant.TestSupport.IntegrationCase

  defmodule FlakyWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      workflow_error_retry(base_ms: 60_000, max_ms: 60_000, backoff: :constant)
      unique_id(fn input -> "workflow-error:#{input[:id] || input["id"]}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      id = input[:id] || input["id"]

      case Application.get_env(:endurant, __MODULE__, :fail) do
        :ok ->
          %{id: id, ok: true}

        _ ->
          raise "workflow orchestration exploded for #{id}"
      end
    end
  end

  defmodule OtherFlakyWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      workflow_error_retry(base_ms: 60_000, max_ms: 60_000, backoff: :constant)
      unique_id(fn input -> "workflow-error-other:#{input[:id] || input["id"]}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      id = input[:id] || input["id"]

      case Application.get_env(:endurant, __MODULE__, :fail) do
        :ok ->
          %{id: id, ok: true}

        _ ->
          raise "other workflow orchestration exploded for #{id}"
      end
    end
  end

  defmodule AutoRetryWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      workflow_error_retry(base_ms: 25, max_ms: 100, backoff: :exponential)
      unique_id(fn input -> "workflow-error-auto:#{input[:id] || input["id"]}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      id = input[:id] || input["id"]

      case Application.get_env(:endurant, __MODULE__, :fail) do
        :ok ->
          %{id: id, ok: true}

        _ ->
          raise "workflow orchestration exploded for #{id}"
      end
    end
  end

  defmodule CappedAutoRetryWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      workflow_error_retry(base_ms: 25, max_ms: 100, max_attempts: 1, backoff: :exponential)
      unique_id(fn input -> "workflow-error-capped:#{input[:id] || input["id"]}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      id = input[:id] || input["id"]

      case Application.get_env(:endurant, __MODULE__, :fail) do
        :ok ->
          %{id: id, ok: true}

        _ ->
          raise "workflow orchestration exploded for #{id}"
      end
    end
  end

  defmodule MultiEpisodeWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      workflow_error_retry(base_ms: 60_000, max_ms: 60_000, backoff: :constant)
      unique_id(fn input -> "workflow-error-multi:#{input[:id] || input["id"]}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      id = input[:id] || input["id"]

      first = wait_signal("first")
      maybe_fail(:first, id)

      second = wait_signal("second")
      maybe_fail(:second, id)

      %{id: id, first: first, second: second, ok: true}
    end

    defp maybe_fail(step, id) do
      case Application.get_env(:endurant, __MODULE__, %{}) do
        %{^step => :fail} ->
          raise "workflow orchestration exploded for #{id} on #{step}"

        _ ->
          :ok
      end
    end
  end

  setup do
    Application.put_env(:endurant, FlakyWorkflow, :fail)
    Application.put_env(:endurant, OtherFlakyWorkflow, :fail)
    Application.put_env(:endurant, AutoRetryWorkflow, :fail)
    Application.put_env(:endurant, CappedAutoRetryWorkflow, :fail)
    Application.put_env(:endurant, MultiEpisodeWorkflow, %{first: :fail, second: :fail})

    on_exit(fn ->
      Application.delete_env(:endurant, FlakyWorkflow)
      Application.delete_env(:endurant, OtherFlakyWorkflow)
      Application.delete_env(:endurant, AutoRetryWorkflow)
      Application.delete_env(:endurant, CappedAutoRetryWorkflow)
      Application.delete_env(:endurant, MultiEpisodeWorkflow)
    end)

    :ok
  end

  test "run/2 failures outside tasks park executions in workflow_error and allow explicit resume",
       %{
         engine_name: engine_name,
         runtime_opts: runtime_opts
       } do
    assert {:ok, execution} =
             Endurant.insert(FlakyWorkflow, %{id: "a-1"}, instance: engine_name)

    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    assert {:error, :unique_conflict} =
             Endurant.insert(FlakyWorkflow, %{id: "a-1"}, instance: engine_name)

    assert {:error, :not_active} =
             Endurant.signal(execution.id, "ignored", %{}, instance: engine_name)

    events = Endurant.events(execution.id, instance: engine_name)
    assert Enum.any?(events, &(&1.type == :execution_workflow_errored))
    refute Enum.any?(events, &(&1.type == :execution_failed))

    Application.put_env(:endurant, FlakyWorkflow, :ok)

    assert :ok = Endurant.resume_workflow_error(execution.id, instance: engine_name)

    assert {:ok, %{status: :completed, result: %{id: "a-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)

    events = Endurant.events(execution.id, instance: engine_name)
    assert 1 == Enum.count(events, &(&1.type == :execution_started))
  end

  test "workflow_error executions retry automatically without appending new start events",
       %{
         engine_name: engine_name,
         runtime_opts: runtime_opts
       } do
    test_pid = self()
    handler_id = "workflow-error-telemetry-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:endurant, :execution, :workflow_errored],
        &__MODULE__.handle_telemetry/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, execution} =
             Endurant.insert(AutoRetryWorkflow, %{id: "auto-1"}, instance: engine_name)

    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    assert_receive {:telemetry, [:endurant, :execution, :workflow_errored], measurements,
                    metadata}

    assert %{count: 1, attempt: 1, retry_delay_ms: 25} = measurements
    assert %{retry_scheduled: true, error_kind: "exception"} = metadata

    Application.put_env(:endurant, AutoRetryWorkflow, :ok)

    assert {:ok, %{status: :completed, result: %{id: "auto-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 10_000, runtime_opts)

    events = Endurant.events(execution.id, instance: engine_name)

    assert 1 == Enum.count(events, &(&1.type == :execution_workflow_errored))
    assert 1 == Enum.count(events, &(&1.type == :execution_started))
    assert 0 == Enum.count(events, &(&1.type == :execution_resumed))
  end

  test "workflow_error max_attempts parks without scheduling another retry", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    assert {:ok, execution} =
             Endurant.insert(CappedAutoRetryWorkflow, %{id: "cap-1"}, instance: engine_name)

    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    Process.sleep(200)

    assert %{status: :workflow_error} = Endurant.execution(execution.id, instance: engine_name)

    events = Endurant.events(execution.id, instance: engine_name)

    assert 1 == Enum.count(events, &(&1.type == :execution_workflow_errored))
    assert 1 == Enum.count(events, &(&1.type == :execution_started))
    assert 0 == Enum.count(events, &(&1.type == :execution_resumed))
  end

  test "workflow_error executions can be cancelled explicitly", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    assert {:ok, execution} =
             Endurant.insert(FlakyWorkflow, %{id: "a-2"}, instance: engine_name)

    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    assert :ok = Endurant.cancel(execution.id, instance: engine_name)
    assert :cancelled = wait_for_status(execution.id, :cancelled, 5_000, runtime_opts)
  end

  test "workflow_error retry state is cleared between distinct error episodes", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    assert {:ok, execution} =
             Endurant.insert(MultiEpisodeWorkflow, %{id: "multi-1"}, instance: engine_name)

    assert :waiting = wait_for_status(execution.id, :waiting, 5_000, runtime_opts)
    assert :ok = Endurant.signal(execution.id, "first", %{step: 1}, instance: engine_name)
    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    assert %{metadata: metadata} = Endurant.execution(execution.id, instance: engine_name)
    assert 1 == workflow_error_attempts_from_metadata(metadata)

    Application.put_env(:endurant, MultiEpisodeWorkflow, %{first: :ok, second: :fail})

    assert :ok = Endurant.resume_workflow_error(execution.id, instance: engine_name)
    assert :waiting = wait_for_status(execution.id, :waiting, 5_000, runtime_opts)

    assert %{metadata: metadata} = Endurant.execution(execution.id, instance: engine_name)
    refute workflow_error_metadata_present?(metadata)

    assert :ok = Endurant.signal(execution.id, "second", %{step: 2}, instance: engine_name)
    assert :workflow_error = wait_for_status(execution.id, :workflow_error, 5_000, runtime_opts)

    assert %{metadata: metadata} = Endurant.execution(execution.id, instance: engine_name)
    assert 1 == workflow_error_attempts_from_metadata(metadata)

    events = Endurant.events(execution.id, instance: engine_name)
    assert 2 == Enum.count(events, &(&1.type == :execution_workflow_errored))

    Application.put_env(:endurant, MultiEpisodeWorkflow, %{first: :ok, second: :ok})

    assert :ok = Endurant.resume_workflow_error(execution.id, instance: engine_name)

    assert {:ok,
            %{status: :completed, result: %{id: "multi-1", first: %{step: 1}, second: %{step: 2}}}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)
  end

  test "batch resume can target a workflow or all workflow_error executions", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    assert {:ok, first} = Endurant.insert(FlakyWorkflow, %{id: "b-1"}, instance: engine_name)
    assert {:ok, second} = Endurant.insert(FlakyWorkflow, %{id: "b-2"}, instance: engine_name)

    assert {:ok, other} =
             Endurant.insert(OtherFlakyWorkflow, %{id: "b-3"}, instance: engine_name)

    assert :workflow_error = wait_for_status(first.id, :workflow_error, 5_000, runtime_opts)
    assert :workflow_error = wait_for_status(second.id, :workflow_error, 5_000, runtime_opts)
    assert :workflow_error = wait_for_status(other.id, :workflow_error, 5_000, runtime_opts)

    Application.put_env(:endurant, FlakyWorkflow, :ok)
    Application.put_env(:endurant, OtherFlakyWorkflow, :ok)

    assert 2 ==
             Endurant.resume_workflow_errors(
               workflow: FlakyWorkflow,
               instance: engine_name
             )

    assert {:ok, %{status: :completed, result: %{id: "b-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(first.id, 5_000, runtime_opts)

    assert {:ok, %{status: :completed, result: %{id: "b-2", ok: true}}} =
             PostgresHelper.wait_for_execution!(second.id, 5_000, runtime_opts)

    first_events = Endurant.events(first.id, instance: engine_name)
    second_events = Endurant.events(second.id, instance: engine_name)

    assert 1 == Enum.count(first_events, &(&1.type == :execution_started))
    assert 1 == Enum.count(second_events, &(&1.type == :execution_started))

    assert %{status: :workflow_error} = Endurant.execution(other.id, instance: engine_name)

    assert 1 == Endurant.resume_workflow_errors(instance: engine_name)

    assert {:ok, %{status: :completed, result: %{id: "b-3", ok: true}}} =
             PostgresHelper.wait_for_execution!(other.id, 5_000, runtime_opts)

    other_events = Endurant.events(other.id, instance: engine_name)
    assert 1 == Enum.count(other_events, &(&1.type == :execution_started))
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(execution_id, instance: Keyword.fetch!(runtime_opts, :instance)) do
      %{status: ^expected_status} ->
        expected_status

      _other ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise ExUnit.AssertionError,
                "execution #{execution_id} did not reach #{inspect(expected_status)} in time"
        else
          Process.sleep(25)
          do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
        end
    end
  end

  @spec workflow_error_attempts_from_metadata(map()) :: non_neg_integer()
  defp workflow_error_attempts_from_metadata(metadata) when is_map(metadata) do
    metadata
    |> workflow_error_metadata_from_metadata()
    |> Map.get("attempts", 0)
  end

  @spec workflow_error_metadata_present?(map()) :: boolean()
  defp workflow_error_metadata_present?(metadata) when is_map(metadata) do
    metadata
    |> workflow_error_metadata_from_metadata()
    |> map_size() > 0
  end

  @spec workflow_error_metadata_from_metadata(map()) :: map()
  defp workflow_error_metadata_from_metadata(metadata) when is_map(metadata) do
    metadata
    |> Map.get("endurant", %{})
    |> Map.get("workflow_error", %{})
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end
end
