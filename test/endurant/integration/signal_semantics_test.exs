defmodule Endurant.Integration.SignalSemanticsTest do
  use Endurant.TestSupport.IntegrationCase

  test "signals sent after wait starts are consumed in order", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.SignalSemanticsTest.AfterWaitWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal-after-wait:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            first = wait_signal("approval_requested")
            second = wait_signal("approval_requested")
            %{id: input["id"], approvals: [first, second]}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.SignalSemanticsTest.AfterWaitWorkflow,
               %{id: "aw-1"},
               runtime_opts
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{n: 1},
               runtime_opts
             )

    assert :ok =
             Endurant.signal(
               execution.id,
               "approval_requested",
               %{n: 2},
               runtime_opts
             )

    assert {:ok, %{status: :completed, result: result}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    assert result == %{id: "aw-1", approvals: [%{n: 1}, %{n: 2}]}
  end

  test "signal to completed execution returns not_active and is not appended", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.SignalSemanticsTest.CompletedSignalWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal-completed:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{id: input["id"], done: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.SignalSemanticsTest.CompletedSignalWorkflow,
               %{id: "sc-1"},
               runtime_opts
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)

    assert {:error, :not_active} =
             Endurant.signal(execution.id, "after_done", %{n: 1}, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    refute Enum.any?(events, &(&1.type == :signal_received))
  end

  test "does not spam execution_waiting for repeated signal waits", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.SignalSemanticsTest.WaitEventSpamWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal-wait-spam:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            first = wait_signal("user_message")

            task(nil, "assistant_reply:1", fn _ ->
              text = Map.get(first, :text) || Map.get(first, "text") || ""
              %{"content" => text}
            end)

            second = wait_signal("user_message")
            %{id: input["id"], first: first, second: second}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.SignalSemanticsTest.WaitEventSpamWorkflow,
               %{id: "spam-1"},
               runtime_opts
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "user_message",
               %{text: "Hi"},
               runtime_opts
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)
    Process.sleep(300)

    {:ok, mid_events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(mid_events, &(&1.type == :execution_waiting)) == 2
    assert Enum.count(mid_events, &(&1.type == :task_started)) == 1
    assert Enum.count(mid_events, &(&1.type == :task_completed)) == 1

    assert :ok =
             Endurant.signal(
               execution.id,
               "user_message",
               %{text: "Again"},
               runtime_opts
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, final_events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(final_events, &(&1.type == :execution_waiting)) == 2
  end

  test "crash during task then restart resumes and completes with queued signal", %{
    runtime_opts: runtime_opts,
    engine_name: engine_name
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.SignalSemanticsTest.CrashDuringTaskWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "signal-crash-resume:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            payload = wait_signal("user_message")

            task(nil, "assistant_reply:1", fn _ ->
              Process.sleep(5_000)
              %{content: payload["text"] || payload[:text] || ""}
            end)

            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.SignalSemanticsTest.CrashDuringTaskWorkflow,
               %{id: "crash-1"},
               runtime_opts
             )

    assert :waiting = wait_for_status(execution.id, :waiting, 2_000, runtime_opts)

    assert :ok =
             Endurant.signal(
               execution.id,
               "user_message",
               %{text: "resume me"},
               runtime_opts
             )

    assert :ok =
             wait_for_event(
               execution.id,
               fn event ->
                 event.type == :task_started and
                   payload_value(event.payload, "task") == "assistant_reply:1"
               end,
               2_000,
               runtime_opts
             )

    pid = :global.whereis_name({:endurant_supervisor, engine_name})

    if is_pid(pid) do
      Process.exit(pid, :kill)
      Process.sleep(100)
    else
      flunk("engine supervisor #{inspect(engine_name)} not found")
    end

    force_lock_expired!(execution.id, runtime_opts)

    assert :ok = wait_for_process_up(engine_name, 2_000)

    assert {:ok, %{status: :completed, result: %{id: "crash-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 10_000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.any?(events, &(&1.type == :execution_abandoned))
    assert Enum.any?(events, &(&1.type == :execution_resumed))
    assert Enum.count(events, &(&1.type == :task_started)) == 2
    assert Enum.count(events, &(&1.type == :task_completed)) == 1
  end

  @spec wait_for_status(binary(), atom(), pos_integer(), keyword()) :: atom()
  defp wait_for_status(execution_id, expected_status, timeout_ms, runtime_opts) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
  end

  @spec do_wait_for_status(binary(), atom(), integer(), keyword()) :: atom()
  defp do_wait_for_status(execution_id, expected_status, deadline, runtime_opts) do
    case Endurant.execution(execution_id, runtime_opts) do
      %{status: ^expected_status} ->
        expected_status

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("execution #{execution_id} did not reach status #{inspect(expected_status)}")
        else
          Process.sleep(25)
          do_wait_for_status(execution_id, expected_status, deadline, runtime_opts)
        end
    end
  end

  @spec wait_for_event(binary(), (map() -> boolean()), pos_integer(), keyword()) :: :ok
  defp wait_for_event(execution_id, matcher, timeout_ms, runtime_opts)
       when is_binary(execution_id) and is_function(matcher, 1) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_event(execution_id, matcher, deadline, runtime_opts)
  end

  @spec do_wait_for_event(binary(), (map() -> boolean()), integer(), keyword()) :: :ok
  defp do_wait_for_event(execution_id, matcher, deadline, runtime_opts) do
    {:ok, events} = PostgresHelper.history(execution_id, runtime_opts)

    if Enum.any?(events, matcher) do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("event match not found for execution #{execution_id}")
      else
        Process.sleep(25)
        do_wait_for_event(execution_id, matcher, deadline, runtime_opts)
      end
    end
  end

  @spec force_lock_expired!(binary(), keyword()) :: :ok
  defp force_lock_expired!(execution_id, runtime_opts) do
    prefix = Keyword.fetch!(runtime_opts, :prefix)
    repo = Keyword.fetch!(runtime_opts, :repo)
    db_id = Ecto.UUID.dump!(execution_id)

    repo.query!(
      "UPDATE #{prefix}.endurant_executions SET locked_until = timezone('UTC', now()) - interval '1 second', updated_at = timezone('UTC', now()) WHERE id = $1",
      [db_id]
    )

    :ok
  end

  @spec wait_for_process_up(String.t(), pos_integer()) :: :ok
  defp wait_for_process_up(name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_process_up(name, deadline)
  end

  @spec do_wait_for_process_up(String.t(), integer()) :: :ok
  defp do_wait_for_process_up(name, deadline) do
    if is_pid(:global.whereis_name({:endurant_supervisor, name})) do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("process #{inspect(name)} did not come up")
      else
        Process.sleep(25)
        do_wait_for_process_up(name, deadline)
      end
    end
  end

  @spec payload_value(map() | nil, String.t()) :: term()
  defp payload_value(nil, _key), do: nil

  defp payload_value(map, "task") when is_map(map),
    do: Map.get(map, "task") || Map.get(map, :task)

  defp payload_value(map, _key) when is_map(map), do: map

  defp payload_value(_value, _key), do: nil
end
