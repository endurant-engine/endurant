defmodule Endurant.Integration.RetryPolicyTest do
  use Endurant.TestSupport.IntegrationCase

  defmodule RetryProbe do
    use Agent

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      Agent.start_link(fn -> %{} end, name: name)
    end

    @spec reset() :: :ok
    def reset do
      Agent.update(__MODULE__, fn _ -> %{} end)
    end

    @spec fail_always(term()) :: no_return()
    def fail_always(_key), do: raise("always fail")

    @spec fail_once(term()) :: no_return()
    def fail_once(_key), do: raise("fail once")

    @spec fail_twice_then_ok(term()) :: map() | no_return()
    def fail_twice_then_ok(key) do
      attempt =
        Agent.get_and_update(__MODULE__, fn state ->
          current = Map.get(state, key, 0) + 1
          {current, Map.put(state, key, current)}
        end)

      if attempt <= 2 do
        raise("fail twice")
      else
        %{ok: true, key: key}
      end
    end
  end

  setup do
    {:ok, _pid} = start_supervised({RetryProbe, name: RetryProbe})
    :ok = RetryProbe.reset()
    :ok
  end

  test "max attempts exhausted marks execution failed", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.RetryPolicyTest.ExhaustedWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "retry-exhausted:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "failing_step",
              fn _ ->
                Endurant.Integration.RetryPolicyTest.RetryProbe.fail_always(input["id"])
              end,
              retry: [max_attempts: 2, backoff: :constant, base_ms: 10]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.RetryPolicyTest.ExhaustedWorkflow,
               %{id: "re-1"},
               runtime_opts
             )

    assert {:ok, %{status: :failed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_failed)) == 2
    assert Enum.any?(events, &(&1.type == :execution_failed))
  end

  test "max attempts controls retry count", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.RetryPolicyTest.RetryableFalseWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "retry-max-attempts:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "failing_step",
              fn _ ->
                Endurant.Integration.RetryPolicyTest.RetryProbe.fail_once(input["id"])
              end,
              retry: [
                max_attempts: 5,
                backoff: :constant,
                base_ms: 10
              ]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.RetryPolicyTest.RetryableFalseWorkflow,
               %{id: "rf-1"},
               runtime_opts
             )

    assert {:ok, %{status: :failed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)
    assert Enum.count(events, &(&1.type == :task_failed)) == 5
  end

  test "exponential backoff emits increasing retry waits", %{runtime_opts: runtime_opts} do
    workflow_module =
      quote do
        defmodule Endurant.Integration.RetryPolicyTest.ExponentialWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "retry-exponential:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            task(
              nil,
              "unstable_step",
              fn _ ->
                Endurant.Integration.RetryPolicyTest.RetryProbe.fail_twice_then_ok(input["id"])
              end,
              retry: [max_attempts: 3, backoff: :exponential, base_ms: 40, max_ms: 1_000]
            )
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.RetryPolicyTest.ExponentialWorkflow,
               %{id: "rx-1"},
               runtime_opts
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 8_000, runtime_opts)

    {:ok, events} = PostgresHelper.history(execution.id, runtime_opts)

    retry_wait_delays =
      events
      |> Enum.filter(&(&1.type == :execution_waiting))
      |> Enum.filter(fn event ->
        wait_key = event.payload["wait_key"] || event.payload[:wait_key]
        is_binary(wait_key) and String.starts_with?(wait_key, "unstable_step:")
      end)
      |> Enum.map(fn event -> event.payload["delay_ms"] || event.payload[:delay_ms] end)

    assert retry_wait_delays == [40, 80]
  end
end
