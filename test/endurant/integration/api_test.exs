defmodule Endurant.Integration.ApiTest do
  use Endurant.TestSupport.IntegrationCase

  test "execution/2 and events/2 work with explicit repo and prefix", %{
    runtime_opts: runtime_opts
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ApiTest.BasicWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "api-basic:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input), do: %{id: input["id"], ok: true}
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ApiTest.BasicWorkflow,
               %{id: "a-1"},
               runtime_opts
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)

    execution_id = execution.id

    assert %{id: ^execution_id, status: :completed} =
             Endurant.execution(execution.id, runtime_opts)

    events = Endurant.events(execution.id, runtime_opts)
    assert is_list(events)
    assert Enum.any?(events, &(&1.type == :execution_created))
    assert Enum.any?(events, &(&1.type == :execution_completed))
  end

  test "execution/2 and events/2 use configured repo fallback when repo option omitted", %{
    prefix: prefix
  } do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ApiTest.RepoFallbackWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "api-fallback:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input), do: %{id: input["id"], ok: true}
        end
      end

    Code.compile_quoted(workflow_module)

    opts = [repo: PostgresHelper.Repo, prefix: prefix]

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ApiTest.RepoFallbackWorkflow,
               %{id: "a-2"},
               opts
             )

    assert {:ok, %{status: :completed}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, opts)

    execution_id = execution.id

    assert %{id: ^execution_id, status: :completed} =
             Endurant.execution(execution.id, prefix: prefix)

    events = Endurant.events(execution.id, prefix: prefix)
    assert Enum.any?(events, &(&1.type == :execution_created))
  end

  test "signal to unknown execution returns not_found", %{runtime_opts: runtime_opts} do
    unknown_execution_id = Ecto.UUID.generate()

    assert {:error, :not_found} =
             Endurant.signal(unknown_execution_id, "unknown", %{}, runtime_opts)
  end
end
