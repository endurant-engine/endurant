defmodule Endurant.Integration.InsertMultiTest do
  use Endurant.TestSupport.IntegrationCase

  defmodule MultiWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      queue("orders")
      unique_id(fn %{"id" => id} -> "insert-multi:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      %{id: input["id"], ok: true}
    end
  end

  test "insert composes with prior multi changes for a named instance", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    repo = Keyword.fetch!(runtime_opts, :repo)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:seed, fn _repo, _changes -> {:ok, "multi-1"} end)
      |> Endurant.insert(
        :execution,
        MultiWorkflow,
        fn %{seed: seed} -> %{"id" => seed} end,
        instance: engine_name
      )

    assert {:ok, %{seed: "multi-1", execution: execution}} = repo.transaction(multi)

    assert {:ok, %{status: :completed, result: %{id: "multi-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 5_000, runtime_opts)
  end

  test "insert rolls back execution and event writes when later multi steps fail", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  } do
    repo = Keyword.fetch!(runtime_opts, :repo)

    multi =
      Ecto.Multi.new()
      |> Endurant.insert(:execution, MultiWorkflow, %{"id" => "rolled-back"},
        instance: engine_name
      )
      |> Ecto.Multi.run(:boom, fn _repo, _changes -> {:error, :boom} end)

    assert {:error, :boom, :boom, %{execution: execution}} = repo.transaction(multi)

    assert nil == Endurant.execution(execution.id, instance: engine_name)
    assert [] == Endurant.events(execution.id, instance: engine_name)
  end
end
