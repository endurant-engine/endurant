defmodule Endurant.Integration.ApiTest do
  use Endurant.TestSupport.IntegrationCase

  defmodule PrefixMigration do
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
      Application.fetch_env!(:endurant, :multi_test_prefix)
    end
  end

  defmodule QueueRequiredWorkflow do
    use Endurant.Workflow, version: "1"

    workflow do
      unique_id(fn %{id: id} -> "api-no-queue:#{id}" end)
    end

    @impl Endurant.Workflow
    def run(_version, input) do
      %{id: input["id"], ok: true}
    end
  end

  test("instance-targeted API resolves repo/prefix from local instance config", %{
    engine_name: engine_name,
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ApiTest.InstanceTargetWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "api-instance:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go_#{input["id"]}")
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    assert {:ok, execution} =
             Endurant.insert(Endurant.Integration.ApiTest.InstanceTargetWorkflow, %{id: "inst-1"},
               instance: engine_name
             )

    assert %{status: status} = Endurant.execution(execution.id, instance: engine_name)
    assert status in [:pending, :running, :waiting]
    assert is_list(Endurant.events(execution.id, instance: engine_name))
    assert :ok = Endurant.signal(execution.id, "go_inst-1", %{}, instance: engine_name)

    assert {:ok, %{status: :completed, result: %{id: "inst-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(execution.id, 5000, runtime_opts)
  end

  test("signal to unknown execution returns not_found", %{engine_name: engine_name}) do
    unknown_execution_id = Ecto.UUID.generate()

    assert {:error, :not_found} =
             Endurant.signal(unknown_execution_id, "unknown", %{}, instance: engine_name)
  end

  test("insert requires workflows to declare a queue", %{engine_name: engine_name}) do
    assert_raise ArgumentError, ~r/workflow must define queue/, fn ->
      Endurant.insert(QueueRequiredWorkflow, %{id: "missing-queue"}, instance: engine_name)
    end
  end

  test("start_link loads repo/prefix/queues from application config", %{
    prefix: prefix,
    runtime_opts: runtime_opts
  }) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ApiTest.ConfigLoadedWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "api-config:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    repo = Keyword.fetch!(runtime_opts, :repo)
    instance = :api_config_instance

    Application.put_env(:endurant, instance,
      repo: repo,
      prefix: prefix,
      queues: [orders: [concurrency: 1, cached_limit: 1, poll_interval: 25]]
    )

    assert {:ok, supervisor_pid} = Endurant.start_link(name: instance)

    on_exit(fn ->
      stop_if_alive(supervisor_pid)
      Application.delete_env(:endurant, instance)
      Endurant.Registry.clear_instance(instance)
    end)

    assert {:ok, execution} =
             Endurant.insert(
               Endurant.Integration.ApiTest.ConfigLoadedWorkflow,
               %{id: "cfg-1"},
               instance: instance
             )

    assert {:ok, %{status: :completed, result: %{id: "cfg-1", ok: true}}} =
             PostgresHelper.wait_for_execution!(
               execution.id,
               5000,
               Keyword.put(runtime_opts, :instance, instance)
             )
  end

  test("multiple instances are isolated across prefixes", %{runtime_opts: runtime_opts}) do
    workflow_module =
      quote do
        defmodule Endurant.Integration.ApiTest.MultiInstanceWorkflow do
          use Endurant.Workflow, version: "1"

          workflow do
            queue("orders")
            unique_id(fn %{id: id} -> "api-multi:#{id}" end)
          end

          @impl Endurant.Workflow
          def run(_version, input) do
            wait_signal("go_#{input["id"]}")
            %{id: input["id"], ok: true}
          end
        end
      end

    Code.compile_quoted(workflow_module)

    repo = Keyword.fetch!(runtime_opts, :repo)
    suffix = System.unique_integer([:positive])
    migration_base_version = 30_000_000_000_000
    version_a = migration_base_version + suffix * 10 + 1
    version_b = migration_base_version + suffix * 10 + 2

    instance_a = "multi_a_#{suffix}"
    instance_b = "multi_b_#{suffix}"
    prefix_a = "workflow_multi_a_#{suffix}"
    prefix_b = "workflow_multi_b_#{suffix}"

    :ok = prepare_prefix_with_version!(repo, prefix_a, version_a)
    :ok = prepare_prefix_with_version!(repo, prefix_b, version_b)

    assert {:ok, pid_a} =
             Endurant.start_link(
               name: instance_a,
               repo: repo,
               prefix: prefix_a,
               queues: [orders: [concurrency: 1, cached_limit: 1, poll_interval: 25]]
             )

    assert {:ok, pid_b} =
             Endurant.start_link(
               name: instance_b,
               repo: repo,
               prefix: prefix_b,
               queues: [orders: [concurrency: 1, cached_limit: 1, poll_interval: 25]]
             )

    on_exit(fn ->
      stop_if_alive(pid_a)
      stop_if_alive(pid_b)
      Endurant.Registry.clear_instance(instance_a)
      Endurant.Registry.clear_instance(instance_b)
      :ok = cleanup_prefix_with_version!(repo, prefix_a, version_a)
      :ok = cleanup_prefix_with_version!(repo, prefix_b, version_b)
      Application.delete_env(:endurant, :multi_test_prefix)
    end)

    assert {:ok, execution_a} =
             Endurant.insert(
               Endurant.Integration.ApiTest.MultiInstanceWorkflow,
               %{id: "a"},
               instance: instance_a
             )

    assert {:ok, execution_b} =
             Endurant.insert(
               Endurant.Integration.ApiTest.MultiInstanceWorkflow,
               %{id: "b"},
               instance: instance_b
             )

    :ok = wait_for_status!(instance_a, execution_a.id, [:running, :waiting], 5_000)
    :ok = wait_for_status!(instance_b, execution_b.id, [:running, :waiting], 5_000)

    assert nil == Endurant.execution(execution_b.id, instance: instance_a)

    assert {:error, :not_found} ==
             Endurant.signal(execution_b.id, "go_b", %{}, instance: instance_a)

    assert {:error, :not_found} == Endurant.cancel(execution_b.id, instance: instance_a)

    assert :ok == Endurant.signal(execution_a.id, "go_a", %{}, instance: instance_a)
    assert :ok == Endurant.signal(execution_b.id, "go_b", %{}, instance: instance_b)

    assert {:ok, %{status: :completed, result: %{id: id_a, ok: true}}} =
             PostgresHelper.wait_for_execution!(
               execution_a.id,
               20_000,
               PostgresHelper.runtime_opts(prefix_a, instance_a)
             )

    assert id_a in ["a", :a]

    assert {:ok, %{status: :completed, result: %{id: id_b, ok: true}}} =
             PostgresHelper.wait_for_execution!(
               execution_b.id,
               20_000,
               PostgresHelper.runtime_opts(prefix_b, instance_b)
             )

    assert id_b in ["b", :b]
  end

  @spec stop_if_alive(pid() | term()) :: :ok
  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        _ = Supervisor.stop(pid, :normal, 5000)
        :ok
      catch
        :exit, _reason ->
          :ok
      end
    else
      :ok
    end
  end

  defp stop_if_alive(_), do: :ok

  @spec wait_for_status!(Endurant.Config.instance_name(), binary(), [atom()], non_neg_integer()) ::
          :ok
  defp wait_for_status!(instance, execution_id, allowed_statuses, timeout_ms)
       when is_list(allowed_statuses) and is_integer(timeout_ms) and timeout_ms >= 0 do
    poll_ms = 25
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_status(instance, execution_id, allowed_statuses, poll_ms, deadline, timeout_ms)
  end

  @spec do_wait_for_status(
          Endurant.Config.instance_name(),
          binary(),
          [atom()],
          non_neg_integer(),
          integer(),
          non_neg_integer()
        ) :: :ok
  defp do_wait_for_status(instance, execution_id, allowed_statuses, poll_ms, deadline, timeout_ms) do
    case Endurant.execution(execution_id, instance: instance) do
      %{status: status} ->
        if status in allowed_statuses do
          :ok
        else
          wait_for_next_status_poll(
            instance,
            execution_id,
            allowed_statuses,
            poll_ms,
            deadline,
            timeout_ms
          )
        end

      _ ->
        wait_for_next_status_poll(
          instance,
          execution_id,
          allowed_statuses,
          poll_ms,
          deadline,
          timeout_ms
        )
    end
  end

  @spec wait_for_next_status_poll(
          Endurant.Config.instance_name(),
          binary(),
          [atom()],
          non_neg_integer(),
          integer(),
          non_neg_integer()
        ) :: :ok
  defp wait_for_next_status_poll(
         instance,
         execution_id,
         allowed_statuses,
         poll_ms,
         deadline,
         timeout_ms
       ) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise ExUnit.AssertionError,
            "execution #{execution_id} did not reach #{inspect(allowed_statuses)} within #{timeout_ms}ms"
    else
      Process.sleep(poll_ms)
      do_wait_for_status(instance, execution_id, allowed_statuses, poll_ms, deadline, timeout_ms)
    end
  end

  @spec prepare_prefix_with_version!(module(), String.t(), non_neg_integer()) :: :ok
  defp prepare_prefix_with_version!(repo, prefix, version) do
    Application.put_env(:endurant, :multi_test_prefix, prefix)
    _ = Ecto.Migrator.up(repo, version, PrefixMigration, log: false)
    :ok
  end

  @spec cleanup_prefix_with_version!(module(), String.t(), non_neg_integer()) :: :ok
  defp cleanup_prefix_with_version!(repo, prefix, version) do
    Application.put_env(:endurant, :multi_test_prefix, prefix)

    case Ecto.Migrator.down(repo, version, PrefixMigration, log: false) do
      :ok -> :ok
      :already_down -> :ok
    end

    repo.query!("DROP SCHEMA IF EXISTS #{prefix} CASCADE")
    :ok
  end
end
