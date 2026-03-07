defmodule Endurant.TestSupport.IntegrationCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Endurant.TestSupport.PostgresHelper

  using do
    quote do
      use ExUnit.Case, async: false

      alias Endurant.TestSupport.PostgresHelper
      alias Endurant.TestSupport.WorkflowHelpers
    end
  end

  setup_all context do
    prefix = "workflow_dx"
    engine_name = "#{inspect(context.module)}.engine"

    :ok = PostgresHelper.start_repo!()
    {:ok, _pid} = start_supervised(PostgresHelper.Repo)
    :ok = PostgresHelper.prepare_prefix!(prefix)
    Application.put_env(:endurant, :repo, PostgresHelper.Repo)

    {:ok, _pid} =
      start_supervised(
        {Endurant.Supervisor,
         name: engine_name,
         queues: [
           orders:
             [limit: 1, parked_limit: 1, poll_interval: 25] ++
               PostgresHelper.runtime_opts(prefix)
         ]}
      )

    on_exit(fn ->
      pid = :global.whereis_name({:endurant_supervisor, engine_name})

      if is_pid(pid) do
        Process.exit(pid, :shutdown)
        Process.sleep(50)
      end

      :ok = PostgresHelper.cleanup_prefix!(prefix)
    end)

    {:ok,
     prefix: prefix, runtime_opts: PostgresHelper.runtime_opts(prefix), engine_name: engine_name}
  end

  setup %{prefix: prefix} do
    :ok = PostgresHelper.truncate_prefix!(prefix)
    :ok
  end
end
