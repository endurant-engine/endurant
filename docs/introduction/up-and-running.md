# Up and Running

This page shows one end-to-end flow: define a workflow, insert it, signal it,
and inspect execution state.

## 1. Define a Workflow

```elixir
defmodule MyApp.Workflows.OrderApprovalWorkflow do
  use Endurant.Workflow, version: "1"

  workflow do
    queue("orders")
    unique_id(fn %{"order_id" => order_id} -> "order:#{order_id}" end)
  end

  @impl Endurant.Workflow
  def run(_version, input) do
    order =
      task(input, "load_order", fn i ->
        MyApp.Shop.load_order!(i["order_id"])
      end)

    approval = wait_signal("approval_requested")

    task({order, approval}, "finalize", fn {o, a} ->
      MyApp.Shop.finalize_order!(o, a)
    end)
  end
end
```

## 2. Insert an Execution

```elixir
{:ok, execution} =
  Endurant.insert(
    MyApp.Workflows.OrderApprovalWorkflow,
    %{"order_id" => "o-123"},
    repo: MyApp.Repo,
    prefix: "public"
  )
```

## 3. Signal the Workflow

```elixir
:ok =
  Endurant.signal(
    execution.id,
    "approval_requested",
    %{"approved" => true},
  )
```

## 4. Inspect State and History

```elixir
execution_state = Endurant.execution(execution.id)
events = Endurant.events(execution.id)
```

`execution_state.status` moves through lifecycle states such as `:pending`,
`:running`, `:waiting`, and terminal states like `:completed` or `:failed`.
