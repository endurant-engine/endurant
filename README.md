# Endurant

## Description
Endurant is a durable workflow engine where workflows are written as Elixir code. It has built-in retries, signals, and crash-safe recovery. It uses an event log and replays it during recovery to reconstruct workflow state.

## Workflow Example: Order Approval

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

    pricing_h =
      task_async(order, "quote_pricing", fn o ->
        MyApp.Shop.quote_pricing!(o)
      end)

    stock_h =
      task_async(order, "check_stock", fn o ->
        MyApp.Shop.check_stock!(o)
      end)

    checks = task_await_many([pricing_h, stock_h])

    task({order, checks}, "request_approval", fn {o, result} ->
      MyApp.Shop.request_approval!(o, result)
      :ok
    end)

    approval = wait_signal("approval_requested")

    task(
      {order, approval, checks},
      "finalize_order",
      fn {o, a, result} ->
        MyApp.Shop.finalize_order!(o, a, result)
      end,
      retry: [max_attempts: 3, backoff: :exponential, base_ms: 250]
    )
  end
end
```

Start a workflow:

```elixir
{:ok, execution} =
  Endurant.insert(
    MyApp.Workflows.OrderApprovalWorkflow,
    %{"order_id" => "o-123"}
  )
```

Resume it later with external input:

```elixir
:ok =
  Endurant.signal(
    execution.id,
    "approval_requested",
    %{"approved" => true, "approved_by" => "ops@example.com"}
  )
```


## Migrations

Endurant uses host-application wrapper migrations. In your app, generate a migration
and call `Endurant.Migration` from `up/0` and `down/0`.

```elixir
defmodule MyApp.Repo.Migrations.AddEndurant do
  use Ecto.Migration

  def up do
    Endurant.Migration.up(version: 1)
  end

  def down do
    Endurant.Migration.down(version: 1)
  end
end
```

When Endurant introduces a new migration version, add another host migration:

```elixir
defmodule MyApp.Repo.Migrations.UpgradeEndurantToV2 do
  use Ecto.Migration

  def up do
    Endurant.Migration.up(version: 2)
  end

  def down do
    Endurant.Migration.down(version: 1)
  end
end
```

Migrated version tracking uses a comment on `endurant_executions`.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `endurant` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:endurant, "~> 0.1.0"}
  ]
end
```
