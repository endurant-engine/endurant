# Overview

Endurant is a durable workflow engine for Elixir.

Workflows are defined as Elixir modules and written as code. Executions state is persistet in an event log. If a worker crashes or restarts, Endurant replays those events and resumes execution deterministically.

## Features

- Workflow logic written in Elixir
- Durable execution with retry support
- Signal-based waiting for external input
- Time-based waiting with `sleep/2`
- One-time future scheduling with `Endurant.schedule/..`
- Recurring cron scheduling with `Endurant.cron/..`
- Parallel task patterns with async primitives
- Event history for debugging and auditing

## Example

This example shows a workflow with durable tasks and a signal wait:

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

    task({order, approval}, "finalize_order", fn {o, a} ->
      MyApp.Shop.finalize_order!(o, a)
    end)
  end
end
```

Start an execution:

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

Inspect current state or history:

```elixir
execution_state = Endurant.execution(execution.id)
events = Endurant.events(execution.id)
```

The same workflow style can also be used with scheduling APIs:

```elixir
{:ok, scheduled} =
  Endurant.schedule(
    MyApp.Workflows.OrderApprovalWorkflow,
    %{"order_id" => "o-124"},
    DateTime.add(DateTime.utc_now(), 3_600, :second)
  )

{:ok, cron} =
  Endurant.cron(
    MyApp.Workflows.OrderApprovalWorkflow,
    %{"order_id" => "o-125"},
    "0 * * * *"
  )
```
