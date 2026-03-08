# Signals

Signals let you push data into a running or waiting workflow.

Inside a workflow, use `wait_signal/1`:

```elixir
approval = wait_signal("approval_requested")
```

From outside the workflow, send a signal with `Endurant.signal/4`:

```elixir
:ok =
  Endurant.signal(
    execution_id,
    "approval_requested",
    %{"approved" => true}
  )
```

## Semantics

- Signals are persisted as events.
- Signal payloads are consumed one at a time (FIFO) per signal key.
- A signal sent before `wait_signal/1` is still available when the workflow
  later waits for that key.

## Signals and Parking

When a workflow waits on `wait_signal/1`, Endurant marks the execution as
waiting and the queue manager either:

- parks the executor process in memory for fast resume, or
- releases it and lets another claim resume it later.

Which path is used depends on queue settings such as `parked_limit`.
Both paths are durable because resume is driven by persisted events.

See [Parking](../advanced/parking.md) for tuning and runtime behavior.
