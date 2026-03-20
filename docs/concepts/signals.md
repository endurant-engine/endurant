# Signals

Signals let you push data into a running or waiting workflow.

Inside a workflow, use `wait_signal/1` or `wait_signal/2`:

```elixir
approval = wait_signal("approval_requested")
```

You can override the cached TTL for a specific wait:

```elixir
approval = wait_signal("approval_requested", cached_ttl_ms: 5_000)
```

From outside the workflow, send a signal with `Endurant.signal/3` or `Endurant.signal/4`:

```elixir
:ok =
  Endurant.signal(
    execution_id,
    "approval_requested",
    %{"approved" => true}
  )
```

To target a named instance:

```elixir
:ok =
  Endurant.signal(
    execution_id,
    "approval_requested",
    %{"approved" => true},
    instance: :my_endurant
  )
```

## Semantics

- Signals are persisted as events.
- Signal payloads are consumed one at a time (FIFO) per signal key.
- A signal sent before `wait_signal/1` is still available when the workflow
  later waits for that key.

## Signals and Caching

When a workflow waits on `wait_signal/1`, Endurant marks the execution as
waiting and the queue manager either:

- caches the executor process in memory for fast resume, or
- releases it and lets another claim resume it later.

Which path is used depends on queue settings such as `cached_limit` and
`cached_ttl_ms`, plus any workflow-level `cached_ttl_ms/1` override.
Both paths are durable because resume is driven by persisted events.

See [Caching](../advanced/caching.md) for tuning and runtime behavior.
