# Internals

This page explains the runtime model behind Endurant.

## Event Log and Replay

Endurant persists workflow lifecycle events and task events in storage.

On recovery, it replays event history to reconstruct workflow runtime state:

- `execution_created`
- `execution_started`
- `execution_waiting`
- `execution_resumed`
- `execution_abandoned`
- `execution_completed`
- `execution_failed`
- `cancel_requested`
- `execution_cancelled`
- `task_started`
- `task_completed`
- `task_failed`
- `signal_received`

This replay model is what makes execution durable and deterministic across
worker crashes and restarts.

## Determinism Rules

The workflow as a whole must be deterministic and replay-safe for the same
event history.

Practical rules:

- keep task names stable
- avoid non-deterministic branching outside task results/signals
- treat stream ordering as non-deterministic
- treat task-level idempotency as a safety net, not the primary guarantee

## Lifecycle

Executions move through states such as:

- `:pending`
- `:running`
- `:waiting`
- `:continuable`
- terminal states like `:completed`, `:failed`, `:cancelled`

See the state diagram in [State Machine](../state_machine.md).
