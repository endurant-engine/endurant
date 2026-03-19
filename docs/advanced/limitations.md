# Limitations

This page documents the workflow patterns that are intentionally unsupported
or unsafe in the current runtime.

## Do Not Catch Workflow Control Flow

Do not wrap workflow control primitives in `try/catch`.

That includes:

- `wait_signal/1`
- `sleep/2`
- `continue_as_new/1`
- `continue_as_new/2`
- child workflow waiting paths such as `child_workflow/3` and `child_workflow_await/1`

These APIs currently rely on internal control-flow throws to stop the current
execution and hand control back to the executor.

Bad:

```elixir
try do
  wait_signal("approval")
catch
  _kind, _reason ->
  :ignored
end
```

## Run Workflow Primitives Only In The Main Workflow Process

Workflow primitives are process-bound. Call them only from the main workflow
execution process inside `run/2` or helpers called directly from it.

This includes:

- `task/3`
- `task/4`
- `task_async/3`
- `task_async/4`
- `task_async_stream/3`
- `task_async_stream/4`
- `wait_signal/1`
- `sleep/2`
- `continue_as_new/1`
- `continue_as_new/2`
- child workflow primitives

Do not call them from:

- `spawn/1`
- `Task.start/1`
- `Task.async/1`
- separate `GenServer` processes
- any process other than the workflow executor process

These APIs depend on workflow runtime state stored in the current process and on
messages delivered to that process while the workflow is running.

Bad:

```elixir
task(input, "outer", fn i ->
  Task.async(fn ->
    wait_signal("approval")
  end)
  |> Task.await()
end)
```

## Do Not Reuse Task Names For Different Work

Task results are keyed by task name and reused during replay, they need to be unique inside
a Workflow.

Good task names are stable and deterministic:

```elixir
task(input, "fetch_user", fn i -> Accounts.fetch_user!(i["user_id"]) end)
task(item, "process_item:#{item.id}", &process_item/1)
```

## Do Not Put Non-Deterministic Logic In Workflow Code

Workflow code outside durable primitives must replay the same way for the same
history. If you need non-deterministic or external work, put it in a `task`.

## Do Not Depend On Async Stream Ordering

`task_async_stream/3` and `task_async_stream/4` are for bounded parallel work.
Do not make workflow semantics depend on the order in which stream items are
produced.

## Watch Event History Growth

Every task, signal, wait, retry, and lifecycle transition adds entries to event
history. Long-running workflows can accumulate large histories, which makes
replay slower and increases storage usage.

If a workflow is expected to run for a long time or handle many repeated
interactions, use `continue_as_new/2` to roll it forward before the history gets
too large.

Use `history_length/0` and `history_size/0` to monitor this from workflow code.

Using child workflows can also help reducing the history.
