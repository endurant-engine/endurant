# Tasks

Tasks are durable workflow steps recorded in event history.

Use `task/3` or `task/4` inside `run/2`:

```elixir
result =
  task(input, "fetch_user", fn i ->
    MyApp.Accounts.fetch_user!(i["user_id"])
  end)
```

If you want to know whether a task result was executed in the current run or
reused from workflow history, call `task_source/1` after the task resolves:

```elixir
result =
  task(input, "fetch_user", fn i ->
    MyApp.Accounts.fetch_user!(i["user_id"])
  end)

source = task_source("fetch_user")
```

Task names are important: Endurant keys task results by name. On replay, the
result for the same task name is reused instead of rerunning the task.

`task_source/1` returns:

- `:executed` when the task ran in the current workflow execution pass
- `:history` when the result came from existing workflow history during replay

## Retry

`task/4` supports retry options:

```elixir
invoice =
  task(
    input,
    "issue_invoice",
    fn i -> MyApp.Billing.issue_invoice!(i["order_id"]) end,
    retry: [max_attempts: 3, backoff: :exponential, base_ms: 200, max_ms: 5_000]
  )
```

Supported retry options:

- `max_attempts` (default `1`)
- `backoff` (`:constant` or `:exponential`)
- `base_ms` (default `100`)
- `max_ms` (default `30_000`)

#
