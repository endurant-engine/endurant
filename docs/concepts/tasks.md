# Tasks

Tasks are durable workflow steps recorded in event history.

Use `task/3` or `task/4` inside `run/2`:

```elixir
result =
  task(input, "fetch_user", fn i ->
    MyApp.Accounts.fetch_user!(i["user_id"])
  end)
```

Task names are important: Endurant keys task results by name. On replay, the
result for the same task name is reused instead of rerunning the task.

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