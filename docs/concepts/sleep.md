# Sleep

Use `sleep/2` to pause workflow progress until a future time.

```elixir
sleep("retry:attempt:1", 1_000)
```

Arguments:

- `wait_key` - stable key for this logical wait site
- `delay_ms` - positive delay in milliseconds

## Why `wait_key` Matters

Endurant tracks waits by key during replay. Use stable keys so the same history
replays deterministically:

```elixir
sleep("payment_retry:#{attempt}", retry_delay_ms)
```

## Typical Uses

- Retry backoff between attempts
- Human timeout windows
- Scheduled follow-up actions inside long-running workflows
