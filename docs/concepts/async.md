# Async

Endurant supports deterministic parallel work with async task primitives.

## `task_async` and `task_await_many`

Run a few tasks in parallel and join their results:

```elixir
user_h = task_async(input, "fetch_user", &MyApp.Accounts.fetch_user!/1)
cart_h = task_async(input, "fetch_cart", &MyApp.Carts.fetch_cart!/1)

results = task_await_many([user_h, cart_h])
user = results["fetch_user"]
cart = results["fetch_cart"]
```

Use stable task names and keep logic deterministic for replay.

## `task_await`

When you only need one async result, wait on a single handle:

```elixir
user_h = task_async(input, "fetch_user", &MyApp.Accounts.fetch_user!/1)
user = task_await(user_h)
```

## `task_async_stream`

Fan out over many items with bounded concurrency:

```elixir
offers =
  task_async_stream(
    vendors,
    fn vendor -> "offer:#{vendor}" end,
    fn vendor -> MyApp.Marketplace.fetch_offer!(vendor, order) end,
    max_concurrency: 8
  )
  |> Enum.map(fn {_task_key, offer} -> offer end)
```

Notes:

- Returned stream items are `{task_key, result}` tuples.
- Order can vary between runs; do not depend on stream order.
- Task keys must be unique within the stream.
