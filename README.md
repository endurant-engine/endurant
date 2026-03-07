# Endurant

**TODO: Add description**

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




# TODO
- look into SKIP LOCKED
- dialyzer
- do not run await, task in child process
- update version of a runnig workflow
- retrinking heartbeat, freezing executor processes
- task ..., error_as_failure: true, or {:task_error, reason}/ add a task!
- rollbacks (saga)
- loglevel
- maybe return {:new/cached, result} in tasks, source = task_source("fetch_user") # :executed or :history
- is it possible to store any data in an input, not just json
- recover_expired_locks, Heavy per-tick workload

  task(input, "fetch_user", fn i -> Api.fetch!(i.id) end,
    retry: [
      max_attempts: 5,
      retryable: fn error_or_reason ->
        match?(%RuntimeError{}, error_or_reason)
      end
    ]
  )
- signal_wait_for(1000) stop process after this
- add a default "suspense" time to queue settings and to workflow settings
- workflow querying/filtering
- add propper benchmarking
- find a name for waiting processes that are "paused" or "suspended"
- query/filter workflows, use temporal system, register custom properties to create indicies
- globally handling prefix and repo