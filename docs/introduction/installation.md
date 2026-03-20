# Installation

## 1. Add Dependency

```elixir
def deps do
  [
    {:endurant, "~> 0.1.0"}
  ]
end
```

## 2. Add Migration

Create a migration in your app and call `Endurant.Migration`:

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

## 3. Add Endurant to `application.ex`

```elixir
def start(_type, _args) do
  children = [
    MyApp.Repo,
    %{
      id: Endurant,
      start:
        {Endurant, :start_link,
         [[
           repo: MyApp.Repo,
           db_log: false,
           queues: [
             orders: [
               concurrency: 10,
               cached_limit: 100,
               poll_interval: 200
             ]
           ]
         ]]}
    }
  ]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

`db_log` defaults to `false`. Set it to `true` or a Logger level like `:debug`
or `:info` if you want Endurant-issued database queries logged.
