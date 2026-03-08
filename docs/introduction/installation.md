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
    {Endurant.Supervisor,
      name: "my_endurant",
      queues: [
        orders: [
          limit: 10,
          parked_limit: 100,
          poll_interval: 200,
          repo: MyApp.Repo,
          prefix: "public"
        ]
      ]}
  ]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

