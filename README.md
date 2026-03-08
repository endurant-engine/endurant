# Endurant

## Description
Endurant is a durable workflow engine where workflows are written as Elixir code. It has built-in retries, signals, and crash-safe recovery. It uses an event log and replays it during recovery to reconstruct workflow state.

## Workflow Example: Shop Order With Vendor Choice

```elixir
defmodule MyApp.Workflows.ShopOrderWorkflow do
  use Endurant.Workflow, version: "1"

  workflow do
    queue("orders")
    unique_id(fn %{"order_id" => order_id} ->
      "shop-order:#{order_id}"
    end)
  end

  @impl Endurant.Workflow
  def run(_version, input) do
    vendors = Map.get(input, "vendors", ["shop_a", "shop_b", "shop_c"])

    order =
      task(input, "load_order", fn i ->
        MyApp.Shop.load_order!(i["order_id"])
      end)

    shipping_h = task_async(order, "quote_shipping", &MyApp.Shop.quote_shipping!/1)
    tax_h = task_async(order, "quote_tax", &MyApp.Shop.quote_tax!/1)

    offers =
      task_async_stream(
        vendors,
        fn vendor -> "offer:#{vendor}" end,
        fn vendor -> MyApp.Marketplace.fetch_offer!(vendor, order) end,
        max_concurrency: 5
      )
      |> Enum.map(fn {_task_key, offer} -> offer end)

    totals = task_await_many([shipping_h, tax_h])

    task({order, offers, totals}, "ask_user_to_choose_vendor", fn {o, candidate_offers, quoted} ->
      MyApp.Shop.notify_vendor_options!(o, candidate_offers, quoted)
      :ok
    end)

    # Expected signal payload: %{"vendor_id" => "shop_b"}
    selection = wait_signal("vendor_selected")

    chosen_offer =
      task({offers, selection}, "pick_vendor", fn {candidate_offers, picked} ->
        vendor_id = picked["vendor_id"]

        Enum.find(candidate_offers, fn offer -> offer.vendor_id == vendor_id end) ||
          raise("unknown vendor: #{vendor_id}")
      end)

    payment =
      task(
        {order, chosen_offer, totals},
        "charge_payment",
        fn {o, offer, t} ->
          MyApp.Payments.charge!(
            order: o,
            vendor_offer: offer,
            shipping: t["quote_shipping"],
            tax: t["quote_tax"]
          )
        end,
        retry: [max_attempts: 3, backoff: :exponential, base_ms: 200]
      )

    task({order, chosen_offer, payment}, "place_order", fn {o, offer, p} ->
      MyApp.Shop.place_order!(o, offer, p)
      %{
        order_id: o.id,
        vendor_id: offer.vendor_id,
        payment_id: p.id,
        status: "confirmed"
      }
    end)
  end
end
```


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
