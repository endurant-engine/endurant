# Continue As New

Use `continue_as_new/2` to end the current execution and immediately start a
new execution for the same workflow identity.

This is useful for long-running or effectively infinite workflows that would
otherwise keep growing their event history forever.

## Infinite Workflow Example

```elixir
defmodule MyApp.Workflows.DeviceWorkflow do
  use Endurant.Workflow, version: "1"

  @max_history_events 5_000
  @max_history_bytes 5_000_000

  workflow do
    queue("devices")
    unique_id(fn %{"device_id" => device_id} ->
      "device:#{device_id}"
    end)
  end

  @impl Endurant.Workflow
  def run(version, input) do
    state =
      input
      |> Map.put_new("online", false)
      |> Map.put_new("last_temperature_c", nil)
      |> Map.put_new("firmware_version", nil)
      |> Map.put_new("updates_processed", 0)

    loop(version, state)
  end

  defp loop(version, state) do
    signal = wait_signal("device_update")

    next_state = %{
      "device_id" => state["device_id"],
      "online" => Map.get(signal, "online", state["online"]),
      "last_temperature_c" => Map.get(signal, "temperature_c", state["last_temperature_c"]),
      "firmware_version" => Map.get(signal, "firmware_version", state["firmware_version"]),
      "updates_processed" => state["updates_processed"] + 1
    }

    if should_continue_as_new?() do
      continue_as_new(
        next_state,
        version: version,
        rollover_signals: true
      )
    else
      loop(version, next_state)
    end
  end

  defp should_continue_as_new? do
    history_length() >= @max_history_events or
      history_size() >= @max_history_bytes
  end
end
```

## Signals And Rollover

By default, continue-as-new does **not** copy unused signals into the new
execution.

If you want unused observed signals to move to the new execution, opt in:

```elixir
continue_as_new(
  next_state,
  version: version,
  rollover_signals: true
)
```

## Current Options

`continue_as_new/2` currently supports:

- `version: "..."` to start the new execution with a different workflow version
- `rollover_signals: true` to carry unused observed signals into the new execution
