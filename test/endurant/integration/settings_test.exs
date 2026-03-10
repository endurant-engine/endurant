defmodule Endurant.Integration.SettingsTest do
  use Endurant.TestSupport.IntegrationCase, async: false

  alias Endurant.Scheduler
  alias Endurant.Settings

  test "put/get setting", %{runtime_opts: runtime_opts} do
    id = "settings:put_get:#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = Settings.put(id, %{"enabled" => true, "value" => 10}, runtime_opts)
    assert %{"enabled" => true, "value" => 10} = Settings.get(id, runtime_opts)
  end

  test "lease can be claimed, blocked, released, and reclaimed", %{runtime_opts: runtime_opts} do
    id = "settings:lease:#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = Settings.put_new(id, %{}, runtime_opts)
    assert {:ok, 1} = Settings.claim_lease(id, "owner-a", 500, runtime_opts)
    assert :busy = Settings.claim_lease(id, "owner-b", 500, runtime_opts)
    assert :ok = Settings.release_lease(id, "owner-a", runtime_opts)
    assert {:ok, 2} = Settings.claim_lease(id, "owner-b", 500, runtime_opts)
  end

  test "heartbeat reports lock_lost after lease expiry", %{runtime_opts: runtime_opts} do
    id = "settings:heartbeat:#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = Settings.put_new(id, %{}, runtime_opts)
    assert {:ok, 1} = Settings.claim_lease(id, "owner-a", 30, runtime_opts)
    Process.sleep(80)
    assert {:error, :lock_lost} = Settings.heartbeat_lease(id, "owner-a", 30, runtime_opts)
  end

  test "scheduler acquires its lease row", %{runtime_opts: runtime_opts, engine_name: engine_name} do
    id = Scheduler.setting_id(engine_name)

    assert wait_until(fn ->
             case Settings.get(id, runtime_opts) do
               %{"owner" => owner, "lease_until_ms" => lease_until}
               when is_binary(owner) and is_integer(lease_until) ->
                 owner != ""

               _ ->
                 false
             end
           end)
  end

  defp wait_until(fun, timeout_ms \\ 2_000, poll_ms \\ 25) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, poll_ms)
  end

  defp do_wait_until(fun, deadline, poll_ms) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(poll_ms)
        do_wait_until(fun, deadline, poll_ms)
      end
    end
  end
end
