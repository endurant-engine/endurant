defmodule Endurant.PublicApiTest do
  use ExUnit.Case, async: true

  test "Endurant exposes only the intended public API functions" do
    exported =
      Endurant.__info__(:functions)
      |> Enum.reject(fn {name, _arity} -> name in [:module_info, :__info__] end)
      |> MapSet.new()

    expected =
      MapSet.new([
        {:start_link, 1},
        {:insert, 2},
        {:insert, 3},
        {:schedule, 3},
        {:schedule, 4},
        {:schedule, 5},
        {:scheduled, 0},
        {:scheduled, 1},
        {:scheduled, 2},
        {:cancel_scheduled, 1},
        {:cancel_scheduled, 2},
        {:cron, 3},
        {:cron, 4},
        {:cron, 5},
        {:crons, 0},
        {:crons, 1},
        {:crons, 2},
        {:cron_fires, 1},
        {:cron_fires, 2},
        {:cron_fires, 3},
        {:pause_cron, 1},
        {:pause_cron, 2},
        {:resume_cron, 1},
        {:resume_cron, 2},
        {:delete_cron, 1},
        {:delete_cron, 2},
        {:signal, 2},
        {:signal, 3},
        {:signal, 4},
        {:cancel, 1},
        {:cancel, 2},
        {:execution, 1},
        {:execution, 2},
        {:executions, 0},
        {:executions, 1},
        {:executions, 2},
        {:events, 1},
        {:events, 2}
      ])

    assert exported == expected
  end

  test "Endurant.Migration exposes only migration entrypoints" do
    exported =
      Endurant.Migration.__info__(:functions)
      |> Enum.reject(fn {name, _arity} -> name in [:module_info, :__info__] end)
      |> MapSet.new()

    expected =
      MapSet.new([
        {:__migration__, 0},
        {:up, 0},
        {:up, 1},
        {:down, 0},
        {:down, 1},
        {:migrated_version, 0},
        {:migrated_version, 1}
      ])

    assert exported == expected
  end
end
