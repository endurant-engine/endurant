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
        {:signal, 2},
        {:signal, 3},
        {:signal, 4},
        {:cancel, 1},
        {:cancel, 2},
        {:execution, 1},
        {:execution, 2},
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
