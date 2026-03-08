defmodule Endurant.MixProject do
  use Mix.Project

  def project do
    [
      app: :endurant,
      version: "0.1.0-alpha.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      dialyzer: [plt_add_apps: [:mix]],
      description: "Durable workflow engine for Elixir",
      source_url: "https://github.com/endurant-engine/endurant",
      docs: docs(),
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.20"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/endurant-engine/endurant"
      }
    ]
  end

  defp docs do
    introduction = [
      "docs/introduction/overview.md",
      "docs/introduction/installation.md",
      "docs/introduction/up-and-running.md"
    ]

    concepts = [
      "docs/concepts/tasks.md",
      "docs/concepts/signals.md",
      "docs/concepts/sleep.md",
      "docs/concepts/async.md"
    ]

    advanced = [
      "docs/advanced/internals.md",
      "docs/advanced/parking.md",
      "docs/state_machine.md"
    ]

    [
      main: "overview",
      extras: introduction ++ concepts ++ advanced,
      groups_for_extras: [
        Introduction: introduction,
        Concepts: concepts,
        Advanced: advanced
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
