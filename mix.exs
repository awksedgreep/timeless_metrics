defmodule TimelessMetrics.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless_metrics,
      version: "0.6.2",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {TimelessMetrics.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:gorilla_stream, "~> 2.0"},
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", only: [:dev, :test]}
    ]
  end
end
