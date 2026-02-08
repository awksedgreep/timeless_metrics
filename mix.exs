defmodule Timeless.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless,
      version: "0.5.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Timeless.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:gorilla_stream, "~> 1.3"},
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", only: [:dev, :test]}
    ]
  end
end
