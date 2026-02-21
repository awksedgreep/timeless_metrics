defmodule TimelessMetrics.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless_metrics,
      version: "0.7.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_env: fn ->
        erts_include_dir =
          Path.join([
            to_string(:code.root_dir()),
            "erts-#{:erlang.system_info(:version)}",
            "include"
          ])

        %{"ERTS_INCLUDE_DIR" => erts_include_dir}
      end,
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
      {:gorilla_stream, path: "../gorilla_stream"},
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", only: [:dev, :test]},
      {:elixir_make, "~> 0.9", runtime: false}
    ]
  end
end
