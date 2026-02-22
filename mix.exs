defmodule TimelessMetrics.MixProject do
  use Mix.Project

  @version "1.0.0"

  def project do
    [
      app: :timeless_metrics,
      version: @version,
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
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url:
        "https://github.com/awksedgreep/timeless_metrics/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "prometheus_nif",
      make_precompiler_priv_paths: ["prometheus_nif.*"],
      make_precompiler_nif_versions: [versions: ["2.16", "2.17"]],
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
      {:gorilla_stream, "~> 2.2"},
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", only: [:dev, :test]},
      {:elixir_make, "~> 0.9", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false}
    ]
  end
end
