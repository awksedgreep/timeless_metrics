defmodule TimelessMetrics.MixProject do
  use Mix.Project

  @version "6.2.2"

  def project do
    [
      app: :timeless_metrics,
      version: @version,
      elixir: "~> 1.18",
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_clean: ["clean"],
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url:
        "https://github.com/awksedgreep/timeless_metrics/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "tms_engine",
      make_precompiler_priv_paths: ["native/tms_engine.*", "native/timeless_sqlite_ext.*"],
      make_precompiler_nif_versions: [versions: ["2.16", "2.17", "2.18"]],
      # A source checkout must not silently restore a released NIF whose
      # exports predate the Rust sources being tested. Hex installs still use
      # the published precompiled artefact.
      make_force_build: File.exists?(Path.join(__DIR__, ".git")),
      description: "Embedded time series database for Elixir with a Rust-native hot path.",
      source_url: "https://github.com/awksedgreep/timeless_metrics",
      homepage_url: "https://github.com/awksedgreep/timeless_metrics",
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  defp package do
    [
      maintainers: ["Mark Cotner"],
      files: ~w(
        lib
        native/tms_engine/Cargo.toml
        native/tms_engine/Cargo.lock
        native/tms_engine/src
        native/timeless_sqlite_ext/Cargo.toml
        native/timeless_sqlite_ext/Cargo.lock
        native/timeless_sqlite_ext/src
        Makefile
        mix.exs
        checksum.exs
        README.md
        LICENSE
      ),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/awksedgreep/timeless_metrics"}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras:
        ["README.md", "LICENSE"] ++
          Path.wildcard("docs/*.md")
    ]
  end

  def application do
    [
      mod: {TimelessMetrics.Application, []},
      extra_applications: [:logger, :inets]
    ]
  end

  defp deps do
    [
      {:ex_alp, "~> 0.1"},
      {:ex_openzl, "~> 0.4"},
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:rocket, "~> 0.2"},
      {:req, "~> 0.5"},
      {:elixir_make, "~> 0.9", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false},
      {:rustler, "~> 0.35"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
