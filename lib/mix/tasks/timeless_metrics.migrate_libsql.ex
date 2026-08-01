defmodule Mix.Tasks.TimelessMetrics.MigrateLibsql do
  use Mix.Task

  @shortdoc "Migrate a stopped Rust-engine store into metrics.db/libSQL"

  @moduledoc """
  Stages and verifies a filesystem-Rust-engine migration.

      mix timeless_metrics.migrate_libsql DATA_DIR
      mix timeless_metrics.migrate_libsql DATA_DIR --activate

  Without `--activate`, the verified database remains under
  `DATA_DIR/.libsql-migration`. Activation retains `metrics.db.pre-libsql` and
  the original `rust_engine/` directory for rollback.
  """

  @impl true
  def run(args) do
    # Loading this project's application would also start any configured
    # TimelessMetrics store—the opposite of an offline migration. Start only
    # the native/database dependencies the converter itself needs.
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:exqlite)
    {:ok, _} = Application.ensure_all_started(:rustler)
    Code.ensure_loaded!(TimelessMetrics.RustEngine.Nif)

    {opts, positional, invalid} = OptionParser.parse(args, strict: [activate: :boolean])

    case {positional, invalid} do
      {[data_dir], []} ->
        case TimelessMetrics.LibsqlMigration.run(Path.expand(data_dir), opts) do
          {:ok, report} -> Mix.shell().info("libSQL migration verified: #{inspect(report)}")
          {:error, reason} -> Mix.raise("libSQL migration failed: #{reason}")
        end

      _ ->
        Mix.raise("usage: mix timeless_metrics.migrate_libsql DATA_DIR [--activate]")
    end
  end
end
