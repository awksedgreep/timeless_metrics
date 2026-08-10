defmodule TimelessMetrics.LibsqlMigrationStageRecoveryTest do
  @moduledoc """
  Recovering from a migration that died partway through staging.

  The conversion runs during supervisor start, so anything it refuses to do is
  not a degraded migration — the application does not boot. A staging directory
  left behind by an OOM kill, power loss, or container restart carries no
  completion marker, so the activate path rejects it as incomplete and the
  staging path rejects it because the directory exists. Both are permanent: the
  advice to "restart to resume" cannot succeed, because a restart arrives at
  exactly the same state.

  Staging never writes `metrics.db` or `rust_engine/`, so an unverifiable stage
  is scratch and discarding it costs nothing. A stage that *does* verify is a
  real resumable migration and must survive — that is the second test here, and
  it is what stops the first fix from turning into "delete the work every boot".
  """

  use ExUnit.Case, async: false

  alias TimelessMetrics.LibsqlMigration

  defp legacy_store(tag) do
    dir =
      Path.join(System.tmp_dir!(), "tm_stage_#{tag}_#{System.unique_integer([:positive])}")

    store = :"stage_recovery_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      TimelessMetrics.Supervisor.start_link(
        name: store,
        engine: :rust,
        data_dir: dir,
        scraping: false,
        self_monitor: false
      )

    :ok =
      TimelessMetrics.write_batch(
        store,
        for(i <- 1..50, do: {"cpu", %{"host" => "a"}, i * 1.0, i})
      )

    :ok = TimelessMetrics.flush(store)
    Supervisor.stop(sup)
    Process.sleep(200)

    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  # What a kill after `File.mkdir/1` and the admin snapshot, but before the
  # completion marker, actually leaves on disk.
  defp interrupted_stage(dir) do
    stage = Path.join(dir, ".libsql-migration")
    File.mkdir_p!(stage)
    File.cp!(Path.join(dir, "metrics.db"), Path.join(stage, "metrics.db"))
    stage
  end

  test "a stage left by a killed migration is discarded and the migration completes" do
    dir = legacy_store("killed")
    interrupted_stage(dir)

    assert {:ok, %{activated: true, series: 1, points: 50}} =
             LibsqlMigration.run(dir, activate: true)
  end

  test "an empty stage directory is likewise not fatal" do
    dir = legacy_store("empty")
    File.mkdir_p!(Path.join(dir, ".libsql-migration"))

    assert {:ok, %{activated: true, points: 50}} = LibsqlMigration.run(dir, activate: true)
  end

  test "the source survives a killed stage untouched" do
    dir = legacy_store("source")
    interrupted_stage(dir)

    assert {:ok, _} = LibsqlMigration.run(dir, activate: true)

    # Activation retains both for rollback; nothing about the interrupted stage
    # may cost the operator their source data.
    assert File.dir?(Path.join(dir, "rust_engine"))
    assert File.regular?(Path.join(dir, "metrics.db.pre-libsql"))
  end

  test "a complete stage is resumed, not thrown away and redone" do
    dir = legacy_store("complete")

    assert {:ok, %{activated: false, points: 50}} = LibsqlMigration.run(dir)

    staged_db = Path.join(dir, ".libsql-migration/metrics.db")
    assert File.regular?(staged_db)
    before = File.stat!(staged_db).mtime

    assert {:ok, %{activated: true, points: 50}} = LibsqlMigration.run(dir, activate: true)

    # Had the stage been discarded, the activated database would have been
    # rebuilt from scratch rather than promoted.
    assert File.stat!(Path.join(dir, "metrics.db")).mtime == before
  end
end
