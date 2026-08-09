defmodule TimelessMetrics.LibsqlMigrationTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @store :libsql_migration_source

  test "offline migration verifies and activates without deleting rollback data" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "timeless_migration_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)

    start_supervised!(
      {TimelessMetrics,
       name: @store, engine: :rust, data_dir: data_dir, scraping: false, self_monitor: false}
    )

    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"cpu", %{"host" => "a"}, 1.25, 10},
               {"cpu", %{"host" => "a"}, 2.5, 20},
               {"cpu", %{"host" => "b"}, 3.75, 15}
             ])

    assert {:ok, _empty_sid} =
             TimelessMetrics.resolve_series(@store, "empty", %{"kind" => "catalog"})

    assert :ok = TimelessMetrics.flush(@store)
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")

    assert_libsql_startup_refused(data_dir)

    assert {:ok, %{activated: false, series: 3, points: 3}} =
             TimelessMetrics.LibsqlMigration.run(data_dir)

    assert File.exists?(Path.join(data_dir, ".libsql-migration/metrics.db"))
    refute File.exists?(Path.join(data_dir, "metrics.db.pre-libsql"))

    assert {:error, activation_error} =
             TimelessMetrics.LibsqlMigration.run(data_dir,
               activate: true,
               activation_failpoint: :after_staged_rename
             )

    assert activation_error =~ "injected_activation_failure"
    assert activation_error =~ "source_restored"
    assert File.exists?(Path.join(data_dir, "metrics.db"))
    assert File.exists?(Path.join(data_dir, ".libsql-migration/metrics.db"))
    refute File.exists?(Path.join(data_dir, "metrics.db.pre-libsql"))

    changed_source = Path.join(data_dir, "rust_engine/source-changed-after-staging")
    File.write!(changed_source, "changed")

    assert {:error, reason} =
             TimelessMetrics.LibsqlMigration.run(data_dir, activate: true)

    assert reason =~ "source store changed after the migration was staged"
    File.rm!(changed_source)

    assert {:ok, %{activated: true, series: 3, points: 3}} =
             TimelessMetrics.LibsqlMigration.run(data_dir, activate: true)

    assert File.exists?(Path.join(data_dir, "metrics.db.pre-libsql"))
    assert File.dir?(Path.join(data_dir, "rust_engine"))

    start_supervised!(
      {TimelessMetrics,
       name: @store,
       engine: :libsql,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 1}
    )

    assert {:ok, [{10, 1.25}, {20, 2.5}]} =
             TimelessMetrics.query(@store, "cpu", %{"host" => "a"}, from: 0, to: 30)

    assert {:ok, [%{labels: %{"kind" => "catalog"}}]} =
             TimelessMetrics.list_series(@store, "empty")
  end

  defp assert_libsql_startup_refused(data_dir) do
    previous = Process.flag(:trap_exit, true)

    assert {:error, reason} =
             TimelessMetrics.Supervisor.start_link(
               name: @store,
               engine: :libsql,
               # Refusal is now the OPT-OUT behavior; the default
               # auto-converts (covered by libsql_auto_migrate_test.exs).
               auto_migrate: false,
               data_dir: data_dir,
               scraping: false,
               self_monitor: false,
               reader_pool_size: 1
             )

    assert inspect(reason) =~ "refusing to start engine: :libsql"

    receive do
      {:EXIT, _pid, _reason} -> :ok
    after
      0 -> :ok
    end

    Process.flag(:trap_exit, previous)
  end
end
