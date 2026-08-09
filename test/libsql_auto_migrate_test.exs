defmodule TimelessMetrics.LibsqlAutoMigrateTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @store :libsql_auto_migrate_source

  test "starting engine: :libsql over a rust_engine/ store converts it automatically" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "timeless_auto_migrate_test_#{System.unique_integer([:positive])}"
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

    assert :ok = TimelessMetrics.flush(@store)
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")

    # Default startup on :libsql runs the staged/verified/activated
    # migration automatically, then serves the converted data.
    start_supervised!(
      {TimelessMetrics,
       name: @store,
       engine: :libsql,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 1}
    )

    assert {:ok, points} =
             TimelessMetrics.query(@store, "cpu", %{"host" => "a"}, from: 0, to: 100)

    assert length(points) == 2

    # Rollback material is preserved, exactly like the manual migration.
    assert File.exists?(Path.join(data_dir, "metrics.db.pre-libsql"))
    assert File.dir?(Path.join(data_dir, "rust_engine"))

    # A restart does not re-convert: the activation marker short-circuits.
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")

    start_supervised!(
      {TimelessMetrics,
       name: @store,
       engine: :libsql,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 1}
    )

    assert {:ok, points} =
             TimelessMetrics.query(@store, "cpu", %{"host" => "b"}, from: 0, to: 100)

    assert length(points) == 1
  end
end
