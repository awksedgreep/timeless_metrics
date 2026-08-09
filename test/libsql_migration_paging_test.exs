defmodule TimelessMetrics.LibsqlMigrationPagingTest do
  @moduledoc """
  The rust_engine → libSQL conversion streams the source a page at a time
  instead of materialising whole series, so peak memory tracks the page size
  rather than the largest series.

  These tests exercise the paths a single-page fixture cannot: series larger
  than one page, values whose exact bits must survive, and a verification step
  that no longer has both sides in memory to compare.

  See docs/2026-08-09_libsql_conversion_memory.md.
  """

  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @store :libsql_migration_paging_source

  # The reader's page limit is 4096, so this spans several pages and lands on a
  # boundary rather than just past one.
  @multi_page_points 12_288

  defp data_dir do
    Path.join(
      System.tmp_dir!(),
      "timeless_migration_paging_#{System.unique_integer([:positive])}"
    )
  end

  defp start_source(dir) do
    start_supervised!(
      {TimelessMetrics,
       name: @store, engine: :rust, data_dir: dir, scraping: false, self_monitor: false}
    )
  end

  defp stop_source do
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")
  end

  defp staged_points(dir, metric, labels) do
    stage = Path.join(dir, ".libsql-migration/metrics.db")
    conn = TimelessMetrics.LibsqlEngine.open_connection(stage)

    try do
      store = :"paging_verify_#{System.unique_integer([:positive])}"

      start_supervised!(
        {TimelessMetrics,
         name: store,
         engine: :libsql,
         data_dir: Path.join(dir, ".libsql-migration"),
         mode: :memory,
         scraping: false,
         self_monitor: false}
      )

      {:ok, points} =
        TimelessMetrics.LibsqlEngine.query_raw(store, metric, labels,
          from: -9_223_372_036_854_775_808,
          to: 9_223_372_036_854_775_807
        )

      stop_supervised!({TimelessMetrics, store})
      Enum.sort_by(points, &elem(&1, 0))
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  test "a series spanning many pages migrates completely and verifies" do
    dir = data_dir()
    on_exit(fn -> File.rm_rf!(dir) end)

    start_source(dir)

    expected =
      for i <- 1..@multi_page_points do
        # Values that are not round: a lossy round-trip would change the digest.
        {i, i * 1.0e-3 + 0.1}
      end

    expected
    |> Enum.chunk_every(2_000)
    |> Enum.each(fn chunk ->
      batch = Enum.map(chunk, fn {ts, value} -> {"paged", %{"host" => "a"}, value, ts} end)
      assert :ok = TimelessMetrics.write_batch(@store, batch)
    end)

    assert :ok = TimelessMetrics.flush(@store)
    stop_source()

    assert {:ok, %{activated: false, series: 1, points: @multi_page_points}} =
             TimelessMetrics.LibsqlMigration.run(dir)

    migrated = staged_points(dir, "paged", %{"host" => "a"})

    assert length(migrated) == @multi_page_points
    assert migrated == expected
  end

  test "values keep their exact bits across the paged copy" do
    dir = data_dir()
    on_exit(fn -> File.rm_rf!(dir) end)

    start_source(dir)

    # Signed zero, subnormals, and the float extremes are the values a lossy
    # copy or a sloppy comparison would quietly damage.
    tricky = [
      {1, 0.0},
      {2, -0.0},
      {3, 1.0e-308},
      {4, -1.0e-308},
      {5, 1.7976931348623157e308},
      {6, -1.7976931348623157e308},
      {7, 0.1 + 0.2},
      {8, -123_456.789}
    ]

    batch = Enum.map(tricky, fn {ts, value} -> {"exact", %{"k" => "v"}, value, ts} end)
    assert :ok = TimelessMetrics.write_batch(@store, batch)
    assert :ok = TimelessMetrics.flush(@store)
    stop_source()

    assert {:ok, %{points: 8}} = TimelessMetrics.LibsqlMigration.run(dir)

    migrated = staged_points(dir, "exact", %{"k" => "v"})

    for {{_ts, expected}, {_ts2, actual}} <- Enum.zip(tricky, migrated) do
      assert <<expected::float-native-64>> == <<actual::float-native-64>>
    end
  end

  test "a series with no points still migrates and verifies" do
    dir = data_dir()
    on_exit(fn -> File.rm_rf!(dir) end)

    start_source(dir)

    assert {:ok, _sid} = TimelessMetrics.resolve_series(@store, "empty", %{"kind" => "catalog"})
    assert :ok = TimelessMetrics.write_batch(@store, [{"has_points", %{}, 1.0, 1}])
    assert :ok = TimelessMetrics.flush(@store)
    stop_source()

    assert {:ok, %{series: 2, points: 1}} = TimelessMetrics.LibsqlMigration.run(dir)
  end

  test "many points sharing one timestamp survive the window walk" do
    dir = data_dir()
    on_exit(fn -> File.rm_rf!(dir) end)

    start_source(dir)

    # Verification sizes its windows from timestamp span over point count. When
    # the span collapses to a single timestamp that arithmetic must still yield
    # a usable window instead of dividing itself into nothing.
    batch = for i <- 1..500, do: {"collapsed", %{"n" => "1"}, i * 1.0, 42}
    assert :ok = TimelessMetrics.write_batch(@store, batch)
    assert :ok = TimelessMetrics.flush(@store)
    stop_source()

    assert {:ok, %{series: 1}} = TimelessMetrics.LibsqlMigration.run(dir)
  end
end
