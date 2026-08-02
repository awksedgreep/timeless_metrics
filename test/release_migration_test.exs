defmodule TimelessMetrics.ReleaseMigrationTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @source_store :release_migration_source
  @target_store :release_migration_target

  test "bounded public batches resume at every transaction boundary and preserve the source" do
    data_dir = temp_dir("metrics_release_migration")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    build_source(data_dir)
    source_before = source_snapshot(data_dir)

    assert {:error, disk_error} =
             TimelessMetrics.ReleaseMigration.stage(data_dir, available_bytes: 0)

    assert disk_error =~ "insufficient disk"
    refute File.exists?(TimelessMetrics.ReleaseMigration.candidate_path(data_dir))

    for point <- [
          :before_batch,
          :disk_full,
          :after_batch_before_journal,
          :after_journal_before_commit
        ] do
      assert {:error, error} =
               TimelessMetrics.ReleaseMigration.stage(data_dir, failpoint: {point, 1})

      assert error =~ "injected migration failure"
      assert journal_count(data_dir) == 0
      assert source_snapshot(data_dir) == source_before
    end

    assert {:error, error} =
             TimelessMetrics.ReleaseMigration.stage(data_dir,
               failpoint: {:after_checkpoint, 1}
             )

    assert error =~ "committed work is resumable"
    assert journal_count(data_dir) == 4_096
    assert source_snapshot(data_dir) == source_before

    assert {:ok, report} = TimelessMetrics.ReleaseMigration.stage(data_dir)
    assert report.phase == :verified
    assert report.series == 3
    assert report.points == 8_195
    assert report.checkpoints == 5
    assert report.retries == 5
    assert report.wal_bytes == 0
    assert report.process_hwm_bytes > 0
    assert report.candidate_bytes > 0
    assert source_snapshot(data_dir) == source_before

    candidate_dir = Path.dirname(TimelessMetrics.ReleaseMigration.candidate_path(data_dir))

    start_supervised!(
      {TimelessMetrics,
       name: @target_store,
       engine: :libsql,
       data_dir: candidate_dir,
       mode: :memory,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 1}
    )

    assert {:ok, rows} =
             TimelessMetrics.query(@target_store, "cpu", %{"host" => "a"},
               from: -10,
               to: 10_000
             )

    # Product label matching is intentionally subset-based, so this query
    # includes both exact identities. The cold migration validator resolves
    # catalog IDs exactly and already proved the two source series were not
    # cross-copied.
    assert length(rows) == 8_195

    assert {:ok, [hourly]} =
             TimelessMetrics.LibsqlEngine.query_rollup(
               @target_store,
               "cpu",
               %{"host" => "a"},
               3_600,
               0,
               7_199
             )

    assert hourly.bucket == 0
    assert hourly.count == 3_600
    assert hourly.last == 359.9
    assert hourly.max == 359.9
    assert hourly.min == 0.0

    assert {:ok, [{1, 99.0}, {2, 100.0}]} =
             TimelessMetrics.query(
               @target_store,
               "cpu",
               %{"host" => "a", "region" => "west"},
               from: -10,
               to: 10_000
             )

    assert {:ok, [%{labels: %{"kind" => "catalog"}}]} =
             TimelessMetrics.list_series(@target_store, "empty")

    stop_supervised!({TimelessMetrics, @target_store})
    TestHelper.await_down(:"#{@target_store}_sup")

    assert {:ok, retry_report} = TimelessMetrics.ReleaseMigration.stage(data_dir)
    assert retry_report.points == report.points
    assert retry_report.identity_digest == report.identity_digest
    assert retry_report.retries == report.retries + 1
    assert source_snapshot(data_dir) == source_before
  end

  test "fresh migration reports scan, public write, maintenance, storage, and HWM costs" do
    data_dir = temp_dir("metrics_release_migration_benchmark")
    on_exit(fn -> File.rm_rf!(data_dir) end)
    build_source(data_dir)

    assert {:ok, report} = TimelessMetrics.ReleaseMigration.stage(data_dir)
    assert report.points == 8_195
    assert report.source_scan_ns > 0
    assert report.public_write_ns > 0
    assert report.compact_ns > 0
    assert report.rollup_ns > 0
    assert report.checkpoint_ns > 0
    assert report.physical_bytes >= report.candidate_bytes
    assert report.process_hwm_bytes > 0

    if System.get_env("TIMELESS_MIGRATION_BENCH") == "1",
      do: IO.inspect(report, label: "metrics migration benchmark")
  end

  defp build_source(data_dir) do
    start_supervised!(
      {TimelessMetrics,
       name: @source_store,
       engine: :rust,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false}
    )

    base =
      for index <- 0..8_192 do
        value = if index == 0, do: -0.0, else: index / 10.0
        {"cpu", %{"host" => "a"}, value, index}
      end

    assert :ok = TimelessMetrics.write_batch(@source_store, base)

    assert :ok =
             TimelessMetrics.write_batch(@source_store, [
               {"cpu", %{"host" => "a", "region" => "west"}, 99.0, 1},
               {"cpu", %{"host" => "a", "region" => "west"}, 100.0, 2}
             ])

    assert {:ok, _} =
             TimelessMetrics.resolve_series(@source_store, "empty", %{"kind" => "catalog"})

    assert :ok = TimelessMetrics.flush(@source_store)

    assert {:ok, [[0, _frames, _checkpointed]]} =
             TimelessMetrics.DB.write(
               :"#{@source_store}_db",
               "PRAGMA wal_checkpoint(TRUNCATE)",
               []
             )

    stop_supervised!({TimelessMetrics, @source_store})
    TestHelper.await_down(:"#{@source_store}_sup")
    await_absent(Path.join(data_dir, "metrics.db-wal"))
    await_absent(Path.join(data_dir, "metrics.db-shm"))
  end

  defp await_absent(path, attempts \\ 1_000)
  defp await_absent(_path, 0), do: :ok

  defp await_absent(path, attempts) do
    if File.exists?(path) do
      Process.sleep(1)
      await_absent(path, attempts - 1)
    else
      :ok
    end
  end

  defp journal_count(data_dir) do
    path = TimelessMetrics.ReleaseMigration.candidate_path(data_dir)
    {:ok, conn} = Exqlite.Sqlite3.open(path, mode: :readonly)

    try do
      {:ok, [[count]]} =
        TimelessMetrics.DB.execute(
          conn,
          "SELECT records_completed FROM _timeless_migration WHERE singleton=1",
          []
        )

      count
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp source_snapshot(data_dir) do
    [Path.join(data_dir, "metrics.db"), Path.join(data_dir, "rust_engine")]
    |> Enum.flat_map(&regular_files/1)
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)
      {Path.relative_to(path, data_dir), stat.size, stat.mtime, sha256(path)}
    end)
  end

  defp regular_files(path) do
    if File.dir?(path) do
      path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
    else
      [path]
    end
  end

  defp sha256(path) do
    :crypto.hash(:sha256, File.read!(path))
  end

  defp temp_dir(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end
