defmodule TimelessMetrics.ReleaseStartupTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.{DB, LegacyReader, LibsqlEngine, ReleaseStartup, TestHelper}

  test "fresh startup creates the canonical public metrics vtab and is idempotent" do
    root = temp_dir("metrics_startup_fresh")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, %{state: :fresh, target_path: target}} = ReleaseStartup.detect(root, opts())
    assert Path.basename(target) == "metrics.db"
    assert {:ok, %{state: :valid_libsql, ready: true}} = ReleaseStartup.prepare(root, opts())
    assert {:ok, %{state: :valid_libsql, ready: true}} = ReleaseStartup.prepare(root, opts())

    incompatible = temp_dir("metrics_startup_incompatible_extension")
    on_exit(fn -> File.rm_rf!(incompatible) end)

    assert {:error, %{state: :incompatible_version, ready: false}} =
             ReleaseStartup.prepare(incompatible, extension_path: "/missing/timeless-ext.so")

    refute File.exists?(Path.join(incompatible, "metrics.db"))
  end

  test "Rust blocks resume across copy, seal, and rename crashes with the source retained" do
    root = temp_dir("metrics_startup_cutover")
    on_exit(fn -> File.rm_rf!(root) end)
    build_source(root, 17)
    before = source_snapshot(root)

    assert {:ok, %{state: :legacy, generation: :rust_block, target_path: target}} =
             ReleaseStartup.detect(root, opts())

    assert Path.basename(target) == "metrics.libsql.db"

    assert {:error, %{error: checkpoint_error}} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    assert checkpoint_error =~ "committed work is resumable"
    migration_before_detection = migration_fingerprint(root)
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert %{
             records_completed: 17,
             records_total: 17,
             ready: false,
             candidate_physical_bytes: candidate_bytes,
             wal_bytes: wal_bytes
           } = ReleaseStartup.stats(root, opts())

    assert candidate_bytes > 0
    assert wal_bytes >= 0
    assert migration_fingerprint(root) == migration_before_detection

    assert source_snapshot(root) == before

    assert {:error, _} =
             ReleaseStartup.prepare(root, Keyword.merge(opts(), failpoint: :after_seal))

    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert {:error, _renamed_error} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: :after_rename_before_fsync)
             )

    assert {:ok,
            %{
              state: :completed_cutover,
              ready: true,
              source_retained: true,
              source_manifest_digest: digest,
              target_path: target
            }} = ReleaseStartup.detect(root, opts())

    assert Path.basename(target) == "metrics.libsql.db"
    assert source_snapshot(root) == before
    refute File.exists?(TimelessMetrics.ReleaseMigration.candidate_path(root))

    assert {:ok, legacy_reader} = LegacyReader.open(Path.join(root, "rust_engine"))
    assert {:ok, [{"cpu", %{"host" => "a"}}]} = LegacyReader.series(legacy_reader)

    conn = LibsqlEngine.open_connection(target, extension_path())

    try do
      assert {:ok, rows} =
               DB.execute(
                 conn,
                 "SELECT points FROM timeless_raw_batches('metric_samples','cpu',NULL,?1,?2)",
                 [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807]
               )

      assert Enum.sum_by(rows, fn [blob] -> length(LibsqlEngine.decode_point_batch(blob)) end) ==
               17
    after
      Exqlite.Sqlite3.close(conn)
    end

    assert {:ok, %{source_retained: false}} = ReleaseStartup.cleanup_legacy(root, digest, opts())
    refute File.exists?(Path.join(root, "metrics.db"))
    refute File.exists?(Path.join(root, "rust_engine"))

    assert {:ok, %{state: :completed_cutover, source_retained: false}} =
             ReleaseStartup.detect(root, opts())
  end

  test "source drift, dual libSQL targets, future schema, and future journal fail closed" do
    drift = temp_dir("metrics_startup_drift")
    on_exit(fn -> File.rm_rf!(drift) end)
    build_source(drift, 3)

    assert {:error, _} =
             ReleaseStartup.prepare(
               drift,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    [chunk | _] =
      drift
      |> Path.join("rust_engine")
      |> regular_files()
      |> Enum.reject(&String.ends_with?(&1, "series.json"))

    File.write!(chunk, "drift", [:append])
    assert {:ok, %{state: :corruption, error: error}} = ReleaseStartup.detect(drift, opts())
    assert error =~ "fingerprint drifted"

    dual = temp_dir("metrics_startup_dual")
    on_exit(fn -> File.rm_rf!(dual) end)
    create_target(Path.join(dual, "metrics.db"))
    create_target(Path.join(dual, "metrics.libsql.db"))
    assert {:ok, %{state: :ambiguous_dual_store}} = ReleaseStartup.detect(dual, opts())

    future = temp_dir("metrics_startup_future")
    on_exit(fn -> File.rm_rf!(future) end)
    target = Path.join(future, "metrics.db")
    create_target(target)
    conn = LibsqlEngine.open_connection(target, extension_path())

    assert {:ok, _} =
             DB.execute(
               conn,
               """
               CREATE TABLE _timeless_schema_migrations(
                 signal TEXT NOT NULL,version INTEGER NOT NULL,applied_at_unix INTEGER NOT NULL,
                 server_version TEXT NOT NULL,extension_version TEXT NOT NULL,
                 extension_data_abi INTEGER NOT NULL,PRIMARY KEY(signal,version)
               ) STRICT
               """,
               []
             )

    assert {:ok, _} =
             DB.execute(
               conn,
               "INSERT INTO _timeless_schema_migrations VALUES ('metrics',2,unixepoch(),'future','future',1)",
               []
             )

    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :incompatible_version}} = ReleaseStartup.detect(future, opts())

    journal = temp_dir("metrics_startup_future_journal")
    on_exit(fn -> File.rm_rf!(journal) end)
    build_source(journal, 1)

    assert {:error, _} =
             ReleaseStartup.prepare(
               journal,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    candidate = TimelessMetrics.ReleaseMigration.candidate_path(journal)
    {:ok, conn} = Exqlite.Sqlite3.open(candidate)
    assert {:ok, _} = DB.execute(conn, "UPDATE _timeless_migration SET version=2", [])
    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :incompatible_version}} = ReleaseStartup.detect(journal, opts())
  end

  test "truncated targets, wrong-signal vtabs, incomplete legacy pairs, and owner conflicts stop startup" do
    corrupt = temp_dir("metrics_startup_corrupt")
    on_exit(fn -> File.rm_rf!(corrupt) end)
    File.write!(Path.join(corrupt, "metrics.db"), "truncated")
    assert {:ok, %{state: :corruption}} = ReleaseStartup.detect(corrupt, opts())

    wrong = temp_dir("metrics_startup_wrong")
    on_exit(fn -> File.rm_rf!(wrong) end)
    path = Path.join(wrong, "metrics.db")
    {:ok, conn} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, true)
    assert {:ok, _} = DB.execute(conn, "SELECT load_extension(?1)", [extension_path()])
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, false)
    assert {:ok, _} = DB.execute(conn, "CREATE VIRTUAL TABLE logs USING timeless_logs", [])
    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :corruption}} = ReleaseStartup.detect(wrong, opts())

    incomplete = temp_dir("metrics_startup_incomplete")
    on_exit(fn -> File.rm_rf!(incomplete) end)
    File.mkdir_p!(Path.join(incomplete, "rust_engine"))
    File.write!(Path.join([incomplete, "rust_engine", "orphan"]), "data")

    assert {:ok, %{state: :corruption, error: pair_error}} =
             ReleaseStartup.detect(incomplete, opts())

    assert pair_error =~ "without its recognized legacy metrics.db"

    owner = temp_dir("metrics_startup_owner")
    on_exit(fn -> File.rm_rf!(owner) end)
    owner_dir = Path.join([owner, ".timeless-migration", "metrics"])
    File.mkdir_p!(owner_dir)
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(owner_dir, "owner.db"))
    assert {:ok, _} = DB.execute(conn, "CREATE TABLE owner(singleton INTEGER PRIMARY KEY)", [])
    assert {:ok, _} = DB.execute(conn, "BEGIN EXCLUSIVE", [])
    assert {:error, %{error: owner_error}} = ReleaseStartup.prepare(owner, opts())
    assert owner_error =~ "owner is active"
    assert {:ok, _} = DB.execute(conn, "ROLLBACK", [])
    Exqlite.Sqlite3.close(conn)

    locked = temp_dir("metrics_startup_legacy_owner")
    on_exit(fn -> File.rm_rf!(locked) end)
    build_source(locked, 1)
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(locked, "metrics.db"))
    assert {:ok, _} = DB.execute(conn, "BEGIN EXCLUSIVE", [])
    assert {:error, %{error: legacy_owner_error}} = ReleaseStartup.prepare(locked, opts())
    assert legacy_owner_error =~ "active legacy metrics SQLite owner"
    assert {:ok, _} = DB.execute(conn, "ROLLBACK", [])
    Exqlite.Sqlite3.close(conn)
  end

  test "process kill after sealing a metrics candidate resumes without source mutation" do
    root = temp_dir("metrics_startup_kill")
    on_exit(fn -> File.rm_rf!(root) end)
    build_source(root, 5)
    before = source_snapshot(root)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          ReleaseStartup.prepare(
            root,
            Keyword.merge(opts(), pause_at: :after_seal, notify: parent)
          )

        send(parent, {:unexpected_startup_result, result})
      end)

    assert_receive {:startup_paused, ^pid, :after_seal}, 5_000
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000
    refute_receive {:unexpected_startup_result, _}

    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())
    assert {:ok, %{state: :completed_cutover}} = ReleaseStartup.prepare(root, opts())
    assert source_snapshot(root) == before
  end

  defp build_source(root, count) do
    store = :"metrics_startup_source_#{System.unique_integer([:positive])}"

    start_supervised!(
      {TimelessMetrics,
       name: store, engine: :rust, data_dir: root, scraping: false, self_monitor: false}
    )

    points = for index <- 0..(count - 1), do: {"cpu", %{"host" => "a"}, index / 10.0, index}
    assert :ok = TimelessMetrics.write_batch(store, points)
    assert :ok = TimelessMetrics.flush(store)

    assert {:ok, [[0, _frames, _checkpointed]]} =
             DB.write(:"#{store}_db", "PRAGMA wal_checkpoint(TRUNCATE)", [])

    stop_supervised!({TimelessMetrics, store})
    TestHelper.await_down(:"#{store}_sup")
    await_absent(Path.join(root, "metrics.db-wal"))
    await_absent(Path.join(root, "metrics.db-shm"))
  end

  defp create_target(path) do
    :ok =
      LibsqlEngine.initialize_release_database(
        path,
        TimelessMetrics.Schema.default(),
        extension_path()
      )
  end

  defp source_snapshot(root) do
    [Path.join(root, "metrics.db"), Path.join(root, "rust_engine")]
    |> Enum.flat_map(&regular_files/1)
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  defp migration_fingerprint(root) do
    path = TimelessMetrics.ReleaseMigration.candidate_path(root)
    conn = LibsqlEngine.open_readonly_connection(path, extension_path())

    try do
      for sql <- [
            "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
            "SELECT * FROM _timeless_migration",
            "SELECT * FROM _timeless_migration_events ORDER BY sequence",
            "SELECT COUNT(*) FROM metric_samples"
          ] do
        assert {:ok, rows} = DB.execute(conn, sql, [])
        rows
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp regular_files(path) do
    if File.dir?(path) do
      path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
    else
      [path]
    end
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

  defp opts, do: [extension_path: extension_path()]

  defp extension_path do
    System.get_env("TIMELESS_EXT_PATH") ||
      Path.expand("../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)
  end

  defp temp_dir(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end
