defmodule TimelessMetrics.BackupTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_backup_test_#{System.os_time(:millisecond)}"
  @backup_dir "/tmp/timeless_backup_target_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!({TimelessMetrics, name: :backup_test, data_dir: @data_dir, buffer_shards: 2})

    on_exit(fn ->
      File.rm_rf!(@data_dir)
      File.rm_rf!(@backup_dir)
    end)

    :ok
  end

  test "backup creates valid SQLite files in target directory" do
    now = System.os_time(:second)

    for i <- 0..4 do
      TimelessMetrics.write(:backup_test, "cpu", %{"host" => "h1"}, 50.0 + i,
        timestamp: now - 300 + i * 60
      )
    end

    TimelessMetrics.flush(:backup_test)

    {:ok, result} = TimelessMetrics.backup(:backup_test, @backup_dir)

    assert result.path == @backup_dir
    assert "metrics.db" in result.files
    assert result.total_bytes > 0

    # Verify main DB is valid SQLite
    main_path = Path.join(@backup_dir, "metrics.db")
    assert File.exists?(main_path)
    {:ok, conn} = Exqlite.Sqlite3.open(main_path, mode: :readonly)
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT COUNT(*) FROM series")
    {:row, [count]} = Exqlite.Sqlite3.step(conn, stmt)
    assert count >= 1
    Exqlite.Sqlite3.release(conn, stmt)
    Exqlite.Sqlite3.close(conn)
  end

  test "backup includes all shard files" do
    now = System.os_time(:second)

    # Write to multiple series to spread across shards
    for i <- 0..9 do
      TimelessMetrics.write(:backup_test, "metric_#{i}", %{"k" => "v"}, i * 1.0,
        timestamp: now - 60
      )
    end

    TimelessMetrics.flush(:backup_test)

    {:ok, result} = TimelessMetrics.backup(:backup_test, @backup_dir)

    # 2 shards configured (directories, not .db files)
    assert "shard_0" in result.files
    assert "shard_1" in result.files
    assert length(result.files) == 3  # metrics.db + 2 shards
  end

  test "backed-up shard DB is independently queryable" do
    now = System.os_time(:second)

    for i <- 0..4 do
      TimelessMetrics.write(:backup_test, "temp", %{"sensor" => "s1"}, 20.0 + i,
        timestamp: now - 300 + i * 60
      )
    end

    TimelessMetrics.flush(:backup_test)

    {:ok, _result} = TimelessMetrics.backup(:backup_test, @backup_dir)

    # Verify raw segment files were backed up (WAL or .seg files)
    total_raw_files =
      Enum.reduce(0..1, 0, fn i, acc ->
        raw_dir = Path.join(@backup_dir, "shard_#{i}/raw")
        case File.ls(raw_dir) do
          {:ok, files} ->
            raw_count = Enum.count(files, fn f ->
              String.ends_with?(f, ".seg") or String.ends_with?(f, ".wal")
            end)
            acc + raw_count
          _ -> acc
        end
      end)

    assert total_raw_files >= 1
  end

  test "backup during active writes does not crash" do
    now = System.os_time(:second)

    # Start background writes
    writer =
      Task.async(fn ->
        for i <- 0..99 do
          TimelessMetrics.write(:backup_test, "load", %{"id" => "w1"}, i * 1.0,
            timestamp: now - 1000 + i * 10
          )
        end
      end)

    # Give writes a head start
    Process.sleep(10)

    # Backup while writes are happening
    {:ok, result} = TimelessMetrics.backup(:backup_test, @backup_dir)
    assert result.total_bytes > 0

    Task.await(writer)
  end

  test "HTTP POST /api/v1/backup triggers backup with custom path" do
    now = System.os_time(:second)

    TimelessMetrics.write(:backup_test, "http_metric", %{"x" => "y"}, 42.0, timestamp: now - 60)
    TimelessMetrics.flush(:backup_test)

    http_backup_dir = @backup_dir <> "_http"

    on_exit(fn -> File.rm_rf!(http_backup_dir) end)

    body = Jason.encode!(%{path: http_backup_dir})

    conn =
      Plug.Test.conn(:post, "/api/v1/backup", body)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> TimelessMetrics.HTTP.call(store: :backup_test)

    assert conn.status == 200
    resp = Jason.decode!(conn.resp_body)
    assert resp["status"] == "ok"
    assert resp["path"] == http_backup_dir
    assert is_list(resp["files"])
    assert resp["total_bytes"] > 0
    assert File.exists?(Path.join(http_backup_dir, "metrics.db"))
  end

  test "HTTP POST /api/v1/backup uses default path when no body" do
    now = System.os_time(:second)

    TimelessMetrics.write(:backup_test, "default_path", %{"a" => "b"}, 1.0, timestamp: now - 60)
    TimelessMetrics.flush(:backup_test)

    conn =
      Plug.Test.conn(:post, "/api/v1/backup", "")
      |> TimelessMetrics.HTTP.call(store: :backup_test)

    assert conn.status == 200
    resp = Jason.decode!(conn.resp_body)
    assert resp["status"] == "ok"
    assert String.contains?(resp["path"], "backups")

    on_exit(fn -> File.rm_rf!(resp["path"]) end)
  end
end
