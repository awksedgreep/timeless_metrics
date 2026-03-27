defmodule TimelessMetrics.BackupTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @data_dir "/tmp/timeless_backup_test_#{System.os_time(:millisecond)}"
  @backup_dir "/tmp/timeless_backup_target_#{System.os_time(:millisecond)}"
  @port 18_402

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :backup_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:backup_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:backup_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:backup_test, nil})
    start_supervised!({TimelessMetrics, name: :backup_test, data_dir: @data_dir})

    on_exit(fn ->
      TestHelper.await_down(:backup_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:backup_test, nil})
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

    main_path = Path.join(@backup_dir, "metrics.db")
    assert File.exists?(main_path)
    {:ok, conn} = Exqlite.Sqlite3.open(main_path, mode: :readonly)
    # Verify SQLite is valid and readable
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(conn, "SELECT name FROM sqlite_master WHERE type='table'")

    {:row, _} = Exqlite.Sqlite3.step(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)
    Exqlite.Sqlite3.close(conn)
  end

  test "backup includes shard directories" do
    now = System.os_time(:second)

    for i <- 0..4 do
      TimelessMetrics.write(:backup_test, "metric_#{i}", %{"k" => "v"}, i * 1.0,
        timestamp: now - 60
      )
    end

    TimelessMetrics.flush(:backup_test)

    {:ok, result} = TimelessMetrics.backup(:backup_test, @backup_dir)

    assert "metrics.db" in result.files
    # Rust engine uses "rust_engine", legacy uses "shard_*"
    has_data =
      "rust_engine" in result.files or
        Enum.any?(result.files, &String.starts_with?(&1, "shard_"))

    assert has_data
  end

  test "backup during active writes does not crash" do
    now = System.os_time(:second)

    writer =
      Task.async(fn ->
        for i <- 0..99 do
          TimelessMetrics.write(:backup_test, "load", %{"id" => "w1"}, i * 1.0,
            timestamp: now - 1000 + i * 10
          )
        end
      end)

    Process.sleep(10)

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

    body = :json.encode(%{path: http_backup_dir}) |> IO.iodata_to_binary()

    resp =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/backup", body,
        content_type: "application/json"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "ok"
    assert result["path"] == http_backup_dir
    assert is_list(result["files"])
    assert result["total_bytes"] > 0
    assert File.exists?(Path.join(http_backup_dir, "metrics.db"))
  end

  test "HTTP POST /api/v1/backup uses default path when no body" do
    now = System.os_time(:second)

    TimelessMetrics.write(:backup_test, "default_path", %{"a" => "b"}, 1.0, timestamp: now - 60)
    TimelessMetrics.flush(:backup_test)

    resp = TimelessMetrics.TestHTTP.post(@port, "/api/v1/backup", "")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert result["status"] == "ok"
    assert String.contains?(result["path"], "backups")

    on_exit(fn -> File.rm_rf!(result["path"]) end)
  end
end
