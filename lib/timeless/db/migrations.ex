defmodule Timeless.DB.Migrations do
  @moduledoc false

  def run(conn) do
    create_metadata_table(conn)
    version = get_version(conn)
    run_from(conn, version)
  end

  defp create_metadata_table(conn) do
    execute(conn, """
    CREATE TABLE IF NOT EXISTS _metadata (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) WITHOUT ROWID
    """)
  end

  defp get_version(conn) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT value FROM _metadata WHERE key = 'schema_version'")
    result = Exqlite.Sqlite3.step(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)

    case result do
      {:row, [version]} -> String.to_integer(version)
      :done -> 0
    end
  end

  defp set_version(conn, version) do
    execute(conn, "INSERT OR REPLACE INTO _metadata (key, value) VALUES ('schema_version', ?1)", [to_string(version)])
  end

  defp run_from(conn, version) when version < 1 do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS series (
      id           INTEGER PRIMARY KEY,
      metric_name  TEXT NOT NULL,
      labels       TEXT NOT NULL,
      created_at   INTEGER NOT NULL,
      UNIQUE(metric_name, labels)
    )
    """)

    execute(conn, "CREATE INDEX IF NOT EXISTS idx_series_metric ON series(metric_name)")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS raw_segments (
      series_id    INTEGER NOT NULL,
      start_time   INTEGER NOT NULL,
      end_time     INTEGER NOT NULL,
      point_count  INTEGER NOT NULL,
      data         BLOB NOT NULL,
      PRIMARY KEY (series_id, start_time)
    ) WITHOUT ROWID
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS tier_hourly (
      series_id   INTEGER NOT NULL,
      bucket      INTEGER NOT NULL,
      avg         REAL,
      min         REAL,
      max         REAL,
      count       INTEGER,
      sum         REAL,
      last        REAL,
      PRIMARY KEY (series_id, bucket)
    ) WITHOUT ROWID
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS tier_daily (
      series_id   INTEGER NOT NULL,
      bucket      INTEGER NOT NULL,
      avg         REAL,
      min         REAL,
      max         REAL,
      count       INTEGER,
      sum         REAL,
      last        REAL,
      PRIMARY KEY (series_id, bucket)
    ) WITHOUT ROWID
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS tier_monthly (
      series_id   INTEGER NOT NULL,
      bucket      INTEGER NOT NULL,
      avg         REAL,
      min         REAL,
      max         REAL,
      count       INTEGER,
      sum         REAL,
      last        REAL,
      PRIMARY KEY (series_id, bucket)
    ) WITHOUT ROWID
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS _watermarks (
      tier         TEXT PRIMARY KEY,
      last_bucket  INTEGER NOT NULL
    ) WITHOUT ROWID
    """)

    set_version(conn, 1)
    execute(conn, "COMMIT")

    run_from(conn, 1)
  end

  defp run_from(conn, 1) do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS metric_metadata (
      metric_name  TEXT PRIMARY KEY,
      metric_type  TEXT NOT NULL DEFAULT 'gauge',
      unit         TEXT,
      description  TEXT
    ) WITHOUT ROWID
    """)

    set_version(conn, 2)
    execute(conn, "COMMIT")

    run_from(conn, 2)
  end

  defp run_from(conn, 2) do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS annotations (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp   INTEGER NOT NULL,
      title       TEXT NOT NULL,
      description TEXT,
      tags        TEXT,
      created_at  INTEGER NOT NULL
    )
    """)

    execute(conn, "CREATE INDEX IF NOT EXISTS idx_annotations_time ON annotations(timestamp)")

    set_version(conn, 3)
    execute(conn, "COMMIT")

    run_from(conn, 3)
  end

  defp run_from(conn, 3) do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS alert_rules (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      name        TEXT NOT NULL,
      metric      TEXT NOT NULL,
      labels      TEXT,
      condition   TEXT NOT NULL,
      threshold   REAL NOT NULL,
      duration    INTEGER NOT NULL DEFAULT 0,
      aggregate   TEXT NOT NULL DEFAULT 'avg',
      webhook_url TEXT,
      enabled     INTEGER NOT NULL DEFAULT 1,
      created_at  INTEGER NOT NULL
    )
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS alert_state (
      rule_id        INTEGER NOT NULL,
      series_labels  TEXT NOT NULL,
      state          TEXT NOT NULL,
      triggered_at   INTEGER,
      resolved_at    INTEGER,
      last_value     REAL,
      PRIMARY KEY (rule_id, series_labels)
    ) WITHOUT ROWID
    """)

    set_version(conn, 4)
    execute(conn, "COMMIT")

    run_from(conn, 4)
  end

  defp run_from(_conn, 4), do: :ok

  defp execute(conn, sql, params \\ []) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)

    if params != [] do
      :ok = Exqlite.Sqlite3.bind(stmt, params)
    end

    :done = Exqlite.Sqlite3.step(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)
  end
end
