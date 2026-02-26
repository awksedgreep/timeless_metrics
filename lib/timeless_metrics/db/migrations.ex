defmodule TimelessMetrics.DB.Migrations do
  @moduledoc false

  @max_retries 8

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
    get_version_with_retry(conn, @max_retries)
  end

  defp get_version_with_retry(conn, retries) do
    case Exqlite.Sqlite3.prepare(conn, "SELECT value FROM _metadata WHERE key = 'schema_version'") do
      {:ok, stmt} ->
        result = Exqlite.Sqlite3.step(conn, stmt)
        Exqlite.Sqlite3.release(conn, stmt)

        case result do
          {:row, [version]} -> String.to_integer(version)
          :done -> 0
        end

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        get_version_with_retry(conn, retries - 1)

      {:error, reason} ->
        raise "SQLite get_version failed after retries: #{inspect(reason)}"
    end
  end

  defp set_version(conn, version) do
    execute(conn, "INSERT OR REPLACE INTO _metadata (key, value) VALUES ('schema_version', ?1)", [
      to_string(version)
    ])
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

  defp run_from(conn, 4) do
    execute(conn, "BEGIN")
    execute(conn, "DROP TABLE IF EXISTS raw_segments")
    execute(conn, "DROP TABLE IF EXISTS tier_hourly")
    execute(conn, "DROP TABLE IF EXISTS tier_monthly")
    set_version(conn, 5)
    execute(conn, "COMMIT")
    run_from(conn, 5)
  end

  defp run_from(conn, 5) do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS scrape_targets (
      id                     INTEGER PRIMARY KEY AUTOINCREMENT,
      job_name               TEXT NOT NULL,
      scheme                 TEXT NOT NULL DEFAULT 'http',
      address                TEXT NOT NULL,
      metrics_path           TEXT NOT NULL DEFAULT '/metrics',
      scrape_interval        INTEGER NOT NULL DEFAULT 30,
      scrape_timeout         INTEGER NOT NULL DEFAULT 10,
      labels                 TEXT NOT NULL DEFAULT '{}',
      honor_labels           INTEGER NOT NULL DEFAULT 0,
      honor_timestamps       INTEGER NOT NULL DEFAULT 1,
      relabel_configs        TEXT,
      metric_relabel_configs TEXT,
      enabled                INTEGER NOT NULL DEFAULT 1,
      created_at             INTEGER NOT NULL,
      updated_at             INTEGER NOT NULL,
      UNIQUE(job_name, address, metrics_path)
    )
    """)

    execute(conn, """
    CREATE TABLE IF NOT EXISTS scrape_health (
      target_id          INTEGER PRIMARY KEY,
      health             TEXT NOT NULL DEFAULT 'unknown',
      last_scrape        INTEGER,
      last_duration_ms   INTEGER,
      last_error         TEXT,
      samples_scraped    INTEGER DEFAULT 0,
      FOREIGN KEY (target_id) REFERENCES scrape_targets(id) ON DELETE CASCADE
    )
    """)

    set_version(conn, 6)
    execute(conn, "COMMIT")

    run_from(conn, 6)
  end

  defp run_from(conn, 6) do
    execute(conn, "BEGIN")

    execute(conn, """
    CREATE TABLE IF NOT EXISTS alert_history (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      rule_id      INTEGER NOT NULL,
      rule_name    TEXT NOT NULL,
      metric       TEXT NOT NULL,
      series_labels TEXT NOT NULL,
      state        TEXT NOT NULL,
      value        REAL,
      threshold    REAL,
      condition    TEXT,
      triggered_at INTEGER NOT NULL,
      resolved_at  INTEGER,
      acknowledged INTEGER NOT NULL DEFAULT 0,
      created_at   INTEGER NOT NULL
    )
    """)

    execute(conn, "CREATE INDEX IF NOT EXISTS idx_alert_history_rule ON alert_history(rule_id)")

    execute(
      conn,
      "CREATE INDEX IF NOT EXISTS idx_alert_history_created ON alert_history(created_at)"
    )

    set_version(conn, 7)
    execute(conn, "COMMIT")

    run_from(conn, 7)
  end

  defp run_from(_conn, 7), do: :ok

  defp execute(conn, sql, params \\ []) do
    execute_with_retry(conn, sql, params, @max_retries)
  end

  defp execute_with_retry(conn, sql, params, retries) do
    case Exqlite.Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        if params != [] do
          :ok = Exqlite.Sqlite3.bind(stmt, params)
        end

        :done = Exqlite.Sqlite3.step(conn, stmt)
        Exqlite.Sqlite3.release(conn, stmt)

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        execute_with_retry(conn, sql, params, retries - 1)

      {:error, reason} ->
        raise "SQLite migration failed after retries: #{inspect(reason)} (sql: #{sql})"
    end
  end

  defp retry_backoff(attempt), do: 100 * Integer.pow(2, attempt)
end
