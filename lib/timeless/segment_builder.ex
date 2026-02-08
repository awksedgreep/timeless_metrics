defmodule Timeless.SegmentBuilder do
  @moduledoc """
  Accumulates points per series and writes gorilla-compressed segments to a
  per-shard SQLite database.

  Each SegmentBuilder owns its own shard DB file (`shard_{i}.db`), eliminating
  the single-writer bottleneck. Points arrive in batches from the paired Buffer
  shard. The SegmentBuilder groups them into time-bounded segments (default 4h),
  compresses with GorillaStream + zstd, and writes directly to its shard DB.
  """

  use GenServer

  defstruct [
    :segments,
    :segment_duration,
    :compression,
    :writer,
    :readers,
    :shard_id,
    :data_dir,
    :db_path,
    :name,
    :schema
  ]

  @default_segment_duration 14_400  # 4 hours in seconds

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Ingest a batch of grouped points. Called by Buffer shards after flush."
  def ingest(builder, grouped_points) do
    GenServer.cast(builder, {:ingest, grouped_points})
  end

  @doc "Synchronous ingest. Used during shutdown to ensure data is received before termination."
  def ingest_sync(builder, grouped_points) do
    GenServer.call(builder, {:ingest, grouped_points}, :infinity)
  end

  @doc "Force flush all open segments to disk."
  def flush(builder) do
    GenServer.call(builder, :flush, :infinity)
  end

  @doc """
  Read from this shard's DB using a reader connection. Lock-free (no GenServer).

  Returns `{:ok, rows}`.
  """
  def read_shard(builder_name, sql, params \\ []) do
    readers = :persistent_term.get({__MODULE__, builder_name, :readers})
    reader = Enum.random(readers)
    execute(reader, sql, params)
  end

  @doc """
  Execute a write transaction on this shard's DB.
  The callback receives the writer connection. Goes through GenServer for write serialization.
  Returns `{:ok, result}` or `{:error, exception}`.
  """
  def write_transaction_shard(builder_name, fun) do
    GenServer.call(builder_name, {:write_transaction, fun}, :infinity)
  end

  @doc """
  Delete from this shard's DB. Goes through the GenServer for write serialization.
  """
  def delete_shard(builder_name, sql, params \\ []) do
    GenServer.call(builder_name, {:delete_shard, sql, params}, :infinity)
  end

  @doc "Get the shard database path."
  def shard_db_path(builder_name) do
    :persistent_term.get({__MODULE__, builder_name, :db_path})
  end

  # --- Server ---

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_id = Keyword.fetch!(opts, :shard_id)
    data_dir = Keyword.fetch!(opts, :data_dir)
    segment_duration = Keyword.get(opts, :segment_duration, @default_segment_duration)
    compression = Keyword.get(opts, :compression, :zstd)
    schema = Keyword.get(opts, :schema)

    Process.flag(:trap_exit, true)

    # Open shard DB
    File.mkdir_p!(data_dir)
    db_path = Path.join(data_dir, "shard_#{shard_id}.db")

    {:ok, writer} = Exqlite.Sqlite3.open(db_path)
    configure_writer(writer)
    create_raw_segments_table(writer)

    # Create tier tables and watermarks if schema is provided
    if schema do
      create_tier_tables(writer, schema)
    end

    reader_count = max(System.schedulers_online() |> div(2), 2)

    readers =
      for _ <- 1..reader_count do
        {:ok, conn} = Exqlite.Sqlite3.open(db_path)
        configure_reader(conn)
        conn
      end

    # Store readers and db_path in persistent_term for lock-free access
    :persistent_term.put({__MODULE__, name, :readers}, readers)
    :persistent_term.put({__MODULE__, name, :db_path}, db_path)

    state = %__MODULE__{
      segments: %{},
      segment_duration: segment_duration,
      compression: compression,
      writer: writer,
      readers: readers,
      shard_id: shard_id,
      data_dir: data_dir,
      db_path: db_path,
      name: name,
      schema: schema
    }

    # Periodic check for completed segments
    Process.send_after(self(), :check_segments, :timer.seconds(10))

    {:ok, state}
  end

  @impl true
  def handle_cast({:ingest, grouped_points}, state) do
    new_segments =
      Enum.reduce(grouped_points, state.segments, fn {series_id, points}, segments ->
        Enum.reduce(points, segments, fn {ts, val}, segs ->
          bucket = segment_bucket(ts, state.segment_duration)
          key = {series_id, bucket}

          seg =
            Map.get(segs, key, %{
              series_id: series_id,
              start_time: bucket,
              end_time: bucket + state.segment_duration,
              points: []
            })

          updated = %{seg | points: [{ts, val} | seg.points]}
          Map.put(segs, key, updated)
        end)
      end)

    # Check if any segments are complete (their time window has passed)
    {completed, pending} = split_completed(new_segments, state.segment_duration)

    if completed != [] do
      write_segments(completed, state)
    end

    {:noreply, %{state | segments: pending}}
  end

  @impl true
  def handle_call({:ingest, grouped_points}, _from, state) do
    {:noreply, new_state} = handle_cast({:ingest, grouped_points}, state)
    {:reply, :ok, new_state}
  end

  def handle_call(:flush, _from, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    {:reply, :ok, %{state | segments: %{}}}
  end

  def handle_call({:write_transaction, fun}, _from, state) do
    result = run_write_transaction(state.writer, fun)
    {:reply, result, state}
  end

  def handle_call({:delete_shard, sql, params}, _from, state) do
    result = execute(state.writer, sql, params)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:check_segments, state) do
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    if completed != [] do
      write_segments(completed, state)
    end

    Process.send_after(self(), :check_segments, :timer.seconds(10))
    {:noreply, %{state | segments: pending}}
  end

  @impl true
  def terminate(_reason, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    # Clean up persistent_term
    :persistent_term.erase({__MODULE__, state.name, :readers})
    :persistent_term.erase({__MODULE__, state.name, :db_path})

    # Close connections
    Exqlite.Sqlite3.close(state.writer)
    Enum.each(state.readers, &Exqlite.Sqlite3.close/1)

    :ok
  end

  # --- Internals ---

  defp segment_bucket(timestamp, duration) do
    div(timestamp, duration) * duration
  end

  defp split_completed(segments, duration) do
    now = System.os_time(:second)
    current_bucket = segment_bucket(now, duration)

    {completed_map, pending_map} =
      Enum.split_with(segments, fn {{_series_id, bucket}, _seg} ->
        bucket < current_bucket
      end)

    completed = Enum.map(completed_map, fn {_key, seg} -> seg end)
    pending = Map.new(pending_map)

    {completed, pending}
  end

  @insert_sql "INSERT OR REPLACE INTO raw_segments (series_id, start_time, end_time, point_count, data) VALUES (?1, ?2, ?3, ?4, ?5)"

  defp write_segments(segments, state) do
    # Phase 1: Compress outside the write lock (CPU-bound, runs in this builder's process)
    compressed =
      Enum.flat_map(segments, fn seg ->
        sorted_points = Enum.sort_by(seg.points, &elem(&1, 0))

        case GorillaStream.compress(sorted_points, compression: state.compression) do
          {:ok, blob} ->
            point_count = length(sorted_points)
            {first_ts, _} = List.first(sorted_points)
            {last_ts, _} = List.last(sorted_points)

            :telemetry.execute(
              [:timeless, :segment, :write],
              %{point_count: point_count, compressed_bytes: byte_size(blob)},
              %{series_id: seg.series_id}
            )

            [{seg.series_id, first_ts, last_ts, point_count, blob}]

          {:error, reason} ->
            require Logger
            Logger.warning("Failed to compress segment for series #{seg.series_id}: #{inspect(reason)}")
            []
        end
      end)

    # Phase 2: Write compressed blobs directly to shard DB (no GenServer bottleneck)
    if compressed != [] do
      execute(state.writer, "BEGIN IMMEDIATE", [])

      try do
        {:ok, stmt} = Exqlite.Sqlite3.prepare(state.writer, @insert_sql)

        try do
          Enum.each(compressed, fn {series_id, first_ts, last_ts, point_count, blob} ->
            :ok = Exqlite.Sqlite3.bind(stmt, [series_id, first_ts, last_ts, point_count, blob])
            :done = Exqlite.Sqlite3.step(state.writer, stmt)
            :ok = Exqlite.Sqlite3.reset(stmt)
          end)
        after
          Exqlite.Sqlite3.release(state.writer, stmt)
        end

        execute(state.writer, "COMMIT", [])
      rescue
        e ->
          try do
            execute(state.writer, "ROLLBACK", [])
          rescue
            _ -> :ok  # Transaction may have auto-rolled back
          end

          require Logger
          Logger.warning("Shard write failed: #{inspect(e)}")
      end
    end
  end

  defp run_write_transaction(conn, fun) do
    execute(conn, "BEGIN IMMEDIATE", [])

    try do
      result = fun.(conn)
      execute(conn, "COMMIT", [])
      {:ok, result}
    rescue
      e ->
        try do
          execute(conn, "ROLLBACK", [])
        rescue
          _ -> :ok
        end

        {:error, e}
    end
  end

  defp create_tier_tables(conn, schema) do
    execute(conn, """
    CREATE TABLE IF NOT EXISTS _watermarks (
      tier         TEXT PRIMARY KEY,
      last_bucket  INTEGER NOT NULL
    ) WITHOUT ROWID
    """, [])

    Enum.each(schema.tiers, fn tier ->
      execute(conn, """
      CREATE TABLE IF NOT EXISTS #{tier.table_name} (
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
      """, [])

      execute(conn, "INSERT OR IGNORE INTO _watermarks (tier, last_bucket) VALUES (?1, 0)",
        [to_string(tier.name)])
    end)
  end

  defp configure_writer(conn) do
    pragmas = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA cache_size = -64000",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA mmap_size = 2147483648",
      "PRAGMA wal_autocheckpoint = 10000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  defp configure_reader(conn) do
    pragmas = [
      "PRAGMA mmap_size = 2147483648",
      "PRAGMA cache_size = -4000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  defp create_raw_segments_table(conn) do
    execute(conn, """
    CREATE TABLE IF NOT EXISTS raw_segments (
      series_id    INTEGER NOT NULL,
      start_time   INTEGER NOT NULL,
      end_time     INTEGER NOT NULL,
      point_count  INTEGER NOT NULL,
      data         BLOB NOT NULL,
      PRIMARY KEY (series_id, start_time)
    ) WITHOUT ROWID
    """, [])
  end

  @doc false
  def execute(conn, sql, params) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)

    if params != [] do
      :ok = Exqlite.Sqlite3.bind(stmt, params)
    end

    rows = fetch_all(conn, stmt, [])
    Exqlite.Sqlite3.release(conn, stmt)
    {:ok, rows}
  end

  defp fetch_all(conn, stmt, acc) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_all(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
      {:error, reason} -> raise "SQLite error: #{reason}"
    end
  end
end
