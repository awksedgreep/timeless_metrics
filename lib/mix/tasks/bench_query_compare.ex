defmodule Mix.Tasks.BenchQueryCompare do
  @shortdoc "Compare query latency: compressed chunks vs old row-per-bucket"
  @moduledoc """
  Creates both old-style uncompressed tier tables and new compressed chunk tables
  side by side with identical data, then benchmarks query latency.

  ## Usage

      mix bench_query_compare                     # default: 1K series, 30 days
      mix bench_query_compare --series 10000      # 10K series
      mix bench_query_compare --days 90           # 90 days

  ## What it measures

  - Single-series aggregate query latency at 1h, 24h, 7d, 30d windows
  - Multi-series query latency (10 series)
  - Both direct SELECT (old) and decompress-and-filter (new)
  """

  use Mix.Task

  @warmup_rounds 3
  @bench_rounds 10

  def run(args) do
    Mix.Task.run("app.start")

    {series_count, days} = parse_args(args)

    IO.puts(bar())
    IO.puts("  Query Latency: Compressed vs Uncompressed")
    IO.puts(bar())
    IO.puts("  Series:  #{fmt(series_count)}")
    IO.puts("  Days:    #{days}")
    IO.puts("  Rounds:  #{@bench_rounds} (#{@warmup_rounds} warmup)")
    IO.puts(bar())
    IO.puts("")

    data_dir = "/tmp/timeless_qbench_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)
    db_path = Path.join(data_dir, "bench.db")

    {:ok, conn} = Exqlite.Sqlite3.open(db_path)
    configure(conn)

    # Create both table styles
    create_old_table(conn)
    create_new_table(conn)

    # Generate and insert data
    IO.puts("Populating #{fmt(series_count)} series x #{days} days...")
    {pop_us, bucket_count} = :timer.tc(fn -> populate(conn, series_count, days) end)
    IO.puts("  #{fmt(bucket_count)} hourly buckets per series")
    IO.puts("  #{fmt(bucket_count * series_count)} total buckets")
    IO.puts("  Populated in #{fmt_ms(pop_us)}")
    IO.puts("")

    # Measure storage
    old_bytes = table_bytes(conn, "tier_old")
    new_bytes = table_bytes(conn, "tier_new")
    IO.puts("Storage:")
    IO.puts("  Old (rows):       #{fmt_bytes(old_bytes)}")
    IO.puts("  New (chunks):     #{fmt_bytes(new_bytes)}")
    IO.puts("  Ratio:            #{Float.round(old_bytes / max(new_bytes, 1), 1)}x smaller")
    IO.puts("")

    # Open read-only connections for benchmarking
    {:ok, reader} = Exqlite.Sqlite3.open(db_path, mode: :readonly)
    configure(reader)

    now = System.os_time(:second)
    series_id = 0

    # Single-series benchmarks at various windows
    IO.puts("== Single-Series Query ==")

    IO.puts(
      String.pad_trailing("  Window", 12) <>
        String.pad_trailing("Old (rows)", 16) <>
        String.pad_trailing("New (chunks)", 16) <>
        "Diff"
    )

    IO.puts("  " <> String.duplicate("-", 54))

    for {label, hours} <- [{"1h", 1}, {"6h", 6}, {"24h", 24}, {"7d", 168}, {"30d", 720}] do
      from = now - hours * 3600
      to = now

      if hours * 3600 <= days * 86_400 do
        old_us = bench_old_query(reader, series_id, from, to)
        new_us = bench_new_query(reader, series_id, from, to)
        diff = if old_us > 0, do: new_us / old_us, else: 0.0

        IO.puts(
          String.pad_trailing("  #{label}", 12) <>
            String.pad_trailing(fmt_ms(old_us), 16) <>
            String.pad_trailing(fmt_ms(new_us), 16) <>
            "#{Float.round(diff, 2)}x"
        )
      end
    end

    IO.puts("")

    # Multi-series benchmark (10 series, 24h window)
    IO.puts("== Multi-Series Query (10 series, 24h) ==")
    from = now - 86_400
    to = now

    old_multi =
      bench_fn(@bench_rounds, fn ->
        for sid <- 0..9, do: query_old(reader, sid, from, to)
      end)

    new_multi =
      bench_fn(@bench_rounds, fn ->
        for sid <- 0..9, do: query_new(reader, sid, from, to)
      end)

    diff = if old_multi > 0, do: new_multi / old_multi, else: 0.0
    IO.puts("  Old (rows):    #{fmt_ms(old_multi)}")
    IO.puts("  New (chunks):  #{fmt_ms(new_multi)}")
    IO.puts("  Ratio:         #{Float.round(diff, 2)}x")
    IO.puts("")

    # Full table scan comparison (all series, 24h — simulates dashboard)
    IO.puts("== Full Scan: All #{fmt(series_count)} series, 24h ==")

    old_scan =
      bench_fn(max(div(@bench_rounds, 2), 3), fn ->
        for sid <- 0..(series_count - 1), do: query_old(reader, sid, from, to)
      end)

    new_scan =
      bench_fn(max(div(@bench_rounds, 2), 3), fn ->
        for sid <- 0..(series_count - 1), do: query_new(reader, sid, from, to)
      end)

    diff = if old_scan > 0, do: new_scan / old_scan, else: 0.0
    IO.puts("  Old (rows):    #{fmt_ms(old_scan)}")
    IO.puts("  New (chunks):  #{fmt_ms(new_scan)}")
    IO.puts("  Ratio:         #{Float.round(diff, 2)}x")
    IO.puts("")

    IO.puts(bar())
    IO.puts("  < 1.0x = new is faster, > 1.0x = old is faster")
    IO.puts(bar())
    IO.puts("")
    IO.puts("  Cleanup: rm -rf #{data_dir}")

    Exqlite.Sqlite3.close(reader)
    Exqlite.Sqlite3.close(conn)
  end

  # --- Table creation ---

  defp create_old_table(conn) do
    exec(conn, """
    CREATE TABLE IF NOT EXISTS tier_old (
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
  end

  defp create_new_table(conn) do
    exec(conn, """
    CREATE TABLE IF NOT EXISTS tier_new (
      series_id    INTEGER NOT NULL,
      chunk_start  INTEGER NOT NULL,
      chunk_end    INTEGER NOT NULL,
      bucket_count INTEGER NOT NULL,
      data         BLOB NOT NULL,
      PRIMARY KEY (series_id, chunk_start)
    ) WITHOUT ROWID
    """)
  end

  # --- Data population ---

  defp populate(conn, series_count, days) do
    now = System.os_time(:second)
    start_ts = div(now - days * 86_400, 3600) * 3600
    bucket_count = days * 24
    chunk_seconds = 86_400

    exec(conn, "BEGIN IMMEDIATE")

    # Prepare old insert
    {:ok, old_stmt} =
      Exqlite.Sqlite3.prepare(
        conn,
        "INSERT OR REPLACE INTO tier_old (series_id, bucket, avg, min, max, count, sum, last) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
      )

    # Prepare new insert
    {:ok, new_stmt} =
      Exqlite.Sqlite3.prepare(
        conn,
        "INSERT OR REPLACE INTO tier_new (series_id, chunk_start, chunk_end, bucket_count, data) VALUES (?1, ?2, ?3, ?4, ?5)"
      )

    aggregates = [:avg, :min, :max, :count, :sum, :last]

    for sid <- 0..(series_count - 1) do
      # Generate hourly buckets
      buckets =
        for h <- 0..(bucket_count - 1) do
          ts = start_ts + h * 3600
          base = 50.0 + :rand.uniform() * 40

          %{
            bucket: ts,
            avg: base,
            min: base - 10.0 - :rand.uniform() * 10,
            max: base + 10.0 + :rand.uniform() * 10,
            count: 12,
            sum: base * 12,
            last: base + (:rand.uniform() - 0.5) * 5
          }
        end

      # Insert into old table (one row per bucket)
      Enum.each(buckets, fn b ->
        :ok =
          Exqlite.Sqlite3.bind(old_stmt, [
            sid,
            b.bucket,
            b.avg,
            b.min,
            b.max,
            b.count,
            b.sum,
            b.last
          ])

        :done = Exqlite.Sqlite3.step(conn, old_stmt)
        :ok = Exqlite.Sqlite3.reset(old_stmt)
      end)

      # Insert into new table (chunks)
      buckets
      |> Enum.group_by(fn b -> div(b.bucket, chunk_seconds) * chunk_seconds end)
      |> Enum.each(fn {cs, chunk_buckets} ->
        ce = cs + chunk_seconds
        blob = TimelessMetrics.TierChunk.encode(chunk_buckets, aggregates)
        bc = length(chunk_buckets)
        :ok = Exqlite.Sqlite3.bind(new_stmt, [sid, cs, ce, bc, blob])
        :done = Exqlite.Sqlite3.step(conn, new_stmt)
        :ok = Exqlite.Sqlite3.reset(new_stmt)
      end)

      if series_count >= 1000 and rem(sid + 1, 1000) == 0 do
        IO.puts("    #{fmt(sid + 1)}/#{fmt(series_count)} series")
      end
    end

    Exqlite.Sqlite3.release(conn, old_stmt)
    Exqlite.Sqlite3.release(conn, new_stmt)
    exec(conn, "COMMIT")

    bucket_count
  end

  # --- Query functions ---

  defp query_old(conn, series_id, from, to) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(
        conn,
        "SELECT bucket, avg, min, max, count, sum, last FROM tier_old WHERE series_id = ?1 AND bucket >= ?2 AND bucket < ?3 ORDER BY bucket"
      )

    :ok = Exqlite.Sqlite3.bind(stmt, [series_id, from, to])
    rows = fetch_all(conn, stmt, [])
    Exqlite.Sqlite3.release(conn, stmt)
    rows
  end

  defp query_new(conn, series_id, from, to) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(
        conn,
        "SELECT data FROM tier_new WHERE series_id = ?1 AND chunk_end > ?2 AND chunk_start < ?3 ORDER BY chunk_start"
      )

    :ok = Exqlite.Sqlite3.bind(stmt, [series_id, from, to])
    chunks = fetch_all(conn, stmt, [])
    Exqlite.Sqlite3.release(conn, stmt)

    # Decompress and filter
    Enum.flat_map(chunks, fn [blob] ->
      {_aggs, buckets} = TimelessMetrics.TierChunk.decode(blob)
      Enum.filter(buckets, fn b -> b.bucket >= from and b.bucket < to end)
    end)
  end

  # --- Benchmark helpers ---

  defp bench_old_query(conn, series_id, from, to) do
    # Warmup
    for _ <- 1..@warmup_rounds, do: query_old(conn, series_id, from, to)

    bench_fn(@bench_rounds, fn -> query_old(conn, series_id, from, to) end)
  end

  defp bench_new_query(conn, series_id, from, to) do
    for _ <- 1..@warmup_rounds, do: query_new(conn, series_id, from, to)

    bench_fn(@bench_rounds, fn -> query_new(conn, series_id, from, to) end)
  end

  defp bench_fn(rounds, fun) do
    times =
      for _ <- 1..rounds do
        {us, _} = :timer.tc(fun)
        us
      end

    # Median
    sorted = Enum.sort(times)
    Enum.at(sorted, div(length(sorted), 2))
  end

  # --- SQLite helpers ---

  defp configure(conn) do
    exec(conn, "PRAGMA page_size = 16384")
    exec(conn, "PRAGMA journal_mode = WAL")
    exec(conn, "PRAGMA synchronous = NORMAL")
    exec(conn, "PRAGMA cache_size = -64000")
    exec(conn, "PRAGMA mmap_size = 2147483648")
    exec(conn, "PRAGMA temp_store = MEMORY")
  end

  defp exec(conn, sql) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)
    # Step until done (some PRAGMAs return rows)
    drain(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)
  end

  defp drain(conn, stmt) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, _} -> drain(conn, stmt)
      :done -> :ok
    end
  end

  defp fetch_all(conn, stmt, acc) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_all(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end

  defp table_bytes(conn, "tier_old") do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT COUNT(*) FROM tier_old")
    {:row, [count]} = Exqlite.Sqlite3.step(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)
    count * 78
  end

  defp table_bytes(conn, "tier_new") do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(conn, "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM tier_new")

    {:row, [val]} = Exqlite.Sqlite3.step(conn, stmt)
    Exqlite.Sqlite3.release(conn, stmt)
    val || 0
  end

  # --- Arg parsing ---

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args, strict: [series: :integer, days: :integer])

    {Keyword.get(opts, :series, 1_000), Keyword.get(opts, :days, 30)}
  end

  # --- Formatters ---

  defp fmt(n) when is_float(n), do: fmt(trunc(n))
  defp fmt(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp fmt(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt(n), do: "#{n}"

  defp fmt_ms(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)}s"
  defp fmt_ms(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 1)}ms"
  defp fmt_ms(us), do: "#{us}us"

  defp fmt_bytes(b) when b >= 1_073_741_824, do: "#{Float.round(b / 1_073_741_824, 2)} GB"
  defp fmt_bytes(b) when b >= 1_048_576, do: "#{Float.round(b / 1_048_576, 1)} MB"
  defp fmt_bytes(b) when b >= 1_024, do: "#{Float.round(b / 1_024, 1)} KB"
  defp fmt_bytes(b), do: "#{b} B"

  defp bar, do: String.duplicate("=", 56)
end
