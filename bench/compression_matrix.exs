# Compression Level × Block Size Matrix Benchmark
#
# Phase 1: Vary compression level (fix segment_duration at 300s for ~10K pts/series)
# Phase 2: Vary segment_duration to control effective block size (fix level at Phase 1 winner)
#
# Run: mix run bench/compression_matrix.exs

Application.ensure_all_started(:inets)

defmodule CompressionBench do
  @devices 1_000
  @metrics 20
  @series @devices * @metrics
  @batch 300
  @seed 2_000_000
  @measure_seconds 30
  @writers System.schedulers_online()
  @query_workers 2

  def run_test(level, segment_duration, label) do
    data_dir = "/tmp/timeless_comp_bench_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    # Start store with test params
    store = :"bench_#{System.unique_integer([:positive])}"
    {:ok, sup} = TimelessMetrics.Supervisor.start_link(
      name: store,
      data_dir: data_dir,
      compression_level: level,
      segment_duration: segment_duration,
      pending_flush_interval: :timer.seconds(5),
      flush_interval: :timer.seconds(2),
      flush_threshold: 5_000,
      self_monitor: false,
      scraping: false
    )

    :persistent_term.put({TimelessMetrics.HTTP, :config}, {store, nil})
    port = 19_000 + :rand.uniform(999)
    {:ok, rocket} = Rocket.start_link(port: port, handler: TimelessMetrics.HTTP, max_body: 10 * 1024 * 1024)
    Process.sleep(500)

    # Build prometheus text bodies
    series_lines = for d <- 1..@devices, m <- 0..(@metrics - 1) do
      "bench_m#{m}{h=\"d#{String.pad_leading(Integer.to_string(d), 5, "0")}\",e=\"p\"}"
    end

    # Seed phase
    seed_start = System.monotonic_time(:millisecond)
    seed_count = seed_points(port, series_lines, @seed)
    seed_ms = System.monotonic_time(:millisecond) - seed_start
    seed_rate = seed_count / (seed_ms / 1000)

    # Flush to force compression
    TimelessMetrics.flush(store)
    Process.sleep(2000)

    # Measure phase — concurrent writes + queries
    {write_pts, write_ms, query_count, query_total_us} = measure(port, series_lines, store, @measure_seconds)
    write_rate = write_pts / (write_ms / 1000)
    query_rate = query_count / (@measure_seconds)
    avg_query_us = if query_count > 0, do: div(query_total_us, query_count), else: 0

    # Flush and capture storage
    TimelessMetrics.flush(store)
    Process.sleep(3000)

    health = TimelessMetrics.info(store)
    total_points = health.total_points
    storage_bytes = dir_size(data_dir)
    bytes_per_point = if total_points > 0, do: Float.round(storage_bytes / total_points, 2), else: 0.0

    # Print result line
    IO.puts(
      "#{String.pad_trailing(label, 20)} | " <>
      "seed: #{fmt(seed_rate)}/s | " <>
      "write: #{fmt(write_rate)}/s | " <>
      "query: #{fmt(query_rate * 1.0)}/s @ #{div(avg_query_us, 1000)}ms | " <>
      "storage: #{fmt_bytes(storage_bytes)} / #{fmt(total_points * 1.0)} pts = #{bytes_per_point} B/pt"
    )

    # Cleanup — stop Rocket first so no new requests arrive
    try do GenServer.stop(rocket, :normal, 5000) catch _, _ -> :ok end
    Process.sleep(500)
    Supervisor.stop(sup, :normal, 10_000)
    Process.sleep(500)
    File.rm_rf!(data_dir)

    %{
      label: label,
      level: level,
      segment_duration: segment_duration,
      seed_rate: seed_rate,
      write_rate: write_rate,
      query_rate: query_rate,
      avg_query_ms: avg_query_us / 1000,
      bytes_per_point: bytes_per_point,
      total_points: total_points,
      storage_bytes: storage_bytes
    }
  end

  defp seed_points(port, series_lines, target) do
    url = ~c"http://127.0.0.1:#{port}/api/v1/import/prometheus"
    count = :atomics.new(1, [])
    stop = :atomics.new(1, [])

    tasks = for _w <- 1..@writers do
      Task.async(fn ->
        seed_loop(url, series_lines, count, stop, target)
      end)
    end

    # Poll until target
    poll_seed(count, stop, target)
    Enum.each(tasks, &Task.await(&1, 60_000))
    :atomics.get(count, 1)
  end

  defp seed_loop(url, series_lines, count, stop, _target) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      body = build_body(series_lines)
      case :httpc.request(:post, {url, [], ~c"text/plain", body}, [{:timeout, 10_000}], []) do
        {:ok, {{_, 204, _}, _, _}} ->
          :atomics.add(count, 1, @batch)
        _ -> :ok
      end
      seed_loop(url, series_lines, count, stop, _target)
    end
  end

  defp poll_seed(count, stop, target) do
    Process.sleep(500)
    if :atomics.get(count, 1) >= target do
      :atomics.put(stop, 1, 1)
    else
      poll_seed(count, stop, target)
    end
  end

  defp measure(port, series_lines, store, seconds) do
    url = ~c"http://127.0.0.1:#{port}/api/v1/import/prometheus"
    write_count = :atomics.new(1, [])
    query_count = :atomics.new(1, [])
    query_us = :atomics.new(1, [])
    stop = :atomics.new(1, [])

    start_ms = System.monotonic_time(:millisecond)

    # Writers
    write_tasks = for _w <- 1..@writers do
      Task.async(fn ->
        write_loop(url, series_lines, write_count, stop)
      end)
    end

    # Query workers
    query_tasks = for _q <- 1..@query_workers do
      Task.async(fn ->
        query_loop(port, store, query_count, query_us, stop)
      end)
    end

    Process.sleep(seconds * 1000)
    :atomics.put(stop, 1, 1)

    Enum.each(write_tasks ++ query_tasks, &Task.await(&1, 30_000))
    elapsed_ms = System.monotonic_time(:millisecond) - start_ms

    {:atomics.get(write_count, 1), elapsed_ms, :atomics.get(query_count, 1), :atomics.get(query_us, 1)}
  end

  defp write_loop(url, series_lines, count, stop) do
    if :atomics.get(stop, 1) == 1, do: :ok, else: (
      body = build_body(series_lines)
      case :httpc.request(:post, {url, [], ~c"text/plain", body}, [{:timeout, 10_000}], []) do
        {:ok, {{_, 204, _}, _, _}} -> :atomics.add(count, 1, @batch)
        _ -> :ok
      end
      write_loop(url, series_lines, count, stop)
    )
  end

  defp query_loop(port, _store, count, us_total, stop) do
    if :atomics.get(stop, 1) == 1, do: :ok, else: (
      device = :rand.uniform(@devices)
      metric = :rand.uniform(@metrics) - 1
      now = System.os_time(:second)
      query = URI.encode("bench_m#{metric}{h=\"d#{String.pad_leading(Integer.to_string(device), 5, "0")}\"}")
      url = ~c"http://127.0.0.1:#{port}/api/v1/query_range?query=#{query}&start=#{now - 3600}&end=#{now}&step=60"

      {elapsed, _} = :timer.tc(fn ->
        :httpc.request(:get, {url, []}, [{:timeout, 5_000}], [])
      end)

      :atomics.add(count, 1, 1)
      :atomics.add(us_total, 1, elapsed)
      query_loop(port, _store, count, us_total, stop)
    )
  end

  defp build_body(series_lines) do
    ts_ms = Integer.to_string(System.os_time(:millisecond))
    lines = Enum.take_random(series_lines, @batch)
    body = Enum.map_join(lines, "\n", fn line ->
      "#{line} #{Float.to_string(:rand.uniform() * 100)} #{ts_ms}"
    end)
    String.to_charlist(body)
  end

  defp dir_size(path) do
    case File.ls(path) do
      {:ok, entries} ->
        Enum.reduce(entries, 0, fn entry, acc ->
          full = Path.join(path, entry)
          case File.stat(full) do
            {:ok, %{type: :regular, size: s}} -> acc + s
            {:ok, %{type: :directory}} -> acc + dir_size(full)
            _ -> acc
          end
        end)
      _ -> 0
    end
  end

  defp fmt(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 2)}M"
  defp fmt(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt(n), do: "#{Float.round(n, 1)}"

  defp fmt_bytes(n) when n >= 1_073_741_824, do: "#{Float.round(n / 1_073_741_824, 2)}GB"
  defp fmt_bytes(n) when n >= 1_048_576, do: "#{Float.round(n / 1_048_576, 1)}MB"
  defp fmt_bytes(n) when n >= 1024, do: "#{Float.round(n / 1024, 1)}KB"
  defp fmt_bytes(n), do: "#{n}B"
end

IO.puts("=== Compression Level × Block Size Matrix ===")
IO.puts("20K series, 300 pts/batch, #{System.schedulers_online()} writers\n")

IO.puts("--- Phase 1: Compression Level (segment_duration=300s) ---")
IO.puts(String.duplicate("-", 120))

phase1_results =
  for level <- [1, 2, 5, 9, 15] do
    CompressionBench.run_test(level, 300, "level=#{level} seg=300s")
  end

IO.puts("")

# Pick winner: best write_rate with < 2x bytes/point vs best compression
best_compression = Enum.min_by(phase1_results, & &1.bytes_per_point)
best_throughput = Enum.max_by(phase1_results, & &1.write_rate)

IO.puts("Best throughput:   #{best_throughput.label} (#{Float.round(best_throughput.write_rate / 1000, 1)}K/s)")
IO.puts("Best compression:  #{best_compression.label} (#{best_compression.bytes_per_point} B/pt)")

# Use level 2 for phase 2 (fast path)
winner_level = 2

IO.puts("\n--- Phase 2: Segment Duration with level=#{winner_level} ---")
IO.puts(String.duplicate("-", 120))

for seg <- [60, 300, 900, 3600, 14400] do
  CompressionBench.run_test(winner_level, seg, "level=#{winner_level} seg=#{seg}s")
end

IO.puts("\nDone.")
