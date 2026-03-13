#!/usr/bin/env elixir
# Benchmark: concurrent writes + queries WITHOUT HTTP
#
# Isolates whether the write degradation under query load is caused by
# the HTTP/PromQL layer or by internal engine contention.
#
# Usage:
#   mix run bench/write_query_contention.exs [--series 100000] [--writers 32] [--query-workers 8] [--warmup 30] [--duration 60]

opts =
  System.argv()
  |> OptionParser.parse!(strict: [
    series: :integer,
    writers: :integer,
    query_workers: :integer,
    warmup: :integer,
    duration: :integer,
    batch: :integer
  ])
  |> elem(0)

series_count = Keyword.get(opts, :series, 100_000)
writer_count = Keyword.get(opts, :writers, 32)
query_worker_count = Keyword.get(opts, :query_workers, 8)
warmup_s = Keyword.get(opts, :warmup, 30)
duration_s = Keyword.get(opts, :duration, 60)
batch_size = Keyword.get(opts, :batch, 50)
metrics_count = 20

IO.puts("=== Write + Query Contention Benchmark (no HTTP) ===")
IO.puts("Series:       #{series_count}")
IO.puts("Writers:      #{writer_count}")
IO.puts("Query wkrs:   #{query_worker_count}")
IO.puts("Batch:        #{batch_size}")
IO.puts("Warmup:       #{warmup_s}s")
IO.puts("Duration:     #{duration_s}s\n")

data_dir = Path.join(System.tmp_dir!(), "tm_contention_#{System.unique_integer([:positive])}")
File.mkdir_p!(data_dir)

{:ok, _pid} =
  TimelessMetrics.Supervisor.start_link(
    name: :bench_store,
    data_dir: data_dir,
    self_monitor: false,
    scraping: false
  )

manager = :bench_store_actor_manager

# Pre-create all series and resolve PIDs
IO.puts("Creating #{series_count} series...")

{create_us, pids_tuple} =
  :timer.tc(fn ->
    pids =
      for i <- 1..series_count do
        metric = "bench.metric_#{rem(i, metrics_count)}"
        labels = %{"host" => "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}", "env" => "prod"}
        {_id, pid} = TimelessMetrics.Actor.SeriesManager.get_or_start(manager, metric, labels)
        {pid, metric, labels}
      end

    :erlang.list_to_tuple(pids)
  end)

total = tuple_size(pids_tuple)
IO.puts("Created #{total} series in #{Float.round(create_us / 1_000_000, 2)}s\n")

# Counters: 1=write_pts, 2=query_count, 3=query_latency_us
counters = :atomics.new(3, [])

# Writer: sends directly to resolved PIDs (no HTTP, no ETS lookup)
writer_fn = fn stop_flag, offset ->
  Stream.iterate(offset, &(&1 + batch_size))
  |> Enum.reduce_while(:ok, fn pos, _acc ->
    if :atomics.get(stop_flag, 1) == 1 do
      {:halt, :ok}
    else
      ts = System.os_time(:second)

      for i <- pos..(pos + batch_size - 1) do
        {pid, _metric, _labels} = elem(pids_tuple, rem(i, total))
        send(pid, {:write, ts, :rand.uniform() * 100.0})
      end

      :atomics.add(counters, 1, batch_size)
      {:cont, :ok}
    end
  end)
end

# Query: calls engine directly (no HTTP, no PromQL parsing)
query_fn = fn stop_flag ->
  now = System.os_time(:second)
  query_opts = [from: now - 3600, to: now + 3600, bucket: {60, :seconds}, aggregate: :avg]

  Stream.repeatedly(fn -> :rand.uniform(total) - 1 end)
  |> Enum.reduce_while(:ok, fn idx, _acc ->
    if :atomics.get(stop_flag, 1) == 1 do
      {:halt, :ok}
    else
      {_pid, metric, labels} = elem(pids_tuple, idx)

      {us, _} =
        :timer.tc(fn ->
          TimelessMetrics.Actor.Engine.query_aggregate_multi(:bench_store, metric, labels, query_opts)
        end)

      :atomics.add(counters, 2, 1)
      :atomics.add(counters, 3, us)
      {:cont, :ok}
    end
  end)
end

# Reporter
reporter_fn = fn stop_flag, interval_ms ->
  Stream.iterate({0, 0, 0, 0}, fn {_pw, _pq, _pl, elapsed} ->
    Process.sleep(interval_ms)
    new_elapsed = elapsed + div(interval_ms, 1000)

    w = :atomics.get(counters, 1)
    q = :atomics.get(counters, 2)
    ql = :atomics.get(counters, 3)

    d_w = w - _pw
    d_q = q - _pq
    d_ql = ql - _pl

    interval_s = interval_ms / 1000
    wps = d_w / interval_s
    qps = d_q / interval_s
    avg_q_ms = if d_q > 0, do: d_ql / d_q / 1000, else: 0.0

    w_str = cond do
      wps >= 1_000_000 -> "#{Float.round(wps / 1_000_000, 2)}M"
      wps >= 1_000 -> "#{Float.round(wps / 1_000, 1)}K"
      true -> "#{Float.round(wps, 0)}"
    end

    IO.puts("[#{new_elapsed}s] W: #{w_str} pts/s  Q: #{Float.round(qps, 1)}/s (avg #{Float.round(avg_q_ms, 2)}ms)")

    {w, q, ql, new_elapsed}
  end)
  |> Enum.reduce_while(:ok, fn _, _ ->
    if :atomics.get(stop_flag, 1) == 1, do: {:halt, :ok}, else: {:cont, :ok}
  end)
end

# === Warmup: writes only ===
IO.puts("--- Warmup (#{warmup_s}s, writes only) ---")
warmup_stop = :atomics.new(1, [])

warmup_tasks =
  for w <- 1..writer_count do
    offset = div(total * (w - 1), writer_count)
    Task.async(fn -> writer_fn.(warmup_stop, offset) end)
  end

warmup_reporter = Task.async(fn -> reporter_fn.(warmup_stop, 5_000) end)

Process.sleep(warmup_s * 1000)
:atomics.put(warmup_stop, 1, 1)
Enum.each(warmup_tasks, &Task.await(&1, 30_000))
Task.await(warmup_reporter, 5_000)

warmup_pts = :atomics.get(counters, 1)
IO.puts("\nWarmup: #{warmup_pts} points written")

# Reset
for i <- 1..3, do: :atomics.put(counters, i, 0)

# === Measurement: writes + queries ===
IO.puts("\n--- Measurement (#{duration_s}s, writes + queries) ---")
stop = :atomics.new(1, [])

write_tasks =
  for w <- 1..writer_count do
    offset = div(total * (w - 1), writer_count)
    Task.async(fn -> writer_fn.(stop, offset) end)
  end

query_tasks =
  for _q <- 1..query_worker_count do
    Task.async(fn -> query_fn.(stop) end)
  end

report_task = Task.async(fn -> reporter_fn.(stop, 5_000) end)

Process.sleep(duration_s * 1000)
:atomics.put(stop, 1, 1)
Enum.each(write_tasks ++ query_tasks, &Task.await(&1, 60_000))
Task.await(report_task, 5_000)

# Results
total_pts = :atomics.get(counters, 1)
total_q = :atomics.get(counters, 2)
total_ql = :atomics.get(counters, 3)
avg_q_ms = if total_q > 0, do: total_ql / total_q / 1000, else: 0.0

IO.puts("\n=== Results ===")
IO.puts("Write points:  #{total_pts}")
IO.puts("Write rate:    #{Float.round(total_pts / duration_s / 1000, 1)}K pts/s")
IO.puts("Queries:       #{total_q}")
IO.puts("Query rate:    #{Float.round(total_q / duration_s, 1)} q/s")
IO.puts("Avg query lat: #{Float.round(avg_q_ms, 2)}ms")

# Fast path vs slow path stats
stats = TimelessMetrics.Stats.snapshot(:bench_store)
fast = stats.query_fast_path
slow = stats.query_slow_path
total_lookups = fast + slow
hit_rate = if total_lookups > 0, do: Float.round(fast / total_lookups * 100, 1), else: 0.0
IO.puts("\n=== Read Path ===")
IO.puts("Fast path (ETS):   #{fast}")
IO.puts("Slow path (GenServer): #{slow}")
IO.puts("Fast path hit rate: #{hit_rate}%")

File.rm_rf!(data_dir)
System.halt(0)
