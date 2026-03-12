#!/usr/bin/env elixir
# Benchmark: query_aggregate_multi fast path vs scan path
#
# Tests that exact label queries use O(1) ETS lookup instead of
# scanning all series for that metric.
#
# Usage:
#   mix run bench/query_path_bench.exs [--series 100000]

opts =
  System.argv()
  |> OptionParser.parse!(strict: [series: :integer])
  |> elem(0)

series_count = Keyword.get(opts, :series, 100_000)
metrics_count = 20

IO.puts("=== Query Path Benchmark ===")
IO.puts("Series: #{series_count} (#{metrics_count} metrics, #{div(series_count, metrics_count)} series/metric)\n")

data_dir = Path.join(System.tmp_dir!(), "tm_qbench_#{System.unique_integer([:positive])}")
File.mkdir_p!(data_dir)

{:ok, _pid} =
  TimelessMetrics.Supervisor.start_link(
    name: :bench_store,
    data_dir: data_dir,
    self_monitor: false,
    scraping: false,
    defer_compression: true
  )

manager = :bench_store_actor_manager

# Create all series
series =
  for i <- 1..series_count do
    metric = "bench.metric_#{rem(i, metrics_count)}"
    labels = %{"host" => "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}", "env" => "prod"}
    {metric, labels}
  end

IO.puts("Creating #{series_count} series...")

{create_us, _} =
  :timer.tc(fn ->
    Enum.each(series, fn {metric, labels} ->
      TimelessMetrics.Actor.SeriesManager.get_or_start(manager, metric, labels)
    end)
  end)

IO.puts("Created in #{Float.round(create_us / 1_000_000, 2)}s\n")

# Write a few points per series so queries return data
IO.puts("Writing 5 points per series...")
ts = System.os_time(:second)

Enum.each(series, fn {metric, labels} ->
  pid = TimelessMetrics.Actor.Engine.resolve_series(:bench_store, metric, labels)
  for i <- 1..5, do: send(pid, {:write, ts - 300 + i * 60, :rand.uniform() * 100.0})
end)

Process.sleep(1000)
IO.puts("Done.\n")

# Pick random series for queries
query_samples = Enum.take_random(series, min(1000, series_count))
now = System.os_time(:second)
query_opts = [from: now - 3600, to: now, bucket: {60, :seconds}, aggregate: :avg]

# --- Benchmark: query_aggregate_multi (PromQL path, now with fast path) ---
IO.puts("--- query_aggregate_multi (PromQL path) ---")

{multi_us, _} =
  :timer.tc(fn ->
    Enum.each(query_samples, fn {metric, labels} ->
      {:ok, _} = TimelessMetrics.Actor.Engine.query_aggregate_multi(:bench_store, metric, labels, query_opts)
    end)
  end)

multi_per = multi_us / length(query_samples)
IO.puts("Queries:    #{length(query_samples)}")
IO.puts("Total:      #{Float.round(multi_us / 1000, 1)} ms")
IO.puts("Per query:  #{Float.round(multi_per, 1)} us (#{Float.round(multi_per / 1000, 3)} ms)")

# --- Benchmark: query_aggregate (direct single-series path) ---
IO.puts("\n--- query_aggregate (direct path) ---")

{direct_us, _} =
  :timer.tc(fn ->
    Enum.each(query_samples, fn {metric, labels} ->
      {:ok, _} = TimelessMetrics.Actor.Engine.query_aggregate(:bench_store, metric, labels, query_opts)
    end)
  end)

direct_per = direct_us / length(query_samples)
IO.puts("Queries:    #{length(query_samples)}")
IO.puts("Total:      #{Float.round(direct_us / 1000, 1)} ms")
IO.puts("Per query:  #{Float.round(direct_per, 1)} us (#{Float.round(direct_per / 1000, 3)} ms)")

# --- Benchmark: forced scan path (empty filter = wildcard) ---
# --- Benchmark: partial label match via label index (PromQL pattern: metric{host="X"}) ---
IO.puts("\n--- query_aggregate_multi PARTIAL label (host only, label index) ---")

{partial_us, _} =
  :timer.tc(fn ->
    Enum.each(query_samples, fn {metric, labels} ->
      host_only = Map.take(labels, ["host"])
      {:ok, _} = TimelessMetrics.Actor.Engine.query_aggregate_multi(:bench_store, metric, host_only, query_opts)
    end)
  end)

partial_per = partial_us / length(query_samples)
IO.puts("Queries:    #{length(query_samples)}")
IO.puts("Total:      #{Float.round(partial_us / 1000, 1)} ms")
IO.puts("Per query:  #{Float.round(partial_per, 1)} us (#{Float.round(partial_per / 1000, 3)} ms)")

IO.puts("\n--- query_aggregate_multi SCAN path (empty filter, all series for metric) ---")

scan_samples = Enum.take_random(query_samples, min(20, length(query_samples)))

{scan_us, _} =
  :timer.tc(fn ->
    Enum.each(scan_samples, fn {metric, _labels} ->
      {:ok, _} = TimelessMetrics.Actor.Engine.query_aggregate_multi(:bench_store, metric, %{}, query_opts)
    end)
  end)

scan_per = scan_us / length(scan_samples)
IO.puts("Queries:    #{length(scan_samples)} (scanning #{div(series_count, metrics_count)} series each)")
IO.puts("Total:      #{Float.round(scan_us / 1000, 1)} ms")
IO.puts("Per query:  #{Float.round(scan_per, 1)} us (#{Float.round(scan_per / 1000, 3)} ms)")

IO.puts("\n--- Summary ---")
IO.puts("Full labels (ETS key):   #{Float.round(multi_per, 1)} us")
IO.puts("Partial label (index):   #{Float.round(partial_per, 1)} us")
IO.puts("Direct (single):         #{Float.round(direct_per, 1)} us")
IO.puts("Scan (all series):       #{Float.round(scan_per, 1)} us")
IO.puts("Label index speedup:     #{Float.round(scan_per / partial_per, 1)}x vs scan")

File.rm_rf!(data_dir)
System.halt(0)
