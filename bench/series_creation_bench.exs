#!/usr/bin/env elixir
# Benchmark: series creation → then sustained write/query load
#
# Phase 1: Create all series with zero writes (isolate creation cost)
# Phase 2: Sustained write load with concurrent queries
#
# Usage:
#   mix run bench/series_creation_bench.exs [--series 10000] [--write-seconds 30] [--writers 4] [--query-workers 4]

defmodule BenchHelpers do
  def write_loop(resolved, size, write_counter, stop_flag, pos) do
    if :atomics.get(stop_flag, 1) == 1 do
      :ok
    else
      idx = rem(pos, size)
      {pid, _m, _l} = :array.get(idx, resolved)
      ts = System.os_time(:second)

      for _ <- 1..10 do
        send(pid, {:write, ts, :rand.uniform() * 100})
      end

      :atomics.add(write_counter, 1, 10)
      write_loop(resolved, size, write_counter, stop_flag, pos + 1)
    end
  end

  def query_loop(series_list, query_counter, query_error_counter, query_latency_sum, stop_flag) do
    if :atomics.get(stop_flag, 1) == 1 do
      :ok
    else
      # Realistic query: single series, like a user graphing one device's metric
      {metric, labels} = Enum.random(series_list)
      now = System.os_time(:second)

      {us, result} =
        :timer.tc(fn ->
          try do
            TimelessMetrics.Actor.Engine.query_aggregate(
              :bench_store,
              metric,
              labels,
              from: now - 3600,
              to: now,
              bucket: {60, :seconds},
              aggregate: :avg
            )
          catch
            :exit, _ -> :error
          end
        end)

      case result do
        {:ok, _} ->
          :atomics.add(query_counter, 1, 1)
          :atomics.add(query_latency_sum, 1, us)

        :error ->
          :atomics.add(query_error_counter, 1, 1)
      end

      query_loop(series_list, query_counter, query_error_counter, query_latency_sum, stop_flag)
    end
  end
end

opts =
  System.argv()
  |> OptionParser.parse!(
    strict: [series: :integer, write_seconds: :integer, writers: :integer, query_workers: :integer]
  )
  |> elem(0)

series_count = Keyword.get(opts, :series, 10_000)
write_seconds = Keyword.get(opts, :write_seconds, 30)
writers = Keyword.get(opts, :writers, 4)
query_workers = Keyword.get(opts, :query_workers, 4)

IO.puts("=== Series Creation + Load Benchmark ===")
IO.puts(
  "Series: #{series_count}, Write phase: #{write_seconds}s, Writers: #{writers}, Query workers: #{query_workers}\n"
)

# Start a TimelessMetrics instance with a temp data dir
data_dir = Path.join(System.tmp_dir!(), "tm_bench_#{System.unique_integer([:positive])}")
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

# Pre-generate all series keys
series =
  for i <- 1..series_count do
    metric = "bench.metric_#{rem(i, 20)}"

    labels = %{
      "host" => "device_#{String.pad_leading(Integer.to_string(i), 6, "0")}",
      "env" => "prod"
    }

    {metric, labels}
  end

# ==============================
# Phase 1: Pure creation (zero writes)
# ==============================
IO.puts("--- Phase 1: Series Creation (zero writes) ---")

{create_us, _} =
  :timer.tc(fn ->
    Enum.each(series, fn {metric, labels} ->
      TimelessMetrics.Actor.SeriesManager.get_or_start(manager, metric, labels)
    end)
  end)

create_s = create_us / 1_000_000
create_rate = series_count / create_s
IO.puts("Series created:  #{series_count}")
IO.puts("Time:            #{Float.round(create_s, 2)}s")
IO.puts("Rate:            #{Float.round(create_rate, 0)} series/sec")
IO.puts("Per series:      #{Float.round(create_us / series_count, 1)} us")

process_count = length(Process.list())
mem_mb = :erlang.memory(:total) / (1024 * 1024)
IO.puts("Processes:       #{process_count}")
IO.puts("BEAM memory:     #{Float.round(mem_mb, 1)} MB")

# ==============================
# Phase 2: Sustained write + query load
# ==============================
IO.puts("\n--- Phase 2: Write + Query Load (#{write_seconds}s) ---")

# Resolve all PIDs upfront
resolved =
  Enum.map(series, fn {metric, labels} ->
    pid = TimelessMetrics.Actor.Engine.resolve_series(:bench_store, metric, labels)
    {pid, metric, labels}
  end)

resolved_array = :array.from_list(resolved)
array_size = :array.size(resolved_array)

stop_flag = :atomics.new(1, [])
write_counter = :atomics.new(1, [])
query_counter = :atomics.new(1, [])
query_error_counter = :atomics.new(1, [])
query_latency_sum = :atomics.new(1, [])

# Writer tasks
writer_tasks =
  for w <- 1..writers do
    # Stagger start positions so writers don't all hit the same series
    offset = div(array_size * (w - 1), writers)

    Task.async(fn ->
      BenchHelpers.write_loop(resolved_array, array_size, write_counter, stop_flag, offset)
    end)
  end

# Query tasks
query_tasks =
  for _q <- 1..query_workers do
    Task.async(fn ->
      BenchHelpers.query_loop(
        series,
        query_counter,
        query_error_counter,
        query_latency_sum,
        stop_flag
      )
    end)
  end

# Let it run
Process.sleep(write_seconds * 1000)
:atomics.put(stop_flag, 1, 1)

# Wait for all tasks
Enum.each(writer_tasks ++ query_tasks, &Task.await(&1, 30_000))

total_writes = :atomics.get(write_counter, 1)
total_queries = :atomics.get(query_counter, 1)
total_query_errors = :atomics.get(query_error_counter, 1)
total_query_latency_us = :atomics.get(query_latency_sum, 1)

write_rate = total_writes / write_seconds
query_rate = total_queries / write_seconds

avg_query_us =
  if total_queries > 0, do: total_query_latency_us / total_queries, else: 0

IO.puts("Write points:    #{total_writes}")
IO.puts("Write rate:      #{Float.round(write_rate, 0)} pts/sec")
IO.puts("Queries:         #{total_queries}")
IO.puts("Query rate:      #{Float.round(query_rate, 1)} queries/sec")
IO.puts("Query errors:    #{total_query_errors}")
IO.puts("Avg query lat:   #{Float.round(avg_query_us / 1000, 2)} ms")

mem_mb2 = :erlang.memory(:total) / (1024 * 1024)
IO.puts("BEAM memory:     #{Float.round(mem_mb2, 1)} MB (was #{Float.round(mem_mb, 1)} MB)")

# Cleanup
IO.puts("\nDone.")
System.halt(0)
