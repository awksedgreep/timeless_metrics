defmodule WriteBench do
  @moduledoc """
  Embedded write/query benchmark using the programmatic API.

  Tests both disk and memory-only modes with realistic DataGenerator data.
  Measures: write throughput (single + concurrent), flush + compression,
  query latency, and compression ratio.

  Usage:
    mix run bench/write_bench.exs              # disk mode
    mix run bench/write_bench.exs --memory     # memory-only mode
  """

  def run(args) do
    memory_only = "--memory" in args

    mode_label = if memory_only, do: "MEMORY-ONLY", else: "DISK"

    IO.puts("\n=============================================")
    IO.puts("  TimelessMetrics Embedded Benchmark (#{mode_label})")
    IO.puts("  Schedulers: #{System.schedulers_online()}")
    IO.puts("=============================================\n")

    for {label, series, pts_per_series} <- [
          {"Small (1K series × 100 pts)", 1_000, 100},
          {"Medium (10K series × 100 pts)", 10_000, 100},
          {"Large (10K series × 1K pts)", 10_000, 1_000}
        ] do
      run_scale(label, series, pts_per_series, memory_only)
    end

    IO.puts("--- Compression by Data Pattern ---\n")
    run_compression_analysis(memory_only)
  end

  defp run_scale(label, series_count, pts_per_series, memory_only) do
    total = series_count * pts_per_series
    data_dir = "/tmp/timeless_bench_#{System.os_time(:millisecond)}"

    store_opts =
      if memory_only do
        [name: :bench, mode: :memory, self_monitor: false]
      else
        [name: :bench, data_dir: data_dir, self_monitor: false]
      end

    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, store_opts}],
        strategy: :one_for_one
      )

    now = System.os_time(:second)
    metrics = TimelessMetrics.DataGenerator.metrics() |> Enum.take(20)

    IO.puts("--- #{label} = #{format_number(total)} points ---\n")

    # --- Single-threaded writes ---
    {single_us, _} =
      :timer.tc(fn ->
        for s <- 1..series_count do
          metric = Enum.at(metrics, rem(s, 20))
          labels = %{"host" => "device_#{s}"}

          for i <- 0..(pts_per_series - 1) do
            ts = now - pts_per_series + i
            val = TimelessMetrics.DataGenerator.value(metric, ts * 1000, s)
            TimelessMetrics.write(:bench, metric, labels, val, timestamp: ts)
          end
        end
      end)

    single_rate = trunc(total / (single_us / 1_000_000))
    IO.puts("  Write (single):     #{format_number(single_rate)}/sec  (#{div(single_us, 1000)}ms)")

    # --- Query before flush (from Buffer ETS) ---
    sample_metric = Enum.at(metrics, rem(1, 20))

    {q_ets_us, {:ok, ets_pts}} =
      :timer.tc(fn ->
        TimelessMetrics.query(:bench, sample_metric, %{"host" => "device_1"},
          from: now - pts_per_series - 1,
          to: now + 1
        )
      end)

    IO.puts("  Query (ETS hot):    #{length(ets_pts)} pts in #{div(q_ets_us, 1000)}ms")

    # --- Flush + compress ---
    {flush_us, _} = :timer.tc(fn -> TimelessMetrics.flush(:bench) end)
    IO.puts("  Flush + compress:   #{div(flush_us, 1000)}ms")

    # --- Query after flush (from compressed ETS cache) ---
    {q_cache_us, {:ok, cache_pts}} =
      :timer.tc(fn ->
        TimelessMetrics.query(:bench, sample_metric, %{"host" => "device_1"},
          from: now - pts_per_series - 1,
          to: now + 1
        )
      end)

    IO.puts("  Query (compressed): #{length(cache_pts)} pts in #{div(q_cache_us, 1000)}ms")

    # --- Storage stats ---
    info = TimelessMetrics.info(:bench)
    IO.puts("  Series:             #{info.series_count}")
    IO.puts("  Points ingested:    #{format_number(info.points_ingested)}")
    IO.puts("  Storage:            #{format_bytes(info.storage_bytes)}")
    IO.puts("  Bytes/point:        #{Float.round(info.bytes_per_point, 3)}")

    # --- Concurrent write throughput ---
    # Reset store
    Supervisor.stop(sup)
    cleanup_persistent_terms(:bench)
    if not memory_only, do: File.rm_rf!(data_dir)
    data_dir2 = "/tmp/timeless_bench_conc_#{System.os_time(:millisecond)}"

    store_opts2 =
      if memory_only do
        [name: :bench, mode: :memory, self_monitor: false]
      else
        [name: :bench, data_dir: data_dir2, self_monitor: false]
      end

    {:ok, sup2} =
      Supervisor.start_link(
        [{TimelessMetrics, store_opts2}],
        strategy: :one_for_one
      )

    writers = System.schedulers_online()
    chunk_size = div(series_count, writers)

    {conc_us, _} =
      :timer.tc(fn ->
        1..writers
        |> Enum.map(fn w ->
          Task.async(fn ->
            start_s = (w - 1) * chunk_size + 1
            end_s = min(w * chunk_size, series_count)

            for s <- start_s..end_s do
              metric = Enum.at(metrics, rem(s, 20))
              labels = %{"host" => "device_#{s}"}

              for i <- 0..(pts_per_series - 1) do
                ts = now - pts_per_series + i
                val = TimelessMetrics.DataGenerator.value(metric, ts * 1000, s)
                TimelessMetrics.write(:bench, metric, labels, val, timestamp: ts)
              end
            end
          end)
        end)
        |> Enum.each(&Task.await(&1, 60_000))
      end)

    conc_rate = trunc(total / (conc_us / 1_000_000))
    IO.puts("  Write (#{writers} workers): #{format_number(conc_rate)}/sec  (#{div(conc_us, 1000)}ms)")
    IO.puts("")

    Supervisor.stop(sup2)
    cleanup_persistent_terms(:bench)
    if not memory_only, do: File.rm_rf!(data_dir2)
  end

  defp run_compression_analysis(memory_only) do
    now = System.os_time(:second)
    n = 10_000
    metrics = TimelessMetrics.DataGenerator.metrics()

    # Use DataGenerator for realistic patterns
    patterns = [
      {"node_load1 (gauge)", fn i -> TimelessMetrics.DataGenerator.value("node_load1", (now + i) * 1000, 1) end},
      {"node_cpu_seconds (ctr)", fn i -> TimelessMetrics.DataGenerator.value("node_cpu_seconds_total", (now + i) * 1000, 1) end},
      {"node_mem_total (const)", fn i -> TimelessMetrics.DataGenerator.value("node_memory_MemTotal_bytes", (now + i) * 1000, 1) end},
      {"node_disk_read (ctr)", fn i -> TimelessMetrics.DataGenerator.value("node_disk_read_bytes_total", (now + i) * 1000, 1) end},
      {"node_entropy (gauge)", fn i -> TimelessMetrics.DataGenerator.value("node_entropy_avail_bits", (now + i) * 1000, 1) end},
      {"Random float", fn _i -> :rand.uniform() * 1000.0 end}
    ]

    IO.puts("  #{format_number(n)} points per pattern:\n")
    IO.puts("  ┌────────────────────────────┬────────────┬──────────────┐")
    IO.puts("  │ Pattern                    │ Compressed │ Bytes/point  │")
    IO.puts("  ├────────────────────────────┼────────────┼──────────────┤")

    Enum.each(patterns, fn {label, gen_fn} ->
      data_dir = "/tmp/timeless_cmp_#{System.os_time(:millisecond)}"

      store_opts =
        if memory_only do
          [name: :cmp, mode: :memory, buffer_shards: 1, self_monitor: false]
        else
          [name: :cmp, data_dir: data_dir, buffer_shards: 1, self_monitor: false]
        end

      {:ok, sup} =
        Supervisor.start_link(
          [{TimelessMetrics, store_opts}],
          strategy: :one_for_one
        )

      for i <- 0..(n - 1) do
        TimelessMetrics.write(:cmp, "bench", %{"t" => "1"}, gen_fn.(i), timestamp: now + i)
      end

      TimelessMetrics.flush(:cmp)

      info = TimelessMetrics.info(:cmp)
      bpp = if info.points_ingested > 0, do: Float.round(info.storage_bytes / info.points_ingested, 2), else: 0.0

      label_fmt = String.pad_trailing(label, 26)
      bytes_fmt = String.pad_leading(format_bytes(info.storage_bytes), 10)
      bpp_fmt = String.pad_leading("#{bpp}", 12)

      IO.puts("  │ #{label_fmt} │ #{bytes_fmt} │ #{bpp_fmt} │")

      Supervisor.stop(sup)
      cleanup_persistent_terms(:cmp)
      if not memory_only, do: File.rm_rf!(data_dir)
    end)

    IO.puts("  └────────────────────────────┴────────────┴──────────────┘")
    IO.puts("")
  end

  defp cleanup_persistent_terms(store) do
    for key <- [:schema, :shard_count, :data_dir, :raw_retention_seconds, :daily_retention_seconds, :ingest_queue] do
      try do
        :persistent_term.erase({TimelessMetrics, store, key})
      rescue
        _ -> :ok
      end
    end
  end

  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 2)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_number(n), do: "#{n}"

  defp format_bytes(b) when b >= 1_048_576, do: "#{Float.round(b / 1_048_576, 1)} MB"
  defp format_bytes(b) when b >= 1024, do: "#{Float.round(b / 1024, 1)} KB"
  defp format_bytes(b), do: "#{b} B"
end

WriteBench.run(System.argv())
