defmodule WriteBench do
  @moduledoc """
  Embedded write/query benchmark using the programmatic API.

  Splits cold population from steady-state ingest so series creation cost
  does not dominate the throughput numbers.

  Usage:
    mix run bench/write_bench.exs
    mix run bench/write_bench.exs --libsql
    mix run bench/write_bench.exs --memory
    mix run bench/write_bench.exs --libsql --scale large
  """

  @steady_state_seconds 10
  @batch_window 100

  def run(args) do
    memory_only = "--memory" in args
    engine = if "--libsql" in args, do: :libsql, else: :rust

    if memory_only and engine == :libsql do
      raise "--memory and --libsql cannot be combined; libSQL is a disk-backed preview"
    end

    mode_label = if memory_only, do: "MEMORY-ONLY", else: "DISK / #{engine}"

    IO.puts("\n=============================================")
    IO.puts("  TimelessMetrics Embedded Benchmark (#{mode_label})")
    IO.puts("  Schedulers: #{System.schedulers_online()}")
    IO.puts("  Steady-state window: #{@steady_state_seconds}s")
    IO.puts("=============================================\n")

    scales = [
      {"small", "Small (1K series x 100 pts)", 1_000, 100},
      {"medium", "Medium (10K series x 100 pts)", 10_000, 100},
      {"large", "Large (10K series x 1K pts)", 10_000, 1_000}
    ]

    selected_scale = option_value(args, "--scale")

    for {name, label, series_count, pts_per_series} <- scales,
        selected_scale in [nil, name] do
      run_scale(label, series_count, pts_per_series, memory_only, engine)
    end

    IO.puts("--- Compression by Data Pattern ---\n")
    run_compression_analysis(memory_only, engine)
  end

  defp run_scale(label, series_count, pts_per_series, memory_only, engine) do
    total = series_count * pts_per_series
    data_dir = "/tmp/timeless_bench_#{System.os_time(:millisecond)}"
    store = :"bench_#{System.unique_integer([:positive])}"

    store_opts =
      if memory_only do
        [name: store, mode: :memory, self_monitor: false]
      else
        [name: store, data_dir: data_dir, engine: engine, self_monitor: false, scraping: false]
      end

    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, store_opts}],
        strategy: :one_for_one
      )

    try do
      now = System.os_time(:second)
      metrics = TimelessMetrics.DataGenerator.metrics() |> Enum.take(20)
      series = build_series(series_count, metrics)
      population_start = now - pts_per_series

      IO.puts("--- #{label} = #{format_number(total)} points ---\n")

      {populate_us, _} =
        :timer.tc(fn ->
          populate_store(store, series, pts_per_series, population_start)
        end)

      populate_rate = trunc(total / (populate_us / 1_000_000))

      IO.puts(
        "  Populate (cold):    #{format_number(populate_rate)}/sec  (#{div(populate_us, 1000)}ms)"
      )

      {sample_metric, sample_labels, _sample_idx} = hd(series)

      {q_hot_us, {:ok, hot_points}} =
        :timer.tc(fn ->
          TimelessMetrics.query(store, sample_metric, sample_labels,
            from: population_start - 1,
            to: now + 1
          )
        end)

      IO.puts("  Query (hot):        #{length(hot_points)} pts in #{div(q_hot_us, 1000)}ms")

      steady_start = now + 1

      {single_count, single_us} = run_single_phase(store, series, steady_start)

      IO.puts(
        "  Write (single):     #{format_number(rate(single_count, single_us))}/sec  (#{@steady_state_seconds}s steady-state)"
      )

      {conc_count, conc_us} = run_concurrent_phase(store, series, steady_start)

      IO.puts(
        "  Write (#{System.schedulers_online()} workers): #{format_number(rate(conc_count, conc_us))}/sec  (#{@steady_state_seconds}s steady-state)"
      )

      {batch_count, batch_us} = run_batch_phase(store, series, steady_start)

      IO.puts(
        "  Write (batch):      #{format_number(rate(batch_count, batch_us))}/sec  (#{@steady_state_seconds}s steady-state)"
      )

      {flush_us, _} = :timer.tc(fn -> TimelessMetrics.flush(store) end)
      IO.puts("  Flush + compress:   #{div(flush_us, 1000)}ms")

      {q_compressed_us, {:ok, compressed_points}} =
        :timer.tc(fn ->
          TimelessMetrics.query(store, sample_metric, sample_labels,
            from: population_start - 1,
            to: steady_start + @steady_state_seconds + 1
          )
        end)

      IO.puts(
        "  Query (compressed): #{length(compressed_points)} pts in #{div(q_compressed_us, 1000)}ms"
      )

      query_latencies =
        for _ <- 1..100 do
          {elapsed, {:ok, _points}} =
            :timer.tc(fn ->
              TimelessMetrics.query(store, sample_metric, sample_labels,
                from: population_start - 1,
                to: steady_start + @steady_state_seconds + 1
              )
            end)

          elapsed
        end

      IO.puts("  Query p95 (warm):   #{percentile(query_latencies, 0.95)}µs (100 runs)")

      info = TimelessMetrics.info(store)
      points_ingested = Map.get(info, :points_ingested, info.total_points)
      IO.puts("  Series:             #{info.series_count}")
      IO.puts("  Points ingested:    #{format_number(points_ingested)}")
      IO.puts("  Storage:            #{format_bytes(info.storage_bytes)}")
      IO.puts("  Bytes/point:        #{Float.round(info.bytes_per_point, 3)}")
      IO.puts("")
    after
      Supervisor.stop(sup)
      if not memory_only, do: File.rm_rf!(data_dir)
    end
  end

  defp build_series(series_count, metrics) do
    for series_idx <- 1..series_count do
      metric = Enum.at(metrics, rem(series_idx, length(metrics)))
      labels = %{"host" => "device_#{series_idx}"}
      {metric, labels, series_idx}
    end
  end

  defp populate_store(store, series, pts_per_series, population_start) do
    for ts_offset <- 0..(pts_per_series - 1) do
      ts = population_start + ts_offset

      entries =
        Enum.map(series, fn {metric, labels, series_idx} ->
          value = TimelessMetrics.DataGenerator.value(metric, ts * 1000, series_idx)
          {metric, labels, value, ts}
        end)

      :ok = TimelessMetrics.write_batch(store, entries)
    end
  end

  defp run_single_phase(store, series, steady_start) do
    deadline = System.monotonic_time(:millisecond) + @steady_state_seconds * 1000

    {count, _ts} =
      Stream.cycle(series)
      |> Enum.reduce_while({0, steady_start}, fn {metric, labels, series_idx}, {count, ts} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:halt, {count, ts}}
        else
          value = TimelessMetrics.DataGenerator.value(metric, ts * 1000, series_idx)
          :ok = TimelessMetrics.write(store, metric, labels, value, timestamp: ts)
          {:cont, {count + 1, ts + 1}}
        end
      end)

    {count, @steady_state_seconds * 1_000_000}
  end

  defp run_concurrent_phase(store, series, steady_start) do
    deadline = System.monotonic_time(:millisecond) + @steady_state_seconds * 1000
    worker_count = System.schedulers_online()
    counter = :atomics.new(1, [])
    chunk_size = max(div(length(series), worker_count), 1)

    series
    |> Enum.chunk_every(chunk_size)
    |> Enum.with_index()
    |> Enum.map(fn {chunk, worker_idx} ->
      Task.async(fn ->
        Stream.cycle(chunk)
        |> Enum.reduce_while(steady_start + worker_idx, fn {metric, labels, series_idx}, ts ->
          if System.monotonic_time(:millisecond) >= deadline do
            {:halt, :ok}
          else
            value = TimelessMetrics.DataGenerator.value(metric, ts * 1000, series_idx)
            :ok = TimelessMetrics.write(store, metric, labels, value, timestamp: ts)
            :atomics.add_get(counter, 1, 1)
            {:cont, ts + worker_count}
          end
        end)
      end)
    end)
    |> Task.await_many(:infinity)

    {:atomics.get(counter, 1), @steady_state_seconds * 1_000_000}
  end

  defp run_batch_phase(store, series, steady_start) do
    deadline = System.monotonic_time(:millisecond) + @steady_state_seconds * 1000
    windows = Enum.chunk_every(series, @batch_window)

    {count, _ts} =
      Stream.cycle(windows)
      |> Enum.reduce_while({0, steady_start}, fn window, {count, ts} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:halt, {count, ts}}
        else
          entries =
            Enum.map(window, fn {metric, labels, series_idx} ->
              value = TimelessMetrics.DataGenerator.value(metric, ts * 1000, series_idx)
              {metric, labels, value, ts}
            end)

          :ok = TimelessMetrics.write_batch(store, entries)
          {:cont, {count + length(entries), ts + 1}}
        end
      end)

    {count, @steady_state_seconds * 1_000_000}
  end

  defp run_compression_analysis(memory_only, engine) do
    now = System.os_time(:second)
    n = 10_000

    patterns = [
      {"node_load1 (gauge)",
       fn i -> TimelessMetrics.DataGenerator.value("node_load1", (now + i) * 1000, 1) end},
      {"node_cpu_seconds (ctr)",
       fn i ->
         TimelessMetrics.DataGenerator.value("node_cpu_seconds_total", (now + i) * 1000, 1)
       end},
      {"node_mem_total (const)",
       fn i ->
         TimelessMetrics.DataGenerator.value("node_memory_MemTotal_bytes", (now + i) * 1000, 1)
       end},
      {"node_disk_read (ctr)",
       fn i ->
         TimelessMetrics.DataGenerator.value("node_disk_read_bytes_total", (now + i) * 1000, 1)
       end},
      {"node_entropy (gauge)",
       fn i ->
         TimelessMetrics.DataGenerator.value("node_entropy_avail_bits", (now + i) * 1000, 1)
       end},
      {"Random float", fn _i -> :rand.uniform() * 1000.0 end}
    ]

    IO.puts("  #{format_number(n)} points per pattern:\n")
    IO.puts("  ┌────────────────────────────┬────────────┬──────────────┐")
    IO.puts("  │ Pattern                    │ Compressed │ Bytes/point  │")
    IO.puts("  ├────────────────────────────┼────────────┼──────────────┤")

    Enum.each(patterns, fn {label, gen_fn} ->
      data_dir = "/tmp/timeless_cmp_#{System.os_time(:millisecond)}"
      store = :"cmp_#{System.unique_integer([:positive])}"

      store_opts =
        if memory_only do
          [name: store, mode: :memory, self_monitor: false]
        else
          [name: store, data_dir: data_dir, engine: engine, self_monitor: false, scraping: false]
        end

      {:ok, sup} =
        Supervisor.start_link(
          [{TimelessMetrics, store_opts}],
          strategy: :one_for_one
        )

      try do
        for i <- 0..(n - 1) do
          TimelessMetrics.write(store, "bench", %{"t" => "1"}, gen_fn.(i), timestamp: now + i)
        end

        TimelessMetrics.flush(store)

        info = TimelessMetrics.info(store)
        points_ingested = Map.get(info, :points_ingested, info.total_points)

        bpp =
          if points_ingested > 0 do
            Float.round(info.storage_bytes / points_ingested, 2)
          else
            0.0
          end

        label_fmt = String.pad_trailing(label, 26)
        bytes_fmt = String.pad_leading(format_bytes(info.storage_bytes), 10)
        bpp_fmt = String.pad_leading("#{bpp}", 12)

        IO.puts("  │ #{label_fmt} │ #{bytes_fmt} │ #{bpp_fmt} │")
      after
        Supervisor.stop(sup)
        if not memory_only, do: File.rm_rf!(data_dir)
      end
    end)

    IO.puts("  └────────────────────────────┴────────────┴──────────────┘")
    IO.puts("")
  end

  defp rate(count, microseconds), do: trunc(count / (microseconds / 1_000_000))

  defp percentile(values, quantile) do
    sorted = Enum.sort(values)
    Enum.at(sorted, max(ceil(length(sorted) * quantile) - 1, 0))
  end

  defp option_value(args, option) do
    case Enum.find_index(args, &(&1 == option)) do
      nil -> nil
      index -> Enum.at(args, index + 1)
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
