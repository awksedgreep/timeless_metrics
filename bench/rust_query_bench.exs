defmodule RustQueryBench do
  @moduledoc """
  Targeted Rust-engine query benchmark for high-fanout multi-series reads.

  Builds one metric across many matching series, flushes to disk or memory mode,
  then measures repeated multi-series range and aggregate queries.

  Usage:
    mix run bench/rust_query_bench.exs
    mix run bench/rust_query_bench.exs --memory
    mix run bench/rust_query_bench.exs --series 12000 --points 60 --runs 5
  """

  alias TimelessMetrics.DataGenerator
  alias TimelessMetrics.RustEngine
  alias TimelessMetrics.RustEngine.Nif

  @default_series 12_000
  @default_points 60
  @default_runs 5
  @batch_size 1_000

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          memory: :boolean,
          series: :integer,
          points: :integer,
          runs: :integer
        ]
      )

    memory_only = Keyword.get(opts, :memory, false)
    series_count = Keyword.get(opts, :series, @default_series)
    points_per_series = Keyword.get(opts, :points, @default_points)
    runs = Keyword.get(opts, :runs, @default_runs)

    mode_label = if memory_only, do: "MEMORY-ONLY", else: "DISK"
    data_dir = "/tmp/timeless_rust_query_bench_#{System.os_time(:millisecond)}"
    store = :"rust_query_bench_#{System.unique_integer([:positive])}"

    store_opts =
      if memory_only do
        [name: store, data_dir: data_dir, mode: :memory, self_monitor: false]
      else
        [name: store, data_dir: data_dir, self_monitor: false]
      end

    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, store_opts}],
        strategy: :one_for_one
      )

    try do
      ref = RustEngine.ref(store)
      metric = "query_hot_metric"
      query_filter = %{"env" => "prod"}
      now = System.os_time(:second)
      start_ts = now - points_per_series - 10
      end_ts = start_ts + points_per_series + 5
      series = build_series(series_count)

      IO.puts("")
      IO.puts("=============================================")
      IO.puts("  TimelessMetrics Rust Query Benchmark (#{mode_label})")
      IO.puts("  Schedulers: #{System.schedulers_online()}")
      IO.puts("  Series fanout: #{format_number(series_count)}")
      IO.puts("  Points/series: #{format_number(points_per_series)}")
      IO.puts("  Timed runs: #{runs}")
      IO.puts("=============================================")
      IO.puts("")

      {populate_us, :ok} =
        :timer.tc(fn ->
          populate(store, metric, series, points_per_series, start_ts)
        end)

      {flush_us, :ok} =
        :timer.tc(fn ->
          TimelessMetrics.flush(store)
        end)

      {:ok, warm_range} =
        TimelessMetrics.query_multi(store, metric, query_filter, from: start_ts, to: end_ts)

      {:ok, warm_nif_range} =
        Nif.engine_query_range(ref, metric, query_filter, start_ts, end_ts)
        |> unwrap_nif_ok()

      {:ok, warm_agg} =
        TimelessMetrics.query_aggregate_multi(store, metric, query_filter,
          from: start_ts,
          to: end_ts,
          aggregate: :avg
        )

      range_runs =
        for _ <- 1..runs do
          elem(
            :timer.tc(fn ->
              {:ok, _} =
                TimelessMetrics.query_multi(store, metric, query_filter,
                  from: start_ts,
                  to: end_ts
                )
            end),
            0
          )
        end

      nif_range_runs =
        for _ <- 1..runs do
          elem(
            :timer.tc(fn ->
              {:ok, _} =
                Nif.engine_query_range(ref, metric, query_filter, start_ts, end_ts)
                |> unwrap_nif_ok()
            end),
            0
          )
        end

      aggregate_runs =
        for _ <- 1..runs do
          elem(
            :timer.tc(fn ->
              {:ok, _} =
                TimelessMetrics.query_aggregate_multi(store, metric, query_filter,
                  from: start_ts,
                  to: end_ts,
                  aggregate: :avg
                )
            end),
            0
          )
        end

      range_stats = summarize_us(range_runs)
      nif_range_stats = summarize_us(nif_range_runs)
      aggregate_stats = summarize_us(aggregate_runs)
      total_points = series_count * points_per_series

      IO.puts(
        "Populate:                      #{format_number(rate(total_points, populate_us))}/sec  (#{div(populate_us, 1000)}ms)"
      )

      IO.puts("Flush:                         #{div(flush_us, 1000)}ms")
      IO.puts("Warm range result:             #{length(warm_range)} series")
      IO.puts("Warm direct NIF range result:  #{length(warm_nif_range)} series")
      IO.puts("Warm aggregate result:         #{length(warm_agg)} series")
      IO.puts("Range query median:            #{format_ms(range_stats.median_us)}ms")
      IO.puts("Range query best:              #{format_ms(range_stats.min_us)}ms")
      IO.puts("Range query worst:             #{format_ms(range_stats.max_us)}ms")
      IO.puts("Direct NIF range median:       #{format_ms(nif_range_stats.median_us)}ms")
      IO.puts("Direct NIF range best:         #{format_ms(nif_range_stats.min_us)}ms")
      IO.puts("Direct NIF range worst:        #{format_ms(nif_range_stats.max_us)}ms")
      IO.puts("Aggregate query median:        #{format_ms(aggregate_stats.median_us)}ms")
      IO.puts("Aggregate query best:          #{format_ms(aggregate_stats.min_us)}ms")
      IO.puts("Aggregate query worst:         #{format_ms(aggregate_stats.max_us)}ms")
      IO.puts("")
    after
      Supervisor.stop(sup)
      if not memory_only, do: File.rm_rf!(data_dir)
    end
  end

  defp build_series(series_count) do
    for idx <- 1..series_count do
      labels = %{
        "host" => "device_#{idx}",
        "env" => "prod",
        "region" => if(rem(idx, 2) == 0, do: "us-east", else: "us-west"),
        "service" => "svc_#{rem(idx, 16)}"
      }

      {labels, idx}
    end
  end

  defp populate(store, metric, series, points_per_series, start_ts) do
    0..(points_per_series - 1)
    |> Enum.each(fn point_idx ->
      ts = start_ts + point_idx

      entries =
        Enum.map(series, fn {labels, series_idx} ->
          value = DataGenerator.value(metric, ts * 1000, series_idx)
          {metric, labels, value, ts}
        end)

      entries
      |> Enum.chunk_every(@batch_size)
      |> Enum.each(fn batch ->
        :ok = TimelessMetrics.write_batch(store, batch)
      end)
    end)

    :ok
  end

  defp summarize_us(times) do
    sorted = Enum.sort(times)
    len = length(sorted)
    median_us = Enum.at(sorted, div(len, 2))

    %{
      min_us: hd(sorted),
      median_us: median_us,
      max_us: List.last(sorted)
    }
  end

  defp rate(count, elapsed_us) when elapsed_us > 0, do: trunc(count * 1_000_000 / elapsed_us)

  defp format_ms(us) do
    Float.round(us / 1_000.0, 2)
  end

  defp format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/.{3}(?=.)/, "\\0,")
    |> String.reverse()
  end

  defp unwrap_nif_ok({:ok, {:ok, value}}), do: {:ok, value}
  defp unwrap_nif_ok({:ok, value}), do: {:ok, value}
  defp unwrap_nif_ok({:error, _} = error), do: error
end

RustQueryBench.run(System.argv())
