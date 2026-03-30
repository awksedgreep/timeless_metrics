defmodule RustEngineBaseline do
  @moduledoc """
  Focused Rust-engine benchmark for the current hot paths.

  Measures:
  - labeled writes against already-known series
  - labeled writes that create new series
  - raw binary writes with pre-resolved series ids
  - flush latency
  - single-series and multi-series query latency after flush

  Usage:
    mix run bench/rust_engine_baseline.exs
    mix run bench/rust_engine_baseline.exs --memory
    mix run bench/rust_engine_baseline.exs --series 5000 --batch-size 1000 --points 200
  """

  alias TimelessMetrics.DataGenerator
  alias TimelessMetrics.RustEngine
  alias TimelessMetrics.RustEngine.Nif

  @default_series 2_000
  @default_points 120
  @default_batch_size 500

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          memory: :boolean,
          series: :integer,
          points: :integer,
          batch_size: :integer
        ]
      )

    memory_only = Keyword.get(opts, :memory, false)
    series_count = Keyword.get(opts, :series, @default_series)
    points_per_series = Keyword.get(opts, :points, @default_points)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    mode_label = if memory_only, do: "MEMORY-ONLY", else: "DISK"
    data_dir = "/tmp/timeless_rust_engine_baseline_#{System.os_time(:millisecond)}"
    store = :"rust_engine_baseline_#{System.unique_integer([:positive])}"

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
      metrics = DataGenerator.metrics() |> Enum.take(12)
      base_series = build_series(series_count, metrics, 0)
      new_series = build_series(series_count, metrics, series_count)
      now = System.os_time(:second)
      start_ts = now - points_per_series - 10
      prewarmed_series_ids = prewarm_series_ids(store, base_series)

      IO.puts("")
      IO.puts("=============================================")
      IO.puts("  TimelessMetrics Rust Engine Baseline (#{mode_label})")
      IO.puts("  Schedulers: #{System.schedulers_online()}")
      IO.puts("  Series: #{format_number(series_count)}")
      IO.puts("  Points/series: #{format_number(points_per_series)}")
      IO.puts("  Batch size: #{format_number(batch_size)}")
      IO.puts("=============================================")
      IO.puts("")

      labeled_existing_entries =
        build_labeled_entries(base_series, points_per_series, start_ts)

      {existing_write_us, :ok} =
        :timer.tc(fn ->
          write_labeled_batches(store, labeled_existing_entries, batch_size)
        end)

      raw_entries =
        build_raw_entries(
          base_series,
          prewarmed_series_ids,
          points_per_series,
          start_ts + points_per_series
        )

      {raw_write_us, :ok} =
        :timer.tc(fn ->
          write_raw_batches(ref, raw_entries, batch_size)
        end)

      new_series_entries =
        build_labeled_entries(new_series, 1, start_ts + points_per_series * 2 + 1)

      {new_series_us, :ok} =
        :timer.tc(fn ->
          write_labeled_batches(store, new_series_entries, batch_size)
        end)

      {flush_us, flush_result} =
        :timer.tc(fn ->
          Nif.engine_flush(ref)
          |> unwrap_nif_ok()
        end)

      {sample_metric, sample_labels, _sample_idx} = hd(base_series)
      query_from = start_ts
      query_to = start_ts + points_per_series * 2 + 5

      {single_query_us, {:ok, single_results}} =
        :timer.tc(fn ->
          Nif.engine_query_range(ref, sample_metric, sample_labels, query_from, query_to)
          |> unwrap_nif_ok()
        end)

      multi_filter = %{"env" => "prod"}

      {multi_query_us, {:ok, multi_results}} =
        :timer.tc(fn ->
          Nif.engine_query_range(ref, sample_metric, multi_filter, query_from, query_to)
          |> unwrap_nif_ok()
        end)

      {aggregate_us, {:ok, aggregate_results}} =
        :timer.tc(fn ->
          Nif.engine_query_aggregate(ref, sample_metric, multi_filter, query_from, query_to, :avg)
          |> unwrap_nif_ok()
        end)

      {:ok, info} =
        Nif.engine_info(ref)
        |> unwrap_nif_ok()

      existing_points = length(labeled_existing_entries)
      raw_points = length(raw_entries)
      new_series_points = length(new_series_entries)

      print_result("Labeled write (cached series)", existing_points, existing_write_us)
      print_result("Raw write (pre-resolved ids)", raw_points, raw_write_us)
      print_result("Labeled write (new series)", new_series_points, new_series_us)
      {:ok, :ok} = flush_result
      IO.puts("Flush:                         #{div(flush_us, 1000)}ms")

      total_bytes = trunc(info["total_bytes"] || 0)
      bytes_per_point = info["bytes_per_point"] || 0.0
      disk_points = trunc(info["disk_points"] || 0)
      buffered_points = trunc(info["buffered_points"] || 0)

      single_points =
        case single_results do
          [{_, points}] -> length(points)
          [] -> 0
        end

      IO.puts(
        "Single-series query:           #{single_points} pts in #{div(single_query_us, 1000)}ms"
      )

      IO.puts(
        "Multi-series query:            #{length(multi_results)} series in #{div(multi_query_us, 1000)}ms"
      )

      IO.puts(
        "Aggregate query:               #{length(aggregate_results)} series in #{div(aggregate_us, 1000)}ms"
      )

      IO.puts("Storage bytes:                 #{format_number(total_bytes)}")
      IO.puts("Bytes/point:                   #{Float.round(bytes_per_point, 3)}")
      IO.puts("Disk points:                   #{format_number(disk_points)}")
      IO.puts("Buffered points:               #{format_number(buffered_points)}")
      IO.puts("")
      IO.puts("Suggested follow-up comparisons:")
      IO.puts("  1. existing-series labeled vs raw write throughput")
      IO.puts("  2. new-series labeled throughput before/after series persistence changes")
      IO.puts("  3. multi-series query latency before/after file-read reuse")
      IO.puts("")
    after
      Supervisor.stop(sup)
      if not memory_only, do: File.rm_rf!(data_dir)
    end
  end

  defp build_series(series_count, metrics, offset) do
    metric_count = length(metrics)

    for series_idx <- 1..series_count do
      absolute_idx = offset + series_idx
      metric = Enum.at(metrics, rem(absolute_idx, metric_count))

      labels = %{
        "host" => "device_#{absolute_idx}",
        "env" => "prod",
        "region" => if(rem(absolute_idx, 2) == 0, do: "us-east", else: "us-west"),
        "service" => "svc_#{rem(absolute_idx, 8)}"
      }

      {metric, labels, absolute_idx}
    end
  end

  defp build_labeled_entries(series, points_per_series, start_ts) do
    for point_idx <- 0..(points_per_series - 1),
        {metric, labels, series_idx} <- series do
      ts = start_ts + point_idx
      value = DataGenerator.value(metric, ts * 1000, series_idx)
      {metric, labels, value, ts}
    end
  end

  defp prewarm_series_ids(store, series) do
    Map.new(series, fn {metric, labels, series_idx} ->
      {:ok, series_id} =
        TimelessMetrics.resolve_series(store, metric, labels)

      {series_idx, series_id}
    end)
  end

  defp build_raw_entries(series, series_ids, points_per_series, start_ts) do
    for point_idx <- 0..(points_per_series - 1),
        {metric, _labels, series_idx} <- series do
      ts = start_ts + point_idx
      value = DataGenerator.value(metric, ts * 1000, series_idx)
      {Map.fetch!(series_ids, series_idx), ts, value}
    end
  end

  defp write_labeled_batches(store, entries, batch_size) do
    entries
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn batch ->
      :ok = TimelessMetrics.write_batch(store, batch)
    end)

    :ok
  end

  defp write_raw_batches(ref, entries, batch_size) do
    entries
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn batch ->
      payload = encode_raw_batch(batch)

      {:ok, :ok} =
        Nif.engine_write_batch_raw(ref, payload)
        |> unwrap_nif_ok()
    end)

    :ok
  end

  defp encode_raw_batch(entries) do
    for {series_id, ts, value} <- entries, into: <<>> do
      <<series_id::signed-native-64, ts::signed-native-64, value::float-native-64>>
    end
  end

  defp print_result(label, count, elapsed_us) do
    rate = if elapsed_us > 0, do: trunc(count * 1_000_000 / elapsed_us), else: 0

    IO.puts(
      "#{String.pad_trailing(label <> ":", 31)} #{format_number(rate)}/sec  (#{div(elapsed_us, 1000)}ms)"
    )
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

RustEngineBaseline.run(System.argv())
