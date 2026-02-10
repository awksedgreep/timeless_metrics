defmodule Mix.Tasks.BenchCompressedTiers do
  @shortdoc "Benchmark compressed tier storage, rollup speed, and query latency"
  @moduledoc """
  Populates a Timeless store, triggers rollup, and measures:

  1. Storage: bytes on disk for raw vs tier data, bytes per bucket
  2. Rollup speed: time to roll up raw → hourly → daily
  3. Query latency: aggregate queries at various time ranges
  4. Compression ratios: chunk blob size vs old row-per-bucket estimate

  ## Usage

      mix bench_compressed_tiers                    # default: 1K series, 7 days
      mix bench_compressed_tiers --series 10000     # 10K series
      mix bench_compressed_tiers --days 30          # 30 days of data

  ## What it measures

  - Ingestion rate (points/sec)
  - Rollup wall time per tier
  - On-disk storage breakdown (raw segments vs tier chunks)
  - Compression ratio (tier bytes / old row estimate)
  - Query latency for 1h, 24h, 7d aggregate windows
  """

  use Mix.Task

  @metrics ~w(cpu mem disk load net_in net_out errors packets temp signal)
  @interval 300
  @intervals_per_day 288

  def run(args) do
    Mix.Task.run("app.start")

    {series_target, days} = parse_args(args)

    devices = max(div(series_target, length(@metrics)), 1)
    metrics_count = length(@metrics)
    actual_series = devices * metrics_count
    shards = System.schedulers_online()

    IO.puts(bar())
    IO.puts("  Compressed Tier Benchmark")
    IO.puts(bar())
    IO.puts("  Devices:        #{fmt(devices)}")
    IO.puts("  Metrics/device: #{metrics_count}")
    IO.puts("  Series:         #{fmt(actual_series)}")
    IO.puts("  Days:           #{days}")
    IO.puts("  Shards:         #{shards}")
    IO.puts(bar())
    IO.puts("")

    data_dir = "/tmp/timeless_bench_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    schema = Timeless.Schema.default()

    {:ok, sup} =
      Timeless.Supervisor.start_link(
        name: :bench_ct,
        data_dir: data_dir,
        buffer_shards: shards,
        schema: schema,
        flush_threshold: 50_000,
        flush_interval: :timer.seconds(60)
      )

    # --- Ingest ---
    IO.puts("== Ingestion ==")
    {ingest_us, total_points} = :timer.tc(fn -> populate(:bench_ct, devices, days) end)
    IO.puts("  #{fmt(total_points)} points in #{fmt_ms(ingest_us)}")
    IO.puts("  Rate: #{fmt(trunc(total_points / (ingest_us / 1_000_000)))} pts/sec")
    IO.puts("")

    # --- Rollup ---
    IO.puts("== Rollup ==")

    {rollup1_us, _} = :timer.tc(fn -> Timeless.rollup(:bench_ct, :all) end)
    IO.puts("  First rollup:  #{fmt_ms(rollup1_us)}")

    {rollup2_us, _} = :timer.tc(fn -> Timeless.rollup(:bench_ct, :all) end)
    IO.puts("  Second rollup: #{fmt_ms(rollup2_us)} (idempotent, no new data)")
    IO.puts("")

    # Flush everything
    Timeless.flush(:bench_ct)

    # --- Storage ---
    IO.puts("== Storage ==")
    {raw_rows, raw_bytes, tier_stats} = measure_storage(:bench_ct, shards, schema)

    IO.puts("  Raw segments:")
    IO.puts("    Rows:  #{fmt(raw_rows)}")
    IO.puts("    Bytes: #{fmt_bytes(raw_bytes)}")
    if raw_rows > 0 do
      IO.puts("    Avg:   #{Float.round(raw_bytes / raw_rows, 1)} bytes/segment")
    end
    IO.puts("")

    Enum.each(tier_stats, fn {tier, chunks, bucket_count, tier_bytes} ->
      old_estimate = bucket_count * 78
      ratio = if tier_bytes > 0, do: old_estimate / tier_bytes, else: 0.0

      IO.puts("  #{tier}:")
      IO.puts("    Chunks:          #{fmt(chunks)}")
      IO.puts("    Total buckets:   #{fmt(bucket_count)}")
      IO.puts("    Bytes:           #{fmt_bytes(tier_bytes)}")
      if bucket_count > 0 do
        IO.puts("    Bytes/bucket:    #{Float.round(tier_bytes / bucket_count, 1)}")
      end
      IO.puts("    Old row est:     #{fmt_bytes(old_estimate)} (78 bytes/row)")
      IO.puts("    Savings:         #{Float.round(ratio, 1)}x")
      IO.puts("")
    end)

    total_tier_bytes =
      Enum.reduce(tier_stats, 0, fn {_, _, _, bytes}, acc -> acc + bytes end)

    total_disk = measure_disk(data_dir)
    IO.puts("  Total on disk:     #{fmt_bytes(total_disk)}")
    IO.puts("  Raw data:          #{fmt_bytes(raw_bytes)} (#{pct(raw_bytes, total_disk)})")
    IO.puts("  Tier data:         #{fmt_bytes(total_tier_bytes)} (#{pct(total_tier_bytes, total_disk)})")
    IO.puts("")

    # --- Query latency ---
    IO.puts("== Query Latency ==")

    # Pick a random series for querying
    registry = :bench_ct_registry
    metric = hd(@metrics)
    labels = %{"host" => "dev_0"}
    _series_id = Timeless.SeriesRegistry.get_or_create(registry, metric, labels)

    now = System.os_time(:second)

    for {label, from} <- [{"1h", now - 3600}, {"24h", now - 86_400}, {"7d", now - 7 * 86_400}] do
      if from >= now - days * 86_400 do
        {us, {:ok, results}} =
          :timer.tc(fn ->
            Timeless.query_aggregate(:bench_ct, metric, labels,
              from: from,
              to: now,
              bucket: :hour,
              aggregate: :avg
            )
          end)

        IO.puts("  #{label} range: #{fmt_ms(us)} (#{length(results)} buckets)")
      end
    end

    IO.puts("")

    # --- Summary ---
    IO.puts(bar())
    IO.puts("  Summary")
    IO.puts(bar())
    IO.puts("  Series:            #{fmt(actual_series)}")
    IO.puts("  Days:              #{days}")
    IO.puts("  Points ingested:   #{fmt(total_points)}")
    IO.puts("  Ingest rate:       #{fmt(trunc(total_points / (ingest_us / 1_000_000)))} pts/sec")
    IO.puts("  Rollup time:       #{fmt_ms(rollup1_us)}")
    IO.puts("  Total disk:        #{fmt_bytes(total_disk)}")

    if total_points > 0 do
      IO.puts("  Bytes/point:       #{Float.round(total_disk / total_points, 2)}")
    end

    IO.puts(bar())
    IO.puts("")
    IO.puts("  Cleanup: rm -rf #{data_dir}")

    Supervisor.stop(sup)
  end

  # --- Population ---

  defp populate(store, devices, days) do
    now = System.os_time(:second)
    seg_dur = 14_400
    start_ts = div(now - days * 86_400, seg_dur) * seg_dur
    labels_for = 0..(devices - 1) |> Enum.map(&%{"host" => "dev_#{&1}"}) |> List.to_tuple()
    total_points = devices * length(@metrics) * days * @intervals_per_day

    for day <- 0..(days - 1) do
      day_start = start_ts + day * 86_400

      0..(@intervals_per_day - 1)
      |> Enum.chunk_every(48)
      |> Enum.each(fn interval_chunk ->
        batch =
          for interval <- interval_chunk,
              dev <- 0..(devices - 1),
              metric <- @metrics do
            ts = day_start + interval * @interval
            value = :rand.uniform() * 100
            {metric, elem(labels_for, dev), value, ts}
          end

        Timeless.write_batch(store, batch)
      end)

      if rem(day + 1, 5) == 0 do
        IO.puts("    Day #{day + 1}/#{days}")
      end
    end

    Timeless.flush(store)
    total_points
  end

  # --- Storage measurement ---

  defp measure_storage(store, shards, schema) do
    {raw_segs, raw_bytes} =
      Enum.reduce(0..(shards - 1), {0, 0}, fn i, {s_acc, b_acc} ->
        builder = :"#{store}_builder_#{i}"
        stats = Timeless.SegmentBuilder.raw_stats(builder)
        {s_acc + stats.segment_count, b_acc + stats.raw_bytes}
      end)

    tier_stats =
      Enum.map(schema.tiers, fn tier ->
        {chunks, bucket_count, tier_bytes} =
          Enum.reduce(0..(shards - 1), {0, 0, 0}, fn i, {c_acc, b_acc, tb_acc} ->
            builder = :"#{store}_builder_#{i}"
            {c, b, tb} = Timeless.SegmentBuilder.read_tier_stats(builder, tier.name)
            {c_acc + c, b_acc + b, tb_acc + tb}
          end)

        {tier.name, chunks, bucket_count, tier_bytes}
      end)

    {raw_segs, raw_bytes, tier_stats}
  end

  defp measure_disk(data_dir) do
    Path.wildcard(Path.join(data_dir, "**/*"))
    |> Enum.reduce(0, fn path, acc ->
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} -> acc + size
        _ -> acc
      end
    end)
  end

  # --- Arg parsing ---

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args, strict: [series: :integer, days: :integer])

    series = Keyword.get(opts, :series, 1_000)
    days = Keyword.get(opts, :days, 7)
    {series, days}
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

  defp pct(_, 0), do: "0%"
  defp pct(part, whole), do: "#{Float.round(part / whole * 100, 1)}%"

  defp bar, do: String.duplicate("=", 56)
end
