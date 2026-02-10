defmodule Mix.Tasks.BenchRetention do
  @shortdoc "Benchmark retention DELETE with and without end_time/bucket indexes"
  @moduledoc """
  Populates a Timeless store with a configurable dataset, then benchmarks
  retention deletion with and without the secondary indexes on `end_time`
  and tier `bucket` columns.

  ## Usage

      mix bench_retention                    # default: 10K series, 30 days
      mix bench_retention --series 100000    # 100K series
      mix bench_retention --days 90          # 90 days of data
      mix bench_retention --expire-pct 33    # expire 33% of data (default 33)

  ## What it measures

  1. Populates raw_segments + triggers rollup so tier tables have data
  2. Runs DELETE with indexes (as created by segment_builder)
  3. Drops indexes, repopulates, runs DELETE without indexes
  4. Compares wall-clock times
  """

  use Mix.Task

  @metrics ~w(cpu mem disk load net_in net_out errors packets temp signal)
  @interval 300
  @intervals_per_day 288

  def run(args) do
    Mix.Task.run("app.start")

    {series_target, days, expire_pct} = parse_args(args)

    devices = max(div(series_target, length(@metrics)), 1)
    metrics_count = length(@metrics)
    actual_series = devices * metrics_count
    segments_per_series = div(days * @intervals_per_day * @interval, 7200)
    est_segments = actual_series * max(segments_per_series, 1)

    IO.puts(bar())
    IO.puts("  Retention DELETE Benchmark")
    IO.puts(bar())
    IO.puts("  Devices:        #{fmt(devices)}")
    IO.puts("  Metrics/device: #{metrics_count}")
    IO.puts("  Series:         #{fmt(actual_series)}")
    IO.puts("  Days:           #{days}")
    IO.puts("  Est. segments:  #{fmt(est_segments)} (across all shards)")
    IO.puts("  Expire:         #{expire_pct}% of data")
    IO.puts(bar())
    IO.puts("")

    # --- Run WITH indexes (current code creates them) ---
    IO.puts("== Test 1: DELETE WITH indexes ==")
    {with_results, data_dir1} = run_trial(devices, days, expire_pct, :with_index)
    IO.puts("")

    # --- Run WITHOUT indexes ---
    IO.puts("== Test 2: DELETE WITHOUT indexes ==")
    {without_results, data_dir2} = run_trial(devices, days, expire_pct, :without_index)
    IO.puts("")

    # --- Summary ---
    IO.puts(bar())
    IO.puts("  Results")
    IO.puts(bar())
    IO.puts("  WITH indexes:")
    IO.puts("    Raw DELETE:    #{fmt_ms(with_results.raw_delete_us)}")
    IO.puts("    Tier DELETE:   #{fmt_ms(with_results.tier_delete_us)}")
    IO.puts("    Total:         #{fmt_ms(with_results.raw_delete_us + with_results.tier_delete_us)}")
    IO.puts("")
    IO.puts("  WITHOUT indexes:")
    IO.puts("    Raw DELETE:    #{fmt_ms(without_results.raw_delete_us)}")
    IO.puts("    Tier DELETE:   #{fmt_ms(without_results.tier_delete_us)}")
    IO.puts("    Total:         #{fmt_ms(without_results.raw_delete_us + without_results.tier_delete_us)}")
    IO.puts("")

    raw_speedup = safe_div(without_results.raw_delete_us, with_results.raw_delete_us)
    tier_speedup = safe_div(without_results.tier_delete_us, with_results.tier_delete_us)
    total_with = with_results.raw_delete_us + with_results.tier_delete_us
    total_without = without_results.raw_delete_us + without_results.tier_delete_us
    total_speedup = safe_div(total_without, total_with)

    IO.puts("  Speedup:")
    IO.puts("    Raw:           #{Float.round(raw_speedup, 1)}x")
    IO.puts("    Tier:          #{Float.round(tier_speedup, 1)}x")
    IO.puts("    Total:         #{Float.round(total_speedup, 1)}x")
    IO.puts(bar())
    IO.puts("")
    IO.puts("  Cleanup: rm -rf #{data_dir1} #{data_dir2}")
  end

  # --- Trial runner ---

  defp run_trial(devices, days, expire_pct, mode) do
    data_dir = "/tmp/timeless_retention_bench_#{mode}_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    shards = System.schedulers_online()
    seg_dur = 7200

    schema = %Timeless.Schema{
      raw_retention_seconds: (days + 30) * 86_400,
      rollup_interval: :timer.hours(999),
      retention_interval: :timer.hours(999),
      tiers: Timeless.Schema.default().tiers
    }

    store = :"bench_ret_#{mode}"

    {:ok, sup} =
      Timeless.Supervisor.start_link(
        name: store,
        data_dir: data_dir,
        buffer_shards: shards,
        segment_duration: seg_dur,
        schema: schema,
        flush_threshold: 50_000,
        flush_interval: :timer.seconds(60)
      )

    # Populate data
    IO.puts("  Populating #{fmt(devices * length(@metrics))} series × #{days} days...")
    {ingest_us, _} = :timer.tc(fn -> populate(store, devices, days, seg_dur) end)
    IO.puts("  Ingested in #{fmt_ms(ingest_us)}")

    # Trigger rollup so tier tables have data
    IO.puts("  Running rollup...")
    {rollup_us, _} = :timer.tc(fn -> Timeless.rollup(store, :all) end)
    IO.puts("  Rollup in #{fmt_ms(rollup_us)}")

    # Flush everything to disk
    Timeless.flush(store)

    # Report segment counts
    report_counts(store, shards, schema)

    # If testing without index, drop the indexes now
    if mode == :without_index do
      IO.puts("  Dropping indexes...")
      drop_indexes(store, shards, schema)
    end

    # Compute cutoff: expire the oldest N% of data
    now = System.os_time(:second)
    total_span = days * 86_400
    cutoff = now - total_span + div(total_span * expire_pct, 100)

    IO.puts("  Deleting segments older than #{expire_pct}% mark (cutoff: #{cutoff})...")

    # Benchmark raw segment deletion
    {raw_us, _} = :timer.tc(fn ->
      for i <- 0..(shards - 1) do
        builder = :"#{store}_builder_#{i}"
        Timeless.SegmentBuilder.delete_raw_before(builder, cutoff)
      end
    end)

    IO.puts("  Raw DELETE: #{fmt_ms(raw_us)}")

    # Benchmark tier deletion
    {tier_us, _} = :timer.tc(fn ->
      Enum.each(schema.tiers, fn tier ->
        for i <- 0..(shards - 1) do
          builder = :"#{store}_builder_#{i}"
          Timeless.SegmentBuilder.delete_tier_before(builder, tier.name, cutoff)
        end
      end)
    end)

    IO.puts("  Tier DELETE: #{fmt_ms(tier_us)}")

    # Report remaining counts
    report_counts(store, shards, schema)

    # Cleanup supervisor
    Supervisor.stop(sup)

    {%{raw_delete_us: raw_us, tier_delete_us: tier_us}, data_dir}
  end

  # --- Data population ---

  defp populate(store, devices, days, seg_dur) do
    registry = :"#{store}_registry"
    now = System.os_time(:second)
    start_ts = div(now - days * 86_400, seg_dur) * seg_dur

    # Pre-register all series
    for dev <- 0..(devices - 1), metric <- @metrics do
      labels = %{"host" => "dev_#{dev}"}
      Timeless.SeriesRegistry.get_or_create(registry, metric, labels)
    end

    # Write day by day using batch writes
    labels_for = 0..(devices - 1) |> Enum.map(&%{"host" => "dev_#{&1}"}) |> List.to_tuple()

    for day <- 0..(days - 1) do
      day_start = start_ts + day * 86_400

      # Write in chunks to avoid overwhelming buffers
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
        IO.puts("    Day #{day + 1}/#{days} ingested")
      end
    end

    Timeless.flush(store)
  end

  # --- Index management ---

  defp drop_indexes(_store, _shards, _schema) do
    # No-op: file-based storage has no SQL indexes to drop
    :ok
  end

  # --- Reporting ---

  defp report_counts(store, shards, schema) do
    total_raw =
      Enum.reduce(0..(shards - 1), 0, fn i, acc ->
        builder = :"#{store}_builder_#{i}"
        stats = Timeless.SegmentBuilder.raw_stats(builder)
        acc + stats.segment_count
      end)

    tier_counts =
      Enum.map(schema.tiers, fn tier ->
        count =
          Enum.reduce(0..(shards - 1), 0, fn i, acc ->
            builder = :"#{store}_builder_#{i}"
            {c, _, _} = Timeless.SegmentBuilder.read_tier_stats(builder, tier.name)
            acc + c
          end)
        {tier.name, count}
      end)

    IO.puts("  Counts: #{fmt(total_raw)} raw segments" <>
      Enum.map_join(tier_counts, "", fn {name, count} -> ", #{fmt(count)} #{name}" end))
  end

  # --- Arg parsing ---

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [series: :integer, days: :integer, expire_pct: :integer]
      )

    series = Keyword.get(opts, :series, 10_000)
    days = Keyword.get(opts, :days, 30)
    expire_pct = Keyword.get(opts, :expire_pct, 33)

    {series, days, expire_pct}
  end

  # --- Formatters ---

  defp fmt(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp fmt(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt(n), do: "#{n}"

  defp fmt_ms(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)}s"
  defp fmt_ms(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 1)}ms"
  defp fmt_ms(us), do: "#{us}µs"

  defp safe_div(_, 0), do: 0.0
  defp safe_div(a, b), do: a / b

  defp bar, do: String.duplicate("=", 56)
end
