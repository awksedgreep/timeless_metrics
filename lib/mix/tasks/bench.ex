defmodule Mix.Tasks.Bench do
  @shortdoc "Run Timeless benchmarks with realistic ISP data"
  @moduledoc """
  Benchmarks Timeless with realistic ISP/network metric data.

  ## Usage

      mix bench                         # quick: 100 devices × 20 metrics, 90 days
      mix bench --tier fast-stress       # 5K devices × 2 days (~2 min)
      mix bench --tier medium            # 1K devices × 20 metrics, 90 days
      mix bench --tier stress            # 10K devices × 20 metrics, 7 days
      mix bench --devices 500 --days 30  # custom
      mix bench --data-dir /var/bench    # real disk (default: /tmp = tmpfs)

  ## What it measures

  1. **Ingest throughput** — writes/sec for historical backfill
  2. **Incremental rollups** — per-day rollup cost
  3. **Query latency** — raw, aggregate, multi-series, latest
  4. **Storage efficiency** — bytes/point, tier sizes, compression
  """

  use Mix.Task

  @metrics [
    "cpu_usage", "mem_usage", "disk_usage", "load_avg_1m",
    "if_in_octets", "if_out_octets", "if_in_errors", "if_out_errors",
    "if_in_packets", "if_out_packets", "temperature_c",
    "signal_power_dbm", "signal_snr_db", "ping_latency_ms",
    "packet_loss_pct", "uptime_seconds", "dns_queries_total",
    "http_requests_total", "active_connections", "bandwidth_util_pct"
  ]

  @interval 300
  @intervals_per_day 288

  def run(args) do
    Mix.Task.run("app.start")

    {tier_name, devices, days, seg_dur, custom_dir} = parse_args(args)

    metrics_count = length(@metrics)
    series_count = devices * metrics_count
    total_points = series_count * days * @intervals_per_day

    data_dir = custom_dir || "/tmp/timeless_bench_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    banner(tier_name, devices, metrics_count, days, series_count, total_points, seg_dur, data_dir)

    # Schema with disabled background ticks
    schema = %Timeless.Schema{
      raw_retention_seconds: (days + 7) * 86_400,
      rollup_interval: :timer.hours(999),
      retention_interval: :timer.hours(999),
      tiers: Timeless.Schema.default().tiers
    }

    {:ok, _} =
      Timeless.Supervisor.start_link(
        name: :bench,
        data_dir: data_dir,
        buffer_shards: System.schedulers_online(),
        segment_duration: seg_dur,
        schema: schema,
        flush_threshold: 50_000,
        flush_interval: :timer.seconds(60)
      )

    now = System.os_time(:second)
    # Align start_ts to segment boundary to avoid partial segments at day boundaries
    start_ts = div(now - days * 86_400, seg_dur) * seg_dur

    # Pre-compute labels for fast access
    labels_for = 0..(devices - 1) |> Enum.map(&%{"host" => "device_#{&1}"}) |> List.to_tuple()

    # Phase 0: Pre-register series
    phase0_register(devices, labels_for)

    # Phase 1: Ingest + incremental rollup
    phase1_ingest(devices, days, start_ts, labels_for, total_points)

    # Phase 2: Storage report
    phase2_storage()

    # Phase 3: Query benchmarks
    phase3_queries(start_ts, now, devices, labels_for)

    # Phase 4: Realtime write throughput
    phase4_realtime(devices, labels_for)

    # Footer
    IO.puts("")
    IO.puts(bar())
    IO.puts("  Data: #{data_dir}")
    IO.puts("  DB:   #{Timeless.DB.db_path(:bench_db)}")
    IO.puts("  Cleanup: rm -rf #{data_dir}")
    IO.puts(bar())
  end

  # ── Phase 0: Series Registration ──────────────────────────────────

  defp phase0_register(devices, labels_for) do
    header("Phase 0: Series Registration")

    {us, _} =
      :timer.tc(fn ->
        registry = :bench_registry

        for dev <- 0..(devices - 1), metric <- @metrics do
          Timeless.SeriesRegistry.get_or_create(registry, metric, elem(labels_for, dev))
        end
      end)

    series = devices * length(@metrics)
    IO.puts("  Registered #{fmt_int(series)} series in #{fmt_dur(us)}")
  end

  # ── Phase 1: Historical Backfill + Incremental Rollup ──────────────

  defp phase1_ingest(devices, days, start_ts, labels_for, total_points) do
    header("Phase 1: Historical Backfill + Incremental Rollup")

    pts_per_day = devices * length(@metrics) * @intervals_per_day
    total_ingested = :counters.new(1, [:atomics])
    total_write_us = :counters.new(1, [:atomics])
    total_flush_us = :counters.new(1, [:atomics])
    total_rollup_us = :counters.new(1, [:atomics])

    wall_start = System.monotonic_time(:microsecond)

    for day <- 0..(days - 1) do
      day_start = start_ts + day * 86_400

      # Write one day of data
      {write_us, _} =
        :timer.tc(fn ->
          for interval <- 0..(@intervals_per_day - 1) do
            ts = day_start + interval * @interval

            entries =
              for dev <- 0..(devices - 1), metric <- @metrics do
                {metric, elem(labels_for, dev), gen(metric, ts, dev, start_ts), ts}
              end

            Timeless.write_batch(:bench, entries)
          end
        end)

      :counters.add(total_write_us, 1, write_us)
      :counters.add(total_ingested, 1, pts_per_day)

      # Flush
      {flush_us, _} = :timer.tc(fn -> Timeless.flush(:bench) end)
      :counters.add(total_flush_us, 1, flush_us)

      # Incremental rollup (processes only this day since watermark advanced)
      {rollup_us, _} = :timer.tc(fn -> Timeless.rollup(:bench, :all) end)
      :counters.add(total_rollup_us, 1, rollup_us)

      # Progress
      ingested = :counters.get(total_ingested, 1)
      wall_now = System.monotonic_time(:microsecond)
      wall_elapsed = wall_now - wall_start
      avg_rate = if wall_elapsed > 0, do: trunc(ingested / (wall_elapsed / 1_000_000)), else: 0

      IO.write(
        "\r  Day #{String.pad_leading("#{day + 1}", 4)}/#{days}  " <>
          "#{fmt_int(ingested)}/#{fmt_int(total_points)} pts  " <>
          "#{fmt_int(avg_rate)} pts/sec  " <>
          "write=#{fmt_dur(write_us)} flush=#{fmt_dur(flush_us)} rollup=#{fmt_dur(rollup_us)}    "
      )
    end

    wall_end = System.monotonic_time(:microsecond)
    wall_total = wall_end - wall_start

    write_total = :counters.get(total_write_us, 1)
    flush_total = :counters.get(total_flush_us, 1)
    rollup_total = :counters.get(total_rollup_us, 1)

    write_rate = if write_total > 0, do: trunc(total_points / (write_total / 1_000_000)), else: 0
    wall_rate = if wall_total > 0, do: trunc(total_points / (wall_total / 1_000_000)), else: 0

    IO.puts("")
    IO.puts("  " <> String.duplicate("-", 60))
    IO.puts("  Ingest:  #{fmt_int(total_points)} pts  write=#{fmt_dur(write_total)}  [#{fmt_int(write_rate)} pts/sec]")
    IO.puts("  Flush:   #{days} passes in #{fmt_dur(flush_total)}  [#{fmt_dur(div(flush_total, days))} avg/day]")
    IO.puts("  Rollup:  #{days} passes in #{fmt_dur(rollup_total)}  [#{fmt_dur(div(rollup_total, days))} avg/day]")
    IO.puts("  Wall:    #{fmt_dur(wall_total)}  [#{fmt_int(wall_rate)} pts/sec effective]")
  end

  # ── Phase 2: Storage Report ────────────────────────────────────────

  defp phase2_storage do
    header("Phase 2: Storage Report")

    info = Timeless.info(:bench)

    IO.puts("  Series:      #{fmt_int(info.series_count)}")
    IO.puts("  Raw segments: #{fmt_int(info.segment_count)}")
    IO.puts("  Total points: #{fmt_int(info.total_points)}")
    IO.puts("  Compressed:   #{fmt_bytes(info.raw_compressed_bytes)}")
    IO.puts("  Bytes/point:  #{info.bytes_per_point}")
    IO.puts("")

    Enum.each(info.tiers, fn {name, t} ->
      IO.puts("  #{name}: #{fmt_int(t.rows)} rows  res=#{t.resolution_seconds}s  ret=#{t.retention}")
    end)

    IO.puts("")
    IO.puts("  DB file: #{fmt_bytes(info.storage_bytes)}")
  end

  # ── Phase 3: Query Latency ────────────────────────────────────────

  defp phase3_queries(start_ts, now, devices, labels_for) do
    header("Phase 3: Query Latency")

    iterations = 100
    test_dev = div(devices, 2)
    test_labels = elem(labels_for, test_dev)

    queries = [
      {"raw (1h)",
       fn ->
         Timeless.query(:bench, "cpu_usage", test_labels, from: now - 3600, to: now)
       end},
      {"raw (24h)",
       fn ->
         Timeless.query(:bench, "cpu_usage", test_labels, from: now - 86_400, to: now)
       end},
      {"agg (1h, 60s buckets)",
       fn ->
         Timeless.query_aggregate(:bench, "cpu_usage", test_labels,
           from: now - 3600,
           to: now,
           bucket: {60, :seconds},
           aggregate: :avg
         )
       end},
      {"agg (24h, 5m buckets)",
       fn ->
         Timeless.query_aggregate(:bench, "cpu_usage", test_labels,
           from: now - 86_400,
           to: now,
           bucket: {300, :seconds},
           aggregate: :avg
         )
       end},
      {"multi (#{devices} hosts, 1h)",
       fn ->
         Timeless.query_aggregate_multi(:bench, "cpu_usage", %{},
           from: now - 3600,
           to: now,
           bucket: {60, :seconds},
           aggregate: :avg
         )
       end},
      {"latest value",
       fn ->
         Timeless.latest(:bench, "cpu_usage", test_labels)
       end},
      {"tier hourly (7d)",
       fn ->
         Timeless.query_tier(:bench, :hourly, "cpu_usage", test_labels,
           from: now - 7 * 86_400,
           to: now
         )
       end},
      {"tier daily (90d)",
       fn ->
         Timeless.query_tier(:bench, :daily, "cpu_usage", test_labels,
           from: start_ts,
           to: now
         )
       end}
    ]

    IO.puts("  #{iterations} iterations each")
    IO.puts("")

    IO.puts(
      "  #{String.pad_trailing("Query", 28)} #{String.pad_leading("Avg", 10)} #{String.pad_leading("P50", 10)} #{String.pad_leading("P99", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 60)}")

    Enum.each(queries, fn {name, query_fn} ->
      # Warmup
      query_fn.()
      query_fn.()

      times =
        for _ <- 1..iterations do
          {us, _} = :timer.tc(query_fn)
          us
        end

      sorted = Enum.sort(times)
      avg = Enum.sum(times) / iterations
      p50 = percentile(sorted, 0.50)
      p99 = percentile(sorted, 0.99)

      IO.puts(
        "  #{String.pad_trailing(name, 28)} #{String.pad_leading(fmt_us(avg), 10)} #{String.pad_leading(fmt_us(p50), 10)} #{String.pad_leading(fmt_us(p99), 10)}"
      )
    end)
  end

  # ── Phase 4: Realtime Write Throughput ─────────────────────────────

  defp phase4_realtime(devices, labels_for) do
    header("Phase 4: Realtime Write Throughput")

    # Single series write speed
    IO.puts("  Single series (100K writes):")
    labels = elem(labels_for, 0)
    n = 100_000

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          Timeless.write(:bench, "cpu_usage", labels, 42.0)
        end
      end)

    rate = trunc(n / (us / 1_000_000))
    IO.puts("    #{fmt_int(n)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} writes/sec]")

    # Single series, pre-resolved (no registry overhead)
    IO.puts("")
    IO.puts("  Single series, pre-resolved (100K writes):")
    sid = Timeless.resolve_series(:bench, "cpu_usage", labels)

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          Timeless.write_resolved(:bench, sid, 42.0)
        end
      end)

    rate = trunc(n / (us / 1_000_000))
    IO.puts("    #{fmt_int(n)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} writes/sec]")

    # Batch write speed (simulating one collection cycle)
    IO.puts("")
    IO.puts("  Batch write (#{devices} devices × #{length(@metrics)} metrics):")
    batch_iters = 10

    entries_per =
      for dev <- 0..(devices - 1), metric <- @metrics do
        {metric, elem(labels_for, dev), 42.0}
      end

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..batch_iters do
          Timeless.write_batch(:bench, entries_per)
        end
      end)

    total = batch_iters * length(entries_per)
    rate = trunc(total / (us / 1_000_000))
    IO.puts("    #{fmt_int(total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} writes/sec]")

    # Batch write, pre-resolved (no registry overhead)
    IO.puts("")
    IO.puts("  Batch write, pre-resolved (#{devices} devices × #{length(@metrics)} metrics):")

    resolved_entries_per =
      for dev <- 0..(devices - 1), metric <- @metrics do
        sid = Timeless.resolve_series(:bench, metric, elem(labels_for, dev))
        {sid, 42.0}
      end

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..batch_iters do
          Timeless.write_batch_resolved(:bench, resolved_entries_per)
        end
      end)

    total = batch_iters * length(resolved_entries_per)
    rate = trunc(total / (us / 1_000_000))
    IO.puts("    #{fmt_int(total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} writes/sec]")
  end

  # ── Data Generation ────────────────────────────────────────────────

  defp gen(metric, ts, dev_id, start_ts) do
    h = :erlang.phash2({ts, dev_id, metric})
    n = h / 4_294_967_295
    dh = :erlang.phash2(dev_id) / 4_294_967_295
    hour = rem(div(ts, 3600), 24)
    d = 0.5 + 0.5 * :math.sin((hour - 4) / 24 * 2 * :math.pi())
    elapsed = max(ts - start_ts, 0) * 1.0

    case metric do
      "cpu_usage" -> 25.0 + 45.0 * d + n * 15.0 + dh * 10.0
      "mem_usage" -> 45.0 + n * 30.0 + dh * 15.0
      "disk_usage" -> 35.0 + dh * 30.0 + elapsed / 86_400 * 0.2 + n * 3.0
      "load_avg_1m" -> 0.3 + 3.0 * d + n * 1.5 + dh * 0.5
      "if_in_octets" -> (300_000.0 + 700_000.0 * dh) * elapsed
      "if_out_octets" -> (100_000.0 + 300_000.0 * dh) * elapsed
      "if_in_errors" -> (2.0 + 8.0 * dh) * trunc(elapsed / 300)
      "if_out_errors" -> (1.0 + 4.0 * dh) * trunc(elapsed / 300)
      "if_in_packets" -> (1000.0 + 3000.0 * dh) * elapsed
      "if_out_packets" -> (500.0 + 1500.0 * dh) * elapsed
      "temperature_c" -> 28.0 + 12.0 * d + n * 3.0 + dh * 5.0
      "signal_power_dbm" -> -35.0 + dh * 15.0 + n * 3.0
      "signal_snr_db" -> 25.0 + dh * 15.0 + n * 4.0
      "ping_latency_ms" -> 5.0 + n * 40.0 + dh * 10.0
      "packet_loss_pct" -> n * n * 5.0
      "uptime_seconds" -> elapsed
      "dns_queries_total" -> (50.0 + 200.0 * dh) * elapsed
      "http_requests_total" -> (100.0 + 500.0 * dh) * elapsed
      "active_connections" -> 10.0 + 50.0 * d + n * 20.0 + dh * 30.0
      "bandwidth_util_pct" -> 10.0 + 60.0 * d + n * 15.0 + dh * 10.0
    end
  end

  # ── Arg Parsing ────────────────────────────────────────────────────

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [tier: :string, devices: :integer, days: :integer, segment_duration: :integer, data_dir: :string]
      )

    seg_dur = opts[:segment_duration] || 14_400
    data_dir = opts[:data_dir]

    {tier_name, devices, days} =
      case opts[:tier] do
        "medium" -> {:medium, 1_000, 90}
        "stress" -> {:stress, 10_000, 7}
        "fast-stress" -> {:"fast-stress", 5_000, 2}
        "quick" -> {:quick, 100, 90}
        nil ->
          devices = opts[:devices] || 100
          days = opts[:days] || 90
          {:custom, devices, days}
        _ -> {:quick, 100, 90}
      end

    {tier_name, devices, days, seg_dur, data_dir}
  end

  # ── Formatting ─────────────────────────────────────────────────────

  defp banner(tier, devices, metrics, days, series, total, seg_dur, data_dir) do
    fs_type = detect_fs_type(data_dir)
    IO.puts("")
    IO.puts(bar())
    IO.puts("  Timeless Benchmark — #{tier}")
    IO.puts(bar())
    IO.puts("  Devices:    #{fmt_int(devices)}")
    IO.puts("  Metrics:    #{metrics} per device")
    IO.puts("  Series:     #{fmt_int(series)}")
    IO.puts("  Duration:   #{days} days @ #{div(@interval, 60)}-min intervals")
    IO.puts("  Segments:   #{div(seg_dur, 3600)}h (#{div(seg_dur, @interval)} pts/seg)")
    IO.puts("  Points:     #{fmt_int(total)}")
    IO.puts("  CPU:        #{System.schedulers_online()} cores")
    IO.puts("  Storage:    #{data_dir} (#{fs_type})")
    IO.puts(bar())
  end

  defp detect_fs_type(path) do
    case System.cmd("df", ["--output=fstype", path], stderr_to_stdout: true) do
      {output, 0} ->
        output |> String.split("\n", trim: true) |> List.last() |> String.trim()
      _ -> "unknown"
    end
  end

  defp header(title) do
    IO.puts("")
    IO.puts(title)
  end

  defp bar, do: "  " <> String.duplicate("=", 60)

  defp fmt_int(n) when is_float(n), do: fmt_int(trunc(n))
  defp fmt_int(n) when n >= 1_000_000_000, do: "#{:erlang.float_to_binary(n / 1_000_000_000, decimals: 2)}B"
  defp fmt_int(n) when n >= 1_000_000, do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"
  defp fmt_int(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_int(n), do: Integer.to_string(n)

  defp fmt_bytes(n) when n >= 1_073_741_824, do: "#{:erlang.float_to_binary(n / 1_073_741_824, decimals: 1)} GB"
  defp fmt_bytes(n) when n >= 1_048_576, do: "#{:erlang.float_to_binary(n / 1_048_576, decimals: 1)} MB"
  defp fmt_bytes(n) when n >= 1_024, do: "#{:erlang.float_to_binary(n / 1_024, decimals: 1)} KB"
  defp fmt_bytes(n), do: "#{n} B"

  defp fmt_dur(us) when us >= 60_000_000, do: "#{:erlang.float_to_binary(us / 60_000_000, decimals: 1)}m"
  defp fmt_dur(us) when us >= 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 1)}s"
  defp fmt_dur(us) when us >= 1_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_dur(us), do: "#{us}us"

  defp fmt_us(us) when us >= 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
  defp fmt_us(us) when us >= 1_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 2)}ms"
  defp fmt_us(us), do: "#{:erlang.float_to_binary(us / 1, decimals: 0)}us"

  defp percentile(sorted, p) do
    idx = trunc(length(sorted) * p)
    idx = min(idx, length(sorted) - 1)
    Enum.at(sorted, idx)
  end
end
