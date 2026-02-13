# Comparison benchmark: TimelessMetrics vs VictoriaMetrics
#
# Usage: mix run bench/vs_victoriametrics.exs [--devices N] [--days N]
#
# Requires VictoriaMetrics running on localhost:8428
# Note: TimelessMetrics uses native Elixir API (in-process), VM uses HTTP.
# Query comparison is apples-to-apples (both measured end-to-end).
# Ingest comparison favors TimelessMetrics (native vs HTTP) — noted in output.

defmodule VsBench do
  @vm_url "http://localhost:8428"
  @metrics ~w(cpu_usage mem_usage disk_usage load_avg if_in_octets if_out_octets
              temperature signal_power ping_latency bandwidth_util)
  @interval 300
  @intervals_per_day 288

  def run do
    {opts, _, _} =
      OptionParser.parse(System.argv(), switches: [devices: :integer, days: :integer])

    devices = opts[:devices] || 50
    days = opts[:days] || 30
    metrics_count = length(@metrics)
    series_count = devices * metrics_count
    total_points = series_count * days * @intervals_per_day

    IO.puts("\n  " <> String.duplicate("=", 64))
    IO.puts("  TimelessMetrics vs VictoriaMetrics")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Devices:  #{devices}")
    IO.puts("  Metrics:  #{metrics_count} per device")
    IO.puts("  Series:   #{series_count}")
    IO.puts("  Days:     #{days}")
    IO.puts("  Points:   #{fmt_int(total_points)}")
    IO.puts("  " <> String.duplicate("=", 64))

    # Verify VM is reachable
    case Req.get("#{@vm_url}/health") do
      {:ok, %{status: 200}} -> :ok
      _ -> IO.puts("  ERROR: VictoriaMetrics not reachable at #{@vm_url}"); System.halt(1)
    end

    now = System.os_time(:second)
    start_ts = div(now - days * 86_400, 14_400) * 14_400

    labels_for = 0..(devices - 1) |> Enum.map(&%{"host" => "dev_#{&1}"}) |> List.to_tuple()

    # --- Phase 1: Ingest into TimelessMetrics ---
    data_dir = "/tmp/timeless_vs_bench_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    schema = %TimelessMetrics.Schema{
      raw_retention_seconds: (days + 7) * 86_400,
      rollup_interval: :timer.hours(999),
      retention_interval: :timer.hours(999),
      tiers: TimelessMetrics.Schema.default().tiers
    }

    {:ok, _} =
      TimelessMetrics.Supervisor.start_link(
        name: :vs_bench,
        data_dir: data_dir,
        buffer_shards: System.schedulers_online(),
        segment_duration: 14_400,
        schema: schema,
        flush_threshold: 50_000,
        flush_interval: :timer.seconds(60)
      )

    IO.puts("\n  Phase 1: Ingest")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("  (TimelessMetrics = native API, VM = HTTP import — not apples-to-apples)")
    IO.puts("")

    {timeless_us, _} =
      :timer.tc(fn ->
        for day <- 0..(days - 1) do
          day_start = start_ts + day * 86_400

          for interval <- 0..(@intervals_per_day - 1) do
            ts = day_start + interval * @interval

            entries =
              for dev <- 0..(devices - 1), metric <- @metrics do
                {metric, elem(labels_for, dev), gen(metric, ts, dev), ts}
              end

            TimelessMetrics.write_batch(:vs_bench, entries)
          end

          TimelessMetrics.flush(:vs_bench)

          if rem(day + 1, 10) == 0 do
            IO.write("\r    TimelessMetrics: day #{day + 1}/#{days}    ")
          end
        end
      end)

    timeless_rate = trunc(total_points / (timeless_us / 1_000_000))
    IO.puts("\r    TimelessMetrics:          #{fmt_dur(timeless_us)}  [#{fmt_int(timeless_rate)} pts/sec]    ")

    # --- Phase 1b: Ingest into VictoriaMetrics ---
    # Use Prometheus text exposition format via HTTP
    # Build a persistent connection for reuse
    vm_req = Req.new(base_url: @vm_url, connect_options: [timeout: 30_000], receive_timeout: 60_000)

    {vm_us, _} =
      :timer.tc(fn ->
        for day <- 0..(days - 1) do
          day_start = start_ts + day * 86_400

          # Build all points for one day in Prometheus text format
          lines =
            for interval <- 0..(@intervals_per_day - 1) do
              ts = day_start + interval * @interval
              ts_ms = ts * 1000

              for dev <- 0..(devices - 1), metric <- @metrics do
                val = gen(metric, ts, dev)
                host = "dev_#{dev}"
                [metric, ~c'{host="', host, ~c'"} ', Float.to_string(val), ?\s, Integer.to_string(ts_ms), ?\n]
              end
            end

          body = IO.iodata_to_binary(lines)

          Req.post!(vm_req, url: "/api/v1/import/prometheus", body: body,
            headers: [{"content-type", "text/plain"}])

          if rem(day + 1, 10) == 0 do
            IO.write("\r    VictoriaMetrics:   day #{day + 1}/#{days}    ")
          end
        end
      end)

    vm_rate = trunc(total_points / (vm_us / 1_000_000))
    IO.puts("\r    VictoriaMetrics:   #{fmt_dur(vm_us)}  [#{fmt_int(vm_rate)} pts/sec]    ")

    # Let VM flush to disk
    Process.sleep(3_000)

    # --- Phase 2: Storage comparison ---
    IO.puts("\n  Phase 2: Storage")
    IO.puts("  " <> String.duplicate("-", 64))

    timeless_info = TimelessMetrics.info(:vs_bench)
    IO.puts("    TimelessMetrics:")
    IO.puts("      Segments:   #{fmt_int(timeless_info.segment_count)}")
    IO.puts("      Points:     #{fmt_int(timeless_info.total_points)}")
    IO.puts("      Compressed: #{fmt_bytes(timeless_info.raw_compressed_bytes)}")
    IO.puts("      Bytes/pt:   #{timeless_info.bytes_per_point}")
    IO.puts("      DB file:    #{fmt_bytes(timeless_info.storage_bytes)}")

    # VM storage via TSDB status API
    %{status: 200, body: vm_tsdb} = Req.get!(vm_req, url: "/api/v1/status/tsdb")
    vm_series = vm_tsdb["data"]["totalSeries"]
    IO.puts("    VictoriaMetrics:")
    IO.puts("      Series:     #{fmt_int(vm_series)}")

    # Try to get VM data dir size
    case System.cmd("podman", ["exec", "victoriametrics", "du", "-sh", "/victoria-metrics-data"], stderr_to_stdout: true) do
      {output, 0} ->
        [size | _] = String.split(output)
        IO.puts("      Disk usage: #{size}")
      _ ->
        IO.puts("      (disk usage: check container filesystem)")
    end

    # --- Phase 3: Query comparison ---
    IO.puts("\n  Phase 3: Query Latency")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("  (Both measured end-to-end: TimelessMetrics = native, VM = HTTP)")
    IO.puts("")

    query_from = now - 3600
    query_to = now
    iterations = 50
    test_labels = elem(labels_for, div(devices, 2))
    test_host = "dev_#{div(devices, 2)}"

    q_single = "cpu_usage{host=\"#{test_host}\"}"
    q_agg_single = "avg_over_time(cpu_usage{host=\"#{test_host}\"}[60s])"
    q_agg_multi = "avg_over_time(cpu_usage[60s])"

    queries = [
      {"raw 1h (single)", fn ->
        TimelessMetrics.query(:vs_bench, "cpu_usage", test_labels, from: query_from, to: query_to)
      end, fn ->
        Req.get!(vm_req, url: "/api/v1/query_range",
          params: [query: q_single, start: query_from, end: query_to, step: 300])
      end},
      {"raw 24h (single)", fn ->
        TimelessMetrics.query(:vs_bench, "cpu_usage", test_labels, from: now - 86_400, to: now)
      end, fn ->
        Req.get!(vm_req, url: "/api/v1/query_range",
          params: [query: q_single, start: now - 86_400, end: now, step: 300])
      end},
      {"agg 1h avg (single)", fn ->
        TimelessMetrics.query_aggregate(:vs_bench, "cpu_usage", test_labels,
          from: query_from, to: query_to, bucket: {60, :seconds}, aggregate: :avg)
      end, fn ->
        Req.get!(vm_req, url: "/api/v1/query_range",
          params: [query: q_agg_single, start: query_from, end: query_to, step: 60])
      end},
      {"multi #{devices} hosts 1h", fn ->
        TimelessMetrics.query_aggregate_multi(:vs_bench, "cpu_usage", %{},
          from: query_from, to: query_to, bucket: {60, :seconds}, aggregate: :avg)
      end, fn ->
        Req.get!(vm_req, url: "/api/v1/query_range",
          params: [query: q_agg_multi, start: query_from, end: query_to, step: 60])
      end},
      {"latest value", fn ->
        TimelessMetrics.latest(:vs_bench, "cpu_usage", test_labels)
      end, fn ->
        Req.get!(vm_req, url: "/api/v1/query",
          params: [query: q_single])
      end}
    ]

    IO.puts("  #{iterations} iterations each\n")
    IO.puts(
      "  #{String.pad_trailing("Query", 28)} #{String.pad_leading("TimelessMetrics", 12)} #{String.pad_leading("VM", 12)} #{String.pad_leading("Ratio", 8)}"
    )
    IO.puts("  " <> String.duplicate("-", 64))

    Enum.each(queries, fn {name, timeless_fn, vm_fn} ->
      # Warmup
      timeless_fn.()
      vm_fn.()

      t_times = for _ <- 1..iterations do
        {us, _} = :timer.tc(timeless_fn)
        us
      end

      v_times = for _ <- 1..iterations do
        {us, _} = :timer.tc(vm_fn)
        us
      end

      t_avg = Enum.sum(t_times) / iterations
      v_avg = Enum.sum(v_times) / iterations
      ratio = if t_avg > 0, do: Float.round(v_avg / t_avg, 1), else: 0.0

      faster = if ratio > 1, do: "T wins", else: "VM wins"

      IO.puts(
        "  #{String.pad_trailing(name, 28)} #{String.pad_leading(fmt_us(t_avg), 12)} #{String.pad_leading(fmt_us(v_avg), 12)} #{String.pad_leading("#{ratio}x", 8)}  #{faster}"
      )
    end)

    # --- Footer ---
    IO.puts("\n  " <> String.duplicate("=", 64))
    IO.puts("  Notes:")
    IO.puts("    - Ingest: TimelessMetrics native API vs VM HTTP (not comparable)")
    IO.puts("    - Queries: TimelessMetrics native vs VM HTTP (VM includes network overhead)")
    IO.puts("    - VM query ratio >1x means TimelessMetrics is faster")
    IO.puts("  Cleanup:")
    IO.puts("    rm -rf #{data_dir}")
    IO.puts("    curl -s 'http://localhost:8428/api/v1/admin/tsdb/delete_series?match[]={__name__=~\".*\"}' > /dev/null")
    IO.puts("  " <> String.duplicate("=", 64))
  end

  # --- Data gen ---
  defp gen(metric, ts, dev_id) do
    h = :erlang.phash2({ts, dev_id, metric})
    n = h / 4_294_967_295
    dh = :erlang.phash2(dev_id) / 4_294_967_295
    hour = rem(div(ts, 3600), 24)
    d = 0.5 + 0.5 * :math.sin((hour - 4) / 24 * 2 * :math.pi())

    case metric do
      "cpu_usage" -> Float.round(25.0 + 45.0 * d + n * 15.0 + dh * 10.0, 2)
      "mem_usage" -> Float.round(45.0 + n * 30.0 + dh * 15.0, 2)
      "disk_usage" -> Float.round(35.0 + dh * 30.0 + n * 3.0, 2)
      "load_avg" -> Float.round(0.3 + 3.0 * d + n * 1.5 + dh * 0.5, 2)
      "if_in_octets" -> Float.round((300_000.0 + 700_000.0 * dh) * n * 1000, 2)
      "if_out_octets" -> Float.round((100_000.0 + 300_000.0 * dh) * n * 1000, 2)
      "temperature" -> Float.round(28.0 + 12.0 * d + n * 3.0 + dh * 5.0, 2)
      "signal_power" -> Float.round(-35.0 + dh * 15.0 + n * 3.0, 2)
      "ping_latency" -> Float.round(5.0 + n * 40.0 + dh * 10.0, 2)
      "bandwidth_util" -> Float.round(10.0 + 60.0 * d + n * 15.0 + dh * 10.0, 2)
    end
  end

  # --- Formatting ---
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
end

VsBench.run()
