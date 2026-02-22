# Comparison benchmark: TimelessMetrics vs VictoriaMetrics
#
# Usage: mix run bench/vs_victoriametrics.exs [--devices N] [--days N]
#
# Requires VictoriaMetrics running on localhost:8428
# Measures native API, HTTP ingest (apples-to-apples), storage, and query latency.

defmodule VsBench do
  @vm_url "http://localhost:8428"
  @tm_http_port 19_428
  @tm_http_url "http://localhost:#{@tm_http_port}"
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
    IO.puts("  CPU:      #{System.schedulers_online()} cores")
    IO.puts("  " <> String.duplicate("=", 64))

    # Verify VM is reachable
    case Req.get("#{@vm_url}/health") do
      {:ok, %{status: 200}} -> :ok
      _ -> IO.puts("  ERROR: VictoriaMetrics not reachable at #{@vm_url}"); System.halt(1)
    end

    now = System.os_time(:second)
    start_ts = div(now - days * 86_400, 14_400) * 14_400

    labels_for = 0..(devices - 1) |> Enum.map(&%{"host" => "dev_#{&1}"}) |> List.to_tuple()

    # --- Phase 1: Native API Ingest (for query data + reference speed) ---
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
        flush_threshold: 200_000,
        flush_interval: :timer.seconds(60)
      )

    IO.puts("\n  Phase 1: Native API Ingest (concurrent writers, with index lookup)")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("")

    writers = min(System.schedulers_online(), @intervals_per_day)
    total_write_us = :counters.new(1, [:atomics])
    total_flush_us = :counters.new(1, [:atomics])

    wall_start = System.monotonic_time(:microsecond)

    for day <- 0..(days - 1) do
      day_start = start_ts + day * 86_400

      {write_us, _} =
        :timer.tc(fn ->
          0..(@intervals_per_day - 1)
          |> Enum.chunk_every(max(div(@intervals_per_day, writers), 1))
          |> Enum.map(fn intervals ->
            Task.async(fn ->
              Enum.each(intervals, fn interval ->
                ts = day_start + interval * @interval

                entries =
                  for dev <- 0..(devices - 1), metric <- @metrics do
                    {metric, elem(labels_for, dev), gen(metric, ts, dev), ts}
                  end

                TimelessMetrics.write_batch(:vs_bench, entries)
              end)
            end)
          end)
          |> Task.await_many(:infinity)
        end)

      :counters.add(total_write_us, 1, write_us)

      {flush_us, _} = :timer.tc(fn -> TimelessMetrics.flush(:vs_bench) end)
      :counters.add(total_flush_us, 1, flush_us)

      if rem(day + 1, 10) == 0 do
        IO.write("\r    TimelessMetrics: day #{day + 1}/#{days}    ")
      end
    end

    wall_end = System.monotonic_time(:microsecond)
    wall_us = wall_end - wall_start
    write_us = :counters.get(total_write_us, 1)
    flush_us = :counters.get(total_flush_us, 1)

    write_rate = trunc(total_points / (write_us / 1_000_000))
    wall_rate = trunc(total_points / (wall_us / 1_000_000))

    IO.puts("\r    Write only:   #{fmt_dur(write_us)}  [#{fmt_int(write_rate)} pts/sec] (#{writers} writers)      ")
    IO.puts("    Flush+compress: #{fmt_dur(flush_us)}")
    IO.puts("    Wall total:   #{fmt_dur(wall_us)}  [#{fmt_int(wall_rate)} pts/sec effective]")

    # --- Phase 2: HTTP Ingest Comparison (apples-to-apples, sustained) ---
    IO.puts("\n  Phase 2: HTTP Ingest (apples-to-apples, verified)")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("  Measures send + wait until data is queryable (no buffering tricks)")
    IO.puts("")

    # Start TM HTTP server on separate port
    http_data_dir = "/tmp/timeless_vs_bench_http_#{System.os_time(:millisecond)}"
    File.mkdir_p!(http_data_dir)

    {:ok, _} =
      TimelessMetrics.Supervisor.start_link(
        name: :vs_http_bench,
        data_dir: http_data_dir,
        buffer_shards: System.schedulers_online(),
        segment_duration: 14_400,
        schema: schema,
        flush_threshold: 200_000,
        flush_interval: :timer.seconds(60)
      )

    {:ok, _http_pid} = Bandit.start_link(
      plug: {TimelessMetrics.HTTP, [store: :vs_http_bench]},
      port: @tm_http_port
    )

    # Verify TM HTTP is reachable
    case Req.get("#{@tm_http_url}/health") do
      {:ok, %{status: 200}} -> :ok
      other -> IO.puts("  ERROR: TM HTTP not reachable at #{@tm_http_url}: #{inspect(other)}"); System.halt(1)
    end

    tm_req = Req.new(base_url: @tm_http_url, connect_options: [timeout: 30_000], receive_timeout: 60_000)
    vm_req = Req.new(base_url: @vm_url, connect_options: [timeout: 30_000], receive_timeout: 60_000)

    # Pre-build all day payloads (exclude from timing — measures server, not client serialization)
    IO.write("    Building payloads...")
    day_bodies =
      for day <- 0..(days - 1) do
        day_start = start_ts + day * 86_400

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

        IO.iodata_to_binary(lines)
      end

    total_payload = Enum.reduce(day_bodies, 0, &(byte_size(&1) + &2))
    IO.puts(" done (#{length(day_bodies)} days, #{fmt_bytes(total_payload)} total)")
    IO.puts("")

    # --- 2a: TimelessMetrics HTTP (synchronous — data available immediately after 204) ---
    {tm_http_us, _} =
      :timer.tc(fn ->
        for {body, day} <- Enum.with_index(day_bodies, 1) do
          Req.post!(tm_req, url: "/api/v1/import/prometheus", body: body,
            headers: [{"content-type", "text/plain"}])

          if rem(day, 10) == 0 do
            IO.write("\r    TimelessMetrics HTTP: day #{day}/#{days}    ")
          end
        end
      end)

    # TM is synchronous — data is in buffer after response, verify via health endpoint
    %{status: 200, body: tm_health} = Req.get!(tm_req, url: "/health")
    tm_verified_points = tm_health["points"] + tm_health["buffer_points"]

    tm_http_rate = trunc(total_points / (tm_http_us / 1_000_000))
    IO.puts("\r    TimelessMetrics HTTP: #{fmt_dur(tm_http_us)}  [#{fmt_int(tm_http_rate)} pts/sec]  (#{fmt_int(tm_verified_points)} pts verified)    ")

    # --- 2b: VictoriaMetrics HTTP (may buffer — need to wait for data to land) ---
    vm_send_start = System.monotonic_time(:microsecond)

    for {body, day} <- Enum.with_index(day_bodies, 1) do
      Req.post!(vm_req, url: "/api/v1/import/prometheus", body: body,
        headers: [{"content-type", "text/plain"}])

      if rem(day, 10) == 0 do
        IO.write("\r    VictoriaMetrics HTTP:   day #{day}/#{days} (sending)    ")
      end
    end

    vm_send_done = System.monotonic_time(:microsecond)
    vm_send_us = vm_send_done - vm_send_start
    vm_send_rate = trunc(total_points / (vm_send_us / 1_000_000))
    IO.puts("\r    VictoriaMetrics send:   #{fmt_dur(vm_send_us)}  [#{fmt_int(vm_send_rate)} pts/sec] (HTTP accept only)    ")

    # Now wait for VM to actually process the data — poll until series count stabilizes
    IO.write("    Waiting for VM to process...")

    # Force flush
    Req.get(vm_req, url: "/internal/force_flush")

    vm_verified_points = wait_for_vm_stable(vm_req, series_count)
    vm_total_us = System.monotonic_time(:microsecond) - vm_send_start
    vm_real_rate = trunc(total_points / (vm_total_us / 1_000_000))

    IO.puts(" done")
    IO.puts("    VictoriaMetrics real:   #{fmt_dur(vm_total_us)}  [#{fmt_int(vm_real_rate)} pts/sec] (send + process + verify)")
    IO.puts("      VM series confirmed:  #{vm_verified_points}")

    IO.puts("")
    ratio = if tm_http_rate > 0, do: Float.round(vm_real_rate / tm_http_rate, 2), else: 0.0
    winner = if tm_http_rate > vm_real_rate, do: "TM wins", else: "VM wins"
    IO.puts("    Apples-to-apples ratio: #{ratio}x (#{winner})")

    # Flush TM HTTP store
    TimelessMetrics.flush(:vs_http_bench)

    # --- Phase 3: Storage comparison ---
    IO.puts("\n  Phase 3: Storage")
    IO.puts("  " <> String.duplicate("-", 64))

    timeless_info = TimelessMetrics.info(:vs_bench)
    IO.puts("    TimelessMetrics (native):")
    IO.puts("      Segments:   #{fmt_int(timeless_info.segment_count)}")
    IO.puts("      Points:     #{fmt_int(timeless_info.total_points)}")
    IO.puts("      Compressed: #{fmt_bytes(timeless_info.raw_compressed_bytes)}")
    IO.puts("      Bytes/pt:   #{timeless_info.bytes_per_point}")
    IO.puts("      DB file:    #{fmt_bytes(timeless_info.storage_bytes)}")

    http_info = TimelessMetrics.info(:vs_http_bench)
    IO.puts("    TimelessMetrics (HTTP):")
    IO.puts("      Segments:   #{fmt_int(http_info.segment_count)}")
    IO.puts("      Points:     #{fmt_int(http_info.total_points)}")
    IO.puts("      Compressed: #{fmt_bytes(http_info.raw_compressed_bytes)}")
    IO.puts("      Bytes/pt:   #{http_info.bytes_per_point}")
    IO.puts("      DB file:    #{fmt_bytes(http_info.storage_bytes)}")

    # VM storage via TSDB status API
    %{status: 200, body: vm_tsdb} = Req.get!(vm_req, url: "/api/v1/status/tsdb")
    vm_series = vm_tsdb["data"]["totalSeries"]
    IO.puts("    VictoriaMetrics:")
    IO.puts("      Series:     #{fmt_int(vm_series)}")

    case System.cmd("podman", ["exec", "victoriametrics", "du", "-sh", "/victoria-metrics-data"], stderr_to_stdout: true) do
      {output, 0} ->
        [size | _] = String.split(output)
        IO.puts("      Disk usage: #{size}")
      _ ->
        IO.puts("      (disk usage: check container filesystem)")
    end

    # --- Phase 4: Query comparison ---
    IO.puts("\n  Phase 4: Query Latency")
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

    # --- Summary ---
    IO.puts("\n  " <> String.duplicate("=", 64))
    IO.puts("  Ingest Summary")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("  #{String.pad_trailing("Method", 40)} #{String.pad_leading("pts/sec", 12)}")
    IO.puts("  " <> String.duplicate("-", 64))
    IO.puts("  #{String.pad_trailing("TM native write_batch (#{writers} writers)", 40)} #{String.pad_leading(fmt_int(write_rate), 12)}")
    IO.puts("  #{String.pad_trailing("TM native wall (write+flush+compress)", 40)} #{String.pad_leading(fmt_int(wall_rate), 12)}")
    IO.puts("  #{String.pad_trailing("TM HTTP (Prometheus, synchronous)", 40)} #{String.pad_leading(fmt_int(tm_http_rate), 12)}")
    IO.puts("  #{String.pad_trailing("VM HTTP (Prometheus, accept only)", 40)} #{String.pad_leading(fmt_int(vm_send_rate), 12)}")
    IO.puts("  #{String.pad_trailing("VM HTTP (Prometheus, sustained/verified)", 40)} #{String.pad_leading(fmt_int(vm_real_rate), 12)}")
    IO.puts("  " <> String.duplicate("-", 64))
    tm_vs_vm = if vm_real_rate > 0, do: Float.round(write_rate / vm_real_rate, 2), else: 0.0
    IO.puts("  TM native vs VM HTTP sustained: #{tm_vs_vm}x")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Cleanup:")
    IO.puts("    rm -rf #{data_dir} #{http_data_dir}")
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

  # Poll VM's TSDB status until totalSeries reaches expected count or stabilizes
  defp wait_for_vm_stable(vm_req, expected_series) do
    wait_for_vm_stable(vm_req, expected_series, 0, 0, 60)
  end

  defp wait_for_vm_stable(_vm_req, _expected, last_count, stable_checks, 0) do
    IO.write(" (timeout, stable_checks=#{stable_checks})")
    last_count
  end

  defp wait_for_vm_stable(vm_req, expected_series, last_count, stable_checks, retries) do
    Process.sleep(500)

    case Req.get(vm_req, url: "/api/v1/status/tsdb") do
      {:ok, %{status: 200, body: %{"data" => %{"totalSeries" => count}}}} ->
        cond do
          count >= expected_series ->
            # All series present
            count

          count == last_count and stable_checks >= 3 ->
            # Series count stopped growing — VM is done
            IO.write(" (stabilized at #{count}/#{expected_series} series)")
            count

          count == last_count ->
            wait_for_vm_stable(vm_req, expected_series, count, stable_checks + 1, retries - 1)

          true ->
            IO.write(".")
            wait_for_vm_stable(vm_req, expected_series, count, 0, retries - 1)
        end

      _ ->
        wait_for_vm_stable(vm_req, expected_series, last_count, stable_checks, retries - 1)
    end
  end
end

VsBench.run()
