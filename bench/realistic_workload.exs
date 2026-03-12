# Realistic workload benchmark with auto-ramp: TimelessMetrics vs VictoriaMetrics
#
# Tests each target SEQUENTIALLY (full client capacity per target) then prints
# side-by-side results. Auto-ramps by halving the scrape interval each step until
# saturation. Queries ramp alongside writes.
#
# Pure HTTP client — can run from a separate node for fairest results.
# NOTE: When TM runs in the same BEAM as this script, TM's server competes with
# the client for scheduler time. For fairest results, run against a remote TM.
#
# Usage:
#   mix run bench/realistic_workload.exs \
#     --tm-url http://127.0.0.1:8428 \
#     --vm-url http://127.0.0.1:9428 \
#     --devices 500 --metrics 20 --step-seconds 20 --query-workers 20
#
# Firehose mode (500K series, aggressive ramp):
#   mix run bench/realistic_workload.exs \
#     --devices 25000 --metrics 20 --batch 50 \
#     --start-interval 0.5 --ramp-factor 4 --warmup 5
#
# All flags optional. --vm-url "" to skip VictoriaMetrics.
# --steps "5,2,1,0.5" overrides auto-ramp with fixed steps.
# --batch N groups N devices per HTTP POST (reduces client overhead at high cardinality)
# --start-interval S start at S seconds instead of 4s (default)
# --ramp-factor N divide interval by N each step instead of 2 (default)

defmodule RealisticWorkload do
  @metric_pool ~w(
    node_cpu_seconds_total
    node_memory_MemAvailable_bytes
    node_memory_MemTotal_bytes
    node_memory_Buffers_bytes
    node_memory_Cached_bytes
    node_filesystem_avail_bytes
    node_filesystem_size_bytes
    node_disk_read_bytes_total
    node_disk_written_bytes_total
    node_disk_io_time_seconds_total
    node_network_receive_bytes_total
    node_network_transmit_bytes_total
    node_network_receive_errs_total
    node_network_transmit_errs_total
    node_load1
    node_load5
    node_load15
    node_time_seconds
    node_boot_time_seconds
    node_context_switches_total
    node_entropy_avail_bits
    node_filefd_allocated
    node_forks_total
    node_intr_total
    node_procs_running
    node_procs_blocked
    node_scrape_collector_duration_seconds
    node_scrape_collector_success
    node_nf_conntrack_entries
    node_vmstat_pgfault
  )

  @query_templates [
    {:range, "node_cpu_seconds_total", 3600, 60},
    {:range, "node_memory_MemAvailable_bytes", 3600, 60},
    {:range, "node_load1", 3600, 60},
    {:range, "node_network_receive_bytes_total", 3600, 60},
    {:range, "node_cpu_seconds_total", 300, 15},
    {:range, "node_memory_MemAvailable_bytes", 300, 15},
    {:range, "node_filesystem_avail_bytes", 300, 15},
    {:instant, "node_load1", 0, 0},
    {:instant, "node_cpu_seconds_total", 0, 0},
    {:instant, "node_memory_MemAvailable_bytes", 0, 0}
  ]

  # Saturation: stop when any of these trip
  @p99_ceiling_us 100_000
  @error_rate_ceil 0.05
  @throughput_floor 0.60
  @min_interval_ms 1

  def run do
    {opts, _, _} =
      OptionParser.parse(System.argv(),
        switches: [
          tm_url: :string,
          vm_url: :string,
          devices: :integer,
          metrics: :integer,
          step_seconds: :integer,
          steps: :string,
          query_workers: :integer,
          start_interval: :float,
          ramp_factor: :integer,
          warmup: :integer,
          batch: :integer
        ]
      )

    tm_url = opts[:tm_url] || "http://127.0.0.1:8428"
    vm_url = opts[:vm_url]
    vm_url = if vm_url == nil, do: "http://127.0.0.1:9428", else: vm_url
    skip_vm = vm_url == ""
    devices = opts[:devices] || 500
    metrics_count = opts[:metrics] || 20
    step_dur = opts[:step_seconds] || 20
    query_workers = opts[:query_workers] || 20
    settle_s = 5
    warmup_s = opts[:warmup] || 10
    start_interval = opts[:start_interval] || 4.0
    ramp_factor = opts[:ramp_factor] || 2
    batch_size = opts[:batch] || 1

    fixed_steps =
      case opts[:steps] do
        nil -> nil
        s ->
          s |> String.split(",") |> Enum.map(fn p ->
            {f, ""} = Float.parse(String.trim(p))
            f
          end)
      end

    metrics = Enum.take(@metric_pool, metrics_count)
    series_count = devices * metrics_count
    device_structs = pre_generate_devices(devices, metrics)

    cfg = %{
      devices: devices,
      metrics_count: metrics_count,
      step_dur: step_dur,
      settle_s: settle_s,
      warmup_s: warmup_s,
      query_workers: query_workers,
      fixed_steps: fixed_steps,
      start_interval: start_interval,
      ramp_factor: ramp_factor,
      batch_size: batch_size
    }

    # --- Header ---
    IO.puts("")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Realistic Workload Benchmark — Sequential Load Ramp")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Devices:     #{fmt_int(devices)}")
    IO.puts("  Metrics:     #{metrics_count} per device (#{fmt_int(series_count)} series)")

    if fixed_steps do
      IO.puts("  Steps:       #{fixed_steps |> Enum.map(&format_interval/1) |> Enum.join(" -> ")} (fixed)")
    else
      IO.puts("  Ramp:        auto (÷#{ramp_factor} from #{format_interval(start_interval)} until saturation)")
    end

    IO.puts("  Step dur:    #{step_dur}s (#{settle_s}s settle, #{warmup_s}s warmup)")
    IO.puts("  Batch:       #{batch_size} device#{if batch_size > 1, do: "s", else: ""}/POST")
    IO.puts("  Queries:     #{query_workers} workers (ramped with writes)")
    IO.puts("  Mode:        sequential (full client capacity per target)")
    IO.puts("  TM:          #{tm_url}")
    IO.puts("  VM:          #{if skip_vm, do: "(skipped)", else: vm_url}")
    IO.puts("  " <> String.duplicate("=", 64))

    # --- Verify targets ---
    verify_target("TM", tm_url)
    unless skip_vm, do: verify_target("VM", vm_url)

    # --- Run targets sequentially ---
    IO.puts("\n  === TimelessMetrics ===")
    tm_results = run_target(tm_url, device_structs, cfg)

    vm_results =
      if skip_vm do
        nil
      else
        IO.puts("\n  === VictoriaMetrics ===")
        run_target(vm_url, device_structs, cfg)
      end

    # --- Print comparison ---
    IO.puts("")
    print_comparison(tm_results, vm_results, skip_vm)
    print_summary(tm_url, series_count, tm_results, vm_results, skip_vm)
  end

  # --- Run one target to saturation ---

  defp run_target(url, device_structs, cfg) do
    client = build_client(url)

    write_ets = :ets.new(:write, [:ordered_set, :public, {:write_concurrency, true}])
    query_ets = :ets.new(:query, [:ordered_set, :public, {:write_concurrency, true}])
    write_ctr = :counters.new(3, [:atomics])   # [1]=reqs [2]=errs [3]=pts
    query_ctr = :counters.new(2, [:atomics])   # [1]=queries [2]=errs
    write_id = :atomics.new(1, [])
    query_id = :atomics.new(1, [])
    stop = :atomics.new(1, [])
    interval_ms = :atomics.new(1, [])

    initial_ms =
      if cfg.fixed_steps, do: trunc(hd(cfg.fixed_steps) * 1000), else: trunc(cfg.start_interval * 1000)

    :atomics.put(interval_ms, 1, initial_ms)

    ctx = %{
      client: client,
      write_ets: write_ets,
      query_ets: query_ets,
      write_ctr: write_ctr,
      query_ctr: query_ctr,
      write_id: write_id,
      query_id: query_id,
      stop: stop,
      interval_ms: interval_ms,
      devices: cfg.devices,
      metrics_count: cfg.metrics_count,
      step_dur: cfg.step_dur,
      settle_s: cfg.settle_s,
      ramp_factor: cfg.ramp_factor,
      batch_size: cfg.batch_size
    }

    # Spawn writers — batch devices into writer groups for high cardinality
    writer_groups = Enum.chunk_every(device_structs, cfg.batch_size)

    writer_tasks =
      for group <- writer_groups do
        Task.async(fn ->
          batch_writer_loop(group, cfg.metrics_count, client, stop, interval_ms,
            write_ets, write_id, write_ctr)
        end)
      end

    # Spawn query workers
    q_tasks =
      for _i <- 1..cfg.query_workers do
        Task.async(fn ->
          query_loop(device_structs, client, stop, interval_ms,
            query_ets, query_id, query_ctr)
        end)
      end

    # Warmup
    IO.puts("  Warming up (#{cfg.warmup_s}s)...")
    Process.sleep(cfg.warmup_s * 1_000)

    # Run steps
    step_results =
      if cfg.fixed_steps do
        run_fixed(cfg.fixed_steps, ctx)
      else
        run_auto(initial_ms, ctx)
      end

    # Stop
    :atomics.put(stop, 1, 1)
    IO.puts("  Stopping...")
    Task.await_many(writer_tasks ++ q_tasks, 30_000)

    :ets.delete(write_ets)
    :ets.delete(query_ets)

    step_results
  end

  defp run_fixed(steps, ctx) do
    for {step_s, idx} <- Enum.with_index(steps, 1) do
      IO.write("  Step #{idx}/#{length(steps)}: ")
      run_one_step(trunc(step_s * 1000), ctx)
    end
  end

  defp run_auto(start_ms, ctx), do: do_auto(start_ms, ctx, 1, [])

  defp do_auto(int_ms, ctx, n, acc) do
    if int_ms < @min_interval_ms do
      IO.puts("  Auto-ramp stopped: minimum interval")
      acc
    else
      IO.write("  Step #{n}: ")
      result = run_one_step(int_ms, ctx)

      case check_saturation(result, ctx.devices) do
        :ok ->
          next_ms = max(div(int_ms, ctx.ramp_factor), 1)
          do_auto(next_ms, ctx, n + 1, acc ++ [result])

        {:saturated, reason} ->
          IO.puts("  >> Saturated: #{reason}")
          acc ++ [result]
      end
    end
  end

  defp run_one_step(int_ms, ctx) do
    int_s = int_ms / 1000
    writer_count = ceil(ctx.devices / ctx.batch_size)
    target_rps = trunc(writer_count / int_s)
    target_pts = target_rps * ctx.metrics_count * ctx.batch_size

    IO.write("#{format_interval(int_s)} (~#{fmt_int(target_pts)} pts/s) ...")

    :atomics.put(ctx.interval_ms, 1, int_ms)
    Process.sleep(ctx.settle_s * 1_000)

    # Reset ALL stats for this step (writes + queries)
    :ets.delete_all_objects(ctx.write_ets)
    :ets.delete_all_objects(ctx.query_ets)
    :atomics.put(ctx.write_id, 1, 0)
    :atomics.put(ctx.query_id, 1, 0)
    reset_counters(ctx.write_ctr, 3)
    reset_counters(ctx.query_ctr, 2)

    t0 = System.monotonic_time(:microsecond)
    Process.sleep(ctx.step_dur * 1_000)
    elapsed_s = (System.monotonic_time(:microsecond) - t0) / 1_000_000

    w_reqs = :counters.get(ctx.write_ctr, 1)
    w_errs = :counters.get(ctx.write_ctr, 2)
    w_pts = :counters.get(ctx.write_ctr, 3)
    q_count = :counters.get(ctx.query_ctr, 1)
    q_errs = :counters.get(ctx.query_ctr, 2)

    result = %{
      interval_ms: int_ms,
      interval_s: int_s,
      elapsed_s: elapsed_s,
      w_reqs: w_reqs,
      w_errs: w_errs,
      w_pts: w_pts,
      w_pts_target: target_pts,
      w_lats: ets_values(ctx.write_ets),
      q_count: q_count,
      q_errs: q_errs,
      q_lats: ets_values(ctx.query_ets)
    }

    actual_rps = trunc(w_reqs / elapsed_s)
    actual_pts = trunc(w_pts / elapsed_s)
    actual_qps = trunc(q_count / elapsed_s)
    IO.puts(" #{fmt_int(actual_rps)} req/s, #{fmt_int(actual_pts)} pts/s, #{fmt_int(actual_qps)} qps")

    result
  end

  defp check_saturation(r, _devices) do
    target_pts = r.w_pts_target
    actual_pts = r.w_pts / r.elapsed_s
    ratio = actual_pts / max(target_pts, 1)

    w_p99 = percentile(r.w_lats, 99)
    err_rate = if r.w_reqs > 0, do: r.w_errs / r.w_reqs, else: 0

    cond do
      w_p99 > @p99_ceiling_us ->
        {:saturated, "write p99 #{fmt_us(w_p99)} > #{fmt_us(@p99_ceiling_us)}"}

      err_rate > @error_rate_ceil ->
        {:saturated, "error rate #{Float.round(err_rate * 100, 1)}%"}

      ratio < @throughput_floor ->
        {:saturated, "throughput #{trunc(ratio * 100)}% of target (#{fmt_int(actual_pts)} vs #{fmt_int(target_pts)} pts/s)"}

      true ->
        :ok
    end
  end

  # --- Pre-generation ---

  defp pre_generate_devices(count, metrics) do
    for dev_id <- 0..(count - 1) do
      host = "device_#{String.pad_leading(Integer.to_string(dev_id), 6, "0")}"
      region = Enum.at(["us-east", "us-west", "eu-west", "ap-south"], rem(dev_id, 4))
      env = Enum.at(["prod", "staging"], rem(dev_id, 2))
      label_str = ~s(host="#{host}",region="#{region}",env="#{env}")
      metric_lines = for m <- metrics, do: {m, "#{m}{#{label_str}}"}
      %{id: dev_id, host: host, metric_lines: metric_lines}
    end
  end

  # --- Writer loop (batched — one task handles N devices) ---

  defp batch_writer_loop(devs, mc, client, stop, int_ms, ets, wid, ctr) do
    Process.sleep(:rand.uniform(:atomics.get(int_ms, 1)))
    do_batch_write(devs, mc, client, stop, int_ms, ets, wid, ctr)
  end

  defp do_batch_write(devs, mc, client, stop, int_ms, ets, wid, ctr) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      ts_ms = System.os_time(:millisecond)
      total_mc = mc * length(devs)

      payload =
        devs
        |> Enum.map(&build_payload(&1, ts_ms))
        |> IO.iodata_to_binary()

      post_write(client, payload, ets, wid, ctr, total_mc)

      base = :atomics.get(int_ms, 1)
      jitter = max(trunc(base * 0.2), 1)
      sleep = max(base - jitter + :rand.uniform(jitter * 2), 1)
      Process.sleep(sleep)

      do_batch_write(devs, mc, client, stop, int_ms, ets, wid, ctr)
    end
  end

  defp build_payload(dev, ts_ms) do
    for {metric, labeled} <- dev.metric_lines do
      val = gen_value(metric, ts_ms, dev.id)
      [labeled, ?\s, Float.to_string(val), ?\s, Integer.to_string(ts_ms), ?\n]
    end
  end

  defp post_write(client, payload, ets, wid, ctr, mc) do
    {us, result} =
      :timer.tc(fn ->
        Req.post(client, url: "/api/v1/import/prometheus", body: payload,
          headers: [{"content-type", "text/plain"}])
      end)

    id = :atomics.add_get(wid, 1, 1)
    :ets.insert(ets, {id, us})
    :counters.add(ctr, 1, 1)
    :counters.add(ctr, 3, mc)

    case result do
      {:ok, %{status: s}} when s in 200..299 -> :ok
      _ -> :counters.add(ctr, 2, 1)
    end
  end

  # --- Query loop (aggressive ramp, paced to write interval / 5) ---

  defp query_loop(devices, client, stop, interval_ms, ets, qid, ctr) do
    do_query(devices, client, stop, interval_ms, ets, qid, ctr)
  end

  defp do_query(devices, client, stop, interval_ms, ets, qid, ctr) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      {type, metric, range_s, step} = Enum.random(@query_templates)
      dev = Enum.random(devices)
      now = System.os_time(:second)

      run_query(type, metric, dev, now, range_s, step, client, ets, qid, ctr)

      # Aggressive pacing: write_interval / 5, floor 5ms
      write_int = :atomics.get(interval_ms, 1)
      q_sleep = max(div(write_int, 5), 5) + :rand.uniform(5)
      Process.sleep(q_sleep)

      do_query(devices, client, stop, interval_ms, ets, qid, ctr)
    end
  end

  defp run_query(type, metric, dev, now, range_s, step, client, ets, qid, ctr) do
    query = ~s(#{metric}{host="#{dev.host}"})

    {us, result} =
      case type do
        :instant ->
          :timer.tc(fn -> Req.get(client, url: "/api/v1/query", params: [query: query]) end)

        :range ->
          :timer.tc(fn ->
            Req.get(client, url: "/api/v1/query_range",
              params: [query: query, start: now - range_s, end: now, step: step])
          end)
      end

    id = :atomics.add_get(qid, 1, 1)
    :ets.insert(ets, {id, us})
    :counters.add(ctr, 1, 1)

    case result do
      {:ok, %{status: s}} when s in 200..299 -> :ok
      _ -> :counters.add(ctr, 2, 1)
    end
  end

  # --- Data generation ---

  defp gen_value(metric, ts_ms, dev_id) do
    h = :erlang.phash2({ts_ms, dev_id, metric})
    n = h / 4_294_967_295
    dh = :erlang.phash2(dev_id) / 4_294_967_295

    case metric do
      "node_cpu_seconds_total" -> Float.round(1000.0 + ts_ms / 100_000 + dh * 500.0 + n * 10.0, 2)
      "node_memory_MemAvailable_bytes" -> Float.round(2.0e9 + n * 6.0e9 + dh * 2.0e9, 0)
      "node_memory_MemTotal_bytes" -> Float.round(8.0e9 + dh * 8.0e9, 0)
      "node_memory_Buffers_bytes" -> Float.round(1.0e8 + n * 5.0e8, 0)
      "node_memory_Cached_bytes" -> Float.round(5.0e8 + n * 2.0e9, 0)
      "node_filesystem_avail_bytes" -> Float.round(5.0e10 + n * 2.0e11 + dh * 1.0e11, 0)
      "node_filesystem_size_bytes" -> Float.round(5.0e11 + dh * 5.0e11, 0)
      "node_disk_read_bytes_total" -> Float.round(1.0e9 + ts_ms / 1_000 * 1000 + n * 1.0e5, 0)
      "node_disk_written_bytes_total" -> Float.round(2.0e9 + ts_ms / 1_000 * 2000 + n * 2.0e5, 0)
      "node_disk_io_time_seconds_total" -> Float.round(500.0 + ts_ms / 1.0e6 + n * 5.0, 2)
      "node_network_receive_bytes_total" -> Float.round(5.0e9 + ts_ms / 1_000 * 5000 + n * 5.0e5, 0)
      "node_network_transmit_bytes_total" -> Float.round(3.0e9 + ts_ms / 1_000 * 3000 + n * 3.0e5, 0)
      "node_network_receive_errs_total" -> Float.round(n * 100.0, 0)
      "node_network_transmit_errs_total" -> Float.round(n * 50.0, 0)
      "node_load1" -> Float.round(0.5 + n * 4.0 + dh * 2.0, 2)
      "node_load5" -> Float.round(0.4 + n * 3.0 + dh * 1.5, 2)
      "node_load15" -> Float.round(0.3 + n * 2.0 + dh * 1.0, 2)
      "node_time_seconds" -> Float.round(ts_ms / 1_000.0, 3)
      "node_boot_time_seconds" -> Float.round(ts_ms / 1_000 - 86_400.0 * (1 + dh * 30), 0)
      "node_context_switches_total" -> Float.round(1.0e6 + ts_ms / 100 + n * 1.0e4, 0)
      "node_entropy_avail_bits" -> Float.round(2000.0 + n * 2000.0, 0)
      "node_filefd_allocated" -> Float.round(500.0 + n * 2000.0 + dh * 500.0, 0)
      "node_forks_total" -> Float.round(1.0e5 + ts_ms / 1.0e4 + n * 1000.0, 0)
      "node_intr_total" -> Float.round(5.0e7 + ts_ms / 100 + n * 1.0e5, 0)
      "node_procs_running" -> Float.round(1.0 + n * 8.0, 0)
      "node_procs_blocked" -> Float.round(n * 2.0, 0)
      "node_scrape_collector_duration_seconds" -> Float.round(0.001 + n * 0.05, 4)
      "node_scrape_collector_success" -> 1.0
      "node_nf_conntrack_entries" -> Float.round(100.0 + n * 5000.0 + dh * 1000.0, 0)
      "node_vmstat_pgfault" -> Float.round(1.0e7 + ts_ms / 1_000 + n * 5.0e4, 0)
      _ -> Float.round(n * 100.0, 2)
    end
  end

  # --- Helpers ---

  defp build_client(url) do
    Req.new(
      base_url: url,
      retry: false,
      connect_options: [timeout: 10_000],
      receive_timeout: 30_000
    )
  end

  defp verify_target(name, url) do
    case Req.get("#{url}/health") do
      {:ok, %{status: 200}} -> IO.puts("  #{name} OK at #{url}")
      other ->
        IO.puts("  ERROR: #{name} not reachable at #{url}: #{inspect(other)}")
        System.halt(1)
    end
  end

  defp ets_values(tab), do: :ets.tab2list(tab) |> Enum.map(fn {_id, us} -> us end)

  defp reset_counters(ctr, n) do
    for i <- 1..n, do: :counters.put(ctr, i, 0)
  end

  defp percentile([], _p), do: 0

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    k = max(0, trunc(Float.ceil(length(sorted) * p / 100) - 1))
    Enum.at(sorted, k)
  end

  # --- Formatting ---

  defp fmt_int(n) when is_float(n), do: fmt_int(trunc(n))
  defp fmt_int(n) when n >= 1_000_000, do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"
  defp fmt_int(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_int(n), do: Integer.to_string(n)

  defp fmt_us(0), do: "—"
  defp fmt_us(us) when is_float(us), do: fmt_us(trunc(us))
  defp fmt_us(us) when us >= 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
  defp fmt_us(us) when us >= 1_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 2)}ms"
  defp fmt_us(us), do: "#{us}us"

  defp format_interval(s) when is_integer(s), do: format_interval(s / 1)
  defp format_interval(s) when s >= 1.0, do: :erlang.float_to_binary(s, decimals: 1) <> "s"

  defp format_interval(s) when s >= 0.01 do
    :erlang.float_to_binary(s * 1000, decimals: 0) <> "ms"
  end

  defp format_interval(s) do
    :erlang.float_to_binary(s * 1000, decimals: 1) <> "ms"
  end

  # --- Output ---

  @c 10

  defp print_comparison(tm_steps, vm_steps, skip_vm) do
    # Build interval -> {tm_result, vm_result} map
    tm_by_int = Map.new(tm_steps, &{&1.interval_ms, &1})

    vm_by_int =
      if skip_vm, do: %{}, else: Map.new(vm_steps, &{&1.interval_ms, &1})

    all_intervals =
      (Map.keys(tm_by_int) ++ Map.keys(vm_by_int))
      |> Enum.uniq()
      |> Enum.sort(:desc)

    print_write_table(all_intervals, tm_by_int, vm_by_int, skip_vm)
    print_query_table(all_intervals, tm_by_int, vm_by_int, skip_vm)
  end

  defp print_write_table(intervals, tm, vm, skip_vm) do
    c = @c

    IO.puts("  Write Latency")
    IO.puts("  " <> String.duplicate("-", 80))

    if skip_vm do
      IO.puts(
        "  " <>
          pad_r("Interval", c) <> pad_l("Req/s", c) <> pad_l("Pts/s", c) <>
          pad_l("p50", c) <> pad_l("p99", c) <> pad_l("p999", c)
      )
    else
      IO.puts(
        "  " <>
          pad_r("Interval", c) <>
          pad_l("TM Pts/s", c) <> pad_l("TM p50", c) <> pad_l("TM p99", c) <> pad_l("TM p999", c) <>
          pad_l("VM Pts/s", c) <> pad_l("VM p50", c) <> pad_l("VM p99", c) <> pad_l("VM p999", c)
      )
    end

    for int_ms <- intervals do
      tm_r = Map.get(tm, int_ms)
      vm_r = Map.get(vm, int_ms)
      int_s = int_ms / 1000

      if skip_vm do
        if tm_r do
          IO.puts(
            "  " <>
              pad_r(format_interval(int_s), c) <>
              pad_l(fmt_int(tm_r.w_reqs / tm_r.elapsed_s), c) <>
              pad_l(fmt_int(tm_r.w_pts / tm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 50)), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 99)), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 99.9)), c) <>
              err_suffix(tm_r.w_errs, nil)
          )
        end
      else
        tm_part =
          if tm_r do
            pad_l(fmt_int(tm_r.w_pts / tm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 50)), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 99)), c) <>
              pad_l(fmt_us(percentile(tm_r.w_lats, 99.9)), c)
          else
            pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c)
          end

        vm_part =
          if vm_r do
            pad_l(fmt_int(vm_r.w_pts / vm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(vm_r.w_lats, 50)), c) <>
              pad_l(fmt_us(percentile(vm_r.w_lats, 99)), c) <>
              pad_l(fmt_us(percentile(vm_r.w_lats, 99.9)), c)
          else
            pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c)
          end

        IO.puts(
          "  " <> pad_r(format_interval(int_s), c) <> tm_part <> vm_part <>
            err_suffix(tm_r && tm_r.w_errs, vm_r && vm_r.w_errs)
        )
      end
    end
  end

  defp print_query_table(intervals, tm, vm, skip_vm) do
    c = @c

    IO.puts("")
    IO.puts("  Query Latency Under Write Load")
    IO.puts("  " <> String.duplicate("-", 80))

    if skip_vm do
      IO.puts(
        "  " <>
          pad_r("W Pts/s", c) <> pad_l("Q/s", c) <>
          pad_l("p50", c) <> pad_l("p99", c) <> pad_l("p999", c)
      )

      for int_ms <- intervals do
        if tm_r = Map.get(tm, int_ms) do
          IO.puts(
            "  " <>
              pad_r(fmt_int(tm_r.w_pts / tm_r.elapsed_s), c) <>
              pad_l(fmt_int(tm_r.q_count / tm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 50)), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 99)), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 99.9)), c)
          )
        end
      end
    else
      IO.puts(
        "  " <>
          pad_r("W Pts/s", c) <>
          pad_l("TM Q/s", c) <> pad_l("TM p50", c) <> pad_l("TM p99", c) <> pad_l("TM p999", c) <>
          pad_l("VM Q/s", c) <> pad_l("VM p50", c) <> pad_l("VM p99", c) <> pad_l("VM p999", c)
      )

      for int_ms <- intervals do
        tm_r = Map.get(tm, int_ms)
        vm_r = Map.get(vm, int_ms)

        # Use whichever has data for the pts/s label
        ref = tm_r || vm_r
        pts_label = fmt_int(ref.w_pts / ref.elapsed_s)

        tm_q =
          if tm_r && tm_r.q_count > 0 do
            pad_l(fmt_int(tm_r.q_count / tm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 50)), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 99)), c) <>
              pad_l(fmt_us(percentile(tm_r.q_lats, 99.9)), c)
          else
            pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c)
          end

        vm_q =
          if vm_r && vm_r.q_count > 0 do
            pad_l(fmt_int(vm_r.q_count / vm_r.elapsed_s), c) <>
              pad_l(fmt_us(percentile(vm_r.q_lats, 50)), c) <>
              pad_l(fmt_us(percentile(vm_r.q_lats, 99)), c) <>
              pad_l(fmt_us(percentile(vm_r.q_lats, 99.9)), c)
          else
            pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c) <> pad_l("—", c)
          end

        IO.puts("  " <> pad_r(pts_label, c) <> tm_q <> vm_q)
      end
    end
  end

  defp print_summary(tm_url, series_count, tm_steps, vm_steps, skip_vm) do
    IO.puts("")
    IO.puts("  Summary (#{fmt_int(series_count)} series)")
    IO.puts("  " <> String.duplicate("=", 64))

    tm_peak = Enum.max_by(tm_steps, &(&1.w_pts / &1.elapsed_s))
    tm_peak_pts = trunc(tm_peak.w_pts / tm_peak.elapsed_s)
    IO.puts("  TM peak:      #{fmt_int(tm_peak_pts)} pts/s at #{format_interval(tm_peak.interval_s)} interval")

    tm_total_q = Enum.reduce(tm_steps, 0, &(&1.q_count + &2))
    tm_total_qe = Enum.reduce(tm_steps, 0, &(&1.q_errs + &2))
    IO.puts("  TM queries:   #{fmt_int(tm_total_q)} total, #{tm_total_qe} errors")

    unless skip_vm do
      vm_peak = Enum.max_by(vm_steps, &(&1.w_pts / &1.elapsed_s))
      vm_peak_pts = trunc(vm_peak.w_pts / vm_peak.elapsed_s)
      IO.puts("  VM peak:      #{fmt_int(vm_peak_pts)} pts/s at #{format_interval(vm_peak.interval_s)} interval")

      vm_total_q = Enum.reduce(vm_steps, 0, &(&1.q_count + &2))
      vm_total_qe = Enum.reduce(vm_steps, 0, &(&1.q_errs + &2))
      IO.puts("  VM queries:   #{fmt_int(vm_total_q)} total, #{vm_total_qe} errors")
    end

    # Verify TM
    case Req.get("#{tm_url}/health") do
      {:ok, %{status: 200, body: h}} when is_map(h) ->
        series = h["series"] || 0
        points = (h["points"] || 0) + (h["buffer_points"] || 0)
        IO.puts("  TM verified:  #{fmt_int(series)} series, #{fmt_int(points)} points (expected #{fmt_int(series_count)} series)")

      _ ->
        IO.puts("  TM health:    (could not verify)")
    end

    IO.puts("  " <> String.duplicate("=", 64))
  end

  defp pad_l(str, w), do: String.pad_leading(str, w)
  defp pad_r(str, w), do: String.pad_trailing(str, w)

  defp err_suffix(nil, nil), do: ""
  defp err_suffix(tm_e, nil) when is_integer(tm_e) and tm_e > 0, do: "  err: TM=#{tm_e}"
  defp err_suffix(nil, vm_e) when is_integer(vm_e) and vm_e > 0, do: "  err: VM=#{vm_e}"

  defp err_suffix(tm_e, vm_e) when is_integer(tm_e) and is_integer(vm_e) do
    cond do
      tm_e > 0 and vm_e > 0 -> "  err: TM=#{tm_e} VM=#{vm_e}"
      tm_e > 0 -> "  err: TM=#{tm_e}"
      vm_e > 0 -> "  err: VM=#{vm_e}"
      true -> ""
    end
  end

  defp err_suffix(_, _), do: ""
end

RealisticWorkload.run()
