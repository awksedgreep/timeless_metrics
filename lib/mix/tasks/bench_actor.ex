defmodule Mix.Tasks.Bench.Actor do
  @shortdoc "Run actor engine scale benchmark"
  @moduledoc """
  Benchmarks the actor engine to find the single-node ceiling.

  One process per series, fan-out queries via Task.async_stream.
  Measures series startup, write throughput, query fan-out latency,
  memory profile, and compression throughput.

  ## Usage

      mix bench.actor                    # default: 10K series
      mix bench.actor --tier small       # 1K series
      mix bench.actor --tier medium      # 10K series
      mix bench.actor --tier large       # 100K series
      mix bench.actor --tier moon        # 500K series
      mix bench.actor --series 50000     # custom

  ## Phases

  0. Series registration + startup
  1. Write throughput (sequential, batch, concurrent saturation)
  2. Query fan-out latency
  3. Memory profile
  4. Compression throughput
  """

  use Mix.Task

  @tiers %{
    "small" => 1_000,
    "medium" => 10_000,
    "large" => 100_000,
    "moon" => 500_000
  }

  def run(args) do
    Mix.Task.run("app.start")

    {tier_name, series_count} = parse_args(args)
    data_dir = "/tmp/bench_actor_#{System.os_time(:millisecond)}"
    File.mkdir_p!(data_dir)

    banner(tier_name, series_count, data_dir)

    # Phase 0: Series registration
    mem_baseline = :erlang.memory(:total)
    proc_baseline = length(Process.list())

    {:ok, _} =
      TimelessMetrics.Supervisor.start_link(
        name: :bench_actor,
        data_dir: data_dir,
        block_size: 1000,
        max_blocks: 100,
        compression: :zstd,
        flush_interval: :timer.minutes(10)
      )

    now = System.os_time(:second)

    phase0_register(series_count, now)

    # Phase 1: Write throughput
    phase1_writes(series_count, now)

    # Phase 2: Query fan-out latency
    phase2_queries(series_count, now)

    # Phase 3: Memory profile
    phase3_memory(series_count, mem_baseline, proc_baseline)

    # Phase 4: Compression throughput
    phase4_compression(series_count, now)

    # Phase 5: Storage stats
    phase5_storage_stats()

    # Footer
    IO.puts("")
    IO.puts(bar())
    IO.puts("  Data: #{data_dir}")
    IO.puts("  Cleanup: rm -rf #{data_dir}")
    IO.puts(bar())
  end

  # ── Phase 0: Series Registration + Startup ─────────────────────────

  defp phase0_register(series_count, now) do
    header("Phase 0: Series Registration + Startup")

    {us, _} =
      :timer.tc(fn ->
        for n <- 0..(series_count - 1) do
          labels = %{"id" => Integer.to_string(n)}
          TimelessMetrics.write(:bench_actor, "scale_metric", labels, gen_value(), timestamp: now)
        end
      end)

    rate = trunc(series_count / (us / 1_000_000))
    mem_after = :erlang.memory(:total)

    IO.puts("  Registered #{fmt_int(series_count)} series in #{fmt_dur(us)}")
    IO.puts("  Registration rate: #{fmt_int(rate)} series/sec")
    IO.puts("  Memory after startup: #{fmt_bytes(mem_after)}")
  end

  # ── Phase 1: Write Throughput ───────────────────────────────────────
  #
  # Simulates real-world data collection cycles: a poller collects N metrics
  # from all devices every interval. Each cycle writes 1 point per series
  # via write_batch, then the next cycle does the same with a new timestamp.
  # This is how SNMP polling, Prometheus scraping, and agent-based collection
  # actually work — the same set of series, written to repeatedly.

  defp phase1_writes(series_count, base_ts) do
    header("Phase 1: Write Throughput")

    cycles = cycles_for(series_count)

    # Pre-build the label maps once (in real life these are stable)
    labels_by_id =
      for n <- 0..(series_count - 1), into: %{} do
        {n, %{"id" => Integer.to_string(n)}}
      end

    # 1a. Collection cycles via write_batch (the standard ingest path)
    IO.puts("")

    IO.puts(
      "  Collection cycles (#{cycles} cycles × #{fmt_int(series_count)} series, write_batch):"
    )

    {us, _} =
      :timer.tc(fn ->
        for cycle <- 0..(cycles - 1) do
          ts = base_ts + 100 + cycle

          entries =
            for n <- 0..(series_count - 1) do
              {"scale_metric", labels_by_id[n], gen_value(), ts}
            end

          TimelessMetrics.write_batch(:bench_actor, entries)
        end
      end)

    total = series_count * cycles
    rate = trunc(total / (us / 1_000_000))

    IO.puts(
      "    #{fmt_int(total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} pts/sec]  (#{cycles} cycles)"
    )

    # 1b. Collection cycles via individual writes (measures per-write routing cost)
    IO.puts("")

    IO.puts(
      "  Collection cycles (#{cycles} cycles × #{fmt_int(series_count)} series, individual writes):"
    )

    {us, _} =
      :timer.tc(fn ->
        for cycle <- 0..(cycles - 1) do
          ts = base_ts + 200 + cycle

          for n <- 0..(series_count - 1) do
            TimelessMetrics.write(:bench_actor, "scale_metric", labels_by_id[n], gen_value(),
              timestamp: ts
            )
          end
        end
      end)

    rate = trunc(total / (us / 1_000_000))

    IO.puts(
      "    #{fmt_int(total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} pts/sec]  (#{cycles} cycles)"
    )

    # 1c. Concurrent saturation — schedulers_online() writers for 5s
    writers = System.schedulers_online()
    duration_ms = 5_000
    IO.puts("")
    IO.puts("  Concurrent saturation (#{writers} writers × #{div(duration_ms, 1000)}s):")

    counter = :counters.new(1, [:atomics])
    deadline = System.monotonic_time(:millisecond) + duration_ms

    {us, _} =
      :timer.tc(fn ->
        1..writers
        |> Enum.map(fn _w ->
          Task.async(fn ->
            saturate_loop(counter, deadline, series_count, base_ts + 400)
          end)
        end)
        |> Task.await_many(:infinity)
      end)

    total = :counters.get(counter, 1)
    rate = trunc(total / (us / 1_000_000))
    IO.puts("    #{fmt_int(total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} pts/sec]")

    # 1d. Collection cycles via write_each (optimized: no group_by, no batch mapping)
    IO.puts("")

    IO.puts(
      "  Collection cycles (#{cycles} cycles × #{fmt_int(series_count)} series, write_each):"
    )

    each_total = series_count * cycles

    {us, _} =
      :timer.tc(fn ->
        for cycle <- 0..(cycles - 1) do
          ts = base_ts + 300 + cycle

          entries =
            for n <- 0..(series_count - 1) do
              {"scale_metric", labels_by_id[n], gen_value(), ts}
            end

          TimelessMetrics.write_each(:bench_actor, entries)
        end
      end)

    rate = trunc(each_total / (us / 1_000_000))

    IO.puts(
      "    #{fmt_int(each_total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} pts/sec]  (#{cycles} cycles)"
    )

    # 1e. Pre-resolved writes (zero lookup per write)
    IO.puts("")

    IO.puts(
      "  Collection cycles (#{cycles} cycles × #{fmt_int(series_count)} series, write_resolved):"
    )

    # Resolve all PIDs once
    pids_list =
      for n <- 0..(series_count - 1) do
        TimelessMetrics.resolve_series(:bench_actor, "scale_metric", labels_by_id[n])
      end

    {us, _} =
      :timer.tc(fn ->
        for cycle <- 0..(cycles - 1) do
          ts = base_ts + 400 + cycle

          Enum.each(pids_list, fn pid ->
            TimelessMetrics.write_resolved(pid, gen_value(), ts)
          end)
        end
      end)

    rate = trunc(each_total / (us / 1_000_000))

    IO.puts(
      "    #{fmt_int(each_total)} writes in #{fmt_dur(us)}  [#{fmt_int(rate)} pts/sec]  (#{cycles} cycles)"
    )

    # 1f. Write path breakdown — bulk-timed stages
    IO.puts("")
    IO.puts("  Write path breakdown (1 cycle × #{fmt_int(series_count)} series):")
    IO.puts("")

    manager = :bench_actor_actor_manager
    state_info = :persistent_term.get({TimelessMetrics.Actor.SeriesManager, manager})
    index = state_info.index

    # Build entries
    ts_bd = base_ts + 500

    {us_build, bd_entries} =
      :timer.tc(fn ->
        for n <- 0..(series_count - 1) do
          {"scale_metric", labels_by_id[n], gen_value(), ts_bd}
        end
      end)

    # ETS lookup (map key — no encode_labels needed)
    {us_ets, ets_results} =
      :timer.tc(fn ->
        Enum.map(bd_entries, fn {metric_name, labels, _v, _ts} ->
          [{_key, _sid, pid}] = :ets.lookup(index, {metric_name, labels})
          pid
        end)
      end)

    # GenServer.cast
    {us_cast, _} =
      :timer.tc(fn ->
        bd_entries
        |> Enum.zip(ets_results)
        |> Enum.each(fn {{_mn, _l, v, ts}, pid} ->
          GenServer.cast(pid, {:write, ts, v})
        end)
      end)

    # Full write_each for comparison
    ts_full = base_ts + 501

    full_entries =
      for n <- 0..(series_count - 1) do
        {"scale_metric", labels_by_id[n], gen_value(), ts_full}
      end

    {us_full_each, _} =
      :timer.tc(fn ->
        TimelessMetrics.write_each(:bench_actor, full_entries)
      end)

    # Full write_batch for comparison
    ts_full2 = base_ts + 502

    full_entries2 =
      for n <- 0..(series_count - 1) do
        {"scale_metric", labels_by_id[n], gen_value(), ts_full2}
      end

    {us_full_batch, _} =
      :timer.tc(fn ->
        TimelessMetrics.write_batch(:bench_actor, full_entries2)
      end)

    IO.puts("    Build entries:         #{String.pad_leading(fmt_dur(us_build), 10)}")
    IO.puts("    ETS lookup (map key):  #{String.pad_leading(fmt_dur(us_ets), 10)}")
    IO.puts("    GenServer.cast:        #{String.pad_leading(fmt_dur(us_cast), 10)}")
    IO.puts("    ─────────────────────────────────────")

    rate_each = trunc(series_count / (us_full_each / 1_000_000))
    rate_batch = trunc(series_count / (us_full_batch / 1_000_000))

    IO.puts(
      "    write_each:            #{String.pad_leading(fmt_dur(us_full_each), 10)}  [#{fmt_int(rate_each)} pts/sec]"
    )

    IO.puts(
      "    write_batch:           #{String.pad_leading(fmt_dur(us_full_batch), 10)}  [#{fmt_int(rate_batch)} pts/sec]"
    )

    # Let mailboxes drain and GC settle before memory measurements
    IO.write("    Draining mailboxes...")
    drain_mailboxes()
    IO.puts(" done")
  end

  defp cycles_for(series_count) when series_count <= 1_000, do: 1_000
  defp cycles_for(series_count) when series_count <= 5_000, do: 500
  defp cycles_for(series_count) when series_count <= 10_000, do: 200
  defp cycles_for(series_count) when series_count <= 100_000, do: 20
  defp cycles_for(_), do: 5

  defp saturate_loop(counter, deadline, series_count, base_ts) do
    if System.monotonic_time(:millisecond) < deadline do
      n = :rand.uniform(series_count) - 1
      labels = %{"id" => Integer.to_string(n)}

      TimelessMetrics.write(:bench_actor, "scale_metric", labels, gen_value(),
        timestamp: base_ts + :rand.uniform(1000)
      )

      :counters.add(counter, 1, 1)
      saturate_loop(counter, deadline, series_count, base_ts)
    end
  end

  # ── Phase 2: Query Fan-Out Latency ─────────────────────────────────

  defp phase2_queries(series_count, base_ts) do
    header("Phase 2: Query Fan-Out Latency")

    # Use separate metric names for fan-out subsets to avoid expensive regex
    fan_100 = min(100, series_count)
    fan_1k = min(1000, series_count)

    IO.puts("  Seeding 100 pts/series for query material...")
    IO.puts("    (also registering fan_100 and fan_1k subset metrics)")

    {seed_us, _} =
      :timer.tc(fn ->
        for pt <- 0..99 do
          ts = base_ts + 1000 + pt

          # Main metric — all series
          main_entries =
            for n <- 0..(series_count - 1) do
              {"scale_metric", %{"id" => Integer.to_string(n)}, gen_value(), ts}
            end

          # Subset metrics for fan-out benchmarks (separate metric names, no regex)
          fan_100_entries =
            for n <- 0..(fan_100 - 1) do
              {"scale_fan_100", %{"id" => Integer.to_string(n)}, gen_value(), ts}
            end

          fan_1k_entries =
            for n <- 0..(fan_1k - 1) do
              {"scale_fan_1k", %{"id" => Integer.to_string(n)}, gen_value(), ts}
            end

          subset_entries = fan_100_entries ++ fan_1k_entries

          TimelessMetrics.write_batch(:bench_actor, main_entries ++ subset_entries)

          if rem(pt, 25) == 0 do
            IO.write("\r    #{pt + 1}/100 batches written...")
          end
        end
      end)

    IO.puts("\r    Seeded in #{fmt_dur(seed_us)}                    ")
    IO.puts("")

    iterations = 50
    from = base_ts
    to = base_ts + 2000

    # Single series labels
    test_labels = %{"id" => "0"}

    queries = [
      {"raw single",
       fn ->
         TimelessMetrics.query(:bench_actor, "scale_metric", test_labels, from: from, to: to)
       end},
      {"fan-out #{fmt_int(fan_100)}",
       fn ->
         TimelessMetrics.query_multi(:bench_actor, "scale_fan_100", %{}, from: from, to: to)
       end},
      {"fan-out #{fmt_int(fan_1k)}",
       fn ->
         TimelessMetrics.query_multi(:bench_actor, "scale_fan_1k", %{}, from: from, to: to)
       end},
      {"fan-out ALL",
       fn ->
         TimelessMetrics.query_multi(:bench_actor, "scale_metric", %{}, from: from, to: to)
       end},
      {"agg single",
       fn ->
         TimelessMetrics.query_aggregate(:bench_actor, "scale_metric", test_labels,
           from: from,
           to: to,
           bucket: {60, :seconds},
           aggregate: :avg
         )
       end},
      {"agg fan-out ALL",
       fn ->
         TimelessMetrics.query_aggregate_multi(:bench_actor, "scale_metric", %{},
           from: from,
           to: to,
           bucket: {60, :seconds},
           aggregate: :avg
         )
       end},
      {"latest single",
       fn ->
         TimelessMetrics.latest(:bench_actor, "scale_metric", test_labels)
       end},
      {"latest ALL",
       fn ->
         TimelessMetrics.latest_multi(:bench_actor, "scale_metric", %{})
       end}
    ]

    IO.puts("  #{iterations} iterations each")
    IO.puts("")

    IO.puts(
      "  #{String.pad_trailing("Query", 22)} " <>
        "#{String.pad_leading("Avg", 10)} " <>
        "#{String.pad_leading("P50", 10)} " <>
        "#{String.pad_leading("P99", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 54)}")

    Enum.each(queries, fn {name, query_fn} ->
      try do
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
          "  #{String.pad_trailing(name, 22)} " <>
            "#{String.pad_leading(fmt_us(avg), 10)} " <>
            "#{String.pad_leading(fmt_us(p50), 10)} " <>
            "#{String.pad_leading(fmt_us(p99), 10)}"
        )
      catch
        :exit, _ ->
          IO.puts(
            "  #{String.pad_trailing(name, 22)} " <>
              "#{String.pad_leading("TIMEOUT", 10)} " <>
              "#{String.pad_leading("-", 10)} " <>
              "#{String.pad_leading("-", 10)}"
          )
      end
    end)
  end

  # ── Phase 3: Memory Profile ────────────────────────────────────────

  defp phase3_memory(series_count, mem_baseline, proc_baseline) do
    header("Phase 3: Memory Profile")

    mem = :erlang.memory()
    proc_count = length(Process.list())
    proc_delta = proc_count - proc_baseline
    mem_delta = mem[:total] - mem_baseline

    per_series =
      if series_count > 0, do: div(mem_delta, series_count), else: 0

    IO.puts("  Total memory:     #{fmt_bytes(mem[:total])}")
    IO.puts("  Process memory:   #{fmt_bytes(mem[:processes])}")
    IO.puts("  ETS memory:       #{fmt_bytes(mem[:ets])}")
    IO.puts("  Binary memory:    #{fmt_bytes(mem[:binary])}")
    IO.puts("  System memory:    #{fmt_bytes(mem[:system])}")
    IO.puts("")

    IO.puts(
      "  BEAM processes:   #{fmt_int(proc_count)} (#{fmt_int(proc_delta)} from actor engine)"
    )

    IO.puts("  Per-series est:   #{fmt_bytes(per_series)}")
    IO.puts("  Memory delta:     #{fmt_bytes(mem_delta)}")

    # Registry + index ETS memory
    actor_index = :bench_actor_actor_index

    index_mem =
      case :ets.info(actor_index, :memory) do
        :undefined -> 0
        words -> words * :erlang.system_info(:wordsize)
      end

    IO.puts("  Index ETS:        #{fmt_bytes(index_mem)}")
  end

  # ── Phase 4: Compression Throughput ────────────────────────────────

  defp phase4_compression(series_count, base_ts) do
    header("Phase 4: Compression Throughput")

    sample_size = min(100, series_count)
    block_size = 1000

    IO.puts("  Writing #{block_size} pts to #{sample_size} series to trigger compression...")

    {us, _} =
      :timer.tc(fn ->
        for pt <- 0..(block_size - 1) do
          ts = base_ts + 5000 + pt

          entries =
            for n <- 0..(sample_size - 1) do
              {"scale_metric", %{"id" => Integer.to_string(n)}, gen_value(), ts}
            end

          TimelessMetrics.write_batch(:bench_actor, entries)
        end
      end)

    total = sample_size * block_size
    rate = trunc(total / (us / 1_000_000))

    IO.puts("  #{fmt_int(total)} pts written+compressed in #{fmt_dur(us)}")
    IO.puts("  Throughput: #{fmt_int(rate)} pts/sec through GorillaStream compress")
  end

  # ── Phase 5: Storage Stats ────────────────────────────────────────

  defp phase5_storage_stats do
    header("Phase 5: Storage Stats")

    info = TimelessMetrics.info(:bench_actor)

    IO.puts("  Series count:      #{fmt_int(info.series_count)}")
    IO.puts("  Total points:      #{fmt_int(info.total_points)}")
    IO.puts("  Block count:       #{fmt_int(info.block_count)}")
    IO.puts("  Raw buffer pts:    #{fmt_int(info.raw_buffer_points)}")
    IO.puts("  Compressed bytes:  #{fmt_bytes(info.compressed_bytes)}")
    IO.puts("  Bytes/point:       #{info.bytes_per_point}")
    IO.puts("  Storage bytes:     #{fmt_bytes(info.storage_bytes)}")
    IO.puts("  Daily rollup rows: #{fmt_int(info.daily_rollup_rows)}")
    IO.puts("  Process count:     #{fmt_int(info.process_count)}")
    IO.puts("  Index ETS bytes:   #{fmt_bytes(info.index_ets_bytes)}")
  end

  # ── Data Generation ────────────────────────────────────────────────

  defp gen_value, do: 50.0 + :rand.uniform() * 50.0

  defp drain_mailboxes do
    # Wait for all actor GenServers to process pending casts,
    # then force a GC so memory numbers reflect actual retention.
    registry = :bench_actor_actor_registry

    Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(fn pid ->
      try do
        # Synchronous call — blocks until mailbox is drained up to this point
        GenServer.call(pid, :state, :infinity)
      catch
        :exit, _ -> :ok
      end
    end)

    :erlang.garbage_collect()

    Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(fn pid ->
      try do
        :erlang.garbage_collect(pid)
      catch
        _, _ -> :ok
      end
    end)
  end

  # ── Arg Parsing ────────────────────────────────────────────────────

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          tier: :string,
          series: :integer
        ]
      )

    case opts[:series] do
      nil ->
        tier = opts[:tier] || "medium"
        series = Map.get(@tiers, tier, 10_000)
        {tier, series}

      n ->
        {"custom", n}
    end
  end

  # ── Formatting ─────────────────────────────────────────────────────

  defp banner(tier, series_count, data_dir) do
    IO.puts("")
    IO.puts(bar())
    IO.puts("  Actor Engine Scale Benchmark — #{tier}")
    IO.puts(bar())
    IO.puts("  Series:     #{fmt_int(series_count)}")
    IO.puts("  Metric:     scale_metric")
    IO.puts("  Engine:     actor (1 process/series)")
    IO.puts("  CPU:        #{System.schedulers_online()} cores")
    IO.puts("  Block size: 1000 pts")
    IO.puts("  Storage:    #{data_dir}")
    IO.puts(bar())
  end

  defp header(title) do
    IO.puts("")
    IO.puts(title)
  end

  defp bar, do: "  " <> String.duplicate("=", 60)

  defp fmt_int(n) when is_float(n), do: fmt_int(trunc(n))

  defp fmt_int(n) when n >= 1_000_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000_000, decimals: 2)}B"

  defp fmt_int(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_int(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_int(n), do: Integer.to_string(n)

  defp fmt_bytes(n) when n >= 1_073_741_824,
    do: "#{:erlang.float_to_binary(n / 1_073_741_824, decimals: 1)} GB"

  defp fmt_bytes(n) when n >= 1_048_576,
    do: "#{:erlang.float_to_binary(n / 1_048_576, decimals: 1)} MB"

  defp fmt_bytes(n) when n >= 1_024, do: "#{:erlang.float_to_binary(n / 1_024, decimals: 1)} KB"
  defp fmt_bytes(n), do: "#{n} B"

  defp fmt_dur(us) when us >= 60_000_000,
    do: "#{:erlang.float_to_binary(us / 60_000_000, decimals: 1)}m"

  defp fmt_dur(us) when us >= 1_000_000,
    do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 1)}s"

  defp fmt_dur(us) when us >= 1_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_dur(us), do: "#{us}us"

  defp fmt_us(us) when us >= 1_000_000,
    do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"

  defp fmt_us(us) when us >= 1_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 2)}ms"
  defp fmt_us(us), do: "#{:erlang.float_to_binary(us / 1, decimals: 0)}us"

  defp percentile(sorted, p) do
    idx = trunc(length(sorted) * p)
    idx = min(idx, length(sorted) - 1)
    Enum.at(sorted, idx)
  end
end
