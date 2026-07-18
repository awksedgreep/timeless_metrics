defmodule IngestSegmentsBench do
  @moduledoc """
  Segment-level benchmark for the Prometheus ingest pipeline.

  Isolates each stage of scrape/push ingestion to locate where time goes,
  to test the assumptions behind a possible fused parse->resolve->write NIF:

    parse/cpp          C++ NIF parse (includes BEAM term materialization)
    parse/rust-count   Rust parse only, zero terms built (bench prototype)
    parse/rust-terms   Rust parse + same term shape as the C++ NIF
    middle/write_batch normalize + resolve (warm ETS cache) + encode + raw write
    write/raw          pre-resolved ids: encode 24B/point binary + raw write

  Interpretation:
    (rust-terms - rust-count)  = exact term-materialization cost, same parser
    (cpp - rust-count)         ~ terms + parser implementation delta
    (write_batch - raw)        = Elixir middle (normalize/resolve/encode) cost

  Usage:
    mix run bench/ingest_segments_bench.exs
  """

  alias TimelessMetrics.PrometheusNif
  alias TimelessMetrics.RustEngine
  alias TimelessMetrics.RustEngine.Nif

  @parse_iters 30
  @write_iters 20
  @labels_per_series 8

  def run do
    IO.puts("\n==================================================")
    IO.puts("  Ingest Segment Benchmark")
    IO.puts("  Schedulers: #{System.schedulers_online()}")
    IO.puts("  Parse iters: #{@parse_iters}, write iters: #{@write_iters}")
    IO.puts("==================================================\n")

    verify_parser_equivalence()

    for samples <- [1_000, 10_000, 50_000] do
      run_scale(samples)
    end

    IO.puts("Done. Raw numbers above; see moduledoc for interpretation.\n")
  end

  # ── Correctness: Rust parser must agree with C++ parser ─────────────

  defp verify_parser_equivalence do
    body = gen_body(500)
    {cpp_entries, cpp_errors} = PrometheusNif.parse(body)
    {rust_entries, rust_errors} = Nif.parse_prometheus_terms(body)
    {count, count_errors} = Nif.parse_prometheus_count(body)

    cpp_norm = Enum.map(cpp_entries, fn {n, l, v, t} -> {n, Map.new(l), v, t} end)
    rust_norm = Enum.map(rust_entries, fn {n, l, v, t} -> {n, Map.new(l), v, t} end)

    cond do
      cpp_norm != rust_norm ->
        diff = Enum.zip(cpp_norm, rust_norm) |> Enum.find(fn {a, b} -> a != b end)
        raise "parser mismatch: #{inspect(diff, limit: 5)}"

      cpp_errors != rust_errors or count_errors != cpp_errors ->
        raise "error count mismatch: cpp=#{cpp_errors} rust=#{rust_errors} count=#{count_errors}"

      count != length(cpp_norm) ->
        raise "entry count mismatch: cpp=#{length(cpp_norm)} rust-count=#{count}"

      true ->
        IO.puts("parser equivalence: OK (#{count} entries, #{cpp_errors} errors)\n")
    end
  end

  # ── Benchmark one body size ─────────────────────────────────────────

  defp run_scale(samples) do
    body = gen_body(samples)
    body_kb = div(byte_size(body), 1024)

    IO.puts("--- #{fmt_int(samples)} samples (#{body_kb} KB body, #{@labels_per_series} labels/series) ---\n")
    IO.puts(
      String.pad_trailing("segment", 22) <>
        String.pad_leading("min", 10) <>
        String.pad_leading("median", 10) <>
        String.pad_leading("mean", 10) <>
        String.pad_leading("+GC", 10)
    )

    report("parse/cpp", bench(@parse_iters, fn -> PrometheusNif.parse(body) end))
    report("parse/rust-count", bench(@parse_iters, fn -> Nif.parse_prometheus_count(body) end))
    report("parse/rust-terms", bench(@parse_iters, fn -> Nif.parse_prometheus_terms(body) end))

    bench_writes(body, samples)
    IO.puts("")
  end

  defp bench_writes(body, samples) do
    data_dir = "/tmp/timeless_ingest_bench_#{System.os_time(:millisecond)}"
    store = :"ingest_bench_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Supervisor.start_link(
        [{TimelessMetrics, name: store, data_dir: data_dir, self_monitor: false}],
        strategy: :one_for_one
      )

    try do
      {parsed, 0} = PrometheusNif.parse(body)
      base_ts = System.os_time(:second)

      entries =
        Enum.map(parsed, fn {name, labels, value, _ts} ->
          {name, Map.new(labels), value, base_ts}
        end)

      # Cold first write: creates series in registry + warms ETS cache
      {cold_us, :ok} = :timer.tc(fn -> TimelessMetrics.write_batch(store, entries) end)
      IO.puts(String.pad_trailing("middle/first(cold)", 22) <> String.pad_leading(fmt_us(cold_us), 10))

      # Steady state: same series, fresh timestamps each iteration
      {times, gc_times} =
        bench_indexed(@write_iters, fn i ->
          shifted = Enum.map(entries, fn {n, l, v, _t} -> {n, l, v, base_ts + i + 1} end)
          TimelessMetrics.write_batch(store, shifted)
        end)

      report("middle/write_batch", {times, gc_times})

      # Raw path: resolve once, then pure encode + engine_write_batch_raw
      pairs = Enum.map(entries, fn {n, l, _v, _t} -> {n, l} end)
      {:ok, resolved} = RustEngine.resolve_series_batch(store, pairs)

      raw_points =
        Enum.map(entries, fn {n, l, v, _t} -> {Map.fetch!(resolved, {n, l}), v} end)

      engine = RustEngine.ref(store)

      {times, gc_times} =
        bench_indexed(@write_iters, fn i ->
          ts = base_ts + @write_iters + i + 2

          bin =
            for {id, v} <- raw_points, into: <<>> do
              <<id::signed-native-64, ts::signed-native-64, v * 1.0::float-native-64>>
            end

          {:ok, :ok} = Nif.engine_write_batch_raw(engine, bin)
        end)

      report("write/raw(enc+nif)", {times, gc_times})

      # Sanity: everything landed
      info = RustEngine.info(store)
      total = raw_stat(info, samples)
      IO.puts(String.pad_trailing("  (points stored)", 22) <> String.pad_leading(fmt_int(total), 10))
    after
      Supervisor.stop(sup)
      File.rm_rf!(data_dir)
    end
  end

  defp raw_stat(info, _samples) when is_map(info),
    do: Map.get(info, :total_points) || Map.get(info, "total_points") || 0

  # ── Timing helpers ──────────────────────────────────────────────────

  defp bench(iters, fun), do: bench_indexed(iters, fn _i -> fun.() end)

  defp bench_indexed(iters, fun) do
    Enum.map(1..iters, fn i ->
      :erlang.garbage_collect()
      {us, _res} = :timer.tc(fn -> fun.(i) end)
      {gc_us, _} = :timer.tc(fn -> :erlang.garbage_collect() end)
      {us, gc_us}
    end)
    |> Enum.unzip()
  end

  defp report(label, {times, gc_times}) do
    sorted = Enum.sort(times)
    min = List.first(sorted)
    median = Enum.at(sorted, div(length(sorted), 2))
    mean = div(Enum.sum(times), length(times))
    gc_mean = div(Enum.sum(gc_times), length(gc_times))

    IO.puts(
      String.pad_trailing(label, 22) <>
        String.pad_leading(fmt_us(min), 10) <>
        String.pad_leading(fmt_us(median), 10) <>
        String.pad_leading(fmt_us(mean), 10) <>
        String.pad_leading(fmt_us(gc_mean), 10)
    )
  end

  defp fmt_us(us) when us >= 1000, do: "#{Float.round(us / 1000, 2)}ms"
  defp fmt_us(us), do: "#{us}us"

  defp fmt_int(n) when n >= 1000, do: "#{div(n, 1000)}k"
  defp fmt_int(n), do: "#{n}"

  # ── Body generator: realistic exposition text ───────────────────────

  @metrics ~w(
    http_requests_total http_request_duration_seconds node_cpu_seconds_total
    node_memory_bytes process_open_fds go_goroutines jvm_gc_pause_seconds
    disk_io_bytes_total net_rx_packets_total queue_depth
  )

  defp gen_body(samples) do
    now_ms = System.os_time(:millisecond)

    lines =
      for i <- 1..samples do
        metric = Enum.at(@metrics, rem(i, length(@metrics)))

        labels =
          [
            ~s(host="web-#{rem(i, 200)}"),
            ~s(region="us-east-#{rem(i, 4)}"),
            ~s(dc="dc#{rem(i, 8)}"),
            ~s(env="#{Enum.at(~w(prod staging dev), rem(i, 3))}"),
            ~s(instance="10.0.#{rem(i, 250)}.#{rem(i * 7, 250)}:9100"),
            ~s(job="node-exporter"),
            ~s(service="svc-#{rem(i, 40)}"),
            ~s(shard="#{rem(i, 16)}")
          ]
          |> Enum.join(",")

        value = :erlang.float_to_binary(i * 1.5 + rem(i, 97) / 10, decimals: 3)
        "#{metric}{#{labels}} #{value} #{now_ms}"
      end

    header = [
      "# HELP http_requests_total Total HTTP requests",
      "# TYPE http_requests_total counter",
      ""
    ]

    Enum.join(header ++ lines, "\n") <> "\n"
  end
end

IngestSegmentsBench.run()
