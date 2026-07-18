defmodule CompactionBench do
  @moduledoc """
  Raw-first flush + compaction experiment (perf/raw-first-compaction).

  Simulates scrape-style ingestion — many series, small periodic flushes —
  and compares three strategies:

    baseline   compress-at-flush (pco@8), small chunks accumulate
    raw-first  raw chunks at flush, then one compaction pass (pco@12)
    ideal      single big flush compressed once (pco@8) — best case chunk size

  Reports flush wall time (ingest-side cost) and bytes/point (storage cost)
  at each stage.

  Usage: mix run bench/compaction_bench.exs
  """

  alias TimelessMetrics.RustEngine

  @series 2_000
  @rounds 20
  @points_per_round 50

  def run do
    IO.puts("\n==================================================")
    IO.puts("  Compaction Bench: #{@series} series x #{@rounds} rounds x #{@points_per_round} pts")
    IO.puts("  (#{@series * @rounds * @points_per_round} points per strategy)")
    IO.puts("==================================================\n")

    baseline = run_strategy(:baseline, defer: false, single_flush: false)
    raw_first = run_strategy(:raw_first, defer: true, single_flush: false)
    ideal = run_strategy(:ideal, defer: false, single_flush: true)

    IO.puts("\n  Strategy      Flush (hot)  Compact (bg)   Bytes/point   vs baseline")
    IO.puts("  --------------------------------------------------------------------")
    print_row("baseline", baseline, baseline)
    print_row("raw-first", raw_first, baseline)
    print_row("ideal", ideal, baseline)

    IO.puts("""

      raw-first bytes/point is AFTER compaction (pco@12, large chunks).
      ideal = one big pco@8 chunk per series (no small-chunk penalty).
    """)
  end

  defp run_strategy(tag, opts) do
    data_dir = "/tmp/timeless_compact_bench_#{tag}_#{System.os_time(:millisecond)}"
    store = :"compact_bench_#{tag}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {TimelessMetrics,
           name: store,
           data_dir: data_dir,
           self_monitor: false,
           defer_compression: opts[:defer]}
        ],
        strategy: :one_for_one
      )

    try do
      base_ts = 1_700_000_000
      series = for i <- 1..@series, do: series_entry(i)

      flush_us =
        if opts[:single_flush] do
          write_rounds(store, series, base_ts, @rounds)
          {us, :ok} = :timer.tc(fn -> RustEngine.flush(store) end)
          us
        else
          Enum.reduce(1..@rounds, 0, fn round, acc ->
            write_round(store, series, base_ts, round)
            {us, :ok} = :timer.tc(fn -> RustEngine.flush(store) end)
            acc + us
          end)
        end

      compact_us =
        if opts[:defer] do
          {us, {:ok, _series, _chunks}} = :timer.tc(fn -> RustEngine.compact(store, :all) end)
          us
        else
          0
        end

      info = RustEngine.info(store)
      disk_points = Map.get(info, :disk_points, 0)
      bytes = Map.get(info, :storage_bytes, 0)

      %{
        flush_us: flush_us,
        compact_us: compact_us,
        bytes_per_point: if(disk_points > 0, do: bytes / disk_points, else: 0.0),
        disk_points: disk_points
      }
    after
      Supervisor.stop(sup)
      File.rm_rf!(data_dir)
    end
  end

  defp write_rounds(store, series, base_ts, rounds) do
    for round <- 1..rounds, do: write_round(store, series, base_ts, round)
  end

  defp write_round(store, series, base_ts, round) do
    entries =
      Enum.flat_map(series, fn {metric, labels, kind, seed} ->
        for p <- 1..@points_per_round do
          i = (round - 1) * @points_per_round + p
          ts = base_ts + i * 15
          {metric, labels, value_for(kind, seed, i), ts}
        end
      end)

    :ok = TimelessMetrics.write_batch(store, entries)
  end

  defp series_entry(i) do
    metric = if rem(i, 2) == 0, do: "http_requests_total", else: "cpu_usage_percent"
    kind = if rem(i, 2) == 0, do: :counter, else: :gauge

    labels = %{
      "host" => "web-#{rem(i, 200)}",
      "region" => "us-east-#{rem(i, 4)}",
      "service" => "svc-#{rem(i, 40)}",
      "instance" => "10.0.#{rem(i, 250)}.#{rem(i * 7, 250)}:9100"
    }

    {metric, labels, kind, i}
  end

  # Realistic value shapes: counters accumulate with jitter; gauges wander.
  defp value_for(:counter, seed, i), do: 1.0 * seed + i * (3.0 + rem(seed, 7))
  defp value_for(:gauge, seed, i), do: 50.0 + 30.0 * :math.sin(i / 20 + seed) + rem(seed * i, 13) / 10

  defp print_row(label, m, baseline) do
    flush = "#{Float.round(m.flush_us / 1000, 1)}ms"
    compact = "#{Float.round(m.compact_us / 1000, 1)}ms"
    bpp = Float.round(m.bytes_per_point, 3)
    delta = Float.round((1.0 - m.bytes_per_point / baseline.bytes_per_point) * 100, 1)

    IO.puts(
      "  " <>
        String.pad_trailing(label, 14) <>
        String.pad_leading(flush, 11) <>
        String.pad_leading(compact, 14) <>
        String.pad_leading("#{bpp}", 14) <>
        String.pad_leading("#{delta}%", 13)
    )
  end
end

CompactionBench.run()
