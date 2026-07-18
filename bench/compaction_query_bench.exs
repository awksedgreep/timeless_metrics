defmodule CompactionQueryBench do
  @moduledoc """
  Query-latency regression check for raw-first + compaction
  (perf/raw-first-compaction branch).

  Concern under test: compacted chunks are large, and the read path
  decompresses a whole chunk before time-filtering — narrow dashboard
  queries against big chunks could regress.

  Builds two stores with IDENTICAL data (500 series x 20K points), then
  measures query latency shapes on both:

    baseline    compress-at-flush, 20 small chunks/series (1K pts each)
    compacted   raw-first flushes, then compaction -> 1 chunk/series (20K pts)

  Query shapes: narrow window (100 pts), 10% window, full range, full
  aggregate, and multi-series fanout (250 series) narrow + full.

  Usage: mix run bench/compaction_query_bench.exs
  """

  alias TimelessMetrics.RustEngine

  @series 500
  @points 20_000
  @rounds 20
  @interval 15
  # Timestamps end at 'now' so the age-gated compactor spares the
  # recent window, exactly as production would.
  @base_ts System.os_time(:second) - 20_000 * 15
  @sample_series 50
  @iters 5

  def run do
    IO.puts("\n==================================================")
    IO.puts("  Compaction Query Bench: #{@series} series x #{@points} pts")
    IO.puts("  baseline: #{@rounds} chunks/series | compacted: 1 chunk/series")
    IO.puts("==================================================\n")

    baseline = build_and_measure(:baseline, defer: false)
    compacted = build_and_measure(:compacted, defer: true)

    IO.puts("\n  Query shape             baseline    compacted     delta")
    IO.puts("  -------------------------------------------------------")

    for key <- [:narrow, :mid, :full, :aggregate, :fanout_narrow, :fanout_full] do
      b = baseline[key]
      c = compacted[key]
      delta = Float.round((c - b) / b * 100, 1)

      IO.puts(
        "  " <>
          String.pad_trailing("#{key}", 22) <>
          String.pad_leading(fmt(b), 10) <>
          String.pad_leading(fmt(c), 13) <>
          String.pad_leading("#{delta}%", 10)
      )
    end

    IO.puts("\n  (medians of #{@iters} runs; fanout = 250 series, one metric)\n")
  end

  defp build_and_measure(tag, opts) do
    data_dir = "/tmp/timeless_qbench_#{tag}_#{System.os_time(:millisecond)}"
    store = :"qbench_#{tag}"

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
      IO.puts("  populating #{tag}...")
      populate(store)

      if opts[:defer] do
        cutoff = System.os_time(:second) - 3600
        {:ok, series, chunks} = RustEngine.compact(store, cutoff)
        IO.puts("  compacted #{tag}: #{series} series, #{chunks} chunks replaced")
      end

      info = RustEngine.info(store)
      IO.puts("  #{tag}: #{Map.get(info, :disk_points)} disk points, " <>
                "#{Float.round(Map.get(info, :storage_bytes) / max(Map.get(info, :disk_points), 1), 2)} B/pt")

      measure(store)
    after
      Supervisor.stop(sup)
      File.rm_rf!(data_dir)
    end
  end

  defp populate(store) do
    pts_per_round = div(@points, @rounds)

    for round <- 0..(@rounds - 1) do
      entries =
        Enum.flat_map(1..@series, fn s ->
          for p <- 1..pts_per_round do
            i = round * pts_per_round + p
            ts = @base_ts + i * @interval
            {metric(s), labels(s), value(s, i), ts}
          end
        end)

      :ok = TimelessMetrics.write_batch(store, entries)
      :ok = RustEngine.flush(store)
    end
  end

  defp measure(store) do
    t_end = @base_ts + @points * @interval
    narrow_start = t_end - 100 * @interval
    mid_start = t_end - div(@points, 10) * @interval

    sample = Enum.take_random(1..@series, @sample_series)

    %{
      narrow: median_us(fn -> each_series(store, sample, narrow_start, t_end) end),
      mid: median_us(fn -> each_series(store, sample, mid_start, t_end) end),
      full: median_us(fn -> each_series(store, sample, 0, t_end) end),
      aggregate:
        median_us(fn ->
          Enum.each(sample, fn s ->
            _ =
              RustEngine.query_aggregate(store, metric(s), labels(s),
                from: 0,
                to: t_end,
                aggregate: :avg
              )
          end)
        end),
      fanout_narrow:
        median_us(fn ->
          _ = RustEngine.query_multi(store, "qbench_even", %{}, from: narrow_start, to: t_end)
        end),
      fanout_full:
        median_us(fn ->
          _ = RustEngine.query_multi(store, "qbench_even", %{}, from: 0, to: t_end)
        end)
    }
  end

  defp each_series(store, sample, from, to) do
    Enum.each(sample, fn s ->
      _ = RustEngine.query_raw(store, metric(s), labels(s), from: from, to: to)
    end)
  end

  defp median_us(fun) do
    # warm once, then median of timed runs
    fun.()

    1..@iters
    |> Enum.map(fn _ ->
      {us, _} = :timer.tc(fun)
      us
    end)
    |> Enum.sort()
    |> Enum.at(div(@iters, 2))
  end

  defp metric(s), do: if(rem(s, 2) == 0, do: "qbench_even", else: "qbench_odd")

  defp labels(s), do: %{"host" => "h#{s}", "shard" => "#{rem(s, 16)}"}

  defp value(s, i), do: 50.0 + 30.0 * :math.sin(i / 20 + s) + rem(s * i, 13) / 10

  defp fmt(us) when us >= 1000, do: "#{Float.round(us / 1000, 2)}ms"
  defp fmt(us), do: "#{us}us"
end

CompactionQueryBench.run()
