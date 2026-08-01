defmodule TimelessMetrics.RollupQueryBench do
  @moduledoc """
  Compare the former six-call row rollup adapter with the public packed path.

      MIX_ENV=test mix run bench/rollup_query_bench.exs --runs 30 --buckets 1200

  The fixture has one exact-label series and one sample per daily bucket. Two
  extra raw buckets satisfy the engine's one-full-bucket settlement margin.
  """

  @metric "rollup_query_bench"
  @labels %{"host" => "bench"}
  @resolution 86_400

  def run(args) do
    config = parse_args(args)
    unique = System.unique_integer([:positive])
    store = :"rollup_query_bench_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "rollup_query_bench_#{unique}")

    opts = [
      name: store,
      data_dir: data_dir,
      engine: :libsql,
      self_monitor: false,
      scraping: false,
      reader_pool_size: 2
    ]

    {:ok, supervisor} = Supervisor.start_link([{TimelessMetrics, opts}], strategy: :one_for_one)

    try do
      run_workload(store, config)
    after
      Supervisor.stop(supervisor)
      File.rm_rf!(data_dir)
    end
  end

  defp run_workload(store, config) do
    entries =
      Enum.map(0..(config.buckets + 1), fn index ->
        {@metric, @labels, index * 0.25, index * @resolution + 1}
      end)

    {ingest_us, :ok} = :timer.tc(fn -> TimelessMetrics.write_batch(store, entries) end)
    {flush_us, :ok} = :timer.tc(fn -> TimelessMetrics.flush(store) end)
    {rollup_us, :ok} = :timer.tc(fn -> TimelessMetrics.rollup(store) end)

    from = 0
    to = config.buckets * @resolution - 1
    old_query = fn -> six_row_rollup(store, from, to) end

    packed_query = fn ->
      TimelessMetrics.query_daily(store, @metric, @labels, from, to)
    end

    {:ok, old_warm} = old_query.()
    {:ok, packed_warm} = packed_query.()
    ensure!(length(old_warm) == config.buckets, "six-call fixture lost rollup buckets")
    ensure!(old_warm == packed_warm, "packed and row-oriented rollups differ")

    old = measure(config.runs, old_query)
    packed = measure(config.runs, packed_query)

    IO.puts("# benchmark=rollup-query")
    IO.puts("# buckets=#{config.buckets}")
    IO.puts("# runs=#{config.runs}")
    IO.puts("# ingest_us=#{ingest_us}")
    IO.puts("# flush_us=#{flush_us}")
    IO.puts("# rollup_us=#{rollup_us}")
    IO.puts("metric,median_us,p95_us,min_us,max_us,runs,result_buckets")
    print_stats("rollup_six_row_aggregates", old)
    print_stats("rollup_all_batches_public", packed)
  end

  # Reproduce the adapter path that timeless_rollup_batches replaced: six
  # barriers, six SQL executions, repeated label JSON decoding, then a join by
  # timestamp in Elixir.
  defp six_row_rollup(store, from, to) do
    labels_json = IO.iodata_to_binary(:json.encode(@labels))
    writer = TimelessMetrics.LibsqlEngine.writer_name(store)
    readers = :persistent_term.get({TimelessMetrics.LibsqlEngine, store, :readers})
    reader = Enum.at(readers, :erlang.phash2(self(), length(readers)))

    aggregates =
      for agg <- [:avg, :min, :max, :count, :sum, :last], into: %{} do
        :ok = GenServer.call(writer, :read_barrier, :infinity)

        sql =
          "SELECT labels, ts, value FROM " <>
            "timeless_rollup('metric_samples', ?1, ?2, ?3, ?4, ?5, ?6)"

        {:ok, rows} =
          GenServer.call(
            reader,
            {:sql, sql, [@metric, labels_json, @resolution, from, to, Atom.to_string(agg)]},
            :infinity
          )

        values =
          rows
          |> Enum.filter(fn [row_labels, _timestamp, _value] ->
            :json.decode(row_labels) == @labels
          end)
          |> Map.new(fn [_row_labels, timestamp, value] -> {timestamp, value} end)

        {agg, values}
      end

    buckets =
      aggregates.avg
      |> Map.keys()
      |> Enum.sort()
      |> Enum.map(fn timestamp ->
        %{
          bucket: timestamp,
          avg: aggregates.avg[timestamp],
          min: aggregates.min[timestamp],
          max: aggregates.max[timestamp],
          count: trunc(aggregates.count[timestamp]),
          sum: aggregates.sum[timestamp],
          last: aggregates.last[timestamp]
        }
      end)

    {:ok, buckets}
  end

  defp measure(runs, operation) do
    {samples, signature} =
      Enum.map_reduce(1..runs, nil, fn _, expected ->
        :erlang.garbage_collect()
        {elapsed, {:ok, buckets}} = :timer.tc(operation)
        signature = {length(buckets), :erlang.phash2(buckets)}
        ensure!(expected in [nil, signature], "benchmark result changed between runs")
        {elapsed, signature}
      end)

    sorted = Enum.sort(samples)
    p95_index = max(ceil(length(sorted) * 0.95) - 1, 0)

    %{
      median_us: Enum.at(sorted, div(length(sorted), 2)),
      p95_us: Enum.at(sorted, p95_index),
      min_us: hd(sorted),
      max_us: List.last(sorted),
      runs: runs,
      buckets: elem(signature, 0)
    }
  end

  defp print_stats(metric, stats) do
    IO.puts(
      Enum.join(
        [
          metric,
          stats.median_us,
          stats.p95_us,
          stats.min_us,
          stats.max_us,
          stats.runs,
          stats.buckets
        ],
        ","
      )
    )
  end

  defp parse_args(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args, strict: [runs: :integer, buckets: :integer])

    ensure!(positional == [] and invalid == [], usage())
    config = %{runs: Keyword.get(opts, :runs, 30), buckets: Keyword.get(opts, :buckets, 1_200)}
    ensure!(config.runs > 0 and config.buckets >= 1_000, usage())
    config
  end

  defp usage,
    do: "usage: mix run bench/rollup_query_bench.exs [--runs N] [--buckets N>=1000]"

  defp ensure!(true, _message), do: :ok
  defp ensure!(false, message), do: raise(message)
end

TimelessMetrics.RollupQueryBench.run(System.argv())
