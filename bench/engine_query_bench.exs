defmodule TimelessMetrics.EngineQueryBench do
  @moduledoc """
  Public-API Rust/libSQL query comparison for the libSQL read-performance plan.

      MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust
      MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql

  Run engines in separate invocations. Output is comment-prefixed metadata plus
  CSV so results can be checked into a benchmark record without reformatting.
  """

  @metric "query_hot_metric"
  @base_timestamp 1_700_000_000
  @batch_size 1_000

  def run(args) do
    config = parse_args(args)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "timeless_engine_query_#{config.engine}_#{System.unique_integer([:positive])}"
      )

    store = :"engine_query_#{config.engine}_#{System.unique_integer([:positive])}"

    opts = [
      name: store,
      data_dir: data_dir,
      engine: config.engine,
      self_monitor: false,
      scraping: false,
      reader_pool_size: max(div(System.schedulers_online(), 2), 2)
    ]

    {:ok, supervisor} = Supervisor.start_link([{TimelessMetrics, opts}], strategy: :one_for_one)

    try do
      run_workload(store, data_dir, config)
    after
      Supervisor.stop(supervisor)
      File.rm_rf!(data_dir)
    end
  end

  defp run_workload(store, data_dir, config) do
    series = build_series(config.series)
    stop_timestamp = @base_timestamp + config.points - 1
    total_points = config.series * config.points

    {populate_us, :ok} =
      :timer.tc(fn -> populate(store, series, config.points) end)

    {flush_us, :ok} = :timer.tc(fn -> TimelessMetrics.flush(store) end)

    exact_labels = series |> hd() |> elem(0)
    narrow_filter = %{"service" => "svc_1"}
    wide_filter = %{"env" => "prod"}
    selective_regex_filter = [{"host", {:regex, "device_1"}}]
    selective_negative_filter = [{"region", {:not_equal, "us-east"}}]

    exact = fn ->
      TimelessMetrics.query(store, @metric, exact_labels,
        from: @base_timestamp,
        to: stop_timestamp
      )
    end

    narrow = fn ->
      TimelessMetrics.query_multi(store, @metric, narrow_filter,
        from: @base_timestamp,
        to: stop_timestamp
      )
    end

    wide = fn ->
      TimelessMetrics.query_multi(store, @metric, wide_filter,
        from: @base_timestamp,
        to: stop_timestamp
      )
    end

    selective_regex = fn ->
      TimelessMetrics.query_multi(store, @metric, selective_regex_filter,
        from: @base_timestamp,
        to: stop_timestamp
      )
    end

    selective_regex_discovery = fn ->
      TimelessMetrics.StorageEngine.find_series(store, @metric, selective_regex_filter)
    end

    selective_negative = fn ->
      TimelessMetrics.query_multi(store, @metric, selective_negative_filter,
        from: @base_timestamp,
        to: stop_timestamp
      )
    end

    scalar_aggregate = fn ->
      TimelessMetrics.query_aggregate_multi(store, @metric, wide_filter,
        from: @base_timestamp,
        to: stop_timestamp,
        aggregate: :avg
      )
    end

    bucketed_aggregate = fn ->
      TimelessMetrics.query_aggregate_multi(store, @metric, wide_filter,
        from: @base_timestamp,
        to: stop_timestamp,
        bucket: {10, :seconds},
        aggregate: :avg
      )
    end

    latest = fn -> TimelessMetrics.latest_multi(store, @metric, wide_filter) end
    latest_exact = fn -> TimelessMetrics.latest(store, @metric, exact_labels) end

    first_exact = measure(1, exact, &point_result_signature/1)

    ensure!(
      first_exact.signature == {config.points, config.points},
      "unexpected exact result cardinality"
    )

    # Warm every shape before its repeated samples. The publication penalty is
    # preserved separately in first_exact_after_flush.
    {:ok, _} = exact.()
    {:ok, narrow_warm} = narrow.()
    {:ok, wide_warm} = wide.()
    {:ok, selective_regex_warm} = selective_regex.()
    {:ok, selective_discovery_warm} = selective_regex_discovery.()
    {:ok, selective_negative_warm} = selective_negative.()
    {:ok, scalar_warm} = scalar_aggregate.()
    {:ok, bucket_warm} = bucketed_aggregate.()
    {:ok, latest_warm} = latest.()
    {:ok, latest_exact_warm} = latest_exact.()

    expected_narrow = Enum.count(1..config.series, &(rem(&1, 64) == 1))
    ensure!(length(narrow_warm) == expected_narrow, "unexpected narrow series count")

    ensure!(
      total_series_points(narrow_warm) == expected_narrow * config.points,
      "unexpected narrow point count"
    )

    ensure!(length(wide_warm) == config.series, "unexpected wide series count")
    ensure!(total_series_points(wide_warm) == total_points, "unexpected wide point count")

    ensure!(
      length(selective_regex_warm) == 1 and
        total_series_points(selective_regex_warm) == config.points,
      "unexpected selective regex result"
    )

    ensure!(length(selective_discovery_warm) == 1, "unexpected selective discovery result")

    ensure!(
      length(selective_negative_warm) == div(config.series + 1, 2) and
        total_series_points(selective_negative_warm) ==
          div(config.series + 1, 2) * config.points,
      "unexpected selective negative result"
    )

    ensure!(length(scalar_warm) == config.series, "unexpected scalar aggregate count")
    ensure!(length(bucket_warm) == config.series, "unexpected bucket aggregate count")
    ensure!(length(latest_warm) == config.series, "unexpected latest count")
    ensure!(latest_exact_warm != nil, "unexpected empty exact latest")

    wide_memory = measure_process_peak_memory(wide)
    wide_external_bytes = :erlang.external_size(wide_warm)

    ensure!(
      wide_memory.peak - wide_memory.baseline <= wide_external_bytes * 10,
      "wide query exceeded the 10x serialized-result memory bound"
    )

    exact_stats = measure(max(config.exact_runs, 100), exact, &point_result_signature/1)
    narrow_stats = measure(config.runs, narrow, &series_result_signature/1)
    wide_stats = measure(config.runs, wide, &series_result_signature/1)

    selective_regex_stats =
      measure(config.runs, selective_regex, &series_result_signature/1)

    selective_discovery_stats =
      measure(
        max(config.exact_runs, 100),
        selective_regex_discovery,
        &label_set_signature/1
      )

    selective_negative_stats =
      measure(config.runs, selective_negative, &series_result_signature/1)

    scalar_stats =
      measure(config.runs, scalar_aggregate, &aggregate_result_signature/1)

    bucket_stats =
      measure(config.runs, bucketed_aggregate, &aggregate_result_signature/1)

    latest_stats = measure(config.runs, latest, &latest_result_signature/1)

    latest_exact_stats =
      measure(max(config.exact_runs, 100), latest_exact, &exact_latest_result_signature/1)

    profile =
      if config.engine == :libsql do
        profile_libsql(store, config, stop_timestamp, wide_filter)
      else
        []
      end

    sqlite_version =
      if config.engine == :libsql do
        {:ok, [[version]]} =
          TimelessMetrics.DB.read(:"#{store}_db", "SELECT sqlite_version()", [])

        version
      else
        "n/a"
      end

    IO.puts("# benchmark=engine-query")
    IO.puts("# engine=#{config.engine}")
    IO.puts("# sqlite=#{sqlite_version}")
    IO.puts("# otp=#{System.otp_release()}")
    IO.puts("# elixir=#{System.version()}")
    IO.puts("# schedulers=#{System.schedulers_online()}")
    IO.puts("# series=#{config.series}")
    IO.puts("# points_per_series=#{config.points}")
    IO.puts("# total_points=#{total_points}")
    IO.puts("# runs=#{config.runs}")
    IO.puts("# exact_runs=#{max(config.exact_runs, 100)}")
    IO.puts("# populate_us=#{populate_us}")
    IO.puts("# populate_points_per_second=#{rate(total_points, populate_us)}")
    IO.puts("# flush_us=#{flush_us}")
    IO.puts("# database_bytes=#{directory_bytes(data_dir)}")
    IO.puts("# wide_result_external_bytes=#{wide_external_bytes}")
    IO.puts("# wide_query_process_baseline_bytes=#{wide_memory.baseline}")
    IO.puts("# wide_query_process_peak_bytes=#{wide_memory.peak}")

    IO.puts(
      "# wide_query_process_peak_increment_bytes=#{wide_memory.peak - wide_memory.baseline}"
    )

    IO.puts(
      "# wide_query_process_peak_multiple=#{Float.round((wide_memory.peak - wide_memory.baseline) / wide_external_bytes, 3)}"
    )

    IO.puts("metric,median_us,p95_us,min_us,max_us,runs,result_a,result_b")
    print_stats("first_exact_after_flush", first_exact)
    print_stats("exact_raw", exact_stats)
    print_stats("narrow_raw", narrow_stats)
    print_stats("wide_raw", wide_stats)
    print_stats("selective_regex_raw", selective_regex_stats)
    print_stats("selective_regex_discovery", selective_discovery_stats)
    print_stats("selective_negative_raw", selective_negative_stats)
    print_stats("scalar_aggregate", scalar_stats)
    print_stats("bucketed_avg_10s", bucket_stats)
    print_stats("latest_exact", latest_exact_stats)
    print_stats("latest_multi", latest_stats)
    Enum.each(profile, fn {name, stats} -> print_stats(name, stats) end)
  end

  defp profile_libsql(store, config, stop_timestamp, wide_filter) do
    writer = TimelessMetrics.LibsqlEngine.writer_name(store)

    barrier =
      measure(
        max(config.exact_runs, 100),
        fn -> GenServer.call(writer, :read_barrier, :infinity) end,
        fn :ok -> {0, 0} end
      )

    readers = :persistent_term.get({TimelessMetrics.LibsqlEngine, store, :readers})
    reader = Enum.at(readers, :erlang.phash2(self(), length(readers)))
    filter_json = wide_filter |> :json.encode() |> IO.iodata_to_binary()
    params = [@metric, filter_json, @base_timestamp, stop_timestamp]

    fetch = fn -> GenServer.call(reader, {:raw_batches, params}, :infinity) end
    {:ok, rows} = fetch.()
    ensure!(length(rows) == config.series, "unexpected low-level wide series count")

    ensure!(
      packed_point_count(rows) == config.series * config.points,
      "unexpected low-level wide point count"
    )

    sqlite_fetch =
      measure(config.runs, fetch, fn {:ok, fetched} ->
        {length(fetched), packed_point_count(fetched)}
      end)

    decode = fn -> decode_packed_rows(rows) end
    decoded = decode.()

    elixir_decode =
      measure(config.runs, decode, fn result ->
        {length(result), total_series_points(result)}
      end)

    reduce = fn ->
      Enum.map(decoded, fn %{labels: labels, points: points} ->
        values = Enum.map(points, &elem(&1, 1))
        {labels, TimelessMetrics.Aggregation.compute_aggregate(:avg, values, points)}
      end)
    end

    elixir_reduce =
      measure(config.runs, reduce, fn result -> {length(result), length(result)} end)

    sort_shape = fn ->
      Enum.sort_by(decoded, fn %{labels: labels} -> Enum.sort(labels) end)
    end

    elixir_sort_shape =
      measure(config.runs, sort_shape, fn result ->
        {length(result), total_series_points(result)}
      end)

    fetch_decode_shape = fn ->
      {:ok, fetched} = fetch.()

      fetched
      |> decode_packed_rows()
      |> Enum.sort_by(fn %{labels: labels} -> Enum.sort(labels) end)
    end

    combined =
      measure(config.runs, fetch_decode_shape, fn result ->
        {length(result), total_series_points(result)}
      end)

    point_fetch = fn -> GenServer.call(reader, {:raw_points, params}, :infinity) end
    {:ok, point_rows} = point_fetch.()

    ensure!(
      packed_point_count(point_rows) == config.series * config.points,
      "unexpected projected wide point count"
    )

    projected_fetch =
      measure(config.runs, point_fetch, fn {:ok, fetched} ->
        {length(fetched), packed_point_count(fetched)}
      end)

    native_decode = fn -> decode_native_batches(point_rows) end
    decoded_native = native_decode.()

    native_raw_decode =
      measure(config.runs, native_decode, fn result ->
        {length(result), Enum.sum(Enum.map(result, fn {_sid, points} -> length(points) end))}
      end)

    cache = :persistent_term.get({TimelessMetrics.LibsqlEngine, store, :series_cache})

    native_shape = fn -> shape_native_raw(decoded_native, cache) end

    native_raw_shape =
      measure(config.runs, native_shape, fn result ->
        {length(result), total_series_points(result)}
      end)

    projected_combined = fn ->
      {:ok, fetched} = point_fetch.()

      fetched
      |> decode_native_batches()
      |> shape_native_raw(cache)
    end

    native_raw_combined =
      measure(config.runs, projected_combined, fn result ->
        {length(result), total_series_points(result)}
      end)

    frame_fetch = fn -> GenServer.call(reader, {:raw_frame, params}, :infinity) end
    {:ok, [[frame]]} = frame_fetch.()

    ensure!(raw_frame_point_count(frame) == config.series * config.points, "bad raw frame")

    native_frame_fetch =
      measure(config.runs, frame_fetch, fn {:ok, [[fetched]]} ->
        {raw_frame_series_count(fetched), raw_frame_point_count(fetched)}
      end)

    frame_decode = fn -> decode_native_frame(frame) end
    decoded_frame = frame_decode.()

    native_frame_decode =
      measure(config.runs, frame_decode, fn result ->
        {length(result), Enum.sum(Enum.map(result, fn {_sid, points} -> length(points) end))}
      end)

    frame_shape = fn -> shape_native_raw(decoded_frame, cache) end

    native_frame_shape =
      measure(config.runs, frame_shape, fn result ->
        {length(result), total_series_points(result)}
      end)

    frame_combined = fn ->
      {:ok, [[fetched]]} = frame_fetch.()

      fetched
      |> decode_native_frame()
      |> shape_native_raw(cache)
    end

    native_frame_combined =
      measure(config.runs, frame_combined, fn result ->
        {length(result), total_series_points(result)}
      end)

    frame_labels = fn -> raw_frame_label_maps(frame, cache) end
    labels = frame_labels.()

    native_frame_label_lookup =
      measure(config.runs, frame_labels, fn result -> {length(result), length(result)} end)

    frame_final_decode = fn -> decode_native_frame_series(frame, labels) end

    native_frame_final_decode =
      measure(config.runs, frame_final_decode, fn result ->
        {length(result), total_series_points(result)}
      end)

    frame_final_combined = fn ->
      {:ok, [[fetched]]} = frame_fetch.()
      fetched_labels = raw_frame_label_maps(fetched, cache)
      decode_native_frame_series(fetched, fetched_labels)
    end

    native_frame_final_combined =
      measure(config.runs, frame_final_combined, fn result ->
        {length(result), total_series_points(result)}
      end)

    aggregate_params = [@metric, filter_json, @base_timestamp, stop_timestamp, "avg"]
    aggregate_fetch = fn -> GenServer.call(reader, {:aggregate, aggregate_params}, :infinity) end
    {:ok, aggregate_rows} = aggregate_fetch.()

    native_aggregate_fetch =
      measure(config.runs, aggregate_fetch, fn {:ok, fetched} ->
        {length(fetched), length(fetched)}
      end)

    aggregate_shape = fn ->
      Enum.map(aggregate_rows, fn [sid, value] ->
        [{{:series_labels, ^sid}, labels}] = :ets.lookup(cache, {:series_labels, sid})
        %{labels: labels, data: [{@base_timestamp, value}]}
      end)
    end

    native_aggregate_shape =
      measure(config.runs, aggregate_shape, fn result ->
        {length(result), length(result)}
      end)

    aggregate_combined = fn ->
      {:ok, fetched} = aggregate_fetch.()

      Enum.map(fetched, fn [sid, value] ->
        [{{:series_labels, ^sid}, labels}] = :ets.lookup(cache, {:series_labels, sid})
        %{labels: labels, data: [{@base_timestamp, value}]}
      end)
    end

    native_aggregate_combined =
      measure(config.runs, aggregate_combined, fn result ->
        {length(result), length(result)}
      end)

    latest_params = [@metric, filter_json, 0, System.os_time(:second)]
    latest_fetch = fn -> GenServer.call(reader, {:latest, latest_params}, :infinity) end
    {:ok, latest_rows} = latest_fetch.()

    native_latest_fetch =
      measure(config.runs, latest_fetch, fn {:ok, fetched} ->
        {length(fetched), length(fetched)}
      end)

    latest_shape = fn -> shape_latest_rows(latest_rows, cache) end

    native_latest_shape =
      measure(config.runs, latest_shape, fn result ->
        {length(result), length(result)}
      end)

    latest_combined = fn ->
      {:ok, fetched} = latest_fetch.()
      shape_latest_rows(fetched, cache)
    end

    native_latest_combined =
      measure(config.runs, latest_combined, fn result ->
        {length(result), length(result)}
      end)

    window_profile =
      if rem(config.points, 10) == 0 do
        window_params = [
          @metric,
          filter_json,
          @base_timestamp + 9,
          stop_timestamp,
          10,
          10,
          "avg"
        ]

        window_fetch = fn -> GenServer.call(reader, {:window, window_params}, :infinity) end
        {:ok, window_rows} = window_fetch.()
        expected_rows = config.series * div(config.points, 10)
        ensure!(length(window_rows) == expected_rows, "unexpected native window row count")

        native_window_fetch =
          measure(config.runs, window_fetch, fn {:ok, fetched} ->
            {length(fetched), length(fetched)}
          end)

        window_shape = fn -> shape_window_rows(window_rows, 10) end

        native_window_shape =
          measure(config.runs, window_shape, fn result ->
            {length(result), aggregate_point_count(result)}
          end)

        window_combined = fn ->
          {:ok, fetched} = window_fetch.()
          shape_window_rows(fetched, 10)
        end

        native_window_combined =
          measure(config.runs, window_combined, fn result ->
            {length(result), aggregate_point_count(result)}
          end)

        window_batch_fetch = fn ->
          GenServer.call(reader, {:window_batches, window_params}, :infinity)
        end

        {:ok, window_batch_rows} = window_batch_fetch.()

        ensure!(
          length(window_batch_rows) == config.series,
          "unexpected native window batch row count"
        )

        native_window_batch_fetch =
          measure(config.runs, window_batch_fetch, fn {:ok, fetched} ->
            {length(fetched), length(fetched)}
          end)

        window_batch_shape = fn -> shape_window_batch_rows(window_batch_rows, cache, 10) end

        native_window_batch_shape =
          measure(config.runs, window_batch_shape, fn result ->
            {length(result), aggregate_point_count(result)}
          end)

        window_batch_combined = fn ->
          {:ok, fetched} = window_batch_fetch.()
          shape_window_batch_rows(fetched, cache, 10)
        end

        native_window_batch_combined =
          measure(config.runs, window_batch_combined, fn result ->
            {length(result), aggregate_point_count(result)}
          end)

        [
          {"libsql_native_window_fetch", native_window_fetch},
          {"libsql_native_window_shape", native_window_shape},
          {"libsql_native_window_combined", native_window_combined},
          {"libsql_native_window_batch_fetch", native_window_batch_fetch},
          {"libsql_native_window_batch_shape", native_window_batch_shape},
          {"libsql_native_window_batch_combined", native_window_batch_combined}
        ]
      else
        []
      end

    [
      {"libsql_barrier_no_pending", barrier},
      {"libsql_sqlite_wide_fetch", sqlite_fetch},
      {"libsql_elixir_wide_decode", elixir_decode},
      {"libsql_elixir_scalar_reduce", elixir_reduce},
      {"libsql_elixir_sort_shape", elixir_sort_shape},
      {"libsql_fetch_decode_shape", combined},
      {"libsql_projected_wide_fetch", projected_fetch},
      {"libsql_native_raw_decode", native_raw_decode},
      {"libsql_native_raw_shape", native_raw_shape},
      {"libsql_native_raw_combined", native_raw_combined},
      {"libsql_native_frame_fetch", native_frame_fetch},
      {"libsql_native_frame_decode", native_frame_decode},
      {"libsql_native_frame_shape", native_frame_shape},
      {"libsql_native_frame_combined", native_frame_combined},
      {"libsql_native_frame_label_lookup", native_frame_label_lookup},
      {"libsql_native_frame_final_decode", native_frame_final_decode},
      {"libsql_native_frame_final_combined", native_frame_final_combined},
      {"libsql_native_aggregate_fetch", native_aggregate_fetch},
      {"libsql_native_aggregate_shape", native_aggregate_shape},
      {"libsql_native_aggregate_combined", native_aggregate_combined},
      {"libsql_native_latest_fetch", native_latest_fetch},
      {"libsql_native_latest_shape", native_latest_shape},
      {"libsql_native_latest_combined", native_latest_combined}
    ] ++ window_profile
  end

  defp shape_latest_rows(rows, cache) do
    Enum.map(rows, fn [sid, timestamp, value] ->
      [{{:series_labels, ^sid}, labels}] = :ets.lookup(cache, {:series_labels, sid})
      %{labels: labels, timestamp: timestamp, value: value}
    end)
  end

  defp shape_window_rows(rows, step) do
    rows
    |> Enum.chunk_by(fn [labels_json, _timestamp, _value] -> labels_json end)
    |> Enum.map(fn [[labels_json, _timestamp, _value] | _] = series_rows ->
      data =
        Enum.map(series_rows, fn [_labels_json, timestamp, value] ->
          {timestamp - step + 1, value}
        end)

      %{labels: :json.decode(labels_json), data: data}
    end)
  end

  defp shape_window_batch_rows(rows, cache, step) do
    Enum.map(rows, fn [sid, bucket_blob] ->
      [{{:series_labels, ^sid}, labels}] = :ets.lookup(cache, {:series_labels, sid})
      %{labels: labels, data: decode_window_batch(bucket_blob, step)}
    end)
  end

  defp decode_window_batch(<<"TWB1", n::unsigned-little-32, rest::binary>>, step) do
    column_bytes = n * 8
    bitmap_bytes = div(n + 7, 8)

    case rest do
      <<timestamps::binary-size(^column_bytes), bitmap::binary-size(^bitmap_bytes),
        values::binary-size(^column_bytes)>> ->
        decode_window_columns(timestamps, bitmap, values, step, 0, [])

      _ ->
        raise "malformed packed window batch"
    end
  end

  defp decode_window_columns(<<>>, _bitmap, <<>>, _step, _index, acc),
    do: Enum.reverse(acc)

  defp decode_window_columns(
         <<timestamp::signed-little-64, timestamps::binary>>,
         bitmap,
         <<value::float-little-64, values::binary>>,
         step,
         index,
         acc
       ) do
    acc =
      if Bitwise.band(:binary.at(bitmap, div(index, 8)), Bitwise.bsl(1, rem(index, 8))) != 0 do
        [{timestamp - step + 1, value} | acc]
      else
        acc
      end

    decode_window_columns(timestamps, bitmap, values, step, index + 1, acc)
  end

  defp parse_args(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args,
        strict: [engine: :string, series: :integer, points: :integer, runs: :integer]
      )

    if positional != [] or invalid != [] do
      raise usage()
    end

    engine =
      case Keyword.get(opts, :engine) do
        "rust" -> :rust
        "libsql" -> :libsql
        _ -> raise usage()
      end

    config = %{
      engine: engine,
      series: Keyword.get(opts, :series, 12_000),
      points: Keyword.get(opts, :points, 60),
      runs: Keyword.get(opts, :runs, 20),
      exact_runs: 100
    }

    if config.series <= 0 or config.points <= 0 or config.runs <= 0 do
      raise usage()
    end

    config
  end

  defp usage do
    "usage: MIX_ENV=test mix run bench/engine_query_bench.exs " <>
      "--engine rust|libsql [--series N] [--points N] [--runs N]"
  end

  defp build_series(count) do
    for index <- 1..count do
      labels = %{
        "env" => "prod",
        "host" => "device_#{index}",
        "region" => if(rem(index, 2) == 0, do: "us-east", else: "us-west"),
        "service" => "svc_#{rem(index, 64)}"
      }

      {labels, index}
    end
  end

  defp populate(store, series, points_per_series) do
    Enum.each(0..(points_per_series - 1), fn point_index ->
      timestamp = @base_timestamp + point_index

      series
      |> Enum.map(fn {labels, series_index} ->
        {@metric, labels, series_index * 0.001 + point_index, timestamp}
      end)
      |> Enum.chunk_every(@batch_size)
      |> Enum.each(fn batch -> :ok = TimelessMetrics.write_batch(store, batch) end)
    end)

    :ok
  end

  defp measure(count, operation, signature_function) do
    {samples, signature} =
      Enum.map_reduce(1..count, nil, fn _, expected_signature ->
        :erlang.garbage_collect()
        {elapsed, result} = :timer.tc(operation)
        signature = signature_function.(result)

        if expected_signature != nil and signature != expected_signature do
          raise "benchmark result changed: #{inspect(expected_signature)} -> #{inspect(signature)}"
        end

        {elapsed, signature}
      end)

    sorted = Enum.sort(samples)
    p95_index = max(ceil(length(sorted) * 0.95) - 1, 0)

    %{
      median_us: Enum.at(sorted, div(length(sorted), 2)),
      p95_us: Enum.at(sorted, p95_index),
      min_us: hd(sorted),
      max_us: List.last(sorted),
      runs: count,
      signature: signature
    }
  end

  defp measure_process_peak_memory(operation) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        :erlang.garbage_collect()
        baseline = process_memory_with_binaries(self())
        send(parent, {:peak_ready, self(), baseline})

        receive do
          :run_peak_query -> :ok
        end

        {:ok, series} = operation.()
        signature = {length(series), total_series_points(series)}
        finished = process_memory_with_binaries(self())
        send(parent, {:peak_done, self(), finished, signature})

        receive do
          :release_peak_query -> :ok
        end
      end)

    baseline =
      receive do
        {:peak_ready, ^pid, bytes} -> bytes
      end

    send(pid, :run_peak_query)
    await_process_peak(pid, monitor, baseline, baseline)
  end

  defp await_process_peak(pid, monitor, baseline, peak) do
    receive do
      {:peak_done, ^pid, finished, {series, points}} ->
        send(pid, :release_peak_query)

        receive do
          {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
        end

        %{baseline: baseline, peak: max(peak, finished), series: series, points: points}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        raise "peak-memory query process exited: #{inspect(reason)}"
    after
      1 ->
        current =
          if Process.alive?(pid), do: process_memory_with_binaries(pid), else: peak

        await_process_peak(pid, monitor, baseline, max(peak, current))
    end
  end

  defp process_memory_with_binaries(pid) do
    {:memory, process_bytes} = Process.info(pid, :memory)
    {:binary, binaries} = Process.info(pid, :binary)
    process_bytes + Enum.sum(Enum.map(binaries, fn {_id, bytes, _refs} -> bytes end))
  end

  defp point_result_signature({:ok, points}), do: {length(points), length(points)}

  defp series_result_signature({:ok, series}) do
    {length(series), total_series_points(series)}
  end

  defp aggregate_result_signature({:ok, series}) do
    {length(series), aggregate_point_count(series)}
  end

  defp aggregate_point_count(series), do: Enum.sum(Enum.map(series, &length(&1.data)))

  defp latest_result_signature({:ok, series}), do: {length(series), length(series)}

  defp exact_latest_result_signature({:ok, {timestamp, value}}),
    do: {timestamp, :erlang.float_to_binary(value)}

  defp exact_latest_result_signature({:ok, nil}), do: {:empty, :empty}

  defp total_series_points(series) do
    Enum.sum(Enum.map(series, &length(&1.points)))
  end

  defp packed_point_count(rows) do
    Enum.sum(
      Enum.map(rows, fn
        [_series_id, _labels, <<count::unsigned-little-32, _::binary>>] -> count
        [_series_id, <<count::unsigned-little-32, _::binary>>] -> count
      end)
    )
  end

  defp decode_packed_rows(rows) do
    Enum.map(rows, fn [_series_id, labels_json, point_blob] ->
      %{labels: :json.decode(labels_json), points: decode_point_batch(point_blob)}
    end)
  end

  defp decode_native_batches(rows) do
    {:ok, decoded} =
      rows
      |> Enum.map(fn [series_id, point_blob] -> {series_id, point_blob} end)
      |> TimelessMetrics.RustEngine.Nif.decode_raw_batches()

    decoded
  end

  defp raw_frame_series_count(<<"TRF1", count::unsigned-little-32, _::binary>>), do: count

  defp raw_frame_point_count(
         <<"TRF1", _series_count::unsigned-little-32, count::unsigned-little-64, _::binary>>
       ),
       do: count

  defp decode_native_frame(frame) do
    {:ok, decoded} = TimelessMetrics.RustEngine.Nif.decode_raw_frame(frame)
    decoded
  end

  defp raw_frame_label_maps(
         <<"TRF1", series_count::unsigned-little-32, _total_points::unsigned-little-64,
           rest::binary>>,
         cache
       ) do
    id_bytes = series_count * 8
    <<ids::binary-size(^id_bytes), _::binary>> = rest

    for <<series_id::signed-little-64 <- ids>> do
      [{{:series_labels, ^series_id}, labels}] =
        :ets.lookup(cache, {:series_labels, series_id})

      labels
    end
  end

  defp decode_native_frame_series(frame, labels) do
    {:ok, decoded} = TimelessMetrics.RustEngine.Nif.decode_raw_frame_series(frame, labels)
    decoded
  end

  defp shape_native_raw(decoded, cache) do
    decoded
    |> Enum.map(fn {series_id, points} ->
      [{{:series_labels, ^series_id}, labels}] =
        :ets.lookup(cache, {:series_labels, series_id})

      %{labels: labels, points: points}
    end)
    |> Enum.sort_by(fn %{labels: labels} -> Enum.sort(labels) end)
  end

  defp decode_point_batch(<<count::unsigned-little-32, rest::binary>>) do
    column_bytes = count * 8

    case rest do
      <<timestamps::binary-size(^column_bytes), values::binary-size(^column_bytes)>> ->
        decode_point_columns(timestamps, values, [])

      _ ->
        raise "malformed packed point batch"
    end
  end

  defp decode_point_columns(<<>>, <<>>, acc), do: Enum.reverse(acc)

  defp decode_point_columns(
         <<timestamp::signed-little-64, timestamps::binary>>,
         <<value::float-little-64, values::binary>>,
         acc
       ) do
    decode_point_columns(timestamps, values, [{timestamp, value} | acc])
  end

  defp rate(count, elapsed_us), do: trunc(count * 1_000_000 / elapsed_us)

  defp label_set_signature({:ok, labels}) do
    {length(labels), Enum.sum(Enum.map(labels, &map_size/1))}
  end

  defp directory_bytes(path) do
    path
    |> File.ls!()
    |> Enum.reduce(0, fn entry, total ->
      child = Path.join(path, entry)

      total +
        case File.stat!(child) do
          %{type: :directory} -> directory_bytes(child)
          %{size: size} -> size
        end
    end)
  end

  defp ensure!(true, _message), do: :ok
  defp ensure!(false, message), do: raise(message)

  defp print_stats(metric, stats) do
    {result_a, result_b} = stats.signature

    IO.puts(
      "#{metric},#{stats.median_us},#{stats.p95_us},#{stats.min_us}," <>
        "#{stats.max_us},#{stats.runs},#{result_a},#{result_b}"
    )
  end
end

TimelessMetrics.EngineQueryBench.run(System.argv())
