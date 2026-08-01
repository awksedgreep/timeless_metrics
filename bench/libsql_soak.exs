defmodule TimelessMetrics.LibsqlSoak do
  @moduledoc """
  Mixed libSQL write/read/recovery soak used by the storage-engine release gate.

      MIX_ENV=test mix run bench/libsql_soak.exs --seconds 30 --readers 8

  The workload keeps an immutable query corpus alongside live writes. Readers
  continuously compare exact, fan-out, matcher, aggregate, window, latest, and
  discovery results with the corpus captured before the soak. Maintenance runs
  concurrently, the writer is killed once, and the final corpus is checked in
  both the reopened primary database and an online backup.
  """

  defmodule SoakSchema do
    use TimelessMetrics.Schema

    raw_retention({1, :hours})

    tier(:daily,
      resolution: :day,
      aggregates: [:avg, :min, :max, :count, :sum, :last],
      retention: {30, :days}
    )

    rollup_interval(:timer.hours(999))
    retention_interval(:timer.hours(999))
  end

  @metric "libsql_soak_static"
  @live_metric "libsql_soak_live"
  @rollup_metric "libsql_soak_rollup"
  @retention_metric "libsql_soak_retention"
  @series 128
  @points 120
  @batch_size 32

  @writes 1
  @write_transients 2
  @reads 3
  @read_transients 4
  @maintenance 5
  @backups 6
  @restarts 7
  @restart_window 8

  def run(args) do
    config = parse_args(args)
    unique = System.unique_integer([:positive])
    store = :"libsql_soak_#{unique}"
    backup_store = :"libsql_soak_backup_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "libsql_soak_#{unique}")
    backup_root = Path.join(System.tmp_dir!(), "libsql_soak_backups_#{unique}")
    counters = :atomics.new(8, signed: false)

    File.mkdir_p!(backup_root)
    {:ok, supervisor} = start_store(store, data_dir, config.readers)

    try do
      fixture = seed_fixture(store)
      expected = capture_corpus(store, fixture)
      deadline = System.monotonic_time(:millisecond) + config.seconds * 1_000

      writer = Task.async(fn -> writer_loop(store, fixture.now, deadline, counters, 0) end)

      readers =
        for _ <- 1..config.readers do
          Task.async(fn -> reader_loop(expected, deadline, counters) end)
        end

      maintenance =
        Task.async(fn ->
          maintenance_loop(store, backup_root, deadline, counters, 0, nil)
        end)

      restart =
        Task.async(fn ->
          restart_writer(store, div(config.seconds * 1_000, 2), counters)
        end)

      Task.await(writer, :infinity)
      Task.await_many(readers, :infinity)
      last_backup = Task.await(maintenance, :infinity)
      Task.await(restart, :infinity)

      :ok = TimelessMetrics.flush(store)
      verify_corpus!(expected)
      verify_rollup_and_retention!(store, fixture)
      verify_backup!(backup_store, last_backup, config.readers, expected, fixture)

      Supervisor.stop(supervisor)
      {:ok, reopened} = start_store(store, data_dir, config.readers)

      try do
        verify_corpus!(expected)
        verify_rollup_and_retention!(store, fixture)
      after
        Supervisor.stop(reopened)
      end

      print_summary(config, counters, data_dir, last_backup)
    after
      if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
      File.rm_rf!(data_dir)
      File.rm_rf!(backup_root)
    end
  end

  defp start_store(store, data_dir, readers) do
    opts = [
      name: store,
      data_dir: data_dir,
      engine: :libsql,
      schema: SoakSchema,
      self_monitor: false,
      scraping: false,
      reader_pool_size: readers
    ]

    Supervisor.start_link([{TimelessMetrics, opts}], strategy: :one_for_one)
  end

  defp seed_fixture(store) do
    now = System.os_time(:second)
    base = now - @points - 60

    entries =
      for index <- 1..@series,
          offset <- 0..(@points - 1) do
        {@metric, labels(index), index * 1_000.0 + offset, base + offset}
      end

    :ok = TimelessMetrics.write_batch(store, entries)

    day = div(now, 86_400) * 86_400
    rollup_start = day - 7 * 86_400

    rollup_entries =
      for index <- 0..5 do
        {@rollup_metric, %{"host" => "rollup"}, index + 0.25, rollup_start + index * 86_400 + 1}
      end

    :ok = TimelessMetrics.write_batch(store, rollup_entries)
    :ok = TimelessMetrics.write(store, @retention_metric, %{}, -1.0, timestamp: now - 7_200)
    :ok = TimelessMetrics.flush(store)
    :ok = TimelessMetrics.write(store, @retention_metric, %{}, 1.0, timestamp: now)
    :ok = TimelessMetrics.flush(store)
    :ok = TimelessMetrics.rollup(store)

    %{
      now: now,
      base: base,
      stop: base + @points - 1,
      rollup_start: rollup_start,
      rollup_stop: rollup_start + 3 * 86_400 - 1
    }
  end

  defp labels(index) do
    %{
      "host" => "device_#{index}",
      "env" => if(rem(index, 2) == 0, do: "prod", else: "dev"),
      "service" => "svc_#{rem(index, 16)}",
      "region" => if(rem(index, 4) == 0, do: "us-east", else: "us-west")
    }
  end

  defp capture_corpus(store, fixture) do
    operations = corpus_operations(store, fixture)

    Map.new(operations, fn {name, operation} ->
      {name, operation.()}
    end)
    |> Map.put(:operations, operations)
  end

  defp corpus_operations(store, fixture) do
    range = [from: fixture.base, to: fixture.stop]

    [
      exact: fn ->
        {:ok, points} = TimelessMetrics.query(store, @metric, labels(1), range)
        points
      end,
      narrow: fn ->
        {:ok, series} =
          TimelessMetrics.query_multi(store, @metric, %{"service" => "svc_1"}, range)

        canonical_series(series)
      end,
      wide: fn ->
        {:ok, series} = TimelessMetrics.query_multi(store, @metric, %{"env" => "prod"}, range)
        canonical_series(series)
      end,
      regex: fn ->
        {:ok, series} =
          TimelessMetrics.query_multi(
            store,
            @metric,
            [{"host", {:regex, "^device_1$"}}],
            range
          )

        canonical_series(series)
      end,
      aggregate: fn ->
        {:ok, series} =
          TimelessMetrics.query_aggregate_multi(
            store,
            @metric,
            %{"env" => "prod"},
            Keyword.merge(range, aggregate: :avg)
          )

        canonical_series(series)
      end,
      window: fn ->
        {:ok, series} =
          TimelessMetrics.query_aggregate_multi(
            store,
            @metric,
            %{"service" => "svc_1"},
            Keyword.merge(range, bucket: {10, :seconds}, aggregate: :sum)
          )

        canonical_series(series)
      end,
      latest: fn ->
        {:ok, series} = TimelessMetrics.latest_multi(store, @metric, %{"env" => "prod"})
        Enum.sort_by(series, &canonical_labels(&1.labels))
      end,
      discovery: fn ->
        {:ok, labels} =
          TimelessMetrics.StorageEngine.find_series(store, @metric, [
            {"region", {:not_equal, "us-east"}}
          ])

        Enum.sort_by(labels, &canonical_labels/1)
      end
    ]
  end

  defp reader_loop(expected, deadline, counters) do
    if before_deadline?(deadline) do
      Enum.each(expected.operations, fn {name, operation} ->
        case guarded_call(operation, counters) do
          {:ok, actual} ->
            ensure!(actual == Map.fetch!(expected, name), "#{name} corpus changed during soak")
            :atomics.add_get(counters, @reads, 1)

          :transient ->
            :atomics.add_get(counters, @read_transients, 1)
        end
      end)

      reader_loop(expected, deadline, counters)
    else
      :ok
    end
  end

  defp writer_loop(store, now, deadline, counters, batch) do
    if before_deadline?(deadline) do
      timestamp = now + rem(batch, 600)

      entries =
        for index <- 1..@batch_size do
          {@live_metric, %{"host" => "writer_#{index}"}, batch + index / 100.0, timestamp}
        end

      case guarded_call(fn -> TimelessMetrics.write_batch(store, entries) end, counters) do
        {:ok, :ok} -> :atomics.add_get(counters, @writes, length(entries))
        :transient -> :atomics.add_get(counters, @write_transients, length(entries))
      end

      Process.sleep(1)
      writer_loop(store, now, deadline, counters, batch + 1)
    else
      :ok
    end
  end

  defp maintenance_loop(store, backup_root, deadline, counters, iteration, last_backup) do
    if before_deadline?(deadline) do
      results = [
        flush: guarded_call(fn -> TimelessMetrics.flush(store) end, counters),
        retention: guarded_call(fn -> TimelessMetrics.enforce_retention(store) end, counters),
        compact: guarded_call(fn -> TimelessMetrics.StorageEngine.compact(store) end, counters),
        rollup: guarded_call(fn -> TimelessMetrics.rollup(store) end, counters)
      ]

      Enum.each(results, fn
        {_name, {:ok, :ok}} ->
          :atomics.add_get(counters, @maintenance, 1)

        {_name, {:ok, {:ok, _series, _chunks}}} ->
          :atomics.add_get(counters, @maintenance, 1)

        {_name, :transient} ->
          :ok

        {name, {:ok, {:error, reason}}} ->
          raise "#{name} failed during maintenance: #{reason}"
      end)

      last_backup =
        if rem(iteration, 3) == 0 do
          backup_dir = Path.join(backup_root, "backup_#{iteration}")

          case guarded_call(fn -> TimelessMetrics.backup(store, backup_dir) end, counters) do
            {:ok, {:ok, %{files: ["metrics.db"]}}} ->
              :atomics.add_get(counters, @backups, 1)
              backup_dir

            :transient ->
              last_backup
          end
        else
          last_backup
        end

      Process.sleep(250)
      maintenance_loop(store, backup_root, deadline, counters, iteration + 1, last_backup)
    else
      ensure!(is_binary(last_backup), "soak did not produce a successful online backup")
      last_backup
    end
  end

  defp restart_writer(store, delay, counters) do
    Process.sleep(delay)
    :atomics.put(counters, @restart_window, 1)
    writer_name = TimelessMetrics.LibsqlEngine.writer_name(store)
    old_writer = Process.whereis(writer_name)
    ensure!(is_pid(old_writer), "libSQL writer was not running before restart injection")
    monitor = Process.monitor(old_writer)
    Process.exit(old_writer, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^old_writer, :killed} -> :ok
    after
      5_000 -> raise "libSQL writer did not stop after restart injection"
    end

    new_writer = await_restarted_writer(writer_name, old_writer, 500)
    ensure!(is_pid(new_writer), "libSQL writer was not supervised back within five seconds")
    :atomics.add_get(counters, @restarts, 1)
    Process.sleep(100)
    :atomics.put(counters, @restart_window, 0)
  end

  defp await_restarted_writer(_name, _old_writer, 0), do: nil

  defp await_restarted_writer(name, old_writer, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old_writer ->
        pid

      _ ->
        Process.sleep(10)
        await_restarted_writer(name, old_writer, attempts - 1)
    end
  end

  defp guarded_call(operation, counters) do
    {:ok, operation.()}
  rescue
    error ->
      if :atomics.get(counters, @restart_window) == 1 do
        :transient
      else
        reraise error, __STACKTRACE__
      end
  catch
    :exit, reason ->
      if :atomics.get(counters, @restart_window) == 1 do
        :transient
      else
        exit(reason)
      end
  end

  defp verify_corpus!(expected) do
    Enum.each(expected.operations, fn {name, operation} ->
      ensure!(operation.() == Map.fetch!(expected, name), "#{name} corpus changed after reopen")
    end)
  end

  defp verify_rollup_and_retention!(store, fixture) do
    {:ok, buckets} =
      TimelessMetrics.query_daily(
        store,
        @rollup_metric,
        %{"host" => "rollup"},
        fixture.rollup_start,
        fixture.rollup_stop
      )

    ensure!(
      Enum.map(buckets, &{&1.bucket, &1.avg, &1.count}) ==
        for(
          index <- 0..2,
          do: {fixture.rollup_start + index * 86_400, index + 0.25, 1}
        ),
      "daily rollup changed during soak"
    )

    {:ok, retained} =
      TimelessMetrics.query(store, @retention_metric, %{}, from: 0, to: fixture.now)

    ensure!(
      retained == [{fixture.now, 1.0}],
      "retention did not prune only the expired point: #{inspect(retained)}"
    )
  end

  defp verify_backup!(backup_store, backup_dir, readers, expected, fixture) do
    {:ok, supervisor} = start_store(backup_store, backup_dir, readers)

    try do
      verify_corpus!(remap_operations(expected, backup_store, fixture))
      verify_rollup_and_retention!(backup_store, fixture)
    after
      Supervisor.stop(supervisor)
    end
  end

  defp remap_operations(expected, store, fixture) do
    Map.put(expected, :operations, corpus_operations(store, fixture))
  end

  defp canonical_series(series) do
    Enum.sort_by(series, fn item -> canonical_labels(item.labels) end)
  end

  defp canonical_labels(labels), do: Enum.sort(labels)

  defp before_deadline?(deadline),
    do: System.monotonic_time(:millisecond) < deadline

  defp print_summary(config, counters, data_dir, backup_dir) do
    IO.puts("# benchmark=libsql-mixed-soak")
    IO.puts("# seconds=#{config.seconds}")
    IO.puts("# readers=#{config.readers}")
    IO.puts("# writes=#{:atomics.get(counters, @writes)}")
    IO.puts("# write_transients=#{:atomics.get(counters, @write_transients)}")
    IO.puts("# reads=#{:atomics.get(counters, @reads)}")
    IO.puts("# read_transients=#{:atomics.get(counters, @read_transients)}")
    IO.puts("# maintenance_operations=#{:atomics.get(counters, @maintenance)}")
    IO.puts("# backups=#{:atomics.get(counters, @backups)}")
    IO.puts("# writer_restarts=#{:atomics.get(counters, @restarts)}")
    IO.puts("# primary_bytes=#{directory_bytes(data_dir)}")
    IO.puts("# backup_bytes=#{directory_bytes(backup_dir)}")
    IO.puts("status,ok")
  end

  defp directory_bytes(path) do
    path
    |> File.ls!()
    |> Enum.map(fn entry -> File.stat!(Path.join(path, entry)).size end)
    |> Enum.sum()
  end

  defp parse_args(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args, strict: [seconds: :integer, readers: :integer])

    ensure!(positional == [] and invalid == [], usage())
    config = %{seconds: Keyword.get(opts, :seconds, 30), readers: Keyword.get(opts, :readers, 8)}
    ensure!(config.seconds >= 4 and config.readers >= 2, usage())
    config
  end

  defp usage,
    do: "usage: mix run bench/libsql_soak.exs [--seconds N>=4] [--readers N>=2]"

  defp ensure!(true, _message), do: :ok
  defp ensure!(false, message), do: raise(message)
end

TimelessMetrics.LibsqlSoak.run(System.argv())
