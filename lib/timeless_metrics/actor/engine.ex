defmodule TimelessMetrics.Actor.Engine do
  @moduledoc """
  Public API dispatch for the actor engine.

  Stateless module that routes all operations to the appropriate
  SeriesServer processes via the SeriesManager.
  """

  require Logger

  alias TimelessMetrics.Actor.SeriesManager
  alias TimelessMetrics.Stats

  @doc "Write a single metric point."
  def write(store, metric_name, labels, value, opts) do
    Stats.incr_writes(store)
    Stats.add_points(store, 1)
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))
    %{index: index, manager: manager} = :persistent_term.get({SeriesManager, store})

    case :ets.lookup_element(index, {metric_name, labels}, 3, nil) do
      nil ->
        {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
        send(pid, {:write, timestamp, value})

      pid ->
        send(pid, {:write, timestamp, value})
    end
  end

  @doc "Write a batch of metric points."
  def write_batch(store, entries) when is_list(entries) do
    Stats.incr_writes(store)
    Stats.add_points(store, length(entries))
    %{manager: manager} = :persistent_term.get({SeriesManager, store})

    entries
    |> Enum.group_by(
      fn
        {mn, l, _v} -> {mn, l}
        {mn, l, _v, _ts} -> {mn, l}
      end,
      fn
        {_mn, _l, v} -> {System.os_time(:second), v}
        {_mn, _l, v, ts} -> {ts, v}
      end
    )
    |> Enum.each(fn {{metric_name, labels}, batch} ->
      {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
      send(pid, {:write_batch, batch})
    end)

    :ok
  end

  def write_batch(store, groups) when is_map(groups) do
    count = groups |> Map.values() |> Enum.reduce(0, &(length(&1) + &2))
    Stats.incr_writes(store)
    Stats.add_points(store, count)
    %{manager: manager} = :persistent_term.get({SeriesManager, store})

    Enum.each(groups, fn {{metric_name, labels}, batch} ->
      {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
      send(pid, {:write_batch, batch})
    end)

    :ok
  end

  @doc """
  Write entries directly, one per unique series. Skips group_by for
  collection-cycle workloads where each entry targets a different series.

  Hot path is inlined: single ETS lookup + raw send per entry, no
  intermediate allocations. Falls back to full `get_or_start` on miss.

  Entries: `[{metric_name, labels, value} | {metric_name, labels, value, ts}]`
  """
  def write_each(store, entries) do
    Stats.incr_writes(store)
    Stats.add_points(store, length(entries))
    %{index: index, manager: manager} = :persistent_term.get({SeriesManager, store})

    Enum.each(entries, fn
      {metric_name, labels, value} ->
        case :ets.lookup_element(index, {metric_name, labels}, 3, nil) do
          nil ->
            {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
            send(pid, {:write, System.os_time(:second), value})

          pid ->
            send(pid, {:write, System.os_time(:second), value})
        end

      {metric_name, labels, value, ts} ->
        case :ets.lookup_element(index, {metric_name, labels}, 3, nil) do
          nil ->
            {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
            send(pid, {:write, ts, value})

          pid ->
            send(pid, {:write, ts, value})
        end
    end)

    :ok
  end

  @doc """
  Resolve a series to its PID for use with `write_resolved/3`.

  Cache the result for repeated writes to the same series. If the
  process dies, the PID becomes stale — re-resolve when needed.
  """
  def resolve_series(store, metric_name, labels) do
    %{index: index, manager: manager} = :persistent_term.get({SeriesManager, store})

    case :ets.lookup_element(index, {metric_name, labels}, 3, nil) do
      nil ->
        {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
        pid

      pid ->
        pid
    end
  end

  @doc """
  Write directly to a pre-resolved PID. Zero lookup cost.

  Use `resolve_series/3` to get the PID. This is the fastest write path
  for hot loops where the same series are written repeatedly.
  """
  def write_resolved(pid, value, timestamp) do
    send(pid, {:write, timestamp, value})
    :ok
  end

  @doc "Query raw points for a single series."
  def query(store, metric_name, labels, opts) do
    Stats.incr_queries(store)
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    manager = manager_name(store)
    {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
    query_raw_single(store, pid, from, to)
  end

  @doc "Query raw points across multiple series matching a label filter."
  def query_multi(store, metric_name, label_filter, opts) do
    Stats.incr_queries(store)
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    # Fast path: exact labels → try O(1) lookups before scan
    if exact_label_filter?(label_filter) do
      %{index: index} = :persistent_term.get({SeriesManager, store})

      case :ets.lookup(index, {metric_name, label_filter}) do
        [{_key, _series_id, pid}] when is_pid(pid) and is_atom(pid) == false ->
          query_multi_direct(pid, label_filter, from, to, store, metric_name)

        _ ->
          manager = manager_name(store)

          case SeriesManager.find_series(manager, metric_name, label_filter) do
            [{_id, labels, pid}] ->
              query_multi_direct(pid, labels, from, to, store, metric_name)

            [] ->
              {:ok, []}

            matching ->
              query_multi_fanout(matching, from, to)
          end
      end
    else
      query_multi_scan(store, metric_name, label_filter, from, to)
    end
  end

  defp query_multi_direct(pid, labels, from, to, store, _metric_name) do
    try do
      {:ok, points} = query_raw_single(store, pid, from, to)

      if points == [],
        do: {:ok, []},
        else: {:ok, [%{labels: labels, points: points}]}
    catch
      :exit, reason ->
        Logger.error("TimelessMetrics: query_multi direct call crashed: #{inspect(reason)}")
        {:ok, []}
    end
  end

  # Try ETS read buffer for raw points, fall back to GenServer.call.
  # ETS retains points through compression (trimmed only by raw retention),
  # so this fast path serves most queries within the retention window.
  # Falls back to GenServer.call only when ETS has no data for the range
  # (e.g., series just started, or query extends beyond retention).
  defp query_raw_single(store, pid, from, to) do
    %{read_buffer: read_buffer} = :persistent_term.get({SeriesManager, store})

    if read_buffer do
      case Registry.keys(:"#{store}_actor_registry", pid) do
        [series_id] ->
          points = read_buffer_points(read_buffer, series_id, from, to)

          if points != [] do
            {:ok, points}
          else
            GenServer.call(pid, {:query_raw, from, to})
          end

        _ ->
          GenServer.call(pid, {:query_raw, from, to})
      end
    else
      GenServer.call(pid, {:query_raw, from, to})
    end
  end

  defp query_multi_fanout(matching, from, to) do
    results =
      matching
      |> Task.async_stream(
        fn {_id, labels, pid} ->
          {:ok, points} = GenServer.call(pid, {:query_raw, from, to})
          %{labels: labels, points: points}
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, result} ->
          [result]

        {:exit, reason} ->
          Logger.error("TimelessMetrics: query_multi task crashed: #{inspect(reason)}")
          []
      end)
      |> Enum.reject(fn %{points: pts} -> pts == [] end)

    {:ok, results}
  end

  defp query_multi_scan(store, metric_name, label_filter, from, to) do
    manager = manager_name(store)
    matching = SeriesManager.find_series(manager, metric_name, label_filter)

    case matching do
      [{_id, labels, pid}] ->
        query_multi_direct(pid, labels, from, to, store, metric_name)

      [] ->
        {:ok, []}

      _ ->
        query_multi_fanout(matching, from, to)
    end
  end

  @doc "Query with time-bucket aggregation for a single series."
  def query_aggregate(store, metric_name, labels, opts) do
    Stats.incr_queries(store)
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = Keyword.fetch!(opts, :bucket)
    agg_fn = Keyword.fetch!(opts, :aggregate)
    manager = manager_name(store)
    {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
    query_aggregate_single(store, pid, from, to, bucket, agg_fn)
  end

  @doc "Query with aggregation across multiple series matching a label filter."
  def query_aggregate_multi(store, metric_name, label_filter, opts) do
    Stats.incr_queries(store)
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = Keyword.fetch!(opts, :bucket)
    agg_fn = Keyword.fetch!(opts, :aggregate)
    transform = Keyword.get(opts, :transform)

    # Fast path: exact label filter → try O(1) lookups before falling to scan
    if exact_label_filter?(label_filter) and map_size(label_filter) > 0 do
      %{index: index} = :persistent_term.get({SeriesManager, store})

      # Try 1: full key match (query labels == stored labels)
      case :ets.lookup(index, {metric_name, label_filter}) do
        [{_key, _series_id, pid}] when is_pid(pid) and is_atom(pid) == false ->
          query_aggregate_multi_direct(
            pid,
            label_filter,
            from,
            to,
            bucket,
            agg_fn,
            transform,
            store,
            metric_name
          )

        _ ->
          # Try 2: partial label match via label index
          manager = manager_name(store)

          case SeriesManager.find_series(manager, metric_name, label_filter) do
            [{_id, labels, pid}] ->
              query_aggregate_multi_direct(
                pid,
                labels,
                from,
                to,
                bucket,
                agg_fn,
                transform,
                store,
                metric_name
              )

            [] ->
              {:ok, []}

            matching ->
              # Multiple matches — use the full fan-out path
              query_aggregate_multi_fanout(store, matching, from, to, bucket, agg_fn, transform)
          end
      end
    else
      query_aggregate_multi_scan(
        store,
        metric_name,
        label_filter,
        from,
        to,
        bucket,
        agg_fn,
        transform
      )
    end
  end

  # Direct query for a single series — tries ETS read buffer first, falls back to GenServer.call
  defp query_aggregate_multi_direct(
         pid,
         labels,
         from,
         to,
         bucket,
         agg_fn,
         transform,
         store,
         _metric_name
       ) do
    try do
      {:ok, buckets} = query_aggregate_single(store, pid, from, to, bucket, agg_fn)

      case TimelessMetrics.Transform.apply(buckets, transform) do
        [] -> {:ok, []}
        data -> {:ok, [%{labels: labels, data: data}]}
      end
    catch
      :exit, reason ->
        Logger.error(
          "TimelessMetrics: query_aggregate_multi direct call crashed: #{inspect(reason)}"
        )

        {:ok, []}
    end
  end

  # Try ETS read buffer for aggregate queries, fall back to GenServer.call.
  # ETS retains points through compression, so the fast path covers most
  # queries within the raw retention window without touching the mailbox.
  defp query_aggregate_single(store, pid, from, to, bucket, agg_fn) do
    %{read_buffer: read_buffer} = :persistent_term.get({SeriesManager, store})

    if read_buffer do
      case Registry.keys(:"#{store}_actor_registry", pid) do
        [series_id] ->
          raw_points = read_buffer_points(read_buffer, series_id, from, to)

          if raw_points != [] do
            Stats.incr_query_fast_path(store)
            bucket_seconds = TimelessMetrics.Actor.Aggregation.bucket_to_seconds(bucket)

            buckets =
              TimelessMetrics.Actor.Aggregation.bucket_points(raw_points, bucket_seconds, agg_fn)

            {:ok, buckets}
          else
            Stats.incr_query_slow_path(store)
            GenServer.call(pid, {:query_aggregate, from, to, bucket, agg_fn})
          end

        _ ->
          Stats.incr_query_slow_path(store)
          GenServer.call(pid, {:query_aggregate, from, to, bucket, agg_fn})
      end
    else
      Stats.incr_query_slow_path(store)
      GenServer.call(pid, {:query_aggregate, from, to, bucket, agg_fn})
    end
  end

  # Read raw points from the ETS read buffer — no mailbox, no blocking.
  # Uses :ets.select on an ordered_set with composite key {series_id, ts, seq}
  # for efficient range scans. Results are pre-sorted by timestamp.
  defp read_buffer_points(read_buffer, series_id, from, to) do
    :ets.select(read_buffer, [
      {{{series_id, :"$1", :_}, :"$2"}, [{:andalso, {:>=, :"$1", from}, {:"=<", :"$1", to}}],
       [{{:"$1", :"$2"}}]}
    ])
  end

  # Fan-out to multiple series via Task.async_stream
  defp query_aggregate_multi_fanout(store, matching, from, to, bucket, agg_fn, transform) do
    results =
      matching
      |> Task.async_stream(
        fn {_id, labels, pid} ->
          Stats.incr_query_slow_path(store)
          {:ok, buckets} = GenServer.call(pid, {:query_aggregate, from, to, bucket, agg_fn})

          case TimelessMetrics.Transform.apply(buckets, transform) do
            [] -> nil
            data -> %{labels: labels, data: data}
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, nil} ->
          []

        {:ok, result} ->
          [result]

        {:exit, reason} ->
          Logger.error("TimelessMetrics: query_aggregate_multi task crashed: #{inspect(reason)}")
          []
      end)

    {:ok, results}
  end

  defp query_aggregate_multi_scan(
         store,
         metric_name,
         label_filter,
         from,
         to,
         bucket,
         agg_fn,
         transform
       ) do
    manager = manager_name(store)
    matching = SeriesManager.find_series(manager, metric_name, label_filter)

    case matching do
      [{_id, labels, pid}] ->
        query_aggregate_multi_direct(
          pid,
          labels,
          from,
          to,
          bucket,
          agg_fn,
          transform,
          store,
          metric_name
        )

      [] ->
        {:ok, []}

      _ ->
        query_aggregate_multi_fanout(store, matching, from, to, bucket, agg_fn, transform)
    end
  end

  # Returns true if the label filter contains only exact string matches
  # (no regex, not_regex, not_equal, or other special operators)
  defp exact_label_filter?(filter) when map_size(filter) == 0, do: false

  defp exact_label_filter?(filter) do
    Enum.all?(filter, fn
      {_k, v} when is_binary(v) -> true
      _ -> false
    end)
  end

  @doc "Query with cross-series aggregation, grouping by label key."
  def query_aggregate_grouped(store, metric_name, label_filter, opts) do
    group_by = Keyword.fetch!(opts, :group_by)
    cross_agg = Keyword.get(opts, :cross_series_aggregate, :max)

    group_by_keys =
      case group_by do
        keys when is_list(keys) -> keys
        key when is_binary(key) -> [key]
      end

    {:ok, results} = query_aggregate_multi(store, metric_name, label_filter, opts)

    grouped =
      results
      |> Enum.group_by(fn %{labels: labels} -> Map.take(labels, group_by_keys) end)
      |> Enum.map(fn {group_labels, series_list} ->
        all_data = Enum.map(series_list, fn %{data: data} -> data end)
        merged = TimelessMetrics.merge_series_data(all_data, cross_agg)
        %{group: group_labels, data: merged}
      end)

    {:ok, grouped}
  end

  @doc "Query with aggregation across multiple metric names."
  def query_aggregate_multi_metrics(store, metric_names, label_filter, opts) do
    Stats.incr_queries(store)
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket = Keyword.fetch!(opts, :bucket)
    agg_fn = Keyword.fetch!(opts, :aggregate)
    transform = Keyword.get(opts, :transform)
    manager = manager_name(store)

    results =
      metric_names
      |> Enum.flat_map(fn metric_name ->
        matching = SeriesManager.find_series(manager, metric_name, label_filter)

        matching
        |> Task.async_stream(
          fn {_id, labels, pid} ->
            {:ok, buckets} = GenServer.call(pid, {:query_aggregate, from, to, bucket, agg_fn})

            case TimelessMetrics.Transform.apply(buckets, transform) do
              [] -> nil
              data -> %{metric: metric_name, labels: labels, data: data}
            end
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false,
          timeout: :infinity
        )
        |> Enum.flat_map(fn
          {:ok, nil} ->
            []

          {:ok, result} ->
            [result]

          {:exit, reason} ->
            Logger.error(
              "TimelessMetrics: query_aggregate_multi_metrics task crashed: #{inspect(reason)}"
            )

            []
        end)
      end)

    {:ok, results}
  end

  @doc "Query with cross-series aggregation across multiple metrics, with group-by."
  def query_aggregate_grouped_metrics(store, metric_names, label_filter, opts) do
    group_by = Keyword.fetch!(opts, :group_by)
    cross_agg = Keyword.get(opts, :cross_series_aggregate, :max)

    group_by_keys =
      case group_by do
        keys when is_list(keys) -> keys
        key when is_binary(key) -> [key]
      end

    {:ok, results} = query_aggregate_multi_metrics(store, metric_names, label_filter, opts)

    grouped =
      results
      |> Enum.group_by(fn %{labels: labels, metric: metric} ->
        group_map = Map.take(labels, group_by_keys -- ["__name__"])

        if "__name__" in group_by_keys do
          Map.put(group_map, "__name__", metric)
        else
          group_map
        end
      end)
      |> Enum.map(fn {group_labels, series_list} ->
        all_data = Enum.map(series_list, fn %{data: data} -> data end)
        merged = TimelessMetrics.merge_series_data(all_data, cross_agg)
        %{group: group_labels, data: merged}
      end)

    {:ok, grouped}
  end

  @doc "Query with aggregation and threshold filtering."
  def query_aggregate_multi_filtered(store, metric_name, label_filter, opts) do
    threshold = Keyword.get(opts, :threshold)
    {:ok, results} = query_aggregate_multi(store, metric_name, label_filter, opts)

    filtered =
      case threshold do
        nil ->
          results

        {:gt, threshold_val} ->
          Enum.filter(results, fn %{data: data} ->
            Enum.any?(data, fn {_ts, val} -> val > threshold_val end)
          end)

        {:lt, threshold_val} ->
          Enum.filter(results, fn %{data: data} ->
            Enum.any?(data, fn {_ts, val} -> val < threshold_val end)
          end)
      end

    {:ok, filtered}
  end

  @doc "Sort results by a value function and take top N."
  def top_n(results, n, order_fn \\ &last_value/1) do
    results
    |> Enum.sort_by(order_fn, :desc)
    |> Enum.take(n)
  end

  defp last_value(%{data: []}), do: 0.0
  defp last_value(%{data: data}), do: data |> List.last() |> elem(1)

  @doc "Merge small compressed blocks in all series processes."
  def merge_now(store) do
    manager = manager_name(store)
    SeriesManager.merge_all(manager)
  end

  @doc "Force flush all series to disk."
  def flush(store) do
    manager = manager_name(store)
    SeriesManager.flush_all(manager)
  end

  @doc "Get the latest value for a series."
  def latest(store, metric_name, labels) do
    manager = manager_name(store)
    {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels)
    GenServer.call(pid, :latest)
  end

  @doc "Get the latest value for all series matching a metric and label filter."
  def latest_multi(store, metric_name, label_filter) do
    manager = manager_name(store)
    matching = SeriesManager.find_series(manager, metric_name, label_filter)

    results =
      matching
      |> Task.async_stream(
        fn {_id, labels, pid} ->
          case GenServer.call(pid, :latest) do
            {:ok, {ts, val}} -> %{labels: labels, timestamp: ts, value: val}
            {:ok, nil} -> nil
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, nil} ->
          []

        {:ok, result} ->
          [result]

        {:exit, reason} ->
          Logger.error("TimelessMetrics: latest_multi task crashed: #{inspect(reason)}")
          []
      end)

    {:ok, results}
  end

  @doc "List all metric names."
  def list_metrics(store) do
    manager = manager_name(store)
    {:ok, SeriesManager.list_metrics(manager)}
  end

  @doc "List all series for a metric."
  def list_series(store, metric_name) do
    manager = manager_name(store)
    {:ok, SeriesManager.list_series(manager, metric_name)}
  end

  @doc "List distinct values for a label key."
  def label_values(store, metric_name, label_key) do
    manager = manager_name(store)
    {:ok, SeriesManager.label_values(manager, metric_name, label_key)}
  end

  @doc "Collect stats from all series processes."
  def info(store) do
    registry = :"#{store}_actor_registry"
    db = :"#{store}_db"
    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir})

    # Flush pending series registrations before counting
    TimelessMetrics.Actor.SeriesWriter.flush_sync(:"#{store}_series_writer")

    # Series count from DB
    {:ok, [[series_count]]} = TimelessMetrics.DB.read(db, "SELECT COUNT(*) FROM series")

    # Daily rollup row count
    {:ok, [[daily_rollup_rows]]} = TimelessMetrics.DB.read(db, "SELECT COUNT(*) FROM tier_daily")

    # Fan out to all series for stats
    pids = Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    stats =
      pids
      |> Task.async_stream(
        fn {_series_id, pid} ->
          try do
            GenServer.call(pid, :stats, :infinity)
          catch
            :exit, _ -> nil
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, nil} -> []
        {:ok, s} -> [s]
      end)

    # Count total points from blocks (need point_count) + raw
    {block_count, raw_buffer_points, compressed_bytes, oldest_ts, newest_ts} =
      Enum.reduce(stats, {0, 0, 0, nil, nil}, fn s, {bc, rp, cb, oldest, newest} ->
        merged_oldest =
          cond do
            oldest == nil -> s.oldest_ts
            s.oldest_ts == nil -> oldest
            true -> min(oldest, s.oldest_ts)
          end

        merged_newest =
          cond do
            newest == nil -> s.newest_ts
            s.newest_ts == nil -> newest
            true -> max(newest, s.newest_ts)
          end

        {bc + s.block_count, rp + s.raw_count, cb + s.compressed_bytes, merged_oldest,
         merged_newest}
      end)

    # Sum .dat file sizes + DB size for storage_bytes
    db_path = TimelessMetrics.DB.db_path(db)

    db_size =
      case File.stat(db_path) do
        {:ok, %{size: s}} -> s
        _ -> 0
      end

    actor_dir = Path.join(data_dir, "actor")

    dat_size =
      case File.ls(actor_dir) do
        {:ok, files} ->
          files
          |> Enum.filter(&String.ends_with?(&1, ".dat"))
          |> Enum.reduce(0, fn f, acc ->
            case File.stat(Path.join(actor_dir, f)) do
              {:ok, %{size: s}} -> acc + s
              _ -> acc
            end
          end)

        _ ->
          0
      end

    storage_bytes = db_size + dat_size

    # Total points: we don't have per-block point_count in the stats,
    # so use block_count (each block ~ block_size points) as approximation.
    # Actually we need to fan out differently. For now, count total from
    # raw_buffer_points. For blocks we'd need point_count.
    # Let's add it — query each series for their total point count.
    total_block_points =
      pids
      |> Task.async_stream(
        fn {_sid, pid} ->
          try do
            s = GenServer.call(pid, :state, :infinity)

            s.blocks
            |> :queue.to_list()
            |> Enum.reduce(0, fn b, acc -> acc + b.point_count end)
          catch
            :exit, _ -> 0
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.reduce(0, fn
        {:ok, n}, acc -> acc + n
        {:exit, _}, acc -> acc
      end)

    all_points = total_block_points + raw_buffer_points

    bytes_per_point =
      if all_points > 0, do: Float.round(compressed_bytes / all_points, 2), else: 0.0

    process_count = length(pids)

    index_ets = :"#{store}_actor_index"

    index_ets_bytes =
      case :ets.info(index_ets, :memory) do
        :undefined -> 0
        words -> words * :erlang.system_info(:wordsize)
      end

    %{
      series_count: series_count,
      total_points: all_points,
      block_count: block_count,
      raw_buffer_points: raw_buffer_points,
      compressed_bytes: compressed_bytes,
      bytes_per_point: bytes_per_point,
      storage_bytes: storage_bytes,
      oldest_timestamp: oldest_ts,
      newest_timestamp: newest_ts,
      process_count: process_count,
      index_ets_bytes: index_ets_bytes,
      daily_rollup_rows: daily_rollup_rows,
      db_path: db_path
    }
  end

  @doc "Create a consistent backup of the actor engine data."
  def backup(store, target_dir) do
    manager = manager_name(store)
    db = :"#{store}_db"
    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir})

    # 1. Flush pending series registrations + dirty state to disk
    TimelessMetrics.Actor.SeriesWriter.flush_sync(:"#{store}_series_writer")
    SeriesManager.flush_all(manager)

    File.mkdir_p!(target_dir)

    # 2. VACUUM INTO for the SQLite database
    main_target = Path.join(target_dir, "metrics.db")
    {:ok, _} = TimelessMetrics.DB.backup(db, main_target)

    # 3. Copy actor/*.dat files
    actor_src = Path.join(data_dir, "actor")
    actor_dst = Path.join(target_dir, "actor")

    dat_size =
      case File.ls(actor_src) do
        {:ok, files} ->
          dat_files = Enum.filter(files, &String.ends_with?(&1, ".dat"))
          File.mkdir_p!(actor_dst)

          Enum.reduce(dat_files, 0, fn f, acc ->
            File.cp!(Path.join(actor_src, f), Path.join(actor_dst, f))

            case File.stat(Path.join(actor_dst, f)) do
              {:ok, %{size: s}} -> acc + s
              _ -> acc
            end
          end)

        _ ->
          0
      end

    main_size = File.stat!(main_target).size
    total_bytes = main_size + dat_size

    files =
      case File.ls(actor_dst) do
        {:ok, fs} -> ["metrics.db" | Enum.map(fs, &Path.join("actor", &1))]
        _ -> ["metrics.db"]
      end

    {:ok, %{path: target_dir, files: files, total_bytes: total_bytes}}
  end

  # --- Text series API ---

  @doc "Write a single text metric point."
  def write_text(store, metric_name, labels, value, opts \\ []) do
    Stats.incr_writes(store)
    Stats.add_points(store, 1)
    timestamp = Keyword.get(opts, :timestamp, System.os_time(:second))
    %{index: index, manager: manager} = :persistent_term.get({SeriesManager, store})

    case :ets.lookup_element(index, {metric_name, labels}, 3, nil) do
      nil ->
        {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels, :text)
        send(pid, {:write_text, timestamp, value})

      pid ->
        send(pid, {:write_text, timestamp, value})
    end
  end

  @doc "Write a batch of text metric points."
  def write_text_batch(store, entries) do
    Stats.incr_writes(store)
    Stats.add_points(store, length(entries))
    %{manager: manager} = :persistent_term.get({SeriesManager, store})

    entries
    |> Enum.group_by(
      fn
        {mn, l, _v} -> {mn, l}
        {mn, l, _v, _ts} -> {mn, l}
      end,
      fn
        {_mn, _l, v} -> {System.os_time(:second), v}
        {_mn, _l, v, ts} -> {ts, v}
      end
    )
    |> Enum.each(fn {{metric_name, labels}, batch} ->
      {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels, :text)
      send(pid, {:write_text_batch, batch})
    end)

    :ok
  end

  @doc "Query text points for a single series."
  def query_text(store, metric_name, labels, opts \\ []) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    manager = manager_name(store)
    {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels, :text)
    GenServer.call(pid, {:query_raw, from, to})
  end

  @doc "Query text points across multiple series matching a label filter."
  def query_text_multi(store, metric_name, label_filter, opts \\ []) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    manager = manager_name(store)
    matching = SeriesManager.find_series(manager, metric_name, label_filter)

    results =
      matching
      |> Task.async_stream(
        fn {_id, labels, pid} ->
          {:ok, points} = GenServer.call(pid, {:query_raw, from, to})
          %{labels: labels, points: points}
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, result} ->
          [result]

        {:exit, reason} ->
          Logger.error("TimelessMetrics: query_text_multi task crashed: #{inspect(reason)}")
          []
      end)
      |> Enum.reject(fn %{points: pts} -> pts == [] end)

    {:ok, results}
  end

  @doc "Get the latest text value for a series."
  def latest_text(store, metric_name, labels) do
    manager = manager_name(store)
    {_id, pid} = SeriesManager.get_or_start(manager, metric_name, labels, :text)
    GenServer.call(pid, :latest)
  end

  @doc "Query pre-computed daily rollup data."
  def query_daily(store, metric_name, labels, from, to) do
    manager = manager_name(store)
    {series_id, _pid} = SeriesManager.get_or_start(manager, metric_name, labels)
    db = :"#{store}_db"

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT bucket, avg, min, max, count, sum, last FROM tier_daily WHERE series_id = ?1 AND bucket >= ?2 AND bucket <= ?3 ORDER BY bucket",
        [series_id, from, to]
      )

    results =
      Enum.map(rows, fn [bucket, avg, min, max, count, sum, last] ->
        %{bucket: bucket, avg: avg, min: min, max: max, count: count, sum: sum, last: last}
      end)

    {:ok, results}
  end

  defp manager_name(store), do: :"#{store}_actor_manager"
end
