defmodule TimelessMetrics.LibsqlEngine do
  @moduledoc """
  SQLite/libSQL-backed metrics engine.

  Compressed points live in the `metric_samples` virtual table and its shadow
  tables inside the store's existing `metrics.db`. A single writer connection
  preserves SQLite's write discipline while a small reader pool serves raw
  queries and discovery concurrently.
  """

  use GenServer

  @table "metric_samples"
  @flush_interval :timer.seconds(10)
  @ingest_transaction_ms 5
  @ingest_transaction_max 256
  @native_scalar_aggregates [:avg, :sum, :min, :max, :count]
  @native_bucket_aggregates [:avg, :sum, :min, :max, :count]
  @native_window_max_points 1_000_000
  @aggregate_frame_module "timeless_aggregate_frame"
  @latest_frame_module "timeless_latest_frame"

  # -- Public storage API ---------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: writer_name(Keyword.fetch!(opts, :store)))
  end

  def writer_name(store), do: :"#{store}_libsql_engine"
  def reader_name(store, index), do: :"#{store}_libsql_reader_#{index}"

  @doc false
  def raw_batches_sql do
    "SELECT series_id, labels, points " <>
      "FROM timeless_raw_batches('#{@table}', ?1, ?2, ?3, ?4)"
  end

  @doc false
  def raw_points_sql do
    "SELECT series_id, points " <>
      "FROM timeless_raw_batches('#{@table}', ?1, ?2, ?3, ?4)"
  end

  @doc false
  def raw_points_by_id_sql do
    "SELECT series_id, points " <>
      "FROM timeless_raw_batches('#{@table}', ?1, NULL, ?2, ?3) " <>
      "WHERE series_id = ?4"
  end

  @doc false
  def raw_frame_sql do
    "SELECT frame FROM timeless_raw_frame('#{@table}', ?1, ?2, ?3, ?4)"
  end

  @doc false
  def aggregate_frame_sql do
    "SELECT frame " <>
      "FROM timeless_aggregate_frame('#{@table}', ?1, ?2, ?3, ?4, ?5)"
  end

  @doc false
  def latest_frame_sql do
    "SELECT frame FROM timeless_latest_frame('#{@table}', ?1, ?2, ?3, ?4)"
  end

  @doc false
  def detect_query_frame_features(conn) do
    case TimelessMetrics.DB.execute(
           conn,
           "SELECT name FROM pragma_module_list " <>
             "WHERE name IN ('#{@aggregate_frame_module}', '#{@latest_frame_module}')",
           []
         ) do
      {:ok, rows} -> MapSet.new(rows, fn [name] -> name end)
      {:error, _reason} -> MapSet.new()
    end
  end

  @doc false
  def query_frame_features(store) do
    :persistent_term.get({__MODULE__, store, :query_frame_features}, MapSet.new())
  end

  @doc false
  def aggregate_sql do
    "SELECT series_id, value " <>
      "FROM timeless_aggregate('#{@table}', ?1, ?2, ?3, ?4, ?5)"
  end

  @doc false
  def aggregate_by_id_sql do
    "SELECT series_id, value " <>
      "FROM timeless_aggregate('#{@table}', ?1, NULL, ?2, ?3, ?4) " <>
      "WHERE series_id = ?5"
  end

  @doc false
  def latest_sql do
    "SELECT series_id, ts, value " <>
      "FROM timeless_latest('#{@table}', ?1, ?2, ?3, ?4)"
  end

  @doc false
  def latest_by_id_sql do
    "SELECT series_id, ts, value " <>
      "FROM timeless_latest('#{@table}', ?1, NULL, ?2, ?3) " <>
      "WHERE series_id = ?4"
  end

  @doc false
  def window_sql do
    "SELECT labels, ts, value " <>
      "FROM timeless_window('#{@table}', ?1, ?2, ?3, ?4, ?5, ?6, ?7)"
  end

  @doc false
  def window_batches_sql do
    "SELECT series_id, buckets " <>
      "FROM timeless_window_batches('#{@table}', ?1, ?2, ?3, ?4, ?5, ?6, ?7)"
  end

  @doc false
  def rollup_batches_sql do
    "SELECT series_id, buckets " <>
      "FROM timeless_rollup_batches('#{@table}', ?1, ?2, ?3, ?4, ?5)"
  end

  def write(store, metric, labels, value, timestamp) do
    write_batch(store, [{metric, labels, value, timestamp}])
  end

  def write_batch(_store, []), do: :ok

  def write_batch(store, entries) do
    now = System.os_time(:second)

    normalized =
      Enum.map(entries, fn
        {metric, labels, value} -> {metric, Map.new(labels), now, value}
        {metric, labels, value, ts} -> {metric, Map.new(labels), ts, value}
      end)

    cache = cache_ref(store)

    case cached_resolved_entries(cache, normalized) do
      {:ok, resolved} ->
        with {:ok, _rows} <- insert_value(store, {:blob, encode_resolved_batch(resolved)}) do
          :ok
        end

      :miss ->
        keys =
          normalized
          |> Enum.map(fn {metric, labels, _ts, _value} -> {metric, labels} end)
          |> Enum.uniq()

        with {:ok, _rows} <- insert_value(store, {:blob, encode_named_batch(normalized)}) do
          hydrate_series_cache(store, cache, keys)
          :ok
        end
    end
  end

  def resolve_series(store, metric, labels) do
    key = {metric, labels}
    cache = cache_ref(store)

    case :ets.lookup(cache, key) do
      [{^key, sid}] ->
        {:ok, sid}

      [] ->
        labels_json = encode_json(labels)

        case GenServer.call(writer_name(store), {:resolve, metric, labels_json}, :infinity) do
          {:ok, [[sid]]} ->
            true = cache_series(cache, key, sid)
            {:ok, sid}

          {:ok, rows} ->
            {:error, "series resolution returned #{inspect(rows)}"}

          {:error, _} = error ->
            error
        end
    end
  end

  def resolve_series_batch(store, pairs) do
    pairs
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn {metric, labels} = key, {:ok, acc} ->
      case resolve_series(store, metric, labels) do
        {:ok, sid} -> {:cont, {:ok, Map.put(acc, key, sid)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  def write_resolved(store, sid, value, timestamp) do
    with {:ok, _rows} <-
           insert_value(store, {:blob, encode_resolved_batch([{sid, timestamp, value}])}) do
      :ok
    end
  end

  def ingest_prometheus(store, body, default_ts \\ nil) do
    now = default_ts || System.os_time(:second)
    {entries, errors} = TimelessMetrics.PrometheusNif.parse(body)

    normalized =
      Enum.map(entries, fn {metric, labels, value, ts} ->
        timestamp = if ts == 0, do: now, else: div(ts, 1_000)
        {metric, Map.new(labels), value, timestamp}
      end)

    case write_batch(store, normalized) do
      :ok -> {:ok, length(normalized), errors}
      {:error, _} = error -> error
    end
  end

  def flush(store), do: command(store, "flush")
  def rollup(store), do: command(store, "rollup")

  def compact(store, _cutoff \\ :all) do
    case command(store, "compact") do
      :ok -> {:ok, 0, 0}
      {:error, _} = error -> error
    end
  end

  def delete_before(store, cutoff), do: command(store, "prune:#{cutoff}")

  def query_raw(store, metric, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    with {:ok, rows} <- raw_point_rows_exact(store, metric, labels, from, to) do
      point_blobs = Enum.map(rows, fn [_sid, point_blob] -> point_blob end)

      points =
        case point_blobs do
          [] ->
            []

          [point_blob] ->
            decode_point_batch(point_blob)

          multiple ->
            multiple
            |> Enum.flat_map(&decode_point_batch/1)
            |> Enum.sort_by(&elem(&1, 0))
        end

      {:ok, points}
    end
  end

  def query_multi(store, metric, label_filter, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))

    raw_frame_series(store, metric, label_filter, from, to)
  end

  def query_aggregate(store, metric, labels, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket_seconds = bucket_to_seconds(opts[:bucket])
    agg = Keyword.get(opts, :aggregate, :avg)

    cond do
      bucket_seconds == nil and agg in @native_scalar_aggregates ->
        with {:ok, rows} <- aggregate_rows_exact(store, metric, labels, from, to, agg) do
          case rows do
            [] ->
              {:ok, []}

            [[_sid, value]] ->
              {:ok, [{from, value}]}

            # `query_aggregate/4` historically flattened every series selected
            # by a partial label map. Preserve that uncommon behavior rather
            # than combining per-series averages without their counts.
            _multiple ->
              query_aggregate_raw(store, metric, labels, from, to, nil, agg)
          end
        end

      native_bucket_shape?(from, to, bucket_seconds, agg) ->
        with {:ok, series} <-
               window_series(store, metric, labels, from, to, bucket_seconds, agg) do
          case series do
            [] -> {:ok, []}
            [%{data: data}] -> {:ok, data}
            _multiple -> query_aggregate_raw(store, metric, labels, from, to, bucket_seconds, agg)
          end
        end

      true ->
        query_aggregate_raw(store, metric, labels, from, to, bucket_seconds, agg)
    end
  end

  def query_aggregate_multi(store, metric, label_filter, opts) do
    from = Keyword.get(opts, :from, 0)
    to = Keyword.get(opts, :to, System.os_time(:second))
    bucket_seconds = bucket_to_seconds(opts[:bucket])
    agg = Keyword.get(opts, :aggregate, :avg)
    transform = Keyword.get(opts, :transform)

    cond do
      bucket_seconds == nil and agg in @native_scalar_aggregates ->
        with {:ok, rows} <- aggregate_multi_rows(store, metric, label_filter, from, to, agg) do
          cache = cache_ref(store)

          result =
            rows
            |> Enum.map(fn row ->
              {sid, value} = aggregate_row(row)
              data = TimelessMetrics.Transform.apply([{from, value}], transform)
              %{labels: cached_labels_by_sid(cache, sid), data: data}
            end)
            |> Enum.reject(&(&1.data == []))

          {:ok, result}
        end

      native_bucket_shape?(from, to, bucket_seconds, agg) ->
        with {:ok, series} <-
               window_series(store, metric, label_filter, from, to, bucket_seconds, agg) do
          {:ok,
           Enum.map(series, fn %{labels: labels, data: data} ->
             %{labels: labels, data: TimelessMetrics.Transform.apply(data, transform)}
           end)}
        end

      true ->
        query_aggregate_multi_raw(
          store,
          metric,
          label_filter,
          from,
          to,
          bucket_seconds,
          agg,
          transform
        )
    end
  end

  defp query_aggregate_raw(store, metric, labels, from, to, bucket_seconds, agg) do
    with {:ok, points} <- query_raw(store, metric, labels, from: from, to: to) do
      if bucket_seconds do
        {:ok, bucket_points(points, from, bucket_seconds, agg)}
      else
        values = Enum.map(points, &elem(&1, 1))

        case values do
          [] -> {:ok, []}
          _ -> {:ok, [{from, TimelessMetrics.Aggregation.compute_aggregate(agg, values, points)}]}
        end
      end
    end
  end

  defp query_aggregate_multi_raw(
         store,
         metric,
         label_filter,
         from,
         to,
         bucket_seconds,
         agg,
         transform
       ) do
    with {:ok, series} <- query_multi(store, metric, label_filter, from: from, to: to) do
      result =
        Enum.map(series, fn %{labels: labels, points: points} ->
          data =
            if bucket_seconds do
              bucket_points(points, from, bucket_seconds, agg)
            else
              values = Enum.map(points, &elem(&1, 1))

              case values do
                [] -> []
                _ -> [{from, TimelessMetrics.Aggregation.compute_aggregate(agg, values, points)}]
              end
            end

          %{labels: labels, data: TimelessMetrics.Transform.apply(data, transform)}
        end)
        |> Enum.reject(&(&1.data == []))

      {:ok, result}
    end
  end

  def latest(store, metric, labels) do
    with {:ok, rows} <- latest_rows_exact(store, metric, labels, 0, System.os_time(:second)) do
      case rows do
        [[_sid, timestamp, value]] -> {:ok, {timestamp, value}}
        _ -> {:ok, nil}
      end
    end
  end

  def latest_multi(store, metric, filter) do
    with {:ok, rows} <- latest_multi_rows(store, metric, filter, 0, System.os_time(:second)) do
      cache = cache_ref(store)

      {:ok,
       Enum.map(rows, fn row ->
         {sid, timestamp, value} = latest_row(row)

         %{
           labels: cached_labels_by_sid(cache, sid),
           timestamp: timestamp,
           value: value
         }
       end)}
    end
  end

  def list_metrics(store) do
    with {:ok, rows} <-
           read_sql(store, "SELECT DISTINCT name FROM timeless_series('#{@table}') ORDER BY name") do
      {:ok, Enum.map(rows, fn [name] -> name end)}
    end
  end

  def list_series(store, metric) do
    with {:ok, rows} <-
           read_sql(
             store,
             "SELECT labels FROM timeless_series('#{@table}') WHERE name = ?1 ORDER BY labels",
             [metric]
           ) do
      {:ok, Enum.map(rows, fn [labels] -> %{labels: decode_json(labels)} end)}
    end
  end

  def label_values(store, metric, key) do
    with {:ok, rows} <-
           read_sql(store, "SELECT value FROM timeless_label_values('#{@table}', ?1, ?2)", [
             metric,
             key
           ]) do
      {:ok, Enum.map(rows, fn [value] -> value end)}
    end
  end

  def query_rollup(store, metric, labels, resolution, from, to) do
    labels_json = encode_json(labels)

    with {:ok, rows} <-
           read_rollup_batches(store, [metric, labels_json, resolution, from, to]),
         :ok <- ensure_cached_labels(store, metric, rows) do
      cache = cache_ref(store)

      buckets =
        rows
        |> Enum.filter(fn [series_id, _blob] ->
          cached_labels_by_sid(cache, series_id) == labels
        end)
        |> Enum.flat_map(fn [_series_id, blob] -> decode_rollup_batch(blob) end)
        |> Enum.sort_by(& &1.bucket)

      {:ok, buckets}
    end
  end

  def find_series(store, metric, filter) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, rows} <-
               read_sql(
                 store,
                 "SELECT labels FROM timeless_series('#{@table}', ?1, ?2) ORDER BY labels",
                 [metric, encode_json(pushdown)]
               ) do
          labels = Enum.map(rows, fn [encoded] -> decode_json(encoded) end)

          case residual do
            [] ->
              {:ok, labels}

            _ ->
              compiled = TimelessMetrics.LabelMatch.compile(residual)
              {:ok, Enum.filter(labels, &TimelessMetrics.LabelMatch.match?(&1, compiled))}
          end
        end
    end
  end

  def info(store) do
    {:ok, rows} = read_sql(store, "SELECT key, value FROM timeless_stats('#{@table}')")
    stats = Map.new(rows, fn [key, value] -> {key, value} end)
    total_points = Map.get(stats, "disk_points", 0) + Map.get(stats, "buffered_points", 0)
    bytes = Map.get(stats, "bytes_on_disk", 0)
    data_dir = :persistent_term.get({TimelessMetrics, store, :data_dir}, nil)

    %{
      series_count: Map.get(stats, "series", 0),
      disk_points: Map.get(stats, "disk_points", 0),
      total_points: total_points,
      points_ingested: total_points,
      storage_bytes: bytes,
      compressed_bytes: bytes,
      bytes_per_point: Map.get(stats, "bytes_per_point", 0.0),
      raw_buffer_points: Map.get(stats, "buffered_points", 0),
      buffer_points: Map.get(stats, "buffered_points", 0),
      block_count: Map.get(stats, "chunks", 0),
      process_count: 1,
      index_ets_bytes: 0,
      buffer_memory_bytes: Map.get(stats, "buffer_memory", 0),
      daily_rollup_rows: Map.get(stats, "rollup_chunks", 0),
      db_path: if(data_dir, do: Path.join(data_dir, "metrics.db"), else: nil),
      oldest_timestamp: Map.get(stats, "ts_min"),
      newest_timestamp: Map.get(stats, "ts_max")
    }
  end

  # -- Writer process -------------------------------------------------------

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    data_dir = Keyword.fetch!(opts, :data_dir)
    schema = Keyword.fetch!(opts, :schema)
    reject_unmigrated_rust_store!(store, data_dir)
    conn = open_connection(Path.join(data_dir, "metrics.db"))
    create_table(conn, schema)
    {:ok, insert_stmt} = Exqlite.Sqlite3.prepare(conn, insert_command_sql())
    query_frame_features = detect_query_frame_features(conn)

    cache =
      :ets.new(__MODULE__, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, :auto}
      ])

    :persistent_term.put({__MODULE__, store, :series_cache}, cache)
    :persistent_term.put({__MODULE__, store, :query_frame_features}, query_frame_features)
    schedule_flush()
    rollup_timer = schedule_compact(schema.rollup_interval)
    retention_timer = schedule_retention(schema.retention_interval)

    {:ok,
     %{
       store: store,
       schema: schema,
       conn: conn,
       insert_stmt: insert_stmt,
       cache: cache,
       query_frame_features: query_frame_features,
       ingest_count: 0,
       ingest_timer: nil,
       ingest_token: nil,
       rollup_timer: rollup_timer,
       retention_timer: retention_timer
     }}
  end

  @impl true
  def handle_call({:sql, sql, params}, _from, state) do
    with {:ok, state} <- finish_ingest_transaction(state) do
      {:reply, safe_execute(state.conn, sql, params), state}
    else
      {{:error, _} = error, state} -> {:reply, error, state}
    end
  end

  def handle_call(:read_barrier, _from, state) do
    case finish_ingest_transaction(state) do
      {:ok, state} -> {:reply, :ok, state}
      {{:error, _} = error, state} -> {:reply, error, state}
    end
  end

  def handle_call(:cache_ref, _from, state), do: {:reply, state.cache, state}

  def handle_call({:insert, value}, _from, state) do
    case ensure_ingest_transaction(state) do
      {:ok, state} ->
        case execute_prepared_insert(state.conn, state.insert_stmt, value) do
          {:ok, _} = result ->
            state = %{state | ingest_count: state.ingest_count + 1}

            if state.ingest_count >= @ingest_transaction_max do
              case finish_ingest_transaction(state) do
                {:ok, state} -> {:reply, result, state}
                {{:error, _} = error, state} -> {:reply, error, state}
              end
            else
              {:reply, result, state}
            end

          {:error, _} = error ->
            {:reply, error, rollback_ingest_transaction(state)}
        end

      {{:error, _} = error, state} ->
        {:reply, error, state}
    end
  end

  def handle_call({:command, value}, _from, state) do
    with {:ok, state} <- finish_ingest_transaction(state) do
      {:reply, execute_prepared_insert(state.conn, state.insert_stmt, value), state}
    else
      {{:error, _} = error, state} -> {:reply, error, state}
    end
  end

  def handle_call({:resolve, metric, labels_json}, _from, state) do
    with {:ok, state} <- finish_ingest_transaction(state) do
      result =
        with {:ok, _} <-
               safe_execute(
                 state.conn,
                 "INSERT INTO #{@table}(#{@table}, name, labels) VALUES ('resolve', ?1, ?2)",
                 [metric, labels_json]
               ),
             {:ok, [[sid]]} <- safe_execute(state.conn, "SELECT last_insert_rowid()", []) do
          {:ok, [[sid]]}
        end

      {:reply, result, state}
    else
      {{:error, _} = error, state} -> {:reply, error, state}
    end
  end

  @impl true
  def handle_info(:flush, state) do
    state = finish_and_run_command(state, "flush")
    schedule_flush()
    {:noreply, state}
  end

  def handle_info(:compact, state) do
    state = finish_and_run_command(state, "compact")
    timer = schedule_compact(state.schema.rollup_interval)
    {:noreply, %{state | rollup_timer: timer}}
  end

  def handle_info({:commit_ingest, token}, %{ingest_token: token} = state) do
    {_result, state} = normalize_finish_result(finish_ingest_transaction(state))
    {:noreply, state}
  end

  def handle_info({:commit_ingest, _stale_token}, state), do: {:noreply, state}

  def handle_info(:retention, state) do
    state =
      case state.schema do
        %{raw_retention_seconds: seconds} when is_integer(seconds) ->
          finish_and_run_command(state, "prune:#{System.os_time(:second) - seconds}")

        _ ->
          state
      end

    timer = schedule_retention(state.schema.retention_interval)
    {:noreply, %{state | retention_timer: timer}}
  end

  @impl true
  def terminate(_reason, state) do
    {_result, state} = normalize_finish_result(finish_ingest_transaction(state))
    _ = execute_prepared_insert(state.conn, state.insert_stmt, "flush")
    Exqlite.Sqlite3.release(state.conn, state.insert_stmt)
    Exqlite.Sqlite3.close(state.conn)
    :persistent_term.erase({__MODULE__, state.store, :series_cache})
    :persistent_term.erase({__MODULE__, state.store, :query_frame_features})
    :ets.delete(state.cache)
    :ok
  end

  # -- Connection helpers --------------------------------------------------

  def open_connection(db_path) do
    {:ok, conn} = Exqlite.Sqlite3.open(db_path)

    for sql <- [
          "PRAGMA mmap_size = 2147483648",
          "PRAGMA cache_size = -128000",
          "PRAGMA temp_store = MEMORY",
          "PRAGMA busy_timeout = 5000"
        ] do
      {:ok, _} = TimelessMetrics.DB.execute(conn, sql, [])
    end

    :ok = Exqlite.Sqlite3.enable_load_extension(conn, true)
    {:ok, _} = TimelessMetrics.DB.execute(conn, "SELECT load_extension(?1)", [extension_path()])
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, false)
    conn
  end

  defp create_table(conn, schema) do
    rollups =
      schema.tiers
      |> Enum.map(fn tier ->
        retention =
          if tier.retention_seconds == :forever, do: "0", else: "#{tier.retention_seconds}s"

        "#{tier.resolution_seconds}s@#{retention}"
      end)
      |> Enum.join(",")

    sql =
      if rollups == "" do
        "CREATE VIRTUAL TABLE IF NOT EXISTS #{@table} USING timeless_metrics"
      else
        "CREATE VIRTUAL TABLE IF NOT EXISTS #{@table} USING timeless_metrics(rollups='#{rollups}')"
      end

    {:ok, _} = TimelessMetrics.DB.execute(conn, sql, [])
  end

  defp raw_point_rows(store, metric, filter, from, to) do
    {eq, complex} = TimelessMetrics.LabelMatch.split_pushdown(filter)

    with {:ok, rows} <- read_raw_points(store, [metric, encode_json(eq), from, to]),
         :ok <- ensure_cached_labels(store, metric, rows) do
      case complex do
        [] ->
          {:ok, rows}

        _ ->
          compiled = TimelessMetrics.LabelMatch.compile(complex)
          cache = cache_ref(store)

          {:ok,
           Enum.filter(rows, fn [sid, _point_blob] ->
             TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
           end)}
      end
    end
  end

  defp raw_point_rows_exact(store, metric, labels, from, to) do
    if series_id_pushdown?(store) do
      case cached_series_id(store, metric, labels) do
        {:ok, sid} -> read_raw_points_by_id(store, [metric, from, to, sid])
        :miss -> raw_point_rows(store, metric, labels, from, to)
      end
    else
      raw_point_rows(store, metric, labels, from, to)
    end
  end

  defp raw_frame_series(store, metric, filter, from, to) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, rows} <- read_raw_frame(store, [metric, encode_json(pushdown), from, to]),
             :ok <- ensure_cached_labels(store, metric, rows),
             {:ok, series} <- decode_raw_frame_rows(store, rows) do
          case residual do
            [] ->
              {:ok, series}

            _ ->
              compiled = TimelessMetrics.LabelMatch.compile(residual)

              {:ok,
               Enum.filter(series, fn %{labels: labels} ->
                 TimelessMetrics.LabelMatch.match?(labels, compiled)
               end)}
          end
        end
    end
  end

  defp decode_raw_frame_rows(_store, []), do: {:ok, []}

  defp decode_raw_frame_rows(store, [[frame]]) when is_binary(frame) do
    cache = cache_ref(store)

    with {:ok, labels} <- raw_frame_label_maps(frame, cache) do
      TimelessMetrics.RustEngine.Nif.decode_raw_frame_series(frame, labels)
    end
  end

  defp decode_raw_frame_rows(_store, rows) do
    {:error, "timeless_raw_frame returned #{length(rows)} malformed rows"}
  end

  defp raw_frame_label_maps(
         <<"TRF1", series_count::unsigned-little-32, _total_points::unsigned-little-64,
           rest::binary>>,
         cache
       ) do
    id_bytes = series_count * 8

    case rest do
      <<ids::binary-size(^id_bytes), _::binary>> ->
        {:ok,
         for <<series_id::signed-little-64 <- ids>> do
           cached_labels_by_sid(cache, series_id)
         end}

      _ ->
        {:error, "timeless_raw_frame returned a truncated series-id column"}
    end
  end

  defp raw_frame_label_maps(_frame, _cache) do
    {:error, "timeless_raw_frame returned a truncated or unknown frame envelope"}
  end

  defp aggregate_rows(store, metric, filter, from, to, agg) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, rows} <-
               read_aggregate(store, [
                 metric,
                 encode_json(pushdown),
                 from,
                 to,
                 Atom.to_string(agg)
               ]),
             :ok <- ensure_cached_labels(store, metric, rows) do
          case residual do
            [] ->
              {:ok, rows}

            _ ->
              compiled = TimelessMetrics.LabelMatch.compile(residual)
              cache = cache_ref(store)

              {:ok,
               Enum.filter(rows, fn [sid, _value] ->
                 TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
               end)}
          end
        end
    end
  end

  defp aggregate_multi_rows(store, metric, filter, from, to, agg) do
    if frame_feature?(store, @aggregate_frame_module) do
      case aggregate_frame_rows(store, metric, filter, from, to, agg) do
        {:error, :query_frame_unavailable} -> aggregate_rows(store, metric, filter, from, to, agg)
        result -> result
      end
    else
      aggregate_rows(store, metric, filter, from, to, agg)
    end
  end

  defp aggregate_frame_rows(store, metric, filter, from, to, agg) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, frame_rows} <-
               read_aggregate_frame(store, [
                 metric,
                 encode_json(pushdown),
                 from,
                 to,
                 Atom.to_string(agg)
               ]),
             {:ok, rows} <- decode_aggregate_frame_rows(frame_rows, agg),
             :ok <- ensure_cached_labels(store, metric, rows) do
          {:ok, filter_aggregate_rows(store, rows, residual)}
        end
    end
  end

  defp decode_aggregate_frame_rows([], _expected_aggregate), do: {:ok, []}

  defp decode_aggregate_frame_rows([[frame]], expected_aggregate) when is_binary(frame) do
    case TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(frame) do
      {:ok, {^expected_aggregate, rows}} ->
        {:ok, rows}

      {:ok, {actual_aggregate, _rows}} ->
        {:error,
         "timeless_aggregate_frame returned #{actual_aggregate} for #{expected_aggregate} query"}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_aggregate_frame_rows(rows, _expected_aggregate) do
    {:error, "timeless_aggregate_frame returned #{length(rows)} malformed rows"}
  end

  defp filter_aggregate_rows(_store, rows, []), do: rows

  defp filter_aggregate_rows(store, rows, residual) do
    compiled = TimelessMetrics.LabelMatch.compile(residual)
    cache = cache_ref(store)

    Enum.filter(rows, fn row ->
      {sid, _value} = aggregate_row(row)
      TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
    end)
  end

  defp aggregate_rows_exact(store, metric, labels, from, to, agg) do
    if series_id_pushdown?(store) do
      case cached_series_id(store, metric, labels) do
        {:ok, sid} ->
          read_aggregate_by_id(store, [metric, from, to, Atom.to_string(agg), sid])

        :miss ->
          aggregate_rows(store, metric, labels, from, to, agg)
      end
    else
      aggregate_rows(store, metric, labels, from, to, agg)
    end
  end

  defp latest_rows(store, metric, filter, from, to) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, rows} <- read_latest(store, [metric, encode_json(pushdown), from, to]),
             :ok <- ensure_cached_labels(store, metric, rows) do
          case residual do
            [] ->
              {:ok, rows}

            _ ->
              compiled = TimelessMetrics.LabelMatch.compile(residual)
              cache = cache_ref(store)

              {:ok,
               Enum.filter(rows, fn [sid, _timestamp, _value] ->
                 TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
               end)}
          end
        end
    end
  end

  defp latest_multi_rows(store, metric, filter, from, to) do
    if frame_feature?(store, @latest_frame_module) do
      case latest_frame_rows(store, metric, filter, from, to) do
        {:error, :query_frame_unavailable} -> latest_rows(store, metric, filter, from, to)
        result -> result
      end
    else
      latest_rows(store, metric, filter, from, to)
    end
  end

  defp latest_frame_rows(store, metric, filter, from, to) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        with {:ok, frame_rows} <-
               read_latest_frame(store, [metric, encode_json(pushdown), from, to]),
             {:ok, rows} <- decode_latest_frame_rows(frame_rows),
             :ok <- ensure_cached_labels(store, metric, rows) do
          {:ok, filter_latest_rows(store, rows, residual)}
        end
    end
  end

  defp decode_latest_frame_rows([]), do: {:ok, []}

  defp decode_latest_frame_rows([[frame]]) when is_binary(frame) do
    TimelessMetrics.RustEngine.Nif.decode_latest_frame(frame)
  end

  defp decode_latest_frame_rows(rows) do
    {:error, "timeless_latest_frame returned #{length(rows)} malformed rows"}
  end

  defp filter_latest_rows(_store, rows, []), do: rows

  defp filter_latest_rows(store, rows, residual) do
    compiled = TimelessMetrics.LabelMatch.compile(residual)
    cache = cache_ref(store)

    Enum.filter(rows, fn row ->
      {sid, _timestamp, _value} = latest_row(row)
      TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
    end)
  end

  defp aggregate_row([sid, value]), do: {sid, value}
  defp aggregate_row({sid, value}), do: {sid, value}

  defp latest_row([sid, timestamp, value]), do: {sid, timestamp, value}
  defp latest_row({sid, timestamp, value}), do: {sid, timestamp, value}

  defp latest_rows_exact(store, metric, labels, from, to) do
    if series_id_pushdown?(store) do
      case cached_series_id(store, metric, labels) do
        {:ok, sid} -> read_latest_by_id(store, [metric, from, to, sid])
        :miss -> latest_rows(store, metric, labels, from, to)
      end
    else
      latest_rows(store, metric, labels, from, to)
    end
  end

  defp window_series(store, metric, filter, from, to, step, agg) do
    case TimelessMetrics.LabelMatch.split_libsql_pushdown(filter) do
      :none ->
        {:ok, []}

      {pushdown, residual} ->
        window_start = from + step - 1

        with {:ok, rows} <-
               read_window_batches(store, [
                 metric,
                 encode_json(pushdown),
                 window_start,
                 to,
                 step,
                 step,
                 Atom.to_string(agg)
               ]),
             :ok <- ensure_cached_labels(store, metric, rows) do
          cache = cache_ref(store)

          rows =
            case residual do
              [] ->
                rows

              _ ->
                compiled = TimelessMetrics.LabelMatch.compile(residual)

                Enum.filter(rows, fn [sid, _bucket_blob] ->
                  TimelessMetrics.LabelMatch.match?(cached_labels_by_sid(cache, sid), compiled)
                end)
            end

          {:ok,
           Enum.map(rows, fn [sid, bucket_blob] ->
             %{
               labels: cached_labels_by_sid(cache, sid),
               data: decode_window_batch(bucket_blob, step, agg)
             }
           end)}
        end
    end
  end

  defp native_bucket_shape?(from, to, step, agg) do
    if is_integer(from) and is_integer(to) and is_integer(step) and step > 0 and
         agg in @native_bucket_aggregates do
      span = to - from + 1
      span > 0 and rem(span, step) == 0 and div(span, step) <= @native_window_max_points
    else
      false
    end
  end

  defp read_sql(store, sql, params \\ []) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:sql, sql, params}, :infinity)
      else
        write_sql(store, sql, params)
      end
    end
  end

  defp read_raw_points(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:raw_points, params}, :infinity)
      else
        write_sql(store, raw_points_sql(), params)
      end
    end
  end

  defp read_raw_points_by_id(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:raw_points_by_id, params}, :infinity)
      else
        write_sql(store, raw_points_by_id_sql(), params)
      end
    end
  end

  defp read_raw_frame(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:raw_frame, params}, :infinity)
      else
        write_sql(store, raw_frame_sql(), params)
      end
    end
  end

  defp read_aggregate_frame(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:aggregate_frame, params}, :infinity)
      else
        write_sql(store, aggregate_frame_sql(), params)
      end
    end
  end

  defp read_latest_frame(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:latest_frame, params}, :infinity)
      else
        write_sql(store, latest_frame_sql(), params)
      end
    end
  end

  defp read_aggregate(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:aggregate, params}, :infinity)
      else
        write_sql(store, aggregate_sql(), params)
      end
    end
  end

  defp read_aggregate_by_id(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:aggregate_by_id, params}, :infinity)
      else
        write_sql(store, aggregate_by_id_sql(), params)
      end
    end
  end

  defp read_latest(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:latest, params}, :infinity)
      else
        write_sql(store, latest_sql(), params)
      end
    end
  end

  defp read_latest_by_id(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:latest_by_id, params}, :infinity)
      else
        write_sql(store, latest_by_id_sql(), params)
      end
    end
  end

  defp read_window_batches(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:window_batches, params}, :infinity)
      else
        write_sql(store, window_batches_sql(), params)
      end
    end
  end

  defp read_rollup_batches(store, params) do
    with :ok <- read_barrier(store) do
      if target = select_reader(store) do
        GenServer.call(target, {:rollup_batches, params}, :infinity)
      else
        write_sql(store, rollup_batches_sql(), params)
      end
    end
  end

  defp read_barrier(store) do
    GenServer.call(writer_name(store), :read_barrier, :infinity)
  end

  # Keep one caller on one prepared-statement connection so repeated query
  # shapes retain SQLite/vtab cache locality. Independent caller processes
  # still distribute deterministically across the pool.
  defp select_reader(store) do
    readers = :persistent_term.get({__MODULE__, store, :readers}, [])

    case readers do
      [] -> nil
      _ -> Enum.at(readers, :erlang.phash2(self(), length(readers)))
    end
  end

  defp write_sql(store, sql, params) do
    GenServer.call(writer_name(store), {:sql, sql, params}, :infinity)
  end

  defp insert_value(store, value) do
    GenServer.call(writer_name(store), {:insert, value}, :infinity)
  end

  defp command(store, command) do
    case GenServer.call(writer_name(store), {:command, command}, :infinity) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  defp insert_command_sql, do: "INSERT INTO #{@table}(#{@table}) VALUES (?1)"

  defp execute_prepared_insert(conn, stmt, value) do
    result =
      with :ok <- bind_insert_value(stmt, value),
           :done <- Exqlite.Sqlite3.step(conn, stmt) do
        {:ok, []}
      else
        {:error, reason} -> {:error, inspect(reason)}
        other -> {:error, "unexpected SQLite insert result: #{inspect(other)}"}
      end

    _ = Exqlite.Sqlite3.reset(stmt)
    result
  rescue
    error ->
      _ = Exqlite.Sqlite3.reset(stmt)
      {:error, Exception.message(error)}
  end

  defp bind_insert_value(stmt, {:blob, value}), do: Exqlite.Sqlite3.bind_blob(stmt, 1, value)
  defp bind_insert_value(stmt, value), do: Exqlite.Sqlite3.bind_text(stmt, 1, value)

  defp ensure_ingest_transaction(%{ingest_count: 0} = state) do
    case safe_execute(state.conn, "BEGIN IMMEDIATE", []) do
      {:ok, _} ->
        token = make_ref()
        timer = Process.send_after(self(), {:commit_ingest, token}, @ingest_transaction_ms)
        {:ok, %{state | ingest_timer: timer, ingest_token: token}}

      {:error, _} = error ->
        {error, state}
    end
  end

  defp ensure_ingest_transaction(state), do: {:ok, state}

  defp finish_ingest_transaction(%{ingest_count: 0} = state), do: {:ok, state}

  defp finish_ingest_transaction(state) do
    cancel_ingest_timer(state)

    case safe_execute(state.conn, "COMMIT", []) do
      {:ok, _} ->
        {:ok, clear_ingest_transaction(state)}

      {:error, _} = error ->
        _ = safe_execute(state.conn, "ROLLBACK", [])
        {error, clear_ingest_transaction(state)}
    end
  end

  defp rollback_ingest_transaction(state) do
    cancel_ingest_timer(state)
    _ = safe_execute(state.conn, "ROLLBACK", [])
    clear_ingest_transaction(state)
  end

  defp clear_ingest_transaction(state) do
    %{state | ingest_count: 0, ingest_timer: nil, ingest_token: nil}
  end

  defp cancel_ingest_timer(%{ingest_timer: nil}), do: :ok
  defp cancel_ingest_timer(%{ingest_timer: timer}), do: Process.cancel_timer(timer)

  defp normalize_finish_result({:ok, state}), do: {:ok, state}
  defp normalize_finish_result({{:error, _} = error, state}), do: {error, state}

  defp finish_and_run_command(state, command) do
    {_result, state} = normalize_finish_result(finish_ingest_transaction(state))
    _ = execute_prepared_insert(state.conn, state.insert_stmt, command)
    state
  end

  defp safe_execute(conn, sql, params) do
    TimelessMetrics.DB.execute(conn, sql, params)
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp extension_path do
    Application.app_dir(:timeless_metrics, "priv/native/timeless_sqlite_ext.so")
  end

  defp cache_ref(store) do
    case :persistent_term.get({__MODULE__, store, :series_cache}, nil) do
      nil ->
        GenServer.call(writer_name(store), :cache_ref, :infinity)

      cache ->
        if :ets.info(cache) == :undefined do
          GenServer.call(writer_name(store), :cache_ref, :infinity)
        else
          cache
        end
    end
  end

  defp frame_feature?(store, module) do
    MapSet.member?(query_frame_features(store), module)
  end

  # The first extension revision with packed query frames is also the first
  # with output-column series_id constraint pushdown. Older extensions accept
  # the WHERE clause but apply it only after scanning every candidate series,
  # so keep their selective label-filter plans as the compatibility route.
  defp series_id_pushdown?(store) do
    frame_feature?(store, @aggregate_frame_module) and
      frame_feature?(store, @latest_frame_module)
  end

  defp cached_series_id(store, metric, labels) when is_map(labels) do
    key = {metric, labels}

    case :ets.lookup(cache_ref(store), key) do
      [{^key, sid}] -> {:ok, sid}
      [] -> :miss
    end
  end

  defp cached_series_id(_store, _metric, _labels), do: :miss

  defp cached_resolved_entries(cache, entries) do
    entries
    |> Enum.reduce_while([], fn {metric, labels, ts, value}, resolved ->
      key = {metric, labels}

      case :ets.lookup(cache, key) do
        [{^key, sid}] -> {:cont, [{sid, ts, value} | resolved]}
        [] -> {:halt, :miss}
      end
    end)
    |> case do
      :miss -> :miss
      resolved -> {:ok, Enum.reverse(resolved)}
    end
  end

  defp hydrate_series_cache(store, cache, keys) do
    wanted = MapSet.new(keys)

    keys
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
    |> Enum.chunk_every(500)
    |> Enum.each(fn metrics ->
      placeholders =
        1..length(metrics)
        |> Enum.map_join(",", fn index -> "?#{index}" end)

      sql =
        "SELECT series_id, name, labels FROM timeless_series('#{@table}') " <>
          "WHERE name IN (#{placeholders})"

      case read_sql(store, sql, metrics) do
        {:ok, rows} ->
          Enum.each(rows, fn [sid, metric, labels_json] ->
            key = {metric, decode_json(labels_json)}

            if MapSet.member?(wanted, key) do
              cache_series(cache, key, sid)
            else
              cache_labels(cache, elem(key, 1), sid)
            end
          end)

          :ets.insert(cache, Enum.map(metrics, &{{:metric_labels_loaded, &1}, true}))

        {:error, _reason} ->
          :ok
      end
    end)
  end

  defp copy_key({metric, labels}) do
    {:binary.copy(metric), Map.new(labels, fn {k, v} -> {:binary.copy(k), :binary.copy(v)} end)}
  end

  defp cache_series(cache, {_metric, labels} = key, sid) do
    :ets.insert(cache, [
      {copy_key(key), sid},
      {{:series_labels, sid}, Map.new(labels)}
    ])
  end

  defp cache_labels(cache, labels, sid) do
    :ets.insert(cache, {{:series_labels, sid}, Map.new(labels)})
  end

  defp cached_labels_by_sid(cache, sid) do
    key = {:series_labels, sid}
    [{^key, labels}] = :ets.lookup(cache, key)
    labels
  end

  defp ensure_cached_labels(_store, _metric, []), do: :ok

  defp ensure_cached_labels(store, metric, _rows) do
    cache = cache_ref(store)
    marker = {:metric_labels_loaded, metric}

    if :ets.member(cache, marker) do
      :ok
    else
      with {:ok, catalog_rows} <-
             read_sql(
               store,
               "SELECT series_id, name, labels FROM timeless_series('#{@table}') WHERE name = ?1",
               [metric]
             ) do
        Enum.each(catalog_rows, fn [sid, name, labels_json] ->
          cache_series(cache, {name, decode_json(labels_json)}, sid)
        end)

        true = :ets.insert(cache, {marker, true})
        :ok
      end
    end
  end

  defp encode_named_batch(entries) do
    {series, index_by_key} =
      entries
      |> Enum.map(fn {metric, labels, _ts, _value} -> {metric, Map.new(labels)} end)
      |> Enum.uniq()
      |> Enum.with_index()
      |> then(fn indexed -> {Enum.map(indexed, &elem(&1, 0)), Map.new(indexed)} end)

    header = <<0x01, 0, 0::little-16, length(series)::little-32, length(entries)::little-32>>

    series_table =
      Enum.map(series, fn {metric, labels} ->
        labels_json = if map_size(labels) == 0, do: "", else: encode_json(labels)

        [
          <<byte_size(metric)::little-32>>,
          metric,
          <<byte_size(labels_json)::little-32>>,
          labels_json
        ]
      end)

    indexes =
      Enum.map(entries, fn {metric, labels, _ts, _value} ->
        <<Map.fetch!(index_by_key, {metric, Map.new(labels)})::little-32>>
      end)

    timestamps =
      Enum.map(entries, fn {_metric, _labels, ts, _value} -> <<ts::signed-little-64>> end)

    values =
      Enum.map(entries, fn {_metric, _labels, _ts, value} -> <<value * 1.0::float-little-64>> end)

    IO.iodata_to_binary([header, series_table, indexes, timestamps, values])
  end

  defp encode_resolved_batch(entries) do
    header = <<0x02, 0, 0::little-16, length(entries)::little-32>>

    ids = for {sid, _ts, _value} <- entries, into: <<>>, do: <<sid::signed-little-64>>
    timestamps = for {_sid, ts, _value} <- entries, into: <<>>, do: <<ts::signed-little-64>>

    values =
      for {_sid, _ts, value} <- entries, into: <<>>, do: <<value * 1.0::float-little-64>>

    <<header::binary, ids::binary, timestamps::binary, values::binary>>
  end

  defp reject_unmigrated_rust_store!(store, data_dir) do
    rust_dir = Path.join(data_dir, "rust_engine")
    has_rust_data? = match?({:ok, [_ | _]}, File.ls(rust_dir))

    if has_rust_data? do
      db = :"#{store}_db"

      migrated? =
        case TimelessMetrics.DB.read(
               db,
               "SELECT 1 FROM _metadata WHERE key = 'libsql_migration' LIMIT 1",
               []
             ) do
          {:ok, [[1]]} -> true
          _ -> false
        end

      unless migrated? do
        raise "refusing to start engine: :libsql over an unmigrated rust_engine/ store; " <>
                "run mix timeless_metrics.migrate_libsql #{data_dir} --activate while the store is stopped"
      end
    end
  end

  defp encode_json(value), do: value |> :json.encode() |> IO.iodata_to_binary()
  defp decode_json(value), do: :json.decode(value)

  defp decode_point_batch(<<n::unsigned-little-32, rest::binary>>) do
    column_bytes = n * 8

    case rest do
      <<timestamps::binary-size(^column_bytes), values::binary-size(^column_bytes)>> ->
        decode_point_columns(timestamps, values, [])

      _ ->
        raise "timeless_raw_batches returned a malformed #{byte_size(rest) + 4}-byte point blob"
    end
  end

  defp decode_point_batch(blob) do
    raise "timeless_raw_batches returned a truncated #{byte_size(blob)}-byte point blob"
  end

  defp decode_point_columns(<<>>, <<>>, acc), do: Enum.reverse(acc)

  defp decode_point_columns(
         <<ts::signed-little-64, timestamps::binary>>,
         <<value::float-little-64, values::binary>>,
         acc
       ) do
    decode_point_columns(timestamps, values, [{ts, value} | acc])
  end

  defp decode_window_batch(
         <<"TWB1", n::unsigned-little-32, rest::binary>> = blob,
         step,
         agg
       ) do
    column_bytes = n * 8
    bitmap_bytes = div(n + 7, 8)

    case rest do
      <<timestamps::binary-size(^column_bytes), bitmap::binary-size(^bitmap_bytes),
        values::binary-size(^column_bytes)>> ->
        decode_window_columns(timestamps, bitmap, values, step, agg, 0, [])

      _ ->
        raise "timeless_window_batches returned a malformed #{byte_size(blob)}-byte bucket blob"
    end
  end

  defp decode_window_batch(blob, _step, _agg) do
    raise "timeless_window_batches returned an unknown or truncated #{byte_size(blob)}-byte bucket blob"
  end

  defp decode_window_columns(<<>>, _bitmap, <<>>, _step, _agg, _index, acc),
    do: Enum.reverse(acc)

  defp decode_window_columns(
         <<timestamp::signed-little-64, timestamps::binary>>,
         bitmap,
         <<value::float-little-64, values::binary>>,
         step,
         agg,
         index,
         acc
       ) do
    acc =
      if Bitwise.band(:binary.at(bitmap, div(index, 8)), Bitwise.bsl(1, rem(index, 8))) != 0 do
        value = if agg == :count, do: trunc(value), else: value
        [{timestamp - step + 1, value} | acc]
      else
        acc
      end

    decode_window_columns(timestamps, bitmap, values, step, agg, index + 1, acc)
  end

  @doc false
  def decode_rollup_batch(<<"TRB1", n::unsigned-little-32, rest::binary>> = blob) do
    column_bytes = n * 8

    case rest do
      <<timestamps::binary-size(^column_bytes), counts::binary-size(^column_bytes),
        averages::binary-size(^column_bytes), sums::binary-size(^column_bytes),
        minimums::binary-size(^column_bytes), maximums::binary-size(^column_bytes),
        last_timestamps::binary-size(^column_bytes), lasts::binary-size(^column_bytes)>> ->
        decode_rollup_columns(
          timestamps,
          counts,
          averages,
          sums,
          minimums,
          maximums,
          last_timestamps,
          lasts,
          []
        )

      _ ->
        raise "timeless_rollup_batches returned a malformed #{byte_size(blob)}-byte bucket blob"
    end
  end

  def decode_rollup_batch(blob) do
    raise "timeless_rollup_batches returned an unknown or truncated #{byte_size(blob)}-byte bucket blob"
  end

  defp decode_rollup_columns(<<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, acc),
    do: Enum.reverse(acc)

  defp decode_rollup_columns(
         <<timestamp::signed-little-64, timestamps::binary>>,
         <<count::unsigned-little-64, counts::binary>>,
         <<average_bits::unsigned-little-64, averages::binary>>,
         <<sum_bits::unsigned-little-64, sums::binary>>,
         <<minimum_bits::unsigned-little-64, minimums::binary>>,
         <<maximum_bits::unsigned-little-64, maximums::binary>>,
         <<_last_timestamp::signed-little-64, last_timestamps::binary>>,
         <<last_bits::unsigned-little-64, lasts::binary>>,
         acc
       ) do
    bucket = %{
      bucket: timestamp,
      avg: decode_sqlite_float(average_bits),
      min: decode_sqlite_float(minimum_bits),
      max: decode_sqlite_float(maximum_bits),
      count: count,
      sum: decode_sqlite_float(sum_bits),
      last: decode_sqlite_float(last_bits)
    }

    decode_rollup_columns(
      timestamps,
      counts,
      averages,
      sums,
      minimums,
      maximums,
      last_timestamps,
      lasts,
      [bucket | acc]
    )
  end

  # SQLite converts non-finite REAL values to NULL at the row-TVF boundary;
  # BEAM also rejects NaN/Infinity in a float bitstring match. Preserve the
  # established adapter result (`nil`) while the public TRB1 bytes retain the
  # original IEEE-754 payload for direct extension users.
  defp decode_sqlite_float(bits) do
    if Bitwise.band(bits, 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000 do
      nil
    else
      <<value::float-little-64>> = <<bits::unsigned-little-64>>
      value
    end
  end

  defp bucket_to_seconds(nil), do: nil
  defp bucket_to_seconds(:minute), do: 60
  defp bucket_to_seconds(:hour), do: 3_600
  defp bucket_to_seconds(:day), do: 86_400
  defp bucket_to_seconds({n, :seconds}), do: n
  defp bucket_to_seconds({n, :minutes}), do: n * 60
  defp bucket_to_seconds({n, :hours}), do: n * 3_600
  defp bucket_to_seconds(_), do: 60

  defp bucket_points([], _from, _step, _agg), do: []

  defp bucket_points(points, from, step, :rate) do
    points
    |> Enum.sort_by(&elem(&1, 0))
    |> TimelessMetrics.Aggregation.bucket_rate(fn ts -> from + div(ts - from, step) * step end)
  end

  defp bucket_points(points, from, step, agg) do
    points
    |> Enum.group_by(fn {ts, _} -> from + div(ts - from, step) * step end)
    |> Enum.map(fn {bucket, points} ->
      values = Enum.map(points, &elem(&1, 1))
      {bucket, TimelessMetrics.Aggregation.compute_aggregate(agg, values, points)}
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp schedule_flush, do: Process.send_after(self(), :flush, @flush_interval)
  defp schedule_compact(interval), do: Process.send_after(self(), :compact, interval)
  defp schedule_retention(interval), do: Process.send_after(self(), :retention, interval)
end

defmodule TimelessMetrics.LibsqlEngine.Reader do
  @moduledoc false
  use GenServer

  @read_conflict_attempts 1_000
  @read_conflict_sleep_ms 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    conn = TimelessMetrics.LibsqlEngine.open_connection(Path.join(data_dir, "metrics.db"))
    query_frame_features = TimelessMetrics.LibsqlEngine.detect_query_frame_features(conn)

    {:ok, raw_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.raw_batches_sql())

    {:ok, raw_points_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.raw_points_sql())

    {:ok, raw_points_by_id_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.raw_points_by_id_sql())

    {:ok, raw_frame_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.raw_frame_sql())

    aggregate_frame_stmt =
      prepare_optional(
        conn,
        query_frame_features,
        "timeless_aggregate_frame",
        TimelessMetrics.LibsqlEngine.aggregate_frame_sql()
      )

    latest_frame_stmt =
      prepare_optional(
        conn,
        query_frame_features,
        "timeless_latest_frame",
        TimelessMetrics.LibsqlEngine.latest_frame_sql()
      )

    {:ok, aggregate_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.aggregate_sql())

    {:ok, aggregate_by_id_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.aggregate_by_id_sql())

    {:ok, latest_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.latest_sql())

    {:ok, latest_by_id_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.latest_by_id_sql())

    {:ok, window_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.window_sql())

    {:ok, window_batches_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.window_batches_sql())

    {:ok, rollup_batches_stmt} =
      Exqlite.Sqlite3.prepare(conn, TimelessMetrics.LibsqlEngine.rollup_batches_sql())

    {:ok,
     %{
       conn: conn,
       raw_stmt: raw_stmt,
       raw_points_stmt: raw_points_stmt,
       raw_points_by_id_stmt: raw_points_by_id_stmt,
       raw_frame_stmt: raw_frame_stmt,
       aggregate_frame_stmt: aggregate_frame_stmt,
       latest_frame_stmt: latest_frame_stmt,
       aggregate_stmt: aggregate_stmt,
       aggregate_by_id_stmt: aggregate_by_id_stmt,
       latest_stmt: latest_stmt,
       latest_by_id_stmt: latest_by_id_stmt,
       window_stmt: window_stmt,
       window_batches_stmt: window_batches_stmt,
       rollup_batches_stmt: rollup_batches_stmt
     }}
  end

  @impl true
  def handle_call({:sql, sql, params}, _from, state) do
    result =
      retry_read_conflict(fn ->
        try do
          TimelessMetrics.DB.execute(state.conn, sql, params)
        rescue
          error -> {:error, Exception.message(error)}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:raw_batches, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.raw_stmt, params), state}
  end

  def handle_call({:raw_points, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.raw_points_stmt, params), state}
  end

  def handle_call({:raw_points_by_id, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.raw_points_by_id_stmt, params), state}
  end

  def handle_call({:raw_frame, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.raw_frame_stmt, params), state}
  end

  def handle_call({:aggregate_frame, _params}, _from, %{aggregate_frame_stmt: nil} = state) do
    {:reply, {:error, :query_frame_unavailable}, state}
  end

  def handle_call({:aggregate_frame, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.aggregate_frame_stmt, params), state}
  end

  def handle_call({:latest_frame, _params}, _from, %{latest_frame_stmt: nil} = state) do
    {:reply, {:error, :query_frame_unavailable}, state}
  end

  def handle_call({:latest_frame, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.latest_frame_stmt, params), state}
  end

  def handle_call({:aggregate, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.aggregate_stmt, params), state}
  end

  def handle_call({:aggregate_by_id, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.aggregate_by_id_stmt, params), state}
  end

  def handle_call({:latest, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.latest_stmt, params), state}
  end

  def handle_call({:latest_by_id, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.latest_by_id_stmt, params), state}
  end

  def handle_call({:window, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.window_stmt, params), state}
  end

  def handle_call({:window_batches, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.window_batches_stmt, params), state}
  end

  def handle_call({:rollup_batches, params}, _from, state) do
    {:reply, execute_prepared_query(state.conn, state.rollup_batches_stmt, params), state}
  end

  @impl true
  def terminate(_reason, state) do
    Exqlite.Sqlite3.release(state.conn, state.raw_stmt)
    Exqlite.Sqlite3.release(state.conn, state.raw_points_stmt)
    Exqlite.Sqlite3.release(state.conn, state.raw_points_by_id_stmt)
    Exqlite.Sqlite3.release(state.conn, state.raw_frame_stmt)
    release_optional(state.conn, state.aggregate_frame_stmt)
    release_optional(state.conn, state.latest_frame_stmt)
    Exqlite.Sqlite3.release(state.conn, state.aggregate_stmt)
    Exqlite.Sqlite3.release(state.conn, state.aggregate_by_id_stmt)
    Exqlite.Sqlite3.release(state.conn, state.latest_stmt)
    Exqlite.Sqlite3.release(state.conn, state.latest_by_id_stmt)
    Exqlite.Sqlite3.release(state.conn, state.window_stmt)
    Exqlite.Sqlite3.release(state.conn, state.window_batches_stmt)
    Exqlite.Sqlite3.release(state.conn, state.rollup_batches_stmt)
    Exqlite.Sqlite3.close(state.conn)
  end

  defp prepare_optional(conn, features, module, sql) do
    if MapSet.member?(features, module) do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)
      stmt
    end
  end

  defp release_optional(_conn, nil), do: :ok
  defp release_optional(conn, stmt), do: Exqlite.Sqlite3.release(conn, stmt)

  defp execute_prepared_query(conn, stmt, params) do
    retry_read_conflict(fn -> execute_prepared_query_once(conn, stmt, params) end)
  end

  defp execute_prepared_query_once(conn, stmt, params) do
    result =
      with :ok <- Exqlite.Sqlite3.bind(stmt, params) do
        fetch_rows(conn, stmt, [])
      end

    _ = Exqlite.Sqlite3.reset(stmt)
    result
  rescue
    error ->
      _ = Exqlite.Sqlite3.reset(stmt)
      {:error, Exception.message(error)}
  end

  defp retry_read_conflict(operation, attempts \\ @read_conflict_attempts)

  defp retry_read_conflict(operation, attempts) when attempts > 1 do
    case operation.() do
      {:error, reason} = error ->
        if read_conflict?(reason) do
          Process.sleep(@read_conflict_sleep_ms)
          retry_read_conflict(operation, attempts - 1)
        else
          error
        end

      result ->
        result
    end
  end

  defp retry_read_conflict(operation, 1), do: operation.()

  defp read_conflict?(reason) do
    reason
    |> inspect()
    |> String.contains?("active write transaction")
  end

  defp fetch_rows(conn, stmt, rows) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_rows(conn, stmt, [row | rows])
      :done -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, inspect(reason)}
      other -> {:error, "unexpected SQLite query result: #{inspect(other)}"}
    end
  end
end
