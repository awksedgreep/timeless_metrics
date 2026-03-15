defmodule TimelessMetrics.Actor.SeriesManager do
  @moduledoc """
  Manages the lifecycle of per-series processes for the actor engine.

  Provides the metric-level index for fan-out queries and handles series
  creation/startup. Uses an ETS table as a fast index mapping
  `{metric_name, encoded_labels}` to `series_id` for hot-path writes.
  """

  use GenServer

  alias TimelessMetrics.Actor.SeriesServer

  defstruct [
    :store,
    :db,
    :data_dir,
    :registry,
    :dynamic_sup,
    :index,
    :series_writer,
    :next_id,
    :max_blocks,
    :block_size,
    :compression,
    :flush_interval,
    :merge_block_min_count,
    :merge_block_max_points,
    :merge_block_min_age_seconds,
    :merge_compression_level,
    :merge_interval,
    :gc_on_compress,
    :defer_compression,
    :raw_buffer_max,
    :read_buffer
  ]

  # --- Client API ---

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Get or start a series process. Hot path uses ETS + Registry directly.
  Cold path (new series) goes through the GenServer.

  The optional `series_type` parameter (`:numeric` or `:text`) is only used
  when creating a new series. Existing series retain their original type.
  """
  def get_or_start(manager, metric_name, labels, series_type \\ :numeric) do
    state_info = :persistent_term.get({__MODULE__, manager})
    index = state_info.index
    key = {metric_name, labels}

    case :ets.lookup(index, key) do
      [{^key, series_id, pid}] ->
        # Trust the ETS entry — callers handle :noproc lazily
        {series_id, pid}

      [] ->
        GenServer.call(manager, {:get_or_start, metric_name, labels, series_type})
    end
  end

  @doc """
  Find all series matching a metric name and label filter.
  Returns `[{series_id, labels, pid}]`.
  """
  def find_series(manager, metric_name, label_filter) do
    state_info = :persistent_term.get({__MODULE__, manager})

    if exact_label_filter?(label_filter) do
      find_series_indexed(state_info, metric_name, label_filter)
    else
      find_series_scan(state_info, metric_name, label_filter)
    end
  end

  # Fast path: use label index for O(matches) lookup instead of O(all_series_for_metric) scan
  defp find_series_indexed(state_info, metric_name, label_filter)
       when map_size(label_filter) == 0 do
    find_series_scan(state_info, metric_name, label_filter)
  end

  defp find_series_indexed(state_info, metric_name, label_filter) do
    label_index = state_info.label_index

    # Look up by the first filter key, then intersect with remaining keys
    [{first_key, first_val} | rest] = Enum.to_list(label_filter)

    candidates =
      :ets.lookup(label_index, {metric_name, first_key, first_val})
      |> Enum.map(fn {_key, series_id, labels, pid} -> {series_id, labels, pid} end)

    # Intersect with remaining filter keys
    result =
      Enum.reduce(rest, candidates, fn {k, v}, acc ->
        match_set =
          :ets.lookup(label_index, {metric_name, k, v})
          |> MapSet.new(fn {_key, sid, _labels, _pid} -> sid end)

        Enum.filter(acc, fn {sid, _labels, _pid} -> MapSet.member?(match_set, sid) end)
      end)

    result
  end

  # Slow path: full ETS scan for regex/complex filters
  defp find_series_scan(state_info, metric_name, label_filter) do
    index = state_info.index

    :ets.match_object(index, {{metric_name, :_}, :_, :_})
    |> Enum.map(fn {{_metric, labels}, series_id, pid} ->
      {series_id, labels, pid}
    end)
    |> filter_by_labels(label_filter)
  end

  defp exact_label_filter?(filter) when map_size(filter) == 0, do: false

  defp exact_label_filter?(filter) do
    Enum.all?(filter, fn
      {_k, v} when is_binary(v) -> true
      _ -> false
    end)
  end

  @doc "List all unique metric names."
  def list_metrics(manager) do
    state_info = :persistent_term.get({__MODULE__, manager})
    flush_writer(state_info)

    {:ok, rows} =
      TimelessMetrics.DB.read(
        state_info.db,
        "SELECT DISTINCT metric_name FROM series ORDER BY metric_name"
      )

    Enum.map(rows, fn [name] -> name end)
  end

  @doc "List all series (labels) for a given metric."
  def list_series(manager, metric_name) do
    state_info = :persistent_term.get({__MODULE__, manager})
    flush_writer(state_info)

    {:ok, rows} =
      TimelessMetrics.DB.read(
        state_info.db,
        "SELECT labels FROM series WHERE metric_name = ?1 ORDER BY labels",
        [metric_name]
      )

    Enum.map(rows, fn [labels_str] -> %{labels: decode_labels(labels_str)} end)
  end

  @doc "Merge blocks in all series processes."
  def merge_all(manager) do
    state_info = :persistent_term.get({__MODULE__, manager})
    registry = state_info.registry

    results =
      Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
      |> Enum.map(fn pid ->
        try do
          GenServer.call(pid, :merge_blocks, 60_000)
        catch
          :exit, _ -> :noop
        end
      end)

    if Enum.any?(results, &(&1 == :ok)) do
      :ok
    else
      :noop
    end
  end

  @doc "Flush all series to disk."
  def flush_all(manager) do
    state_info = :persistent_term.get({__MODULE__, manager})
    registry = state_info.registry

    Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(fn pid ->
      try do
        GenServer.call(pid, :flush, :infinity)
      catch
        :exit, _ -> :ok
      end
    end)
  end

  @doc "Get label values for a specific label key across all series of a metric."
  def label_values(manager, metric_name, label_key) do
    state_info = :persistent_term.get({__MODULE__, manager})
    flush_writer(state_info)

    {:ok, rows} =
      TimelessMetrics.DB.read(
        state_info.db,
        "SELECT labels FROM series WHERE metric_name = ?1",
        [metric_name]
      )

    rows
    |> Enum.map(fn [labels_str] -> decode_labels(labels_str) end)
    |> Enum.flat_map(fn labels -> Map.get(labels, label_key) |> List.wrap() end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  # --- Server ---

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    db = Keyword.get(opts, :db, :"#{store}_db")
    registry = Keyword.get(opts, :registry, :"#{store}_actor_registry")
    dynamic_sup = Keyword.get(opts, :dynamic_sup, :"#{store}_actor_sup")
    max_blocks = Keyword.get(opts, :max_blocks, 100)
    block_size = Keyword.get(opts, :block_size, 1000)
    compression = Keyword.get(opts, :compression, :zstd)
    flush_interval = Keyword.get(opts, :flush_interval, 60_000)
    merge_block_min_count = Keyword.get(opts, :merge_block_min_count, 4)
    merge_block_max_points = Keyword.get(opts, :merge_block_max_points, 10_000)
    merge_block_min_age_seconds = Keyword.get(opts, :merge_block_min_age_seconds, 300)
    merge_compression_level = Keyword.get(opts, :merge_compression_level, 19)
    merge_interval = Keyword.get(opts, :merge_interval, 300_000)
    gc_on_compress = Keyword.get(opts, :gc_on_compress, true)
    defer_compression = Keyword.get(opts, :defer_compression, false)
    raw_buffer_max = Keyword.get(opts, :raw_buffer_max, 100_000)

    # Create ETS index: {metric_name, encoded_labels} => series_id
    index = :"#{store}_actor_index"

    :ets.new(index, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    # Label index: {metric_name, label_key, label_value} => {series_id, labels, pid}
    # duplicate_bag allows multiple series to share the same label value
    label_index = :"#{store}_label_index"

    :ets.new(label_index, [
      :named_table,
      :duplicate_bag,
      :public,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    # Read buffer: non-blocking read path for raw points.
    # Each series actor writes {series_id, ts, val} entries here.
    # Queries read directly from ETS, bypassing the actor mailbox.
    # Default on — negligible cost at low cardinality, major win at high cardinality.
    ets_read_buffer = Keyword.get(opts, :ets_read_buffer, true)

    read_buffer =
      if ets_read_buffer do
        name = :"#{store}_read_buffer"

        :ets.new(name, [
          :named_table,
          :ordered_set,
          :public,
          read_concurrency: true,
          write_concurrency: :auto
        ])

        name
      else
        nil
      end

    # Seed ID counter from SQLite max
    next_id =
      case TimelessMetrics.DB.read(db, "SELECT COALESCE(MAX(id), 0) FROM series") do
        {:ok, [[max_id]]} -> max_id + 1
        _ -> 1
      end

    series_writer = :"#{store}_series_writer"

    state = %__MODULE__{
      store: store,
      db: db,
      data_dir: data_dir,
      registry: registry,
      dynamic_sup: dynamic_sup,
      index: index,
      series_writer: series_writer,
      next_id: next_id,
      max_blocks: max_blocks,
      block_size: block_size,
      compression: compression,
      flush_interval: flush_interval,
      merge_block_min_count: merge_block_min_count,
      merge_block_max_points: merge_block_max_points,
      merge_block_min_age_seconds: merge_block_min_age_seconds,
      merge_compression_level: merge_compression_level,
      merge_interval: merge_interval,
      gc_on_compress: gc_on_compress,
      defer_compression: defer_compression,
      raw_buffer_max: raw_buffer_max,
      read_buffer: read_buffer
    }

    # Store in persistent_term for fast client-side access
    # Keyed by manager name (for get_or_start) and by store atom (for hot-path writes)
    info = %{
      index: index,
      label_index: label_index,
      read_buffer: read_buffer,
      registry: registry,
      db: db,
      manager: name,
      series_writer: series_writer
    }

    :persistent_term.put({__MODULE__, name}, info)
    :persistent_term.put({__MODULE__, store}, info)

    # Recovery: load all series from DB and start processes
    recover_series(state)

    {:ok, state}
  end

  @impl true
  def handle_call({:get_or_start, metric_name, labels, series_type}, _from, state) do
    key = {metric_name, labels}

    # Double-check ETS (another process may have registered it)
    case :ets.lookup(state.index, key) do
      [{^key, series_id, pid}] ->
        if Process.alive?(pid) do
          {:reply, {series_id, pid}, state}
        else
          {series_id, new_pid} =
            start_series_process(state, series_id, metric_name, labels, series_type)

          :ets.insert(state.index, {key, series_id, new_pid})
          update_label_index_pid(state.store, metric_name, labels, series_id, new_pid)
          {:reply, {series_id, new_pid}, state}
        end

      [] ->
        # Assign ID from counter, start process immediately
        TimelessMetrics.Stats.incr_series_created(state.store)
        series_id = state.next_id
        state = %{state | next_id: state.next_id + 1}

        {series_id, pid} =
          start_series_process(state, series_id, metric_name, labels, series_type)

        :ets.insert(state.index, {key, series_id, pid})
        insert_label_index(state.store, metric_name, labels, series_id, pid)

        # Async bulk write to SQLite (non-blocking)
        encoded = encode_labels(labels)
        now = System.os_time(:second)
        type_str = Atom.to_string(series_type)

        TimelessMetrics.Actor.SeriesWriter.register(
          state.series_writer,
          series_id,
          metric_name,
          encoded,
          now,
          type_str
        )

        {:reply, {series_id, pid}, state}
    end
  end

  def handle_call({:start_series, series_id, metric_name, labels, series_type}, _from, state) do
    {series_id, pid} =
      start_series_process(state, series_id, metric_name, labels, series_type)

    # Update ETS with new PID (process restarted)
    :ets.insert(state.index, {{metric_name, labels}, series_id, pid})
    update_label_index_pid(state.store, metric_name, labels, series_id, pid)
    {:reply, {series_id, pid}, state}
  end

  # --- Internals ---

  defp flush_writer(%{series_writer: writer}),
    do: TimelessMetrics.Actor.SeriesWriter.flush_sync(writer)

  defp recover_series(state) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        state.db,
        "SELECT id, metric_name, labels, series_type FROM series"
      )

    Enum.each(rows, fn [id, metric_name, encoded_labels, type_str] ->
      labels = decode_labels(encoded_labels)
      series_type = String.to_existing_atom(type_str)
      {_id, pid} = start_series_process(state, id, metric_name, labels, series_type)
      :ets.insert(state.index, {{metric_name, labels}, id, pid})
      insert_label_index(state.store, metric_name, labels, id, pid)
    end)
  end

  defp start_series_process(state, series_id, metric_name, labels, series_type) do
    child_spec = %{
      id: {:series, series_id},
      start:
        {SeriesServer, :start_link,
         [
           [
             series_id: series_id,
             metric_name: metric_name,
             labels: labels,
             store: state.store,
             data_dir: state.data_dir,
             registry: state.registry,
             max_blocks: state.max_blocks,
             block_size: state.block_size,
             compression: state.compression,
             flush_interval: state.flush_interval,
             merge_block_min_count: state.merge_block_min_count,
             merge_block_max_points: state.merge_block_max_points,
             merge_block_min_age_seconds: state.merge_block_min_age_seconds,
             merge_compression_level: state.merge_compression_level,
             merge_interval: state.merge_interval,
             gc_on_compress: state.gc_on_compress,
             defer_compression: state.defer_compression,
             raw_buffer_max: state.raw_buffer_max,
             series_type: series_type,
             read_buffer: state.read_buffer
           ]
         ]},
      restart: :transient
    }

    case DynamicSupervisor.start_child(state.dynamic_sup, child_spec) do
      {:ok, pid} ->
        {series_id, pid}

      {:error, {:already_started, pid}} ->
        {series_id, pid}

      {:error, reason} ->
        raise "Failed to start series process #{series_id}: #{inspect(reason)}"
    end
  end

  defp filter_by_labels(series_list, label_filter) when map_size(label_filter) == 0 do
    series_list
  end

  defp filter_by_labels(series_list, label_filter) do
    compiled = compile_label_filter(label_filter)

    Enum.filter(series_list, fn
      {_id, labels} -> matches_filter?(labels, compiled)
      {_id, labels, _pid} -> matches_filter?(labels, compiled)
    end)
  end

  defp matches_filter?(labels, compiled) do
    Enum.all?(compiled, fn
      {k, {:compiled_regex, regex}} ->
        case Map.get(labels, k) do
          nil -> false
          val -> Regex.match?(regex, val)
        end

      {k, v} ->
        Map.get(labels, k) == v
    end)
  end

  defp compile_label_filter(label_filter) do
    Enum.map(label_filter, fn
      {k, {:regex, pattern}} ->
        {:ok, regex} = Regex.compile("^(?:" <> pattern <> ")$")
        {k, {:compiled_regex, regex}}

      {k, v} ->
        {k, v}
    end)
  end

  defp insert_label_index(store, metric_name, labels, series_id, pid) do
    label_index = :"#{store}_label_index"

    Enum.each(labels, fn {k, v} ->
      :ets.insert(label_index, {{metric_name, k, v}, series_id, labels, pid})
    end)
  end

  defp update_label_index_pid(store, metric_name, labels, series_id, new_pid) do
    label_index = :"#{store}_label_index"

    Enum.each(labels, fn {k, v} ->
      # Delete old entries for this series, insert new
      :ets.match_delete(label_index, {{metric_name, k, v}, series_id, :_, :_})
      :ets.insert(label_index, {{metric_name, k, v}, series_id, labels, new_pid})
    end)
  end

  @doc false
  def encode_labels(labels) when is_map(labels) do
    labels
    |> Enum.sort()
    |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
    |> Enum.join(",")
  end

  defp decode_labels(""), do: %{}

  defp decode_labels(labels_str) do
    labels_str
    |> String.split(",")
    |> Enum.map(fn pair ->
      case String.split(pair, "=", parts: 2) do
        [k, v] -> {k, v}
        [k] -> {k, ""}
      end
    end)
    |> Map.new()
  end
end
