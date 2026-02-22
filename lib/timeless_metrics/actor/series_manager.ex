defmodule TimelessMetrics.Actor.SeriesManager do
  @moduledoc """
  Manages the lifecycle of per-series processes for the actor engine.

  Provides the metric-level index for fan-out queries and handles series
  creation/startup. Uses an ETS table as a fast index mapping
  `{metric_name, encoded_labels}` to `series_id` for hot-path writes.
  """

  use GenServer

  alias TimelessMetrics.Actor.SeriesServer

  defstruct [:store, :db, :data_dir, :registry, :dynamic_sup, :index,
             :max_blocks, :block_size, :compression, :flush_interval]

  # --- Client API ---

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Get or start a series process. Hot path uses ETS + Registry directly.
  Cold path (new series) goes through the GenServer.
  """
  def get_or_start(manager, metric_name, labels) do
    state_info = :persistent_term.get({__MODULE__, manager})
    index = state_info.index
    registry = state_info.registry
    encoded = encode_labels(labels)
    key = {metric_name, encoded}

    case :ets.lookup(index, key) do
      [{^key, series_id}] ->
        case Registry.lookup(registry, series_id) do
          [{pid, _}] ->
            {series_id, pid}

          [] ->
            # Process died — restart via GenServer
            GenServer.call(manager, {:start_series, series_id, metric_name, labels})
        end

      [] ->
        GenServer.call(manager, {:get_or_start, metric_name, labels})
    end
  end

  @doc """
  Find all series matching a metric name and label filter.
  Returns `[{series_id, labels, pid}]`.
  """
  def find_series(manager, metric_name, label_filter) do
    state_info = :persistent_term.get({__MODULE__, manager})
    index = state_info.index
    registry = state_info.registry

    # Get all series for this metric
    pattern = {{metric_name, :_}, :_}

    :ets.match_object(index, pattern)
    |> Enum.map(fn {{_metric, encoded_labels}, series_id} ->
      {series_id, decode_labels(encoded_labels)}
    end)
    |> filter_by_labels(label_filter)
    |> Enum.flat_map(fn {series_id, labels} ->
      case Registry.lookup(registry, series_id) do
        [{pid, _}] -> [{series_id, labels, pid}]
        [] -> []
      end
    end)
  end

  @doc "List all unique metric names."
  def list_metrics(manager) do
    state_info = :persistent_term.get({__MODULE__, manager})
    db = state_info.db

    {:ok, rows} =
      TimelessMetrics.DB.read(db, "SELECT DISTINCT metric_name FROM series ORDER BY metric_name")

    Enum.map(rows, fn [name] -> name end)
  end

  @doc "List all series (labels) for a given metric."
  def list_series(manager, metric_name) do
    state_info = :persistent_term.get({__MODULE__, manager})
    db = state_info.db

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT labels FROM series WHERE metric_name = ?1 ORDER BY labels",
        [metric_name]
      )

    Enum.map(rows, fn [labels_str] -> %{labels: decode_labels(labels_str)} end)
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
    db = state_info.db

    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
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

    # Create ETS index: {metric_name, encoded_labels} => series_id
    index = :"#{store}_actor_index"

    :ets.new(index, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    state = %__MODULE__{
      store: store,
      db: db,
      data_dir: data_dir,
      registry: registry,
      dynamic_sup: dynamic_sup,
      index: index,
      max_blocks: max_blocks,
      block_size: block_size,
      compression: compression,
      flush_interval: flush_interval
    }

    # Store in persistent_term for fast client-side access
    :persistent_term.put({__MODULE__, name}, %{
      index: index,
      registry: registry,
      db: db
    })

    # Recovery: load all series from DB and start processes
    recover_series(state)

    {:ok, state}
  end

  @impl true
  def handle_call({:get_or_start, metric_name, labels}, _from, state) do
    encoded = encode_labels(labels)
    key = {metric_name, encoded}

    # Double-check ETS (another process may have registered it)
    case :ets.lookup(state.index, key) do
      [{^key, series_id}] ->
        case Registry.lookup(state.registry, series_id) do
          [{pid, _}] ->
            {:reply, {series_id, pid}, state}

          [] ->
            {:reply, start_series_process(state, series_id, metric_name, labels), state}
        end

      [] ->
        # Create in SQLite
        now = System.os_time(:second)

        {:ok, [[series_id]]} =
          TimelessMetrics.DB.write(
            state.db,
            "INSERT INTO series (metric_name, labels, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(metric_name, labels) DO UPDATE SET created_at = created_at RETURNING id",
            [metric_name, encoded, now]
          )

        # Insert into ETS index
        :ets.insert(state.index, {key, series_id})

        {:reply, start_series_process(state, series_id, metric_name, labels), state}
    end
  end

  def handle_call({:start_series, series_id, metric_name, labels}, _from, state) do
    {:reply, start_series_process(state, series_id, metric_name, labels), state}
  end

  # --- Internals ---

  defp recover_series(state) do
    {:ok, rows} =
      TimelessMetrics.DB.read(state.db, "SELECT id, metric_name, labels FROM series")

    Enum.each(rows, fn [id, metric_name, encoded_labels] ->
      labels = decode_labels(encoded_labels)
      :ets.insert(state.index, {{metric_name, encoded_labels}, id})
      start_series_process(state, id, metric_name, labels)
    end)
  end

  defp start_series_process(state, series_id, metric_name, labels) do
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
             flush_interval: state.flush_interval
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

    Enum.filter(series_list, fn {_id, labels} ->
      Enum.all?(compiled, fn
        {k, {:compiled_regex, regex}} ->
          case Map.get(labels, k) do
            nil -> false
            val -> Regex.match?(regex, val)
          end

        {k, v} ->
          Map.get(labels, k) == v
      end)
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
