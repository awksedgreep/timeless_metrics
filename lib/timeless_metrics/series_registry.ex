defmodule TimelessMetrics.SeriesRegistry do
  @moduledoc """
  Hybrid persistent_term + ETS series ID lookup and registration.

  The hot read path uses persistent_term (zero-copy shared Map).
  New series spill into an ETS overflow table and are periodically
  batch-merged into persistent_term to avoid literal-heap churn.

  Steady state: 100% persistent_term hits, ETS overflow is empty.
  """

  use GenServer

  @publish_interval_ms 5_000

  defstruct [
    :forward_key,
    :reverse_key,
    :overflow,
    :db,
    :name,
    :store,
    :dirty,
    :next_id,
    :pending_inserts
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Get or create a series ID for the given metric name and labels.
  Fast path: persistent_term Map lookup (zero-copy).
  Warm path: ETS overflow lookup (during warm-up).
  Slow path: GenServer → SQLite → ETS overflow → deferred publish.
  """
  def get_or_create(registry, metric_name, labels) do
    fwd_key = forward_key(registry)
    key = {metric_name, labels}

    case Map.get(:persistent_term.get(fwd_key), key) do
      nil ->
        # Check ETS overflow (recently registered series not yet published)
        overflow = overflow_table(registry)

        case :ets.lookup(overflow, key) do
          [{^key, id}] ->
            id

          [] ->
            # Lock-free creation: atomically assign an ID and CAS into ETS
            id_counter = :persistent_term.get({__MODULE__, registry, :id_counter})
            id = :atomics.add_get(id_counter, 1, 1)

            # insert_new is atomic — if another process beat us, use their ID
            case :ets.insert_new(overflow, {key, id}) do
              true ->
                # We won the race — register reverse lookup and queue for SQLite
                :ets.insert(overflow, {{:reverse, id}, metric_name, labels})
                store = :persistent_term.get({__MODULE__, registry, :store})
                if store, do: TimelessMetrics.Stats.incr_series_created(store)
                GenServer.cast(registry, {:queue_insert, id, metric_name, labels})
                id

              false ->
                # Another process registered it — use their ID
                [{^key, existing_id}] = :ets.lookup(overflow, key)
                existing_id
            end
        end

      id ->
        id
    end
  end

  @doc "Return the number of registered series."
  def count(registry) do
    fwd_key = forward_key(registry)
    overflow = overflow_table(registry)
    map_size(:persistent_term.get(fwd_key)) + div(:ets.info(overflow, :size), 2)
  end

  @doc "Flush pending series registrations to SQLite synchronously."
  def flush_pending(registry) do
    GenServer.call(registry, :flush_pending, :infinity)
  end

  @doc "Lookup series metadata by ID."
  def lookup(registry, series_id) do
    rev_key = reverse_key(registry)

    case Map.get(:persistent_term.get(rev_key), series_id) do
      nil ->
        overflow = overflow_table(registry)

        case :ets.lookup(overflow, {:reverse, series_id}) do
          [{_, metric_name, labels}] -> {:ok, {metric_name, labels}}
          [] -> :error
        end

      {metric_name, labels} ->
        {:ok, {metric_name, labels}}
    end
  end

  # --- Server ---

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    db = Keyword.get(opts, :db)
    store = Keyword.get(opts, :store)
    fwd_key = forward_key(name)
    rev_key = reverse_key(name)

    # ETS overflow for new series (fast writes, no literal-heap churn)
    overflow = overflow_table(name)

    :ets.new(overflow, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    # Initialize persistent_term with empty maps
    :persistent_term.put(fwd_key, %{})
    :persistent_term.put(rev_key, %{})

    # Load from SQLite if available (server mode)
    if db, do: load_from_db(fwd_key, rev_key, db)

    # Atomics counter for lock-free ID generation from any process
    id_counter = :atomics.new(1, signed: true)

    seed =
      if db do
        case TimelessMetrics.DB.read(db, "SELECT COALESCE(MAX(id), 0) FROM series") do
          {:ok, [[max_id]]} -> max_id
          _ -> 0
        end
      else
        0
      end

    :atomics.put(id_counter, 1, seed)
    :persistent_term.put({__MODULE__, name, :id_counter}, id_counter)

    # Store name for Stats calls from lock-free path
    :persistent_term.put({__MODULE__, name, :store}, store)

    schedule_publish()

    {:ok,
     %__MODULE__{
       forward_key: fwd_key,
       reverse_key: rev_key,
       overflow: overflow,
       db: db,
       name: name,
       store: store,
       dirty: false,
       next_id: nil,
       pending_inserts: []
     }}
  end

  @impl true
  def handle_cast({:queue_insert, id, metric_name, labels}, state) do
    labels_json = encode_labels(labels)
    now = System.os_time(:second)
    pending = [{id, metric_name, labels_json, now} | state.pending_inserts]
    {:noreply, %{state | dirty: true, pending_inserts: pending}}
  end

  @impl true
  def handle_call(:flush_pending, _from, state) do
    flush_pending_inserts(state)
    {:reply, :ok, %{state | pending_inserts: []}}
  end

  def handle_call({:register, metric_name, labels, key}, _from, state) do
    # Double-check persistent_term (may have been published since caller checked)
    forward_map = :persistent_term.get(state.forward_key)

    case Map.get(forward_map, key) do
      nil ->
        # Also check ETS overflow (another process may have registered it)
        case :ets.lookup(state.overflow, key) do
          [{^key, id}] ->
            {:reply, id, state}

          [] ->
            # Assign ID from counter — no SQLite on hot path
            id = state.next_id
            labels_json = encode_labels(labels)
            now = System.os_time(:second)

            if state.store, do: TimelessMetrics.Stats.incr_series_created(state.store)

            # Write to ETS overflow (immediate visibility)
            :ets.insert(state.overflow, {key, id})
            :ets.insert(state.overflow, {{:reverse, id}, metric_name, labels})

            # Queue for async bulk SQLite insert
            pending = [{id, metric_name, labels_json, now} | state.pending_inserts]

            {:reply, id, %{state | next_id: id + 1, dirty: true, pending_inserts: pending}}
        end

      id ->
        {:reply, id, state}
    end
  end

  @impl true
  def handle_info(:publish, %{dirty: false} = state) do
    schedule_publish()
    {:noreply, state}
  end

  def handle_info(:publish, state) do
    flush_pending_inserts(state)
    publish_overflow(state)
    schedule_publish()
    {:noreply, %{state | dirty: false, pending_inserts: []}}
  end

  # --- Internals ---

  defp flush_pending_inserts(%{pending_inserts: []}), do: :ok
  defp flush_pending_inserts(%{db: nil}), do: :ok

  defp flush_pending_inserts(state) do
    # SQLite has a 32,766 parameter limit. 4 params per row = 8,000 rows per batch.
    state.pending_inserts
    |> Enum.reverse()
    |> Enum.chunk_every(8000)
    |> Enum.each(fn chunk ->
      {placeholders, params} =
        chunk
        |> Enum.with_index(1)
        |> Enum.reduce({[], []}, fn {{id, metric, labels_json, ts}, i}, {ph, pa} ->
          base = (i - 1) * 4
          placeholder = "(?#{base + 1}, ?#{base + 2}, ?#{base + 3}, ?#{base + 4})"
          {[placeholder | ph], pa ++ [id, metric, labels_json, ts]}
        end)

      sql =
        "INSERT OR IGNORE INTO series (id, metric_name, labels, created_at) VALUES " <>
          Enum.join(Enum.reverse(placeholders), ", ")

      TimelessMetrics.DB.write(state.db, sql, params)
    end)
  end

  defp publish_overflow(state) do
    entries = :ets.tab2list(state.overflow)

    if entries != [] do
      forward_map = :persistent_term.get(state.forward_key)
      reverse_map = :persistent_term.get(state.reverse_key)

      {new_fwd, new_rev} =
        Enum.reduce(entries, {forward_map, reverse_map}, fn
          {{:reverse, id} = _rev_key, metric_name, labels}, {fwd, rev} ->
            {fwd, Map.put(rev, id, {metric_name, labels})}

          {key, id}, {fwd, rev} ->
            {Map.put(fwd, key, id), rev}
        end)

      :persistent_term.put(state.forward_key, new_fwd)
      :persistent_term.put(state.reverse_key, new_rev)
      :ets.delete_all_objects(state.overflow)
    end
  end

  defp schedule_publish do
    Process.send_after(self(), :publish, @publish_interval_ms)
  end

  defp load_from_db(fwd_key, rev_key, db) do
    {:ok, rows} = TimelessMetrics.DB.read(db, "SELECT id, metric_name, labels FROM series")

    {forward_map, reverse_map} =
      Enum.reduce(rows, {%{}, %{}}, fn [id, metric_name, labels_json], {fwd, rev} ->
        labels = decode_labels(labels_json)
        key = {metric_name, labels}
        {Map.put(fwd, key, id), Map.put(rev, id, {metric_name, labels})}
      end)

    :persistent_term.put(fwd_key, forward_map)
    :persistent_term.put(rev_key, reverse_map)
  end

  defp forward_key(name) when is_atom(name), do: {__MODULE__, name, :forward}
  defp reverse_key(name) when is_atom(name), do: {__MODULE__, name, :reverse}
  defp overflow_table(name) when is_atom(name), do: :"#{name}_series_overflow"

  defp encode_labels(labels) when is_map(labels) do
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
