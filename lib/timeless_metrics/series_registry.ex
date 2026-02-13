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

  defstruct [:forward_key, :reverse_key, :overflow, :db, :name, :dirty]

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
          [{^key, id}] -> id
          [] -> GenServer.call(registry, {:register, metric_name, labels, key})
        end

      id ->
        id
    end
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
    db = Keyword.fetch!(opts, :db)
    fwd_key = forward_key(name)
    rev_key = reverse_key(name)

    # ETS overflow for new series (fast writes, no literal-heap churn)
    overflow = overflow_table(name)

    :ets.new(overflow, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    # Initialize persistent_term with empty maps, then bulk-load from SQLite
    :persistent_term.put(fwd_key, %{})
    :persistent_term.put(rev_key, %{})
    load_from_db(fwd_key, rev_key, db)

    schedule_publish()

    {:ok,
     %__MODULE__{
       forward_key: fwd_key,
       reverse_key: rev_key,
       overflow: overflow,
       db: db,
       name: name,
       dirty: false
     }}
  end

  @impl true
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
            labels_json = encode_labels(labels)
            now = System.os_time(:second)

            result =
              TimelessMetrics.DB.write(
                state.db,
                "INSERT INTO series (metric_name, labels, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(metric_name, labels) DO UPDATE SET created_at = created_at RETURNING id",
                [metric_name, labels_json, now]
              )

            {:ok, [[id]]} = result

            # Write to ETS overflow (immediate visibility, no literal-heap cost)
            :ets.insert(state.overflow, {key, id})
            :ets.insert(state.overflow, {{:reverse, id}, metric_name, labels})

            {:reply, id, %{state | dirty: true}}
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
    publish_overflow(state)
    schedule_publish()
    {:noreply, %{state | dirty: false}}
  end

  # --- Internals ---

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
