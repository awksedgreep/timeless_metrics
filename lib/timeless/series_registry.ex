defmodule Timeless.SeriesRegistry do
  @moduledoc """
  ETS-backed series ID lookup and registration.

  Maps {metric_name, sorted_labels} -> series_id for fast write-path lookups.
  New series are registered in SQLite and cached in ETS.
  """

  use GenServer

  defstruct [:table, :db, :name]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Get or create a series ID for the given metric name and labels.
  Returns the integer series_id. Fast path is an ETS lookup; slow path
  inserts into SQLite and caches.
  """
  def get_or_create(registry, metric_name, labels) do
    table = table_name(registry)
    key = series_key(metric_name, labels)

    case :ets.lookup(table, key) do
      [{^key, id}] -> id
      [] -> GenServer.call(registry, {:register, metric_name, labels, key})
    end
  end

  @doc "Lookup series metadata by ID."
  def lookup(registry, series_id) do
    table = table_name(registry)

    case :ets.lookup(table, {:reverse, series_id}) do
      [{_, metric_name, labels}] -> {:ok, {metric_name, labels}}
      [] -> :error
    end
  end

  # --- Server ---

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    db = Keyword.fetch!(opts, :db)
    table = table_name(name)

    :ets.new(table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    # Load existing series from SQLite into ETS
    load_from_db(table, db)

    {:ok, %__MODULE__{table: table, db: db, name: name}}
  end

  @impl true
  def handle_call({:register, metric_name, labels, key}, _from, state) do
    # Double-check under lock (another process may have registered it)
    case :ets.lookup(state.table, key) do
      [{^key, id}] ->
        {:reply, id, state}

      [] ->
        labels_json = encode_labels(labels)
        now = System.os_time(:second)

        # Try to insert; if UNIQUE conflict, fetch existing
        result =
          Timeless.DB.write(
            state.db,
            "INSERT INTO series (metric_name, labels, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(metric_name, labels) DO UPDATE SET created_at = created_at RETURNING id",
            [metric_name, labels_json, now]
          )

        {:ok, [[id]]} = result

        # Cache in ETS (both forward and reverse lookups)
        :ets.insert(state.table, {key, id})
        :ets.insert(state.table, {{:reverse, id}, metric_name, labels})

        {:reply, id, state}
    end
  end

  # --- Internals ---

  defp load_from_db(table, db) do
    {:ok, rows} = Timeless.DB.read(db, "SELECT id, metric_name, labels FROM series")

    Enum.each(rows, fn [id, metric_name, labels_json] ->
      labels = decode_labels(labels_json)
      key = series_key(metric_name, labels)
      :ets.insert(table, {key, id})
      :ets.insert(table, {{:reverse, id}, metric_name, labels})
    end)
  end

  defp table_name(name) when is_atom(name) do
    :"#{name}_series"
  end

  defp series_key(metric_name, labels) when is_map(labels) do
    {metric_name, labels}
  end

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
