defmodule TimelessMetrics.Actor.SeriesWriter do
  @moduledoc """
  Async bulk writer for series metadata to SQLite.

  Receives casts from SeriesManager and flushes in batches,
  keeping the series creation hot path free of SQLite latency.
  """

  use GenServer

  @flush_interval_ms 100
  @max_batch 1000

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Queue a series registration for bulk insert."
  def register(writer, series_id, metric_name, encoded_labels, created_at, series_type) do
    GenServer.cast(
      writer,
      {:register, series_id, metric_name, encoded_labels, created_at, series_type}
    )
  end

  @doc "Flush pending writes synchronously. Used before metadata queries."
  def flush_sync(writer) do
    GenServer.call(writer, :flush_sync)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    flush_ref = Process.send_after(self(), :flush, @flush_interval_ms)

    {:ok, %{db: db, buffer: [], count: 0, flush_ref: flush_ref}}
  end

  @impl true
  def handle_cast(
        {:register, series_id, metric_name, encoded_labels, created_at, series_type},
        state
      ) do
    state = %{
      state
      | buffer: [{series_id, metric_name, encoded_labels, created_at, series_type} | state.buffer],
        count: state.count + 1
    }

    if state.count >= @max_batch do
      {:noreply, flush(state)}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:flush_sync, _from, state) do
    {:reply, :ok, flush(state)}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush(state)
    flush_ref = Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, %{state | flush_ref: flush_ref}}
  end

  defp flush(%{count: 0} = state), do: state

  defp flush(state) do
    # Build multi-row INSERT
    {placeholders, params} =
      state.buffer
      |> Enum.reverse()
      |> Enum.with_index(1)
      |> Enum.reduce({[], []}, fn {{id, metric, labels, ts, type}, i}, {ph, pa} ->
        base = (i - 1) * 5
        placeholder = "(?#{base + 1}, ?#{base + 2}, ?#{base + 3}, ?#{base + 4}, ?#{base + 5})"
        {[placeholder | ph], pa ++ [id, metric, labels, ts, type]}
      end)

    sql =
      "INSERT OR IGNORE INTO series (id, metric_name, labels, created_at, series_type) VALUES " <>
        Enum.join(Enum.reverse(placeholders), ", ")

    TimelessMetrics.DB.write(state.db, sql, params)

    %{state | buffer: [], count: 0}
  end
end
