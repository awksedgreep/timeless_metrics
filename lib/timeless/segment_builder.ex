defmodule Timeless.SegmentBuilder do
  @moduledoc """
  Accumulates points per series and writes gorilla-compressed segments to SQLite.

  Points arrive in batches from the Buffer shards. The SegmentBuilder groups them
  into time-bounded segments (default 1 hour). When a segment boundary is crossed
  or a flush is forced, the segment is compressed with GorillaStream + zstd and
  written to the raw_segments table.
  """

  use GenServer

  defstruct [:db, :segments, :segment_duration, :compression]

  @default_segment_duration 3_600  # 1 hour in seconds

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Ingest a batch of grouped points. Called by Buffer shards after flush."
  def ingest(builder, grouped_points) do
    GenServer.cast(builder, {:ingest, grouped_points})
  end

  @doc "Synchronous ingest. Used during shutdown to ensure data is received before termination."
  def ingest_sync(builder, grouped_points) do
    GenServer.call(builder, {:ingest, grouped_points}, :infinity)
  end

  @doc "Force flush all open segments to disk."
  def flush(builder) do
    GenServer.call(builder, :flush, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    segment_duration = Keyword.get(opts, :segment_duration, @default_segment_duration)
    compression = Keyword.get(opts, :compression, :zstd)

    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      db: db,
      segments: %{},
      segment_duration: segment_duration,
      compression: compression
    }

    # Periodic check for completed segments
    Process.send_after(self(), :check_segments, :timer.seconds(10))

    {:ok, state}
  end

  @impl true
  def handle_cast({:ingest, grouped_points}, state) do
    new_segments =
      Enum.reduce(grouped_points, state.segments, fn {series_id, points}, segments ->
        Enum.reduce(points, segments, fn {ts, val}, segs ->
          bucket = segment_bucket(ts, state.segment_duration)
          key = {series_id, bucket}

          seg =
            Map.get(segs, key, %{
              series_id: series_id,
              start_time: bucket,
              end_time: bucket + state.segment_duration,
              points: []
            })

          updated = %{seg | points: [{ts, val} | seg.points]}
          Map.put(segs, key, updated)
        end)
      end)

    # Check if any segments are complete (their time window has passed)
    {completed, pending} = split_completed(new_segments, state.segment_duration)

    if completed != [] do
      write_segments(completed, state)
    end

    {:noreply, %{state | segments: pending}}
  end

  @impl true
  def handle_call({:ingest, grouped_points}, _from, state) do
    {:noreply, new_state} = handle_cast({:ingest, grouped_points}, state)
    {:reply, :ok, new_state}
  end

  def handle_call(:flush, _from, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    {:reply, :ok, %{state | segments: %{}}}
  end

  @impl true
  def handle_info(:check_segments, state) do
    {completed, pending} = split_completed(state.segments, state.segment_duration)

    if completed != [] do
      write_segments(completed, state)
    end

    Process.send_after(self(), :check_segments, :timer.seconds(10))
    {:noreply, %{state | segments: pending}}
  end

  @impl true
  def terminate(_reason, state) do
    all_segments = Map.values(state.segments)

    if all_segments != [] do
      write_segments(all_segments, state)
    end

    :ok
  end

  # --- Internals ---

  defp segment_bucket(timestamp, duration) do
    div(timestamp, duration) * duration
  end

  defp split_completed(segments, duration) do
    now = System.os_time(:second)
    current_bucket = segment_bucket(now, duration)

    {completed_map, pending_map} =
      Enum.split_with(segments, fn {{_series_id, bucket}, _seg} ->
        bucket < current_bucket
      end)

    completed = Enum.map(completed_map, fn {_key, seg} -> seg end)
    pending = Map.new(pending_map)

    {completed, pending}
  end

  defp write_segments(segments, state) do
    Timeless.DB.write_transaction(state.db, fn conn ->
      Enum.each(segments, fn seg ->
        # Sort points by timestamp for optimal gorilla compression
        sorted_points = Enum.sort_by(seg.points, &elem(&1, 0))

        case GorillaStream.compress(sorted_points, compression: state.compression) do
          {:ok, blob} ->
            point_count = length(sorted_points)
            {first_ts, _} = List.first(sorted_points)
            {last_ts, _} = List.last(sorted_points)

            Timeless.DB.execute(
              conn,
              """
              INSERT OR REPLACE INTO raw_segments (series_id, start_time, end_time, point_count, data)
              VALUES (?1, ?2, ?3, ?4, ?5)
              """,
              [seg.series_id, first_ts, last_ts, point_count, blob]
            )

            :telemetry.execute(
              [:timeless, :segment, :write],
              %{point_count: point_count, compressed_bytes: byte_size(blob)},
              %{series_id: seg.series_id}
            )

          {:error, reason} ->
            require Logger
            Logger.warning("Failed to compress segment for series #{seg.series_id}: #{inspect(reason)}")
        end
      end)
    end)
  end
end
