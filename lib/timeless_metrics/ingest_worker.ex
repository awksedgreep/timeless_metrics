defmodule TimelessMetrics.IngestWorker do
  @moduledoc """
  Background ingest worker that drains raw HTTP bodies from an ETS queue,
  parses them, resolves series, and writes to Buffer shards.

  The HTTP handler inserts raw bodies into the queue and returns 204
  immediately. This worker processes them in the background.

  Multiple workers share one ETS queue table. Each worker takes batches
  using :ets.take (atomic) to avoid contention.
  """

  use GenServer

  @drain_interval_ms 10
  @max_batch 100

  defstruct [:store, :queue, :worker_id, :format]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Queue a raw HTTP body for background processing.

  Called from the HTTP handler. Returns immediately.
  `format` is `:prometheus` or `:json`.
  """
  def enqueue(queue_table, body, format) do
    key = :erlang.unique_integer([:positive, :monotonic])
    :ets.insert(queue_table, {key, :ezstd.compress(body, 2), format})
    :ok
  end

  # --- Server ---

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    queue = Keyword.fetch!(opts, :queue)
    worker_id = Keyword.fetch!(opts, :worker_id)

    Process.flag(:trap_exit, true)
    schedule_drain()

    {:ok,
     %__MODULE__{
       store: store,
       queue: queue,
       worker_id: worker_id
     }}
  end

  @impl true
  def handle_info(:drain, state) do
    drain_batch(state)
    schedule_drain()
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Drain everything remaining on shutdown
    drain_all(state)
    :ok
  end

  # --- Internals ---

  defp schedule_drain do
    Process.send_after(self(), :drain, @drain_interval_ms)
  end

  defp drain_batch(state) do
    # Take up to @max_batch entries atomically
    entries = take_batch(state.queue, @max_batch)

    if entries != [] do
      process_entries(entries, state)
      # If we got a full batch, drain again immediately
      if length(entries) >= @max_batch do
        drain_batch(state)
      end
    end
  end

  defp drain_all(state) do
    entries = take_batch(state.queue, 10_000)

    if entries != [] do
      process_entries(entries, state)
      drain_all(state)
    end
  end

  defp take_batch(queue, limit) do
    # Use first/next + take for atomic per-entry removal
    take_loop(queue, :ets.first(queue), limit, [])
  end

  defp take_loop(_queue, :"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp take_loop(_queue, _key, 0, acc), do: Enum.reverse(acc)

  defp take_loop(queue, key, remaining, acc) do
    next = :ets.next(queue, key)

    case :ets.take(queue, key) do
      [{^key, body, format}] ->
        take_loop(queue, next, remaining - 1, [{key, body, format} | acc])

      [] ->
        # Another worker took it
        take_loop(queue, next, remaining, acc)
    end
  end

  defp process_entries(entries, state) do
    registry = :"#{state.store}_registry"
    shard_count = :persistent_term.get({TimelessMetrics, state.store, :shard_count})

    Enum.each(entries, fn {_key, compressed_body, format} ->
      try do
        body = :ezstd.decompress(compressed_body)

        case format do
          :prometheus -> process_prometheus(body, state.store, registry, shard_count)
          :json -> process_json(body, state.store, registry, shard_count)
        end
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end
    end)
  end

  defp process_prometheus(body, store, registry, shard_count) do
    {groups, count, _errors, _samples} =
      if TimelessMetrics.PrometheusNif.available?() do
        parse_prometheus_nif(body)
      else
        parse_prometheus_elixir(body)
      end

    if count > 0 do
      TimelessMetrics.Stats.incr_writes(store)
      TimelessMetrics.Stats.add_points(store, count)

      Enum.each(groups, fn {{metric_name, labels}, batch} ->
        series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
        shard_idx = rem(abs(series_id), shard_count)
        points = Enum.map(batch, fn {ts, val} -> {series_id, ts, val} end)
        TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
      end)
    end
  end

  defp process_json(body, store, registry, shard_count) do
    lines = :binary.split(body, <<"\n">>, [:global, :trim_all])

    {groups, count} =
      Enum.reduce(lines, {%{}, 0}, fn line, {groups, count} ->
        case parse_json_line(line) do
          {:ok, line_groups, line_count} ->
            merged =
              Enum.reduce(line_groups, groups, fn {key, points}, acc ->
                Map.update(acc, key, points, &(points ++ &1))
              end)

            {merged, count + line_count}

          :error ->
            {groups, count}
        end
      end)

    if count > 0 do
      TimelessMetrics.Stats.incr_writes(store)
      TimelessMetrics.Stats.add_points(store, count)

      Enum.each(groups, fn {{metric_name, labels}, batch} ->
        series_id = TimelessMetrics.SeriesRegistry.get_or_create(registry, metric_name, labels)
        shard_idx = rem(abs(series_id), shard_count)
        points = Enum.map(batch, fn {ts, val} -> {series_id, ts, val} end)
        TimelessMetrics.Buffer.write_bulk(:"#{store}_shard_#{shard_idx}", points)
      end)
    end
  end

  # --- Parsers (extracted from HTTP module) ---

  defp parse_prometheus_nif(body) do
    {entries, error_count} = TimelessMetrics.PrometheusNif.parse(body)
    now = System.os_time(:second)

    groups =
      Enum.reduce(entries, %{}, fn {metric, labels_proplist, value, ts_ms}, acc ->
        labels = Map.new(labels_proplist)
        ts = if ts_ms == 0, do: now, else: div(ts_ms, 1000)
        key = {metric, labels}
        val = if is_integer(value), do: value / 1, else: value
        Map.update(acc, key, [{ts, val}], fn existing -> [{ts, val} | existing] end)
      end)

    count = length(entries)
    {groups, count, error_count, []}
  end

  defp parse_prometheus_elixir(body) do
    lines = :binary.split(body, <<"\n">>, [:global, :trim_all])
    now = System.os_time(:second)

    Enum.reduce(lines, {%{}, 0, 0, []}, fn line, {groups, count, errors, samples} ->
      line = String.trim(line)

      cond do
        line == "" or String.starts_with?(line, "#") ->
          {groups, count, errors, samples}

        true ->
          case parse_prometheus_line(line, now) do
            {:ok, metric, labels, value, ts} ->
              key = {metric, labels}

              {Map.update(groups, key, [{ts, value}], fn e -> [{ts, value} | e] end), count + 1,
               errors, samples}

            :error ->
              {groups, count, errors + 1, samples}
          end
      end
    end)
  end

  defp parse_prometheus_line(line, now) do
    case String.split(line, " ") do
      [metric_labels, value_str | rest] ->
        case Float.parse(value_str) do
          {value, _} ->
            ts =
              case rest do
                [ts_str | _] ->
                  case Integer.parse(ts_str) do
                    {ts_ms, _} -> div(ts_ms, 1000)
                    _ -> now
                  end

                _ ->
                  now
              end

            {metric, labels} = parse_metric_labels(metric_labels)
            {:ok, metric, labels, value, ts}

          :error ->
            :error
        end

      _ ->
        :error
    end
  end

  defp parse_metric_labels(str) do
    case String.split(str, "{", parts: 2) do
      [metric, labels_str] ->
        labels_str = String.trim_trailing(labels_str, "}")

        labels =
          labels_str
          |> String.split(",")
          |> Enum.reduce(%{}, fn pair, acc ->
            case String.split(pair, "=", parts: 2) do
              [k, v] -> Map.put(acc, k, String.trim(v, "\""))
              _ -> acc
            end
          end)

        {metric, labels}

      [metric] ->
        {metric, %{}}
    end
  end

  defp parse_json_line(line) do
    case safe_json_decode(line) do
      %{"metric" => metric_map, "values" => values, "timestamps" => timestamps}
      when is_list(values) and is_list(timestamps) and length(values) == length(timestamps) ->
        {name, labels} = extract_metric(metric_map)

        try do
          points = zip_points(timestamps, values, name, labels)
          grouped = Enum.group_by(points, fn {key, _} -> key end, fn {_, pt} -> pt end)
          count = length(points)
          {:ok, grouped, count}
        catch
          :throw, :bad_entry -> :error
        end

      _ ->
        :error
    end
  end

  defp zip_points([], [], _name, _labels), do: []

  defp zip_points([ts | tsr], [v | vr], name, labels) when is_integer(ts) do
    val = if is_number(v), do: if(is_integer(v), do: v / 1, else: v), else: throw(:bad_entry)
    [{{name, labels}, {div(ts, 1000), val}} | zip_points(tsr, vr, name, labels)]
  end

  defp zip_points(_, _, _, _), do: throw(:bad_entry)

  defp extract_metric(metric_map) do
    {name, labels} = Map.pop(metric_map, "__name__", "unknown")
    {name, labels}
  end

  defp safe_json_decode(bin) do
    :json.decode(bin)
  catch
    :error, _ -> :error
  end
end
