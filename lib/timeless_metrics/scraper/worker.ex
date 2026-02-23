defmodule TimelessMetrics.Scraper.Worker do
  @moduledoc false

  use GenServer
  require Logger

  alias TimelessMetrics.Scraper.{Target, Relabel}

  defstruct [
    :target,
    :store,
    :db,
    :anchor,
    :scrape_count,
    :health,
    :last_scrape,
    :last_duration_ms,
    :last_error,
    :samples_scraped,
    :health_flush_counter,
    registered_names: MapSet.new()
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    target = Keyword.fetch!(opts, :target)
    store = Keyword.fetch!(opts, :store)
    db = Keyword.fetch!(opts, :db)

    anchor = System.monotonic_time(:millisecond)
    interval_ms = target.scrape_interval * 1000
    jitter = Enum.random(0..interval_ms)

    state = %__MODULE__{
      target: target,
      store: store,
      db: db,
      anchor: anchor,
      scrape_count: 0,
      health: :unknown,
      last_scrape: nil,
      last_duration_ms: nil,
      last_error: nil,
      samples_scraped: 0,
      health_flush_counter: 0
    }

    Process.send_after(self(), :scrape, jitter)

    {:ok, state}
  end

  @impl true
  def handle_info(:scrape, state) do
    state = schedule_next(state)
    state = do_scrape(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    flush_health(state)
  end

  # --- Scrape logic ---

  defp do_scrape(state) do
    target = state.target
    url = Target.url(target)
    timeout_ms = target.scrape_timeout * 1000
    start_time = System.monotonic_time(:millisecond)
    now = System.os_time(:second)

    {result, duration_ms} =
      try do
        resp = Req.get!(url, receive_timeout: timeout_ms, connect_options: [timeout: timeout_ms])
        elapsed = System.monotonic_time(:millisecond) - start_time

        if resp.status == 200 do
          {{:ok, resp.body}, elapsed}
        else
          {{:error, "HTTP #{resp.status}"}, elapsed}
        end
      rescue
        e ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          {{:error, Exception.message(e)}, elapsed}
      end

    state = %{state | last_scrape: now, last_duration_ms: duration_ms}

    state =
      case result do
        {:ok, body} ->
          {entries, _error_count} = parse_body(body)
          entries = apply_relabeling(entries, target)

          count = length(entries)

          if entries != [] do
            final_entries = finalize_entries(entries, target, now)
            TimelessMetrics.write_batch(state.store, final_entries)
          end

          registered = maybe_register_metadata(state, entries)
          write_self_metrics(registered, 1, duration_ms, count, now)

          prev_health = registered.health
          registered = %{registered | health: :up, last_error: nil, samples_scraped: count}
          maybe_flush_health(registered, prev_health)

        {:error, reason} ->
          Logger.warning("Scrape failed for #{target.job_name}/#{target.address}: #{reason}")

          write_self_metrics(state, 0, duration_ms, 0, now)

          prev_health = state.health
          state = %{state | health: :down, last_error: reason, samples_scraped: 0}
          maybe_flush_health(state, prev_health)
      end

    state
  end

  defp parse_body(body) do
    if TimelessMetrics.PrometheusNif.available?() do
      TimelessMetrics.PrometheusNif.parse(body)
    else
      parse_body_elixir(body)
    end
  end

  defp parse_body_elixir(body) do
    now_ms = System.os_time(:millisecond)

    entries =
      body
      |> String.split("\n", trim: true)
      |> Enum.reduce([], fn line, acc ->
        line = String.trim(line)

        if line == "" or String.starts_with?(line, "#") do
          acc
        else
          case parse_prom_line(line, now_ms) do
            {:ok, entry} -> [entry | acc]
            :error -> acc
          end
        end
      end)

    {entries, 0}
  end

  defp parse_prom_line(line, now_ms) do
    case :binary.match(line, <<"{">>) do
      {pos, 1} ->
        metric = :binary.part(line, 0, pos)
        rest = :binary.part(line, pos + 1, byte_size(line) - pos - 1)

        case :binary.split(rest, <<"} ">>) do
          [labels_str, value_ts] ->
            labels = parse_labels(labels_str)
            parse_value_ts(String.trim(value_ts), metric, labels, now_ms)

          _ ->
            :error
        end

      :nomatch ->
        case :binary.split(line, <<" ">>) do
          [metric, value_ts] ->
            parse_value_ts(String.trim(value_ts), metric, [], now_ms)

          _ ->
            :error
        end
    end
  end

  defp parse_labels(str) when byte_size(str) == 0, do: []

  defp parse_labels(str) do
    str
    |> :binary.split(<<",">>, [:global])
    |> Enum.flat_map(fn pair ->
      case :binary.split(pair, <<"=">>) do
        [key, value] ->
          v = String.trim(value)
          v = if String.starts_with?(v, "\""), do: String.slice(v, 1..-2//1), else: v
          [{String.trim(key), v}]

        _ ->
          []
      end
    end)
  end

  defp parse_value_ts(str, metric, labels, now_ms) do
    case :binary.split(str, <<" ">>) do
      [value_str, ts_str] ->
        with {value, _} <- Float.parse(value_str),
             {ts, _} <- Integer.parse(ts_str) do
          {:ok, {metric, labels, value, ts}}
        else
          _ -> :error
        end

      [value_str] ->
        case Float.parse(value_str) do
          {value, _} -> {:ok, {metric, labels, value, now_ms}}
          :error -> :error
        end
    end
  end

  defp apply_relabeling(entries, target) do
    configs = target.metric_relabel_configs

    if configs == nil do
      entries
    else
      Enum.flat_map(entries, fn {name, labels_proplist, value, ts} ->
        # Build labels map with __name__
        labels_map =
          labels_proplist
          |> proplist_to_map()
          |> Map.put("__name__", name)

        case Relabel.apply_configs(labels_map, configs) do
          :drop ->
            []

          {:ok, relabeled} ->
            {new_name, final_labels} = Map.pop(relabeled, "__name__", name)
            final_labels = Relabel.drop_meta_labels(final_labels)
            [{new_name, map_to_proplist(final_labels), value, ts}]
        end
      end)
    end
  end

  defp finalize_entries(entries, target, now) do
    meta_labels = Relabel.target_meta_labels(target)
    target_labels = Map.merge(meta_labels, target.labels) |> Relabel.drop_meta_labels()
    honor = target.honor_labels

    Enum.map(entries, fn {name, labels_proplist, value, ts} ->
      scraped_labels = proplist_to_map(labels_proplist)

      final_labels =
        Relabel.apply_honor_labels(scraped_labels, target_labels, honor)

      # Timestamp: NIF returns ms, 0 = no timestamp. Elixir fallback also returns ms.
      timestamp =
        cond do
          not target.honor_timestamps -> now
          ts == 0 -> now
          ts > 1_000_000_000_000 -> div(ts, 1000)
          true -> ts
        end

      {name, final_labels, value, timestamp}
    end)
  end

  defp write_self_metrics(state, up_val, duration_ms, samples, now) do
    target = state.target
    meta = %{"job" => target.job_name, "instance" => target.address}
    duration_s = duration_ms / 1000.0

    entries = [
      {"up", meta, up_val / 1, now},
      {"scrape_duration_seconds", meta, duration_s, now},
      {"scrape_samples_scraped", meta, samples / 1, now}
    ]

    TimelessMetrics.write_batch(state.store, entries)
  end

  # --- Scheduling ---

  defp schedule_next(state) do
    count = state.scrape_count + 1
    interval_ms = state.target.scrape_interval * 1000
    next = state.anchor + count * interval_ms
    now = System.monotonic_time(:millisecond)
    delay = max(next - now, 0)

    Process.send_after(self(), :scrape, delay)

    %{state | scrape_count: count}
  end

  # --- Health flushing ---

  defp maybe_flush_health(state, prev_health) do
    counter = state.health_flush_counter + 1
    state = %{state | health_flush_counter: counter}

    if state.health != prev_health or rem(counter, 10) == 0 do
      flush_health(state)
      %{state | health_flush_counter: 0}
    else
      state
    end
  end

  defp flush_health(state) do
    target = state.target

    if target.id do
      TimelessMetrics.DB.write(
        state.db,
        """
        INSERT OR REPLACE INTO scrape_health
          (target_id, health, last_scrape, last_duration_ms, last_error, samples_scraped)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        """,
        [
          target.id,
          to_string(state.health),
          state.last_scrape,
          state.last_duration_ms,
          state.last_error,
          state.samples_scraped
        ]
      )
    end
  end

  defp maybe_register_metadata(state, entries) do
    new_names =
      entries
      |> Enum.map(&elem(&1, 0))
      |> Enum.uniq()
      |> Enum.reject(&MapSet.member?(state.registered_names, &1))

    if new_names == [] do
      state
    else
      Enum.each(new_names, fn name ->
        {type, unit} = infer_type_and_unit(name)
        TimelessMetrics.register_metric(state.store, name, type, unit: unit)
      end)

      %{state | registered_names: Enum.reduce(new_names, state.registered_names, &MapSet.put(&2, &1))}
    end
  end

  defp infer_type_and_unit(name) do
    cond do
      String.ends_with?(name, "_bytes") -> {:gauge, "byte"}
      String.ends_with?(name, "_seconds") -> {:gauge, "second"}
      String.ends_with?(name, "_milliseconds") -> {:gauge, "millisecond"}
      String.ends_with?(name, "_total") -> {:counter, nil}
      String.ends_with?(name, "_count") -> {:counter, nil}
      String.ends_with?(name, "_ratio") -> {:gauge, "ratio"}
      String.ends_with?(name, "_percent") -> {:gauge, "percent"}
      true -> {:gauge, nil}
    end
  end

  defp proplist_to_map(proplist) when is_list(proplist), do: Map.new(proplist)
  defp proplist_to_map(map) when is_map(map), do: map

  defp map_to_proplist(map) when is_map(map), do: Enum.to_list(map)
  defp map_to_proplist(list) when is_list(list), do: list
end
