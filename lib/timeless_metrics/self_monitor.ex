defmodule TimelessMetrics.SelfMonitor do
  @moduledoc false

  use GenServer

  alias TimelessMetrics.Stats

  @default_interval 15_000

  @counter_keys [
    :writes_total,
    :points_ingested,
    :series_created,
    :http_imports,
    :http_queries,
    :http_import_errors,
    :merges_completed,
    :points_merged
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    interval = Keyword.get(opts, :interval, @default_interval)

    state = %{
      store: store,
      interval: interval,
      prev_counters: Stats.snapshot(store),
      prev_mono: System.monotonic_time(:millisecond)
    }

    schedule(interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:collect, state) do
    state = collect(state)
    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :collect, interval)
  end

  defp collect(state) do
    store = state.store
    now = System.os_time(:second)
    mono_now = System.monotonic_time(:millisecond)
    mem = :erlang.memory()
    info = TimelessMetrics.info(store)
    counters = Stats.snapshot(store)
    labels = %{}

    elapsed_ms = mono_now - state.prev_mono
    elapsed_s = max(elapsed_ms / 1000.0, 0.001)

    rates =
      Map.new(@counter_keys, fn key ->
        delta = Map.fetch!(counters, key) - Map.fetch!(state.prev_counters, key)
        {key, delta / elapsed_s}
      end)

    entries = [
      # BEAM memory
      {"vm_memory_total_bytes", labels, mem[:total] / 1, now},
      {"vm_memory_processes_bytes", labels, mem[:processes] / 1, now},
      {"vm_memory_ets_bytes", labels, mem[:ets] / 1, now},
      {"vm_memory_binary_bytes", labels, mem[:binary] / 1, now},
      {"vm_memory_atom_bytes", labels, mem[:atom] / 1, now},
      {"vm_memory_system_bytes", labels, mem[:system] / 1, now},

      # BEAM processes & schedulers
      {"vm_process_count", labels, :erlang.system_info(:process_count) / 1, now},
      {"vm_port_count", labels, :erlang.system_info(:port_count) / 1, now},
      {"vm_run_queue_length", labels, :erlang.statistics(:total_run_queue_lengths_all) / 1, now},

      # TimelessMetrics store stats
      {"timeless_series_count", labels, info.series_count / 1, now},
      {"timeless_total_points", labels, info.total_points / 1, now},
      {"timeless_storage_bytes", labels, info.storage_bytes / 1, now},
      {"timeless_buffer_points", labels, info.raw_buffer_points / 1, now},

      # Counter totals
      {"timeless_writes_total", labels, counters.writes_total / 1, now},
      {"timeless_points_ingested_total", labels, counters.points_ingested / 1, now},
      {"timeless_series_created_total", labels, counters.series_created / 1, now},
      {"timeless_http_imports_total", labels, counters.http_imports / 1, now},
      {"timeless_http_queries_total", labels, counters.http_queries / 1, now},
      {"timeless_http_import_errors_total", labels, counters.http_import_errors / 1, now},
      {"timeless_merges_completed_total", labels, counters.merges_completed / 1, now},
      {"timeless_points_merged_total", labels, counters.points_merged / 1, now},

      # Counter rates (per second)
      {"timeless_writes_per_second", labels, rates.writes_total, now},
      {"timeless_points_per_second", labels, rates.points_ingested, now},
      {"timeless_series_created_per_second", labels, rates.series_created, now},
      {"timeless_http_imports_per_second", labels, rates.http_imports, now},
      {"timeless_http_queries_per_second", labels, rates.http_queries, now},
      {"timeless_http_import_errors_per_second", labels, rates.http_import_errors, now},
      {"timeless_merges_per_second", labels, rates.merges_completed, now},
      {"timeless_points_merged_per_second", labels, rates.points_merged, now}
    ]

    TimelessMetrics.write_batch(store, entries)

    %{state | prev_counters: counters, prev_mono: mono_now}
  end
end
