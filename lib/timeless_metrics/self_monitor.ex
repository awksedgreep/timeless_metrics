defmodule TimelessMetrics.SelfMonitor do
  @moduledoc false

  use GenServer

  @default_interval 15_000

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    interval = Keyword.get(opts, :interval, @default_interval)

    state = %{store: store, interval: interval}
    schedule(interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:collect, state) do
    collect(state.store)
    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :collect, interval)
  end

  defp collect(store) do
    now = System.os_time(:second)
    mem = :erlang.memory()
    info = TimelessMetrics.info(store)
    labels = %{}

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
      {"timeless_buffer_points", labels, info.raw_buffer_points / 1, now}
    ]

    TimelessMetrics.write_batch(store, entries)
  end
end
