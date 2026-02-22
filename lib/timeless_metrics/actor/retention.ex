defmodule TimelessMetrics.Actor.Retention do
  @moduledoc """
  Periodic retention enforcement for the actor engine.

  Drops expired blocks and raw buffer entries from SeriesServer processes,
  deletes old daily rollup rows, and cleans up orphan series.
  """

  use GenServer

  defstruct [:store, :db, :manager, :registry,
             :raw_retention_seconds, :daily_retention_seconds,
             :interval, :timer_ref]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force retention enforcement."
  def enforce(server) do
    GenServer.call(server, :enforce, :infinity)
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    db = Keyword.fetch!(opts, :db)
    manager = Keyword.fetch!(opts, :manager)
    registry = Keyword.fetch!(opts, :registry)
    raw_retention = Keyword.get(opts, :raw_retention_seconds, 604_800)
    daily_retention = Keyword.get(opts, :daily_retention_seconds, 31_536_000)
    interval = Keyword.get(opts, :interval, :timer.hours(1))

    state = %__MODULE__{
      store: store,
      db: db,
      manager: manager,
      registry: registry,
      raw_retention_seconds: raw_retention,
      daily_retention_seconds: daily_retention,
      interval: interval
    }

    timer_ref = Process.send_after(self(), :tick, interval)
    {:ok, %{state | timer_ref: timer_ref}}
  end

  @impl true
  def handle_call(:enforce, _from, state) do
    do_enforce(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    do_enforce(state)
    timer_ref = Process.send_after(self(), :tick, state.interval)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  defp do_enforce(state) do
    now = System.os_time(:second)

    # 1. Raw retention: drop expired blocks and raw buffer entries
    raw_cutoff = now - state.raw_retention_seconds
    pids = all_series_pids(state.registry)

    orphans =
      pids
      |> Task.async_stream(
        fn {series_id, pid} ->
          try do
            case GenServer.call(pid, {:enforce_retention, raw_cutoff}, :infinity) do
              {:ok, _dropped, empty?} when empty? -> {:orphan, series_id, pid}
              _ -> :ok
            end
          catch
            :exit, _ -> :ok
          end
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.flat_map(fn
        {:ok, {:orphan, series_id, pid}} -> [{series_id, pid}]
        _ -> []
      end)

    # 2. Daily retention
    daily_cutoff = now - state.daily_retention_seconds

    TimelessMetrics.DB.write(
      state.db,
      "DELETE FROM tier_daily WHERE bucket < ?1",
      [daily_cutoff]
    )

    # 3. Orphan cleanup: remove series with no data
    Enum.each(orphans, fn {series_id, pid} ->
      # Stop the process
      try do
        DynamicSupervisor.terminate_child(
          :"#{state.store}_actor_sup",
          pid
        )
      catch
        _, _ -> :ok
      end

      # Remove from ETS index
      index = :"#{state.store}_actor_index"

      :ets.match_delete(index, {:_, series_id})

      # Remove from SQLite
      TimelessMetrics.DB.write(
        state.db,
        "DELETE FROM series WHERE id = ?1",
        [series_id]
      )

      # Delete the .dat file
      data_dir = :persistent_term.get({TimelessMetrics, state.store, :data_dir})
      path = TimelessMetrics.Actor.BlockStore.series_path(data_dir, series_id)
      File.rm(path)
    end)

    :ok
  end

  defp all_series_pids(registry) do
    Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
  end
end
