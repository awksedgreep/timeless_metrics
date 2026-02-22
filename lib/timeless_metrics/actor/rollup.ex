defmodule TimelessMetrics.Actor.Rollup do
  @moduledoc """
  Periodic daily rollup for the actor engine.

  Reads raw data from SeriesServer processes, computes daily aggregates
  (avg, min, max, count, sum, last), and writes to the `tier_daily` table.
  Tracks progress via the `_watermarks` table.
  """

  use GenServer

  defstruct [:store, :db, :manager, :registry, :interval, :timer_ref]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force a rollup run."
  def run(server) do
    GenServer.call(server, :run, :infinity)
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    db = Keyword.fetch!(opts, :db)
    manager = Keyword.fetch!(opts, :manager)
    registry = Keyword.fetch!(opts, :registry)
    interval = Keyword.get(opts, :interval, :timer.minutes(5))

    state = %__MODULE__{
      store: store,
      db: db,
      manager: manager,
      registry: registry,
      interval: interval
    }

    timer_ref = Process.send_after(self(), :tick, interval)
    {:ok, %{state | timer_ref: timer_ref}}
  end

  @impl true
  def handle_call(:run, _from, state) do
    do_rollup(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    do_rollup(state)
    timer_ref = Process.send_after(self(), :tick, state.interval)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  defp do_rollup(state) do
    watermark = read_watermark(state.db)
    now = System.os_time(:second)
    today_start = div(now, 86_400) * 86_400
    # Only rollup completed days
    days = completed_days(watermark, today_start)

    if days == [] do
      :ok
    else
      pids = all_series_pids(state.registry)

      Enum.each(days, fn day_start ->
        day_end = day_start + 86_400 - 1

        results =
          pids
          |> Task.async_stream(
            fn {series_id, pid} ->
              try do
                case GenServer.call(pid, {:compute_daily, day_start, day_end}, :infinity) do
                  nil -> nil
                  aggs -> {series_id, aggs}
                end
              catch
                :exit, _ -> nil
              end
            end,
            max_concurrency: System.schedulers_online(),
            ordered: false,
            timeout: :infinity
          )
          |> Enum.flat_map(fn
            {:ok, nil} -> []
            {:ok, result} -> [result]
          end)

        # Batch insert into tier_daily
        if results != [] do
          TimelessMetrics.DB.write_transaction(state.db, fn conn ->
            Enum.each(results, fn {series_id, aggs} ->
              TimelessMetrics.DB.execute(
                conn,
                "INSERT OR REPLACE INTO tier_daily (series_id, bucket, avg, min, max, count, sum, last) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                [series_id, day_start, aggs.avg, aggs.min, aggs.max, aggs.count, aggs.sum, aggs.last]
              )
            end)
          end)
        end

        # Update watermark
        write_watermark(state.db, day_start)
      end)
    end
  end

  defp read_watermark(db) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        db,
        "SELECT last_bucket FROM _watermarks WHERE tier = 'daily'"
      )

    case rows do
      [[ts]] -> ts
      [] -> 0
    end
  end

  defp write_watermark(db, ts) do
    TimelessMetrics.DB.write(
      db,
      "INSERT OR REPLACE INTO _watermarks (tier, last_bucket) VALUES ('daily', ?1)",
      [ts]
    )
  end

  defp completed_days(watermark, today_start) do
    first_day = watermark + 86_400

    if first_day >= today_start do
      []
    else
      Stream.iterate(first_day, &(&1 + 86_400))
      |> Enum.take_while(&(&1 < today_start))
    end
  end

  defp all_series_pids(registry) do
    Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
  end
end
