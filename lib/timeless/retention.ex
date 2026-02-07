defmodule Timeless.Retention do
  @moduledoc """
  Tier-aware retention enforcer.

  Periodically deletes expired raw segments and rollup rows based on
  configured retention periods. Runs incremental vacuum to reclaim space.
  """

  use GenServer

  require Logger

  defstruct [:db, :schema, :vacuum_counter]

  @vacuum_every 24  # Run vacuum every N retention cycles

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Force retention enforcement now."
  def enforce(retention) do
    GenServer.call(retention, :enforce, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    schema = Keyword.fetch!(opts, :schema)

    state = %__MODULE__{db: db, schema: schema, vacuum_counter: 0}

    schedule_tick(schema.retention_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    new_state = do_enforce(state)
    schedule_tick(state.schema.retention_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_call(:enforce, _from, state) do
    new_state = do_enforce(state)
    {:reply, :ok, new_state}
  end

  # --- Core Logic ---

  defp do_enforce(state) do
    now = System.os_time(:second)

    # 1. Drop expired raw segments
    if state.schema.raw_retention_seconds != :forever do
      raw_cutoff = now - state.schema.raw_retention_seconds

      {:ok, _} =
        Timeless.DB.write(
          state.db,
          "DELETE FROM raw_segments WHERE end_time < ?1",
          [raw_cutoff]
        )
    end

    # 2. Drop expired rollup rows per tier
    Enum.each(state.schema.tiers, fn tier ->
      if tier.retention_seconds != :forever do
        cutoff = now - tier.retention_seconds

        {:ok, _} =
          Timeless.DB.write(
            state.db,
            "DELETE FROM #{tier.table_name} WHERE bucket < ?1",
            [cutoff]
          )
      end
    end)

    # 3. Clean orphaned series (no data in any table)
    cleanup_orphaned_series(state)

    # 4. Periodic vacuum
    counter = state.vacuum_counter + 1

    if rem(counter, @vacuum_every) == 0 do
      Timeless.DB.write(state.db, "PRAGMA incremental_vacuum(1000)", [])
    end

    %{state | vacuum_counter: counter}
  end

  defp cleanup_orphaned_series(state) do
    tier_unions =
      state.schema.tiers
      |> Enum.map(fn tier -> "SELECT DISTINCT series_id FROM #{tier.table_name}" end)
      |> Enum.join(" UNION ")

    sql = """
    DELETE FROM series WHERE id NOT IN (
      SELECT DISTINCT series_id FROM raw_segments
      UNION #{tier_unions}
    )
    """

    Timeless.DB.write(state.db, sql, [])
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
