defmodule TimelessMetrics.AlertEvaluator do
  @moduledoc """
  Periodic alert evaluation timer.

  Ticks every `:interval` milliseconds (default 60s) and evaluates all
  alert rules against current metric data.
  """

  use GenServer

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    interval = Keyword.get(opts, :interval, :timer.seconds(60))

    # First evaluation after a short delay to let data accumulate
    Process.send_after(self(), :tick, :timer.seconds(5))
    {:ok, %{store: store, interval: interval}}
  end

  @impl true
  def handle_info(:tick, state) do
    TimelessMetrics.Alert.evaluate(state.store)
    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end
end
