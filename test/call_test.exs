defmodule TimelessMetrics.CallTest do
  use ExUnit.Case, async: false

  defmodule SlowServer do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)
    def init(:ok), do: {:ok, nil}

    def handle_call(:slow, _from, state) do
      Process.sleep(100)
      {:reply, :ok, state}
    end
  end

  test "bounded calls return an error tuple and increment the store timeout counter" do
    TimelessMetrics.Stats.init(:bounded_call_test)
    start_supervised!({SlowServer, :bounded_call_test_db})

    assert {:error, :timeout} =
             TimelessMetrics.Call.call(:bounded_call_test_db, :slow, 10)

    assert TimelessMetrics.Stats.snapshot(:bounded_call_test).timeouts == 1
  end
end
