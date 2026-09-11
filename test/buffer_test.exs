defmodule TimelessMetrics.BufferTest do
  use ExUnit.Case, async: false

  test "configured backpressure threshold is honored" do
    store = :buffer_backpressure_test
    builder = :buffer_backpressure_builder
    shard = :buffer_backpressure_shard
    TimelessMetrics.Stats.init(store)

    builder_pid = spawn(fn -> Process.sleep(:infinity) end)
    Process.register(builder_pid, builder)
    send(builder_pid, :queued_work)

    on_exit(fn ->
      if Process.alive?(builder_pid), do: Process.exit(builder_pid, :kill)
    end)

    start_supervised!(
      {TimelessMetrics.Buffer,
       name: shard, shard_id: 0, store: store, segment_builder: builder, backpressure_threshold: 0}
    )

    assert {:error, :backpressure} = TimelessMetrics.Buffer.write(shard, 1, 10, 1.0)
  end
end
