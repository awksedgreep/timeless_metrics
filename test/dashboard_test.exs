defmodule TimelessMetrics.DashboardTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_dash_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics, name: :dash_test, data_dir: @data_dir, engine: :actor}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)

    :ok
  end

  test "dashboard renders HTML with no metrics" do
    conn =
      Plug.Test.conn(:get, "/")
      |> TimelessMetrics.HTTP.call(store: :dash_test)

    assert conn.status == 200
    assert String.contains?(conn.resp_body, "<!DOCTYPE html>")
    assert String.contains?(conn.resp_body, "TimelessMetrics")
    assert String.contains?(conn.resp_body, "No metrics yet")
  end

  test "dashboard renders metric charts" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data for two metrics
    for i <- 0..5 do
      TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )

      TimelessMetrics.write(:dash_test, "mem_usage", %{"host" => "web-1"}, 70.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:dash_test)

    conn =
      Plug.Test.conn(:get, "/")
      |> TimelessMetrics.HTTP.call(store: :dash_test)

    assert conn.status == 200
    body = conn.resp_body
    assert String.contains?(body, "cpu_usage")
    assert String.contains?(body, "mem_usage")
    assert String.contains?(body, "/chart?metric=cpu_usage")
    assert String.contains?(body, "/chart?metric=mem_usage")
    refute String.contains?(body, "No metrics yet")
  end

  test "dashboard shows firing alert badge" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write high data
    for i <- 0..5 do
      TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:dash_test)

    # Create alert rule and trigger it
    {:ok, _id} =
      TimelessMetrics.create_alert(:dash_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    TimelessMetrics.evaluate_alerts(:dash_test)

    conn =
      Plug.Test.conn(:get, "/")
      |> TimelessMetrics.HTTP.call(store: :dash_test)

    assert conn.status == 200
    body = conn.resp_body
    assert String.contains?(body, "1 firing")
    assert String.contains?(body, "High CPU")
    assert String.contains?(body, "badge-fire")
  end

  test "dashboard respects from parameter" do
    conn =
      Plug.Test.conn(:get, "/?from=-6h")
      |> TimelessMetrics.HTTP.call(store: :dash_test)

    assert conn.status == 200
    body = conn.resp_body
    # The 6 hours option should be selected in the dropdown
    assert String.contains?(body, ~s(value="-6h" selected))
  end

  test "dashboard passes label filters to chart URLs" do
    now = System.os_time(:second)

    TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 50.0, timestamp: now)

    TimelessMetrics.flush(:dash_test)

    conn =
      Plug.Test.conn(:get, "/?host=web-1")
      |> TimelessMetrics.HTTP.call(store: :dash_test)

    assert conn.status == 200
    body = conn.resp_body
    assert String.contains?(body, "host=web-1")
  end
end
