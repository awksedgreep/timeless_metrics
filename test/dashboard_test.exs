defmodule TimelessMetrics.DashboardTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_dash_test_#{System.os_time(:millisecond)}"
  @port 18_401

  setup do
    start_supervised!({TimelessMetrics, name: :dash_test, data_dir: @data_dir, engine: :actor})
    start_supervised!({TimelessMetrics.HTTP, store: :dash_test, port: @port})
    Process.sleep(50)

    on_exit(fn -> File.rm_rf!(@data_dir) end)

    :ok
  end

  test "dashboard renders HTML with no metrics" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/")

    assert resp.status == 200
    assert String.contains?(resp.body, "<!DOCTYPE html>")
    assert String.contains?(resp.body, "TimelessMetrics")
    assert String.contains?(resp.body, "No metrics yet")
  end

  test "dashboard renders metric charts" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )

      TimelessMetrics.write(:dash_test, "mem_usage", %{"host" => "web-1"}, 70.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:dash_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/")

    assert resp.status == 200
    body = resp.body
    assert String.contains?(body, "cpu_usage")
    assert String.contains?(body, "mem_usage")
    assert String.contains?(body, "/chart?metric=cpu_usage")
    assert String.contains?(body, "/chart?metric=mem_usage")
    refute String.contains?(body, "No metrics yet")
  end

  test "dashboard shows firing alert badge" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:dash_test)

    {:ok, _id} =
      TimelessMetrics.create_alert(:dash_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    TimelessMetrics.evaluate_alerts(:dash_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/")

    assert resp.status == 200
    body = resp.body
    assert String.contains?(body, "1 firing")
    assert String.contains?(body, "High CPU")
    assert String.contains?(body, "badge-fire")
  end

  test "dashboard respects from parameter" do
    resp = TimelessMetrics.TestHTTP.get(@port, "/?from=-6h")

    assert resp.status == 200
    body = resp.body
    assert String.contains?(body, ~s(value="-6h" selected))
  end

  test "dashboard passes label filters to chart URLs" do
    now = System.os_time(:second)

    TimelessMetrics.write(:dash_test, "cpu_usage", %{"host" => "web-1"}, 50.0, timestamp: now)

    TimelessMetrics.flush(:dash_test)

    resp = TimelessMetrics.TestHTTP.get(@port, "/?host=web-1")

    assert resp.status == 200
    body = resp.body
    assert String.contains?(body, "host=web-1")
  end
end
