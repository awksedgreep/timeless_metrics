defmodule Timeless.AlertTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_alert_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {Timeless,
       name: :alert_test,
       data_dir: @data_dir,
       buffer_shards: 1,
       segment_duration: 3_600}
    )

    on_exit(fn ->
      :persistent_term.erase({Timeless, :alert_test, :schema})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "create, list, and delete alert rules" do
    {:ok, id} =
      Timeless.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        webhook_url: "http://localhost:9999/webhook"
      )

    assert is_integer(id)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    assert length(rules) == 1

    rule = List.first(rules)
    assert rule.name == "High CPU"
    assert rule.metric == "cpu_usage"
    assert rule.condition == "above"
    assert rule.threshold == 90.0
    assert rule.enabled == true

    Timeless.delete_alert(:alert_test, id)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    assert rules == []
  end

  test "alert fires when threshold exceeded" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data that exceeds threshold
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0 + i * 0.1,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    # Create alert rule — threshold 90, no duration requirement
    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    # Evaluate alerts
    Timeless.evaluate_alerts(:alert_test)

    # Check state — should be firing (no duration requirement)
    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert length(rule.states) > 0

    state = List.first(rule.states)
    assert state.state == "firing"
    assert state.last_value > 90.0
  end

  test "alert stays ok when below threshold" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data below threshold
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i * 0.1,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, _rule_id} =
      Timeless.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = List.first(rules)
    # No state rows because value never breached threshold
    assert rule.states == [] or Enum.all?(rule.states, &(&1.state == "ok"))
  end

  test "alert resolves when value drops below threshold" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write low data (below threshold)
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    # Manually set state to "firing" to simulate a previously-fired alert
    series_key = Jason.encode!(%{"host" => "web-1"})

    Timeless.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'firing', ?3, NULL, 95.0)",
      [rule_id, series_key, now - 300]
    )

    # Evaluate — value is now below threshold, should resolve
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert Enum.any?(rule.states, &(&1.state == "resolved"))
  end

  test "below condition fires correctly" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write low data
    for i <- 0..5 do
      Timeless.write(:alert_test, "disk_free", %{"host" => "db-1"}, 5.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, _id} =
      Timeless.create_alert(:alert_test,
        name: "Low Disk",
        metric: "disk_free",
        condition: :below,
        threshold: 10.0
      )

    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.name == "Low Disk"))
    assert Enum.any?(rule.states, &(&1.state == "firing"))
  end

  test "alert with label filter only evaluates matching series" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write high data for web-1
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    # Write low data for web-2
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-2"}, 30.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, _id} =
      Timeless.create_alert(:alert_test,
        name: "High CPU web-1 only",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "web-1"}
      )

    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = List.first(rules)

    # Only web-1 should have a state entry, and it should be firing
    assert length(rule.states) == 1
    state = List.first(rule.states)
    assert state.state == "firing"
    labels = state.series_labels
    assert labels["host"] == "web-1"
  end

  test "alert CRUD via HTTP" do
    # Create
    conn =
      Plug.Test.conn(
        :post,
        "/api/v1/alerts",
        Jason.encode!(%{
          name: "HTTP Alert",
          metric: "cpu_usage",
          condition: "above",
          threshold: 80.0,
          webhook_url: "http://localhost:9999/hook"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Timeless.HTTP.call(store: :alert_test)

    assert conn.status == 201
    result = Jason.decode!(conn.resp_body)
    id = result["id"]
    assert is_integer(id)

    # List
    conn =
      Plug.Test.conn(:get, "/api/v1/alerts")
      |> Timeless.HTTP.call(store: :alert_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 1
    assert List.first(result["data"])["name"] == "HTTP Alert"

    # Delete
    conn =
      Plug.Test.conn(:delete, "/api/v1/alerts/#{id}")
      |> Timeless.HTTP.call(store: :alert_test)

    assert conn.status == 200

    # Verify deleted
    conn =
      Plug.Test.conn(:get, "/api/v1/alerts")
      |> Timeless.HTTP.call(store: :alert_test)

    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 0
  end

  test "alert fires with duration requirement" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write high data
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    # Duration of 300s — must breach for 5 minutes before firing
    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "Sustained High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        duration: 300
      )

    # First evaluation: should go to pending (not firing)
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))

    if length(rule.states) > 0 do
      state = List.first(rule.states)
      assert state.state == "pending"
    end

    # Manually set triggered_at to 6 minutes ago to simulate passage of time
    series_key = Jason.encode!(%{"host" => "web-1"})

    Timeless.DB.write(
      :alert_test_db,
      "UPDATE alert_state SET triggered_at = ?1 WHERE rule_id = ?2 AND series_labels = ?3",
      [now - 360, rule_id, series_key]
    )

    # Re-evaluate — duration should now be exceeded
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert Enum.any?(rule.states, &(&1.state == "firing"))
  end

  test "ok → pending → firing full transition with duration" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data exceeding threshold
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "transition-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    # Create alert with 300s duration requirement
    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "Transition Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        duration: 300,
        labels: %{"host" => "transition-1"}
      )

    # 1st eval: ok → pending
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert length(rule.states) == 1
    assert List.first(rule.states).state == "pending"

    # Simulate time passing by backdating triggered_at
    series_key = Jason.encode!(%{"host" => "transition-1"})

    Timeless.DB.write(
      :alert_test_db,
      "UPDATE alert_state SET triggered_at = ?1 WHERE rule_id = ?2 AND series_labels = ?3",
      [now - 600, rule_id, series_key]
    )

    # 2nd eval: pending → firing (duration exceeded)
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "firing"
  end

  test "firing → resolved → ok cleans up state row" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data BELOW threshold so it resolves
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "cleanup-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "Cleanup Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "cleanup-1"}
      )

    # Manually set state to "firing"
    series_key = Jason.encode!(%{"host" => "cleanup-1"})

    Timeless.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'firing', ?3, NULL, 95.0)",
      [rule_id, series_key, now - 300]
    )

    # Eval: firing → resolved (value below threshold)
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "resolved"

    # Eval again: resolved → ok (still below threshold, state row should be deleted)
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert rule.states == []
  end

  test "resolved → firing re-triggers on breach (duration=0)" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write data ABOVE threshold
    for i <- 0..5 do
      Timeless.write(:alert_test, "cpu_usage", %{"host" => "retrigger-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    Timeless.flush(:alert_test)

    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "Retrigger Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "retrigger-1"}
      )

    # Manually set state to "resolved" (simulating previous resolution)
    series_key = Jason.encode!(%{"host" => "retrigger-1"})

    Timeless.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'resolved', ?3, ?4, 50.0)",
      [rule_id, series_key, now - 600, now - 300]
    )

    # Eval: resolved → firing (value is above threshold again, duration=0)
    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "firing"
  end

  test "alert with empty labels matches all series" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write high data for multiple hosts
    for host <- ["h1", "h2", "h3"] do
      for i <- 0..5 do
        Timeless.write(:alert_test, "cpu_all", %{"host" => host}, 95.0,
          timestamp: base + i * 60
        )
      end
    end

    Timeless.flush(:alert_test)

    # Alert with empty labels — should match all series
    {:ok, rule_id} =
      Timeless.create_alert(:alert_test,
        name: "All Hosts CPU",
        metric: "cpu_all",
        condition: :above,
        threshold: 90.0,
        labels: %{}
      )

    Timeless.evaluate_alerts(:alert_test)

    {:ok, rules} = Timeless.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))

    # All 3 series should have fired
    assert length(rule.states) == 3
    assert Enum.all?(rule.states, &(&1.state == "firing"))
  end
end
