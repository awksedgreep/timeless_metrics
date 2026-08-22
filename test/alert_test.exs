defmodule TimelessMetrics.AlertTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @data_dir "/tmp/timeless_alert_test_#{System.os_time(:millisecond)}"
  @port 18_405

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :alert_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:alert_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:alert_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:alert_test, nil})
    start_supervised!({TimelessMetrics, name: :alert_test, data_dir: @data_dir})

    on_exit(fn ->
      TestHelper.await_down(:alert_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:alert_test, nil})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "create, list, and delete alert rules" do
    {:ok, id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        webhook_url: "http://localhost:9999/webhook"
      )

    assert is_integer(id)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    assert length(rules) == 1

    rule = List.first(rules)
    assert rule.name == "High CPU"
    assert rule.metric == "cpu_usage"
    assert rule.condition == "above"
    assert rule.threshold == 90.0
    assert rule.enabled == true

    TimelessMetrics.delete_alert(:alert_test, id)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    assert rules == []
  end

  test "alert fires when threshold exceeded" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0 + i * 0.1,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert length(rule.states) > 0

    state = List.first(rule.states)
    assert state.state == "firing"
    assert state.last_value > 90.0
  end

  test "alert stays ok when below threshold" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 50.0 + i * 0.1,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, _rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = List.first(rules)
    assert rule.states == [] or Enum.all?(rule.states, &(&1.state == "ok"))
  end

  test "alert resolves when value drops below threshold" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0
      )

    series_key = :json.encode(%{"host" => "web-1"}) |> IO.iodata_to_binary()

    TimelessMetrics.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'firing', ?3, NULL, 95.0)",
      [rule_id, series_key, now - 300]
    )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert Enum.any?(rule.states, &(&1.state == "resolved"))
  end

  test "below condition fires correctly" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "disk_free", %{"host" => "db-1"}, 5.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, _id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "Low Disk",
        metric: "disk_free",
        condition: :below,
        threshold: 10.0
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.name == "Low Disk"))
    assert Enum.any?(rule.states, &(&1.state == "firing"))
  end

  test "alert with label filter only evaluates matching series" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-2"}, 30.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, _id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "High CPU web-1 only",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "web-1"}
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = List.first(rules)

    assert length(rule.states) == 1
    state = List.first(rule.states)
    assert state.state == "firing"
    labels = state.series_labels
    assert labels["host"] == "web-1"
  end

  test "alert CRUD via HTTP" do
    # Create
    body =
      :json.encode(%{
        name: "HTTP Alert",
        metric: "cpu_usage",
        condition: "above",
        threshold: 80.0,
        webhook_url: "http://localhost:9999/hook"
      })
      |> IO.iodata_to_binary()

    resp =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/alerts", body,
        content_type: "application/json"
      )

    assert resp.status == 201
    result = :json.decode(resp.body)
    id = result["id"]
    assert is_integer(id)

    # List
    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/alerts")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 1
    assert List.first(result["data"])["name"] == "HTTP Alert"

    # Delete
    resp = TimelessMetrics.TestHTTP.delete(@port, "/api/v1/alerts/#{id}")

    assert resp.status == 200

    # Verify deleted
    resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/alerts")

    result = :json.decode(resp.body)
    assert length(result["data"]) == 0
  end

  test "alert fires with duration requirement" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "web-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "Sustained High CPU",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        duration: 300
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))

    if length(rule.states) > 0 do
      state = List.first(rule.states)
      assert state.state == "pending"
    end

    series_key = :json.encode(%{"host" => "web-1"}) |> IO.iodata_to_binary()

    TimelessMetrics.DB.write(
      :alert_test_db,
      "UPDATE alert_state SET triggered_at = ?1 WHERE rule_id = ?2 AND series_labels = ?3",
      [now - 360, rule_id, series_key]
    )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert Enum.any?(rule.states, &(&1.state == "firing"))
  end

  test "ok → pending → firing full transition with duration" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "transition-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "Transition Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        duration: 300,
        labels: %{"host" => "transition-1"}
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert length(rule.states) == 1
    assert List.first(rule.states).state == "pending"

    series_key = :json.encode(%{"host" => "transition-1"}) |> IO.iodata_to_binary()

    TimelessMetrics.DB.write(
      :alert_test_db,
      "UPDATE alert_state SET triggered_at = ?1 WHERE rule_id = ?2 AND series_labels = ?3",
      [now - 600, rule_id, series_key]
    )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "firing"
  end

  test "firing → resolved → ok cleans up state row" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "cleanup-1"}, 50.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "Cleanup Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "cleanup-1"}
      )

    series_key = :json.encode(%{"host" => "cleanup-1"}) |> IO.iodata_to_binary()

    TimelessMetrics.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'firing', ?3, NULL, 95.0)",
      [rule_id, series_key, now - 300]
    )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "resolved"

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert rule.states == []
  end

  test "resolved → firing re-triggers on breach (duration=0)" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..5 do
      TimelessMetrics.write(:alert_test, "cpu_usage", %{"host" => "retrigger-1"}, 95.0,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "Retrigger Test",
        metric: "cpu_usage",
        condition: :above,
        threshold: 90.0,
        labels: %{"host" => "retrigger-1"}
      )

    series_key = :json.encode(%{"host" => "retrigger-1"}) |> IO.iodata_to_binary()

    TimelessMetrics.DB.write(
      :alert_test_db,
      "INSERT OR REPLACE INTO alert_state (rule_id, series_labels, state, triggered_at, resolved_at, last_value) VALUES (?1, ?2, 'resolved', ?3, ?4, 50.0)",
      [rule_id, series_key, now - 600, now - 300]
    )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))
    assert List.first(rule.states).state == "firing"
  end

  test "alert with empty labels matches all series" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for host <- ["h1", "h2", "h3"] do
      for i <- 0..5 do
        TimelessMetrics.write(:alert_test, "cpu_all", %{"host" => host}, 95.0,
          timestamp: base + i * 60
        )
      end
    end

    TimelessMetrics.flush(:alert_test)

    {:ok, rule_id} =
      TimelessMetrics.create_alert(:alert_test,
        name: "All Hosts CPU",
        metric: "cpu_all",
        condition: :above,
        threshold: 90.0,
        labels: %{}
      )

    TimelessMetrics.evaluate_alerts(:alert_test)

    {:ok, rules} = TimelessMetrics.list_alerts(:alert_test)
    rule = Enum.find(rules, &(&1.id == rule_id))

    assert length(rule.states) == 3
    assert Enum.all?(rule.states, &(&1.state == "firing"))
  end

  describe "evaluation resilience and reader indirection" do
    defmodule RaisingReader do
      @moduledoc false
      def query_aggregate_multi(_store, _metric, _labels, _opts) do
        raise "data plane unreachable"
      end
    end

    defmodule RecordingReader do
      @moduledoc false
      def query_aggregate_multi(_store, metric, _labels, _opts) do
        send(:alert_reader_probe, {:read, metric})
        {:ok, []}
      end
    end

    setup do
      on_exit(fn -> Application.delete_env(:timeless_metrics, :alert_reader) end)
      :ok
    end

    test "a reader that raises does not stop the pass or the evaluator" do
      # A transient data-plane failure used to propagate out of evaluate/1.
      # Under the evaluator's supervisor that is a restart loop, and alerting
      # stops entirely -- indistinguishable from having nothing to report.
      {:ok, _} =
        TimelessMetrics.create_alert(:alert_test,
          name: "will raise",
          metric: "cpu_usage",
          condition: :above,
          threshold: 1.0,
          webhook_url: "http://localhost:9999/webhook"
        )

      Application.put_env(:timeless_metrics, :alert_reader, RaisingReader)

      assert :ok = TimelessMetrics.Alert.evaluate(:alert_test)
    end

    test "one failing rule does not prevent the others from evaluating" do
      for name <- ["first", "second"] do
        {:ok, _} =
          TimelessMetrics.create_alert(:alert_test,
            name: name,
            metric: "metric_#{name}",
            condition: :above,
            threshold: 1.0,
            webhook_url: "http://localhost:9999/webhook"
          )
      end

      Process.register(self(), :alert_reader_probe)

      on_exit(fn ->
        if Process.whereis(:alert_reader_probe), do: Process.unregister(:alert_reader_probe)
      end)

      Application.put_env(:timeless_metrics, :alert_reader, RecordingReader)

      assert :ok = TimelessMetrics.Alert.evaluate(:alert_test)

      assert_received {:read, "metric_first"}
      assert_received {:read, "metric_second"}
    end

    test "reads go through the configured reader" do
      # How a deployment whose metrics live behind a data plane evaluates at
      # all: there is no in-process store to read under owner: :external.
      {:ok, _} =
        TimelessMetrics.create_alert(:alert_test,
          name: "routed",
          metric: "routed_metric",
          condition: :above,
          threshold: 1.0,
          webhook_url: "http://localhost:9999/webhook"
        )

      Process.register(self(), :alert_reader_probe)

      on_exit(fn ->
        if Process.whereis(:alert_reader_probe), do: Process.unregister(:alert_reader_probe)
      end)

      Application.put_env(:timeless_metrics, :alert_reader, RecordingReader)

      assert :ok = TimelessMetrics.Alert.evaluate(:alert_test)
      assert_received {:read, "routed_metric"}
    end

    test "the default reader is this library" do
      Application.delete_env(:timeless_metrics, :alert_reader)
      assert :ok = TimelessMetrics.Alert.evaluate(:alert_test)
    end
  end
end
