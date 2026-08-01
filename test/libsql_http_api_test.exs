defmodule TimelessMetrics.LibsqlHTTPAPITest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @store :libsql_http_test
  @port 18_452

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: @store, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {@store, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:libsql_http_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {@store, nil})

    data_dir =
      Path.join(System.tmp_dir!(), "timeless_libsql_http_#{System.unique_integer([:positive])}")

    start_supervised!(
      {TimelessMetrics,
       name: @store,
       engine: :libsql,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 2}
    )

    on_exit(fn ->
      TestHelper.await_down(:libsql_http_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {@store, nil})
      File.rm_rf!(data_dir)
    end)

    :ok
  end

  test "JSON-line and Prometheus HTTP imports reach the libSQL writer" do
    now_s = 1_700_000_000
    now_ms = now_s * 1_000

    json =
      :json.encode(%{
        metric: %{__name__: "http_json", host: "web-1"},
        values: [7.5],
        timestamps: [now_ms]
      })
      |> IO.iodata_to_binary()

    assert %{status: 204} = TimelessMetrics.TestHTTP.post(@port, "/api/v1/import", json)

    prometheus = "http_prom{host=\"web-2\"} 8.5 #{now_ms}\nmalformed line\n"

    assert %{status: 204} =
             TimelessMetrics.TestHTTP.post(@port, "/api/v1/import/prometheus", prometheus,
               content_type: "text/plain"
             )

    flush = TimelessMetrics.TestHTTP.post(@port, "/api/v1/flush", "")
    assert flush.status == 200

    assert %{
             "status" => "ok",
             "admitted_batches" => 2,
             "completed_batches" => 2,
             "completed_points" => 2
           } = :json.decode(flush.body)

    health = TimelessMetrics.TestHTTP.get(@port, "/health")
    health_body = :json.decode(health.body)
    assert health_body["queued_batches"] == 0
    assert health_body["in_flight_batches"] == 0
    assert health_body["oldest_queued_ms"] == 0
    assert health_body["import_errors"] == 1

    assert {:ok, [{^now_s, 7.5}]} =
             TimelessMetrics.query(@store, "http_json", %{"host" => "web-1"},
               from: now_s - 1,
               to: now_s + 1
             )

    assert {:ok, [{^now_s, 8.5}]} =
             TimelessMetrics.query(@store, "http_prom", %{"host" => "web-2"},
               from: now_s - 1,
               to: now_s + 1
             )
  end

  test "health and Prometheus query endpoints use libSQL stats and reads" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    assert :ok =
             TimelessMetrics.write(@store, "api_latency", %{"service" => "edge"}, 12.5,
               timestamp: base
             )

    health = TimelessMetrics.TestHTTP.get(@port, "/health")
    assert health.status == 200
    health_body = :json.decode(health.body)
    assert health_body["status"] == "ok"
    assert health_body["series"] == 1
    assert health_body["points"] == 1
    assert is_integer(health_body["completed_points"])
    assert is_integer(health_body["admitted_batches"])
    assert is_integer(health_body["completed_batches"])
    assert is_integer(health_body["queued_batches"])
    assert is_integer(health_body["in_flight_batches"])
    assert is_integer(health_body["oldest_queued_ms"])
    assert is_integer(health_body["import_errors"])

    query =
      URI.encode_query(%{
        "query" => ~s(api_latency{service="edge"}),
        "start" => base,
        "end" => base + 60,
        "step" => 60
      })

    response = TimelessMetrics.TestHTTP.get(@port, "/prometheus/api/v1/query_range?#{query}")

    assert response.status == 200
    body = :json.decode(response.body)
    assert body["status"] == "success"
    assert body["data"]["resultType"] == "matrix"

    assert [%{"metric" => %{"__name__" => "api_latency", "service" => "edge"}}] =
             body["data"]["result"]
  end

  test "selector-scoped discovery uses matcher-aware storage candidates" do
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"cpu", %{"host" => "web-1", "env" => "prod"}, 1.0, 10},
               {"cpu", %{"host" => "web-2", "env" => "dev"}, 2.0, 10},
               {"cpu", %{"host" => "db-1"}, 3.0, 10}
             ])

    assert :ok = TimelessMetrics.flush(@store)
    selector = ~s(cpu{host=~"web-.*",env!="dev"})
    query = URI.encode_query(%{"match[]" => selector})

    series = TimelessMetrics.TestHTTP.get(@port, "/api/v1/series?#{query}")
    assert series.status == 200

    assert %{
             "status" => "success",
             "data" => [
               %{"__name__" => "cpu", "env" => "prod", "host" => "web-1"}
             ]
           } = :json.decode(series.body)

    values = TimelessMetrics.TestHTTP.get(@port, "/api/v1/label/host/values?#{query}")
    assert values.status == 200
    assert %{"status" => "success", "data" => ["web-1"]} = :json.decode(values.body)
  end
end
