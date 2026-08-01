defmodule TimelessMetrics.DataPlaneContractTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @base div(System.os_time(:second), 60) * 60

  test "Rust block engine pins the HTTP data-plane contract" do
    assert_data_plane_contract(:rust, 18_454)
  end

  test "libSQL engine pins the HTTP data-plane contract" do
    assert_data_plane_contract(:libsql, 18_455)
  end

  test "OpenAPI describes completion-aware health and flush controls" do
    spec = TimelessMetrics.OpenAPI.spec()
    health = spec["paths"]["/health"]["get"]
    flush = spec["paths"]["/api/v1/flush"]["post"]
    properties = health["responses"]["200"]["content"]["application/json"]["schema"]["properties"]

    for field <- [
          "completed_points",
          "admitted_batches",
          "completed_batches",
          "queued_batches",
          "in_flight_batches",
          "oldest_queued_ms",
          "import_errors"
        ] do
      assert properties[field]["type"] == "integer"
    end

    assert flush["operationId"] == "flushMetrics"
    assert Map.has_key?(flush["responses"], "200")
    assert Map.has_key?(flush["responses"], "503")
  end

  defp assert_data_plane_contract(engine, port) do
    unique = System.unique_integer([:positive])
    store = :"data_plane_contract_#{engine}_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "timeless_contract_#{engine}_#{unique}")

    start_supervised!(
      {TimelessMetrics,
       name: store,
       engine: engine,
       data_dir: data_dir,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 2}
    )

    start_supervised!({TimelessMetrics.HTTP, store: store, port: port})
    Process.sleep(50)

    on_exit(fn ->
      TestHelper.await_down(:"#{store}_sup")
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {store, nil})
      File.rm_rf!(data_dir)
    end)

    initial_health = get_json(port, "/health")

    assert %{
             "status" => "ok",
             "admitted_batches" => 0,
             "completed_batches" => 0,
             "completed_points" => 0,
             "queued_batches" => 0,
             "in_flight_batches" => 0,
             "oldest_queued_ms" => 0,
             "import_errors" => 0
           } = initial_health

    base_ms = @base * 1_000

    victoria_body =
      [
        json_line("contract_vm", %{"host" => "edge", "env" => "test"}, [1.5, 2.5], [
          base_ms,
          base_ms + 1_000
        ]),
        json_line("contract_vm", %{"host" => "edge", "env" => "test"}, [3.5], [
          base_ms + 2_000
        ]),
        ~s({"metric":)
      ]
      |> Enum.join("\n")

    assert %{status: 204, body: ""} =
             TimelessMetrics.TestHTTP.post(port, "/api/v1/import", victoria_body)

    prometheus_body = """
    contract_prom{host="edge",env="test"} 4.5 #{base_ms}
    contract_prom{host="edge",env="test"} NaN #{base_ms + 1_000}
    contract_prom{host="edge",env="test"} +Inf #{base_ms + 2_000}
    malformed line
    """

    assert %{status: 204, body: ""} =
             TimelessMetrics.TestHTTP.post(port, "/api/v1/import/prometheus", prometheus_body,
               content_type: "text/plain"
             )

    flush = TimelessMetrics.TestHTTP.post(port, "/api/v1/flush", "")
    assert flush.status == 200

    assert %{
             "status" => "ok",
             "admitted_batches" => 2,
             "completed_batches" => 2,
             "completed_points" => 4
           } = :json.decode(flush.body)

    health = get_json(port, "/health")
    assert health["queued_batches"] == 0
    assert health["in_flight_batches"] == 0
    assert health["oldest_queued_ms"] == 0
    assert health["import_errors"] == 4

    latest = get_json(port, "/api/v1/query?metric=contract_vm&host=edge&env=test")

    assert latest == %{
             "labels" => %{"env" => "test", "host" => "edge"},
             "timestamp" => @base + 2,
             "value" => 3.5
           }

    export =
      TimelessMetrics.TestHTTP.get(
        port,
        "/api/v1/export?metric=contract_vm&host=edge&env=test&from=#{@base}&to=#{@base + 2}"
      )

    assert export.status == 200

    assert %{
             "metric" => %{"__name__" => "contract_vm", "env" => "test", "host" => "edge"},
             "timestamps" => [^base_ms, export_second_ms, export_third_ms],
             "values" => [1.5, 2.5, 3.5]
           } = :json.decode(export.body)

    assert export_second_ms == base_ms + 1_000
    assert export_third_ms == base_ms + 2_000

    range =
      get_json(
        port,
        "/api/v1/query_range?metric=contract_vm&host=edge&env=test&from=#{@base}&to=#{@base + 2}&step=1&aggregate=avg"
      )

    assert range["metric"] == "contract_vm"

    assert [%{"labels" => %{"env" => "test", "host" => "edge"}, "data" => data}] =
             range["series"]

    assert data == [[@base, 1.5], [@base + 1, 2.5], [@base + 2, 3.5]]

    assert get_json(port, "/api/v1/labels") == %{
             "status" => "success",
             "data" => ["__name__", "env", "host"]
           }

    assert get_json(port, "/api/v1/label/host/values?metric=contract_vm") == %{
             "status" => "success",
             "data" => ["edge"]
           }

    series = get_json(port, "/api/v1/series?metric=contract_vm")
    assert series["status"] == "success"

    assert [%{"labels" => %{"env" => "test", "host" => "edge"}}] =
             Enum.map(series["data"], &Map.take(&1, ["labels"]))

    assert get_json(
             port,
             "/api/v1/export?metric=missing&from=#{@base}&to=#{@base + 2}"
           ) == :empty
  end

  defp json_line(metric, labels, values, timestamps) do
    :json.encode(%{
      metric: Map.put(labels, "__name__", metric),
      values: values,
      timestamps: timestamps
    })
    |> IO.iodata_to_binary()
  end

  defp get_json(port, path) do
    response = TimelessMetrics.TestHTTP.get(port, path)
    assert response.status == 200

    case response.body do
      "" -> :empty
      body -> :json.decode(body)
    end
  end
end
