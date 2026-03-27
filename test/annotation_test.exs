defmodule TimelessMetrics.AnnotationTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @data_dir "/tmp/timeless_annot_test_#{System.os_time(:millisecond)}"
  @port 18_404

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :annot_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:annot_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:annot_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:annot_test, nil})
    start_supervised!({TimelessMetrics, name: :annot_test, data_dir: @data_dir, engine: :actor})

    on_exit(fn ->
      TestHelper.await_down(:annot_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:annot_test, nil})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "create and query annotations" do
    now = System.os_time(:second)

    {:ok, id1} =
      TimelessMetrics.annotate(:annot_test, now - 1800, "Deploy v2.3",
        tags: ["deploy", "web"],
        description: "Rolling deploy of web tier"
      )

    {:ok, id2} =
      TimelessMetrics.annotate(:annot_test, now - 600, "Config change", tags: ["config"])

    assert is_integer(id1)
    assert id2 > id1

    {:ok, results} = TimelessMetrics.annotations(:annot_test, now - 3600, now)
    assert length(results) == 2

    first = List.first(results)
    assert first.title == "Deploy v2.3"
    assert first.description == "Rolling deploy of web tier"
    assert first.tags == ["deploy", "web"]
  end

  test "filter annotations by tags" do
    now = System.os_time(:second)

    TimelessMetrics.annotate(:annot_test, now - 100, "Deploy", tags: ["deploy"])
    TimelessMetrics.annotate(:annot_test, now - 50, "Alert fired", tags: ["alert"])

    {:ok, deploy_only} =
      TimelessMetrics.annotations(:annot_test, now - 3600, now, tags: ["deploy"])

    assert length(deploy_only) == 1
    assert List.first(deploy_only).title == "Deploy"

    {:ok, all} = TimelessMetrics.annotations(:annot_test, now - 3600, now)
    assert length(all) == 2
  end

  test "delete annotation" do
    now = System.os_time(:second)

    {:ok, id} = TimelessMetrics.annotate(:annot_test, now, "Temp annotation")
    {:ok, before} = TimelessMetrics.annotations(:annot_test, now - 60, now + 60)
    assert length(before) == 1

    TimelessMetrics.delete_annotation(:annot_test, id)

    {:ok, after_delete} = TimelessMetrics.annotations(:annot_test, now - 60, now + 60)
    assert length(after_delete) == 0
  end

  test "annotation CRUD via HTTP" do
    now = System.os_time(:second)

    # Create
    body =
      :json.encode(%{
        timestamp: now,
        title: "HTTP Deploy",
        description: "Deployed via CI",
        tags: ["deploy", "ci"]
      })
      |> IO.iodata_to_binary()

    resp =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/annotations", body,
        content_type: "application/json"
      )

    assert resp.status == 201
    result = :json.decode(resp.body)
    id = result["id"]
    assert is_integer(id)

    # List
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 1
    assert List.first(result["data"])["title"] == "HTTP Deploy"

    # Filter by tags
    resp =
      TimelessMetrics.TestHTTP.get(
        @port,
        "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}&tags=deploy"
      )

    assert resp.status == 200
    result = :json.decode(resp.body)
    assert length(result["data"]) == 1

    # Delete
    resp = TimelessMetrics.TestHTTP.delete(@port, "/api/v1/annotations/#{id}")

    assert resp.status == 200

    # Verify deleted
    resp =
      TimelessMetrics.TestHTTP.get(@port, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")

    result = :json.decode(resp.body)
    assert length(result["data"]) == 0
  end

  test "annotations appear on chart SVG" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    for i <- 0..9 do
      TimelessMetrics.write(:annot_test, "cpu", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:annot_test)

    TimelessMetrics.annotate(:annot_test, base + 300, "Deploy v3")

    resp = TimelessMetrics.TestHTTP.get(@port, "/chart?metric=cpu&from=#{base}&to=#{base + 600}")

    assert resp.status == 200
    assert String.contains?(resp.body, "Deploy v3")
    assert String.contains?(resp.body, "#f59e0b")
    assert String.contains?(resp.body, "stroke-dasharray=\"3,3\"")
  end

  test "annotation with default timestamp" do
    body =
      :json.encode(%{
        title: "Now annotation"
      })
      |> IO.iodata_to_binary()

    resp =
      TimelessMetrics.TestHTTP.post(@port, "/api/v1/annotations", body,
        content_type: "application/json"
      )

    assert resp.status == 201

    now = System.os_time(:second)
    {:ok, results} = TimelessMetrics.annotations(:annot_test, now - 5, now + 5)
    assert length(results) == 1
  end
end
