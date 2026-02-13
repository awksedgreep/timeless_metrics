defmodule TimelessMetrics.AnnotationTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_annot_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :annot_test, data_dir: @data_dir, buffer_shards: 1, segment_duration: 3_600}
    )

    on_exit(fn ->
      :persistent_term.erase({TimelessMetrics, :annot_test, :schema})
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
    conn =
      Plug.Test.conn(
        :post,
        "/api/v1/annotations",
        Jason.encode!(%{
          timestamp: now,
          title: "HTTP Deploy",
          description: "Deployed via CI",
          tags: ["deploy", "ci"]
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 201
    result = Jason.decode!(conn.resp_body)
    id = result["id"]
    assert is_integer(id)

    # List
    conn =
      Plug.Test.conn(:get, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 1
    assert List.first(result["data"])["title"] == "HTTP Deploy"

    # Filter by tags
    conn =
      Plug.Test.conn(:get, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}&tags=deploy")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 200
    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 1

    # Delete
    conn =
      Plug.Test.conn(:delete, "/api/v1/annotations/#{id}")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 200

    # Verify deleted
    conn =
      Plug.Test.conn(:get, "/api/v1/annotations?from=#{now - 60}&to=#{now + 60}")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    result = Jason.decode!(conn.resp_body)
    assert length(result["data"]) == 0
  end

  test "annotations appear on chart SVG" do
    now = System.os_time(:second)
    base = div(now, 60) * 60

    # Write some data
    for i <- 0..9 do
      TimelessMetrics.write(:annot_test, "cpu", %{"host" => "web-1"}, 50.0 + i,
        timestamp: base + i * 60
      )
    end

    TimelessMetrics.flush(:annot_test)

    # Create an annotation in the middle of the data range
    TimelessMetrics.annotate(:annot_test, base + 300, "Deploy v3")

    # Render chart
    conn =
      Plug.Test.conn(:get, "/chart?metric=cpu&from=#{base}&to=#{base + 600}")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 200
    assert String.contains?(conn.resp_body, "Deploy v3")
    assert String.contains?(conn.resp_body, "#f59e0b")
    assert String.contains?(conn.resp_body, "stroke-dasharray=\"3,3\"")
  end

  test "annotation with default timestamp" do
    conn =
      Plug.Test.conn(
        :post,
        "/api/v1/annotations",
        Jason.encode!(%{
          title: "Now annotation"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> TimelessMetrics.HTTP.call(store: :annot_test)

    assert conn.status == 201

    now = System.os_time(:second)
    {:ok, results} = TimelessMetrics.annotations(:annot_test, now - 5, now + 5)
    assert length(results) == 1
  end
end
