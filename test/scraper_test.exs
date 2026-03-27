defmodule TimelessMetrics.ScraperTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  @data_dir "/tmp/timeless_scraper_test_#{System.os_time(:millisecond)}"
  @port 18_408

  setup_all do
    start_supervised!({TimelessMetrics.HTTP, store: :scraper_test, port: @port})
    Process.sleep(50)

    on_exit(fn ->
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:scraper_test, nil})
    end)

    :ok
  end

  setup do
    TestHelper.await_down(:scraper_test_sup)
    :persistent_term.put({TimelessMetrics.HTTP, :config}, {:scraper_test, nil})
    start_supervised!({TimelessMetrics, name: :scraper_test, data_dir: @data_dir, scraping: true})

    on_exit(fn ->
      TestHelper.await_down(:scraper_test_sup)
      :persistent_term.put({TimelessMetrics.HTTP, :config}, {:scraper_test, nil})
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "target CRUD" do
    test "add, list, get, delete target" do
      scraper = :scraper_test_scraper

      # Add
      {:ok, id} =
        TimelessMetrics.Scraper.add_target(scraper, %{
          "job_name" => "test_job",
          "address" => "localhost:9999",
          "scrape_interval" => 300
        })

      assert is_integer(id)

      # List
      {:ok, targets} = TimelessMetrics.Scraper.list_targets(scraper)
      assert length(targets) == 1
      target = List.first(targets)
      assert target.job_name == "test_job"
      assert target.address == "localhost:9999"
      assert target.scrape_interval == 300
      assert target.scheme == "http"
      assert target.metrics_path == "/metrics"
      assert target.health.health == "unknown"

      # Get
      {:ok, target} = TimelessMetrics.Scraper.get_target(scraper, id)
      assert target.job_name == "test_job"

      # Delete
      :ok = TimelessMetrics.Scraper.delete_target(scraper, id)

      {:ok, targets} = TimelessMetrics.Scraper.list_targets(scraper)
      assert targets == []
    end

    test "add target with all fields" do
      scraper = :scraper_test_scraper

      {:ok, id} =
        TimelessMetrics.Scraper.add_target(scraper, %{
          "job_name" => "full_job",
          "scheme" => "https",
          "address" => "myhost:9100",
          "metrics_path" => "/custom/metrics",
          "scrape_interval" => 60,
          "scrape_timeout" => 15,
          "labels" => %{"env" => "prod", "region" => "us-east"},
          "honor_labels" => true,
          "honor_timestamps" => false,
          "metric_relabel_configs" => [
            %{"action" => "drop", "source_labels" => ["__name__"], "regex" => "go_.*"}
          ]
        })

      {:ok, target} = TimelessMetrics.Scraper.get_target(scraper, id)
      assert target.scheme == "https"
      assert target.metrics_path == "/custom/metrics"
      assert target.scrape_interval == 60
      assert target.scrape_timeout == 15
      assert target.labels == %{"env" => "prod", "region" => "us-east"}
      assert target.honor_labels == true
      assert target.honor_timestamps == false
      assert is_list(target.metric_relabel_configs)

      :ok = TimelessMetrics.Scraper.delete_target(scraper, id)
    end

    test "update target" do
      scraper = :scraper_test_scraper

      {:ok, id} =
        TimelessMetrics.Scraper.add_target(scraper, %{
          "job_name" => "update_job",
          "address" => "localhost:9100"
        })

      :ok =
        TimelessMetrics.Scraper.update_target(scraper, id, %{
          "job_name" => "updated_job",
          "address" => "localhost:9200",
          "scrape_interval" => 60
        })

      {:ok, target} = TimelessMetrics.Scraper.get_target(scraper, id)
      assert target.job_name == "updated_job"
      assert target.address == "localhost:9200"
      assert target.scrape_interval == 60

      :ok = TimelessMetrics.Scraper.delete_target(scraper, id)
    end

    test "validation rejects missing job_name" do
      scraper = :scraper_test_scraper

      assert {:error, "missing required field: job_name"} =
               TimelessMetrics.Scraper.add_target(scraper, %{
                 "address" => "localhost:9100"
               })
    end

    test "validation rejects missing address" do
      scraper = :scraper_test_scraper

      assert {:error, "missing required field: address"} =
               TimelessMetrics.Scraper.add_target(scraper, %{
                 "job_name" => "test"
               })
    end

    test "get non-existent target returns error" do
      scraper = :scraper_test_scraper
      assert {:error, :not_found} = TimelessMetrics.Scraper.get_target(scraper, 99999)
    end
  end

  describe "HTTP CRUD" do
    test "create, list, get, update, delete via HTTP" do
      # Create
      resp =
        TimelessMetrics.TestHTTP.post(
          @port,
          "/api/v1/scrape_targets",
          :json.encode(%{
            job_name: "http_test",
            address: "localhost:9999",
            scrape_interval: 300
          })
          |> IO.iodata_to_binary(),
          content_type: "application/json"
        )

      assert resp.status == 201
      result = :json.decode(resp.body)
      id = result["id"]
      assert is_integer(id)

      # List
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/scrape_targets")

      assert resp.status == 200
      result = :json.decode(resp.body)
      assert length(result["data"]) == 1
      assert List.first(result["data"])["job_name"] == "http_test"

      # Get
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/scrape_targets/#{id}")

      assert resp.status == 200
      result = :json.decode(resp.body)
      assert result["job_name"] == "http_test"

      # Update
      resp =
        TimelessMetrics.TestHTTP.put(
          @port,
          "/api/v1/scrape_targets/#{id}",
          :json.encode(%{
            job_name: "http_test_updated",
            address: "localhost:9998",
            scrape_interval: 60
          })
          |> IO.iodata_to_binary(),
          content_type: "application/json"
        )

      assert resp.status == 200

      # Verify update
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/scrape_targets/#{id}")

      result = :json.decode(resp.body)
      assert result["job_name"] == "http_test_updated"
      assert result["address"] == "localhost:9998"

      # Delete
      resp = TimelessMetrics.TestHTTP.delete(@port, "/api/v1/scrape_targets/#{id}")

      assert resp.status == 200

      # Verify deleted
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/scrape_targets")

      result = :json.decode(resp.body)
      assert result["data"] == []
    end

    test "get non-existent target returns 404" do
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/v1/scrape_targets/99999")

      assert resp.status == 404
    end
  end

  describe "OpenAPI" do
    test "GET /api/openapi.json returns valid spec" do
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/openapi.json")

      assert resp.status == 200
      assert {"content-type", "application/json"} in resp.headers

      spec = :json.decode(resp.body)
      assert spec["openapi"] == "3.1.0"
      assert spec["info"]["title"] == "TimelessMetrics API"
      assert Map.has_key?(spec["paths"], "/api/v1/scrape_targets")
      assert Map.has_key?(spec["paths"], "/api/v1/scrape_targets/{id}")
      assert Map.has_key?(spec["components"]["schemas"], "ScrapeTargetCreate")
      assert Map.has_key?(spec["components"]["schemas"], "RelabelConfig")
    end

    test "GET /api/docs returns Scalar HTML" do
      resp = TimelessMetrics.TestHTTP.get(@port, "/api/docs")

      assert resp.status == 200
      assert {"content-type", "text/html"} in resp.headers
      assert resp.body =~ "scalar"
      assert resp.body =~ "/api/openapi.json"
    end
  end

  describe "target lifecycle" do
    test "scraping enabled by default" do
      data_dir = "/tmp/timeless_default_scraper_#{System.os_time(:millisecond)}"

      start_supervised!(
        {TimelessMetrics, name: :default_scraper_test, data_dir: data_dir},
        id: :default_scraper
      )

      on_exit(fn -> File.rm_rf!(data_dir) end)

      assert Process.whereis(:default_scraper_test_scraper) != nil
      assert Process.whereis(:default_scraper_test_scrape_sup) != nil
    end

    test "scraping: false disables scraper" do
      data_dir = "/tmp/timeless_no_scraper_#{System.os_time(:millisecond)}"

      start_supervised!(
        {TimelessMetrics, name: :no_scraper_test, data_dir: data_dir, scraping: false},
        id: :no_scraper
      )

      on_exit(fn -> File.rm_rf!(data_dir) end)

      assert Process.whereis(:no_scraper_test_scraper) == nil
    end
  end
end
