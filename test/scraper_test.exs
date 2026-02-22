defmodule TimelessMetrics.ScraperTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_scraper_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics, name: :scraper_test, data_dir: @data_dir, scraping: true}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)

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
      conn =
        Plug.Test.conn(
          :post,
          "/api/v1/scrape_targets",
          Jason.encode!(%{
            job_name: "http_test",
            address: "localhost:9999",
            scrape_interval: 300
          })
        )
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 201
      result = Jason.decode!(conn.resp_body)
      id = result["id"]
      assert is_integer(id)

      # List
      conn =
        Plug.Test.conn(:get, "/api/v1/scrape_targets")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 200
      result = Jason.decode!(conn.resp_body)
      assert length(result["data"]) == 1
      assert List.first(result["data"])["job_name"] == "http_test"

      # Get
      conn =
        Plug.Test.conn(:get, "/api/v1/scrape_targets/#{id}")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 200
      result = Jason.decode!(conn.resp_body)
      assert result["job_name"] == "http_test"

      # Update
      conn =
        Plug.Test.conn(
          :put,
          "/api/v1/scrape_targets/#{id}",
          Jason.encode!(%{
            job_name: "http_test_updated",
            address: "localhost:9998",
            scrape_interval: 60
          })
        )
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 200

      # Verify update
      conn =
        Plug.Test.conn(:get, "/api/v1/scrape_targets/#{id}")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      result = Jason.decode!(conn.resp_body)
      assert result["job_name"] == "http_test_updated"
      assert result["address"] == "localhost:9998"

      # Delete
      conn =
        Plug.Test.conn(:delete, "/api/v1/scrape_targets/#{id}")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 200

      # Verify deleted
      conn =
        Plug.Test.conn(:get, "/api/v1/scrape_targets")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      result = Jason.decode!(conn.resp_body)
      assert result["data"] == []
    end

    test "get non-existent target returns 404" do
      conn =
        Plug.Test.conn(:get, "/api/v1/scrape_targets/99999")
        |> TimelessMetrics.HTTP.call(store: :scraper_test)

      assert conn.status == 404
    end
  end

  describe "target lifecycle" do
    test "scraping: false does not start scraper" do
      # The default TimelessMetrics does not start scraper processes
      data_dir = "/tmp/timeless_no_scraper_#{System.os_time(:millisecond)}"

      start_supervised!(
        {TimelessMetrics, name: :no_scraper_test, data_dir: data_dir},
        id: :no_scraper
      )

      on_exit(fn -> File.rm_rf!(data_dir) end)

      # Scraper process should not exist
      assert Process.whereis(:no_scraper_test_scraper) == nil
    end

    test "scraping: true starts scraper processes" do
      # Our setup starts with scraping: true
      assert Process.whereis(:scraper_test_scraper) != nil
      assert Process.whereis(:scraper_test_scrape_sup) != nil
    end
  end
end
