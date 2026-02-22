defmodule TimelessMetrics.Scraper.RelabelTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.Scraper.Relabel

  describe "apply_configs/2" do
    test "nil configs passes through" do
      labels = %{"job" => "test", "instance" => "localhost:9090"}
      assert {:ok, ^labels} = Relabel.apply_configs(labels, nil)
    end

    test "empty configs passes through" do
      labels = %{"job" => "test"}
      assert {:ok, ^labels} = Relabel.apply_configs(labels, [])
    end

    test "replace action with capture groups" do
      configs = [
        %{
          "action" => "replace",
          "source_labels" => ["__address__"],
          "regex" => "(.+):.*",
          "target_label" => "host",
          "replacement" => "$1",
          "__compiled_regex__" => ~r/(.+):.*/
        }
      ]

      labels = %{"__address__" => "myhost:9090", "job" => "test"}
      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result["host"] == "myhost"
    end

    test "replace action — no match leaves labels unchanged" do
      configs = [
        %{
          "action" => "replace",
          "source_labels" => ["__address__"],
          "regex" => "nomatch",
          "target_label" => "host",
          "replacement" => "$1",
          "__compiled_regex__" => ~r/nomatch/
        }
      ]

      labels = %{"__address__" => "myhost:9090"}
      assert {:ok, ^labels} = Relabel.apply_configs(labels, configs)
    end

    test "keep action — matching labels kept" do
      configs = [
        %{
          "action" => "keep",
          "source_labels" => ["job"],
          "regex" => "important.*",
          "__compiled_regex__" => ~r/important.*/
        }
      ]

      labels = %{"job" => "important_service", "instance" => "host:9090"}
      assert {:ok, ^labels} = Relabel.apply_configs(labels, configs)
    end

    test "keep action — non-matching drops" do
      configs = [
        %{
          "action" => "keep",
          "source_labels" => ["job"],
          "regex" => "important.*",
          "__compiled_regex__" => ~r/important.*/
        }
      ]

      labels = %{"job" => "boring_service", "instance" => "host:9090"}
      assert :drop = Relabel.apply_configs(labels, configs)
    end

    test "drop action — matching drops" do
      configs = [
        %{
          "action" => "drop",
          "source_labels" => ["__name__"],
          "regex" => "go_.*",
          "__compiled_regex__" => ~r/go_.*/
        }
      ]

      labels = %{"__name__" => "go_goroutines", "instance" => "host:9090"}
      assert :drop = Relabel.apply_configs(labels, configs)
    end

    test "drop action — non-matching passes through" do
      configs = [
        %{
          "action" => "drop",
          "source_labels" => ["__name__"],
          "regex" => "go_.*",
          "__compiled_regex__" => ~r/go_.*/
        }
      ]

      labels = %{"__name__" => "http_requests_total", "instance" => "host:9090"}
      assert {:ok, ^labels} = Relabel.apply_configs(labels, configs)
    end

    test "labeldrop removes matching label keys" do
      configs = [
        %{
          "action" => "labeldrop",
          "regex" => "__.*__",
          "__compiled_regex__" => ~r/__.*__/
        }
      ]

      labels = %{"__address__" => "host:9090", "__scheme__" => "http", "job" => "test"}
      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result == %{"job" => "test"}
    end

    test "labelkeep keeps only matching label keys" do
      configs = [
        %{
          "action" => "labelkeep",
          "regex" => "job|instance",
          "__compiled_regex__" => ~r/job|instance/
        }
      ]

      labels = %{"job" => "test", "instance" => "host:9090", "extra" => "val", "more" => "stuff"}
      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result == %{"job" => "test", "instance" => "host:9090"}
    end

    test "labelmap renames matching label keys" do
      configs = [
        %{
          "action" => "labelmap",
          "regex" => "__meta_k8s_(.*)",
          "replacement" => "k8s_$1",
          "__compiled_regex__" => ~r/__meta_k8s_(.*)/
        }
      ]

      labels = %{
        "__meta_k8s_namespace" => "default",
        "__meta_k8s_pod" => "my-pod",
        "job" => "test"
      }

      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result["k8s_namespace"] == "default"
      assert result["k8s_pod"] == "my-pod"
      assert result["job"] == "test"
      refute Map.has_key?(result, "__meta_k8s_namespace")
    end

    test "chained configs apply sequentially" do
      configs = [
        %{
          "action" => "replace",
          "source_labels" => ["__address__"],
          "regex" => "(.+):.*",
          "target_label" => "host",
          "replacement" => "$1",
          "__compiled_regex__" => ~r/(.+):.*/
        },
        %{
          "action" => "labeldrop",
          "regex" => "__.*",
          "__compiled_regex__" => ~r/__.*/
        }
      ]

      labels = %{"__address__" => "myhost:9090", "job" => "test"}
      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result == %{"host" => "myhost", "job" => "test"}
    end

    test "drop in chain stops processing" do
      configs = [
        %{
          "action" => "drop",
          "source_labels" => ["job"],
          "regex" => "skip_me",
          "__compiled_regex__" => ~r/skip_me/
        },
        %{
          "action" => "replace",
          "source_labels" => ["job"],
          "target_label" => "should_not_run",
          "replacement" => "value",
          "__compiled_regex__" => ~r/.*/
        }
      ]

      labels = %{"job" => "skip_me"}
      assert :drop = Relabel.apply_configs(labels, configs)
    end

    test "separator joins multiple source labels" do
      configs = [
        %{
          "action" => "replace",
          "source_labels" => ["job", "instance"],
          "separator" => "/",
          "regex" => "(.*)",
          "target_label" => "combined",
          "replacement" => "$1",
          "__compiled_regex__" => ~r/(.*)/
        }
      ]

      labels = %{"job" => "api", "instance" => "host:9090"}
      assert {:ok, result} = Relabel.apply_configs(labels, configs)
      assert result["combined"] == "api/host:9090"
    end
  end

  describe "target_meta_labels/1" do
    test "builds meta labels from target map" do
      target = %{
        address: "localhost:9100",
        metrics_path: "/metrics",
        scheme: "http",
        scrape_interval: 30,
        scrape_timeout: 10,
        job_name: "node"
      }

      labels = Relabel.target_meta_labels(target)
      assert labels["__address__"] == "localhost:9100"
      assert labels["__metrics_path__"] == "/metrics"
      assert labels["__scheme__"] == "http"
      assert labels["__scrape_interval__"] == "30s"
      assert labels["__scrape_timeout__"] == "10s"
      assert labels["job"] == "node"
      assert labels["instance"] == "localhost:9100"
    end
  end

  describe "drop_meta_labels/1" do
    test "strips double-underscore prefixed keys" do
      labels = %{
        "__address__" => "host:9090",
        "__scheme__" => "http",
        "job" => "test",
        "instance" => "host:9090"
      }

      result = Relabel.drop_meta_labels(labels)
      assert result == %{"job" => "test", "instance" => "host:9090"}
    end
  end

  describe "apply_honor_labels/3" do
    test "honor_labels=true — scraped labels win" do
      scraped = %{"job" => "scraped_job", "extra" => "val"}
      target = %{"job" => "target_job", "instance" => "host:9090"}

      result = Relabel.apply_honor_labels(scraped, target, true)
      assert result["job"] == "scraped_job"
      assert result["instance"] == "host:9090"
      assert result["extra"] == "val"
    end

    test "honor_labels=false — target labels win, conflicting scraped get exported_ prefix" do
      scraped = %{"job" => "scraped_job", "extra" => "val"}
      target = %{"job" => "target_job", "instance" => "host:9090"}

      result = Relabel.apply_honor_labels(scraped, target, false)
      assert result["job"] == "target_job"
      assert result["exported_job"] == "scraped_job"
      assert result["instance"] == "host:9090"
      assert result["extra"] == "val"
    end
  end
end
