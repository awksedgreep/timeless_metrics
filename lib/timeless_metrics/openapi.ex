defmodule TimelessMetrics.OpenAPI do
  @moduledoc false

  def spec do
    %{
      "openapi" => "3.1.0",
      "info" => %{
        "title" => "TimelessMetrics API",
        "description" => "Embedded time series storage for Elixir. Compatible with Prometheus, VictoriaMetrics, and InfluxDB ingest formats.",
        "version" => "1.0.2"
      },
      "servers" => [
        %{"url" => "/", "description" => "Current instance"}
      ],
      "tags" => tags(),
      "paths" => paths(),
      "components" => components()
    }
  end

  def spec_json do
    Jason.encode!(spec(), pretty: true)
  end

  defp tags do
    [
      %{"name" => "Scrape Targets", "description" => "Manage Prometheus scrape targets"},
      %{"name" => "Ingest", "description" => "Push metrics data"},
      %{"name" => "Query", "description" => "Query time series data"},
      %{"name" => "Metadata", "description" => "Metric metadata and discovery"},
      %{"name" => "Alerts", "description" => "Alert rule management"},
      %{"name" => "Annotations", "description" => "Event markers on time series"},
      %{"name" => "Analytics", "description" => "Forecasting and anomaly detection"},
      %{"name" => "Operational", "description" => "Health, backup, charts"}
    ]
  end

  defp paths do
    %{}
    |> Map.merge(scrape_target_paths())
    |> Map.merge(ingest_paths())
    |> Map.merge(query_paths())
    |> Map.merge(metadata_paths())
    |> Map.merge(alert_paths())
    |> Map.merge(annotation_paths())
    |> Map.merge(analytics_paths())
    |> Map.merge(operational_paths())
  end

  # --- Scrape Targets ---

  defp scrape_target_paths do
    %{
      "/api/v1/scrape_targets" => %{
        "post" => %{
          "tags" => ["Scrape Targets"],
          "summary" => "Create scrape target",
          "description" => "Add a new Prometheus scrape target. A worker process will begin scraping immediately.",
          "operationId" => "createScrapeTarget",
          "requestBody" => %{
            "required" => true,
            "content" => %{
              "application/json" => %{
                "schema" => %{"$ref" => "#/components/schemas/ScrapeTargetCreate"},
                "examples" => %{
                  "minimal" => %{
                    "summary" => "Minimal — just job name and address",
                    "value" => %{
                      "job_name" => "node_exporter",
                      "address" => "localhost:9100"
                    }
                  },
                  "full" => %{
                    "summary" => "Full — all options",
                    "value" => %{
                      "job_name" => "app_metrics",
                      "scheme" => "https",
                      "address" => "myapp:9090",
                      "metrics_path" => "/custom/metrics",
                      "scrape_interval" => 60,
                      "scrape_timeout" => 15,
                      "labels" => %{"env" => "production", "region" => "us-east-1"},
                      "honor_labels" => false,
                      "honor_timestamps" => true,
                      "metric_relabel_configs" => [
                        %{
                          "action" => "drop",
                          "source_labels" => ["__name__"],
                          "regex" => "go_.*"
                        }
                      ]
                    }
                  }
                }
              }
            }
          },
          "responses" => %{
            "201" => %{
              "description" => "Target created",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "id" => %{"type" => "integer"},
                      "status" => %{"type" => "string", "example" => "created"}
                    }
                  }
                }
              }
            },
            "400" => json_error_response("Validation error")
          }
        },
        "get" => %{
          "tags" => ["Scrape Targets"],
          "summary" => "List all scrape targets",
          "description" => "Returns all scrape targets with their current health status.",
          "operationId" => "listScrapeTargets",
          "responses" => %{
            "200" => %{
              "description" => "List of targets with health",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "data" => %{
                        "type" => "array",
                        "items" => %{"$ref" => "#/components/schemas/ScrapeTargetWithHealth"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/api/v1/scrape_targets/{id}" => %{
        "get" => %{
          "tags" => ["Scrape Targets"],
          "summary" => "Get scrape target",
          "description" => "Get a single scrape target with health status.",
          "operationId" => "getScrapeTarget",
          "parameters" => [target_id_param()],
          "responses" => %{
            "200" => %{
              "description" => "Target with health",
              "content" => %{
                "application/json" => %{
                  "schema" => %{"$ref" => "#/components/schemas/ScrapeTargetWithHealth"}
                }
              }
            },
            "404" => json_error_response("Target not found")
          }
        },
        "put" => %{
          "tags" => ["Scrape Targets"],
          "summary" => "Update scrape target",
          "description" => "Update a scrape target. The worker is restarted with the new configuration.",
          "operationId" => "updateScrapeTarget",
          "parameters" => [target_id_param()],
          "requestBody" => %{
            "required" => true,
            "content" => %{
              "application/json" => %{
                "schema" => %{"$ref" => "#/components/schemas/ScrapeTargetCreate"}
              }
            }
          },
          "responses" => %{
            "200" => status_response("updated"),
            "400" => json_error_response("Validation error")
          }
        },
        "delete" => %{
          "tags" => ["Scrape Targets"],
          "summary" => "Delete scrape target",
          "description" => "Stop the scrape worker and remove the target. Health data is cascade-deleted.",
          "operationId" => "deleteScrapeTarget",
          "parameters" => [target_id_param()],
          "responses" => %{
            "200" => status_response("deleted")
          }
        }
      }
    }
  end

  # --- Ingest ---

  defp ingest_paths do
    %{
      "/api/v1/import" => %{
        "post" => %{
          "tags" => ["Ingest"],
          "summary" => "Import (VictoriaMetrics JSON lines)",
          "description" => "Import metrics in VictoriaMetrics JSON line format. Each line: `{\"metric\":{\"__name__\":\"cpu\",\"host\":\"web-1\"},\"values\":[73.2],\"timestamps\":[1700000000]}`",
          "operationId" => "importJsonLines",
          "requestBody" => text_body("VictoriaMetrics JSON lines",
            "{\"metric\":{\"__name__\":\"cpu_usage\",\"host\":\"web-1\"},\"values\":[73.2,74.1],\"timestamps\":[1700000000,1700000060]}"),
          "responses" => ingest_responses()
        }
      },
      "/api/v1/import/prometheus" => %{
        "post" => %{
          "tags" => ["Ingest"],
          "summary" => "Import (Prometheus text format)",
          "description" => "Import metrics in Prometheus text exposition format. Uses NIF parser for high throughput.",
          "operationId" => "importPrometheus",
          "requestBody" => text_body("Prometheus text format",
            "http_requests_total{method=\"GET\",code=\"200\"} 1234 1700000000000\nnode_cpu_seconds_total{cpu=\"0\",mode=\"idle\"} 98765.43"),
          "responses" => ingest_responses()
        }
      },
      "/write" => %{
        "post" => %{
          "tags" => ["Ingest"],
          "summary" => "Import (InfluxDB line protocol)",
          "description" => "Import metrics in InfluxDB line protocol format. Compatible with TSBS and VictoriaMetrics /write.",
          "operationId" => "importInflux",
          "requestBody" => text_body("InfluxDB line protocol",
            "cpu,host=web-1,region=us usage_user=73.2,usage_system=12.1 1700000000000000000"),
          "responses" => ingest_responses()
        }
      }
    }
  end

  # --- Query ---

  defp query_paths do
    time_params = [
      query_param("start", "string", "Start time (unix seconds or relative: -1h, -30m)", false),
      query_param("end", "string", "End time (unix seconds or 'now')", false)
    ]

    %{
      "/api/v1/query" => %{
        "get" => %{
          "tags" => ["Query"],
          "summary" => "Instant query",
          "description" => "Get the latest value for matching series.",
          "operationId" => "instantQuery",
          "parameters" => [
            query_param("metric", "string", "Metric name", true)
          ] ++ time_params,
          "responses" => %{
            "200" => %{
              "description" => "Latest values",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            },
            "400" => json_error_response("Missing metric parameter")
          }
        }
      },
      "/api/v1/query_range" => %{
        "get" => %{
          "tags" => ["Query"],
          "summary" => "Range query",
          "description" => "Bucketed aggregation over a time range. Supports both native params and PromQL via `query` param.",
          "operationId" => "rangeQuery",
          "parameters" => [
            query_param("metric", "string", "Metric name (native mode)", false),
            query_param("query", "string", "PromQL expression (alternative to metric param)", false),
            query_param("step", "string", "Bucket size in seconds (default: 60)", false),
            query_param("aggregate", "string", "Aggregate function: avg, min, max, sum, count, last, first (default: avg)", false),
            query_param("group_by", "string", "Comma-separated label keys to group by", false),
            query_param("transform", "string", "Transform: rate, delta, cumulative", false),
            query_param("limit", "string", "Top N series by last value", false)
          ] ++ time_params,
          "responses" => %{
            "200" => %{
              "description" => "Aggregated time series data",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            },
            "400" => json_error_response("Invalid query parameters")
          }
        }
      },
      "/api/v1/export" => %{
        "get" => %{
          "tags" => ["Query"],
          "summary" => "Export raw points",
          "description" => "Export raw points in VictoriaMetrics JSON line format.",
          "operationId" => "exportData",
          "parameters" => [
            query_param("metric", "string", "Metric name", true)
          ] ++ time_params,
          "responses" => %{
            "200" => %{
              "description" => "JSON lines of raw data",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      },
      "/prometheus/api/v1/query_range" => %{
        "get" => %{
          "tags" => ["Query"],
          "summary" => "Prometheus-compatible range query",
          "description" => "Prometheus API compatible endpoint for Grafana and TSBS.",
          "operationId" => "promQueryRange",
          "parameters" => [
            query_param("query", "string", "PromQL expression", true),
            query_param("start", "string", "Start time (unix timestamp)", false),
            query_param("end", "string", "End time (unix timestamp)", false),
            query_param("step", "string", "Step duration (seconds or duration string)", false)
          ],
          "responses" => %{
            "200" => %{
              "description" => "Prometheus response format",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      }
    }
  end

  # --- Metadata ---

  defp metadata_paths do
    %{
      "/api/v1/metadata" => %{
        "post" => %{
          "tags" => ["Metadata"],
          "summary" => "Register metric metadata",
          "description" => "Set the type, unit, and description for a metric.",
          "operationId" => "registerMetadata",
          "requestBody" => %{
            "required" => true,
            "content" => %{
              "application/json" => %{
                "schema" => %{
                  "type" => "object",
                  "required" => ["metric", "type"],
                  "properties" => %{
                    "metric" => %{"type" => "string", "description" => "Metric name"},
                    "type" => %{"type" => "string", "enum" => ["gauge", "counter", "histogram"]},
                    "unit" => %{"type" => "string", "description" => "Unit (e.g. bytes, seconds, %)"},
                    "description" => %{"type" => "string"}
                  }
                }
              }
            }
          },
          "responses" => %{
            "200" => status_response("ok"),
            "400" => json_error_response("Invalid type or missing fields")
          }
        },
        "get" => %{
          "tags" => ["Metadata"],
          "summary" => "Get metric metadata",
          "operationId" => "getMetadata",
          "parameters" => [query_param("metric", "string", "Metric name", true)],
          "responses" => %{
            "200" => %{
              "description" => "Metric metadata",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "metric" => %{"type" => "string"},
                      "type" => %{"type" => "string"},
                      "unit" => %{"type" => "string", "nullable" => true},
                      "description" => %{"type" => "string", "nullable" => true}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/api/v1/label/__name__/values" => %{
        "get" => %{
          "tags" => ["Metadata"],
          "summary" => "List all metric names",
          "operationId" => "listMetricNames",
          "responses" => %{
            "200" => %{
              "description" => "Metric names",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "status" => %{"type" => "string"},
                      "data" => %{"type" => "array", "items" => %{"type" => "string"}}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/api/v1/label/{name}/values" => %{
        "get" => %{
          "tags" => ["Metadata"],
          "summary" => "List label values",
          "description" => "List distinct values for a label key across all series of a metric.",
          "operationId" => "listLabelValues",
          "parameters" => [
            %{"name" => "name", "in" => "path", "required" => true,
              "schema" => %{"type" => "string"}, "description" => "Label key"},
            query_param("metric", "string", "Metric name", true)
          ],
          "responses" => %{
            "200" => %{
              "description" => "Label values",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "status" => %{"type" => "string"},
                      "data" => %{"type" => "array", "items" => %{"type" => "string"}}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/api/v1/series" => %{
        "get" => %{
          "tags" => ["Metadata"],
          "summary" => "List series for a metric",
          "operationId" => "listSeries",
          "parameters" => [query_param("metric", "string", "Metric name", true)],
          "responses" => %{
            "200" => %{
              "description" => "Series list",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "status" => %{"type" => "string"},
                      "data" => %{"type" => "array", "items" => %{"type" => "object"}}
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  end

  # --- Alerts ---

  defp alert_paths do
    %{
      "/api/v1/alerts" => %{
        "post" => %{
          "tags" => ["Alerts"],
          "summary" => "Create alert rule",
          "operationId" => "createAlert",
          "requestBody" => %{
            "required" => true,
            "content" => %{
              "application/json" => %{
                "schema" => %{
                  "type" => "object",
                  "required" => ["name", "metric", "condition", "threshold"],
                  "properties" => %{
                    "name" => %{"type" => "string", "description" => "Alert rule name"},
                    "metric" => %{"type" => "string", "description" => "Metric to monitor"},
                    "condition" => %{"type" => "string", "enum" => ["above", "below"]},
                    "threshold" => %{"type" => "number", "description" => "Threshold value"},
                    "labels" => %{"type" => "object", "additionalProperties" => %{"type" => "string"}, "description" => "Label filter (empty = all series)"},
                    "duration" => %{"type" => "integer", "description" => "Seconds threshold must be breached before firing (0 = instant)", "default" => 0},
                    "aggregate" => %{"type" => "string", "enum" => ["avg", "min", "max", "sum", "count"], "default" => "avg"},
                    "webhook_url" => %{"type" => "string", "format" => "uri", "description" => "URL to POST when alert fires"}
                  }
                },
                "example" => %{
                  "name" => "High CPU",
                  "metric" => "cpu_usage",
                  "condition" => "above",
                  "threshold" => 90.0,
                  "duration" => 300,
                  "webhook_url" => "https://hooks.slack.com/..."
                }
              }
            }
          },
          "responses" => %{
            "201" => created_response(),
            "400" => json_error_response("Missing required fields")
          }
        },
        "get" => %{
          "tags" => ["Alerts"],
          "summary" => "List alert rules",
          "description" => "List all alert rules with their current state.",
          "operationId" => "listAlerts",
          "responses" => %{
            "200" => %{
              "description" => "Alert rules with state",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      },
      "/api/v1/alerts/{id}" => %{
        "delete" => %{
          "tags" => ["Alerts"],
          "summary" => "Delete alert rule",
          "operationId" => "deleteAlert",
          "parameters" => [id_path_param("Alert rule ID")],
          "responses" => %{"200" => status_response("deleted")}
        }
      }
    }
  end

  # --- Annotations ---

  defp annotation_paths do
    %{
      "/api/v1/annotations" => %{
        "post" => %{
          "tags" => ["Annotations"],
          "summary" => "Create annotation",
          "description" => "Add an event marker on the time series timeline.",
          "operationId" => "createAnnotation",
          "requestBody" => %{
            "required" => true,
            "content" => %{
              "application/json" => %{
                "schema" => %{
                  "type" => "object",
                  "required" => ["title"],
                  "properties" => %{
                    "title" => %{"type" => "string"},
                    "description" => %{"type" => "string"},
                    "timestamp" => %{"type" => "integer", "description" => "Unix seconds (default: now)"},
                    "tags" => %{"type" => "array", "items" => %{"type" => "string"}}
                  }
                },
                "example" => %{
                  "title" => "Deployed v2.1.0",
                  "description" => "Rolling deploy to production",
                  "tags" => ["deploy", "production"]
                }
              }
            }
          },
          "responses" => %{
            "201" => created_response(),
            "400" => json_error_response("Missing title")
          }
        },
        "get" => %{
          "tags" => ["Annotations"],
          "summary" => "Query annotations",
          "description" => "Query annotations within a time range, optionally filtered by tags.",
          "operationId" => "listAnnotations",
          "parameters" => [
            query_param("from", "string", "Start time (default: 24h ago)", false),
            query_param("to", "string", "End time (default: now)", false),
            query_param("tags", "string", "Comma-separated tag filter", false)
          ],
          "responses" => %{
            "200" => %{
              "description" => "Annotations",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      },
      "/api/v1/annotations/{id}" => %{
        "delete" => %{
          "tags" => ["Annotations"],
          "summary" => "Delete annotation",
          "operationId" => "deleteAnnotation",
          "parameters" => [id_path_param("Annotation ID")],
          "responses" => %{"200" => status_response("deleted")}
        }
      }
    }
  end

  # --- Analytics ---

  defp analytics_paths do
    metric_time_params = [
      query_param("metric", "string", "Metric name", true),
      query_param("start", "string", "Start time", false),
      query_param("end", "string", "End time", false),
      query_param("step", "string", "Bucket size in seconds (default: 300)", false),
      query_param("transform", "string", "Transform: rate, delta, cumulative", false)
    ]

    %{
      "/api/v1/forecast" => %{
        "get" => %{
          "tags" => ["Analytics"],
          "summary" => "Forecast future values",
          "operationId" => "forecast",
          "parameters" => metric_time_params ++ [
            query_param("horizon", "string", "Forecast horizon (duration: 1h, 6h, 1d)", false)
          ],
          "responses" => %{
            "200" => %{
              "description" => "Historical data with forecast",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      },
      "/api/v1/anomalies" => %{
        "get" => %{
          "tags" => ["Analytics"],
          "summary" => "Detect anomalies",
          "operationId" => "detectAnomalies",
          "parameters" => metric_time_params ++ [
            query_param("sensitivity", "string", "Sensitivity: low, medium, high (default: medium)", false)
          ],
          "responses" => %{
            "200" => %{
              "description" => "Anomaly analysis",
              "content" => %{"application/json" => %{"schema" => %{"type" => "object"}}}
            }
          }
        }
      }
    }
  end

  # --- Operational ---

  defp operational_paths do
    %{
      "/health" => %{
        "get" => %{
          "tags" => ["Operational"],
          "summary" => "Health check",
          "description" => "Health check with store statistics. Auth-exempt for load balancers.",
          "operationId" => "healthCheck",
          "responses" => %{
            "200" => %{
              "description" => "Store health",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "status" => %{"type" => "string", "example" => "ok"},
                      "series" => %{"type" => "integer"},
                      "points" => %{"type" => "integer"},
                      "storage_bytes" => %{"type" => "integer"},
                      "buffer_points" => %{"type" => "integer"},
                      "bytes_per_point" => %{"type" => "number"}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/api/v1/backup" => %{
        "post" => %{
          "tags" => ["Operational"],
          "summary" => "Create backup",
          "description" => "Create a consistent online backup of all databases.",
          "operationId" => "createBackup",
          "requestBody" => %{
            "required" => false,
            "content" => %{
              "application/json" => %{
                "schema" => %{
                  "type" => "object",
                  "properties" => %{
                    "path" => %{"type" => "string", "description" => "Target directory (default: auto-generated)"}
                  }
                }
              }
            }
          },
          "responses" => %{
            "200" => %{
              "description" => "Backup result",
              "content" => %{
                "application/json" => %{
                  "schema" => %{
                    "type" => "object",
                    "properties" => %{
                      "status" => %{"type" => "string"},
                      "path" => %{"type" => "string"},
                      "files" => %{"type" => "array", "items" => %{"type" => "string"}},
                      "total_bytes" => %{"type" => "integer"}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "/chart" => %{
        "get" => %{
          "tags" => ["Operational"],
          "summary" => "SVG chart",
          "description" => "Embeddable SVG line chart. Use via `<img src=\"/chart?metric=cpu_usage\">`.",
          "operationId" => "chart",
          "parameters" => [
            query_param("metric", "string", "Metric name", true),
            query_param("from", "string", "Start time (default: -1h)", false),
            query_param("to", "string", "End time (default: now)", false),
            query_param("step", "string", "Bucket size in seconds", false),
            query_param("aggregate", "string", "Aggregate function (default: avg)", false),
            query_param("width", "string", "Image width in pixels (default: 800)", false),
            query_param("height", "string", "Image height in pixels (default: 300)", false),
            query_param("theme", "string", "Color theme: light, dark, auto (default: auto)", false),
            query_param("forecast", "string", "Forecast horizon (e.g. 1h)", false),
            query_param("anomalies", "string", "Anomaly sensitivity (low/medium/high)", false)
          ],
          "responses" => %{
            "200" => %{
              "description" => "SVG chart image",
              "content" => %{"image/svg+xml" => %{"schema" => %{"type" => "string"}}}
            }
          }
        }
      }
    }
  end

  # --- Components ---

  defp components do
    %{
      "schemas" => %{
        "ScrapeTargetCreate" => %{
          "type" => "object",
          "required" => ["job_name", "address"],
          "properties" => %{
            "job_name" => %{
              "type" => "string",
              "description" => "Job name for this scrape target (used in 'job' label)"
            },
            "address" => %{
              "type" => "string",
              "description" => "Host:port to scrape (e.g. localhost:9100)"
            },
            "scheme" => %{
              "type" => "string",
              "enum" => ["http", "https"],
              "default" => "http",
              "description" => "HTTP scheme"
            },
            "metrics_path" => %{
              "type" => "string",
              "default" => "/metrics",
              "description" => "Path to metrics endpoint"
            },
            "scrape_interval" => %{
              "type" => "integer",
              "default" => 30,
              "minimum" => 1,
              "description" => "Scrape interval in seconds"
            },
            "scrape_timeout" => %{
              "type" => "integer",
              "default" => 10,
              "minimum" => 1,
              "description" => "Scrape timeout in seconds"
            },
            "labels" => %{
              "type" => "object",
              "additionalProperties" => %{"type" => "string"},
              "default" => %{},
              "description" => "Extra labels to attach to all scraped metrics"
            },
            "honor_labels" => %{
              "type" => "boolean",
              "default" => false,
              "description" => "When true, scraped labels take precedence over target labels on conflict. When false, conflicting scraped labels are prefixed with 'exported_'."
            },
            "honor_timestamps" => %{
              "type" => "boolean",
              "default" => true,
              "description" => "When true, use timestamps from scraped data. When false, use scrape time."
            },
            "relabel_configs" => %{
              "type" => "array",
              "nullable" => true,
              "description" => "Relabeling rules applied to target labels before scraping",
              "items" => %{"$ref" => "#/components/schemas/RelabelConfig"}
            },
            "metric_relabel_configs" => %{
              "type" => "array",
              "nullable" => true,
              "description" => "Relabeling rules applied to each scraped metric",
              "items" => %{"$ref" => "#/components/schemas/RelabelConfig"}
            },
            "enabled" => %{
              "type" => "boolean",
              "default" => true,
              "description" => "Whether the target is actively being scraped"
            }
          }
        },
        "RelabelConfig" => %{
          "type" => "object",
          "description" => "Prometheus-compatible relabeling configuration",
          "properties" => %{
            "action" => %{
              "type" => "string",
              "enum" => ["replace", "keep", "drop", "labeldrop", "labelkeep", "labelmap"],
              "default" => "replace",
              "description" => "Relabeling action to perform"
            },
            "source_labels" => %{
              "type" => "array",
              "items" => %{"type" => "string"},
              "description" => "Labels to concatenate (joined by separator) as source value"
            },
            "separator" => %{
              "type" => "string",
              "default" => ";",
              "description" => "Separator for joining source labels"
            },
            "target_label" => %{
              "type" => "string",
              "description" => "Label to write the result to (for replace action)"
            },
            "regex" => %{
              "type" => "string",
              "default" => ".*",
              "description" => "Regex to match against source value"
            },
            "replacement" => %{
              "type" => "string",
              "default" => "$1",
              "description" => "Replacement string (supports $1, $2 capture groups)"
            }
          }
        },
        "ScrapeHealth" => %{
          "type" => "object",
          "properties" => %{
            "health" => %{
              "type" => "string",
              "enum" => ["up", "down", "unknown"],
              "description" => "Current health status"
            },
            "last_scrape" => %{
              "type" => "integer",
              "nullable" => true,
              "description" => "Unix timestamp of last scrape attempt"
            },
            "last_duration_ms" => %{
              "type" => "integer",
              "nullable" => true,
              "description" => "Duration of last scrape in milliseconds"
            },
            "last_error" => %{
              "type" => "string",
              "nullable" => true,
              "description" => "Error message from last failed scrape"
            },
            "samples_scraped" => %{
              "type" => "integer",
              "description" => "Number of samples from last successful scrape"
            }
          }
        },
        "ScrapeTargetWithHealth" => %{
          "type" => "object",
          "properties" => %{
            "id" => %{"type" => "integer"},
            "job_name" => %{"type" => "string"},
            "scheme" => %{"type" => "string"},
            "address" => %{"type" => "string"},
            "metrics_path" => %{"type" => "string"},
            "scrape_interval" => %{"type" => "integer"},
            "scrape_timeout" => %{"type" => "integer"},
            "labels" => %{"type" => "object", "additionalProperties" => %{"type" => "string"}},
            "honor_labels" => %{"type" => "boolean"},
            "honor_timestamps" => %{"type" => "boolean"},
            "relabel_configs" => %{"type" => "array", "nullable" => true, "items" => %{"$ref" => "#/components/schemas/RelabelConfig"}},
            "metric_relabel_configs" => %{"type" => "array", "nullable" => true, "items" => %{"$ref" => "#/components/schemas/RelabelConfig"}},
            "enabled" => %{"type" => "boolean"},
            "created_at" => %{"type" => "integer"},
            "updated_at" => %{"type" => "integer"},
            "health" => %{"$ref" => "#/components/schemas/ScrapeHealth"}
          }
        }
      },
      "securitySchemes" => %{
        "bearerAuth" => %{
          "type" => "http",
          "scheme" => "bearer",
          "description" => "Optional bearer token. Only required if the server is configured with bearer_token."
        }
      }
    }
  end

  # --- Helpers ---

  defp target_id_param do
    %{
      "name" => "id",
      "in" => "path",
      "required" => true,
      "schema" => %{"type" => "integer"},
      "description" => "Scrape target ID"
    }
  end

  defp id_path_param(description) do
    %{
      "name" => "id",
      "in" => "path",
      "required" => true,
      "schema" => %{"type" => "integer"},
      "description" => description
    }
  end

  defp query_param(name, type, description, required) do
    param = %{
      "name" => name,
      "in" => "query",
      "schema" => %{"type" => type},
      "description" => description
    }

    if required, do: Map.put(param, "required", true), else: param
  end

  defp json_error_response(description) do
    %{
      "description" => description,
      "content" => %{
        "application/json" => %{
          "schema" => %{
            "type" => "object",
            "properties" => %{
              "error" => %{"type" => "string"}
            }
          }
        }
      }
    }
  end

  defp status_response(status) do
    %{
      "description" => "Success",
      "content" => %{
        "application/json" => %{
          "schema" => %{
            "type" => "object",
            "properties" => %{
              "status" => %{"type" => "string", "example" => status}
            }
          }
        }
      }
    }
  end

  defp created_response do
    %{
      "description" => "Created",
      "content" => %{
        "application/json" => %{
          "schema" => %{
            "type" => "object",
            "properties" => %{
              "id" => %{"type" => "integer"},
              "status" => %{"type" => "string", "example" => "created"}
            }
          }
        }
      }
    }
  end

  defp text_body(description, example) do
    %{
      "required" => true,
      "description" => description,
      "content" => %{
        "text/plain" => %{
          "schema" => %{"type" => "string"},
          "example" => example
        }
      }
    }
  end

  defp ingest_responses do
    %{
      "204" => %{"description" => "All samples ingested successfully"},
      "200" => %{
        "description" => "Partial success — some lines failed to parse",
        "content" => %{
          "application/json" => %{
            "schema" => %{
              "type" => "object",
              "properties" => %{
                "samples" => %{"type" => "integer"},
                "errors" => %{"type" => "integer"},
                "failed_lines" => %{"type" => "array", "items" => %{"type" => "string"}}
              }
            }
          }
        }
      },
      "413" => json_error_response("Request body too large (max 10 MB)")
    }
  end
end
