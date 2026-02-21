defmodule TimelessMetrics.PrometheusNifTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.PrometheusNif

  test "NIF is loaded and available" do
    assert PrometheusNif.available?()
  end

  test "parses empty body" do
    {entries, errors} = PrometheusNif.parse(<<>>)
    assert entries == []
    assert errors == 0
  end

  test "parses metric with labels and timestamp" do
    body = ~s(cpu_usage{host="web-1",region="us-east"} 42.5 1700000000000\n)
    {[entry], errors} = PrometheusNif.parse(body)
    assert errors == 0

    {name, labels, value, ts} = entry
    assert name == "cpu_usage"
    assert value == 42.5
    assert ts == 1_700_000_000_000
    labels_map = Map.new(labels)
    assert labels_map == %{"host" => "web-1", "region" => "us-east"}
  end

  test "parses metric without labels" do
    body = "up 1.0 1700000000000\n"
    {[entry], errors} = PrometheusNif.parse(body)
    assert errors == 0

    {name, labels, value, ts} = entry
    assert name == "up"
    assert labels == []
    assert value == 1.0
    assert ts == 1_700_000_000_000
  end

  test "parses metric without timestamp (uses 0 sentinel)" do
    body = "memory_bytes 1024.0\n"
    {[entry], errors} = PrometheusNif.parse(body)
    assert errors == 0

    {name, _labels, value, ts} = entry
    assert name == "memory_bytes"
    assert value == 1024.0
    assert ts == 0
  end

  test "skips comment and empty lines" do
    body = """
    # HELP cpu_usage CPU usage percentage
    # TYPE cpu_usage gauge

    cpu_usage{host="a"} 50.0 1700000000000
    """

    {entries, errors} = PrometheusNif.parse(body)
    assert errors == 0
    assert length(entries) == 1

    {name, _labels, value, _ts} = hd(entries)
    assert name == "cpu_usage"
    assert value == 50.0
  end

  test "parses multiple lines" do
    body = """
    metric_a{host="h1"} 1.0 1700000000000
    metric_b{host="h2"} 2.0 1700000000000
    metric_c 3.0
    """

    {entries, errors} = PrometheusNif.parse(body)
    assert errors == 0
    assert length(entries) == 3
  end

  test "counts errors for malformed lines" do
    body = """
    valid_metric 1.0
    not valid at all
    another_valid{k="v"} 2.0 1700000000000
    """

    {entries, errors} = PrometheusNif.parse(body)
    assert errors == 1
    assert length(entries) == 2
  end

  test "rejects Inf/NaN values (BEAM doesn't support them)" do
    body = """
    inf_metric +Inf
    ninf_metric -Inf
    nan_metric NaN
    valid_metric 1.0
    """

    {entries, errors} = PrometheusNif.parse(body)
    assert errors == 3
    assert length(entries) == 1
    assert {_, _, 1.0, _} = hd(entries)
  end

  test "parses integer values" do
    body = "counter_total 42 1700000000000\n"
    {[entry], errors} = PrometheusNif.parse(body)
    assert errors == 0

    {_name, _labels, value, _ts} = entry
    assert value == 42.0
  end

  test "handles empty label set" do
    body = "metric{} 1.0\n"
    {[entry], errors} = PrometheusNif.parse(body)
    assert errors == 0

    {name, labels, value, _ts} = entry
    assert name == "metric"
    assert labels == []
    assert value == 1.0
  end
end
