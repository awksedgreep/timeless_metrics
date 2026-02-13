defmodule TimelessMetrics.AnomalyTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.Anomaly

  # Helper: generate smooth sine wave with optional outliers
  defp smooth_data(n, opts \\ []) do
    interval = Keyword.get(opts, :interval, 300)
    t0 = Keyword.get(opts, :t0, 1_700_000_000)
    outlier_indices = Keyword.get(opts, :outliers, [])
    outlier_value = Keyword.get(opts, :outlier_value, 500.0)

    for i <- 0..(n - 1) do
      ts = t0 + i * interval

      val =
        if i in outlier_indices do
          outlier_value
        else
          100.0 + 20.0 * :math.sin(2 * :math.pi() * i / 288)
        end

      {ts, val}
    end
  end

  test "detects obvious outliers in smooth data" do
    # 200 points of smooth sine with one massive outlier at index 100
    data = smooth_data(200, outliers: [100], outlier_value: 1000.0)

    {:ok, results} = Anomaly.detect(data)

    anomalies = Enum.filter(results, & &1.anomaly)

    # The outlier at index 100 should be flagged
    assert length(anomalies) >= 1

    outlier_point = Enum.at(results, 100)
    assert outlier_point.anomaly == true
    assert outlier_point.score > 2.5
  end

  test "sensitivity :low flags only extreme outliers" do
    data = smooth_data(200, outliers: [100], outlier_value: 1000.0)

    {:ok, results_low} = Anomaly.detect(data, sensitivity: :low)
    {:ok, results_high} = Anomaly.detect(data, sensitivity: :high)

    anomalies_low = Enum.count(results_low, & &1.anomaly)
    anomalies_high = Enum.count(results_high, & &1.anomaly)

    # Higher sensitivity should flag at least as many anomalies
    assert anomalies_high >= anomalies_low
  end

  test "sensitivity :high flags more points than :medium" do
    # Add some moderate outliers that high catches but medium might not
    data = smooth_data(200, outliers: [50, 100, 150], outlier_value: 250.0)

    {:ok, results_medium} = Anomaly.detect(data, sensitivity: :medium)
    {:ok, results_high} = Anomaly.detect(data, sensitivity: :high)

    anomalies_medium = Enum.count(results_medium, & &1.anomaly)
    anomalies_high = Enum.count(results_high, & &1.anomaly)

    assert anomalies_high >= anomalies_medium
  end

  test "constant series doesn't crash (std_dev → 0)" do
    data = for i <- 0..50, do: {1_700_000_000 + i * 300, 42.0}

    {:ok, results} = Anomaly.detect(data)

    assert length(results) == 51

    # With constant data and constant predictions, residuals are ~0
    # std_dev clamped to 1e-10, so z-scores should be near 0
    Enum.each(results, fn r ->
      assert is_float(r.score)
      assert is_boolean(r.anomaly)
    end)
  end

  test "empty data returns error" do
    assert {:error, :insufficient_data} = Anomaly.detect([])
  end

  test "single point returns error" do
    assert {:error, :insufficient_data} = Anomaly.detect([{1_700_000_000, 42.0}])
  end

  test "all values identical except one outlier" do
    data =
      for i <- 0..50 do
        val = if i == 25, do: 999.0, else: 42.0
        {1_700_000_000 + i * 300, val}
      end

    {:ok, results} = Anomaly.detect(data)

    outlier = Enum.at(results, 25)
    assert outlier.anomaly == true
    assert outlier.value == 999.0
  end

  test "result structure has all expected fields" do
    data = smooth_data(100)

    {:ok, results} = Anomaly.detect(data)

    Enum.each(results, fn r ->
      assert Map.has_key?(r, :timestamp)
      assert Map.has_key?(r, :value)
      assert Map.has_key?(r, :expected)
      assert Map.has_key?(r, :score)
      assert Map.has_key?(r, :anomaly)
    end)
  end
end
