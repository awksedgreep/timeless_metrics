defmodule Timeless.ForecastTest do
  use ExUnit.Case, async: true

  alias Timeless.Forecast

  # Helper: generate sinusoidal data at a given interval
  defp sinusoidal_data(opts) do
    n = Keyword.fetch!(opts, :n)
    interval = Keyword.fetch!(opts, :interval)
    period = Keyword.get(opts, :period, 86_400)
    amplitude = Keyword.get(opts, :amplitude, 50.0)
    offset = Keyword.get(opts, :offset, 100.0)
    t0 = Keyword.get(opts, :t0, 1_700_000_000)

    for i <- 0..(n - 1) do
      ts = t0 + i * interval
      val = offset + amplitude * :math.sin(2 * :math.pi() * ts / period)
      {ts, val}
    end
  end

  # --- auto_periods ---

  test "auto_periods: sub-hourly interval returns daily + half-daily" do
    # 5-minute intervals
    timestamps = for i <- 0..100, do: 1_700_000_000 + i * 300
    assert Forecast.auto_periods(timestamps) == [86_400, 43_200]
  end

  test "auto_periods: hourly interval returns daily + weekly" do
    timestamps = for i <- 0..100, do: 1_700_000_000 + i * 3_600
    assert Forecast.auto_periods(timestamps) == [86_400, 604_800]
  end

  test "auto_periods: daily interval returns weekly + yearly" do
    timestamps = for i <- 0..100, do: 1_700_000_000 + i * 86_400
    assert Forecast.auto_periods(timestamps) == [604_800, 31_536_000]
  end

  # --- predict ---

  test "predict returns future values from sinusoidal data" do
    # 5-minute intervals, 3 days of data, predict 1 day ahead
    data = sinusoidal_data(n: 864, interval: 300, period: 86_400)

    {:ok, predictions} = Forecast.predict(data, horizon: 86_400)

    assert length(predictions) > 0

    # All predictions should be {timestamp, value} tuples
    Enum.each(predictions, fn {ts, val} ->
      assert is_integer(ts)
      assert is_float(val)
    end)

    # Timestamps should be after the training data
    {last_ts, _} = List.last(data)
    {first_pred_ts, _} = hd(predictions)
    assert first_pred_ts > last_ts

    # Last prediction should be within horizon
    {last_pred_ts, _} = List.last(predictions)
    assert last_pred_ts <= last_ts + 86_400
  end

  test "predict with sinusoidal data produces values near the original pattern" do
    # Generate clean sine wave, predict forward, check predictions are reasonable
    data = sinusoidal_data(n: 576, interval: 300, period: 86_400, amplitude: 50.0, offset: 100.0)

    {:ok, predictions} = Forecast.predict(data, horizon: 86_400)

    # Predicted values should stay roughly in the [50, 150] range (offset ± amplitude)
    Enum.each(predictions, fn {_ts, val} ->
      assert val > 20.0 and val < 180.0,
             "Predicted value #{val} outside expected range"
    end)
  end

  test "predict with custom periods" do
    data = sinusoidal_data(n: 200, interval: 300, period: 3_600)

    {:ok, predictions} = Forecast.predict(data, horizon: 3_600, periods: [3_600])

    assert length(predictions) > 0
  end

  # --- insufficient data ---

  test "predict returns error with insufficient data" do
    # With 2 auto periods (4 Fourier features), need at least 3 + 4 = 7 points
    data = for i <- 0..5, do: {1_700_000_000 + i * 300, 42.0}

    assert {:error, :insufficient_data} = Forecast.predict(data, horizon: 3_600)
  end

  test "predict returns error with single point" do
    data = [{1_700_000_000, 42.0}]

    assert {:error, :insufficient_data} = Forecast.predict(data, horizon: 3_600)
  end

  # --- constant value series ---

  test "predict handles constant value series without crashing" do
    # All same value — trend should be flat
    data = for i <- 0..50, do: {1_700_000_000 + i * 300, 42.0}

    {:ok, predictions} = Forecast.predict(data, horizon: 3_600)

    # All predictions should be close to 42.0
    Enum.each(predictions, fn {_ts, val} ->
      assert_in_delta val, 42.0, 1.0,
        "Constant series prediction #{val} too far from 42.0"
    end)
  end

  # --- fit_predict ---

  test "fit_predict returns fitted values matching training data length" do
    data = sinusoidal_data(n: 200, interval: 300)

    {:ok, fitted} = Forecast.fit_predict(data)

    assert length(fitted) == length(data)
    assert Enum.all?(fitted, &is_float/1)
  end

  test "fit_predict on clean sinusoidal data fits closely" do
    data = sinusoidal_data(n: 576, interval: 300, period: 86_400, amplitude: 50.0, offset: 100.0)

    {:ok, fitted} = Forecast.fit_predict(data)

    {_timestamps, values} = Enum.unzip(data)

    # Mean absolute error should be small for clean sine data
    mae =
      Enum.zip(values, fitted)
      |> Enum.map(fn {actual, pred} -> abs(actual - pred) end)
      |> Enum.sum()
      |> Kernel./(length(values))

    assert mae < 5.0, "MAE #{mae} too large for clean sinusoidal data"
  end

  test "fit_predict returns error with insufficient data" do
    data = [{1_700_000_000, 1.0}, {1_700_000_300, 2.0}]

    assert {:error, :insufficient_data} = Forecast.fit_predict(data)
  end
end
