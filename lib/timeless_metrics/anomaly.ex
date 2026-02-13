defmodule TimelessMetrics.Anomaly do
  @moduledoc """
  Anomaly detection via z-score analysis on residuals from a trend model.

  Fits the same polynomial + seasonal model as `TimelessMetrics.Forecast`, computes
  residuals (actual - predicted), and flags points where the z-score exceeds
  a threshold based on the chosen sensitivity level.

  Sensitivity thresholds:
    * `:low` — z > 3.0 (flags only extreme outliers)
    * `:medium` — z > 2.5 (default, good balance)
    * `:high` — z > 2.0 (flags more subtle anomalies)
  """

  @thresholds %{low: 3.0, medium: 2.5, high: 2.0}

  @doc """
  Detect anomalies in time series data.

  ## Parameters

    * `data` - List of `{timestamp, value}` tuples
    * `opts` - Options:
      * `:sensitivity` - `:low`, `:medium`, or `:high` (default: `:medium`)
      * `:periods` - Seasonal period lengths in seconds (default: auto-detected)

  Returns `{:ok, [%{timestamp, value, expected, score, anomaly}, ...]}` or `{:error, reason}`.
  """
  def detect(data, opts \\ []) do
    sensitivity = Keyword.get(opts, :sensitivity, :medium)
    threshold = Map.get(@thresholds, sensitivity, 2.5)
    forecast_opts = Keyword.take(opts, [:periods])

    case TimelessMetrics.Forecast.fit_predict(data, forecast_opts) do
      {:ok, expected_values} ->
        {timestamps, values} = Enum.unzip(data)

        residuals =
          Enum.zip(values, expected_values)
          |> Enum.map(fn {actual, expected} -> actual - expected end)

        mean_r = mean(residuals)
        std_r = max(std_dev(residuals, mean_r), 1.0e-10)

        results =
          [timestamps, values, expected_values, residuals]
          |> Enum.zip()
          |> Enum.map(fn {ts, val, expected, residual} ->
            z = (residual - mean_r) / std_r

            %{
              timestamp: ts,
              value: Float.round(val * 1.0, 4),
              expected: Float.round(expected * 1.0, 4),
              score: Float.round(abs(z), 2),
              anomaly: abs(z) > threshold
            }
          end)

        {:ok, results}

      error ->
        error
    end
  end

  defp mean(values) do
    Enum.sum(values) / length(values)
  end

  defp std_dev(values, mean) do
    n = length(values)

    variance =
      values
      |> Enum.reduce(0.0, fn v, acc -> acc + (v - mean) * (v - mean) end)
      |> Kernel./(n)

    :math.sqrt(variance)
  end
end
