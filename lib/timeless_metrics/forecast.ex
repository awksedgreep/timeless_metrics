defmodule TimelessMetrics.Forecast do
  @moduledoc """
  Time series forecasting using linear regression with seasonal features.

  Fits a polynomial trend (degree 2) plus Fourier seasonal terms whose periods
  are automatically chosen based on the data's sampling interval:

    * **Sub-hourly** data (e.g. 5-min) → daily + half-daily periods (operational monitoring)
    * **Hourly** data → daily + weekly periods
    * **Daily** data → weekly + yearly periods (capacity planning)

  Periods can also be set explicitly via the `:periods` option (list of seconds).

  Uses the normal equation (X'X)⁻¹X'y — no external ML libraries needed.
  """

  @doc """
  Predict future values from historical time series data.

  ## Parameters

    * `data` - List of `{timestamp, value}` tuples
    * `opts` - Options:
      * `:horizon` - Seconds to forecast ahead (required)
      * `:bucket` - Seconds between forecast points (default: inferred from data)
      * `:periods` - List of seasonal period lengths in seconds
        (default: auto-detected from sampling interval)

  Returns `{:ok, [{future_timestamp, predicted_value}, ...]}` or `{:error, reason}`.
  """
  def predict(data, opts) do
    horizon = Keyword.fetch!(opts, :horizon)

    case fit(data, opts) do
      {:ok, coefficients, t_min, t_range, periods} ->
        {timestamps, _values} = Enum.unzip(data)
        t_max = Enum.max(timestamps)
        bucket = Keyword.get_lazy(opts, :bucket, fn -> infer_bucket(timestamps) end)

        future_ts = generate_future(t_max, horizon, bucket)
        future_features = Enum.map(future_ts, &build_row(&1, t_min, t_range, periods))
        predictions = predict_rows(coefficients, future_features)

        results = Enum.zip(future_ts, predictions)
        {:ok, results}

      error ->
        error
    end
  end

  @doc """
  Fit a model to data and return predictions for the training data itself.
  Used by anomaly detection to compute residuals.

  ## Options

    * `:periods` - List of seasonal period lengths in seconds
      (default: auto-detected from sampling interval)
  """
  def fit_predict(data, opts \\ []) do
    case fit(data, opts) do
      {:ok, coefficients, t_min, t_range, periods} ->
        {timestamps, _values} = Enum.unzip(data)
        features = Enum.map(timestamps, &build_row(&1, t_min, t_range, periods))
        {:ok, predict_rows(coefficients, features)}

      error ->
        error
    end
  end

  defp fit(data, opts) do
    {timestamps, values} = Enum.unzip(data)
    n = length(data)
    periods = Keyword.get_lazy(opts, :periods, fn -> auto_periods(timestamps) end)
    # intercept(1) + t(1) + t²(1) + 2 features per period
    min_points = 3 + 2 * length(periods)

    if n < min_points do
      {:error, :insufficient_data}
    else
      t_min = Enum.min(timestamps)
      t_range = max(Enum.max(timestamps) - t_min, 1)

      # Build design matrix with intercept column
      x_rows =
        Enum.map(timestamps, fn ts ->
          [1.0 | build_row(ts, t_min, t_range, periods)]
        end)

      coefficients = solve_ols(x_rows, values)
      {:ok, coefficients, t_min, t_range, periods}
    end
  end

  defp build_row(ts, t_min, t_range, periods) do
    t_norm = (ts - t_min) / t_range

    seasonal =
      Enum.flat_map(periods, fn period ->
        angle = 2 * :math.pi() * ts / period
        [:math.sin(angle), :math.cos(angle)]
      end)

    [t_norm, t_norm * t_norm | seasonal]
  end

  @doc """
  Auto-detect seasonal periods based on median sampling interval.

  Returns a list of period lengths in seconds:

    * Interval < 1 hour → `[86400, 43200]` (daily + half-daily)
    * Interval 1h–23h → `[86400, 604800]` (daily + weekly)
    * Interval >= 1 day → `[604800, 31_536_000]` (weekly + yearly)
  """
  def auto_periods(timestamps) do
    interval = median_interval(timestamps)

    cond do
      interval >= 86_400 -> [604_800, 31_536_000]
      interval >= 3_600 -> [86_400, 604_800]
      true -> [86_400, 43_200]
    end
  end

  defp median_interval(timestamps) do
    diffs =
      timestamps
      |> Enum.sort()
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [a, b] -> b - a end)
      |> Enum.sort()

    case diffs do
      [] -> 300
      _ -> Enum.at(diffs, div(length(diffs), 2))
    end
  end

  defp predict_rows(coefficients, feature_rows) do
    Enum.map(feature_rows, fn row ->
      [1.0 | row]
      |> Enum.zip(coefficients)
      |> Enum.reduce(0.0, fn {x, c}, acc -> acc + x * c end)
      |> Float.round(4)
    end)
  end

  # Ordinary least squares via normal equation: θ = (X'X)⁻¹ X'y
  defp solve_ols(x_rows, y_values) do
    p = length(hd(x_rows))

    # X'X (p x p)
    xtx = transpose_multiply(x_rows, p)

    # X'y (p x 1)
    xty = transpose_vector_multiply(x_rows, y_values, p)

    # Solve X'X * θ = X'y via Gaussian elimination
    gauss_solve(xtx, xty)
  end

  # Compute X'X efficiently: result[i][j] = sum of x[k][i] * x[k][j] for all rows k
  defp transpose_multiply(rows, p) do
    # Convert rows to tuple of tuples for O(1) access
    row_tuples = Enum.map(rows, &List.to_tuple/1) |> List.to_tuple()
    n = tuple_size(row_tuples)

    for i <- 0..(p - 1) do
      for j <- 0..(p - 1) do
        Enum.reduce(0..(n - 1), 0.0, fn k, acc ->
          row = elem(row_tuples, k)
          acc + elem(row, i) * elem(row, j)
        end)
      end
    end
  end

  # Compute X'y: result[i] = sum of x[k][i] * y[k] for all rows k
  defp transpose_vector_multiply(rows, y_values, p) do
    row_tuples = Enum.map(rows, &List.to_tuple/1) |> List.to_tuple()
    y_tuple = List.to_tuple(y_values)
    n = tuple_size(row_tuples)

    for i <- 0..(p - 1) do
      Enum.reduce(0..(n - 1), 0.0, fn k, acc ->
        acc + elem(elem(row_tuples, k), i) * elem(y_tuple, k)
      end)
    end
  end

  # Gaussian elimination with partial pivoting
  defp gauss_solve(a_matrix, b_vector) do
    n = length(b_vector)

    # Build augmented matrix as tuple of tuples for fast access
    aug =
      Enum.zip(a_matrix, b_vector)
      |> Enum.map(fn {row, b} -> List.to_tuple(row ++ [b]) end)
      |> List.to_tuple()

    # Forward elimination
    aug =
      Enum.reduce(0..(n - 2), aug, fn col, aug ->
        # Find pivot row (max absolute value in column)
        pivot_idx =
          col..(n - 1)
          |> Enum.max_by(fn i -> abs(elem(elem(aug, i), col)) end)

        # Swap rows
        aug =
          if pivot_idx != col do
            row_col = elem(aug, col)
            row_pivot = elem(aug, pivot_idx)
            aug |> put_elem(col, row_pivot) |> put_elem(pivot_idx, row_col)
          else
            aug
          end

        pivot_row = elem(aug, col)
        pivot_val = elem(pivot_row, col)

        # Eliminate below
        Enum.reduce((col + 1)..(n - 1), aug, fn i, aug ->
          row = elem(aug, i)
          factor = elem(row, col) / pivot_val

          new_row =
            for j <- 0..n do
              elem(row, j) - factor * elem(pivot_row, j)
            end
            |> List.to_tuple()

          put_elem(aug, i, new_row)
        end)
      end)

    # Back substitution
    x = :array.new(n, default: 0.0)

    x =
      Enum.reduce((n - 1)..0//-1, x, fn i, x ->
        row = elem(aug, i)

        sum =
          Enum.reduce((i + 1)..(n - 1)//1, 0.0, fn j, acc ->
            acc + elem(row, j) * :array.get(j, x)
          end)

        val = (elem(row, n) - sum) / elem(row, i)
        :array.set(i, val, x)
      end)

    for i <- 0..(n - 1), do: :array.get(i, x)
  end

  defp generate_future(t_max, horizon, bucket) do
    first = t_max + bucket
    last = t_max + horizon
    Enum.take_while(Stream.iterate(first, &(&1 + bucket)), &(&1 <= last))
  end

  defp infer_bucket(timestamps) do
    timestamps
    |> Enum.sort()
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [a, b] -> b - a end)
    |> then(fn
      [] -> 300
      diffs -> Enum.min(diffs)
    end)
  end
end
