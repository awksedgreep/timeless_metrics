defmodule TimelessMetrics.PrometheusNif do
  @moduledoc """
  NIF-based Prometheus text format parser.

  Parses the entire Prometheus exposition body in a single NIF call,
  returning `{entries, error_count}` where entries is a list of
  `{metric_name, labels_proplist, value, timestamp}` tuples.

  Lines without timestamps use 0 as sentinel.
  """

  @on_load :load_nif

  def load_nif do
    path = :filename.join(:code.priv_dir(:timeless_metrics), ~c"prometheus_nif")

    case :erlang.load_nif(path, 0) do
      :ok -> :ok
      {:error, {:reload, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Check if the NIF is loaded and available.
  """
  def available? do
    case parse(<<>>) do
      {[], 0} -> true
      _ -> true
    end
  rescue
    _ -> false
  end

  @doc """
  Parse Prometheus text format body.

  Returns `{entries, error_count}` where entries is a list of
  `{metric_name, labels_proplist, value, timestamp}` tuples.
  """
  def parse(_body), do: :erlang.nif_error(:not_loaded)
end
