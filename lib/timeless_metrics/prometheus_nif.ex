defmodule TimelessMetrics.PrometheusNif do
  @moduledoc """
  NIF-based Prometheus text format parser.

  Parses the entire Prometheus exposition body in a single NIF call,
  returning `{entries, error_count}` where entries is a list of
  `{metric_name, labels_proplist, value, timestamp}` tuples.

  Lines without timestamps use 0 as sentinel.

  Backed by the Rust parser in `native/tms_engine` (it replaced an
  earlier C++ NIF). Entry binaries are zero-copy sub-binaries of `body`:
  they keep the whole body alive until garbage collected, so anything
  storing them long-term must `:binary.copy/1` them first.
  """

  @doc """
  Check if the NIF is loaded and available.
  """
  def available? do
    match?({[], 0}, parse(<<>>))
  rescue
    _ -> false
  end

  @doc """
  Parse Prometheus text format body.

  Returns `{entries, error_count}` where entries is a list of
  `{metric_name, labels_proplist, value, timestamp}` tuples.
  """
  defdelegate parse(body), to: TimelessMetrics.RustEngine.Nif, as: :parse_prometheus
end
