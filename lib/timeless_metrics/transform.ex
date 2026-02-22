defmodule TimelessMetrics.Transform do
  @moduledoc """
  Post-query value transforms for unit conversion.

  Applied to aggregated values after bucketing. Useful for converting
  raw SNMP values (e.g. tenths of dBmV) to human-readable units.

  ## Supported transforms

    * `{:multiply, n}` — multiply each value by n
    * `{:divide, n}` — divide each value by n
    * `{:offset, n}` — add n to each value
    * `{:scale, multiplier, offset}` — `value * multiplier + offset` (e.g. Celsius to Fahrenheit)
    * `{:log10}` — base-10 logarithm (useful for RF power ratios)
    * `nil` — no transform (passthrough)
  """

  @doc """
  Apply a transform to a list of `{timestamp, value}` tuples.
  """
  def apply(data, nil), do: data
  def apply(data, {:multiply, n}), do: Enum.map(data, fn {ts, v} -> {ts, v * n} end)
  def apply(data, {:divide, n}), do: Enum.map(data, fn {ts, v} -> {ts, v / n} end)
  def apply(data, {:offset, n}), do: Enum.map(data, fn {ts, v} -> {ts, v + n} end)

  def apply(data, {:scale, m, b}),
    do: Enum.map(data, fn {ts, v} -> {ts, v * m + b} end)

  def apply(data, {:log10}),
    do: Enum.map(data, fn {ts, v} -> {ts, if(v > 0, do: :math.log10(v), else: 0.0)} end)

  @doc """
  Parse a transform string from HTTP params.

  ## Examples

      parse("divide:10")    #=> {:divide, 10.0}
      parse("multiply:100") #=> {:multiply, 100.0}
      parse("offset:-273")  #=> {:offset, -273.0}
      parse("scale:1.8:32") #=> {:scale, 1.8, 32.0}
      parse("log10")        #=> {:log10}
      parse(nil)            #=> nil
  """
  def parse(nil), do: nil

  def parse(str) when is_binary(str) do
    case String.split(str, ":", parts: 3) do
      ["multiply", n] -> {:multiply, parse_number(n)}
      ["divide", n] -> {:divide, parse_number(n)}
      ["offset", n] -> {:offset, parse_number(n)}
      ["scale", m, b] -> {:scale, parse_number(m), parse_number(b)}
      ["log10"] -> {:log10}
      _ -> nil
    end
  end

  @doc """
  Parse a threshold filter string.

  ## Examples

      parse_threshold("filter_gt:90")  #=> {:gt, 90.0}
      parse_threshold("filter_lt:10")  #=> {:lt, 10.0}
      parse_threshold(nil)             #=> nil
  """
  def parse_threshold(nil), do: nil

  def parse_threshold(str) when is_binary(str) do
    case String.split(str, ":", parts: 2) do
      ["filter_gt", n] -> {:gt, parse_number(n)}
      ["filter_lt", n] -> {:lt, parse_number(n)}
      _ -> nil
    end
  end

  defp parse_number(str) do
    case Float.parse(str) do
      {n, _} -> n
      :error -> 0.0
    end
  end
end
