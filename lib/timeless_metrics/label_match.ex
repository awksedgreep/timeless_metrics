defmodule TimelessMetrics.LabelMatch do
  @moduledoc """
  Shared label-filter matching for query paths (both engines).

  Filter entries take the shapes produced by the PromQL parser and native
  HTTP params:

    * `{key, "value"}` — exact match
    * `{key, {:regex, pattern}}` — anchored regex match
    * `{key, {:not_equal, "value"}}` — negation
    * `{key, {:not_regex, pattern}}` — anchored regex negation

  Semantics follow Prometheus: a series that lacks the label is treated as
  having the empty-string value, so `label!="v"` and `label=~".*"` match
  series without the label, and `label=""` matches only series without it.
  Invalid regex patterns match nothing (the query layer is expected to
  reject them earlier).
  """

  @doc """
  Pre-compile the regex entries of a filter. Returns an opaque compiled
  filter for `match?/2`.
  """
  def compile(label_filter) do
    Enum.map(label_filter, fn
      {k, {:regex, pattern}} -> {k, {:regex, compile_anchored(pattern)}}
      {k, {:not_regex, pattern}} -> {k, {:not_regex, compile_anchored(pattern)}}
      {k, other} -> {k, other}
    end)
  end

  @doc """
  Does a series' label map satisfy every entry of a compiled filter?
  """
  def match?(labels, compiled_filter) do
    Enum.all?(compiled_filter, fn {k, matcher} ->
      value = Map.get(labels, k, "")

      case matcher do
        {:regex, :invalid} -> false
        {:regex, re} -> Regex.match?(re, value)
        {:not_regex, :invalid} -> false
        {:not_regex, re} -> not Regex.match?(re, value)
        {:not_equal, v} -> value != v
        v when is_binary(v) -> value == v
      end
    end)
  end

  @doc """
  Split a filter into `{equality_map, complex_entries}` where the equality
  map contains only non-empty exact matches (safe to push down to storage
  lookups keyed on present labels) and complex entries need `match?/2`
  post-filtering.
  """
  def split_pushdown(label_filter) do
    {eq, complex} =
      Enum.split_with(label_filter, fn {_k, v} -> is_binary(v) and v != "" end)

    {Map.new(eq), complex}
  end

  defp compile_anchored(pattern) do
    case Regex.compile("^(?:" <> pattern <> ")$") do
      {:ok, re} -> re
      {:error, _} -> :invalid
    end
  end
end
