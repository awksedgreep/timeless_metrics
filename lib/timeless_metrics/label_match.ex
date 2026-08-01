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

    # A map can't hold duplicate keys, and Prometheus ANDs duplicate matchers
    # ({host="a",host="b"} matches nothing) — keys appearing more than once
    # must stay in the post-filter list.
    dup_keys =
      eq
      |> Enum.frequencies_by(fn {k, _v} -> k end)
      |> Enum.filter(fn {_k, n} -> n > 1 end)
      |> MapSet.new(fn {k, _n} -> k end)

    {eq_unique, eq_dup} = Enum.split_with(eq, fn {k, _v} -> k not in dup_keys end)

    {Map.new(eq_unique), eq_dup ++ complex}
  end

  @doc """
  Build the strongest libSQL matcher JSON that is provably equivalent to a
  necessary subset of `label_filter`.

  The extension supports equality, inequality, and RE2-family regex matchers,
  while TimelessMetrics currently evaluates regular expressions with PCRE.
  Only a deliberately portable regex subset is pushed down. Unsupported
  patterns remain in the residual filter, so correctness never depends on a
  dialect guess. Duplicate matchers keep their full residual AND expression;
  one safe matcher may still narrow the storage candidates.

  Returns `:none` when an invalid regex makes the complete filter impossible,
  otherwise `{extension_filter, residual_filter}`.
  """
  def split_libsql_pushdown(label_filter) do
    entries = Enum.to_list(label_filter)

    if Enum.any?(entries, fn {_key, matcher} -> invalid_regex_matcher?(matcher) end) do
      :none
    else
      entries
      |> Enum.map(&elem(&1, 0))
      |> Enum.uniq()
      |> Enum.reduce({%{}, []}, fn key, {pushdown, residual} ->
        keyed = Enum.filter(entries, fn {entry_key, _matcher} -> entry_key == key end)

        case Enum.find_value(keyed, fn {_key, matcher} -> libsql_matcher(matcher) end) do
          nil ->
            {pushdown, residual ++ keyed}

          encoded when length(keyed) == 1 ->
            {Map.put(pushdown, key, encoded), residual}

          encoded ->
            # JSON cannot retain duplicate object keys. A single matcher is a
            # safe necessary condition; the complete duplicate AND remains
            # above the boundary.
            {Map.put(pushdown, key, encoded), residual ++ keyed}
        end
      end)
    end
  end

  defp invalid_regex_matcher?({kind, pattern}) when kind in [:regex, :not_regex],
    do: compile_anchored(pattern) == :invalid

  defp invalid_regex_matcher?(_matcher), do: false

  defp libsql_matcher(value) when is_binary(value) and value != "", do: value

  # The registry equality index distinguishes an absent label from an explicit
  # empty value. Prometheus does not, so express empty equality as an anchored
  # regex; the extension applies regexes to absent labels as "".
  defp libsql_matcher(""), do: %{"re" => ""}
  defp libsql_matcher({:not_equal, value}) when is_binary(value), do: %{"neq" => value}

  defp libsql_matcher({:regex, pattern}) when is_binary(pattern) do
    if portable_libsql_regex?(pattern), do: %{"re" => pattern}
  end

  defp libsql_matcher({:not_regex, pattern}) when is_binary(pattern) do
    if portable_libsql_regex?(pattern), do: %{"nre" => pattern}
  end

  defp libsql_matcher(_matcher), do: nil

  # This subset is intentionally smaller than either regex engine. Printable
  # ASCII literals, grouping, alternation, and * / + repetition agree across
  # PCRE and Rust regex. Backslashes, classes, anchors, optional/advanced
  # groups, counted quantifiers, and a bare dot stay in the residual path.
  # Dot-star and dot-plus are safe for valid UTF-8 strings; a bare dot is not
  # (PCRE without its unicode option counts bytes, Rust regex counts scalars).
  defp portable_libsql_regex?(pattern) do
    compile_anchored(pattern) != :invalid and
      pattern
      |> :binary.bin_to_list()
      |> Enum.all?(&(&1 in 0x20..0x7E)) and
      not String.contains?(pattern, ["\\", "[", "]", "{", "}", "^", "$", "?"]) and
      not String.contains?(pattern, "(*") and
      not Regex.match?(~r/[*+][*+]/, pattern) and
      repeated_dots_only?(pattern)
  end

  defp repeated_dots_only?(pattern) do
    pattern
    |> :binary.matches(".")
    |> Enum.all?(fn {index, 1} ->
      index + 1 < byte_size(pattern) and
        :binary.at(pattern, index + 1) in [?*, ?+]
    end)
  end

  defp compile_anchored(pattern) do
    case Regex.compile("^(?:" <> pattern <> ")$") do
      {:ok, re} -> re
      {:error, _} -> :invalid
    end
  end
end
