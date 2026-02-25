defmodule TimelessMetrics.Scraper.Relabel do
  @moduledoc false

  @default_separator ";"

  def apply_configs(labels, nil), do: {:ok, labels}
  def apply_configs(labels, []), do: {:ok, labels}

  def apply_configs(labels, [config | rest]) do
    case apply_one(labels, config) do
      :drop -> :drop
      {:ok, labels} -> apply_configs(labels, rest)
    end
  end

  def target_meta_labels(%{} = target) do
    %{
      "__address__" => target.address,
      "__metrics_path__" => target.metrics_path,
      "__scheme__" => target.scheme,
      "__scrape_interval__" => to_string(target.scrape_interval) <> "s",
      "__scrape_timeout__" => to_string(target.scrape_timeout) <> "s",
      "job" => target.job_name,
      "instance" => target.address
    }
  end

  def drop_meta_labels(labels) do
    Map.reject(labels, fn {k, _v} -> String.starts_with?(k, "__") end)
  end

  def apply_honor_labels(scraped_labels, target_labels, true = _honor?) do
    Map.merge(target_labels, scraped_labels)
  end

  def apply_honor_labels(scraped_labels, target_labels, false = _honor?) do
    Enum.reduce(scraped_labels, target_labels, fn {k, v}, acc ->
      if Map.has_key?(acc, k) do
        acc
        |> Map.put("exported_" <> k, v)
      else
        Map.put(acc, k, v)
      end
    end)
  end

  # --- Actions ---

  defp apply_one(labels, %{"action" => "replace"} = config) do
    source_labels = Map.get(config, "source_labels", [])
    separator = Map.get(config, "separator", @default_separator)
    target_label = Map.get(config, "target_label", "__name__")
    replacement = Map.get(config, "replacement", "$1")
    regex = get_regex(config)

    source_value = join_source_labels(labels, source_labels, separator)

    case Regex.run(regex, source_value) do
      nil ->
        {:ok, labels}

      captures ->
        new_value = apply_replacement(replacement, captures)
        {:ok, Map.put(labels, target_label, new_value)}
    end
  end

  defp apply_one(labels, %{"action" => "keep"} = config) do
    source_labels = Map.get(config, "source_labels", [])
    separator = Map.get(config, "separator", @default_separator)
    regex = get_regex(config)

    source_value = join_source_labels(labels, source_labels, separator)

    if Regex.match?(regex, source_value), do: {:ok, labels}, else: :drop
  end

  defp apply_one(labels, %{"action" => "drop"} = config) do
    source_labels = Map.get(config, "source_labels", [])
    separator = Map.get(config, "separator", @default_separator)
    regex = get_regex(config)

    source_value = join_source_labels(labels, source_labels, separator)

    if Regex.match?(regex, source_value), do: :drop, else: {:ok, labels}
  end

  defp apply_one(labels, %{"action" => "labeldrop"} = config) do
    regex = get_regex(config)
    filtered = Map.reject(labels, fn {k, _v} -> Regex.match?(regex, k) end)
    {:ok, filtered}
  end

  defp apply_one(labels, %{"action" => "labelkeep"} = config) do
    regex = get_regex(config)
    filtered = Map.filter(labels, fn {k, _v} -> Regex.match?(regex, k) end)
    {:ok, filtered}
  end

  defp apply_one(labels, %{"action" => "labelmap"} = config) do
    regex = get_regex(config)
    replacement = Map.get(config, "replacement", "$1")

    remapped =
      Enum.reduce(labels, %{}, fn {k, v}, acc ->
        case Regex.run(regex, k) do
          nil ->
            Map.put(acc, k, v)

          captures ->
            new_key = apply_replacement(replacement, captures)
            Map.put(acc, new_key, v)
        end
      end)

    {:ok, remapped}
  end

  # Default: treat as replace
  defp apply_one(labels, config) when is_map(config) do
    apply_one(labels, Map.put_new(config, "action", "replace"))
  end

  # --- Helpers ---

  defp get_regex(%{"__compiled_regex__" => regex}), do: regex
  defp get_regex(%{"regex" => pattern}), do: Regex.compile!(pattern)
  defp get_regex(_), do: ~r/.*/

  defp join_source_labels(labels, source_labels, separator) do
    source_labels
    |> Enum.map(fn key -> Map.get(labels, key, "") end)
    |> Enum.join(separator)
  end

  defp apply_replacement(replacement, captures) do
    full_match = List.first(captures) || ""

    replacement
    |> String.replace("$0", full_match)
    |> replace_numbered_captures(captures)
  end

  defp replace_numbered_captures(str, captures) do
    captures
    |> Enum.with_index()
    |> Enum.drop(1)
    |> Enum.reduce(str, fn {capture, idx}, acc ->
      String.replace(acc, "$#{idx}", capture)
    end)
  end
end
