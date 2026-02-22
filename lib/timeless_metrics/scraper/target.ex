defmodule TimelessMetrics.Scraper.Target do
  @moduledoc false

  defstruct [
    :id,
    :job_name,
    :address,
    :created_at,
    :updated_at,
    scheme: "http",
    metrics_path: "/metrics",
    scrape_interval: 30,
    scrape_timeout: 10,
    labels: %{},
    honor_labels: false,
    honor_timestamps: true,
    relabel_configs: nil,
    metric_relabel_configs: nil,
    enabled: true
  ]

  def from_row([id, job_name, scheme, address, metrics_path, scrape_interval, scrape_timeout,
                labels_json, honor_labels, honor_timestamps, relabel_json, metric_relabel_json,
                enabled, created_at, updated_at]) do
    %__MODULE__{
      id: id,
      job_name: job_name,
      scheme: scheme,
      address: address,
      metrics_path: metrics_path,
      scrape_interval: scrape_interval,
      scrape_timeout: scrape_timeout,
      labels: decode_json(labels_json, %{}),
      honor_labels: honor_labels == 1,
      honor_timestamps: honor_timestamps == 1,
      relabel_configs: decode_relabel_configs(relabel_json),
      metric_relabel_configs: decode_relabel_configs(metric_relabel_json),
      enabled: enabled == 1,
      created_at: created_at,
      updated_at: updated_at
    }
  end

  def validate(params) when is_map(params) do
    with :ok <- require_field(params, "job_name"),
         :ok <- require_field(params, "address"),
         :ok <- validate_interval(params) do
      {:ok, normalize_params(params)}
    end
  end

  def to_params(%__MODULE__{} = target) do
    [
      target.job_name,
      target.scheme,
      target.address,
      target.metrics_path,
      target.scrape_interval,
      target.scrape_timeout,
      Jason.encode!(target.labels),
      bool_to_int(target.honor_labels),
      bool_to_int(target.honor_timestamps),
      encode_relabel_configs(target.relabel_configs),
      encode_relabel_configs(target.metric_relabel_configs),
      bool_to_int(target.enabled)
    ]
  end

  def from_params(params) when is_map(params) do
    now = System.os_time(:second)

    %__MODULE__{
      job_name: params["job_name"],
      scheme: params["scheme"] || "http",
      address: params["address"],
      metrics_path: params["metrics_path"] || "/metrics",
      scrape_interval: params["scrape_interval"] || 30,
      scrape_timeout: params["scrape_timeout"] || 10,
      labels: params["labels"] || %{},
      honor_labels: params["honor_labels"] || false,
      honor_timestamps: Map.get(params, "honor_timestamps", true),
      relabel_configs: compile_relabel_configs(params["relabel_configs"]),
      metric_relabel_configs: compile_relabel_configs(params["metric_relabel_configs"]),
      enabled: Map.get(params, "enabled", true),
      created_at: now,
      updated_at: now
    }
  end

  def url(%__MODULE__{} = target) do
    "#{target.scheme}://#{target.address}#{target.metrics_path}"
  end

  # --- Private ---

  defp require_field(params, key) do
    case params[key] do
      nil -> {:error, "missing required field: #{key}"}
      "" -> {:error, "#{key} cannot be empty"}
      _ -> :ok
    end
  end

  defp validate_interval(params) do
    interval = params["scrape_interval"] || 30
    if is_integer(interval) and interval > 0, do: :ok, else: {:error, "scrape_interval must be > 0"}
  end

  defp normalize_params(params) do
    params
    |> Map.put_new("scheme", "http")
    |> Map.put_new("metrics_path", "/metrics")
    |> Map.put_new("scrape_interval", 30)
    |> Map.put_new("scrape_timeout", 10)
    |> Map.put_new("labels", %{})
    |> Map.put_new("honor_labels", false)
    |> Map.put_new("honor_timestamps", true)
    |> Map.put_new("enabled", true)
  end

  defp decode_json(nil, default), do: default
  defp decode_json(str, default) when is_binary(str) do
    case Jason.decode(str) do
      {:ok, val} -> val
      _ -> default
    end
  end

  defp decode_relabel_configs(nil), do: nil
  defp decode_relabel_configs(str) when is_binary(str) do
    case Jason.decode(str) do
      {:ok, configs} when is_list(configs) -> compile_relabel_configs(configs)
      _ -> nil
    end
  end

  defp compile_relabel_configs(nil), do: nil
  defp compile_relabel_configs(configs) when is_list(configs) do
    Enum.map(configs, fn config ->
      regex = config["regex"] || ".*"
      compiled = Regex.compile!(regex)
      Map.put(config, "__compiled_regex__", compiled)
    end)
  end

  defp encode_relabel_configs(nil), do: nil
  defp encode_relabel_configs(configs) when is_list(configs) do
    cleaned = Enum.map(configs, &Map.delete(&1, "__compiled_regex__"))
    Jason.encode!(cleaned)
  end

  defp bool_to_int(true), do: 1
  defp bool_to_int(false), do: 0
  defp bool_to_int(1), do: 1
  defp bool_to_int(0), do: 0
end
