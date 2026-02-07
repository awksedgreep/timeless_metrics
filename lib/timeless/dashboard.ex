defmodule Timeless.Dashboard do
  @moduledoc """
  Zero-dependency HTML dashboard generator.

  Renders a responsive page of metric charts using embedded SVG `<img>` tags,
  alert status badges, and auto-refresh. No JavaScript frameworks — just inline
  CSS and ~30 lines of vanilla JS.
  """

  @doc """
  Render the dashboard HTML page.

  ## Options

    * `:store` - Store name (required)
    * `:from` - Time range start (default: "-1h")
    * `:to` - Time range end (default: "now")
    * `:filter` - Label filter map from query params
    * `:width` - Chart width (default: 740)
    * `:height` - Chart height (default: 220)
  """
  def render(opts) do
    store = Keyword.fetch!(opts, :store)
    from = Keyword.get(opts, :from, "-1h")
    to = Keyword.get(opts, :to, "now")
    filter = Keyword.get(opts, :filter, %{})
    chart_w = Keyword.get(opts, :width, 740)
    chart_h = Keyword.get(opts, :height, 220)

    {:ok, metrics} = Timeless.list_metrics(store)
    {:ok, alerts} = Timeless.list_alerts(store)

    firing_count =
      alerts
      |> Enum.flat_map(& &1.states)
      |> Enum.count(&(&1.state == "firing"))

    info = Timeless.info(store)

    label_qs =
      filter
      |> Enum.map(fn {k, v} -> "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}" end)
      |> Enum.join("&")

    chart_cards =
      metrics
      |> Enum.map(fn metric ->
        qs = "metric=#{URI.encode_www_form(metric)}&from=#{from}&to=#{to}&theme=auto&width=#{chart_w}&height=#{chart_h}"
        qs = if label_qs != "", do: qs <> "&" <> label_qs, else: qs

        """
        <div class="card">
          <img src="/chart?#{qs}" alt="#{escape(metric)}" loading="lazy" width="#{chart_w}" height="#{chart_h}">
        </div>
        """
      end)
      |> Enum.join("\n")

    alert_rows =
      alerts
      |> Enum.flat_map(fn rule ->
        rule.states
        |> Enum.filter(&(&1.state in ["firing", "pending"]))
        |> Enum.map(fn state ->
          badge = if state.state == "firing", do: "badge-fire", else: "badge-pending"

          """
          <tr>
            <td><span class="badge #{badge}">#{state.state}</span></td>
            <td>#{escape(rule.name)}</td>
            <td>#{escape(rule.metric)}</td>
            <td>#{format_labels(state.series_labels)}</td>
            <td>#{format_value(state.last_value)} #{rule.condition} #{format_value(rule.threshold)}</td>
          </tr>
          """
        end)
      end)
      |> Enum.join("\n")

    alert_section =
      if alert_rows != "" do
        """
        <details class="alert-section" open>
          <summary>Active Alerts (#{firing_count} firing)</summary>
          <table class="alert-table">
            <thead><tr><th>State</th><th>Name</th><th>Metric</th><th>Series</th><th>Value</th></tr></thead>
            <tbody>#{alert_rows}</tbody>
          </table>
        </details>
        """
      else
        ""
      end

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Timeless</title>
      <style>
        :root { --bg: #f9fafb; --card: #ffffff; --text: #111827; --muted: #6b7280; --border: #e5e7eb; --accent: #2563eb; }
        @media (prefers-color-scheme: dark) {
          :root { --bg: #111827; --card: #1f2937; --text: #f9fafb; --muted: #9ca3af; --border: #374151; --accent: #60a5fa; }
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: var(--bg); color: var(--text); font-family: -apple-system, system-ui, sans-serif; font-size: 14px; }
        .header { padding: 12px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
        .header h1 { font-size: 18px; font-weight: 600; }
        .header .stats { color: var(--muted); font-size: 13px; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
        .badge-fire { background: #dc2626; color: #fff; }
        .badge-pending { background: #d97706; color: #fff; }
        .badge-ok { background: #16a34a; color: #fff; }
        .fire-count { margin-left: auto; }
        .controls { padding: 8px 20px; border-bottom: 1px solid var(--border); display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
        .controls select, .controls input { background: var(--card); color: var(--text); border: 1px solid var(--border); border-radius: 4px; padding: 4px 8px; font-size: 13px; }
        .controls button { background: var(--accent); color: #fff; border: none; border-radius: 4px; padding: 4px 12px; font-size: 13px; cursor: pointer; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(#{chart_w}px, 1fr)); gap: 12px; padding: 16px 20px; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
        .card img { display: block; width: 100%; height: auto; }
        .alert-section { padding: 8px 20px; }
        .alert-section summary { cursor: pointer; font-weight: 600; font-size: 14px; padding: 4px 0; }
        .alert-table { width: 100%; border-collapse: collapse; margin-top: 4px; font-size: 13px; }
        .alert-table th, .alert-table td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--border); }
        .alert-table th { color: var(--muted); font-weight: 500; }
        .empty { text-align: center; padding: 80px 20px; color: var(--muted); }
        @media (max-width: 800px) { .grid { grid-template-columns: 1fr; } .card img { width: 100%; } }
      </style>
    </head>
    <body>
      <div class="header">
        <h1>Timeless</h1>
        <span class="stats">#{info.series_count} series &middot; #{format_points(info.total_points)} points &middot; #{format_bytes(info.storage_bytes)}</span>
        #{if firing_count > 0, do: ~s(<span class="fire-count"><span class="badge badge-fire">#{firing_count} firing</span></span>), else: ""}
      </div>
      <div class="controls">
        <select id="range" onchange="applyRange()">
          <option value="-15m"#{if from == "-15m", do: " selected"}>15 min</option>
          <option value="-1h"#{if from == "-1h", do: " selected"}>1 hour</option>
          <option value="-6h"#{if from == "-6h", do: " selected"}>6 hours</option>
          <option value="-24h"#{if from == "-24h", do: " selected"}>24 hours</option>
          <option value="-7d"#{if from == "-7d", do: " selected"}>7 days</option>
          <option value="-30d"#{if from == "-30d", do: " selected"}>30 days</option>
        </select>
        <button onclick="refresh()">Refresh</button>
      </div>
      #{alert_section}
      #{if metrics == [], do: ~s(<div class="empty">No metrics yet. Start writing data to see charts.</div>), else: ~s(<div class="grid">#{chart_cards}</div>)}
      <script>
        function applyRange() {
          const range = document.getElementById('range').value;
          const url = new URL(window.location);
          url.searchParams.set('from', range);
          window.location = url;
        }
        function refresh() {
          document.querySelectorAll('.card img').forEach(img => {
            const url = new URL(img.src, window.location);
            url.searchParams.set('_t', Date.now());
            img.src = url;
          });
        }
        setInterval(refresh, 60000);
      </script>
    </body>
    </html>
    """
  end

  defp escape(str) when is_binary(str) do
    str
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  defp escape(nil), do: ""

  defp format_labels(labels) when is_map(labels) do
    labels
    |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
    |> Enum.join(", ")
    |> escape()
  end

  defp format_labels(_), do: ""

  defp format_value(val) when is_float(val), do: :erlang.float_to_binary(val, decimals: 2)
  defp format_value(val) when is_integer(val), do: Integer.to_string(val)
  defp format_value(nil), do: "—"

  defp format_points(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp format_points(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_points(n), do: "#{n}"

  defp format_bytes(n) when n >= 1_073_741_824, do: "#{Float.round(n / 1_073_741_824, 1)} GB"
  defp format_bytes(n) when n >= 1_048_576, do: "#{Float.round(n / 1_048_576, 1)} MB"
  defp format_bytes(n) when n >= 1024, do: "#{Float.round(n / 1024, 1)} KB"
  defp format_bytes(n), do: "#{n} B"
end
