defmodule TimelessMetrics.Chart do
  @moduledoc """
  Pure-Elixir SVG chart generator for time series data.

  Generates clean, embeddable SVG line charts with no external dependencies.
  Use in `<img>` tags, markdown, emails, notebooks — anywhere that renders images.

  ## Example

      data = [
        %{labels: %{"host" => "web-1"}, data: [{1700000000, 73.2}, {1700000060, 74.1}]},
        %{labels: %{"host" => "web-2"}, data: [{1700000000, 81.0}, {1700000060, 79.5}]}
      ]

      svg = TimelessMetrics.Chart.render("cpu_usage", data, width: 800, height: 300)
  """

  @default_width 800
  @default_height 300
  @padding %{top: 24, right: 10, bottom: 28, left: 45}
  @legend_row_height 16
  @legend_padding 8
  @colors ~w(#2563eb #dc2626 #16a34a #d97706 #7c3aed #0891b2 #be185d #65a30d)

  @themes %{
    light: %{bg: "#ffffff", text: "#374151", grid: "#e5e7eb", muted: "#9ca3af"},
    dark: %{bg: "#1f2937", text: "#e5e7eb", grid: "#374151", muted: "#6b7280"}
  }

  @doc """
  Render a multi-series SVG chart.

  ## Parameters

    * `title` - Chart title (usually the metric name)
    * `series` - List of `%{labels: map, data: [{timestamp, value}, ...]}`
    * `opts` - Options:
      * `:width` - SVG width in pixels (default: 800)
      * `:height` - SVG height in pixels (default: 300)
      * `:label_key` - Label key to use for legend (auto-detected if omitted)
      * `:theme` - `:light`, `:dark`, or `:auto` (default: `:auto`)
        * `:auto` uses CSS `prefers-color-scheme` to switch at render time
      * `:annotations` - List of `%{timestamp: ts, title: "..."}` for vertical markers

  Returns an SVG string.
  """
  def render(title, series, opts \\ []) do
    width = Keyword.get(opts, :width, @default_width)
    height = Keyword.get(opts, :height, @default_height)
    label_key = Keyword.get(opts, :label_key) || detect_label_key(series)
    theme = Keyword.get(opts, :theme, :auto)
    annots = Keyword.get(opts, :annotations, [])
    forecast_data = Keyword.get(opts, :forecast, [])
    anomaly_points = Keyword.get(opts, :anomalies, [])

    has_legend = length(series) > 1
    label_key_val = label_key
    legend_h = if has_legend, do: legend_height(series, label_key_val, width), else: 0
    plot_w = width - @padding.left - @padding.right
    total_height = height + legend_h
    plot_h = height - @padding.top - @padding.bottom

    # Compute global bounds across all series + forecast + anomaly points
    extra_points = forecast_data ++ anomaly_points
    {t_min, t_max, v_min, v_max} = compute_bounds(series, extra_points)

    # Handle edge cases
    if t_min == nil or t_max == nil do
      render_empty(title, width, height, theme)
    else
      # Add 5% padding to value range
      v_range = v_max - v_min
      v_pad = if v_range == 0, do: 1.0, else: v_range * 0.05
      v_min = v_min - v_pad
      v_max = v_max + v_pad

      t_range = max(t_max - t_min, 1)
      v_range = v_max - v_min

      scale_x = fn ts -> @padding.left + (ts - t_min) / t_range * plot_w end
      scale_y = fn val -> @padding.top + plot_h - (val - v_min) / v_range * plot_h end

      lines = render_series(series, scale_x, scale_y, label_key)
      grid = render_grid(t_min, t_max, v_min, v_max, scale_x, scale_y, plot_w, plot_h, theme)
      legend = render_legend(series, label_key, width, height, theme)
      annotation_markers = render_annotations(annots, t_min, t_max, scale_x, plot_h, theme)
      forecast_line = render_forecast(forecast_data, scale_x, scale_y)
      anomaly_dots = render_anomaly_dots(anomaly_points, scale_x, scale_y)

      """
      <svg xmlns="http://www.w3.org/2000/svg" width="#{width}" height="#{total_height}" viewBox="0 0 #{width} #{total_height}" style="font-family:-apple-system,system-ui,sans-serif;font-size:11px">
        #{theme_style(theme)}<rect width="#{width}" height="#{total_height}" #{bg_attr(theme)}/>
        <text x="#{div(width, 2)}" y="16" text-anchor="middle" #{text_attr(theme)} font-size="13" font-weight="600">#{escape(title)}</text>
        #{grid}
        #{lines}
        #{forecast_line}
        #{anomaly_dots}
        #{annotation_markers}
        #{legend}
      </svg>
      """
    end
  end

  # --- Series rendering ---

  defp render_series(series, scale_x, scale_y, _label_key) do
    series
    |> Enum.with_index()
    |> Enum.map(fn {%{data: data}, idx} ->
      color = Enum.at(@colors, rem(idx, length(@colors)))

      points =
        data
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(fn {ts, val} ->
          "#{Float.round(scale_x.(ts), 1)},#{Float.round(scale_y.(val), 1)}"
        end)
        |> Enum.join(" ")

      ~s(<polyline points="#{points}" fill="none" stroke="#{color}" stroke-width="1.5" stroke-linejoin="round"/>)
    end)
    |> Enum.join("\n    ")
  end

  # --- Grid and axes ---

  defp render_grid(t_min, t_max, v_min, v_max, scale_x, scale_y, plot_w, plot_h, theme) do
    ga = grid_attr(theme)
    ta = text_attr(theme)

    # Y-axis grid lines and labels
    y_ticks = nice_ticks(v_min, v_max, 5)

    y_lines =
      Enum.map(y_ticks, fn val ->
        y = Float.round(scale_y.(val), 1)
        label = format_value(val)

        """
          <line x1="#{@padding.left}" y1="#{y}" x2="#{@padding.left + plot_w}" y2="#{y}" #{ga} stroke-width="1"/>
          <text x="#{@padding.left - 4}" y="#{y + 4}" text-anchor="end" #{ta} font-size="10">#{label}</text>
        """
      end)
      |> Enum.join()

    # X-axis time labels
    x_ticks = nice_time_ticks(t_min, t_max, 6)

    x_labels =
      Enum.map(x_ticks, fn ts ->
        x = Float.round(scale_x.(ts), 1)
        label = format_timestamp(ts, t_max - t_min)

        """
          <line x1="#{x}" y1="#{@padding.top}" x2="#{x}" y2="#{@padding.top + plot_h}" #{ga} stroke-width="1" stroke-dasharray="4,4"/>
          <text x="#{x}" y="#{@padding.top + plot_h + 12}" text-anchor="middle" #{ta} font-size="10">#{label}</text>
        """
      end)
      |> Enum.join()

    # Plot border
    border = ~s(<rect x="#{@padding.left}" y="#{@padding.top}" width="#{plot_w}" height="#{plot_h}" #{border_attr(theme)} stroke-width="1"/>)

    border <> y_lines <> x_labels
  end

  # --- Legend ---

  defp render_legend(series, label_key, width, height, theme) do
    if length(series) <= 1 do
      ""
    else
      ta = text_attr(theme)

      items =
        series
        |> Enum.with_index()
        |> Enum.map(fn {%{labels: labels}, idx} ->
          color = Enum.at(@colors, rem(idx, length(@colors)))
          name = Map.get(labels, label_key, "series_#{idx}")
          {color, name}
        end)

      # Lay out legend items with wrapping
      usable_width = width - 20
      rows = layout_legend_rows(items, usable_width)
      legend_y_start = height + @legend_padding

      rows
      |> Enum.with_index()
      |> Enum.flat_map(fn {row_items, row_idx} ->
        row_total = Enum.reduce(row_items, 0, fn {_c, _n, w}, acc -> acc + w end)
        start_x = div(width - row_total, 2)
        y = legend_y_start + row_idx * @legend_row_height

        {svgs, _} =
          Enum.reduce(row_items, {[], start_x}, fn {color, name, _w}, {acc, x} ->
            svg =
              ~s(<rect x="#{x}" y="#{y - 3}" width="12" height="3" fill="#{color}"/>) <>
                ~s(<text x="#{x + 16}" y="#{y}" #{ta} font-size="10">#{escape(name)}</text>)

            item_w = 20 + String.length(name) * 6 + 12
            {[svg | acc], x + item_w}
          end)

        Enum.reverse(svgs)
      end)
      |> Enum.join("\n    ")
    end
  end

  defp layout_legend_rows(items, max_width) do
    items_with_width =
      Enum.map(items, fn {color, name} ->
        w = 20 + String.length(name) * 6 + 12
        {color, name, w}
      end)

    {rows, current_row, _current_width} =
      Enum.reduce(items_with_width, {[], [], 0}, fn {_c, _n, w} = item, {rows, row, row_w} ->
        if row == [] or row_w + w <= max_width do
          {rows, [item | row], row_w + w}
        else
          {[Enum.reverse(row) | rows], [item], w}
        end
      end)

    rows = if current_row != [], do: [Enum.reverse(current_row) | rows], else: rows
    Enum.reverse(rows)
  end

  defp legend_height(series, label_key, width) do
    if length(series) <= 1 do
      0
    else
      items =
        Enum.map(series, fn %{labels: labels} ->
          name = Map.get(labels, label_key, "series")
          {"", name}
        end)

      usable_width = width - 20
      rows = layout_legend_rows(items, usable_width)
      length(rows) * @legend_row_height + @legend_padding * 2
    end
  end

  # --- Annotations ---

  @annotation_color "#f59e0b"

  defp render_annotations([], _t_min, _t_max, _scale_x, _plot_h, _theme), do: ""

  defp render_annotations(annotations, t_min, t_max, scale_x, plot_h, _theme) do
    annotations
    |> Enum.filter(fn %{timestamp: ts} -> ts >= t_min and ts <= t_max end)
    |> Enum.map(fn %{timestamp: ts, title: title} ->
      x = Float.round(scale_x.(ts), 1)
      y_top = @padding.top
      y_bot = @padding.top + plot_h

      """
        <line x1="#{x}" y1="#{y_top}" x2="#{x}" y2="#{y_bot}" stroke="#{@annotation_color}" stroke-width="1" stroke-dasharray="3,3" opacity="0.8"/>
        <text x="#{x + 3}" y="#{y_top + 10}" fill="#{@annotation_color}" font-size="9" font-weight="500">#{escape(title)}</text>
      """
    end)
    |> Enum.join()
  end

  # --- Empty state ---

  defp render_empty(title, width, height, theme) do
    """
    <svg xmlns="http://www.w3.org/2000/svg" width="#{width}" height="#{height}" viewBox="0 0 #{width} #{height}" style="font-family:-apple-system,system-ui,sans-serif">
      #{theme_style(theme)}<rect width="#{width}" height="#{height}" #{bg_attr(theme)}/>
      <text x="#{div(width, 2)}" y="16" text-anchor="middle" #{text_attr(theme)} font-size="13" font-weight="600">#{escape(title)}</text>
      <text x="#{div(width, 2)}" y="#{div(height, 2)}" text-anchor="middle" #{muted_attr(theme)} font-size="12">No data</text>
    </svg>
    """
  end

  # --- Tick computation ---

  defp nice_ticks(min_val, max_val, target_count) do
    range = max_val - min_val

    if range == 0 do
      [min_val]
    else
      # Find a "nice" step size
      raw_step = range / target_count
      magnitude = :math.pow(10, Float.floor(:math.log10(raw_step)))

      nice_step =
        cond do
          raw_step / magnitude <= 1.5 -> magnitude
          raw_step / magnitude <= 3.5 -> magnitude * 2
          raw_step / magnitude <= 7.5 -> magnitude * 5
          true -> magnitude * 10
        end

      first = Float.ceil(min_val / nice_step) * nice_step
      generate_ticks(first, max_val, nice_step, [])
    end
  end

  defp generate_ticks(current, max_val, step, acc) do
    if current > max_val do
      Enum.reverse(acc)
    else
      generate_ticks(current + step, max_val, step, [current | acc])
    end
  end

  defp nice_time_ticks(t_min, t_max, target_count) do
    range = t_max - t_min
    raw_step = range / target_count

    # Snap to nice time intervals
    step =
      cond do
        raw_step <= 30 -> 30
        raw_step <= 60 -> 60
        raw_step <= 300 -> 300
        raw_step <= 600 -> 600
        raw_step <= 1800 -> 1800
        raw_step <= 3600 -> 3600
        raw_step <= 7200 -> 7200
        raw_step <= 21600 -> 21600
        raw_step <= 43200 -> 43200
        raw_step <= 86400 -> 86400
        true -> Float.ceil(raw_step / 86400) * 86400 |> trunc()
      end

    first = (div(t_min, step) + 1) * step
    generate_ticks(first, t_max, step, []) |> Enum.map(&trunc/1)
  end

  # --- Formatting ---

  defp format_value(val) when abs(val) >= 1_000_000, do: "#{Float.round(val / 1_000_000, 1)}M"
  defp format_value(val) when abs(val) >= 1_000, do: "#{Float.round(val / 1_000, 1)}K"

  defp format_value(val) do
    if val == Float.round(val, 0) do
      "#{trunc(val)}"
    else
      "#{Float.round(val, 2)}"
    end
  end

  defp format_timestamp(ts, span) do
    dt = DateTime.from_unix!(ts)

    cond do
      span <= 86400 ->
        "#{pad(dt.hour)}:#{pad(dt.minute)}"

      span <= 604_800 ->
        day = Date.day_of_week(Date.new!(dt.year, dt.month, dt.day))
        name = Enum.at(~w(Mon Tue Wed Thu Fri Sat Sun), day - 1)
        "#{name} #{pad(dt.hour)}:#{pad(dt.minute)}"

      true ->
        "#{dt.month}/#{dt.day}"
    end
  end

  defp pad(n) when n < 10, do: "0#{n}"
  defp pad(n), do: "#{n}"

  # --- Theme ---

  # For :auto, embed a <style> block with prefers-color-scheme and use CSS classes.
  # For :light/:dark, use inline fill/stroke attributes directly (no CSS overhead).
  defp theme_style(:auto) do
    l = @themes.light
    d = @themes.dark

    """
    <style>
        .ms-bg { fill: #{l.bg} }
        .ms-text { fill: #{l.text} }
        .ms-grid { stroke: #{l.grid} }
        .ms-border { stroke: #{l.grid}; fill: none }
        .ms-muted { fill: #{l.muted} }
        @media (prefers-color-scheme: dark) {
          .ms-bg { fill: #{d.bg} }
          .ms-text { fill: #{d.text} }
          .ms-grid { stroke: #{d.grid} }
          .ms-border { stroke: #{d.grid} }
          .ms-muted { fill: #{d.muted} }
        }
      </style>
    """
  end

  defp theme_style(_fixed), do: ""

  # Attribute helpers — return the right SVG attribute string per theme mode
  defp bg_attr(:auto), do: ~s(class="ms-bg")
  defp bg_attr(mode), do: ~s(fill="#{Map.fetch!(@themes, mode).bg}")

  defp text_attr(:auto), do: ~s(class="ms-text")
  defp text_attr(mode), do: ~s(fill="#{Map.fetch!(@themes, mode).text}")

  defp grid_attr(:auto), do: ~s(class="ms-grid")
  defp grid_attr(mode), do: ~s(stroke="#{Map.fetch!(@themes, mode).grid}")

  defp border_attr(:auto), do: ~s(class="ms-border")
  defp border_attr(mode), do: ~s(fill="none" stroke="#{Map.fetch!(@themes, mode).grid}")

  defp muted_attr(:auto), do: ~s(class="ms-muted")
  defp muted_attr(mode), do: ~s(fill="#{Map.fetch!(@themes, mode).muted}")

  # --- Helpers ---

  defp compute_bounds(series, extra_points) do
    series_points = Enum.flat_map(series, fn %{data: data} -> data end)
    all_points = series_points ++ extra_points

    if all_points == [] do
      {nil, nil, nil, nil}
    else
      timestamps = Enum.map(all_points, &elem(&1, 0))
      values = Enum.map(all_points, &elem(&1, 1))
      {Enum.min(timestamps), Enum.max(timestamps), Enum.min(values), Enum.max(values)}
    end
  end

  defp detect_label_key(series) do
    case series do
      [%{labels: labels} | _] ->
        # Pick the label key with the most distinct values
        all_labels = Enum.map(series, & &1.labels)
        keys = labels |> Map.keys()

        keys
        |> Enum.max_by(fn k ->
          all_labels |> Enum.map(&Map.get(&1, k)) |> Enum.uniq() |> length()
        end, fn -> nil end)

      _ ->
        nil
    end
  end

  # --- Forecast overlay ---

  @forecast_color "#8b5cf6"

  defp render_forecast([], _scale_x, _scale_y), do: ""

  defp render_forecast(forecast_data, scale_x, scale_y) do
    points =
      forecast_data
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {ts, val} ->
        "#{Float.round(scale_x.(ts * 1.0), 1)},#{Float.round(scale_y.(val * 1.0), 1)}"
      end)
      |> Enum.join(" ")

    ~s(<polyline points="#{points}" fill="none" stroke="#{@forecast_color}" stroke-width="2" stroke-dasharray="6,3" opacity="0.8"/>)
  end

  # --- Anomaly dots ---

  defp render_anomaly_dots([], _scale_x, _scale_y), do: ""

  defp render_anomaly_dots(anomaly_points, scale_x, scale_y) do
    anomaly_points
    |> Enum.map(fn {ts, val} ->
      x = Float.round(scale_x.(ts * 1.0), 1)
      y = Float.round(scale_y.(val * 1.0), 1)

      ~s(<circle cx="#{x}" cy="#{y}" r="4" fill="#ef4444" stroke="#ffffff" stroke-width="1.5" opacity="0.9"/>)
    end)
    |> Enum.join("\n    ")
  end

  defp escape(str) do
    str
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end
end
