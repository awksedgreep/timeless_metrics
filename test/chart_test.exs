defmodule TimelessMetrics.ChartTest do
  use ExUnit.Case, async: true

  test "render honors explicit x_domain for sparse series" do
    svg =
      TimelessMetrics.Chart.render(
        "sparse",
        [
          %{
            labels: %{"host" => "web-1"},
            data: [{1_000, 10.0}, {1_600, 20.0}]
          }
        ],
        width: 800,
        height: 300,
        x_domain: {0, 86_400}
      )

    [points] = Regex.run(~r/<polyline points="([^"]+)"/, svg, capture: :all_but_first)
    xs = extract_xs(points)

    assert Enum.max(xs) < 70.0
  end

  defp extract_xs(points) do
    points
    |> String.split(" ", trim: true)
    |> Enum.map(fn point ->
      [x, _y] = String.split(point, ",")
      String.to_float(x)
    end)
  end
end
