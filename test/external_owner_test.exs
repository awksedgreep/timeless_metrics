defmodule TimelessMetrics.ExternalOwnerTest do
  use ExUnit.Case, async: true

  test "external ownership starts no store or Rocket child and does not touch data" do
    root =
      Path.join(
        System.tmp_dir!(),
        "timeless_metrics_external_#{System.unique_integer([:positive])}"
      )

    assert [] =
             TimelessMetrics.Application.configured_children(
               owner: :external,
               data_dir: root,
               engine: :rust,
               port: 8428
             )

    refute File.exists?(root)
  end

  test "unknown ownership fails explicitly" do
    assert_raise ArgumentError, ~r/expected :embedded or :external/, fn ->
      TimelessMetrics.Application.configured_children(owner: :automatic)
    end
  end
end
