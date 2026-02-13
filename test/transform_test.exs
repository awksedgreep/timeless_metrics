defmodule TimelessMetrics.TransformTest do
  use ExUnit.Case, async: true

  alias TimelessMetrics.Transform

  # --- parse ---

  test "parse divide:10" do
    assert Transform.parse("divide:10") == {:divide, 10.0}
  end

  test "parse multiply:100" do
    assert Transform.parse("multiply:100") == {:multiply, 100.0}
  end

  test "parse offset:-32" do
    assert Transform.parse("offset:-32") == {:offset, -32.0}
  end

  test "parse scale:1.8:32" do
    assert Transform.parse("scale:1.8:32") == {:scale, 1.8, 32.0}
  end

  test "parse log10" do
    assert Transform.parse("log10") == {:log10}
  end

  test "parse invalid string returns nil" do
    assert Transform.parse("bogus") == nil
    assert Transform.parse("foo:bar:baz:qux") == nil
    assert Transform.parse("") == nil
  end

  test "parse nil returns nil" do
    assert Transform.parse(nil) == nil
  end

  # --- apply ---

  @sample_data [{1000, 100.0}, {2000, 200.0}, {3000, 50.0}]

  test "apply nil is passthrough" do
    assert Transform.apply(@sample_data, nil) == @sample_data
  end

  test "apply divide" do
    result = Transform.apply(@sample_data, {:divide, 10.0})
    assert result == [{1000, 10.0}, {2000, 20.0}, {3000, 5.0}]
  end

  test "apply multiply" do
    result = Transform.apply(@sample_data, {:multiply, 2.0})
    assert result == [{1000, 200.0}, {2000, 400.0}, {3000, 100.0}]
  end

  test "apply offset" do
    result = Transform.apply(@sample_data, {:offset, -50.0})
    assert result == [{1000, 50.0}, {2000, 150.0}, {3000, 0.0}]
  end

  test "apply scale (e.g. Celsius to Fahrenheit)" do
    # F = C * 1.8 + 32
    data = [{1000, 0.0}, {2000, 100.0}, {3000, -40.0}]
    result = Transform.apply(data, {:scale, 1.8, 32.0})

    assert result == [{1000, 32.0}, {2000, 212.0}, {3000, -40.0}]
  end

  test "apply log10" do
    data = [{1000, 100.0}, {2000, 1000.0}, {3000, 1.0}]
    result = Transform.apply(data, {:log10})

    [{_, v1}, {_, v2}, {_, v3}] = result
    assert_in_delta v1, 2.0, 0.0001
    assert_in_delta v2, 3.0, 0.0001
    assert_in_delta v3, 0.0, 0.0001
  end

  test "log10 of zero returns 0.0" do
    data = [{1000, 0.0}]
    [{_, val}] = Transform.apply(data, {:log10})
    assert val == 0.0
  end

  test "log10 of negative returns 0.0" do
    data = [{1000, -5.0}]
    [{_, val}] = Transform.apply(data, {:log10})
    assert val == 0.0
  end

  test "apply empty data returns empty" do
    assert Transform.apply([], {:multiply, 2.0}) == []
    assert Transform.apply([], {:log10}) == []
    assert Transform.apply([], nil) == []
  end
end
