defmodule Timeless.SchemaTest do
  use ExUnit.Case, async: true

  defmodule TestSchema do
    use Timeless.Schema

    raw_retention {3, :days}

    tier :hourly,
      resolution: :hour,
      aggregates: [:avg, :min, :max, :count],
      retention: {14, :days}

    tier :daily,
      resolution: :day,
      aggregates: [:avg, :min, :max],
      retention: {90, :days}

    tier :monthly,
      resolution: {30, :days},
      aggregates: [:avg],
      retention: :forever
  end

  test "schema DSL produces correct config" do
    schema = TestSchema.__schema__()

    assert schema.raw_retention_seconds == 3 * 86_400
    assert length(schema.tiers) == 3
  end

  test "tier ordering is preserved" do
    schema = TestSchema.__schema__()
    names = Enum.map(schema.tiers, & &1.name)
    assert names == [:hourly, :daily, :monthly]
  end

  test "tier resolutions are correct" do
    schema = TestSchema.__schema__()
    [hourly, daily, monthly] = schema.tiers

    assert hourly.resolution_seconds == 3_600
    assert daily.resolution_seconds == 86_400
    assert monthly.resolution_seconds == 30 * 86_400
  end

  test "tier retentions are correct" do
    schema = TestSchema.__schema__()
    [hourly, daily, monthly] = schema.tiers

    assert hourly.retention_seconds == 14 * 86_400
    assert daily.retention_seconds == 90 * 86_400
    assert monthly.retention_seconds == :forever
  end

  test "tier table names are generated" do
    schema = TestSchema.__schema__()
    tables = Enum.map(schema.tiers, & &1.table_name)
    assert tables == ["tier_hourly", "tier_daily", "tier_monthly"]
  end

  test "tier aggregates are preserved" do
    schema = TestSchema.__schema__()
    [hourly, daily, monthly] = schema.tiers

    assert hourly.aggregates == [:avg, :min, :max, :count]
    assert daily.aggregates == [:avg, :min, :max]
    assert monthly.aggregates == [:avg]
  end

  test "default schema has sensible values" do
    schema = Timeless.Schema.default()

    assert schema.raw_retention_seconds == 7 * 86_400
    assert length(schema.tiers) == 3
    assert schema.rollup_interval == :timer.minutes(5)
    assert schema.retention_interval == :timer.hours(1)
  end
end
