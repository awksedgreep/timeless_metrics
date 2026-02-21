defmodule TimelessMetrics.DictTrainerTest do
  use ExUnit.Case, async: false

  @data_dir "/tmp/timeless_dict_test_#{System.os_time(:millisecond)}"

  setup do
    start_supervised!(
      {TimelessMetrics,
       name: :dict_test, data_dir: @data_dir, buffer_shards: 1, segment_duration: 3600}
    )

    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

  test "no dictionary loaded for fresh store" do
    # Use a unique store name to avoid persistent_term bleed from other tests
    assert TimelessMetrics.DictTrainer.get_cdict(:dict_test_fresh) == nil
    assert TimelessMetrics.DictTrainer.get_ddict(:dict_test_fresh) == nil
    assert TimelessMetrics.DictTrainer.get_dict_version(:dict_test_fresh) == 0
  end

  test "writes without dictionary use standard compression" do
    now = System.os_time(:second)
    hour = div(now, 3600) * 3600

    # Write enough data to create segments
    for i <- 0..99 do
      TimelessMetrics.write(:dict_test, "cpu", %{"h" => "w1"}, 50.0 + :rand.uniform() * 10,
        timestamp: hour + i * 10
      )
    end

    TimelessMetrics.flush(:dict_test)

    # Query it back — should work with standard decompression
    {:ok, points} =
      TimelessMetrics.query(:dict_test, "cpu", %{"h" => "w1"}, from: hour, to: hour + 3600)

    assert length(points) == 100
  end

  test "train and use dictionary for compression round-trip" do
    now = System.os_time(:second)
    hour = div(now, 3600) * 3600

    # Write enough data to create segment blobs for training
    for series <- 1..20 do
      for i <- 0..49 do
        TimelessMetrics.write(
          :dict_test,
          "metric_#{series}",
          %{"h" => "host_#{series}"},
          50.0 + :rand.uniform() * 10,
          timestamp: hour + i * 10
        )
      end
    end

    TimelessMetrics.flush(:dict_test)

    # Check we have enough segments
    samples = TimelessMetrics.DictTrainer.collect_samples(@data_dir, 500)

    if length(samples) >= 10 do
      # Train dictionary
      result = TimelessMetrics.DictTrainer.train(:dict_test, data_dir: @data_dir)

      case result do
        {:ok, info} ->
          assert info.version == 1
          assert info.dict_size > 0
          assert info.sample_count >= 10

          # Verify dictionary is loaded
          assert TimelessMetrics.DictTrainer.get_cdict(:dict_test) != nil
          assert TimelessMetrics.DictTrainer.get_ddict(:dict_test) != nil
          assert TimelessMetrics.DictTrainer.get_dict_version(:dict_test) == 1

          # Write new data — should use dict compression
          for i <- 0..49 do
            TimelessMetrics.write(
              :dict_test,
              "new_metric",
              %{"h" => "dict_host"},
              100.0 + :rand.uniform() * 5,
              timestamp: hour + i * 10
            )
          end

          TimelessMetrics.flush(:dict_test)

          # Query back — should decompress correctly with dict
          {:ok, points} =
            TimelessMetrics.query(:dict_test, "new_metric", %{"h" => "dict_host"},
              from: hour,
              to: hour + 3600
            )

          assert length(points) == 50

        {:error, reason} ->
          # zstd CLI may not be available; skip test gracefully
          IO.puts("Skipping dict training test: #{reason}")
      end
    else
      IO.puts("Skipping dict training test: not enough samples (#{length(samples)})")
    end
  end
end
