defmodule TimelessMetrics.LibsqlEngineTest do
  use ExUnit.Case, async: false

  alias TimelessMetrics.TestHelper

  defmodule MaintenanceSchema do
    use TimelessMetrics.Schema

    raw_retention(:forever)
    rollup_interval({1, :minutes})
    retention_interval({2, :minutes})
  end

  @store :libsql_engine_test

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "timeless_libsql_test_#{System.unique_integer([:positive])}")

    start_store(data_dir)
    on_exit(fn -> File.rm_rf!(data_dir) end)
    {:ok, data_dir: data_dir}
  end

  test "capability preflight raises loudly without the extension surface" do
    # A connection that never loaded the extension stands in for a
    # pre-handshake (< 0.4.0) timeless-libsql: the writer must refuse it
    # with an error naming the requirement, never drive it silently.
    {:ok, conn} = Exqlite.Sqlite3.open(":memory:")

    try do
      assert_raise RuntimeError, ~r/capability preflight failed.*resolved-v1/s, fn ->
        TimelessMetrics.LibsqlEngine.verify_capabilities!(conn)
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  test "named and resolved batches share the matcher-aware raw waist" do
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"cpu", %{"host" => "web-1", "env" => "prod"}, 1.0, 10},
               {"cpu", %{"host" => "web-2", "env" => "dev"}, 2.0, 10},
               {"cpu", %{"host" => "db-1"}, 3.0, 10}
             ])

    assert {:ok, sid} =
             TimelessMetrics.resolve_series(@store, "cpu", %{
               "host" => "web-1",
               "env" => "prod"
             })

    assert :ok = TimelessMetrics.write_resolved(@store, sid, 4.0, timestamp: 20)

    assert {:ok, [{10, 1.0}, {20, 4.0}]} =
             TimelessMetrics.query(
               @store,
               "cpu",
               %{"host" => "web-1", "env" => "prod"},
               from: 0,
               to: 30
             )

    assert {:ok, matched} =
             TimelessMetrics.query_multi(
               @store,
               "cpu",
               %{"host" => {:regex, "web-.*"}, "env" => {:not_equal, "dev"}},
               from: 0,
               to: 30
             )

    assert [%{labels: %{"host" => "web-1", "env" => "prod"}}] = matched
    assert {:ok, ["cpu"]} = TimelessMetrics.list_metrics(@store)

    assert {:ok, ["db-1", "web-1", "web-2"]} =
             TimelessMetrics.label_values(@store, "cpu", "host")
  end

  test "matcher and discovery pushdown stays differential with Elixir semantics" do
    labels = [
      %{"host" => "web-1", "env" => "prod", "code" => "a"},
      %{"host" => "web-2", "env" => "dev", "code" => "é"},
      %{"host" => "db-1"},
      %{"host" => "empty", "env" => ""}
    ]

    assert :ok =
             TimelessMetrics.write_batch(
               @store,
               Enum.with_index(labels, fn series_labels, index ->
                 {"matcher", series_labels, index + 1.0, 10}
               end)
             )

    assert :ok = TimelessMetrics.flush(@store)

    filters = [
      [{"host", "web-1"}],
      [{"env", ""}],
      [{"env", {:not_equal, "prod"}}],
      [{"env", {:not_equal, ""}}],
      [{"host", {:regex, "web-.*"}}],
      [{"rack", {:regex, ""}}],
      [{"env", {:not_regex, ".+"}}],
      [{"host", "web-1"}, {"host", "web-2"}],
      [{"host", {:regex, "web-.*"}}, {"host", {:not_equal, "web-2"}}],
      # PCRE-only lookahead and byte-counting dot stay above the boundary.
      [{"host", {:regex, "(?=web).*1"}}],
      [{"code", {:regex, "."}}],
      [{"host", {:regex, "["}}],
      [{"host", {:not_regex, "["}}]
    ]

    Enum.each(filters, fn filter ->
      compiled = TimelessMetrics.LabelMatch.compile(filter)

      expected =
        labels
        |> Enum.filter(&TimelessMetrics.LabelMatch.match?(&1, compiled))
        |> Enum.sort()

      assert {:ok, queried} =
               TimelessMetrics.query_multi(@store, "matcher", filter, from: 0, to: 20)

      assert queried |> Enum.map(& &1.labels) |> Enum.sort() == expected

      assert {:ok, discovered} =
               TimelessMetrics.StorageEngine.find_series(@store, "matcher", filter)

      assert Enum.sort(discovered) == expected
    end)

    selective = [{"host", {:regex, "web-.*"}}, {"env", {:not_equal, "dev"}}]
    expected = [%{"host" => "web-1", "env" => "prod", "code" => "a"}]

    assert {:ok, aggregate} =
             TimelessMetrics.query_aggregate_multi(@store, "matcher", selective,
               from: 0,
               to: 20,
               aggregate: :avg
             )

    assert Enum.map(aggregate, & &1.labels) == expected

    assert {:ok, bucketed} =
             TimelessMetrics.query_aggregate_multi(@store, "matcher", selective,
               from: 10,
               to: 19,
               bucket: {10, :seconds},
               aggregate: :avg
             )

    assert Enum.map(bucketed, & &1.labels) == expected

    assert {:ok, latest} = TimelessMetrics.latest_multi(@store, "matcher", selective)
    assert Enum.map(latest, & &1.labels) == expected
  end

  test "flush persists into metrics.db and recovers after restart", %{data_dir: data_dir} do
    assert :ok =
             TimelessMetrics.write(@store, "requests", %{"service" => "api"}, 7, timestamp: 100)

    assert :ok = TimelessMetrics.flush(@store)
    assert File.exists?(Path.join(data_dir, "metrics.db"))
    refute File.exists?(Path.join(data_dir, "rust_engine"))

    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")
    start_store(data_dir)

    assert {:ok, [{100, 7.0}]} =
             TimelessMetrics.query(@store, "requests", %{"service" => "api"},
               from: 0,
               to: 200
             )

    info = TimelessMetrics.info(@store)
    assert info.series_count == 1
    assert info.total_points == 1
    assert info.block_count == 1
  end

  test "native scalar aggregates preserve filters, transforms, fallbacks, and restart", %{
    data_dir: data_dir
  } do
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"aggregate", %{"host" => "web-1", "env" => "prod"}, 1.0, -10},
               {"aggregate", %{"host" => "web-1", "env" => "prod"}, 3.0, 0},
               {"aggregate", %{"host" => "web-1", "env" => "prod"}, 5.0, 10},
               {"aggregate", %{"host" => "web-2", "env" => "dev"}, -4.0, 0},
               {"aggregate", %{"host" => "web-2", "env" => "dev"}, 6.0, 20},
               {"aggregate", %{"host" => "web-1", "env" => "prod", "rack" => "r1"}, 100.0, 10}
             ])

    assert :ok = TimelessMetrics.flush(@store)

    assert :ok =
             TimelessMetrics.write(
               @store,
               "aggregate",
               %{"host" => "web-1", "env" => "prod"},
               7.0,
               timestamp: 30
             )

    expectations = %{
      avg: %{"web-1" => 4.0, "web-2" => 1.0},
      sum: %{"web-1" => 16.0, "web-2" => 2.0},
      min: %{"web-1" => 1.0, "web-2" => -4.0},
      max: %{"web-1" => 7.0, "web-2" => 6.0},
      count: %{"web-1" => 4, "web-2" => 2}
    }

    Enum.each(expectations, fn {aggregate, expected} ->
      assert {:ok, results} =
               TimelessMetrics.query_aggregate_multi(
                 @store,
                 "aggregate",
                 %{"rack" => {:not_equal, "r1"}},
                 from: -10,
                 to: 30,
                 aggregate: aggregate
               )

      assert Map.new(results, fn %{labels: labels, data: [{-10, value}]} ->
               {labels["host"], value}
             end) == expected
    end)

    assert {:ok, [%{labels: %{"host" => "web-1", "env" => "prod"}, data: [{-10, 8.0}]}]} =
             TimelessMetrics.query_aggregate_multi(
               @store,
               "aggregate",
               %{"host" => {:regex, "web-1"}, "rack" => {:not_equal, "r1"}},
               from: -10,
               to: 30,
               aggregate: :avg,
               transform: {:multiply, 2}
             )

    assert {:ok, [{-10, 4.0}]} =
             TimelessMetrics.query_aggregate(
               @store,
               "aggregate",
               %{"host" => "web-1", "env" => "prod"},
               from: -10,
               to: 30,
               aggregate: :avg
             )

    # Unsupported scalar and bucketed shapes retain the raw fallback.
    assert {:ok, [{-10, 7.0}]} =
             TimelessMetrics.query_aggregate(
               @store,
               "aggregate",
               %{"host" => "web-1", "env" => "prod"},
               from: -10,
               to: 30,
               aggregate: :last
             )

    assert {:ok, bucketed} =
             TimelessMetrics.query_aggregate(
               @store,
               "aggregate",
               %{"host" => "web-1", "env" => "prod"},
               from: -10,
               to: 30,
               bucket: {20, :seconds},
               aggregate: :sum
             )

    assert bucketed == [{-10, 4.0}, {10, 5.0}, {30, 7.0}]

    assert :ok = TimelessMetrics.flush(@store)
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")
    start_store(data_dir)

    assert {:ok, [%{data: [{-10, 16.0}]}]} =
             TimelessMetrics.query_aggregate_multi(
               @store,
               "aggregate",
               %{"host" => "web-1", "rack" => {:not_equal, "r1"}},
               from: -10,
               to: 30,
               aggregate: :sum
             )
  end

  test "native bucket kernels match the raw oracle at the compatibility boundary" do
    labels_a = %{"env" => "prod", "host" => "a"}
    labels_b = %{"env" => "dev", "host" => "b"}

    assert {:ok, _empty_sid} =
             TimelessMetrics.resolve_series(@store, "bucketed", %{
               "env" => "prod",
               "host" => "empty"
             })

    # Deliberately out of timestamp order, with exact edges and a duplicate.
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"bucketed", labels_a, 7.0, 10},
               {"bucketed", labels_a, 1.0, -10},
               {"bucketed", labels_a, -2.0, 9},
               {"bucketed", labels_a, 3.0, -1},
               {"bucketed", labels_a, 5.0, 10},
               {"bucketed", labels_b, 4.0, 0},
               {"bucketed", %{"env" => "skip", "host" => "c"}, 1_000.0, 0}
             ])

    assert :ok = TimelessMetrics.flush(@store)

    # Keep the terminal points buffered so the native kernel is also checked
    # against read-your-writes state, not only persisted blocks.
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"bucketed", labels_a, 9.0, 29},
               {"bucketed", labels_b, -4.0, 29}
             ])

    filter = %{"env" => {:not_equal, "skip"}}

    assert {:ok, raw_series} =
             TimelessMetrics.query_multi(@store, "bucketed", filter, from: -10, to: 29)

    Enum.each([:avg, :sum, :min, :max, :count], fn aggregate ->
      expected = oracle_bucket_map(raw_series, -10, 20, aggregate)

      assert {:ok, actual} =
               TimelessMetrics.query_aggregate_multi(
                 @store,
                 "bucketed",
                 filter,
                 from: -10,
                 to: 29,
                 bucket: {20, :seconds},
                 aggregate: aggregate
               )

      assert series_data_map(actual) == expected

      if aggregate == :count do
        assert Enum.all?(actual, fn %{data: data} ->
                 Enum.all?(data, fn {_timestamp, value} -> is_integer(value) end)
               end)
      end
    end)

    assert {:ok, [%{labels: ^labels_a, data: exact_data}]} =
             TimelessMetrics.query_aggregate_multi(
               @store,
               "bucketed",
               labels_a,
               from: -10,
               to: 29,
               bucket: {20, :seconds},
               aggregate: :sum,
               transform: {:scale, 2, 1}
             )

    assert exact_data == [{-10, 5.0}, {10, 43.0}]

    assert {:ok, [{-10, 2.0}, {10, 21.0}]} =
             TimelessMetrics.query_aggregate(
               @store,
               "bucketed",
               labels_a,
               from: -10,
               to: 29,
               bucket: {20, :seconds},
               aggregate: :sum
             )

    # A partial terminal bucket is part of the public contract. It cannot be
    # represented by the full-width native windows and therefore stays raw.
    assert :ok = TimelessMetrics.write(@store, "bucketed", labels_a, 11.0, timestamp: 30)

    assert {:ok, [{-10, 2.0}, {10, 21.0}, {30, 11.0}]} =
             TimelessMetrics.query_aggregate(
               @store,
               "bucketed",
               labels_a,
               from: -10,
               to: 30,
               bucket: {20, :seconds},
               aggregate: :sum
             )

    # Timeless rate uses the last point from the preceding bucket as carry-in;
    # timeless_window rate is independently window-local. Keep this shape raw.
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"counter", %{"host" => "a"}, 0.0, 0},
               {"counter", %{"host" => "a"}, 20.0, 20}
             ])

    assert {:ok, [{20, 1.0}]} =
             TimelessMetrics.query_aggregate(
               @store,
               "counter",
               %{"host" => "a"},
               from: 0,
               to: 39,
               bucket: {20, :seconds},
               aggregate: :rate
             )
  end

  test "native latest preserves filters, duplicate ties, omission, and restart", %{
    data_dir: data_dir
  } do
    assert {:ok, _empty_sid} =
             TimelessMetrics.resolve_series(@store, "latest", %{"host" => "empty"})

    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"latest", %{"host" => "a", "env" => "prod"}, 1.0, 10},
               {"latest", %{"host" => "a", "env" => "prod"}, 3.0, 30},
               {"latest", %{"host" => "b", "env" => "dev"}, 2.0, 20}
             ])

    assert {:ok, {30, 3.0}} =
             TimelessMetrics.latest(@store, "latest", %{"host" => "a", "env" => "prod"})

    assert {:ok, nil} = TimelessMetrics.latest(@store, "latest", %{"host" => "empty"})

    # `latest/3` is an exact-series API. A partial selector resolving more
    # than one non-empty series follows the Rust engine contract and returns
    # nil; callers wanting fan-out use latest_multi/3.
    assert {:ok, nil} = TimelessMetrics.latest(@store, "latest", %{})
    assert :ok = TimelessMetrics.flush(@store)

    # The second chunk sorts first by min timestamp, so its ts=30 duplicate
    # wins the same stable tie as the raw engine range.
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"latest", %{"host" => "a", "env" => "prod"}, 4.0, 30},
               {"latest", %{"host" => "a", "env" => "prod"}, 0.5, 5}
             ])

    assert :ok = TimelessMetrics.flush(@store)

    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"latest", %{"host" => "a", "env" => "prod"}, 5.0, 30},
               {"latest", %{"host" => "a", "env" => "prod"}, 6.0, 40}
             ])

    assert {:ok, {40, 6.0}} =
             TimelessMetrics.latest(@store, "latest", %{"host" => "a", "env" => "prod"})

    assert {:ok, [%{labels: %{"host" => "b", "env" => "dev"}, timestamp: 20, value: 2.0}]} =
             TimelessMetrics.latest_multi(@store, "latest", %{
               "env" => {:not_equal, "prod"}
             })

    assert {:ok, [%{labels: %{"host" => "a", "env" => "prod"}, timestamp: 40, value: 6.0}]} =
             TimelessMetrics.latest_multi(@store, "latest", %{
               "host" => {:regex, "a"},
               "env" => "prod"
             })

    assert :ok = TimelessMetrics.flush(@store)
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")
    start_store(data_dir)

    assert {:ok, latest} = TimelessMetrics.latest_multi(@store, "latest")

    assert Map.new(latest, fn %{labels: labels, timestamp: timestamp, value: value} ->
             {labels["host"], {timestamp, value}}
           end) == %{"a" => {40, 6.0}, "b" => {20, 2.0}}
  end

  test "backup is a self-contained metrics.db", %{data_dir: data_dir} do
    backup_dir = data_dir <> "_backup"
    on_exit(fn -> File.rm_rf!(backup_dir) end)

    assert :ok = TimelessMetrics.write(@store, "up", %{}, 1, timestamp: 10)
    assert {:ok, result} = TimelessMetrics.backup(@store, backup_dir)
    assert result.files == ["metrics.db"]
    assert result.total_bytes > 0

    conn = TimelessMetrics.LibsqlEngine.open_connection(Path.join(backup_dir, "metrics.db"))

    assert {:ok, [[1]]} =
             TimelessMetrics.DB.execute(conn, "SELECT COUNT(*) FROM metric_samples", [])

    Exqlite.Sqlite3.close(conn)
  end

  test "packed rollup reads survive restart and retention uses libSQL blocks", %{
    data_dir: data_dir
  } do
    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"temperature", %{"room" => "lab"}, 1.0, 1_000},
               {"temperature", %{"room" => "lab"}, 3.0, 2_000},
               {"temperature", %{"room" => "lab"}, 9.0, 200_000}
             ])

    assert :ok = TimelessMetrics.flush(@store)
    assert :ok = TimelessMetrics.rollup(@store)

    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")
    start_store(data_dir)

    assert {:ok,
            [
              %{
                bucket: 0,
                avg: 2.0,
                min: 1.0,
                max: 3.0,
                count: 2,
                sum: 4.0,
                last: 3.0
              }
            ]} =
             TimelessMetrics.query_daily(
               @store,
               "temperature",
               %{"room" => "lab"},
               0,
               86_399
             )

    now = System.os_time(:second)
    old = now - 8 * 86_400

    assert :ok = TimelessMetrics.write(@store, "retained", %{}, 1.0, timestamp: old)
    assert :ok = TimelessMetrics.flush(@store)
    assert :ok = TimelessMetrics.write(@store, "retained", %{}, 2.0, timestamp: now)
    assert :ok = TimelessMetrics.flush(@store)
    assert :ok = TimelessMetrics.enforce_retention(@store)

    assert {:ok, [{^now, 2.0}]} =
             TimelessMetrics.query(@store, "retained", %{}, from: 0, to: now)
  end

  test "rollup batch decoder keeps integer counts exact and rejects bad versions and lengths" do
    count = 9_007_199_254_740_999

    blob =
      <<"TRB1", 1::unsigned-little-32, -300::signed-little-64, count::unsigned-little-64,
        1.5::float-little-64, -0.0::float-little-64, -2.0::float-little-64, 4.0::float-little-64,
        -1::signed-little-64, 3.0::float-little-64>>

    assert [bucket] = TimelessMetrics.LibsqlEngine.decode_rollup_batch(blob)
    assert bucket.bucket == -300
    assert bucket.count == count
    assert bucket.avg == 1.5
    assert <<bucket.sum::float-little-64>> == <<-0.0::float-little-64>>
    assert bucket.min == -2.0
    assert bucket.max == 4.0
    assert bucket.last == 3.0

    assert_raise RuntimeError, ~r/unknown or truncated/, fn ->
      TimelessMetrics.LibsqlEngine.decode_rollup_batch(<<"TRB2", 0::unsigned-little-32>>)
    end

    assert_raise RuntimeError, ~r/malformed/, fn ->
      TimelessMetrics.LibsqlEngine.decode_rollup_batch(<<"TRB1", 1::unsigned-little-32>>)
    end

    nan = 0x7FF8_0000_0000_0042

    nonfinite_blob =
      <<"TRB1", 1::unsigned-little-32, 0::signed-little-64, 1::unsigned-little-64,
        nan::unsigned-little-64, nan::unsigned-little-64, nan::unsigned-little-64,
        nan::unsigned-little-64, 0::signed-little-64, nan::unsigned-little-64>>

    assert [%{avg: nil, sum: nil, min: nil, max: nil, last: nil}] =
             TimelessMetrics.LibsqlEngine.decode_rollup_batch(nonfinite_blob)
  end

  test "native raw batch decoder preserves series and point order" do
    first =
      <<2::unsigned-little-32, -1::signed-little-64, 5::signed-little-64, 1.25::float-little-64,
        -2.5::float-little-64>>

    second = <<1::unsigned-little-32, 9::signed-little-64, 3.5::float-little-64>>

    assert {:ok, [{7, [{-1, 1.25}, {5, -2.5}]}, {8, [{9, 3.5}]}]} =
             TimelessMetrics.RustEngine.Nif.decode_raw_batches([{7, first}, {8, second}])
  end

  test "native raw frame decoder validates the envelope and preserves series and point order" do
    frame =
      <<"TRF1", 2::unsigned-little-32, 3::unsigned-little-64, 7::signed-little-64,
        8::signed-little-64, 2::unsigned-little-32, 1::unsigned-little-32, -1::signed-little-64,
        5::signed-little-64, 9::signed-little-64, 1.25::float-little-64, -2.5::float-little-64,
        3.5::float-little-64>>

    assert {:ok, [{7, [{-1, 1.25}, {5, -2.5}]}, {8, [{9, 3.5}]}]} =
             TimelessMetrics.RustEngine.Nif.decode_raw_frame(frame)

    assert {:ok,
            [
              %{labels: %{"host" => "a"}, points: [{-1, 1.25}, {5, -2.5}]},
              %{labels: %{"host" => "b"}, points: [{9, 3.5}]}
            ]} =
             TimelessMetrics.RustEngine.Nif.decode_raw_frame_series(frame, [
               %{"host" => "a"},
               %{"host" => "b"}
             ])

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_raw_frame_series(frame, [%{"host" => "a"}])

    assert message =~ "2 series but received 1 label maps"

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_raw_frame(
               <<"TRF2", 0::unsigned-little-32, 0::unsigned-little-64>>
             )

    assert message =~ "unknown frame version"

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_raw_frame(
               <<"TRF1", 1::unsigned-little-32, 1::unsigned-little-64, 7::signed-little-64,
                 0::unsigned-little-32, 9::signed-little-64, 3.5::float-little-64>>
             )

    assert message =~ "point counts sum"
  end

  test "native aggregate and latest frame decoders preserve types and reject malformed frames" do
    aggregate_frame =
      <<"TAF1", 0, 0, 0::unsigned-little-16, 3::unsigned-little-32, 7::signed-little-64,
        8::signed-little-64, 9::signed-little-64, 0b101, 1.25::float-little-64,
        0::unsigned-little-64, -2.5::float-little-64>>

    assert {:ok, {:avg, [{7, 1.25}, {8, nil}, {9, -2.5}]}} =
             TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(aggregate_frame)

    count_frame =
      <<"TAF1", 4, 0, 0::unsigned-little-16, 2::unsigned-little-32, 7::signed-little-64,
        8::signed-little-64, 0b11, 3::unsigned-little-64, 5::unsigned-little-64>>

    assert {:ok, {:count, [{7, 3}, {8, 5}]}} =
             TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(count_frame)

    latest_frame =
      <<"TLF1", 2::unsigned-little-32, 7::signed-little-64, 8::signed-little-64,
        10::signed-little-64, 20::signed-little-64, 0b01, 1.25::float-little-64,
        0::unsigned-little-64>>

    assert {:ok, [{7, 10, 1.25}, {8, 20, nil}]} =
             TimelessMetrics.RustEngine.Nif.decode_latest_frame(latest_frame)

    assert {:error, "TAF1: unknown magic/version"} =
             TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(
               "TAF2" <> binary_part(aggregate_frame, 4, byte_size(aggregate_frame) - 4)
             )

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(
               <<"TAF1", 0, 0, 0::unsigned-little-16, 1::unsigned-little-32, 7::signed-little-64,
                 0b10, 0::unsigned-little-64>>
             )

    assert message =~ "bitmap padding"

    nan_bits = 0x7FF8_0000_0000_0042

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_aggregate_frame(
               <<"TAF1", 0, 0, 0::unsigned-little-16, 1::unsigned-little-32, 7::signed-little-64,
                 0b1, nan_bits::unsigned-little-64>>
             )

    assert message =~ "must not be NaN"

    assert {:error, message} =
             TimelessMetrics.RustEngine.Nif.decode_latest_frame(
               <<"TLF1", 1::unsigned-little-32, 7::signed-little-64, 10::signed-little-64, 0b0,
                 1::unsigned-little-64>>
             )

    assert message =~ "nonzero word"
  end

  test "packed aggregate/latest adoption retains row fallbacks and exact series-id reads" do
    exact_labels = %{"env" => "prod", "host" => "a"}

    assert :ok =
             TimelessMetrics.write_batch(@store, [
               {"frames", exact_labels, 1.0, 10},
               {"frames", exact_labels, 3.0, 20},
               {"frames", Map.put(exact_labels, "rack", "r1"), 100.0, 30},
               {"frames", %{"env" => "prod", "host" => "b"}, 5.0, 15}
             ])

    assert TimelessMetrics.LibsqlEngine.query_frame_features(@store) ==
             MapSet.new(["timeless_aggregate_frame", "timeless_latest_frame"])

    # A cached exact selector must not also read a series whose labels are a
    # strict superset. These calls exercise all three selected-ID statements.
    assert {:ok, [{10, 1.0}, {20, 3.0}]} =
             TimelessMetrics.query(@store, "frames", exact_labels, from: 0, to: 40)

    assert {:ok, [{0, 2.0}]} =
             TimelessMetrics.LibsqlEngine.query_aggregate(
               @store,
               "frames",
               exact_labels,
               from: 0,
               to: 40,
               aggregate: :avg
             )

    assert {:ok, {20, 3.0}} = TimelessMetrics.latest(@store, "frames", exact_labels)

    filter = %{"env" => "prod"}

    assert {:ok, framed_aggregate} =
             TimelessMetrics.query_aggregate_multi(@store, "frames", filter,
               from: 0,
               to: 40,
               aggregate: :avg
             )

    assert {:ok, framed_latest} = TimelessMetrics.latest_multi(@store, "frames", filter)

    feature_key = {TimelessMetrics.LibsqlEngine, @store, :query_frame_features}
    features = TimelessMetrics.LibsqlEngine.query_frame_features(@store)

    try do
      :persistent_term.put(feature_key, MapSet.new())

      assert {:ok, row_aggregate} =
               TimelessMetrics.query_aggregate_multi(@store, "frames", filter,
                 from: 0,
                 to: 40,
                 aggregate: :avg
               )

      assert {:ok, row_latest} = TimelessMetrics.latest_multi(@store, "frames", filter)
      assert sort_series(framed_aggregate) == sort_series(row_aggregate)
      assert sort_series(framed_latest) == sort_series(row_latest)
    after
      :persistent_term.put(feature_key, features)
    end
  end

  test "writer crash preserves buffered work: the engine outlives the connection" do
    assert :ok = TimelessMetrics.write(@store, "crash", %{}, 1.0, timestamp: 10)
    assert :ok = TimelessMetrics.flush(@store)
    assert :ok = TimelessMetrics.write(@store, "crash", %{}, 2.0, timestamp: 20)
    Process.sleep(20)

    old_writer = Process.whereis(TimelessMetrics.LibsqlEngine.writer_name(@store))
    monitor = Process.monitor(old_writer)
    Process.exit(old_writer, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^old_writer, :killed}, 5_000

    new_writer = await_restarted_writer(old_writer)
    assert is_pid(new_writer)

    assert :ok = TimelessMetrics.write(@store, "crash", %{}, 3.0, timestamp: 30)
    assert :ok = TimelessMetrics.flush(@store)

    # Since extension 0.6.1 (P1 per-connection engine pins) the engine —
    # buffer included — lives in the extension's process-global registry, so
    # a killed writer's replacement reattaches to it and the buffered point
    # survives. Before that, the buffer died with the connection and ts=20
    # was the crash casualty this test asserted. The loss boundary is now
    # OS-process death, which an in-process test cannot simulate.
    assert {:ok, [{10, 1.0}, {20, 2.0}, {30, 3.0}]} =
             TimelessMetrics.query(@store, "crash", %{}, from: 0, to: 40)
  end

  test "pooled readers retry transaction-private chunk visibility conflicts", %{
    data_dir: data_dir
  } do
    assert :ok = TimelessMetrics.write(@store, "read_gate", %{"host" => "a"}, 1.0, timestamp: 10)
    assert :ok = TimelessMetrics.flush(@store)

    external =
      TimelessMetrics.LibsqlEngine.open_connection(Path.join(data_dir, "metrics.db"))

    try do
      assert {:ok, []} = TimelessMetrics.DB.execute(external, "BEGIN IMMEDIATE", [])

      assert {:ok, []} =
               TimelessMetrics.DB.execute(
                 external,
                 "INSERT INTO metric_samples(name, labels, ts, value) VALUES (?1, ?2, ?3, ?4)",
                 ["read_gate", ~s({"host":"a"}), 20, 2.0]
               )

      assert {:ok, []} =
               TimelessMetrics.DB.execute(
                 external,
                 "INSERT INTO metric_samples(metric_samples) VALUES ('flush')",
                 []
               )

      query =
        Task.async(fn ->
          TimelessMetrics.query(@store, "read_gate", %{"host" => "a"}, from: 0, to: 30)
        end)

      assert Task.yield(query, 50) == nil
      assert {:ok, []} = TimelessMetrics.DB.execute(external, "COMMIT", [])

      assert {:ok, [{10, 1.0}, {20, 2.0}]} = Task.await(query, 5_000)
    after
      Exqlite.Sqlite3.close(external)
    end
  end

  test "maintenance timers come from the configured schema", %{data_dir: data_dir} do
    stop_supervised!({TimelessMetrics, @store})
    TestHelper.await_down(:"#{@store}_sup")

    schema = MaintenanceSchema.__schema__()

    start_store(data_dir, schema: MaintenanceSchema)

    state = :sys.get_state(TimelessMetrics.LibsqlEngine.writer_name(@store))
    rollup_remaining = Process.read_timer(state.rollup_timer)
    retention_remaining = Process.read_timer(state.retention_timer)

    assert state.schema == schema
    assert rollup_remaining in 55_000..60_000
    assert retention_remaining in 115_000..120_000
  end

  defp start_store(data_dir, extra_opts \\ []) do
    opts =
      [
        name: @store,
        engine: :libsql,
        data_dir: data_dir,
        scraping: false,
        self_monitor: false,
        reader_pool_size: 2
      ]
      |> Keyword.merge(extra_opts)

    start_supervised!({TimelessMetrics, opts})
  end

  defp oracle_bucket_map(series, from, step, aggregate) do
    Map.new(series, fn %{labels: labels, points: points} ->
      data =
        points
        |> Enum.group_by(fn {timestamp, _value} ->
          from + div(timestamp - from, step) * step
        end)
        |> Enum.map(fn {bucket, bucket_points} ->
          values = Enum.map(bucket_points, &elem(&1, 1))

          {bucket,
           TimelessMetrics.Aggregation.compute_aggregate(aggregate, values, bucket_points)}
        end)
        |> Enum.sort_by(&elem(&1, 0))

      {labels, data}
    end)
  end

  defp series_data_map(series) do
    Map.new(series, fn %{labels: labels, data: data} -> {labels, data} end)
  end

  defp sort_series(series), do: Enum.sort_by(series, & &1.labels)

  defp await_restarted_writer(old_writer, attempts \\ 100)

  defp await_restarted_writer(_old_writer, 0), do: nil

  defp await_restarted_writer(old_writer, attempts) do
    case Process.whereis(TimelessMetrics.LibsqlEngine.writer_name(@store)) do
      pid when is_pid(pid) and pid != old_writer ->
        pid

      _ ->
        Process.sleep(10)
        await_restarted_writer(old_writer, attempts - 1)
    end
  end
end
