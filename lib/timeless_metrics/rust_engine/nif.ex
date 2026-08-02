defmodule TimelessMetrics.RustEngine.Nif do
  @moduledoc false
  use Rustler, otp_app: :timeless_metrics, crate: "tms_engine", skip_compilation?: true

  def engine_new(
        _data_dir,
        _flush_threshold,
        _min_flush_size,
        _compression_level,
        _memory_budget_mb,
        _defer_compression
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  def engine_compact(_engine, _cutoff_ts), do: :erlang.nif_error(:nif_not_loaded)

  def engine_write_batch_labeled(_engine, _entries), do: :erlang.nif_error(:nif_not_loaded)
  def engine_write_batch_raw(_engine, _binary), do: :erlang.nif_error(:nif_not_loaded)
  def engine_resolve_series(_engine, _metric, _labels), do: :erlang.nif_error(:nif_not_loaded)
  def engine_resolve_series_batch(_engine, _entries), do: :erlang.nif_error(:nif_not_loaded)
  def engine_flush_pending(_engine), do: :erlang.nif_error(:nif_not_loaded)
  def engine_flush_cold(_engine, _max_idle_secs), do: :erlang.nif_error(:nif_not_loaded)
  def engine_flush_by_memory(_engine), do: :erlang.nif_error(:nif_not_loaded)
  def engine_flush(_engine), do: :erlang.nif_error(:nif_not_loaded)
  def engine_delete_before(_engine, _before_ts), do: :erlang.nif_error(:nif_not_loaded)
  def engine_shutdown(_engine), do: :erlang.nif_error(:nif_not_loaded)

  def engine_query_range(_engine, _metric, _labels, _t_start, _t_end),
    do: :erlang.nif_error(:nif_not_loaded)

  def engine_query_aggregate(_engine, _metric, _labels, _t_start, _t_end, _agg),
    do: :erlang.nif_error(:nif_not_loaded)

  def engine_list_metrics(_engine), do: :erlang.nif_error(:nif_not_loaded)
  def engine_list_labels(_engine), do: :erlang.nif_error(:nif_not_loaded)
  def engine_label_values(_engine, _metric, _label_key), do: :erlang.nif_error(:nif_not_loaded)
  def engine_list_series(_engine, _metric), do: :erlang.nif_error(:nif_not_loaded)
  def engine_info(_engine), do: :erlang.nif_error(:nif_not_loaded)

  # Strict read-only migration surface. These functions use a separate
  # resource type that cannot be passed to any mutation NIF.
  def engine_legacy_open(_data_dir), do: :erlang.nif_error(:nif_not_loaded)
  def engine_legacy_list_series(_reader), do: :erlang.nif_error(:nif_not_loaded)

  def engine_legacy_query_page(_reader, _metric, _labels, _after, _limit),
    do: :erlang.nif_error(:nif_not_loaded)

  def engine_legacy_info(_reader), do: :erlang.nif_error(:nif_not_loaded)

  # Prometheus text parser (production; wrapped by TimelessMetrics.PrometheusNif)
  def parse_prometheus(_body), do: :erlang.nif_error(:nif_not_loaded)
  # Fused parse -> resolve -> write ingest; returns {:ok, {count, errors}}
  def engine_ingest_prometheus(_engine, _body, _default_ts),
    do: :erlang.nif_error(:nif_not_loaded)

  # Bench-only: parse without term materialization (bench/ingest_segments_bench.exs)
  def parse_prometheus_count(_body), do: :erlang.nif_error(:nif_not_loaded)
  def decode_raw_batches(_batches), do: :erlang.nif_error(:nif_not_loaded)
  def decode_raw_frame(_frame), do: :erlang.nif_error(:nif_not_loaded)
  def decode_raw_frame_series(_frame, _labels), do: :erlang.nif_error(:nif_not_loaded)
  def decode_aggregate_frame(_frame), do: :erlang.nif_error(:nif_not_loaded)
  def decode_latest_frame(_frame), do: :erlang.nif_error(:nif_not_loaded)
end
