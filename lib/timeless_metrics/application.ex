defmodule TimelessMetrics.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config = Application.get_all_env(:timeless_metrics)

    children =
      case Keyword.fetch(config, :data_dir) do
        {:ok, data_dir} ->
          port = Keyword.get(config, :port, 8428)
          bearer_token = Keyword.get(config, :bearer_token)

          store_opts =
            config
            |> Keyword.take([
              :engine,
              :mode,
              :schema,
              :reader_pool_size,
              :ingest_workers,
              :alert_interval,
              :self_monitor,
              :self_monitor_labels,
              :scraping,
              :compression,
              :compression_level,
              :buffer_shards,
              :segment_duration,
              :max_blocks,
              :block_size,
              :flush_interval,
              :merge_block_min_count,
              :merge_block_max_points,
              :merge_block_min_age_seconds,
              :merge_compression_level,
              :merge_interval,
              :gc_on_compress,
              :defer_compression,
              :raw_buffer_max,
              :rollup_interval,
              :retention_interval
            ])
            |> Keyword.merge(name: :timeless_metrics, data_dir: data_dir)

          [
            {TimelessMetrics, store_opts},
            {TimelessMetrics.HTTP,
             store: :timeless_metrics, port: port, bearer_token: bearer_token}
          ]

        :error ->
          # No config (dev/test) — tests start their own instances
          []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: TimelessMetrics.AppSupervisor)
  end
end
