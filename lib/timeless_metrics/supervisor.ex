defmodule TimelessMetrics.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    sup_name = :"#{name}_sup"
    Supervisor.start_link(__MODULE__, opts, name: sup_name)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    shard_count = Keyword.get(opts, :buffer_shards, max(div(System.schedulers_online(), 2), 2))
    flush_interval = Keyword.get(opts, :flush_interval, :timer.seconds(5))
    flush_threshold = Keyword.get(opts, :flush_threshold, 10_000)
    segment_duration = Keyword.get(opts, :segment_duration, 14_400)
    pending_flush_interval = Keyword.get(opts, :pending_flush_interval, :timer.seconds(60))
    compression = Keyword.get(opts, :compression, :zstd)
    compression_level = Keyword.get(opts, :compression_level, 9)

    raw_retention_seconds = Keyword.get(opts, :raw_retention_seconds, 604_800)
    daily_retention_seconds = Keyword.get(opts, :daily_retention_seconds, 31_536_000)
    rollup_interval = Keyword.get(opts, :rollup_interval, :timer.minutes(5))
    retention_interval = Keyword.get(opts, :retention_interval, :timer.hours(1))

    schema =
      case Keyword.get(opts, :schema) do
        nil -> TimelessMetrics.Schema.default()
        mod when is_atom(mod) -> mod.__schema__()
        %TimelessMetrics.Schema{} = s -> s
      end

    :persistent_term.put({TimelessMetrics, name, :schema}, schema)
    :persistent_term.put({TimelessMetrics, name, :shard_count}, shard_count)
    :persistent_term.put({TimelessMetrics, name, :data_dir}, data_dir)
    :persistent_term.put({TimelessMetrics, name, :raw_retention_seconds}, raw_retention_seconds)
    :persistent_term.put({TimelessMetrics, name, :daily_retention_seconds}, daily_retention_seconds)

    TimelessMetrics.Stats.init(name)

    db_name = :"#{name}_db"
    registry_name = :"#{name}_registry"
    dict_trainer_name = :"#{name}_dict_trainer"

    # Each buffer shard gets its own SegmentBuilder for parallel compression
    builder_and_buffer_shards =
      for i <- 0..(shard_count - 1) do
        builder_name = :"#{name}_builder_#{i}"
        shard_name = :"#{name}_shard_#{i}"

        [
          %{
            id: builder_name,
            start:
              {TimelessMetrics.SegmentBuilder, :start_link,
               [
                 [
                   name: builder_name,
                   store: name,
                   shard_id: i,
                   data_dir: data_dir,
                   segment_duration: segment_duration,
                   pending_flush_interval: pending_flush_interval,
                   compression: compression,
                   compression_level: compression_level,
                   schema: schema
                 ]
               ]}
          },
          %{
            id: shard_name,
            start:
              {TimelessMetrics.Buffer, :start_link,
               [
                 [
                   name: shard_name,
                   store: name,
                   shard_id: i,
                   segment_builder: builder_name,
                   flush_interval: flush_interval,
                   flush_threshold: flush_threshold
                 ]
               ]}
          }
        ]
      end
      |> List.flatten()

    children =
      [
        {TimelessMetrics.DB, name: db_name, data_dir: data_dir},
        {TimelessMetrics.SeriesRegistry, name: registry_name, db: db_name},
        {TimelessMetrics.DictTrainer, name: dict_trainer_name, store: name, data_dir: data_dir}
      ] ++
        builder_and_buffer_shards ++
        [
          {TimelessMetrics.Rollup,
           name: :"#{name}_rollup",
           db: db_name,
           store: name,
           schema: schema,
           compression: compression,
           compression_level: compression_level},
          {TimelessMetrics.Retention,
           name: :"#{name}_retention",
           db: db_name,
           store: name,
           schema: schema}
        ]

    # New features — kept from actor era
    alert_interval = Keyword.get(opts, :alert_interval, :timer.seconds(60))

    alert_children = [
      {TimelessMetrics.AlertEvaluator,
       name: :"#{name}_alert_evaluator", store: name, interval: alert_interval}
    ]

    self_monitor_children =
      if Keyword.get(opts, :self_monitor, true) do
        labels = Keyword.get(opts, :self_monitor_labels, %{})

        [
          {TimelessMetrics.SelfMonitor,
           name: :"#{name}_self_monitor", store: name, labels: labels}
        ]
      else
        []
      end

    scrape_sup_name = :"#{name}_scrape_sup"
    scraper_name = :"#{name}_scraper"

    scraper_children =
      if Keyword.get(opts, :scraping, true) do
        [
          {DynamicSupervisor, name: scrape_sup_name, strategy: :one_for_one},
          {TimelessMetrics.Scraper,
           name: scraper_name, store: name, db: db_name, scrape_sup: scrape_sup_name}
        ]
      else
        []
      end

    Supervisor.init(children ++ alert_children ++ self_monitor_children ++ scraper_children,
      strategy: :rest_for_one
    )
  end
end
