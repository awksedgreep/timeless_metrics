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
    memory_only = Keyword.get(opts, :mode) == :memory
    data_dir = if memory_only, do: nil, else: Keyword.fetch!(opts, :data_dir)
    shard_count = Keyword.get(opts, :buffer_shards, max(div(System.schedulers_online(), 2), 2))
    segment_duration = Keyword.get(opts, :segment_duration, 14_400)
    compression = Keyword.get(opts, :compression, :zstd)
    compression_level = Keyword.get(opts, :compression_level, 2)

    raw_retention_seconds = Keyword.get(opts, :raw_retention_seconds, 604_800)
    daily_retention_seconds = Keyword.get(opts, :daily_retention_seconds, 31_536_000)
    _rollup_interval = Keyword.get(opts, :rollup_interval, :timer.minutes(5))
    _retention_interval = Keyword.get(opts, :retention_interval, :timer.hours(1))

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

    :persistent_term.put(
      {TimelessMetrics, name, :daily_retention_seconds},
      daily_retention_seconds
    )

    TimelessMetrics.Stats.init(name)

    # Ingest queue: ETS table for raw HTTP bodies awaiting background processing
    ingest_queue = :"#{name}_ingest_queue"

    :ets.new(ingest_queue, [
      :named_table,
      :ordered_set,
      :public,
      write_concurrency: :auto
    ])

    :persistent_term.put({TimelessMetrics, name, :ingest_queue}, ingest_queue)

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
                   memory_only: memory_only,
                   segment_duration: segment_duration,
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
                   segment_builder: builder_name
                 ]
               ]}
          }
        ]
      end
      |> List.flatten()

    # Ingest workers: background processors that drain the ETS queue
    ingest_worker_count =
      Keyword.get(opts, :ingest_workers, max(div(System.schedulers_online(), 4), 2))

    ingest_workers =
      for i <- 0..(ingest_worker_count - 1) do
        %{
          id: :"#{name}_ingest_worker_#{i}",
          start:
            {TimelessMetrics.IngestWorker, :start_link,
             [
               [
                 name: :"#{name}_ingest_worker_#{i}",
                 store: name,
                 queue: ingest_queue,
                 worker_id: i
               ]
             ]}
        }
      end

    # Core children — always present
    foundation =
      if memory_only do
        [
          {TimelessMetrics.SeriesRegistry, name: registry_name, db: nil, store: name}
        ]
      else
        [
          {TimelessMetrics.DB, name: db_name, data_dir: data_dir},
          {TimelessMetrics.SeriesRegistry, name: registry_name, db: db_name, store: name},
          {TimelessMetrics.DictTrainer, name: dict_trainer_name, store: name, data_dir: data_dir}
        ]
      end

    # Rollup and retention — both modes (memory mode uses ETS-backed segments)
    management = [
      {TimelessMetrics.Rollup,
       name: :"#{name}_rollup",
       db: if(!memory_only, do: db_name),
       store: name,
       schema: schema,
       compression: compression,
       compression_level: compression_level},
      {TimelessMetrics.Retention,
       name: :"#{name}_retention",
       db: if(!memory_only, do: db_name),
       store: name,
       schema: schema}
    ]

    children =
      foundation ++
        builder_and_buffer_shards ++
        (if memory_only, do: [], else: ingest_workers) ++
        management

    # New features — kept from actor era
    alert_interval = Keyword.get(opts, :alert_interval, :timer.seconds(60))

    alert_children =
      if memory_only do
        []
      else
        [
          {TimelessMetrics.AlertEvaluator,
           name: :"#{name}_alert_evaluator", store: name, interval: alert_interval}
        ]
      end

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
      if Keyword.get(opts, :scraping, true) and not memory_only do
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
