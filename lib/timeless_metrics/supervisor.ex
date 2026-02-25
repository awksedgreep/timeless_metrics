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
    compression = Keyword.get(opts, :compression, :zstd)
    max_blocks = Keyword.get(opts, :max_blocks, 100)
    block_size = Keyword.get(opts, :block_size, 1000)
    flush_interval = Keyword.get(opts, :flush_interval, 60_000)

    merge_block_min_count = Keyword.get(opts, :merge_block_min_count, 4)
    merge_block_max_points = Keyword.get(opts, :merge_block_max_points, 10_000)
    merge_block_min_age_seconds = Keyword.get(opts, :merge_block_min_age_seconds, 300)
    merge_interval = Keyword.get(opts, :merge_interval, 300_000)

    raw_retention_seconds = Keyword.get(opts, :raw_retention_seconds, 604_800)
    daily_retention_seconds = Keyword.get(opts, :daily_retention_seconds, 31_536_000)
    rollup_interval = Keyword.get(opts, :rollup_interval, :timer.minutes(5))
    retention_interval = Keyword.get(opts, :retention_interval, :timer.hours(1))

    :persistent_term.put({TimelessMetrics, name, :data_dir}, data_dir)
    :persistent_term.put({TimelessMetrics, name, :raw_retention_seconds}, raw_retention_seconds)
    :persistent_term.put({TimelessMetrics, name, :daily_retention_seconds}, daily_retention_seconds)

    db_name = :"#{name}_db"
    registry_name = :"#{name}_actor_registry"
    dynamic_sup_name = :"#{name}_actor_sup"
    manager_name = :"#{name}_actor_manager"

    scrape_sup_name = :"#{name}_scrape_sup"
    scraper_name = :"#{name}_scraper"

    children = [
      {TimelessMetrics.DB, name: db_name, data_dir: data_dir},
      {Registry, keys: :unique, name: registry_name},
      {DynamicSupervisor, name: dynamic_sup_name, strategy: :one_for_one},
      {TimelessMetrics.Actor.SeriesManager,
       name: manager_name,
       store: name,
       data_dir: data_dir,
       db: db_name,
       registry: registry_name,
       dynamic_sup: dynamic_sup_name,
       max_blocks: max_blocks,
       block_size: block_size,
       compression: compression,
       flush_interval: flush_interval,
       merge_block_min_count: merge_block_min_count,
       merge_block_max_points: merge_block_max_points,
       merge_block_min_age_seconds: merge_block_min_age_seconds,
       merge_interval: merge_interval},
      {TimelessMetrics.Actor.Rollup,
       name: :"#{name}_rollup",
       store: name,
       db: db_name,
       manager: manager_name,
       registry: registry_name,
       interval: rollup_interval},
      {TimelessMetrics.Actor.Retention,
       name: :"#{name}_retention",
       store: name,
       db: db_name,
       manager: manager_name,
       registry: registry_name,
       raw_retention_seconds: raw_retention_seconds,
       daily_retention_seconds: daily_retention_seconds,
       interval: retention_interval}
    ]

    alert_interval = Keyword.get(opts, :alert_interval, :timer.seconds(60))

    alert_children = [
      {TimelessMetrics.AlertEvaluator,
       name: :"#{name}_alert_evaluator",
       store: name,
       interval: alert_interval}
    ]

    self_monitor_children =
      if Keyword.get(opts, :self_monitor, true) do
        [{TimelessMetrics.SelfMonitor, name: :"#{name}_self_monitor", store: name}]
      else
        []
      end

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

    Supervisor.init(children ++ alert_children ++ self_monitor_children ++ scraper_children, strategy: :rest_for_one)
  end
end
