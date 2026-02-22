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
       flush_interval: flush_interval},
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

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
