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

    schema =
      case Keyword.get(opts, :schema) do
        nil -> TimelessMetrics.Schema.default()
        mod when is_atom(mod) -> mod.__schema__()
        %TimelessMetrics.Schema{} = s -> s
      end

    db_name = :"#{name}_db"
    registry_name = :"#{name}_registry"
    rollup_name = :"#{name}_rollup"
    retention_name = :"#{name}_retention"
    dict_trainer_name = :"#{name}_dict_trainer"

    # Store schema, shard count, and data_dir in persistent_term for fast access
    :persistent_term.put({TimelessMetrics, name, :schema}, schema)
    :persistent_term.put({TimelessMetrics, name, :shard_count}, shard_count)
    :persistent_term.put({TimelessMetrics, name, :data_dir}, data_dir)

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

    # 3. Sharded segment builders + buffer shards (each buffer paired with its builder)
    children =
      [
        # 1. SQLite connection manager
        {TimelessMetrics.DB, name: db_name, data_dir: data_dir},

        # 2. Series registry (depends on DB)
        {TimelessMetrics.SeriesRegistry, name: registry_name, db: db_name},

        # 3. Dict trainer (loads existing dictionaries for compression/decompression)
        {TimelessMetrics.DictTrainer, name: dict_trainer_name, store: name, data_dir: data_dir}
      ] ++
        builder_and_buffer_shards ++
        [
          # 4. Rollup engine (depends on DB + data being written)
          {TimelessMetrics.Rollup,
           name: rollup_name,
           db: db_name,
           store: name,
           schema: schema,
           compression: compression,
           compression_level: compression_level},

          # 5. Retention enforcer (depends on DB)
          {TimelessMetrics.Retention,
           name: retention_name, db: db_name, store: name, schema: schema}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
