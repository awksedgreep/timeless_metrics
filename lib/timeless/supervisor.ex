defmodule Timeless.Supervisor do
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
    shard_count = Keyword.get(opts, :buffer_shards, System.schedulers_online())
    flush_interval = Keyword.get(opts, :flush_interval, :timer.seconds(5))
    flush_threshold = Keyword.get(opts, :flush_threshold, 10_000)
    segment_duration = Keyword.get(opts, :segment_duration, 3_600)
    compression = Keyword.get(opts, :compression, :zstd)

    schema =
      case Keyword.get(opts, :schema) do
        nil -> Timeless.Schema.default()
        mod when is_atom(mod) -> mod.__schema__()
        %Timeless.Schema{} = s -> s
      end

    db_name = :"#{name}_db"
    registry_name = :"#{name}_registry"
    builder_name = :"#{name}_builder"
    rollup_name = :"#{name}_rollup"
    retention_name = :"#{name}_retention"

    # Store schema in persistent_term for query access
    :persistent_term.put({Timeless, name, :schema}, schema)

    buffer_shards =
      for i <- 0..(shard_count - 1) do
        shard_name = :"#{name}_shard_#{i}"

        %{
          id: shard_name,
          start:
            {Timeless.Buffer, :start_link,
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
      end

    children =
      [
        # 1. SQLite connection manager
        {Timeless.DB, name: db_name, data_dir: data_dir},

        # 2. Series registry (depends on DB)
        {Timeless.SeriesRegistry, name: registry_name, db: db_name},

        # 3. Segment builder (depends on DB)
        {Timeless.SegmentBuilder,
         name: builder_name,
         db: db_name,
         segment_duration: segment_duration,
         compression: compression}
      ] ++
        buffer_shards ++
        [
          # 5. Rollup engine (depends on DB + data being written)
          {Timeless.Rollup,
           name: rollup_name,
           db: db_name,
           store: name,
           schema: schema,
           compression: compression},

          # 6. Retention enforcer (depends on DB)
          {Timeless.Retention,
           name: retention_name,
           db: db_name,
           schema: schema}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
