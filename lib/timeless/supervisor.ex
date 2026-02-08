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
    rollup_name = :"#{name}_rollup"
    retention_name = :"#{name}_retention"

    # Store schema and shard count in persistent_term for fast access
    :persistent_term.put({Timeless, name, :schema}, schema)
    :persistent_term.put({Timeless, name, :shard_count}, shard_count)

    # Each buffer shard gets its own SegmentBuilder for parallel compression
    builder_and_buffer_shards =
      for i <- 0..(shard_count - 1) do
        builder_name = :"#{name}_builder_#{i}"
        shard_name = :"#{name}_shard_#{i}"

        [
          %{
            id: builder_name,
            start:
              {Timeless.SegmentBuilder, :start_link,
               [
                 [
                   name: builder_name,
                   shard_id: i,
                   data_dir: data_dir,
                   segment_duration: segment_duration,
                   compression: compression,
                   schema: schema
                 ]
               ]}
          },
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
        ]
      end
      |> List.flatten()

    children =
      [
        # 1. SQLite connection manager
        {Timeless.DB, name: db_name, data_dir: data_dir},

        # 2. Series registry (depends on DB)
        {Timeless.SeriesRegistry, name: registry_name, db: db_name}
      ] ++
        # 3. Sharded segment builders + buffer shards (each buffer paired with its builder)
        builder_and_buffer_shards ++
        [
          # 4. Rollup engine (depends on DB + data being written)
          {Timeless.Rollup,
           name: rollup_name,
           db: db_name,
           store: name,
           schema: schema,
           compression: compression},

          # 5. Retention enforcer (depends on DB)
          {Timeless.Retention,
           name: retention_name,
           db: db_name,
           store: name,
           schema: schema}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
