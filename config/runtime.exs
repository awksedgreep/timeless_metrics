import Config

positive_integer = fn name, default ->
  case System.get_env(name) do
    nil ->
      default

    value ->
      case Integer.parse(value) do
        {parsed, ""} when parsed > 0 ->
          parsed

        _ ->
          IO.warn("ignoring invalid #{name}=#{inspect(value)}; using #{default}")
          default
      end
  end
end

boolean = fn name, default ->
  case System.get_env(name) do
    nil ->
      default

    value when value in ["1", "true", "TRUE", "yes"] ->
      true

    value when value in ["0", "false", "FALSE", "no"] ->
      false

    value ->
      IO.warn("ignoring invalid #{name}=#{inspect(value)}; using #{default}")
      default
  end
end

common = [
  port: positive_integer.("TIMELESS_PORT", 8428),
  reader_pool_size:
    positive_integer.("TIMELESS_READER_POOL_SIZE", max(div(System.schedulers_online(), 2), 2)),
  ingest_workers:
    positive_integer.("TIMELESS_INGEST_WORKERS", max(div(System.schedulers_online(), 4), 2)),
  flush_interval: positive_integer.("TIMELESS_FLUSH_INTERVAL_MS", 10_000),
  ingest_transaction_ms: positive_integer.("TIMELESS_INGEST_TRANSACTION_MS", 5),
  ingest_transaction_max: positive_integer.("TIMELESS_INGEST_TRANSACTION_MAX", 256),
  busy_timeout: positive_integer.("TIMELESS_BUSY_TIMEOUT_MS", 5_000),
  max_body: positive_integer.("TIMELESS_MAX_BODY_BYTES", 10 * 1024 * 1024),
  auto_migrate: boolean.("TIMELESS_AUTO_MIGRATE", true),
  maintenance: boolean.("TIMELESS_MAINTENANCE", true),
  defer_compression: boolean.("TIMELESS_DEFER_COMPRESSION", false),
  extension_path: System.get_env("TIMELESS_EXT_PATH")
]

if config_env() == :dev do
  config :timeless_metrics,
         Keyword.merge(common,
           data_dir: System.get_env("TIMELESS_DATA_DIR", "/tmp/timeless_metrics_dev")
         )
end

if config_env() == :prod do
  config :timeless_metrics,
         Keyword.merge(common,
           data_dir: System.get_env("TIMELESS_DATA_DIR", "/data"),
           bearer_token: System.get_env("TIMELESS_BEARER_TOKEN")
         )
end
