import Config

if config_env() == :prod do
  shards =
    case System.get_env("TIMELESS_SHARDS") do
      nil -> System.schedulers_online()
      val -> String.to_integer(val)
    end

  config :timeless_metrics,
    data_dir: System.get_env("TIMELESS_DATA_DIR", "/data"),
    port: String.to_integer(System.get_env("TIMELESS_PORT", "8428")),
    buffer_shards: shards,
    segment_duration: String.to_integer(System.get_env("TIMELESS_SEGMENT_DURATION", "14400")),
    pending_flush_interval:
      String.to_integer(System.get_env("TIMELESS_PENDING_FLUSH_INTERVAL", "60")) * 1000,
    bearer_token: System.get_env("TIMELESS_BEARER_TOKEN")
end
