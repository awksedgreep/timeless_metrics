import Config

if config_env() == :dev do
  config :timeless_metrics,
    data_dir: System.get_env("TIMELESS_DATA_DIR", "/tmp/timeless_metrics_dev"),
    port: String.to_integer(System.get_env("TIMELESS_PORT", "8428")),
    defer_compression: System.get_env("TIMELESS_DEFER_COMPRESSION", "false") == "true"
end

if config_env() == :prod do
  config :timeless_metrics,
    data_dir: System.get_env("TIMELESS_DATA_DIR", "/data"),
    port: String.to_integer(System.get_env("TIMELESS_PORT", "8428")),
    bearer_token: System.get_env("TIMELESS_BEARER_TOKEN")
end
