import Config

if config_env() == :prod do
  config :timeless_metrics,
    data_dir: System.get_env("TIMELESS_DATA_DIR", "/data"),
    port: String.to_integer(System.get_env("TIMELESS_PORT", "8428")),
    bearer_token: System.get_env("TIMELESS_BEARER_TOKEN")
end
