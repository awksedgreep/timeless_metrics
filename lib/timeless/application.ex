defmodule Timeless.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config = Application.get_all_env(:timeless)

    children =
      case Keyword.fetch(config, :data_dir) do
        {:ok, data_dir} ->
          port = Keyword.get(config, :port, 8428)
          shards = Keyword.get(config, :buffer_shards, System.schedulers_online())
          segment_duration = Keyword.get(config, :segment_duration, 14_400)

          [
            {Timeless,
             name: :timeless,
             data_dir: data_dir,
             buffer_shards: shards,
             segment_duration: segment_duration},
            {Timeless.HTTP, store: :timeless, port: port}
          ]

        :error ->
          # No config (dev/test) — tests start their own instances
          []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: Timeless.AppSupervisor)
  end
end
