defmodule TimelessMetrics.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config = Application.get_all_env(:timeless_metrics)

    children =
      case Keyword.fetch(config, :data_dir) do
        {:ok, data_dir} ->
          port = Keyword.get(config, :port, 8428)
          bearer_token = Keyword.get(config, :bearer_token)

          [
            {TimelessMetrics,
             name: :timeless_metrics,
             data_dir: data_dir},
            {TimelessMetrics.HTTP,
             store: :timeless_metrics, port: port, bearer_token: bearer_token}
          ]

        :error ->
          # No config (dev/test) — tests start their own instances
          []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: TimelessMetrics.AppSupervisor)
  end
end
