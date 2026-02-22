defmodule TimelessMetrics.Scraper do
  @moduledoc false

  use GenServer
  require Logger

  alias TimelessMetrics.Scraper.{Target, Worker}

  defstruct [:store, :db, :scrape_sup, :workers]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def add_target(scraper, params), do: GenServer.call(scraper, {:add_target, params})
  def update_target(scraper, id, params), do: GenServer.call(scraper, {:update_target, id, params})
  def delete_target(scraper, id), do: GenServer.call(scraper, {:delete_target, id})
  def get_target(scraper, id), do: GenServer.call(scraper, {:get_target, id})
  def list_targets(scraper), do: GenServer.call(scraper, :list_targets)

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)
    db = Keyword.fetch!(opts, :db)
    scrape_sup = Keyword.fetch!(opts, :scrape_sup)

    state = %__MODULE__{
      store: store,
      db: db,
      scrape_sup: scrape_sup,
      workers: %{}
    }

    # Load existing enabled targets and start workers
    state = load_targets(state)

    {:ok, state}
  end

  @impl true
  def handle_call({:add_target, params}, _from, state) do
    case Target.validate(params) do
      {:ok, params} ->
        target = Target.from_params(params)
        now = System.os_time(:second)

        result =
          TimelessMetrics.DB.write_transaction(state.db, fn conn ->
            TimelessMetrics.DB.execute(
              conn,
              """
              INSERT INTO scrape_targets
                (job_name, scheme, address, metrics_path, scrape_interval, scrape_timeout,
                 labels, honor_labels, honor_timestamps, relabel_configs, metric_relabel_configs,
                 enabled, created_at, updated_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
              """,
              Target.to_params(target) ++ [now, now]
            )

            {:ok, [[id]]} = TimelessMetrics.DB.execute(conn, "SELECT last_insert_rowid()", [])
            id
          end)

        case result do
          {:ok, id} ->
            target = %{target | id: id}
            state = start_worker(state, target)
            {:reply, {:ok, id}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:update_target, id, params}, _from, state) do
    case Target.validate(params) do
      {:ok, params} ->
        target = Target.from_params(params)
        now = System.os_time(:second)

        sql_params = Target.to_params(target) ++ [now, id]

        TimelessMetrics.DB.write(
          state.db,
          """
          UPDATE scrape_targets SET
            job_name = ?1, scheme = ?2, address = ?3, metrics_path = ?4,
            scrape_interval = ?5, scrape_timeout = ?6, labels = ?7,
            honor_labels = ?8, honor_timestamps = ?9, relabel_configs = ?10,
            metric_relabel_configs = ?11, enabled = ?12, updated_at = ?13
          WHERE id = ?14
          """,
          sql_params
        )

        state = stop_worker(state, id)
        target = %{target | id: id}

        state =
          if target.enabled do
            start_worker(state, target)
          else
            state
          end

        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete_target, id}, _from, state) do
    state = stop_worker(state, id)
    TimelessMetrics.DB.write(state.db, "DELETE FROM scrape_targets WHERE id = ?1", [id])
    {:reply, :ok, state}
  end

  def handle_call({:get_target, id}, _from, state) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        state.db,
        """
        SELECT t.*, h.health, h.last_scrape, h.last_duration_ms, h.last_error, h.samples_scraped
        FROM scrape_targets t
        LEFT JOIN scrape_health h ON h.target_id = t.id
        WHERE t.id = ?1
        """,
        [id]
      )

    result =
      case rows do
        [row] -> {:ok, format_target_with_health(row)}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call(:list_targets, _from, state) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        state.db,
        """
        SELECT t.*, h.health, h.last_scrape, h.last_duration_ms, h.last_error, h.samples_scraped
        FROM scrape_targets t
        LEFT JOIN scrape_health h ON h.target_id = t.id
        ORDER BY t.id
        """,
        []
      )

    targets = Enum.map(rows, &format_target_with_health/1)
    {:reply, {:ok, targets}, state}
  end

  # --- Private ---

  defp load_targets(state) do
    {:ok, rows} =
      TimelessMetrics.DB.read(
        state.db,
        """
        SELECT id, job_name, scheme, address, metrics_path, scrape_interval, scrape_timeout,
               labels, honor_labels, honor_timestamps, relabel_configs, metric_relabel_configs,
               enabled, created_at, updated_at
        FROM scrape_targets WHERE enabled = 1
        """,
        []
      )

    Enum.reduce(rows, state, fn row, acc ->
      target = Target.from_row(row)
      start_worker(acc, target)
    end)
  end

  defp start_worker(state, target) do
    case DynamicSupervisor.start_child(
           state.scrape_sup,
           {Worker, target: target, store: state.store, db: state.db}
         ) do
      {:ok, pid} ->
        %{state | workers: Map.put(state.workers, target.id, pid)}

      {:error, reason} ->
        Logger.error("Failed to start scrape worker for target #{target.id}: #{inspect(reason)}")
        state
    end
  end

  defp stop_worker(state, id) do
    case Map.pop(state.workers, id) do
      {nil, workers} ->
        %{state | workers: workers}

      {pid, workers} ->
        DynamicSupervisor.terminate_child(state.scrape_sup, pid)
        %{state | workers: workers}
    end
  end

  defp format_target_with_health(row) do
    # Row: 15 target columns + 5 health columns
    target_cols = Enum.take(row, 15)
    health_cols = Enum.drop(row, 15)

    target = Target.from_row(target_cols)

    health =
      case health_cols do
        [nil, _, _, _, _] ->
          %{health: "unknown", last_scrape: nil, last_duration_ms: nil, last_error: nil, samples_scraped: 0}

        [health, last_scrape, last_duration_ms, last_error, samples_scraped] ->
          %{
            health: health || "unknown",
            last_scrape: last_scrape,
            last_duration_ms: last_duration_ms,
            last_error: last_error,
            samples_scraped: samples_scraped || 0
          }

        _ ->
          %{health: "unknown", last_scrape: nil, last_duration_ms: nil, last_error: nil, samples_scraped: 0}
      end

    %{
      id: target.id,
      job_name: target.job_name,
      scheme: target.scheme,
      address: target.address,
      metrics_path: target.metrics_path,
      scrape_interval: target.scrape_interval,
      scrape_timeout: target.scrape_timeout,
      labels: target.labels,
      honor_labels: target.honor_labels,
      honor_timestamps: target.honor_timestamps,
      relabel_configs: clean_configs(target.relabel_configs),
      metric_relabel_configs: clean_configs(target.metric_relabel_configs),
      enabled: target.enabled,
      created_at: target.created_at,
      updated_at: target.updated_at,
      health: health
    }
  end

  defp clean_configs(nil), do: nil
  defp clean_configs(configs) when is_list(configs) do
    Enum.map(configs, &Map.delete(&1, "__compiled_regex__"))
  end
end
