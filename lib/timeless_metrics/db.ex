defmodule TimelessMetrics.DB do
  @moduledoc """
  SQLite connection manager with a single writer and pooled readers.

  Uses WAL mode for concurrent reads during writes. The writer is serialized
  through a GenServer to respect SQLite's single-writer constraint.
  """

  use GenServer

  defstruct [:writer, :readers, :data_dir, :db_path, :name]

  @max_retries 8

  @type server :: GenServer.server()
  @type row :: [term()]
  @type query_result :: {:ok, [row()]} | {:error, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Execute a write query (INSERT, UPDATE, DELETE) through the serialized writer."
  @spec write(server(), String.t(), list()) :: query_result()
  def write(db, sql, params \\ []) do
    TimelessMetrics.Call.write(db, {:write, sql, params})
  end

  @doc "Execute multiple write queries in a single transaction."
  @spec write_transaction(server(), (term() -> result)) :: {:ok, result} | {:error, term()}
        when result: term()
  def write_transaction(db, fun) when is_function(fun, 1) do
    TimelessMetrics.Call.maintenance(db, {:write_transaction, fun})
  end

  @doc "Execute a read query using a reader connection from the pool."
  @spec read(server(), String.t(), list()) :: query_result()
  def read(db, sql, params \\ []) do
    TimelessMetrics.Call.read(db, {:read, sql, params})
  end

  @doc "Get the database path."
  @spec db_path(server()) :: String.t() | {:error, term()}
  def db_path(db) do
    TimelessMetrics.Call.read(db, :db_path)
  end

  @doc "Create a consistent backup of this database using VACUUM INTO."
  @spec backup(server(), Path.t()) :: query_result()
  def backup(db, target_path) do
    TimelessMetrics.Call.maintenance(db, {:backup, target_path})
  end

  # --- Server ---

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    name = Keyword.fetch!(opts, :name)
    File.mkdir_p!(data_dir)

    db_path = Path.join(data_dir, "metrics.db")

    writer = open_with_retry(db_path, @max_retries)
    busy_timeout = Keyword.get(opts, :busy_timeout, 5_000)
    configure_connection(writer, busy_timeout)
    run_migrations(writer)

    default_readers =
      case System.get_env("CI") do
        nil -> System.schedulers_online()
        _ -> 1
      end

    reader_count = Keyword.get(opts, :reader_pool_size) || default_readers

    readers =
      for _ <- 1..reader_count do
        open_and_configure_reader(db_path, busy_timeout)
      end

    state = %__MODULE__{
      writer: writer,
      readers: readers,
      data_dir: data_dir,
      db_path: db_path,
      name: name
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:write, sql, params}, _from, state) do
    result = execute(state.writer, sql, params)
    {:reply, result, state}
  end

  def handle_call({:write_transaction, fun}, _from, state) do
    case execute(state.writer, "BEGIN IMMEDIATE", []) do
      {:ok, _} ->
        try do
          case fun.(state.writer) do
            {:error, _} = error ->
              _ = execute(state.writer, "ROLLBACK", [])
              {:reply, error, state}

            result ->
              case execute(state.writer, "COMMIT", []) do
                {:ok, _} ->
                  {:reply, {:ok, result}, state}

                {:error, _} = error ->
                  _ = execute(state.writer, "ROLLBACK", [])
                  {:reply, error, state}
              end
          end
        rescue
          exception ->
            _ = execute(state.writer, "ROLLBACK", [])
            {:reply, {:error, exception}, state}
        catch
          kind, reason ->
            _ = execute(state.writer, "ROLLBACK", [])
            {:reply, {:error, {kind, reason}}, state}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:read, sql, params}, {caller, _tag}, state) do
    # Keep each caller on a stable reader so repeated statement shapes retain
    # SQLite cache locality while independent callers still spread out.
    reader = Enum.at(state.readers, :erlang.phash2(caller, length(state.readers)))
    result = execute(reader, sql, params)
    {:reply, result, state}
  end

  def handle_call(:db_path, _from, state) do
    {:reply, state.db_path, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    result = execute(state.writer, "VACUUM INTO ?1", [target_path])
    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, state) do
    Exqlite.Sqlite3.close(state.writer)
    Enum.each(state.readers, &Exqlite.Sqlite3.close/1)
  end

  # --- Internals ---

  defp open_with_retry(path, retries) do
    case Exqlite.Sqlite3.open(path) do
      {:ok, conn} ->
        conn

      {:error, _reason} when retries > 0 ->
        Process.sleep(retry_backoff(@max_retries - retries))
        open_with_retry(path, retries - 1)

      {:error, reason} ->
        raise "failed to open SQLite database #{path}: #{inspect(reason)}"
    end
  end

  defp configure_connection(conn, busy_timeout) do
    pragmas = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA cache_size = -128000",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA mmap_size = #{mmap_size()}",
      "PRAGMA wal_autocheckpoint = 10000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = #{busy_timeout}"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  defp open_and_configure_reader(db_path, busy_timeout, attempts \\ 5) do
    conn = open_with_retry(db_path, @max_retries)

    try do
      configure_reader(conn, busy_timeout)
      conn
    rescue
      e ->
        Exqlite.Sqlite3.close(conn)

        if attempts > 1 do
          Process.sleep(200 * (6 - attempts))
          open_and_configure_reader(db_path, busy_timeout, attempts - 1)
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  defp configure_reader(conn, busy_timeout) do
    pragmas = [
      "PRAGMA mmap_size = #{mmap_size()}",
      "PRAGMA cache_size = -8000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = #{busy_timeout}"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  # 2GB mmap on real systems, disabled on CI/overlay filesystems
  defp mmap_size do
    case System.get_env("CI") do
      nil -> 2_147_483_648
      _ -> 0
    end
  end

  defp run_migrations(conn) do
    TimelessMetrics.DB.Migrations.run(conn)
  end

  @doc false
  @spec execute(term(), String.t(), list()) :: query_result()
  def execute(conn, sql, params) do
    try do
      execute_with_retry(conn, sql, params, @max_retries)
    rescue
      exception -> {:error, exception}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp execute_with_retry(conn, sql, params, retries) do
    case Exqlite.Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        result =
          with :ok <- bind_params(stmt, params) do
            fetch_all(conn, stmt, [])
          end

        _ = Exqlite.Sqlite3.release(conn, stmt)
        result

      {:error, reason} ->
        if retries > 0 and retryable_sqlite_error?(reason) do
          Process.sleep(retry_backoff(@max_retries - retries))
          execute_with_retry(conn, sql, params, retries - 1)
        else
          {:error, {:sqlite, reason, sql}}
        end
    end
  end

  defp bind_params(_stmt, []), do: :ok
  defp bind_params(stmt, params), do: Exqlite.Sqlite3.bind(stmt, params)

  defp retryable_sqlite_error?(reason) do
    message = reason |> inspect() |> String.downcase()
    String.contains?(message, "busy") or String.contains?(message, "locked")
  end

  defp fetch_all(conn, stmt, acc) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_all(conn, stmt, [row | acc])
      :done -> {:ok, Enum.reverse(acc)}
      :busy -> {:error, {:sqlite, :busy}}
      {:error, reason} -> {:error, {:sqlite, reason}}
    end
  end

  # Exponential backoff: 100, 200, 400, 800, 1600, 3200, 6400, 12800ms
  defp retry_backoff(attempt), do: 100 * Integer.pow(2, attempt)
end
