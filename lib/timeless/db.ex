defmodule Timeless.DB do
  @moduledoc """
  SQLite connection manager with a single writer and pooled readers.

  Uses WAL mode for concurrent reads during writes. The writer is serialized
  through a GenServer to respect SQLite's single-writer constraint.
  """

  use GenServer

  defstruct [:writer, :readers, :data_dir, :db_path, :name]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Execute a write query (INSERT, UPDATE, DELETE) through the serialized writer."
  def write(db, sql, params \\ []) do
    GenServer.call(db, {:write, sql, params}, :infinity)
  end

  @doc "Execute multiple write queries in a single transaction."
  def write_transaction(db, fun) when is_function(fun, 1) do
    GenServer.call(db, {:write_transaction, fun}, :infinity)
  end

  @doc "Execute a read query using a reader connection from the pool."
  def read(db, sql, params \\ []) do
    GenServer.call(db, {:read, sql, params}, :infinity)
  end

  @doc "Get the database path."
  def db_path(db) do
    GenServer.call(db, :db_path)
  end

  @doc "Create a consistent backup of this database using VACUUM INTO."
  def backup(db, target_path) do
    GenServer.call(db, {:backup, target_path}, :infinity)
  end

  # --- Server ---

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    name = Keyword.fetch!(opts, :name)
    File.mkdir_p!(data_dir)

    db_path = Path.join(data_dir, "metrics.db")

    {:ok, writer} = Exqlite.Sqlite3.open(db_path)
    configure_connection(writer)
    run_migrations(writer)

    reader_count = Keyword.get(opts, :reader_pool_size, System.schedulers_online())

    readers =
      for _ <- 1..reader_count do
        {:ok, conn} = Exqlite.Sqlite3.open(db_path)
        configure_reader(conn)
        conn
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
    execute(state.writer, "BEGIN IMMEDIATE", [])

    try do
      result = fun.(state.writer)
      execute(state.writer, "COMMIT", [])
      {:reply, {:ok, result}, state}
    rescue
      e ->
        execute(state.writer, "ROLLBACK", [])
        {:reply, {:error, e}, state}
    end
  end

  def handle_call({:read, sql, params}, _from, state) do
    # Simple round-robin reader selection
    reader = Enum.random(state.readers)
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

  defp configure_connection(conn) do
    pragmas = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA cache_size = -128000",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA mmap_size = 2147483648",
      "PRAGMA wal_autocheckpoint = 10000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  defp configure_reader(conn) do
    pragmas = [
      "PRAGMA mmap_size = 2147483648",
      "PRAGMA cache_size = -8000",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.each(pragmas, &execute(conn, &1, []))
  end

  defp run_migrations(conn) do
    Timeless.DB.Migrations.run(conn)
  end

  @doc false
  def execute(conn, sql, params) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)

    if params != [] do
      :ok = Exqlite.Sqlite3.bind(stmt, params)
    end

    rows = fetch_all(conn, stmt, [])
    Exqlite.Sqlite3.release(conn, stmt)
    {:ok, rows}
  end

  defp fetch_all(conn, stmt, acc) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> fetch_all(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end
end
