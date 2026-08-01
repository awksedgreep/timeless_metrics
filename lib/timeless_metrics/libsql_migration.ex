defmodule TimelessMetrics.LibsqlMigration do
  @moduledoc """
  Offline, verified migration from the filesystem Rust engine to libSQL.

  The source is never deleted. Import happens in `.libsql-migration/metrics.db`;
  activation is an explicit, same-filesystem rename which retains the prior
  administrative database as `metrics.db.pre-libsql` and leaves `rust_engine/`
  untouched for rollback.
  """

  alias TimelessMetrics.RustEngine.Nif

  @migration_store :timeless_libsql_migration_target
  @stage_name ".libsql-migration"

  def run(data_dir, opts \\ []) do
    activate? = Keyword.get(opts, :activate, false)
    activation_failpoint = Keyword.get(opts, :activation_failpoint)
    source_db = Path.join(data_dir, "metrics.db")
    source_engine = Path.join(data_dir, "rust_engine")
    stage_dir = Path.join(data_dir, @stage_name)
    stage_db = Path.join(stage_dir, "metrics.db")

    with :ok <- require_source(source_db, source_engine) do
      if activate? and File.dir?(stage_dir) do
        activate_staged(
          data_dir,
          source_db,
          source_engine,
          stage_dir,
          stage_db,
          activation_failpoint
        )
      else
        stage_new(
          data_dir,
          source_db,
          source_engine,
          stage_dir,
          stage_db,
          activate?,
          activation_failpoint
        )
      end
    end
  end

  defp stage_new(
         data_dir,
         source_db,
         source_engine,
         stage_dir,
         stage_db,
         activate?,
         activation_failpoint
       ) do
    with :ok <- require_clean_stage(stage_dir),
         :ok <- assert_exclusive(source_db),
         {:ok, source_fingerprints} <- source_fingerprints(source_db, source_engine),
         :ok <- File.mkdir(stage_dir),
         :ok <- snapshot_admin(source_db, stage_db),
         {:ok, report} <-
           import_and_verify(
             source_db,
             source_engine,
             stage_dir,
             source_fingerprints
           ),
         :ok <- sync_file(stage_db),
         :ok <- maybe_activate(activate?, data_dir, stage_dir, activation_failpoint) do
      {:ok, Map.put(report, :activated, activate?)}
    end
  end

  defp activate_staged(
         data_dir,
         source_db,
         source_engine,
         stage_dir,
         stage_db,
         activation_failpoint
       ) do
    with :ok <- assert_exclusive(source_db),
         {:ok, marker} <- verify_staged(stage_db),
         :ok <- verify_recorded_source(marker, source_db, source_engine),
         :ok <- maybe_activate(true, data_dir, stage_dir, activation_failpoint) do
      {:ok,
       %{
         activated: true,
         series: marker_value(marker, "series"),
         points: marker_value(marker, "points"),
         staged_db: stage_db
       }}
    end
  end

  defp require_source(source_db, source_engine) do
    cond do
      not File.regular?(source_db) -> {:error, "missing source database #{source_db}"}
      not File.dir?(source_engine) -> {:error, "missing source Rust engine #{source_engine}"}
      true -> :ok
    end
  end

  defp require_clean_stage(stage_dir) do
    if File.exists?(stage_dir) do
      {:error, "migration staging path already exists: #{stage_dir}"}
    else
      :ok
    end
  end

  defp assert_exclusive(source_db) do
    {:ok, conn} = Exqlite.Sqlite3.open(source_db)

    try do
      {:ok, _} = TimelessMetrics.DB.execute(conn, "PRAGMA busy_timeout = 1000", [])

      {:ok, [[0, _log_frames, _checkpointed_frames]]} =
        TimelessMetrics.DB.execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

      {:ok, _} = TimelessMetrics.DB.execute(conn, "BEGIN EXCLUSIVE", [])
      {:ok, _} = TimelessMetrics.DB.execute(conn, "ROLLBACK", [])
      :ok
    rescue
      error ->
        {:error, "source store must be stopped before migration: #{Exception.message(error)}"}
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp snapshot_admin(source_db, stage_db) do
    {:ok, conn} = Exqlite.Sqlite3.open(source_db)

    try do
      {:ok, _} = TimelessMetrics.DB.execute(conn, "VACUUM INTO ?1", [stage_db])
      :ok
    rescue
      error -> {:error, "failed to snapshot admin database: #{Exception.message(error)}"}
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp import_and_verify(source_db, source_engine, stage_dir, source_fingerprints) do
    source = Nif.engine_new(source_engine, 8_192, 64, 8, 0, false)

    child =
      {TimelessMetrics,
       name: @migration_store,
       engine: :libsql,
       data_dir: stage_dir,
       mode: :memory,
       scraping: false,
       self_monitor: false,
       reader_pool_size: 1}

    {:ok, supervisor} = Supervisor.start_link([child], strategy: :one_for_one)

    try do
      series = source_series(source)

      {series_count, point_count} =
        Enum.reduce(series, {0, 0}, fn {metric, labels}, {series_count, point_count} ->
          {:ok, _sid} =
            TimelessMetrics.LibsqlEngine.resolve_series(@migration_store, metric, labels)

          points = source_points(source, metric, labels)

          points
          |> Enum.chunk_every(100_000)
          |> Enum.each(fn chunk ->
            entries = Enum.map(chunk, fn {ts, value} -> {metric, labels, value, ts} end)
            :ok = TimelessMetrics.LibsqlEngine.write_batch(@migration_store, entries)
          end)

          {series_count + 1, point_count + length(points)}
        end)

      :ok = TimelessMetrics.LibsqlEngine.flush(@migration_store)
      verify_all(source, series)
      assert_integrity()
      verify_source_fingerprints!(source_fingerprints, source_db, source_engine)
      write_marker(source_db, source_engine, source_fingerprints, series_count, point_count)
      checkpoint()

      {:ok,
       %{series: series_count, points: point_count, staged_db: Path.join(stage_dir, "metrics.db")}}
    after
      Supervisor.stop(supervisor)
      _ = Nif.engine_shutdown(source)
    end
  rescue
    error -> {:error, Exception.format(:error, error, __STACKTRACE__)}
  end

  defp source_series(source) do
    {:ok, metrics} = normalize(Nif.engine_list_metrics(source))

    metrics
    |> Enum.flat_map(fn metric ->
      {:ok, labels_sets} = normalize(Nif.engine_list_series(source, metric))
      Enum.map(labels_sets, &{metric, Map.new(&1)})
    end)
  end

  defp source_points(source, metric, labels) do
    {:ok, rows} =
      normalize(
        Nif.engine_query_range(
          source,
          metric,
          labels,
          -9_223_372_036_854_775_808,
          9_223_372_036_854_775_807
        )
      )

    rows
    |> Enum.flat_map(fn {_returned_labels, points} -> points end)
    |> Enum.sort_by(fn {ts, value} -> {ts, float_bits(value)} end)
  end

  defp verify_all(source, series) do
    Enum.each(series, fn {metric, labels} ->
      expected = source_points(source, metric, labels)

      {:ok, actual} =
        TimelessMetrics.LibsqlEngine.query_raw(@migration_store, metric, labels,
          from: -9_223_372_036_854_775_808,
          to: 9_223_372_036_854_775_807
        )

      actual = Enum.sort_by(actual, fn {ts, value} -> {ts, float_bits(value)} end)

      unless bit_exact?(expected, actual) do
        raise "verification failed for #{metric} #{inspect(labels)}: " <>
                "source=#{length(expected)} target=#{length(actual)}"
      end
    end)
  end

  defp bit_exact?(left, right) when length(left) != length(right), do: false

  defp bit_exact?(left, right) do
    Enum.zip(left, right)
    |> Enum.all?(fn {{lts, lv}, {rts, rv}} -> lts == rts and float_bits(lv) == float_bits(rv) end)
  end

  defp float_bits(value), do: <<value * 1.0::float-native-64>>

  defp assert_integrity do
    db = :"#{@migration_store}_db"
    {:ok, [["ok"]]} = TimelessMetrics.DB.read(db, "PRAGMA integrity_check", [])
    :ok
  end

  defp write_marker(source_db, source_engine, source_fingerprints, series, points) do
    db = :"#{@migration_store}_db"

    marker =
      :json.encode(%{
        version: 1,
        source_db: Path.expand(source_db),
        source_engine: Path.expand(source_engine),
        source_db_sha256: source_fingerprints.db,
        source_engine_sha256: source_fingerprints.engine,
        series: series,
        points: points,
        migrated_at: System.system_time(:second)
      })
      |> IO.iodata_to_binary()

    {:ok, _} =
      TimelessMetrics.DB.write(
        db,
        "INSERT OR REPLACE INTO _metadata(key, value) VALUES ('libsql_migration', ?1)",
        [marker]
      )

    :ok
  end

  defp checkpoint do
    db = :"#{@migration_store}_db"
    {:ok, _} = TimelessMetrics.DB.write(db, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    :ok
  end

  defp verify_staged(stage_db) do
    conn = TimelessMetrics.LibsqlEngine.open_connection(stage_db)

    try do
      with {:ok, [["ok"]]} <- TimelessMetrics.DB.execute(conn, "PRAGMA integrity_check", []),
           {:ok, [[marker_json]]} <-
             TimelessMetrics.DB.execute(
               conn,
               "SELECT value FROM _metadata WHERE key = 'libsql_migration'",
               []
             ),
           marker when is_map(marker) <- :json.decode(marker_json),
           1 <- marker_value(marker, "version") do
        {:ok, marker}
      else
        other -> {:error, "staged migration is incomplete or invalid: #{inspect(other)}"}
      end
    rescue
      error -> {:error, "failed to verify staged migration: #{Exception.message(error)}"}
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp source_fingerprints(source_db, source_engine) do
    {:ok, %{db: sha256_file(source_db), engine: sha256_tree(source_engine)}}
  rescue
    error -> {:error, "failed to fingerprint source store: #{Exception.message(error)}"}
  end

  defp verify_recorded_source(marker, source_db, source_engine) do
    expected = %{
      db: marker_value(marker, "source_db_sha256"),
      engine: marker_value(marker, "source_engine_sha256")
    }

    verify_source_fingerprints(expected, source_db, source_engine)
  end

  defp verify_source_fingerprints(expected, source_db, source_engine) do
    with {:ok, actual} <- source_fingerprints(source_db, source_engine) do
      if actual == expected do
        :ok
      else
        {:error,
         "source store changed after the migration was staged; discard the stage and migrate again"}
      end
    end
  end

  defp verify_source_fingerprints!(expected, source_db, source_engine) do
    case verify_source_fingerprints(expected, source_db, source_engine) do
      :ok -> :ok
      {:error, reason} -> raise reason
    end
  end

  defp sha256_tree(root) do
    root
    |> regular_files()
    |> Enum.sort()
    |> Enum.reduce(:crypto.hash_init(:sha256), fn path, context ->
      relative = Path.relative_to(path, root)
      context = :crypto.hash_update(context, <<byte_size(relative)::unsigned-little-64>>)
      context = :crypto.hash_update(context, relative)
      hash_file_into(context, path)
    end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp regular_files(dir) do
    dir
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      path = Path.join(dir, name)

      case File.stat!(path).type do
        :directory -> regular_files(path)
        :regular -> [path]
        _ -> []
      end
    end)
  end

  defp sha256_file(path) do
    :crypto.hash_init(:sha256)
    |> hash_file_into(path)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp hash_file_into(context, path) do
    File.open!(path, [:read, :binary], fn io -> hash_io(context, io) end)
  end

  defp hash_io(context, io) do
    case IO.binread(io, 1_048_576) do
      :eof -> context
      data when is_binary(data) -> hash_io(:crypto.hash_update(context, data), io)
      {:error, reason} -> raise "failed reading migration source: #{inspect(reason)}"
    end
  end

  defp marker_value(marker, key), do: Map.get(marker, key, Map.get(marker, String.to_atom(key)))

  defp maybe_activate(false, _data_dir, _stage_dir, _failpoint), do: :ok

  defp maybe_activate(true, data_dir, stage_dir, failpoint) do
    current = Path.join(data_dir, "metrics.db")
    backup = Path.join(data_dir, "metrics.db.pre-libsql")
    staged = Path.join(stage_dir, "metrics.db")

    if File.exists?(backup) do
      {:error, "activation backup already exists: #{backup}"}
    else
      with :ok <- File.rename(current, backup),
           :ok <- activation_failpoint(failpoint, :after_source_rename),
           :ok <- move_sqlite_sidecars(current, backup),
           :ok <- File.rename(staged, current),
           :ok <- activation_failpoint(failpoint, :after_staged_rename),
           :ok <- sync_file(current),
           :ok <- File.rmdir(stage_dir) do
        :ok
      else
        {:error, reason} ->
          recovery = recover_activation(current, backup, staged, stage_dir)
          {:error, "activation failed: #{inspect(reason)}; recovery: #{inspect(recovery)}"}
      end
    end
  end

  defp activation_failpoint(point, point), do: {:error, {:injected_activation_failure, point}}
  defp activation_failpoint(_configured, _point), do: :ok

  defp recover_activation(current, backup, staged, stage_dir) do
    if File.exists?(backup) do
      with :ok <- File.mkdir_p(stage_dir),
           :ok <- restore_staged_candidate(current, staged),
           :ok <- File.rename(backup, current),
           :ok <- move_sqlite_sidecars(backup, current),
           :ok <- sync_file(current) do
        :source_restored
      end
    else
      :source_unchanged
    end
  end

  defp restore_staged_candidate(current, staged) do
    cond do
      File.exists?(staged) -> :ok
      File.exists?(current) -> File.rename(current, staged)
      true -> :ok
    end
  end

  defp move_sqlite_sidecars(from, to) do
    Enum.reduce_while(["-wal", "-shm"], :ok, fn suffix, :ok ->
      source = from <> suffix

      if File.exists?(source) do
        case File.rename(source, to <> suffix) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp sync_file(path) do
    with {:ok, io} <- :file.open(String.to_charlist(path), [:read, :raw, :binary]),
         :ok <- :file.sync(io),
         :ok <- :file.close(io) do
      :ok
    end
  end

  defp normalize(value), do: TimelessMetrics.RustEngine.normalize_nif_result(value)
end
