defmodule TimelessMetrics.ReleaseMigration do
  @moduledoc false

  alias TimelessMetrics.{DB, LegacyReader, LibsqlEngine}

  @signal "metrics"
  @journal_version 1
  @page_size 4_096
  @migration_store :timeless_metrics_release_migration
  @digest_modulus Integer.pow(2, 256)
  @minimum_headroom 64 * 1_024 * 1_024

  @doc "Stage or resume an immutable Rust-block-store conversion. Cutover is Session 3."
  def stage(data_dir, opts \\ []) do
    data_dir = Path.expand(data_dir)
    source_db = Path.join(data_dir, "metrics.db")
    source_engine = Path.join(data_dir, "rust_engine")
    candidate_dir = Path.join([data_dir, ".timeless-migration", @signal])
    candidate_db = Path.join(candidate_dir, "metrics.db")
    started = System.monotonic_time(:nanosecond)
    start_observation(candidate_db)

    # WAL is durable source data when present. The shared-memory file is a
    # transient lock/index cache that SQLite may remove just after the last
    # legacy connection exits; treating it as source bytes creates a false
    # mutation race without preserving any user data.
    source_paths =
      [source_db, source_db <> "-wal", source_engine]
      |> Enum.filter(&durable_source_path?/1)

    with :ok <- require_source(source_db, source_engine),
         {:ok, manifest} <- source_manifest(data_dir, source_paths),
         :ok <- preflight_disk(candidate_dir, manifest.bytes, opts),
         {:ok, reader} <- LegacyReader.open(source_engine),
         {:ok, series} <- LegacyReader.series(reader),
         {:ok, info} <- LegacyReader.info(reader),
         :ok <- File.mkdir_p(candidate_dir),
         {:ok, supervisor} <- start_candidate(candidate_dir, opts) do
      try do
        with :ok <- require_migration_capability(),
             :ok <- LibsqlEngine.migration_tune(@migration_store),
             {:ok, journal} <- initialize_or_resume(manifest, info, length(series)),
             {:ok, copied} <- copy_series(reader, series, journal, opts),
             {:ok, maintenance} <- finish_public_maintenance(),
             :ok <- mark_phase("validating"),
             :ok <- verify_manifest(data_dir, source_paths, manifest),
             :ok <- stop_candidate(supervisor),
             {:ok, validation} <-
               cold_validate(
                 candidate_db,
                 series,
                 copied.records_completed,
                 copied.identity_digest,
                 opts
               ),
             {:ok, report} <-
               finish_report(
                 candidate_db,
                 manifest,
                 copied,
                 validation,
                 maintenance,
                 started,
                 opts
               ) do
          {:ok, report}
        end
      rescue
        error -> {:error, Exception.format(:error, error, __STACKTRACE__)}
      catch
        {:migration_failpoint, point} ->
          {:error, "injected migration failure at #{point}; committed work is resumable"}
      after
        if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
      end
    end
  end

  def candidate_path(data_dir) do
    Path.join([Path.expand(data_dir), ".timeless-migration", @signal, "metrics.db"])
  end

  @doc false
  def legacy_manifest(data_dir) do
    data_dir = Path.expand(data_dir)
    source_db = Path.join(data_dir, "metrics.db")
    source_engine = Path.join(data_dir, "rust_engine")

    source_paths =
      [source_db, source_db <> "-wal", source_engine]
      |> Enum.filter(&durable_source_path?/1)

    with :ok <- require_source(source_db, source_engine) do
      source_manifest(data_dir, source_paths)
    end
  end

  @doc false
  def validate_checkpoint(path, series, opts \\ []) when is_list(series) do
    with {:ok, conn} <- Exqlite.Sqlite3.open(path, mode: :readonly) do
      result =
        case DB.execute(
               conn,
               "SELECT series_index,cursor_json,records_completed,identity_digest FROM _timeless_migration WHERE singleton=1",
               []
             ) do
          {:ok, [[series_index, cursor, completed, digest]]} ->
            migrated_series = Enum.take(series, series_index + if(is_nil(cursor), do: 0, else: 1))
            {:ok, migrated_series, completed, digest}

          other ->
            {:error, "invalid metrics checkpoint journal: #{inspect(other)}"}
        end

      Exqlite.Sqlite3.close(conn)

      with {:ok, migrated_series, completed, digest} <- result,
           {:ok, _} <- cold_validate(path, migrated_series, completed, digest, opts) do
        :ok
      end
    end
  end

  defp require_source(source_db, source_engine) do
    cond do
      not File.regular?(source_db) -> {:error, "missing legacy metrics database #{source_db}"}
      not File.dir?(source_engine) -> {:error, "missing legacy Rust store #{source_engine}"}
      File.ls!(source_engine) == [] -> {:error, "legacy Rust store is empty: #{source_engine}"}
      true -> :ok
    end
  end

  defp durable_source_path?(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: 0}} -> not String.ends_with?(path, ".db-wal")
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  defp start_candidate(candidate_dir, opts) do
    schema = Keyword.get(opts, :schema, TimelessMetrics.Schema.default())

    # maintenance: false is the staging profile — no periodic compact, no
    # wall-clock retention prune. The candidate holds historical data
    # mid-copy that a `prune:now-raw_retention` tick destroys (issue #2);
    # maintenance runs only where stage/2 explicitly orders it, and
    # tiering applies post-cutover under the operator's schema.
    child =
      {TimelessMetrics,
       name: @migration_store,
       engine: :libsql,
       data_dir: candidate_dir,
       extension_path: Keyword.get(opts, :extension_path),
       mode: :memory,
       schema: schema,
       scraping: false,
       self_monitor: false,
       maintenance: false,
       reader_pool_size: 1}

    Supervisor.start_link([child], strategy: :one_for_one)
  end

  defp require_migration_capability do
    with {:ok, [[json]]} <-
           GenServer.call(
             LibsqlEngine.writer_name(@migration_store),
             {:sql, "SELECT timeless_capabilities()", []},
             :infinity
           ),
         capabilities <- :json.decode(json),
         true <- capabilities["data_abi"] == 1,
         batches when is_list(batches) <- get_in(capabilities, ["signals", "metrics", "batches"]),
         true <- "resolved-v1" in batches do
      :ok
    else
      other ->
        {:error,
         "extension lacks metrics resolved-v1/data ABI 1 migration capability: #{inspect(other)}"}
    end
  rescue
    error -> {:error, "extension capability handshake failed: #{Exception.message(error)}"}
  end

  defp initialize_or_resume(manifest, info, series_total) do
    db = db_name()

    for sql <- [
          """
          CREATE TABLE IF NOT EXISTS _timeless_migration (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL,
            signal TEXT NOT NULL,
            phase TEXT NOT NULL,
            source_manifest_json TEXT NOT NULL,
            source_manifest_digest TEXT NOT NULL,
            series_index INTEGER NOT NULL,
            cursor_json TEXT,
            records_completed INTEGER NOT NULL,
            records_total INTEGER NOT NULL,
            series_completed INTEGER NOT NULL,
            series_total INTEGER NOT NULL,
            identity_digest TEXT NOT NULL,
            checkpoints INTEGER NOT NULL,
            retries INTEGER NOT NULL,
            started_at_ns INTEGER NOT NULL,
            updated_at_ns INTEGER NOT NULL
          ) STRICT
          """,
          """
          CREATE TABLE IF NOT EXISTS _timeless_migration_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            phase TEXT NOT NULL,
            series_index INTEGER NOT NULL,
            cursor_json TEXT,
            records_completed INTEGER NOT NULL,
            at_ns INTEGER NOT NULL
          ) STRICT
          """
        ] do
      {:ok, _} = DB.write(db, sql, [])
    end

    {:ok, rows} =
      DB.read(
        db,
        """
        SELECT version, signal, phase, source_manifest_json, source_manifest_digest,
               series_index, cursor_json, records_completed, records_total,
               series_completed, series_total, identity_digest, checkpoints, retries
        FROM _timeless_migration WHERE singleton = 1
        """,
        []
      )

    case rows do
      [] ->
        now = System.system_time(:nanosecond)
        records_total = map_value(info, "points")

        {:ok, _} =
          DB.write(
            db,
            """
            INSERT INTO _timeless_migration
              (singleton,version,signal,phase,source_manifest_json,
               source_manifest_digest,series_index,cursor_json,
               records_completed,records_total,series_completed,series_total,
               identity_digest,checkpoints,retries,started_at_ns,updated_at_ns)
            VALUES (1,?1,?2,'copying',?3,?4,0,NULL,0,?5,0,?6,?7,0,0,?8,?8)
            """,
            [
              @journal_version,
              @signal,
              manifest.json,
              manifest.digest,
              records_total,
              series_total,
              zero_digest(),
              now
            ]
          )

        read_journal()

      [
        [
          version,
          signal,
          phase,
          stored_manifest_json,
          digest,
          series_index,
          cursor_json,
          records_completed,
          records_total,
          series_completed,
          recorded_series_total,
          identity_digest,
          checkpoints,
          retries
        ]
      ] ->
        cond do
          version != @journal_version ->
            {:error,
             "incompatible metrics migration journal version #{version}; expected #{@journal_version}"}

          signal != @signal ->
            {:error, "candidate journal belongs to #{signal}, not #{@signal}"}

          not manifest_matches?(stored_manifest_json, digest, manifest) ->
            {:error, "legacy metrics source changed since migration began"}

          records_total != map_value(info, "points") or recorded_series_total != series_total ->
            {:error, "legacy metrics inventory changed since migration began"}

          true ->
            {:ok, _} =
              DB.write(
                db,
                "UPDATE _timeless_migration SET retries = retries + 1 WHERE singleton=1"
              )

            {:ok,
             %{
               phase: phase,
               series_index: series_index,
               cursor: decode_cursor(cursor_json),
               records_completed: records_completed,
               records_total: records_total,
               series_completed: series_completed,
               series_total: recorded_series_total,
               identity_digest: identity_digest,
               checkpoints: checkpoints,
               retries: retries + 1,
               source_scan_ns: 0,
               public_write_ns: 0
             }}
        end
    end
  end

  defp read_journal do
    {:ok,
     [
       [
         phase,
         series_index,
         cursor_json,
         records_completed,
         records_total,
         series_completed,
         series_total,
         identity_digest,
         checkpoints,
         retries
       ]
     ]} =
      DB.read(
        db_name(),
        """
        SELECT phase,series_index,cursor_json,records_completed,records_total,
               series_completed,series_total,identity_digest,checkpoints,retries
        FROM _timeless_migration WHERE singleton=1
        """,
        []
      )

    {:ok,
     %{
       phase: phase,
       series_index: series_index,
       cursor: decode_cursor(cursor_json),
       records_completed: records_completed,
       records_total: records_total,
       series_completed: series_completed,
       series_total: series_total,
       identity_digest: identity_digest,
       checkpoints: checkpoints,
       retries: retries,
       source_scan_ns: 0,
       public_write_ns: 0
     }}
  end

  # One fsync'd checkpoint transaction per point-less series turns a
  # high-cardinality junk registry into hours of wall clock (issue #3),
  # so runs of fully-empty series coalesce into one batched checkpoint.
  # The identity digest is a commutative sum and 'resolve' is idempotent,
  # so resume after a crash replays the open batch harmlessly.
  @empty_batch_size 1_024

  defp copy_series(reader, series, journal, opts) do
    series
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, Map.put(journal, :pending, [])}, fn {identity, index},
                                                                   {:ok, state} ->
      cond do
        index < state.series_index ->
          {:cont, {:ok, state}}

        index > state.series_index ->
          {:halt, {:error, "migration journal skipped metrics series #{state.series_index}"}}

        true ->
          case copy_one_series(reader, identity, index, state, opts) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, _} = error -> {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, state} ->
        with {:ok, state} <- flush_pending(state, opts) do
          {:ok, Map.delete(state, :pending)}
        end

      other ->
        other
    end
  end

  defp flush_pending(%{pending: []} = state, _opts), do: {:ok, state}

  defp flush_pending(%{pending: pending} = state, opts) do
    checkpoint_number = state.checkpoints + 1
    failpoint = selected_failpoint(opts[:failpoint], checkpoint_number)

    if failpoint == :before_batch, do: throw({:migration_failpoint, :before_batch})

    write_started = System.monotonic_time(:nanosecond)

    with :ok <-
           LibsqlEngine.migration_resolve_batch(
             @migration_store,
             Enum.reverse(pending),
             journal_params(state),
             failpoint: failpoint
           ) do
      state = %{
        state
        | pending: [],
          checkpoints: checkpoint_number,
          public_write_ns:
            state.public_write_ns + System.monotonic_time(:nanosecond) - write_started
      }

      observe_hwm(candidate_path_from_store())

      if failpoint == :after_checkpoint,
        do: throw({:migration_failpoint, :after_checkpoint})

      {:ok, state}
    end
  end

  defp copy_one_series(reader, {metric, labels} = identity, index, state, opts) do
    scan_started = System.monotonic_time(:nanosecond)

    case LegacyReader.page(reader, metric, labels, state.cursor, @page_size) do
      # A fully-empty series (no cursor, no data, no continuation): fold
      # its identity into the pending resolve batch instead of paying a
      # checkpoint transaction of its own.
      {:ok, [], _next_cursor, false} when state.cursor == nil ->
        scan_ns = System.monotonic_time(:nanosecond) - scan_started

        state =
          identity
          |> checkpoint_state(index, state, [], nil, false)
          |> Map.update!(:source_scan_ns, &(&1 + scan_ns))
          |> Map.update!(:pending, &[identity | &1])

        if length(state.pending) >= @empty_batch_size do
          flush_pending(state, opts)
        else
          {:ok, state}
        end

      {:ok, points, next_cursor, has_more?} ->
        scan_ns = System.monotonic_time(:nanosecond) - scan_started

        case flush_pending(state, opts) do
          {:ok, state} ->
            copy_series_pages(
              reader,
              identity,
              index,
              state,
              opts,
              points,
              next_cursor,
              has_more?,
              scan_ns
            )

          {:error, _} = error ->
            error
        end

      {:error, reason} ->
        {:error, "failed reading legacy metrics series #{metric} #{inspect(labels)}: #{reason}"}
    end
  end

  defp copy_series_pages(
         reader,
         identity,
         index,
         state,
         opts,
         points,
         next_cursor,
         has_more?,
         scan_ns
       ) do
    state =
      identity
      |> checkpoint_state(index, state, points, next_cursor, has_more?)
      |> Map.update!(:source_scan_ns, &(&1 + scan_ns))

    checkpoint_number = state.checkpoints + 1
    failpoint = selected_failpoint(opts[:failpoint], checkpoint_number)

    if failpoint == :before_batch, do: throw({:migration_failpoint, :before_batch})

    write_started = System.monotonic_time(:nanosecond)

    with :ok <-
           LibsqlEngine.migration_checkpoint(
             @migration_store,
             identity,
             points,
             journal_params(state),
             failpoint: failpoint,
             final_page: not has_more?
           ) do
      committed = %{
        state
        | checkpoints: checkpoint_number,
          public_write_ns:
            state.public_write_ns + System.monotonic_time(:nanosecond) - write_started
      }

      observe_hwm(candidate_path_from_store())

      if failpoint == :after_checkpoint,
        do: throw({:migration_failpoint, :after_checkpoint})

      if has_more? do
        copy_one_series(reader, identity, index, committed, opts)
      else
        {:ok, committed}
      end
    end
  end

  defp checkpoint_state(identity, index, state, points, next_cursor, has_more?) do
    digest =
      Enum.reduce(points, state.identity_digest, fn {timestamp, value}, digest ->
        digest_add(digest, point_identity(identity, timestamp, value))
      end)

    {series_index, series_completed, cursor, digest} =
      if has_more? do
        {index, state.series_completed, next_cursor, digest}
      else
        digest = digest_add(digest, series_identity(identity))
        {index + 1, state.series_completed + 1, nil, digest}
      end

    %{
      state
      | phase: "copying",
        series_index: series_index,
        cursor: cursor,
        records_completed: state.records_completed + length(points),
        series_completed: series_completed,
        identity_digest: digest
    }
  end

  defp journal_params(state) do
    %{
      phase: state.phase,
      series_index: state.series_index,
      cursor_json: encode_cursor(state.cursor),
      records_completed: state.records_completed,
      series_completed: state.series_completed,
      identity_digest: state.identity_digest,
      updated_at_ns: System.system_time(:nanosecond)
    }
  end

  defp finish_public_maintenance do
    flush_started = System.monotonic_time(:nanosecond)

    with :ok <- LibsqlEngine.flush(@migration_store) do
      flush_ns = System.monotonic_time(:nanosecond) - flush_started
      compact_started = System.monotonic_time(:nanosecond)

      with {:ok, _removed, _files} <- LibsqlEngine.compact(@migration_store) do
        compact_ns = System.monotonic_time(:nanosecond) - compact_started
        rollup_started = System.monotonic_time(:nanosecond)

        with :ok <- LibsqlEngine.rollup(@migration_store) do
          rollup_ns = System.monotonic_time(:nanosecond) - rollup_started
          checkpoint_started = System.monotonic_time(:nanosecond)

          with {:ok, _} <- DB.write(db_name(), "PRAGMA wal_checkpoint(TRUNCATE)", []) do
            {:ok,
             %{
               flush_ns: flush_ns,
               compact_ns: compact_ns,
               rollup_ns: rollup_ns,
               checkpoint_ns: System.monotonic_time(:nanosecond) - checkpoint_started
             }}
          end
        end
      end
    end
  end

  defp mark_phase(phase) do
    now = System.system_time(:nanosecond)

    case DB.write_transaction(db_name(), fn conn ->
           {:ok, _} =
             DB.execute(
               conn,
               "UPDATE _timeless_migration SET phase=?1,updated_at_ns=?2 WHERE singleton=1",
               [phase, now]
             )

           {:ok, [[series_index, cursor, records]]} =
             DB.execute(
               conn,
               "SELECT series_index,cursor_json,records_completed FROM _timeless_migration WHERE singleton=1",
               []
             )

           {:ok, _} =
             DB.execute(
               conn,
               "INSERT INTO _timeless_migration_events(phase,series_index,cursor_json,records_completed,at_ns) VALUES (?1,?2,?3,?4,?5)",
               [phase, series_index, cursor, records, now]
             )
         end) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, Exception.message(error)}
    end
  end

  defp stop_candidate(supervisor) do
    Supervisor.stop(supervisor)
    :ok
  end

  defp cold_validate(candidate_db, series, expected_points, expected_digest, opts) do
    conn =
      LibsqlEngine.open_readonly_connection(
        candidate_db,
        Keyword.get(opts, :extension_path)
      )

    try do
      with {:ok, [["ok"]]} <- DB.execute(conn, "PRAGMA integrity_check", []),
           {:ok, actual} <- target_digest(conn, series),
           {:ok, [[rollup_chunks]]} <-
             DB.execute(
               conn,
               "SELECT CAST(value AS INTEGER) FROM timeless_stats('metric_samples') WHERE key='rollup_chunks'",
               []
             ) do
        # A valid non-empty source can contain no completed rollup bucket (for
        # example, seventeen seconds inside the first hourly bucket). Exact
        # rollup aggregates are pinned by the threshold-crossing fixture; do
        # not invent a chunk requirement for a range the public rollup command
        # correctly leaves unsettled.
        if actual.points == expected_points and actual.series == length(series) and
             actual.digest == expected_digest do
          {:ok, Map.put(actual, :rollup_chunks, rollup_chunks)}
        else
          {:error,
           "cold metrics parity mismatch: expected points=#{expected_points} series=#{length(series)} digest=#{expected_digest}; " <>
             "actual points=#{actual.points} series=#{actual.series} digest=#{actual.digest} " <>
             "rollup_chunks=#{rollup_chunks}"}
        end
      else
        other -> {:error, "cold metrics validation failed: #{inspect(other)}"}
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp target_digest(conn, series) do
    with {:ok, catalog} <- target_catalog(conn) do
      Enum.reduce_while(series, {:ok, %{digest: zero_digest(), points: 0, series: 0}}, fn
        {metric, labels} = identity, {:ok, acc} ->
          labels_json = canonical_json(labels)

          with {:ok, series_id} <- Map.fetch(catalog, {metric, labels_json}),
               {:ok, point_state} <- stream_target_points(conn, metric, series_id, identity, acc) do
            {:cont,
             {:ok,
              %{
                point_state
                | digest: digest_add(point_state.digest, series_identity(identity)),
                  series: point_state.series + 1
              }}}
          else
            :error ->
              {:halt,
               {:error,
                "target series mismatch for #{metric} #{labels_json}: absent from candidate catalog"}}

            other ->
              {:halt,
               {:error, "target series mismatch for #{metric} #{labels_json}: #{inspect(other)}"}}
          end
      end)
    end
  end

  # One catalog scan shared by the whole validation pass. A per-series
  # `WHERE name=? AND labels=?` probe cannot be pushed into the TVF (those
  # are output columns, not hidden arguments), so each probe materializes
  # every catalog row — O(series²) across the loop, which on a
  # high-cardinality store ran for days without finishing.
  defp target_catalog(conn) do
    with {:ok, rows} <-
           DB.execute(
             conn,
             "SELECT series_id, name, labels FROM timeless_series('metric_samples')",
             []
           ) do
      catalog =
        Map.new(rows, fn [series_id, name, labels_json] -> {{name, labels_json}, series_id} end)

      if map_size(catalog) == length(rows) do
        {:ok, catalog}
      else
        {:error, "candidate catalog repeats a (name, labels) identity"}
      end
    end
  end

  defp stream_target_points(conn, metric, series_id, identity, state) do
    sql =
      "SELECT points FROM timeless_raw_batches('metric_samples', ?1, NULL, ?2, ?3) " <>
        "WHERE series_id=?4"

    with {:ok, statement} <- Exqlite.Sqlite3.prepare(conn, sql),
         :ok <-
           Exqlite.Sqlite3.bind(statement, [
             metric,
             -9_223_372_036_854_775_808,
             9_223_372_036_854_775_807,
             series_id
           ]) do
      try do
        stream_target_statement(conn, statement, identity, state)
      after
        Exqlite.Sqlite3.release(conn, statement)
      end
    else
      {:error, reason} -> {:error, "prepare target batch stream: #{inspect(reason)}"}
    end
  end

  defp stream_target_statement(conn, statement, identity, state) do
    case Exqlite.Sqlite3.step(conn, statement) do
      {:row, [blob]} ->
        points = LibsqlEngine.decode_point_batch(blob)

        state =
          Enum.reduce(points, state, fn {timestamp, value}, acc ->
            %{
              acc
              | digest: digest_add(acc.digest, point_identity(identity, timestamp, value)),
                points: acc.points + 1
            }
          end)

        stream_target_statement(conn, statement, identity, state)

      :done ->
        {:ok, state}

      {:error, reason} ->
        {:error, "stream target batch: #{inspect(reason)}"}
    end
  end

  defp finish_report(candidate_db, manifest, state, validation, maintenance, started, opts) do
    observe_hwm(candidate_db)
    observed = :persistent_term.get({__MODULE__, :observed})
    conn = LibsqlEngine.open_connection(candidate_db, Keyword.get(opts, :extension_path))
    now = System.system_time(:nanosecond)

    try do
      {:ok, _} =
        DB.execute(
          conn,
          "UPDATE _timeless_migration SET phase='verified',updated_at_ns=?1 WHERE singleton=1",
          [now]
        )

      {:ok, [[0, _frames, _checkpointed]]} =
        DB.execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

      {:ok, [[pages, page_size, freelist]]} =
        DB.execute(
          conn,
          "SELECT (SELECT page_count FROM pragma_page_count), (SELECT page_size FROM pragma_page_size), (SELECT freelist_count FROM pragma_freelist_count)",
          []
        )

      elapsed_ns = max(System.monotonic_time(:nanosecond) - started, 1)

      {:ok,
       %{
         signal: @signal,
         phase: :verified,
         candidate: candidate_db,
         source_manifest_digest: manifest.digest,
         source_bytes: manifest.bytes,
         series: validation.series,
         points: validation.points,
         checkpoints: state.checkpoints,
         retries: state.retries,
         identity_digest: validation.digest,
         rollup_chunks: validation.rollup_chunks,
         source_scan_ns: state.source_scan_ns,
         public_write_ns: state.public_write_ns,
         flush_ns: maintenance.flush_ns,
         compact_ns: maintenance.compact_ns,
         rollup_ns: maintenance.rollup_ns,
         checkpoint_ns: maintenance.checkpoint_ns,
         durable_points_per_second: validation.points * 1_000_000_000 / elapsed_ns,
         elapsed_ns: elapsed_ns,
         candidate_bytes: file_size(candidate_db),
         wal_bytes: file_size(candidate_db <> "-wal"),
         physical_bytes:
           file_size(candidate_db) + file_size(candidate_db <> "-wal") +
             file_size(candidate_db <> "-shm"),
         migration_rss_baseline_bytes: observed.rss_baseline,
         migration_rss_hwm_bytes: observed.rss_hwm,
         migration_rss_delta_bytes: max(observed.rss_hwm - observed.rss_baseline, 0),
         candidate_peak_physical_bytes: observed.candidate_peak_bytes,
         sqlite_logical_bytes: pages * page_size,
         sqlite_freelist_bytes: freelist * page_size,
         process_hwm_bytes: process_hwm_bytes()
       }}
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp source_manifest(root, paths) do
    files =
      paths
      |> Enum.flat_map(&regular_files/1)
      |> Enum.sort()
      |> Enum.map(fn path ->
        stat = File.stat!(path, time: :posix)

        %{
          path: Path.relative_to(path, root),
          size: stat.size,
          mtime: stat.mtime,
          sha256: sha256_file(path)
        }
      end)

    json = canonical_json(%{version: 1, signal: @signal, files: files})
    {:ok, %{files: files, json: json, digest: sha256(json), bytes: Enum.sum_by(files, & &1.size)}}
  rescue
    error -> {:error, "failed to inventory legacy metrics source: #{Exception.message(error)}"}
  end

  # `:json.encode` writes atom-keyed maps in atom-table order, which differs
  # between VM instances, so the stored digest only matches a journal written
  # by the same build. A resume under a new release must compare the decoded
  # manifests instead: JSON objects decode to binary-keyed maps, whose
  # equality is order-independent.
  defp manifest_matches?(stored_json, stored_digest, manifest) do
    stored_digest == manifest.digest or
      try do
        :json.decode(stored_json) == :json.decode(manifest.json)
      rescue
        _ -> false
      end
  end

  defp verify_manifest(root, paths, expected) do
    case source_manifest(root, paths) do
      {:ok, %{digest: digest}} when digest == expected.digest -> :ok
      {:ok, _} -> {:error, "legacy metrics source changed during migration"}
      {:error, _} = error -> error
    end
  end

  defp regular_files(path) do
    cond do
      File.regular?(path) -> [path]
      File.dir?(path) -> path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
      true -> raise "legacy source contains non-regular path #{path}"
    end
  end

  defp preflight_disk(candidate_dir, source_bytes, opts) do
    parent = Path.dirname(candidate_dir)
    File.mkdir_p!(parent)

    required =
      max(source_bytes * 2, @minimum_headroom) + div(max(source_bytes * 2, @minimum_headroom), 4)

    available = Keyword.get_lazy(opts, :available_bytes, fn -> available_bytes(parent) end)

    if available >= required do
      :ok
    else
      {:error,
       "insufficient disk for metrics migration: require #{required} bytes including WAL and 25% safety margin; #{available} bytes available"}
    end
  end

  defp available_bytes(path) do
    case System.cmd("df", ["-Pk", path], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split("\n", trim: true)
        |> List.last()
        |> String.split(~r/\s+/, trim: true)
        |> Enum.at(3)
        |> String.to_integer()
        |> Kernel.*(1_024)

      {output, status} ->
        raise "cannot determine free space (df exit #{status}): #{String.trim(output)}"
    end
  end

  defp selected_failpoint({point, checkpoint}, checkpoint), do: point
  defp selected_failpoint(point, _checkpoint) when is_atom(point), do: point
  defp selected_failpoint(_, _checkpoint), do: nil

  defp encode_cursor(nil), do: nil

  defp encode_cursor({timestamp, value_bits, path, data_offset, ordinal}) do
    canonical_json(%{
      timestamp: timestamp,
      value_bits: Integer.to_string(value_bits),
      path: path,
      data_offset: data_offset,
      ordinal: ordinal
    })
  end

  defp decode_cursor(nil), do: nil

  defp decode_cursor(json) do
    cursor = :json.decode(json)

    {
      map_value(cursor, "timestamp"),
      cursor |> map_value("value_bits") |> String.to_integer(),
      map_value(cursor, "path"),
      map_value(cursor, "data_offset"),
      map_value(cursor, "ordinal")
    }
  end

  defp point_identity({metric, labels}, timestamp, value) do
    [
      series_identity({metric, labels}),
      <<timestamp::signed-big-64, value_bits(value)::unsigned-big-64>>
    ]
  end

  defp series_identity({metric, labels}) do
    labels = canonical_json(labels)

    [
      "series\0",
      <<byte_size(metric)::unsigned-big-32>>,
      metric,
      <<byte_size(labels)::unsigned-big-32>>,
      labels
    ]
  end

  defp digest_add(digest, identity) do
    current = digest |> Base.decode16!(case: :mixed) |> :binary.decode_unsigned()

    addition =
      identity
      |> IO.iodata_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> :binary.decode_unsigned()

    encoded = rem(current + addition, @digest_modulus) |> :binary.encode_unsigned()
    padded = :binary.copy(<<0>>, 32 - byte_size(encoded)) <> encoded
    Base.encode16(padded, case: :lower)
  end

  defp zero_digest, do: String.duplicate("0", 64)
  defp value_bits(value), do: :binary.decode_unsigned(<<value * 1.0::float-big-64>>)
  defp canonical_json(value), do: value |> :json.encode() |> IO.iodata_to_binary()
  defp sha256(data), do: :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)

  defp sha256_file(path) do
    File.open!(path, [:read, :binary], fn io -> hash_io(:crypto.hash_init(:sha256), io) end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp hash_io(context, io) do
    case IO.binread(io, 1_048_576) do
      :eof -> context
      data when is_binary(data) -> hash_io(:crypto.hash_update(context, data), io)
      {:error, reason} -> raise "read source: #{inspect(reason)}"
    end
  end

  defp process_hwm_bytes do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        case Regex.run(~r/^VmHWM:\s+(\d+)\s+kB$/m, status) do
          [_, kib] -> String.to_integer(kib) * 1_024
          _ -> :erlang.memory(:total)
        end

      _ ->
        :erlang.memory(:total)
    end
  end

  defp observe_hwm(candidate_db) do
    current = process_rss_bytes()
    prior = :persistent_term.get({__MODULE__, :observed})

    :persistent_term.put({__MODULE__, :observed}, %{
      prior
      | rss_hwm: max(prior.rss_hwm, current),
        candidate_peak_bytes: max(prior.candidate_peak_bytes, physical_size(candidate_db))
    })
  end

  defp start_observation(candidate_db) do
    baseline = process_rss_bytes()

    :persistent_term.put(
      {__MODULE__, :observed},
      %{
        rss_baseline: baseline,
        rss_hwm: baseline,
        candidate_peak_bytes: physical_size(candidate_db)
      }
    )
  end

  defp process_rss_bytes do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        case Regex.run(~r/^VmRSS:\s+(\d+)\s+kB$/m, status) do
          [_, kib] -> String.to_integer(kib) * 1_024
          _ -> :erlang.memory(:total)
        end

      _ ->
        :erlang.memory(:total)
    end
  end

  defp physical_size(path),
    do: file_size(path) + file_size(path <> "-wal") + file_size(path <> "-shm")

  defp candidate_path_from_store do
    DB.db_path(db_name())
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  defp map_value(map, key), do: Map.get(map, key, Map.get(map, String.to_atom(key)))
  defp db_name, do: :"#{@migration_store}_db"
end
