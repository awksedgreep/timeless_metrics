defmodule TimelessMetrics.ReleaseStartup do
  @moduledoc false

  alias TimelessMetrics.{DB, LegacyReader, LibsqlEngine, ReleaseMigration, Schema}

  @signal "metrics"
  @primary_name "metrics.db"
  @cutover_name "metrics.libsql.db"
  @journal_version 1
  @cutover_version 1
  @data_schema_version 1
  @sqlite_header "SQLite format 3\0"

  @type state ::
          :fresh
          | :valid_libsql
          | :legacy
          | :resumable_migration
          | :completed_cutover
          | :incompatible_version
          | :corruption
          | :ambiguous_dual_store

  @doc "Inspect storage without creating, upgrading, recovering, or selecting a store."
  @spec detect(String.t(), keyword()) :: {:ok, map()}
  def detect(data_dir, opts \\ []) do
    root = Path.expand(data_dir)
    primary_path = Path.join(root, @primary_name)
    cutover_path = Path.join(root, @cutover_name)
    candidate_path = ReleaseMigration.candidate_path(root)

    with {:ok, primary} <- inspect_primary(primary_path, opts),
         {:ok, cutover_target} <- inspect_database(cutover_path, :target, opts),
         {:ok, candidate} <- inspect_database(candidate_path, :candidate, opts),
         {:ok, target, target_path, legacy_admin?} <-
           select_target(primary, primary_path, cutover_target, cutover_path),
         {:ok, legacy} <- inspect_legacy(root, legacy_admin?),
         :ok <- validate_candidate_checkpoint(candidate, candidate_path, legacy, opts) do
      combine(root, target_path, candidate_path, target, candidate, legacy)
    else
      {:state, state, detail} -> state_result(root, state, detail)
    end
  rescue
    error -> state_result(Path.expand(data_dir), :corruption, Exception.message(error))
  end

  defp validate_candidate_checkpoint(:absent, _path, _legacy, _opts), do: :ok
  defp validate_candidate_checkpoint(_candidate, _path, :absent, _opts), do: :ok

  defp validate_candidate_checkpoint(_candidate, path, legacy, opts) do
    case ReleaseMigration.validate_checkpoint(path, legacy.series, opts) do
      :ok ->
        :ok

      {:error, reason} ->
        {:state, :corruption,
         %{error: "durable metrics migration checkpoint failed semantic validation: #{reason}"}}
    end
  end

  @doc "Acquire exclusive ownership and make one validated libSQL target ready."
  @spec prepare(String.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def prepare(data_dir, opts \\ []) do
    root = Path.expand(data_dir)
    File.mkdir_p!(root)

    case acquire_owner(root) do
      {:ok, owner} ->
        try do
          with :ok <- require_extension_capability(opts),
               :ok <- legacy_owner_available(root),
               {:ok, detected} <- detect(root, opts) do
            run_detected(root, detected, opts)
          else
            {:error, reason} -> failure(root, :corruption, reason)
            {:state, state, detail} -> failure_detail(root, state, detail)
          end
        catch
          {:startup_failpoint, point} ->
            failpoint_failure(root, opts, point)
        after
          release_owner(owner)
        end

      {:error, reason} ->
        failure(root, :corruption, "metrics storage owner is active: #{reason}")
    end
  end

  @doc "Return readiness plus durable migration journal progress."
  def stats(data_dir, opts \\ []) do
    root = Path.expand(data_dir)
    {:ok, detected} = detect(root, opts)

    progress =
      case detected.state do
        :resumable_migration -> journal_progress(detected.candidate_path)
        _ -> %{}
      end

    Map.merge(
      %{
        signal: @signal,
        state: detected.state,
        ready: detected.state in [:valid_libsql, :completed_cutover],
        target_path: Map.get(detected, :target_path),
        source_manifest_digest: Map.get(detected, :source_manifest_digest),
        records_total: Map.get(detected, :records_total),
        error: Map.get(detected, :error)
      },
      progress
    )
  end

  @doc "Delete only the verified retained source after an explicit digest-confirmed request."
  def cleanup_legacy(data_dir, expected_digest, opts \\ []) when is_binary(expected_digest) do
    root = Path.expand(data_dir)

    case acquire_owner(root) do
      {:ok, owner} ->
        try do
          with :ok <- legacy_owner_available(root) do
            cleanup_verified_source(
              root,
              Path.join(root, @cutover_name),
              expected_digest,
              opts
            )
          end
        catch
          {:startup_failpoint, point} ->
            {:error,
             "explicit metrics cleanup interrupted at #{inspect(point)}; rerun with the same digest"}
        after
          release_owner(owner)
        end

      {:error, reason} ->
        {:error, "metrics storage owner is active: #{reason}"}
    end
  end

  defp run_detected(_root, %{state: state} = detected, _opts)
       when state in [:valid_libsql, :completed_cutover],
       do: {:ok, Map.put(detected, :ready, true)}

  defp run_detected(root, %{state: :fresh}, opts) do
    target = Path.join(root, @primary_name)
    failpoint!(opts, :before_fresh_create)

    with :ok <- create_fresh_target(target, opts),
         :ok <- failpoint_result(opts, :after_fresh_create),
         :ok <- sync_file(target),
         :ok <- sync_directory(root) do
      ready_after_action(root, opts)
    else
      {:error, reason} -> failure(root, :fresh, inspect(reason))
    end
  end

  defp run_detected(root, %{state: state} = detected, opts)
       when state in [:legacy, :resumable_migration] do
    with {:ok, source_owner} <- acquire_legacy_source(root) do
      try do
        with {:ok, report} <- ReleaseMigration.stage(root, opts),
             :ok <- failpoint_result(opts, :before_seal),
             {:ok, seal} <- seal_candidate(root, report),
             :ok <- failpoint_result(opts, :after_seal),
             :ok <- cutover(root, seal, detected.target_path, opts) do
          ready_after_action(root, opts)
        else
          {:error, reason} when is_binary(reason) -> failure(root, state, reason)
          {:error, %{state: _} = error} -> {:error, error}
          {:error, reason} -> failure(root, state, inspect(reason))
        end
      after
        release_legacy_source(source_owner)
      end
    else
      {:error, reason} -> failure(root, state, reason)
    end
  end

  defp run_detected(_root, %{state: state} = detected, _opts)
       when state in [:incompatible_version, :corruption, :ambiguous_dual_store],
       do: {:error, Map.put(detected, :ready, false)}

  defp create_fresh_target(target, opts) do
    LibsqlEngine.initialize_release_database(
      target,
      Keyword.get(opts, :schema, Schema.default()),
      extension_path(opts)
    )
  end

  defp ready_after_action(root, opts) do
    case detect(root, opts) do
      {:ok, %{state: state} = detected}
      when state in [:valid_libsql, :completed_cutover] ->
        {:ok, Map.put(detected, :ready, true)}

      {:ok, detected} ->
        failure(root, detected.state, "startup action ended in #{detected.state}")
    end
  end

  defp failpoint_failure(root, opts, point) do
    case detect(root, opts) do
      {:ok, detected} ->
        failure(root, detected.state, "injected startup failure at #{point}")

      _ ->
        failure(root, :corruption, "injected startup failure at #{point}; state recheck failed")
    end
  end

  defp combine(root, target_path, candidate_path, target, candidate, legacy) do
    cond do
      target != :absent and candidate != :absent ->
        state_result(root, :ambiguous_dual_store, %{
          error: "canonical metrics target and migration candidate both exist",
          target_path: target_path,
          candidate_path: candidate_path
        })

      candidate != :absent and legacy == :absent ->
        state_result(root, :corruption, %{
          error: "metrics migration candidate has no retained legacy source",
          candidate_path: candidate_path
        })

      candidate != :absent and candidate.source_manifest_digest != legacy.manifest.digest ->
        state_result(root, :corruption, %{
          error: "legacy metrics source fingerprint drifted from the migration journal",
          candidate_path: candidate_path,
          expected_source_manifest_digest: candidate.source_manifest_digest,
          source_manifest_digest: legacy.manifest.digest
        })

      candidate != :absent ->
        state_result(root, :resumable_migration, %{
          candidate_path: candidate_path,
          target_path: target_path,
          records_total: legacy.inventory.info["points"],
          source_manifest_digest: legacy.manifest.digest,
          generation: legacy.generation
        })

      target == :absent and legacy == :absent ->
        state_result(root, :fresh, %{target_path: target_path})

      target == :absent ->
        state_result(root, :legacy, %{
          target_path: target_path,
          records_total: legacy.inventory.info["points"],
          source_manifest_digest: legacy.manifest.digest,
          generation: legacy.generation
        })

      legacy == :absent and target.cutover == nil ->
        state_result(root, :valid_libsql, %{target_path: target_path})

      legacy == :absent and target.cutover.source_retained == 0 ->
        state_result(root, :completed_cutover, %{
          target_path: target_path,
          cutover_generation: target.cutover.generation,
          source_retained: false
        })

      legacy == :absent ->
        state_result(root, :corruption, %{
          error: "verified metrics cutover is missing its retained rollback source",
          target_path: target_path
        })

      target.cutover == nil ->
        state_result(root, :ambiguous_dual_store, %{
          error: "unlinked legacy metrics source exists beside a valid libSQL target",
          target_path: target_path
        })

      target.cutover.source_manifest_digest != legacy.manifest.digest ->
        state_result(root, :corruption, %{
          error: "retained metrics source does not match the verified cutover manifest",
          target_path: target_path,
          expected_source_manifest_digest: target.cutover.source_manifest_digest,
          source_manifest_digest: legacy.manifest.digest
        })

      true ->
        state_result(root, :completed_cutover, %{
          target_path: target_path,
          cutover_generation: target.cutover.generation,
          source_manifest_digest: legacy.manifest.digest,
          source_retained: true
        })
    end
  end

  defp inspect_primary(path, opts) do
    case path_kind(path) do
      :absent ->
        {:ok, :absent}

      {:error, reason} ->
        {:state, :corruption, %{error: reason, path: path}}

      :regular ->
        with :ok <- require_sqlite_header(path),
             {:ok, conn} <- Exqlite.Sqlite3.open(path, mode: :readonly) do
          try do
            with :ok <- load_extension(conn, extension_path(opts)),
                 {:ok, [["ok"]]} <- execute(conn, "PRAGMA integrity_check"),
                 {:ok, target?} <- table_exists?(conn, "metric_samples") do
              if target? do
                with :ok <- require_signal_vtable(conn),
                     :ok <- require_schema_version(conn),
                     {:ok, target} <- inspect_role(conn, :target) do
                  {:ok, {:target, target}}
                end
              else
                inspect_legacy_admin(conn)
              end
            else
              {:state, _, _} = state ->
                state

              {:error, reason} ->
                {:state, :corruption,
                 %{error: "cannot validate metrics.db: #{inspect(reason)}", path: path}}

              other ->
                {:state, :corruption,
                 %{error: "invalid metrics.db: #{inspect(other)}", path: path}}
            end
          after
            Exqlite.Sqlite3.close(conn)
          end
        else
          {:state, _, _} = state ->
            state

          {:error, reason} ->
            {:state, :corruption,
             %{error: "cannot open metrics.db: #{inspect(reason)}", path: path}}
        end
    end
  end

  defp inspect_legacy_admin(conn) do
    with {:ok, true} <- table_exists?(conn, "series"),
         {:ok, true} <- table_exists?(conn, "_metadata"),
         {:ok, [[version]]} <-
           execute(
             conn,
             "SELECT CAST(value AS INTEGER) FROM _metadata WHERE key='schema_version'"
           ) do
      max_version = TimelessMetrics.DB.Migrations.current_version()

      cond do
        version > max_version ->
          {:state, :incompatible_version,
           %{error: "legacy metrics schema #{version} is newer than supported #{max_version}"}}

        version < 1 ->
          {:state, :incompatible_version,
           %{
             error:
               "legacy metrics schema #{version} is too old; supported versions are 1 through #{max_version}"
           }}

        true ->
          {:ok, :legacy_admin}
      end
    else
      {:ok, false} ->
        {:state, :corruption,
         %{
           error: "metrics.db is neither a timeless_metrics vtab nor a recognized legacy database"
         }}

      other ->
        {:state, :corruption, %{error: "invalid legacy metrics database: #{inspect(other)}"}}
    end
  end

  defp select_target({:target, _}, _primary_path, target, _cutover_path)
       when target != :absent do
    {:state, :ambiguous_dual_store,
     %{error: "both metrics.db and metrics.libsql.db are plausible active libSQL targets"}}
  end

  defp select_target({:target, target}, primary_path, :absent, _cutover_path),
    do: {:ok, target, primary_path, false}

  defp select_target(:legacy_admin, _primary_path, target, cutover_path),
    do: {:ok, target, cutover_path, true}

  defp select_target(:absent, _primary_path, target, cutover_path) when target != :absent,
    do: {:ok, target, cutover_path, false}

  defp select_target(:absent, primary_path, :absent, _cutover_path),
    do: {:ok, :absent, primary_path, false}

  defp inspect_database(path, _role, _opts) when not is_binary(path), do: {:ok, :absent}

  defp inspect_database(path, role, opts) do
    case path_kind(path) do
      :absent ->
        {:ok, :absent}

      {:error, reason} ->
        {:state, :corruption, %{error: reason, path: path}}

      :regular ->
        with :ok <- require_sqlite_header(path),
             {:ok, conn} <- Exqlite.Sqlite3.open(path, mode: :readonly) do
          try do
            with :ok <- load_extension(conn, extension_path(opts)),
                 {:ok, [["ok"]]} <- execute(conn, "PRAGMA integrity_check"),
                 :ok <- require_signal_vtable(conn),
                 :ok <- require_schema_version(conn),
                 result <- inspect_role(conn, role) do
              result
            else
              {:state, _, _} = state ->
                state

              {:error, reason} ->
                {:state, :corruption,
                 %{
                   error: "cannot validate #{role} metrics database: #{inspect(reason)}",
                   path: path
                 }}

              other ->
                {:state, :corruption,
                 %{error: "invalid #{role} metrics database: #{inspect(other)}", path: path}}
            end
          after
            Exqlite.Sqlite3.close(conn)
          end
        else
          {:state, _, _} = state ->
            state

          {:error, reason} ->
            {:state, :corruption,
             %{error: "cannot open #{role} metrics database: #{inspect(reason)}", path: path}}
        end
    end
  end

  defp inspect_role(conn, :target) do
    with {:ok, exists} <- table_exists?(conn, "_timeless_cutover") do
      if exists do
        case execute(
               conn,
               "SELECT version,signal,generation,source_manifest_digest,source_retained FROM _timeless_cutover WHERE singleton=1"
             ) do
          {:ok, [[version, @signal, generation, digest, retained]]}
          when version == @cutover_version and retained in [0, 1] ->
            {:ok,
             %{
               cutover: %{
                 generation: generation,
                 source_manifest_digest: digest,
                 source_retained: retained
               }
             }}

          {:ok, [[version, _, _, _, _]]} when version > @cutover_version ->
            {:state, :incompatible_version,
             %{
               error:
                 "metrics cutover version #{version} is newer than supported #{@cutover_version}"
             }}

          other ->
            {:state, :corruption, %{error: "invalid metrics cutover record: #{inspect(other)}"}}
        end
      else
        with {:ok, migration?} <- table_exists?(conn, "_timeless_migration") do
          if migration? do
            {:state, :corruption,
             %{error: "canonical metrics target contains an unsealed migration journal"}}
          else
            {:ok, %{cutover: nil}}
          end
        end
      end
    end
  end

  defp inspect_role(conn, :candidate) do
    with {:ok, true} <- table_exists?(conn, "_timeless_migration"),
         {:ok, [[version, signal, phase, digest]]} <-
           execute(
             conn,
             "SELECT version,signal,phase,source_manifest_digest FROM _timeless_migration WHERE singleton=1"
           ) do
      cond do
        version > @journal_version ->
          {:state, :incompatible_version,
           %{
             error:
               "metrics migration journal version #{version} is newer than supported #{@journal_version}"
           }}

        version != @journal_version ->
          {:state, :incompatible_version,
           %{error: "unsupported metrics migration journal version #{version}"}}

        signal != @signal ->
          {:state, :corruption, %{error: "migration candidate belongs to #{signal}"}}

        phase not in ["copying", "validating", "verified", "cutover_ready"] ->
          {:state, :corruption, %{error: "invalid metrics migration phase #{inspect(phase)}"}}

        true ->
          {:ok, %{phase: phase, source_manifest_digest: digest}}
      end
    else
      {:ok, false} ->
        {:state, :corruption, %{error: "metrics candidate is missing its migration journal"}}

      other ->
        {:state, :corruption, %{error: "invalid metrics migration journal: #{inspect(other)}"}}
    end
  end

  defp inspect_legacy(root, legacy_admin?) do
    engine_path = Path.join(root, "rust_engine")

    case File.lstat(engine_path) do
      {:error, :enoent} when legacy_admin? ->
        {:state, :corruption, %{error: "legacy metrics.db exists without its rust_engine store"}}

      {:error, :enoent} ->
        {:ok, :absent}

      {:ok, %{type: :symlink}} ->
        {:state, :corruption, %{error: "legacy metrics Rust store is a symlink: #{engine_path}"}}

      {:ok, %{type: type}} when type != :directory ->
        {:state, :corruption,
         %{error: "legacy metrics Rust store has invalid type #{type}: #{engine_path}"}}

      {:ok, %{type: :directory}} when not legacy_admin? ->
        {:state, :corruption,
         %{error: "rust_engine exists without its recognized legacy metrics.db"}}

      {:ok, %{type: :directory}} ->
        if File.ls!(engine_path) == [] do
          {:state, :corruption, %{error: "legacy metrics Rust store is empty: #{engine_path}"}}
        else
          with {:ok, reader} <- LegacyReader.open(engine_path),
               {:ok, series} <- LegacyReader.series(reader),
               {:ok, info} <- LegacyReader.info(reader),
               {:ok, manifest} <- ReleaseMigration.legacy_manifest(root) do
            {:ok,
             %{
               generation: :rust_block,
               series: series,
               inventory: %{series: length(series), info: info},
               manifest: manifest
             }}
          else
            {:error, reason} -> classify_legacy_error(reason)
          end
        end

      {:error, reason} ->
        {:state, :corruption,
         %{error: "cannot inspect legacy metrics Rust store: #{inspect(reason)}"}}
    end
  end

  defp classify_legacy_error(reason) do
    state =
      if String.contains?(to_string(reason), "unsupported"),
        do: :incompatible_version,
        else: :corruption

    {:state, state, %{error: to_string(reason)}}
  end

  defp seal_candidate(root, report) do
    path = ReleaseMigration.candidate_path(root)

    with {:ok, conn} <- Exqlite.Sqlite3.open(path) do
      try do
        with {:ok, _} <- execute(conn, "BEGIN IMMEDIATE"),
             {:ok, [[phase, manifest_json, digest]]} <-
               execute(
                 conn,
                 "SELECT phase,source_manifest_json,source_manifest_digest FROM _timeless_migration WHERE singleton=1"
               ),
             true <- phase in ["verified", "cutover_ready"],
             true <- digest == report.source_manifest_digest,
             {:ok, _} <-
               execute(
                 conn,
                 """
                 CREATE TABLE IF NOT EXISTS _timeless_cutover(
                   singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   version INTEGER NOT NULL,
                   signal TEXT NOT NULL,
                   generation TEXT NOT NULL,
                   source_manifest_json TEXT NOT NULL,
                   source_manifest_digest TEXT NOT NULL,
                   source_retained INTEGER NOT NULL CHECK(source_retained IN (0,1)),
                   verified_at_ns INTEGER NOT NULL
                 ) STRICT
                 """
               ),
             generation = "metrics-" <> String.slice(digest, 0, 16),
             {:ok, _} <-
               execute(
                 conn,
                 "INSERT OR REPLACE INTO _timeless_cutover VALUES (1,?1,?2,?3,?4,?5,1,?6)",
                 [
                   @cutover_version,
                   @signal,
                   generation,
                   manifest_json,
                   digest,
                   System.system_time(:nanosecond)
                 ]
               ),
             {:ok, _} <-
               execute(
                 conn,
                 "UPDATE _timeless_migration SET phase='cutover_ready',updated_at_ns=?1 WHERE singleton=1",
                 [System.system_time(:nanosecond)]
               ),
             {:ok, _} <- execute(conn, "COMMIT"),
             {:ok, _} <- execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)") do
          {:ok, %{path: path, generation: generation, source_manifest_digest: digest}}
        else
          false -> rollback_error(conn, "candidate verification changed before cutover")
          {:error, reason} -> rollback_error(conn, inspect(reason))
          other -> rollback_error(conn, "invalid verified candidate: #{inspect(other)}")
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  end

  defp rollback_error(conn, reason) do
    _ = execute(conn, "ROLLBACK")
    {:error, reason}
  end

  defp cutover(root, seal, target, opts) do
    with :ok <- cutover_target_available(target),
         :ok <- same_filesystem(seal.path, root),
         :ok <- sync_file(seal.path),
         :ok <- failpoint_result(opts, :before_rename),
         :ok <- File.rename(seal.path, target),
         :ok <- failpoint_result(opts, :after_rename_before_fsync),
         :ok <- sync_directory(Path.dirname(seal.path)),
         :ok <- failpoint_result(opts, :after_source_parent_fsync),
         :ok <- sync_directory(root),
         :ok <- failpoint_result(opts, :after_fsync) do
      :ok
    else
      {:error, reason} -> {:error, "metrics atomic cutover failed: #{inspect(reason)}"}
    end
  end

  defp cutover_target_available(path) do
    case path_kind(path) do
      :absent -> :ok
      _ -> {:error, "canonical metrics target appeared before cutover"}
    end
  end

  defp require_signal_vtable(conn) do
    case execute(
           conn,
           "SELECT sql FROM sqlite_schema WHERE type='table' AND name='metric_samples'"
         ) do
      {:ok, [[sql]]} when is_binary(sql) ->
        if String.contains?(String.downcase(sql), "using timeless_metrics") do
          :ok
        else
          {:error, "metric_samples is not the timeless_metrics virtual table"}
        end

      {:ok, []} ->
        case execute(
               conn,
               "SELECT name,sql FROM sqlite_schema WHERE type='table' AND lower(sql) LIKE '%using timeless_%'"
             ) do
          {:ok, rows} when rows != [] -> {:error, "wrong-signal virtual table #{inspect(rows)}"}
          _ -> {:error, "missing timeless_metrics virtual table"}
        end

      other ->
        {:error, "cannot inspect metrics virtual table: #{inspect(other)}"}
    end
  end

  defp require_schema_version(conn) do
    with {:ok, exists} <- table_exists?(conn, "_timeless_schema_migrations") do
      if exists do
        case execute(
               conn,
               "SELECT version,extension_data_abi FROM _timeless_schema_migrations WHERE signal=?1 ORDER BY version DESC LIMIT 1",
               [@signal]
             ) do
          {:ok, []} ->
            :ok

          {:ok, [[version, _abi]]} when version > @data_schema_version ->
            {:state, :incompatible_version,
             %{
               error:
                 "metrics data schema #{version} is newer than supported #{@data_schema_version}"
             }}

          {:ok, [[_version, abi]]} when abi != 1 ->
            {:state, :incompatible_version,
             %{error: "metrics database data ABI #{abi} is incompatible with required ABI 1"}}

          {:ok, [[_version, 1]]} ->
            :ok

          other ->
            {:error, "invalid metrics schema ledger: #{inspect(other)}"}
        end
      else
        :ok
      end
    end
  end

  defp journal_progress(path) do
    with {:ok, conn} <- Exqlite.Sqlite3.open(path, mode: :readonly) do
      try do
        case execute(
               conn,
               "SELECT phase,cursor_json,records_completed,records_total,checkpoints,retries,started_at_ns,updated_at_ns,source_manifest_json FROM _timeless_migration WHERE singleton=1"
             ) do
          {:ok,
           [[phase, cursor, completed, total, checkpoints, retries, started, updated, manifest]]} ->
            elapsed_ns = max(updated - started, 1)
            rate = completed * 1_000_000_000 / elapsed_ns
            physical = file_size(path) + file_size(path <> "-wal") + file_size(path <> "-shm")

            %{
              phase: phase,
              cursor: cursor,
              records_completed: completed,
              records_total: total,
              percent: if(total == 0, do: 100.0, else: completed * 100.0 / total),
              checkpoints: checkpoints,
              retries: retries,
              last_checkpoint_ns: updated,
              durable_records_per_second: rate,
              eta_seconds: if(rate > 0, do: max(total - completed, 0) / rate, else: nil),
              source_bytes: manifest_bytes(manifest),
              candidate_physical_bytes: physical,
              wal_bytes: file_size(path <> "-wal"),
              process_hwm_bytes: process_hwm_bytes()
            }

          _ ->
            %{}
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    else
      _ -> %{}
    end
  end

  defp manifest_bytes(json) do
    case :json.decode(json) do
      %{"files" => files} when is_list(files) -> Enum.sum_by(files, &(&1["size"] || 0))
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
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

  defp cleanup_verified_source(root, target, expected_digest, opts) do
    with :regular <- path_kind(target),
         {:ok, conn} <- Exqlite.Sqlite3.open(target) do
      try do
        with {:ok, [[@cutover_version, @signal, manifest_json, ^expected_digest, retained]]} <-
               execute(
                 conn,
                 "SELECT version,signal,source_manifest_json,source_manifest_digest,source_retained FROM _timeless_cutover WHERE singleton=1"
               ),
             :ok <- delete_manifest_files(root, manifest_json, opts),
             {:ok, _} <- execute(conn, "BEGIN IMMEDIATE"),
             {:ok, _} <-
               execute(
                 conn,
                 "UPDATE _timeless_cutover SET source_retained=0 WHERE singleton=1 AND source_manifest_digest=?1",
                 [expected_digest]
               ),
             {:ok, _} <- execute(conn, "COMMIT"),
             {:ok, _} <- execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
             :ok <- sync_file(target),
             :ok <- sync_directory(root) do
          {:ok,
           %{
             signal: @signal,
             state: :completed_cutover,
             target_path: target,
             source_manifest_digest: expected_digest,
             source_retained: false,
             already_clean: retained == 0
           }}
        else
          {:ok, [[_version, _signal, _json, actual, _retained]]} ->
            {:error,
             "cleanup digest mismatch: requested #{expected_digest}, verified cutover is #{actual}"}

          other ->
            _ = execute(conn, "ROLLBACK")
            {:error, "cannot clean verified metrics source: #{inspect(other)}"}
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    else
      :absent -> {:error, "verified metrics target is missing: #{target}"}
      {:error, reason} -> {:error, reason}
      other -> {:error, "cannot open verified metrics target: #{inspect(other)}"}
    end
  end

  defp delete_manifest_files(root, manifest_json, opts) do
    with manifest when is_map(manifest) <- :json.decode(manifest_json),
         files when is_list(files) <- manifest["files"] do
      files
      |> Enum.with_index(1)
      |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, parents} ->
        with relative when is_binary(relative) <- entry["path"],
             {:ok, path} <- safe_source_path(root, relative),
             :ok <- delete_verified_file(path, entry) do
          failpoint!(opts, {:cleanup_after_file, index})
          {:cont, {:ok, [Path.dirname(path) | parents]}}
        else
          other -> {:halt, {:error, "cleanup source validation failed: #{inspect(other)}"}}
        end
      end)
      |> case do
        {:ok, parents} -> remove_empty_source_directories(root, parents)
        {:error, _} = error -> error
      end
    else
      other -> {:error, "invalid cutover source manifest: #{inspect(other)}"}
    end
  end

  defp safe_source_path(root, relative) do
    path = Path.expand(relative, root)
    back = Path.relative_to(path, root)

    if back == relative and back != "." and not String.starts_with?(back, "..") do
      {:ok, path}
    else
      {:error, "source manifest path escapes data directory: #{inspect(relative)}"}
    end
  end

  defp delete_verified_file(path, entry) do
    expected_size = entry["size"]

    case File.lstat(path) do
      {:error, :enoent} ->
        :ok

      {:ok, %{type: :regular, size: size}} when size == expected_size ->
        if sha256_file(path) == entry["sha256"] do
          File.rm(path)
        else
          {:error, "source hash changed before cleanup: #{path}"}
        end

      {:ok, %{type: :regular, size: size}} ->
        {:error, "source size changed before cleanup: #{path} (#{size})"}

      {:ok, %{type: type}} ->
        {:error, "source path changed type before cleanup: #{path} (#{type})"}

      {:error, reason} ->
        {:error, "cannot inspect cleanup source #{path}: #{inspect(reason)}"}
    end
  end

  defp remove_empty_source_directories(root, parents) do
    parents
    |> Enum.flat_map(&parents_below_root(&1, root, []))
    |> Enum.uniq()
    |> Enum.sort_by(&path_depth/1, :desc)
    |> Enum.each(fn path ->
      case File.rmdir(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, :eexist} -> :ok
        {:error, :enotempty} -> :ok
        {:error, _} -> :ok
      end
    end)

    :ok
  end

  defp parents_below_root(root, root, acc), do: acc

  defp parents_below_root(path, root, acc),
    do: parents_below_root(Path.dirname(path), root, [path | acc])

  defp path_depth(path), do: path |> Path.split() |> length()

  defp sha256_file(path) do
    File.open!(path, [:read, :binary], fn io -> hash_io(:crypto.hash_init(:sha256), io) end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp hash_io(context, io) do
    case IO.binread(io, 1_048_576) do
      :eof -> context
      data when is_binary(data) -> hash_io(:crypto.hash_update(context, data), io)
      {:error, reason} -> raise "read cleanup source: #{inspect(reason)}"
    end
  end

  defp acquire_owner(root) do
    owner_dir = Path.join([root, ".timeless-migration", @signal])
    File.mkdir_p!(owner_dir)
    path = Path.join(owner_dir, "owner.db")

    case Exqlite.Sqlite3.open(path) do
      {:ok, conn} ->
        result =
          with {:ok, _} <- execute_once(conn, "PRAGMA busy_timeout=0"),
               {:ok, _} <-
                 execute_once(
                   conn,
                   "CREATE TABLE IF NOT EXISTS owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1))"
                 ),
               {:ok, _} <- execute_once(conn, "BEGIN EXCLUSIVE") do
            {:ok, conn}
          end

        if match?({:error, _}, result), do: Exqlite.Sqlite3.close(conn)
        result

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp legacy_owner_available(root) do
    path = Path.join(root, "metrics.db")

    if path_kind(path) == :regular do
      case Exqlite.Sqlite3.open(path) do
        {:ok, conn} ->
          result =
            with {:ok, _} <- execute_once(conn, "PRAGMA busy_timeout=0"),
                 {:ok, _} <- execute_once(conn, "BEGIN EXCLUSIVE"),
                 {:ok, _} <- execute_once(conn, "ROLLBACK") do
              :ok
            else
              {:error, reason} ->
                {:error, "active legacy metrics SQLite owner or lock: #{inspect(reason)}"}
            end

          Exqlite.Sqlite3.close(conn)
          result

        {:error, reason} ->
          {:error, "cannot probe legacy metrics SQLite ownership: #{inspect(reason)}"}
      end
    else
      :ok
    end
  end

  defp acquire_legacy_source(root) do
    path = Path.join(root, "metrics.db")

    case Exqlite.Sqlite3.open(path) do
      {:ok, conn} ->
        result =
          with {:ok, _} <- execute_once(conn, "PRAGMA busy_timeout=0"),
               {:ok, _} <- execute_once(conn, "BEGIN EXCLUSIVE") do
            {:ok, conn}
          else
            {:error, reason} ->
              {:error, "cannot acquire exclusive legacy metrics ownership: #{inspect(reason)}"}
          end

        if match?({:error, _}, result), do: Exqlite.Sqlite3.close(conn)
        result

      {:error, reason} ->
        {:error, "cannot open legacy metrics database for ownership: #{inspect(reason)}"}
    end
  end

  defp release_legacy_source(conn) do
    _ = execute_once(conn, "ROLLBACK")
    Exqlite.Sqlite3.close(conn)
  end

  defp release_owner(conn) do
    _ = execute(conn, "ROLLBACK")
    Exqlite.Sqlite3.close(conn)
  end

  defp load_extension(conn, path) do
    with :ok <- Exqlite.Sqlite3.enable_load_extension(conn, true),
         {:ok, _} <- execute_once(conn, "SELECT load_extension(?1)", [path]),
         :ok <- Exqlite.Sqlite3.enable_load_extension(conn, false),
         {:ok, [[json]]} <- execute_once(conn, "SELECT timeless_capabilities()"),
         capabilities <- :json.decode(json),
         true <- capabilities["data_abi"] == 1,
         batches when is_list(batches) <- get_in(capabilities, ["signals", @signal, "batches"]),
         true <- "resolved-v1" in batches do
      :ok
    else
      other ->
        {:state, :incompatible_version,
         %{error: "extension capability handshake failed: #{inspect(other)}"}}
    end
  end

  defp require_extension_capability(opts) do
    with {:ok, conn} <- Exqlite.Sqlite3.open(":memory:") do
      try do
        load_extension(conn, extension_path(opts))
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  end

  defp extension_path(opts) do
    Keyword.get(opts, :extension_path) ||
      Application.get_env(:timeless_metrics, :extension_path) ||
      System.get_env("TIMELESS_EXT_PATH") ||
      Application.app_dir(:timeless_metrics, "priv/libtimeless_ext.so")
  end

  defp table_exists?(conn, name) do
    case execute(
           conn,
           "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
           [name]
         ) do
      {:ok, [[value]]} -> {:ok, value == 1}
      other -> {:error, other}
    end
  end

  defp require_sqlite_header(path) do
    case File.open(path, [:read, :binary], fn io -> IO.binread(io, byte_size(@sqlite_header)) end) do
      {:ok, @sqlite_header} ->
        :ok

      {:ok, other} ->
        {:state, :corruption, %{error: "invalid SQLite header #{inspect(other)}", path: path}}

      {:error, reason} ->
        {:state, :corruption, %{error: "cannot read #{path}: #{inspect(reason)}"}}
    end
  end

  defp path_kind(path) do
    case File.lstat(path) do
      {:error, :enoent} ->
        :absent

      {:ok, %{type: :regular, size: 0}} ->
        :absent

      {:ok, %{type: :regular}} ->
        :regular

      {:ok, %{type: :symlink}} ->
        {:error, "recognized storage path is a symlink: #{path}"}

      {:ok, %{type: type}} ->
        {:error, "recognized storage path is not a regular file: #{path} (#{type})"}

      {:error, reason} ->
        {:error, "cannot inspect #{path}: #{inspect(reason)}"}
    end
  end

  defp same_filesystem(path, directory) do
    with {:ok, source} <- File.stat(path),
         {:ok, target} <- File.stat(directory),
         true <-
           {source.major_device, source.minor_device} ==
             {target.major_device, target.minor_device} do
      :ok
    else
      false -> {:error, "candidate and target are on different filesystems"}
      other -> {:error, "cannot prove same-filesystem cutover: #{inspect(other)}"}
    end
  end

  defp sync_file(path) do
    with {:ok, io} <- :file.open(String.to_charlist(path), [:read, :raw]),
         :ok <- :file.sync(io),
         :ok <- :file.close(io) do
      :ok
    else
      other -> {:error, "fsync #{path}: #{inspect(other)}"}
    end
  end

  defp sync_directory(path) do
    with {:ok, io} <- :file.open(String.to_charlist(path), [:read, :raw, :directory]),
         :ok <- :file.sync(io),
         :ok <- :file.close(io) do
      :ok
    else
      other -> {:error, "fsync directory #{path}: #{inspect(other)}"}
    end
  end

  defp failpoint!(opts, point) do
    if Keyword.get(opts, :pause_at) == point do
      if notify = Keyword.get(opts, :notify), do: send(notify, {:startup_paused, self(), point})
      receive do: (:continue -> :ok)
    end

    if Keyword.get(opts, :failpoint) == point, do: throw({:startup_failpoint, point})
    :ok
  end

  defp failpoint_result(opts, point) do
    failpoint!(opts, point)
  end

  defp execute(conn, sql, params \\ []), do: DB.execute(conn, sql, params)

  defp execute_once(conn, sql, params \\ []) do
    with {:ok, statement} <- Exqlite.Sqlite3.prepare(conn, sql) do
      try do
        if params != [], do: :ok = Exqlite.Sqlite3.bind(statement, params)

        case Exqlite.Sqlite3.step(conn, statement) do
          :done -> {:ok, []}
          {:row, row} -> {:ok, [row]}
          {:error, _} = error -> error
          other -> {:error, other}
        end
      after
        Exqlite.Sqlite3.release(conn, statement)
      end
    end
  end

  defp state_result(root, state, detail) when is_binary(detail),
    do: state_result(root, state, %{error: detail})

  defp state_result(root, state, detail) do
    {:ok,
     detail
     |> Map.merge(%{signal: @signal, state: state, data_dir: root})
     |> Map.put_new(:ready, state in [:valid_libsql, :completed_cutover])}
  end

  defp failure(root, state, reason) do
    {:error,
     %{
       signal: @signal,
       state: state,
       data_dir: root,
       ready: false,
       error: reason
     }}
  end

  defp failure_detail(root, state, detail) do
    {:error,
     detail
     |> Map.merge(%{signal: @signal, state: state, data_dir: root, ready: false})}
  end
end
