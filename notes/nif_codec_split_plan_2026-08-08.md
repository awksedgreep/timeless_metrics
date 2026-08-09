# NIF codec split — the 7.0 prerequisite (assessed 2026-08-08)

Status: PLANNED, not started. This is the enabling step for the 7.0
cleanup (deleting the `:rust` engine and the legacy engine). It is
deliberately NOT bundled with 6.3.0: the default flip must stay easy to
revert, and deleting `:rust` — or destabilizing NIF packaging in the
same release — would remove that rollback.

## Why a split is required at all

`engine: :libsql` still loads `priv/native/tms_engine.so` because the
frame decoders and the Prometheus text parser live inside the Rust
ENGINE's NIF, not with the engine-agnostic code:

- `native/tms_engine/src/lib.rs` ~2700–3200 (~500 LOC):
  `parse_prom_body_visit`, `parse_prometheus`, `parse_prometheus_count`,
  `decode_raw_batches`, `decode_raw_frame` (+`_points`, `_series`),
  `decode_aggregate_frame`, `decode_latest_frame`.
- Verified self-contained: that range references no `EngineRef`,
  dashmap, or partition state. It needs only rustler (+ pco for batch
  payloads).
- Callers: `TimelessMetrics.RustEngine.Nif` is invoked from
  `libsql_engine.ex` (frame decode on every accelerated read) and the
  Prometheus ingest path — both engine-agnostic consumers.

Deleting `tms_engine` today would break the libSQL default's fast path.
Decoding frames in Elixir instead would erode the recorded frame wins
(TAF1 34.7→12.7ms class); the decode belongs in native code.

## Plan

1. New crate `native/tms_codec` (rustler cdylib): move the ~500-LOC
   region verbatim; `rustler::init!("Elixir.TimelessMetrics.Codec.Nif")`.
2. New `TimelessMetrics.Codec.Nif` module; `libsql_engine.ex`,
   `ingest_worker.ex`/HTTP Prometheus ingest, and anything else calling
   `RustEngine.Nif.decode_*`/`parse_prometheus*` switch to it.
   `RustEngine.Nif` keeps its engine functions only.
3. Packaging (the risky part — do on a quiet release):
   - `Makefile`: third artifact `priv/native/tms_codec.so`.
   - `mix.exs`: add to `make_precompiler_priv_paths` and the packaged
     `files` list.
   - `checksum.exs` / precompiled-artifact flow: new artifact enters the
     checksum set; verify the cc_precompiler matrix (incl. the macOS
     targets fixed in 6.2.4/6.2.5) before publishing.
4. Only after tms_codec ships and soaks: 7.0 may delete
   `native/tms_engine` + `rust_engine.ex` (breaking migration-from-rust
   support — requires the documented sunset of
   `mix timeless_metrics.migrate_libsql`, `ReleaseMigration`,
   `ReleaseStartup`'s rust paths, and `LegacyReader`) and the legacy
   engine (blocked on porting or retiring `mode: :memory` and
   query_daily/tier features per the deprecation warning).

## Open questions for 7.0 proper

- How long must migration-from-rust support live? Field installs that
  never migrated lose their upgrade path when `LegacyReader` goes.
- `ReleaseStartup` has no in-repo caller — inventory its external
  consumers (TimelessUI.MetricsDataPlane.Process) before changing its
  contract.
