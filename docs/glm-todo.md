# tms_engine Rust/NIF Code Review - Action Items

Source: Code review of `native/tms_engine/src/lib.rs` (~2012 lines)
Date: 2026-04-02

---

## Performance Impact Summary

| # | Change | Impact | Magnitude | Notes |
|---|--------|--------|-----------|-------|
| 1 | Resolve cache collision verification | Slight regression | ~50-100ns per write | Adds a HashMap lookup in `series_info` on every cache hit. Goes from ~50ns to ~100-150ns per resolve. Negligible in context (100ns x 1000 series = 0.1ms per batch). |
| 2 | `cold_flush_running` CAS guard | Neutral / slight improvement | 0% on normal path | Atomic CAS is ~5ns. `flush_cold` runs every 5 min. Prevents duplicate work if races occur. |
| 3 | Crash-safe chunk writes (tmp+rename) | Slight regression | ~1-2% on flush | `fs::rename` is near-zero cost on same filesystem (POSIX atomic rename). Extra syscall negligible vs compression + write I/O. |
| 4 | Release index lock before decompression | **Significant improvement** | 10-50% on concurrent workloads | Unblocks concurrent flushes during queries. Big win on write-heavy + query concurrent workloads. |
| 5 | Split lib.rs into modules | Neutral | 0% (compile-time only) | Pure refactor. No runtime impact. May slightly improve incremental compile times. |
| 6 | Handle lock poisoning gracefully | Neutral | 0% | `unwrap_or_else` on non-poisoned lock is same cost as `unwrap`. Only matters on error path. |
| 7 | Cross-query file read cache (LRU) | **Significant improvement** | 20-80% on repeated queries | Eliminates redundant disk I/O. Biggest single win for Grafana-style repeated range queries. Cost: modest memory increase. |
| 8 | `match_agg` returns error on unknown | Neutral | 0% | Only fires on bad input. Happy path unchanged. |
| 9 | `ensure_dir` error instead of panic | Neutral | 0% | Happy path unchanged. |
| 10 | `delete_before` reports file errors | Neutral | 0% | `fs::remove_file` still runs either way. Just don't discard the Result. |
| 11 | Series registry load validates data | Neutral to slight regression | ~0% on load | CRC check adds ~microseconds to startup. Totally irrelevant. Prevents silent data loss. |
| 12 | Typed error enum | Neutral to slight improvement | 0% | Rust enum dispatch is same speed. May be slightly faster (no heap allocation for static errors). |
| 13 | `engine_info` returns integers | Neutral | 0% | Encoding `i64` vs `f64` via Rustler is identical cost. |

| A | Eliminate clone in `resolve_series` read path | Improvement | ~200-500ns per miss | Avoid BTreeMap clone on every read-path lookup |
| B | `chunks_exact` in `write_batch_raw` | **Improvement** | 10-30% raw write throughput | Eliminates per-field bounds checks in hot loop |
| C | Track out-of-order flag, skip sort scan | Improvement | 1-5% on flush | Avoid O(n) scan when data is already sorted (common case) |
| D | **Bounded header reads during index rebuild** | **Major improvement** | 5-20x faster startup | Read ~60 bytes instead of full multi-MB chunk files |
| E | Replace flush_queue Mutex with lock-free structure | Improvement | 50-200ns per threshold write | Eliminates mutex contention on write path |
| F | Merge `Vec<i64>` + `Vec<f64>` into `Vec<(i64,f64)>` | Improvement | 5-15% write+query | Better cache locality, half the allocations |
| G | Batch-resolve then batch-write in `write_batch_labeled` | **Improvement** | 10-20% labeled batch throughput | Groups writes by series, improves DashMap locality |

**Net result: overall performance improvement.** Items #4, #7, and #D are the biggest wins.
Estimated overall improvement: 20-50% on query-heavy workloads with concurrent writes, 10-30% on write throughput with items B+G+F.

---

## HIGH PRIORITY

### 1. Resolve cache hash collision risk

- **File**: `lib.rs` ~lines 540-554
- **Problem**: `resolve_cache: DashMap<u64, i64>` maps a SipHash-64 digest to a `series_id` with zero collision detection. A hash collision silently returns the wrong series, causing data corruption (points written to the wrong metric).
- **Current code**:
  - `fast_series_hash(metric_name, labels_map)` hashes `metric_name` and sorted label k/v pairs into a `u64` via `DefaultHasher` (SipHash).
  - `resolve_cached()` looks up `u64 -> i64` and returns the series_id on hit, no verification.
- **Fix options**:
  - (a) Use `DashMap<(String, Labels), i64>` where `Labels = BTreeMap<String, String>`. Slower but correct. BTreeMap is already `Hash + Eq`.
  - (b) Keep the u64 hash for speed but verify on hit by comparing `(metric, labels)` against the `SeriesRegistry` reverse lookup. Only slightly slower on cache hit.
  - (c) Use a cryptographic hash (e.g., blake3) to reduce collision probability to negligible. Still not zero.
- **Recommendation**: Option (b) — keep the fast cache but add a verification step. The `SeriesRegistry` already has `series_info: HashMap<i64, SeriesInfo>` so verification is a single HashMap lookup.
- **Perf impact**: Slight regression (~50-100ns per resolve hit). Current cache hit is ~50ns pure DashMap lookup. Adding verification brings it to ~100-150ns (DashMap hit + HashMap reverse lookup). In context this is negligible: a batch of 1000 series adds 0.1ms total overhead.

### 2. `cold_flush_running` AtomicBool is dead code

- **File**: `lib.rs` ~line 377 (field declaration in `Engine`)
- **Problem**: The `cold_flush_running: AtomicBool` is declared as a field on `Engine` but never actually used as a guard. Multiple concurrent `flush_cold` calls (e.g., from overlapping Elixir timers) can race, causing duplicate flush work and potentially corrupted batch files if two threads write PCB1 files for overlapping partitions.
- **Fix**: At the top of `flush_cold()`, do a compare-and-swap on `cold_flush_running`:
  ```rust
  if self.cold_flush_running.compare_exchange(
      false, true, Ordering::SeqCst, Ordering::SeqCst
  ).is_err() {
      return Ok((0, 0, Vec::new())); // already running
  }
  ```
  Then reset to `false` on return (use a guard / Drop pattern to ensure reset on panic).
- **Perf impact**: Neutral. Atomic CAS is ~5ns. `flush_cold` runs every 5 minutes. Net improvement if it prevents duplicate flush work from races.

### 3. Crash-safe chunk file writes

- **File**: `lib.rs` — `flush_partition_to_pco1` and `flush_batch_to_pcb1` functions
- **Problem**: The series registry uses atomic tmp+rename, but chunk file writers write directly to the target path. A crash mid-write leaves a corrupt partial file. On next startup, `rebuild_index` will fail to parse it and the whole rebuild may abort or silently skip valid chunks in the same file.
- **Fix**: Apply the same tmp+rename pattern:
  - Write to `<path>.tmp`
  - `fs::rename(<path>.tmp, <path>)` (atomic on same filesystem on POSIX)
  - In `rebuild_index`, skip files with `.tmp` extension (cleanup stale ones)
- **Affected functions**: `flush_partition_to_pco1`, `flush_batch_to_pcb1`
- **Perf impact**: ~1-2% regression on flush. The extra `fs::rename` syscall is near-zero cost on the same filesystem (POSIX atomic rename is a directory entry swap, no data copy). The actual overhead comes from the additional filesystem metadata operation, which is negligible compared to the pco compression + write I/O that dominates flush time.

---

## MEDIUM PRIORITY

### 4. Index read lock held during decompression

- **File**: `lib.rs` ~lines 1073-1086 (query functions)
- **Problem**: The `self.index.read().unwrap()` lock on the BTreeMap is held while iterating over matching chunks, reading files from disk, and decompressing data. This blocks all index mutations (new flushes) for the entire query duration, which can be significant for large time ranges.
- **Fix**: Clone or collect the matching `ChunkMeta` entries while holding the lock, then drop the lock before doing file I/O and decompression:
  ```rust
  let matching: Vec<ChunkMeta> = {
      let idx = self.index.read().unwrap();
      idx.range(..).filter(|(_, meta)| matches(meta)).cloned().collect()
  }; // lock dropped here
  // now do I/O and decompression without holding the lock
  ```
- **Trade-off**: Slightly higher memory for the cloned vec, but unblocks concurrent flushes.
- **Perf impact**: **10-50% improvement on concurrent workloads.** Currently a query holding the index read lock blocks ALL flush operations (which need a write lock on the index). For a query that takes 100ms to decompress data, flushes are stalled for 100ms. After fix, flushes proceed concurrently. The clone cost is minimal (ChunkMeta is ~100 bytes, even 10k chunks = 1MB). Pure write throughput unaffected; pure query throughput unaffected; mixed workloads see the biggest win.

### 5. Split `lib.rs` into modules

- **File**: `native/tms_engine/src/lib.rs` (2012 lines)
- **Problem**: Everything is in one file. Hard to navigate, review, and test in isolation.
- **Proposed module layout**:
  ```
  src/
    lib.rs          -- NIF registration, atoms, EngineResource, module init
    engine.rs       -- Engine struct, write/flush methods
    registry.rs     -- SeriesRegistry, SeriesInfo, resolve methods
    chunk.rs        -- ChunkMeta, CompressedPartition, PCO1/PCB1 read/write
    query.rs        -- query_range, query_aggregate, label matching
    index.rs        -- Index rebuild, BTreeMap operations
    nif.rs          -- All #[rustler::nif] function wrappers
  ```
- **Note**: This is a refactor-only change. No logic changes. Run existing tests after.
- **Perf impact**: Neutral (0% runtime). Rust compiler inlines across modules at the same optimization level. May slightly improve incremental compile times during development.

### 6. Handle lock poisoning gracefully

- **File**: Throughout `lib.rs`
- **Problem**: Every `.read().unwrap()` and `.write().unwrap()` on RwLock/Mutex will panic if the lock is poisoned (a thread panicked while holding it). In a BEAM NIF context, Rustler catches panics, but the engine is left in an unknown state and subsequent operations will also panic.
- **Fix**: Use a helper or recover from poisoning:
  ```rust
  fn read_lock(lock: &RwLock<T>) -> Result<RwLockReadGuard<'_, T>, String> {
      lock.read().map_err(|e| format!("lock poisoned: {}", e))
  }
  // or recover:
  lock.read().unwrap_or_else(|e| e.into_inner())
  ```
- **Locations**: `self.series` RwLock, `self.flush_queue` Mutex, `self.created_dirs` Mutex.
- **Consideration**: Recovering from a poisoned lock means operating on potentially inconsistent data. For a metrics engine, this is usually better than hard crashing. Add logging if possible.
- **Perf impact**: Neutral (0%). `unwrap_or_else` on a non-poisoned lock compiles to the same code as `unwrap()`. Only differs on the error path (which is already in a degraded state).

### 7. Add cross-query file read cache (LRU)

- **File**: `lib.rs` — query_range and query_aggregate functions
- **Problem**: Each query creates a fresh `HashMap<PathBuf, Vec<u8>>` file cache. If two queries touch the same chunk file (very common for overlapping time ranges), the file is read and decompressed twice.
- **Fix options**:
  - (a) Add an `Arc<DashMap<PathBuf, Arc<Vec<u8>>>>` on `Engine` as an LRU file cache. Evict entries older than N seconds or when memory budget is exceeded.
  - (b) Use an `moka` crate for a concurrent LRU cache with TTL.
  - (c) Keep it simple: use a `DashMap<PathBuf, (Instant, Arc<Vec<u8>>)>` and periodically clean entries older than e.g., 60s.
- **Trade-off**: Increases memory usage but dramatically reduces I/O for repeated queries.
- **Perf impact**: **20-80% improvement on repeated queries.** A Grafana dashboard refreshing every 5s hits the same time ranges repeatedly. Currently each query re-reads and decompresses the same chunk files from disk. A shared cache eliminates this entirely. Example: a 50MB chunk file read from disk takes ~5ms (SSD) or ~50ms (HDD); from memory it takes ~microseconds. Cost is memory proportional to cache size (tunable). No impact on write path.

---

## LOW PRIORITY

### 8. `match_agg` silently defaults to `Avg` on unknown atom

- **File**: `lib.rs` ~lines 1874-1888
- **Problem**: If an unrecognized aggregation atom is passed from Elixir, the function silently falls back to `Avg` instead of returning an error. This can mask bugs in calling code.
- **Fix**: Return `Err("unknown aggregation function: ...".to_string())` in the default case.
- **Perf impact**: Neutral (0%). Only fires on invalid input. Happy path unchanged.

### 9. `ensure_dir` panics on `path.parent().unwrap()`

- **File**: `lib.rs` ~line 975
- **Problem**: If the path has no parent component (e.g., just a filename like `"foo"`), `path.parent()` returns `None` and `unwrap()` panics.
- **Fix**: Return a `Result` and propagate the error, or use `path.parent().ok_or_else(|| format!("path has no parent: {:?}", path))?`.
- **Perf impact**: Neutral (0%). Happy path unchanged. `ok_or_else` is a branch on Some/None, same cost as the existing `unwrap()`.

### 10. `delete_before` ignores file deletion errors

- **File**: `lib.rs` ~line 1353
- **Problem**: `let _ = fs::remove_file(path)` silently swallows errors. If the file is locked, permissions changed, or disk is failing, the operator gets no feedback.
- **Fix**: Collect errors and return them, or at minimum log a warning. Could return `{:ok, entries, files, errors}` or log to Elixir via `env.error()`.
- **Perf impact**: Neutral (0%). `fs::remove_file` still runs either way. Just don't discard the Result. Collecting errors into a Vec is negligible.

### 11. Series registry `load()` silently ignores parse errors

- **File**: `lib.rs` ~lines 257-259
- **Problem**: If the series registry file is partially corrupted, `load()` reads what it can and silently drops the rest. `from_utf8_lossy` converts corrupted bytes to replacement characters instead of failing. This means the system can start up with missing series, and the data for those series becomes orphaned (exists on disk but unretrievable).
- **Fix**:
  - Return an error on corruption instead of silently continuing.
  - At minimum, log/count how many entries were skipped.
  - Consider writing a checksum/CRC alongside the registry for integrity verification.
- **Perf impact**: Neutral to slight regression (~0% at load time). Adding a CRC32 check on the registry file adds microseconds to startup. The registry file is small (typically <1MB). Totally irrelevant to steady-state performance. The value of preventing silent data loss far outweighs the cost.

### 12. Replace `Result<T, String>` with typed error enum

- **File**: Throughout `lib.rs`
- **Problem**: All functions return `Result<T, String>`. This makes it impossible to pattern-match on specific error types in Elixir (or Rust) and reduces the usefulness of error messages.
- **Proposed fix**:
  ```rust
  enum EngineError {
      Io(std::io::Error),
      CorruptData { path: String, reason: String },
      SeriesNotFound(i64),
      InvalidArgument(String),
      LockPoisoned(String),
      FlushInProgress,
  }
  ```
  Then implement `Encoder` for `EngineError` to convert to Elixir terms.
- **Perf impact**: Neutral to slight improvement. Rust enum dispatch is the same speed as returning a String. For static error variants (e.g., `FlushInProgress`), may actually be slightly faster since no heap allocation is needed (vs `String::from("...")`). The `Encoder` impl to Elixir terms is identical cost either way.

### 13. `engine_info` returns all values as `f64`

- **File**: `lib.rs` ~lines 1848-1872
- **Problem**: `engine_info` returns a Rustler map with all values as `f64`. For large integers like `total_bytes` or `buffer_memory_bytes`, values above 2^53 lose precision in IEEE 754 doubles.
- **Fix**: Return integer-valued fields as `i64` or `u64` instead. Rustler supports encoding `i64` / `u64` directly. This may require changes on the Elixir side if it pattern-matches on floats.
- **Perf impact**: Neutral (0%). Encoding `i64` vs `f64` via Rustler is identical cost (both are a single NIF term creation).

---

## PURE PERFORMANCE WINS (No correctness/risk trade-off)

### A. Eliminate clone in `resolve_series` read path

- **File**: `lib.rs` line 452
- **Problem**: `reg.series_map.get(&(metric_name.to_string(), labels.clone()))` allocates a new `String` and clones the entire `BTreeMap<String, String>` on every read-path lookup, even on cache hits that don't need to create anything. This is ~200-500ns of pure waste per lookup.
- **Current code**:
  ```rust
  if let Some(&id) = reg.series_map.get(&(metric_name.to_string(), labels.clone())) {
      return Ok(id);
  }
  ```
- **Fix options**:
  - (a) Use a two-level map: `HashMap<String, HashMap<Labels, i64>>` where the outer key is `metric_name`. Then lookup is `map.get(metric_name)?.get(labels)` — no clone on `&str` lookup.
  - (b) Implement `Borrow` for a custom key type that allows borrowed lookups. This is more complex but preserves the single-map structure.
  - (c) Use a `HashMap<Arc<str>, HashMap<Labels, i64>>` to avoid cloning metric names across the registry.
- **Recommendation**: Option (a) — two-level map. Simple, idiomatic, eliminates both the String and BTreeMap clone.
- **Perf impact**: ~200-500ns per resolve miss. The read lock is already held, so this directly reduces the critical section duration. Also reduces allocator pressure under high concurrency.

### B. Use `chunks_exact` or cast in `write_batch_raw`

- **File**: `lib.rs` lines 581-586
- **Problem**: The raw batch write loop does per-field slice creation + `try_into().unwrap()` + bounds checks on every iteration:
  ```rust
  for i in 0..count {
      let o = i * ENTRY_SIZE;
      let series_id = i64::from_ne_bytes(data[o..o + 8].try_into().unwrap());
      let ts = i64::from_ne_bytes(data[o + 8..o + 16].try_into().unwrap());
      let val = f64::from_ne_bytes(data[o + 16..o + 24].try_into().unwrap());
      self.write_point(series_id, ts, val);
  }
  ```
  The length is already validated before the loop (line 573), so the per-iteration bounds checks are redundant.
- **Fix options**:
  - (a) Use `chunks_exact(24)` which eliminates per-chunk bounds checks:
    ```rust
    for chunk in data.chunks_exact(24) {
        let series_id = i64::from_ne_bytes([chunk[0], chunk[1], ...]);
        // or: let series_id = i64::from_ne_bytes(chunk[0..8].try_into().unwrap());
    }
    ```
  - (b) Cast the slice to `&[[u8; 24]]` (safe since length is validated). Then use fixed array indexing — LLVM can fully unroll and eliminate bounds checks.
  - (c) Use `unsafe` pointer casts: `*(data.as_ptr().add(o) as *const i64)`. Maximum speed but requires careful alignment/soundness reasoning.
- **Recommendation**: Option (a) — `chunks_exact`. Safe, idiomatic, and LLVM will eliminate the remaining bounds checks. Option (c) for an additional ~5% if benchmarking shows it matters.
- **Perf impact**: 10-30% on raw write path throughput for large batches. The raw write path is the hot path for pre-resolved writes (steady-state scraping). Each iteration currently does 3x `[o..o+N]` slice + 3x `try_into()` + 3x bounds check = 9 branches eliminated to ~0.

### C. Track out-of-order flag on PartitionBuffer, skip sort scan

- **File**: `lib.rs` line 775 (in `compress_partition`)
- **Problem**: Every flush calls `timestamps.windows(2).any(|w| w[0] > w[1])` to check if timestamps are sorted. This is an O(n) scan over every point. For typical time-series data (Prometheus scraping), timestamps are almost always monotonic — the sort is rarely needed.
- **Current code**:
  ```rust
  let needs_sort = timestamps.windows(2).any(|w| w[0] > w[1]);
  let sorted_points = if needs_sort { /* expensive sort + allocate */ } else { None };
  ```
- **Fix**: Add an `out_of_order: bool` field to `PartitionBuffer`. Set it to `true` in `write_point` when a new timestamp is less than the last timestamp in the buffer:
  ```rust
  if !buf.timestamps.is_empty() && ts < *buf.timestamps.last().unwrap() {
      buf.out_of_order = true;
  }
  ```
  Then in `compress_partition`, check the flag instead of scanning:
  ```rust
  let needs_sort = out_of_order_flag; // O(1) instead of O(n)
  ```
  Reset the flag when draining the partition.
- **Perf impact**: 1-5% on flush time. The O(n) scan is ~1ns per point, so for a 10k-point partition this saves ~10μs. Small but free. More importantly, it avoids the sort+unzip allocation on the common path (which is a larger win when partitions are large).

### D. Bounded header reads during index rebuild (BIGGEST WIN)

- **File**: `lib.rs` lines 1406 (`read_pco1_header`) and 1454 (`read_pcb1_headers`)
- **Problem**: During startup, `rebuild_index` scans all chunk files. Both `read_pco1_header` and `read_pcb1_headers` call `fs::read(path)` which reads the **entire file** into memory, but only parse the header. For PCO1 files, the header is ~60 bytes + pk_len. For PCB1, the header is `9 + 64*n` bytes. The compressed data (which can be megabytes per file) is loaded and immediately discarded.
  ```rust
  fn read_pco1_header(path: &PathBuf) -> Result<...> {
      let data = fs::read(path).map_err(|e| e.to_string())?;  // reads ENTIRE file
      // ... only reads first ~60 bytes
  }
  ```
- **Fix**: Use bounded reads:
  - For PCO1: Read only the first ~256 bytes (header is small and variable-length due to pk_len, but bounded). Use `File::open` + `read_exact` into a small stack buffer.
  - For PCB1: Read `9` bytes first to get `n`, then read `9 + 64*n` bytes for the full header table.
  ```rust
  fn read_pco1_header(path: &PathBuf) -> Result<...> {
      let mut file = File::open(path)?;
      let mut header = vec![0u8; 256]; // generous bound for header
      let bytes_read = file.read(&mut header)?;
      let data = &header[..bytes_read];
      // parse as before
  }
  ```
  For PCB1, do a two-phase read: first 9 bytes for `n`, then the rest.
- **Perf impact**: **5-20x faster startup when chunks are large.** If you have 10k chunks averaging 1MB each, current code reads ~10GB from disk. After fix, it reads ~1MB of headers. On SSD this could be the difference between 2-second and 100ms startup. On HDD even more dramatic. This is the single biggest pure performance win in the codebase.

### E. Replace flush_queue Mutex with lock-free structure

- **File**: `lib.rs` lines 373 (field), 532 (producer in `write_point`), 594-599 (consumer in `flush_pending`)
- **Problem**: `flush_queue: Mutex<Vec<PartitionKey>>` is contended on every `write_point` that crosses the flush threshold. The consumer in `flush_pending` drains it, allocates a `HashSet` for dedup, and iterates. This is a mutex on the write hot path.
  ```rust
  // Producer (write_point, line 532):
  if needs_flush {
      self.flush_queue.lock().unwrap().push(key);
  }
  // Consumer (flush_pending, lines 594-599):
  let keys: Vec<PartitionKey> = {
      let mut queue = self.flush_queue.lock().unwrap();
      std::mem::take(&mut *queue)
  };
  let mut seen = HashSet::new();
  let unique: Vec<PartitionKey> = keys.into_iter().filter(|k| seen.insert(*k)).collect();
  ```
- **Fix options**:
  - (a) Use `DashSet<PartitionKey>` instead of `Mutex<Vec>`. Producers do `set.insert(key)`, consumer does `set.iter() + set.clear()`. No mutex, automatic dedup, no separate HashSet step.
  - (b) Use `crossbeam::channel::unbounded()`. Producer sends, consumer drains. Doesn't dedup but avoids mutex.
  - (c) Use a lock-free MPSC queue (e.g., `seg_queue` from crossbeam).
- **Recommendation**: Option (a) — `DashSet`. Simplest, eliminates mutex AND the dedup step. `DashSet` is already a dependency (via `dashmap` crate which is already imported).
- **Perf impact**: 50-200ns per write that crosses the flush threshold. Not huge per-operation, but eliminates a mutex contention point under high concurrency. Also eliminates the `HashSet` allocation and dedup iteration in `flush_pending`.

### F. Merge PartitionBuffer fields into `Vec<(i64, f64)>`

- **File**: `lib.rs` ~line 330 (`PartitionBuffer` struct)
- **Problem**: `PartitionBuffer` has separate `timestamps: Vec<i64>` and `values: Vec<f64>`. Each point's timestamp and value are always accessed together (write, query, flush), but they're stored in separate heap allocations with separate cache lines. This causes cache misses on every access pattern.
  ```rust
  struct PartitionBuffer {
      timestamps: Vec<i64>,
      values: Vec<f64>,
      last_write: Instant,
  }
  ```
- **Fix**: Merge into a single `Vec<Point>` where `struct Point { ts: i64, val: f64 }`, or use `Vec<(i64, f64)>`:
  ```rust
  struct PartitionBuffer {
      points: Vec<(i64, f64)>,
      last_write: Instant,
      out_of_order: bool, // from item C above
  }
  ```
  Update `write_point`, query functions, and flush functions accordingly. The sort in `compress_partition` becomes `points.sort_unstable_by_key(|&(ts, _)| ts)` which is cleaner.
- **Perf impact**: 5-15% on both write and query paths.
  - **Write**: One `push` instead of two, one allocation instead of two, better cache line utilization.
  - **Query**: Iterating `points` gives (ts, val) in adjacent memory — one cache line fetch serves both values. Currently two separate cache line fetches.
  - **Flush**: Sort operates on a single vec instead of creating a zipped vec.
  - **Memory**: Saves one Vec header (24 bytes: ptr + len + cap) per series. For 100k series, saves ~2.4MB of metadata plus reduces heap fragmentation.
- **Trade-off**: Slightly worse for operations that only need timestamps (e.g., `buf.timestamps.iter().min()` becomes `buf.points.iter().map(|(ts,_)| ts).min()`). But these are rare and the compiler may optimize equally well.

### G. Batch-resolve then batch-write in `write_batch_labeled`

- **File**: `lib.rs` lines 562-565 (`write_batch_labeled`)
- **Problem**: Labeled batch writes resolve each series ID individually, then immediately write the point. This means DashMap lookups and potential lock acquisitions are interleaved per-entry:
  ```rust
  for (metric, labels_hm, ts, val) in entries {
      let series_id = self.resolve_cached(&metric, &labels_hm)?;
      self.write_point(series_id, ts, val);
  }
  ```
  For Prometheus scraping, many entries share the same series (e.g., 100 samples for `http_requests_total{method="GET"}`). Each hits the same DashMap shard, causing repeated lock/unlock cycles.
- **Fix**: Two-pass approach:
  ```rust
  // Pass 1: Resolve all series IDs (cache hits are ~50ns each)
  let resolved: Vec<(i64, i64, f64)> = entries.into_iter().map(|(metric, labels_hm, ts, val)| {
      let series_id = self.resolve_cached(&metric, &labels_hm)?;
      Ok((series_id, ts, val))
  }).collect::<EngineResult<Vec<_>>>()?;

  // Pass 2: Group by series_id, then batch-write
  resolved.sort_unstable_by_key(|(sid, _, _)| *sid);
  for (series_id, ts, val) in resolved {
      self.write_point(series_id, ts, val);
  }
  ```
  Alternatively, use `itertools::group_by` or a small HashMap to batch writes per series. Since `write_point` already uses `DashMap::entry`, consecutive writes to the same key hit the same shard and the entry is likely hot in cache.
- **Perf impact**: 10-20% on labeled write batch throughput when entries share series (typical in Prometheus scraping where a single scrape produces N samples per metric). The sort is O(n log n) but n is typically small (<10k per batch). The DashMap locality improvement dominates for large batches with repeated series.
- **Trade-off**: Adds an allocation for the `resolved` vec. For very small batches (<10 entries) this might be slightly slower. Consider only applying the two-pass approach when `entries.len() > SOME_THRESHOLD` (e.g., 64).
