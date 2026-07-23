use dashmap::DashMap;
use rayon::prelude::*;
use rustler::types::tuple::make_tuple;
use rustler::{Atom, Binary, Encoder, Env, ResourceArc, Term};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod atoms {
    rustler::atoms! {
        ok, error, avg, sum, min, max, count,
    }
}

type EngineResult<T> = Result<T, String>;

const BATCH_CHUNK_SIZE: usize = 1000;

/// How long a compressed chunk file stays in the read cache.
const FILE_CACHE_TTL: Duration = Duration::from_secs(60);

/// Chunks newer than this are never compacted: the recent window keeps
/// small chunks so narrow dashboard queries stay cheap.
const COMPACT_MIN_AGE_SECS: i64 = 3600;

// ═══════════════════════════════════════════════════════════════════════
// Core types
// ═══════════════════════════════════════════════════════════════════════

/// Sorted label set. BTreeMap gives deterministic ordering for hashing.
type Labels = BTreeMap<String, String>;

/// Partition key is just a series_id. The series registry maps
/// (metric_name, labels) → series_id.
#[derive(Hash, Eq, PartialEq, Clone, Debug, Ord, PartialOrd, Copy)]
struct PartitionKey {
    series_id: i64,
}

/// Chunk index key. The trailing sequence number is a per-engine
/// monotonic id (not persisted): two chunks for the same series may
/// legitimately share a min_ts (backfill, duplicate timestamps across
/// flush boundaries, compaction output), and a two-field key would let
/// the second insert silently shadow the first.
type ChunkKey = (PartitionKey, i64, u64);

/// Full identity of a series for reverse lookups and label queries.
#[derive(Clone)]
struct SeriesInfo {
    metric_name: String,
    labels: Labels,
}

struct PartitionBuffer {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    last_write: Instant,
    queued_for_flush: bool,
}

impl PartitionBuffer {
    fn new() -> Self {
        PartitionBuffer {
            timestamps: Vec::new(),
            values: Vec::new(),
            last_write: Instant::now(),
            queued_for_flush: false,
        }
    }
    fn memory_bytes(&self) -> usize {
        (self.timestamps.len() + self.values.len()) * 8
    }
}

/// Payload encoding for a chunk: pco-compressed (the durable format) or
/// raw big-endian arrays (transient, written by deferred-compression
/// flushes and consumed by compaction).
const ENC_PCO: u8 = 0;
const ENC_RAW: u8 = 1;

#[derive(Clone)]
struct ChunkMeta {
    min_ts: i64,
    max_ts: i64,
    point_count: u32,
    min_val: f64,
    max_val: f64,
    sum_val: f64,
    path: PathBuf,
    data_offset: u64,
    #[allow(dead_code)]
    data_len: u32,
    encoding: u8,
}

// ═══════════════════════════════════════════════════════════════════════
// Series Registry — maps (metric_name, labels) → series_id
// ═══════════════════════════════════════════════════════════════════════

struct SeriesRegistry {
    /// Forward: (metric, labels) → series_id
    series_map: HashMap<(String, Labels), i64>,
    /// Reverse: series_id → SeriesInfo
    series_info: HashMap<i64, SeriesInfo>,
    /// Inverted label index: (label_key, label_value) → set of series_ids
    label_index: HashMap<(String, String), HashSet<i64>>,
    /// Metric name → set of series_ids
    metric_index: HashMap<String, HashSet<i64>>,
    /// Next ID
    next_id: AtomicI64,
    dirty: bool,
}

impl SeriesRegistry {
    fn new() -> Self {
        SeriesRegistry {
            series_map: HashMap::new(),
            series_info: HashMap::new(),
            label_index: HashMap::new(),
            metric_index: HashMap::new(),
            next_id: AtomicI64::new(1),
            dirty: false,
        }
    }

    /// Resolve (metric_name, labels) → series_id. Creates if new.
    fn get_or_create(&mut self, metric_name: &str, labels: &Labels) -> i64 {
        let key = (metric_name.to_string(), labels.clone());
        if let Some(&id) = self.series_map.get(&key) {
            return id;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // Forward map
        self.series_map.insert(key, id);

        // Reverse map
        self.series_info.insert(
            id,
            SeriesInfo {
                metric_name: metric_name.to_string(),
                labels: labels.clone(),
            },
        );

        // Label index — index every label pair + __name__
        self.metric_index
            .entry(metric_name.to_string())
            .or_default()
            .insert(id);
        for (k, v) in labels {
            self.label_index
                .entry((k.clone(), v.clone()))
                .or_default()
                .insert(id);
        }

        self.dirty = true;
        id
    }

    fn info_for(&self, id: i64) -> Option<&SeriesInfo> {
        self.series_info.get(&id)
    }

    /// Find all series_ids matching a metric name and optional label filters.
    fn find_series(&self, metric_name: &str, label_filter: &Labels) -> Vec<i64> {
        let metric_ids = match self.metric_index.get(metric_name) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        if label_filter.is_empty() {
            return metric_ids.into_iter().collect();
        }

        let mut smallest = &metric_ids;

        for (k, v) in label_filter {
            let matching = match self.label_index.get(&(k.clone(), v.clone())) {
                Some(ids) => ids,
                None => return Vec::new(),
            };
            if matching.len() < smallest.len() {
                smallest = matching;
            }
        }

        smallest
            .iter()
            .copied()
            .filter(|id| {
                let Some(info) = self.series_info.get(id) else {
                    return false;
                };

                info.metric_name == metric_name
                    && label_filter
                        .iter()
                        .all(|(k, v)| info.labels.get(k).is_some_and(|actual| actual == v))
            })
            .collect()
    }

    fn list_metrics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.metric_index.keys().cloned().collect();
        names.sort();
        names
    }

    fn label_values(&self, metric_name: &str, label_key: &str) -> Vec<String> {
        let series_ids = match self.metric_index.get(metric_name) {
            Some(ids) => ids,
            None => return Vec::new(),
        };

        let mut values: HashSet<String> = HashSet::new();
        for &id in series_ids {
            if let Some(info) = self.series_info.get(&id) {
                if let Some(val) = info.labels.get(label_key) {
                    values.insert(val.clone());
                }
            }
        }

        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        result
    }

    fn all_label_names(&self) -> Vec<String> {
        let mut names: HashSet<String> = HashSet::new();
        names.insert("__name__".to_string());
        for (k, _) in self.label_index.keys() {
            names.insert(k.clone());
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    fn series_count(&self) -> usize {
        self.series_map.len()
    }

    /// Persist to disk.
    /// Format: [count: u32] [id: i64, metric_len: u16, metric: bytes,
    ///   label_count: u16, [key_len: u16, key, val_len: u16, val]...]...
    fn save(&mut self, path: &PathBuf) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let mut out = Vec::new();
        let count = self.series_info.len() as u32;
        out.extend_from_slice(&count.to_be_bytes());

        let mut entries: Vec<(&i64, &SeriesInfo)> = self.series_info.iter().collect();
        entries.sort_by_key(|&(id, _)| *id);

        for (&id, info) in entries {
            out.extend_from_slice(&id.to_be_bytes());
            let mb = info.metric_name.as_bytes();
            out.extend_from_slice(&(mb.len() as u16).to_be_bytes());
            out.extend_from_slice(mb);
            out.extend_from_slice(&(info.labels.len() as u16).to_be_bytes());
            for (k, v) in &info.labels {
                let kb = k.as_bytes();
                let vb = v.as_bytes();
                out.extend_from_slice(&(kb.len() as u16).to_be_bytes());
                out.extend_from_slice(kb);
                out.extend_from_slice(&(vb.len() as u16).to_be_bytes());
                out.extend_from_slice(vb);
            }
        }

        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }

        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, &out)?;
        fs::rename(&tmp_path, path)?;
        self.dirty = false;
        Ok(())
    }

    fn load(path: &PathBuf) -> Result<Self, String> {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return Ok(Self::new()),
        };
        if data.len() < 4 {
            return Err("series registry file too small".to_string());
        }

        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut reg = SeriesRegistry::new();
        let mut max_id: i64 = 0;
        let mut pos = 4;

        for entry_idx in 0..count {
            if pos + 10 > data.len() {
                return Err(format!(
                    "series registry truncated at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let ml = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + ml > data.len() {
                return Err(format!(
                    "series registry truncated: metric name at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let metric_name = String::from_utf8(data[pos..pos + ml].to_vec()).map_err(|e| {
                format!("invalid UTF-8 in metric name at entry {}: {}", entry_idx, e)
            })?;
            pos += ml;

            if pos + 2 > data.len() {
                return Err(format!(
                    "series registry truncated: label count at entry {} (pos {} of {})",
                    entry_idx,
                    pos,
                    data.len()
                ));
            }
            let lc = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut labels = BTreeMap::new();
            for label_idx in 0..lc {
                if pos + 2 > data.len() {
                    return Err(format!(
                        "series registry truncated: label key len at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let kl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + kl > data.len() {
                    return Err(format!(
                        "series registry truncated: label key at entry {} label {} (pos {} of {})",
                        entry_idx,
                        label_idx,
                        pos,
                        data.len()
                    ));
                }
                let k = String::from_utf8(data[pos..pos + kl].to_vec()).map_err(|e| {
                    format!(
                        "invalid UTF-8 in label key at entry {} label {}: {}",
                        entry_idx, label_idx, e
                    )
                })?;
                pos += kl;
                if pos + 2 > data.len() {
                    return Err(format!(
                        "series registry truncated: label value len at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let vl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + vl > data.len() {
                    return Err(format!(
                        "series registry truncated: label value at entry {} label {} (pos {} of {})",
                        entry_idx, label_idx, pos, data.len()
                    ));
                }
                let v = String::from_utf8(data[pos..pos + vl].to_vec()).map_err(|e| {
                    format!(
                        "invalid UTF-8 in label value at entry {} label {}: {}",
                        entry_idx, label_idx, e
                    )
                })?;
                pos += vl;
                labels.insert(k, v);
            }

            let key = (metric_name.clone(), labels.clone());
            reg.series_map.insert(key, id);
            reg.series_info.insert(
                id,
                SeriesInfo {
                    metric_name: metric_name.clone(),
                    labels: labels.clone(),
                },
            );
            reg.metric_index.entry(metric_name).or_default().insert(id);
            for (k, v) in &labels {
                reg.label_index
                    .entry((k.clone(), v.clone()))
                    .or_default()
                    .insert(id);
            }
            if id > max_id {
                max_id = id;
            }
        }

        reg.next_id = AtomicI64::new(max_id + 1);
        Ok(reg)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Engine
// ═══════════════════════════════════════════════════════════════════════

/// Fast hash of (metric, labels) for the resolution cache.
/// Uses std DefaultHasher which is SipHash — fast and collision-resistant.
fn fast_series_hash(metric: &str, labels: &HashMap<String, String>) -> u64 {
    let mut pairs: Vec<(&str, &str)> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.sort_unstable_by_key(|&(k, _)| k);
    fast_series_hash_pairs(metric, &pairs)
}

/// Hash core shared by the HashMap path and the fused-ingest path.
/// Pairs MUST be sorted by key and deduplicated — both callers guarantee
/// it — so both paths produce identical hashes for the same series and
/// share the resolve cache.
fn fast_series_hash_pairs(metric: &str, sorted_pairs: &[(&str, &str)]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric.hash(&mut hasher);
    for &(k, v) in sorted_pairs {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

struct Engine {
    data_dir: PathBuf,
    flush_threshold: usize,
    min_flush_size: usize,
    compression_level: usize,
    memory_budget: usize,
    /// Raw-first mode: flushes write raw (uncompressed) chunks; the
    /// periodic compactor later merges them into large pco chunks.
    defer_compression: bool,
    partitions: DashMap<PartitionKey, PartitionBuffer>,
    index: RwLock<BTreeMap<ChunkKey, ChunkMeta>>,
    /// Source of the ChunkKey sequence field. In-memory only — restart
    /// recovery re-assigns fresh values while scanning.
    chunk_seq: AtomicU64,
    series: RwLock<SeriesRegistry>,
    created_dirs: Mutex<HashSet<PathBuf>>,
    flush_queue: Mutex<Vec<PartitionKey>>,
    buffer_memory: AtomicUsize,
    batch_counter: AtomicUsize,
    instance_id: u128,
    cold_flush_running: AtomicBool,
    compaction_running: AtomicBool,
    /// Fast resolution cache: hash(metric, labels) → series_id.
    /// Persists across batches — steady-state scraping is pure cache hits.
    resolve_cache: DashMap<u64, i64>,
    file_cache: DashMap<PathBuf, (Instant, Arc<Vec<u8>>)>,
}

struct EngineResource {
    engine: Engine,
}

unsafe impl Send for EngineResource {}
unsafe impl Sync for EngineResource {}
impl std::panic::RefUnwindSafe for EngineResource {}
impl std::panic::UnwindSafe for EngineResource {}
impl rustler::Resource for EngineResource {}

struct CompressedPartition {
    key: PartitionKey,
    min_ts: i64,
    max_ts: i64,
    point_count: u32,
    min_val: f64,
    max_val: f64,
    sum_val: f64,
    ts_compressed: Vec<u8>,
    val_compressed: Vec<u8>,
    /// ENC_PCO or ENC_RAW — what ts_compressed/val_compressed contain.
    encoding: u8,
}

struct ColdFlushGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for ColdFlushGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

impl Engine {
    fn index_read(&self) -> RwLockReadGuard<'_, BTreeMap<ChunkKey, ChunkMeta>> {
        self.index.read().unwrap_or_else(|e| e.into_inner())
    }

    fn index_write(&self) -> RwLockWriteGuard<'_, BTreeMap<ChunkKey, ChunkMeta>> {
        self.index.write().unwrap_or_else(|e| e.into_inner())
    }

    fn next_chunk_seq(&self) -> u64 {
        self.chunk_seq.fetch_add(1, Ordering::Relaxed)
    }

    fn series_read(&self) -> RwLockReadGuard<'_, SeriesRegistry> {
        self.series.read().unwrap_or_else(|e| e.into_inner())
    }

    fn series_write(&self) -> RwLockWriteGuard<'_, SeriesRegistry> {
        self.series.write().unwrap_or_else(|e| e.into_inner())
    }

    fn flush_queue_lock(&self) -> MutexGuard<'_, Vec<PartitionKey>> {
        self.flush_queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn created_dirs_lock(&self) -> MutexGuard<'_, HashSet<PathBuf>> {
        self.created_dirs.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn forget_created_dir(&self, dir: &std::path::Path) {
        self.created_dirs_lock().remove(dir);
    }

    fn series_path(data_dir: &PathBuf) -> PathBuf {
        data_dir.join("series.bin")
    }

    fn new(
        data_dir: PathBuf,
        flush_threshold: usize,
        min_flush_size: usize,
        compression_level: usize,
        memory_budget: usize,
        defer_compression: bool,
    ) -> Self {
        let registry = SeriesRegistry::load(&Self::series_path(&data_dir)).unwrap_or_else(|e| {
            eprintln!("WARNING: corrupt series registry, starting fresh: {}", e);
            SeriesRegistry::new()
        });
        let instance_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        let engine = Engine {
            data_dir,
            flush_threshold,
            min_flush_size,
            compression_level,
            memory_budget,
            defer_compression,
            partitions: DashMap::new(),
            index: RwLock::new(BTreeMap::new()),
            series: RwLock::new(registry),
            created_dirs: Mutex::new(HashSet::new()),
            flush_queue: Mutex::new(Vec::new()),
            buffer_memory: AtomicUsize::new(0),
            batch_counter: AtomicUsize::new(0),
            chunk_seq: AtomicU64::new(0),
            instance_id,
            cold_flush_running: AtomicBool::new(false),
            compaction_running: AtomicBool::new(false),
            resolve_cache: DashMap::new(),
            file_cache: DashMap::new(),
        };
        // Finish any compaction interrupted by a crash BEFORE scanning
        // files into the index, so superseded chunks never resurface.
        Self::recover_compaction_manifest(&engine.data_dir);
        engine.rebuild_index();
        engine
    }

    // ── Series resolution ────────────────────────────────────────────

    /// Resolve (metric, labels) → series_id. Fast read path, slow write path.
    fn resolve_series(&self, metric_name: &str, labels: &Labels) -> EngineResult<i64> {
        let key = (metric_name.to_string(), labels.clone());
        let mut reg = self.series_write();
        if let Some(&id) = reg.series_map.get(&key) {
            return Ok(id);
        }
        Ok(reg.get_or_create(metric_name, labels))
    }

    fn resolve_series_batch(&self, entries: &[(String, Labels)]) -> EngineResult<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(entries.len());
        let mut misses: Vec<(usize, &str, &Labels)> = Vec::new();

        {
            let reg = self.series_read();
            for (idx, (metric_name, labels)) in entries.iter().enumerate() {
                if let Some(&id) = reg.series_map.get(&(metric_name.clone(), labels.clone())) {
                    out.push(id);
                } else {
                    out.push(0);
                    misses.push((idx, metric_name.as_str(), labels));
                }
            }
        }

        if misses.is_empty() {
            return Ok(out);
        }

        let mut reg = self.series_write();
        for (idx, metric_name, labels) in misses {
            out[idx] = reg.get_or_create(metric_name, labels);
        }

        Ok(out)
    }

    fn save_series(&self) -> EngineResult<()> {
        let mut reg = self.series_write();
        reg.save(&Self::series_path(&self.data_dir))
            .map_err(|err| format!("failed to persist series registry: {err}"))
    }

    // ── Write path ───────────────────────────────────────────────────

    #[inline]
    fn write_point(&self, series_id: i64, ts: i64, val: f64) {
        let key = PartitionKey { series_id };
        let should_queue_flush;
        let mem_delta: isize;

        {
            let mut entry = self
                .partitions
                .entry(key)
                .or_insert_with(PartitionBuffer::new);
            let buf = entry.value_mut();
            let old_cap = buf.memory_bytes();
            buf.timestamps.push(ts);
            buf.values.push(val);
            buf.last_write = Instant::now();
            let new_cap = buf.memory_bytes();
            mem_delta = (new_cap as isize) - (old_cap as isize);
            should_queue_flush =
                buf.timestamps.len() >= self.flush_threshold && !buf.queued_for_flush;
            if should_queue_flush {
                buf.queued_for_flush = true;
            }
        }

        if mem_delta > 0 {
            self.buffer_memory
                .fetch_add(mem_delta as usize, Ordering::Relaxed);
        } else if mem_delta < 0 {
            self.buffer_memory
                .fetch_sub((-mem_delta) as usize, Ordering::Relaxed);
        }

        if should_queue_flush {
            self.flush_queue_lock().push(key);
        }
    }

    /// Resolve series using the persistent hash cache.
    /// Fast path: DashMap hash lookup + verification (~100ns).
    /// Slow path: full registry resolve + cache insert.
    /// Verification prevents silent data corruption from hash collisions.
    #[inline]
    fn resolve_cached(&self, metric: &str, labels: &HashMap<String, String>) -> EngineResult<i64> {
        let hash = fast_series_hash(metric, labels);

        // Fast path: cache hit with verification
        if let Some(id) = self.resolve_cache.get(&hash) {
            let series_id = *id;
            if self.verify_series_identity(series_id, metric, labels) {
                return Ok(series_id);
            }
            // Hash collision detected — fall through to slow path
        }

        // Slow path: full resolve + cache
        let labels_bt: BTreeMap<String, String> =
            labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let id = self.resolve_series(metric, &labels_bt)?;
        self.resolve_cache.insert(hash, id);
        Ok(id)
    }

    /// Verify that a cached series_id still matches (metric, labels).
    /// Reads from series_info under read lock — single HashMap lookup.
    #[inline]
    fn verify_series_identity(
        &self,
        series_id: i64,
        metric: &str,
        labels: &HashMap<String, String>,
    ) -> bool {
        let reg = match self.series.read() {
            Ok(r) => r,
            Err(_) => return false,
        };
        match reg.info_for(series_id) {
            Some(info) => {
                if info.metric_name != metric {
                    return false;
                }
                if info.labels.len() != labels.len() {
                    return false;
                }
                for (k, v) in labels {
                    match info.labels.get(k) {
                        Some(iv) if iv == v => {}
                        _ => return false,
                    }
                }
                true
            }
            None => false,
        }
    }

    /// Write a batch of labeled entries. Resolves series internally.
    /// Uses persistent hash cache — steady-state scraping is pure cache hits.
    fn write_batch_labeled(
        &self,
        entries: Vec<(String, HashMap<String, String>, i64, f64)>,
    ) -> EngineResult<()> {
        for (metric, labels_hm, ts, val) in entries {
            let series_id = self.resolve_cached(&metric, &labels_hm)?;
            self.write_point(series_id, ts, val);
        }
        Ok(())
    }

    /// Binary batch: [series_id: i64, ts: i64, val: f64] = 24 bytes per entry.
    /// Use after pre-resolving series IDs.
    fn write_batch_raw(&self, data: &[u8]) -> EngineResult<()> {
        const ENTRY_SIZE: usize = 24;
        if data.len() % ENTRY_SIZE != 0 {
            return Err(format!(
                "raw batch length {} is not a multiple of {}",
                data.len(),
                ENTRY_SIZE
            ));
        }
        let count = data.len() / ENTRY_SIZE;
        for i in 0..count {
            let o = i * ENTRY_SIZE;
            let series_id = i64::from_ne_bytes(data[o..o + 8].try_into().unwrap());
            let ts = i64::from_ne_bytes(data[o + 8..o + 16].try_into().unwrap());
            let val = f64::from_ne_bytes(data[o + 16..o + 24].try_into().unwrap());
            self.write_point(series_id, ts, val);
        }
        Ok(())
    }

    /// Verify a cached series_id against borrowed (metric, sorted pairs).
    /// BTreeMap iterates sorted by key, so element-wise zip comparison works.
    #[inline]
    fn verify_series_identity_pairs(
        &self,
        series_id: i64,
        metric: &str,
        sorted_pairs: &[(&str, &str)],
    ) -> bool {
        let reg = self.series_read();
        match reg.info_for(series_id) {
            Some(info) => {
                info.metric_name == metric
                    && info.labels.len() == sorted_pairs.len()
                    && info
                        .labels
                        .iter()
                        .zip(sorted_pairs)
                        .all(|((ik, iv), &(k, v))| ik == k && iv == v)
            }
            None => false,
        }
    }

    /// Slow path for the fused ingest: materialize owned strings, resolve
    /// through the registry, and cache under the precomputed hash.
    fn resolve_pairs_slow(
        &self,
        hash: u64,
        metric: &str,
        sorted_pairs: &[(&str, &str)],
    ) -> EngineResult<i64> {
        let labels_bt: Labels = sorted_pairs
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let id = self.resolve_series(metric, &labels_bt)?;
        self.resolve_cache.insert(hash, id);
        Ok(id)
    }

    /// Fused ingest: Prometheus text → resolve → buffer in one pass.
    /// No BEAM terms are built per sample; on the steady-state cache-hit
    /// path no allocations happen per sample either. `default_ts` (epoch
    /// seconds) is used for samples without a timestamp; millisecond
    /// timestamps are normalized to seconds, matching the scraper.
    /// Returns (samples_written, parse_errors).
    fn ingest_prometheus(&self, body: &[u8], default_ts: i64) -> EngineResult<(usize, usize)> {
        let mut sorted: Vec<(&str, &str)> = Vec::with_capacity(16);
        let mut failure: EngineResult<()> = Ok(());

        let (count, errors) = parse_prom_body_visit(body, |name, labels, value, ts| {
            if failure.is_err() {
                return;
            }

            let ts = if ts == 0 {
                default_ts
            } else if ts > 1_000_000_000_000 {
                ts / 1000
            } else {
                ts
            };

            match self.resolve_entry(name, labels, &mut sorted) {
                Ok(series_id) => self.write_point(series_id, ts, value),
                Err(e) => failure = Err(e),
            }
        });

        failure?;
        Ok((count, errors))
    }

    /// Resolve one parsed sample to a series_id. Cache hits touch only
    /// borrowed data; UTF-8 validation (not conversion) keeps hashing
    /// identical to the String-based path so both share resolve_cache.
    fn resolve_entry<'a>(
        &self,
        name: &'a [u8],
        labels: &[(&'a [u8], &'a [u8])],
        sorted: &mut Vec<(&'a str, &'a str)>,
    ) -> EngineResult<i64> {
        let Some(metric) = std::str::from_utf8(name).ok() else {
            return self.resolve_lossy(name, labels);
        };

        sorted.clear();
        for &(k, v) in labels {
            match (std::str::from_utf8(k), std::str::from_utf8(v)) {
                (Ok(k), Ok(v)) => sorted.push((k, v)),
                _ => return self.resolve_lossy(name, labels),
            }
        }

        // Sort by key (stable) and keep the LAST occurrence of duplicate
        // keys, matching HashMap/BTreeMap insert semantics downstream.
        sorted.sort_by_key(|&(k, _)| k);
        let mut w = 0;
        for i in 0..sorted.len() {
            if i + 1 < sorted.len() && sorted[i + 1].0 == sorted[i].0 {
                continue;
            }
            sorted[w] = sorted[i];
            w += 1;
        }
        sorted.truncate(w);

        let hash = fast_series_hash_pairs(metric, sorted);

        if let Some(id) = self.resolve_cache.get(&hash) {
            let series_id = *id;
            if self.verify_series_identity_pairs(series_id, metric, sorted) {
                return Ok(series_id);
            }
            // Hash collision — fall through to the verified slow path
        }

        self.resolve_pairs_slow(hash, metric, sorted)
    }

    /// Rare fallback for invalid UTF-8 in names/labels: resolve through
    /// the registry with lossy conversion, bypassing the hash cache.
    fn resolve_lossy(&self, name: &[u8], labels: &[(&[u8], &[u8])]) -> EngineResult<i64> {
        let metric = String::from_utf8_lossy(name);
        let labels_bt: Labels = labels
            .iter()
            .map(|&(k, v)| {
                (
                    String::from_utf8_lossy(k).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                )
            })
            .collect();
        self.resolve_series(&metric, &labels_bt)
    }

    // ── Flush ────────────────────────────────────────────────────────

    fn flush_pending(&self) -> EngineResult<usize> {
        let keys: Vec<PartitionKey> = {
            let mut queue = self.flush_queue_lock();
            std::mem::take(&mut *queue)
        };
        let mut count = 0;
        for key in keys {
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| buf.timestamps.len() >= self.min_flush_size)
            {
                let cp = self.compress_partition(&key, &timestamps, &values)?;
                let meta = self.write_individual_chunk(&cp)?;
                self.index
                    .write()
                    .unwrap()
                    .insert((key, meta.min_ts, self.next_chunk_seq()), meta);
                count += 1;
            } else {
                self.clear_flush_queued(&key);
            }
        }
        self.save_series()?;
        Ok(count)
    }

    #[allow(dead_code)]
    fn flush_partition_individual(&self, key: &PartitionKey) -> EngineResult<()> {
        if let Some((timestamps, values)) =
            self.drain_partition_if(key, |buf| !buf.timestamps.is_empty())
        {
            let cp = self.compress_partition(key, &timestamps, &values)?;
            let meta = self.write_individual_chunk(&cp)?;
            self.index
                .write()
                .unwrap()
                .insert((*key, meta.min_ts, self.next_chunk_seq()), meta);
        }
        Ok(())
    }

    /// Drop expired file-cache entries. The read path only evicts entries
    /// it happens to touch after expiry, so a file read once and never
    /// again would stay resident forever without this periodic sweep.
    fn sweep_file_cache(&self) {
        self.file_cache
            .retain(|_, (cached_at, _)| cached_at.elapsed() < FILE_CACHE_TTL);
    }

    fn flush_cold(&self, max_idle_secs: u64) -> EngineResult<(usize, usize, usize)> {
        if self
            .cold_flush_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok((0, 0, 0));
        }

        let _guard = ColdFlushGuard {
            flag: &self.cold_flush_running,
        };

        // Piggyback on the periodic cold-flush timer to bound cache memory.
        self.sweep_file_cache();

        // In raw-first mode, the same timer drives compaction of raw and
        // undersized chunks into large pco chunks. Recent chunks are
        // excluded: dashboards query recent windows, and small chunks
        // keep those narrow reads cheap (no whole-chunk decompression).
        if self.defer_compression {
            let cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
                - COMPACT_MIN_AGE_SECS;
            self.compact_partitions(cutoff)?;
        }

        let now = Instant::now();
        let cold_keys: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|e| now.duration_since(e.value().last_write).as_secs() >= max_idle_secs)
            .map(|e| *e.key())
            .collect();

        let mut compressed: Vec<CompressedPartition> = Vec::new();
        let mut evicted = 0;

        for key in &cold_keys {
            if let Some((timestamps, values)) = self.drain_partition_if(key, |buf| {
                now.duration_since(buf.last_write).as_secs() >= max_idle_secs
                    && !buf.timestamps.is_empty()
            }) {
                compressed.push(self.compress_partition(key, &timestamps, &values)?);
                evicted += 1;
            }
        }

        if compressed.is_empty() {
            return Ok((0, evicted, 0));
        }

        let flushed = compressed.len();
        let mut files_written = 0;
        for batch in compressed.chunks(1000) {
            let metas = self.write_batched_chunk(batch)?;
            let mut index = self.index_write();
            for (key, meta) in metas {
                index.insert((key, meta.min_ts, self.next_chunk_seq()), meta);
            }
            files_written += 1;
        }

        self.save_series()?;
        Ok((flushed, evicted, files_written))
    }

    fn flush_by_memory(&self) -> EngineResult<usize> {
        let current = self.buffer_memory.load(Ordering::Relaxed);
        if current <= self.memory_budget {
            return Ok(0);
        }

        let mut sizes: Vec<(PartitionKey, usize)> = self
            .partitions
            .iter()
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();
        sizes.sort_by(|a, b| b.1.cmp(&a.1));

        let mut freed = 0usize;
        let overage = current - self.memory_budget;
        let mut compressed: Vec<CompressedPartition> = Vec::new();

        for (key, _) in sizes {
            if freed >= overage {
                break;
            }
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| !buf.timestamps.is_empty())
            {
                freed += partition_vec_memory(&timestamps, &values);
                compressed.push(self.compress_partition(&key, &timestamps, &values)?);
            }
        }

        let count = compressed.len();
        if !compressed.is_empty() {
            for batch in compressed.chunks(BATCH_CHUNK_SIZE) {
                let metas = self.write_batched_chunk(batch)?;
                let mut index = self.index_write();
                for (key, meta) in metas {
                    index.insert((key, meta.min_ts, self.next_chunk_seq()), meta);
                }
            }
        }
        self.save_series()?;
        Ok(count)
    }

    fn flush_all(&self) -> EngineResult<()> {
        let keys: Vec<(PartitionKey, usize)> = self
            .partitions
            .iter()
            .filter(|e| !e.value().timestamps.is_empty())
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();

        let mut small_compressed: Vec<CompressedPartition> = Vec::new();
        let mut new_individual: Vec<(PartitionKey, ChunkMeta)> = Vec::new();

        for (key, len) in keys {
            if let Some((timestamps, values)) =
                self.drain_partition_if(&key, |buf| !buf.timestamps.is_empty())
            {
                let cp = self.compress_partition(&key, &timestamps, &values)?;
                if len >= self.min_flush_size {
                    new_individual.push((key, self.write_individual_chunk(&cp)?));
                } else {
                    small_compressed.push(cp);
                }
            }
        }

        let mut all_metas = new_individual;
        for batch in small_compressed.chunks(BATCH_CHUNK_SIZE) {
            all_metas.extend(self.write_batched_chunk(batch)?);
        }
        if !all_metas.is_empty() {
            let mut index = self.index_write();
            for (key, meta) in all_metas {
                index.insert((key, meta.min_ts, self.next_chunk_seq()), meta);
            }
        }
        self.save_series()?;
        Ok(())
    }

    fn shutdown(&self) -> EngineResult<()> {
        self.flush_all()?;
        self.save_series()
    }

    // ── Compression ──────────────────────────────────────────────────

    fn compress_partition(
        &self,
        key: &PartitionKey,
        timestamps: &[i64],
        values: &[f64],
    ) -> EngineResult<CompressedPartition> {
        if self.defer_compression {
            self.encode_partition(key, timestamps, values, ENC_RAW, self.compression_level)
        } else {
            self.encode_partition(key, timestamps, values, ENC_PCO, self.compression_level)
        }
    }

    fn encode_partition(
        &self,
        key: &PartitionKey,
        timestamps: &[i64],
        values: &[f64],
        encoding: u8,
        level: usize,
    ) -> EngineResult<CompressedPartition> {
        if timestamps.is_empty() || timestamps.len() != values.len() {
            return Err(format!(
                "invalid partition payload for series {}: {} timestamps, {} values",
                key.series_id,
                timestamps.len(),
                values.len()
            ));
        }

        let needs_sort = timestamps.windows(2).any(|w| w[0] > w[1]);
        let sorted_points = if needs_sort {
            let mut points: Vec<(i64, f64)> = timestamps
                .iter()
                .copied()
                .zip(values.iter().copied())
                .collect();
            points.sort_unstable_by_key(|&(ts, _)| ts);
            Some(points.into_iter().unzip::<_, _, Vec<i64>, Vec<f64>>())
        } else {
            None
        };
        let (ts_slice, val_slice) = match &sorted_points {
            Some((ts, vals)) => (&ts[..], &vals[..]),
            None => (timestamps, values),
        };

        let (ts_compressed, val_compressed) = if encoding == ENC_RAW {
            let mut ts_raw = Vec::with_capacity(ts_slice.len() * 8);
            for ts in ts_slice {
                ts_raw.extend_from_slice(&ts.to_be_bytes());
            }
            let mut val_raw = Vec::with_capacity(val_slice.len() * 8);
            for v in val_slice {
                val_raw.extend_from_slice(&v.to_be_bytes());
            }
            (ts_raw, val_raw)
        } else {
            let config = pco::ChunkConfig::default().with_compression_level(level);
            let ts_compressed = pco::standalone::simple_compress(ts_slice, &config)
                .map_err(|err| {
                    format!(
                        "failed to compress timestamps for series {}: {err}",
                        key.series_id
                    )
                })?;
            let val_compressed = pco::standalone::simple_compress(val_slice, &config)
                .map_err(|err| {
                    format!(
                        "failed to compress values for series {}: {err}",
                        key.series_id
                    )
                })?;
            (ts_compressed, val_compressed)
        };

        let min_ts = ts_slice[0];
        let max_ts = ts_slice[ts_slice.len() - 1];
        let point_count = ts_slice.len() as u32;
        let (mut min_val, mut max_val, mut sum_val) = (val_slice[0], val_slice[0], 0.0f64);
        for &v in val_slice {
            if v < min_val {
                min_val = v;
            }
            if v > max_val {
                max_val = v;
            }
            sum_val += v;
        }

        Ok(CompressedPartition {
            key: *key,
            min_ts,
            max_ts,
            point_count,
            min_val,
            max_val,
            sum_val,
            ts_compressed,
            val_compressed,
            encoding,
        })
    }

    // ── Individual chunk writer (PCO1) ───────────────────────────────

    fn write_individual_chunk(&self, cp: &CompressedPartition) -> EngineResult<ChunkMeta> {
        let (meta, _written) = self.write_individual_chunk_at(cp, false)?;
        Ok(meta)
    }

    /// Write a chunk file. With `pending`, the file is left at
    /// `<final>.pending` — invisible to rebuild_index — and the caller
    /// renames it to the final path later (compaction manifest protocol).
    /// The returned meta always carries the FINAL path; the second value
    /// is the path actually on disk.
    fn write_individual_chunk_at(
        &self,
        cp: &CompressedPartition,
        pending: bool,
    ) -> EngineResult<(ChunkMeta, PathBuf)> {
        let series_id_str = cp.key.series_id.to_string();
        let file_id = self.next_file_id();

        let path = self
            .data_dir
            .join("chunks")
            .join(&series_id_str)
            .join(format!("{}_{}.pco1", cp.min_ts, file_id));

        self.ensure_dir(&path)
            .map_err(|err| format!("failed to create chunk dir {}: {err}", path.display()))?;

        // Store series_id as the partition key string in PCO1
        let pk_bytes = series_id_str.as_bytes();

        let mut out = Vec::with_capacity(
            64 + pk_bytes.len() + cp.ts_compressed.len() + cp.val_compressed.len(),
        );
        out.extend_from_slice(b"PCO1");
        // Version byte doubles as payload encoding: 1 = pco, 2 = raw
        out.push(if cp.encoding == ENC_RAW { 2u8 } else { 1u8 });
        out.extend_from_slice(&cp.point_count.to_be_bytes());
        out.extend_from_slice(&cp.min_ts.to_be_bytes());
        out.extend_from_slice(&cp.max_ts.to_be_bytes());
        out.extend_from_slice(&(pk_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(pk_bytes);
        out.extend_from_slice(&cp.min_val.to_be_bytes());
        out.extend_from_slice(&cp.max_val.to_be_bytes());
        out.extend_from_slice(&cp.sum_val.to_be_bytes());
        out.extend_from_slice(&(cp.ts_compressed.len() as u32).to_be_bytes());
        out.extend_from_slice(&cp.ts_compressed);
        out.extend_from_slice(&(cp.val_compressed.len() as u32).to_be_bytes());
        out.extend_from_slice(&cp.val_compressed);

        let tmp_path = path.with_extension("pco1.tmp");
        fs::File::create(&tmp_path)
            .and_then(|mut file| file.write_all(&out))
            .map_err(|err| format!("failed to write chunk {}: {err}", path.display()))?;

        let written = if pending {
            path.with_extension("pco1.pending")
        } else {
            path.clone()
        };
        fs::rename(&tmp_path, &written)
            .map_err(|err| format!("failed to rename chunk {}: {err}", written.display()))?;

        Ok((
            ChunkMeta {
                min_ts: cp.min_ts,
                max_ts: cp.max_ts,
                point_count: cp.point_count,
                min_val: cp.min_val,
                max_val: cp.max_val,
                sum_val: cp.sum_val,
                path,
                data_offset: 0,
                data_len: 0,
                encoding: cp.encoding,
            },
            written,
        ))
    }

    // ── Batched chunk writer (PCB1) ──────────────────────────────────

    fn write_batched_chunk(
        &self,
        partitions: &[CompressedPartition],
    ) -> EngineResult<Vec<(PartitionKey, ChunkMeta)>> {
        let batch_id = self.next_file_id();
        let path = self
            .data_dir
            .join("batches")
            .join(format!("batch_{}.pcb1", batch_id));
        self.ensure_dir(&path)
            .map_err(|err| format!("failed to create batch dir {}: {err}", path.display()))?;

        let n = partitions.len() as u32;
        let header_size = 4 + 1 + 4;
        // Per entry: series_id(8) + point_count(4) + min_ts(8) + max_ts(8) +
        //   min_val(8) + max_val(8) + sum_val(8) + data_offset(8) + data_len(4) = 64
        let entry_size = 64;
        let table_size = n as usize * entry_size;
        let data_start = header_size + table_size;

        let mut data_offsets = Vec::with_capacity(partitions.len());
        let mut offset = data_start;
        for cp in partitions {
            data_offsets.push(offset);
            offset += 4 + cp.ts_compressed.len() + 4 + cp.val_compressed.len();
        }

        let mut out = Vec::with_capacity(offset);

        out.extend_from_slice(b"PCB1");
        // Version byte doubles as payload encoding for ALL partitions in
        // the batch (flushes produce uniform encoding): 1 = pco, 2 = raw
        let batch_encoding = partitions.first().map(|cp| cp.encoding).unwrap_or(ENC_PCO);
        out.push(if batch_encoding == ENC_RAW { 2u8 } else { 1u8 });
        out.extend_from_slice(&n.to_be_bytes());

        for (i, cp) in partitions.iter().enumerate() {
            let data_len = (4 + cp.ts_compressed.len() + 4 + cp.val_compressed.len()) as u32;
            out.extend_from_slice(&cp.key.series_id.to_be_bytes());
            out.extend_from_slice(&cp.point_count.to_be_bytes());
            out.extend_from_slice(&cp.min_ts.to_be_bytes());
            out.extend_from_slice(&cp.max_ts.to_be_bytes());
            out.extend_from_slice(&cp.min_val.to_be_bytes());
            out.extend_from_slice(&cp.max_val.to_be_bytes());
            out.extend_from_slice(&cp.sum_val.to_be_bytes());
            out.extend_from_slice(&(data_offsets[i] as u64).to_be_bytes());
            out.extend_from_slice(&data_len.to_be_bytes());
        }

        for cp in partitions {
            out.extend_from_slice(&(cp.ts_compressed.len() as u32).to_be_bytes());
            out.extend_from_slice(&cp.ts_compressed);
            out.extend_from_slice(&(cp.val_compressed.len() as u32).to_be_bytes());
            out.extend_from_slice(&cp.val_compressed);
        }

        let tmp_path = path.with_extension("pcb1.tmp");
        fs::File::create(&tmp_path)
            .and_then(|mut file| file.write_all(&out))
            .map_err(|err| format!("failed to write batch {}: {err}", path.display()))?;
        fs::rename(&tmp_path, &path)
            .map_err(|err| format!("failed to rename batch {}: {err}", path.display()))?;

        Ok(partitions
            .iter()
            .enumerate()
            .map(|(i, cp)| {
                let data_len = (4 + cp.ts_compressed.len() + 4 + cp.val_compressed.len()) as u32;
                (
                    cp.key,
                    ChunkMeta {
                        min_ts: cp.min_ts,
                        max_ts: cp.max_ts,
                        point_count: cp.point_count,
                        min_val: cp.min_val,
                        max_val: cp.max_val,
                        sum_val: cp.sum_val,
                        path: path.clone(),
                        data_offset: data_offsets[i] as u64,
                        data_len,
                        encoding: cp.encoding,
                    },
                )
            })
            .collect())
    }

    // ── Compaction ───────────────────────────────────────────────────

    /// Merge each series' raw and undersized chunks into large pco chunks
    /// at maximum compression. Only chunks entirely older than `cutoff_ts`
    /// are eligible — the recent window stays in small/raw chunks so
    /// narrow dashboard queries never pay whole-chunk decompression.
    ///
    /// Crash safety (manifest protocol): replacement chunks are written
    /// as `.pending` files (invisible to rebuild_index), then a manifest
    /// records the renames and deletions before either happens. A crash
    /// at any point either leaves the pre-compaction state (stray
    /// .pending files are swept at startup) or is completed by
    /// `recover_compaction_manifest` on the next start. Old files are
    /// deleted only when no surviving index entry references them (batch
    /// files are shared across series).
    fn compact_partitions(&self, cutoff_ts: i64) -> EngineResult<(usize, usize)> {
        const SMALL_CHUNK_POINTS: u32 = 16 * 1024;
        const MAX_OUTPUT_POINTS: usize = 32 * 1024;
        const COMPACTION_LEVEL: usize = 12;

        // Single-flight: the cold-flush timer and the explicit NIF may
        // both call in; one compaction at a time.
        if self
            .compaction_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok((0, 0));
        }
        let _guard = ColdFlushGuard {
            flag: &self.compaction_running,
        };

        // Group eligible chunks by series: all raw chunks, plus pco
        // chunks small enough that merging improves the ratio.
        let mut candidates: HashMap<PartitionKey, Vec<(ChunkKey, ChunkMeta)>> = HashMap::new();
        {
            let index = self.index_read();
            for (chunk_key, meta) in index.iter() {
                let eligible = meta.max_ts < cutoff_ts
                    && (meta.encoding == ENC_RAW || meta.point_count < SMALL_CHUNK_POINTS);
                if eligible {
                    candidates
                        .entry(chunk_key.0)
                        .or_default()
                        .push((*chunk_key, meta.clone()));
                }
            }
        }
        candidates.retain(|_, chunks| {
            chunks.len() >= 2 || chunks.iter().any(|(_, m)| m.encoding == ENC_RAW)
        });

        if candidates.is_empty() {
            return Ok((0, 0));
        }

        // Phase 1: write every replacement chunk as .pending — nothing
        // is visible to queries or rebuild_index yet.
        let mut plans: Vec<(PartitionKey, Vec<(ChunkKey, ChunkMeta)>, Vec<ChunkMeta>)> = Vec::new();
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();

        for (key, chunks) in candidates {
            let mut per_query_cache: HashMap<PathBuf, Arc<Vec<u8>>> = HashMap::new();
            let mut points: Vec<(i64, f64)> = Vec::new();
            for (_, meta) in &chunks {
                points.extend(self.read_chunk_data_cached(
                    meta,
                    i64::MIN,
                    i64::MAX,
                    &mut per_query_cache,
                )?);
            }
            if points.is_empty() {
                continue;
            }
            points.sort_unstable_by_key(|&(ts, _)| ts);

            let mut new_metas: Vec<ChunkMeta> = Vec::new();
            for slice in points.chunks(MAX_OUTPUT_POINTS) {
                let (ts, vals): (Vec<i64>, Vec<f64>) = slice.iter().copied().unzip();
                let cp = self.encode_partition(&key, &ts, &vals, ENC_PCO, COMPACTION_LEVEL)?;
                let (meta, written) = self.write_individual_chunk_at(&cp, true)?;
                renames.push((written, meta.path.clone()));
                new_metas.push(meta);
            }
            plans.push((key, chunks, new_metas));
        }

        if plans.is_empty() {
            return Ok((0, 0));
        }

        // Old files are deletable only if no surviving (non-replaced)
        // index entry still references them.
        let removed: HashSet<ChunkKey> = plans
            .iter()
            .flat_map(|(_, chunks, _)| chunks.iter().map(|(chunk_key, _)| *chunk_key))
            .collect();
        let deletable: HashSet<PathBuf> = {
            let index = self.index_read();
            let survivors: HashSet<&PathBuf> = index
                .iter()
                .filter(|(entry_key, _)| !removed.contains(entry_key))
                .map(|(_, m)| &m.path)
                .collect();
            plans
                .iter()
                .flat_map(|(_, chunks, _)| chunks.iter().map(|(_, m)| m.path.clone()))
                .filter(|p| !survivors.contains(p))
                .collect()
        };

        // Phase 2: durable intent, then execute. From here, a crash is
        // completed by recovery at next startup.
        self.write_compaction_manifest(&renames, &deletable)?;

        for (pending, final_path) in &renames {
            fs::rename(pending, final_path).map_err(|err| {
                format!("failed to finalize chunk {}: {err}", final_path.display())
            })?;
        }

        {
            let mut index = self.index_write();
            for (key, chunks, new_metas) in &plans {
                for (chunk_key, _) in chunks {
                    index.remove(chunk_key);
                }
                for meta in new_metas {
                    index.insert((*key, meta.min_ts, self.next_chunk_seq()), meta.clone());
                }
            }
        }

        for path in &deletable {
            self.file_cache.remove(path);
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_file(Self::manifest_path(&self.data_dir));

        let series_compacted = plans.len();
        let chunks_replaced = plans.iter().map(|(_, chunks, _)| chunks.len()).sum();
        Ok((series_compacted, chunks_replaced))
    }

    fn manifest_path(data_dir: &std::path::Path) -> PathBuf {
        data_dir.join("compaction.manifest")
    }

    /// Durably record compaction intent: pending->final renames and the
    /// old files to delete. Written via tmp+rename so it is atomic.
    fn write_compaction_manifest(
        &self,
        renames: &[(PathBuf, PathBuf)],
        deletes: &HashSet<PathBuf>,
    ) -> EngineResult<()> {
        let mut out = String::new();
        for (pending, final_path) in renames {
            out.push_str(&format!(
                "P\t{}\t{}\n",
                pending.display(),
                final_path.display()
            ));
        }
        for path in deletes {
            out.push_str(&format!("D\t{}\n", path.display()));
        }

        let manifest = Self::manifest_path(&self.data_dir);
        let tmp = manifest.with_extension("manifest.tmp");
        fs::write(&tmp, out).map_err(|e| format!("failed to write manifest: {e}"))?;
        fs::rename(&tmp, &manifest).map_err(|e| format!("failed to commit manifest: {e}"))?;
        Ok(())
    }

    /// Complete an interrupted compaction at startup: finish any pending
    /// renames, delete superseded files, remove the manifest. Called
    /// before rebuild_index. If no manifest exists this is a no-op
    /// (stray .pending files from a pre-manifest crash are swept by
    /// scan_dir_recursive instead, leaving the pre-compaction state).
    fn recover_compaction_manifest(data_dir: &std::path::Path) {
        let manifest = Self::manifest_path(data_dir);
        let Ok(content) = fs::read_to_string(&manifest) else {
            return;
        };

        for line in content.lines() {
            let mut parts = line.split('\t');
            match parts.next() {
                Some("P") => {
                    if let (Some(pending), Some(final_path)) = (parts.next(), parts.next()) {
                        let pending = PathBuf::from(pending);
                        if pending.exists() {
                            let _ = fs::rename(&pending, PathBuf::from(final_path));
                        }
                    }
                }
                Some("D") => {
                    if let Some(path) = parts.next() {
                        let _ = fs::remove_file(path);
                    }
                }
                _ => {}
            }
        }
        let _ = fs::remove_file(&manifest);
    }

    fn ensure_dir(&self, path: &std::path::Path) -> io::Result<()> {
        let dir = path
            .parent()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("path has no parent: {:?}", path),
                )
            })?
            .to_path_buf();
        let mut dirs = self.created_dirs_lock();
        if !dirs.contains(&dir) {
            fs::create_dir_all(&dir)?;
            dirs.insert(dir);
        }
        Ok(())
    }

    fn next_file_id(&self) -> String {
        let seq = self.batch_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{:08}", self.instance_id, seq)
    }

    fn drain_partition_if<F>(
        &self,
        key: &PartitionKey,
        should_drain: F,
    ) -> Option<(Vec<i64>, Vec<f64>)>
    where
        F: FnOnce(&PartitionBuffer) -> bool,
    {
        let mut entry = self.partitions.get_mut(key)?;
        if !should_drain(&entry) {
            return None;
        }

        let freed = entry.memory_bytes();
        let timestamps = std::mem::take(&mut entry.timestamps);
        let values = std::mem::take(&mut entry.values);
        entry.queued_for_flush = false;
        entry.last_write = Instant::now();
        drop(entry);

        if freed > 0 {
            self.buffer_memory.fetch_sub(freed, Ordering::Relaxed);
        }

        Some((timestamps, values))
    }

    fn clear_flush_queued(&self, key: &PartitionKey) {
        if let Some(mut entry) = self.partitions.get_mut(key) {
            entry.queued_for_flush = false;
        }
    }

    // ── Queries ──────────────────────────────────────────────────────

    /// Query by metric name + label filter. Returns data for all matching series.
    fn query_range_labeled(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(Labels, Vec<(i64, f64)>)>> {
        let candidates: Vec<(i64, Labels)> = {
            let reg = self.series_read();
            reg.find_series(metric_name, label_filter)
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        candidates
            .into_par_iter()
            .map(|(sid, labels)| {
                let points = self.query_range_by_id(sid, t_start, t_end)?;
                Ok(if points.is_empty() {
                    None
                } else {
                    Some((labels, points))
                })
            })
            .filter_map(
                |result: EngineResult<Option<(Labels, Vec<(i64, f64)>)>>| match result {
                    Ok(Some(value)) => Some(Ok(value)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect()
    }

    /// Query a single series by ID.
    fn query_range_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
    ) -> EngineResult<Vec<(i64, f64)>> {
        let mut file_cache: HashMap<PathBuf, Arc<Vec<u8>>> = HashMap::new();
        self.query_range_by_id_cached(series_id, t_start, t_end, &mut file_cache)
    }

    fn query_range_by_id_cached(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        file_cache: &mut HashMap<PathBuf, Arc<Vec<u8>>>,
    ) -> EngineResult<Vec<(i64, f64)>> {
        let pk = PartitionKey { series_id };

        let matching: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((k, _, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };

        let mut results = Vec::new();
        for meta in &matching {
            results.extend(self.read_chunk_data_cached(meta, t_start, t_end, file_cache)?);
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                let ts = buf.timestamps[i];
                if ts >= t_start && ts <= t_end {
                    results.push((ts, buf.values[i]));
                }
            }
        }

        results.sort_by_key(|&(ts, _)| ts);
        Ok(results)
    }

    /// Aggregate query by metric + labels. Returns per-series aggregates.
    fn query_aggregate_labeled(
        &self,
        metric_name: &str,
        label_filter: &Labels,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
    ) -> EngineResult<Vec<(Labels, f64)>> {
        let candidates: Vec<(i64, Labels)> = {
            let reg = self.series_read();
            reg.find_series(metric_name, label_filter)
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        candidates
            .into_par_iter()
            .map(|(sid, labels)| {
                let value = self.query_aggregate_by_id(sid, t_start, t_end, agg)?;
                Ok(value.map(|val| (labels, val)))
            })
            .filter_map(|result: EngineResult<Option<(Labels, f64)>>| match result {
                Ok(Some(value)) => Some(Ok(value)),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            })
            .collect()
    }

    fn query_aggregate_by_id(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
    ) -> EngineResult<Option<f64>> {
        let mut file_cache: HashMap<PathBuf, Arc<Vec<u8>>> = HashMap::new();
        self.query_aggregate_by_id_cached(series_id, t_start, t_end, agg, &mut file_cache)
    }

    fn query_aggregate_by_id_cached(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
        file_cache: &mut HashMap<PathBuf, Arc<Vec<u8>>>,
    ) -> EngineResult<Option<f64>> {
        let pk = PartitionKey { series_id };

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: Option<f64> = None;
        let mut global_max: Option<f64> = None;

        let chunks: Vec<ChunkMeta> = {
            let index = self.index_read();
            index
                .range((pk, i64::MIN, u64::MIN)..)
                .take_while(|((k, _, _), _)| k == &pk)
                .filter(|(_, meta)| meta.min_ts <= t_end && meta.max_ts >= t_start)
                .map(|(_, meta)| meta.clone())
                .collect()
        };

        for meta in &chunks {
            if meta.min_ts >= t_start && meta.max_ts <= t_end {
                total_count += meta.point_count as u64;
                total_sum += meta.sum_val;
                global_min = Some(match global_min {
                    Some(m) => m.min(meta.min_val),
                    None => meta.min_val,
                });
                global_max = Some(match global_max {
                    Some(m) => m.max(meta.max_val),
                    None => meta.max_val,
                });
            } else {
                let points = self.read_chunk_data_cached(meta, t_start, t_end, file_cache)?;
                for &(_, val) in &points {
                    total_count += 1;
                    total_sum += val;
                    global_min = Some(match global_min {
                        Some(m) => m.min(val),
                        None => val,
                    });
                    global_max = Some(match global_max {
                        Some(m) => m.max(val),
                        None => val,
                    });
                }
            }
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                if buf.timestamps[i] >= t_start && buf.timestamps[i] <= t_end {
                    let val = buf.values[i];
                    total_count += 1;
                    total_sum += val;
                    global_min = Some(match global_min {
                        Some(m) => m.min(val),
                        None => val,
                    });
                    global_max = Some(match global_max {
                        Some(m) => m.max(val),
                        None => val,
                    });
                }
            }
        }

        if total_count == 0 {
            return Ok(None);
        }
        Ok(Some(match agg {
            AggFn::Avg => total_sum / total_count as f64,
            AggFn::Sum => total_sum,
            AggFn::Min => global_min.unwrap(),
            AggFn::Max => global_max.unwrap(),
            AggFn::Count => total_count as f64,
        }))
    }

    // ── Chunk reading ────────────────────────────────────────────────

    #[allow(dead_code)]
    fn read_chunk_data(
        &self,
        meta: &ChunkMeta,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, f64)>, String> {
        let mut file_cache: HashMap<PathBuf, Arc<Vec<u8>>> = HashMap::new();
        self.read_chunk_data_cached(meta, t_start, t_end, &mut file_cache)
    }

    fn read_chunk_data_cached(
        &self,
        meta: &ChunkMeta,
        t_start: i64,
        t_end: i64,
        per_query_cache: &mut HashMap<PathBuf, Arc<Vec<u8>>>,
    ) -> Result<Vec<(i64, f64)>, String> {
        let data: Arc<Vec<u8>> = if let Some(d) = per_query_cache.get(&meta.path) {
            Arc::clone(d)
        } else if let Some(entry) = self.file_cache.get(&meta.path) {
            if entry.0.elapsed() < FILE_CACHE_TTL {
                Arc::clone(&entry.1)
            } else {
                drop(entry);
                self.file_cache.remove(&meta.path);
                let data: Arc<Vec<u8>> = Arc::new(fs::read(&meta.path).map_err(|e| e.to_string())?);
                self.file_cache
                    .insert(meta.path.clone(), (Instant::now(), Arc::clone(&data)));
                data
            }
        } else {
            let data: Arc<Vec<u8>> = Arc::new(fs::read(&meta.path).map_err(|e| e.to_string())?);
            self.file_cache
                .insert(meta.path.clone(), (Instant::now(), Arc::clone(&data)));
            data
        };

        per_query_cache
            .entry(meta.path.clone())
            .or_insert_with(|| Arc::clone(&data));

        let (ts_data, val_data) = if meta.data_offset > 0 {
            Self::parse_partition_data(&data, meta.data_offset as usize)?
        } else {
            Self::parse_pco1_data(&data)?
        };

        let (timestamps, values): (Vec<i64>, Vec<f64>) = if meta.encoding == ENC_RAW {
            if ts_data.len() % 8 != 0 || val_data.len() % 8 != 0 {
                return Err(format!("raw payload misaligned in {}", meta.path.display()));
            }
            (
                ts_data
                    .chunks_exact(8)
                    .map(|b| i64::from_be_bytes(b.try_into().unwrap()))
                    .collect(),
                val_data
                    .chunks_exact(8)
                    .map(|b| f64::from_be_bytes(b.try_into().unwrap()))
                    .collect(),
            )
        } else {
            (
                pco::standalone::simple_decompress(ts_data).map_err(|e| e.to_string())?,
                pco::standalone::simple_decompress(val_data).map_err(|e| e.to_string())?,
            )
        };
        if timestamps.len() != values.len() {
            return Err(format!(
                "timestamp/value length mismatch in {}: {} vs {}",
                meta.path.display(),
                timestamps.len(),
                values.len()
            ));
        }

        let mut results = Vec::new();
        for i in 0..timestamps.len() {
            if timestamps[i] >= t_start && timestamps[i] <= t_end {
                results.push((timestamps[i], values[i]));
            }
        }
        Ok(results)
    }

    fn parse_partition_data(data: &[u8], offset: usize) -> Result<(&[u8], &[u8]), String> {
        if offset + 4 > data.len() {
            return Err(format!("offset {} past file len {}", offset, data.len()));
        }
        let mut pos = offset;
        let ts_size = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + ts_size + 4 > data.len() {
            return Err(format!("ts overrun at {}", offset));
        }
        let ts_data = &data[pos..pos + ts_size];
        pos += ts_size;
        let val_size = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + val_size > data.len() {
            return Err(format!("val overrun at {}", offset));
        }
        let val_data = &data[pos..pos + val_size];
        Ok((ts_data, val_data))
    }

    fn parse_pco1_data(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
        if data.len() < 4 || &data[0..4] != b"PCO1" {
            return Err("invalid PCO1".into());
        }
        let mut pos = 5;
        if pos + 4 + 16 + 2 > data.len() {
            return Err("truncated PCO1 header".into());
        }
        pos += 4;
        pos += 16;
        let pk_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + pk_len > data.len() {
            return Err("truncated PCO1 partition key".into());
        }
        pos += pk_len;
        if pos + 24 > data.len() {
            return Err("truncated PCO1 metadata".into());
        }
        pos += 24;
        if pos + 4 > data.len() {
            return Err("truncated PCO1 partition data".into());
        }
        Self::parse_partition_data(data, pos)
    }

    // ── Retention ────────────────────────────────────────────────────

    fn delete_before(&self, before_ts: i64) -> (usize, usize, Vec<String>) {
        let mut index = self.index_write();

        let to_remove: Vec<ChunkKey> = index
            .iter()
            .filter(|(_, meta)| meta.max_ts < before_ts)
            .map(|(k, _)| *k)
            .collect();

        let entries_removed = to_remove.len();
        let mut file_refcount: HashMap<PathBuf, usize> = HashMap::new();
        for meta in index.values() {
            *file_refcount.entry(meta.path.clone()).or_insert(0) += 1;
        }

        let mut files_to_delete: HashSet<PathBuf> = HashSet::new();
        for key in &to_remove {
            if let Some(meta) = index.remove(key) {
                if let Some(count) = file_refcount.get_mut(&meta.path) {
                    *count -= 1;
                    if *count == 0 {
                        files_to_delete.insert(meta.path.clone());
                    }
                }
            }
        }

        drop(index);
        let files_deleted = files_to_delete.len();
        let mut errors: Vec<String> = Vec::new();
        for path in &files_to_delete {
            if let Err(e) = fs::remove_file(path) {
                errors.push(format!("failed to remove {}: {}", path.display(), e));
            }
            if let Some(dir) = path.parent() {
                match fs::remove_dir(dir) {
                    Ok(()) => self.forget_created_dir(dir),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        self.forget_created_dir(dir);
                    }
                    Err(_) => {}
                }
            }
        }

        (entries_removed, files_deleted, errors)
    }

    // ── Index rebuild ────────────────────────────────────────────────

    fn rebuild_index(&self) {
        let mut index = self.index_write();
        for dir_name in &["chunks", "batches"] {
            let dir = self.data_dir.join(dir_name);
            if dir.exists() {
                self.scan_dir_recursive(&dir, &mut index);
            }
        }
    }

    fn scan_dir_recursive(&self, dir: &PathBuf, index: &mut BTreeMap<ChunkKey, ChunkMeta>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                self.scan_dir_recursive(&path, index);
            } else {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("pco1") => {
                        if let Ok(entries) = Self::read_pco1_header(&path) {
                            for (pk, meta) in entries {
                                index.insert((pk, meta.min_ts, self.next_chunk_seq()), meta);
                            }
                        }
                    }
                    Some("pcb1") => {
                        if let Ok(entries) = Self::read_pcb1_headers(&path) {
                            for (pk, meta) in entries {
                                index.insert((pk, meta.min_ts, self.next_chunk_seq()), meta);
                            }
                        }
                    }
                    // tmp: interrupted chunk write; pending: compaction
                    // that crashed before its manifest — old chunks are
                    // still intact, so dropping the orphan is correct.
                    Some("tmp") | Some("pending") => {
                        let _ = fs::remove_file(&path);
                    }
                    _ => {}
                }
            }
        }
    }

    fn read_pco1_header(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let fixed = read_exact_at(&mut file, 0, 31)?;
        if &fixed[0..4] != b"PCO1" {
            return Err("invalid".into());
        }
        let encoding = if fixed[4] == 2 { ENC_RAW } else { ENC_PCO };

        let mut pos = 5;
        let point_count = u32::from_be_bytes(fixed[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let min_ts = i64::from_be_bytes(fixed[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_ts = i64::from_be_bytes(fixed[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let pk_len = u16::from_be_bytes(fixed[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let variable = read_exact_at(&mut file, pos as u64, pk_len + 24)?;
        let pk_str = String::from_utf8_lossy(&variable[0..pk_len]).to_string();
        let mut pos = pk_len;
        let min_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let sum_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());

        // pk_str is the series_id as a string
        let series_id = pk_str.parse::<i64>().unwrap_or(0);

        Ok(vec![(
            PartitionKey { series_id },
            ChunkMeta {
                min_ts,
                max_ts,
                point_count,
                min_val,
                max_val,
                sum_val,
                path: path.clone(),
                data_offset: 0,
                data_len: 0,
                encoding,
            },
        )])
    }

    fn read_pcb1_headers(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let file_len = file.metadata().map_err(|e| e.to_string())?.len();
        let fixed = read_exact_at(&mut file, 0, 9)?;
        if &fixed[0..4] != b"PCB1" {
            return Err("invalid".into());
        }
        let encoding = if fixed[4] == 2 { ENC_RAW } else { ENC_PCO };

        let n = u32::from_be_bytes(fixed[5..9].try_into().unwrap()) as usize;
        let table_len = n
            .checked_mul(64)
            .ok_or_else(|| "PCB1 table overflow".to_string())?;
        let table_start = 9usize;
        let table_end = table_start
            .checked_add(table_len)
            .ok_or_else(|| "PCB1 table overflow".to_string())?;
        let table = read_exact_at(&mut file, table_start as u64, table_len)?;
        let mut results = Vec::with_capacity(n);
        let mut pos = 0;
        let mut data_entries: Vec<(u64, u32)> = Vec::with_capacity(n);
        for _ in 0..n {
            let series_id = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let point_count = u32::from_be_bytes(table[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let min_ts = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_ts = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let min_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let sum_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_len = u32::from_be_bytes(table[pos..pos + 4].try_into().unwrap());
            pos += 4;

            data_entries.push((data_offset, data_len));
            results.push((
                PartitionKey { series_id },
                ChunkMeta {
                    min_ts,
                    max_ts,
                    point_count,
                    min_val,
                    max_val,
                    sum_val,
                    path: path.clone(),
                    data_offset,
                    data_len,
                    encoding,
                },
            ));
        }

        for (data_offset, data_len) in data_entries {
            if data_offset < table_end as u64 {
                return Err(format!(
                    "PCB1 data offset {} is within table region (table ends at {})",
                    data_offset, table_end
                ));
            }
            let end = data_offset
                .checked_add(data_len as u64)
                .ok_or_else(|| "PCB1 data entry overflow".to_string())?;
            if end > file_len {
                return Err(format!(
                    "PCB1 data entry overflows file at offset {} (end {} > {})",
                    data_offset, end, file_len
                ));
            }
        }

        Ok(results)
    }

    fn info(&self) -> EngineInfo {
        let index = self.index_read();
        let series_reg = self.series_read();
        let chunk_count = index.len();
        let partition_count = self.partitions.len();
        let series_count = series_reg.series_count();
        let buffered_points: usize = self
            .partitions
            .iter()
            .map(|e| e.value().timestamps.len())
            .sum();
        let buffer_memory = self.buffer_memory.load(Ordering::Relaxed);

        let mut unique_files: HashSet<&PathBuf> = HashSet::new();
        let mut total_disk_points: u64 = 0;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;
        for meta in index.values() {
            total_disk_points += meta.point_count as u64;
            unique_files.insert(&meta.path);
            oldest_ts = match oldest_ts {
                Some(existing) => Some(existing.min(meta.min_ts)),
                None => Some(meta.min_ts),
            };
            newest_ts = match newest_ts {
                Some(existing) => Some(existing.max(meta.max_ts)),
                None => Some(meta.max_ts),
            };
        }

        for entry in self.partitions.iter() {
            let buf = entry.value();
            if let Some(min_ts) = buf.timestamps.iter().min() {
                oldest_ts = match oldest_ts {
                    Some(existing) => Some(existing.min(*min_ts)),
                    None => Some(*min_ts),
                };
            }
            if let Some(max_ts) = buf.timestamps.iter().max() {
                newest_ts = match newest_ts {
                    Some(existing) => Some(existing.max(*max_ts)),
                    None => Some(*max_ts),
                };
            }
        }

        let total_bytes: u64 = unique_files
            .iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|s| s.len())
            .sum();
        let total_points = total_disk_points + buffered_points as u64;
        let bytes_per_point = if total_disk_points > 0 {
            total_bytes as f64 / total_disk_points as f64
        } else {
            0.0
        };

        EngineInfo {
            chunk_count,
            partition_count,
            series_count,
            disk_points: total_disk_points,
            buffered_points,
            total_points,
            total_bytes,
            bytes_per_point,
            buffer_memory,
            file_count: unique_files.len(),
            oldest_ts,
            newest_ts,
        }
    }
}

struct EngineInfo {
    chunk_count: usize,
    partition_count: usize,
    series_count: usize,
    disk_points: u64,
    buffered_points: usize,
    total_points: u64,
    total_bytes: u64,
    bytes_per_point: f64,
    buffer_memory: usize,
    file_count: usize,
    oldest_ts: Option<i64>,
    newest_ts: Option<i64>,
}

#[derive(Clone, Copy)]
enum AggFn {
    Avg,
    Sum,
    Min,
    Max,
    Count,
}

// ═══════════════════════════════════════════════════════════════════════
// Prometheus text-format parser (bench prototype)
//
// Mirrors c_src/prometheus_nif.cpp semantics: entries are
// (name, [(label_key, label_value)], value, timestamp), timestamp 0 when
// absent, NaN/Inf values rejected, malformed non-comment lines counted
// as errors. Exposed as two NIFs so parse cost and term-materialization
// cost can be measured separately.
// ═══════════════════════════════════════════════════════════════════════

/// Parse a Prometheus sample value. Rejects NaN/Inf — the BEAM cannot
/// represent non-finite floats.
fn parse_prom_value(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let v: f64 = s.parse().ok()?;
    v.is_finite().then_some(v)
}

/// Parse the inside of a `{key="val",key2="val2"}` label block into `out`.
/// Escaped characters in values are kept raw, as the C++ parser does.
fn parse_prom_labels_into<'a>(mut s: &'a [u8], out: &mut Vec<(&'a [u8], &'a [u8])>) {
    loop {
        while let Some((&b, rest)) = s.split_first() {
            if b == b' ' || b == b',' {
                s = rest;
            } else {
                break;
            }
        }
        if s.is_empty() {
            break;
        }

        let Some(eq) = s.iter().position(|&b| b == b'=') else {
            break;
        };
        let mut key = &s[..eq];
        while let [rest @ .., b' '] = key {
            key = rest;
        }
        s = &s[eq + 1..];

        let Some((&b'"', rest)) = s.split_first() else {
            break;
        };
        s = rest;

        let mut i = 0;
        while i < s.len() && s[i] != b'"' {
            if s[i] == b'\\' && i + 1 < s.len() {
                i += 2;
            } else {
                i += 1;
            }
        }
        out.push((key, &s[..i]));
        s = if i < s.len() { &s[i + 1..] } else { &s[i..] };
    }
}

/// Parse one exposition line. Labels land in the caller's scratch buffer;
/// returns (name, value, timestamp) on success. Returns None for comments,
/// blanks, and malformed lines — the caller decides which count as errors.
fn parse_prom_line_into<'a>(
    line: &'a [u8],
    labels: &mut Vec<(&'a [u8], &'a [u8])>,
) -> Option<(&'a [u8], f64, i64)> {
    let line = line.trim_ascii();
    if line.is_empty() || line[0] == b'#' {
        return None;
    }

    let name_end = line
        .iter()
        .position(|&b| b == b'{' || b == b' ' || b == b'\t')?;
    if name_end == 0 {
        return None;
    }
    let name = &line[..name_end];

    let rest = if line[name_end] == b'{' {
        let close = name_end
            + 1
            + line[name_end + 1..].iter().position(|&b| b == b'}')?;
        parse_prom_labels_into(&line[name_end + 1..close], labels);
        &line[close + 1..]
    } else {
        &line[name_end..]
    };

    let mut fields = rest
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|f| !f.is_empty());
    let value = parse_prom_value(fields.next()?)?;
    let timestamp = fields
        .next()
        .and_then(|f| std::str::from_utf8(f).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    Some((name, value, timestamp))
}

/// Streaming parse: invokes `sink` once per valid sample with borrowed
/// views into `data`. One scratch label buffer is reused across all lines,
/// so steady-state parsing performs zero heap allocations. Returns
/// (entry_count, error_count).
fn parse_prom_body_visit<'a, F>(data: &'a [u8], mut sink: F) -> (usize, usize)
where
    F: FnMut(&'a [u8], &[(&'a [u8], &'a [u8])], f64, i64),
{
    let mut labels: Vec<(&[u8], &[u8])> = Vec::with_capacity(16);
    let mut count = 0;
    let mut errors = 0;

    for line in data.split(|&b| b == b'\n') {
        labels.clear();
        match parse_prom_line_into(line, &mut labels) {
            Some((name, value, timestamp)) => {
                count += 1;
                sink(name, &labels, value, timestamp);
            }
            None => {
                let t = line.trim_ascii();
                if !t.is_empty() && t[0] != b'#' {
                    errors += 1;
                }
            }
        }
    }
    (count, errors)
}

/// Bench-only NIF: parse the body and return (entry_count, error_count).
/// Zero BEAM terms are built per entry — this measures pure parse cost
/// (see bench/ingest_segments_bench.exs).
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_prometheus_count(body: Binary) -> (usize, usize) {
    parse_prom_body_visit(body.as_slice(), |_name, _labels, _value, _ts| {})
}

/// Make a zero-copy sub-binary term for a parse slice. The slice is
/// guaranteed to point into `body`, so (offset, len) is always in range.
/// The resulting term shares the body's bytes — no allocation, no copy.
fn slice_term<'a>(env: Env<'a>, body: &Binary<'a>, slice: &[u8]) -> Term<'a> {
    let offset = slice.as_ptr() as usize - body.as_slice().as_ptr() as usize;
    body.make_subbinary(offset, slice.len())
        .expect("parse slice within body")
        .encode(env)
}

/// Production Prometheus text parser (replaced the former C++ NIF):
/// {[{name, [{k, v}, ...], value, ts}, ...], error_count}, timestamp 0
/// when absent.
///
/// Strings are emitted as sub-binaries of the request body: O(1) each,
/// zero copies. Trade-off: the entry terms keep the whole body binary
/// alive until they are garbage collected — fine for transient scrape
/// processing, wrong for long-lived storage of small pieces. Anything
/// that stores these binaries long-term must :binary.copy/1 them first
/// (see cache_series_id in TimelessMetrics.RustEngine).
#[rustler::nif(schedule = "DirtyCpu")]
fn parse_prometheus<'a>(
    env: Env<'a>,
    body: Binary<'a>,
) -> rustler::NifResult<(Term<'a>, usize)> {
    let mut list = Term::list_new_empty(env);
    let mut pair_scratch: Vec<Term<'a>> = Vec::with_capacity(16);

    let (_count, errors) = parse_prom_body_visit(body.as_slice(), |name, labels, value, ts| {
        pair_scratch.clear();
        for &(k, v) in labels {
            pair_scratch.push(make_tuple(
                env,
                &[slice_term(env, &body, k), slice_term(env, &body, v)],
            ));
        }
        let entry = make_tuple(
            env,
            &[
                slice_term(env, &body, name),
                pair_scratch.encode(env),
                value.encode(env),
                ts.encode(env),
            ],
        );
        list = list.list_prepend(entry);
    });

    Ok((list.list_reverse()?, errors))
}

// ═══════════════════════════════════════════════════════════════════════
// NIF interface
// ═══════════════════════════════════════════════════════════════════════

#[rustler::nif]
fn engine_new(
    data_dir: String,
    flush_threshold: usize,
    min_flush_size: usize,
    compression_level: usize,
    memory_budget_mb: usize,
    defer_compression: bool,
) -> ResourceArc<EngineResource> {
    let budget = if memory_budget_mb == 0 {
        usize::MAX
    } else {
        memory_budget_mb * 1024 * 1024
    };
    ResourceArc::new(EngineResource {
        engine: Engine::new(
            PathBuf::from(data_dir),
            flush_threshold,
            min_flush_size,
            compression_level,
            budget,
            defer_compression,
        ),
    })
}

/// Force a compaction pass (raw/undersized chunks -> large pco chunks).
/// Also runs automatically from the cold-flush timer in raw-first mode.
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_compact(
    resource: ResourceArc<EngineResource>,
    cutoff_ts: i64,
) -> Result<(usize, usize), String> {
    resource.deref().engine.compact_partitions(cutoff_ts)
}

/// Write labeled entries: [{metric_name, %{label => value}, timestamp, value}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_write_batch_labeled(
    resource: ResourceArc<EngineResource>,
    entries: Vec<(String, HashMap<String, String>, i64, f64)>,
) -> Result<Atom, String> {
    resource.deref().engine.write_batch_labeled(entries)?;
    Ok(atoms::ok())
}

/// Write pre-resolved binary: [series_id: i64, ts: i64, val: f64] × N (24 bytes each)
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_write_batch_raw(
    resource: ResourceArc<EngineResource>,
    data: Binary,
) -> Result<Atom, String> {
    resource.deref().engine.write_batch_raw(data.as_slice())?;
    Ok(atoms::ok())
}

/// Resolve (metric, labels) → series_id for pre-resolved write path.
#[rustler::nif]
fn engine_resolve_series(
    resource: ResourceArc<EngineResource>,
    metric: String,
    labels: HashMap<String, String>,
) -> Result<(Atom, i64), String> {
    let labels_bt: BTreeMap<String, String> = labels.into_iter().collect();
    Ok((
        atoms::ok(),
        resource
            .deref()
            .engine
            .resolve_series(&metric, &labels_bt)?,
    ))
}

#[rustler::nif]
fn engine_resolve_series_batch(
    resource: ResourceArc<EngineResource>,
    entries: Vec<(String, HashMap<String, String>)>,
) -> Result<(Atom, Vec<i64>), String> {
    let normalized: Vec<(String, BTreeMap<String, String>)> = entries
        .into_iter()
        .map(|(metric, labels)| (metric, labels.into_iter().collect()))
        .collect();

    Ok((
        atoms::ok(),
        resource.deref().engine.resolve_series_batch(&normalized)?,
    ))
}

/// Fused ingest: parse Prometheus text and write points in one NIF call.
/// Returns {:ok, {samples_written, parse_errors}}.
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_ingest_prometheus(
    resource: ResourceArc<EngineResource>,
    body: Binary,
    default_ts: i64,
) -> Result<(usize, usize), String> {
    resource
        .deref()
        .engine
        .ingest_prometheus(body.as_slice(), default_ts)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_pending(resource: ResourceArc<EngineResource>) -> Result<(Atom, usize), String> {
    Ok((atoms::ok(), resource.deref().engine.flush_pending()?))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_cold(
    resource: ResourceArc<EngineResource>,
    max_idle_secs: u64,
) -> Result<(Atom, usize, usize, usize), String> {
    let (f, e, fi) = resource.deref().engine.flush_cold(max_idle_secs)?;
    Ok((atoms::ok(), f, e, fi))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_by_memory(resource: ResourceArc<EngineResource>) -> Result<(Atom, usize), String> {
    Ok((atoms::ok(), resource.deref().engine.flush_by_memory()?))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush(resource: ResourceArc<EngineResource>) -> Result<Atom, String> {
    resource.deref().engine.flush_all()?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_delete_before(
    resource: ResourceArc<EngineResource>,
    before_ts: i64,
) -> (Atom, usize, usize, Vec<String>) {
    let (e, f, errors) = resource.deref().engine.delete_before(before_ts);
    (atoms::ok(), e, f, errors)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_shutdown(resource: ResourceArc<EngineResource>) -> Result<Atom, String> {
    resource.deref().engine.shutdown()?;
    Ok(atoms::ok())
}

/// Query range for all series matching metric + labels.
/// Returns [{%{label => value}, [{ts, val}]}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_query_range(
    resource: ResourceArc<EngineResource>,
    metric: String,
    labels: HashMap<String, String>,
    t_start: i64,
    t_end: i64,
) -> Result<(Atom, Vec<(HashMap<String, String>, Vec<(i64, f64)>)>), String> {
    let label_filter: BTreeMap<String, String> = labels.into_iter().collect();
    let results =
        resource
            .deref()
            .engine
            .query_range_labeled(&metric, &label_filter, t_start, t_end)?;
    let out: Vec<(HashMap<String, String>, Vec<(i64, f64)>)> = results
        .into_iter()
        .map(|(labels, points)| (labels.into_iter().collect(), points))
        .collect();
    Ok((atoms::ok(), out))
}

/// Aggregate query for all series matching metric + labels.
/// Returns [{%{label => value}, value}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_query_aggregate(
    resource: ResourceArc<EngineResource>,
    metric: String,
    labels: HashMap<String, String>,
    t_start: i64,
    t_end: i64,
    agg: Atom,
) -> Result<(Atom, Vec<(HashMap<String, String>, f64)>), String> {
    let label_filter: BTreeMap<String, String> = labels.into_iter().collect();
    let results = resource.deref().engine.query_aggregate_labeled(
        &metric,
        &label_filter,
        t_start,
        t_end,
        match_agg(agg)?,
    )?;
    let out: Vec<(HashMap<String, String>, f64)> = results
        .into_iter()
        .map(|(labels, val)| (labels.into_iter().collect(), val))
        .collect();
    Ok((atoms::ok(), out))
}

/// List all metric names.
#[rustler::nif]
fn engine_list_metrics(resource: ResourceArc<EngineResource>) -> (Atom, Vec<String>) {
    (
        atoms::ok(),
        resource
            .deref()
            .engine
            .series
            .read()
            .unwrap()
            .list_metrics(),
    )
}

/// List all label names.
#[rustler::nif]
fn engine_list_labels(resource: ResourceArc<EngineResource>) -> (Atom, Vec<String>) {
    (
        atoms::ok(),
        resource
            .deref()
            .engine
            .series
            .read()
            .unwrap()
            .all_label_names(),
    )
}

/// List values for a label key, optionally scoped to a metric.
#[rustler::nif]
fn engine_label_values(
    resource: ResourceArc<EngineResource>,
    metric: String,
    label_key: String,
) -> (Atom, Vec<String>) {
    (
        atoms::ok(),
        resource
            .deref()
            .engine
            .series
            .read()
            .unwrap()
            .label_values(&metric, &label_key),
    )
}

/// List all series (labels) for a metric.
#[rustler::nif]
fn engine_list_series(
    resource: ResourceArc<EngineResource>,
    metric: String,
) -> (Atom, Vec<HashMap<String, String>>) {
    let reg = resource.deref().engine.series_read();
    let ids = reg.find_series(&metric, &BTreeMap::new());
    let out: Vec<HashMap<String, String>> = ids
        .into_iter()
        .filter_map(|id| {
            reg.info_for(id)
                .map(|info| info.labels.clone().into_iter().collect())
        })
        .collect();
    (atoms::ok(), out)
}

#[rustler::nif]
fn engine_info<'a>(
    env: rustler::Env<'a>,
    resource: ResourceArc<EngineResource>,
) -> Result<Term<'a>, String> {
    let info = resource.deref().engine.info();
    let map = rustler::types::map::map_new(env);
    let map = map.map_put("chunk_count", info.chunk_count as i64).unwrap();
    let map = map
        .map_put("partition_count", info.partition_count as i64)
        .unwrap();
    let map = map
        .map_put("series_count", info.series_count as i64)
        .unwrap();
    let map = map.map_put("disk_points", info.disk_points as i64).unwrap();
    let map = map
        .map_put("buffered_points", info.buffered_points as i64)
        .unwrap();
    let map = map
        .map_put("total_points", info.total_points as i64)
        .unwrap();
    let map = map.map_put("total_bytes", info.total_bytes as i64).unwrap();
    let map = map
        .map_put("bytes_per_point", info.bytes_per_point)
        .unwrap();
    let map = map
        .map_put("buffer_memory_bytes", info.buffer_memory as i64)
        .unwrap();
    let map = map
        .map_put(
            "buffer_memory_mb",
            info.buffer_memory as f64 / 1024.0 / 1024.0,
        )
        .unwrap();
    let map = map.map_put("file_count", info.file_count as i64).unwrap();
    let map = if let Some(oldest_ts) = info.oldest_ts {
        map.map_put("oldest_timestamp", oldest_ts).unwrap()
    } else {
        map
    };
    let map = if let Some(newest_ts) = info.newest_ts {
        map.map_put("newest_timestamp", newest_ts).unwrap()
    } else {
        map
    };
    Ok(map)
}

fn match_agg(atom: Atom) -> Result<AggFn, String> {
    if atom == atoms::avg() {
        Ok(AggFn::Avg)
    } else if atom == atoms::sum() {
        Ok(AggFn::Sum)
    } else if atom == atoms::min() {
        Ok(AggFn::Min)
    } else if atom == atoms::max() {
        Ok(AggFn::Max)
    } else if atom == atoms::count() {
        Ok(AggFn::Count)
    } else {
        Err(format!("unknown aggregation function: {:?}", atom))
    }
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    env.register::<EngineResource>().is_ok()
}

rustler::init!("Elixir.TimelessMetrics.RustEngine.Nif", load = load);

fn partition_vec_memory(timestamps: &Vec<i64>, values: &Vec<f64>) -> usize {
    (timestamps.len() + values.len()) * 8
}

fn read_exact_at(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>, String> {
    let mut buf = vec![0u8; len];
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fused_hash_matches_hashmap_hash() {
        // Both resolve paths must produce identical cache keys.
        let mut hm = HashMap::new();
        hm.insert("host".to_string(), "web-1".to_string());
        hm.insert("region".to_string(), "us-east".to_string());
        let pairs = [("host", "web-1"), ("region", "us-east")];
        assert_eq!(
            fast_series_hash("cpu", &hm),
            fast_series_hash_pairs("cpu", &pairs)
        );
    }

    #[test]
    fn fused_ingest_shares_series_with_labeled_path() {
        let engine = Engine::new(test_dir("fused"), 100, 64, 8, usize::MAX, false);

        // Create the series through the labeled (HashMap) path first
        let mut hm = HashMap::new();
        hm.insert("host".to_string(), "a".to_string());
        let labeled_id = engine.resolve_cached("cpu", &hm).unwrap();

        // Fused ingest of the same series must resolve to the same id
        let body = b"cpu{host=\"a\"} 1.5 1700000000000\ncpu{host=\"a\"} 2.5\n";
        let (count, errors) = engine.ingest_prometheus(body, 1_700_000_100).unwrap();
        assert_eq!((count, errors), (2, 0));

        assert_eq!(engine.series_read().series_count(), 1);
        let points = engine
            .query_range_by_id(labeled_id, 0, i64::MAX)
            .unwrap();
        // ms timestamp normalized to seconds; missing timestamp -> default
        assert_eq!(points, vec![(1_700_000_000, 1.5), (1_700_000_100, 2.5)]);
    }

    #[test]
    fn fused_ingest_duplicate_label_keys_keep_last() {
        let engine = Engine::new(test_dir("fused_dup"), 100, 64, 8, usize::MAX, false);
        let (count, errors) = engine
            .ingest_prometheus(b"m{k=\"1\",k=\"2\"} 5.0\n", 100)
            .unwrap();
        assert_eq!((count, errors), (1, 0));

        let reg = engine.series_read();
        let info = reg.info_for(1).unwrap();
        assert_eq!(info.labels.get("k").map(String::as_str), Some("2"));
    }

    #[test]
    fn raw_first_flush_roundtrips_and_survives_restart() {
        let dir = test_dir("raw_first");
        let engine = Engine::new(dir.clone(), 100, 1, 8, usize::MAX, true);

        for i in 0..10 {
            engine.write_point(1, i, i as f64 * 1.5);
        }
        engine.flush_all().unwrap();

        // Chunk on disk is raw-encoded and queryable
        assert!(engine.index_read().values().all(|m| m.encoding == ENC_RAW));
        assert_eq!(engine.query_range_by_id(1, 0, 100).unwrap().len(), 10);

        // Restart: rebuild_index must recover the raw encoding from the
        // version byte and still serve the data
        let restarted = Engine::new(dir, 100, 1, 8, usize::MAX, true);
        assert!(restarted.index_read().values().all(|m| m.encoding == ENC_RAW));
        let points = restarted.query_range_by_id(1, 0, 100).unwrap();
        assert_eq!(points.len(), 10);
        assert_eq!(points[3], (3, 4.5));
    }

    #[test]
    fn compaction_merges_raw_chunks_into_one_pco_chunk() {
        let dir = test_dir("compact");
        let engine = Engine::new(dir, 100, 1, 8, usize::MAX, true);

        // Three separate raw flushes -> three raw chunks for series 1
        for round in 0..3 {
            for i in 0..50 {
                engine.write_point(1, round * 50 + i, i as f64);
            }
            engine.flush_all().unwrap();
        }
        assert_eq!(engine.index_read().len(), 3);

        let (series, replaced) = engine.compact_partitions(i64::MAX).unwrap();
        assert_eq!((series, replaced), (1, 3));

        // One pco chunk remains; data intact and ordered
        let (encoding, point_count) = {
            let index = engine.index_read();
            assert_eq!(index.len(), 1);
            let meta = index.values().next().unwrap();
            (meta.encoding, meta.point_count)
        };
        assert_eq!(encoding, ENC_PCO);
        assert_eq!(point_count, 150);

        let points = engine.query_range_by_id(1, 0, 1000).unwrap();
        assert_eq!(points.len(), 150);
        assert!(points.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    #[test]
    fn compaction_crash_after_manifest_recovers_without_duplicates() {
        let dir = test_dir("compact_crash");
        let engine = Engine::new(dir.clone(), 100, 1, 8, usize::MAX, true);

        // Two raw chunks for series 1
        for round in 0..2 {
            for i in 0..50 {
                engine.write_point(1, round * 50 + i, i as f64);
            }
            engine.flush_all().unwrap();
        }
        let old_paths: Vec<PathBuf> =
            engine.index_read().values().map(|m| m.path.clone()).collect();
        assert_eq!(old_paths.len(), 2);

        // Simulate a crash mid-compaction, right after the manifest is
        // durably written: pending chunk exists, manifest exists, but no
        // rename/index-swap/deletion has happened.
        let points = engine.query_range_by_id(1, 0, 1000).unwrap();
        let (ts, vals): (Vec<i64>, Vec<f64>) = points.iter().copied().unzip();
        let key = PartitionKey { series_id: 1 };
        let cp = engine.encode_partition(&key, &ts, &vals, ENC_PCO, 12).unwrap();
        let (meta, written) = engine.write_individual_chunk_at(&cp, true).unwrap();
        let deletable: HashSet<PathBuf> = old_paths.iter().cloned().collect();
        engine
            .write_compaction_manifest(&[(written, meta.path.clone())], &deletable)
            .unwrap();
        drop(engine); // "crash"

        // Restart: recovery must finalize the pending chunk, delete the
        // superseded files, and leave exactly the compacted data.
        let restarted = Engine::new(dir.clone(), 100, 1, 8, usize::MAX, true);
        assert!(!Engine::manifest_path(&dir).exists());
        assert!(meta.path.exists());
        assert!(old_paths.iter().all(|p| !p.exists()));
        assert_eq!(restarted.index_read().len(), 1);
        let recovered = restarted.query_range_by_id(1, 0, 1000).unwrap();
        assert_eq!(recovered.len(), 100, "no duplicates, no loss");
    }

    #[test]
    fn compaction_crash_before_manifest_leaves_prior_state() {
        let dir = test_dir("compact_crash_early");
        let engine = Engine::new(dir.clone(), 100, 1, 8, usize::MAX, true);

        for i in 0..50 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_all().unwrap();

        // Simulate a crash after writing a pending chunk but BEFORE the
        // manifest: the orphan .pending must be swept, old data intact.
        let points = engine.query_range_by_id(1, 0, 1000).unwrap();
        let (ts, vals): (Vec<i64>, Vec<f64>) = points.iter().copied().unzip();
        let key = PartitionKey { series_id: 1 };
        let cp = engine.encode_partition(&key, &ts, &vals, ENC_PCO, 12).unwrap();
        let (_meta, written) = engine.write_individual_chunk_at(&cp, true).unwrap();
        assert!(written.exists());
        drop(engine); // "crash"

        let restarted = Engine::new(dir, 100, 1, 8, usize::MAX, true);
        assert!(!written.exists(), "orphan pending file swept");
        assert_eq!(restarted.index_read().len(), 1);
        assert_eq!(restarted.query_range_by_id(1, 0, 1000).unwrap().len(), 50);
    }

    #[test]
    fn compaction_age_gate_spares_recent_chunks() {
        let dir = test_dir("compact_age");
        let engine = Engine::new(dir, 100, 1, 8, usize::MAX, true);

        for i in 0..50 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_all().unwrap();
        for i in 0..50 {
            engine.write_point(1, 1000 + i, i as f64);
        }
        engine.flush_all().unwrap();

        // Cutoff between the two chunks: only the old one is eligible
        let (series, replaced) = engine.compact_partitions(500).unwrap();
        assert_eq!((series, replaced), (1, 1));

        let index = engine.index_read();
        assert_eq!(index.len(), 2);
        let encodings: Vec<u8> = index.values().map(|m| m.encoding).collect();
        assert!(encodings.contains(&ENC_PCO), "old chunk compacted to pco");
        assert!(encodings.contains(&ENC_RAW), "recent chunk left raw");
    }

    #[test]
    fn compaction_handles_shared_batch_files() {
        let dir = test_dir("compact_batch");
        // min_flush_size high so flush_all routes partitions into a
        // shared PCB1 batch file
        let engine = Engine::new(dir, 10_000, 1_000, 8, usize::MAX, true);

        for series in 1..=3 {
            for i in 0..20 {
                engine.write_point(series, i, i as f64);
            }
        }
        engine.flush_all().unwrap();
        assert_eq!(engine.index_read().len(), 3);

        // A second raw chunk for series 1, then compact everything.
        for i in 20..40 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_all().unwrap();

        let (series, _) = engine.compact_partitions(i64::MAX).unwrap();
        assert_eq!(series, 3);

        for id in 1..=3 {
            let expected = if id == 1 { 40 } else { 20 };
            assert_eq!(
                engine.query_range_by_id(id, 0, 1000).unwrap().len(),
                expected
            );
        }
        assert!(engine.index_read().values().all(|m| m.encoding == ENC_PCO));
    }

    #[test]
    fn sweep_file_cache_drops_only_expired_entries() {
        let engine = Engine::new(test_dir("sweep"), 100, 64, 8, usize::MAX, false);
        let fresh = PathBuf::from("/fresh.tms");
        let stale = PathBuf::from("/stale.tms");

        engine
            .file_cache
            .insert(fresh.clone(), (Instant::now(), Arc::new(vec![1u8])));
        engine.file_cache.insert(
            stale.clone(),
            (
                Instant::now() - (FILE_CACHE_TTL + Duration::from_secs(60)),
                Arc::new(vec![2u8]),
            ),
        );

        engine.sweep_file_cache();

        assert!(engine.file_cache.contains_key(&fresh));
        assert!(!engine.file_cache.contains_key(&stale));
    }

    // ── Prometheus parser ────────────────────────────────────────────

    /// Collect parsed entries into owned data for assertions.
    fn parse_collect(data: &[u8]) -> (Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>, f64, i64)>, usize) {
        let mut out = Vec::new();
        let (_count, errors) = parse_prom_body_visit(data, |name, labels, value, ts| {
            let owned_labels = labels
                .iter()
                .map(|&(k, v)| (k.to_vec(), v.to_vec()))
                .collect();
            out.push((name.to_vec(), owned_labels, value, ts));
        });
        (out, errors)
    }

    #[test]
    fn prom_parses_basic_line() {
        let (entries, errors) = parse_collect(b"cpu{host=\"a\",dc=\"b\"} 42.5 1700000000000\n");
        assert_eq!(errors, 0);
        let (name, labels, value, ts) = &entries[0];
        assert_eq!(name, b"cpu");
        assert_eq!(labels.len(), 2);
        assert_eq!(*value, 42.5);
        assert_eq!(*ts, 1_700_000_000_000);
    }

    #[test]
    fn prom_label_scratch_does_not_leak_between_lines() {
        // The visitor reuses one scratch Vec; a labelless line after a
        // labeled one must see an empty slice, not the previous labels.
        let (entries, errors) = parse_collect(b"a{k=\"v\"} 1\nb 2\n");
        assert_eq!(errors, 0);
        assert_eq!(entries[0].1.len(), 1);
        assert!(entries[1].1.is_empty());
    }

    #[test]
    fn prom_eof_mid_escape_is_error() {
        // Body ends inside an escape sequence, no closing brace.
        let (entries, errors) = parse_collect(b"m{k=\"v\\");
        assert!(entries.is_empty());
        assert_eq!(errors, 1);
    }

    #[test]
    fn prom_escaped_quote_kept_raw_and_truncated_at_brace() {
        // Input line: m{k="a\"} 1 — the '}' is found before label parsing,
        // so the escaped quote runs to the end of the label block.
        let (entries, errors) = parse_collect(b"m{k=\"a\\\"} 1\n");
        assert_eq!(errors, 0);
        assert_eq!(entries[0].1[0], (b"k".to_vec(), b"a\\\"".to_vec()));
        assert_eq!(entries[0].2, 1.0);
    }

    #[test]
    fn prom_rejects_nonfinite_and_hex_values() {
        for body in [
            &b"m NaN\n"[..],
            b"m nan\n",
            b"m +Inf\n",
            b"m -Inf\n",
            b"m inf\n",
            b"m infinity\n",
            b"m 1e400\n",
            // Contract decision: hex floats are not valid Prometheus even
            // though C's strtod accepts them.
            b"m 0x10\n",
            b"m 0x1p3\n",
        ] {
            let (entries, errors) = parse_collect(body);
            assert!(entries.is_empty(), "accepted {:?}", body);
            assert_eq!(errors, 1, "no error for {:?}", body);
        }
    }

    #[test]
    fn prom_timestamp_overflow_uses_zero_sentinel() {
        // Contract decision: an out-of-range timestamp is garbage and gets
        // the 0 sentinel ("no timestamp"), unlike strtoll's i64::MAX
        // saturation in the old C++ parser.
        let (entries, errors) = parse_collect(b"m 1.0 99999999999999999999\n");
        assert_eq!(errors, 0);
        assert_eq!(entries[0].3, 0);
    }

    #[test]
    fn prom_long_numeric_fields_parse() {
        // Contract decision: no 64-byte field limit (the old C++ parser
        // rejected numerics >= 64 chars due to a fixed stack buffer).
        let body = format!("m {}5\n", "0".repeat(80));
        let (entries, errors) = parse_collect(body.as_bytes());
        assert_eq!(errors, 0);
        assert_eq!(entries[0].2, 5.0);
    }

    #[test]
    fn prom_error_and_skip_accounting() {
        let body = b"# comment\n\n   \nok 1\nbad line here\n# more\nok2 2 123\n";
        let mut count = 0;
        let (visited, errors) = parse_prom_body_visit(body, |_, _, _, _| count += 1);
        assert_eq!(visited, 2);
        assert_eq!(count, 2);
        assert_eq!(errors, 1);
    }

    #[test]
    fn prom_labels_edge_cases() {
        // Trailing comma, empty value, spaces around entries.
        let (entries, errors) = parse_collect(b"m{ a=\"\", b=\"x\" ,} 1\n");
        assert_eq!(errors, 0);
        assert_eq!(
            entries[0].1,
            vec![
                (b"a".to_vec(), b"".to_vec()),
                (b"b".to_vec(), b"x".to_vec())
            ]
        );
    }
    use std::sync::Arc;
    use std::thread;

    fn test_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("tms_engine_{name}_{unique}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn flush_sorts_out_of_order_points() {
        let dir = test_dir("sorts");
        let engine = Engine::new(test_dir("fused"), 100, 64, 8, usize::MAX, false);
        let key = PartitionKey { series_id: 1 };

        engine.write_point(1, 30, 3.0);
        engine.write_point(1, 10, 1.0);
        engine.write_point(1, 20, 2.0);
        engine.flush_all().unwrap();

        let meta = engine
            .index_read()
            .range((key, 10, u64::MIN)..)
            .next()
            .map(|(_, m)| m.clone())
            .unwrap();
        assert_eq!(meta.min_ts, 10);
        assert_eq!(meta.max_ts, 30);
        assert_eq!(
            engine.query_range_by_id(1, 0, 100).unwrap(),
            vec![(10, 1.0), (20, 2.0), (30, 3.0)]
        );
    }

    #[test]
    fn duplicate_min_ts_chunks_do_not_shadow() {
        // Two flush cycles producing chunks with the same (series, min_ts)
        // — e.g. backfill re-ingesting an overlapping export. Both chunks
        // must stay queryable, in memory and across restart.
        let dir = test_dir("dup_min_ts");
        let engine = Engine::new(dir.clone(), 100, 1, 8, usize::MAX, false);

        engine.write_point(1, 100, 1.0);
        engine.flush_all().unwrap(); // chunk A: min_ts=100
        engine.write_point(1, 100, 2.0);
        engine.write_point(1, 200, 3.0);
        engine.flush_all().unwrap(); // chunk B: min_ts=100

        assert_eq!(engine.index_read().len(), 2);
        let points = engine.query_range_by_id(1, 0, 1000).unwrap();
        assert_eq!(
            points.iter().map(|&(ts, _)| ts).collect::<Vec<_>>(),
            vec![100, 100, 200]
        );

        let restarted = Engine::new(dir, 100, 1, 8, usize::MAX, false);
        assert_eq!(restarted.index_read().len(), 2);
        assert_eq!(restarted.query_range_by_id(1, 0, 1000).unwrap().len(), 3);
    }

    #[test]
    fn restart_does_not_overwrite_existing_batch_files() {
        let dir = test_dir("restart");

        let engine = Engine::new(dir.clone(), 100, 64, 8, usize::MAX, false);
        for i in 0..10 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_cold(0).unwrap();
        let first_files = fs::read_dir(dir.join("batches")).unwrap().count();
        assert_eq!(first_files, 1);

        let engine = Engine::new(dir.clone(), 100, 64, 8, usize::MAX, false);
        for i in 10..20 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_cold(0).unwrap();

        let batch_dir = dir.join("batches");
        let file_count = fs::read_dir(&batch_dir).unwrap().count();
        assert_eq!(
            file_count,
            2,
            "expected two distinct batch files in {}",
            batch_dir.display()
        );
    }

    #[test]
    fn rebuild_index_finds_individual_chunks_without_reading_payloads() {
        let dir = test_dir("rebuild_individual");
        let engine = Engine::new(dir.clone(), 1, 1, 8, usize::MAX, false);

        for i in 0..5 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_all().unwrap();
        assert_eq!(engine.query_range_by_id(1, 0, 10).unwrap().len(), 5);

        let restarted = Engine::new(dir, 1, 1, 8, usize::MAX, false);
        assert_eq!(restarted.index_read().len(), 1);
        assert_eq!(restarted.query_range_by_id(1, 0, 10).unwrap().len(), 5);
    }

    #[test]
    fn rebuild_index_finds_batched_chunks_without_reading_payloads() {
        let dir = test_dir("rebuild_batched");
        let engine = Engine::new(dir.clone(), 100, 64, 8, usize::MAX, false);

        for series_id in 1..=3 {
            for ts in 0..3 {
                engine.write_point(series_id, ts, (series_id * 10 + ts) as f64);
            }
        }
        engine.flush_all().unwrap();
        assert_eq!(engine.index_read().len(), 3);

        let restarted = Engine::new(dir, 100, 64, 8, usize::MAX, false);
        assert_eq!(restarted.index_read().len(), 3);
        assert_eq!(restarted.query_range_by_id(1, 0, 10).unwrap().len(), 3);
        assert_eq!(restarted.query_range_by_id(2, 0, 10).unwrap().len(), 3);
        assert_eq!(restarted.query_range_by_id(3, 0, 10).unwrap().len(), 3);
    }

    #[test]
    fn truncated_pco1_header_returns_error() {
        let dir = test_dir("truncated_pco1");
        let path = dir.join("bad.pco1");
        fs::write(&path, b"PCO1\x01").unwrap();

        assert!(Engine::read_pco1_header(&path).is_err());
    }

    #[test]
    fn truncated_pcb1_header_returns_error() {
        let dir = test_dir("truncated_pcb1");
        let path = dir.join("bad.pcb1");
        let mut data = Vec::new();
        data.extend_from_slice(b"PCB1");
        data.push(1);
        data.extend_from_slice(&1u32.to_be_bytes());
        fs::write(&path, data).unwrap();

        assert!(Engine::read_pcb1_headers(&path).is_err());
    }

    #[test]
    fn concurrent_flushes_do_not_drop_writes() {
        let dir = test_dir("concurrent");
        let engine = Arc::new(Engine::new(dir, 10_000, 64, 8, usize::MAX, false));

        let writer = {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..2_000 {
                    engine.write_point(1, i, i as f64);
                }
            })
        };

        let flusher = {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for _ in 0..50 {
                    engine.flush_all().unwrap();
                }
            })
        };

        writer.join().unwrap();
        flusher.join().unwrap();
        engine.flush_all().unwrap();

        let points = engine.query_range_by_id(1, 0, 5_000).unwrap();
        assert_eq!(points.len(), 2_000);
        assert_eq!(points.first(), Some(&(0, 0.0)));
        assert_eq!(points.last(), Some(&(1_999, 1_999.0)));
    }

    #[test]
    fn hot_partition_is_only_queued_once_before_flush() {
        let dir = test_dir("dedupe_queue");
        let engine = Engine::new(dir, 3, 1, 8, usize::MAX, false);

        for i in 0..10 {
            engine.write_point(1, i, i as f64);
        }

        assert_eq!(
            engine.flush_queue_lock().len(),
            1,
            "hot partition should have one pending flush entry"
        );

        assert_eq!(engine.flush_pending().unwrap(), 1);
        assert!(engine.flush_queue_lock().is_empty());
        assert_eq!(engine.query_range_by_id(1, 0, 20).unwrap().len(), 10);

        for i in 10..13 {
            engine.write_point(1, i, i as f64);
        }

        assert_eq!(
            engine.flush_queue_lock().len(),
            1,
            "partition should be queueable again after drain"
        );
        assert_eq!(engine.flush_pending().unwrap(), 1);
        assert_eq!(engine.query_range_by_id(1, 0, 20).unwrap().len(), 13);
    }

    #[test]
    fn pending_flush_that_does_not_drain_can_requeue_later() {
        let dir = test_dir("dedupe_queue_min_size");
        let engine = Engine::new(dir, 3, 5, 8, usize::MAX, false);

        for i in 0..3 {
            engine.write_point(1, i, i as f64);
        }

        assert_eq!(engine.flush_queue_lock().len(), 1);
        assert_eq!(engine.flush_pending().unwrap(), 0);
        assert!(engine.flush_queue_lock().is_empty());

        engine.write_point(1, 3, 3.0);
        assert_eq!(
            engine.flush_queue_lock().len(),
            1,
            "partition should requeue after a skipped pending flush"
        );

        engine.write_point(1, 4, 4.0);
        assert_eq!(
            engine.flush_queue_lock().len(),
            1,
            "additional writes before drain should not duplicate the queue entry"
        );
        assert_eq!(engine.flush_pending().unwrap(), 1);
        assert_eq!(engine.query_range_by_id(1, 0, 10).unwrap().len(), 5);
    }

    #[test]
    fn find_series_keeps_metric_scope_when_starting_from_label_index() {
        let mut reg = SeriesRegistry::new();

        let mut labels_a = BTreeMap::new();
        labels_a.insert("host".to_string(), "shared".to_string());
        labels_a.insert("region".to_string(), "west".to_string());

        let mut labels_b = BTreeMap::new();
        labels_b.insert("host".to_string(), "shared".to_string());
        labels_b.insert("region".to_string(), "east".to_string());

        let metric_a_id = reg.get_or_create("cpu_usage", &labels_a);
        let metric_b_id = reg.get_or_create("mem_usage", &labels_b);

        let mut filter = BTreeMap::new();
        filter.insert("host".to_string(), "shared".to_string());

        assert_eq!(reg.find_series("cpu_usage", &filter), vec![metric_a_id]);
        assert_eq!(reg.find_series("mem_usage", &filter), vec![metric_b_id]);
    }

    #[test]
    fn find_series_handles_multiple_label_filters_and_misses() {
        let mut reg = SeriesRegistry::new();

        let mut west_api = BTreeMap::new();
        west_api.insert("region".to_string(), "west".to_string());
        west_api.insert("service".to_string(), "api".to_string());

        let mut west_db = BTreeMap::new();
        west_db.insert("region".to_string(), "west".to_string());
        west_db.insert("service".to_string(), "db".to_string());

        let mut east_api = BTreeMap::new();
        east_api.insert("region".to_string(), "east".to_string());
        east_api.insert("service".to_string(), "api".to_string());

        let west_api_id = reg.get_or_create("latency", &west_api);
        reg.get_or_create("latency", &west_db);
        reg.get_or_create("latency", &east_api);

        let mut filter = BTreeMap::new();
        filter.insert("region".to_string(), "west".to_string());
        filter.insert("service".to_string(), "api".to_string());

        assert_eq!(reg.find_series("latency", &filter), vec![west_api_id]);

        filter.insert("service".to_string(), "queue".to_string());
        assert!(reg.find_series("latency", &filter).is_empty());
    }

    #[test]
    fn raw_batch_rejects_invalid_payload_length() {
        let dir = test_dir("raw_batch");
        let engine = Engine::new(dir, 100, 64, 8, usize::MAX, false);

        let err = engine.write_batch_raw(&[1, 2, 3]).unwrap_err();
        assert!(err.contains("not a multiple"));
    }

    #[test]
    fn rewrite_after_retention_recreates_deleted_series_dir() {
        let dir = test_dir("retention_rewrite");
        let engine = Engine::new(dir.clone(), 1, 1, 8, usize::MAX, false);

        engine.write_point(1, 1, 1.0);
        engine.flush_all().unwrap();

        let chunk_dir = dir.join("chunks").join("1");
        assert!(chunk_dir.exists());

        let (entries_removed, files_deleted, errors) = engine.delete_before(2);
        assert_eq!(entries_removed, 1);
        assert_eq!(files_deleted, 1);
        assert!(errors.is_empty());
        assert!(!chunk_dir.exists());

        engine.write_point(1, 3, 3.0);
        engine.flush_all().unwrap();

        assert!(chunk_dir.exists());
        assert_eq!(engine.query_range_by_id(1, 0, 10).unwrap(), vec![(3, 3.0)]);
    }
}
