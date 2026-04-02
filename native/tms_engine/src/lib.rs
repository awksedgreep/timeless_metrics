#![allow(dead_code)]

use dashmap::DashMap;
use rayon::prelude::*;
use rustler::{Atom, Binary, ResourceArc};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

mod atoms {
    rustler::atoms! {
        ok, error, avg, sum, min, max, count,
    }
}

type EngineResult<T> = Result<T, String>;

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
}

impl PartitionBuffer {
    fn new() -> Self {
        PartitionBuffer {
            timestamps: Vec::new(),
            values: Vec::new(),
            last_write: Instant::now(),
        }
    }
    fn memory_bytes(&self) -> usize {
        (self.timestamps.capacity() + self.values.capacity()) * 8
    }
}

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
    data_len: u32,
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
        // Start with all series for this metric
        let candidates = match self.metric_index.get(metric_name) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        if label_filter.is_empty() {
            return candidates.into_iter().collect();
        }

        // Intersect with each label filter
        let mut result = candidates;
        for (k, v) in label_filter {
            if let Some(matching) = self.label_index.get(&(k.clone(), v.clone())) {
                result = result.intersection(matching).copied().collect();
            } else {
                return Vec::new();
            }
        }

        result.into_iter().collect()
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

    fn load(path: &PathBuf) -> Self {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return Self::new(),
        };
        if data.len() < 4 {
            return Self::new();
        }

        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut reg = SeriesRegistry::new();
        let mut max_id: i64 = 0;
        let mut pos = 4;

        for _ in 0..count {
            if pos + 10 > data.len() {
                break;
            }
            let id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let ml = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + ml > data.len() {
                break;
            }
            let metric_name = String::from_utf8_lossy(&data[pos..pos + ml]).to_string();
            pos += ml;

            if pos + 2 > data.len() {
                break;
            }
            let lc = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut labels = BTreeMap::new();
            for _ in 0..lc {
                if pos + 2 > data.len() {
                    break;
                }
                let kl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + kl > data.len() {
                    break;
                }
                let k = String::from_utf8_lossy(&data[pos..pos + kl]).to_string();
                pos += kl;
                if pos + 2 > data.len() {
                    break;
                }
                let vl = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + vl > data.len() {
                    break;
                }
                let v = String::from_utf8_lossy(&data[pos..pos + vl]).to_string();
                pos += vl;
                labels.insert(k, v);
            }

            // Rebuild all indexes
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
        reg
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Engine
// ═══════════════════════════════════════════════════════════════════════

/// Fast hash of (metric, labels) for the resolution cache.
/// Uses std DefaultHasher which is SipHash — fast and collision-resistant.
fn fast_series_hash(metric: &str, labels: &HashMap<String, String>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric.hash(&mut hasher);
    // Sort label keys for deterministic hashing without BTreeMap conversion
    let mut pairs: Vec<(&str, &str)> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.sort_unstable_by_key(|&(k, _)| k);
    for (k, v) in pairs {
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
    partitions: DashMap<PartitionKey, PartitionBuffer>,
    index: RwLock<BTreeMap<(PartitionKey, i64), ChunkMeta>>,
    series: RwLock<SeriesRegistry>,
    created_dirs: Mutex<HashSet<PathBuf>>,
    flush_queue: Mutex<Vec<PartitionKey>>,
    buffer_memory: AtomicUsize,
    batch_counter: AtomicUsize,
    instance_id: u128,
    cold_flush_running: AtomicBool,
    /// Fast resolution cache: hash(metric, labels) → series_id.
    /// Persists across batches — steady-state scraping is pure cache hits.
    resolve_cache: DashMap<u64, i64>,
}

struct EngineResource {
    engine: Engine,
}

unsafe impl Send for EngineResource {}
unsafe impl Sync for EngineResource {}
impl std::panic::RefUnwindSafe for EngineResource {}
impl std::panic::UnwindSafe for EngineResource {}

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
    fn series_path(data_dir: &PathBuf) -> PathBuf {
        data_dir.join("series.bin")
    }

    fn new(
        data_dir: PathBuf,
        flush_threshold: usize,
        min_flush_size: usize,
        compression_level: usize,
        memory_budget: usize,
    ) -> Self {
        let registry = SeriesRegistry::load(&Self::series_path(&data_dir));
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
            partitions: DashMap::new(),
            index: RwLock::new(BTreeMap::new()),
            series: RwLock::new(registry),
            created_dirs: Mutex::new(HashSet::new()),
            flush_queue: Mutex::new(Vec::new()),
            buffer_memory: AtomicUsize::new(0),
            batch_counter: AtomicUsize::new(0),
            instance_id,
            cold_flush_running: AtomicBool::new(false),
            resolve_cache: DashMap::new(),
        };
        engine.rebuild_index();
        engine
    }

    // ── Series resolution ────────────────────────────────────────────

    /// Resolve (metric, labels) → series_id. Fast read path, slow write path.
    fn resolve_series(&self, metric_name: &str, labels: &Labels) -> EngineResult<i64> {
        // Fast: read lock
        {
            let reg = self.series.read().unwrap();
            if let Some(&id) = reg
                .series_map
                .get(&(metric_name.to_string(), labels.clone()))
            {
                return Ok(id);
            }
        }
        // Slow: write lock + persist
        let mut reg = self.series.write().unwrap();
        Ok(reg.get_or_create(metric_name, labels))
    }

    fn resolve_series_batch(&self, entries: &[(String, Labels)]) -> EngineResult<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(entries.len());
        let mut misses: Vec<(usize, &str, &Labels)> = Vec::new();

        {
            let reg = self.series.read().unwrap();
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

        let mut reg = self.series.write().unwrap();
        for (idx, metric_name, labels) in misses {
            out[idx] = reg.get_or_create(metric_name, labels);
        }

        Ok(out)
    }

    fn save_series(&self) -> EngineResult<()> {
        let mut reg = self.series.write().unwrap();
        reg.save(&Self::series_path(&self.data_dir))
            .map_err(|err| format!("failed to persist series registry: {err}"))
    }

    // ── Write path ───────────────────────────────────────────────────

    #[inline]
    fn write_point(&self, series_id: i64, ts: i64, val: f64) {
        let key = PartitionKey { series_id };
        let needs_flush;
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
            needs_flush = buf.timestamps.len() >= self.flush_threshold;
        }

        if mem_delta > 0 {
            self.buffer_memory
                .fetch_add(mem_delta as usize, Ordering::Relaxed);
        } else if mem_delta < 0 {
            self.buffer_memory
                .fetch_sub((-mem_delta) as usize, Ordering::Relaxed);
        }

        if needs_flush {
            self.flush_queue.lock().unwrap().push(key);
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

    // ── Flush ────────────────────────────────────────────────────────

    fn flush_pending(&self) -> EngineResult<usize> {
        let keys: Vec<PartitionKey> = {
            let mut queue = self.flush_queue.lock().unwrap();
            std::mem::take(&mut *queue)
        };
        let mut seen = HashSet::new();
        let unique: Vec<PartitionKey> = keys.into_iter().filter(|k| seen.insert(*k)).collect();

        let mut count = 0;
        for key in unique {
            let should = self
                .partitions
                .get(&key)
                .map(|b| b.timestamps.len() >= self.min_flush_size)
                .unwrap_or(false);
            if should {
                self.flush_partition_individual(&key)?;
                count += 1;
            }
        }
        self.save_series()?;
        Ok(count)
    }

    fn flush_partition_individual(&self, key: &PartitionKey) -> EngineResult<()> {
        if let Some((timestamps, values)) =
            self.drain_partition_if(key, |buf| !buf.timestamps.is_empty())
        {
            let cp = self.compress_partition(key, &timestamps, &values)?;
            let meta = self.write_individual_chunk(&cp)?;
            self.index
                .write()
                .unwrap()
                .insert((*key, meta.min_ts), meta);
        }
        Ok(())
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
            let mut index = self.index.write().unwrap();
            for (key, meta) in metas {
                index.insert((key, meta.min_ts), meta);
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
            for batch in compressed.chunks(1000) {
                let metas = self.write_batched_chunk(batch)?;
                let mut index = self.index.write().unwrap();
                for (key, meta) in metas {
                    index.insert((key, meta.min_ts), meta);
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
        for batch in small_compressed.chunks(1000) {
            all_metas.extend(self.write_batched_chunk(batch)?);
        }
        if !all_metas.is_empty() {
            let mut index = self.index.write().unwrap();
            for (key, meta) in all_metas {
                index.insert((key, meta.min_ts), meta);
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

        let config = pco::ChunkConfig::default().with_compression_level(self.compression_level);
        let ts_compressed = match pco::standalone::simple_compress(ts_slice, &config) {
            Ok(data) => data,
            Err(err) => {
                return Err(format!(
                    "failed to compress timestamps for series {}: {err}",
                    key.series_id
                ));
            }
        };
        let val_compressed = match pco::standalone::simple_compress(val_slice, &config) {
            Ok(data) => data,
            Err(err) => {
                return Err(format!(
                    "failed to compress values for series {}: {err}",
                    key.series_id
                ));
            }
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
        })
    }

    // ── Individual chunk writer (PCO1) ───────────────────────────────

    fn write_individual_chunk(&self, cp: &CompressedPartition) -> EngineResult<ChunkMeta> {
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
        out.push(1u8);
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
        fs::rename(&tmp_path, &path)
            .map_err(|err| format!("failed to rename chunk {}: {err}", path.display()))?;

        Ok(ChunkMeta {
            min_ts: cp.min_ts,
            max_ts: cp.max_ts,
            point_count: cp.point_count,
            min_val: cp.min_val,
            max_val: cp.max_val,
            sum_val: cp.sum_val,
            path,
            data_offset: 0,
            data_len: 0,
        })
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
        out.push(1u8);
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
                    },
                )
            })
            .collect())
    }

    fn ensure_dir(&self, path: &PathBuf) -> io::Result<()> {
        let dir = path.parent().unwrap().to_path_buf();
        let mut dirs = self.created_dirs.lock().unwrap();
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
        entry.last_write = Instant::now();
        drop(entry);

        if freed > 0 {
            self.buffer_memory.fetch_sub(freed, Ordering::Relaxed);
        }

        Some((timestamps, values))
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
            let reg = self.series.read().unwrap();
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
        let mut file_cache: HashMap<PathBuf, Vec<u8>> = HashMap::new();
        self.query_range_by_id_cached(series_id, t_start, t_end, &mut file_cache)
    }

    fn query_range_by_id_cached(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        file_cache: &mut HashMap<PathBuf, Vec<u8>>,
    ) -> EngineResult<Vec<(i64, f64)>> {
        let pk = PartitionKey { series_id };
        let mut results = Vec::new();

        {
            let index = self.index.read().unwrap();
            for ((k, _), meta) in index.range((pk, i64::MIN)..) {
                if k != &pk {
                    break;
                }
                if meta.min_ts > t_end {
                    break;
                }
                if meta.max_ts < t_start {
                    continue;
                }
                results.extend(Self::read_chunk_data_cached(
                    meta, t_start, t_end, file_cache,
                )?);
            }
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
            let reg = self.series.read().unwrap();
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
        let mut file_cache: HashMap<PathBuf, Vec<u8>> = HashMap::new();
        self.query_aggregate_by_id_cached(series_id, t_start, t_end, agg, &mut file_cache)
    }

    fn query_aggregate_by_id_cached(
        &self,
        series_id: i64,
        t_start: i64,
        t_end: i64,
        agg: AggFn,
        file_cache: &mut HashMap<PathBuf, Vec<u8>>,
    ) -> EngineResult<Option<f64>> {
        let pk = PartitionKey { series_id };

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: Option<f64> = None;
        let mut global_max: Option<f64> = None;

        {
            let index = self.index.read().unwrap();
            for ((k, _), meta) in index.range((pk, i64::MIN)..) {
                if k != &pk {
                    break;
                }
                if meta.min_ts > t_end {
                    break;
                }
                if meta.max_ts < t_start {
                    continue;
                }

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
                    let points = Self::read_chunk_data_cached(meta, t_start, t_end, file_cache)?;
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

    fn read_chunk_data(
        meta: &ChunkMeta,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, f64)>, String> {
        let mut file_cache: HashMap<PathBuf, Vec<u8>> = HashMap::new();
        Self::read_chunk_data_cached(meta, t_start, t_end, &mut file_cache)
    }

    fn read_chunk_data_cached(
        meta: &ChunkMeta,
        t_start: i64,
        t_end: i64,
        file_cache: &mut HashMap<PathBuf, Vec<u8>>,
    ) -> Result<Vec<(i64, f64)>, String> {
        let data = if let Some(data) = file_cache.get(&meta.path) {
            data
        } else {
            let data = fs::read(&meta.path).map_err(|e| e.to_string())?;
            file_cache.entry(meta.path.clone()).or_insert(data)
        };
        let (ts_data, val_data) = if meta.data_offset > 0 {
            Self::parse_partition_data(&data, meta.data_offset as usize)?
        } else {
            Self::parse_pco1_data(&data)?
        };

        let timestamps: Vec<i64> =
            pco::standalone::simple_decompress(ts_data).map_err(|e| e.to_string())?;
        let values: Vec<f64> =
            pco::standalone::simple_decompress(val_data).map_err(|e| e.to_string())?;
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
        if pos + 2 + pk_len + 24 > data.len() {
            return Err("truncated PCO1 partition key".into());
        }
        pos += 2 + pk_len;
        pos += 24;
        Self::parse_partition_data(data, pos)
    }

    // ── Retention ────────────────────────────────────────────────────

    fn delete_before(&self, before_ts: i64) -> (usize, usize) {
        let mut index = self.index.write().unwrap();

        let to_remove: Vec<(PartitionKey, i64)> = index
            .iter()
            .filter(|(_, meta)| meta.max_ts < before_ts)
            .map(|(k, _)| k.clone())
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
        for path in &files_to_delete {
            let _ = fs::remove_file(path);
            if let Some(dir) = path.parent() {
                let _ = fs::remove_dir(dir);
            }
        }

        (entries_removed, files_deleted)
    }

    // ── Index rebuild ────────────────────────────────────────────────

    fn rebuild_index(&self) {
        let mut index = self.index.write().unwrap();
        for dir_name in &["chunks", "batches"] {
            let dir = self.data_dir.join(dir_name);
            if dir.exists() {
                Self::scan_dir_recursive(&dir, &mut index);
            }
        }
    }

    fn scan_dir_recursive(dir: &PathBuf, index: &mut BTreeMap<(PartitionKey, i64), ChunkMeta>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::scan_dir_recursive(&path, index);
            } else {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("pco1") => {
                        if let Ok(entries) = Self::read_pco1_header(&path) {
                            for (pk, meta) in entries {
                                index.insert((pk, meta.min_ts), meta);
                            }
                        }
                    }
                    Some("pcb1") => {
                        if let Ok(entries) = Self::read_pcb1_headers(&path) {
                            for (pk, meta) in entries {
                                index.insert((pk, meta.min_ts), meta);
                            }
                        }
                    }
                    Some("tmp") => {
                        let _ = fs::remove_file(&path);
                    }
                    _ => {}
                }
            }
        }
    }

    fn read_pco1_header(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        if data.len() < 4 || &data[0..4] != b"PCO1" {
            return Err("invalid".into());
        }

        let mut pos = 5;
        if pos + 4 + 16 + 2 > data.len() {
            return Err("truncated PCO1 header".into());
        }
        let point_count = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let min_ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let pk_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + pk_len + 24 > data.len() {
            return Err("truncated PCO1 metadata".into());
        }
        let pk_str = String::from_utf8_lossy(&data[pos..pos + pk_len]).to_string();
        pos += pk_len;
        let min_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let sum_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());

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
            },
        )])
    }

    fn read_pcb1_headers(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        if data.len() < 9 || &data[0..4] != b"PCB1" {
            return Err("invalid".into());
        }

        let n = u32::from_be_bytes(data[5..9].try_into().unwrap()) as usize;
        let mut results = Vec::with_capacity(n);
        let mut pos = 9;
        let table_len = n
            .checked_mul(64)
            .ok_or_else(|| "PCB1 table overflow".to_string())?;
        if pos + table_len > data.len() {
            return Err("truncated PCB1 header".into());
        }

        for _ in 0..n {
            let series_id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let point_count = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let min_ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let min_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let sum_val = f64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

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
                },
            ));
        }

        Ok(results)
    }

    fn info(&self) -> EngineInfo {
        let index = self.index.read().unwrap();
        let series_reg = self.series.read().unwrap();
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
// NIF interface
// ═══════════════════════════════════════════════════════════════════════

#[rustler::nif]
fn engine_new(
    data_dir: String,
    flush_threshold: usize,
    min_flush_size: usize,
    compression_level: usize,
    memory_budget_mb: usize,
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
        ),
    })
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
) -> (Atom, usize, usize) {
    let (e, f) = resource.deref().engine.delete_before(before_ts);
    (atoms::ok(), e, f)
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
        match_agg(agg),
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
    let reg = resource.deref().engine.series.read().unwrap();
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
fn engine_info(resource: ResourceArc<EngineResource>) -> Result<HashMap<String, f64>, String> {
    let info = resource.deref().engine.info();
    let mut m = HashMap::new();
    m.insert("chunk_count".into(), info.chunk_count as f64);
    m.insert("partition_count".into(), info.partition_count as f64);
    m.insert("series_count".into(), info.series_count as f64);
    m.insert("disk_points".into(), info.disk_points as f64);
    m.insert("buffered_points".into(), info.buffered_points as f64);
    m.insert("total_points".into(), info.total_points as f64);
    m.insert("total_bytes".into(), info.total_bytes as f64);
    m.insert("bytes_per_point".into(), info.bytes_per_point);
    m.insert("buffer_memory_bytes".into(), info.buffer_memory as f64);
    m.insert(
        "buffer_memory_mb".into(),
        info.buffer_memory as f64 / 1024.0 / 1024.0,
    );
    m.insert("file_count".into(), info.file_count as f64);
    if let Some(oldest_ts) = info.oldest_ts {
        m.insert("oldest_timestamp".into(), oldest_ts as f64);
    }
    if let Some(newest_ts) = info.newest_ts {
        m.insert("newest_timestamp".into(), newest_ts as f64);
    }
    Ok(m)
}

fn match_agg(atom: Atom) -> AggFn {
    if atom == atoms::avg() {
        AggFn::Avg
    } else if atom == atoms::sum() {
        AggFn::Sum
    } else if atom == atoms::min() {
        AggFn::Min
    } else if atom == atoms::max() {
        AggFn::Max
    } else if atom == atoms::count() {
        AggFn::Count
    } else {
        AggFn::Avg
    }
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    let _ = rustler::resource!(EngineResource, env);
    true
}

rustler::init!("Elixir.TimelessMetrics.RustEngine.Nif", load = load);

fn partition_vec_memory(timestamps: &Vec<i64>, values: &Vec<f64>) -> usize {
    (timestamps.capacity() + values.capacity()) * 8
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let engine = Engine::new(dir, 100, 64, 8, usize::MAX);
        let key = PartitionKey { series_id: 1 };

        engine.write_point(1, 30, 3.0);
        engine.write_point(1, 10, 1.0);
        engine.write_point(1, 20, 2.0);
        engine.flush_all().unwrap();

        let meta = engine
            .index
            .read()
            .unwrap()
            .get(&(key, 10))
            .cloned()
            .unwrap();
        assert_eq!(meta.min_ts, 10);
        assert_eq!(meta.max_ts, 30);
        assert_eq!(
            engine.query_range_by_id(1, 0, 100).unwrap(),
            vec![(10, 1.0), (20, 2.0), (30, 3.0)]
        );
    }

    #[test]
    fn restart_does_not_overwrite_existing_batch_files() {
        let dir = test_dir("restart");

        let engine = Engine::new(dir.clone(), 100, 64, 8, usize::MAX);
        for i in 0..10 {
            engine.write_point(1, i, i as f64);
        }
        engine.flush_cold(0).unwrap();
        let first_files = fs::read_dir(dir.join("batches")).unwrap().count();
        assert_eq!(first_files, 1);

        let engine = Engine::new(dir.clone(), 100, 64, 8, usize::MAX);
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
    fn concurrent_flushes_do_not_drop_writes() {
        let dir = test_dir("concurrent");
        let engine = Arc::new(Engine::new(dir, 10_000, 64, 8, usize::MAX));

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
    fn raw_batch_rejects_invalid_payload_length() {
        let dir = test_dir("raw_batch");
        let engine = Engine::new(dir, 100, 64, 8, usize::MAX);

        let err = engine.write_batch_raw(&[1, 2, 3]).unwrap_err();
        assert!(err.contains("not a multiple"));
    }
}
