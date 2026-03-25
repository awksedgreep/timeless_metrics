#![allow(dead_code)]

use dashmap::DashMap;
use rustler::{Atom, Binary, ResourceArc};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

mod atoms {
    rustler::atoms! {
        ok, error, avg, sum, min, max, count,
    }
}

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
        PartitionBuffer { timestamps: Vec::new(), values: Vec::new(), last_write: Instant::now() }
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
        self.series_info.insert(id, SeriesInfo {
            metric_name: metric_name.to_string(),
            labels: labels.clone(),
        });

        // Label index — index every label pair + __name__
        self.metric_index.entry(metric_name.to_string()).or_default().insert(id);
        for (k, v) in labels {
            self.label_index.entry((k.clone(), v.clone())).or_default().insert(id);
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
    fn save(&mut self, path: &PathBuf) {
        if !self.dirty { return; }
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

        if let Some(dir) = path.parent() { let _ = fs::create_dir_all(dir); }
        let _ = fs::write(path, &out);
        self.dirty = false;
    }

    fn load(path: &PathBuf) -> Self {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return Self::new(),
        };
        if data.len() < 4 { return Self::new(); }

        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut reg = SeriesRegistry::new();
        let mut max_id: i64 = 0;
        let mut pos = 4;

        for _ in 0..count {
            if pos + 10 > data.len() { break; }
            let id = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let ml = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
            if pos + ml > data.len() { break; }
            let metric_name = String::from_utf8_lossy(&data[pos..pos+ml]).to_string(); pos += ml;

            if pos + 2 > data.len() { break; }
            let lc = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
            let mut labels = BTreeMap::new();
            for _ in 0..lc {
                if pos + 2 > data.len() { break; }
                let kl = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
                if pos + kl > data.len() { break; }
                let k = String::from_utf8_lossy(&data[pos..pos+kl]).to_string(); pos += kl;
                if pos + 2 > data.len() { break; }
                let vl = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
                if pos + vl > data.len() { break; }
                let v = String::from_utf8_lossy(&data[pos..pos+vl]).to_string(); pos += vl;
                labels.insert(k, v);
            }

            // Rebuild all indexes
            let key = (metric_name.clone(), labels.clone());
            reg.series_map.insert(key, id);
            reg.series_info.insert(id, SeriesInfo { metric_name: metric_name.clone(), labels: labels.clone() });
            reg.metric_index.entry(metric_name).or_default().insert(id);
            for (k, v) in &labels {
                reg.label_index.entry((k.clone(), v.clone())).or_default().insert(id);
            }
            if id > max_id { max_id = id; }
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
    let mut pairs: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
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
    cold_flush_running: AtomicBool,
    /// Fast resolution cache: hash(metric, labels) → series_id.
    /// Persists across batches — steady-state scraping is pure cache hits.
    resolve_cache: DashMap<u64, i64>,
}

struct EngineResource { engine: Engine }

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

impl Engine {
    fn series_path(data_dir: &PathBuf) -> PathBuf {
        data_dir.join("series.bin")
    }

    fn new(data_dir: PathBuf, flush_threshold: usize, min_flush_size: usize, compression_level: usize, memory_budget: usize) -> Self {
        let registry = SeriesRegistry::load(&Self::series_path(&data_dir));

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
            cold_flush_running: AtomicBool::new(false),
            resolve_cache: DashMap::new(),
        };
        engine.rebuild_index();
        engine
    }

    // ── Series resolution ────────────────────────────────────────────

    /// Resolve (metric, labels) → series_id. Fast read path, slow write path.
    fn resolve_series(&self, metric_name: &str, labels: &Labels) -> i64 {
        // Fast: read lock
        {
            let reg = self.series.read().unwrap();
            if let Some(&id) = reg.series_map.get(&(metric_name.to_string(), labels.clone())) {
                return id;
            }
        }
        // Slow: write lock + persist
        let mut reg = self.series.write().unwrap();
        let id = reg.get_or_create(metric_name, labels);
        reg.save(&Self::series_path(&self.data_dir));
        id
    }

    fn save_series(&self) {
        let mut reg = self.series.write().unwrap();
        reg.save(&Self::series_path(&self.data_dir));
    }

    // ── Write path ───────────────────────────────────────────────────

    #[inline]
    fn write_point(&self, series_id: i64, ts: i64, val: f64) {
        let key = PartitionKey { series_id };
        let needs_flush;
        let mem_delta: isize;

        {
            let mut entry = self.partitions.entry(key).or_insert_with(PartitionBuffer::new);
            let buf = entry.value_mut();
            let old_cap = buf.memory_bytes();
            buf.timestamps.push(ts);
            buf.values.push(val);
            buf.last_write = Instant::now();
            let new_cap = buf.memory_bytes();
            mem_delta = (new_cap as isize) - (old_cap as isize);
            needs_flush = buf.timestamps.len() >= self.flush_threshold;
        }

        if mem_delta > 0 { self.buffer_memory.fetch_add(mem_delta as usize, Ordering::Relaxed); }
        else if mem_delta < 0 { self.buffer_memory.fetch_sub((-mem_delta) as usize, Ordering::Relaxed); }

        if needs_flush {
            self.flush_queue.lock().unwrap().push(key);
        }
    }

    /// Resolve series using the persistent hash cache.
    /// Fast path: DashMap hash lookup (~50ns).
    /// Slow path: full registry resolve + cache insert.
    #[inline]
    fn resolve_cached(&self, metric: &str, labels: &HashMap<String, String>) -> i64 {
        let hash = fast_series_hash(metric, labels);

        // Fast path: cache hit
        if let Some(id) = self.resolve_cache.get(&hash) {
            return *id;
        }

        // Slow path: full resolve + cache
        let labels_bt: BTreeMap<String, String> = labels.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let id = self.resolve_series(metric, &labels_bt);
        self.resolve_cache.insert(hash, id);
        id
    }

    /// Write a batch of labeled entries. Resolves series internally.
    /// Uses persistent hash cache — steady-state scraping is pure cache hits.
    fn write_batch_labeled(&self, entries: Vec<(String, HashMap<String, String>, i64, f64)>) {
        for (metric, labels_hm, ts, val) in entries {
            let series_id = self.resolve_cached(&metric, &labels_hm);
            self.write_point(series_id, ts, val);
        }
    }

    /// Binary batch: [series_id: i64, ts: i64, val: f64] = 24 bytes per entry.
    /// Use after pre-resolving series IDs.
    fn write_batch_raw(&self, data: &[u8]) {
        const ENTRY_SIZE: usize = 24;
        let count = data.len() / ENTRY_SIZE;
        for i in 0..count {
            let o = i * ENTRY_SIZE;
            let series_id = i64::from_ne_bytes(data[o..o+8].try_into().unwrap());
            let ts = i64::from_ne_bytes(data[o+8..o+16].try_into().unwrap());
            let val = f64::from_ne_bytes(data[o+16..o+24].try_into().unwrap());
            self.write_point(series_id, ts, val);
        }
    }

    // ── Flush ────────────────────────────────────────────────────────

    fn flush_pending(&self) -> usize {
        let keys: Vec<PartitionKey> = {
            let mut queue = self.flush_queue.lock().unwrap();
            std::mem::take(&mut *queue)
        };
        let mut seen = HashSet::new();
        let unique: Vec<PartitionKey> = keys.into_iter().filter(|k| seen.insert(*k)).collect();

        let mut count = 0;
        for key in unique {
            let should = self.partitions.get(&key).map(|b| b.timestamps.len() >= self.min_flush_size).unwrap_or(false);
            if should {
                self.flush_partition_individual(&key);
                count += 1;
            }
        }
        count
    }

    fn flush_partition_individual(&self, key: &PartitionKey) {
        if let Some((_, mut buf)) = self.partitions.remove(key) {
            if !buf.timestamps.is_empty() {
                let cp = self.compress_partition(key, &buf.timestamps, &buf.values);
                let meta = self.write_individual_chunk(&cp);
                self.index.write().unwrap().insert((*key, meta.min_ts), meta);
            }
            buf.timestamps.clear();
            buf.values.clear();
            buf.last_write = Instant::now();
            self.partitions.insert(*key, buf);
        }
    }

    fn flush_cold(&self, max_idle_secs: u64) -> (usize, usize, usize) {
        let now = Instant::now();
        let cold_keys: Vec<PartitionKey> = self.partitions.iter()
            .filter(|e| now.duration_since(e.value().last_write).as_secs() >= max_idle_secs)
            .map(|e| *e.key())
            .collect();

        let mut compressed: Vec<CompressedPartition> = Vec::new();
        let mut evicted = 0;
        let mut freed_mem = 0usize;

        for key in &cold_keys {
            if let Some((_, buf)) = self.partitions.remove(key) {
                freed_mem += buf.memory_bytes();
                if !buf.timestamps.is_empty() {
                    compressed.push(self.compress_partition(key, &buf.timestamps, &buf.values));
                }
                evicted += 1;
            }
        }

        self.buffer_memory.fetch_sub(freed_mem, Ordering::Relaxed);

        if compressed.is_empty() { return (0, evicted, 0); }

        let flushed = compressed.len();
        let mut files_written = 0;
        for batch in compressed.chunks(1000) {
            let metas = self.write_batched_chunk(batch);
            let mut index = self.index.write().unwrap();
            for (key, meta) in metas { index.insert((key, meta.min_ts), meta); }
            files_written += 1;
        }

        (flushed, evicted, files_written)
    }

    fn flush_by_memory(&self) -> usize {
        let current = self.buffer_memory.load(Ordering::Relaxed);
        if current <= self.memory_budget { return 0; }

        let mut sizes: Vec<(PartitionKey, usize)> = self.partitions.iter()
            .map(|e| (*e.key(), e.value().timestamps.len())).collect();
        sizes.sort_by(|a, b| b.1.cmp(&a.1));

        let mut freed = 0usize;
        let overage = current - self.memory_budget;
        let mut compressed: Vec<CompressedPartition> = Vec::new();

        for (key, _) in sizes {
            if freed >= overage { break; }
            if let Some((_, buf)) = self.partitions.remove(&key) {
                freed += buf.memory_bytes();
                if !buf.timestamps.is_empty() {
                    compressed.push(self.compress_partition(&key, &buf.timestamps, &buf.values));
                }
            }
        }

        self.buffer_memory.fetch_sub(freed, Ordering::Relaxed);
        let count = compressed.len();
        if !compressed.is_empty() {
            for batch in compressed.chunks(1000) {
                let metas = self.write_batched_chunk(batch);
                let mut index = self.index.write().unwrap();
                for (key, meta) in metas { index.insert((key, meta.min_ts), meta); }
            }
        }
        count
    }

    fn flush_all(&self) {
        let keys: Vec<(PartitionKey, usize)> = self.partitions.iter()
            .filter(|e| !e.value().timestamps.is_empty())
            .map(|e| (*e.key(), e.value().timestamps.len()))
            .collect();

        let mut small_compressed: Vec<CompressedPartition> = Vec::new();
        let mut new_individual: Vec<(PartitionKey, ChunkMeta)> = Vec::new();

        for (key, len) in keys {
            if let Some((_, mut buf)) = self.partitions.remove(&key) {
                if !buf.timestamps.is_empty() {
                    let cp = self.compress_partition(&key, &buf.timestamps, &buf.values);
                    if len >= self.min_flush_size {
                        new_individual.push((key, self.write_individual_chunk(&cp)));
                    } else {
                        small_compressed.push(cp);
                    }
                }
                buf.timestamps.clear();
                buf.values.clear();
                buf.last_write = Instant::now();
                self.partitions.insert(key, buf);
            }
        }

        let mut all_metas = new_individual;
        for batch in small_compressed.chunks(1000) {
            all_metas.extend(self.write_batched_chunk(batch));
        }
        if !all_metas.is_empty() {
            let mut index = self.index.write().unwrap();
            for (key, meta) in all_metas { index.insert((key, meta.min_ts), meta); }
        }
    }

    fn shutdown(&self) {
        self.flush_all();
        self.save_series();
    }

    // ── Compression ──────────────────────────────────────────────────

    fn compress_partition(&self, key: &PartitionKey, timestamps: &[i64], values: &[f64]) -> CompressedPartition {
        let config = pco::ChunkConfig::default().with_compression_level(self.compression_level);
        let ts_compressed = pco::standalone::simple_compress(timestamps, &config).expect("pco ts");
        let val_compressed = pco::standalone::simple_compress(values, &config).expect("pco val");

        let min_ts = timestamps[0];
        let max_ts = timestamps[timestamps.len() - 1];
        let point_count = timestamps.len() as u32;
        let (mut min_val, mut max_val, mut sum_val) = (values[0], values[0], 0.0f64);
        for &v in values { if v < min_val { min_val = v; } if v > max_val { max_val = v; } sum_val += v; }

        CompressedPartition { key: *key, min_ts, max_ts, point_count, min_val, max_val, sum_val, ts_compressed, val_compressed }
    }

    // ── Individual chunk writer (PCO1) ───────────────────────────────

    fn write_individual_chunk(&self, cp: &CompressedPartition) -> ChunkMeta {
        let series_id_str = cp.key.series_id.to_string();

        let path = self.data_dir.join("chunks")
            .join(&series_id_str)
            .join(format!("{}.pco1", cp.min_ts));

        self.ensure_dir(&path);

        // Store series_id as the partition key string in PCO1
        let pk_bytes = series_id_str.as_bytes();

        let mut out = Vec::with_capacity(64 + pk_bytes.len() + cp.ts_compressed.len() + cp.val_compressed.len());
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

        fs::File::create(&path).expect("create").write_all(&out).expect("write");

        ChunkMeta {
            min_ts: cp.min_ts, max_ts: cp.max_ts, point_count: cp.point_count,
            min_val: cp.min_val, max_val: cp.max_val, sum_val: cp.sum_val,
            path, data_offset: 0, data_len: 0,
        }
    }

    // ── Batched chunk writer (PCB1) ──────────────────────────────────

    fn write_batched_chunk(&self, partitions: &[CompressedPartition]) -> Vec<(PartitionKey, ChunkMeta)> {
        let batch_id = self.batch_counter.fetch_add(1, Ordering::Relaxed);
        let path = self.data_dir.join("batches").join(format!("batch_{:08}.pcb1", batch_id));
        self.ensure_dir(&path);

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

        fs::File::create(&path).expect("create batch").write_all(&out).expect("write batch");

        partitions.iter().enumerate().map(|(i, cp)| {
            let data_len = (4 + cp.ts_compressed.len() + 4 + cp.val_compressed.len()) as u32;
            (cp.key, ChunkMeta {
                min_ts: cp.min_ts, max_ts: cp.max_ts, point_count: cp.point_count,
                min_val: cp.min_val, max_val: cp.max_val, sum_val: cp.sum_val,
                path: path.clone(), data_offset: data_offsets[i] as u64, data_len,
            })
        }).collect()
    }

    fn ensure_dir(&self, path: &PathBuf) {
        let dir = path.parent().unwrap().to_path_buf();
        let mut dirs = self.created_dirs.lock().unwrap();
        if !dirs.contains(&dir) { fs::create_dir_all(&dir).expect("mkdir"); dirs.insert(dir); }
    }

    // ── Queries ──────────────────────────────────────────────────────

    /// Query by metric name + label filter. Returns data for all matching series.
    fn query_range_labeled(&self, metric_name: &str, label_filter: &Labels, t_start: i64, t_end: i64) -> Vec<(Labels, Vec<(i64, f64)>)> {
        let reg = self.series.read().unwrap();
        let series_ids = reg.find_series(metric_name, label_filter);

        series_ids.into_iter().filter_map(|sid| {
            let info = reg.info_for(sid)?;
            let labels = info.labels.clone();
            let points = self.query_range_by_id(sid, t_start, t_end);
            if points.is_empty() { None } else { Some((labels, points)) }
        }).collect()
    }

    /// Query a single series by ID.
    fn query_range_by_id(&self, series_id: i64, t_start: i64, t_end: i64) -> Vec<(i64, f64)> {
        let pk = PartitionKey { series_id };
        let mut results = Vec::new();

        {
            let index = self.index.read().unwrap();
            for ((k, _), meta) in index.range((pk, i64::MIN)..) {
                if k != &pk { break; }
                if meta.min_ts > t_end { break; }
                if meta.max_ts < t_start { continue; }
                if let Ok(points) = Self::read_chunk_data(meta, t_start, t_end) {
                    results.extend(points);
                }
            }
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                let ts = buf.timestamps[i];
                if ts >= t_start && ts <= t_end { results.push((ts, buf.values[i])); }
            }
        }

        results.sort_by_key(|&(ts, _)| ts);
        results
    }

    /// Aggregate query by metric + labels. Returns per-series aggregates.
    fn query_aggregate_labeled(&self, metric_name: &str, label_filter: &Labels, t_start: i64, t_end: i64, agg: AggFn) -> Vec<(Labels, f64)> {
        let reg = self.series.read().unwrap();
        let series_ids = reg.find_series(metric_name, label_filter);

        series_ids.into_iter().filter_map(|sid| {
            let info = reg.info_for(sid)?;
            let labels = info.labels.clone();
            let val = self.query_aggregate_by_id(sid, t_start, t_end, agg)?;
            Some((labels, val))
        }).collect()
    }

    fn query_aggregate_by_id(&self, series_id: i64, t_start: i64, t_end: i64, agg: AggFn) -> Option<f64> {
        let pk = PartitionKey { series_id };

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: Option<f64> = None;
        let mut global_max: Option<f64> = None;

        {
            let index = self.index.read().unwrap();
            for ((k, _), meta) in index.range((pk, i64::MIN)..) {
                if k != &pk { break; }
                if meta.min_ts > t_end { break; }
                if meta.max_ts < t_start { continue; }

                if meta.min_ts >= t_start && meta.max_ts <= t_end {
                    total_count += meta.point_count as u64;
                    total_sum += meta.sum_val;
                    global_min = Some(match global_min { Some(m) => m.min(meta.min_val), None => meta.min_val });
                    global_max = Some(match global_max { Some(m) => m.max(meta.max_val), None => meta.max_val });
                } else {
                    if let Ok(points) = Self::read_chunk_data(meta, t_start, t_end) {
                        for &(_, val) in &points {
                            total_count += 1; total_sum += val;
                            global_min = Some(match global_min { Some(m) => m.min(val), None => val });
                            global_max = Some(match global_max { Some(m) => m.max(val), None => val });
                        }
                    }
                }
            }
        }

        if let Some(buf) = self.partitions.get(&pk) {
            for i in 0..buf.timestamps.len() {
                if buf.timestamps[i] >= t_start && buf.timestamps[i] <= t_end {
                    let val = buf.values[i];
                    total_count += 1; total_sum += val;
                    global_min = Some(match global_min { Some(m) => m.min(val), None => val });
                    global_max = Some(match global_max { Some(m) => m.max(val), None => val });
                }
            }
        }

        if total_count == 0 { return None; }
        Some(match agg {
            AggFn::Avg => total_sum / total_count as f64,
            AggFn::Sum => total_sum,
            AggFn::Min => global_min.unwrap(),
            AggFn::Max => global_max.unwrap(),
            AggFn::Count => total_count as f64,
        })
    }

    // ── Chunk reading ────────────────────────────────────────────────

    fn read_chunk_data(meta: &ChunkMeta, t_start: i64, t_end: i64) -> Result<Vec<(i64, f64)>, String> {
        let data = fs::read(&meta.path).map_err(|e| e.to_string())?;
        let (ts_data, val_data) = if meta.data_offset > 0 {
            Self::parse_partition_data(&data, meta.data_offset as usize)?
        } else {
            Self::parse_pco1_data(&data)?
        };

        let timestamps: Vec<i64> = pco::standalone::simple_decompress(ts_data).map_err(|e| e.to_string())?;
        let values: Vec<f64> = pco::standalone::simple_decompress(val_data).map_err(|e| e.to_string())?;

        let mut results = Vec::new();
        for i in 0..timestamps.len() {
            if timestamps[i] >= t_start && timestamps[i] <= t_end {
                results.push((timestamps[i], values[i]));
            }
        }
        Ok(results)
    }

    fn parse_partition_data(data: &[u8], offset: usize) -> Result<(&[u8], &[u8]), String> {
        if offset + 4 > data.len() { return Err(format!("offset {} past file len {}", offset, data.len())); }
        let mut pos = offset;
        let ts_size = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
        if pos + ts_size + 4 > data.len() { return Err(format!("ts overrun at {}", offset)); }
        let ts_data = &data[pos..pos+ts_size]; pos += ts_size;
        let val_size = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
        if pos + val_size > data.len() { return Err(format!("val overrun at {}", offset)); }
        let val_data = &data[pos..pos+val_size];
        Ok((ts_data, val_data))
    }

    fn parse_pco1_data(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
        if data.len() < 4 || &data[0..4] != b"PCO1" { return Err("invalid PCO1".into()); }
        let mut pos = 5;
        pos += 4;
        pos += 16;
        let pk_len = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize;
        pos += 2 + pk_len;
        pos += 24;
        Self::parse_partition_data(data, pos)
    }

    // ── Retention ────────────────────────────────────────────────────

    fn delete_before(&self, before_ts: i64) -> (usize, usize) {
        let mut index = self.index.write().unwrap();

        let to_remove: Vec<(PartitionKey, i64)> = index.iter()
            .filter(|(_, meta)| meta.max_ts < before_ts)
            .map(|(k, _)| k.clone())
            .collect();

        let entries_removed = to_remove.len();
        let mut file_refcount: HashMap<PathBuf, usize> = HashMap::new();
        for meta in index.values() { *file_refcount.entry(meta.path.clone()).or_insert(0) += 1; }

        let mut files_to_delete: HashSet<PathBuf> = HashSet::new();
        for key in &to_remove {
            if let Some(meta) = index.remove(key) {
                if let Some(count) = file_refcount.get_mut(&meta.path) {
                    *count -= 1;
                    if *count == 0 { files_to_delete.insert(meta.path.clone()); }
                }
            }
        }

        drop(index);
        let files_deleted = files_to_delete.len();
        for path in &files_to_delete {
            let _ = fs::remove_file(path);
            if let Some(dir) = path.parent() { let _ = fs::remove_dir(dir); }
        }

        (entries_removed, files_deleted)
    }

    // ── Index rebuild ────────────────────────────────────────────────

    fn rebuild_index(&self) {
        let mut index = self.index.write().unwrap();
        for dir_name in &["chunks", "batches"] {
            let dir = self.data_dir.join(dir_name);
            if dir.exists() { Self::scan_dir_recursive(&dir, &mut index); }
        }
    }

    fn scan_dir_recursive(dir: &PathBuf, index: &mut BTreeMap<(PartitionKey, i64), ChunkMeta>) {
        let entries = match fs::read_dir(dir) { Ok(e) => e, Err(_) => return };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::scan_dir_recursive(&path, index);
            } else {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("pco1") => { if let Ok(entries) = Self::read_pco1_header(&path) { for (pk, meta) in entries { index.insert((pk, meta.min_ts), meta); } } }
                    Some("pcb1") => { if let Ok(entries) = Self::read_pcb1_headers(&path) { for (pk, meta) in entries { index.insert((pk, meta.min_ts), meta); } } }
                    _ => {}
                }
            }
        }
    }

    fn read_pco1_header(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        if data.len() < 4 || &data[0..4] != b"PCO1" { return Err("invalid".into()); }

        let mut pos = 5;
        let point_count = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap()); pos += 4;
        let min_ts = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
        let max_ts = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
        let pk_len = u16::from_be_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
        let pk_str = String::from_utf8_lossy(&data[pos..pos+pk_len]).to_string(); pos += pk_len;
        let min_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
        let max_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
        let sum_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap());

        // pk_str is the series_id as a string
        let series_id = pk_str.parse::<i64>().unwrap_or(0);

        Ok(vec![(
            PartitionKey { series_id },
            ChunkMeta { min_ts, max_ts, point_count, min_val, max_val, sum_val, path: path.clone(), data_offset: 0, data_len: 0 },
        )])
    }

    fn read_pcb1_headers(path: &PathBuf) -> Result<Vec<(PartitionKey, ChunkMeta)>, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        if data.len() < 4 || &data[0..4] != b"PCB1" { return Err("invalid".into()); }

        let n = u32::from_be_bytes(data[5..9].try_into().unwrap()) as usize;
        let mut results = Vec::with_capacity(n);
        let mut pos = 9;

        for _ in 0..n {
            let series_id = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let point_count = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap()); pos += 4;
            let min_ts = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let max_ts = i64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let min_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let max_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let sum_val = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let data_offset = u64::from_be_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
            let data_len = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap()); pos += 4;

            results.push((
                PartitionKey { series_id },
                ChunkMeta { min_ts, max_ts, point_count, min_val, max_val, sum_val, path: path.clone(), data_offset, data_len },
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
        let buffered_points: usize = self.partitions.iter().map(|e| e.value().timestamps.len()).sum();
        let buffer_memory = self.buffer_memory.load(Ordering::Relaxed);

        let mut unique_files: HashSet<&PathBuf> = HashSet::new();
        let mut total_disk_points: u64 = 0;
        for meta in index.values() { total_disk_points += meta.point_count as u64; unique_files.insert(&meta.path); }

        let total_bytes: u64 = unique_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|s| s.len()).sum();
        let total_points = total_disk_points + buffered_points as u64;
        let bytes_per_point = if total_disk_points > 0 { total_bytes as f64 / total_disk_points as f64 } else { 0.0 };

        EngineInfo { chunk_count, partition_count, series_count, buffered_points, total_points, total_bytes, bytes_per_point, buffer_memory, file_count: unique_files.len() }
    }
}

struct EngineInfo {
    chunk_count: usize, partition_count: usize, series_count: usize, buffered_points: usize,
    total_points: u64, total_bytes: u64, bytes_per_point: f64, buffer_memory: usize, file_count: usize,
}

#[derive(Clone, Copy)]
enum AggFn { Avg, Sum, Min, Max, Count }

// ═══════════════════════════════════════════════════════════════════════
// NIF interface
// ═══════════════════════════════════════════════════════════════════════

#[rustler::nif]
fn engine_new(data_dir: String, flush_threshold: usize, min_flush_size: usize, compression_level: usize, memory_budget_mb: usize) -> ResourceArc<EngineResource> {
    let budget = if memory_budget_mb == 0 { usize::MAX } else { memory_budget_mb * 1024 * 1024 };
    ResourceArc::new(EngineResource { engine: Engine::new(PathBuf::from(data_dir), flush_threshold, min_flush_size, compression_level, budget) })
}

/// Write labeled entries: [{metric_name, %{label => value}, timestamp, value}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_write_batch_labeled(resource: ResourceArc<EngineResource>, entries: Vec<(String, HashMap<String, String>, i64, f64)>) -> Atom {
    resource.deref().engine.write_batch_labeled(entries);
    atoms::ok()
}

/// Write pre-resolved binary: [series_id: i64, ts: i64, val: f64] × N (24 bytes each)
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_write_batch_raw(resource: ResourceArc<EngineResource>, data: Binary) -> Atom {
    resource.deref().engine.write_batch_raw(data.as_slice());
    atoms::ok()
}

/// Resolve (metric, labels) → series_id for pre-resolved write path.
#[rustler::nif]
fn engine_resolve_series(resource: ResourceArc<EngineResource>, metric: String, labels: HashMap<String, String>) -> (Atom, i64) {
    let labels_bt: BTreeMap<String, String> = labels.into_iter().collect();
    (atoms::ok(), resource.deref().engine.resolve_series(&metric, &labels_bt))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_pending(resource: ResourceArc<EngineResource>) -> (Atom, usize) {
    (atoms::ok(), resource.deref().engine.flush_pending())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_cold(resource: ResourceArc<EngineResource>, max_idle_secs: u64) -> (Atom, usize, usize, usize) {
    let (f, e, fi) = resource.deref().engine.flush_cold(max_idle_secs);
    (atoms::ok(), f, e, fi)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush_by_memory(resource: ResourceArc<EngineResource>) -> (Atom, usize) {
    (atoms::ok(), resource.deref().engine.flush_by_memory())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_flush(resource: ResourceArc<EngineResource>) -> Atom {
    resource.deref().engine.flush_all(); atoms::ok()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_delete_before(resource: ResourceArc<EngineResource>, before_ts: i64) -> (Atom, usize, usize) {
    let (e, f) = resource.deref().engine.delete_before(before_ts);
    (atoms::ok(), e, f)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn engine_shutdown(resource: ResourceArc<EngineResource>) -> Atom {
    resource.deref().engine.shutdown(); atoms::ok()
}

/// Query range for all series matching metric + labels.
/// Returns [{%{label => value}, [{ts, val}]}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_query_range(resource: ResourceArc<EngineResource>, metric: String, labels: HashMap<String, String>, t_start: i64, t_end: i64) -> (Atom, Vec<(HashMap<String, String>, Vec<(i64, f64)>)>) {
    let label_filter: BTreeMap<String, String> = labels.into_iter().collect();
    let results = resource.deref().engine.query_range_labeled(&metric, &label_filter, t_start, t_end);
    let out: Vec<(HashMap<String, String>, Vec<(i64, f64)>)> = results.into_iter()
        .map(|(labels, points)| (labels.into_iter().collect(), points))
        .collect();
    (atoms::ok(), out)
}

/// Aggregate query for all series matching metric + labels.
/// Returns [{%{label => value}, value}]
#[rustler::nif(schedule = "DirtyCpu")]
fn engine_query_aggregate(resource: ResourceArc<EngineResource>, metric: String, labels: HashMap<String, String>, t_start: i64, t_end: i64, agg: Atom) -> (Atom, Vec<(HashMap<String, String>, f64)>) {
    let label_filter: BTreeMap<String, String> = labels.into_iter().collect();
    let results = resource.deref().engine.query_aggregate_labeled(&metric, &label_filter, t_start, t_end, match_agg(agg));
    let out: Vec<(HashMap<String, String>, f64)> = results.into_iter()
        .map(|(labels, val)| (labels.into_iter().collect(), val))
        .collect();
    (atoms::ok(), out)
}

/// List all metric names.
#[rustler::nif]
fn engine_list_metrics(resource: ResourceArc<EngineResource>) -> (Atom, Vec<String>) {
    (atoms::ok(), resource.deref().engine.series.read().unwrap().list_metrics())
}

/// List all label names.
#[rustler::nif]
fn engine_list_labels(resource: ResourceArc<EngineResource>) -> (Atom, Vec<String>) {
    (atoms::ok(), resource.deref().engine.series.read().unwrap().all_label_names())
}

/// List values for a label key, optionally scoped to a metric.
#[rustler::nif]
fn engine_label_values(resource: ResourceArc<EngineResource>, metric: String, label_key: String) -> (Atom, Vec<String>) {
    (atoms::ok(), resource.deref().engine.series.read().unwrap().label_values(&metric, &label_key))
}

/// List all series (labels) for a metric.
#[rustler::nif]
fn engine_list_series(resource: ResourceArc<EngineResource>, metric: String) -> (Atom, Vec<HashMap<String, String>>) {
    let reg = resource.deref().engine.series.read().unwrap();
    let ids = reg.find_series(&metric, &BTreeMap::new());
    let out: Vec<HashMap<String, String>> = ids.into_iter().filter_map(|id| {
        reg.info_for(id).map(|info| info.labels.clone().into_iter().collect())
    }).collect();
    (atoms::ok(), out)
}

#[rustler::nif]
fn engine_info(resource: ResourceArc<EngineResource>) -> HashMap<String, f64> {
    let info = resource.deref().engine.info();
    let mut m = HashMap::new();
    m.insert("chunk_count".into(), info.chunk_count as f64);
    m.insert("partition_count".into(), info.partition_count as f64);
    m.insert("series_count".into(), info.series_count as f64);
    m.insert("buffered_points".into(), info.buffered_points as f64);
    m.insert("total_points".into(), info.total_points as f64);
    m.insert("total_bytes".into(), info.total_bytes as f64);
    m.insert("bytes_per_point".into(), info.bytes_per_point);
    m.insert("buffer_memory_mb".into(), info.buffer_memory as f64 / 1024.0 / 1024.0);
    m.insert("file_count".into(), info.file_count as f64);
    m
}

fn match_agg(atom: Atom) -> AggFn {
    if atom == atoms::avg() { AggFn::Avg }
    else if atom == atoms::sum() { AggFn::Sum }
    else if atom == atoms::min() { AggFn::Min }
    else if atom == atoms::max() { AggFn::Max }
    else if atom == atoms::count() { AggFn::Count }
    else { AggFn::Avg }
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    rustler::resource!(EngineResource, env);
    true
}

rustler::init!("Elixir.TimelessMetrics.RustEngine.Nif", load = load);
