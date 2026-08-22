use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use jazz::large_values::{
    BytePatch, ChunkedValue, ContentDomain, ContentTree, KvContentStore, LargeValue,
    MemoryContentStore, TailBounds, ValueEdit, ValueKind, ValueSelection,
};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use tempfile::TempDir;

fn elapsed_us(start: Instant) -> u128 {
    start.elapsed().as_micros()
}

fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn deterministic_bytes(size: usize) -> Vec<u8> {
    let mut state = 0x4c61_7267_6556_616c_u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn deterministic_text(size: usize) -> String {
    deterministic_bytes(size)
        .into_iter()
        .map(|byte| char::from(b'a' + byte % 26))
        .collect()
}

/// A reproducible source which keeps only the current transport buffer alive.
struct DeterministicChunks {
    remaining: usize,
    chunk_bytes: usize,
    state: u64,
}

impl DeterministicChunks {
    fn new(bytes: usize, chunk_bytes: usize) -> Self {
        Self {
            remaining: bytes,
            chunk_bytes,
            state: 0x4c61_7267_6556_616c_u64,
        }
    }
}

impl Iterator for DeterministicChunks {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.remaining.min(self.chunk_bytes);
        if len == 0 {
            return None;
        }
        self.remaining -= len;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            bytes.push(self.state as u8);
        }
        Some(bytes)
    }
}

/// Linux resident-set snapshot. This intentionally reports both current RSS
/// and high-water mark: RocksDB has a fixed cache/write-buffer floor, while
/// the deltas between workload phases expose value-size-dependent memory.
fn rss_bytes() -> serde_json::Value {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return serde_json::Value::Null;
    };
    let kb = |field| {
        status.lines().find_map(|line| {
            line.strip_prefix(field)
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
                .map(|value| value * 1024)
        })
    };
    serde_json::json!({ "current": kb("VmRSS:"), "peak": kb("VmHWM:") })
}

fn rss_current(snapshot: &serde_json::Value) -> Option<u64> {
    snapshot.get("current")?.as_u64()
}

fn rss_delta_from_open(
    snapshot: &serde_json::Value,
    after_open: &serde_json::Value,
) -> Option<i64> {
    Some(rss_current(snapshot)? as i64 - rss_current(after_open)? as i64)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => directory_bytes(&path),
                Ok(kind) if kind.is_file() => entry.metadata().map(|meta| meta.len()).unwrap_or(0),
                _ => 0,
            }
        })
        .sum()
}

fn rocks_streaming_receipt() {
    // 64 MiB keeps the default developer receipt practical. A one-GiB receipt
    // is deliberately one environment variable away:
    // JAZZ_LARGE_VALUE_ROCKS_BYTES=1073741824 cargo bench -p jazz --bench large_values
    let size = std::env::var("JAZZ_LARGE_VALUE_ROCKS_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64 * 1024 * 1024);
    let chunk_bytes = std::env::var("JAZZ_LARGE_VALUE_ROCKS_CHUNK_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64 * 1024);
    let domain = ContentDomain::new(b"large-value-rocks-benchmark".to_vec()).unwrap();
    let tree = ContentTree::new(Default::default()).unwrap();
    let directory = TempDir::new().expect("create RocksDB benchmark directory");
    let storage = RocksDbStorage::open_with_durability(
        directory.path(),
        &[jazz::large_values::CONTENT_OBJECTS_CF],
        Durability::WalNoSync,
    )
    .expect("open RocksDB content storage");
    let rss_after_open = rss_bytes();
    let start = Instant::now();
    let (root, root_byte_len) = {
        let mut store = KvContentStore::new(&storage);
        tree.build_streaming(
            &domain,
            DeterministicChunks::new(size, chunk_bytes),
            &mut store,
        )
        .expect("stream content into RocksDB")
    };
    let create_elapsed = start.elapsed();
    let rss_after_create = rss_bytes();
    let metrics_after_create = storage.metrics().expect("read RocksDB metrics");
    let directory_bytes_after_create = directory_bytes(directory.path());
    let value = LargeValue::Chunked(ChunkedValue {
        root,
        root_byte_len,
        edit_tail: Vec::new(),
    });

    // Reopen before reads to ensure this is a persisted-lookup receipt rather
    // than a favorable path through the writer's just-populated RocksDB cache.
    drop(storage);
    let rss_after_close = rss_bytes();
    let start = Instant::now();
    let storage = RocksDbStorage::open_with_durability(
        directory.path(),
        &[jazz::large_values::CONTENT_OBJECTS_CF],
        Durability::WalNoSync,
    )
    .expect("reopen RocksDB content storage");
    let reopen_us = elapsed_us(start);
    let rss_after_reopen = rss_bytes();

    let range_len = 4096_u64;
    let offsets = [0, root_byte_len / 2, root_byte_len - range_len];
    let mut range_us = Vec::new();
    for offset in offsets {
        let start = Instant::now();
        let selected = {
            let store = KvContentStore::new(&storage);
            value
                .select(
                    ValueKind::Bytes,
                    &ValueSelection::ByteRange {
                        offset,
                        len: range_len,
                    },
                    &domain,
                    tree,
                    &store,
                )
                .expect("read bounded byte range")
        };
        black_box(selected);
        range_us.push(elapsed_us(start));
    }
    let rss_after_ranges = rss_bytes();

    println!(
        "{}",
        serde_json::json!({
            "scenario": "large_value_rocksdb_streaming",
            "logical_bytes": root_byte_len,
            "source_chunk_bytes": chunk_bytes,
            "active_streaming_bytes_upper_bound": streaming_memory_upper_bound(size, chunk_bytes),
            "create_us": create_elapsed.as_micros(),
            "create_mib_per_s": mib_per_second(size, create_elapsed),
            "range_bytes": range_len,
            "range_offsets": offsets,
            "range_us": range_us,
            "rss_after_open": rss_after_open,
            "rss_after_create": rss_after_create,
            "rss_after_close": rss_after_close,
            "rss_after_reopen": rss_after_reopen,
            "rss_after_ranges": rss_after_ranges,
            "rss_current_delta_after_create_from_open": rss_delta_from_open(&rss_after_create, &rss_after_open),
            "rss_current_delta_after_reopen_from_open": rss_delta_from_open(&rss_after_reopen, &rss_after_open),
            "rss_current_delta_after_ranges_from_open": rss_delta_from_open(&rss_after_ranges, &rss_after_open),
            "reopen_us": reopen_us,
            "rocksdb_after_create": metrics_after_create,
            "directory_bytes_after_create": directory_bytes_after_create,
            "note": "RSS includes RocksDB's shared cache/write-buffer floor; compare phase deltas and active_streaming_bytes_upper_bound, not RSS to logical_bytes directly.",
        })
    );
}

fn streaming_memory_upper_bound(logical_bytes: usize, source_chunk_bytes: usize) -> usize {
    // Default profile: one <=64KiB leaf, the current source buffer, and at
    // most one unfinished 128-child descriptor list per live level. Count
    // levels using the *minimum* fanout, making this a conservative bound even
    // when content-defined boundaries happen unusually early.
    let profile = jazz::large_values::ChunkingProfile::default();
    let mut children = logical_bytes.div_ceil(profile.min_leaf_bytes).max(1);
    let mut levels = 1;
    while children > 1 {
        children = children.div_ceil(profile.min_children);
        levels += 1;
    }
    source_chunk_bytes
        + profile.max_leaf_bytes
        + levels * profile.max_children * (32 + std::mem::size_of::<u64>())
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    let size = std::env::var("JAZZ_LARGE_VALUE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1024 * 1024);
    let bytes = deterministic_bytes(size);
    let domain = ContentDomain::new(b"large-value-benchmark".to_vec()).unwrap();
    let tree = ContentTree::new(Default::default()).unwrap();
    let mut store = MemoryContentStore::default();

    let start = Instant::now();
    let mut value = LargeValue::create(
        ValueKind::Bytes,
        &domain,
        bytes.clone(),
        8 * 1024,
        tree,
        &mut store,
    )
    .unwrap();
    let create_elapsed = start.elapsed();
    let objects_after_create = store.len();

    let start = Instant::now();
    for edit in 0..32_u64 {
        let operation = ValueEdit::Bytes(BytePatch::insert((size as u64 / 2) + edit, [b'x']));
        let patch = value
            .lower_edit(ValueKind::Bytes, operation, &domain, tree, &store)
            .unwrap();
        value = value
            .apply_edit(
                ValueKind::Bytes,
                &domain,
                patch,
                8 * 1024,
                TailBounds::default(),
                tree,
                &mut store,
            )
            .unwrap();
    }
    let edit_us = elapsed_us(start);

    let start = Instant::now();
    let range = value
        .select(
            ValueKind::Bytes,
            &ValueSelection::ByteRange {
                offset: size as u64 / 2,
                len: 4096,
            },
            &domain,
            tree,
            &store,
        )
        .unwrap();
    black_box(range);
    let range_us = elapsed_us(start);

    let start = Instant::now();
    let materialized = value
        .materialize(ValueKind::Bytes, &domain, tree, &store)
        .unwrap();
    black_box(&materialized);
    let materialize_elapsed = start.elapsed();

    let objects_before_consolidate = store.len();
    let start = Instant::now();
    value = value
        .apply_edit(
            ValueKind::Bytes,
            &domain,
            BytePatch::insert(size as u64 / 3, b"consolidate"),
            8 * 1024,
            TailBounds {
                max_entries: 32,
                max_encoded_bytes: 16 * 1024,
            },
            tree,
            &mut store,
        )
        .unwrap();
    black_box(&value);
    let consolidate_us = elapsed_us(start);

    println!(
        "{}",
        serde_json::json!({
            "scenario": "large_value_pipeline",
            "bytes": size,
            "create_us": create_elapsed.as_micros(),
            "create_mib_per_s": mib_per_second(size, create_elapsed),
            "objects_after_create": objects_after_create,
            "edit_count": 32,
            "edit_tail_total_us": edit_us,
            "edit_tail_us_per_edit": edit_us as f64 / 32.0,
            "range_bytes": 4096,
            "range_us": range_us,
            "materialize_us": materialize_elapsed.as_micros(),
            "materialize_mib_per_s": mib_per_second(materialized.len(), materialize_elapsed),
            "consolidate_us": consolidate_us,
            "new_objects_on_consolidate": store.len() - objects_before_consolidate,
        })
    );

    let json_source = serde_json::to_vec(&serde_json::json!({
        "meta": { "version": 1 },
        "payload": deterministic_text(size),
    }))
    .unwrap();
    let mut json_store = MemoryContentStore::default();
    let start = Instant::now();
    let json_value = LargeValue::create(
        ValueKind::Json,
        &domain,
        json_source.clone(),
        8 * 1024,
        tree,
        &mut json_store,
    )
    .unwrap();
    let json_create_elapsed = start.elapsed();

    let start = Instant::now();
    let selected = json_value
        .select(
            ValueKind::Json,
            &ValueSelection::JsonPointer("/meta/version".to_owned()),
            &domain,
            tree,
            &json_store,
        )
        .unwrap();
    black_box(selected);
    let json_pointer_us = elapsed_us(start);

    let replacement = serde_json::json!({
        "meta": { "version": 2 },
        "payload": deterministic_text(size),
    });
    let start = Instant::now();
    let patch = json_value
        .lower_edit(
            ValueKind::Json,
            ValueEdit::Json(replacement),
            &domain,
            tree,
            &json_store,
        )
        .unwrap();
    let json_diff_us = elapsed_us(start);
    let inserted_patch_bytes = patch.insert.len();

    println!(
        "{}",
        serde_json::json!({
            "scenario": "large_json_pipeline",
            "bytes": json_source.len(),
            "create_us": json_create_elapsed.as_micros(),
            "create_mib_per_s": mib_per_second(json_source.len(), json_create_elapsed),
            "objects_after_create": json_store.len(),
            "json_pointer_us": json_pointer_us,
            "json_replace_diff_us": json_diff_us,
            "json_replace_insert_bytes": inserted_patch_bytes,
        })
    );

    rocks_streaming_receipt();
}
