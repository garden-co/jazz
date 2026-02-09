// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//! WASM test suite and benchmarks for bf-tree.
//!
//! Run tests with:
//! ```bash
//! cd tests/wasm
//! RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' wasm-pack test --headless --chrome
//! ```
//!
//! Build benchmarks with:
//! ```bash
//! cd tests/wasm
//! RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' wasm-pack build --target web
//! ```

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

// Test module - only compiled during `wasm-pack test`
#[cfg(test)]
mod tests {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Test basic in-memory tree operations in WASM.
    #[wasm_bindgen_test]
    fn test_memory_tree_basic() {
        let tree = bf_tree::wasm::create_memory_tree(1024 * 1024).unwrap();

        assert!(tree.insert(b"key1", b"value1"));
        assert!(tree.insert(b"key2", b"value2"));
        assert!(tree.insert(b"key3", b"value3"));

        let result = tree.read(b"key1");
        assert_eq!(result, Some(b"value1".to_vec()));

        let result = tree.read(b"key2");
        assert_eq!(result, Some(b"value2".to_vec()));

        let result = tree.read(b"key3");
        assert_eq!(result, Some(b"value3".to_vec()));
    }

    /// Test delete operations in WASM.
    #[wasm_bindgen_test]
    fn test_memory_tree_delete() {
        let tree = bf_tree::wasm::create_memory_tree(1024 * 1024).unwrap();

        assert!(tree.insert(b"key", b"value"));

        let result = tree.read(b"key");
        assert_eq!(result, Some(b"value".to_vec()));

        tree.delete(b"key");

        // After delete, read returns None (deleted is treated as not found in the wrapper)
        let result = tree.read(b"key");
        assert_eq!(result, None);
    }

    /// Test overwriting values.
    #[wasm_bindgen_test]
    fn test_memory_tree_overwrite() {
        let tree = bf_tree::wasm::create_memory_tree(1024 * 1024).unwrap();

        assert!(tree.insert(b"key", b"value1"));

        let result = tree.read(b"key");
        assert_eq!(result, Some(b"value1".to_vec()));

        assert!(tree.insert(b"key", b"value2_longer"));

        let result = tree.read(b"key");
        assert_eq!(result, Some(b"value2_longer".to_vec()));
    }

    /// Test reading non-existent keys.
    #[wasm_bindgen_test]
    fn test_memory_tree_not_found() {
        let tree = bf_tree::wasm::create_memory_tree(1024 * 1024).unwrap();

        let result = tree.read(b"nonexistent");
        assert_eq!(result, None);
    }

    /// Test with many keys to trigger tree splits.
    #[wasm_bindgen_test]
    fn test_memory_tree_many_keys() {
        let tree = bf_tree::wasm::create_memory_tree(4 * 1024 * 1024).unwrap();

        // Insert 1000 key-value pairs
        for i in 0..1000u32 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{:06}", i);
            assert!(tree.insert(key.as_bytes(), value.as_bytes()));
        }

        // Verify some samples
        for i in [0, 100, 500, 999] {
            let key = format!("key_{:06}", i);
            let expected = format!("value_{:06}", i);
            let result = tree.read(key.as_bytes());
            assert_eq!(result, Some(expected.into_bytes()));
        }
    }

    /// Test OPFS tree creation (async).
    /// Note: This test requires running in a Web Worker context with HTTPS.
    #[wasm_bindgen_test]
    async fn test_opfs_tree_basic() {
        // Try to create an OPFS-backed tree
        let result = bf_tree::wasm::open_tree_with_opfs("test_basic.db", 1024 * 1024).await;

        match result {
            Ok(tree) => {
                assert!(tree.insert(b"opfs_key", b"opfs_value"));

                let read_result = tree.read(b"opfs_key");
                assert_eq!(read_result, Some(b"opfs_value".to_vec()));
            }
            Err(e) => {
                // OPFS might not be available in all test environments
                web_sys::console::log_1(&format!("OPFS not available: {:?}", e).into());
            }
        }
    }

    /// Helper to remove OPFS files. Ignores errors if files don't exist.
    async fn cleanup_opfs(db_name: &str) {
        let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
        let storage = global.navigator().storage();
        let root: web_sys::FileSystemDirectoryHandle =
            wasm_bindgen_futures::JsFuture::from(storage.get_directory())
                .await
                .unwrap()
                .unchecked_into();

        for suffix in &[".bftree", ".wal"] {
            let name = format!("{}{}", db_name, suffix);
            let opts = js_sys::Object::new();
            js_sys::Reflect::set(&opts, &"recursive".into(), &true.into()).unwrap();
            let remove_fn = js_sys::Reflect::get(&root, &"removeEntry".into()).unwrap();
            let remove_fn: js_sys::Function = remove_fn.unchecked_into();
            let promise = remove_fn.call2(&root, &name.into(), &opts);
            if let Ok(p) = promise {
                let p: js_sys::Promise = p.unchecked_into();
                let _ = wasm_bindgen_futures::JsFuture::from(p).await;
            }
        }
    }

    /// Test snapshot round-trip: insert keys, snapshot, drop, reopen, verify.
    #[wasm_bindgen_test]
    async fn test_opfs_snapshot_round_trip() {
        let db_name = "test_snapshot_rt";
        cleanup_opfs(db_name).await;

        // Phase 1: Create tree, insert data, snapshot
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to open persistent tree");

            for i in 0..100u32 {
                let key = format!("snap_key_{:06}", i);
                let value = format!("snap_val_{:06}", i);
                assert!(tree.insert(key.as_bytes(), value.as_bytes()));
            }

            tree.snapshot();
            // Tree dropped here — OPFS file handles closed
        }

        // Phase 2: Reopen and verify all keys
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to reopen persistent tree");

            for i in 0..100u32 {
                let key = format!("snap_key_{:06}", i);
                let expected = format!("snap_val_{:06}", i);
                let result = tree.read(key.as_bytes());
                assert_eq!(
                    result,
                    Some(expected.into_bytes()),
                    "Key {} not found after snapshot recovery",
                    i
                );
            }
        }

        cleanup_opfs(db_name).await;
    }

    /// Test WAL recovery: insert keys (no snapshot), drop, reopen, verify WAL replay.
    #[wasm_bindgen_test]
    async fn test_opfs_wal_recovery() {
        let db_name = "test_wal_recovery";
        cleanup_opfs(db_name).await;

        // Phase 1: Create tree, insert data, flush WAL (no snapshot)
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to open persistent tree");

            for i in 0..50u32 {
                let key = format!("wal_key_{:06}", i);
                let value = format!("wal_val_{:06}", i);
                assert!(tree.insert(key.as_bytes(), value.as_bytes()));
            }

            tree.flush_wal();
            // No snapshot! WAL should contain all writes.
        }

        // Phase 2: Reopen — should replay WAL
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to reopen persistent tree");

            for i in 0..50u32 {
                let key = format!("wal_key_{:06}", i);
                let expected = format!("wal_val_{:06}", i);
                let result = tree.read(key.as_bytes());
                assert_eq!(
                    result,
                    Some(expected.into_bytes()),
                    "Key {} not found after WAL recovery",
                    i
                );
            }
        }

        cleanup_opfs(db_name).await;
    }

    /// Test snapshot + WAL: insert, snapshot, insert more, flush WAL, reopen.
    /// Snapshot restores first batch, WAL replays second batch.
    #[wasm_bindgen_test]
    async fn test_opfs_snapshot_plus_wal() {
        let db_name = "test_snap_wal";
        cleanup_opfs(db_name).await;

        // Phase 1: Insert 50 keys, snapshot, insert 50 more, flush WAL
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to open persistent tree");

            // First batch
            for i in 0..50u32 {
                let key = format!("sw_key_{:06}", i);
                let value = format!("sw_val_{:06}", i);
                assert!(tree.insert(key.as_bytes(), value.as_bytes()));
            }

            tree.snapshot();

            // Second batch (after snapshot, only in WAL)
            for i in 50..100u32 {
                let key = format!("sw_key_{:06}", i);
                let value = format!("sw_val_{:06}", i);
                assert!(tree.insert(key.as_bytes(), value.as_bytes()));
            }

            tree.flush_wal();
        }

        // Phase 2: Reopen — snapshot restores 0-49, WAL replays 50-99
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to reopen persistent tree");

            for i in 0..100u32 {
                let key = format!("sw_key_{:06}", i);
                let expected = format!("sw_val_{:06}", i);
                let result = tree.read(key.as_bytes());
                assert_eq!(
                    result,
                    Some(expected.into_bytes()),
                    "Key {} not found after snapshot+WAL recovery",
                    i
                );
            }
        }

        cleanup_opfs(db_name).await;
    }

    /// Test fresh start: open with new db_name, insert/read works.
    #[wasm_bindgen_test]
    async fn test_opfs_fresh_start() {
        let db_name = "test_fresh_start";
        cleanup_opfs(db_name).await;

        let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
            .await
            .expect("Failed to open fresh persistent tree");

        assert!(tree.insert(b"fresh_key", b"fresh_value"));
        let result = tree.read(b"fresh_key");
        assert_eq!(result, Some(b"fresh_value".to_vec()));

        // Non-existent key should return None
        let result = tree.read(b"no_such_key");
        assert_eq!(result, None);

        cleanup_opfs(db_name).await;
    }

    /// Test scan_with_end_key works correctly after snapshot + WAL recovery.
    #[wasm_bindgen_test]
    async fn test_opfs_scan_after_recovery() {
        let db_name = "test_scan_recovery";
        cleanup_opfs(db_name).await;

        // Phase 1: Insert keys with sortable names, snapshot some, WAL the rest
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to open persistent tree");

            // Insert keys a_000 through a_019 (sorted range)
            for i in 0..20u32 {
                let key = format!("a_{:03}", i);
                let value = format!("val_{:03}", i);
                tree.insert(key.as_bytes(), value.as_bytes());
            }

            tree.snapshot();

            // Insert more keys after snapshot (these go to WAL only)
            for i in 20..30u32 {
                let key = format!("a_{:03}", i);
                let value = format!("val_{:03}", i);
                tree.insert(key.as_bytes(), value.as_bytes());
            }

            tree.flush_wal();
        }

        // Phase 2: Reopen and verify range scan
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to reopen persistent tree");

            // Scan a_005 through a_014 (end key is inclusive)
            let results = tree.scan_range(b"a_005", b"a_014");
            // results is [key, value, key, value, ...] so pairs = len/2
            let pair_count = results.len() / 2;
            assert_eq!(
                pair_count, 10,
                "Scan a_005..=a_014 should return 10 keys, got {}",
                pair_count
            );

            // Verify first key is a_005
            let first_key = results[0].to_vec();
            assert_eq!(first_key, b"a_005", "First scan key should be a_005");

            // Verify last key is a_014
            let last_key = results[(pair_count - 1) * 2].to_vec();
            assert_eq!(last_key, b"a_014", "Last scan key should be a_014");

            // Scan across the snapshot/WAL boundary: a_018 through a_024 (inclusive)
            let cross_results = tree.scan_range(b"a_018", b"a_024");
            let cross_count = cross_results.len() / 2;
            assert_eq!(
                cross_count, 7,
                "Cross-boundary scan a_018..=a_024 should return 7 keys, got {}",
                cross_count
            );
        }

        cleanup_opfs(db_name).await;
    }

    /// Test delete survives recovery via WAL.
    #[wasm_bindgen_test]
    async fn test_opfs_delete_recovery() {
        let db_name = "test_delete_recovery";
        cleanup_opfs(db_name).await;

        // Phase 1: Insert, then delete, flush WAL
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to open persistent tree");

            assert!(tree.insert(b"del_key", b"del_value"));
            tree.delete(b"del_key");
            tree.flush_wal();
        }

        // Phase 2: Reopen — delete should have been replayed
        {
            let tree = bf_tree::wasm::open_tree_with_opfs_persistent(db_name, 1024 * 1024)
                .await
                .expect("Failed to reopen persistent tree");

            let result = tree.read(b"del_key");
            assert_eq!(result, None, "Deleted key should not be found after WAL recovery");
        }

        cleanup_opfs(db_name).await;
    }
}

// Benchmark exports for manual testing

/// Get performance object - works in both Window and Worker contexts.
fn get_performance() -> web_sys::Performance {
    // Try window first (main thread)
    if let Some(window) = web_sys::window() {
        if let Some(perf) = window.performance() {
            return perf;
        }
    }

    // Fall back to worker global scope
    let global = js_sys::global();
    let worker_scope: web_sys::DedicatedWorkerGlobalScope = global.unchecked_into();
    worker_scope.performance().expect("Performance API not available")
}

/// Benchmark result structure for JavaScript consumption.
#[wasm_bindgen]
pub struct BenchmarkResult {
    operation: String,
    count: u32,
    total_ms: f64,
    ops_per_sec: f64,
}

#[wasm_bindgen]
impl BenchmarkResult {
    #[wasm_bindgen(getter)]
    pub fn operation(&self) -> String {
        self.operation.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn count(&self) -> u32 {
        self.count
    }

    #[wasm_bindgen(getter)]
    pub fn total_ms(&self) -> f64 {
        self.total_ms
    }

    #[wasm_bindgen(getter)]
    pub fn ops_per_sec(&self) -> f64 {
        self.ops_per_sec
    }
}

/// Run sequential insert benchmark.
#[wasm_bindgen]
pub fn bench_sequential_insert(count: u32, cache_size_mb: u32) -> BenchmarkResult {
    let tree = bf_tree::wasm::create_memory_tree((cache_size_mb as usize) * 1024 * 1024).unwrap();

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        tree.insert(key.as_bytes(), value.as_bytes());
    }
    let elapsed = perf.now() - start;

    BenchmarkResult {
        operation: "sequential_insert".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    }
}

/// Run random read benchmark (requires pre-populated tree).
#[wasm_bindgen]
pub fn bench_random_read(count: u32, cache_size_mb: u32) -> BenchmarkResult {
    let tree = bf_tree::wasm::create_memory_tree((cache_size_mb as usize) * 1024 * 1024).unwrap();

    // Pre-populate
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        tree.insert(key.as_bytes(), value.as_bytes());
    }

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        // Pseudo-random access pattern
        let idx = (i.wrapping_mul(7919)) % count;
        let key = format!("key_{:08}", idx);
        let _ = tree.read(key.as_bytes());
    }
    let elapsed = perf.now() - start;

    BenchmarkResult {
        operation: "random_read".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    }
}

/// Run mixed read/write benchmark (80% read, 20% write).
#[wasm_bindgen]
pub fn bench_mixed(count: u32, cache_size_mb: u32) -> BenchmarkResult {
    let tree = bf_tree::wasm::create_memory_tree((cache_size_mb as usize) * 1024 * 1024).unwrap();

    // Pre-populate with some data
    let base_count = count / 10;
    for i in 0..base_count {
        let key = format!("mixed_{:06}", i);
        tree.insert(key.as_bytes(), b"initial");
    }

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        let key_idx = i % base_count;
        let key = format!("mixed_{:06}", key_idx);

        if i % 5 == 0 {
            // 20% writes
            let value = format!("updated_{}", i);
            tree.insert(key.as_bytes(), value.as_bytes());
        } else {
            // 80% reads
            let _ = tree.read(key.as_bytes());
        }
    }
    let elapsed = perf.now() - start;

    BenchmarkResult {
        operation: "mixed_80read_20write".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    }
}

// ============== OPFS Benchmarks ==============

/// Run OPFS sequential insert benchmark.
#[wasm_bindgen]
pub async fn bench_opfs_sequential_insert(count: u32, cache_size_mb: u32) -> Result<BenchmarkResult, JsValue> {
    // Use a unique filename to avoid conflicts
    let db_name = format!("bench_insert_{}.db", js_sys::Date::now() as u64);
    let tree = bf_tree::wasm::open_tree_with_opfs(&db_name, (cache_size_mb as usize) * 1024 * 1024).await?;

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        tree.insert(key.as_bytes(), value.as_bytes());
    }
    let elapsed = perf.now() - start;

    Ok(BenchmarkResult {
        operation: "opfs_sequential_insert".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    })
}

/// Run OPFS random read benchmark.
#[wasm_bindgen]
pub async fn bench_opfs_random_read(count: u32, cache_size_mb: u32) -> Result<BenchmarkResult, JsValue> {
    let db_name = format!("bench_read_{}.db", js_sys::Date::now() as u64);
    let tree = bf_tree::wasm::open_tree_with_opfs(&db_name, (cache_size_mb as usize) * 1024 * 1024).await?;

    // Pre-populate
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        tree.insert(key.as_bytes(), value.as_bytes());
    }

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        // Pseudo-random access pattern
        let idx = (i.wrapping_mul(7919)) % count;
        let key = format!("key_{:08}", idx);
        let _ = tree.read(key.as_bytes());
    }
    let elapsed = perf.now() - start;

    Ok(BenchmarkResult {
        operation: "opfs_random_read".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    })
}

/// Run OPFS mixed read/write benchmark (80% read, 20% write).
#[wasm_bindgen]
pub async fn bench_opfs_mixed(count: u32, cache_size_mb: u32) -> Result<BenchmarkResult, JsValue> {
    let db_name = format!("bench_mixed_{}.db", js_sys::Date::now() as u64);
    let tree = bf_tree::wasm::open_tree_with_opfs(&db_name, (cache_size_mb as usize) * 1024 * 1024).await?;

    // Pre-populate with some data
    let base_count = count / 10;
    for i in 0..base_count {
        let key = format!("mixed_{:06}", i);
        tree.insert(key.as_bytes(), b"initial");
    }

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        let key_idx = i % base_count;
        let key = format!("mixed_{:06}", key_idx);

        if i % 5 == 0 {
            // 20% writes
            let value = format!("updated_{}", i);
            tree.insert(key.as_bytes(), value.as_bytes());
        } else {
            // 80% reads
            let _ = tree.read(key.as_bytes());
        }
    }
    let elapsed = perf.now() - start;

    Ok(BenchmarkResult {
        operation: "opfs_mixed_80read_20write".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    })
}

/// Get OPFS storage usage info.
#[wasm_bindgen]
pub async fn get_opfs_storage_info() -> Result<JsValue, JsValue> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &"navigator".into())?;
    let storage = js_sys::Reflect::get(&navigator, &"storage".into())?;

    let estimate_fn = js_sys::Reflect::get(&storage, &"estimate".into())?;
    let estimate_fn: js_sys::Function = estimate_fn.unchecked_into();

    let promise = estimate_fn.call0(&storage)?;
    let promise: js_sys::Promise = promise.unchecked_into();

    let result = wasm_bindgen_futures::JsFuture::from(promise).await?;
    Ok(result)
}

/// Clear all OPFS storage for this origin.
#[wasm_bindgen]
pub async fn clear_opfs_storage() -> Result<u32, JsValue> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &"navigator".into())?;
    let storage = js_sys::Reflect::get(&navigator, &"storage".into())?;

    // Get the OPFS root directory
    let get_directory_fn = js_sys::Reflect::get(&storage, &"getDirectory".into())?;
    let get_directory_fn: js_sys::Function = get_directory_fn.unchecked_into();
    let promise = get_directory_fn.call0(&storage)?;
    let promise: js_sys::Promise = promise.unchecked_into();
    let root = wasm_bindgen_futures::JsFuture::from(promise).await?;

    // Get entries and delete them
    let entries_fn = js_sys::Reflect::get(&root, &"entries".into())?;
    let entries_fn: js_sys::Function = entries_fn.unchecked_into();
    let iterator = entries_fn.call0(&root)?;

    let mut deleted_count = 0u32;
    loop {
        let next_fn = js_sys::Reflect::get(&iterator, &"next".into())?;
        let next_fn: js_sys::Function = next_fn.unchecked_into();
        let next_result = next_fn.call0(&iterator)?;
        let next_result: js_sys::Promise = next_result.unchecked_into();
        let entry = wasm_bindgen_futures::JsFuture::from(next_result).await?;

        let done = js_sys::Reflect::get(&entry, &"done".into())?;
        if done.as_bool().unwrap_or(true) {
            break;
        }

        let value = js_sys::Reflect::get(&entry, &"value".into())?;
        let arr: js_sys::Array = value.unchecked_into();
        let name = arr.get(0);
        let name_str = name.as_string().unwrap_or_default();

        // Delete the entry
        let remove_fn = js_sys::Reflect::get(&root, &"removeEntry".into())?;
        let remove_fn: js_sys::Function = remove_fn.unchecked_into();

        let opts = js_sys::Object::new();
        js_sys::Reflect::set(&opts, &"recursive".into(), &true.into())?;

        let remove_promise = remove_fn.call2(&root, &name, &opts)?;
        let remove_promise: js_sys::Promise = remove_promise.unchecked_into();
        wasm_bindgen_futures::JsFuture::from(remove_promise).await?;

        web_sys::console::log_1(&format!("Deleted: {}", name_str).into());
        deleted_count += 1;
    }

    Ok(deleted_count)
}

/// Run OPFS cold read benchmark - reads data from a freshly opened tree.
/// This tests reading from persistent storage, not from cache.
#[wasm_bindgen]
pub async fn bench_opfs_cold_read(count: u32, cache_size_mb: u32) -> Result<BenchmarkResult, JsValue> {
    let db_name = format!("bench_cold_{}.db", js_sys::Date::now() as u64);

    // Phase 1: Create tree and populate with data
    {
        let tree = bf_tree::wasm::open_tree_with_opfs(&db_name, (cache_size_mb as usize) * 1024 * 1024).await?;
        for i in 0..count {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            tree.insert(key.as_bytes(), value.as_bytes());
        }
        // Tree is dropped here, closing the file
    }

    // Phase 2: Open a fresh tree instance and read (cold reads)
    let tree = bf_tree::wasm::open_tree_with_opfs(&db_name, (cache_size_mb as usize) * 1024 * 1024).await?;

    let perf = get_performance();

    let start = perf.now();
    for i in 0..count {
        // Pseudo-random access pattern
        let idx = (i.wrapping_mul(7919)) % count;
        let key = format!("key_{:08}", idx);
        let _ = tree.read(key.as_bytes());
    }
    let elapsed = perf.now() - start;

    Ok(BenchmarkResult {
        operation: "opfs_cold_read".to_string(),
        count,
        total_ms: elapsed,
        ops_per_sec: (count as f64 / elapsed) * 1000.0,
    })
}
