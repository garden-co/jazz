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

                // Note: In a real test, you'd want to close and reopen to verify persistence
            }
            Err(e) => {
                // OPFS might not be available in all test environments
                web_sys::console::log_1(&format!("OPFS not available: {:?}", e).into());
            }
        }
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
