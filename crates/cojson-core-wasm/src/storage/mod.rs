use bf_tree::{BfTree as BfTreeInner, LeafInsertResult, LeafReadResult, ScanReturnField};
use wasm_bindgen::prelude::*;

// Maximum buffer size for read/scan operations (64 KB)
const READ_BUFFER_SIZE: usize = 65536;

// ============================================================================
// BfTreeStore - WASM wrapper for bf-tree
// ============================================================================

/// WASM-friendly wrapper around BfTree for use as a storage engine.
///
/// Provides key-value operations (insert, read, delete) and prefix-based
/// range scans. Keys and values are byte slices.
#[wasm_bindgen]
pub struct BfTreeStore {
    inner: BfTreeInner,
}

#[wasm_bindgen]
impl BfTreeStore {
    /// Insert a key-value pair into the tree.
    ///
    /// If the key already exists, the value will be overwritten.
    /// Returns `true` on success, `false` if the key/value exceeds size limits.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> bool {
        matches!(self.inner.insert(key, value), LeafInsertResult::Success)
    }

    /// Read the value for a given key.
    ///
    /// Returns the value as a byte array, or `undefined` if not found or deleted.
    pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; READ_BUFFER_SIZE];
        match self.inner.read(key, &mut buf) {
            LeafReadResult::Found(len) => {
                buf.truncate(len as usize);
                Some(buf)
            }
            _ => None,
        }
    }

    /// Delete a key from the tree.
    ///
    /// The key is marked as deleted (tombstone). Subsequent reads will
    /// return `undefined`.
    pub fn delete(&self, key: &[u8]) {
        self.inner.delete(key);
    }

    /// Scan keys starting from `prefix`, returning up to `limit` key-value pairs.
    ///
    /// Returns a JavaScript `Array` of `[Uint8Array, Uint8Array]` pairs (key, value).
    /// The scan stops when:
    /// - A key is found that does not start with the given prefix
    /// - The limit is reached
    /// - There are no more keys
    ///
    /// This is used for prefix-based lookups (e.g., all sessions for a CoValue).
    pub fn scan(&self, prefix: &[u8], limit: u32) -> js_sys::Array {
        let results = js_sys::Array::new();

        if prefix.is_empty() || limit == 0 {
            return results;
        }

        // Use scan_with_count starting from the prefix key.
        // We request limit entries and filter by prefix match.
        let scan_result = self
            .inner
            .scan_with_count(prefix, limit as usize, ScanReturnField::KeyAndValue);

        let mut iter = match scan_result {
            Ok(iter) => iter,
            Err(_) => return results,
        };

        let mut buf = vec![0u8; READ_BUFFER_SIZE];

        while let Some((key_len, value_len)) = iter.next(&mut buf) {
            let key = &buf[..key_len];

            // Stop if the key no longer matches our prefix
            if !key.starts_with(prefix) {
                break;
            }

            let value = &buf[key_len..key_len + value_len];

            // Create a JS array pair [key, value]
            let pair = js_sys::Array::new_with_length(2);
            pair.set(0, js_sys::Uint8Array::from(key).into());
            pair.set(1, js_sys::Uint8Array::from(value).into());
            results.push(&pair);
        }

        results
    }
}

// ============================================================================
// Shared configuration
// ============================================================================

/// Build a Jazz-tuned bf-tree Config for the given cache size and backend.
///
/// Settings:
/// - Max key length: 256 bytes (Jazz composite keys ≈ 150 bytes)
/// - Max record size: 16,384 bytes
/// - Min record size: 8 bytes (satisfies leaf_page_size / min_record_size <= 4096)
/// - Leaf page size: 32,768 bytes
fn jazz_bftree_config(cache_size_bytes: usize, backend: bf_tree::StorageBackend) -> bf_tree::Config {
    let mut config = bf_tree::Config::default();
    config
        .cb_max_key_len(256)
        .cb_min_record_size(8)
        .cb_max_record_size(15360)
        .leaf_page_size(32768)
        .cb_size_byte(cache_size_bytes)
        .storage_backend(backend);
    config
}

// ============================================================================
// Factory functions
// ============================================================================

/// Open a BfTree backed by OPFS (Origin Private File System).
///
/// Must be called from a **Web Worker** context (OPFS sync access is Worker-only).
/// Requires HTTPS (secure context).
///
/// # Arguments
///
/// * `db_name` - The filename to use in OPFS (e.g., "jazz-storage.db")
/// * `cache_size_bytes` - Size of the in-memory cache in bytes (must be a power of two)
///
/// # Configuration
///
/// The tree is configured for Jazz's storage needs:
/// - Max key length: 256 bytes
/// - Max record size: 16,384 bytes
/// - Leaf page size: 32,768 bytes
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub async fn open_bftree_opfs(
    db_name: &str,
    cache_size_bytes: usize,
) -> Result<BfTreeStore, JsValue> {
    let opfs_vfs = bf_tree::OpfsVfs::open(db_name).await?;
    let config = jazz_bftree_config(cache_size_bytes, bf_tree::StorageBackend::Opfs);

    let inner = BfTreeInner::with_opfs_vfs_and_config(opfs_vfs, config)
        .map_err(|e| JsValue::from_str(&format!("BfTree config error: {:?}", e)))?;

    Ok(BfTreeStore { inner })
}

/// Create a BfTree with in-memory storage only (no persistence).
///
/// Useful for testing or temporary data that doesn't need to survive page reloads.
/// Uses the same Jazz-specific configuration as `open_bftree_opfs` (256-byte keys,
/// 16 KB records, 32 KB leaf pages).
///
/// # Arguments
///
/// * `cache_size_bytes` - Size of the in-memory cache in bytes (must be a power of two)
#[wasm_bindgen]
pub fn create_bftree_memory(cache_size_bytes: usize) -> Result<BfTreeStore, JsValue> {
    let config = jazz_bftree_config(cache_size_bytes, bf_tree::StorageBackend::Memory);

    let inner = BfTreeInner::with_config(config, None)
        .map_err(|e| JsValue::from_str(&format!("BfTree config error: {:?}", e)))?;
    Ok(BfTreeStore { inner })
}
