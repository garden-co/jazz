// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#![doc = include_str!("../README.md")]
#![allow(unexpected_cfgs, unused_imports, clippy::derivable_impls)]
pub mod circular_buffer;
mod config;
mod error;
mod fs;
mod nodes;
mod wal;

#[cfg(any(
    feature = "metrics",
    feature = "metrics-rt",
    feature = "metrics-rt-debug-all",
    feature = "metrics-rt-debug-timer"
))]
pub mod metric;

mod mini_page_op;
mod range_scan;
mod storage;
pub(crate) mod sync;
#[cfg(test)]
mod tests;

mod snapshot;
mod tree;
mod utils;

pub use config::{Config, StorageBackend, WalConfig};
pub use error::ConfigError;
pub use nodes::leaf_node::LeafReadResult;
pub use range_scan::{ScanIter, ScanReturnField};
pub use tree::{BfTree, LeafInsertResult, ScanIterError};

#[cfg(not(target_arch = "wasm32"))]
pub use wal::WalReader;

// Re-export OPFS VFS for WASM users who need direct access
#[cfg(target_arch = "wasm32")]
pub use fs::OpfsVfs;

/// WASM-specific module for creating BfTree instances backed by OPFS.
///
/// This module provides async functions for initializing BfTree with OPFS storage,
/// which is the recommended persistent storage backend for browser environments.
///
/// # Requirements
///
/// - Must run in a **Dedicated Web Worker** (OPFS sync API is Worker-only)
/// - Requires **HTTPS** (secure context needed for OPFS)
/// - Build with: `RUSTFLAGS=--cfg=web_sys_unstable_apis`
///
/// # Example
///
/// ```ignore
/// use bf_tree::wasm;
///
/// // In a Web Worker context
/// async fn init_tree() -> Result<bf_tree::BfTree, wasm_bindgen::JsValue> {
///     let tree = wasm::open_tree_with_opfs("my_database.db", 1024 * 1024 * 32).await?;
///     Ok(tree)
/// }
/// ```
#[cfg(target_arch = "wasm32")]
pub mod wasm {
    use wasm_bindgen::prelude::*;

    use crate::fs::OpfsVfs;
    use crate::nodes::leaf_node::LeafReadResult;
    use crate::tree::BfTree as BfTreeInner;

    /// WASM-friendly wrapper around BfTree.
    ///
    /// This wrapper provides JavaScript-compatible methods for interacting
    /// with the underlying BfTree instance.
    #[wasm_bindgen]
    pub struct BfTree {
        inner: BfTreeInner,
    }

    #[wasm_bindgen]
    impl BfTree {
        /// Insert a key-value pair into the tree.
        ///
        /// If the key already exists, the value will be overwritten.
        pub fn insert(&self, key: &[u8], value: &[u8]) -> bool {
            matches!(
                self.inner.insert(key, value),
                crate::tree::LeafInsertResult::Success
            )
        }

        /// Read the value for a given key.
        ///
        /// Returns the value as a byte array, or null if not found or deleted.
        pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
            let mut buf = vec![0u8; 65536]; // Max value size
            match self.inner.read(key, &mut buf) {
                LeafReadResult::Found(len) => {
                    buf.truncate(len as usize);
                    Some(buf)
                }
                _ => None,
            }
        }

        /// Read the value for a given key into a provided buffer.
        ///
        /// Returns the number of bytes read, or -1 if not found, -2 if deleted.
        pub fn read_into(&self, key: &[u8], buf: &mut [u8]) -> i32 {
            match self.inner.read(key, buf) {
                LeafReadResult::Found(len) => len as i32,
                LeafReadResult::NotFound => -1,
                LeafReadResult::Deleted => -2,
                LeafReadResult::InvalidKey => -3,
            }
        }

        /// Delete a key from the tree.
        pub fn delete(&self, key: &[u8]) {
            self.inner.delete(key);
        }

        /// Take a snapshot of the current tree state.
        ///
        /// Flushes all in-memory data to the VFS, writing the tree metadata,
        /// inner nodes, and leaf page mappings. After snapshot, the tree can
        /// be recovered from the VFS file alone.
        pub fn snapshot(&self) {
            self.inner.snapshot();
        }

        /// Flush the WAL buffer to ensure all buffered writes are persisted.
        pub fn flush_wal(&self) {
            self.inner.flush_wal();
        }

        /// Scan keys in the range [start_key, end_key).
        ///
        /// Returns a list of (key, value) pairs as a flat Vec of Vec<u8> pairs.
        /// Results are returned as [key1, value1, key2, value2, ...].
        pub fn scan_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<js_sys::Uint8Array> {
            let scan_result = self.inner.scan_with_end_key(
                start_key,
                end_key,
                crate::ScanReturnField::KeyAndValue,
            );
            let mut scan_iter = match scan_result {
                Ok(iter) => iter,
                Err(_) => return Vec::new(),
            };
            let mut results = Vec::new();
            let mut buf = vec![0u8; 65536];
            while let Some((key_len, val_len)) = scan_iter.next(&mut buf) {
                let key = js_sys::Uint8Array::from(&buf[..key_len]);
                let value = js_sys::Uint8Array::from(&buf[key_len..key_len + val_len]);
                results.push(key);
                results.push(value);
            }
            results
        }
    }

    /// Create a BfTree backed by OPFS.
    ///
    /// This is the primary way to create a persistent BfTree in browser environments.
    /// The tree will be stored in the Origin Private File System, which provides
    /// fast synchronous access within Web Workers.
    ///
    /// # Arguments
    ///
    /// * `db_name` - The filename to use in OPFS (e.g., "my_database.db")
    /// * `cache_size_byte` - Size of the in-memory cache in bytes
    ///
    /// # Returns
    ///
    /// Returns a `BfTree` instance on success, or a `JsValue` error on failure.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Not running in a Web Worker context
    /// - OPFS is not available (not a secure context)
    /// - File operations fail
    #[wasm_bindgen]
    pub async fn open_tree_with_opfs(
        db_name: &str,
        cache_size_byte: usize,
    ) -> Result<BfTree, JsValue> {
        // Initialize OPFS VFS asynchronously
        let opfs_vfs = OpfsVfs::open(db_name).await?;

        // Create BfTree with the OPFS VFS
        let mut config = crate::Config::default();
        config.cb_size_byte(cache_size_byte);
        let inner = BfTreeInner::with_opfs_vfs(opfs_vfs, config)
            .map_err(|e| JsValue::from_str(&format!("Config error: {:?}", e)))?;

        Ok(BfTree { inner })
    }

    /// Create a persistent BfTree backed by OPFS with snapshot + WAL recovery.
    ///
    /// This opens two OPFS files: `{db_name}.bftree` for the tree data and
    /// `{db_name}.wal` for the write-ahead log. On open:
    /// - If a previous snapshot exists, it is loaded
    /// - If WAL entries exist, they are replayed
    /// - A fresh WAL is started for ongoing writes
    ///
    /// This is the recommended way to create a persistent BfTree that survives
    /// Worker termination.
    #[wasm_bindgen]
    pub async fn open_tree_with_opfs_persistent(
        db_name: &str,
        cache_size_byte: usize,
    ) -> Result<BfTree, JsValue> {
        let tree_name = format!("{}.bftree", db_name);
        let wal_name = format!("{}.wal", db_name);

        let tree_vfs = OpfsVfs::open(&tree_name).await?;
        let wal_vfs = OpfsVfs::open(&wal_name).await?;

        let mut config = crate::Config::default();
        config.cb_size_byte(cache_size_byte);

        let inner = BfTreeInner::open_with_opfs(tree_vfs, wal_vfs, config)
            .map_err(|e| JsValue::from_str(&format!("Config error: {:?}", e)))?;

        Ok(BfTree { inner })
    }

    /// Create a BfTree with in-memory storage only (no persistence).
    ///
    /// This is useful for testing or temporary data that doesn't need
    /// to survive page reloads.
    ///
    /// # Arguments
    ///
    /// * `cache_size_byte` - Size of the in-memory cache in bytes
    #[wasm_bindgen]
    pub fn create_memory_tree(cache_size_byte: usize) -> Result<BfTree, JsValue> {
        let inner = BfTreeInner::new(":memory:", cache_size_byte)
            .map_err(|e| JsValue::from_str(&format!("Config error: {:?}", e)))?;
        Ok(BfTree { inner })
    }
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        #[cfg(all(feature = "tracing", debug_assertions))]
        {
            tracing::info!($($arg)*);
        }

        #[cfg(not(all(feature = "tracing", debug_assertions)))]
        {
        }
    };
}

#[macro_export]
macro_rules! counter {
    ($event:ident) => {
        #[cfg(feature = "metrics-rt")]
        {
            $crate::metric::get_tls_recorder()
                .increment_counter($crate::metric::Counter::$event, 1);
        }
    };
    ($event:ident, $value:literal) => {
        #[cfg(feature = "metrics-rt")]
        {
            $crate::metric::get_tls_recorder()
                .increment_counter($crate::metric::Counter::$event, $value);
        }
    };
}

#[macro_export]
macro_rules! histogram {
    ($event:ident, $value:expr) => {
        #[cfg(feature = "metrics-rt")]
        {
            $crate::metric::get_tls_recorder()
                .hit_histogram($crate::metric::Histogram::$event, $value);
        }
    };
}

#[macro_export]
macro_rules! timer {
    ($event:expr) => {
        let _timer_guard = if cfg!(feature = "metrics-rt") {
            Some($crate::metric::get_tls_recorder().timer_guard($event))
        } else {
            None
        };
    };
}

#[macro_export]
macro_rules! check_parent {
    ($self:ident, $node:expr, $parent:expr) => {
        if let Some(ref p) = $parent {
            p.check_version()?;
        } else if $node != $self.get_root_page().0 {
            return Err(TreeError::Locked);
        }
    };
}
