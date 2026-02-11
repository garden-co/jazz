// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#![doc = include_str!("../README.md")]
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

#[cfg(not(target_arch = "wasm32"))]
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
        let inner = BfTreeInner::with_opfs_vfs(opfs_vfs, cache_size_byte)
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
