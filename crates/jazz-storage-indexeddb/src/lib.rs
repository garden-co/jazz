//! IndexedDB-backed ordered storage.
//!
//! IndexedDB supplies only opaque atomic page commits. `opfs-btree` retains
//! ownership of key ordering, scans, caching, and page layout.

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(target_arch = "wasm32")]
pub use wasm::IndexedDbOrderedStorage;
