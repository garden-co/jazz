#![doc = include_str!("../README.md")]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

/// IndexedDB VFS implementation with relaxed durability guarantees.
pub mod relaxed_idb;

/// Origin Private File System (OPFS) VFS implementation using `SyncAccessHandle`.
pub mod sahpool;

#[cfg(test)]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);
