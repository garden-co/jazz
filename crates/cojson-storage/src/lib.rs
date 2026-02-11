//! Core storage abstractions for Jazz/CoJSON.
//!
//! This crate provides platform-agnostic storage traits and types that can be
//! implemented by various backends (BF-Tree, SQLite, IndexedDB, etc.).
//!
//! # Features
//!
//! - `async` - Enable async storage backend support via tokio
//! - `serde` - Enable serde serialization for storage types
//! - `bftree` - Enable BF-Tree storage backend (native platforms only)
//!
//! # Example
//!
//! ```rust,ignore
//! use cojson_storage::{StorageBackend, StorageTransaction};
//!
//! struct MyStorage { /* ... */ }
//!
//! impl StorageBackend for MyStorage {
//!     // Implement the trait methods
//! }
//! ```

pub mod error;
pub mod file_io;
pub mod traits;
pub mod types;

#[cfg(feature = "async")]
pub mod traits_async;

#[cfg(all(feature = "bftree", not(target_arch = "wasm32")))]
pub mod bftree;

pub use error::{StorageError, StorageResult};
pub use traits::{StorageBackend, StorageTransaction};
pub use types::*;

#[cfg(feature = "async")]
pub use traits_async::{StorageBackendAsync, StorageTransactionAsync};
