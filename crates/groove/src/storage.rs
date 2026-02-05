//! Storage types and errors.
//!
//! This module contains types used by the IoHandler trait for storage operations.
//! The actual storage implementation is in `io_handler.rs`.

use serde::{Deserialize, Serialize};

/// BLAKE3 hash of blob content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    NotFound,
    IoError(String),
}

/// Legacy storage request type used by BTreeIndex page-based storage.
///
/// TODO: Remove when BTreeIndex is replaced by IoHandler-based indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageRequest {
    LoadIndexMeta {
        table: String,
        column: String,
    },
    StoreIndexMeta {
        table: String,
        column: String,
        data: Vec<u8>,
    },
    LoadIndexPage {
        table: String,
        column: String,
        page_id: u64,
    },
    StoreIndexPage {
        table: String,
        column: String,
        page_id: u64,
        data: Vec<u8>,
    },
    DeleteIndexPage {
        table: String,
        column: String,
        page_id: u64,
    },
}
