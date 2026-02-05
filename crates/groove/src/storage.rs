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
