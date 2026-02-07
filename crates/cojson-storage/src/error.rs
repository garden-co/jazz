//! Error types for storage operations.

use std::fmt;

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors that can occur during storage operations.
#[derive(Debug)]
pub enum StorageError {
    /// The requested CoValue was not found.
    NotFound(String),

    /// A database/storage operation failed.
    Database(String),

    /// Serialization or deserialization failed.
    Serialization(String),

    /// An I/O operation failed.
    Io(std::io::Error),

    /// A transaction was aborted or rolled back.
    TransactionAborted(String),

    /// The storage is in an invalid state.
    InvalidState(String),

    /// A constraint was violated (e.g., duplicate key).
    ConstraintViolation(String),

    /// The operation is not supported by this backend.
    Unsupported(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound(id) => write!(f, "CoValue not found: {}", id),
            StorageError::Database(msg) => write!(f, "Database error: {}", msg),
            StorageError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            StorageError::Io(err) => write!(f, "I/O error: {}", err),
            StorageError::TransactionAborted(msg) => write!(f, "Transaction aborted: {}", msg),
            StorageError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            StorageError::ConstraintViolation(msg) => write!(f, "Constraint violation: {}", msg),
            StorageError::Unsupported(msg) => write!(f, "Unsupported operation: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

#[cfg(feature = "serde")]
impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}
