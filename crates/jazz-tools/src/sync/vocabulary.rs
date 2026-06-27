//! Shared sync protocol vocabulary.
//!
//! This module is the narrow home for peer endpoints and wire payloads used by
//! sync-adjacent tooling.
//!
//! `SyncPayload` is legacy test vocabulary from the retired alpha fanout hub.
//! Product sync should use direct websocket/core events instead.

#[cfg(any(test, feature = "test-utils"))]
pub use crate::sync::types::SyncPayload;
pub use crate::sync::types::{
    ConnectionSchemaDiagnostics, Destination, QueryId, QueryPropagation, SchemaWarning, Source,
    SyncError,
};
