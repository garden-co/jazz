//! Shared sync protocol vocabulary.
//!
//! This module is the narrow home for peer endpoints and wire payloads used by
//! sync-adjacent tooling, so callers do not need to couple to `sync_manager`.

pub use crate::sync::types::{
    ConnectionSchemaDiagnostics, Destination, QueryId, QueryPropagation, RowMetadata,
    SchemaWarning, Source, SyncError, SyncPayload,
};
