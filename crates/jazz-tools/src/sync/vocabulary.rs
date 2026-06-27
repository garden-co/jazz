//! Shared sync protocol vocabulary.
//!
//! This module is the narrow home for peer endpoints and wire payloads used by
//! sync-adjacent tooling.
//!
pub use crate::sync::types::{
    ConnectionSchemaDiagnostics, Destination, QueryId, QueryPropagation, SchemaWarning, Source,
    SyncError,
};
