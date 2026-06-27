use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) mod clock;
#[cfg(any(test, feature = "test-utils"))]
pub mod sync_tracer;
pub(crate) mod types;
pub mod vocabulary;

/// Persistence tier — declaration order defines Ord (Local < EdgeServer < GlobalServer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum DurabilityTier {
    Local,
    EdgeServer,
    GlobalServer,
}

/// Unique identifier for a server connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServerId(pub Uuid);

impl ServerId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ServerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ServerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientId(pub Uuid);

impl ClientId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parse from UUID string.
    pub fn parse(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(ClientId)
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub use sync_tracer::SyncTracer;
pub use types::{
    ConnectionSchemaDiagnostics, Destination, QueryId, QueryPropagation, SchemaWarning, Source,
    SyncError,
};
