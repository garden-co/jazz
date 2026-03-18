pub mod binding_support;
pub mod commit;
pub mod metadata;
#[cfg(feature = "cli")]
pub mod middleware;
pub mod object;
pub mod object_manager;
pub mod query_manager;
#[cfg(feature = "cli")]
pub mod routes;
pub mod runtime_core;
pub mod schema_manager;
#[cfg(feature = "cli")]
pub mod server;
pub mod storage;
pub mod sync_manager;
pub mod wire_types;

#[cfg(feature = "runtime-tokio")]
pub mod runtime_tokio;
#[cfg(feature = "runtime-tokio")]
pub use runtime_tokio as jazz_tokio;

#[cfg(feature = "transport")]
pub mod transport_protocol;
#[cfg(feature = "transport")]
pub use transport_protocol as jazz_transport;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
mod transport;

#[cfg(feature = "client")]
use std::path::PathBuf;

#[cfg(feature = "client")]
use thiserror::Error;

#[cfg(feature = "client")]
pub use client::{JazzClient, SessionClient};

#[cfg(all(feature = "client", feature = "transport"))]
pub use jazz_transport::ServerEvent;
#[cfg(feature = "client")]
pub use object::ObjectId;
#[cfg(feature = "client")]
pub use query_manager::query::{Query, QueryBuilder};
#[cfg(feature = "client")]
pub use query_manager::session::Session;
#[cfg(feature = "client")]
pub use query_manager::types::{
    ColumnType, OrderedRowDelta, Row, RowDelta, Schema, SchemaBuilder, TableName, TableSchema,
    Value,
};
#[cfg(feature = "client")]
pub use schema_manager::AppId;
#[cfg(feature = "client")]
pub use sync_manager::ClientId;
#[cfg(feature = "client")]
pub use sync_manager::DurabilityTier;
#[cfg(feature = "client")]
pub use sync_manager::ServerId;

/// Configuration for connecting to Jazz.
#[cfg(feature = "client")]
#[derive(Debug, Clone)]
pub struct AppContext {
    /// Application ID.
    pub app_id: AppId,
    /// Client ID (generated if not provided).
    pub client_id: Option<ClientId>,
    /// Schema for this client.
    pub schema: Schema,
    /// Server URL for sync (e.g., "http://localhost:1625").
    pub server_url: String,
    /// Local data directory for Fjall storage.
    pub data_dir: PathBuf,

    // Authentication fields
    /// JWT token for frontend authentication.
    /// Sent as `Authorization: Bearer <token>`.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    /// Enables `for_session()` to act as any user.
    pub backend_secret: Option<String>,
    /// Admin secret for schema/policy sync.
    /// Required to sync catalogue objects.
    pub admin_secret: Option<String>,
}

/// Errors from Jazz client operations.
#[cfg(feature = "client")]
#[derive(Error, Debug)]
pub enum JazzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel closed")]
    ChannelClosed,
}

/// Result type for Jazz operations.
#[cfg(feature = "client")]
pub type Result<T> = std::result::Result<T, JazzError>;

/// Handle to a subscription.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

/// Stream of row deltas from a subscription.
#[cfg(feature = "client")]
pub struct SubscriptionStream {
    receiver: tokio::sync::mpsc::Receiver<OrderedRowDelta>,
}

#[cfg(feature = "client")]
impl SubscriptionStream {
    /// Create a new subscription stream.
    pub(crate) fn new(receiver: tokio::sync::mpsc::Receiver<OrderedRowDelta>) -> Self {
        Self { receiver }
    }

    /// Get the next delta, waiting if necessary.
    pub async fn next(&mut self) -> Option<OrderedRowDelta> {
        self.receiver.recv().await
    }
}
