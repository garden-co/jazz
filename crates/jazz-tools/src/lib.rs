pub mod app_id;
#[cfg(any(feature = "server", test))]
pub(crate) mod catalogue_payload_codec;
pub mod commit;
pub mod digest;
pub mod identity;
pub mod metadata;
#[cfg(any(feature = "cli", feature = "server"))]
pub mod middleware;
pub mod object;
#[cfg(feature = "otel-core")]
pub mod otel;
#[allow(dead_code, unused_imports, clippy::wrong_self_convention)]
pub(crate) mod query_api;
pub mod row_format;
pub mod schema_api;
pub mod schema_lens;
#[cfg(any(feature = "cli", feature = "server"))]
pub mod server;
pub mod sync;
#[cfg(feature = "test-utils")]
pub mod test_support;
pub mod transaction;

pub mod transport_auth;
pub mod transport_error;

#[cfg(feature = "client")]
#[allow(clippy::await_holding_refcell_ref)]
mod client;

#[cfg(feature = "client")]
use std::path::PathBuf;

#[cfg(feature = "client")]
use thiserror::Error;

pub use app_id::AppId;
pub use schema_api::{
    AuthMode, BatchId, ColumnDescriptor, ColumnMergeStrategy, ColumnType, Operation,
    OrderedRowDelta, PolicyExpr, Query, QueryBuilder, Row, RowDelta, RowDescriptor, Schema,
    SchemaBuilder, SchemaHash, Session, TableName, TablePolicies, TableSchema, Value, WriteContext,
    permissions, policy_expr,
};
pub use schema_lens::{Direction, Lens, LensOp, LensTransform};

#[cfg(feature = "client")]
pub use client::{JazzClient, JazzTransaction};

pub use object::ObjectId;
#[cfg(feature = "client")]
pub use sync::ClientId;
#[cfg(feature = "client")]
pub use sync::DurabilityTier;

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
    /// Local data directory for persistent storage.
    pub data_dir: PathBuf,
    /// Local storage backend.
    pub storage: ClientStorage,

    // Authentication fields
    /// JWT token for frontend authentication.
    /// Sent as `Authorization: Bearer <token>`.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    /// Enables `for_session()` to act as any user.
    pub backend_secret: Option<String>,
    /// Admin secret for privileged sync over WebSocket and `/admin/*` HTTP.
    /// On `/ws`, a valid admin secret authenticates this client as the backend.
    pub admin_secret: Option<String>,
}

#[cfg(feature = "test-utils")]
impl AppContext {
    pub fn test(schema: Schema) -> AppContext {
        AppContext {
            app_id: crate::AppId::random(),
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir: std::env::temp_dir(),
            storage: crate::ClientStorage::Memory,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        }
    }
}

/// Local storage backend for a client application.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientStorage {
    /// Persist client state to disk under `AppContext::data_dir`.
    #[default]
    Persistent,
    /// Keep all client state in memory for the lifetime of the process only.
    Memory,
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
    receiver: tokio::sync::mpsc::UnboundedReceiver<OrderedRowDelta>,
}

#[cfg(feature = "client")]
impl SubscriptionStream {
    /// Create a new subscription stream.
    #[allow(dead_code)]
    pub(crate) fn new(receiver: tokio::sync::mpsc::UnboundedReceiver<OrderedRowDelta>) -> Self {
        Self { receiver }
    }

    /// Get the next delta, waiting if necessary.
    pub async fn next(&mut self) -> Option<OrderedRowDelta> {
        self.receiver.recv().await
    }
}
