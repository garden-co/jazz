#[doc(hidden)]
pub mod admin_catalogue_row_format;
pub mod app_id;
pub mod identity;
pub mod metadata;
/// Target-shell factory boundary for native peer transports.
pub mod native_transport_connector;
mod object;
pub mod policy_claims;
pub(crate) mod public_api;
pub mod public_schema;
#[doc(hidden)]
pub mod public_schema_convert;
pub mod schema_lens;
pub mod sync;
#[cfg(feature = "testing")]
pub mod test_support;
pub mod transaction;

pub mod transport_error;
pub mod websocket_prelude_auth;

#[cfg(feature = "runtime")]
#[allow(clippy::await_holding_refcell_ref)]
mod client;

#[cfg(feature = "runtime")]
use std::path::PathBuf;
#[cfg(feature = "runtime")]
use std::sync::Arc;

#[cfg(feature = "runtime")]
use thiserror::Error;

pub use app_id::AppId;
pub use public_schema::{
    AuthMode, CmpOp, ColumnDescriptor, ColumnMergeStrategy, ColumnType, Operation, OrderedRowDelta,
    PolicyExpr, PolicyValue, QueryResult, QueryResultField, Row, RowDelta, RowDescriptor, Schema,
    SchemaBuilder, SchemaHash, Session, TableName, TablePolicies, TableSchema, TableSchemaBuilder,
    TransactionId, Value, WriteContext, permissions, policy_expr,
};
pub use schema_lens::{Direction, Lens, LensOp, LensTransform};
pub use transaction::OpenTransactionId;

#[cfg(feature = "runtime")]
pub use client::{JazzClient, JazzTransaction};

pub(crate) use object::OutputOccurrenceId;
pub use object::{BranchName, ObjectId, ResultKey};
#[cfg(feature = "runtime")]
pub use sync::ClientId;
#[cfg(feature = "runtime")]
pub use sync::{DurabilityTier, ReadTier};

/// Configuration for connecting to Jazz.
#[cfg(feature = "runtime")]
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
    /// Target-shell factory for persistent local storage.
    pub storage_factory: Option<Arc<dyn crate::groove::storage::StorageFactory>>,

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

#[cfg(feature = "testing")]
impl AppContext {
    pub fn test(schema: Schema) -> AppContext {
        AppContext {
            app_id: crate::tools::AppId::random(),
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir: std::env::temp_dir(),
            storage: crate::tools::ClientStorage::Memory,
            storage_factory: None,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        }
    }
}

/// Local storage backend for a client application.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientStorage {
    /// Persist client state to disk under `AppContext::data_dir`.
    #[default]
    Persistent,
    /// Keep all client state in memory for the lifetime of the process only.
    Memory,
}

/// Errors from Jazz client operations.
#[cfg(feature = "runtime")]
#[derive(Error, Debug)]
pub enum JazzError {
    #[error("Identity error: {0}")]
    Identity(#[from] crate::ids::AuthorSubjectError),

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
#[cfg(feature = "runtime")]
pub type Result<T> = std::result::Result<T, JazzError>;

/// Handle to a subscription.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

/// Reason a subscription stream was rejected by a serving peer.
#[cfg(feature = "runtime")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionRejectReason {
    /// The serving peer cannot currently maintain this query shape/read view.
    UnsupportedShapeCapability {
        /// Human-readable diagnostic. Not part of semantic compatibility.
        detail: String,
    },
    /// The shape is valid, but its schema has not yet reached the serving runtime.
    ShapeRegistrationPendingCatalogueAdmission,
    /// The serving peer failed without exposing internal diagnostic detail.
    ServerFailure {
        /// Stable, client-safe server failure classification.
        code: SubscriptionServerFailureCode,
    },
}

/// Client-safe server failure classifications for subscriptions.
#[cfg(feature = "runtime")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionServerFailureCode {
    /// The requested table was absent on the serving peer.
    TableNotFound,
    /// The server could not resolve the requested schema or shape.
    SchemaResolution,
    /// The server rejected the query during validation.
    QueryValidation,
    /// The server could not lower the query.
    QueryLowering,
    /// The server could not evaluate the subscription policy.
    PolicyEvaluation,
    /// Another server-side failure occurred.
    Internal,
}

/// Item yielded by a public subscription stream.
#[cfg(feature = "runtime")]
#[derive(Clone, Debug)]
pub enum SubscriptionStreamItem {
    /// Incremental or reset row delta.
    Delta(OrderedRowDelta),
    /// The serving peer rejected the propagated upstream subscription.
    Rejected {
        /// Stable rejection class plus diagnostic detail.
        reason: SubscriptionRejectReason,
    },
}

/// Stream of row deltas from a subscription.
#[cfg(feature = "runtime")]
pub struct SubscriptionStream {
    receiver: tokio::sync::mpsc::UnboundedReceiver<SubscriptionStreamItem>,
    /// Signals the facade's forwarding task to drop its core subscription when
    /// the public stream is dropped. Closing `receiver` alone cannot do that
    /// for an otherwise idle core subscription.
    cancellation: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(feature = "runtime")]
impl SubscriptionStream {
    /// Create a new subscription stream.
    #[allow(dead_code)]
    pub(crate) fn new(
        receiver: tokio::sync::mpsc::UnboundedReceiver<SubscriptionStreamItem>,
        cancellation: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            receiver,
            cancellation: Some(cancellation),
        }
    }

    /// Get the next subscription item, waiting if necessary.
    pub async fn next(&mut self) -> Option<SubscriptionStreamItem> {
        self.receiver.recv().await
    }
}

#[cfg(feature = "runtime")]
impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        // Wake the forwarding task even when no core event is pending;
        // dropping its core stream then runs subscription cleanup.
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.send(());
        }
    }
}
