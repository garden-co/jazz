// The data-model and identity modules below moved out of `tools` so the
// lower layers no longer reach up into the public API module; they stay
// reachable at their old `tools::` paths.
#[doc(hidden)]
pub use crate::model::admin_catalogue_row_format;
pub(crate) use crate::model::public_api;
#[doc(hidden)]
pub use crate::model::public_schema_convert;
pub use crate::model::{metadata, policy_claims, public_schema, transaction};
pub use crate::{app_id, identity};
#[doc(hidden)]
pub mod deployment;
/// Target-shell factory boundary for native peer transports.
pub mod native_transport_connector;
pub mod schema_lens;
pub mod sync;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub mod transport_error;
pub mod websocket_prelude_auth;

#[cfg(feature = "runtime")]
#[allow(clippy::await_holding_refcell_ref)]
mod client;

#[cfg(feature = "runtime")]
use std::path::PathBuf;

#[cfg(feature = "runtime")]
use thiserror::Error;

pub use app_id::AppId;
pub use public_api::session::unverified_jwt_scope_subject;
pub use public_schema::{
    AuthMode, CmpOp, ColumnDescriptor, ColumnMergeStrategy, ColumnType, HistoryRetention,
    Operation, OrderedRowDelta, PolicyExpr, PolicyValue, QueryResult, QueryResultField, Row,
    RowDelta, RowDescriptor, Schema, SchemaBuilder, SchemaHash, Session, TableName, TablePolicies,
    TableSchema, TableSchemaBuilder, TransactionId, Value, WriteContext, permissions, policy_expr,
};
pub use schema_lens::{Direction, Lens, LensOp, LensTransform};
pub use transaction::OpenTransactionId;

#[cfg(feature = "runtime")]
pub use client::{
    JazzClient, JazzTransaction, MutationError, PermissionAdviceRequest, StreamingUpload,
};

pub use crate::object::{BranchName, ObjectId, ResultKey};
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

    // Authentication fields
    /// Registry-resolved account metadata for this low-level native client.
    /// Never read from JWT claims. The server independently checks the bearer
    /// against this assignment; a copied account ID grants no authority.
    pub account_id: Option<crate::account_registry::AccountId>,
    /// JWT token for frontend authentication.
    /// Sent as `Authorization: Bearer <token>`.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    /// Enables `for_session()` to act as any user.
    pub backend_secret: Option<String>,
    /// With `backend_secret`: the user this client acts for, as the app's
    /// backend states it (the way `createJazz` relays its users' clients).
    /// The client is then that user's session, not the backend's own link.
    pub backend_session: Option<Session>,
    /// With `backend_session`: the published permissions bundle (its hex
    /// id) that session is pinned to, as `createJazz` pins every session it
    /// states. A core governs a stated session only by its bundle, and
    /// refuses one without.
    pub bundle: Option<String>,
    /// Admin secret for privileged sync over WebSocket and `/admin/*` HTTP.
    /// On `/ws`, a valid admin secret authenticates this client as the backend.
    pub admin_secret: Option<String>,
    /// The app's schema versions and the lenses between them: what
    /// [`deployment::artifact::load_client`] reads from `jazz.app.json`, or
    /// [`deployment::published::lowered_client_migrations`] lowers from the
    /// compiled schema generated bindings embed. The store reaches `schema`
    /// along them, so it stores each version as the server does (SPEC 10.7).
    pub migrations: crate::db::Migrations,
}

#[cfg(feature = "testing")]
impl AppContext {
    pub fn test(schema: Schema) -> AppContext {
        AppContext {
            app_id: crate::app_id::AppId::random(),
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir: std::env::temp_dir(),
            storage: crate::tools::ClientStorage::Memory,
            account_id: None,
            jwt_token: None,
            backend_secret: None,
            backend_session: None,
            bundle: None,
            admin_secret: None,
            migrations: Default::default(),
        }
    }
}

/// Local storage backend for a client application.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientStorage {
    /// Persist client state to `jazz.sqlite` under `AppContext::data_dir`.
    #[default]
    Persistent,
    /// Keep all client state in memory for the lifetime of the process only.
    Memory,
}

/// Errors from Jazz client operations. More may come: match it with a
/// wildcard arm.
#[cfg(feature = "runtime")]
#[derive(Error, Debug)]
#[non_exhaustive]
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

    /// The authority rejected a write: a durable wait, a transaction's or
    /// [`JazzClient::wait_for_sync`]'s. `code` and `reason` are TS's
    /// (`PersistedWriteRejectedError`): `code` is `permission_denied`,
    /// `serialization_conflict`, `transaction_conflict`,
    /// `client_clock_too_far_ahead` or `write_rejected`.
    #[error("Write rejected ({code}): {reason}")]
    #[non_exhaustive]
    WriteRejected { code: String, reason: String },

    #[error("Storage error: {0}")]
    Storage(String),

    /// The data directory holds a store from before the SQLite engine.
    /// It is left untouched; delete it and resync from Core. Writes in it
    /// that never reached Core are lost.
    #[error("Unsupported storage format: {0}")]
    UnsupportedStorage(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel closed")]
    ChannelClosed,
}

/// How a durable wait's error for a refused transaction begins.
#[cfg(feature = "runtime")]
pub(crate) const REJECTED_BEFORE: &str = "transaction was rejected before reaching";

#[cfg(feature = "runtime")]
impl JazzError {
    /// Whether this is [`JazzClient::wait_for_transaction`]'s error for a
    /// transaction the server refused, rather than a wait that failed for
    /// another reason. For `jazz-app-support`'s mutation-error reporting.
    #[doc(hidden)]
    pub fn is_transaction_rejection(&self) -> bool {
        matches!(self, Self::WriteRejected { .. })
            || matches!(self, Self::Sync(message) | Self::Write(message) if message.starts_with(REJECTED_BEFORE))
    }

    /// The rejection of a write the core reported as `error`, if it is one.
    pub(crate) fn write_rejected(error: &crate::db::Error) -> Option<Self> {
        let (code, reason) = rejection_code_and_reason(error)?;
        Some(Self::WriteRejected { code, reason })
    }
}

/// TS's code and reason for a write the core rejected with `error`
/// (`native-runtime-adapter`'s `rejectionCode` and `rejectionReason`), or
/// `None` when `error` is no rejection.
#[cfg(feature = "runtime")]
pub(crate) fn rejection_code_and_reason(error: &crate::db::Error) -> Option<(String, String)> {
    use crate::db::{ErrorCode, Reason};
    match error.code {
        // TS keeps the whole diagnostic when the message is empty.
        ErrorCode::TransactionConflict if error.message.is_empty() => {
            Some(("transaction_conflict".to_owned(), error.to_string()))
        }
        ErrorCode::TransactionConflict => {
            Some(("transaction_conflict".to_owned(), error.message.clone()))
        }
        ErrorCode::WriteRejected => {
            let (code, readable) = match error.reason {
                Some(Reason::AuthorizationDenied) => (
                    "permission_denied",
                    Some("Write rejected by server authorization"),
                ),
                Some(Reason::SerializationConflict) => (
                    "serialization_conflict",
                    Some("Serializable transaction conflicted with another write"),
                ),
                Some(Reason::ClientClockTooFarAhead) => (
                    "client_clock_too_far_ahead",
                    Some("Client clock is too far ahead"),
                ),
                _ => ("write_rejected", None),
            };
            let reason = match readable {
                Some(readable) => readable.to_owned(),
                None if error.message.is_empty() => "Write rejected".to_owned(),
                None => error.message.clone(),
            };
            Some((code.to_owned(), reason))
        }
        _ => None,
    }
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
    /// The local receiver rejected an impossible authority source-closure
    /// transition before publishing it.
    InvalidAuthoritySourceClosure {
        /// Client-safe transition diagnosis with no row or policy data.
        transition: String,
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
    /// Bounded: a reader that falls behind leaves the rest in the core,
    /// which coalesces it into one reset (`OrderedRowDelta::coalesced`).
    receiver: tokio::sync::mpsc::Receiver<SubscriptionStreamItem>,
    /// Asks the forwarder for more once the reader made room.
    on_read: Box<dyn Fn()>,
    /// Signals the facade's forwarding task to drop its core subscription when
    /// the public stream is dropped. Closing `receiver` alone cannot do that
    /// for an otherwise idle core subscription.
    cancellation: Option<tokio::sync::oneshot::Sender<()>>,
    /// Drops the core subscription now, after `cancellation` marked it,
    /// so its unsubscription is queued before whatever the caller does next.
    on_drop: Option<Box<dyn FnOnce()>>,
}

#[cfg(feature = "runtime")]
impl SubscriptionStream {
    /// Create a new subscription stream.
    #[allow(dead_code)]
    pub(crate) fn new(
        receiver: tokio::sync::mpsc::Receiver<SubscriptionStreamItem>,
        cancellation: tokio::sync::oneshot::Sender<()>,
        on_drop: Box<dyn FnOnce()>,
        on_read: Box<dyn Fn()>,
    ) -> Self {
        Self {
            receiver,
            on_read,
            cancellation: Some(cancellation),
            on_drop: Some(on_drop),
        }
    }

    /// Get the next subscription item, waiting if necessary.
    pub async fn next(&mut self) -> Option<SubscriptionStreamItem> {
        let item = self.receiver.recv().await;
        (self.on_read)();
        item
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
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
    }
}

#[cfg(all(test, feature = "runtime"))]
mod rejection_code_tests {
    use super::rejection_code_and_reason;
    use crate::db::{Error, ErrorCode, Reason};

    fn rejected(reason: Option<Reason>, message: &str) -> Error {
        Error {
            code: ErrorCode::WriteRejected,
            reason,
            message: message.to_owned(),
            space: None,
        }
    }

    /// TS's `rejectionCode` and `rejectionReason`, case by case.
    #[test]
    fn rejections_carry_ts_codes_and_reasons() {
        let pair = rejection_code_and_reason;
        let owned = |code: &str, reason: &str| Some((code.to_owned(), reason.to_owned()));
        assert_eq!(
            pair(&rejected(
                Some(Reason::AuthorizationDenied),
                "authorization_denied: x"
            )),
            owned(
                "permission_denied",
                "Write rejected by server authorization"
            )
        );
        assert_eq!(
            pair(&rejected(Some(Reason::SerializationConflict), "x")),
            owned(
                "serialization_conflict",
                "Serializable transaction conflicted with another write"
            )
        );
        assert_eq!(
            pair(&rejected(Some(Reason::ClientClockTooFarAhead), "x")),
            owned(
                "client_clock_too_far_ahead",
                "Client clock is too far ahead"
            )
        );
        assert_eq!(
            pair(&rejected(Some(Reason::RowAlreadyExists), "row exists")),
            owned("write_rejected", "row exists")
        );
        assert_eq!(
            pair(&rejected(None, "")),
            owned("write_rejected", "Write rejected")
        );
        let conflict = Error {
            code: ErrorCode::TransactionConflict,
            reason: None,
            message: "read changed".to_owned(),
            space: None,
        };
        assert_eq!(
            pair(&conflict),
            owned("transaction_conflict", "read changed")
        );
        let other = Error {
            code: ErrorCode::Storage,
            reason: None,
            message: "disk".to_owned(),
            space: None,
        };
        assert_eq!(pair(&other), None);
    }
}
