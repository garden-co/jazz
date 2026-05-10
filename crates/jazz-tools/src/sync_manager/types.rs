use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::batch_fate::{BatchFate, SealedBatchSubmission};
use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::SchemaHash;
use crate::row_histories::{BatchId, StoredRowBatch};

/// Error returned when a policy denies an operation.
#[derive(Debug, Clone)]
pub struct PolicyError {
    pub message: String,
}

// ============================================================================
// ID Types
// ============================================================================

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

/// Unique identifier for a query subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QueryPropagation {
    #[default]
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "local-only")]
    LocalOnly,
}

/// Unique identifier for a pending permission check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PendingUpdateId(pub u64);

/// Stable identity for one concrete row batch entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowBatchKey {
    pub row_id: ObjectId,
    pub branch_name: BranchName,
    pub batch_id: BatchId,
}

impl RowBatchKey {
    pub fn new(row_id: ObjectId, branch_name: BranchName, batch_id: BatchId) -> Self {
        Self {
            row_id,
            branch_name,
            batch_id,
        }
    }

    pub fn from_row(row: &StoredRowBatch) -> Self {
        Self::new(row.row_id, BranchName::new(&row.branch), row.batch_id)
    }
}

/// Deferred query settlement waiting for stream sequencing prerequisites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingQuerySettled {
    pub server_id: Option<ServerId>,
    pub query_id: QueryId,
    pub tier: DurabilityTier,
    pub through_seq: u64,
}

/// Deferred query rejection waiting for QueryManager to drop local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingQueryRejection {
    pub query_id: QueryId,
    pub code: String,
    pub reason: String,
}

// ============================================================================
// Client Roles
// ============================================================================

/// Role-based access control for client connections.
///
/// Determines how incoming writes from a client are routed:
/// - `User`: Requires session, ReBAC for rows, rejected for catalogue unless
///   development-only schema auto-push is enabled
/// - `Backend`: Trusted backend data access (rows only, no catalogue writes)
/// - `Admin`: Full access (catalogue + data, no ReBAC)
/// - `Peer`: Trusted relay (server-to-server), bypasses all auth
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientRole {
    #[default]
    User,
    Backend,
    Admin,
    Peer,
}

// ============================================================================
// Connection State
// ============================================================================

/// Tracking state for a connected server.
#[derive(Debug, Clone, Default)]
pub struct ServerState {
    /// What we've pushed to this server for row-history sync:
    /// (row object, branch) -> set of known batch ids.
    pub sent_batch_ids: HashMap<(ObjectId, BranchName), HashSet<BatchId>>,
    /// Row IDs for which we've sent metadata.
    pub sent_metadata: HashSet<ObjectId>,
}

/// A query's scope and session for policy filtering.
#[derive(Debug, Clone, Default)]
pub struct QueryScope {
    /// The scope of objects/branches this query covers.
    pub scope: HashSet<(ObjectId, BranchName)>,
    /// The session to use for policy filtering (captured at registration time).
    pub session: Option<Session>,
}

/// Tracking state for a connected client.
#[derive(Debug, Clone, Default)]
pub struct ClientState {
    /// Client's role for access control.
    pub role: ClientRole,
    /// Client's session for policy evaluation.
    pub session: Option<Session>,
    /// Active queries from this client.
    pub queries: HashMap<QueryId, QueryScope>,
    /// What we've sent to this client for row-history sync:
    /// (row object, branch) -> set of known batch ids.
    pub sent_batch_ids: HashMap<(ObjectId, BranchName), HashSet<BatchId>>,
    /// Row IDs for which we've sent metadata.
    pub sent_metadata: HashSet<ObjectId>,
}

impl ClientState {
    /// Create a new ClientState with an optional session.
    pub fn with_session(session: Option<Session>) -> Self {
        Self {
            session,
            ..Default::default()
        }
    }

    /// Check if an object/branch is in any of this client's query scopes.
    pub fn is_in_scope(&self, object_id: ObjectId, branch_name: &BranchName) -> bool {
        self.queries
            .values()
            .any(|q| q.scope.contains(&(object_id, *branch_name)))
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Strongly typed errors for sync operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncError {
    /// Operation denied due to insufficient permission.
    PermissionDenied {
        object_id: ObjectId,
        branch_name: BranchName,
        code: String,
        reason: String,
    },
    /// Client must have a session to write.
    SessionRequired {
        object_id: ObjectId,
        branch_name: BranchName,
    },
    /// User clients cannot write catalogue objects.
    CatalogueWriteDenied {
        object_id: ObjectId,
        branch_name: BranchName,
    },
    /// Query subscription was rejected (e.g. query compilation failed).
    QuerySubscriptionRejected {
        query_id: QueryId,
        code: String,
        reason: String,
    },
}

// ============================================================================
// Message Protocol
// ============================================================================

/// Row metadata sent once per destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowMetadata {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
}

/// Payload for sync messages between peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncPayload {
    /// Semantic update for one catalogue/system entry.
    CatalogueEntryUpdated { entry: CatalogueEntry },

    /// Upstream replication of a newly created or newly learned row batch entry.
    RowBatchCreated {
        metadata: Option<RowMetadata>,
        row: StoredRowBatch,
    },

    /// Downstream delivery of a row batch entry that is needed for a subscriber's scope.
    RowBatchNeeded {
        metadata: Option<RowMetadata>,
        row: StoredRowBatch,
    },

    /// Replayable fate for one logical batch.
    BatchFate { fate: BatchFate },

    /// Request current replayable fate for specific batch ids.
    BatchFateNeeded { batch_ids: Vec<BatchId> },

    /// Explicitly seal a transactional batch so the authority can validate it.
    SealBatch { submission: SealedBatchSubmission },

    /// Subscribe to a query (client to server).
    /// Server will build QueryGraph and send matching objects.
    QuerySubscription {
        query_id: QueryId,
        query: Box<Query>,
        #[serde(with = "query_subscription_session_serde")]
        session: Option<Session>,
        #[serde(default)]
        required_tier: Option<DurabilityTier>,
        #[serde(default)]
        propagation: QueryPropagation,
        #[serde(default)]
        policy_context_tables: Vec<String>,
    },

    /// Unsubscribe from a query (client to server).
    QueryUnsubscription { query_id: QueryId },

    /// Query frontier settlement notification with the authoritative query scope
    /// for the settled server result.
    ///
    /// This means the upstream server has reached a complete first frontier for the
    /// subscription. Per-batch durability and visibility are replayed via `BatchFate`.
    QuerySettled {
        query_id: QueryId,
        tier: DurabilityTier,
        scope: Vec<(ObjectId, BranchName)>,
        /// Highest stream sequence known to be emitted before this notification.
        through_seq: u64,
    },

    /// Warning that rows exist on an older schema branch but are currently unreachable.
    SchemaWarning(SchemaWarning),

    /// Connection-time schema diagnostics for observability.
    ConnectionSchemaDiagnostics(ConnectionSchemaDiagnostics),

    /// Error response.
    Error(SyncError),
}

/// Warning emitted when a query encounters rows that cannot be transformed into the
/// subscriber's target schema because no reviewed migration path exists yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaWarning {
    pub query_id: QueryId,
    pub table_name: String,
    pub row_count: usize,
    pub from_hash: SchemaHash,
    pub to_hash: SchemaHash,
}

/// Warning sent to the client when its schema is either disconnected from the permissions schema
/// or not connected to other schemas known to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSchemaDiagnostics {
    pub client_schema_hash: SchemaHash,
    pub disconnected_permissions_schema_hash: Option<SchemaHash>,
    pub unreachable_schema_hashes: Vec<SchemaHash>,
}

impl ConnectionSchemaDiagnostics {
    pub fn has_issues(&self) -> bool {
        self.disconnected_permissions_schema_hash.is_some()
            || !self.unreachable_schema_hashes.is_empty()
    }
}

/// Sessions contain claims as a JSON object.
/// postcard does not support the dynamic deserialization style it expects (deserialize_any)
/// so we need a custom serializer/deserializer to serialize/deserialize the claims as a string.
mod query_subscription_session_serde {
    use crate::query_manager::session::{AuthMode, Session};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SessionWire {
        user_id: String,
        claims_json: String,
        auth_mode: AuthMode,
    }

    pub fn serialize<S>(value: &Option<Session>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }

        let wire: Option<SessionWire> = value
            .as_ref()
            .map(|session| {
                let claims_json =
                    serde_json::to_string(&session.claims).map_err(serde::ser::Error::custom)?;
                Ok(SessionWire {
                    user_id: session.user_id.clone(),
                    claims_json,
                    auth_mode: session.auth_mode,
                })
            })
            .transpose()?;

        wire.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Session>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return Option::<Session>::deserialize(deserializer);
        }

        let wire = Option::<SessionWire>::deserialize(deserializer)?;
        wire.map(|session_wire| {
            let claims = serde_json::from_str(&session_wire.claims_json)
                .map_err(serde::de::Error::custom)?;
            Ok(Session {
                user_id: session_wire.user_id,
                claims,
                auth_mode: session_wire.auth_mode,
            })
        })
        .transpose()
    }
}

impl SyncPayload {
    pub fn object_id(&self) -> Option<ObjectId> {
        match self {
            SyncPayload::CatalogueEntryUpdated { entry } => Some(entry.object_id),
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                Some(row.row_id)
            }
            SyncPayload::BatchFate { .. } => None,
            SyncPayload::BatchFateNeeded { .. } => None,
            SyncPayload::SealBatch { submission } => {
                submission.members.first().map(|member| member.object_id)
            }
            SyncPayload::QuerySettled { scope, .. } => {
                scope.first().map(|(object_id, _)| *object_id)
            }
            _ => None,
        }
    }

    pub fn branch_name(&self) -> Option<BranchName> {
        match self {
            SyncPayload::CatalogueEntryUpdated { .. } => None,
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                Some(BranchName::new(&row.branch))
            }
            SyncPayload::BatchFate { .. } => None,
            SyncPayload::BatchFateNeeded { .. } => None,
            SyncPayload::SealBatch { .. } => None,
            SyncPayload::QuerySettled { scope, .. } => {
                scope.first().map(|(_, branch_name)| *branch_name)
            }
            _ => None,
        }
    }

    /// True when handling this payload may mutate local storage.
    pub fn writes_storage(&self) -> bool {
        matches!(
            self,
            SyncPayload::CatalogueEntryUpdated { .. }
                | SyncPayload::RowBatchCreated { .. }
                | SyncPayload::RowBatchNeeded { .. }
                | SyncPayload::BatchFate { .. }
                | SyncPayload::SealBatch { .. }
        )
    }

    /// Encode this payload using postcard.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Decode a payload from postcard bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Check if this payload carries a catalogue object (schema or lens).
    pub fn is_catalogue(&self) -> bool {
        matches!(self, SyncPayload::CatalogueEntryUpdated { entry } if entry.is_catalogue())
    }

    /// Check if this payload carries a structural schema catalogue object.
    pub fn is_structural_schema_catalogue(&self) -> bool {
        matches!(self, SyncPayload::CatalogueEntryUpdated { entry } if entry.is_structural_schema_catalogue())
    }

    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            SyncPayload::CatalogueEntryUpdated { .. } => "CatalogueEntryUpdated",
            SyncPayload::RowBatchCreated { .. } => "RowBatchCreated",
            SyncPayload::RowBatchNeeded { .. } => "RowBatchNeeded",
            SyncPayload::BatchFate { .. } => "BatchFate",
            SyncPayload::BatchFateNeeded { .. } => "BatchFateNeeded",
            SyncPayload::SealBatch { .. } => "SealBatch",
            SyncPayload::QuerySubscription { .. } => "QuerySubscription",
            SyncPayload::QueryUnsubscription { .. } => "QueryUnsubscription",
            SyncPayload::QuerySettled { .. } => "QuerySettled",
            SyncPayload::SchemaWarning(_) => "SchemaWarning",
            SyncPayload::ConnectionSchemaDiagnostics(_) => "ConnectionSchemaDiagnostics",
            SyncPayload::Error(_) => "Error",
        }
    }
}

/// Destination for an outbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Destination {
    Server(ServerId),
    Client(ClientId),
}

/// Source of an inbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Source {
    Server(ServerId),
    Client(ClientId),
}

/// Outgoing message to be sent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub destination: Destination,
    pub payload: SyncPayload,
}

/// Incoming message to be processed.
#[derive(Debug, Clone)]
pub struct InboxEntry {
    pub source: Source,
    pub payload: SyncPayload,
}

/// A pending query subscription that needs QueryGraph building.
#[derive(Debug, Clone)]
pub struct PendingQuerySubscription {
    pub client_id: ClientId,
    pub query_id: QueryId,
    pub query: Query,
    pub session: Option<Session>,
    pub required_tier: Option<DurabilityTier>,
    pub propagation: QueryPropagation,
    pub policy_context_tables: Vec<String>,
}

/// A pending query unsubscription that needs cleanup.
#[derive(Debug, Clone)]
pub struct PendingQueryUnsubscription {
    pub client_id: ClientId,
    pub query_id: QueryId,
}

/// A write from a User client awaiting permission check (policy evaluation).
///
/// Row-level policy evaluation which may require async graph settling.
#[derive(Debug, Clone)]
pub struct PendingPermissionCheck {
    pub id: PendingUpdateId,
    pub client_id: ClientId,
    pub payload: SyncPayload,
    pub session: Session,
    /// When schema resolution started deferring this check.
    pub schema_wait_started_at: Option<Instant>,
    /// Object metadata for policy evaluation.
    pub metadata: HashMap<String, String>,
    /// Old content for UPDATE/DELETE (None for INSERT).
    pub old_content: Option<Vec<u8>>,
    /// New content for INSERT/UPDATE (None for DELETE).
    pub new_content: Option<Vec<u8>>,
    /// Inferred operation type.
    pub operation: Operation,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::session::AuthMode;

    #[test]
    fn query_subscription_postcard_roundtrip_preserves_session_auth_mode() {
        let payload = SyncPayload::QuerySubscription {
            query_id: QueryId(7),
            query: Box::new(Query::new("todos")),
            session: Some(Session::new("alice").with_auth_mode(AuthMode::LocalFirst)),
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: Vec::new(),
        };

        let bytes = payload.to_bytes().expect("encode payload");
        let decoded = SyncPayload::from_bytes(&bytes).expect("decode payload");

        match decoded {
            SyncPayload::QuerySubscription {
                session: Some(session),
                ..
            } => assert_eq!(session.auth_mode, AuthMode::LocalFirst),
            other => panic!("expected QuerySubscription with session, got {other:?}"),
        }
    }
}
