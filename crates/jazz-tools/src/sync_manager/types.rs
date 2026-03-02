use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;

/// Error returned when a policy denies an operation.
#[derive(Debug, Clone)]
pub struct PolicyError {
    pub message: String,
}

// ============================================================================
// ID Types
// ============================================================================

/// Persistence tier — declaration order defines Ord (Worker < EdgeServer < CoreServer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum PersistenceTier {
    Worker,
    EdgeServer,
    CoreServer,
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

/// Data needed to sync a branch: (object_id, metadata, branch_name, tips).
pub(super) type BranchSyncData = (
    ObjectId,
    HashMap<String, String>,
    BranchName,
    HashSet<CommitId>,
);

// ============================================================================
// Client Roles
// ============================================================================

/// Role-based access control for client connections.
///
/// Determines how incoming writes from a client are routed:
/// - `User`: Requires session, ReBAC for rows, rejected for catalogue
/// - `Admin`: Full access (catalogue + data, no ReBAC)
/// - `Peer`: Trusted relay (server-to-server), bypasses all auth
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientRole {
    #[default]
    User,
    Admin,
    Peer,
}

// ============================================================================
// Connection State
// ============================================================================

/// Tracking state for a connected server.
#[derive(Debug, Clone, Default)]
pub struct ServerState {
    /// What we've pushed to this server: (object, branch) → set of commit tips.
    pub sent_tips: HashMap<(ObjectId, BranchName), HashSet<CommitId>>,
    /// Object IDs for which we've sent metadata.
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
    /// What we've sent to this client.
    pub sent_tips: HashMap<(ObjectId, BranchName), HashSet<CommitId>>,
    /// Object IDs for which we've sent metadata.
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
    QuerySubscriptionRejected { query_id: QueryId, reason: String },
}

// ============================================================================
// Message Protocol
// ============================================================================

/// Object metadata sent once per destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
}

/// Payload for sync messages between peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncPayload {
    /// Object or branch update with commits.
    ObjectUpdated {
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,
        branch_name: BranchName,
        commits: Vec<Commit>,
    },

    /// Branch truncated - new tail boundary.
    ObjectTruncated {
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    },

    /// Subscribe to a query (client to server).
    /// Server will build QueryGraph and send matching objects.
    QuerySubscription {
        query_id: QueryId,
        query: Box<Query>,
        #[serde(with = "query_subscription_session_serde")]
        session: Option<Session>,
        #[serde(default)]
        propagation: QueryPropagation,
    },

    /// Unsubscribe from a query (client to server).
    QueryUnsubscription { query_id: QueryId },

    /// Persistence acknowledgment — confirms a set of commits were persisted at a tier.
    PersistenceAck {
        object_id: ObjectId,
        branch_name: BranchName,
        confirmed_commits: HashSet<CommitId>,
        tier: PersistenceTier,
    },

    /// Query settlement notification — a query has settled at a given persistence tier.
    QuerySettled {
        query_id: QueryId,
        tier: PersistenceTier,
        /// Highest stream sequence known to be emitted before this notification.
        through_seq: u64,
    },

    /// Error response.
    Error(SyncError),
}

mod query_subscription_session_serde {
    use super::Session;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SessionWire {
        user_id: String,
        claims_json: String,
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
            })
        })
        .transpose()
    }
}

impl SyncPayload {
    /// Encode this payload using bitcode.
    pub fn to_bitcode_bytes(&self) -> Result<Vec<u8>, bitcode::Error> {
        bitcode::serialize(self)
    }

    /// Decode a payload from bitcode bytes.
    pub fn from_bitcode_bytes(bytes: &[u8]) -> Result<Self, bitcode::Error> {
        bitcode::deserialize(bytes)
    }

    /// Check if this payload carries a catalogue object (schema or lens).
    pub fn is_catalogue(&self) -> bool {
        let metadata = match self {
            SyncPayload::ObjectUpdated {
                metadata: Some(m), ..
            } => &m.metadata,
            SyncPayload::ObjectTruncated { .. } => {
                // Truncation could be catalogue, but we check conservatively.
                // The object_id might not have metadata attached to the truncation payload,
                // so we can't determine type from the payload alone.
                // Catalogue truncation is rare; treat as non-catalogue for routing.
                return false;
            }
            _ => return false,
        };
        matches!(
            metadata.get(crate::metadata::MetadataKey::Type.as_str()).map(|s| s.as_str()),
            Some(t) if t == crate::metadata::ObjectType::CatalogueSchema.as_str()
                || t == crate::metadata::ObjectType::CatalogueLens.as_str()
        )
    }

    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            SyncPayload::ObjectUpdated { .. } => "ObjectUpdated",
            SyncPayload::ObjectTruncated { .. } => "ObjectTruncated",
            SyncPayload::QuerySubscription { .. } => "QuerySubscription",
            SyncPayload::QueryUnsubscription { .. } => "QueryUnsubscription",
            SyncPayload::PersistenceAck { .. } => "PersistenceAck",
            SyncPayload::QuerySettled { .. } => "QuerySettled",
            SyncPayload::Error(_) => "Error",
        }
    }
}

/// Destination for an outbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
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
    pub propagation: QueryPropagation,
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
    /// Object metadata for policy evaluation.
    pub metadata: HashMap<String, String>,
    /// Old content for UPDATE/DELETE (None for INSERT).
    pub old_content: Option<Vec<u8>>,
    /// New content for INSERT/UPDATE (None for DELETE).
    pub new_content: Option<Vec<u8>>,
    /// Inferred operation type.
    pub operation: Operation,
}
