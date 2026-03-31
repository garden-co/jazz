use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::BatchBranchKey;
use crate::query_manager::types::SchemaHash;
use crate::schema_manager::QuerySchemaContext;

/// Error returned when a policy denies an operation.
#[derive(Debug, Clone)]
pub struct PolicyError {
    pub message: String,
}

// ============================================================================
// ID Types
// ============================================================================

/// Persistence tier — declaration order defines Ord (Worker < EdgeServer < GlobalServer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum DurabilityTier {
    Worker,
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
    /// What we've pushed to this server: (object, branch) → set of commit tips.
    pub sent_tips: HashMap<(ObjectId, BatchBranchKey), HashSet<CommitId>>,
    /// Object IDs for which we've sent metadata.
    pub sent_metadata: HashSet<ObjectId>,
}

pub type ScopedBranchKey = (ObjectId, BatchBranchKey);

/// A query's scope and session for policy filtering.
#[derive(Debug, Clone, Default)]
pub struct QueryScope {
    /// The scope of objects/branches this query covers.
    pub scope: HashSet<ScopedBranchKey>,
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
    pub sent_tips: HashMap<ScopedBranchKey, HashSet<CommitId>>,
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
        let branch_key = BatchBranchKey::from_branch_name(*branch_name);
        self.queries
            .values()
            .any(|q| q.scope.contains(&(object_id, branch_key)))
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
        schema_context: QuerySchemaContext,
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
        tier: DurabilityTier,
    },

    /// Query settlement notification — a query has settled at a given persistence tier.
    QuerySettled {
        query_id: QueryId,
        tier: DurabilityTier,
        /// Highest stream sequence known to be emitted before this notification.
        through_seq: u64,
    },

    /// Warning that rows exist on an older schema branch but are currently unreachable.
    SchemaWarning(SchemaWarning),

    /// Error response.
    Error(SyncError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum WireBranchRef {
    Raw(BranchName),
    Known {
        prefix_ord: u32,
        batch_id: crate::query_manager::types::BatchId,
    },
    Define {
        prefix_ord: u32,
        prefix_name: BranchName,
        batch_id: crate::query_manager::types::BatchId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum WireSyncError {
    PermissionDenied {
        object_id: ObjectId,
        branch: WireBranchRef,
        reason: String,
    },
    SessionRequired {
        object_id: ObjectId,
        branch: WireBranchRef,
    },
    CatalogueWriteDenied {
        object_id: ObjectId,
        branch: WireBranchRef,
    },
    QuerySubscriptionRejected {
        query_id: QueryId,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum WireSyncPayload {
    ObjectUpdated {
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,
        branch: WireBranchRef,
        commits: Vec<Commit>,
    },
    ObjectTruncated {
        object_id: ObjectId,
        branch: WireBranchRef,
        tails: HashSet<CommitId>,
    },
    QuerySubscription {
        query_id: QueryId,
        query: Box<Query>,
        schema_context: QuerySchemaContext,
        #[serde(with = "query_subscription_session_serde")]
        session: Option<Session>,
        #[serde(default)]
        propagation: QueryPropagation,
    },
    QueryUnsubscription {
        query_id: QueryId,
    },
    PersistenceAck {
        object_id: ObjectId,
        branch: WireBranchRef,
        confirmed_commits: HashSet<CommitId>,
        tier: DurabilityTier,
    },
    QuerySettled {
        query_id: QueryId,
        tier: DurabilityTier,
        through_seq: u64,
    },
    SchemaWarning(SchemaWarning),
    Error(WireSyncError),
}

#[derive(Debug, Default)]
pub struct SyncConnectionCodec {
    outbound_prefixes: HashMap<BranchName, u32>,
    inbound_prefixes: HashMap<u32, BranchName>,
    next_prefix_ord: u32,
}

#[derive(Debug)]
pub enum SyncConnectionCodecError {
    Postcard(postcard::Error),
    UnknownPrefixOrd(u32),
    ConflictingPrefixOrd {
        prefix_ord: u32,
        existing: BranchName,
        received: BranchName,
    },
}

impl std::fmt::Display for SyncConnectionCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postcard(error) => write!(f, "postcard error: {error}"),
            Self::UnknownPrefixOrd(prefix_ord) => {
                write!(f, "unknown sync prefix ord {prefix_ord}")
            }
            Self::ConflictingPrefixOrd {
                prefix_ord,
                existing,
                received,
            } => write!(
                f,
                "conflicting sync prefix ord {prefix_ord}: {existing} vs {received}"
            ),
        }
    }
}

impl std::error::Error for SyncConnectionCodecError {}

impl From<postcard::Error> for SyncConnectionCodecError {
    fn from(value: postcard::Error) -> Self {
        Self::Postcard(value)
    }
}

impl SyncConnectionCodec {
    pub fn reset(&mut self) {
        self.outbound_prefixes.clear();
        self.inbound_prefixes.clear();
        self.next_prefix_ord = 0;
    }

    pub fn encode_payload(
        &mut self,
        payload: SyncPayload,
    ) -> Result<Vec<u8>, SyncConnectionCodecError> {
        let wire = self.encode_wire_payload(payload);
        Ok(postcard::to_allocvec(&wire)?)
    }

    pub fn decode_payload(
        &mut self,
        bytes: &[u8],
    ) -> Result<SyncPayload, SyncConnectionCodecError> {
        let wire: WireSyncPayload = postcard::from_bytes(bytes)?;
        self.decode_wire_payload(wire)
    }

    fn encode_wire_payload(&mut self, payload: SyncPayload) -> WireSyncPayload {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => WireSyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch: self.encode_branch(branch_name),
                commits,
            },
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => WireSyncPayload::ObjectTruncated {
                object_id,
                branch: self.encode_branch(branch_name),
                tails,
            },
            SyncPayload::QuerySubscription {
                query_id,
                query,
                schema_context,
                session,
                propagation,
            } => WireSyncPayload::QuerySubscription {
                query_id,
                query,
                schema_context,
                session,
                propagation,
            },
            SyncPayload::QueryUnsubscription { query_id } => {
                WireSyncPayload::QueryUnsubscription { query_id }
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => WireSyncPayload::PersistenceAck {
                object_id,
                branch: self.encode_branch(branch_name),
                confirmed_commits,
                tier,
            },
            SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            } => WireSyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            },
            SyncPayload::SchemaWarning(warning) => WireSyncPayload::SchemaWarning(warning),
            SyncPayload::Error(error) => WireSyncPayload::Error(self.encode_error(error)),
        }
    }

    fn decode_wire_payload(
        &mut self,
        payload: WireSyncPayload,
    ) -> Result<SyncPayload, SyncConnectionCodecError> {
        match payload {
            WireSyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch,
                commits,
            } => Ok(SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name: self.decode_branch(branch)?,
                commits,
            }),
            WireSyncPayload::ObjectTruncated {
                object_id,
                branch,
                tails,
            } => Ok(SyncPayload::ObjectTruncated {
                object_id,
                branch_name: self.decode_branch(branch)?,
                tails,
            }),
            WireSyncPayload::QuerySubscription {
                query_id,
                query,
                schema_context,
                session,
                propagation,
            } => Ok(SyncPayload::QuerySubscription {
                query_id,
                query,
                schema_context,
                session,
                propagation,
            }),
            WireSyncPayload::QueryUnsubscription { query_id } => {
                Ok(SyncPayload::QueryUnsubscription { query_id })
            }
            WireSyncPayload::PersistenceAck {
                object_id,
                branch,
                confirmed_commits,
                tier,
            } => Ok(SyncPayload::PersistenceAck {
                object_id,
                branch_name: self.decode_branch(branch)?,
                confirmed_commits,
                tier,
            }),
            WireSyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            } => Ok(SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            }),
            WireSyncPayload::SchemaWarning(warning) => Ok(SyncPayload::SchemaWarning(warning)),
            WireSyncPayload::Error(error) => Ok(SyncPayload::Error(self.decode_error(error)?)),
        }
    }

    fn encode_error(&mut self, error: SyncError) -> WireSyncError {
        match error {
            SyncError::PermissionDenied {
                object_id,
                branch_name,
                reason,
            } => WireSyncError::PermissionDenied {
                object_id,
                branch: self.encode_branch(branch_name),
                reason,
            },
            SyncError::SessionRequired {
                object_id,
                branch_name,
            } => WireSyncError::SessionRequired {
                object_id,
                branch: self.encode_branch(branch_name),
            },
            SyncError::CatalogueWriteDenied {
                object_id,
                branch_name,
            } => WireSyncError::CatalogueWriteDenied {
                object_id,
                branch: self.encode_branch(branch_name),
            },
            SyncError::QuerySubscriptionRejected { query_id, reason } => {
                WireSyncError::QuerySubscriptionRejected { query_id, reason }
            }
        }
    }

    fn decode_error(
        &mut self,
        error: WireSyncError,
    ) -> Result<SyncError, SyncConnectionCodecError> {
        match error {
            WireSyncError::PermissionDenied {
                object_id,
                branch,
                reason,
            } => Ok(SyncError::PermissionDenied {
                object_id,
                branch_name: self.decode_branch(branch)?,
                reason,
            }),
            WireSyncError::SessionRequired { object_id, branch } => {
                Ok(SyncError::SessionRequired {
                    object_id,
                    branch_name: self.decode_branch(branch)?,
                })
            }
            WireSyncError::CatalogueWriteDenied { object_id, branch } => {
                Ok(SyncError::CatalogueWriteDenied {
                    object_id,
                    branch_name: self.decode_branch(branch)?,
                })
            }
            WireSyncError::QuerySubscriptionRejected { query_id, reason } => {
                Ok(SyncError::QuerySubscriptionRejected { query_id, reason })
            }
        }
    }

    fn encode_branch(&mut self, branch_name: BranchName) -> WireBranchRef {
        let Some(branch_key) = BatchBranchKey::try_from_branch_name(branch_name) else {
            return WireBranchRef::Raw(branch_name);
        };
        let prefix_name = branch_key.prefix_name();
        if let Some(prefix_ord) = self.outbound_prefixes.get(&prefix_name).copied() {
            return WireBranchRef::Known {
                prefix_ord,
                batch_id: branch_key.batch_id(),
            };
        }

        let prefix_ord = self.next_prefix_ord;
        self.next_prefix_ord = self.next_prefix_ord.saturating_add(1);
        self.outbound_prefixes.insert(prefix_name, prefix_ord);
        WireBranchRef::Define {
            prefix_ord,
            prefix_name,
            batch_id: branch_key.batch_id(),
        }
    }

    fn decode_branch(
        &mut self,
        branch: WireBranchRef,
    ) -> Result<BranchName, SyncConnectionCodecError> {
        match branch {
            WireBranchRef::Raw(branch_name) => Ok(branch_name),
            WireBranchRef::Known {
                prefix_ord,
                batch_id,
            } => {
                let prefix_name = self
                    .inbound_prefixes
                    .get(&prefix_ord)
                    .copied()
                    .ok_or(SyncConnectionCodecError::UnknownPrefixOrd(prefix_ord))?;
                Ok(BatchBranchKey::from_prefix_name_and_batch(prefix_name, batch_id).branch_name())
            }
            WireBranchRef::Define {
                prefix_ord,
                prefix_name,
                batch_id,
            } => {
                if let Some(existing) = self.inbound_prefixes.get(&prefix_ord).copied() {
                    if existing != prefix_name {
                        return Err(SyncConnectionCodecError::ConflictingPrefixOrd {
                            prefix_ord,
                            existing,
                            received: prefix_name,
                        });
                    }
                } else {
                    self.inbound_prefixes.insert(prefix_ord, prefix_name);
                }
                Ok(BatchBranchKey::from_prefix_name_and_batch(prefix_name, batch_id).branch_name())
            }
        }
    }
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

/// Sessions contain claims as a JSON object.
/// postcard does not support the dynamic deserialization style it expects (deserialize_any)
/// so we need a custom serializer/deserializer to serialize/deserialize the claims as a string.
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
            Some(t) if crate::metadata::ObjectType::is_catalogue_type_str(t)
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
            SyncPayload::SchemaWarning(_) => "SchemaWarning",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{Commit, CommitAckState, StoredState};
    use crate::query_manager::types::{BatchId, BranchPrefixName, SchemaHash};
    use smallvec::smallvec;
    use uuid::Uuid;

    fn branch(batch_seed: u128) -> BranchName {
        BranchPrefixName::new("dev", SchemaHash::from_bytes([7; 32]), "main")
            .with_batch_id(BatchId::from_uuid(Uuid::from_u128(batch_seed)))
            .to_branch_name()
    }

    fn object_updated(branch_name: BranchName, content: &[u8]) -> SyncPayload {
        SyncPayload::ObjectUpdated {
            object_id: ObjectId::new(),
            metadata: None,
            branch_name,
            commits: vec![Commit {
                parents: smallvec![],
                content: content.to_vec(),
                timestamp: 1,
                author: ObjectId::new(),
                metadata: None,
                stored_state: StoredState::default(),
                ack_state: CommitAckState::default(),
            }],
        }
    }

    #[test]
    fn connection_codec_roundtrips_branch_payloads_with_prefix_dictionary() {
        let first = object_updated(branch(1), b"alice");
        let second = object_updated(branch(2), b"bob");
        let mut encoder = SyncConnectionCodec::default();
        let mut decoder = SyncConnectionCodec::default();

        let first_bytes = encoder
            .encode_payload(first.clone())
            .expect("encode first payload");
        let decoded_first = decoder
            .decode_payload(&first_bytes)
            .expect("decode first payload");
        let second_bytes = encoder
            .encode_payload(second.clone())
            .expect("encode second payload");
        let decoded_second = decoder
            .decode_payload(&second_bytes)
            .expect("decode second payload");

        assert_eq!(decoded_first, first);
        assert_eq!(decoded_second, second);
        assert!(
            second_bytes.len() < first_bytes.len(),
            "second payload should reuse the previously defined prefix"
        );
        assert!(
            second_bytes.len() < second.to_bytes().expect("raw postcard payload").len(),
            "connection codec should beat raw postcard once the prefix is known"
        );
    }

    #[test]
    fn connection_codec_rejects_unknown_prefix_ord() {
        let payload = object_updated(branch(1), b"alice");
        let mut encoder = SyncConnectionCodec::default();
        let bytes = encoder.encode_payload(payload).expect("encode payload");
        let wire: WireSyncPayload = postcard::from_bytes(&bytes).expect("decode wire payload");
        let tampered = match wire {
            WireSyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch: WireBranchRef::Define { batch_id, .. },
                commits,
            } => WireSyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch: WireBranchRef::Known {
                    prefix_ord: 999,
                    batch_id,
                },
                commits,
            },
            _ => panic!("expected first payload to define a prefix"),
        };

        let encoded = postcard::to_allocvec(&tampered).expect("re-encode tampered payload");
        let mut decoder = SyncConnectionCodec::default();
        let error = decoder
            .decode_payload(&encoded)
            .expect_err("unknown prefix ord should fail");

        match error {
            SyncConnectionCodecError::UnknownPrefixOrd(999) => {}
            other => panic!("unexpected decode error: {other}"),
        }
    }
}

/// A pending query subscription that needs QueryGraph building.
#[derive(Debug, Clone)]
pub struct PendingQuerySubscription {
    pub client_id: ClientId,
    pub query_id: QueryId,
    pub query: Query,
    pub schema_context: QuerySchemaContext,
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
