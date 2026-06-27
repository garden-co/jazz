use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::batch_fate::{BatchFate, SealedBatchSubmission};
use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::SchemaHash;
use crate::row_histories::{BatchId, StoredRowBatch};
use crate::sync::{ClientId, DurabilityTier, ServerId};

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

/// Row metadata sent once per destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowMetadata {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
}

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
    /// This client role cannot write catalogue objects.
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
        match self {
            SyncPayload::CatalogueEntryUpdated { entry } => entry.is_catalogue(),
            SyncPayload::RowBatchCreated { metadata, .. }
            | SyncPayload::RowBatchNeeded { metadata, .. } => metadata
                .as_ref()
                .and_then(|metadata| {
                    metadata
                        .metadata
                        .get(crate::metadata::MetadataKey::Type.as_str())
                })
                .is_some_and(|kind| crate::metadata::ObjectType::is_catalogue_type_str(kind)),
            _ => false,
        }
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

/// Either end of a peer relationship. `Source` and `Destination` are mirror
/// images, and both expose the same peer identity fields for telemetry.
trait PeerEnd {
    fn descriptor(&self) -> (&'static str, Uuid);
}

/// Destination for an outbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Destination {
    Server(ServerId),
    Client(ClientId),
}

impl PeerEnd for Destination {
    fn descriptor(&self) -> (&'static str, Uuid) {
        match self {
            Destination::Server(id) => ("server", id.0),
            Destination::Client(id) => ("client", id.0),
        }
    }
}

impl Destination {
    pub fn peer_kind(&self) -> &'static str {
        PeerEnd::descriptor(self).0
    }

    pub fn peer_uuid(&self) -> Uuid {
        PeerEnd::descriptor(self).1
    }
}

/// Source of an inbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Source {
    Server(ServerId),
    Client(ClientId),
}

impl PeerEnd for Source {
    fn descriptor(&self) -> (&'static str, Uuid) {
        match self {
            Source::Server(id) => ("server", id.0),
            Source::Client(id) => ("client", id.0),
        }
    }
}

impl Source {
    pub fn peer_kind(&self) -> &'static str {
        PeerEnd::descriptor(self).0
    }

    pub fn peer_uuid(&self) -> Uuid {
        PeerEnd::descriptor(self).1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::session::AuthMode;

    #[test]
    fn destination_exposes_peer_identity_for_telemetry() {
        let server_id = ServerId::new();
        let client_id = ClientId::new();

        let server = Destination::Server(server_id);
        let client = Destination::Client(client_id);

        assert_eq!(server.peer_kind(), "server");
        assert_eq!(server.peer_uuid(), server_id.0);
        assert_eq!(client.peer_kind(), "client");
        assert_eq!(client.peer_uuid(), client_id.0);
    }

    #[test]
    fn source_exposes_peer_identity_for_telemetry() {
        let server_id = ServerId::new();
        let client_id = ClientId::new();

        let server = Source::Server(server_id);
        let client = Source::Client(client_id);

        assert_eq!(server.peer_kind(), "server");
        assert_eq!(server.peer_uuid(), server_id.0);
        assert_eq!(client.peer_kind(), "client");
        assert_eq!(client.peer_uuid(), client_id.0);
    }

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
