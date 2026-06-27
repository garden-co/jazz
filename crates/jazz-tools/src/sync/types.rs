use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::SchemaHash;
use crate::sync::{ClientId, ServerId};

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
}
