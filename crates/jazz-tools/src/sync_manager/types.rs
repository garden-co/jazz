use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::row_histories::{BatchId, StoredRowBatch};
use crate::sync::{ClientId, DurabilityTier, ServerId};

/// Error returned when a policy denies an operation.
#[derive(Debug, Clone)]
pub struct PolicyError {
    pub message: String,
}

pub use crate::sync::types::{
    ConnectionSchemaDiagnostics, Destination, QueryId, QueryPropagation, RowMetadata,
    SchemaWarning, Source, SyncError, SyncPayload,
};

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

/// The set of batch ids already sent to a peer for one `(row, branch)`.
///
/// A newtype around the underlying set purely so its `Clone` can be observed.
/// The set grows with a row's history, and an earlier regression cloned the
/// whole set on every queued batch just to test membership, making each forward
/// O(n) in the history length. Membership is now checked by borrow; the custom
/// `Clone` is instrumented under `cfg(test)` so a guard test can assert the
/// forwarding hot path never clones the set again.
#[derive(Debug, Default)]
pub struct SentBatchIds(HashSet<BatchId>);

impl Clone for SentBatchIds {
    fn clone(&self) -> Self {
        #[cfg(test)]
        sent_batch_clone_probe::record();
        Self(self.0.clone())
    }
}

impl std::ops::Deref for SentBatchIds {
    type Target = HashSet<BatchId>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for SentBatchIds {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for SentBatchIds {
    type Item = BatchId;
    type IntoIter = std::collections::hash_set::IntoIter<BatchId>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<const N: usize> From<[BatchId; N]> for SentBatchIds {
    fn from(batch_ids: [BatchId; N]) -> Self {
        Self(HashSet::from(batch_ids))
    }
}

/// Test-only probe counting clones of [`SentBatchIds`] on the current thread, so
/// a guard test can assert the forwarding hot path checks membership by borrow
/// rather than by cloning the whole set.
#[cfg(test)]
pub(crate) mod sent_batch_clone_probe {
    use std::cell::Cell;

    thread_local! {
        static CLONES: Cell<usize> = Cell::new(0);
    }

    pub(crate) fn reset() {
        CLONES.with(|clones| clones.set(0));
    }

    pub(crate) fn record() {
        CLONES.with(|clones| clones.set(clones.get() + 1));
    }

    pub(crate) fn count() -> usize {
        CLONES.with(Cell::get)
    }
}

/// Tracking state for a connected server.
#[derive(Debug, Clone, Default)]
pub struct ServerState {
    /// What we've pushed to this server for row-history sync:
    /// (row object, branch) -> set of known batch ids.
    pub sent_batch_ids: HashMap<(ObjectId, BranchName), SentBatchIds>,
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
    pub sent_batch_ids: HashMap<(ObjectId, BranchName), SentBatchIds>,
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
