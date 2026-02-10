use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::{BlobId, ObjectManager};
use crate::query_manager::policy::Operation;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::storage::Storage;

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

/// Unique identifier for a pending permission check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PendingUpdateId(pub u64);

/// Data needed to sync a branch: (object_id, metadata, branch_name, tips).
type BranchSyncData = (
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
    /// Blob request denied due to insufficient permission.
    BlobAccessDenied { blob_id: BlobId },
    /// Blob not found in storage.
    BlobNotFound { blob_id: BlobId },
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

    /// Request a blob by ID.
    BlobRequest { blob_id: BlobId },

    /// Response to a blob request.
    BlobResponse { blob_id: BlobId, data: Vec<u8> },

    /// Subscribe to a query (client to server).
    /// Server will build QueryGraph and send matching objects.
    QuerySubscription {
        query_id: QueryId,
        query: Query,
        session: Option<Session>,
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
    },

    /// Error response.
    Error(SyncError),
}

impl SyncPayload {
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
            metadata.get("type").map(|s| s.as_str()),
            Some("catalogue_schema" | "catalogue_lens")
        )
    }

    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            SyncPayload::ObjectUpdated { .. } => "ObjectUpdated",
            SyncPayload::ObjectTruncated { .. } => "ObjectTruncated",
            SyncPayload::BlobRequest { .. } => "BlobRequest",
            SyncPayload::BlobResponse { .. } => "BlobResponse",
            SyncPayload::QuerySubscription { .. } => "QuerySubscription",
            SyncPayload::QueryUnsubscription { .. } => "QueryUnsubscription",
            SyncPayload::PersistenceAck { .. } => "PersistenceAck",
            SyncPayload::QuerySettled { .. } => "QuerySettled",
            SyncPayload::Error(_) => "Error",
        }
    }
}

/// Destination for an outbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone)]
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

// ============================================================================
// SyncManager
// ============================================================================

/// Manages synchronization state atop ObjectManager.
///
/// Coordinates:
/// - Upstream servers (trusted, receive all our objects)
/// - Downstream clients (untrusted, receive query-filtered subsets)
#[derive(Clone)]
pub struct SyncManager {
    pub object_manager: ObjectManager,

    servers: HashMap<ServerId, ServerState>,
    clients: HashMap<ClientId, ClientState>,

    inbox: Vec<InboxEntry>,
    outbox: Vec<OutboxEntry>,
    /// Pending permission checks awaiting policy evaluation.
    pending_permission_checks: Vec<PendingPermissionCheck>,
    /// Pending query subscriptions awaiting QueryGraph building by QueryManager.
    pending_query_subscriptions: Vec<PendingQuerySubscription>,
    /// Pending query unsubscriptions awaiting cleanup by QueryManager.
    pending_query_unsubscriptions: Vec<PendingQueryUnsubscription>,

    next_pending_id: u64,

    /// This node's persistence tier (None = don't emit acks).
    my_tier: Option<PersistenceTier>,
    /// Tracks which clients are interested in acks for each commit.
    commit_interest: HashMap<CommitId, HashSet<ClientId>>,

    /// Tracks which clients originated each query (for relaying QuerySettled).
    query_origin: HashMap<QueryId, HashSet<ClientId>>,
    /// Pending QuerySettled notifications for QueryManager to process.
    pending_query_settled: Vec<(QueryId, PersistenceTier)>,

    /// Acks received during inbox processing, for RuntimeCore to consume.
    received_acks: Vec<(CommitId, PersistenceTier)>,
}

impl std::fmt::Debug for SyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncManager")
            .field("object_manager", &self.object_manager)
            .field("servers", &self.servers)
            .field("clients", &self.clients)
            .field("inbox", &self.inbox)
            .field("outbox", &self.outbox)
            .field("pending_permission_checks", &self.pending_permission_checks)
            .field(
                "pending_query_subscriptions",
                &self.pending_query_subscriptions,
            )
            .field(
                "pending_query_unsubscriptions",
                &self.pending_query_unsubscriptions,
            )
            .field("next_pending_id", &self.next_pending_id)
            .field("my_tier", &self.my_tier)
            .field("commit_interest", &self.commit_interest)
            .field("query_origin", &self.query_origin)
            .field("pending_query_settled", &self.pending_query_settled)
            .field("received_acks", &self.received_acks)
            .finish()
    }
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            object_manager: ObjectManager::new(),
            servers: HashMap::new(),
            clients: HashMap::new(),
            inbox: Vec::new(),
            outbox: Vec::new(),
            pending_permission_checks: Vec::new(),
            pending_query_subscriptions: Vec::new(),
            pending_query_unsubscriptions: Vec::new(),
            next_pending_id: 0,
            my_tier: None,
            commit_interest: HashMap::new(),
            query_origin: HashMap::new(),
            pending_query_settled: Vec::new(),
            received_acks: Vec::new(),
        }
    }

    /// Create with an existing ObjectManager.
    pub fn with_object_manager(object_manager: ObjectManager) -> Self {
        Self {
            object_manager,
            servers: HashMap::new(),
            clients: HashMap::new(),
            inbox: Vec::new(),
            outbox: Vec::new(),
            pending_permission_checks: Vec::new(),
            pending_query_subscriptions: Vec::new(),
            pending_query_unsubscriptions: Vec::new(),
            next_pending_id: 0,
            my_tier: None,
            commit_interest: HashMap::new(),
            query_origin: HashMap::new(),
            pending_query_settled: Vec::new(),
            received_acks: Vec::new(),
        }
    }

    /// Set this node's persistence tier (enables ack emission).
    pub fn with_tier(mut self, tier: PersistenceTier) -> Self {
        self.my_tier = Some(tier);
        self
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Add a server connection. Queues all existing objects to sync.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.servers.insert(server_id, ServerState::default());
        self.queue_full_sync_to_server(server_id);
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.servers.remove(&server_id);
    }

    /// Add a client connection.
    pub fn add_client(&mut self, client_id: ClientId) {
        self.clients.insert(client_id, ClientState::default());
    }

    /// Add a client connection with Peer role and sync all existing objects to them.
    ///
    /// Peer role means trusted relay (server-to-server) — bypasses all auth checks.
    /// This is used when another server connects as a client for replication.
    pub fn add_client_with_full_sync(&mut self, client_id: ClientId) {
        let state = ClientState {
            role: ClientRole::Peer,
            ..Default::default()
        };
        self.clients.insert(client_id, state);
        self.queue_full_sync_to_client(client_id);
    }

    /// Remove a client connection.
    pub fn remove_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
        // Clean up interest map
        self.commit_interest.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        // Clean up query origin map
        self.query_origin.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
    }

    /// Get server state.
    pub fn get_server(&self, server_id: ServerId) -> Option<&ServerState> {
        self.servers.get(&server_id)
    }

    /// Get client state.
    pub fn get_client(&self, client_id: ClientId) -> Option<&ClientState> {
        self.clients.get(&client_id)
    }

    /// Set the session for a client.
    pub fn set_client_session(&mut self, client_id: ClientId, session: Session) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.session = Some(session);
        }
    }

    /// Set the role for a client.
    pub fn set_client_role(&mut self, client_id: ClientId, role: ClientRole) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.role = role;
        }
    }

    // ========================================================================
    // Outbox / Inbox
    // ========================================================================

    /// Take all outbox entries, clearing the outbox.
    pub fn take_outbox(&mut self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.outbox)
    }

    /// Get a reference to the outbox (for checking if empty).
    pub fn outbox(&self) -> &[OutboxEntry] {
        &self.outbox
    }

    /// Push an entry to the inbox for processing.
    pub fn push_inbox(&mut self, entry: InboxEntry) {
        self.inbox.push(entry);
    }

    /// Process all inbox entries.
    pub fn process_inbox<H: Storage>(&mut self, storage: &mut H) {
        let entries = std::mem::take(&mut self.inbox);
        for entry in entries {
            self.process_inbox_entry(storage, entry);
        }
    }

    // ========================================================================
    // Catalogue Object Creation
    // ========================================================================

    /// Create an object with initial content for catalogue storage.
    ///
    /// Creates an object with the specified ID, metadata, and content.
    /// The content is stored as a commit on the "main" branch.
    ///
    /// Used for storing schemas and lenses in the catalogue.
    pub fn create_object_with_content<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        content: Vec<u8>,
    ) {
        // Create the object if it doesn't exist
        if self.object_manager.get(object_id).is_none() {
            self.object_manager
                .create_with_id(storage, object_id, Some(metadata));
        }

        // Add content as a commit on the "main" branch
        let _ = self.object_manager.add_commit(
            storage,
            object_id,
            "main",
            Vec::new(), // No parents - root commit
            content,
            ObjectId::from_uuid(uuid::Uuid::nil()), // System author
            None,
        );
    }

    // ========================================================================
    // Pending Query Subscriptions
    // ========================================================================

    /// Take pending query subscriptions for QueryManager to process.
    ///
    /// QueryManager will build QueryGraphs for these and call back with computed scopes.
    pub fn take_pending_query_subscriptions(&mut self) -> Vec<PendingQuerySubscription> {
        std::mem::take(&mut self.pending_query_subscriptions)
    }

    /// Re-queue pending query subscriptions that couldn't be processed yet.
    ///
    /// Called by QueryManager when schema isn't available for some subscriptions.
    pub fn requeue_pending_query_subscriptions(&mut self, subs: Vec<PendingQuerySubscription>) {
        self.pending_query_subscriptions.extend(subs);
    }

    /// Take pending query unsubscriptions for QueryManager to process.
    ///
    /// QueryManager will remove server-side QueryGraphs and forward upstream.
    pub fn take_pending_query_unsubscriptions(&mut self) -> Vec<PendingQueryUnsubscription> {
        std::mem::take(&mut self.pending_query_unsubscriptions)
    }

    /// Set the scope for a client's query subscription.
    ///
    /// Called by QueryManager after building QueryGraph and computing contributing ObjectIds.
    /// This triggers initial sync of all objects in the scope.
    pub fn set_client_query_scope(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        scope: HashSet<(ObjectId, BranchName)>,
        session: Option<Session>,
    ) {
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        // Collect all objects currently in any query scope
        let old_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        // Insert/update the query with the computed scope
        client.queries.insert(
            query_id,
            QueryScope {
                scope: scope.clone(),
                session,
            },
        );

        // Collect all objects now in any query scope
        let new_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        // Find newly visible (object, branch) pairs
        let newly_visible: Vec<(ObjectId, BranchName)> =
            new_scope.difference(&old_scope).cloned().collect();

        // Queue initial syncs for newly visible objects
        for (object_id, branch_name) in newly_visible {
            self.queue_initial_sync_to_client(client_id, object_id, branch_name);
        }
    }

    /// Send a QuerySubscription to all connected servers.
    ///
    /// Called by QueryManager when a client creates a subscription that should
    /// be forwarded upstream for server-side evaluation.
    pub fn send_query_subscription_to_servers(
        &mut self,
        query_id: QueryId,
        query: Query,
        session: Option<Session>,
    ) {
        for &server_id in self.servers.keys() {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::QuerySubscription {
                    query_id,
                    query: query.clone(),
                    session: session.clone(),
                },
            });
        }
    }

    /// Send a QueryUnsubscription to all connected servers.
    ///
    /// Called by QueryManager when a client unsubscribes from a synced query.
    pub fn send_query_unsubscription_to_servers(&mut self, query_id: QueryId) {
        for &server_id in self.servers.keys() {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::QueryUnsubscription { query_id },
            });
        }
    }

    /// Take pending QuerySettled notifications for QueryManager to process.
    pub fn take_pending_query_settled(&mut self) -> Vec<(QueryId, PersistenceTier)> {
        std::mem::take(&mut self.pending_query_settled)
    }

    /// Take received persistence acks since last call.
    /// Used by RuntimeCore to resolve `_persisted` mutation receivers.
    pub fn take_received_acks(&mut self) -> Vec<(CommitId, PersistenceTier)> {
        std::mem::take(&mut self.received_acks)
    }

    /// Emit a QuerySettled notification to a client.
    ///
    /// Called by QueryManager when a server subscription settles for the first time.
    pub fn emit_query_settled(&mut self, client_id: ClientId, query_id: QueryId) {
        if let Some(tier) = self.my_tier {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::QuerySettled { query_id, tier },
            });
        }
    }

    // ========================================================================
    // Pending Permission Checks
    // ========================================================================

    /// Take all pending permission checks for policy evaluation.
    ///
    /// Called by QueryManager to get writes that need permission evaluation.
    pub fn take_pending_permission_checks(&mut self) -> Vec<PendingPermissionCheck> {
        std::mem::take(&mut self.pending_permission_checks)
    }

    /// Approve a pending permission check, applying the payload.
    ///
    /// This takes the full PendingPermissionCheck since it was already taken
    /// from the queue by take_pending_permission_checks().
    pub fn approve_permission_check<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
    ) {
        self.apply_payload_from_client(storage, check.client_id, check.payload, true);
    }

    /// Reject a pending permission check, sending error back to client.
    ///
    /// This takes the full PendingPermissionCheck since it was already taken
    /// from the queue by take_pending_permission_checks().
    pub fn reject_permission_check(&mut self, check: PendingPermissionCheck, reason: String) {
        // Extract object_id and branch_name from payload
        let (object_id, branch_name) = match &check.payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            } => (*object_id, *branch_name),
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => (*object_id, *branch_name),
            _ => return,
        };

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(check.client_id),
            payload: SyncPayload::Error(SyncError::PermissionDenied {
                object_id,
                branch_name,
                reason,
            }),
        });
    }

    /// Queue a payload for permission checking.
    ///
    /// Called internally when a client write needs policy evaluation.
    #[allow(clippy::too_many_arguments)]
    fn queue_for_permission_check(
        &mut self,
        client_id: ClientId,
        payload: SyncPayload,
        session: Session,
        metadata: HashMap<String, String>,
        old_content: Option<Vec<u8>>,
        new_content: Option<Vec<u8>>,
        operation: Operation,
    ) -> PendingUpdateId {
        let id = PendingUpdateId(self.next_pending_id);
        self.next_pending_id += 1;
        self.pending_permission_checks.push(PendingPermissionCheck {
            id,
            client_id,
            payload,
            session,
            metadata,
            old_content,
            new_content,
            operation,
        });
        id
    }

    // ========================================================================
    // Internal: Sync Logic
    // ========================================================================

    /// Queue all existing objects to sync to a new server.
    fn queue_full_sync_to_server(&mut self, server_id: ServerId) {
        // Collect all object/branch/tips we need to sync
        let mut to_sync: Vec<BranchSyncData> = Vec::new();

        for (object_id, object) in &self.object_manager.objects {
            for (branch_name, branch) in &object.branches {
                to_sync.push((
                    *object_id,
                    object.metadata.clone(),
                    *branch_name,
                    branch.tips.iter().copied().collect(),
                ));
            }
        }

        // Now queue messages (borrowing self.servers mutably)
        for (object_id, metadata, branch_name, tips) in to_sync {
            self.queue_tips_to_server(server_id, object_id, metadata, branch_name, tips);
        }
    }

    /// Queue all existing objects to sync to a new client.
    ///
    /// This is called when a client first connects to send them all known data.
    /// For production, clients should use query subscriptions to scope what they receive.
    fn queue_full_sync_to_client(&mut self, client_id: ClientId) {
        // Collect all object/branch/tips we need to sync
        let mut to_sync: Vec<BranchSyncData> = Vec::new();

        for (object_id, object) in &self.object_manager.objects {
            for (branch_name, branch) in &object.branches {
                to_sync.push((
                    *object_id,
                    object.metadata.clone(),
                    *branch_name,
                    branch.tips.iter().copied().collect(),
                ));
            }
        }

        // Now queue messages
        for (object_id, metadata, branch_name, tips) in to_sync {
            self.queue_tips_to_client_full_sync(client_id, object_id, metadata, branch_name, tips);
        }
    }

    /// Queue tips to a client during full sync (bypasses scope check).
    fn queue_tips_to_client_full_sync(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        if metadata.get("nosync").map(|v| v == "true").unwrap_or(false) {
            return;
        }

        // Extract needed info without holding mutable borrow
        let (include_metadata, already_sent) = {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };
            let include_metadata = !client.sent_metadata.contains(&object_id);
            let already_sent = client
                .sent_tips
                .get(&(object_id, branch_name))
                .cloned()
                .unwrap_or_default();
            (include_metadata, already_sent)
        };

        // Collect commits
        let commits = self.collect_commits_to_send(object_id, &branch_name, &already_sent, &tips);

        if commits.is_empty() && !include_metadata {
            return;
        }

        // Now update client state
        let client = self.clients.get_mut(&client_id).unwrap();
        if include_metadata {
            client.sent_metadata.insert(object_id);
        }
        client.sent_tips.insert((object_id, branch_name), tips);

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
                branch_name,
                commits,
            },
        });
    }

    /// Queue tips to a server, including metadata if first time.
    fn queue_tips_to_server(
        &mut self,
        server_id: ServerId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        if metadata.get("nosync").map(|v| v == "true").unwrap_or(false) {
            return;
        }

        // Extract needed info without holding mutable borrow
        let (include_metadata, already_sent) = {
            let Some(server) = self.servers.get(&server_id) else {
                return;
            };
            let include_metadata = !server.sent_metadata.contains(&object_id);
            let already_sent = server
                .sent_tips
                .get(&(object_id, branch_name))
                .cloned()
                .unwrap_or_default();
            (include_metadata, already_sent)
        };

        // Collect commits we need to send
        let commits = self.collect_commits_to_send(object_id, &branch_name, &already_sent, &tips);

        if commits.is_empty() && !include_metadata {
            return; // Nothing new to send
        }

        // Now update server state
        let server = self.servers.get_mut(&server_id).unwrap();
        if include_metadata {
            server.sent_metadata.insert(object_id);
        }
        server.sent_tips.insert((object_id, branch_name), tips);

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::ObjectUpdated {
                object_id,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
                branch_name,
                commits,
            },
        });
    }

    /// Queue initial sync to a client for a newly visible object/branch.
    fn queue_initial_sync_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        branch_name: BranchName,
    ) {
        // Get current tips from object manager
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        self.queue_tips_to_client(client_id, object_id, metadata, branch_name, tips);
    }

    /// Queue tips to a client, including metadata if first time.
    fn queue_tips_to_client(
        &mut self,
        client_id: ClientId,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        branch_name: BranchName,
        tips: HashSet<CommitId>,
    ) {
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        if metadata.get("nosync").map(|v| v == "true").unwrap_or(false) {
            return;
        }

        // Extract needed info without holding mutable borrow
        let (in_scope, include_metadata, already_sent) = {
            let Some(client) = self.clients.get(&client_id) else {
                return;
            };

            // Check if in scope
            let in_scope = client.is_in_scope(object_id, &branch_name);

            let include_metadata = !client.sent_metadata.contains(&object_id);

            let already_sent = client
                .sent_tips
                .get(&(object_id, branch_name))
                .cloned()
                .unwrap_or_default();

            (in_scope, include_metadata, already_sent)
        };

        if !in_scope {
            return;
        }

        // Collect commits
        let commits = self.collect_commits_to_send(object_id, &branch_name, &already_sent, &tips);

        if commits.is_empty() && !include_metadata {
            return;
        }

        // Now update client state
        let client = self.clients.get_mut(&client_id).unwrap();
        if include_metadata {
            client.sent_metadata.insert(object_id);
        }
        client.sent_tips.insert((object_id, branch_name), tips);

        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id,
                metadata: if include_metadata {
                    Some(ObjectMetadata {
                        id: object_id,
                        metadata,
                    })
                } else {
                    None
                },
                branch_name,
                commits,
            },
        });
    }

    /// Collect commits needed to bring destination from already_sent to new_tips.
    /// Returns commits in topological order (parents first).
    fn collect_commits_to_send(
        &self,
        object_id: ObjectId,
        branch_name: &BranchName,
        already_sent: &HashSet<CommitId>,
        new_tips: &HashSet<CommitId>,
    ) -> Vec<Commit> {
        let Some(object) = self.object_manager.get(object_id) else {
            return Vec::new();
        };
        let Some(branch) = object.branches.get(branch_name) else {
            return Vec::new();
        };

        // If no commits yet sent, send all commits reachable from tips
        // If commits were sent, send only new commits (those not in ancestry of already_sent)

        let mut to_send: HashSet<CommitId> = HashSet::new();
        let mut to_visit: Vec<CommitId> = new_tips.iter().copied().collect();
        let mut visited: HashSet<CommitId> = HashSet::new();

        while let Some(commit_id) = to_visit.pop() {
            if visited.contains(&commit_id) {
                continue;
            }
            visited.insert(commit_id);

            // If already sent this commit (or its descendant), stop traversal
            if already_sent.contains(&commit_id) {
                continue;
            }

            to_send.insert(commit_id);

            // Visit parents
            if let Some(commit) = branch.commits.get(&commit_id) {
                for parent in &commit.parents {
                    if !visited.contains(parent) {
                        to_visit.push(*parent);
                    }
                }
            }
        }

        // Sort topologically (parents before children)
        self.topological_sort(&branch.commits, to_send)
    }

    /// Sort commits topologically (parents first).
    fn topological_sort(
        &self,
        all_commits: &HashMap<CommitId, Commit>,
        to_sort: HashSet<CommitId>,
    ) -> Vec<Commit> {
        let mut result = Vec::new();
        let mut remaining: HashSet<CommitId> = to_sort.clone();
        let mut added: HashSet<CommitId> = HashSet::new();

        // Simple iterative approach: repeatedly add commits whose parents are all added
        while !remaining.is_empty() {
            let mut progress = false;
            let current: Vec<CommitId> = remaining.iter().copied().collect();

            for commit_id in current {
                let Some(commit) = all_commits.get(&commit_id) else {
                    // Commit not found, skip
                    remaining.remove(&commit_id);
                    progress = true;
                    continue;
                };

                // Check if all parents in to_sort are already added
                let parents_ready = commit
                    .parents
                    .iter()
                    .all(|p| !to_sort.contains(p) || added.contains(p));

                if parents_ready {
                    result.push(commit.clone());
                    added.insert(commit_id);
                    remaining.remove(&commit_id);
                    progress = true;
                }
            }

            if !progress {
                // Cycle detected or missing parents, break to avoid infinite loop
                break;
            }
        }

        result
    }

    /// Process a single inbox entry.
    fn process_inbox_entry<H: Storage>(&mut self, storage: &mut H, entry: InboxEntry) {
        match entry.source {
            Source::Server(server_id) => {
                self.process_from_server(storage, server_id, entry.payload)
            }
            Source::Client(client_id) => {
                self.process_from_client(storage, client_id, entry.payload)
            }
        }
    }

    /// Process a payload from a server.
    fn process_from_server<H: Storage>(
        &mut self,
        storage: &mut H,
        server_id: ServerId,
        payload: SyncPayload,
    ) {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => {
                let persisted =
                    self.apply_object_updated(storage, object_id, metadata, branch_name, commits);

                // Emit ack back to server if we have a tier
                if let Some(tier) = self.my_tier
                    && !persisted.is_empty()
                {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Server(server_id),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: persisted,
                            tier,
                        },
                    });
                }

                // Forward to clients whose scope includes this object/branch
                self.forward_update_to_clients(object_id, branch_name);
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => {
                // Apply truncation locally
                let _ = self.object_manager.truncate_branch(
                    storage,
                    object_id,
                    branch_name,
                    tails.clone(),
                );

                // Forward to clients
                self.forward_truncation_to_clients(object_id, branch_name, tails);
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => {
                // Persist ack state and update in-memory
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    if let Some(commit) =
                        self.object_manager
                            .get_commit_mut(object_id, &branch_name, commit_id)
                    {
                        commit.ack_state.confirmed_tiers.insert(tier);
                    }
                    // Notify RuntimeCore of received ack
                    self.received_acks.push((commit_id, tier));
                }
                // Relay to interested clients
                let mut interested = HashSet::new();
                for &commit_id in &confirmed_commits {
                    if let Some(clients) = self.commit_interest.get(&commit_id) {
                        interested.extend(clients);
                    }
                }
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: confirmed_commits.clone(),
                            tier,
                        },
                    });
                }
            }
            SyncPayload::BlobResponse { blob_id, data } => {
                let _ = self.object_manager.put_blob(
                    storage,
                    blob_id.object_id,
                    blob_id.branch_name,
                    blob_id.commit_id,
                    data.clone(),
                );
            }
            SyncPayload::QuerySettled { query_id, tier } => {
                // Queue for local QueryManager to process
                self.pending_query_settled.push((query_id, tier));

                // Relay to interested clients
                if let Some(clients) = self.query_origin.get(&query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::QuerySettled { query_id, tier },
                        });
                    }
                }
            }
            SyncPayload::Error(err) => {
                // Log or handle server error
                eprintln!("Error from server {:?}: {:?}", server_id, err);
            }
            // Servers shouldn't send these to us
            SyncPayload::BlobRequest { .. }
            | SyncPayload::QuerySubscription { .. }
            | SyncPayload::QueryUnsubscription { .. } => {}
        }
    }

    /// Process a payload from a client.
    fn process_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
    ) {
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };

        match &payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                commits,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        // Trusted — apply directly
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        // User requires session
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        // User cannot write catalogue objects
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        // Row data — queue for ReBAC permission check
                        let (metadata, old_content) = self
                            .object_manager
                            .get(object_id)
                            .map(|obj| {
                                let old = obj
                                    .branches
                                    .get(&branch_name)
                                    .and_then(|branch| {
                                        branch
                                            .tips
                                            .iter()
                                            .next()
                                            .and_then(|tip_id| branch.commits.get(tip_id))
                                    })
                                    .map(|commit| commit.content.clone());
                                (obj.metadata.clone(), old)
                            })
                            .unwrap_or_default();
                        let new_content = commits.last().map(|c| c.content.clone());
                        let operation = if old_content.is_some() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content,
                            new_content,
                            operation,
                        );
                    }
                }
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        let (metadata, old_content) = self
                            .object_manager
                            .get(object_id)
                            .map(|obj| {
                                let old = obj
                                    .branches
                                    .get(&branch_name)
                                    .and_then(|branch| {
                                        branch
                                            .tips
                                            .iter()
                                            .next()
                                            .and_then(|tip_id| branch.commits.get(tip_id))
                                    })
                                    .map(|commit| commit.content.clone());
                                (obj.metadata.clone(), old)
                            })
                            .unwrap_or_default();
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content,
                            None,
                            Operation::Delete,
                        );
                    }
                }
            }
            SyncPayload::BlobRequest { blob_id } => {
                // Peer/Admin bypass scope check for blobs
                let has_permission = matches!(client.role, ClientRole::Peer | ClientRole::Admin)
                    || client
                        .queries
                        .values()
                        .flat_map(|q| q.scope.iter())
                        .any(|(obj_id, _)| *obj_id == blob_id.object_id);

                if !has_permission {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(client_id),
                        payload: SyncPayload::Error(SyncError::BlobAccessDenied {
                            blob_id: blob_id.clone(),
                        }),
                    });
                } else {
                    match self.object_manager.get_blob(&blob_id.content_hash) {
                        Ok(data) => {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::BlobResponse {
                                    blob_id: blob_id.clone(),
                                    data: data.to_vec(),
                                },
                            });
                        }
                        Err(_) => {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::BlobNotFound {
                                    blob_id: blob_id.clone(),
                                }),
                            });
                        }
                    }
                }
            }
            // Handle query subscription with full Query struct
            // Queue for QueryManager to process (SyncManager doesn't know about QueryGraph)
            SyncPayload::QuerySubscription {
                query_id,
                query,
                session,
            } => {
                // Track origin for QuerySettled relay
                self.query_origin
                    .entry(*query_id)
                    .or_default()
                    .insert(client_id);
                self.pending_query_subscriptions
                    .push(PendingQuerySubscription {
                        client_id,
                        query_id: *query_id,
                        query: query.clone(),
                        session: session.clone(),
                    });
            }
            // Handle query unsubscription
            // Queue for QueryManager to process (remove server-side QueryGraph, forward upstream)
            SyncPayload::QueryUnsubscription { query_id } => {
                if let Some(client) = self.clients.get_mut(&client_id) {
                    client.queries.remove(query_id);
                }
                // Clean up query origin
                if let Some(clients) = self.query_origin.get_mut(query_id) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.query_origin.remove(query_id);
                    }
                }
                self.pending_query_unsubscriptions
                    .push(PendingQueryUnsubscription {
                        client_id,
                        query_id: *query_id,
                    });
            }
            SyncPayload::PersistenceAck {
                object_id,
                branch_name,
                confirmed_commits,
                tier,
            } => {
                let object_id = *object_id;
                let branch_name = *branch_name;
                let tier = *tier;
                let confirmed_commits = confirmed_commits.clone();
                // A client relaying an ack (e.g. from a further-upstream tier)
                // Persist ack state and update in-memory
                for &commit_id in &confirmed_commits {
                    let _ = storage.store_ack_tier(commit_id, tier);
                    if let Some(commit) =
                        self.object_manager
                            .get_commit_mut(object_id, &branch_name, commit_id)
                    {
                        commit.ack_state.confirmed_tiers.insert(tier);
                    }
                    // Notify RuntimeCore of received ack
                    self.received_acks.push((commit_id, tier));
                }
                // Relay to interested clients (excluding the sender)
                let mut interested = HashSet::new();
                for &commit_id in &confirmed_commits {
                    if let Some(clients) = self.commit_interest.get(&commit_id) {
                        interested.extend(clients);
                    }
                }
                interested.remove(&client_id);
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: confirmed_commits.clone(),
                            tier,
                        },
                    });
                }
            }
            SyncPayload::QuerySettled { query_id, tier } => {
                // Client relaying a QuerySettled from downstream
                self.pending_query_settled.push((*query_id, *tier));
            }
            // Clients shouldn't send these
            SyncPayload::BlobResponse { .. } | SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    fn apply_payload_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        _was_pending: bool,
    ) {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => {
                // Track client interest for ack relay
                for commit in &commits {
                    self.commit_interest
                        .entry(commit.id())
                        .or_default()
                        .insert(client_id);
                }

                let persisted =
                    self.apply_object_updated(storage, object_id, metadata, branch_name, commits);

                // Emit ack back to client if we have a tier
                if let Some(tier) = self.my_tier
                    && !persisted.is_empty()
                {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(client_id),
                        payload: SyncPayload::PersistenceAck {
                            object_id,
                            branch_name,
                            confirmed_commits: persisted,
                            tier,
                        },
                    });
                }

                // Forward to servers
                self.forward_update_to_servers(object_id, branch_name);

                // Forward to other clients (not the sender)
                self.forward_update_to_clients_except(object_id, branch_name, client_id);
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => {
                let _ = self.object_manager.truncate_branch(
                    storage,
                    object_id,
                    branch_name,
                    tails.clone(),
                );

                // Forward to servers
                self.forward_truncation_to_servers(object_id, branch_name, tails.clone());

                // Forward to other clients
                self.forward_truncation_to_clients_except(object_id, branch_name, tails, client_id);
            }
            _ => {}
        }
    }

    /// Apply an ObjectUpdated payload to the local ObjectManager.
    /// Returns the set of newly persisted commit IDs (excludes duplicates).
    fn apply_object_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,
        branch_name: BranchName,
        commits: Vec<Commit>,
    ) -> HashSet<CommitId> {
        // If we don't have this object yet and metadata is provided, create it
        if self.object_manager.get(object_id).is_none() {
            if let Some(meta) = metadata {
                self.object_manager
                    .receive_object(storage, object_id, meta.metadata);
            } else {
                return HashSet::new();
            }
        }

        let mut persisted = HashSet::new();
        for commit in commits {
            let commit_id = commit.id();
            // Check if commit already exists before applying
            let already_exists = self
                .object_manager
                .get(object_id)
                .and_then(|obj| obj.branches.get(&branch_name))
                .is_some_and(|branch| branch.commits.contains_key(&commit_id));

            if self
                .object_manager
                .receive_commit(storage, object_id, branch_name, commit)
                .is_ok()
                && !already_exists
            {
                persisted.insert(commit_id);
            }
        }
        persisted
    }

    /// Forward an update to all servers.
    ///
    /// Call this after local writes to sync changes to connected servers.
    pub fn forward_update_to_servers(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        for server_id in server_ids {
            self.queue_tips_to_server(
                server_id,
                object_id,
                metadata.clone(),
                branch_name,
                tips.clone(),
            );
        }
    }

    /// Forward an update to clients whose scope includes this object/branch.
    fn forward_update_to_clients(&mut self, object_id: ObjectId, branch_name: BranchName) {
        self.forward_update_to_clients_except(object_id, branch_name, ClientId(Uuid::nil()));
    }

    /// Forward an update to clients except the specified one.
    fn forward_update_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        except: ClientId,
    ) {
        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| **id != except && client.is_in_scope(object_id, &branch_name))
            .map(|(id, _)| *id)
            .collect();

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips: HashSet<CommitId> = branch.tips.iter().copied().collect();
        let metadata = object.metadata.clone();

        for client_id in client_ids {
            self.queue_tips_to_client(
                client_id,
                object_id,
                metadata.clone(),
                branch_name,
                tips.clone(),
            );
        }
    }

    /// Forward a truncation to all servers.
    fn forward_truncation_to_servers(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    ) {
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        if object
            .metadata
            .get("nosync")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();

        for server_id in server_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name,
                    tails: tails.clone(),
                },
            });
        }
    }

    /// Forward a truncation to clients whose scope includes this object/branch.
    fn forward_truncation_to_clients(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    ) {
        self.forward_truncation_to_clients_except(
            object_id,
            branch_name,
            tails,
            ClientId(Uuid::nil()),
        );
    }

    /// Forward a truncation to clients except the specified one.
    fn forward_truncation_to_clients_except(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
        except: ClientId,
    ) {
        // Skip objects marked as nosync (local-only, e.g., index nodes)
        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        if object
            .metadata
            .get("nosync")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return;
        }

        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| **id != except && client.is_in_scope(object_id, &branch_name))
            .map(|(id, _)| *id)
            .collect();

        for client_id in client_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name,
                    tails: tails.clone(),
                },
            });
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use smallvec::smallvec;

    // ========================================================================
    // Phase 1: Foundation Tests
    // ========================================================================

    #[test]
    fn can_create_sync_manager() {
        let sm = SyncManager::new();
        assert!(sm.servers.is_empty());
        assert!(sm.clients.is_empty());
    }

    // ========================================================================
    // Phase 2: Server Sync Tests
    // ========================================================================

    #[test]
    fn add_server_receives_existing_objects() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create an object with a commit
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        // Add server
        let server_id = ServerId::new();
        sm.add_server(server_id);

        // Check outbox has the object update
        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => {
                assert_eq!(*object_id, obj_id);
                assert!(metadata.is_some()); // First sync includes metadata
                assert_eq!(branch_name.as_str(), "main");
                assert_eq!(commits.len(), 1);
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    #[test]
    fn local_commit_syncs_to_server() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();
        let server_id = ServerId::new();
        sm.add_server(server_id);

        // Clear initial outbox
        sm.take_outbox();

        // Create object and commit
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"content".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Manually trigger sync (in real usage, this would be called after local changes)
        sm.forward_update_to_servers(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { commits, .. } => {
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].id(), commit_id);
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    #[test]
    fn remove_server_stops_sync() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        sm.remove_server(server_id);

        // Create new object
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        sm.forward_update_to_servers(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // No server to send to
    }

    #[test]
    fn commits_sent_in_causal_order() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();

        // Create chain: c1 <- c2 <- c3
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();
        let c2 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![c1],
                b"c2".to_vec(),
                author,
                None,
            )
            .unwrap();
        let c3 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![c2],
                b"c3".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add server - should receive all commits in order
        let server_id = ServerId::new();
        sm.add_server(server_id);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { commits, .. } => {
                assert_eq!(commits.len(), 3);
                // Parents should come before children
                assert_eq!(commits[0].id(), c1);
                assert_eq!(commits[1].id(), c2);
                assert_eq!(commits[2].id(), c3);
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    // ========================================================================
    // Phase 3: Client Query Tests
    // ========================================================================

    #[test]
    fn client_with_query_receives_matching_objects() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        // Add client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0] {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::ObjectUpdated { object_id, .. },
            } => {
                assert_eq!(*id, client_id);
                assert_eq!(*object_id, obj_id);
            }
            _ => panic!("Expected ObjectUpdated to client"),
        }
    }

    #[test]
    fn client_without_query_receives_nothing() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        // Add client without query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty());
    }

    #[test]
    fn local_commit_in_scope_syncs_to_client() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Setup client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox(); // Clear initial sync

        // Add commit
        let commit_id = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"content".to_vec(),
                author,
                None,
            )
            .unwrap();

        sm.forward_update_to_clients(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { commits, .. } => {
                assert!(commits.iter().any(|c| c.id() == commit_id));
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    #[test]
    fn local_commit_out_of_scope_not_sent_to_client() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        // Client has query for obj1/main
        let obj1 = sm.object_manager.create(&mut io, None);
        let mut scope = HashSet::new();
        scope.insert((obj1, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Create commit on different object
        let obj2 = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj2,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        sm.forward_update_to_clients(obj2, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // obj2 not in client's scope
    }

    #[test]
    fn query_update_adds_scope_triggers_initial_sync() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create two objects
        let obj1 = sm.object_manager.create(&mut io, None);
        let obj2 = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj1,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        );
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj2,
            "main",
            vec![],
            b"c2".to_vec(),
            author,
            None,
        );

        // Client initially only has obj1
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashSet::new();
        scope.insert((obj1, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox(); // Clear obj1 sync

        // Update query to also include obj2
        let mut new_scope = HashSet::new();
        new_scope.insert((obj1, "main".into()));
        new_scope.insert((obj2, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), new_scope, None);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1); // Only obj2 (newly visible)

        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { object_id, .. } => {
                assert_eq!(*object_id, obj2);
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    #[test]
    fn query_removal_stops_future_updates() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Remove query by directly manipulating client state
        sm.clients
            .get_mut(&client_id)
            .unwrap()
            .queries
            .remove(&QueryId(1));

        // Add commit
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        );

        sm.forward_update_to_clients(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // Client no longer in scope
    }

    // ========================================================================
    // ReBAC Permission Enforcement Tests
    // ========================================================================

    #[test]
    fn peer_writes_applied_directly() {
        // Peer role writes are applied directly without permission checks
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        sm.set_client_role(client_id, ClientRole::Peer);

        sm.take_outbox();

        // Client pushes update - Peer role bypasses all checks
        let commit = Commit {
            parents: smallvec![c1],
            content: b"update".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // No pending permission checks — Peer bypasses
        let pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 0);

        // Verify commit was applied
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(tips.contains(&commit.id()));
    }

    #[test]
    fn admin_writes_catalogue_directly() {
        // Admin role can write catalogue objects directly
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        sm.set_client_role(client_id, ClientRole::Admin);

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: smallvec![],
            content: b"schema data".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        let mut cat_metadata = HashMap::new();
        cat_metadata.insert("type".to_string(), "catalogue_schema".to_string());

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: Some(ObjectMetadata {
                    id: obj_id,
                    metadata: cat_metadata,
                }),
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // No pending permission checks — Admin bypasses
        let pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 0);

        // Commit should be applied directly
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(tips.contains(&commit.id()));
    }

    #[test]
    fn admin_writes_row_directly() {
        // Admin role can write row objects directly without ReBAC
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        sm.set_client_role(client_id, ClientRole::Admin);
        sm.take_outbox();

        let commit = Commit {
            parents: smallvec![c1],
            content: b"updated".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        let pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 0);

        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(tips.contains(&commit.id()));
    }

    #[test]
    fn user_with_session_goes_to_permission_check() {
        // User with session sends row data → queued for ReBAC
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        sm.set_client_session(client_id, Session::new("alice"));
        sm.take_outbox();

        let commit = Commit {
            parents: smallvec![c1],
            content: b"update".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // Should be queued for permission check
        let pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].client_id, client_id);
        assert_eq!(pending[0].session.user_id, "alice");

        // Should NOT be applied yet
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(!tips.contains(&commit.id()));
    }

    #[test]
    fn user_without_session_rejected() {
        // User without session → SessionRequired error
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        // No session set — default User role

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: smallvec![],
            content: b"data".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit],
            },
        });

        sm.process_inbox(&mut io);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::SessionRequired {
                object_id,
                branch_name,
            }) => {
                assert_eq!(*object_id, obj_id);
                assert_eq!(branch_name.as_str(), "main");
            }
            other => panic!("Expected SessionRequired error, got {:?}", other),
        }

        // Object should not exist
        assert!(sm.object_manager.get(obj_id).is_none());
    }

    #[test]
    fn user_catalogue_write_rejected() {
        // User with session tries to write catalogue → CatalogueWriteDenied
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        sm.set_client_session(client_id, Session::new("alice"));

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: smallvec![],
            content: b"schema data".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        let mut cat_metadata = HashMap::new();
        cat_metadata.insert("type".to_string(), "catalogue_schema".to_string());

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: Some(ObjectMetadata {
                    id: obj_id,
                    metadata: cat_metadata,
                }),
                branch_name: "main".into(),
                commits: vec![commit],
            },
        });

        sm.process_inbox(&mut io);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::CatalogueWriteDenied {
                object_id,
                branch_name,
            }) => {
                assert_eq!(*object_id, obj_id);
                assert_eq!(branch_name.as_str(), "main");
            }
            other => panic!("Expected CatalogueWriteDenied error, got {:?}", other),
        }

        // Object should not exist
        assert!(sm.object_manager.get(obj_id).is_none());
    }

    #[test]
    fn add_client_with_full_sync_sets_peer_role() {
        let mut sm = SyncManager::new();
        let client_id = ClientId::new();
        sm.add_client_with_full_sync(client_id);

        let client = sm.get_client(client_id).unwrap();
        assert_eq!(client.role, ClientRole::Peer);
    }

    #[test]
    fn write_with_session_goes_to_pending_permission_checks() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        // Set session on client
        if let Some(client) = sm.clients.get_mut(&client_id) {
            client.session = Some(Session::new("user123"));
        }

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Client tries to push update
        let commit = Commit {
            parents: smallvec![c1],
            content: b"new_content".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // Should be in pending permission checks
        let pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].session.user_id, "user123");
        assert_eq!(pending[0].operation, Operation::Update);
        assert_eq!(pending[0].old_content, Some(b"original".to_vec()));
        assert_eq!(pending[0].new_content, Some(b"new_content".to_vec()));

        // Commit should NOT be applied yet
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(!tips.contains(&commit.id()));
    }

    #[test]
    fn approve_permission_check_applies_write() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        // Set session on client
        if let Some(client) = sm.clients.get_mut(&client_id) {
            client.session = Some(Session::new("user123"));
        }

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Client pushes update
        let commit = Commit {
            parents: smallvec![c1],
            content: b"allowed".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // Get pending check and approve it
        let mut pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 1);
        let check = pending.remove(0);

        sm.approve_permission_check(&mut io, check);

        // Commit should now be applied
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(tips.contains(&commit.id()));
    }

    #[test]
    fn reject_permission_check_sends_error() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"original".to_vec(),
                author,
                None,
            )
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        // Set session on client
        if let Some(client) = sm.clients.get_mut(&client_id) {
            client.session = Some(Session::new("user123"));
        }

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Client tries to push update
        let commit = Commit {
            parents: smallvec![c1],
            content: b"denied".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // Get pending check and reject it
        let mut pending = sm.take_pending_permission_checks();
        assert_eq!(pending.len(), 1);
        let check = pending.remove(0);

        sm.reject_permission_check(check, "access denied by policy".to_string());

        // Should get permission denied error
        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::PermissionDenied { reason, .. }) => {
                assert_eq!(reason, "access denied by policy");
            }
            _ => panic!("Expected PermissionDenied error"),
        }

        // Commit should NOT be applied
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(!tips.contains(&commit.id()));
    }

    #[test]
    fn server_update_forwarded_to_matching_clients() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Setup server
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        // Setup client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = ObjectId::new();
        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Server sends update
        let author = ObjectId::new();
        let commit = Commit {
            parents: smallvec![],
            content: b"from server".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: Some(ObjectMetadata {
                    id: obj_id,
                    metadata: HashMap::new(),
                }),
                branch_name: "main".into(),
                commits: vec![commit.clone()],
            },
        });

        sm.process_inbox(&mut io);

        // Client should receive forwarded update
        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0] {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::ObjectUpdated { object_id, .. },
            } => {
                assert_eq!(*id, client_id);
                assert_eq!(*object_id, obj_id);
            }
            _ => panic!("Expected ObjectUpdated to client"),
        }
    }

    // ========================================================================
    // Phase 6: Blob Handling Tests
    // ========================================================================

    #[test]
    fn blob_request_with_permission_returns_data() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object with blob
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"content".to_vec(),
                author,
                None,
            )
            .unwrap();

        let content_hash = sm
            .object_manager
            .put_blob(&mut io, obj_id, "main", commit_id, b"blob data".to_vec())
            .unwrap();

        // Client with read permission
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox();

        // Request blob
        let blob_id = BlobId {
            object_id: obj_id,
            branch_name: "main".into(),
            commit_id,
            content_hash,
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::BlobRequest {
                blob_id: blob_id.clone(),
            },
        });

        sm.process_inbox(&mut io);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::BlobResponse {
                blob_id: resp_id,
                data,
            } => {
                assert_eq!(*resp_id, blob_id);
                assert_eq!(data, b"blob data");
            }
            _ => panic!("Expected BlobResponse"),
        }
    }

    #[test]
    fn blob_request_without_permission_returns_error() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object with blob
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"content".to_vec(),
                author,
                None,
            )
            .unwrap();

        let content_hash = sm
            .object_manager
            .put_blob(&mut io, obj_id, "main", commit_id, b"blob data".to_vec())
            .unwrap();

        // Client WITHOUT permission for this object
        let client_id = ClientId::new();
        sm.add_client(client_id);
        // No query = no scope

        let blob_id = BlobId {
            object_id: obj_id,
            branch_name: "main".into(),
            commit_id,
            content_hash,
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::BlobRequest {
                blob_id: blob_id.clone(),
            },
        });

        sm.process_inbox(&mut io);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::BlobAccessDenied {
                blob_id: err_blob_id,
            }) => {
                assert_eq!(*err_blob_id, blob_id);
            }
            _ => panic!("Expected BlobAccessDenied error"),
        }
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn client_update_forwarded_to_server_and_other_clients() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Setup server
        let server_id = ServerId::new();
        sm.add_server(server_id);

        // Create object
        let obj_id = sm.object_manager.create(&mut io, None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"initial".to_vec(),
                author,
                None,
            )
            .unwrap();

        sm.take_outbox();

        // Setup two clients — client1 is Peer so writes go through directly
        let client1 = ClientId::new();
        let client2 = ClientId::new();
        sm.add_client(client1);
        sm.set_client_role(client1, ClientRole::Peer);
        sm.add_client(client2);

        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client1, QueryId(1), scope.clone(), None);
        sm.set_client_query_scope(client2, QueryId(1), scope, None);
        sm.take_outbox();

        // Client1 sends update
        let commit = Commit {
            parents: smallvec![c1],
            content: b"from client1".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client1),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: None,
                branch_name: "main".into(),
                commits: vec![commit],
            },
        });

        sm.process_inbox(&mut io);

        let outbox = sm.take_outbox();

        // Should have updates for: server + client2 (not client1)
        assert_eq!(outbox.len(), 2);

        let destinations: HashSet<_> = outbox.iter().map(|e| &e.destination).collect();
        assert!(destinations.contains(&Destination::Server(server_id)));
        assert!(destinations.contains(&Destination::Client(client2)));
        assert!(!destinations.contains(&Destination::Client(client1)));
    }

    #[test]
    fn metadata_sent_only_once_per_destination() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object BEFORE adding server
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("key".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Now add server - should receive existing object with metadata
        let server_id = ServerId::new();
        sm.add_server(server_id);

        let outbox = sm.take_outbox();

        // First message should have metadata
        assert_eq!(outbox.len(), 1);
        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { metadata, .. } => {
                assert!(metadata.is_some());
            }
            _ => panic!("Expected ObjectUpdated"),
        }

        // Add another commit (as child of c1)
        let _ = sm.object_manager.add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        );

        sm.forward_update_to_servers(obj_id, "main".into());

        let outbox = sm.take_outbox();

        // Second message should NOT have metadata
        match &outbox[0].payload {
            SyncPayload::ObjectUpdated { metadata, .. } => {
                assert!(metadata.is_none());
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    // ========================================================================
    // nosync Filtering Tests
    // ========================================================================

    #[test]
    fn nosync_object_not_synced_to_server() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object with nosync: "true" metadata
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("nosync".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        sm.object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add server - should NOT receive the nosync object
        let server_id = ServerId::new();
        sm.add_server(server_id);

        let outbox = sm.take_outbox();
        assert!(
            outbox.is_empty(),
            "nosync object should not be synced to server"
        );
    }

    #[test]
    fn nosync_object_not_synced_to_client() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object with nosync: "true" metadata
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("nosync".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        sm.object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add client with scope including the object
        let client_id = ClientId::new();
        sm.add_client(client_id);
        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);

        let outbox = sm.take_outbox();
        assert!(
            outbox.is_empty(),
            "nosync object should not be synced to client"
        );
    }

    #[test]
    fn nosync_object_update_not_forwarded_to_server() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create nosync object
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("nosync".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add server
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox(); // Clear any initial sync messages

        // Add another commit
        sm.object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![c1],
                b"c2".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Forward update to servers
        sm.forward_update_to_servers(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert!(
            outbox.is_empty(),
            "nosync object update should not be forwarded to server"
        );
    }

    #[test]
    fn nosync_object_truncation_not_forwarded_to_server() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create nosync object with some history
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("nosync".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();
        let c2 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![c1],
                b"c2".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add server
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox(); // Clear any initial sync messages

        // Forward truncation to servers (simulating what would happen after truncation)
        // The nosync check should prevent any message from being sent
        sm.forward_truncation_to_servers(obj_id, "main".into(), [c2].into_iter().collect());

        let outbox = sm.take_outbox();
        assert!(
            outbox.is_empty(),
            "nosync object truncation should not be forwarded to server"
        );
    }

    #[test]
    fn nosync_object_truncation_not_forwarded_to_client() {
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create nosync object with some history
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("nosync".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();
        let c2 = sm
            .object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![c1],
                b"c2".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add client with scope including the object
        let client_id = ClientId::new();
        sm.add_client(client_id);
        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));
        sm.set_client_query_scope(client_id, QueryId(1), scope, None);
        sm.take_outbox(); // Clear any initial sync messages

        // Forward truncation to clients (simulating what would happen after truncation)
        // The nosync check should prevent any message from being sent
        sm.forward_truncation_to_clients(obj_id, "main".into(), [c2].into_iter().collect());

        let outbox = sm.take_outbox();
        assert!(
            outbox.is_empty(),
            "nosync object truncation should not be forwarded to client"
        );
    }

    #[test]
    fn regular_object_still_syncs_to_server() {
        // Ensure regular objects without nosync still sync properly
        let mut sm = SyncManager::new();
        let mut io = MemoryStorage::new();

        // Create object WITHOUT nosync metadata
        let obj_id = sm.object_manager.create(
            &mut io,
            Some(
                [("key".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let author = ObjectId::new();
        sm.object_manager
            .add_commit(
                &mut io,
                obj_id,
                "main",
                vec![],
                b"c1".to_vec(),
                author,
                None,
            )
            .unwrap();

        // Add server - should receive the object
        let server_id = ServerId::new();
        sm.add_server(server_id);

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1, "regular object should sync to server");
    }

    // ========================================================================
    // Session Propagation Tests
    // ========================================================================

    #[test]
    fn set_query_scope_stores_session() {
        let mut sm = SyncManager::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = ObjectId::new();
        let mut scope = HashSet::new();
        scope.insert((obj_id, "main".into()));

        let session = Session::new("alice");
        sm.set_client_query_scope(client_id, QueryId(1), scope.clone(), Some(session));

        let client = sm.get_client(client_id).expect("client should exist");
        let query = client.queries.get(&QueryId(1)).expect("query should exist");
        assert_eq!(query.scope, scope);
        assert!(query.session.is_some());
        assert_eq!(query.session.as_ref().unwrap().user_id, "alice");
    }

    #[test]
    fn send_query_subscription_includes_session() {
        // Test that send_query_subscription_to_servers includes the session
        use crate::query_manager::query::QueryBuilder;

        let mut sm = SyncManager::new();

        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        let query = QueryBuilder::new("users").branch("main").build();
        let session = Session::new("alice");

        sm.send_query_subscription_to_servers(QueryId(1), query.clone(), Some(session.clone()));

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::QuerySubscription {
                query_id,
                query: sent_query,
                session: sent_session,
            } => {
                assert_eq!(*query_id, QueryId(1));
                assert_eq!(sent_query.table, query.table);
                assert!(sent_session.is_some());
                assert_eq!(sent_session.as_ref().unwrap().user_id, "alice");
            }
            _ => panic!("Expected QuerySubscription"),
        }
    }

    // ========================================================================
    // Phase 6a: Persistence Ack E2E Tests
    // ========================================================================

    /// Route messages between three tiers: A ↔ B ↔ C.
    ///
    /// A is a client of B, B is a client of C.
    /// Pumps until no messages remain or 10 rounds (whichever comes first).
    /// Auto-approves pending updates on B and C (simulates permissive server).
    fn pump_messages_3tier(
        a: &mut SyncManager,
        b: &mut SyncManager,
        c: &mut SyncManager,
        a_io: &mut MemoryStorage,
        b_io: &mut MemoryStorage,
        c_io: &mut MemoryStorage,
        a_client_of_b: ClientId,
        b_server_for_a: ServerId,
        b_client_of_c: ClientId,
        c_server_for_b: ServerId,
    ) {
        for _ in 0..10 {
            let mut any_messages = false;

            // A outbox → B inbox (A sends to server b_server_for_a → B receives from client a_client_of_b)
            for entry in a.take_outbox() {
                if entry.destination == Destination::Server(b_server_for_a) {
                    any_messages = true;
                    b.push_inbox(InboxEntry {
                        source: Source::Client(a_client_of_b),
                        payload: entry.payload,
                    });
                }
            }

            // B outbox → route to A or C
            for entry in b.take_outbox() {
                match &entry.destination {
                    Destination::Client(cid) if *cid == a_client_of_b => {
                        any_messages = true;
                        a.push_inbox(InboxEntry {
                            source: Source::Server(b_server_for_a),
                            payload: entry.payload,
                        });
                    }
                    Destination::Server(sid) if *sid == c_server_for_b => {
                        any_messages = true;
                        c.push_inbox(InboxEntry {
                            source: Source::Client(b_client_of_c),
                            payload: entry.payload,
                        });
                    }
                    _ => {}
                }
            }

            // C outbox → B inbox
            for entry in c.take_outbox() {
                if entry.destination == Destination::Client(b_client_of_c) {
                    any_messages = true;
                    b.push_inbox(InboxEntry {
                        source: Source::Server(c_server_for_b),
                        payload: entry.payload,
                    });
                }
            }

            if !any_messages && a.inbox.is_empty() && b.inbox.is_empty() && c.inbox.is_empty() {
                break;
            }

            a.process_inbox(a_io);
            b.process_inbox(b_io);
            c.process_inbox(c_io);
        }
    }

    /// Setup helper: creates A ↔ B ↔ C topology.
    /// Returns (a, b, c, a_io, b_io, c_io, ids...).
    struct ThreeTierSetup {
        a: SyncManager,
        b: SyncManager,
        c: SyncManager,
        a_io: MemoryStorage,
        b_io: MemoryStorage,
        c_io: MemoryStorage,
        a_client_of_b: ClientId,
        b_server_for_a: ServerId,
        b_client_of_c: ClientId,
        c_server_for_b: ServerId,
    }

    fn setup_3tier() -> ThreeTierSetup {
        let a_client_of_b = ClientId::new();
        let b_server_for_a = ServerId::new();
        let b_client_of_c = ClientId::new();
        let c_server_for_b = ServerId::new();

        let a = SyncManager::new();
        let mut b = SyncManager::new().with_tier(PersistenceTier::Worker);
        let mut c = SyncManager::new().with_tier(PersistenceTier::EdgeServer);

        // A connects to B as server
        // B adds A as client (with full sync for simplicity)
        b.add_client_with_full_sync(a_client_of_b);

        // B connects to C as server
        // C adds B as client (with full sync)
        c.add_client_with_full_sync(b_client_of_c);
        b.add_server(c_server_for_b);

        ThreeTierSetup {
            a,
            b,
            c,
            a_io: MemoryStorage::new(),
            b_io: MemoryStorage::new(),
            c_io: MemoryStorage::new(),
            a_client_of_b,
            b_server_for_a,
            b_client_of_c,
            c_server_for_b,
        }
    }

    fn make_test_commit(content: &[u8], parents: Vec<CommitId>) -> Commit {
        Commit {
            parents: parents.into(),
            content: content.to_vec(),
            timestamp: 1000,
            author: ObjectId::from_uuid(uuid::Uuid::nil()),
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
            ack_state: Default::default(),
        }
    }

    #[test]
    fn persistence_ack_direct() {
        let mut s = setup_3tier();

        // Create object on A and add commit
        let obj_id = s.a.object_manager.create(&mut s.a_io, None);
        let commit = make_test_commit(b"hello", vec![]);
        let commit_id = commit.id();
        let _ =
            s.a.object_manager
                .receive_commit(&mut s.a_io, obj_id, "main", commit);
        s.a.add_server(s.b_server_for_a);
        s.a.forward_update_to_servers(obj_id, "main".into());

        pump_messages_3tier(
            &mut s.a,
            &mut s.b,
            &mut s.c,
            &mut s.a_io,
            &mut s.b_io,
            &mut s.c_io,
            s.a_client_of_b,
            s.b_server_for_a,
            s.b_client_of_c,
            s.c_server_for_b,
        );

        // A should have received a PersistenceAck from B (tier=Worker)
        // Check A's processed state — the ack was processed by A's process_inbox
        // Since A has no tier, it doesn't re-emit, but it should have received the ack
        // Let's check: the ack was delivered to A's inbox and processed.
        // Since A processes PersistenceAck from server, it stores it in io and updates in-memory.
        let a_commit =
            s.a.object_manager
                .get_commit_mut(obj_id, &"main".into(), commit_id);
        assert!(a_commit.is_some(), "Commit should exist on A");
        assert!(
            a_commit
                .unwrap()
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::Worker),
            "A should have received Worker ack from B"
        );
    }

    #[test]
    fn persistence_ack_relay() {
        let mut s = setup_3tier();

        // Create object on A
        let obj_id = s.a.object_manager.create(&mut s.a_io, None);
        let commit = make_test_commit(b"hello-relay", vec![]);
        let commit_id = commit.id();
        let _ =
            s.a.object_manager
                .receive_commit(&mut s.a_io, obj_id, "main", commit);
        s.a.add_server(s.b_server_for_a);
        s.a.forward_update_to_servers(obj_id, "main".into());

        pump_messages_3tier(
            &mut s.a,
            &mut s.b,
            &mut s.c,
            &mut s.a_io,
            &mut s.b_io,
            &mut s.c_io,
            s.a_client_of_b,
            s.b_server_for_a,
            s.b_client_of_c,
            s.c_server_for_b,
        );

        // A should have received EdgeServer ack (relayed through B from C)
        let a_commit =
            s.a.object_manager
                .get_commit_mut(obj_id, &"main".into(), commit_id)
                .expect("Commit should exist on A");
        assert!(
            a_commit
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::EdgeServer),
            "A should have received EdgeServer ack relayed through B"
        );
    }

    #[test]
    fn persistence_ack_both_tiers() {
        let mut s = setup_3tier();

        let obj_id = s.a.object_manager.create(&mut s.a_io, None);
        let commit = make_test_commit(b"hello-both", vec![]);
        let commit_id = commit.id();
        let _ =
            s.a.object_manager
                .receive_commit(&mut s.a_io, obj_id, "main", commit);
        s.a.add_server(s.b_server_for_a);
        s.a.forward_update_to_servers(obj_id, "main".into());

        pump_messages_3tier(
            &mut s.a,
            &mut s.b,
            &mut s.c,
            &mut s.a_io,
            &mut s.b_io,
            &mut s.c_io,
            s.a_client_of_b,
            s.b_server_for_a,
            s.b_client_of_c,
            s.c_server_for_b,
        );

        let a_commit =
            s.a.object_manager
                .get_commit_mut(obj_id, &"main".into(), commit_id)
                .expect("Commit should exist on A");
        assert!(
            a_commit
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::Worker),
            "Should have Worker ack from B"
        );
        assert!(
            a_commit
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::EdgeServer),
            "Should have EdgeServer ack from C"
        );
    }

    #[test]
    fn persistence_ack_idempotent() {
        let mut s = setup_3tier();

        let obj_id = s.a.object_manager.create(&mut s.a_io, None);
        let commit = make_test_commit(b"idempotent", vec![]);
        let commit_id = commit.id();
        let _ =
            s.a.object_manager
                .receive_commit(&mut s.a_io, obj_id, "main", commit.clone());
        s.a.add_server(s.b_server_for_a);
        s.a.forward_update_to_servers(obj_id, "main".into());

        // Pump once
        pump_messages_3tier(
            &mut s.a,
            &mut s.b,
            &mut s.c,
            &mut s.a_io,
            &mut s.b_io,
            &mut s.c_io,
            s.a_client_of_b,
            s.b_server_for_a,
            s.b_client_of_c,
            s.c_server_for_b,
        );

        // Send the same commit again — should not panic
        s.a.forward_update_to_servers(obj_id, "main".into());

        pump_messages_3tier(
            &mut s.a,
            &mut s.b,
            &mut s.c,
            &mut s.a_io,
            &mut s.b_io,
            &mut s.c_io,
            s.a_client_of_b,
            s.b_server_for_a,
            s.b_client_of_c,
            s.c_server_for_b,
        );

        // Still has acks
        let a_commit =
            s.a.object_manager
                .get_commit_mut(obj_id, &"main".into(), commit_id)
                .expect("Commit should exist on A");
        assert!(
            a_commit
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::Worker)
        );
    }

    #[test]
    fn persistence_ack_cleanup_on_disconnect() {
        let mut s = setup_3tier();

        // A creates and sends a commit to B
        let obj_id = s.a.object_manager.create(&mut s.a_io, None);
        let commit = make_test_commit(b"disconnect-test", vec![]);
        let _ =
            s.a.object_manager
                .receive_commit(&mut s.a_io, obj_id, "main", commit);
        s.a.add_server(s.b_server_for_a);
        s.a.forward_update_to_servers(obj_id, "main".into());

        // Pump A→B only (one round)
        for entry in s.a.take_outbox() {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                s.b.push_inbox(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }
        s.b.process_inbox(&mut s.b_io);
        // B should now have interest for A's commits

        // Disconnect A from B
        s.b.remove_client(s.a_client_of_b);

        // C acks arrive at B — should not crash when trying to relay to disconnected A
        // Forward B→C and let C ack back
        for entry in s.b.take_outbox() {
            match &entry.destination {
                Destination::Server(sid) if *sid == s.c_server_for_b => {
                    s.c.push_inbox(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }
        s.c.process_inbox(&mut s.c_io);

        // C sends ack back to B
        for entry in s.c.take_outbox() {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                s.b.push_inbox(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }
        // Should not panic — A's interest was cleaned up
        s.b.process_inbox(&mut s.b_io);

        // B should not have any outbox entries for the disconnected client
        let outbox = s.b.take_outbox();
        for entry in &outbox {
            if let Destination::Client(cid) = &entry.destination {
                assert_ne!(
                    *cid, s.a_client_of_b,
                    "Should not relay to disconnected client"
                );
            }
        }
    }

    #[test]
    fn persistence_ack_survives_reload() {
        let mut io = MemoryStorage::new();

        let obj_id = ObjectId::new();
        io.create_object(obj_id, HashMap::new()).unwrap();

        let commit = make_test_commit(b"persist-test", vec![]);
        let commit_id = commit.id();
        io.append_commit(obj_id, &"main".into(), commit).unwrap();

        // Store ack tier
        io.store_ack_tier(commit_id, PersistenceTier::EdgeServer)
            .unwrap();

        // Load branch and verify ack_state is populated
        let loaded = io
            .load_branch(obj_id, &"main".into())
            .unwrap()
            .expect("Branch should exist");

        assert_eq!(loaded.commits.len(), 1);
        assert!(
            loaded.commits[0]
                .ack_state
                .confirmed_tiers
                .contains(&PersistenceTier::EdgeServer),
            "Loaded commit should have EdgeServer ack"
        );
    }

    #[test]
    fn ack_state_does_not_affect_commit_id_sync() {
        // Verify that commits with different ack_state have the same ID
        // (complementary to the unit test in commit.rs)
        let mut ack_state = crate::commit::CommitAckState::default();
        ack_state
            .confirmed_tiers
            .insert(PersistenceTier::CoreServer);

        let commit1 = make_test_commit(b"same-content", vec![]);
        let mut commit2 = make_test_commit(b"same-content", vec![]);
        commit2.ack_state = ack_state;

        assert_eq!(commit1.id(), commit2.id());
    }
}
