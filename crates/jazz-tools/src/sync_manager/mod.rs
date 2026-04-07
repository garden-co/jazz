use std::collections::{HashMap, HashSet};

use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::object_manager::{ObjectManager, VisibleRowUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::storage::Storage;

// Module declarations
pub mod forwarding;
pub mod inbox;
pub mod permissions;
pub mod sync_logic;
pub mod types;

#[cfg(test)]
mod tests;

// Re-export all public types
pub use types::*;

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
    pub(super) catalogue_entries: HashMap<ObjectId, CatalogueEntry>,
    pub(super) allow_unprivileged_schema_catalogue_writes: bool,

    pub(super) servers: HashMap<ServerId, ServerState>,
    pub(super) clients: HashMap<ClientId, ClientState>,

    pub(super) inbox: Vec<InboxEntry>,
    pub(super) outbox: Vec<OutboxEntry>,
    /// Pending permission checks awaiting policy evaluation.
    pub(super) pending_permission_checks: Vec<PendingPermissionCheck>,
    /// Pending query subscriptions awaiting QueryGraph building by QueryManager.
    pub(super) pending_query_subscriptions: Vec<PendingQuerySubscription>,
    /// Pending query unsubscriptions awaiting cleanup by QueryManager.
    pub(super) pending_query_unsubscriptions: Vec<PendingQueryUnsubscription>,
    /// Row updates applied through row-region-native sync.
    pub(super) pending_row_updates: Vec<VisibleRowUpdate>,
    /// Catalogue/system entry updates awaiting SchemaManager processing.
    pub(super) pending_catalogue_updates: Vec<CatalogueEntry>,

    pub(super) next_pending_id: u64,

    /// This node's durability identities (empty = don't emit durability notifications).
    pub(super) my_tiers: HashSet<DurabilityTier>,
    /// Tracks which clients are interested in row-version state updates.
    pub(super) row_version_interest: HashMap<RowVersionKey, HashSet<ClientId>>,

    /// Tracks which clients originated each query (for relaying QuerySettled).
    pub(super) query_origin: HashMap<QueryId, HashSet<ClientId>>,
    /// Pending QuerySettled notifications for QueryManager to process.
    pub(super) pending_query_settled: Vec<QueryId>,

    /// Row-version state acks received during inbox processing.
    pub(super) received_row_version_acks: Vec<(RowVersionKey, DurabilityTier)>,
}

impl std::fmt::Debug for SyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncManager")
            .field("object_manager", &self.object_manager)
            .field("catalogue_entries", &self.catalogue_entries)
            .field(
                "allow_unprivileged_schema_catalogue_writes",
                &self.allow_unprivileged_schema_catalogue_writes,
            )
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
            .field("pending_row_updates", &self.pending_row_updates)
            .field("pending_catalogue_updates", &self.pending_catalogue_updates)
            .field("next_pending_id", &self.next_pending_id)
            .field("my_tiers", &self.my_tiers)
            .field("row_version_interest", &self.row_version_interest)
            .field("query_origin", &self.query_origin)
            .field("pending_query_settled", &self.pending_query_settled)
            .field("received_row_version_acks", &self.received_row_version_acks)
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
        Self::with_object_manager(ObjectManager::new())
    }

    /// Create with an existing ObjectManager.
    pub fn with_object_manager(object_manager: ObjectManager) -> Self {
        Self {
            object_manager,
            catalogue_entries: HashMap::new(),
            allow_unprivileged_schema_catalogue_writes: false,
            servers: HashMap::new(),
            clients: HashMap::new(),
            inbox: Vec::new(),
            outbox: Vec::new(),
            pending_permission_checks: Vec::new(),
            pending_query_subscriptions: Vec::new(),
            pending_query_unsubscriptions: Vec::new(),
            pending_row_updates: Vec::new(),
            pending_catalogue_updates: Vec::new(),
            next_pending_id: 0,
            my_tiers: HashSet::new(),
            row_version_interest: HashMap::new(),
            query_origin: HashMap::new(),
            pending_query_settled: Vec::new(),
            received_row_version_acks: Vec::new(),
        }
    }

    /// Add a durability identity for this node (enables durability notifications).
    pub fn with_durability_tier(mut self, tier: DurabilityTier) -> Self {
        self.my_tiers.insert(tier);
        self
    }

    /// Allow authenticated user clients to publish structural schema catalogue
    /// objects directly. Intended for development servers only.
    pub fn with_unprivileged_schema_catalogue_writes(mut self) -> Self {
        self.allow_unprivileged_schema_catalogue_writes = true;
        self
    }

    /// Add multiple durability identities for this node.
    pub fn with_durability_tiers<I>(mut self, tiers: I) -> Self
    where
        I: IntoIterator<Item = DurabilityTier>,
    {
        self.my_tiers.extend(tiers);
        self
    }

    /// True when this runtime instance represents a durability tier identity
    /// (worker/edge/global) rather than a top-level client.
    pub fn has_durability_identity(&self) -> bool {
        !self.my_tiers.is_empty()
    }

    /// True when this node can satisfy acknowledgements for the requested tier
    /// using one of its local durability identities.
    pub fn has_local_durability_at_least(&self, requested_tier: DurabilityTier) -> bool {
        self.my_tiers
            .iter()
            .any(|local_tier| *local_tier >= requested_tier)
    }

    /// Return this node's local durability identities.
    pub fn local_durability_tiers(&self) -> HashSet<DurabilityTier> {
        self.my_tiers.clone()
    }

    /// Return the strongest durability tier this node can attest to locally.
    pub fn max_local_durability_tier(&self) -> Option<DurabilityTier> {
        self.my_tiers.iter().copied().max()
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Add a server connection using storage-backed current-state replay.
    pub fn add_server_with_storage<H: Storage>(
        &mut self,
        server_id: ServerId,
        skip_catalogue_sync: bool,
        storage: &H,
    ) {
        self.servers.insert(server_id, ServerState::default());
        self.queue_full_sync_to_server_from_storage(server_id, storage);
        if !skip_catalogue_sync {
            self.queue_catalogue_sync_to_server_from_storage(server_id, storage);
        }
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.servers.remove(&server_id);
    }

    /// Add a client connection using storage-backed catalogue replay.
    pub fn add_client_with_storage<H: Storage>(&mut self, storage: &H, client_id: ClientId) {
        self.clients.insert(client_id, ClientState::default());
        self.queue_catalogue_sync_to_client_from_storage(client_id, storage);
    }

    /// Remove a client connection and all associated state.
    ///
    /// Returns `false` if the client has unprocessed inbox entries — the
    /// caller should retry later to avoid dropping data that hasn't been
    /// persisted to storage yet.
    pub fn remove_client(&mut self, client_id: ClientId) -> bool {
        let has_inbox = self
            .inbox
            .iter()
            .any(|e| e.source == Source::Client(client_id));

        if has_inbox {
            tracing::warn!(
                %client_id,
                "skipping reap: client has unprocessed inbox entries"
            );
            return false;
        }

        self.clients.remove(&client_id);
        // Clean up interest map
        self.row_version_interest.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        // Clean up query origin map
        self.query_origin.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        // Clean up pending queues
        self.pending_permission_checks
            .retain(|c| c.client_id != client_id);
        self.pending_query_subscriptions
            .retain(|s| s.client_id != client_id);
        self.pending_query_unsubscriptions
            .retain(|u| u.client_id != client_id);
        // Drop queued outbox messages for this client
        self.outbox
            .retain(|e| e.destination != Destination::Client(client_id));
        true
    }

    /// Get server state.
    pub fn get_server(&self, server_id: ServerId) -> Option<&ServerState> {
        self.servers.get(&server_id)
    }

    pub fn has_servers(&self) -> bool {
        !self.servers.is_empty()
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

    /// Storage-backed version of `set_client_query_scope` that can replay row
    /// objects directly from visible row regions.
    pub fn set_client_query_scope_with_storage<H: Storage + ?Sized>(
        &mut self,
        storage: &H,
        client_id: ClientId,
        query_id: QueryId,
        scope: HashSet<(ObjectId, BranchName)>,
        session: Option<Session>,
    ) {
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        let old_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        client.queries.insert(
            query_id,
            QueryScope {
                scope: scope.clone(),
                session,
            },
        );

        let new_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        let no_longer_visible: HashSet<(ObjectId, BranchName)> =
            old_scope.difference(&new_scope).cloned().collect();
        let newly_visible: Vec<(ObjectId, BranchName)> =
            new_scope.difference(&old_scope).cloned().collect();

        self.prune_client_scope_tracking(client_id, &no_longer_visible);

        for (object_id, branch_name) in newly_visible {
            self.queue_initial_sync_to_client_with_storage(
                storage,
                client_id,
                object_id,
                branch_name,
            );
        }
    }

    /// Drop a client's query subscription state.
    ///
    /// Removes per-query scope and origin tracking.
    pub fn drop_client_query_subscription(&mut self, client_id: ClientId, query_id: QueryId) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            let old_scope: HashSet<(ObjectId, BranchName)> = client
                .queries
                .values()
                .flat_map(|q| q.scope.iter().cloned())
                .collect();
            client.queries.remove(&query_id);
            let new_scope: HashSet<(ObjectId, BranchName)> = client
                .queries
                .values()
                .flat_map(|q| q.scope.iter().cloned())
                .collect();
            let no_longer_visible: HashSet<(ObjectId, BranchName)> =
                old_scope.difference(&new_scope).cloned().collect();
            self.prune_client_scope_tracking(client_id, &no_longer_visible);
        }

        if let Some(clients) = self.query_origin.get_mut(&query_id) {
            clients.remove(&client_id);
            if clients.is_empty() {
                self.query_origin.remove(&query_id);
            }
        }
    }

    fn prune_client_scope_tracking(
        &mut self,
        client_id: ClientId,
        removed_scope: &HashSet<(ObjectId, BranchName)>,
    ) {
        if removed_scope.is_empty() {
            return;
        }

        let mut removed_row_versions = Vec::new();
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        for &(object_id, branch_name) in removed_scope {
            if let Some(version_ids) = client.sent_row_versions.remove(&(object_id, branch_name)) {
                removed_row_versions.extend(
                    version_ids
                        .into_iter()
                        .map(|version_id| RowVersionKey::new(object_id, branch_name, version_id)),
                );
            }
        }

        for key in removed_row_versions {
            if let Some(clients) = self.row_version_interest.get_mut(&key) {
                clients.remove(&client_id);
                if clients.is_empty() {
                    self.row_version_interest.remove(&key);
                }
            }
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
        propagation: QueryPropagation,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        for server_id in server_ids {
            self.send_query_subscription_to_server(
                server_id,
                query_id,
                query.clone(),
                session.clone(),
                propagation,
            );
        }
    }

    /// Send a QuerySubscription to one specific server.
    ///
    /// Used when replaying existing subscriptions after a late server connect.
    pub fn send_query_subscription_to_server(
        &mut self,
        server_id: ServerId,
        query_id: QueryId,
        query: Query,
        session: Option<Session>,
        propagation: QueryPropagation,
    ) {
        if !self.servers.contains_key(&server_id) {
            return;
        }

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::QuerySubscription {
                query_id,
                query: Box::new(query),
                session,
                propagation,
            },
        });
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
    pub fn take_pending_query_settled(&mut self) -> Vec<QueryId> {
        std::mem::take(&mut self.pending_query_settled)
    }

    /// Take received row-version persistence state since last call.
    /// Used by RuntimeCore to resolve row `_persisted` mutation receivers.
    pub fn take_received_row_version_acks(&mut self) -> Vec<(RowVersionKey, DurabilityTier)> {
        std::mem::take(&mut self.received_row_version_acks)
    }

    /// Take pending row updates for QueryManager to materialize into indices
    /// and subscriptions.
    pub fn take_pending_row_updates(&mut self) -> Vec<VisibleRowUpdate> {
        std::mem::take(&mut self.pending_row_updates)
    }

    /// Take pending catalogue/system entry updates for QueryManager/SchemaManager.
    pub fn take_pending_catalogue_updates(&mut self) -> Vec<CatalogueEntry> {
        std::mem::take(&mut self.pending_catalogue_updates)
    }

    /// Requeue row updates that could not be processed yet, typically because
    /// the corresponding schema has not been activated yet.
    pub fn requeue_pending_row_updates(&mut self, updates: Vec<VisibleRowUpdate>) {
        self.pending_row_updates.extend(updates);
    }

    /// Emit a QuerySettled notification to a client.
    ///
    /// Called by QueryManager when a server subscription settles for the first time.
    pub fn emit_query_settled(&mut self, client_id: ClientId, query_id: QueryId) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::QuerySettled {
                query_id,
                through_seq: 0,
            },
        });
    }

    /// Emit a schema warning to a client.
    pub fn emit_schema_warning(&mut self, client_id: ClientId, warning: SchemaWarning) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::SchemaWarning(warning),
        });
    }

    /// Emit a query subscription rejection error to a client.
    pub fn emit_query_subscription_rejected(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        reason: impl Into<String>,
    ) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                query_id,
                reason: reason.into(),
            }),
        });
    }
}
