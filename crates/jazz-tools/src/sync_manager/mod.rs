use std::collections::{HashMap, HashSet};
use std::time::Duration;

// Use web_time::Instant so this compiles on wasm32-unknown-unknown; std's
// Instant panics in browsers. Duration has no platform dependency.
use web_time::Instant;

use crate::catalogue::CatalogueEntry;
use crate::monotonic_clock::MonotonicClock;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::row_histories::RowVisibilityChange;
use crate::storage::Storage;

/// How long an installed transport may sit in `pending_servers` before callers
/// treat it as offline. Caps the initial-frontier hold introduced by the
/// "Hold remote query frontier while transport connects" change — without a
/// bound, a never-connecting transport stalls every first subscription
/// delivery forever.
pub const PENDING_SERVER_TIMEOUT: Duration = Duration::from_secs(2);

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

const HASH_MAP_ENTRY_OVERHEAD: usize = 48;
const HASH_SET_ENTRY_OVERHEAD: usize = 32;

/// Manages synchronization state atop storage-backed row and catalogue state.
///
/// Coordinates:
/// - Upstream servers (trusted, receive all our objects)
/// - Downstream clients (untrusted, receive query-filtered subsets)
#[derive(Clone)]
pub struct SyncManager {
    pub(super) clock: MonotonicClock,
    pub(super) catalogue_entries: HashMap<ObjectId, CatalogueEntry>,
    pub(super) allow_unprivileged_schema_catalogue_writes: bool,

    pub(super) servers: HashMap<ServerId, ServerState>,
    /// Servers whose transport handshake is still in flight. Each entry records
    /// when the transport was installed so we can time it out.
    pub(super) pending_servers: HashMap<ServerId, Instant>,
    pub(super) clients: HashMap<ClientId, ClientState>,

    pub(super) inbox: Vec<InboxEntry>,
    pub(super) outbox: Vec<OutboxEntry>,
    /// Pending permission checks awaiting policy evaluation.
    pub(super) pending_permission_checks: Vec<PendingPermissionCheck>,
    /// Pending query subscriptions awaiting QueryGraph building by QueryManager.
    pub(super) pending_query_subscriptions: Vec<PendingQuerySubscription>,
    /// Pending query unsubscriptions awaiting cleanup by QueryManager.
    pub(super) pending_query_unsubscriptions: Vec<PendingQueryUnsubscription>,
    /// Row visibility changes applied through row-history sync.
    pub(super) pending_row_visibility_changes: Vec<RowVisibilityChange>,
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
    pub(super) pending_query_settled: Vec<PendingQuerySettled>,

    /// Row-version state acks received during inbox processing.
    pub(super) received_row_version_acks: Vec<(RowVersionKey, DurabilityTier)>,
}

impl std::fmt::Debug for SyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncManager")
            .field("clock", &self.clock)
            .field("catalogue_entries", &self.catalogue_entries)
            .field(
                "allow_unprivileged_schema_catalogue_writes",
                &self.allow_unprivileged_schema_catalogue_writes,
            )
            .field("servers", &self.servers)
            .field("pending_servers", &self.pending_servers)
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
            .field(
                "pending_row_visibility_changes",
                &self.pending_row_visibility_changes,
            )
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

fn short_hash(hash: &impl ToString) -> String {
    hash.to_string().chars().take(12).collect()
}

pub(crate) fn log_schema_warning(
    warning: &SchemaWarning,
    origin: Option<&str>,
    subscription_id: Option<u64>,
) {
    tracing::warn!(
        origin = origin,
        sub_id = subscription_id,
        query_id = warning.query_id.0,
        table = warning.table_name,
        row_count = warning.row_count,
        from_hash = %warning.from_hash,
        to_hash = %warning.to_hash,
        "Detected {} rows of {} with differing schema versions. To ensure data visibility and forward/backward compatibility, run `npx jazz-tools@alpha schema export --schema-hash {}`. Then generate a migration with `npx jazz-tools@alpha migrations create --fromHash {} --toHash <targetHash>`.",
        warning.row_count,
        warning.table_name,
        short_hash(&warning.from_hash),
        short_hash(&warning.from_hash),
    );
}

pub(crate) fn log_connection_schema_diagnostics(
    diagnostics: &ConnectionSchemaDiagnostics,
    origin: Option<&str>,
) {
    let client_hash = short_hash(&diagnostics.client_schema_hash);

    if let Some(permissions_hash) = diagnostics.disconnected_permissions_schema_hash {
        let permissions_hash = short_hash(&permissions_hash);
        tracing::error!(
            origin = origin,
            client_schema_hash = %client_hash,
            permissions_schema_hash = %permissions_hash,
            "Your declared schema {} is disconnected from the schema used to enforce permissions: {}. Reads and writes may fail until you add a migration. To recover, run `npx jazz-tools@alpha migrations create --fromHash {} --toHash {}`.",
            client_hash,
            permissions_hash,
            permissions_hash,
            client_hash,
        );
    }

    if !diagnostics.unreachable_schema_hashes.is_empty() {
        let unreachable_hashes: Vec<String> = diagnostics
            .unreachable_schema_hashes
            .iter()
            .map(short_hash)
            .collect();
        tracing::warn!(
            origin = origin,
            client_schema_hash = %client_hash,
            unreachable_schema_hashes = ?unreachable_hashes,
            "Server knows schema branches that are unreachable from your declared schema {}: {}. Some data may be missing from reads until you add migrations. To recover, run `npx jazz-tools@alpha migrations create --fromHash <unreachableHash> --toHash {}` for each listed schema.",
            client_hash,
            unreachable_hashes.join(", "),
            client_hash,
        );
    }
}

fn serialized_size<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

fn estimate_string_map(map: &HashMap<String, String>) -> usize {
    map.iter()
        .map(|(key, value)| key.len() + value.len() + HASH_MAP_ENTRY_OVERHEAD)
        .sum()
}

fn estimate_branch_name(branch_name: &BranchName) -> usize {
    std::mem::size_of::<BranchName>() + branch_name.as_str().len()
}

fn estimate_session(session: &Session) -> usize {
    std::mem::size_of::<Session>() + serialized_size(session)
}

fn estimate_query(query: &Query) -> usize {
    std::mem::size_of::<Query>() + serialized_size(query)
}

fn estimate_sync_payload(payload: &SyncPayload) -> usize {
    std::mem::size_of::<SyncPayload>() + serialized_size(payload)
}

fn estimate_catalogue_entry(entry: &CatalogueEntry) -> usize {
    std::mem::size_of::<CatalogueEntry>() + serialized_size(entry)
}

fn estimate_query_scope(scope: &QueryScope) -> usize {
    std::mem::size_of::<QueryScope>()
        + scope
            .scope
            .iter()
            .map(|(object_id, branch_name)| {
                std::mem::size_of_val(object_id)
                    + estimate_branch_name(branch_name)
                    + HASH_SET_ENTRY_OVERHEAD
            })
            .sum::<usize>()
        + scope.session.as_ref().map_or(0, estimate_session)
}

fn estimate_version_tracking(
    versions: &HashMap<(ObjectId, BranchName), HashSet<crate::commit::CommitId>>,
) -> usize {
    versions
        .iter()
        .map(|((object_id, branch_name), commit_ids)| {
            std::mem::size_of_val(object_id)
                + estimate_branch_name(branch_name)
                + HASH_MAP_ENTRY_OVERHEAD
                + commit_ids
                    .iter()
                    .map(|commit_id| std::mem::size_of_val(commit_id) + HASH_SET_ENTRY_OVERHEAD)
                    .sum::<usize>()
        })
        .sum()
}

fn estimate_server_state(state: &ServerState) -> usize {
    std::mem::size_of::<ServerState>()
        + estimate_version_tracking(&state.sent_row_versions)
        + state
            .sent_metadata
            .iter()
            .map(|object_id| std::mem::size_of_val(object_id) + HASH_SET_ENTRY_OVERHEAD)
            .sum::<usize>()
}

fn estimate_client_state(state: &ClientState) -> usize {
    std::mem::size_of::<ClientState>()
        + state.session.as_ref().map_or(0, estimate_session)
        + state
            .queries
            .iter()
            .map(|(query_id, scope)| {
                std::mem::size_of_val(query_id)
                    + HASH_MAP_ENTRY_OVERHEAD
                    + estimate_query_scope(scope)
            })
            .sum::<usize>()
        + estimate_version_tracking(&state.sent_row_versions)
        + state
            .sent_metadata
            .iter()
            .map(|object_id| std::mem::size_of_val(object_id) + HASH_SET_ENTRY_OVERHEAD)
            .sum::<usize>()
}

fn estimate_outbox_entry(entry: &OutboxEntry) -> usize {
    std::mem::size_of::<OutboxEntry>() + estimate_sync_payload(&entry.payload)
}

fn estimate_inbox_entry(entry: &InboxEntry) -> usize {
    std::mem::size_of::<InboxEntry>() + estimate_sync_payload(&entry.payload)
}

fn estimate_pending_query_subscription(subscription: &PendingQuerySubscription) -> usize {
    std::mem::size_of::<PendingQuerySubscription>()
        + estimate_query(&subscription.query)
        + subscription.session.as_ref().map_or(0, estimate_session)
}

fn estimate_pending_permission_check(check: &PendingPermissionCheck) -> usize {
    std::mem::size_of::<PendingPermissionCheck>()
        + estimate_sync_payload(&check.payload)
        + estimate_session(&check.session)
        + estimate_string_map(&check.metadata)
        + check.old_content.as_ref().map_or(0, Vec::len)
        + check.new_content.as_ref().map_or(0, Vec::len)
}

fn estimate_row_visibility_change(change: &RowVisibilityChange) -> usize {
    std::mem::size_of::<RowVisibilityChange>()
        + serialized_size(&change.row_locator)
        + serialized_size(&change.row)
        + change.previous_row.as_ref().map_or(0, serialized_size)
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            clock: MonotonicClock::new(),
            catalogue_entries: HashMap::new(),
            allow_unprivileged_schema_catalogue_writes: false,
            servers: HashMap::new(),
            pending_servers: HashMap::new(),
            clients: HashMap::new(),
            inbox: Vec::new(),
            outbox: Vec::new(),
            pending_permission_checks: Vec::new(),
            pending_query_subscriptions: Vec::new(),
            pending_query_unsubscriptions: Vec::new(),
            pending_row_visibility_changes: Vec::new(),
            pending_catalogue_updates: Vec::new(),
            next_pending_id: 0,
            my_tiers: HashSet::new(),
            row_version_interest: HashMap::new(),
            query_origin: HashMap::new(),
            pending_query_settled: Vec::new(),
            received_row_version_acks: Vec::new(),
        }
    }

    pub fn reserve_timestamp(&mut self) -> u64 {
        self.clock.reserve_timestamp()
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

    /// Calculate memory usage breakdown for profiling.
    ///
    /// Returns a tuple: (row_objects, index_objects, subscriptions, outbox_inbox, total).
    /// This is a rough estimate based on sync-layer container state and payload sizes.
    pub fn memory_size(&self) -> (usize, usize, usize, usize, usize) {
        let row_objects = self
            .catalogue_entries
            .iter()
            .map(|(object_id, entry)| {
                std::mem::size_of_val(object_id)
                    + HASH_MAP_ENTRY_OVERHEAD
                    + estimate_catalogue_entry(entry)
            })
            .sum::<usize>()
            + self
                .row_version_interest
                .iter()
                .map(|(row_version_key, client_ids)| {
                    std::mem::size_of_val(row_version_key)
                        + HASH_MAP_ENTRY_OVERHEAD
                        + client_ids
                            .iter()
                            .map(|client_id| {
                                std::mem::size_of_val(client_id) + HASH_SET_ENTRY_OVERHEAD
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self
                .received_row_version_acks
                .iter()
                .map(|(row_version_key, durability_tier)| {
                    std::mem::size_of_val(row_version_key) + std::mem::size_of_val(durability_tier)
                })
                .sum::<usize>();

        let index_objects = 0usize;

        let subscriptions = self
            .servers
            .iter()
            .map(|(server_id, state)| {
                std::mem::size_of_val(server_id)
                    + HASH_MAP_ENTRY_OVERHEAD
                    + estimate_server_state(state)
            })
            .sum::<usize>()
            + self
                .clients
                .iter()
                .map(|(client_id, state)| {
                    std::mem::size_of_val(client_id)
                        + HASH_MAP_ENTRY_OVERHEAD
                        + estimate_client_state(state)
                })
                .sum::<usize>()
            + self
                .my_tiers
                .iter()
                .map(|tier| std::mem::size_of_val(tier) + HASH_SET_ENTRY_OVERHEAD)
                .sum::<usize>()
            + self
                .query_origin
                .iter()
                .map(|(query_id, client_ids)| {
                    std::mem::size_of_val(query_id)
                        + HASH_MAP_ENTRY_OVERHEAD
                        + client_ids
                            .iter()
                            .map(|client_id| {
                                std::mem::size_of_val(client_id) + HASH_SET_ENTRY_OVERHEAD
                            })
                            .sum::<usize>()
                })
                .sum::<usize>();

        let outbox_inbox = self.outbox.iter().map(estimate_outbox_entry).sum::<usize>()
            + self.inbox.iter().map(estimate_inbox_entry).sum::<usize>()
            + self
                .pending_permission_checks
                .iter()
                .map(estimate_pending_permission_check)
                .sum::<usize>()
            + self
                .pending_query_subscriptions
                .iter()
                .map(estimate_pending_query_subscription)
                .sum::<usize>()
            + self.pending_query_unsubscriptions.len()
                * std::mem::size_of::<PendingQueryUnsubscription>()
            + self
                .pending_row_visibility_changes
                .iter()
                .map(estimate_row_visibility_change)
                .sum::<usize>()
            + self
                .pending_catalogue_updates
                .iter()
                .map(estimate_catalogue_entry)
                .sum::<usize>()
            + self.pending_query_settled.len() * std::mem::size_of::<PendingQuerySettled>();

        let total = row_objects + index_objects + subscriptions + outbox_inbox;
        (
            row_objects,
            index_objects,
            subscriptions,
            outbox_inbox,
            total,
        )
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
        self.pending_servers.remove(&server_id);
        self.servers.insert(server_id, ServerState::default());
        self.queue_full_sync_to_server_from_storage(server_id, storage);
        if !skip_catalogue_sync {
            self.queue_catalogue_sync_to_server_from_storage(server_id, storage);
        }
    }

    /// Mark a transport-owned server as pending while the connection handshake runs.
    pub fn add_pending_server(&mut self, server_id: ServerId) {
        if self.servers.contains_key(&server_id) {
            return;
        }
        self.pending_servers.insert(server_id, Instant::now());
    }

    /// Drop the pending flag for a transport whose first connect/handshake
    /// attempt has failed. Lets held initial subscriptions deliver against
    /// local state while the transport keeps retrying in the background.
    pub fn remove_pending_server(&mut self, server_id: ServerId) {
        self.pending_servers.remove(&server_id);
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.servers.remove(&server_id);
        self.pending_servers.remove(&server_id);
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

    pub fn has_servers_or_pending_servers(&self) -> bool {
        if !self.servers.is_empty() {
            return true;
        }
        let now = Instant::now();
        self.pending_servers
            .values()
            .any(|since| now.duration_since(*since) < PENDING_SERVER_TIMEOUT)
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
    /// objects directly from storage-backed visible rows.
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

        let old_query_scope = client
            .queries
            .get(&query_id)
            .map(|query| query.scope.clone())
            .unwrap_or_default();
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
        let newly_visible_for_query: Vec<(ObjectId, BranchName)> =
            scope.difference(&old_query_scope).cloned().collect();

        self.prune_client_scope_tracking(client_id, &no_longer_visible);

        for (object_id, branch_name) in newly_visible_for_query {
            self.queue_initial_sync_to_client_with_storage(
                storage,
                client_id,
                object_id,
                branch_name,
                true,
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
    pub fn take_pending_query_settled(&mut self) -> Vec<PendingQuerySettled> {
        std::mem::take(&mut self.pending_query_settled)
    }

    /// Re-queue QuerySettled notifications that are still blocked on stream sequencing.
    pub fn requeue_pending_query_settled(&mut self, pending: Vec<PendingQuerySettled>) {
        self.pending_query_settled.extend(pending);
    }

    /// Take received row-version persistence state since last call.
    /// Used by RuntimeCore to resolve row `_persisted` mutation receivers.
    pub fn take_received_row_version_acks(&mut self) -> Vec<(RowVersionKey, DurabilityTier)> {
        std::mem::take(&mut self.received_row_version_acks)
    }

    /// Take pending row visibility changes for QueryManager to materialize
    /// into indices and subscriptions.
    pub fn take_pending_row_visibility_changes(&mut self) -> Vec<RowVisibilityChange> {
        std::mem::take(&mut self.pending_row_visibility_changes)
    }

    /// Take pending catalogue/system entry updates for QueryManager/SchemaManager.
    pub fn take_pending_catalogue_updates(&mut self) -> Vec<CatalogueEntry> {
        std::mem::take(&mut self.pending_catalogue_updates)
    }

    /// Requeue row visibility changes that could not be processed yet,
    /// typically because the corresponding schema has not been activated yet.
    pub fn requeue_pending_row_visibility_changes(&mut self, updates: Vec<RowVisibilityChange>) {
        self.pending_row_visibility_changes.extend(updates);
    }

    /// Emit a QuerySettled notification to a client.
    ///
    /// Called by QueryManager when a server subscription settles for the first time.
    pub fn emit_query_settled(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        tier: DurabilityTier,
    ) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::QuerySettled {
                query_id,
                tier,
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
