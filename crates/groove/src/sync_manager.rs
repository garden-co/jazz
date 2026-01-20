use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::{BlobId, ObjectManager};

// ============================================================================
// ID Types
// ============================================================================

/// Unique identifier for a server connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub Uuid);

impl ClientId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique identifier for a query subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub u64);

/// Unique identifier for a pending update awaiting approval.
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
// Permission
// ============================================================================

/// Permission level for client access to an object/branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    Readable,
    ReadableAndWritable,
}

impl Permission {
    /// Returns the more permissive of two permissions.
    pub fn merge(self, other: Permission) -> Permission {
        match (self, other) {
            (Permission::ReadableAndWritable, _) | (_, Permission::ReadableAndWritable) => {
                Permission::ReadableAndWritable
            }
            _ => Permission::Readable,
        }
    }
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
    /// Queries we've forwarded to this server.
    pub forwarded_queries: HashMap<QueryId, HashMap<(ObjectId, BranchName), Permission>>,
}

/// Tracking state for a connected client.
#[derive(Debug, Clone, Default)]
pub struct ClientState {
    /// Active queries from this client.
    pub queries: HashMap<QueryId, HashMap<(ObjectId, BranchName), Permission>>,
    /// Derived effective scope (most permissive permission per object/branch).
    pub effective_scope: HashMap<(ObjectId, BranchName), Permission>,
    /// What we've sent to this client.
    pub sent_tips: HashMap<(ObjectId, BranchName), HashSet<CommitId>>,
    /// Object IDs for which we've sent metadata.
    pub sent_metadata: HashSet<ObjectId>,
}

impl ClientState {
    /// Recompute effective_scope from all active queries.
    pub fn recompute_effective_scope(&mut self) {
        self.effective_scope.clear();
        for scope in self.queries.values() {
            for ((object_id, branch_name), permission) in scope {
                let entry = self
                    .effective_scope
                    .entry((*object_id, branch_name.clone()))
                    .or_insert(Permission::Readable);
                *entry = entry.merge(*permission);
            }
        }
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Strongly typed errors for sync operations.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

// ============================================================================
// Message Protocol
// ============================================================================

/// Object metadata sent once per destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
}

/// Payload for sync messages between peers.
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// Register a query (to servers only).
    QueryRegistration {
        query_id: QueryId,
        scope: HashMap<(ObjectId, BranchName), Permission>,
    },

    /// Unregister a query (to servers only).
    QueryUnregistration { query_id: QueryId },

    /// Error response.
    Error(SyncError),
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

/// An update from a client awaiting approval.
#[derive(Debug, Clone)]
pub struct PendingUpdate {
    pub id: PendingUpdateId,
    pub client_id: ClientId,
    pub payload: SyncPayload,
}

// ============================================================================
// SyncManager
// ============================================================================

/// Manages synchronization state atop ObjectManager.
///
/// Coordinates:
/// - Upstream servers (trusted, receive all our objects)
/// - Downstream clients (untrusted, receive query-filtered subsets)
#[derive(Debug, Clone)]
pub struct SyncManager {
    pub object_manager: ObjectManager,

    servers: HashMap<ServerId, ServerState>,
    clients: HashMap<ClientId, ClientState>,

    inbox: Vec<InboxEntry>,
    outbox: Vec<OutboxEntry>,
    pending_updates: Vec<PendingUpdate>,

    next_pending_id: u64,
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
            pending_updates: Vec::new(),
            next_pending_id: 0,
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
            pending_updates: Vec::new(),
            next_pending_id: 0,
        }
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

    /// Remove a client connection.
    pub fn remove_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }

    /// Get server state.
    pub fn get_server(&self, server_id: ServerId) -> Option<&ServerState> {
        self.servers.get(&server_id)
    }

    /// Get client state.
    pub fn get_client(&self, client_id: ClientId) -> Option<&ClientState> {
        self.clients.get(&client_id)
    }

    // ========================================================================
    // Outbox / Inbox
    // ========================================================================

    /// Take all outbox entries, clearing the outbox.
    pub fn take_outbox(&mut self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.outbox)
    }

    /// Push an entry to the inbox for processing.
    pub fn push_inbox(&mut self, entry: InboxEntry) {
        self.inbox.push(entry);
    }

    /// Process all inbox entries.
    pub fn process_inbox(&mut self) {
        let entries = std::mem::take(&mut self.inbox);
        for entry in entries {
            self.process_inbox_entry(entry);
        }
    }

    // ========================================================================
    // Pending Updates
    // ========================================================================

    /// Take all pending updates for upper layer evaluation.
    pub fn take_pending_updates(&mut self) -> Vec<PendingUpdate> {
        std::mem::take(&mut self.pending_updates)
    }

    /// Approve a pending update, applying it.
    pub fn approve_update(&mut self, pending_id: PendingUpdateId) {
        // Find and remove the pending update
        if let Some(pos) = self.pending_updates.iter().position(|p| p.id == pending_id) {
            let pending = self.pending_updates.remove(pos);
            self.apply_payload_from_client(pending.client_id, pending.payload, true);
        }
    }

    /// Reject a pending update, sending error back to client.
    pub fn reject_update(&mut self, pending_id: PendingUpdateId, reason: String) {
        if let Some(pos) = self.pending_updates.iter().position(|p| p.id == pending_id) {
            let pending = self.pending_updates.remove(pos);

            // Extract object_id and branch_name from payload
            let (object_id, branch_name) = match &pending.payload {
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    ..
                } => (*object_id, branch_name.clone()),
                SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name,
                    ..
                } => (*object_id, branch_name.clone()),
                _ => return, // Shouldn't happen for pending updates
            };

            self.outbox.push(OutboxEntry {
                destination: Destination::Client(pending.client_id),
                payload: SyncPayload::Error(SyncError::PermissionDenied {
                    object_id,
                    branch_name,
                    reason,
                }),
            });
        }
    }

    // ========================================================================
    // Query Management (Clients)
    // ========================================================================

    /// Add or update a query for a client.
    pub fn add_or_update_query(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        scope: HashMap<(ObjectId, BranchName), Permission>,
    ) {
        // Collect newly visible keys while holding the mutable borrow
        let newly_visible: Vec<(ObjectId, BranchName)> = {
            let Some(client) = self.clients.get_mut(&client_id) else {
                return;
            };

            let old_scope = client.effective_scope.clone();

            // Update the query
            client.queries.insert(query_id, scope);
            client.recompute_effective_scope();

            // Find newly visible (object, branch) pairs
            client
                .effective_scope
                .keys()
                .filter(|key| !old_scope.contains_key(*key))
                .cloned()
                .collect()
        };

        // Now queue initial syncs (no longer borrowing clients mutably for iteration)
        for (object_id, branch_name) in newly_visible {
            self.queue_initial_sync_to_client(client_id, object_id, branch_name);
        }
    }

    /// Unsubscribe a client from a query.
    pub fn unsubscribe_from_query(&mut self, client_id: ClientId, query_id: QueryId) {
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        client.queries.remove(&query_id);
        client.recompute_effective_scope();
        // Note: We don't "unsend" - just stop sending future updates
    }

    // ========================================================================
    // Query Forwarding (Servers)
    // ========================================================================

    /// Forward a query to a server.
    pub fn forward_query_to_server(
        &mut self,
        server_id: ServerId,
        query_id: QueryId,
        scope: HashMap<(ObjectId, BranchName), Permission>,
    ) {
        let Some(server) = self.servers.get_mut(&server_id) else {
            return;
        };

        server.forwarded_queries.insert(query_id, scope.clone());

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::QueryRegistration { query_id, scope },
        });
    }

    /// Stop forwarding a query to a server.
    pub fn unforward_query_from_server(&mut self, server_id: ServerId, query_id: QueryId) {
        let Some(server) = self.servers.get_mut(&server_id) else {
            return;
        };

        server.forwarded_queries.remove(&query_id);

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::QueryUnregistration { query_id },
        });
    }

    // ========================================================================
    // Internal: Sync Logic
    // ========================================================================

    /// Queue all existing objects to sync to a new server.
    fn queue_full_sync_to_server(&mut self, server_id: ServerId) {
        // Collect all object/branch/tips we need to sync
        let mut to_sync: Vec<BranchSyncData> = Vec::new();

        for (object_id, object_state) in &self.object_manager.objects {
            if let Some(object) = match object_state {
                crate::object::ObjectState::Creating(obj)
                | crate::object::ObjectState::Available(obj) => Some(obj),
                crate::object::ObjectState::Loading => None,
            } {
                for (branch_name, branch) in &object.branches {
                    to_sync.push((
                        *object_id,
                        object.metadata.clone(),
                        branch_name.clone(),
                        branch.tips.clone(),
                    ));
                }
            }
        }

        // Now queue messages (borrowing self.servers mutably)
        for (object_id, metadata, branch_name, tips) in to_sync {
            self.queue_tips_to_server(server_id, object_id, metadata, branch_name, tips);
        }
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
                .get(&(object_id, branch_name.clone()))
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
        server
            .sent_tips
            .insert((object_id, branch_name.clone()), tips);

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
        let tips = branch.tips.clone();
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
            let in_scope = client
                .effective_scope
                .contains_key(&(object_id, branch_name.clone()));

            let include_metadata = !client.sent_metadata.contains(&object_id);

            let already_sent = client
                .sent_tips
                .get(&(object_id, branch_name.clone()))
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
        client
            .sent_tips
            .insert((object_id, branch_name.clone()), tips);

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
    fn process_inbox_entry(&mut self, entry: InboxEntry) {
        match entry.source {
            Source::Server(server_id) => self.process_from_server(server_id, entry.payload),
            Source::Client(client_id) => self.process_from_client(client_id, entry.payload),
        }
    }

    /// Process a payload from a server.
    fn process_from_server(&mut self, server_id: ServerId, payload: SyncPayload) {
        match payload {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
            } => {
                self.apply_object_updated(object_id, metadata, branch_name.clone(), commits);

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
                    object_id,
                    branch_name.clone(),
                    tails.clone(),
                );

                // Forward to clients
                self.forward_truncation_to_clients(object_id, branch_name, tails);
            }
            SyncPayload::BlobResponse { blob_id, data } => {
                let _ = self.object_manager.put_blob(
                    blob_id.object_id,
                    blob_id.branch_name.clone(),
                    blob_id.commit_id,
                    data.clone(),
                );
            }
            SyncPayload::Error(err) => {
                // Log or handle server error
                eprintln!("Error from server {:?}: {:?}", server_id, err);
            }
            // Servers shouldn't send these to us
            SyncPayload::BlobRequest { .. }
            | SyncPayload::QueryRegistration { .. }
            | SyncPayload::QueryUnregistration { .. } => {}
        }
    }

    /// Process a payload from a client.
    fn process_from_client(&mut self, client_id: ClientId, payload: SyncPayload) {
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };

        match &payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            } => {
                let key = (*object_id, branch_name.clone());

                match client.effective_scope.get(&key) {
                    Some(Permission::ReadableAndWritable) => {
                        // Client has write permission - apply immediately
                        self.apply_payload_from_client(client_id, payload, false);
                    }
                    Some(Permission::Readable) => {
                        // Client has read-only permission - reject
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::PermissionDenied {
                                object_id: *object_id,
                                branch_name: branch_name.clone(),
                                reason: "read-only access".to_string(),
                            }),
                        });
                    }
                    None => {
                        // Out of scope - queue for approval
                        let id = PendingUpdateId(self.next_pending_id);
                        self.next_pending_id += 1;
                        self.pending_updates.push(PendingUpdate {
                            id,
                            client_id,
                            payload,
                        });
                    }
                }
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => {
                let key = (*object_id, branch_name.clone());

                match client.effective_scope.get(&key) {
                    Some(Permission::ReadableAndWritable) => {
                        self.apply_payload_from_client(client_id, payload, false);
                    }
                    Some(Permission::Readable) => {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::PermissionDenied {
                                object_id: *object_id,
                                branch_name: branch_name.clone(),
                                reason: "read-only access".to_string(),
                            }),
                        });
                    }
                    None => {
                        let id = PendingUpdateId(self.next_pending_id);
                        self.next_pending_id += 1;
                        self.pending_updates.push(PendingUpdate {
                            id,
                            client_id,
                            payload,
                        });
                    }
                }
            }
            SyncPayload::BlobRequest { blob_id } => {
                // Check if client has read permission for any object referencing this blob
                let has_permission = client
                    .effective_scope
                    .keys()
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
            // Clients shouldn't send these
            SyncPayload::BlobResponse { .. }
            | SyncPayload::QueryRegistration { .. }
            | SyncPayload::QueryUnregistration { .. }
            | SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    fn apply_payload_from_client(
        &mut self,
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
                self.apply_object_updated(object_id, metadata, branch_name.clone(), commits);

                // Forward to servers
                self.forward_update_to_servers(object_id, branch_name.clone());

                // Forward to other clients (not the sender)
                self.forward_update_to_clients_except(object_id, branch_name, client_id);
            }
            SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                tails,
            } => {
                let _ = self.object_manager.truncate_branch(
                    object_id,
                    branch_name.clone(),
                    tails.clone(),
                );

                // Forward to servers
                self.forward_truncation_to_servers(object_id, branch_name.clone(), tails.clone());

                // Forward to other clients
                self.forward_truncation_to_clients_except(object_id, branch_name, tails, client_id);
            }
            _ => {}
        }
    }

    /// Apply an ObjectUpdated payload to the local ObjectManager.
    fn apply_object_updated(
        &mut self,
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,
        branch_name: BranchName,
        commits: Vec<Commit>,
    ) {
        // If we don't have this object yet and metadata is provided, create it
        if self.object_manager.get(object_id).is_none() {
            if let Some(meta) = metadata {
                // Create object with metadata (we need to set the specific ID)
                self.object_manager.receive_object(object_id, meta.metadata);
            } else {
                // Can't create without metadata
                return;
            }
        }

        // Apply each commit
        for commit in commits {
            let _ = self
                .object_manager
                .receive_commit(object_id, branch_name.clone(), commit);
        }
    }

    /// Forward an update to all servers.
    fn forward_update_to_servers(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips = branch.tips.clone();
        let metadata = object.metadata.clone();

        for server_id in server_ids {
            self.queue_tips_to_server(
                server_id,
                object_id,
                metadata.clone(),
                branch_name.clone(),
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
            .filter(|(id, client)| {
                **id != except
                    && client
                        .effective_scope
                        .contains_key(&(object_id, branch_name.clone()))
            })
            .map(|(id, _)| *id)
            .collect();

        let Some(object) = self.object_manager.get(object_id) else {
            return;
        };
        let Some(branch) = object.branches.get(&branch_name) else {
            return;
        };
        let tips = branch.tips.clone();
        let metadata = object.metadata.clone();

        for client_id in client_ids {
            self.queue_tips_to_client(
                client_id,
                object_id,
                metadata.clone(),
                branch_name.clone(),
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
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();

        for server_id in server_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name: branch_name.clone(),
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
        let client_ids: Vec<ClientId> = self
            .clients
            .iter()
            .filter(|(id, client)| {
                **id != except
                    && client
                        .effective_scope
                        .contains_key(&(object_id, branch_name.clone()))
            })
            .map(|(id, _)| *id)
            .collect();

        for client_id in client_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::ObjectTruncated {
                    object_id,
                    branch_name: branch_name.clone(),
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

    // ========================================================================
    // Phase 1: Foundation Tests
    // ========================================================================

    #[test]
    fn can_create_sync_manager() {
        let sm = SyncManager::new();
        assert!(sm.servers.is_empty());
        assert!(sm.clients.is_empty());
    }

    #[test]
    fn permission_merge_takes_most_permissive() {
        assert_eq!(
            Permission::Readable.merge(Permission::Readable),
            Permission::Readable
        );
        assert_eq!(
            Permission::Readable.merge(Permission::ReadableAndWritable),
            Permission::ReadableAndWritable
        );
        assert_eq!(
            Permission::ReadableAndWritable.merge(Permission::Readable),
            Permission::ReadableAndWritable
        );
    }

    // ========================================================================
    // Phase 2: Server Sync Tests
    // ========================================================================

    #[test]
    fn add_server_receives_existing_objects() {
        let mut sm = SyncManager::new();

        // Create an object with a commit
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None);

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
                assert_eq!(branch_name.0, "main");
                assert_eq!(commits.len(), 1);
            }
            _ => panic!("Expected ObjectUpdated"),
        }
    }

    #[test]
    fn local_commit_syncs_to_server() {
        let mut sm = SyncManager::new();
        let server_id = ServerId::new();
        sm.add_server(server_id);

        // Clear initial outbox
        sm.take_outbox();

        // Create object and commit
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None)
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
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        sm.remove_server(server_id);

        // Create new object
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None);

        sm.forward_update_to_servers(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // No server to send to
    }

    #[test]
    fn commits_sent_in_causal_order() {
        let mut sm = SyncManager::new();
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();

        // Create chain: c1 <- c2 <- c3
        let c1 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"c1".to_vec(), author, None)
            .unwrap();
        let c2 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![c1], b"c2".to_vec(), author, None)
            .unwrap();
        let c3 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![c2], b"c3".to_vec(), author, None)
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

        // Create object
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None);

        // Add client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);

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

        // Create object
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None);

        // Add client without query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty());
    }

    #[test]
    fn local_commit_in_scope_syncs_to_client() {
        let mut sm = SyncManager::new();

        // Setup client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox(); // Clear initial sync

        // Add commit
        let commit_id = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None)
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

        let client_id = ClientId::new();
        sm.add_client(client_id);

        // Client has query for obj1/main
        let obj1 = sm.object_manager.create(None);
        let mut scope = HashMap::new();
        scope.insert((obj1, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox();

        // Create commit on different object
        let obj2 = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ =
            sm.object_manager
                .add_commit(obj2, "main", vec![], b"content".to_vec(), author, None);

        sm.forward_update_to_clients(obj2, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // obj2 not in client's scope
    }

    #[test]
    fn query_update_adds_scope_triggers_initial_sync() {
        let mut sm = SyncManager::new();

        // Create two objects
        let obj1 = sm.object_manager.create(None);
        let obj2 = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ = sm
            .object_manager
            .add_commit(obj1, "main", vec![], b"c1".to_vec(), author, None);
        let _ = sm
            .object_manager
            .add_commit(obj2, "main", vec![], b"c2".to_vec(), author, None);

        // Client initially only has obj1
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj1, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox(); // Clear obj1 sync

        // Update query to also include obj2
        let mut new_scope = HashMap::new();
        new_scope.insert((obj1, "main".into()), Permission::Readable);
        new_scope.insert((obj2, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), new_scope);

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

        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox();

        // Remove query
        sm.unsubscribe_from_query(client_id, QueryId(1));

        // Add commit
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None);

        sm.forward_update_to_clients(obj_id, "main".into());

        let outbox = sm.take_outbox();
        assert!(outbox.is_empty()); // Client no longer in scope
    }

    // ========================================================================
    // Phase 4: Permission Enforcement Tests
    // ========================================================================

    #[test]
    fn client_with_readable_permission_cannot_push() {
        let mut sm = SyncManager::new();

        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let _ = sm.object_manager.add_commit(
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        );

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable); // Read-only
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox();

        // Client tries to push update
        let commit = Commit {
            parents: vec![],
            content: b"malicious".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
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

        sm.process_inbox();

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::PermissionDenied {
                object_id,
                branch_name,
                reason,
            }) => {
                assert_eq!(*object_id, obj_id);
                assert_eq!(branch_name.0, "main");
                assert_eq!(reason, "read-only access");
            }
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[test]
    fn client_with_writable_permission_can_push() {
        let mut sm = SyncManager::new();

        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"original".to_vec(), author, None)
            .unwrap();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::ReadableAndWritable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox();

        // Client pushes valid update
        let commit = Commit {
            parents: vec![c1],
            content: b"update".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
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

        sm.process_inbox();

        // Verify commit was applied
        let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
        assert!(tips.contains(&commit.id()));
    }

    // ========================================================================
    // Phase 4b: Pending Updates Tests
    // ========================================================================

    #[test]
    fn out_of_scope_update_goes_to_pending() {
        let mut sm = SyncManager::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);
        // Client has no queries - everything is out of scope

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: vec![],
            content: b"new object".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: Some(ObjectMetadata {
                    id: obj_id,
                    metadata: HashMap::new(),
                }),
                branch_name: "main".into(),
                commits: vec![commit],
            },
        });

        sm.process_inbox();

        let pending = sm.take_pending_updates();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].client_id, client_id);
    }

    #[test]
    fn approved_pending_update_is_applied() {
        let mut sm = SyncManager::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: vec![],
            content: b"new object".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
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

        sm.process_inbox();

        let pending = sm.take_pending_updates();
        let pending_id = pending[0].id;

        // Re-add the pending update (take removes it)
        sm.pending_updates = pending;

        // Approve
        sm.approve_update(pending_id);

        // Verify object was created
        assert!(sm.object_manager.get(obj_id).is_some());
    }

    #[test]
    fn rejected_pending_update_sends_error() {
        let mut sm = SyncManager::new();

        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = ObjectId::new();
        let author = ObjectId::new();
        let commit = Commit {
            parents: vec![],
            content: b"new object".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
        };

        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::ObjectUpdated {
                object_id: obj_id,
                metadata: Some(ObjectMetadata {
                    id: obj_id,
                    metadata: HashMap::new(),
                }),
                branch_name: "main".into(),
                commits: vec![commit],
            },
        });

        sm.process_inbox();

        let pending = sm.take_pending_updates();
        let pending_id = pending[0].id;
        sm.pending_updates = pending;

        sm.reject_update(pending_id, "Not allowed".to_string());

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::Error(SyncError::PermissionDenied {
                object_id,
                branch_name,
                reason,
            }) => {
                assert_eq!(*object_id, obj_id);
                assert_eq!(branch_name.0, "main");
                assert_eq!(reason, "Not allowed");
            }
            _ => panic!("Expected PermissionDenied error"),
        }

        // Object should not exist
        assert!(sm.object_manager.get(obj_id).is_none());
    }

    // ========================================================================
    // Phase 5: Query Forwarding Tests
    // ========================================================================

    #[test]
    fn forward_query_to_server_sends_registration() {
        let mut sm = SyncManager::new();

        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        let obj_id = ObjectId::new();
        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);

        sm.forward_query_to_server(server_id, QueryId(1), scope.clone());

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::QueryRegistration {
                query_id,
                scope: sent_scope,
            } => {
                assert_eq!(*query_id, QueryId(1));
                assert_eq!(*sent_scope, scope);
            }
            _ => panic!("Expected QueryRegistration"),
        }
    }

    #[test]
    fn unforward_query_from_server_sends_unregistration() {
        let mut sm = SyncManager::new();

        let server_id = ServerId::new();
        sm.add_server(server_id);

        let obj_id = ObjectId::new();
        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.forward_query_to_server(server_id, QueryId(1), scope);
        sm.take_outbox();

        sm.unforward_query_from_server(server_id, QueryId(1));

        let outbox = sm.take_outbox();
        assert_eq!(outbox.len(), 1);

        match &outbox[0].payload {
            SyncPayload::QueryUnregistration { query_id } => {
                assert_eq!(*query_id, QueryId(1));
            }
            _ => panic!("Expected QueryUnregistration"),
        }
    }

    #[test]
    fn server_update_forwarded_to_matching_clients() {
        let mut sm = SyncManager::new();

        // Setup server
        let server_id = ServerId::new();
        sm.add_server(server_id);
        sm.take_outbox();

        // Setup client with query
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let obj_id = ObjectId::new();
        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
        sm.take_outbox();

        // Server sends update
        let author = ObjectId::new();
        let commit = Commit {
            parents: vec![],
            content: b"from server".to_vec(),
            timestamp: 1000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
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

        sm.process_inbox();

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

        // Create object with blob
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None)
            .unwrap();

        let content_hash = sm
            .object_manager
            .put_blob(obj_id, "main", commit_id, b"blob data".to_vec())
            .unwrap();

        // Client with read permission
        let client_id = ClientId::new();
        sm.add_client(client_id);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::Readable);
        sm.add_or_update_query(client_id, QueryId(1), scope);
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

        sm.process_inbox();

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

        // Create object with blob
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let commit_id = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"content".to_vec(), author, None)
            .unwrap();

        let content_hash = sm
            .object_manager
            .put_blob(obj_id, "main", commit_id, b"blob data".to_vec())
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

        sm.process_inbox();

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

        // Setup server
        let server_id = ServerId::new();
        sm.add_server(server_id);

        // Create object
        let obj_id = sm.object_manager.create(None);
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"initial".to_vec(), author, None)
            .unwrap();

        sm.take_outbox();

        // Setup two clients
        let client1 = ClientId::new();
        let client2 = ClientId::new();
        sm.add_client(client1);
        sm.add_client(client2);

        let mut scope = HashMap::new();
        scope.insert((obj_id, "main".into()), Permission::ReadableAndWritable);
        sm.add_or_update_query(client1, QueryId(1), scope.clone());
        sm.add_or_update_query(client2, QueryId(1), scope);
        sm.take_outbox();

        // Client1 sends update
        let commit = Commit {
            parents: vec![c1],
            content: b"from client1".to_vec(),
            timestamp: 2000,
            author,
            metadata: None,
            stored_state: crate::commit::StoredState::Stored,
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

        sm.process_inbox();

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

        // Create object BEFORE adding server
        let obj_id = sm.object_manager.create(Some(
            [("key".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        ));
        let author = ObjectId::new();
        let c1 = sm
            .object_manager
            .add_commit(obj_id, "main", vec![], b"c1".to_vec(), author, None)
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
        let _ =
            sm.object_manager
                .add_commit(obj_id, "main", vec![c1], b"c2".to_vec(), author, None);

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
}
