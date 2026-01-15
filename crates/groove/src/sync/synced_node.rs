//! SyncedNode - Object storage with sync capabilities.
//!
//! SyncedNode wraps a LocalNode (object store) and adds:
//! - Upstream server connections (servers we sync TO)
//! - Connected client sessions (clients that sync FROM us)
//! - Automatic write batching/debouncing
//! - Automatic SSE event application
//!
//! SyncedNode knows NOTHING about databases, SQL, schemas, or tables.
//! It operates purely at the object/commit level. The database layer
//! can observe incoming objects by registering a callback.
//!
//! Objects marked as `node_private` in their metadata are never synced.

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

// Platform-agnostic Instant
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

use crate::commit::{Commit, CommitId};
use crate::node::LocalNode;
use crate::object::ObjectId;

use super::env::{ClientEnv, ClientError};
use super::negotiation::{FrontierComparison, commits_to_send, compare_frontiers};
use super::protocol::{
    PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest, SubscriptionOptions,
};
use super::runtime::{ReconnectConfig, Runtime, calculate_reconnect_delay_with_jitter};
use super::shared::Shared;

#[cfg(not(target_arch = "wasm32"))]
use super::server::{ClientIdentity, ClientSession, SessionId, SseSender, TokenValidator};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for sync behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Debounce delay before pushing writes upstream (ms).
    pub write_debounce_ms: u64,

    /// Maximum batch size for upstream pushes.
    pub max_batch_size: usize,

    /// Force push after this delay regardless of debounce (ms).
    pub max_batch_age_ms: u64,

    /// Session timeout for connected clients (ms).
    pub session_timeout_ms: u64,

    /// Heartbeat interval for SSE connections (ms).
    pub heartbeat_interval_ms: u64,

    /// Reconnection configuration.
    pub reconnect: ReconnectConfig,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            write_debounce_ms: 100,
            max_batch_size: 100,
            max_batch_age_ms: 1000,
            session_timeout_ms: 60_000,    // 1 minute
            heartbeat_interval_ms: 30_000, // 30 seconds
            reconnect: ReconnectConfig::default(),
        }
    }
}

// ============================================================================
// Upstream Server Connection
// ============================================================================

/// Unique identifier for an upstream server connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpstreamId(pub u64);

/// State of an upstream connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamState {
    /// Not connected.
    Disconnected,
    /// Attempting to connect.
    Connecting,
    /// Connected and syncing.
    Connected,
    /// Reconnecting after disconnect.
    Reconnecting { attempt: u32, next_delay_ms: u64 },
}

/// A connection to an upstream server (a server we sync TO).
pub struct UpstreamServer<E: ClientEnv> {
    /// Unique identifier.
    pub id: UpstreamId,

    /// Transport environment.
    env: E,

    /// Connection state.
    state: UpstreamState,

    /// Active query subscriptions.
    subscriptions: HashMap<u32, UpstreamSubscription>,

    /// Next subscription ID.
    next_subscription_id: u32,

    /// Server's assumed known state per object.
    server_known_state: HashMap<ObjectId, Vec<CommitId>>,
}

/// A query subscription on an upstream server.
#[derive(Debug)]
pub struct UpstreamSubscription {
    /// The SQL query.
    pub query: String,
    /// Subscription options.
    pub options: SubscriptionOptions,
    /// Objects received via this subscription.
    pub objects: std::collections::HashSet<ObjectId>,
}

impl<E: ClientEnv> UpstreamServer<E> {
    /// Create a new upstream server connection.
    pub fn new(id: UpstreamId, env: E) -> Self {
        Self {
            id,
            env,
            state: UpstreamState::Disconnected,
            subscriptions: HashMap::new(),
            next_subscription_id: 1,
            server_known_state: HashMap::new(),
        }
    }

    /// Get the current connection state.
    pub fn state(&self) -> &UpstreamState {
        &self.state
    }

    /// Set the connection state.
    pub fn set_state(&mut self, state: UpstreamState) {
        self.state = state;
    }

    /// Get server's assumed known state for an object.
    pub fn server_known_state(&self, object_id: &ObjectId) -> Option<&Vec<CommitId>> {
        self.server_known_state.get(object_id)
    }

    /// Update server's assumed known state.
    pub fn update_server_known_state(&mut self, object_id: ObjectId, frontier: Vec<CommitId>) {
        self.server_known_state.insert(object_id, frontier);
    }

    /// Get the transport environment.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// Allocate a new subscription ID.
    fn next_subscription_id(&mut self) -> u32 {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }

    /// Subscribe to a query.
    pub async fn subscribe(
        &mut self,
        query: String,
        options: SubscriptionOptions,
    ) -> Result<
        (
            u32,
            futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
        ),
        ClientError,
    > {
        let sub_id = self.next_subscription_id();
        let request = SubscribeRequest {
            query: query.clone(),
            options: options.clone(),
        };

        let stream = self.env.subscribe(request).await?;

        self.subscriptions.insert(
            sub_id,
            UpstreamSubscription {
                query,
                options,
                objects: std::collections::HashSet::new(),
            },
        );
        self.state = UpstreamState::Connected;

        Ok((sub_id, stream))
    }

    /// Push commits to the upstream server.
    pub async fn push(&mut self, request: PushRequest) -> Result<PushResponse, ClientError> {
        let response = self.env.push(request).await?;
        if response.accepted {
            self.update_server_known_state(response.object_id, response.frontier.clone());
        }
        Ok(response)
    }

    /// Request reconciliation for an object.
    pub async fn reconcile(&mut self, request: ReconcileRequest) -> Result<SseEvent, ClientError> {
        self.env.reconcile(request).await
    }

    /// Get all active subscriptions for reconnection.
    ///
    /// Returns (query, options) pairs that should be re-subscribed after reconnect.
    pub fn active_subscriptions(&self) -> Vec<(String, SubscriptionOptions)> {
        self.subscriptions
            .values()
            .map(|s| (s.query.clone(), s.options.clone()))
            .collect()
    }

    /// Get all objects tracked via subscriptions.
    ///
    /// Returns deduplicated list of object IDs from all subscriptions.
    pub fn tracked_objects(&self) -> Vec<ObjectId> {
        self.subscriptions
            .values()
            .flat_map(|s| s.objects.iter().copied())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Record that an object was received via a subscription.
    pub fn track_object(&mut self, subscription_id: u32, object_id: ObjectId) {
        if let Some(sub) = self.subscriptions.get_mut(&subscription_id) {
            sub.objects.insert(object_id);
        }
    }

    /// Clear all subscriptions (for reconnection).
    pub fn clear_subscriptions(&mut self) {
        self.subscriptions.clear();
        self.next_subscription_id = 1;
    }

    /// Check if any subscriptions exist.
    pub fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Add a subscription (for use when subscribe was called externally).
    ///
    /// Returns the subscription ID.
    pub fn add_subscription(&mut self, query: String, options: SubscriptionOptions) -> u32 {
        let sub_id = self.next_subscription_id();
        self.subscriptions.insert(
            sub_id,
            UpstreamSubscription {
                query,
                options,
                objects: std::collections::HashSet::new(),
            },
        );
        sub_id
    }
}

/// Manager for upstream server connections.
pub struct UpstreamServers<E: ClientEnv> {
    /// Active upstream connections.
    connections: HashMap<UpstreamId, UpstreamServer<E>>,

    /// Next connection ID.
    next_id: u64,
}

impl<E: ClientEnv> Default for UpstreamServers<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: ClientEnv> UpstreamServers<E> {
    /// Create a new upstream server manager.
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            next_id: 1,
        }
    }

    /// Add an upstream server connection.
    pub fn add(&mut self, env: E) -> UpstreamId {
        let id = UpstreamId(self.next_id);
        self.next_id += 1;
        self.connections.insert(id, UpstreamServer::new(id, env));
        id
    }

    /// Get an upstream server by ID.
    pub fn get(&self, id: UpstreamId) -> Option<&UpstreamServer<E>> {
        self.connections.get(&id)
    }

    /// Get a mutable reference to an upstream server.
    pub fn get_mut(&mut self, id: UpstreamId) -> Option<&mut UpstreamServer<E>> {
        self.connections.get_mut(&id)
    }

    /// Remove an upstream server connection.
    pub fn remove(&mut self, id: UpstreamId) -> Option<UpstreamServer<E>> {
        self.connections.remove(&id)
    }

    /// Get all upstream IDs.
    pub fn ids(&self) -> impl Iterator<Item = UpstreamId> + '_ {
        self.connections.keys().copied()
    }

    /// Check if there are any upstream connections.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

// ============================================================================
// Connected Clients (Server-side)
// ============================================================================

#[cfg(not(target_arch = "wasm32"))]
/// Manager for connected client sessions (clients that sync FROM us).
pub struct ConnectedClients {
    /// Active client sessions.
    sessions: HashMap<SessionId, ClientSession>,

    /// Reverse index: object -> sessions tracking it.
    object_sessions: HashMap<ObjectId, std::collections::HashSet<SessionId>>,

    /// Identity -> sessions mapping.
    identity_sessions: HashMap<String, std::collections::HashSet<SessionId>>,

    /// Token validator.
    token_validator: Option<Rc<dyn TokenValidator>>,

    /// Next session ID.
    next_session_id: u64,

    /// Stale client_known_state (kept after session removal for reconnection).
    stale_states: HashMap<String, StaleClientState>,
}

#[cfg(not(target_arch = "wasm32"))]
/// Stale client state kept for grace period after disconnect.
pub struct StaleClientState {
    /// The client's known state at disconnect.
    pub known_state: HashMap<ObjectId, Vec<CommitId>>,
    /// When the session was removed.
    pub removed_at: Instant,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for ConnectedClients {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConnectedClients {
    /// Create a new connected clients manager.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            object_sessions: HashMap::new(),
            identity_sessions: HashMap::new(),
            token_validator: None,
            next_session_id: 1,
            stale_states: HashMap::new(),
        }
    }

    /// Set the token validator.
    pub fn set_token_validator(&mut self, validator: Rc<dyn TokenValidator>) {
        self.token_validator = Some(validator);
    }

    /// Accept a new client session.
    pub fn accept_session(&mut self, identity: ClientIdentity, sse_sender: SseSender) -> SessionId {
        let id = SessionId(self.next_session_id);
        self.next_session_id += 1;

        let session = ClientSession::new(identity.clone(), sse_sender);

        // Track by identity
        self.identity_sessions
            .entry(identity.external_id.clone())
            .or_default()
            .insert(id);

        self.sessions.insert(id, session);
        id
    }

    /// Get a session by ID.
    pub fn get_session(&self, id: &SessionId) -> Option<&ClientSession> {
        self.sessions.get(id)
    }

    /// Get a mutable reference to a session.
    pub fn get_session_mut(&mut self, id: &SessionId) -> Option<&mut ClientSession> {
        self.sessions.get_mut(id)
    }

    /// Remove a session.
    pub fn remove_session(&mut self, id: SessionId) -> Option<ClientSession> {
        if let Some(session) = self.sessions.remove(&id) {
            // Remove from identity index
            if let Some(sessions) = self
                .identity_sessions
                .get_mut(&session.identity.external_id)
            {
                sessions.remove(&id);
            }

            // Remove from object index
            for sessions in self.object_sessions.values_mut() {
                sessions.remove(&id);
            }

            // Keep stale state for reconnection
            self.stale_states.insert(
                session.identity.external_id.clone(),
                StaleClientState {
                    known_state: session.client_known_state.clone(),
                    removed_at: Instant::now(),
                },
            );

            Some(session)
        } else {
            None
        }
    }

    /// Register an object with a session.
    pub fn register_object_session(&mut self, object_id: ObjectId, session_id: SessionId) {
        self.object_sessions
            .entry(object_id)
            .or_default()
            .insert(session_id);
    }

    /// Get sessions tracking an object.
    pub fn sessions_for_object(&self, object_id: &ObjectId) -> Vec<SessionId> {
        self.object_sessions
            .get(object_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Check for timed-out sessions.
    pub fn check_timeouts(&mut self, timeout: Duration) -> Vec<SessionId> {
        let now = Instant::now();
        let expired: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) > timeout)
            .map(|(id, _)| *id)
            .collect();

        expired
    }

    /// Clean up stale states older than grace period.
    pub fn cleanup_stale_states(&mut self, grace_period: Duration) {
        let now = Instant::now();
        self.stale_states
            .retain(|_, state| now.duration_since(state.removed_at) < grace_period);
    }
}

// ============================================================================
// Write Buffer
// ============================================================================

/// Pending writes for an object.
#[derive(Debug)]
pub struct PendingWrites {
    /// Object ID.
    pub object_id: ObjectId,
    /// Branch name.
    pub branch: String,
    /// First write timestamp.
    pub first_write: Instant,
    /// Last write timestamp.
    pub last_write: Instant,
}

/// Buffer for batching writes before pushing upstream.
pub struct WriteBuffer {
    /// Pending writes per object.
    pending: HashMap<ObjectId, PendingWrites>,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBuffer {
    /// Create a new write buffer.
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Add a pending write.
    pub fn add(&mut self, object_id: ObjectId, branch: &str) {
        let now = Instant::now();
        self.pending
            .entry(object_id)
            .and_modify(|p| p.last_write = now)
            .or_insert_with(|| PendingWrites {
                object_id,
                branch: branch.to_string(),
                first_write: now,
                last_write: now,
            });
    }

    /// Get objects ready to push (debounce expired or max age reached).
    pub fn ready_to_push(&self, debounce_ms: u64, max_age_ms: u64) -> Vec<ObjectId> {
        let now = Instant::now();
        let debounce = Duration::from_millis(debounce_ms);
        let max_age = Duration::from_millis(max_age_ms);

        self.pending
            .iter()
            .filter(|(_, p)| {
                let since_last = now.duration_since(p.last_write);
                let since_first = now.duration_since(p.first_write);
                since_last >= debounce || since_first >= max_age
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Remove an object from pending.
    pub fn remove(&mut self, object_id: &ObjectId) -> Option<PendingWrites> {
        self.pending.remove(object_id)
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get all pending object IDs.
    pub fn pending_objects(&self) -> Vec<ObjectId> {
        self.pending.keys().copied().collect()
    }
}

// ============================================================================
// SyncedNode
// ============================================================================

/// Callback type for observing objects received from sync.
/// The callback receives (object_id, commits, object_meta).
///
/// No Send + Sync bounds - the sync layer is single-threaded.
pub type OnObjectsReceivedCallback =
    std::rc::Rc<dyn Fn(ObjectId, &[Commit], Option<&std::collections::BTreeMap<String, String>>)>;

/// Object storage with sync capabilities.
///
/// SyncedNode wraps a LocalNode (object store) and adds:
/// - Connections to upstream servers (servers we sync TO)
/// - Sessions from connected clients (clients that sync FROM us)
/// - Automatic write batching and debouncing
/// - Automatic SSE event application
///
/// SyncedNode knows NOTHING about databases, SQL, schemas, or tables.
/// It operates purely at the object/commit level. Higher layers (e.g., Database)
/// can observe incoming objects by registering a callback via `set_on_objects_received`.
///
/// Objects marked as `node_private: true` in their metadata are never synced.
pub struct SyncedNode<R: Runtime, E: ClientEnv> {
    /// The underlying object store.
    node: Rc<LocalNode>,

    /// Runtime for spawning async tasks.
    runtime: R,

    /// Connections to upstream servers.
    upstream_servers: Shared<UpstreamServers<E>>,

    /// Connected client sessions (server-side).
    #[cfg(not(target_arch = "wasm32"))]
    connected_clients: Shared<ConnectedClients>,

    /// Write buffer for batching upstream pushes.
    write_buffer: Shared<WriteBuffer>,

    /// Sync configuration.
    config: SyncConfig,

    /// Callback for when objects are received from upstream.
    /// Higher layers (e.g., Database) register here to observe synced objects.
    on_objects_received: std::cell::RefCell<Option<OnObjectsReceivedCallback>>,
}

impl<R: Runtime, E: ClientEnv> SyncedNode<R, E> {
    /// Create a new SyncedNode from a LocalNode.
    pub fn new(node: Rc<LocalNode>, runtime: R) -> Self {
        Self {
            node,
            runtime,
            upstream_servers: Shared::new(UpstreamServers::new()),
            #[cfg(not(target_arch = "wasm32"))]
            connected_clients: Shared::new(ConnectedClients::new()),
            write_buffer: Shared::new(WriteBuffer::new()),
            config: SyncConfig::default(),
            on_objects_received: std::cell::RefCell::new(None),
        }
    }

    /// Create a new SyncedNode with custom configuration.
    pub fn with_config(node: Rc<LocalNode>, runtime: R, config: SyncConfig) -> Self {
        Self {
            node,
            runtime,
            upstream_servers: Shared::new(UpstreamServers::new()),
            #[cfg(not(target_arch = "wasm32"))]
            connected_clients: Shared::new(ConnectedClients::new()),
            write_buffer: Shared::new(WriteBuffer::new()),
            config,
            on_objects_received: std::cell::RefCell::new(None),
        }
    }

    /// Get a reference to the underlying LocalNode.
    pub fn node(&self) -> &LocalNode {
        &self.node
    }

    /// Get the underlying LocalNode as an Arc.
    pub fn node_arc(&self) -> Rc<LocalNode> {
        Rc::clone(&self.node)
    }

    /// Get the runtime.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Get the sync configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Set the callback for when objects are received from upstream.
    ///
    /// Higher layers (e.g., Database) register here to observe synced objects.
    /// The callback receives (object_id, commits, object_meta).
    pub fn set_on_objects_received(&self, callback: OnObjectsReceivedCallback) {
        *self.on_objects_received.borrow_mut() = Some(callback);
    }

    // ========== Upstream Server API ==========

    /// Add an upstream server connection.
    ///
    /// Returns the UpstreamId for the new connection.
    pub fn add_upstream(&self, env: E) -> UpstreamId {
        self.upstream_servers.write().add(env)
    }

    /// Remove an upstream server connection.
    pub fn remove_upstream(&self, id: UpstreamId) -> bool {
        self.upstream_servers.write().remove(id).is_some()
    }

    /// Get all upstream server IDs.
    pub fn upstream_ids(&self) -> Vec<UpstreamId> {
        self.upstream_servers.read().ids().collect()
    }

    /// Check if there are any upstream connections.
    pub fn has_upstream(&self) -> bool {
        !self.upstream_servers.read().is_empty()
    }

    /// Get the state of an upstream connection.
    pub fn upstream_state(&self, id: UpstreamId) -> Option<UpstreamState> {
        self.upstream_servers
            .read()
            .get(id)
            .map(|u| u.state().clone())
    }

    // ========== Connected Clients API (Server-side) ==========

    #[cfg(not(target_arch = "wasm32"))]
    /// Set the token validator for accepting client connections.
    pub fn set_token_validator(&self, validator: Rc<dyn TokenValidator>) {
        self.connected_clients
            .write()
            .set_token_validator(validator);
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Accept a new client session.
    pub fn accept_client(&self, identity: ClientIdentity, sse_sender: SseSender) -> SessionId {
        self.connected_clients
            .write()
            .accept_session(identity, sse_sender)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Remove a client session.
    pub fn remove_client(&self, session_id: SessionId) {
        self.connected_clients.write().remove_session(session_id);
    }

    // ========== Write Buffer API ==========

    /// Queue an object for upstream push.
    ///
    /// Objects marked as `node_private` in their metadata are skipped - they
    /// should never be synced (e.g., table_rows objects, index objects).
    pub fn queue_for_push(&self, object_id: ObjectId, branch: &str) {
        // Check if object is node_private - skip sync for private objects
        if let Some(obj) = self.node.get_object(object_id)
            && let Ok(obj_guard) = obj.read()
            && obj_guard.is_node_private()
        {
            return; // Don't sync node_private objects
        }
        self.write_buffer.write().add(object_id, branch);
    }

    /// Get objects ready to push.
    pub fn ready_to_push(&self) -> Vec<ObjectId> {
        self.write_buffer
            .read()
            .ready_to_push(self.config.write_debounce_ms, self.config.max_batch_age_ms)
    }

    /// Mark an object as pushed (remove from buffer).
    pub fn mark_pushed(&self, object_id: &ObjectId) {
        self.write_buffer.write().remove(object_id);
    }

    // ========== High-level Operations ==========

    /// Apply commits received from an upstream server.
    ///
    /// This is called automatically by the SSE event loop.
    /// Commits are applied directly to LocalNode, then observers are notified.
    pub fn apply_upstream_commits(
        &self,
        upstream_id: UpstreamId,
        object_id: ObjectId,
        commits: Vec<Commit>,
        frontier: Vec<CommitId>,
        object_meta: Option<std::collections::BTreeMap<String, String>>,
    ) {
        // Ensure object exists with metadata (if provided)
        // The metadata must be set BEFORE apply_commits so that the
        // on_commits_applied callback can access it (e.g., to find table name)
        if let Some(ref meta) = object_meta {
            self.node
                .ensure_object_with_meta(object_id, "synced", meta.clone());
        } else {
            self.node.ensure_object(object_id, "synced");
        }

        // If object already existed but we have new metadata, update it
        if let Some(ref meta) = object_meta
            && let Some(obj) = self.node.get_object(object_id)
            && let Ok(mut obj_write) = obj.write()
        {
            obj_write.set_meta(meta.clone());
        }

        // Apply commits to the object's main branch
        if !commits.is_empty() {
            self.node.apply_commits(object_id, "main", commits.clone());
        }

        // Notify observers (e.g., database layer) that new commits arrived
        if let Some(callback) = self.on_objects_received.borrow().as_ref() {
            callback(object_id, &commits, object_meta.as_ref());
        }

        // Update upstream's known state
        if let Some(upstream) = self.upstream_servers.write().get_mut(upstream_id) {
            upstream.update_server_known_state(object_id, frontier.clone());
        }

        // Broadcast to connected clients (for edge server scenario)
        #[cfg(not(target_arch = "wasm32"))]
        {
            let event = SseEvent::Commits {
                object_id,
                commits,
                frontier,
                object_meta,
            };
            self.broadcast_to_clients(object_id, &event);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Broadcast an event to all clients tracking an object.
    pub fn broadcast_to_clients(&self, object_id: ObjectId, event: &SseEvent) {
        let clients = self.connected_clients.read();
        let session_ids = clients.sessions_for_object(&object_id);

        for session_id in session_ids {
            if let Some(session) = clients.get_session(&session_id) {
                // Fire and forget - ignore send errors
                let _ = session.sse_sender.try_send(event.clone());
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Update last activity for a session (call on any client interaction).
    pub fn touch_session(&self, session_id: SessionId) {
        if let Some(session) = self.connected_clients.write().get_session_mut(&session_id) {
            session.touch();
        }
    }
}

// Methods that spawn background tasks require 'static bounds
#[cfg(not(target_arch = "wasm32"))]
impl<R: Runtime, E: ClientEnv + 'static> SyncedNode<R, E> {
    /// Start the session timeout monitoring loop.
    ///
    /// This spawns a background task that periodically checks for timed-out
    /// sessions and removes them. Stale states are kept for a grace period
    /// to allow reconnection.
    pub fn start_timeout_monitor(self: &Rc<Self>) {
        let node = Rc::clone(self);
        let timeout = Duration::from_millis(self.config.session_timeout_ms);
        let check_interval = Duration::from_secs(10);
        let grace_period = Duration::from_secs(300); // 5 minutes

        self.runtime.spawn(async move {
            loop {
                tokio::time::sleep(check_interval).await;

                let expired = {
                    let mut clients = node.connected_clients.write();
                    let expired = clients.check_timeouts(timeout);
                    for session_id in &expired {
                        clients.remove_session(*session_id);
                    }
                    // Also clean up old stale states
                    clients.cleanup_stale_states(grace_period);
                    expired
                };

                if !expired.is_empty() {
                    // Log or notify about expired sessions if needed
                    let _ = expired; // Suppress unused warning
                }
            }
        });
    }
}

// ============================================================================
// Upstream Sync Event Loop
// ============================================================================

/// Result of processing upstream events.
#[derive(Debug)]
pub enum UpstreamEventResult {
    /// Event processed successfully.
    Ok,
    /// Stream ended (server closed connection).
    StreamEnded,
    /// Error occurred.
    Error(ClientError),
}

// Methods for upstream sync with reconnection
impl<R: Runtime, E: ClientEnv + Clone + 'static> SyncedNode<R, E> {
    /// Start the upstream sync event loop for a connection.
    ///
    /// Spawns a background task that:
    /// - Subscribes to the configured queries
    /// - Processes incoming SSE events
    /// - Handles disconnection with exponential backoff + jitter
    /// - Re-subscribes to all queries after reconnect
    ///
    /// The `queries` parameter specifies the initial queries to subscribe to.
    ///
    /// Note: This method requires `Rc<Self>` for the spawn. On platforms where
    /// this isn't available (e.g., WASM), use `upstream_event_loop` directly
    /// with your own spawn pattern.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_upstream_sync(
        self: &Rc<Self>,
        upstream_id: UpstreamId,
        queries: Vec<(String, SubscriptionOptions)>,
    ) {
        let node = Rc::clone(self);
        self.runtime.spawn(async move {
            node.upstream_event_loop(upstream_id, queries).await;
        });
    }

    /// Main upstream event loop with reconnection handling.
    ///
    /// This method runs the sync loop for a single upstream connection,
    /// handling subscriptions, event processing, and reconnection.
    ///
    /// On native platforms, use `start_upstream_sync` which spawns this
    /// in a background task. On WASM, spawn this manually using the runtime.
    pub async fn upstream_event_loop(
        &self,
        upstream_id: UpstreamId,
        initial_queries: Vec<(String, SubscriptionOptions)>,
    ) {
        let mut attempt: u32 = 0;

        loop {
            // Try to connect and subscribe
            match self
                .connect_and_subscribe(upstream_id, &initial_queries)
                .await
            {
                Ok(streams) => {
                    // Reset attempt counter on successful connection
                    attempt = 0;

                    // Update state to connected
                    if let Some(upstream) = self.upstream_servers.write().get_mut(upstream_id) {
                        upstream.set_state(UpstreamState::Connected);
                    }

                    // Process events until stream ends or error
                    let result = self.process_upstream_streams(upstream_id, streams).await;

                    match result {
                        UpstreamEventResult::Ok => {
                            // Normal completion - shouldn't happen in practice
                            continue;
                        }
                        UpstreamEventResult::StreamEnded => {
                            // Server closed connection - reconnect
                        }
                        UpstreamEventResult::Error(_err) => {
                            // Error occurred - reconnect
                        }
                    }
                }
                Err(_err) => {
                    // Connection failed - will retry
                }
            }

            // Check if max attempts exceeded
            if let Some(max) = self.config.reconnect.max_attempts
                && attempt >= max
            {
                // Give up
                if let Some(upstream) = self.upstream_servers.write().get_mut(upstream_id) {
                    upstream.set_state(UpstreamState::Disconnected);
                }
                return;
            }

            // Calculate delay with jitter using runtime's random
            let delay_ms = calculate_reconnect_delay_with_jitter(
                attempt,
                &self.config.reconnect,
                self.runtime.random_f64(),
            );

            // Update state to reconnecting
            if let Some(upstream) = self.upstream_servers.write().get_mut(upstream_id) {
                upstream.set_state(UpstreamState::Reconnecting {
                    attempt,
                    next_delay_ms: delay_ms,
                });
            }

            // Wait before retry using runtime's sleep
            self.runtime.sleep(delay_ms).await;

            attempt += 1;
        }
    }
}

// ============================================================================
// WASM-safe event loop (avoids holding RefCell borrows across await points)
// ============================================================================

/// Run the upstream event loop without holding a persistent borrow.
///
/// This is designed for WASM where RefCell borrows cannot be held across
/// await points without blocking other operations on the same SyncedNode.
pub async fn run_upstream_event_loop<R: Runtime, E: ClientEnv>(
    synced_node: Shared<SyncedNode<R, E>>,
    upstream_id: UpstreamId,
    initial_queries: Vec<(String, SubscriptionOptions)>,
) {
    let mut attempt: u32 = 0;

    loop {
        // Try to connect and subscribe (borrow released between operations)
        let connect_result =
            connect_and_subscribe_standalone(synced_node.clone(), upstream_id, &initial_queries)
                .await;

        match connect_result {
            Ok(streams) => {
                // Reset attempt counter on successful connection
                attempt = 0;

                // Update state to connected
                {
                    let node = synced_node.read();
                    if let Some(upstream) = node.upstream_servers.write().get_mut(upstream_id) {
                        upstream.set_state(UpstreamState::Connected);
                    }
                }

                // Process events until stream ends or error
                let result =
                    process_upstream_streams_standalone(synced_node.clone(), upstream_id, streams)
                        .await;

                match result {
                    UpstreamEventResult::Ok => {
                        continue;
                    }
                    UpstreamEventResult::StreamEnded => {
                        // Server closed connection - reconnect
                    }
                    UpstreamEventResult::Error(_err) => {
                        // Error occurred - reconnect
                    }
                }
            }
            Err(_err) => {
                // Connection failed - will retry
            }
        }

        // Check if max attempts exceeded
        let should_give_up = {
            let node = synced_node.read();
            if let Some(max) = node.config.reconnect.max_attempts {
                if attempt >= max {
                    if let Some(upstream) = node.upstream_servers.write().get_mut(upstream_id) {
                        upstream.set_state(UpstreamState::Disconnected);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if should_give_up {
            return;
        }

        // Calculate delay with jitter
        let delay_ms = {
            let node = synced_node.read();
            let delay = calculate_reconnect_delay_with_jitter(
                attempt,
                &node.config.reconnect,
                node.runtime.random_f64(),
            );

            // Update state to reconnecting
            if let Some(upstream) = node.upstream_servers.write().get_mut(upstream_id) {
                upstream.set_state(UpstreamState::Reconnecting {
                    attempt,
                    next_delay_ms: delay,
                });
            }

            delay
        };

        // Wait before retry (no borrow held during sleep)
        let sleep_future = {
            let node = synced_node.read();
            node.runtime.sleep(delay_ms)
        };
        sleep_future.await;

        attempt += 1;
    }
}

/// Connect and subscribe to all queries without holding a persistent borrow.
async fn connect_and_subscribe_standalone<R: Runtime, E: ClientEnv>(
    synced_node: Shared<SyncedNode<R, E>>,
    upstream_id: UpstreamId,
    queries: &[(String, SubscriptionOptions)],
) -> Result<
    Vec<(
        u32,
        futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
    )>,
    ClientError,
> {
    let mut streams = Vec::new();

    // Clear old subscriptions and set state
    {
        let node = synced_node.read();
        let mut servers = node.upstream_servers.write();
        if let Some(upstream) = servers.get_mut(upstream_id) {
            upstream.clear_subscriptions();
            upstream.set_state(UpstreamState::Connecting);
        }
    }

    // Subscribe to each query
    for (query, options) in queries {
        // Get the env clone outside the lock
        let env = {
            let node = synced_node.read();
            let servers = node.upstream_servers.read();
            let upstream = servers
                .get(upstream_id)
                .ok_or(ClientError::new(0, "Upstream not connected"))?;
            upstream.env().clone()
        };

        // Create subscribe request
        let request = SubscribeRequest {
            query: query.clone(),
            options: options.clone(),
        };

        // Make the subscribe call without holding any locks
        let stream = env.subscribe(request).await?;

        // Update subscription tracking
        {
            let node = synced_node.read();
            let mut servers = node.upstream_servers.write();
            if let Some(upstream) = servers.get_mut(upstream_id) {
                let sub_id = upstream.add_subscription(query.clone(), options.clone());
                upstream.set_state(UpstreamState::Connected);
                streams.push((sub_id, stream));
            }
        }
    }

    Ok(streams)
}

/// Push an object to upstream without holding a persistent borrow.
///
/// This is designed for WASM where RefCell borrows cannot be held across
/// await points.
pub async fn push_object_standalone<R: Runtime, E: ClientEnv>(
    synced_node: Shared<SyncedNode<R, E>>,
    upstream_id: UpstreamId,
    object_id: ObjectId,
    branch: &str,
) -> Result<PushResponse, ClientError> {
    // Get server's known frontier and env (all in one borrow)
    let (server_frontier, env) = {
        let node = synced_node.read();
        let servers = node.upstream_servers.read();
        let upstream = servers
            .get(upstream_id)
            .ok_or(ClientError::new(0, "Upstream not connected"))?;
        (
            upstream
                .server_known_state(&object_id)
                .cloned()
                .unwrap_or_default(),
            upstream.env().clone(),
        )
    };

    // Get local frontier and commits to send
    let (_local_frontier, commits, object_meta) = {
        let node = synced_node.read();
        let local_frontier = node
            .node
            .frontier(object_id, branch)
            .ok()
            .flatten()
            .unwrap_or_default();

        // Check if we need to push anything
        if compare_frontiers(&local_frontier, &server_frontier) == FrontierComparison::Identical {
            return Ok(PushResponse {
                object_id,
                accepted: true,
                frontier: local_frontier,
            });
        }

        // Get the branch to find commits to send
        let obj = node
            .node
            .get_object(object_id)
            .ok_or(ClientError::new(0, "Object not found"))?;
        let obj_read = obj
            .read()
            .map_err(|_| ClientError::new(0, "Object lock poisoned"))?;
        let branch_ref = obj_read
            .branch_ref(branch)
            .ok_or(ClientError::new(0, "Branch not found"))?;
        let branch_read = branch_ref
            .read()
            .map_err(|_| ClientError::new(0, "Branch lock poisoned"))?;

        let commits = commits_to_send(&branch_read, &local_frontier, &server_frontier);

        if commits.is_empty() {
            return Ok(PushResponse {
                object_id,
                accepted: true,
                frontier: local_frontier,
            });
        }

        // Include object metadata on first push (when server has no known state)
        let object_meta = if server_frontier.is_empty() {
            obj_read.meta.clone()
        } else {
            None
        };

        (local_frontier, commits, object_meta)
    };

    let request = PushRequest {
        object_id,
        commits,
        object_meta,
    };

    // Send push request (no synced_node borrow held during the async call)
    let response = env.push(request).await?;

    // Update server known state on success
    if response.accepted {
        let node = synced_node.read();
        let mut servers = node.upstream_servers.write();
        if let Some(upstream) = servers.get_mut(upstream_id) {
            upstream.update_server_known_state(object_id, response.frontier.clone());
        }
    }

    Ok(response)
}

/// Process upstream streams without holding a persistent borrow.
async fn process_upstream_streams_standalone<R: Runtime, E: ClientEnv>(
    synced_node: Shared<SyncedNode<R, E>>,
    upstream_id: UpstreamId,
    streams: Vec<(
        u32,
        futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
    )>,
) -> UpstreamEventResult {
    use futures::StreamExt;

    // Merge all streams into one
    let mut merged = futures::stream::select_all(
        streams
            .into_iter()
            .map(|(sub_id, stream)| stream.map(move |r| (sub_id, r))),
    );

    while let Some((sub_id, result)) = merged.next().await {
        match result {
            Ok(event) => {
                // Handle event with a scoped borrow
                let node = synced_node.read();
                node.handle_upstream_event(upstream_id, sub_id, event);
            }
            Err(e) => {
                return UpstreamEventResult::Error(e);
            }
        }
    }

    UpstreamEventResult::StreamEnded
}

impl<R: Runtime, E: ClientEnv> SyncedNode<R, E> {
    /// Connect and subscribe to all queries.
    async fn connect_and_subscribe(
        &self,
        upstream_id: UpstreamId,
        queries: &[(String, SubscriptionOptions)],
    ) -> Result<
        Vec<(
            u32,
            futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
        )>,
        ClientError,
    > {
        let mut streams = Vec::new();

        // Clear old subscriptions and set state
        {
            let mut servers = self.upstream_servers.write();
            if let Some(upstream) = servers.get_mut(upstream_id) {
                upstream.clear_subscriptions();
                upstream.set_state(UpstreamState::Connecting);
            }
        }

        // Subscribe to each query
        for (query, options) in queries {
            // Get the env clone outside the lock
            let env = {
                let servers = self.upstream_servers.read();
                let upstream = servers
                    .get(upstream_id)
                    .ok_or(ClientError::new(0, "Upstream not connected"))?;
                upstream.env().clone()
            };

            // Create subscribe request
            let request = SubscribeRequest {
                query: query.clone(),
                options: options.clone(),
            };

            // Make the subscribe call without holding any locks
            let stream = env.subscribe(request).await?;

            // Update subscription tracking
            {
                let mut servers = self.upstream_servers.write();
                if let Some(upstream) = servers.get_mut(upstream_id) {
                    let sub_id = upstream.add_subscription(query.clone(), options.clone());
                    upstream.set_state(UpstreamState::Connected);
                    streams.push((sub_id, stream));
                }
            }
        }

        Ok(streams)
    }

    /// Process events from all subscription streams.
    async fn process_upstream_streams(
        &self,
        upstream_id: UpstreamId,
        streams: Vec<(
            u32,
            futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
        )>,
    ) -> UpstreamEventResult {
        use futures::StreamExt;

        // Merge all streams into one
        let mut merged = futures::stream::select_all(
            streams
                .into_iter()
                .map(|(sub_id, stream)| stream.map(move |r| (sub_id, r))),
        );

        while let Some((sub_id, result)) = merged.next().await {
            match result {
                Ok(event) => {
                    self.handle_upstream_event(upstream_id, sub_id, event);
                }
                Err(e) => {
                    return UpstreamEventResult::Error(e);
                }
            }
        }

        // All streams ended
        UpstreamEventResult::StreamEnded
    }

    /// Handle a single SSE event from upstream.
    fn handle_upstream_event(&self, upstream_id: UpstreamId, sub_id: u32, event: SseEvent) {
        match event {
            SseEvent::Commits {
                object_id,
                commits,
                frontier,
                object_meta,
            } => {
                // Track this object in the subscription
                if let Some(upstream) = self.upstream_servers.write().get_mut(upstream_id) {
                    upstream.track_object(sub_id, object_id);
                }

                // Apply commits (ensure_object is called inside apply_upstream_commits)
                self.apply_upstream_commits(upstream_id, object_id, commits, frontier, object_meta);
            }
            SseEvent::Excluded { object_id } => {
                // Object no longer matches query - could clean up local tracking
                let _ = object_id;
            }
            SseEvent::Truncate {
                object_id,
                truncate_at: _,
            } => {
                // Handle truncation if needed
                let _ = object_id;
            }
            SseEvent::Request {
                object_id: _,
                commit_ids: _,
            } => {
                // Server requesting specific commits - push them
                // TODO: Implement push response
            }
            SseEvent::Error {
                message: _,
                code: _,
            } => {
                // Log error
            }
        }
    }

    /// Subscribe to a query on an upstream server.
    ///
    /// This is a convenience method for subscribing to a single query
    /// without using the full event loop.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn subscribe_upstream(
        &self,
        upstream_id: UpstreamId,
        query: String,
        options: SubscriptionOptions,
    ) -> Result<
        (
            u32,
            futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>,
        ),
        ClientError,
    > {
        let mut servers = self.upstream_servers.write();
        let upstream = servers
            .get_mut(upstream_id)
            .ok_or(ClientError::new(0, "Upstream not connected"))?;
        upstream.subscribe(query, options).await
    }

    /// Push commits to an upstream server.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn push_upstream(
        &self,
        upstream_id: UpstreamId,
        request: PushRequest,
    ) -> Result<PushResponse, ClientError> {
        let mut servers = self.upstream_servers.write();
        let upstream = servers
            .get_mut(upstream_id)
            .ok_or(ClientError::new(0, "Upstream not connected"))?;
        upstream.push(request).await
    }

    /// Push an object's commits to an upstream server.
    ///
    /// This creates the push request automatically by computing the delta
    /// between local and server-known commits.
    #[allow(clippy::await_holding_refcell_ref)]
    #[allow(clippy::await_holding_lock)]
    pub async fn push_object(
        &self,
        upstream_id: UpstreamId,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<PushResponse, ClientError> {
        // Get server's known frontier (empty if unknown)
        let server_frontier = {
            let servers = self.upstream_servers.read();
            let upstream = servers
                .get(upstream_id)
                .ok_or(ClientError::new(0, "Upstream not connected"))?;
            upstream
                .server_known_state(&object_id)
                .cloned()
                .unwrap_or_default()
        };

        // Get local frontier
        let local_frontier = self
            .node
            .frontier(object_id, branch)
            .ok()
            .flatten()
            .unwrap_or_default();

        // Check if we need to push anything
        if compare_frontiers(&local_frontier, &server_frontier) == FrontierComparison::Identical {
            return Ok(PushResponse {
                object_id,
                accepted: true,
                frontier: local_frontier,
            });
        }

        // Get the branch to find commits to send
        let obj = self
            .node
            .get_object(object_id)
            .ok_or(ClientError::new(0, "Object not found"))?;
        let obj_read = obj
            .read()
            .map_err(|_| ClientError::new(0, "Object lock poisoned"))?;
        let branch_ref = obj_read
            .branch_ref(branch)
            .ok_or(ClientError::new(0, "Branch not found"))?;
        let branch_read = branch_ref
            .read()
            .map_err(|_| ClientError::new(0, "Branch lock poisoned"))?;

        let commits = commits_to_send(&branch_read, &local_frontier, &server_frontier);

        if commits.is_empty() {
            return Ok(PushResponse {
                object_id,
                accepted: true,
                frontier: local_frontier,
            });
        }

        // Include object metadata on first push (when server has no known state)
        let object_meta = if server_frontier.is_empty() {
            obj_read.meta.clone()
        } else {
            None
        };

        let request = PushRequest {
            object_id,
            commits,
            object_meta,
        };

        drop(branch_read);
        drop(obj_read);

        // Send push request
        let response = {
            let mut servers = self.upstream_servers.write();
            let upstream = servers
                .get_mut(upstream_id)
                .ok_or(ClientError::new(0, "Upstream not connected"))?;
            upstream.push(request).await?
        };

        // Update server known state on success
        if response.accepted {
            let mut servers = self.upstream_servers.write();
            if let Some(upstream) = servers.get_mut(upstream_id) {
                upstream.update_server_known_state(object_id, response.frontier.clone());
            }
        }

        Ok(response)
    }
}
