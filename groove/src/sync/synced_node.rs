//! SyncedNode - LocalNode with sync capabilities.
//!
//! SyncedNode wraps a LocalNode and adds:
//! - Upstream server connections (servers we sync TO)
//! - Connected client sessions (clients that sync FROM us)
//! - Automatic write batching/debouncing
//! - Automatic SSE event application

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::commit::CommitId;
use crate::node::LocalNode;
use crate::object::ObjectId;

use super::client::ReconnectConfig;
use super::env::{ClientEnv, ClientError};
use super::protocol::{
    PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest, SubscriptionOptions,
};
use super::runtime::Runtime;

#[cfg(feature = "sync-server")]
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

#[cfg(feature = "sync-server")]
/// Manager for connected client sessions (clients that sync FROM us).
pub struct ConnectedClients {
    /// Active client sessions.
    sessions: HashMap<SessionId, ClientSession>,

    /// Reverse index: object -> sessions tracking it.
    object_sessions: HashMap<ObjectId, std::collections::HashSet<SessionId>>,

    /// Identity -> sessions mapping.
    identity_sessions: HashMap<String, std::collections::HashSet<SessionId>>,

    /// Token validator.
    token_validator: Option<Arc<dyn TokenValidator>>,

    /// Next session ID.
    next_session_id: u64,

    /// Stale client_known_state (kept after session removal for reconnection).
    stale_states: HashMap<String, StaleClientState>,
}

#[cfg(feature = "sync-server")]
/// Stale client state kept for grace period after disconnect.
pub struct StaleClientState {
    /// The client's known state at disconnect.
    pub known_state: HashMap<ObjectId, Vec<CommitId>>,
    /// When the session was removed.
    pub removed_at: Instant,
}

#[cfg(feature = "sync-server")]
impl Default for ConnectedClients {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "sync-server")]
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
    pub fn set_token_validator(&mut self, validator: Arc<dyn TokenValidator>) {
        self.token_validator = Some(validator);
    }

    /// Accept a new client session.
    pub fn accept_session(&mut self, identity: ClientIdentity, sse_sender: SseSender) -> SessionId {
        let id = SessionId(self.next_session_id);
        self.next_session_id += 1;

        let session = ClientSession::new(identity.clone(), sse_sender);

        // Track by identity
        self.identity_sessions
            .entry(identity.id.clone())
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
            if let Some(sessions) = self.identity_sessions.get_mut(&session.identity.id) {
                sessions.remove(&id);
            }

            // Remove from object index
            for (_, sessions) in &mut self.object_sessions {
                sessions.remove(&id);
            }

            // Keep stale state for reconnection
            self.stale_states.insert(
                session.identity.id.clone(),
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

/// A LocalNode with sync capabilities.
///
/// SyncedNode wraps a LocalNode and adds:
/// - Connections to upstream servers (servers we sync TO)
/// - Sessions from connected clients (clients that sync FROM us)
/// - Automatic write batching and debouncing
/// - Automatic SSE event application
pub struct SyncedNode<R: Runtime, E: ClientEnv> {
    /// The underlying LocalNode.
    node: Arc<LocalNode>,

    /// Runtime for spawning async tasks.
    runtime: R,

    /// Connections to upstream servers.
    upstream_servers: RwLock<UpstreamServers<E>>,

    /// Connected client sessions (server-side).
    #[cfg(feature = "sync-server")]
    connected_clients: RwLock<ConnectedClients>,

    /// Write buffer for batching upstream pushes.
    write_buffer: RwLock<WriteBuffer>,

    /// Sync configuration.
    config: SyncConfig,
}

impl<R: Runtime, E: ClientEnv> SyncedNode<R, E> {
    /// Create a new SyncedNode.
    pub fn new(node: Arc<LocalNode>, runtime: R) -> Self {
        Self {
            node,
            runtime,
            upstream_servers: RwLock::new(UpstreamServers::new()),
            #[cfg(feature = "sync-server")]
            connected_clients: RwLock::new(ConnectedClients::new()),
            write_buffer: RwLock::new(WriteBuffer::new()),
            config: SyncConfig::default(),
        }
    }

    /// Create a new SyncedNode with custom configuration.
    pub fn with_config(node: Arc<LocalNode>, runtime: R, config: SyncConfig) -> Self {
        Self {
            node,
            runtime,
            upstream_servers: RwLock::new(UpstreamServers::new()),
            #[cfg(feature = "sync-server")]
            connected_clients: RwLock::new(ConnectedClients::new()),
            write_buffer: RwLock::new(WriteBuffer::new()),
            config,
        }
    }

    /// Get a reference to the underlying LocalNode.
    pub fn inner(&self) -> &Arc<LocalNode> {
        &self.node
    }

    /// Get the runtime.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Get the sync configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    // ========== Upstream Server API ==========

    /// Add an upstream server connection.
    ///
    /// Returns the UpstreamId for the new connection.
    pub fn add_upstream(&self, env: E) -> UpstreamId {
        self.upstream_servers.write().unwrap().add(env)
    }

    /// Remove an upstream server connection.
    pub fn remove_upstream(&self, id: UpstreamId) -> bool {
        self.upstream_servers.write().unwrap().remove(id).is_some()
    }

    /// Get all upstream server IDs.
    pub fn upstream_ids(&self) -> Vec<UpstreamId> {
        self.upstream_servers.read().unwrap().ids().collect()
    }

    /// Check if there are any upstream connections.
    pub fn has_upstream(&self) -> bool {
        !self.upstream_servers.read().unwrap().is_empty()
    }

    /// Get the state of an upstream connection.
    pub fn upstream_state(&self, id: UpstreamId) -> Option<UpstreamState> {
        self.upstream_servers
            .read()
            .unwrap()
            .get(id)
            .map(|u| u.state().clone())
    }

    // ========== Connected Clients API (Server-side) ==========

    #[cfg(feature = "sync-server")]
    /// Set the token validator for accepting client connections.
    pub fn set_token_validator(&self, validator: Arc<dyn TokenValidator>) {
        self.connected_clients
            .write()
            .unwrap()
            .set_token_validator(validator);
    }

    #[cfg(feature = "sync-server")]
    /// Accept a new client session.
    pub fn accept_client(&self, identity: ClientIdentity, sse_sender: SseSender) -> SessionId {
        self.connected_clients
            .write()
            .unwrap()
            .accept_session(identity, sse_sender)
    }

    #[cfg(feature = "sync-server")]
    /// Remove a client session.
    pub fn remove_client(&self, session_id: SessionId) {
        self.connected_clients
            .write()
            .unwrap()
            .remove_session(session_id);
    }

    // ========== Write Buffer API ==========

    /// Queue an object for upstream push.
    pub fn queue_for_push(&self, object_id: ObjectId, branch: &str) {
        self.write_buffer.write().unwrap().add(object_id, branch);
    }

    /// Get objects ready to push.
    pub fn ready_to_push(&self) -> Vec<ObjectId> {
        self.write_buffer
            .read()
            .unwrap()
            .ready_to_push(self.config.write_debounce_ms, self.config.max_batch_age_ms)
    }

    /// Mark an object as pushed (remove from buffer).
    pub fn mark_pushed(&self, object_id: &ObjectId) {
        self.write_buffer.write().unwrap().remove(object_id);
    }

    // ========== High-level Operations ==========

    /// Apply commits received from an upstream server.
    ///
    /// This is called automatically by the SSE event loop.
    pub fn apply_upstream_commits(
        &self,
        upstream_id: UpstreamId,
        object_id: ObjectId,
        commits: Vec<crate::commit::Commit>,
        frontier: Vec<CommitId>,
    ) {
        // Apply to local storage
        self.node.apply_commits(object_id, "main", commits.clone());

        // Update upstream's known state
        if let Some(upstream) = self.upstream_servers.write().unwrap().get_mut(upstream_id) {
            upstream.update_server_known_state(object_id, frontier.clone());
        }

        // Broadcast to connected clients (for edge server scenario)
        #[cfg(feature = "sync-server")]
        {
            let event = SseEvent::Commits {
                object_id,
                commits,
                frontier,
                object_meta: None, // TODO: Include metadata for first sync
            };
            self.broadcast_to_clients(object_id, &event);
        }
    }

    #[cfg(feature = "sync-server")]
    /// Broadcast an event to all clients tracking an object.
    pub fn broadcast_to_clients(&self, object_id: ObjectId, event: &SseEvent) {
        let clients = self.connected_clients.read().unwrap();
        let session_ids = clients.sessions_for_object(&object_id);

        for session_id in session_ids {
            if let Some(session) = clients.get_session(&session_id) {
                // Fire and forget - ignore send errors
                let _ = session.sse_sender.try_send(event.clone());
            }
        }
    }

    #[cfg(feature = "sync-server")]
    /// Update last activity for a session (call on any client interaction).
    pub fn touch_session(&self, session_id: SessionId) {
        if let Some(session) = self
            .connected_clients
            .write()
            .unwrap()
            .get_session_mut(&session_id)
        {
            session.touch();
        }
    }
}

// Methods that spawn background tasks require 'static bounds
#[cfg(feature = "sync-server")]
impl<R: Runtime, E: ClientEnv + 'static> SyncedNode<R, E> {
    /// Start the session timeout monitoring loop.
    ///
    /// This spawns a background task that periodically checks for timed-out
    /// sessions and removes them. Stale states are kept for a grace period
    /// to allow reconnection.
    pub fn start_timeout_monitor(self: &Arc<Self>) {
        let node = Arc::clone(self);
        let timeout = Duration::from_millis(self.config.session_timeout_ms);
        let check_interval = Duration::from_secs(10);
        let grace_period = Duration::from_secs(300); // 5 minutes

        self.runtime.spawn(async move {
            loop {
                tokio::time::sleep(check_interval).await;

                let expired = {
                    let mut clients = node.connected_clients.write().unwrap();
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
// Reconnection Logic
// ============================================================================

/// Calculate the next reconnection delay with exponential backoff.
pub fn calculate_reconnect_delay(attempt: u32, config: &ReconnectConfig) -> u64 {
    let delay = (config.initial_delay_ms as f64) * config.backoff_multiplier.powi(attempt as i32);
    (delay as u64).min(config.max_delay_ms)
}
