//! Runtime-less sync engine with explicit inboxes/outboxes.
//!
//! This module implements a synchronous state machine architecture for sync.
//! Instead of async event loops, all state transitions are driven by a `pass()` function
//! that processes inboxes and produces outboxes. External "drivers" handle I/O and
//! feed events back into the engine.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        DRIVER                                │
//! │  (Platform-specific: WASM, Native CLI, Server)              │
//! │                                                              │
//! │  • Receives I/O events (SSE, HTTP responses, timers)        │
//! │  • Puts events into INBOXES                                  │
//! │  • Calls `pass()` on core state machine                      │
//! │  • Takes actions from OUTBOXES                               │
//! │  • Makes HTTP requests, opens SSE connections                │
//! └──────────────────────────┬──────────────────────────────────┘
//!                            │
//!                     pass() │ (synchronous)
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SYNC ENGINE                               │
//! │  (Pure, synchronous, no async, no spawning)                 │
//! │                                                              │
//! │  LocalNode + SyncState + Database                           │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use crate::commit::{Commit, CommitId};
use crate::node::LocalNode;
use crate::object::ObjectId;

use super::protocol::{PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscriptionOptions};

// ============================================================================
// Identifiers
// ============================================================================

/// Unique identifier for an upstream server connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpstreamId(pub u64);

/// Unique identifier for a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(pub u64);

/// Unique identifier for a query subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)] // Part of intended API, will be used for query subscriptions
pub struct QueryId(pub u64);

// ============================================================================
// Connection State
// ============================================================================

/// State of an upstream connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected.
    Disconnected,
    /// Attempting to connect.
    Connecting,
    /// Connected and syncing.
    Connected,
    /// Reconnecting after disconnect.
    Reconnecting { attempt: u32, next_delay_ms: u64 },
}

// ============================================================================
// Inboxes (External → Engine)
// ============================================================================

/// All incoming events for a pass.
#[derive(Default)]
pub struct Inboxes {
    /// Local writes from application layer.
    pub local_writes: Vec<LocalWriteEvent>,

    /// SSE events from upstream servers.
    pub sse_events: Vec<SseInboxEvent>,

    /// Responses to push requests.
    pub push_responses: Vec<PushResponseEvent>,

    /// Timer/clock ticks for debouncing, timeouts.
    pub tick: Option<TickEvent>,

    /// Connection state changes (from driver).
    pub connection_events: Vec<ConnectionEvent>,

    /// Subscribe requests from application layer.
    pub subscribe_requests: Vec<SubscribeRequestEvent>,
}

/// A local write event from the application layer.
#[derive(Debug, Clone)]
pub struct LocalWriteEvent {
    pub object_id: ObjectId,
    pub branch: String,
    pub content: Vec<u8>,
    pub author: String,
    pub timestamp: u64,
}

/// An SSE event received from an upstream server.
#[derive(Debug, Clone)]
pub struct SseInboxEvent {
    pub upstream_id: UpstreamId,
    pub subscription_id: u32,
    pub event: SseEvent,
}

/// A push response received from an upstream server.
#[derive(Debug, Clone)]
pub struct PushResponseEvent {
    pub upstream_id: UpstreamId,
    pub object_id: ObjectId,
    pub result: Result<PushResponse, String>,
}

/// A tick event with current timestamp.
#[derive(Debug, Clone)]
pub struct TickEvent {
    /// Current time in milliseconds since epoch.
    pub now_ms: u64,
}

/// A connection state change from the driver.
#[derive(Debug, Clone)]
pub struct ConnectionEvent {
    pub upstream_id: UpstreamId,
    pub event: ConnectionEventKind,
}

/// Kind of connection event.
#[derive(Debug, Clone)]
pub enum ConnectionEventKind {
    /// SSE stream opened successfully.
    StreamOpened { subscription_id: u32 },
    /// SSE stream closed or errored.
    StreamClosed {
        subscription_id: u32,
        error: Option<String>,
    },
    /// Connection attempt failed.
    ConnectFailed { error: String },
}

/// A subscribe request from the application layer.
#[derive(Debug, Clone)]
pub struct SubscribeRequestEvent {
    pub upstream_id: UpstreamId,
    pub query: String,
    pub options: SubscriptionOptions,
}

// ============================================================================
// Outboxes (Engine → External)
// ============================================================================

/// All outgoing actions from a pass.
#[derive(Default)]
pub struct Outboxes {
    /// HTTP requests to make.
    pub requests: Vec<OutboundRequest>,

    /// SSE streams to open/close.
    pub stream_actions: Vec<StreamAction>,

    /// Notifications for external subscribers.
    pub notifications: Vec<Notification>,

    /// Timer requests.
    pub timers: Vec<TimerRequest>,

    /// Storage requests (fire-and-forget persistence).
    pub storage: Vec<StorageRequest>,
}

/// A storage request for persistence.
///
/// These are fire-and-forget operations - the driver executes them
/// asynchronously and doesn't report results back. This matches the
/// previous `spawn_persist` behavior but makes storage explicit.
#[derive(Debug, Clone)]
pub enum StorageRequest {
    /// Persist a commit.
    PutCommit { commit: Commit },
    /// Update the frontier for an object's branch.
    SetFrontier {
        object_id: ObjectId,
        branch: String,
        frontier: Vec<CommitId>,
    },
}

/// An outbound HTTP request.
#[derive(Debug, Clone)]
pub enum OutboundRequest {
    /// Push commits to upstream.
    Push {
        upstream_id: UpstreamId,
        request: PushRequest,
    },
    /// Request reconciliation.
    Reconcile {
        upstream_id: UpstreamId,
        request: ReconcileRequest,
    },
    /// Unsubscribe from a query.
    Unsubscribe {
        upstream_id: UpstreamId,
        subscription_id: u32,
    },
}

/// An SSE stream action.
#[derive(Debug, Clone)]
pub enum StreamAction {
    /// Open a new SSE stream.
    Open {
        upstream_id: UpstreamId,
        subscription_id: u32,
        query: String,
        options: SubscriptionOptions,
    },
    /// Close an existing SSE stream.
    Close {
        upstream_id: UpstreamId,
        subscription_id: u32,
    },
}

/// A notification for external subscribers.
#[derive(Debug, Clone)]
pub enum Notification {
    /// Objects received from sync (for Database layer).
    ObjectsReceived {
        object_id: ObjectId,
        commits: Vec<Commit>,
        object_meta: Option<BTreeMap<String, String>>,
    },
    /// Connection state changed.
    ConnectionStateChanged {
        upstream_id: UpstreamId,
        state: ConnectionState,
    },
}

/// Purpose of a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerPurpose {
    /// Reconnection attempt.
    Reconnect,
    /// Write debounce.
    Debounce,
    /// Session timeout check.
    SessionTimeout,
}

/// A timer request.
#[derive(Debug, Clone)]
pub struct TimerRequest {
    pub id: TimerId,
    pub delay_ms: u64,
    pub purpose: TimerPurpose,
}

// ============================================================================
// Sync State
// ============================================================================

/// State for a single upstream connection.
pub struct UpstreamState {
    /// Connection state.
    pub connection: ConnectionState,

    /// Active subscriptions: subscription_id -> query info.
    pub subscriptions: HashMap<u32, SubscriptionState>,

    /// Next subscription ID.
    next_subscription_id: u32,

    /// Server's assumed known state per object (for delta calculation).
    pub server_known_state: HashMap<ObjectId, Vec<CommitId>>,
}

impl Default for UpstreamState {
    fn default() -> Self {
        Self {
            connection: ConnectionState::Disconnected,
            subscriptions: HashMap::new(),
            next_subscription_id: 1,
            server_known_state: HashMap::new(),
        }
    }
}

impl UpstreamState {
    /// Allocate a new subscription ID.
    pub fn next_subscription_id(&mut self) -> u32 {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }
}

/// State for a single subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionState {
    /// The SQL query.
    pub query: String,
    /// Subscription options.
    pub options: SubscriptionOptions,
    /// Objects received via this subscription.
    pub objects: HashSet<ObjectId>,
}

/// Pending write tracking for debouncing.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub object_id: ObjectId,
    pub branch: String,
    /// Timestamp of first write (ms since epoch).
    pub first_write_ms: u64,
    /// Timestamp of last write (ms since epoch).
    pub last_write_ms: u64,
}

/// Configuration for sync behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Debounce delay before pushing writes upstream (ms).
    pub write_debounce_ms: u64,
    /// Force push after this delay regardless of debounce (ms).
    pub max_write_age_ms: u64,
    /// Base delay for reconnection (ms).
    pub reconnect_base_delay_ms: u64,
    /// Maximum reconnection delay (ms).
    pub reconnect_max_delay_ms: u64,
    /// Maximum reconnection attempts before giving up.
    pub reconnect_max_attempts: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            write_debounce_ms: 100,
            max_write_age_ms: 1000,
            reconnect_base_delay_ms: 1000,
            reconnect_max_delay_ms: 30_000,
            reconnect_max_attempts: 10,
        }
    }
}

// ============================================================================
// Sync Engine
// ============================================================================

/// The runtime-less sync engine.
///
/// This is a pure state machine that processes inboxes and produces outboxes.
/// All I/O is handled externally by a "driver".
pub struct SyncEngine {
    /// Local object storage (shared with Database).
    pub local_node: Rc<LocalNode>,

    /// Configuration.
    pub config: SyncConfig,

    /// Upstream server states.
    upstreams: HashMap<UpstreamId, UpstreamState>,

    /// Next upstream ID.
    next_upstream_id: u64,

    /// Pending writes awaiting push.
    pending_writes: HashMap<ObjectId, PendingWrite>,

    /// Next timer ID.
    next_timer_id: u64,
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncEngine {
    /// Create a new sync engine with default configuration.
    pub fn new() -> Self {
        Self {
            local_node: Rc::new(LocalNode::default()),
            config: SyncConfig::default(),
            upstreams: HashMap::new(),
            next_upstream_id: 1,
            pending_writes: HashMap::new(),
            next_timer_id: 1,
        }
    }

    /// Create a sync engine with a shared LocalNode.
    pub fn with_local_node(local_node: Rc<LocalNode>) -> Self {
        Self {
            local_node,
            config: SyncConfig::default(),
            upstreams: HashMap::new(),
            next_upstream_id: 1,
            pending_writes: HashMap::new(),
            next_timer_id: 1,
        }
    }

    /// Add an upstream server. Returns its ID.
    pub fn add_upstream(&mut self) -> UpstreamId {
        let id = UpstreamId(self.next_upstream_id);
        self.next_upstream_id += 1;
        self.upstreams.insert(id, UpstreamState::default());
        id
    }

    /// Check if there are pending writes awaiting push.
    pub fn has_pending_writes(&self) -> bool {
        !self.pending_writes.is_empty()
    }

    /// Get an upstream state by ID.
    pub fn upstream(&self, id: UpstreamId) -> Option<&UpstreamState> {
        self.upstreams.get(&id)
    }

    /// Get a mutable upstream state by ID.
    pub fn upstream_mut(&mut self, id: UpstreamId) -> Option<&mut UpstreamState> {
        self.upstreams.get_mut(&id)
    }

    /// Allocate a new timer ID.
    fn next_timer_id(&mut self) -> TimerId {
        let id = TimerId(self.next_timer_id);
        self.next_timer_id += 1;
        id
    }

    // ========================================================================
    // Pass: The main synchronous entry point
    // ========================================================================

    /// Process all pending inboxes synchronously.
    /// Returns outboxes with actions for the driver to execute.
    pub fn pass(&mut self, inboxes: Inboxes) -> Outboxes {
        let mut outboxes = Outboxes::default();

        // 1. Process subscribe requests → open SSE streams
        for request in inboxes.subscribe_requests {
            self.process_subscribe_request(request, &mut outboxes);
        }

        // 2. Process connection events → update connection state
        for event in inboxes.connection_events {
            self.process_connection_event(event, &mut outboxes);
        }

        // 3. Process local writes from inbox (if any)
        for write in inboxes.local_writes {
            self.process_local_write(write);
        }

        // 4. Drain changed objects from LocalNode → add to pending_writes
        self.drain_changed_objects(&mut outboxes);

        // 5. Process SSE events → apply commits, update state
        for sse in inboxes.sse_events {
            self.process_sse_event(sse, &mut outboxes);
        }

        // 6. Process push responses → update server_known_state
        for response in inboxes.push_responses {
            self.process_push_response(response, &mut outboxes);
        }

        // 7. Process tick → check debounce timers, generate push requests
        if let Some(tick) = inboxes.tick {
            self.process_tick(tick, &mut outboxes);
        }

        // 8. Drain storage requests from LocalNode
        outboxes
            .storage
            .extend(self.local_node.drain_storage_requests());

        outboxes
    }

    // ========================================================================
    // Event Processors
    // ========================================================================

    fn process_subscribe_request(
        &mut self,
        request: SubscribeRequestEvent,
        outboxes: &mut Outboxes,
    ) {
        let upstream = match self.upstreams.get_mut(&request.upstream_id) {
            Some(u) => u,
            None => return,
        };

        let sub_id = upstream.next_subscription_id();

        // Track the subscription
        upstream.subscriptions.insert(
            sub_id,
            SubscriptionState {
                query: request.query.clone(),
                options: request.options.clone(),
                objects: HashSet::new(),
            },
        );

        // Update connection state
        upstream.connection = ConnectionState::Connecting;

        // Request driver to open SSE stream
        outboxes.stream_actions.push(StreamAction::Open {
            upstream_id: request.upstream_id,
            subscription_id: sub_id,
            query: request.query,
            options: request.options,
        });
    }

    fn process_connection_event(&mut self, event: ConnectionEvent, outboxes: &mut Outboxes) {
        // Pre-allocate timer IDs to avoid borrow conflicts
        let timer_id = self.next_timer_id();

        let upstream = match self.upstreams.get_mut(&event.upstream_id) {
            Some(u) => u,
            None => return,
        };

        match event.event {
            ConnectionEventKind::StreamOpened { subscription_id: _ } => {
                upstream.connection = ConnectionState::Connected;
                outboxes
                    .notifications
                    .push(Notification::ConnectionStateChanged {
                        upstream_id: event.upstream_id,
                        state: ConnectionState::Connected,
                    });
            }
            ConnectionEventKind::StreamClosed {
                subscription_id,
                error,
            } => {
                // Remove subscription
                upstream.subscriptions.remove(&subscription_id);

                // If no more subscriptions, move to disconnected/reconnecting
                if upstream.subscriptions.is_empty() {
                    if error.is_some() {
                        // Start reconnection
                        let attempt = match &upstream.connection {
                            ConnectionState::Reconnecting { attempt, .. } => attempt + 1,
                            _ => 1,
                        };

                        if attempt <= self.config.reconnect_max_attempts {
                            let delay_ms = calculate_reconnect_delay(
                                attempt,
                                self.config.reconnect_base_delay_ms,
                                self.config.reconnect_max_delay_ms,
                            );
                            upstream.connection = ConnectionState::Reconnecting {
                                attempt,
                                next_delay_ms: delay_ms,
                            };

                            // Request timer for reconnection
                            outboxes.timers.push(TimerRequest {
                                id: timer_id,
                                delay_ms,
                                purpose: TimerPurpose::Reconnect,
                            });
                        } else {
                            upstream.connection = ConnectionState::Disconnected;
                        }
                    } else {
                        upstream.connection = ConnectionState::Disconnected;
                    }

                    outboxes
                        .notifications
                        .push(Notification::ConnectionStateChanged {
                            upstream_id: event.upstream_id,
                            state: upstream.connection.clone(),
                        });
                }
            }
            ConnectionEventKind::ConnectFailed { error: _ } => {
                // Handle like StreamClosed with error
                let attempt = match &upstream.connection {
                    ConnectionState::Reconnecting { attempt, .. } => attempt + 1,
                    _ => 1,
                };

                if attempt <= self.config.reconnect_max_attempts {
                    let delay_ms = calculate_reconnect_delay(
                        attempt,
                        self.config.reconnect_base_delay_ms,
                        self.config.reconnect_max_delay_ms,
                    );
                    upstream.connection = ConnectionState::Reconnecting {
                        attempt,
                        next_delay_ms: delay_ms,
                    };

                    outboxes.timers.push(TimerRequest {
                        id: timer_id,
                        delay_ms,
                        purpose: TimerPurpose::Reconnect,
                    });
                } else {
                    upstream.connection = ConnectionState::Disconnected;
                }

                outboxes
                    .notifications
                    .push(Notification::ConnectionStateChanged {
                        upstream_id: event.upstream_id,
                        state: upstream.connection.clone(),
                    });
            }
        }
    }

    /// Process a local write from inbox.
    /// The actual pending_writes tracking is done via drain_changed_objects.
    fn process_local_write(&mut self, write: LocalWriteEvent) {
        // Apply write to LocalNode (this will record the change)
        let _ = self.local_node.write_with_meta(
            write.object_id,
            &write.branch,
            &write.content,
            &write.author,
            write.timestamp,
            None,
        );
    }

    /// Drain changed objects from LocalNode and add to pending_writes.
    fn drain_changed_objects(&mut self, outboxes: &mut Outboxes) {
        let changes = self.local_node.drain_changed_objects();
        if changes.is_empty() {
            return;
        }

        let mut needs_timer = false;

        for change in changes {
            self.pending_writes
                .entry(change.object_id)
                .and_modify(|p| p.last_write_ms = change.timestamp)
                .or_insert_with(|| {
                    needs_timer = true;
                    PendingWrite {
                        object_id: change.object_id,
                        branch: change.branch,
                        first_write_ms: change.timestamp,
                        last_write_ms: change.timestamp,
                    }
                });
        }

        // Request debounce timer if we added new pending writes
        if needs_timer {
            outboxes.timers.push(TimerRequest {
                id: self.next_timer_id(),
                delay_ms: self.config.write_debounce_ms,
                purpose: TimerPurpose::Debounce,
            });
        }
    }

    fn process_sse_event(&mut self, sse: SseInboxEvent, outboxes: &mut Outboxes) {
        let upstream = match self.upstreams.get_mut(&sse.upstream_id) {
            Some(u) => u,
            None => return,
        };

        match sse.event {
            SseEvent::Commits {
                object_id,
                commits,
                frontier,
                object_meta,
            } => {
                // Track object in subscription
                if let Some(sub) = upstream.subscriptions.get_mut(&sse.subscription_id) {
                    sub.objects.insert(object_id);
                }

                // Update server known state
                upstream.server_known_state.insert(object_id, frontier);

                // Apply commits to LocalNode
                if !commits.is_empty() {
                    self.local_node
                        .apply_commits(object_id, "main", commits.clone());

                    // Notify observers
                    outboxes.notifications.push(Notification::ObjectsReceived {
                        object_id,
                        commits,
                        object_meta,
                    });
                }
            }
            SseEvent::Excluded { object_id } => {
                // Remove object from subscription tracking
                if let Some(sub) = upstream.subscriptions.get_mut(&sse.subscription_id) {
                    sub.objects.remove(&object_id);
                }
            }
            SseEvent::Truncate {
                object_id,
                truncate_at,
            } => {
                // Apply truncation to LocalNode
                let _ = self.local_node.truncate_at(object_id, "main", truncate_at);
            }
            SseEvent::Request {
                object_id,
                commit_ids: _,
            } => {
                // Server is requesting commits we have. Queue a reconcile.
                let local_frontier = self
                    .local_node
                    .frontier(object_id, "main")
                    .ok()
                    .flatten()
                    .unwrap_or_default();

                outboxes.requests.push(OutboundRequest::Reconcile {
                    upstream_id: sse.upstream_id,
                    request: ReconcileRequest {
                        object_id,
                        local_frontier,
                    },
                });
            }
            SseEvent::Error {
                code: _,
                message: _,
            } => {
                // Log error but don't disconnect - server might recover
            }
        }
    }

    fn process_push_response(&mut self, response: PushResponseEvent, _outboxes: &mut Outboxes) {
        let upstream = match self.upstreams.get_mut(&response.upstream_id) {
            Some(u) => u,
            None => return,
        };

        match response.result {
            Ok(push_response) => {
                if push_response.accepted {
                    // Update server known state
                    upstream
                        .server_known_state
                        .insert(push_response.object_id, push_response.frontier);
                }
                // Remove from pending writes
                self.pending_writes.remove(&push_response.object_id);
            }
            Err(_) => {
                // Push failed - will retry on next tick
            }
        }
    }

    fn process_tick(&mut self, tick: TickEvent, outboxes: &mut Outboxes) {
        let now_ms = tick.now_ms;

        // Check for writes ready to push
        let ready: Vec<ObjectId> = self
            .pending_writes
            .iter()
            .filter(|(_, p)| {
                let since_last = now_ms.saturating_sub(p.last_write_ms);
                let since_first = now_ms.saturating_sub(p.first_write_ms);
                since_last >= self.config.write_debounce_ms
                    || since_first >= self.config.max_write_age_ms
            })
            .map(|(id, _)| *id)
            .collect();

        // Generate push requests for ready objects
        for object_id in ready {
            if let Some(pending) = self.pending_writes.get(&object_id) {
                // Find an upstream to push to
                for (&upstream_id, upstream) in &self.upstreams {
                    if upstream.connection == ConnectionState::Connected {
                        // Get local frontier
                        let local_frontier = self
                            .local_node
                            .frontier(object_id, &pending.branch)
                            .ok()
                            .flatten()
                            .unwrap_or_default();

                        // Get server known state
                        let server_frontier = upstream
                            .server_known_state
                            .get(&object_id)
                            .cloned()
                            .unwrap_or_default();

                        // Collect commits to send (delta)
                        let commits = self.collect_commits_to_send(
                            object_id,
                            &pending.branch,
                            &local_frontier,
                            &server_frontier,
                        );

                        if !commits.is_empty() {
                            // Get object metadata for first push
                            let object_meta = if server_frontier.is_empty() {
                                self.local_node.get_object(object_id).and_then(|obj| {
                                    let obj = obj.read().ok()?;
                                    obj.meta.clone()
                                })
                            } else {
                                None
                            };

                            outboxes.requests.push(OutboundRequest::Push {
                                upstream_id,
                                request: PushRequest {
                                    object_id,
                                    commits,
                                    object_meta,
                                },
                            });
                        }

                        break; // Only push to first connected upstream
                    }
                }
            }
        }
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Collect commits to send based on local and server frontiers.
    fn collect_commits_to_send(
        &self,
        object_id: ObjectId,
        branch: &str,
        local_frontier: &[CommitId],
        server_frontier: &[CommitId],
    ) -> Vec<Commit> {
        use super::negotiation::{FrontierComparison, commits_to_send, compare_frontiers};

        let obj = match self.local_node.get_object(object_id) {
            Some(o) => o,
            None => return vec![],
        };

        let obj_guard = match obj.read() {
            Ok(g) => g,
            Err(_) => return vec![],
        };

        let branch_ref = match obj_guard.branch_ref(branch) {
            Some(b) => b,
            None => return vec![],
        };

        let branch_guard = match branch_ref.read() {
            Ok(g) => g,
            Err(_) => return vec![],
        };

        // Compare frontiers
        let comparison = compare_frontiers(local_frontier, server_frontier);

        match comparison {
            FrontierComparison::LocalAhead | FrontierComparison::Diverged => {
                commits_to_send(&branch_guard, local_frontier, server_frontier)
            }
            _ => vec![],
        }
    }
}

// ============================================================================
// Utilities
// ============================================================================

/// Calculate reconnect delay with exponential backoff.
fn calculate_reconnect_delay(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    let delay = base_ms.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
    delay.min(max_ms)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let engine = SyncEngine::new();
        assert!(engine.upstreams.is_empty());
        assert!(engine.pending_writes.is_empty());
    }

    #[test]
    fn test_add_upstream() {
        let mut engine = SyncEngine::new();
        let id = engine.add_upstream();
        assert_eq!(id, UpstreamId(1));
        assert!(engine.upstream(id).is_some());
    }

    #[test]
    fn test_subscribe_request() {
        let mut engine = SyncEngine::new();
        let upstream_id = engine.add_upstream();

        let inboxes = Inboxes {
            subscribe_requests: vec![SubscribeRequestEvent {
                upstream_id,
                query: "SELECT * FROM users".to_string(),
                options: SubscriptionOptions::default(),
            }],
            ..Default::default()
        };

        let outboxes = engine.pass(inboxes);

        // Should have a stream open action
        assert_eq!(outboxes.stream_actions.len(), 1);
        assert!(matches!(
            &outboxes.stream_actions[0],
            StreamAction::Open { query, .. } if query == "SELECT * FROM users"
        ));

        // Upstream should be connecting
        let upstream = engine.upstream(upstream_id).unwrap();
        assert_eq!(upstream.connection, ConnectionState::Connecting);
    }

    #[test]
    fn test_local_write_queues_debounce() {
        let mut engine = SyncEngine::new();
        let object_id = engine.local_node.create_object("test");

        let inboxes = Inboxes {
            local_writes: vec![LocalWriteEvent {
                object_id,
                branch: "main".to_string(),
                content: b"hello".to_vec(),
                author: "test".to_string(),
                timestamp: 1000,
            }],
            ..Default::default()
        };

        let outboxes = engine.pass(inboxes);

        // Should have a debounce timer
        assert!(
            outboxes
                .timers
                .iter()
                .any(|t| t.purpose == TimerPurpose::Debounce)
        );

        // Should have pending write
        assert!(engine.pending_writes.contains_key(&object_id));
    }

    #[test]
    fn test_sse_commits_apply_and_notify() {
        let mut engine = SyncEngine::new();
        let upstream_id = engine.add_upstream();

        // Set upstream to connected
        engine.upstream_mut(upstream_id).unwrap().connection = ConnectionState::Connected;
        engine
            .upstream_mut(upstream_id)
            .unwrap()
            .subscriptions
            .insert(
                1,
                SubscriptionState {
                    query: "SELECT * FROM test".to_string(),
                    options: SubscriptionOptions::default(),
                    objects: HashSet::new(),
                },
            );

        let object_id = ObjectId::new(12345);
        let commit = Commit {
            parents: vec![],
            content: b"test data".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = commit.compute_id();

        let inboxes = Inboxes {
            sse_events: vec![SseInboxEvent {
                upstream_id,
                subscription_id: 1,
                event: SseEvent::Commits {
                    object_id,
                    commits: vec![commit],
                    frontier: vec![commit_id],
                    object_meta: None,
                },
            }],
            ..Default::default()
        };

        let outboxes = engine.pass(inboxes);

        // Should notify about received objects
        assert!(outboxes.notifications.iter().any(|n| matches!(
            n,
            Notification::ObjectsReceived { object_id: oid, .. } if *oid == object_id
        )));

        // Object should be tracked in subscription
        let upstream = engine.upstream(upstream_id).unwrap();
        assert!(upstream.subscriptions[&1].objects.contains(&object_id));

        // Server known state should be updated
        assert!(upstream.server_known_state.contains_key(&object_id));
    }

    #[test]
    fn test_reconnect_delay_calculation() {
        assert_eq!(calculate_reconnect_delay(1, 1000, 30000), 1000);
        assert_eq!(calculate_reconnect_delay(2, 1000, 30000), 2000);
        assert_eq!(calculate_reconnect_delay(3, 1000, 30000), 4000);
        assert_eq!(calculate_reconnect_delay(4, 1000, 30000), 8000);
        assert_eq!(calculate_reconnect_delay(5, 1000, 30000), 16000);
        assert_eq!(calculate_reconnect_delay(6, 1000, 30000), 30000); // Capped
    }
}
