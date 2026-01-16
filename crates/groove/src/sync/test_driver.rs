//! Test driver for deterministic sync testing.
//!
//! The TestDriver provides a controlled environment for testing sync behavior
//! between multiple nodes. It:
//!
//! - Manages in-memory storage (replaces Environment trait for tests)
//! - Simulates transport between clients and server
//! - Provides deterministic time control via tick()
//! - Enables assertions on node state
//!
//! # Example
//!
//! ```ignore
//! let mut driver = TestDriver::new();
//!
//! // Create clients
//! driver.add_client("alice");
//! driver.add_client("bob");
//!
//! // Alice creates data
//! driver.execute("alice", "CREATE TABLE users (name STRING)");
//! driver.execute("alice", "INSERT INTO users (name) VALUES ('Alice')");
//!
//! // Connect and sync
//! driver.connect("alice", "SELECT * FROM users");
//! driver.connect("bob", "SELECT * FROM users");
//! driver.settle();
//!
//! // Bob should see Alice's data
//! let rows = driver.query("bob", "SELECT * FROM users");
//! assert_eq!(rows.len(), 1);
//! ```

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::commit::{Commit, CommitId};
use crate::object::ObjectId;
use crate::sql::row_buffer::{OwnedRow, RowValue};
use crate::sql::{Database, DatabaseState, ExecuteResult};

use super::engine::{
    ConnectionEvent, ConnectionEventKind, ConnectionState, Inboxes, OutboundRequest, Outboxes,
    PushResponseEvent, SseInboxEvent, StorageRequest, StreamAction, SubscribeRequestEvent,
    SyncEngine, TickEvent, UpstreamId,
};
use super::protocol::{PushRequest, PushResponse, SseEvent, SubscriptionOptions};

// ============================================================================
// Test Storage
// ============================================================================

/// In-memory storage for test driver.
///
/// This replaces the Environment trait for tests - storage is synchronous
/// and directly managed by the test driver.
#[derive(Debug, Default)]
pub struct TestStorage {
    /// Commits by ID.
    commits: HashMap<CommitId, Commit>,
    /// Frontiers by (object_id, branch).
    frontiers: HashMap<(ObjectId, String), Vec<CommitId>>,
}

impl TestStorage {
    /// Create new empty storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Execute a storage request.
    pub fn execute(&mut self, request: StorageRequest) {
        match request {
            StorageRequest::PutCommit { commit } => {
                let id = commit.compute_id();
                self.commits.insert(id, commit);
            }
            StorageRequest::SetFrontier {
                object_id,
                branch,
                frontier,
            } => {
                self.frontiers.insert((object_id, branch), frontier);
            }
        }
    }

    /// Get a commit by ID.
    pub fn get_commit(&self, id: &CommitId) -> Option<&Commit> {
        self.commits.get(id)
    }

    /// Get frontier for an object's branch.
    pub fn get_frontier(&self, object_id: ObjectId, branch: &str) -> Vec<CommitId> {
        self.frontiers
            .get(&(object_id, branch.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get all commits count.
    pub fn commit_count(&self) -> usize {
        self.commits.len()
    }
}

// ============================================================================
// Test Node
// ============================================================================

/// A node in the test driver (client or server).
struct TestNode {
    /// The sync engine.
    engine: Rc<RefCell<SyncEngine>>,
    /// Database state (shares LocalNode with engine).
    db: Rc<DatabaseState>,
    /// Upstream ID (for client nodes).
    upstream_id: UpstreamId,
    /// Active subscription (for SSE).
    subscription_id: Option<u32>,
    /// Query this node is subscribed to.
    query: Option<String>,
}

impl TestNode {
    fn new() -> Self {
        let db = Database::in_memory();
        let db_state = db.into_state();

        // Create engine with the database's LocalNode (shared)
        let node = db_state.node_arc();
        let mut engine = SyncEngine::with_local_node(node);

        // Add upstream server
        let upstream_id = engine.add_upstream();

        Self {
            engine: Rc::new(RefCell::new(engine)),
            db: db_state,
            upstream_id,
            subscription_id: None,
            query: None,
        }
    }

    fn database(&self) -> Database {
        Database::from_state(Rc::clone(&self.db))
    }
}

// ============================================================================
// Pending Events
// ============================================================================

/// An event pending delivery between nodes.
#[derive(Debug)]
enum PendingEvent {
    /// Push request from client to server.
    PushToServer {
        client: String,
        request: PushRequest,
    },
    /// Push response from server to client.
    PushResponse {
        client: String,
        response: Result<PushResponse, String>,
        object_id: ObjectId,
    },
    /// SSE event from server to client.
    SseEvent { client: String, event: SseEvent },
    /// Stream opened notification.
    StreamOpened {
        client: String,
        subscription_id: u32,
    },
}

// ============================================================================
// Test Driver
// ============================================================================

/// Test driver for deterministic sync testing.
///
/// Manages multiple client nodes and a simulated server, providing
/// deterministic control over timing and message delivery.
pub struct TestDriver {
    /// Storage for all nodes (shared).
    storage: TestStorage,
    /// Client nodes by name.
    clients: HashMap<String, TestNode>,
    /// Server state: what objects/commits it knows about.
    server_state: HashMap<ObjectId, Vec<CommitId>>,
    /// Server's view of object commits (for sending deltas).
    server_commits: HashMap<ObjectId, HashMap<CommitId, Commit>>,
    /// Pending events to deliver.
    pending: VecDeque<PendingEvent>,
    /// Current virtual time (ms).
    now_ms: u64,
}

impl Default for TestDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl TestDriver {
    /// Create a new test driver.
    pub fn new() -> Self {
        // Start at actual current time so timestamps from Database.execute() work
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            storage: TestStorage::new(),
            clients: HashMap::new(),
            server_state: HashMap::new(),
            server_commits: HashMap::new(),
            pending: VecDeque::new(),
            now_ms,
        }
    }

    /// Add a client node.
    pub fn add_client(&mut self, name: &str) {
        self.clients.insert(name.to_string(), TestNode::new());
    }

    /// Connect a client to the server with a query.
    pub fn connect(&mut self, client: &str, query: &str) {
        // Get upstream_id and run pass
        let outboxes = {
            let node = self.clients.get(client).expect("client not found");
            let inboxes = Inboxes {
                subscribe_requests: vec![SubscribeRequestEvent {
                    upstream_id: node.upstream_id,
                    query: query.to_string(),
                    options: SubscriptionOptions::default(),
                }],
                ..Default::default()
            };
            node.engine.borrow_mut().pass(inboxes)
        };

        // Handle outboxes
        self.handle_outboxes(client, outboxes);

        // Store query for later
        let node = self.clients.get_mut(client).expect("client not found");
        node.query = Some(query.to_string());
    }

    /// Execute SQL on a client.
    pub fn execute(&mut self, client: &str, sql: &str) -> ExecuteResult {
        let result = {
            let node = self.clients.get(client).expect("client not found");
            let db = node.database();
            db.execute(sql).expect("execute failed")
        };

        // Run a pass to pick up changed objects for sync
        let outboxes = {
            let node = self.clients.get(client).expect("client not found");
            node.engine.borrow_mut().pass(Inboxes::default())
        };
        self.handle_outboxes(client, outboxes);

        result
    }

    /// Query on a client.
    pub fn query(&self, client: &str, sql: &str) -> Vec<(ObjectId, OwnedRow)> {
        let node = self.clients.get(client).expect("client not found");
        let db = node.database();
        db.query(sql).expect("query failed")
    }

    /// Get a row value by column name from query results.
    pub fn query_values(&self, client: &str, sql: &str, column: &str) -> Vec<String> {
        self.query(client, sql)
            .iter()
            .map(|(_, row)| match row.get_by_name(column) {
                Some(RowValue::String(s)) => s.to_string(),
                Some(RowValue::I32(n)) => n.to_string(),
                Some(RowValue::Ref(id)) => id.to_string(),
                Some(RowValue::Null) => "NULL".to_string(),
                _ => "?".to_string(),
            })
            .collect()
    }

    /// Advance time and process one tick.
    pub fn tick(&mut self) {
        self.now_ms += 100; // 100ms per tick
        self.process_tick();
    }

    /// Advance time and run until no more pending events.
    pub fn settle(&mut self) {
        // First deliver any pending events
        self.deliver_pending();

        // Keep ticking until nothing happens
        for _ in 0..100 {
            self.tick();
            self.deliver_pending();

            if self.pending.is_empty() && !self.has_pending_writes() {
                break;
            }
        }
    }

    /// Check if any client has pending writes.
    fn has_pending_writes(&self) -> bool {
        for node in self.clients.values() {
            let engine = node.engine.borrow();
            if engine.has_pending_writes() {
                return true;
            }
        }
        false
    }

    /// Process a tick on all clients.
    fn process_tick(&mut self) {
        let client_names: Vec<String> = self.clients.keys().cloned().collect();

        for client in client_names {
            let node = self.clients.get(&client).unwrap();
            let inboxes = Inboxes {
                tick: Some(TickEvent {
                    now_ms: self.now_ms,
                }),
                ..Default::default()
            };

            let outboxes = node.engine.borrow_mut().pass(inboxes);
            self.handle_outboxes(&client, outboxes);
        }
    }

    /// Deliver all pending events.
    fn deliver_pending(&mut self) {
        while let Some(event) = self.pending.pop_front() {
            match event {
                PendingEvent::PushToServer { client, request } => {
                    self.handle_push_at_server(&client, request);
                }
                PendingEvent::PushResponse {
                    client,
                    response,
                    object_id,
                } => {
                    self.deliver_push_response(&client, object_id, response);
                }
                PendingEvent::SseEvent { client, event } => {
                    self.deliver_sse_event(&client, event);
                }
                PendingEvent::StreamOpened {
                    client,
                    subscription_id,
                } => {
                    self.deliver_stream_opened(&client, subscription_id);
                }
            }
        }
    }

    /// Handle outboxes from a client pass.
    fn handle_outboxes(&mut self, client: &str, outboxes: Outboxes) {
        // Execute storage requests immediately
        for req in outboxes.storage {
            self.storage.execute(req);
        }

        // Handle stream actions
        for action in outboxes.stream_actions {
            match action {
                StreamAction::Open {
                    subscription_id,
                    query: _,
                    ..
                } => {
                    // Store subscription ID
                    if let Some(node) = self.clients.get_mut(client) {
                        node.subscription_id = Some(subscription_id);
                    }
                    // Queue stream opened event
                    self.pending.push_back(PendingEvent::StreamOpened {
                        client: client.to_string(),
                        subscription_id,
                    });
                }
                StreamAction::Close { .. } => {
                    if let Some(node) = self.clients.get_mut(client) {
                        node.subscription_id = None;
                    }
                }
            }
        }

        // Handle outbound requests
        for request in outboxes.requests {
            match request {
                OutboundRequest::Push { request, .. } => {
                    self.pending.push_back(PendingEvent::PushToServer {
                        client: client.to_string(),
                        request,
                    });
                }
                OutboundRequest::Reconcile { .. } | OutboundRequest::Unsubscribe { .. } => {
                    // TODO: implement if needed
                }
            }
        }
    }

    /// Handle a push request at the server.
    fn handle_push_at_server(&mut self, from_client: &str, request: PushRequest) {
        let object_id = request.object_id;

        // Store commits at server
        let commits_map = self.server_commits.entry(object_id).or_default();
        for commit in &request.commits {
            let id = commit.compute_id();
            commits_map.insert(id, commit.clone());
        }

        // Update server frontier
        let new_frontier: Vec<CommitId> = request.commits.iter().map(|c| c.compute_id()).collect();
        // For simplicity, just use the new commits as frontier
        // (A real server would compute proper frontier merge)
        let frontier = if let Some(existing) = self.server_state.get(&object_id) {
            // Merge: keep commits from new_frontier that are newer
            let mut merged = existing.clone();
            for id in &new_frontier {
                if !merged.contains(id) {
                    merged.push(*id);
                }
            }
            // Keep only the latest (simplified - just use new if we have commits)
            if !new_frontier.is_empty() {
                new_frontier.clone()
            } else {
                merged
            }
        } else {
            new_frontier
        };

        self.server_state.insert(object_id, frontier.clone());

        // Send push response back to client
        self.pending.push_back(PendingEvent::PushResponse {
            client: from_client.to_string(),
            response: Ok(PushResponse {
                object_id,
                accepted: true,
                frontier: frontier.clone(),
            }),
            object_id,
        });

        // Broadcast to other connected clients
        for (client_name, node) in &self.clients {
            if client_name == from_client {
                continue; // Don't send back to sender
            }
            if node.subscription_id.is_some() {
                // Client is subscribed - send SSE event
                self.pending.push_back(PendingEvent::SseEvent {
                    client: client_name.clone(),
                    event: SseEvent::Commits {
                        object_id,
                        commits: request.commits.clone(),
                        frontier: frontier.clone(),
                        object_meta: request.object_meta.clone(),
                    },
                });
            }
        }
    }

    /// Deliver a push response to a client.
    fn deliver_push_response(
        &mut self,
        client: &str,
        object_id: ObjectId,
        response: Result<PushResponse, String>,
    ) {
        let node = self.clients.get(client).expect("client not found");

        let inboxes = Inboxes {
            push_responses: vec![PushResponseEvent {
                upstream_id: node.upstream_id,
                object_id,
                result: response,
            }],
            ..Default::default()
        };

        let outboxes = node.engine.borrow_mut().pass(inboxes);
        self.handle_outboxes(client, outboxes);
    }

    /// Deliver an SSE event to a client.
    fn deliver_sse_event(&mut self, client: &str, event: SseEvent) {
        let node = self.clients.get(client).expect("client not found");
        let subscription_id = node.subscription_id.unwrap_or(1);

        let inboxes = Inboxes {
            sse_events: vec![SseInboxEvent {
                upstream_id: node.upstream_id,
                subscription_id,
                event,
            }],
            ..Default::default()
        };

        let outboxes = node.engine.borrow_mut().pass(inboxes);
        self.handle_outboxes(client, outboxes);
    }

    /// Deliver stream opened event to a client.
    fn deliver_stream_opened(&mut self, client: &str, subscription_id: u32) {
        let node = self.clients.get(client).expect("client not found");

        let inboxes = Inboxes {
            connection_events: vec![ConnectionEvent {
                upstream_id: node.upstream_id,
                event: ConnectionEventKind::StreamOpened { subscription_id },
            }],
            ..Default::default()
        };

        let outboxes = node.engine.borrow_mut().pass(inboxes);
        self.handle_outboxes(client, outboxes);
    }

    /// Get the connection state of a client.
    pub fn connection_state(&self, client: &str) -> ConnectionState {
        let node = self.clients.get(client).expect("client not found");
        let engine = node.engine.borrow();
        engine
            .upstream(node.upstream_id)
            .map(|u| u.connection.clone())
            .unwrap_or(ConnectionState::Disconnected)
    }

    /// Get storage for assertions.
    pub fn storage(&self) -> &TestStorage {
        &self.storage
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_basic() {
        let mut driver = TestDriver::new();

        driver.add_client("alice");

        driver.execute("alice", "CREATE TABLE users (name STRING NOT NULL)");
        driver.execute("alice", "INSERT INTO users (name) VALUES ('Alice')");

        let rows = driver.query("alice", "SELECT * FROM users");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_driver_connection() {
        let mut driver = TestDriver::new();

        driver.add_client("alice");
        driver.execute("alice", "CREATE TABLE users (name STRING NOT NULL)");

        // Connect
        driver.connect("alice", "SELECT * FROM users");
        driver.settle();

        // Should be connected
        assert_eq!(driver.connection_state("alice"), ConnectionState::Connected);
    }

    #[test]
    fn test_driver_push_to_server() {
        let mut driver = TestDriver::new();

        // Create client
        driver.add_client("alice");

        // Create table and data
        driver.execute("alice", "CREATE TABLE items (name STRING NOT NULL)");
        driver.execute("alice", "INSERT INTO items (name) VALUES ('Item1')");

        // Connect and sync
        driver.connect("alice", "SELECT * FROM items");
        driver.settle();

        // Verify storage has commits (data was persisted via outboxes)
        assert!(
            driver.storage().commit_count() > 0,
            "should have stored commits"
        );

        // Verify server received the data
        assert!(
            !driver.server_state.is_empty(),
            "server should know about objects"
        );
    }

    #[test]
    fn test_driver_sync_commits_to_other_client() {
        let mut driver = TestDriver::new();

        // Create two clients
        driver.add_client("alice");
        driver.add_client("bob");

        // Alice creates data
        driver.execute("alice", "CREATE TABLE items (name STRING NOT NULL)");
        driver.execute("alice", "INSERT INTO items (name) VALUES ('Item1')");

        // Both connect
        driver.connect("alice", "SELECT * FROM items");
        driver.connect("bob", "SELECT * FROM items");
        driver.settle();

        // Get the row object ID from alice
        let alice_rows = driver.query("alice", "SELECT id FROM items");
        assert_eq!(alice_rows.len(), 1, "alice should have 1 row");
        let row_id = alice_rows[0].0;

        // Verify Bob's engine received the commits
        let bob_node = driver.clients.get("bob").unwrap();
        let bob_engine = bob_node.engine.borrow();
        let has_object = bob_engine.local_node.get_object(row_id).is_some();

        // Bob should have received the object via SSE sync
        assert!(
            has_object,
            "bob should have received the row object via sync"
        );
    }

    #[test]
    fn test_driver_bidirectional_sync() {
        let mut driver = TestDriver::new();

        // Create two clients
        driver.add_client("alice");
        driver.add_client("bob");

        // Both connect first
        driver.connect("alice", "SELECT 1");
        driver.connect("bob", "SELECT 1");
        driver.settle();

        // Alice creates an object
        let alice_node = driver.clients.get("alice").unwrap();
        let obj1 = alice_node.engine.borrow().local_node.create_object("test");
        alice_node
            .engine
            .borrow()
            .local_node
            .write(obj1, "main", b"hello from alice", "alice", 1000)
            .unwrap();

        // Trigger sync
        driver.settle();

        // Check server knows about the object
        assert!(
            driver.server_state.contains_key(&obj1),
            "server should know about alice's object"
        );

        // Check bob received it
        let bob_node = driver.clients.get("bob").unwrap();
        assert!(
            bob_node
                .engine
                .borrow()
                .local_node
                .get_object(obj1)
                .is_some(),
            "bob should have alice's object"
        );
    }

    #[test]
    fn test_storage_operations() {
        let mut storage = TestStorage::new();

        let commit = Commit {
            parents: vec![],
            content: b"test".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = commit.compute_id();
        let object_id = ObjectId::new(12345);

        storage.execute(StorageRequest::PutCommit {
            commit: commit.clone(),
        });
        storage.execute(StorageRequest::SetFrontier {
            object_id,
            branch: "main".to_string(),
            frontier: vec![commit_id],
        });

        assert!(storage.get_commit(&commit_id).is_some());
        assert_eq!(storage.get_frontier(object_id, "main"), vec![commit_id]);
    }
}
