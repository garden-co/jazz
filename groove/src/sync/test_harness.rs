//! In-memory test harness for sync integration testing.
//!
//! This module provides a test ensemble where:
//! - Each client has its OWN LocalNode with its OWN storage
//! - The server has its own storage
//! - Writes go through LocalNode::write() → SyncClient::push() → server
//! - Server broadcasts to other clients via SSE
//! - Clients apply received commits to their LocalNode via apply_commits()
//!
//! This tests the complete sync flow including LocalNode integration.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::{mpsc, RwLock};

use crate::commit::CommitId;
use crate::node::LocalNode;
use crate::object::ObjectId;
use crate::storage::MemoryEnvironment;

use super::env::{ClientEnv, ClientError};
use super::protocol::{
    PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest, SubscriptionOptions,
};
use super::server::{
    AcceptAllTokens, ActiveQuery, ClientIdentity, SessionId, SseSender, SyncServer, TokenValidator,
};

// ============================================================================
// Test Transport (Server-side)
// ============================================================================

/// A test transport that routes requests directly to a SyncServer.
///
/// The server has its own storage, separate from client storage.
pub struct TestTransport {
    /// The sync server
    server: Arc<RwLock<SyncServer<MemoryEnvironment>>>,
    /// The server's storage environment
    server_env: Arc<MemoryEnvironment>,
}

impl TestTransport {
    /// Create a new test transport with an in-memory server.
    pub fn new() -> Self {
        let server_env = Arc::new(MemoryEnvironment::new());
        let validator: Arc<dyn TokenValidator> = Arc::new(AcceptAllTokens);
        let server = SyncServer::new(Arc::clone(&server_env), validator);
        Self {
            server: Arc::new(RwLock::new(server)),
            server_env,
        }
    }

    /// Get the server's storage environment.
    pub fn server_env(&self) -> &Arc<MemoryEnvironment> {
        &self.server_env
    }

    /// Process a subscribe request.
    pub async fn subscribe(
        &self,
        token: &str,
        request: SubscribeRequest,
    ) -> Result<(SessionId, mpsc::Receiver<SseEvent>), ClientError> {
        let identity = ClientIdentity {
            id: token.to_string(),
            name: None,
        };

        let (tx, rx) = mpsc::channel::<SseEvent>(32);

        let (session_id, query_id) = {
            let mut server = self.server.write().await;
            let session_id = server.create_session(identity, tx.clone());

            let session = server.get_session_mut(&session_id).unwrap();
            let query_id = session.next_query_id();
            session.queries.insert(
                query_id,
                ActiveQuery::new(request.query.clone(), request.options.clone()),
            );

            (session_id, query_id)
        };

        // Send initial data for matching objects (simplified: * matches all)
        if request.query == "*" || request.query.to_lowercase().contains("select * from") {
            self.send_initial_data(session_id, query_id, tx).await;
        }

        Ok((session_id, rx))
    }

    /// Send initial data for all objects to a new subscription.
    async fn send_initial_data(
        &self,
        session_id: SessionId,
        query_id: super::server::QueryId,
        tx: SseSender,
    ) {
        use crate::storage::CommitStore;

        let object_ids: Vec<u128> = self.server_env.list_objects().collect().await;

        for oid in object_ids {
            let object_id = ObjectId(oid);
            let frontier = self.server_env.get_frontier(oid, "main").await;
            if frontier.is_empty() {
                continue;
            }

            let commit_ids: Vec<_> = self.server_env.list_commits(oid, "main").collect().await;
            let mut commits = Vec::new();
            for commit_id in commit_ids {
                if let Some(commit) = self.server_env.get_commit(&commit_id).await {
                    commits.push(commit);
                }
            }

            if !commits.is_empty() {
                let event = SseEvent::Commits {
                    object_id,
                    commits,
                    frontier: frontier.clone(),
                };
                let _ = tx.send(event).await;
            }

            {
                let mut server = self.server.write().await;
                if let Some(session) = server.get_session_mut(&session_id) {
                    session.add_object_to_query(object_id, query_id);
                    session.client_known_state.insert(object_id, frontier);
                }
                server.register_object_session(object_id, session_id);
            }
        }
    }

    /// Process a push request.
    pub async fn push(
        &self,
        token: &str,
        request: PushRequest,
    ) -> Result<PushResponse, ClientError> {
        if request.commits.is_empty() {
            return Ok(PushResponse {
                object_id: request.object_id,
                accepted: true,
                frontier: vec![],
            });
        }

        let sender_session = {
            let server = self.server.read().await;
            server.sessions_for_identity(token).into_iter().next()
        };

        // Store commits in SERVER's storage
        let frontier = {
            let server = self.server.read().await;
            server
                .store_commits(request.object_id, &request.commits, "main")
                .await
        };

        // Register with sessions that have wildcard queries
        {
            let mut server = self.server.write().await;
            let session_ids: Vec<_> = server.sessions.keys().copied().collect();
            for session_id in session_ids {
                let has_wildcard = server
                    .sessions
                    .get(&session_id)
                    .map(|s| s.queries.values().any(|q| q.query == "*"))
                    .unwrap_or(false);

                if has_wildcard {
                    if !server
                        .sessions_for_object(&request.object_id)
                        .contains(&session_id)
                    {
                        server.register_object_session(request.object_id, session_id);
                        if let Some(session) = server.get_session_mut(&session_id) {
                            if let Some((&query_id, _)) =
                                session.queries.iter().find(|(_, q)| q.query == "*")
                            {
                                session.add_object_to_query(request.object_id, query_id);
                            }
                        }
                    }
                }
            }
        }

        // Broadcast to other sessions
        {
            let server = self.server.read().await;
            server
                .broadcast_commits(
                    request.object_id,
                    request.commits.clone(),
                    frontier.clone(),
                    sender_session,
                )
                .await;
        }

        // Update sender's known state
        if let Some(session_id) = sender_session {
            let mut server = self.server.write().await;
            server.update_client_known_state(&session_id, request.object_id, frontier.clone());
        }

        Ok(PushResponse {
            object_id: request.object_id,
            accepted: true,
            frontier,
        })
    }

    /// Process a reconcile request.
    pub async fn reconcile(
        &self,
        token: &str,
        request: ReconcileRequest,
    ) -> Result<SseEvent, ClientError> {
        use crate::storage::CommitStore;

        let server_frontier = self
            .server_env
            .get_frontier(request.object_id.0, "main")
            .await;

        if server_frontier.is_empty() {
            return Ok(SseEvent::Commits {
                object_id: request.object_id,
                commits: vec![],
                frontier: vec![],
            });
        }

        let client_known: HashSet<_> = request.local_frontier.iter().copied().collect();
        let commit_ids: Vec<_> = self
            .server_env
            .list_commits(request.object_id.0, "main")
            .collect()
            .await;

        let mut commits_to_send = Vec::new();
        for commit_id in &commit_ids {
            if !client_known.contains(commit_id) {
                if let Some(commit) = self.server_env.get_commit(commit_id).await {
                    commits_to_send.push(commit);
                }
            }
        }

        if let Some(session_id) = {
            let server = self.server.read().await;
            server.sessions_for_identity(token).into_iter().next()
        } {
            let mut server = self.server.write().await;
            server.update_client_known_state(
                &session_id,
                request.object_id,
                server_frontier.clone(),
            );
        }

        Ok(SseEvent::Commits {
            object_id: request.object_id,
            commits: commits_to_send,
            frontier: server_frontier,
        })
    }

    /// Process an unsubscribe request.
    pub async fn unsubscribe(&self, token: &str, subscription_id: u32) -> Result<(), ClientError> {
        let mut server = self.server.write().await;
        let session_ids = server.sessions_for_identity(token);
        let query_id = super::server::QueryId(subscription_id);

        for session_id in session_ids {
            if let Some(session) = server.get_session_mut(&session_id) {
                if session.queries.remove(&query_id).is_some() {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Default for TestTransport {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Test Client Environment
// ============================================================================

/// A ClientEnv implementation that routes requests through TestTransport.
pub struct TestClientEnv {
    transport: Arc<TestTransport>,
    auth_token: String,
}

impl TestClientEnv {
    pub fn new(transport: Arc<TestTransport>, auth_token: impl Into<String>) -> Self {
        Self {
            transport,
            auth_token: auth_token.into(),
        }
    }
}

#[async_trait]
impl ClientEnv for TestClientEnv {
    async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
        let (_session_id, rx) = self
            .transport
            .subscribe(&self.auth_token, request)
            .await?;

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
            .map(Ok)
            .boxed();

        Ok(stream)
    }

    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError> {
        self.transport.push(&self.auth_token, request).await
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<SseEvent, ClientError> {
        self.transport.reconcile(&self.auth_token, request).await
    }

    async fn unsubscribe(&self, subscription_id: u32) -> Result<(), ClientError> {
        self.transport
            .unsubscribe(&self.auth_token, subscription_id)
            .await
    }
}

// ============================================================================
// Test Client
// ============================================================================

/// A test client with its OWN LocalNode (separate storage from server).
pub struct TestClient {
    /// The sync client
    pub sync_client: super::client::SyncClient<TestClientEnv>,
    /// Client identifier
    pub id: String,
}

impl TestClient {
    /// Create a new test client with its own LocalNode.
    fn new(transport: Arc<TestTransport>, id: impl Into<String>) -> Self {
        let id = id.into();
        let env = TestClientEnv::new(Arc::clone(&transport), &id);
        // Each client gets its OWN MemoryEnvironment - NOT shared with server
        let client_env = Arc::new(MemoryEnvironment::new());
        let node = Arc::new(LocalNode::new(client_env));
        let sync_client = super::client::SyncClient::new(env, node);
        Self { sync_client, id }
    }

    /// Get the client's LocalNode.
    pub fn node(&self) -> &Arc<LocalNode> {
        self.sync_client.node()
    }

    /// Subscribe to all objects (query = "*").
    pub async fn subscribe_all(
        &mut self,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
        self.sync_client
            .subscribe("*".to_string(), SubscriptionOptions::default())
            .await
    }

    /// Write to LocalNode and push to server.
    ///
    /// This is the primary way to create and sync data:
    /// 1. Creates/gets object in client's LocalNode
    /// 2. Writes commit to LocalNode
    /// 3. Pushes to server via sync
    ///
    /// Returns the commit ID and push response.
    pub async fn write_and_push(
        &mut self,
        object_id: ObjectId,
        content: &[u8],
    ) -> Result<(CommitId, PushResponse), ClientError> {
        // Ensure object exists in LocalNode
        self.node().ensure_object(object_id, "");

        // Write to LocalNode
        let commit_id = self
            .node()
            .write(object_id, "main", content, &self.id, 0)
            .map_err(|e| ClientError::new(500, &format!("Write failed: {:?}", e)))?;

        // Push to server
        let response = self.sync_client.push(object_id, "main").await?;

        Ok((commit_id, response))
    }

    /// Apply an SSE event to this client's LocalNode.
    pub fn apply_event(&mut self, event: &SseEvent) {
        self.sync_client.handle_sse_event(event, "main");
    }

    /// Check if client has a specific commit.
    pub fn has_commit(&self, object_id: ObjectId, commit_id: &CommitId) -> bool {
        self.node().has_commit(object_id, "main", commit_id)
    }

    /// Get content of a commit.
    pub fn get_commit_content(&self, object_id: ObjectId, commit_id: &CommitId) -> Option<Vec<u8>> {
        self.node()
            .get_commit(object_id, "main", commit_id)
            .map(|c| c.content.to_vec())
    }

    /// Read current content from LocalNode.
    pub fn read(&self, object_id: ObjectId) -> Option<Vec<u8>> {
        self.node().read(object_id, "main").ok().flatten()
    }

    /// Get the frontier for an object.
    pub fn frontier(&self, object_id: ObjectId) -> Vec<CommitId> {
        self.node()
            .frontier(object_id, "main")
            .ok()
            .flatten()
            .unwrap_or_default()
    }
}

// ============================================================================
// Test Harness
// ============================================================================

/// Test harness for multi-client sync integration testing.
pub struct TestHarness {
    transport: Arc<TestTransport>,
}

impl TestHarness {
    pub fn new() -> Self {
        Self {
            transport: Arc::new(TestTransport::new()),
        }
    }

    /// Get the server's storage environment.
    pub fn server_env(&self) -> &Arc<MemoryEnvironment> {
        self.transport.server_env()
    }

    /// Create a new test client with its own LocalNode.
    pub fn create_client(&self, id: impl Into<String>) -> TestClient {
        TestClient::new(Arc::clone(&self.transport), id)
    }
}

impl Default for TestHarness {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Helper to receive an event with timeout.
    async fn recv_event(
        stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>,
    ) -> SseEvent {
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("timeout waiting for event")
            .expect("stream ended")
            .unwrap()
    }

    /// Helper to check no event is pending.
    async fn assert_no_event(stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>) {
        let result = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(result.is_err(), "expected no event but received one");
    }

    // ========================================================================
    // Basic Sync Tests
    // ========================================================================

    #[tokio::test]
    async fn test_write_and_push() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let _stream = alice.subscribe_all().await.unwrap();

        let object_id = ObjectId(42);
        let (commit_id, response) = alice.write_and_push(object_id, b"hello world").await.unwrap();

        // Push should succeed
        assert!(response.accepted);
        assert_eq!(response.frontier, vec![commit_id]);

        // Alice's LocalNode should have the commit
        assert!(alice.has_commit(object_id, &commit_id));
        assert_eq!(
            alice.get_commit_content(object_id, &commit_id),
            Some(b"hello world".to_vec())
        );
    }

    #[tokio::test]
    async fn test_two_clients_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(123);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice writes and pushes
        let (commit_id, response) = alice.write_and_push(object_id, b"alice's data").await.unwrap();
        assert!(response.accepted);

        // Alice should have the commit
        assert!(alice.has_commit(object_id, &commit_id));

        // Bob receives the broadcast
        let event = recv_event(&mut bob_stream).await;
        match &event {
            SseEvent::Commits {
                object_id: oid,
                commits,
                frontier,
            } => {
                assert_eq!(*oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "alice");
                assert_eq!(*frontier, vec![commit_id]);
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }

        // Bob applies the event to his LocalNode
        bob.apply_event(&event);

        // Now Bob should have the commit too
        assert!(bob.has_commit(object_id, &commit_id));
        assert_eq!(
            bob.get_commit_content(object_id, &commit_id),
            Some(b"alice's data".to_vec())
        );

        // Both should be able to read the same content
        assert_eq!(alice.read(object_id), Some(b"alice's data".to_vec()));
        assert_eq!(bob.read(object_id), Some(b"alice's data".to_vec()));
    }

    #[tokio::test]
    async fn test_bidirectional_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_a = ObjectId(100);
        let object_b = ObjectId(200);

        let mut alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice writes object A
        let (commit_a, _) = alice.write_and_push(object_a, b"from alice").await.unwrap();

        // Bob receives and applies
        let event = recv_event(&mut bob_stream).await;
        bob.apply_event(&event);
        assert!(bob.has_commit(object_a, &commit_a));

        // Bob writes object B
        let (commit_b, _) = bob.write_and_push(object_b, b"from bob").await.unwrap();

        // Alice receives and applies
        let event = recv_event(&mut alice_stream).await;
        alice.apply_event(&event);
        assert!(alice.has_commit(object_b, &commit_b));

        // Both have both objects now
        assert!(alice.has_commit(object_a, &commit_a));
        assert!(alice.has_commit(object_b, &commit_b));
        assert!(bob.has_commit(object_a, &commit_a));
        assert!(bob.has_commit(object_b, &commit_b));
    }

    #[tokio::test]
    async fn test_three_clients_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");
        let mut charlie = harness.create_client("charlie");

        let object_id = ObjectId(300);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();
        let mut charlie_stream = charlie.subscribe_all().await.unwrap();

        // Alice writes
        let (commit_id, _) = alice.write_and_push(object_id, b"shared data").await.unwrap();

        // Both Bob and Charlie receive
        let bob_event = recv_event(&mut bob_stream).await;
        let charlie_event = recv_event(&mut charlie_stream).await;

        bob.apply_event(&bob_event);
        charlie.apply_event(&charlie_event);

        // All three have the commit
        assert!(alice.has_commit(object_id, &commit_id));
        assert!(bob.has_commit(object_id, &commit_id));
        assert!(charlie.has_commit(object_id, &commit_id));

        // All three can read the same content
        assert_eq!(alice.read(object_id), Some(b"shared data".to_vec()));
        assert_eq!(bob.read(object_id), Some(b"shared data".to_vec()));
        assert_eq!(charlie.read(object_id), Some(b"shared data".to_vec()));
    }

    #[tokio::test]
    async fn test_sequential_commits() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(400);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice writes three sequential commits
        let (c1, _) = alice.write_and_push(object_id, b"version 1").await.unwrap();
        let event1 = recv_event(&mut bob_stream).await;
        bob.apply_event(&event1);

        let (c2, _) = alice.write_and_push(object_id, b"version 2").await.unwrap();
        let event2 = recv_event(&mut bob_stream).await;
        bob.apply_event(&event2);

        let (c3, _) = alice.write_and_push(object_id, b"version 3").await.unwrap();
        let event3 = recv_event(&mut bob_stream).await;
        bob.apply_event(&event3);

        // Bob has all commits
        assert!(bob.has_commit(object_id, &c1));
        assert!(bob.has_commit(object_id, &c2));
        assert!(bob.has_commit(object_id, &c3));

        // Bob's content is the latest
        assert_eq!(bob.read(object_id), Some(b"version 3".to_vec()));

        // Both have the same frontier
        assert_eq!(alice.frontier(object_id), vec![c3]);
        assert_eq!(bob.frontier(object_id), vec![c3]);
    }

    #[tokio::test]
    async fn test_pusher_does_not_receive_own_broadcast() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let object_id = ObjectId(500);

        let mut alice_stream = alice.subscribe_all().await.unwrap();

        alice.write_and_push(object_id, b"my data").await.unwrap();

        // Alice should NOT receive her own broadcast
        assert_no_event(&mut alice_stream).await;
    }

    // ========================================================================
    // Multiple Objects Tests
    // ========================================================================

    #[tokio::test]
    async fn test_multiple_objects() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice creates 5 different objects
        let mut commits = Vec::new();
        for i in 0..5 {
            let object_id = ObjectId(600 + i);
            let (commit_id, _) = alice
                .write_and_push(object_id, format!("object {}", i).as_bytes())
                .await
                .unwrap();
            commits.push((object_id, commit_id));
        }

        // Bob receives all 5 broadcasts
        for _ in 0..5 {
            let event = recv_event(&mut bob_stream).await;
            bob.apply_event(&event);
        }

        // Bob has all commits
        for (object_id, commit_id) in &commits {
            assert!(bob.has_commit(*object_id, commit_id));
        }
    }

    // ========================================================================
    // Concurrent Write Tests
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_writes_to_different_objects() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_a = ObjectId(700);
        let object_b = ObjectId(701);

        let mut alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Both write simultaneously to different objects
        let (ca, _) = alice.write_and_push(object_a, b"alice's object").await.unwrap();
        let (cb, _) = bob.write_and_push(object_b, b"bob's object").await.unwrap();

        // Each receives the other's broadcast
        let alice_event = recv_event(&mut alice_stream).await;
        let bob_event = recv_event(&mut bob_stream).await;

        alice.apply_event(&alice_event);
        bob.apply_event(&bob_event);

        // Both have both objects
        assert!(alice.has_commit(object_a, &ca));
        assert!(alice.has_commit(object_b, &cb));
        assert!(bob.has_commit(object_a, &ca));
        assert!(bob.has_commit(object_b, &cb));
    }

    #[tokio::test]
    async fn test_concurrent_writes_to_same_object() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(800);

        let mut alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice writes first
        let (ca, _) = alice
            .write_and_push(object_id, b"alice's version")
            .await
            .unwrap();

        // Bob receives Alice's commit
        let bob_event = recv_event(&mut bob_stream).await;
        bob.apply_event(&bob_event);

        // Now Bob writes (his commit will have Alice's as parent)
        let (cb, _) = bob.write_and_push(object_id, b"bob's version").await.unwrap();

        // Alice receives Bob's commit
        let alice_event = recv_event(&mut alice_stream).await;
        alice.apply_event(&alice_event);

        // Both have both commits
        assert!(alice.has_commit(object_id, &ca));
        assert!(alice.has_commit(object_id, &cb));
        assert!(bob.has_commit(object_id, &ca));
        assert!(bob.has_commit(object_id, &cb));

        // Both should have the same frontier (Bob's commit)
        assert_eq!(alice.frontier(object_id), vec![cb]);
        assert_eq!(bob.frontier(object_id), vec![cb]);
    }

    // ========================================================================
    // Data Integrity Tests
    // ========================================================================

    #[tokio::test]
    async fn test_large_content() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(900);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Create 1MB of data
        let large_content: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let (commit_id, _) = alice
            .write_and_push(object_id, &large_content)
            .await
            .unwrap();

        let event = recv_event(&mut bob_stream).await;
        bob.apply_event(&event);

        // Bob should have the exact same content
        assert!(bob.has_commit(object_id, &commit_id));
        assert_eq!(bob.read(object_id), Some(large_content));
    }

    #[tokio::test]
    async fn test_storage_isolation() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let bob = harness.create_client("bob"); // Bob doesn't subscribe

        let object_id = ObjectId(1000);
        let _alice_stream = alice.subscribe_all().await.unwrap();

        // Alice writes
        let (commit_id, _) = alice.write_and_push(object_id, b"private").await.unwrap();

        // Alice has the commit
        assert!(alice.has_commit(object_id, &commit_id));

        // Bob does NOT have it (he never subscribed and never received the event)
        assert!(!bob.has_commit(object_id, &commit_id));
        assert_eq!(bob.read(object_id), None);
    }

    // ========================================================================
    // Many Clients Tests
    // ========================================================================

    #[tokio::test]
    async fn test_many_clients() {
        let harness = TestHarness::new();
        let object_id = ObjectId(1100);

        // Create 10 clients
        let mut clients: Vec<_> = (0..10)
            .map(|i| harness.create_client(format!("client_{}", i)))
            .collect();

        // All subscribe
        let mut streams = Vec::new();
        for client in &mut clients {
            streams.push(client.subscribe_all().await.unwrap());
        }

        // First client writes
        let (commit_id, _) = clients[0]
            .write_and_push(object_id, b"broadcast")
            .await
            .unwrap();

        // All other clients receive and apply
        for (i, stream) in streams.iter_mut().enumerate().skip(1) {
            let event = recv_event(stream).await;
            clients[i].apply_event(&event);
        }

        // All clients have the commit
        for client in &clients {
            assert!(client.has_commit(object_id, &commit_id));
        }
    }

    // ========================================================================
    // Server State Tests
    // ========================================================================

    #[tokio::test]
    async fn test_server_stores_commits() {
        use crate::storage::CommitStore;

        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let object_id = ObjectId(1200);
        let _stream = alice.subscribe_all().await.unwrap();

        let (commit_id, _) = alice.write_and_push(object_id, b"server data").await.unwrap();

        // Server should have the commit
        let server_commit = harness.server_env().get_commit(&commit_id).await;
        assert!(server_commit.is_some());
        assert_eq!(server_commit.unwrap().content.as_ref(), b"server data");

        // Server frontier should be updated
        let server_frontier = harness.server_env().get_frontier(object_id.0, "main").await;
        assert_eq!(server_frontier, vec![commit_id]);
    }

    #[tokio::test]
    async fn test_new_subscriber_receives_existing_data() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let object_id = ObjectId(1300);

        // Alice subscribes and writes
        let _alice_stream = alice.subscribe_all().await.unwrap();
        let (commit_id, _) = alice.write_and_push(object_id, b"existing").await.unwrap();

        // Bob subscribes AFTER the write
        let mut bob = harness.create_client("bob");
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Bob should receive initial data with the existing commit
        let event = recv_event(&mut bob_stream).await;
        bob.apply_event(&event);

        // Bob now has the commit
        assert!(bob.has_commit(object_id, &commit_id));
        assert_eq!(bob.read(object_id), Some(b"existing".to_vec()));
    }
}
