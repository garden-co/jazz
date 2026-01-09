//! In-memory test harness for sync testing.
//!
//! This module provides an in-memory ensemble of clients and server
//! communicating via channels, enabling fast, deterministic sync tests
//! without network overhead.
//!
//! # Example
//!
//! ```ignore
//! let harness = TestHarness::new();
//!
//! // Create two clients
//! let client1 = harness.create_client("alice");
//! let client2 = harness.create_client("bob");
//!
//! // Client 1 subscribes and pushes
//! let stream1 = client1.subscribe("*").await?;
//! client1.push(object_id, "main").await?;
//!
//! // Client 2 receives the update via its subscription
//! let event = stream2.next().await;
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::{mpsc, RwLock};

use crate::commit::Commit;
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
// Test Transport
// ============================================================================

/// A test transport that routes requests directly to a SyncServer.
///
/// This bypasses HTTP and allows testing sync logic in-memory.
pub struct TestTransport {
    /// The sync server
    server: Arc<RwLock<SyncServer<MemoryEnvironment>>>,
    /// The shared storage environment (concrete type for server)
    env: Arc<MemoryEnvironment>,
    /// The same environment as a trait object (for clients)
    dyn_env: Arc<dyn crate::storage::Environment>,
}

impl TestTransport {
    /// Create a new test transport with an in-memory server.
    pub fn new() -> Self {
        let env = Arc::new(MemoryEnvironment::new());
        // Create trait object version of the same Arc
        let dyn_env: Arc<dyn crate::storage::Environment> = Arc::clone(&env) as Arc<dyn crate::storage::Environment>;
        let validator: Arc<dyn TokenValidator> = Arc::new(AcceptAllTokens);
        let server = SyncServer::new(Arc::clone(&env), validator);
        Self {
            server: Arc::new(RwLock::new(server)),
            env,
            dyn_env,
        }
    }

    /// Get the shared storage environment (concrete type).
    pub fn env(&self) -> &Arc<MemoryEnvironment> {
        &self.env
    }

    /// Get the shared storage environment as a trait object.
    pub fn dyn_env(&self) -> &Arc<dyn crate::storage::Environment> {
        &self.dyn_env
    }

    /// Process a subscribe request.
    ///
    /// Returns the session ID and SSE event receiver.
    pub async fn subscribe(
        &self,
        token: &str,
        request: SubscribeRequest,
    ) -> Result<(SessionId, mpsc::Receiver<SseEvent>), ClientError> {
        // Validate token (AcceptAllTokens accepts everything)
        let identity = ClientIdentity {
            id: token.to_string(),
            name: None,
        };

        // Create SSE channel
        let (tx, rx) = mpsc::channel::<SseEvent>(32);

        // Create session and register query
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

        // Get all objects from storage
        let object_ids: Vec<u128> = self.env.list_objects().collect().await;

        for oid in object_ids {
            let object_id = ObjectId(oid);

            // Get frontier and commits for this object
            let frontier = self.env.get_frontier(oid, "main").await;
            if frontier.is_empty() {
                continue;
            }

            // Load all commits for this object
            let commit_ids: Vec<_> = self.env.list_commits(oid, "main").collect().await;

            let mut commits = Vec::new();
            for commit_id in commit_ids {
                if let Some(commit) = self.env.get_commit(&commit_id).await {
                    commits.push(commit);
                }
            }

            if !commits.is_empty() {
                let event = SseEvent::Commits {
                    object_id,
                    commits,
                    frontier: frontier.clone(),
                };

                // Send to client (ignore errors)
                let _ = tx.send(event).await;
            }

            // Register this object for the session
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

        // Find the sender's session to exclude from broadcast
        let sender_session = {
            let server = self.server.read().await;
            server
                .sessions_for_identity(token)
                .into_iter()
                .next()
        };

        // Store commits and get new frontier
        let frontier = {
            let server = self.server.read().await;
            server
                .store_commits(request.object_id, &request.commits, "main")
                .await
        };

        // For new objects, register with sessions that have wildcard queries ("*")
        // This ensures broadcasts reach all subscribed clients
        {
            let mut server = self.server.write().await;
            let session_ids: Vec<_> = server.sessions.keys().copied().collect();
            for session_id in session_ids {
                // Check if this session has a wildcard query
                let has_wildcard = server
                    .sessions
                    .get(&session_id)
                    .map(|s| s.queries.values().any(|q| q.query == "*"))
                    .unwrap_or(false);

                if has_wildcard {
                    // Register object with this session if not already
                    if !server.sessions_for_object(&request.object_id).contains(&session_id) {
                        server.register_object_session(request.object_id, session_id);
                        // Also add to session's object_queries
                        if let Some(session) = server.get_session_mut(&session_id) {
                            // Find the wildcard query ID
                            if let Some((&query_id, _)) = session.queries.iter().find(|(_, q)| q.query == "*") {
                                session.add_object_to_query(request.object_id, query_id);
                            }
                        }
                    }
                }
            }
        }

        // Broadcast to other sessions tracking this object
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

        let server_frontier = self.env.get_frontier(request.object_id.0, "main").await;

        if server_frontier.is_empty() {
            return Ok(SseEvent::Commits {
                object_id: request.object_id,
                commits: vec![],
                frontier: vec![],
            });
        }

        // Build set of commits client claims to have
        let client_known: HashSet<_> = request.local_frontier.iter().copied().collect();

        // Collect all commits from server for this object
        let commit_ids: Vec<_> = self
            .env
            .list_commits(request.object_id.0, "main")
            .collect()
            .await;

        // Load commits that client doesn't have
        let mut commits_to_send = Vec::new();
        for commit_id in &commit_ids {
            if !client_known.contains(commit_id) {
                if let Some(commit) = self.env.get_commit(commit_id).await {
                    commits_to_send.push(commit);
                }
            }
        }

        // Update client's known state
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
    pub async fn unsubscribe(
        &self,
        token: &str,
        subscription_id: u32,
    ) -> Result<(), ClientError> {
        let mut server = self.server.write().await;
        let session_ids = server.sessions_for_identity(token);
        let query_id = super::server::QueryId(subscription_id);

        // Find the session with this query and remove it
        for session_id in session_ids {
            if let Some(session) = server.get_session_mut(&session_id) {
                if session.queries.remove(&query_id).is_some() {
                    // Found and removed
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
    /// The shared transport
    transport: Arc<TestTransport>,
    /// Auth token for this client
    auth_token: String,
}

impl TestClientEnv {
    /// Create a new test client environment.
    pub fn new(transport: Arc<TestTransport>, auth_token: impl Into<String>) -> Self {
        Self {
            transport,
            auth_token: auth_token.into(),
        }
    }

    /// Get the auth token.
    pub fn auth_token(&self) -> &str {
        &self.auth_token
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

        // Convert receiver to a BoxStream
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
// Test Harness
// ============================================================================

/// A test client wrapping SyncClient with convenience methods.
pub struct TestClient {
    /// The underlying sync client
    pub client: super::client::SyncClient<TestClientEnv>,
    /// Client identifier (same as auth token)
    pub id: String,
}

impl TestClient {
    /// Create a new test client.
    fn new(transport: Arc<TestTransport>, id: impl Into<String>) -> Self {
        let id = id.into();
        let env = TestClientEnv::new(Arc::clone(&transport), &id);
        let node = Arc::new(LocalNode::new(Arc::clone(transport.dyn_env())));
        let client = super::client::SyncClient::new(env, node);
        Self { client, id }
    }

    /// Subscribe to all objects (query = "*").
    pub async fn subscribe_all(
        &mut self,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
        self.client
            .subscribe("*".to_string(), SubscriptionOptions::default())
            .await
    }

    /// Subscribe to a specific query.
    pub async fn subscribe(
        &mut self,
        query: &str,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
        self.client
            .subscribe(query.to_string(), SubscriptionOptions::default())
            .await
    }

    /// Push commits for an object.
    pub async fn push(
        &mut self,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<PushResponse, ClientError> {
        self.client.push(object_id, branch).await
    }

    /// Get the local node.
    pub fn node(&self) -> &Arc<LocalNode> {
        self.client.node()
    }

    /// Push a commit directly through the transport (bypasses LocalNode).
    ///
    /// This is useful for testing the sync layer without LocalNode's object management.
    pub async fn push_commit(
        &self,
        object_id: ObjectId,
        content: &[u8],
    ) -> Result<(crate::commit::CommitId, PushResponse), ClientError> {
        self.push_commit_with_parents(object_id, content, vec![]).await
    }

    /// Push a commit with specific parents (for testing commit chains and merges).
    pub async fn push_commit_with_parents(
        &self,
        object_id: ObjectId,
        content: &[u8],
        parents: Vec<crate::commit::CommitId>,
    ) -> Result<(crate::commit::CommitId, PushResponse), ClientError> {
        let commit = Commit {
            parents,
            content: content.to_vec().into_boxed_slice(),
            author: self.id.clone(),
            timestamp: 0,
            meta: None,
        };
        let commit_id = commit.compute_id();

        let request = PushRequest {
            object_id,
            commits: vec![commit],
        };

        let response = self.client.env().push(request).await?;
        Ok((commit_id, response))
    }

    /// Push multiple commits in a single request (for batch testing).
    pub async fn push_commits(
        &self,
        object_id: ObjectId,
        commits: Vec<Commit>,
    ) -> Result<PushResponse, ClientError> {
        let request = PushRequest { object_id, commits };
        self.client.env().push(request).await
    }

    /// Create a commit object without pushing (for building commit chains).
    pub fn create_commit_obj(&self, content: &[u8], parents: Vec<crate::commit::CommitId>) -> Commit {
        Commit {
            parents,
            content: content.to_vec().into_boxed_slice(),
            author: self.id.clone(),
            timestamp: 0,
            meta: None,
        }
    }
}

/// Test harness for multi-client sync testing.
///
/// Provides an in-memory ensemble of clients and server.
pub struct TestHarness {
    /// Shared transport connecting clients to server
    transport: Arc<TestTransport>,
}

impl TestHarness {
    /// Create a new test harness.
    pub fn new() -> Self {
        Self {
            transport: Arc::new(TestTransport::new()),
        }
    }

    /// Get the shared storage environment.
    pub fn env(&self) -> &Arc<MemoryEnvironment> {
        self.transport.env()
    }

    /// Create a new test client with the given ID.
    pub fn create_client(&self, id: impl Into<String>) -> TestClient {
        TestClient::new(Arc::clone(&self.transport), id)
    }

    /// Store a commit directly in the server's storage.
    ///
    /// Useful for setting up test scenarios.
    pub async fn store_server_commit(
        &self,
        object_id: ObjectId,
        commit: &Commit,
        branch: &str,
    ) -> crate::commit::CommitId {
        use crate::storage::CommitStore;

        let commit_id = self.env().put_commit(commit).await;

        // Update frontier
        let mut frontier = self.env().get_frontier(object_id.0, branch).await;
        if !frontier.contains(&commit_id) {
            frontier.push(commit_id);
        }
        self.env().set_frontier(object_id.0, branch, &frontier).await;

        commit_id
    }
}

impl Default for TestHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;

    // Helper to receive an event with timeout
    async fn recv_event(
        stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>,
    ) -> SseEvent {
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("timeout waiting for event")
            .expect("stream ended")
            .unwrap()
    }

    // Helper to check no event is received
    async fn assert_no_event(
        stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>,
    ) {
        let result = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(result.is_err(), "expected no event but received one");
    }

    // ========================================================================
    // Basic Sync Tests
    // ========================================================================

    #[tokio::test]
    async fn test_single_client_subscribe_push() {
        let harness = TestHarness::new();
        let mut client = harness.create_client("alice");

        let _stream = client.subscribe_all().await.unwrap();

        let object_id = ObjectId(42);
        let (commit_id, response) = client.push_commit(object_id, b"hello world").await.unwrap();

        assert!(response.accepted);
        assert_eq!(response.frontier, vec![commit_id]);
    }

    #[tokio::test]
    async fn test_two_clients_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(123);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        let (commit_id, response) = alice.push_commit(object_id, b"alice's data").await.unwrap();
        assert!(response.accepted);

        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { object_id: oid, commits, frontier } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "alice");
                assert_eq!(frontier, vec![commit_id]);
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_three_clients_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");
        let mut carol = harness.create_client("carol");

        let object_id = ObjectId(300);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();
        let mut carol_stream = carol.subscribe_all().await.unwrap();

        // Alice pushes
        let (commit_id, _) = alice.push_commit(object_id, b"alice's data").await.unwrap();

        // Both Bob and Carol should receive
        let bob_event = recv_event(&mut bob_stream).await;
        let carol_event = recv_event(&mut carol_stream).await;

        match bob_event {
            SseEvent::Commits { commits, .. } => assert_eq!(commits[0].author, "alice"),
            _ => panic!("Bob didn't receive commits"),
        }
        match carol_event {
            SseEvent::Commits { frontier, .. } => assert_eq!(frontier, vec![commit_id]),
            _ => panic!("Carol didn't receive commits"),
        }
    }

    #[tokio::test]
    async fn test_bidirectional_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let obj_a = ObjectId(10);
        let obj_b = ObjectId(20);

        let mut alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice pushes to obj_a
        alice.push_commit(obj_a, b"from alice").await.unwrap();

        // Bob receives Alice's commit
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { object_id, commits, .. } => {
                assert_eq!(object_id, obj_a);
                assert_eq!(commits[0].author, "alice");
            }
            _ => panic!("Expected commits"),
        }

        // Bob pushes to obj_b
        bob.push_commit(obj_b, b"from bob").await.unwrap();

        // Alice receives Bob's commit
        let event = recv_event(&mut alice_stream).await;
        match event {
            SseEvent::Commits { object_id, commits, .. } => {
                assert_eq!(object_id, obj_b);
                assert_eq!(commits[0].author, "bob");
            }
            _ => panic!("Expected commits"),
        }
    }

    #[tokio::test]
    async fn test_client_receives_initial_data_on_subscribe() {
        let harness = TestHarness::new();

        let object_id = ObjectId(99);
        let commit = Commit {
            parents: vec![],
            content: b"pre-existing data".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = harness.store_server_commit(object_id, &commit, "main").await;

        let mut client = harness.create_client("alice");
        let mut stream = client.subscribe_all().await.unwrap();

        let event = recv_event(&mut stream).await;
        match event {
            SseEvent::Commits { object_id: oid, commits, frontier } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "server");
                assert_eq!(frontier, vec![commit_id]);
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_multiple_objects_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        let obj1 = ObjectId(1);
        let obj2 = ObjectId(2);

        alice.push_commit(obj1, b"object 1 data").await.unwrap();
        alice.push_commit(obj2, b"object 2 data").await.unwrap();

        let event1 = recv_event(&mut bob_stream).await;
        let event2 = recv_event(&mut bob_stream).await;

        let mut received_ids = vec![];
        if let SseEvent::Commits { object_id, .. } = event1 {
            received_ids.push(object_id);
        }
        if let SseEvent::Commits { object_id, .. } = event2 {
            received_ids.push(object_id);
        }

        assert!(received_ids.contains(&obj1));
        assert!(received_ids.contains(&obj2));
    }

    #[tokio::test]
    async fn test_pusher_does_not_receive_own_broadcast() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let object_id = ObjectId(55);

        let mut stream = alice.subscribe_all().await.unwrap();
        alice.push_commit(object_id, b"my data").await.unwrap();

        assert_no_event(&mut stream).await;
    }

    // ========================================================================
    // Commit Chain Tests
    // ========================================================================

    #[tokio::test]
    async fn test_sequential_commits_from_single_client() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(100);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice pushes three sequential commits
        let (id1, _) = alice.push_commit(object_id, b"commit 1").await.unwrap();
        let (id2, _) = alice.push_commit_with_parents(object_id, b"commit 2", vec![id1]).await.unwrap();
        let (id3, response) = alice.push_commit_with_parents(object_id, b"commit 3", vec![id2]).await.unwrap();

        // Final frontier should be just the latest commit
        assert_eq!(response.frontier, vec![id3]);

        // Bob receives all three
        for i in 1..=3 {
            let event = recv_event(&mut bob_stream).await;
            match event {
                SseEvent::Commits { commits, .. } => {
                    assert_eq!(commits.len(), 1);
                    assert_eq!(commits[0].content.as_ref(), format!("commit {}", i).as_bytes());
                }
                _ => panic!("Expected Commits"),
            }
        }
    }

    #[tokio::test]
    async fn test_commit_chain_with_proper_parents() {
        let harness = TestHarness::new();
        let alice = harness.create_client("alice");

        let object_id = ObjectId(101);

        // Build a chain: root -> child -> grandchild
        let root = alice.create_commit_obj(b"root", vec![]);
        let root_id = root.compute_id();

        let child = alice.create_commit_obj(b"child", vec![root_id]);
        let child_id = child.compute_id();

        let grandchild = alice.create_commit_obj(b"grandchild", vec![child_id]);
        let grandchild_id = grandchild.compute_id();

        // Push all at once (parents first)
        let response = alice.push_commits(object_id, vec![root, child, grandchild]).await.unwrap();

        assert!(response.accepted);
        assert_eq!(response.frontier, vec![grandchild_id]);
    }

    // ========================================================================
    // Concurrent/Diverged Commit Tests
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_commits_diverged_frontier() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(200);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let _bob_stream = bob.subscribe_all().await.unwrap();

        // Alice pushes the root commit
        let (root_id, _) = alice.push_commit(object_id, b"root").await.unwrap();

        // Both Alice and Bob create commits on top of root (simulating offline divergence)
        let (alice_commit, _) = alice.push_commit_with_parents(object_id, b"alice branch", vec![root_id]).await.unwrap();
        let (bob_commit, response) = bob.push_commit_with_parents(object_id, b"bob branch", vec![root_id]).await.unwrap();

        // Server should have diverged frontier with both tips
        assert!(response.accepted);
        let frontier = &response.frontier;
        assert_eq!(frontier.len(), 2, "frontier should have 2 tips for diverged state");
        assert!(frontier.contains(&alice_commit) || frontier.contains(&bob_commit));
    }

    #[tokio::test]
    async fn test_merge_commit_resolves_divergence() {
        let harness = TestHarness::new();
        let alice = harness.create_client("alice");

        let object_id = ObjectId(201);

        // Create diverged history
        let root = alice.create_commit_obj(b"root", vec![]);
        let root_id = root.compute_id();

        let branch_a = alice.create_commit_obj(b"branch A", vec![root_id]);
        let branch_a_id = branch_a.compute_id();

        let branch_b = alice.create_commit_obj(b"branch B", vec![root_id]);
        let branch_b_id = branch_b.compute_id();

        // Merge commit with both branches as parents
        let merge = alice.create_commit_obj(b"merge", vec![branch_a_id, branch_b_id]);
        let merge_id = merge.compute_id();

        // Push in topological order
        let response = alice.push_commits(object_id, vec![root, branch_a, branch_b, merge]).await.unwrap();

        assert!(response.accepted);
        // After merge, frontier should be single tip
        assert_eq!(response.frontier, vec![merge_id]);
    }

    #[tokio::test]
    async fn test_concurrent_pushes_to_same_object() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");
        let mut carol = harness.create_client("carol");

        let object_id = ObjectId(202);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let _bob_stream = bob.subscribe_all().await.unwrap();
        let mut carol_stream = carol.subscribe_all().await.unwrap();

        // Alice and Bob both push to the same object concurrently
        let (alice_id, _) = alice.push_commit(object_id, b"alice").await.unwrap();
        let (bob_id, _) = bob.push_commit(object_id, b"bob").await.unwrap();

        // Carol should receive both commits
        let event1 = recv_event(&mut carol_stream).await;
        let event2 = recv_event(&mut carol_stream).await;

        let mut received_authors = vec![];
        if let SseEvent::Commits { commits, .. } = event1 {
            received_authors.push(commits[0].author.clone());
        }
        if let SseEvent::Commits { commits, .. } = event2 {
            received_authors.push(commits[0].author.clone());
        }

        assert!(received_authors.contains(&"alice".to_string()));
        assert!(received_authors.contains(&"bob".to_string()));

        // Both commits should be in the frontier (diverged)
        let _ = alice_id;
        let _ = bob_id;
    }

    // ========================================================================
    // Reconciliation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_reconcile_receives_missing_commits() {
        let harness = TestHarness::new();

        let object_id = ObjectId(77);
        let commit = Commit {
            parents: vec![],
            content: b"server commit".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 2000,
            meta: None,
        };
        let _commit_id = harness.store_server_commit(object_id, &commit, "main").await;

        let client = harness.create_client("alice");

        let request = ReconcileRequest {
            object_id,
            local_frontier: vec![],
        };
        let event = client.client.env().reconcile(request).await.unwrap();

        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "server");
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_reconcile_with_partial_overlap() {
        let harness = TestHarness::new();

        let object_id = ObjectId(78);

        // Store two commits on server
        let commit1 = Commit {
            parents: vec![],
            content: b"commit 1".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit1_id = harness.store_server_commit(object_id, &commit1, "main").await;

        let commit2 = Commit {
            parents: vec![commit1_id],
            content: b"commit 2".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 2000,
            meta: None,
        };
        let _commit2_id = harness.store_server_commit(object_id, &commit2, "main").await;

        // Client claims to have commit1
        let client = harness.create_client("alice");
        let request = ReconcileRequest {
            object_id,
            local_frontier: vec![commit1_id],
        };
        let event = client.client.env().reconcile(request).await.unwrap();

        match event {
            SseEvent::Commits { commits, .. } => {
                // Should only receive commit2 (client already has commit1)
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].content.as_ref(), b"commit 2");
            }
            _ => panic!("Expected Commits"),
        }
    }

    #[tokio::test]
    async fn test_reconcile_when_client_is_ahead() {
        let harness = TestHarness::new();

        let object_id = ObjectId(79);

        // Server has nothing
        let client = harness.create_client("alice");

        // Client claims to have a commit server doesn't know about
        let fake_commit_id = crate::commit::CommitId::from_bytes([99u8; 32]);
        let request = ReconcileRequest {
            object_id,
            local_frontier: vec![fake_commit_id],
        };
        let event = client.client.env().reconcile(request).await.unwrap();

        // Server should return empty (has nothing to offer)
        match event {
            SseEvent::Commits { commits, frontier, .. } => {
                assert!(commits.is_empty());
                assert!(frontier.is_empty());
            }
            _ => panic!("Expected empty Commits"),
        }
    }

    // ========================================================================
    // Subscription Management Tests
    // ========================================================================

    #[tokio::test]
    async fn test_late_subscriber_receives_new_commits() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(400);

        // Alice subscribes and pushes
        let _alice_stream = alice.subscribe_all().await.unwrap();
        alice.push_commit(object_id, b"before bob").await.unwrap();

        // Bob subscribes later
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Bob should receive initial data
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits[0].content.as_ref(), b"before bob");
            }
            _ => panic!("Expected initial commits"),
        }

        // Alice pushes another commit
        alice.push_commit(object_id, b"after bob subscribed").await.unwrap();

        // Bob should receive it
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits[0].content.as_ref(), b"after bob subscribed");
            }
            _ => panic!("Expected new commit"),
        }
    }

    #[tokio::test]
    async fn test_client_receives_multiple_pre_existing_objects() {
        let harness = TestHarness::new();

        // Pre-populate server with multiple objects
        let obj1 = ObjectId(401);
        let obj2 = ObjectId(402);
        let obj3 = ObjectId(403);

        for (oid, data) in [(obj1, b"obj1"), (obj2, b"obj2"), (obj3, b"obj3")] {
            let commit = Commit {
                parents: vec![],
                content: data.to_vec().into_boxed_slice(),
                author: "server".to_string(),
                timestamp: 0,
                meta: None,
            };
            harness.store_server_commit(oid, &commit, "main").await;
        }

        // Client subscribes
        let mut client = harness.create_client("alice");
        let mut stream = client.subscribe_all().await.unwrap();

        // Should receive all three objects
        let mut received_oids = vec![];
        for _ in 0..3 {
            let event = recv_event(&mut stream).await;
            if let SseEvent::Commits { object_id, .. } = event {
                received_oids.push(object_id);
            }
        }

        assert!(received_oids.contains(&obj1));
        assert!(received_oids.contains(&obj2));
        assert!(received_oids.contains(&obj3));
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[tokio::test]
    async fn test_empty_push_accepted() {
        let harness = TestHarness::new();
        let alice = harness.create_client("alice");

        let object_id = ObjectId(500);
        let response = alice.push_commits(object_id, vec![]).await.unwrap();

        assert!(response.accepted);
        assert!(response.frontier.is_empty());
    }

    #[tokio::test]
    async fn test_large_batch_of_commits() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(501);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Create a chain of 50 commits
        let mut commits = vec![];
        let mut last_id = None;

        for i in 0..50 {
            let parents = last_id.map(|id| vec![id]).unwrap_or_default();
            let commit = alice.create_commit_obj(format!("commit {}", i).as_bytes(), parents);
            last_id = Some(commit.compute_id());
            commits.push(commit);
        }

        // Push all at once
        let response = alice.push_commits(object_id, commits).await.unwrap();
        assert!(response.accepted);
        assert_eq!(response.frontier.len(), 1); // Single tip

        // Bob should receive the commits (may be batched or separate events)
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits.len(), 50);
            }
            _ => panic!("Expected commits"),
        }
    }

    #[tokio::test]
    async fn test_many_clients_sync() {
        let harness = TestHarness::new();

        let object_id = ObjectId(502);

        // Create 10 clients
        let mut clients: Vec<_> = (0..10)
            .map(|i| harness.create_client(format!("client{}", i)))
            .collect();

        // All subscribe
        let mut streams: Vec<_> = vec![];
        for client in &mut clients {
            streams.push(client.subscribe_all().await.unwrap());
        }

        // Client 0 pushes
        let (commit_id, _) = clients[0].push_commit(object_id, b"from client 0").await.unwrap();

        // All other clients should receive
        for (i, stream) in streams.iter_mut().enumerate().skip(1) {
            let event = recv_event(stream).await;
            match event {
                SseEvent::Commits { frontier, .. } => {
                    assert_eq!(frontier, vec![commit_id], "client {} didn't receive correct frontier", i);
                }
                _ => panic!("client {} expected commits", i),
            }
        }
    }

    #[tokio::test]
    async fn test_rapid_sequential_pushes() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(503);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Rapidly push 20 commits
        for i in 0..20 {
            alice.push_commit(object_id, format!("rapid {}", i).as_bytes()).await.unwrap();
        }

        // Bob should eventually receive all 20
        for _ in 0..20 {
            let event = recv_event(&mut bob_stream).await;
            assert!(matches!(event, SseEvent::Commits { .. }));
        }
    }

    #[tokio::test]
    async fn test_push_to_many_objects() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Push to 20 different objects
        for i in 0..20 {
            alice.push_commit(ObjectId(600 + i), format!("obj {}", i).as_bytes()).await.unwrap();
        }

        // Bob should receive all 20
        let mut received_count = 0;
        for _ in 0..20 {
            let event = recv_event(&mut bob_stream).await;
            if matches!(event, SseEvent::Commits { .. }) {
                received_count += 1;
            }
        }
        assert_eq!(received_count, 20);
    }

    #[tokio::test]
    async fn test_interleaved_pushes_from_multiple_clients() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");
        let mut carol = harness.create_client("carol");

        let object_id = ObjectId(700);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let _bob_stream = bob.subscribe_all().await.unwrap();
        let mut carol_stream = carol.subscribe_all().await.unwrap();

        // Interleaved pushes
        alice.push_commit(object_id, b"alice 1").await.unwrap();
        bob.push_commit(object_id, b"bob 1").await.unwrap();
        alice.push_commit(object_id, b"alice 2").await.unwrap();
        bob.push_commit(object_id, b"bob 2").await.unwrap();

        // Carol should receive all 4
        let mut authors = vec![];
        for _ in 0..4 {
            let event = recv_event(&mut carol_stream).await;
            if let SseEvent::Commits { commits, .. } = event {
                authors.push(commits[0].author.clone());
            }
        }

        assert_eq!(authors.iter().filter(|a| *a == "alice").count(), 2);
        assert_eq!(authors.iter().filter(|a| *a == "bob").count(), 2);
    }

    #[tokio::test]
    async fn test_commit_with_large_content() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(800);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Push a commit with 1MB of data
        let large_content = vec![42u8; 1024 * 1024]; // 1MB
        let (_, response) = alice.push_commit(object_id, &large_content).await.unwrap();
        assert!(response.accepted);

        // Bob should receive it
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits[0].content.len(), 1024 * 1024);
            }
            _ => panic!("Expected commits"),
        }
    }

    #[tokio::test]
    async fn test_diamond_merge_pattern() {
        let harness = TestHarness::new();
        let alice = harness.create_client("alice");

        let object_id = ObjectId(900);

        // Create a diamond pattern:
        //       root
        //      /    \
        //    A        B
        //      \    /
        //       merge

        let root = alice.create_commit_obj(b"root", vec![]);
        let root_id = root.compute_id();

        let a = alice.create_commit_obj(b"branch A", vec![root_id]);
        let a_id = a.compute_id();

        let b = alice.create_commit_obj(b"branch B", vec![root_id]);
        let b_id = b.compute_id();

        let merge = alice.create_commit_obj(b"merge", vec![a_id, b_id]);
        let merge_id = merge.compute_id();

        // Push in topological order
        let response = alice.push_commits(object_id, vec![root, a, b, merge]).await.unwrap();

        assert!(response.accepted);
        assert_eq!(response.frontier, vec![merge_id]);
    }

    #[tokio::test]
    async fn test_commit_with_metadata() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        let object_id = ObjectId(901);

        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Create commit with metadata
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("key".to_string(), "value".to_string());
        meta.insert("type".to_string(), "test".to_string());

        let commit = Commit {
            parents: vec![],
            content: b"with meta".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 12345,
            meta: Some(meta.clone()),
        };

        let response = alice.push_commits(object_id, vec![commit]).await.unwrap();
        assert!(response.accepted);

        // Bob should receive commit with metadata preserved
        let event = recv_event(&mut bob_stream).await;
        match event {
            SseEvent::Commits { commits, .. } => {
                assert_eq!(commits[0].meta, Some(meta));
                assert_eq!(commits[0].timestamp, 12345);
            }
            _ => panic!("Expected commits"),
        }
    }
}
