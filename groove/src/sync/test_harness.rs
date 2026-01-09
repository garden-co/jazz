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
        let commit = Commit {
            parents: vec![],
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

    #[tokio::test]
    async fn test_single_client_subscribe_push() {
        let harness = TestHarness::new();
        let mut client = harness.create_client("alice");

        // Subscribe to all objects
        let _stream = client.subscribe_all().await.unwrap();

        // Push a commit directly through the transport
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

        // Both clients subscribe
        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice pushes a commit
        let (commit_id, response) = alice.push_commit(object_id, b"alice's data").await.unwrap();
        assert!(response.accepted);

        // Bob should receive the commit via SSE
        let event = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            bob_stream.next(),
        )
        .await
        .expect("timeout waiting for event")
        .expect("stream ended");

        match event.unwrap() {
            SseEvent::Commits {
                object_id: oid,
                commits,
                frontier,
            } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "alice");
                assert_eq!(frontier, vec![commit_id]);
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_client_receives_initial_data_on_subscribe() {
        let harness = TestHarness::new();

        // Store a commit on the server first
        let object_id = ObjectId(99);
        let commit = Commit {
            parents: vec![],
            content: b"pre-existing data".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = harness.store_server_commit(object_id, &commit, "main").await;

        // Now create a client and subscribe
        let mut client = harness.create_client("alice");
        let mut stream = client.subscribe_all().await.unwrap();

        // Client should receive the pre-existing commit
        let event = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            stream.next(),
        )
        .await
        .expect("timeout waiting for event")
        .expect("stream ended");

        match event.unwrap() {
            SseEvent::Commits {
                object_id: oid,
                commits,
                frontier,
            } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "server");
                assert_eq!(frontier, vec![commit_id]);
            }
            other => panic!("Expected Commits event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_reconcile_receives_missing_commits() {
        let harness = TestHarness::new();

        // Store a commit on the server
        let object_id = ObjectId(77);
        let commit = Commit {
            parents: vec![],
            content: b"server commit".to_vec().into_boxed_slice(),
            author: "server".to_string(),
            timestamp: 2000,
            meta: None,
        };
        let _commit_id = harness.store_server_commit(object_id, &commit, "main").await;

        // Create client with no local data
        let client = harness.create_client("alice");

        // Reconcile should return the missing commit
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
    async fn test_pusher_does_not_receive_own_broadcast() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");

        let object_id = ObjectId(55);

        // Subscribe
        let mut stream = alice.subscribe_all().await.unwrap();

        // Push a commit
        alice.push_commit(object_id, b"my data").await.unwrap();

        // Alice should NOT receive her own commit back
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            stream.next(),
        )
        .await;

        // Should timeout (no event received)
        assert!(result.is_err(), "Alice should not receive her own broadcast");
    }

    #[tokio::test]
    async fn test_multiple_objects_sync() {
        let harness = TestHarness::new();
        let mut alice = harness.create_client("alice");
        let mut bob = harness.create_client("bob");

        // Both subscribe
        let _alice_stream = alice.subscribe_all().await.unwrap();
        let mut bob_stream = bob.subscribe_all().await.unwrap();

        // Alice pushes commits for two different objects
        let obj1 = ObjectId(1);
        let obj2 = ObjectId(2);

        alice.push_commit(obj1, b"object 1 data").await.unwrap();
        alice.push_commit(obj2, b"object 2 data").await.unwrap();

        // Bob should receive both
        let event1 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            bob_stream.next(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let event2 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            bob_stream.next(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        // Collect received object IDs
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
}
