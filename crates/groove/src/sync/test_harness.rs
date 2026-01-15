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
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{RwLock, mpsc};

use crate::commit::CommitId;
use crate::node::LocalNode;
use crate::object::ObjectId;
use crate::sql::Database;
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
                    object_meta: None, // TODO: Include metadata for first sync
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

                if has_wildcard
                    && !server
                        .sessions_for_object(&request.object_id)
                        .contains(&session_id)
                {
                    server.register_object_session(request.object_id, session_id);
                    if let Some(session) = server.get_session_mut(&session_id)
                        && let Some((&query_id, _)) =
                            session.queries.iter().find(|(_, q)| q.query == "*")
                    {
                        session.add_object_to_query(request.object_id, query_id);
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
                    request.object_meta.clone(),
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
                object_meta: None,
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
            if !client_known.contains(commit_id)
                && let Some(commit) = self.server_env.get_commit(commit_id).await
            {
                commits_to_send.push(commit);
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
            object_meta: None, // TODO: Include metadata for first sync
        })
    }

    /// Process an unsubscribe request.
    pub async fn unsubscribe(&self, token: &str, subscription_id: u32) -> Result<(), ClientError> {
        let mut server = self.server.write().await;
        let session_ids = server.sessions_for_identity(token);
        let query_id = super::server::QueryId(subscription_id);

        for session_id in session_ids {
            if let Some(session) = server.get_session_mut(&session_id)
                && session.queries.remove(&query_id).is_some()
            {
                break;
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
#[derive(Clone)]
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
        let (_session_id, rx) = self.transport.subscribe(&self.auth_token, request).await?;

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
    /// Create a new test client with its own Database.
    fn new(transport: Arc<TestTransport>, id: impl Into<String>) -> Self {
        let id = id.into();
        let env = TestClientEnv::new(Arc::clone(&transport), &id);
        // Each client gets its OWN Database - NOT shared with server
        let db = crate::sql::Database::in_memory();
        let db_state = db.into_state();
        let sync_client = super::client::SyncClient::new(env, db_state);
        Self { sync_client, id }
    }

    /// Get the client's LocalNode.
    pub fn node(&self) -> &LocalNode {
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
            .map_err(|e| ClientError::new(500, format!("Write failed: {:?}", e)))?;

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
// SyncedNode Test Support
// ============================================================================

use super::runtime::TokioRuntime;
use super::synced_node::{SyncConfig, SyncedNode};

/// Create a SyncedNode for testing with a given client environment.
pub fn create_synced_node(
    _transport: Arc<TestTransport>,
    _id: &str,
) -> Arc<SyncedNode<TokioRuntime, TestClientEnv>> {
    let db = Database::in_memory();
    Arc::new(SyncedNode::new(db.into_state(), TokioRuntime))
}

/// Create a SyncedNode with custom config for testing.
pub fn create_synced_node_with_config(
    _transport: Arc<TestTransport>,
    _id: &str,
    config: SyncConfig,
) -> Arc<SyncedNode<TokioRuntime, TestClientEnv>> {
    let db = Database::in_memory();
    Arc::new(SyncedNode::with_config(db.into_state(), TokioRuntime, config))
}

// ============================================================================
// Multi-Server Test Harness
// ============================================================================

use super::synced_node::UpstreamId;
use std::collections::HashMap;

/// A server in the multi-server test harness.
pub struct TestServer {
    /// The server's transport (handles incoming requests).
    pub transport: Arc<TestTransport>,
    /// The server's SyncedNode (for upstream connections).
    pub synced_node: Arc<SyncedNode<TokioRuntime, TestClientEnv>>,
    /// Server name/identifier.
    pub name: String,
}

impl TestServer {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let transport = Arc::new(TestTransport::new());
        let db = Database::in_memory();
        let synced_node = Arc::new(SyncedNode::new(db.into_state(), TokioRuntime));
        Self {
            transport,
            synced_node,
            name,
        }
    }

    /// Get the server's storage environment.
    pub fn storage(&self) -> &Arc<MemoryEnvironment> {
        self.transport.server_env()
    }
}

/// Multi-server test harness for hierarchical sync testing.
///
/// Supports topologies like:
/// ```text
///   Client A -> Edge Server -> Origin Server
///   Client B -> Edge Server -> Origin Server
/// ```
///
/// Each server has its own storage and can connect upstream to other servers.
pub struct MultiServerHarness {
    /// Named servers in the harness.
    servers: HashMap<String, TestServer>,
}

impl MultiServerHarness {
    /// Create a new multi-server harness.
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
        }
    }

    /// Create a new server in the harness.
    ///
    /// Returns a reference to the server for further configuration.
    pub fn create_server(&mut self, name: impl Into<String>) -> &TestServer {
        let name = name.into();
        let server = TestServer::new(&name);
        self.servers.insert(name.clone(), server);
        self.servers.get(&name).unwrap()
    }

    /// Get a server by name.
    pub fn get_server(&self, name: &str) -> Option<&TestServer> {
        self.servers.get(name)
    }

    /// Connect one server to another as upstream.
    ///
    /// After this, the `from` server can subscribe to and push to the `to` server.
    ///
    /// Returns the UpstreamId for the new connection.
    pub fn connect_upstream(&self, from: &str, to: &str) -> Option<UpstreamId> {
        let from_server = self.servers.get(from)?;
        let to_server = self.servers.get(to)?;

        // Create a ClientEnv that routes to the upstream server's transport
        let env = TestClientEnv::new(Arc::clone(&to_server.transport), format!("server:{}", from));

        // Add the upstream connection
        let upstream_id = from_server.synced_node.add_upstream(env);
        Some(upstream_id)
    }

    /// Create a client connected to a specific server.
    pub fn create_client(&self, id: impl Into<String>, server_name: &str) -> Option<TestClient> {
        let server = self.servers.get(server_name)?;
        Some(TestClient::new(Arc::clone(&server.transport), id))
    }

    /// Start upstream sync for a server connection.
    ///
    /// This starts the background event loop that processes SSE events
    /// from the upstream server.
    pub fn start_upstream_sync(
        &self,
        server_name: &str,
        upstream_id: UpstreamId,
        query: &str,
    ) -> bool {
        if let Some(server) = self.servers.get(server_name) {
            server.synced_node.start_upstream_sync(
                upstream_id,
                vec![(query.to_string(), SubscriptionOptions::default())],
            );
            true
        } else {
            false
        }
    }
}

impl Default for MultiServerHarness {
    fn default() -> Self {
        Self::new()
    }
}
