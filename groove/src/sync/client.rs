//! Sync client implementation.
//!
//! The client handles:
//! - HTTP requests to server (subscribe, push, reconcile) via ClientEnv
//! - SSE event handling for real-time updates
//! - Persistent tracking of unsynced objects via SyncStateStore
//! - Automatic reconnection with exponential backoff

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::commit::CommitId;
use crate::node::LocalNode;
use crate::object::ObjectId;

use super::env::{ClientEnv, ClientError};
use super::negotiation::{commits_to_send, compare_frontiers, FrontierComparison};
use super::protocol::{
    PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest, SubscriptionOptions,
};

/// State of a query subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Subscription is pending (request sent, waiting for response)
    Pending,
    /// Subscription is active
    Active,
    /// Subscription failed
    Failed(String),
}

/// A query subscription on the client.
#[derive(Debug)]
pub struct QuerySubscription {
    /// The SQL query
    pub query: String,
    /// Subscription options
    pub options: SubscriptionOptions,
    /// Current state
    pub state: SubscriptionState,
    /// Objects received via this subscription
    pub objects: HashSet<ObjectId>,
}

impl QuerySubscription {
    /// Create a new pending subscription.
    pub fn new(query: String, options: SubscriptionOptions) -> Self {
        Self {
            query,
            options,
            state: SubscriptionState::Pending,
            objects: HashSet::new(),
        }
    }
}

/// Configuration for automatic reconnection.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnect attempt (ms)
    pub initial_delay_ms: u64,
    /// Maximum delay between reconnect attempts (ms)
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Maximum number of reconnect attempts (None = unlimited)
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
            max_attempts: None,
        }
    }
}

/// Connection state of the sync client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Attempting to connect
    Connecting,
    /// Connected and syncing
    Connected,
    /// Reconnecting after disconnect
    Reconnecting { attempt: u32 },
}

/// The sync client.
///
/// Manages connection to server, query subscriptions, and sync state.
/// Generic over `E: ClientEnv` which provides the transport layer.
///
/// Unsynced object tracking is delegated to the `LocalNode`'s storage
/// via the `SyncStateStore` trait for persistence across restarts.
pub struct SyncClient<E: ClientEnv> {
    /// Transport environment for HTTP/SSE operations
    env: E,
    /// Local node for reading/writing objects
    node: Arc<LocalNode>,
    /// Current connection state
    pub connection_state: ConnectionState,
    /// Active query subscriptions by ID
    pub subscriptions: HashMap<u32, QuerySubscription>,
    /// Next subscription ID
    next_subscription_id: u32,
    /// Server's assumed known state per object
    pub server_known_state: HashMap<ObjectId, Vec<CommitId>>,
    /// Reconnection configuration
    pub reconnect_config: ReconnectConfig,
}

impl<E: ClientEnv> SyncClient<E> {
    /// Create a new sync client.
    ///
    /// # Arguments
    ///
    /// * `env` - The transport environment (implements ClientEnv)
    /// * `node` - The local node for object storage
    pub fn new(env: E, node: Arc<LocalNode>) -> Self {
        Self {
            env,
            node,
            connection_state: ConnectionState::Disconnected,
            subscriptions: HashMap::new(),
            next_subscription_id: 1,
            server_known_state: HashMap::new(),
            reconnect_config: ReconnectConfig::default(),
        }
    }

    /// Create a new sync client with custom reconnection configuration.
    pub fn with_reconnect_config(env: E, node: Arc<LocalNode>, reconnect_config: ReconnectConfig) -> Self {
        Self {
            env,
            node,
            connection_state: ConnectionState::Disconnected,
            subscriptions: HashMap::new(),
            next_subscription_id: 1,
            server_known_state: HashMap::new(),
            reconnect_config,
        }
    }

    /// Get a reference to the transport environment.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// Get a reference to the local node.
    pub fn node(&self) -> &Arc<LocalNode> {
        &self.node
    }

    /// Allocate a new subscription ID.
    fn next_subscription_id(&mut self) -> u32 {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }

    /// Create a subscription request.
    pub fn create_subscription(&mut self, query: String, options: SubscriptionOptions) -> u32 {
        let id = self.next_subscription_id();
        self.subscriptions
            .insert(id, QuerySubscription::new(query, options));
        id
    }

    /// Mark a subscription as active.
    pub fn mark_subscription_active(&mut self, id: u32) {
        if let Some(sub) = self.subscriptions.get_mut(&id) {
            sub.state = SubscriptionState::Active;
        }
    }

    /// Mark a subscription as failed.
    pub fn mark_subscription_failed(&mut self, id: u32, error: String) {
        if let Some(sub) = self.subscriptions.get_mut(&id) {
            sub.state = SubscriptionState::Failed(error);
        }
    }

    /// Remove a subscription.
    pub fn remove_subscription(&mut self, id: u32) -> Option<QuerySubscription> {
        self.subscriptions.remove(&id)
    }

    /// Get subscription request for an ID.
    pub fn get_subscribe_request(&self, id: u32) -> Option<SubscribeRequest> {
        self.subscriptions.get(&id).map(|sub| SubscribeRequest {
            query: sub.query.clone(),
            options: sub.options.clone(),
        })
    }

    /// Update server's assumed known state for an object.
    pub fn update_server_known_state(&mut self, object_id: ObjectId, frontier: Vec<CommitId>) {
        self.server_known_state.insert(object_id, frontier);
    }

    /// Get server's assumed known state for an object.
    pub fn get_server_known_state(&self, object_id: &ObjectId) -> Option<&Vec<CommitId>> {
        self.server_known_state.get(object_id)
    }

    /// Create a push request for an object.
    ///
    /// Returns None if there are no commits to push.
    pub fn create_push_request(&self, object_id: ObjectId, branch: &str) -> Option<PushRequest> {
        // Get local frontier
        let local_frontier = self.node.frontier(object_id, branch).ok()??;

        // Get server's known frontier (empty if unknown)
        let server_frontier = self
            .server_known_state
            .get(&object_id)
            .cloned()
            .unwrap_or_default();

        // Check if we need to push anything
        if compare_frontiers(&local_frontier, &server_frontier) == FrontierComparison::Identical {
            return None;
        }

        // Get the branch to find commits to send
        let obj = self.node.get_object(object_id)?;
        let obj_read = obj.read().ok()?;
        let branch_ref = obj_read.branch_ref(branch)?;
        let branch_read = branch_ref.read().ok()?;

        let commits = commits_to_send(&branch_read, &local_frontier, &server_frontier);

        if commits.is_empty() {
            return None;
        }

        Some(PushRequest { object_id, commits })
    }

    /// Handle a push response from the server.
    ///
    /// Updates server known state on success.
    /// Note: Clearing unsynced flag should be done via storage after calling this.
    pub fn handle_push_response(&mut self, response: &PushResponse) {
        if response.accepted {
            // Update server known state
            self.update_server_known_state(response.object_id, response.frontier.clone());
        }
    }

    /// Handle an SSE event from the server.
    pub fn handle_sse_event(&mut self, event: &SseEvent) {
        match event {
            SseEvent::Commits {
                object_id,
                commits: _,
                frontier,
            } => {
                // Apply commits to local node
                // TODO: This requires adding commits to LocalNode - implementation detail

                // Update server known state
                self.update_server_known_state(*object_id, frontier.clone());
            }
            SseEvent::Excluded { object_id } => {
                // Server says this object is no longer in any subscribed query
                // Remove from our tracking but keep local data
                self.server_known_state.remove(object_id);
            }
            SseEvent::Truncate {
                object_id: _,
                truncate_at: _,
            } => {
                // Server is truncating history
                // TODO: Truncate local copy - requires truncation support in LocalNode
            }
            SseEvent::Request {
                object_id: _,
                commit_ids: _,
            } => {
                // Server is requesting commits we have
                // TODO: Push these commits - would trigger a push request
            }
            SseEvent::Error { code: _, message: _ } => {
                // TODO: Handle error - log or surface to application
            }
        }
    }

    /// Create a reconcile request for an object.
    pub fn create_reconcile_request(
        &self,
        object_id: ObjectId,
        branch: &str,
    ) -> Option<ReconcileRequest> {
        let local_frontier = self.node.frontier(object_id, branch).ok()??;
        Some(ReconcileRequest {
            object_id,
            local_frontier,
        })
    }

    /// Set connection state.
    pub fn set_connection_state(&mut self, state: ConnectionState) {
        self.connection_state = state;
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.connection_state == ConnectionState::Connected
    }

    // ========================================================================
    // High-level async operations using ClientEnv
    // ========================================================================

    /// Subscribe to a query and return the SSE event stream.
    pub async fn subscribe(
        &mut self,
        query: String,
        options: SubscriptionOptions,
    ) -> Result<futures::stream::BoxStream<'static, Result<SseEvent, ClientError>>, ClientError>
    {
        let id = self.next_subscription_id();
        let request = SubscribeRequest {
            query: query.clone(),
            options: options.clone(),
        };

        // Call environment to make HTTP request and get SSE stream
        let stream = self.env.subscribe(request).await?;

        // Track subscription locally
        self.subscriptions
            .insert(id, QuerySubscription::new(query, options));
        self.mark_subscription_active(id);

        Ok(stream)
    }

    /// Push local commits for an object to the server.
    ///
    /// On success, clears the unsynced flag via storage.
    pub async fn push(
        &mut self,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<PushResponse, ClientError> {
        let request = self
            .create_push_request(object_id, branch)
            .ok_or_else(|| ClientError::new(0, "No commits to push"))?;

        let response = self.env.push(request).await?;

        // Handle response (updates server known state)
        self.handle_push_response(&response);

        // On successful push, clear unsynced flag via storage
        if response.accepted {
            self.node.env().clear_unsynced(&object_id).await;
        }

        Ok(response)
    }

    /// Request reconciliation for an object.
    pub async fn reconcile(
        &mut self,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<SseEvent, ClientError> {
        let request = self
            .create_reconcile_request(object_id, branch)
            .ok_or_else(|| ClientError::new(0, "Object or branch not found"))?;

        let event = self.env.reconcile(request).await?;
        self.handle_sse_event(&event);

        Ok(event)
    }

    /// Unsubscribe from a query by subscription ID.
    pub async fn unsubscribe(&mut self, subscription_id: u32) -> Result<(), ClientError> {
        self.env.unsubscribe(subscription_id).await?;
        self.remove_subscription(subscription_id);
        Ok(())
    }

    /// Push all unsynced objects on reconnect (data loss prevention).
    ///
    /// Returns results for each push attempt.
    pub async fn push_all_unsynced(
        &mut self,
        branch: &str,
    ) -> Vec<(ObjectId, Result<PushResponse, ClientError>)> {
        let unsynced = self.node.env().get_unsynced_objects().await;
        let mut results = Vec::new();
        for object_id in unsynced {
            let result = self.push(object_id, branch).await;
            results.push((object_id, result));
        }
        results
    }

    /// Mark an object as having unsynced local changes via storage.
    pub async fn mark_unsynced(&self, object_id: ObjectId) {
        self.node.env().mark_unsynced(object_id).await;
    }

    /// Check if an object has unsynced changes via storage.
    pub async fn is_unsynced(&self, object_id: &ObjectId) -> bool {
        self.node.env().is_unsynced(object_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;

    // Mock ClientEnv for testing
    struct MockClientEnv;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl ClientEnv for MockClientEnv {
        async fn subscribe(
            &self,
            _request: SubscribeRequest,
        ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
            // Return empty stream for testing
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn push(&self, _request: PushRequest) -> Result<PushResponse, ClientError> {
            Err(ClientError::new(501, "Not implemented in mock"))
        }

        async fn reconcile(&self, _request: ReconcileRequest) -> Result<SseEvent, ClientError> {
            Err(ClientError::new(501, "Not implemented in mock"))
        }

        async fn unsubscribe(&self, _subscription_id: u32) -> Result<(), ClientError> {
            Ok(())
        }
    }

    fn make_client() -> SyncClient<MockClientEnv> {
        let node = Arc::new(LocalNode::in_memory());
        SyncClient::new(MockClientEnv, node)
    }

    #[test]
    fn test_create_subscription() {
        let mut client = make_client();
        let id = client.create_subscription(
            "SELECT * FROM users".to_string(),
            SubscriptionOptions::default(),
        );
        assert_eq!(id, 1);

        let sub = client.subscriptions.get(&id).unwrap();
        assert_eq!(sub.query, "SELECT * FROM users");
        assert_eq!(sub.state, SubscriptionState::Pending);
    }

    #[test]
    fn test_subscription_lifecycle() {
        let mut client = make_client();
        let id = client.create_subscription(
            "SELECT * FROM users".to_string(),
            SubscriptionOptions::default(),
        );

        // Initially pending
        assert_eq!(
            client.subscriptions.get(&id).unwrap().state,
            SubscriptionState::Pending
        );

        // Mark active
        client.mark_subscription_active(id);
        assert_eq!(
            client.subscriptions.get(&id).unwrap().state,
            SubscriptionState::Active
        );

        // Remove
        let removed = client.remove_subscription(id);
        assert!(removed.is_some());
        assert!(client.subscriptions.get(&id).is_none());
    }

    #[test]
    fn test_server_known_state() {
        let mut client = make_client();
        let obj = ObjectId(42);
        let frontier = vec![CommitId::from_bytes([1u8; 32])];

        assert!(client.get_server_known_state(&obj).is_none());

        client.update_server_known_state(obj, frontier.clone());
        assert_eq!(client.get_server_known_state(&obj), Some(&frontier));
    }

    #[test]
    fn test_connection_state() {
        let mut client = make_client();

        assert_eq!(client.connection_state, ConnectionState::Disconnected);
        assert!(!client.is_connected());

        client.set_connection_state(ConnectionState::Connected);
        assert!(client.is_connected());
    }

    #[test]
    fn test_handle_push_response() {
        let mut client = make_client();
        let obj = ObjectId(42);
        let frontier = vec![CommitId::from_bytes([1u8; 32])];

        let response = PushResponse {
            object_id: obj,
            accepted: true,
            frontier: frontier.clone(),
        };

        client.handle_push_response(&response);

        // Should update server known state
        assert_eq!(client.get_server_known_state(&obj), Some(&frontier));
    }

    #[test]
    fn test_handle_push_response_rejected() {
        let mut client = make_client();
        let obj = ObjectId(42);

        let response = PushResponse {
            object_id: obj,
            accepted: false,
            frontier: vec![],
        };

        client.handle_push_response(&response);

        // Should not update server known state on rejection
        assert!(client.get_server_known_state(&obj).is_none());
    }
}
