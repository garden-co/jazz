//! Sync server implementation.
//!
//! The server handles:
//! - HTTP endpoints for subscribe, push, reconcile, unsubscribe
//! - SSE streams for real-time updates to clients
//! - Session management with query subscriptions
//! - Multi-query reference counting for objects
//!
//! This module is only available with the `sync-server` feature.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::storage::Environment;

use super::protocol::{SseEvent, SubscriptionOptions};

/// Unique identifier for a client session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// Unique identifier for a query subscription within a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub u32);

/// Identity of an authenticated client.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    /// Unique identifier for the client/user
    pub id: String,
    /// Optional display name
    pub name: Option<String>,
}

/// Trait for validating authentication tokens.
pub trait TokenValidator: Send + Sync {
    /// Validate a bearer token and return the client identity if valid.
    fn validate(&self, token: &str) -> Option<ClientIdentity>;
}

/// A simple token validator that accepts any token (for testing).
pub struct AcceptAllTokens;

impl TokenValidator for AcceptAllTokens {
    fn validate(&self, token: &str) -> Option<ClientIdentity> {
        Some(ClientIdentity {
            id: token.to_string(),
            name: None,
        })
    }
}

/// Channel for sending SSE events to a client.
pub type SseSender = tokio::sync::mpsc::Sender<SseEvent>;

/// State for a single client session.
#[derive(Debug)]
pub struct ClientSession {
    /// Client identity from authentication
    pub identity: ClientIdentity,
    /// Channel to send SSE events to this client
    pub sse_sender: SseSender,
    /// Assumed known state: what commits the client has per object
    pub client_known_state: HashMap<ObjectId, Vec<CommitId>>,
    /// Multi-query reference counting: which queries need each object
    pub object_queries: HashMap<ObjectId, HashSet<QueryId>>,
    /// Active query subscriptions
    pub queries: HashMap<QueryId, ActiveQuery>,
    /// Next query ID
    next_query_id: u32,
    /// Last activity timestamp for timeout detection.
    pub last_activity: Instant,
}

impl ClientSession {
    /// Create a new client session.
    pub fn new(identity: ClientIdentity, sse_sender: SseSender) -> Self {
        Self {
            identity,
            sse_sender,
            client_known_state: HashMap::new(),
            object_queries: HashMap::new(),
            queries: HashMap::new(),
            next_query_id: 1,
            last_activity: Instant::now(),
        }
    }

    /// Update the last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Allocate a new query ID.
    pub fn next_query_id(&mut self) -> QueryId {
        let id = QueryId(self.next_query_id);
        self.next_query_id += 1;
        id
    }

    /// Add an object to a query's sync set.
    pub fn add_object_to_query(&mut self, object_id: ObjectId, query_id: QueryId) {
        self.object_queries
            .entry(object_id)
            .or_default()
            .insert(query_id);
    }

    /// Remove an object from a query's sync set.
    /// Returns true if the object is no longer needed by any query.
    pub fn remove_object_from_query(&mut self, object_id: ObjectId, query_id: QueryId) -> bool {
        if let Some(queries) = self.object_queries.get_mut(&object_id) {
            queries.remove(&query_id);
            if queries.is_empty() {
                self.object_queries.remove(&object_id);
                return true;
            }
        }
        false
    }

    /// Check if an object is needed by any query.
    pub fn is_object_needed(&self, object_id: &ObjectId) -> bool {
        self.object_queries.contains_key(object_id)
    }

    /// Get all queries that need an object.
    pub fn queries_needing_object(&self, object_id: &ObjectId) -> HashSet<QueryId> {
        self.object_queries.get(object_id).cloned().unwrap_or_default()
    }
}

/// An active query subscription.
#[derive(Debug)]
pub struct ActiveQuery {
    /// The SQL query string
    pub query: String,
    /// Subscription options
    pub options: SubscriptionOptions,
    /// Objects currently matching this query
    pub matching_objects: HashSet<ObjectId>,
}

impl ActiveQuery {
    /// Create a new active query.
    pub fn new(query: String, options: SubscriptionOptions) -> Self {
        Self {
            query,
            options,
            matching_objects: HashSet::new(),
        }
    }
}

/// The sync server.
///
/// Manages sessions, query subscriptions, and object sync state.
pub struct SyncServer<E: Environment> {
    /// Storage environment
    pub env: Arc<E>,
    /// Token validator for authentication
    pub token_validator: Arc<dyn TokenValidator>,
    /// Active client sessions
    pub sessions: HashMap<SessionId, ClientSession>,
    /// Reverse index: object -> sessions that have it
    pub object_sessions: HashMap<ObjectId, HashSet<SessionId>>,
    /// Reverse index: identity -> sessions (for finding session by auth)
    pub identity_sessions: HashMap<String, HashSet<SessionId>>,
    /// Next session ID
    next_session_id: u64,
}

impl<E: Environment> SyncServer<E> {
    /// Create a new sync server.
    pub fn new(env: Arc<E>, token_validator: Arc<dyn TokenValidator>) -> Self {
        Self {
            env,
            token_validator,
            sessions: HashMap::new(),
            object_sessions: HashMap::new(),
            identity_sessions: HashMap::new(),
            next_session_id: 1,
        }
    }

    /// Create a new client session.
    pub fn create_session(&mut self, identity: ClientIdentity, sse_sender: SseSender) -> SessionId {
        let id = SessionId(self.next_session_id);
        self.next_session_id += 1;

        // Track identity -> session mapping
        self.identity_sessions
            .entry(identity.id.clone())
            .or_default()
            .insert(id);

        self.sessions.insert(id, ClientSession::new(identity, sse_sender));
        id
    }

    /// Remove a client session and clean up subscriptions.
    pub fn remove_session(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.remove(&session_id) {
            // Clean up object_sessions reverse index
            for object_id in session.object_queries.keys() {
                if let Some(sessions) = self.object_sessions.get_mut(object_id) {
                    sessions.remove(&session_id);
                    if sessions.is_empty() {
                        self.object_sessions.remove(object_id);
                    }
                }
            }

            // Clean up identity_sessions reverse index
            if let Some(sessions) = self.identity_sessions.get_mut(&session.identity.id) {
                sessions.remove(&session_id);
                if sessions.is_empty() {
                    self.identity_sessions.remove(&session.identity.id);
                }
            }
        }
    }

    /// Get sessions for an identity.
    pub fn sessions_for_identity(&self, identity_id: &str) -> HashSet<SessionId> {
        self.identity_sessions.get(identity_id).cloned().unwrap_or_default()
    }

    /// Get a session by ID.
    pub fn get_session(&self, session_id: &SessionId) -> Option<&ClientSession> {
        self.sessions.get(session_id)
    }

    /// Get a mutable session by ID.
    pub fn get_session_mut(&mut self, session_id: &SessionId) -> Option<&mut ClientSession> {
        self.sessions.get_mut(session_id)
    }

    /// Register that a session is tracking an object.
    pub fn register_object_session(&mut self, object_id: ObjectId, session_id: SessionId) {
        self.object_sessions
            .entry(object_id)
            .or_default()
            .insert(session_id);
    }

    /// Unregister that a session is tracking an object.
    pub fn unregister_object_session(&mut self, object_id: &ObjectId, session_id: &SessionId) {
        if let Some(sessions) = self.object_sessions.get_mut(object_id) {
            sessions.remove(session_id);
            if sessions.is_empty() {
                self.object_sessions.remove(object_id);
            }
        }
    }

    /// Get all sessions tracking an object.
    pub fn sessions_for_object(&self, object_id: &ObjectId) -> HashSet<SessionId> {
        self.object_sessions.get(object_id).cloned().unwrap_or_default()
    }

    /// Broadcast an event to all sessions tracking an object.
    pub async fn broadcast_to_object(&self, object_id: &ObjectId, event: SseEvent) {
        let sessions = self.sessions_for_object(object_id);
        for session_id in sessions {
            if let Some(session) = self.sessions.get(&session_id) {
                // Ignore send errors (client may have disconnected)
                let _ = session.sse_sender.send(event.clone()).await;
            }
        }
    }

    /// Update client's known state for an object.
    pub fn update_client_known_state(
        &mut self,
        session_id: &SessionId,
        object_id: ObjectId,
        frontier: Vec<CommitId>,
    ) {
        if let Some(session) = self.sessions.get_mut(session_id) {
            session.client_known_state.insert(object_id, frontier);
        }
    }

    /// Get client's assumed known state for an object.
    pub fn get_client_known_state(
        &self,
        session_id: &SessionId,
        object_id: &ObjectId,
    ) -> Option<&Vec<CommitId>> {
        self.sessions
            .get(session_id)
            .and_then(|s| s.client_known_state.get(object_id))
    }

    /// Store commits for an object and update the frontier.
    ///
    /// Returns the new frontier after applying commits.
    pub async fn store_commits(
        &self,
        object_id: ObjectId,
        commits: &[crate::commit::Commit],
        branch: &str,
    ) -> Vec<CommitId> {
        // Store each commit
        let mut commit_ids = Vec::new();
        for commit in commits {
            let id = self.env.put_commit(commit).await;
            commit_ids.push(id);
        }

        // Get current frontier
        let mut frontier = self.env.get_frontier(object_id.0, branch).await;

        // Update frontier: remove parents of new commits, add new tips
        let parent_set: std::collections::HashSet<CommitId> = commits
            .iter()
            .flat_map(|c| c.parents.iter().copied())
            .collect();

        frontier.retain(|id| !parent_set.contains(id));

        // Add commits that are not parents of any other new commit
        for &id in &commit_ids {
            // Only add if this commit is not a parent of another new commit
            let is_parent = commits.iter().any(|other| other.parents.contains(&id));
            if !is_parent && !frontier.contains(&id) {
                frontier.push(id);
            }
        }

        // Deduplicate frontier
        let frontier: Vec<CommitId> = frontier
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Save updated frontier
        self.env.set_frontier(object_id.0, branch, &frontier).await;

        frontier
    }

    /// Broadcast commits to all sessions tracking an object (except the sender).
    pub async fn broadcast_commits(
        &self,
        object_id: ObjectId,
        commits: Vec<crate::commit::Commit>,
        frontier: Vec<CommitId>,
        exclude_session: Option<SessionId>,
    ) {
        let sessions = self.sessions_for_object(&object_id);
        let event = SseEvent::Commits {
            object_id,
            commits,
            frontier,
        };

        for session_id in sessions {
            // Skip the sender session
            if Some(session_id) == exclude_session {
                continue;
            }

            if let Some(session) = self.sessions.get(&session_id) {
                // Ignore send errors (client may have disconnected)
                let _ = session.sse_sender.send(event.clone()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryEnvironment;

    fn make_server() -> SyncServer<MemoryEnvironment> {
        let env = Arc::new(MemoryEnvironment::new());
        let validator = Arc::new(AcceptAllTokens);
        SyncServer::new(env, validator)
    }

    #[tokio::test]
    async fn test_create_session() {
        let mut server = make_server();
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let identity = ClientIdentity {
            id: "user1".to_string(),
            name: Some("User One".to_string()),
        };

        let session_id = server.create_session(identity, tx);
        assert!(server.get_session(&session_id).is_some());
    }

    #[tokio::test]
    async fn test_remove_session() {
        let mut server = make_server();
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let identity = ClientIdentity {
            id: "user1".to_string(),
            name: None,
        };

        let session_id = server.create_session(identity, tx);
        server.remove_session(session_id);
        assert!(server.get_session(&session_id).is_none());
    }

    #[tokio::test]
    async fn test_object_query_reference_counting() {
        let mut server = make_server();
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let identity = ClientIdentity {
            id: "user1".to_string(),
            name: None,
        };

        let session_id = server.create_session(identity, tx);
        let session = server.get_session_mut(&session_id).unwrap();

        let obj = ObjectId(42);
        let q1 = QueryId(1);
        let q2 = QueryId(2);

        // Add object to two queries
        session.add_object_to_query(obj, q1);
        session.add_object_to_query(obj, q2);
        assert!(session.is_object_needed(&obj));
        assert_eq!(session.queries_needing_object(&obj).len(), 2);

        // Remove from one query - still needed
        let removed = session.remove_object_from_query(obj, q1);
        assert!(!removed);
        assert!(session.is_object_needed(&obj));

        // Remove from second query - no longer needed
        let removed = session.remove_object_from_query(obj, q2);
        assert!(removed);
        assert!(!session.is_object_needed(&obj));
    }

    #[tokio::test]
    async fn test_session_cleanup_on_remove() {
        let mut server = make_server();
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let identity = ClientIdentity {
            id: "user1".to_string(),
            name: None,
        };

        let session_id = server.create_session(identity, tx);
        let obj = ObjectId(42);

        // Register object-session mapping
        server.register_object_session(obj, session_id);
        {
            let session = server.get_session_mut(&session_id).unwrap();
            session.add_object_to_query(obj, QueryId(1));
        }

        assert!(server.sessions_for_object(&obj).contains(&session_id));

        // Remove session - should clean up object_sessions
        server.remove_session(session_id);
        assert!(server.sessions_for_object(&obj).is_empty());
    }

    #[tokio::test]
    async fn test_broadcast_to_object() {
        let mut server = make_server();
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(16);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(16);

        let s1 = server.create_session(ClientIdentity { id: "u1".to_string(), name: None }, tx1);
        let s2 = server.create_session(ClientIdentity { id: "u2".to_string(), name: None }, tx2);

        let obj = ObjectId(42);
        server.register_object_session(obj, s1);
        server.register_object_session(obj, s2);

        let event = SseEvent::Excluded { object_id: obj };
        server.broadcast_to_object(&obj, event).await;

        // Both receivers should get the event
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
