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

// ==================== API Key Authentication ====================

/// Scopes that can be granted to an API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKeyScope {
    /// Deploy schemas to development environment
    SchemaDeployDev,
    /// Deploy schemas to staging environment
    SchemaDeployStagina,
    /// Deploy schemas to production environment
    SchemaDeployProd,
    /// Read schema/descriptor information
    SchemaRead,
    /// Full admin access
    Admin,
}

impl ApiKeyScope {
    /// Parse scope from string (e.g., "schema:deploy:dev").
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "schema:deploy:dev" => Some(ApiKeyScope::SchemaDeployDev),
            "schema:deploy:staging" => Some(ApiKeyScope::SchemaDeployStagina),
            "schema:deploy:prod" => Some(ApiKeyScope::SchemaDeployProd),
            "schema:read" => Some(ApiKeyScope::SchemaRead),
            "admin" => Some(ApiKeyScope::Admin),
            _ => None,
        }
    }

    /// Get the string representation of the scope.
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiKeyScope::SchemaDeployDev => "schema:deploy:dev",
            ApiKeyScope::SchemaDeployStagina => "schema:deploy:staging",
            ApiKeyScope::SchemaDeployProd => "schema:deploy:prod",
            ApiKeyScope::SchemaRead => "schema:read",
            ApiKeyScope::Admin => "admin",
        }
    }
}

/// Validated API key identity with scopes.
#[derive(Debug, Clone)]
pub struct ApiKeyIdentity {
    /// The API key ID
    pub key_id: String,
    /// Description of the key (for logging)
    pub description: Option<String>,
    /// Granted scopes
    pub scopes: HashSet<ApiKeyScope>,
}

impl ApiKeyIdentity {
    /// Check if this key has a specific scope.
    pub fn has_scope(&self, scope: ApiKeyScope) -> bool {
        self.scopes.contains(&ApiKeyScope::Admin) || self.scopes.contains(&scope)
    }

    /// Check if this key can deploy to an environment.
    pub fn can_deploy_to(&self, env: &str) -> bool {
        if self.scopes.contains(&ApiKeyScope::Admin) {
            return true;
        }
        match env {
            "dev" | "development" => self.scopes.contains(&ApiKeyScope::SchemaDeployDev),
            "staging" => self.scopes.contains(&ApiKeyScope::SchemaDeployStagina),
            "prod" | "production" => self.scopes.contains(&ApiKeyScope::SchemaDeployProd),
            _ => false,
        }
    }
}

/// Trait for validating API keys.
pub trait ApiKeyValidator: Send + Sync {
    /// Validate an API key and return its identity with scopes.
    fn validate_api_key(&self, key: &str) -> Option<ApiKeyIdentity>;
}

/// Simple in-memory API key validator (for testing/development).
pub struct InMemoryApiKeyValidator {
    keys: HashMap<String, ApiKeyIdentity>,
}

impl InMemoryApiKeyValidator {
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Add an API key with given scopes.
    pub fn add_key(
        &mut self,
        key: String,
        key_id: String,
        description: Option<String>,
        scopes: Vec<ApiKeyScope>,
    ) {
        self.keys.insert(
            key,
            ApiKeyIdentity {
                key_id,
                description,
                scopes: scopes.into_iter().collect(),
            },
        );
    }
}

impl Default for InMemoryApiKeyValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiKeyValidator for InMemoryApiKeyValidator {
    fn validate_api_key(&self, key: &str) -> Option<ApiKeyIdentity> {
        self.keys.get(key).cloned()
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
        self.object_queries
            .get(object_id)
            .cloned()
            .unwrap_or_default()
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
    /// Object metadata cache (for sending to new subscribers)
    pub object_metadata: HashMap<ObjectId, std::collections::BTreeMap<String, String>>,
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
            object_metadata: HashMap::new(),
            next_session_id: 1,
        }
    }

    /// Store object metadata for later retrieval by new subscribers.
    pub fn store_object_meta(
        &mut self,
        object_id: ObjectId,
        meta: std::collections::BTreeMap<String, String>,
    ) {
        self.object_metadata.insert(object_id, meta);
    }

    /// Get cached object metadata (for sending to new subscribers).
    pub fn get_object_meta(
        &self,
        object_id: &ObjectId,
    ) -> Option<std::collections::BTreeMap<String, String>> {
        self.object_metadata.get(object_id).cloned()
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

        self.sessions
            .insert(id, ClientSession::new(identity, sse_sender));
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
        self.identity_sessions
            .get(identity_id)
            .cloned()
            .unwrap_or_default()
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
        self.object_sessions
            .get(object_id)
            .cloned()
            .unwrap_or_default()
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
        object_meta: Option<std::collections::BTreeMap<String, String>>,
        exclude_session: Option<SessionId>,
    ) {
        let sessions = self.sessions_for_object(&object_id);
        let event = SseEvent::Commits {
            object_id,
            commits,
            frontier,
            object_meta,
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

    /// Broadcast commits to ALL active sessions (except the sender).
    /// This is a simplified version for MVP that doesn't require query matching.
    pub async fn broadcast_commits_to_all(
        &self,
        object_id: ObjectId,
        commits: Vec<crate::commit::Commit>,
        frontier: Vec<CommitId>,
        object_meta: Option<std::collections::BTreeMap<String, String>>,
        exclude_session: Option<SessionId>,
    ) {
        let event = SseEvent::Commits {
            object_id,
            commits,
            frontier,
            object_meta,
        };

        for (session_id, session) in &self.sessions {
            // Skip the sender session
            if Some(*session_id) == exclude_session {
                continue;
            }

            // Ignore send errors (client may have disconnected)
            let _ = session.sse_sender.send(event.clone()).await;
        }
    }
}

// ==================== Schema Registry ====================

use crate::sql::{
    Catalog, CatalogError, DescriptorId, Lens, LensGenerationOptions, TableDescriptor, TableSchema,
    diff_schemas, generate_lens,
};

/// Error during schema registry operations.
#[derive(Debug, Clone)]
pub enum SchemaRegistryError {
    /// Table not found in catalog
    TableNotFound(String),
    /// Descriptor not found in store
    DescriptorNotFound(DescriptorId),
    /// Unauthorized - API key doesn't have required scope
    Unauthorized(String),
    /// Environment not configured
    InvalidEnvironment(String),
    /// Catalog error
    CatalogError(CatalogError),
    /// Storage error
    StorageError(String),
}

impl std::fmt::Display for SchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaRegistryError::TableNotFound(name) => write!(f, "table not found: {}", name),
            SchemaRegistryError::DescriptorNotFound(id) => {
                write!(f, "descriptor not found: {}", id)
            }
            SchemaRegistryError::Unauthorized(msg) => write!(f, "unauthorized: {}", msg),
            SchemaRegistryError::InvalidEnvironment(env) => {
                write!(f, "invalid environment: {}", env)
            }
            SchemaRegistryError::CatalogError(e) => write!(f, "catalog error: {}", e),
            SchemaRegistryError::StorageError(msg) => write!(f, "storage error: {}", msg),
        }
    }
}

impl std::error::Error for SchemaRegistryError {}

impl From<CatalogError> for SchemaRegistryError {
    fn from(e: CatalogError) -> Self {
        SchemaRegistryError::CatalogError(e)
    }
}

/// Result of deploying a schema.
#[derive(Debug)]
pub struct SchemaDeployResult {
    /// The new descriptor ID
    pub descriptor_id: DescriptorId,
    /// The generated lens (if migrating from a parent)
    pub lens: Option<Lens>,
    /// Warnings from lens generation
    pub warnings: Vec<String>,
}

/// Schema registry for managing table schemas and descriptors.
///
/// The registry:
/// - Stores descriptors content-addressed by their ID
/// - Tracks schema history (DAG of descriptors)
/// - Manages the current schema version per table
/// - Validates API key permissions for schema deployment
///
/// TODO(GCO-1089): The SchemaRegistry provides the data structures, but actual HTTP endpoints
/// are not implemented. Need to add: GET /api/schema/:table, POST /api/schema/:table/deploy,
/// and integrate with an HTTP framework (e.g., axum) in groove-server.
pub struct SchemaRegistry {
    /// Current catalog (table name → descriptor ID)
    catalog: Catalog,
    /// Descriptor store: ID → serialized descriptor
    descriptors: HashMap<DescriptorId, TableDescriptor>,
    /// Schema history: table → list of descriptor IDs (chronological)
    schema_history: HashMap<String, Vec<DescriptorId>>,
    /// API key validator
    api_key_validator: Option<Arc<dyn ApiKeyValidator>>,
}

impl SchemaRegistry {
    /// Create a new schema registry.
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            descriptors: HashMap::new(),
            schema_history: HashMap::new(),
            api_key_validator: None,
        }
    }

    /// Create a schema registry with API key validation.
    pub fn with_api_key_validator(validator: Arc<dyn ApiKeyValidator>) -> Self {
        Self {
            catalog: Catalog::new(),
            descriptors: HashMap::new(),
            schema_history: HashMap::new(),
            api_key_validator: Some(validator),
        }
    }

    /// Set the API key validator.
    pub fn set_api_key_validator(&mut self, validator: Arc<dyn ApiKeyValidator>) {
        self.api_key_validator = Some(validator);
    }

    /// Validate an API key and check it has permission to deploy to the given environment.
    pub fn validate_deploy_permission(
        &self,
        api_key: &str,
        environment: &str,
    ) -> Result<ApiKeyIdentity, SchemaRegistryError> {
        let validator = self.api_key_validator.as_ref().ok_or_else(|| {
            SchemaRegistryError::Unauthorized("no API key validator configured".to_string())
        })?;

        let identity = validator
            .validate_api_key(api_key)
            .ok_or_else(|| SchemaRegistryError::Unauthorized("invalid API key".to_string()))?;

        if !identity.can_deploy_to(environment) {
            return Err(SchemaRegistryError::Unauthorized(format!(
                "API key does not have permission to deploy to {}",
                environment
            )));
        }

        Ok(identity)
    }

    /// Register a new table with its initial descriptor.
    ///
    /// This is used when creating a new table (no parent schema).
    pub fn register_table(&mut self, table_name: String, descriptor: TableDescriptor) {
        let id = DescriptorId::new();
        self.descriptors.insert(id, descriptor);
        self.catalog.tables.insert(table_name.clone(), id);
        self.schema_history.entry(table_name).or_default().push(id);
    }

    /// Deploy a new schema version for a table.
    ///
    /// This creates a new descriptor with the old schema as parent,
    /// generates a lens for the migration, and updates the catalog.
    pub fn deploy_schema(
        &mut self,
        table_name: &str,
        new_schema: TableSchema,
        options: LensGenerationOptions,
        api_key: Option<&str>,
        environment: Option<&str>,
    ) -> Result<SchemaDeployResult, SchemaRegistryError> {
        // Validate API key if provided
        if let (Some(key), Some(env)) = (api_key, environment) {
            self.validate_deploy_permission(key, env)?;
        }

        // Get current descriptor
        let current_id = self
            .catalog
            .get_descriptor_id(table_name)
            .ok_or_else(|| SchemaRegistryError::TableNotFound(table_name.to_string()))?;

        let current_descriptor = self
            .descriptors
            .get(&current_id)
            .ok_or(SchemaRegistryError::DescriptorNotFound(current_id))?;

        // Generate lens from schema diff
        let diff = diff_schemas(&current_descriptor.schema, &new_schema);
        let result = generate_lens(&diff, &options);
        let warnings: Vec<String> = result.warnings.iter().map(|w| w.message.clone()).collect();

        // Create new descriptor
        let new_descriptor = TableDescriptor {
            schema: new_schema,
            policies: current_descriptor.policies.clone(),
            parent_descriptors: vec![current_id],
            lenses: vec![result.lens.clone()],
            rows_object_id: current_descriptor.rows_object_id,
            schema_object_id: current_descriptor.schema_object_id,
            index_object_ids: current_descriptor.index_object_ids.clone(),
        };

        let new_id = DescriptorId::new();

        // Store new descriptor
        self.descriptors.insert(new_id, new_descriptor);

        // Update catalog
        self.catalog.tables.insert(table_name.to_string(), new_id);

        // Update history
        self.schema_history
            .entry(table_name.to_string())
            .or_default()
            .push(new_id);

        Ok(SchemaDeployResult {
            descriptor_id: new_id,
            lens: Some(result.lens),
            warnings,
        })
    }

    /// Get the current descriptor ID for a table.
    pub fn get_current_descriptor_id(&self, table_name: &str) -> Option<DescriptorId> {
        self.catalog.get_descriptor_id(table_name)
    }

    /// Get a descriptor by ID.
    pub fn get_descriptor(&self, id: &DescriptorId) -> Option<&TableDescriptor> {
        self.descriptors.get(id)
    }

    /// Get the schema history for a table (list of descriptor IDs).
    pub fn get_schema_history(&self, table_name: &str) -> Option<&[DescriptorId]> {
        self.schema_history.get(table_name).map(|v| v.as_slice())
    }

    /// List all tables in the registry.
    pub fn list_tables(&self) -> Vec<&String> {
        self.catalog.tables.keys().collect()
    }

    /// Get the current catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Serialize the registry state for persistence.
    ///
    /// Format:
    /// - Catalog bytes (length-prefixed)
    /// - Number of descriptors (u32)
    /// - For each descriptor:
    ///   - DescriptorId (16 bytes - ObjectId)
    ///   - Descriptor bytes (length-prefixed)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Catalog
        let catalog_bytes = self.catalog.to_bytes();
        buf.extend_from_slice(&(catalog_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&catalog_bytes);

        // Descriptors
        buf.extend_from_slice(&(self.descriptors.len() as u32).to_le_bytes());
        for (id, descriptor) in &self.descriptors {
            buf.extend_from_slice(&u128::from(id.as_object_id()).to_le_bytes());
            let desc_bytes = descriptor.to_bytes();
            buf.extend_from_slice(&(desc_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&desc_bytes);
        }

        // Schema history
        buf.extend_from_slice(&(self.schema_history.len() as u32).to_le_bytes());
        for (table_name, history) in &self.schema_history {
            let name_bytes = table_name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(history.len() as u32).to_le_bytes());
            for id in history {
                buf.extend_from_slice(&u128::from(id.as_object_id()).to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize the registry state from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SchemaRegistryError> {
        let mut pos = 0;

        // Catalog length
        if data.len() < pos + 4 {
            return Err(SchemaRegistryError::StorageError(
                "unexpected EOF".to_string(),
            ));
        }
        let catalog_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Catalog bytes
        if data.len() < pos + catalog_len {
            return Err(SchemaRegistryError::StorageError(
                "unexpected EOF".to_string(),
            ));
        }
        let catalog = Catalog::from_bytes(&data[pos..pos + catalog_len])?;
        pos += catalog_len;

        // Number of descriptors
        if data.len() < pos + 4 {
            return Err(SchemaRegistryError::StorageError(
                "unexpected EOF".to_string(),
            ));
        }
        let num_descriptors = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut descriptors = HashMap::with_capacity(num_descriptors);
        for _ in 0..num_descriptors {
            // DescriptorId (16 bytes - ObjectId)
            if data.len() < pos + 16 {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let object_id = ObjectId::new(u128::from_le_bytes(id_bytes));
            let id = DescriptorId::from_object_id(object_id);
            pos += 16;

            // Descriptor length
            if data.len() < pos + 4 {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let desc_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Descriptor bytes
            if data.len() < pos + desc_len {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let descriptor = TableDescriptor::from_bytes(&data[pos..pos + desc_len])?;
            pos += desc_len;

            descriptors.insert(id, descriptor);
        }

        // Schema history
        if data.len() < pos + 4 {
            return Err(SchemaRegistryError::StorageError(
                "unexpected EOF".to_string(),
            ));
        }
        let num_tables = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut schema_history = HashMap::with_capacity(num_tables);
        for _ in 0..num_tables {
            // Table name length
            if data.len() < pos + 4 {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Table name
            if data.len() < pos + name_len {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let table_name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|_| SchemaRegistryError::StorageError("invalid UTF-8".to_string()))?;
            pos += name_len;

            // History length
            if data.len() < pos + 4 {
                return Err(SchemaRegistryError::StorageError(
                    "unexpected EOF".to_string(),
                ));
            }
            let history_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut history = Vec::with_capacity(history_len);
            for _ in 0..history_len {
                if data.len() < pos + 16 {
                    return Err(SchemaRegistryError::StorageError(
                        "unexpected EOF".to_string(),
                    ));
                }
                let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
                let object_id = ObjectId::new(u128::from_le_bytes(id_bytes));
                history.push(DescriptorId::from_object_id(object_id));
                pos += 16;
            }

            schema_history.insert(table_name, history);
        }

        Ok(Self {
            catalog,
            descriptors,
            schema_history,
            api_key_validator: None,
        })
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
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

        let s1 = server.create_session(
            ClientIdentity {
                id: "u1".to_string(),
                name: None,
            },
            tx1,
        );
        let s2 = server.create_session(
            ClientIdentity {
                id: "u2".to_string(),
                name: None,
            },
            tx2,
        );

        let obj = ObjectId(42);
        server.register_object_session(obj, s1);
        server.register_object_session(obj, s2);

        let event = SseEvent::Excluded { object_id: obj };
        server.broadcast_to_object(&obj, event).await;

        // Both receivers should get the event
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    // ==================== API Key Tests ====================

    #[test]
    fn test_api_key_scope_parsing() {
        assert_eq!(
            ApiKeyScope::parse("schema:deploy:dev"),
            Some(ApiKeyScope::SchemaDeployDev)
        );
        assert_eq!(
            ApiKeyScope::parse("schema:deploy:staging"),
            Some(ApiKeyScope::SchemaDeployStagina)
        );
        assert_eq!(
            ApiKeyScope::parse("schema:deploy:prod"),
            Some(ApiKeyScope::SchemaDeployProd)
        );
        assert_eq!(ApiKeyScope::parse("admin"), Some(ApiKeyScope::Admin));
        assert_eq!(ApiKeyScope::parse("invalid"), None);
    }

    #[test]
    fn test_api_key_identity_can_deploy() {
        let dev_identity = ApiKeyIdentity {
            key_id: "key1".to_string(),
            description: None,
            scopes: [ApiKeyScope::SchemaDeployDev].into_iter().collect(),
        };

        assert!(dev_identity.can_deploy_to("dev"));
        assert!(dev_identity.can_deploy_to("development"));
        assert!(!dev_identity.can_deploy_to("staging"));
        assert!(!dev_identity.can_deploy_to("prod"));

        let admin_identity = ApiKeyIdentity {
            key_id: "admin".to_string(),
            description: None,
            scopes: [ApiKeyScope::Admin].into_iter().collect(),
        };

        assert!(admin_identity.can_deploy_to("dev"));
        assert!(admin_identity.can_deploy_to("staging"));
        assert!(admin_identity.can_deploy_to("prod"));
    }

    #[test]
    fn test_in_memory_api_key_validator() {
        let mut validator = InMemoryApiKeyValidator::new();
        validator.add_key(
            "test-key-123".to_string(),
            "key1".to_string(),
            Some("Test key".to_string()),
            vec![ApiKeyScope::SchemaDeployDev, ApiKeyScope::SchemaRead],
        );

        let identity = validator.validate_api_key("test-key-123").unwrap();
        assert_eq!(identity.key_id, "key1");
        assert!(identity.has_scope(ApiKeyScope::SchemaDeployDev));
        assert!(identity.has_scope(ApiKeyScope::SchemaRead));
        assert!(!identity.has_scope(ApiKeyScope::SchemaDeployProd));

        assert!(validator.validate_api_key("invalid-key").is_none());
    }

    // ==================== Schema Registry Tests ====================

    #[test]
    fn test_schema_registry_register_table() {
        use crate::sql::{ColumnDef, ColumnType, TablePolicies};

        let mut registry = SchemaRegistry::new();

        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            }],
        };

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            parent_descriptors: vec![],
            lenses: vec![],
            rows_object_id: ObjectId::new(1),
            schema_object_id: ObjectId::new(2),
            index_object_ids: HashMap::new(),
        };

        registry.register_table("users".to_string(), descriptor);

        assert!(registry.get_current_descriptor_id("users").is_some());
        assert!(registry.get_current_descriptor_id("nonexistent").is_none());
    }

    #[test]
    fn test_schema_registry_deploy_requires_auth() {
        use crate::sql::{ColumnDef, ColumnType, TablePolicies};

        let mut validator = InMemoryApiKeyValidator::new();
        validator.add_key(
            "dev-key".to_string(),
            "key1".to_string(),
            None,
            vec![ApiKeyScope::SchemaDeployDev],
        );

        let mut registry = SchemaRegistry::with_api_key_validator(Arc::new(validator));

        // Register initial table
        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "name".to_string(),
                ty: ColumnType::String,
                nullable: false,
            }],
        };

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            parent_descriptors: vec![],
            lenses: vec![],
            rows_object_id: ObjectId::new(1),
            schema_object_id: ObjectId::new(2),
            index_object_ids: HashMap::new(),
        };

        registry.register_table("users".to_string(), descriptor);

        // Try to deploy to prod with dev key - should fail
        let new_schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "full_name".to_string(),
                ty: ColumnType::String,
                nullable: false,
            }],
        };

        let result = registry.deploy_schema(
            "users",
            new_schema.clone(),
            LensGenerationOptions::default(),
            Some("dev-key"),
            Some("prod"),
        );

        assert!(result.is_err());
        match result {
            Err(SchemaRegistryError::Unauthorized(_)) => {}
            _ => panic!("expected Unauthorized error"),
        }

        // Deploy to dev with dev key - should succeed
        let result = registry.deploy_schema(
            "users",
            new_schema,
            LensGenerationOptions::default(),
            Some("dev-key"),
            Some("dev"),
        );

        assert!(result.is_ok());
        let deploy_result = result.unwrap();
        assert!(deploy_result.lens.is_some());
    }

    #[test]
    fn test_schema_registry_roundtrip() {
        use crate::sql::{ColumnDef, ColumnType, TablePolicies};

        let mut registry = SchemaRegistry::new();

        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            }],
        };

        let descriptor = TableDescriptor {
            schema,
            policies: TablePolicies::default(),
            parent_descriptors: vec![],
            lenses: vec![],
            rows_object_id: ObjectId::new(1),
            schema_object_id: ObjectId::new(2),
            index_object_ids: HashMap::new(),
        };

        registry.register_table("users".to_string(), descriptor);

        // Serialize and deserialize
        let bytes = registry.to_bytes();
        let restored = SchemaRegistry::from_bytes(&bytes).unwrap();

        assert_eq!(restored.list_tables().len(), 1);
        assert!(restored.get_current_descriptor_id("users").is_some());
    }
}
