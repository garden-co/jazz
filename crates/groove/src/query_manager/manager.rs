use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::commit::{CommitId, StoredState};
use crate::metadata::{
    DeleteKind, MetadataKey, ObjectType, hard_delete_metadata, soft_delete_metadata,
};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::AllObjectUpdate;
use crate::schema_manager::{LensTransformer, SchemaContext};
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, PendingPermissionCheck, PendingUpdateId, PersistenceTier, QueryId, SyncManager,
};

use super::encoding::{decode_column, decode_row, encode_row};
use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::policy::{ComplexClause, Operation, evaluate_simple_parts, resolve_session_value};
use super::policy_graph::PolicyGraph;
use super::query::{Query, QueryBuilder};
use super::session::Session;
use super::types::{
    ComposedBranchName, Row, RowDelta, RowDescriptor, Schema, SchemaHash, TableName, TableSchema,
    Value,
};

/// Error types for QueryManager operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    TableNotFound(TableName),
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    EncodingError(String),
    ObjectNotFound(ObjectId),
    QueryCompilationError(String),
    IndexError(String),
    /// Cannot undelete or truncate a row that is not soft-deleted.
    RowNotDeleted(ObjectId),
    /// Cannot delete an already-deleted row.
    RowAlreadyDeleted(ObjectId),
    /// Cannot operate on a hard-deleted row (it no longer exists).
    RowHardDeleted(ObjectId),
    /// Policy denied the operation.
    PolicyDenied {
        table: TableName,
        operation: Operation,
    },
    /// Unknown schema hash - client should sync schema first.
    UnknownSchema(SchemaHash),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::TableNotFound(t) => write!(f, "table not found: {}", t),
            QueryError::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {expected}, got {actual}"
                )
            }
            QueryError::EncodingError(msg) => write!(f, "encoding error: {msg}"),
            QueryError::ObjectNotFound(id) => write!(f, "object not found: {:?}", id),
            QueryError::QueryCompilationError(msg) => write!(f, "query compilation error: {msg}"),
            QueryError::IndexError(msg) => write!(f, "index error: {msg}"),
            QueryError::RowNotDeleted(id) => write!(f, "row not deleted: {:?}", id),
            QueryError::RowAlreadyDeleted(id) => write!(f, "row already deleted: {:?}", id),
            QueryError::RowHardDeleted(id) => write!(f, "row hard deleted: {:?}", id),
            QueryError::PolicyDenied { table, operation } => {
                write!(f, "policy denied {} on table {}", operation, table)
            }
            QueryError::UnknownSchema(hash) => {
                write!(
                    f,
                    "unknown schema: {} - client should sync schema first",
                    hash.short()
                )
            }
        }
    }
}

impl std::error::Error for QueryError {}

/// Handle to a pending query.
///
/// Used to correlate query results with the original request.
/// Wrappers (groove-runtime, groove-wasm) use this to fulfill
/// platform-specific futures/promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryHandle(pub u64);

/// Handle for tracking insert completion.
///
/// Poll via `is_complete()` to check if the row is persisted.
/// Poll via `is_indexed()` to check if the row is indexed.
#[derive(Debug, Clone)]
pub struct InsertHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the row data.
    pub row_commit_id: CommitId,
}

/// Handle for tracking delete completion.
#[derive(Debug, Clone)]
pub struct DeleteHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the delete tombstone commit.
    pub delete_commit_id: CommitId,
}

impl InsertHandle {
    /// Check if the row data is durable (persisted to storage).
    ///
    /// Must call `QueryManager::process()` between checks to drive storage operations.
    pub fn is_complete(&self, qm: &QueryManager) -> bool {
        qm.is_commit_stored(self.row_id, &self.row_commit_id)
    }

    /// Check if the row is indexed (appears in the _id index).
    ///
    /// After insert + process(), the row should be indexed.
    pub fn is_indexed(&self, qm: &QueryManager, storage: &dyn Storage, table: &str) -> bool {
        qm.row_is_indexed(storage, table, self.row_id)
    }
}

/// Query subscription info.
#[derive(Debug)]
pub(crate) struct QuerySubscription {
    /// Original query for recompilation when schemas change.
    pub(crate) query: Query,
    /// Compiled query graph.
    pub(crate) graph: QueryGraph,
    #[allow(dead_code)]
    pub(crate) mode: SubscriptionMode,
    /// Branches to read from (updated on recompile).
    pub(crate) branches: Vec<String>,
    /// Session for policy filtering (if any).
    pub(crate) session: Option<Session>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    pub(crate) needs_recompile: bool,
    /// Flag indicating this subscription has settled at least once.
    /// Used to ensure one-shot queries receive an initial callback (even if empty).
    pub(crate) settled_once: bool,
    /// Required persistence tier before first delivery (None = immediate).
    pub(crate) settled_tier: Option<PersistenceTier>,
    /// Tiers that have confirmed settlement for this query.
    pub(crate) achieved_tiers: HashSet<PersistenceTier>,
}

/// Subscription mode (reserved for future use).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum SubscriptionMode {
    Delta,
    Full,
}

/// Update for a query subscription.
#[derive(Debug, Clone)]
pub struct QueryUpdate {
    pub subscription_id: QuerySubscriptionId,
    pub delta: RowDelta,
    /// Output descriptor for decoding the binary row data.
    /// This matches the query's output schema (handles JOINs, projections, etc).
    pub descriptor: RowDescriptor,
}

/// State for an active policy check (graphs and associated data).
#[derive(Debug)]
struct PolicyCheckState {
    /// Policy graphs that need to settle.
    graphs: Vec<PolicyGraph>,
    /// Table name for error messages.
    table: TableName,
    /// The original pending permission check.
    pending_check: PendingPermissionCheck,
}

/// Server-side query subscription state.
///
/// When a client sends a QuerySubscription, the server builds a QueryGraph
/// and tracks contributing ObjectIds. This struct holds that state.
#[derive(Debug)]
struct ServerQuerySubscription {
    /// The original query.
    query: Query,
    /// Compiled QueryGraph (with client's session for policy filtering).
    graph: QueryGraph,
    /// Client's session for permission evaluation.
    session: Option<Session>,
    /// Resolved branches (from query.branches or schema context at creation time).
    branches: Vec<String>,
    /// Last computed scope (for detecting changes).
    last_scope: HashSet<(ObjectId, BranchName)>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    needs_recompile: bool,
    /// Flag indicating this server subscription has settled at least once.
    /// Used to emit QuerySettled to the client on first settlement.
    settled_once: bool,
}

/// A catalogue object update received via sync.
///
/// Used to pass schema/lens updates from QueryManager to SchemaManager.
#[derive(Debug, Clone)]
pub struct CatalogueUpdate {
    /// The object ID of the catalogue object.
    pub object_id: ObjectId,
    /// Metadata from the object (includes type, app_id, etc.).
    pub metadata: HashMap<String, String>,
    /// Content from the latest commit.
    pub content: Vec<u8>,
}

/// Manages reactive SQL queries over object-based storage.
///
/// No global Setup/Ready state machine - indices and data are loaded lazily
/// from ObjectManager. Operations work immediately; queries return empty/Pending
/// results until data is available.
///
/// ObjectManager is the source of truth for row data - no caching layer on top.
pub struct QueryManager {
    sync_manager: SyncManager,
    schema: Arc<Schema>,

    /// Pending catalogue updates (schemas/lenses received via sync).
    /// SchemaManager should call take_pending_catalogue_updates() to process these.
    pending_catalogue_updates: Vec<CatalogueUpdate>,

    /// Active query subscriptions (local)
    subscriptions: HashMap<QuerySubscriptionId, QuerySubscription>,
    next_subscription_id: u64,

    /// Pending query updates
    update_outbox: Vec<QueryUpdate>,

    /// Active policy checks being evaluated.
    active_policy_checks: HashMap<PendingUpdateId, PolicyCheckState>,

    /// Server-side query subscriptions from downstream clients.
    /// Key is (client_id, query_id) to allow multiple queries per client.
    server_subscriptions: HashMap<(ClientId, QueryId), ServerQuerySubscription>,

    /// Schema context for multi-schema queries.
    /// Starts empty; initialized via set_current_schema().
    /// Enables lens transforms for rows from old schema branches.
    schema_context: SchemaContext,

    /// Maps branch name to schema hash (derived from schema_context).
    /// Used to determine which schema a branch uses.
    branch_schema_map: HashMap<String, SchemaHash>,

    /// Buffered row updates for unknown schema branches.
    /// These are retried when new schemas activate via try_activate_pending().
    pending_row_updates: Vec<AllObjectUpdate>,

    /// Known schemas (for server-mode operation).
    /// Synced from SchemaManager's known_schemas to enable lazy branch activation.
    /// When a row arrives with unknown branch, we parse the branch name to extract
    /// the short hash, then look up the full schema in this map.
    known_schemas: HashMap<SchemaHash, Schema>,
}

impl QueryManager {
    /// Create a new QueryManager with empty schema context.
    ///
    /// Call `set_current_schema()` to initialize the current schema before queries.
    /// Use `add_live_schema()` and `register_lens()` to add additional schemas.
    ///
    /// Row-level security is evaluated via `process()` which handles pending
    /// permission checks from SyncManager.
    pub fn new(mut sync_manager: SyncManager) -> Self {
        // Subscribe to all object updates so we receive sync'd data
        sync_manager.object_manager.subscribe_all();

        Self {
            sync_manager,
            schema: Arc::new(Schema::new()),
            pending_catalogue_updates: Vec::new(),
            subscriptions: HashMap::new(),
            next_subscription_id: 0,
            update_outbox: Vec::new(),
            active_policy_checks: HashMap::new(),
            server_subscriptions: HashMap::new(),
            schema_context: SchemaContext::empty(),
            branch_schema_map: HashMap::new(),
            pending_row_updates: Vec::new(),
            known_schemas: HashMap::new(),
        }
    }

    /// Set the current schema (the one this client writes to).
    ///
    /// Must be called before queries. Can only be called once.
    /// Creates indices for the current schema's branch.
    pub fn set_current_schema(&mut self, schema: Schema, env: &str, user_branch: &str) {
        self.schema_context
            .set_current(schema.clone(), env, user_branch);
        self.schema = Arc::new(schema.clone());

        // Update branch -> schema hash map
        let branch = self.schema_context.branch_name();
        self.branch_schema_map.insert(
            branch.as_str().to_string(),
            self.schema_context.current_hash,
        );
    }

    /// Add a live schema (one we can read from but don't write to).
    ///
    /// Creates indices for the schema's branch.
    /// Marks subscriptions for recompilation to include the new branch.
    pub fn add_live_schema(&mut self, schema: Schema) {
        let hash = SchemaHash::compute(&schema);

        // Skip if already live or is current
        if self.schema_context.is_live(&hash) {
            return;
        }

        // Build branch name for this schema
        let branch = ComposedBranchName::new(
            &self.schema_context.env,
            hash,
            &self.schema_context.user_branch,
        )
        .to_branch_name();

        // Add to live_schemas (without lens - caller should register lens separately)
        self.schema_context
            .live_schemas
            .insert(hash, schema.clone());

        // Update branch -> schema hash map
        self.branch_schema_map
            .insert(branch.as_str().to_string(), hash);

        // Mark subscriptions for recompile to pick up new branch
        self.mark_subscriptions_for_recompile();
    }

    /// Register a lens between two schemas.
    ///
    /// Also attempts to activate any pending schemas that may now be reachable.
    pub fn register_lens(&mut self, lens: super::super::schema_manager::lens::Lens) {
        self.schema_context.register_lens(lens);

        // Try to activate pending schemas
        let activated = self.schema_context.try_activate_pending();
        if !activated.is_empty() {
            // New schemas activated - register branches and mark for recompile
            for hash in activated {
                if let Some(_schema) = self.schema_context.live_schemas.get(&hash).cloned() {
                    let branch = ComposedBranchName::new(
                        &self.schema_context.env,
                        hash,
                        &self.schema_context.user_branch,
                    )
                    .to_branch_name();

                    self.branch_schema_map
                        .insert(branch.as_str().to_string(), hash);
                }
            }
            self.mark_subscriptions_for_recompile();
        }
    }

    /// Mark all subscriptions for recompilation.
    ///
    /// Called when live schemas change to ensure subscriptions pick up new branches.
    fn mark_subscriptions_for_recompile(&mut self) {
        for sub in self.subscriptions.values_mut() {
            sub.needs_recompile = true;
        }
        for sub in self.server_subscriptions.values_mut() {
            sub.needs_recompile = true;
        }
    }

    /// Recompile subscriptions that are marked as stale.
    ///
    /// Called during process() to rebuild QueryGraphs when schemas change.
    fn recompile_stale_subscriptions(&mut self) {
        // Recompile local subscriptions
        for sub in self.subscriptions.values_mut() {
            if sub.needs_recompile {
                // Update branches from current schema context
                sub.branches = self
                    .schema_context
                    .all_branch_names()
                    .into_iter()
                    .map(|b| b.as_str().to_string())
                    .collect();

                // Recompile the graph
                if let Some(new_graph) = QueryGraph::compile_with_schema_context(
                    &sub.query,
                    &self.schema,
                    sub.session.clone(),
                    &self.schema_context,
                ) {
                    sub.graph = new_graph;
                }
                sub.needs_recompile = false;
            }
        }

        // Recompile server-side subscriptions
        for sub in self.server_subscriptions.values_mut() {
            if sub.needs_recompile {
                // Recompile the graph
                if let Some(new_graph) = QueryGraph::compile_with_schema_context(
                    &sub.query,
                    &self.schema,
                    sub.session.clone(),
                    &self.schema_context,
                ) {
                    sub.graph = new_graph;
                }
                sub.needs_recompile = false;
            }
        }
    }

    /// Get the schema context.
    pub fn schema_context(&self) -> &SchemaContext {
        &self.schema_context
    }

    /// Get the current branch name for writes.
    ///
    /// Returns the branch for the current schema, or "main" if context isn't initialized.
    fn current_branch(&self) -> String {
        if self.schema_context.is_initialized() {
            self.schema_context.branch_name().as_str().to_string()
        } else {
            "main".to_string()
        }
    }

    /// Get all branches to query for a table (current + live schemas).
    pub fn all_query_branches(&self) -> Vec<String> {
        self.schema_context
            .all_branch_names()
            .into_iter()
            .map(|b| b.as_str().to_string())
            .collect()
    }

    /// No-op: Storage manages its own index storage.
    /// Kept as public API for SchemaManager compatibility.
    pub fn ensure_indices_for_branch(
        &mut self,
        _table: &str,
        _branch: &str,
        _table_schema: &TableSchema,
    ) {
        // No-op: Storage manages index storage directly
    }

    /// Get the underlying SyncManager.
    pub fn sync_manager(&self) -> &SyncManager {
        &self.sync_manager
    }

    /// Get mutable reference to the underlying SyncManager.
    pub fn sync_manager_mut(&mut self) -> &mut SyncManager {
        &mut self.sync_manager
    }

    /// Get the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Test accessor for subscriptions (internal testing only).
    #[cfg(test)]
    pub(crate) fn test_subscriptions_mut(
        &mut self,
    ) -> &mut HashMap<super::graph_nodes::output::QuerySubscriptionId, QuerySubscription> {
        &mut self.subscriptions
    }

    /// Test accessor for subscriptions (internal testing only).
    #[cfg(test)]
    pub(crate) fn test_subscriptions(
        &self,
    ) -> &HashMap<super::graph_nodes::output::QuerySubscriptionId, QuerySubscription> {
        &self.subscriptions
    }

    /// Get subscription results as decoded rows with ObjectIds (for testing).
    /// Returns `Vec<(ObjectId, Vec<Value>)>` to match the old execute() return type.
    #[cfg(test)]
    pub fn get_subscription_results(
        &self,
        sub_id: super::graph_nodes::output::QuerySubscriptionId,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let Some(subscription) = self.subscriptions.get(&sub_id) else {
            return vec![];
        };

        let descriptor = &subscription.graph.combined_descriptor;

        subscription
            .graph
            .current_result()
            .iter()
            .filter_map(|row| {
                decode_row(descriptor, &row.data)
                    .ok()
                    .map(|values| (row.id, values))
            })
            .collect()
    }

    /// Get contributing ObjectIds for a subscription (for testing).
    #[cfg(test)]
    pub fn get_subscription_contributing_ids(
        &self,
        sub_id: super::graph_nodes::output::QuerySubscriptionId,
    ) -> std::collections::HashSet<(crate::object::ObjectId, crate::object::BranchName)> {
        self.subscriptions
            .get(&sub_id)
            .map(|sub| sub.graph.contributing_object_ids())
            .unwrap_or_default()
    }

    /// Check if a row is indexed on a specific branch (appears in the _id index).
    pub fn row_is_indexed_on_branch(
        &self,
        storage: &dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> bool {
        let ids = storage.index_lookup(table, "_id", branch, &Value::Uuid(row_id));
        ids.contains(&row_id)
    }

    /// Check if a row is indexed on the default branch (appears in the _id index).
    pub fn row_is_indexed(&self, storage: &dyn Storage, table: &str, row_id: ObjectId) -> bool {
        self.row_is_indexed_on_branch(storage, table, &self.current_branch(), row_id)
    }

    /// Check if a row is soft-deleted on a specific branch.
    pub fn row_is_deleted_on_branch(
        &self,
        storage: &dyn Storage,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> bool {
        let ids = storage.index_lookup(table, "_id_deleted", branch, &Value::Uuid(row_id));
        ids.contains(&row_id)
    }

    /// Check if a row is soft-deleted (appears in _id_deleted but not _id).
    pub fn row_is_deleted(&self, storage: &dyn Storage, table: &str, row_id: ObjectId) -> bool {
        self.row_is_deleted_on_branch(storage, table, &self.current_branch(), row_id)
    }

    /// Check if a row has a hard delete tombstone (empty content + delete: hard metadata).
    fn is_hard_deleted(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Hard delete: empty content + delete: hard metadata
        commit.content.is_empty()
            && commit
                .metadata
                .as_ref()
                .and_then(|m| m.get(MetadataKey::Delete.as_str()))
                .map(|v| v == DeleteKind::Hard.as_str())
                .unwrap_or(false)
    }

    /// Check if the current tip has `delete: soft` metadata.
    fn is_soft_delete_commit(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Soft delete: has delete: soft metadata (content is preserved)
        commit
            .metadata
            .as_ref()
            .and_then(|m| m.get(MetadataKey::Delete.as_str()))
            .map(|v| v == DeleteKind::Soft.as_str())
            .unwrap_or(false)
    }

    /// Check if a commit has been stored to disk.
    ///
    /// With sync storage, commits are stored immediately.
    /// Used by `InsertHandle::is_complete()` to check durability.
    pub fn is_commit_stored(&self, object_id: ObjectId, commit_id: &CommitId) -> bool {
        if let Some(obj) = self.sync_manager.object_manager.get(object_id) {
            // Check all branches for the commit
            for branch in obj.branches.values() {
                if let Some(commit) = branch.commits.get(commit_id) {
                    return matches!(commit.stored_state, StoredState::Stored);
                }
            }
        }
        false
    }

    /// Insert a new row into a table.
    ///
    /// Returns an `InsertHandle` that can be polled to check durability.
    /// Index updates happen immediately (creating sentinels if needed).
    pub fn insert<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        self.insert_with_session(storage, table, values, None)
    }

    /// Insert a new row with session-based policy checking.
    ///
    /// If the table has an INSERT WITH CHECK policy and a session is provided,
    /// the policy is evaluated against the new row values. If the policy
    /// denies the insert, `PolicyDenied` is returned.
    pub fn insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertHandle, QueryError> {
        let table_name = TableName::new(table);
        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();
        let insert_policy = table_schema.policies.insert.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Check INSERT WITH CHECK policy
        if let (Some(session), Some(policy)) = (session, insert_policy)
            && !self.evaluate_policy_for_values(&policy, values, &descriptor, session, table)
        {
            return Err(QueryError::PolicyDenied {
                table: table_name,
                operation: Operation::Insert,
            });
        }

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id = self
            .sync_manager
            .object_manager
            .create(storage, Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data
        let branch = self.current_branch();
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                object_id,
                &branch,
                vec![],
                data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Forward new row to all connected servers
        self.sync_manager
            .forward_update_to_servers(object_id, branch.into());

        // Update indices immediately and persist
        self.update_indices_for_insert(storage, table, object_id, &data, &descriptor)?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);

        Ok(InsertHandle {
            row_id: object_id,
            row_commit_id,
        })
    }

    /// Insert a new row into a table on a specific branch.
    ///
    /// Used by SchemaManager for schema-aware inserts.
    pub fn insert_on_branch<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        self.insert_on_branch_with_session(storage, table, branch, values, None)
    }

    /// Insert a new row on a specific branch with session-based policy checking.
    pub fn insert_on_branch_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertHandle, QueryError> {
        let table_name = TableName::new(table);
        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();
        let insert_policy = table_schema.policies.insert.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Check INSERT WITH CHECK policy
        if let (Some(session), Some(policy)) = (session, insert_policy)
            && !self.evaluate_policy_for_values(&policy, values, &descriptor, session, table)
        {
            return Err(QueryError::PolicyDenied {
                table: table_name,
                operation: Operation::Insert,
            });
        }

        // Encode to binary
        let data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Create object with table metadata
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());

        let object_id = self
            .sync_manager
            .object_manager
            .create(storage, Some(metadata));
        let author = object_id; // Self-authored

        // Add commit with row data to specified branch
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                object_id,
                branch,
                vec![],
                data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(object_id))?;

        // Forward new row to all connected servers
        self.sync_manager
            .forward_update_to_servers(object_id, branch.into());

        // Update indices on specified branch
        Self::update_indices_for_insert_on_branch(
            storage,
            table,
            branch,
            object_id,
            &data,
            &descriptor,
        )?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);

        Ok(InsertHandle {
            row_id: object_id,
            row_commit_id,
        })
    }

    /// Evaluate a policy expression against row values (pre-encoding).
    ///
    /// This is used for write policy checking (INSERT/UPDATE WITH CHECK).
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_policy_for_values(
        &self,
        policy: &crate::query_manager::policy::PolicyExpr,
        values: &[Value],
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
    ) -> bool {
        use crate::query_manager::policy::PolicyExpr;

        match policy {
            PolicyExpr::True => true,
            PolicyExpr::False => false,

            PolicyExpr::Cmp { column, op, value } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                let col_value = &values[col_index];
                let cmp_value = match value {
                    crate::query_manager::policy::PolicyValue::Literal(v) => v.clone(),
                    crate::query_manager::policy::PolicyValue::SessionRef(path) => {
                        match resolve_session_value(path, session) {
                            Some(v) => v,
                            None => return false,
                        }
                    }
                };
                self.compare_values(col_value, &cmp_value, op)
            }

            PolicyExpr::IsNull { column } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                matches!(values[col_index], Value::Null)
            }

            PolicyExpr::IsNotNull { column } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                !matches!(values[col_index], Value::Null)
            }

            PolicyExpr::In {
                column,
                session_path,
            } => {
                let col_index = match descriptor.column_index(column) {
                    Some(idx) => idx,
                    None => return false,
                };
                let col_value = &values[col_index];
                let session_array = match session.get_array(session_path) {
                    Some(arr) => arr,
                    None => return false,
                };
                self.value_in_json_array(col_value, session_array)
            }

            PolicyExpr::And(exprs) => exprs
                .iter()
                .all(|e| self.evaluate_policy_for_values(e, values, descriptor, session, table)),

            PolicyExpr::Or(exprs) => exprs
                .iter()
                .any(|e| self.evaluate_policy_for_values(e, values, descriptor, session, table)),

            PolicyExpr::Not(expr) => {
                !self.evaluate_policy_for_values(expr, values, descriptor, session, table)
            }

            PolicyExpr::Exists { .. } | PolicyExpr::Inherits { .. } => {
                // EXISTS and INHERITS require actual row data - for writes, return true
                // (TODO: implement for write policies that need these)
                true
            }
        }
    }

    /// Compare two Values with the given operator.
    fn compare_values(
        &self,
        a: &Value,
        b: &Value,
        op: &crate::query_manager::policy::CmpOp,
    ) -> bool {
        use crate::query_manager::policy::CmpOp;
        use std::cmp::Ordering;

        let ord = match (a, b) {
            (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
            (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
            (Value::Integer(x), Value::BigInt(y)) => (*x as i64).cmp(y),
            (Value::BigInt(x), Value::Integer(y)) => x.cmp(&(*y as i64)),
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
            (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
            (Value::Uuid(x), Value::Uuid(y)) => x.0.cmp(&y.0),
            _ => return false, // Type mismatch
        };

        match op {
            CmpOp::Eq => ord == Ordering::Equal,
            CmpOp::Ne => ord != Ordering::Equal,
            CmpOp::Lt => ord == Ordering::Less,
            CmpOp::Le => ord != Ordering::Greater,
            CmpOp::Gt => ord == Ordering::Greater,
            CmpOp::Ge => ord != Ordering::Less,
        }
    }

    /// Check if a Value is in a JSON array.
    fn value_in_json_array(&self, value: &Value, array: &[serde_json::Value]) -> bool {
        match value {
            Value::Text(s) => array.iter().any(|v| v.as_str() == Some(s.as_str())),
            Value::Integer(i) => array.iter().any(|v| v.as_i64() == Some(*i as i64)),
            Value::BigInt(i) => array.iter().any(|v| v.as_i64() == Some(*i)),
            _ => false,
        }
    }

    /// Update a row.
    pub fn update<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
    ) -> Result<CommitId, QueryError> {
        self.update_with_session(storage, id, values, None)
    }

    /// Update a row with session-based policy checking.
    ///
    /// If the table has policies and a session is provided:
    /// - USING policy is checked against the old row (if exists)
    /// - WITH CHECK policy is checked against the new values
    pub fn update_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<CommitId, QueryError> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Get old data from ObjectManager
        let (old_data, commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();
        let using_policy = table_schema.policies.update.using.clone();
        let check_policy = table_schema.policies.update.with_check.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Check UPDATE USING policy against old row
        if let (Some(session), Some(policy)) = (session, &using_policy) {
            let old_row = crate::query_manager::types::Row::new(id, old_data.clone(), commit_id);
            if !self.evaluate_policy_for_row(policy, &old_row, &descriptor, session, &table) {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Update,
                });
            }
        }

        // Check UPDATE WITH CHECK policy against new values
        if let (Some(session), Some(policy)) = (session, check_policy)
            && !self.evaluate_policy_for_values(&policy, values, &descriptor, session, &table)
        {
            return Err(QueryError::PolicyDenied {
                table: table_name,
                operation: Operation::Update,
            });
        }

        // Encode new data
        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with new data
        let commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                new_data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Forward update to all connected servers
        let branch = self.current_branch();
        self.sync_manager
            .forward_update_to_servers(id, branch.into());

        // Update indices and persist modified nodes
        self.update_indices_for_update(
            storage,
            &table_name.0,
            id,
            &old_data,
            &new_data,
            &descriptor,
        )?;

        // Mark subscriptions dirty and notify about content update
        self.mark_subscriptions_dirty(&table_name.0);
        self.mark_row_updated_in_subscriptions(&table_name.0, id);

        Ok(commit_id)
    }

    /// Evaluate a policy expression against an encoded row.
    fn evaluate_policy_for_row(
        &self,
        policy: &crate::query_manager::policy::PolicyExpr,
        row: &crate::query_manager::types::Row,
        descriptor: &RowDescriptor,
        session: &Session,
        table: &str,
    ) -> bool {
        use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;

        // Create a temporary PolicyFilterNode to evaluate the policy
        let filter = PolicyFilterNode::new(
            descriptor.clone(),
            policy.clone(),
            session.clone(),
            (*self.schema).clone(),
            table,
        );
        filter.evaluate(row)
    }

    /// Soft delete a row.
    ///
    /// Creates a commit with the same content as the previous tip, plus `delete: soft` metadata.
    /// This preserves the row data for queries with `include_deleted`.
    /// Removes from `_id` and all column indices, adds to `_id_deleted` index.
    pub fn delete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager (for index removal and content preservation)
        let (old_data, _) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let delete_metadata = soft_delete_metadata();

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                old_data.clone(), // Preserve content for soft deletes
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(storage, &table, id, &old_data, &descriptor)?;

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Soft delete a row with session-based policy checking.
    ///
    /// Checks DELETE USING policy against the existing row before allowing deletion.
    /// Falls back to UPDATE's USING policy if no DELETE policy is defined.
    pub fn delete_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Check if already soft-deleted
        if self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager (for index removal and content preservation)
        let (old_data, commit_id) = self
            .load_row_from_object(id)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Check DELETE USING policy (falls back to UPDATE's USING)
        let using_policy = table_schema.policies.effective_delete_using().cloned();
        if let (Some(session), Some(policy)) = (session, using_policy) {
            let old_row = Row::new(id, old_data.clone(), commit_id);
            if !self.evaluate_policy_for_row(&policy, &old_row, &descriptor, session, &table) {
                return Err(QueryError::PolicyDenied {
                    table: table_name,
                    operation: Operation::Delete,
                });
            }
        }

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let delete_metadata = soft_delete_metadata();

        // Add commit with preserved content + delete: soft metadata
        // Content is copied from previous tip so soft-deleted rows can still be read
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                old_data.clone(), // Preserve content for soft deletes
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Forward delete to all connected servers
        {
            let branch = self.current_branch();
            self.sync_manager
                .forward_update_to_servers(id, branch.into());
        }

        // Update indices: remove from _id and column indices, add to _id_deleted
        self.update_indices_for_soft_delete(storage, &table, id, &old_data, &descriptor)?;

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Soft delete a row on a specific branch.
    ///
    /// Used by SchemaManager for schema-aware deletes.
    pub fn delete_on_branch<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        branch: &str,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first (checks default branch)
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        let table_name = TableName::new(table);

        // Check if already soft-deleted on this branch
        if self.row_is_deleted_on_branch(storage, table, branch, id) {
            return Err(QueryError::RowAlreadyDeleted(id));
        }

        // Get old data from ObjectManager on this branch
        let (old_data, _) = self
            .load_row_from_object_on_branch(id, branch)
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit on this branch
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, branch)
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create delete metadata
        let delete_metadata = soft_delete_metadata();

        // Add commit with preserved content + delete: soft metadata
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                branch,
                parents,
                old_data.clone(),
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices on this branch
        Self::update_indices_for_soft_delete_on_branch(
            storage,
            table,
            branch,
            id,
            &old_data,
            &descriptor,
        )?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(table);
        self.mark_row_deleted_in_subscriptions(table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Undelete a soft-deleted row.
    ///
    /// Restores a row from the `_id_deleted` index back to the `_id` and column indices.
    /// Creates a new commit with the provided values (no `delete` metadata).
    pub fn undelete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
        values: &[Value],
    ) -> Result<InsertHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        if values.len() != descriptor.columns.len() {
            return Err(QueryError::ColumnCountMismatch {
                expected: descriptor.columns.len(),
                actual: values.len(),
            });
        }

        // Encode new row data
        let new_data = encode_row(&descriptor, values)
            .map_err(|e| QueryError::EncodingError(e.to_string()))?;

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Add commit with row data (no delete metadata = undelete)
        let row_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                new_data.clone(),
                author,
                None,
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from _id_deleted, add to _id and column indices
        self.update_indices_for_undelete(storage, &table, id, &new_data, &descriptor)?;

        // Mark subscriptions dirty
        self.mark_subscriptions_dirty(&table);

        Ok(InsertHandle {
            row_id: id,
            row_commit_id,
        })
    }

    /// Hard delete a row.
    ///
    /// Creates a commit with empty content and `delete: hard` metadata.
    /// Removes from ALL indices including `_id_deleted`.
    /// Truncates history: only the hard delete tombstone remains.
    /// Hard deletes are authoritative and override any concurrent or subsequent commits.
    pub fn hard_delete<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check if already hard-deleted
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        let table_name = TableName::new(&table);

        // Try to get old data (may be empty if already soft-deleted)
        // Treat empty content as no data (tombstone)
        let old_data = self
            .load_row_from_object(id)
            .map(|(data, _)| data)
            .filter(|data| !data.is_empty());

        let table_schema = self
            .schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;
        let descriptor = table_schema.descriptor.clone();

        // Get parent commit
        let tips = self
            .sync_manager
            .object_manager
            .get_tip_ids(id, self.current_branch())
            .map_err(|_| QueryError::ObjectNotFound(id))?
            .clone();

        let parents: Vec<_> = tips.into_iter().collect();
        let author = id;

        // Create hard delete metadata
        let delete_metadata = hard_delete_metadata();

        // Add commit with empty content + delete: hard metadata
        let delete_commit_id = self
            .sync_manager
            .object_manager
            .add_commit(
                storage,
                id,
                self.current_branch(),
                parents,
                vec![], // Empty content for tombstone
                author,
                Some(delete_metadata),
            )
            .map_err(|_| QueryError::ObjectNotFound(id))?;

        // Update indices: remove from ALL indices including _id_deleted
        self.update_indices_for_hard_delete(storage, &table, id, old_data.as_deref(), &descriptor)?;

        // Truncate branch: set tails = [delete_commit_id], removing all history
        // (In ObjectManager, this would be done via set_tails or similar)
        // For now, we just record the hard delete tombstone
        let mut tail_ids = std::collections::HashSet::new();
        tail_ids.insert(delete_commit_id);
        let _ = self.sync_manager.object_manager.truncate_branch(
            storage,
            id,
            self.current_branch(),
            tail_ids,
        );

        // Mark subscriptions dirty and mark row as deleted
        self.mark_subscriptions_dirty(&table);
        self.mark_row_deleted_in_subscriptions(&table, id);

        Ok(DeleteHandle {
            row_id: id,
            delete_commit_id,
        })
    }

    /// Truncate a soft-deleted row (upgrade to hard delete).
    ///
    /// Can only be called on rows that are already soft-deleted.
    /// Removes the row from `_id_deleted` and truncates history.
    pub fn truncate<H: Storage>(
        &mut self,
        storage: &mut H,
        id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        // Check for hard delete first
        if self.is_hard_deleted(id) {
            return Err(QueryError::RowHardDeleted(id));
        }

        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)
            .and_then(|obj| obj.metadata.get(MetadataKey::Table.as_str()).cloned())
            .ok_or(QueryError::ObjectNotFound(id))?;

        // Verify row is in _id_deleted index (soft-deleted)
        if !self.row_is_deleted(storage, &table, id) {
            return Err(QueryError::RowNotDeleted(id));
        }

        // Upgrade to hard delete
        self.hard_delete(storage, id)
    }

    /// Get a row by ID if loaded in ObjectManager.
    ///
    /// Returns decoded values and the table name if the row exists.
    pub fn get_row(&self, id: ObjectId) -> Option<(String, Vec<Value>)> {
        // Get table name from object metadata
        let table = self
            .sync_manager
            .object_manager
            .get(id)?
            .metadata
            .get(MetadataKey::Table.as_str())?
            .clone();
        let table_name = TableName::new(&table);

        // Get row data from ObjectManager
        let (data, _) = self.load_row_from_object(id)?;

        let table_schema = self.schema.get(&table_name)?;
        let values = decode_row(&table_schema.descriptor, &data).ok()?;
        Some((table, values))
    }

    /// Test helper: get a row by ID if loaded in ObjectManager.
    ///
    /// Production code should use queries to read data, not this method.
    /// This exists only to verify test expectations about what's loaded.
    #[cfg(test)]
    pub fn test_get_row_if_loaded(&self, id: ObjectId) -> Option<Vec<Value>> {
        self.get_row(id).map(|(_, values)| values)
    }

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Subscribe to query results (delta mode).
    pub fn subscribe(&mut self, query: Query) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_session(query, None, None)
    }

    /// Subscribe to query results with session-based policy filtering.
    ///
    /// When a session is provided and the table has a SELECT policy, rows are
    /// filtered based on the policy expression evaluated against the session context.
    ///
    /// Uses schema-aware compilation with:
    /// - Automatic branch expansion to include all live schemas
    /// - Column name translation for old schema indices
    /// - Lens transforms for rows from old schema branches
    ///
    /// `settled_tier`: If Some, holds first delivery until the specified tier confirms.
    pub fn subscribe_with_session(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        // Determine branches
        let branches: Vec<String> = if !query.branches.is_empty() {
            query.branches.clone()
        } else if self.schema_context.is_initialized() {
            self.schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        } else {
            return Err(QueryError::QueryCompilationError(
                "schema context not initialized - call set_current_schema() first".into(),
            ));
        };

        // Compile query graph with schema context
        let graph = QueryGraph::compile_with_schema_context(
            &query,
            &self.schema,
            session.clone(),
            &self.schema_context,
        )
        .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                query,
                graph,
                mode: SubscriptionMode::Delta,
                branches,
                session,
                needs_recompile: false,
                settled_once: false,
                settled_tier,
                achieved_tiers: HashSet::new(),
            },
        );

        Ok(id)
    }

    /// Subscribe with explicit schema and context (for server use).
    ///
    /// This is the core subscription method for server-side query processing.
    /// The regular `subscribe_with_session()` calls the internal context,
    /// but servers need to use the client's schema context.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to subscribe to
    /// * `schema` - The target schema (client's schema)
    /// * `schema_context` - Context with current schema and any live schemas
    /// * `session` - Optional session for policy evaluation
    pub fn subscribe_with_explicit_context(
        &mut self,
        query: Query,
        schema: &Schema,
        schema_context: &SchemaContext,
        session: Option<Session>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let table_name = &query.table;
        let _table_schema = schema
            .get(table_name)
            .ok_or(QueryError::TableNotFound(*table_name))?;

        // Determine branches from query or context
        let branches: Vec<String> = if !query.branches.is_empty() {
            query.branches.clone()
        } else {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        };

        // Compile query graph with explicit schema context
        let graph = QueryGraph::compile_with_schema_context(
            &query,
            schema,
            session.clone(),
            schema_context,
        )
        .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                query,
                graph,
                mode: SubscriptionMode::Delta,
                branches,
                session,
                needs_recompile: false,
                settled_once: false,
                settled_tier: None,
                achieved_tiers: HashSet::new(),
            },
        );

        Ok(id)
    }

    /// Subscribe to query results and sync matching objects from servers.
    ///
    /// This method:
    /// 1. Creates a local subscription (like `subscribe_with_session`)
    /// 2. Sends a QuerySubscription to all connected servers
    ///
    /// Servers will evaluate the query against their data and send ObjectUpdated
    /// messages for all matching objects. As new objects match the query on
    /// the server, they are automatically synced.
    ///
    /// The returned QuerySubscriptionId is used both locally and in the sync protocol.
    pub fn subscribe_with_sync(
        &mut self,
        query: Query,
        session: Option<Session>,
        settled_tier: Option<PersistenceTier>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        // Create local subscription
        let sub_id = self.subscribe_with_session(query.clone(), session.clone(), settled_tier)?;

        // Expand branches for sync payload - server needs explicit branch to resolve schema
        let mut sync_query = query;
        if sync_query.branches.is_empty() && self.schema_context.is_initialized() {
            sync_query.branches = vec![self.schema_context.branch_name().as_str().to_string()];
        }

        // Send QuerySubscription to all servers
        // Use the subscription ID as the query ID for simplicity
        let query_id = crate::sync_manager::QueryId(sub_id.0);
        self.sync_manager
            .send_query_subscription_to_servers(query_id, sync_query, session);

        Ok(sub_id)
    }

    /// Unsubscribe from a synced query.
    ///
    /// This method:
    /// 1. Removes the local subscription
    /// 2. Sends a QueryUnsubscription to all connected servers
    pub fn unsubscribe_with_sync(&mut self, id: QuerySubscriptionId) {
        self.subscriptions.remove(&id);

        // Send QueryUnsubscription to all servers
        let query_id = crate::sync_manager::QueryId(id.0);
        self.sync_manager
            .send_query_unsubscription_to_servers(query_id);
    }

    /// Take pending query updates.
    pub fn take_updates(&mut self) -> Vec<QueryUpdate> {
        std::mem::take(&mut self.update_outbox)
    }

    /// Take pending catalogue updates (schemas/lenses received via sync).
    ///
    /// SchemaManager should call this to process new schemas and lenses
    /// discovered through catalogue sync.
    pub fn take_pending_catalogue_updates(&mut self) -> Vec<CatalogueUpdate> {
        std::mem::take(&mut self.pending_catalogue_updates)
    }

    /// Retry processing buffered row updates.
    ///
    /// Call this after activating new schemas (via try_activate_pending_schemas)
    /// and updating the schema context (via sync_context). Rows that arrived
    /// before their schema was known will be reprocessed.
    pub fn retry_pending_row_updates(&mut self, storage: &mut dyn Storage) {
        let pending = std::mem::take(&mut self.pending_row_updates);
        for update in pending {
            self.handle_object_update(storage, update);
        }
    }

    /// Take all pending row updates (used by sync_context to preserve across rebuild).
    pub fn take_pending_row_updates(&mut self) -> Vec<AllObjectUpdate> {
        std::mem::take(&mut self.pending_row_updates)
    }

    /// Restore pending row updates (used by sync_context after rebuild).
    pub fn restore_pending_row_updates(&mut self, updates: Vec<AllObjectUpdate>) {
        self.pending_row_updates = updates;
    }

    /// Set known schemas for server-mode operation.
    ///
    /// Called by SchemaManager.process() to sync the known_schemas map.
    /// This enables lazy branch activation when rows arrive with unknown branches.
    pub fn set_known_schemas(&mut self, schemas: HashMap<SchemaHash, Schema>) {
        self.known_schemas = schemas;
    }

    /// Add a branch → schema hash mapping (for server-mode schema activation).
    ///
    /// Used when a subscription arrives with explicit schema context.
    pub fn add_schema_branch(&mut self, branch: &str, schema_hash: SchemaHash) {
        self.branch_schema_map
            .insert(branch.to_string(), schema_hash);
    }

    /// Find a schema in known_schemas by its short hash prefix.
    ///
    /// Returns the full SchemaHash if found. The partial hash has the first 4 bytes
    /// filled with the short hash, and the rest zeroed (as produced by ComposedBranchName::parse).
    fn find_schema_by_short_hash(&self, partial: &SchemaHash) -> Option<SchemaHash> {
        let target_short = &partial.0[..4];

        // Search known_schemas for matching short hash
        for full_hash in self.known_schemas.keys() {
            if &full_hash.0[..4] == target_short {
                return Some(*full_hash);
            }
        }
        None
    }

    /// Update the branch_schema_map from the current schema context.
    ///
    /// Called internally after schema changes to ensure the map
    /// includes all live schemas.
    pub fn update_branch_schema_map(&mut self) {
        if !self.schema_context.is_initialized() {
            return;
        }

        // Current schema branch
        self.branch_schema_map.insert(
            self.schema_context.branch_name().as_str().to_string(),
            self.schema_context.current_hash,
        );

        // Live schema branches
        for &live_hash in self.schema_context.live_schemas.keys() {
            let live_branch = ComposedBranchName::new(
                &self.schema_context.env,
                live_hash,
                &self.schema_context.user_branch,
            )
            .to_branch_name();
            self.branch_schema_map
                .insert(live_branch.as_str().to_string(), live_hash);
        }
    }

    /// Process pending changes and settle all subscription graphs.
    ///
    /// This method drives async progress:
    /// - Processes SyncManager inbox (receives client writes)
    /// - Evaluates pending permission checks
    /// - Settles policy graphs and finalizes completed checks
    /// - Processes object updates from SyncManager
    /// - Flushes pending index updates when indices become ready
    /// - Marks subscriptions with pending IDs dirty when objects become available
    /// - Settles all subscription graphs (row data loaded on-demand from ObjectManager)
    pub fn process<H: Storage>(&mut self, storage: &mut H) {
        // 1. Process SyncManager inbox (receives client writes)
        self.sync_manager.process_inbox(storage);

        // 2. Process object updates from SyncManager FIRST
        // This ensures indices are updated before query subscriptions are processed,
        // so new subscriptions can find data that arrived in the same batch.
        let updates = self.sync_manager.object_manager.take_all_object_updates();
        for update in updates {
            self.handle_object_update(storage, update);
        }

        // 3. Process pending query subscriptions from downstream clients
        // (after indices are updated, so initial settle finds existing data)
        self.process_pending_query_subscriptions(storage);

        // 3b. Process pending query unsubscriptions from downstream clients
        self.process_pending_query_unsubscriptions();

        // 4. Pick up new permission check intents from SyncManager
        self.pick_up_pending_permission_checks(storage);

        // 4b. Settle policy graphs and finalize completed checks
        self.settle_policy_checks(storage);

        // 5. Index storage is handled by Storage via batched_tick() - not here.
        // Tests/benchmarks that don't need real storage use NullStorage.

        // 6. Recompile any subscriptions marked as stale due to schema changes
        self.recompile_stale_subscriptions();

        // 7a. Process incoming QuerySettled notifications
        let settled_notifications = self.sync_manager.take_pending_query_settled();
        for (query_id, tier) in settled_notifications {
            let sub_id = QuerySubscriptionId(query_id.0);
            if let Some(sub) = self.subscriptions.get_mut(&sub_id) {
                sub.achieved_tiers.insert(tier);
            }
        }

        // 7. Settle all subscriptions - row_loader reads from subscription's branches
        // Extract references to avoid borrowing self in the closure
        let om = &mut self.sync_manager.object_manager;
        let storage_ref: &dyn Storage = storage;
        let schema_context = &self.schema_context;
        let branch_schema_map = &self.branch_schema_map;

        for (sub_id, subscription) in &mut self.subscriptions {
            let branches = &subscription.branches;
            let table = subscription.graph.table.as_str().to_string();

            // Row loader returns None for empty content (hard delete tombstones)
            // Soft deletes have preserved content and can be materialized normally
            // For single-branch subscriptions, reads from that branch
            // For multi-branch subscriptions, uses LWW across branches
            // When schema context is present, applies lens transform for old schema branches
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj = om.get_or_load(id, storage_ref, branches)?;
                // Find the newest commit across all subscription branches (LWW)
                // Also track which branch it came from for schema transformation
                let mut best: Option<(u64, Vec<u8>, CommitId, String)> = None;

                for branch_name in branches {
                    if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                        for &tip_id in &branch.tips {
                            if let Some(commit) = branch.commits.get(&tip_id) {
                                match &best {
                                    None => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                            branch_name.clone(),
                                        ));
                                    }
                                    Some((best_ts, _, _, _)) if commit.timestamp > *best_ts => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                            branch_name.clone(),
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                // Filter out empty content (hard delete tombstones only)
                let (_, content, commit_id, source_branch) =
                    best.filter(|(_, content, _, _)| !content.is_empty())?;

                // Apply lens transform if row is from an old schema branch
                if let Some(&source_hash) = branch_schema_map.get(&source_branch)
                    && source_hash != schema_context.current_hash
                {
                    // Transform the row data using lens
                    let transformer = LensTransformer::new(schema_context, &table);
                    match transformer.transform(&content, commit_id, source_hash) {
                        Ok(result) => {
                            return Some((result.data, commit_id));
                        }
                        Err(_) => {
                            // Transform failed - return original data
                            // This allows graceful degradation
                            return Some((content, commit_id));
                        }
                    }
                }

                Some((content, commit_id))
            };

            let delta = subscription.graph.settle(storage_ref, row_loader);

            let tier_satisfied = match &subscription.settled_tier {
                None => true, // No tier requirement → immediate (current behavior)
                Some(required) => subscription.achieved_tiers.iter().any(|t| t >= required),
            };

            if !tier_satisfied {
                // Graph state updated by settle(), but don't deliver yet
                continue;
            }

            if !subscription.settled_once {
                // First delivery — full current state snapshot
                subscription.settled_once = true;
                let full_result = subscription.graph.current_result_as_delta();
                // For settled_tier=None, deliver even if empty (preserves current behavior
                // where one-shot queries get an initial callback)
                if !full_result.is_empty() || subscription.settled_tier.is_none() {
                    self.update_outbox.push(QueryUpdate {
                        subscription_id: *sub_id,
                        delta: full_result,
                        descriptor: subscription.graph.combined_descriptor.clone(),
                    });
                }
            } else if !delta.is_empty() {
                // Incremental delivery
                self.update_outbox.push(QueryUpdate {
                    subscription_id: *sub_id,
                    delta,
                    descriptor: subscription.graph.combined_descriptor.clone(),
                });
            }
        }

        // Note: With sync storage, object loading is immediate. No need to request
        // async loads - objects are available when we query for them.

        // 8. Settle server-side subscriptions and update scopes
        self.settle_server_subscriptions(storage_ref);
    }

    /// Pick up pending permission checks from SyncManager and evaluate them.
    fn pick_up_pending_permission_checks<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_permission_checks();

        for check in pending {
            self.evaluate_write_permission(storage, check);
        }
    }

    /// Process pending query subscriptions from downstream clients.
    ///
    /// For each pending subscription:
    /// 1. Build a QueryGraph with the client's session
    /// 2. Settle the graph to get contributing ObjectIds
    /// 3. Set the scope in SyncManager (which triggers initial sync)
    fn process_pending_query_subscriptions<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_query_subscriptions();
        let mut deferred = Vec::new();

        for sub in pending {
            // Resolve schema: use self.schema if available, otherwise look up from known_schemas (server mode)
            let schema_for_compile: Arc<Schema> = if !self.schema.is_empty() {
                self.schema.clone()
            } else {
                // Server mode: resolve schema from known_schemas via branch name (short hash prefix match)
                let schema = sub
                    .query
                    .branches
                    .first()
                    .and_then(|b| ComposedBranchName::parse(&BranchName::new(b)))
                    .and_then(|composed| {
                        // Use prefix match since branch only contains short hash
                        self.find_schema_by_short_hash(&composed.schema_hash)
                    })
                    .and_then(|full_hash| self.known_schemas.get(&full_hash))
                    .cloned();
                match schema {
                    Some(s) => Arc::new(s),
                    None => {
                        // Schema not available yet — re-queue for next process() call
                        deferred.push(sub);
                        continue;
                    }
                }
            };

            // Build QueryGraph with client's session for policy filtering (schema-aware)
            let graph = QueryGraph::compile_with_schema_context(
                &sub.query,
                &schema_for_compile,
                sub.session.clone(),
                &self.schema_context,
            );

            let Some(mut graph) = graph else {
                // Query compilation failed (e.g., missing table)
                // TODO: Send error back to client
                continue;
            };

            // Initial settle to populate the graph
            let om = &mut self.sync_manager.object_manager;
            let storage_ref: &dyn Storage = storage;

            // Resolve branches: use explicit branches or fall back to schema context
            let branches: Vec<String> = if sub.query.branches.is_empty() {
                self.schema_context
                    .all_branch_names()
                    .into_iter()
                    .map(|b| b.as_str().to_string())
                    .collect()
            } else {
                sub.query.branches.clone()
            };

            // Simple row loader for server-side graphs (no schema transform needed)
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj = om.get_or_load(id, storage_ref, &branches)?;
                let mut best: Option<(u64, Vec<u8>, CommitId)> = None;
                for branch_name in &branches {
                    if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                        for &tip_id in &branch.tips {
                            if let Some(commit) = branch.commits.get(&tip_id) {
                                match &best {
                                    None => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                        ));
                                    }
                                    Some((best_ts, _, _)) if commit.timestamp > *best_ts => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                best.filter(|(_, content, _)| !content.is_empty())
                    .map(|(_, content, commit_id)| (content, commit_id))
            };

            let _delta = graph.settle(storage_ref, row_loader);

            // Get contributing ObjectIds
            let scope = graph.contributing_object_ids();

            // Set scope in SyncManager (triggers initial sync)
            self.sync_manager.set_client_query_scope(
                sub.client_id,
                sub.query_id,
                scope.clone(),
                sub.session.clone(),
            );

            // Forward QuerySubscription to upstream servers (multi-tier forwarding)
            // This allows hub servers to know about the query and push matching data
            self.sync_manager.send_query_subscription_to_servers(
                sub.query_id,
                sub.query.clone(),
                sub.session.clone(),
            );

            // Store the server subscription for reactive updates
            self.server_subscriptions.insert(
                (sub.client_id, sub.query_id),
                ServerQuerySubscription {
                    query: sub.query,
                    graph,
                    session: sub.session,
                    branches,
                    last_scope: scope,
                    needs_recompile: false,
                    settled_once: false,
                },
            );
        }

        // Re-queue subscriptions whose schema wasn't available yet
        if !deferred.is_empty() {
            self.sync_manager
                .requeue_pending_query_subscriptions(deferred);
        }
    }

    /// Process pending query unsubscriptions from downstream clients.
    ///
    /// For each pending unsubscription:
    /// 1. Remove the server-side QueryGraph
    /// 2. Forward the unsubscription to upstream servers
    fn process_pending_query_unsubscriptions(&mut self) {
        let pending = self.sync_manager.take_pending_query_unsubscriptions();

        for unsub in pending {
            // Remove the server subscription
            self.server_subscriptions
                .remove(&(unsub.client_id, unsub.query_id));

            // Forward unsubscription to upstream servers
            self.sync_manager
                .send_query_unsubscription_to_servers(unsub.query_id);
        }
    }

    /// Settle server-side query subscriptions and update scopes.
    ///
    /// Called after local data changes to detect when new objects match
    /// a client's query subscription.
    #[allow(clippy::type_complexity)]
    fn settle_server_subscriptions(&mut self, storage: &dyn Storage) {
        // Collect updates to avoid borrow issues
        let mut scope_updates: Vec<(
            ClientId,
            QueryId,
            HashSet<(ObjectId, BranchName)>,
            Option<Session>,
        )> = Vec::new();
        let mut settled_notifications: Vec<(ClientId, QueryId)> = Vec::new();

        let om = &mut self.sync_manager.object_manager;

        for ((client_id, query_id), sub) in &mut self.server_subscriptions {
            let branches = &sub.branches;

            // Row loader for this subscription
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj = om.get_or_load(id, storage, branches)?;
                let mut best: Option<(u64, Vec<u8>, CommitId)> = None;
                for branch_name in branches {
                    if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                        for &tip_id in &branch.tips {
                            if let Some(commit) = branch.commits.get(&tip_id) {
                                match &best {
                                    None => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                        ));
                                    }
                                    Some((best_ts, _, _)) if commit.timestamp > *best_ts => {
                                        best = Some((
                                            commit.timestamp,
                                            commit.content.clone(),
                                            tip_id,
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                best.filter(|(_, content, _)| !content.is_empty())
                    .map(|(_, content, commit_id)| (content, commit_id))
            };

            // Settle the graph
            let _delta = sub.graph.settle(storage, row_loader);

            // Emit QuerySettled on first settlement
            if !sub.settled_once {
                sub.settled_once = true;
                settled_notifications.push((*client_id, *query_id));
            }

            // Check if scope changed
            let new_scope = sub.graph.contributing_object_ids();
            if new_scope != sub.last_scope {
                scope_updates.push((
                    *client_id,
                    *query_id,
                    new_scope.clone(),
                    sub.session.clone(),
                ));
                sub.last_scope = new_scope;
            }
        }

        // Apply scope updates
        for (client_id, query_id, new_scope, session) in scope_updates {
            self.sync_manager
                .set_client_query_scope(client_id, query_id, new_scope, session);
        }

        // Emit QuerySettled notifications
        for (client_id, query_id) in settled_notifications {
            self.sync_manager.emit_query_settled(client_id, query_id);
        }
    }

    /// Evaluate a write permission check.
    ///
    /// If the simple parts of the policy fail, reject immediately.
    /// If there are complex clauses (INHERITS/EXISTS), create policy graphs.
    /// If all simple parts pass and no complex clauses, approve immediately.
    ///
    /// For UPDATE operations, we evaluate two policies:
    /// - USING against old_content (can the session see the old row?)
    /// - WITH CHECK against new_content (is the new row valid?)
    fn evaluate_write_permission<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
    ) {
        // Get table name from metadata
        let table_name = match check.metadata.get(MetadataKey::Table.as_str()) {
            Some(t) => TableName::new(t),
            None => {
                // Not a row object, allow
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // Look up table schema - clone to avoid borrowing self
        let table_schema = match self.schema.get(&table_name).cloned() {
            Some(s) => s,
            None => {
                // Unknown table, allow
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // Handle UPDATE specially - needs both USING and WITH CHECK
        if check.operation == Operation::Update {
            self.evaluate_update_permission(storage, check, table_name, table_schema);
            return;
        }

        // Get the appropriate policy based on operation
        let policy = match check.operation {
            Operation::Insert => table_schema.policies.insert.with_check.as_ref(),
            Operation::Update => unreachable!(), // Handled above
            Operation::Delete => table_schema.policies.effective_delete_using(),
            Operation::Select => {
                // SELECT not checked via write permission
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // If no policy defined, allow
        let policy = match policy {
            Some(p) => p.clone(),
            None => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // Get the content to evaluate
        let content = match check.operation {
            Operation::Insert => check.new_content.as_ref(),
            Operation::Update => unreachable!(), // Handled above
            Operation::Delete => check.old_content.as_ref(),
            Operation::Select => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let content = match content {
            Some(c) => c,
            None => {
                // No content to evaluate - allow
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // Evaluate simple parts of the policy
        let result =
            evaluate_simple_parts(&policy, content, &table_schema.descriptor, &check.session);

        if !result.passed {
            // Simple parts failed - reject immediately
            let reason = format!(
                "{:?} denied by policy on table {}",
                check.operation, table_name.0
            );
            self.sync_manager.reject_permission_check(check, reason);
            return;
        }

        if result.complex_clauses.is_empty() {
            // All simple parts passed and no complex clauses - approve immediately
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Has complex clauses - create policy graphs for them
        let graphs = self.create_policy_graphs_for_complex_clauses(
            &result.complex_clauses,
            content,
            &table_schema.descriptor,
            &table_name,
            &check.session,
        );

        if graphs.is_empty() {
            // No graphs created (maybe missing tables) - allow
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Store for settling
        let check_id = check.id;
        self.active_policy_checks.insert(
            check_id,
            PolicyCheckState {
                graphs,
                table: table_name,
                pending_check: check,
            },
        );
    }

    /// Evaluate UPDATE permission with both USING (old row) and WITH CHECK (new row).
    ///
    /// For UPDATE, we need to check:
    /// 1. USING policy against old_content - can the session see the row being updated?
    /// 2. WITH CHECK policy against new_content - is the resulting row valid?
    ///
    /// Both must pass for the update to be allowed.
    fn evaluate_update_permission<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
        table_name: TableName,
        table_schema: TableSchema,
    ) {
        let using_policy = table_schema.policies.update.using.as_ref();
        let check_policy = table_schema.policies.update.with_check.as_ref();

        // If no policies defined, allow
        if using_policy.is_none() && check_policy.is_none() {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        let mut all_complex_clauses: Vec<(ComplexClause, Vec<u8>)> = Vec::new();

        // Step 1: Evaluate USING policy against old_content
        if let Some(using) = using_policy {
            let old_content = match check.old_content.as_ref() {
                Some(c) if !c.is_empty() => c,
                _ => {
                    // No old content means this is actually an INSERT, not UPDATE
                    // Reject - UPDATE USING requires seeing the old row
                    let reason = format!(
                        "Update denied by USING policy on table {} - no old content",
                        table_name.0
                    );
                    self.sync_manager.reject_permission_check(check, reason);
                    return;
                }
            };

            let result =
                evaluate_simple_parts(using, old_content, &table_schema.descriptor, &check.session);

            if !result.passed {
                // USING check failed - session cannot see the old row
                let reason = format!(
                    "Update denied by USING policy on table {} - cannot see old row",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }

            // Collect complex clauses with old_content for USING
            for clause in result.complex_clauses {
                all_complex_clauses.push((clause, old_content.clone()));
            }
        }

        // Step 2: Evaluate WITH CHECK policy against new_content
        if let Some(with_check) = check_policy {
            let new_content = match check.new_content.as_ref() {
                Some(c) => c,
                None => {
                    // No new content - allow (shouldn't happen for UPDATE)
                    self.sync_manager.approve_permission_check(storage, check);
                    return;
                }
            };

            let result = evaluate_simple_parts(
                with_check,
                new_content,
                &table_schema.descriptor,
                &check.session,
            );

            if !result.passed {
                // WITH CHECK failed - new row is not valid
                let reason = format!(
                    "Update denied by WITH CHECK policy on table {}",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }

            // Collect complex clauses with new_content for WITH CHECK
            for clause in result.complex_clauses {
                all_complex_clauses.push((clause, new_content.clone()));
            }
        }

        // If no complex clauses, both simple checks passed - approve
        if all_complex_clauses.is_empty() {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Create policy graphs for all complex clauses
        let mut graphs = Vec::new();
        for (clause, content) in &all_complex_clauses {
            let clause_graphs = self.create_policy_graphs_for_complex_clauses(
                std::slice::from_ref(clause),
                content,
                &table_schema.descriptor,
                &table_name,
                &check.session,
            );
            graphs.extend(clause_graphs);
        }

        if graphs.is_empty() {
            // No graphs created (maybe missing tables) - allow
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Store for settling
        let check_id = check.id;
        self.active_policy_checks.insert(
            check_id,
            PolicyCheckState {
                graphs,
                table: table_name,
                pending_check: check,
            },
        );
    }

    /// Create policy graphs for complex clauses (INHERITS/EXISTS).
    fn create_policy_graphs_for_complex_clauses(
        &self,
        clauses: &[ComplexClause],
        content: &[u8],
        descriptor: &RowDescriptor,
        _table: &TableName,
        session: &Session,
    ) -> Vec<PolicyGraph> {
        let mut graphs = Vec::new();
        let branch = self.current_branch();

        for clause in clauses {
            match clause {
                ComplexClause::Inherits {
                    operation,
                    via_column,
                } => {
                    // Get the FK column to find the parent
                    let col_idx = match descriptor.column_index(via_column) {
                        Some(idx) => idx,
                        None => continue, // Column not found
                    };

                    // Get the referenced table
                    let parent_table = match &descriptor.columns[col_idx].references {
                        Some(t) => *t,
                        None => continue, // No FK reference
                    };

                    // Check if FK is NULL - if so, INHERITS passes
                    if super::encoding::column_is_null(descriptor, content, col_idx)
                        .unwrap_or(false)
                    {
                        continue; // NULL FK passes INHERITS
                    }

                    // Decode the FK value to get parent ObjectId
                    let parent_id =
                        match super::encoding::decode_column(descriptor, content, col_idx) {
                            Ok(Value::Uuid(id)) => id,
                            _ => continue, // Can't decode FK
                        };

                    // Get parent's policy for the specified operation
                    let parent_schema = match self.schema.get(&parent_table) {
                        Some(s) => s,
                        None => continue, // Parent table not in schema
                    };

                    let parent_policy = match operation {
                        Operation::Select => parent_schema.policies.select.using.as_ref(),
                        Operation::Insert => parent_schema.policies.insert.with_check.as_ref(),
                        Operation::Update => parent_schema.policies.update.using.as_ref(),
                        Operation::Delete => parent_schema.policies.effective_delete_using(),
                    };

                    // If parent has no policy, INHERITS passes
                    let parent_policy = match parent_policy {
                        Some(p) => p,
                        None => continue,
                    };

                    // Create policy graph for INHERITS
                    if let Some(graph) = PolicyGraph::for_inherits(
                        &parent_table,
                        parent_id,
                        parent_policy,
                        session,
                        &self.schema,
                        &branch,
                    ) {
                        graphs.push(graph);
                    }
                }
                ComplexClause::Exists { table, condition } => {
                    let target_table = TableName::new(table);
                    if let Some(graph) = PolicyGraph::for_exists(
                        &target_table,
                        condition,
                        session,
                        &self.schema,
                        &branch,
                    ) {
                        graphs.push(graph);
                    }
                }
            }
        }

        graphs
    }

    /// Settle active policy checks and finalize completed ones.
    fn settle_policy_checks<H: Storage>(&mut self, storage: &mut H) {
        // Collect IDs to finalize
        let mut to_approve = Vec::new();
        let mut to_reject = Vec::new();

        // Create row loader for settling
        let current_branch = self.current_branch();
        let branches = vec![current_branch.clone()];
        let om = &mut self.sync_manager.object_manager;
        let storage_ref: &dyn Storage = storage;

        // Settle each active policy check
        for (pending_id, state) in &mut self.active_policy_checks {
            let mut row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj = om.get_or_load(id, storage_ref, &branches)?;
                let branch = obj.branches.get(&BranchName::new(&current_branch))?;
                let tip_id = branch.tips.iter().next()?;
                let commit = branch.commits.get(tip_id)?;
                if commit.content.is_empty() {
                    return None;
                }
                Some((commit.content.clone(), *tip_id))
            };

            // Settle all graphs
            let all_complete = state
                .graphs
                .iter_mut()
                .all(|g| g.settle(storage_ref, &mut row_loader));

            if all_complete {
                // All graphs settled - check results
                let all_pass = state.graphs.iter().all(|g| g.result());

                if all_pass {
                    to_approve.push(*pending_id);
                } else {
                    let reason = format!(
                        "{:?} denied by policy on table {} (INHERITS check failed)",
                        state.pending_check.operation, state.table.0
                    );
                    to_reject.push((*pending_id, reason));
                }
            }
        }

        // Finalize completed checks
        for id in to_approve {
            if let Some(state) = self.active_policy_checks.remove(&id) {
                self.sync_manager
                    .approve_permission_check(storage, state.pending_check);
            }
        }

        for (id, reason) in to_reject {
            if let Some(state) = self.active_policy_checks.remove(&id) {
                self.sync_manager
                    .reject_permission_check(state.pending_check, reason);
            }
        }
    }

    /// Load a row's data from a specific branch using LWW (last-writer-wins by timestamp).
    /// When multiple concurrent tips exist, returns content from the tip with highest timestamp.
    fn load_row_from_object_on_branch(
        &self,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<(Vec<u8>, CommitId)> {
        let obj = self.sync_manager.object_manager.get(row_id)?;
        let branch = obj.branches.get(&BranchName::new(branch_name))?;
        // Sort tips by timestamp (oldest first), take last (newest = LWW winner)
        let mut tips: Vec<_> = branch.tips.iter().copied().collect();
        tips.sort_by_key(|id| branch.commits.get(id).map(|c| c.timestamp).unwrap_or(0));
        let tip_id = tips.last()?;
        let commit = branch.commits.get(tip_id)?;
        Some((commit.content.clone(), *tip_id))
    }

    /// Load a row's data from ObjectManager using the default branch.
    fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        self.load_row_from_object_on_branch(row_id, &self.current_branch())
    }

    /// Load content from a catalogue object's "main" branch.
    ///
    /// Used for loading schema/lens data from catalogue objects.
    fn load_object_content(&self, object_id: ObjectId) -> Option<Vec<u8>> {
        self.load_row_from_object_on_branch(object_id, "main")
            .map(|(content, _)| content)
    }

    /// Load a row's data from multiple branches, using LWW (last-writer-wins) to select
    /// the branch with the highest timestamp when the same ObjectId exists on multiple branches.
    ///
    /// Returns the content and commit ID from the branch with the newest commit.
    #[allow(dead_code)]
    fn load_row_from_object_multi_branch(
        &self,
        row_id: ObjectId,
        branches: &[String],
    ) -> Option<(Vec<u8>, CommitId)> {
        let obj = self.sync_manager.object_manager.get(row_id)?;

        // Collect the newest tip from each branch
        let mut best: Option<(u64, Vec<u8>, CommitId)> = None; // (timestamp, content, commit_id)

        for branch_name in branches {
            if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                // Find the tip with the highest timestamp on this branch
                for &tip_id in &branch.tips {
                    if let Some(commit) = branch.commits.get(&tip_id) {
                        match &best {
                            None => {
                                best = Some((commit.timestamp, commit.content.clone(), tip_id));
                            }
                            Some((best_ts, _, _)) if commit.timestamp > *best_ts => {
                                best = Some((commit.timestamp, commit.content.clone(), tip_id));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        best.map(|(_, content, commit_id)| (content, commit_id))
    }

    /// Load a row's data from multiple branches with source branch info.
    ///
    /// Same as load_row_from_object_multi_branch but also returns which branch the data came from.
    #[allow(dead_code)]
    fn load_row_from_object_multi_branch_with_source(
        &self,
        row_id: ObjectId,
        branches: &[String],
    ) -> Option<(Vec<u8>, CommitId, String)> {
        let obj = self.sync_manager.object_manager.get(row_id)?;

        // Collect the newest tip from each branch
        let mut best: Option<(u64, Vec<u8>, CommitId, String)> = None;

        for branch_name in branches {
            if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                for &tip_id in &branch.tips {
                    if let Some(commit) = branch.commits.get(&tip_id) {
                        match &best {
                            None => {
                                best = Some((
                                    commit.timestamp,
                                    commit.content.clone(),
                                    tip_id,
                                    branch_name.clone(),
                                ));
                            }
                            Some((best_ts, _, _, _)) if commit.timestamp > *best_ts => {
                                best = Some((
                                    commit.timestamp,
                                    commit.content.clone(),
                                    tip_id,
                                    branch_name.clone(),
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        best.map(|(_, content, commit_id, branch)| (content, commit_id, branch))
    }

    /// Handle an object update from the global subscription.
    fn handle_object_update(&mut self, storage: &mut dyn Storage, update: AllObjectUpdate) {
        // Check if this is a catalogue object (schema or lens)
        if let Some(type_str) = update.metadata.get(MetadataKey::Type.as_str())
            && (type_str == ObjectType::CatalogueSchema.as_str()
                || type_str == ObjectType::CatalogueLens.as_str())
        {
            // Queue for SchemaManager processing
            // Load content from the object's latest commit
            if let Some(content) = self.load_object_content(update.object_id) {
                self.pending_catalogue_updates.push(CatalogueUpdate {
                    object_id: update.object_id,
                    metadata: update.metadata.clone(),
                    content,
                });
            }
            return;
        }

        // Check if this is a row object
        let table = match update.metadata.get(MetadataKey::Table.as_str()) {
            Some(t) => t.clone(),
            None => return,
        };

        let table_name = TableName::new(&table);
        let branch = update.branch_name.as_str();

        // Look up the correct schema for this branch
        let schema_hash = match self.branch_schema_map.get(branch) {
            Some(&hash) => hash,
            None => {
                // Unknown branch - try lazy activation from known_schemas
                let branch_name = BranchName::new(branch);
                if let Some(composed) = ComposedBranchName::parse(&branch_name) {
                    // Search known_schemas for matching short hash
                    if let Some(full_hash) = self.find_schema_by_short_hash(&composed.schema_hash) {
                        // Activate this branch/schema combination
                        self.branch_schema_map.insert(branch.to_string(), full_hash);
                        full_hash
                    } else {
                        // Schema not known yet - buffer for retry
                        self.pending_row_updates.push(update);
                        return;
                    }
                } else {
                    // Can't parse branch - buffer for retry
                    self.pending_row_updates.push(update);
                    return;
                }
            }
        };

        // Get the correct schema for this branch
        let table_schema = if schema_hash == self.schema_context.current_hash {
            // Current schema - use self.schema
            match self.schema.get(&table_name) {
                Some(schema) => schema.clone(),
                None => return,
            }
        } else if let Some(schema) = self.schema_context.get_schema(&schema_hash) {
            // Live schema from context
            match schema.get(&table_name) {
                Some(table_schema) => table_schema.clone(),
                None => return,
            }
        } else if let Some(schema) = self.known_schemas.get(&schema_hash) {
            // Known schema (server mode) - not in context but available
            match schema.get(&table_name) {
                Some(table_schema) => table_schema.clone(),
                None => return,
            }
        } else {
            // Schema not available - buffer for retry
            self.pending_row_updates.push(update);
            return;
        };

        let descriptor = table_schema.descriptor.clone();

        // Check if we have a local hard delete tombstone - if so, ignore incoming updates
        if self.is_hard_deleted(update.object_id) {
            // Hard delete is authoritative - ignore incoming updates
            return;
        }

        // Check if incoming update is a hard delete
        if self.is_incoming_hard_delete(update.object_id) {
            // Apply hard delete unconditionally
            let old_data = update.old_content.as_deref();
            let _ = Self::update_indices_for_hard_delete_on_branch(
                storage,
                &table,
                branch,
                update.object_id,
                old_data,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        // Check if incoming update is a soft delete
        if self.is_soft_delete_commit(update.object_id) {
            // Apply soft delete - remove from _id and column indices, add to _id_deleted
            if let Some(old_data) = &update.old_content {
                let _ = Self::update_indices_for_soft_delete_on_branch(
                    storage,
                    &table,
                    branch,
                    update.object_id,
                    old_data,
                    &descriptor,
                );
            } else {
                // No old content - just remove from _id and add to _id_deleted
                let _ = storage.index_remove(
                    &table,
                    "_id",
                    branch,
                    &Value::Uuid(update.object_id),
                    update.object_id,
                );
                let _ = storage.index_insert(
                    &table,
                    "_id_deleted",
                    branch,
                    &Value::Uuid(update.object_id),
                    update.object_id,
                );
            }
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        // Check if this is an undelete (non-empty content for previously soft-deleted row)
        let was_soft_deleted =
            self.row_is_deleted_on_branch(storage, &table, branch, update.object_id);

        // Extract current (new) data from the object on this branch
        let new_data = match self.load_row_from_object_on_branch(update.object_id, branch) {
            Some((data, _)) => data,
            None => return,
        };

        if was_soft_deleted {
            // This is an undelete - remove from _id_deleted, add to _id and column indices
            let _ = Self::update_indices_for_undelete_on_branch(
                storage,
                &table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            return;
        }

        // Normal update handling
        if update.is_new_object || update.previous_commit_ids.is_empty() {
            // First commit on branch (new object or synced first commit) - insert into all indices
            let _ = Self::update_indices_for_insert_on_branch(
                storage,
                &table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            );
        } else if let Some(old_data) = update.old_content {
            // Synced update - compute index delta using old_content
            // TODO: Future merge strategies - currently last-writer-wins by timestamp
            let _ = Self::update_indices_for_update_on_branch(
                storage,
                &table,
                branch,
                update.object_id,
                &old_data,
                &new_data,
                &descriptor,
            );
        }
        // If old_content is None with previous_commit_ids: truncated old data, accept staleness

        self.mark_subscriptions_dirty(&table);
        self.mark_row_updated_in_subscriptions(&table, update.object_id);
    }

    /// Check if an incoming update has hard delete metadata.
    fn is_incoming_hard_delete(&self, id: ObjectId) -> bool {
        let Some(obj) = self.sync_manager.object_manager.get(id) else {
            return false;
        };
        let Some(branch) = obj.branches.get(&BranchName::new(self.current_branch())) else {
            return false;
        };
        let Some(tip_id) = branch.tips.iter().next() else {
            return false;
        };
        let Some(commit) = branch.commits.get(tip_id) else {
            return false;
        };
        // Hard delete: empty content + delete: hard metadata
        commit.content.is_empty()
            && commit
                .metadata
                .as_ref()
                .and_then(|m| m.get(MetadataKey::Delete.as_str()))
                .map(|v| v == DeleteKind::Hard.as_str())
                .unwrap_or(false)
    }

    /// Update indices when a row is inserted on a specific branch.
    fn update_indices_for_insert_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Update "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        // Update column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, data, col_idx)
                && value != Value::Null
            {
                storage
                    .index_insert(table, col.name.as_str(), branch, &value, object_id)
                    .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;
            }
        }

        Ok(())
    }

    /// Update indices when a row is inserted (on the default branch).
    fn update_indices_for_insert(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_insert_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            data,
            descriptor,
        )
    }

    /// Update indices when a row is updated on a specific branch.
    fn update_indices_for_update_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // "_id" index doesn't change on update

        // Update column indices (remove old value, add new value)
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            // Remove old value
            if let Ok(old_value) = decode_column(descriptor, old_data, col_idx)
                && old_value != Value::Null
            {
                let _ =
                    storage.index_remove(table, col.name.as_str(), branch, &old_value, object_id);
            }
            // Add new value
            if let Ok(new_value) = decode_column(descriptor, new_data, col_idx)
                && new_value != Value::Null
            {
                let _ =
                    storage.index_insert(table, col.name.as_str(), branch, &new_value, object_id);
            }
        }

        Ok(())
    }

    /// Update indices when a row is updated (on the default branch).
    fn update_indices_for_update(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_update_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            new_data,
            descriptor,
        )
    }

    /// Update indices for soft delete on a specific branch.
    fn update_indices_for_soft_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index
        let _ = storage.index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id);

        // Remove from all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, old_data, col_idx)
                && value != Value::Null
            {
                let _ = storage.index_remove(table, col.name.as_str(), branch, &value, object_id);
            }
        }

        // Add to "_id_deleted" index
        storage
            .index_insert(
                table,
                "_id_deleted",
                branch,
                &Value::Uuid(object_id),
                object_id,
            )
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        Ok(())
    }

    /// Update indices for soft delete (on the default branch).
    fn update_indices_for_soft_delete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_soft_delete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for hard delete on a specific branch.
    fn update_indices_for_hard_delete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id" index (may not be present if already soft-deleted)
        let _ = storage.index_remove(table, "_id", branch, &Value::Uuid(object_id), object_id);

        // Remove from all column indices (if we have old data)
        if let Some(data) = old_data {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                if let Ok(value) = decode_column(descriptor, data, col_idx)
                    && value != Value::Null
                {
                    let _ =
                        storage.index_remove(table, col.name.as_str(), branch, &value, object_id);
                }
            }
        }

        // Remove from "_id_deleted" index (handles soft→hard upgrade)
        let _ = storage.index_remove(
            table,
            "_id_deleted",
            branch,
            &Value::Uuid(object_id),
            object_id,
        );

        Ok(())
    }

    /// Update indices for hard delete (on the default branch).
    fn update_indices_for_hard_delete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        old_data: Option<&[u8]>,
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_hard_delete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            old_data,
            descriptor,
        )
    }

    /// Update indices for undelete on a specific branch.
    fn update_indices_for_undelete_on_branch(
        storage: &mut dyn Storage,
        table: &str,
        branch: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        // Remove from "_id_deleted" index
        let _ = storage.index_remove(
            table,
            "_id_deleted",
            branch,
            &Value::Uuid(object_id),
            object_id,
        );

        // Add to "_id" index
        storage
            .index_insert(table, "_id", branch, &Value::Uuid(object_id), object_id)
            .map_err(|e| QueryError::IndexError(format!("{:?}", e)))?;

        // Add to all column indices
        for (col_idx, col) in descriptor.columns.iter().enumerate() {
            if let Ok(value) = decode_column(descriptor, new_data, col_idx)
                && value != Value::Null
            {
                let _ = storage.index_insert(table, col.name.as_str(), branch, &value, object_id);
            }
        }

        Ok(())
    }

    /// Update indices for undelete (on the default branch).
    fn update_indices_for_undelete(
        &self,
        storage: &mut dyn Storage,
        table: &str,
        object_id: ObjectId,
        new_data: &[u8],
        descriptor: &RowDescriptor,
    ) -> Result<(), QueryError> {
        Self::update_indices_for_undelete_on_branch(
            storage,
            table,
            &self.current_branch(),
            object_id,
            new_data,
            descriptor,
        )
    }

    /// Mark subscriptions dirty for a table.
    /// Checks all tables involved in the subscription (including joined tables).
    /// Also marks server-side subscriptions for downstream clients.
    fn mark_subscriptions_dirty(&mut self, table: &str) {
        // Mark local subscriptions dirty
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_dirty_for_table(table);
            }
        }

        // Mark server subscriptions dirty (for downstream clients)
        for server_sub in self.server_subscriptions.values_mut() {
            if Self::subscription_involves_table(&server_sub.graph, table) {
                server_sub.graph.mark_dirty_for_table(table);
            }
        }
    }

    /// Mark a row as updated in all subscriptions for a table.
    /// This triggers content change detection during settle().
    /// Checks all tables involved in the subscription (including joined tables).
    fn mark_row_updated_in_subscriptions(&mut self, table: &str, id: ObjectId) {
        // Mark local subscriptions
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_row_updated(id);
            }
        }
        // Mark server subscriptions (serving downstream clients)
        for server_sub in self.server_subscriptions.values_mut() {
            if Self::subscription_involves_table(&server_sub.graph, table) {
                server_sub.graph.mark_row_updated(id);
            }
        }
    }

    /// Mark a row as deleted in all subscriptions for a table.
    /// This triggers removal delta emission during settle().
    /// Checks all tables involved in the subscription (including joined tables).
    fn mark_row_deleted_in_subscriptions(&mut self, table: &str, id: ObjectId) {
        // Mark local subscriptions
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_row_deleted(id);
            }
        }
        // Mark server subscriptions (serving downstream clients)
        for server_sub in self.server_subscriptions.values_mut() {
            if Self::subscription_involves_table(&server_sub.graph, table) {
                server_sub.graph.mark_row_deleted(id);
            }
        }
    }

    /// Check if a subscription involves a given table (base table, joined table, or array subquery inner table).
    fn subscription_involves_table(graph: &super::graph::QueryGraph, table: &str) -> bool {
        graph.involves_table(table)
    }

    // ========================================================================
    // No-op storage driver (for tests)
    // ========================================================================

    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Calculate memory usage breakdown for profiling.
    ///
    /// Returns a tuple: (indices, subscriptions, policy_checks, total)
    /// Note: indices are managed by Storage, so index memory is reported as 0.
    pub fn memory_size(&self) -> (usize, usize, usize, usize) {
        let indices = 0usize; // Indices managed by Storage

        // Subscriptions (QueryGraph can be large)
        let mut subscriptions = 0usize;
        for (id, sub) in &self.subscriptions {
            subscriptions += std::mem::size_of_val(id);
            subscriptions += std::mem::size_of::<QuerySubscription>();
            subscriptions += sub.graph.estimate_memory_size();
            subscriptions += 48; // HashMap entry overhead
        }
        subscriptions += self.update_outbox.len() * 256; // QueryUpdate overhead

        // Active policy checks
        let mut policy_checks = 0usize;
        for state in self.active_policy_checks.values() {
            policy_checks += 48; // HashMap entry
            policy_checks += state.graphs.len() * 1024; // Rough estimate per PolicyGraph
            policy_checks += state.table.0.len();
        }

        let total = indices + subscriptions + policy_checks;
        (indices, subscriptions, policy_checks, total)
    }
}
