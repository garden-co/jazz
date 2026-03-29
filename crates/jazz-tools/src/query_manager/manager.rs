use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::commit::CommitId;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::AllObjectUpdate;
use crate::schema_manager::SchemaContext;
use crate::storage::{CatalogueManifestOp, Storage, StorageError};
use crate::sync_manager::{
    ClientId, DurabilityTier, PendingPermissionCheck, PendingUpdateId, QueryId, QueryPropagation,
    SyncManager,
};

use super::graph::{QueryCompileError, QueryGraph};
use super::graph_nodes::output::QuerySubscriptionId;
use super::policy::Operation;
use super::policy_graph::PolicyGraph;
use super::query::Query;
use super::session::Session;
use super::types::{
    BatchId, ComposedBranchName, LoadedRow, OrderedRowDelta, QueryBranchRef, RowDelta,
    RowDescriptor, Schema, SchemaHash, TableName, TableSchema, Value,
    build_ordered_delta_with_post_ids,
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
    IndexValueTooLarge {
        table: TableName,
        column: String,
        branch: String,
        key_bytes: usize,
        max_key_bytes: usize,
    },
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
            QueryError::IndexValueTooLarge {
                table,
                column,
                branch,
                key_bytes,
                max_key_bytes,
            } => write!(
                f,
                "indexed value too large for {table}.{column} on branch {branch}: index key would be {key_bytes} bytes (max {max_key_bytes})"
            ),
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
/// Wrappers (jazz-runtime, jazz-wasm) use this to fulfill
/// platform-specific futures/promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryHandle(pub u64);

/// Result of an insert, including durability metadata and row values.
///
/// Poll via `is_complete()` to check if the row is persisted.
/// Poll via `is_indexed()` to check if the row is indexed.
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the row data.
    pub row_commit_id: CommitId,
    /// Inserted row values in table column order.
    pub row_values: Vec<Value>,
}

/// Handle for tracking delete completion.
#[derive(Debug, Clone)]
pub struct DeleteHandle {
    /// The row's ObjectId.
    pub row_id: ObjectId,
    /// CommitId of the delete tombstone commit.
    pub delete_commit_id: CommitId,
}

impl InsertResult {
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
    /// Branches to read from (updated on recompile).
    pub(crate) branches: Vec<QueryBranchRef>,
    /// Session for policy filtering (if any).
    pub(crate) session: Option<Session>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    pub(crate) needs_recompile: bool,
    /// Flag indicating this subscription has settled at least once.
    /// Used to ensure one-shot queries receive an initial callback (even if empty).
    pub(crate) settled_once: bool,
    /// Required durability tier before non-local delivery (None = immediate).
    pub(crate) durability_tier: Option<DurabilityTier>,
    /// How local writes behave while waiting for durability.
    pub(crate) local_updates: LocalUpdates,
    /// True when this subscription observed a local write since last delivery.
    pub(crate) has_pending_local_updates: bool,
    /// Tiers that have confirmed settlement for this query.
    pub(crate) achieved_tiers: HashSet<DurabilityTier>,
    /// Current ordered IDs for ordered delta construction.
    pub(crate) current_ordered_ids: Vec<ObjectId>,
    /// Whether this subscription should be forwarded to upstream servers.
    pub(crate) propagation: QueryPropagation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalUpdates {
    #[default]
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerSubscriptionTelemetryGroup {
    #[serde(rename = "groupKey")]
    pub group_key: String,
    pub count: usize,
    pub table: String,
    pub query: String,
    pub branches: Vec<String>,
    pub propagation: QueryPropagation,
}

/// Update for a query subscription.
#[derive(Debug, Clone)]
pub struct QueryUpdate {
    pub subscription_id: QuerySubscriptionId,
    pub delta: RowDelta,
    pub ordered_delta: OrderedRowDelta,
    /// Output descriptor for decoding the binary row data.
    /// This matches the query's output schema (handles JOINs, projections, etc).
    pub descriptor: RowDescriptor,
}

/// Terminal failure for a local query subscription.
#[derive(Debug, Clone)]
pub struct QuerySubscriptionFailure {
    pub subscription_id: QuerySubscriptionId,
    pub reason: String,
}

/// State for an active policy check (graphs and associated data).
#[derive(Debug)]
pub(super) struct PolicyCheckState {
    /// Policy graphs that need to settle.
    pub(super) graphs: Vec<PolicyGraph>,
    /// Table name for error messages.
    pub(super) table: TableName,
    /// Branch whose visible state the policy graphs should read.
    pub(super) branch: BranchName,
    /// The original pending permission check.
    pub(super) pending_check: PendingPermissionCheck,
}

/// Server-side query subscription state.
///
/// When a client sends a QuerySubscription, the server builds a QueryGraph
/// and tracks contributing ObjectIds. This struct holds that state.
#[derive(Debug)]
pub(super) struct ServerQuerySubscription {
    /// The original query.
    pub(super) query: Query,
    /// Compiled QueryGraph (with client's session for policy filtering).
    pub(super) graph: QueryGraph,
    /// Subscription-specific schema context derived from the downstream client schema.
    pub(super) schema_context: SchemaContext,
    /// Client's session for permission evaluation.
    pub(super) session: Option<Session>,
    /// Resolved branches (from query.branches or schema context at creation time).
    pub(super) branches: Vec<QueryBranchRef>,
    /// Last computed scope (for detecting changes).
    pub(super) last_scope: HashSet<(ObjectId, BranchName)>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    pub(super) needs_recompile: bool,
    /// Flag indicating this server subscription has settled at least once.
    /// Used to emit QuerySettled to the client on first settlement.
    pub(super) settled_once: bool,
    /// Whether this subscription should be propagated to upstream servers.
    pub(super) propagation: QueryPropagation,
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
    pub(super) sync_manager: SyncManager,
    pub(super) schema: Arc<Schema>,

    /// Pending catalogue updates (schemas/lenses received via sync).
    /// SchemaManager should call take_pending_catalogue_updates() to process these.
    pub(super) pending_catalogue_updates: Vec<CatalogueUpdate>,

    /// Active query subscriptions (local)
    pub(super) subscriptions: HashMap<QuerySubscriptionId, QuerySubscription>,
    pub(super) next_subscription_id: u64,

    /// Pending query updates
    pub(super) update_outbox: Vec<QueryUpdate>,

    /// Terminal local subscription failures.
    pub(super) failed_subscriptions: Vec<QuerySubscriptionFailure>,

    /// Active policy checks being evaluated.
    pub(super) active_policy_checks: HashMap<PendingUpdateId, PolicyCheckState>,

    /// Server-side query subscriptions from downstream clients.
    /// Key is (client_id, query_id) to allow multiple queries per client.
    pub(super) server_subscriptions: HashMap<(ClientId, QueryId), ServerQuerySubscription>,

    /// Schema context for multi-schema queries.
    /// Starts empty; initialized via set_current_schema().
    /// Enables lens transforms for rows from old schema branches.
    pub(super) schema_context: SchemaContext,

    /// Maps branch name to schema hash (derived from schema_context).
    /// Used to determine which schema a branch uses.
    pub(super) branch_schema_map: HashMap<String, SchemaHash>,

    /// Buffered row updates for unknown schema branches.
    /// These are retried when new schemas activate via try_activate_pending().
    pub(super) pending_row_updates: Vec<AllObjectUpdate>,

    /// Known schemas (for server-mode operation).
    /// Synced from SchemaManager's known_schemas to enable lazy branch activation.
    /// When a row arrives with unknown branch, we parse the branch name to extract
    /// the short hash, then look up the full schema in this map.
    pub(super) known_schemas: Arc<HashMap<SchemaHash, Schema>>,
}

impl QueryManager {
    pub(super) fn branch_names_for_query_branches(branches: &[QueryBranchRef]) -> Vec<String> {
        branches
            .iter()
            .map(|branch| branch.as_str().to_string())
            .collect()
    }

    pub(super) fn resolve_query_branch_ref_for_context(
        schema_context: &SchemaContext,
        branch: &str,
    ) -> QueryBranchRef {
        QueryBranchRef::from_branch_name(schema_context.resolve_query_branch_name(branch))
    }

    pub(super) fn resolve_query_branch_ref(&self, branch: &str) -> QueryBranchRef {
        Self::resolve_query_branch_ref_for_context(&self.schema_context, branch)
    }

    pub(super) fn resolve_branch_name(&self, branch: &str) -> BranchName {
        self.schema_context.resolve_query_branch_name(branch)
    }

    pub fn server_subscription_telemetry(&self) -> Vec<ServerSubscriptionTelemetryGroup> {
        let mut groups: HashMap<String, ServerSubscriptionTelemetryGroup> = HashMap::new();

        for subscription in self.server_subscriptions.values() {
            let query = serde_json::to_string(&subscription.query)
                .unwrap_or_else(|_| "{\"error\":\"query serialization failed\"}".to_string());
            let propagation = propagation_label(subscription.propagation);
            let group_key = subscription_group_key(&query, &subscription.branches, propagation);

            groups
                .entry(group_key.clone())
                .and_modify(|group| group.count += 1)
                .or_insert_with(|| ServerSubscriptionTelemetryGroup {
                    group_key,
                    count: 1,
                    table: subscription.query.table.as_str().to_string(),
                    query,
                    branches: Self::branch_names_for_query_branches(&subscription.branches),
                    propagation: subscription.propagation,
                });
        }

        groups.into_values().collect()
    }

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
            failed_subscriptions: Vec::new(),
            active_policy_checks: HashMap::new(),
            server_subscriptions: HashMap::new(),
            schema_context: SchemaContext::empty(),
            branch_schema_map: HashMap::new(),
            pending_row_updates: Vec::new(),
            known_schemas: Arc::new(HashMap::new()),
        }
    }

    /// Set the current schema (the one this client writes to).
    ///
    /// Must be called before queries. Can only be called once.
    /// Uses the deterministic nil batch for callers that do not manage
    /// explicit batch identities themselves.
    pub fn set_current_schema(&mut self, schema: Schema, env: &str, user_branch: &str) {
        self.set_current_schema_with_batch(schema, env, user_branch, BatchId::nil());
    }

    pub fn set_current_schema_with_batch(
        &mut self,
        schema: Schema,
        env: &str,
        user_branch: &str,
        batch_id: BatchId,
    ) {
        self.schema_context
            .set_current_with_batch(schema.clone(), env, user_branch, batch_id);
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
        let branch = self.schema_context.branch_name_for_hash(hash);

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
                    let branch = self.schema_context.branch_name_for_hash(hash);

                    self.branch_schema_map
                        .insert(branch.as_str().to_string(), hash);
                }
            }
            self.mark_subscriptions_for_recompile();
        }
    }

    pub(super) fn compile_graph(
        query: &Query,
        branches: &[QueryBranchRef],
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Result<QueryGraph, QueryCompileError> {
        QueryGraph::try_compile_with_schema_context_and_branches(
            query,
            branches,
            schema,
            session,
            schema_context,
        )
    }

    fn default_query_branches_for_table(
        storage: &dyn Storage,
        table: &str,
        schema_context: &SchemaContext,
    ) -> Result<Vec<QueryBranchRef>, StorageError> {
        let mut branches = Vec::new();

        for branch_name in schema_context.all_branch_names() {
            let Some(composed_branch) = ComposedBranchName::parse(&branch_name) else {
                branches.push(QueryBranchRef::from_branch_name(branch_name));
                continue;
            };

            let prefix = BranchName::new(composed_branch.prefix().branch_prefix());
            let mut prefix_branches = storage.load_table_prefix_branches(table, prefix)?;

            if prefix_branches.is_empty() {
                branches.push(QueryBranchRef::from_branch_name(branch_name));
            } else {
                branches.append(&mut prefix_branches);
            }
        }

        branches.sort_by_key(|branch| branch.as_str().to_string());
        branches.dedup();
        Ok(branches)
    }

    pub(super) fn resolve_query_branches_for_context(
        storage: &dyn Storage,
        query: &Query,
        schema_context: &SchemaContext,
    ) -> Result<Vec<QueryBranchRef>, StorageError> {
        if query.branches.is_empty() {
            Self::default_query_branches_for_table(storage, query.table.as_str(), schema_context)
        } else {
            Ok(query
                .branches
                .iter()
                .map(|branch| Self::resolve_query_branch_ref_for_context(schema_context, branch))
                .collect())
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
    fn recompile_stale_subscriptions(&mut self, storage: &dyn Storage) {
        let mut failed_local: Vec<(QuerySubscriptionId, String)> = Vec::new();

        // Recompile local subscriptions
        for (sub_id, sub) in &mut self.subscriptions {
            let next_branches = match Self::resolve_query_branches_for_context(
                storage,
                &sub.query,
                &self.schema_context,
            ) {
                Ok(branches) => branches,
                Err(error) => {
                    tracing::warn!(
                        sub_id = sub_id.0,
                        table = %sub.query.table,
                        error = %error,
                        "failed to resolve local query branches; keeping previous branch set"
                    );
                    sub.branches.clone()
                }
            };

            if sub.needs_recompile || sub.branches != next_branches {
                match Self::compile_graph(
                    &sub.query,
                    &next_branches,
                    &self.schema,
                    sub.session.clone(),
                    &self.schema_context,
                ) {
                    Ok(new_graph) => {
                        sub.graph = new_graph;
                        sub.branches = next_branches;
                        sub.needs_recompile = false;
                    }
                    Err(err) => {
                        let reason = err.to_string();
                        tracing::error!(
                            sub_id = sub_id.0,
                            table = %sub.graph.table,
                            error = %reason,
                            "subscription stale recompile failed; dropping subscription"
                        );
                        failed_local.push((*sub_id, reason));
                    }
                }
            }
        }

        for (sub_id, reason) in failed_local {
            let propagation = self
                .subscriptions
                .remove(&sub_id)
                .map(|sub| sub.propagation)
                .unwrap_or(QueryPropagation::Full);
            self.failed_subscriptions.push(QuerySubscriptionFailure {
                subscription_id: sub_id,
                reason: reason.clone(),
            });
            if propagation == QueryPropagation::Full {
                // Keep upstream state in sync for subscriptions created via subscribe_with_sync.
                self.sync_manager
                    .send_query_unsubscription_to_servers(QueryId(sub_id.0));
            }
        }

        let mut failed_server: Vec<(ClientId, QueryId, String, QueryPropagation)> = Vec::new();

        // Recompile server-side subscriptions
        for ((client_id, query_id), sub) in &mut self.server_subscriptions {
            let compile_query = Self::query_for_server_compile(&sub.query, &sub.schema_context);
            let next_branches = match Self::resolve_query_branches_for_context(
                storage,
                &compile_query,
                &sub.schema_context,
            ) {
                Ok(branches) => branches,
                Err(error) => {
                    tracing::warn!(
                        %client_id,
                        query_id = query_id.0,
                        table = %sub.query.table,
                        error = %error,
                        "failed to resolve server query branches; keeping previous branch set"
                    );
                    sub.branches.clone()
                }
            };

            if sub.needs_recompile || sub.branches != next_branches {
                match Self::compile_graph(
                    &compile_query,
                    &next_branches,
                    &sub.schema_context.current_schema,
                    sub.session.clone(),
                    &sub.schema_context,
                ) {
                    Ok(new_graph) => {
                        sub.branches = next_branches;
                        sub.graph = new_graph;
                        sub.needs_recompile = false;
                    }
                    Err(err) => {
                        let reason = err.to_string();
                        tracing::error!(
                            %client_id,
                            query_id = query_id.0,
                            error = %reason,
                            "server subscription stale recompile failed; dropping subscription"
                        );
                        failed_server.push((*client_id, *query_id, reason, sub.propagation));
                    }
                }
            }
        }

        for (client_id, query_id, reason, propagation) in failed_server {
            self.server_subscriptions.remove(&(client_id, query_id));
            self.sync_manager
                .drop_client_query_subscription(client_id, query_id);
            if propagation == QueryPropagation::Full {
                self.sync_manager
                    .send_query_unsubscription_to_servers(query_id);
            }
            self.sync_manager.emit_query_subscription_rejected(
                client_id,
                query_id,
                format!(
                    "query recompilation failed for query_id {}: {}",
                    query_id.0, reason
                ),
            );
        }
    }

    /// Get the schema context.
    pub fn schema_context(&self) -> &SchemaContext {
        &self.schema_context
    }

    /// Get the current branch name for writes.
    pub(super) fn current_branch(&self) -> BranchName {
        assert!(
            self.schema_context.is_initialized(),
            "schema context not initialized before current_branch()"
        );
        self.schema_context.branch_name()
    }

    /// Get the schema-context branches (current + one branch per live schema).
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

    /// Get subscription results as decoded rows with ObjectIds (for testing).
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
        let _span = tracing::trace_span!("QueryManager::process").entered();

        // 1. Process SyncManager inbox (receives client writes)
        self.sync_manager.process_inbox(storage);

        // 2. Process object updates from SyncManager FIRST
        // This ensures indices are updated before query subscriptions are processed,
        // so new subscriptions can find data that arrived in the same batch.
        let updates = self.sync_manager.object_manager.take_all_object_updates();
        if !updates.is_empty() {
            tracing::debug!(count = updates.len(), "processing object updates");
        }
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
        let storage_ref: &dyn Storage = storage;
        self.recompile_stale_subscriptions(storage_ref);

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
        let dirty_count = self
            .subscriptions
            .values()
            .filter(|s| s.graph.has_dirty_nodes())
            .count();
        if dirty_count > 0 {
            tracing::debug!(
                dirty_count,
                total = self.subscriptions.len(),
                "settling subscriptions"
            );
        }
        let om = &mut self.sync_manager.object_manager;
        let schema_context = &self.schema_context;
        let branch_schema_map = &self.branch_schema_map;

        for (sub_id, subscription) in &mut self.subscriptions {
            let _sub_span = tracing::trace_span!("settle_subscription", sub_id = sub_id.0, table = %subscription.graph.table).entered();
            let branches = &subscription.branches;
            let requested_branches = Self::branch_names_for_query_branches(branches);
            let table = subscription.graph.table.as_str().to_string();
            let include_deleted = subscription.query.include_deleted;

            let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                let obj = om.get_or_load_tips(id, storage_ref, &requested_branches);
                if obj.is_none() {
                    tracing::trace!(%id, "row_loader: object not found");
                    return None;
                }
                let obj = obj?;
                let resolved = Self::resolve_latest_row_with_schema_transform(
                    id,
                    obj,
                    branches,
                    &table,
                    branch_schema_map,
                    schema_context,
                )?;
                if resolved.is_soft_deleted && !include_deleted {
                    return None;
                }

                Some(LoadedRow::new(
                    resolved.content,
                    resolved.commit_id,
                    [(id, resolved.branch_name)].into_iter().collect(),
                ))
            };

            let delta = subscription.graph.settle(storage_ref, row_loader);
            if !delta.added.is_empty() || !delta.removed.is_empty() {
                tracing::debug!(
                    sub_id = sub_id.0,
                    added = delta.added.len(),
                    removed = delta.removed.len(),
                    "settle delta"
                );
            }

            let tier_satisfied = match &subscription.durability_tier {
                None => true, // No durability requirement → immediate
                Some(required) => subscription.achieved_tiers.iter().any(|t| t >= required),
            };
            let allow_local_while_waiting = !tier_satisfied
                && subscription.settled_once
                && subscription.local_updates == LocalUpdates::Immediate
                && subscription.has_pending_local_updates;

            if !tier_satisfied && !allow_local_while_waiting {
                // Graph state updated by settle(), but don't deliver yet
                tracing::trace!("tier not satisfied, holding delivery");
                continue;
            }

            if !subscription.settled_once {
                // First delivery — full current state snapshot
                subscription.settled_once = true;
                let full_result = subscription.graph.current_result_as_delta();
                let ordered_ids_after: Vec<ObjectId> = subscription
                    .graph
                    .current_result()
                    .iter()
                    .map(|row| row.id)
                    .collect();
                let ordered = build_ordered_delta_with_post_ids(
                    &subscription.current_ordered_ids,
                    &ordered_ids_after,
                    &full_result,
                    false,
                );
                subscription.current_ordered_ids = ordered.ordered_ids_after;
                // Always emit the first snapshot once tier is satisfied, even if empty.
                // This guarantees one-shot queries can resolve to [] instead of hanging.
                tracing::debug!(
                    sub_id = sub_id.0,
                    added = full_result.added.len(),
                    "first delivery (snapshot)"
                );
                self.update_outbox.push(QueryUpdate {
                    subscription_id: *sub_id,
                    delta: full_result,
                    ordered_delta: ordered.delta,
                    descriptor: subscription.graph.combined_descriptor.clone(),
                });
                subscription.has_pending_local_updates = false;
            } else if !delta.is_empty() {
                let ordered_ids_after: Vec<ObjectId> = subscription
                    .graph
                    .current_result()
                    .iter()
                    .map(|row| row.id)
                    .collect();
                let ordered = build_ordered_delta_with_post_ids(
                    &subscription.current_ordered_ids,
                    &ordered_ids_after,
                    &delta,
                    false,
                );
                subscription.current_ordered_ids = ordered.ordered_ids_after;
                tracing::debug!(
                    sub_id = sub_id.0,
                    added = delta.added.len(),
                    removed = delta.removed.len(),
                    updated = delta.updated.len(),
                    "incremental delivery"
                );
                // Incremental delivery
                self.update_outbox.push(QueryUpdate {
                    subscription_id: *sub_id,
                    delta: delta.clone(),
                    ordered_delta: ordered.delta,
                    descriptor: subscription.graph.combined_descriptor.clone(),
                });
                subscription.has_pending_local_updates = false;
            }
        }

        // Note: With sync storage, object loading is immediate. No need to request
        // async loads - objects are available when we query for them.

        // 8. Settle server-side subscriptions and update scopes
        self.settle_server_subscriptions(storage_ref);
    }
    /// Load a row's data from a specific branch using LWW (last-writer-wins by timestamp).
    /// When timestamps tie, CommitId provides a deterministic secondary ordering.
    pub(super) fn load_row_from_object_on_branch(
        &self,
        row_id: ObjectId,
        branch_name: &str,
    ) -> Option<(Vec<u8>, CommitId)> {
        let obj = self.sync_manager.object_manager.get(row_id)?;
        let branch_name = self.resolve_branch_name(branch_name);
        let branch = obj.branches.get(&branch_name)?;
        // Sort tips by (timestamp, CommitId) ascending, take last (newest = LWW winner)
        let mut tips: Vec<_> = branch.tips.iter().copied().collect();
        tips.sort_by_key(|id| {
            (
                branch.commits.get(id).map(|c| c.timestamp).unwrap_or(0),
                *id,
            )
        });
        let tip_id = tips.last()?;
        let commit = branch.commits.get(tip_id)?;
        Some((commit.content.clone(), *tip_id))
    }

    /// Load a row's data from ObjectManager using the default branch.
    pub(super) fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        let branch = self.current_branch();
        self.load_row_from_object_on_branch(row_id, branch.as_str())
    }

    /// Load content from a catalogue object's deterministic batch branch.
    ///
    /// Used for loading schema/lens data from catalogue objects.
    pub(super) fn load_object_content(&self, object_id: ObjectId) -> Option<Vec<u8>> {
        self.load_row_from_object_on_branch(
            object_id,
            crate::schema_manager::catalogue_branch_name().as_str(),
        )
        .map(|(content, _)| content)
    }

    fn parse_schema_hash_hex(hex_str: &str) -> Option<SchemaHash> {
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(SchemaHash::from_bytes(arr))
    }

    fn catalogue_manifest_append(
        metadata: &HashMap<String, String>,
        object_id: ObjectId,
    ) -> Option<(ObjectId, CatalogueManifestOp)> {
        let app_id_str = metadata.get(MetadataKey::AppId.as_str())?;
        let app_uuid = uuid::Uuid::parse_str(app_id_str).ok()?;
        let app_id = ObjectId::from_uuid(app_uuid);

        let type_str = metadata.get(MetadataKey::Type.as_str())?;
        let op = match type_str.as_str() {
            t if t == ObjectType::CatalogueSchema.as_str() => {
                let schema_hash_hex = metadata.get(MetadataKey::SchemaHash.as_str())?;
                let schema_hash = Self::parse_schema_hash_hex(schema_hash_hex)?;
                CatalogueManifestOp::SchemaSeen {
                    object_id,
                    schema_hash,
                }
            }
            t if t == ObjectType::CatalogueLens.as_str() => {
                let source_hex = metadata.get(MetadataKey::SourceHash.as_str())?;
                let target_hex = metadata.get(MetadataKey::TargetHash.as_str())?;
                let source_hash = Self::parse_schema_hash_hex(source_hex)?;
                let target_hash = Self::parse_schema_hash_hex(target_hex)?;
                CatalogueManifestOp::LensSeen {
                    object_id,
                    source_hash,
                    target_hash,
                }
            }
            _ => return None,
        };

        Some((app_id, op))
    }

    /// Handle an object update from the global subscription.
    pub(super) fn handle_object_update(
        &mut self,
        storage: &mut dyn Storage,
        update: AllObjectUpdate,
    ) {
        // Check if this is a catalogue object (schema or lens)
        if let Some(type_str) = update.metadata.get(MetadataKey::Type.as_str())
            && (type_str == ObjectType::CatalogueSchema.as_str()
                || type_str == ObjectType::CatalogueLens.as_str())
        {
            if let Some((app_id, op)) =
                Self::catalogue_manifest_append(&update.metadata, update.object_id)
                && let Err(error) = storage.append_catalogue_manifest_op(app_id, op)
            {
                tracing::warn!(
                    object_id = %update.object_id,
                    app_id = %app_id,
                    ?error,
                    "failed to persist catalogue manifest op"
                );
            }

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
        let resolved_branch_name = self.resolve_branch_name(update.branch_name.as_str());
        let branch = resolved_branch_name.as_str();

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
                        let schema_short = composed.schema_hash.short();
                        tracing::error!(
                            object_id = %update.object_id,
                            branch = %branch,
                            schema_hash = %schema_short,
                            "buffering row update for unknown schema hash; schema not yet known"
                        );
                        // Schema not known yet - buffer for retry
                        self.pending_row_updates.push(update);
                        return;
                    }
                } else {
                    tracing::error!(
                        object_id = %update.object_id,
                        branch = %branch,
                        "buffering row update for unknown branch; cannot parse schema hash"
                    );
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
            tracing::error!(
                object_id = %update.object_id,
                branch = %branch,
                schema_hash = %schema_hash.short(),
                "buffering row update because schema for branch is not available yet"
            );
            // Schema not available - buffer for retry
            self.pending_row_updates.push(update);
            return;
        };

        let descriptor = table_schema.columns.clone();

        // Check if we have a local hard delete tombstone - if so, ignore incoming updates
        if self.is_hard_deleted(update.object_id) {
            // Hard delete is authoritative - ignore incoming updates
            return;
        }

        let Some((head_commit_id, head_commit)) =
            self.tip_commit_on_branch(update.object_id, &resolved_branch_name)
        else {
            return;
        };

        if head_commit.content.is_empty() && head_commit.is_hard_deleted() {
            let _ = self.reconcile_indices_after_hard_delete_commit(
                storage,
                &table,
                &resolved_branch_name,
                update.object_id,
                head_commit_id,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        if head_commit.is_soft_deleted() {
            let _ = self.reconcile_indices_after_soft_delete_commit(
                storage,
                &table,
                &resolved_branch_name,
                update.object_id,
                head_commit_id,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&table);
            self.mark_row_deleted_in_subscriptions(&table, update.object_id);
            return;
        }

        if let Err(error) = self.reconcile_indices_after_live_commit(
            storage,
            &table,
            &resolved_branch_name,
            update.object_id,
            head_commit_id,
            &head_commit.content,
            &descriptor,
        ) {
            tracing::error!(
                table,
                branch,
                object_id = %update.object_id,
                %error,
                "failed to reconcile indices for synced live row commit"
            );
        }

        self.mark_subscriptions_dirty(&table);
        self.mark_row_updated_in_subscriptions(&table, update.object_id);
    }
    /// Mark subscriptions dirty for a table based on update origin.
    fn mark_subscriptions_dirty_with_origin(&mut self, table: &str, local_update: bool) {
        // Mark local subscriptions dirty
        for subscription in self.subscriptions.values_mut() {
            if Self::subscription_involves_table(&subscription.graph, table) {
                subscription.graph.mark_dirty_for_table(table);
                if local_update {
                    subscription.has_pending_local_updates = true;
                }
            }
        }

        // Mark server subscriptions dirty (for downstream clients)
        for server_sub in self.server_subscriptions.values_mut() {
            if Self::subscription_involves_table(&server_sub.graph, table) {
                server_sub.graph.mark_dirty_for_table(table);
            }
        }
    }

    /// Mark subscriptions dirty from external updates (default behavior).
    ///
    /// Checks all tables involved in the subscription (including joined tables).
    /// Also marks server-side subscriptions for downstream clients.
    pub(super) fn mark_subscriptions_dirty(&mut self, table: &str) {
        self.mark_subscriptions_dirty_with_origin(table, false);
    }

    /// Mark subscriptions dirty from local writes.
    pub(super) fn mark_subscriptions_dirty_local(&mut self, table: &str) {
        self.mark_subscriptions_dirty_with_origin(table, true);
    }

    /// Mark a row as updated in all subscriptions for a table.
    /// This triggers content change detection during settle().
    /// Checks all tables involved in the subscription (including joined tables).
    pub(super) fn mark_row_updated_in_subscriptions(&mut self, table: &str, id: ObjectId) {
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
    pub(super) fn mark_row_deleted_in_subscriptions(&mut self, table: &str, id: ObjectId) {
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
    pub(super) fn subscription_involves_table(
        graph: &super::graph::QueryGraph,
        table: &str,
    ) -> bool {
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

fn propagation_label(propagation: QueryPropagation) -> &'static str {
    match propagation {
        QueryPropagation::Full => "full",
        QueryPropagation::LocalOnly => "local-only",
    }
}

fn subscription_group_key(query: &str, branches: &[QueryBranchRef], propagation: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    hasher.update([0]);
    hasher.update(propagation.as_bytes());
    hasher.update([0]);
    for branch in branches {
        hasher.update(branch.as_str().as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}
