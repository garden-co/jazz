use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::commit::CommitId;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::AllObjectUpdate;
use crate::schema_manager::{LensTransformer, SchemaContext};
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, PendingPermissionCheck, PendingUpdateId, PersistenceTier, QueryId, SyncManager,
};

use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::policy::Operation;
use super::policy_graph::PolicyGraph;
use super::query::Query;
use super::session::Session;
use super::types::{
    ComposedBranchName, RowDelta, RowDescriptor, Schema, SchemaHash, TableName, TableSchema, Value,
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
/// Wrappers (groove-runtime, jazz-wasm) use this to fulfill
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
pub(super) struct PolicyCheckState {
    /// Policy graphs that need to settle.
    pub(super) graphs: Vec<PolicyGraph>,
    /// Table name for error messages.
    pub(super) table: TableName,
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
    /// Client's session for permission evaluation.
    pub(super) session: Option<Session>,
    /// Resolved branches (from query.branches or schema context at creation time).
    pub(super) branches: Vec<String>,
    /// Last computed scope (for detecting changes).
    pub(super) last_scope: HashSet<(ObjectId, BranchName)>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    pub(super) needs_recompile: bool,
    /// Flag indicating this server subscription has settled at least once.
    /// Used to emit QuerySettled to the client on first settlement.
    pub(super) settled_once: bool,
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
    pub(super) known_schemas: HashMap<SchemaHash, Schema>,
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

    pub(super) fn compile_graph(
        query: &Query,
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<QueryGraph> {
        QueryGraph::compile_with_schema_context(query, schema, session, schema_context)
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
                let new_graph = Self::compile_graph(
                    &sub.query,
                    &self.schema,
                    sub.session.clone(),
                    &self.schema_context,
                );
                if let Some(new_graph) = new_graph {
                    sub.graph = new_graph;
                }
                sub.needs_recompile = false;
            }
        }

        // Recompile server-side subscriptions
        for sub in self.server_subscriptions.values_mut() {
            if sub.needs_recompile {
                // Recompile the graph
                let new_graph = Self::compile_graph(
                    &sub.query,
                    &self.schema,
                    sub.session.clone(),
                    &self.schema_context,
                );
                if let Some(new_graph) = new_graph {
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
    pub(super) fn current_branch(&self) -> String {
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
        let storage_ref: &dyn Storage = storage;
        let schema_context = &self.schema_context;
        let branch_schema_map = &self.branch_schema_map;

        for (sub_id, subscription) in &mut self.subscriptions {
            let _sub_span = tracing::trace_span!("settle_subscription", sub_id = sub_id.0, table = %subscription.graph.table).entered();
            let branches = &subscription.branches;
            let table = subscription.graph.table.as_str().to_string();

            // Row loader returns None for empty content (hard delete tombstones)
            // Soft deletes have preserved content and can be materialized normally
            // For single-branch subscriptions, reads from that branch
            // For multi-branch subscriptions, uses LWW across branches
            // When schema context is present, applies lens transform for old schema branches
            let row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
                let obj = om.get_or_load(id, storage_ref, branches);
                if obj.is_none() {
                    tracing::trace!(%id, "row_loader: object not found");
                    return None;
                }
                let obj = obj?;
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
            if !delta.added.is_empty() || !delta.removed.is_empty() {
                tracing::debug!(
                    sub_id = sub_id.0,
                    added = delta.added.len(),
                    removed = delta.removed.len(),
                    "settle delta"
                );
            }

            let tier_satisfied = match &subscription.settled_tier {
                None => true, // No tier requirement → immediate (current behavior)
                Some(required) => subscription.achieved_tiers.iter().any(|t| t >= required),
            };

            if !tier_satisfied {
                // Graph state updated by settle(), but don't deliver yet
                tracing::trace!("tier not satisfied, holding delivery");
                continue;
            }

            if !subscription.settled_once {
                // First delivery — full current state snapshot
                subscription.settled_once = true;
                let full_result = subscription.graph.current_result_as_delta();
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
                    descriptor: subscription.graph.combined_descriptor.clone(),
                });
            } else if !delta.is_empty() {
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
    /// Load a row's data from a specific branch using LWW (last-writer-wins by timestamp).
    /// When multiple concurrent tips exist, returns content from the tip with highest timestamp.
    pub(super) fn load_row_from_object_on_branch(
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
    pub(super) fn load_row_from_object(&self, row_id: ObjectId) -> Option<(Vec<u8>, CommitId)> {
        self.load_row_from_object_on_branch(row_id, &self.current_branch())
    }

    /// Load content from a catalogue object's "main" branch.
    ///
    /// Used for loading schema/lens data from catalogue objects.
    pub(super) fn load_object_content(&self, object_id: ObjectId) -> Option<Vec<u8>> {
        self.load_row_from_object_on_branch(object_id, "main")
            .map(|(content, _)| content)
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
    /// Mark subscriptions dirty for a table.
    /// Checks all tables involved in the subscription (including joined tables).
    /// Also marks server-side subscriptions for downstream clients.
    pub(super) fn mark_subscriptions_dirty(&mut self, table: &str) {
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
