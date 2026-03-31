use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::commit::CommitId;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::{BranchName, ObjectId};
use crate::object_manager::AllObjectUpdate;
use crate::schema_manager::{
    LensTransformer, SchemaContext, resolve_current_table_name, translate_table_name_to_schema,
};
use crate::storage::{CatalogueManifestOp, Storage};
use crate::sync_manager::{
    ClientId, DurabilityTier, PendingPermissionCheck, PendingUpdateId, QueryId, QueryPropagation,
    SchemaWarning, SyncManager,
};

use super::graph::{QueryCompileError, QueryGraph};
use super::graph_nodes::output::QuerySubscriptionId;
use super::policy::Operation;
use super::policy_graph::PolicyGraph;
use super::query::Query;
use super::session::Session;
use super::types::{
    ComposedBranchName, LoadedRow, OrderedRowDelta, Row, RowDelta, RowDescriptor, Schema,
    SchemaHash, TableName, TablePolicies, TableSchema, Value, build_ordered_delta_with_post_ids,
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
    pub(crate) branches: Vec<String>,
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
    /// Last visible rows delivered to the subscriber when explicit auth filtering is active.
    pub(crate) current_visible_rows: HashMap<ObjectId, Row>,
    /// Whether this subscription uses post-settle auth filtering instead of graph policies.
    pub(crate) uses_explicit_authorization_filtering: bool,
    /// Whether this subscription should be forwarded to upstream servers.
    pub(crate) propagation: QueryPropagation,
    /// Schema mismatch warnings already emitted for the latest settled state.
    pub(crate) reported_schema_warnings: HashSet<SchemaWarningKey>,
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
    /// Branch the write is being evaluated on.
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
    pub(super) branches: Vec<String>,
    /// Last computed scope (for detecting changes).
    pub(super) last_scope: HashSet<(ObjectId, BranchName)>,
    /// Flag indicating this subscription needs recompilation due to schema change.
    pub(super) needs_recompile: bool,
    /// Flag indicating this server subscription has settled at least once.
    /// Used to emit QuerySettled to the client on first settlement.
    pub(super) settled_once: bool,
    /// Whether this subscription should be propagated to upstream servers.
    pub(super) propagation: QueryPropagation,
    /// Schema mismatch warnings already emitted for the latest settled state.
    pub(super) reported_schema_warnings: HashSet<SchemaWarningKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SchemaWarningKey {
    pub(crate) table_name: String,
    pub(crate) from_hash: SchemaHash,
    pub(crate) to_hash: SchemaHash,
}

impl SchemaWarningKey {
    fn from_warning(warning: &SchemaWarning) -> Self {
        Self {
            table_name: warning.table_name.clone(),
            from_hash: warning.from_hash,
            to_hash: warning.to_hash,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct SchemaWarningAccumulator {
    counts: HashMap<SchemaWarningKey, usize>,
}

impl SchemaWarningAccumulator {
    pub(super) fn record(&mut self, table_name: &str, from_hash: SchemaHash, to_hash: SchemaHash) {
        let key = SchemaWarningKey {
            table_name: table_name.to_string(),
            from_hash,
            to_hash,
        };
        *self.counts.entry(key).or_default() += 1;
    }

    pub(super) fn warnings_for_query(&self, query_id: QueryId) -> Vec<SchemaWarning> {
        let mut warnings: Vec<SchemaWarning> = self
            .counts
            .iter()
            .map(|(key, row_count)| SchemaWarning {
                query_id,
                table_name: key.table_name.clone(),
                row_count: *row_count,
                from_hash: key.from_hash,
                to_hash: key.to_hash,
            })
            .collect();
        warnings.sort_by(|a, b| {
            a.table_name
                .cmp(&b.table_name)
                .then_with(|| a.from_hash.to_string().cmp(&b.from_hash.to_string()))
                .then_with(|| a.to_hash.to_string().cmp(&b.to_hash.to_string()))
        });
        warnings
    }
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
    pub(super) authorization_schema: Option<Arc<Schema>>,
    pub(super) authorization_schema_required: bool,

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
    pub(super) fn finalize_schema_warnings(
        reported: &mut HashSet<SchemaWarningKey>,
        warnings: Vec<SchemaWarning>,
    ) -> Vec<SchemaWarning> {
        let current_keys: HashSet<SchemaWarningKey> = warnings
            .iter()
            .map(SchemaWarningKey::from_warning)
            .collect();
        let new_warnings = warnings
            .into_iter()
            .filter(|warning| !reported.contains(&SchemaWarningKey::from_warning(warning)))
            .collect();
        *reported = current_keys;
        new_warnings
    }

    pub(super) fn log_schema_warning(
        warning: &SchemaWarning,
        subscription_id: Option<QuerySubscriptionId>,
    ) {
        fn short_hash(hash: &impl ToString) -> String {
            hash.to_string().chars().take(12).collect()
        }

        tracing::warn!(
            sub_id = subscription_id.map(|id| id.0),
            query_id = warning.query_id.0,
            table = warning.table_name,
            row_count = warning.row_count,
            from_hash = %warning.from_hash,
            to_hash = %warning.to_hash,
            "Detected {} rows of {} with differing schema versions. To ensure data visibility and forward/backward compatibility please create a new migration with `npx jazz-tools migrations create {} {}`",
            warning.row_count,
            warning.table_name,
            short_hash(&warning.from_hash),
            short_hash(&warning.to_hash),
        );
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
                    branches: subscription.branches.clone(),
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
            authorization_schema: None,
            authorization_schema_required: false,
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
    /// Creates indices for the current schema's branch.
    pub fn set_current_schema(&mut self, schema: Schema, env: &str, user_branch: &str) {
        self.schema_context
            .set_current(schema.clone(), env, user_branch);
        self.schema = Arc::new(schema.clone());
        self.authorization_schema = Some(Arc::new(schema.clone()));
        self.authorization_schema_required = true;

        // Update branch -> schema hash map
        let branch = self.schema_context.branch_name();
        self.branch_schema_map.insert(
            branch.as_str().to_string(),
            self.schema_context.current_hash,
        );
    }

    pub fn set_authorization_schema(&mut self, schema: Schema) {
        self.authorization_schema = Some(Arc::new(schema));
        self.authorization_schema_required = true;
        self.mark_subscriptions_for_recompile();
    }

    pub fn require_authorization_schema(&mut self) {
        self.authorization_schema_required = true;
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
    ) -> Result<QueryGraph, QueryCompileError> {
        QueryGraph::try_compile_with_schema_context(query, schema, session, schema_context)
    }

    pub(super) fn local_subscription_uses_explicit_authorization(
        &self,
        session: Option<&Session>,
    ) -> bool {
        session.is_some()
            && self
                .authorization_schema
                .as_ref()
                .map(|auth_schema| auth_schema.as_ref() != self.schema.as_ref())
                .unwrap_or(false)
    }

    pub(super) fn local_subscription_compile_schema(&self, session: Option<&Session>) -> Schema {
        if self.local_subscription_uses_explicit_authorization(session) {
            self.schema
                .iter()
                .map(|(table_name, table_schema)| {
                    let mut structural = table_schema.clone();
                    structural.policies = TablePolicies::default();
                    (*table_name, structural)
                })
                .collect()
        } else {
            self.schema.as_ref().clone()
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
        let mut failed_local: Vec<(QuerySubscriptionId, String)> = Vec::new();
        let current_schema = self.schema.clone();
        let current_schema_context = self.schema_context.clone();
        let authorization_schema = self.authorization_schema.clone();

        // Recompile local subscriptions
        for (sub_id, sub) in &mut self.subscriptions {
            if sub.needs_recompile {
                // Resolve next branches from current schema context.
                let next_branches: Vec<String> = current_schema_context
                    .all_branch_names()
                    .into_iter()
                    .map(|b| b.as_str().to_string())
                    .collect();
                let uses_explicit_authorization_filtering = sub.session.is_some()
                    && authorization_schema
                        .as_ref()
                        .map(|auth_schema| auth_schema.as_ref() != current_schema.as_ref())
                        .unwrap_or(false);
                let compile_schema = if uses_explicit_authorization_filtering {
                    current_schema
                        .iter()
                        .map(|(table_name, table_schema)| {
                            let mut structural = table_schema.clone();
                            structural.policies = TablePolicies::default();
                            (*table_name, structural)
                        })
                        .collect()
                } else {
                    current_schema.as_ref().clone()
                };

                // Recompile the graph
                match Self::compile_graph(
                    &sub.query,
                    &compile_schema,
                    sub.session.clone(),
                    &current_schema_context,
                ) {
                    Ok(new_graph) => {
                        sub.graph = new_graph;
                        sub.branches = next_branches;
                        sub.uses_explicit_authorization_filtering =
                            uses_explicit_authorization_filtering;
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
            if sub.needs_recompile {
                let query_for_compile =
                    Self::query_for_server_compile(&sub.query, &sub.schema_context);
                let compile_schema: Schema = sub
                    .schema_context
                    .current_schema
                    .iter()
                    .map(|(table_name, table_schema)| {
                        let mut structural = table_schema.clone();
                        structural.policies = TablePolicies::default();
                        (*table_name, structural)
                    })
                    .collect();
                // Recompile the graph
                match Self::compile_graph(
                    &query_for_compile,
                    &compile_schema,
                    sub.session.clone(),
                    &sub.schema_context,
                ) {
                    Ok(new_graph) => {
                        sub.branches = Self::resolved_server_query_branches(
                            &query_for_compile,
                            &sub.schema_context,
                        );
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
        let storage_ref: &dyn Storage = storage;
        let schema_context = self.schema_context.clone();
        let branch_schema_map = self.branch_schema_map.clone();
        let subscription_ids: Vec<_> = self.subscriptions.keys().copied().collect();

        for sub_id in subscription_ids {
            let Some(mut subscription) = self.subscriptions.remove(&sub_id) else {
                continue;
            };

            let _sub_span = tracing::trace_span!("settle_subscription", sub_id = sub_id.0, table = %subscription.graph.table).entered();
            let branches = subscription.branches.clone();
            let table = subscription.graph.table.as_str().to_string();
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let include_deleted = subscription.query.include_deleted;

            // Row loader returns None for empty content (hard delete tombstones)
            // Soft deletes have preserved content and can be materialized normally
            // For single-branch subscriptions, reads from that branch
            // For multi-branch subscriptions, uses LWW across branches
            // When schema context is present, applies lens transform for old schema branches
            let delta = {
                let om = &mut self.sync_manager.object_manager;
                let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                    let obj = om.get_or_load(id, storage_ref, &branches);
                    if obj.is_none() {
                        tracing::trace!(%id, "row_loader: object not found");
                        return None;
                    }
                    let obj = obj?;
                    let mut best: Option<(u64, CommitId, Vec<u8>, String, bool)> = None;

                    for branch_name in &branches {
                        if let Some(branch) = obj.branches.get(&BranchName::new(branch_name)) {
                            for &tip_id in &branch.tips {
                                if let Some(commit) = branch.commits.get(&tip_id) {
                                    let is_soft_deleted = commit.is_soft_deleted();
                                    match &best {
                                        None => {
                                            best = Some((
                                                commit.timestamp,
                                                tip_id,
                                                commit.content.clone(),
                                                branch_name.clone(),
                                                is_soft_deleted,
                                            ));
                                        }
                                        Some((best_ts, best_id, _, _, _))
                                            if (commit.timestamp, tip_id)
                                                > (*best_ts, *best_id) =>
                                        {
                                            best = Some((
                                                commit.timestamp,
                                                tip_id,
                                                commit.content.clone(),
                                                branch_name.clone(),
                                                is_soft_deleted,
                                            ));
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }

                    let (_, commit_id, content, source_branch, is_soft_deleted) =
                        best.filter(|(_, _, content, _, _)| !content.is_empty())?;

                    if let Some(&source_hash) = branch_schema_map.get(&source_branch)
                        && source_hash != schema_context.current_hash
                    {
                        let transformer = LensTransformer::new(&schema_context, &table);
                        match transformer.transform(&content, commit_id, source_hash) {
                            Ok(result) => {
                                if is_soft_deleted && !include_deleted {
                                    return None;
                                }
                                return Some(LoadedRow::new(
                                    result.data,
                                    commit_id,
                                    [(id, BranchName::new(&source_branch))]
                                        .into_iter()
                                        .collect(),
                                ));
                            }
                            Err(err) => {
                                schema_warnings.record(
                                    &table,
                                    source_hash,
                                    schema_context.current_hash,
                                );
                                tracing::debug!(
                                    sub_id = sub_id.0,
                                    row_id = %id,
                                    table = %table,
                                    source_branch = %source_branch,
                                    source_schema = %source_hash.short(),
                                    target_schema = %schema_context.current_hash.short(),
                                    error = %err,
                                    "lens transform failed; row will be counted in aggregated schema warning"
                                );
                                return None;
                            }
                        }
                    }

                    if is_soft_deleted && !include_deleted {
                        return None;
                    }

                    Some(LoadedRow::new(
                        content,
                        commit_id,
                        [(id, BranchName::new(&source_branch))]
                            .into_iter()
                            .collect(),
                    ))
                };

                subscription.graph.settle(storage_ref, row_loader)
            };
            let new_schema_warnings = Self::finalize_schema_warnings(
                &mut subscription.reported_schema_warnings,
                schema_warnings.warnings_for_query(QueryId(sub_id.0)),
            );
            for warning in &new_schema_warnings {
                Self::log_schema_warning(warning, Some(sub_id));
            }
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
                self.subscriptions.insert(sub_id, subscription);
                continue;
            }

            if subscription.uses_explicit_authorization_filtering {
                let visible_rows = self.authorized_rows_from_graph(
                    storage_ref,
                    &subscription.graph,
                    &schema_context,
                    &branch_schema_map,
                    subscription.session.as_ref(),
                );
                let visible_rows_by_id: HashMap<_, _> = visible_rows
                    .iter()
                    .cloned()
                    .map(|row| (row.id, row))
                    .collect();
                let visible_delta = Self::row_delta_from_rows(
                    &subscription.current_visible_rows,
                    &subscription.current_ordered_ids,
                    &visible_rows,
                );
                let ordered_ids_after: Vec<ObjectId> =
                    visible_rows.iter().map(|row| row.id).collect();

                if !subscription.settled_once {
                    subscription.settled_once = true;
                    let ordered = build_ordered_delta_with_post_ids(
                        &subscription.current_ordered_ids,
                        &ordered_ids_after,
                        &visible_delta,
                        false,
                    );
                    subscription.current_ordered_ids = ordered.ordered_ids_after;
                    subscription.current_visible_rows = visible_rows_by_id;
                    tracing::debug!(
                        sub_id = sub_id.0,
                        added = visible_delta.added.len(),
                        "first delivery (snapshot)"
                    );
                    self.update_outbox.push(QueryUpdate {
                        subscription_id: sub_id,
                        delta: visible_delta,
                        ordered_delta: ordered.delta,
                        descriptor: subscription.graph.combined_descriptor.clone(),
                    });
                    subscription.has_pending_local_updates = false;
                } else if !visible_delta.is_empty() {
                    let ordered = build_ordered_delta_with_post_ids(
                        &subscription.current_ordered_ids,
                        &ordered_ids_after,
                        &visible_delta,
                        false,
                    );
                    subscription.current_ordered_ids = ordered.ordered_ids_after;
                    subscription.current_visible_rows = visible_rows_by_id;
                    tracing::debug!(
                        sub_id = sub_id.0,
                        added = visible_delta.added.len(),
                        removed = visible_delta.removed.len(),
                        updated = visible_delta.updated.len(),
                        "incremental delivery"
                    );
                    self.update_outbox.push(QueryUpdate {
                        subscription_id: sub_id,
                        delta: visible_delta,
                        ordered_delta: ordered.delta,
                        descriptor: subscription.graph.combined_descriptor.clone(),
                    });
                    subscription.has_pending_local_updates = false;
                }
            } else {
                subscription.current_visible_rows.clear();
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
                        subscription_id: sub_id,
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
                        subscription_id: sub_id,
                        delta: delta.clone(),
                        ordered_delta: ordered.delta,
                        descriptor: subscription.graph.combined_descriptor.clone(),
                    });
                    subscription.has_pending_local_updates = false;
                }
            }

            self.subscriptions.insert(sub_id, subscription);
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
        let branch = obj.branches.get(&BranchName::new(branch_name))?;
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
        self.load_row_from_object_on_branch(row_id, &self.current_branch())
    }

    /// Load content from a catalogue object's "main" branch.
    ///
    /// Used for loading schema/lens data from catalogue objects.
    pub(super) fn load_object_content(&self, object_id: ObjectId) -> Option<Vec<u8>> {
        self.load_row_from_object_on_branch(object_id, "main")
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
            t if t == ObjectType::CataloguePermissions.as_str() => {
                let schema_hash_hex = metadata.get(MetadataKey::SchemaHash.as_str())?;
                let schema_hash = Self::parse_schema_hash_hex(schema_hash_hex)?;
                CatalogueManifestOp::PermissionsSeen {
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
            && ObjectType::is_catalogue_type_str(type_str)
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
        let table_hint = match update.metadata.get(MetadataKey::Table.as_str()) {
            Some(t) => t.clone(),
            None => return,
        };
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

        let current_table = resolve_current_table_name(&self.schema_context, &table_hint)
            .unwrap_or_else(|| table_hint.clone());
        let branch_table = if schema_hash == self.schema_context.current_hash {
            current_table.clone()
        } else {
            translate_table_name_to_schema(&self.schema_context, &current_table, &schema_hash)
                .unwrap_or_else(|| table_hint.clone())
        };
        let table_name = TableName::new(&branch_table);

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
        let has_prior_history = !update.previous_commit_ids.is_empty();

        // Check if we have a local hard delete tombstone - if so, ignore incoming updates
        if self.is_hard_deleted(update.object_id) {
            // Hard delete is authoritative - ignore incoming updates
            return;
        }

        // Check if incoming update is a hard delete
        if self.is_incoming_hard_delete(update.object_id) {
            let old_data = if has_prior_history {
                Some(update.old_content.as_deref().unwrap_or_else(|| {
                    panic!(
                        "missing old_content for historical sync update (hard delete): object_id={}, table={}, branch={}, prev_tips={}, commit_ids={}",
                        update.object_id,
                        branch_table,
                        branch,
                        update.previous_commit_ids.len(),
                        update.commit_ids.len(),
                    )
                }))
            } else {
                update.old_content.as_deref()
            };
            // Apply hard delete unconditionally
            let _ = Self::update_indices_for_hard_delete_on_branch(
                storage,
                &branch_table,
                branch,
                update.object_id,
                old_data,
                &descriptor,
            );
            self.mark_subscriptions_dirty(&branch_table);
            self.mark_row_deleted_in_subscriptions(&branch_table, update.object_id);
            return;
        }

        // Check if incoming update is a soft delete
        if self.is_soft_delete_commit(update.object_id) {
            let old_data = if has_prior_history {
                Some(update.old_content.as_deref().unwrap_or_else(|| {
                    panic!(
                        "missing old_content for historical sync update (soft delete): object_id={}, table={}, branch={}, prev_tips={}, commit_ids={}",
                        update.object_id,
                        branch_table,
                        branch,
                        update.previous_commit_ids.len(),
                        update.commit_ids.len(),
                    )
                }))
            } else {
                update.old_content.as_deref()
            };

            // Apply soft delete - remove from _id and column indices, add to _id_deleted
            if let Some(old_data) = old_data {
                let _ = Self::update_indices_for_soft_delete_on_branch(
                    storage,
                    &branch_table,
                    branch,
                    update.object_id,
                    old_data,
                    &descriptor,
                );
            } else {
                // No old content - just remove from _id and add to _id_deleted
                let _ = storage.index_remove(
                    &branch_table,
                    "_id",
                    branch,
                    &Value::Uuid(update.object_id),
                    update.object_id,
                );
                if let Err(error) = storage.index_insert(
                    &branch_table,
                    "_id_deleted",
                    branch,
                    &Value::Uuid(update.object_id),
                    update.object_id,
                ) {
                    tracing::error!(
                        table = branch_table,
                        branch,
                        object_id = %update.object_id,
                        %error,
                        "failed to insert synced _id_deleted index entry"
                    );
                }
            }
            self.mark_subscriptions_dirty(&branch_table);
            self.mark_row_deleted_in_subscriptions(&branch_table, update.object_id);
            return;
        }

        // Check if this is an undelete (non-empty content for previously soft-deleted row)
        let was_soft_deleted =
            self.row_is_deleted_on_branch(storage, &branch_table, branch, update.object_id);

        // Extract current (new) data from the object on this branch
        let new_data = match self.load_row_from_object_on_branch(update.object_id, branch) {
            Some((data, _)) => data,
            None => return,
        };

        if was_soft_deleted {
            // This is an undelete - remove from _id_deleted, add to _id and column indices
            if let Err(error) = Self::update_indices_for_undelete_on_branch(
                storage,
                &branch_table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            ) {
                tracing::error!(
                    table = branch_table,
                    branch,
                    object_id = %update.object_id,
                    %error,
                    "failed to update indices for synced undelete"
                );
            }
            self.mark_subscriptions_dirty(&branch_table);
            return;
        }

        // Normal update handling
        if update.is_new_object || update.previous_commit_ids.is_empty() {
            // First commit on branch (new object or synced first commit) - insert into all indices
            if let Err(error) = Self::update_indices_for_insert_on_branch(
                storage,
                &branch_table,
                branch,
                update.object_id,
                &new_data,
                &descriptor,
            ) {
                tracing::error!(
                    table = branch_table,
                    branch,
                    object_id = %update.object_id,
                    %error,
                    "failed to update indices for synced insert"
                );
            }
        } else if let Some(old_data) = update.old_content {
            // Synced update - compute index delta using old_content
            // TODO: Future merge strategies - currently last-writer-wins by timestamp
            if let Err(error) = Self::update_indices_for_update_on_branch(
                storage,
                &branch_table,
                branch,
                update.object_id,
                &old_data,
                &new_data,
                &descriptor,
            ) {
                tracing::error!(
                    table = branch_table,
                    branch,
                    object_id = %update.object_id,
                    %error,
                    "failed to update indices for synced update"
                );
            }
        } else {
            panic!(
                "missing old_content for historical sync update: object_id={}, table={}, branch={}, prev_tips={}, commit_ids={}",
                update.object_id,
                branch_table,
                branch,
                update.previous_commit_ids.len(),
                update.commit_ids.len(),
            );
        }

        self.mark_subscriptions_dirty(&branch_table);
        self.mark_row_updated_in_subscriptions(&branch_table, update.object_id);
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

    pub(super) fn row_delta_from_rows(
        previous_rows: &HashMap<ObjectId, Row>,
        previous_order: &[ObjectId],
        next_rows: &[Row],
    ) -> RowDelta {
        let next_rows_by_id: HashMap<_, _> =
            next_rows.iter().cloned().map(|row| (row.id, row)).collect();
        let previous_indices: HashMap<_, _> = previous_order
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect();
        let next_indices: HashMap<_, _> = next_rows
            .iter()
            .enumerate()
            .map(|(index, row)| (row.id, index))
            .collect();

        let added = next_rows
            .iter()
            .filter(|row| !previous_rows.contains_key(&row.id))
            .cloned()
            .collect();
        let removed = previous_order
            .iter()
            .filter_map(|id| previous_rows.get(id))
            .filter(|row| !next_rows_by_id.contains_key(&row.id))
            .cloned()
            .collect();
        let updated = next_rows
            .iter()
            .filter_map(|row| {
                previous_rows.get(&row.id).and_then(|previous| {
                    (previous.data != row.data || previous.commit_id != row.commit_id)
                        .then(|| (previous.clone(), row.clone()))
                })
            })
            .collect();
        let moved = next_rows
            .iter()
            .filter(|row| {
                previous_rows.contains_key(&row.id)
                    && previous_rows
                        .get(&row.id)
                        .map(|previous| {
                            previous.data == row.data && previous.commit_id == row.commit_id
                        })
                        .unwrap_or(false)
                    && previous_indices.get(&row.id) != next_indices.get(&row.id)
            })
            .map(|row| row.id)
            .collect();

        RowDelta {
            added,
            removed,
            moved,
            updated,
        }
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
            policy_checks += state.branch.as_str().len();
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

fn subscription_group_key(query: &str, branches: &[String], propagation: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    hasher.update([0]);
    hasher.update(propagation.as_bytes());
    hasher.update([0]);
    for branch in branches {
        hasher.update(branch.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}
