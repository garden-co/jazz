use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::object::ObjectId;
use crate::row_histories::RowVisibilityChange;
use crate::storage::Storage;
use crate::sync_manager::{DurabilityTier, QueryId, ServerId};
use crate::sync_manager::{OutgoingQuerySubscription, QueryPropagation};

#[cfg(test)]
use super::encoding::decode_row;
use super::graph_nodes::output::QuerySubscriptionId;
use super::manager::{
    CatalogueUpdate, LocalUpdates, QueryError, QueryManager, QuerySubscription,
    QuerySubscriptionFailure, QueryUpdate,
};
use super::query::{Query, QueryBuilder};
use super::session::Session;
#[cfg(test)]
use super::types::Value;
use super::types::{ComposedBranchName, Schema, SchemaHash};

type ReplayableQuerySubscription = (
    QueryId,
    Query,
    Option<Session>,
    Option<DurabilityTier>,
    QueryPropagation,
    Vec<String>,
);

pub(crate) struct SubscriptionExecutionOptions {
    pub(crate) local_updates: LocalUpdates,
    pub(crate) propagation: QueryPropagation,
    pub(crate) local_overlay_rows: HashMap<ObjectId, crate::sync_manager::RowBatchKey>,
}

impl QueryManager {
    pub(crate) fn policy_context_tables_for_graph(graph: &super::graph::QueryGraph) -> Vec<String> {
        let mut tables: Vec<String> = graph
            .policy_filter_tables
            .iter()
            .map(|(_, table)| table.as_str().to_string())
            .collect();
        tables.sort();
        tables.dedup();
        tables
    }

    fn should_send_local_subscription_upstream(&self, propagation: QueryPropagation) -> bool {
        propagation == QueryPropagation::Full || !self.sync_manager.has_durability_identity()
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
    /// `durability_tier`: If Some, holds non-local delivery until the tier confirms.
    pub fn subscribe_with_session(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_session_with_local_updates(
            query,
            session,
            durability_tier,
            LocalUpdates::Immediate,
        )
    }

    /// Subscribe with explicit local update behavior while waiting for durability.
    pub fn subscribe_with_session_with_local_updates(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        local_updates: LocalUpdates,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_session_and_propagation(
            query,
            session,
            durability_tier,
            SubscriptionExecutionOptions {
                local_updates,
                propagation: QueryPropagation::Full,
                local_overlay_rows: HashMap::new(),
            },
        )
    }

    fn subscribe_with_session_and_propagation(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        options: SubscriptionExecutionOptions,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let SubscriptionExecutionOptions {
            local_updates,
            propagation,
            local_overlay_rows,
        } = options;
        let _span =
            tracing::debug_span!("QM::subscribe", table = %query.table, ?durability_tier).entered();
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

        let uses_explicit_authorization_filtering =
            self.local_subscription_uses_explicit_authorization(session.as_ref());
        let compile_schema = self.local_subscription_compile_schema(session.as_ref());
        let compile_row_policy_mode = if uses_explicit_authorization_filtering {
            crate::query_manager::types::RowPolicyMode::PermissiveLocal
        } else {
            self.row_policy_mode
        };
        let graph = Self::compile_graph(
            &query,
            &compile_schema,
            session.clone(),
            &self.schema_context,
            compile_row_policy_mode,
        )
        .map_err(|err| QueryError::QueryCompilationError(err.to_string()))?;
        let policy_context_tables = Self::policy_context_tables_for_graph(&graph);

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        let query_frontier_settled_tier = (durability_tier.is_none()
            || !self.should_send_local_subscription_upstream(propagation)
            || !self.sync_manager.has_servers_or_pending_servers())
        .then_some(DurabilityTier::GlobalServer);
        tracing::debug!(
            sub_id = id.0,
            ?branches,
            node_count = graph.nodes.len(),
            "subscription created"
        );
        self.subscriptions.insert(
            id,
            QuerySubscription {
                query,
                graph,
                branches,
                session,
                needs_recompile: false,
                settled_once: false,
                needs_visibility_recompute: true,
                durability_tier,
                local_updates,
                has_pending_local_updates: false,
                pending_local_row_ids: HashSet::new(),
                local_overlay_rows,
                query_frontier_settled_tier,
                current_ordered_ids: Vec::new(),
                current_visible_rows: HashMap::new(),
                policy_context_tables,
                uses_explicit_authorization_filtering,
                sync_backed: false,
                propagation,
                reported_schema_warnings: HashSet::new(),
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
        schema_context: &crate::schema_manager::SchemaContext,
        session: Option<Session>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let compile_schema: Schema = schema
            .iter()
            .map(|(table_name, table_schema)| {
                let mut structural = table_schema.clone();
                structural.policies = crate::query_manager::types::TablePolicies::default();
                (*table_name, structural)
            })
            .collect();

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
        let graph = Self::compile_graph(
            &query,
            &compile_schema,
            session.clone(),
            schema_context,
            crate::query_manager::types::RowPolicyMode::PermissiveLocal,
        )
        .map_err(|err| QueryError::QueryCompilationError(err.to_string()))?;
        let policy_context_tables = Self::policy_context_tables_for_graph(&graph);

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                query,
                graph,
                branches,
                session,
                needs_recompile: false,
                settled_once: false,
                needs_visibility_recompute: true,
                durability_tier: None,
                local_updates: LocalUpdates::Immediate,
                has_pending_local_updates: false,
                pending_local_row_ids: HashSet::new(),
                local_overlay_rows: HashMap::new(),
                query_frontier_settled_tier: Some(DurabilityTier::GlobalServer),
                current_ordered_ids: Vec::new(),
                current_visible_rows: HashMap::new(),
                policy_context_tables,
                uses_explicit_authorization_filtering: false,
                sync_backed: false,
                propagation: QueryPropagation::Full,
                reported_schema_warnings: HashSet::new(),
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
    /// Servers will evaluate the query against their data and send row-batch
    /// messages for matching rows plus legacy object updates for non-row
    /// objects. As new objects match the query on the server, they are
    /// automatically synced.
    ///
    /// The returned QuerySubscriptionId is used both locally and in the sync protocol.
    pub fn subscribe_with_sync(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_sync_with_local_updates(
            query,
            session,
            durability_tier,
            LocalUpdates::Immediate,
        )
    }

    pub fn subscribe_with_sync_with_local_updates(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        local_updates: LocalUpdates,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_sync_and_propagation_with_local_updates(
            query,
            session,
            durability_tier,
            local_updates,
            QueryPropagation::Full,
        )
    }

    /// Subscribe to query results and configure upstream propagation.
    pub fn subscribe_with_sync_and_propagation(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        propagation: QueryPropagation,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_sync_and_propagation_with_local_updates(
            query,
            session,
            durability_tier,
            LocalUpdates::Immediate,
            propagation,
        )
    }

    pub(crate) fn subscribe_with_sync_and_propagation_with_local_updates(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        local_updates: LocalUpdates,
        propagation: QueryPropagation,
    ) -> Result<QuerySubscriptionId, QueryError> {
        self.subscribe_with_sync_and_propagation_with_local_overlay(
            query,
            session,
            durability_tier,
            SubscriptionExecutionOptions {
                local_updates,
                propagation,
                local_overlay_rows: HashMap::new(),
            },
        )
    }

    pub(crate) fn subscribe_with_sync_and_propagation_with_local_overlay(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        options: SubscriptionExecutionOptions,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let overlay_row_ids: HashSet<_> = options.local_overlay_rows.keys().copied().collect();
        let propagation = options.propagation;
        let sub_id = self.subscribe_with_session_and_propagation(
            query.clone(),
            session.clone(),
            durability_tier,
            options,
        )?;

        if let Some(subscription) = self.subscriptions.get_mut(&sub_id) {
            subscription.sync_backed = true;
            if !overlay_row_ids.is_empty() {
                subscription.pending_local_row_ids.extend(overlay_row_ids);
                subscription.has_pending_local_updates = true;
            } else if subscription.local_updates == LocalUpdates::Immediate
                && !self.pending_local_row_batches.is_empty()
            {
                subscription
                    .pending_local_row_ids
                    .extend(self.pending_local_row_batches.keys().copied());
                subscription.has_pending_local_updates = true;
            }
        }

        let sync_query = self.sync_query_payload_for_upstream(&query);

        // Send QuerySubscription to connected servers/tiers.
        // local-only still needs to reach the directly connected storage tier
        // (e.g. worker OPFS), and will be prevented from forwarding upstream.
        if self.should_send_local_subscription_upstream(propagation) {
            let query_id = QueryId(sub_id.0);
            let policy_context_tables = self
                .subscriptions
                .get(&sub_id)
                .map(|subscription| subscription.policy_context_tables.clone())
                .unwrap_or_default();
            self.sync_manager.send_query_subscription_to_servers(
                query_id,
                sync_query,
                session,
                durability_tier,
                propagation,
                policy_context_tables,
            );
        }

        Ok(sub_id)
    }

    pub fn add_server_with_storage<H: Storage>(
        &mut self,
        storage: &H,
        server_id: ServerId,
        skip_catalogue_sync: bool,
    ) {
        self.sync_manager
            .add_server_with_storage(server_id, skip_catalogue_sync, storage);
        self.replay_active_query_subscriptions_to_server(server_id);
    }

    /// Unsubscribe from a synced query.
    ///
    /// This method:
    /// 1. Removes the local subscription
    /// 2. Sends a QueryUnsubscription to all connected servers
    pub fn unsubscribe_with_sync(&mut self, id: QuerySubscriptionId) {
        let propagation = self
            .subscriptions
            .get(&id)
            .map(|sub| sub.propagation)
            .unwrap_or(QueryPropagation::Full);
        self.subscriptions.remove(&id);

        if self.should_send_local_subscription_upstream(propagation) {
            let query_id = QueryId(id.0);
            self.sync_manager
                .send_query_unsubscription_to_servers(query_id);
        }
    }

    /// Build the sync payload query for upstream forwarding.
    ///
    /// If branches are not explicitly set, upstream expects the current write branch
    /// to be included so it can resolve schema context correctly.
    fn sync_query_payload_for_upstream(&self, query: &Query) -> Query {
        let mut sync_query = query.clone();
        if sync_query.branches.is_empty() && self.schema_context.is_initialized() {
            sync_query.branches = vec![self.schema_context.branch_name().as_str().to_string()];
        }
        sync_query
    }

    /// Replay all currently active local and downstream query subscriptions
    /// to a newly added upstream server.
    fn replay_active_query_subscriptions_to_server(&mut self, server_id: ServerId) {
        let local_subs: Vec<ReplayableQuerySubscription> = self
            .subscriptions
            .iter()
            .map(|(sub_id, sub)| {
                (
                    QueryId(sub_id.0),
                    self.sync_query_payload_for_upstream(&sub.query),
                    sub.session.clone(),
                    sub.durability_tier,
                    sub.propagation,
                    sub.policy_context_tables.clone(),
                )
            })
            .collect();

        for (query_id, query, session, required_tier, propagation, policy_context_tables) in
            local_subs
        {
            if self
                .sync_manager
                .consume_pending_query_subscription_marker(server_id, query_id)
            {
                continue;
            }
            if self.should_send_local_subscription_upstream(propagation) {
                self.sync_manager.send_query_subscription_to_server(
                    server_id,
                    OutgoingQuerySubscription {
                        query_id,
                        query,
                        session,
                        required_tier,
                        propagation,
                        policy_context_tables,
                    },
                );
            }
        }

        let downstream_subs: Vec<ReplayableQuerySubscription> = self
            .server_subscriptions
            .iter()
            .filter(|(_, sub)| sub.propagation == QueryPropagation::Full)
            .map(|((_, query_id), sub)| {
                (
                    *query_id,
                    sub.query.clone(),
                    sub.session.clone(),
                    sub.required_tier,
                    sub.propagation,
                    sub.policy_context_tables.clone(),
                )
            })
            .collect();

        for (query_id, query, session, required_tier, propagation, policy_context_tables) in
            downstream_subs
        {
            if self
                .sync_manager
                .consume_pending_query_subscription_marker(server_id, query_id)
            {
                continue;
            }
            self.sync_manager.send_query_subscription_to_server(
                server_id,
                OutgoingQuerySubscription {
                    query_id,
                    query,
                    session,
                    required_tier,
                    propagation,
                    policy_context_tables,
                },
            );
        }
    }

    /// Take pending query updates.
    pub fn take_updates(&mut self) -> Vec<QueryUpdate> {
        std::mem::take(&mut self.update_outbox)
    }

    /// Take terminal local subscription failures.
    pub fn take_failed_subscriptions(&mut self) -> Vec<QuerySubscriptionFailure> {
        std::mem::take(&mut self.failed_subscriptions)
    }

    /// Take pending catalogue updates (schemas/lenses received via sync).
    ///
    /// SchemaManager should call this to process new schemas and lenses
    /// discovered through catalogue sync.
    pub fn take_pending_catalogue_updates(&mut self) -> Vec<CatalogueUpdate> {
        std::mem::take(&mut self.pending_catalogue_updates)
    }

    /// Retry processing buffered row visibility changes.
    ///
    /// Call this after activating new schemas (via try_activate_pending_schemas)
    /// and updating the schema context (via sync_context). Rows that arrived
    /// before their schema was known will be reprocessed.
    pub fn retry_pending_row_visibility_changes(&mut self, storage: &mut dyn Storage) {
        let pending = std::mem::take(&mut self.pending_row_visibility_changes);
        for update in pending {
            self.handle_row_update(storage, update);
        }
    }

    /// Take all pending row visibility changes (used by sync_context to preserve across rebuild).
    pub fn take_pending_row_visibility_changes(&mut self) -> Vec<RowVisibilityChange> {
        std::mem::take(&mut self.pending_row_visibility_changes)
    }

    /// Restore pending row visibility changes (used by sync_context after rebuild).
    pub fn restore_pending_row_visibility_changes(&mut self, updates: Vec<RowVisibilityChange>) {
        self.pending_row_visibility_changes = updates;
    }

    /// Set known schemas for server-mode operation.
    ///
    /// Called by SchemaManager.process() to sync the known_schemas map.
    /// This enables lazy branch activation when rows arrive with unknown branches.
    pub fn set_known_schemas(&mut self, schemas: Arc<HashMap<SchemaHash, Schema>>) {
        self.known_schemas = schemas;
        self.authorization_context_cache.clear();
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
    /// Returns the full SchemaHash if found. The partial hash has the first 6 bytes
    /// filled with the short hash, and the rest zeroed (as produced by ComposedBranchName::parse).
    pub(super) fn find_schema_by_short_hash(&self, partial: &SchemaHash) -> Option<SchemaHash> {
        let target_short = &partial.0[..6];

        if &self.schema_context.current_hash.0[..6] == target_short {
            return Some(self.schema_context.current_hash);
        }

        for &full_hash in self.schema_context.live_schemas.keys() {
            if &full_hash.0[..6] == target_short {
                return Some(full_hash);
            }
        }

        // Search known_schemas for matching short hash
        for full_hash in self.known_schemas.keys() {
            if &full_hash.0[..6] == target_short {
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
        let rows: Vec<_> = if subscription.uses_explicit_authorization_filtering {
            subscription
                .current_ordered_ids
                .iter()
                .filter_map(|id| subscription.current_visible_rows.get(id))
                .cloned()
                .collect()
        } else {
            subscription.graph.current_result()
        };

        rows.iter()
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
            .map(|sub| sub.graph.sync_scope_object_ids())
            .unwrap_or_default()
    }
}
