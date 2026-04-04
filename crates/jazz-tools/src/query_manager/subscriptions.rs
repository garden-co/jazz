use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::object_manager::AllObjectUpdate;
use crate::storage::Storage;
use crate::sync_manager::QueryPropagation;
use crate::sync_manager::{DurabilityTier, QueryId, ServerId};

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
use super::types::{Schema, SchemaHash};
#[cfg(test)]
use crate::object::ObjectId;

impl QueryManager {
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
            local_updates,
            QueryPropagation::Full,
        )
    }

    fn subscribe_with_session_and_propagation(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        local_updates: LocalUpdates,
        propagation: QueryPropagation,
    ) -> Result<QuerySubscriptionId, QueryError> {
        let _span =
            tracing::debug_span!("QM::subscribe", table = %query.table, ?durability_tier).entered();
        // Determine branches
        let branches: Vec<crate::query_manager::types::QueryBranchRef> =
            if !query.branches.is_empty() {
                query
                    .branches
                    .iter()
                    .map(|branch| self.resolve_query_branch_ref(branch))
                    .collect()
            } else if self.schema_context.is_initialized() {
                self.schema_context
                    .all_branch_names()
                    .into_iter()
                    .map(crate::query_manager::types::QueryBranchRef::from_branch_name)
                    .collect()
            } else {
                return Err(QueryError::QueryCompilationError(
                    "schema context not initialized - call set_current_schema() first".into(),
                ));
            };

        let uses_explicit_authorization_filtering =
            self.local_subscription_uses_explicit_authorization(session.as_ref());
        let compile_schema = self.local_subscription_compile_schema(session.as_ref());
        let graph = Self::compile_graph(
            None,
            &query,
            &branches,
            &compile_schema,
            session.clone(),
            &self.schema_context,
        )
        .map_err(|err| QueryError::QueryCompilationError(err.to_string()))?;
        let needs_initial_recompile = query.branches.is_empty();

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        let achieved_tiers = self.sync_manager.local_durability_tiers();

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
                needs_recompile: needs_initial_recompile,
                settled_once: false,
                durability_tier,
                local_updates,
                has_pending_local_updates: false,
                achieved_tiers,
                current_ordered_ids: Vec::new(),
                current_visible_rows: HashMap::new(),
                uses_explicit_authorization_filtering,
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
        let branches: Vec<crate::query_manager::types::QueryBranchRef> = if !query
            .branches
            .is_empty()
        {
            query
                .branches
                .iter()
                .map(|branch| Self::resolve_query_branch_ref_for_context(schema_context, branch))
                .collect()
        } else {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(crate::query_manager::types::QueryBranchRef::from_branch_name)
                .collect()
        };

        // Compile query graph with explicit schema context
        let graph = Self::compile_graph(
            None,
            &query,
            &branches,
            &compile_schema,
            session.clone(),
            schema_context,
        )
        .map_err(|err| QueryError::QueryCompilationError(err.to_string()))?;
        let needs_initial_recompile = query.branches.is_empty();

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        self.subscriptions.insert(
            id,
            QuerySubscription {
                query,
                graph,
                branches,
                session,
                needs_recompile: needs_initial_recompile,
                settled_once: false,
                durability_tier: None,
                local_updates: LocalUpdates::Immediate,
                has_pending_local_updates: false,
                achieved_tiers: HashSet::new(),
                current_ordered_ids: Vec::new(),
                current_visible_rows: HashMap::new(),
                uses_explicit_authorization_filtering: false,
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
    /// Servers will evaluate the query against their data and send ObjectUpdated
    /// messages for all matching objects. As new objects match the query on
    /// the server, they are automatically synced.
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

    pub fn subscribe_with_sync_and_propagation_with_local_updates(
        &mut self,
        query: Query,
        session: Option<Session>,
        durability_tier: Option<DurabilityTier>,
        local_updates: LocalUpdates,
        propagation: QueryPropagation,
    ) -> Result<QuerySubscriptionId, QueryError> {
        // Create local subscription
        let sub_id = self.subscribe_with_session_and_propagation(
            query.clone(),
            session.clone(),
            durability_tier,
            local_updates,
            propagation,
        )?;

        let sync_query = self.sync_query_payload_for_upstream(&query);

        // Send QuerySubscription to connected servers/tiers.
        // local-only still needs to reach the directly connected storage tier
        // (e.g. worker OPFS), and will be prevented from forwarding upstream.
        if self.should_send_local_subscription_upstream(propagation) {
            let query_id = QueryId(sub_id.0);
            self.sync_manager.send_query_subscription_to_servers(
                query_id,
                sync_query,
                self.schema_context.query_context(),
                session,
                propagation,
            );
        }

        Ok(sub_id)
    }

    /// Add an upstream server and replay all active query subscriptions.
    ///
    /// This ensures subscriptions that became active before the server connection
    /// are forwarded once the server is available.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.add_server_with_catalogue_match(server_id, false);
    }

    /// Add an upstream server and optionally skip replaying catalogue objects
    /// when the remote side already has the same catalogue state.
    pub fn add_server_with_catalogue_match(
        &mut self,
        server_id: ServerId,
        skip_catalogue_sync: bool,
    ) {
        self.sync_manager
            .add_server_with_catalogue_match(server_id, skip_catalogue_sync);
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
        let local_subs: Vec<(
            QueryId,
            Query,
            crate::schema_manager::QuerySchemaContext,
            Option<Session>,
            QueryPropagation,
        )> = self
            .subscriptions
            .iter()
            .map(|(sub_id, sub)| {
                (
                    QueryId(sub_id.0),
                    self.sync_query_payload_for_upstream(&sub.query),
                    self.schema_context.query_context(),
                    sub.session.clone(),
                    sub.propagation,
                )
            })
            .collect();

        for (query_id, query, schema_context, session, propagation) in local_subs {
            if self.should_send_local_subscription_upstream(propagation) {
                self.sync_manager.send_query_subscription_to_server(
                    server_id,
                    query_id,
                    query,
                    schema_context,
                    session,
                    propagation,
                );
            }
        }

        let downstream_subs: Vec<(
            QueryId,
            Query,
            crate::schema_manager::QuerySchemaContext,
            Option<Session>,
            QueryPropagation,
        )> = self
            .server_subscriptions
            .iter()
            .filter(|(_, sub)| sub.propagation == QueryPropagation::Full)
            .map(|((_, query_id), sub)| {
                (
                    *query_id,
                    sub.query.clone(),
                    sub.schema_context.query_context(),
                    sub.session.clone(),
                    sub.propagation,
                )
            })
            .collect();

        for (query_id, query, schema_context, session, propagation) in downstream_subs {
            self.sync_manager.send_query_subscription_to_server(
                server_id,
                query_id,
                query,
                schema_context,
                session,
                propagation,
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
    pub fn set_known_schemas(&mut self, schemas: Arc<HashMap<SchemaHash, Schema>>) {
        self.known_schemas = schemas;
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
