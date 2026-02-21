use std::collections::{HashMap, HashSet};

use crate::object_manager::AllObjectUpdate;
use crate::storage::Storage;
use crate::sync_manager::{PersistenceTier, QueryId, ServerId};

#[cfg(test)]
use super::encoding::decode_row;
use super::graph::QueryGraph;
use super::graph_nodes::output::QuerySubscriptionId;
use super::manager::{CatalogueUpdate, QueryError, QueryManager, QuerySubscription, QueryUpdate};
use super::query::{Query, QueryBuilder};
use super::session::Session;
#[cfg(test)]
use super::types::Value;
use super::types::{ComposedBranchName, Schema, SchemaHash};
#[cfg(test)]
use crate::object::ObjectId;

impl QueryManager {
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
        let _span =
            tracing::debug_span!("QM::subscribe", table = %query.table, ?settled_tier).entered();
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
        let graph = if let Some(relation) = query.relation_ir.as_ref() {
            QueryGraph::compile_relation_ir_with_schema_context_and_features(
                relation,
                &self.schema,
                &query.branches,
                session.clone(),
                &self.schema_context,
                query.include_deleted,
                query.array_subqueries.clone(),
            )
        } else {
            QueryGraph::compile_with_schema_context(
                &query,
                &self.schema,
                session.clone(),
                &self.schema_context,
            )
        }
        .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

        let id = QuerySubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

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
        schema_context: &crate::schema_manager::SchemaContext,
        session: Option<Session>,
    ) -> Result<QuerySubscriptionId, QueryError> {
        if query.relation_ir.is_none() {
            let table_name = &query.table;
            let _table_schema = schema
                .get(table_name)
                .ok_or(QueryError::TableNotFound(*table_name))?;
        }

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
        let graph = if let Some(relation) = query.relation_ir.as_ref() {
            QueryGraph::compile_relation_ir_with_schema_context_and_features(
                relation,
                schema,
                &query.branches,
                session.clone(),
                schema_context,
                query.include_deleted,
                query.array_subqueries.clone(),
            )
        } else {
            QueryGraph::compile_with_schema_context(&query, schema, session.clone(), schema_context)
        }
        .ok_or_else(|| QueryError::QueryCompilationError("failed to compile query".into()))?;

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

        let sync_query = self.sync_query_payload_for_upstream(&query);

        // Send QuerySubscription to all servers
        // Use the subscription ID as the query ID for simplicity
        let query_id = QueryId(sub_id.0);
        self.sync_manager
            .send_query_subscription_to_servers(query_id, sync_query, session);

        Ok(sub_id)
    }

    /// Add an upstream server and replay all active query subscriptions.
    ///
    /// This ensures subscriptions that became active before the server connection
    /// are forwarded once the server is available.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.sync_manager.add_server(server_id);
        self.replay_active_query_subscriptions_to_server(server_id);
    }

    /// Unsubscribe from a synced query.
    ///
    /// This method:
    /// 1. Removes the local subscription
    /// 2. Sends a QueryUnsubscription to all connected servers
    pub fn unsubscribe_with_sync(&mut self, id: QuerySubscriptionId) {
        self.subscriptions.remove(&id);

        // Send QueryUnsubscription to all servers
        let query_id = QueryId(id.0);
        self.sync_manager
            .send_query_unsubscription_to_servers(query_id);
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
        let local_subs: Vec<(QueryId, Query, Option<Session>)> = self
            .subscriptions
            .iter()
            .map(|(sub_id, sub)| {
                (
                    QueryId(sub_id.0),
                    self.sync_query_payload_for_upstream(&sub.query),
                    sub.session.clone(),
                )
            })
            .collect();

        for (query_id, query, session) in local_subs {
            self.sync_manager
                .send_query_subscription_to_server(server_id, query_id, query, session);
        }

        let downstream_subs: Vec<(QueryId, Query, Option<Session>)> = self
            .server_subscriptions
            .iter()
            .map(|((_, query_id), sub)| (*query_id, sub.query.clone(), sub.session.clone()))
            .collect();

        for (query_id, query, session) in downstream_subs {
            self.sync_manager
                .send_query_subscription_to_server(server_id, query_id, query, session);
        }
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
    pub(super) fn find_schema_by_short_hash(&self, partial: &SchemaHash) -> Option<SchemaHash> {
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
}
