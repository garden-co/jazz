use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::commit::CommitId;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::schema_manager::LensTransformer;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, ClientRole, PendingPermissionCheck, QueryId, SyncPayload};

use super::manager::{PolicyCheckState, QueryManager, ServerQuerySubscription};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts};
use super::policy_graph::PolicyGraph;
use super::session::Session;
use super::types::{
    ComposedBranchName, LoadedRow, RowDescriptor, Schema, TableName, TableSchema, Value,
};

enum WriteSchemaResolution {
    Resolved(Box<TableSchema>),
    PendingSchema,
    Unresolved,
}

pub(super) struct ResolvedSchemaRow {
    pub branch_name: BranchName,
    pub commit_id: CommitId,
    pub content: Vec<u8>,
}

const SCHEMA_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);

impl QueryManager {
    pub(super) fn build_server_subscription_context(
        &self,
        query: &crate::query_manager::query::Query,
    ) -> Option<(Arc<Schema>, crate::schema_manager::SchemaContext)> {
        if !self.schema.is_empty() {
            return Some((self.schema.clone(), self.schema_context.clone()));
        }

        let composed = query
            .branches
            .first()
            .and_then(|b| ComposedBranchName::parse(&BranchName::new(b)))?;
        let full_hash = self.find_schema_by_short_hash(&composed.schema_hash)?;
        let target_schema = self.known_schemas.get(&full_hash)?.clone();

        let mut schema_context = crate::schema_manager::SchemaContext::new(
            target_schema.clone(),
            &composed.env,
            &composed.user_branch,
        );

        for lens in self.schema_context.lenses.values() {
            schema_context.register_lens(lens.clone());
        }

        for (hash, schema) in self.known_schemas.iter() {
            if *hash != full_hash {
                schema_context.add_pending_schema(schema.clone());
            }
        }

        schema_context.try_activate_pending();

        Some((Arc::new(target_schema), schema_context))
    }

    pub(super) fn branch_schema_map_for_context(
        schema_context: &crate::schema_manager::SchemaContext,
    ) -> std::collections::HashMap<String, crate::query_manager::types::SchemaHash> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            schema_context.branch_name().as_str().to_string(),
            schema_context.current_hash,
        );

        for hash in schema_context.live_schemas.keys() {
            let branch =
                ComposedBranchName::new(&schema_context.env, *hash, &schema_context.user_branch)
                    .to_branch_name();
            map.insert(branch.as_str().to_string(), *hash);
        }

        map
    }

    pub(super) fn resolved_server_query_branches(
        query: &crate::query_manager::query::Query,
        schema_context: &crate::schema_manager::SchemaContext,
    ) -> Vec<String> {
        let all_branches = || {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        };

        if query.branches.is_empty() {
            return all_branches();
        }

        let current_branch = schema_context.branch_name().as_str().to_string();
        if query.branches.len() == 1 && query.branches[0] == current_branch {
            return all_branches();
        }

        query.branches.clone()
    }

    pub(super) fn query_for_server_compile(
        query: &crate::query_manager::query::Query,
        schema_context: &crate::schema_manager::SchemaContext,
    ) -> crate::query_manager::query::Query {
        let mut normalized = query.clone();
        let current_branch = schema_context.branch_name().as_str().to_string();
        if normalized.branches.len() == 1 && normalized.branches[0] == current_branch {
            normalized.branches.clear();
        }
        normalized
    }

    pub(super) fn resolve_latest_row_with_schema_transform(
        id: ObjectId,
        obj: &crate::object::Object,
        branches: &[String],
        table: &str,
        branch_schema_map: &std::collections::HashMap<
            String,
            crate::query_manager::types::SchemaHash,
        >,
        schema_context: &crate::schema_manager::SchemaContext,
    ) -> Option<ResolvedSchemaRow> {
        let mut best: Option<(u64, Vec<u8>, CommitId, BranchName)> = None;

        for branch_name in branches {
            let branch_name = BranchName::new(branch_name);
            let Some(branch) = obj.branches.get(&branch_name) else {
                continue;
            };
            for &tip_id in &branch.tips {
                let Some(commit) = branch.commits.get(&tip_id) else {
                    continue;
                };
                if commit.content.is_empty() {
                    continue;
                }

                match &best {
                    None => {
                        best = Some((
                            commit.timestamp,
                            commit.content.clone(),
                            tip_id,
                            branch_name,
                        ));
                    }
                    Some((best_ts, _, _, _)) if commit.timestamp > *best_ts => {
                        best = Some((
                            commit.timestamp,
                            commit.content.clone(),
                            tip_id,
                            branch_name,
                        ));
                    }
                    _ => {}
                }
            }
        }

        let (_, content, commit_id, branch_name) = best?;
        Self::transform_row_with_schema(
            id,
            content,
            commit_id,
            branch_name,
            table,
            branch_schema_map,
            schema_context,
        )
    }

    pub(super) fn transform_row_with_schema(
        id: ObjectId,
        content: Vec<u8>,
        commit_id: CommitId,
        branch_name: BranchName,
        table: &str,
        branch_schema_map: &std::collections::HashMap<
            String,
            crate::query_manager::types::SchemaHash,
        >,
        schema_context: &crate::schema_manager::SchemaContext,
    ) -> Option<ResolvedSchemaRow> {
        let source_hash = branch_schema_map.get(branch_name.as_str()).copied();

        if let Some(source_hash) = source_hash
            && source_hash != schema_context.current_hash
        {
            let transformer = LensTransformer::new(schema_context, table);
            match transformer.transform(&content, commit_id, source_hash) {
                Ok(result) => {
                    return Some(ResolvedSchemaRow {
                        branch_name,
                        commit_id,
                        content: result.data,
                    });
                }
                Err(err) => {
                    tracing::warn!(
                        row_id = %id,
                        table,
                        source_branch = %branch_name,
                        source_schema = %source_hash.short(),
                        target_schema = %schema_context.current_hash.short(),
                        error = %err,
                        "lens transform failed; dropping row from server query result"
                    );
                    return None;
                }
            }
        }

        Some(ResolvedSchemaRow {
            branch_name,
            commit_id,
            content,
        })
    }

    fn branch_has_live_tip(branch: &crate::object::Branch) -> bool {
        branch.tips.iter().any(|tip_id| {
            branch
                .commits
                .get(tip_id)
                .map(|commit| !commit.content.is_empty())
                .unwrap_or(false)
        })
    }

    fn should_sync_policy_context_rows(&self, client_id: ClientId) -> bool {
        self.sync_manager
            .get_client(client_id)
            .map(|client| {
                matches!(
                    client.role,
                    ClientRole::Peer | ClientRole::Admin | ClientRole::Backend
                )
            })
            .unwrap_or(false)
    }

    fn scope_with_policy_context_rows_from_object_manager(
        base_scope: &HashSet<(ObjectId, BranchName)>,
        graph: &super::graph::QueryGraph,
        branches: &[String],
        object_manager: &crate::object_manager::ObjectManager,
    ) -> HashSet<(ObjectId, BranchName)> {
        let mut scope = base_scope.clone();

        let policy_tables: HashSet<TableName> = graph
            .policy_filter_tables
            .iter()
            .map(|(_, table)| *table)
            .collect();
        if policy_tables.is_empty() {
            return scope;
        }

        let branch_names: Vec<BranchName> = branches.iter().map(BranchName::new).collect();
        for (object_id, object) in &object_manager.objects {
            let Some(table_name) = object.metadata.get(MetadataKey::Table.as_str()) else {
                continue;
            };
            if !policy_tables
                .iter()
                .any(|table| table.as_str() == table_name)
            {
                continue;
            }

            for branch_name in &branch_names {
                let Some(branch) = object.branches.get(branch_name) else {
                    continue;
                };
                if Self::branch_has_live_tip(branch) {
                    scope.insert((*object_id, *branch_name));
                }
            }
        }

        scope
    }

    /// Process pending query subscriptions from downstream clients.
    ///
    /// For each pending subscription:
    /// 1. Build a QueryGraph with the client's session
    /// 2. Settle the graph to get contributing ObjectIds
    /// 3. Set the scope in SyncManager (which triggers initial sync)
    pub(super) fn process_pending_query_subscriptions<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_query_subscriptions();
        let mut deferred = Vec::new();

        for sub in pending {
            let Some((schema_for_compile, subscription_context)) =
                self.build_server_subscription_context(&sub.query)
            else {
                deferred.push(sub);
                continue;
            };

            // Defence in depth: if the subscription has no session (client omitted
            // it), fall back to the connection-level session set during JWT auth
            // on the WebSocket handshake. This ensures the PolicyFilterNode is
            // always present — at worst it will fail closed (zero results) rather
            // than fail open (bypass policies).
            let session_for_policy = sub.session.clone().or_else(|| {
                self.sync_manager
                    .get_client(sub.client_id)
                    .and_then(|c| c.session.clone())
            });

            // Build QueryGraph with client's session for policy filtering (schema-aware)
            let query_for_compile =
                Self::query_for_server_compile(&sub.query, &subscription_context);
            let graph = Self::compile_graph(
                &query_for_compile,
                &schema_for_compile,
                session_for_policy.clone(),
                &subscription_context,
            );

            let Ok(mut graph) = graph else {
                // Query compilation failed (e.g., missing table) - notify client with compiler context.
                let compile_error = graph
                    .err()
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "unknown compile error".to_string());
                let reason = format!(
                    "query compilation failed for query_id {}: {}",
                    sub.query_id.0, compile_error
                );
                self.sync_manager.emit_query_subscription_rejected(
                    sub.client_id,
                    sub.query_id,
                    reason,
                );
                continue;
            };

            let sync_policy_context_rows = self.should_sync_policy_context_rows(sub.client_id);
            let branch_schema_map = Self::branch_schema_map_for_context(&subscription_context);

            // Initial settle to populate the graph
            let om = &mut self.sync_manager.object_manager;
            let storage_ref: &dyn Storage = storage;

            let branches =
                Self::resolved_server_query_branches(&query_for_compile, &subscription_context);
            let table = sub.query.table.as_str().to_string();
            let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                let obj = om.get_or_load(id, storage_ref, &branches)?;
                let resolved = Self::resolve_latest_row_with_schema_transform(
                    id,
                    obj,
                    &branches,
                    &table,
                    &branch_schema_map,
                    &subscription_context,
                )?;
                Some(LoadedRow::new(
                    resolved.content,
                    resolved.commit_id,
                    [(id, resolved.branch_name)].into_iter().collect(),
                ))
            };

            let _delta = graph.settle(storage_ref, row_loader);

            // Sync the rows needed for the client to reproduce the current result
            // locally, including any ordered prefix required by pagination.
            let result_scope = graph.sync_scope_object_ids();
            // Trusted clients (Peer/Admin) also need policy context rows.
            let scope = if sync_policy_context_rows {
                Self::scope_with_policy_context_rows_from_object_manager(
                    &result_scope,
                    &graph,
                    &branches,
                    om,
                )
            } else {
                result_scope
            };

            // Set scope in SyncManager (triggers initial sync)
            self.sync_manager.set_client_query_scope(
                sub.client_id,
                sub.query_id,
                scope.clone(),
                session_for_policy.clone(),
            );

            // Forward QuerySubscription to upstream servers (multi-tier forwarding)
            // This allows hub servers to know about the query and push matching data
            if sub.propagation == crate::sync_manager::QueryPropagation::Full {
                self.sync_manager.send_query_subscription_to_servers(
                    sub.query_id,
                    sub.query.clone(),
                    session_for_policy.clone(),
                    sub.propagation,
                );
            }

            // Store the server subscription for reactive updates
            self.server_subscriptions.insert(
                (sub.client_id, sub.query_id),
                ServerQuerySubscription {
                    query: sub.query,
                    graph,
                    schema_context: subscription_context,
                    session: session_for_policy,
                    branches,
                    last_scope: scope,
                    needs_recompile: false,
                    settled_once: false,
                    propagation: sub.propagation,
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
    pub(super) fn process_pending_query_unsubscriptions(&mut self) {
        let pending = self.sync_manager.take_pending_query_unsubscriptions();

        for unsub in pending {
            let propagation = self
                .server_subscriptions
                .remove(&(unsub.client_id, unsub.query_id))
                .map(|sub| sub.propagation)
                .unwrap_or(crate::sync_manager::QueryPropagation::Full);

            if propagation == crate::sync_manager::QueryPropagation::Full {
                // Forward unsubscription to upstream servers
                self.sync_manager
                    .send_query_unsubscription_to_servers(unsub.query_id);
            }
        }
    }

    /// Settle server-side query subscriptions and update scopes.
    ///
    /// Called after local data changes to detect when new objects match
    /// a client's query subscription.
    #[allow(clippy::type_complexity)]
    pub(super) fn settle_server_subscriptions(&mut self, storage: &dyn Storage) {
        // Collect updates to avoid borrow issues
        let mut scope_updates: Vec<(
            ClientId,
            QueryId,
            HashSet<(ObjectId, BranchName)>,
            Option<Session>,
        )> = Vec::new();
        let mut settled_notifications: Vec<(ClientId, QueryId)> = Vec::new();

        let trusted_clients: HashSet<ClientId> = self
            .sync_manager
            .clients
            .iter()
            .filter_map(|(client_id, client)| {
                if matches!(
                    client.role,
                    ClientRole::Peer | ClientRole::Admin | ClientRole::Backend
                ) {
                    Some(*client_id)
                } else {
                    None
                }
            })
            .collect();

        let om = &mut self.sync_manager.object_manager;

        for ((client_id, query_id), sub) in &mut self.server_subscriptions {
            let branches = &sub.branches;
            let table = sub.query.table.as_str().to_string();
            let branch_schema_map = Self::branch_schema_map_for_context(&sub.schema_context);

            // Row loader for this subscription
            let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                let obj = om.get_or_load(id, storage, branches)?;
                let resolved = Self::resolve_latest_row_with_schema_transform(
                    id,
                    obj,
                    branches,
                    &table,
                    &branch_schema_map,
                    &sub.schema_context,
                )?;
                Some(LoadedRow::new(
                    resolved.content,
                    resolved.commit_id,
                    [(id, resolved.branch_name)].into_iter().collect(),
                ))
            };

            let new_scope = {
                // Settle the graph
                let _delta = sub.graph.settle(storage, row_loader);

                // Emit QuerySettled on first settlement
                if !sub.settled_once {
                    sub.settled_once = true;
                    settled_notifications.push((*client_id, *query_id));
                }

                // Check if scope changed
                let result_scope = sub.graph.sync_scope_object_ids();
                if trusted_clients.contains(client_id) {
                    Self::scope_with_policy_context_rows_from_object_manager(
                        &result_scope,
                        &sub.graph,
                        branches,
                        om,
                    )
                } else {
                    result_scope
                }
            };
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

    /// Pick up pending permission checks from SyncManager and evaluate them.
    pub(super) fn pick_up_pending_permission_checks<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_permission_checks();

        for check in pending {
            self.evaluate_write_permission(storage, check);
        }
    }

    fn schema_for_write_hash(&self, schema_hash: super::types::SchemaHash) -> Option<&Schema> {
        if self.schema_context.is_initialized() && schema_hash == self.schema_context.current_hash {
            return Some(self.schema.as_ref());
        }

        self.schema_context
            .get_schema(&schema_hash)
            .or_else(|| self.known_schemas.get(&schema_hash))
    }

    fn resolve_write_table_schema(
        &mut self,
        table_name: TableName,
        branch_name: BranchName,
    ) -> WriteSchemaResolution {
        let parsed_branch = ComposedBranchName::parse(&branch_name);
        let schema_hash = self
            .branch_schema_map
            .get(branch_name.as_str())
            .copied()
            .or_else(|| {
                parsed_branch
                    .as_ref()
                    .and_then(|composed| self.find_schema_by_short_hash(&composed.schema_hash))
            });

        if let Some(schema_hash) = schema_hash {
            self.branch_schema_map
                .insert(branch_name.as_str().to_string(), schema_hash);

            let Some(schema) = self.schema_for_write_hash(schema_hash) else {
                return WriteSchemaResolution::PendingSchema;
            };

            return schema
                .get(&table_name)
                .cloned()
                .map(Box::new)
                .map(WriteSchemaResolution::Resolved)
                .unwrap_or(WriteSchemaResolution::Unresolved);
        }

        // When the write targets the current initialized branch, self.schema is authoritative.
        if self.schema_context.is_initialized()
            && branch_name.as_str() == self.schema_context.branch_name().as_str()
        {
            return self
                .schema
                .get(&table_name)
                .cloned()
                .map(Box::new)
                .map(WriteSchemaResolution::Resolved)
                .unwrap_or(WriteSchemaResolution::Unresolved);
        }

        // In pure local/client mode (no server-known schemas and a non-empty current schema),
        // self.schema is still authoritative.
        if self.known_schemas.is_empty() && !self.schema.is_empty() {
            return self
                .schema
                .get(&table_name)
                .cloned()
                .map(Box::new)
                .map(WriteSchemaResolution::Resolved)
                .unwrap_or(WriteSchemaResolution::Unresolved);
        }

        if parsed_branch.is_some() {
            return WriteSchemaResolution::PendingSchema;
        }

        WriteSchemaResolution::Unresolved
    }

    /// Evaluate a write permission check.
    ///
    /// If the simple parts of the policy fail, reject immediately.
    /// If there are complex clauses (INHERITS/EXISTS), create policy graphs.
    /// If all simple parts pass and no complex clauses, approve immediately.
    ///
    /// For UPDATE operations, we evaluate two policies:
    /// - USING against old_content (can the session see the old row?)
    /// - WITH CHECK against new_content (is the resulting row valid?)
    pub(super) fn evaluate_write_permission<H: Storage>(
        &mut self,
        storage: &mut H,
        mut check: PendingPermissionCheck,
    ) {
        // Get table name from metadata
        let table_name = match check.metadata.get(MetadataKey::Table.as_str()) {
            Some(t) => TableName::new(t),
            None => {
                // No table metadata means this is not a row write (e.g. generic object data),
                // so ReBAC row policies do not apply.
                tracing::trace!(
                    operation = ?check.operation,
                    metadata_keys = ?check.metadata.keys().collect::<Vec<_>>(),
                    "allowing write with no table metadata (non-row object)"
                );
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let branch_name = match &check.payload {
            SyncPayload::ObjectUpdated { branch_name, .. } => *branch_name,
            SyncPayload::ObjectTruncated { branch_name, .. } => *branch_name,
            _ => BranchName::new(self.current_branch()),
        };

        // Look up table schema for the write branch.
        let table_schema = match self.resolve_write_table_schema(table_name, branch_name) {
            WriteSchemaResolution::Resolved(schema) => *schema,
            WriteSchemaResolution::PendingSchema => {
                let wait_started_at = check
                    .schema_wait_started_at
                    .get_or_insert_with(Instant::now);
                let wait_elapsed = wait_started_at.elapsed();

                if wait_elapsed >= SCHEMA_RESOLUTION_TIMEOUT {
                    tracing::warn!(
                        operation = ?check.operation,
                        table = %table_name,
                        branch = %branch_name,
                        waited_ms = wait_elapsed.as_millis() as u64,
                        "denying deferred write because schema did not become available in time"
                    );
                    let reason = format!(
                        "{:?} denied on table {} - schema unavailable for branch {} after waiting {}s",
                        check.operation,
                        table_name.0,
                        branch_name,
                        SCHEMA_RESOLUTION_TIMEOUT.as_secs()
                    );
                    self.sync_manager.reject_permission_check(check, reason);
                    return;
                }

                tracing::debug!(
                    operation = ?check.operation,
                    table = %table_name,
                    branch = %branch_name,
                    waited_ms = wait_elapsed.as_millis() as u64,
                    "deferring write permission check until schema becomes available"
                );
                self.sync_manager
                    .requeue_pending_permission_checks(vec![check]);
                return;
            }
            WriteSchemaResolution::Unresolved => {
                // Fail closed: if we cannot resolve the table schema for this write,
                // we cannot safely evaluate permissions.
                tracing::warn!(
                    operation = ?check.operation,
                    table = %table_name,
                    branch = %branch_name,
                    "denying write because schema could not be resolved"
                );
                let reason = format!(
                    "{:?} denied on table {} - schema unavailable for branch {}",
                    check.operation, table_name.0, branch_name
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }
        };

        if check.operation == Operation::Insert
            && let Some(new_content) = check.new_content.as_ref()
            && let Err(err) = self.validate_json_for_content(&table_schema.columns, new_content)
        {
            self.sync_manager
                .reject_permission_check(check, err.to_string());
            return;
        }

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
                tracing::trace!(
                    operation = ?check.operation,
                    table = %table_name,
                    branch = %branch_name,
                    "allowing write because no policy is defined for operation"
                );
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
                tracing::trace!(
                    operation = ?check.operation,
                    table = %table_name,
                    branch = %branch_name,
                    "allowing write because there is no content to evaluate"
                );
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        // Evaluate simple parts of the policy
        let result = evaluate_simple_parts(&policy, content, &table_schema.columns, &check.session);
        tracing::trace!(
            operation = ?check.operation,
            table = %table_name,
            branch = %branch_name,
            session_user_id = %check.session.user_id,
            passed = result.passed,
            complex_clause_count = result.complex_clauses.len(),
            "evaluated write policy simple parts"
        );

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

        let mut graph_clauses = Vec::new();
        for clause in result.complex_clauses {
            match clause {
                ComplexClause::InheritsReferencing {
                    operation,
                    source_table,
                    via_column,
                    max_depth,
                } => {
                    let (object_id, branch_name) = match &check.payload {
                        SyncPayload::ObjectUpdated {
                            object_id,
                            branch_name,
                            ..
                        } => (*object_id, branch_name.as_str()),
                        _ => {
                            let reason = format!(
                                "{:?} denied by policy on table {} (missing row context for INHERITS REFERENCING)",
                                check.operation, table_name.0
                            );
                            self.sync_manager.reject_permission_check(check, reason);
                            return;
                        }
                    };

                    if !self.evaluate_referencing_inherited_access(
                        storage,
                        table_name,
                        object_id,
                        operation,
                        &source_table,
                        &via_column,
                        max_depth,
                        &check.session,
                        branch_name,
                    ) {
                        let reason = format!(
                            "{:?} denied by policy on table {} (INHERITS REFERENCING failed)",
                            check.operation, table_name.0
                        );
                        self.sync_manager.reject_permission_check(check, reason);
                        return;
                    }
                }
                other => graph_clauses.push(other),
            }
        }

        if graph_clauses.is_empty() {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Remaining complex clauses use policy graphs.
        let graphs = self.create_policy_graphs_for_complex_clauses(
            &graph_clauses,
            content,
            &table_schema.columns,
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
    pub(super) fn evaluate_update_permission<H: Storage>(
        &mut self,
        storage: &mut H,
        check: PendingPermissionCheck,
        table_name: TableName,
        table_schema: TableSchema,
    ) {
        if let Some(new_content) = check.new_content.as_ref()
            && let Err(err) = self.validate_json_for_content(&table_schema.columns, new_content)
        {
            self.sync_manager
                .reject_permission_check(check, err.to_string());
            return;
        }

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
                evaluate_simple_parts(using, old_content, &table_schema.columns, &check.session);

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
                &table_schema.columns,
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

        let row_context = match &check.payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            } => Some((*object_id, branch_name.as_str())),
            _ => None,
        };

        let mut graph_inputs: Vec<(ComplexClause, Vec<u8>)> = Vec::new();
        for (clause, content) in all_complex_clauses {
            match clause {
                ComplexClause::InheritsReferencing {
                    operation,
                    source_table,
                    via_column,
                    max_depth,
                } => {
                    let Some((object_id, branch_name)) = row_context else {
                        let reason = format!(
                            "Update denied by policy on table {} (missing row context for INHERITS REFERENCING)",
                            table_name.0
                        );
                        self.sync_manager.reject_permission_check(check, reason);
                        return;
                    };
                    if !self.evaluate_referencing_inherited_access(
                        storage,
                        table_name,
                        object_id,
                        operation,
                        &source_table,
                        &via_column,
                        max_depth,
                        &check.session,
                        branch_name,
                    ) {
                        let reason = format!(
                            "Update denied by policy on table {} (INHERITS REFERENCING failed)",
                            table_name.0
                        );
                        self.sync_manager.reject_permission_check(check, reason);
                        return;
                    }
                }
                other => graph_inputs.push((other, content)),
            }
        }

        if graph_inputs.is_empty() {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        // Create policy graphs for remaining complex clauses
        let mut graphs = Vec::new();
        for (clause, content) in &graph_inputs {
            let clause_graphs = self.create_policy_graphs_for_complex_clauses(
                std::slice::from_ref(clause),
                content,
                &table_schema.columns,
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
    pub(super) fn create_policy_graphs_for_complex_clauses(
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
                    max_depth: _,
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
                        1,
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
                ComplexClause::ExistsRel { rel } => {
                    if let Some(graph) = PolicyGraph::for_exists_rel(rel, &self.schema, &branch) {
                        graphs.push(graph);
                    }
                }
                ComplexClause::InheritsReferencing { .. } => {
                    // Evaluated directly in write permission checks (needs target row context).
                }
            }
        }

        graphs
    }

    /// Settle active policy checks and finalize completed ones.
    pub(super) fn settle_policy_checks<H: Storage>(&mut self, storage: &mut H) {
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
            let mut row_loader = |id: ObjectId| -> Option<LoadedRow> {
                let obj = om.get_or_load(id, storage_ref, &branches)?;
                let branch = obj.branches.get(&BranchName::new(&current_branch))?;
                let tip_id = branch.tips.iter().next()?;
                let commit = branch.commits.get(tip_id)?;
                if commit.content.is_empty() {
                    return None;
                }
                Some(LoadedRow::new(
                    commit.content.clone(),
                    *tip_id,
                    [(id, BranchName::new(&current_branch))]
                        .into_iter()
                        .collect(),
                ))
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
                        "{:?} denied by policy on table {} (complex policy check failed)",
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
}
