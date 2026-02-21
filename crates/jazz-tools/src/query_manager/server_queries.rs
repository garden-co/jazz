use std::collections::HashSet;
use std::sync::Arc;

use crate::commit::CommitId;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::storage::Storage;
use crate::sync_manager::{ClientId, PendingPermissionCheck, QueryId};

use super::graph::QueryGraph;
use super::manager::{PolicyCheckState, QueryManager, ServerQuerySubscription};
use super::policy::{ComplexClause, Operation, evaluate_simple_parts};
use super::policy_graph::PolicyGraph;
use super::session::Session;
use super::types::{ComposedBranchName, RowDescriptor, Schema, TableName, TableSchema, Value};

impl QueryManager {
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
    pub(super) fn process_pending_query_unsubscriptions(&mut self) {
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
    pub(super) fn settle_server_subscriptions(&mut self, storage: &dyn Storage) {
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

    /// Pick up pending permission checks from SyncManager and evaluate them.
    pub(super) fn pick_up_pending_permission_checks<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_permission_checks();

        for check in pending {
            self.evaluate_write_permission(storage, check);
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
    /// - WITH CHECK against new_content (is the resulting row valid?)
    pub(super) fn evaluate_write_permission<H: Storage>(
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
    pub(super) fn evaluate_update_permission<H: Storage>(
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
