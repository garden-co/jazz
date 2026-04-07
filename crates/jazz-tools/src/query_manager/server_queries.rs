use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::commit::CommitId;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::graph_nodes::policy_eval::PolicyContextEvaluator;
use crate::schema_manager::LensTransformer;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, ClientRole, PendingPermissionCheck, QueryId, SyncPayload};

use super::manager::{QueryManager, SchemaWarningAccumulator, ServerQuerySubscription};
use super::policy::{ComplexClause, Operation, PolicyExpr};
use super::policy_graph::PolicyGraph;
use super::session::Session;
use super::types::{
    ComposedBranchName, LoadedRow, Row, RowDescriptor, Schema, SchemaHash, TableName, TableSchema,
    Value,
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

pub(super) struct RowTransformContext<'a> {
    pub(super) table: &'a str,
    pub(super) branch_schema_map:
        &'a std::collections::HashMap<String, crate::query_manager::types::SchemaHash>,
    pub(super) schema_context: &'a crate::schema_manager::SchemaContext,
    pub(super) schema_warnings: &'a mut SchemaWarningAccumulator,
}

pub(crate) struct AuthorizationPolicyRequest<'a> {
    pub(crate) object_id: ObjectId,
    pub(crate) branch_name: BranchName,
    pub(crate) table_name: TableName,
    pub(crate) policy: &'a PolicyExpr,
    pub(crate) content: &'a [u8],
    pub(crate) provenance: &'a crate::metadata::RowProvenance,
    pub(crate) session: &'a Session,
    pub(crate) auth_schema: &'a Schema,
    pub(crate) auth_context: &'a crate::schema_manager::SchemaContext,
    pub(crate) source_branch_schema_map: &'a std::collections::HashMap<String, SchemaHash>,
    pub(crate) operation: Operation,
}

struct UpdatePermissionRequest<'a> {
    object_id: ObjectId,
    branch_name: BranchName,
    table_name: TableName,
    branch_table_schema: &'a TableSchema,
    auth_schema: &'a Schema,
    auth_context: &'a crate::schema_manager::SchemaContext,
}

impl QueryManager {
    fn current_row_provenance(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch_name: BranchName,
    ) -> Option<RowProvenance> {
        let branches = vec![branch_name.as_str().to_string()];
        let (_, row) = Self::load_best_visible_row_version(storage, object_id, &branches)?;
        Some(row.row_provenance())
    }

    fn payload_row_provenance(payload: &SyncPayload) -> Option<RowProvenance> {
        match payload {
            SyncPayload::RowVersionCreated { row, .. }
            | SyncPayload::RowVersionNeeded { row, .. } => Some(row.row_provenance()),
            _ => None,
        }
    }

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

    pub(super) fn authorization_schema_for_context(
        &self,
        env: &str,
        user_branch: &str,
    ) -> Option<(Arc<Schema>, crate::schema_manager::SchemaContext)> {
        let schema = self
            .authorization_schema
            .clone()
            .or_else(|| (!self.schema.is_empty()).then(|| self.schema.clone()))?;

        let mut schema_context =
            crate::schema_manager::SchemaContext::new((*schema).clone(), env, user_branch);

        for lens in self.schema_context.lenses.values() {
            schema_context.register_lens(lens.clone());
        }

        for (hash, known_schema) in self.known_schemas.iter() {
            if *hash != schema_context.current_hash {
                schema_context.add_pending_schema(known_schema.clone());
            }
        }

        schema_context.try_activate_pending();

        Some((schema, schema_context))
    }

    pub(super) fn authorization_schema_for_branch(
        &self,
        branch_name: &BranchName,
    ) -> Option<(Arc<Schema>, crate::schema_manager::SchemaContext)> {
        if let Some(composed) = ComposedBranchName::parse(branch_name) {
            if let Some(parts) =
                self.authorization_schema_for_context(&composed.env, &composed.user_branch)
            {
                return Some(parts);
            }

            if self.authorization_schema_required {
                return None;
            }

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

            for (hash, known_schema) in self.known_schemas.iter() {
                if *hash != full_hash {
                    schema_context.add_pending_schema(known_schema.clone());
                }
            }

            schema_context.try_activate_pending();

            return Some((Arc::new(target_schema), schema_context));
        }

        if self.schema_context.is_initialized() {
            return self
                .authorization_schema_for_context(
                    &self.schema_context.env,
                    &self.schema_context.user_branch,
                )
                .or_else(|| Some((self.schema.clone(), self.schema_context.clone())));
        }

        None
    }

    fn transform_content_to_authorization_schema(
        &self,
        table: &str,
        content: &[u8],
        commit_id: CommitId,
        branch_name: BranchName,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        auth_context: &crate::schema_manager::SchemaContext,
    ) -> Option<Vec<u8>> {
        let source_hash = source_branch_schema_map
            .get(branch_name.as_str())
            .copied()
            .or_else(|| {
                (branch_name.as_str() == auth_context.branch_name().as_str())
                    .then_some(auth_context.current_hash)
            })
            .or_else(|| {
                ComposedBranchName::parse(&branch_name)
                    .and_then(|composed| self.find_schema_by_short_hash(&composed.schema_hash))
            });
        let source_hash = match source_hash {
            Some(source_hash) => source_hash,
            None if ComposedBranchName::parse(&branch_name).is_some() => return None,
            None => return Some(content.to_vec()),
        };

        if source_hash == auth_context.current_hash {
            return Some(content.to_vec());
        }

        let transformer = LensTransformer::new(auth_context, table);
        transformer
            .transform(content, commit_id, source_hash)
            .ok()
            .map(|result| result.data)
    }

    fn load_row_for_authorization_context(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch_name: BranchName,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        auth_context: &crate::schema_manager::SchemaContext,
    ) -> Option<LoadedRow> {
        let branches = vec![branch_name.as_str().to_string()];
        let (table, row) = Self::load_best_visible_row_version(storage, object_id, &branches)?;
        if row.is_hard_deleted() {
            return None;
        }

        let tip_commit_id = row.version_id();
        let tip_content = row.data.clone();
        let tip_provenance = row.row_provenance();

        let transformed = self.transform_content_to_authorization_schema(
            &table,
            &tip_content,
            tip_commit_id,
            branch_name,
            source_branch_schema_map,
            auth_context,
        )?;

        Some(LoadedRow::new(
            transformed,
            tip_commit_id,
            tip_provenance,
            [(object_id, branch_name)].into_iter().collect(),
        ))
    }

    pub(super) fn evaluate_authorization_policy(
        &mut self,
        storage: &dyn Storage,
        request: AuthorizationPolicyRequest<'_>,
    ) -> bool {
        let AuthorizationPolicyRequest {
            object_id,
            branch_name,
            table_name,
            policy,
            content,
            provenance,
            session,
            auth_schema,
            auth_context,
            source_branch_schema_map,
            operation,
        } = request;

        let Some(table_schema) = auth_schema.get(&table_name) else {
            return false;
        };
        let Some(transformed) = self.transform_content_to_authorization_schema(
            table_name.as_str(),
            content,
            CommitId([0; 32]),
            branch_name,
            source_branch_schema_map,
            auth_context,
        ) else {
            return false;
        };

        let row = Row::new(
            object_id,
            transformed,
            CommitId([0; 32]),
            provenance.clone(),
        );
        let evaluator = PolicyContextEvaluator::new(auth_schema, session, branch_name.as_str());
        let mut visited = HashSet::new();
        let mut row_loader = |related_id: ObjectId| {
            self.load_row_for_authorization_context(
                storage,
                related_id,
                branch_name,
                source_branch_schema_map,
                auth_context,
            )
        };

        evaluator.evaluate_row_access(
            operation,
            &row,
            &table_schema.columns,
            table_name.as_str(),
            Some(policy),
            storage,
            &mut row_loader,
            0,
            &mut visited,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn provenance_row_matches_current_select_policy(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch_name: BranchName,
        session: Option<&Session>,
        auth_schema: &Schema,
        auth_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
    ) -> bool {
        let branches = vec![branch_name.as_str().to_string()];
        let Some((table, row)) = Self::load_best_visible_row_version(storage, object_id, &branches)
        else {
            return false;
        };
        if row.is_hard_deleted() {
            return false;
        }

        let tip_content = row.data.clone();
        let tip_provenance = row.row_provenance();

        let table_name = TableName::new(&table);
        let Some(select_policy) = auth_schema
            .get(&table_name)
            .and_then(|table_schema| table_schema.policies.select.using.as_ref())
        else {
            return auth_schema.contains_key(&table_name);
        };
        let Some(session) = session else {
            return false;
        };

        self.evaluate_authorization_policy(
            storage,
            AuthorizationPolicyRequest {
                object_id,
                branch_name,
                table_name,
                policy: select_policy,
                content: &tip_content,
                provenance: &tip_provenance,
                session,
                auth_schema,
                auth_context,
                source_branch_schema_map,
                operation: Operation::Select,
            },
        )
    }

    pub(super) fn authorized_rows_from_graph(
        &mut self,
        storage: &dyn Storage,
        graph: &super::graph::QueryGraph,
        schema_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        session: Option<&Session>,
    ) -> Vec<Row> {
        let Some((auth_schema, auth_context)) =
            self.authorization_schema_for_context(&schema_context.env, &schema_context.user_branch)
        else {
            if !self.authorization_schema_required {
                return graph.current_result();
            }
            return Vec::new();
        };

        if auth_schema
            .values()
            .all(|table_schema| table_schema.policies.select.using.is_none())
        {
            return graph.current_result();
        }

        let mut authorization_cache: HashMap<(ObjectId, BranchName), bool> = HashMap::new();

        graph
            .current_output_rows_with_provenance()
            .into_iter()
            .filter_map(|(row, provenance)| {
                provenance
                    .iter()
                    .copied()
                    .all(|(object_id, branch_name)| {
                        *authorization_cache
                            .entry((object_id, branch_name))
                            .or_insert_with(|| {
                                self.provenance_row_matches_current_select_policy(
                                    storage,
                                    object_id,
                                    branch_name,
                                    session,
                                    &auth_schema,
                                    &auth_context,
                                    source_branch_schema_map,
                                )
                            })
                    })
                    .then_some(row)
            })
            .collect()
    }

    fn authorized_scope_from_graph(
        &mut self,
        storage: &dyn Storage,
        graph: &super::graph::QueryGraph,
        schema_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        session: Option<&Session>,
    ) -> HashSet<(ObjectId, BranchName)> {
        let Some((auth_schema, auth_context)) =
            self.authorization_schema_for_context(&schema_context.env, &schema_context.user_branch)
        else {
            if !self.authorization_schema_required {
                return graph.sync_scope_object_ids();
            }
            return HashSet::new();
        };

        if auth_schema
            .values()
            .all(|table_schema| table_schema.policies.select.using.is_none())
        {
            return graph.sync_scope_object_ids();
        }

        let mut authorization_cache: HashMap<(ObjectId, BranchName), bool> = HashMap::new();

        graph
            .current_output_tuples()
            .into_iter()
            .filter_map(|tuple| {
                tuple
                    .provenance()
                    .iter()
                    .copied()
                    .all(|(object_id, branch_name)| {
                        *authorization_cache
                            .entry((object_id, branch_name))
                            .or_insert_with(|| {
                                self.provenance_row_matches_current_select_policy(
                                    storage,
                                    object_id,
                                    branch_name,
                                    session,
                                    &auth_schema,
                                    &auth_context,
                                    source_branch_schema_map,
                                )
                            })
                    })
                    .then(|| tuple.provenance().clone())
            })
            .flatten()
            .collect()
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

    pub(super) fn transform_row_with_schema(
        id: ObjectId,
        content: Vec<u8>,
        commit_id: CommitId,
        branch_name: BranchName,
        context: &mut RowTransformContext<'_>,
    ) -> Option<ResolvedSchemaRow> {
        let source_hash = context.branch_schema_map.get(branch_name.as_str()).copied();

        if let Some(source_hash) = source_hash
            && source_hash != context.schema_context.current_hash
        {
            let transformer = LensTransformer::new(context.schema_context, context.table);
            match transformer.transform(&content, commit_id, source_hash) {
                Ok(result) => {
                    return Some(ResolvedSchemaRow {
                        branch_name,
                        commit_id,
                        content: result.data,
                    });
                }
                Err(err) => {
                    context.schema_warnings.record(
                        context.table,
                        source_hash,
                        context.schema_context.current_hash,
                    );
                    tracing::debug!(
                        row_id = %id,
                        table = context.table,
                        source_branch = %branch_name,
                        source_schema = %source_hash.short(),
                        target_schema = %context.schema_context.current_hash.short(),
                        error = %err,
                        "lens transform failed; row will be counted in aggregated schema warning"
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

    fn should_sync_policy_context_rows(&self, client_id: ClientId) -> bool {
        self.client_bypasses_authorization_filtering(client_id)
    }

    fn client_bypasses_authorization_filtering(&self, client_id: ClientId) -> bool {
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

    fn scope_with_policy_context_rows<H: Storage + ?Sized>(
        base_scope: &HashSet<(ObjectId, BranchName)>,
        graph: &super::graph::QueryGraph,
        branches: &[String],
        object_manager: &crate::object_manager::ObjectManager,
        storage: &H,
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
        for (object_id, metadata) in &object_manager.metadata_by_id {
            let Some(table_name) = metadata.get(MetadataKey::Table.as_str()) else {
                continue;
            };
            if !policy_tables
                .iter()
                .any(|table| table.as_str() == table_name)
            {
                continue;
            }

            for branch_name in &branch_names {
                let Some(row) = storage
                    .load_visible_region_row(table_name, branch_name.as_str(), *object_id)
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                if !row.is_hard_deleted() {
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
        let mut schema_warning_notifications = Vec::new();

        for sub in pending {
            let Some((schema_for_compile, subscription_context)) =
                self.build_server_subscription_context(&sub.query)
            else {
                deferred.push(sub);
                continue;
            };
            if self
                .authorization_schema_for_context(
                    &subscription_context.env,
                    &subscription_context.user_branch,
                )
                .is_none()
                && self.schema.is_empty()
                && self.authorization_schema_required
            {
                deferred.push(sub);
                continue;
            }

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
            let storage_ref: &dyn Storage = storage;

            let branches =
                Self::resolved_server_query_branches(&query_for_compile, &subscription_context);
            let table = sub.query.table.as_str().to_string();
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let include_deleted = sub.query.include_deleted;
            {
                let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                    Self::load_visible_row_for_query(
                        storage_ref,
                        id,
                        &branches,
                        include_deleted,
                        &subscription_context,
                        &branch_schema_map,
                        &table,
                        super::graph_nodes::output::QuerySubscriptionId(sub.query_id.0),
                        &mut schema_warnings,
                    )
                };

                let _delta = graph.settle(storage_ref, row_loader);
            }
            let mut reported_schema_warnings = HashSet::new();
            let new_schema_warnings = Self::finalize_schema_warnings(
                &mut reported_schema_warnings,
                schema_warnings.warnings_for_query(sub.query_id),
            );
            schema_warning_notifications.extend(
                new_schema_warnings
                    .into_iter()
                    .map(|warning| (sub.client_id, warning)),
            );

            // Sync the rows needed for the client to reproduce the current result
            // locally, including any ordered prefix required by pagination.
            let result_scope = if self.client_bypasses_authorization_filtering(sub.client_id) {
                graph.sync_scope_object_ids()
            } else {
                self.authorized_scope_from_graph(
                    storage_ref,
                    &graph,
                    &subscription_context,
                    &branch_schema_map,
                    session_for_policy.as_ref(),
                )
            };
            // Trusted clients (Peer/Admin) also need policy context rows.
            let scope = if sync_policy_context_rows {
                let om = &self.sync_manager.object_manager;
                Self::scope_with_policy_context_rows(
                    &result_scope,
                    &graph,
                    &branches,
                    om,
                    storage_ref,
                )
            } else {
                result_scope
            };

            for (object_id, branch_name) in &scope {
                let _ = self.sync_manager.object_manager.get_or_load(
                    *object_id,
                    storage_ref,
                    &[branch_name.as_str().to_string()],
                );
            }

            // Set scope in SyncManager (triggers initial sync)
            self.sync_manager.set_client_query_scope_with_storage(
                storage_ref,
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
                    reported_schema_warnings,
                },
            );
        }

        for (client_id, warning) in schema_warning_notifications {
            self.sync_manager.emit_schema_warning(client_id, warning);
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
        let mut schema_warning_notifications: Vec<(ClientId, crate::sync_manager::SchemaWarning)> =
            Vec::new();

        let subscription_keys: Vec<_> = self.server_subscriptions.keys().copied().collect();

        for (client_id, query_id) in subscription_keys {
            let Some(mut sub) = self.server_subscriptions.remove(&(client_id, query_id)) else {
                continue;
            };
            let branches = &sub.branches;
            let table = sub.query.table.as_str().to_string();
            let include_deleted = sub.query.include_deleted;
            let branch_schema_map = Self::branch_schema_map_for_context(&sub.schema_context);
            let mut schema_warnings = SchemaWarningAccumulator::default();

            // Row loader for this subscription
            let new_scope = {
                {
                    let row_loader = |id: ObjectId| -> Option<LoadedRow> {
                        Self::load_visible_row_for_query(
                            storage,
                            id,
                            branches,
                            include_deleted,
                            &sub.schema_context,
                            &branch_schema_map,
                            &table,
                            super::graph_nodes::output::QuerySubscriptionId(query_id.0),
                            &mut schema_warnings,
                        )
                    };

                    let _delta = sub.graph.settle(storage, row_loader);
                }
                let new_schema_warnings = Self::finalize_schema_warnings(
                    &mut sub.reported_schema_warnings,
                    schema_warnings.warnings_for_query(query_id),
                );
                schema_warning_notifications.extend(
                    new_schema_warnings
                        .into_iter()
                        .map(|warning| (client_id, warning)),
                );

                // Emit QuerySettled on first settlement
                if !sub.settled_once {
                    sub.settled_once = true;
                    settled_notifications.push((client_id, query_id));
                }

                // Check if scope changed
                let result_scope = if self.client_bypasses_authorization_filtering(client_id) {
                    sub.graph.sync_scope_object_ids()
                } else {
                    self.authorized_scope_from_graph(
                        storage,
                        &sub.graph,
                        &sub.schema_context,
                        &branch_schema_map,
                        sub.session.as_ref(),
                    )
                };
                if self.should_sync_policy_context_rows(client_id) {
                    let om = &self.sync_manager.object_manager;
                    Self::scope_with_policy_context_rows(
                        &result_scope,
                        &sub.graph,
                        branches,
                        om,
                        storage,
                    )
                } else {
                    result_scope
                }
            };
            if new_scope != sub.last_scope {
                for (object_id, branch_name) in &new_scope {
                    let _ = self.sync_manager.object_manager.get_or_load(
                        *object_id,
                        storage,
                        &[branch_name.as_str().to_string()],
                    );
                }
                scope_updates.push((client_id, query_id, new_scope.clone(), sub.session.clone()));
                sub.last_scope = new_scope;
            }

            self.server_subscriptions.insert((client_id, query_id), sub);
        }

        // Apply scope updates
        for (client_id, query_id, new_scope, session) in scope_updates {
            self.sync_manager.set_client_query_scope_with_storage(
                storage, client_id, query_id, new_scope, session,
            );
        }

        for (client_id, warning) in schema_warning_notifications {
            self.sync_manager.emit_schema_warning(client_id, warning);
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
    pub(super) fn evaluate_write_permission<H: Storage>(
        &mut self,
        storage: &mut H,
        mut check: PendingPermissionCheck,
    ) {
        let table_name = match check.metadata.get(MetadataKey::Table.as_str()) {
            Some(t) => TableName::new(t),
            None => {
                tracing::trace!(
                    operation = ?check.operation,
                    metadata_keys = ?check.metadata.keys().collect::<Vec<_>>(),
                    "allowing write with no table metadata (non-row object)"
                );
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let branch_name = check
            .payload
            .branch_name()
            .unwrap_or_else(|| BranchName::new(self.current_branch()));
        let object_id = check.payload.object_id().unwrap_or_default();

        let branch_table_schema = match self.resolve_write_table_schema(table_name, branch_name) {
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
            && let Err(err) =
                self.validate_json_for_content(&branch_table_schema.columns, new_content)
        {
            self.sync_manager
                .reject_permission_check(check, err.to_string());
            return;
        }

        let (auth_schema, auth_context) = match self.authorization_schema_for_branch(&branch_name) {
            Some(parts) => parts,
            None => {
                if !self.authorization_schema_required {
                    self.sync_manager.approve_permission_check(storage, check);
                    return;
                }
                let wait_started_at = check
                    .schema_wait_started_at
                    .get_or_insert_with(Instant::now);
                let wait_elapsed = wait_started_at.elapsed();

                if wait_elapsed >= SCHEMA_RESOLUTION_TIMEOUT {
                    let reason = format!(
                        "{:?} denied on table {} - current permissions unavailable for branch {} after waiting {}s",
                        check.operation,
                        table_name.0,
                        branch_name,
                        SCHEMA_RESOLUTION_TIMEOUT.as_secs()
                    );
                    self.sync_manager.reject_permission_check(check, reason);
                } else {
                    self.sync_manager
                        .requeue_pending_permission_checks(vec![check]);
                }
                return;
            }
        };
        let Some(auth_table_schema) = auth_schema.get(&table_name) else {
            let reason = format!(
                "{:?} denied on table {} - table missing from current permission schema",
                check.operation, table_name.0
            );
            self.sync_manager.reject_permission_check(check, reason);
            return;
        };

        if check.operation == Operation::Update {
            self.evaluate_update_permission(
                storage,
                check,
                UpdatePermissionRequest {
                    object_id,
                    branch_name,
                    table_name,
                    branch_table_schema: &branch_table_schema,
                    auth_schema: &auth_schema,
                    auth_context: &auth_context,
                },
            );
            return;
        }

        let policy = match check.operation {
            Operation::Insert => auth_table_schema.policies.insert.with_check.as_ref(),
            Operation::Update => unreachable!(),
            Operation::Delete => auth_table_schema.policies.effective_delete_using(),
            Operation::Select => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let policy = match policy {
            Some(p) => p,
            None => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let content = match check.operation {
            Operation::Insert => check.new_content.as_ref(),
            Operation::Update => unreachable!(),
            Operation::Delete => check.old_content.as_ref(),
            Operation::Select => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };

        let content = match content {
            Some(content) if !content.is_empty() => content,
            None => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
            Some(_) => {
                self.sync_manager.approve_permission_check(storage, check);
                return;
            }
        };
        let provenance = match check.operation {
            Operation::Insert => Self::payload_row_provenance(&check.payload),
            Operation::Delete => self.current_row_provenance(storage, object_id, branch_name),
            Operation::Update | Operation::Select => None,
        };
        let Some(provenance) = provenance else {
            let reason = format!(
                "{:?} denied on table {} - missing row provenance",
                check.operation, table_name.0
            );
            self.sync_manager.reject_permission_check(check, reason);
            return;
        };
        let source_branch_schema_map = self.branch_schema_map.clone();

        if !self.evaluate_authorization_policy(
            storage,
            AuthorizationPolicyRequest {
                object_id,
                branch_name,
                table_name,
                policy,
                content,
                provenance: &provenance,
                session: &check.session,
                auth_schema: &auth_schema,
                auth_context: &auth_context,
                source_branch_schema_map: &source_branch_schema_map,
                operation: check.operation,
            },
        ) {
            let reason = format!(
                "{:?} denied by policy on table {}",
                check.operation, table_name.0
            );
            self.sync_manager.reject_permission_check(check, reason);
            return;
        }

        self.sync_manager.approve_permission_check(storage, check);
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
        request: UpdatePermissionRequest<'_>,
    ) {
        let UpdatePermissionRequest {
            object_id,
            branch_name,
            table_name,
            branch_table_schema,
            auth_schema,
            auth_context,
        } = request;

        if let Some(new_content) = check.new_content.as_ref()
            && let Err(err) =
                self.validate_json_for_content(&branch_table_schema.columns, new_content)
        {
            self.sync_manager
                .reject_permission_check(check, err.to_string());
            return;
        }

        let Some(table_schema) = auth_schema.get(&table_name) else {
            self.sync_manager.reject_permission_check(
                check,
                format!(
                    "Update denied on table {} - table missing from current permission schema",
                    table_name.0
                ),
            );
            return;
        };
        let using_policy = table_schema.policies.update.using.as_ref();
        let check_policy = table_schema.policies.update.with_check.as_ref();
        let source_branch_schema_map = self.branch_schema_map.clone();
        let old_provenance = self.current_row_provenance(storage, object_id, branch_name);
        let new_provenance = Self::payload_row_provenance(&check.payload);

        if using_policy.is_none() && check_policy.is_none() {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        if let Some(using) = using_policy {
            let old_content = match check.old_content.as_ref() {
                Some(c) if !c.is_empty() => c,
                _ => {
                    let reason = format!(
                        "Update denied by USING policy on table {} - no old content",
                        table_name.0
                    );
                    self.sync_manager.reject_permission_check(check, reason);
                    return;
                }
            };
            let Some(old_provenance) = old_provenance.as_ref() else {
                let reason = format!(
                    "Update denied by USING policy on table {} - missing old provenance",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            };

            if !self.evaluate_authorization_policy(
                storage,
                AuthorizationPolicyRequest {
                    object_id,
                    branch_name,
                    table_name,
                    policy: using,
                    content: old_content,
                    provenance: old_provenance,
                    session: &check.session,
                    auth_schema,
                    auth_context,
                    source_branch_schema_map: &source_branch_schema_map,
                    operation: Operation::Update,
                },
            ) {
                let reason = format!(
                    "Update denied by USING policy on table {} - cannot see old row",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }
        }

        if let Some(with_check) = check_policy {
            let new_content = match check.new_content.as_ref() {
                Some(c) => c,
                None => {
                    self.sync_manager.approve_permission_check(storage, check);
                    return;
                }
            };
            let Some(new_provenance) = new_provenance.as_ref() else {
                let reason = format!(
                    "Update denied by WITH CHECK policy on table {} - missing new provenance",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            };

            if !self.evaluate_authorization_policy(
                storage,
                AuthorizationPolicyRequest {
                    object_id,
                    branch_name,
                    table_name,
                    policy: with_check,
                    content: new_content,
                    provenance: new_provenance,
                    session: &check.session,
                    auth_schema,
                    auth_context,
                    source_branch_schema_map: &source_branch_schema_map,
                    operation: Operation::Update,
                },
            ) {
                let reason = format!(
                    "Update denied by WITH CHECK policy on table {}",
                    table_name.0
                );
                self.sync_manager.reject_permission_check(check, reason);
                return;
            }
        }

        self.sync_manager.approve_permission_check(storage, check);
    }

    /// Create policy graphs for complex clauses (INHERITS/EXISTS).
    pub(super) fn create_policy_graphs_for_complex_clauses(
        &self,
        clauses: &[ComplexClause],
        content: &[u8],
        descriptor: &RowDescriptor,
        _table: &TableName,
        session: &Session,
        branch: &str,
    ) -> Vec<PolicyGraph> {
        let mut graphs = Vec::new();

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
                        continue;
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
                        branch,
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
                        branch,
                    ) {
                        graphs.push(graph);
                    }
                }
                ComplexClause::ExistsRel { rel } => {
                    if let Some(graph) = PolicyGraph::for_exists_rel(rel, &self.schema, branch) {
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

        // Settle each active policy check
        for (pending_id, state) in &mut self.active_policy_checks {
            let branch = state.branch;
            let branches = vec![branch.as_str().to_string()];
            let mut row_loader = |id: ObjectId| -> Option<LoadedRow> {
                let (_, row) = Self::load_best_visible_row_version(storage, id, &branches)?;
                if row.is_hard_deleted() {
                    return None;
                }
                let version_id = row.version_id();
                let provenance = row.row_provenance();
                let source_branch = BranchName::new(&row.branch);
                Some(LoadedRow::new(
                    row.data,
                    version_id,
                    provenance,
                    [(id, source_branch)].into_iter().collect(),
                ))
            };

            // Settle all graphs
            let all_complete = state
                .graphs
                .iter_mut()
                .all(|g| g.settle(storage, &mut row_loader));

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
