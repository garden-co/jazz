use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::commit::CommitId;
use crate::metadata::MetadataKey;
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
    BatchBranchKey, ComposedBranchName, LoadedRow, QueryBranchRef, Row, RowDescriptor, Schema,
    SchemaHash, TableName, TableSchema, TupleProvenance, Value,
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
    pub is_soft_deleted: bool,
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
    pub(super) fn build_server_subscription_context(
        &self,
        ctx: &crate::schema_manager::QuerySchemaContext,
    ) -> Option<(Arc<Schema>, crate::schema_manager::SchemaContext)> {
        let target_schema =
            if !self.schema.is_empty() && self.schema_context.current_hash == ctx.schema_hash {
                self.schema.as_ref().clone()
            } else {
                self.known_schemas.get(&ctx.schema_hash)?.clone()
            };

        let mut schema_context = crate::schema_manager::SchemaContext::new_with_batch_id(
            target_schema.clone(),
            &ctx.env,
            &ctx.user_branch,
            ctx.batch_id,
        );

        for lens in self.schema_context.lenses.values() {
            schema_context.register_lens(lens.clone());
        }

        for (hash, schema) in self.known_schemas.iter() {
            if *hash != ctx.schema_hash {
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
            let branch = schema_context.branch_name_for_hash(*hash);
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
        let (table, tip_commit_id, tip_content) = {
            let object = self
                .sync_manager
                .object_manager
                .get_or_load(object_id, storage, &branches)?;
            let table = object.metadata.get(MetadataKey::Table.as_str())?.clone();
            let branch = object.branches.get(&branch_name)?;
            let tip = branch
                .tips
                .iter()
                .filter_map(|tip_id| branch.commits.get(tip_id).map(|commit| (*tip_id, commit)))
                .max_by_key(|(_, commit)| commit.timestamp)?;
            if tip.1.content.is_empty() {
                return None;
            }
            Some((table, tip.0, tip.1.content.clone()))
        }?;

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
            [(object_id, BatchBranchKey::from_branch_name(branch_name))]
                .into_iter()
                .collect(),
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

        let row = Row::new(object_id, transformed, CommitId([0; 32]));
        let evaluator = PolicyContextEvaluator::new(auth_schema, session, branch_name.as_str());
        let mut visited = HashSet::new();
        let mut row_loader = |related_id: ObjectId, _provenance: Option<&TupleProvenance>| {
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
        let Some((table, tip_content)) = ({
            let Some(object) = self
                .sync_manager
                .object_manager
                .get_or_load(object_id, storage, &branches)
            else {
                return false;
            };
            let Some(table) = object.metadata.get(MetadataKey::Table.as_str()).cloned() else {
                return false;
            };
            let Some(branch) = object.branches.get(&branch_name) else {
                return false;
            };
            let Some(tip_commit) = branch
                .tips
                .iter()
                .filter_map(|tip_id| branch.commits.get(tip_id))
                .max_by_key(|commit| commit.timestamp)
            else {
                return false;
            };
            if tip_commit.content.is_empty() {
                return false;
            }
            Some((table, tip_commit.content.clone()))
        }) else {
            return false;
        };

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

        let mut authorization_cache: HashMap<(ObjectId, BatchBranchKey), bool> = HashMap::new();

        graph
            .current_output_rows_with_provenance()
            .into_iter()
            .filter_map(|(row, provenance)| {
                provenance
                    .iter()
                    .copied()
                    .all(|(object_id, branch_key)| {
                        *authorization_cache
                            .entry((object_id, branch_key))
                            .or_insert_with(|| {
                                self.provenance_row_matches_current_select_policy(
                                    storage,
                                    object_id,
                                    branch_key.branch_name(),
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
    ) -> HashSet<(ObjectId, BatchBranchKey)> {
        let Some((auth_schema, auth_context)) =
            self.authorization_schema_for_context(&schema_context.env, &schema_context.user_branch)
        else {
            if !self.authorization_schema_required {
                return graph.sync_scope_object_keys();
            }
            return HashSet::new();
        };

        if auth_schema
            .values()
            .all(|table_schema| table_schema.policies.select.using.is_none())
        {
            return graph.sync_scope_object_keys();
        }

        let mut authorization_cache: HashMap<(ObjectId, BatchBranchKey), bool> = HashMap::new();

        graph
            .current_output_tuples()
            .into_iter()
            .filter_map(|tuple| {
                tuple
                    .provenance()
                    .iter()
                    .copied()
                    .all(|(object_id, branch_key)| {
                        *authorization_cache
                            .entry((object_id, branch_key))
                            .or_insert_with(|| {
                                self.provenance_row_matches_current_select_policy(
                                    storage,
                                    object_id,
                                    branch_key.branch_name(),
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
        branches: &[QueryBranchRef],
        context: &mut RowTransformContext<'_>,
    ) -> Option<ResolvedSchemaRow> {
        let mut best: Option<(u64, CommitId, Vec<u8>, BranchName, bool)> = None;

        for branch_ref in branches {
            let branch_name = branch_ref.branch_name();
            let Some(branch) = obj.branches.get(&branch_name) else {
                continue;
            };
            for &tip_id in &branch.tips {
                let Some(commit) = branch.commits.get(&tip_id) else {
                    continue;
                };
                let is_soft_deleted = commit.is_soft_deleted();
                match &best {
                    None => {
                        best = Some((
                            commit.timestamp,
                            tip_id,
                            commit.content.clone(),
                            branch_name,
                            is_soft_deleted,
                        ));
                    }
                    Some((best_ts, best_id, _, _, _))
                        if (commit.timestamp, tip_id) > (*best_ts, *best_id) =>
                    {
                        best = Some((
                            commit.timestamp,
                            tip_id,
                            commit.content.clone(),
                            branch_name,
                            is_soft_deleted,
                        ));
                    }
                    _ => {}
                }
            }
        }

        let (_, commit_id, content, branch_name, is_soft_deleted) = best?;
        if content.is_empty() {
            return None;
        }
        Self::transform_row_with_schema(
            id,
            content,
            commit_id,
            branch_name,
            is_soft_deleted,
            context,
        )
    }

    pub(super) fn transform_row_with_schema(
        id: ObjectId,
        content: Vec<u8>,
        commit_id: CommitId,
        branch_name: BranchName,
        is_soft_deleted: bool,
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
                        is_soft_deleted,
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
            is_soft_deleted,
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

    fn scope_with_policy_context_rows_from_object_manager(
        base_scope: &HashSet<(ObjectId, BatchBranchKey)>,
        graph: &super::graph::QueryGraph,
        branches: &[QueryBranchRef],
        object_manager: &crate::object_manager::ObjectManager,
    ) -> HashSet<(ObjectId, BatchBranchKey)> {
        let mut scope = base_scope.clone();

        let policy_tables: HashSet<TableName> = graph
            .policy_filter_tables
            .iter()
            .map(|(_, table)| *table)
            .collect();
        if policy_tables.is_empty() {
            return scope;
        }

        let branch_keys: Vec<BatchBranchKey> = branches
            .iter()
            .map(QueryBranchRef::batch_branch_key)
            .collect();
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

            for branch_key in &branch_keys {
                let Some(branch) = object.branches.get(&branch_key.branch_name()) else {
                    continue;
                };
                if Self::branch_has_live_tip(branch) {
                    scope.insert((*object_id, *branch_key));
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
                self.build_server_subscription_context(&sub.schema_context)
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
            let branches = match Self::resolve_query_branches_for_context(
                storage,
                &query_for_compile,
                &subscription_context,
            ) {
                Ok(branches) => branches,
                Err(error) => {
                    tracing::warn!(
                        %sub.client_id,
                        query_id = sub.query_id.0,
                        table = %sub.query.table,
                        error = %error,
                        "failed to resolve server query branches; falling back to schema-context defaults"
                    );
                    if query_for_compile.branches.is_empty() {
                        subscription_context
                            .all_branch_names()
                            .into_iter()
                            .map(QueryBranchRef::from_branch_name)
                            .collect()
                    } else {
                        query_for_compile
                            .branches
                            .iter()
                            .map(|branch| {
                                Self::resolve_query_branch_ref_for_context(
                                    &subscription_context,
                                    branch,
                                )
                            })
                            .collect()
                    }
                }
            };
            let graph = Self::compile_graph(
                Some(storage),
                &query_for_compile,
                &branches,
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

            let table = sub.query.table.as_str().to_string();
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let include_deleted = sub.query.include_deleted;
            let mut transform_context = RowTransformContext {
                table: &table,
                branch_schema_map: &branch_schema_map,
                schema_context: &subscription_context,
                schema_warnings: &mut schema_warnings,
            };
            {
                let om = &mut self.sync_manager.object_manager;
                let row_loader =
                    |id: ObjectId, provenance: Option<&TupleProvenance>| -> Option<LoadedRow> {
                        let candidate_branches =
                            Self::candidate_query_branches_for_row(id, provenance, &branches);
                        let candidate_branch_names =
                            Self::branch_names_for_query_branches(&candidate_branches);
                        let obj = om.get_or_load_tips(id, storage_ref, &candidate_branch_names)?;
                        let resolved = Self::resolve_latest_row_with_schema_transform(
                            id,
                            obj,
                            &candidate_branches,
                            &mut transform_context,
                        )?;
                        if resolved.is_soft_deleted && !include_deleted {
                            return None;
                        }
                        Some(LoadedRow::new(
                            resolved.content,
                            resolved.commit_id,
                            [(id, BatchBranchKey::from_branch_name(resolved.branch_name))]
                                .into_iter()
                                .collect(),
                        ))
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
                graph.sync_scope_object_keys()
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
            self.sync_manager.set_client_query_scope_keys(
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
                    subscription_context.query_context(),
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
            HashSet<(ObjectId, BatchBranchKey)>,
            Option<Session>,
        )> = Vec::new();
        let mut removed_row_resyncs: Vec<(ClientId, ObjectId, BatchBranchKey)> = Vec::new();
        let mut settled_notifications: Vec<(ClientId, QueryId)> = Vec::new();
        let mut schema_warning_notifications: Vec<(ClientId, crate::sync_manager::SchemaWarning)> =
            Vec::new();

        let subscription_keys: Vec<_> = self.server_subscriptions.keys().copied().collect();

        for (client_id, query_id) in subscription_keys {
            let Some(mut sub) = self.server_subscriptions.remove(&(client_id, query_id)) else {
                continue;
            };
            let previous_scope = sub.last_scope.clone();
            let branches = &sub.branches;
            let table = sub.query.table.as_str().to_string();
            let include_deleted = sub.query.include_deleted;
            let branch_schema_map = Self::branch_schema_map_for_context(&sub.schema_context);
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let mut transform_context = RowTransformContext {
                table: &table,
                branch_schema_map: &branch_schema_map,
                schema_context: &sub.schema_context,
                schema_warnings: &mut schema_warnings,
            };

            // Row loader for this subscription
            let new_scope = {
                {
                    let om = &mut self.sync_manager.object_manager;
                    let row_loader =
                        |id: ObjectId, provenance: Option<&TupleProvenance>| -> Option<LoadedRow> {
                            let candidate_branches =
                                Self::candidate_query_branches_for_row(id, provenance, branches);
                            let candidate_branch_names =
                                Self::branch_names_for_query_branches(&candidate_branches);
                            let obj = om.get_or_load_tips(id, storage, &candidate_branch_names)?;
                            let resolved = Self::resolve_latest_row_with_schema_transform(
                                id,
                                obj,
                                &candidate_branches,
                                &mut transform_context,
                            )?;
                            if resolved.is_soft_deleted && !include_deleted {
                                return None;
                            }
                            Some(LoadedRow::new(
                                resolved.content,
                                resolved.commit_id,
                                [(id, BatchBranchKey::from_branch_name(resolved.branch_name))]
                                    .into_iter()
                                    .collect(),
                            ))
                        };

                    let _delta = sub.graph.settle(storage, row_loader);
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
                        sub.graph.sync_scope_object_keys()
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
                        Self::scope_with_policy_context_rows_from_object_manager(
                            &result_scope,
                            &sub.graph,
                            branches,
                            om,
                        )
                    } else {
                        result_scope
                    }
                }
            };
            if new_scope != sub.last_scope {
                scope_updates.push((client_id, query_id, new_scope.clone(), sub.session.clone()));
                sub.last_scope = new_scope;
            }
            for (object_id, old_branch_key) in previous_scope.difference(&sub.last_scope) {
                if sub
                    .branches
                    .iter()
                    .any(|branch| branch.batch_branch_key() == *old_branch_key)
                {
                    continue;
                }

                for branch in &sub.branches {
                    removed_row_resyncs.push((client_id, *object_id, branch.batch_branch_key()));
                }
            }

            self.server_subscriptions.insert((client_id, query_id), sub);
        }

        // Apply scope updates
        for (client_id, query_id, new_scope, session) in scope_updates {
            self.sync_manager
                .set_client_query_scope_keys(client_id, query_id, new_scope, session);
        }

        let removed_row_resyncs: HashSet<_> = removed_row_resyncs.into_iter().collect();
        for (client_id, object_id, branch_key) in removed_row_resyncs {
            let Some(object) = self.sync_manager.object_manager.get(object_id) else {
                continue;
            };
            let Some(branch) = object.branches.get(&branch_key.branch_name()) else {
                continue;
            };
            let metadata = object.metadata.clone();
            let tips = branch.tips.iter().copied().collect();
            self.sync_manager.queue_tips_to_client_unscoped(
                client_id,
                object_id,
                metadata,
                branch_key.branch_name(),
                tips,
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
        let schema_hash = self.branch_schema_map.get(branch_name.as_str()).copied();

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

        if let Some(composed) = ComposedBranchName::parse(&branch_name) {
            let Some(schema_hash) = self.find_schema_by_short_hash(&composed.schema_hash) else {
                return WriteSchemaResolution::PendingSchema;
            };

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

        let (object_id, branch_name) = match &check.payload {
            SyncPayload::ObjectUpdated {
                object_id,
                branch_name,
                ..
            }
            | SyncPayload::ObjectTruncated {
                object_id,
                branch_name,
                ..
            } => (*object_id, *branch_name),
            payload => {
                tracing::error!(
                    operation = ?check.operation,
                    payload = payload.variant_name(),
                    "dropping unexpected non-write payload in pending permission check"
                );
                return;
            }
        };

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
        let source_branch_schema_map = self.branch_schema_map.clone();

        if !self.evaluate_authorization_policy(
            storage,
            AuthorizationPolicyRequest {
                object_id,
                branch_name,
                table_name,
                policy,
                content,
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

            if !self.evaluate_authorization_policy(
                storage,
                AuthorizationPolicyRequest {
                    object_id,
                    branch_name,
                    table_name,
                    policy: using,
                    content: old_content,
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

            if !self.evaluate_authorization_policy(
                storage,
                AuthorizationPolicyRequest {
                    object_id,
                    branch_name,
                    table_name,
                    policy: with_check,
                    content: new_content,
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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_policy_graphs_for_complex_clauses(
        &self,
        storage: &dyn Storage,
        clauses: &[ComplexClause],
        content: &[u8],
        descriptor: &RowDescriptor,
        _table: &TableName,
        session: &Session,
        branch: &BranchName,
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
                        branch.as_str(),
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
                        branch.as_str(),
                        storage,
                    ) {
                        graphs.push(graph);
                    }
                }
                ComplexClause::ExistsRel { rel } => {
                    if let Some(graph) =
                        PolicyGraph::for_exists_rel(rel, &self.schema, branch.as_str(), storage)
                    {
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
        let om = &mut self.sync_manager.object_manager;
        let storage_ref: &dyn Storage = storage;

        // Settle each active policy check
        for (pending_id, state) in &mut self.active_policy_checks {
            let branch = state.branch;
            let requested_branches = vec![QueryBranchRef::from_branch_name(branch)];
            let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
            let schema_context = self.schema_context.clone();
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let mut row_loader =
                |id: ObjectId, provenance: Option<&TupleProvenance>| -> Option<LoadedRow> {
                    let candidate_branches =
                        Self::candidate_query_branches_for_row(id, provenance, &requested_branches);
                    let candidate_branch_names =
                        Self::branch_names_for_query_branches(&candidate_branches);
                    let obj = om.get_or_load_tips(id, storage_ref, &candidate_branch_names)?;
                    let table = obj.metadata.get(MetadataKey::Table.as_str())?.clone();
                    let mut transform_context = RowTransformContext {
                        table: &table,
                        branch_schema_map: &branch_schema_map,
                        schema_context: &schema_context,
                        schema_warnings: &mut schema_warnings,
                    };
                    let resolved = Self::resolve_latest_row_with_schema_transform(
                        id,
                        obj,
                        &candidate_branches,
                        &mut transform_context,
                    )?;
                    if resolved.content.is_empty() {
                        return None;
                    }
                    Some(LoadedRow::new(
                        resolved.content,
                        resolved.commit_id,
                        [(id, BatchBranchKey::from_branch_name(resolved.branch_name))]
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
