use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metadata::{MetadataKey, RowProvenance};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::row_histories::BatchId;
use crate::schema_manager::LensTransformer;
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, ClientRole, DurabilityTier, PendingPermissionCheck, SyncPayload,
};

use super::manager::{QueryManager, SchemaWarningAccumulator, ServerQuerySubscription};
use super::permission_routing::{PermissionEvaluationRequest, branch_policy_scope};
use super::policy::{ComplexClause, Operation};
use super::policy_graph::{PolicyGraph, PolicyGraphBuildOptions};
use super::session::Session;
use super::settlement_eval_cache::SettlementEvalCache;
use super::types::{
    ComposedBranchName, LoadedRow, PermissionPhase, RowDescriptor, Schema, SchemaHash, TableName,
    TableSchema, Tuple, TupleElement, TupleProvenance, Value,
};

const MAX_INITIAL_QUERY_REPLAY_OUTBOX_PER_PASS: usize = 32;

enum WriteSchemaResolution {
    Resolved(Box<TableSchema>),
    PendingSchema,
    Unresolved,
}

enum AuthorizedTuplesResult {
    Ready(Vec<super::types::Tuple>),
    PermissionsUnavailable,
}

pub(super) struct ResolvedSchemaRow {
    pub branch_name: BranchName,
    pub batch_id: BatchId,
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

struct UpdatePermissionRequest<'a> {
    object_id: ObjectId,
    branch_name: BranchName,
    table_name: TableName,
    branch_table_schema: &'a TableSchema,
    auth_schema: &'a Schema,
    auth_context: &'a crate::schema_manager::SchemaContext,
}

impl QueryManager {
    fn should_emit_query_settled_to_downstream(
        required_tier: Option<DurabilityTier>,
        tier: DurabilityTier,
        sent_below_required_settled: &mut bool,
        last_emitted_settled_tier: &mut Option<DurabilityTier>,
        scope_changed: bool,
    ) -> bool {
        let is_required_tier = required_tier.is_none_or(|required_tier| tier >= required_tier);

        if is_required_tier
            && (scope_changed || last_emitted_settled_tier.is_none_or(|last_tier| tier > last_tier))
        {
            *last_emitted_settled_tier =
                Some(last_emitted_settled_tier.map_or(tier, |last_tier| last_tier.max(tier)));
            return true;
        }

        if !is_required_tier && !*sent_below_required_settled {
            *sent_below_required_settled = true;
            *last_emitted_settled_tier =
                Some(last_emitted_settled_tier.map_or(tier, |last_tier| last_tier.max(tier)));
            return true;
        }

        false
    }

    pub(super) fn missing_permissions_head_reason() -> &'static str {
        "backend has no published permissions head; push permissions before running session-scoped queries or writes against this backend"
    }

    fn current_row_provenance(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch_name: BranchName,
    ) -> Option<RowProvenance> {
        let branches = vec![branch_name.as_str().to_string()];
        let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
        let (_, row) = self.load_best_visible_row_batch(
            storage,
            object_id,
            &branches,
            None,
            &self.schema_context,
            &branch_schema_map,
        )?;
        Some(row.row_provenance())
    }

    fn payload_row_provenance(payload: &SyncPayload) -> Option<RowProvenance> {
        match payload {
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                Some(row.row_provenance())
            }
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
                schema_context.add_pending_schema_with_hash(*hash, schema.clone());
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
        &mut self,
        env: &str,
        user_branch: &str,
    ) -> Option<(Arc<Schema>, Arc<crate::schema_manager::SchemaContext>)> {
        if self.authorization_schema_required && self.authorization_schema.is_none() {
            return None;
        }

        let schema = self
            .authorization_schema
            .clone()
            .or_else(|| (!self.schema.is_empty()).then(|| self.schema.clone()))?;

        let cache_key = (env.to_string(), user_branch.to_string());
        if let Some(context) = self.authorization_context_cache.get(&cache_key) {
            return Some((schema, context.clone()));
        }

        let mut schema_context =
            crate::schema_manager::SchemaContext::new((*schema).clone(), env, user_branch);

        for lens in self.schema_context.lenses.values() {
            schema_context.register_lens(lens.clone());
        }

        for (hash, known_schema) in self.known_schemas.iter() {
            if *hash != schema_context.current_hash {
                schema_context.add_pending_schema_with_hash(*hash, known_schema.clone());
            }
        }

        schema_context.try_activate_pending();

        let schema_context = Arc::new(schema_context);
        self.authorization_context_cache
            .insert(cache_key, schema_context.clone());

        Some((schema, schema_context))
    }

    pub(super) fn authorization_schema_for_branch(
        &mut self,
        branch_name: &BranchName,
    ) -> Option<(Arc<Schema>, Arc<crate::schema_manager::SchemaContext>)> {
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
                    schema_context.add_pending_schema_with_hash(*hash, known_schema.clone());
                }
            }

            schema_context.try_activate_pending();

            return Some((Arc::new(target_schema), Arc::new(schema_context)));
        }

        if self.schema_context.is_initialized() {
            let env = self.schema_context.env.clone();
            let user_branch = self.schema_context.user_branch.clone();
            return self
                .authorization_schema_for_context(&env, &user_branch)
                .or_else(|| Some((self.schema.clone(), Arc::new(self.schema_context.clone()))));
        }

        None
    }

    pub(super) fn transform_content_to_authorization_schema(
        &self,
        table: &str,
        content: &[u8],
        batch_id: BatchId,
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
            .transform(content, batch_id, source_hash)
            .ok()
            .map(|result| result.data)
    }

    pub(super) fn load_row_for_authorization_context(
        &mut self,
        storage: &dyn Storage,
        object_id: ObjectId,
        branch_name: BranchName,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        auth_context: &crate::schema_manager::SchemaContext,
    ) -> Option<LoadedRow> {
        let branches = vec![branch_name.as_str().to_string()];
        let (table, row) = self.load_best_visible_row_batch(
            storage,
            object_id,
            &branches,
            None,
            auth_context,
            source_branch_schema_map,
        )?;
        if row.is_hard_deleted() {
            return None;
        }

        let tip_batch_id = row.batch_id;
        let tip_content = row.data.clone();
        let tip_provenance = row.row_provenance();

        let transformed = self.transform_content_to_authorization_schema(
            &table,
            &tip_content,
            tip_batch_id,
            branch_name,
            source_branch_schema_map,
            auth_context,
        )?;

        Some(LoadedRow::new(
            transformed,
            tip_provenance,
            [(object_id, branch_name)].into_iter().collect(),
            row.batch_id,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn provenance_row_matches_current_select_policy(
        &mut self,
        storage: &dyn Storage,
        settlement_eval_cache: &mut SettlementEvalCache,
        object_id: ObjectId,
        branch_name: BranchName,
        session: Option<&Session>,
        auth_schema: &Schema,
        auth_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
    ) -> bool {
        let branches = vec![branch_name.as_str().to_string()];
        let Some((table, row)) = self.load_best_visible_row_batch(
            storage,
            object_id,
            &branches,
            None,
            auth_context,
            source_branch_schema_map,
        ) else {
            return false;
        };
        if row.is_hard_deleted() {
            return false;
        }

        let tip_content = row.data.clone();
        let tip_provenance = row.row_provenance();

        let table_name = TableName::new(&table);
        let Some(session) = session else {
            if branch_policy_scope(&branch_name).is_some() {
                return false;
            }
            let Some(table_schema) = auth_schema.get(&table_name) else {
                return false;
            };
            return table_schema.policies.select_policy().is_none()
                && !self.row_policy_mode.denies_missing_explicit_policy();
        };

        let route = self.resolve_permission_route(
            storage,
            branch_name,
            table_name,
            session,
            auth_schema,
            auth_context,
            source_branch_schema_map,
        );
        if route.is_denied() {
            return false;
        }

        self.evaluate_permission_route(
            storage,
            &route,
            PermissionEvaluationRequest {
                object_id,
                branch_name,
                table_name,
                content: &tip_content,
                provenance: &tip_provenance,
                session,
                auth_schema,
                auth_context,
                source_branch_schema_map,
                operation: Operation::Select,
                phase: PermissionPhase::Using,
                settlement_eval_cache: Some(settlement_eval_cache),
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn authorized_tuple_provenance(
        &mut self,
        storage: &dyn Storage,
        settlement_eval_cache: &mut SettlementEvalCache,
        tuple: &Tuple,
        session: Option<&Session>,
        auth_schema: &Schema,
        auth_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        authorization_cache: &mut HashMap<(ObjectId, BranchName), bool>,
    ) -> Option<TupleProvenance> {
        let mut authorized = TupleProvenance::new();
        for (object_id, branch_name) in tuple.provenance().iter().copied() {
            let allowed = *authorization_cache
                .entry((object_id, branch_name))
                .or_insert_with(|| {
                    self.provenance_row_matches_current_select_policy(
                        storage,
                        settlement_eval_cache,
                        object_id,
                        branch_name,
                        session,
                        auth_schema,
                        auth_context,
                        source_branch_schema_map,
                    )
                });
            if allowed {
                authorized.insert((object_id, branch_name));
            }
        }

        let output_rows_are_visible = tuple.iter().all(|element| {
            authorized
                .iter()
                .any(|(object_id, _)| *object_id == element.id())
        });
        output_rows_are_visible.then_some(authorized)
    }

    fn filter_unauthorized_nested_rows(value: &mut Value, authorized_ids: &HashSet<ObjectId>) {
        match value {
            Value::Array(values) => {
                values.retain_mut(|value| match value {
                    Value::Row {
                        id: Some(object_id),
                        values,
                    } => {
                        if !authorized_ids.contains(object_id) {
                            return false;
                        }
                        for value in values {
                            Self::filter_unauthorized_nested_rows(value, authorized_ids);
                        }
                        true
                    }
                    other => {
                        Self::filter_unauthorized_nested_rows(other, authorized_ids);
                        true
                    }
                });
            }
            Value::Row { values, .. } => {
                for value in values {
                    Self::filter_unauthorized_nested_rows(value, authorized_ids);
                }
            }
            _ => {}
        }
    }

    fn tuple_with_authorized_scope(
        mut tuple: Tuple,
        descriptor: &RowDescriptor,
        authorized_provenance: TupleProvenance,
    ) -> Option<Tuple> {
        let authorized_ids: HashSet<ObjectId> = authorized_provenance
            .iter()
            .map(|(object_id, _)| *object_id)
            .collect();

        if tuple.len() == 1
            && let Some(TupleElement::Row { content, .. }) = tuple.get_mut(0)
        {
            let mut values = decode_row(descriptor, content).ok()?;
            for value in &mut values {
                Self::filter_unauthorized_nested_rows(value, &authorized_ids);
            }
            *content = encode_row(descriptor, &values).ok()?.into();
        }

        Some(tuple.with_provenance(authorized_provenance))
    }

    fn authorized_tuples_from_graph_result(
        &mut self,
        storage: &dyn Storage,
        settlement_eval_cache: &mut SettlementEvalCache,
        graph: &super::graph::QueryGraph,
        schema_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        session: Option<&Session>,
    ) -> AuthorizedTuplesResult {
        if self.authorization_schema_required && self.authorization_schema.is_none() {
            return AuthorizedTuplesResult::PermissionsUnavailable;
        }

        let Some((auth_schema, auth_context)) =
            self.authorization_schema_for_context(&schema_context.env, &schema_context.user_branch)
        else {
            if !self.authorization_schema_required {
                return AuthorizedTuplesResult::Ready(graph.current_output_tuples());
            }
            return AuthorizedTuplesResult::PermissionsUnavailable;
        };

        if !self.row_policy_mode.denies_missing_explicit_policy()
            && Self::schema_has_no_explicit_select_policies(&auth_schema)
        {
            return AuthorizedTuplesResult::Ready(graph.current_output_tuples());
        }

        let mut authorization_cache: HashMap<(ObjectId, BranchName), bool> = HashMap::new();

        AuthorizedTuplesResult::Ready(
            graph
                .current_output_tuples()
                .into_iter()
                .filter_map(|tuple| {
                    let authorized_provenance = self.authorized_tuple_provenance(
                        storage,
                        settlement_eval_cache,
                        &tuple,
                        session,
                        &auth_schema,
                        &auth_context,
                        source_branch_schema_map,
                        &mut authorization_cache,
                    )?;
                    Self::tuple_with_authorized_scope(
                        tuple,
                        &graph.combined_descriptor,
                        authorized_provenance,
                    )
                })
                .collect(),
        )
    }

    pub(super) fn authorized_tuples_from_graph_with_cache(
        &mut self,
        storage: &dyn Storage,
        settlement_eval_cache: &mut SettlementEvalCache,
        graph: &super::graph::QueryGraph,
        schema_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        session: Option<&Session>,
    ) -> Vec<super::types::Tuple> {
        match self.authorized_tuples_from_graph_result(
            storage,
            settlement_eval_cache,
            graph,
            schema_context,
            source_branch_schema_map,
            session,
        ) {
            AuthorizedTuplesResult::Ready(tuples) => tuples,
            AuthorizedTuplesResult::PermissionsUnavailable => Vec::new(),
        }
    }

    fn authorized_scope_from_graph_if_available(
        &mut self,
        storage: &dyn Storage,
        settlement_eval_cache: &mut SettlementEvalCache,
        graph: &super::graph::QueryGraph,
        schema_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &std::collections::HashMap<String, SchemaHash>,
        session: Option<&Session>,
    ) -> Option<HashSet<(ObjectId, BranchName)>> {
        let Some((auth_schema, auth_context)) =
            self.authorization_schema_for_context(&schema_context.env, &schema_context.user_branch)
        else {
            if !self.authorization_schema_required {
                return Some(graph.sync_scope_object_ids());
            }
            return None;
        };

        if !self.row_policy_mode.denies_missing_explicit_policy()
            && Self::schema_has_no_explicit_select_policies(&auth_schema)
        {
            return Some(graph.sync_scope_object_ids());
        }

        let mut authorization_cache: HashMap<(ObjectId, BranchName), bool> = HashMap::new();
        let mut authorized_scope_by_tuple: HashMap<Vec<ObjectId>, TupleProvenance> = HashMap::new();

        let authorized_scope_tuples = graph.filtered_sync_scope_tuples(|tuple| {
            let key = tuple.ids();
            match self.authorized_tuple_provenance(
                storage,
                settlement_eval_cache,
                tuple,
                session,
                &auth_schema,
                &auth_context,
                source_branch_schema_map,
                &mut authorization_cache,
            ) {
                Some(provenance) => {
                    authorized_scope_by_tuple.insert(key, provenance);
                    true
                }
                None => false,
            }
        });

        Some(
            authorized_scope_tuples
                .into_iter()
                .flat_map(|tuple| {
                    authorized_scope_by_tuple
                        .remove(&tuple.ids())
                        .unwrap_or_default()
                        .into_iter()
                })
                .collect(),
        )
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

    pub(super) fn resolved_server_query_branches_for_graph(
        query: &crate::query_manager::query::Query,
        schema_context: &crate::schema_manager::SchemaContext,
        graph: &crate::query_manager::graph::QueryGraph,
    ) -> Vec<String> {
        fn push_unique(branches: &mut Vec<String>, branch: String) {
            if !branches.iter().any(|existing| existing == &branch) {
                branches.push(branch);
            }
        }

        let scan_branches = graph.scan_branches();
        let mut branches = if !scan_branches.is_empty() {
            scan_branches
        } else {
            Self::resolved_server_query_branches(query, schema_context)
        };

        for branch in query.explicit_array_subquery_branches() {
            push_unique(&mut branches, branch);
        }

        branches
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
        batch_id: BatchId,
        branch_name: BranchName,
        context: &mut RowTransformContext<'_>,
    ) -> Option<ResolvedSchemaRow> {
        let source_hash = context.branch_schema_map.get(branch_name.as_str()).copied();

        if let Some(source_hash) = source_hash
            && source_hash != context.schema_context.current_hash
        {
            let transformer = LensTransformer::new(context.schema_context, context.table);
            match transformer.transform(&content, batch_id, source_hash) {
                Ok(result) => {
                    return Some(ResolvedSchemaRow {
                        branch_name,
                        batch_id: result.batch_id,
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
            batch_id,
            content,
        })
    }

    fn client_bypasses_authorization_filtering(
        &self,
        client_id: ClientId,
        session: Option<&Session>,
    ) -> bool {
        self.sync_manager
            .get_client(client_id)
            .map(|client| {
                matches!(client.role, ClientRole::Peer | ClientRole::Admin)
                    || matches!(client.role, ClientRole::Backend)
                        && client.session.is_none()
                        && session.is_none()
            })
            .unwrap_or(false)
    }

    fn schema_has_no_explicit_select_policies(schema: &Schema) -> bool {
        schema.values().all(|table_schema| {
            table_schema.policies.select.using.is_none()
                && table_schema
                    .policies
                    .for_branch
                    .values()
                    .all(|policies| policies.select.using.is_none())
        })
    }

    fn scope_with_policy_context_rows_for_tables<H: Storage + ?Sized>(
        base_scope: &HashSet<(ObjectId, BranchName)>,
        policy_tables: &HashSet<TableName>,
        branches: &[String],
        storage: &H,
    ) -> HashSet<(ObjectId, BranchName)> {
        let mut scope = base_scope.clone();
        if policy_tables.is_empty() {
            return scope;
        }

        let branch_names: Vec<BranchName> = branches.iter().map(BranchName::new).collect();
        let Ok(objects) = storage.scan_row_locators() else {
            return scope;
        };
        for (object_id, row_locator) in objects {
            let table_name = row_locator.table.as_str();
            if !policy_tables
                .iter()
                .any(|table| table.as_str() == table_name)
            {
                continue;
            }

            for branch_name in &branch_names {
                let Some(row) = storage
                    .load_visible_region_row(table_name, branch_name.as_str(), object_id)
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                if !row.is_hard_deleted() {
                    scope.insert((object_id, *branch_name));
                }
            }
        }

        scope
    }

    fn merged_policy_context_tables(
        graph: &super::graph::QueryGraph,
        explicit_tables: &[String],
    ) -> HashSet<TableName> {
        let mut policy_tables: HashSet<TableName> = graph
            .policy_filter_tables
            .iter()
            .map(|(_, table)| *table)
            .collect();
        policy_tables.extend(explicit_tables.iter().map(TableName::new));
        policy_tables
    }

    /// Process pending query subscriptions from downstream clients.
    ///
    /// For each pending subscription:
    /// 1. Build a QueryGraph with the client's session
    /// 2. Settle the graph to get contributing ObjectIds
    /// 3. Set the scope in SyncManager (which triggers initial sync)
    pub(super) fn process_pending_query_subscriptions<H: Storage>(&mut self, storage: &mut H) {
        let pending = self.sync_manager.take_pending_query_subscriptions();
        let mut pending_by_key = HashMap::new();
        let mut pending_keys = Vec::new();
        for sub in pending {
            let key = (sub.client_id, sub.query_id);
            if !pending_by_key.contains_key(&key) {
                pending_keys.push(key);
            }
            pending_by_key.insert(key, sub);
        }
        let mut deferred = Vec::new();
        let mut schema_warning_notifications = Vec::new();

        for (key_index, key) in pending_keys.iter().copied().enumerate() {
            let Some(sub) = pending_by_key.remove(&key) else {
                continue;
            };
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
            let existing_subscription_state = self
                .server_subscriptions
                .get(&(sub.client_id, sub.query_id))
                .map(|existing| {
                    (
                        existing.query == sub.query
                            && existing.session == session_for_policy
                            && existing.required_tier == sub.required_tier
                            && existing.propagation == sub.propagation
                            && existing.policy_context_tables == sub.policy_context_tables,
                        existing.sent_below_required_settled,
                        existing.last_emitted_settled_tier,
                        existing.last_scope.clone(),
                        existing.settled_once,
                    )
                });
            let equivalent_existing_subscription = existing_subscription_state
                .as_ref()
                .is_some_and(|(equivalent, ..)| *equivalent);

            if equivalent_existing_subscription
                && existing_subscription_state
                    .as_ref()
                    .is_some_and(|(_, _, _, _, settled_once)| *settled_once)
            {
                let settled_tier = self
                    .sync_manager
                    .max_local_durability_tier()
                    .unwrap_or(DurabilityTier::Local);
                let mut emission_scope = None;

                if let Some(existing) = self
                    .server_subscriptions
                    .get_mut(&(sub.client_id, sub.query_id))
                    && Self::should_emit_query_settled_to_downstream(
                        existing.required_tier,
                        settled_tier,
                        &mut existing.sent_below_required_settled,
                        &mut existing.last_emitted_settled_tier,
                        false,
                    )
                {
                    emission_scope = Some(existing.last_scope.clone());
                }

                if let Some(scope) = emission_scope.as_ref() {
                    self.sync_manager.emit_query_settled(
                        sub.client_id,
                        sub.query_id,
                        settled_tier,
                        scope,
                    );
                }

                continue;
            }

            // Build QueryGraph with client's session for policy filtering (schema-aware)
            let query_for_compile =
                Self::query_for_server_compile(&sub.query, &subscription_context);
            let authorization_schema = self
                .authorization_schema_for_context(
                    &subscription_context.env,
                    &subscription_context.user_branch,
                )
                .map(|(auth_schema, _)| auth_schema);
            let (compile_schema, compile_row_policy_mode, _) =
                Self::compile_options_with_authorization_schema(
                    schema_for_compile.as_ref(),
                    authorization_schema.as_deref(),
                    self.row_policy_mode,
                    session_for_policy.as_ref(),
                );
            let graph = Self::compile_graph(
                &query_for_compile,
                &compile_schema,
                session_for_policy.clone(),
                &subscription_context,
                compile_row_policy_mode,
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
                    "query_compilation_failed",
                    reason,
                );
                continue;
            };

            let branch_schema_map = Self::branch_schema_map_for_context(&subscription_context);

            // Initial settle to populate the graph
            let storage_ref: &dyn Storage = storage;

            let branches = Self::resolved_server_query_branches_for_graph(
                &query_for_compile,
                &subscription_context,
                &graph,
            );
            let table = sub.query.table.as_str().to_string();
            let mut schema_warnings = SchemaWarningAccumulator::default();
            let include_deleted = sub.query.include_deleted;
            {
                let row_loader =
                    |id: ObjectId, table_hint: Option<TableName>| -> Option<LoadedRow> {
                        Self::load_visible_row_for_query(
                            storage_ref,
                            id,
                            table_hint.as_ref().map(TableName::as_str),
                            &branches,
                            None,
                            None,
                            false,
                            false,
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
            let policy_context_tables =
                Self::merged_policy_context_tables(&graph, &sub.policy_context_tables);
            let scope = if self
                .client_bypasses_authorization_filtering(sub.client_id, session_for_policy.as_ref())
            {
                let result_scope = graph.sync_scope_object_ids();
                Some(if !policy_context_tables.is_empty() {
                    Self::scope_with_policy_context_rows_for_tables(
                        &result_scope,
                        &policy_context_tables,
                        &branches,
                        storage_ref,
                    )
                } else {
                    result_scope
                })
            } else {
                let mut settlement_eval_cache = SettlementEvalCache::default();
                self.authorized_scope_from_graph_if_available(
                    storage_ref,
                    &mut settlement_eval_cache,
                    &graph,
                    &subscription_context,
                    &branch_schema_map,
                    session_for_policy.as_ref(),
                )
            };
            let settled_once = scope.is_some();
            let mut sent_below_required_settled = existing_subscription_state
                .as_ref()
                .filter(|(equivalent, ..)| *equivalent)
                .map(|(_, sent_below_required_settled, ..)| *sent_below_required_settled)
                .unwrap_or(false);
            let mut last_emitted_settled_tier = existing_subscription_state
                .as_ref()
                .filter(|(equivalent, ..)| *equivalent)
                .and_then(|(_, _, last_emitted_settled_tier, _, _)| *last_emitted_settled_tier);

            if let Some(scope) = scope.as_ref() {
                let scope_changed = !equivalent_existing_subscription
                    || existing_subscription_state
                        .as_ref()
                        .is_none_or(|(_, _, _, last_scope, _)| *last_scope != *scope);

                if scope_changed {
                    self.sync_manager.set_client_query_scope_with_storage(
                        storage_ref,
                        sub.client_id,
                        sub.query_id,
                        scope.clone(),
                        session_for_policy.clone(),
                    );
                }

                let settled_tier = self
                    .sync_manager
                    .max_local_durability_tier()
                    .unwrap_or(DurabilityTier::Local);
                if Self::should_emit_query_settled_to_downstream(
                    sub.required_tier,
                    settled_tier,
                    &mut sent_below_required_settled,
                    &mut last_emitted_settled_tier,
                    scope_changed,
                ) {
                    // Keep the QuerySettled marker immediately after the rows
                    // for this query's scope. Deferring all settlements until
                    // after every pending subscription lets one huge query put
                    // unrelated smaller queries' first callbacks behind its
                    // entire row replay.
                    self.sync_manager.emit_query_settled(
                        sub.client_id,
                        sub.query_id,
                        settled_tier,
                        scope,
                    );
                }
            }

            // Forward QuerySubscription to upstream servers (multi-tier forwarding)
            // This allows hub servers to know about the query and push matching data
            if sub.propagation == crate::sync_manager::QueryPropagation::Full {
                tracing::trace!(
                    %sub.client_id,
                    query_id = sub.query_id.0,
                    table = %sub.query.table,
                    "jazz trace forwarding downstream query subscription upstream"
                );
                self.sync_manager.send_query_subscription_to_servers(
                    sub.query_id,
                    sub.query.clone(),
                    session_for_policy.clone(),
                    None,
                    sub.propagation,
                    sub.policy_context_tables.clone(),
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
                    policy_context_tables: sub.policy_context_tables,
                    required_tier: sub.required_tier,
                    sent_below_required_settled,
                    last_emitted_settled_tier,
                    last_scope: scope.unwrap_or_default(),
                    needs_recompile: false,
                    settled_once,
                    propagation: sub.propagation,
                    reported_schema_warnings,
                },
            );

            if self.sync_manager.outbox().len() >= MAX_INITIAL_QUERY_REPLAY_OUTBOX_PER_PASS {
                for remaining_key in pending_keys.iter().skip(key_index + 1) {
                    if let Some(sub) = pending_by_key.remove(remaining_key) {
                        deferred.push(sub);
                    }
                }
                break;
            }
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
            let had_dirty_graph = sub.graph.has_dirty_nodes();

            if sub.settled_once && !had_dirty_graph && !sub.needs_recompile {
                let settled_tier = self
                    .sync_manager
                    .max_local_durability_tier()
                    .unwrap_or(DurabilityTier::Local);
                if Self::should_emit_query_settled_to_downstream(
                    sub.required_tier,
                    settled_tier,
                    &mut sub.sent_below_required_settled,
                    &mut sub.last_emitted_settled_tier,
                    false,
                ) {
                    tracing::trace!(
                        %client_id,
                        query_id = query_id.0,
                        tier = ?settled_tier,
                        scope_len = sub.last_scope.len(),
                        "jazz trace server subscription settled from clean cached scope"
                    );
                    self.sync_manager.emit_query_settled(
                        client_id,
                        query_id,
                        settled_tier,
                        &sub.last_scope,
                    );
                }

                self.server_subscriptions.insert((client_id, query_id), sub);
                continue;
            }

            // Row loader for this subscription
            let new_scope: Option<Cow<'_, HashSet<(ObjectId, BranchName)>>> = {
                {
                    let row_loader =
                        |id: ObjectId, table_hint: Option<TableName>| -> Option<LoadedRow> {
                            Self::load_visible_row_for_query(
                                storage,
                                id,
                                table_hint.as_ref().map(TableName::as_str),
                                branches,
                                None,
                                None,
                                false,
                                false,
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

                // Check if scope changed
                let policy_context_tables =
                    Self::merged_policy_context_tables(&sub.graph, &sub.policy_context_tables);
                if self.client_bypasses_authorization_filtering(client_id, sub.session.as_ref()) {
                    if !policy_context_tables.is_empty() {
                        let result_scope = sub.graph.sync_scope_object_ids();
                        Some(Cow::Owned(Self::scope_with_policy_context_rows_for_tables(
                            &result_scope,
                            &policy_context_tables,
                            branches,
                            storage,
                        )))
                    } else if let Some(scope) = sub.graph.sync_scope_object_ids_ref() {
                        Some(Cow::Borrowed(scope))
                    } else {
                        Some(Cow::Owned(sub.graph.sync_scope_object_ids()))
                    }
                } else {
                    let mut settlement_eval_cache = SettlementEvalCache::default();
                    self.authorized_scope_from_graph_if_available(
                        storage,
                        &mut settlement_eval_cache,
                        &sub.graph,
                        &sub.schema_context,
                        &branch_schema_map,
                        sub.session.as_ref(),
                    )
                    .map(Cow::Owned)
                }
            };
            if let Some(new_scope) = new_scope {
                let scope_changed = new_scope.as_ref() != &sub.last_scope;
                if scope_changed {
                    let owned_scope = new_scope.into_owned();
                    self.sync_manager.set_client_query_scope_with_storage(
                        storage,
                        client_id,
                        query_id,
                        owned_scope.clone(),
                        sub.session.clone(),
                    );
                    sub.last_scope = owned_scope;
                }

                // Emit an authoritative QuerySettled once the scope for this
                // settled frame has been computed. A computed empty scope is
                // authoritative; missing permissions/schema context returns None
                // and must keep the subscription unsettled.
                if !sub.settled_once {
                    sub.settled_once = true;
                    let settled_tier = self
                        .sync_manager
                        .max_local_durability_tier()
                        .unwrap_or(DurabilityTier::Local);
                    if Self::should_emit_query_settled_to_downstream(
                        sub.required_tier,
                        settled_tier,
                        &mut sub.sent_below_required_settled,
                        &mut sub.last_emitted_settled_tier,
                        true,
                    ) {
                        tracing::trace!(
                            %client_id,
                            query_id = query_id.0,
                            tier = ?settled_tier,
                            scope_len = sub.last_scope.len(),
                            "jazz trace server subscription settled"
                        );
                        self.sync_manager.emit_query_settled(
                            client_id,
                            query_id,
                            settled_tier,
                            &sub.last_scope,
                        );
                    }
                } else if scope_changed || had_dirty_graph {
                    let settled_tier = self
                        .sync_manager
                        .max_local_durability_tier()
                        .unwrap_or(DurabilityTier::Local);
                    if Self::should_emit_query_settled_to_downstream(
                        sub.required_tier,
                        settled_tier,
                        &mut sub.sent_below_required_settled,
                        &mut sub.last_emitted_settled_tier,
                        scope_changed,
                    ) {
                        tracing::trace!(
                            %client_id,
                            query_id = query_id.0,
                            tier = ?settled_tier,
                            scope_len = sub.last_scope.len(),
                            "jazz trace server subscription settled"
                        );
                        self.sync_manager.emit_query_settled(
                            client_id,
                            query_id,
                            settled_tier,
                            &sub.last_scope,
                        );
                    }
                }
            }

            self.server_subscriptions.insert((client_id, query_id), sub);
        }

        for (client_id, warning) in schema_warning_notifications {
            self.sync_manager.emit_schema_warning(client_id, warning);
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
                    self.sync_manager
                        .reject_permission_check(storage, check, reason);
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
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            }
        };

        if check.operation == Operation::Insert
            && let Some(new_content) = check.new_content.as_ref()
            && let Err(err) =
                self.validate_json_for_content(&branch_table_schema.columns, new_content)
        {
            self.sync_manager
                .reject_permission_check(storage, check, err.to_string());
            return;
        }

        let (auth_schema, auth_context) = match self.authorization_schema_for_branch(&branch_name) {
            Some(parts) => parts,
            None => {
                if !self.authorization_schema_required {
                    self.sync_manager.approve_permission_check(storage, check);
                    return;
                }
                if self.authorization_schema.is_none() {
                    let reason = format!(
                        "{:?} denied on table {} - {}",
                        check.operation,
                        table_name.0,
                        Self::missing_permissions_head_reason()
                    );
                    self.sync_manager.reject_permission_check_with_code(
                        storage,
                        check,
                        "permissions_head_missing".to_string(),
                        reason,
                    );
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
                    self.sync_manager
                        .reject_permission_check(storage, check, reason);
                } else {
                    self.sync_manager
                        .requeue_pending_permission_checks(vec![check]);
                }
                return;
            }
        };
        let Some(_) = auth_schema.get(&table_name) else {
            let reason = format!(
                "{:?} denied on table {} - table missing from current permission schema",
                check.operation, table_name.0
            );
            self.sync_manager
                .reject_permission_check(storage, check, reason);
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
                let reason = format!(
                    "{:?} denied on table {} - missing row content",
                    check.operation, table_name.0
                );
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            }
            Some(_) => {
                let reason = format!(
                    "{:?} denied on table {} - empty row content",
                    check.operation, table_name.0
                );
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
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
            self.sync_manager
                .reject_permission_check(storage, check, reason);
            return;
        };
        let source_branch_schema_map = self.branch_schema_map.clone();

        let route = self.resolve_permission_route(
            storage,
            branch_name,
            table_name,
            &check.session,
            &auth_schema,
            &auth_context,
            &source_branch_schema_map,
        );
        if route.is_denied() {
            let reason = format!(
                "{:?} denied by branch policy on table {}",
                check.operation, table_name.0
            );
            self.sync_manager
                .reject_permission_check(storage, check, reason);
            return;
        }
        if route
            .policy_for_operation(check.operation, PermissionPhase::Check)
            .is_none()
        {
            if route.allows_missing_policy(check.operation, self.row_policy_mode) {
                self.sync_manager.approve_permission_check(storage, check);
            } else if route.is_branch() {
                let reason = format!(
                    "{:?} denied by branch policy on table {}",
                    check.operation, table_name.0
                );
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
            } else {
                let reason = format!(
                    "{:?} denied on table {} - missing explicit policy",
                    check.operation, table_name.0
                );
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
            }
            return;
        }

        if self.evaluate_permission_route(
            storage,
            &route,
            PermissionEvaluationRequest {
                object_id,
                branch_name,
                table_name,
                content,
                provenance: &provenance,
                session: &check.session,
                auth_schema: &auth_schema,
                auth_context: &auth_context,
                source_branch_schema_map: &source_branch_schema_map,
                operation: check.operation,
                phase: PermissionPhase::Check,
                settlement_eval_cache: None,
            },
        ) {
            self.sync_manager.approve_permission_check(storage, check);
            return;
        }

        let reason = if route.is_branch() {
            format!(
                "{:?} denied by branch policy on table {}",
                check.operation, table_name.0
            )
        } else {
            format!(
                "{:?} denied by policy on table {}",
                check.operation, table_name.0
            )
        };
        self.sync_manager
            .reject_permission_check(storage, check, reason);
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
        mut check: PendingPermissionCheck,
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
                .reject_permission_check(storage, check, err.to_string());
            return;
        }

        if check
            .old_content
            .as_ref()
            .is_none_or(|content| content.is_empty())
            && let Ok(Some(previous_row)) = storage.load_visible_region_row(
                table_name.as_str(),
                branch_name.as_str(),
                object_id,
            )
        {
            check.old_content = Some(previous_row.data.to_vec());
        }

        if auth_schema.get(&table_name).is_none() {
            self.sync_manager.reject_permission_check(
                storage,
                check,
                format!(
                    "Update denied on table {} - table missing from current permission schema",
                    table_name.0
                ),
            );
            return;
        };
        let source_branch_schema_map = self.branch_schema_map.clone();
        let old_provenance = self.current_row_provenance(storage, object_id, branch_name);
        let new_provenance = Self::payload_row_provenance(&check.payload);

        let route = self.resolve_permission_route(
            storage,
            branch_name,
            table_name,
            &check.session,
            auth_schema,
            auth_context,
            &source_branch_schema_map,
        );
        if route.is_denied() {
            self.sync_manager.reject_permission_check(
                storage,
                check,
                format!("Update denied by branch policy on table {}", table_name.0),
            );
            return;
        }

        let has_using_policy = route
            .policy_for_operation(Operation::Update, PermissionPhase::Using)
            .is_some();
        let has_check_policy = route
            .policy_for_operation(Operation::Update, PermissionPhase::Check)
            .is_some();
        if !has_using_policy && !has_check_policy {
            if route.allows_missing_policy(Operation::Update, self.row_policy_mode) {
                self.sync_manager.approve_permission_check(storage, check);
            } else if route.is_branch() {
                self.sync_manager.reject_permission_check(
                    storage,
                    check,
                    format!("Update denied by branch policy on table {}", table_name.0),
                );
            } else {
                self.sync_manager.reject_permission_check(
                    storage,
                    check,
                    format!(
                        "Update denied on table {} - missing explicit update policy",
                        table_name.0
                    ),
                );
            }
            return;
        }

        if has_using_policy {
            let old_content = match check.old_content.as_ref() {
                Some(c) if !c.is_empty() => c,
                _ => {
                    let reason = if route.is_branch() {
                        format!(
                            "Update denied by branch policy on table {} - no old content",
                            table_name.0
                        )
                    } else {
                        format!(
                            "Update denied by USING policy on table {} - no old content",
                            table_name.0
                        )
                    };
                    self.sync_manager
                        .reject_permission_check(storage, check, reason);
                    return;
                }
            };
            let Some(old_provenance) = old_provenance.as_ref() else {
                let reason = if route.is_branch() {
                    format!(
                        "Update denied by branch policy on table {} - missing old provenance",
                        table_name.0
                    )
                } else {
                    format!(
                        "Update denied by USING policy on table {} - missing old provenance",
                        table_name.0
                    )
                };
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            };

            if !self.evaluate_permission_route(
                storage,
                &route,
                PermissionEvaluationRequest {
                    object_id,
                    branch_name,
                    table_name,
                    content: old_content,
                    provenance: old_provenance,
                    session: &check.session,
                    auth_schema,
                    auth_context,
                    source_branch_schema_map: &source_branch_schema_map,
                    operation: Operation::Update,
                    phase: PermissionPhase::Using,
                    settlement_eval_cache: None,
                },
            ) {
                let reason = if route.is_branch() {
                    format!(
                        "Update denied by branch USING policy on table {}",
                        table_name.0
                    )
                } else {
                    format!(
                        "Update denied by USING policy on table {} - cannot see old row",
                        table_name.0
                    )
                };
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            }
        }

        if has_check_policy {
            let new_content = match check.new_content.as_ref() {
                Some(c) => c,
                None => {
                    let reason = if route.is_branch() {
                        format!(
                            "Update denied by branch policy on table {} - missing new content",
                            table_name.0
                        )
                    } else {
                        format!(
                            "Update denied by WITH CHECK policy on table {} - missing new content",
                            table_name.0
                        )
                    };
                    self.sync_manager
                        .reject_permission_check(storage, check, reason);
                    return;
                }
            };
            let Some(new_provenance) = new_provenance.as_ref() else {
                let reason = if route.is_branch() {
                    format!(
                        "Update denied by branch policy on table {} - missing new provenance",
                        table_name.0
                    )
                } else {
                    format!(
                        "Update denied by WITH CHECK policy on table {} - missing new provenance",
                        table_name.0
                    )
                };
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            };

            if !self.evaluate_permission_route(
                storage,
                &route,
                PermissionEvaluationRequest {
                    object_id,
                    branch_name,
                    table_name,
                    content: new_content,
                    provenance: new_provenance,
                    session: &check.session,
                    auth_schema,
                    auth_context,
                    source_branch_schema_map: &source_branch_schema_map,
                    operation: Operation::Update,
                    phase: PermissionPhase::Check,
                    settlement_eval_cache: None,
                },
            ) {
                let reason = if route.is_branch() {
                    format!(
                        "Update denied by branch WITH CHECK policy on table {}",
                        table_name.0
                    )
                } else {
                    format!(
                        "Update denied by WITH CHECK policy on table {}",
                        table_name.0
                    )
                };
                self.sync_manager
                    .reject_permission_check(storage, check, reason);
                return;
            }
        }

        self.sync_manager.approve_permission_check(storage, check);
    }

    /// Create policy graphs for complex clauses (INHERITS/EXISTS).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_policy_graphs_for_complex_clauses(
        &self,
        clauses: &[ComplexClause],
        content: &[u8],
        descriptor: &RowDescriptor,
        table: &TableName,
        operation: Operation,
        session: &Session,
        branch: &str,
    ) -> Option<Vec<PolicyGraph>> {
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
                    let parent_schema = self.schema.get(&parent_table)?;

                    let parent_policy = parent_schema
                        .policies
                        .policy_for_operation(*operation, PermissionPhase::Using);
                    let Some(parent_policy) = parent_policy else {
                        if self.row_policy_mode.denies_missing_explicit_policy() {
                            return None;
                        }
                        continue;
                    };

                    // Create policy graph for INHERITS
                    if let Some(graph) = PolicyGraph::for_inherits(
                        &parent_table,
                        parent_id,
                        parent_policy,
                        session,
                        &self.schema,
                        PolicyGraphBuildOptions::new(branch, self.row_policy_mode)
                            .with_initial_depth(1),
                    ) {
                        graphs.push(graph);
                    } else {
                        return None;
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
                        operation,
                        self.row_policy_mode,
                    ) {
                        graphs.push(graph);
                    } else {
                        return None;
                    }
                }
                ComplexClause::ExistsRel { rel } => {
                    if let Some(graph) = PolicyGraph::for_exists_rel(
                        rel,
                        &self.schema,
                        branch,
                        Some(session.clone()),
                        self.row_policy_mode,
                        Some(table),
                        false,
                    ) {
                        graphs.push(graph);
                    } else {
                        return None;
                    }
                }
                ComplexClause::InheritsReferencing { .. } => {
                    // Evaluated directly in write permission checks (needs target row context).
                }
            }
        }

        Some(graphs)
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
            let branch_schema_map = Self::branch_schema_map_for_context(&self.schema_context);
            let mut row_loader =
                |id: ObjectId, table_hint: Option<TableName>| -> Option<LoadedRow> {
                    let (_, row) = Self::load_best_visible_row_batch_with_hint_or_locator(
                        storage,
                        id,
                        table_hint.as_ref().map(TableName::as_str),
                        &branches,
                        None,
                        &self.schema_context,
                        &branch_schema_map,
                    )?;
                    if row.is_hard_deleted() {
                        return None;
                    }
                    let batch_id = row.batch_id;
                    let provenance = row.row_provenance();
                    let source_branch = BranchName::new(&row.branch);
                    Some(LoadedRow::new(
                        row.data,
                        provenance,
                        [(id, source_branch)].into_iter().collect(),
                        batch_id,
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
                    .reject_permission_check(storage, state.pending_check, reason);
            }
        }
    }
}
