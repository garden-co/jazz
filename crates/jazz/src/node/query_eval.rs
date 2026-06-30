//! Query execution, shape registration, binding routing, and read-set
//! evaluation for `jazz/SPEC/6_queries.md`. This module owns lowering validated Jazz
//! queries to groove plans, evaluating one-shot reads, recording predicate reads,
//! and applying binding deltas; the pure AST lives in [`crate::query`], policy
//! checks in [`super::policy`], and sync view payload assembly in [`super::views`].
//! It is the node layer's query bridge to groove IVM.

use super::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use groove::ivm::{Subscription, TopByOrder};
use groove::records::{RecordDescriptor, ValueType};
use groove::schema::ColumnType;

use super::maintained_subscription_view::MaintainedSubscriptionView;
use super::policy::ViewEvaluationContext;
use crate::protocol::{ResultRowEntry, ShapeAst, Subscribe, SubscriptionKey};
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, PolicyBranch, Predicate,
    QUERY_NAMESPACE, Query as JazzQuery, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationPredicate, RelationProjectExpr, RelationQuery,
    RelationRowIdRef, RelationValueRef, ShapeId, ValidatedQuery, col, eq, in_list, lit,
};

const CLAIM_PARAM_PREFIX: &str = "__jazz_claim_";

#[allow(dead_code)]
pub(crate) struct ReachableGraphs {
    pub(crate) closure: GraphBuilder,
    pub(crate) edge_current: GraphBuilder,
    pub(crate) access_current: GraphBuilder,
    pub(crate) seed_param: String,
}

pub(crate) struct LocalMaintainedViewSubscription {
    subscription: Subscription,
    maintained: MaintainedSubscriptionView,
    tables: BTreeMap<String, TableSchema>,
    result_table: String,
    result_set: BTreeSet<ResultRowEntry>,
    identity: AuthorId,
}

pub(crate) struct LocalMaintainedViewSubscriptionUpdate {
    pub(crate) adds: Vec<CurrentRow>,
    pub(crate) removes: Vec<ResultRowEntry>,
}

#[derive(Clone, Debug)]
struct LoweredQueryCore {
    source: LoweredQuerySource,
    graph: GraphBuilder,
    param_names: Vec<String>,
    param_types: Vec<groove::schema::ColumnType>,
}

#[derive(Clone)]
struct LoweredQueryClauseOptions {
    tier: DurabilityTier,
    output_fields: Vec<String>,
    keep_binding_params_in_output: bool,
    binding_source_shape: String,
    source_overrides: BTreeMap<String, GraphBuilder>,
    table_overrides: BTreeMap<String, TableSchema>,
}

#[derive(Clone, Debug)]
struct RelationEvalScopedRow {
    table: String,
    row: CurrentRow,
}

#[derive(Clone, Debug)]
struct RelationEvalRow {
    current: CurrentRow,
    scopes: BTreeMap<String, RelationEvalScopedRow>,
}

impl RelationEvalRow {
    fn from_row(scope: String, row: CurrentRow) -> Self {
        let mut scopes = BTreeMap::new();
        scopes.insert(
            scope.clone(),
            RelationEvalScopedRow {
                table: scope,
                row: row.clone(),
            },
        );
        Self {
            current: row,
            scopes,
        }
    }

    fn scoped_row(&self, scope: Option<&str>) -> Option<&RelationEvalScopedRow> {
        match scope {
            Some(scope) => self.scopes.get(scope),
            None => self.scopes.get(self.current.table()),
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut scopes = self.scopes.clone();
        scopes.extend(other.scopes.clone());
        Self {
            current: other.current.clone(),
            scopes,
        }
    }

    fn with_aliases(mut self, aliases: impl IntoIterator<Item = String>) -> Self {
        for alias in aliases {
            self.scopes.insert(
                alias,
                RelationEvalScopedRow {
                    table: self.current.table().to_owned(),
                    row: self.current.clone(),
                },
            );
        }
        self
    }

    fn retarget_to_scope(mut self, scope: Option<&str>) -> Option<Self> {
        let scoped = self.scoped_row(scope)?.clone();
        self.current = scoped.row.clone();
        self.scopes.insert(
            scoped.table.clone(),
            RelationEvalScopedRow {
                table: scoped.table,
                row: scoped.row,
            },
        );
        Some(self)
    }
}

#[derive(Default)]
struct RelationEvalContext {
    frontier: Option<CurrentRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LoweredQuerySource {
    VisibleCurrent { table: String, tier: DurabilityTier },
    IncludeDeletedCurrent { table: String, tier: DurabilityTier },
    HistoricalCurrent { table: String, position: GlobalSeq },
    InlineCurrent { table: String },
}

impl LoweredQuerySource {
    fn is_visible_current_for(&self, table: &str, tier: DurabilityTier) -> bool {
        matches!(
            self,
            Self::VisibleCurrent {
                table: source_table,
                tier: source_tier,
            } if source_table == table && *source_tier == tier
        )
    }

    fn is_historical_current_for(&self, table: &str, position: GlobalSeq) -> bool {
        matches!(
            self,
            Self::HistoricalCurrent {
                table: source_table,
                position: source_position,
            } if source_table == table && *source_position == position
        )
    }

    fn is_include_deleted_current_for(&self, table: &str, tier: DurabilityTier) -> bool {
        matches!(
            self,
            Self::IncludeDeletedCurrent {
                table: source_table,
                tier: source_tier,
            } if source_table == table && *source_tier == tier
        )
    }

    fn is_inline_current_for(&self, table: &str) -> bool {
        matches!(
            self,
            Self::InlineCurrent {
                table: source_table,
            } if source_table == table
        )
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn resolve_time_travel_position(
        &mut self,
        time: TxTime,
    ) -> Result<GlobalSeq, Error> {
        let raws = if time.0 == u64::MAX {
            self.database
                .primary_key_scan_raw("jazz_transactions", &[])?
        } else {
            self.database.primary_key_scan_range_raw(
                "jazz_transactions",
                &[Value::U64(0), Value::U64(0)],
                &[Value::U64(time.0 + 1), Value::U64(0)],
            )?
        };
        let mut position = GlobalSeq(0);
        for raw in raws {
            let record = raw.record();
            let Some(global_seq) = record
                .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_SEQ_IDX)?
                .map(GlobalSeq)
            else {
                continue;
            };
            position = position.max(global_seq);
        }
        Ok(position)
    }

    /// Resolve a registered shape id back to its validated query, if known.
    ///
    /// Used by the `Db` sync surface to reconstruct `(shape, binding)` from the
    /// `RegisterShape` / `Subscribe` a subscriber sent over a connection.
    pub(crate) fn registered_shape(&self, shape_id: ShapeId) -> Option<ValidatedQuery> {
        self.query.registered_shapes.get(&shape_id).cloned()
    }

    pub(super) fn register_shape(&mut self, shape_id: ShapeId, ast: ShapeAst) -> Result<(), Error> {
        if ast.version != ShapeAst::VERSION {
            return Err(Error::InvalidStoredValue("unsupported query AST version"));
        }
        let Some(schema) = self.catalogue.catalogue_schemas.get(&ast.schema_version) else {
            self.sync_metrics.parked_catalogue_shapes += 1;
            self.parking
                .parked_shape_registrations
                .insert(shape_id, ast);
            return Ok(());
        };
        let shape = ast.query.validate(&schema.schema)?;
        if shape.shape_id() != shape_id {
            return Err(Error::InvalidStoredValue("shape id does not match AST"));
        }
        self.prepared_query_plan(&shape, DurabilityTier::Local)?;
        self.prepared_query_plan(&shape, DurabilityTier::Global)?;
        self.query.registered_shapes.insert(shape_id, shape);
        self.drain_parked_binding_deltas_for_shape(shape_id)?;
        Ok(())
    }

    pub(super) fn drain_parked_shape_registrations(&mut self) -> Result<(), Error> {
        let ready = self
            .parking
            .parked_shape_registrations
            .iter()
            .filter_map(|(shape_id, ast)| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&ast.schema_version)
                    .then_some((*shape_id, ast.clone()))
            })
            .collect::<Vec<_>>();
        for (shape_id, ast) in ready {
            self.parking.parked_shape_registrations.remove(&shape_id);
            self.sync_metrics.parked_catalogue_shapes_resolved += 1;
            self.register_shape(shape_id, ast)?;
        }
        Ok(())
    }

    pub(super) fn apply_subscribe(&mut self, subscribe: Subscribe) -> Result<(), Error> {
        let Some(shape) = self
            .query
            .registered_shapes
            .get(&subscribe.shape_id)
            .cloned()
        else {
            self.parking
                .parked_binding_deltas
                .entry(subscribe.shape_id)
                .or_default()
                .push(subscribe);
            return Ok(());
        };
        self.apply_known_shape_subscribe(&shape, subscribe)
    }

    fn drain_parked_binding_deltas_for_shape(&mut self, shape_id: ShapeId) -> Result<(), Error> {
        let Some(deltas) = self.parking.parked_binding_deltas.remove(&shape_id) else {
            return Ok(());
        };
        let Some(shape) = self.query.registered_shapes.get(&shape_id).cloned() else {
            self.parking.parked_binding_deltas.insert(shape_id, deltas);
            return Ok(());
        };
        for subscribe in deltas {
            self.apply_known_shape_subscribe(&shape, subscribe)?;
        }
        Ok(())
    }

    fn apply_known_shape_subscribe(
        &mut self,
        shape: &ValidatedQuery,
        subscribe: Subscribe,
    ) -> Result<(), Error> {
        if subscribe.values.len() != shape.params().len() {
            return Err(Error::InvalidStoredValue("binding arity mismatch"));
        }
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(subscribe.values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let _binding = shape.bind(value_map)?;
        self.query
            .registered_bindings
            .entry(subscribe.shape_id)
            .or_default()
            .insert(subscribe.subscription.binding_id, subscribe.values);
        Ok(())
    }

    pub(crate) fn apply_unsubscribe(&mut self, subscription: SubscriptionKey) {
        if let Some(bindings) = self
            .query
            .registered_bindings
            .get_mut(&subscription.shape_id)
        {
            bindings.remove(&subscription.binding_id);
        }
        self.query.settled_result_sets.remove(&subscription);
    }

    pub(crate) fn has_settled_result_set(&self, subscription: SubscriptionKey) -> bool {
        self.query.settled_result_sets.contains_key(&subscription)
    }

    /// Evaluate a validated query shape against this node's local knowledge.
    ///
    /// Phase B step 2 returns output-relation rows only. Provenance-closure
    /// shipping and settled result set reads are introduced by the wire step.
    pub fn query_rows(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan(shape, binding, tier, None)
    }

    pub(crate) fn query_rows_with_prepared_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlan>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            AuthorId::SYSTEM,
        )
    }

    #[cfg(test)]
    pub(crate) fn clear_prepared_query_plan_cache_for_test(&mut self) {
        self.query.query_shape_cache.clear();
    }

    #[cfg(test)]
    pub(crate) fn prepared_query_plan_cache_is_empty_for_test(&self) -> bool {
        self.query.query_shape_cache.is_empty()
    }

    pub(crate) fn query_rows_with_prepared_plan_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlan>,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_options_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            identity,
            false,
        )
    }

    pub(crate) fn query_rows_prefer_settled_result_set(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        prepared_plan: Option<&PreparedQueryPlan>,
    ) -> Result<Vec<CurrentRow>, Error> {
        if !self.uses_partitioned_or_schema_projected_read(shape) {
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            };
            if self.query.settled_result_sets.contains_key(&subscription) {
                return self.query_rows_from_result_set(shape, subscription);
            }
        }

        self.query_rows_with_prepared_plan(shape, binding, DurabilityTier::Local, prepared_plan)
    }

    pub(crate) fn query_rows_including_deleted_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlan>,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_options_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            identity,
            true,
        )
    }

    fn query_rows_with_options_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlan>,
        identity: AuthorId,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        if !include_deleted && !shape.query().policy_branches.is_empty() {
            return self.query_rows_with_policy_branches(shape, binding, tier, identity);
        }
        if include_deleted {
            let mut rows =
                self.query_rows_including_deleted_with_lowered_clauses(shape, binding, tier)?;
            let query = shape.query();
            self.apply_include_modes(query, shape.schema_version(), &mut rows, tier, identity)?;
            self.finish_query_rows(query, &mut rows)?;
            return Ok(rows);
        }
        if matches!(tier, DurabilityTier::Edge | DurabilityTier::Global)
            && shape.query().reachable.is_empty()
            && !self.uses_partitioned_or_schema_projected_read(shape)
        {
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            };
            if self.query.settled_result_sets.contains_key(&subscription) {
                return self.query_rows_from_result_set(shape, subscription);
            }
        }
        if tier != DurabilityTier::Global || self.uses_partitioned_or_schema_projected_read(shape) {
            return self.query_rows_from_projected_current_source(shape, binding, tier, identity);
        }
        let lowered_include_modes =
            prepared_plan.is_none() && self.should_lower_current_include_modes(shape, tier);
        let plan = if lowered_include_modes {
            self.current_query_plan_with_lowered_include_modes(shape, tier, identity)?
        } else {
            match prepared_plan {
                Some(plan) => plan.clone(),
                None => self.prepared_query_plan(shape, tier)?,
            }
        };
        let table_schema = self.table(&shape.query().table)?.clone();
        let deltas_result = match plan {
            PreparedQueryPlan::Graph(graph) => {
                self.database.query_graph(graph).map_err(Error::Groove)
            }
            PreparedQueryPlan::Prepared {
                shape,
                param_names,
                param_types,
            } => {
                let values = binding_values_for_plan(binding, &param_names, &param_types)?;
                self.database
                    .bind_shape(shape, &values)
                    .map_err(Error::Groove)
                    .and_then(|subscription| {
                        subscription.recv().map_err(|_| Error::SubscriptionClosed)
                    })
            }
        };
        let deltas = deltas_result?;
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let row = decode_current_row(&table_schema, record)?;
                rows.push(self.materialize_current_row(&table_schema, row)?);
            }
        }
        if !lowered_include_modes {
            self.apply_include_modes(
                shape.query(),
                shape.schema_version(),
                &mut rows,
                tier,
                identity,
            )?;
        }
        self.finish_query_rows(shape.query(), &mut rows)?;
        Ok(rows)
    }

    fn query_rows_with_policy_branches(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut base_query = shape.query().clone();
        let branches = std::mem::take(&mut base_query.policy_branches);
        let mut rows_by_id = BTreeMap::new();
        let base_shape = base_query.validate(&self.catalogue.schema)?;
        let base_binding = binding_for_shape(&base_shape, binding)?;
        let base_rows = self.query_rows_with_options_for_identity(
            &base_shape,
            &base_binding,
            tier,
            None,
            identity,
            false,
        )?;
        for row in base_rows {
            rows_by_id.insert(row.row_uuid(), row);
        }
        for branch in branches {
            let branch_shape = branch
                .as_query(&shape.query().table)
                .validate(&self.catalogue.schema)?;
            let branch_binding = binding_for_shape(&branch_shape, binding)?;
            let branch_rows = self.query_rows_with_options_for_identity(
                &branch_shape,
                &branch_binding,
                tier,
                None,
                identity,
                false,
            )?;
            for row in branch_rows {
                rows_by_id.insert(row.row_uuid(), row);
            }
        }
        let mut rows = rows_by_id.into_values().collect::<Vec<_>>();
        self.finish_query_rows(shape.query(), &mut rows)?;
        Ok(rows)
    }

    /// Evaluate a validated query against the globally settled state at
    /// `position`.
    ///
    /// This is a settled-history read: it considers only transactions with
    /// `global_seq <= position`, chooses the ordinary per-row winners from
    /// that subset, and evaluates the query against that historical state.
    pub fn query_rows_at(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = self.query_rows_at_with_lowered_clauses(shape, binding, position)?;
        self.finish_query_rows(shape.query(), &mut rows)?;
        Ok(rows)
    }

    fn query_rows_at_with_lowered_clauses(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let query = lowered_shape.query();
        let table = self
            .table_in_schema(&query.table, lowered_shape.schema_version())?
            .clone();
        let LoweredQueryCore {
            source,
            graph,
            param_names,
            param_types,
        } = self.lower_historical_query_core(&lowered_shape, position)?;
        debug_assert!(source.is_historical_current_for(&query.table, position));
        if !param_names.is_empty() || !param_types.is_empty() {
            return Err(Error::InvalidStoredValue(
                "historical snapshot lowering must bind all params",
            ));
        }
        let deltas = self.database.query_graph(graph).map_err(Error::Groove)?;
        self.materialize_historical_query_rows(table, deltas)
    }

    fn query_rows_including_deleted_with_lowered_clauses(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let query = lowered_shape.query();
        let table = self
            .table_in_schema(&query.table, lowered_shape.schema_version())?
            .clone();
        let LoweredQueryCore {
            source,
            graph,
            param_names,
            param_types,
        } = self.lower_include_deleted_query_core(&lowered_shape, tier)?;
        debug_assert!(source.is_include_deleted_current_for(&query.table, tier));
        if !param_names.is_empty() || !param_types.is_empty() {
            return Err(Error::InvalidStoredValue(
                "include-deleted snapshot lowering must bind all params",
            ));
        }
        let deltas = self.database.query_graph(graph).map_err(Error::Groove)?;
        self.materialize_include_deleted_query_rows(table, deltas)
    }

    fn materialize_historical_query_rows(
        &mut self,
        table: TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let row = decode_current_row(&table, record)?;
                rows.push(self.materialize_current_row(&table, row)?);
            }
        }
        Ok(rows)
    }

    fn materialize_include_deleted_query_rows(
        &mut self,
        table: TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let deleted_field_idx = current_row_fields(&table).len();
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let deleted = record.get_bool(deleted_field_idx)?;
                let row = decode_current_row(&table, record)?;
                let row = self.materialize_current_row(&table, row)?;
                rows.push(if deleted { row.into_deleted() } else { row });
            }
        }
        Ok(rows)
    }

    fn materialize_inline_current_query_rows(
        &mut self,
        table: &TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let row = decode_current_row(table, record)?;
                rows.push(self.materialize_current_row(table, row)?);
            }
        }
        Ok(rows)
    }

    pub(super) fn current_rows_at(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let mut rows = Vec::new();
        for row_uuid in self.global_row_ids_at(table, position)? {
            if self
                .global_layer_winner_at(table, row_uuid, VersionLayer::Deletion, position)?
                .is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let Some(content) =
                self.global_layer_winner_at(table, row_uuid, VersionLayer::Content, position)?
            else {
                continue;
            };
            rows.push(self.current_row_from_materialized_version(&table_schema, &content)?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn global_row_ids_at(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        let raws = if position.0 == u64::MAX {
            self.database
                .index_scan_raw("jazz_global_changes", "by_global_seq", &[])?
        } else {
            self.database.index_scan_range_raw(
                "jazz_global_changes",
                "by_global_seq",
                &[Value::U64(0)],
                &[Value::U64(position.0 + 1)],
            )?
        };
        let mut row_ids = BTreeSet::new();
        for raw in raws {
            let record = raw.record();
            if record.get_bytes(GlobalChangeRowRecord::FIELD_TABLE_NAME_IDX)? == table.as_bytes() {
                row_ids.insert(RowUuid(
                    record.get_uuid(GlobalChangeRowRecord::FIELD_ROW_UUID_IDX)?,
                ));
            }
        }
        Ok(row_ids)
    }

    pub(crate) fn subscribe_query_binding_with_plan(
        &mut self,
        binding: &Binding,
        plan: &PreparedQueryPlan,
    ) -> Result<Option<Subscription>, Error> {
        let subscription = match plan.clone() {
            PreparedQueryPlan::Graph(graph) => {
                self.database.subscribe(graph).map_err(Error::Groove)?
            }
            PreparedQueryPlan::Prepared {
                shape,
                param_names,
                param_types,
            } => {
                let values = binding_values_for_plan(binding, &param_names, &param_types)?;
                self.database
                    .bind_shape(shape, &values)
                    .map_err(Error::Groove)?
            }
        };
        let _initial = subscription.recv().map_err(|_| Error::SubscriptionClosed)?;
        Ok(Some(subscription))
    }

    pub(crate) fn open_local_maintained_view_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<(LocalMaintainedViewSubscription, Vec<CurrentRow>), Error> {
        let (subscription, maintained, transitions, tables) =
            self.maintained_subscription_view_from_cold_snapshot(shape, binding, identity)?;
        let mut local = LocalMaintainedViewSubscription {
            subscription,
            maintained,
            tables,
            result_table: shape.query().table.clone(),
            result_set: BTreeSet::new(),
            identity,
        };
        let initial = self.apply_local_maintained_view_transitions(&mut local, transitions)?;
        Ok((local, initial.adds))
    }

    pub(crate) fn drain_local_maintained_view_subscription(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
    ) -> Result<Option<LocalMaintainedViewSubscriptionUpdate>, Error> {
        self.database.flush().map_err(Error::Groove)?;
        let mut states = BTreeMap::<ResultRowEntry, (bool, bool)>::new();
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = local.maintained.apply_tagged_deltas(
                        &deltas,
                        &local.tables,
                        &self.node_aliases,
                    )?;
                    for entry in transitions.adds {
                        let before = local.result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = true)
                            .or_insert((before, true));
                    }
                    for entry in transitions.removes {
                        let before = local.result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = false)
                            .or_insert((before, false));
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Error::SubscriptionClosed);
                }
            }
        }
        if states.is_empty() {
            return Ok(None);
        }
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        for (entry, (before, after)) in states {
            match (before, after) {
                (false, true) => transitions.adds.push(entry),
                (true, false) => transitions.removes.push(entry),
                _ => {}
            }
        }
        let update = self.apply_local_maintained_view_transitions(local, transitions)?;
        if update.adds.is_empty() && update.removes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(update))
        }
    }

    fn apply_local_maintained_view_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        let mut adds = Vec::new();
        for entry in transitions.adds {
            if entry.0.as_str() != local.result_table {
                continue;
            }
            if local.result_set.insert(entry)
                && let Some(row) =
                    self.materialize_local_maintained_view_result_entry(local, entry)?
            {
                adds.push(row);
            }
        }
        let mut removes = Vec::new();
        for entry in transitions.removes {
            if entry.0.as_str() != local.result_table {
                continue;
            }
            if local.result_set.remove(&entry) {
                removes.push(entry);
            }
        }
        Ok(LocalMaintainedViewSubscriptionUpdate { adds, removes })
    }

    fn materialize_local_maintained_view_result_entry(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        entry: ResultRowEntry,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(entry.0.as_str())?.clone();
        let mut tx_versions = local.maintained.versions_by_tx(entry.2);
        let version = if let Some(version) =
            local_maintained_view_content_witness(&tx_versions, entry.0.as_str(), entry.1)
        {
            version.clone()
        } else {
            let (content_winner, _) = local.maintained.replacement_for(entry.0.as_str(), entry.1);
            let Some(content_winner) = content_winner else {
                return Ok(None);
            };
            if self.version_tx_id(&content_winner)? != entry.2 {
                return Ok(None);
            }
            tx_versions.push(content_winner);
            tx_versions
                .last()
                .ok_or(Error::MissingTransaction(entry.2))?
                .clone()
        };
        let mut context = ViewEvaluationContext::default();
        if !self.read_policy_allows_version_memo(&table, &version, local.identity, &mut context)? {
            return Ok(None);
        }
        self.current_row_from_materialized_version(&table, &version)
            .map(Some)
    }

    pub(crate) fn prepare_query_binding_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlan), Error> {
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        let shape = maintained_view_bind_filter_literals_with_mode(
            &shape,
            &binding,
            &self.catalogue.schema,
            ParamBindingMode::RetainAllParams,
        )?;
        let mut values = binding.values().clone();
        insert_claim_bindings(
            &mut values,
            shape.params(),
            identity,
            self.session_claims.get(&identity),
        );
        let binding = shape.bind(values)?;
        let plan = self.prepared_query_plan(&shape, tier)?;
        Ok((shape, binding, plan))
    }

    pub(crate) fn query_rows_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        self.query_rows_with_prepared_plan_for_identity(&shape, &binding, tier, None, identity)
    }

    /// Evaluate a query plus its array-subquery relation payload against local
    /// visible-current knowledge for one identity.
    pub(crate) fn query_relation_snapshot_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        let mut snapshot = RelationSnapshot::default();
        let mut relation_source_rows =
            self.query_relation_source_rows_before_pagination(shape, binding, tier, identity)?;
        self.retain_rows_satisfying_array_subquery_requirements(
            &mut relation_source_rows,
            &shape.query().array_subqueries,
            shape.schema_version(),
            tier,
            identity,
        )?;
        let mut relation_source_query = shape.query().clone();
        relation_source_query.select = None;
        self.finish_query_rows(&relation_source_query, &mut relation_source_rows)?;
        let root_rows = if shape.query().select.is_some() {
            let mut rows = relation_source_rows.clone();
            self.apply_projection(shape.query(), &mut rows)?;
            rows
        } else {
            relation_source_rows.clone()
        };
        let mut row_keys = BTreeSet::new();
        for row in &root_rows {
            row_keys.insert((row.table().to_owned(), row.row_uuid()));
        }
        snapshot.root_count = root_rows.len();
        snapshot.rows.extend(root_rows.iter().cloned());
        let root_table = self
            .table_in_schema(&shape.query().table, shape.schema_version())?
            .clone();
        self.materialize_array_subqueries(
            &root_table,
            &relation_source_rows,
            &shape.query().array_subqueries,
            shape.schema_version(),
            tier,
            identity,
            &mut snapshot,
            &mut row_keys,
        )?;
        Ok(snapshot)
    }

    fn query_relation_source_rows_before_pagination(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut query = shape.query().clone();
        query.select = None;
        query.limit = None;
        query.offset = 0;
        let source_shape = query
            .validate(&self.catalogue.schema)
            .map_err(Error::Query)?;
        let source_binding = source_shape
            .bind(binding.values().clone())
            .map_err(Error::Query)?;
        self.query_rows_for_link(&source_shape, &source_binding, tier, identity)
    }

    pub(crate) fn query_relation_query_snapshot_for_link(
        &mut self,
        query: &RelationQuery,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        let mut context = RelationEvalContext { frontier: None };
        let mut rows = self.eval_relation_expr(&query.rel, tier, identity, &mut context)?;
        rows.sort_by(|left, right| {
            subscription_row_key_for_eval(left).cmp(&subscription_row_key_for_eval(right))
        });
        rows.dedup_by(|left, right| {
            subscription_row_key_for_eval(left) == subscription_row_key_for_eval(right)
        });
        let rows = rows.into_iter().map(|row| row.current).collect::<Vec<_>>();
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    fn eval_relation_expr(
        &mut self,
        expr: &RelationExpr,
        tier: DurabilityTier,
        identity: AuthorId,
        context: &mut RelationEvalContext,
    ) -> Result<Vec<RelationEvalRow>, Error> {
        match expr {
            RelationExpr::TableScan { table } => {
                let shape = JazzQuery::from(table.as_str()).validate(&self.catalogue.schema)?;
                let binding = shape.bind(BTreeMap::new())?;
                let rows = self.query_rows_for_link(&shape, &binding, tier, identity)?;
                Ok(rows
                    .into_iter()
                    .map(|row| RelationEvalRow::from_row(table.clone(), row))
                    .collect())
            }
            RelationExpr::Filter { input, predicate } => {
                let rows = self.eval_relation_expr(input, tier, identity, context)?;
                rows.into_iter()
                    .filter_map(|row| {
                        match self.eval_relation_predicate(predicate, &row, context) {
                            Ok(true) => Some(Ok(row)),
                            Ok(false) => None,
                            Err(err) => Some(Err(err)),
                        }
                    })
                    .collect()
            }
            RelationExpr::Union { inputs } => {
                let mut out = Vec::new();
                for input in inputs {
                    out.extend(self.eval_relation_expr(input, tier, identity, context)?);
                }
                Ok(out)
            }
            RelationExpr::Join {
                left,
                right,
                on,
                join_kind,
            } => {
                if *join_kind != RelationJoinKind::Inner {
                    return Err(Error::InvalidStoredValue(
                        "left relation joins are not supported yet",
                    ));
                }
                let left_rows = self.eval_relation_expr(left, tier, identity, context)?;
                let right_rows = self.eval_relation_expr(right, tier, identity, context)?;
                let left_aliases = relation_join_left_aliases(on);
                let right_aliases = relation_join_right_aliases(on);
                let mut out = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let left_row = left_row.clone().with_aliases(left_aliases.clone());
                        let right_row = right_row.clone().with_aliases(right_aliases.clone());
                        if self.relation_join_matches(&left_row, &right_row, on, context)? {
                            out.push(left_row.merge(&right_row));
                        }
                    }
                }
                Ok(out)
            }
            RelationExpr::Project { input, columns } => {
                let rows = self.eval_relation_expr(input, tier, identity, context)?;
                let mut out = Vec::new();
                for row in rows {
                    let Some(projected) = self.project_relation_row(row, columns, context)? else {
                        continue;
                    };
                    out.push(projected);
                }
                Ok(out)
            }
            RelationExpr::Gather {
                seed,
                step,
                max_depth,
                ..
            } => {
                let seed_rows = self.eval_relation_expr(seed, tier, identity, context)?;
                let mut by_key = BTreeMap::new();
                let mut frontier = seed_rows.clone();
                for row in seed_rows {
                    by_key.insert(subscription_row_key_for_eval(&row), row);
                }
                for _ in 0..*max_depth {
                    if frontier.is_empty() {
                        break;
                    }
                    let mut next_frontier = Vec::new();
                    for frontier_row in frontier {
                        context.frontier = Some(frontier_row.current.clone());
                        let step_rows = self.eval_relation_expr(step, tier, identity, context)?;
                        context.frontier = None;
                        for step_row in step_rows {
                            let key = subscription_row_key_for_eval(&step_row);
                            if let std::collections::btree_map::Entry::Vacant(entry) =
                                by_key.entry(key)
                            {
                                entry.insert(step_row.clone());
                                next_frontier.push(step_row);
                            }
                        }
                    }
                    frontier = next_frontier;
                }
                Ok(by_key.into_values().collect())
            }
            RelationExpr::Distinct { input, .. } => {
                let mut rows = self.eval_relation_expr(input, tier, identity, context)?;
                rows.sort_by(|left, right| {
                    subscription_row_key_for_eval(left).cmp(&subscription_row_key_for_eval(right))
                });
                rows.dedup_by(|left, right| {
                    subscription_row_key_for_eval(left) == subscription_row_key_for_eval(right)
                });
                Ok(rows)
            }
            RelationExpr::OrderBy { input, terms } => {
                let mut rows = self.eval_relation_expr(input, tier, identity, context)?;
                rows.sort_by(|left, right| {
                    for term in terms {
                        let ordering = compare_optional_values(
                            self.relation_column_value(left, &term.column)
                                .ok()
                                .flatten(),
                            self.relation_column_value(right, &term.column)
                                .ok()
                                .flatten(),
                        );
                        if ordering != Ordering::Equal {
                            return match term.direction {
                                OrderDirection::Asc => ordering,
                                OrderDirection::Desc => ordering.reverse(),
                            };
                        }
                    }
                    subscription_row_key_for_eval(left).cmp(&subscription_row_key_for_eval(right))
                });
                Ok(rows)
            }
            RelationExpr::Offset { input, offset } => {
                let rows = self.eval_relation_expr(input, tier, identity, context)?;
                Ok(rows.into_iter().skip(*offset).collect())
            }
            RelationExpr::Limit { input, limit } => {
                let rows = self.eval_relation_expr(input, tier, identity, context)?;
                Ok(rows.into_iter().take(*limit).collect())
            }
        }
    }

    fn relation_join_matches(
        &self,
        left: &RelationEvalRow,
        right: &RelationEvalRow,
        on: &[RelationJoinCondition],
        _context: &RelationEvalContext,
    ) -> Result<bool, Error> {
        for condition in on {
            let left_value = self.relation_column_value(left, &condition.left)?;
            let right_value = self.relation_column_value(right, &condition.right)?;
            if !relation_values_equal_or_contains(left_value.as_ref(), right_value.as_ref()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn project_relation_row(
        &self,
        row: RelationEvalRow,
        columns: &[crate::query::RelationProjectColumn],
        context: &RelationEvalContext,
    ) -> Result<Option<RelationEvalRow>, Error> {
        for column in columns {
            if let RelationProjectExpr::Column(column_ref) = &column.expr {
                if column.alias == "id" || column_ref.column == "id" {
                    return Ok(row.retarget_to_scope(column_ref.scope.as_deref()));
                }
            }
            if let RelationProjectExpr::RowId(RelationRowIdRef::Current) = column.expr {
                return Ok(Some(row));
            }
            if let RelationProjectExpr::RowId(RelationRowIdRef::Frontier) = column.expr {
                if let Some(frontier) = &context.frontier {
                    return Ok(Some(RelationEvalRow::from_row(
                        frontier.table().to_owned(),
                        frontier.clone(),
                    )));
                }
            }
        }
        Ok(Some(row))
    }

    fn eval_relation_predicate(
        &self,
        predicate: &RelationPredicate,
        row: &RelationEvalRow,
        context: &RelationEvalContext,
    ) -> Result<bool, Error> {
        match predicate {
            RelationPredicate::Cmp { left, op, right } => {
                let left = self.relation_column_value(row, left)?;
                let right = self.relation_value_ref(row, right, context)?;
                Ok(match op {
                    RelationCmpOp::Eq => {
                        relation_values_equal_or_contains(left.as_ref(), right.as_ref())
                    }
                    RelationCmpOp::Ne => {
                        !relation_values_equal_or_contains(left.as_ref(), right.as_ref())
                    }
                    RelationCmpOp::Lt
                    | RelationCmpOp::Le
                    | RelationCmpOp::Gt
                    | RelationCmpOp::Ge => {
                        let ordering = left
                            .as_ref()
                            .zip(right.as_ref())
                            .and_then(|(left, right)| compare_values(left, right));
                        matches!(
                            (op, ordering),
                            (RelationCmpOp::Lt, Some(Ordering::Less))
                                | (RelationCmpOp::Le, Some(Ordering::Less | Ordering::Equal))
                                | (RelationCmpOp::Gt, Some(Ordering::Greater))
                                | (RelationCmpOp::Ge, Some(Ordering::Greater | Ordering::Equal))
                        )
                    }
                })
            }
            RelationPredicate::IsNull { column } => {
                Ok(self.relation_column_value(row, column)?.is_none())
            }
            RelationPredicate::IsNotNull { column } => {
                Ok(self.relation_column_value(row, column)?.is_some())
            }
            RelationPredicate::In { left, values } => {
                let left = self.relation_column_value(row, left)?;
                for value in values {
                    let right = self.relation_value_ref(row, value, context)?;
                    if relation_values_equal_or_contains(left.as_ref(), right.as_ref()) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            RelationPredicate::Contains { left, right } => {
                let left = self.relation_column_value(row, left)?;
                let right = self.relation_value_ref(row, right, context)?;
                Ok(relation_value_contains(left.as_ref(), right.as_ref()))
            }
            RelationPredicate::And(predicates) => {
                for predicate in predicates {
                    if !self.eval_relation_predicate(predicate, row, context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            RelationPredicate::Or(predicates) => {
                for predicate in predicates {
                    if self.eval_relation_predicate(predicate, row, context)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            RelationPredicate::Not(predicate) => {
                Ok(!self.eval_relation_predicate(predicate, row, context)?)
            }
            RelationPredicate::True => Ok(true),
            RelationPredicate::False => Ok(false),
        }
    }

    fn relation_value_ref(
        &self,
        row: &RelationEvalRow,
        value: &RelationValueRef,
        context: &RelationEvalContext,
    ) -> Result<Option<Value>, Error> {
        match value {
            RelationValueRef::Literal(value) => Ok(json_relation_value(value)),
            RelationValueRef::OuterColumn(column) | RelationValueRef::FrontierColumn(column) => {
                self.relation_column_value(row, column)
            }
            RelationValueRef::RowId(RelationRowIdRef::Current)
            | RelationValueRef::RowId(RelationRowIdRef::Outer) => {
                Ok(Some(Value::Uuid(row.current.row_uuid().0)))
            }
            RelationValueRef::RowId(RelationRowIdRef::Frontier) => Ok(context
                .frontier
                .as_ref()
                .map(|row| Value::Uuid(row.row_uuid().0))),
            RelationValueRef::SessionRef(_) => Ok(None),
        }
    }

    fn relation_column_value(
        &self,
        row: &RelationEvalRow,
        column: &RelationColumnRef,
    ) -> Result<Option<Value>, Error> {
        let Some(scoped) = row.scoped_row(column.scope.as_deref()) else {
            return Ok(None);
        };
        if column.column == "id" {
            return Ok(Some(Value::Uuid(scoped.row.row_uuid().0)));
        }
        let table = self.table(&scoped.table)?;
        Ok(scoped.row.cell(table, &column.column))
    }

    pub(crate) fn subscription_snapshot_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        if shape.query().array_subqueries.is_empty() {
            let rows = self.query_rows_for_link(shape, binding, tier, identity)?;
            return Ok(RelationSnapshot {
                root_count: rows.len(),
                rows,
                edges: Vec::new(),
            });
        }
        self.query_relation_snapshot_for_link(shape, binding, tier, identity)
    }

    pub(crate) fn query_rows_for_link_including_deleted(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        self.query_rows_including_deleted_for_identity(&shape, &binding, tier, None, identity)
    }

    #[allow(dead_code)] // Slice 2 wires this into API-level routing.
    pub(crate) fn query_rows_at_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        self.query_rows_at(&shape, &binding, position)
    }

    fn query_rows_from_result_set(
        &mut self,
        shape: &ValidatedQuery,
        subscription: SubscriptionKey,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_name = &shape.query().table;
        let table_schema = self.table(table_name)?.clone();
        let content_descriptor = table_schema.history_storage_table().record_schema();
        let mut rows = Vec::new();
        let row_result_set = self
            .query
            .settled_result_sets
            .get(&subscription)
            .cloned()
            .unwrap_or_default();
        for (entry_table, row_uuid, tx_id) in row_result_set {
            if entry_table.as_str() != table_name {
                continue;
            }
            let tx_node_alias = self
                .node_aliases
                .get(&tx_id.node)
                .copied()
                .ok_or(Error::MissingTransaction(tx_id))?;
            let version = self
                .query_version_by_alias_with_descriptor(
                    table_name,
                    row_uuid,
                    VersionLayer::Content,
                    tx_id.time,
                    tx_node_alias,
                    &content_descriptor,
                )?
                .ok_or(Error::MissingTransaction(tx_id))?;
            rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
        }
        self.finish_query_rows(shape.query(), &mut rows)?;
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_array_subqueries(
        &mut self,
        parent_table: &TableSchema,
        parent_rows: &[CurrentRow],
        subqueries: &[ArraySubquery],
        schema_version: SchemaVersionId,
        tier: DurabilityTier,
        identity: AuthorId,
        snapshot: &mut RelationSnapshot,
        row_keys: &mut BTreeSet<(String, RowUuid)>,
    ) -> Result<(), Error> {
        for subquery in subqueries {
            let child_table = self
                .table_in_schema(&subquery.table, schema_version)?
                .clone();
            for parent in parent_rows {
                let Some(value) =
                    relation_outer_value(parent_table, parent, &subquery.outer_column)
                else {
                    continue;
                };
                let child_rows = if relation_correlation_value_is_null(&value) {
                    Vec::new()
                } else {
                    self.query_array_subquery_rows(
                        subquery,
                        schema_version,
                        tier,
                        identity,
                        value.clone(),
                    )?
                };
                if matches!(subquery.requirement, ArraySubqueryRequirement::AtLeastOne)
                    && child_rows.is_empty()
                    || matches!(
                        subquery.requirement,
                        ArraySubqueryRequirement::MatchCorrelationCardinality
                    ) && !Self::array_subquery_matches_correlation_cardinality(
                        &value,
                        &child_table,
                        &child_rows,
                        &subquery.inner_column,
                    )?
                {
                    snapshot.rows.retain(|row| {
                        row.table() != parent.table() || row.row_uuid() != parent.row_uuid()
                    });
                    snapshot.edges.retain(|edge| {
                        (edge.source_table != parent.table()
                            || edge.source_row != parent.row_uuid())
                            && (edge.target_table != parent.table()
                                || edge.target_row != parent.row_uuid())
                    });
                    row_keys.remove(&(parent.table().to_owned(), parent.row_uuid()));
                    continue;
                }
                for child in &child_rows {
                    if row_keys.insert((child.table().to_owned(), child.row_uuid())) {
                        snapshot.rows.push(child.clone());
                    }
                    snapshot.edges.push(RelationEdge {
                        source_table: parent.table().to_owned(),
                        source_row: parent.row_uuid(),
                        relation: subquery.column_name.clone(),
                        target_table: child.table().to_owned(),
                        target_row: child.row_uuid(),
                    });
                }
                self.materialize_array_subqueries(
                    &child_table,
                    &child_rows,
                    &subquery.nested_arrays,
                    schema_version,
                    tier,
                    identity,
                    snapshot,
                    row_keys,
                )?;
            }
        }
        Ok(())
    }

    fn retain_rows_satisfying_array_subquery_requirements(
        &mut self,
        rows: &mut Vec<CurrentRow>,
        subqueries: &[ArraySubquery],
        schema_version: SchemaVersionId,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(), Error> {
        if subqueries.is_empty() || rows.is_empty() {
            return Ok(());
        }
        let mut retained = Vec::with_capacity(rows.len());
        for row in std::mem::take(rows) {
            let parent_table = self.table_in_schema(row.table(), schema_version)?.clone();
            if self.row_satisfies_array_subquery_requirements(
                &parent_table,
                &row,
                subqueries,
                schema_version,
                tier,
                identity,
            )? {
                retained.push(row);
            }
        }
        *rows = retained;
        Ok(())
    }

    fn row_satisfies_array_subquery_requirements(
        &mut self,
        parent_table: &TableSchema,
        parent: &CurrentRow,
        subqueries: &[ArraySubquery],
        schema_version: SchemaVersionId,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        for subquery in subqueries {
            let Some(value) = relation_outer_value(parent_table, parent, &subquery.outer_column)
            else {
                continue;
            };
            let child_table = self
                .table_in_schema(&subquery.table, schema_version)?
                .clone();
            let child_rows = if relation_correlation_value_is_null(&value) {
                Vec::new()
            } else {
                self.query_array_subquery_rows(
                    subquery,
                    schema_version,
                    tier,
                    identity,
                    value.clone(),
                )?
            };
            let satisfies_requirement = match subquery.requirement {
                ArraySubqueryRequirement::Optional => true,
                ArraySubqueryRequirement::AtLeastOne => !child_rows.is_empty(),
                ArraySubqueryRequirement::MatchCorrelationCardinality => {
                    Self::array_subquery_matches_correlation_cardinality(
                        &value,
                        &child_table,
                        &child_rows,
                        &subquery.inner_column,
                    )?
                }
            };
            if !satisfies_requirement {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn query_array_subquery_rows(
        &mut self,
        subquery: &ArraySubquery,
        schema_version: SchemaVersionId,
        tier: DurabilityTier,
        identity: AuthorId,
        value: Value,
    ) -> Result<Vec<CurrentRow>, Error> {
        let correlation = match value {
            Value::Array(values) => in_list(
                col(subquery.inner_column.clone()),
                values.into_iter().map(lit).collect::<Vec<_>>(),
            ),
            _ => eq(col(subquery.inner_column.clone()), lit(value)),
        };
        let mut query = JazzQuery::from(subquery.table.clone()).filter(correlation);
        for filter in &subquery.filters {
            query = query.filter(filter.clone());
        }
        for order in &subquery.order_by {
            query = query.order_by(order.column.clone(), order.direction);
        }
        if let Some(select) = &subquery.select {
            query = query.select(select.clone());
        }
        if let Some(limit) = subquery.limit {
            query = query.limit(limit);
        }
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let shape = query.validate(&schema.schema).map_err(Error::Query)?;
        let binding = shape.bind(BTreeMap::new()).map_err(Error::Query)?;
        self.query_rows_for_link(&shape, &binding, tier, identity)
    }

    fn array_subquery_matches_correlation_cardinality(
        correlation_value: &Value,
        child_table: &TableSchema,
        child_rows: &[CurrentRow],
        inner_column: &str,
    ) -> Result<bool, Error> {
        let Value::Array(correlation_values) = correlation_value else {
            return Ok(!child_rows.is_empty());
        };
        let required = correlation_values
            .iter()
            .filter_map(|value| match value {
                Value::Uuid(uuid) => Some(RowUuid(*uuid)),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if required.len() != correlation_values.len() {
            return Ok(false);
        }
        if required.is_empty() {
            return Ok(true);
        }
        let mut covered = BTreeSet::new();
        for child in child_rows {
            let value = if inner_column == "id" {
                Some(Value::Uuid(child.row_uuid().0))
            } else {
                child.cell(child_table, inner_column)
            };
            if let Some(Value::Uuid(uuid)) = value {
                covered.insert(RowUuid(uuid));
            }
        }
        Ok(required.is_subset(&covered))
    }

    fn query_rows_from_projected_current_source(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let query = shape.query();
        let table = self
            .table_in_schema(&query.table, shape.schema_version())?
            .clone();
        let source_rows =
            self.current_rows_for_schema(&query.table, shape.schema_version(), tier)?;
        let (source_overrides, table_overrides) =
            self.inline_current_source_overrides_for_schema(shape, tier)?;
        let mut rows = self.query_rows_from_inline_current_source(
            shape,
            binding,
            &table,
            source_rows,
            tier,
            source_overrides,
            table_overrides,
        )?;
        self.apply_include_modes(query, shape.schema_version(), &mut rows, tier, identity)?;
        self.finish_query_rows(query, &mut rows)?;
        Ok(rows)
    }

    pub(crate) fn uses_partitioned_or_schema_projected_read(&self, shape: &ValidatedQuery) -> bool {
        shape.schema_version() != self.catalogue.current_schema_version_id
            || self.query_storage_read_tables(shape).is_some_and(|tables| {
                self.catalogue.partitions.iter().any(|(table, version)| {
                    *version != self.catalogue.current_schema_version_id && tables.contains(table)
                })
            })
    }

    fn query_storage_read_tables(&self, shape: &ValidatedQuery) -> Option<BTreeSet<String>> {
        let query = shape.query();
        let read_schema_version = shape.schema_version();
        let mut tables = BTreeSet::from([query.table.clone()]);
        tables.extend(query.joins.iter().map(|join| join.table.clone()));
        for reachable in &query.reachable {
            tables.insert(reachable.access_table.clone());
            tables.insert(reachable.edge_table.clone());
        }
        self.collect_include_read_tables(
            &query.table,
            read_schema_version,
            &query.includes,
            &mut tables,
        )?;
        Some(tables)
    }

    fn collect_include_read_tables(
        &self,
        root_table: &str,
        read_schema_version: SchemaVersionId,
        includes: &[Include],
        tables: &mut BTreeSet<String>,
    ) -> Option<()> {
        for include in includes {
            if !include.require && include.join_mode != crate::query::JoinMode::Inner {
                continue;
            }
            let mut current_table_name = root_table.to_owned();
            for segment in include.path.split('.') {
                let current_table = self
                    .table_in_schema(&current_table_name, read_schema_version)
                    .ok()?;
                let target_table = current_table.references.get(segment)?.clone();
                tables.insert(target_table.clone());
                current_table_name = target_table;
            }
        }
        Some(())
    }

    fn finish_query_rows(
        &self,
        query: &crate::query::Query,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        if query.aggregate.is_some() {
            *rows = self.aggregate_rows(query, rows)?;
        }
        self.apply_query_order(query, rows)?;
        apply_pagination(query, rows);
        self.apply_projection(query, rows)
    }

    fn aggregate_rows(
        &self,
        query: &crate::query::Query,
        rows: &[CurrentRow],
    ) -> Result<Vec<CurrentRow>, Error> {
        let aggregate = query
            .aggregate
            .as_ref()
            .ok_or(Error::InvalidStoredValue("missing aggregate query"))?;
        let table = self.table(&query.table)?.clone();
        let mut groups = Vec::<(Option<Value>, Vec<&CurrentRow>)>::new();
        if let Some(group_by) = &aggregate.group_by {
            for row in rows {
                let group = row.cell(&table, group_by);
                if let Some((_, rows)) = groups.iter_mut().find(|(key, _)| key == &group) {
                    rows.push(row);
                } else {
                    groups.push((group, vec![row]));
                }
            }
        } else {
            groups.push((None, rows.iter().collect()));
        }

        let descriptor = aggregate_row_descriptor(&table, aggregate)?;
        let mut output = Vec::new();
        for (group, rows) in groups {
            let mut values = vec![Value::Uuid(aggregate_row_uuid(&group))];
            if aggregate.group_by.is_some() {
                values.push(Value::Nullable(group.map(Box::new)));
            }
            for aggregate in &aggregate.aggregates {
                values.push(Value::Nullable(
                    aggregate_value(&table, aggregate, &rows)?.map(Box::new),
                ));
            }
            values.push(Value::U64(0));
            values.push(Value::U64(0));
            let raw = descriptor.create(&values)?;
            output.push(CurrentRow::new(
                format!("{}_aggregate", query.table),
                OwnedRecord::new(raw, descriptor),
            ));
        }
        Ok(output)
    }

    pub(crate) fn apply_query_order(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        if query.order_by.is_empty() {
            sort_query_default_rows(rows);
            return Ok(());
        }
        sort_current_rows(rows);
        if query.aggregate.is_some() {
            rows.sort_by(|left, right| {
                for order in &query.order_by {
                    let ordering = compare_optional_values(
                        aggregate_row_cell(left, &order.column),
                        aggregate_row_cell(right, &order.column),
                    );
                    let ordering = match order.direction {
                        OrderDirection::Asc => ordering,
                        OrderDirection::Desc => ordering.reverse(),
                    };
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes())
            });
            return Ok(());
        }
        let table = self.table(&query.table)?.clone();
        rows.sort_by(|left, right| {
            for order in &query.order_by {
                let ordering = compare_optional_values(
                    query_order_value(left, &table, &order.column),
                    query_order_value(right, &table, &order.column),
                );
                let ordering = match order.direction {
                    OrderDirection::Asc => ordering,
                    OrderDirection::Desc => ordering.reverse(),
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes())
        });
        Ok(())
    }

    fn apply_include_modes(
        &mut self,
        query: &crate::query::Query,
        read_schema_version: SchemaVersionId,
        rows: &mut Vec<CurrentRow>,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(), Error> {
        if rows.is_empty()
            || query.includes.is_empty()
            || query.includes.iter().all(|include| {
                !include.require && include.join_mode != crate::query::JoinMode::Inner
            })
        {
            return Ok(());
        }
        let include_modes =
            self.prepare_include_modes(query, read_schema_version, tier, identity)?;
        rows.retain(|row| include_modes.row_satisfies(row));
        Ok(())
    }

    fn apply_projection(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        let Some(columns) = &query.select else {
            return Ok(());
        };
        let table = self.table(&query.table)?.clone();
        for row in rows {
            *row = row.project(&table, columns)?;
        }
        Ok(())
    }

    fn prepare_include_modes(
        &mut self,
        query: &crate::query::Query,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<PreparedIncludeModes, Error> {
        let mut prepared = Vec::new();
        let mut target_tables = BTreeSet::new();
        for include in &query.includes {
            if !include.require && include.join_mode != crate::query::JoinMode::Inner {
                continue;
            }
            let mut current_table_name = query.table.clone();
            let mut segments = Vec::new();
            for segment in include.path.split('.') {
                let current_table =
                    self.table_in_schema(&current_table_name, read_schema_version)?;
                let column_position = current_table
                    .columns
                    .iter()
                    .position(|column| column.name == segment)
                    .ok_or(Error::InvalidStoredValue(
                        "include column was not validated",
                    ))?;
                let target_table = current_table
                    .references
                    .get(segment)
                    .cloned()
                    .ok_or(Error::InvalidStoredValue("include path was not validated"))?;
                target_tables.insert(target_table.clone());
                current_table_name = target_table.clone();
                segments.push(PreparedIncludeSegment {
                    column_position,
                    target_table,
                });
            }
            prepared.push(PreparedIncludePath { segments });
        }

        let mut rows_by_table = BTreeMap::new();
        let alias_nodes = self
            .node_aliases
            .iter()
            .map(|(node, alias)| (*alias, *node))
            .collect::<BTreeMap<_, _>>();
        let mut context = ViewEvaluationContext::for_policy_read_tier(tier);
        for table in target_tables {
            let mut rows = BTreeMap::new();
            for row in self.current_rows_for_schema(&table, read_schema_version, tier)? {
                let readable = if identity == AuthorId::SYSTEM {
                    true
                } else {
                    let Some((time, alias)) = row.projected_tx_alias() else {
                        return Err(Error::InvalidStoredValue(
                            "current row did not project content tx alias",
                        ));
                    };
                    let Some(node) = alias_nodes.get(&alias).copied() else {
                        return Err(Error::InvalidStoredValue("unknown content tx node alias"));
                    };
                    self.result_set_entry_read_policy_allows_memo(
                        &table,
                        row.row_uuid(),
                        TxId { time, node },
                        identity,
                        &mut context,
                    )?
                };
                if readable {
                    rows.insert(row.row_uuid(), row);
                }
            }
            rows_by_table.insert(table, rows);
        }
        Ok(PreparedIncludeModes {
            paths: prepared,
            rows_by_table,
        })
    }

    pub(super) fn current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        if read_schema_version == self.catalogue.current_schema_version_id
            && !self.catalogue.partitions.iter().any(|(logical, version)| {
                logical == table && *version != self.catalogue.current_schema_version_id
            })
        {
            return self.current_rows(table, tier);
        }
        let read_table = self.table_in_schema(table, read_schema_version)?;
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            let visible_at_tier = match tier {
                DurabilityTier::Global => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Global
                }
                DurabilityTier::Edge => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Edge
                }
                DurabilityTier::None | DurabilityTier::Local => {
                    !matches!(tx.fate, Fate::Rejected(_))
                }
            };
            if !visible_at_tier {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let replace = target.get(&version.row_uuid()).is_none_or(|existing| {
                version.tx_time().sort_key(tx_id.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            });
            if replace {
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, version) in content {
            if deletions.get(&row_uuid).is_some_and(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > version.tx_time()
            }) {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = version.cells(&source_table)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?;
            if projected_table == table {
                rows.push(current_row_from_cells(&read_table, row_uuid, &cells)?);
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn include_deleted_current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<(CurrentRow, bool)>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            let visible_at_tier = match tier {
                DurabilityTier::Global => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Global
                }
                DurabilityTier::Edge => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Edge
                }
                DurabilityTier::None | DurabilityTier::Local => {
                    !matches!(tx.fate, Fate::Rejected(_))
                }
            };
            if !visible_at_tier {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let replace = target.get(&version.row_uuid()).is_none_or(|existing| {
                version.tx_time().sort_key(tx_id.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            });
            if replace {
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, version) in content {
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = version.cells(&source_table)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?;
            if projected_table != table {
                continue;
            }
            let deletion = deletions.get(&row_uuid).filter(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > version.tx_time()
            });
            let deleted = deletion.is_some();
            let provenance = deletion.unwrap_or(&version);
            rows.push((
                current_row_from_materialized_cells_with_provenance(
                    &read_table,
                    &version,
                    provenance,
                    &cells,
                )?,
                deleted,
            ));
        }
        rows.sort_by(|(left, _), (right, _)| {
            left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes())
        });
        Ok(rows)
    }

    fn current_rows_for_schema_at(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        if read_schema_version == self.catalogue.current_schema_version_id
            && !self.catalogue.partitions.iter().any(|(logical, version)| {
                logical == table && *version != self.catalogue.current_schema_version_id
            })
        {
            return self.current_rows_at(table, position);
        }
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        let mut tx_ids = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            if !matches!(tx.fate, Fate::Accepted)
                || tx.durability < DurabilityTier::Global
                || tx.global_seq.is_none_or(|global_seq| global_seq > position)
            {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let key = (version.row_uuid(), version.layer());
            let replace = tx_ids.get(&key).is_none_or(|existing_tx_id| {
                version.tx_time().sort_key(tx_id.node)
                    > target
                        .get(&version.row_uuid())
                        .expect("tracked version exists")
                        .tx_time()
                        .sort_key(existing_tx_id.node)
            });
            if replace {
                tx_ids.insert(key, tx_id);
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, content) in content {
            if deletions.get(&row_uuid).is_some_and(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > content.tx_time()
            }) {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(content.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(content.table(), source_schema)?;
            let mut cells = content.cells(&source_table)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                content.table(),
                &mut cells,
            )?;
            if projected_table == table {
                rows.push(current_row_from_cells(&read_table, row_uuid, &cells)?);
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn translate_cells(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<String, Error> {
        if source == target {
            return Ok(table.to_owned());
        }
        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let forward_table = apply_compiled_lens_path(&path, cells);
            return Ok(forward_table);
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let reverse_table = apply_compiled_lens_path(&path, cells);
            return Ok(reverse_table);
        }
        Err(Error::InvalidCatalogueUpdate("lens chain is unknown"))
    }

    /// Evaluate a validated query inside an open exclusive transaction.
    pub fn tx_query(
        &mut self,
        tx_id: OpenTxId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        let query = shape.query();
        let predicate_len = self.open_tx(tx_id)?.predicate_reads.len();
        let table = self.table(&query.table)?.clone();
        let source_rows = self.tx_current_rows(tx_id, &query.table)?;
        let source_overrides = self.inline_current_source_overrides_for_tx(tx_id, shape)?;
        let mut rows = self.query_rows_from_inline_current_source(
            shape,
            binding,
            &table,
            source_rows,
            DurabilityTier::Local,
            source_overrides,
            BTreeMap::new(),
        )?;
        let predicate_read = PredicateRead {
            table: query.table.clone(),
            shape_id: shape.shape_id(),
            shape: shape.query().clone(),
            binding_id: binding.binding_id(),
            binding_values: binding.values().clone(),
        };
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx.predicate_reads.truncate(predicate_len);
        open_tx.predicate_reads.push(predicate_read);
        self.finish_query_rows(query, &mut rows)?;
        Ok(rows)
    }

    pub(super) fn query_rows_from_inline_current_source(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        table: &TableSchema,
        rows: Vec<CurrentRow>,
        tier: DurabilityTier,
        source_overrides: BTreeMap<String, GraphBuilder>,
        table_overrides: BTreeMap<String, TableSchema>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let LoweredQueryCore {
            source,
            graph,
            param_names,
            param_types,
        } = self.lower_inline_current_query_core(
            &lowered_shape,
            table,
            rows,
            tier,
            source_overrides,
            table_overrides,
        )?;
        debug_assert!(source.is_inline_current_for(&shape.query().table));
        if !param_names.is_empty() || !param_types.is_empty() {
            return Err(Error::InvalidStoredValue(
                "inline current snapshot lowering must bind all params",
            ));
        }
        let deltas = self.database.query_graph(graph).map_err(Error::Groove)?;
        self.materialize_inline_current_query_rows(table, deltas)
    }

    fn inline_current_source_overrides_for_schema(
        &mut self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<
        (
            BTreeMap<String, GraphBuilder>,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        let mut sources = BTreeMap::new();
        let mut tables = BTreeMap::new();
        for table_name in collect_join_source_tables(shape.query()) {
            let table = self
                .table_in_schema(&table_name, shape.schema_version())?
                .clone();
            let rows = self.current_rows_for_schema(&table_name, shape.schema_version(), tier)?;
            sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
            tables.insert(table_name, table);
        }
        for reachable in &shape.query().reachable {
            for table_name in [&reachable.access_table, &reachable.edge_table] {
                if sources.contains_key(table_name) {
                    continue;
                }
                let table = self
                    .table_in_schema(table_name, shape.schema_version())?
                    .clone();
                let rows =
                    self.current_rows_for_schema(table_name, shape.schema_version(), tier)?;
                sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
                tables.insert(table_name.clone(), table);
            }
        }
        Ok((sources, tables))
    }

    fn inline_current_source_overrides_for_tx(
        &mut self,
        tx_id: OpenTxId,
        shape: &ValidatedQuery,
    ) -> Result<BTreeMap<String, GraphBuilder>, Error> {
        let mut sources = BTreeMap::new();
        for table_name in collect_join_source_tables(shape.query()) {
            let table = self.table(&table_name)?.clone();
            let rows = self.tx_current_rows(tx_id, &table_name)?;
            sources.insert(table_name, inline_current_graph(&table, rows)?);
        }
        for reachable in &shape.query().reachable {
            for table_name in [&reachable.access_table, &reachable.edge_table] {
                if sources.contains_key(table_name) {
                    continue;
                }
                let table = self.table(table_name)?.clone();
                let rows = self.tx_current_rows(tx_id, table_name)?;
                sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
            }
        }
        Ok(sources)
    }

    pub(crate) fn prepared_query_plan(
        &mut self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<PreparedQueryPlan, Error> {
        let key = (shape.shape_id(), tier);
        if let Some(plan) = self.query.query_shape_cache.get(&key) {
            return Ok(plan.clone());
        }
        let LoweredQueryCore {
            source,
            graph,
            param_names,
            param_types,
        } = self.lower_query_core(shape, tier)?;
        debug_assert!(source.is_visible_current_for(&shape.query().table, tier));
        let binding_descriptor = RecordDescriptor::new(
            param_names.iter().cloned().zip(
                param_types
                    .iter()
                    .map(|column_type| column_type.value_type()),
            ),
        );
        let plan = if param_names.is_empty() {
            PreparedQueryPlan::Graph(graph)
        } else {
            let prepared = self.database.prepare(
                graph,
                query_binding_source_shape(shape),
                binding_descriptor,
                param_names.iter().cloned(),
            )?;
            PreparedQueryPlan::Prepared {
                shape: prepared.id(),
                param_names,
                param_types,
            }
        };
        self.query.query_shape_cache.insert(key, plan.clone());
        Ok(plan)
    }

    fn current_query_plan_with_lowered_include_modes(
        &mut self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<PreparedQueryPlan, Error> {
        let LoweredQueryCore {
            source,
            graph,
            param_names,
            param_types,
        } = self.lower_query_core_with_include_modes(shape, tier, identity)?;
        debug_assert!(source.is_visible_current_for(&shape.query().table, tier));
        if param_names.is_empty() {
            return Ok(PreparedQueryPlan::Graph(graph));
        }
        let binding_descriptor = RecordDescriptor::new(
            param_names.iter().cloned().zip(
                param_types
                    .iter()
                    .map(|column_type| column_type.value_type()),
            ),
        );
        let prepared = self.database.prepare(
            graph,
            query_binding_source_shape(shape),
            binding_descriptor,
            param_names.iter().cloned(),
        )?;
        Ok(PreparedQueryPlan::Prepared {
            shape: prepared.id(),
            param_names,
            param_types,
        })
    }

    fn should_lower_current_include_modes(
        &self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> bool {
        tier == DurabilityTier::Global
            && shape.schema_version() == self.catalogue.current_schema_version_id
            && shape.query().select.is_none()
            && shape.query().aggregate.is_none()
            && !shape
                .params()
                .keys()
                .any(|param| param.starts_with("__jazz_claim_"))
            && !shape.query().includes.is_empty()
            && shape.query().includes.iter().any(|include| {
                include.require || include.join_mode == crate::query::JoinMode::Inner
            })
    }

    fn lower_query_core(
        &self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<LoweredQueryCore, Error> {
        self.lower_query_core_with_required_include_modes(shape, tier, None)
    }

    fn lower_query_core_with_include_modes(
        &self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<LoweredQueryCore, Error> {
        self.lower_query_core_with_required_include_modes(shape, tier, Some(identity))
    }

    fn lower_query_core_with_required_include_modes(
        &self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
        include_identity: Option<AuthorId>,
    ) -> Result<LoweredQueryCore, Error> {
        let query = shape.query();
        let table = self.table(&query.table)?;
        let param_types = self.inline_current_param_types(shape, &table)?;
        let mut graph = self.apply_lowered_query_clauses(
            visible_current_graph(table, tier),
            shape,
            table,
            &param_types,
            LoweredQueryClauseOptions {
                tier,
                output_fields: current_row_fields(table),
                keep_binding_params_in_output: true,
                binding_source_shape: query_binding_source_shape(shape),
                source_overrides: BTreeMap::new(),
                table_overrides: BTreeMap::new(),
            },
        )?;
        if let Some(identity) = include_identity {
            graph = self.filter_root_current_by_required_include_modes(
                graph,
                table,
                &query.includes,
                identity,
                current_row_fields_with_params(table, &param_types),
                ParamBindingMode::InlineAllReachableSeeds,
                &BTreeMap::new(),
            )?;
        }
        let param_names = param_types.keys().cloned().collect::<Vec<_>>();
        let param_types = param_names
            .iter()
            .map(|name| {
                param_types
                    .get(name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(name.clone()).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(LoweredQueryCore {
            source: LoweredQuerySource::VisibleCurrent {
                table: table.name.clone(),
                tier,
            },
            graph,
            param_names,
            param_types,
        })
    }

    fn lower_historical_query_core(
        &mut self,
        shape: &ValidatedQuery,
        position: GlobalSeq,
    ) -> Result<LoweredQueryCore, Error> {
        let query = shape.query();
        let table = self
            .table_in_schema(&query.table, shape.schema_version())?
            .clone();
        let param_types = self.inline_current_param_types(shape, &table)?;
        let rows =
            self.current_rows_for_schema_at(&query.table, shape.schema_version(), position)?;
        let (source_overrides, table_overrides) =
            self.historical_source_overrides_for_position(shape, position)?;
        let graph = self.apply_lowered_query_clauses(
            inline_current_graph(&table, rows)?,
            shape,
            &table,
            &param_types,
            LoweredQueryClauseOptions {
                tier: DurabilityTier::Global,
                output_fields: current_row_fields(&table),
                keep_binding_params_in_output: true,
                binding_source_shape: historical_query_binding_source_shape(shape, position),
                source_overrides,
                table_overrides,
            },
        )?;
        let param_names = param_types.keys().cloned().collect::<Vec<_>>();
        let param_types = param_names
            .iter()
            .map(|name| {
                param_types
                    .get(name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(name.clone()).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(LoweredQueryCore {
            source: LoweredQuerySource::HistoricalCurrent {
                table: table.name.clone(),
                position,
            },
            graph,
            param_names,
            param_types,
        })
    }

    fn historical_source_overrides_for_position(
        &mut self,
        shape: &ValidatedQuery,
        position: GlobalSeq,
    ) -> Result<
        (
            BTreeMap<String, GraphBuilder>,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        let mut sources = BTreeMap::new();
        let mut tables = BTreeMap::new();
        for table_name in collect_join_source_tables(shape.query()) {
            let table = self
                .table_in_schema(&table_name, shape.schema_version())?
                .clone();
            let rows =
                self.current_rows_for_schema_at(&table_name, shape.schema_version(), position)?;
            sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
            tables.insert(table_name, table);
        }
        for reachable in &shape.query().reachable {
            for table_name in [&reachable.access_table, &reachable.edge_table] {
                if sources.contains_key(table_name) {
                    continue;
                }
                let table = self
                    .table_in_schema(table_name, shape.schema_version())?
                    .clone();
                let rows =
                    self.current_rows_for_schema_at(table_name, shape.schema_version(), position)?;
                sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
                tables.insert(table_name.clone(), table);
            }
        }
        Ok((sources, tables))
    }

    fn lower_include_deleted_query_core(
        &mut self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<LoweredQueryCore, Error> {
        let query = shape.query();
        let table = self
            .table_in_schema(&query.table, shape.schema_version())?
            .clone();
        let param_types = self.inline_current_param_types(shape, &table)?;
        let mut output_fields = current_row_fields(&table);
        output_fields.push("__jazz_deleted".to_owned());
        let source = if self.uses_partitioned_or_schema_projected_read(shape) {
            let rows = self.include_deleted_current_rows_for_schema(
                &query.table,
                shape.schema_version(),
                tier,
            )?;
            inline_include_deleted_current_graph(&table, rows)?
        } else {
            include_deleted_current_graph(&table, tier)
        };
        let (source_overrides, table_overrides) =
            self.include_deleted_source_overrides(shape, tier)?;
        let graph = self.apply_lowered_query_clauses(
            source,
            shape,
            &table,
            &param_types,
            LoweredQueryClauseOptions {
                tier,
                output_fields,
                keep_binding_params_in_output: true,
                binding_source_shape: include_deleted_query_binding_source_shape(shape, tier),
                source_overrides,
                table_overrides,
            },
        )?;
        let param_names = param_types.keys().cloned().collect::<Vec<_>>();
        let param_types = param_names
            .iter()
            .map(|name| {
                param_types
                    .get(name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(name.clone()).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(LoweredQueryCore {
            source: LoweredQuerySource::IncludeDeletedCurrent {
                table: table.name.clone(),
                tier,
            },
            graph,
            param_names,
            param_types,
        })
    }

    fn include_deleted_source_overrides(
        &mut self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<
        (
            BTreeMap<String, GraphBuilder>,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        let mut sources = BTreeMap::new();
        let mut tables = BTreeMap::new();
        for table_name in collect_join_source_tables(shape.query()) {
            let table = self
                .table_in_schema(&table_name, shape.schema_version())?
                .clone();
            let rows = self.current_rows_for_schema(&table_name, shape.schema_version(), tier)?;
            sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
            tables.insert(table_name, table);
        }
        for reachable in &shape.query().reachable {
            for table_name in [&reachable.access_table, &reachable.edge_table] {
                if sources.contains_key(table_name) {
                    continue;
                }
                let table = self
                    .table_in_schema(table_name, shape.schema_version())?
                    .clone();
                let rows =
                    self.current_rows_for_schema(table_name, shape.schema_version(), tier)?;
                sources.insert(table_name.clone(), inline_current_graph(&table, rows)?);
                tables.insert(table_name.clone(), table);
            }
        }
        Ok((sources, tables))
    }

    fn lower_inline_current_query_core(
        &self,
        shape: &ValidatedQuery,
        table: &TableSchema,
        rows: Vec<CurrentRow>,
        tier: DurabilityTier,
        source_overrides: BTreeMap<String, GraphBuilder>,
        table_overrides: BTreeMap<String, TableSchema>,
    ) -> Result<LoweredQueryCore, Error> {
        let param_types = self.inline_current_param_types(shape, table)?;
        let graph = self.apply_lowered_query_clauses(
            inline_current_graph(table, rows)?,
            shape,
            table,
            &param_types,
            LoweredQueryClauseOptions {
                tier,
                output_fields: current_row_fields(table),
                keep_binding_params_in_output: true,
                binding_source_shape: query_binding_source_shape(shape),
                source_overrides,
                table_overrides,
            },
        )?;
        let param_names = param_types.keys().cloned().collect::<Vec<_>>();
        let param_types = param_names
            .iter()
            .map(|name| {
                param_types
                    .get(name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(name.clone()).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(LoweredQueryCore {
            source: LoweredQuerySource::InlineCurrent {
                table: table.name.clone(),
            },
            graph,
            param_names,
            param_types,
        })
    }

    fn inline_current_param_types(
        &self,
        shape: &ValidatedQuery,
        table: &TableSchema,
    ) -> Result<BTreeMap<String, groove::schema::ColumnType>, Error> {
        let mut param_types = shape.params().clone();
        let query = shape.query();
        self.collect_query_nullable_param_types(
            query,
            table,
            shape.schema_version(),
            &mut param_types,
        )?;
        Ok(param_types)
    }

    fn collect_query_nullable_param_types(
        &self,
        query: &crate::query::Query,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        param_types: &mut BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(), Error> {
        collect_nullable_param_types(table, &query.filters, param_types)?;
        for join in &query.joins {
            self.collect_join_nullable_param_types(join, schema_version, param_types)?;
        }
        for reachable in &query.reachable {
            if let Operand::Param(param) = &reachable.from {
                param_types.insert(param.clone(), groove::schema::ColumnType::Uuid);
            }
            let access_table = self.table_in_schema(&reachable.access_table, schema_version)?;
            collect_nullable_param_types(&access_table, &reachable.access_filters, param_types)?;
            let edge_table = self.table_in_schema(&reachable.edge_table, schema_version)?;
            collect_nullable_param_types(&edge_table, &reachable.edge_filters, param_types)?;
        }
        for branch in &query.policy_branches {
            let branch_query = branch
                .as_query(&query.table)
                .validate(&self.catalogue.schema)?;
            let branch_table = self.table_in_schema(&branch_query.query().table, schema_version)?;
            self.collect_query_nullable_param_types(
                branch_query.query(),
                &branch_table,
                schema_version,
                param_types,
            )?;
        }
        Ok(())
    }

    fn collect_join_nullable_param_types(
        &self,
        join: &JoinVia,
        schema_version: SchemaVersionId,
        param_types: &mut BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(), Error> {
        let join_table = self.table_in_schema(&join.table, schema_version)?;
        collect_nullable_param_types(&join_table, &join.filters, param_types)?;
        for nested in &join.nested_joins {
            self.collect_join_nullable_param_types(nested, schema_version, param_types)?;
        }
        Ok(())
    }

    fn apply_lowered_query_clauses(
        &self,
        mut graph: GraphBuilder,
        shape: &ValidatedQuery,
        table: &TableSchema,
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        options: LoweredQueryClauseOptions,
    ) -> Result<GraphBuilder, Error> {
        if !shape.query().policy_branches.is_empty() {
            let mut base_query = shape.query().clone();
            let branches = std::mem::take(&mut base_query.policy_branches);
            let base_shape = base_query.validate(&self.catalogue.schema)?;
            let key_options = LoweredQueryClauseOptions {
                output_fields: options.output_fields.clone(),
                ..options.clone()
            };
            let key_fields = std::iter::once("row_uuid".to_owned())
                .chain(
                    options
                        .keep_binding_params_in_output
                        .then(|| param_types.keys().cloned())
                        .into_iter()
                        .flatten(),
                )
                .collect::<Vec<_>>();
            let mut key_graphs = vec![
                attach_output_binding_params(
                    self.apply_lowered_query_clauses(
                        graph.clone(),
                        &base_shape,
                        table,
                        param_types,
                        key_options.clone(),
                    )?,
                    param_types,
                    &key_options,
                )?
                .project(key_fields.clone()),
            ];
            for branch in branches {
                let branch_shape = branch
                    .as_query(&shape.query().table)
                    .validate(&self.catalogue.schema)?;
                key_graphs.push(
                    attach_output_binding_params(
                        self.apply_lowered_query_clauses(
                            graph.clone(),
                            &branch_shape,
                            table,
                            param_types,
                            key_options.clone(),
                        )?,
                        param_types,
                        &key_options,
                    )?
                    .project(key_fields.clone()),
                );
            }
            let authorized_keys = GraphBuilder::union(key_graphs);
            if options.keep_binding_params_in_output {
                let graph_with_params =
                    attach_output_binding_params(graph, param_types, &key_options)?;
                return Ok(GraphBuilder::join(
                    graph_with_params,
                    authorized_keys,
                    key_fields.clone(),
                    key_fields,
                )
                .project_fields(
                    options
                        .output_fields
                        .iter()
                        .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                        .chain(
                            param_types
                                .keys()
                                .cloned()
                                .map(|param| ProjectField::renamed(format!("left.{param}"), param)),
                        ),
                ));
            }
            let authorized = GraphBuilder::join(graph, authorized_keys, ["row_uuid"], ["row_uuid"])
                .project_fields(
                    options
                        .output_fields
                        .iter()
                        .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
                );
            return Ok(authorized);
        }
        let mut carried_params = BTreeSet::new();
        graph = apply_filters_with_predicate_params(
            graph,
            table,
            param_types,
            &shape.query().filters,
            options.output_fields.clone(),
            options.keep_binding_params_in_output,
            &options.binding_source_shape,
        )?;
        if options.keep_binding_params_in_output
            && !predicate_params(&shape.query().filters).is_empty()
        {
            carried_params.extend(predicate_params(&shape.query().filters));
        }
        for join in &shape.query().joins {
            if let Some(lookup) = &join.source_lookup {
                let lookup_params = options.keep_binding_params_in_output.then(|| {
                    carried_params
                        .iter()
                        .map(|param| ProjectField::renamed(format!("left.{param}"), param.clone()))
                        .collect::<Vec<_>>()
                });
                graph = self.apply_source_lookup_join(
                    graph,
                    lookup,
                    options
                        .output_fields
                        .iter()
                        .map(|field| ProjectField::renamed(format!("left.{field}"), field.clone()))
                        .chain(std::iter::once(ProjectField::renamed(
                            format!("right.{}", query_field(&lookup.value_column)),
                            query_field(&lookup.value_column),
                        )))
                        .chain(lookup_params.into_iter().flatten()),
                    &options,
                )?;
            }
            let join_table = self.lowered_related_table(&join.table, &options)?;
            let primary_join_key = join_key(join);
            let primary_left_key = if let Some(source_column) = &join.source_column {
                query_field(source_column)
            } else {
                "row_uuid".to_owned()
            };
            if !matches!(join.source_column.as_deref(), None | Some("id")) {
                graph = graph.unwrap_nullable(primary_left_key.clone());
            }
            for correlation in &join.correlated_filters {
                graph = graph.unwrap_nullable(query_field(&correlation.source_column));
            }
            let mut join_graph = options
                .source_overrides
                .get(&join.table)
                .cloned()
                .unwrap_or_else(|| visible_current_graph(join_table, options.tier));
            if join.source_column.is_none() {
                join_graph = join_graph.unwrap_nullable(primary_join_key.clone());
            } else {
                join_graph = join_graph
                    .filter(PredicateExpr::IsNotNull {
                        field: primary_join_key.clone(),
                    })
                    .unwrap_nullable(primary_join_key.clone());
            }
            for correlation in &join.correlated_filters {
                let join_field = query_field(&correlation.join_column);
                join_graph = join_graph
                    .filter(PredicateExpr::IsNotNull {
                        field: join_field.clone(),
                    })
                    .unwrap_nullable(join_field);
            }
            let join_params = if options.keep_binding_params_in_output
                && !predicate_params(&join.filters).is_empty()
            {
                predicate_params(&join.filters)
            } else {
                BTreeSet::new()
            };
            join_graph = apply_filters_with_predicate_params(
                join_graph,
                join_table,
                param_types,
                &join.filters,
                current_row_fields(join_table),
                options.keep_binding_params_in_output,
                &options.binding_source_shape,
            )?;
            let (nested_join_graph, nested_join_params) = self.apply_nested_join_clauses(
                join_graph,
                join_table,
                &join.nested_joins,
                param_types,
                &options,
            )?;
            join_graph = nested_join_graph;
            let join_params = join_params
                .into_iter()
                .chain(nested_join_params)
                .collect::<BTreeSet<_>>();
            let params = options.keep_binding_params_in_output.then(|| {
                shape
                    .params()
                    .keys()
                    .filter_map(|param| {
                        if carried_params.contains(param) {
                            Some(ProjectField::renamed(
                                format!("left.{param}"),
                                param.clone(),
                            ))
                        } else if join_params.contains(param) {
                            Some(ProjectField::renamed(
                                format!("right.{param}"),
                                param.clone(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            });
            graph = GraphBuilder::join(
                graph,
                join_graph,
                join_left_keys(join, &primary_left_key),
                join_right_keys(join, &primary_join_key),
            )
            .project_fields(
                options
                    .output_fields
                    .iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field.clone()))
                    .chain(params.into_iter().flatten()),
            );
            carried_params.extend(join_params);
        }
        for reachable in &shape.query().reachable {
            let access_table = self.lowered_related_table(&reachable.access_table, &options)?;
            let edge_table = self.lowered_related_table(&reachable.edge_table, &options)?;
            let reachable_seed_param = reachable_seed_param(reachable)?;
            let reachable_graph = self.lower_reachable_graph(
                shape,
                param_types,
                reachable,
                access_table,
                edge_table,
                options.tier,
                &options.source_overrides,
                &options.binding_source_shape,
            )?;
            let params = options.keep_binding_params_in_output.then(|| {
                let reachable_param_idx = options.output_fields.len() + carried_params.len() + 1;
                param_types
                    .keys()
                    .filter_map(|param| {
                        if carried_params.contains(param) {
                            Some(ProjectField::renamed(
                                format!("left.{param}"),
                                param.clone(),
                            ))
                        } else if *param == reachable_seed_param {
                            Some(ProjectField::renamed_resolved(
                                reachable_param_idx,
                                param.clone(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            });
            graph = GraphBuilder::join(
                graph,
                reachable_graph,
                ["row_uuid".to_owned()],
                ["access_row_uuid".to_owned()],
            )
            .project_fields(
                options
                    .output_fields
                    .iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field.clone()))
                    .chain(params.into_iter().flatten()),
            );
            if options.keep_binding_params_in_output {
                carried_params.insert(reachable_seed_param);
            }
        }
        Ok(graph)
    }

    fn apply_source_lookup_join(
        &self,
        graph: GraphBuilder,
        lookup: &crate::query::JoinSourceLookup,
        projected_fields: impl IntoIterator<Item = ProjectField>,
        options: &LoweredQueryClauseOptions,
    ) -> Result<GraphBuilder, Error> {
        let lookup_table = self.lowered_related_table(&lookup.table, options)?;
        let lookup_graph = options
            .source_overrides
            .get(&lookup.table)
            .cloned()
            .unwrap_or_else(|| visible_current_graph(lookup_table, options.tier));
        Ok(GraphBuilder::join(
            graph.unwrap_nullable(query_field(&lookup.row_id_source_column)),
            lookup_graph,
            [query_field(&lookup.row_id_source_column)],
            ["row_uuid".to_owned()],
        )
        .project_fields(projected_fields))
    }

    fn apply_nested_join_clauses(
        &self,
        mut graph: GraphBuilder,
        table: &TableSchema,
        joins: &[JoinVia],
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        options: &LoweredQueryClauseOptions,
    ) -> Result<(GraphBuilder, BTreeSet<String>), Error> {
        let mut carried_params = BTreeSet::new();
        for join in joins {
            if let Some(lookup) = &join.source_lookup {
                graph = self.apply_source_lookup_join(
                    graph,
                    lookup,
                    current_row_fields(table)
                        .into_iter()
                        .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                        .chain(
                            carried_params
                                .iter()
                                .cloned()
                                .map(|param| ProjectField::renamed(format!("left.{param}"), param)),
                        )
                        .chain(std::iter::once(ProjectField::renamed(
                            format!("right.{}", query_field(&lookup.value_column)),
                            query_field(&lookup.value_column),
                        ))),
                    options,
                )?;
            }

            let join_table = self.lowered_related_table(&join.table, options)?;
            let primary_join_key = join_key(join);
            let primary_left_key = if let Some(source_column) = &join.source_column {
                query_field(source_column)
            } else {
                "row_uuid".to_owned()
            };
            if !matches!(join.source_column.as_deref(), None | Some("id")) {
                graph = graph.unwrap_nullable(primary_left_key.clone());
            }
            for correlation in &join.correlated_filters {
                graph = graph.unwrap_nullable(query_field(&correlation.source_column));
            }
            let mut join_graph = options
                .source_overrides
                .get(&join.table)
                .cloned()
                .unwrap_or_else(|| visible_current_graph(join_table, options.tier));
            if join.source_column.is_none() {
                join_graph = join_graph.unwrap_nullable(primary_join_key.clone());
            } else {
                join_graph = join_graph
                    .filter(PredicateExpr::IsNotNull {
                        field: primary_join_key.clone(),
                    })
                    .unwrap_nullable(primary_join_key.clone());
            }
            for correlation in &join.correlated_filters {
                let join_field = query_field(&correlation.join_column);
                join_graph = join_graph
                    .filter(PredicateExpr::IsNotNull {
                        field: join_field.clone(),
                    })
                    .unwrap_nullable(join_field);
            }
            join_graph = apply_filters_with_predicate_params(
                join_graph,
                join_table,
                param_types,
                &join.filters,
                current_row_fields(join_table),
                options.keep_binding_params_in_output,
                &options.binding_source_shape,
            )?;
            let (nested_join_graph, nested_join_params) = self.apply_nested_join_clauses(
                join_graph,
                join_table,
                &join.nested_joins,
                param_types,
                options,
            )?;
            join_graph = nested_join_graph;
            let join_params = if options.keep_binding_params_in_output {
                predicate_params(&join.filters)
                    .into_iter()
                    .chain(nested_join_params)
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            graph = GraphBuilder::join(
                graph,
                join_graph,
                join_left_keys(join, &primary_left_key),
                join_right_keys(join, &primary_join_key),
            )
            .project_fields(
                current_row_fields(table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                    .chain(
                        carried_params
                            .iter()
                            .cloned()
                            .map(|param| ProjectField::renamed(format!("left.{param}"), param)),
                    )
                    .chain(
                        join_params
                            .iter()
                            .cloned()
                            .map(|param| ProjectField::renamed(format!("right.{param}"), param)),
                    ),
            );
            carried_params.extend(join_params);
        }
        Ok((graph, carried_params))
    }

    fn lowered_related_table<'a>(
        &'a self,
        table_name: &str,
        options: &'a LoweredQueryClauseOptions,
    ) -> Result<&'a TableSchema, Error> {
        options
            .table_overrides
            .get(table_name)
            .map(Ok)
            .unwrap_or_else(|| self.table(table_name))
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_result_current(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        let (shape, _binding, graph) =
            self.maintained_view_result_current_graph(shape, binding, identity)?;
        self.materialize_maintained_view_graph(graph, &shape)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_query_update_with_bundle_read_metrics(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_row_result_set: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
    ) -> Result<(SyncMessage, groove::db::StorageReadMetrics), Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let previous_row_result_set = previous_row_result_set.into_iter().collect::<BTreeSet<_>>();
        let current_row_result_set =
            self.maintained_view_result_current_set(shape, binding, identity)?;
        let result_row_adds = current_row_result_set
            .difference(&previous_row_result_set)
            .cloned()
            .collect::<Vec<_>>();
        let result_row_removes = previous_row_result_set
            .difference(&current_row_result_set)
            .cloned()
            .collect::<Vec<_>>();
        let previous_result_set = previous_row_result_set
            .iter()
            .map(|(_, _, tx_id)| *tx_id)
            .collect::<BTreeSet<_>>();
        let stream_b_versions_by_tx =
            self.maintained_view_policy_readable_version_rows_by_tx(shape, identity)?;
        let replacement_for_remove_by_row =
            self.maintained_view_replacement_for_remove_by_row(shape, identity)?;
        self.reset_storage_read_metrics();
        let update = self.view_update_for_query_result_delta_maintained_view_add_bundles(
            MaintainedViewBundleInputs {
                subscription,
                peer_complete_tx_payloads: peer_complete_tx_payloads.into_iter().collect(),
                complete_exclusive_payloads: false,
                previous_result_set,
                result_row_adds,
                result_row_removes,
                identity,
                tier: DurabilityTier::Global,
                versions_by_tx: |tx_id| {
                    stream_b_versions_by_tx
                        .get(&tx_id)
                        .cloned()
                        .unwrap_or_default()
                },
                replacement_for: |_: String, row_uuid| {
                    replacement_for_remove_by_row
                        .get(&row_uuid)
                        .map(|replacement| {
                            (
                                replacement.content_winner.clone(),
                                replacement.deletion_winner.clone(),
                            )
                        })
                        .unwrap_or_default()
                },
            },
        )?;
        let bundle_read_metrics = self.take_storage_read_metrics();
        Ok((update, bundle_read_metrics))
    }

    #[cfg(test)]
    fn maintained_view_result_current_set(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<BTreeSet<ResultRowEntry>, Error> {
        let result_current = self.maintained_view_result_current(shape, binding, identity)?;
        let row_idx =
            result_current
                .descriptor
                .field_index("row_uuid")
                .ok_or(Error::InvalidStoredValue(
                    "maintained view result_current must project row_uuid",
                ))?;
        let time_idx = result_current
            .descriptor
            .field_index("content_tx_time")
            .ok_or(Error::InvalidStoredValue(
                "maintained view result_current must project content_tx_time",
            ))?;
        let node_idx = result_current
            .descriptor
            .field_index("content_tx_node_id")
            .ok_or(Error::InvalidStoredValue(
                "maintained view result_current must project content_tx_node_id",
            ))?;
        let table = groove::Intern::new(shape.query().table.clone());
        let mut result_set = BTreeSet::new();
        for (values, weight) in result_current.to_values()? {
            if weight <= 0 {
                continue;
            }
            let Value::Uuid(row_uuid) = values[row_idx] else {
                return Err(Error::InvalidStoredValue(
                    "maintained view result_current row_uuid must be uuid",
                ));
            };
            let Value::U64(tx_time) = values[time_idx] else {
                return Err(Error::InvalidStoredValue(
                    "maintained view result_current content_tx_time must be u64",
                ));
            };
            let Value::U64(tx_node_alias) = values[node_idx] else {
                return Err(Error::InvalidStoredValue(
                    "maintained view result_current content_tx_node_id must be u64",
                ));
            };
            let node =
                self.node_for_alias(NodeAlias(tx_node_alias))
                    .ok_or(Error::InvalidStoredValue(
                        "maintained view result_current tx node alias must exist",
                    ))?;
            result_set.insert((table, RowUuid(row_uuid), TxId::new(TxTime(tx_time), node)));
        }
        Ok(result_set)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_policy_readable_versions(
        &mut self,
        shape: &ValidatedQuery,
        _binding: &Binding,
        identity: AuthorId,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let table = self.table(&shape.query().table)?.clone();
        let policy_shape = self.maintained_view_table_policy_shape(&table, identity)?;
        let graph = self.maintained_view_policy_readable_versions_graph(&table, &policy_shape)?;
        self.materialize_maintained_view_graph(graph, &policy_shape)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_policy_readable_version_rows_by_tx(
        &mut self,
        shape: &ValidatedQuery,
        identity: AuthorId,
    ) -> Result<BTreeMap<TxId, Vec<VersionRow>>, Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let table = self.table(&shape.query().table)?.clone();
        let policy_shape = self.maintained_view_table_policy_shape(&table, identity)?;

        let mut versions_by_tx = BTreeMap::<TxId, Vec<VersionRow>>::new();
        let content = self.apply_maintained_view_filters(
            GraphBuilder::table(history_table_name(&table.name)),
            &policy_shape,
            &table,
            maintained_view_version_fields(&table),
        )?;
        let content_rows = self.materialize_maintained_view_graph(content, &policy_shape)?;
        let expected_content_descriptor = table.history_storage_table().record_schema();
        if content_rows.descriptor != expected_content_descriptor {
            return Err(Error::InvalidStoredValue(
                "maintained view policy-readable content stream must preserve raw history rows",
            ));
        }
        for (record, weight) in content_rows.iter() {
            if weight <= 0 {
                continue;
            }
            let version = VersionRow {
                table: groove::Intern::new(table.name.clone()),
                record: OwnedRecord::new(record.raw().to_vec(), content_rows.descriptor),
            };
            let tx_id = self.version_tx_id(&version)?;
            versions_by_tx.entry(tx_id).or_default().push(version);
        }

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(&table)),
                &policy_shape,
                &table,
                current_row_fields(&table),
            )?
            .project(["row_uuid"]);
        let readable_current =
            self.materialize_maintained_view_graph(readable_current, &policy_shape)?;
        let row_idx = readable_current.descriptor.field_index("row_uuid").ok_or(
            Error::InvalidStoredValue("maintained view readable_current must project row_uuid"),
        )?;
        let mut readable_rows = BTreeSet::new();
        for (record, weight) in readable_current.iter() {
            if weight <= 0 {
                continue;
            }
            readable_rows.insert(RowUuid(record.get_uuid(row_idx)?));
        }

        let deletion = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion_rows = self.materialize_maintained_view_graph(deletion, &policy_shape)?;
        let expected_deletion_descriptor = table.register_storage_table().record_schema();
        if deletion_rows.descriptor != expected_deletion_descriptor {
            return Err(Error::InvalidStoredValue(
                "maintained view policy-readable deletion stream must preserve raw register rows",
            ));
        }
        for (record, weight) in deletion_rows.iter() {
            if weight <= 0 {
                continue;
            }
            let version = VersionRow {
                table: groove::Intern::new(table.name.clone()),
                record: OwnedRecord::new(record.raw().to_vec(), deletion_rows.descriptor),
            };
            if !readable_rows.contains(&version.row_uuid()) {
                continue;
            }
            let tx_id = self.version_tx_id(&version)?;
            versions_by_tx.entry(tx_id).or_default().push(version);
        }

        // Match `query_versions_for_tx`'s ordering (table, row_uuid, layer) so a
        // bundle's `versions` vec is byte-identical to the old path regardless of
        // graph materialization order.
        for versions in versions_by_tx.values_mut() {
            versions.sort_by(|left, right| {
                left.table()
                    .cmp(right.table())
                    .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                    .then_with(|| left.layer().cmp(&right.layer()))
            });
        }

        Ok(versions_by_tx)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_replacement_for_remove_by_row(
        &mut self,
        shape: &ValidatedQuery,
        identity: AuthorId,
    ) -> Result<BTreeMap<RowUuid, MaintainedViewReplacementForRemove>, Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let table = self.table(&shape.query().table)?.clone();
        let policy_shape = self.maintained_view_table_policy_shape(&table, identity)?;

        let mut replacements = BTreeMap::<RowUuid, MaintainedViewReplacementForRemove>::new();
        let visible_content_keys = visible_current_graph(&table, DurabilityTier::Global).project([
            "row_uuid",
            "tx_time",
            "tx_node_id",
        ]);
        let content = GraphBuilder::join(
            GraphBuilder::table(history_table_name(&table.name)),
            visible_content_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_history_storage_fields(&table, "left."));
        let content = self.apply_maintained_view_filters(
            content,
            &policy_shape,
            &table,
            maintained_view_history_storage_field_names(&table),
        )?;
        let content_rows = self.materialize_maintained_view_graph(content, &policy_shape)?;
        let expected_content_descriptor = table.history_storage_table().record_schema();
        if content_rows.descriptor != expected_content_descriptor {
            return Err(Error::InvalidStoredValue(
                "maintained view replacement content stream must preserve raw history rows",
            ));
        }
        for (record, weight) in content_rows.iter() {
            if weight <= 0 {
                continue;
            }
            let version = VersionRow {
                table: groove::Intern::new(table.name.clone()),
                record: OwnedRecord::new(record.raw().to_vec(), content_rows.descriptor),
            };
            let row_uuid = version.row_uuid();
            replacements.entry(row_uuid).or_default().content_winner = Some(version);
        }

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(&table)),
                &policy_shape,
                &table,
                current_row_fields(&table),
            )?
            .project(["row_uuid"]);
        let readable_current =
            self.materialize_maintained_view_graph(readable_current, &policy_shape)?;
        let row_idx = readable_current.descriptor.field_index("row_uuid").ok_or(
            Error::InvalidStoredValue(
                "maintained view replacement readable_current must project row_uuid",
            ),
        )?;
        let mut readable_rows = BTreeSet::new();
        for (record, weight) in readable_current.iter() {
            if weight <= 0 {
                continue;
            }
            readable_rows.insert(RowUuid(record.get_uuid(row_idx)?));
        }

        let deletion_current_keys =
            GraphBuilder::table(register_global_current_table_name(&table.name)).project([
                "row_uuid",
                "tx_time",
                "tx_node_id",
            ]);
        let deletion = GraphBuilder::join(
            GraphBuilder::table(register_table_name(&table.name)),
            deletion_current_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_register_storage_fields("left."));
        let deletion_rows = self.materialize_maintained_view_graph(deletion, &policy_shape)?;
        let expected_deletion_descriptor = table.register_storage_table().record_schema();
        if deletion_rows.descriptor != expected_deletion_descriptor {
            return Err(Error::InvalidStoredValue(
                "maintained view replacement deletion stream must preserve raw register rows",
            ));
        }
        for (record, weight) in deletion_rows.iter() {
            if weight <= 0 {
                continue;
            }
            let version = VersionRow {
                table: groove::Intern::new(table.name.clone()),
                record: OwnedRecord::new(record.raw().to_vec(), deletion_rows.descriptor),
            };
            let row_uuid = version.row_uuid();
            if readable_rows.contains(&row_uuid) {
                replacements.entry(row_uuid).or_default().deletion_winner = Some(version);
            }
        }

        Ok(replacements)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_tagged_terminal(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        let shape = maintained_view_bind_filter_literals_with_mode(
            &shape,
            &binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        self.ensure_maintained_view_query_slice(shape.query())?;
        let graph = self.maintained_view_tagged_terminal_graph_for_shape(
            &shape,
            identity,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        self.materialize_maintained_view_graph(graph, &shape)
    }

    pub(crate) fn maintained_subscription_view_from_cold_snapshot(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<
        (
            groove::ivm::Subscription,
            MaintainedSubscriptionView,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        let (_shape, routing_shape, _binding, graph, routing_graph) =
            self.maintained_view_tagged_terminal_graph(shape, binding, identity)?;
        let tables = self.maintained_view_terminal_tables(&_shape)?;
        self.database.flush().map_err(Error::Groove)?;
        let subscription = self.subscribe_maintained_view_tagged_graph(
            &routing_shape,
            &_binding,
            identity,
            graph,
            routing_graph,
        )?;
        let mut maintained = MaintainedSubscriptionView::default();
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        let snapshot = subscription
            .recv()
            .map_err(|_| Error::InvalidStoredValue("cold snapshot subscription disconnected"))?;
        let snapshot_transitions =
            maintained.apply_tagged_deltas(&snapshot, &tables, &self.node_aliases)?;
        transitions.adds.extend(snapshot_transitions.adds);
        transitions.removes.extend(snapshot_transitions.removes);
        loop {
            match subscription.try_recv() {
                Ok(deltas) => {
                    let delta_transitions =
                        maintained.apply_tagged_deltas(&deltas, &tables, &self.node_aliases)?;
                    transitions.adds.extend(delta_transitions.adds);
                    transitions.removes.extend(delta_transitions.removes);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Error::InvalidStoredValue(
                        "cold snapshot subscription disconnected",
                    ));
                }
            }
        }
        Ok((subscription, maintained, transitions, tables))
    }

    fn subscribe_maintained_view_tagged_graph(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        graph: GraphBuilder,
        routing_graph: GraphBuilder,
    ) -> Result<groove::ivm::Subscription, Error> {
        let param_types = self.maintained_view_hidden_param_types_for_shape(
            shape,
            identity,
            ParamBindingMode::RetainAllParams,
        )?;
        if param_types.is_empty() {
            return self.database.subscribe(graph).map_err(Error::Groove);
        }
        let param_names = param_types.keys().cloned().collect::<Vec<_>>();
        let param_type_list = param_names
            .iter()
            .map(|name| {
                param_types
                    .get(name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(name.clone()).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let binding_descriptor = RecordDescriptor::new(
            param_names.iter().cloned().zip(
                param_type_list
                    .iter()
                    .map(|column_type| column_type.value_type()),
            ),
        );
        let binding_source_shape = maintained_view_binding_source_shape(shape);
        let prepared = self.database.prepare_with_routing(
            graph,
            routing_graph,
            binding_source_shape,
            binding_descriptor,
            param_names.iter().cloned(),
        )?;
        let mut binding_values = binding.values().clone();
        insert_claim_bindings(
            &mut binding_values,
            &param_types,
            identity,
            self.session_claims.get(&identity),
        );
        let values =
            binding_values_for_param_names(&binding_values, &param_names, &param_type_list)?;
        self.database
            .bind_shape(prepared.id(), &values)
            .map_err(Error::Groove)
    }

    pub(crate) fn maintained_view_tagged_terminal_graph(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<
        (
            ValidatedQuery,
            ValidatedQuery,
            Binding,
            GraphBuilder,
            GraphBuilder,
        ),
        Error,
    > {
        let (composed_shape, composed_binding) =
            self.policy_composed_shape_binding(shape, binding, identity)?;
        let clean_shape = maintained_view_bind_filter_literals_with_mode(
            &composed_shape,
            &composed_binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let retained_shape = maintained_view_bind_filter_literals_with_mode(
            &composed_shape,
            &composed_binding,
            &self.catalogue.schema,
            ParamBindingMode::RetainAllParams,
        )?;
        self.ensure_maintained_view_query_slice(clean_shape.query())?;
        self.ensure_maintained_view_query_slice(retained_shape.query())?;
        let terminal_tables = self.maintained_view_terminal_tables(&retained_shape)?;
        let clean_graph = self.maintained_view_tagged_terminal_graph_for_shape(
            &clean_shape,
            identity,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let retained_param_types = self.maintained_view_hidden_param_types_for_shape(
            &retained_shape,
            identity,
            ParamBindingMode::RetainAllParams,
        )?;
        let output_fields = maintained_view_tagged_field_names(terminal_tables.values());
        let (graph, routing_graph) =
            if maintained_view_has_binding_dependent_reachable(&retained_shape) {
                let retained_graph = self.maintained_view_tagged_terminal_graph_for_shape(
                    &retained_shape,
                    identity,
                    ParamBindingMode::RetainAllParams,
                )?;
                let graph = retained_graph
                    .clone()
                    .project_fields(output_fields.iter().cloned().map(ProjectField::named));
                let routing_graph = retained_graph.project_fields(
                    output_fields
                        .iter()
                        .cloned()
                        .map(ProjectField::named)
                        .chain(
                            retained_param_types
                                .keys()
                                .cloned()
                                .map(ProjectField::named),
                        ),
                );
                (graph, routing_graph)
            } else {
                let graph = maintained_view_public_terminal_graph_with_bound_params(
                    clean_graph.clone(),
                    terminal_tables.values(),
                    &retained_param_types,
                    &maintained_view_binding_source_shape(&retained_shape),
                )?;
                let routing_graph = attach_params_to_graph(
                    clean_graph,
                    &retained_param_types,
                    output_fields,
                    true,
                    &maintained_view_binding_source_shape(&retained_shape),
                )?;
                (graph, routing_graph)
            };
        Ok((
            clean_shape,
            retained_shape,
            composed_binding,
            graph,
            routing_graph,
        ))
    }

    fn maintained_view_tagged_terminal_graph_for_shape(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        policy_param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        let terminal_tables = self.maintained_view_terminal_tables(shape)?;
        let hidden_param_types = self.maintained_view_hidden_param_types_for_shape(
            shape,
            identity,
            policy_param_binding_mode,
        )?;
        let result_current = self.maintained_view_result_closure_graph(
            shape,
            identity,
            &terminal_tables,
            policy_param_binding_mode,
            &hidden_param_types,
        )?;
        let mut graphs = vec![result_current];
        for table in terminal_tables.values() {
            let policy_shape = self.maintained_view_table_policy_shape_with_mode(
                table,
                identity,
                policy_param_binding_mode,
            )?;
            let (version_content, version_deletion) = self
                .maintained_view_policy_readable_version_tagged_graphs(
                    table,
                    &policy_shape,
                    terminal_tables.values(),
                    policy_param_binding_mode,
                    &hidden_param_types,
                )?;
            let (replacement_content, replacement_deletion) = self
                .maintained_view_replacement_tagged_graphs(
                    table,
                    &policy_shape,
                    terminal_tables.values(),
                    policy_param_binding_mode,
                    &hidden_param_types,
                )?;
            graphs.extend([
                version_content,
                version_deletion,
                replacement_content,
                replacement_deletion,
            ]);
        }
        for reachable in &shape.query().reachable {
            for table_name in [&reachable.edge_table, &reachable.access_table] {
                let table = terminal_tables
                    .get(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
                let (version_content, version_deletion) = self
                    .reachable_policy_readable_version_tagged_graphs(
                        &shape,
                        reachable,
                        table,
                        terminal_tables.values(),
                        policy_param_binding_mode,
                        &hidden_param_types,
                    )?;
                let (replacement_content, replacement_deletion) = self
                    .reachable_replacement_tagged_graphs(
                        &shape,
                        reachable,
                        table,
                        terminal_tables.values(),
                        policy_param_binding_mode,
                        &hidden_param_types,
                    )?;
                graphs.extend([
                    version_content,
                    version_deletion,
                    replacement_content,
                    replacement_deletion,
                ]);
            }
        }
        Ok(GraphBuilder::union(graphs))
    }

    pub(crate) fn maintained_view_terminal_tables(
        &self,
        shape: &ValidatedQuery,
    ) -> Result<BTreeMap<String, TableSchema>, Error> {
        let mut tables = BTreeMap::new();
        self.collect_maintained_view_terminal_tables_for_query(shape.query(), &mut tables)?;
        Ok(tables)
    }

    fn collect_maintained_view_terminal_tables_for_query(
        &self,
        query: &crate::query::Query,
        tables: &mut BTreeMap<String, TableSchema>,
    ) -> Result<(), Error> {
        let root = self.table(&query.table)?.clone();
        tables.insert(root.name.clone(), root.clone());
        for target in root.references.values() {
            let table = self.table(target)?.clone();
            tables.insert(table.name.clone(), table);
        }
        for include in &query.includes {
            let mut current_table = root.clone();
            for segment in include.path.split('.') {
                let Some(target) = current_table.references.get(segment) else {
                    return Err(Error::InvalidStoredValue("include path was not validated"));
                };
                let table = self.table(target)?.clone();
                tables.insert(table.name.clone(), table.clone());
                current_table = table;
            }
        }
        for join in &query.joins {
            self.collect_maintained_view_terminal_tables_for_join(join, tables)?;
        }
        for reachable in &query.reachable {
            let access_table = self.table(&reachable.access_table)?.clone();
            tables.insert(access_table.name.clone(), access_table);
            let edge_table = self.table(&reachable.edge_table)?.clone();
            tables.insert(edge_table.name.clone(), edge_table);
        }
        for branch in &query.policy_branches {
            self.collect_maintained_view_terminal_tables_for_query(
                &branch.as_query(&query.table),
                tables,
            )?;
        }
        Ok(())
    }

    fn collect_maintained_view_terminal_tables_for_join(
        &self,
        join: &JoinVia,
        tables: &mut BTreeMap<String, TableSchema>,
    ) -> Result<(), Error> {
        let join_table = self.table(&join.table)?.clone();
        self.collect_terminal_table_and_references(join_table, tables)?;
        if let Some(lookup) = &join.source_lookup {
            let lookup_table = self.table(&lookup.table)?.clone();
            self.collect_terminal_table_and_references(lookup_table, tables)?;
        }
        for nested in &join.nested_joins {
            self.collect_maintained_view_terminal_tables_for_join(nested, tables)?;
        }
        Ok(())
    }

    fn collect_terminal_table_and_references(
        &self,
        table: TableSchema,
        tables: &mut BTreeMap<String, TableSchema>,
    ) -> Result<(), Error> {
        tables.insert(table.name.clone(), table.clone());
        for target in table.references.values() {
            let table = self.table(target)?.clone();
            tables.insert(table.name.clone(), table);
        }
        Ok(())
    }

    #[cfg(test)]
    fn maintained_view_table_policy_shape(
        &self,
        table: &TableSchema,
        identity: AuthorId,
    ) -> Result<ValidatedQuery, Error> {
        self.maintained_view_table_policy_shape_with_mode(
            table,
            identity,
            ParamBindingMode::InlineAllReachableSeeds,
        )
    }

    fn maintained_view_table_policy_shape_with_mode(
        &self,
        table: &TableSchema,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
    ) -> Result<ValidatedQuery, Error> {
        let policy_shape =
            crate::query::Query::from(table.name.as_str()).validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let (policy_shape, policy_binding) =
            self.policy_composed_shape_binding(&policy_shape, &policy_binding, identity)?;
        if !policy_shape.query().includes.is_empty() {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view policy slice does not support include policies",
            ));
        }
        self.ensure_maintained_view_query_slice(policy_shape.query())?;
        let policy_shape = maintained_view_bind_filter_literals_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            param_binding_mode,
        )?;
        Ok(policy_shape)
    }

    fn maintained_view_hidden_param_types_for_shape(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
    ) -> Result<BTreeMap<String, groove::schema::ColumnType>, Error> {
        if matches!(
            param_binding_mode,
            ParamBindingMode::InlineAllReachableSeeds
        ) {
            return Ok(BTreeMap::new());
        }
        let mut param_types = maintained_view_hidden_param_column_types(&graph_param_types(
            shape,
            &self.catalogue.schema,
        )?);
        for table in self.maintained_view_terminal_tables(shape)?.values() {
            let policy_shape = self.maintained_view_table_policy_shape_with_mode(
                table,
                identity,
                param_binding_mode,
            )?;
            param_types.extend(maintained_view_hidden_param_column_types(
                &graph_param_types(&policy_shape, &self.catalogue.schema)?,
            ));
        }
        Ok(param_types)
    }

    fn maintained_view_policy_readable_version_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        let filter_param_types = graph_param_types(policy_shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&filter_param_types, param_binding_mode);
        let content = self.apply_maintained_view_filters(
            GraphBuilder::table(history_table_name(&table.name)),
            policy_shape,
            table,
            maintained_view_version_fields(table),
        )?;
        let content = content.project_fields(maintained_view_tagged_content_fields(
            table,
            "version_content",
            "",
            terminal_tables.clone(),
            output_hidden_param_types,
            available_hidden_param_types,
            "",
        ));

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(table)),
                policy_shape,
                table,
                current_row_fields(table),
            )?
            .project(
                std::iter::once("row_uuid".to_owned())
                    .chain(available_hidden_param_types.keys().cloned())
                    .collect::<Vec<_>>(),
            );
        let deleted = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion = GraphBuilder::join(deleted, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "version_deletion",
                "left.",
                terminal_tables,
                output_hidden_param_types,
                available_hidden_param_types,
                "right.",
            ));

        Ok((content, deletion))
    }

    fn maintained_view_result_closure_graph(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<GraphBuilder, Error> {
        let root_table = self.table(&shape.query().table)?.clone();
        let root_current = self.maintained_view_bound_query_current_graph(shape)?;
        let result_current = self.maintained_view_filter_result_current_by_include_modes(
            root_current.clone(),
            &root_table,
            shape,
            identity,
            param_binding_mode,
        )?;
        let result_current = apply_maintained_view_result_limit(result_current, shape.query());
        let param_types = maintained_view_hidden_param_column_types(&graph_param_types(
            shape,
            &self.catalogue.schema,
        )?);
        let hidden_param_types =
            hidden_maintained_view_param_types(&param_types, param_binding_mode);
        let mut result_current_param_types = hidden_param_types.clone();
        result_current_param_types.extend(self.include_policy_hidden_param_types(
            &root_table,
            &shape.query().includes,
            identity,
            param_binding_mode,
            true,
        )?);
        let mut graphs =
            vec![
                result_current
                    .clone()
                    .project_fields(maintained_view_tagged_content_fields(
                        &root_table,
                        "result_current",
                        "",
                        terminal_tables.values(),
                        output_hidden_param_types,
                        &result_current_param_types,
                        "",
                    )),
            ];

        for (column, target_table_name) in &root_table.references {
            let target_table = self.table(target_table_name)?.clone();
            graphs.push(self.maintained_view_reference_result_graph(
                result_current.clone(),
                column,
                &target_table,
                identity,
                terminal_tables,
                true,
                output_hidden_param_types,
                hidden_param_types,
                param_binding_mode,
            )?);
        }

        for include in &shape.query().includes {
            graphs.extend(self.maintained_view_include_result_graphs(
                result_current.clone(),
                &root_table,
                include,
                identity,
                terminal_tables,
                output_hidden_param_types,
                &result_current_param_types,
                param_binding_mode,
            )?);
        }

        for join in &shape.query().joins {
            let join_table = self.table(&join.table)?.clone();
            let join_current = self.maintained_view_join_closure_current_graph(
                root_current.clone(),
                &root_table,
                join,
                identity,
                param_binding_mode,
            )?;
            graphs.push(join_current.clone().project_fields(
                maintained_view_tagged_content_fields(
                    &join_table,
                    "result_current",
                    "",
                    terminal_tables.values(),
                    output_hidden_param_types,
                    hidden_param_types,
                    "",
                ),
            ));
            for (column, target_table_name) in &join_table.references {
                let target_table = self.table(target_table_name)?.clone();
                graphs.push(self.maintained_view_reference_result_graph(
                    join_current.clone(),
                    column,
                    &target_table,
                    identity,
                    terminal_tables,
                    true,
                    output_hidden_param_types,
                    hidden_param_types,
                    param_binding_mode,
                )?);
            }
        }

        Ok(GraphBuilder::union(graphs))
    }

    fn maintained_view_filter_result_current_by_include_modes(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        shape: &ValidatedQuery,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        self.filter_root_current_by_required_include_modes(
            root,
            root_table,
            &shape.query().includes,
            identity,
            maintained_view_version_fields(root_table),
            param_binding_mode,
            hidden_maintained_view_param_types(
                &graph_param_types(shape, &self.catalogue.schema)?,
                param_binding_mode,
            ),
        )
    }

    fn filter_root_current_by_required_include_modes(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        includes: &[Include],
        identity: AuthorId,
        root_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
        preserved_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<GraphBuilder, Error> {
        let mut graph = root;
        for include in includes {
            if !include.require && include.join_mode != crate::query::JoinMode::Inner {
                continue;
            }
            graph = self.filter_root_current_by_required_include_path(
                graph,
                root_table,
                include,
                identity,
                root_fields.clone(),
                param_binding_mode,
                preserved_param_types,
            )?;
        }
        Ok(graph)
    }

    fn filter_root_current_by_required_include_path(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        include: &Include,
        identity: AuthorId,
        root_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
        preserved_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<GraphBuilder, Error> {
        let segments = include.path.split('.').collect::<Vec<_>>();
        let mut current = root.clone();
        let mut current_table = root_table.clone();
        let mut current_param_types = preserved_param_types.clone();
        for (idx, segment) in segments.iter().enumerate() {
            let target_table_name = current_table
                .references
                .get(*segment)
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?
                .clone();
            let target_table = self.table(&target_table_name)?.clone();
            let target_policy_shape = self.maintained_view_table_policy_shape_with_mode(
                &target_table,
                identity,
                param_binding_mode,
            )?;
            let mut target_param_types = hidden_maintained_view_param_types(
                &graph_param_types(&target_policy_shape, &self.catalogue.schema)?,
                param_binding_mode,
            )
            .clone();
            target_param_types.retain(|param, _| !current_param_types.contains_key(param));
            let target = self.maintained_view_policy_readable_current_graph(
                &target_table,
                identity,
                param_binding_mode,
            )?;
            let source_key = format!("user_{segment}");
            let source = current.unwrap_nullable(source_key.clone());
            let mut fields = root_fields
                .iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field.clone()))
                .collect::<Vec<_>>();
            if idx + 1 < segments.len() {
                let next_segment = segments[idx + 1];
                fields.push(ProjectField::renamed(
                    format!("right.user_{next_segment}"),
                    format!("user_{next_segment}"),
                ));
            }
            fields.extend(
                current_param_types
                    .keys()
                    .map(|param| ProjectField::renamed(format!("left.{param}"), param.clone())),
            );
            fields.extend(
                target_param_types
                    .keys()
                    .map(|param| ProjectField::renamed(format!("right.{param}"), param.clone())),
            );
            current = GraphBuilder::join(source, target, [source_key], ["row_uuid"])
                .project_fields(fields);
            current_param_types.extend(target_param_types);
            current_table = target_table;
        }
        Ok(
            GraphBuilder::join(root, current, ["row_uuid"], ["row_uuid"]).project_fields(
                root_fields
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                    .chain(current_param_types.keys().map(|param| {
                        ProjectField::renamed(format!("right.{param}"), param.clone())
                    })),
            ),
        )
    }

    fn maintained_view_bound_query_current_graph(
        &self,
        shape: &ValidatedQuery,
    ) -> Result<GraphBuilder, Error> {
        let table = self.table(&shape.query().table)?;
        let graph = self.maintained_view_content_current_with_version(table)?;
        self.apply_maintained_view_filters(
            graph,
            shape,
            table,
            maintained_view_version_fields(table),
        )
    }

    fn maintained_view_policy_readable_current_graph(
        &self,
        table: &TableSchema,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        let policy_shape =
            self.maintained_view_table_policy_shape_with_mode(table, identity, param_binding_mode)?;
        let graph = self.maintained_view_content_current_with_version(table)?;
        self.apply_maintained_view_filters(
            graph,
            &policy_shape,
            table,
            maintained_view_version_fields(table),
        )
    }

    fn include_policy_hidden_param_types(
        &self,
        root_table: &TableSchema,
        includes: &[Include],
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        required_only: bool,
    ) -> Result<BTreeMap<String, groove::schema::ColumnType>, Error> {
        let mut param_types = BTreeMap::new();
        for include in includes {
            if required_only
                && !include.require
                && include.join_mode != crate::query::JoinMode::Inner
            {
                continue;
            }
            let mut current_table = root_table.clone();
            for segment in include.path.split('.') {
                let target_table_name = current_table
                    .references
                    .get(segment)
                    .ok_or(Error::InvalidStoredValue("include path was not validated"))?
                    .clone();
                let target_table = self.table(&target_table_name)?.clone();
                let policy_shape = self.maintained_view_table_policy_shape_with_mode(
                    &target_table,
                    identity,
                    param_binding_mode,
                )?;
                let policy_param_types = graph_param_types(&policy_shape, &self.catalogue.schema)?;
                param_types.extend(maintained_view_hidden_param_column_types(
                    hidden_maintained_view_param_types(&policy_param_types, param_binding_mode),
                ));
                current_table = target_table;
            }
        }
        Ok(param_types)
    }

    fn maintained_view_reference_result_graph(
        &self,
        source: GraphBuilder,
        source_column: &str,
        target_table: &TableSchema,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
        unwrap_source: bool,
        output_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        source_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        let target_policy_shape = self.maintained_view_table_policy_shape_with_mode(
            target_table,
            identity,
            param_binding_mode,
        )?;
        let mut target_param_types = hidden_maintained_view_param_types(
            &graph_param_types(&target_policy_shape, &self.catalogue.schema)?,
            param_binding_mode,
        )
        .clone();
        target_param_types.retain(|param, _| !source_param_types.contains_key(param));
        let mut available_param_types = source_param_types.clone();
        available_param_types.extend(target_param_types.clone());
        let target = self.maintained_view_policy_readable_current_graph(
            target_table,
            identity,
            param_binding_mode,
        )?;
        let source_key = format!("user_{source_column}");
        let source = if unwrap_source {
            source.unwrap_nullable(source_key.clone())
        } else {
            source
        };
        let joined =
            GraphBuilder::join(source, target, [source_key], ["row_uuid"]).project_fields(
                maintained_view_version_fields(target_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("right.{field}"), field))
                    .chain(
                        source_param_types.keys().map(|param| {
                            ProjectField::renamed(format!("left.{param}"), param.clone())
                        }),
                    )
                    .chain(target_param_types.keys().map(|param| {
                        ProjectField::renamed(format!("right.{param}"), param.clone())
                    })),
            );
        Ok(joined.project_fields(maintained_view_tagged_content_fields(
            target_table,
            "result_current",
            "",
            terminal_tables.values(),
            output_param_types,
            &available_param_types,
            "",
        )))
    }

    fn maintained_view_include_result_graphs(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        include: &Include,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
        output_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        initial_available_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        param_binding_mode: ParamBindingMode,
    ) -> Result<Vec<GraphBuilder>, Error> {
        let mut graphs = Vec::new();
        let mut current = root;
        let mut current_table = root_table.clone();
        let mut available_param_types = initial_available_param_types.clone();
        for segment in include.path.split('.') {
            let target_table_name = current_table
                .references
                .get(segment)
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?
                .clone();
            let target_table = self.table(&target_table_name)?.clone();
            let target_policy_shape = self.maintained_view_table_policy_shape_with_mode(
                &target_table,
                identity,
                param_binding_mode,
            )?;
            let mut target_param_types = hidden_maintained_view_param_types(
                &graph_param_types(&target_policy_shape, &self.catalogue.schema)?,
                param_binding_mode,
            )
            .clone();
            target_param_types.retain(|param, _| !available_param_types.contains_key(param));
            let target = self.maintained_view_policy_readable_current_graph(
                &target_table,
                identity,
                param_binding_mode,
            )?;
            let source_key = format!("user_{segment}");
            let source = current.unwrap_nullable(source_key.clone());
            current = GraphBuilder::join(source, target, [source_key], ["row_uuid"])
                .project_fields(
                    maintained_view_version_fields(&target_table)
                        .into_iter()
                        .map(|field| ProjectField::renamed(format!("right.{field}"), field))
                        .chain(available_param_types.keys().map(|param| {
                            ProjectField::renamed(format!("left.{param}"), param.clone())
                        }))
                        .chain(target_param_types.keys().map(|param| {
                            ProjectField::renamed(format!("right.{param}"), param.clone())
                        })),
                );
            available_param_types.extend(target_param_types);
            graphs.push(
                current
                    .clone()
                    .project_fields(maintained_view_tagged_content_fields(
                        &target_table,
                        "result_current",
                        "",
                        terminal_tables.values(),
                        output_param_types,
                        &available_param_types,
                        "",
                    )),
            );
            current_table = target_table;
        }
        Ok(graphs)
    }

    fn maintained_view_join_closure_current_graph(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        join: &JoinVia,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        let join_table = self.table(&join.table)?.clone();
        let mut join_query = crate::query::Query::from(join.table.as_str());
        for predicate in &join.filters {
            join_query = join_query.filter(predicate.clone());
        }
        let join_shape = join_query.validate(&self.catalogue.schema)?;
        let join_shape = self.maintained_view_bind_filter_literals_for_empty_binding_with_mode(
            &join_shape,
            identity,
            param_binding_mode,
        )?;
        let join_param_types = graph_param_types(&join_shape, &self.catalogue.schema)?;
        let join_hidden_param_types =
            hidden_maintained_view_param_types(&join_param_types, param_binding_mode);
        let join_current = self.apply_maintained_view_policy_to_current_graph(
            self.maintained_view_content_current_with_version(&join_table)?,
            &join_table,
            &join_shape,
            identity,
            maintained_view_version_fields(&join_table),
            param_binding_mode,
        )?;
        let root = if let Some(lookup) = &join.source_lookup {
            let lookup_table = self.table(&lookup.table)?.clone();
            GraphBuilder::join(
                root.unwrap_nullable(query_field(&lookup.row_id_source_column)),
                self.maintained_view_content_current_with_version(&lookup_table)?,
                [query_field(&lookup.row_id_source_column)],
                ["row_uuid".to_owned()],
            )
            .project_fields(
                maintained_view_version_fields(root_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                    .chain(std::iter::once(ProjectField::renamed(
                        format!("right.{}", query_field(&lookup.value_column)),
                        query_field(&lookup.value_column),
                    ))),
            )
        } else {
            root
        };
        let primary_left_key = join
            .source_column
            .as_ref()
            .map(|column| query_field(column))
            .unwrap_or_else(|| "row_uuid".to_owned());
        let primary_join_key = join_key(join);
        let root = if matches!(join.target, JoinTarget::RowId)
            && !matches!(join.source_column.as_deref(), None | Some("id"))
        {
            root.unwrap_nullable(primary_left_key.clone())
        } else {
            root
        };
        let root = join
            .correlated_filters
            .iter()
            .fold(root, |root, correlation| {
                root.unwrap_nullable(query_field(&correlation.source_column))
            });
        let joined = if join.source_column.is_none() {
            let root_fields = std::iter::once("row_uuid".to_owned())
                .chain(
                    join.correlated_filters
                        .iter()
                        .map(|correlation| query_field(&correlation.source_column)),
                )
                .collect::<Vec<_>>();
            let join_current = join.correlated_filters.iter().fold(
                join_current
                    .clone()
                    .unwrap_nullable(primary_join_key.clone()),
                |join_current, correlation| {
                    let join_field = query_field(&correlation.join_column);
                    join_current
                        .filter(PredicateExpr::IsNotNull {
                            field: join_field.clone(),
                        })
                        .unwrap_nullable(join_field)
                },
            );
            let eligible = GraphBuilder::join(
                root.project(root_fields),
                join_current.clone(),
                join_left_keys(join, &primary_left_key),
                join_right_keys(join, &primary_join_key),
            )
            .project_fields([ProjectField::renamed("right.row_uuid", "row_uuid")]);
            GraphBuilder::join(join_current, eligible, ["row_uuid"], ["row_uuid"]).project_fields(
                maintained_view_version_fields(&join_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                    .chain(join_hidden_param_types.keys().map(|param| {
                        ProjectField::renamed(format!("left.{param}"), param.clone())
                    })),
            )
        } else {
            let join_current = join.correlated_filters.iter().fold(
                join_current
                    .filter(PredicateExpr::IsNotNull {
                        field: primary_join_key.clone(),
                    })
                    .unwrap_nullable(primary_join_key.clone()),
                |join_current, correlation| {
                    let join_field = query_field(&correlation.join_column);
                    join_current
                        .filter(PredicateExpr::IsNotNull {
                            field: join_field.clone(),
                        })
                        .unwrap_nullable(join_field)
                },
            );
            GraphBuilder::join(
                root,
                join_current,
                join_left_keys(join, &primary_left_key),
                join_right_keys(join, &primary_join_key),
            )
            .project_fields(
                maintained_view_version_fields(&join_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("right.{field}"), field))
                    .chain(join_hidden_param_types.keys().map(|param| {
                        ProjectField::renamed(format!("right.{param}"), param.clone())
                    })),
            )
        };
        Ok(joined)
    }

    fn maintained_view_bind_filter_literals_for_empty_binding_with_mode(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        mode: ParamBindingMode,
    ) -> Result<ValidatedQuery, Error> {
        let mut values = BTreeMap::new();
        insert_claim_bindings(
            &mut values,
            shape.params(),
            identity,
            self.session_claims.get(&identity),
        );
        let binding = shape.bind(values)?;
        maintained_view_bind_filter_literals_with_mode(
            shape,
            &binding,
            &self.catalogue.schema,
            mode,
        )
    }

    fn apply_maintained_view_policy_to_current_graph(
        &self,
        graph: GraphBuilder,
        table: &TableSchema,
        shape: &ValidatedQuery,
        identity: AuthorId,
        output_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
    ) -> Result<GraphBuilder, Error> {
        let mut values = BTreeMap::new();
        insert_claim_bindings(
            &mut values,
            shape.params(),
            identity,
            self.session_claims.get(&identity),
        );
        let base_binding = shape.bind(values)?;
        let (policy_shape, policy_binding) =
            self.policy_composed_shape_binding(shape, &base_binding, identity)?;
        let policy_shape = maintained_view_bind_filter_literals_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            param_binding_mode,
        )?;
        self.apply_maintained_view_filters(graph, &policy_shape, table, output_fields)
    }

    fn maintained_view_replacement_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        let filter_param_types = graph_param_types(policy_shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&filter_param_types, param_binding_mode);
        let visible_content_keys = visible_current_graph(table, DurabilityTier::Global).project([
            "row_uuid",
            "tx_time",
            "tx_node_id",
        ]);
        let content = GraphBuilder::join(
            GraphBuilder::table(history_table_name(&table.name)),
            visible_content_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_history_storage_fields(table, "left."));
        let content = self.apply_maintained_view_filters(
            content,
            policy_shape,
            table,
            maintained_view_history_storage_field_names(table),
        )?;
        let content = content.project_fields(maintained_view_tagged_content_fields(
            table,
            "replacement_content",
            "",
            terminal_tables.clone(),
            output_hidden_param_types,
            available_hidden_param_types,
            "",
        ));

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(table)),
                policy_shape,
                table,
                current_row_fields(table),
            )?
            .project(
                std::iter::once("row_uuid".to_owned())
                    .chain(available_hidden_param_types.keys().cloned())
                    .collect::<Vec<_>>(),
            );
        let deletion_current_keys =
            GraphBuilder::table(register_global_current_table_name(&table.name)).project([
                "row_uuid",
                "tx_time",
                "tx_node_id",
            ]);
        let deletion = GraphBuilder::join(
            GraphBuilder::table(register_table_name(&table.name)),
            deletion_current_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_register_storage_fields("left."));
        let deletion = GraphBuilder::join(deletion, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "replacement_deletion",
                "left.",
                terminal_tables,
                output_hidden_param_types,
                available_hidden_param_types,
                "right.",
            ));

        Ok((content, deletion))
    }

    pub(crate) fn reachable_edge_constituent_current_graph(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
    ) -> Result<GraphBuilder, Error> {
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
        let edge_table = self.table(&reachable.edge_table)?;
        let access_table = self.table(&reachable.access_table)?;
        let reachable_graphs = self.lower_reachable_graph_parts(
            shape,
            &param_types,
            reachable,
            access_table,
            edge_table,
            DurabilityTier::Global,
            &BTreeMap::new(),
        )?;
        let edge_current = self.maintained_view_content_current_with_version(edge_table)?;
        let edge_keyed = GraphBuilder::join(
            edge_current
                .clone()
                .unwrap_nullable(query_field(&reachable.edge_member_column))
                .unwrap_nullable(query_field(&reachable.edge_parent_column)),
            reachable_graphs.edge_current.project(["row_uuid"]),
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            ["row_uuid", &query_field(&reachable.edge_member_column)]
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
        );
        let reachable_edge_keys = GraphBuilder::join(
            edge_keyed,
            reachable_graphs
                .closure
                .clone()
                .project(["team", "reachable_team"]),
            [query_field(&reachable.edge_member_column)],
            ["reachable_team".to_owned()],
        )
        .project_fields([
            ProjectField::renamed("left.row_uuid", "row_uuid"),
            ProjectField::renamed_resolved(2, reachable_graphs.seed_param.clone()),
        ])
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::nullable(
                reachable_graphs.seed_param.clone(),
                reachable_graphs.seed_param.clone(),
            ),
        ]);
        Ok(GraphBuilder::join(
            edge_current,
            reachable_edge_keys,
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields({
            let left_fields = maintained_view_version_fields(edge_table);
            let seed_idx = left_fields.len() + 1;
            left_fields
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                .chain(
                    param_types
                        .keys()
                        .map(|param| ProjectField::renamed_resolved(seed_idx, param.clone())),
                )
                .collect::<Vec<_>>()
        })
        .project_fields(
            maintained_view_version_fields(edge_table)
                .into_iter()
                .map(ProjectField::named)
                .chain(param_types.keys().cloned().map(ProjectField::named)),
        ))
    }

    pub(crate) fn reachable_access_constituent_current_graph(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
    ) -> Result<GraphBuilder, Error> {
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
        let edge_table = self.table(&reachable.edge_table)?;
        let access_table = self.table(&reachable.access_table)?;
        let reachable_graphs = self.lower_reachable_graph_parts(
            shape,
            &param_types,
            reachable,
            access_table,
            edge_table,
            DurabilityTier::Global,
            &BTreeMap::new(),
        )?;
        let access_current = self.maintained_view_content_current_with_version(access_table)?;
        let access_keyed = GraphBuilder::join(
            access_current
                .clone()
                .unwrap_nullable(query_field(&reachable.access_row_column))
                .unwrap_nullable(query_field(&reachable.access_team_column)),
            reachable_graphs.access_current.project(["row_uuid"]),
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            ["row_uuid", &query_field(&reachable.access_team_column)]
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
        );
        let reachable_access_keys = GraphBuilder::join(
            access_keyed,
            reachable_graphs.closure.project(["team", "reachable_team"]),
            [query_field(&reachable.access_team_column)],
            ["reachable_team".to_owned()],
        )
        .project_fields([
            ProjectField::renamed("left.row_uuid", "row_uuid"),
            ProjectField::renamed_resolved(2, reachable_graphs.seed_param.clone()),
        ])
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::nullable(
                reachable_graphs.seed_param.clone(),
                reachable_graphs.seed_param.clone(),
            ),
        ]);
        Ok(GraphBuilder::join(
            access_current,
            reachable_access_keys,
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields({
            let left_fields = maintained_view_version_fields(access_table);
            let seed_idx = left_fields.len() + 1;
            left_fields
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                .chain(
                    param_types
                        .keys()
                        .map(|param| ProjectField::renamed_resolved(seed_idx, param.clone())),
                )
                .collect::<Vec<_>>()
        })
        .project_fields(
            maintained_view_version_fields(access_table)
                .into_iter()
                .map(ProjectField::named)
                .chain(param_types.keys().cloned().map(ProjectField::named)),
        ))
    }

    pub(crate) fn reachable_policy_readable_version_tagged_graphs<'a>(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
        table: &TableSchema,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        self.ensure_reachable_constituent_table(reachable, table)?;
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&param_types, param_binding_mode);
        let readable_current = match table.name.as_str() {
            name if name == reachable.edge_table => {
                self.reachable_edge_constituent_current_graph(shape, reachable)?
            }
            name if name == reachable.access_table => {
                self.reachable_access_constituent_current_graph(shape, reachable)?
            }
            _ => unreachable!("checked above"),
        }
        .project(
            std::iter::once("row_uuid".to_owned())
                .chain(available_hidden_param_types.keys().cloned())
                .collect::<Vec<_>>(),
        );
        let content =
            GraphBuilder::join(
                GraphBuilder::table(history_table_name(&table.name)),
                readable_current.clone(),
                ["row_uuid"],
                ["row_uuid"],
            )
            .project_fields(
                maintained_view_version_fields(table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                    .chain(available_hidden_param_types.keys().map(|param| {
                        ProjectField::renamed(format!("right.{param}"), param.clone())
                    })),
            )
            .project_fields(maintained_view_tagged_content_fields(
                table,
                "version_content",
                "",
                terminal_tables.clone(),
                output_hidden_param_types,
                available_hidden_param_types,
                "",
            ));
        let deleted = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion = GraphBuilder::join(deleted, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "version_deletion",
                "left.",
                terminal_tables,
                output_hidden_param_types,
                available_hidden_param_types,
                "right.",
            ));
        Ok((content, deletion))
    }

    pub(crate) fn reachable_replacement_tagged_graphs<'a>(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
        table: &TableSchema,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        self.ensure_reachable_constituent_table(reachable, table)?;
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&param_types, param_binding_mode);
        let content = match table.name.as_str() {
            name if name == reachable.edge_table => {
                self.reachable_edge_constituent_current_graph(shape, reachable)?
            }
            name if name == reachable.access_table => {
                self.reachable_access_constituent_current_graph(shape, reachable)?
            }
            _ => unreachable!("checked above"),
        };
        let content = content.project_fields(maintained_view_tagged_content_fields(
            table,
            "replacement_content",
            "",
            terminal_tables.clone(),
            output_hidden_param_types,
            available_hidden_param_types,
            "",
        ));
        let readable_current = content.clone().project(
            std::iter::once("row_uuid".to_owned())
                .chain(available_hidden_param_types.keys().cloned())
                .collect::<Vec<_>>(),
        );
        let deletion_current_keys =
            GraphBuilder::table(register_global_current_table_name(&table.name)).project([
                "row_uuid",
                "tx_time",
                "tx_node_id",
            ]);
        let deletion = GraphBuilder::join(
            GraphBuilder::table(register_table_name(&table.name)),
            deletion_current_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_register_storage_fields("left."));
        let deletion = GraphBuilder::join(deletion, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "replacement_deletion",
                "left.",
                terminal_tables,
                output_hidden_param_types,
                available_hidden_param_types,
                "right.",
            ));
        Ok((content, deletion))
    }

    fn ensure_reachable_constituent_table(
        &self,
        reachable: &crate::query::ReachableVia,
        table: &TableSchema,
    ) -> Result<(), Error> {
        if table.name == reachable.edge_table || table.name == reachable.access_table {
            Ok(())
        } else {
            Err(Error::InvalidStoredValue(
                "reachable constituent table does not match reachable clause",
            ))
        }
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_result_current_graph(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, GraphBuilder), Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        self.ensure_maintained_view_query_slice(shape.query())?;
        let shape = maintained_view_bind_filter_literals(&shape, &binding, &self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        let table = self.table(&shape.query().table)?;
        let graph = self.maintained_view_content_current_with_version(table)?;
        let graph = self.apply_maintained_view_filters(
            graph,
            &shape,
            table,
            maintained_view_version_fields(table),
        )?;
        Ok((
            shape,
            binding,
            graph.project_fields(maintained_view_result_current_fields(table)),
        ))
    }

    #[cfg(test)]
    fn maintained_view_policy_readable_versions_graph(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
    ) -> Result<GraphBuilder, Error> {
        let content = self.apply_maintained_view_filters(
            GraphBuilder::table(history_table_name(&table.name)),
            policy_shape,
            table,
            maintained_view_version_fields(table),
        )?;
        let content = content.project_fields(maintained_view_policy_content_fields(table));

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(table)),
                policy_shape,
                table,
                current_row_fields(table),
            )?
            .project(["row_uuid"]);
        let deleted = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion = GraphBuilder::join(deleted, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_policy_deletion_fields(table));

        Ok(GraphBuilder::union([content, deletion]))
    }

    #[allow(dead_code)]
    fn maintained_view_content_current_with_version(
        &self,
        table: &TableSchema,
    ) -> Result<GraphBuilder, Error> {
        let history = GraphBuilder::table(history_table_name(&table.name)).project([
            "row_uuid",
            "tx_time",
            "tx_node_id",
            "schema_version",
            "parents",
        ]);
        Ok(GraphBuilder::join(
            visible_current_graph(table, DurabilityTier::Global),
            history,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field))
                .chain([
                    ProjectField::renamed("right.schema_version", "schema_version"),
                    ProjectField::renamed("right.parents", "parents"),
                ]),
        ))
    }

    fn apply_maintained_view_filters(
        &self,
        graph: GraphBuilder,
        shape: &ValidatedQuery,
        table: &TableSchema,
        output_fields: Vec<String>,
    ) -> Result<GraphBuilder, Error> {
        let param_types = maintained_view_hidden_param_column_types(&graph_param_types(
            shape,
            &self.catalogue.schema,
        )?);
        self.apply_lowered_query_clauses(
            graph,
            shape,
            table,
            &param_types,
            LoweredQueryClauseOptions {
                tier: DurabilityTier::Global,
                output_fields,
                keep_binding_params_in_output: true,
                binding_source_shape: maintained_view_binding_source_shape(shape),
                source_overrides: BTreeMap::new(),
                table_overrides: BTreeMap::new(),
            },
        )
    }

    #[cfg(test)]
    fn materialize_maintained_view_graph(
        &mut self,
        graph: GraphBuilder,
        shape: &ValidatedQuery,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        record_maintained_view_materialize_call();
        if !shape.params().is_empty() {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view materializer expects bound filter literals",
            ));
        }
        self.database.query_graph(graph).map_err(Error::Groove)
    }

    fn ensure_maintained_view_query_slice(&self, query: &crate::query::Query) -> Result<(), Error> {
        if !maintained_view_query_slice_supported(query) {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view subscription does not support this query shape",
            ));
        }
        Ok(())
    }

    fn lower_reachable_graph(
        &self,
        shape: &ValidatedQuery,
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        reachable: &crate::query::ReachableVia,
        access_table: &TableSchema,
        edge_table: &TableSchema,
        tier: DurabilityTier,
        source_overrides: &BTreeMap<String, GraphBuilder>,
        binding_source_shape: &str,
    ) -> Result<GraphBuilder, Error> {
        let reachable_graphs = self.lower_reachable_graph_parts_with_binding_source(
            shape,
            param_types,
            reachable,
            access_table,
            edge_table,
            tier,
            source_overrides,
            binding_source_shape,
        )?;
        let seed_param = reachable_graphs.seed_param.clone();
        let seed_field_idx = current_row_fields(access_table).len();
        let graph = GraphBuilder::join(
            reachable_graphs.access_current,
            reachable_graphs.closure,
            [query_field(&reachable.access_team_column)],
            ["reachable_team".to_owned()],
        )
        .project_fields([
            ProjectField::renamed(
                format!("left.{}", query_field(&reachable.access_row_column)),
                "access_row_uuid",
            ),
            ProjectField::renamed_resolved(seed_field_idx, seed_param.clone()),
        ]);
        if param_types
            .get(&seed_param)
            .is_some_and(|column_type| matches!(column_type.value_type(), ValueType::Nullable(_)))
        {
            Ok(graph.project_fields([
                ProjectField::named("access_row_uuid"),
                ProjectField::nullable(seed_param.clone(), seed_param),
            ]))
        } else {
            Ok(graph)
        }
    }

    pub(crate) fn lower_reachable_graph_parts(
        &self,
        shape: &ValidatedQuery,
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        reachable: &crate::query::ReachableVia,
        access_table: &TableSchema,
        edge_table: &TableSchema,
        tier: DurabilityTier,
        source_overrides: &BTreeMap<String, GraphBuilder>,
    ) -> Result<ReachableGraphs, Error> {
        self.lower_reachable_graph_parts_with_binding_source(
            shape,
            param_types,
            reachable,
            access_table,
            edge_table,
            tier,
            source_overrides,
            &query_binding_source_shape(shape),
        )
    }

    fn lower_reachable_graph_parts_with_binding_source(
        &self,
        _shape: &ValidatedQuery,
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        reachable: &crate::query::ReachableVia,
        access_table: &TableSchema,
        edge_table: &TableSchema,
        tier: DurabilityTier,
        source_overrides: &BTreeMap<String, GraphBuilder>,
        binding_source_shape: &str,
    ) -> Result<ReachableGraphs, Error> {
        let team_desc = RecordDescriptor::new([
            ("team".to_owned(), groove::records::ValueType::Uuid),
            (
                "reachable_team".to_owned(),
                groove::records::ValueType::Uuid,
            ),
        ]);
        let seed_param = reachable_seed_param(reachable)?;
        let seed = if let Some(seed) = &reachable.seed {
            let seed_table = self.table(&seed.table)?;
            let mut seed_graph = current_source_graph(&seed_table, tier, source_overrides)
                .unwrap_nullable(query_field(&seed.team_column));
            seed_graph = apply_filters_with_predicate_params(
                seed_graph,
                &seed_table,
                param_types,
                &seed.filters,
                current_row_fields(&seed_table),
                true,
                binding_source_shape,
            )?;
            seed_graph.project_fields([
                ProjectField::renamed(query_field(&seed.team_column), "team"),
                ProjectField::renamed(query_field(&seed.team_column), "reachable_team"),
            ])
        } else {
            match &reachable.from {
                Operand::Param(param) => {
                    let mut seed =
                        GraphBuilder::binding_source(
                            binding_source_shape.to_owned(),
                            RecordDescriptor::new(param_types.iter().map(|(name, column_type)| {
                                (name.clone(), column_type.value_type())
                            })),
                        );
                    if param_types.get(param).is_some_and(|column_type| {
                        matches!(column_type.value_type(), ValueType::Nullable(_))
                    }) {
                        seed = seed.unwrap_nullable(param.clone());
                    }
                    seed.project_fields([
                        ProjectField::renamed(param.clone(), "team"),
                        ProjectField::renamed(param.clone(), "reachable_team"),
                    ])
                }
                Operand::Literal(Value::Uuid(seed)) => GraphBuilder::values(
                    team_desc.clone(),
                    [[Value::Uuid(*seed), Value::Uuid(*seed)]],
                )?,
                Operand::Claim(_) => {
                    return Err(Error::InvalidStoredValue(
                        "query claims must be rewritten to params before lowering",
                    ));
                }
                Operand::Column(_) | Operand::Literal(_) => {
                    return Err(Error::InvalidStoredValue(
                        "reachable_via currently supports uuid parameter/claim/literal seeds only",
                    ));
                }
            }
        };
        let frontier = GraphBuilder::frontier_source("reachable_frontier", team_desc);
        let mut edge_graph = current_source_graph(edge_table, tier, source_overrides)
            .unwrap_nullable(query_field(&reachable.edge_member_column))
            .unwrap_nullable(query_field(&reachable.edge_parent_column));
        edge_graph = apply_filters_with_predicate_params(
            edge_graph,
            edge_table,
            param_types,
            &reachable.edge_filters,
            current_row_fields(edge_table),
            true,
            binding_source_shape,
        )?;
        let step = GraphBuilder::join(
            frontier,
            edge_graph.clone(),
            ["reachable_team".to_owned()],
            [query_field(&reachable.edge_member_column)],
        )
        .project_fields([
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed(
                format!("right.{}", query_field(&reachable.edge_parent_column)),
                "reachable_team",
            ),
        ]);
        let closure =
            GraphBuilder::recursive(seed, step, "reachable_frontier", reachable.max_depth.max(1));
        let mut access_graph = current_source_graph(access_table, tier, source_overrides)
            .unwrap_nullable(query_field(&reachable.access_row_column))
            .unwrap_nullable(query_field(&reachable.access_team_column));
        access_graph = apply_filters_with_predicate_params(
            access_graph,
            access_table,
            param_types,
            &reachable.access_filters,
            current_row_fields(access_table),
            true,
            binding_source_shape,
        )?;
        Ok(ReachableGraphs {
            closure,
            edge_current: edge_graph,
            access_current: access_graph,
            seed_param,
        })
    }

    fn policy_composed_shape_binding(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding), Error> {
        if identity == AuthorId::SYSTEM {
            return Ok((shape.clone(), binding.clone()));
        }
        let Some(policy) = self.table(&shape.query().table)?.read_policy.clone() else {
            return Ok((shape.clone(), binding.clone()));
        };
        let claims = self.session_claims.get(&identity);
        let mut query = shape.query().clone();
        let base_filters = query.filters.clone();
        let base_joins = query.joins.clone();
        let base_reachable = query.reachable.clone();
        query.filters.extend(
            policy
                .filters
                .into_iter()
                .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims)),
        );
        query.joins.extend(
            policy
                .joins
                .into_iter()
                .map(|join| rewrite_claim_join_for_binding(join, claims)),
        );
        query
            .reachable
            .extend(policy.reachable.into_iter().map(|mut reachable| {
                reachable.from = rewrite_claim_operand_for_binding(reachable.from);
                reachable.access_filters = reachable
                    .access_filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect();
                reachable.edge_filters = reachable
                    .edge_filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect();
                if let Some(seed) = &mut reachable.seed {
                    seed.filters = std::mem::take(&mut seed.filters)
                        .into_iter()
                        .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                        .collect();
                }
                reachable
            }));
        query.includes.extend(policy.includes);
        query
            .policy_branches
            .extend(policy.policy_branches.into_iter().map(|branch| {
                compose_policy_branch(branch, &base_filters, &base_joins, &base_reachable, claims)
            }));
        let composed = query.validate(&self.catalogue.schema)?;
        let mut values = binding.values().clone();
        // Claim bindings are derived from the authenticated peer identity on
        // the server. Clients never supply these values, so they cannot widen
        // their own subscription by choosing a different claim binding.
        insert_claim_bindings(&mut values, composed.params(), identity, claims);
        let binding = composed.bind(values)?;
        Ok((composed, binding))
    }

    pub(crate) fn permission_scope_shape_binding(
        &self,
        table: &str,
        writer: AuthorId,
    ) -> Result<Option<(ValidatedQuery, Binding)>, Error> {
        let Some(policy) = self.table(table)?.write_policies.any() else {
            return Ok(None);
        };
        let claims = self.session_claims.get(&writer);
        let mut query = policy;
        query.filters = query
            .filters
            .into_iter()
            .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
            .collect();
        query.joins = query
            .joins
            .into_iter()
            .map(|join| rewrite_claim_join_for_binding(join, claims))
            .collect();
        query.reachable = query
            .reachable
            .into_iter()
            .map(|mut reachable| {
                reachable.from = rewrite_claim_operand_for_binding(reachable.from);
                reachable.access_filters = reachable
                    .access_filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect();
                reachable.edge_filters = reachable
                    .edge_filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect();
                if let Some(seed) = &mut reachable.seed {
                    seed.filters = std::mem::take(&mut seed.filters)
                        .into_iter()
                        .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                        .collect();
                }
                reachable
            })
            .collect();
        let shape = query.validate(&self.catalogue.schema)?;
        let mut values = BTreeMap::new();
        insert_claim_bindings(&mut values, shape.params(), writer, claims);
        let binding = shape.bind(values)?;
        Ok(Some((shape, binding)))
    }
}

fn maintained_view_query_slice_supported(query: &crate::query::Query) -> bool {
    query.aggregate.is_none() && maintained_view_window_supported(query)
}

fn maintained_view_window_supported(query: &crate::query::Query) -> bool {
    if query.order_by.is_empty() {
        query.offset == 0 && (query.limit.is_none() || query.limit == Some(1))
    } else {
        true
    }
}

fn apply_maintained_view_result_limit(
    graph: GraphBuilder,
    query: &crate::query::Query,
) -> GraphBuilder {
    if !query.order_by.is_empty() {
        let order_cols = query.order_by.iter().map(|order| {
            let field = query_field(&order.column);
            match order.direction {
                OrderDirection::Asc => TopByOrder::asc(field),
                OrderDirection::Desc => TopByOrder::desc(field),
            }
        });
        return GraphBuilder::top_by(
            graph,
            std::iter::empty::<&str>(),
            order_cols,
            ["row_uuid"],
            query.offset,
            query.limit.unwrap_or(usize::MAX),
        );
    }

    if query.limit == Some(1) {
        GraphBuilder::arg_min_by(graph, std::iter::empty::<&str>(), ["row_uuid"])
    } else {
        graph
    }
}

impl<S> HistoricalRead<'_, S>
where
    S: OrderedKvStorage,
{
    /// Read a validated query at this handle's historical settle position.
    ///
    /// Partial nodes return [`Error::HistoricalReadRequiresServer`] rather than
    /// answering from incomplete local history. A later protocol slice wires
    /// that error to a server-evaluated one-shot.
    pub fn read(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        if !self.node.is_history_complete_for(shape, self.position) {
            return Err(Error::HistoricalReadRequiresServer);
        }
        self.node.query_rows_at(shape, binding, self.position)
    }
}

fn compose_policy_branch(
    branch: PolicyBranch,
    base_filters: &[Predicate],
    base_joins: &[JoinVia],
    base_reachable: &[crate::query::ReachableVia],
    claims: Option<&BTreeMap<String, Value>>,
) -> PolicyBranch {
    let mut filters = base_filters.to_vec();
    filters.extend(
        branch
            .filters
            .into_iter()
            .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims)),
    );
    let mut joins = base_joins.to_vec();
    joins.extend(
        branch
            .joins
            .into_iter()
            .map(|join| rewrite_claim_join_for_binding(join, claims)),
    );
    let mut reachable = base_reachable.to_vec();
    reachable.extend(
        branch
            .reachable
            .into_iter()
            .map(|reachable| rewrite_claim_reachable_for_binding(reachable, claims)),
    );
    PolicyBranch {
        filters,
        joins,
        reachable,
    }
}

fn rewrite_claim_join_for_binding(
    join: JoinVia,
    claims: Option<&BTreeMap<String, Value>>,
) -> JoinVia {
    JoinVia {
        table: join.table,
        on_column: join.on_column,
        target: join.target,
        source_column: join.source_column,
        source_lookup: join.source_lookup,
        correlated_filters: join.correlated_filters,
        filters: join
            .filters
            .into_iter()
            .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
            .collect(),
        nested_joins: join
            .nested_joins
            .into_iter()
            .map(|join| rewrite_claim_join_for_binding(join, claims))
            .collect(),
    }
}

fn rewrite_claim_reachable_for_binding(
    mut reachable: crate::query::ReachableVia,
    claims: Option<&BTreeMap<String, Value>>,
) -> crate::query::ReachableVia {
    reachable.from = rewrite_claim_operand_for_binding(reachable.from);
    reachable.access_filters = reachable
        .access_filters
        .into_iter()
        .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
        .collect();
    reachable.edge_filters = reachable
        .edge_filters
        .into_iter()
        .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
        .collect();
    if let Some(seed) = &mut reachable.seed {
        seed.filters = std::mem::take(&mut seed.filters)
            .into_iter()
            .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
            .collect();
    }
    reachable
}

fn rewrite_claim_predicate_for_binding(
    predicate: Predicate,
    claims: Option<&BTreeMap<String, Value>>,
) -> Predicate {
    match predicate {
        Predicate::All(predicates) => Predicate::All(
            predicates
                .into_iter()
                .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                .collect(),
        ),
        Predicate::Any(predicates) => Predicate::Any(
            predicates
                .into_iter()
                .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                .collect(),
        ),
        Predicate::Not(predicate) if predicate_contains_unbound_claim(&predicate, claims) => {
            false_predicate()
        }
        Predicate::Not(predicate) => Predicate::Not(Box::new(rewrite_claim_predicate_for_binding(
            *predicate, claims,
        ))),
        Predicate::Eq(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Eq(left, right) => Predicate::Eq(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::Ne(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Ne(left, right) => Predicate::Ne(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::In(left, values)
            if operands_contain_unbound_claim(
                std::iter::once(&left)
                    .chain(values.iter())
                    .collect::<Vec<_>>(),
                claims,
            ) =>
        {
            false_predicate()
        }
        Predicate::In(left, values) => Predicate::In(
            rewrite_claim_operand_for_binding(left),
            values
                .into_iter()
                .map(rewrite_claim_operand_for_binding)
                .collect(),
        ),
        Predicate::Gt(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Gt(left, right) => Predicate::Gt(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::Gte(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Gte(left, right) => Predicate::Gte(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::Lt(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Lt(left, right) => Predicate::Lt(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::Lte(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Lte(left, right) => Predicate::Lte(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::Contains(left, right)
            if operands_contain_unbound_claim([&left, &right], claims) =>
        {
            false_predicate()
        }
        Predicate::Contains(left, right) => Predicate::Contains(
            rewrite_claim_operand_for_binding(left),
            rewrite_claim_operand_for_binding(right),
        ),
        Predicate::IsNull(operand) if operand_contains_unbound_claim(&operand, claims) => {
            false_predicate()
        }
        Predicate::IsNull(operand) => Predicate::IsNull(rewrite_claim_operand_for_binding(operand)),
    }
}

fn rewrite_claim_operand_for_binding(operand: Operand) -> Operand {
    match operand {
        Operand::Claim(name) => Operand::Param(claim_param_name(&name)),
        other => other,
    }
}

fn claim_param_name(name: &str) -> String {
    format!("{CLAIM_PARAM_PREFIX}{name}")
}

fn false_predicate() -> Predicate {
    Predicate::Eq(
        Operand::Literal(Value::Bool(true)),
        Operand::Literal(Value::Bool(false)),
    )
}

fn is_false_predicate(predicate: &Predicate) -> bool {
    matches!(
        predicate,
        Predicate::Eq(
            Operand::Literal(Value::Bool(true)),
            Operand::Literal(Value::Bool(false))
        )
    )
}

fn predicate_contains_unbound_claim(
    predicate: &Predicate,
    claims: Option<&BTreeMap<String, Value>>,
) -> bool {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => predicates
            .iter()
            .any(|predicate| predicate_contains_unbound_claim(predicate, claims)),
        Predicate::Not(predicate) => predicate_contains_unbound_claim(predicate, claims),
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => operands_contain_unbound_claim([left, right], claims),
        Predicate::In(left, values) => {
            operand_contains_unbound_claim(left, claims)
                || values
                    .iter()
                    .any(|operand| operand_contains_unbound_claim(operand, claims))
        }
        Predicate::IsNull(operand) => operand_contains_unbound_claim(operand, claims),
    }
}

fn operands_contain_unbound_claim<'a>(
    operands: impl IntoIterator<Item = &'a Operand>,
    claims: Option<&BTreeMap<String, Value>>,
) -> bool {
    operands
        .into_iter()
        .any(|operand| operand_contains_unbound_claim(operand, claims))
}

fn operand_contains_unbound_claim(
    operand: &Operand,
    claims: Option<&BTreeMap<String, Value>>,
) -> bool {
    matches!(operand, Operand::Claim(name) if name != "sub" && name != "user_id" && name != "isAdmin" && !claims.is_some_and(|claims| claims.contains_key(name)))
}

fn insert_claim_bindings(
    values: &mut BTreeMap<String, Value>,
    params: &BTreeMap<String, ColumnType>,
    identity: AuthorId,
    claims: Option<&BTreeMap<String, Value>>,
) {
    let sub = claim_param_name("sub");
    if params.contains_key(&sub) {
        values.insert(
            sub.clone(),
            claim_binding_value(
                params.get(&sub),
                claims
                    .and_then(|claims| claims.get("sub"))
                    .cloned()
                    .unwrap_or(Value::Uuid(identity.0)),
            ),
        );
    }
    let user_id = claim_param_name("user_id");
    if params.contains_key(&user_id) {
        values.insert(
            user_id.clone(),
            claim_binding_value(
                params.get(&user_id),
                claims
                    .and_then(|claims| claims.get("user_id"))
                    .cloned()
                    .unwrap_or_else(|| Value::String(identity.0.to_string())),
            ),
        );
    }
    let is_admin = claim_param_name("isAdmin");
    if params.contains_key(&is_admin) {
        values.insert(
            is_admin.clone(),
            claim_binding_value(
                params.get(&is_admin),
                claims
                    .and_then(|claims| claims.get("isAdmin"))
                    .cloned()
                    .unwrap_or(Value::Bool(false)),
            ),
        );
    }
    if let Some(claims) = claims {
        for (name, value) in claims {
            let param = claim_param_name(name);
            if params.contains_key(&param) && param != sub && param != user_id && param != is_admin
            {
                values.insert(
                    param.clone(),
                    claim_binding_value(params.get(&param), value.clone()),
                );
            }
        }
    }
}

fn claim_binding_value(column_type: Option<&ColumnType>, value: Value) -> Value {
    match column_type {
        Some(ColumnType::Uuid) => match value {
            Value::String(value) => uuid::Uuid::parse_str(&value)
                .map(Value::Uuid)
                .unwrap_or(Value::String(value)),
            other => other,
        },
        Some(ColumnType::Array(member_type)) => match value {
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|value| claim_binding_value(Some(member_type.as_ref()), value))
                    .collect(),
            ),
            other => other,
        },
        Some(ColumnType::Nullable(_)) if !matches!(value, Value::Nullable(_)) => {
            let inner = match column_type {
                Some(ColumnType::Nullable(inner)) => Some(inner.as_ref()),
                _ => None,
            };
            Value::Nullable(Some(Box::new(claim_binding_value(inner, value))))
        }
        _ => value,
    }
}

struct PreparedIncludeModes {
    paths: Vec<PreparedIncludePath>,
    rows_by_table: BTreeMap<String, BTreeMap<RowUuid, CurrentRow>>,
}

impl PreparedIncludeModes {
    fn row_satisfies(&self, row: &CurrentRow) -> bool {
        self.paths.iter().all(|path| path.resolves(row, self))
    }

    fn row(&self, table: &str, row_uuid: RowUuid) -> Option<&CurrentRow> {
        self.rows_by_table.get(table)?.get(&row_uuid)
    }
}

struct PreparedIncludePath {
    segments: Vec<PreparedIncludeSegment>,
}

impl PreparedIncludePath {
    fn resolves(&self, row: &CurrentRow, modes: &PreparedIncludeModes) -> bool {
        let mut current_rows = vec![row];
        for segment in &self.segments {
            let mut next_rows = Vec::new();
            for current_row in current_rows {
                match current_row.cell_at(segment.column_position) {
                    Some(Value::Uuid(target_uuid)) => {
                        let Some(next_row) = modes.row(&segment.target_table, RowUuid(target_uuid))
                        else {
                            return false;
                        };
                        next_rows.push(next_row);
                    }
                    Some(Value::Array(targets)) => {
                        for target in targets {
                            let Value::Uuid(target_uuid) = target else {
                                return false;
                            };
                            let Some(next_row) =
                                modes.row(&segment.target_table, RowUuid(target_uuid))
                            else {
                                return false;
                            };
                            next_rows.push(next_row);
                        }
                    }
                    Some(Value::Nullable(None)) | None => {}
                    Some(Value::Nullable(Some(target))) => {
                        let Value::Uuid(target_uuid) = target.as_ref() else {
                            return false;
                        };
                        let Some(next_row) =
                            modes.row(&segment.target_table, RowUuid(*target_uuid))
                        else {
                            return false;
                        };
                        next_rows.push(next_row);
                    }
                    Some(_) => return false,
                }
            }
            current_rows = next_rows;
            if current_rows.is_empty() {
                return true;
            }
        }
        true
    }
}

struct PreparedIncludeSegment {
    column_position: usize,
    target_table: String,
}

fn apply_query_filters(
    graph: GraphBuilder,
    table: &TableSchema,
    predicates: &[Predicate],
) -> Result<GraphBuilder, Error> {
    let mut residual = Vec::new();
    for predicate in predicates {
        match lower_maintained_residual_predicate(table, predicate)? {
            LoweredMaintainedPredicate::AlwaysTrue => {}
            LoweredMaintainedPredicate::AlwaysFalse => {
                return Ok(GraphBuilder::anti_join(
                    graph.clone(),
                    graph,
                    ["row_uuid"],
                    ["row_uuid"],
                ));
            }
            LoweredMaintainedPredicate::Residual(predicate) => residual.push(predicate),
        }
    }
    let _ = table;
    if residual.is_empty() {
        Ok(graph)
    } else {
        Ok(graph.filter(PredicateExpr::And(residual).canonicalize()))
    }
}

enum LoweredMaintainedPredicate {
    AlwaysTrue,
    AlwaysFalse,
    Residual(PredicateExpr),
}

fn lower_maintained_residual_predicate(
    table: &TableSchema,
    predicate: &Predicate,
) -> Result<LoweredMaintainedPredicate, Error> {
    match predicate {
        predicate if is_false_predicate(predicate) => Ok(LoweredMaintainedPredicate::AlwaysFalse),
        Predicate::All(predicates) => {
            let mut residual = Vec::new();
            for predicate in predicates {
                match lower_maintained_residual_predicate(table, predicate)? {
                    LoweredMaintainedPredicate::AlwaysTrue => {}
                    LoweredMaintainedPredicate::AlwaysFalse => {
                        return Ok(LoweredMaintainedPredicate::AlwaysFalse);
                    }
                    LoweredMaintainedPredicate::Residual(predicate) => residual.push(predicate),
                }
            }
            match residual.len() {
                0 => Ok(LoweredMaintainedPredicate::AlwaysTrue),
                1 => Ok(LoweredMaintainedPredicate::Residual(residual.remove(0))),
                _ => Ok(LoweredMaintainedPredicate::Residual(
                    PredicateExpr::And(residual).canonicalize(),
                )),
            }
        }
        Predicate::Any(predicates) if predicates.len() == 1 => {
            lower_maintained_residual_predicate(table, &predicates[0])
        }
        Predicate::Any(predicates) => {
            let mut residual = Vec::new();
            for predicate in predicates {
                match lower_maintained_residual_predicate(table, predicate)? {
                    LoweredMaintainedPredicate::AlwaysTrue => {
                        return Ok(LoweredMaintainedPredicate::AlwaysTrue);
                    }
                    LoweredMaintainedPredicate::AlwaysFalse => {}
                    LoweredMaintainedPredicate::Residual(predicate) => residual.push(predicate),
                }
            }
            match residual.len() {
                0 => Ok(LoweredMaintainedPredicate::AlwaysFalse),
                1 => Ok(LoweredMaintainedPredicate::Residual(residual.remove(0))),
                _ => Ok(LoweredMaintainedPredicate::Residual(
                    PredicateExpr::Or(residual).canonicalize(),
                )),
            }
        }
        Predicate::In(Operand::Column(column), values) => {
            if values.is_empty() {
                return Ok(LoweredMaintainedPredicate::AlwaysFalse);
            }
            let mut residual = Vec::new();
            let column_type = non_null_query_column_type(table_column_type(table, column)?);
            for value in values {
                let Operand::Literal(value) = value else {
                    return Err(Error::InvalidStoredValue(
                        "unsupported query predicate shape",
                    ));
                };
                residual.push(match (column_type, value) {
                    (groove::schema::ColumnType::Array(_), Value::Array(_)) => PredicateExpr::eq(
                        query_field(column),
                        nullable_cell_value(table, column, value.clone())?,
                    ),
                    (groove::schema::ColumnType::Array(_), _) => PredicateExpr::Contains {
                        field: query_field(column),
                        value: nullable_cell_value(table, column, value.clone())?.into(),
                    },
                    _ => PredicateExpr::eq(
                        query_field(column),
                        nullable_cell_value(table, column, value.clone())?,
                    ),
                });
            }
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::Or(residual).canonicalize(),
            ))
        }
        Predicate::Eq(Operand::Column(column), Operand::Literal(value))
        | Predicate::Eq(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::eq(
                query_field(column),
                nullable_cell_value(table, column, value.clone())?,
            )))
        }
        Predicate::Ne(Operand::Column(column), Operand::Literal(value))
        | Predicate::Ne(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::Neq {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Gt(Operand::Column(column), Operand::Literal(value)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::Gt {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Gt(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::Lt {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Gte(Operand::Column(column), Operand::Literal(value)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::GtEq {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Gte(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::LtEq {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Lt(Operand::Column(column), Operand::Literal(value)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::Lt {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Lt(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::Gt {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Lte(Operand::Column(column), Operand::Literal(value)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::LtEq {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Lte(Operand::Literal(value), Operand::Column(column)) => {
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::GtEq {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }))
        }
        Predicate::Contains(Operand::Column(column), Operand::Literal(value)) => Ok(
            LoweredMaintainedPredicate::Residual(PredicateExpr::Contains {
                field: query_field(column),
                value: nullable_cell_value(table, column, value.clone())?.into(),
            }),
        ),
        Predicate::IsNull(Operand::Column(column)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::IsNull {
                    field: query_field(column),
                },
            ))
        }
        Predicate::Not(predicate)
            if matches!(
                predicate.as_ref(),
                Predicate::Ne(Operand::Column(_), Operand::Literal(_))
                    | Predicate::Ne(Operand::Literal(_), Operand::Column(_))
            ) =>
        {
            let (column, value) = match predicate.as_ref() {
                Predicate::Ne(Operand::Column(column), Operand::Literal(value))
                | Predicate::Ne(Operand::Literal(value), Operand::Column(column)) => {
                    (column, value)
                }
                _ => unreachable!("matches! guard ensures this shape"),
            };
            Ok(LoweredMaintainedPredicate::Residual(PredicateExpr::eq(
                query_field(column),
                nullable_cell_value(table, column, value.clone())?,
            )))
        }
        Predicate::Not(predicate)
            if matches!(predicate.as_ref(), Predicate::IsNull(Operand::Column(_))) =>
        {
            let Predicate::IsNull(Operand::Column(column)) = predicate.as_ref() else {
                unreachable!("matches! guard ensures this shape");
            };
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::IsNotNull {
                    field: query_field(column),
                },
            ))
        }
        Predicate::Eq(Operand::Column(column), Operand::Param(param))
        | Predicate::Eq(Operand::Param(param), Operand::Column(column)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::EqField {
                    field: query_field(column),
                    value_field: param.clone(),
                },
            ))
        }
        Predicate::Contains(Operand::Column(column), Operand::Param(param)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::ContainsField {
                    field: query_field(column),
                    needle_field: param.clone(),
                },
            ))
        }
        Predicate::Contains(Operand::Param(param), Operand::Column(column)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::ContainsField {
                    field: param.clone(),
                    needle_field: query_field(column),
                },
            ))
        }
        Predicate::Ne(Operand::Column(column), Operand::Param(param))
        | Predicate::Ne(Operand::Param(param), Operand::Column(column)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::Residual(
                PredicateExpr::NeqField {
                    field: query_field(column),
                    value_field: param.clone(),
                },
            ))
        }
        _ => Err(Error::InvalidStoredValue(
            "unsupported query predicate shape",
        )),
    }
}

pub(super) fn binding_for_shape(
    shape: &ValidatedQuery,
    binding: &Binding,
) -> Result<Binding, Error> {
    let values = shape
        .params()
        .keys()
        .map(|name| {
            binding
                .values()
                .get(name)
                .cloned()
                .map(|value| (name.clone(), value))
                .ok_or_else(|| QueryError::MissingParam(name.clone()))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(shape.bind(values)?)
}

fn attach_output_binding_params(
    graph: GraphBuilder,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    options: &LoweredQueryClauseOptions,
) -> Result<GraphBuilder, Error> {
    if !options.keep_binding_params_in_output || param_types.is_empty() {
        return Ok(graph);
    }
    attach_params_to_graph(
        graph,
        param_types,
        options.output_fields.clone(),
        true,
        &options.binding_source_shape,
    )
}

fn apply_filters_with_predicate_params(
    graph: GraphBuilder,
    table: &TableSchema,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    predicates: &[Predicate],
    output_fields: Vec<String>,
    keep_params: bool,
    binding_source_shape: &str,
) -> Result<GraphBuilder, Error> {
    let predicate_params = predicate_params(predicates);
    if predicate_params.is_empty() {
        return apply_query_filters(graph, table, predicates);
    }
    let param_types = param_types
        .iter()
        .filter(|(param, _)| predicate_params.contains(*param))
        .map(|(param, column_type)| (param.clone(), column_type.clone()))
        .collect::<BTreeMap<_, _>>();
    let graph = attach_params_to_graph(
        graph,
        &param_types,
        output_fields.clone(),
        keep_params,
        binding_source_shape,
    )?;
    let graph = apply_query_filters(graph, table, predicates)?;
    Ok(
        graph.project_fields(output_fields.into_iter().map(ProjectField::named).chain(
            if keep_params {
                param_types
                    .keys()
                    .cloned()
                    .map(ProjectField::named)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            },
        )),
    )
}

fn attach_params_to_graph(
    graph: GraphBuilder,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    output_fields: Vec<String>,
    keep_params: bool,
    binding_source_shape: &str,
) -> Result<GraphBuilder, Error> {
    if param_types.is_empty() {
        return Ok(graph);
    }
    const PARAM_ROUTING_JOIN: &str = "__jazz_param_binding_join";
    let graph = graph.project_fields(
        output_fields
            .iter()
            .cloned()
            .map(ProjectField::named)
            .chain([ProjectField::literal(PARAM_ROUTING_JOIN, Value::U8(0))]),
    );
    let binding = GraphBuilder::binding_source(
        binding_source_shape.to_owned(),
        RecordDescriptor::new(
            param_types
                .iter()
                .map(|(name, column_type)| (name.clone(), column_type.value_type())),
        ),
    )
    .project_fields(
        param_types
            .keys()
            .cloned()
            .map(ProjectField::named)
            .chain([ProjectField::literal(PARAM_ROUTING_JOIN, Value::U8(0))]),
    );
    Ok(
        GraphBuilder::join(binding, graph, [PARAM_ROUTING_JOIN], [PARAM_ROUTING_JOIN])
            .project_fields(
                output_fields
                    .iter()
                    .cloned()
                    .map(|field| ProjectField::renamed(format!("right.{field}"), field))
                    .chain(if keep_params {
                        param_types
                            .keys()
                            .cloned()
                            .map(|param| ProjectField::renamed(format!("left.{param}"), param))
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }),
            ),
    )
}

fn reachable_seed_param(reachable: &crate::query::ReachableVia) -> Result<String, Error> {
    if let Some(seed) = &reachable.seed {
        return Ok(predicate_params(&seed.filters)
            .into_iter()
            .next()
            .unwrap_or_else(|| "__reachable_seed".to_owned()));
    }
    match &reachable.from {
        Operand::Param(param) => Ok(param.clone()),
        Operand::Literal(Value::Uuid(_)) => Ok("__reachable_seed".to_owned()),
        Operand::Claim(_) => Err(Error::InvalidStoredValue(
            "query claims must be rewritten to params before lowering",
        )),
        Operand::Column(_) | Operand::Literal(_) => Err(Error::InvalidStoredValue(
            "reachable_via currently supports uuid parameter/claim/literal seeds only",
        )),
    }
}

#[cfg(test)]
fn maintained_view_bind_filter_literals(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
) -> Result<ValidatedQuery, Error> {
    maintained_view_bind_filter_literals_with_mode(
        shape,
        binding,
        schema,
        ParamBindingMode::InlineAllReachableSeeds,
    )
}

#[derive(Clone, Copy)]
pub(crate) enum ParamBindingMode {
    InlineAllReachableSeeds,
    RetainAllParams,
}

fn hidden_maintained_view_param_types(
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    mode: ParamBindingMode,
) -> &BTreeMap<String, groove::schema::ColumnType> {
    static EMPTY: std::sync::LazyLock<BTreeMap<String, groove::schema::ColumnType>> =
        std::sync::LazyLock::new(BTreeMap::new);
    match mode {
        ParamBindingMode::InlineAllReachableSeeds => &EMPTY,
        ParamBindingMode::RetainAllParams => param_types,
    }
}

fn maintained_view_bind_filter_literals_with_mode(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
    mode: ParamBindingMode,
) -> Result<ValidatedQuery, Error> {
    let mut query = shape.query().clone();
    query.filters = query
        .filters
        .into_iter()
        .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
        .collect::<Result<Vec<_>, _>>()?;
    query.joins = query
        .joins
        .into_iter()
        .map(|join| bind_join_filter_literals(join, binding, mode))
        .collect::<Result<Vec<_>, Error>>()?;
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            if should_inline_reachable_seed(&reachable.from, mode) {
                reachable.from = maintained_view_bind_operand(reachable.from, binding, mode)?;
            }
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                .collect::<Result<Vec<_>, _>>()?;
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                .collect::<Result<Vec<_>, _>>()?;
            bind_reachable_seed_filters(&mut reachable, binding, mode)?;
            Ok(reachable)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.policy_branches = query
        .policy_branches
        .into_iter()
        .map(|mut branch| {
            branch.filters = branch
                .filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                .collect::<Result<Vec<_>, _>>()?;
            branch.joins = branch
                .joins
                .into_iter()
                .map(|join| bind_join_filter_literals(join, binding, mode))
                .collect::<Result<Vec<_>, Error>>()?;
            branch.reachable = branch
                .reachable
                .into_iter()
                .map(|mut reachable| {
                    if should_inline_reachable_seed(&reachable.from, mode) {
                        reachable.from =
                            maintained_view_bind_operand(reachable.from, binding, mode)?;
                    }
                    reachable.access_filters = reachable
                        .access_filters
                        .into_iter()
                        .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                        .collect::<Result<Vec<_>, _>>()?;
                    reachable.edge_filters = reachable
                        .edge_filters
                        .into_iter()
                        .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                        .collect::<Result<Vec<_>, _>>()?;
                    bind_reachable_seed_filters(&mut reachable, binding, mode)?;
                    Ok(reachable)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(branch)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let rebound = query.validate(schema)?;
    if rebound.schema_version() != shape.schema_version() {
        return Err(Error::InvalidStoredValue(
            "maintained subscription view rebound query schema changed",
        ));
    }
    Ok(rebound)
}

fn inline_snapshot_bind_filter_literals(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
) -> Result<ValidatedQuery, Error> {
    let mut query = shape.query().clone();
    query.filters = query
        .filters
        .into_iter()
        .map(|predicate| {
            maintained_view_bind_predicate(
                predicate,
                binding,
                ParamBindingMode::InlineAllReachableSeeds,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    query.joins = query
        .joins
        .into_iter()
        .map(|join| {
            bind_join_filter_literals(join, binding, ParamBindingMode::InlineAllReachableSeeds)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            reachable.from = maintained_view_bind_operand(
                reachable.from,
                binding,
                ParamBindingMode::InlineAllReachableSeeds,
            )?;
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|predicate| {
                    maintained_view_bind_predicate(
                        predicate,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|predicate| {
                    maintained_view_bind_predicate(
                        predicate,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            bind_reachable_seed_filters(
                &mut reachable,
                binding,
                ParamBindingMode::InlineAllReachableSeeds,
            )?;
            Ok(reachable)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.policy_branches = query
        .policy_branches
        .into_iter()
        .map(|mut branch| {
            branch.filters = branch
                .filters
                .into_iter()
                .map(|predicate| {
                    maintained_view_bind_predicate(
                        predicate,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            branch.joins = branch
                .joins
                .into_iter()
                .map(|join| {
                    bind_join_filter_literals(
                        join,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            branch.reachable = branch
                .reachable
                .into_iter()
                .map(|mut reachable| {
                    reachable.from = maintained_view_bind_operand(
                        reachable.from,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )?;
                    reachable.access_filters = reachable
                        .access_filters
                        .into_iter()
                        .map(|predicate| {
                            maintained_view_bind_predicate(
                                predicate,
                                binding,
                                ParamBindingMode::InlineAllReachableSeeds,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    reachable.edge_filters = reachable
                        .edge_filters
                        .into_iter()
                        .map(|predicate| {
                            maintained_view_bind_predicate(
                                predicate,
                                binding,
                                ParamBindingMode::InlineAllReachableSeeds,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    bind_reachable_seed_filters(
                        &mut reachable,
                        binding,
                        ParamBindingMode::InlineAllReachableSeeds,
                    )?;
                    Ok(reachable)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(branch)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let rebound = query.validate(schema)?;
    if rebound.schema_version() != shape.schema_version() {
        return Err(Error::InvalidStoredValue(
            "inline snapshot rebound query schema changed",
        ));
    }
    Ok(rebound)
}

fn maintained_view_bind_predicate(
    predicate: Predicate,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<Predicate, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => Predicate::All(
            predicates
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Any(predicates) => Predicate::Any(
            predicates
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Not(predicate) => Predicate::Not(Box::new(maintained_view_bind_predicate(
            *predicate, binding, mode,
        )?)),
        Predicate::Eq(left, right) => Predicate::Eq(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::Ne(left, right) => Predicate::Ne(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::In(left, values) => Predicate::In(
            maintained_view_bind_operand(left, binding, mode)?,
            values
                .into_iter()
                .map(|operand| maintained_view_bind_operand(operand, binding, mode))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Gt(left, right) => Predicate::Gt(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::Gte(left, right) => Predicate::Gte(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::Lt(left, right) => Predicate::Lt(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::Lte(left, right) => Predicate::Lte(
            maintained_view_bind_operand(left, binding, mode)?,
            maintained_view_bind_operand(right, binding, mode)?,
        ),
        Predicate::Contains(left, right) => {
            let left = maintained_view_bind_operand(left, binding, mode)?;
            let right = maintained_view_bind_operand(right, binding, mode)?;
            match left {
                Operand::Literal(Value::Array(values)) => {
                    Predicate::In(right, values.into_iter().map(Operand::Literal).collect())
                }
                left => Predicate::Contains(left, right),
            }
        }
        Predicate::IsNull(operand) => {
            Predicate::IsNull(maintained_view_bind_operand(operand, binding, mode)?)
        }
    })
}

fn bind_reachable_seed_filters(
    reachable: &mut crate::query::ReachableVia,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<(), Error> {
    if let Some(seed) = &mut reachable.seed {
        seed.filters = std::mem::take(&mut seed.filters)
            .into_iter()
            .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
            .collect::<Result<Vec<_>, _>>()?;
    }
    Ok(())
}

fn collect_join_source_tables(query: &crate::query::Query) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for join in &query.joins {
        collect_join_source_tables_for_join(join, &mut tables);
    }
    for branch in &query.policy_branches {
        let branch_query = branch.as_query(&query.table);
        tables.extend(collect_join_source_tables(&branch_query));
    }
    tables
}

fn collect_join_source_tables_for_join(join: &JoinVia, tables: &mut BTreeSet<String>) {
    tables.insert(join.table.clone());
    if let Some(lookup) = &join.source_lookup {
        tables.insert(lookup.table.clone());
    }
    for nested in &join.nested_joins {
        collect_join_source_tables_for_join(nested, tables);
    }
}

fn bind_join_filter_literals(
    mut join: JoinVia,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<JoinVia, Error> {
    join.filters = join
        .filters
        .into_iter()
        .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
        .collect::<Result<Vec<_>, _>>()?;
    join.nested_joins = join
        .nested_joins
        .into_iter()
        .map(|join| bind_join_filter_literals(join, binding, mode))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(join)
}

fn should_inline_reachable_seed(operand: &Operand, mode: ParamBindingMode) -> bool {
    match (operand, mode) {
        (Operand::Param(_), ParamBindingMode::InlineAllReachableSeeds) => true,
        (Operand::Param(_), ParamBindingMode::RetainAllParams) => false,
        _ => false,
    }
}

fn maintained_view_has_binding_dependent_reachable(shape: &ValidatedQuery) -> bool {
    fn query_has_binding_dependent_reachable(query: &crate::query::Query) -> bool {
        query.reachable.iter().any(|reachable| {
            matches!(reachable.from, Operand::Param(_))
                || reachable
                    .seed
                    .as_ref()
                    .is_some_and(|seed| !predicate_params(&seed.filters).is_empty())
        }) || query
            .policy_branches
            .iter()
            .any(|branch| query_has_binding_dependent_reachable(&branch.as_query(&query.table)))
    }

    query_has_binding_dependent_reachable(shape.query())
}

fn maintained_view_bind_operand(
    operand: Operand,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<Operand, Error> {
    Ok(match operand {
        Operand::Param(name) if matches!(mode, ParamBindingMode::RetainAllParams) => {
            Operand::Param(name)
        }
        Operand::Param(name) => Operand::Literal(
            binding
                .values()
                .get(&name)
                .cloned()
                .ok_or_else(|| QueryError::MissingParam(name.clone()))?,
        ),
        Operand::Column(_) | Operand::Claim(_) | Operand::Literal(_) => operand,
    })
}

fn graph_param_types(
    shape: &ValidatedQuery,
    schema: &JazzSchema,
) -> Result<BTreeMap<String, groove::schema::ColumnType>, Error> {
    let mut param_types = shape.params().clone();
    let query = shape.query();
    collect_query_nullable_param_types_from_schema(query, schema, &mut param_types)?;
    Ok(param_types)
}

fn collect_query_nullable_param_types_from_schema(
    query: &crate::query::Query,
    schema: &JazzSchema,
    param_types: &mut BTreeMap<String, groove::schema::ColumnType>,
) -> Result<(), Error> {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == query.table)
        .ok_or_else(|| Error::TableNotFound(query.table.clone()))?;
    collect_nullable_param_types(table, &query.filters, param_types)?;
    for join in &query.joins {
        collect_join_nullable_param_types_from_schema(join, schema, param_types)?;
    }
    for reachable in &query.reachable {
        if let Operand::Param(param) = &reachable.from {
            param_types.insert(param.clone(), groove::schema::ColumnType::Uuid);
        }
        let access_table = schema
            .tables
            .iter()
            .find(|table| table.name == reachable.access_table)
            .ok_or_else(|| Error::TableNotFound(reachable.access_table.clone()))?;
        collect_nullable_param_types(access_table, &reachable.access_filters, param_types)?;
        let edge_table = schema
            .tables
            .iter()
            .find(|table| table.name == reachable.edge_table)
            .ok_or_else(|| Error::TableNotFound(reachable.edge_table.clone()))?;
        collect_nullable_param_types(edge_table, &reachable.edge_filters, param_types)?;
        if let Some(seed) = &reachable.seed {
            let seed_table = schema
                .tables
                .iter()
                .find(|table| table.name == seed.table)
                .ok_or_else(|| Error::TableNotFound(seed.table.clone()))?;
            collect_nullable_param_types(seed_table, &seed.filters, param_types)?;
        }
    }
    for branch in &query.policy_branches {
        let branch_query = branch.as_query(&query.table).validate(schema)?;
        collect_query_nullable_param_types_from_schema(branch_query.query(), schema, param_types)?;
    }
    Ok(())
}

fn collect_join_nullable_param_types_from_schema(
    join: &JoinVia,
    schema: &JazzSchema,
    param_types: &mut BTreeMap<String, groove::schema::ColumnType>,
) -> Result<(), Error> {
    let join_table = schema
        .tables
        .iter()
        .find(|table| table.name == join.table)
        .ok_or_else(|| Error::TableNotFound(join.table.clone()))?;
    collect_nullable_param_types(join_table, &join.filters, param_types)?;
    for nested in &join.nested_joins {
        collect_join_nullable_param_types_from_schema(nested, schema, param_types)?;
    }
    Ok(())
}

fn query_binding_source_shape(shape: &ValidatedQuery) -> String {
    format!("jazz-query:{}", shape.shape_id().0)
}

fn historical_query_binding_source_shape(shape: &ValidatedQuery, position: GlobalSeq) -> String {
    format!("jazz-query-at:{}:{}", shape.shape_id().0, position.0)
}

fn include_deleted_query_binding_source_shape(
    shape: &ValidatedQuery,
    tier: DurabilityTier,
) -> String {
    format!(
        "jazz-query-include-deleted:{}:{}",
        shape.shape_id().0,
        tier as u8
    )
}

fn maintained_view_binding_source_shape(shape: &ValidatedQuery) -> String {
    format!("jazz-maintained-query:{}", shape.shape_id().0)
}

fn maintained_view_public_terminal_graph_with_bound_params<'a>(
    graph: GraphBuilder,
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    binding_source_shape: &str,
) -> Result<GraphBuilder, Error> {
    let output_fields = maintained_view_tagged_field_names(terminal_tables);
    let graph = graph.project_fields(output_fields.iter().cloned().map(ProjectField::named));
    attach_params_to_graph(
        graph,
        param_types,
        output_fields,
        true,
        binding_source_shape,
    )
}

fn collect_nullable_param_types(
    table: &TableSchema,
    predicates: &[Predicate],
    param_types: &mut BTreeMap<String, groove::schema::ColumnType>,
) -> Result<(), Error> {
    for predicate in predicates {
        match predicate {
            Predicate::All(predicates) | Predicate::Any(predicates) => {
                collect_nullable_param_types(table, predicates, param_types)?;
            }
            Predicate::Not(predicate) => {
                collect_nullable_param_types(table, std::slice::from_ref(predicate), param_types)?;
            }
            Predicate::Eq(Operand::Column(column), Operand::Param(param))
            | Predicate::Eq(Operand::Param(param), Operand::Column(column))
            | Predicate::Ne(Operand::Column(column), Operand::Param(param))
            | Predicate::Ne(Operand::Param(param), Operand::Column(column))
            | Predicate::Contains(Operand::Column(column), Operand::Param(param)) => {
                let column_type = table_column_type(table, column)?;
                param_types.insert(param.clone(), column_type.clone());
            }
            _ => {}
        }
    }
    Ok(())
}

fn binding_values_for_plan(
    binding: &Binding,
    param_names: &[String],
    param_types: &[groove::schema::ColumnType],
) -> Result<Vec<Value>, Error> {
    binding_values_for_param_names(binding.values(), param_names, param_types)
}

fn binding_values_for_param_names(
    values: &BTreeMap<String, Value>,
    param_names: &[String],
    param_types: &[groove::schema::ColumnType],
) -> Result<Vec<Value>, Error> {
    param_names
        .iter()
        .zip(param_types)
        .map(|(name, column_type)| {
            let value = values
                .get(name)
                .cloned()
                .ok_or_else(|| QueryError::MissingParam(name.clone()))?;
            Ok(match column_type {
                groove::schema::ColumnType::Nullable(_) if !matches!(value, Value::Nullable(_)) => {
                    Value::Nullable(Some(Box::new(value)))
                }
                _ => value,
            })
        })
        .collect()
}

fn local_maintained_view_content_witness<'a>(
    versions: &'a [VersionRow],
    table: &str,
    row_uuid: RowUuid,
) -> Option<&'a VersionRow> {
    versions
        .iter()
        .find(|version| version.table() == table && version.row_uuid() == row_uuid)
        .filter(|version| version.deletion().is_none())
}

fn apply_pagination(query: &crate::query::Query, rows: &mut Vec<CurrentRow>) {
    if query.offset == 0 && query.limit.is_none() {
        return;
    }
    let start = query.offset.min(rows.len());
    let end = query
        .limit
        .map(|limit| start.saturating_add(limit).min(rows.len()))
        .unwrap_or(rows.len());
    *rows = rows[start..end].to_vec();
}

fn aggregate_row_descriptor(
    table: &TableSchema,
    aggregate: &AggregateQuery,
) -> Result<RecordDescriptor, Error> {
    let mut fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
    if let Some(group_by) = &aggregate.group_by {
        fields.push((
            format!("user_{group_by}"),
            ValueType::Nullable(Box::new(
                table_column_type(table, group_by)?.clone().value_type(),
            )),
        ));
    }
    for aggregate in &aggregate.aggregates {
        fields.push((
            format!("user_{}", aggregate.alias),
            ValueType::Nullable(Box::new(
                aggregate_value_type(table, aggregate)?.value_type(),
            )),
        ));
    }
    fields.push(("tx_time".to_owned(), ValueType::U64));
    fields.push(("tx_node_id".to_owned(), ValueType::U64));
    Ok(RecordDescriptor::new(fields))
}

fn aggregate_value_type(
    table: &TableSchema,
    aggregate: &Aggregate,
) -> Result<groove::schema::ColumnType, Error> {
    Ok(match aggregate.function {
        AggregateFunction::Count => groove::schema::ColumnType::U64,
        AggregateFunction::Sum => match table_column_type(
            table,
            aggregate
                .column
                .as_deref()
                .ok_or(Error::InvalidStoredValue("sum aggregate missing column"))?,
        )? {
            groove::schema::ColumnType::F64 => groove::schema::ColumnType::F64,
            _ => groove::schema::ColumnType::U64,
        },
        AggregateFunction::Min | AggregateFunction::Max => table_column_type(
            table,
            aggregate
                .column
                .as_deref()
                .ok_or(Error::InvalidStoredValue(
                    "min/max aggregate missing column",
                ))?,
        )?
        .clone(),
    })
}

fn aggregate_value(
    table: &TableSchema,
    aggregate: &Aggregate,
    rows: &[&CurrentRow],
) -> Result<Option<Value>, Error> {
    let Some(column) = aggregate.column.as_deref() else {
        return Ok(Some(Value::U64(rows.len() as u64)));
    };
    match aggregate.function {
        AggregateFunction::Count => Ok(Some(Value::U64(
            rows.iter()
                .filter(|row| row.cell(table, column).is_some())
                .count() as u64,
        ))),
        AggregateFunction::Sum => sum_aggregate_value(table, column, rows),
        AggregateFunction::Min => min_max_aggregate_value(table, column, rows, false),
        AggregateFunction::Max => min_max_aggregate_value(table, column, rows, true),
    }
}

fn sum_aggregate_value(
    table: &TableSchema,
    column: &str,
    rows: &[&CurrentRow],
) -> Result<Option<Value>, Error> {
    let mut int_sum = 0_u64;
    let mut float_sum = 0.0_f64;
    let mut has_value = false;
    let is_float = matches!(
        table_column_type(table, column)?,
        groove::schema::ColumnType::F64
    );
    for row in rows {
        let Some(value) = row.cell(table, column) else {
            continue;
        };
        has_value = true;
        match value {
            Value::U8(value) => int_sum = int_sum.saturating_add(value as u64),
            Value::U16(value) => int_sum = int_sum.saturating_add(value as u64),
            Value::U32(value) => int_sum = int_sum.saturating_add(value as u64),
            Value::U64(value) => int_sum = int_sum.saturating_add(value),
            Value::F64(value) => float_sum += value,
            _ => {
                return Err(Error::InvalidStoredValue(
                    "aggregate column was not numeric",
                ));
            }
        }
    }
    if !has_value {
        return Ok(None);
    }
    if is_float {
        Ok(Some(Value::F64(float_sum)))
    } else {
        Ok(Some(Value::U64(int_sum)))
    }
}

fn min_max_aggregate_value(
    table: &TableSchema,
    column: &str,
    rows: &[&CurrentRow],
    max: bool,
) -> Result<Option<Value>, Error> {
    let mut best = None::<Value>;
    for row in rows {
        let Some(value) = row.cell(table, column) else {
            continue;
        };
        let replace = best.as_ref().is_none_or(|current| {
            compare_values(&value, current).is_some_and(|ordering| {
                if max {
                    ordering.is_gt()
                } else {
                    ordering.is_lt()
                }
            })
        });
        if replace {
            best = Some(value);
        }
    }
    Ok(best)
}

fn aggregate_row_uuid(group: &Option<Value>) -> uuid::Uuid {
    match group {
        Some(value) => uuid::Uuid::new_v5(&QUERY_NAMESPACE, format!("{value:?}").as_bytes()),
        None => uuid::Uuid::nil(),
    }
}

fn compare_optional_values(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_order_values(&left, &right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn sort_query_default_rows(rows: &mut [CurrentRow]) {
    rows.sort_by(|left, right| {
        left.projected_tx_alias()
            .cmp(&right.projected_tx_alias())
            .then_with(|| left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes()))
            .then_with(|| left.record.raw().cmp(right.record.raw()))
    });
}

fn aggregate_row_cell(row: &CurrentRow, column: &str) -> Option<Value> {
    let user_name = format!("user_{column}");
    let idx = row.record.descriptor().fields().iter().position(|field| {
        field.name.as_deref() == Some(user_name.as_str()) || field.name.as_deref() == Some(column)
    })?;
    nullable_value(row.record.borrowed().get_idx(idx).ok()?).ok()?
}

fn compare_order_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.cmp(right),
        (Value::U16(left), Value::U16(right)) => left.cmp(right),
        (Value::U32(left), Value::U32(right)) => left.cmp(right),
        (Value::U64(left), Value::U64(right)) => left.cmp(right),
        (Value::F64(left), Value::F64(right)) => left.total_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::Enum(left), Value::Enum(right)) => left.cmp(right),
        (Value::Tuple(left), Value::Tuple(right)) | (Value::Array(left), Value::Array(right)) => {
            compare_order_value_slices(left, right)
        }
        (Value::Nullable(left), Value::Nullable(right)) => match (left, right) {
            (Some(left), Some(right)) => compare_order_values(left, right),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
        _ => Ordering::Equal,
    }
}

fn compare_order_value_slices(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = compare_order_values(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn nullable_cell_value(table: &TableSchema, column: &str, value: Value) -> Result<Value, Error> {
    if column == "id" {
        return Ok(value);
    }
    if is_magic_current_column(column) {
        return Ok(value);
    }
    let _ = table_column_type(table, column)?;
    if matches!(value, Value::Nullable(_)) {
        return Ok(value);
    }
    Ok(Value::Nullable(Some(Box::new(value))))
}

fn table_column_type<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a groove::schema::ColumnType, Error> {
    if column == "id" {
        return Ok(&groove::schema::ColumnType::Uuid);
    }
    if let Some(column_type) = magic_current_column_type(column) {
        return Ok(column_type);
    }
    table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|column| &column.column_type)
        .ok_or(Error::InvalidStoredValue("query column was not validated"))
}

fn magic_current_column_type(column: &str) -> Option<&'static groove::schema::ColumnType> {
    match column {
        "$createdBy" | "$updatedBy" => Some(&groove::schema::ColumnType::Uuid),
        "$createdAt" | "$updatedAt" => Some(&groove::schema::ColumnType::U64),
        _ => None,
    }
}

fn is_magic_current_column(column: &str) -> bool {
    magic_current_column_type(column).is_some()
}

fn non_null_query_column_type(
    column_type: &groove::schema::ColumnType,
) -> &groove::schema::ColumnType {
    match column_type {
        groove::schema::ColumnType::Nullable(inner) => inner.as_ref(),
        other => other,
    }
}

fn predicate_params(predicates: &[Predicate]) -> BTreeSet<String> {
    let mut params = BTreeSet::new();
    for predicate in predicates {
        match predicate {
            Predicate::All(predicates) | Predicate::Any(predicates) => {
                params.extend(predicate_params(predicates));
            }
            Predicate::Not(predicate) => {
                params.extend(predicate_params(std::slice::from_ref(predicate)));
            }
            Predicate::Eq(Operand::Column(_), Operand::Param(param))
            | Predicate::Eq(Operand::Param(param), Operand::Column(_))
            | Predicate::Ne(Operand::Column(_), Operand::Param(param))
            | Predicate::Ne(Operand::Param(param), Operand::Column(_))
            | Predicate::Contains(Operand::Column(_), Operand::Param(param))
            | Predicate::Contains(Operand::Param(param), Operand::Column(_)) => {
                params.insert(param.clone());
            }
            _ => {}
        }
    }
    params
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => None,
        (Value::Nullable(Some(left)), right) => compare_values(left, right),
        (left, Value::Nullable(Some(right))) => compare_values(left, right),
        (Value::U8(left), Value::U8(right)) => left.partial_cmp(right),
        (Value::U16(left), Value::U16(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        _ => None,
    }
}

fn query_order_value(row: &CurrentRow, table: &TableSchema, column: &str) -> Option<Value> {
    if column == "id" {
        return Some(Value::Uuid(row.row_uuid().0));
    }
    if is_magic_current_column(column) {
        return row.raw_field(column);
    }
    row.cell(table, column)
}

fn subscription_row_key_for_eval(row: &RelationEvalRow) -> (String, RowUuid) {
    (row.current.table().to_owned(), row.current.row_uuid())
}

fn relation_join_left_aliases(on: &[RelationJoinCondition]) -> Vec<String> {
    on.iter()
        .filter_map(|condition| condition.left.scope.clone())
        .collect()
}

fn relation_join_right_aliases(on: &[RelationJoinCondition]) -> Vec<String> {
    on.iter()
        .filter_map(|condition| condition.right.scope.clone())
        .collect()
}

fn json_relation_value(value: &serde_json::Value) -> Option<Value> {
    if let serde_json::Value::Object(object) = value {
        if let Some(serde_json::Value::String(value_type)) = object.get("type") {
            let payload = object.get("value");
            return match value_type.as_str() {
                "Null" => None,
                "Boolean" => payload
                    .and_then(serde_json::Value::as_bool)
                    .map(Value::Bool),
                "Text" | "Enum" => payload
                    .and_then(serde_json::Value::as_str)
                    .map(|value| Value::String(value.to_owned())),
                "Uuid" => payload
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| uuid::Uuid::parse_str(value).ok())
                    .map(Value::Uuid),
                "Integer" | "BigInt" | "Timestamp" => {
                    payload.and_then(serde_json::Value::as_u64).map(Value::U64)
                }
                "Double" => payload.and_then(serde_json::Value::as_f64).map(Value::F64),
                "Bytea" => payload.as_ref().and_then(|payload| {
                    payload.as_array().map(|bytes| {
                        Value::Bytes(
                            bytes
                                .iter()
                                .filter_map(|byte| byte.as_u64().map(|byte| byte as u8))
                                .collect(),
                        )
                    })
                }),
                "Array" => payload.as_ref().and_then(|payload| {
                    payload.as_array().map(|values| {
                        Value::Array(values.iter().filter_map(json_relation_value).collect())
                    })
                }),
                _ => None,
            };
        }
    }
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(value) => Some(Value::Bool(*value)),
        serde_json::Value::Number(value) => value
            .as_u64()
            .map(Value::U64)
            .or_else(|| value.as_f64().map(Value::F64)),
        serde_json::Value::String(value) => uuid::Uuid::parse_str(value)
            .map(Value::Uuid)
            .unwrap_or_else(|_| Value::String(value.clone()))
            .into(),
        serde_json::Value::Array(values) => Some(Value::Array(
            values
                .iter()
                .filter_map(json_relation_value)
                .collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(_) => None,
    }
}

fn relation_values_equal_or_contains(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            relation_value_eq(left, right)
                || relation_value_contains(Some(left), Some(right))
                || relation_value_contains(Some(right), Some(left))
        }
        (None, None) => true,
        _ => false,
    }
}

fn relation_value_contains(left: Option<&Value>, right: Option<&Value>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return false;
    };
    match left {
        Value::Array(values) | Value::Tuple(values) => {
            values.iter().any(|value| relation_value_eq(value, right))
        }
        Value::Nullable(Some(value)) => relation_value_contains(Some(value), Some(right)),
        _ => false,
    }
}

fn relation_value_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nullable(None), Value::Nullable(None)) => true,
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => false,
        (Value::Nullable(Some(left)), right) => relation_value_eq(left, right),
        (left, Value::Nullable(Some(right))) => relation_value_eq(left, right),
        (Value::Uuid(left), Value::String(right)) | (Value::String(right), Value::Uuid(left)) => {
            uuid::Uuid::parse_str(right).is_ok_and(|right| *left == right)
        }
        _ => left == right,
    }
}

fn relation_outer_value(table: &TableSchema, row: &CurrentRow, column: &str) -> Option<Value> {
    if column == "id" {
        return Some(Value::Uuid(row.row_uuid().0));
    }
    if is_magic_current_column(column) {
        return row.raw_field(column);
    }
    row.cell(table, column)
}

fn relation_correlation_value_is_null(value: &Value) -> bool {
    matches!(value, Value::Nullable(None))
}

fn current_row_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| format!("user_{}", column.name)),
    );
    fields.push("$createdBy".to_owned());
    fields.push("$createdAt".to_owned());
    fields.push("$updatedBy".to_owned());
    fields.push("$updatedAt".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    fields
}

fn current_row_fields_with_params(
    table: &TableSchema,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
) -> Vec<String> {
    current_row_fields(table)
        .into_iter()
        .chain(param_types.keys().cloned())
        .collect()
}

fn current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    format!("user_{}", column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone().value_type())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), ValueType::Uuid),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::Uuid),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ]),
    )
}

fn inline_current_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    if let Some(provenance) = row.provenance()? {
        values.push(Value::Uuid(provenance.created_by.0));
        values.push(Value::U64(provenance.created_at.0));
        values.push(Value::Uuid(provenance.updated_by.0));
        values.push(Value::U64(provenance.updated_at.0));
    } else {
        values.push(Value::Uuid(AuthorId::SYSTEM.0));
        values.push(Value::U64(0));
        values.push(Value::Uuid(AuthorId::SYSTEM.0));
        values.push(Value::U64(0));
    }
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.push(Value::U64(tx_time.0));
    values.push(Value::U64(tx_node_alias.0));
    Ok(descriptor.create(&values)?)
}

pub(super) fn inline_current_graph(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
) -> Result<GraphBuilder, Error> {
    let descriptor = current_row_descriptor(table);
    let records = rows
        .iter()
        .map(|row| inline_current_record(table, &descriptor, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GraphBuilder::inline_records(descriptor, records))
}

fn include_deleted_current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    format!("user_{}", column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone().value_type())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), ValueType::Uuid),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::Uuid),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ])
            .chain([("__jazz_deleted".to_owned(), ValueType::Bool)]),
    )
}

fn inline_include_deleted_current_graph(
    table: &TableSchema,
    rows: Vec<(CurrentRow, bool)>,
) -> Result<GraphBuilder, Error> {
    let descriptor = include_deleted_current_row_descriptor(table);
    let records = rows
        .iter()
        .map(|(row, deleted)| {
            let mut values = Vec::with_capacity(table.columns.len() + 8);
            values.push(Value::Uuid(row.row_uuid().0));
            for column in &table.columns {
                values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
            }
            if let Some(provenance) = row.provenance()? {
                values.push(Value::Uuid(provenance.created_by.0));
                values.push(Value::U64(provenance.created_at.0));
                values.push(Value::Uuid(provenance.updated_by.0));
                values.push(Value::U64(provenance.updated_at.0));
            } else {
                values.push(Value::Uuid(AuthorId::SYSTEM.0));
                values.push(Value::U64(0));
                values.push(Value::Uuid(AuthorId::SYSTEM.0));
                values.push(Value::U64(0));
            }
            let (tx_time, tx_node_alias) = row
                .projected_tx_alias()
                .unwrap_or((TxTime(0), NodeAlias(0)));
            values.push(Value::U64(tx_time.0));
            values.push(Value::U64(tx_node_alias.0));
            values.push(Value::Bool(*deleted));
            Ok(descriptor.create(&values)?)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(GraphBuilder::inline_records(descriptor, records))
}

fn current_source_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    source_overrides: &BTreeMap<String, GraphBuilder>,
) -> GraphBuilder {
    source_overrides
        .get(&table.name)
        .cloned()
        .unwrap_or_else(|| visible_current_graph(table, tier))
}

fn include_deleted_current_graph(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| format!("user_{}", column.name))
        .collect::<Vec<_>>();
    let mut content_storage_fields = vec!["row_uuid".to_owned()];
    content_storage_fields.extend(user_fields.iter().cloned());
    content_storage_fields.push("created_by".to_owned());
    content_storage_fields.push("created_at".to_owned());
    content_storage_fields.push("updated_by".to_owned());
    content_storage_fields.push("updated_at".to_owned());
    content_storage_fields.push("tx_time".to_owned());
    content_storage_fields.push("tx_node_id".to_owned());
    let normalize_content_fields = |graph: GraphBuilder| {
        graph.project_fields(
            ["row_uuid".to_owned()]
                .into_iter()
                .chain(user_fields.iter().cloned())
                .map(ProjectField::named)
                .chain([
                    ProjectField::renamed("created_by", "$createdBy"),
                    ProjectField::renamed("created_at", "$createdAt"),
                    ProjectField::renamed("updated_by", "$updatedBy"),
                    ProjectField::renamed("updated_at", "$updatedAt"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                ]),
        )
    };
    let edge_visible_ahead = |table_name: String, fields: Vec<String>| {
        GraphBuilder::join(
            GraphBuilder::table(table_name).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::Enum(2)),
                        PredicateExpr::eq("durability", Value::Enum(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
        )
    };
    let (content_current, deletion_current) = if tier == DurabilityTier::Global {
        (
            normalize_content_fields(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            ),
            GraphBuilder::table(register_global_current_table_name(&table.name)),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            normalize_content_fields(edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_storage_fields.clone(),
            ))
        } else {
            normalize_content_fields(
                GraphBuilder::table(ahead_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            )
        };
        let ahead_deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deletion = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                ahead_deletion_fields,
            )
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&table.name))
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    normalize_content_fields(
                        GraphBuilder::table(global_current_table_name(&table.name))
                            .project(content_storage_fields.clone()),
                    ),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(current_row_fields(table)),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table(register_global_current_table_name(&table.name)),
                    ahead_deletion,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ),
        )
    };
    let deleted_winners = deletion_current
        .filter(PredicateExpr::eq("_deletion", Value::Enum(0)))
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
        ]);
    let undeleted = GraphBuilder::anti_join(
        content_current.clone(),
        deleted_winners.clone(),
        ["row_uuid"],
        ["row_uuid"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(ProjectField::named)
            .chain([ProjectField::literal("__jazz_deleted", Value::Bool(false))]),
    );
    let deleted = GraphBuilder::join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(|field| {
                    let source = match field.as_str() {
                        "$updatedBy" | "$updatedAt" | "tx_time" | "tx_node_id" => {
                            format!("right.{field}")
                        }
                        _ => format!("left.{field}"),
                    };
                    ProjectField::renamed(source, field)
                })
                .chain([ProjectField::literal("__jazz_deleted", Value::Bool(true))]),
        );
    GraphBuilder::union([undeleted, deleted])
}

fn maintained_view_history_storage_field_names(table: &TableSchema) -> Vec<String> {
    let mut fields = vec![
        "row_uuid".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| format!("user_{}", column.name)),
    );
    fields
}

fn maintained_view_history_storage_fields(table: &TableSchema, prefix: &str) -> Vec<ProjectField> {
    maintained_view_history_storage_field_names(table)
        .into_iter()
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
        .collect()
}

fn maintained_view_register_storage_fields(prefix: &str) -> Vec<ProjectField> {
    [
        "row_uuid",
        "tx_time",
        "tx_node_id",
        "schema_version",
        "parents",
        "_deletion",
    ]
    .into_iter()
    .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
    .collect()
}

fn maintained_view_version_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = current_row_fields(table);
    fields.extend(["schema_version".to_owned(), "parents".to_owned()]);
    fields
}

fn maintained_view_nullable_deletion_type() -> ValueType {
    ValueType::Nullable(Box::new(ValueType::U8))
}

#[cfg(test)]
fn maintained_view_result_current_fields(table: &TableSchema) -> Vec<ProjectField> {
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String("result_content".to_owned())),
        ProjectField::named("row_uuid"),
        ProjectField::renamed("tx_time", "content_tx_time"),
        ProjectField::renamed("tx_node_id", "content_tx_node_id"),
        ProjectField::renamed("tx_time", "version_tx_time"),
        ProjectField::renamed("tx_node_id", "version_tx_node_id"),
        ProjectField::named("schema_version"),
        ProjectField::named("parents"),
        ProjectField::null_typed("_deletion", maintained_view_nullable_deletion_type()),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| ProjectField::named(format!("user_{}", column.name))),
    );
    fields
}

#[cfg(test)]
fn maintained_view_policy_content_fields(table: &TableSchema) -> Vec<ProjectField> {
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String("content".to_owned())),
        ProjectField::renamed("row_uuid", "version_row_uuid"),
        ProjectField::renamed("tx_time", "version_tx_time"),
        ProjectField::renamed("tx_node_id", "version_tx_node_id"),
        ProjectField::named("schema_version"),
        ProjectField::named("parents"),
        ProjectField::null_typed("_deletion", maintained_view_nullable_deletion_type()),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| ProjectField::named(format!("user_{}", column.name))),
    );
    fields
}

#[cfg(test)]
fn maintained_view_policy_deletion_fields(table: &TableSchema) -> Vec<ProjectField> {
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String("deletion".to_owned())),
        ProjectField::renamed("left.row_uuid", "version_row_uuid"),
        ProjectField::renamed("left.tx_time", "version_tx_time"),
        ProjectField::renamed("left.tx_node_id", "version_tx_node_id"),
        ProjectField::renamed("left.schema_version", "schema_version"),
        ProjectField::renamed("left.parents", "parents"),
        ProjectField::literal("_deletion", Value::Nullable(Some(Box::new(Value::U8(0))))),
    ];
    fields.extend(table.columns.iter().map(|column| {
        ProjectField::null_typed(
            format!("user_{}", column.name),
            ValueType::Nullable(Box::new(column.column_type.value_type())),
        )
    }));
    fields
}

fn maintained_view_tagged_content_fields<'a>(
    table: &TableSchema,
    event_kind: &str,
    prefix: &str,
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    available_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    param_prefix: &str,
) -> Vec<ProjectField> {
    let source = |field: &str| format!("{prefix}{field}");
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String(event_kind.to_owned())),
        ProjectField::literal("table_name", Value::String(table.name.clone())),
        ProjectField::renamed(source("row_uuid"), "row_uuid"),
        ProjectField::renamed(source("tx_time"), "content_tx_time"),
        ProjectField::renamed(source("tx_node_id"), "content_tx_node_id"),
        ProjectField::renamed(source("tx_time"), "tx_time"),
        ProjectField::renamed(source("tx_node_id"), "tx_node_id"),
        ProjectField::renamed(source("schema_version"), "schema_version"),
        ProjectField::renamed(source("parents"), "parents"),
        ProjectField::null_typed("_deletion", maintained_view_nullable_deletion_type()),
    ];
    let table_columns = table
        .columns
        .iter()
        .map(|column| (column.name.as_str(), &column.column_type))
        .collect::<BTreeMap<_, _>>();
    fields.extend(
        maintained_view_terminal_user_columns(terminal_tables)
            .into_iter()
            .map(|((table_name, column_name), column_type)| {
                let user_field = format!("user_{column_name}");
                let tagged_field = maintained_view_tagged_user_field(&table_name, &column_name);
                if table_name == table.name && table_columns.contains_key(column_name.as_str()) {
                    ProjectField::renamed(source(&user_field), tagged_field)
                } else {
                    ProjectField::null_typed(
                        tagged_field,
                        ValueType::Nullable(Box::new(column_type.value_type())),
                    )
                }
            }),
    );
    append_maintained_view_param_fields(
        &mut fields,
        param_types,
        available_param_types,
        param_prefix,
    );
    fields
}

fn maintained_view_tagged_field_names<'a>(
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
) -> Vec<String> {
    let mut fields = [
        "event_kind",
        "table_name",
        "row_uuid",
        "content_tx_time",
        "content_tx_node_id",
        "tx_time",
        "tx_node_id",
        "schema_version",
        "parents",
        "_deletion",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    fields.extend(
        maintained_view_terminal_user_columns(terminal_tables)
            .into_keys()
            .map(|(table, column)| maintained_view_tagged_user_field(&table, &column)),
    );
    fields
}

fn maintained_view_tagged_deletion_fields<'a>(
    table: &TableSchema,
    event_kind: &str,
    prefix: &str,
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    available_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    param_prefix: &str,
) -> Vec<ProjectField> {
    let source = |field: &str| format!("{prefix}{field}");
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String(event_kind.to_owned())),
        ProjectField::literal("table_name", Value::String(table.name.clone())),
        ProjectField::renamed(source("row_uuid"), "row_uuid"),
        ProjectField::renamed(source("tx_time"), "content_tx_time"),
        ProjectField::renamed(source("tx_node_id"), "content_tx_node_id"),
        ProjectField::renamed(source("tx_time"), "tx_time"),
        ProjectField::renamed(source("tx_node_id"), "tx_node_id"),
        ProjectField::renamed(source("schema_version"), "schema_version"),
        ProjectField::renamed(source("parents"), "parents"),
        ProjectField::literal("_deletion", Value::Nullable(Some(Box::new(Value::U8(0))))),
    ];
    fields.extend(
        maintained_view_terminal_user_columns(terminal_tables)
            .into_iter()
            .map(|((table_name, column_name), column_type)| {
                ProjectField::null_typed(
                    maintained_view_tagged_user_field(&table_name, &column_name),
                    ValueType::Nullable(Box::new(column_type.value_type())),
                )
            }),
    );
    append_maintained_view_param_fields(
        &mut fields,
        param_types,
        available_param_types,
        param_prefix,
    );
    fields
}

fn append_maintained_view_param_fields(
    fields: &mut Vec<ProjectField>,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    available_param_types: &BTreeMap<String, groove::schema::ColumnType>,
    prefix: &str,
) {
    fields.extend(param_types.iter().map(|(param, column_type)| {
        if available_param_types.contains_key(param) {
            ProjectField::renamed(format!("{prefix}{param}"), param.clone())
        } else {
            ProjectField::null_typed(
                param.clone(),
                maintained_view_hidden_param_value_type(column_type),
            )
        }
    }));
}

fn maintained_view_hidden_param_value_type(column_type: &groove::schema::ColumnType) -> ValueType {
    match column_type.value_type() {
        ValueType::Nullable(_) => column_type.value_type(),
        value_type => ValueType::Nullable(Box::new(value_type)),
    }
}

fn maintained_view_hidden_param_column_types(
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
) -> BTreeMap<String, groove::schema::ColumnType> {
    param_types
        .iter()
        .map(|(name, column_type)| {
            let column_type = match column_type {
                groove::schema::ColumnType::Nullable(_) => column_type.clone(),
                column_type => groove::schema::ColumnType::Nullable(Box::new(column_type.clone())),
            };
            (name.clone(), column_type)
        })
        .collect()
}

fn maintained_view_terminal_user_columns<'a>(
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
) -> BTreeMap<(String, String), groove::schema::ColumnType> {
    let mut columns = BTreeMap::new();
    for table in terminal_tables {
        for column in &table.columns {
            columns
                .entry((table.name.clone(), column.name.clone()))
                .or_insert_with(|| column.column_type.clone());
        }
    }
    columns
}

pub(crate) fn maintained_view_tagged_user_field(table: &str, column: &str) -> String {
    format!("user__{table}__{column}")
}

fn query_field(column: &str) -> String {
    if column == "id" {
        return "row_uuid".to_owned();
    }
    if is_magic_current_column(column) {
        return column.to_owned();
    }
    format!("user_{column}")
}

fn join_key(join: &JoinVia) -> String {
    match join.target {
        JoinTarget::Column => query_field(&join.on_column),
        JoinTarget::RowId => "row_uuid".to_owned(),
    }
}

fn join_left_keys(join: &JoinVia, primary_left_key: &str) -> Vec<String> {
    std::iter::once(primary_left_key.to_owned())
        .chain(
            join.correlated_filters
                .iter()
                .map(|correlation| query_field(&correlation.source_column)),
        )
        .collect()
}

fn join_right_keys(join: &JoinVia, primary_join_key: &str) -> Vec<String> {
    std::iter::once(primary_join_key.to_owned())
        .chain(
            join.correlated_filters
                .iter()
                .map(|correlation| query_field(&correlation.join_column)),
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use groove::schema::{ColumnSchema, ColumnType};
    use groove::storage::{Durability, RocksDbStorage};

    use crate::ids::{AuthorId, NodeUuid, RowUuid};
    use crate::node::{MergeableCommit, NodeState};
    use crate::peer::PeerState;
    use crate::protocol::{RegisterShapeOptions, ShapeAst, SyncMessage};
    use crate::query::{
        Aggregate, OrderDirection, Query, claim, col, contains, eq, gt, in_list, lit, lte, param,
    };
    use crate::schema::{JazzSchema, TableSchema};

    use super::*;

    fn schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                ],
            )
            .with_reference("assignee", "users"),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "issue_members",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("user", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("user", "users"),
        ])
    }

    fn open_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
        let schema = schema();
        open_node_with_uuid(NodeUuid::from_bytes([9; 16]), schema)
    }

    fn open_node_with_uuid(
        node_uuid: NodeUuid,
        schema: JazzSchema,
    ) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
                .expect("open rocksdb");
        let node = NodeState::new(node_uuid, schema, storage).expect("node");
        (temp_dir, node)
    }

    fn recursive_schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "resourceAccess",
                [
                    ColumnSchema::new("resource", ColumnType::Uuid),
                    ColumnSchema::new("team", ColumnType::Uuid),
                ],
            )
            .with_reference("resource", "resources")
            .with_reference("team", "teams"),
            TableSchema::new(
                "teamTeamMemberships",
                [
                    ColumnSchema::new("member", ColumnType::Uuid),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                    ColumnSchema::new("onlyAdmins", ColumnType::Bool),
                ],
            )
            .with_reference("member", "teams")
            .with_reference("parent", "teams"),
        ])
    }

    fn open_recursive_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
        open_node_with_uuid(NodeUuid::from_bytes([9; 16]), recursive_schema())
    }

    fn row(idx: usize) -> RowUuid {
        let mut bytes = [0_u8; 16];
        bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
        RowUuid::from_bytes(bytes)
    }

    fn commit_global_cells(
        node: &mut NodeState<RocksDbStorage>,
        table: &str,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
        now_ms: u64,
        global_seq: u64,
    ) -> TxId {
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new(table, row_uuid, now_ms)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells),
            )
            .expect("commit row");
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(global_seq)),
            Some(DurabilityTier::Global),
        )
        .expect("accept row");
        tx_id
    }

    fn delete_global(
        node: &mut NodeState<RocksDbStorage>,
        table: &str,
        row_uuid: RowUuid,
        now_ms: u64,
        global_seq: u64,
    ) -> TxId {
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new(table, row_uuid, now_ms)
                    .made_by(AuthorId::SYSTEM)
                    .deletion(crate::tx::DeletionEvent::Deleted),
            )
            .expect("delete row");
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(global_seq)),
            Some(DurabilityTier::Global),
        )
        .expect("accept delete");
        tx_id
    }

    fn author(byte: u8) -> AuthorId {
        AuthorId::from_bytes([byte; 16])
    }

    fn commit_issue(
        node: &mut NodeState<RocksDbStorage>,
        idx: usize,
        state: &str,
        assignee: AuthorId,
    ) {
        node.commit_mergeable_unit(
            MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                    ("state".to_owned(), Value::String(state.to_owned())),
                    ("assignee".to_owned(), Value::Uuid(assignee.0)),
                    ("priority".to_owned(), Value::U64(idx as u64)),
                ])),
        )
        .expect("commit issue");
    }

    fn commit_global_issue(
        node: &mut NodeState<RocksDbStorage>,
        idx: usize,
        state: &str,
        assignee: AuthorId,
        seq: u64,
    ) -> TxId {
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
                    .made_by(AuthorId::SYSTEM)
                    .cells(BTreeMap::from([
                        ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                        ("state".to_owned(), Value::String(state.to_owned())),
                        ("assignee".to_owned(), Value::Uuid(assignee.0)),
                        ("priority".to_owned(), Value::U64(idx as u64)),
                    ])),
            )
            .expect("commit issue");
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(seq)),
            Some(DurabilityTier::Global),
        )
        .expect("accept issue");
        tx_id
    }

    fn commit_member(
        node: &mut NodeState<RocksDbStorage>,
        idx: usize,
        issue: RowUuid,
        user: AuthorId,
    ) {
        node.commit_mergeable_unit(
            MergeableCommit::new("issue_members", row(10_000 + idx), 10_000 + idx as u64)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([
                    ("issue".to_owned(), Value::Uuid(issue.0)),
                    ("user".to_owned(), Value::Uuid(user.0)),
                ])),
        )
        .expect("commit member");
    }

    fn commit_global_user(
        node: &mut NodeState<RocksDbStorage>,
        user: AuthorId,
        name: &str,
        seq: u64,
    ) {
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("users", RowUuid(user.0), 2_000 + seq)
                    .made_by(AuthorId::SYSTEM)
                    .cells(BTreeMap::from([(
                        "name".to_owned(),
                        Value::String(name.to_owned()),
                    )])),
            )
            .expect("commit user");
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(seq)),
            Some(DurabilityTier::Global),
        )
        .expect("accept user");
    }

    fn commit_global_member(
        node: &mut NodeState<RocksDbStorage>,
        idx: usize,
        issue: RowUuid,
        user: AuthorId,
        seq: u64,
    ) {
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("issue_members", row(10_000 + idx), 3_000 + seq)
                    .made_by(AuthorId::SYSTEM)
                    .cells(BTreeMap::from([
                        ("issue".to_owned(), Value::Uuid(issue.0)),
                        ("user".to_owned(), Value::Uuid(user.0)),
                    ])),
            )
            .expect("commit member");
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(seq)),
            Some(DurabilityTier::Global),
        )
        .expect("accept member");
    }

    fn recursive_shape(schema: &JazzSchema) -> ValidatedQuery {
        Query::from("resources")
            .reachable_via(
                "resourceAccess",
                "resource",
                "team",
                param("team"),
                "teamTeamMemberships",
                "member",
                "parent",
                [eq(col("onlyAdmins"), lit(false))],
            )
            .validate(schema)
            .unwrap()
    }

    #[test]
    fn recursive_reachability_subscription_grants_and_revokes_incrementally() {
        let (_dir, mut core) = open_recursive_node();
        let schema = recursive_schema();
        let team1 = row(1);
        let team2 = row(2);
        let team3 = row(3);
        let team4 = row(4);
        let resource1 = row(101);
        let resource2 = row(102);
        commit_global_cells(
            &mut core,
            "resources",
            resource1,
            BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
            10,
            1,
        );
        commit_global_cells(
            &mut core,
            "resources",
            resource2,
            BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
            11,
            2,
        );
        commit_global_cells(
            &mut core,
            "resourceAccess",
            row(201),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource1.0)),
                ("team".to_owned(), Value::Uuid(team3.0)),
            ]),
            12,
            3,
        );
        commit_global_cells(
            &mut core,
            "resourceAccess",
            row(202),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource2.0)),
                ("team".to_owned(), Value::Uuid(team4.0)),
            ]),
            13,
            4,
        );
        for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
            commit_global_cells(
                &mut core,
                "teamTeamMemberships",
                row(idx),
                BTreeMap::from([
                    ("member".to_owned(), Value::Uuid(member.0)),
                    ("parent".to_owned(), Value::Uuid(parent.0)),
                    ("onlyAdmins".to_owned(), Value::Bool(false)),
                ]),
                10 + seq,
                seq,
            );
        }

        let shape = recursive_shape(&schema);
        let binding = shape
            .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
            .unwrap();
        let mut peer = PeerState::new();
        let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(matches!(
            initial,
            SyncMessage::ViewUpdate {
                result_row_adds,
                ..
            } if result_row_adds.iter().any(|(_, row_uuid, _)| *row_uuid == resource1)
                && result_row_adds.iter().all(|(_, row_uuid, _)| *row_uuid != resource2)
        ));

        commit_global_cells(
            &mut core,
            "teamTeamMemberships",
            row(303),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(team3.0)),
                ("parent".to_owned(), Value::Uuid(team4.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            17,
            7,
        );
        let grant = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert!(matches!(
            grant,
            SyncMessage::ViewUpdate {
                result_row_adds,
                result_row_removes,
                ..
            } if result_row_adds.iter().any(|(_, row_uuid, _)| *row_uuid == resource2)
                && result_row_removes.is_empty()
        ));

        delete_global(&mut core, "teamTeamMemberships", row(302), 18, 8);
        let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert!(matches!(
            revoke,
            SyncMessage::ViewUpdate {
                result_row_adds,
                result_row_removes,
                ..
            } if result_row_adds.is_empty()
                && result_row_removes.iter().any(|(_, row_uuid, _)| *row_uuid == resource1)
                && result_row_removes.iter().any(|(_, row_uuid, _)| *row_uuid == resource2)
        ));
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
    }

    #[test]
    fn reachable_query_rows_uses_prepared_groove_plan() {
        let (_dir, mut node) = open_recursive_node();
        let schema = recursive_schema();
        let team1 = row(1);
        let team2 = row(2);
        let team3 = row(3);
        let resource1 = row(101);
        let resource2 = row(102);
        commit_global_cells(
            &mut node,
            "resources",
            resource1,
            BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
            10,
            1,
        );
        commit_global_cells(
            &mut node,
            "resources",
            resource2,
            BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
            11,
            2,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(201),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource1.0)),
                ("team".to_owned(), Value::Uuid(team3.0)),
            ]),
            12,
            3,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(202),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource2.0)),
                ("team".to_owned(), Value::Uuid(team1.0)),
            ]),
            13,
            4,
        );
        for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
            commit_global_cells(
                &mut node,
                "teamTeamMemberships",
                row(idx),
                BTreeMap::from([
                    ("member".to_owned(), Value::Uuid(member.0)),
                    ("parent".to_owned(), Value::Uuid(parent.0)),
                    ("onlyAdmins".to_owned(), Value::Bool(false)),
                ]),
                10 + seq,
                seq,
            );
        }

        let shape = recursive_shape(&schema);
        let binding = shape
            .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
            .unwrap();
        assert!(
            !node
                .query
                .query_shape_cache
                .contains_key(&(shape.shape_id(), DurabilityTier::Global))
        );

        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(rows, BTreeSet::from([resource1, resource2]));
        assert!(matches!(
            node.query
                .query_shape_cache
                .get(&(shape.shape_id(), DurabilityTier::Global)),
            Some(PreparedQueryPlan::Prepared { .. })
        ));
    }

    #[test]
    fn query_rows_at_lowers_reachable_against_historical_current_sources() {
        let (_dir, mut node) = open_recursive_node();
        let schema = recursive_schema();
        let team1 = row(1);
        let team2 = row(2);
        let team3 = row(3);
        let resource1 = row(101);
        let resource2 = row(102);
        commit_global_cells(
            &mut node,
            "resources",
            resource1,
            BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
            10,
            1,
        );
        commit_global_cells(
            &mut node,
            "resources",
            resource2,
            BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
            11,
            2,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(201),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource1.0)),
                ("team".to_owned(), Value::Uuid(team3.0)),
            ]),
            12,
            3,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(202),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource2.0)),
                ("team".to_owned(), Value::Uuid(team1.0)),
            ]),
            13,
            4,
        );
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(301),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(team1.0)),
                ("parent".to_owned(), Value::Uuid(team2.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            14,
            5,
        );
        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(302),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(team2.0)),
                ("parent".to_owned(), Value::Uuid(team3.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            15,
            6,
        );
        let shape = recursive_shape(&schema);
        let binding = shape
            .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
            .unwrap();

        let before_delete = node
            .query_rows_at(&shape, &binding, GlobalSeq(6))
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        delete_global(&mut node, "teamTeamMemberships", row(302), 16, 7);
        let after_delete = node
            .query_rows_at(&shape, &binding, GlobalSeq(7))
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(before_delete, BTreeSet::from([resource1, resource2]));
        assert!(
            after_delete == BTreeSet::from([resource2]),
            "later historical cuts should see the edge deletion while preserving direct access"
        );
    }

    #[test]
    fn query_filter_matches_naive_local_scan() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        let mut expected = BTreeSet::new();
        for idx in 0..48 {
            let state = if idx % 3 == 0 { "done" } else { "open" };
            let assignee = if idx % 2 == 0 { alice } else { bob };
            if state == "open" && assignee == alice {
                expected.insert(row(idx));
            }
            commit_issue(&mut node, idx, state, assignee);
        }
        let shape = Query::from("issues")
            .filter(eq(col("state"), lit("open")))
            .filter(eq(col("assignee"), param("user")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let actual = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn policy_claim_array_string_ids_bind_as_uuid_array() {
        let schema = JazzSchema::new([
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                ],
            )
            .with_reference("assignee", "users")
            .with_read_policy(
                Query::from("issues").filter(contains(claim("team_ids"), col("assignee"))),
            ),
        ]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([8; 16]), schema.clone());
        let alice = author(1);
        let bob = author(2);
        commit_issue(&mut node, 1, "open", alice);
        commit_issue(&mut node, 2, "open", bob);

        let reader = author(9);
        node.set_session_claims(
            reader,
            BTreeMap::from([(
                "team_ids".to_owned(),
                Value::Array(vec![Value::String(alice.0.to_string())]),
            )]),
        );
        let shape = Query::from("issues").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let visible = node
            .query_rows_for_link(&shape, &binding, DurabilityTier::Local, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(visible, BTreeSet::from([row(1)]));
    }

    #[test]
    fn text_range_predicates_use_lexicographic_row_comparison() {
        assert_eq!(
            compare_values(
                &Value::String("beta".to_owned()),
                &Value::String("alpha".to_owned())
            ),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            compare_values(
                &Value::String("alpha".to_owned()),
                &Value::String("alpha".to_owned())
            ),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            compare_values(
                &Value::String("alpha".to_owned()),
                &Value::String("beta".to_owned())
            ),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn text_range_query_filters_rows_lexicographically() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        for idx in 0..6 {
            commit_issue(&mut node, idx, "open", alice);
        }
        let shape = Query::from("issues")
            .filter(gt(col("title"), lit("issue-2")))
            .filter(lte(col("title"), lit("issue-4")))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let actual = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, BTreeSet::from([row(3), row(4)]));
    }

    #[test]
    fn public_id_equality_query_filters_rows_by_row_uuid() {
        let (_dir, mut node) = open_node();
        for idx in 0..4 {
            commit_issue(&mut node, idx, "open", author(1));
        }
        let shape = Query::from("issues")
            .filter(eq(col("id"), lit(Value::Uuid(row(2).0))))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let actual = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![row(2)]);
    }

    #[test]
    fn public_id_in_query_filters_rows_by_row_uuid() {
        let (_dir, mut node) = open_node();
        for idx in 0..5 {
            commit_issue(&mut node, idx, "open", author(1));
        }
        let shape = Query::from("issues")
            .filter(in_list(
                col("id"),
                [lit(Value::Uuid(row(1).0)), lit(Value::Uuid(row(3).0))],
            ))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let actual = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, BTreeSet::from([row(1), row(3)]));
    }

    #[test]
    fn public_id_range_query_and_order_by_use_row_uuid() {
        let (_dir, mut node) = open_node();
        for idx in [3, 1, 4, 0, 2] {
            commit_issue(&mut node, idx, "open", author(1));
        }
        let shape = Query::from("issues")
            .filter(gt(col("id"), lit(Value::Uuid(row(1).0))))
            .order_by("id", OrderDirection::Desc)
            .limit(2)
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let actual = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![row(4), row(3)]);
    }

    #[test]
    fn query_order_by_sorts_before_limit_offset() {
        let (_dir, mut node) = open_node();
        for idx in [3, 1, 4, 0, 2] {
            commit_issue(&mut node, idx, "open", author(1));
        }

        let asc_shape = Query::from("issues")
            .order_by("title", OrderDirection::Asc)
            .validate(&schema())
            .unwrap();
        let asc_binding = asc_shape.bind(BTreeMap::new()).unwrap();
        let asc_rows = node
            .query_rows(&asc_shape, &asc_binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(asc_rows, vec![row(0), row(1), row(2), row(3), row(4)]);

        let shape = Query::from("issues")
            .order_by("title", OrderDirection::Desc)
            .offset(1)
            .limit(2)
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![row(3), row(2)]);
    }

    #[test]
    fn query_order_by_multi_key_is_deterministic() {
        let (_dir, mut node) = open_node();
        commit_issue(&mut node, 3, "done", author(1));
        commit_issue(&mut node, 1, "open", author(1));
        commit_issue(&mut node, 2, "open", author(1));
        commit_issue(&mut node, 0, "done", author(1));

        let shape = Query::from("issues")
            .order_by("state", OrderDirection::Asc)
            .order_by("title", OrderDirection::Desc)
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![row(3), row(0), row(2), row(1)]);
    }

    #[test]
    fn aggregate_count_over_filtered_query() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        for idx in 0..8 {
            let assignee = if idx % 2 == 0 { alice } else { bob };
            let state = if idx == 6 { "done" } else { "open" };
            commit_issue(&mut node, idx, state, assignee);
        }
        let shape = Query::from("issues")
            .filter(eq(col("state"), lit("open")))
            .filter(eq(col("assignee"), param("user")))
            .count()
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].test_cells_by_descriptor()["count"], Value::U64(3));
    }

    #[test]
    fn aggregate_sum_min_max_over_filtered_query() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        for idx in 0..6 {
            let assignee = if idx % 2 == 0 { alice } else { bob };
            commit_issue(&mut node, idx, "open", assignee);
        }
        let shape = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .aggregate([
                Aggregate::sum("priority"),
                Aggregate::min("priority"),
                Aggregate::max("priority"),
            ])
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        let cells = rows[0].test_cells_by_descriptor();
        assert_eq!(cells["sum_priority"], Value::U64(6));
        assert_eq!(cells["min_priority"], Value::U64(0));
        assert_eq!(cells["max_priority"], Value::U64(4));
    }

    #[test]
    fn aggregate_grouped_count_orders_before_limit_offset() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        for idx in 0..6 {
            let state = match idx {
                0 => "done",
                1 | 2 => "open",
                _ => "blocked",
            };
            commit_issue(&mut node, idx, state, alice);
        }
        let shape = Query::from("issues")
            .count()
            .group_by("state")
            .order_by("count", OrderDirection::Desc)
            .order_by("state", OrderDirection::Asc)
            .offset(1)
            .limit(1)
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        assert_eq!(rows.len(), 1);
        let cells = rows[0].test_cells_by_descriptor();
        assert_eq!(cells["state"], Value::String("open".to_owned()));
        assert_eq!(cells["count"], Value::U64(2));
    }

    #[test]
    fn query_join_via_matches_junction_semantics() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        for idx in 0..6 {
            commit_issue(&mut node, idx, "open", bob);
        }
        commit_member(&mut node, 0, row(0), alice);
        commit_member(&mut node, 1, row(2), alice);
        commit_member(&mut node, 2, row(2), bob);
        commit_member(&mut node, 3, row(5), bob);
        let shape = Query::from("issues")
            .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
            .validate(&schema())
            .unwrap();
        let alice_binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let bob_binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(bob.0))]))
            .unwrap();
        let alice_rows = node
            .query_rows(&shape, &alice_binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        let bob_rows = node
            .query_rows(&shape, &bob_binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(alice_rows, BTreeSet::from([row(0), row(2)]));
        assert_eq!(bob_rows, BTreeSet::from([row(2), row(5)]));
    }

    #[test]
    fn exclusive_join_shape_uses_shared_snapshot_lowering() {
        let schema = schema();
        let (_client_dir, mut client) =
            open_node_with_uuid(NodeUuid::from_bytes([1; 16]), schema.clone());
        let alice = author(1);
        client
            .commit_mergeable(
                MergeableCommit::new("issues", row(1), 10).cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("issue".to_owned()),
                )])),
            )
            .unwrap();
        client
            .commit_mergeable(MergeableCommit::new("issue_members", row(2), 11).cells(
                BTreeMap::from([
                    ("issue".to_owned(), Value::Uuid(row(1).0)),
                    ("user".to_owned(), Value::Uuid(alice.0)),
                ]),
            ))
            .unwrap();

        let shape = Query::from("issues")
            .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
            .validate(&schema)
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();

        let open = client.open_exclusive().unwrap();
        let rows = client
            .tx_query(open, &shape, &binding)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(rows, BTreeSet::from([row(1)]));
    }

    #[test]
    fn unsettled_query_reads_own_pending_write() {
        let (_dir, mut node) = open_node();
        commit_issue(&mut node, 1, "open", author(1));
        let shape = Query::from("issues")
            .filter(eq(col("state"), lit("open")))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        assert_eq!(
            node.query_rows(&shape, &binding, DurabilityTier::Local)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            node.query_rows(&shape, &binding, DurabilityTier::Global)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn tx_query_snapshot_is_stable_after_concurrent_arrival() {
        let (_dir, mut node) = open_node();
        commit_issue(&mut node, 1, "open", author(1));
        let shape = Query::from("issues")
            .filter(eq(col("state"), lit("open")))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let tx = node.open_exclusive().unwrap();
        assert_eq!(node.tx_query(tx, &shape, &binding).unwrap().len(), 1);
        commit_issue(&mut node, 2, "open", author(1));
        assert_eq!(node.tx_query(tx, &shape, &binding).unwrap().len(), 1);
        node.abandon_tx(tx).unwrap();
    }

    #[test]
    fn tx_query_reachable_uses_shared_snapshot_sources() {
        let (_dir, mut node) = open_recursive_node();
        let schema = recursive_schema();
        let team1 = row(1);
        let team2 = row(2);
        let team3 = row(3);
        let team4 = row(4);
        let resource1 = row(101);
        let resource2 = row(102);
        commit_global_cells(
            &mut node,
            "resources",
            resource1,
            BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
            10,
            1,
        );
        commit_global_cells(
            &mut node,
            "resources",
            resource2,
            BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
            11,
            2,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(201),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource1.0)),
                ("team".to_owned(), Value::Uuid(team3.0)),
            ]),
            12,
            3,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(202),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource2.0)),
                ("team".to_owned(), Value::Uuid(team4.0)),
            ]),
            13,
            4,
        );
        for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
            commit_global_cells(
                &mut node,
                "teamTeamMemberships",
                row(idx),
                BTreeMap::from([
                    ("member".to_owned(), Value::Uuid(member.0)),
                    ("parent".to_owned(), Value::Uuid(parent.0)),
                    ("onlyAdmins".to_owned(), Value::Bool(false)),
                ]),
                10 + seq,
                seq,
            );
        }

        let shape = recursive_shape(&schema);
        let binding = shape
            .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
            .unwrap();
        let tx = node.open_exclusive().unwrap();
        let rows = node
            .tx_query(tx, &shape, &binding)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(rows, BTreeSet::from([resource1]));

        commit_global_cells(
            &mut node,
            "teamTeamMemberships",
            row(303),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(team3.0)),
                ("parent".to_owned(), Value::Uuid(team4.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            20,
            7,
        );
        let rows = node
            .tx_query(tx, &shape, &binding)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(rows, BTreeSet::from([resource1]));
        node.abandon_tx(tx).unwrap();
    }

    #[test]
    fn prepared_query_lowering_matches_expected_sets() {
        for seed in 0..12_u64 {
            let (_dir, mut prepared_node) = open_node();
            let alice = author(1);
            let bob = author(2);
            let user = if seed & 1 == 0 { alice } else { bob };
            let mut filtered_expected = BTreeSet::new();
            let mut joined_expected = BTreeSet::new();
            for idx in 0..36 {
                let mixed = seed.wrapping_add(idx as u64 * 17);
                let state = if mixed % 4 == 0 { "done" } else { "open" };
                let assignee = if mixed & 1 == 0 { alice } else { bob };
                commit_issue(&mut prepared_node, idx, state, assignee);
                if state == "open" && assignee == user {
                    filtered_expected.insert(row(idx));
                }
                if mixed % 3 == 0 {
                    let member_user = if mixed & 2 == 0 { alice } else { bob };
                    commit_member(&mut prepared_node, idx, row(idx), member_user);
                    if member_user == user {
                        joined_expected.insert(row(idx));
                    }
                }
            }

            let shapes = [
                (
                    Query::from("issues")
                        .filter(eq(col("state"), lit("open")))
                        .filter(eq(col("assignee"), param("user")))
                        .validate(&schema())
                        .unwrap(),
                    filtered_expected,
                ),
                (
                    Query::from("issues")
                        .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
                        .validate(&schema())
                        .unwrap(),
                    joined_expected,
                ),
            ];
            for (shape, expected) in shapes {
                let binding = shape
                    .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(user.0))]))
                    .unwrap();
                let prepared = prepared_node
                    .query_rows(&shape, &binding, DurabilityTier::Local)
                    .unwrap()
                    .into_iter()
                    .map(|row| row.row_uuid())
                    .collect::<BTreeSet<_>>();
                assert_eq!(prepared, expected, "seed {seed}");
            }
        }
    }

    #[test]
    fn query_subscription_result_sets_track_bindings_and_rehydrate() {
        let (_server_dir, mut server) = open_node();
        let (_reader_dir, mut reader) = open_node();
        let alice = author(1);
        let bob = author(2);
        let shape = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate(&schema())
            .unwrap();
        let alice_binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let bob_binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(bob.0))]))
            .unwrap();

        server
            .apply_sync_message(SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(&shape),
                opts: RegisterShapeOptions::default(),
            })
            .unwrap();
        server
            .apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
                shape_id: shape.shape_id(),
                subscription: SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: alice_binding.binding_id(),
                },
                values: alice_binding.values().values().cloned().collect(),
            }))
            .unwrap();
        server
            .apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
                shape_id: shape.shape_id(),
                subscription: SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: bob_binding.binding_id(),
                },
                values: bob_binding.values().values().cloned().collect(),
            }))
            .unwrap();

        let mut peer = PeerState::new();
        commit_global_issue(&mut server, 0, "open", alice, 1);
        commit_global_issue(&mut server, 1, "open", bob, 2);
        let alice_initial = peer
            .rehydrate_query(&mut server, &shape, &alice_binding)
            .unwrap();
        reader.apply_sync_message(alice_initial).unwrap();
        let bob_initial = peer
            .rehydrate_query(&mut server, &shape, &bob_binding)
            .unwrap();
        reader.apply_sync_message(bob_initial).unwrap();

        assert_eq!(
            reader
                .query_rows(&shape, &alice_binding, DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([row(0)])
        );
        assert_eq!(
            reader
                .query_rows(&shape, &bob_binding, DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([row(1)])
        );

        commit_global_issue(&mut server, 2, "open", alice, 3);
        let alice_delta = peer
            .query_update(&mut server, &shape, &alice_binding)
            .unwrap();
        reader.apply_sync_message(alice_delta).unwrap();
        let bob_delta = peer
            .query_update(&mut server, &shape, &bob_binding)
            .unwrap();
        reader.apply_sync_message(bob_delta).unwrap();
        assert_eq!(
            reader
                .query_rows(&shape, &alice_binding, DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([row(0), row(2)])
        );

        server
            .apply_sync_message(SyncMessage::Unsubscribe {
                subscription: SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: alice_binding.binding_id(),
                },
            })
            .unwrap();
        peer.forget_query_binding(&shape, &alice_binding);
        commit_global_issue(&mut server, 3, "open", alice, 4);
        let removed_delta = peer
            .query_update(&mut server, &shape, &alice_binding)
            .unwrap();
        assert!(matches!(
            removed_delta,
            SyncMessage::ViewUpdate {
                result_row_adds,
                result_row_removes,
                ..
            } if result_row_adds.is_empty() && result_row_removes.is_empty()
        ));

        let reset = peer
            .rehydrate_query(&mut server, &shape, &alice_binding)
            .unwrap();
        let SyncMessage::ViewUpdate {
            reset_result_set, ..
        } = &reset
        else {
            panic!("expected view update");
        };
        assert!(reset_result_set);
        reader.apply_sync_message(reset).unwrap();
        assert_eq!(
            reader
                .query_rows(&shape, &alice_binding, DurabilityTier::Global)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn query_subscription_ships_provenance_closure_for_local_evaluation() {
        let (_server_dir, mut server) = open_node();
        let (_reader_dir, mut reader) = open_node();
        let alice = author(1);
        let bob = author(2);
        commit_global_user(&mut server, alice, "alice", 1);
        commit_global_user(&mut server, bob, "bob", 2);
        commit_global_issue(&mut server, 0, "open", bob, 3);
        commit_global_issue(&mut server, 1, "open", bob, 4);
        commit_global_member(&mut server, 0, row(0), alice, 5);
        commit_global_member(&mut server, 1, row(1), bob, 6);

        let shape = Query::from("issues")
            .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
            .include("assignee")
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();
        let mut peer = PeerState::new();
        let update = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds, ..
        } = &update
        else {
            panic!("expected view update");
        };
        let result_set_tables = result_row_adds
            .iter()
            .map(|(table, _, _)| table.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            result_set_tables,
            BTreeSet::from(["issues", "issue_members", "users"])
        );
        reader.apply_sync_message(update).unwrap();

        let local_rows = reader
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(local_rows, BTreeSet::from([row(0)]));
        let settled_rows = reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(settled_rows, BTreeSet::from([row(0)]));
    }
}
