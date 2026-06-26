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
use crate::protocol::{BindingDelta, ResultRowEntry, ShapeAst, SubscriptionKey};
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, Binding, Include, JoinVia, Operand,
    OrderDirection, Predicate, QUERY_NAMESPACE, ShapeId, ValidatedQuery,
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

struct LoweredQueryClauseOptions {
    tier: DurabilityTier,
    output_fields: Vec<String>,
    retain_params: bool,
    binding_source_shape: String,
    source_overrides: BTreeMap<String, GraphBuilder>,
    table_overrides: BTreeMap<String, TableSchema>,
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
    /// `RegisterShape` / `BindingDelta` a subscriber sent over a connection.
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

    pub(super) fn apply_binding_delta(&mut self, delta: BindingDelta) -> Result<(), Error> {
        let Some(shape) = self.query.registered_shapes.get(&delta.shape_id).cloned() else {
            return Err(Error::InvalidStoredValue("binding delta for unknown shape"));
        };
        for (binding_id, values) in delta.adds {
            if values.len() != shape.params().len() {
                return Err(Error::InvalidStoredValue("binding arity mismatch"));
            }
            let value_map = shape
                .params()
                .keys()
                .cloned()
                .zip(values.iter().cloned())
                .collect::<BTreeMap<_, _>>();
            let binding = shape.bind(value_map)?;
            if binding.binding_id() != binding_id {
                return Err(Error::InvalidStoredValue(
                    "binding id does not match values",
                ));
            }
            self.query
                .registered_bindings
                .entry(delta.shape_id)
                .or_default()
                .insert(binding_id, values);
        }
        for binding_id in delta.removes {
            if let Some(bindings) = self.query.registered_bindings.get_mut(&delta.shape_id) {
                bindings.remove(&binding_id);
            }
            let subscription = SubscriptionKey {
                shape_id: delta.shape_id,
                binding_id,
            };
            self.query.settled_result_sets.remove(&subscription);
        }
        Ok(())
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
        if include_deleted {
            let mut rows =
                self.query_rows_including_deleted_with_lowered_clauses(shape, binding, tier)?;
            let query = shape.query();
            self.apply_include_modes(query, shape.schema_version(), &mut rows, tier, identity)?;
            self.finish_query_rows(query, &mut rows)?;
            return Ok(rows);
        }
        if tier == DurabilityTier::Global
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
        if self.uses_partitioned_or_schema_projected_read(shape) {
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
        self.maintained_view_support(shape, binding, identity)?;
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

    fn apply_query_order(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        sort_current_rows(rows);
        if query.order_by.is_empty() {
            return Ok(());
        }
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
                    left.cell(&table, &order.column),
                    right.cell(&table, &order.column),
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
        let mut context = ViewEvaluationContext::default();
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
            let deleted = deletions.get(&row_uuid).is_some_and(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > version.tx_time()
            });
            rows.push((
                current_row_from_materialized_cells(&read_table, &version, &cells)?,
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
        for join in &shape.query().joins {
            let table = self
                .table_in_schema(&join.table, shape.schema_version())?
                .clone();
            let rows = self.current_rows_for_schema(&join.table, shape.schema_version(), tier)?;
            sources.insert(join.table.clone(), inline_current_graph(&table, rows)?);
            tables.insert(join.table.clone(), table);
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
        for join in &shape.query().joins {
            let table = self.table(&join.table)?.clone();
            let rows = self.tx_current_rows(tx_id, &join.table)?;
            sources.insert(join.table.clone(), inline_current_graph(&table, rows)?);
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
                retain_params: true,
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
                current_row_fields(table),
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
                retain_params: true,
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
        for join in &shape.query().joins {
            let table = self
                .table_in_schema(&join.table, shape.schema_version())?
                .clone();
            let rows =
                self.current_rows_for_schema_at(&join.table, shape.schema_version(), position)?;
            sources.insert(join.table.clone(), inline_current_graph(&table, rows)?);
            tables.insert(join.table.clone(), table);
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
                retain_params: true,
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
        for join in &shape.query().joins {
            let table = self
                .table_in_schema(&join.table, shape.schema_version())?
                .clone();
            let rows = self.current_rows_for_schema(&join.table, shape.schema_version(), tier)?;
            sources.insert(join.table.clone(), inline_current_graph(&table, rows)?);
            tables.insert(join.table.clone(), table);
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
                retain_params: true,
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
        collect_nullable_param_types(table, &query.filters, &mut param_types)?;
        for join in &query.joins {
            let join_table = self.table_in_schema(&join.table, shape.schema_version())?;
            collect_nullable_param_types(&join_table, &join.filters, &mut param_types)?;
        }
        for reachable in &query.reachable {
            if let Operand::Param(param) = &reachable.from {
                param_types.insert(param.clone(), groove::schema::ColumnType::Uuid);
            }
            let access_table =
                self.table_in_schema(&reachable.access_table, shape.schema_version())?;
            collect_nullable_param_types(
                &access_table,
                &reachable.access_filters,
                &mut param_types,
            )?;
            let edge_table = self.table_in_schema(&reachable.edge_table, shape.schema_version())?;
            collect_nullable_param_types(&edge_table, &reachable.edge_filters, &mut param_types)?;
        }
        Ok(param_types)
    }

    fn apply_lowered_query_clauses(
        &self,
        mut graph: GraphBuilder,
        shape: &ValidatedQuery,
        table: &TableSchema,
        param_types: &BTreeMap<String, groove::schema::ColumnType>,
        options: LoweredQueryClauseOptions,
    ) -> Result<GraphBuilder, Error> {
        graph = apply_query_filters(graph, table, &shape.query().filters)?;
        let mut carried_params =
            if options.retain_params && !predicate_params(&shape.query().filters).is_empty() {
                shape.params().keys().cloned().collect()
            } else {
                BTreeSet::new()
            };
        graph = join_params_if_needed(
            graph,
            shape,
            param_types,
            &shape.query().filters,
            options.output_fields.clone(),
            options.retain_params,
            &options.binding_source_shape,
        )?;
        for join in &shape.query().joins {
            let join_table = self.lowered_related_table(&join.table, &options)?;
            let join_key = format!("user_{}", join.on_column);
            let left_key = if let Some(source_column) = &join.source_column {
                format!("user_{source_column}")
            } else {
                "row_uuid".to_owned()
            };
            let mut join_graph = options
                .source_overrides
                .get(&join.table)
                .cloned()
                .unwrap_or_else(|| visible_current_graph(join_table, options.tier));
            if join.source_column.is_none() {
                join_graph = join_graph.unwrap_nullable(join_key.clone());
            } else {
                join_graph = join_graph.filter(PredicateExpr::IsNotNull {
                    field: join_key.clone(),
                });
            }
            join_graph = apply_query_filters(join_graph, join_table, &join.filters)?;
            let join_params =
                if options.retain_params && !predicate_params(&join.filters).is_empty() {
                    shape.params().keys().cloned().collect()
                } else {
                    BTreeSet::new()
                };
            join_graph = join_params_if_needed(
                join_graph,
                shape,
                param_types,
                &join.filters,
                current_row_fields(join_table),
                options.retain_params,
                &options.binding_source_shape,
            )?;
            let params = options.retain_params.then(|| {
                shape.params().keys().map(|param| {
                    if carried_params.contains(param) {
                        ProjectField::renamed(format!("left.{param}"), param.clone())
                    } else {
                        ProjectField::renamed(format!("right.{param}"), param.clone())
                    }
                })
            });
            graph = GraphBuilder::join(graph, join_graph, [left_key], [join_key]).project_fields(
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
            let params = options.retain_params.then(|| {
                param_types.keys().filter_map(|param| {
                    if carried_params.contains(param) {
                        Some(ProjectField::renamed(
                            format!("left.{param}"),
                            param.clone(),
                        ))
                    } else if *param == reachable_seed_param {
                        Some(ProjectField::renamed(
                            format!("right.{param}"),
                            param.clone(),
                        ))
                    } else {
                        None
                    }
                })
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
            if options.retain_params {
                carried_params.insert(reachable_seed_param);
            }
        }
        Ok(graph)
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
                previous_result_set,
                result_row_adds,
                result_row_removes,
                identity,
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
        let (shape, _binding, graph) =
            self.maintained_view_tagged_terminal_graph(shape, binding, identity)?;
        self.materialize_maintained_view_graph(graph, &shape)
    }

    #[cfg(test)]
    pub(crate) fn maintained_view_seed_from_cold_snapshot(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<MaintainedSubscriptionView, Error> {
        let (_subscription, maintained, _transitions, _tables) =
            self.maintained_subscription_view_from_cold_snapshot(shape, binding, identity)?;
        Ok(maintained)
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
        let (_shape, _binding, graph) =
            self.maintained_view_tagged_terminal_graph(shape, binding, identity)?;
        let tables = self.maintained_view_terminal_tables(&_shape)?;
        self.database.flush().map_err(Error::Groove)?;
        let subscription =
            self.subscribe_maintained_view_tagged_graph(&_shape, &_binding, graph)?;
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
        graph: GraphBuilder,
    ) -> Result<groove::ivm::Subscription, Error> {
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
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
        let binding_source_shape = maintained_view_binding_source_shape(shape, binding);
        let graph = rewrite_binding_sources(graph, &binding_source_shape, binding_descriptor);
        let prepared = self.database.prepare(
            graph,
            binding_source_shape,
            binding_descriptor,
            param_names.iter().cloned(),
        )?;
        let values = binding_values_for_plan(binding, &param_names, &param_type_list)?;
        self.database
            .bind_shape(prepared.id(), &values)
            .map_err(Error::Groove)
    }

    pub(crate) fn maintained_view_tagged_terminal_graph(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, GraphBuilder), Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        self.ensure_maintained_view_query_slice(shape.query())?;
        let shape = maintained_view_bind_filter_literals(&shape, &binding, &self.catalogue.schema)?;
        let terminal_tables = self.maintained_view_terminal_tables(&shape)?;
        let result_current =
            self.maintained_view_result_closure_graph(&shape, identity, &terminal_tables)?;
        let mut graphs = vec![result_current];
        for table in terminal_tables.values() {
            let policy_shape = self.maintained_view_table_policy_shape(table, identity)?;
            let (version_content, version_deletion) = self
                .maintained_view_policy_readable_version_tagged_graphs(
                    table,
                    &policy_shape,
                    terminal_tables.values(),
                )?;
            let (replacement_content, replacement_deletion) = self
                .maintained_view_replacement_tagged_graphs(
                    table,
                    &policy_shape,
                    terminal_tables.values(),
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
                    )?;
                let (replacement_content, replacement_deletion) = self
                    .reachable_replacement_tagged_graphs(
                        &shape,
                        reachable,
                        table,
                        terminal_tables.values(),
                    )?;
                graphs.extend([
                    version_content,
                    version_deletion,
                    replacement_content,
                    replacement_deletion,
                ]);
            }
        }
        let param_types = graph_param_types(&shape, &self.catalogue.schema)?;
        let graph = append_maintained_view_binding_params_for_routing(
            GraphBuilder::union(graphs),
            &shape,
            &param_types,
            terminal_tables.values(),
        );
        Ok((shape, binding, graph))
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
            let join_table = self.table(&join.table)?.clone();
            tables.insert(join_table.name.clone(), join_table.clone());
            for target in join_table.references.values() {
                let table = self.table(target)?.clone();
                tables.insert(table.name.clone(), table);
            }
        }
        for reachable in &query.reachable {
            let access_table = self.table(&reachable.access_table)?.clone();
            tables.insert(access_table.name.clone(), access_table);
            let edge_table = self.table(&reachable.edge_table)?.clone();
            tables.insert(edge_table.name.clone(), edge_table);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn supported_maintained_view(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> bool {
        self.maintained_view_support(shape, binding, identity)
            .is_ok()
    }

    pub(crate) fn maintained_view_support(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<(), Error> {
        self.ensure_maintained_view_query_slice(shape.query())?;
        let tables = self.maintained_view_terminal_tables(shape)?;
        for table in tables.values() {
            let policy_shape = self.maintained_view_table_policy_shape(table, identity)?;
            self.ensure_maintained_view_query_slice(policy_shape.query())?;
        }
        self.maintained_view_tagged_terminal_graph(shape, binding, identity)?;
        Ok(())
    }

    fn maintained_view_table_policy_shape(
        &self,
        table: &TableSchema,
        identity: AuthorId,
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
        let policy_shape = maintained_view_bind_filter_literals(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
        )?;
        Ok(policy_shape)
    }

    fn maintained_view_policy_readable_version_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
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
        ));

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
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "version_deletion",
                "left.",
                terminal_tables,
            ));

        Ok((content, deletion))
    }

    fn maintained_view_result_closure_graph(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<GraphBuilder, Error> {
        let root_table = self.table(&shape.query().table)?.clone();
        let root_current = self.maintained_view_bound_query_current_graph(shape)?;
        let result_current = self.maintained_view_filter_result_current_by_include_modes(
            root_current.clone(),
            &root_table,
            shape,
            identity,
        )?;
        let result_current = apply_maintained_view_result_limit(result_current, shape.query());
        let mut graphs =
            vec![
                result_current
                    .clone()
                    .project_fields(maintained_view_tagged_content_fields(
                        &root_table,
                        "result_current",
                        "",
                        terminal_tables.values(),
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
            )?);
        }

        for include in &shape.query().includes {
            graphs.extend(self.maintained_view_include_result_graphs(
                result_current.clone(),
                &root_table,
                include,
                identity,
                terminal_tables,
            )?);
        }

        for join in &shape.query().joins {
            let join_table = self.table(&join.table)?.clone();
            let join_current = self.maintained_view_join_closure_current_graph(
                root_current.clone(),
                &root_table,
                join,
                identity,
            )?;
            graphs.push(join_current.clone().project_fields(
                maintained_view_tagged_content_fields(
                    &join_table,
                    "result_current",
                    "",
                    terminal_tables.values(),
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
    ) -> Result<GraphBuilder, Error> {
        self.filter_root_current_by_required_include_modes(
            root,
            root_table,
            &shape.query().includes,
            identity,
            maintained_view_version_fields(root_table),
        )
    }

    fn filter_root_current_by_required_include_modes(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        includes: &[Include],
        identity: AuthorId,
        root_fields: Vec<String>,
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
    ) -> Result<GraphBuilder, Error> {
        let segments = include.path.split('.').collect::<Vec<_>>();
        let mut current = root.clone();
        let mut current_table = root_table.clone();
        for (idx, segment) in segments.iter().enumerate() {
            let target_table_name = current_table
                .references
                .get(*segment)
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?
                .clone();
            let target_table = self.table(&target_table_name)?.clone();
            let target =
                self.maintained_view_policy_readable_current_graph(&target_table, identity)?;
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
            current = GraphBuilder::join(source, target, [source_key], ["row_uuid"])
                .project_fields(fields);
            current_table = target_table;
        }
        Ok(
            GraphBuilder::join(root, current, ["row_uuid"], ["row_uuid"]).project_fields(
                root_fields
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
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
    ) -> Result<GraphBuilder, Error> {
        let policy_shape = self.maintained_view_table_policy_shape(table, identity)?;
        let graph = self.maintained_view_content_current_with_version(table)?;
        self.apply_maintained_view_filters(
            graph,
            &policy_shape,
            table,
            maintained_view_version_fields(table),
        )
    }

    fn maintained_view_reference_result_graph(
        &self,
        source: GraphBuilder,
        source_column: &str,
        target_table: &TableSchema,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
        unwrap_source: bool,
    ) -> Result<GraphBuilder, Error> {
        let target = self.maintained_view_policy_readable_current_graph(target_table, identity)?;
        let source_key = format!("user_{source_column}");
        let source = if unwrap_source {
            source.unwrap_nullable(source_key.clone())
        } else {
            source
        };
        Ok(
            GraphBuilder::join(source, target, [source_key], ["row_uuid"]).project_fields(
                maintained_view_tagged_content_fields(
                    target_table,
                    "result_current",
                    "right.",
                    terminal_tables.values(),
                ),
            ),
        )
    }

    fn maintained_view_include_result_graphs(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        include: &Include,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<Vec<GraphBuilder>, Error> {
        let mut graphs = Vec::new();
        let mut current = root;
        let mut current_table = root_table.clone();
        for segment in include.path.split('.') {
            let target_table_name = current_table
                .references
                .get(segment)
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?
                .clone();
            let target_table = self.table(&target_table_name)?.clone();
            let target =
                self.maintained_view_policy_readable_current_graph(&target_table, identity)?;
            let source_key = format!("user_{segment}");
            let source = current.unwrap_nullable(source_key.clone());
            current = GraphBuilder::join(source, target, [source_key], ["row_uuid"])
                .project_fields(
                    maintained_view_version_fields(&target_table)
                        .into_iter()
                        .map(|field| ProjectField::renamed(format!("right.{field}"), field)),
                );
            graphs.push(
                current
                    .clone()
                    .project_fields(maintained_view_tagged_content_fields(
                        &target_table,
                        "result_current",
                        "",
                        terminal_tables.values(),
                    )),
            );
            current_table = target_table;
        }
        Ok(graphs)
    }

    fn maintained_view_join_closure_current_graph(
        &self,
        root: GraphBuilder,
        _root_table: &TableSchema,
        join: &JoinVia,
        identity: AuthorId,
    ) -> Result<GraphBuilder, Error> {
        let join_table = self.table(&join.table)?.clone();
        let mut join_query = crate::query::Query::from(join.table.as_str());
        for predicate in &join.filters {
            join_query = join_query.filter(predicate.clone());
        }
        let join_shape = join_query.validate(&self.catalogue.schema)?;
        let join_shape =
            self.maintained_view_bind_filter_literals_for_empty_binding(&join_shape)?;
        let join_current = self.apply_maintained_view_policy_to_current_graph(
            self.maintained_view_content_current_with_version(&join_table)?,
            &join_table,
            &join_shape,
            identity,
            maintained_view_version_fields(&join_table),
        )?;
        let left_key = join
            .source_column
            .as_ref()
            .map(|column| format!("user_{column}"))
            .unwrap_or_else(|| "row_uuid".to_owned());
        let join_key = format!("user_{}", join.on_column);
        let joined = if join.source_column.is_none() {
            let eligible = GraphBuilder::join(
                root.project(["row_uuid"]),
                join_current.clone().unwrap_nullable(join_key.clone()),
                [left_key],
                [join_key],
            )
            .project_fields([ProjectField::renamed("right.row_uuid", "row_uuid")]);
            GraphBuilder::join(join_current, eligible, ["row_uuid"], ["row_uuid"]).project_fields(
                maintained_view_version_fields(&join_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
            )
        } else {
            GraphBuilder::join(root, join_current, [left_key], [join_key]).project_fields(
                maintained_view_version_fields(&join_table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(format!("right.{field}"), field)),
            )
        };
        Ok(joined)
    }

    fn maintained_view_bind_filter_literals_for_empty_binding(
        &self,
        shape: &ValidatedQuery,
    ) -> Result<ValidatedQuery, Error> {
        let binding = shape.bind(BTreeMap::new())?;
        maintained_view_bind_filter_literals(shape, &binding, &self.catalogue.schema)
    }

    fn apply_maintained_view_policy_to_current_graph(
        &self,
        graph: GraphBuilder,
        table: &TableSchema,
        shape: &ValidatedQuery,
        identity: AuthorId,
        output_fields: Vec<String>,
    ) -> Result<GraphBuilder, Error> {
        let (policy_shape, policy_binding) =
            self.policy_composed_shape_binding(shape, &shape.bind(BTreeMap::new())?, identity)?;
        let policy_shape = maintained_view_bind_filter_literals(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
        )?;
        self.apply_maintained_view_filters(graph, &policy_shape, table, output_fields)
    }

    fn maintained_view_replacement_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
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
        ));

        let readable_current = self
            .apply_maintained_view_filters(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(current_row_fields(table)),
                policy_shape,
                table,
                current_row_fields(table),
            )?
            .project(["row_uuid"]);
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
        .project_fields([ProjectField::renamed("left.row_uuid", "row_uuid")]);
        Ok(GraphBuilder::join(
            edge_current,
            reachable_edge_keys,
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            maintained_view_version_fields(edge_table)
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
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
            reachable_graphs.closure.project(["reachable_team"]),
            [query_field(&reachable.access_team_column)],
            ["reachable_team".to_owned()],
        )
        .project_fields([ProjectField::renamed("left.row_uuid", "row_uuid")]);
        Ok(GraphBuilder::join(
            access_current,
            reachable_access_keys,
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            maintained_view_version_fields(access_table)
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
        ))
    }

    pub(crate) fn reachable_policy_readable_version_tagged_graphs<'a>(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
        table: &TableSchema,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        self.ensure_reachable_constituent_table(reachable, table)?;
        let readable_current = match table.name.as_str() {
            name if name == reachable.edge_table => {
                self.reachable_edge_constituent_current_graph(shape, reachable)?
            }
            name if name == reachable.access_table => {
                self.reachable_access_constituent_current_graph(shape, reachable)?
            }
            _ => unreachable!("checked above"),
        }
        .project(["row_uuid"]);
        let content = GraphBuilder::join(
            GraphBuilder::table(history_table_name(&table.name)),
            readable_current.clone(),
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            maintained_view_version_fields(table)
                .into_iter()
                .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
        )
        .project_fields(maintained_view_tagged_content_fields(
            table,
            "version_content",
            "",
            terminal_tables.clone(),
        ));
        let deleted = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion = GraphBuilder::join(deleted, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_tagged_deletion_fields(
                table,
                "version_deletion",
                "left.",
                terminal_tables,
            ));
        Ok((content, deletion))
    }

    pub(crate) fn reachable_replacement_tagged_graphs<'a>(
        &self,
        shape: &ValidatedQuery,
        reachable: &crate::query::ReachableVia,
        table: &TableSchema,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        self.ensure_reachable_constituent_table(reachable, table)?;
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
        ));
        let readable_current = content.clone().project(["row_uuid"]);
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
        let param_types = graph_param_types(shape, &self.catalogue.schema)?;
        self.apply_lowered_query_clauses(
            graph,
            shape,
            table,
            &param_types,
            LoweredQueryClauseOptions {
                tier: DurabilityTier::Global,
                output_fields,
                retain_params: false,
                binding_source_shape: query_binding_source_shape(shape),
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
        Ok(GraphBuilder::join(
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
            ProjectField::renamed("right.team", reachable_graphs.seed_param),
        ]))
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
        shape: &ValidatedQuery,
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
        let seed_param = match &reachable.from {
            Operand::Param(param) => param.clone(),
            Operand::Literal(Value::Uuid(_)) => "__reachable_seed".to_owned(),
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
        };
        let seed = match &reachable.from {
            Operand::Param(param) => GraphBuilder::binding_source(
                binding_source_shape.to_owned(),
                RecordDescriptor::new(
                    param_types
                        .iter()
                        .map(|(name, column_type)| (name.clone(), column_type.value_type())),
                ),
            )
            .project_fields([
                ProjectField::renamed(param.clone(), "team"),
                ProjectField::renamed(param.clone(), "reachable_team"),
            ]),
            Operand::Literal(Value::Uuid(seed)) => {
                let access_seed = current_source_graph(access_table, tier, source_overrides)
                    .unwrap_nullable(query_field(&reachable.access_team_column))
                    .filter(PredicateExpr::eq(
                        query_field(&reachable.access_team_column),
                        Value::Uuid(*seed),
                    ))
                    .project_fields([
                        ProjectField::renamed(query_field(&reachable.access_team_column), "team"),
                        ProjectField::renamed(
                            query_field(&reachable.access_team_column),
                            "reachable_team",
                        ),
                    ]);
                let edge_seed = current_source_graph(edge_table, tier, source_overrides)
                    .unwrap_nullable(query_field(&reachable.edge_member_column))
                    .filter(PredicateExpr::eq(
                        query_field(&reachable.edge_member_column),
                        Value::Uuid(*seed),
                    ))
                    .project_fields([
                        ProjectField::renamed(query_field(&reachable.edge_member_column), "team"),
                        ProjectField::renamed(
                            query_field(&reachable.edge_member_column),
                            "reachable_team",
                        ),
                    ]);
                GraphBuilder::union([access_seed, edge_seed])
            }
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
        };
        let frontier = GraphBuilder::frontier_source("reachable_frontier", team_desc);
        let mut edge_graph = current_source_graph(edge_table, tier, source_overrides)
            .unwrap_nullable(query_field(&reachable.edge_member_column))
            .unwrap_nullable(query_field(&reachable.edge_parent_column));
        edge_graph = apply_query_filters(edge_graph, edge_table, &reachable.edge_filters)?;
        edge_graph = join_params_if_needed(
            edge_graph,
            shape,
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
        access_graph = apply_query_filters(access_graph, access_table, &reachable.access_filters)?;
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
        query.filters.extend(
            policy
                .filters
                .into_iter()
                .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims)),
        );
        query.joins.extend(policy.joins.into_iter().map(|join| {
            JoinVia {
                table: join.table,
                on_column: join.on_column,
                source_column: join.source_column,
                filters: join
                    .filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect(),
            }
        }));
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
                reachable
            }));
        query.includes.extend(policy.includes);
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
        let Some(policy) = self.table(table)?.write_policy.clone() else {
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
            .map(|join| JoinVia {
                table: join.table,
                on_column: join.on_column,
                source_column: join.source_column,
                filters: join
                    .filters
                    .into_iter()
                    .map(|predicate| rewrite_claim_predicate_for_binding(predicate, claims))
                    .collect(),
            })
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
    query.aggregate.is_none()
        && maintained_view_window_supported(query)
        && !query_has_unsupported_param_disjunction(query)
}

fn maintained_view_window_supported(query: &crate::query::Query) -> bool {
    if query.order_by.is_empty() {
        query.offset == 0 && (query.limit.is_none() || query.limit == Some(1))
    } else {
        query.limit.is_some()
    }
}

fn query_has_unsupported_param_disjunction(query: &crate::query::Query) -> bool {
    query
        .filters
        .iter()
        .any(predicate_has_unsupported_param_disjunction)
        || query.joins.iter().any(|join| {
            join.filters
                .iter()
                .any(predicate_has_unsupported_param_disjunction)
        })
        || query.reachable.iter().any(|reachable| {
            reachable
                .access_filters
                .iter()
                .any(predicate_has_unsupported_param_disjunction)
                || reachable
                    .edge_filters
                    .iter()
                    .any(predicate_has_unsupported_param_disjunction)
        })
}

fn predicate_has_unsupported_param_disjunction(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Any(_) if predicate_has_param(predicate) => true,
        Predicate::In(_, _) if predicate_has_param(predicate) => true,
        Predicate::All(predicates) | Predicate::Any(predicates) => predicates
            .iter()
            .any(predicate_has_unsupported_param_disjunction),
        Predicate::Not(predicate) => predicate_has_unsupported_param_disjunction(predicate),
        _ => false,
    }
}

fn apply_maintained_view_result_limit(
    graph: GraphBuilder,
    query: &crate::query::Query,
) -> GraphBuilder {
    if !query.order_by.is_empty() {
        if let Some(limit) = query.limit {
            let order_cols = query.order_by.iter().map(|order| {
                let field = format!("user_{}", order.column);
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
                limit,
            );
        }
        return graph;
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
    matches!(operand, Operand::Claim(name) if name != "sub" && name != "isAdmin" && !claims.is_some_and(|claims| claims.contains_key(name)))
}

fn insert_claim_bindings(
    values: &mut BTreeMap<String, Value>,
    params: &BTreeMap<String, ColumnType>,
    identity: AuthorId,
    claims: Option<&BTreeMap<String, Value>>,
) {
    let sub = claim_param_name("sub");
    if params.contains_key(&sub) {
        values.insert(sub.clone(), Value::Uuid(identity.0));
    }
    let is_admin = claim_param_name("isAdmin");
    if params.contains_key(&is_admin) {
        values.insert(
            is_admin.clone(),
            claims
                .and_then(|claims| claims.get("isAdmin"))
                .cloned()
                .unwrap_or(Value::Bool(false)),
        );
    }
    if let Some(claims) = claims {
        for (name, value) in claims {
            let param = claim_param_name(name);
            if params.contains_key(&param) && param != sub && param != is_admin {
                values.insert(param, value.clone());
            }
        }
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
        let mut current_row = row;
        for segment in &self.segments {
            let Some(Value::Uuid(target_uuid)) = current_row.cell_at(segment.column_position)
            else {
                return false;
            };
            let Some(next_row) = modes.row(&segment.target_table, RowUuid(target_uuid)) else {
                return false;
            };
            current_row = next_row;
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
        Predicate::Any(predicates) => {
            if predicate_has_param(predicate) {
                return Err(Error::InvalidStoredValue(
                    "unsupported query predicate shape",
                ));
            }
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
            for value in values {
                let Operand::Literal(value) = value else {
                    return Err(Error::InvalidStoredValue(
                        "unsupported query predicate shape",
                    ));
                };
                residual.push(PredicateExpr::eq(
                    query_field(column),
                    nullable_cell_value(table, column, value.clone())?,
                ));
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
        Predicate::Contains(Operand::Column(column), Operand::Param(_))
        | Predicate::Eq(Operand::Column(column), Operand::Param(_))
        | Predicate::Eq(Operand::Param(_), Operand::Column(column))
        | Predicate::Ne(Operand::Column(column), Operand::Param(_))
        | Predicate::Ne(Operand::Param(_), Operand::Column(column)) => {
            let _ = table_column_type(table, column)?;
            Ok(LoweredMaintainedPredicate::AlwaysTrue)
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

fn join_params_if_needed(
    graph: GraphBuilder,
    shape: &ValidatedQuery,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    predicates: &[Predicate],
    output_fields: Vec<String>,
    keep_params: bool,
    binding_source_shape: &str,
) -> Result<GraphBuilder, Error> {
    let mut pairs = BTreeSet::new();
    let mut contains_params = BTreeSet::new();
    let mut neq_params = BTreeSet::new();
    for predicate in predicates {
        collect_param_join(predicate, &mut pairs, &mut contains_params, &mut neq_params)?;
    }
    if pairs.is_empty() && contains_params.is_empty() && neq_params.is_empty() {
        return Ok(graph);
    }
    const PARAM_ROUTING_JOIN: &str = "__jazz_param_binding_join";
    let uses_routing_join = pairs.is_empty();
    let (left_on, right_on): (Vec<_>, Vec<_>) = if uses_routing_join {
        (
            vec![PARAM_ROUTING_JOIN.to_owned()],
            vec![PARAM_ROUTING_JOIN.to_owned()],
        )
    } else {
        pairs.into_iter().unzip()
    };
    let graph = if uses_routing_join {
        graph.project_fields(
            output_fields
                .iter()
                .cloned()
                .map(ProjectField::named)
                .chain([ProjectField::literal(PARAM_ROUTING_JOIN, Value::U8(0))]),
        )
    } else {
        graph
    };
    let mut binding = GraphBuilder::binding_source(
        binding_source_shape.to_owned(),
        RecordDescriptor::new(
            param_types
                .iter()
                .map(|(name, column_type)| (name.clone(), column_type.value_type())),
        ),
    );
    if uses_routing_join {
        binding = binding.project_fields(
            param_types
                .keys()
                .cloned()
                .map(ProjectField::named)
                .chain([ProjectField::literal(PARAM_ROUTING_JOIN, Value::U8(0))]),
        );
    }
    let mut joined = GraphBuilder::join(binding, graph, left_on, right_on).project_fields(
        output_fields
            .iter()
            .cloned()
            .map(|field| ProjectField::renamed(format!("right.{field}"), field))
            .chain(
                shape
                    .params()
                    .keys()
                    .cloned()
                    .map(|param| ProjectField::renamed(format!("left.{param}"), param)),
            ),
    );
    if !contains_params.is_empty() || !neq_params.is_empty() {
        joined =
            joined.filter(
                PredicateExpr::And(
                    contains_params
                        .into_iter()
                        .map(|(field, param)| PredicateExpr::ContainsField {
                            field,
                            needle_field: param,
                        })
                        .chain(neq_params.into_iter().map(|(field, param)| {
                            PredicateExpr::NeqField {
                                field,
                                value_field: param,
                            }
                        }))
                        .collect(),
                )
                .canonicalize(),
            );
    }
    Ok(
        joined.project_fields(output_fields.into_iter().map(ProjectField::named).chain(
            if keep_params {
                shape
                    .params()
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

fn collect_param_join(
    predicate: &Predicate,
    pairs: &mut BTreeSet<(String, String)>,
    contains_params: &mut BTreeSet<(String, String)>,
    neq_params: &mut BTreeSet<(String, String)>,
) -> Result<(), Error> {
    match predicate {
        Predicate::All(predicates) => {
            for predicate in predicates {
                collect_param_join(predicate, pairs, contains_params, neq_params)?;
            }
        }
        Predicate::Any(_) if predicate_has_param(predicate) => {
            return Err(Error::InvalidStoredValue(
                "unsupported query predicate shape",
            ));
        }
        Predicate::Any(predicates) => {
            for predicate in predicates {
                collect_param_join(predicate, pairs, contains_params, neq_params)?;
            }
        }
        Predicate::Not(predicate) => {
            collect_param_join(predicate, pairs, contains_params, neq_params)?
        }
        Predicate::In(_, _) if predicate_has_param(predicate) => {
            return Err(Error::InvalidStoredValue(
                "unsupported query predicate shape",
            ));
        }
        Predicate::Eq(Operand::Column(column), Operand::Param(param))
        | Predicate::Eq(Operand::Param(param), Operand::Column(column)) => {
            pairs.insert((param.clone(), query_field(column)));
        }
        Predicate::Ne(Operand::Column(column), Operand::Param(param))
        | Predicate::Ne(Operand::Param(param), Operand::Column(column)) => {
            neq_params.insert((query_field(column), param.clone()));
        }
        Predicate::Contains(Operand::Column(column), Operand::Param(param)) => {
            contains_params.insert((query_field(column), param.clone()));
        }
        Predicate::Eq(_, _)
        | Predicate::Ne(_, _)
        | Predicate::In(_, _)
        | Predicate::Gt(_, _)
        | Predicate::Gte(_, _)
        | Predicate::Lt(_, _)
        | Predicate::Lte(_, _)
        | Predicate::Contains(_, _)
        | Predicate::IsNull(_) => {}
    }
    Ok(())
}

fn reachable_seed_param(reachable: &crate::query::ReachableVia) -> Result<String, Error> {
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

fn maintained_view_bind_filter_literals(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
) -> Result<ValidatedQuery, Error> {
    let mut query = shape.query().clone();
    query.filters = query
        .filters
        .into_iter()
        .map(|predicate| maintained_view_bind_predicate(predicate, binding))
        .collect::<Result<Vec<_>, _>>()?;
    query.joins = query
        .joins
        .into_iter()
        .map(|mut join| {
            join.filters = join
                .filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(join)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            if matches!(&reachable.from, Operand::Param(name) if name.starts_with(CLAIM_PARAM_PREFIX))
            {
                reachable.from = maintained_view_bind_operand(reachable.from, binding)?;
            }
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(reachable)
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
        .map(|predicate| maintained_view_bind_predicate(predicate, binding))
        .collect::<Result<Vec<_>, _>>()?;
    query.joins = query
        .joins
        .into_iter()
        .map(|mut join| {
            join.filters = join
                .filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(join)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            reachable.from = maintained_view_bind_operand(reachable.from, binding)?;
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(reachable)
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
) -> Result<Predicate, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => Predicate::All(
            predicates
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Any(predicates) => Predicate::Any(
            predicates
                .into_iter()
                .map(|predicate| maintained_view_bind_predicate(predicate, binding))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Not(predicate) => Predicate::Not(Box::new(maintained_view_bind_predicate(
            *predicate, binding,
        )?)),
        Predicate::Eq(left, right) => Predicate::Eq(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::Ne(left, right) => Predicate::Ne(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::In(left, values) => Predicate::In(
            maintained_view_bind_operand(left, binding)?,
            values
                .into_iter()
                .map(|operand| maintained_view_bind_operand(operand, binding))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Gt(left, right) => Predicate::Gt(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::Gte(left, right) => Predicate::Gte(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::Lt(left, right) => Predicate::Lt(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::Lte(left, right) => Predicate::Lte(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::Contains(left, right) => Predicate::Contains(
            maintained_view_bind_operand(left, binding)?,
            maintained_view_bind_operand(right, binding)?,
        ),
        Predicate::IsNull(operand) => {
            Predicate::IsNull(maintained_view_bind_operand(operand, binding)?)
        }
    })
}

fn maintained_view_bind_operand(operand: Operand, binding: &Binding) -> Result<Operand, Error> {
    Ok(match operand {
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
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == query.table)
        .ok_or_else(|| Error::TableNotFound(query.table.clone()))?;
    collect_nullable_param_types(table, &query.filters, &mut param_types)?;
    for join in &query.joins {
        let join_table = schema
            .tables
            .iter()
            .find(|table| table.name == join.table)
            .ok_or_else(|| Error::TableNotFound(join.table.clone()))?;
        collect_nullable_param_types(join_table, &join.filters, &mut param_types)?;
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
        collect_nullable_param_types(access_table, &reachable.access_filters, &mut param_types)?;
        let edge_table = schema
            .tables
            .iter()
            .find(|table| table.name == reachable.edge_table)
            .ok_or_else(|| Error::TableNotFound(reachable.edge_table.clone()))?;
        collect_nullable_param_types(edge_table, &reachable.edge_filters, &mut param_types)?;
    }
    Ok(param_types)
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

fn maintained_view_binding_source_shape(shape: &ValidatedQuery, binding: &Binding) -> String {
    format!(
        "jazz-maintained-view:{}:{}",
        shape.shape_id().0,
        binding.binding_id().0
    )
}

fn append_maintained_view_binding_params_for_routing<'a>(
    graph: GraphBuilder,
    shape: &ValidatedQuery,
    param_types: &BTreeMap<String, groove::schema::ColumnType>,
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
) -> GraphBuilder {
    if param_types.is_empty() {
        return graph;
    }
    const ROUTING_JOIN: &str = "__jazz_maintained_view_binding_join";
    let output_fields = maintained_view_tagged_field_names(terminal_tables);
    let graph = graph.project_fields(
        output_fields
            .iter()
            .cloned()
            .map(ProjectField::named)
            .chain([ProjectField::literal(ROUTING_JOIN, Value::U8(0))]),
    );
    let binding = GraphBuilder::binding_source(
        query_binding_source_shape(shape),
        RecordDescriptor::new(param_types.keys().map(|name| {
            (
                name.clone(),
                param_types
                    .get(name)
                    .expect("graph_param_types includes every shape param")
                    .value_type(),
            )
        })),
    )
    .project_fields(
        param_types
            .keys()
            .cloned()
            .map(ProjectField::named)
            .chain([ProjectField::literal(ROUTING_JOIN, Value::U8(0))]),
    );
    GraphBuilder::join(graph, binding, [ROUTING_JOIN], [ROUTING_JOIN]).project_fields(
        output_fields
            .into_iter()
            .map(|field| ProjectField::renamed(format!("left.{field}"), field))
            .chain(
                param_types
                    .keys()
                    .cloned()
                    .map(|param| ProjectField::renamed(format!("right.{param}"), param)),
            ),
    )
}

fn rewrite_binding_sources(
    graph: GraphBuilder,
    new_shape: &str,
    output: RecordDescriptor,
) -> GraphBuilder {
    match graph {
        GraphBuilder::BindingSource { .. } => {
            GraphBuilder::binding_source(new_shape.to_owned(), output)
        }
        GraphBuilder::Recursive {
            seed,
            step,
            frontier,
            max_iters,
        } => GraphBuilder::Recursive {
            seed: Box::new(rewrite_binding_sources(*seed, new_shape, output)),
            step: Box::new(rewrite_binding_sources(*step, new_shape, output)),
            frontier,
            max_iters,
        },
        GraphBuilder::Filter { input, predicate } => GraphBuilder::Filter {
            input: Box::new(rewrite_binding_sources(*input, new_shape, output)),
            predicate,
        },
        GraphBuilder::UnwrapNullable { input, field } => GraphBuilder::UnwrapNullable {
            input: Box::new(rewrite_binding_sources(*input, new_shape, output)),
            field,
        },
        GraphBuilder::Project { input, fields } => GraphBuilder::Project {
            input: Box::new(rewrite_binding_sources(*input, new_shape, output)),
            fields,
        },
        GraphBuilder::Union { inputs } => GraphBuilder::Union {
            inputs: inputs
                .into_iter()
                .map(|input| rewrite_binding_sources(input, new_shape, output))
                .collect(),
        },
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
        } => GraphBuilder::Join {
            left: Box::new(rewrite_binding_sources(*left, new_shape, output)),
            right: Box::new(rewrite_binding_sources(*right, new_shape, output)),
            left_on,
            right_on,
        },
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
        } => GraphBuilder::AntiJoin {
            left: Box::new(rewrite_binding_sources(*left, new_shape, output)),
            right: Box::new(rewrite_binding_sources(*right, new_shape, output)),
            left_on,
            right_on,
        },
        GraphBuilder::ArgMaxBy {
            input,
            group_cols,
            order_cols,
        } => GraphBuilder::ArgMaxBy {
            input: Box::new(rewrite_binding_sources(*input, new_shape, output)),
            group_cols,
            order_cols,
        },
        other => other,
    }
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
                param_types.insert(param.clone(), column_type.clone().nullable());
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
    param_names
        .iter()
        .zip(param_types)
        .map(|(name, column_type)| {
            let value = binding
                .values()
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
    let _ = table_column_type(table, column)?;
    Ok(Value::Nullable(Some(Box::new(value))))
}

fn table_column_type<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a groove::schema::ColumnType, Error> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|column| &column.column_type)
        .ok_or(Error::InvalidStoredValue("query column was not validated"))
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
            | Predicate::Contains(Operand::Column(_), Operand::Param(param)) => {
                params.insert(param.clone());
            }
            _ => {}
        }
    }
    params
}

fn predicate_has_param(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => {
            predicates.iter().any(predicate_has_param)
        }
        Predicate::Not(predicate) => predicate_has_param(predicate),
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => operand_has_param(left) || operand_has_param(right),
        Predicate::In(left, values) => {
            operand_has_param(left) || values.iter().any(operand_has_param)
        }
        Predicate::IsNull(operand) => operand_has_param(operand),
    }
}

fn operand_has_param(operand: &Operand) -> bool {
    matches!(operand, Operand::Param(_) | Operand::Claim(_))
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
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        _ => None,
    }
}

fn current_row_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| format!("user_{}", column.name)),
    );
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    fields
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
    let mut values = Vec::with_capacity(table.columns.len() + 3);
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
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
            let mut values = Vec::with_capacity(table.columns.len() + 4);
            values.push(Value::Uuid(row.row_uuid().0));
            for column in &table.columns {
                values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
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
    let mut content_fields = vec!["row_uuid".to_owned()];
    content_fields.extend(user_fields.iter().cloned());
    content_fields.push("tx_time".to_owned());
    content_fields.push("tx_node_id".to_owned());
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
            GraphBuilder::table(global_current_table_name(&table.name)).project(content_fields),
            GraphBuilder::table(register_global_current_table_name(&table.name)),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_fields.clone(),
            )
        } else {
            GraphBuilder::table(ahead_current_table_name(&table.name))
                .project(content_fields.clone())
        };
        let ahead_deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
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
                    GraphBuilder::table(global_current_table_name(&table.name))
                        .project(content_fields.clone()),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(content_fields),
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
        .project(["row_uuid"]);
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
                .map(|field| ProjectField::renamed(format!("left.{field}"), field))
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
            .map(|(column_name, column_type)| {
                let field = format!("user_{column_name}");
                if table_columns.contains_key(column_name.as_str()) {
                    ProjectField::renamed(source(&field), field)
                } else {
                    ProjectField::null_typed(
                        field,
                        ValueType::Nullable(Box::new(column_type.value_type())),
                    )
                }
            }),
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
            .map(|column| format!("user_{column}")),
    );
    fields
}

fn maintained_view_tagged_deletion_fields<'a>(
    table: &TableSchema,
    event_kind: &str,
    prefix: &str,
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
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
            .map(|(column_name, column_type)| {
                ProjectField::null_typed(
                    format!("user_{column_name}"),
                    ValueType::Nullable(Box::new(column_type.value_type())),
                )
            }),
    );
    fields
}

fn maintained_view_terminal_user_columns<'a>(
    terminal_tables: impl IntoIterator<Item = &'a TableSchema>,
) -> BTreeMap<String, groove::schema::ColumnType> {
    let mut columns = BTreeMap::new();
    for table in terminal_tables {
        for column in &table.columns {
            columns
                .entry(column.name.clone())
                .or_insert_with(|| column.column_type.clone());
        }
    }
    columns
}

fn query_field(column: &str) -> String {
    format!("user_{column}")
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
    use crate::query::{Aggregate, OrderDirection, Query, col, eq, gt, lit, lte, param};
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
            .apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
                shape_id: shape.shape_id(),
                adds: vec![
                    (
                        alice_binding.binding_id(),
                        alice_binding.values().values().cloned().collect(),
                    ),
                    (
                        bob_binding.binding_id(),
                        bob_binding.values().values().cloned().collect(),
                    ),
                ],
                removes: Vec::new(),
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
            .apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
                shape_id: shape.shape_id(),
                adds: Vec::new(),
                removes: vec![alice_binding.binding_id()],
            }))
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
