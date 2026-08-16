//! Query execution, shape registration, binding routing, and read-set
//! evaluation for `jazz/SPEC/6_queries.md`. This module owns lowering validated Jazz
//! queries to groove plans, evaluating one-shot reads, recording predicate reads,
//! and applying binding deltas; the pure AST lives in [`crate::query`], policy
//! checks in [`super::policy`], and sync view transport in [`super::views`].
//! It is the node layer's query bridge to groove IVM.

use super::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::time::Instant;

use groove::ivm::{LiteralValue, PreparedShapeId, RoutedMultisinkTerminal, StaticScanSpec};
use groove::ivm::{MultisinkDeltas, MultisinkSubscription, RecordDeltas};
use groove::records::{BorrowedRecord, OwnedRecord, RecordDescriptor, ValueType};
use groove::schema::ColumnType;

use super::maintained_subscription_view::{MaintainedSubscriptionView, MaintainedTerminalSchemas};
#[cfg(feature = "testing")]
use super::maintained_subscription_view::{
    MaintainedSubscriptionViewFootprint, MaintainedTerminalSchemasFootprint,
};
use super::query_engine::{
    AggregateExpr as NormalizedAggregateExpr, AggregateFunction as NormalizedAggregateFunction,
    AppProjectionTree, AppRowOutputRequest, AppRowSchema, CapabilityReport, ClaimPath, ClosurePath,
    ClosurePathSegment, ClosureRootGate, ComparisonOp as NormalizedComparisonOp,
    ContentVersionSource, CorrelationRequirement, DataSource, DeletionRegisterSource,
    FieldProjection, FieldRequirement, FrontierId, JoinContribution,
    JoinMode as NormalizedJoinMode, LensSelection, NormalizedRowSetShape, NormalizedShapeIdentity,
    NormalizedValueRef, OrderKey as NormalizedOrderKey, OutputTerminalSchema, OverlayRef,
    OverlayStack, PathCardinality, PathHolePolicy, PayloadProjection, PolicyContext,
    PolicyDecisionRole, PolicyEnforcementMode, PredicateExpr as NormalizedPredicateExpr,
    ProgramBinding, ProgramClaimParam, ProgramFactKey, ProgramOutputSchemas, ProgramPathId,
    ProvenanceField, QueryAuthorizationMode, QueryProgram, QueryProgramRequest, QueryReadSet,
    ReachableContribution, ReadView, RequestedReadSet, RequestedSourceStage, ResolvedSource,
    ResultId, ResultMembershipVersionSchema, ResultRowRef, RowIdRef, RowProjection,
    RowRefSchema as QueryEngineRowRefSchema, RowSetExpr, RowSetNodeId, RowSetOutputRequest,
    RowSetProgramInput, RowVisibility, SchemaFamilySelection, SchemaProjection, SettledBindingRows,
    SortDirection as NormalizedSortDirection, SourceAuthorizationRequest, SourceExpr, SourceGap,
    SourceId, SourceMetadataFields, SourceMetadataRequirement, SourcePath, SourceRequest,
    SourceRequirements, SourceResolutionError, SourceResolver, SourceRole, SourceRowShape,
    StorageSchemaSelection, TypedOutputField, UnionInput, ValueSourceColumn, ValueSourceMode,
    VersionIdentityFields, VersionedRowRefSchema, aggregate_output_app_field,
    aggregate_output_column, aggregate_output_field, claim_param_field,
    claim_path_from_param_field, left_field, lower_query_program, right_field, route_param_field,
    user_column_field,
};
use crate::protocol::{
    AuthorizationOperationKey, AuthorizationScopeOperation, AuthorizationSupportScopeKey,
    BindingViewKey, KnownStateCompleteness, KnownStateDeclaration, PermissionAdviceAction,
    ProgramFactEntry, ReadViewKey, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    RelationEdgeEntry, ResultMemberEntry, ResultMemberPayloadEntry, ResultRowLayer, RowVersionRef,
    RowVersionRefEntry, ShapeAst, ShapeBody, Subscribe, SubscriptionKey, SyntheticReplacementToken,
};
use crate::protocol_limits::MAX_KNOWN_STATE_EXACT_REFS;
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, Predicate, Query as JazzQuery,
    QueryError, ShapeId, ValidatedQuery, binding_id_for_values, relation_query_to_query,
};
use crate::schema::{ColumnSchema, JazzSchema, branch_metadata_table_schema};
use crate::tools::{ObjectId, OutputOccurrenceId};

mod materialization;
mod prepared_bindings;
mod query_read_sets;
mod query_result_rows;

use prepared_bindings::*;
use query_read_sets::*;
use query_result_rows::{
    aggregate_query_row_uuid, aggregate_result_table, aggregate_row_cell, compare_optional_values,
};

#[cfg(test)]
pub(crate) fn exact_known_state_declaration_for_test(
    shape_id: ShapeId,
    subscription: SubscriptionKey,
    values: &[Value],
    refs: Vec<RowVersionRef>,
) -> Option<KnownStateDeclaration> {
    exact_known_state_declaration_if_within_limits(shape_id, subscription, values, refs)
}

pub(crate) const JAZZ_APP_ROWS_SINK: &str = "app_rows";
const PENDING_BINDING_SOURCE_SHAPE: &str = "__jazz_pending_binding_source";

/// The caller-owned meaning of an absent claim while binding a prepared plan.
/// Ordinary prepared queries require all declared bindings. Authorization
/// support, on the other hand, must represent an absent policy claim as an
/// empty proof for the commit's permission subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedClaimBindingMode {
    Strict,
    FailClosedAuthorizationSupport,
}

/// Exact, action-specific policy support compiled for a hypothetical operation.
///
/// The support key deliberately excludes the row/candidate operation key: two
/// operations reuse hydration only when their compiled support is identical.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct AuthorizationSupportScope {
    pub(crate) key: AuthorizationSupportScopeKey,
    pub(crate) operation: AuthorizationOperationKey,
    /// The sole read/serving semantics under which support can authorize an
    /// operation.  A scope must never be satisfied by a branch, snapshot, or
    /// local-tier view that merely happens to have the same query identity.
    pub(crate) options: RegisterShapeOptions,
    pub(crate) subscriptions: Vec<(ValidatedQuery, Binding)>,
}

/// Aggregate terminal membership is structurally identified by the aggregate
/// query plan and its synthetic group-key member. Its table label is not an
/// identity and must not participate in public delivery decisions.
fn is_public_aggregate_result_member(
    member: &ResultMemberEntry,
    _result_table: &str,
    aggregate_query: bool,
) -> bool {
    aggregate_query && matches!(member, ResultMemberEntry::Synthetic { .. })
}

fn replace_stale_authoritative_occurrence_member(
    result_set: &mut BTreeSet<ResultMemberEntry>,
    result_payloads: &mut BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    authoritative_member_adds: &BTreeSet<ResultMemberEntry>,
    member: &ResultMemberEntry,
    result_table: &str,
    aggregate_query: bool,
) -> Result<(), Error> {
    if !authoritative_member_adds.contains(member) {
        return Ok(());
    }
    let Some(occurrence_id) =
        public_result_member_occurrence_id(member, result_table, aggregate_query)?
    else {
        return Ok(());
    };
    let replaced = result_set
        .iter()
        .filter(|candidate| *candidate != member)
        .filter_map(|candidate| {
            public_result_member_occurrence_id(candidate, result_table, aggregate_query)
                .transpose()
                .map(|result| result.map(|candidate_id| (candidate, candidate_id)))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .filter(|(_, candidate_id)| *candidate_id == occurrence_id)
        .map(|(candidate, _)| candidate.clone())
        .collect::<Vec<_>>();
    for replaced in replaced {
        result_set.remove(&replaced);
        result_payloads.remove(&replaced);
    }
    Ok(())
}

fn is_public_result_member(
    member: &ResultMemberEntry,
    result_table: &str,
    aggregate_query: bool,
) -> bool {
    member.table_name() == Some(result_table)
        || is_public_aggregate_result_member(member, result_table, aggregate_query)
}

fn aggregate_result_member_row_uuid(member: &ResultMemberEntry) -> Result<RowUuid, Error> {
    let ResultMemberEntry::Synthetic { table, row, .. } = member else {
        return Err(Error::InvalidStoredValue(
            "aggregate result member must be synthetic",
        ));
    };
    let mut identity = b"jazz:aggregate-result:v1".to_vec();
    identity.extend_from_slice(&(table.len() as u64).to_be_bytes());
    identity.extend_from_slice(table.as_bytes());
    identity.extend_from_slice(&(row.len() as u64).to_be_bytes());
    identity.extend_from_slice(row);
    Ok(RowUuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        &identity,
    )))
}

fn public_result_member_occurrence_id(
    member: &ResultMemberEntry,
    result_table: &str,
    aggregate_query: bool,
) -> Result<Option<OutputOccurrenceId>, Error> {
    if let Some(occurrence) = member.output_occurrence_id() {
        return Ok(Some(occurrence));
    }
    is_public_aggregate_result_member(member, result_table, aggregate_query)
        .then(|| {
            aggregate_result_member_row_uuid(member)
                .map(|row| OutputOccurrenceId::single_source(ObjectId::from_uuid(row.0)))
        })
        .transpose()
}

pub(crate) struct LocalMaintainedViewSubscription {
    subscription: MultisinkSubscription,
    _retained_prepared_plan: Option<SubscriptionPreparedPlan>,
    maintained: MaintainedSubscriptionView,
    terminal_schemas: MaintainedTerminalSchemas,
    tables: BTreeMap<String, TableSchema>,
    result_query: JazzQuery,
    result_table: String,
    result_schema_version: SchemaVersionId,
    binding_view_key: BindingViewKey,
    result_select: Option<Vec<String>>,
    result_set: BTreeSet<ResultMemberEntry>,
    authoritative_result_set: BTreeSet<ResultMemberEntry>,
    authoritative_result_generation: u64,
    result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    program_facts: BTreeSet<ProgramFactEntry>,
    root_occurrence_ids: Vec<OutputOccurrenceId>,
}

impl LocalMaintainedViewSubscription {
    pub(crate) fn terminal_root_layout(&self) -> Option<&crate::db::TerminalRootLayout> {
        self.terminal_schemas.terminal_root_layout()
    }
}

/// A plan retained solely to keep a maintained subscription graph alive.
/// Its provenance is established by the compiler path that produced it, so a
/// caller cannot relabel a ClientLocal plan as TrustedServing after the fact.
pub(crate) struct SubscriptionPreparedPlan {
    plan: PreparedQueryPlanHandle,
    authorization_mode: QueryAuthorizationMode,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg(feature = "testing")]
pub(crate) struct LocalMaintainedViewSubscriptionFootprint {
    pub(crate) maintained: MaintainedSubscriptionViewFootprint,
    pub(crate) terminal_schemas: MaintainedTerminalSchemasFootprint,
    pub(crate) tables: usize,
    pub(crate) result_set: usize,
    pub(crate) result_payloads: usize,
    pub(crate) program_facts: usize,
    pub(crate) control_state_bytes: usize,
    pub(crate) total_heap_bytes: usize,
}

impl LocalMaintainedViewSubscription {
    pub(crate) fn subscription_id(&self) -> groove::ivm::SubscriptionId {
        self.subscription.id()
    }

    pub(crate) fn root_occurrence_ids(&self) -> &[OutputOccurrenceId] {
        &self.root_occurrence_ids
    }

    #[cfg(test)]
    pub(crate) fn retained_plan_authorization_mode(&self) -> Option<QueryAuthorizationMode> {
        self._retained_prepared_plan
            .as_ref()
            .map(|plan| plan.authorization_mode)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn footprint(&self) -> LocalMaintainedViewSubscriptionFootprint {
        let maintained = self.maintained.footprint();
        let terminal_schemas = self.terminal_schemas.footprint();
        let tables_bytes = self
            .tables
            .iter()
            .map(|(name, schema)| name.len() + std::mem::size_of_val(schema))
            .sum::<usize>()
            + self.tables.len() * 96;
        let result_set_bytes = self
            .result_set
            .iter()
            .map(|member| {
                postcard::to_allocvec(member)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>()
            + self.result_set.len() * 64;
        let authoritative_result_set_bytes = self
            .authoritative_result_set
            .iter()
            .map(|member| {
                postcard::to_allocvec(member)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>()
            + self.authoritative_result_set.len() * 64;
        let result_payloads_bytes = self
            .result_payloads
            .iter()
            .map(|(member, payload)| {
                postcard::to_allocvec(member)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
                    + postcard::to_allocvec(payload)
                        .map(|bytes| bytes.len())
                        .unwrap_or(0)
            })
            .sum::<usize>()
            + self.result_payloads.len() * 96;
        let program_facts_bytes = self
            .program_facts
            .iter()
            .map(|fact| {
                postcard::to_allocvec(fact)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>()
            + self.program_facts.len() * 64;
        let control_state_bytes = terminal_schemas.terminal_schemas_bytes
            + tables_bytes
            + self.result_table.len()
            + self
                .result_select
                .as_ref()
                .map(|columns| columns.iter().map(String::len).sum::<usize>())
                .unwrap_or_default()
            + result_set_bytes
            + authoritative_result_set_bytes
            + result_payloads_bytes
            + program_facts_bytes;
        LocalMaintainedViewSubscriptionFootprint {
            maintained,
            terminal_schemas,
            tables: self.tables.len(),
            result_set: self.result_set.len(),
            result_payloads: self.result_payloads.len(),
            program_facts: self.program_facts.len(),
            control_state_bytes,
            total_heap_bytes: maintained.total_heap_bytes + control_state_bytes,
        }
    }
}

pub(crate) fn take_required_sink_deltas(
    mut deltas: MultisinkDeltas,
    sink: &str,
) -> Result<RecordDeltas, Error> {
    deltas.sinks.remove(sink).ok_or({
        Error::InvalidStoredValue("multisink subscription did not deliver required sink")
    })
}

mod lowering;

pub(crate) use lowering::PolicyAuthorizationGraph;
use lowering::*;

pub(crate) struct LocalMaintainedViewSubscriptionUpdate {
    pub(crate) authoritative_membership_changed: bool,
    pub(crate) added: Vec<(OutputOccurrenceId, CurrentRow)>,
    pub(crate) removed: Vec<OutputOccurrenceId>,
    pub(crate) added_edges: Vec<(RelationEdge, Option<CurrentRow>)>,
    pub(crate) removed_edges: Vec<RelationEdge>,
    pub(crate) terminal_operations: Vec<groove::ivm::TerminalOperation>,
    pub(crate) terminal_layout: Option<crate::db::TerminalRootLayout>,
}

enum CurrentQueryProgramOutput {
    AppRows,
    PolicyPredicate,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

mod source_resolution;

use source_resolution::*;

mod normalization;

use normalization::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    fn table_in_schema_or_branch_metadata(
        &self,
        table: &str,
        schema_version: SchemaVersionId,
    ) -> Result<TableSchema, Error> {
        if table == "jazz_branches" {
            Ok(branch_metadata_table_schema())
        } else {
            self.table_in_schema(table, schema_version)
        }
    }

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

    fn program_binding_for_shape(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        source_shape: Option<String>,
        extra_user_params: BTreeMap<String, ColumnType>,
        claim_params: BTreeMap<String, ProgramClaimParam>,
    ) -> ProgramBinding {
        let mut param_types = shape.params().clone();
        param_types.extend(extra_user_params.clone());
        ProgramBinding {
            id: binding.binding_id(),
            source_shape,
            extra_user_params,
            param_types,
            claim_params,
            values: binding.values().clone(),
        }
    }

    fn program_binding_for_shape_and_policy(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        source_shape: Option<String>,
        extra_user_params: BTreeMap<String, ColumnType>,
        claim_params: BTreeMap<String, ProgramClaimParam>,
        policy: &PolicyContext,
    ) -> Result<ProgramBinding, Error> {
        self.program_binding_for_shape_and_policy_with_prepared_claim_mode(
            shape,
            binding,
            source_shape,
            extra_user_params,
            claim_params,
            policy,
            PreparedClaimBindingMode::Strict,
        )
    }

    fn program_binding_for_shape_and_policy_with_prepared_claim_mode(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        source_shape: Option<String>,
        extra_user_params: BTreeMap<String, ColumnType>,
        claim_params: BTreeMap<String, ProgramClaimParam>,
        policy: &PolicyContext,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<ProgramBinding, Error> {
        let mut program_binding = self.program_binding_for_shape(
            shape,
            binding,
            source_shape,
            extra_user_params,
            claim_params.clone(),
        );
        if !claim_params.is_empty() {
            let mut values = binding.values().clone();
            for (name, claim) in &claim_params {
                let Some(value) = prepared_claim_value(&claim.path, policy)? else {
                    if prepared_claim_binding_mode
                        == PreparedClaimBindingMode::FailClosedAuthorizationSupport
                    {
                        return Err(Error::AuthorizationSupportMissingClaim(
                            claim.path.0.join("."),
                        ));
                    }
                    // An absent claim cannot establish a policy proof. Leave
                    // it as a capability gap so the policy source resolver
                    // lowers the proof to an empty authorization graph.
                    if matches!(policy, PolicyContext::AuthorizationSubplan { .. }) {
                        return Err(Error::QueryCapability(format!(
                            "policy authorization requires unbound claim {}",
                            claim.path.0.join(".")
                        )));
                    }
                    return Err(Error::InvalidStoredValue(
                        "claim prepared param is not bound",
                    ));
                };
                values.insert(
                    name.clone(),
                    coerce_prepared_binding_value(value, &claim.ty),
                );
            }
            program_binding.id = binding_id_for_values(&values);
            program_binding.values = values;
        }
        Ok(program_binding)
    }

    pub(super) fn register_shape(&mut self, shape_id: ShapeId, ast: ShapeAst) -> Result<(), Error> {
        if ast.version != ShapeAst::VERSION {
            return Err(Error::InvalidStoredValue("unsupported query AST version"));
        }
        let schema = if ast.schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            let Some(schema) = self.catalogue.catalogue_schemas.get(&ast.schema_version) else {
                self.sync_metrics.parked_catalogue_shapes += 1;
                self.parking
                    .parked_shape_registrations
                    .insert(shape_id, ast);
                return Ok(());
            };
            &schema.schema
        };
        let shape = match &ast.body {
            ShapeBody::Query(query) => {
                query.validate_with_schema_version(schema, ast.schema_version)?
            }
            ShapeBody::Relation(relation) => relation_query_to_query(relation)?
                .validate_with_schema_version(schema, ast.schema_version)?,
        };
        if shape.shape_id() != shape_id {
            return Err(Error::InvalidStoredValue("shape id does not match AST"));
        }
        self.query.registered_shapes.insert(shape_id, shape);
        self.drain_parked_binding_deltas_for_shape(shape_id)?;
        Ok(())
    }

    pub(crate) fn validate_shape_ast_for_registration(
        &self,
        shape_id: ShapeId,
        ast: &ShapeAst,
    ) -> Result<Option<ValidatedQuery>, Error> {
        if ast.version != ShapeAst::VERSION {
            return Err(Error::InvalidStoredValue("unsupported query AST version"));
        }
        let schema = if ast.schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            let Some(schema) = self.catalogue.catalogue_schemas.get(&ast.schema_version) else {
                return Ok(None);
            };
            &schema.schema
        };
        let shape = match &ast.body {
            ShapeBody::Query(query) => {
                query.validate_with_schema_version(schema, ast.schema_version)?
            }
            ShapeBody::Relation(relation) => relation_query_to_query(relation)?
                .validate_with_schema_version(schema, ast.schema_version)?,
        };
        if shape.shape_id() != shape_id {
            return Err(Error::InvalidStoredValue("shape id does not match AST"));
        }
        Ok(Some(shape))
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

    pub(crate) fn register_query_subscription_for_peer(
        &mut self,
        shape_id: ShapeId,
        ast: ShapeAst,
        subscribe: Subscribe,
    ) -> Result<(), Error> {
        self.register_shape(shape_id, ast)?;
        self.apply_subscribe(subscribe)
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
        let binding = shape.bind(value_map)?;
        let binding_view_key = BindingViewKey {
            shape_id: subscribe.shape_id,
            binding_id: binding.binding_id(),
            read_view: subscribe.subscription.read_view,
        };
        if subscribe.known_state.is_some() {
            self.query
                .known_state_declared_binding_views
                .insert(binding_view_key);
        } else {
            self.query
                .known_state_declared_binding_views
                .remove(&binding_view_key);
        }
        self.query
            .registered_bindings
            .entry(subscribe.shape_id)
            .or_default()
            .insert(
                subscribe.subscription.binding_id,
                RegisteredBinding {
                    values: subscribe.values,
                    read_view: subscribe.subscription.read_view,
                    binding_view_key,
                },
            );
        Ok(())
    }

    pub(crate) fn apply_unsubscribe(&mut self, subscription: SubscriptionKey) {
        let binding_view_key = self.binding_view_key_for_subscription(subscription).ok();
        if let Some(bindings) = self
            .query
            .registered_bindings
            .get_mut(&subscription.shape_id)
        {
            bindings.remove(&subscription.binding_id);
        }
        if let Some(binding_view_key) = binding_view_key
            && !self.registered_binding_resolves_to_binding_view_key(binding_view_key)
        {
            self.clear_settled_result_view(binding_view_key);
            self.query.settled_program_facts.remove(&binding_view_key);
            self.query
                .known_state_declared_binding_views
                .remove(&binding_view_key);
            self.query
                .initial_hydration_binding_views
                .remove(&binding_view_key);
            self.query
                .pending_opening_binding_views
                .remove(&binding_view_key);
        }
    }

    fn registered_binding_resolves_to_binding_view_key(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        let Some(bindings) = self
            .query
            .registered_bindings
            .get(&binding_view_key.shape_id)
        else {
            return false;
        };
        bindings.values().any(|registered| {
            if registered.read_view != binding_view_key.read_view {
                return false;
            }
            registered.binding_view_key == binding_view_key
        })
    }

    pub(crate) fn has_settled_result_set(&self, binding_view_key: BindingViewKey) -> bool {
        self.query
            .settled_result_sets
            .contains_key(&binding_view_key)
    }

    pub(crate) fn applied_view_update_generation(&self, binding_view_key: BindingViewKey) -> u64 {
        self.query
            .applied_view_update_generations
            .get(&binding_view_key)
            .copied()
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn reset_subscription_snapshot_for_link_call_count(&mut self) {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(|calls| calls.set(0));
    }

    #[cfg(test)]
    pub(crate) fn subscription_snapshot_for_link_call_count(&self) -> usize {
        SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(std::cell::Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn inject_pending_authoritative_reset_for_test(
        &mut self,
        binding_view_key: BindingViewKey,
        members: impl IntoIterator<Item = ResultMemberEntry>,
        settled_through: GlobalSeq,
    ) {
        self.clear_settled_result_view(binding_view_key);
        for member in members {
            self.insert_settled_result_member_indexed(binding_view_key, member);
        }
        self.query
            .settled_through_by_binding_view
            .insert(binding_view_key, settled_through);
        self.query
            .pending_authoritative_reset_binding_views
            .insert(binding_view_key);
    }

    pub(crate) fn take_pending_authoritative_reset_binding_views(
        &mut self,
    ) -> BTreeSet<BindingViewKey> {
        std::mem::take(&mut self.query.pending_authoritative_reset_binding_views)
    }

    pub(crate) fn take_pending_terminal_operations(
        &mut self,
        binding_view_key: BindingViewKey,
    ) -> Vec<groove::ivm::TerminalOperation> {
        self.query
            .pending_terminal_operations_by_binding_view
            .remove(&binding_view_key)
            .unwrap_or_default()
    }

    pub(crate) fn defer_authoritative_reset_for_binding_view(
        &mut self,
        binding_view_key: BindingViewKey,
    ) {
        self.query
            .pending_authoritative_reset_binding_views
            .insert(binding_view_key);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_authoritative_reset_for_test(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .pending_authoritative_reset_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn publication_deferred_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .deferred_publication_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn opening_pending_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        self.query
            .pending_opening_binding_views
            .contains(&binding_view_key)
    }

    pub(crate) fn settled_result_transitions_for_subscription(
        &self,
        subscription: SubscriptionKey,
        previous_member_result_set: &BTreeSet<ResultMemberEntry>,
        previous_program_fact_set: &BTreeSet<ProgramFactEntry>,
        result_table_filter: Option<&str>,
        output_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        let binding_view_key = self.binding_view_key_for_subscription(subscription)?;
        // Settled binding views are shared by canonical query binding, while a
        // table read policy is identity-scoped. Never relay a synthetic
        // aggregate from that shared cache across an identity boundary; the
        // per-peer maintained program remains the authority for policy-shaped
        // aggregate output.
        let shared_view_has_read_policy = self
            .query
            .registered_shapes
            .get(&subscription.shape_id)
            .and_then(|shape| self.table(shape.query().table.as_str()).ok())
            .is_some_and(|table| table.read_policy.is_some());
        let Some(settled_members) = self.query.settled_result_sets.get(&binding_view_key) else {
            return Ok(None);
        };
        let settled_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        let member_is_visible = |member: &ResultMemberEntry| {
            let Some(table_name) = member.table_name() else {
                return false;
            };
            result_table_filter.is_none_or(|table| table_name == table)
                && (output_tables.contains_key(table_name)
                    || (matches!(member, ResultMemberEntry::Synthetic { .. })
                        && !shared_view_has_read_policy))
        };
        let current = settled_members
            .iter()
            .filter(|member| member_is_visible(member))
            .cloned()
            .collect::<BTreeSet<_>>();
        let previous = previous_member_result_set
            .iter()
            .filter(|member| member_is_visible(member))
            .cloned()
            .collect::<BTreeSet<_>>();
        let fact_is_visible = |fact: &ProgramFactEntry| match fact {
            ProgramFactEntry::ResultPayload(payload) => member_is_visible(&payload.member),
            _ => true,
        };
        let current_facts = settled_facts
            .into_iter()
            .filter(fact_is_visible)
            .collect::<BTreeSet<_>>();
        let previous_facts = previous_program_fact_set
            .iter()
            .filter(|fact| fact_is_visible(fact))
            .cloned()
            .collect::<BTreeSet<_>>();
        let program_fact_adds = current_facts
            .difference(&previous_facts)
            .cloned()
            .collect::<Vec<_>>();
        let program_fact_removes = previous_facts
            .difference(&current_facts)
            .cloned()
            .collect::<Vec<_>>();
        // A synthetic aggregate member is meaningful only together with its
        // payload fact. In particular, an empty aggregate has a member and a
        // payload whose aggregate field is `Nullable(None)`; it is not a
        // member with a missing payload. Carry both representations through
        // the settled-view handoff so facade materialization can retain that
        // distinction.
        let result_payload_adds = program_fact_adds
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect();
        let result_payload_removes = program_fact_removes
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                _ => None,
            })
            .collect();
        Ok(Some(
            super::maintained_subscription_view::ResultTransitions {
                authoritative_membership_changed: false,
                authoritative_member_adds: BTreeSet::new(),
                adds: current.difference(&previous).cloned().collect(),
                removes: previous.difference(&current).cloned().collect(),
                result_payload_adds,
                result_payload_removes,
                program_fact_adds,
                program_fact_removes,
                structured_app_row_changes: BTreeSet::new(),
                allow_storage_witness_fallback: true,
                observed_result_delta_batches: 0,
                requires_authoritative_membership_reconcile: false,
                terminal_operations: Vec::new(),
            },
        ))
    }

    pub(crate) fn authoritative_reset_snapshot_for_binding_view(
        &mut self,
        shape: &ValidatedQuery,
        binding_view_key: BindingViewKey,
    ) -> Result<Option<RelationSnapshot>, Error> {
        let Some(result_members) = self
            .query
            .settled_result_sets
            .get(&binding_view_key)
            .cloned()
        else {
            return Ok(None);
        };
        let program_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        let result_payloads = program_facts
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload) => {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();

        let result_table = shape.query().table.as_str();
        let mut rows = Vec::new();
        let mut row_keys = BTreeSet::new();
        for member in result_members.iter().filter(|member| {
            is_public_result_member(member, result_table, shape.query().aggregate.is_some())
        }) {
            let Some(row) = self.materialize_authoritative_reset_member(
                shape.query(),
                member,
                &result_payloads,
            )?
            else {
                continue;
            };
            row_keys.insert((row.table().to_owned(), row.row_uuid()));
            rows.push(row);
        }
        // Result-member ordering is for identity and deduplication, not public
        // query rank. Membership/windowing is already lowered; only restore the
        // selected roots to their advertised order before sending a reset.
        self.apply_query_order_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        if shape.query().flat_join.is_none() {
            self.apply_projection_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        }
        let root_count = rows.len();
        let mut edges = Vec::new();
        for fact in program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            // Program facts retain canonical authored identity.  The public
            // relation snapshot, including its removal index, is keyed in the
            // subscription read schema; project the edge identity alongside
            // the row it references rather than mixing canonical `users`
            // with a materialized `people` row.
            let read_edge =
                self.project_relation_edge_through_read_schema(&edge, shape.schema_version())?;
            if row_keys.insert((read_edge.target_table.clone(), read_edge.target_row))
                && let Some(version) = &edge.target_version
                && let Some(row) = self.materialize_authoritative_reset_relation_edge_target(
                    shape.schema_version(),
                    edge.target_table.as_str(),
                    edge.target_row,
                    version,
                )?
            {
                rows.push(row);
            }
            edges.push(read_edge);
        }
        Ok(Some(RelationSnapshot {
            root_count,
            rows,
            edges,
        }))
    }

    pub(crate) fn settled_through_for_binding_view(
        &self,
        binding_view_key: BindingViewKey,
    ) -> Option<GlobalSeq> {
        self.query
            .settled_through_by_binding_view
            .get(&binding_view_key)
            .copied()
    }

    pub(crate) fn known_state_declaration_for_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        values: &[Value],
        identity: AuthorId,
    ) -> Result<Option<KnownStateDeclaration>, Error> {
        let binding_view_key = BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: subscription.read_view,
        };
        if !self.has_settled_result_set(binding_view_key) {
            let _ = self.load_known_state_fact(binding_view_key)?;
            // Slow exact declarations are still known-state declarations: they
            // must describe a binding view the server has previously settled
            // for this client. A purely local first subscription could include
            // rows the serving peer has not observed yet; truncating that to an
            // exact set would silently overclaim and can make stale rehydrate
            // responses suppress local live state.
            return Ok(None);
        }
        if let Some(position) = self.settled_through_for_binding_view(binding_view_key) {
            let authorization_progress = self
                .query
                .authorization_progress_by_binding_view
                .get(&binding_view_key)
                .copied();
            return Ok(Some(match authorization_progress {
                Some(authorization_progress) => {
                    KnownStateDeclaration::FastWithAuthorizationProgress {
                        completeness: KnownStateCompleteness::FastCurrentMembership,
                        position,
                        authorization_progress,
                    }
                }
                None => KnownStateDeclaration::Fast {
                    completeness: KnownStateCompleteness::FastCurrentMembership,
                    position,
                },
            }));
        }
        if let Some(position) = self.load_known_state_fact(binding_view_key)? {
            return Ok(Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position,
            }));
        }
        let mut refs = Vec::new();
        for row in self.query_rows_for_link(shape, binding, DurabilityTier::Local, identity)? {
            let Some(tx_id) = self.current_row_tx_id(&row) else {
                continue;
            };
            refs.push(RowVersionRef::new(
                row.table().to_owned(),
                row.row_uuid(),
                tx_id,
            ));
        }
        refs.sort();
        refs.dedup();
        if refs.is_empty() {
            return Ok(None);
        }
        Ok(exact_known_state_declaration_if_within_limits(
            shape.shape_id(),
            subscription,
            values,
            refs,
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn subscription_is_known_state_declared(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<bool, Error> {
        let binding_view_key = match self.binding_view_key_for_subscription(subscription) {
            Ok(binding_view_key) => binding_view_key,
            Err(Error::InvalidStoredValue(
                "subscription referenced unregistered shape"
                | "subscription referenced unregistered binding",
            )) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(self
            .query
            .known_state_declared_binding_views
            .contains(&binding_view_key))
    }

    pub(crate) fn binding_view_key_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<BindingViewKey, Error> {
        if let Some(registered) = self
            .query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&subscription.binding_id))
        {
            return Ok(registered.binding_view_key);
        }
        if let Some(binding_view_key) = self.canonical_whole_table_binding_view_key(subscription)? {
            return Ok(binding_view_key);
        }
        Err(Error::InvalidStoredValue(
            "subscription referenced unregistered binding",
        ))
    }

    fn canonical_whole_table_binding_view_key(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<Option<BindingViewKey>, Error> {
        for table in &self.catalogue.schema.tables {
            if self.whole_table_subscription_key(&table.name)? == subscription {
                return Ok(Some(BindingViewKey::from_canonical_subscription_key(
                    subscription,
                )));
            }
        }
        Ok(None)
    }

    fn compile_current_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    fn compile_current_query_program_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_with_settled_view(
            shape,
            binding,
            tier,
            identity,
            output,
            &ReadViewSpec::default(),
            None,
            authorization_mode,
        )
    }

    #[cfg(test)]
    fn compile_current_query_program_for_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_for_read_view_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    fn compile_current_query_program_for_read_view_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_with_settled_view(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            None,
            authorization_mode,
        )
    }

    fn compile_current_query_program_with_settled_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_with_settled_view_and_prepared_claim_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            settled_binding_view,
            authorization_mode,
            PreparedClaimBindingMode::Strict,
        )
    }

    fn compile_current_query_program_with_settled_view_and_prepared_claim_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<QueryProgram, Error> {
        let request = self.current_query_program_request_with_prepared_claim_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            settled_binding_view,
            authorization_mode,
            prepared_claim_binding_mode,
        )?;
        self.compile_query_program_request(request)
    }

    fn compile_current_query_program_for_one_shot_read(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        let access_paths = self.one_shot_access_paths(shape, binding, tier)?;
        let request = self.current_query_program_request(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
            &ReadViewSpec::default(),
            settled_binding_view,
            authorization_mode,
        )?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
    }

    fn compile_current_query_program_with_selected_access_paths(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        let access_paths = self.current_query_primary_key_access_paths(shape, binding)?;
        let request = self.current_query_program_request(
            shape,
            binding,
            tier,
            identity,
            output,
            &ReadViewSpec::default(),
            None,
            QueryAuthorizationMode::TrustedServing,
        )?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
    }

    fn compile_historical_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        let input_shape = self.normalized_row_set_shape(shape, binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                shape,
                binding,
                query_binding_source_shape_for_parts_if_needed(
                    shape.params(),
                    &binding_claim_params_for_shape(&input_shape, shape.params()),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, shape.params()),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: historical_query_read_set(&input.shape, shape.schema_version(), position),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, shape.query()),
        };
        self.compile_query_program_request(request)
    }

    fn compile_include_deleted_query_program_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        let input_shape = self.normalized_include_deleted_row_set_shape(shape, binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                shape,
                binding,
                query_binding_source_shape_for_parts_if_needed(
                    shape.params(),
                    &binding_claim_params_for_shape(&input_shape, shape.params()),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, shape.params()),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode,
            reads: current_query_read_set(
                &input.shape,
                shape.schema_version(),
                self.read_policy_schema_for_table_name(
                    &shape.query().table,
                    shape.schema_version(),
                    &input.shape,
                ),
                tier,
                None,
            ),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(CurrentQueryProgramOutput::AppRows, shape.query()),
        };
        self.compile_query_program_request(request)
    }

    fn compile_open_tx_query_program(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let input_shape = if include_deleted {
            self.normalized_include_deleted_row_set_shape(&lowered_shape, &binding)?
        } else {
            self.normalized_row_set_shape(&lowered_shape, &binding)?
        };
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                &lowered_shape,
                &binding,
                query_binding_source_shape_for_parts_if_needed(
                    lowered_shape.params(),
                    &binding_claim_params_for_shape(&input_shape, lowered_shape.params()),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, lowered_shape.params()),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode,
            reads: tx_query_read_set(
                &input.shape,
                lowered_shape.schema_version(),
                tx_id,
                snapshot,
            ),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, lowered_shape.query()),
        };
        self.compile_query_program_request(request)
    }

    fn compile_branch_query_program_in_authorization_mode(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let input_shape = self.normalized_row_set_shape(&lowered_shape, &binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                &lowered_shape,
                &binding,
                query_binding_source_shape_for_parts_if_needed(
                    lowered_shape.params(),
                    &binding_claim_params_for_shape(&input_shape, lowered_shape.params()),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, lowered_shape.params()),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode,
            reads: branch_query_read_set(
                &input.shape,
                lowered_shape.schema_version(),
                DurabilityTier::Local,
                branch_id,
            ),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, lowered_shape.query()),
        };
        self.compile_query_program_request(request)
    }

    pub(super) fn query_rows_on_branch_query_engine(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_on_branch_query_engine_in_authorization_mode(
            branch_id,
            shape,
            binding,
            identity,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    pub(super) fn query_rows_on_branch_query_engine_for_client(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_on_branch_query_engine_in_authorization_mode(
            branch_id,
            shape,
            binding,
            identity,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    fn query_rows_on_branch_query_engine_in_authorization_mode(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table = self.query_output_table(shape.query(), shape.schema_version())?;
        let program = self.compile_branch_query_program_in_authorization_mode(
            branch_id,
            shape,
            binding,
            identity,
            CurrentQueryProgramOutput::AppRows,
            authorization_mode,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_materialization_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        let mut rows = if shape.query().aggregate.is_some() {
            self.materialize_aggregate_query_rows(shape.query(), &table, deltas)
        } else {
            self.materialize_inline_current_query_rows(&table, deltas)
        }?;
        self.finish_engine_query_rows_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        if shape.query().array_subqueries.is_empty() {
            self.apply_projection_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        }
        Ok(rows)
    }

    fn policy_authorization_row_id_graph(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<PolicyAuthorizationGraph, Error> {
        self.query_engine_read_metrics.policy_authorization_graphs += 1;
        let cache_key = policy_authorization_graph_cache_key(&request);
        if let Some(graph) = self.query.policy_authorization_graph_cache.get(&cache_key) {
            return Ok(graph.clone());
        }
        let proof_table = match &request.policy {
            PolicyContext::AuthorizationSubplan {
                protected_source, ..
            } => Some(protected_source.table.clone()),
            PolicyContext::System | PolicyContext::Identity { .. } => None,
        };
        if let Some(table) = &proof_table {
            let depth = self.query.policy_proof_stack.len();
            if self
                .query
                .policy_proof_stack
                .iter()
                .any(|active| active == table)
            {
                return Err(Error::PolicyProofCycle {
                    table: table.clone(),
                    depth,
                });
            }
            self.query.policy_proof_stack.push(table.clone());
        }

        let result = (|| {
            let program = self.compile_query_program_request(request)?;
            let graph = lowered_terminal_graph(&program, "policy.authorized_rows")?;
            let route_fields = program
                .lowered
                .terminals
                .iter()
                .find_map(|terminal| {
                    (terminal.sink == "policy.authorized_rows").then(|| match &terminal.output {
                        OutputTerminalSchema::Fact(fact) => {
                            output_routing_fields_for_query_eval(fact)
                        }
                        OutputTerminalSchema::AppRows(_) => BTreeSet::new(),
                    })
                })
                .unwrap_or_default();
            let graph = PolicyAuthorizationGraph {
                graph,
                route_fields,
            };
            self.query
                .policy_authorization_graph_cache
                .insert(cache_key, graph.clone());
            Ok(graph)
        })();

        if proof_table.is_some() {
            self.query
                .policy_proof_stack
                .pop()
                .expect("policy proof stack entry is balanced");
        }
        result
    }

    pub(super) fn branch_read_policy_authorized_branch_ids(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        let Some(policy) = self.catalogue.schema.branch_read_policy.clone() else {
            return Ok(BTreeSet::from([RowUuid(branch_id.0)]));
        };
        let mut query = policy;
        query.filters.push(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(branch_id.0)),
        ));
        let policy_shape = query.validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        if !policy_shape.params().is_empty() {
            return Err(Error::QueryCapability(
                "branch read policy filters with runtime parameters must lower through query-engine binding sources"
                    .to_owned(),
            ));
        }
        let binding = policy_shape.bind(BTreeMap::new())?;
        let input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                &policy_shape,
                &binding,
                None,
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, policy_shape.params()),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: current_query_read_set(
                &input.shape,
                policy_shape.schema_version(),
                policy_shape.schema_version(),
                DurabilityTier::Local,
                None,
            ),
            policy: match self.query_program_policy_context(identity) {
                PolicyContext::Identity {
                    mode,
                    permission_subject,
                    claims,
                    attribution,
                } => PolicyContext::AuthorizationSubplan {
                    protected_source: root_source_id(policy_shape.query().table.as_str()),
                    role: PolicyDecisionRole::Read,
                    mode,
                    permission_subject,
                    claims,
                    attribution,
                },
                other => other,
            },
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AuthorizedRows,
                policy_shape.query(),
            ),
        };
        let graph = self.policy_authorization_row_id_graph(request)?.graph;
        let deltas = self.database.query_graph(graph).map_err(Error::Groove)?;
        let row_idx =
            deltas
                .descriptor
                .field_index("row_uuid")
                .ok_or(Error::InvalidStoredValue(
                    "branch read authorization terminal is missing row_uuid",
                ))?;
        let mut rows = BTreeSet::new();
        for (record, weight) in deltas.iter() {
            if weight <= 0 {
                continue;
            }
            rows.insert(RowUuid(record.get_uuid(row_idx)?));
        }
        Ok(rows)
    }

    fn current_query_program_request(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgramRequest, Error> {
        self.current_query_program_request_with_prepared_claim_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            settled_binding_view,
            authorization_mode,
            PreparedClaimBindingMode::Strict,
        )
    }

    fn current_query_program_request_with_prepared_claim_mode(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<QueryProgramRequest, Error> {
        let lowered_shape;
        let lowered_binding;
        // Prepared binding sources are a serving-side optimization. Client
        // local execution must lower concrete bindings into its locally
        // available (already upstream-scoped at Edge/Global) data, rather
        // than trying to evaluate a server-maintained binding graph.
        let use_prepared_binding_source = authorization_mode
            == QueryAuthorizationMode::TrustedServing
            && self.can_use_prepared_current_query_plan(shape)
            && settled_binding_view.is_none()
            && !matches!(output, CurrentQueryProgramOutput::RelationSnapshot);
        let (shape, binding) = if !use_prepared_binding_source {
            let read_schema = self
                .catalogue
                .catalogue_schemas
                .get(&shape.schema_version())
                .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
            lowered_shape =
                inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
            lowered_binding = lowered_shape.bind(BTreeMap::new())?;
            (&lowered_shape, &lowered_binding)
        } else {
            (shape, binding)
        };
        let input_shape = self.normalized_row_set_shape(shape, binding)?;
        let policy = self.query_program_policy_context(identity);
        let policy_schema_version = self.read_policy_schema_for_table_name(
            &shape.query().table,
            shape.schema_version(),
            &input_shape,
        );
        let mut binding_claim_params = binding_claim_params_for_shape(&input_shape, shape.params());
        if use_prepared_binding_source {
            let policy_schema = self
                .catalogue
                .catalogue_schemas
                .get(&policy_schema_version)
                .ok_or(Error::InvalidStoredValue(
                    "policy schema version is unknown",
                ))?;
            self.collect_policy_dependency_claim_params(
                &policy_schema.schema,
                &policy,
                &input_shape,
                &mut binding_claim_params,
            )?;
        }
        // System reads bypass policy evaluation and have no session from
        // which a prepared claim can be bound. A policy-derived claim slot
        // must therefore never survive into their shared descriptor.
        if matches!(policy, PolicyContext::System) {
            binding_claim_params.clear();
        }
        let source_shape = use_prepared_binding_source
            .then(|| {
                query_binding_source_shape_for_parts_if_needed(
                    shape.params(),
                    &binding_claim_params,
                )
            })
            .flatten();
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape_and_policy_with_prepared_claim_mode(
                shape,
                binding,
                source_shape,
                BTreeMap::new(),
                binding_claim_params,
                &policy,
                prepared_claim_binding_mode,
            )?,
            shape: input_shape,
        };
        Ok(QueryProgramRequest {
            authorization_mode,
            reads: query_read_set_for_read_view(
                &input.shape,
                shape.schema_version(),
                policy_schema_version,
                tier,
                read_view,
                settled_binding_view,
                shape.query().aggregate.is_some(),
            )?,
            policy,
            input,
            output: current_query_output_request(output, shape.query()),
        })
    }

    fn query_program_policy_context(&self, identity: AuthorId) -> PolicyContext {
        if identity == AuthorId::SYSTEM {
            PolicyContext::System
        } else {
            let mut claims = default_policy_claim_values(identity);
            if let Some(session_claims) = self.session_claims.get(&identity) {
                claims.extend(session_claims.clone());
            }
            claims.insert("sub".to_owned(), Value::Uuid(identity.0));
            PolicyContext::Identity {
                mode: PolicyEnforcementMode::Enforcing,
                permission_subject: identity,
                claims,
                attribution: None,
            }
        }
    }

    pub(super) fn write_policy_query_allows_current_row(
        &mut self,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let mut query = policy.clone();
        query.filters.push(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(row_uuid.0)),
        ));
        let policy_shape = query.validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let binding = policy_shape.bind(BTreeMap::new())?;
        let program = self.compile_current_query_program_with_selected_access_paths(
            &policy_shape,
            &binding,
            DurabilityTier::Local,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
    }

    /// Authorize an inline old/candidate row through the query program.
    ///
    /// Insert candidates reinterpret plain `inherits(parent)` as parent
    /// update-using authorization. Existing/update-check rows retain ordinary
    /// read inheritance unless the policy names an explicit write operation.
    pub(super) fn write_policy_query_allows_candidate(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorId,
        insert_candidate: bool,
        branch_id: Option<BranchId>,
    ) -> Result<bool, Error> {
        let policy_schema_version = if self
            .catalogue
            .schema
            .tables
            .iter()
            .any(|known| known == table)
        {
            self.catalogue.current_schema_version_id
        } else {
            self.catalogue
                .catalogue_schemas
                .iter()
                .find_map(|(schema_version, payload)| {
                    payload
                        .schema
                        .tables
                        .iter()
                        .any(|known| known == table)
                        .then_some(*schema_version)
                })
                .unwrap_or(self.catalogue.current_schema_version_id)
        };
        self.write_policy_query_allows_candidate_for_schema(
            policy_schema_version,
            table,
            policy,
            row_uuid,
            cells,
            identity,
            insert_candidate,
            branch_id,
        )
    }

    pub(super) fn write_policy_query_allows_candidate_for_schema(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorId,
        insert_candidate: bool,
        branch_id: Option<BranchId>,
    ) -> Result<bool, Error> {
        let mut policy = policy.clone();
        if insert_candidate {
            for inherits in &mut policy.inherits {
                if inherits.operation == crate::query::InheritsOperation::Select {
                    inherits.operation = crate::query::InheritsOperation::Update;
                }
            }
            for branch in &mut policy.policy_branches {
                for inherits in &mut branch.inherits {
                    if inherits.operation == crate::query::InheritsOperation::Select {
                        inherits.operation = crate::query::InheritsOperation::Update;
                    }
                }
            }
        }
        let policy_schema = if policy_schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&policy_schema_version)
                .ok_or(Error::InvalidStoredValue("policy schema payload missing"))?
                .schema
        };
        let policy_shape = policy
            .clone()
            .validate_with_schema_version(policy_schema, policy_schema_version)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            policy_schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let binding = policy_shape.bind(BTreeMap::new())?;
        let input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
        let root_source = root_source_id(policy_shape.query().table.as_str());
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                &policy_shape,
                &binding,
                query_binding_source_shape_for_parts_if_needed(
                    policy_shape.params(),
                    &binding_claim_params_for_shape(&input_shape, policy_shape.params()),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape, policy_shape.params()),
            ),
            shape: input_shape,
        };
        let policy = match self.query_program_policy_context(identity) {
            PolicyContext::Identity {
                mode,
                permission_subject,
                claims,
                attribution,
            } => PolicyContext::AuthorizationSubplan {
                protected_source: root_source_id(policy_shape.query().table.as_str()),
                role: PolicyDecisionRole::Write,
                mode,
                permission_subject,
                claims,
                attribution,
            },
            other => other,
        };
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: match branch_id {
                Some(branch_id) => branch_query_read_set(
                    &input.shape,
                    policy_shape.schema_version(),
                    DurabilityTier::Local,
                    branch_id,
                ),
                None => current_query_read_set(
                    &input.shape,
                    policy_shape.schema_version(),
                    policy_shape.schema_version(),
                    DurabilityTier::Local,
                    None,
                ),
            },
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::PolicyPredicate,
                policy_shape.query(),
            ),
        };
        let candidate = current_row_from_cells(table, row_uuid, cells)?;
        let inline_sources = BTreeMap::from([(root_source, vec![candidate])]);
        let access_paths = if branch_id.is_some() {
            BTreeMap::new()
        } else {
            self.current_query_primary_key_access_paths(&policy_shape, &binding)?
        };
        let program = self.compile_query_program_request_with_inline_sources_and_access_paths(
            request,
            inline_sources,
            access_paths,
        )?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
    }

    pub(super) fn branch_write_policy_query_allows_candidate(
        &mut self,
        branch_id: BranchId,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorId,
        insert_candidate: bool,
    ) -> Result<bool, Error> {
        self.write_policy_query_allows_candidate(
            table,
            policy,
            row_uuid,
            cells,
            identity,
            insert_candidate,
            Some(branch_id),
        )
    }

    fn write_policy_query_program_allows(
        &mut self,
        program: &QueryProgram,
        policy_shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<bool, Error> {
        let deltas =
            match self.prepared_query_plan_from_program(&program, &policy_shape, &binding)? {
                PreparedQueryPlan::Graph(graph) => {
                    self.database.query_graph(graph).map_err(Error::Groove)?
                }
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(
                        &binding,
                        &params,
                        &program.request.policy,
                        PreparedClaimBindingMode::Strict,
                    )?;
                    take_required_sink_deltas(
                        self.bind_shape_snapshot(shape, &values)?,
                        JAZZ_APP_ROWS_SINK,
                    )?
                }
                PreparedQueryPlan::PeerMaintainedMarker => {
                    return Err(Error::InvalidStoredValue(
                        "peer maintained marker cannot execute write policy plan",
                    ));
                }
            };
        Ok(deltas.iter().any(|(_, weight)| weight > 0))
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
        self.require_catalogue_ready()?;
        self.query_rows_with_prepared_plan(shape, binding, tier, None)
    }

    pub(crate) fn query_rows_with_prepared_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
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
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_options_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            identity,
            false,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    /// Execute an ordinary local client read. The upstream serving edge is the
    /// confidentiality boundary; this path must not re-evaluate row policy.
    pub(crate) fn query_rows_for_client(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_options_for_identity(
            shape,
            binding,
            tier,
            None,
            identity,
            false,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    pub(crate) fn query_rows_for_client_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(binding_view) =
            self.client_settled_binding_view_key_for_query(shape, binding, tier, read_view)
        else {
            return Ok(Vec::new());
        };
        let Some(snapshot) =
            self.authoritative_reset_snapshot_for_binding_view(shape, binding_view)?
        else {
            return Ok(Vec::new());
        };
        Ok(snapshot
            .rows
            .into_iter()
            .take(snapshot.root_count)
            .collect())
    }

    pub(crate) fn query_rows_local_preview(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan(shape, binding, DurabilityTier::Local, prepared_plan)
    }

    pub(crate) fn query_rows_local_preview_profiled(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        self.query_rows_with_options_for_identity_profiled(
            shape,
            binding,
            DurabilityTier::Local,
            prepared_plan,
            AuthorId::SYSTEM,
        )
    }

    pub(crate) fn query_rows_including_deleted_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_options_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            identity,
            true,
            authorization_mode,
        )
    }

    fn query_rows_with_options_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorId,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        if include_deleted {
            let mut rows = self.query_rows_including_deleted_with_query_engine(
                shape,
                binding,
                tier,
                identity,
                authorization_mode,
            )?;
            let query = shape.query();
            self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
            self.apply_projection_in_schema(query, shape.schema_version(), &mut rows)?;
            return Ok(rows);
        }
        let settled_binding_view = match authorization_mode {
            QueryAuthorizationMode::ClientLocal => self.client_settled_binding_view_key_for_query(
                shape,
                binding,
                tier,
                &ReadViewSpec::default(),
            ),
            QueryAuthorizationMode::TrustedServing => (tier == DurabilityTier::Global)
                .then(|| self.settled_binding_view_key_for_query(shape, binding))
                .transpose()?
                .flatten(),
        };
        // Ordinary Edge/Global reads are allowed to consume only a source
        // binding view registered by upstream coverage. A client-local plan
        // without that host-owned route must not fall back to its raw overlay.
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && tier >= DurabilityTier::Edge
            && settled_binding_view.is_none()
        {
            return Ok(Vec::new());
        }
        // A concrete one-shot access path is binding-specific. Inline that
        // binding so execution keeps the selected graph instead of replacing it
        // with the generic cached parameterized plan.
        let inline_query = if prepared_plan.is_none()
            && settled_binding_view.is_none()
            && !self.one_shot_access_paths(shape, binding, tier)?.is_empty()
        {
            let schema = self
                .catalogue
                .catalogue_schemas
                .get(&shape.schema_version())
                .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
            let inline_shape =
                inline_snapshot_bind_filter_literals(shape, binding, &schema.schema)?;
            let inline_binding = inline_shape.bind(BTreeMap::new())?;
            Some((inline_shape, inline_binding))
        } else {
            None
        };
        let (shape, binding) = inline_query
            .as_ref()
            .map(|(shape, binding)| (shape, binding))
            .unwrap_or((shape, binding));
        let prepared_plan = prepared_plan
            .filter(|plan| !matches!(plan.as_ref(), PreparedQueryPlan::PeerMaintainedMarker));
        let program = if prepared_plan.is_some() {
            None
        } else {
            Some(self.compile_current_query_program_for_one_shot_read(
                shape,
                binding,
                tier,
                identity,
                settled_binding_view,
                authorization_mode,
            )?)
        };
        let needs_binding = || {
            let parameters = &program
                .as_ref()
                .expect("program is compiled when no prepared plan is supplied")
                .lowered
                .parameters;
            !parameters.user_params.is_empty() || !parameters.claim_params.is_empty()
        };
        let plan = match prepared_plan {
            Some(plan) if settled_binding_view.is_none() => Some(plan.clone()),
            Some(_) => None,
            None if authorization_mode == QueryAuthorizationMode::TrustedServing
                && settled_binding_view.is_none()
                && self.can_use_prepared_current_query_plan(shape)
                && needs_binding() =>
            {
                Some(self.prepared_query_plan(shape, binding, tier, identity)?)
            }
            None if authorization_mode == QueryAuthorizationMode::TrustedServing
                && settled_binding_view.is_none()
                && needs_binding() =>
            {
                Some(std::sync::Arc::new(
                    self.prepared_query_plan_from_program(
                        program
                            .as_ref()
                            .expect("program is compiled when no prepared plan is supplied"),
                        shape,
                        binding,
                    )?,
                ))
            }
            None => None,
        };
        let policy = self.query_program_policy_context(identity);
        let table_schema = self.query_output_table(shape.query(), shape.schema_version())?;
        let deltas_result = match plan {
            None => self
                .database
                .query_graph(lowered_materialization_app_rows_graph(
                    &program.expect("program is compiled when no prepared plan is supplied"),
                )?)
                .map_err(Error::Groove),
            Some(plan) => match plan.as_ref() {
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(
                        binding,
                        params,
                        &policy,
                        PreparedClaimBindingMode::Strict,
                    )?;
                    self.bind_shape_snapshot(*shape, &values)
                        .and_then(|deltas| take_required_sink_deltas(deltas, JAZZ_APP_ROWS_SINK))
                }
                PreparedQueryPlan::Graph(graph) => self
                    .database
                    .query_graph(graph.clone())
                    .map_err(Error::Groove),
                PreparedQueryPlan::PeerMaintainedMarker => {
                    unreachable!("peer maintained markers are filtered before query execution")
                }
            },
        };
        let deltas = deltas_result?;
        let mut rows = if shape.query().aggregate.is_some() {
            self.materialize_aggregate_query_rows(shape.query(), &table_schema, deltas)?
        } else if shape.query().flat_join.is_some() {
            deltas
                .iter()
                .filter(|(_, weight)| *weight > 0)
                .map(|(record, _)| {
                    CurrentRow::new(
                        shape.query().table.clone(),
                        OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
                    )
                })
                .collect()
        } else {
            let mut rows = Vec::new();
            for (record, weight) in deltas.iter() {
                if weight > 0 {
                    let row = decode_current_row(&table_schema, record)?;
                    rows.push(self.materialize_current_row(&table_schema, row)?);
                }
            }
            rows
        };
        // The graph used for synchronous materialization intentionally retains
        // physical provenance fields so policy witnesses can
        // be resolved above.  Do not let that internal descriptor cross the
        // public CurrentRow boundary: subscriptions use the public terminal
        // shape, and native/WASM consumers must see the same layout from both
        // read paths.
        // Tree collectors own relation fields such as `posts` in their public
        // app-row descriptor.  Those fields are not columns of the root
        // table, so normalizing a structured result against that table would
        // silently discard the recursive payload before the client can read
        // it.  Flat rows still need this boundary to remove materializer-only
        // physical fields.
        if shape.query().flat_join.is_none() && shape.query().array_subqueries.is_empty() {
            normalize_public_current_rows(&table_schema, &mut rows)?;
        }
        let query = shape.query();
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        if query.flat_join.is_none() && query.array_subqueries.is_empty() {
            self.apply_projection_in_schema(query, shape.schema_version(), &mut rows)?;
        }
        Ok(rows)
    }

    fn query_rows_with_options_for_identity_profiled(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorId,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let total_started = Instant::now();
        let phase_started = Instant::now();
        let settled_binding_view = (tier == DurabilityTier::Global)
            .then(|| self.settled_binding_view_key_for_query(shape, binding))
            .transpose()?
            .flatten();
        // A concrete one-shot access path is binding-specific. Inline that
        // binding so execution keeps the selected graph instead of replacing it
        // with the generic cached parameterized plan.
        let inline_query = if prepared_plan.is_none()
            && settled_binding_view.is_none()
            && !self.one_shot_access_paths(shape, binding, tier)?.is_empty()
        {
            let schema = self
                .catalogue
                .catalogue_schemas
                .get(&shape.schema_version())
                .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
            let inline_shape =
                inline_snapshot_bind_filter_literals(shape, binding, &schema.schema)?;
            let inline_binding = inline_shape.bind(BTreeMap::new())?;
            Some((inline_shape, inline_binding))
        } else {
            None
        };
        let (shape, binding) = inline_query
            .as_ref()
            .map(|(shape, binding)| (shape, binding))
            .unwrap_or((shape, binding));
        let prepared_plan = prepared_plan
            .filter(|plan| !matches!(plan.as_ref(), PreparedQueryPlan::PeerMaintainedMarker));
        let mut profile = QueryReadProfile {
            resolve_view: phase_started.elapsed(),
            ..Default::default()
        };

        let phase_started = Instant::now();
        let program = if prepared_plan.is_some() {
            None
        } else {
            Some(self.compile_current_query_program_for_one_shot_read(
                shape,
                binding,
                tier,
                identity,
                settled_binding_view,
                QueryAuthorizationMode::TrustedServing,
            )?)
        };
        profile.compile_program = phase_started.elapsed();

        let phase_started = Instant::now();
        let needs_binding = || {
            let parameters = &program
                .as_ref()
                .expect("program is compiled when no prepared plan is supplied")
                .lowered
                .parameters;
            !parameters.user_params.is_empty() || !parameters.claim_params.is_empty()
        };
        let plan = match prepared_plan {
            Some(plan) if settled_binding_view.is_none() => Some(plan.clone()),
            Some(_) => None,
            None if settled_binding_view.is_none()
                && self.can_use_prepared_current_query_plan(shape)
                && needs_binding() =>
            {
                Some(self.prepared_query_plan(shape, binding, tier, identity)?)
            }
            None if settled_binding_view.is_none() && needs_binding() => Some(std::sync::Arc::new(
                self.prepared_query_plan_from_program(
                    program
                        .as_ref()
                        .expect("program is compiled when no prepared plan is supplied"),
                    shape,
                    binding,
                )?,
            )),
            None => None,
        };
        let policy = self.query_program_policy_context(identity);
        let table_schema = self.query_output_table(shape.query(), shape.schema_version())?;
        profile.select_plan = phase_started.elapsed();

        let phase_started = Instant::now();
        let deltas_result = match plan {
            None => self
                .database
                .query_graph(lowered_materialization_app_rows_graph(
                    &program.expect("program is compiled when no prepared plan is supplied"),
                )?)
                .map_err(Error::Groove),
            Some(plan) => match plan.as_ref() {
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(
                        binding,
                        params,
                        &policy,
                        PreparedClaimBindingMode::Strict,
                    )?;
                    self.bind_shape_snapshot(*shape, &values)
                        .and_then(|deltas| take_required_sink_deltas(deltas, JAZZ_APP_ROWS_SINK))
                }
                PreparedQueryPlan::Graph(graph) => self
                    .database
                    .query_graph(graph.clone())
                    .map_err(Error::Groove),
                PreparedQueryPlan::PeerMaintainedMarker => {
                    unreachable!("peer maintained markers are filtered before query execution")
                }
            },
        };
        let deltas = deltas_result?;
        profile.execute_plan = phase_started.elapsed();

        let phase_started = Instant::now();
        let mut rows = if shape.query().aggregate.is_some() {
            self.materialize_aggregate_query_rows(shape.query(), &table_schema, deltas)?
        } else {
            let mut rows = Vec::new();
            for (record, weight) in deltas.iter() {
                if weight > 0 {
                    let row = decode_current_row(&table_schema, record)?;
                    rows.push(self.materialize_current_row(&table_schema, row)?);
                }
            }
            rows
        };
        profile.decode_materialize = phase_started.elapsed();

        let query = shape.query();
        let phase_started = Instant::now();
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        profile.finish_rows = phase_started.elapsed();

        let phase_started = Instant::now();
        if query.array_subqueries.is_empty() {
            self.apply_projection_in_schema(query, shape.schema_version(), &mut rows)?;
        }
        profile.apply_projection = phase_started.elapsed();
        profile.total = total_started.elapsed();
        Ok((rows, profile))
    }

    fn settled_binding_view_key_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Option<BindingViewKey>, Error> {
        if !self.can_use_prepared_current_query_plan(shape)
            || self.query_uses_heterogeneous_physical_lineage(shape)
        {
            return Ok(None);
        }
        let binding_view_key = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            ReadViewKey::default(),
        );
        Ok(self
            .query
            .settled_result_sets
            .contains_key(&binding_view_key)
            .then_some(binding_view_key))
    }

    /// Select the server-owned result boundary for an ordinary client read.
    ///
    /// Local and process-only reads intentionally scan the complete local
    /// overlay. Edge/global reads consume only the identity-scoped result
    /// members emitted by the serving host. This is host-owned routing, not
    /// request-controlled authorization.
    fn client_settled_binding_view_key_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Option<BindingViewKey> {
        (tier >= DurabilityTier::Edge).then(|| {
            BindingViewKey::new(
                shape.shape_id(),
                binding.binding_id(),
                RegisterShapeOptions {
                    // A non-durable browser-side runtime consumes the worker
                    // relay's Edge handoff rather than the worker's Global
                    // upstream coverage.
                    tier: if self.authored_commit_durability == DurabilityTier::None {
                        tier
                    } else {
                        DurabilityTier::Global
                    },
                    read_view: read_view.clone(),
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            )
        })
    }

    fn can_use_prepared_current_query_plan(&self, shape: &ValidatedQuery) -> bool {
        shape.schema_version() == self.catalogue.current_schema_version_id
            && !self.required_include_membership_is_identity_sensitive(shape)
    }

    fn required_include_membership_is_identity_sensitive(&self, shape: &ValidatedQuery) -> bool {
        for include in &shape.query().includes {
            if !include.require && include.join_mode != crate::query::JoinMode::Inner {
                continue;
            }
            let mut table_name = shape.query().table.clone();
            for segment in include.path.split('.') {
                let Ok(table) = self.table_in_schema(&table_name, shape.schema_version()) else {
                    return true;
                };
                let Some(target_name) = table.references.get(segment) else {
                    return true;
                };
                let Ok(target) = self.table_in_schema(target_name, shape.schema_version()) else {
                    return true;
                };
                if target.read_policy.is_some() {
                    return true;
                }
                table_name = target_name.clone();
            }
        }
        false
    }

    fn query_uses_heterogeneous_physical_lineage(&self, shape: &ValidatedQuery) -> bool {
        let Some(tables) = self.query_storage_read_tables(shape) else {
            return true;
        };
        tables.into_iter().any(|logical_table| {
            let Ok(table_id) =
                self.physical_table_id_for_schema(shape.schema_version(), &logical_table)
            else {
                return true;
            };
            self.catalogue
                .physical_mappings
                .iter()
                .any(|(schema_version, mapping)| {
                    *schema_version != shape.schema_version()
                        && mapping
                            .tables
                            .values()
                            .any(|table| table.table_id == table_id)
                })
        })
    }

    fn query_storage_read_tables(&self, shape: &ValidatedQuery) -> Option<BTreeSet<String>> {
        let query = shape.query();
        let read_schema_version = shape.schema_version();
        let mut tables = BTreeSet::from([query.table.clone()]);
        for join in &query.joins {
            collect_join_read_tables(join, &mut tables);
        }
        for reachable in &query.reachable {
            tables.insert(reachable.access_table.clone());
            tables.insert(reachable.edge_table.clone());
            if let Some(seed) = &reachable.seed {
                tables.insert(seed.table.clone());
            }
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

    fn settled_binding_view_source_rows(
        &mut self,
        table: &str,
        read_schema: SchemaVersionId,
        binding_view: BindingViewKey,
        rows: SettledBindingRows,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(row_result_set) = self.query.settled_result_sets.get(&binding_view) else {
            return Ok(Vec::new());
        };
        let mut row_entries = matches!(rows, SettledBindingRows::ResultMembers)
            .then(|| {
                row_result_set
                    .iter()
                    .filter_map(ResultMemberEntry::as_row)
                    .map(|(entry_table, row_uuid, tx_id)| {
                        ((entry_table.to_string(), row_uuid, tx_id), None)
                    })
                    .collect::<BTreeMap<_, Option<RowVersionRefEntry>>>()
            })
            .unwrap_or_default();
        if let Some(program_facts) = self.query.settled_program_facts.get(&binding_view)
            && matches!(rows, SettledBindingRows::ResultMembers)
        {
            row_entries.extend(program_facts.iter().filter_map(|fact| {
                let ProgramFactEntry::RelationEdge(edge) = fact else {
                    return None;
                };
                edge.target_version.as_ref().map(|version| {
                    (
                        (edge.target_table.to_string(), edge.target_row, version.tx),
                        Some(version.clone()),
                    )
                })
            }));
        }
        if let Some(program_facts) = self.query.settled_program_facts.get(&binding_view)
            && matches!(rows, SettledBindingRows::FlatTupleContributor { .. })
        {
            row_entries.extend(program_facts.iter().filter_map(|fact| {
                let ProgramFactEntry::RelationEdge(edge) = fact else {
                    return None;
                };
                edge.target_version.as_ref().map(|version| {
                    (
                        (edge.target_table.to_string(), edge.target_row, version.tx),
                        Some(version.clone()),
                    )
                })
            }));
            row_entries.extend(program_facts.iter().filter_map(|fact| {
                let ProgramFactEntry::ContributingMembers(contribution) = fact else {
                    return None;
                };
                if !matches!(
                    rows,
                    SettledBindingRows::FlatTupleContributor { source_index }
                        if contribution.role.as_deref()
                            == Some(&format!("flat_tuple_source:{source_index}"))
                ) || !row_result_set.contains(&contribution.result)
                {
                    return None;
                }
                contribution
                    .contributor
                    .as_real_row()
                    .and_then(|contributor| {
                        contributor.row_projection().map(|(table, row, tx)| {
                            (
                                (table.to_string(), row, tx),
                                Some(RowVersionRefEntry {
                                    tx,
                                    schema_version: contributor.schema_version,
                                    layer: contributor.layer,
                                    batch: contributor.batch,
                                    branch_or_prefix: contributor.branch_or_prefix.clone(),
                                    row_digest: contributor.row_digest.clone(),
                                }),
                            )
                        })
                    })
            }));
        }
        let mut rows = Vec::with_capacity(row_entries.len());
        for ((canonical_table, row_uuid, tx_id), relation_version) in row_entries {
            let version = if let Some(version_ref) = relation_version {
                self.resolve_relation_edge_version(&canonical_table, row_uuid, &version_ref)?
            } else {
                let tx_node_alias = self
                    .node_aliases
                    .get(&tx_id.node)
                    .copied()
                    .ok_or(Error::MissingTransaction(tx_id))?;
                self.query_version_by_alias(
                    &canonical_table,
                    row_uuid,
                    VersionLayer::Content,
                    tx_id.time,
                    tx_node_alias,
                )?
                .ok_or(Error::MissingTransaction(tx_id))?
            };
            if let Some(row) = self.projected_current_row_from_materialized_version_in_read_schema(
                read_schema,
                &version,
            )? && row.table() == table
            {
                rows.push(row);
            }
        }
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
        self.require_catalogue_ready()?;
        self.query_rows_at_for_identity(shape, binding, position, AuthorId::SYSTEM)
    }

    fn query_rows_at_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = self.query_rows_at_with_query_engine(shape, binding, position, identity)?;
        let query = shape.query();
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        Ok(rows)
    }

    fn query_rows_at_with_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program = self.compile_historical_query_program(
            &lowered_shape,
            &binding,
            position,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        let table = self
            .table_in_schema(&lowered_shape.query().table, lowered_shape.schema_version())?
            .clone();
        self.materialize_historical_query_rows(table, deltas)
    }

    fn query_rows_including_deleted_with_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let query = lowered_shape.query();
        let table = if query.aggregate.is_some() {
            self.query_output_table(query, lowered_shape.schema_version())?
        } else {
            self.table_in_schema(&query.table, lowered_shape.schema_version())?
                .clone()
        };
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program = self.compile_include_deleted_query_program_in_authorization_mode(
            &lowered_shape,
            &binding,
            tier,
            identity,
            authorization_mode,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_materialization_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        if query.aggregate.is_some() {
            self.materialize_aggregate_query_rows(query, &table, deltas)
        } else {
            self.materialize_include_deleted_query_rows(table, deltas)
        }
    }

    pub(super) fn current_rows_at(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_engine_read_metrics.source_global_seq_range_scans += 1;
        self.bounded_historical_current_rows(table, position)
    }

    fn bounded_global_change_records_at(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<Vec<groove::db::EncodedKeyValue<'_>>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_schema_version_id, table)?;
        if position.0 == u64::MAX {
            Ok(self.database.index_scan_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::U64(table_id.0)],
            )?)
        } else {
            Ok(self.database.index_scan_range_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::U64(table_id.0), Value::U64(0)],
                &[Value::U64(table_id.0), Value::U64(position.0 + 1)],
            )?)
        }
    }

    pub(crate) fn open_maintained_view_subscription_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        retained_prepared_plan: Option<SubscriptionPreparedPlan>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<(LocalMaintainedViewSubscription, RelationSnapshot), Error> {
        if let Some(retained) = retained_prepared_plan.as_ref() {
            if retained.authorization_mode != authorization_mode {
                return Err(Error::InvalidStoredValue(
                    "maintained subscription retained a plan from another authorization mode",
                ));
            }
            debug_assert!(std::sync::Arc::strong_count(&retained.plan) > 0);
        }
        let settled_binding_view = (authorization_mode == QueryAuthorizationMode::ClientLocal)
            .then(|| {
                self.client_settled_binding_view_key_for_query(shape, binding, tier, read_view)
            })
            .flatten();
        let (subscription, maintained, terminal_schemas, transitions, tables) = self
            .open_seeded_maintained_subscription_view_in_authorization_mode(
                shape,
                binding,
                identity,
                tier,
                read_view,
                authorization_mode,
                settled_binding_view,
                PreparedClaimBindingMode::Strict,
            )?;
        let mut local = LocalMaintainedViewSubscription {
            subscription,
            _retained_prepared_plan: retained_prepared_plan,
            maintained,
            terminal_schemas,
            tables,
            result_query: shape.query().clone(),
            result_table: shape.query().table.clone(),
            result_schema_version: shape.schema_version(),
            binding_view_key: settled_binding_view.unwrap_or_else(|| {
                BindingViewKey::new(
                    shape.shape_id(),
                    binding.binding_id(),
                    RegisterShapeOptions {
                        tier,
                        read_view: read_view.clone(),
                        ..RegisterShapeOptions::default()
                    }
                    .read_view_key(),
                )
            }),
            result_select: shape.query().select.clone(),
            result_set: BTreeSet::new(),
            authoritative_result_set: BTreeSet::new(),
            authoritative_result_generation: 0,
            result_payloads: BTreeMap::new(),
            program_facts: BTreeSet::new(),
            root_occurrence_ids: Vec::new(),
        };
        let _initial_delta =
            self.apply_local_maintained_view_transitions(&mut local, transitions)?;
        let initial =
            self.materialize_local_maintained_relation_snapshot_with_occurrences(&local)?;
        local.root_occurrence_ids = initial.root_occurrence_ids;
        Ok((local, initial.snapshot))
    }

    pub(crate) fn drain_local_maintained_view_subscription(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_binding_view: Option<BindingViewKey>,
    ) -> Result<Option<LocalMaintainedViewSubscriptionUpdate>, Error> {
        let Some(transitions) = self.drain_local_maintained_view_subscription_transitions(
            local,
            authoritative_binding_view,
        )?
        else {
            return Ok(None);
        };
        let update = self.apply_local_maintained_view_transitions(local, transitions)?;
        Ok(Some(update))
    }

    pub(crate) fn drain_local_maintained_view_subscription_state(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_binding_view: Option<BindingViewKey>,
    ) -> Result<bool, Error> {
        let Some(transitions) = self.drain_local_maintained_view_subscription_transitions(
            local,
            authoritative_binding_view,
        )?
        else {
            return Ok(false);
        };
        let _ = self.apply_local_maintained_view_transitions_inner(local, transitions, false)?;
        Ok(true)
    }

    pub(crate) fn reset_local_maintained_view_subscription_from_binding_view(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        binding_view_key: BindingViewKey,
    ) -> Result<(), Error> {
        // Settled result sets can include support members used to maintain relations or
        // policies. The occurrence sidecar describes only public query roots, matching
        // the authoritative snapshot's `root_count`, so exclude those support members.
        local.result_set = self
            .query
            .settled_result_sets
            .get(&binding_view_key)
            .map(|members| {
                members
                    .iter()
                    .filter(|member| {
                        is_public_result_member(
                            member,
                            local.result_table.as_str(),
                            local.result_query.aggregate.is_some(),
                        )
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        local.authoritative_result_set = local.result_set.clone();
        local.authoritative_result_generation =
            self.applied_view_update_generation(binding_view_key);
        local.program_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        if local.result_query.aggregate.is_some() {
            local
                .maintained
                .replace_aggregate_result_state(&local.result_set, &local.program_facts);
        }
        local.result_payloads = local
            .program_facts
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload)
                    if is_public_result_member(
                        &payload.member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ) =>
                {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect();
        // An authoritative reset replaces membership without flowing through
        // the ordinary local transition reducer. Rebuild the occurrence
        // sidecar from exactly that new state before the caller pairs it with
        // the reset snapshot; retaining the opening vector makes a later
        // reset fail its root-count invariant (or, worse, pair wrong roots).
        local.root_occurrence_ids = self
            .materialize_local_maintained_relation_snapshot_with_occurrences(local)?
            .root_occurrence_ids;
        Ok(())
    }

    pub(crate) fn seed_local_maintained_authoritative_result_membership(
        &self,
        local: &mut LocalMaintainedViewSubscription,
        binding_view_key: BindingViewKey,
    ) {
        local.authoritative_result_set = self
            .query
            .settled_result_sets
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        local.authoritative_result_generation =
            self.applied_view_update_generation(binding_view_key);
    }

    fn drain_local_maintained_view_subscription_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        authoritative_binding_view: Option<BindingViewKey>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        if local.result_query.aggregate.is_some()
            && let Some(remote_members) =
                self.query.settled_result_sets.get(&local.binding_view_key)
            && let Some(remote_facts) = self
                .query
                .settled_program_facts
                .get(&local.binding_view_key)
        {
            let visible_members = remote_members
                .iter()
                .filter(|member| {
                    is_public_result_member(
                        member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    )
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            let visible_facts = remote_facts
                .iter()
                .filter(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => is_public_result_member(
                        &payload.member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ),
                    _ => false,
                })
                .cloned()
                .collect::<BTreeSet<_>>();
            if visible_members != local.result_set || visible_facts != local.program_facts {
                let mut transitions = super::maintained_subscription_view::ResultTransitions {
                    adds: visible_members
                        .difference(&local.result_set)
                        .cloned()
                        .collect(),
                    removes: local
                        .result_set
                        .difference(&visible_members)
                        .cloned()
                        .collect(),
                    program_fact_adds: visible_facts
                        .difference(&local.program_facts)
                        .cloned()
                        .collect(),
                    program_fact_removes: local
                        .program_facts
                        .difference(&visible_facts)
                        .cloned()
                        .collect(),
                    ..Default::default()
                };
                transitions.result_payload_adds = transitions
                    .program_fact_adds
                    .iter()
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => {
                            Some((payload.member.clone(), payload.clone()))
                        }
                        _ => None,
                    })
                    .collect();
                transitions.result_payload_removes = transitions
                    .program_fact_removes
                    .iter()
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                        _ => None,
                    })
                    .collect();
                return Ok(Some(transitions));
            }
        }
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        let mut payload_states = BTreeMap::<
            ResultMemberEntry,
            (
                Option<ResultMemberPayloadEntry>,
                Option<ResultMemberPayloadEntry>,
            ),
        >::new();
        let mut fact_states = BTreeMap::<ProgramFactEntry, (bool, bool)>::new();
        let mut structured_app_row_changes = BTreeSet::new();
        let mut terminal_operations = Vec::new();
        let mut authoritative_membership_changed = false;
        let mut authoritative_member_adds = BTreeSet::new();
        if let Some(binding_view) = authoritative_binding_view {
            let authoritative_generation = self.applied_view_update_generation(binding_view);
            // Local optimistic changes can advance the maintained graph
            // without any newer serving-peer membership decision. Keep them
            // visible until an authoritative generation advances.
            if authoritative_generation != local.authoritative_result_generation {
                let remote_members = self
                    .query
                    .settled_result_sets
                    .get(&binding_view)
                    .cloned()
                    .unwrap_or_default();
                let remote_payloads = self
                    .query
                    .settled_program_facts
                    .get(&binding_view)
                    .into_iter()
                    .flatten()
                    .filter_map(|fact| match fact {
                        ProgramFactEntry::ResultPayload(payload) => {
                            Some((payload.member.clone(), payload.clone()))
                        }
                        _ => None,
                    })
                    .collect::<BTreeMap<_, _>>();
                // The local maintained graph may intentionally be behind the
                // authority frontier (for example, an Edge-tier window over a
                // client-local database).  An authoritative ViewUpdate is not
                // merely a revocation signal: it is the current membership
                // decision for that frontier.  Import members newly admitted
                // there so a row promoted across a TopBy boundary is delivered
                // even though none of its locally-visible source facts changed.
                for entry in remote_members.difference(&local.authoritative_result_set) {
                    if !local.result_set.contains(entry) {
                        let materializable = if remote_payloads.contains_key(entry) {
                            true
                        } else if let Some(row) =
                            self.materialize_local_maintained_view_result_member(local, entry)?
                        {
                            let table = self.table(row.table())?;
                            current_row_has_required_subscription_cells(
                                &row,
                                table,
                                local.result_select.as_deref(),
                            )
                        } else {
                            false
                        };
                        // Authority membership can arrive before the admitted
                        // row's readable content bundle. Do not publish a
                        // synthetic placeholder root: the ordinary maintained
                        // source transition will add it once that content is
                        // locally materializable.
                        if !materializable {
                            continue;
                        }
                        authoritative_membership_changed = true;
                        authoritative_member_adds.insert(entry.clone());
                        states.insert(entry.clone(), (false, true));
                        if let Some(payload) = remote_payloads.get(entry) {
                            payload_states.insert(
                                entry.clone(),
                                (
                                    local.result_payloads.get(entry).cloned(),
                                    Some(payload.clone()),
                                ),
                            );
                        }
                    }
                }
                for entry in local.authoritative_result_set.difference(&remote_members) {
                    // Replace the exact prior member even when its output
                    // occurrence remains visible through a new content
                    // version. The snapshot reducer coalesces the matching
                    // occurrence add/remove into one replacement.
                    if local.result_set.contains(entry) {
                        authoritative_membership_changed = true;
                        states.insert(entry.clone(), (true, false));
                        if local.result_payloads.contains_key(entry) {
                            payload_states.insert(
                                entry.clone(),
                                (local.result_payloads.get(entry).cloned(), None),
                            );
                        }
                    }
                }
                local.authoritative_result_set = remote_members;
                local.authoritative_result_generation = authoritative_generation;
            }
        }
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = local.maintained.apply_multisink_deltas(
                        deltas,
                        &local.terminal_schemas,
                        &local.tables,
                        &self.node_aliases,
                    )?;
                    structured_app_row_changes.extend(transitions.structured_app_row_changes);
                    terminal_operations.extend(transitions.terminal_operations);
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
                    for member in transitions.result_payload_removes {
                        let before = local.result_payloads.get(&member).cloned();
                        payload_states
                            .entry(member)
                            .and_modify(|(_, after)| *after = None)
                            .or_insert((before, None));
                    }
                    for (member, payload) in transitions.result_payload_adds {
                        let before = local.result_payloads.get(&member).cloned();
                        payload_states
                            .entry(member)
                            .and_modify(|(_, after)| *after = Some(payload.clone()))
                            .or_insert((before, Some(payload)));
                    }
                    for fact in transitions.program_fact_adds {
                        let before = local.program_facts.contains(&fact);
                        fact_states
                            .entry(fact)
                            .and_modify(|(_, after)| *after = true)
                            .or_insert((before, true));
                    }
                    for fact in transitions.program_fact_removes {
                        let before = local.program_facts.contains(&fact);
                        fact_states
                            .entry(fact)
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
        if states.is_empty()
            && payload_states.is_empty()
            && fact_states.is_empty()
            && structured_app_row_changes.is_empty()
            && terminal_operations.is_empty()
        {
            return Ok(None);
        }
        let mut transitions = super::maintained_subscription_view::ResultTransitions {
            authoritative_membership_changed,
            authoritative_member_adds,
            structured_app_row_changes,
            terminal_operations,
            ..Default::default()
        };
        for (entry, (before, after)) in states {
            match (before, after) {
                (false, true) => transitions.adds.push(entry),
                (true, false) => transitions.removes.push(entry),
                _ => {}
            }
        }
        for (member, (before, after)) in payload_states {
            match (before, after) {
                (None, Some(payload)) => transitions.result_payload_adds.push((member, payload)),
                (Some(_), None) => transitions.result_payload_removes.push(member),
                (Some(before), Some(after)) if before != after => {
                    transitions.result_payload_removes.push(member.clone());
                    transitions.result_payload_adds.push((member, after));
                }
                _ => {}
            }
        }
        for (fact, (before, after)) in fact_states {
            match (before, after) {
                (false, true) => transitions.program_fact_adds.push(fact),
                (true, false) => transitions.program_fact_removes.push(fact),
                _ => {}
            }
        }
        // Aggregate facts are the payload vocabulary for synthetic result
        // members. Preserve their current values alongside membership while
        // coalescing a multisink batch; otherwise a present NULL or a revised
        // aggregate can be mistaken for an absent payload at materialization.
        if local.result_query.aggregate.is_some() {
            transitions.result_payload_adds = transitions
                .program_fact_adds
                .iter()
                .filter_map(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => {
                        Some((payload.member.clone(), payload.clone()))
                    }
                    _ => None,
                })
                .collect();
            transitions.result_payload_removes = transitions
                .program_fact_removes
                .iter()
                .filter_map(|fact| match fact {
                    ProgramFactEntry::ResultPayload(payload) => Some(payload.member.clone()),
                    _ => None,
                })
                .collect();
        }
        Ok(Some(transitions))
    }

    fn apply_local_maintained_view_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        self.apply_local_maintained_view_transitions_inner(local, transitions, true)
    }

    fn apply_local_maintained_view_transitions_inner(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
        transitions: super::maintained_subscription_view::ResultTransitions,
        materialize_update: bool,
    ) -> Result<LocalMaintainedViewSubscriptionUpdate, Error> {
        let structured_output = !local.result_query.array_subqueries.is_empty();
        let authoritative_membership_changed = transitions.authoritative_membership_changed;
        let authoritative_member_adds = transitions.authoritative_member_adds;
        let structured_app_row_changes = transitions.structured_app_row_changes.clone();
        let terminal_operations = transitions.terminal_operations.clone();
        let terminal_layout = (!terminal_operations.is_empty())
            .then(|| local.terminal_schemas.terminal_root_layout().cloned())
            .flatten();
        let aggregate_replacements = transitions
            .adds
            .iter()
            .filter(|member| {
                is_public_aggregate_result_member(
                    member,
                    local.result_table.as_str(),
                    local.result_query.aggregate.is_some(),
                )
            })
            .map(aggregate_result_member_row_uuid)
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut added_edges = Vec::new();
        let mut removed_edges = Vec::new();
        for member in transitions.result_payload_removes {
            local.result_payloads.remove(&member);
        }
        for (member, payload) in transitions.result_payload_adds {
            if is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                local.result_payloads.insert(member, payload);
            }
        }
        for member in transitions.adds {
            if !is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                continue;
            }
            // Authority-scope re-entry can surface a new internal result
            // member for an occurrence the public facade already tracks. In
            // that case replace the stale member so the current scope wins.
            // Ordinary content updates keep their member identity: replacing
            // them here turns an update into a duplicate add and leaves the
            // public subscription with the old payload.
            replace_stale_authoritative_occurrence_member(
                &mut local.result_set,
                &mut local.result_payloads,
                &authoritative_member_adds,
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            )?;
            if local.result_set.insert(member.clone()) && materialize_update && !structured_output {
                if let Some(row) =
                    self.materialize_local_maintained_view_result_member(local, &member)?
                    && let Some(occurrence_id) = public_result_member_occurrence_id(
                        &member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    )?
                {
                    added.push((occurrence_id, row));
                }
            }
        }
        for member in transitions.removes {
            if !is_public_result_member(
                &member,
                local.result_table.as_str(),
                local.result_query.aggregate.is_some(),
            ) {
                continue;
            }
            if local.result_set.remove(&member) {
                if materialize_update && !structured_output {
                    if let Some(occurrence_id) = member.output_occurrence_id() {
                        removed.push(occurrence_id);
                    } else if is_public_aggregate_result_member(
                        &member,
                        local.result_table.as_str(),
                        local.result_query.aggregate.is_some(),
                    ) {
                        let row_uuid = aggregate_result_member_row_uuid(&member)?;
                        let replacement_is_current = local.result_set.iter().any(|candidate| {
                            is_public_aggregate_result_member(
                                candidate,
                                local.result_table.as_str(),
                                local.result_query.aggregate.is_some(),
                            ) && aggregate_result_member_row_uuid(candidate)
                                .is_ok_and(|candidate_uuid| candidate_uuid == row_uuid)
                        });
                        if !aggregate_replacements.contains(&row_uuid) && !replacement_is_current {
                            removed.push(OutputOccurrenceId::single_source(ObjectId::from_uuid(
                                row_uuid.0,
                            )));
                        }
                    }
                }
            }
        }
        for fact in transitions.program_fact_removes {
            if local.program_facts.remove(&fact) {
                if materialize_update
                    && !structured_output
                    && let ProgramFactEntry::RelationEdge(edge) = fact
                {
                    removed_edges.push(self.project_relation_edge_through_read_schema(
                        &edge,
                        local.result_schema_version,
                    )?);
                }
            }
        }
        for fact in transitions.program_fact_adds {
            let edge = (materialize_update && !structured_output)
                .then(|| match &fact {
                    ProgramFactEntry::RelationEdge(edge) => Some(edge.clone()),
                    _ => None,
                })
                .flatten();
            if local.program_facts.insert(fact)
                && let Some(edge) = edge
            {
                let relation_edge = self.project_relation_edge_through_read_schema(
                    &edge,
                    local.result_schema_version,
                )?;
                let row = if let Some(version) = &edge.target_version {
                    self.materialize_local_maintained_view_relation_edge_row(
                        local,
                        edge.target_table.as_str(),
                        edge.target_row,
                        version.tx,
                    )?
                } else {
                    None
                };
                added_edges.push((relation_edge, row));
            }
        }
        if materialize_update && structured_output {
            for root in structured_app_row_changes {
                match local.maintained.structured_app_row(root) {
                    Some(record) => added.push((
                        OutputOccurrenceId::single_source(ObjectId::from_uuid(root.0)),
                        CurrentRow::new(local.result_table.clone(), record),
                    )),
                    None => removed.push(OutputOccurrenceId::single_source(ObjectId::from_uuid(
                        root.0,
                    ))),
                }
            }
        }
        Ok(LocalMaintainedViewSubscriptionUpdate {
            authoritative_membership_changed,
            added,
            removed,
            added_edges,
            removed_edges,
            terminal_operations,
            terminal_layout,
        })
    }

    fn bounded_historical_current_rows(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let mut rows_by_uuid = BTreeMap::<
            RowUuid,
            (
                Option<(TxTime, NodeAlias)>,
                Option<(TxTime, NodeAlias, Option<DeletionEvent>)>,
            ),
        >::new();
        for raw in self.bounded_global_change_records_at(table, position)? {
            let record = raw.record();
            let row_uuid = RowUuid(record.get_uuid(GlobalChangeRowRecord::FIELD_ROW_UUID_IDX)?);
            let layer = record.get_bytes(GlobalChangeRowRecord::FIELD_LAYER_IDX)?;
            let tx_time = TxTime(record.get_u64(GlobalChangeRowRecord::FIELD_TX_TIME_IDX)?);
            let tx_node = NodeAlias(record.get_u64(GlobalChangeRowRecord::FIELD_TX_NODE_ID_IDX)?);
            let deletion = record
                .get_nullable_enum(GlobalChangeRowRecord::FIELD__DELETION_IDX)?
                .map(|value| deletion_event_from_value(Value::EnumTag(value)))
                .transpose()?;
            let entry = rows_by_uuid.entry(row_uuid).or_insert((None, None));
            if layer == version_layer_string(VersionLayer::Content).as_bytes() {
                if entry.0.is_none_or(|current| (tx_time, tx_node) > current) {
                    entry.0 = Some((tx_time, tx_node));
                }
            }
            if entry.1.is_none_or(|(current_time, current_node, _)| {
                (tx_time, tx_node) > (current_time, current_node)
            }) {
                entry.1 = Some((tx_time, tx_node, deletion));
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, (content, latest_event)) in rows_by_uuid {
            let Some((_, _, latest_deletion)) = latest_event else {
                continue;
            };
            if latest_deletion == Some(DeletionEvent::Deleted) {
                continue;
            }
            let Some((tx_time, tx_node_alias)) = content else {
                continue;
            };
            let version = self
                .query_version_by_alias(
                    table,
                    row_uuid,
                    VersionLayer::Content,
                    tx_time,
                    tx_node_alias,
                )?
                .ok_or(Error::InvalidStoredValue(
                    "historical content winner is missing",
                ))?;
            rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }
    fn historical_content_witness_at(
        &mut self,
        table: &str,
        read_schema: SchemaVersionId,
        row_uuid: RowUuid,
        position: GlobalSeq,
    ) -> Result<Option<TxId>, Error> {
        let mut content = None::<(TxTime, NodeAlias)>;
        let mut latest_event = None::<(TxTime, NodeAlias, Option<DeletionEvent>)>;
        let table_id = self.physical_table_id_for_schema(read_schema, table)?;
        let raw_records = if position.0 == u64::MAX {
            self.database.index_scan_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::U64(table_id.0)],
            )?
        } else {
            self.database.index_scan_range_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::U64(table_id.0), Value::U64(0)],
                &[Value::U64(table_id.0), Value::U64(position.0 + 1)],
            )?
        };
        for raw in raw_records {
            let record = raw.record();
            if RowUuid(record.get_uuid(GlobalChangeRowRecord::FIELD_ROW_UUID_IDX)?) != row_uuid {
                continue;
            }
            let time = TxTime(record.get_u64(GlobalChangeRowRecord::FIELD_TX_TIME_IDX)?);
            let alias = NodeAlias(record.get_u64(GlobalChangeRowRecord::FIELD_TX_NODE_ID_IDX)?);
            if record.get_bytes(GlobalChangeRowRecord::FIELD_LAYER_IDX)?
                == version_layer_string(VersionLayer::Content).as_bytes()
                && content.is_none_or(|current| (time, alias) > current)
            {
                content = Some((time, alias));
            }
            let deletion = record
                .get_nullable_enum(GlobalChangeRowRecord::FIELD__DELETION_IDX)?
                .map(|value| deletion_event_from_value(Value::EnumTag(value)))
                .transpose()?;
            if latest_event.is_none_or(|(current_time, current_alias, _)| {
                (time, alias) > (current_time, current_alias)
            }) {
                latest_event = Some((time, alias, deletion));
            }
        }
        if latest_event.is_some_and(|(_, _, deletion)| deletion == Some(DeletionEvent::Deleted)) {
            return Ok(None);
        }
        let Some((time, alias)) = content else {
            return Ok(None);
        };
        let node = self
            .node_aliases
            .iter()
            .find_map(|(node, candidate)| (*candidate == alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "historical content witness node alias is missing",
            ))?;
        Ok(Some(TxId::new(time, node)))
    }
    fn query_relation_snapshot_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        let program = self.compile_current_query_program_for_read_view_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::RelationSnapshot,
            read_view,
            authorization_mode,
        )?;
        let snapshots = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .map_err(Error::Groove)?;
        self.materialize_relation_snapshot_from_query_engine(shape, read_view, &snapshots)
    }

    pub(crate) fn maintained_witness_for_result_member<'a>(
        &self,
        versions: &'a [VersionRow],
        result_schema: SchemaVersionId,
        result_table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<&'a VersionRow>, Error> {
        let table_id = self.physical_table_id_for_schema(result_schema, result_table)?;
        for version in versions.iter().rev() {
            if version.row_uuid() == row_uuid
                && !version.is_register_record()
                && version.deletion().is_none()
                && self.physical_table_id_for_version(version)? == table_id
            {
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    pub(crate) fn prepare_query_binding_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlanHandle), Error> {
        let (shape, binding) = self.query_binding_for_link(shape, binding)?;
        let plan = self.prepared_query_plan(&shape, &binding, tier, identity)?;
        Ok((shape, binding, plan))
    }

    pub(crate) fn prepare_query_binding_for_link_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        match authorization_mode {
            QueryAuthorizationMode::ClientLocal => {
                self.prepare_client_subscription_binding(shape, binding, tier, identity)
            }
            QueryAuthorizationMode::TrustedServing => {
                self.prepare_trusted_subscription_binding(shape, binding, tier, identity)
            }
        }
    }

    fn prepare_client_subscription_binding(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        let (shape, binding, plan) = self
            .prepare_query_binding_for_link_with_shared_claim_fragments(
                shape, binding, tier, identity,
            )?;
        Ok((
            shape,
            binding,
            SubscriptionPreparedPlan {
                plan,
                authorization_mode: QueryAuthorizationMode::ClientLocal,
            },
        ))
    }

    fn prepare_trusted_subscription_binding(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        let (shape, binding, plan) =
            self.prepare_query_binding_for_link(shape, binding, tier, identity)?;
        Ok((
            shape,
            binding,
            SubscriptionPreparedPlan {
                plan,
                authorization_mode: QueryAuthorizationMode::TrustedServing,
            },
        ))
    }

    pub(crate) fn prepare_query_binding_for_link_with_shared_claim_fragments(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlanHandle), Error> {
        let (shape, binding) = self.query_binding_for_link(shape, binding)?;
        // This plan only keeps the local maintained subscription's graph alive.
        // The upstream shape is registered separately below, where serving
        // compilation stays TrustedServing. Do not lower local policy here:
        // locally stored rows are already scoped by that upstream boundary.
        let program = self.compile_current_query_program_in_authorization_mode(
            &shape,
            &binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
            QueryAuthorizationMode::ClientLocal,
        )?;
        let has_claim_binding = !program.lowered.parameters.claim_params.is_empty();
        let plan = if has_claim_binding {
            let key = (
                shape.shape_id(),
                tier,
                format!(
                    "client-local:{}",
                    policy_plan_cache_signature(
                        &binding,
                        identity,
                        self.session_claim_revision(identity),
                    )
                ),
            );
            if let Some(plan) = self.query.query_shape_cache.get(&key) {
                plan.clone()
            } else {
                let plan = std::sync::Arc::new(
                    self.prepared_query_plan_from_program(&program, &shape, &binding)?,
                );
                self.query.query_shape_cache.insert(key, plan.clone());
                plan
            }
        } else {
            std::sync::Arc::new(self.prepared_query_plan_from_program(&program, &shape, &binding)?)
        };
        Ok((shape, binding, plan))
    }

    pub(crate) fn query_binding_for_link(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<(ValidatedQuery, Binding), Error> {
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let shape = bind_query_params_with_mode(
            shape,
            binding,
            &schema.schema,
            ParamBindingMode::RetainAllParams,
        )?;
        let binding = shape.bind(binding.values().clone())?;
        Ok((shape, binding))
    }

    pub(crate) fn query_rows_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan_for_identity(shape, binding, tier, None, identity)
    }

    #[cfg(test)]
    pub(crate) fn query_rows_for_link_forced_full_scan_for_test(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table = self
            .table_in_schema(&shape.query().table, shape.schema_version())?
            .clone();
        let request = self.current_query_program_request(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
            &ReadViewSpec::default(),
            None,
            QueryAuthorizationMode::TrustedServing,
        )?;
        let program =
            self.compile_query_program_request_with_access_paths(request, BTreeMap::new())?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        let mut rows = if shape.query().aggregate.is_some() {
            self.materialize_aggregate_query_rows(shape.query(), &table, deltas)?
        } else {
            self.materialize_inline_current_query_rows(&table, deltas)?
        };
        let query = shape.query();
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        Ok(rows)
    }

    /// Evaluate a query plus its array-subquery relation payload against local
    /// visible-current knowledge for one identity.
    #[cfg(test)]
    pub(crate) fn query_relation_snapshot_for_serving(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        self.query_relation_snapshot_for_serving_in_read_view(
            shape,
            binding,
            tier,
            identity,
            &ReadViewSpec::default(),
        )
    }

    pub(crate) fn query_relation_snapshot_for_serving_in_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
    ) -> Result<RelationSnapshot, Error> {
        self.query_relation_snapshot_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            read_view,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    pub(crate) fn query_relation_snapshot_for_client(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
    ) -> Result<RelationSnapshot, Error> {
        self.query_relation_snapshot_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            read_view,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    #[cfg(test)]
    pub(crate) fn query_relation_branch_discriminators_for_test(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
    ) -> Result<Vec<(Option<uuid::Uuid>, Option<uuid::Uuid>)>, Error> {
        let program = self.compile_current_query_program_for_read_view_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::RelationSnapshot,
            read_view,
            QueryAuthorizationMode::ClientLocal,
        )?;
        let snapshots = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .map_err(Error::Groove)?;
        let Some(edges) = snapshots.get("maintained.relation_edges") else {
            return Ok(Vec::new());
        };
        let decode =
            |record: &BorrowedRecord<'_>, field: &str| -> Result<Option<uuid::Uuid>, Error> {
                let index = required_field_idx(&edges.descriptor, field)?;
                match record.get_idx(index)? {
                    Value::Uuid(value) => Ok(Some(value)),
                    Value::Nullable(Some(value)) => match *value {
                        Value::Uuid(value) => Ok(Some(value)),
                        _ => Err(Error::InvalidStoredValue(
                            "branch discriminator must be UUID",
                        )),
                    },
                    Value::Nullable(None) => Ok(None),
                    _ => Err(Error::InvalidStoredValue(
                        "branch discriminator must be UUID",
                    )),
                }
            };
        edges
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(record, _)| {
                Ok((
                    decode(&record, "source_branch_or_prefix")?,
                    decode(&record, "target_branch_or_prefix")?,
                ))
            })
            .collect()
    }

    pub(crate) fn subscription_snapshot_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        #[cfg(test)]
        record_subscription_snapshot_for_link_call();
        if shape.query().array_subqueries.is_empty() {
            let rows = match authorization_mode {
                QueryAuthorizationMode::ClientLocal => {
                    self.query_rows_for_client(shape, binding, tier, identity)?
                }
                QueryAuthorizationMode::TrustedServing => self
                    .query_rows_with_prepared_plan_for_identity(
                        shape, binding, tier, None, identity,
                    )?,
            };
            return Ok(RelationSnapshot {
                root_count: rows.len(),
                rows,
                edges: Vec::new(),
            });
        }
        match authorization_mode {
            QueryAuthorizationMode::ClientLocal => {
                self.query_relation_snapshot_for_client(shape, binding, tier, identity, read_view)
            }
            QueryAuthorizationMode::TrustedServing => self
                .query_relation_snapshot_for_serving_in_read_view(
                    shape, binding, tier, identity, read_view,
                ),
        }
    }

    #[allow(dead_code)] // Slice 2 wires this into API-level routing.
    pub(crate) fn query_rows_at_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_at_for_identity(shape, binding, position, identity)
    }

    pub(crate) fn uses_schema_projected_read(&self, shape: &ValidatedQuery) -> bool {
        shape.schema_version() != self.catalogue.current_schema_version_id
    }

    pub(crate) fn apply_query_order(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        self.apply_query_order_in_schema(query, self.catalogue.current_write_schema.schema, rows)
    }

    pub(crate) fn apply_query_order_with_occurrences(
        &self,
        query: &crate::query::Query,
        rows: &mut Vec<CurrentRow>,
        occurrence_ids: &mut Vec<OutputOccurrenceId>,
    ) -> Result<(), Error> {
        let table = if query.order_by.is_empty() || query.aggregate.is_some() {
            None
        } else {
            Some(self.table_in_schema(&query.table, self.catalogue.current_write_schema.schema)?)
        };
        Self::sort_query_rows_with_occurrences(query, table.as_ref(), rows, occurrence_ids)
    }

    fn apply_projection(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        self.apply_projection_in_schema(query, self.catalogue.current_write_schema.schema, rows)
    }

    /// Evaluate a validated query inside an open exclusive transaction.
    pub fn tx_query(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_with_options(tx_id, shape, binding, false)
    }

    /// Evaluate a validated query inside an open transaction using the local
    /// client read boundary with explicit root-row deletion visibility.
    pub fn tx_query_with_options(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_in_authorization_mode(
            tx_id,
            shape,
            binding,
            AuthorId::SYSTEM,
            include_deleted,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    /// Evaluate a validated query inside an open exclusive transaction as `identity`.
    pub fn tx_query_for_identity(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_for_identity_with_options(tx_id, shape, binding, identity, false)
    }

    /// Evaluate a validated query inside an open transaction with explicit
    /// root-row deletion visibility.
    pub fn tx_query_for_identity_with_options(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_in_authorization_mode(
            tx_id,
            shape,
            binding,
            identity,
            include_deleted,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    fn tx_query_in_authorization_mode(
        &mut self,
        tx_id: OpenBatchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let query = shape.query();
        let predicate_len = self.open_tx(tx_id)?.predicate_reads.len();
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        let program = self.compile_open_tx_query_program(
            tx_id,
            shape,
            binding,
            identity,
            CurrentQueryProgramOutput::AppRows,
            include_deleted,
            authorization_mode,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_materialization_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        let mut rows = self.materialize_inline_current_query_rows(&table, deltas)?;
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
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        if query.array_subqueries.is_empty() {
            self.apply_projection_in_schema(query, shape.schema_version(), &mut rows)?;
        }
        Ok(rows)
    }

    pub(crate) fn prepared_query_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<PreparedQueryPlanHandle, Error> {
        let key = (
            shape.shape_id(),
            tier,
            policy_plan_cache_signature(binding, identity, self.session_claim_revision(identity)),
        );
        if let Some(plan) = self.query.query_shape_cache.get(&key)
            && !matches!(plan.as_ref(), PreparedQueryPlan::PeerMaintainedMarker)
        {
            return Ok(plan.clone());
        }
        let program = self.compile_current_query_program(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        let plan =
            std::sync::Arc::new(self.prepared_query_plan_from_program(&program, shape, binding)?);
        self.query.query_shape_cache.insert(key, plan.clone());
        Ok(plan)
    }

    pub(crate) fn ensure_peer_maintained_subscription_view_supported(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<(), Error> {
        // `JoinVia` is an existential constraint on this query's root-row
        // result, not flat joined output: maintained membership and delivery
        // remain addressed by the selected root row. Flat public join output
        // carries its source tuple through the maintained terminal, so it can
        // safely address several occurrences for one root as well.
        self.compile_current_query_program_for_read_view_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::MaintainedView,
            read_view,
            authorization_mode,
        )
        .map(|_| ())
    }

    pub(crate) fn mark_peer_maintained_query_shape_cache(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> PreparedQueryPlanHandle {
        let key = (
            shape.shape_id(),
            tier,
            query_binding_value_signature(binding),
        );
        self.query
            .query_shape_cache
            .entry(key)
            .or_insert_with(|| std::sync::Arc::new(PreparedQueryPlan::PeerMaintainedMarker))
            .clone()
    }

    pub(crate) fn open_seeded_maintained_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        self.open_seeded_maintained_subscription_view_in_authorization_mode(
            shape,
            binding,
            identity,
            tier,
            read_view,
            QueryAuthorizationMode::TrustedServing,
            None,
            PreparedClaimBindingMode::Strict,
        )
    }

    /// Hydrate a terminal CommitUnit authorization-support clause. Unlike an
    /// ordinary prepared query, a missing policy claim is a denied proof and
    /// is surfaced to the peer as an empty, settled authorization view.
    pub(crate) fn open_seeded_authorization_support_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        self.open_seeded_maintained_subscription_view_in_authorization_mode(
            shape,
            binding,
            identity,
            tier,
            read_view,
            QueryAuthorizationMode::TrustedServing,
            None,
            PreparedClaimBindingMode::FailClosedAuthorizationSupport,
        )
    }

    fn open_seeded_maintained_subscription_view_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
        settled_binding_view: Option<BindingViewKey>,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
        ),
        Error,
    > {
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let shape = bind_query_params_with_mode(
            shape,
            binding,
            &schema.schema,
            ParamBindingMode::RetainAllParams,
        )?;
        let binding = shape.bind(binding.values().clone())?;
        let program = self
            .compile_current_query_program_with_settled_view_and_prepared_claim_mode(
                &shape,
                &binding,
                tier,
                identity,
                CurrentQueryProgramOutput::MaintainedView,
                read_view,
                settled_binding_view,
                authorization_mode,
                prepared_claim_binding_mode,
            )?;
        let tables = program.lowered.maintained_terminal_tables.clone();
        let terminal_schemas = MaintainedSubscriptionView::terminal_schemas_for_program(&program);
        let binding_source_shape = program
            .request
            .input
            .binding
            .source_shape
            .clone()
            .unwrap_or_else(|| {
                query_binding_source_shape_for_prepared_params(&prepared_params_from_domain(
                    &program.lowered.parameters,
                ))
            });
        let subscription = self.subscribe_lowered_program(
            program,
            &binding,
            binding_source_shape,
            prepared_claim_binding_mode,
        )?;
        let mut maintained = MaintainedSubscriptionView::default();
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        let snapshot = subscription.recv().map_err(|_| {
            Error::InvalidStoredValue("seeded maintained subscription disconnected")
        })?;
        let snapshot_transitions = maintained.apply_multisink_deltas(
            snapshot,
            &terminal_schemas,
            &tables,
            &self.node_aliases,
        )?;
        transitions.adds.extend(snapshot_transitions.adds);
        transitions.removes.extend(snapshot_transitions.removes);
        transitions
            .result_payload_adds
            .extend(snapshot_transitions.result_payload_adds);
        transitions
            .result_payload_removes
            .extend(snapshot_transitions.result_payload_removes);
        transitions
            .program_fact_adds
            .extend(snapshot_transitions.program_fact_adds);
        transitions
            .program_fact_removes
            .extend(snapshot_transitions.program_fact_removes);
        transitions
            .structured_app_row_changes
            .extend(snapshot_transitions.structured_app_row_changes);
        loop {
            match subscription.try_recv() {
                Ok(deltas) => {
                    let delta_transitions = maintained.apply_multisink_deltas(
                        deltas,
                        &terminal_schemas,
                        &tables,
                        &self.node_aliases,
                    )?;
                    transitions.adds.extend(delta_transitions.adds);
                    transitions.removes.extend(delta_transitions.removes);
                    transitions
                        .result_payload_adds
                        .extend(delta_transitions.result_payload_adds);
                    transitions
                        .result_payload_removes
                        .extend(delta_transitions.result_payload_removes);
                    transitions
                        .program_fact_adds
                        .extend(delta_transitions.program_fact_adds);
                    transitions
                        .program_fact_removes
                        .extend(delta_transitions.program_fact_removes);
                    transitions
                        .structured_app_row_changes
                        .extend(delta_transitions.structured_app_row_changes);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Error::InvalidStoredValue(
                        "seeded maintained subscription disconnected",
                    ));
                }
            }
        }
        Ok((
            subscription,
            maintained,
            terminal_schemas,
            transitions,
            tables,
        ))
    }

    fn bind_shape_snapshot(
        &mut self,
        shape: PreparedShapeId,
        values: &[groove::records::Value],
    ) -> Result<MultisinkDeltas, Error> {
        let subscription = self
            .database
            .bind_shape(shape, values)
            .map_err(Error::Groove)?;
        let subscription_id = subscription.id();
        let snapshot = subscription.recv().map_err(|_| Error::SubscriptionClosed);
        self.database.unsubscribe(subscription_id);
        snapshot
    }

    fn policy_filtered_current_source_graph_via_query_engine(
        &mut self,
        policy_request: Result<QueryProgramRequest, Error>,
        base: GraphBuilder,
        output_fields: &[String],
    ) -> Result<PolicyAuthorizationGraph, Error> {
        self.query_engine_read_metrics
            .policy_authorized_source_joins += 1;
        let policy_request = match policy_request {
            Ok(policy_request) => policy_request,
            Err(Error::QueryCapability(err)) if err.contains("PolicyProofCycle") => {
                return Err(Error::QueryCapability(err));
            }
            Err(Error::QueryCapability(_)) => {
                return Ok(empty_policy_filtered_current_source_graph(
                    base,
                    output_fields,
                ));
            }
            Err(err) => return Err(err),
        };
        // The protected storage source has no binding fields of its own, but
        // the authorization proof is routed by the enclosing prepared
        // binding. Carry that descriptor alongside the source before joining
        // the proof so a later storage delta has every route field the proof
        // advertises.
        let binding_routes = policy_request
            .input
            .binding
            .source_shape
            .as_ref()
            .map(|shape| {
                let descriptor = RecordDescriptor::new(
                    policy_request
                        .input
                        .binding
                        .param_types
                        .iter()
                        .map(|(name, ty)| (name.clone(), ty.clone()))
                        .chain(
                            policy_request
                                .input
                                .binding
                                .claim_params
                                .iter()
                                .map(|(name, claim)| (name.clone(), claim.ty.clone())),
                        ),
                );
                (
                    GraphBuilder::binding_source(shape.clone(), descriptor),
                    policy_request
                        .input
                        .binding
                        .param_types
                        .keys()
                        .chain(policy_request.input.binding.claim_params.keys())
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                )
            });
        let authorized = match self.policy_authorization_row_id_graph(policy_request) {
            Ok(authorized) => authorized,
            Err(Error::QueryCapability(err)) if err.contains("PolicyProofCycle") => {
                return Err(Error::QueryCapability(err));
            }
            Err(Error::QueryCapability(_err)) => PolicyAuthorizationGraph {
                graph: empty_authorized_row_id_graph(),
                route_fields: BTreeSet::new(),
            },
            Err(err) => return Err(err),
        };
        // Authorization is existential per protected row and binding route:
        // multiple policy branches or multiple qualifying grant rows are
        // alternative proofs, not additional copies of the application row.
        // Collapse those proofs before the protected source reaches ordinary
        // relational operators (especially finite TopBy windows). Route
        // fields are part of the key because one prepared authorization graph
        // can serve several independently routed bindings.
        let mut authorization_keys = vec!["row_uuid".to_owned()];
        authorization_keys.extend(authorized.route_fields.iter().cloned());
        let authorized_graph = GraphBuilder::arg_max_by(
            authorized.graph,
            authorization_keys.clone(),
            authorization_keys,
        );
        let (base, binding_route_fields) =
            match binding_routes {
                Some((binding, route_fields)) => (
                    GraphBuilder::join(
                        base,
                        binding,
                        std::iter::empty::<String>(),
                        std::iter::empty::<String>(),
                    )
                    .project_fields(
                        output_fields
                            .iter()
                            .map(|field| ProjectField::renamed(left_field(field), field.clone()))
                            .chain(route_fields.iter().map(|field| {
                                ProjectField::renamed(right_field(field), field.clone())
                            }))
                            .collect::<Vec<_>>(),
                    ),
                    route_fields,
                ),
                None => (base, BTreeSet::new()),
            };
        let mut join_keys = vec!["row_uuid".to_owned()];
        join_keys.extend(authorized.route_fields.iter().cloned());
        if authorized.route_fields.is_empty() {
            let mut fields = output_fields
                .iter()
                .map(|field| ProjectField::renamed(left_field(&field), field.clone()))
                .collect::<Vec<_>>();
            fields.extend(
                binding_route_fields
                    .iter()
                    .map(|field| ProjectField::renamed(left_field(field), field.clone())),
            );
            return Ok(PolicyAuthorizationGraph {
                graph: GraphBuilder::join(base, authorized_graph, join_keys.clone(), join_keys)
                    .project_fields(fields),
                route_fields: binding_route_fields,
            });
        }
        let mut fields = output_fields
            .iter()
            .map(|field| ProjectField::renamed(left_field(&field), field.clone()))
            .collect::<Vec<_>>();
        fields.extend(
            authorized
                .route_fields
                .iter()
                .map(|field| ProjectField::renamed(right_field(field), field.clone())),
        );
        fields.extend(
            binding_route_fields
                .iter()
                .filter(|field| !authorized.route_fields.contains(*field))
                .map(|field| ProjectField::renamed(left_field(field), field.clone())),
        );
        Ok(PolicyAuthorizationGraph {
            graph: GraphBuilder::join(base, authorized_graph, join_keys.clone(), join_keys)
                .project_fields(fields),
            route_fields: binding_route_fields,
        })
    }

    fn table_read_policy_authorization_request(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
        binding_source_shape: Option<String>,
        binding_user_params: BTreeMap<String, ColumnType>,
        binding_claim_params: BTreeMap<String, ProgramClaimParam>,
    ) -> Result<QueryProgramRequest, Error> {
        self.table_read_policy_authorization_request_with_root_visibility(
            policy_schema_version,
            table_name,
            identity,
            param_binding_mode,
            tier,
            binding_source_shape,
            binding_user_params,
            binding_claim_params,
            false,
        )
    }

    fn table_read_policy_authorization_request_at(
        &self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        position: GlobalSeq,
        binding_source_shape: Option<String>,
        binding_user_params: BTreeMap<String, ColumnType>,
        binding_claim_params: BTreeMap<String, ProgramClaimParam>,
    ) -> Result<QueryProgramRequest, Error> {
        let policy_schema = if policy_schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&policy_schema_version)
                .ok_or(Error::InvalidStoredValue(
                    "policy schema version is unknown",
                ))?
                .schema
        };
        let table = policy_schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
        let query = authorization_query_from_read_policy(table);
        if !query.includes.is_empty() {
            return Err(Error::InvalidStoredValue(
                "historical policy source filters do not support include policies",
            ));
        }
        let policy_shape = query.validate(policy_schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            policy_schema,
            param_binding_mode,
        )?;
        if !policy_shape.params().is_empty() {
            return Err(Error::QueryCapability(
                "historical policy source filters with runtime parameters must lower through query-engine binding sources"
                    .to_owned(),
            ));
        }
        let binding = policy_shape.bind(BTreeMap::new())?;
        let mut input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
        let mut claim_params = binding_claim_params;
        claim_params.extend(binding_claim_params_for_shape(
            &input_shape,
            policy_shape.params(),
        ));
        collect_reachable_seed_claim_params(
            policy_schema,
            policy_shape.query(),
            &mut claim_params,
        )?;
        let binding_source_shape = binding_source_shape.clone().or_else(|| {
            authorization_binding_source_shape(&policy_shape, &binding_user_params, &claim_params)
        });
        if let Some(source_shape) = binding_source_shape.clone() {
            retarget_binding_value_sources(&mut input_shape, &source_shape);
        }
        let policy = match self.query_program_policy_context(identity) {
            PolicyContext::Identity {
                mode,
                permission_subject,
                claims,
                attribution,
            } => PolicyContext::AuthorizationSubplan {
                protected_source: root_source_id(policy_shape.query().table.as_str()),
                role: PolicyDecisionRole::Read,
                mode,
                permission_subject,
                claims,
                attribution,
            },
            other => other,
        };
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape_and_policy(
                &policy_shape,
                &binding,
                binding_source_shape,
                binding_user_params,
                claim_params,
                &policy,
            )?,
            shape: input_shape,
        };
        Ok(QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: historical_query_read_set(&input.shape, policy_schema_version, position),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AuthorizedRows,
                policy_shape.query(),
            ),
        })
    }

    fn table_read_policy_authorization_request_for_include_deleted(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorId,
        tier: DurabilityTier,
        binding_source_shape: Option<String>,
        binding_user_params: BTreeMap<String, ColumnType>,
        binding_claim_params: BTreeMap<String, ProgramClaimParam>,
    ) -> Result<QueryProgramRequest, Error> {
        self.table_read_policy_authorization_request_with_root_visibility(
            policy_schema_version,
            table_name,
            identity,
            ParamBindingMode::InlineAllReachableSeeds,
            tier,
            binding_source_shape,
            binding_user_params,
            binding_claim_params,
            true,
        )
    }

    fn table_read_policy_authorization_request_with_root_visibility(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
        binding_source_shape: Option<String>,
        binding_user_params: BTreeMap<String, ColumnType>,
        binding_claim_params: BTreeMap<String, ProgramClaimParam>,
        include_deleted_root: bool,
    ) -> Result<QueryProgramRequest, Error> {
        let cache_key = ReadPolicyAuthorizationRequestCacheKey {
            policy_schema_version,
            table_name: table_name.to_owned(),
            identity,
            param_binding_mode: param_binding_mode.cache_key(),
            tier,
            binding_source_shape: binding_source_shape.clone(),
            binding_user_params: binding_user_params_cache_key(&binding_user_params),
            binding_claim_params: binding_claim_params_cache_key(&binding_claim_params),
            include_deleted_root,
        };
        if let Some(request) = self
            .query
            .read_policy_authorization_request_cache
            .get(&cache_key)
        {
            return Ok(request.clone());
        }
        let policy_schema = if policy_schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&policy_schema_version)
                .ok_or(Error::InvalidStoredValue(
                    "policy schema version is unknown",
                ))?
                .schema
        };
        let table = policy_schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
        let policy = match self.query_program_policy_context(identity) {
            PolicyContext::Identity {
                mode,
                permission_subject,
                claims,
                attribution,
            } => PolicyContext::AuthorizationSubplan {
                protected_source: root_source_id(table_name),
                role: PolicyDecisionRole::Read,
                mode,
                permission_subject,
                claims,
                attribution,
            },
            other => other,
        };
        let mut query = authorization_query_from_read_policy(table);
        let mut policy_binding_values = BTreeMap::new();
        if matches!(param_binding_mode, ParamBindingMode::RetainAllParams)
            && let PolicyContext::AuthorizationSubplan { claims, .. } = &policy
        {
            bind_scope_claim_operands(&mut query, claims, &mut policy_binding_values);
        }
        if !query.includes.is_empty() {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view policy slice does not support include policies",
            ));
        }
        let declared_claim_params = disambiguate_policy_claim_params_with_outer_slots(
            &mut query,
            policy_schema,
            &mut policy_binding_values,
            &binding_claim_params,
        )?;
        let policy_shape = query.validate(policy_schema)?;
        coerce_binding_values_for_shape(&policy_shape, &mut policy_binding_values);
        let policy_binding = policy_shape.bind(policy_binding_values.clone())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            policy_schema,
            param_binding_mode,
        )?;
        if policy_shape
            .params()
            .keys()
            .any(|name| !policy_binding_values.contains_key(name))
        {
            return Err(Error::QueryCapability(
                "maintained policy source filters with runtime parameters must lower through query-engine binding sources"
                    .to_owned(),
            ));
        }
        let binding = policy_shape.bind(policy_binding_values)?;
        let mut input_shape = if include_deleted_root {
            self.normalized_include_deleted_row_set_shape(&policy_shape, &binding)?
        } else {
            self.normalized_row_set_shape(&policy_shape, &binding)?
        };
        let mut claim_params = binding_claim_params;
        claim_params.extend(binding_claim_params_for_shape(
            &input_shape,
            policy_shape.params(),
        ));
        claim_params.extend(declared_claim_params);
        collect_reachable_seed_claim_params(
            policy_schema,
            policy_shape.query(),
            &mut claim_params,
        )?;
        for (name, claim) in &mut claim_params {
            if let Some(ty) = policy_shape.params().get(name) {
                claim.ty = ty.clone();
            }
        }
        let binding_source_shape = binding_source_shape.clone().or_else(|| {
            authorization_binding_source_shape(&policy_shape, &binding_user_params, &claim_params)
        });
        if let Some(source_shape) = binding_source_shape.clone() {
            retarget_binding_value_sources(&mut input_shape, &source_shape);
        }
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape_and_policy(
                &policy_shape,
                &binding,
                binding_source_shape,
                binding_user_params,
                claim_params,
                &policy,
            )?,
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: current_query_read_set(
                &input.shape,
                policy_schema_version,
                policy_schema_version,
                tier,
                None,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AuthorizedRows,
                policy_shape.query(),
            ),
        };
        self.query
            .read_policy_authorization_request_cache
            .insert(cache_key, request.clone());
        Ok(request)
    }

    fn branch_table_read_policy_authorization_request(
        &self,
        branch_id: BranchId,
        table: &TableSchema,
        identity: AuthorId,
        binding_source_shape: Option<String>,
        binding_user_params: BTreeMap<String, ColumnType>,
        binding_claim_params: BTreeMap<String, ProgramClaimParam>,
    ) -> Result<QueryProgramRequest, Error> {
        let query = authorization_query_from_read_policy(table);
        if !query.includes.is_empty() {
            return Err(Error::InvalidStoredValue(
                "branch policy source filters do not support include policies",
            ));
        }
        let policy_shape = query.validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        if !policy_shape.params().is_empty() {
            return Err(Error::QueryCapability(
                "branch policy source filters with runtime parameters must lower through query-engine binding sources"
                    .to_owned(),
            ));
        }
        let binding = policy_shape.bind(BTreeMap::new())?;
        let mut input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
        let mut claim_params = binding_claim_params;
        claim_params.extend(binding_claim_params_for_shape(
            &input_shape,
            policy_shape.params(),
        ));
        collect_reachable_seed_claim_params(
            &self.catalogue.schema,
            policy_shape.query(),
            &mut claim_params,
        )?;
        let binding_source_shape = binding_source_shape.clone().or_else(|| {
            authorization_binding_source_shape(&policy_shape, &binding_user_params, &claim_params)
        });
        if let Some(source_shape) = binding_source_shape.clone() {
            retarget_binding_value_sources(&mut input_shape, &source_shape);
        }
        let policy = match self.query_program_policy_context(identity) {
            PolicyContext::Identity {
                mode,
                permission_subject,
                claims,
                attribution,
            } => PolicyContext::AuthorizationSubplan {
                protected_source: root_source_id(policy_shape.query().table.as_str()),
                role: PolicyDecisionRole::Read,
                mode,
                permission_subject,
                claims,
                attribution,
            },
            other => other,
        };
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape_and_policy(
                &policy_shape,
                &binding,
                binding_source_shape,
                binding_user_params,
                claim_params,
                &policy,
            )?,
            shape: input_shape,
        };
        Ok(QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: branch_query_read_set(
                &input.shape,
                policy_shape.schema_version(),
                DurabilityTier::Local,
                branch_id,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AuthorizedRows,
                policy_shape.query(),
            ),
        })
    }

    fn maintained_view_content_current_with_version(
        &self,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let schema_version = self.catalogue.current_schema_version_id;
        self.maintained_view_content_current_with_version_in_schema(table, tier, schema_version)
    }

    fn maintained_view_content_current_with_version_in_schema(
        &self,
        table: &TableSchema,
        tier: DurabilityTier,
        schema_version: SchemaVersionId,
    ) -> Result<GraphBuilder, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, &table.name)?;
        let content_fields = global_current_storage_fields(table, true, true);
        let global_content = self
            .physical_current_source_graph(
                schema_version,
                &table.name,
                PhysicalCurrentClass::Global,
            )?
            .project(content_fields.clone());
        let global_deletion =
            GraphBuilder::table(physical_register_global_current_table_name(table_id))
                .project_fields(register_storage_fields_for_query_engine(""));

        let (content, deletion) = if tier == DurabilityTier::Global {
            (global_content, global_deletion)
        } else {
            let ahead_content = self.physical_current_source_graph(
                schema_version,
                &table.name,
                PhysicalCurrentClass::Ahead,
            )?;
            let ahead_content = if tier == DurabilityTier::Edge {
                edge_visible_ahead_current_source_graph(ahead_content, content_fields.clone())
            } else {
                ahead_content.project(content_fields.clone())
            };
            let ahead_deletion =
                GraphBuilder::table(physical_register_ahead_current_table_name(table_id));
            let ahead_deletion = if tier == DurabilityTier::Edge {
                edge_visible_ahead_current_source_graph(
                    ahead_deletion,
                    register_storage_field_names(),
                )
            } else {
                ahead_deletion.project_fields(register_storage_fields_for_query_engine(""))
            };
            (
                GraphBuilder::arg_max_by(
                    GraphBuilder::union([global_content, ahead_content]),
                    ["row_uuid"],
                    ["tx_time", "tx_node_id"],
                )
                .project(content_fields),
                GraphBuilder::arg_max_by(
                    GraphBuilder::union([global_deletion, ahead_deletion]),
                    ["row_uuid"],
                    ["tx_time", "tx_node_id"],
                )
                .project_fields(register_storage_fields_for_query_engine("")),
            )
        };
        let deleted = deletion
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]);
        Ok(GraphBuilder::anti_join(
            content,
            deleted,
            ["row_uuid"],
            ["row_uuid"],
        ))
    }

    #[cfg(test)]
    pub(crate) fn test_content_current_with_version(
        &mut self,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        let graph = self.maintained_view_content_current_with_version(table, tier)?;
        self.database.query_graph(graph).map_err(Error::Groove)
    }

    /// Pick the policy-owning schema for one authorization-support operation.
    ///
    /// A newer read-policy revision must not shadow an older write-policy
    /// revision (or vice versa): the terminal proof must hydrate precisely the
    /// policy clause that admission will evaluate.
    fn authorization_scope_policy_schema_for_action(
        &self,
        table: &str,
        operation: AuthorizationScopeOperation,
    ) -> SchemaVersionId {
        let write_schema = self.catalogue.current_write_schema.schema;
        let has_operation_policy = self
            .table_in_schema(table, write_schema)
            .is_ok_and(|table| match operation {
                AuthorizationScopeOperation::Read => {
                    table.read_policy.is_some() || access_edge_parent_reference(&table).is_some()
                }
                AuthorizationScopeOperation::Insert
                | AuthorizationScopeOperation::Update
                | AuthorizationScopeOperation::Delete => {
                    !authorization_policy_queries(&table, operation).is_empty()
                }
            });
        if has_operation_policy {
            write_schema
        } else {
            self.catalogue.current_schema_version_id
        }
    }

    /// Compile the exact policy clauses needed for one non-mutating operation.
    /// This is intentionally separate from the legacy table-wide write scope:
    /// callers are not switched until the receipt transport is negotiated.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn authorization_support_scope(
        &self,
        writer: AuthorId,
        action: &PermissionAdviceAction,
    ) -> Result<AuthorizationSupportScope, Error> {
        let (operation, table_name) = authorization_scope_action(action);
        // `PermissionAdviceAction` is reconstructed after policy projection,
        // so its table belongs to the policy-owning schema, not necessarily
        // this node's base API schema. In particular, a rename lens can turn
        // an authored `users` version into a `people` policy action while an
        // old-schema subscriber remains live. Resolve both the policy and its
        // support-query schema from that policy view.
        let policy_schema_version =
            self.authorization_scope_policy_schema_for_action(table_name, operation);
        let policy_schema = self
            .catalogue
            .catalogue_schemas
            .get(&policy_schema_version)
            .ok_or(Error::InvalidStoredValue(
                "authorization policy schema is unknown",
            ))?;
        let policies = authorization_policy_queries(
            &self.table_in_schema(table_name, policy_schema_version)?,
            operation,
        );
        let claims = self.session_claims.get(&writer);
        let mut claim_values = default_permission_scope_claim_values(writer);
        if let Some(claims) = claims {
            claim_values.extend(claims.clone());
        }
        // Authorization support is authority-current: historic/branch views
        // and weaker durability tiers cannot vouch for the authoritative edge.
        let options = RegisterShapeOptions::default();
        let subscriptions = policies
            .iter()
            .map(|policy| {
                compile_permission_scope_policy(
                    policy.clone(),
                    claims,
                    &claim_values,
                    &policy_schema.schema,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let policy_bytes = postcard::to_allocvec(&(operation, &policies))
            .map_err(|_| Error::InvalidStoredValue("authorization policy serialization failed"))?;
        let claim_bytes = postcard::to_allocvec(&claim_values)
            .map_err(|_| Error::InvalidStoredValue("authorization claims serialization failed"))?;
        let support_bytes = postcard::to_allocvec(&(
            operation,
            &options,
            subscriptions
                .iter()
                .map(|(shape, binding)| (shape.shape_id(), binding.binding_id()))
                .collect::<Vec<_>>(),
        ))
        .map_err(|_| Error::InvalidStoredValue("authorization scope serialization failed"))?;
        let action_bytes = postcard::to_allocvec(action).map_err(|_| {
            Error::InvalidStoredValue("authorization operation serialization failed")
        })?;
        let operation_key =
            authorization_operation_key(operation, table_name, action, action_bytes);
        Ok(AuthorizationSupportScope {
            key: AuthorizationSupportScopeKey {
                support_shape_digest: *blake3::hash(&support_bytes).as_bytes(),
                subject: writer,
                claims_digest: *blake3::hash(&claim_bytes).as_bytes(),
                policy_digest: *blake3::hash(&policy_bytes).as_bytes(),
            },
            operation: operation_key,
            options,
            subscriptions,
        })
    }
}

fn empty_policy_filtered_current_source_graph(
    base: GraphBuilder,
    output_fields: &[String],
) -> PolicyAuthorizationGraph {
    let keys = ["row_uuid".to_owned()];
    PolicyAuthorizationGraph {
        graph: GraphBuilder::join(base, empty_authorized_row_id_graph(), keys.clone(), keys)
            .project_fields(
                output_fields
                    .iter()
                    .map(|field| ProjectField::renamed(left_field(field), field.clone()))
                    .collect::<Vec<_>>(),
            ),
        route_fields: BTreeSet::new(),
    }
}

/// Wrap a compiler aggregate record in the minimal [`CurrentRow`] envelope.
///
/// Aggregate result fields deliberately retain their compiler names here: a
/// grouped public column can have the same logical label as an aggregate
/// output, and collapsing either into a table-schema cell map loses one of
/// them. Consumers with a public aggregate query translate those names through
/// the centralized helpers at their boundary.
fn aggregate_current_row_from_record(
    table: &str,
    row_uuid: RowUuid,
    record: &BorrowedRecord<'_>,
) -> Result<CurrentRow, Error> {
    let mut fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
    let mut values = vec![Value::Uuid(row_uuid.0)];
    for (index, field) in record.descriptor().fields().iter().enumerate() {
        let name = field.name.clone().ok_or(Error::InvalidStoredValue(
            "aggregate record field must be named",
        ))?;
        fields.push((name, field.value_type.clone()));
        values.push(record.get_idx(index)?);
    }
    let descriptor = RecordDescriptor::new(fields);
    let raw = descriptor.create(&values)?;
    Ok(CurrentRow::new(
        table.to_owned(),
        OwnedRecord::new(raw, descriptor),
    ))
}

#[cfg_attr(not(test), allow(dead_code))]
fn compile_permission_scope_policy(
    mut query: JazzQuery,
    claims: Option<&BTreeMap<String, Value>>,
    claim_values: &BTreeMap<String, Value>,
    schema: &JazzSchema,
) -> Result<(ValidatedQuery, Binding), Error> {
    query.filters = query
        .filters
        .into_iter()
        .map(|p| rewrite_claim_predicate_for_binding(p, claims))
        .collect();
    query.joins = query
        .joins
        .into_iter()
        .map(|j| rewrite_claim_join_for_binding(j, claims))
        .collect();
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|p| rewrite_claim_predicate_for_binding(p, claims))
                .collect();
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|p| rewrite_claim_predicate_for_binding(p, claims))
                .collect();
            if let Some(seed) = &mut reachable.seed {
                seed.filters = std::mem::take(&mut seed.filters)
                    .into_iter()
                    .map(|p| rewrite_claim_predicate_for_binding(p, claims))
                    .collect();
            }
            reachable
        })
        .collect();
    let mut values = BTreeMap::new();
    bind_scope_claim_operands(&mut query, claim_values, &mut values);
    let shape = query.validate(schema)?;
    coerce_binding_values_for_shape(&shape, &mut values);
    let binding = shape.bind(values)?;
    Ok((shape, binding))
}

#[cfg_attr(not(test), allow(dead_code))]
fn authorization_scope_action(
    action: &PermissionAdviceAction,
) -> (AuthorizationScopeOperation, &str) {
    match action {
        PermissionAdviceAction::Read { table, .. } => (AuthorizationScopeOperation::Read, table),
        PermissionAdviceAction::Insert { table, .. } => {
            (AuthorizationScopeOperation::Insert, table)
        }
        PermissionAdviceAction::Update { table, .. } => {
            (AuthorizationScopeOperation::Update, table)
        }
        PermissionAdviceAction::Delete { table, .. } => {
            (AuthorizationScopeOperation::Delete, table)
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn authorization_policy_queries(
    table: &crate::schema::TableSchema,
    operation: AuthorizationScopeOperation,
) -> Vec<JazzQuery> {
    match operation {
        AuthorizationScopeOperation::Read
            if table.read_policy.is_none() && access_edge_parent_reference(table).is_none() =>
        {
            Vec::new()
        }
        AuthorizationScopeOperation::Read => vec![authorization_query_from_read_policy(table)],
        AuthorizationScopeOperation::Insert => table
            .write_policies
            .insert_check
            .clone()
            .into_iter()
            .collect(),
        AuthorizationScopeOperation::Update => [
            table.write_policies.update_using.clone(),
            table.write_policies.update_check.clone(),
        ]
        .into_iter()
        .flatten()
        .collect(),
        AuthorizationScopeOperation::Delete => table
            .write_policies
            .delete_using
            .clone()
            .into_iter()
            .collect(),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn authorization_operation_key(
    operation: AuthorizationScopeOperation,
    table: &str,
    action: &PermissionAdviceAction,
    action_bytes: Vec<u8>,
) -> AuthorizationOperationKey {
    let row = match action {
        PermissionAdviceAction::Read { row, .. }
        | PermissionAdviceAction::Update { row, .. }
        | PermissionAdviceAction::Delete { row, .. } => Some(*row),
        PermissionAdviceAction::Insert { .. } => None,
    };
    AuthorizationOperationKey {
        operation,
        table: table.to_owned(),
        row,
        candidate_digest: *blake3::hash(&action_bytes).as_bytes(),
    }
}

#[cfg(test)]
mod authorization_scope_compiler_tests {
    use super::*;
    use crate::ids::NodeUuid;
    use crate::node::NodeState;
    use crate::protocol::TableLens;
    use crate::schema::WritePolicies;
    use groove::schema::ColumnType;
    use groove::storage::{Durability, RocksDbStorage};

    fn table() -> crate::schema::TableSchema {
        crate::schema::TableSchema::new("protected", Vec::<ColumnSchema>::new())
            .with_read_policy(JazzQuery::from("read_support"))
            .with_write_policies(WritePolicies {
                insert_check: Some(JazzQuery::from("insert_support")),
                update_using: Some(JazzQuery::from("old_support")),
                update_check: Some(JazzQuery::from("new_support")),
                delete_using: Some(JazzQuery::from("delete_support")),
            })
    }

    #[test]
    fn action_selects_exact_policy_dependencies() {
        let table = table();
        assert_eq!(
            authorization_policy_queries(&table, AuthorizationScopeOperation::Read),
            vec![authorization_query_from_read_policy(&table)]
        );
        assert_eq!(
            authorization_policy_queries(&table, AuthorizationScopeOperation::Insert),
            vec![JazzQuery::from("insert_support")]
        );
        assert_eq!(
            authorization_policy_queries(&table, AuthorizationScopeOperation::Update),
            vec![
                JazzQuery::from("old_support"),
                JazzQuery::from("new_support")
            ]
        );
        assert_eq!(
            authorization_policy_queries(&table, AuthorizationScopeOperation::Delete),
            vec![JazzQuery::from("delete_support")]
        );
    }

    #[test]
    fn operation_key_keeps_row_and_candidate_out_of_shareable_scope_identity() {
        let first = PermissionAdviceAction::Update {
            table: "protected".to_owned(),
            row: RowUuid::from_bytes([1; 16]),
            patch: BTreeMap::new(),
        };
        let second = PermissionAdviceAction::Update {
            table: "protected".to_owned(),
            row: RowUuid::from_bytes([2; 16]),
            patch: BTreeMap::new(),
        };
        let (operation, table_name) = authorization_scope_action(&first);
        let policy_bytes =
            postcard::to_allocvec(&(operation, authorization_policy_queries(&table(), operation)))
                .unwrap();
        let first_key = authorization_operation_key(
            operation,
            table_name,
            &first,
            postcard::to_allocvec(&first).unwrap(),
        );
        let second_key = authorization_operation_key(
            operation,
            table_name,
            &second,
            postcard::to_allocvec(&second).unwrap(),
        );
        assert_ne!(first_key, second_key);
        assert_eq!(
            policy_bytes,
            postcard::to_allocvec(&(operation, authorization_policy_queries(&table(), operation)))
                .unwrap()
        );
    }

    #[test]
    fn actual_compiler_uses_claims_and_access_edge_parent_inheritance() {
        let schema = JazzSchema::new([
            crate::schema::TableSchema::new(
                "resources",
                [ColumnSchema::new("owner", ColumnType::Uuid)],
            )
            .with_read_policy(JazzQuery::from("resources").filter(crate::query::eq(
                crate::query::col("owner"),
                crate::query::claim("sub"),
            ))),
            crate::schema::TableSchema::new(
                "document_access_edges",
                [ColumnSchema::new("resource_id", ColumnType::Uuid)],
            )
            .with_reference("resource_id", "resources")
            .with_read_policy(JazzQuery::from("document_access_edges")),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node = NodeState::new(NodeUuid::from_bytes([7; 16]), schema, storage).unwrap();
        let identity = AuthorId::from_bytes([8; 16]);
        node.set_session_claims(
            identity,
            BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
        );
        let first_action = PermissionAdviceAction::Read {
            table: "document_access_edges".to_owned(),
            row: RowUuid::from_bytes([1; 16]),
        };
        let first = node
            .authorization_support_scope(identity, &first_action)
            .unwrap();
        assert_eq!(first.subscriptions.len(), 1);
        let raw = node
            .table("document_access_edges")
            .unwrap()
            .read_policy
            .clone()
            .unwrap();
        let raw_compiled = compile_permission_scope_policy(
            raw,
            node.session_claims.get(&identity),
            &default_permission_scope_claim_values(identity),
            &node.catalogue.schema,
        )
        .unwrap();
        assert_ne!(
            first.subscriptions[0].0.shape_id(),
            raw_compiled.0.shape_id(),
            "canonical access-edge parent inheritance must alter the compiled support"
        );
        let second_action = PermissionAdviceAction::Read {
            table: "document_access_edges".to_owned(),
            row: RowUuid::from_bytes([2; 16]),
        };
        let second = node
            .authorization_support_scope(identity, &second_action)
            .unwrap();
        assert_eq!(
            first.key, second.key,
            "same compiled support should coalesce across rows"
        );
        assert_ne!(
            first.operation, second.operation,
            "row remains an ephemeral evaluation key"
        );
        node.set_session_claims(
            identity,
            BTreeMap::from([("role".to_owned(), Value::String("viewer".to_owned()))]),
        );
        let changed_claims = node
            .authorization_support_scope(identity, &first_action)
            .unwrap();
        assert_ne!(first.key.claims_digest, changed_claims.key.claims_digest);
    }

    #[test]
    fn actual_compiler_selects_write_clauses_and_skips_public_read_support() {
        let claim_policy = |column: &str| {
            JazzQuery::from("protected").filter(crate::query::eq(
                crate::query::col(column),
                crate::query::claim("sub"),
            ))
        };
        let schema = JazzSchema::new([
            crate::schema::TableSchema::new("public", Vec::<ColumnSchema>::new()),
            crate::schema::TableSchema::new(
                "protected",
                [
                    ColumnSchema::new("value", ColumnType::String),
                    ColumnSchema::new("insert_owner", ColumnType::Uuid),
                    ColumnSchema::new("old_owner", ColumnType::Uuid),
                    ColumnSchema::new("new_owner", ColumnType::Uuid),
                    ColumnSchema::new("delete_owner", ColumnType::Uuid),
                ],
            )
            .with_write_policies(WritePolicies {
                insert_check: Some(claim_policy("insert_owner")),
                update_using: Some(claim_policy("old_owner")),
                update_check: Some(claim_policy("new_owner")),
                delete_using: Some(claim_policy("delete_owner")),
            }),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node = NodeState::new(NodeUuid::from_bytes([9; 16]), schema, storage).unwrap();
        let identity = AuthorId::from_bytes([3; 16]);
        node.set_session_claims(
            identity,
            BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
        );
        let cells = BTreeMap::from([("value".to_owned(), Value::String("next".to_owned()))]);
        let insert = node
            .authorization_support_scope(
                identity,
                &PermissionAdviceAction::Insert {
                    table: "protected".to_owned(),
                    cells: cells.clone(),
                },
            )
            .unwrap();
        let update = node
            .authorization_support_scope(
                identity,
                &PermissionAdviceAction::Update {
                    table: "protected".to_owned(),
                    row: RowUuid::from_bytes([1; 16]),
                    patch: cells,
                },
            )
            .unwrap();
        let delete = node
            .authorization_support_scope(
                identity,
                &PermissionAdviceAction::Delete {
                    table: "protected".to_owned(),
                    row: RowUuid::from_bytes([1; 16]),
                },
            )
            .unwrap();
        let public = node
            .authorization_support_scope(
                identity,
                &PermissionAdviceAction::Read {
                    table: "public".to_owned(),
                    row: RowUuid::from_bytes([1; 16]),
                },
            )
            .unwrap();
        assert_eq!(insert.subscriptions.len(), 1);
        assert_eq!(
            update.subscriptions.len(),
            2,
            "update must hydrate both using and check support"
        );
        assert_eq!(delete.subscriptions.len(), 1);
        assert!(
            public.subscriptions.is_empty(),
            "public read must not create a support subscription"
        );
        assert_ne!(insert.key, update.key);
        assert_ne!(update.key, delete.key);
    }

    /// A newer read-only policy produces support for Alice's read while Bob's
    /// insert retains the older schema's restrictive write-policy support.
    #[test]
    fn authorization_scope_uses_newer_read_only_policy_schema() {
        // A structural v2 schema gains a restrictive read policy without any
        // write policy. Read advice must compile v2's support query rather
        // than treating the base v1 table as public.
        let base = JazzSchema::new([crate::schema::TableSchema::new(
            "notes",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_write_policies(WritePolicies {
            insert_check: Some(JazzQuery::from("notes").filter(crate::query::eq(
                crate::query::col("owner"),
                crate::query::claim("sub"),
            ))),
            update_using: None,
            update_check: None,
            delete_using: None,
        })]);
        let evolved = JazzSchema::new([crate::schema::TableSchema::new(
            "notes",
            [
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_read_policy(JazzQuery::from("notes").filter(crate::query::eq(
            crate::query::col("body"),
            crate::query::lit(Value::String("private".to_owned())),
        )))]);
        let dir = tempfile::tempdir().unwrap();
        let refs = base.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node =
            NodeState::new(NodeUuid::from_bytes([0x31; 16]), base.clone(), storage).unwrap();
        let evolved_id = evolved.version_id();
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                SchemaVersion::new(evolved),
                MigrationLens::new(
                    base.version_id(),
                    evolved_id,
                    vec![TableLens {
                        source_table: "notes".to_owned(),
                        target_table: "notes".to_owned(),
                        ops: vec![LensOp::AddColumn {
                            column: "body".to_owned(),
                            default: Value::String(String::new()),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved_id,
            },
        })
        .unwrap();

        let scope = node
            .authorization_support_scope(
                AuthorId::from_bytes([0x32; 16]),
                &PermissionAdviceAction::Read {
                    table: "notes".to_owned(),
                    row: RowUuid::from_bytes([0x33; 16]),
                },
            )
            .unwrap();
        assert_eq!(
            scope.subscriptions.len(),
            1,
            "v2-only body policy must compile into one support subscription"
        );
        let insert = node
            .authorization_support_scope(
                AuthorId::from_bytes([0x32; 16]),
                &PermissionAdviceAction::Insert {
                    table: "notes".to_owned(),
                    cells: BTreeMap::from([(
                        "owner".to_owned(),
                        Value::Uuid(uuid::Uuid::from_bytes([0x32; 16])),
                    )]),
                },
            )
            .unwrap();
        assert_eq!(
            insert.subscriptions.len(),
            1,
            "insert must retain v1's write-policy support after v2 adds only a read policy"
        );
    }

    /// Alice's v1 `users` version is projected through a rename lens to v2
    /// `people`, where Bob's terminal insert proof compiles the v2 policy.
    ///
    /// alice v1 write ──rename lens──► people action ──► bob's v2 support proof
    #[test]
    fn projected_rename_action_uses_terminal_policy_schema_for_support() {
        let base = JazzSchema::new([crate::schema::TableSchema::new(
            "users",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )]);
        let evolved = JazzSchema::new([crate::schema::TableSchema::new(
            "people",
            [
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_write_policies(WritePolicies {
            insert_check: Some(JazzQuery::from("people").filter(crate::query::eq(
                crate::query::col("body"),
                crate::query::lit(Value::String("migrated".to_owned())),
            ))),
            update_using: None,
            update_check: None,
            delete_using: None,
        })]);
        let dir = tempfile::tempdir().unwrap();
        let refs = base.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node =
            NodeState::new(NodeUuid::from_bytes([0x41; 16]), base.clone(), storage).unwrap();
        let evolved_id = evolved.version_id();
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                SchemaVersion::new(evolved),
                MigrationLens::new(
                    base.version_id(),
                    evolved_id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![
                            LensOp::RenameTable {
                                from: "users".to_owned(),
                                to: "people".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "body".to_owned(),
                                default: Value::String("migrated".to_owned()),
                            },
                        ],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved_id,
            },
        })
        .unwrap();

        let commit = MergeableCommit::new("users", RowUuid::from_bytes([0x42; 16]), 1).cells(
            BTreeMap::from([(
                "owner".to_owned(),
                Value::Uuid(uuid::Uuid::from_bytes([0x43; 16])),
            )]),
        );
        let version =
            VersionRecord::from_commit(&commit, &base.tables[0], base.version_id()).unwrap();
        let actions = node.authorization_actions_for_versions(&[version]).unwrap();
        let [PermissionAdviceAction::Insert { table, .. }] = actions.as_slice() else {
            panic!("v1 insert must project to one v2 insert action: {actions:?}");
        };
        assert_eq!(table, "people");
        let scope = node
            .authorization_support_scope(AuthorId::from_bytes([0x44; 16]), &actions[0])
            .unwrap();
        assert_eq!(
            scope.subscriptions.len(),
            1,
            "v2 projected insert policy needs support"
        );
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

fn authorization_query_from_read_policy(table: &TableSchema) -> JazzQuery {
    let Some(policy) = &table.read_policy else {
        return crate::query::Query::from(table.name.as_str());
    };
    let mut query = crate::query::Query::from(table.name.as_str());
    query.filters = policy.filters.clone();
    query.joins = policy.joins.clone();
    query.reachable = policy.reachable.clone();
    query.inherits = policy.inherits.clone();
    query.includes = policy.includes.clone();
    query.policy_branches = policy.policy_branches.clone();
    if let Some(parent_column) = access_edge_parent_reference(table) {
        query.policy_branches.push(crate::query::PolicyBranch {
            filters: Vec::new(),
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: vec![crate::query::InheritsVia {
                parent_column,
                operation: crate::query::InheritsOperation::Select,
                max_depth: None,
            }],
        });
    }
    query
}

fn access_edge_parent_reference(table: &TableSchema) -> Option<String> {
    if !table.name.ends_with("_access_edges") && table.name != "team_access_edges" {
        return None;
    }
    table
        .references
        .contains_key("resource_id")
        .then(|| "resource_id".to_owned())
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
        Predicate::Eq(left, right) => Predicate::Eq(left, right),
        Predicate::Ne(left, right) if operands_contain_unbound_claim([&left, &right], claims) => {
            false_predicate()
        }
        Predicate::Ne(left, right) => Predicate::Ne(left, right),
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
        Predicate::In(left, values) => Predicate::In(left, values),
        Predicate::Gt(_, _) | Predicate::Gte(_, _) | Predicate::Lt(_, _) | Predicate::Lte(_, _) => {
            false_predicate()
        }
        Predicate::Contains(left, right)
            if operands_contain_unbound_claim([&left, &right], claims) =>
        {
            false_predicate()
        }
        Predicate::Contains(left, right) => Predicate::Contains(left, right),
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => Predicate::EnumMatch {
            column,
            case,
            payload: Box::new(rewrite_claim_predicate_for_binding(*payload, claims)),
        },
        Predicate::IsNull(_) => false_predicate(),
    }
}

fn default_permission_scope_claim_values(writer: AuthorId) -> BTreeMap<String, Value> {
    default_policy_claim_values(writer)
}

fn default_policy_claim_values(writer: AuthorId) -> BTreeMap<String, Value> {
    // Alpha-compat built-ins live at the node admission/query boundary, not in
    // the compiler: lowering receives ordinary claim values plus spec `sub`.
    BUILTIN_POLICY_CLAIMS
        .iter()
        .map(|name| {
            let value = match *name {
                "sub" => Value::Uuid(writer.0),
                "user_id" => Value::String(writer.0.to_string()),
                "isAdmin" => Value::Bool(false),
                _ => unreachable!("unknown built-in policy claim"),
            };
            ((*name).to_owned(), value)
        })
        .collect()
}

const BUILTIN_POLICY_CLAIMS: &[&str] = &["sub", "user_id", "isAdmin"];

fn is_builtin_policy_claim(name: &str) -> bool {
    BUILTIN_POLICY_CLAIMS.contains(&name)
}

fn bind_scope_claim_operands(
    query: &mut JazzQuery,
    claim_values: &BTreeMap<String, Value>,
    binding_values: &mut BTreeMap<String, Value>,
) {
    for predicate in &mut query.filters {
        bind_scope_claim_predicate(predicate, claim_values, binding_values);
    }
    for join in &mut query.joins {
        bind_scope_claim_join(join, claim_values, binding_values);
    }
    for reachable in &mut query.reachable {
        for predicate in &mut reachable.access_filters {
            bind_scope_claim_predicate(predicate, claim_values, binding_values);
        }
        for predicate in &mut reachable.edge_filters {
            bind_scope_claim_predicate(predicate, claim_values, binding_values);
        }
        if let Some(seed) = &mut reachable.seed {
            for predicate in &mut seed.filters {
                bind_scope_claim_predicate(predicate, claim_values, binding_values);
            }
        }
    }
    for branch in &mut query.policy_branches {
        for predicate in &mut branch.filters {
            bind_scope_claim_predicate(predicate, claim_values, binding_values);
        }
        for join in &mut branch.joins {
            bind_scope_claim_join(join, claim_values, binding_values);
        }
        for reachable in &mut branch.reachable {
            for predicate in &mut reachable.access_filters {
                bind_scope_claim_predicate(predicate, claim_values, binding_values);
            }
            for predicate in &mut reachable.edge_filters {
                bind_scope_claim_predicate(predicate, claim_values, binding_values);
            }
            if let Some(seed) = &mut reachable.seed {
                for predicate in &mut seed.filters {
                    bind_scope_claim_predicate(predicate, claim_values, binding_values);
                }
            }
        }
    }
}

fn bind_scope_claim_join(
    join: &mut JoinVia,
    claim_values: &BTreeMap<String, Value>,
    binding_values: &mut BTreeMap<String, Value>,
) {
    for predicate in &mut join.filters {
        bind_scope_claim_predicate(predicate, claim_values, binding_values);
    }
    for join in &mut join.nested_joins {
        bind_scope_claim_join(join, claim_values, binding_values);
    }
}

fn bind_scope_claim_predicate(
    predicate: &mut Predicate,
    claim_values: &BTreeMap<String, Value>,
    binding_values: &mut BTreeMap<String, Value>,
) {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => {
            for predicate in predicates {
                bind_scope_claim_predicate(predicate, claim_values, binding_values);
            }
        }
        Predicate::Not(predicate) => {
            bind_scope_claim_predicate(predicate, claim_values, binding_values);
        }
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => {
            bind_scope_claim_operand(left, claim_values, binding_values);
            bind_scope_claim_operand(right, claim_values, binding_values);
        }
        Predicate::In(left, values) => {
            bind_scope_claim_operand(left, claim_values, binding_values);
            for value in values {
                bind_scope_claim_operand(value, claim_values, binding_values);
            }
        }
        Predicate::IsNull(operand) => {
            bind_scope_claim_operand(operand, claim_values, binding_values);
        }
        Predicate::EnumMatch { payload, .. } => {
            bind_scope_claim_predicate(payload, claim_values, binding_values);
        }
    }
}

fn bind_scope_claim_operand(
    operand: &mut Operand,
    claim_values: &BTreeMap<String, Value>,
    binding_values: &mut BTreeMap<String, Value>,
) {
    let Operand::Claim(name) = operand else {
        return;
    };
    let Some(value) = claim_values.get(name).cloned() else {
        return;
    };
    let param = claim_param_field(&ClaimPath(vec![name.clone()]));
    binding_values.insert(param.clone(), value);
    *operand = Operand::Param(param);
}

fn disambiguate_policy_claim_params(
    query: &mut JazzQuery,
    schema: &JazzSchema,
    binding_values: &mut BTreeMap<String, Value>,
) -> Result<BTreeMap<String, ProgramClaimParam>, Error> {
    disambiguate_policy_claim_params_with_outer_slots(
        query,
        schema,
        binding_values,
        &BTreeMap::new(),
    )
}

/// Give a policy-local claim parameter a stable binding slot. A nested policy
/// which is lowered under an already-prepared outer source must reuse that
/// source's slot when its claim path and validated type are identical. Creating
/// a fresh typed alias in that case changes the shared source descriptor after
/// it was registered. Different validated types deliberately retain distinct
/// aliases, so a claim cannot cross a type boundary through source reuse.
fn disambiguate_policy_claim_params_with_outer_slots(
    query: &mut JazzQuery,
    schema: &JazzSchema,
    binding_values: &mut BTreeMap<String, Value>,
    outer_slots: &BTreeMap<String, ProgramClaimParam>,
) -> Result<BTreeMap<String, ProgramClaimParam>, Error> {
    let shape = query.validate(schema)?;
    let mut aliases = BTreeMap::new();
    let mut claims = BTreeMap::new();
    for (name, ty) in shape.params() {
        let Some(path) = claim_path_from_param_field(name) else {
            continue;
        };
        let alias = outer_slots
            .iter()
            .find(|(_, slot)| slot.path == path && slot.ty == *ty)
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| typed_claim_param_alias(name, ty));
        aliases.insert(name.clone(), alias.clone());
        claims.insert(
            alias,
            ProgramClaimParam {
                path,
                ty: ty.clone(),
            },
        );
    }
    rename_query_params(query, &aliases);
    for (name, alias) in aliases {
        if let Some(value) = binding_values.remove(&name) {
            binding_values.insert(alias, value);
        }
    }
    Ok(claims)
}

fn typed_claim_param_alias(name: &str, ty: &ColumnType) -> String {
    let ty = format!("{ty:?}");
    format!("__jazz_claim_typed:{}:{ty}:{name}", ty.len())
}

fn rename_query_params(query: &mut JazzQuery, aliases: &BTreeMap<String, String>) {
    for predicate in &mut query.filters {
        rename_predicate_params(predicate, aliases);
    }
    for join in &mut query.joins {
        rename_join_params(join, aliases);
    }
    for reachable in &mut query.reachable {
        rename_reachable_params(reachable, aliases);
    }
    for branch in &mut query.policy_branches {
        for predicate in &mut branch.filters {
            rename_predicate_params(predicate, aliases);
        }
        for join in &mut branch.joins {
            rename_join_params(join, aliases);
        }
        for reachable in &mut branch.reachable {
            rename_reachable_params(reachable, aliases);
        }
    }
}

fn rename_join_params(join: &mut JoinVia, aliases: &BTreeMap<String, String>) {
    for predicate in &mut join.filters {
        rename_predicate_params(predicate, aliases);
    }
    for join in &mut join.nested_joins {
        rename_join_params(join, aliases);
    }
}

fn rename_reachable_params(
    reachable: &mut crate::query::ReachableVia,
    aliases: &BTreeMap<String, String>,
) {
    rename_operand_param(&mut reachable.from, aliases);
    for predicate in &mut reachable.access_filters {
        rename_predicate_params(predicate, aliases);
    }
    for predicate in &mut reachable.edge_filters {
        rename_predicate_params(predicate, aliases);
    }
    if let Some(seed) = &mut reachable.seed {
        for predicate in &mut seed.filters {
            rename_predicate_params(predicate, aliases);
        }
    }
}

fn rename_predicate_params(predicate: &mut Predicate, aliases: &BTreeMap<String, String>) {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => {
            for predicate in predicates {
                rename_predicate_params(predicate, aliases);
            }
        }
        Predicate::Not(predicate) => rename_predicate_params(predicate, aliases),
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => {
            rename_operand_param(left, aliases);
            rename_operand_param(right, aliases);
        }
        Predicate::In(left, values) => {
            rename_operand_param(left, aliases);
            for value in values {
                rename_operand_param(value, aliases);
            }
        }
        Predicate::IsNull(operand) => rename_operand_param(operand, aliases),
        Predicate::EnumMatch { payload, .. } => rename_predicate_params(payload, aliases),
    }
}

fn rename_operand_param(operand: &mut Operand, aliases: &BTreeMap<String, String>) {
    let Operand::Param(name) = operand else {
        return;
    };
    if let Some(alias) = aliases.get(name) {
        *name = alias.clone();
    }
}

fn false_predicate() -> Predicate {
    Predicate::Eq(
        Operand::Literal(Value::Bool(true)),
        Operand::Literal(Value::Bool(false)),
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
        Predicate::EnumMatch { payload, .. } => predicate_contains_unbound_claim(payload, claims),
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
    matches!(operand, Operand::Claim(name) if !is_builtin_policy_claim(name) && !claims.is_some_and(|claims| claims.contains_key(name)))
}

#[derive(Clone, Copy)]
pub(crate) enum ParamBindingMode {
    InlineAllReachableSeeds,
    RetainAllParams,
}

impl ParamBindingMode {
    fn cache_key(self) -> ParamBindingModeCacheKey {
        match self {
            Self::InlineAllReachableSeeds => ParamBindingModeCacheKey::InlineAllReachableSeeds,
            Self::RetainAllParams => ParamBindingModeCacheKey::RetainAllParams,
        }
    }
}

fn binding_user_params_cache_key(params: &BTreeMap<String, ColumnType>) -> String {
    format!("{params:?}")
}

fn binding_claim_params_cache_key(params: &BTreeMap<String, ProgramClaimParam>) -> String {
    format!("{params:?}")
}

fn bind_query_params_with_mode(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
    mode: ParamBindingMode,
) -> Result<ValidatedQuery, Error> {
    let mut query = shape.query().clone();
    let root_source = root_source_id(&query.table);
    query.filters = query
        .filters
        .into_iter()
        .map(|predicate| bind_query_predicate(predicate, binding, schema, &root_source, mode))
        .collect::<Result<Vec<_>, _>>()?;
    query.joins = query
        .joins
        .into_iter()
        .map(|join| bind_join_filter_literals(join, binding, schema, mode))
        .collect::<Result<Vec<_>, Error>>()?;
    query.reachable = query
        .reachable
        .into_iter()
        .map(|mut reachable| {
            if should_inline_reachable_seed(&reachable.from, mode) {
                reachable.from = bind_query_operand(reachable.from, binding, mode)?;
            }
            reachable.access_filters = reachable
                .access_filters
                .into_iter()
                .map(|predicate| {
                    bind_query_predicate(
                        predicate,
                        binding,
                        schema,
                        &bind_source_for_table(&reachable.access_table),
                        mode,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            reachable.edge_filters = reachable
                .edge_filters
                .into_iter()
                .map(|predicate| {
                    bind_query_predicate(
                        predicate,
                        binding,
                        schema,
                        &bind_source_for_table(&reachable.edge_table),
                        mode,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            bind_reachable_seed_filters(&mut reachable, binding, schema, mode)?;
            Ok(reachable)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    query.array_subqueries = query
        .array_subqueries
        .into_iter()
        .map(|subquery| bind_array_subquery_filter_literals(subquery, binding, schema, mode))
        .collect::<Result<Vec<_>, Error>>()?;
    query.policy_branches = query
        .policy_branches
        .into_iter()
        .map(|mut branch| {
            branch.filters = branch
                .filters
                .into_iter()
                .map(|predicate| {
                    bind_query_predicate(predicate, binding, schema, &root_source, mode)
                })
                .collect::<Result<Vec<_>, _>>()?;
            branch.joins = branch
                .joins
                .into_iter()
                .map(|join| bind_join_filter_literals(join, binding, schema, mode))
                .collect::<Result<Vec<_>, Error>>()?;
            branch.reachable = branch
                .reachable
                .into_iter()
                .map(|mut reachable| {
                    if should_inline_reachable_seed(&reachable.from, mode) {
                        reachable.from = bind_query_operand(reachable.from, binding, mode)?;
                    }
                    reachable.access_filters = reachable
                        .access_filters
                        .into_iter()
                        .map(|predicate| {
                            bind_query_predicate(
                                predicate,
                                binding,
                                schema,
                                &bind_source_for_table(&reachable.access_table),
                                mode,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    reachable.edge_filters = reachable
                        .edge_filters
                        .into_iter()
                        .map(|predicate| {
                            bind_query_predicate(
                                predicate,
                                binding,
                                schema,
                                &bind_source_for_table(&reachable.edge_table),
                                mode,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    bind_reachable_seed_filters(&mut reachable, binding, schema, mode)?;
                    Ok(reachable)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(branch)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let rebound = query.validate_with_schema_version(schema, shape.schema_version())?;
    if rebound.schema_version() != shape.schema_version() {
        return Err(Error::InvalidStoredValue("bound query schema changed"));
    }
    Ok(rebound)
}

fn bind_array_subquery_filter_literals(
    mut subquery: ArraySubquery,
    binding: &Binding,
    schema: &JazzSchema,
    mode: ParamBindingMode,
) -> Result<ArraySubquery, Error> {
    let source = bind_source_for_table(&subquery.table);
    subquery.filters = subquery
        .filters
        .into_iter()
        .map(|predicate| bind_query_predicate(predicate, binding, schema, &source, mode))
        .collect::<Result<Vec<_>, _>>()?;
    subquery.nested_arrays = subquery
        .nested_arrays
        .into_iter()
        .map(|nested| bind_array_subquery_filter_literals(nested, binding, schema, mode))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(subquery)
}

fn inline_snapshot_bind_filter_literals(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &JazzSchema,
) -> Result<ValidatedQuery, Error> {
    bind_query_params_with_mode(
        shape,
        binding,
        schema,
        ParamBindingMode::InlineAllReachableSeeds,
    )
}

fn retarget_binding_value_sources(shape: &mut NormalizedRowSetShape, binding_source_shape: &str) {
    for node in shape.nodes.values_mut() {
        if let RowSetExpr::ValueSource {
            shape,
            mode: ValueSourceMode::Binding,
            ..
        } = node
        {
            *shape = binding_source_shape.to_owned();
        }
    }
}

fn binding_claim_params_for_shape(
    shape: &NormalizedRowSetShape,
    param_types: &BTreeMap<String, ColumnType>,
) -> BTreeMap<String, ProgramClaimParam> {
    let mut params = BTreeMap::new();
    for node in shape.nodes.values() {
        if let RowSetExpr::ValueSource {
            columns,
            mode: ValueSourceMode::Binding,
            ..
        } = node
        {
            for column in columns {
                let NormalizedValueRef::Claim(path) = &column.value else {
                    continue;
                };
                params.insert(
                    claim_param_field(path),
                    ProgramClaimParam {
                        path: path.clone(),
                        ty: column.ty.clone(),
                    },
                );
            }
        }
        collect_claim_field_params_from_node(node, param_types, &mut params);
    }
    params
}

fn normalized_source_tables(shape: &NormalizedRowSetShape) -> BTreeSet<String> {
    shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some(source.table.clone()),
            _ => None,
        })
        .chain(
            shape
                .auxiliary_sources
                .iter()
                .map(|source| source.table.clone()),
        )
        .collect()
}

fn collect_reachable_seed_claim_params(
    schema: &JazzSchema,
    query: &JazzQuery,
    params: &mut BTreeMap<String, ProgramClaimParam>,
) -> Result<(), Error> {
    for reachable in query.reachable.iter().chain(
        query
            .policy_branches
            .iter()
            .flat_map(|branch| branch.reachable.iter()),
    ) {
        let Some(seed) = &reachable.seed else {
            continue;
        };
        let (Some(user_column), Some(user_claim)) = (&seed.user_column, &seed.user_claim) else {
            continue;
        };
        let table = schema
            .tables
            .iter()
            .find(|candidate| candidate.name == seed.table)
            .ok_or_else(|| Error::TableNotFound(seed.table.clone()))?;
        let column = table
            .columns
            .iter()
            .find(|candidate| candidate.name == *user_column)
            .ok_or(Error::InvalidStoredValue(
                "reachable seed column is missing from schema",
            ))?;
        let path = ClaimPath(user_claim.split('.').map(str::to_owned).collect());
        params.insert(
            claim_param_field(&path),
            ProgramClaimParam {
                path,
                ty: column.column_type.clone(),
            },
        );
    }
    Ok(())
}

fn collect_claim_field_params_from_node(
    node: &RowSetExpr,
    param_types: &BTreeMap<String, ColumnType>,
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    match node {
        RowSetExpr::Filter { predicate, .. } | RowSetExpr::Join { on: predicate, .. } => {
            collect_claim_field_params_from_predicate(predicate, param_types, params);
        }
        RowSetExpr::RecursiveRelation {
            frontier_key,
            dedupe_keys,
            ..
        } => {
            collect_claim_field_param_authoritative(frontier_key, ColumnType::Uuid, params);
            for key in dedupe_keys {
                collect_claim_field_param_authoritative(key, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::Project { columns, .. } => {
            for column in columns {
                collect_claim_field_param_authoritative(
                    &column.value,
                    column.output.ty.clone(),
                    params,
                );
            }
        }
        RowSetExpr::Distinct { keys, .. } => {
            for key in keys {
                collect_claim_field_param_authoritative(key, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::CorrelatedPathProjection { correlation, .. } => {
            collect_claim_field_params_from_predicate(correlation, param_types, params);
        }
        RowSetExpr::OrderBy { keys, .. } => {
            for key in keys {
                collect_claim_field_param_authoritative(&key.value, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::Slice {
            partition_by,
            tie_breaker,
            ..
        } => {
            for value in partition_by.iter().chain(tie_breaker) {
                collect_claim_field_param_authoritative(value, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::Aggregate {
            group_by, outputs, ..
        } => {
            for value in group_by {
                collect_claim_field_param_authoritative(value, ColumnType::Uuid, params);
            }
            for output in outputs {
                if let Some(input) = &output.input {
                    collect_claim_field_param_authoritative(
                        input,
                        output.output.ty.clone(),
                        params,
                    );
                }
            }
        }
        RowSetExpr::ValueSource { .. }
        | RowSetExpr::FrontierSource { .. }
        | RowSetExpr::Source { .. }
        | RowSetExpr::Union { .. } => {}
    }
}

fn collect_claim_field_params_from_predicate(
    predicate: &NormalizedPredicateExpr,
    param_types: &BTreeMap<String, ColumnType>,
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    match predicate {
        NormalizedPredicateExpr::True | NormalizedPredicateExpr::False => {}
        NormalizedPredicateExpr::Compare { left, right, .. } => {
            collect_claim_field_param(left, param_types, params);
            collect_claim_field_param(right, param_types, params);
        }
        NormalizedPredicateExpr::In { value, options } => {
            collect_claim_field_param(value, param_types, params);
            for option in options {
                collect_claim_field_param(option, param_types, params);
            }
        }
        NormalizedPredicateExpr::ArrayContains { value, needle }
        | NormalizedPredicateExpr::TextContains { value, needle } => {
            collect_claim_field_param(value, param_types, params);
            collect_claim_field_param(needle, param_types, params);
        }
        NormalizedPredicateExpr::IsNull(value) | NormalizedPredicateExpr::IsNotNull(value) => {
            collect_claim_field_param(value, param_types, params);
        }
        NormalizedPredicateExpr::And(children) | NormalizedPredicateExpr::Or(children) => {
            for child in children {
                collect_claim_field_params_from_predicate(child, param_types, params);
            }
        }
        NormalizedPredicateExpr::Not(child) => {
            collect_claim_field_params_from_predicate(child, param_types, params);
        }
        // Payload fields belong to the enum value rather than the containing
        // record. They can still contain claim parameters, so walk the nested
        // predicate while only collecting the enclosing record value here.
        NormalizedPredicateExpr::EnumMatch { value, payload, .. } => {
            collect_claim_field_param(value, param_types, params);
            collect_claim_field_params_from_predicate(payload, param_types, params);
        }
    }
}

fn collect_claim_field_param(
    value: &NormalizedValueRef,
    param_types: &BTreeMap<String, ColumnType>,
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    let NormalizedValueRef::Param(param) = value else {
        return;
    };
    let Some(path) = claim_path_from_param_field(param) else {
        return;
    };
    let Some(ty) = param_types.get(param).cloned() else {
        return;
    };
    params
        .entry(param.clone())
        .or_insert(ProgramClaimParam { path, ty });
}

fn collect_claim_field_param_authoritative(
    value: &NormalizedValueRef,
    ty: ColumnType,
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    let NormalizedValueRef::Param(param) = value else {
        return;
    };
    let Some(path) = claim_path_from_param_field(param) else {
        return;
    };
    params.insert(param.clone(), ProgramClaimParam { path, ty });
}

fn bind_query_predicate(
    predicate: Predicate,
    binding: &Binding,
    schema: &JazzSchema,
    source: &SourceId,
    mode: ParamBindingMode,
) -> Result<Predicate, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => Predicate::All(
            predicates
                .into_iter()
                .map(|predicate| bind_query_predicate(predicate, binding, schema, source, mode))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Any(predicates) => Predicate::Any(
            predicates
                .into_iter()
                .map(|predicate| bind_query_predicate(predicate, binding, schema, source, mode))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Not(predicate) => Predicate::Not(Box::new(bind_query_predicate(
            *predicate, binding, schema, source, mode,
        )?)),
        Predicate::Eq(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Eq)?
        }
        Predicate::Ne(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Ne)?
        }
        Predicate::In(left, values) => {
            let left = bind_query_operand(left, binding, mode)?;
            let target_type = operand_column_type(schema, source, &left)?;
            Predicate::In(
                left,
                values
                    .into_iter()
                    .map(|operand| {
                        bind_query_operand_with_target_type(
                            operand,
                            binding,
                            target_type.as_ref(),
                            mode,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        Predicate::Gt(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Gt)?
        }
        Predicate::Gte(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Gte)?
        }
        Predicate::Lt(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Lt)?
        }
        Predicate::Lte(left, right) => {
            bind_binary_predicate(left, right, binding, schema, source, mode, Predicate::Lte)?
        }
        Predicate::Contains(left, right) => {
            let left = bind_query_operand(left, binding, mode)?;
            let needle_type = contains_needle_type(schema, source, &left)?;
            let right =
                bind_query_operand_with_target_type(right, binding, needle_type.as_ref(), mode)?;
            match left {
                Operand::Literal(Value::Array(values)) => {
                    let target_type = operand_column_type(schema, source, &right)?;
                    Predicate::In(
                        right,
                        values
                            .into_iter()
                            .map(|value| {
                                Operand::Literal(
                                    target_type
                                        .as_ref()
                                        .map(|target_type| {
                                            coerce_literal_for_column_type(
                                                value.clone(),
                                                target_type,
                                            )
                                        })
                                        .unwrap_or(value),
                                )
                            })
                            .collect(),
                    )
                }
                left => Predicate::Contains(left, right),
            }
        }
        Predicate::IsNull(operand) => {
            Predicate::IsNull(bind_query_operand(operand, binding, mode)?)
        }
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => Predicate::EnumMatch {
            column,
            case,
            payload,
        },
    })
}

fn bind_reachable_seed_filters(
    reachable: &mut crate::query::ReachableVia,
    binding: &Binding,
    schema: &JazzSchema,
    mode: ParamBindingMode,
) -> Result<(), Error> {
    if let Some(seed) = &mut reachable.seed {
        let source = bind_source_for_table(&seed.table);
        seed.filters = std::mem::take(&mut seed.filters)
            .into_iter()
            .map(|predicate| bind_query_predicate(predicate, binding, schema, &source, mode))
            .collect::<Result<Vec<_>, _>>()?;
    }
    Ok(())
}

fn bind_join_filter_literals(
    mut join: JoinVia,
    binding: &Binding,
    schema: &JazzSchema,
    mode: ParamBindingMode,
) -> Result<JoinVia, Error> {
    let source = bind_source_for_table(&join.table);
    join.filters = join
        .filters
        .into_iter()
        .map(|predicate| bind_query_predicate(predicate, binding, schema, &source, mode))
        .collect::<Result<Vec<_>, _>>()?;
    join.nested_joins = join
        .nested_joins
        .into_iter()
        .map(|join| bind_join_filter_literals(join, binding, schema, mode))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(join)
}

fn bind_binary_predicate(
    left: Operand,
    right: Operand,
    binding: &Binding,
    schema: &JazzSchema,
    source: &SourceId,
    mode: ParamBindingMode,
    build: impl FnOnce(Operand, Operand) -> Predicate,
) -> Result<Predicate, Error> {
    let left_type = operand_column_type(schema, source, &left)?;
    let right_type = operand_column_type(schema, source, &right)?;
    Ok(build(
        bind_query_operand_with_target_type(left, binding, right_type.as_ref(), mode)?,
        bind_query_operand_with_target_type(right, binding, left_type.as_ref(), mode)?,
    ))
}

fn bind_source_for_table(table: &str) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: Vec::new(),
        },
    }
}

fn should_inline_reachable_seed(operand: &Operand, mode: ParamBindingMode) -> bool {
    match (operand, mode) {
        (Operand::Param(_), ParamBindingMode::InlineAllReachableSeeds) => true,
        (Operand::Param(_), ParamBindingMode::RetainAllParams) => false,
        _ => false,
    }
}

fn bind_query_operand(
    operand: Operand,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<Operand, Error> {
    bind_query_operand_with_target_type(operand, binding, None, mode)
}

fn bind_query_operand_with_target_type(
    operand: Operand,
    binding: &Binding,
    target_type: Option<&ColumnType>,
    mode: ParamBindingMode,
) -> Result<Operand, Error> {
    Ok(match operand {
        Operand::Param(name) if matches!(mode, ParamBindingMode::RetainAllParams) => {
            Operand::Param(name)
        }
        Operand::Param(name) => {
            let value = binding
                .values()
                .get(&name)
                .cloned()
                .ok_or_else(|| QueryError::MissingParam(name.clone()))?;
            Operand::Literal(
                target_type
                    .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                    .unwrap_or(value),
            )
        }
        Operand::Literal(value) => Operand::Literal(
            target_type
                .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                .unwrap_or(value),
        ),
        Operand::Column(_) | Operand::Claim(_) => operand,
    })
}

fn local_maintained_view_content_witness<'a>(
    versions: &'a [VersionRow],
    table: &str,
    row_uuid: RowUuid,
) -> Option<&'a VersionRow> {
    // `versions_by_tx` is canonically ordered by encoded record, not by write time.
    // Within one transaction the complete content witness sorts after the
    // metadata-only register projection, so search from the back.
    versions.iter().rev().find(|version| {
        version.table() == table
            && version.row_uuid() == row_uuid
            && !version.is_register_record()
            && version.deletion().is_none()
    })
}

fn current_row_has_required_subscription_cells(
    row: &CurrentRow,
    table: &TableSchema,
    projection: Option<&[String]>,
) -> bool {
    table.columns.iter().all(|column| {
        projection.is_some_and(|columns| !columns.contains(&column.name))
            || matches!(column.column_type, ValueType::Nullable(_))
            || row.cell(table, &column.name).is_some()
    })
}

fn contiguous_tx_time_spans(times: &BTreeSet<TxTime>) -> Vec<(TxTime, Option<TxTime>)> {
    let mut spans = Vec::new();
    let mut iter = times.iter().copied();
    let Some(mut start) = iter.next() else {
        return spans;
    };
    let mut last = start;
    for time in iter {
        if last.0.checked_add(1) == Some(time.0) {
            last = time;
            continue;
        }
        spans.push((start, last.0.checked_add(1).map(TxTime)));
        start = time;
        last = time;
    }
    spans.push((start, last.0.checked_add(1).map(TxTime)));
    spans
}

fn sort_query_default_rows(rows: &mut [CurrentRow]) {
    rows.sort_by(default_query_row_order);
}

/// Convert materializer-only rows back to the canonical application row
/// descriptor before exposing them through a one-shot query.  The materializer
/// may retain physical schema/provenance fields while resolving a row, whereas
/// subscriptions are emitted from the public app-row terminal directly.
fn normalize_public_current_rows(
    table: &TableSchema,
    rows: &mut [CurrentRow],
) -> Result<(), Error> {
    let columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    for row in rows {
        *row = row.project(table, &columns)?;
    }
    Ok(())
}

fn default_query_row_order(left: &CurrentRow, right: &CurrentRow) -> Ordering {
    left.row_uuid()
        .to_bytes()
        .cmp(&right.row_uuid().to_bytes())
        .then_with(|| left.projected_tx_alias().cmp(&right.projected_tx_alias()))
        .then_with(|| left.record.raw().cmp(right.record.raw()))
}

fn apply_query_window(query: &crate::query::Query, rows: &mut Vec<CurrentRow>) {
    let offset = query.offset.min(rows.len());
    let limit = query.limit.unwrap_or(rows.len().saturating_sub(offset));
    let end = offset.saturating_add(limit).min(rows.len());
    if offset > 0 || end < rows.len() {
        *rows = rows[offset..end].to_vec();
    }
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
            Predicate::Eq(left, right)
            | Predicate::Ne(left, right)
            | Predicate::Gt(left, right)
            | Predicate::Gte(left, right)
            | Predicate::Lt(left, right)
            | Predicate::Lte(left, right)
            | Predicate::Contains(left, right) => {
                collect_operand_param(left, &mut params);
                collect_operand_param(right, &mut params);
            }
            Predicate::In(operand, choices) => {
                collect_operand_param(operand, &mut params);
                for choice in choices {
                    collect_operand_param(choice, &mut params);
                }
            }
            Predicate::IsNull(operand) => collect_operand_param(operand, &mut params),
            Predicate::EnumMatch { payload, .. } => {
                params.extend(predicate_params(std::slice::from_ref(payload)));
            }
        }
    }
    params
}

fn collect_operand_param(operand: &Operand, params: &mut BTreeSet<String>) {
    if let Operand::Param(param) = operand {
        params.insert(param.clone());
    }
}

fn collect_join_read_tables(join: &crate::query::JoinVia, tables: &mut BTreeSet<String>) {
    tables.insert(join.table.clone());
    if let Some(source_lookup) = &join.source_lookup {
        tables.insert(source_lookup.table.clone());
    }
    for nested_join in &join.nested_joins {
        collect_join_read_tables(nested_join, tables);
    }
}

#[cfg(test)]
fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => None,
        (Value::Nullable(Some(left)), right) => compare_values(left, right),
        (left, Value::Nullable(Some(right))) => compare_values(left, right),
        (Value::U8(left), Value::U8(right)) => left.partial_cmp(right),
        (Value::U16(left), Value::U16(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::I32(left), Value::I32(right)) => left.partial_cmp(right),
        (Value::I64(left), Value::I64(right)) => left.partial_cmp(right),
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

fn current_row_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("$createdBy".to_owned());
    fields.push("$createdAt".to_owned());
    fields.push("$updatedBy".to_owned());
    fields.push("$updatedAt".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    fields
}

fn global_current_storage_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    if include_version {
        fields.extend([
            "schema_version".to_owned(),
            "parents".to_owned(),
            "authored_columns".to_owned(),
        ]);
    }
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("created_by".to_owned());
    fields.push("created_at".to_owned());
    fields.push("updated_by".to_owned());
    fields.push("updated_at".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    if include_settle_position {
        fields.push("global_seq".to_owned());
    }
    fields
}

fn current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    user_column_field(&column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
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

fn empty_authorized_row_id_graph() -> GraphBuilder {
    GraphBuilder::inline_records(
        RecordDescriptor::new([("row_uuid", ValueType::Uuid)]),
        Vec::<Vec<u8>>::new(),
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

fn inline_current_graph_with_source_metadata(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let mut metadata = BTreeMap::new();
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
    {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: None,
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition)
    {
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }

    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let records = rows
        .iter()
        .map(|row| {
            inline_current_record_with_source_metadata(
                table,
                &descriptor,
                row,
                schema_version_alias,
                coverage,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

fn inline_current_record_with_source_metadata(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    let provenance = row.provenance()?.unwrap_or(RowProvenance {
        created_by: AuthorId::SYSTEM,
        created_at: TxTime(0),
        updated_by: AuthorId::SYSTEM,
        updated_at: TxTime(0),
    });
    values.extend([
        Value::Uuid(provenance.created_by.0),
        Value::U64(provenance.created_at.0),
        Value::Uuid(provenance.updated_by.0),
        Value::U64(provenance.updated_at.0),
    ]);
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
    if descriptor.field_index("table").is_some() {
        values.extend([
            Value::String(table.name.clone()),
            Value::String("content".to_owned()),
            Value::U64(schema_version_alias.0),
            Value::Array(Vec::new()),
            Value::Uuid(provenance.created_by.0),
            Value::U64(provenance.created_at.0),
            Value::Uuid(provenance.updated_by.0),
            Value::U64(provenance.updated_at.0),
            Value::Nullable(None),
        ]);
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String(coverage.to_owned()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    Ok(descriptor.create(&values)?)
}

fn inline_include_deleted_current_graph(
    table: &TableSchema,
    rows: Vec<(CurrentRow, bool)>,
) -> Result<GraphBuilder, Error> {
    let descriptor = include_deleted_current_row_descriptor(table);
    let mut records = Vec::with_capacity(rows.len());
    for (row, deleted) in rows {
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
        values.push(Value::Bool(deleted));
        records.push(descriptor.create(&values)?);
    }
    Ok(GraphBuilder::inline_records(descriptor, records))
}

fn inline_branch_current_graph(
    table: &TableSchema,
    rows: Vec<(CurrentRow, TxTime, NodeAlias, Option<BranchId>)>,
    schema_version_alias: SchemaVersionAlias,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let mut metadata = BTreeMap::new();
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
    {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: Some("branch_id".to_owned()),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition)
    {
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }
    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let records = rows
        .iter()
        .map(|(row, tx_time, tx_node, branch)| {
            inline_branch_current_record(
                table,
                &descriptor,
                row,
                schema_version_alias,
                (*tx_time, *tx_node),
                *branch,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

fn inline_branch_current_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    witness: (TxTime, NodeAlias),
    branch_id: Option<BranchId>,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    let provenance = row.provenance()?.unwrap_or(RowProvenance {
        created_by: AuthorId::SYSTEM,
        created_at: TxTime(0),
        updated_by: AuthorId::SYSTEM,
        updated_at: TxTime(0),
    });
    values.extend([
        Value::Uuid(provenance.created_by.0),
        Value::U64(provenance.created_at.0),
        Value::Uuid(provenance.updated_by.0),
        Value::U64(provenance.updated_at.0),
    ]);
    let (tx_time, tx_node_alias) = witness;
    values.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
    if descriptor.field_index("table").is_some() {
        values.extend([
            Value::String(table.name.clone()),
            Value::String("content".to_owned()),
            Value::U64(schema_version_alias.0),
            Value::Array(Vec::new()),
            Value::Uuid(provenance.created_by.0),
            Value::U64(provenance.created_at.0),
            Value::Uuid(provenance.updated_by.0),
            Value::U64(provenance.updated_at.0),
            Value::Nullable(None),
        ]);
        if descriptor.field_index("branch_id").is_some() {
            values.push(Value::Nullable(
                branch_id.map(|branch| Box::new(Value::Uuid(branch.0))),
            ));
        }
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String("branch-current".to_owned()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    Ok(descriptor.create(&values)?)
}

#[cfg(test)]
fn historical_current_graph_full_scan(
    table: &TableSchema,
    table_id: PhysicalTableId,
    position: GlobalSeq,
    history_rows: GraphBuilder,
) -> GraphBuilder {
    let cut_predicate = PredicateExpr::And(vec![
        PredicateExpr::eq("physical_table_id", Value::U64(table_id.0)),
        PredicateExpr::LtEq {
            field: "global_seq".to_owned(),
            value: Value::U64(position.0).into(),
        },
    ])
    .canonicalize();
    let changes_for_layer = |layer: &'static str| {
        GraphBuilder::table("jazz_global_changes").filter(
            PredicateExpr::And(vec![
                cut_predicate.clone(),
                PredicateExpr::eq("layer", Value::Bytes(layer.as_bytes().to_vec())),
            ])
            .canonicalize(),
        )
    };
    let nullable_deletion_type = ValueType::Nullable(Box::new(ValueType::EnumTag(
        groove::records::ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(groove::records::SystemVariantRegistry::deletion_state()),
    )));
    let content_events = changes_for_layer("content").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("content".to_owned())),
        ProjectField::null_typed("deletion", nullable_deletion_type.clone()),
    ]);
    let register_events = changes_for_layer("deletion").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("deletion".to_owned())),
        ProjectField::renamed("_deletion", "deletion"),
    ]);
    let latest_event = GraphBuilder::arg_max_by(
        GraphBuilder::union([content_events.clone(), register_events]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    );
    let content_winners =
        GraphBuilder::arg_max_by(content_events, ["row_uuid"], ["tx_time", "tx_node_id"]);
    let history_rows = history_rows.project(maintained_view_history_storage_field_names(table));
    let content_current = GraphBuilder::join(
        history_rows,
        content_winners,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        ["row_uuid".to_owned()]
            .into_iter()
            .chain(
                table
                    .columns
                    .iter()
                    .map(|column| user_column_field(&column.name)),
            )
            .map(|field| ProjectField::renamed(left_field(&field), field))
            .chain([
                ProjectField::renamed("left.created_by", "$createdBy"),
                ProjectField::renamed("left.created_at", "$createdAt"),
                ProjectField::renamed("left.updated_by", "$updatedBy"),
                ProjectField::renamed("left.updated_at", "$updatedAt"),
                ProjectField::renamed("left.tx_time", "tx_time"),
                ProjectField::renamed("left.tx_node_id", "tx_node_id"),
            ]),
    );
    let latest_content = latest_event.clone().filter(PredicateExpr::eq(
        "event_layer",
        Value::String("content".to_owned()),
    ));
    let content_is_latest = GraphBuilder::join(
        content_current.clone(),
        latest_content,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    );
    let latest_restore = latest_event.filter(
        PredicateExpr::And(vec![
            PredicateExpr::eq("event_layer", Value::String("deletion".to_owned())),
            PredicateExpr::eq(
                "deletion",
                Value::Nullable(Some(Box::new(Value::EnumTag(1)))),
            ),
        ])
        .canonicalize(),
    );
    let restored_content =
        GraphBuilder::join(content_current, latest_restore, ["row_uuid"], ["row_uuid"])
            .project_fields(
                current_row_fields(table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(left_field(&field), field)),
            );
    GraphBuilder::union([content_is_latest, restored_content])
}

fn include_deleted_current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    user_column_field(&column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
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

fn include_deleted_current_graph(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_storage_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
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
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
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
                .map(|field| ProjectField::renamed(left_field(&field), field)),
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
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deletion = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
            )
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&table.name))
                .project(deletion_fields.clone())
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
                    GraphBuilder::table(register_global_current_table_name(&table.name))
                        .project(deletion_fields),
                    ahead_deletion,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ),
        )
    };
    let deleted_winners = deletion_current
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
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
                            right_field(&field)
                        }
                        _ => left_field(&field),
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
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("authored_columns".to_owned());
    fields
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use groove::schema::{ColumnSchema, ColumnType};
    use groove::storage::{Durability, RocksDbStorage};

    use crate::ids::{AuthorId, BranchId, NodeUuid, RowUuid};
    use crate::node::query_engine::{CoverageScope, FieldRequirement, ProgramFactOutput};
    use crate::node::{MergeableCommit, NodeState};
    use crate::peer::PeerState;
    use crate::protocol::{
        CurrentWriteSchema, MigrationLens, ReadViewSourceSpec, ReadViewSpec, RealRowMemberEntry,
        RegisterShapeOptions, RelationEdgeEntry, ResultRowLayer, RowVersionRefEntry, SchemaVersion,
        ShapeAst, Subscribe, SyncMessage, TableLens,
    };
    use crate::query::{
        Aggregate, ArraySubquery, FlatJoin, FlatJoinOn, FlatJoinSource, JoinSourceLookup,
        OrderDirection, PolicyBranch, Query, claim, col, contains, eq, gt, in_list, lit, lte,
        param,
    };
    use crate::schema::{JazzSchema, Policy, TableSchema};

    use super::*;

    #[test]
    fn prepared_relation_terminal_keeps_branch_discriminator_in_public_payload() {
        // Prepared subscriptions wrap each production terminal in a routed
        // projection. `versioned_row_ref_fields` is the public payload list
        // supplied to that projection; omitting this field makes lowering look
        // correct while the decoder receives no branch witness.
        let versioned_ref = |prefix: &str| {
            let branch_field = format!("{prefix}_branch_or_prefix");
            VersionedRowRefSchema {
                row: super::super::query_engine::RowRefSchema {
                    source_field: format!("{prefix}_source"),
                    table_field: format!("{prefix}_table"),
                    row_field: format!("{prefix}_row"),
                },
                version: Some(ResultMembershipVersionSchema::Content(
                    super::super::query_engine::ContentVersionFields {
                        tx_time_field: format!("{prefix}_tx_time"),
                        tx_node_field: format!("{prefix}_tx_node"),
                    },
                )),
                branch_or_prefix_field: Some(branch_field),
            }
        };
        let schema = super::super::query_engine::ProgramFactSchema::RelationEdges(
            super::super::query_engine::RelationEdgeSchema {
                source: versioned_ref("source"),
                path_field: "path".to_owned(),
                target: versioned_ref("target"),
                kind_field: "kind".to_owned(),
                depth_field: None,
                edge_id_field: None,
                branch_field: None,
                role_field: None,
                order_field: None,
                hole_state_field: None,
            },
        );
        let public_payload = fact_public_fields(&schema).expect("relation facts are routable");
        for prefix in ["source", "target"] {
            let branch_field = format!("{prefix}_branch_or_prefix");
            assert!(
                public_payload.contains(&branch_field),
                "prepared routed {prefix} payload must retain its branch discriminator"
            );
        }
    }

    #[test]
    fn branch_source_witness_discriminator_tracks_each_row_lineage() {
        let table = TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]);
        let row = current_row_from_cells(
            &table,
            row(0xf3),
            &BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        )
        .expect("build projected row");
        let metadata = BTreeMap::from([(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: Some("branch_id".to_owned()),
            },
        )]);
        let descriptor = current_row_descriptor_with_hidden_source_fields(&table, &metadata);
        let branch = BranchId::from_bytes([0xf4; 16]);
        let root_record = inline_branch_current_record(
            &table,
            &descriptor,
            &row,
            SchemaVersionAlias(1),
            (TxTime(1), NodeAlias(1)),
            None,
        )
        .expect("encode root/base row in branch view");
        let overlay_record = inline_branch_current_record(
            &table,
            &descriptor,
            &row,
            SchemaVersionAlias(1),
            (TxTime(2), NodeAlias(1)),
            Some(branch),
        )
        .expect("encode branch overlay row");
        let branch_idx = descriptor.field_index("branch_id").expect("branch field");
        assert!(matches!(
            BorrowedRecord::new(&root_record, &descriptor).get_idx(branch_idx),
            Ok(Value::Nullable(None))
        ));
        assert!(matches!(
            BorrowedRecord::new(&overlay_record, &descriptor).get_idx(branch_idx),
            Ok(Value::Nullable(Some(value))) if matches!(*value, Value::Uuid(id) if id == branch.0)
        ));
    }

    /// A coalesced authority re-entry for Alice's document must replace only
    /// that exact member; Bob's ordinary content update in the same batch must
    /// retain update semantics.
    ///
    /// authority ──re-admit alice──► replacement set
    /// bob ──content update─────────► ordinary add (not replacement)
    #[test]
    fn authoritative_replacement_provenance_is_member_specific_in_a_mixed_batch() {
        let member = |row_byte, time| {
            ResultMemberEntry::row((
                groove::Intern::from("documents".to_owned()),
                RowUuid::from_bytes([row_byte; 16]),
                TxId::new(
                    crate::time::TxTime::from(time),
                    NodeUuid::from_bytes([0x91; 16]),
                ),
            ))
        };
        let stale_authority_member = member(0x11, 1);
        let authority_reentry = member(0x11, 2);
        let stable_ordinary_member = member(0x22, 3);
        let ordinary_content_update = member(0x22, 4);
        let provenance = BTreeSet::from([authority_reentry.clone()]);
        let mut result_set = BTreeSet::from([
            stale_authority_member.clone(),
            stable_ordinary_member.clone(),
        ]);
        let mut payloads = BTreeMap::new();

        for added in [&authority_reentry, &ordinary_content_update] {
            replace_stale_authoritative_occurrence_member(
                &mut result_set,
                &mut payloads,
                &provenance,
                added,
                "documents",
                false,
            )
            .expect("reduce mixed authoritative and ordinary additions");
            result_set.insert(added.clone());
        }

        assert!(!result_set.contains(&stale_authority_member));
        assert!(result_set.contains(&authority_reentry));
        assert!(result_set.contains(&stable_ordinary_member));
        assert!(result_set.contains(&ordinary_content_update));
    }

    /// A real settled Edge ViewUpdate seeds the client's authority membership;
    /// a later local content version of that occurrence remains an update when
    /// the ClientLocal maintained graph drains.
    ///
    /// server ──ViewUpdate(issue v1)──► client authority
    /// client ──title v2──────────────► one maintained drain
    #[test]
    fn settled_edge_authority_preserves_an_ordinary_local_content_update() {
        let (_server_dir, mut server) = open_node();
        let (_client_dir, mut client) = open_node();
        let issue = row(0);
        let shape = Query::from("issues")
            .select(["title", "state", "assignee", "priority"])
            .order_by("title", OrderDirection::Asc)
            .validate(&schema())
            .expect("validate issues query");
        let binding = shape.bind(BTreeMap::new()).expect("bind issues query");
        let opts = RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        };
        register_query_shape(&mut server, &shape, opts.clone());
        subscribe_query_binding(&mut server, &shape, &binding);
        register_query_shape(&mut client, &shape, opts.clone());
        subscribe_query_binding(&mut client, &shape, &binding);

        let initial_tx = commit_global_issue(&mut server, 0, "open", AuthorId::SYSTEM, 1);
        let mut peer = PeerState::edge_client(AuthorId::SYSTEM);
        let initial = peer
            .rehydrate_query_with_opts(&mut server, &shape, &binding, opts.clone())
            .expect("serve initial settled issues view");
        client
            .apply_sync_message(initial)
            .expect("apply initial settled issues view");
        let binding_view = *client
            .query
            .settled_result_sets
            .keys()
            .find(|key| key.shape_id == shape.shape_id() && key.binding_id == binding.binding_id())
            .expect("applied ViewUpdate registers a settled binding view");
        assert!(client.has_settled_result_set(binding_view));

        let (local_shape, local_binding, local_plan) = client
            .prepare_query_binding_for_link_in_authorization_mode(
                &shape,
                &binding,
                DurabilityTier::Local,
                AuthorId::SYSTEM,
                QueryAuthorizationMode::ClientLocal,
            )
            .expect("prepare client-local maintained issues query");
        let (mut local, initial_snapshot) = client
            .open_maintained_view_subscription_in_authorization_mode(
                &local_shape,
                &local_binding,
                AuthorId::SYSTEM,
                DurabilityTier::Local,
                &ReadViewSpec::default(),
                Some(local_plan),
                QueryAuthorizationMode::ClientLocal,
            )
            .expect("open client-local maintained issues query");
        assert_eq!(initial_snapshot.root_count, 1);
        client.seed_local_maintained_authoritative_result_membership(&mut local, binding_view);

        let updated_tx = client
            .commit_mergeable(
                MergeableCommit::new("issues", issue, 2_000)
                    .made_by(AuthorId::SYSTEM)
                    .parents(vec![initial_tx])
                    .cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("updated title".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
                        ("priority".to_owned(), Value::U64(0)),
                    ])),
            )
            .expect("commit ordinary local issue update");
        let _ = updated_tx;

        let update = client
            .drain_local_maintained_view_subscription(&mut local, Some(binding_view))
            .expect("drain client-local maintained update")
            .expect("ordinary content update produces a delta");
        assert!(!update.authoritative_membership_changed);
        let issue_occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(issue.0));
        assert!(update.added.iter().any(|(id, _)| id == &issue_occurrence));
        assert!(update.removed.iter().any(|id| id == &issue_occurrence));
        let updated = update
            .added
            .iter()
            .find(|(id, _)| id == &issue_occurrence)
            .expect("updated issue is paired as an add/remove update");
        assert_eq!(
            updated.1.cell(client.table("issues").unwrap(), "title"),
            Some(Value::String("updated title".to_owned()))
        );
    }

    #[test]
    fn required_cell_guard_resolves_a_later_projected_column_by_name() {
        let table = TableSchema::new(
            "items",
            [
                ColumnSchema::new("first", ColumnType::String),
                ColumnSchema::new("second", ColumnType::String),
                ColumnSchema::new("third", ColumnType::String),
            ],
        );
        let row_id = RowUuid(uuid::Uuid::from_u128(1));
        let complete = current_row_from_cells(
            &table,
            row_id,
            &BTreeMap::from([
                ("first".to_owned(), Value::String("one".to_owned())),
                ("second".to_owned(), Value::String("two".to_owned())),
                ("third".to_owned(), Value::String("three".to_owned())),
            ]),
        )
        .expect("build complete row")
        .project(&table, &["third".to_owned()])
        .expect("project later column");
        assert!(current_row_has_required_subscription_cells(
            &complete,
            &table,
            Some(&["third".to_owned()]),
        ));

        let missing = current_row_from_cells(
            &table,
            row_id,
            &BTreeMap::from([
                ("first".to_owned(), Value::String("one".to_owned())),
                ("second".to_owned(), Value::String("two".to_owned())),
            ]),
        )
        .expect("build row missing projected required cell")
        .project(&table, &["third".to_owned()])
        .expect("project missing later column");
        assert!(!current_row_has_required_subscription_cells(
            &missing,
            &table,
            Some(&["third".to_owned()]),
        ));
    }

    #[test]
    fn prepared_integer_bindings_coerce_only_when_representable() {
        let cases = [
            (Value::I64(7), ColumnType::U8, Value::U8(7)),
            (
                Value::U32(u8::MAX as u32),
                ColumnType::U8,
                Value::U8(u8::MAX),
            ),
            (
                Value::U32(u16::MAX as u32),
                ColumnType::U16,
                Value::U16(u16::MAX),
            ),
            (
                Value::U64(u32::MAX as u64),
                ColumnType::U32,
                Value::U32(u32::MAX),
            ),
            (Value::I32(7), ColumnType::U64, Value::U64(7)),
            (
                Value::I64(i32::MIN as i64),
                ColumnType::I32,
                Value::I32(i32::MIN),
            ),
            (
                Value::U64(i64::MAX as u64),
                ColumnType::I64,
                Value::I64(i64::MAX),
            ),
        ];

        for (value, column_type, expected) in cases {
            assert_eq!(coerce_prepared_binding_value(value, &column_type), expected);
        }
    }

    // This lowerer-level assertion protects the case-local descriptor lookup;
    // public one-shot and maintained behavior is exercised by the enum query
    // integration coverage.
    #[test]
    fn payload_enum_normalization_uses_case_local_field_types() {
        let descriptor = RecordDescriptor::new([
            ("shared", ValueType::Uuid),
            ("case_only", ValueType::String),
        ]);
        let source = root_source_id("events");
        let uuid = uuid::Uuid::from_u128(7);
        let predicate = Predicate::Eq(
            Operand::Column("shared".to_owned()),
            Operand::Literal(Value::String(uuid.to_string())),
        );

        let normalized = normalize_enum_payload_predicate(&descriptor, &source, &predicate)
            .expect("case-local field normalizes");
        let NormalizedPredicateExpr::Compare { right, .. } = normalized else {
            panic!("expected comparison");
        };
        let NormalizedValueRef::Literal(bytes) = right else {
            panic!("expected literal");
        };
        assert_eq!(
            postcard::from_bytes::<Value>(&bytes).unwrap(),
            Value::Uuid(uuid)
        );

        let outer_only = Predicate::Eq(
            Operand::Column("outer_only".to_owned()),
            Operand::Literal(Value::String("not a payload field".to_owned())),
        );
        assert!(normalize_enum_payload_predicate(&descriptor, &source, &outer_only).is_err());
    }

    #[test]
    fn prepared_integer_bindings_do_not_wrap_out_of_range_values() {
        let cases = [
            (Value::I64(-1), ColumnType::U8),
            (Value::U16(u8::MAX as u16 + 1), ColumnType::U8),
            (Value::U32(u16::MAX as u32 + 1), ColumnType::U16),
            (Value::U64(u32::MAX as u64 + 1), ColumnType::U32),
            (Value::I64(-1), ColumnType::U64),
            (Value::I64(i32::MIN as i64 - 1), ColumnType::I32),
            (Value::U64(i64::MAX as u64 + 1), ColumnType::I64),
        ];

        for (value, column_type) in cases {
            assert_eq!(
                coerce_prepared_binding_value(value.clone(), &column_type),
                value,
                "unrepresentable values must fail closed rather than wrap"
            );
        }
    }

    #[test]
    fn prepared_nullable_integer_bindings_normalize_exactly_once() {
        let nullable_u8 = ColumnType::Nullable(Box::new(ColumnType::U8));
        let some_i64 = Value::Nullable(Some(Box::new(Value::I64(7))));
        let none = Value::Nullable(None);

        let cases = [
            (
                Value::I64(7),
                ColumnType::U8,
                Value::U8(7),
                "nonnullable source to nonnullable target",
            ),
            (
                Value::I64(7),
                nullable_u8.clone(),
                Value::Nullable(Some(Box::new(Value::U8(7)))),
                "nonnullable source to nullable target",
            ),
            (
                some_i64.clone(),
                ColumnType::U8,
                Value::Nullable(Some(Box::new(Value::U8(7)))),
                "nullable source to nonnullable target",
            ),
            (
                some_i64,
                nullable_u8.clone(),
                Value::Nullable(Some(Box::new(Value::U8(7)))),
                "nullable source to nullable target must not double-wrap",
            ),
            (
                none.clone(),
                ColumnType::U8,
                none.clone(),
                "nullable None to nonnullable target",
            ),
            (
                none.clone(),
                nullable_u8.clone(),
                none,
                "nullable None to nullable target",
            ),
        ];

        for (value, column_type, expected, case) in cases {
            assert_eq!(
                coerce_prepared_binding_value(value, &column_type),
                expected,
                "{case}"
            );
        }

        let out_of_range = Value::Nullable(Some(Box::new(Value::I64(256))));
        for column_type in [ColumnType::U8, nullable_u8] {
            assert_eq!(
                coerce_prepared_binding_value(out_of_range.clone(), &column_type),
                out_of_range,
                "out-of-range nullable integers must not wrap or narrow"
            );
        }
    }

    #[test]
    fn prepared_claim_descriptor_uses_validated_param_type_for_both_equality_orders() {
        let schema = JazzSchema::new([
            TableSchema::new(
                "text_owners",
                [ColumnSchema::new("owner", ColumnType::String)],
            ),
            TableSchema::new(
                "nullable_owners",
                [ColumnSchema::new("owner", ColumnType::String.nullable())],
            ),
            TableSchema::new(
                "uuid_owners",
                [ColumnSchema::new("owner", ColumnType::Uuid)],
            ),
        ]);
        let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0xb4; 16]), schema.clone());
        let claim_param = claim_param_field(&ClaimPath(vec!["user_id".to_owned()]));
        let cases = [
            (
                Query::from("text_owners").filter(eq(col("owner"), param(&claim_param))),
                ColumnType::String,
                Value::String("alice".to_owned()),
            ),
            (
                Query::from("nullable_owners").filter(eq(param(&claim_param), col("owner"))),
                ColumnType::String.nullable(),
                Value::Nullable(Some(Box::new(Value::String("alice".to_owned())))),
            ),
            (
                Query::from("uuid_owners").filter(eq(col("owner"), param(&claim_param))),
                ColumnType::Uuid,
                Value::Uuid(uuid::Uuid::from_bytes([0xb5; 16])),
            ),
        ];

        for (query, expected_type, value) in cases {
            let shape = query.validate(&schema).unwrap();
            let binding = shape
                .bind(BTreeMap::from([(claim_param.clone(), value)]))
                .unwrap();
            let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
            let claims = binding_claim_params_for_shape(&normalized, shape.params());
            assert_eq!(
                claims.get(&claim_param).map(|claim| &claim.ty),
                Some(&expected_type),
                "prepared descriptor must retain the validator's paired-column type",
            );
        }
    }

    #[test]
    fn maintained_root_order_keeps_occurrence_sidecar_aligned() {
        let descriptor =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("user_rank", ValueType::U64)]);
        let make_row = |id: u8, rank: u64| {
            CurrentRow::new(
                "todos",
                OwnedRecord::new(
                    descriptor
                        .create(&[
                            Value::Uuid(uuid::Uuid::from_bytes([id; 16])),
                            Value::U64(rank),
                        ])
                        .expect("test row"),
                    descriptor,
                ),
            )
        };
        let occurrence = |id: u8| {
            OutputOccurrenceId::single_source(ObjectId::from_uuid(uuid::Uuid::from_bytes([id; 16])))
        };
        let mut rows = vec![make_row(0xa1, 3), make_row(0xb2, 1), make_row(0xc3, 2)];
        let mut occurrences = vec![occurrence(0xa1), occurrence(0xb2), occurrence(0xc3)];
        let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
        let table = TableSchema::new("todos", [ColumnSchema::new("rank", ColumnType::U64)]);

        NodeState::<RocksDbStorage>::sort_query_rows_with_occurrences(
            &query,
            Some(&table),
            &mut rows,
            &mut occurrences,
        )
        .expect("sort maintained roots");

        assert_eq!(
            rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
            vec![
                RowUuid(uuid::Uuid::from_bytes([0xb2; 16])),
                RowUuid(uuid::Uuid::from_bytes([0xc3; 16])),
                RowUuid(uuid::Uuid::from_bytes([0xa1; 16]))
            ]
        );
        assert_eq!(
            occurrences,
            vec![occurrence(0xb2), occurrence(0xc3), occurrence(0xa1)]
        );
    }

    #[test]
    fn predicate_params_collects_every_operand_position_and_operator() {
        let predicates = [Predicate::All(vec![
            Predicate::Gt(param("left"), col("value")),
            Predicate::In(
                col("kind"),
                vec![lit("fixed"), param("choice"), param("second_choice")],
            ),
            Predicate::IsNull(param("nullable")),
            Predicate::Not(Box::new(Predicate::Lte(col("limit"), param("upper")))),
        ])];

        assert_eq!(
            predicate_params(&predicates),
            BTreeSet::from([
                "choice".to_owned(),
                "left".to_owned(),
                "nullable".to_owned(),
                "second_choice".to_owned(),
                "upper".to_owned(),
            ])
        );
    }

    #[test]
    fn join_read_tables_include_source_lookups_and_nested_joins() {
        let nested = crate::query::JoinVia {
            table: "nested_junction".to_owned(),
            on_column: "target".to_owned(),
            target: Default::default(),
            source_column: None,
            source_lookup: Some(JoinSourceLookup {
                table: "nested_lookup".to_owned(),
                row_id_source_column: "lookup_id".to_owned(),
                value_column: "value".to_owned(),
            }),
            correlated_filters: vec![],
            filters: vec![],
            nested_joins: vec![],
        };
        let root = crate::query::JoinVia {
            table: "root_junction".to_owned(),
            on_column: "target".to_owned(),
            target: Default::default(),
            source_column: None,
            source_lookup: Some(JoinSourceLookup {
                table: "root_lookup".to_owned(),
                row_id_source_column: "lookup_id".to_owned(),
                value_column: "value".to_owned(),
            }),
            correlated_filters: vec![],
            filters: vec![],
            nested_joins: vec![nested],
        };
        let mut tables = BTreeSet::new();

        collect_join_read_tables(&root, &mut tables);

        assert_eq!(
            tables,
            BTreeSet::from([
                "nested_junction".to_owned(),
                "nested_lookup".to_owned(),
                "root_junction".to_owned(),
                "root_lookup".to_owned(),
            ])
        );
    }

    #[test]
    fn unordered_array_windows_materialize_per_parent_row_id_order() {
        let windows =
            NodeState::<RocksDbStorage>::relation_snapshot_no_order_windows(&[ArraySubquery::new(
                "comments", "comments", "todo_id", "id",
            )
            .offset(1)
            .limit(2)]);
        assert_eq!(
            windows
                .get("comments")
                .map(|window| (window.offset, window.limit)),
            Some((1, Some(2)))
        );
    }

    #[test]
    fn reverse_table_lens_projects_membership_and_content_version_sources() {
        // This is intentionally an internal assertion: the public subscription
        // regression proves the observable row result, while this checks that
        // both inputs to its content-version semi-join select the same source.
        let base = JazzSchema::new([TableSchema::new(
            "users",
            [ColumnSchema::new("email", ColumnType::String)],
        )]);
        let evolved = JazzSchema::new([TableSchema::new(
            "people",
            [ColumnSchema::new("email", ColumnType::String)],
        )]);
        let evolved_payload = SchemaVersion::new(evolved);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xa2; 16]), base.clone());
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved_payload.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved_payload.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "users".to_owned(),
                            to: "people".to_owned(),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved_payload.id,
            },
        })
        .unwrap();

        let shape = Query::from("users").validate(&base).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let query_request = node
            .current_query_program_request(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorId::SYSTEM,
                CurrentQueryProgramOutput::MaintainedView,
                &ReadViewSpec::default(),
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        let read_view = query_request.reads.primary;
        let source_request = SourceRequest {
            source: root_source_id("users"),
            visibility: RowVisibility::Visible,
            authorization: SourceAuthorizationRequest::System,
            requirements: SourceRequirements {
                app_fields: FieldRequirement::All,
                metadata: BTreeSet::from([SourceMetadataRequirement::VersionPayloads]),
            },
        };
        let expected_people_current = physical_global_current_table_name(
            node.physical_table_id_for_schema(evolved_payload.id, "people")
                .unwrap(),
        );
        let mut resolver = CurrentQuerySourceResolver {
            node: &mut node,
            read_view: &read_view,
            prepare_branch_subscription_sources: false,
            inline_sources: BTreeMap::new(),
            access_paths: BTreeMap::new(),
            current_projection_targets: BTreeMap::new(),
        };

        assert!(resolver.needs_projected_current_source("users"));
        let resolved = resolver.resolve_source(&source_request).unwrap();
        assert_eq!(
            resolver.current_projection_targets.len(),
            1,
            "the Global source and its content-version sidecar share one cached projection target",
        );
        let content_version = resolved
            .content_version
            .expect("version-payload requirements need a content-version source");

        assert!(
            format!("{:?}", resolved.graph).contains(&expected_people_current),
            "membership source must include the shared physical current table"
        );
        assert!(
            format!("{:?}", content_version.graph).contains(&expected_people_current),
            "content-version source must include the shared physical current table"
        );
    }

    #[test]
    fn binding_source_shape_is_descriptor_and_claim_path_identity() {
        let mut params = BTreeMap::new();
        params.insert("route".to_owned(), ColumnType::String);
        let claims = BTreeMap::from([(
            claim_param_field(&ClaimPath(vec!["sub".to_owned()])),
            ProgramClaimParam {
                path: ClaimPath(vec!["sub".to_owned()]),
                ty: ColumnType::Uuid,
            },
        )]);

        let first = query_binding_source_shape_for_parts(&params, &claims);
        let second = query_binding_source_shape_for_parts(&params, &claims);
        assert_eq!(first, second);
        assert!(!first.contains("jazz-query:"));

        let mut different_params = params.clone();
        different_params.insert("route".to_owned(), ColumnType::Uuid);
        assert_ne!(
            first,
            query_binding_source_shape_for_parts(&different_params, &claims)
        );

        let different_claims = BTreeMap::from([(
            claim_param_field(&ClaimPath(vec!["team".to_owned(), "id".to_owned()])),
            ProgramClaimParam {
                path: ClaimPath(vec!["team".to_owned(), "id".to_owned()]),
                ty: ColumnType::Uuid,
            },
        )]);
        assert_ne!(
            first,
            query_binding_source_shape_for_parts(&params, &different_claims)
        );
    }

    #[test]
    fn nested_read_policies_reuse_an_outer_equivalent_claim_slot() {
        // A maintained outer query owns this prepared source. Its first
        // nested policy uses the legacy claim field name; a later protected
        // source validates the same claim as Text and must reuse that slot
        // rather than add a redundant typed alias under the already-active
        // source name.
        let schema = JazzSchema::new([
            TableSchema::new(
                "public_profiles",
                [ColumnSchema::new("name", ColumnType::String)],
            )
            .with_read_policy(Policy::public()),
            TableSchema::new(
                "private_chats",
                [ColumnSchema::new("owner", ColumnType::String)],
            )
            .with_read_policy(Policy::shape(
                Query::from("private_chats").filter(eq(col("owner"), claim("user_id"))),
            )),
        ]);
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xf4; 16]), schema.clone());
        let identity = author(0xf5);
        node.set_session_claims(
            identity,
            BTreeMap::from([("user_id".to_owned(), Value::String(identity.0.to_string()))]),
        );

        let claim_name = claim_param_field(&ClaimPath(vec!["user_id".to_owned()]));
        let outer_claims = BTreeMap::from([(
            claim_name.clone(),
            ProgramClaimParam {
                path: ClaimPath(vec!["user_id".to_owned()]),
                ty: ColumnType::String,
            },
        )]);
        let source_shape = query_binding_source_shape_for_parts(&BTreeMap::new(), &outer_claims);
        let binding = Query::from("public_profiles")
            .validate(&schema)
            .unwrap()
            .bind(BTreeMap::new())
            .unwrap();

        for table in ["public_profiles", "private_chats"] {
            let request = node
                .table_read_policy_authorization_request(
                    node.catalogue.current_schema_version_id,
                    table,
                    identity,
                    ParamBindingMode::RetainAllParams,
                    DurabilityTier::Edge,
                    Some(source_shape.clone()),
                    BTreeMap::new(),
                    outer_claims.clone(),
                )
                .unwrap();
            let program = node.compile_query_program_request(request).unwrap();
            node.subscribe_lowered_program(
                program,
                &binding,
                source_shape.clone(),
                PreparedClaimBindingMode::Strict,
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{table} must reuse the outer String claim slot instead of registering a divergent binding descriptor: {error:?}"
                )
            });
        }
    }

    #[test]
    fn nested_read_policy_claim_slots_do_not_cross_validated_types() {
        let schema = JazzSchema::new([
            TableSchema::new(
                "uuid_owners",
                [ColumnSchema::new("owner", ColumnType::Uuid)],
            ),
            TableSchema::new(
                "other_string_owners",
                [ColumnSchema::new("owner", ColumnType::String)],
            ),
            TableSchema::new(
                "nullable_string_owners",
                [ColumnSchema::new("owner", ColumnType::String.nullable())],
            ),
        ]);
        let plain_name = claim_param_field(&ClaimPath(vec!["user_id".to_owned()]));
        let outer_slots = BTreeMap::from([(
            plain_name.clone(),
            ProgramClaimParam {
                path: ClaimPath(vec!["user_id".to_owned()]),
                ty: ColumnType::String,
            },
        )]);
        let mut query = Query::from("uuid_owners").filter(eq(col("owner"), claim("user_id")));
        let mut binding_values = BTreeMap::new();
        bind_scope_claim_operands(
            &mut query,
            &BTreeMap::from([("user_id".to_owned(), Value::String("not-a-uuid".to_owned()))]),
            &mut binding_values,
        );
        let slots = disambiguate_policy_claim_params_with_outer_slots(
            &mut query,
            &schema,
            &mut binding_values,
            &outer_slots,
        )
        .expect("the policy shape is valid before binding values");

        let uuid_slot = typed_claim_param_alias(&plain_name, &ColumnType::Uuid);
        assert!(slots.contains_key(&uuid_slot));
        assert!(
            !slots.contains_key(&plain_name),
            "a String outer claim slot must never be reused for a UUID policy operand"
        );
        assert!(
            query
                .validate(&schema)
                .unwrap()
                .params()
                .contains_key(&uuid_slot),
            "the policy query itself must reference the UUID-specific slot"
        );

        let other_path = ClaimPath(vec!["account_id".to_owned()]);
        let other_name = claim_param_field(&other_path);
        let mut other_path_query =
            Query::from("other_string_owners").filter(eq(col("owner"), claim("account_id")));
        let mut other_path_values = BTreeMap::new();
        bind_scope_claim_operands(
            &mut other_path_query,
            &BTreeMap::from([(
                "account_id".to_owned(),
                Value::String("same-type-different-path".to_owned()),
            )]),
            &mut other_path_values,
        );
        let other_path_slots = disambiguate_policy_claim_params_with_outer_slots(
            &mut other_path_query,
            &schema,
            &mut other_path_values,
            &outer_slots,
        )
        .expect("same-type claim at another path validates");
        let other_path_alias = typed_claim_param_alias(&other_name, &ColumnType::String);
        assert!(other_path_slots.contains_key(&other_path_alias));
        assert!(
            !other_path_slots.contains_key(&plain_name),
            "an outer user_id slot must not be reused by account_id merely because both are String"
        );

        let mut nullable_query =
            Query::from("nullable_string_owners").filter(eq(col("owner"), claim("user_id")));
        let mut nullable_values = BTreeMap::new();
        bind_scope_claim_operands(
            &mut nullable_query,
            &BTreeMap::from([(
                "user_id".to_owned(),
                Value::String("nullable-boundary".to_owned()),
            )]),
            &mut nullable_values,
        );
        let nullable_slots = disambiguate_policy_claim_params_with_outer_slots(
            &mut nullable_query,
            &schema,
            &mut nullable_values,
            &outer_slots,
        )
        .expect("nullable policy claim validates");
        let nullable_alias = typed_claim_param_alias(&plain_name, &ColumnType::String.nullable());
        assert!(nullable_slots.contains_key(&nullable_alias));
        assert!(
            !nullable_slots.contains_key(&plain_name),
            "a non-nullable String outer slot must not be reused for a nullable String operand"
        );
    }

    #[test]
    fn one_claim_path_has_distinct_prepared_slots_per_numeric_width() {
        let field = claim_param_field(&ClaimPath(vec!["access_level".to_owned()]));
        let i32_alias = typed_claim_param_alias(&field, &ColumnType::I32);
        let i64_alias = typed_claim_param_alias(&field, &ColumnType::I64);

        assert_ne!(i32_alias, i64_alias);
        assert_eq!(
            claim_path_from_param_field(&i32_alias),
            Some(ClaimPath(vec!["access_level".to_owned()]))
        );
        let slots = BTreeMap::from([
            (
                i32_alias,
                ProgramClaimParam {
                    path: ClaimPath(vec!["access_level".to_owned()]),
                    ty: ColumnType::I32,
                },
            ),
            (
                i64_alias,
                ProgramClaimParam {
                    path: ClaimPath(vec!["access_level".to_owned()]),
                    ty: ColumnType::I64,
                },
            ),
        ]);
        assert_eq!(slots.len(), 2);
        assert_eq!(
            slots
                .values()
                .map(|slot| slot.path.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([ClaimPath(vec!["access_level".to_owned()])])
        );
    }

    #[test]
    fn prepared_nested_policy_claim_routes_keep_outer_descriptor_slots() {
        // This intentionally exercises the compiler/prepare boundary rather
        // than a public transport: a JS invite subscription previously failed
        // while Groove prepared its shared binding descriptor, before any
        // observable query could run. Keeping the reproducer here makes the
        // descriptor contract cheap to validate without NAPI or a browser.
        let schema = JazzSchema::new([
            TableSchema::new(
                "chats",
                [
                    ColumnSchema::new("name", ColumnType::String.nullable()),
                    ColumnSchema::new("isPublic", ColumnType::Bool),
                    ColumnSchema::new("createdBy", ColumnType::String),
                    ColumnSchema::new("joinCode", ColumnType::String.nullable()),
                ],
            )
            .with_read_policy(Policy::shape(
                Query::from("chats")
                    .filter(Predicate::Any(Vec::new()))
                    .policy_branch(PolicyBranch::single_alternative_from_query(
                        Query::from("chats").filter(eq(col("isPublic"), lit(true))),
                    ))
                    .policy_branch(PolicyBranch::single_alternative_from_query(
                        Query::from("chats").filter(eq(col("joinCode"), claim("join_code"))),
                    ))
                    .policy_branch(PolicyBranch::single_alternative_from_query(
                        Query::from("chats").join_via_column(
                            "chatMembers",
                            "chatId",
                            "id",
                            [eq(col("userId"), claim("user_id"))],
                        ),
                    )),
            ))
            .with_write_policy(Policy::public()),
            TableSchema::new(
                "chatMembers",
                [
                    ColumnSchema::new("chatId", ColumnType::Uuid),
                    ColumnSchema::new("userId", ColumnType::String),
                    ColumnSchema::new("joinCode", ColumnType::String.nullable()),
                ],
            )
            .with_reference("chatId", "chats")
            .with_read_policy(Policy::shape(
                Query::from("chatMembers")
                    .filter(Predicate::Any(Vec::new()))
                    .policy_branch(PolicyBranch::single_alternative_from_query(
                        Query::from("chatMembers").filter(eq(col("userId"), claim("user_id"))),
                    ))
                    .policy_branch(PolicyBranch::single_alternative_from_query(
                        Query::from("chatMembers").join_via_column(
                            "chatMembers",
                            "chatId",
                            "chatId",
                            [eq(col("userId"), claim("user_id"))],
                        ),
                    )),
            ))
            .with_write_policy(Policy::public()),
            TableSchema::new(
                "profiles",
                [
                    ColumnSchema::new("userId", ColumnType::String),
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("avatar", ColumnType::String.nullable()),
                ],
            )
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
            TableSchema::new(
                "messages",
                [
                    ColumnSchema::new("chatId", ColumnType::Uuid),
                    ColumnSchema::new("senderId", ColumnType::Uuid),
                    ColumnSchema::new("text", ColumnType::String),
                    ColumnSchema::new("createdAt", ColumnType::U64),
                ],
            )
            .with_reference("chatId", "chats")
            .with_reference("senderId", "profiles")
            .with_read_policy(Policy::shape(Query::from("messages").join_via_column(
                "chatMembers",
                "chatId",
                "chatId",
                [eq(col("userId"), claim("user_id"))],
            )))
            .with_write_policy(Policy::public()),
        ]);
        let identity = author(0xa9);
        let (_client_dir, mut client) =
            open_node_with_uuid(NodeUuid::from_bytes([0xa7; 16]), schema.clone());
        client.set_session_claims(
            identity,
            BTreeMap::from([(
                "join_code".to_owned(),
                Value::String("invite-123".to_owned()),
            )]),
        );
        let client_shape = Query::from("chats")
            .filter(eq(col("id"), param("id")))
            .validate(&schema)
            .unwrap();
        let client_binding = client_shape
            .bind(BTreeMap::from([(
                "id".to_owned(),
                Value::Uuid(row(0xaa).0),
            )]))
            .unwrap();
        let (shape, binding, _client_plan) = client
            .prepare_query_binding_for_link(
                &client_shape,
                &client_binding,
                DurabilityTier::Edge,
                identity,
            )
            .expect("prepare retained invite binding on the client before server coverage");
        register_query_shape(
            &mut client,
            &shape,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        );
        subscribe_query_binding(&mut client, &shape, &binding);

        let (_server_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xa8; 16]), schema.clone());
        node.set_session_claims(
            identity,
            BTreeMap::from([(
                "join_code".to_owned(),
                Value::String("invite-123".to_owned()),
            )]),
        );
        let chat = row(0xaa);
        let chat_tx = node
            .commit_mergeable(
                MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
                    ("name".to_owned(), Value::Nullable(None)),
                    ("isPublic".to_owned(), Value::Bool(false)),
                    (
                        "createdBy".to_owned(),
                        Value::String(identity.0.to_string()),
                    ),
                    (
                        "joinCode".to_owned(),
                        Value::Nullable(Some(Box::new(Value::String("invite-123".to_owned())))),
                    ),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            chat_tx,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let profile = row(0xac);
        let profile_tx = node
            .commit_mergeable(
                MergeableCommit::new("profiles", profile, 11).cells(BTreeMap::from([
                    ("userId".to_owned(), Value::String(identity.0.to_string())),
                    ("name".to_owned(), Value::String("Alice".to_owned())),
                    ("avatar".to_owned(), Value::Nullable(None)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            profile_tx,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let message = row(0xad);
        let message_tx = node
            .commit_mergeable(
                MergeableCommit::new("messages", message, 12).cells(BTreeMap::from([
                    ("chatId".to_owned(), Value::Uuid(chat.0)),
                    ("senderId".to_owned(), Value::Uuid(profile.0)),
                    (
                        "text".to_owned(),
                        Value::String("invite-only seed".to_owned()),
                    ),
                    ("createdAt".to_owned(), Value::U64(1)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            message_tx,
            Fate::Accepted,
            Some(GlobalSeq(3)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        // Mirror the wire receiver: the server reconstructs the client
        // binding from RegisterShape + Subscribe before it prepares the
        // maintained graph under the invite-authenticated identity.
        register_query_shape(
            &mut node,
            &shape,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        );
        subscribe_query_binding(&mut node, &shape, &binding);
        let registered_values = node
            .query
            .registered_bindings
            .get(&shape.shape_id())
            .and_then(|bindings| bindings.get(&binding.binding_id()))
            .map(|registered| registered.values.clone())
            .expect("server reconstructed the subscribed invite binding");
        let server_binding = shape
            .bind(
                shape
                    .params()
                    .keys()
                    .cloned()
                    .zip(registered_values)
                    .collect(),
            )
            .expect("registered wire values reconstruct the invite binding");
        let program = node
            .compile_current_query_program_for_read_view(
                &shape,
                &server_binding,
                DurabilityTier::Edge,
                identity,
                CurrentQueryProgramOutput::MaintainedView,
                &ReadViewSpec::default(),
            )
            .expect("compile invite policy topology");
        let typed_join_code = typed_claim_param_alias(
            &claim_param_field(&ClaimPath(vec!["join_code".to_owned()])),
            &ColumnType::String.nullable(),
        );
        assert!(
            program
                .request
                .input
                .binding
                .values
                .contains_key(&typed_join_code),
            "the prepared invite binding must retain its nullable typed join-code slot"
        );
        let members = node.table("chatMembers").unwrap().clone();
        let members_policy = node
            .table_read_policy_authorization_request(
                shape.schema_version(),
                "chatMembers",
                identity,
                ParamBindingMode::RetainAllParams,
                DurabilityTier::Edge,
                program.request.input.binding.source_shape.clone(),
                program.request.input.binding.extra_user_params.clone(),
                program.request.input.binding.claim_params.clone(),
            )
            .expect("compile nested chat-members policy against the invite binding");
        let members_authorized = node
            .policy_filtered_current_source_graph_via_query_engine(
                Ok(members_policy),
                node.maintained_view_content_current_with_version(&members, DurabilityTier::Edge)
                    .expect("compile chat-members storage source"),
                &global_current_storage_fields(&members, true, true),
            )
            .expect("route nested chat-members policy through the invite binding");
        assert!(
            members_authorized.route_fields.contains(&typed_join_code),
            "a nested policy source must carry the outer invite slot even when its own policy only consumes user_id"
        );
        let member_fields =
            crate::node::query_engine::graph_declared_output_fields(&members_authorized.graph)
                .expect("nested policy graph has a declared descriptor");
        assert!(
            member_fields.contains(&typed_join_code),
            "a membership CommitUnit must reach the live invite subscription with its outer claim route"
        );
        let app_program = node
            .compile_current_query_program_for_read_view(
                &shape,
                &server_binding,
                DurabilityTier::Edge,
                identity,
                CurrentQueryProgramOutput::AppRows,
                &ReadViewSpec::default(),
            )
            .expect("compile invite app-row topology");
        let system_program = node
            .compile_current_query_program_for_read_view(
                &shape,
                &server_binding,
                DurabilityTier::Edge,
                AuthorId::SYSTEM,
                CurrentQueryProgramOutput::MaintainedView,
                &ReadViewSpec::default(),
            )
            .expect("System/asBackend reads must not require invite claim values");
        assert!(
            system_program.request.input.binding.claim_params.is_empty(),
            "System/asBackend prepared descriptors cannot retain session claim slots"
        );
        for terminal in &program.lowered.terminals {
            let expected_routes = match &terminal.output {
                OutputTerminalSchema::Fact(fact) => output_routing_fields_for_query_eval(fact),
                OutputTerminalSchema::AppRows(_) => BTreeSet::new(),
            };
            let declared = crate::node::query_engine::graph_declared_output_fields(&terminal.graph)
                .expect("the invite terminal has a statically declared output descriptor");
            assert!(
                expected_routes.is_subset(&declared),
                "every advertised invite route must be produced by its terminal; expected {expected_routes:?}, declared {declared:?}"
            );
        }
        let mut descriptors_by_shape = BTreeMap::new();
        let mut projected_binding_fields = BTreeMap::new();
        for terminal in program
            .lowered
            .terminals
            .iter()
            .chain(app_program.lowered.terminals.iter())
        {
            collect_binding_source_descriptor_fields(&terminal.graph, &mut descriptors_by_shape);
            collect_binding_source_projected_fields(&terminal.graph, &mut projected_binding_fields);
        }
        assert!(
            projected_binding_fields
                .values()
                .flatten()
                .all(|fields| fields.contains(&typed_join_code)),
            "every nested policy binding projection must preserve the outer nullable invite slot; {projected_binding_fields:?}"
        );
        assert!(
            descriptors_by_shape
                .values()
                .all(|descriptors| descriptors.len() == 1),
            "every binding-source shape must retain one shared descriptor; {descriptors_by_shape:?}"
        );

        node.open_seeded_maintained_subscription_view(
            &shape,
            &server_binding,
            identity,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
        )
        .expect(
            "nested policy claim routes must prepare and bind against the root binding descriptor",
        );
        node.query_rows_with_prepared_plan_for_identity(
            &shape,
            &server_binding,
            DurabilityTier::Edge,
            None,
            identity,
        )
        .expect("one-shot nested policy claim routes must bind against the root descriptor");

        let mut edge = PeerState::edge_client(identity);
        let update = edge
            .rehydrate_query_with_opts(
                &mut node,
                &shape,
                &server_binding,
                RegisterShapeOptions {
                    tier: DurabilityTier::Edge,
                    ..RegisterShapeOptions::default()
                },
            )
            .expect("the serving maintained view must retain the invite claim route");
        client
            .apply_sync_message(update)
            .expect("the client must materialize the invited chat update");

        // The browser failure occurred only after the invite subscription was
        // live and accepting membership was committed. This must wake the
        // maintained graph without dropping its outer invite claim route.
        let member_tx = node
            .commit_mergeable(MergeableCommit::new("chatMembers", row(0xab), 11).cells(
                BTreeMap::from([
                    ("chatId".to_owned(), Value::Uuid(chat.0)),
                    ("userId".to_owned(), Value::String(identity.0.to_string())),
                    (
                        "joinCode".to_owned(),
                        Value::Nullable(Some(Box::new(Value::String("invite-123".to_owned())))),
                    ),
                ]),
            ))
            .unwrap();
        node.apply_fate_update(
            member_tx,
            Fate::Accepted,
            Some(GlobalSeq(4)),
            Some(DurabilityTier::Global),
        )
        .expect("a live invite subscription must tolerate its membership CommitUnit");
        edge.query_update(&mut node, &shape, &server_binding)
            .expect("flushing the live invite subscription after membership must preserve its claim route");

        // The invite has now become ordinary membership. A later normal
        // session must materialize an already-existing private message through
        // its sender include and timestamp order, not merely discover chat
        // membership itself.
        node.set_session_claims(
            identity,
            BTreeMap::from([("user_id".to_owned(), Value::String(identity.0.to_string()))]),
        );
        let message_shape = Query::from("messages")
            .filter(eq(col("chatId"), param("chat_id")))
            .array_subquery(ArraySubquery::new("sender", "profiles", "id", "senderId"))
            .order_by("createdAt", OrderDirection::Asc)
            .validate(&schema)
            .expect("validate normal-member message query");
        let message_binding = message_shape
            .bind(BTreeMap::from([(
                "chat_id".to_owned(),
                Value::Uuid(chat.0),
            )]))
            .expect("bind normal-member message query");
        let message_rows = node
            .query_rows_with_prepared_plan_for_identity(
                &message_shape,
                &message_binding,
                DurabilityTier::Edge,
                None,
                identity,
            )
            .expect("materialize private seed message with sender include and timestamp order");
        assert_eq!(
            message_rows
                .iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>(),
            vec![message],
            "normal membership reads the seed message after invite acceptance"
        );
        node.open_seeded_maintained_subscription_view(
            &message_shape,
            &message_binding,
            identity,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
        )
        .expect("prepare and hydrate normal-member message include/order subscription");
        let (_normal_client_dir, mut normal_client) =
            open_node_with_uuid(NodeUuid::from_bytes([0xae; 16]), schema.clone());
        normal_client.set_session_claims(
            identity,
            BTreeMap::from([("user_id".to_owned(), Value::String(identity.0.to_string()))]),
        );
        let mut normal_membership_peer = PeerState::edge_client(identity);
        normal_client
            .apply_sync_message(
                normal_membership_peer
                    .current_rows_update(&mut node, "chatMembers")
                    .expect("serve the accepted membership to the normal client"),
            )
            .expect("normal client applies its accepted membership before querying messages");
        let simple_message_shape = Query::from("messages")
            .filter(eq(col("chatId"), param("chat_id")))
            .validate(&schema)
            .expect("validate normal-member message query without include");
        let simple_message_binding = simple_message_shape
            .bind(BTreeMap::from([(
                "chat_id".to_owned(),
                Value::Uuid(chat.0),
            )]))
            .expect("bind normal-member message query without include");
        register_query_shape(
            &mut normal_client,
            &simple_message_shape,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        );
        subscribe_query_binding(
            &mut normal_client,
            &simple_message_shape,
            &simple_message_binding,
        );
        let mut normal_simple_peer = PeerState::edge_client(identity);
        normal_client
            .apply_sync_message(
                normal_simple_peer
                    .rehydrate_query_with_opts(
                        &mut node,
                        &simple_message_shape,
                        &simple_message_binding,
                        RegisterShapeOptions {
                            tier: DurabilityTier::Edge,
                            ..RegisterShapeOptions::default()
                        },
                    )
                    .expect("serve normal-member message snapshot without include"),
            )
            .expect("client applies normal-member message snapshot without include");
        assert_eq!(
            normal_client
                .query_rows_for_client(
                    &simple_message_shape,
                    &simple_message_binding,
                    DurabilityTier::Edge,
                    identity,
                )
                .expect("client materializes the private seed message without include")
                .iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>(),
            vec![message],
            "the normal client must first materialize the private seed message without include"
        );
        register_query_shape(
            &mut normal_client,
            &message_shape,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        );
        subscribe_query_binding(&mut normal_client, &message_shape, &message_binding);
        let mut normal_peer = PeerState::edge_client(identity);
        let normal_update = normal_peer
            .rehydrate_query_with_opts(
                &mut node,
                &message_shape,
                &message_binding,
                RegisterShapeOptions {
                    tier: DurabilityTier::Edge,
                    ..RegisterShapeOptions::default()
                },
            )
            .expect("serve normal-member message include/order snapshot");
        let normal_versions = normal_update
            .expand_version_carriers_for_receive()
            .expect("expand normal-member message include/order payloads");
        if let SyncMessage::ViewUpdate {
            version_bundles, ..
        } = &normal_versions
        {
            let (profile_bundle, profile_version) = version_bundles
                .iter()
                .find_map(|bundle| {
                    bundle
                        .versions
                        .iter()
                        .find(|version| {
                            version.table() == "profiles" && version.row_uuid() == profile
                        })
                        .map(|version| (bundle, version))
                })
                .expect("the relation snapshot ships the sender version");
            assert_eq!(
                profile_bundle.tx.tx_id, profile_tx,
                "the sender witness must retain the profile version identity rather than borrow the message anchor"
            );
            assert_eq!(
                profile_version
                    .record()
                    .borrowed()
                    .get_idx(7)
                    .expect("decode sender wire userId"),
                Value::Nullable(Some(Box::new(Value::String(identity.0.to_string())))),
                "the relation sender version ships userId content"
            );
        } else {
            panic!("expected normal-member view update")
        }
        let missing = normal_client
            .missing_known_state_row_version_refs(&normal_versions)
            .expect("inspect normal-member message include/order repair requirements");
        assert!(
            missing.is_empty(),
            "the server snapshot already carries every visible row-version payload; missing {missing:?}"
        );
        if !missing.is_empty() {
            let messages = normal_peer
                .handle_row_versions_fetch(
                    &mut node,
                    SyncMessage::FetchRowVersions {
                        requests: missing.clone(),
                    },
                )
                .expect("serve normal-member message include/order repair payloads");
            let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
                panic!("expected row-version repair payloads")
            };
            normal_client
                .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
                .expect("apply normal-member message include/order repair payloads");
        }
        normal_client
            .apply_sync_message(normal_versions)
            .expect("client applies normal-member message include/order snapshot");
        assert!(
            normal_client
                .current_rows("profiles", DurabilityTier::Local)
                .expect("inspect locally materialized sender rows")
                .iter()
                .any(|row| row.row_uuid() == profile),
            "the include snapshot must deliver the sender row before local query evaluation"
        );
        assert_eq!(
            normal_client
                .current_rows("profiles", DurabilityTier::Local)
                .expect("inspect the local sender table")
                .iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([profile]),
            "the sender table must not receive the message row through relation delivery"
        );
        assert_eq!(
            normal_client
                .current_rows("profiles", DurabilityTier::Local)
                .expect("inspect the local sender payload")
                .into_iter()
                .next()
                .and_then(|row| row.cell(normal_client.table("profiles").unwrap(), "userId")),
            Some(Value::String(identity.0.to_string())),
            "the delivered sender version retains its required userId"
        );
        let (local_shape, local_binding, local_plan) = normal_client
            .prepare_query_binding_for_link_in_authorization_mode(
                &message_shape,
                &message_binding,
                DurabilityTier::Edge,
                identity,
                QueryAuthorizationMode::ClientLocal,
            )
            .expect(
                "prepare the same client-local maintained relation subscription as the browser",
            );
        let (_local_subscription, local_snapshot) = normal_client
            .open_maintained_view_subscription_in_authorization_mode(
                &local_shape,
                &local_binding,
                identity,
                DurabilityTier::Edge,
                &ReadViewSpec::default(),
                Some(local_plan),
                QueryAuthorizationMode::ClientLocal,
            )
            .expect("open the client-local maintained relation subscription");
        assert_eq!(
            local_snapshot.root_count, 1,
            "the maintained client-local relation subscription retains the seed message"
        );
        let local_one_shot = normal_client
            .query_relation_snapshot_for_client(
                &message_shape,
                &message_binding,
                DurabilityTier::Edge,
                identity,
                &ReadViewSpec::default(),
            )
            .expect("materialize the client-local relation snapshot API used by WASM");
        assert_eq!(
            local_one_shot.root_count, 1,
            "the client-local relation snapshot API retains the seed message"
        );
        assert_eq!(
            normal_client
                .query_rows_for_client(
                    &message_shape,
                    &message_binding,
                    DurabilityTier::Edge,
                    identity,
                )
                .expect("client materializes normal-member message include/order snapshot")
                .iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>(),
            vec![message],
            "the normal client must retain the seed message when the sender include is added"
        );
    }

    fn collect_binding_source_descriptor_fields(
        graph: &GraphBuilder,
        descriptors_by_shape: &mut BTreeMap<String, BTreeSet<BTreeSet<String>>>,
    ) {
        match graph {
            GraphBuilder::BindingSource { shape, output } => {
                let fields = output
                    .fields()
                    .iter()
                    .map(|field| field.name.clone().expect("binding fields are named"))
                    .collect();
                descriptors_by_shape
                    .entry(shape.clone())
                    .or_default()
                    .insert(fields);
            }
            GraphBuilder::Recursive { seed, step, .. } => {
                collect_binding_source_descriptor_fields(seed, descriptors_by_shape);
                collect_binding_source_descriptor_fields(step, descriptors_by_shape);
            }
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::Project { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::CollectBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => {
                collect_binding_source_descriptor_fields(input, descriptors_by_shape);
            }
            GraphBuilder::Union { inputs } => {
                for input in inputs {
                    collect_binding_source_descriptor_fields(input, descriptors_by_shape);
                }
            }
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                collect_binding_source_descriptor_fields(left, descriptors_by_shape);
                collect_binding_source_descriptor_fields(right, descriptors_by_shape);
            }
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. } => {}
        }
    }

    fn collect_binding_source_projected_fields(
        graph: &GraphBuilder,
        projected_by_shape: &mut BTreeMap<String, BTreeSet<BTreeSet<String>>>,
    ) {
        match graph {
            GraphBuilder::Project { input, fields } => {
                if let GraphBuilder::BindingSource { shape, .. } = input.as_ref() {
                    projected_by_shape.entry(shape.clone()).or_default().insert(
                        fields
                            .iter()
                            .map(|field| field.output_name.clone())
                            .collect(),
                    );
                }
                collect_binding_source_projected_fields(input, projected_by_shape);
            }
            GraphBuilder::Recursive { seed, step, .. } => {
                collect_binding_source_projected_fields(seed, projected_by_shape);
                collect_binding_source_projected_fields(step, projected_by_shape);
            }
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::CollectBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => {
                collect_binding_source_projected_fields(input, projected_by_shape);
            }
            GraphBuilder::Union { inputs } => {
                for input in inputs {
                    collect_binding_source_projected_fields(input, projected_by_shape);
                }
            }
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                collect_binding_source_projected_fields(left, projected_by_shape);
                collect_binding_source_projected_fields(right, projected_by_shape);
            }
            GraphBuilder::BindingSource { .. }
            | GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. } => {}
        }
    }

    fn register_query_shape(
        node: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        opts: RegisterShapeOptions,
    ) {
        node.apply_sync_message(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts,
        })
        .unwrap();
    }

    fn subscribe_query_binding(
        node: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) {
        subscribe_query_binding_with_opts(node, shape, binding, RegisterShapeOptions::default());
    }

    fn subscribe_query_binding_with_opts(
        node: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) {
        let values = shape
            .params()
            .keys()
            .map(|name| binding.values().get(name).cloned().unwrap())
            .collect();
        node.apply_sync_message(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: opts.read_view_key(),
            },
            values,
            known_state: None,
        }))
        .unwrap();
    }

    fn register_shape_binding_for_receiver(
        node: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) {
        register_query_shape(node, shape, RegisterShapeOptions::default());
        subscribe_query_binding(node, shape, binding);
    }

    fn lowered_current_app_rows_graph(
        node: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        read_view: &ReadViewSpec,
    ) -> GraphBuilder {
        let program = node
            .compile_current_query_program_for_read_view(
                shape,
                binding,
                DurabilityTier::Local,
                identity,
                CurrentQueryProgramOutput::AppRows,
                read_view,
            )
            .expect("compile current query program");
        lowered_app_rows_graph(&program).expect("app rows graph")
    }

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

    fn signed_metric_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "metrics",
            [
                ColumnSchema::new("bucket", ColumnType::String),
                ColumnSchema::new("score", ColumnType::I64),
            ],
        )])
    }

    fn owner_policy_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("requiresAdmin", ColumnType::Bool),
            ],
        )
        .with_read_policy(Query::from("issues").filter(eq(col("assignee"), claim("sub"))))])
    }

    #[test]
    fn lowered_groove_graph_differs_for_distinct_identity_claims() {
        let schema = owner_policy_schema();
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xa1; 16]), schema.clone());
        let shape = Query::from("issues").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();

        let alice_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            author(0xa1),
            &ReadViewSpec::default(),
        );
        let bob_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            author(0xb2),
            &ReadViewSpec::default(),
        );

        assert_ne!(
            alice_graph, bob_graph,
            "claim('sub') must be encoded in the lowered Groove descriptor graph"
        );
    }

    #[test]
    fn lowered_groove_graph_differs_for_distinct_session_claim_values() {
        let schema = JazzSchema::new([TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("requiresAdmin", ColumnType::Bool),
            ],
        )]);
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xa2; 16]), schema.clone());
        let identity = author(0xa3);
        let shape = Query::from("issues")
            .filter(eq(col("requiresAdmin"), claim("isAdmin")))
            .validate(&schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();

        node.set_session_claims(
            identity,
            BTreeMap::from([("isAdmin".to_owned(), Value::Bool(true))]),
        );
        let admin_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            identity,
            &ReadViewSpec::default(),
        );

        node.set_session_claims(
            identity,
            BTreeMap::from([("isAdmin".to_owned(), Value::Bool(false))]),
        );
        let non_admin_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            identity,
            &ReadViewSpec::default(),
        );

        assert_ne!(
            admin_graph, non_admin_graph,
            "session claim values must be encoded in the lowered Groove descriptor graph"
        );
    }

    #[test]
    fn lowered_groove_graph_differs_for_distinct_read_views() {
        let schema = JazzSchema::new([TableSchema::new(
            "docs",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xa4; 16]), schema.clone());
        let shape = Query::from("docs").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let identity = AuthorId::SYSTEM;
        let branch_id = BranchId::from_bytes([0xbe; 16]);
        node.create_branch(branch_id).unwrap();

        let current_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            identity,
            &ReadViewSpec::default(),
        );
        let branch_graph = lowered_current_app_rows_graph(
            &mut node,
            &shape,
            &binding,
            identity,
            &ReadViewSpec {
                source: ReadViewSourceSpec::Branch {
                    branch: branch_id.0,
                },
                ..ReadViewSpec::default()
            },
        );

        assert_ne!(
            current_graph, branch_graph,
            "read-view source must be encoded in the lowered Groove descriptor graph"
        );
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

    /// Stores a version in the non-base schema partition. The extra `body`
    /// cell makes using the base history descriptor observably wrong at the
    /// native row-batch boundary.
    fn evolved_todos_version() -> (
        tempfile::TempDir,
        NodeState<RocksDbStorage>,
        TableSchema,
        RowUuid,
        TxId,
    ) {
        let base = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let evolved_todos = TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
            ],
        );
        let evolved_payload = SchemaVersion::new(JazzSchema::new([evolved_todos.clone()]));
        let (dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe1; 16]), base.clone());
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved_payload.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved_payload.id,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![LensOp::AddColumn {
                            column: "body".to_owned(),
                            default: Value::String("base-default".to_owned()),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved_payload.id,
            },
        })
        .unwrap();
        let todo = row(0xe2);
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("todos", todo, 0xe3).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("partition-title".to_owned()),
                    ),
                    (
                        "body".to_owned(),
                        Value::String("partition-body".to_owned()),
                    ),
                ])),
            )
            .unwrap();
        (dir, node, evolved_todos, todo, tx_id)
    }

    #[test]
    fn authoritative_reset_version_uses_non_base_partition_descriptor() {
        let (_dir, mut node, evolved_table, todo, tx_id) = evolved_todos_version();
        let table = node.table("todos").unwrap().clone();
        let row = node
            .materialize_authoritative_reset_version_row("todos", todo, tx_id, None)
            .unwrap()
            .expect("stored evolved version");
        assert_eq!(
            row.cell(&table, "title"),
            Some(Value::String("partition-title".to_owned()))
        );
        let alias = *node
            .node_aliases
            .get(&tx_id.node)
            .expect("local node alias");
        let version = node
            .query_version_by_alias("todos", todo, VersionLayer::Content, tx_id.time, alias)
            .unwrap()
            .expect("non-base partition version");
        assert_eq!(version.tx_time(), tx_id.time);
        assert_eq!(version.tx_node_alias(), alias);
        assert_eq!(
            version.cell(&evolved_table, "body").unwrap(),
            Some(Value::String("partition-body".to_owned()))
        );
    }

    #[test]
    fn relation_edge_target_uses_non_base_partition_descriptor() {
        let (_dir, mut node, evolved_table, todo, tx_id) = evolved_todos_version();
        let table = node.table("todos").unwrap().clone();
        let alias = *node
            .node_aliases
            .get(&tx_id.node)
            .expect("local node alias");
        let row = node
            .materialize_relation_edge_target_row(
                &ReadViewSpec::default(),
                node.catalogue.current_schema_version_id,
                "todos",
                todo,
                tx_id.time,
                alias,
            )
            .unwrap();
        assert_eq!(
            row.cell(&table, "title"),
            Some(Value::String("partition-title".to_owned()))
        );
        let version = node
            .query_version_by_alias("todos", todo, VersionLayer::Content, tx_id.time, alias)
            .unwrap()
            .expect("non-base partition version");
        assert_eq!(version.tx_time(), tx_id.time);
        assert_eq!(version.tx_node_alias(), alias);
        assert_eq!(
            version.cell(&evolved_table, "body").unwrap(),
            Some(Value::String("partition-body".to_owned()))
        );
    }

    /// Proves that an authoritative relation-edge witness is rendered through
    /// the subscriber's newer read lens, rather than decoding the old
    /// authored record with the new descriptor.
    ///
    /// alice(v1 row) ──relation witness──► bob(v2 read lens)
    ///                                  └──► v2 defaulted cell
    ///
    /// This is deliberately an internal seam test: the production relation
    /// snapshot receives only an exact `(table, row, tx)` witness here; the
    /// broader client/edge scenario remains the black-box catalogue test.
    #[test]
    fn relation_edge_target_projects_old_witness_into_read_schema() {
        let base = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe4; 16]), base.clone());
        let todo = row(0xe5);
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("todos", todo, 0xe6).cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("written-by-alice".to_owned()),
                )])),
            )
            .expect("commit v1 todo");

        let evolved_table = TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
            ],
        );
        let evolved = SchemaVersion::new(JazzSchema::new([evolved_table.clone()]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved.id,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![LensOp::AddColumn {
                            column: "body".to_owned(),
                            default: Value::String("from-lens-default".to_owned()),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish v2 lens");

        let alias = *node
            .node_aliases
            .get(&tx_id.node)
            .expect("local node alias");
        let row = node
            .materialize_relation_edge_target_row(
                &ReadViewSpec::default(),
                evolved.id,
                "todos",
                todo,
                tx_id.time,
                alias,
            )
            .expect("render projected relation target");
        assert_eq!(
            row.cell(&evolved_table, "title"),
            Some(Value::String("written-by-alice".to_owned()))
        );
        assert_eq!(
            row.cell(&evolved_table, "body"),
            Some(Value::String("from-lens-default".to_owned()))
        );
    }

    /// Proves that a remote authority reset renders an old joined target in
    /// the subscription schema, including table rename and added-column lens
    /// operations.
    ///
    /// alice(v1 users row) ──authority reset witness──► bob(v2 people read)
    ///                                           └──► renamed table + default
    ///
    /// The direct seam is necessary because this reset path receives only the
    /// authority's exact relation-edge version tuple; end-to-end delivery is
    /// covered by the catalogue integration scenarios.
    #[test]
    fn authoritative_reset_relation_target_projects_old_renamed_witness() {
        let base = JazzSchema::new([TableSchema::new(
            "users",
            [ColumnSchema::new("name", ColumnType::String)],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe7; 16]), base.clone());
        let user = row(0xe8);
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("users", user, 0xe9).cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String("alice".to_owned()),
                )])),
            )
            .expect("commit v1 user");

        let people = TableSchema::new(
            "people",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("label", ColumnType::String),
            ],
        );
        let evolved = SchemaVersion::new(JazzSchema::new([people.clone()]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![
                            LensOp::RenameTable {
                                from: "users".to_owned(),
                                to: "people".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "label".to_owned(),
                                default: Value::String("migrated".to_owned()),
                            },
                        ],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish people lens");

        let target_version = RowVersionRefEntry {
            tx: tx_id,
            schema_version: None,
            layer: ResultRowLayer::Content,
            batch: None,
            branch_or_prefix: None,
            row_digest: None,
        };
        let row = node
            // The wire fact names the canonical authored table.  The receiver
            // must lens it to the v2 read table before materializing it.
            .materialize_authoritative_reset_relation_edge_target(
                evolved.id,
                "users",
                user,
                &target_version,
            )
            .expect("render authority relation target")
            .expect("authority has stored target witness");
        assert_eq!(row.table(), "people");
        assert_eq!(
            row.cell(&people, "name"),
            Some(Value::String("alice".to_owned()))
        );
        assert_eq!(
            row.cell(&people, "label"),
            Some(Value::String("migrated".to_owned()))
        );

        let canonical_edge = RelationEdgeEntry {
            path: "author".to_owned(),
            // The root is already expressed in Bob's result schema; only the
            // related witness needs the lineage translation here.
            source_table: groove::Intern::new("people".to_owned()),
            source_row: user,
            target_table: groove::Intern::new("users".to_owned()),
            target_row: user,
            kind: None,
            source_version: None,
            target_version: Some(target_version),
            depth: None,
            edge_id: None,
            branch: None,
            role: None,
            order: None,
            hole_state: None,
        };
        let read_edge = node
            .project_relation_edge_through_read_schema(&canonical_edge, evolved.id)
            .expect("project canonical edge identity for reset index");
        assert_eq!(canonical_edge.target_table.as_str(), "users");
        assert_eq!(read_edge.target_table, "people");
        assert_eq!(read_edge.target_row, user);
    }

    #[test]
    fn authoritative_reset_relation_target_projects_two_hop_canonical_witness() {
        let v1 = JazzSchema::new([TableSchema::new(
            "users",
            [ColumnSchema::new("name", ColumnType::String)],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf0; 16]), v1.clone());
        let user = row(0xf1);
        let tx_id = node
            .commit_mergeable(
                MergeableCommit::new("users", user, 0xf2).cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String("alice".to_owned()),
                )])),
            )
            .expect("commit v1 user");

        let v2 = SchemaVersion::new(JazzSchema::new([TableSchema::new(
            "people",
            [ColumnSchema::new("name", ColumnType::String)],
        )]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                v2.clone(),
                MigrationLens::new(
                    v1.version_id(),
                    v2.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "users".to_owned(),
                            to: "people".to_owned(),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish v2 rename");

        let members = TableSchema::new(
            "members",
            [
                ColumnSchema::new("display_name", ColumnType::String),
                ColumnSchema::new("origin", ColumnType::String),
            ],
        );
        let v3 = SchemaVersion::new(JazzSchema::new([members.clone()]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(SchemaLineagePublication::new(
                v3.clone(),
                MigrationLens::new(
                    v2.id,
                    v3.id,
                    vec![TableLens {
                        source_table: "people".to_owned(),
                        target_table: "members".to_owned(),
                        ops: vec![
                            LensOp::RenameTable {
                                from: "people".to_owned(),
                                to: "members".to_owned(),
                            },
                            LensOp::RenameColumn {
                                from: "name".to_owned(),
                                to: "display_name".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "origin".to_owned(),
                                default: Value::String("v1".to_owned()),
                            },
                        ],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish v3 rename");

        let target_version = RowVersionRefEntry {
            tx: tx_id,
            schema_version: Some(v1.version_id()),
            layer: ResultRowLayer::Content,
            batch: None,
            branch_or_prefix: None,
            row_digest: None,
        };
        let edge = RelationEdgeEntry {
            path: "author".to_owned(),
            source_table: groove::Intern::new("members".to_owned()),
            source_row: user,
            target_table: groove::Intern::new("users".to_owned()),
            target_row: user,
            kind: None,
            source_version: None,
            target_version: Some(target_version.clone()),
            depth: None,
            edge_id: None,
            branch: None,
            role: None,
            order: None,
            hole_state: None,
        };
        let projected_edge = node
            .project_relation_edge_through_read_schema(&edge, v3.id)
            .expect("project canonical edge through both lenses");
        assert_eq!(projected_edge.target_table, "members");

        let query = Query::from("members")
            .validate(&v3.schema)
            .expect("validate v3 members query");
        let binding = query.bind(BTreeMap::new()).expect("bind members query");
        let binding_view = BindingViewKey {
            shape_id: query.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        };
        node.query
            .settled_result_sets
            .insert(binding_view, BTreeSet::new());
        node.query.settled_program_facts.insert(
            binding_view,
            BTreeSet::from([ProgramFactEntry::RelationEdge(edge.clone())]),
        );
        let settled_rows = node
            .settled_binding_view_source_rows(
                "members",
                v3.id,
                binding_view,
                SettledBindingRows::ResultMembers,
            )
            .expect("project canonical settled relation source through both lenses");
        assert_eq!(settled_rows.len(), 1);
        assert_eq!(settled_rows[0].table(), "members");

        let row = node
            .materialize_authoritative_reset_relation_edge_target(
                v3.id,
                "users",
                user,
                &target_version,
            )
            .expect("render canonical relation witness through v3")
            .expect("stored target witness");
        assert_eq!(row.table(), "members");
        assert_eq!(
            row.cell(&members, "display_name"),
            Some(Value::String("alice".to_owned()))
        );
        assert_eq!(
            row.cell(&members, "origin"),
            Some(Value::String("v1".to_owned()))
        );
    }

    /// A v2 flat join must correlate the lens-projected v1 post and author
    /// cells, rather than only materializing each source independently.
    ///
    /// alice ──v1 users/posts──► node ──users→people lens──► v2 flat join
    #[test]
    fn flat_join_correlates_projected_v1_sources_across_table_rename() {
        let v1 = JazzSchema::new([
            TableSchema::new(
                "users",
                [
                    ColumnSchema::new("id", ColumnType::Uuid),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            ),
            TableSchema::new(
                "posts",
                [
                    ColumnSchema::new("id", ColumnType::Uuid),
                    ColumnSchema::new("author_id", ColumnType::Uuid),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            ),
        ]);
        let people = TableSchema::new(
            "people",
            [
                ColumnSchema::new("id", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
            ],
        );
        let v2 = SchemaVersion::new(JazzSchema::new([
            people,
            TableSchema::new(
                "posts",
                [
                    ColumnSchema::new("id", ColumnType::Uuid),
                    ColumnSchema::new("author_id", ColumnType::Uuid),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            ),
        ]));
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf6; 16]), v1.clone());
        let (_client_dir, mut client) =
            open_node_with_uuid(NodeUuid::from_bytes([0xf9; 16]), v1.clone());
        let author = row(0xf7);
        let post = row(0xf8);
        let mismatched_author_row = row(0xf9);
        let mismatched_author_id = row(0xfa);
        let mismatched_post = row(0xfb);
        let author_tx = node
            .commit_mergeable(
                MergeableCommit::new("users", author, 1).cells(BTreeMap::from([
                    ("id".to_owned(), Value::Uuid(author.0)),
                    ("name".to_owned(), Value::String("alice".to_owned())),
                ])),
            )
            .expect("commit v1 author");
        node.apply_fate_update(
            author_tx,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .expect("settle v1 author");
        let post_tx = node
            .commit_mergeable(
                MergeableCommit::new("posts", post, 2).cells(BTreeMap::from([
                    ("id".to_owned(), Value::Uuid(post.0)),
                    ("author_id".to_owned(), Value::Uuid(author.0)),
                    ("title".to_owned(), Value::String("hello".to_owned())),
                ])),
            )
            .expect("commit v1 post");
        node.apply_fate_update(
            post_tx,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .expect("settle v1 post");
        let mismatched_author_tx = node
            .commit_mergeable(
                MergeableCommit::new("users", mismatched_author_row, 3).cells(BTreeMap::from([
                    ("id".to_owned(), Value::Uuid(mismatched_author_id.0)),
                    ("name".to_owned(), Value::String("unmatched".to_owned())),
                ])),
            )
            .expect("commit v1 author with distinct row identity");
        node.apply_fate_update(
            mismatched_author_tx,
            Fate::Accepted,
            Some(GlobalSeq(3)),
            Some(DurabilityTier::Global),
        )
        .expect("settle mismatched v1 author");
        let mismatched_post_tx = node
            .commit_mergeable(MergeableCommit::new("posts", mismatched_post, 4).cells(
                BTreeMap::from([
                    ("id".to_owned(), Value::Uuid(mismatched_post.0)),
                    ("author_id".to_owned(), Value::Uuid(mismatched_author_id.0)),
                    (
                        "title".to_owned(),
                        Value::String("must not join".to_owned()),
                    ),
                ]),
            ))
            .expect("commit v1 post whose foreign key is not the author row identity");
        node.apply_fate_update(
            mismatched_post_tx,
            Fate::Accepted,
            Some(GlobalSeq(4)),
            Some(DurabilityTier::Global),
        )
        .expect("settle mismatched v1 post");
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                v2.clone(),
                MigrationLens::new(
                    v1.version_id(),
                    v2.id,
                    vec![
                        TableLens {
                            source_table: "users".to_owned(),
                            target_table: "people".to_owned(),
                            ops: vec![LensOp::RenameTable {
                                from: "users".to_owned(),
                                to: "people".to_owned(),
                            }],
                        },
                        TableLens {
                            source_table: "posts".to_owned(),
                            target_table: "posts".to_owned(),
                            ops: Vec::new(),
                        },
                    ],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish users to people lens");
        client
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: AuthorId::SYSTEM,
                catalogue_seq: 1,
                publication: Box::new(SchemaLineagePublication::new(
                    v2.clone(),
                    MigrationLens::new(
                        v1.version_id(),
                        v2.id,
                        vec![
                            TableLens {
                                source_table: "users".to_owned(),
                                target_table: "people".to_owned(),
                                ops: vec![LensOp::RenameTable {
                                    from: "users".to_owned(),
                                    to: "people".to_owned(),
                                }],
                            },
                            TableLens {
                                source_table: "posts".to_owned(),
                                target_table: "posts".to_owned(),
                                ops: Vec::new(),
                            },
                        ],
                    ),
                    Vec::<String>::new(),
                    Vec::<String>::new(),
                )),
            })
            .expect("publish users to people lens to client");
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: v2.id,
            },
        })
        .expect("activate v2 read schema");
        client
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: AuthorId::SYSTEM,
                pointer: CurrentWriteSchema {
                    revision: 1,
                    schema: v2.id,
                },
            })
            .expect("activate v2 client read schema");

        for table in ["people", "posts"] {
            let shape = Query::from(table)
                .validate(&v2.schema)
                .expect("validate source");
            let binding = shape.bind(BTreeMap::new()).expect("bind source");
            assert_eq!(
                node.query_rows_at(&shape, &binding, GlobalSeq(4))
                    .expect("read projected source")
                    .len(),
                2,
                "{table} must independently project its v1 row"
            );
        }
        let mut query = Query::from("posts");
        query.flat_join = Some(FlatJoin {
            root_alias: None,
            sources: vec![FlatJoinSource {
                table: "people".to_owned(),
                alias: None,
                on: FlatJoinOn {
                    left: "posts.author_id".to_owned(),
                    right: "people.id".to_owned(),
                },
            }],
        });
        let shape = query.validate(&v2.schema).expect("validate v2 flat join");
        let binding = shape.bind(BTreeMap::new()).expect("bind v2 flat join");
        let rows = node
            .query_rows_at(&shape, &binding, GlobalSeq(4))
            .expect("evaluate v2 flat join");
        assert_eq!(
            rows.len(),
            1,
            "flat joins must use the source row identity for `id`, not an arbitrary stored id cell"
        );

        let opts = RegisterShapeOptions {
            tier: DurabilityTier::Global,
            ..RegisterShapeOptions::default()
        };
        register_query_shape(&mut node, &shape, opts.clone());
        subscribe_query_binding_with_opts(&mut node, &shape, &binding, opts.clone());
        register_query_shape(&mut client, &shape, opts.clone());
        subscribe_query_binding_with_opts(&mut client, &shape, &binding, opts.clone());
        let binding_view =
            BindingViewKey::new(shape.shape_id(), binding.binding_id(), opts.read_view_key());
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        let mut peer = PeerState::edge_client(AuthorId::SYSTEM);
        let known_author = RowVersionRef::new("users", author, author_tx);
        peer.declare_known_state(
            subscription,
            Some(KnownStateDeclaration::ExactVersionSet {
                versions: vec![known_author.clone()],
            }),
        );
        let update = peer
            .rehydrate_query_with_opts(&mut node, &shape, &binding, opts.clone())
            .expect("rehydrate maintained v2 flat join");
        let missing = client
            .missing_known_state_row_version_refs(&update)
            .expect("detect omitted canonical contributor body");
        assert_eq!(missing, vec![known_author]);
        let repair = peer
            .handle_row_versions_fetch(
                &mut node,
                SyncMessage::FetchRowVersions {
                    requests: missing.clone(),
                },
            )
            .expect("serve canonical contributor repair");
        let [SyncMessage::RowVersionPayloads { version_bundles }] = repair.as_slice() else {
            panic!("known contributor repair must carry row-version payloads");
        };
        client
            .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
            .expect("apply canonical contributor repair");
        client
            .apply_sync_message(update.clone())
            .expect("apply maintained v2 flat join on client");
        let SyncMessage::ViewUpdate {
            reset_result_set,
            result_member_adds,
            ..
        } = update
        else {
            panic!("flat join rehydrate must emit a view update");
        };
        assert!(reset_result_set);
        assert_eq!(
            result_member_adds.len(),
            1,
            "maintained v2 flat join must retain the projected source tuple"
        );
        let snapshot = client
            .authoritative_reset_snapshot_for_binding_view(&shape, binding_view)
            .expect("materialize applied flat-join authority snapshot")
            .expect("applied flat-join authority snapshot");
        assert_eq!(snapshot.root_count, 1);
        // The authority payload can render this tuple, but the receiver's
        // local IVM must instead rebuild it from canonical source versions.
        assert_eq!(
            client
                .query_rows_for_client(&shape, &binding, DurabilityTier::Global, AuthorId::SYSTEM)
                .expect("read applied v2 flat join on client")
                .len(),
            1,
            "the client must retain the authority-maintained flat join tuple"
        );

        let updated_author_tx = node
            .commit_mergeable(
                MergeableCommit::new("people", author, 5).cells(BTreeMap::from([
                    ("id".to_owned(), Value::Uuid(author.0)),
                    ("name".to_owned(), Value::String("alice".to_owned())),
                ])),
            )
            .expect("update renamed author");
        node.apply_fate_update(
            updated_author_tx,
            Fate::Accepted,
            Some(GlobalSeq(5)),
            Some(DurabilityTier::Global),
        )
        .expect("settle renamed author update");
        let replacement = peer
            .query_update_for_subscription_with_opts(
                &mut node,
                subscription,
                &shape,
                &binding,
                opts.clone(),
            )
            .expect("publish flat tuple replacement");
        let SyncMessage::ViewUpdate {
            reset_result_set,
            version_carriers,
            version_bundles,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        } = &replacement
        else {
            panic!("flat tuple replacement must emit a view update");
        };
        assert!(
            !reset_result_set,
            "unchanged result membership must take the non-reset rehydrate path"
        );
        assert!(
            result_member_adds.is_empty() && result_member_removes.is_empty(),
            "a no-op source version must retain the same result member"
        );
        let outgoing_contributor_adds = program_fact_adds
            .iter()
            .filter(|fact| {
                matches!(
                    fact,
                    ProgramFactEntry::ContributingMembers(contribution)
                        if contribution
                            .role
                            .as_deref()
                            .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                )
            })
            .count();
        let outgoing_contributor_removes = program_fact_removes
            .iter()
            .filter(|fact| {
                matches!(
                    fact,
                    ProgramFactEntry::ContributingMembers(contribution)
                        if contribution
                            .role
                            .as_deref()
                            .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                )
            })
            .count();
        assert_eq!(outgoing_contributor_adds, 1);
        assert_eq!(outgoing_contributor_removes, 1);
        let mut replacement_bundles = version_bundles.clone();
        replacement_bundles.extend(
            crate::protocol::expand_version_carriers(version_carriers)
                .expect("expand replacement contributor bundles"),
        );
        assert_eq!(
            replacement_bundles
                .iter()
                .filter(|bundle| bundle.tx.tx_id == updated_author_tx)
                .flat_map(|bundle| &bundle.versions)
                .count(),
            1,
            "the changed canonical contributor must ship exactly one body"
        );
        assert!(program_fact_removes.iter().any(|fact| {
            matches!(
                fact,
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                        && contribution
                            .contributor
                            .as_real_row()
                            .and_then(RealRowMemberEntry::row_projection)
                            .is_some_and(|(table, row, tx)| table.to_string() == "users" && row == author && tx == author_tx)
            )
        }));
        assert!(program_fact_adds.iter().any(|fact| {
            matches!(
                fact,
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                        && contribution
                            .contributor
                            .as_real_row()
                            .and_then(RealRowMemberEntry::row_projection)
                            .is_some_and(|(table, row, tx)| table.to_string() == "people" && row == author && tx == updated_author_tx)
            )
        }));
        client
            .apply_sync_message(replacement)
            .expect("apply flat tuple replacement");
        let active_contributors = client
            .query
            .settled_program_facts
            .get(&binding_view)
            .expect("flat tuple facts remain scoped to the binding view")
            .iter()
            .filter(|fact| {
                matches!(
                    fact,
                    ProgramFactEntry::ContributingMembers(contribution)
                        if contribution
                            .role
                            .as_deref()
                            .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(active_contributors.len(), 1);
        assert!(matches!(
            active_contributors[0],
            ProgramFactEntry::ContributingMembers(contribution)
                if contribution
                    .contributor
                    .as_real_row()
                    .and_then(RealRowMemberEntry::row_projection)
                    .is_some_and(|(table, row, tx)| table.to_string() == "people" && row == author && tx == updated_author_tx)
        ));
        assert_eq!(
            client
                .query_rows_for_client(&shape, &binding, DurabilityTier::Global, AuthorId::SYSTEM)
                .expect("read retained flat tuple after no-op source version")
                .len(),
            1
        );
    }

    /// A canonical relation witness can name a branch-only v1 row. Projection
    /// and reset materialization must honor its branch discriminator, lens the
    /// old `users` table to `people`, and never substitute root history.
    #[test]
    fn branch_relation_target_projects_old_renamed_witness() {
        let base = JazzSchema::new([TableSchema::new(
            "users",
            [ColumnSchema::new("name", ColumnType::String)],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xea; 16]), base.clone());
        let branch = BranchId::from_bytes([0xeb; 16]);
        node.create_branch(branch).expect("create branch");
        let user = row(0xec);
        let tx_id = node
            .commit_mergeable_on_branch(
                branch,
                MergeableCommit::new("users", user, 0xed).cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String("branch-alice".to_owned()),
                )])),
            )
            .expect("commit branch-only v1 user");

        let people = TableSchema::new("people", [ColumnSchema::new("name", ColumnType::String)]);
        let evolved = SchemaVersion::new(JazzSchema::new([people.clone()]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "users".to_owned(),
                            to: "people".to_owned(),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish people lens");
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .expect("activate people schema");

        let alias = *node.node_aliases.get(&tx_id.node).expect("node alias");
        assert!(
            node.query_version_by_alias("users", user, VersionLayer::Content, tx_id.time, alias,)
                .expect("query root history")
                .is_none(),
            "branch-only relation witness must not be found through root history"
        );
        let branch_version = RowVersionRefEntry {
            tx: tx_id,
            schema_version: Some(base.version_id()),
            layer: ResultRowLayer::Content,
            batch: None,
            branch_or_prefix: Some(branch.to_bytes()),
            row_digest: None,
        };
        let canonical_edge = RelationEdgeEntry {
            path: "author".to_owned(),
            source_table: groove::Intern::new("people".to_owned()),
            source_row: user,
            target_table: groove::Intern::new("users".to_owned()),
            target_row: user,
            kind: None,
            source_version: None,
            target_version: Some(branch_version.clone()),
            depth: None,
            edge_id: None,
            branch: None,
            role: None,
            order: None,
            hole_state: None,
        };
        let projected = node
            .project_relation_edge_through_read_schema(&canonical_edge, evolved.id)
            .expect("project branch edge identity");
        assert_eq!(projected.target_table, "people");

        node.commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("people", user, 0xee)
                .parents(vec![tx_id])
                .cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String("branch-bob".to_owned()),
                )])),
        )
        .expect("commit a later branch winner");

        let row = node
            .materialize_authoritative_reset_relation_edge_target(
                evolved.id,
                "users",
                user,
                &branch_version,
            )
            .expect("materialize branch relation target")
            .expect("branch target row exists");
        assert_eq!(row.table(), "people");
        assert_eq!(
            row.cell(&people, "name"),
            Some(Value::String("branch-alice".to_owned())),
            "the authority reset must render its exact v1 witness, not the later branch winner"
        );
    }

    #[test]
    fn renamed_branch_terminal_resolves_root_target_from_emitted_read_table() {
        let issue = row(0xf8);
        let v1 = JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("key", ColumnType::Uuid),
                ],
            ),
            TableSchema::new(
                "users",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("issue", ColumnType::Uuid),
                ],
            ),
        ]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf5; 16]), v1.clone());
        let user = row(0xf6);
        let user_tx = node
            .commit_mergeable(
                MergeableCommit::new("users", user, 1).cells(BTreeMap::from([
                    ("name".to_owned(), Value::String("root-alice".to_owned())),
                    ("issue".to_owned(), Value::Uuid(issue.0)),
                ])),
            )
            .expect("commit root user");
        node.apply_fate_update(
            user_tx,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .expect("settle root user before branch snapshot");

        let issues = TableSchema::new(
            "issues",
            [
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("key", ColumnType::Uuid),
            ],
        );
        let people = TableSchema::new(
            "people",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("issue", ColumnType::Uuid),
            ],
        );
        let v2 = SchemaVersion::new(JazzSchema::new([issues, people]));
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                v2.clone(),
                MigrationLens::new(
                    v1.version_id(),
                    v2.id,
                    vec![
                        TableLens {
                            source_table: "issues".to_owned(),
                            target_table: "issues".to_owned(),
                            ops: Vec::new(),
                        },
                        TableLens {
                            source_table: "users".to_owned(),
                            target_table: "people".to_owned(),
                            ops: vec![LensOp::RenameTable {
                                from: "users".to_owned(),
                                to: "people".to_owned(),
                            }],
                        },
                    ],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish users to people lens");
        node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: v2.id,
            },
        })
        .expect("activate v2");

        let branch = BranchId::from_bytes([0xf7; 16]);
        node.create_branch(branch).expect("create branch");
        node.commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("issues", issue, 2).cells(BTreeMap::from([
                ("assignee".to_owned(), Value::Uuid(user.0)),
                ("key".to_owned(), Value::Uuid(issue.0)),
            ])),
        )
        .expect("commit branch issue referencing root user");
        let branch_state = node
            .branches
            .branches
            .get(&branch)
            .cloned()
            .expect("branch");
        let branch_people = node
            .branch_current_rows_for_schema("people", &branch_state, v2.id)
            .expect("project root users into branch people view");
        assert_eq!(branch_people.len(), 1);
        assert_eq!(
            node.historical_content_witness_at(
                "people",
                v2.id,
                user,
                branch_state.base.as_ref().expect("branch base").global_base,
            )
            .expect("recover frozen root witness"),
            Some(user_tx)
        );

        let people_shape = Query::from("people")
            .validate(&v2.schema)
            .expect("validate branch people query");
        let people_binding = people_shape
            .bind(BTreeMap::new())
            .expect("bind branch people query");
        assert_eq!(
            node.query_rows_on_branch_query_engine(
                branch,
                &people_shape,
                &people_binding,
                AuthorId::SYSTEM,
            )
            .expect("query frozen root people through branch engine")
            .len(),
            1
        );
        let issue_shape = Query::from("issues")
            .validate(&v2.schema)
            .expect("validate branch issues query");
        let issue_binding = issue_shape
            .bind(BTreeMap::new())
            .expect("bind branch issues query");
        let issue_rows = node
            .query_rows_on_branch_query_engine(
                branch,
                &issue_shape,
                &issue_binding,
                AuthorId::SYSTEM,
            )
            .expect("query branch issues");
        assert_eq!(issue_rows.len(), 1);
        assert_eq!(
            issue_rows[0].cell(
                &node.table_in_schema("issues", v2.id).expect("issues table"),
                "assignee",
            ),
            Some(Value::Uuid(user.0))
        );
        let root_terminal_ref = RowVersionRefEntry {
            tx: user_tx,
            schema_version: None,
            layer: ResultRowLayer::Content,
            batch: None,
            branch_or_prefix: None,
            row_digest: None,
        };
        let canonical = node
            .resolve_relation_terminal_version("people", user, &root_terminal_ref, v2.id)
            .expect("resolve emitted people literal to exact authored root witness");
        assert_eq!(canonical.table(), "users");
    }

    fn recursive_schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "teamSeeds",
                [
                    ColumnSchema::new("team", ColumnType::Uuid),
                    ColumnSchema::new("kind", ColumnType::String),
                ],
            )
            .with_reference("team", "teams"),
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

    fn missing_session_seed_policy_schema() -> JazzSchema {
        let mut policy = Query::from("resources").reachable_via(
            "resourceAccess",
            "resource",
            "team",
            lit("seeded-by-session"),
            "teamMemberships",
            "member",
            "parent",
            [],
        );
        policy.reachable[0].seed = Some(crate::query::ReachableSeed {
            table: "teamSeeds".to_owned(),
            user_column: Some("user".to_owned()),
            user_claim: Some("session_id".to_owned()),
            team_column: "team".to_owned(),
            filters: Vec::new(),
        });
        JazzSchema::new([
            TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)])
                .with_read_policy(policy),
            TableSchema::new(
                "teamSeeds",
                [
                    ColumnSchema::new("team", ColumnType::Uuid),
                    ColumnSchema::new("user", ColumnType::Uuid),
                ],
            )
            .with_reference("team", "teams"),
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
                "teamMemberships",
                [
                    ColumnSchema::new("member", ColumnType::Uuid),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_reference("member", "teams")
            .with_reference("parent", "teams"),
        ])
    }

    fn row(idx: usize) -> RowUuid {
        let mut bytes = [0_u8; 16];
        bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
        RowUuid::from_bytes(bytes)
    }

    #[test]
    fn missing_policy_relation_seed_claim_fails_closed_without_breaking_prepared_bindings() {
        // This reproduces the server-side shape of a SessionRef policy graph:
        // the outer query is prepared first, then the protected source builds
        // a nested authorization subplan whose reachable seed needs a custom
        // session claim. An absent claim is a denied proof, not malformed
        // stored state or an unavailable source.
        let schema = missing_session_seed_policy_schema();
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xc1; 16]), schema.clone());
        let reader = author(0xc2);
        let team = row(0xc3);
        let resource = row(0xc4);
        commit_global_cells(
            &mut node,
            "resources",
            resource,
            BTreeMap::from([("name".to_owned(), Value::String("secret".to_owned()))]),
            1,
            1,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(0xc5),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ]),
            2,
            2,
        );
        commit_global_cells(
            &mut node,
            "teamSeeds",
            row(0xc6),
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("user".to_owned(), Value::Uuid(reader.0)),
            ]),
            3,
            3,
        );

        let shape = Query::from("resources").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let missing_rows = node
            .query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .expect("an absent policy seed claim must compile to a denied proof")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert!(
            missing_rows.is_empty(),
            "missing custom claim must deny access"
        );

        let ordinary_error = node
            .program_binding_for_shape_and_policy(
                &shape,
                &binding,
                None,
                BTreeMap::new(),
                BTreeMap::from([(
                    claim_param_field(&ClaimPath(vec!["session_id".to_owned()])),
                    ProgramClaimParam {
                        path: ClaimPath(vec!["session_id".to_owned()]),
                        ty: ColumnType::Uuid,
                    },
                )]),
                &node.query_program_policy_context(reader),
            )
            .expect_err("ordinary prepared bindings must still reject missing claims");
        assert!(matches!(ordinary_error, Error::InvalidStoredValue(_)));

        node.set_session_claims(
            reader,
            BTreeMap::from([("session_id".to_owned(), Value::Uuid(reader.0))]),
        );
        let allowed_rows = node
            .query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .expect("bound policy seed claim must compile")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        assert_eq!(allowed_rows, BTreeSet::from([resource]));
    }

    #[test]
    fn missing_policy_seed_claim_denies_authorization_support_rehydration() {
        // Terminal CommitUnit admission rehydrates a compiled read-policy
        // support subscription. This is distinct from the one-shot policy
        // read above: the support shape itself has no user parameter, while
        // its protected source carries the seed claim as a prepared route.
        let schema = missing_session_seed_policy_schema();
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xc7; 16]), schema.clone());
        let writer = author(0xc8);
        let team = row(0xc9);
        let resource = row(0xca);
        commit_global_cells(
            &mut node,
            "resources",
            resource,
            BTreeMap::from([("name".to_owned(), Value::String("secret".to_owned()))]),
            1,
            1,
        );
        commit_global_cells(
            &mut node,
            "resourceAccess",
            row(0xcb),
            BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ]),
            2,
            2,
        );
        commit_global_cells(
            &mut node,
            "teamSeeds",
            row(0xcc),
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("user".to_owned(), Value::Uuid(writer.0)),
            ]),
            3,
            3,
        );

        let scope = node
            .authorization_support_scope(
                writer,
                &PermissionAdviceAction::Read {
                    table: "resources".to_owned(),
                    row: resource,
                },
            )
            .expect("missing policy claim is represented by a denied support shape");
        let options = scope.options.clone();
        let (shape, binding) = scope
            .subscriptions
            .into_iter()
            .next()
            .expect("read policy requires one support subscription");
        let mut peer = PeerState::client_link(writer);
        let ordinary_error = peer
            .rehydrate_query(&mut node, &shape, &binding)
            .expect_err(
                "ordinary prepared subscription hydration must retain missing-claim errors",
            );
        assert!(matches!(ordinary_error, Error::InvalidStoredValue(_)));

        let update = peer
            .rehydrate_authorization_support_query(&mut node, &shape, &binding, options)
            .expect("missing policy seed claim must hydrate as an empty authorization proof");
        let SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } = update
        else {
            panic!("authorization support must return a settled view update");
        };
        assert!(result_member_adds.is_empty());
        assert!(result_member_removes.is_empty());
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

    fn current_titles(
        table: &TableSchema,
        rows: impl IntoIterator<Item = CurrentRow>,
    ) -> BTreeMap<RowUuid, Value> {
        rows.into_iter()
            .map(|row| {
                (
                    row.row_uuid(),
                    row.cell(table, "title")
                        .expect("test row should carry title"),
                )
            })
            .collect()
    }

    fn historical_titles_via_full_scan(
        node: &mut NodeState<RocksDbStorage>,
        table: &TableSchema,
        position: GlobalSeq,
    ) -> BTreeMap<RowUuid, Value> {
        let table_id = node
            .physical_table_id_for_schema(node.catalogue.current_schema_version_id, &table.name)
            .expect("physical table id");
        let history_source = node
            .physical_history_source_graph(node.catalogue.current_schema_version_id, &table.name)
            .expect("physical history source");
        let deltas = node
            .database
            .query_graph(historical_current_graph_full_scan(
                table,
                table_id,
                position,
                history_source,
            ))
            .expect("full-scan historical graph");
        let rows = node
            .materialize_inline_current_query_rows(table, deltas)
            .expect("materialize full-scan historical graph");
        current_titles(table, rows)
    }

    #[test]
    fn historical_cut_bounded_source_matches_full_scan_graph() {
        let schema = JazzSchema::new([TableSchema::new(
            "docs",
            [crate::schema::ColumnSchema::new(
                "title",
                ColumnType::String,
            )],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x31; 16]), schema);
        let table = node.table("docs").expect("docs table").clone();
        let first = row(0x31);
        let second = row(0x32);
        commit_global_cells(
            &mut node,
            "docs",
            first,
            BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
            1_000,
            1,
        );
        commit_global_cells(
            &mut node,
            "docs",
            second,
            BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
            1_001,
            2,
        );
        let delete_tx = node
            .commit_mergeable(
                MergeableCommit::new("docs", first, 1_002).deletion(DeletionEvent::Deleted),
            )
            .expect("commit delete");
        node.apply_fate_update(
            delete_tx,
            Fate::Accepted,
            Some(GlobalSeq(3)),
            Some(DurabilityTier::Global),
        )
        .expect("accept delete");
        // Keep an unrelated later write in the same table to ensure the full-scan
        // control has more history available than the bounded cut should read.
        commit_global_cells(
            &mut node,
            "docs",
            row(0x33),
            BTreeMap::from([("title".to_owned(), Value::String("later".to_owned()))]),
            1_003,
            4,
        );

        node.reset_query_engine_read_metrics();
        let shape = Query::from("docs")
            .validate(&node.catalogue.schema)
            .expect("shape");
        let binding = shape.bind(BTreeMap::new()).expect("binding");
        let bounded = current_titles(
            &table,
            node.query_rows_at(&shape, &binding, GlobalSeq(2))
                .expect("bounded historical query"),
        );
        let selected_metrics = node.query_engine_read_metrics().clone();
        let full = historical_titles_via_full_scan(&mut node, &table, GlobalSeq(2));

        assert_eq!(bounded, full);
        assert_eq!(selected_metrics.source_global_seq_range_scans, 1);
        assert_eq!(selected_metrics.source_full_scans, 0);
    }

    #[test]
    fn historical_cut_reads_only_table_global_seq_range() {
        let schema = JazzSchema::new([TableSchema::new(
            "docs",
            [crate::schema::ColumnSchema::new(
                "title",
                ColumnType::String,
            )],
        )]);
        let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x32; 16]), schema);
        let table = node.table("docs").expect("docs table").clone();
        let shape = Query::from("docs")
            .validate(&node.catalogue.schema)
            .expect("shape");
        let binding = shape.bind(BTreeMap::new()).expect("binding");
        commit_global_cells(
            &mut node,
            "docs",
            row(0x41),
            BTreeMap::from([("title".to_owned(), Value::String("at-cut".to_owned()))]),
            1_000,
            1,
        );
        for idx in 0_u8..40 {
            commit_global_cells(
                &mut node,
                "docs",
                row((0x50 + idx) as usize),
                BTreeMap::from([("title".to_owned(), Value::String(format!("later-{idx}")))]),
                1_010 + idx as u64,
                2 + idx as u64,
            );
        }

        node.reset_query_engine_read_metrics();
        node.reset_storage_read_metrics();
        let rows = current_titles(
            &table,
            node.query_rows_at(&shape, &binding, GlobalSeq(1))
                .expect("bounded historical query"),
        );
        let read_metrics = node.take_storage_read_metrics();
        let selected_metrics = node.query_engine_read_metrics().clone();

        assert_eq!(
            rows,
            BTreeMap::from([(row(0x41), Value::String("at-cut".to_owned()))])
        );
        assert_eq!(selected_metrics.source_global_seq_range_scans, 1);
        assert_eq!(
            read_metrics.global_changes_indexes.ranges, 1,
            "bounded cut should use one by_table_global_seq range"
        );
        assert!(
            read_metrics.global_changes_indexes.reads <= 2,
            "small cut should not read the later same-table history: {:?}",
            read_metrics.global_changes_indexes
        );
        assert!(
            read_metrics.global_changes_rows.reads <= 2,
            "small cut should not fetch later same-table change rows: {:?}",
            read_metrics.global_changes_rows
        );
    }

    #[test]
    fn denormalized_current_content_witness_matches_history_payload_bytes() {
        let (_dir, mut node) = open_node();
        let first = commit_global_cells(
            &mut node,
            "issues",
            row(11),
            BTreeMap::from([
                ("title".to_owned(), Value::String("first".to_owned())),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(1).0)),
                ("priority".to_owned(), Value::U64(1)),
            ]),
            1_000,
            1,
        );
        let second = node
            .commit_mergeable(
                MergeableCommit::new("issues", row(11), 1_100)
                    .made_by(AuthorId::SYSTEM)
                    .parents(vec![first])
                    .cells(BTreeMap::from([
                        ("title".to_owned(), Value::String("second".to_owned())),
                        ("state".to_owned(), Value::String("closed".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(2).0)),
                        ("priority".to_owned(), Value::U64(2)),
                    ])),
            )
            .expect("commit second version");
        node.apply_fate_update(
            second,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .expect("accept second version");

        let table = node.table("issues").expect("issues table").clone();
        let current_source = node
            .physical_current_source_graph(
                node.catalogue.current_schema_version_id,
                "issues",
                PhysicalCurrentClass::Global,
            )
            .expect("physical current source")
            .project(maintained_view_history_storage_field_names(&table));
        let current_deltas = node
            .database
            .query_graph(current_source)
            .expect("query denormalized current payload");
        let current_rows = current_deltas
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(record, _)| record.raw().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(current_rows.len(), 1);

        let history_source = node
            .physical_history_source_graph(node.catalogue.current_schema_version_id, "issues")
            .expect("physical history source");
        let history_deltas = node
            .database
            .query_graph(
                history_source
                    .project(maintained_view_history_storage_field_names(&table))
                    .filter(
                        PredicateExpr::And(vec![
                            PredicateExpr::eq("row_uuid", Value::Uuid(row(11).0)),
                            PredicateExpr::eq("tx_time", Value::U64(second.time.0)),
                        ])
                        .canonicalize(),
                    ),
            )
            .expect("query canonical history payload");
        let history_rows = history_deltas
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(record, _)| record.raw().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(history_rows.len(), 1);
        assert_eq!(
            current_rows[0], history_rows[0],
            "denormalized current witness payload must byte-match canonical history payload"
        );
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

    fn commit_signed_metric(
        node: &mut NodeState<RocksDbStorage>,
        idx: usize,
        bucket: &str,
        score: i64,
    ) {
        node.commit_mergeable_unit(
            MergeableCommit::new("metrics", row(idx), 1_000 + idx as u64)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([
                    ("bucket".to_owned(), Value::String(bucket.to_owned())),
                    ("score".to_owned(), Value::I64(score)),
                ])),
        )
        .expect("commit signed metric");
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

    #[test]
    fn branch_program_maintained_view_provides_branch_deletion_witness_source() {
        // A maintained branch source carries both the overlay content and its
        // deletion witness, so replacement, delete, and restore can remain
        // live without falling back to a one-shot branch read.
        let (_dir, mut node) = open_node();
        let branch_id = BranchId::from_bytes([0x42; 16]);
        node.create_branch(branch_id).unwrap();
        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", row(1), 1_000).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("branch issue".to_owned())),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                ("priority".to_owned(), Value::U64(1)),
            ])),
        )
        .unwrap();

        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let app_rows = node
            .query_rows_on_branch_query_engine(branch_id, &shape, &binding, AuthorId::SYSTEM)
            .unwrap();
        assert_eq!(
            app_rows
                .iter()
                .map(CurrentRow::row_uuid)
                .collect::<Vec<_>>(),
            vec![row(1)]
        );

        node.compile_branch_query_program_in_authorization_mode(
            branch_id,
            &shape,
            &binding,
            AuthorId::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            QueryAuthorizationMode::TrustedServing,
        )
        .expect("maintained branch compilation must provide deletion witnesses");
    }

    /// A branch read renders a frozen root together with an overlay relation
    /// through Groove's structured terminal, while its internal relation fact
    /// retains the mixed canonical provenance needed for future deltas.
    ///
    /// alice --accept issue--> core --freeze branch base--> branch view
    /// branch --write user----► branch view --array correlation--> issue.assigneeRows
    ///
    /// The public array payload is deliberately not assembled from
    /// `RelationSnapshot::edges`: structured app rows are its sole owner. The
    /// separate discriminator assertion pins the internal relation terminal so
    /// a base root and overlay target cannot silently lose their correlation
    /// witness while still returning an empty array.
    #[test]
    fn branch_relation_array_uses_frozen_root_and_overlay_target() {
        let (_dir, mut node) = open_node();
        let issue = row(0x71);
        let overlay_user = author(0x72);
        commit_global_issue(&mut node, 0x71, "open", overlay_user, 1);
        let branch_id = BranchId::from_bytes([0x73; 16]);
        node.create_branch(branch_id).expect("freeze branch base");
        let live_root_update = node
            .commit_mergeable(
                MergeableCommit::new("issues", issue, 2_500)
                    .made_by(AuthorId::SYSTEM)
                    .cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("must not leak past branch base".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("closed".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(overlay_user.0)),
                        ("priority".to_owned(), Value::U64(0x71)),
                    ])),
            )
            .expect("write post-branch global root update");
        node.apply_fate_update(
            live_root_update,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .expect("accept post-branch global root update");
        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("users", RowUuid(overlay_user.0), 2_000).cells(BTreeMap::from([
                ("name".to_owned(), Value::String("overlay user".to_owned())),
            ])),
        )
        .expect("write overlay target");

        let shape = Query::from("issues")
            .filter(eq(col("id"), lit(Value::Uuid(issue.0))))
            .array_subquery(ArraySubquery::new(
                "assigneeRows",
                "users",
                "id",
                "assignee",
            ))
            .validate(&node.catalogue.schema)
            .expect("validate correlated branch query");
        let binding = shape.bind(BTreeMap::new()).expect("bind query");
        let read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        };

        let snapshot = node
            .query_relation_snapshot_for_serving_in_read_view(
                &shape,
                &binding,
                DurabilityTier::Local,
                AuthorId::SYSTEM,
                &read_view,
            )
            .expect("render branch relation snapshot");
        assert_eq!(snapshot.root_count, 1);
        assert_eq!(snapshot.rows.len(), 1);
        let issue_table = node.table("issues").expect("issues table");
        assert_eq!(
            snapshot.rows[0].cell(issue_table, "title"),
            Some(Value::String("issue-113".to_owned())),
            "branch root must remain at the frozen base rather than leak the later global winner"
        );
        assert!(
            snapshot.edges.is_empty(),
            "structured rows own public arrays"
        );
        let (descriptor, raw) = snapshot.rows[0].encoded_record();
        let Value::Array(assignees) = descriptor.bind(raw).get("assigneeRows").unwrap() else {
            panic!("expected structured assignee array")
        };
        assert_eq!(assignees.len(), 1, "one overlay target must correlate");
        let Value::Record(assignee) = &assignees[0] else {
            panic!("expected structured assignee record")
        };
        assert_eq!(assignee.get("row_uuid"), Ok(Value::Uuid(overlay_user.0)));

        assert_eq!(
            node.query_relation_branch_discriminators_for_test(
                &shape,
                &binding,
                DurabilityTier::Local,
                AuthorId::SYSTEM,
                &read_view,
            )
            .expect("relation terminal keeps mixed branch witnesses"),
            vec![(None, Some(branch_id.0))],
            "the frozen issue and overlay user must keep distinct canonical provenance"
        );
    }

    #[test]
    fn branch_program_maintained_view_tracks_local_overlay_replacement() {
        let (_dir, mut node) = open_node();
        let branch_id = BranchId::from_bytes([0x43; 16]);
        node.create_branch(branch_id).unwrap();
        let issue = row(7);
        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", issue, 1_000).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("first title".to_owned())),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                ("priority".to_owned(), Value::U64(1)),
            ])),
        )
        .unwrap();
        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        };
        let (mut local, initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                DurabilityTier::Local,
                &read_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert_eq!(initial.root_count, 1);

        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("second title".to_owned())),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                ("priority".to_owned(), Value::U64(1)),
            ])),
        )
        .unwrap();
        let update = node
            .drain_local_maintained_view_subscription(&mut local, None)
            .unwrap()
            .expect("branch overlay replacement must reach the maintained terminal");
        assert!(
            update.added.iter().any(|(_, row)| row.row_uuid() == issue),
            "replacement must leave a current row in the maintained result"
        );

        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", issue, 3_000).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
        let deletion = node
            .drain_local_maintained_view_subscription(&mut local, None)
            .unwrap()
            .expect("branch deletion must reach the maintained terminal");
        assert!(
            deletion.removed.iter().any(|occurrence| {
                *occurrence
                    == crate::tools::OutputOccurrenceId::single_source(
                        crate::tools::ObjectId::from_uuid(issue.0),
                    )
            }),
            "branch deletion must retract the overlay row"
        );

        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", issue, 4_000).deletion(DeletionEvent::Restored),
        )
        .unwrap();
        let restoration = node
            .drain_local_maintained_view_subscription(&mut local, None)
            .unwrap()
            .expect("branch restoration must reach the maintained terminal");
        assert!(
            restoration
                .added
                .iter()
                .any(|(_, row)| row.row_uuid() == issue),
            "branch restoration must reintroduce the overlay row"
        );
    }

    #[test]
    fn branch_program_maintained_view_survives_first_overlay_partition_write() {
        let (_dir, mut node) = open_node();
        let branch_id = BranchId::from_bytes([0x44; 16]);
        let issue = row(7);
        commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
        node.create_branch(branch_id).unwrap();
        let table_id = node
            .physical_table_id_for_schema(node.catalogue.current_schema_version_id, "issues")
            .unwrap();
        assert!(
            !node
                .branches
                .branch_partitions
                .contains(&(table_id, branch_id)),
            "the durable sparse partition must not exist before the first overlay write"
        );

        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        };
        let (mut subscription, initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                DurabilityTier::Edge,
                &read_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert_eq!(
            initial.root_count, 1,
            "frozen base is available before overlay"
        );
        assert!(
            !node
                .branches
                .branch_partitions
                .contains(&(table_id, branch_id)),
            "opening the maintained view must not publish a branch partition"
        );

        let first_overlay = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("first overlay".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(7)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            first_overlay,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
        assert!(
            node.branches
                .branch_partitions
                .contains(&(table_id, branch_id)),
            "the first accepted overlay write must durably publish its partition"
        );
        let update = node
            .drain_local_maintained_view_subscription(&mut subscription, None)
            .unwrap()
            .expect("first overlay write must keep the pre-existing subscription live");
        assert!(
            update.added.iter().any(|(_, row)| {
                row.row_uuid() == issue
                    && row.cell(node.table("issues").unwrap(), "title")
                        == Some(Value::String("first overlay".to_owned()))
            }),
            "the first accepted overlay write must produce its exact replacement delta"
        );
    }

    #[test]
    fn branch_program_maintained_views_isolate_sibling_first_writes() {
        let (_dir, mut node) = open_node();
        let first_branch = BranchId::from_bytes([0x45; 16]);
        let sibling_branch = BranchId::from_bytes([0x46; 16]);
        let issue = row(7);
        commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
        node.create_branch(first_branch).unwrap();
        node.create_branch(sibling_branch).unwrap();

        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let first_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: first_branch.0,
            },
            ..ReadViewSpec::default()
        };
        let sibling_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: sibling_branch.0,
            },
            ..ReadViewSpec::default()
        };
        let (mut first_subscription, first_initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                DurabilityTier::Edge,
                &first_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        let (mut sibling_subscription, sibling_initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                DurabilityTier::Edge,
                &sibling_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert_eq!(first_initial.root_count, 1);
        assert_eq!(sibling_initial.root_count, 1);

        let first_write = node
            .commit_mergeable_on_branch(
                first_branch,
                MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("first branch".to_owned())),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(7)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            first_write,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
        let first_update = node
            .drain_local_maintained_view_subscription(&mut first_subscription, None)
            .unwrap()
            .expect("first branch must receive its own accepted overlay update");
        assert!(
            first_update
                .added
                .iter()
                .any(|(_, row)| row.row_uuid() == issue)
        );
        assert!(
            node.drain_local_maintained_view_subscription(&mut sibling_subscription, None)
                .unwrap()
                .is_none(),
            "a sibling branch subscription must not receive first branch deltas"
        );
    }

    #[test]
    fn branch_program_maintained_view_settles_overlay_fates_at_every_tier() {
        for (tier, acceptance) in [
            (DurabilityTier::Local, (None, DurabilityTier::Edge)),
            (DurabilityTier::Edge, (None, DurabilityTier::Edge)),
            (
                DurabilityTier::Global,
                (Some(GlobalSeq(4)), DurabilityTier::Global),
            ),
        ] {
            let (_dir, mut node) = open_node();
            let branch_id = BranchId::from_bytes([tier as u8 + 0x50; 16]);
            let issue = row(7);
            let frozen_only_issue = row(8);
            commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
            commit_global_issue(&mut node, 8, "open", author(0xa1), 2);
            node.create_branch(branch_id).unwrap();
            let initial_overlay = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", issue, 2_500).cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("initial overlay".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                        ("priority".to_owned(), Value::U64(7)),
                    ])),
                )
                .unwrap();
            node.apply_fate_update(
                initial_overlay,
                Fate::Accepted,
                Some(GlobalSeq(3)),
                Some(DurabilityTier::Global),
            )
            .unwrap();

            let shape = Query::from("issues")
                .validate(&node.catalogue.schema)
                .unwrap();
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let read_view = ReadViewSpec {
                source: ReadViewSourceSpec::Branch {
                    branch: branch_id.0,
                },
                ..ReadViewSpec::default()
            };
            let (mut subscription, initial) = node
                .open_maintained_view_subscription_in_authorization_mode(
                    &shape,
                    &binding,
                    AuthorId::SYSTEM,
                    tier,
                    &read_view,
                    None,
                    QueryAuthorizationMode::TrustedServing,
                )
                .unwrap();
            assert_eq!(
                initial.root_count, 2,
                "{tier:?} subscription must include the frozen base"
            );

            let replacement = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", issue, 3_000).cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("overlay title".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                        ("priority".to_owned(), Value::U64(7)),
                    ])),
                )
                .unwrap();
            if tier == DurabilityTier::Local {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_some(),
                    "Local subscriptions must see pending branch writes"
                );
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "{tier:?} subscriptions must not expose pending branch writes"
                );
            }
            node.apply_fate_update(
                replacement,
                Fate::Accepted,
                acceptance.0,
                Some(acceptance.1),
            )
            .unwrap();
            if tier >= DurabilityTier::Edge {
                let update = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("accepted branch replacement must reach the requested tier");
                assert!(update.added.iter().any(|(_, row)| row.row_uuid() == issue));
            }

            let deletion = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", frozen_only_issue, 4_000)
                        .deletion(DeletionEvent::Deleted),
                )
                .unwrap();
            let deletion_acceptance = match tier {
                DurabilityTier::Global => (Some(GlobalSeq(5)), DurabilityTier::Global),
                _ => (None, DurabilityTier::Edge),
            };
            if tier == DurabilityTier::Local {
                let pending_deletion = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("Local branch deletion must publish while pending");
                assert!(pending_deletion.removed.iter().any(|occurrence| {
                    *occurrence
                        == crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(frozen_only_issue.0),
                        )
                }));
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "{tier:?} subscriptions must not expose pending branch deletion"
                );
            }
            node.apply_fate_update(
                deletion,
                Fate::Accepted,
                deletion_acceptance.0,
                Some(deletion_acceptance.1),
            )
            .unwrap();
            if tier >= DurabilityTier::Edge {
                let deletion_update = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("accepted branch deletion must reach the requested tier");
                assert!(
                    deletion_update.removed.iter().any(|occurrence| {
                        *occurrence
                            == crate::tools::OutputOccurrenceId::single_source(
                                crate::tools::ObjectId::from_uuid(frozen_only_issue.0),
                            )
                    }),
                    "{tier:?} branch deletion must mask frozen-base membership"
                );
            }

            let restoration = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", frozen_only_issue, 5_000)
                        .deletion(DeletionEvent::Restored),
                )
                .unwrap();
            let restoration_acceptance = match tier {
                DurabilityTier::Global => (Some(GlobalSeq(6)), DurabilityTier::Global),
                _ => (None, DurabilityTier::Edge),
            };
            if tier == DurabilityTier::Local {
                let pending_restoration = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("Local branch restore must publish while pending");
                assert!(
                    pending_restoration
                        .added
                        .iter()
                        .any(|(_, row)| row.row_uuid() == frozen_only_issue)
                );
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "{tier:?} subscriptions must not expose pending branch restore"
                );
            }
            node.apply_fate_update(
                restoration,
                Fate::Accepted,
                restoration_acceptance.0,
                Some(restoration_acceptance.1),
            )
            .unwrap();
            if tier >= DurabilityTier::Edge {
                let restoration_update = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("accepted branch restore must reach the requested tier");
                assert!(
                    restoration_update
                        .added
                        .iter()
                        .any(|(_, row)| row.row_uuid() == frozen_only_issue),
                    "{tier:?} branch restore must re-expose the frozen base"
                );
            }
        }
    }

    #[test]
    fn branch_program_maintained_view_retracts_rejected_pending_overlay_versions() {
        for tier in [
            DurabilityTier::Local,
            DurabilityTier::Edge,
            DurabilityTier::Global,
        ] {
            let (_dir, mut node) = open_node();
            let branch_id = BranchId::from_bytes([tier as u8 + 0x60; 16]);
            let issue = row(7);
            let rejected_only = row(9);
            commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
            node.create_branch(branch_id).unwrap();
            let accepted = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", issue, 2_500).cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("accepted overlay".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                        ("priority".to_owned(), Value::U64(7)),
                    ])),
                )
                .unwrap();
            node.apply_fate_update(
                accepted,
                Fate::Accepted,
                Some(GlobalSeq(3)),
                Some(DurabilityTier::Global),
            )
            .unwrap();

            let shape = Query::from("issues")
                .validate(&node.catalogue.schema)
                .unwrap();
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let read_view = ReadViewSpec {
                source: ReadViewSourceSpec::Branch {
                    branch: branch_id.0,
                },
                ..ReadViewSpec::default()
            };
            let (mut subscription, initial) = node
                .open_maintained_view_subscription_in_authorization_mode(
                    &shape,
                    &binding,
                    AuthorId::SYSTEM,
                    tier,
                    &read_view,
                    None,
                    QueryAuthorizationMode::TrustedServing,
                )
                .unwrap();
            assert!(initial.rows.iter().any(|current| {
                current.row_uuid() == issue
                    && current.cell(node.table("issues").unwrap(), "title")
                        == Some(Value::String("accepted overlay".to_owned()))
            }));

            let rejected_replacement = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", issue, 3_000).cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("rejected replacement".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                        ("priority".to_owned(), Value::U64(8)),
                    ])),
                )
                .unwrap();
            if tier == DurabilityTier::Local {
                let pending = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("Local must expose a pending replacement");
                assert!(pending.added.iter().any(|(_, current)| {
                    current.row_uuid() == issue
                        && current.cell(node.table("issues").unwrap(), "title")
                            == Some(Value::String("rejected replacement".to_owned()))
                }));
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "{tier:?} must not expose a pending replacement"
                );
            }
            node.apply_fate_update(
                rejected_replacement,
                Fate::Rejected(crate::tx::RejectionReason::AuthorizationDenied),
                None,
                None,
            )
            .unwrap();
            if tier == DurabilityTier::Local {
                let retracted = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("rejecting a pending replacement must restore the accepted winner");
                assert!(retracted.added.iter().any(|(_, current)| {
                    current.row_uuid() == issue
                        && current.cell(node.table("issues").unwrap(), "title")
                            == Some(Value::String("accepted overlay".to_owned()))
                }));
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "a rejected replacement must never perturb {tier:?}"
                );
            }

            let rejected_insert = node
                .commit_mergeable_on_branch(
                    branch_id,
                    MergeableCommit::new("issues", rejected_only, 4_000).cells(BTreeMap::from([
                        (
                            "title".to_owned(),
                            Value::String("rejected insert".to_owned()),
                        ),
                        ("state".to_owned(), Value::String("open".to_owned())),
                        ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                        ("priority".to_owned(), Value::U64(9)),
                    ])),
                )
                .unwrap();
            if tier == DurabilityTier::Local {
                let pending = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("Local must expose a pending insert");
                assert!(
                    pending
                        .added
                        .iter()
                        .any(|(_, current)| current.row_uuid() == rejected_only)
                );
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "{tier:?} must not expose a pending insert"
                );
            }
            node.apply_fate_update(
                rejected_insert,
                Fate::Rejected(crate::tx::RejectionReason::AuthorizationDenied),
                None,
                None,
            )
            .unwrap();
            if tier == DurabilityTier::Local {
                let retracted = node
                    .drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .expect("rejecting a pending insert must retract it");
                assert!(retracted.removed.iter().any(|occurrence| {
                    *occurrence
                        == crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(rejected_only.0),
                        )
                }));
            } else {
                assert!(
                    node.drain_local_maintained_view_subscription(&mut subscription, None)
                        .unwrap()
                        .is_none(),
                    "a rejected insert must never perturb {tier:?}"
                );
            }
        }
    }

    #[test]
    fn branch_program_tier_filter_preserves_claim_policy_fields() {
        let schema = JazzSchema::new([TableSchema::new(
            "rooms",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("join_code", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("rooms").filter(eq(col("join_code"), claim("join_code"))),
        ))
        .with_write_policy(Policy::public())]);
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0x71; 16]), schema.clone());
        let identity = author(0x72);
        node.set_session_claims(
            identity,
            BTreeMap::from([(
                "join_code".to_owned(),
                Value::String("branch-secret".to_owned()),
            )]),
        );
        let branch_id = BranchId::from_bytes([0x73; 16]);
        node.create_branch(branch_id).unwrap();
        let room = row(7);
        let tx_id = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("rooms", room, 1_000).cells(BTreeMap::from([
                    ("name".to_owned(), Value::String("branch room".to_owned())),
                    (
                        "join_code".to_owned(),
                        Value::String("branch-secret".to_owned()),
                    ),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let shape = Query::from("rooms").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows_on_branch_query_engine(branch_id, &shape, &binding, identity)
            .unwrap();
        assert_eq!(
            rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
            vec![room]
        );
        let (_, snapshot) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                identity,
                DurabilityTier::Global,
                &ReadViewSpec {
                    source: ReadViewSourceSpec::Branch {
                        branch: branch_id.0,
                    },
                    ..ReadViewSpec::default()
                },
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert_eq!(
            snapshot
                .rows
                .iter()
                .map(CurrentRow::row_uuid)
                .collect::<Vec<_>>(),
            vec![room]
        );
        let (_, local_snapshot) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                identity,
                DurabilityTier::Global,
                &ReadViewSpec {
                    source: ReadViewSourceSpec::Branch {
                        branch: branch_id.0,
                    },
                    ..ReadViewSpec::default()
                },
                None,
                QueryAuthorizationMode::ClientLocal,
            )
            .unwrap();
        assert_eq!(
            local_snapshot
                .rows
                .iter()
                .map(CurrentRow::row_uuid)
                .collect::<Vec<_>>(),
            vec![room]
        );
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
                result_member_adds,
                ..
            } if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
                && result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).all(|(_, row_uuid, _)| row_uuid != resource2)
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
                result_member_adds,
                result_member_removes,
                ..
            } if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
                && result_member_removes.is_empty()
        ));

        delete_global(&mut core, "teamTeamMemberships", row(302), 18, 8);
        let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert!(matches!(
            revoke,
            SyncMessage::ViewUpdate {
                result_member_adds,
                result_member_removes,
                ..
            } if result_member_adds.is_empty()
                && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
                && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
        ));
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
                .keys()
                .any(|(shape_id, tier, _)| {
                    *shape_id == shape.shape_id() && *tier == DurabilityTier::Global
                })
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
                .iter()
                .find(|((shape_id, tier, _), _)| {
                    *shape_id == shape.shape_id() && *tier == DurabilityTier::Global
                })
                .map(|(_, plan)| plan.as_ref()),
            Some(PreparedQueryPlan::Prepared { .. })
        ));
    }

    #[test]
    fn reachable_relation_seed_query_rows_lowers_through_query_engine() {
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
        commit_global_cells(
            &mut node,
            "teamSeeds",
            row(401),
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team1.0)),
                ("kind".to_owned(), Value::String("sync".to_owned())),
            ]),
            14,
            5,
        );
        commit_global_cells(
            &mut node,
            "teamSeeds",
            row(402),
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team4.0)),
                ("kind".to_owned(), Value::String("other".to_owned())),
            ]),
            15,
            6,
        );
        for (idx, member, parent, seq) in [(301, team1, team2, 7), (302, team2, team3, 8)] {
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

        let mut query = Query::from("resources").reachable_via(
            "resourceAccess",
            "resource",
            "team",
            lit("ignored-by-relation-seed"),
            "teamTeamMemberships",
            "member",
            "parent",
            [eq(col("onlyAdmins"), lit(false))],
        );
        query.reachable[0].seed = Some(crate::query::ReachableSeed {
            table: "teamSeeds".to_owned(),
            user_column: None,
            user_claim: None,
            team_column: "team".to_owned(),
            filters: vec![gt(col("kind"), param("seed_kind_lower_bound"))],
        });
        let shape = query.validate(&schema).unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "seed_kind_lower_bound".to_owned(),
                Value::String("s".to_owned()),
            )]))
            .unwrap();

        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(rows, BTreeSet::from([resource1]));
    }

    #[test]
    fn reachable_relation_seed_hydrates_from_primary_key_scan() {
        let (_dir, mut node) = open_recursive_node();
        let schema = recursive_schema();
        let team1 = row(1);
        let team2 = row(2);
        let team3 = row(3);
        let team4 = row(4);
        let resource1 = row(101);
        let resource2 = row(102);
        let seed = row(401);
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
        for idx in 0..128 {
            commit_global_cells(
                &mut node,
                "teamSeeds",
                row(500 + idx),
                BTreeMap::from([
                    ("team".to_owned(), Value::Uuid(team4.0)),
                    ("kind".to_owned(), Value::String(format!("noise-{idx}"))),
                ]),
                1_000 + idx as u64,
                20 + idx as u64,
            );
        }
        commit_global_cells(
            &mut node,
            "teamSeeds",
            seed,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team1.0)),
                ("kind".to_owned(), Value::String("sync".to_owned())),
            ]),
            14,
            5,
        );
        for (idx, member, parent, seq) in [(301, team1, team2, 7), (302, team2, team3, 8)] {
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

        let mut query = Query::from("resources").reachable_via(
            "resourceAccess",
            "resource",
            "team",
            lit("ignored-by-relation-seed"),
            "teamTeamMemberships",
            "member",
            "parent",
            [eq(col("onlyAdmins"), lit(false))],
        );
        query.reachable[0].seed = Some(crate::query::ReachableSeed {
            table: "teamSeeds".to_owned(),
            user_column: None,
            user_claim: None,
            team_column: "team".to_owned(),
            filters: vec![eq(col("id"), lit(Value::Uuid(seed.0)))],
        });
        let shape = query.validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();

        node.reset_query_engine_read_metrics();
        let selected = node
            .query_rows_for_link(&shape, &binding, DurabilityTier::Global, AuthorId::SYSTEM)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        let selected_metrics = node.query_engine_read_metrics().clone();
        node.reset_query_engine_read_metrics();
        let forced = node
            .query_rows_for_link_forced_full_scan_for_test(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorId::SYSTEM,
            )
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();
        let forced_metrics = node.query_engine_read_metrics().clone();

        assert_eq!(selected, forced);
        assert_eq!(selected, BTreeSet::from([resource1]));
        assert_eq!(selected_metrics.source_primary_key_scans, 1);
        assert!(
            forced_metrics.source_full_scans > selected_metrics.source_full_scans,
            "forced full scan must scan the seed source instead of using its point lookup"
        );
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
    fn prepared_policy_plan_is_recompiled_after_same_identity_claim_revision_changes() {
        let schema = JazzSchema::new([TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("state", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("priority", ColumnType::U64),
            ],
        )
        .with_read_policy(
            Query::from("issues").filter(eq(col("assignee"), claim("selected_assignee"))),
        )]);
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0x81; 16]), schema.clone());
        let alice = author(0x82);
        let bob = author(0x83);
        commit_issue(&mut node, 1, "open", alice);
        commit_issue(&mut node, 2, "open", bob);

        let identity = author(0x84);
        let shape = Query::from("issues").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let visible_for = |node: &mut NodeState<RocksDbStorage>| {
            node.query_rows_for_link(&shape, &binding, DurabilityTier::Local, identity)
                .unwrap()
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>()
        };

        node.set_session_claims(
            identity,
            BTreeMap::from([("selected_assignee".to_owned(), Value::Uuid(alice.0))]),
        );
        assert_eq!(visible_for(&mut node), BTreeSet::from([row(1)]));

        node.set_session_claims(
            identity,
            BTreeMap::from([("selected_assignee".to_owned(), Value::Uuid(bob.0))]),
        );
        assert_eq!(
            visible_for(&mut node),
            BTreeSet::from([row(2)]),
            "the same prepared shape and identity must not reuse the plan compiled for prior claims",
        );
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
    fn aggregate_query_normalizes_to_query_engine_aggregate_node() {
        let (_dir, node) = open_node();
        let shape = Query::from("issues")
            .filter(eq(col("state"), lit("open")))
            .count()
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
        assert!(matches!(
            normalized.nodes.get(&normalized.root),
            Some(RowSetExpr::Aggregate { .. })
        ));
    }

    #[test]
    fn production_policy_union_labels_survive_reorder_and_unrelated_insertion() {
        fn branch(state: &str) -> crate::query::PolicyBranch {
            crate::query::PolicyBranch {
                filters: vec![eq(col("state"), lit(state))],
                joins: Vec::new(),
                reachable: Vec::new(),
                inherits: Vec::new(),
            }
        }
        fn labels(node: &NodeState<RocksDbStorage>, branches: &[&str]) -> BTreeSet<String> {
            let mut query = Query::from("issues");
            query.policy_branches = branches.iter().map(|state| branch(state)).collect();
            let shape = query.validate(&schema()).unwrap();
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
            normalized
                .nodes
                .values()
                .find_map(|node| match node {
                    RowSetExpr::Union { inputs } => Some(
                        inputs
                            .iter()
                            .map(|input| input.label.clone())
                            .collect::<BTreeSet<_>>(),
                    ),
                    _ => None,
                })
                .expect("policy alternatives normalize through Union")
        }

        let (_dir, node) = open_node();
        let original = labels(&node, &["open", "done"]);
        let reordered_with_insert = labels(&node, &["done", "blocked", "open"]);
        assert!(original.is_subset(&reordered_with_insert));
        assert_eq!(reordered_with_insert.len(), original.len() + 1);
        assert_ne!(labels(&node, &["open"]), labels(&node, &["changed"]));
    }

    #[test]
    fn join_via_nested_joins_normalize_as_parent_projection_gate() {
        let (_dir, node) = open_node();
        let nested = Query::from("issue_members")
            .join_via_row_id("users", "user", [eq(col("name"), lit("Alice"))])
            .joins
            .into_iter()
            .next()
            .unwrap();
        let shape = Query::from("issues")
            .join_via_with_nested_joins("issue_members", "issue", [], [nested])
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();

        assert_eq!(normalized.join_contributions.len(), 1);
        let contribution = &normalized.join_contributions[0];
        assert_eq!(contribution.input.0, "join_via:0:nested:0:parent_project");
        assert!(matches!(
            normalized.nodes.get(&contribution.input),
            Some(RowSetExpr::Project { input, columns })
                if input.0 == "join_via:0:nested:0:join"
                    && columns.iter().any(|column| column.output.name == "id")
                    && columns.iter().any(|column| column.output.name == "issue")
                    && columns.iter().any(|column| column.output.name == "user")
        ));
        assert!(matches!(
            normalized.nodes.get(&RowSetNodeId("join_via:0:nested:0:join".to_owned())),
            Some(RowSetExpr::Join { left, right, .. })
                if left.0 == "join_via:0:source"
                    && right.0 == "join_via:0:nested:0:filter"
        ));
        assert!(matches!(
            normalized.nodes.get(&normalized.root),
            Some(RowSetExpr::Join { right, .. }) if right == &contribution.input
        ));
    }

    #[test]
    fn join_via_source_lookup_normalizes_as_lookup_bridge_projection() {
        let (_dir, node) = open_node();
        let shape = Query::from("issues")
            .join_via_source_lookup(
                "issue_members",
                "user",
                JoinSourceLookup {
                    table: "users".to_owned(),
                    row_id_source_column: "assignee".to_owned(),
                    value_column: "id".to_owned(),
                },
                [],
            )
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();

        assert_eq!(normalized.join_contributions.len(), 1);
        let contribution = &normalized.join_contributions[0];
        assert_eq!(contribution.input.0, "join_via:0:lookup_project");
        assert!(matches!(
            normalized.nodes.get(&contribution.input),
            Some(RowSetExpr::Project { input, columns })
                if input.0 == "join_via:0:lookup_join"
                    && columns.iter().any(|column| column.output.name == "id")
                    && columns.iter().any(|column| column.output.name == "issue")
                    && columns.iter().any(|column| column.output.name == "user")
                    && columns.iter().any(|column| column.output.name == "assignee")
        ));
        assert!(matches!(
            normalized.nodes.get(&normalized.root),
            Some(RowSetExpr::Join { right, on, .. })
                if right == &contribution.input
                    && matches!(
                        on,
                        NormalizedPredicateExpr::Compare { left, right, .. }
                            if matches!(
                                left,
                                NormalizedValueRef::SourceField { field, .. } if field == "assignee"
                            ) && matches!(
                                right,
                                NormalizedValueRef::SourceField { field, .. } if field == "assignee"
                            )
                    )
        ));
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
    fn aggregate_sum_avg_min_max_support_signed_i64_inputs() {
        let schema = signed_metric_schema();
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xb5; 16]), schema.clone());
        commit_signed_metric(&mut node, 0x10, "a", -3);
        commit_signed_metric(&mut node, 0x11, "a", 2);
        let shape = Query::from("metrics")
            .aggregate([
                Aggregate::sum("score"),
                Aggregate::avg("score"),
                Aggregate::min("score"),
                Aggregate::max("score"),
            ])
            .validate(&schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();

        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        let cells = rows[0].test_cells_by_descriptor();
        assert_eq!(cells["sum_score"], Value::I64(-1));
        assert_eq!(cells["avg_score"], Value::F64(-0.5));
        assert_eq!(cells["min_score"], Value::I64(-3));
        assert_eq!(cells["max_score"], Value::I64(2));
    }

    #[test]
    fn aggregate_explicit_user_prefix_alias_remains_a_logical_name() {
        let schema = signed_metric_schema();
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0xb6; 16]), schema.clone());
        commit_signed_metric(&mut node, 0x12, "a", -3);
        commit_signed_metric(&mut node, 0x13, "a", 2);
        let shape = Query::from("metrics")
            .aggregate([Aggregate::sum("score").alias("user_total")])
            .validate(&schema)
            .expect("explicit user-prefix aggregate alias is valid");
        let rows = node
            .query_rows(
                &shape,
                &shape.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].test_cells_by_descriptor()["user_total"],
            Value::I64(-1),
        );
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
    fn query_join_via_nested_joins_filters_visible_roots() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        commit_global_user(&mut node, alice, "Alice", 1);
        commit_global_user(&mut node, bob, "Bob", 2);
        for idx in 0..4 {
            commit_issue(&mut node, idx, "open", bob);
        }
        commit_member(&mut node, 0, row(0), alice);
        commit_member(&mut node, 1, row(1), bob);
        commit_member(&mut node, 2, row(2), alice);

        let nested = Query::from("issue_members")
            .join_via_row_id("users", "user", [eq(col("name"), lit("Alice"))])
            .joins
            .into_iter()
            .next()
            .unwrap();
        let shape = Query::from("issues")
            .join_via_with_nested_joins("issue_members", "issue", [], [nested])
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(rows, BTreeSet::from([row(0), row(2)]));
    }

    #[test]
    fn query_join_via_source_lookup_filters_visible_roots() {
        let (_dir, mut node) = open_node();
        let alice = author(1);
        let bob = author(2);
        commit_global_user(&mut node, alice, "Alice", 1);
        commit_global_user(&mut node, bob, "Bob", 2);
        commit_issue(&mut node, 0, "open", alice);
        commit_issue(&mut node, 1, "open", bob);
        commit_issue(&mut node, 2, "open", alice);
        commit_member(&mut node, 0, row(100), alice);
        commit_member(&mut node, 1, row(101), bob);

        let shape = Query::from("issues")
            .join_via_source_lookup(
                "issue_members",
                "user",
                JoinSourceLookup {
                    table: "users".to_owned(),
                    row_id_source_column: "assignee".to_owned(),
                    value_column: "id".to_owned(),
                },
                [eq(col("issue"), lit(Value::Uuid(row(100).0)))],
            )
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>();

        assert_eq!(rows, BTreeSet::from([row(0), row(2)]));
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

        let open = OpenBatchId::new();
        client.open_exclusive(open).unwrap();
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
        let tx = OpenBatchId::new();
        node.open_exclusive(tx).unwrap();
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
        let tx = OpenBatchId::new();
        node.open_exclusive(tx).unwrap();
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

        register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut server, &shape, &alice_binding);
        subscribe_query_binding(&mut server, &shape, &bob_binding);
        register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut reader, &shape, &alice_binding);
        subscribe_query_binding(&mut reader, &shape, &bob_binding);

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
                    read_view: Default::default(),
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
                result_member_adds,
                result_member_removes,
                ..
            } if result_member_adds.is_empty() && result_member_removes.is_empty()
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
    fn settled_binding_view_sources_provide_source_coverage_metadata() {
        let (_server_dir, mut server) = open_node();
        let (_reader_dir, mut reader) = open_node();
        let alice = author(1);
        let shape = Query::from("users")
            .filter(eq(col("name"), param("name")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "name".to_owned(),
                Value::String("alice".to_owned()),
            )]))
            .unwrap();

        register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut server, &shape, &binding);
        register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut reader, &shape, &binding);

        commit_global_user(&mut server, alice, "alice", 1);
        let mut peer = PeerState::new();
        let initial = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
        reader.apply_sync_message(initial).unwrap();

        let settled_binding_view = reader
            .settled_binding_view_key_for_query(&shape, &binding)
            .unwrap()
            .expect("receiver should have a settled binding view after rehydrate");
        let mut request = reader
            .current_query_program_request(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorId::SYSTEM,
                CurrentQueryProgramOutput::AppRows,
                &ReadViewSpec::default(),
                Some(settled_binding_view),
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        request
            .output
            .facts
            .insert(ProgramFactKey::SourceCoverage(CoverageScope::Program));

        let program = reader
            .compile_query_program_request(request)
            .expect("settled binding-view source should lower source coverage facts");
        assert!(
            matches!(
                &program.lowered.output,
                ProgramOutputSchemas::RowSet(terminals)
                    if terminals.iter().any(|terminal| matches!(
                        terminal,
                        OutputTerminalSchema::Fact(ProgramFactOutput {
                            key: ProgramFactKey::SourceCoverage(CoverageScope::Program),
                            ..
                        })
                    ))
            ),
            "compiled program should include a source coverage terminal"
        );
    }

    #[test]
    fn settled_binding_view_root_with_reference_include_sources_lowers() {
        // A settled binding view contains root result membership only. Shapes
        // with implicit reference closures need auxiliary source coverage too,
        // so the mixed settled-root/current-auxiliary read set must still be
        // able to lower coverage facts.
        let (_server_dir, mut server) = open_node();
        let (_reader_dir, mut reader) = open_node();
        let alice = author(1);
        let shape = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .include("assignee")
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(alice.0))]))
            .unwrap();

        register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut server, &shape, &binding);
        register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(&mut reader, &shape, &binding);

        commit_global_cells(
            &mut server,
            "users",
            RowUuid(alice.0),
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
            1,
            1,
        );
        commit_global_issue(&mut server, 0, "open", alice, 2);
        let mut peer = PeerState::new();
        let initial = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
        reader.apply_sync_message(initial).unwrap();

        let settled_binding_view = reader
            .settled_binding_view_key_for_query(&shape, &binding)
            .unwrap()
            .expect("receiver should have a settled binding view after rehydrate");
        reader.catalogue.current_schema_version_alias = None;
        let request = reader
            .current_query_program_request(
                &shape,
                &binding,
                DurabilityTier::Global,
                alice,
                CurrentQueryProgramOutput::MaintainedView,
                &ReadViewSpec::default(),
                Some(settled_binding_view),
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        let mut request = request;
        request
            .output
            .facts
            .insert(ProgramFactKey::SourceCoverage(CoverageScope::Program));

        let sources = format!("{:?}", request.reads);
        assert!(sources.contains("SettledBindingView"), "{sources}");
        assert!(sources.contains("VisibleCurrent"), "{sources}");
        reader
            .compile_query_program_request(request)
            .expect("settled binding-view root with current include sources should lower");
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
        register_shape_binding_for_receiver(&mut reader, &shape, &binding);
        let mut peer = PeerState::new();
        let update = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_member_adds, ..
        } = &update
        else {
            panic!("expected view update");
        };
        let result_set_tables = result_member_adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(table, _, _)| table.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            result_set_tables,
            BTreeSet::from([
                "issues".to_owned(),
                "issue_members".to_owned(),
                "users".to_owned(),
            ])
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
