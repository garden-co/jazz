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

use groove::ivm::{
    InputSourceId, InputSourceReplacement, LiteralValue, PreparedShapeId, RoutedMultisinkTerminal,
    StaticScanSpec,
};
use groove::ivm::{MultisinkDeltas, MultisinkSubscription, RecordDeltas};
use groove::records::{BorrowedRecord, OwnedRecord, RecordDescriptor, ValueType};
use groove::schema::ColumnType;

use super::maintained_subscription_view::{MaintainedSubscriptionView, MaintainedTerminalSchemas};
#[cfg(feature = "testing")]
use super::maintained_subscription_view::{
    MaintainedSubscriptionViewFootprint, MaintainedTerminalSchemasFootprint,
};
use super::query_engine::BranchViewSourceBase;
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
    RowSetProgramInput, RowVisibility, SchemaFamilySelection, SchemaProjection,
    SortDirection as NormalizedSortDirection, SourceAuthorizationRequest, SourceExpr, SourceGap,
    SourceGraphPreparer, SourceId, SourceMetadataFields, SourceMetadataRequirement, SourcePath,
    SourceRequest, SourceRequirements, SourceResolutionError, SourceRole, SourceRowShape,
    StorageSchemaSelection, TypedOutputField, UnionInput, ValueSourceColumn, ValueSourceMode,
    VersionIdentityFields, VersionedRowRefSchema, aggregate_output_app_field,
    aggregate_output_column, aggregate_output_field, claim_param_field,
    claim_path_from_param_field, left_field, prepare_and_lower_query_program,
    query_program_source_requests, right_field, route_param_field, user_column_field,
};
#[cfg(test)]
use crate::protocol::ReadViewKey;
use crate::protocol::{
    AuthorizationOperationKey, AuthorizationScopeOperation, AuthorizationSupportScopeKey,
    BindingViewKey, KnownStateCompleteness, KnownStateDeclaration, PermissionAdviceAction,
    ProgramFactEntry, ProgramSourceId, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    RelationEdgeEntry, ResultMemberEntry, ResultMemberPayloadEntry, ResultRowLayer, RowVersionRef,
    RowVersionRefEntry, ShapeAst, ShapeBody, Subscribe, SubscriptionKey, SyntheticReplacementToken,
};
use crate::protocol_limits::MAX_KNOWN_STATE_EXACT_REFS;
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, Predicate, Query as JazzQuery,
    QueryError, ShapeId, ValidatedQuery, binding_id_for_values, relation_query_to_query,
};
use crate::schema::{ColumnSchema, RuntimeSchema};
use crate::tools::{ObjectId, OutputOccurrenceId};

mod materialization;
mod prepared_bindings;
mod query_read_sets;
mod query_result_rows;

pub(crate) use prepared_bindings::coerce_prepared_binding_value;
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

#[cfg(test)]
thread_local! {
    static CLIENT_PHYSICAL_ROW_QUERY_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_client_physical_row_query_calls_for_test() -> usize {
    CLIENT_PHYSICAL_ROW_QUERY_CALLS.with(|calls| calls.replace(0))
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

/// A receiver-owned authority input page and the slice to apply relative to
/// that page. A non-durable client can retain only a bounded source window,
/// so a compatible Local query must never apply its absolute offset again.
#[derive(Clone, Debug)]
struct ClientSettledBindingView {
    key: BindingViewKey,
    retained_window: Option<RetainedRootWindowSource>,
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

enum CurrentQueryProgramOutput {
    AppRows,
    PolicyPredicate,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

mod read_sources;

use read_sources::*;

mod normalization;

use normalization::*;

mod subscriptions;

mod maintained_views;

mod local_authority_reconciliation;
pub(crate) use local_authority_reconciliation::LocalAuthorityReconciliation;

#[cfg(feature = "testing")]
pub(crate) use maintained_views::LocalMaintainedViewSubscriptionFootprint;
use maintained_views::SubscriptionPreparedPlan;
pub(crate) use maintained_views::{
    CoveredInputReceiver, LocalMaintainedViewSubscription, LocalMaintainedViewSubscriptionUpdate,
};

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn resolve_time_travel_position(
        &mut self,
        time: TxTime,
    ) -> Result<GlobalTime, Error> {
        let raws = if time.0 == u64::MAX {
            self.database
                .primary_key_scan_raw("jazz_transactions", &[])
                .await?
        } else {
            self.database
                .primary_key_scan_range_raw(
                    "jazz_transactions",
                    &[Value::U64(0), Value::U64(0)],
                    &[Value::U64(time.0 + 1), Value::U64(0)],
                )
                .await?
        };
        let mut position = GlobalTime(0);
        for raw in raws {
            let record = raw.record();
            let Some(global_time) = record
                .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
                .map(GlobalTime)
            else {
                continue;
            };
            position = position.max(global_time);
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

    /// Compiler-owned source identities accepted in one strict peer closure.
    /// This is intentionally capability discovery only: it allocates no Groove
    /// inputs and never derives authority output from local rows.
    pub(crate) fn compiled_covered_input_sources_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<BTreeSet<ProgramSourceId>, Error> {
        let registered = self
            .unique_registered_binding_for_subscription(subscription)
            .ok_or(Error::InvalidStoredValue(
                "subscription referenced unregistered binding",
            ))?;
        let shape = self
            .query
            .registered_shapes
            .get(&subscription.shape_id)
            .ok_or(Error::InvalidStoredValue(
                "subscription referenced unregistered shape",
            ))?;
        let binding = shape.bind(
            shape
                .params()
                .keys()
                .cloned()
                .zip(registered.values.iter().cloned())
                .collect(),
        )?;
        let request = self.current_query_program_request(
            shape,
            &binding,
            registered.options.tier,
            registered.compiler_identity.clone(),
            CurrentQueryProgramOutput::MaintainedView,
            &registered.options.read_view,
            Some(registered.binding_view_key),
            QueryAuthorizationMode::ClientLocal,
        )?;
        Ok(query_program_source_requests(&request)
            .map_err(|report| Error::QueryCapability(format!("{report:?}")))?
            .into_iter()
            .filter(|source_request| {
                source_request.visibility == RowVisibility::Visible
                    && matches!(
                        request.reads.primary.sources.get(&source_request.source),
                        Some(SourceExpr::SettledBindingView { .. })
                    )
            })
            .map(|source_request| source_request.source.program_source_id())
            .collect())
    }

    async fn compile_current_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    async fn compile_current_query_program_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    #[cfg(test)]
    async fn compile_current_query_program_for_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    async fn compile_current_query_program_for_read_view_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    async fn compile_current_query_program_with_settled_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    async fn compile_current_query_program_with_settled_view_and_prepared_claim_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<QueryProgram, Error> {
        let allow_secondary_indexes = matches!(&output, CurrentQueryProgramOutput::MaintainedView);
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
            false,
        )?;
        // Retain the established, guarded primary-key paths. In particular a
        // policy-scoped or declared-`id` root must stay a complete current
        // source: a point cap there can strand a deletion-driven membership
        // transition. The new selector contributes only secondary equality
        // indexes for concrete maintained roots; it must not widen that
        // legacy guard by injecting another primary-key cap.
        let mut access_paths = self.current_query_primary_key_access_paths(shape, binding)?;
        if allow_secondary_indexes {
            access_paths.extend(
                self.query_program_access_paths(&request, true)?
                    .into_iter()
                    .filter(|(_, path)| matches!(path, CurrentAccessPath::Index { .. })),
            );
        }
        self.compile_query_program_request_with_access_paths(request, access_paths)
            .await
    }

    async fn compile_current_query_program_for_one_shot_read(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
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
        // One-shot reads can use every eligible access path. Maintained reads
        // deliberately retain their ordinary source except for the separately
        // proved physical primary-key path: secondary indexes can settle at a
        // frontier distinct from their maintained source, while one immutable
        // physical row has no such independent frontier.
        let access_paths = self.one_shot_access_paths(shape, binding, tier)?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
            .await
    }

    async fn compile_current_query_program_with_access_paths(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgram, Error> {
        let request = self.current_query_program_request_with_inline_binding_source(
            shape,
            binding,
            tier,
            identity,
            output,
            &ReadViewSpec::default(),
            authorization_mode,
        )?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
            .await
    }

    async fn compile_historical_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalTime,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
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
            output: current_query_output_request(output, shape.query(), &query_schema.schema),
        };
        self.compile_query_program_request(request).await
    }

    async fn compile_snapshot_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        snapshot: &Snapshot,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
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
            reads: snapshot_query_read_set(&input.shape, shape.schema_version(), snapshot.clone()),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, shape.query(), &query_schema.schema),
        };
        self.compile_query_program_request(request).await
    }

    async fn compile_include_deleted_query_program_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
        read_view: &ReadViewSpec,
    ) -> Result<QueryProgram, Error> {
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
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
            reads: query_read_set_for_read_view(
                &input.shape,
                shape.schema_version(),
                self.read_policy_schema_for_table_name(
                    &shape.query().table,
                    shape.schema_version(),
                    &input.shape,
                ),
                tier,
                read_view,
                None,
                None,
                shape.query().aggregate.is_some(),
                &self.catalogue.schema,
            )?,
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AppRows,
                shape.query(),
                &query_schema.schema,
            ),
        };
        // This one-shot include-deleted source has no deletion anti-join after
        // it. The proof remains deliberately narrower than ordinary visible
        // reads, which discard the physical cap before their anti-join.
        let access_paths = self.one_shot_access_paths(shape, binding, tier)?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
            .await
    }

    async fn compile_open_tx_query_program(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
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
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&lowered_shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
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
            output: current_query_output_request(
                output,
                lowered_shape.query(),
                &query_schema.schema,
            ),
        };
        self.compile_query_program_request(request).await
    }

    fn current_query_program_request(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
            false,
        )
    }

    fn current_query_program_request_with_inline_binding_source(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<QueryProgramRequest, Error> {
        self.current_query_program_request_with_prepared_claim_mode(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            None,
            authorization_mode,
            PreparedClaimBindingMode::Strict,
            true,
        )
    }

    /// The storage-backed maintained subset retrieves omitted witnesses by the
    /// result table name at the wire boundary.  A table-rename lens breaks
    /// that identity: its result member is named with the projected table but
    /// its persisted history is named with the authored table.  Keep such
    /// shapes on the self-contained witness path until the fallback resolves
    /// canonical physical identities end-to-end.
    fn storage_backed_maintained_root_has_identity_table_mapping(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
    ) -> bool {
        if schema_version != self.catalogue.current_schema_version_id {
            return false;
        }
        let Some(table_id) = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(table))
            .map(|mapping| mapping.table_id)
        else {
            return false;
        };
        self.catalogue.physical_mappings.values().all(|mapping| {
            mapping.tables.iter().all(|(candidate_name, candidate)| {
                candidate.table_id != table_id || candidate_name == table
            })
        })
    }

    fn current_query_program_request_with_prepared_claim_mode(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
        settled_binding_view: Option<BindingViewKey>,
        authorization_mode: QueryAuthorizationMode,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
        force_inline_binding_source: bool,
    ) -> Result<QueryProgramRequest, Error> {
        let policy = self.query_program_policy_context(identity);
        // Linked shapes carry read-policy alternatives so a trusted serving
        // authority can establish the authorized residual frontier.  Neither
        // System authority nor a client-local receiver may evaluate those
        // alternatives: System bypasses them, while a receiver consumes the
        // exact already-authorized CoveredInput closure.  Retaining branches
        // locally would both re-evaluate policy and turn policy-proof sources
        // into receiver inputs.
        let residual_shape;
        let residual_binding;
        let strips_policy_branches = matches!(policy, PolicyContext::System)
            || authorization_mode == QueryAuthorizationMode::ClientLocal;
        let (shape, binding) = if strips_policy_branches
            && !shape.query().policy_branches.is_empty()
        {
            let schema = if shape.schema_version() == self.catalogue.current_schema_version_id {
                &self.catalogue.schema
            } else {
                &self
                    .catalogue
                    .catalogue_schemas
                    .get(&shape.schema_version())
                    .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
                    .schema
            };
            let mut query = shape.query().clone();
            query.policy_branches.clear();
            residual_shape = query.validate_with_schema_version(schema, shape.schema_version())?;
            residual_binding = residual_shape.bind(
                binding
                    .values()
                    .iter()
                    .filter(|(name, _)| residual_shape.params().contains_key(*name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            )?;
            (&residual_shape, &residual_binding)
        } else {
            (shape, binding)
        };
        let lowered_shape;
        let lowered_binding;
        // Prepared binding sources are a serving-side optimization. Client
        // local execution must lower concrete bindings into its locally
        // available (already upstream-scoped at Edge/Global) data, rather
        // than trying to evaluate a server-maintained binding graph.
        let use_prepared_binding_source = authorization_mode
            == QueryAuthorizationMode::TrustedServing
            && !force_inline_binding_source
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
        let mut input_shape = self.normalized_row_set_shape(shape, binding)?;
        let settled_view_matches_query_window = settled_binding_view.is_some_and(|binding_view| {
            self.query
                .registered_shapes
                .get(&binding_view.shape_id)
                .is_some_and(|source_shape| {
                    source_shape.query().offset == shape.query().offset
                        && source_shape.query().limit == shape.query().limit
                })
        });
        let settled_window_input = (settled_view_matches_query_window
            && shape.query().aggregate.is_none())
        .then(|| match input_shape.nodes.get(&input_shape.root) {
            Some(RowSetExpr::Slice { input, .. }) => Some(input.clone()),
            _ => None,
        })
        .flatten();
        if let Some(input) = settled_window_input {
            // The settled binding source is the authority-selected result
            // membership, including LIMIT/OFFSET. Keep evaluating the public
            // row shape over those members, but do not slice that window a
            // second time on the client.
            input_shape.nodes.remove(&input_shape.root);
            input_shape.root = input;
        }
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
        // Prepared binding-source names are runtime identities.  Claim values
        // normally route independent bindings through one shape, but equal
        // author identities may hold distinct authenticated sessions.  Give
        // their claim scopes separate source identities so a later session
        // cannot replace an already-maintained sibling binding.
        let source_shape = source_shape.map(|source_shape| {
            self.active_session_claim_scope_key(identity)
                .map(|scope| format!("{source_shape}:session:{scope}"))
                .unwrap_or(source_shape)
        });
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=program_scope identity={identity:?} mode={authorization_mode:?} prepared={use_prepared_binding_source} source_shape={source_shape:?} strips_policy_branches={strips_policy_branches} query_policy_branches={} query_includes={} policy={policy:?}",
                shape.query().policy_branches.len(),
                shape.query().includes.len(),
            );
        }
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let root_has_read_policy = query_schema
            .schema
            .tables
            .iter()
            .find(|table| table.name == shape.query().table)
            .is_some_and(|table| table.read_policy.is_some());
        let storage_backed_result_materialization = matches!(output, CurrentQueryProgramOutput::MaintainedView)
                // A strict receiver obtains its result only by replacing its
                // descriptor-bound CoveredInput sources from this exact
                // settled authority view.  Dropping version witnesses here
                // would leave it with no source registry and tempt the
                // runtime to reopen local storage for an authority reset.
                // The storage-backed optimization remains valid for a cold
                // local-only maintained view, where there is no remote
                // closure to claim.
                // Client-local programs use the same mutable source slots
                // for ordinary propagated Local reads and strict remote
                // reads.  At opening time the latter has not yet learnt its
                // settled binding view, so that view cannot safely decide
                // whether source witnesses are needed.  Keep the registry
                // for every client-local maintained program; only trusted
                // serving may take the storage-backed shortcut.
                && authorization_mode != QueryAuthorizationMode::ClientLocal
                && settled_binding_view.is_none()
                && !root_has_read_policy
                && self.storage_backed_maintained_root_has_identity_table_mapping(
                    shape.schema_version(),
                    &shape.query().table,
                )
                && storage_backed_maintained_view_eligible(
                    shape.query(),
                    tier,
                    read_view,
                    &input_shape,
                );
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
        let mut output_request =
            current_query_output_request(output, shape.query(), &query_schema.schema);
        if storage_backed_result_materialization {
            // A simple current root query carries the exact visible content
            // transaction in its result-member terminal.  Keeping every
            // source version/replacement body in every binding merely so the
            // facade can re-read that immutable version multiplies retained
            // state by source rows × bindings.  The member's exact identity
            // is enough to load the immutable body from the node store on
            // entry; deletion/restore removals need only retire the old
            // occurrence, never materialize a newer winner.
            output_request
                .facts
                .remove(&ProgramFactKey::VersionWitnesses);
        }
        Ok(QueryProgramRequest {
            authorization_mode,
            reads: query_read_set_for_read_view(
                &input.shape,
                shape.schema_version(),
                policy_schema_version,
                tier,
                read_view,
                settled_binding_view,
                None,
                shape.query().aggregate.is_some(),
                &query_schema.schema,
            )?,
            policy,
            input,
            output: output_request,
        })
    }

    pub(crate) async fn query_rows_with_prepared_plan(
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
            AuthorSubject::SYSTEM,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn clear_prepared_query_plan_cache_for_test(&mut self) {
        self.query.query_shape_cache.clear();
    }

    #[cfg(test)]
    pub(crate) fn prepared_query_plan_cache_is_empty_for_test(&self) -> bool {
        self.query.query_shape_cache.is_empty()
    }

    pub(crate) async fn query_rows_with_prepared_plan_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorSubject,
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
        .await
    }

    /// Execute an ordinary local client read. The upstream serving edge is the
    /// confidentiality boundary; this path must not re-evaluate row policy.
    pub(crate) async fn query_rows_for_client(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    /// Allocate exact receiver input capabilities from compiler-owned source
    /// requirements, before lowering the receiver graph.  This deliberately
    /// does not compile a placeholder `SettledBindingView` from result
    /// members merely to discover its descriptor.
    fn allocate_client_receiver_input_sources(
        &mut self,
        request: &QueryProgramRequest,
        include_local_first_bootstrap: bool,
    ) -> Result<
        (
            BTreeMap<SourceId, InputSourceId>,
            BTreeMap<SourceId, RecordDescriptor>,
            BTreeMap<SourceId, InputSourceId>,
            BTreeMap<ProgramSourceId, maintained_views::CoveredInputSource>,
        ),
        Error,
    > {
        let mut runtime_sources = BTreeMap::new();
        let mut runtime_source_descriptors = BTreeMap::new();
        let mut provisional_local_gates = BTreeMap::new();
        let mut sources = BTreeMap::new();
        for source_request in query_program_source_requests(request)
            .map_err(|report| Error::QueryCapability(format!("{report:?}")))?
        {
            if source_request.visibility != RowVisibility::Visible {
                continue;
            }
            let Some(expression) = request.reads.primary.sources.get(&source_request.source) else {
                continue;
            };
            let settled = matches!(expression, SourceExpr::SettledBindingView { .. });
            let local_first = include_local_first_bootstrap
                && matches!(
                    expression,
                    SourceExpr::VisibleCurrent {
                        tier: DurabilityTier::Local,
                        ..
                    }
                );
            if !(settled || local_first) {
                continue;
            }
            let table = self.table_in_schema(
                &source_request.source.table,
                request.reads.primary.read_schema,
            )?;
            let metadata = read_sources::inline_source_metadata(&source_request.requirements, None);
            let descriptor =
                read_sources::current_row_descriptor_with_hidden_source_fields_for_current_storage(
                    &table, &metadata,
                );
            let input = self.database.allocate_input_source(descriptor.clone());
            let provisional_local_gate = local_first.then(|| {
                self.database.allocate_input_source(
                    maintained_views::local_first_bootstrap_gate_descriptor(),
                )
            });
            let source_id = source_request.source.program_source_id();
            if sources
                .insert(
                    source_id,
                    maintained_views::CoveredInputSource {
                        id: input,
                        descriptor,
                        provisional_local_gate,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidStoredValue(
                    "duplicate compiled receiver program source identity",
                ));
            }
            runtime_sources.insert(source_request.source.clone(), input);
            runtime_source_descriptors.insert(source_request.source.clone(), descriptor);
            if let Some(gate) = provisional_local_gate {
                provisional_local_gates.insert(source_request.source, gate);
            }
        }
        Ok((
            runtime_sources,
            runtime_source_descriptors,
            provisional_local_gates,
            sources,
        ))
    }

    /// Recompile one client-side settled read against ephemeral, exact
    /// CoveredInput sources.  A one-shot is not allowed to materialize an
    /// authority result set: it uses the same residual source descriptors as
    /// a maintained receiver, installs the exact closure, evaluates once,
    /// and retires its runtime-local sources immediately afterwards.
    ///
    /// `None` means the named authority result has not yet claimed a complete
    /// closure.  Callers preserve normal remote pending behavior in that
    /// case; they must not fall back to retained result members or storage.
    async fn compile_client_one_shot_with_covered_inputs(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        result_schema_version: SchemaVersionId,
        read_view: &ReadViewSpec,
        authority_result_key: &AuthorityResultKey,
    ) -> Result<Option<(QueryProgram, maintained_views::CoveredInputReceiver)>, Error> {
        let (runtime_sources, runtime_source_descriptors, provisional_local_gates, sources) =
            self.allocate_client_receiver_input_sources(&request, false)?;
        if sources.is_empty() {
            return Err(Error::InvalidStoredValue(
                "client settled read has no covered-input source occurrences",
            ));
        }
        let program = match self
            .compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs(
                request,
                BTreeMap::new(),
                access_paths,
                runtime_sources,
                runtime_source_descriptors,
                provisional_local_gates,
            )
            .await
        {
            Ok(program) => program,
            Err(error) => {
                self.retire_covered_input_sources(&sources).await?;
                return Err(error);
            }
        };
        let mut receiver = maintained_views::CoveredInputReceiver::new(sources, read_view.clone());
        let installed = match self
            .replace_covered_input_receiver(
                &mut receiver,
                result_schema_version,
                authority_result_key,
            )
            .await
        {
            Ok(installed) => installed,
            Err(error) => {
                self.retire_covered_input_sources(&receiver.sources).await?;
                return Err(error);
            }
        };
        if !installed {
            self.retire_covered_input_sources(&receiver.sources).await?;
            return Ok(None);
        }
        Ok(Some((program, receiver)))
    }

    /// Every descriptor-bound receiver allocation has both a data source and,
    /// for Local-first, an optional bootstrap gate. Compilation/closure
    /// validation failures happen before either is owned by a subscription, so
    /// they must retire both rather than leaving unreachable Groove inputs.
    async fn retire_covered_input_sources(
        &mut self,
        sources: &BTreeMap<ProgramSourceId, maintained_views::CoveredInputSource>,
    ) -> Result<(), Error> {
        self.database
            .retire_input_sources(
                sources.values().flat_map(|source| {
                    std::iter::once(source.id).chain(source.provisional_local_gate)
                }),
            )
            .await
            .map(|_| ())
            .map_err(Error::Groove)
    }

    pub(crate) async fn query_rows_local_preview(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan(shape, binding, DurabilityTier::Local, prepared_plan)
            .await
    }

    pub(crate) async fn query_rows_local_preview_profiled(
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
            AuthorSubject::SYSTEM,
        )
        .await
    }

    pub(crate) async fn query_rows_including_deleted_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorSubject,
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
        .await
    }

    async fn query_rows_with_options_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorSubject,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        if include_deleted {
            let mut rows = self
                .query_rows_including_deleted_with_query_engine(
                    shape,
                    binding,
                    tier,
                    identity,
                    authorization_mode,
                    &ReadViewSpec::default(),
                )
                .await?;
            let query = shape.query();
            self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
            self.apply_projection_in_schema(query, shape.schema_version(), &mut rows)?;
            return Ok(rows);
        }
        let client_settled_binding_view = (authorization_mode
            == QueryAuthorizationMode::ClientLocal)
            .then(|| {
                self.client_settled_binding_view_for_query(
                    shape,
                    binding,
                    tier,
                    &ReadViewSpec::default(),
                )
            })
            .flatten();
        let settled_binding_view = match authorization_mode {
            QueryAuthorizationMode::ClientLocal => {
                client_settled_binding_view.as_ref().map(|view| view.key)
            }
            // A serving node evaluates its complete authority program. A
            // `SettledBindingView` is a receiver-local CoveredInput source,
            // not a server-side cache or an alternate trusted read path.
            QueryAuthorizationMode::TrustedServing => None,
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
        // A binding-view name is a routing key, not authorization evidence.
        // Client-side source installation must select the one exact,
        // policy-scoped receipt that owns that view before it can evaluate a
        // one-shot graph.
        let settled_authority_result_key =
            if authorization_mode == QueryAuthorizationMode::ClientLocal {
                settled_binding_view.and_then(|binding_view| {
                    self.unique_authority_result_key_for_binding_view(binding_view)
                })
            } else {
                None
            };
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && settled_binding_view.is_some()
            && settled_authority_result_key.is_none()
        {
            return Ok(Vec::new());
        }
        let has_one_shot_access_path = settled_binding_view.is_none()
            && !self.one_shot_access_paths(shape, binding, tier)?.is_empty();
        // A concrete one-shot access path is binding-specific. Inline that
        // binding so execution keeps the selected graph instead of replacing it
        // with the generic cached parameterized plan. Prepared Local reads also
        // take this path: their reusable graph cannot embed the current binding's
        // physical index prefix and would otherwise hydrate the complete table.
        let inline_query = if has_one_shot_access_path {
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
        let rebased_window_query = client_settled_binding_view
            .as_ref()
            .and_then(|view| view.retained_window.as_ref())
            .map(|source_window| {
                let schema = self
                    .catalogue
                    .catalogue_schemas
                    .get(&shape.schema_version())
                    .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
                let mut rebased_query = shape.query().clone();
                let (relative_offset, relative_limit) = source_window.relative_window_for(shape);
                rebased_query.offset = relative_offset;
                rebased_query.limit = relative_limit;
                let rebased_shape = rebased_query
                    .validate_with_schema_version(&schema.schema, shape.schema_version())?;
                let rebased_binding = rebased_shape.bind(binding.values().clone())?;
                Ok::<_, Error>((rebased_shape, rebased_binding))
            })
            .transpose()?;
        let (shape, binding) = rebased_window_query
            .as_ref()
            .or(inline_query.as_ref())
            .as_ref()
            .map(|(shape, binding)| (shape, binding))
            .unwrap_or((shape, binding));
        let prepared_plan = prepared_plan.filter(|plan| {
            !has_one_shot_access_path
                && !matches!(plan.as_ref(), PreparedQueryPlan::PeerMaintainedMarker)
        });
        let mut ephemeral_covered_inputs = None;
        let program = if prepared_plan.is_some() {
            None
        } else if let Some(authority_result_key) = settled_authority_result_key.as_ref() {
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
            let access_paths = self.one_shot_access_paths(shape, binding, tier)?;
            let Some((program, receiver)) = self
                .compile_client_one_shot_with_covered_inputs(
                    request,
                    access_paths,
                    shape.schema_version(),
                    &ReadViewSpec::default(),
                    authority_result_key,
                )
                .await?
            else {
                return Ok(Vec::new());
            };
            ephemeral_covered_inputs = Some(receiver);
            Some(program)
        } else {
            Some(
                self.compile_current_query_program_for_one_shot_read(
                    shape,
                    binding,
                    tier,
                    identity,
                    settled_binding_view,
                    authorization_mode,
                )
                .await?,
            )
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
                Some(
                    self.prepared_query_plan(shape, binding, tier, identity)
                        .await?,
                )
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
                    )
                    .await?,
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
                .await
                .map_err(Error::Groove),
            Some(plan) => match plan.as_ref() {
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(
                        binding,
                        params,
                        &policy,
                        PreparedClaimBindingMode::Strict,
                    )?;
                    take_required_sink_deltas(
                        self.bind_shape_snapshot(*shape, &values).await?,
                        JAZZ_APP_ROWS_SINK,
                    )
                }
                PreparedQueryPlan::Graph(graph) => self
                    .database
                    .query_graph(graph.clone())
                    .await
                    .map_err(Error::Groove),
                PreparedQueryPlan::PeerMaintainedMarker => {
                    unreachable!("peer maintained markers are filtered before query execution")
                }
            },
        };
        // Retire transient receiver inputs even if the one-shot graph itself
        // fails.  These identities are runtime-local capabilities and must
        // never be re-used by a later receipt.
        let retire_result = if let Some(receiver) = ephemeral_covered_inputs {
            self.database
                .retire_input_sources(receiver.sources.values().map(|source| source.id))
                .await
                .map(|_| ())
                .map_err(Error::Groove)
        } else {
            Ok(())
        };
        let deltas = deltas_result?;
        retire_result?;
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

    async fn query_rows_with_options_for_identity_profiled(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
        identity: AuthorSubject,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let total_started = Instant::now();
        let phase_started = Instant::now();
        // Profiled reads are trusted-serving only, so they evaluate the
        // complete authority program rather than attempting to resolve a
        // receiver-local CoveredInput receipt.
        let settled_binding_view = None;
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
            Some(
                self.compile_current_query_program_for_one_shot_read(
                    shape,
                    binding,
                    tier,
                    identity,
                    settled_binding_view,
                    QueryAuthorizationMode::TrustedServing,
                )
                .await?,
            )
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
                Some(
                    self.prepared_query_plan(shape, binding, tier, identity)
                        .await?,
                )
            }
            None if settled_binding_view.is_none() && needs_binding() => Some(std::sync::Arc::new(
                self.prepared_query_plan_from_program(
                    program
                        .as_ref()
                        .expect("program is compiled when no prepared plan is supplied"),
                    shape,
                    binding,
                )
                .await?,
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
                .await
                .map_err(Error::Groove),
            Some(plan) => match plan.as_ref() {
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(
                        binding,
                        params,
                        &policy,
                        PreparedClaimBindingMode::Strict,
                    )?;
                    take_required_sink_deltas(
                        self.bind_shape_snapshot(*shape, &values).await?,
                        JAZZ_APP_ROWS_SINK,
                    )
                }
                PreparedQueryPlan::Graph(graph) => self
                    .database
                    .query_graph(graph.clone())
                    .await
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

    #[cfg(test)]
    fn settled_binding_view_key_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Option<BindingViewKey>, Error> {
        if self.is_history_complete()
            || !self.can_use_prepared_current_query_plan(shape)
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
            .authority_result_state_for_binding_view(binding_view_key)
            .is_some()
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
        self.client_settled_binding_view_for_query(shape, binding, tier, read_view)
            .map(|view| view.key)
    }

    fn client_settled_binding_view_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Option<ClientSettledBindingView> {
        // A no-tier read is an ordinary process-local overlay read. It has no
        // authority-receipt contract, so constructing a synthetic binding
        // view here would make the caller wait for a receipt that cannot
        // exist and incorrectly hide its local rows.
        if tier == DurabilityTier::None {
            return None;
        }
        if tier == DurabilityTier::Local {
            // A Local subscription still propagates. Once its own exact
            // authority closure has arrived, an identical Local read must
            // consume that receiver-local source graph rather than reopening
            // the raw overlay. This direct receipt is distinct from the
            // bounded Edge-page reuse below: the latter is a compiler-owned
            // containment optimization for a *different* Local query shape.
            let local_read_view = RegisterShapeOptions {
                tier: DurabilityTier::Local,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key();
            let direct = self
                .query
                .authority_results
                .iter()
                .filter_map(|(authority_key, state)| {
                    (authority_key.binding_view
                        == BindingViewKey::new(
                            shape.shape_id(),
                            binding.binding_id(),
                            local_read_view,
                        )
                        && matches!(state.source_closure, AuthoritySourceClosure::Claimed { .. }))
                    .then_some(ClientSettledBindingView {
                        key: authority_key.binding_view,
                        retained_window: RetainedRootWindowSource::for_shape(shape)
                            .is_bounded()
                            .then(|| RetainedRootWindowSource::for_shape(shape)),
                    })
                })
                .collect::<Vec<_>>();
            if direct.len() == 1 {
                return direct.into_iter().next();
            }
            if direct.len() > 1 {
                // A public local facade may not guess across policy scopes.
                return None;
            }
            // A non-durable foreground can use an active exact source page,
            // or reuse a detached page only through its compiler-owned
            // descriptor. There is deliberately no search through similarly
            // shaped registrations or authority output.
            if self.authored_commit_durability != DurabilityTier::None {
                return None;
            }
            let edge_read_view = RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key();
            let active = self
                .query
                .authority_results
                .iter()
                .filter_map(|(authority_key, state)| {
                    if authority_key.binding_view.binding_id != binding.binding_id()
                        || authority_key.binding_view.read_view != edge_read_view
                        || !matches!(state.source_closure, AuthoritySourceClosure::Claimed { .. })
                    {
                        return None;
                    }
                    let source_shape = self
                        .query
                        .registered_shapes
                        .get(&authority_key.binding_view.shape_id)?;
                    let descriptor = RetainedRootWindowSource::for_shape(source_shape);
                    descriptor
                        .is_bounded()
                        .then_some(descriptor)
                        .filter(|descriptor| descriptor.contains_target(shape))
                        .map(|descriptor| ClientSettledBindingView {
                            key: authority_key.binding_view,
                            retained_window: Some(descriptor),
                        })
                })
                .collect::<Vec<_>>();
            if active.len() == 1 {
                return active.into_iter().next();
            }
            // More than one active policy scope is not a public local-read
            // capability. The ordinary local overlay remains available, but
            // cannot impersonate either policy-scoped authority source.
            if active.len() > 1 {
                return None;
            }
            let matches = self
                .query
                .retained_root_window_sources
                .iter()
                .filter(|(authority_key, descriptor)| {
                    authority_key.binding_view.binding_id == binding.binding_id()
                        && authority_key.binding_view.read_view == edge_read_view
                        && descriptor.is_bounded()
                        && descriptor.contains_target(shape)
                })
                .map(|(authority_key, descriptor)| ClientSettledBindingView {
                    key: authority_key.binding_view,
                    retained_window: Some(descriptor.clone()),
                })
                .collect::<Vec<_>>();
            // A query facade has no caller-controlled policy selector. Never
            // guess across two scoped receipts; the scope owner must open a
            // fresh authority usage site instead.
            return (matches.len() == 1)
                .then(|| matches.into_iter().next().expect("one retained window"));
        }
        let settled_tier = tier;
        let binding_view = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            RegisterShapeOptions {
                // A non-durable browser-side runtime consumes the worker
                // relay's Edge handoff. Ordinary durable clients retain the
                // Global source their upstream coverage actually populated.
                // The relay-only authority source is selected explicitly by
                // `open_seeded_relay_edge_subscription_view` below.
                tier: if self.authored_commit_durability == DurabilityTier::None {
                    settled_tier
                } else {
                    DurabilityTier::Global
                },
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
        );
        Some(ClientSettledBindingView {
            key: binding_view,
            retained_window: RetainedRootWindowSource::for_shape(shape)
                .is_bounded()
                .then(|| RetainedRootWindowSource::for_shape(shape)),
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

    #[cfg(test)]
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

    pub(crate) fn transaction_row_keys_for_query(
        &self,
        shape: &ValidatedQuery,
        row_keys: &BTreeSet<(String, RowUuid)>,
    ) -> BTreeSet<(String, RowUuid)> {
        let query_tables = self.query_storage_read_tables(shape);
        let mut row_keys = row_keys.clone();
        if let Some(query_tables) = query_tables {
            row_keys.retain(|(table, _)| query_tables.contains(table));
        }
        row_keys
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

    /// Evaluate a validated query against the globally settled state at
    /// `position`.
    ///
    /// This is a settled-history read: it considers only transactions with
    /// `global_time <= position`, chooses the ordinary per-row winners from
    /// that subset, and evaluates the query against that historical state.
    pub async fn query_rows_at(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalTime,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.require_catalogue_ready()?;
        self.query_rows_at_for_identity(shape, binding, position, AuthorSubject::SYSTEM)
            .await
    }

    async fn query_rows_at_for_identity(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalTime,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = self
            .query_rows_at_with_query_engine(shape, binding, position, identity)
            .await?;
        let query = shape.query();
        self.finish_engine_query_rows_in_schema(query, shape.schema_version(), &mut rows)?;
        Ok(rows)
    }

    async fn query_rows_at_with_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalTime,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program = self
            .compile_historical_query_program(
                &lowered_shape,
                &binding,
                position,
                identity,
                CurrentQueryProgramOutput::AppRows,
            )
            .await?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .await
            .map_err(Error::Groove)?;
        let table = self
            .table_in_schema(&lowered_shape.query().table, lowered_shape.schema_version())?
            .clone();
        self.materialize_historical_query_rows(table, deltas)
    }

    pub(super) async fn query_rows_at_snapshot(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        snapshot: &Snapshot,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
        let lowered_shape =
            inline_snapshot_bind_filter_literals(shape, binding, &read_schema.schema)?;
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program = self
            .compile_snapshot_query_program(
                &lowered_shape,
                &binding,
                snapshot,
                AuthorSubject::SYSTEM,
                CurrentQueryProgramOutput::AppRows,
            )
            .await?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .await
            .map_err(Error::Groove)?;
        let table = self
            .table_in_schema(&lowered_shape.query().table, lowered_shape.schema_version())?
            .clone();
        let mut rows = self.materialize_historical_query_rows(table, deltas)?;
        self.finish_engine_query_rows_in_schema(
            lowered_shape.query(),
            lowered_shape.schema_version(),
            &mut rows,
        )?;
        Ok(rows)
    }

    pub(super) async fn query_rows_including_deleted_with_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
        read_view: &ReadViewSpec,
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
        let program = self
            .compile_include_deleted_query_program_in_authorization_mode(
                &lowered_shape,
                &binding,
                tier,
                identity,
                authorization_mode,
                read_view,
            )
            .await?;
        let deltas = self
            .database
            .query_graph(lowered_materialization_app_rows_graph(&program)?)
            .await
            .map_err(Error::Groove)?;
        if query.aggregate.is_some() {
            self.materialize_aggregate_query_rows(query, &table, deltas)
        } else {
            self.materialize_include_deleted_query_rows(table, deltas)
        }
    }

    #[allow(dead_code)]
    pub(super) async fn current_rows_at(
        &mut self,
        table: &str,
        position: GlobalTime,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_engine_read_metrics
            .source_global_time_range_scans += 1;
        self.bounded_historical_current_rows(table, position).await
    }

    async fn bounded_global_change_records_at(
        &mut self,
        table: &str,
        position: GlobalTime,
    ) -> Result<Vec<groove::db::EncodedKeyValue<'_>>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_schema_version_id, table)?;
        if position.0 == u64::MAX {
            Ok(self
                .database
                .index_scan_raw(
                    "jazz_global_changes",
                    "by_table_global_time",
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                    ],
                )
                .await?)
        } else {
            Ok(self
                .database
                .index_scan_range_raw(
                    "jazz_global_changes",
                    "by_table_global_time",
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                        Value::U64(0),
                    ],
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                        Value::U64(position.0 + 1),
                    ],
                )
                .await?)
        }
    }

    async fn bounded_historical_current_rows(
        &mut self,
        table: &str,
        position: GlobalTime,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let mut rows_by_uuid = BTreeMap::<
            RowUuid,
            (
                Option<(TxTime, NodeAlias)>,
                Option<(TxTime, NodeAlias, Option<DeletionEvent>)>,
            ),
        >::new();
        for raw in self
            .bounded_global_change_records_at(table, position)
            .await?
        {
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
                )
                .await?
                .ok_or(Error::InvalidStoredValue(
                    "historical content winner is missing",
                ))?;
            rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }
    #[allow(dead_code)]
    async fn historical_content_witness_at(
        &mut self,
        table: &str,
        read_schema: SchemaVersionId,
        row_uuid: RowUuid,
        position: GlobalTime,
    ) -> Result<Option<TxId>, Error> {
        let mut content = None::<(TxTime, NodeAlias)>;
        let mut latest_event = None::<(TxTime, NodeAlias, Option<DeletionEvent>)>;
        let table_id = self.physical_table_id_for_schema(read_schema, table)?;
        let raw_records = if position.0 == u64::MAX {
            self.database
                .index_scan_raw(
                    "jazz_global_changes",
                    "by_table_global_time",
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                    ],
                )
                .await?
        } else {
            self.database
                .index_scan_range_raw(
                    "jazz_global_changes",
                    "by_table_global_time",
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                        Value::U64(0),
                    ],
                    &[
                        Value::U64(table_id.0),
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                        Value::U64(position.0 + 1),
                    ],
                )
                .await?
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
    async fn query_relation_snapshot_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        // Structured reads are not an exception to receiver-local authority
        // evaluation.  Use the exact same selected receipt and ephemeral
        // descriptor-bound inputs as scalar one-shots; otherwise a tree read
        // could silently reopen local storage while a scalar read correctly
        // consumes CoveredInput.
        let client_settled_view = (authorization_mode == QueryAuthorizationMode::ClientLocal)
            .then(|| self.client_settled_binding_view_for_query(shape, binding, tier, read_view))
            .flatten();
        let settled_binding_view = client_settled_view.as_ref().map(|view| view.key);
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && tier >= DurabilityTier::Edge
            && settled_binding_view.is_none()
        {
            return Ok(RelationSnapshot {
                root_count: 0,
                rows: Vec::new(),
                edges: Vec::new(),
            });
        }
        let authority_result_key = settled_binding_view.and_then(|binding_view| {
            self.unique_authority_result_key_for_binding_view(binding_view)
        });
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && settled_binding_view.is_some()
            && authority_result_key.is_none()
        {
            return Ok(RelationSnapshot {
                root_count: 0,
                rows: Vec::new(),
                edges: Vec::new(),
            });
        }
        let (program, receiver) = if let Some(authority_result_key) = authority_result_key.as_ref()
        {
            let request = self.current_query_program_request(
                shape,
                binding,
                tier,
                identity,
                CurrentQueryProgramOutput::RelationSnapshot,
                read_view,
                settled_binding_view,
                authorization_mode,
            )?;
            let access_paths = self.one_shot_access_paths(shape, binding, tier)?;
            let Some((program, receiver)) = self
                .compile_client_one_shot_with_covered_inputs(
                    request,
                    access_paths,
                    shape.schema_version(),
                    read_view,
                    authority_result_key,
                )
                .await?
            else {
                return Ok(RelationSnapshot {
                    root_count: 0,
                    rows: Vec::new(),
                    edges: Vec::new(),
                });
            };
            (program, Some(receiver))
        } else {
            (
                self.compile_current_query_program_for_read_view_in_authorization_mode(
                    shape,
                    binding,
                    tier,
                    identity,
                    CurrentQueryProgramOutput::RelationSnapshot,
                    read_view,
                    authorization_mode,
                )
                .await?,
                None,
            )
        };
        let snapshots_result = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .await
            .map_err(Error::Groove);
        let retire_result = if let Some(receiver) = receiver {
            self.retire_covered_input_sources(&receiver.sources).await
        } else {
            Ok(())
        };
        let snapshots = snapshots_result?;
        retire_result?;
        self.materialize_relation_snapshot_from_query_engine(shape, read_view, &snapshots)
            .await
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

    pub(crate) async fn prepare_query_binding_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlanHandle), Error> {
        let (shape, binding) = self.query_binding_for_link(shape, binding)?;
        let plan = self
            .prepared_query_plan(&shape, &binding, tier, identity)
            .await?;
        Ok((shape, binding, plan))
    }

    pub(crate) async fn prepare_query_binding_for_link_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        match authorization_mode {
            QueryAuthorizationMode::ClientLocal => {
                self.prepare_client_subscription_binding(shape, binding, tier, identity)
                    .await
            }
            QueryAuthorizationMode::TrustedServing => {
                self.prepare_trusted_subscription_binding(shape, binding, tier, identity)
                    .await
            }
        }
    }

    async fn prepare_client_subscription_binding(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        let (shape, binding, plan) = self
            .prepare_query_binding_for_link_with_shared_claim_fragments(
                shape, binding, tier, identity,
            )
            .await?;
        Ok((
            shape,
            binding,
            SubscriptionPreparedPlan {
                plan,
                authorization_mode: QueryAuthorizationMode::ClientLocal,
            },
        ))
    }

    async fn prepare_trusted_subscription_binding(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<(ValidatedQuery, Binding, SubscriptionPreparedPlan), Error> {
        let (shape, binding, plan) = self
            .prepare_query_binding_for_link(shape, binding, tier, identity)
            .await?;
        Ok((
            shape,
            binding,
            SubscriptionPreparedPlan {
                plan,
                authorization_mode: QueryAuthorizationMode::TrustedServing,
            },
        ))
    }

    pub(crate) async fn prepare_query_binding_for_link_with_shared_claim_fragments(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlanHandle), Error> {
        let (shape, binding) = self.query_binding_for_link(shape, binding)?;
        // This plan only keeps the local maintained subscription's graph alive.
        // The upstream shape is registered separately below, where serving
        // compilation stays TrustedServing. Do not lower local policy here:
        // locally stored rows are already scoped by that upstream boundary.
        let program = self
            .compile_current_query_program_in_authorization_mode(
                &shape,
                &binding,
                tier,
                identity,
                CurrentQueryProgramOutput::AppRows,
                QueryAuthorizationMode::ClientLocal,
            )
            .await?;
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
                    self.prepared_query_plan_from_program(&program, &shape, &binding)
                        .await?,
                );
                self.query.query_shape_cache.insert(key, plan.clone());
                plan
            }
        } else {
            std::sync::Arc::new(
                self.prepared_query_plan_from_program(&program, &shape, &binding)
                    .await?,
            )
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

    pub(crate) async fn query_rows_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan_for_identity(shape, binding, tier, None, identity)
            .await
    }

    /// Execute a serving query with its root constrained to a physical row
    /// UUID. This is for internal authorization probes: public `id` may be a
    /// declared user column and must not be used as the storage-row selector.
    pub(crate) async fn query_rows_for_link_physical_row(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        row_uuid: RowUuid,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_for_physical_row_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            row_uuid,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    pub(crate) async fn query_rows_for_client_physical_row(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        row_uuid: RowUuid,
    ) -> Result<Vec<CurrentRow>, Error> {
        #[cfg(test)]
        CLIENT_PHYSICAL_ROW_QUERY_CALLS.with(|calls| calls.set(calls.get() + 1));
        self.query_rows_for_physical_row_in_authorization_mode(
            shape,
            binding,
            tier,
            identity,
            row_uuid,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    async fn query_rows_for_physical_row_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        row_uuid: RowUuid,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table = self
            .table_in_schema(&shape.query().table, shape.schema_version())?
            .clone();
        let access_paths = BTreeMap::from([(
            root_source_id(&shape.query().table),
            CurrentAccessPath::PrimaryKey(vec![Value::Uuid(row_uuid.0)]),
        )]);
        let program = self
            .compile_current_query_program_with_access_paths(
                shape,
                binding,
                tier,
                identity,
                CurrentQueryProgramOutput::AppRows,
                access_paths,
                authorization_mode,
            )
            .await?;
        // A policy can introduce claim parameters even though this physical
        // row lookup has no public query parameters. Those programs must go
        // through Groove's prepare/bind boundary just like ordinary serving
        // reads; executing the lowered graph directly leaves its binding
        // source unprepared and fails instead of representing a denied read.
        let plan = self
            .prepared_query_plan_from_program(&program, shape, binding)
            .await?;
        let policy = self.query_program_policy_context(identity);
        let deltas = match plan {
            PreparedQueryPlan::Prepared { shape, params } => {
                let values = binding_values_for_plan(
                    binding,
                    &params,
                    &policy,
                    PreparedClaimBindingMode::Strict,
                )?;
                self.bind_disposable_shape_snapshot(shape, &values).await?
            }
            PreparedQueryPlan::Graph(graph) => self
                .database
                .query_graph(graph)
                .await
                .map_err(Error::Groove)?,
            PreparedQueryPlan::PeerMaintainedMarker => {
                unreachable!("point reads never use peer-maintained plans")
            }
        };
        let mut rows = self.materialize_inline_current_query_rows(&table, deltas)?;
        self.finish_engine_query_rows_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        Ok(rows)
    }

    #[cfg(test)]
    pub(crate) async fn query_rows_for_link_forced_full_scan_for_test(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        let program = self
            .compile_query_program_request_with_access_paths(request, BTreeMap::new())
            .await?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .await
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
    pub(crate) async fn query_relation_snapshot_for_serving(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
    ) -> Result<RelationSnapshot, Error> {
        self.query_relation_snapshot_for_serving_in_read_view(
            shape,
            binding,
            tier,
            identity,
            &ReadViewSpec::default(),
        )
        .await
    }

    pub(crate) async fn query_relation_snapshot_for_serving_in_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    pub(crate) async fn query_relation_snapshot_for_client(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
    }

    pub(crate) async fn subscription_snapshot_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        #[cfg(test)]
        record_subscription_snapshot_for_link_call();
        if shape.query().array_subqueries.is_empty() && read_view == &ReadViewSpec::default() {
            let rows = match authorization_mode {
                QueryAuthorizationMode::ClientLocal => {
                    self.query_rows_for_client(shape, binding, tier, identity)
                        .await?
                }
                QueryAuthorizationMode::TrustedServing => {
                    self.query_rows_with_prepared_plan_for_identity(
                        shape, binding, tier, None, identity,
                    )
                    .await?
                }
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
                    .await
            }
            QueryAuthorizationMode::TrustedServing => {
                self.query_relation_snapshot_for_serving_in_read_view(
                    shape, binding, tier, identity, read_view,
                )
                .await
            }
        }
    }

    #[allow(dead_code)] // Slice 2 wires this into API-level routing.
    pub(crate) async fn query_rows_at_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalTime,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_at_for_identity(shape, binding, position, identity)
            .await
    }

    pub(crate) fn uses_schema_projected_read(&self, shape: &ValidatedQuery) -> bool {
        shape.schema_version() != self.catalogue.current_schema_version_id
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
    pub async fn tx_query(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_with_options(tx_id, shape, binding, false)
            .await
    }

    /// Evaluate a validated query inside an open transaction using the local
    /// client read boundary with explicit root-row deletion visibility.
    pub async fn tx_query_with_options(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_in_authorization_mode(
            tx_id,
            shape,
            binding,
            AuthorSubject::SYSTEM,
            include_deleted,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Evaluate a validated query inside an open exclusive transaction as `identity`.
    pub async fn tx_query_for_identity(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_for_identity_with_options(tx_id, shape, binding, identity, false)
            .await
    }

    /// Evaluate a validated query inside an open transaction with explicit
    /// root-row deletion visibility.
    pub async fn tx_query_for_identity_with_options(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
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
        .await
    }

    /// Evaluate a query and its relation payload inside an open transaction.
    ///
    /// This uses the same transaction snapshot and staged overlay as
    /// [`Self::tx_query_with_options`].  In particular, it must not fall back
    /// to the ordinary maintained relation view: doing so would silently omit
    /// writes staged by the transaction.
    pub(crate) async fn tx_relation_snapshot_with_options(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        include_deleted: bool,
    ) -> Result<RelationSnapshot, Error> {
        self.tx_relation_snapshot_in_authorization_mode(
            tx_id,
            shape,
            binding,
            AuthorSubject::SYSTEM,
            include_deleted,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Evaluate a relation payload inside an open transaction as its bound
    /// serving identity.
    pub(crate) async fn tx_relation_snapshot_for_identity_with_options(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        include_deleted: bool,
    ) -> Result<RelationSnapshot, Error> {
        self.tx_relation_snapshot_in_authorization_mode(
            tx_id,
            shape,
            binding,
            identity,
            include_deleted,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    async fn tx_relation_snapshot_in_authorization_mode(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<RelationSnapshot, Error> {
        let identity = self.transaction_query_identity(tx_id, identity, authorization_mode)?;
        let predicate_len = self.open_tx(tx_id)?.predicate_reads.len();
        let program = self
            .compile_open_tx_query_program(
                tx_id,
                shape,
                binding,
                identity,
                CurrentQueryProgramOutput::RelationSnapshot,
                include_deleted,
                authorization_mode,
            )
            .await?;
        let snapshots = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .await
            .map_err(Error::Groove)?;
        let snapshot = self
            .materialize_relation_snapshot_from_query_engine(
                shape,
                &ReadViewSpec::default(),
                &snapshots,
            )
            .await?;
        let predicate_read = PredicateRead {
            table: shape.query().table.clone(),
            shape_id: shape.shape_id(),
            shape: shape.query().clone(),
            binding_id: binding.binding_id(),
            binding_values: binding.values().clone(),
        };
        let open_tx = self.open_tx_mut(tx_id)?;
        open_tx.predicate_reads.truncate(predicate_len);
        open_tx.predicate_reads.push(predicate_read);
        Ok(snapshot)
    }

    async fn tx_query_in_authorization_mode(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let identity = self.transaction_query_identity(tx_id, identity, authorization_mode)?;
        let query = shape.query();
        let predicate_len = self.open_tx(tx_id)?.predicate_reads.len();
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        let program = self
            .compile_open_tx_query_program(
                tx_id,
                shape,
                binding,
                identity,
                CurrentQueryProgramOutput::AppRows,
                include_deleted,
                authorization_mode,
            )
            .await?;
        let deltas = self
            .database
            .query_graph(lowered_materialization_app_rows_graph(&program)?)
            .await
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

    fn transaction_query_identity(
        &self,
        tx_id: OpenTransactionId,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<AuthorSubject, Error> {
        Ok(match self.open_tx(tx_id)?.kind {
            OpenTransactionKind::Exclusive {
                bound_author: Some(bound_identity),
            } => {
                if matches!(authorization_mode, QueryAuthorizationMode::TrustedServing)
                    && identity != bound_identity
                {
                    return Err(Error::OpenTransactionIdentityMismatch);
                }
                // Explicitly bound exclusive transactions are identity capabilities:
                // ordinary and serving reads use the identity fixed at begin.
                bound_identity
            }
            OpenTransactionKind::Exclusive { bound_author: None } => identity,
            OpenTransactionKind::Mergeable {
                permission_subject: Some(bound_identity),
                ..
            } => {
                if matches!(authorization_mode, QueryAuthorizationMode::TrustedServing)
                    && identity != bound_identity
                {
                    return Err(Error::OpenTransactionIdentityMismatch);
                }
                // A serving-side mergeable batch is also an identity
                // capability. The raw foreign-function argument selects no
                // authority beyond the subject fixed at begin.
                bound_identity
            }
            OpenTransactionKind::Mergeable {
                permission_subject: None,
                ..
            } => identity,
        })
    }

    pub(crate) async fn prepared_query_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        let program = self
            .compile_current_query_program(
                shape,
                binding,
                tier,
                identity,
                CurrentQueryProgramOutput::AppRows,
            )
            .await?;
        let plan = std::sync::Arc::new(
            self.prepared_query_plan_from_program(&program, shape, binding)
                .await?,
        );
        self.query.query_shape_cache.insert(key, plan.clone());
        Ok(plan)
    }

    pub(crate) async fn ensure_peer_maintained_subscription_view_supported(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorSubject,
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
        .await
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

    #[allow(dead_code)] // Test-only and feature-gated direct view callers keep the no-owner form.
    pub(crate) async fn open_seeded_maintained_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
        ),
        Error,
    > {
        self.open_seeded_maintained_subscription_view_with_waker(
            shape,
            binding,
            identity,
            tier,
            read_view,
            RegisterShapeOptions {
                tier,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
            None,
        )
        .await
    }

    /// Owner-loop variant retaining a durable wake route during cold initial
    /// hydration.
    pub(crate) async fn open_seeded_maintained_subscription_view_with_waker(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        read_view_key: ReadViewKey,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
        ),
        Error,
    > {
        self.open_seeded_maintained_subscription_view_in_authorization_mode(
            shape,
            binding,
            identity,
            tier,
            read_view,
            read_view_key,
            QueryAuthorizationMode::TrustedServing,
            None,
            None,
            PreparedClaimBindingMode::Strict,
            progress_waker,
        )
        .await
        .map(
            |(subscription, maintained, schemas, transitions, tables, received, _)| {
                (
                    subscription,
                    maintained,
                    schemas,
                    transitions,
                    tables,
                    received,
                )
            },
        )
    }

    /// Re-publish an Edge window from a durable relay to its non-durable
    /// browser peer. The relay's Global receipt already names the
    /// authority-selected members, so this must consume that membership as
    /// its source instead of applying the query window a second time.
    #[allow(dead_code)] // Test-only and feature-gated direct view callers keep the no-owner form.
    pub(crate) async fn open_seeded_relay_edge_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        read_view: &ReadViewSpec,
        authority_result_key: AuthorityResultKey,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
            maintained_views::CoveredInputReceiver,
        ),
        Error,
    > {
        self.open_seeded_relay_edge_subscription_view_with_waker(
            shape,
            binding,
            identity,
            read_view,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
            authority_result_key,
            None,
        )
        .await
    }

    pub(crate) async fn open_seeded_relay_edge_subscription_view_with_waker(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        read_view: &ReadViewSpec,
        read_view_key: ReadViewKey,
        authority_result_key: AuthorityResultKey,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
            maintained_views::CoveredInputReceiver,
        ),
        Error,
    > {
        let settled_binding_view = Some(authority_result_key.binding_view);
        let (
            subscription,
            maintained,
            terminal_schemas,
            transitions,
            tables,
            initial_received,
            covered_input_sources,
        ) = self
            .open_seeded_maintained_subscription_view_in_authorization_mode(
                shape,
                binding,
                identity,
                DurabilityTier::Edge,
                read_view,
                read_view_key,
                QueryAuthorizationMode::ClientLocal,
                settled_binding_view,
                Some(authority_result_key.clone()),
                PreparedClaimBindingMode::Strict,
                progress_waker,
            )
            .await?;
        // Seeded relay children are ordinary receiver-local maintained views.
        // Construct their retained local state before installing the exact
        // closure so their first reset and later terminal transitions share
        // the same install → drive → drain reducer as a late client opener.
        let mut local = maintained_views::LocalMaintainedViewSubscription {
            subscription,
            _retained_prepared_plan: None,
            maintained,
            terminal_schemas,
            tables,
            result_query: shape.query().clone(),
            result_table: shape.query().table.clone(),
            result_schema_version: shape.schema_version(),
            result_select: shape.query().select.clone(),
            result_set: BTreeSet::new(),
            result_payloads: BTreeMap::new(),
            program_facts: BTreeSet::new(),
            root_occurrence_ids: Vec::new(),
            initial_received,
            covered_input_receiver: maintained_views::CoveredInputReceiver::new(
                covered_input_sources,
                read_view.clone(),
            ),
        };
        let _ = self
            .apply_local_maintained_view_transitions_inner(&mut local, transitions.clone(), false)
            .await?;
        let mut transitions = transitions;
        if !local.covered_input_receiver.is_empty() {
            match self
                .install_opened_local_covered_receiver(
                    &mut local,
                    &authority_result_key,
                    progress_waker,
                )
                .await?
            {
                Some(extra) => {
                    transitions.adds.extend(extra.adds);
                    transitions.removes.extend(extra.removes);
                    transitions
                        .result_payload_adds
                        .extend(extra.result_payload_adds);
                    transitions
                        .result_payload_removes
                        .extend(extra.result_payload_removes);
                    transitions
                        .program_fact_adds
                        .extend(extra.program_fact_adds);
                    transitions
                        .program_fact_removes
                        .extend(extra.program_fact_removes);
                    transitions
                        .terminal_operations
                        .extend(extra.terminal_operations);
                }
                None => local.initial_received = false,
            }
        }
        Ok((
            local.subscription,
            local.maintained,
            local.terminal_schemas,
            transitions,
            local.tables,
            local.initial_received,
            local.covered_input_receiver,
        ))
    }

    /// Hydrate a terminal CommitUnit authorization-support clause. Unlike an
    /// ordinary prepared query, a missing policy claim is a denied proof and
    /// is surfaced to the peer as an empty, settled authorization view.
    #[allow(dead_code)] // Test-only and feature-gated direct view callers keep the no-owner form.
    pub(crate) async fn open_seeded_authorization_support_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
        ),
        Error,
    > {
        self.open_seeded_authorization_support_subscription_view_with_waker(
            shape,
            binding,
            identity,
            tier,
            read_view,
            RegisterShapeOptions {
                tier,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
            None,
        )
        .await
    }

    pub(crate) async fn open_seeded_authorization_support_subscription_view_with_waker(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        read_view_key: ReadViewKey,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
        ),
        Error,
    > {
        self.open_seeded_maintained_subscription_view_in_authorization_mode(
            shape,
            binding,
            identity,
            tier,
            read_view,
            read_view_key,
            QueryAuthorizationMode::TrustedServing,
            None,
            None,
            PreparedClaimBindingMode::FailClosedAuthorizationSupport,
            progress_waker,
        )
        .await
        .map(
            |(subscription, maintained, schemas, transitions, tables, received, _)| {
                (
                    subscription,
                    maintained,
                    schemas,
                    transitions,
                    tables,
                    received,
                )
            },
        )
    }

    async fn open_seeded_maintained_subscription_view_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        read_view_key: ReadViewKey,
        authorization_mode: QueryAuthorizationMode,
        settled_binding_view: Option<BindingViewKey>,
        settled_authority_result_key: Option<AuthorityResultKey>,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<
        (
            MultisinkSubscription,
            MaintainedSubscriptionView,
            MaintainedTerminalSchemas,
            super::maintained_subscription_view::ResultTransitions,
            BTreeMap<String, TableSchema>,
            bool,
            BTreeMap<ProgramSourceId, maintained_views::CoveredInputSource>,
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
        let mut request = self.current_query_program_request_with_prepared_claim_mode(
            &shape,
            &binding,
            tier,
            identity,
            CurrentQueryProgramOutput::MaintainedView,
            read_view,
            settled_binding_view,
            authorization_mode,
            prepared_claim_binding_mode,
            false,
        )?;
        if let Some(authority_result_key) = settled_authority_result_key.as_ref() {
            for source in request.reads.primary.sources.values_mut() {
                if let SourceExpr::SettledBindingView {
                    authority_result_key: selected,
                    ..
                } = source
                {
                    *selected = Some(authority_result_key.clone());
                }
            }
        }
        let mut access_paths = self.current_query_primary_key_access_paths(&shape, &binding)?;
        access_paths.extend(
            self.query_program_access_paths(&request, true)?
                .into_iter()
                .filter(|(_, path)| matches!(path, CurrentAccessPath::Index { .. })),
        );
        // Receiver inputs are allocated from the compiler's source
        // requirements before any source is resolved. This keeps the initial
        // lowering from consulting an authority result just to discover a
        // descriptor.
        let (
            runtime_sources,
            runtime_source_descriptors,
            provisional_local_gates,
            covered_input_sources,
        ) = if authorization_mode == QueryAuthorizationMode::ClientLocal {
            self.allocate_client_receiver_input_sources(&request, true)?
        } else {
            (
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
        };
        let program = if runtime_sources.is_empty() {
            match self
                .compile_query_program_request_with_access_paths(request, access_paths)
                .await
            {
                Ok(program) => program,
                Err(error) => {
                    self.retire_covered_input_sources(&covered_input_sources)
                        .await?;
                    return Err(error);
                }
            }
        } else {
            match self
                .compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs(
                    request,
                    BTreeMap::new(),
                    access_paths,
                    runtime_sources,
                    runtime_source_descriptors,
                    provisional_local_gates,
                )
                .await
            {
                Ok(program) => program,
                Err(error) => {
                    self.retire_covered_input_sources(&covered_input_sources)
                        .await?;
                    return Err(error);
                }
            }
        };
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=opened_program table={} node={:?} mode={authorization_mode:?} identity={identity:?} tier={tier:?} settled_view={settled_binding_view:?} authority_key={settled_authority_result_key:?} sources={:?} descriptors={:?}",
                shape.query().table,
                self.node_uuid,
                program
                    .request
                    .reads
                    .primary
                    .sources
                    .keys()
                    .collect::<Vec<_>>(),
                program.source_descriptors.keys().collect::<Vec<_>>(),
            );
        }
        // Before the first exact authority closure, Local-first preserves its
        // immediate cached-open behavior through descriptor-bound runtime
        // inputs.  This is retired wholesale by the first claimed closure;
        // strict remote sources have no provisional records.
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && let Err(error) = self
                .start_provisional_local_receiver_inputs(&covered_input_sources)
                .await
        {
            self.retire_covered_input_sources(&covered_input_sources)
                .await?;
            return Err(error);
        }
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
        let storage_backed_result_materialization = !program
            .request
            .output
            .facts
            .contains(&ProgramFactKey::VersionWitnesses);
        let inline_content_branch_keys = program
            .request
            .reads
            .primary
            .sources
            .values()
            .filter_map(|source| match source {
                SourceExpr::BranchView {
                    base: Some(BranchViewSourceBase::Snapshot(branch_key, _)),
                    ..
                } => Some(branch_key.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let subscription = match self
            .subscribe_lowered_program(
                program,
                &binding,
                binding_source_shape,
                prepared_claim_binding_mode,
                progress_waker,
            )
            .await
        {
            Ok(subscription) => subscription,
            Err(error) => {
                self.retire_covered_input_sources(&covered_input_sources)
                    .await?;
                return Err(error);
            }
        };
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!("JAZZ_COVERED_INPUT_TRACE stage=receiver_subscription_opened");
        }
        let mut maintained = MaintainedSubscriptionView::default();
        maintained.set_read_view(read_view_key);
        if storage_backed_result_materialization {
            maintained.enable_storage_backed_result_materialization();
        }
        for branch_key in &inline_content_branch_keys {
            maintained.enable_inline_content_branch_key(branch_key);
        }
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        // A cold opening may depend on a peer that can only be advanced after
        // this call returns. Keep Stream A unpublished until the first
        // complete Stream B snapshot arrives; publication drains this same
        // subscription and gates ViewUpdate on `initial_received`.
        let initial_received = match subscription.try_recv() {
            Ok(snapshot) => {
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    eprintln!("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_snapshot");
                }
                let snapshot_transitions = match maintained.apply_multisink_deltas(
                    snapshot,
                    &terminal_schemas,
                    &tables,
                    &self.node_aliases,
                ) {
                    Ok(transitions) => transitions,
                    Err(error) => {
                        self.database.unsubscribe(subscription.id());
                        self.retire_covered_input_sources(&covered_input_sources)
                            .await?;
                        return Err(error);
                    }
                };
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
                // A root collector's opening is its first ordinary terminal
                // transition.  Retain the same root and descendant edits that
                // seeded `maintained`, so the first published/reset snapshot
                // is folded from the receiver-local terminal tree rather than
                // a relational root record with empty nested collections.
                transitions
                    .terminal_operations
                    .extend(snapshot_transitions.terminal_operations);
                true
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => false,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.database.unsubscribe(subscription.id());
                self.retire_covered_input_sources(&covered_input_sources)
                    .await?;
                return Err(Error::InvalidStoredValue(
                    "seeded maintained subscription disconnected",
                ));
            }
        };
        if initial_received {
            loop {
                match subscription.try_recv() {
                    Ok(deltas) => {
                        let delta_transitions = match maintained.apply_multisink_deltas(
                            deltas,
                            &terminal_schemas,
                            &tables,
                            &self.node_aliases,
                        ) {
                            Ok(transitions) => transitions,
                            Err(error) => {
                                self.database.unsubscribe(subscription.id());
                                self.retire_covered_input_sources(&covered_input_sources)
                                    .await?;
                                return Err(error);
                            }
                        };
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
                            .terminal_operations
                            .extend(delta_transitions.terminal_operations);
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        self.database.unsubscribe(subscription.id());
                        self.retire_covered_input_sources(&covered_input_sources)
                            .await?;
                        return Err(Error::InvalidStoredValue(
                            "seeded maintained subscription disconnected",
                        ));
                    }
                }
            }
        }
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_applied");
        }
        Ok((
            subscription,
            maintained,
            terminal_schemas,
            transitions,
            tables,
            initial_received,
            covered_input_sources,
        ))
    }

    async fn bind_shape_snapshot(
        &mut self,
        shape: PreparedShapeId,
        values: &[groove::records::Value],
    ) -> Result<MultisinkDeltas, Error> {
        let subscription = self
            .database
            .bind_shape(shape, values)
            .await
            .map_err(Error::Groove)?;
        let subscription_id = subscription.id();
        let snapshot = subscription.recv().map_err(|_| Error::SubscriptionClosed);
        self.database.unsubscribe(subscription_id);
        snapshot
    }

    /// Bind and immediately retire a one-shot prepared graph. Point reads
    /// compile a physical-row access path, so their graph cannot be safely
    /// shared with later rows; retaining it would leak one shape per advice or
    /// repair request.
    async fn bind_disposable_shape_snapshot(
        &mut self,
        shape: PreparedShapeId,
        values: &[groove::records::Value],
    ) -> Result<RecordDeltas, Error> {
        let subscription = match self
            .database
            .bind_shape(shape, values)
            .await
            .map_err(Error::Groove)
        {
            Ok(subscription) => subscription,
            Err(error) => {
                self.database
                    .retire_prepared_shape(shape)
                    .map_err(Error::Groove)?;
                return Err(error);
            }
        };
        let subscription_id = subscription.id();
        let snapshot = subscription.recv().map_err(|_| Error::SubscriptionClosed);
        self.database.unsubscribe(subscription_id);
        self.database
            .retire_prepared_shape(shape)
            .map_err(Error::Groove)?;
        snapshot.and_then(|deltas| take_required_sink_deltas(deltas, JAZZ_APP_ROWS_SINK))
    }
}

/// Normalize a compiler aggregate record into the one application-row layout.
///
/// Groove aggregate terminals use `__jazz_aggregate_*` names so an aggregate
/// alias can never collide with a grouped source field.  That is an internal
/// graph representation, not a second public record format.  Both a fresh
/// collector reset and later aggregate deltas pass through this conversion so
/// they expose the same synthetic `CurrentRow` descriptor.
fn aggregate_current_row_from_record(
    query: &crate::query::Query,
    row_uuid: RowUuid,
    record: &BorrowedRecord<'_>,
) -> Result<CurrentRow, Error> {
    let mut fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
    let mut values = vec![Value::Uuid(row_uuid.0)];
    let aggregate = query.aggregate.as_ref().ok_or(Error::InvalidStoredValue(
        "aggregate record has no aggregate query shape",
    ))?;

    if let Some(group_by) = &aggregate.group_by {
        let logical = user_column_field(group_by);
        let index = record
            .descriptor()
            .field_index(&logical)
            .or_else(|| record.descriptor().field_index(group_by))
            .ok_or(Error::InvalidStoredValue(
                "aggregate record is missing group output",
            ))?;
        let descriptor = record.descriptor();
        let field = descriptor
            .fields()
            .get(index)
            .ok_or(Error::InvalidStoredValue(
                "aggregate group descriptor is missing",
            ))?;
        fields.push((logical, field.value_type.clone()));
        values.push(record.get_idx(index)?);
    }

    for output in &aggregate.aggregates {
        let app_field = aggregate_output_app_field(&output.alias);
        let internal_field = aggregate_output_field(&output.alias);
        let index = record
            .descriptor()
            .field_index(&app_field)
            .or_else(|| record.descriptor().field_index(&internal_field))
            .ok_or(Error::InvalidStoredValue(
                "aggregate record is missing aggregate output",
            ))?;
        let descriptor = record.descriptor();
        let field = descriptor
            .fields()
            .get(index)
            .ok_or(Error::InvalidStoredValue(
                "aggregate output descriptor is missing",
            ))?;
        fields.push((app_field, field.value_type.clone()));
        values.push(record.get_idx(index)?);
    }
    let descriptor = RecordDescriptor::new(fields);
    let raw = descriptor.create(&values)?;
    Ok(CurrentRow::new(
        query.table.clone(),
        OwnedRecord::new(raw, descriptor),
    ))
}

mod authorization;

impl<S> HistoricalRead<'_, S>
where
    S: OrderedKvStorage,
{
    /// Read a validated query at this handle's historical settle position.
    ///
    /// Partial nodes return [`Error::HistoricalReadRequiresServer`] rather than
    /// answering from incomplete local history. A later protocol slice wires
    /// that error to a server-evaluated one-shot.
    pub async fn read(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        if !self.node.is_history_complete_for(shape, self.position) {
            return Err(Error::HistoricalReadRequiresServer);
        }
        self.node.query_rows_at(shape, binding, self.position).await
    }
}

mod bindings;

use bindings::*;
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
        "$createdBy" | "$updatedBy" => Some(&groove::schema::ColumnType::String),
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
    if column == "id" && !table.columns.iter().any(|candidate| candidate.name == "id") {
        return Some(Value::Uuid(row.row_uuid().0));
    }
    if is_magic_current_column(column) {
        return row.raw_field(column);
    }
    row.cell(table, column)
}

#[cfg(test)]
mod tests;
