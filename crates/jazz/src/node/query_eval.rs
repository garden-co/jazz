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
    SourceGraphPreparer, SourceId, SourceMetadataFields, SourceMetadataRequirement, SourcePath,
    SourceRequest, SourceRequirements, SourceResolutionError, SourceRole, SourceRowShape,
    StorageSchemaSelection, TypedOutputField, UnionInput, ValueSourceColumn, ValueSourceMode,
    VersionIdentityFields, VersionedRowRefSchema, aggregate_output_app_field,
    aggregate_output_column, aggregate_output_field, claim_param_field,
    claim_path_from_param_field, left_field, prepare_and_lower_query_program,
    query_program_source_requests, right_field, route_param_field, user_column_field,
};
use crate::protocol::{
    AuthorizationOperationKey, AuthorizationScopeOperation, AuthorizationSupportScopeKey,
    BindingSource, BindingViewKey, KnownStateCompleteness, KnownStateDeclaration,
    PermissionAdviceAction, ProgramFactEntry, ReadViewKey, ReadViewSourceSpec, ReadViewSpec,
    RegisterShapeOptions, RelationEdgeEntry, ResultMemberEntry, ResultMemberPayloadEntry,
    ResultRowLayer, RowVersionRef, RowVersionRefEntry, ShapeAst, ShapeBody, Subscribe,
    SubscriptionKey, SyntheticReplacementToken,
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

/// A host-settled result membership and the slice offset to apply relative to
/// that membership.  A non-durable client may retain only a bounded authority
/// window, so a narrower read inside that window cannot use its absolute
/// offset against the local cache.
#[derive(Clone, Debug)]
struct ClientSettledBindingView {
    key: BindingViewKey,
    relative_offset: usize,
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

#[cfg(feature = "testing")]
pub(crate) use maintained_views::LocalMaintainedViewSubscriptionFootprint;
use maintained_views::SubscriptionPreparedPlan;
pub(crate) use maintained_views::{
    LocalMaintainedViewSubscription, LocalMaintainedViewSubscriptionUpdate,
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
        // A maintained source may use the same conservative equality access
        // path as a one-shot source.  The source remains an ordinary IVM
        // source: its hydration is selected from the durable index while
        // subsequent table deltas pass through the very same predicate graph.
        // In particular, this is not a separate snapshot/RPC path, so its
        // initial frontier and live continuation share one membership proof.
        let access_paths = self.query_program_access_paths(&request, allow_secondary_indexes)?;
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
        // Linked client shapes carry their read-policy alternatives so an
        // identity-scoped server can maintain the authorized result. System
        // authority is different: it bypasses those alternatives entirely.
        // Drop them before normalization, rather than merely clearing their
        // prepared claim descriptor later. Otherwise normalization lowers a
        // policy `Claim` into `__jazz_claim_*` and the System program still
        // attempts to execute that unbound predicate.
        let system_shape;
        let system_binding;
        let (shape, binding) = if matches!(policy, PolicyContext::System)
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
            system_shape = query.validate_with_schema_version(schema, shape.schema_version())?;
            system_binding = system_shape.bind(
                binding
                    .values()
                    .iter()
                    .filter(|(name, _)| system_shape.params().contains_key(*name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            )?;
            (&system_shape, &system_binding)
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
        let query_schema = self
            .catalogue
            .catalogue_schemas
            .get(&shape.schema_version())
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
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
                &query_schema.schema,
            )?,
            policy,
            input,
            output: current_query_output_request(output, shape.query(), &query_schema.schema),
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

    #[allow(dead_code)]
    pub(crate) async fn query_rows_for_client_read_view(
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
        let Some(snapshot) = self
            .authoritative_reset_snapshot_for_binding_view(shape, binding_view)
            .await?
        else {
            return Ok(Vec::new());
        };
        Ok(snapshot
            .rows
            .into_iter()
            .take(snapshot.root_count)
            .collect())
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
            .filter(|view| {
                view.relative_offset != 0 || {
                    self.query
                        .registered_shapes
                        .get(&view.key.shape_id)
                        .is_some_and(|source| source.query().limit != shape.query().limit)
                }
            })
            .map(|view| {
                let schema = self
                    .catalogue
                    .catalogue_schemas
                    .get(&shape.schema_version())
                    .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?;
                let mut rebased_query = shape.query().clone();
                rebased_query.offset = view.relative_offset;
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
        self.client_settled_binding_view_for_query(shape, binding, tier, read_view)
            .map(|view| view.key)
    }

    fn is_policy_scoped_exact_id_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<bool, Error> {
        let table = self.table_in_schema(&shape.query().table, shape.schema_version())?;
        Ok(table.read_policy.is_some()
            && root_literal_equalities(shape.query(), binding)?.contains_key("id"))
    }

    /// Relay forwarding consumes a selected upstream authority receipt only
    /// where local re-evaluation would change its meaning: windows would
    /// otherwise apply their offset twice, and policy-scoped exact-ID reads
    /// could resurrect a cached row after revocation. A browser worker's
    /// dedicated authority-session identity keeps that selected receipt
    /// separate from ordinary Global coverage; it does not turn every Edge
    /// child into a second projection path.
    pub(crate) fn relay_edge_query_requires_authority_source(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<bool, Error> {
        if shape.query().offset != 0 {
            return Ok(true);
        }
        self.is_policy_scoped_exact_id_query(shape, binding)
    }

    fn client_settled_binding_view_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
    ) -> Option<ClientSettledBindingView> {
        let settled_tier = if tier >= DurabilityTier::Edge {
            tier
        } else if self.authored_commit_durability == DurabilityTier::None
            && shape.query().offset != 0
        {
            // The browser main thread keeps received rows only as a
            // materialized cache. After its worker has supplied an Edge
            // window, a Local read of that same nonzero-offset shape must
            // consume the selected membership rather than offsetting the
            // small cache a second time. Before that receipt, preserve the
            // ordinary local overlay path.
            DurabilityTier::Edge
        } else {
            return None;
        };
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
        if tier >= DurabilityTier::Edge
            && self
                .query
                .local_materialized_window_binding_views
                .contains(&binding_view)
        {
            // A detached browser window is only enough to rebase a Local
            // read over the materialized overlay. It must never stand in for
            // a fresh Edge/Global authorization receipt.
            return None;
        }
        if tier == DurabilityTier::Local
            && !self.query.settled_result_sets.contains_key(&binding_view)
        {
            // A store read may request a narrower window than the one the
            // browser worker already delivered. It is safe to reuse a source
            // only when the source query is identical apart from its root
            // window and fully contains the requested window. The caller
            // applies the requested slice relative to that source membership.
            let target = shape.query();
            let target_end = target
                .limit
                .map(|limit| target.offset.saturating_add(limit));
            let mut target_without_window = target.clone();
            target_without_window.offset = 0;
            target_without_window.limit = None;
            let read_view_key = RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                read_view: read_view.clone(),
                ..RegisterShapeOptions::default()
            }
            .read_view_key();
            return self.query.registered_shapes.values().find_map(|source_shape| {
                if source_shape.schema_version() != shape.schema_version()
                    || source_shape.params() != shape.params()
                {
                    return None;
                }
                let source = source_shape.query();
                let mut source_without_window = source.clone();
                source_without_window.offset = 0;
                source_without_window.limit = None;
                if source_without_window != target_without_window || source.offset > target.offset {
                    return None;
                }
                let source_end = source
                    .limit
                    .map(|limit| source.offset.saturating_add(limit));
                if matches!((source_end, target_end), (Some(source_end), Some(target_end)) if target_end > source_end)
                    || matches!((source_end, target_end), (Some(_), None))
                {
                    return None;
                }
                let key = BindingViewKey::new(
                    source_shape.shape_id(),
                    binding.binding_id(),
                    read_view_key,
                );
                self.query.settled_result_sets.contains_key(&key).then_some(
                    ClientSettledBindingView {
                        key,
                        relative_offset: target.offset - source.offset,
                    },
                )
            });
        }
        Some(ClientSettledBindingView {
            key: binding_view,
            relative_offset: 0,
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

    async fn settled_binding_view_source_rows(
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
        let result_payloads = matches!(rows, SettledBindingRows::ResultMembers)
            .then(|| {
                self.query
                    .settled_program_facts
                    .get(&binding_view)
                    .into_iter()
                    .flatten()
                    .filter_map(|fact| {
                        let ProgramFactEntry::ResultPayload(payload) = fact else {
                            return None;
                        };
                        // Flat joins also retain tuple payloads, but their
                        // canonical source versions still drive source
                        // reconstruction (including schema-lens projection).
                        // Only branch-qualified memberships represent a
                        // public row that can differ from its supplier
                        // version and therefore replace source materialization.
                        payload.member.as_real_row()?.branch_or_prefix.as_ref()?;
                        payload
                            .member
                            .as_row()
                            .map(|(table, row, tx)| ((table.to_string(), row, tx), payload.clone()))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let mut rows = Vec::with_capacity(row_entries.len());
        for ((canonical_table, row_uuid, tx_id), relation_version) in row_entries {
            if let Some(payload) = result_payloads.get(&(canonical_table.clone(), row_uuid, tx_id))
            {
                let output_table = self.table_in_schema(table, read_schema)?.clone();
                let row = self.current_row_from_result_payload(&output_table, payload)?;
                if row.table() == table {
                    rows.push(row);
                }
                continue;
            }
            let version = if let Some(version_ref) = relation_version {
                self.resolve_relation_edge_version(&canonical_table, row_uuid, &version_ref)
                    .await?
            } else {
                let tx_node_alias = self
                    .node_aliases
                    .get(&tx_id.node)
                    .copied()
                    .ok_or(Error::MissingTransaction(tx_id))?;
                let shared = self
                    .query_version_by_alias(
                        &canonical_table,
                        row_uuid,
                        VersionLayer::Content,
                        tx_id.time,
                        tx_node_alias,
                    )
                    .await?;
                if let Some(version) = shared {
                    version
                } else {
                    let versions = self.query_versions_for_tx(tx_id).await?;
                    self.maintained_witness_for_result_member(
                        &versions,
                        read_schema,
                        &canonical_table,
                        row_uuid,
                    )?
                    .cloned()
                    .ok_or(Error::MissingTransaction(tx_id))?
                }
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
        let program = self
            .compile_current_query_program_for_read_view_in_authorization_mode(
                shape,
                binding,
                tier,
                identity,
                CurrentQueryProgramOutput::RelationSnapshot,
                read_view,
                authorization_mode,
            )
            .await?;
        let snapshots = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .await
            .map_err(Error::Groove)?;
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

    async fn tx_query_in_authorization_mode(
        &mut self,
        tx_id: OpenTransactionId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        include_deleted: bool,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let identity = match self.open_tx(tx_id)?.kind {
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
            OpenTransactionKind::Mergeable { .. } => identity,
        };
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
            shape, binding, identity, tier, read_view, None,
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
            QueryAuthorizationMode::TrustedServing,
            None,
            PreparedClaimBindingMode::Strict,
            progress_waker,
        )
        .await
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
        self.open_seeded_relay_edge_subscription_view_with_waker(
            shape, binding, identity, read_view, None,
        )
        .await
    }

    pub(crate) async fn open_seeded_relay_edge_subscription_view_with_waker(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        read_view: &ReadViewSpec,
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
        let settled_binding_view =
            self.relay_edge_subscription_source_binding_view_key(shape, binding, read_view);
        self.open_seeded_maintained_subscription_view_in_authorization_mode(
            shape,
            binding,
            identity,
            DurabilityTier::Edge,
            read_view,
            QueryAuthorizationMode::ClientLocal,
            settled_binding_view,
            PreparedClaimBindingMode::Strict,
            progress_waker,
        )
        .await
    }

    /// The non-public source identity owned by a durable browser relay for a
    /// downstream Edge handoff.  Only the relay publication path asks for
    /// this key; ordinary client reads continue using their normal upstream
    /// Global/Edge coverage identity.
    pub(crate) fn relay_authority_session_binding_view_key(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        read_view: &ReadViewSpec,
    ) -> BindingViewKey {
        BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            RegisterShapeOptions {
                tier: DurabilityTier::Global,
                read_view: read_view.clone(),
                binding_source: BindingSource::RelayAuthoritySession,
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
        )
    }

    /// Select the source receipt used while relaying an Edge view. The
    /// browser-worker topology gives its upstream coverage a dedicated source
    /// identity; older/general relay paths retain their existing Global view.
    pub(crate) fn relay_edge_subscription_source_binding_view_key(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        read_view: &ReadViewSpec,
    ) -> Option<BindingViewKey> {
        if self.is_relay_authority_session_owner() {
            Some(self.relay_authority_session_binding_view_key(shape, binding, read_view))
        } else {
            self.client_settled_binding_view_key_for_query(
                shape,
                binding,
                DurabilityTier::Edge,
                read_view,
            )
        }
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
            shape, binding, identity, tier, read_view, None,
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
            QueryAuthorizationMode::TrustedServing,
            None,
            PreparedClaimBindingMode::FailClosedAuthorizationSupport,
            progress_waker,
        )
        .await
    }

    async fn open_seeded_maintained_subscription_view_in_authorization_mode(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorSubject,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        authorization_mode: QueryAuthorizationMode,
        settled_binding_view: Option<BindingViewKey>,
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
            )
            .await?;
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
        let subscription = self
            .subscribe_lowered_program(
                program,
                &binding,
                binding_source_shape,
                prepared_claim_binding_mode,
                progress_waker,
            )
            .await?;
        // Opening a maintained view is an async ownership boundary: one
        // stream cannot be published while another is still hydrating its
        // content witnesses. The bounded opening poll may leave cold storage
        // pending, but this future remains the real owner until it receives
        // the complete initial multisink snapshot.
        let initial_snapshot = self
            .database
            .next_multisink_subscription(&subscription)
            .await
            .map_err(Error::Groove)?;
        let mut maintained = MaintainedSubscriptionView::default();
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        let snapshot_transitions = maintained.apply_multisink_deltas(
            initial_snapshot,
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
            true,
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
