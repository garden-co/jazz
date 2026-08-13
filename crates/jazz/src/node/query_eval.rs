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
    RowSetProgramInput, RowVisibility, SchemaFamilySelection, SchemaProjection,
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
    ResultMemberEntry, ResultMemberPayloadEntry, RowVersionRef, ShapeAst, ShapeBody, Subscribe,
    SubscriptionKey, SyntheticReplacementToken,
};
use crate::protocol_limits::MAX_KNOWN_STATE_EXACT_REFS;
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, Predicate, Query as JazzQuery,
    QueryError, ShapeId, ValidatedQuery, binding_id_for_values, relation_query_to_query,
};
use crate::schema::{ColumnSchema, JazzSchema, branch_metadata_table_schema};
use crate::tools::{ObjectId, OutputOccurrenceId};

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

#[derive(Clone, Copy)]
struct RelationSnapshotWindow {
    offset: usize,
    limit: Option<usize>,
}

pub(crate) struct LocalMaintainedViewSubscription {
    subscription: MultisinkSubscription,
    _retained_prepared_plan: Option<SubscriptionPreparedPlan>,
    maintained: MaintainedSubscriptionView,
    terminal_schemas: MaintainedTerminalSchemas,
    tables: BTreeMap<String, TableSchema>,
    result_query: JazzQuery,
    result_table: String,
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

#[derive(Default)]
struct LocalMaintainedMaterializationCache {
    tx_versions: BTreeMap<TxId, Vec<VersionRow>>,
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

fn app_row_terminal_fields(output: &ProgramOutputSchemas) -> Result<Vec<String>, Error> {
    app_row_terminal_schema(output).and_then(|app_rows| {
        app_rows
            .descriptor
            .fields()
            .iter()
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "app row terminal field must be named",
                ))
            })
            .collect()
    })
}

fn app_row_terminal_route_eligible_fields(
    output: &ProgramOutputSchemas,
) -> Result<Vec<String>, Error> {
    let app_rows = app_row_terminal_schema(output)?;
    let mut fields = app_row_terminal_fields(output)?;
    fields.extend(app_rows.hidden_fields.iter().cloned());
    Ok(fields)
}

fn app_row_terminal_schema(output: &ProgramOutputSchemas) -> Result<&AppRowSchema, Error> {
    let ProgramOutputSchemas::RowSet(terminals) = output;
    terminals
        .iter()
        .find_map(|terminal| match terminal {
            OutputTerminalSchema::AppRows(rows) => Some(rows),
            OutputTerminalSchema::Fact(_) => None,
        })
        .ok_or(Error::InvalidStoredValue(
            "query program did not emit app row terminal",
        ))
}

fn lowered_terminal_graph(program: &QueryProgram, sink: &str) -> Result<GraphBuilder, Error> {
    program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == sink)
        .map(|terminal| terminal.graph.clone())
        .ok_or_else(|| Error::QueryLowering(format!("query program did not emit sink {sink}")))
}

fn lowered_app_rows_graph(program: &QueryProgram) -> Result<GraphBuilder, Error> {
    lowered_terminal_graph(program, JAZZ_APP_ROWS_SINK)
}

fn lowered_materialization_app_rows_graph(program: &QueryProgram) -> Result<GraphBuilder, Error> {
    let publishes_structured_tree = matches!(
        program.request.output.app_rows.as_ref().map(|rows| &rows.projection),
        Some(PayloadProjection::Tree(tree)) if !tree.paths.is_empty()
    );
    let public_root_owns_membership =
        program
            .request
            .input
            .shape
            .closure_paths
            .iter()
            .any(|path| {
                matches!(
                    path,
                    ClosurePath::ExplicitInclude {
                        root_gate: Some(_),
                        ..
                    }
                )
            });
    if publishes_structured_tree || public_root_owns_membership {
        return lowered_app_rows_graph(program);
    }
    program
        .lowered
        .internal_app_rows_graph
        .clone()
        .map(Ok)
        .unwrap_or_else(|| lowered_app_rows_graph(program))
}

fn lowered_program_sinks(program: &QueryProgram) -> Vec<(String, GraphBuilder)> {
    program
        .lowered
        .terminals
        .iter()
        .map(|terminal| (terminal.sink.clone(), terminal.graph.clone()))
        .collect()
}

fn prepared_params_from_domain(
    parameters: &super::query_engine::ParameterDomain,
) -> Vec<PreparedQueryParam> {
    let mut params = parameters
        .user_params
        .iter()
        .map(|(name, ty)| PreparedQueryParam {
            name: name.clone(),
            ty: ty.clone(),
            source: PreparedQueryParamSource::User,
        })
        .collect::<Vec<_>>();
    params.extend(
        parameters
            .claim_params
            .iter()
            .map(|(name, claim)| PreparedQueryParam {
                name: name.clone(),
                ty: claim.ty.clone(),
                source: PreparedQueryParamSource::Claim(claim.path.clone()),
            }),
    );
    params
}

fn prepared_param_route_field(param: &PreparedQueryParam) -> String {
    match &param.source {
        PreparedQueryParamSource::User => route_param_field(&param.name),
        PreparedQueryParamSource::Claim(_) => param.name.clone(),
    }
}

fn prepared_route_param_names(parameters: &super::query_engine::ParameterDomain) -> Vec<String> {
    prepared_params_from_domain(parameters)
        .iter()
        .map(prepared_param_route_field)
        .filter(|field| parameters.routing_params.contains(field))
        .collect()
}

fn prepared_route_value_indices(
    params: &[PreparedQueryParam],
    route_fields: &[String],
) -> Vec<usize> {
    route_fields
        .iter()
        .map(|route_field| {
            params
                .iter()
                .position(|param| prepared_param_route_field(param) == *route_field)
                .expect("terminal route fields come from the prepared parameter domain")
        })
        .collect()
}

fn terminal_route_fields(route_params: &[String], route_eligible_fields: &[String]) -> Vec<String> {
    let route_eligible_fields = route_eligible_fields.iter().collect::<BTreeSet<_>>();
    route_params
        .iter()
        .filter(|param| route_eligible_fields.contains(param))
        .cloned()
        .collect()
}

fn terminal_public_fields(terminal: &OutputTerminalSchema) -> Result<Vec<String>, Error> {
    match terminal {
        OutputTerminalSchema::AppRows(rows) => descriptor_field_names(&rows.descriptor),
        OutputTerminalSchema::Fact(fact) => fact_public_fields(&fact.schema),
    }
}

fn terminal_route_eligible_fields(terminal: &OutputTerminalSchema) -> Result<Vec<String>, Error> {
    let mut fields = terminal_public_fields(terminal)?;
    if let OutputTerminalSchema::AppRows(rows) = terminal {
        fields.extend(rows.hidden_fields.iter().cloned());
    }
    Ok(fields)
}

fn fact_public_fields(
    schema: &super::query_engine::ProgramFactSchema,
) -> Result<Vec<String>, Error> {
    use super::query_engine::ProgramFactSchema;

    match schema {
        ProgramFactSchema::AuthorizedRows(schema) => {
            let mut fields = vec![schema.row_field.clone()];
            fields.extend(schema.routing_param_fields.iter().cloned());
            Ok(fields)
        }
        ProgramFactSchema::ResultMembership(schema) => {
            let mut fields = vec![schema.table_field.clone(), schema.row_field.clone()];
            fields.extend(
                schema
                    .occurrence_id_fields
                    .iter()
                    .filter(|field| **field != schema.row_field)
                    .cloned(),
            );
            fields.extend(schema.branch_or_prefix_field.clone());
            fields.extend(result_membership_version_fields(&schema.version));
            fields.extend(schema.settle_position_field.clone());
            fields.extend(schema.routing_param_fields.iter().cloned());
            fields.extend(schema.payload_fields.iter().map(|field| field.name.clone()));
            Ok(fields)
        }
        ProgramFactSchema::AggregateResult(schema) => {
            let mut fields = vec![
                schema.synthetic.table_field.clone(),
                schema.synthetic.row_field.clone(),
                schema.synthetic.replacement_field.clone(),
            ];
            fields.extend(
                schema
                    .group_key_fields
                    .iter()
                    .chain(&schema.value_fields)
                    .map(|field| field.name.clone()),
            );
            fields.extend(schema.routing_param_fields.iter().cloned());
            Ok(fields)
        }
        ProgramFactSchema::RelationEdges(schema) => {
            let mut fields = Vec::new();
            fields.extend(versioned_row_ref_fields(&schema.source));
            fields.push(schema.path_field.clone());
            fields.extend(versioned_row_ref_fields(&schema.target));
            fields.push(schema.kind_field.clone());
            fields.extend(schema.depth_field.clone());
            fields.extend(schema.edge_id_field.clone());
            fields.extend(schema.branch_field.clone());
            fields.extend(schema.role_field.clone());
            fields.extend(schema.order_field.clone());
            fields.extend(schema.hole_state_field.clone());
            Ok(fields)
        }
        ProgramFactSchema::VersionWitnesses(schema)
        | ProgramFactSchema::ReplacementWitnesses(schema) => {
            let witness = schema.content.as_ref().or(schema.deletion.as_ref()).ok_or(
                Error::InvalidStoredValue("version witness fact schema has no terminal schema"),
            )?;
            Ok(version_witness_public_fields(&schema.role_field, witness))
        }
        unsupported => Err(Error::InvalidStoredValue(match unsupported {
            ProgramFactSchema::PathCorrelationCoverage(_) => {
                "path correlation coverage facts are not prepared yet"
            }
            ProgramFactSchema::SourceCoverage(_) => "source coverage facts are not prepared yet",
            ProgramFactSchema::ReadFrontierSettled(_) => "read frontier facts are not prepared yet",
            ProgramFactSchema::CompleteTxPayloadCoverage(_) => {
                "complete transaction coverage facts are not prepared yet"
            }
            ProgramFactSchema::ViewCompleteExclusiveCoverage(_) => {
                "view-complete coverage facts are not prepared yet"
            }
            ProgramFactSchema::PolicyDecision(_) => "policy decision facts are not prepared yet",
            ProgramFactSchema::PolicyWitnesses(_) => "policy witness facts are not prepared yet",
            ProgramFactSchema::ContributingMembers(_) => {
                "contributing member facts are not prepared yet"
            }
            ProgramFactSchema::PredicateReads(_) => "predicate-read facts are not prepared yet",
            ProgramFactSchema::PredicateOutputSet(_) => {
                "predicate output set facts are not prepared yet"
            }
            ProgramFactSchema::PointReads(_) => "point-read facts are not prepared yet",
            ProgramFactSchema::LargeValueExtents(_) => {
                "large-value extent facts are not prepared yet"
            }
            ProgramFactSchema::AuthorizedRows(_)
            | ProgramFactSchema::ResultMembership(_)
            | ProgramFactSchema::AggregateResult(_)
            | ProgramFactSchema::RelationEdges(_)
            | ProgramFactSchema::VersionWitnesses(_)
            | ProgramFactSchema::ReplacementWitnesses(_) => unreachable!(),
        })),
    }
}

#[derive(Clone, Debug)]
pub(super) struct PolicyAuthorizationGraph {
    graph: GraphBuilder,
    route_fields: BTreeSet<String>,
}

fn policy_authorization_graph_cache_key(request: &QueryProgramRequest) -> String {
    format!("{request:?}")
}

fn output_routing_fields_for_query_eval(
    output: &super::query_engine::ProgramFactOutput,
) -> BTreeSet<String> {
    match &output.schema {
        super::query_engine::ProgramFactSchema::AuthorizedRows(schema) => {
            schema.routing_param_fields.clone()
        }
        super::query_engine::ProgramFactSchema::ResultMembership(schema) => {
            schema.routing_param_fields.clone()
        }
        super::query_engine::ProgramFactSchema::AggregateResult(schema) => {
            schema.routing_param_fields.clone()
        }
        super::query_engine::ProgramFactSchema::SourceCoverage(schema) => {
            schema.routing_param_fields.clone()
        }
        super::query_engine::ProgramFactSchema::ReadFrontierSettled(schema) => {
            schema.routing_param_fields.clone()
        }
        _ => BTreeSet::new(),
    }
}

fn version_witness_public_fields(
    role_field: &str,
    schema: &super::query_engine::VersionWitnessSchema,
) -> Vec<String> {
    let mut fields = vec![
        role_field.to_owned(),
        schema.identity.table_field.clone(),
        schema.identity.row_field.clone(),
        "content_tx_time".to_owned(),
        "content_tx_node_id".to_owned(),
        schema.identity.tx_time_field.clone(),
        schema.identity.tx_node_field.clone(),
        schema.identity.schema_field.clone(),
        schema.parents_field.clone(),
        schema.authored_columns_field.clone(),
        schema.created_by_field.clone(),
        schema.created_at_field.clone(),
        schema.updated_by_field.clone(),
        schema.updated_at_field.clone(),
        schema.deletion_field.clone(),
    ];
    fields.extend(schema.user_fields.values().cloned());
    fields
}

fn descriptor_field_names(descriptor: &RecordDescriptor) -> Result<Vec<String>, Error> {
    descriptor
        .fields()
        .iter()
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "query-engine terminal field must be named",
            ))
        })
        .collect()
}

fn row_ref_fields(schema: &QueryEngineRowRefSchema) -> Vec<String> {
    vec![
        schema.source_field.clone(),
        schema.table_field.clone(),
        schema.row_field.clone(),
    ]
}

fn versioned_row_ref_fields(schema: &VersionedRowRefSchema) -> Vec<String> {
    let mut fields = row_ref_fields(&schema.row);
    if let Some(version) = &schema.version {
        fields.extend(result_membership_version_fields(version));
    }
    fields
}

fn result_membership_version_fields(schema: &ResultMembershipVersionSchema) -> Vec<String> {
    match schema {
        ResultMembershipVersionSchema::Content(content) => content_version_fields(content),
        ResultMembershipVersionSchema::ContentOrDeletion {
            content,
            deletion,
            deletion_state_field,
        } => {
            let mut fields = content_version_fields(content);
            fields.extend(version_identity_fields(deletion));
            fields.push(deletion_state_field.clone());
            fields
        }
    }
}

fn content_version_fields(schema: &super::query_engine::ContentVersionFields) -> Vec<String> {
    vec![schema.tx_time_field.clone(), schema.tx_node_field.clone()]
}

fn version_identity_fields(schema: &VersionIdentityFields) -> Vec<String> {
    let mut fields = vec![
        schema.table_field.clone(),
        schema.row_field.clone(),
        schema.tx_time_field.clone(),
        schema.tx_node_field.clone(),
        schema.schema_field.clone(),
        schema.layer_field.clone(),
    ];
    fields.extend(schema.batch_id_field.clone());
    fields.extend(schema.branch_or_prefix_field.clone());
    fields.extend(schema.row_digest_field.clone());
    fields
}

pub(crate) struct LocalMaintainedViewSubscriptionUpdate {
    pub(crate) added: Vec<(OutputOccurrenceId, CurrentRow)>,
    pub(crate) removed: Vec<OutputOccurrenceId>,
    pub(crate) added_edges: Vec<(RelationEdge, Option<CurrentRow>)>,
    pub(crate) removed_edges: Vec<RelationEdge>,
    pub(crate) terminal_operations: Vec<groove::ivm::TerminalOperation>,
    pub(crate) terminal_layout: Option<crate::db::TerminalRootLayout>,
}

pub(crate) struct LocalMaintainedRelationSnapshot {
    pub(crate) snapshot: RelationSnapshot,
    pub(crate) root_occurrence_ids: Vec<OutputOccurrenceId>,
}

enum CurrentQueryProgramOutput {
    AppRows,
    PolicyPredicate,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

struct CurrentQuerySourceResolver<'a, S> {
    node: &'a mut NodeState<S>,
    read_view: &'a ReadView<RequestedSourceStage>,
    inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
    access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    /// Query-local enum boundary targets, keyed by logical source.  Defining
    /// a variant target invalidates table inputs, so reuse it across the main
    /// source, access path, and metadata sidecars of one compiled program.
    current_projection_targets: BTreeMap<SourceId, String>,
}

struct CurrentSourceGraph {
    graph: GraphBuilder,
    descriptor: RecordDescriptor,
    metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

#[derive(Clone, Debug)]
enum CurrentAccessPath {
    PrimaryKey(Vec<Value>),
    Index { column: String, prefix: Vec<Value> },
}

impl<S> SourceResolver for CurrentQuerySourceResolver<'_, S>
where
    S: OrderedKvStorage,
{
    fn resolve_source(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let Some(source) = self.read_view.sources.get(&request.source) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        let (projection, graph_tier, history_position, open_tx_overlay, branch_data) = match source
        {
            SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                tier,
            } => (projection, Some(*tier), None, None, None),
            SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Branch(branch_id),
                tier,
            } => (projection, Some(*tier), None, None, Some(*branch_id)),
            SourceExpr::HistoryCut {
                projection,
                data: DataSource::Current,
                position,
            } => (projection, None, Some(*position), None, None),
            SourceExpr::SettledBindingView {
                projection,
                binding_view,
            } => {
                if request.visibility != RowVisibility::Visible {
                    return Err(source_resolution_error(request, SourceGap::Coverage));
                }
                if !matches!(projection.schema_family, SchemaFamilySelection::Current)
                    || !matches!(projection.storage, StorageSchemaSelection::Single(_))
                    || !matches!(projection.lens, LensSelection::Canonical)
                {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::SchemaProjection,
                    ));
                }
                match self.node.settled_binding_view_source_rows(
                    &request.source.table,
                    self.read_view.read_schema,
                    *binding_view,
                ) {
                    Ok(rows) => {
                        let table = self
                            .node
                            .table_in_schema(&request.source.table, self.read_view.read_schema)
                            .map_err(|_| {
                                source_resolution_error(request, SourceGap::SchemaProjection)
                            })?;
                        let schema_version_alias = self
                            .node
                            .ensure_schema_version_alias(self.read_view.read_schema)
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        let (graph, descriptor, metadata) =
                            inline_current_graph_with_source_metadata(
                                &table,
                                rows,
                                schema_version_alias,
                                "settled-binding-view",
                                &request.requirements,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        return Ok(ResolvedSource {
                            table_schema: table,
                            graph,
                            row_shape: SourceRowShape {
                                source: request.source.clone(),
                                descriptor,
                                row_uuid_field: "row_uuid".to_owned(),
                                metadata,
                            },
                            routing_fields: BTreeSet::new(),
                            content_version: None,
                            deletion_register: None,
                        });
                    }
                    Err(Error::MissingTransaction(_)) => {}
                    Err(_) => {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                }
                (projection, Some(DurabilityTier::Global), None, None, None)
            }
            SourceExpr::WithOverlays { input, overlays } => {
                let (projection, tier) = match input.as_ref() {
                    SourceExpr::VisibleCurrent {
                        projection,
                        data: DataSource::Current,
                        tier,
                    } => (projection, Some(*tier)),
                    SourceExpr::SnapshotRef {
                        projection,
                        data: DataSource::Current,
                        snapshot: _,
                    } => (projection, None),
                    _ => {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::TransactionReadOverlay,
                        ));
                    }
                };
                let [OverlayRef::OpenTransaction(tx_id)] = overlays.entries.as_slice() else {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::TransactionReadOverlay,
                    ));
                };
                (projection, tier, None, Some(*tx_id), None)
            }
            _ => {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema_or_branch_metadata(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        // Policy-proof dependencies are raw evidence for the outer policy.
        // Re-applying their own read policy recursively both changes the
        // outer predicate's meaning and can manufacture a proof cycle.
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        if let Some(rows) = self.inline_sources.get(&request.source) {
            if request.visibility != RowVisibility::Visible
                || !request.requirements.metadata.is_empty()
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let graph = inline_current_graph(&table, rows.clone())
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let descriptor = current_row_descriptor(&table);
            return Ok(ResolvedSource {
                table_schema: table,
                graph,
                row_shape: SourceRowShape {
                    source: request.source.clone(),
                    descriptor,
                    row_uuid_field: "row_uuid".to_owned(),
                    metadata: BTreeMap::new(),
                },
                routing_fields: BTreeSet::new(),
                content_version: None,
                deletion_register: None,
            });
        }
        let (graph, descriptor, metadata, routing_fields) = if table.name == "jazz_branches"
            && history_position.is_none()
            && open_tx_overlay.is_none()
        {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::SchemaProjection,
                ));
            }
            let rows = self
                .node
                .branch_metadata_current_rows()
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let base = inline_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let descriptor = current_row_descriptor(&table);
            (base, descriptor, BTreeMap::new(), BTreeSet::new())
        } else if let Some(position) = history_position {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let needs_settle_position = request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::SettlePosition);
            let mut metadata = BTreeMap::new();
            if needs_settle_position {
                metadata.insert(
                    SourceMetadataRequirement::SettlePosition,
                    SourceMetadataFields::SettlePosition {
                        settle_position_field: "settle_position".to_owned(),
                    },
                );
            }
            let descriptor = current_row_descriptor_with_hidden_source_fields(&table, &metadata);
            let base = self.projected_historical_source_graph(request, &table, position)?;
            let base = if needs_settle_position {
                base.project_fields(
                    current_row_fields(&table)
                        .into_iter()
                        .map(ProjectField::named)
                        .chain([ProjectField::null_typed(
                            "settle_position",
                            ValueType::Nullable(Box::new(ValueType::U64)),
                        )])
                        .collect::<Vec<_>>(),
                )
            } else {
                base
            };
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::HistoricalStorageCut,
                        ));
                    }
                    let policy_request = self.node.table_read_policy_authorization_request_at(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        ParamBindingMode::InlineAllReachableSeeds,
                        position,
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &descriptor_field_names(&descriptor).map_err(|_| {
                                source_resolution_error(request, SourceGap::HistoricalStorageCut)
                            })?,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(branch_id) = branch_data {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::SchemaProjection,
                ));
            }
            let branch = self
                .node
                .branches
                .branches
                .get(&branch_id)
                .cloned()
                .ok_or_else(|| source_resolution_error(request, SourceGap::Coverage))?;
            let rows = self
                .node
                .branch_current_rows_for_schema(
                    &request.source.table,
                    &branch,
                    self.read_view.read_schema,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let schema_version_alias = self
                .node
                .ensure_schema_version_alias(self.read_view.read_schema)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let (base, descriptor, metadata) = inline_branch_current_graph(
                &table,
                rows,
                schema_version_alias,
                branch_id,
                &request.requirements,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self.node.branch_table_read_policy_authorization_request(
                        branch_id,
                        &table,
                        *permission_subject,
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    let output_fields = descriptor_field_names(&descriptor)
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(tx_id) = open_tx_overlay {
            let include_deleted = request.visibility == RowVisibility::IncludeDeleted;
            let rows = self
                .node
                .tx_current_rows_in_schema_with_options(
                    tx_id,
                    self.read_view.read_schema,
                    &request.source.table,
                    include_deleted,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?;
            let (graph, descriptor) = if include_deleted {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        let deleted = row.is_deleted();
                        (row, deleted)
                    })
                    .collect();
                (
                    inline_include_deleted_current_graph(&table, rows).map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?,
                    include_deleted_current_row_descriptor(&table),
                )
            } else {
                (
                    inline_current_graph(&table, rows).map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?,
                    current_row_descriptor(&table),
                )
            };
            (graph, descriptor, BTreeMap::new(), BTreeSet::new())
        } else if request.visibility == RowVisibility::Visible
            && self.needs_projected_current_source(&request.source.table)
        {
            if !request.requirements.metadata.is_empty() {
                let source = self.projected_maintained_visible_current_source_graph(
                    request,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                )?;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(source.graph),
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let source = self.projected_visible_current_source_graph(
                    request,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                )?;
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => source.graph,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        if plan.protected_source.table != table.name
                            || plan.role != PolicyDecisionRole::Read
                            || plan.protected_row_field != "row_uuid"
                        {
                            return Err(source_resolution_error(request, SourceGap::Coverage));
                        }
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            ParamBindingMode::InlineAllReachableSeeds,
                            graph_tier.expect("visible current source has a tier"),
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        self.node
                            .policy_filtered_current_source_graph_via_query_engine(
                                policy_request,
                                source.graph,
                                &current_row_fields(&table),
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                (graph, source.descriptor, source.metadata, BTreeSet::new())
            }
        } else if request.visibility == RowVisibility::IncludeDeleted
            && self.needs_projected_current_source(&request.source.table)
        {
            let tier = graph_tier.expect("visible current source has a tier");
            let rows = self
                .node
                .include_deleted_current_rows_for_schema(
                    &request.source.table,
                    self.read_view.read_schema,
                    tier,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let base = inline_include_deleted_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base.clone(),
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base.clone(),
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else if request.visibility == RowVisibility::IncludeDeleted {
            let tier = graph_tier.expect("visible current source has a tier");
            let base = include_deleted_current_graph(&table, tier);
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else {
            let selected_base = self.selected_global_current_source_graph(
                request,
                &table,
                graph_tier.expect("visible current source has a tier"),
            )?;
            if selected_base.is_none() {
                self.node.query_engine_read_metrics.source_full_scans += 1;
            }
            resolved_current_source_graph(
                self.node,
                &table,
                graph_tier.expect("visible current source has a tier"),
                &request.requirements,
                &authorization,
                self.read_view.policy_schema,
                selected_base,
            )
            .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
        };
        let deletion_register = self.deletion_register_source_for_request(
            request,
            &table,
            graph_tier,
            history_position,
            open_tx_overlay,
            branch_data,
        )?;
        let content_version = self.content_version_source_for_request(
            request,
            &table,
            graph_tier,
            history_position,
            open_tx_overlay,
            branch_data,
        )?;
        Ok(ResolvedSource {
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields,
            content_version,
            deletion_register,
        })
    }
}

impl<S> CurrentQuerySourceResolver<'_, S>
where
    S: OrderedKvStorage,
{
    fn current_projection_target(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> Result<String, SourceResolutionError> {
        if let Some(target) = self.current_projection_targets.get(&request.source) {
            return Ok(target.clone());
        }
        let required_fields = self.current_projection_required_fields(request, table);
        let target = self
            .node
            .ensure_physical_current_projection_for_enum_columns(
                self.read_view.read_schema,
                &table.name,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        self.current_projection_targets
            .insert(request.source.clone(), target.clone());
        Ok(target)
    }

    fn current_projection_required_fields(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> BTreeSet<String> {
        // The public query may select a subset of columns, but a maintained
        // version witness feeds `VersionRecord` serialization. A row version
        // is a complete replicated commit unit, so its source must retain all
        // logical user columns before the terminal applies app projection
        // (INV-DATA-18 and INV-SYNC-16). Policy and relation facts have their
        // own typed terminals; they never express omitted VersionRecord cells.
        if request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionWitnesses)
        {
            return table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
        }
        match &request.requirements.app_fields {
            // `All` still goes through the query-local boundary. The durable
            // all-fields projection predates compatibility-sensitive reads and
            // reports a non-total enum remap as an execution error; a read
            // must instead omit precisely that row before its query graph.
            FieldRequirement::All => table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            FieldRequirement::None => BTreeSet::new(),
            FieldRequirement::Fields(fields) => fields.clone(),
        }
    }

    fn selected_global_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        let Some(access_path) = self.access_paths.get(&request.source).cloned() else {
            return Ok(None);
        };
        match access_path {
            CurrentAccessPath::PrimaryKey(prefix) => {
                self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                Ok(Some(selected_visible_current_primary_key_graph(
                    table, tier, prefix,
                )))
            }
            CurrentAccessPath::Index { column, prefix } => {
                if tier != DurabilityTier::Global {
                    return Ok(None);
                }
                let projection_target = self.current_projection_target(request, table)?;
                let rows = self
                    .node
                    .physical_global_current_source_for_index_scan(
                        table,
                        self.read_view.read_schema,
                        &column,
                        &prefix,
                        &projection_target,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                self.node.query_engine_read_metrics.source_index_probes += 1;
                Ok(Some(rows))
            }
        }
    }

    fn deletion_register_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalSeq>,
        open_tx_overlay: Option<OpenBatchId>,
        branch_data: Option<BranchId>,
    ) -> Result<Option<DeletionRegisterSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::DeletionMarkers)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
            || table.name == "jazz_branches"
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if branch_data.is_some() {
            return Err(source_resolution_error(request, SourceGap::BranchOverlay));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(DeletionRegisterSource {
                graph: self.projected_deletion_register_current_source_graph(request, tier)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        let register_table = self
            .node
            .physical_register_table_for_schema(
                self.node.catalogue.current_schema_version_id,
                &table.name,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        Ok(Some(DeletionRegisterSource {
            graph: deletion_register_current_source_graph(&table.name, &register_table, tier),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    fn content_version_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalSeq>,
        open_tx_overlay: Option<OpenBatchId>,
        branch_data: Option<BranchId>,
    ) -> Result<Option<ContentVersionSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionPayloads)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
            || table.name == "jazz_branches"
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if branch_data.is_some() {
            return Err(source_resolution_error(request, SourceGap::BranchOverlay));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(ContentVersionSource {
                graph: self
                    .projected_content_current_source_graph(request, table, tier, false, false)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        Ok(Some(ContentVersionSource {
            graph: content_version_current_source_graph(table, tier, false),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    fn projected_historical_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        position: GlobalSeq,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        if self.can_use_bounded_historical_source(&request.source.table) {
            self.node
                .query_engine_read_metrics
                .source_global_seq_range_scans += 1;
            let rows = self
                .node
                .bounded_historical_current_rows(&request.source.table, position)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            return inline_current_graph(table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut));
        }
        self.node.query_engine_read_metrics.source_full_scans += 1;
        let rows = self
            .node
            .projected_historical_current_rows(
                &request.source.table,
                self.read_view.read_schema,
                position,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
        inline_current_graph(table, rows)
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))
    }

    fn projected_maintained_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        // The schema-aware projection already retains the complete current
        // version witness tuple.  Joining it to the generic physical witness
        // graph would independently decode every enum cell, defeating a
        // title-only old-schema subscription before its narrowed source has a
        // chance to replace unused enum values with typed nulls.
        let projected =
            self.projected_content_current_source_graph(request, table, tier, true, true)?;
        Ok(CurrentSourceGraph {
            graph: projected,
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    fn projected_content_current_source_graph(
        &mut self,
        request: &SourceRequest,
        read_table: &TableSchema,
        tier: DurabilityTier,
        include_global_seq: bool,
        exclude_deleted: bool,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let fields = global_current_storage_fields(read_table, true, include_global_seq);
        // Global current storage has already selected the physical winner.  Apply
        // the ordinary lens-aware projection directly so added-column defaults
        // survive instead of being replaced with physical nulls by the raw
        // winner projection.  Local and Edge reads still need to choose between
        // Global and Ahead candidates before their compatibility boundary.
        if tier == DurabilityTier::Global {
            let projection_target = self.current_projection_target(request, read_table)?;
            let content = match self.access_paths.get(&request.source).cloned() {
                Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                    self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                    self.node
                        .physical_current_source_scan_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target,
                            static_scan_for_prefix(prefix, 1),
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
                Some(CurrentAccessPath::Index { column, prefix }) => {
                    self.node.query_engine_read_metrics.source_index_probes += 1;
                    self.node
                        .physical_global_current_source_for_index_scan(
                            read_table,
                            self.read_view.read_schema,
                            &column,
                            &prefix,
                            &projection_target,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                }
                None => {
                    self.node.query_engine_read_metrics.source_full_scans += 1;
                    self.node
                        .physical_current_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target,
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
            };
            if !exclude_deleted {
                return Ok(content.project(fields));
            }
            let deleted_winners = self
                .projected_deletion_register_current_source_graph(request, tier)?
                .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                .project(["row_uuid"]);
            return Ok(GraphBuilder::anti_join(
                content,
                deleted_winners,
                ["row_uuid"],
                ["row_uuid"],
            ));
        }
        let required_fields = self.current_projection_required_fields(request, read_table);
        let (projection_target, physical_fields) = self
            .node
            .ensure_physical_current_winner_projection(
                self.read_view.read_schema,
                &request.source.table,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let post_winner_fields = self
            .node
            .physical_current_post_winner_projection_fields(
                self.read_view.read_schema,
                &request.source.table,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let raw_global_output = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .and_then(|table_id| {
                self.node
                    .database
                    .table_schema(&physical_global_current_table_name(table_id))
                    .map(|schema| schema.record_schema())
                    .map_err(Error::Groove)
            })
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let access_path = self.access_paths.get(&request.source).cloned();
        let global = match &access_path {
            Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                self.node
                    .physical_current_source_scan_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Global,
                        projection_target.clone(),
                        static_scan_for_prefix(prefix.clone(), 1),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?
            }
            Some(CurrentAccessPath::Index { column, prefix }) if tier == DurabilityTier::Global => {
                // A Global index already selects from the canonical settled
                // winner relation. Project those raw physical rows first, then
                // apply the compatibility boundary below.
                self.node.query_engine_read_metrics.source_index_probes += 1;
                self.node
                    .physical_global_current_source_for_index_scan_with_output(
                        read_table,
                        self.read_view.read_schema,
                        column,
                        prefix,
                        &projection_target,
                        raw_global_output.clone(),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            }
            _ => {
                self.node.query_engine_read_metrics.source_full_scans += 1;
                self.node
                    .physical_current_source_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Global,
                        projection_target.clone(),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?
            }
        };
        let content = if tier == DurabilityTier::Global {
            global
        } else {
            let global = global.project(physical_fields.clone());
            let ahead = match &access_path {
                Some(CurrentAccessPath::PrimaryKey(prefix)) => self
                    .node
                    .physical_current_source_scan_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Ahead,
                        projection_target.clone(),
                        static_scan_for_prefix(prefix.clone(), 3),
                    ),
                _ => self
                    .node
                    .physical_current_source_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Ahead,
                        projection_target,
                    ),
            }
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let ahead = if tier == DurabilityTier::Edge {
                edge_visible_ahead_current_source_graph(ahead, physical_fields.clone())
            } else {
                ahead.project(physical_fields.clone())
            };
            GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(physical_fields)
        };
        let content = content.project_fields(post_winner_fields);
        if !exclude_deleted {
            return Ok(content.project(fields));
        }
        let deleted_winners = self
            .projected_deletion_register_current_source_graph(request, tier)?
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]);
        Ok(GraphBuilder::anti_join(
            content,
            deleted_winners,
            ["row_uuid"],
            ["row_uuid"],
        ))
    }

    fn projected_deletion_register_current_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let table_id = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let fields = register_storage_fields_for_query_engine("");
        let global = GraphBuilder::table(physical_register_global_current_table_name(table_id));
        if tier == DurabilityTier::Global {
            return Ok(global);
        }
        let global = global.project_fields(fields.clone());
        let ahead = GraphBuilder::table(physical_register_ahead_current_table_name(table_id));
        let ahead = if tier == DurabilityTier::Edge {
            edge_visible_ahead_current_source_graph(ahead, register_storage_field_names())
        } else {
            ahead.project_fields(fields.clone())
        };
        Ok(GraphBuilder::arg_max_by(
            GraphBuilder::union([global, ahead]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project_fields(fields))
    }

    fn projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        Ok(CurrentSourceGraph {
            // Project heterogeneous physical rows at the source boundary so the
            // rest of the query graph retains the requested logical descriptor.
            graph: self
                .projected_content_current_source_graph(request, table, tier, false, true)?
                .project_fields(storage_to_canonical_current_source_fields(
                    table, true, false,
                )),
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    fn can_use_bounded_historical_source(&self, table: &str) -> bool {
        if self.read_view.read_schema != self.node.catalogue.current_schema_version_id {
            return false;
        }
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }

    fn needs_projected_current_source(&mut self, table: &str) -> bool {
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }
}

fn deletion_register_current_source_graph(
    table: &str,
    physical_register_table: &str,
    tier: DurabilityTier,
) -> GraphBuilder {
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table))
            .project_fields(register_storage_fields_for_query_engine(""));
    }
    let current_keys = deletion_register_current_keys_graph(table, tier);
    GraphBuilder::join(
        GraphBuilder::table(physical_register_table),
        current_keys,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(register_storage_fields_for_query_engine("left."))
}

fn edge_visible_ahead_current_source_graph(
    source: GraphBuilder,
    fields: Vec<String>,
) -> GraphBuilder {
    GraphBuilder::join(
        source.project(fields.clone()),
        GraphBuilder::table("jazz_transactions")
            .filter(
                PredicateExpr::And(vec![
                    PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
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
}

fn content_version_current_source_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    include_global_seq: bool,
) -> GraphBuilder {
    let mut fields = maintained_view_history_storage_field_names(table);
    if include_global_seq {
        fields.push("global_seq".to_owned());
    }
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(global_current_table_name(&table.name)).project(fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone()),
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
                .iter()
                .cloned()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone())
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(global_current_table_name(&table.name)).project(fields.clone()),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(fields)
}

fn deletion_register_current_keys_graph(table: &str, tier: DurabilityTier) -> GraphBuilder {
    let key_fields = ["row_uuid", "tx_time", "tx_node_id"];
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table)).project(key_fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields),
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
            key_fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields)
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(register_global_current_table_name(table)).project(key_fields),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(key_fields)
}

fn selected_visible_current_primary_key_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    prefix: Vec<Value>,
) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
    content_fields.extend(user_fields.iter().cloned());
    content_fields.extend([
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
    ]);
    let content_scan = static_scan_for_prefix(prefix.clone(), 1);
    let deletion_scan = static_scan_for_prefix(prefix, 1);
    let edge_visible_ahead = |table_name: String, fields: Vec<String>, scan: StaticScanSpec| {
        GraphBuilder::join(
            GraphBuilder::table_scan(table_name, scan).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::EnumTag(2)),
                            PredicateExpr::eq("durability", Value::EnumTag(3)),
                        ])
                        .canonicalize(),
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
    let (content_current, deleted_winners) = if tier == DurabilityTier::Global {
        (
            GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                .project(content_fields.clone()),
            GraphBuilder::table_scan(
                register_global_current_table_name(&table.name),
                deletion_scan,
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_fields.clone(),
                content_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(ahead_current_table_name(&table.name), content_scan.clone())
                .project(content_fields.clone())
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
        let ahead_deleted = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
                deletion_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(
                register_ahead_current_table_name(&table.name),
                deletion_scan.clone(),
            )
            .project(deletion_fields.clone())
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                        .project(content_fields.clone()),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(content_fields.clone()),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(
                        register_global_current_table_name(&table.name),
                        deletion_scan,
                    )
                    .project(deletion_fields),
                    ahead_deleted,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    };
    GraphBuilder::anti_join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project(content_fields)
}

fn register_storage_fields_for_query_engine(prefix: &str) -> Vec<ProjectField> {
    register_storage_field_names()
        .into_iter()
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
        .collect()
}

fn register_storage_field_names() -> Vec<String> {
    [
        "row_uuid",
        "tx_time",
        "tx_node_id",
        "schema_version",
        "parents",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
        "_deletion",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn source_resolution_error(request: &SourceRequest, gap: SourceGap) -> SourceResolutionError {
    SourceResolutionError {
        request: Box::new(request.clone()),
        gap,
    }
}

fn source_resolution_error_from_policy_proof(
    request: &SourceRequest,
    error: Error,
) -> SourceResolutionError {
    match error {
        Error::PolicyProofCycle { table, depth } => {
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        Error::QueryCapability(message) => {
            let Some((table, depth)) = policy_proof_cycle_from_capability(&message) else {
                return source_resolution_error(request, SourceGap::Coverage);
            };
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        _ => source_resolution_error(request, SourceGap::Coverage),
    }
}

fn policy_proof_cycle_from_capability(message: &str) -> Option<(String, usize)> {
    let (_, suffix) = message.rsplit_once("PolicyProofCycle { table: \"")?;
    let (table, suffix) = suffix.split_once('"')?;
    let (_, suffix) = suffix.split_once(", depth: ")?;
    let depth = suffix
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((table.to_owned(), depth))
}

fn capability_trace_enabled() -> bool {
    std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some()
        || std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE").is_some()
}

fn trace_capability_compile(
    node_uuid: NodeUuid,
    node_alias: Option<NodeAlias>,
    request: &QueryProgramRequest,
    result: Result<&QueryProgram, &CapabilityReport>,
) {
    let Some(path) = std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE") else {
        if std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some() {
            eprintln!(
                "JAZZ_CAPABILITY_TRACE set without JAZZ_CAPABILITY_TRACE_FILE; capability trace skipped"
            );
        }
        return;
    };
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => {
            eprintln!(
                "failed to open JAZZ_CAPABILITY_TRACE_FILE {:?}: {err}",
                path
            );
            return;
        }
    };
    let event = match result {
        Ok(_) => "compile_success",
        Err(_) => "compile_failure",
    };
    let _ = writeln!(
        file,
        "\n=== JAZZ_CAPABILITY_TRACE {event} pid={} node_uuid={:?} node_alias={:?} ===",
        std::process::id(),
        node_uuid,
        node_alias
    );
    let _ = writeln!(file, "request:\n{request:#?}");
    match result {
        Ok(program) => {
            let _ = writeln!(file, "explain:\n{:#?}", program.explain);
            let _ = writeln!(file, "output:\n{:#?}", program.lowered.output);
        }
        Err(report) => {
            let _ = writeln!(file, "report:\n{report:#?}");
        }
    }
    let _ = writeln!(
        file,
        "backtrace:\n{}",
        std::backtrace::Backtrace::force_capture()
    );
}

fn resolved_current_source_graph<S>(
    node: &mut NodeState<S>,
    table: &TableSchema,
    tier: DurabilityTier,
    requirements: &SourceRequirements,
    authorization: &SourceAuthorizationRequest,
    policy_schema: SchemaVersionId,
    selected_base: Option<GraphBuilder>,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
        BTreeSet<String>,
    ),
    Error,
>
where
    S: OrderedKvStorage,
{
    let mut fields = current_row_fields(table)
        .into_iter()
        .map(ProjectField::named)
        .collect::<Vec<_>>();
    let mut metadata = BTreeMap::new();
    let needs_version_witnesses = requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
        || requirements
            .metadata
            .iter()
            .any(|requirement| matches!(requirement, SourceMetadataRequirement::Provenance(_)));
    let needs_settle_position = requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition);

    if needs_version_witnesses {
        fields.extend([
            ProjectField::literal("table", Value::String(table.name.clone())),
            ProjectField::literal("layer", Value::String("content".to_owned())),
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
            ProjectField::renamed("$createdBy", "created_by"),
            ProjectField::renamed("$createdAt", "created_at"),
            ProjectField::renamed("$updatedBy", "updated_by"),
            ProjectField::renamed("$updatedAt", "updated_at"),
        ]);
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
    if needs_settle_position {
        fields.push(ProjectField::named("settle_position"));
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        fields.push(ProjectField::literal(
            "coverage",
            Value::String("visible-current".to_owned()),
        ));
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    // Every resolver branch normalizes storage names before it
                    // exposes this source. Metadata therefore names the
                    // canonical field on the resolved graph, never the
                    // physical storage column used below that boundary.
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }

    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let (base, routing_fields) = match authorization {
        SourceAuthorizationRequest::System => {
            let graph = if let Some(selected_base) = selected_base.clone() {
                selected_base.project_fields(storage_to_canonical_current_source_fields(
                    table,
                    needs_version_witnesses,
                    needs_settle_position,
                ))
            } else if needs_version_witnesses {
                node.maintained_view_content_current_with_version(table, tier)?
                    .project_fields(storage_to_canonical_current_source_fields(
                        table,
                        true,
                        needs_settle_position,
                    ))
            } else {
                visible_current_graph(table, tier)
                    .project_fields(canonical_current_source_fields(table, false))
            };
            (graph, BTreeSet::new())
        }
        SourceAuthorizationRequest::PolicyFiltered {
            permission_subject,
            plan,
        }
        | SourceAuthorizationRequest::PolicyProof {
            permission_subject,
            plan,
        } => {
            if plan.protected_source.table != table.name
                || plan.role != PolicyDecisionRole::Read
                || plan.protected_row_field != "row_uuid"
            {
                return Err(Error::QueryCapability(
                    "policy authorization plan does not match resolved source".to_owned(),
                ));
            }
            let binding_source_shape = plan.binding_source_shape.clone();
            let binding_user_params = plan.binding_user_params.clone();
            let binding_claim_params = plan.binding_claim_params.clone();
            let param_binding_mode = if binding_source_shape.is_some() {
                ParamBindingMode::RetainAllParams
            } else {
                ParamBindingMode::InlineAllReachableSeeds
            };
            let policy_request = node.table_read_policy_authorization_request(
                policy_schema,
                &table.name,
                *permission_subject,
                param_binding_mode,
                tier,
                binding_source_shape.clone(),
                binding_user_params.clone(),
                binding_claim_params,
            );
            let output_fields = global_current_storage_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            let base = match selected_base {
                Some(selected_base) => selected_base,
                None => node.maintained_view_content_current_with_version(table, tier)?,
            };
            let storage_graph = node.policy_filtered_current_source_graph_via_query_engine(
                policy_request,
                base.clone(),
                &output_fields,
            )?;
            let mut canonical_fields = storage_to_canonical_current_source_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            canonical_fields.extend(
                storage_graph
                    .route_fields
                    .iter()
                    .map(|field| ProjectField::named(field.clone())),
            );
            (
                storage_graph.graph.project_fields(canonical_fields),
                storage_graph.route_fields,
            )
        }
    };
    fields.extend(
        routing_fields
            .iter()
            .map(|field| ProjectField::named(field.clone())),
    );
    let graph = if metadata.is_empty() {
        base
    } else {
        base.project_fields(fields)
    };
    Ok((graph, descriptor, metadata, routing_fields))
}

fn canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(
            table
                .columns
                .iter()
                .map(|column| ProjectField::named(user_column_field(&column.name))),
        )
        .chain([
            ProjectField::named("$createdBy"),
            ProjectField::named("$createdAt"),
            ProjectField::named("$updatedBy"),
            ProjectField::named("$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    fields
}

fn source_provenance_field(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

fn storage_to_canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(
            table
                .columns
                .iter()
                .map(|column| ProjectField::named(user_column_field(&column.name))),
        )
        .chain([
            ProjectField::renamed("created_by", "$createdBy"),
            ProjectField::renamed("created_at", "$createdAt"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    if include_settle_position {
        fields.push(ProjectField::renamed("global_seq", "settle_position"));
    }
    fields
}

fn current_row_descriptor_with_hidden_source_fields(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
) -> RecordDescriptor {
    let mut fields = current_row_descriptor_fields(table);
    if metadata.contains_key(&SourceMetadataRequirement::VersionWitnesses) {
        fields.extend([
            ("table".to_owned(), ValueType::String),
            ("layer".to_owned(), ValueType::String),
            ("schema_version".to_owned(), ValueType::U64),
            (
                "parents".to_owned(),
                ValueType::Array(Box::new(ValueType::Tuple(vec![
                    ValueType::U64,
                    ValueType::Uuid,
                ]))),
            ),
            ("created_by".to_owned(), ValueType::Uuid),
            ("created_at".to_owned(), ValueType::U64),
            ("updated_by".to_owned(), ValueType::Uuid),
            ("updated_at".to_owned(), ValueType::U64),
            (
                "authored_columns".to_owned(),
                ValueType::Nullable(Box::new(ValueType::Bytes)),
            ),
        ]);
        if let Some(SourceMetadataFields::VersionWitnesses {
            branch_or_prefix_field: Some(field),
            ..
        }) = metadata.get(&SourceMetadataRequirement::VersionWitnesses)
        {
            fields.push((field.clone(), ValueType::Uuid));
        }
    }
    if metadata.contains_key(&SourceMetadataRequirement::Coverage) {
        fields.push(("coverage".to_owned(), ValueType::String));
    }
    if metadata.contains_key(&SourceMetadataRequirement::SettlePosition) {
        fields.push((
            "settle_position".to_owned(),
            ValueType::Nullable(Box::new(ValueType::U64)),
        ));
    }
    RecordDescriptor::new(fields)
}

fn current_row_descriptor_fields(table: &TableSchema) -> Vec<(String, ValueType)> {
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
        .collect()
}

fn root_source_id(table: &str) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![SourceRole::Root],
        },
    }
}

fn nested_join_source_id(join: &JoinVia, path: &str) -> SourceId {
    SourceId {
        table: join.table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(path.to_owned())],
        },
    }
}

fn join_lookup_source_id(lookup: &crate::query::JoinSourceLookup, path: &str) -> SourceId {
    SourceId {
        table: lookup.table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!("{path}:source_lookup"))],
        },
    }
}

fn current_query_read_set(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    settled_binding_view: Option<BindingViewKey>,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(read_schema),
        lens: LensSelection::Canonical,
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                if let Some(binding_view) = settled_binding_view {
                    SourceExpr::SettledBindingView {
                        projection: projection.clone(),
                        binding_view,
                    }
                } else {
                    SourceExpr::VisibleCurrent {
                        projection: projection.clone(),
                        data: DataSource::Current,
                        tier,
                    }
                },
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        // Auxiliary closure sources are not result members of the settled binding
        // view. Keep the result/root source pinned to the settled view, but read
        // implicit reference targets from current storage so serving can resolve
        // their rows instead of treating missing result-set entries as coverage
        // gaps.
        sources.insert(
            source.clone(),
            SourceExpr::VisibleCurrent {
                projection: projection.clone(),
                data: DataSource::Current,
                tier,
            },
        );
    }
    QueryReadSet::primary(ReadView {
        read_schema,
        policy_schema,
        sources,
    })
}

fn historical_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    position: GlobalSeq,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                SourceExpr::HistoryCut {
                    projection: projection.clone(),
                    data: DataSource::Current,
                    position,
                },
            )),
            _ => None,
        })
        .collect();
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

fn tx_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
    snapshot: Snapshot,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                SourceExpr::WithOverlays {
                    input: Box::new(SourceExpr::SnapshotRef {
                        projection: projection.clone(),
                        data: DataSource::Current,
                        snapshot: snapshot.clone(),
                    }),
                    overlays: OverlayStack {
                        entries: vec![OverlayRef::OpenTransaction(tx_id)],
                    },
                },
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        sources.insert(
            source.clone(),
            SourceExpr::WithOverlays {
                input: Box::new(SourceExpr::SnapshotRef {
                    projection: projection.clone(),
                    data: DataSource::Current,
                    snapshot: snapshot.clone(),
                }),
                overlays: OverlayStack {
                    entries: vec![OverlayRef::OpenTransaction(tx_id)],
                },
            },
        );
    }
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

fn branch_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tier: DurabilityTier,
    branch_id: BranchId,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                SourceExpr::VisibleCurrent {
                    projection: projection.clone(),
                    data: DataSource::Branch(branch_id),
                    tier,
                },
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        sources.insert(
            source.clone(),
            SourceExpr::VisibleCurrent {
                projection: projection.clone(),
                data: DataSource::Branch(branch_id),
                tier,
            },
        );
    }
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

fn query_read_set_for_read_view(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
    settled_binding_view: Option<BindingViewKey>,
    aggregate_query: bool,
) -> Result<RequestedReadSet, Error> {
    // A settled binding view stores aggregate output as synthetic result
    // members, not source-table rows. Re-feeding it through the source graph
    // would turn an aggregate replacement into an empty source and retract
    // the public row. Its authoritative result is materialized directly by
    // the subscription facade instead.
    let settled_binding_view = (!aggregate_query).then_some(settled_binding_view).flatten();
    if settled_binding_view.is_some() {
        if !read_view.is_default() {
            return Err(Error::QueryCapability(
                "settled binding view sources do not support non-default read_view yet".to_owned(),
            ));
        }
        return Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            settled_binding_view,
        ));
    }
    match &read_view.source {
        ReadViewSourceSpec::Current => Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            None,
        )),
        ReadViewSourceSpec::Branch { branch }
            if read_view.schema == Default::default() && read_view.overlays.is_empty() =>
        {
            Ok(branch_query_read_set(
                shape,
                read_schema,
                tier,
                BranchId(*branch),
            ))
        }
        ReadViewSourceSpec::MergedBranches { .. } => Err(Error::QueryCapability(
            "merged branch read_view requires unified branch merge source lowering".to_owned(),
        )),
        ReadViewSourceSpec::Snapshot { .. } => Err(Error::QueryCapability(
            "snapshot read_view requires unified snapshot source lowering".to_owned(),
        )),
        ReadViewSourceSpec::Branch { .. } => Err(Error::QueryCapability(
            "branch read_view does not support schema lenses or overlays yet".to_owned(),
        )),
    }
}

fn current_query_output_request(
    output: CurrentQueryProgramOutput,
    query: &JazzQuery,
) -> RowSetOutputRequest {
    let facts = match output {
        CurrentQueryProgramOutput::AppRows | CurrentQueryProgramOutput::PolicyPredicate => {
            BTreeSet::new()
        }
        CurrentQueryProgramOutput::AuthorizedRows => {
            BTreeSet::from([ProgramFactKey::AuthorizedRows])
        }
        CurrentQueryProgramOutput::RelationSnapshot
            if !query.array_subqueries.is_empty() || !query.reachable.is_empty() =>
        {
            BTreeSet::from([
                ProgramFactKey::RelationEdges,
                ProgramFactKey::PathCorrelationCoverage,
            ])
        }
        CurrentQueryProgramOutput::RelationSnapshot => BTreeSet::new(),
        CurrentQueryProgramOutput::MaintainedView if !query.array_subqueries.is_empty() => {
            BTreeSet::from([
                ProgramFactKey::ResultMembership,
                ProgramFactKey::VersionWitnesses,
                ProgramFactKey::ReplacementWitnesses,
                ProgramFactKey::RelationEdges,
            ])
        }
        CurrentQueryProgramOutput::MaintainedView => BTreeSet::from([
            ProgramFactKey::ResultMembership,
            ProgramFactKey::VersionWitnesses,
            ProgramFactKey::ReplacementWitnesses,
        ]),
    };
    RowSetOutputRequest {
        app_rows: (matches!(
            output,
            CurrentQueryProgramOutput::AppRows
                | CurrentQueryProgramOutput::PolicyPredicate
                | CurrentQueryProgramOutput::RelationSnapshot
                | CurrentQueryProgramOutput::MaintainedView
        ))
        .then(|| AppRowOutputRequest {
            public_terminal: !matches!(output, CurrentQueryProgramOutput::PolicyPredicate),
            projection: app_row_payload_projection(
                query,
                matches!(output, CurrentQueryProgramOutput::MaintainedView)
                    || !query.array_subqueries.is_empty(),
            ),
            large_values: Vec::new(),
        }),
        facts,
    }
}

fn app_row_payload_projection(query: &JazzQuery, collect_relations: bool) -> PayloadProjection {
    let paths = if collect_relations {
        app_row_path_projections(&root_source_id(&query.table), &query.array_subqueries, &[])
    } else {
        Vec::new()
    };
    if query.select.is_none() && paths.is_empty() {
        return PayloadProjection::ShapeDefault;
    }
    let fields = query
        .select
        .as_ref()
        .map(|select| {
            let mut fields = select
                .iter()
                .filter(|field| field.as_str() != "id")
                .cloned()
                .collect::<BTreeSet<_>>();
            for include in &query.includes {
                if let Some(root_field) = include.path.split('.').next() {
                    fields.insert(root_field.to_owned());
                }
            }
            FieldProjection::Fields(fields)
        })
        .unwrap_or(FieldProjection::All);
    PayloadProjection::Tree(AppProjectionTree { fields, paths })
}

fn app_row_path_projections(
    owner: &SourceId,
    subqueries: &[ArraySubquery],
    path: &[usize],
) -> Vec<super::query_engine::AppPathProjection> {
    subqueries
        .iter()
        .enumerate()
        .map(|(index, subquery)| {
            let mut child_path = path.to_vec();
            child_path.push(index);
            let child = correlated_child_source_id(owner, subquery, &child_path);
            let fields = subquery
                .select
                .as_ref()
                .map(|select| {
                    FieldProjection::Fields(
                        select
                            .iter()
                            .filter(|field| field.as_str() != "id")
                            .cloned()
                            .collect(),
                    )
                })
                .unwrap_or(FieldProjection::All);
            super::query_engine::AppPathProjection {
                path: ProgramPathId {
                    owner: owner.clone(),
                    child: child.clone(),
                },
                field: subquery.column_name.clone(),
                cardinality: PathCardinality::Many,
                fields,
                children: app_row_path_projections(&child, &subquery.nested_arrays, &child_path),
                hole_policy: PathHolePolicy::KeepParentWithHoles,
                large_values: Vec::new(),
            }
        })
        .collect()
}

fn required_field_idx(descriptor: &RecordDescriptor, field: &str) -> Result<usize, Error> {
    descriptor.field_index(field).ok_or_else(|| {
        Error::QueryLowering(format!(
            "query-engine relation snapshot sink did not emit field '{field}'"
        ))
    })
}

fn normalize_predicates(
    schema: &JazzSchema,
    source: &SourceId,
    predicates: &[Predicate],
) -> Result<NormalizedPredicateExpr, Error> {
    match predicates {
        [] => Ok(NormalizedPredicateExpr::True),
        [predicate] => normalize_predicate(schema, source, predicate),
        _ => predicates
            .iter()
            .map(|predicate| normalize_predicate(schema, source, predicate))
            .collect::<Result<Vec<_>, Error>>()
            .map(NormalizedPredicateExpr::And),
    }
}

fn root_literal_equalities(
    query: &JazzQuery,
    binding: &Binding,
) -> Result<BTreeMap<String, Value>, Error> {
    literal_equalities_for_filters(&query.filters, binding)
}

fn literal_equalities_for_filters(
    filters: &[Predicate],
    binding: &Binding,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut equalities = BTreeMap::new();
    for predicate in filters {
        collect_root_literal_equalities(predicate, binding, &mut equalities)?;
    }
    Ok(equalities)
}

fn collect_root_literal_equalities(
    predicate: &Predicate,
    binding: &Binding,
    equalities: &mut BTreeMap<String, Value>,
) -> Result<(), Error> {
    match predicate {
        Predicate::All(predicates) => {
            for predicate in predicates {
                collect_root_literal_equalities(predicate, binding, equalities)?;
            }
        }
        Predicate::Eq(left, right) => {
            if let Some((field, value)) = root_equality_literal(left, right, binding)? {
                equalities.entry(field).or_insert(value);
            } else if let Some((field, value)) = root_equality_literal(right, left, binding)? {
                equalities.entry(field).or_insert(value);
            }
        }
        Predicate::Any(_)
        | Predicate::Not(_)
        | Predicate::Ne(_, _)
        | Predicate::In(_, _)
        | Predicate::Gt(_, _)
        | Predicate::Gte(_, _)
        | Predicate::Lt(_, _)
        | Predicate::Lte(_, _)
        | Predicate::Contains(_, _)
        | Predicate::EnumMatch { .. }
        | Predicate::IsNull(_) => {}
    }
    Ok(())
}

fn root_equality_literal(
    field: &Operand,
    value: &Operand,
    binding: &Binding,
) -> Result<Option<(String, Value)>, Error> {
    let Operand::Column(column) = field else {
        return Ok(None);
    };
    let value = match value {
        Operand::Literal(value) => value.clone(),
        Operand::Param(name) => binding
            .values()
            .get(name)
            .cloned()
            .ok_or_else(|| QueryError::MissingParam(name.clone()))?,
        Operand::Column(_) | Operand::Claim(_) => return Ok(None),
    };
    Ok(Some((column.clone(), value)))
}

fn select_current_access_path(
    table: &TableSchema,
    equalities: &BTreeMap<String, Value>,
) -> Option<CurrentAccessPath> {
    if let Some(value) = equalities.get("id").cloned() {
        return Some(CurrentAccessPath::PrimaryKey(vec![value]));
    }
    for column in table.global_current_indexed_columns() {
        if let Some(value) = equalities.get(&column).cloned() {
            return Some(CurrentAccessPath::Index {
                column,
                prefix: vec![Value::Nullable(Some(Box::new(value)))],
            });
        }
    }
    None
}

fn static_scan_for_prefix(prefix: Vec<Value>, full_key_len: usize) -> StaticScanSpec {
    let values = prefix
        .into_iter()
        .map(LiteralValue::from)
        .collect::<Vec<_>>();
    if values.len() == full_key_len {
        StaticScanSpec::Point(values)
    } else {
        StaticScanSpec::Prefix(values)
    }
}

fn normalize_predicate(
    schema: &JazzSchema,
    source: &SourceId,
    predicate: &Predicate,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => NormalizedPredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| normalize_predicate(schema, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Any(predicates) => NormalizedPredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| normalize_predicate(schema, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Not(predicate) => {
            NormalizedPredicateExpr::Not(Box::new(normalize_predicate(schema, source, predicate)?))
        }
        Predicate::Eq(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Eq, right)?
        }
        Predicate::Ne(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Ne, right)?
        }
        Predicate::Gt(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Gt, right)?
        }
        Predicate::Gte(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Gte, right)?
        }
        Predicate::Lt(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Lt, right)?
        }
        Predicate::Lte(left, right) => {
            normalize_compare(schema, source, left, NormalizedComparisonOp::Lte, right)?
        }
        Predicate::In(value, options) => NormalizedPredicateExpr::In {
            value: normalize_operand(source, value)?,
            options: options
                .iter()
                .map(|operand| {
                    normalize_operand_with_target_type(
                        source,
                        operand,
                        operand_column_type(schema, source, value)?.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?,
        },
        Predicate::Contains(value, needle) => NormalizedPredicateExpr::ArrayContains {
            value: normalize_operand(source, value)?,
            needle: normalize_operand_with_target_type(
                source,
                needle,
                contains_needle_type(schema, source, value)?.as_ref(),
            )?,
        },
        Predicate::IsNull(value) => {
            NormalizedPredicateExpr::IsNull(normalize_operand(source, value)?)
        }
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            let column_type =
                operand_column_type(schema, source, &Operand::Column(column.clone()))?.ok_or_else(
                    || Error::QueryLowering("enum match column has no type".to_owned()),
                )?;
            let column_type = match column_type {
                ColumnType::Nullable(inner) => *inner,
                other => other,
            };
            let ColumnType::Enum(enum_schema) = column_type else {
                return Err(Error::QueryLowering(
                    "enum match requires a payload enum column".to_owned(),
                ));
            };
            let case_tag = enum_schema
                .tag(case)
                .map_err(|_| Error::QueryLowering(format!("unknown payload enum case {case}")))?;
            let enum_case = enum_schema
                .case(case_tag)
                .map_err(|_| Error::QueryLowering(format!("unknown payload enum case {case}")))?;
            NormalizedPredicateExpr::EnumMatch {
                value: normalize_operand(source, &Operand::Column(column.clone()))?,
                case_tag,
                payload: Box::new(normalize_enum_payload_predicate(
                    &enum_case.payload,
                    source,
                    payload,
                )?),
            }
        }
    })
}

/// Normalize a predicate evaluated inside one selected payload-enum case.
///
/// Payload fields are case-local. They must never be resolved against the
/// outer table, even when that table happens to have the same field name.
fn normalize_enum_payload_predicate(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    predicate: &Predicate,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => NormalizedPredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| normalize_enum_payload_predicate(descriptor, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Any(predicates) => NormalizedPredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| normalize_enum_payload_predicate(descriptor, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Not(predicate) => NormalizedPredicateExpr::Not(Box::new(
            normalize_enum_payload_predicate(descriptor, source, predicate)?,
        )),
        Predicate::Eq(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Eq,
            right,
        )?,
        Predicate::Ne(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Ne,
            right,
        )?,
        Predicate::Gt(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Gt,
            right,
        )?,
        Predicate::Gte(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Gte,
            right,
        )?,
        Predicate::Lt(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Lt,
            right,
        )?,
        Predicate::Lte(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Lte,
            right,
        )?,
        Predicate::In(value, options) => {
            let target_type = enum_payload_operand_type(descriptor, value)?;
            NormalizedPredicateExpr::In {
                value: normalize_enum_payload_operand(descriptor, source, value, None)?,
                options: options
                    .iter()
                    .map(|operand| {
                        normalize_enum_payload_operand(
                            descriptor,
                            source,
                            operand,
                            target_type.as_ref(),
                        )
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            }
        }
        Predicate::Contains(value, needle) => {
            let needle_type = enum_payload_contains_needle_type(descriptor, value)?;
            NormalizedPredicateExpr::ArrayContains {
                value: normalize_enum_payload_operand(descriptor, source, value, None)?,
                needle: normalize_enum_payload_operand(
                    descriptor,
                    source,
                    needle,
                    needle_type.as_ref(),
                )?,
            }
        }
        Predicate::IsNull(value) => NormalizedPredicateExpr::IsNull(
            normalize_enum_payload_operand(descriptor, source, value, None)?,
        ),
        Predicate::EnumMatch { .. } => {
            return Err(Error::QueryLowering(
                "nested payload enum matches are not supported".to_owned(),
            ));
        }
    })
}

fn normalize_enum_payload_compare(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    left: &Operand,
    op: NormalizedComparisonOp,
    right: &Operand,
) -> Result<NormalizedPredicateExpr, Error> {
    let left_type = enum_payload_operand_type(descriptor, left)?;
    let right_type = enum_payload_operand_type(descriptor, right)?;
    Ok(NormalizedPredicateExpr::Compare {
        left: normalize_enum_payload_operand(descriptor, source, left, right_type.as_ref())?,
        op,
        right: normalize_enum_payload_operand(descriptor, source, right, left_type.as_ref())?,
    })
}

fn normalize_enum_payload_operand(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
) -> Result<NormalizedValueRef, Error> {
    match operand {
        Operand::Column(column) => {
            if enum_payload_field_type(descriptor, column).is_none() {
                return Err(Error::QueryLowering(format!(
                    "unknown payload enum field {column}"
                )));
            }
            Ok(NormalizedValueRef::SourceField {
                source: source.clone(),
                field: column.clone(),
            })
        }
        Operand::Param(param) => Ok(NormalizedValueRef::Param(param.clone())),
        Operand::Claim(claim) => Ok(NormalizedValueRef::Claim(ClaimPath(
            claim.split('.').map(str::to_owned).collect(),
        ))),
        Operand::Literal(value) => {
            let value = target_type
                .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                .unwrap_or_else(|| value.clone());
            Ok(NormalizedValueRef::Literal(
                postcard::to_allocvec(&value).map_err(|err| {
                    Error::QueryLowering(format!("literal encoding failed: {err}"))
                })?,
            ))
        }
    }
}

fn enum_payload_operand_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    operand: &Operand,
) -> Result<Option<ColumnType>, Error> {
    match operand {
        Operand::Column(column) => enum_payload_field_type(descriptor, column)
            .map(Some)
            .ok_or_else(|| Error::QueryLowering(format!("unknown payload enum field {column}"))),
        Operand::Literal(_) | Operand::Param(_) | Operand::Claim(_) => Ok(None),
    }
}

fn enum_payload_field_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    field: &str,
) -> Option<ColumnType> {
    descriptor
        .fields()
        .iter()
        .find(|candidate| candidate.name.as_deref() == Some(field))
        .map(|candidate| candidate.value_type.clone())
}

fn enum_payload_contains_needle_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    value: &Operand,
) -> Result<Option<ColumnType>, Error> {
    Ok(match enum_payload_operand_type(descriptor, value)? {
        Some(ColumnType::Array(member)) => Some(*member),
        Some(ColumnType::Nullable(inner)) => match *inner {
            ColumnType::Array(member) => Some(*member),
            ColumnType::String => Some(ColumnType::String),
            _ => None,
        },
        Some(ColumnType::String) => Some(ColumnType::String),
        _ => None,
    })
}

fn normalize_compare(
    schema: &JazzSchema,
    source: &SourceId,
    left: &Operand,
    op: NormalizedComparisonOp,
    right: &Operand,
) -> Result<NormalizedPredicateExpr, Error> {
    let left_type = operand_column_type(schema, source, left)?;
    let right_type = operand_column_type(schema, source, right)?;
    Ok(NormalizedPredicateExpr::Compare {
        left: normalize_operand_with_target_type(source, left, right_type.as_ref())?,
        op,
        right: normalize_operand_with_target_type(source, right, left_type.as_ref())?,
    })
}

fn normalize_operand(source: &SourceId, operand: &Operand) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type(source, operand, None)
}

fn normalize_operand_with_target_type(
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
) -> Result<NormalizedValueRef, Error> {
    Ok(match operand {
        Operand::Column(column) if column == "id" => {
            NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
        }
        Operand::Column(column) => match provenance_field(column) {
            Some(field) => NormalizedValueRef::Provenance {
                source: source.clone(),
                field,
            },
            None => NormalizedValueRef::SourceField {
                source: source.clone(),
                field: column.clone(),
            },
        },
        Operand::Param(param) => NormalizedValueRef::Param(param.clone()),
        Operand::Claim(claim) => {
            NormalizedValueRef::Claim(ClaimPath(claim.split('.').map(str::to_owned).collect()))
        }
        Operand::Literal(value) => {
            let value = target_type
                .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                .unwrap_or_else(|| value.clone());
            NormalizedValueRef::Literal(
                postcard::to_allocvec(&value).map_err(|err| {
                    Error::QueryLowering(format!("literal encoding failed: {err}"))
                })?,
            )
        }
    })
}

fn operand_column_type(
    schema: &JazzSchema,
    source: &SourceId,
    operand: &Operand,
) -> Result<Option<ColumnType>, Error> {
    let Operand::Column(column) = operand else {
        return Ok(None);
    };
    if column == "id" {
        return Ok(Some(ColumnType::Uuid));
    }
    if let Some(field) = provenance_field(column) {
        return Ok(Some(match field {
            ProvenanceField::CreatedAt | ProvenanceField::UpdatedAt => ColumnType::U64,
            ProvenanceField::CreatedBy | ProvenanceField::UpdatedBy => ColumnType::Uuid,
        }));
    }
    let table = table_schema(schema, &source.table)?;
    Ok(table
        .columns
        .iter()
        .find(|candidate| candidate.name == *column)
        .map(|column| column.column_type.clone()))
}

fn contains_needle_type(
    schema: &JazzSchema,
    source: &SourceId,
    value: &Operand,
) -> Result<Option<ColumnType>, Error> {
    Ok(match operand_column_type(schema, source, value)? {
        Some(ColumnType::Array(member)) => Some(*member),
        Some(ColumnType::Nullable(inner)) => match *inner {
            ColumnType::Array(member) => Some(*member),
            ColumnType::String => Some(ColumnType::String),
            _ => None,
        },
        Some(ColumnType::String) => Some(ColumnType::String),
        _ => None,
    })
}

fn coerce_literal_for_column_type(value: Value, column_type: &ColumnType) -> Value {
    match (value, column_type) {
        (Value::Uuid(value), ColumnType::String) => Value::String(value.to_string()),
        (Value::String(value), ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => Value::Nullable(Some(
            Box::new(coerce_literal_for_column_type(*value, inner)),
        )),
        (Value::Array(values), ColumnType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| coerce_literal_for_column_type(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), ColumnType::Tuple(types)) if values.len() == types.len() => {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_literal_for_column_type(value, column_type))
                    .collect(),
            )
        }
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_literal_for_column_type(*value, column_type),
        ))),
        (value, ColumnType::Nullable(inner)) => coerce_literal_for_column_type(value, inner),
        (value, _) => value,
    }
}

fn provenance_field(column: &str) -> Option<ProvenanceField> {
    match column {
        "$createdAt" => Some(ProvenanceField::CreatedAt),
        "$createdBy" => Some(ProvenanceField::CreatedBy),
        "$updatedAt" => Some(ProvenanceField::UpdatedAt),
        "$updatedBy" => Some(ProvenanceField::UpdatedBy),
        _ => None,
    }
}

fn normalize_order_key(
    source: &SourceId,
    order: &crate::query::OrderBy,
) -> Result<NormalizedOrderKey, Error> {
    Ok(NormalizedOrderKey {
        value: normalize_operand(source, &Operand::Column(order.column.clone()))?,
        direction: match order.direction {
            OrderDirection::Asc => NormalizedSortDirection::Asc,
            OrderDirection::Desc => NormalizedSortDirection::Desc,
        },
    })
}

fn normalized_aggregate_group_by(
    source: &SourceId,
    aggregate: &AggregateQuery,
) -> Result<Vec<NormalizedValueRef>, Error> {
    aggregate
        .group_by
        .iter()
        .map(|column| normalize_operand(source, &Operand::Column(column.clone())))
        .collect()
}

fn normalized_aggregate_outputs(
    source: &SourceId,
    aggregate: &AggregateQuery,
) -> Result<Vec<NormalizedAggregateExpr>, Error> {
    aggregate
        .aggregates
        .iter()
        .map(|aggregate| {
            Ok(NormalizedAggregateExpr {
                output: typed_output_field(
                    aggregate_output_field(&aggregate.alias),
                    normalized_aggregate_output_type(aggregate),
                ),
                function: normalized_aggregate_function(aggregate.function),
                input: aggregate
                    .column
                    .as_ref()
                    .map(|column| normalize_operand(source, &Operand::Column(column.clone())))
                    .transpose()?,
            })
        })
        .collect()
}

fn normalized_aggregate_function(function: AggregateFunction) -> NormalizedAggregateFunction {
    match function {
        AggregateFunction::Count => NormalizedAggregateFunction::Count,
        AggregateFunction::Sum => NormalizedAggregateFunction::Sum,
        AggregateFunction::Avg => NormalizedAggregateFunction::Avg,
        AggregateFunction::Min => NormalizedAggregateFunction::Min,
        AggregateFunction::Max => NormalizedAggregateFunction::Max,
    }
}

fn normalized_aggregate_output_type(aggregate: &Aggregate) -> ColumnType {
    match aggregate.function {
        AggregateFunction::Count => ColumnType::U64,
        AggregateFunction::Avg => ColumnType::Nullable(Box::new(ColumnType::F64)),
        // Aggregate lowering is currently reported as an unsupported
        // query-engine capability before Groove needs the exact result type.
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            ColumnType::Nullable(Box::new(ColumnType::Bytes))
        }
    }
}

fn normalization_gap(message: impl Into<String>) -> Error {
    Error::QueryLowering(message.into())
}

fn array_requirement(requirement: ArraySubqueryRequirement) -> CorrelationRequirement {
    match requirement {
        ArraySubqueryRequirement::Optional => CorrelationRequirement::Optional,
        ArraySubqueryRequirement::AtLeastOne => CorrelationRequirement::AtLeastOne,
        ArraySubqueryRequirement::MatchCorrelationCardinality => {
            CorrelationRequirement::MatchCorrelationCardinality
        }
    }
}

fn correlated_child_source_id(
    owner: &SourceId,
    subquery: &ArraySubquery,
    path: &[usize],
) -> SourceId {
    let mut components = owner.path.components.clone();
    let path_id = path
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(".");
    components.push(SourceRole::CorrelatedChild(format!(
        "{path_id}:{}",
        subquery.column_name
    )));
    SourceId {
        table: subquery.table.clone(),
        path: SourcePath { components },
    }
}

fn include_auxiliary_source_id(
    table: impl Into<String>,
    include_index: usize,
    segment_index: usize,
) -> SourceId {
    SourceId {
        table: table.into(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::Alias(format!("include:{include_index}:{segment_index}")),
            ],
        },
    }
}

fn collect_closure_paths<S>(
    node: &NodeState<S>,
    root_table: &str,
    schema_version: SchemaVersionId,
    includes: &[Include],
) -> Result<(BTreeSet<SourceId>, Vec<ClosurePath>), Error>
where
    S: OrderedKvStorage,
{
    let mut sources = BTreeSet::new();
    let mut paths = Vec::new();
    let root_source = root_source_id(root_table);
    let root_schema = node.table_in_schema_or_branch_metadata(root_table, schema_version)?;
    let explicit_root_segments = includes
        .iter()
        .filter_map(|include| include.path.split('.').next())
        .collect::<BTreeSet<_>>();
    for (reference_index, (column, target_table)) in root_schema.references.iter().enumerate() {
        if explicit_root_segments.contains(column.as_str()) {
            continue;
        }
        let target = include_auxiliary_source_id(target_table.clone(), usize::MAX, reference_index);
        sources.insert(target.clone());
        paths.push(ClosurePath::ImplicitRootReference {
            id: format!("reference:{column}"),
            segment: ClosurePathSegment {
                parent: root_source.clone(),
                target,
                source_field: column.clone(),
            },
        });
    }
    for (include_index, include) in includes.iter().enumerate() {
        let mut current_table_name = root_table.to_owned();
        let mut parent = root_source.clone();
        let mut segments = Vec::new();
        for (segment_index, segment) in include.path.split('.').enumerate() {
            let current_table =
                node.table_in_schema_or_branch_metadata(&current_table_name, schema_version)?;
            let target_table = current_table
                .references
                .get(segment)
                .cloned()
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?;
            let target =
                include_auxiliary_source_id(target_table.clone(), include_index, segment_index);
            sources.insert(target.clone());
            segments.push(ClosurePathSegment {
                parent: parent.clone(),
                target: target.clone(),
                source_field: segment.to_owned(),
            });
            parent = target;
            current_table_name = target_table;
        }
        paths.push(ClosurePath::ExplicitInclude {
            id: format!("include:{include_index}:{}", include.path),
            segments,
            root_gate: if include.require {
                Some(ClosureRootGate::Required)
            } else if include.join_mode == crate::query::JoinMode::Inner {
                Some(ClosureRootGate::Inner)
            } else {
                None
            },
        });
    }
    Ok((sources, paths))
}

fn normalize_array_subquery(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    current: RowSetNodeId,
    schema: &JazzSchema,
    owner_source: &SourceId,
    subquery: &ArraySubquery,
    path: &[usize],
) -> Result<RowSetNodeId, Error> {
    let path_id = path
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(".");
    let child_source = correlated_child_source_id(owner_source, subquery, path);
    let child_node = RowSetNodeId(format!("array_subquery:{path_id}:source"));
    nodes.insert(
        child_node.clone(),
        RowSetExpr::Source {
            source: child_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut child_current = child_node;

    if !subquery.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("array_subquery:{path_id}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: child_current,
                predicate: normalize_predicates(schema, &child_source, &subquery.filters)
                    .map_err(|err| normalization_gap(err.to_string()))?,
            },
        );
        child_current = filter_node;
    }

    if !subquery.order_by.is_empty() {
        let order_node = RowSetNodeId(format!("array_subquery:{path_id}:order"));
        nodes.insert(
            order_node.clone(),
            RowSetExpr::OrderBy {
                input: child_current,
                keys: subquery
                    .order_by
                    .iter()
                    .map(|order| normalize_order_key(&child_source, order))
                    .collect::<Result<Vec<_>, Error>>()
                    .map_err(|err| normalization_gap(err.to_string()))?,
            },
        );
        child_current = order_node;
    }

    if subquery.limit.is_some() || subquery.offset != 0 {
        let slice_node = RowSetNodeId(format!("array_subquery:{path_id}:slice"));
        nodes.insert(
            slice_node.clone(),
            RowSetExpr::Slice {
                input: child_current,
                partition_by: vec![NormalizedValueRef::SourceField {
                    source: child_source.clone(),
                    field: subquery.inner_column.clone(),
                }],
                limit: subquery
                    .limit
                    .map(|limit| limit.min(u32::MAX as usize) as u32),
                offset: subquery.offset.min(u32::MAX as usize) as u32,
                tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                    child_source.clone(),
                ))],
                rank_output: None,
            },
        );
        child_current = slice_node;
    }

    let nested_parent_input = child_current.clone();
    let path_node = RowSetNodeId(format!("array_subquery:{path_id}:path"));
    nodes.insert(
        path_node.clone(),
        RowSetExpr::CorrelatedPathProjection {
            input: current,
            child_input: child_current,
            path: ProgramPathId {
                owner: owner_source.clone(),
                child: child_source.clone(),
            },
            correlation: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::SourceField {
                    source: child_source.clone(),
                    field: subquery.inner_column.clone(),
                },
                op: NormalizedComparisonOp::Eq,
                right: normalize_operand(
                    owner_source,
                    &Operand::Column(subquery.outer_column.clone()),
                )
                .map_err(|err| normalization_gap(err.to_string()))?,
            },
            requirement: array_requirement(subquery.requirement),
        },
    );
    for (nested_index, nested) in subquery.nested_arrays.iter().enumerate() {
        let mut nested_path = path.to_vec();
        nested_path.push(nested_index);
        normalize_array_subquery(
            nodes,
            nested_parent_input.clone(),
            schema,
            &child_source,
            nested,
            &nested_path,
        )?;
    }
    Ok(path_node)
}

fn normalize_reachable(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    current: RowSetNodeId,
    schema: &JazzSchema,
    root_source: &SourceId,
    reachable: &crate::query::ReachableVia,
    index: usize,
    prefix: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, ReachableContribution), Error> {
    let reachable_id = if prefix.is_empty() {
        format!("reachable:{index}")
    } else {
        format!("{prefix}:reachable:{index}")
    };
    let frontier = FrontierId(format!("{reachable_id}:frontier"));
    let (seed_node, columns) = normalize_reachable_seed(
        nodes,
        schema,
        reachable,
        &reachable_id,
        binding_source_shape,
        param_types,
    )?;
    let frontier_node = RowSetNodeId(format!("{reachable_id}:frontier"));
    nodes.insert(
        frontier_node.clone(),
        RowSetExpr::FrontierSource {
            frontier: frontier.clone(),
            columns: columns.clone(),
        },
    );

    let edge_source = reachable_edge_source_id(reachable, &reachable_id);
    let edge_source_node = RowSetNodeId(format!("{reachable_id}:edge_source"));
    nodes.insert(
        edge_source_node.clone(),
        RowSetExpr::Source {
            source: edge_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut edge_current = edge_source_node;
    if !reachable.edge_filters.is_empty() {
        let edge_filter_node = RowSetNodeId(format!("{reachable_id}:edge_filter"));
        nodes.insert(
            edge_filter_node.clone(),
            RowSetExpr::Filter {
                input: edge_current,
                predicate: normalize_predicates(schema, &edge_source, &reachable.edge_filters)?,
            },
        );
        edge_current = edge_filter_node;
    }

    let step_join_node = RowSetNodeId(format!("{reachable_id}:step_join"));
    nodes.insert(
        step_join_node.clone(),
        RowSetExpr::Join {
            left: frontier_node,
            right: edge_current,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: "reachable_team".to_owned(),
                },
                op: NormalizedComparisonOp::Eq,
                right: if reachable.edge_member_column == "id" {
                    NormalizedValueRef::RowId(RowIdRef::Source(edge_source.clone()))
                } else {
                    NormalizedValueRef::SourceField {
                        source: edge_source.clone(),
                        field: reachable.edge_member_column.clone(),
                    }
                },
            },
        },
    );
    let step_project_node = RowSetNodeId(format!("{reachable_id}:step_project"));
    let mut step_columns = vec![
        RowProjection {
            output: typed_output_field("team", ColumnType::Uuid),
            value: NormalizedValueRef::FrontierColumn {
                frontier: frontier.clone(),
                field: "team".to_owned(),
            },
        },
        RowProjection {
            output: typed_output_field("reachable_team", ColumnType::Uuid),
            value: NormalizedValueRef::SourceField {
                source: edge_source.clone(),
                field: reachable.edge_parent_column.clone(),
            },
        },
    ];
    step_columns.extend(
        columns
            .iter()
            .filter(|column| column.name != "team" && column.name != "reachable_team")
            .map(|column| RowProjection {
                output: typed_output_field(&column.name, column.ty.clone()),
                value: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: column.name.clone(),
                },
            }),
    );
    nodes.insert(
        step_project_node.clone(),
        RowSetExpr::Project {
            input: step_join_node,
            columns: step_columns,
        },
    );

    let closure_node = RowSetNodeId(format!("{reachable_id}:closure"));
    nodes.insert(
        closure_node.clone(),
        RowSetExpr::RecursiveRelation {
            seed: seed_node,
            step: step_project_node,
            frontier: frontier.clone(),
            frontier_key: NormalizedValueRef::FrontierColumn {
                frontier: frontier.clone(),
                field: "reachable_team".to_owned(),
            },
            dedupe_keys: reachable_dedupe_keys(&frontier, &columns),
            bound: reachable.bound,
        },
    );

    let access_source = reachable_access_source_id(reachable, &reachable_id);
    let access_source_node = RowSetNodeId(format!("{reachable_id}:access_source"));
    nodes.insert(
        access_source_node.clone(),
        RowSetExpr::Source {
            source: access_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut access_current = access_source_node;
    if !reachable.access_filters.is_empty() {
        let access_filter_node = RowSetNodeId(format!("{reachable_id}:access_filter"));
        nodes.insert(
            access_filter_node.clone(),
            RowSetExpr::Filter {
                input: access_current,
                predicate: normalize_predicates(schema, &access_source, &reachable.access_filters)?,
            },
        );
        access_current = access_filter_node;
    }

    let access_join_node = RowSetNodeId(format!("{reachable_id}:access_join"));
    nodes.insert(
        access_join_node.clone(),
        RowSetExpr::Join {
            left: access_current,
            right: closure_node,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: reachable_access_key(
                    &access_source,
                    &reachable.access_team_column,
                    reachable.access_team_target,
                ),
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: "reachable_team".to_owned(),
                },
            },
        },
    );

    let root_join_node = RowSetNodeId(format!("{reachable_id}:root_join"));
    nodes.insert(
        root_join_node.clone(),
        RowSetExpr::Join {
            left: current,
            right: access_join_node.clone(),
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                op: NormalizedComparisonOp::Eq,
                right: reachable_access_key(
                    &access_source,
                    &reachable.access_row_column,
                    JoinTarget::Column,
                ),
            },
        },
    );
    Ok((
        root_join_node,
        ReachableContribution {
            id: reachable_id,
            access_source,
            access_input: access_join_node,
            root_ref_field: reachable.access_row_column.clone(),
        },
    ))
}

fn reachable_access_key(
    access_source: &SourceId,
    column: &str,
    target: JoinTarget,
) -> NormalizedValueRef {
    if column == "id" || target == JoinTarget::RowId {
        NormalizedValueRef::RowId(RowIdRef::Source(access_source.clone()))
    } else {
        NormalizedValueRef::SourceField {
            source: access_source.clone(),
            field: column.to_owned(),
        }
    }
}

fn normalize_join_via_right(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    schema: &JazzSchema,
    join: &JoinVia,
    path: &str,
) -> Result<(RowSetNodeId, SourceId), Error> {
    let join_source = nested_join_source_id(join, path);
    auxiliary_sources.insert(join_source.clone());
    let table = table_schema(schema, &join.table)?;
    let source_node = RowSetNodeId(format!("{path}:source"));
    nodes.insert(
        source_node.clone(),
        RowSetExpr::Source {
            source: join_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut current = source_node;
    if !join.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("{path}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: current,
                predicate: normalize_predicates(schema, &join_source, &join.filters)?,
            },
        );
        current = filter_node;
    }

    if let Some(lookup) = &join.source_lookup {
        let lookup_source = join_lookup_source_id(lookup, path);
        auxiliary_sources.insert(lookup_source.clone());
        let lookup_source_node = RowSetNodeId(format!("{path}:lookup_source"));
        nodes.insert(
            lookup_source_node.clone(),
            RowSetExpr::Source {
                source: lookup_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let lookup_join_node = RowSetNodeId(format!("{path}:lookup_join"));
        nodes.insert(
            lookup_join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right: lookup_source_node,
                mode: NormalizedJoinMode::Inner,
                on: NormalizedPredicateExpr::Compare {
                    left: join_via_target_key(&join_source, join),
                    op: NormalizedComparisonOp::Eq,
                    right: if lookup.value_column == "id" {
                        NormalizedValueRef::RowId(RowIdRef::Source(lookup_source.clone()))
                    } else {
                        NormalizedValueRef::SourceField {
                            source: lookup_source.clone(),
                            field: lookup.value_column.clone(),
                        }
                    },
                },
            },
        );
        let lookup_project_node = RowSetNodeId(format!("{path}:lookup_project"));
        let mut columns = source_public_field_projections(table, &join_source);
        columns.push(RowProjection {
            output: typed_output_field(lookup.row_id_source_column.clone(), ColumnType::Uuid),
            value: NormalizedValueRef::RowId(RowIdRef::Source(lookup_source)),
        });
        nodes.insert(
            lookup_project_node.clone(),
            RowSetExpr::Project {
                input: lookup_join_node,
                columns,
            },
        );
        current = lookup_project_node;
    }

    for (nested_index, nested) in join.nested_joins.iter().enumerate() {
        let nested_path = format!("{path}:nested:{nested_index}");
        let (nested_right, nested_source) =
            normalize_join_via_right(nodes, auxiliary_sources, schema, nested, &nested_path)?;
        let nested_join_node = RowSetNodeId(format!("{nested_path}:join"));
        nodes.insert(
            nested_join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right: nested_right,
                mode: NormalizedJoinMode::Inner,
                on: join_via_predicate(&join_source, &nested_source, nested),
            },
        );
        let project_node = RowSetNodeId(format!("{nested_path}:parent_project"));
        nodes.insert(
            project_node.clone(),
            RowSetExpr::Project {
                input: nested_join_node,
                columns: source_public_field_projections(table, &join_source),
            },
        );
        current = project_node;
    }

    Ok((current, join_source))
}

fn reachable_dedupe_keys(
    frontier: &FrontierId,
    columns: &[ValueSourceColumn],
) -> Vec<NormalizedValueRef> {
    std::iter::once("reachable_team")
        .chain(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| *name != "team" && *name != "reachable_team"),
        )
        .map(|field| NormalizedValueRef::FrontierColumn {
            frontier: frontier.clone(),
            field: field.to_owned(),
        })
        .collect()
}

fn normalize_reachable_seed(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    schema: &JazzSchema,
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, Vec<ValueSourceColumn>), Error> {
    if let Some(seed) = &reachable.seed {
        let seed_source = reachable_seed_source_id(seed, reachable_id);
        let mut columns = reachable_seed_frontier_columns(schema, &seed_source, seed)?;
        let edge_route_columns = reachable_edge_route_columns(reachable, param_types)?;
        for column in &edge_route_columns {
            if !columns.iter().any(|existing| existing.name == column.name) {
                columns.push(column.clone());
            }
        }
        let user_column_ty = seed
            .user_column
            .as_ref()
            .map(|column| schema_column_type(schema, &seed.table, column))
            .transpose()?;
        let team_column_ty = schema_column_type(schema, &seed.table, &seed.team_column)?;
        if team_column_ty != ColumnType::Uuid {
            return Err(Error::QueryLowering(format!(
                "reachable_via seed {}.{} must be uuid, found {:?}",
                seed.table, seed.team_column, team_column_ty
            )));
        }
        let seed_source_node = RowSetNodeId(format!("{reachable_id}:seed_source"));
        nodes.insert(
            seed_source_node.clone(),
            RowSetExpr::Source {
                source: seed_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let mut seed_current = seed_source_node;
        let claim_route_field = seed.user_claim.as_ref().map(|user_claim| {
            let claim_path = ClaimPath(user_claim.split('.').map(str::to_owned).collect());
            (claim_path.clone(), claim_param_field(&claim_path))
        });
        if let (Some(user_column), Some((_, claim_field))) = (&seed.user_column, &claim_route_field)
        {
            let seed_claim_filter_node = RowSetNodeId(format!("{reachable_id}:seed_claim_filter"));
            nodes.insert(
                seed_claim_filter_node.clone(),
                RowSetExpr::Filter {
                    input: seed_current,
                    predicate: NormalizedPredicateExpr::Compare {
                        left: NormalizedValueRef::SourceField {
                            source: seed_source.clone(),
                            field: user_column.clone(),
                        },
                        op: NormalizedComparisonOp::Eq,
                        right: NormalizedValueRef::Param(claim_field.clone()),
                    },
                },
            );
            seed_current = seed_claim_filter_node;
        }
        if !seed.filters.is_empty() {
            let seed_filter_node = RowSetNodeId(format!("{reachable_id}:seed_filter"));
            nodes.insert(
                seed_filter_node.clone(),
                RowSetExpr::Filter {
                    input: seed_current,
                    predicate: normalize_predicates(schema, &seed_source, &seed.filters)?,
                },
            );
            seed_current = seed_filter_node;
        }
        let seed_project_node = RowSetNodeId(format!("{reachable_id}:seed_project"));
        let seed_team_value = if seed.team_column == "id" {
            NormalizedValueRef::RowId(RowIdRef::Source(seed_source.clone()))
        } else {
            NormalizedValueRef::SourceField {
                source: seed_source.clone(),
                field: seed.team_column.clone(),
            }
        };
        let mut seed_columns = vec![
            RowProjection {
                output: typed_output_field("team", ColumnType::Uuid),
                value: seed_team_value.clone(),
            },
            RowProjection {
                output: typed_output_field("reachable_team", ColumnType::Uuid),
                value: seed_team_value,
            },
        ];
        if let Some((_, claim_field)) = &claim_route_field {
            seed_columns.push(RowProjection {
                output: typed_output_field(
                    claim_field,
                    user_column_ty.clone().unwrap_or(ColumnType::Uuid),
                ),
                value: NormalizedValueRef::Param(claim_field.clone()),
            });
        }
        seed_columns.extend(edge_route_columns.into_iter().map(|column| RowProjection {
            output: typed_output_field(&column.name, column.ty),
            value: column.value,
        }));
        nodes.insert(
            seed_project_node.clone(),
            RowSetExpr::Project {
                input: seed_current,
                columns: seed_columns,
            },
        );
        return Ok((seed_project_node, columns));
    }

    let mut columns = reachable_frontier_columns(&reachable.from, param_types)?;
    for column in reachable_edge_route_columns(reachable, param_types)? {
        if !columns.iter().any(|existing| existing.name == column.name) {
            columns.push(column);
        }
    }
    let seed_node = RowSetNodeId(format!("{reachable_id}:seed"));
    nodes.insert(
        seed_node.clone(),
        RowSetExpr::ValueSource {
            shape: binding_source_shape.to_owned(),
            columns: columns.clone(),
            mode: reachable_seed_value_source_mode(&reachable.from)?,
        },
    );
    Ok((seed_node, columns))
}

fn reachable_edge_route_columns(
    reachable: &crate::query::ReachableVia,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ValueSourceColumn>, Error> {
    predicate_params(&reachable.edge_filters)
        .into_iter()
        .map(|param| {
            let ty = param_types.get(&param).cloned().ok_or_else(|| {
                Error::QueryLowering(format!("unknown reachable edge parameter {param}"))
            })?;
            Ok(ValueSourceColumn {
                name: route_param_field(&param),
                value: NormalizedValueRef::Param(param),
                ty,
            })
        })
        .collect()
}

fn reachable_seed_frontier_columns(
    schema: &JazzSchema,
    source: &SourceId,
    seed: &crate::query::ReachableSeed,
) -> Result<Vec<ValueSourceColumn>, Error> {
    let team_column_ty = schema_column_type(schema, &seed.table, &seed.team_column)?;
    if team_column_ty != ColumnType::Uuid {
        return Err(Error::QueryLowering(format!(
            "reachable_via seed {}.{} must be uuid, found {:?}",
            seed.table, seed.team_column, team_column_ty
        )));
    }
    let value = if seed.team_column == "id" {
        NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
    } else {
        NormalizedValueRef::SourceField {
            source: source.clone(),
            field: seed.team_column.clone(),
        }
    };
    let mut columns = vec![
        ValueSourceColumn {
            name: "team".to_owned(),
            value: value.clone(),
            ty: ColumnType::Uuid,
        },
        ValueSourceColumn {
            name: "reachable_team".to_owned(),
            value,
            ty: ColumnType::Uuid,
        },
    ];
    if let Some(user_claim) = &seed.user_claim {
        let Some(user_column) = &seed.user_column else {
            return Err(Error::QueryLowering(
                "reachable_via relation seed user_claim requires user_column".to_owned(),
            ));
        };
        let user_column_ty = schema_column_type(schema, &seed.table, user_column)?;
        let path = ClaimPath(user_claim.split('.').map(str::to_owned).collect());
        columns.push(ValueSourceColumn {
            name: claim_param_field(&path),
            value: NormalizedValueRef::Claim(path),
            ty: user_column_ty,
        });
    }
    Ok(columns)
}

fn reachable_frontier_columns(
    seed: &Operand,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ValueSourceColumn>, Error> {
    let value = reachable_seed_value_ref(seed)?;
    let ty = match seed {
        Operand::Param(param) => param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        Operand::Literal(Value::Uuid(_)) => ColumnType::Uuid,
        Operand::Claim(_) => ColumnType::Uuid,
        Operand::Column(_) | Operand::Literal(_) => {
            return Err(normalization_gap(
                "reachable_via currently supports uuid parameter/claim/literal seeds only",
            ));
        }
    };
    let mut columns = vec![
        ValueSourceColumn {
            name: "team".to_owned(),
            value: value.clone(),
            ty: ty.clone(),
        },
        ValueSourceColumn {
            name: "reachable_team".to_owned(),
            value,
            ty,
        },
    ];
    if let Operand::Param(param) = seed {
        columns.push(ValueSourceColumn {
            name: route_param_field(param),
            value: NormalizedValueRef::Param(param.clone()),
            ty: param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        });
    }
    if let Operand::Claim(claim) = seed {
        let path = ClaimPath(claim.split('.').map(str::to_owned).collect());
        columns.push(ValueSourceColumn {
            name: claim_param_field(&path),
            value: NormalizedValueRef::Claim(path),
            ty: ColumnType::Uuid,
        });
    }
    if let Operand::Param(param) = seed
        && param != "team"
        && param != "reachable_team"
    {
        columns.push(ValueSourceColumn {
            name: param.clone(),
            value: NormalizedValueRef::Param(param.clone()),
            ty: param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        });
    }
    Ok(columns)
}

fn reachable_seed_value_ref(seed: &Operand) -> Result<NormalizedValueRef, Error> {
    match seed {
        Operand::Param(param) => Ok(NormalizedValueRef::Param(param.clone())),
        Operand::Literal(Value::Uuid(uuid)) => literal_value_ref(&Value::Uuid(*uuid)),
        Operand::Claim(claim) => Ok(NormalizedValueRef::Claim(ClaimPath(
            claim.split('.').map(str::to_owned).collect(),
        ))),
        Operand::Column(_) | Operand::Literal(_) => Err(normalization_gap(
            "reachable_via currently supports uuid parameter/claim/literal seeds only",
        )),
    }
}

fn reachable_seed_value_source_mode(seed: &Operand) -> Result<ValueSourceMode, Error> {
    match seed {
        Operand::Param(_) | Operand::Claim(_) => Ok(ValueSourceMode::Binding),
        Operand::Literal(Value::Uuid(_)) => Ok(ValueSourceMode::Inline),
        Operand::Column(_) | Operand::Literal(_) => Err(normalization_gap(
            "reachable_via currently supports uuid parameter/claim/literal seeds only",
        )),
    }
}

fn literal_value_ref(value: &Value) -> Result<NormalizedValueRef, Error> {
    Ok(NormalizedValueRef::Literal(
        postcard::to_allocvec(value)
            .map_err(|err| Error::QueryLowering(format!("literal encoding failed: {err}")))?,
    ))
}

fn typed_output_field(name: impl Into<String>, ty: ColumnType) -> TypedOutputField {
    TypedOutputField {
        name: name.into(),
        ty,
    }
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> Result<&'a TableSchema, Error> {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| Error::QueryLowering(format!("unknown query table {table}")))
}

fn schema_column_type(schema: &JazzSchema, table: &str, column: &str) -> Result<ColumnType, Error> {
    if column == "id" {
        return Ok(ColumnType::Uuid);
    }
    table_schema(schema, table)?
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|column| column.column_type.clone())
        .ok_or_else(|| Error::QueryLowering(format!("unknown query column {table}.{column}")))
}

fn row_id_output_field() -> TypedOutputField {
    typed_output_field("id", ColumnType::Uuid)
}

fn source_public_field_projections(table: &TableSchema, source: &SourceId) -> Vec<RowProjection> {
    std::iter::once(RowProjection {
        output: row_id_output_field(),
        value: NormalizedValueRef::RowId(RowIdRef::Source(source.clone())),
    })
    .chain(table.columns.iter().map(|column| RowProjection {
        output: typed_output_field(column.name.clone(), column.column_type.clone()),
        value: NormalizedValueRef::SourceField {
            source: source.clone(),
            field: column.name.clone(),
        },
    }))
    .collect()
}

fn join_via_root_key(root_source: &SourceId, join: &JoinVia) -> NormalizedValueRef {
    join.source_column
        .as_ref()
        .map(|field| {
            if field == "id" {
                NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone()))
            } else {
                NormalizedValueRef::SourceField {
                    source: root_source.clone(),
                    field: field.clone(),
                }
            }
        })
        .unwrap_or_else(|| NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())))
}

fn join_via_target_key(join_source: &SourceId, join: &JoinVia) -> NormalizedValueRef {
    match join.target {
        JoinTarget::Column => NormalizedValueRef::SourceField {
            source: join_source.clone(),
            field: join.on_column.clone(),
        },
        JoinTarget::RowId => NormalizedValueRef::RowId(RowIdRef::Source(join_source.clone())),
    }
}

fn join_via_predicate(
    left_source: &SourceId,
    right_source: &SourceId,
    join: &JoinVia,
) -> NormalizedPredicateExpr {
    let mut key_pairs = vec![if let Some(lookup) = &join.source_lookup {
        (
            NormalizedValueRef::SourceField {
                source: left_source.clone(),
                field: lookup.row_id_source_column.clone(),
            },
            NormalizedValueRef::SourceField {
                source: right_source.clone(),
                field: lookup.row_id_source_column.clone(),
            },
        )
    } else {
        (
            join_via_root_key(left_source, join),
            join_via_target_key(right_source, join),
        )
    }];
    key_pairs.extend(join.correlated_filters.iter().map(|correlation| {
        (
            NormalizedValueRef::SourceField {
                source: left_source.clone(),
                field: correlation.source_column.clone(),
            },
            NormalizedValueRef::SourceField {
                source: right_source.clone(),
                field: correlation.join_column.clone(),
            },
        )
    }));
    if key_pairs.len() == 1 {
        let (left, right) = key_pairs.remove(0);
        NormalizedPredicateExpr::Compare {
            left,
            op: NormalizedComparisonOp::Eq,
            right,
        }
    } else {
        NormalizedPredicateExpr::And(
            key_pairs
                .into_iter()
                .map(|(left, right)| NormalizedPredicateExpr::Compare {
                    left,
                    op: NormalizedComparisonOp::Eq,
                    right,
                })
                .collect(),
        )
    }
}

fn reachable_edge_source_id(
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
) -> SourceId {
    SourceId {
        table: reachable.edge_table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveStep(format!("{reachable_id}:{}", reachable.edge_table)),
            ],
        },
    }
}

fn reachable_access_source_id(
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
) -> SourceId {
    SourceId {
        table: reachable.access_table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!(
                "{reachable_id}:{}",
                reachable.access_table
            ))],
        },
    }
}

fn reachable_seed_source_id(seed: &crate::query::ReachableSeed, reachable_id: &str) -> SourceId {
    SourceId {
        table: seed.table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveSeed(format!("{reachable_id}:{}", seed.table)),
            ],
        },
    }
}

fn inherited_parent_source_id(table: &str, prefix: &str) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![SourceRole::Alias(prefix.to_owned())],
        },
    }
}

struct FilterJoinChain<'a> {
    filters: &'a [Predicate],
    joins: &'a [JoinVia],
}

struct PolicyAtomChain<'a> {
    filters: &'a [Predicate],
    joins: &'a [JoinVia],
    inherits: &'a [crate::query::InheritsVia],
    reachable: &'a [crate::query::ReachableVia],
}

/// The inheritance atoms expanded on the current policy-composition path.
///
/// A policy can refer back to its own table through an `InheritsVia`. The
/// normalized graph is finite only when that expansion is bounded; keep this
/// state per path so independent policy alternatives do not consume each
/// other's depth budget.
#[derive(Clone, Default)]
struct InheritanceExpansionPath {
    uses: BTreeMap<InheritanceExpansionKey, usize>,
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct InheritanceExpansionKey {
    child_table: String,
    parent_column: String,
    operation: crate::query::InheritsOperation,
}

impl InheritanceExpansionPath {
    fn descend(&self, child_table: &str, inherits: &crate::query::InheritsVia) -> Option<Self> {
        let key = InheritanceExpansionKey {
            child_table: child_table.to_owned(),
            parent_column: inherits.parent_column.clone(),
            operation: inherits.operation,
        };
        let used = self.uses.get(&key).copied().unwrap_or(0);
        let limit = inherits
            .max_depth
            .unwrap_or_else(|| crate::query::RecursionBound::default_max_depth().depth_steps());
        if used >= limit {
            return None;
        }
        let mut next = self.clone();
        next.uses.insert(key, used + 1);
        Some(next)
    }
}

fn normalize_false_policy_branch(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    input: RowSetNodeId,
    prefix: &str,
) -> RowSetNodeId {
    let node = RowSetNodeId(format!("{prefix}:max_depth"));
    nodes.insert(
        node.clone(),
        RowSetExpr::Filter {
            input,
            predicate: NormalizedPredicateExpr::Or(Vec::new()),
        },
    );
    node
}

fn normalize_filter_join_chain(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    schema: &JazzSchema,
    root_source: &SourceId,
    start: RowSetNodeId,
    prefix: &str,
    chain: FilterJoinChain<'_>,
    record_join_contributions: bool,
) -> Result<RowSetNodeId, Error> {
    let mut current = start;
    if !chain.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("{prefix}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: current,
                predicate: normalize_predicates(schema, root_source, chain.filters)?,
            },
        );
        current = filter_node;
    }

    for (index, join) in chain.joins.iter().enumerate() {
        let path = if prefix == "query" {
            format!("join_via:{index}")
        } else {
            format!("{prefix}:join_via:{index}")
        };
        let (right, join_source) =
            normalize_join_via_right(nodes, auxiliary_sources, schema, join, &path)?;
        let join_predicate = join_via_predicate(root_source, &join_source, join);
        if record_join_contributions {
            join_contributions.push(JoinContribution {
                id: path.clone(),
                source: join_source.clone(),
                input: right.clone(),
                membership: join_predicate.clone(),
            });
        }
        let join_node = RowSetNodeId(format!("{path}:join"));
        nodes.insert(
            join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right,
                mode: NormalizedJoinMode::Inner,
                on: join_predicate,
            },
        );
        current = join_node;
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn normalize_policy_atom_chain(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &JazzSchema,
    root_source: &SourceId,
    start: RowSetNodeId,
    prefix: &str,
    chain: PolicyAtomChain<'_>,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    record_join_contributions: bool,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let mut current = normalize_filter_join_chain(
        nodes,
        auxiliary_sources,
        join_contributions,
        schema,
        root_source,
        start,
        prefix,
        FilterJoinChain {
            filters: chain.filters,
            joins: chain.joins,
        },
        record_join_contributions,
    )?;
    for (index, inherits) in chain.inherits.iter().enumerate() {
        current = normalize_inherited_parent_policy(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            current,
            inherits,
            &format!("{prefix}:inherits:{index}"),
            binding_source_shape,
            param_types,
            inheritance_path,
        )?;
    }
    for (index, reachable) in chain.reachable.iter().enumerate() {
        let reachable_prefix = if prefix == "query" { "" } else { prefix };
        let (next, contribution) = normalize_reachable(
            nodes,
            current,
            schema,
            root_source,
            reachable,
            index,
            reachable_prefix,
            binding_source_shape,
            param_types,
        )?;
        current = next;
        reachable_contributions.push(contribution);
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn normalize_inherited_parent_policy(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &JazzSchema,
    child_source: &SourceId,
    child_current: RowSetNodeId,
    inherits: &crate::query::InheritsVia,
    prefix: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let child_table = table_schema(schema, &child_source.table)?;
    let parent_table_name = child_table
        .references
        .get(&inherits.parent_column)
        .cloned()
        .ok_or_else(|| {
            Error::QueryLowering(format!(
                "{}.{} is not a parent reference",
                child_source.table, inherits.parent_column
            ))
        })?;
    let parent_table = table_schema(schema, &parent_table_name)?;
    let Some(parent_inheritance_path) = inheritance_path.descend(&child_source.table, inherits)
    else {
        return Ok(normalize_false_policy_branch(nodes, child_current, prefix));
    };
    let parent_source = inherited_parent_source_id(&parent_table_name, prefix);
    auxiliary_sources.insert(parent_source.clone());
    let parent_source_node = RowSetNodeId(format!("{prefix}:source"));
    nodes.insert(
        parent_source_node.clone(),
        RowSetExpr::Source {
            source: parent_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut parent_current = parent_source_node;
    let parent_policy = match inherits.operation {
        crate::query::InheritsOperation::Select => parent_table.read_policy.as_ref(),
        crate::query::InheritsOperation::Insert => {
            parent_table.write_policies.insert_check.as_ref()
        }
        crate::query::InheritsOperation::Update => {
            parent_table.write_policies.update_using.as_ref()
        }
        crate::query::InheritsOperation::Delete => {
            parent_table.write_policies.delete_using.as_ref()
        }
    };
    if let Some(policy) = parent_policy {
        parent_current = if !policy.policy_branches.is_empty() {
            normalize_policy_branch_authorization(
                nodes,
                auxiliary_sources,
                join_contributions,
                reachable_contributions,
                schema,
                &parent_source,
                parent_current,
                &format!("{prefix}:parent_policy"),
                policy,
                binding_source_shape,
                param_types,
                &parent_inheritance_path,
            )?
        } else {
            normalize_policy_atom_chain(
                nodes,
                auxiliary_sources,
                join_contributions,
                reachable_contributions,
                schema,
                &parent_source,
                parent_current,
                &format!("{prefix}:parent_policy"),
                PolicyAtomChain {
                    filters: &policy.filters,
                    joins: &policy.joins,
                    inherits: &policy.inherits,
                    reachable: &policy.reachable,
                },
                binding_source_shape,
                param_types,
                false,
                &parent_inheritance_path,
            )?
        };
    }
    let join_node = RowSetNodeId(format!("{prefix}:join"));
    nodes.insert(
        join_node.clone(),
        RowSetExpr::Join {
            left: child_current,
            right: parent_current,
            mode: NormalizedJoinMode::Semi,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::SourceField {
                    source: child_source.clone(),
                    field: inherits.parent_column.clone(),
                },
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::RowId(RowIdRef::Source(parent_source)),
            },
        },
    );
    Ok(join_node)
}

#[allow(clippy::too_many_arguments)]
fn normalize_policy_branch_authorization(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &JazzSchema,
    root_source: &SourceId,
    current: RowSetNodeId,
    prefix: &str,
    policy: &JazzQuery,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let mut union_inputs = Vec::new();
    if !policy_branch_base_is_converter_false(policy) {
        let base_source_node = RowSetNodeId(format!("{prefix}:base:root"));
        nodes.insert(
            base_source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let base = normalize_policy_atom_chain(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            base_source_node,
            &format!("{prefix}:base"),
            PolicyAtomChain {
                filters: &policy.filters,
                joins: &policy.joins,
                inherits: &policy.inherits,
                reachable: &policy.reachable,
            },
            binding_source_shape,
            param_types,
            false,
            inheritance_path,
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                base,
                root_source,
                RowSetNodeId(format!("{prefix}:base:row_id")),
            ),
            label: policy_branch_semantic_label(
                &policy.filters,
                &policy.joins,
                &policy.reachable,
                &policy.inherits,
            )?,
        });
    }

    for (index, branch) in policy.policy_branches.iter().enumerate() {
        let branch_source_node = RowSetNodeId(format!("{prefix}:{index}:root"));
        nodes.insert(
            branch_source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let branch_current = normalize_policy_atom_chain(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            branch_source_node,
            &format!("{prefix}:{index}"),
            PolicyAtomChain {
                filters: &branch.filters,
                joins: &branch.joins,
                inherits: &branch.inherits,
                reachable: &branch.reachable,
            },
            binding_source_shape,
            param_types,
            false,
            inheritance_path,
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                branch_current,
                root_source,
                RowSetNodeId(format!("{prefix}:{index}:row_id")),
            ),
            label: policy_branch_semantic_label(
                &branch.filters,
                &branch.joins,
                &branch.reachable,
                &branch.inherits,
            )?,
        });
    }

    let union_node = RowSetNodeId(format!("{prefix}:authorized_rows"));
    nodes.insert(
        union_node.clone(),
        RowSetExpr::Union {
            inputs: union_inputs,
        },
    );
    let join_node = RowSetNodeId(format!("{prefix}:authorize"));
    nodes.insert(
        join_node.clone(),
        RowSetExpr::Join {
            left: current,
            right: union_node,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::SourceField {
                    source: root_source.clone(),
                    field: "row_uuid".to_owned(),
                },
            },
        },
    );
    Ok(join_node)
}

fn policy_branch_semantic_label(
    filters: &[crate::query::Predicate],
    joins: &[crate::query::JoinVia],
    reachable: &[crate::query::ReachableVia],
    inherits: &[crate::query::InheritsVia],
) -> Result<String, Error> {
    let bytes = postcard::to_allocvec(&(filters, joins, reachable, inherits)).map_err(|error| {
        Error::QueryLowering(format!(
            "policy branch fingerprint encoding failed: {error}"
        ))
    })?;
    Ok(format!("policy:{}", blake3::hash(&bytes).to_hex()))
}

fn normalize_row_id_projection(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    input: RowSetNodeId,
    root_source: &SourceId,
    node_id: RowSetNodeId,
) -> RowSetNodeId {
    nodes.insert(
        node_id.clone(),
        RowSetExpr::Project {
            input,
            columns: vec![RowProjection {
                output: TypedOutputField {
                    name: "row_uuid".to_owned(),
                    ty: ColumnType::Uuid,
                },
                value: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
            }],
        },
    );
    node_id
}

fn unsupported_policy_branch_reason(query: &JazzQuery) -> Option<String> {
    let _ = query;
    None
}

fn policy_branch_base_is_converter_false(query: &JazzQuery) -> bool {
    matches!(query.filters.as_slice(), [Predicate::Any(predicates)] if predicates.is_empty())
        && query.joins.is_empty()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
}

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
                adds: current.difference(&previous).cloned().collect(),
                removes: previous.difference(&current).cloned().collect(),
                result_payload_adds,
                result_payload_removes,
                program_fact_adds,
                program_fact_removes,
                structured_app_row_changes: BTreeSet::new(),
                allow_storage_witness_fallback: true,
                observed_delta_batches: 0,
                observed_result_delta_batches: 0,
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
        self.apply_query_order(shape.query(), &mut rows)?;
        if shape.query().flat_join.is_none() {
            self.apply_projection_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        }
        let root_count = rows.len();
        let mut edges = Vec::new();
        for fact in program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            edges.push(RelationEdge {
                source_table: edge.source_table.to_string(),
                source_row: edge.source_row,
                relation: edge.path.clone(),
                target_table: edge.target_table.to_string(),
                target_row: edge.target_row,
            });
            if row_keys.insert((edge.target_table.to_string(), edge.target_row))
                && let Some(version) = &edge.target_version
                && let Some(row) = self.materialize_authoritative_reset_version_row(
                    edge.target_table.as_str(),
                    edge.target_row,
                    version.tx,
                    None,
                )?
            {
                rows.push(row);
            }
        }
        Ok(Some(RelationSnapshot {
            root_count,
            rows,
            edges,
        }))
    }

    fn materialize_authoritative_reset_member(
        &mut self,
        query: &crate::query::Query,
        member: &ResultMemberEntry,
        result_payloads: &BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            query.table.as_str(),
            query.aggregate.is_some(),
        ) && let Some(payload) = result_payloads.get(member)
        {
            return self
                .current_row_from_aggregate_result_payload(query, member, payload)
                .map(Some);
        }
        if (query.flat_join.is_some() || member.as_row().is_none())
            && let Some(payload) = result_payloads.get(member)
        {
            let Some(table_name) = member.table_name() else {
                return Err(Error::InvalidStoredValue(
                    "result payload member must name a table",
                ));
            };
            let table = self.table(table_name)?.clone();
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }

        let Some((table_name, row_uuid, tx_id)) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "authoritative reset cannot materialize non-row result without payload",
            ));
        };
        if let Some(row) =
            self.materialize_authoritative_reset_current_row(table_name.as_str(), row_uuid)?
        {
            return Ok(Some(row));
        }
        self.materialize_authoritative_reset_version_row(table_name.as_str(), row_uuid, tx_id, None)
    }

    fn materialize_authoritative_reset_current_row(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let schema_version = self.catalogue.current_schema_version_id;
        let table_id = self.physical_table_id_for_schema(schema_version, table_name)?;
        let content_graph = self.physical_current_source_scan_graph(
            schema_version,
            table_name,
            PhysicalCurrentClass::Global,
            StaticScanSpec::Point(vec![groove::ivm::LiteralValue::from(Value::Uuid(
                row_uuid.0,
            ))]),
        )?;
        let content = self
            .database
            .query_graph(content_graph)
            .map_err(|error| Self::malformed_current_query_error(table_name, row_uuid, error))?;
        let Some(content_delta) = content.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let content_record = BorrowedRecord::new(&content_delta.record, &content.descriptor);
        let content_tx = self.current_record_sort_key(table_name, row_uuid, content_record)?;
        if let Some(deletion_raw) = self.database.primary_key_get_raw(
            &physical_register_global_current_table_name(table_id),
            &[Value::Uuid(row_uuid.0)],
        )? {
            let deletion_record = deletion_raw.record();
            let deletion_tx =
                self.current_record_sort_key(table_name, row_uuid, deletion_record)?;
            let deletion = deletion_event_from_value(
                deletion_record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?;
            if deletion_tx > content_tx && deletion == DeletionEvent::Deleted {
                return Ok(None);
            }
        }
        let row = decode_current_row(&table, content_record)?;
        self.materialize_current_row(&table, row).map(Some)
    }

    fn materialize_authoritative_reset_version_row(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        projection: Option<&[String]>,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let Some(tx_node_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Err(Error::MissingTransaction(tx_id));
        };
        let Some(version) = self.query_version_by_alias(
            table_name,
            row_uuid,
            VersionLayer::Content,
            tx_id.time,
            tx_node_alias,
        )?
        else {
            if self.query_transaction(tx_id)?.is_some() {
                return Ok(None);
            }
            return Err(Error::MissingTransaction(tx_id));
        };
        let mut row = self.current_row_from_materialized_version(&table, &version)?;
        if let Some(columns) = projection {
            row = row.project(&table, columns)?;
        }
        Ok(Some(row))
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

    fn one_shot_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        self.current_query_access_paths(shape, binding, tier)
    }

    fn current_query_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        if tier != DurabilityTier::Global {
            return Ok(BTreeMap::new());
        }
        let query = shape.query();
        if !query.joins.is_empty()
            || !query.policy_branches.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
        {
            return Ok(BTreeMap::new());
        }
        let mut access_paths = self.current_query_primary_key_access_paths(shape, binding)?;
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        let equalities = root_literal_equalities(query, binding)?;
        let Some(access_path) = select_current_access_path(&table, &equalities) else {
            return Ok(access_paths);
        };
        access_paths.insert(root_source_id(&query.table), access_path);
        self.add_reachable_access_paths(query, shape.schema_version(), binding, &mut access_paths)?;
        Ok(access_paths)
    }

    fn current_query_primary_key_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        let query = shape.query();
        let mut access_paths = BTreeMap::new();
        let equalities = root_literal_equalities(query, binding)?;
        if let Some(value) = equalities.get("id").cloned() {
            access_paths.insert(
                root_source_id(&query.table),
                CurrentAccessPath::PrimaryKey(vec![value]),
            );
        }
        self.add_reachable_access_paths(query, shape.schema_version(), binding, &mut access_paths)?;
        Ok(access_paths)
    }

    fn add_reachable_access_paths(
        &self,
        query: &JazzQuery,
        schema_version: SchemaVersionId,
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        for (index, reachable) in query.reachable.iter().enumerate() {
            let reachable_id = format!("reachable:{index}");
            if let Some(seed) = &reachable.seed {
                let source = reachable_seed_source_id(seed, &reachable_id);
                self.add_primary_key_access_path_for_filters(
                    &source,
                    &seed.table,
                    schema_version,
                    &seed.filters,
                    binding,
                    access_paths,
                )?;
            }
            let edge_source = reachable_edge_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &edge_source,
                &reachable.edge_table,
                schema_version,
                &reachable.edge_filters,
                binding,
                access_paths,
            )?;
            let access_source = reachable_access_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &access_source,
                &reachable.access_table,
                schema_version,
                &reachable.access_filters,
                binding,
                access_paths,
            )?;
        }
        Ok(())
    }

    fn add_primary_key_access_path_for_filters(
        &self,
        source: &SourceId,
        table_name: &str,
        schema_version: SchemaVersionId,
        filters: &[Predicate],
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        let table = self.table_in_schema(table_name, schema_version)?;
        let equalities = literal_equalities_for_filters(filters, binding)?;
        if let Some(value) = equalities.get("id").cloned() {
            access_paths.insert(source.clone(), CurrentAccessPath::PrimaryKey(vec![value]));
        } else if let Some(access_path) = select_current_access_path(&table, &equalities)
            && matches!(access_path, CurrentAccessPath::PrimaryKey(_))
        {
            access_paths.insert(source.clone(), access_path);
        }
        Ok(())
    }

    fn physical_global_current_source_for_index_scan(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        projection_target: &str,
    ) -> Result<GraphBuilder, Error> {
        self.physical_global_current_source_for_index_scan_with_output(
            table,
            schema_version,
            column,
            prefix,
            projection_target,
            table.global_current_storage_tables()[0].record_schema(),
        )
    }

    fn physical_global_current_source_for_index_scan_with_output(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        projection_target: &str,
        output: RecordDescriptor,
    ) -> Result<GraphBuilder, Error> {
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(&table.name))
            .ok_or(Error::InvalidStoredValue(
                "physical current index table mapping missing",
            ))?;
        let column_id = mapping
            .columns
            .get(column)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current index column mapping missing",
            ))?;
        let storage_table = physical_global_current_table_name(mapping.table_id);
        let indexed = self.database.index_scan_raw(
            &storage_table,
            &physical_current_index_name(column_id),
            prefix,
        )?;
        let mut records = Vec::with_capacity(indexed.len());
        for raw in indexed {
            let variant_tag = raw.variant_tag();
            let record = groove::records::VariantRecord::new(variant_tag, raw.owned_record());
            if let Some(projected) =
                self.database
                    .project_variant_record(&storage_table, projection_target, &record)?
            {
                records.push(projected.raw().to_vec());
            }
        }
        Ok(GraphBuilder::inline_records(output, records))
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

    fn compile_query_program_request(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_access_paths(request, BTreeMap::new())
    }

    fn compile_query_program_request_with_access_paths(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_inline_sources_and_access_paths(
            request,
            BTreeMap::new(),
            access_paths,
        )
    }

    fn compile_query_program_request_with_inline_sources_and_access_paths(
        &mut self,
        request: QueryProgramRequest,
        inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        let trace_request = capability_trace_enabled().then(|| request.clone());
        let read_view = request.reads.primary.clone();
        let mut resolver = CurrentQuerySourceResolver {
            node: self,
            read_view: &read_view,
            inline_sources,
            access_paths,
            current_projection_targets: BTreeMap::new(),
        };
        let node_uuid = resolver.node.node_uuid;
        let node_alias = resolver.node.self_node_alias;
        let result = lower_query_program(request, &mut resolver);
        if let Some(request) = trace_request {
            trace_capability_compile(
                node_uuid,
                node_alias,
                &request,
                result.as_ref().map_err(|report| report.as_ref()),
            );
        }
        result.map_err(|report| Error::QueryCapability(format!("{report:?}")))
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

    fn collect_policy_dependency_claim_params(
        &self,
        schema: &JazzSchema,
        policy: &PolicyContext,
        input: &NormalizedRowSetShape,
        params: &mut BTreeMap<String, ProgramClaimParam>,
    ) -> Result<(), Error> {
        let claims = match policy {
            PolicyContext::Identity { claims, .. }
            | PolicyContext::AuthorizationSubplan { claims, .. } => claims,
            PolicyContext::System => return Ok(()),
        };
        for table_name in normalized_source_tables(input) {
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
            let mut query = authorization_query_from_read_policy(table);
            let mut values = BTreeMap::new();
            bind_scope_claim_operands(&mut query, claims, &mut values);
            for (name, claim) in disambiguate_policy_claim_params(&mut query, schema, &mut values)?
            {
                // The root policy may rediscover the same claim slot while
                // walking its source tables. Keep the already-lowered slot in
                // that case; a typed alias is only needed when the same claim
                // path is required at a genuinely different schema type.
                if params
                    .values()
                    .any(|existing| existing.path == claim.path && existing.ty == claim.ty)
                {
                    continue;
                }
                params.insert(name, claim);
            }
        }
        Ok(())
    }

    fn normalized_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        _binding: &Binding,
    ) -> Result<NormalizedRowSetShape, Error> {
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
        let query = shape.query();
        let root_source = root_source_id(&query.table);
        let (mut auxiliary_sources, closure_paths) =
            collect_closure_paths(self, &query.table, shape.schema_version(), &query.includes)?;
        let source_node = RowSetNodeId("root".to_owned());
        let mut nodes = BTreeMap::from([(
            source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        )]);
        let mut current = source_node;
        let mut join_contributions = Vec::new();
        let mut reachable_contributions = Vec::new();
        let inheritance_path = InheritanceExpansionPath::default();

        let binding_source_shape = PENDING_BINDING_SOURCE_SHAPE.to_owned();
        let unsupported_policy_branch = unsupported_policy_branch_reason(query);
        if unsupported_policy_branch.is_none() && !query.policy_branches.is_empty() {
            let mut union_inputs = Vec::new();
            if !policy_branch_base_is_converter_false(query) {
                let base_source_node = RowSetNodeId("policy_branch:base:root".to_owned());
                nodes.insert(
                    base_source_node.clone(),
                    RowSetExpr::Source {
                        source: root_source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                let base = normalize_policy_atom_chain(
                    &mut nodes,
                    &mut auxiliary_sources,
                    &mut join_contributions,
                    &mut reachable_contributions,
                    schema,
                    &root_source,
                    base_source_node,
                    "policy_branch:base",
                    PolicyAtomChain {
                        filters: &query.filters,
                        joins: &query.joins,
                        inherits: &query.inherits,
                        reachable: &query.reachable,
                    },
                    &binding_source_shape,
                    shape.params(),
                    false,
                    &inheritance_path,
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        base,
                        &root_source,
                        RowSetNodeId("policy_branch:base:row_id".to_owned()),
                    ),
                    label: policy_branch_semantic_label(
                        &query.filters,
                        &query.joins,
                        &query.reachable,
                        &query.inherits,
                    )?,
                });
            }

            for (index, branch) in query.policy_branches.iter().enumerate() {
                let branch_source_node = RowSetNodeId(format!("policy_branch:{index}:root"));
                nodes.insert(
                    branch_source_node.clone(),
                    RowSetExpr::Source {
                        source: root_source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                let branch_current = normalize_policy_atom_chain(
                    &mut nodes,
                    &mut auxiliary_sources,
                    &mut join_contributions,
                    &mut reachable_contributions,
                    schema,
                    &root_source,
                    branch_source_node,
                    &format!("policy_branch:{index}"),
                    PolicyAtomChain {
                        filters: &branch.filters,
                        joins: &branch.joins,
                        inherits: &branch.inherits,
                        reachable: &branch.reachable,
                    },
                    &binding_source_shape,
                    shape.params(),
                    false,
                    &inheritance_path,
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        branch_current,
                        &root_source,
                        RowSetNodeId(format!("policy_branch:{index}:row_id")),
                    ),
                    label: policy_branch_semantic_label(
                        &branch.filters,
                        &branch.joins,
                        &branch.reachable,
                        &branch.inherits,
                    )?,
                });
            }

            let union_node = RowSetNodeId("policy_branch:authorized_rows".to_owned());
            nodes.insert(
                union_node.clone(),
                RowSetExpr::Union {
                    inputs: union_inputs,
                },
            );
            let join_node = RowSetNodeId("policy_branch:authorize".to_owned());
            nodes.insert(
                join_node.clone(),
                RowSetExpr::Join {
                    left: current,
                    right: union_node,
                    mode: NormalizedJoinMode::Inner,
                    on: NormalizedPredicateExpr::Compare {
                        left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                        op: NormalizedComparisonOp::Eq,
                        right: NormalizedValueRef::SourceField {
                            source: root_source.clone(),
                            field: "row_uuid".to_owned(),
                        },
                    },
                },
            );
            current = join_node;
        } else {
            current = normalize_policy_atom_chain(
                &mut nodes,
                &mut auxiliary_sources,
                &mut join_contributions,
                &mut reachable_contributions,
                schema,
                &root_source,
                current,
                "query",
                PolicyAtomChain {
                    filters: &query.filters,
                    joins: &query.joins,
                    inherits: &query.inherits,
                    reachable: &query.reachable,
                },
                &binding_source_shape,
                shape.params(),
                true,
                &inheritance_path,
            )?;
        }

        // Flat joins are an output form separate from `JoinVia`. Every input
        // stays a normal source, so read-policy filtering and read-view/lens
        // projection happen before Groove's inner JoinOp combines records.
        if let Some(flat_join) = &query.flat_join {
            let root_name = flat_join
                .root_alias
                .as_deref()
                .unwrap_or(query.table.as_str())
                .to_owned();
            let mut sources = BTreeMap::from([(root_name, root_source.clone())]);
            let mut tuple_sources = vec![root_source.clone()];
            let mut output_sources = vec![(
                flat_join
                    .root_alias
                    .as_deref()
                    .unwrap_or(query.table.as_str())
                    .to_owned(),
                root_source.clone(),
            )];

            for (index, join) in flat_join.sources.iter().enumerate() {
                let name = join
                    .alias
                    .as_deref()
                    .unwrap_or(join.table.as_str())
                    .to_owned();
                let source = SourceId {
                    table: join.table.clone(),
                    path: SourcePath {
                        components: vec![SourceRole::Alias(format!("flat_join:{index}:{name}"))],
                    },
                };
                let source_node = RowSetNodeId(format!("flat_join:{index}:source"));
                nodes.insert(
                    source_node.clone(),
                    RowSetExpr::Source {
                        source: source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                auxiliary_sources.insert(source.clone());
                let value_ref = |field: &str| -> Result<NormalizedValueRef, Error> {
                    let (scope, column) = field.rsplit_once('.').ok_or_else(|| {
                        Error::QueryCapability(format!(
                            "flat join field must be qualified: {field}"
                        ))
                    })?;
                    let source = sources.get(scope).ok_or_else(|| {
                        Error::QueryCapability(format!("unknown flat join source {scope}"))
                    })?;
                    Ok(if column == "id" || column == "_id" {
                        NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
                    } else {
                        NormalizedValueRef::SourceField {
                            source: source.clone(),
                            field: column.to_owned(),
                        }
                    })
                };
                let (_, right_column) = join.on.right.rsplit_once('.').ok_or_else(|| {
                    Error::QueryCapability(format!(
                        "flat join field must be qualified: {}",
                        join.on.right
                    ))
                })?;
                let join_node = RowSetNodeId(format!("flat_join:{index}:join"));
                nodes.insert(
                    join_node.clone(),
                    RowSetExpr::Join {
                        left: current,
                        right: source_node,
                        mode: NormalizedJoinMode::Inner,
                        on: NormalizedPredicateExpr::Compare {
                            left: value_ref(&join.on.left)?,
                            op: NormalizedComparisonOp::Eq,
                            right: if right_column == "id" || right_column == "_id" {
                                NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
                            } else {
                                NormalizedValueRef::SourceField {
                                    source: source.clone(),
                                    field: right_column.to_owned(),
                                }
                            },
                        },
                    },
                );
                current = join_node;
                sources.insert(name.clone(), source.clone());
                output_sources.push((name, source.clone()));
                tuple_sources.push(source);
            }

            let projection_node = RowSetNodeId("flat_join:output".to_owned());
            let mut columns = Vec::new();
            for (position, source) in tuple_sources.iter().enumerate() {
                columns.push(RowProjection {
                    output: TypedOutputField {
                        name: if position == 0 {
                            "row_uuid".to_owned()
                        } else {
                            format!("__flat_join_row_{position}")
                        },
                        ty: ColumnType::Uuid,
                    },
                    value: NormalizedValueRef::RowId(RowIdRef::Source(source.clone())),
                });
            }
            // Retain the representative root version used by the existing
            // real-row membership envelope. Joined source versions stay in
            // their own source terminals; the rendered tuple itself is kept
            // in the membership payload below.
            for (name, ty) in [
                ("tx_time", ColumnType::U64),
                ("tx_node_id", ColumnType::U64),
            ] {
                columns.push(RowProjection {
                    output: TypedOutputField {
                        name: name.to_owned(),
                        ty,
                    },
                    value: NormalizedValueRef::SourceField {
                        source: root_source.clone(),
                        field: name.to_owned(),
                    },
                });
            }
            for (name, source) in &output_sources {
                let source_schema = schema
                    .tables
                    .iter()
                    .find(|table| table.name == source.table)
                    .ok_or_else(|| {
                        Error::QueryCapability(format!("unknown flat join table {}", source.table))
                    })?;
                for column in source_schema.columns.iter() {
                    columns.push(RowProjection {
                        output: TypedOutputField {
                            name: format!("{name}.{}", column.name),
                            ty: column.column_type.clone(),
                        },
                        value: NormalizedValueRef::SourceField {
                            source: source.clone(),
                            field: column.name.clone(),
                        },
                    });
                }
            }
            nodes.insert(
                projection_node.clone(),
                RowSetExpr::Project {
                    input: current,
                    columns,
                },
            );
            current = projection_node;
        }

        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            current = normalize_array_subquery(
                &mut nodes,
                current,
                schema,
                &root_source,
                subquery,
                &[index],
            )?;
        }

        if query.aggregate.is_none() && !query.order_by.is_empty() {
            let order_node = RowSetNodeId("order".to_owned());
            nodes.insert(
                order_node.clone(),
                RowSetExpr::OrderBy {
                    input: current,
                    keys: query
                        .order_by
                        .iter()
                        .map(|order| normalize_order_key(&root_source, order))
                        .collect::<Result<Vec<_>, Error>>()?,
                },
            );
            current = order_node;
        }
        if query.aggregate.is_none() && (query.limit.is_some() || query.offset != 0) {
            let slice_node = RowSetNodeId("slice".to_owned());
            nodes.insert(
                slice_node.clone(),
                RowSetExpr::Slice {
                    input: current,
                    partition_by: Vec::new(),
                    limit: query.limit.map(|limit| limit.min(u32::MAX as usize) as u32),
                    offset: query.offset.min(u32::MAX as usize) as u32,
                    tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                        root_source.clone(),
                    ))],
                    rank_output: None,
                },
            );
            current = slice_node;
        }

        if let Some(marker) = unsupported_policy_branch {
            let node = RowSetNodeId("unsupported:policy_branches".to_owned());
            nodes.insert(
                node.clone(),
                RowSetExpr::Distinct {
                    input: current,
                    keys: vec![NormalizedValueRef::Literal(marker.into_bytes())],
                },
            );
            current = node;
        }

        if let Some(aggregate) = &query.aggregate {
            let aggregate_node = RowSetNodeId("aggregate".to_owned());
            nodes.insert(
                aggregate_node.clone(),
                RowSetExpr::Aggregate {
                    input: current,
                    group_by: normalized_aggregate_group_by(&root_source, aggregate)?,
                    outputs: normalized_aggregate_outputs(&root_source, aggregate)?,
                },
            );
            current = aggregate_node;
        }

        let mut normalized = NormalizedRowSetShape {
            identity: NormalizedShapeIdentity {
                shape_id: shape.shape_id(),
                canonical: shape.canonical_bytes().to_vec(),
            },
            root: current,
            result: ResultId::RealRow {
                table: query.table.clone(),
                row: ResultRowRef::Source(root_source),
            },
            auxiliary_sources,
            closure_paths,
            join_contributions,
            reachable_contributions,
            nodes,
        };
        let claim_params = binding_claim_params_for_shape(&normalized, shape.params());
        let binding_source_shape =
            query_binding_source_shape_for_parts(shape.params(), &claim_params);
        retarget_binding_value_sources(&mut normalized, &binding_source_shape);
        Ok(normalized)
    }

    fn normalized_include_deleted_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<NormalizedRowSetShape, Error> {
        let mut normalized = self.normalized_row_set_shape(shape, binding)?;
        let root_source = root_source_id(&shape.query().table);
        for node in normalized.nodes.values_mut() {
            if let RowSetExpr::Source { source, visibility } = node
                && *source == root_source
            {
                *visibility = RowVisibility::IncludeDeleted;
            }
        }
        Ok(normalized)
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
            self.finish_engine_query_rows(query, &mut rows)?;
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
        // physical provenance fields so large values and policy witnesses can
        // be resolved above.  Do not let that internal descriptor cross the
        // public CurrentRow boundary: subscriptions use the public terminal
        // shape, and native/WASM consumers must see the same layout from both
        // read paths.
        if shape.query().flat_join.is_none() {
            normalize_public_current_rows(&table_schema, &mut rows)?;
        }
        let query = shape.query();
        self.finish_engine_query_rows(query, &mut rows)?;
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
        self.finish_engine_query_rows(query, &mut rows)?;
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
                    // Upstream client coverage is canonicalized to Global.
                    tier: DurabilityTier::Global,
                    read_view: read_view.clone(),
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
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(row_result_set) = self.query.settled_result_sets.get(&binding_view) else {
            return Ok(Vec::new());
        };
        let mut row_entries = row_result_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .filter(|(entry_table, _, _)| entry_table.as_str() == table)
            .map(|(_, row_uuid, tx_id)| (row_uuid, tx_id))
            .collect::<BTreeSet<_>>();
        if let Some(program_facts) = self.query.settled_program_facts.get(&binding_view) {
            row_entries.extend(program_facts.iter().filter_map(|fact| {
                let ProgramFactEntry::RelationEdge(edge) = fact else {
                    return None;
                };
                (edge.target_table.as_str() == table)
                    .then(|| {
                        edge.target_version
                            .as_ref()
                            .map(|version| (edge.target_row, version.tx))
                    })
                    .flatten()
            }));
        }
        let read_table = self.table_in_schema(table, read_schema)?.clone();
        let mut rows = Vec::with_capacity(row_entries.len());
        for (row_uuid, tx_id) in row_entries {
            let tx_node_alias = self
                .node_aliases
                .get(&tx_id.node)
                .copied()
                .ok_or(Error::MissingTransaction(tx_id))?;
            let version = self
                .query_version_by_alias(
                    table,
                    row_uuid,
                    VersionLayer::Content,
                    tx_id.time,
                    tx_node_alias,
                )?
                .ok_or(Error::MissingTransaction(tx_id))?;
            let authored_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "settled view row schema version alias missing",
                ))?;
            let authored_table = self
                .table_in_schema(version.table(), authored_schema)?
                .clone();
            let mut cells = self.materialized_cells_for_version(&authored_table, &version)?;
            let Some(projected_table) =
                self.translate_cells(authored_schema, read_schema, version.table(), &mut cells)?
            else {
                continue;
            };
            if projected_table == table {
                rows.push(current_row_from_materialized_cells(
                    &read_table,
                    &version,
                    &cells,
                )?);
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
        self.finish_engine_query_rows(query, &mut rows)?;
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

    fn materialize_aggregate_query_rows(
        &mut self,
        query: &crate::query::Query,
        _table: &TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, _weight) in deltas.iter().filter(|(_, weight)| *weight > 0) {
            rows.push(aggregate_current_row_from_record(
                &query.table,
                aggregate_query_row_uuid(query, &record)?,
                &record,
            )?);
        }
        Ok(rows)
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
            binding_view_key: settled_binding_view.unwrap_or_else(|| {
                BindingViewKey::new(
                    shape.shape_id(),
                    binding.binding_id(),
                    RegisterShapeOptions {
                        tier,
                        read_view: read_view.clone(),
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
                let remote_occurrences = remote_members
                    .iter()
                    .filter_map(ResultMemberEntry::output_occurrence_id)
                    .collect::<BTreeSet<_>>();
                for entry in local.authoritative_result_set.difference(&remote_members) {
                    // A content-version replacement is still the same
                    // authorized output occurrence. Membership reconciliation
                    // must not retract the locally maintained row while its
                    // newer payload is being applied.
                    let remains_authoritative = entry
                        .output_occurrence_id()
                        .is_some_and(|occurrence| remote_occurrences.contains(&occurrence));
                    if !remains_authoritative && local.result_set.contains(entry) {
                        states.insert(entry.clone(), (true, false));
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
                    removed_edges.push(RelationEdge {
                        source_table: edge.source_table.to_string(),
                        source_row: edge.source_row,
                        relation: edge.path,
                        target_table: edge.target_table.to_string(),
                        target_row: edge.target_row,
                    });
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
                let relation_edge = RelationEdge {
                    source_table: edge.source_table.to_string(),
                    source_row: edge.source_row,
                    relation: edge.path.clone(),
                    target_table: edge.target_table.to_string(),
                    target_row: edge.target_row,
                };
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
            added,
            removed,
            added_edges,
            removed_edges,
            terminal_operations,
            terminal_layout,
        })
    }

    pub(crate) fn materialize_local_maintained_relation_snapshot(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<RelationSnapshot, Error> {
        Ok(self
            .materialize_local_maintained_relation_snapshot_with_occurrences(local)?
            .snapshot)
    }

    fn materialize_local_maintained_relation_snapshot_with_occurrences(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<LocalMaintainedRelationSnapshot, Error> {
        if !local.result_query.array_subqueries.is_empty() {
            let mut rows = local
                .maintained
                .structured_app_rows()
                .into_iter()
                .map(|(_, record)| CurrentRow::new(local.result_table.clone(), record))
                .collect::<Vec<_>>();
            self.apply_query_order(&local.result_query, &mut rows)?;
            let root_occurrence_ids = rows
                .iter()
                .map(|row| OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0)))
                .collect();
            return Ok(LocalMaintainedRelationSnapshot {
                snapshot: RelationSnapshot {
                    root_count: rows.len(),
                    rows,
                    edges: Vec::new(),
                },
                root_occurrence_ids,
            });
        }
        let mut cache = self.preload_local_maintained_materialization_cache(local)?;
        let mut rows = Vec::with_capacity(local.result_set.len());
        let mut root_occurrence_ids = Vec::with_capacity(local.result_set.len());
        let mut row_keys = BTreeSet::new();
        for member in &local.result_set {
            if let Some(row) = self.materialize_local_maintained_view_result_member_with_cache(
                local, member, &mut cache,
            )? {
                let occurrence_id = public_result_member_occurrence_id(
                    member,
                    local.result_table.as_str(),
                    local.result_query.aggregate.is_some(),
                )?
                .ok_or(Error::InvalidStoredValue(
                    "maintained root member has no occurrence identity",
                ))?;
                row_keys.insert((row.table().to_owned(), row.row_uuid()));
                rows.push(row);
                root_occurrence_ids.push(occurrence_id);
            }
        }
        // `result_set` is keyed by member identity, so its BTreeSet iteration
        // order cannot be exposed as a query reset order. Do not re-window: the
        // maintained program already chose this result set. Materialize full
        // rows first, because an order key may not be in the public projection.
        self.apply_query_order_with_occurrences(
            &local.result_query,
            &mut rows,
            &mut root_occurrence_ids,
        )?;
        if local.result_query.aggregate.is_some() {
            root_occurrence_ids = rows
                .iter()
                .map(|row| OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0)))
                .collect();
        }
        self.apply_projection(&local.result_query, &mut rows)?;
        let root_count = rows.len();
        let mut edges = Vec::with_capacity(local.program_facts.len());
        for fact in &local.program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            edges.push(RelationEdge {
                source_table: edge.source_table.to_string(),
                source_row: edge.source_row,
                relation: edge.path.clone(),
                target_table: edge.target_table.to_string(),
                target_row: edge.target_row,
            });
            if row_keys.insert((edge.target_table.to_string(), edge.target_row))
                && let Some(version) = &edge.target_version
                && let Some(row) = self
                    .materialize_local_maintained_view_relation_edge_row_with_cache(
                        local,
                        edge.target_table.as_str(),
                        edge.target_row,
                        version.tx,
                        &mut cache,
                    )?
            {
                rows.push(row);
            }
        }
        Ok(LocalMaintainedRelationSnapshot {
            snapshot: RelationSnapshot {
                root_count,
                rows,
                edges,
            },
            root_occurrence_ids,
        })
    }

    fn preload_local_maintained_materialization_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<LocalMaintainedMaterializationCache, Error> {
        let mut cache = LocalMaintainedMaterializationCache::default();
        let mut tx_ids = BTreeSet::new();
        for member in &local.result_set {
            let Some((_, _, tx_id)) = member.as_row() else {
                continue;
            };
            tx_ids.insert(tx_id);
            cache
                .tx_versions
                .entry(tx_id)
                .or_insert_with(|| local.maintained.versions_by_tx(tx_id));
        }
        for fact in &local.program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            let Some(version) = &edge.target_version else {
                continue;
            };
            tx_ids.insert(version.tx);
            cache
                .tx_versions
                .entry(version.tx)
                .or_insert_with(|| local.maintained.versions_by_tx(version.tx));
        }
        self.preload_tx_versions_for_materialization(tx_ids, &mut cache.tx_versions)?;
        Ok(cache)
    }

    fn materialize_local_maintained_view_relation_edge_row(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let tx_versions = local.maintained.versions_by_tx(tx_id);
        let Some(version) =
            local_maintained_view_content_witness(&tx_versions, table_name, row_uuid)
        else {
            return Ok(None);
        };
        self.current_row_from_materialized_version(&table, version)
            .map(Some)
    }

    fn materialize_local_maintained_view_relation_edge_row_with_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let tx_versions = self.local_maintained_tx_versions(local, tx_id, cache);
        let Some(version) =
            local_maintained_view_content_witness(tx_versions, table_name, row_uuid)
        else {
            return Ok(None);
        };
        let version = version.clone();
        self.current_row_from_materialized_version_with_materialization_cache(
            &table, &version, cache,
        )
        .map(Some)
    }

    fn materialize_local_maintained_view_result_member(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        member: &ResultMemberEntry,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            local.result_table.as_str(),
            local.result_query.aggregate.is_some(),
        ) {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "aggregate result member is missing its payload",
                ))?;
            return self
                .current_row_from_aggregate_result_payload(&local.result_query, member, payload)
                .map(Some);
        }
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        let table = self.table(entry.0.as_str())?.clone();
        if local.result_query.flat_join.is_some() {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "flat joined result member is missing its tuple payload",
                ))?;
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }
        if local.result_select.is_some()
            && let Some(payload) = local.result_payloads.get(member)
        {
            let mut row = self.current_row_from_result_payload(&table, payload)?;
            if let Some(columns) = &local.result_select {
                row = row.project(&table, columns)?;
            }
            return Ok(Some(row));
        }
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
        let mut row = self.current_row_from_materialized_version(&table, &version)?;
        if let Some(columns) = &local.result_select {
            row = row.project(&table, columns)?;
        }
        Ok(Some(row))
    }

    fn materialize_local_maintained_view_result_member_with_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        member: &ResultMemberEntry,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            local.result_table.as_str(),
            local.result_query.aggregate.is_some(),
        ) {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "aggregate result member is missing its payload",
                ))?;
            return self
                .current_row_from_aggregate_result_payload(&local.result_query, member, payload)
                .map(Some);
        }
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        let table = self.table(entry.0.as_str())?.clone();
        if local.result_query.flat_join.is_some() {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "flat joined result member is missing its tuple payload",
                ))?;
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }
        let tx_versions = self.local_maintained_tx_versions(local, entry.2, cache);
        let version = if let Some(version) =
            local_maintained_view_content_witness(tx_versions, entry.0.as_str(), entry.1)
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
            let tx_versions = cache.tx_versions.entry(entry.2).or_default();
            tx_versions.push(content_winner);
            tx_versions
                .last()
                .ok_or(Error::MissingTransaction(entry.2))?
                .clone()
        };
        self.current_row_from_materialized_version_with_materialization_cache(
            &table, &version, cache,
        )
        .map(Some)
    }

    fn local_maintained_tx_versions<'a>(
        &'a mut self,
        local: &LocalMaintainedViewSubscription,
        tx_id: TxId,
        cache: &'a mut LocalMaintainedMaterializationCache,
    ) -> &'a [VersionRow] {
        cache
            .tx_versions
            .entry(tx_id)
            .or_insert_with(|| local.maintained.versions_by_tx(tx_id))
            .as_slice()
    }

    fn preload_tx_versions_for_materialization(
        &mut self,
        tx_ids: impl IntoIterator<Item = TxId>,
        cache: &mut BTreeMap<TxId, Vec<VersionRow>>,
    ) -> Result<(), Error> {
        let mut by_alias = BTreeMap::<(NodeUuid, NodeAlias), BTreeSet<TxTime>>::new();
        for tx_id in tx_ids {
            if cache
                .get(&tx_id)
                .is_some_and(|versions| !versions.is_empty())
            {
                continue;
            }
            if let Some(versions) = self.cached_tx_versions(tx_id) {
                cache.insert(tx_id, versions);
                continue;
            }
            if let Some(alias) = self.node_aliases.get(&tx_id.node).copied() {
                by_alias
                    .entry((tx_id.node, alias))
                    .or_default()
                    .insert(tx_id.time);
                cache.entry(tx_id).or_default();
            }
        }

        if by_alias.is_empty() {
            return Ok(());
        }

        let tables = self.tx_version_scan_tables();
        for ((node, alias), times) in by_alias {
            for (start, end) in contiguous_tx_time_spans(&times) {
                let Some(end) = end else {
                    let tx_id = TxId::new(start, node);
                    let versions = self.query_versions_for_tx(tx_id)?;
                    cache.insert(tx_id, versions);
                    continue;
                };
                let mut scanned_sources = BTreeSet::new();
                for table in &tables {
                    for storage_table in self.version_storage_sources(table)? {
                        if !scanned_sources.insert(storage_table.clone()) {
                            continue;
                        }
                        let raws = self
                            .database
                            .index_scan_range_raw(
                                &storage_table,
                                "by_tx",
                                &[Value::U64(start.0), Value::U64(alias.0)],
                                &[Value::U64(end.0), Value::U64(0)],
                            )?
                            .into_iter()
                            .map(|raw| raw.owned_record())
                            .collect::<Vec<_>>();
                        for record in raws {
                            let version =
                                self.decode_history_owned_record(table, &storage_table, record)?;
                            if version.tx_node_alias() != alias
                                || !times.contains(&version.tx_time())
                            {
                                continue;
                            }
                            let tx_id = TxId::new(version.tx_time(), node);
                            cache.entry(tx_id).or_default().push(version);
                        }
                    }
                }
            }
        }

        for versions in cache.values_mut() {
            versions.sort_by(|left, right| {
                left.table()
                    .cmp(right.table())
                    .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                    .then_with(|| left.layer().cmp(&right.layer()))
            });
        }
        Ok(())
    }

    fn tx_versions_for_materialization<'a>(
        &'a mut self,
        tx_id: TxId,
        cache: &'a mut LocalMaintainedMaterializationCache,
    ) -> Result<&'a [VersionRow], Error> {
        if let std::collections::btree_map::Entry::Vacant(entry) = cache.tx_versions.entry(tx_id) {
            let versions = self.query_versions_for_tx(tx_id)?;
            entry.insert(versions);
        }
        Ok(cache
            .tx_versions
            .get(&tx_id)
            .expect("tx version cache was just populated")
            .as_slice())
    }

    fn large_value_version_for_tx_with_materialization_cache(
        &mut self,
        tx_id: TxId,
        row_uuid: RowUuid,
        table_id: PhysicalTableId,
        column_id: PhysicalColumnId,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<(VersionRow, TableSchema, String, SchemaVersionId), Error> {
        let versions = self.tx_versions_for_materialization(tx_id, cache)?.to_vec();
        for version in versions {
            if version.row_uuid() != row_uuid || version.layer() != VersionLayer::Content {
                continue;
            }
            let schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "large-value ancestor schema alias is unknown",
                ))?;
            let Some(table_mapping) = self
                .catalogue
                .physical_mappings
                .get(&schema)
                .and_then(|mapping| mapping.tables.get(version.table()))
            else {
                continue;
            };
            if table_mapping.table_id != table_id {
                continue;
            }
            let Some(column) = table_mapping
                .columns
                .iter()
                .find_map(|(name, id)| (*id == column_id).then(|| name.clone()))
            else {
                continue;
            };
            let table = self.table_in_schema(version.table(), schema)?.clone();
            return Ok((version, table, column, schema));
        }
        Err(Error::MissingTransaction(tx_id))
    }

    fn current_row_from_materialized_version_with_materialization_cache(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<CurrentRow, Error> {
        if !table
            .columns
            .iter()
            .any(|column| column.large_value.is_some())
        {
            return current_row_from_version_projection(table, version);
        }
        let cells =
            self.materialized_cells_for_version_with_materialization_cache(table, version, cache)?;
        current_row_from_materialized_cells(table, version, &cells)
    }

    fn materialized_cells_for_version_with_materialization_cache(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let mut cells = BTreeMap::new();
        for column in &table.columns {
            let value = if let Some(kind) = column.large_value {
                Some(Value::Bytes(
                    self.large_value_handle_for_version_with_materialization_cache(
                        table,
                        version,
                        &column.name,
                        kind,
                        cache,
                    )?,
                ))
            } else {
                version.cell(table, &column.name)?
            };
            if let Some(value) = value {
                cells.insert(column.name.clone(), value);
            }
        }
        Ok(cells)
    }

    fn large_value_handle_for_version_with_materialization_cache(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        column: &str,
        kind: LargeValueKind,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Vec<u8>, Error> {
        let canonical = self.canonical_history_version_for_maintained_witness(version)?;
        let version = &canonical;
        let authored_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "large-value schema alias is unknown",
            ))?;
        let (authored_table, authored_column) =
            self.authored_large_value_identity(authored_schema, table, column)?;
        let authored_table_schema = self
            .table_in_schema(&authored_table, authored_schema)?
            .clone();
        let len = self.large_value_column_len_with_materialization_cache(
            &authored_table_schema,
            version,
            &authored_column,
            cache,
        )?;
        let refs = self.large_value_extent_refs_for_version_with_materialization_cache(
            &authored_table_schema,
            version,
            &authored_column,
            kind,
            cache,
        )?;
        let tx_id = self.version_tx_id(version)?;
        encode_large_value_handle(
            authored_schema,
            &authored_table,
            version.row_uuid(),
            &authored_column,
            tx_id,
            kind,
            len,
            refs,
        )
    }

    fn large_value_column_len_with_materialization_cache(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<usize, Error> {
        let mut suffix = Vec::new();
        let mut current = self.version_tx_id(winner)?;
        let mut checkpoint_len = None;
        let schema = self
            .schema_version_for_alias(winner.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "large-value schema alias is unknown",
            ))?;
        let (authored_table, authored_column) =
            self.authored_large_value_identity(schema, table, column)?;
        let (table_id, column_id) =
            self.large_value_lineage_ids(schema, &authored_table, &authored_column)?;
        loop {
            let (version, version_table, version_column, version_schema) = self
                .large_value_version_for_tx_with_materialization_cache(
                    current,
                    winner.row_uuid(),
                    table_id,
                    column_id,
                    cache,
                )?;
            if let Some(value) = self.large_value_checkpoint(
                version_schema,
                &version_table,
                version.row_uuid(),
                &version_column,
                current,
            )? {
                checkpoint_len = Some(value.len());
                break;
            }
            let parents = version.parents();
            suffix.push((version, version_table, version_column));
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut value_len = checkpoint_len.unwrap_or_default();
        for (version, version_table, version_column) in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(version_table, version_column)? else {
                continue;
            };
            match column_large_value_kind(version_table, version_column)? {
                LargeValueKind::Text => {
                    let op = self.decode_text_storage_op(&payload)?;
                    let value = vec![0; value_len];
                    value_len = op
                        .apply(&value)
                        .map_err(|_| Error::InvalidStoredValue("invalid text op payload"))?
                        .len();
                }
                LargeValueKind::Blob => {
                    for op in text_oplog::decode(&payload)? {
                        match op {
                            TextOp::Insert { content, .. } => {
                                value_len =
                                    value_len.checked_add(text_content_len(&content)?).ok_or(
                                        Error::InvalidStoredValue("large value length overflow"),
                                    )?;
                            }
                            TextOp::Delete { len, .. } => {
                                value_len = value_len.checked_sub(len).ok_or(
                                    Error::InvalidStoredValue("large value length underflow"),
                                )?;
                            }
                        }
                    }
                }
            }
        }
        Ok(value_len)
    }

    fn large_value_extent_refs_for_version_with_materialization_cache(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
        kind: LargeValueKind,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Vec<content_store::Extent>, Error> {
        let mut suffix = Vec::new();
        let mut current = self.version_tx_id(winner)?;
        let schema = self
            .schema_version_for_alias(winner.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "large-value schema alias is unknown",
            ))?;
        let (authored_table, authored_column) =
            self.authored_large_value_identity(schema, table, column)?;
        let (table_id, column_id) =
            self.large_value_lineage_ids(schema, &authored_table, &authored_column)?;
        loop {
            let (version, version_table, version_column, _) = self
                .large_value_version_for_tx_with_materialization_cache(
                    current,
                    winner.row_uuid(),
                    table_id,
                    column_id,
                    cache,
                )?;
            let parents = version.parents();
            suffix.push((version, version_table, version_column));
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut refs = Vec::new();
        for (version, version_table, version_column) in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(version_table, version_column)? else {
                continue;
            };
            match kind {
                LargeValueKind::Text => {
                    if let Some(extent_payload) = payload.strip_prefix(TEXT_EXTENT_OPS_MAGIC) {
                        refs.extend(content_refs_in_text_ops(text_oplog::decode(
                            extent_payload,
                        )?));
                    }
                }
                LargeValueKind::Blob => {
                    refs.extend(content_refs_in_text_ops(text_oplog::decode(&payload)?));
                }
            }
        }
        refs.sort();
        refs.dedup();
        Ok(refs)
    }

    fn current_row_from_aggregate_result_payload(
        &mut self,
        query: &crate::query::Query,
        member: &ResultMemberEntry,
        payload: &ResultMemberPayloadEntry,
    ) -> Result<CurrentRow, Error> {
        let fields: Vec<(Option<String>, ValueType)> = postcard::from_bytes(&payload.descriptor)
            .map_err(|_| Error::InvalidStoredValue("result payload descriptor is invalid"))?;
        let payload_descriptor = RecordDescriptor::new(
            fields
                .into_iter()
                .map(|(name, value_type)| {
                    name.map(|name| (name, value_type))
                        .ok_or(Error::InvalidStoredValue(
                            "result payload descriptor field must be named",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let payload_record = BorrowedRecord::new(&payload.record, &payload_descriptor);
        aggregate_current_row_from_record(
            query.table.as_str(),
            aggregate_result_member_row_uuid(member)?,
            &payload_record,
        )
    }

    fn current_row_from_result_payload(
        &mut self,
        table: &TableSchema,
        payload: &ResultMemberPayloadEntry,
    ) -> Result<CurrentRow, Error> {
        let fields: Vec<(Option<String>, ValueType)> = postcard::from_bytes(&payload.descriptor)
            .map_err(|_| Error::InvalidStoredValue("result payload descriptor is invalid"))?;
        let payload_descriptor = RecordDescriptor::new(
            fields
                .into_iter()
                .map(|(name, value_type)| {
                    name.map(|name| (name, value_type))
                        .ok_or(Error::InvalidStoredValue(
                            "result payload descriptor field must be named",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let payload_record = BorrowedRecord::new(&payload.record, &payload_descriptor);
        let row_uuid_idx = payload_descriptor
            .field_index("row_uuid")
            .or_else(|| payload_descriptor.field_index("id"))
            .ok_or(Error::InvalidStoredValue(
                "result payload is missing row identity",
            ))?;
        let row_uuid = payload_record.get_uuid(row_uuid_idx)?;
        let mut descriptor_fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
        let mut values = vec![Value::Uuid(row_uuid)];
        for (index, field) in payload_descriptor.fields().iter().enumerate() {
            let Some(name) = &field.name else {
                continue;
            };
            if name == "row_uuid" || name == "id" {
                continue;
            }
            descriptor_fields.push((name.clone(), field.value_type.clone()));
            values.push(payload_record.get_idx(index)?);
        }
        let descriptor = RecordDescriptor::new(descriptor_fields);
        let raw = descriptor.create(&values)?;
        let row = CurrentRow::new(table.name.clone(), OwnedRecord::new(raw, descriptor));
        if row.raw_field("__flat_join_row_1").is_some() {
            return Ok(row);
        }
        self.materialize_current_row(table, row)
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
                    policy_plan_cache_signature(&binding, identity)
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
        self.finish_engine_query_rows(query, &mut rows)?;
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

    fn materialize_relation_snapshot_from_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        read_view: &ReadViewSpec,
        snapshots: &MultisinkDeltas,
    ) -> Result<RelationSnapshot, Error> {
        let root_rows = self.materialize_relation_snapshot_root_rows(shape, snapshots)?;
        let root_count = root_rows.len();
        // Groove's app-rows terminal is the sole structured-output owner.
        // Jazz transports its recursive roots; relation facts are not a second
        // public representation and never participate in tree assembly.
        if !shape.query().array_subqueries.is_empty() {
            return Ok(RelationSnapshot {
                root_count,
                rows: root_rows,
                edges: Vec::new(),
            });
        }
        let mut snapshot = RelationSnapshot {
            root_count,
            rows: root_rows,
            edges: Vec::new(),
        };
        let mut row_keys = snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>();
        let Some(edges) = snapshots.get("maintained.relation_edges") else {
            return Ok(snapshot);
        };
        #[derive(Clone)]
        struct RelationEdgeCandidate {
            edge: RelationEdge,
            target_tx_time: TxTime,
            target_tx_node: NodeAlias,
        }

        let windows = Self::relation_snapshot_no_order_windows(&shape.query().array_subqueries);
        let descriptor = &edges.descriptor;
        let source_table_idx = required_field_idx(descriptor, "source_table")?;
        let source_row_idx = required_field_idx(descriptor, "source_row")?;
        let relation_idx = required_field_idx(descriptor, "path")?;
        let target_table_idx = required_field_idx(descriptor, "target_table")?;
        let target_row_idx = required_field_idx(descriptor, "target_row")?;
        let target_tx_time_idx = required_field_idx(descriptor, "target_tx_time")?;
        let target_tx_node_idx = required_field_idx(descriptor, "target_tx_node_id")?;
        let mut candidates = Vec::new();
        for (record, weight) in edges.iter() {
            if weight <= 0 {
                continue;
            }
            let source_table = record.get_str(source_table_idx)?.to_owned();
            let source_row = RowUuid(record.get_uuid(source_row_idx)?);
            let relation = record.get_str(relation_idx)?.to_owned();
            let target_table_name = record.get_str(target_table_idx)?.to_owned();
            let target_row = RowUuid(record.get_uuid(target_row_idx)?);
            let target_tx_time = TxTime(record.get_u64(target_tx_time_idx)?);
            let target_tx_node = NodeAlias(record.get_u64(target_tx_node_idx)?);
            candidates.push(RelationEdgeCandidate {
                edge: RelationEdge {
                    source_table,
                    source_row,
                    relation,
                    target_table: target_table_name,
                    target_row,
                },
                target_tx_time,
                target_tx_node,
            });
        }
        candidates.sort_by(|left, right| {
            (
                &left.edge.source_table,
                left.edge.source_row,
                &left.edge.relation,
                left.edge.target_row,
            )
                .cmp(&(
                    &right.edge.source_table,
                    right.edge.source_row,
                    &right.edge.relation,
                    right.edge.target_row,
                ))
        });
        let mut counts = BTreeMap::<(String, RowUuid, String), usize>::new();
        let mut target_tables = BTreeMap::<String, TableSchema>::new();
        for candidate in candidates {
            let group = (
                candidate.edge.source_table.clone(),
                candidate.edge.source_row,
                candidate.edge.relation.clone(),
            );
            let count = counts.entry(group).or_default();
            let window = windows.get(&candidate.edge.relation).copied();
            let ordinal = *count;
            *count += 1;
            if let Some(window) = window {
                if ordinal < window.offset
                    || window
                        .limit
                        .is_some_and(|limit| ordinal >= window.offset.saturating_add(limit))
                {
                    continue;
                }
            }
            if row_keys.insert((
                candidate.edge.target_table.clone(),
                candidate.edge.target_row,
            )) {
                if !target_tables.contains_key(&candidate.edge.target_table) {
                    let target_table = self
                        .table_in_schema(&candidate.edge.target_table, shape.schema_version())?
                        .clone();
                    target_tables.insert(candidate.edge.target_table.clone(), target_table);
                }
                let target_table = target_tables
                    .get(&candidate.edge.target_table)
                    .expect("target table was inserted");
                let row = self.materialize_relation_edge_target_row(
                    read_view,
                    target_table,
                    &candidate.edge.target_table,
                    candidate.edge.target_row,
                    candidate.target_tx_time,
                    candidate.target_tx_node,
                )?;
                snapshot.rows.push(row);
            }
            snapshot.edges.push(candidate.edge);
        }
        Ok(snapshot)
    }

    fn materialize_relation_edge_target_row(
        &mut self,
        read_view: &ReadViewSpec,
        target_table: &TableSchema,
        target_table_name: &str,
        target_row: RowUuid,
        target_tx_time: TxTime,
        target_tx_node: NodeAlias,
    ) -> Result<CurrentRow, Error> {
        if let Some(version) = self.query_version_by_alias(
            target_table_name,
            target_row,
            VersionLayer::Content,
            target_tx_time,
            target_tx_node,
        )? {
            return self.current_row_from_materialized_version(target_table, &version);
        }
        let ReadViewSourceSpec::Branch { branch } = read_view.source else {
            return Err(Error::InvalidStoredValue(
                "relation edge target version is missing",
            ));
        };
        let branch = self
            .branches
            .branches
            .get(&BranchId(branch))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "relation edge target branch is missing",
            ))?;
        self.branch_current_rows(target_table_name, &branch)?
            .into_iter()
            .find(|row| row.row_uuid() == target_row)
            .ok_or(Error::InvalidStoredValue(
                "relation edge target branch row is missing",
            ))
    }

    fn relation_snapshot_no_order_windows(
        subqueries: &[ArraySubquery],
    ) -> BTreeMap<String, RelationSnapshotWindow> {
        let mut windows = BTreeMap::new();
        for subquery in subqueries {
            if subquery.order_by.is_empty() && (subquery.limit.is_some() || subquery.offset != 0) {
                windows.insert(
                    subquery.column_name.clone(),
                    RelationSnapshotWindow {
                        offset: subquery.offset,
                        limit: subquery.limit,
                    },
                );
            }
            windows.extend(Self::relation_snapshot_no_order_windows(
                &subquery.nested_arrays,
            ));
        }
        windows
    }

    fn materialize_relation_snapshot_root_rows(
        &mut self,
        shape: &ValidatedQuery,
        snapshots: &MultisinkDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(app_rows) = snapshots.get(JAZZ_APP_ROWS_SINK) else {
            return Err(Error::QueryLowering(
                "relation snapshot program did not emit app rows".to_owned(),
            ));
        };
        let table = self
            .table_in_schema(&shape.query().table, shape.schema_version())?
            .clone();
        let mut rows = Vec::new();
        for (record, weight) in app_rows.iter() {
            if weight > 0 {
                let row = decode_current_row(&table, record)?;
                rows.push(self.materialize_current_row(&table, row)?);
            }
        }
        // Multisink records are transport-key ordered. Restore public root rank
        // while retaining the lowered program's membership and window.
        self.apply_query_order(shape.query(), &mut rows)?;
        Ok(rows)
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

    fn finish_engine_query_rows(
        &self,
        query: &crate::query::Query,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        self.finish_engine_query_rows_in_schema(
            query,
            self.catalogue.current_write_schema.schema,
            rows,
        )
    }

    fn finish_engine_query_rows_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        if query.aggregate.is_some() {
            self.apply_query_order_in_schema(query, schema_version, rows)?;
            apply_query_window(query, rows);
            return Ok(());
        }
        // Groove lowering owns membership/windowing, but one-shot APIs still
        // return a deterministic Vec. Re-apply ordering to the selected rows
        // without re-applying pagination.
        self.apply_query_order_in_schema(query, schema_version, rows)
    }

    fn query_output_table(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
    ) -> Result<TableSchema, Error> {
        let source_table = self.table_in_schema(&query.table, schema_version)?;
        if query.aggregate.is_some() {
            aggregate_result_table(query, &source_table)
        } else {
            Ok(source_table)
        }
    }

    pub(crate) fn apply_query_order(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        self.apply_query_order_in_schema(query, self.catalogue.current_write_schema.schema, rows)
    }

    fn apply_query_order_with_occurrences(
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

    fn sort_query_rows_with_occurrences(
        query: &crate::query::Query,
        table: Option<&TableSchema>,
        rows: &mut Vec<CurrentRow>,
        occurrence_ids: &mut Vec<OutputOccurrenceId>,
    ) -> Result<(), Error> {
        if rows.len() != occurrence_ids.len() {
            return Err(Error::InvalidStoredValue(
                "maintained root occurrence sidecar length does not match rows",
            ));
        }
        let mut paired = rows
            .drain(..)
            .zip(occurrence_ids.drain(..))
            .collect::<Vec<_>>();
        if query.order_by.is_empty() {
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    default_query_row_order(left_row, right_row)
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        } else if query.aggregate.is_some() {
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    for order in &query.order_by {
                        let ordering = compare_optional_values(
                            aggregate_row_cell(left_row, query, &order.column),
                            aggregate_row_cell(right_row, query, &order.column),
                        );
                        let ordering = match order.direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    left_row
                        .row_uuid()
                        .to_bytes()
                        .cmp(&right_row.row_uuid().to_bytes())
                        .then_with(|| left_row.record.raw().cmp(right_row.record.raw()))
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        } else {
            let table = table.ok_or(Error::InvalidStoredValue(
                "ordered maintained rows are missing their table schema",
            ))?;
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    for order in &query.order_by {
                        let ordering = compare_optional_values(
                            query_order_value(left_row, &table, &order.column),
                            query_order_value(right_row, &table, &order.column),
                        );
                        let ordering = match order.direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    left_row
                        .row_uuid()
                        .to_bytes()
                        .cmp(&right_row.row_uuid().to_bytes())
                        .then_with(|| left_row.record.raw().cmp(right_row.record.raw()))
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        }
        for (row, occurrence) in paired {
            rows.push(row);
            occurrence_ids.push(occurrence);
        }
        Ok(())
    }

    fn apply_query_order_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
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
                        aggregate_row_cell(left, query, &order.column),
                        aggregate_row_cell(right, query, &order.column),
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
        let table = self.table_in_schema(&query.table, schema_version)?;
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

    fn apply_projection(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        self.apply_projection_in_schema(query, self.catalogue.current_write_schema.schema, rows)
    }

    fn apply_projection_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        let Some(columns) = &query.select else {
            return Ok(());
        };
        let table = self.table_in_schema(&query.table, schema_version)?;
        for row in rows {
            *row = row.project(&table, columns)?;
        }
        Ok(())
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
            policy_plan_cache_signature(binding, identity),
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

    fn prepared_query_plan_from_program(
        &mut self,
        program: &QueryProgram,
        _shape: &ValidatedQuery,
        _binding: &Binding,
    ) -> Result<PreparedQueryPlan, Error> {
        let app_row_fields = app_row_terminal_fields(&program.lowered.output)?;
        let graph = lowered_materialization_app_rows_graph(&program)?;
        let params = prepared_params_from_domain(&program.lowered.parameters);
        let route_eligible_fields =
            app_row_terminal_route_eligible_fields(&program.lowered.output)?;
        let route_eligible_fields = route_eligible_fields.into_iter().collect::<BTreeSet<_>>();
        // A terminal may expose only a subset of the program's routes (for
        // example, an include policy can consume a claim without routing the
        // app-row terminal by it). Keep that terminal's routes as the exact
        // binding-value prefix Groove zips against.
        let route_params = params
            .iter()
            .map(prepared_param_route_field)
            .filter(|field| route_eligible_fields.contains(field))
            .collect::<Vec<_>>();
        let param_names = params
            .iter()
            .map(|param| param.name.clone())
            .collect::<Vec<_>>();
        let binding_descriptor = RecordDescriptor::new(
            param_names
                .iter()
                .cloned()
                .zip(params.iter().map(|param| param.ty.clone())),
        );
        if params.is_empty() {
            Ok(PreparedQueryPlan::Graph(graph))
        } else {
            let binding_source_shape = program
                .request
                .input
                .binding
                .source_shape
                .clone()
                .unwrap_or_else(|| query_binding_source_shape_for_prepared_params(&params));
            let route_fields = route_params;
            let route_value_indices = prepared_route_value_indices(&params, &route_fields);
            let prepared = self.database.prepare(
                [groove::ivm::RoutedMultisinkTerminal::new(
                    JAZZ_APP_ROWS_SINK,
                    graph,
                    route_fields,
                    app_row_fields,
                )
                .with_route_value_indices(route_value_indices)],
                binding_source_shape,
                binding_descriptor,
            )?;
            Ok(PreparedQueryPlan::Prepared {
                shape: prepared.id(),
                params,
            })
        }
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

    fn subscribe_lowered_program(
        &mut self,
        program: QueryProgram,
        binding: &Binding,
        binding_source_shape: String,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<MultisinkSubscription, Error> {
        let params = prepared_params_from_domain(&program.lowered.parameters);
        let route_params = prepared_route_param_names(&program.lowered.parameters);
        if params.is_empty() {
            let sinks: Vec<(String, GraphBuilder)> = program
                .lowered
                .terminals
                .into_iter()
                .map(|terminal| (terminal.sink, terminal.graph))
                .collect();
            return self.database.subscribe(sinks).map_err(Error::Groove);
        }
        let param_names = params
            .iter()
            .map(|param| param.name.clone())
            .collect::<Vec<_>>();
        let binding_descriptor = RecordDescriptor::new(
            param_names
                .iter()
                .cloned()
                .zip(params.iter().map(|param| param.ty.clone())),
        );
        let values = binding_values_for_plan(
            binding,
            &params,
            &program.request.policy,
            prepared_claim_binding_mode,
        )?;
        let terminals = program
            .lowered
            .terminals
            .into_iter()
            .map(|terminal| {
                let public_fields = terminal_public_fields(&terminal.output)?;
                let route_fields = terminal_route_fields(
                    &route_params,
                    &terminal_route_eligible_fields(&terminal.output)?,
                );
                let route_value_indices = prepared_route_value_indices(&params, &route_fields);
                Ok(RoutedMultisinkTerminal::new(
                    terminal.sink,
                    terminal.graph,
                    route_fields,
                    public_fields,
                )
                .with_route_value_indices(route_value_indices))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let prepared =
            self.database
                .prepare(terminals, binding_source_shape, binding_descriptor)?;
        self.database
            .bind_shape(prepared.id(), &values)
            .map_err(Error::Groove)
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
        let policies = authorization_policy_queries(self.table(table_name)?, operation);
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
                    &self.catalogue.schema,
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

fn query_binding_value_signature(binding: &Binding) -> String {
    binding
        .values()
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(",")
}

fn policy_plan_cache_signature(binding: &Binding, identity: AuthorId) -> String {
    // Authorization lowering still embeds the permission subject in source
    // plans. Claim values are routed at bind time, but plans from different
    // subjects are not interchangeable until that subject is parameterized.
    format!(
        "{}|subject={identity:?}",
        query_binding_value_signature(binding)
    )
}

fn exact_known_state_declaration_if_within_limits(
    _shape_id: ShapeId,
    _subscription: SubscriptionKey,
    _values: &[Value],
    refs: Vec<RowVersionRef>,
) -> Option<KnownStateDeclaration> {
    if refs.len() > MAX_KNOWN_STATE_EXACT_REFS {
        return None;
    }
    Some(KnownStateDeclaration::ExactVersionSet { versions: refs })
}

#[cfg(test)]
pub(crate) fn exact_known_state_declaration_for_test(
    shape_id: ShapeId,
    subscription: SubscriptionKey,
    values: &[Value],
    refs: Vec<RowVersionRef>,
) -> Option<KnownStateDeclaration> {
    exact_known_state_declaration_if_within_limits(shape_id, subscription, values, refs)
}

fn query_binding_source_shape_for_prepared_params(params: &[PreparedQueryParam]) -> String {
    let mut user_params = BTreeMap::new();
    let mut claim_params = BTreeMap::new();
    for param in params {
        match &param.source {
            PreparedQueryParamSource::User => {
                user_params.insert(param.name.clone(), param.ty.clone());
            }
            PreparedQueryParamSource::Claim(path) => {
                claim_params.insert(
                    param.name.clone(),
                    ProgramClaimParam {
                        path: path.clone(),
                        ty: param.ty.clone(),
                    },
                );
            }
        }
    }
    query_binding_source_shape_for_parts(&user_params, &claim_params)
}

fn query_binding_source_shape_for_parts(
    param_types: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-binding-source-v1");
    push_usize(&mut bytes, param_types.len());
    for (name, ty) in param_types {
        push_str(&mut bytes, name);
        push_str(&mut bytes, &format!("{ty:?}"));
    }
    push_usize(&mut bytes, claim_params.len());
    for (name, claim) in claim_params {
        push_str(&mut bytes, name);
        push_usize(&mut bytes, claim.path.0.len());
        for segment in &claim.path.0 {
            push_str(&mut bytes, segment);
        }
        push_str(&mut bytes, &format!("{:?}", claim.ty));
    }
    let hash = blake3::hash(&bytes);
    format!("jazz-query-binding:{}", hash.to_hex())
}

fn query_binding_source_shape_for_parts_if_needed(
    param_types: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> Option<String> {
    (!param_types.is_empty() || !claim_params.is_empty())
        .then(|| query_binding_source_shape_for_parts(param_types, claim_params))
}

fn authorization_binding_source_shape(
    shape: &ValidatedQuery,
    extra_user_params: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> Option<String> {
    let mut param_types = shape.params().clone();
    param_types.extend(extra_user_params.clone());
    (!param_types.is_empty() || !claim_params.is_empty())
        .then(|| query_binding_source_shape_for_parts(&param_types, claim_params))
}

fn push_usize(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u64).to_le_bytes());
}

fn push_str(bytes: &mut Vec<u8>, value: &str) {
    push_usize(bytes, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

fn binding_values_for_plan(
    binding: &Binding,
    params: &[PreparedQueryParam],
    policy: &PolicyContext,
    prepared_claim_binding_mode: PreparedClaimBindingMode,
) -> Result<Vec<Value>, Error> {
    params
        .iter()
        .map(|param| match param.source {
            PreparedQueryParamSource::User => {
                let value = binding
                    .values()
                    .get(&param.name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(param.name.clone()))?;
                Ok::<_, Error>(coerce_prepared_binding_value(value, &param.ty))
            }
            PreparedQueryParamSource::Claim(ref path) => {
                let value = match prepared_claim_value(path, policy)? {
                    Some(value) => value,
                    None if prepared_claim_binding_mode
                        == PreparedClaimBindingMode::FailClosedAuthorizationSupport =>
                    {
                        return Err(Error::AuthorizationSupportMissingClaim(path.0.join(".")));
                    }
                    None => {
                        return Err(Error::InvalidStoredValue(
                            "claim prepared param is not bound",
                        ));
                    }
                };
                Ok::<_, Error>(coerce_prepared_binding_value(value, &param.ty))
            }
        })
        .collect()
}

fn prepared_claim_value(path: &ClaimPath, policy: &PolicyContext) -> Result<Option<Value>, Error> {
    let (permission_subject, claims) = match policy {
        PolicyContext::Identity {
            permission_subject,
            claims,
            ..
        }
        | PolicyContext::AuthorizationSubplan {
            permission_subject,
            claims,
            ..
        } => (permission_subject, claims),
        PolicyContext::System => {
            return Err(Error::InvalidStoredValue(
                "claim prepared params require an identity policy context",
            ));
        }
    };
    let [name] = path.0.as_slice() else {
        return Err(Error::InvalidStoredValue(
            "nested claim prepared params are not supported yet",
        ));
    };
    if let Some(value) = claims.get(name) {
        return Ok(Some(value.clone()));
    }
    if let Some(value) = default_policy_claim_values(*permission_subject).get(name) {
        return Ok(Some(value.clone()));
    }
    Ok(None)
}

fn coerce_prepared_binding_value(value: Value, column_type: &groove::schema::ColumnType) -> Value {
    if let Some(value) = coerce_prepared_integer_value(&value, column_type) {
        return value;
    }
    match (value, column_type) {
        (Value::Uuid(value), groove::schema::ColumnType::String) => {
            Value::String(value.to_string())
        }
        (Value::String(value), groove::schema::ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(value), groove::schema::ColumnType::Nullable(inner)) => Value::Nullable(
            value.map(|value| Box::new(coerce_prepared_binding_value(*value, inner))),
        ),
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_prepared_binding_value(*value, column_type),
        ))),
        (value @ Value::Nullable(None), _) => value,
        (Value::Array(values), groove::schema::ColumnType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| coerce_prepared_binding_value(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), groove::schema::ColumnType::Tuple(types))
            if values.len() == types.len() =>
        {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_prepared_binding_value(value, column_type))
                    .collect(),
            )
        }
        (value, groove::schema::ColumnType::Nullable(inner))
            if !matches!(value, Value::Nullable(_)) =>
        {
            Value::Nullable(Some(Box::new(coerce_prepared_binding_value(value, inner))))
        }
        (value, _) => value,
    }
}

/// Normalizes prepared integer values. Failed conversions intentionally return
/// `None`, so the original typed value stays in the binding and cannot wrap
/// into an authorized value.
fn coerce_prepared_integer_value(
    value: &Value,
    column_type: &groove::schema::ColumnType,
) -> Option<Value> {
    let value = match value {
        Value::U8(value) => i128::from(*value),
        Value::U16(value) => i128::from(*value),
        Value::U32(value) => i128::from(*value),
        Value::U64(value) => i128::from(*value),
        Value::I32(value) => i128::from(*value),
        Value::I64(value) => i128::from(*value),
        _ => return None,
    };
    match column_type {
        groove::schema::ColumnType::U8 => u8::try_from(value).ok().map(Value::U8),
        groove::schema::ColumnType::U16 => u16::try_from(value).ok().map(Value::U16),
        groove::schema::ColumnType::U32 => u32::try_from(value).ok().map(Value::U32),
        groove::schema::ColumnType::U64 => u64::try_from(value).ok().map(Value::U64),
        groove::schema::ColumnType::I32 => i32::try_from(value).ok().map(Value::I32),
        groove::schema::ColumnType::I64 => i64::try_from(value).ok().map(Value::I64),
        _ => None,
    }
}

fn coerce_binding_values_for_shape(shape: &ValidatedQuery, values: &mut BTreeMap<String, Value>) {
    for (name, value) in values {
        let Some(ty) = shape.params().get(name) else {
            continue;
        };
        *value = coerce_prepared_binding_value(value.clone(), ty);
    }
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

fn compare_optional_values(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_order_values(&left, &right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
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

fn aggregate_row_cell(
    row: &CurrentRow,
    query: &crate::query::Query,
    column: &str,
) -> Option<Value> {
    let field = if query
        .aggregate
        .as_ref()
        .and_then(|aggregate| aggregate.group_by.as_deref())
        == Some(column)
    {
        user_column_field(column)
    } else if query.aggregate.as_ref().is_some_and(|aggregate| {
        aggregate
            .aggregates
            .iter()
            .any(|aggregate| aggregate.alias == column)
    }) {
        aggregate_output_app_field(column)
    } else {
        user_column_field(column)
    };
    let idx = row.record.descriptor().field_index(&field)?;
    nullable_value(row.record.borrowed().get_idx(idx).ok()?).ok()?
}

fn aggregate_result_table(
    query: &crate::query::Query,
    source_table: &TableSchema,
) -> Result<TableSchema, Error> {
    let aggregate = query.aggregate.as_ref().ok_or(Error::InvalidStoredValue(
        "aggregate query missing aggregate",
    ))?;
    let mut columns = Vec::new();
    if let Some(group_by) = &aggregate.group_by {
        let column = source_table
            .columns
            .iter()
            .find(|column| &column.name == group_by)
            .ok_or(Error::InvalidStoredValue("aggregate group column missing"))?;
        columns.push(ColumnSchema::new(&column.name, column.column_type.clone()));
    }
    for aggregate in &aggregate.aggregates {
        columns.push(ColumnSchema::new(
            aggregate_output_column(&aggregate.alias),
            aggregate_result_column_type(aggregate, source_table)?,
        ));
    }
    Ok(TableSchema::new(&query.table, columns))
}

fn aggregate_result_column_type(
    aggregate: &Aggregate,
    source_table: &TableSchema,
) -> Result<ColumnType, Error> {
    match aggregate.function {
        AggregateFunction::Count => Ok(ColumnType::U64),
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            let column = aggregate
                .column
                .as_ref()
                .ok_or(Error::InvalidStoredValue("aggregate input column missing"))?;
            let column_type = source_table
                .columns
                .iter()
                .find(|candidate| &candidate.name == column)
                .map(|column| column.column_type.clone())
                .ok_or(Error::InvalidStoredValue("aggregate input column missing"))?;
            // `CurrentRow` supplies the public nullable envelope.  The
            // aggregate payload itself carries the SQL nullable layer, which
            // `current_row_from_aggregate_result_payload` flattens before it
            // reaches this synthetic table schema.
            Ok(match column_type {
                ColumnType::Nullable(inner) => *inner,
                column_type => column_type,
            })
        }
        AggregateFunction::Avg => Ok(ColumnType::F64),
    }
}

/// Use the same stable identity for direct aggregate reads and maintained
/// aggregate delivery.  A global aggregate is keyed by `"global"`; grouped
/// aggregates are keyed by their lowered group value.
fn aggregate_query_row_uuid(
    query: &crate::query::Query,
    record: &BorrowedRecord<'_>,
) -> Result<RowUuid, Error> {
    let aggregate = query.aggregate.as_ref().ok_or(Error::InvalidStoredValue(
        "aggregate query missing aggregate",
    ))?;
    let row_value = match &aggregate.group_by {
        Some(group_by) => {
            let field = user_column_field(group_by);
            let index = record
                .descriptor()
                .field_index(&field)
                .or_else(|| record.descriptor().field_index(group_by))
                .ok_or(Error::InvalidStoredValue(
                    "aggregate record is missing group identity",
                ))?;
            record.get_idx(index)?
        }
        None => Value::String("global".to_owned()),
    };
    let row = postcard::to_allocvec(&row_value)
        .map_err(|_| Error::InvalidStoredValue("aggregate result row encoding failed"))?;
    aggregate_result_member_row_uuid(&ResultMemberEntry::Synthetic {
        table: "aggregate_result".to_owned(),
        row,
        replacement: SyntheticReplacementToken::from_encoded_record(Vec::new()),
    })
}

fn apply_query_window(query: &crate::query::Query, rows: &mut Vec<CurrentRow>) {
    let offset = query.offset.min(rows.len());
    let limit = query.limit.unwrap_or(rows.len().saturating_sub(offset));
    let end = offset.saturating_add(limit).min(rows.len());
    if offset > 0 || end < rows.len() {
        *rows = rows[offset..end].to_vec();
    }
}

fn compare_order_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.cmp(right),
        (Value::U16(left), Value::U16(right)) => left.cmp(right),
        (Value::U32(left), Value::U32(right)) => left.cmp(right),
        (Value::U64(left), Value::U64(right)) => left.cmp(right),
        (Value::I32(left), Value::I32(right)) => left.cmp(right),
        (Value::I64(left), Value::I64(right)) => left.cmp(right),
        (Value::F64(left), Value::F64(right)) => left.total_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::EnumTag(left), Value::EnumTag(right)) => left.cmp(right),
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
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    branch_id: BranchId,
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
        .map(|row| {
            inline_branch_current_record(table, &descriptor, row, schema_version_alias, branch_id)
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
    branch_id: BranchId,
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
        if descriptor.field_index("branch_id").is_some() {
            values.push(Value::Uuid(branch_id.0));
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
        CurrentWriteSchema, MigrationLens, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
        SchemaVersion, ShapeAst, Subscribe, SyncMessage, TableLens,
    };
    use crate::query::{
        Aggregate, ArraySubquery, JoinSourceLookup, OrderDirection, PolicyBranch, Query, claim,
        col, contains, eq, gt, in_list, lit, lte, param,
    };
    use crate::schema::{JazzSchema, Policy, TableSchema};

    use super::*;

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
                read_view: Default::default(),
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
                &table,
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
    fn branch_program_maintained_view_requires_branch_deletion_witness_source() {
        // Internal compiler-boundary coverage: the public DB tests assert the
        // user-visible subscription rejection, while this pins which output
        // profile needs branch deletion witness metadata.
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

        let error = node
            .compile_branch_query_program_in_authorization_mode(
                branch_id,
                &shape,
                &binding,
                AuthorId::SYSTEM,
                CurrentQueryProgramOutput::MaintainedView,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap_err();
        let Error::QueryCapability(report) = error else {
            panic!("expected branch witness capability gap, got {error:?}");
        };
        assert!(
            report.contains("BranchOverlay"),
            "unexpected capability report: {report}"
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
