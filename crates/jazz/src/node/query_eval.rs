//! Query execution, shape registration, binding routing, and read-set
//! evaluation for `jazz/SPEC/6_queries.md`. This module owns lowering validated Jazz
//! queries to groove plans, evaluating one-shot reads, recording predicate reads,
//! and applying binding deltas; the pure AST lives in [`crate::query`], policy
//! checks in [`super::policy`], and sync view payload assembly in [`super::views`].
//! It is the node layer's query bridge to groove IVM.

use super::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use groove::ivm::{LiteralValue, RoutedMultisinkTerminal, StaticScanSpec};
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
    FieldProjection, FrontierId, JoinContribution, JoinMode as NormalizedJoinMode, LensSelection,
    NormalizedRowSetShape, NormalizedShapeIdentity, NormalizedValueRef,
    OrderKey as NormalizedOrderKey, OutputTerminalSchema, OverlayRef, OverlayStack,
    PayloadProjection, PolicyContext, PolicyDecisionRole, PolicyEnforcementMode,
    PredicateExpr as NormalizedPredicateExpr, ProgramBinding, ProgramClaimParam, ProgramFactKey,
    ProgramOutputSchemas, ProgramPathId, ProvenanceField, QueryProgram, QueryProgramRequest,
    QueryReadSet, ReachableContribution, ReadView, RequestedReadSet, RequestedSourceStage,
    ResolvedSource, ResultId, ResultMembershipVersionSchema, ResultRowRef, RowIdRef, RowProjection,
    RowRefSchema as QueryEngineRowRefSchema, RowSetExpr, RowSetNodeId, RowSetOutputRequest,
    RowSetProgramInput, RowVisibility, SchemaFamilySelection, SchemaProjection,
    SortDirection as NormalizedSortDirection, SourceAuthorizationRequest, SourceExpr, SourceGap,
    SourceId, SourceMetadataFields, SourceMetadataRequirement, SourcePath, SourceRequest,
    SourceRequirements, SourceResolutionError, SourceResolver, SourceRole, SourceRowShape,
    StorageSchemaSelection, TypedOutputField, UnionInput, ValueSourceColumn, ValueSourceMode,
    VersionIdentityFields, VersionedRowRefSchema, claim_param_field, claim_path_from_param_field,
    left_field, logical_user_column, lower_query_program, right_field, route_param_field,
    user_column_field,
};
use crate::protocol::{
    BindingViewKey, KnownStateCompleteness, KnownStateDeclaration, ProgramFactEntry, ReadViewKey,
    ReadViewSourceSpec, ReadViewSpec, ResultMemberEntry, ResultMemberPayloadEntry, RowVersionRef,
    ShapeAst, ShapeBody, Subscribe, SubscriptionKey, SyncMessage,
};
use crate::protocol_limits::{MAX_KNOWN_STATE_EXACT_REFS, MAX_SYNC_MESSAGE_BYTES};
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, Predicate, Query as JazzQuery,
    QueryError, ShapeId, ValidatedQuery, binding_id_for_values, relation_query_to_query,
};
use crate::schema::{ColumnSchema, branch_metadata_table_schema, global_current_index_name};

pub(crate) const JAZZ_APP_ROWS_SINK: &str = "app_rows";
const PENDING_BINDING_SOURCE_SHAPE: &str = "__jazz_pending_binding_source";

pub(crate) struct LocalMaintainedViewSubscription {
    subscription: MultisinkSubscription,
    _retained_prepared_plan: Option<PreparedQueryPlanHandle>,
    maintained: MaintainedSubscriptionView,
    terminal_schemas: MaintainedTerminalSchemas,
    tables: BTreeMap<String, TableSchema>,
    result_table: String,
    result_select: Option<Vec<String>>,
    result_set: BTreeSet<ResultMemberEntry>,
    result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    program_facts: BTreeSet<ProgramFactEntry>,
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

fn prepared_route_param_names(parameters: &super::query_engine::ParameterDomain) -> Vec<String> {
    parameters.routing_params.iter().cloned().collect()
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
            fields.extend(schema.branch_or_prefix_field.clone());
            fields.extend(result_membership_version_fields(&schema.version));
            fields.extend(schema.settle_position_field.clone());
            fields.extend(schema.routing_param_fields.iter().cloned());
            Ok(fields)
        }
        ProgramFactSchema::AggregateResult(schema) => {
            let mut fields = vec![
                schema.synthetic.table_field.clone(),
                schema.synthetic.row_field.clone(),
                schema.synthetic.revision_field.clone(),
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
    pub(crate) added: Vec<CurrentRow>,
    pub(crate) removed: Vec<(String, RowUuid)>,
    pub(crate) added_edges: Vec<(RelationEdge, Option<CurrentRow>)>,
    pub(crate) removed_edges: Vec<RelationEdge>,
}

enum CurrentQueryProgramOutput {
    AppRows,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

struct CurrentQuerySourceResolver<'a, S> {
    node: &'a mut NodeState<S>,
    read_view: &'a ReadView<RequestedSourceStage>,
    inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
    access_paths: BTreeMap<SourceId, CurrentAccessPath>,
}

struct CurrentSourceGraph {
    graph: GraphBuilder,
    descriptor: RecordDescriptor,
    metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

#[derive(Clone, Debug)]
enum CurrentAccessPath {
    PrimaryKey(Vec<Value>),
    Index { index: String, prefix: Vec<Value> },
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
                match self
                    .node
                    .settled_binding_view_source_rows(&request.source.table, *binding_view)
                {
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
            || !matches!(projection.storage, StorageSchemaSelection::Single(_))
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
        if let Some(rows) = self.inline_sources.get(&request.source) {
            if request.visibility != RowVisibility::Visible
                || !request.requirements.metadata.is_empty()
                || !matches!(request.authorization, SourceAuthorizationRequest::System)
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
            let graph = match &request.authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
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
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_at(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            ParamBindingMode::InlineAllReachableSeeds,
                            position,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::HistoricalStorageCut)
                        })?;
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &descriptor_field_names(&descriptor).map_err(|_| {
                                source_resolution_error(request, SourceGap::HistoricalStorageCut)
                            })?,
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::HistoricalStorageCut)
                        })?
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
                .branch_current_rows(&request.source.table, &branch)
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
            let graph = match &request.authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
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
                        .branch_table_read_policy_authorization_request(
                            branch_id,
                            &table,
                            *permission_subject,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    let output_fields = descriptor_field_names(&descriptor)
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(tx_id) = open_tx_overlay {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::TransactionReadOverlay,
                ));
            }
            let rows = self
                .node
                .tx_current_rows(tx_id, &request.source.table)
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?;
            let graph = inline_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?;
            let descriptor = current_row_descriptor(&table);
            (graph, descriptor, BTreeMap::new(), BTreeSet::new())
        } else if request.visibility == RowVisibility::Visible
            && (self.read_view.read_schema != self.node.catalogue.current_schema_version_id
                || self
                    .node
                    .catalogue
                    .partitions
                    .iter()
                    .any(|(logical, version)| {
                        logical == &request.source.table
                            && *version != self.node.catalogue.current_schema_version_id
                    }))
        {
            if !request.requirements.metadata.is_empty() {
                if self.node.table(&request.source.table).is_ok() {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::SchemaProjection,
                    ));
                }
                self.node.query_engine_read_metrics.source_full_scans += 1;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                    &request.requirements,
                    &request.authorization,
                    self.read_view.policy_schema,
                    None,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let source = self.projected_visible_current_source_graph(
                    request,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                )?;
                let graph = match &request.authorization {
                    SourceAuthorizationRequest::System => source.graph,
                    SourceAuthorizationRequest::PolicyFiltered {
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
                            .table_read_policy_authorization_request(
                                self.read_view.policy_schema,
                                &table.name,
                                *permission_subject,
                                ParamBindingMode::InlineAllReachableSeeds,
                                graph_tier.expect("visible current source has a tier"),
                                plan.binding_source_shape.clone(),
                                plan.binding_user_params.clone(),
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        self.node
                            .policy_filtered_current_source_graph_via_query_engine(
                                policy_request,
                                source.graph,
                                &current_row_fields(&table),
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                            .graph
                    }
                };
                (graph, source.descriptor, source.metadata, BTreeSet::new())
            }
        } else if request.visibility == RowVisibility::IncludeDeleted
            && (self.read_view.read_schema != self.node.catalogue.current_schema_version_id
                || self
                    .node
                    .catalogue
                    .partitions
                    .iter()
                    .any(|(logical, version)| {
                        logical == &request.source.table
                            && *version != self.node.catalogue.current_schema_version_id
                    }))
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
            let graph = match &request.authorization {
                SourceAuthorizationRequest::System => base.clone(),
                SourceAuthorizationRequest::PolicyFiltered {
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
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base.clone(),
                            &output_fields,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
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
            let graph = match &request.authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
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
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
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
                &request.authorization,
                self.read_view.policy_schema,
                selected_base,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
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
            CurrentAccessPath::Index { index, prefix } => {
                if tier != DurabilityTier::Global {
                    return Ok(None);
                }
                let rows = self
                    .node
                    .global_current_rows_for_index_scan(table, &index, &prefix)
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                self.node.query_engine_read_metrics.source_index_probes += 1;
                Ok(Some(rows))
            }
        }
    }

    fn deletion_register_source_for_request(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalSeq>,
        open_tx_overlay: Option<OpenTxId>,
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
        Ok(Some(DeletionRegisterSource {
            graph: deletion_register_current_source_graph(&table.name, tier),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    fn content_version_source_for_request(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalSeq>,
        open_tx_overlay: Option<OpenTxId>,
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
        if self.uses_current_schema_partition(&request.source.table) {
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

    fn projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        let rows = self
            .node
            .current_rows_for_schema(&request.source.table, self.read_view.read_schema, tier)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let graph = inline_current_graph(table, rows)
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
        Ok(CurrentSourceGraph {
            graph,
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    fn uses_current_schema_partition(&self, table: &str) -> bool {
        self.read_view.read_schema == self.node.catalogue.current_schema_version_id
            && !self
                .node
                .catalogue
                .partitions
                .iter()
                .any(|(logical, version)| {
                    logical == table && *version != self.node.catalogue.current_schema_version_id
                })
    }
}

fn deletion_register_current_source_graph(table: &str, tier: DurabilityTier) -> GraphBuilder {
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table))
            .project_fields(register_storage_fields_for_query_engine(""));
    }
    let current_keys = deletion_register_current_keys_graph(table, tier);
    GraphBuilder::join(
        GraphBuilder::table(register_table_name(table)),
        current_keys,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(register_storage_fields_for_query_engine("left."))
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
    let mut content_fields = vec!["row_uuid".to_owned()];
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
                        PredicateExpr::eq("fate", Value::Enum(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::Enum(2)),
                            PredicateExpr::eq("durability", Value::Enum(3)),
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
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)))
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
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)))
            .project(["row_uuid"]),
        )
    };
    GraphBuilder::anti_join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project(content_fields)
}

fn register_storage_fields_for_query_engine(prefix: &str) -> Vec<ProjectField> {
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
    .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
    .collect()
}

fn source_resolution_error(request: &SourceRequest, gap: SourceGap) -> SourceResolutionError {
    SourceResolutionError {
        request: Box::new(request.clone()),
        gap,
    }
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
        .contains(&SourceMetadataRequirement::VersionWitnesses);
    let needs_settle_position = requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition);

    if needs_version_witnesses {
        fields.extend([
            ProjectField::literal("table", Value::String(table.name.clone())),
            ProjectField::literal("layer", Value::String("content".to_owned())),
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
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
            let policy_request = node.table_read_policy_authorization_request(
                policy_schema,
                &table.name,
                *permission_subject,
                ParamBindingMode::InlineAllReachableSeeds,
                tier,
                binding_source_shape.clone(),
                binding_user_params.clone(),
            )?;
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
    tx_id: OpenTxId,
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
) -> Result<RequestedReadSet, Error> {
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
        CurrentQueryProgramOutput::AppRows => BTreeSet::new(),
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
        app_rows: matches!(
            output,
            CurrentQueryProgramOutput::AppRows | CurrentQueryProgramOutput::RelationSnapshot
        )
        .then(|| AppRowOutputRequest {
            projection: app_row_payload_projection(query),
            large_values: Vec::new(),
        }),
        facts,
    }
}

fn app_row_payload_projection(query: &JazzQuery) -> PayloadProjection {
    let Some(select) = &query.select else {
        return PayloadProjection::ShapeDefault;
    };
    let mut fields = select
        .iter()
        .filter(|field| field.as_str() != "id")
        .filter(|field| !field.starts_with('$'))
        .cloned()
        .collect::<BTreeSet<_>>();
    for include in &query.includes {
        if let Some(root_field) = include.path.split('.').next() {
            fields.insert(root_field.to_owned());
        }
    }
    PayloadProjection::Tree(AppProjectionTree {
        fields: FieldProjection::Fields(fields),
        paths: Vec::new(),
    })
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
                index: global_current_index_name(&column),
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
                    user_column_field(&aggregate.alias),
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
        AggregateFunction::Avg => ColumnType::F64,
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

    if subquery.limit.is_some() {
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
                offset: 0,
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
                right: NormalizedValueRef::SourceField {
                    source: edge_source.clone(),
                    field: reachable.edge_member_column.clone(),
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
        if !predicate_params(&seed.filters).is_empty() {
            return Err(normalization_gap(
                "reachable_via relation seed filters with retained params need binding-param filter lowering",
            ));
        }
        let seed_source = reachable_seed_source_id(seed, reachable_id);
        let columns = reachable_seed_frontier_columns(schema, &seed_source, seed)?;
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
        let mut seed_columns = vec![
            RowProjection {
                output: typed_output_field("team", ColumnType::Uuid),
                value: NormalizedValueRef::SourceField {
                    source: seed_source.clone(),
                    field: seed.team_column.clone(),
                },
            },
            RowProjection {
                output: typed_output_field("reachable_team", ColumnType::Uuid),
                value: NormalizedValueRef::SourceField {
                    source: seed_source.clone(),
                    field: seed.team_column.clone(),
                },
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
        nodes.insert(
            seed_project_node.clone(),
            RowSetExpr::Project {
                input: seed_current,
                columns: seed_columns,
            },
        );
        return Ok((seed_project_node, columns));
    }

    let columns = reachable_frontier_columns(&reachable.from, param_types)?;
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
    let value = NormalizedValueRef::SourceField {
        source: source.clone(),
        field: seed.team_column.clone(),
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
    if let Some(policy) = &parent_table.read_policy {
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
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                base,
                root_source,
                RowSetNodeId(format!("{prefix}:base:row_id")),
            ),
            label: "base".to_owned(),
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
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                branch_current,
                root_source,
                RowSetNodeId(format!("{prefix}:{index}:row_id")),
            ),
            label: index.to_string(),
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
                let value = prepared_claim_value(&claim.path, policy)?;
                values.insert(
                    name.clone(),
                    coerce_prepared_binding_value(value, &claim.ty),
                );
            }
            program_binding.id = binding_id_for_values(&values);
        }
        Ok(program_binding)
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
        let shape = match &ast.body {
            ShapeBody::Query(query) => {
                query.validate_with_schema_version(&schema.schema, ast.schema_version)?
            }
            ShapeBody::Relation(relation) => relation_query_to_query(relation)?
                .validate_with_schema_version(&schema.schema, ast.schema_version)?,
        };
        if shape.shape_id() != shape_id {
            return Err(Error::InvalidStoredValue("shape id does not match AST"));
        }
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

    pub(crate) fn settled_result_transitions_for_subscription(
        &self,
        subscription: SubscriptionKey,
        previous_member_result_set: &BTreeSet<ResultMemberEntry>,
        previous_program_fact_set: &BTreeSet<ProgramFactEntry>,
        result_table_filter: Option<&str>,
        output_tables: &BTreeMap<String, TableSchema>,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        let binding_view_key = self.binding_view_key_for_subscription(subscription)?;
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
                    || matches!(member, ResultMemberEntry::Synthetic { .. }))
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
        Ok(Some(
            super::maintained_subscription_view::ResultTransitions {
                adds: current.difference(&previous).cloned().collect(),
                removes: previous.difference(&current).cloned().collect(),
                result_payload_adds: Vec::new(),
                result_payload_removes: Vec::new(),
                program_fact_adds: current_facts.difference(&previous_facts).cloned().collect(),
                program_fact_removes: previous_facts.difference(&current_facts).cloned().collect(),
                allow_storage_witness_fallback: true,
                observed_delta_batches: 0,
                observed_result_delta_batches: 0,
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
        for member in result_members
            .iter()
            .filter(|member| member.table_name() == Some(result_table))
        {
            let Some(row) =
                self.materialize_authoritative_reset_member(shape, member, &result_payloads, true)?
            else {
                continue;
            };
            row_keys.insert((row.table().to_owned(), row.row_uuid()));
            rows.push(row);
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
        shape: &ValidatedQuery,
        member: &ResultMemberEntry,
        result_payloads: &BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
        apply_root_projection: bool,
    ) -> Result<Option<CurrentRow>, Error> {
        if let Some(payload) = result_payloads.get(member) {
            let Some(table_name) = member.table_name() else {
                return Err(Error::InvalidStoredValue(
                    "result payload member must name a table",
                ));
            };
            let table = self.table(table_name)?.clone();
            let mut row = self.current_row_from_result_payload(&table, payload)?;
            if apply_root_projection
                && table.name == shape.query().table
                && let Some(columns) = &shape.query().select
            {
                row = row.project(&table, columns)?;
            }
            return Ok(Some(row));
        }

        let Some((table_name, row_uuid, tx_id)) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "authoritative reset cannot materialize non-row result without payload",
            ));
        };
        let projection = (apply_root_projection && table_name.as_str() == shape.query().table)
            .then(|| shape.query().select.as_deref())
            .flatten();
        if let Some(mut row) =
            self.materialize_authoritative_reset_current_row(table_name.as_str(), row_uuid)?
        {
            if let Some(columns) = projection {
                let table = self.table(table_name.as_str())?.clone();
                row = row.project(&table, columns)?;
            }
            return Ok(Some(row));
        }
        self.materialize_authoritative_reset_version_row(
            table_name.as_str(),
            row_uuid,
            tx_id,
            projection,
        )
    }

    fn materialize_authoritative_reset_current_row(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let global_tables = table.global_current_storage_tables();
        let Some(content_raw) = self
            .database
            .primary_key_get_raw(&global_tables[0].name, &[Value::Uuid(row_uuid.0)])?
        else {
            return Ok(None);
        };
        let content_record = content_raw.record();
        let content_tx = self.current_record_sort_key(content_record)?;
        if let Some(deletion_raw) = self
            .database
            .primary_key_get_raw(&global_tables[1].name, &[Value::Uuid(row_uuid.0)])?
        {
            let deletion_record = deletion_raw.record();
            let deletion_tx = self.current_record_sort_key(deletion_record)?;
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
        let content_descriptor = table.history_storage_table().record_schema();
        let Some(tx_node_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Err(Error::MissingTransaction(tx_id));
        };
        let Some(version) = self.query_version_by_alias_with_descriptor(
            table_name,
            row_uuid,
            VersionLayer::Content,
            tx_id.time,
            tx_node_alias,
            &content_descriptor,
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
            return Ok(Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position,
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
        self.compile_current_query_program_with_settled_view(
            shape,
            binding,
            tier,
            identity,
            output,
            &ReadViewSpec::default(),
            None,
        )
    }

    fn compile_current_query_program_for_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
        read_view: &ReadViewSpec,
    ) -> Result<QueryProgram, Error> {
        self.compile_current_query_program_with_settled_view(
            shape, binding, tier, identity, output, read_view, None,
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
    ) -> Result<QueryProgram, Error> {
        let request = self.current_query_program_request(
            shape,
            binding,
            tier,
            identity,
            output,
            read_view,
            settled_binding_view,
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

    fn global_current_rows_for_index_scan(
        &self,
        table: &TableSchema,
        index: &str,
        prefix: &[Value],
    ) -> Result<GraphBuilder, Error> {
        Ok(GraphBuilder::inline_records(
            table.global_current_storage_tables()[0].record_schema(),
            self.global_current_row_records_for_index_scan(table, index, prefix)?,
        ))
    }

    fn global_current_row_records_for_index_scan(
        &self,
        table: &TableSchema,
        index: &str,
        prefix: &[Value],
    ) -> Result<Vec<Vec<u8>>, Error> {
        let storage_table = global_current_table_name(&table.name);
        self.encoded_records_for_index_scan(&storage_table, index, prefix)
    }

    fn encoded_records_for_index_scan(
        &self,
        storage_table: &str,
        index: &str,
        prefix: &[Value],
    ) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .database
            .index_scan_raw(storage_table, index, prefix)?
            .into_iter()
            .map(|raw| raw.record().raw().to_vec())
            .collect())
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
                    &binding_claim_params_for_shape(&input_shape),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            reads: historical_query_read_set(&input.shape, shape.schema_version(), position),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, shape.query()),
        };
        self.compile_query_program_request(request)
    }

    fn compile_include_deleted_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<QueryProgram, Error> {
        let input_shape = self.normalized_include_deleted_row_set_shape(shape, binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                shape,
                binding,
                query_binding_source_shape_for_parts_if_needed(
                    shape.params(),
                    &binding_claim_params_for_shape(&input_shape),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
            reads: current_query_read_set(
                &input.shape,
                shape.schema_version(),
                self.catalogue.current_schema_version_id,
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
        tx_id: OpenTxId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
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
        let input_shape = self.normalized_row_set_shape(&lowered_shape, &binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                &lowered_shape,
                &binding,
                query_binding_source_shape_for_parts_if_needed(
                    lowered_shape.params(),
                    &binding_claim_params_for_shape(&input_shape),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
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

    fn compile_branch_query_program(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
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
                    &binding_claim_params_for_shape(&input_shape),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
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
        let table = self.query_output_table(shape.query(), shape.schema_version())?;
        let program = self.compile_branch_query_program(
            branch_id,
            shape,
            binding,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
        if shape.query().aggregate.is_some() {
            self.materialize_aggregate_query_rows(shape.query(), &table, deltas)
        } else {
            self.materialize_inline_current_query_rows(&table, deltas)
        }
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
        let program = self.compile_query_program_request(request)?;
        let graph = lowered_terminal_graph(&program, "policy.authorized_rows")?;
        let route_fields = program
            .lowered
            .terminals
            .iter()
            .find_map(|terminal| {
                (terminal.sink == "policy.authorized_rows").then(|| match &terminal.output {
                    OutputTerminalSchema::Fact(fact) => output_routing_fields_for_query_eval(fact),
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
                binding_claim_params_for_shape(&input_shape),
            ),
            shape: input_shape,
        };
        let request = QueryProgramRequest {
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
    ) -> Result<QueryProgramRequest, Error> {
        let lowered_shape;
        let lowered_binding;
        let use_prepared_binding_source = self.can_use_prepared_current_query_plan(shape)
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
        let binding_claim_params = binding_claim_params_for_shape(&input_shape);
        let source_shape = use_prepared_binding_source
            .then(|| {
                query_binding_source_shape_for_parts_if_needed(
                    shape.params(),
                    &binding_claim_params,
                )
            })
            .flatten();
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape_and_policy(
                shape,
                binding,
                source_shape,
                BTreeMap::new(),
                binding_claim_params,
                &policy,
            )?,
            shape: input_shape,
        };
        Ok(QueryProgramRequest {
            reads: query_read_set_for_read_view(
                &input.shape,
                shape.schema_version(),
                self.catalogue.current_schema_version_id,
                tier,
                read_view,
                settled_binding_view,
            )?,
            policy,
            input,
            output: current_query_output_request(output, shape.query()),
        })
    }

    fn normalized_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        _binding: &Binding,
    ) -> Result<NormalizedRowSetShape, Error> {
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
                    &self.catalogue.schema,
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
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        base,
                        &root_source,
                        RowSetNodeId("policy_branch:base:row_id".to_owned()),
                    ),
                    label: "base".to_owned(),
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
                    &self.catalogue.schema,
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
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        branch_current,
                        &root_source,
                        RowSetNodeId(format!("policy_branch:{index}:row_id")),
                    ),
                    label: index.to_string(),
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
                &self.catalogue.schema,
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
            )?;
        }

        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            current = normalize_array_subquery(
                &mut nodes,
                current,
                &self.catalogue.schema,
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
        let claim_params = binding_claim_params_for_shape(&normalized);
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

    pub(super) fn write_policy_query_allows_insert_candidate(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if !policy.inherits.is_empty()
            || policy
                .policy_branches
                .iter()
                .any(|branch| !branch.inherits.is_empty())
        {
            return self.policy_allows_insert_candidate(table, policy, row_uuid, identity, cells);
        }
        let policy_shape = policy.clone().validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
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
                    &binding_claim_params_for_shape(&input_shape),
                ),
                BTreeMap::new(),
                binding_claim_params_for_shape(&input_shape),
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
                mode,
                permission_subject,
                claims,
                attribution,
            },
            other => other,
        };
        let request = QueryProgramRequest {
            reads: current_query_read_set(
                &input.shape,
                policy_shape.schema_version(),
                policy_shape.schema_version(),
                DurabilityTier::Local,
                None,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AppRows,
                policy_shape.query(),
            ),
        };
        let candidate = current_row_from_cells(table, row_uuid, cells)?;
        let inline_sources = BTreeMap::from([(root_source, vec![candidate])]);
        let access_paths = self.current_query_primary_key_access_paths(&policy_shape, &binding)?;
        let program = self.compile_query_program_request_with_inline_sources_and_access_paths(
            request,
            inline_sources,
            access_paths,
        )?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
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
                    let values =
                        binding_values_for_plan(&binding, &params, &program.request.policy)?;
                    let subscription = self.database.bind_shape(shape, &values)?;
                    take_required_sink_deltas(
                        subscription.recv().map_err(|_| Error::SubscriptionClosed)?,
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

    pub(crate) fn query_rows_including_deleted_for_identity(
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
            true,
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
    ) -> Result<Vec<CurrentRow>, Error> {
        if include_deleted {
            let mut rows = self
                .query_rows_including_deleted_with_query_engine(shape, binding, tier, identity)?;
            let query = shape.query();
            self.finish_engine_query_rows(query, &mut rows)?;
            self.apply_projection(query, &mut rows)?;
            return Ok(rows);
        }
        let settled_binding_view = (tier == DurabilityTier::Global)
            .then(|| self.settled_binding_view_key_for_query(shape, binding))
            .transpose()?
            .flatten();
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
        let deltas_result = match plan {
            None => self
                .database
                .query_graph(lowered_app_rows_graph(
                    &program.expect("program is compiled when no prepared plan is supplied"),
                )?)
                .map_err(Error::Groove),
            Some(plan) => match plan.as_ref() {
                PreparedQueryPlan::Prepared { shape, params } => {
                    let values = binding_values_for_plan(binding, params, &policy)?;
                    self.database
                        .bind_shape(*shape, &values)
                        .map_err(Error::Groove)
                        .and_then(|subscription| {
                            subscription
                                .recv()
                                .map_err(|_| Error::SubscriptionClosed)
                                .and_then(|deltas| {
                                    take_required_sink_deltas(deltas, JAZZ_APP_ROWS_SINK)
                                })
                        })
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
        let query = shape.query();
        self.finish_engine_query_rows(query, &mut rows)?;
        self.apply_projection(query, &mut rows)?;
        Ok(rows)
    }

    fn settled_binding_view_key_for_query(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Option<BindingViewKey>, Error> {
        if !self.can_use_prepared_current_query_plan(shape) {
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

    fn can_use_prepared_current_query_plan(&self, shape: &ValidatedQuery) -> bool {
        shape.schema_version() == self.catalogue.current_schema_version_id
            && !self.catalogue.partitions.iter().any(|(table, version)| {
                table == &shape.query().table
                    && *version != self.catalogue.current_schema_version_id
            })
    }

    fn settled_binding_view_source_rows(
        &mut self,
        table: &str,
        binding_view: BindingViewKey,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(row_result_set) = self.query.settled_result_sets.get(&binding_view) else {
            return Ok(Vec::new());
        };
        let row_entries = row_result_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .filter(|(entry_table, _, _)| entry_table.as_str() == table)
            .collect::<Vec<_>>();
        let table_schema = self.table(table)?.clone();
        let content_descriptor = table_schema.history_storage_table().record_schema();
        let mut rows = Vec::with_capacity(row_entries.len());
        for (_, row_uuid, tx_id) in row_entries {
            let tx_node_alias = self
                .node_aliases
                .get(&tx_id.node)
                .copied()
                .ok_or(Error::MissingTransaction(tx_id))?;
            let version = self
                .query_version_by_alias_with_descriptor(
                    table,
                    row_uuid,
                    VersionLayer::Content,
                    tx_id.time,
                    tx_node_alias,
                    &content_descriptor,
                )?
                .ok_or(Error::MissingTransaction(tx_id))?;
            rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
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
        self.apply_projection(query, &mut rows)?;
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
        let program =
            self.compile_include_deleted_query_program(&lowered_shape, &binding, tier, identity)?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
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
        _query: &crate::query::Query,
        table: &TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (index, (record, _weight)) in
            deltas.iter().filter(|(_, weight)| *weight > 0).enumerate()
        {
            let mut cells = BTreeMap::new();
            for field in record.descriptor().fields() {
                let Some(name) = field.name.as_deref() else {
                    continue;
                };
                let logical_name = logical_user_column(name);
                if let Some(column) = table
                    .columns
                    .iter()
                    .find(|column| column.name == logical_name)
                {
                    let value =
                        record.get_idx(record.descriptor().field_index(name).ok_or(
                            Error::InvalidStoredValue("aggregate record field missing"),
                        )?)?;
                    cells.insert(column.name.clone(), value);
                }
            }
            rows.push(current_row_from_cells(
                table,
                aggregate_row_uuid(index),
                &cells,
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
        if position.0 == u64::MAX {
            Ok(self.database.index_scan_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::Bytes(table.as_bytes().to_vec())],
            )?)
        } else {
            Ok(self.database.index_scan_range_raw(
                "jazz_global_changes",
                "by_table_global_seq",
                &[Value::Bytes(table.as_bytes().to_vec()), Value::U64(0)],
                &[
                    Value::Bytes(table.as_bytes().to_vec()),
                    Value::U64(position.0 + 1),
                ],
            )?)
        }
    }

    fn bounded_historical_current_rows(
        &mut self,
        table: &str,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let content_descriptor = table_schema.history_storage_table().record_schema();
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
                .map(|value| deletion_event_from_value(Value::Enum(value)))
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
                .query_version_by_alias_with_descriptor(
                    table,
                    row_uuid,
                    VersionLayer::Content,
                    tx_time,
                    tx_node_alias,
                    &content_descriptor,
                )?
                .ok_or(Error::InvalidStoredValue(
                    "historical content winner is missing",
                ))?;
            rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(crate) fn open_local_maintained_view_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
        read_view: &ReadViewSpec,
        retained_prepared_plan: Option<PreparedQueryPlanHandle>,
    ) -> Result<(LocalMaintainedViewSubscription, RelationSnapshot), Error> {
        let (subscription, maintained, terminal_schemas, transitions, tables) = self
            .open_seeded_maintained_subscription_view(shape, binding, identity, tier, read_view)?;
        let mut local = LocalMaintainedViewSubscription {
            subscription,
            _retained_prepared_plan: retained_prepared_plan,
            maintained,
            terminal_schemas,
            tables,
            result_table: shape.query().table.clone(),
            result_select: shape.query().select.clone(),
            result_set: BTreeSet::new(),
            result_payloads: BTreeMap::new(),
            program_facts: BTreeSet::new(),
        };
        let _initial_delta =
            self.apply_local_maintained_view_transitions(&mut local, transitions)?;
        let initial = self.materialize_local_maintained_relation_snapshot(&local)?;
        Ok((local, initial))
    }

    pub(crate) fn drain_local_maintained_view_subscription(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
    ) -> Result<Option<LocalMaintainedViewSubscriptionUpdate>, Error> {
        let Some(transitions) = self.drain_local_maintained_view_subscription_transitions(local)?
        else {
            return Ok(None);
        };
        let update = self.apply_local_maintained_view_transitions(local, transitions)?;
        Ok(Some(update))
    }

    pub(crate) fn drain_local_maintained_view_subscription_state(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
    ) -> Result<bool, Error> {
        let Some(transitions) = self.drain_local_maintained_view_subscription_transitions(local)?
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
    ) {
        local.result_set = self
            .query
            .settled_result_sets
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        local.program_facts = self
            .query
            .settled_program_facts
            .get(&binding_view_key)
            .cloned()
            .unwrap_or_default();
        local.result_payloads = local
            .program_facts
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::ResultPayload(payload)
                    if payload.member.table_name() == Some(local.result_table.as_str()) =>
                {
                    Some((payload.member.clone(), payload.clone()))
                }
                _ => None,
            })
            .collect();
    }

    fn drain_local_maintained_view_subscription_transitions(
        &mut self,
        local: &mut LocalMaintainedViewSubscription,
    ) -> Result<Option<super::maintained_subscription_view::ResultTransitions>, Error> {
        self.database.flush().map_err(Error::Groove)?;
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        let mut fact_states = BTreeMap::<ProgramFactEntry, (bool, bool)>::new();
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = local.maintained.apply_multisink_deltas(
                        deltas,
                        &local.terminal_schemas,
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
        if states.is_empty() && fact_states.is_empty() {
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
        for (fact, (before, after)) in fact_states {
            match (before, after) {
                (false, true) => transitions.program_fact_adds.push(fact),
                (true, false) => transitions.program_fact_removes.push(fact),
                _ => {}
            }
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
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut added_edges = Vec::new();
        let mut removed_edges = Vec::new();
        for member in transitions.result_payload_removes {
            local.result_payloads.remove(&member);
        }
        for (member, payload) in transitions.result_payload_adds {
            if member.table_name() == Some(local.result_table.as_str()) {
                local.result_payloads.insert(member, payload);
            }
        }
        for member in transitions.adds {
            if member.table_name() != Some(local.result_table.as_str()) {
                continue;
            }
            if local.result_set.insert(member.clone()) && materialize_update {
                if let Some(row) =
                    self.materialize_local_maintained_view_result_member(local, &member)?
                {
                    added.push(row);
                }
            }
        }
        for member in transitions.removes {
            if member.table_name() != Some(local.result_table.as_str()) {
                continue;
            }
            if local.result_set.remove(&member) {
                if materialize_update && let Some((table, row_uuid, _)) = member.as_row() {
                    removed.push((table.to_string(), row_uuid));
                }
            }
        }
        for fact in transitions.program_fact_removes {
            if local.program_facts.remove(&fact) {
                if materialize_update && let ProgramFactEntry::RelationEdge(edge) = fact {
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
            let edge = materialize_update
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
        Ok(LocalMaintainedViewSubscriptionUpdate {
            added,
            removed,
            added_edges,
            removed_edges,
        })
    }

    fn materialize_local_maintained_relation_snapshot(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<RelationSnapshot, Error> {
        let mut cache = self.preload_local_maintained_materialization_cache(local)?;
        let mut rows = Vec::with_capacity(local.result_set.len());
        let mut row_keys = BTreeSet::new();
        for member in &local.result_set {
            if let Some(row) = self.materialize_local_maintained_view_result_member_with_cache(
                local, member, &mut cache,
            )? {
                row_keys.insert((row.table().to_owned(), row.row_uuid()));
                rows.push(row);
            }
        }
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
        Ok(RelationSnapshot {
            root_count,
            rows,
            edges,
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
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        let table = self.table(entry.0.as_str())?.clone();
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
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        let table = self.table(entry.0.as_str())?.clone();
        if local.result_select.is_some()
            && let Some(payload) = local.result_payloads.get(member)
        {
            let mut row = self.current_row_from_result_payload(&table, payload)?;
            if let Some(columns) = &local.result_select {
                row = row.project(&table, columns)?;
            }
            return Ok(Some(row));
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
        let mut row = self.current_row_from_materialized_version_with_materialization_cache(
            &table, &version, cache,
        )?;
        if let Some(columns) = &local.result_select {
            row = row.project(&table, columns)?;
        }
        Ok(Some(row))
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
                for table in &tables {
                    for (storage_table, descriptor) in self.version_storage_sources(table)? {
                        let raws = self
                            .database
                            .index_scan_range_raw(
                                &storage_table,
                                "by_tx",
                                &[Value::U64(start.0), Value::U64(alias.0)],
                                &[Value::U64(end.0), Value::U64(0)],
                            )?
                            .into_iter()
                            .map(|raw| raw.raw().to_vec())
                            .collect::<Vec<_>>();
                        for raw in raws {
                            let version = self.decode_history_record(
                                table,
                                BorrowedRecord::new(&raw, &descriptor),
                            )?;
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
        let len =
            self.large_value_column_len_with_materialization_cache(table, version, column, cache)?;
        let refs = self.large_value_extent_refs_for_version_with_materialization_cache(
            table, version, column, kind, cache,
        )?;
        let tx_id = self.version_tx_id(version)?;
        encode_large_value_handle(table, version.row_uuid(), column, tx_id, kind, len, refs)
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
        loop {
            let version = self
                .tx_versions_for_materialization(current, cache)?
                .iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .cloned()
                .ok_or(Error::MissingTransaction(current))?;
            if let Some(value) =
                self.large_value_checkpoint(table, version.row_uuid(), column, current)?
            {
                checkpoint_len = Some(value.len());
                break;
            }
            let parents = version.parents();
            suffix.push(version);
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut value_len = checkpoint_len.unwrap_or_default();
        for version in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(table, column)? else {
                continue;
            };
            match column_large_value_kind(table, column)? {
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
        loop {
            let version = self
                .tx_versions_for_materialization(current, cache)?
                .iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .cloned()
                .ok_or(Error::MissingTransaction(current))?;
            let parents = version.parents();
            suffix.push(version);
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut refs = Vec::new();
        for version in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(table, column)? else {
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

    pub(crate) fn prepare_query_binding_for_link_with_shared_claim_fragments(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<(ValidatedQuery, Binding, PreparedQueryPlanHandle), Error> {
        let (shape, binding) = self.query_binding_for_link(shape, binding)?;
        let program = self.compile_current_query_program(
            &shape,
            &binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        let has_claim_binding = !program.lowered.parameters.claim_params.is_empty();
        let plan = if has_claim_binding {
            let key = (
                shape.shape_id(),
                tier,
                query_binding_value_signature(&binding),
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
        self.apply_projection(query, &mut rows)?;
        Ok(rows)
    }

    pub(crate) fn query_rows_for_link_with_prepared_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_with_prepared_plan_for_identity(
            shape,
            binding,
            tier,
            prepared_plan,
            identity,
        )
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
        self.query_relation_snapshot_for_link_in_read_view(
            shape,
            binding,
            tier,
            identity,
            &ReadViewSpec::default(),
        )
    }

    pub(crate) fn query_relation_snapshot_for_link_in_read_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        read_view: &ReadViewSpec,
    ) -> Result<RelationSnapshot, Error> {
        let program = self.compile_current_query_program_for_read_view(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::RelationSnapshot,
            read_view,
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

        let limits = Self::relation_snapshot_no_order_limits(&shape.query().array_subqueries);
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
        for candidate in candidates {
            let group = (
                candidate.edge.source_table.clone(),
                candidate.edge.source_row,
                candidate.edge.relation.clone(),
            );
            let count = counts.entry(group).or_default();
            if limits
                .get(&candidate.edge.relation)
                .is_some_and(|limit| *count >= *limit)
            {
                continue;
            }
            *count += 1;
            if row_keys.insert((
                candidate.edge.target_table.clone(),
                candidate.edge.target_row,
            )) {
                let target_table = self
                    .table_in_schema(&candidate.edge.target_table, shape.schema_version())?
                    .clone();
                let row = self.materialize_relation_edge_target_row(
                    read_view,
                    &target_table,
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
        if let Some(version) = self.query_version_by_alias_with_descriptor(
            target_table_name,
            target_row,
            VersionLayer::Content,
            target_tx_time,
            target_tx_node,
            &target_table.history_storage_table().record_schema(),
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

    fn relation_snapshot_no_order_limits(subqueries: &[ArraySubquery]) -> BTreeMap<String, usize> {
        let mut limits = BTreeMap::new();
        for subquery in subqueries {
            if subquery.order_by.is_empty()
                && let Some(limit) = subquery.limit
            {
                limits.insert(subquery.column_name.clone(), limit);
            }
            limits.extend(Self::relation_snapshot_no_order_limits(
                &subquery.nested_arrays,
            ));
        }
        limits
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
        Ok(rows)
    }

    pub(crate) fn subscription_snapshot_for_link(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        #[cfg(test)]
        record_subscription_snapshot_for_link_call();
        self.subscription_snapshot_for_link_with_prepared_plan(shape, binding, tier, identity, None)
    }

    pub(crate) fn subscription_snapshot_for_link_with_prepared_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        prepared_plan: Option<&PreparedQueryPlanHandle>,
    ) -> Result<RelationSnapshot, Error> {
        if shape.query().array_subqueries.is_empty() {
            let rows = self.query_rows_for_link_with_prepared_plan(
                shape,
                binding,
                tier,
                identity,
                prepared_plan,
            )?;
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
        self.query_rows_including_deleted_for_identity(shape, binding, tier, None, identity)
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

    fn finish_engine_query_rows(
        &self,
        query: &crate::query::Query,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        if query.aggregate.is_some() {
            self.apply_query_order(query, rows)?;
            apply_query_window(query, rows);
            return Ok(());
        }
        // Groove lowering owns membership/windowing, but one-shot APIs still
        // return a deterministic Vec. Re-apply ordering to the selected rows
        // without re-applying pagination.
        self.apply_query_order(query, rows)
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

    /// Evaluate a validated query inside an open exclusive transaction.
    pub fn tx_query(
        &mut self,
        tx_id: OpenTxId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.tx_query_for_identity(tx_id, shape, binding, AuthorId::SYSTEM)
    }

    /// Evaluate a validated query inside an open exclusive transaction as `identity`.
    pub fn tx_query_for_identity(
        &mut self,
        tx_id: OpenTxId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let query = shape.query();
        let predicate_len = self.open_tx(tx_id)?.predicate_reads.len();
        let table = self.table(&query.table)?.clone();
        let program = self.compile_open_tx_query_program(
            tx_id,
            shape,
            binding,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
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
        self.finish_engine_query_rows(query, &mut rows)?;
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
            query_binding_value_signature(binding),
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
        let graph = lowered_app_rows_graph(&program)?;
        let params = prepared_params_from_domain(&program.lowered.parameters);
        let route_params = prepared_route_param_names(&program.lowered.parameters);
        let param_names = params
            .iter()
            .map(|param| param.name.clone())
            .collect::<Vec<_>>();
        let binding_descriptor = RecordDescriptor::new(
            param_names
                .iter()
                .cloned()
                .zip(params.iter().map(|param| param.ty.value_type())),
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
            let route_fields = terminal_route_fields(
                &route_params,
                &app_row_terminal_route_eligible_fields(&program.lowered.output)?,
            );
            let prepared = self.database.prepare(
                [groove::ivm::RoutedMultisinkTerminal::new(
                    JAZZ_APP_ROWS_SINK,
                    graph,
                    route_fields,
                    app_row_fields,
                )],
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
        let program = self.compile_current_query_program_for_read_view(
            &shape,
            &binding,
            tier,
            identity,
            CurrentQueryProgramOutput::MaintainedView,
            read_view,
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
        let subscription =
            self.subscribe_lowered_program(program, &binding, binding_source_shape)?;
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
        ))
    }

    fn subscribe_lowered_program(
        &mut self,
        program: QueryProgram,
        binding: &Binding,
        binding_source_shape: String,
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
                .zip(params.iter().map(|param| param.ty.value_type())),
        );
        let values = binding_values_for_plan(binding, &params, &program.request.policy)?;
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
                Ok(RoutedMultisinkTerminal::new(
                    terminal.sink,
                    terminal.graph,
                    route_fields,
                    public_fields,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let prepared =
            self.database
                .prepare(terminals, binding_source_shape, binding_descriptor)?;
        self.database
            .bind_shape(prepared.id(), &values)
            .map_err(Error::Groove)
    }

    fn policy_filtered_current_source_graph_via_query_engine(
        &mut self,
        policy_request: QueryProgramRequest,
        base: GraphBuilder,
        output_fields: &[String],
    ) -> Result<PolicyAuthorizationGraph, Error> {
        self.query_engine_read_metrics
            .policy_authorized_source_joins += 1;
        let authorized = match self.policy_authorization_row_id_graph(policy_request) {
            Ok(authorized) => authorized,
            Err(Error::QueryCapability(_err)) => PolicyAuthorizationGraph {
                graph: empty_authorized_row_id_graph(),
                route_fields: BTreeSet::new(),
            },
            Err(err) => return Err(err),
        };
        if authorized.route_fields.is_empty() {
            let fields = output_fields
                .iter()
                .map(|field| ProjectField::renamed(left_field(&field), field.clone()))
                .collect::<Vec<_>>();
            return Ok(PolicyAuthorizationGraph {
                graph: GraphBuilder::join(base, authorized.graph, ["row_uuid"], ["row_uuid"])
                    .project_fields(fields),
                route_fields: authorized.route_fields,
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
        Ok(PolicyAuthorizationGraph {
            graph: GraphBuilder::join(base, authorized.graph, ["row_uuid"], ["row_uuid"])
                .project_fields(fields),
            route_fields: authorized.route_fields,
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
    ) -> Result<QueryProgramRequest, Error> {
        self.table_read_policy_authorization_request_with_root_visibility(
            policy_schema_version,
            table_name,
            identity,
            param_binding_mode,
            tier,
            binding_source_shape,
            binding_user_params,
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
        let mut claim_params = binding_claim_params_for_shape(&input_shape);
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
    ) -> Result<QueryProgramRequest, Error> {
        self.table_read_policy_authorization_request_with_root_visibility(
            policy_schema_version,
            table_name,
            identity,
            ParamBindingMode::InlineAllReachableSeeds,
            tier,
            binding_source_shape,
            binding_user_params,
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
        let query = authorization_query_from_read_policy(table);
        if !query.includes.is_empty() {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view policy slice does not support include policies",
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
                "maintained policy source filters with runtime parameters must lower through query-engine binding sources"
                    .to_owned(),
            ));
        }
        let binding = policy_shape.bind(BTreeMap::new())?;
        let mut input_shape = if include_deleted_root {
            self.normalized_include_deleted_row_set_shape(&policy_shape, &binding)?
        } else {
            self.normalized_row_set_shape(&policy_shape, &binding)?
        };
        let mut claim_params = binding_claim_params_for_shape(&input_shape);
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
        let request = QueryProgramRequest {
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
        let mut claim_params = binding_claim_params_for_shape(&input_shape);
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
        if tier == DurabilityTier::Global {
            let content = GraphBuilder::table(global_current_table_name(&table.name))
                .project(global_current_storage_fields(table, true, true));
            let deleted = GraphBuilder::table(register_global_current_table_name(&table.name))
                .filter(PredicateExpr::eq("_deletion", Value::Enum(0)))
                .project(["row_uuid"]);
            return Ok(GraphBuilder::anti_join(
                content,
                deleted,
                ["row_uuid"],
                ["row_uuid"],
            ));
        }
        let payload = content_version_current_source_graph(table, tier, true).project([
            "row_uuid",
            "tx_time",
            "tx_node_id",
            "schema_version",
            "parents",
            "global_seq",
        ]);
        Ok(GraphBuilder::join(
            visible_current_graph(table, tier),
            payload,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(
            std::iter::once(ProjectField::renamed("left.row_uuid", "row_uuid"))
                .chain(table.columns.iter().map(|column| {
                    let field = user_column_field(&column.name);
                    ProjectField::renamed(left_field(&field), field)
                }))
                .chain([
                    ProjectField::renamed("left.$createdBy", "created_by"),
                    ProjectField::renamed("left.$createdAt", "created_at"),
                    ProjectField::renamed("left.$updatedBy", "updated_by"),
                    ProjectField::renamed("left.$updatedAt", "updated_at"),
                    ProjectField::renamed("left.tx_time", "tx_time"),
                    ProjectField::renamed("left.tx_node_id", "tx_node_id"),
                    ProjectField::renamed("right.schema_version", "schema_version"),
                    ProjectField::renamed("right.parents", "parents"),
                    ProjectField::renamed("right.global_seq", "global_seq"),
                ]),
        ))
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
        let mut claim_values = default_permission_scope_claim_values(writer);
        if let Some(claims) = claims {
            claim_values.extend(claims.clone());
        }
        let mut binding_values = BTreeMap::new();
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
        bind_scope_claim_operands(&mut query, &claim_values, &mut binding_values);
        let shape = query.validate(&self.catalogue.schema)?;
        let binding = shape.bind(binding_values)?;
        Ok(Some((shape, binding)))
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
        collect_claim_field_params_from_node(node, &mut params);
    }
    params
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
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    match node {
        RowSetExpr::Filter { predicate, .. } | RowSetExpr::Join { on: predicate, .. } => {
            collect_claim_field_params_from_predicate(predicate, params);
        }
        RowSetExpr::RecursiveRelation {
            frontier_key,
            dedupe_keys,
            ..
        } => {
            collect_claim_field_param(frontier_key, ColumnType::Uuid, params);
            for key in dedupe_keys {
                collect_claim_field_param(key, ColumnType::Uuid, params);
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
                collect_claim_field_param(key, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::CorrelatedPathProjection { correlation, .. } => {
            collect_claim_field_params_from_predicate(correlation, params);
        }
        RowSetExpr::OrderBy { keys, .. } => {
            for key in keys {
                collect_claim_field_param(&key.value, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::Slice {
            partition_by,
            tie_breaker,
            ..
        } => {
            for value in partition_by.iter().chain(tie_breaker) {
                collect_claim_field_param(value, ColumnType::Uuid, params);
            }
        }
        RowSetExpr::Aggregate {
            group_by, outputs, ..
        } => {
            for value in group_by {
                collect_claim_field_param(value, ColumnType::Uuid, params);
            }
            for output in outputs {
                if let Some(input) = &output.input {
                    collect_claim_field_param(input, output.output.ty.clone(), params);
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
    params: &mut BTreeMap<String, ProgramClaimParam>,
) {
    match predicate {
        NormalizedPredicateExpr::True | NormalizedPredicateExpr::False => {}
        NormalizedPredicateExpr::Compare { left, right, .. } => {
            collect_claim_field_param(left, ColumnType::Uuid, params);
            collect_claim_field_param(right, ColumnType::Uuid, params);
        }
        NormalizedPredicateExpr::In { value, options } => {
            collect_claim_field_param(value, ColumnType::Uuid, params);
            for option in options {
                collect_claim_field_param(option, ColumnType::Uuid, params);
            }
        }
        NormalizedPredicateExpr::ArrayContains { value, needle }
        | NormalizedPredicateExpr::TextContains { value, needle } => {
            collect_claim_field_param(value, ColumnType::Uuid, params);
            collect_claim_field_param(needle, ColumnType::Uuid, params);
        }
        NormalizedPredicateExpr::IsNull(value) | NormalizedPredicateExpr::IsNotNull(value) => {
            collect_claim_field_param(value, ColumnType::Uuid, params);
        }
        NormalizedPredicateExpr::And(children) | NormalizedPredicateExpr::Or(children) => {
            for child in children {
                collect_claim_field_params_from_predicate(child, params);
            }
        }
        NormalizedPredicateExpr::Not(child) => {
            collect_claim_field_params_from_predicate(child, params);
        }
    }
}

fn collect_claim_field_param(
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

fn exact_known_state_declaration_if_within_limits(
    shape_id: ShapeId,
    subscription: SubscriptionKey,
    values: &[Value],
    refs: Vec<RowVersionRef>,
) -> Option<KnownStateDeclaration> {
    if refs.len() > MAX_KNOWN_STATE_EXACT_REFS {
        return None;
    }
    let declaration = KnownStateDeclaration::ExactVersionSet { versions: refs };
    let subscribe = SyncMessage::Subscribe(Subscribe {
        shape_id,
        subscription,
        values: values.to_vec(),
        known_state: Some(declaration.clone()),
    });
    let Ok(bytes) = postcard::to_allocvec(&subscribe) else {
        return None;
    };
    (bytes.len() <= MAX_SYNC_MESSAGE_BYTES).then_some(declaration)
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
                let value = prepared_claim_value(path, policy)?;
                Ok::<_, Error>(coerce_prepared_binding_value(value, &param.ty))
            }
        })
        .collect()
}

fn prepared_claim_value(path: &ClaimPath, policy: &PolicyContext) -> Result<Value, Error> {
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
        return Ok(value.clone());
    }
    if let Some(value) = default_policy_claim_values(*permission_subject).get(name) {
        return Ok(value.clone());
    }
    Err(Error::InvalidStoredValue(
        "claim prepared param is not bound",
    ))
}

fn coerce_prepared_binding_value(value: Value, column_type: &groove::schema::ColumnType) -> Value {
    match (value, column_type) {
        (Value::Uuid(value), groove::schema::ColumnType::String) => {
            Value::String(value.to_string())
        }
        (Value::String(value), groove::schema::ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_prepared_binding_value(*value, column_type),
        ))),
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
    rows.sort_by(|left, right| {
        left.row_uuid()
            .to_bytes()
            .cmp(&right.row_uuid().to_bytes())
            .then_with(|| left.projected_tx_alias().cmp(&right.projected_tx_alias()))
            .then_with(|| left.record.raw().cmp(right.record.raw()))
    });
}

fn aggregate_row_cell(row: &CurrentRow, column: &str) -> Option<Value> {
    let user_name = user_column_field(column);
    let idx = row.record.descriptor().fields().iter().position(|field| {
        field.name.as_deref() == Some(user_name.as_str()) || field.name.as_deref() == Some(column)
    })?;
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
            &aggregate.alias,
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
            source_table
                .columns
                .iter()
                .find(|candidate| &candidate.name == column)
                .map(|column| column.column_type.clone())
                .ok_or(Error::InvalidStoredValue("aggregate input column missing"))
        }
        AggregateFunction::Avg => Ok(ColumnType::F64),
    }
}

fn aggregate_row_uuid(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(b"jazzagg:");
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
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
        fields.extend(["schema_version".to_owned(), "parents".to_owned()]);
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
fn historical_current_graph_full_scan(table: &TableSchema, position: GlobalSeq) -> GraphBuilder {
    let cut_predicate = PredicateExpr::And(vec![
        PredicateExpr::eq("table_name", Value::Bytes(table.name.as_bytes().to_vec())),
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
    let nullable_deletion_type = ValueType::Nullable(Box::new(ValueType::Enum(
        groove::records::EnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum"),
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
    let history_rows = GraphBuilder::table(history_table_name(&table.name))
        .project(maintained_view_history_storage_field_names(table));
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
            PredicateExpr::eq("deletion", Value::Nullable(Some(Box::new(Value::Enum(1))))),
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

fn include_deleted_current_graph(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
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
    fields
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use groove::schema::{ColumnSchema, ColumnType};
    use groove::storage::{Durability, RocksDbStorage};

    use crate::ids::{AuthorId, BranchId, NodeUuid, RowUuid};
    use crate::node::query_engine::{CoverageScope, ProgramFactOutput};
    use crate::node::{MergeableCommit, NodeState};
    use crate::peer::PeerState;
    use crate::protocol::{
        ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions, ShapeAst, Subscribe, SyncMessage,
    };
    use crate::query::{
        Aggregate, JoinSourceLookup, OrderDirection, Query, claim, col, contains, eq, gt, in_list,
        lit, lte, param,
    };
    use crate::schema::{JazzSchema, TableSchema};

    use super::*;

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
        let deltas = node
            .database
            .query_graph(historical_current_graph_full_scan(table, position))
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
        let current_deltas = node
            .database
            .query_graph(content_version_current_source_graph(
                &table,
                DurabilityTier::Global,
                false,
            ))
            .expect("query denormalized current payload");
        let current_rows = current_deltas
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(record, _)| record.raw().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(current_rows.len(), 1);

        let history_deltas = node
            .database
            .query_graph(
                GraphBuilder::table(history_table_name("issues"))
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
            .compile_branch_query_program(
                branch_id,
                &shape,
                &binding,
                AuthorId::SYSTEM,
                CurrentQueryProgramOutput::MaintainedView,
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
            filters: vec![eq(col("kind"), lit("sync"))],
        });
        let shape = query.validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();

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
