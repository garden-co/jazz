//! Query execution, shape registration, binding routing, and read-set
//! evaluation for `jazz/SPEC/6_queries.md`. This module owns lowering validated Jazz
//! queries to groove plans, evaluating one-shot reads, recording predicate reads,
//! and applying binding deltas; the pure AST lives in [`crate::query`], policy
//! checks in [`super::policy`], and sync view payload assembly in [`super::views`].
//! It is the node layer's query bridge to groove IVM.

use super::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use groove::ivm::RoutedMultisinkTerminal;
use groove::ivm::TopByOrder;
use groove::ivm::{MultisinkDeltas, MultisinkSubscription, RecordDeltas};
use groove::records::{EnumSchema, RecordDescriptor, ValueType};
use groove::schema::ColumnType;

use super::maintained_subscription_view::{MaintainedSubscriptionView, MaintainedTerminalSchemas};
use super::policy::ViewEvaluationContext;
use super::query_engine::{
    AggregateExpr as NormalizedAggregateExpr, AggregateFunction as NormalizedAggregateFunction,
    AppProjectionTree, AppRowOutputRequest, ClaimPath, ClosurePath, ClosurePathKind,
    ClosurePathSegment, ClosureRootGate, ComparisonOp as NormalizedComparisonOp,
    CorrelationRequirement, DataSource, FieldProjection, FrontierId, JoinContribution,
    JoinMode as NormalizedJoinMode, LensSelection, NormalizedRowSetShape, NormalizedShapeIdentity,
    NormalizedValueRef, OrderKey as NormalizedOrderKey, OutputTerminalSchema, OverlayRef,
    OverlayStack, PayloadProjection, PolicyContext, PolicyEnforcementMode,
    PredicateExpr as NormalizedPredicateExpr, ProgramBinding, ProgramFactKey, ProgramOutputSchemas,
    ProgramPathId, ProvenanceField, QueryProgram, QueryProgramRequest, QueryReadSet,
    ReachableContribution, ReadView, RequestedReadSet, RequestedSourceStage, ResolvedSource,
    ResultId, ResultMembershipVersionSchema, ResultRowRef, RowIdRef, RowProjection,
    RowRefSchema as QueryEngineRowRefSchema, RowSetExpr, RowSetNodeId, RowSetOutputRequest,
    RowSetProgramInput, RowVisibility, SchemaFamilySelection, SchemaProjection,
    SortDirection as NormalizedSortDirection, SourceExpr, SourceGap, SourceId,
    SourceMetadataFields, SourceMetadataRequirement, SourcePath, SourceRequest, SourceRequirements,
    SourceResolutionError, SourceResolver, SourceRole, SourceRowShape, StorageSchemaSelection,
    TypedOutputField, ValueSourceColumn, ValueSourceMode, VersionIdentityFields,
    VersionedRowRefSchema, lower_query_program,
};
use crate::protocol::{
    BindingViewKey, ResultMemberEntry, ShapeAst, ShapeBody, Subscribe, SubscriptionKey,
};
use crate::query::{
    Aggregate, AggregateFunction, AggregateQuery, ArraySubquery, ArraySubqueryRequirement, Binding,
    Include, JoinTarget, JoinVia, Operand, OrderDirection, PolicyBranch, Predicate,
    QUERY_NAMESPACE, Query as JazzQuery, ShapeId, ValidatedQuery,
};

const CLAIM_PARAM_PREFIX: &str = "__jazz_claim_";
const ROUTE_PARAM_PREFIX: &str = "__jazz_route_";
pub(crate) const JAZZ_APP_ROWS_SINK: &str = "app_rows";

pub(crate) struct ReachableGraphs {
    pub(crate) closure: GraphBuilder,
    #[allow(dead_code)] // Test-oracle reachable constituent helpers still inspect edge rows.
    pub(crate) edge_current: GraphBuilder,
    pub(crate) access_current: GraphBuilder,
    pub(crate) seed_param: String,
    pub(crate) seed_param_available: bool,
}

pub(crate) struct LocalMaintainedViewSubscription {
    subscription: MultisinkSubscription,
    maintained: MaintainedSubscriptionView,
    terminal_schemas: MaintainedTerminalSchemas,
    tables: BTreeMap<String, TableSchema>,
    result_table: String,
    result_set: BTreeSet<ResultMemberEntry>,
    identity: AuthorId,
}

pub(crate) fn take_required_sink_deltas(
    mut deltas: MultisinkDeltas,
    sink: &str,
) -> Result<RecordDeltas, Error> {
    deltas.sinks.remove(sink).ok_or({
        Error::InvalidStoredValue("multisink subscription did not deliver required sink")
    })
}

pub(crate) fn apply_maintained_multisink_deltas(
    maintained: &mut MaintainedSubscriptionView,
    deltas: MultisinkDeltas,
    terminal_schemas: &MaintainedTerminalSchemas,
    tables: &BTreeMap<String, TableSchema>,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<super::maintained_subscription_view::ResultTransitions, Error> {
    let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
    for (sink, deltas) in deltas.sinks {
        let delta_transitions = maintained.apply_typed_deltas(
            &sink,
            &deltas,
            terminal_schemas,
            tables,
            node_aliases,
        )?;
        transitions.adds.extend(delta_transitions.adds);
        transitions.removes.extend(delta_transitions.removes);
    }
    Ok(transitions)
}

fn app_row_terminal_fields(output: &ProgramOutputSchemas) -> Result<Vec<String>, Error> {
    let ProgramOutputSchemas::RowSet(terminals) = output;
    let app_rows = terminals
        .iter()
        .find_map(|terminal| match terminal {
            OutputTerminalSchema::AppRows(rows) => Some(rows),
            OutputTerminalSchema::Fact(_) => None,
        })
        .ok_or(Error::InvalidStoredValue(
            "query program did not emit app row terminal",
        ))?;
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
    parameters
        .user_params
        .iter()
        .map(|(name, ty)| PreparedQueryParam {
            name: name.clone(),
            ty: ty.clone(),
            source: PreparedQueryParamSource::User,
        })
        .chain(
            parameters
                .hidden_params
                .iter()
                .map(|(name, ty)| PreparedQueryParam {
                    name: name.clone(),
                    ty: ty.clone(),
                    source: PreparedQueryParamSource::Claim {
                        name: claim_name_from_param(name).to_owned(),
                    },
                }),
        )
        .collect()
}

fn prepared_route_param_names(parameters: &super::query_engine::ParameterDomain) -> Vec<String> {
    parameters.routing_params.iter().cloned().collect()
}

fn terminal_route_fields(route_params: &[String], public_fields: &[String]) -> Vec<String> {
    let public_fields = public_fields.iter().collect::<BTreeSet<_>>();
    route_params
        .iter()
        .filter(|param| public_fields.contains(param))
        .cloned()
        .collect()
}

fn terminal_public_fields(terminal: &OutputTerminalSchema) -> Result<Vec<String>, Error> {
    match terminal {
        OutputTerminalSchema::AppRows(rows) => descriptor_field_names(&rows.descriptor),
        OutputTerminalSchema::Fact(fact) => fact_public_fields(&fact.schema),
    }
}

fn fact_public_fields(
    schema: &super::query_engine::ProgramFactSchema,
) -> Result<Vec<String>, Error> {
    use super::query_engine::ProgramFactSchema;

    match schema {
        ProgramFactSchema::ResultMembership(schema) => {
            let mut fields = vec![schema.table_field.clone(), schema.row_field.clone()];
            fields.extend(schema.branch_or_prefix_field.clone());
            fields.extend(result_membership_version_fields(&schema.version));
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
            ProgramFactSchema::ResultMembership(_)
            | ProgramFactSchema::RelationEdges(_)
            | ProgramFactSchema::VersionWitnesses(_)
            | ProgramFactSchema::ReplacementWitnesses(_) => unreachable!(),
        })),
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
    pub(crate) adds: Vec<CurrentRow>,
    pub(crate) removes: Vec<ResultMemberEntry>,
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

enum CurrentQueryProgramOutput {
    AppRows,
    RelationSnapshot,
    MaintainedView,
}

struct CurrentQuerySourceResolver<'a, S> {
    node: &'a mut NodeState<S>,
    read_view: &'a ReadView<RequestedSourceStage>,
    policy: &'a PolicyContext,
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
        if history_position.is_none()
            && self.read_view.read_schema != self.node.catalogue.current_schema_version_id
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let (graph, descriptor, metadata) = if let Some(position) = history_position {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let descriptor = current_row_descriptor(&table);
            if self.read_view.read_schema != self.node.catalogue.current_schema_version_id
                || self
                    .node
                    .catalogue
                    .partitions
                    .iter()
                    .any(|(logical, version)| {
                        logical == &request.source.table
                            && *version != self.node.catalogue.current_schema_version_id
                    })
            {
                let rows = self
                    .node
                    .projected_historical_current_rows(
                        &request.source.table,
                        self.read_view.read_schema,
                        position,
                    )
                    .map_err(|_| {
                        source_resolution_error(request, SourceGap::HistoricalStorageCut)
                    })?;
                let graph = inline_current_graph(&table, rows).map_err(|_| {
                    source_resolution_error(request, SourceGap::HistoricalStorageCut)
                })?;
                (graph, descriptor, BTreeMap::new())
            } else {
                (
                    historical_current_graph(&table, position),
                    descriptor,
                    BTreeMap::new(),
                )
            }
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
            let graph = inline_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let descriptor = current_row_descriptor(&table);
            (graph, descriptor, BTreeMap::new())
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
            (graph, descriptor, BTreeMap::new())
        } else if request.visibility == RowVisibility::IncludeDeleted {
            (
                include_deleted_current_graph(
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                ),
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
            )
        } else {
            resolved_current_source_graph(
                self.node,
                &table,
                graph_tier.expect("visible current source has a tier"),
                &request.requirements,
                self.policy,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
        };
        Ok(ResolvedSource {
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
        })
    }
}

fn source_resolution_error(request: &SourceRequest, gap: SourceGap) -> SourceResolutionError {
    SourceResolutionError {
        request: Box::new(request.clone()),
        gap,
    }
}

fn resolved_current_source_graph<S>(
    node: &NodeState<S>,
    table: &TableSchema,
    tier: DurabilityTier,
    requirements: &SourceRequirements,
    policy: &PolicyContext,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
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
    let base = match policy {
        PolicyContext::System => {
            if needs_version_witnesses {
                node.maintained_view_content_current_with_version(table, tier)?
                    .project_fields(storage_to_canonical_current_source_fields(table, true))
            } else {
                visible_current_graph(table, tier)
                    .project_fields(canonical_current_source_fields(table, false))
            }
        }
        PolicyContext::Identity {
            permission_subject, ..
        } => {
            let policy_shape = node.maintained_view_table_policy_shape_with_mode(
                table,
                *permission_subject,
                ParamBindingMode::InlineAllReachableSeeds,
            )?;
            let mut output_fields = global_current_storage_fields(table);
            if needs_version_witnesses {
                output_fields.extend(["schema_version".to_owned(), "parents".to_owned()]);
            }
            let base = node.maintained_view_content_current_with_version(table, tier)?;
            node.apply_maintained_view_filters(base, &policy_shape, table, output_fields, tier)?
                .project_fields(storage_to_canonical_current_source_fields(
                    table,
                    needs_version_witnesses,
                ))
        }
    };
    let graph = if metadata.is_empty() {
        base
    } else {
        base.project_fields(fields)
    };
    Ok((graph, descriptor, metadata))
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
                .map(|column| ProjectField::named(format!("user_{}", column.name))),
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
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(
            table
                .columns
                .iter()
                .map(|column| ProjectField::named(format!("user_{}", column.name))),
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
    }
    if metadata.contains_key(&SourceMetadataRequirement::Coverage) {
        fields.push(("coverage".to_owned(), ValueType::String));
    }
    RecordDescriptor::new(fields)
}

fn current_row_descriptor_fields(table: &TableSchema) -> Vec<(String, ValueType)> {
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

fn join_source_id(join: &JoinVia, index: usize) -> SourceId {
    SourceId {
        table: join.table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!("join_via:{index}"))],
        },
    }
}

fn current_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tier: DurabilityTier,
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
                    data: DataSource::Current,
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
                data: DataSource::Current,
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

fn current_query_output_request(
    output: CurrentQueryProgramOutput,
    query: &JazzQuery,
) -> RowSetOutputRequest {
    let facts = match output {
        CurrentQueryProgramOutput::AppRows => BTreeSet::new(),
        CurrentQueryProgramOutput::RelationSnapshot => BTreeSet::from([
            ProgramFactKey::RelationEdges,
            ProgramFactKey::PathCorrelationCoverage,
        ]),
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
    source: &SourceId,
    predicates: &[Predicate],
) -> Result<NormalizedPredicateExpr, Error> {
    match predicates {
        [] => Ok(NormalizedPredicateExpr::True),
        [predicate] => normalize_predicate(source, predicate),
        _ => predicates
            .iter()
            .map(|predicate| normalize_predicate(source, predicate))
            .collect::<Result<Vec<_>, Error>>()
            .map(NormalizedPredicateExpr::And),
    }
}

fn normalize_predicate(
    source: &SourceId,
    predicate: &Predicate,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => NormalizedPredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| normalize_predicate(source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Any(predicates) => NormalizedPredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| normalize_predicate(source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Not(predicate) => {
            NormalizedPredicateExpr::Not(Box::new(normalize_predicate(source, predicate)?))
        }
        Predicate::Eq(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Eq, right)?
        }
        Predicate::Ne(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Ne, right)?
        }
        Predicate::Gt(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Gt, right)?
        }
        Predicate::Gte(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Gte, right)?
        }
        Predicate::Lt(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Lt, right)?
        }
        Predicate::Lte(left, right) => {
            normalize_compare(source, left, NormalizedComparisonOp::Lte, right)?
        }
        Predicate::In(value, options) => NormalizedPredicateExpr::In {
            value: normalize_operand(source, value)?,
            options: options
                .iter()
                .map(|operand| normalize_operand(source, operand))
                .collect::<Result<Vec<_>, Error>>()?,
        },
        Predicate::Contains(value, needle) => NormalizedPredicateExpr::ArrayContains {
            value: normalize_operand(source, value)?,
            needle: normalize_operand(source, needle)?,
        },
        Predicate::IsNull(value) => {
            NormalizedPredicateExpr::IsNull(normalize_operand(source, value)?)
        }
    })
}

fn normalize_compare(
    source: &SourceId,
    left: &Operand,
    op: NormalizedComparisonOp,
    right: &Operand,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(NormalizedPredicateExpr::Compare {
        left: normalize_operand(source, left)?,
        op,
        right: normalize_operand(source, right)?,
    })
}

fn normalize_operand(source: &SourceId, operand: &Operand) -> Result<NormalizedValueRef, Error> {
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
        Operand::Literal(value) => NormalizedValueRef::Literal(
            postcard::to_allocvec(value)
                .map_err(|err| Error::QueryLowering(format!("literal encoding failed: {err}")))?,
        ),
    })
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
                    format!("user_{}", aggregate.alias),
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
        AggregateFunction::Min => NormalizedAggregateFunction::Min,
        AggregateFunction::Max => NormalizedAggregateFunction::Max,
    }
}

fn normalized_aggregate_output_type(aggregate: &Aggregate) -> ColumnType {
    match aggregate.function {
        AggregateFunction::Count => ColumnType::U64,
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
    let root_schema = node.table_in_schema(root_table, schema_version)?;
    if includes.is_empty() {
        for (reference_index, (column, target_table)) in root_schema.references.iter().enumerate() {
            let target =
                include_auxiliary_source_id(target_table.clone(), usize::MAX, reference_index);
            sources.insert(target.clone());
            paths.push(ClosurePath {
                id: format!("reference:{column}"),
                kind: ClosurePathKind::ImplicitRootReference,
                segments: vec![ClosurePathSegment {
                    parent: root_source.clone(),
                    target,
                    source_field: column.clone(),
                }],
                root_gate: None,
            });
        }
    }
    for (include_index, include) in includes.iter().enumerate() {
        let mut current_table_name = root_table.to_owned();
        let mut parent = root_source.clone();
        let mut segments = Vec::new();
        for (segment_index, segment) in include.path.split('.').enumerate() {
            let current_table = node.table_in_schema(&current_table_name, schema_version)?;
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
        paths.push(ClosurePath {
            id: format!("include:{include_index}:{}", include.path),
            kind: ClosurePathKind::ExplicitInclude,
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
                predicate: normalize_predicates(&child_source, &subquery.filters)
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
    root_source: &SourceId,
    reachable: &crate::query::ReachableVia,
    index: usize,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, ReachableContribution), Error> {
    let frontier = FrontierId(format!("reachable:{index}:frontier"));
    let (seed_node, columns) =
        normalize_reachable_seed(nodes, reachable, index, binding_source_shape, param_types)?;

    let frontier_node = RowSetNodeId(format!("reachable:{index}:frontier"));
    nodes.insert(
        frontier_node.clone(),
        RowSetExpr::FrontierSource {
            frontier: frontier.clone(),
            columns: columns.clone(),
        },
    );

    let edge_source = reachable_edge_source_id(reachable, index);
    let edge_source_node = RowSetNodeId(format!("reachable:{index}:edge_source"));
    nodes.insert(
        edge_source_node.clone(),
        RowSetExpr::Source {
            source: edge_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut edge_current = edge_source_node;
    if !reachable.edge_filters.is_empty() {
        let edge_filter_node = RowSetNodeId(format!("reachable:{index}:edge_filter"));
        nodes.insert(
            edge_filter_node.clone(),
            RowSetExpr::Filter {
                input: edge_current,
                predicate: normalize_predicates(&edge_source, &reachable.edge_filters)?,
            },
        );
        edge_current = edge_filter_node;
    }

    let step_join_node = RowSetNodeId(format!("reachable:{index}:step_join"));
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
    let step_project_node = RowSetNodeId(format!("reachable:{index}:step_project"));
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

    let closure_node = RowSetNodeId(format!("reachable:{index}:closure"));
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

    let access_source = reachable_access_source_id(reachable, index);
    let access_source_node = RowSetNodeId(format!("reachable:{index}:access_source"));
    nodes.insert(
        access_source_node.clone(),
        RowSetExpr::Source {
            source: access_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut access_current = access_source_node;
    if !reachable.access_filters.is_empty() {
        let access_filter_node = RowSetNodeId(format!("reachable:{index}:access_filter"));
        nodes.insert(
            access_filter_node.clone(),
            RowSetExpr::Filter {
                input: access_current,
                predicate: normalize_predicates(&access_source, &reachable.access_filters)?,
            },
        );
        access_current = access_filter_node;
    }

    let access_join_node = RowSetNodeId(format!("reachable:{index}:access_join"));
    nodes.insert(
        access_join_node.clone(),
        RowSetExpr::Join {
            left: access_current,
            right: closure_node,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::SourceField {
                    source: access_source.clone(),
                    field: reachable.access_team_column.clone(),
                },
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: "reachable_team".to_owned(),
                },
            },
        },
    );

    let root_join_node = RowSetNodeId(format!("reachable:{index}:root_join"));
    nodes.insert(
        root_join_node.clone(),
        RowSetExpr::Join {
            left: current,
            right: access_join_node.clone(),
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::SourceField {
                    source: access_source.clone(),
                    field: reachable.access_row_column.clone(),
                },
            },
        },
    );
    Ok((
        root_join_node,
        ReachableContribution {
            id: format!("reachable:{index}"),
            access_source,
            access_input: access_join_node,
            root_ref_field: reachable.access_row_column.clone(),
        },
    ))
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
    reachable: &crate::query::ReachableVia,
    index: usize,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, Vec<ValueSourceColumn>), Error> {
    if let Some(seed) = &reachable.seed {
        if !predicate_params(&seed.filters).is_empty() {
            return Err(normalization_gap(
                "reachable_via relation seed filters with retained params need binding-param filter lowering",
            ));
        }
        let seed_source = reachable_seed_source_id(seed, index);
        let columns = reachable_seed_frontier_columns(&seed_source, seed);
        let seed_source_node = RowSetNodeId(format!("reachable:{index}:seed_source"));
        nodes.insert(
            seed_source_node.clone(),
            RowSetExpr::Source {
                source: seed_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let mut seed_current = seed_source_node;
        if !seed.filters.is_empty() {
            let seed_filter_node = RowSetNodeId(format!("reachable:{index}:seed_filter"));
            nodes.insert(
                seed_filter_node.clone(),
                RowSetExpr::Filter {
                    input: seed_current,
                    predicate: normalize_predicates(&seed_source, &seed.filters)?,
                },
            );
            seed_current = seed_filter_node;
        }
        let seed_project_node = RowSetNodeId(format!("reachable:{index}:seed_project"));
        nodes.insert(
            seed_project_node.clone(),
            RowSetExpr::Project {
                input: seed_current,
                columns: vec![
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
                            source: seed_source,
                            field: seed.team_column.clone(),
                        },
                    },
                ],
            },
        );
        return Ok((seed_project_node, columns));
    }

    let columns = reachable_frontier_columns(&reachable.from, param_types)?;
    let seed_node = RowSetNodeId(format!("reachable:{index}:seed"));
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
    source: &SourceId,
    seed: &crate::query::ReachableSeed,
) -> Vec<ValueSourceColumn> {
    let value = NormalizedValueRef::SourceField {
        source: source.clone(),
        field: seed.team_column.clone(),
    };
    vec![
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
    ]
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
    if let Operand::Param(param) = seed
        && !param.starts_with(CLAIM_PARAM_PREFIX)
    {
        columns.push(ValueSourceColumn {
            name: route_param_field(param),
            value: NormalizedValueRef::Param(param.clone()),
            ty: param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        });
    }
    if let Operand::Param(param) = seed
        && param != "team"
        && param != "reachable_team"
        && !param.starts_with(CLAIM_PARAM_PREFIX)
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
        Operand::Claim(claim) => Ok(NormalizedValueRef::Param(claim_param_name(claim))),
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

fn route_param_field(param: &str) -> String {
    format!("{ROUTE_PARAM_PREFIX}{param}")
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

fn reachable_edge_source_id(reachable: &crate::query::ReachableVia, index: usize) -> SourceId {
    SourceId {
        table: reachable.edge_table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveStep(format!("{index}:{}", reachable.edge_table)),
            ],
        },
    }
}

fn reachable_access_source_id(reachable: &crate::query::ReachableVia, index: usize) -> SourceId {
    SourceId {
        table: reachable.access_table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!(
                "reachable:{index}:{}",
                reachable.access_table
            ))],
        },
    }
}

fn reachable_seed_source_id(seed: &crate::query::ReachableSeed, index: usize) -> SourceId {
    SourceId {
        table: seed.table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveSeed(format!("{index}:{}", seed.table)),
            ],
        },
    }
}

fn unsupported_policy_branch_reason(query: &JazzQuery) -> Option<String> {
    if query.policy_branches.is_empty() {
        return None;
    }

    let mut reasons = Vec::new();
    if !query.joins.is_empty() {
        reasons.push("base joins");
    }
    if !query.reachable.is_empty() {
        reasons.push("base reachable");
    }
    if query
        .policy_branches
        .iter()
        .any(|branch| !branch.joins.is_empty())
    {
        reasons.push("branch joins");
    }
    if query
        .policy_branches
        .iter()
        .any(|branch| !branch.reachable.is_empty())
    {
        reasons.push("branch reachable");
    }

    (!reasons.is_empty()).then(|| {
        format!(
            "policy_branches with {} are not lowered yet",
            reasons.join(", ")
        )
    })
}

fn unsupported_join_via_reason(join: &JoinVia) -> Option<String> {
    let mut reasons = Vec::new();
    if join.source_column.is_some() {
        reasons.push("source_column");
    }
    if join.source_lookup.is_some() {
        reasons.push("source_lookup");
    }
    if join.target != JoinTarget::Column {
        reasons.push("row_id_target");
    }
    if !join.correlated_filters.is_empty() {
        reasons.push("correlated_filters");
    }
    if !join.nested_joins.is_empty() {
        reasons.push("nested_joins");
    }
    (!reasons.is_empty()).then(|| format!("unsupported join_via features: {}", reasons.join(", ")))
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
        let shape = match &ast.body {
            ShapeBody::Query(query) => query.validate(&schema.schema)?,
            ShapeBody::Relation(_) => {
                return Err(Error::InvalidStoredValue(
                    "relation query registration requires unified lowering",
                ));
            }
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
            .insert(
                subscribe.subscription.binding_id,
                RegisteredBinding {
                    values: subscribe.values,
                    read_view: subscribe.subscription.read_view,
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
            self.query.settled_result_sets.remove(&binding_view_key);
            self.query.settled_program_facts.remove(&binding_view_key);
        }
    }

    fn registered_binding_resolves_to_binding_view_key(
        &self,
        binding_view_key: BindingViewKey,
    ) -> bool {
        let Some(shape) = self.query.registered_shapes.get(&binding_view_key.shape_id) else {
            return false;
        };
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
            let value_map = shape
                .params()
                .keys()
                .cloned()
                .zip(registered.values.iter().cloned())
                .collect::<BTreeMap<_, _>>();
            shape
                .bind(value_map)
                .is_ok_and(|binding| binding.binding_id() == binding_view_key.binding_id)
        })
    }

    pub(crate) fn has_settled_result_set(&self, binding_view_key: BindingViewKey) -> bool {
        self.query
            .settled_result_sets
            .contains_key(&binding_view_key)
    }

    pub(crate) fn binding_view_key_for_subscription(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<BindingViewKey, Error> {
        if let Some(binding_view_key) = self.canonical_whole_table_binding_view_key(subscription)? {
            return Ok(binding_view_key);
        }
        let Some(shape) = self.query.registered_shapes.get(&subscription.shape_id) else {
            return Err(Error::InvalidStoredValue(
                "subscription referenced unregistered shape",
            ));
        };
        let Some(registered) = self
            .query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&subscription.binding_id))
        else {
            return Err(Error::InvalidStoredValue(
                "subscription referenced unregistered binding",
            ));
        };
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(registered.values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let binding = shape.bind(value_map)?;
        Ok(BindingViewKey {
            shape_id: subscription.shape_id,
            binding_id: binding.binding_id(),
            read_view: registered.read_view,
        })
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
        let request = self.current_query_program_request(shape, binding, tier, identity, output)?;
        self.compile_query_program_request(request)
    }

    fn compile_historical_query_program(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        position: GlobalSeq,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgram, Error> {
        let input = RowSetProgramInput {
            shape: self.normalized_row_set_shape(shape, binding)?,
            binding: ProgramBinding {
                id: binding.binding_id(),
                values: binding.values().clone(),
            },
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
        let input = RowSetProgramInput {
            shape: self.normalized_include_deleted_row_set_shape(shape, binding)?,
            binding: ProgramBinding {
                id: binding.binding_id(),
                values: binding.values().clone(),
            },
        };
        let request = QueryProgramRequest {
            reads: current_query_read_set(&input.shape, shape.schema_version(), tier),
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
        let input = RowSetProgramInput {
            shape: self.normalized_row_set_shape(&lowered_shape, &binding)?,
            binding: ProgramBinding {
                id: binding.binding_id(),
                values: binding.values().clone(),
            },
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
        let input = RowSetProgramInput {
            shape: self.normalized_row_set_shape(&lowered_shape, &binding)?,
            binding: ProgramBinding {
                id: binding.binding_id(),
                values: binding.values().clone(),
            },
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
        let table = self.table(&shape.query().table)?.clone();
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
        self.materialize_inline_current_query_rows(&table, deltas)
    }

    fn compile_query_program_request(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<QueryProgram, Error> {
        let read_view = request.reads.primary.clone();
        let policy = request.policy.clone();
        let mut resolver = CurrentQuerySourceResolver {
            node: self,
            read_view: &read_view,
            policy: &policy,
        };
        lower_query_program(request, &mut resolver)
            .map_err(|report| Error::QueryLowering(format!("{report:?}")))
    }

    fn current_query_program_request(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
        output: CurrentQueryProgramOutput,
    ) -> Result<QueryProgramRequest, Error> {
        let input = RowSetProgramInput {
            shape: self.normalized_row_set_shape(shape, binding)?,
            binding: ProgramBinding {
                id: binding.binding_id(),
                values: binding.values().clone(),
            },
        };
        Ok(QueryProgramRequest {
            reads: current_query_read_set(&input.shape, shape.schema_version(), tier),
            policy: self.query_program_policy_context(identity),
            input,
            output: current_query_output_request(output, shape.query()),
        })
    }

    fn normalized_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
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

        let unsupported_policy_branch = unsupported_policy_branch_reason(query);
        if unsupported_policy_branch.is_none() && !query.policy_branches.is_empty() {
            let filter_node = RowSetNodeId("policy_branch_filter".to_owned());
            let mut alternatives = vec![normalize_predicates(&root_source, &query.filters)?];
            alternatives.extend(
                query
                    .policy_branches
                    .iter()
                    .map(|branch| normalize_predicates(&root_source, &branch.filters))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            nodes.insert(
                filter_node.clone(),
                RowSetExpr::Filter {
                    input: current,
                    predicate: NormalizedPredicateExpr::Or(alternatives),
                },
            );
            current = filter_node;
        } else if !query.filters.is_empty() {
            let filter_node = RowSetNodeId("filter".to_owned());
            nodes.insert(
                filter_node.clone(),
                RowSetExpr::Filter {
                    input: current,
                    predicate: normalize_predicates(&root_source, &query.filters)?,
                },
            );
            current = filter_node;
        }

        for (index, join) in query.joins.iter().enumerate() {
            if let Some(reason) = unsupported_join_via_reason(join) {
                let marker = format!("join_via:{index}: {reason}");
                let node = RowSetNodeId(marker.clone());
                nodes.insert(
                    node.clone(),
                    RowSetExpr::Distinct {
                        input: current,
                        keys: vec![NormalizedValueRef::Literal(marker.into_bytes())],
                    },
                );
                current = node;
                continue;
            }

            let join_source = join_source_id(join, index);
            auxiliary_sources.insert(join_source.clone());
            let source_node = RowSetNodeId(format!("join_via:{index}:source"));
            nodes.insert(
                source_node.clone(),
                RowSetExpr::Source {
                    source: join_source.clone(),
                    visibility: RowVisibility::Visible,
                },
            );
            let mut right = source_node;
            if !join.filters.is_empty() {
                let filter_node = RowSetNodeId(format!("join_via:{index}:filter"));
                nodes.insert(
                    filter_node.clone(),
                    RowSetExpr::Filter {
                        input: right,
                        predicate: normalize_predicates(&join_source, &join.filters)?,
                    },
                );
                right = filter_node;
            }
            join_contributions.push(JoinContribution {
                id: format!("join_via:{index}"),
                source: join_source.clone(),
                input: right.clone(),
                root_ref_field: join.on_column.clone(),
            });
            let join_node = RowSetNodeId(format!("join_via:{index}:join"));
            nodes.insert(
                join_node.clone(),
                RowSetExpr::Join {
                    left: current,
                    right,
                    mode: NormalizedJoinMode::Inner,
                    on: NormalizedPredicateExpr::Compare {
                        left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                        op: NormalizedComparisonOp::Eq,
                        right: NormalizedValueRef::SourceField {
                            source: join_source,
                            field: join.on_column.clone(),
                        },
                    },
                },
            );
            current = join_node;
        }

        let binding_source_shape = self.query_binding_source_shape_for_binding(shape, binding);
        for (index, reachable) in query.reachable.iter().enumerate() {
            let (next, contribution) = normalize_reachable(
                &mut nodes,
                current,
                &root_source,
                reachable,
                index,
                &binding_source_shape,
                shape.params(),
            )?;
            current = next;
            reachable_contributions.push(contribution);
        }

        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            current =
                normalize_array_subquery(&mut nodes, current, &root_source, subquery, &[index])?;
        }

        if !query.order_by.is_empty() {
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
        if query.limit.is_some() || query.offset != 0 {
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

        Ok(NormalizedRowSetShape {
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
        })
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
            PolicyContext::Identity {
                mode: PolicyEnforcementMode::Enforcing,
                permission_subject: identity,
                claims: self
                    .session_claims
                    .get(&identity)
                    .cloned()
                    .unwrap_or_default(),
                attribution: None,
            }
        }
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

    pub(crate) fn query_rows_local_preview(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        prepared_plan: Option<&PreparedQueryPlan>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let _program = self.compile_current_query_program(
            shape,
            binding,
            DurabilityTier::Local,
            AuthorId::SYSTEM,
            CurrentQueryProgramOutput::AppRows,
        )?;
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
        if include_deleted {
            let mut rows = self.query_rows_including_deleted_with_lowered_clauses(
                shape, binding, tier, identity,
            )?;
            let query = shape.query();
            self.finish_query_rows(query, &mut rows)?;
            return Ok(rows);
        }
        let program = if prepared_plan.is_some() {
            None
        } else {
            Some(self.compile_current_query_program(
                shape,
                binding,
                tier,
                identity,
                CurrentQueryProgramOutput::AppRows,
            )?)
        };
        let plan = match prepared_plan {
            Some(plan) => Some(plan.clone()),
            None if {
                let parameters = &program
                    .as_ref()
                    .expect("program is compiled when no prepared plan is supplied")
                    .lowered
                    .parameters;
                !parameters.user_params.is_empty() || !parameters.hidden_params.is_empty()
            } =>
            {
                Some(self.prepared_query_plan(shape, binding, tier, identity)?)
            }
            None => None,
        };
        let table_schema = self.table(&shape.query().table)?.clone();
        let deltas_result = match plan {
            None => self
                .database
                .query_graph(lowered_app_rows_graph(
                    &program.expect("program is compiled when no prepared plan is supplied"),
                )?)
                .map_err(Error::Groove),
            Some(PreparedQueryPlan::Prepared { shape, params }) => {
                let values = binding_values_for_plan(
                    binding,
                    &params,
                    identity,
                    self.session_claims.get(&identity),
                )?;
                self.database
                    .bind_shape(shape, &values)
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
            Some(PreparedQueryPlan::Graph(graph)) => {
                self.database.query_graph(graph).map_err(Error::Groove)
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
        let query = shape.query();
        self.finish_engine_query_rows(query, &mut rows)?;
        self.apply_projection(query, &mut rows)?;
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
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program = self.compile_historical_query_program(
            &lowered_shape,
            &binding,
            position,
            AuthorId::SYSTEM,
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

    fn query_rows_including_deleted_with_lowered_clauses(
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
        let table = self
            .table_in_schema(&query.table, lowered_shape.schema_version())?
            .clone();
        let binding = lowered_shape.bind(BTreeMap::new())?;
        let program =
            self.compile_include_deleted_query_program(&lowered_shape, &binding, tier, identity)?;
        let deltas = self
            .database
            .query_graph(lowered_app_rows_graph(&program)?)
            .map_err(Error::Groove)?;
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

    pub(crate) fn open_local_maintained_view_subscription(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
    ) -> Result<(LocalMaintainedViewSubscription, Vec<CurrentRow>), Error> {
        let (subscription, maintained, terminal_schemas, transitions, tables) =
            self.open_seeded_maintained_subscription_view(shape, binding, identity, tier)?;
        let mut local = LocalMaintainedViewSubscription {
            subscription,
            maintained,
            terminal_schemas,
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
        let mut states = BTreeMap::<ResultMemberEntry, (bool, bool)>::new();
        loop {
            match local.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = apply_maintained_multisink_deltas(
                        &mut local.maintained,
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
        for member in transitions.adds {
            if member.table_name() != Some(local.result_table.as_str()) {
                continue;
            }
            if local.result_set.insert(member.clone())
                && let Some(row) =
                    self.materialize_local_maintained_view_result_member(local, &member)?
            {
                adds.push(row);
            }
        }
        let mut removes = Vec::new();
        for member in transitions.removes {
            if member.table_name() != Some(local.result_table.as_str()) {
                continue;
            }
            if local.result_set.remove(&member) {
                removes.push(member);
            }
        }
        Ok(LocalMaintainedViewSubscriptionUpdate { adds, removes })
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
        let plan = self.prepared_query_plan(&shape, &binding, tier, AuthorId::SYSTEM)?;
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
        let (shape, binding) = self.policy_composed_shape_binding(shape, binding, identity)?;
        let program = self.compile_current_query_program(
            &shape,
            &binding,
            tier,
            identity,
            CurrentQueryProgramOutput::RelationSnapshot,
        )?;
        let snapshots = self
            .database
            .query_graphs(lowered_program_sinks(&program))
            .map_err(Error::Groove)?;
        self.materialize_relation_snapshot_from_query_engine(&shape, &snapshots)
    }

    fn materialize_relation_snapshot_from_query_engine(
        &mut self,
        shape: &ValidatedQuery,
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
        let descriptor = &edges.descriptor;
        let source_table_idx = required_field_idx(descriptor, "source_table")?;
        let source_row_idx = required_field_idx(descriptor, "source_row")?;
        let relation_idx = required_field_idx(descriptor, "path")?;
        let target_table_idx = required_field_idx(descriptor, "target_table")?;
        let target_row_idx = required_field_idx(descriptor, "target_row")?;
        let target_tx_time_idx = required_field_idx(descriptor, "target_tx_time")?;
        let target_tx_node_idx = required_field_idx(descriptor, "target_tx_node_id")?;
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
            snapshot.edges.push(RelationEdge {
                source_table,
                source_row,
                relation,
                target_table: target_table_name.clone(),
                target_row,
            });
            if row_keys.insert((target_table_name.clone(), target_row)) {
                let target_table = self
                    .table_in_schema(&target_table_name, shape.schema_version())?
                    .clone();
                let version = self
                    .query_version_by_alias_with_descriptor(
                        &target_table_name,
                        target_row,
                        VersionLayer::Content,
                        target_tx_time,
                        target_tx_node,
                        &target_table.history_storage_table().record_schema(),
                    )?
                    .ok_or(Error::InvalidStoredValue(
                        "relation edge target version is missing",
                    ))?;
                let row = self.current_row_from_materialized_version(&target_table, &version)?;
                snapshot.rows.push(row);
            }
        }
        Ok(snapshot)
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

    fn finish_engine_query_rows(
        &self,
        query: &crate::query::Query,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        // Groove lowering owns membership/windowing, but one-shot APIs still
        // return a deterministic Vec. Re-apply ordering to the selected rows
        // without re-applying pagination.
        self.apply_query_order(query, rows)
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

    fn projected_historical_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        // TODO(query-engine): replace this inline projected source once schema
        // lenses/projections are first-class source graph nodes.
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
        let program = self.compile_open_tx_query_program(
            tx_id,
            shape,
            binding,
            AuthorId::SYSTEM,
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
        self.finish_query_rows(query, &mut rows)?;
        Ok(rows)
    }

    pub(crate) fn prepared_query_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
        identity: AuthorId,
    ) -> Result<PreparedQueryPlan, Error> {
        let key = (
            shape.shape_id(),
            tier,
            query_binding_value_signature(binding),
        );
        if let Some(plan) = self.query.query_shape_cache.get(&key) {
            return Ok(plan.clone());
        }
        let program = self.compile_current_query_program(
            shape,
            binding,
            tier,
            identity,
            CurrentQueryProgramOutput::AppRows,
        )?;
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
        let plan = if params.is_empty() {
            PreparedQueryPlan::Graph(graph)
        } else {
            let binding_source_shape = self.query_binding_source_shape_for_binding(shape, binding);
            let prepared = self.database.prepare(
                [groove::ivm::RoutedMultisinkTerminal::new(
                    JAZZ_APP_ROWS_SINK,
                    graph,
                    route_params.iter().cloned(),
                    app_row_fields,
                )],
                binding_source_shape,
                binding_descriptor,
            )?;
            PreparedQueryPlan::Prepared {
                shape: prepared.id(),
                params,
            }
        };
        self.query.query_shape_cache.insert(key, plan.clone());
        Ok(plan)
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
            let reachable_seed_param_available =
                reachable_seed_param_available(reachable, &reachable_seed_param);
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
                param_types
                    .keys()
                    .filter_map(|param| {
                        if carried_params.contains(param) {
                            Some(ProjectField::renamed(
                                format!("left.{param}"),
                                param.clone(),
                            ))
                        } else if *param == reachable_seed_param && reachable_seed_param_available {
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
            if options.keep_binding_params_in_output && reachable_seed_param_available {
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
            DurabilityTier::Global,
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
                normalized_global_current_graph(&table),
                &policy_shape,
                &table,
                current_row_fields(&table),
                DurabilityTier::Global,
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
        let content_current_keys = GraphBuilder::table(global_current_table_name(&table.name))
            .project(["row_uuid", "tx_time", "tx_node_id"]);
        let content = GraphBuilder::join(
            GraphBuilder::table(history_table_name(&table.name)),
            content_current_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_history_storage_fields(&table, "left."));
        let content = self.apply_maintained_view_filters(
            content,
            &policy_shape,
            &table,
            maintained_view_history_storage_field_names(&table),
            DurabilityTier::Global,
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
                normalized_global_current_graph(&table),
                &policy_shape,
                &table,
                current_row_fields(&table),
                DurabilityTier::Global,
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
            DurabilityTier::Global,
        )?;
        self.materialize_maintained_view_graph(graph, &shape)
    }

    pub(crate) fn open_seeded_maintained_subscription_view(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
        tier: DurabilityTier,
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
        let (composed_shape, composed_binding) =
            self.policy_composed_shape_binding(shape, binding, identity)?;
        let shape = maintained_view_bind_filter_literals_with_mode(
            &composed_shape,
            &composed_binding,
            &self.catalogue.schema,
            ParamBindingMode::RetainAllParams,
        )?;
        self.ensure_maintained_view_query_slice(shape.query())?;
        let program = self.compile_current_query_program(
            &shape,
            &composed_binding,
            tier,
            identity,
            CurrentQueryProgramOutput::MaintainedView,
        )?;
        let tables = program.lowered.maintained_terminal_tables.clone();
        let terminal_schemas = MaintainedSubscriptionView::terminal_schemas_for_program(&program);
        self.database.flush().map_err(Error::Groove)?;
        let subscription = self.subscribe_lowered_program(
            &program,
            &composed_binding,
            identity,
            self.query_binding_source_shape_for_binding(&shape, &composed_binding),
        )?;
        let mut maintained = MaintainedSubscriptionView::default();
        let mut transitions = super::maintained_subscription_view::ResultTransitions::default();
        let snapshot = subscription.recv().map_err(|_| {
            Error::InvalidStoredValue("seeded maintained subscription disconnected")
        })?;
        let snapshot_transitions = apply_maintained_multisink_deltas(
            &mut maintained,
            snapshot,
            &terminal_schemas,
            &tables,
            &self.node_aliases,
        )?;
        transitions.adds.extend(snapshot_transitions.adds);
        transitions.removes.extend(snapshot_transitions.removes);
        loop {
            match subscription.try_recv() {
                Ok(deltas) => {
                    let delta_transitions = apply_maintained_multisink_deltas(
                        &mut maintained,
                        deltas,
                        &terminal_schemas,
                        &tables,
                        &self.node_aliases,
                    )?;
                    transitions.adds.extend(delta_transitions.adds);
                    transitions.removes.extend(delta_transitions.removes);
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
        program: &QueryProgram,
        binding: &Binding,
        identity: AuthorId,
        binding_source_shape: String,
    ) -> Result<MultisinkSubscription, Error> {
        let params = prepared_params_from_domain(&program.lowered.parameters);
        let route_params = prepared_route_param_names(&program.lowered.parameters);
        if params.is_empty() {
            let sinks = lowered_program_sinks(program);
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
        let terminals = program
            .lowered
            .terminals
            .iter()
            .map(|terminal| {
                let public_fields = terminal_public_fields(&terminal.output)?;
                let route_fields = terminal_route_fields(&route_params, &public_fields);
                Ok(RoutedMultisinkTerminal::new(
                    terminal.sink.clone(),
                    terminal.graph.clone(),
                    route_fields,
                    public_fields,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let prepared =
            self.database
                .prepare(terminals, binding_source_shape, binding_descriptor)?;
        let values = binding_values_for_plan(
            binding,
            &params,
            identity,
            self.session_claims.get(&identity),
        )?;
        self.database
            .bind_shape(prepared.id(), &values)
            .map_err(Error::Groove)
    }

    // TODO(query-engine): this old maintained graph builder is retained only as
    // a test oracle while query-engine witness/coverage parity tests are moved
    // to public maintained subscription behavior.
    #[cfg(test)]
    fn maintained_view_tagged_terminal_graph_for_shape(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        policy_param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
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
            tier,
        )?;
        let mut graphs = vec![result_current];
        graphs.extend(
            self.maintained_view_tagged_fact_sinks_for_shape(
                shape,
                identity,
                policy_param_binding_mode,
                tier,
            )?
            .into_iter()
            .map(|(_, graph)| graph),
        );
        Ok(GraphBuilder::union(graphs))
    }

    #[cfg(test)]
    fn maintained_view_tagged_fact_sinks_for_shape(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        policy_param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
    ) -> Result<Vec<(String, GraphBuilder)>, Error> {
        let terminal_tables = self.maintained_view_terminal_tables(shape)?;
        let hidden_param_types = self.maintained_view_hidden_param_types_for_shape(
            shape,
            identity,
            policy_param_binding_mode,
        )?;
        let mut sinks = Vec::new();
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
                    tier,
                )?;
            let (replacement_content, replacement_deletion) = self
                .maintained_view_replacement_tagged_graphs(
                    table,
                    &policy_shape,
                    terminal_tables.values(),
                    policy_param_binding_mode,
                    &hidden_param_types,
                    tier,
                )?;
            sinks.extend([
                (
                    maintained_fact_sink("version_content", &table.name),
                    version_content,
                ),
                (
                    maintained_fact_sink("version_deletion", &table.name),
                    version_deletion,
                ),
                (
                    maintained_fact_sink("replacement_content", &table.name),
                    replacement_content,
                ),
                (
                    maintained_fact_sink("replacement_deletion", &table.name),
                    replacement_deletion,
                ),
            ]);
        }
        Ok(sinks)
    }

    // TODO(query-engine): retained only for the old #[cfg(test)] maintained
    // graph oracle. Production maintained metadata now comes from
    // LoweredGraph::maintained_terminal_tables after query-engine source
    // resolution.
    #[cfg(test)]
    pub(crate) fn maintained_view_terminal_tables(
        &self,
        shape: &ValidatedQuery,
    ) -> Result<BTreeMap<String, TableSchema>, Error> {
        let mut tables = BTreeMap::new();
        self.collect_maintained_view_terminal_tables_for_query(shape.query(), &mut tables)?;
        Ok(tables)
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    fn maintained_view_policy_readable_version_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        tier: DurabilityTier,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        let filter_param_types = graph_param_types(policy_shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&filter_param_types, param_binding_mode);
        let content = self.apply_maintained_view_filters(
            GraphBuilder::table(history_table_name(&table.name)),
            policy_shape,
            table,
            maintained_view_version_fields(table),
            tier,
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
                include_deleted_current_graph(table, tier),
                policy_shape,
                table,
                current_row_fields(table),
                tier,
            )?
            .project(
                std::iter::once("row_uuid".to_owned())
                    .chain(available_hidden_param_types.keys().cloned())
                    .collect::<Vec<_>>(),
            );
        let deletion_current_keys = maintained_view_register_current_keys(table, tier);
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
                "version_deletion",
                "left.",
                terminal_tables,
                output_hidden_param_types,
                available_hidden_param_types,
                "right.",
            ));

        Ok((content, deletion))
    }

    #[cfg(test)]
    fn maintained_view_result_closure_graph(
        &self,
        shape: &ValidatedQuery,
        identity: AuthorId,
        terminal_tables: &BTreeMap<String, TableSchema>,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let root_table = self.table(&shape.query().table)?.clone();
        let root_current = self.maintained_view_bound_query_current_graph(shape, tier)?;
        let result_current = self.maintained_view_filter_result_current_by_include_modes(
            root_current.clone(),
            &root_table,
            shape,
            identity,
            param_binding_mode,
            tier,
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
        if maintained_view_has_binding_dependent_reachable(shape) {
            graphs.push(result_current.clone().project_fields(
                maintained_view_tagged_content_fields(
                    &root_table,
                    "version_content",
                    "",
                    terminal_tables.values(),
                    output_hidden_param_types,
                    &result_current_param_types,
                    "",
                ),
            ));
        }

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
                tier,
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
                tier,
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
                tier,
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
                    tier,
                )?);
            }
        }

        Ok(GraphBuilder::union(graphs))
    }

    #[cfg(test)]
    fn maintained_view_filter_result_current_by_include_modes(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        shape: &ValidatedQuery,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
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
            tier,
        )
    }

    #[cfg(test)]
    fn filter_root_current_by_required_include_modes(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        includes: &[Include],
        identity: AuthorId,
        root_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
        preserved_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        tier: DurabilityTier,
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
                tier,
            )?;
        }
        Ok(graph)
    }

    #[cfg(test)]
    fn filter_root_current_by_required_include_path(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        include: &Include,
        identity: AuthorId,
        root_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
        preserved_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        tier: DurabilityTier,
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
                tier,
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

    #[cfg(test)]
    fn maintained_view_bound_query_current_graph(
        &self,
        shape: &ValidatedQuery,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let table = self.table(&shape.query().table)?;
        let graph = self.maintained_view_content_current_with_version(table, tier)?;
        self.apply_maintained_view_filters(
            graph,
            shape,
            table,
            maintained_view_version_fields(table),
            tier,
        )
    }

    #[cfg(test)]
    fn maintained_view_policy_readable_current_graph(
        &self,
        table: &TableSchema,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let policy_shape =
            self.maintained_view_table_policy_shape_with_mode(table, identity, param_binding_mode)?;
        let graph = self.maintained_view_content_current_with_version(table, tier)?;
        self.apply_maintained_view_filters(
            graph,
            &policy_shape,
            table,
            maintained_view_version_fields(table),
            tier,
        )
    }

    #[cfg(test)]
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

    #[cfg(test)]
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
        tier: DurabilityTier,
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
            tier,
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

    #[cfg(test)]
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
        tier: DurabilityTier,
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
                tier,
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

    #[cfg(test)]
    fn maintained_view_join_closure_current_graph(
        &self,
        root: GraphBuilder,
        root_table: &TableSchema,
        join: &JoinVia,
        identity: AuthorId,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
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
            self.maintained_view_content_current_with_version(&join_table, tier)?,
            &join_table,
            &join_shape,
            identity,
            maintained_view_version_fields(&join_table),
            param_binding_mode,
            tier,
        )?;
        let root = if let Some(lookup) = &join.source_lookup {
            let lookup_table = self.table(&lookup.table)?.clone();
            GraphBuilder::join(
                root.unwrap_nullable(query_field(&lookup.row_id_source_column)),
                self.maintained_view_content_current_with_version(&lookup_table, tier)?,
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

    #[cfg(test)]
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

    #[cfg(test)]
    fn apply_maintained_view_policy_to_current_graph(
        &self,
        graph: GraphBuilder,
        table: &TableSchema,
        shape: &ValidatedQuery,
        identity: AuthorId,
        output_fields: Vec<String>,
        param_binding_mode: ParamBindingMode,
        tier: DurabilityTier,
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
        self.apply_maintained_view_filters(graph, &policy_shape, table, output_fields, tier)
    }

    #[cfg(test)]
    fn maintained_view_replacement_tagged_graphs<'a>(
        &self,
        table: &TableSchema,
        policy_shape: &ValidatedQuery,
        terminal_tables: impl IntoIterator<Item = &'a TableSchema> + Clone,
        param_binding_mode: ParamBindingMode,
        output_hidden_param_types: &BTreeMap<String, groove::schema::ColumnType>,
        tier: DurabilityTier,
    ) -> Result<(GraphBuilder, GraphBuilder), Error> {
        let filter_param_types = graph_param_types(policy_shape, &self.catalogue.schema)?;
        let available_hidden_param_types =
            hidden_maintained_view_param_types(&filter_param_types, param_binding_mode);
        let content_current_keys = maintained_view_content_current_keys(table, tier);
        let content = GraphBuilder::join(
            GraphBuilder::table(history_table_name(&table.name)),
            content_current_keys,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(maintained_view_history_storage_fields(table, "left."));
        let content = self.apply_maintained_view_filters(
            content,
            policy_shape,
            table,
            maintained_view_history_storage_field_names(table),
            tier,
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
                include_deleted_current_graph(table, tier),
                policy_shape,
                table,
                current_row_fields(table),
                tier,
            )?
            .project(
                std::iter::once("row_uuid".to_owned())
                    .chain(available_hidden_param_types.keys().cloned())
                    .collect::<Vec<_>>(),
            );
        let deletion_current_keys = maintained_view_register_current_keys(table, tier);
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
        let graph =
            self.maintained_view_content_current_with_version(table, DurabilityTier::Global)?;
        let graph = self.apply_maintained_view_filters(
            graph,
            &shape,
            table,
            maintained_view_version_fields(table),
            DurabilityTier::Global,
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
            DurabilityTier::Global,
        )?;
        let content = content.project_fields(maintained_view_policy_content_fields(table));

        let readable_current = self
            .apply_maintained_view_filters(
                normalized_global_current_graph(table),
                policy_shape,
                table,
                current_row_fields(table),
                DurabilityTier::Global,
            )?
            .project(["row_uuid"]);
        let deleted = GraphBuilder::table(register_table_name(&table.name))
            .filter(PredicateExpr::eq("_deletion", Value::Enum(0)));
        let deletion = GraphBuilder::join(deleted, readable_current, ["row_uuid"], ["row_uuid"])
            .project_fields(maintained_view_policy_deletion_fields(table));

        Ok(GraphBuilder::union([content, deletion]))
    }
    fn maintained_view_content_current_with_version(
        &self,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let history = GraphBuilder::table(history_table_name(&table.name)).project([
            "row_uuid",
            "tx_time",
            "tx_node_id",
            "schema_version",
            "parents",
        ]);
        Ok(GraphBuilder::join(
            visible_current_graph(table, tier),
            history,
            ["row_uuid", "tx_time", "tx_node_id"],
            ["row_uuid", "tx_time", "tx_node_id"],
        )
        .project_fields(
            std::iter::once(ProjectField::renamed("left.row_uuid", "row_uuid"))
                .chain(table.columns.iter().map(|column| {
                    let field = format!("user_{}", column.name);
                    ProjectField::renamed(format!("left.{field}"), field)
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
                ]),
        ))
    }

    fn apply_maintained_view_filters(
        &self,
        graph: GraphBuilder,
        shape: &ValidatedQuery,
        table: &TableSchema,
        output_fields: Vec<String>,
        tier: DurabilityTier,
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
                tier,
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
            // TODO(query-engine): remove this pre-lowering guard once maintained
            // output support is expressed entirely as typed query-engine
            // capabilities and mapped to the same public error at the peer
            // boundary.
            return Err(crate::peer::unsupported_maintained_subscription_shape_error());
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
        let graph = GraphBuilder::join(
            reachable_graphs.access_current,
            reachable_graphs.closure,
            [query_field(&reachable.access_team_column)],
            ["reachable_team".to_owned()],
        )
        .project_fields({
            let mut fields = vec![ProjectField::renamed(
                format!("left.{}", query_field(&reachable.access_row_column)),
                "access_row_uuid",
            )];
            if reachable_graphs.seed_param_available {
                fields.push(ProjectField::renamed(
                    format!("right.{seed_param}"),
                    seed_param.clone(),
                ));
            }
            fields
        });
        if param_types
            .get(&seed_param)
            .is_some_and(|column_type| matches!(column_type.value_type(), ValueType::Nullable(_)))
            && reachable_graphs.seed_param_available
        {
            Ok(graph.project_fields([
                ProjectField::named("access_row_uuid"),
                ProjectField::nullable(seed_param.clone(), seed_param),
            ]))
        } else {
            Ok(graph)
        }
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
        let seed_param = reachable_seed_param(reachable)?;
        let mut seed_params = BTreeSet::new();
        let mut seed_param_value_types = BTreeMap::new();
        let seed = if let Some(seed) = &reachable.seed {
            let seed_table = self.table(&seed.table)?;
            let mut seed_graph = current_source_graph(&seed_table, tier, source_overrides)
                .unwrap_nullable(query_field(&seed.team_column));
            seed_params = predicate_params(&seed.filters);
            for param in &seed_params {
                let column_type = param_types.get(param).ok_or(Error::InvalidStoredValue(
                    "reachable seed param missing from graph param types",
                ))?;
                seed_param_value_types.insert(param.clone(), column_type.value_type());
            }
            seed_graph = apply_filters_with_predicate_params(
                seed_graph,
                &seed_table,
                param_types,
                &seed.filters,
                current_row_fields(&seed_table),
                true,
                binding_source_shape,
            )?;
            seed_graph.project_fields(
                [
                    ProjectField::renamed(query_field(&seed.team_column), "team"),
                    ProjectField::renamed(query_field(&seed.team_column), "reachable_team"),
                ]
                .into_iter()
                .chain(seed_params.iter().cloned().map(ProjectField::named)),
            )
        } else {
            match &reachable.from {
                Operand::Param(param) => {
                    if param != "team" && param != "reachable_team" {
                        seed_params.insert(param.clone());
                    }
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
                    let column_type = param_types.get(param).ok_or(Error::InvalidStoredValue(
                        "reachable seed param missing from graph param types",
                    ))?;
                    let value_type = match column_type.value_type() {
                        ValueType::Nullable(inner) => (*inner).clone(),
                        value_type => value_type,
                    };
                    if param != "team" && param != "reachable_team" {
                        seed_param_value_types.insert(param.clone(), value_type);
                    }
                    let mut fields = vec![
                        ProjectField::renamed(param.clone(), "team"),
                        ProjectField::renamed(param.clone(), "reachable_team"),
                    ];
                    if param != "team" && param != "reachable_team" {
                        fields.push(ProjectField::named(param.clone()));
                    }
                    seed.project_fields(fields)
                }
                Operand::Literal(Value::Uuid(seed)) => GraphBuilder::values(
                    reachable_frontier_descriptor(&seed_param_value_types),
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
        let seed_param_available = seed_param == "team"
            || seed_param == "reachable_team"
            || seed_params.contains(&seed_param);
        let frontier = GraphBuilder::frontier_source(
            "reachable_frontier",
            reachable_frontier_descriptor(&seed_param_value_types),
        );
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
        .project_fields(
            [
                ProjectField::renamed("left.team", "team"),
                ProjectField::renamed(
                    format!("right.{}", query_field(&reachable.edge_parent_column)),
                    "reachable_team",
                ),
            ]
            .into_iter()
            .chain(
                seed_params
                    .iter()
                    .cloned()
                    .map(|param| ProjectField::renamed(format!("left.{param}"), param)),
            ),
        );
        let closure = GraphBuilder::recursive(
            seed,
            step,
            "reachable_frontier",
            reachable.bound.iteration_cap(),
        );
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
            seed_param_available,
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
    maintained_view_window_supported(query)
}

fn maintained_view_window_supported(query: &crate::query::Query) -> bool {
    if query.order_by.is_empty() {
        query.offset == 0 && (query.limit.is_none() || query.limit == Some(1))
    } else {
        true
    }
}

// Groove `TopBy` represents both finite ordered windows and the currently
// supported unbounded ordered suffix (`ORDER BY ... OFFSET n` with no LIMIT).
// Keep the sentinel named so this does not look like an accidental finite
// capability.
#[allow(dead_code)]
const UNBOUNDED_ORDERED_WINDOW_LIMIT: usize = usize::MAX;

#[allow(dead_code)]
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
            query.limit.unwrap_or(UNBOUNDED_ORDERED_WINDOW_LIMIT),
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

fn claim_name_from_param(param: &str) -> &str {
    param
        .strip_prefix(CLAIM_PARAM_PREFIX)
        .expect("hidden claim params must use the claim param prefix")
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

fn claim_value_for_binding(
    name: &str,
    identity: AuthorId,
    claims: Option<&BTreeMap<String, Value>>,
) -> Value {
    claims
        .and_then(|claims| claims.get(name))
        .cloned()
        .unwrap_or_else(|| match name {
            "sub" => Value::Uuid(identity.0),
            "user_id" => Value::String(identity.0.to_string()),
            "isAdmin" => Value::Bool(false),
            _ => Value::Nullable(None),
        })
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

fn reachable_seed_param_available(
    reachable: &crate::query::ReachableVia,
    seed_param: &str,
) -> bool {
    if let Some(seed) = &reachable.seed {
        return predicate_params(&seed.filters).contains(seed_param);
    }
    matches!(&reachable.from, Operand::Param(param) if param == seed_param)
}

fn reachable_frontier_descriptor(
    seed_param_value_types: &BTreeMap<String, groove::records::ValueType>,
) -> RecordDescriptor {
    let mut fields = vec![
        ("team".to_owned(), groove::records::ValueType::Uuid),
        (
            "reachable_team".to_owned(),
            groove::records::ValueType::Uuid,
        ),
    ];
    for (param, value_type) in seed_param_value_types {
        fields.push((param.clone(), value_type.clone()));
    }
    RecordDescriptor::new(fields)
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

#[allow(dead_code)]
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
    query.array_subqueries = query
        .array_subqueries
        .into_iter()
        .map(|subquery| bind_array_subquery_filter_literals(subquery, binding, mode))
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

fn bind_array_subquery_filter_literals(
    mut subquery: ArraySubquery,
    binding: &Binding,
    mode: ParamBindingMode,
) -> Result<ArraySubquery, Error> {
    subquery.filters = subquery
        .filters
        .into_iter()
        .map(|predicate| maintained_view_bind_predicate(predicate, binding, mode))
        .collect::<Result<Vec<_>, _>>()?;
    subquery.nested_arrays = subquery
        .nested_arrays
        .into_iter()
        .map(|nested| bind_array_subquery_filter_literals(nested, binding, mode))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(subquery)
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
    query.array_subqueries = query
        .array_subqueries
        .into_iter()
        .map(|subquery| {
            bind_array_subquery_filter_literals(
                subquery,
                binding,
                ParamBindingMode::InlineAllReachableSeeds,
            )
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

#[allow(dead_code)]
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

fn query_binding_value_signature(binding: &Binding) -> String {
    binding
        .values()
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(",")
}

impl<S: OrderedKvStorage> NodeState<S> {
    fn query_binding_source_shape_for_binding(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> String {
        format!(
            "jazz-query:{}:{}:{}",
            self.groove_runtime_token(),
            shape.shape_id().0,
            query_binding_value_signature(binding)
        )
    }
}

fn maintained_view_binding_source_shape(shape: &ValidatedQuery) -> String {
    format!("jazz-maintained-query:{}", shape.shape_id().0)
}

#[cfg(test)]
fn maintained_fact_sink(event_kind: &str, table: &str) -> String {
    format!("maintained.{event_kind}.{table}")
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
    params: &[PreparedQueryParam],
    identity: AuthorId,
    claims: Option<&BTreeMap<String, Value>>,
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
                Ok(coerce_prepared_binding_value(value, &param.ty))
            }
            PreparedQueryParamSource::Claim { ref name } => Ok(claim_binding_value(
                Some(&param.ty),
                claim_value_for_binding(name, identity, claims),
            )),
        })
        .collect()
}

fn coerce_prepared_binding_value(value: Value, column_type: &groove::schema::ColumnType) -> Value {
    match column_type {
        groove::schema::ColumnType::Nullable(_) if !matches!(value, Value::Nullable(_)) => {
            Value::Nullable(Some(Box::new(value)))
        }
        _ => value,
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
        left.row_uuid()
            .to_bytes()
            .cmp(&right.row_uuid().to_bytes())
            .then_with(|| left.projected_tx_alias().cmp(&right.projected_tx_alias()))
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

fn global_current_storage_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| format!("user_{}", column.name)),
    );
    fields.push("created_by".to_owned());
    fields.push("created_at".to_owned());
    fields.push("updated_by".to_owned());
    fields.push("updated_at".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    fields
}

#[cfg(test)]
fn normalized_global_current_graph(table: &TableSchema) -> GraphBuilder {
    GraphBuilder::table(global_current_table_name(&table.name))
        .project(global_current_storage_fields(table))
        .project_fields(
            std::iter::once(ProjectField::named("row_uuid"))
                .chain(
                    table
                        .columns
                        .iter()
                        .map(|column| ProjectField::named(format!("user_{}", column.name))),
                )
                .chain([
                    ProjectField::renamed("created_by", "$createdBy"),
                    ProjectField::renamed("created_at", "$createdAt"),
                    ProjectField::renamed("updated_by", "$updatedBy"),
                    ProjectField::renamed("updated_at", "$updatedAt"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                ]),
        )
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

fn historical_current_graph(table: &TableSchema, position: GlobalSeq) -> GraphBuilder {
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
        EnumSchema::new("jazz_deletion", ["deleted", "restored"]).expect("valid deletion enum"),
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
                    .map(|column| format!("user_{}", column.name)),
            )
            .map(|field| ProjectField::renamed(format!("left.{field}"), field))
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
            .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
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
                    .map(|field| ProjectField::renamed(format!("left.{field}"), field)),
            );
    GraphBuilder::union([content_is_latest, restored_content])
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

#[allow(dead_code)]
fn maintained_view_content_current_keys(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    maintained_view_current_keys(
        global_current_table_name(&table.name),
        ahead_current_table_name(&table.name),
        tier,
    )
}

#[allow(dead_code)]
fn maintained_view_register_current_keys(
    table: &TableSchema,
    tier: DurabilityTier,
) -> GraphBuilder {
    maintained_view_current_keys(
        register_global_current_table_name(&table.name),
        register_ahead_current_table_name(&table.name),
        tier,
    )
}

#[allow(dead_code)]
fn maintained_view_current_keys(
    global_table: String,
    ahead_table: String,
    tier: DurabilityTier,
) -> GraphBuilder {
    let key_fields = ["row_uuid", "tx_time", "tx_node_id"];
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(global_table).project(key_fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(ahead_table).project(key_fields),
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
        .project_fields([
            ProjectField::renamed("left.row_uuid", "row_uuid"),
            ProjectField::renamed("left.tx_time", "tx_time"),
            ProjectField::renamed("left.tx_node_id", "tx_node_id"),
        ])
    } else {
        GraphBuilder::table(ahead_table).project(key_fields)
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([GraphBuilder::table(global_table).project(key_fields), ahead]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(key_fields)
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
            "created_by".to_owned(),
            "created_at".to_owned(),
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

#[allow(dead_code)]
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
            .map(|column| format!("user_{}", column.name)),
    );
    fields
}

#[allow(dead_code)]
fn maintained_view_history_storage_fields(table: &TableSchema, prefix: &str) -> Vec<ProjectField> {
    maintained_view_history_storage_field_names(table)
        .into_iter()
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
        .collect()
}

#[allow(dead_code)]
fn maintained_view_register_storage_fields(prefix: &str) -> Vec<ProjectField> {
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

#[allow(dead_code)]
fn maintained_view_version_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = global_current_storage_fields(table);
    fields.extend(["schema_version".to_owned(), "parents".to_owned()]);
    fields
}

#[allow(dead_code)]
fn maintained_view_nullable_deletion_type() -> ValueType {
    ValueType::Nullable(Box::new(ValueType::Enum(
        EnumSchema::new("jazz_deletion", ["deleted", "restored"]).expect("valid deletion enum"),
    )))
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
        ProjectField::literal("created_by", Value::Uuid(AuthorId::SYSTEM.0)),
        ProjectField::literal("created_at", Value::U64(0)),
        ProjectField::literal("updated_by", Value::Uuid(AuthorId::SYSTEM.0)),
        ProjectField::literal("updated_at", Value::U64(0)),
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
        ProjectField::literal("created_by", Value::Uuid(AuthorId::SYSTEM.0)),
        ProjectField::literal("created_at", Value::U64(0)),
        ProjectField::literal("updated_by", Value::Uuid(AuthorId::SYSTEM.0)),
        ProjectField::literal("updated_at", Value::U64(0)),
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
        ProjectField::renamed("left.created_by", "created_by"),
        ProjectField::renamed("left.created_at", "created_at"),
        ProjectField::renamed("left.updated_by", "updated_by"),
        ProjectField::renamed("left.updated_at", "updated_at"),
        ProjectField::nullable("left._deletion", "_deletion"),
    ];
    fields.extend(table.columns.iter().map(|column| {
        ProjectField::null_typed(
            format!("user_{}", column.name),
            ValueType::Nullable(Box::new(column.column_type.value_type())),
        )
    }));
    fields
}

#[allow(dead_code)]
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
        ProjectField::renamed(source("created_by"), "created_by"),
        ProjectField::renamed(source("created_at"), "created_at"),
        ProjectField::renamed(source("updated_by"), "updated_by"),
        ProjectField::renamed(source("updated_at"), "updated_at"),
        ProjectField::null_typed("_deletion", maintained_view_nullable_deletion_type()),
    ];
    fields.extend(
        maintained_view_terminal_user_columns(terminal_tables)
            .into_iter()
            .map(|((table_name, column_name), column_type)| {
                let user_field = format!("user_{column_name}");
                let tagged_field = maintained_view_tagged_user_field(&table_name, &column_name);
                if table_name == table.name {
                    ProjectField::nullable_flat(source(&user_field), tagged_field)
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

#[allow(dead_code)]
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
        ProjectField::renamed(source("created_by"), "created_by"),
        ProjectField::renamed(source("created_at"), "created_at"),
        ProjectField::renamed(source("updated_by"), "updated_by"),
        ProjectField::renamed(source("updated_at"), "updated_at"),
        ProjectField::nullable(source("_deletion"), "_deletion"),
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
    use crate::protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SyncMessage};
    use crate::query::{
        Aggregate, OrderDirection, Query, claim, col, contains, eq, gt, in_list, lit, lte, param,
    };
    use crate::schema::{JazzSchema, TableSchema};

    use super::*;

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
                .map(|(_, plan)| plan),
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
