//! Lower normalized query programs into executable Groove graphs and terminals.
//!
//! This stage defines terminal schemas, routing fields, fact payloads, and
//! prepared-parameter layouts. It consumes normalized requests and produces
//! executable graph descriptions; source choice and result materialization live
//! in their neighboring stages.

use super::*;
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

pub(super) fn lowered_terminal_graph(
    program: &QueryProgram,
    sink: &str,
) -> Result<GraphBuilder, Error> {
    program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == sink)
        .map(|terminal| terminal.graph.clone())
        .ok_or_else(|| Error::QueryLowering(format!("query program did not emit sink {sink}")))
}

pub(super) fn lowered_app_rows_graph(program: &QueryProgram) -> Result<GraphBuilder, Error> {
    lowered_terminal_graph(program, JAZZ_APP_ROWS_SINK)
}

pub(super) fn lowered_materialization_app_rows_graph(
    program: &QueryProgram,
) -> Result<GraphBuilder, Error> {
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

pub(super) fn lowered_program_sinks(program: &QueryProgram) -> Vec<(String, GraphBuilder)> {
    program
        .lowered
        .terminals
        .iter()
        .map(|terminal| (terminal.sink.clone(), terminal.graph.clone()))
        .collect()
}

pub(super) fn prepared_params_from_domain(
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

pub(super) fn fact_public_fields(
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
pub(crate) struct PolicyAuthorizationGraph {
    pub(super) graph: GraphBuilder,
    pub(super) route_fields: BTreeSet<String>,
    /// Physical source narrowing derived while compiling this policy program.
    /// This is only an optimization hint: the policy graph remains the sole
    /// authorization decision.
    pub(super) access_paths: BTreeMap<SourceId, CurrentAccessPath>,
}

pub(super) fn policy_authorization_graph_cache_key(request: &QueryProgramRequest) -> String {
    format!("{request:?}")
}

pub(super) fn output_routing_fields_for_query_eval(
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
    fields.extend(schema.identity.branch_or_prefix_field.clone());
    fields
}

pub(super) fn descriptor_field_names(descriptor: &RecordDescriptor) -> Result<Vec<String>, Error> {
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

pub(super) fn versioned_row_ref_fields(schema: &VersionedRowRefSchema) -> Vec<String> {
    let mut fields = row_ref_fields(&schema.row);
    fields.extend(schema.branch_or_prefix_field.clone());
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

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn compile_query_program_request(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<QueryProgram, Error> {
        let access_paths = self.query_program_access_paths(&request)?;
        self.compile_query_program_request_with_access_paths(request, access_paths)
            .await
    }

    pub(super) async fn compile_query_program_request_with_access_paths(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_inline_sources_and_access_paths(
            request,
            BTreeMap::new(),
            access_paths,
        )
        .await
    }

    pub(super) async fn compile_query_program_request_with_inline_sources_and_access_paths(
        &mut self,
        request: QueryProgramRequest,
        inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        Box::pin(self.prepare_query_program_policy_dependencies(&request)).await?;
        let trace_request = capability_trace_enabled().then(|| request.clone());
        let read_view = request.reads.primary.clone();
        let mut resolver = JazzSourceGraphPreparer {
            node: self,
            read_view: &read_view,
            inline_sources,
            access_paths,
            current_projection_targets: BTreeMap::new(),
        };
        let node_uuid = resolver.node.node_uuid;
        let node_alias = resolver.node.self_node_alias;
        let result = Box::pin(prepare_and_lower_query_program(request, &mut resolver)).await;
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

    async fn prepare_query_program_policy_dependencies(
        &mut self,
        request: &QueryProgramRequest,
    ) -> Result<(), Error> {
        let source_requests = query_program_source_requests(request)
            .map_err(|report| Error::QueryCapability(format!("{report:?}")))?;
        let read_view = request.reads.primary.clone();
        let dependencies = {
            let mut preparer = JazzSourceGraphPreparer {
                node: self,
                read_view: &read_view,
                inline_sources: BTreeMap::new(),
                access_paths: BTreeMap::new(),
                current_projection_targets: BTreeMap::new(),
            };
            source_requests
                .iter()
                .map(|source| preparer.policy_dependency_request(source))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        };
        let dependencies = dependencies
            .into_iter()
            .map(|dependency| {
                (
                    policy_authorization_graph_cache_key(&dependency),
                    dependency,
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (cache_key, dependency) in dependencies {
            match Box::pin(self.policy_authorization_row_id_graph(dependency)).await {
                Ok(_) => {}
                Err(Error::QueryCapability(error)) if error.contains("PolicyProofCycle") => {
                    return Err(Error::QueryCapability(error));
                }
                Err(Error::QueryCapability(_)) => {
                    self.query.policy_authorization_graph_cache.insert(
                        cache_key,
                        PolicyAuthorizationGraph {
                            graph: empty_authorized_row_id_graph(),
                            route_fields: BTreeSet::new(),
                            access_paths: BTreeMap::new(),
                        },
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) async fn prepared_query_plan_from_program(
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
            let prepared = self
                .database
                .prepare(
                    [groove::ivm::RoutedMultisinkTerminal::new(
                        JAZZ_APP_ROWS_SINK,
                        graph,
                        route_fields,
                        app_row_fields,
                    )
                    .with_route_value_indices(route_value_indices)],
                    binding_source_shape,
                    binding_descriptor,
                )
                .await?;
            Ok(PreparedQueryPlan::Prepared {
                shape: prepared.id(),
                params,
            })
        }
    }

    pub(super) async fn subscribe_lowered_program(
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
        let prepared = self
            .database
            .prepare(terminals, binding_source_shape, binding_descriptor)
            .await?;
        self.database
            .bind_shape(prepared.id(), &values)
            .await
            .map_err(Error::Groove)
    }
}
