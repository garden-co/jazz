//! Lower normalized query programs into executable Groove graphs and terminals.
//!
//! This stage defines terminal schemas, routing fields, fact payloads, and
//! prepared-parameter layouts. It consumes normalized requests and produces
//! executable graph descriptions; source choice and result materialization live
//! in their neighboring stages.

use super::*;
use crate::node::query_engine::RequestedSourceExpr;
use groove::db::SubscriptionLifetime;

/// A first-result consumer owns exactly its subscription and, when needed,
/// its prepared shape. Dropping a suspended read cannot keep a binding alive
/// or retire graph roots owned by a different subscription.
struct HydrationSubscription<'a> {
    database: &'a mut groove::db::Database,
    subscription: Option<MultisinkSubscription>,
    prepared_shape: Option<PreparedShapeId>,
}

impl HydrationSubscription<'_> {
    fn release(&mut self) -> Result<(), Error> {
        if let Some(subscription) = self.subscription.take() {
            self.database.unsubscribe(subscription.id());
        }
        if let Some(shape) = self.prepared_shape.take() {
            self.database
                .retire_prepared_shape(shape)
                .map_err(Error::Groove)?;
        }
        Ok(())
    }
}

impl Drop for HydrationSubscription<'_> {
    fn drop(&mut self) {
        // A poisoned database can reject retirement. Its ordinary shutdown
        // still owns that state; never panic while cancelling another error.
        let _ = self.release();
    }
}

#[cfg(test)]
pub(super) struct ScopedPolicyGraphReplacementPause;

#[cfg(test)]
thread_local! {
    static SCOPED_POLICY_GRAPH_REPLACEMENT_PAUSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
impl Drop for ScopedPolicyGraphReplacementPause {
    fn drop(&mut self) {
        SCOPED_POLICY_GRAPH_REPLACEMENT_PAUSED.with(|paused| paused.set(false));
    }
}

#[cfg(test)]
pub(super) fn pause_scoped_policy_graph_replacement_for_test() -> ScopedPolicyGraphReplacementPause
{
    SCOPED_POLICY_GRAPH_REPLACEMENT_PAUSED.with(|paused| paused.set(true));
    ScopedPolicyGraphReplacementPause
}

#[cfg(test)]
async fn wait_for_scoped_policy_graph_replacement_for_test() {
    if SCOPED_POLICY_GRAPH_REPLACEMENT_PAUSED.with(|paused| paused.get()) {
        std::future::pending::<()>().await;
    }
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

pub(super) fn materialization_app_row_schema(
    plan: Option<&PreparedQueryPlan>,
    program: Option<&QueryProgram>,
) -> Result<AppRowSchema, Error> {
    match plan {
        Some(plan) => plan
            .app_row_schema()
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "prepared plan has no app-row schema",
            )),
        None => app_row_terminal_schema(
            &program
                .ok_or(Error::InvalidStoredValue(
                    "materialization has no lowered program",
                ))?
                .lowered
                .output,
        )
        .cloned(),
    }
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
    let existence_only = program
        .request
        .output
        .app_rows
        .as_ref()
        .is_some_and(|rows| !rows.public_terminal);
    if existence_only || publishes_structured_tree || public_root_owns_membership {
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
        .execution_terminals()
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
            // A union arm is part of a result occurrence address, just like
            // its paired joined-row id. Retaining only the id causes prepared
            // maintained subscriptions to publish a descriptor that cannot be
            // decoded against the membership schema.
            fields.extend(schema.occurrence_union_arm_fields.values().cloned());
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
                    .map(|field| {
                        field
                            .name
                            .clone()
                            .expect("lowered aggregate field is named")
                    }),
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
            let mut fields = version_witness_public_fields(&schema.role_field, witness);
            fields.extend(schema.routing_param_fields.iter().cloned());
            Ok(fields)
        }
        ProgramFactSchema::ProgramSourceCoverage(schema) => {
            let mut fields = vec!["complete".to_owned()];
            fields.extend(schema.routing_param_fields.iter().cloned());
            Ok(fields)
        }
        unsupported => Err(Error::InvalidStoredValue(match unsupported {
            ProgramFactSchema::PathCorrelationCoverage(_) => {
                "path correlation coverage facts are not prepared yet"
            }
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
            | ProgramFactSchema::ProgramSourceCoverage(_)
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

#[derive(Clone, Debug, Default)]
struct PolicyDependencyFootprint {
    tables: BTreeSet<String>,
    uncertain: bool,
}

impl PolicyDependencyFootprint {
    fn include(&mut self, dependency: &QueryProgramRequest) {
        self.tables.extend(
            dependency
                .reads
                .primary
                .sources
                .keys()
                .map(|source| source.table.clone()),
        );
        self.tables.extend(
            dependency
                .reads
                .fact_reads
                .values()
                .flat_map(|read| read.sources.keys())
                .map(|source| source.table.clone()),
        );
    }
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
        super::query_engine::ProgramFactSchema::ProgramSourceCoverage(schema) => {
            schema.routing_param_fields.clone()
        }
        super::query_engine::ProgramFactSchema::VersionWitnesses(schema)
        | super::query_engine::ProgramFactSchema::ReplacementWitnesses(schema) => {
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

const COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES: usize = 32;

/// Unused admission products kept for their installers. A client opens a
/// whole screen of subscriptions before any installer runs, so this must
/// cover a realistic batch: at 32, a 61-list dashboard recompiled the 29
/// oldest programs. Still well below the 256-proof budget; eviction only
/// repeats compilation.
pub(super) const ADMISSION_HANDOFF_MAX_PROGRAMS: usize = 128;

/// An admission proof may hand its immutable compiler output to the first
/// matching installer. No evaluator, live binding, rows or subscription is retained.
/// Consuming the program leaves the cheap capability proof resident.
#[derive(Clone, Debug)]
pub(crate) struct SupportedQueryProgram {
    fingerprint: [u8; 32],
    program: Option<QueryProgram>,
}

fn admission_program_key(
    request: &QueryProgramRequest,
    access_paths: &BTreeMap<SourceId, CurrentAccessPath>,
) -> Option<[u8; 32]> {
    (matches!(
        request.authorization_mode,
        QueryAuthorizationMode::TrustedServing | QueryAuthorizationMode::ClientLocal
    ) && query_program_sources_cache_safe(request))
    .then(|| *blake3::hash(query_program_cache_key(request, access_paths).as_bytes()).as_bytes())
}

fn query_program_source_cache_safe(source: &RequestedSourceExpr) -> bool {
    match source {
        SourceExpr::VisibleCurrent {
            data: DataSource::Current,
            ..
        } => true,
        SourceExpr::WithOverlays { input, overlays } if overlays.entries.is_empty() => {
            query_program_source_cache_safe(input)
        }
        SourceExpr::LensProject { input, .. } => query_program_source_cache_safe(input),
        SourceExpr::Merge { inputs, .. } => inputs.iter().all(query_program_source_cache_safe),
        _ => false,
    }
}

fn query_program_cache_safe(request: &QueryProgramRequest) -> bool {
    request.authorization_mode == QueryAuthorizationMode::TrustedServing
        && query_program_sources_cache_safe(request)
}

fn query_program_sources_cache_safe(request: &QueryProgramRequest) -> bool {
    request
        .reads
        .primary
        .sources
        .values()
        .all(query_program_source_cache_safe)
        && request
            .reads
            .fact_reads
            .values()
            .flat_map(|read| read.sources.values())
            .all(query_program_source_cache_safe)
}

fn query_program_cache_key(
    request: &QueryProgramRequest,
    access_paths: &BTreeMap<SourceId, CurrentAccessPath>,
) -> String {
    format!("request={request:?};access_paths={access_paths:?}")
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Build the request (including strict claims) on every call. Remember a
    /// successful exact-context proof and hand its compiler output to a matching
    /// installer, which still owns its own evaluator and binding. Programs with
    /// per-receiver covered inputs cannot use this handoff.
    pub(super) async fn ensure_query_program_request_supported(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        // Branches, overlays, inline snapshots and covered inputs
        // likewise stay on the ordinary path. No data-sensitive source is
        // admitted from a remembered success.
        let key = admission_program_key(&request, &access_paths);
        if key.as_ref().is_some_and(|key| {
            self.query
                .supported_query_program_requests
                .iter()
                .any(|entry| &entry.fingerprint == key)
        }) {
            return Ok(());
        }
        let program = self
            .compile_query_program_request_with_access_paths(request, access_paths)
            .await?;
        if let Some(key) = key {
            self.remember_supported_query_program(key, Some(program));
        }
        Ok(())
    }

    fn remember_supported_query_program(&mut self, key: [u8; 32], program: Option<QueryProgram>) {
        if let Some(entry) = self
            .query
            .supported_query_program_requests
            .iter_mut()
            .find(|entry| entry.fingerprint == key)
        {
            if program.is_none() {
                return;
            }
            // The compiler already recorded the proof. Release any old product
            // before attaching this one with the same handoff budget.
            entry.program = None;
        } else {
            // FIFO eviction only causes recompilation. There is no lifetime
            // admission quota and failed/cancelled compilation is never cached.
            const MAX_ADMISSIONS: usize = 256;
            if self.query.supported_query_program_requests.len() == MAX_ADMISSIONS {
                self.query.supported_query_program_requests.pop_front();
            }
            self.query
                .supported_query_program_requests
                .push_back(SupportedQueryProgram {
                    fingerprint: key,
                    program: None,
                });
        }
        if let Some(program) = program {
            // Keep a bounded compiled-program budget independently of the
            // larger proof budget. An abandoned admission cannot retain
            // arbitrarily many executable descriptions; eviction only repeats
            // compilation, never rejects a query. The first installer takes
            // ownership, so used programs do not occupy this handoff budget.
            if self
                .query
                .supported_query_program_requests
                .iter()
                .filter(|entry| entry.program.is_some())
                .count()
                >= ADMISSION_HANDOFF_MAX_PROGRAMS
            {
                if let Some(oldest) = self
                    .query
                    .supported_query_program_requests
                    .iter_mut()
                    .find(|entry| entry.program.is_some())
                {
                    oldest.program = None;
                }
            }
            self.query
                .supported_query_program_requests
                .iter_mut()
                .find(|entry| entry.fingerprint == key)
                .expect("proof inserted above")
                .program = Some(program);
        }
    }

    pub(super) async fn compile_query_program_request(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_access_paths(request, BTreeMap::new())
            .await
    }

    pub(super) async fn compile_query_program_request_with_access_paths(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        let key = admission_program_key(&request, &access_paths);
        if let Some(key) = key
            && let Some(program) = self
                .query
                .supported_query_program_requests
                .iter_mut()
                .find(|entry| entry.fingerprint == key)
                .and_then(|entry| entry.program.take())
        {
            return Ok(program);
        }
        let cache_key = query_program_cache_safe(&request)
            .then(|| query_program_cache_key(&request, &access_paths));
        if let Some(program) = cache_key
            .as_ref()
            .and_then(|key| self.query.compiled_query_program_cache.get(key))
        {
            let program = (**program).clone();
            if let Some(key) = key {
                self.remember_supported_query_program(key, None);
            }
            return Ok(program);
        }
        let program = self
            .compile_query_program_request_with_inline_sources_and_access_paths(
                request,
                BTreeMap::new(),
                access_paths,
            )
            .await?;
        if let Some(cache_key) = cache_key {
            if self.query.compiled_query_program_cache.len()
                >= COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES
                && let Some(eviction_key) = self
                    .query
                    .compiled_query_program_cache
                    .keys()
                    .next()
                    .cloned()
            {
                self.query
                    .compiled_query_program_cache
                    .remove(&eviction_key);
            }
            self.query
                .compiled_query_program_cache
                .insert(cache_key, Arc::new(program.clone()));
        }
        // The order can be reversed: a foreground installs locally before it
        // receives RegisterShape. Its successful compilation is already the
        // exact capability proof; later admission need not compile it again.
        if let Some(key) = key {
            self.remember_supported_query_program(key, None);
        }
        Ok(program)
    }

    pub(super) async fn compile_query_program_request_with_inline_sources_and_access_paths(
        &mut self,
        request: QueryProgramRequest,
        inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs(
            request,
            inline_sources,
            access_paths,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .await
    }

    /// Compile a receiver-local authority-covered program. Each entry is an
    /// already allocated, runtime-owned input keyed by its complete normalized
    /// source occurrence. This is intentionally separate from ordinary inline
    /// snapshots: the caller can atomically replace these records after the
    /// graph is subscribed.
    pub(super) async fn compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs(
        &mut self,
        request: QueryProgramRequest,
        inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        covered_input_sources: BTreeMap<SourceId, GraphBuilder>,
        covered_input_descriptors: BTreeMap<SourceId, RecordDescriptor>,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_inline_sources_and_access_paths_inner(
            request,
            inline_sources,
            access_paths,
            covered_input_sources,
            covered_input_descriptors,
            true,
            None,
        )
        .await
    }

    pub(super) async fn compile_query_program_request_with_shared_access_paths(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        bounded_deletion_register: Option<(SourceId, GraphBuilder)>,
    ) -> Result<QueryProgram, Error> {
        self.compile_query_program_request_with_inline_sources_and_access_paths_inner(
            request,
            BTreeMap::new(),
            access_paths,
            BTreeMap::new(),
            BTreeMap::new(),
            false,
            bounded_deletion_register,
        )
        .await
    }

    pub(super) async fn compile_query_program_request_with_bounded_deletion_register(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        bounded_deletion_register: (SourceId, GraphBuilder),
    ) -> Result<QueryProgram, Error> {
        // The inline register is bound to this snapshot. A cached program
        // would retain stale deletion state after a later write.
        self.compile_query_program_request_with_inline_sources_and_access_paths_inner(
            request,
            BTreeMap::new(),
            access_paths,
            BTreeMap::new(),
            BTreeMap::new(),
            true,
            Some(bounded_deletion_register),
        )
        .await
    }

    #[cfg_attr(
        feature = "cold-settle-attribution",
        tracing::instrument(skip_all, name = "cold.phase.query_lowering")
    )]
    async fn compile_query_program_request_with_inline_sources_and_access_paths_inner(
        &mut self,
        request: QueryProgramRequest,
        inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
        covered_input_sources: BTreeMap<SourceId, GraphBuilder>,
        covered_input_descriptors: BTreeMap<SourceId, RecordDescriptor>,
        count_access_path_metrics: bool,
        bounded_deletion_register: Option<(SourceId, GraphBuilder)>,
    ) -> Result<QueryProgram, Error> {
        #[cfg(any(test, feature = "testing"))]
        {
            self.query_program_compilations += 1;
        }
        #[cfg(any(test, feature = "testing"))]
        if std::env::var_os("JAZZ_COMPILE_SHAPES").is_some() {
            // Opt-in work classification only. Never emit queries, claims,
            // literals or row contents; these process-local hashes are not
            // cache identities and are not a serialization contract.
            let fingerprint = |value: String| blake3::hash(value.as_bytes()).to_hex().to_string();
            eprintln!(
                "JAZZ_COMPILE_SHAPES node={} mode={:?} request={} structure={} sources={} binding={} paths={} inline={} covered={}",
                fingerprint(format!("{:?}", self.node_uuid)),
                request.authorization_mode,
                fingerprint(format!("{request:?}")),
                fingerprint(format!("{:?}", (&request.input.shape, &request.output))),
                fingerprint(format!("{:?}", (&request.reads, &request.policy))),
                fingerprint(format!("{:?}", request.input.binding)),
                fingerprint(format!("{access_paths:?}")),
                inline_sources.len(),
                covered_input_sources.len()
            );
        }
        self.restore_expired_policy_compilation_state();
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
            && !covered_input_sources.is_empty()
        {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=compile_receiver_program requested_sources={:?} runtime_sources={:?}",
                request.reads.primary.sources.keys().collect::<Vec<_>>(),
                covered_input_sources.keys().collect::<Vec<_>>(),
            );
        }
        let policy_replacement_lease = std::rc::Rc::new(());
        let compilation = QueryProgramCompilation::analyze(request)
            .map_err(|report| Error::QueryCapability(format!("{report:?}")))?;
        let request = compilation.request();
        let policy_dependency_footprint = Box::pin(self.prepare_query_program_policy_dependencies(
            request,
            compilation.sources(),
            &access_paths,
            bounded_deletion_register.as_ref(),
            &policy_replacement_lease,
        ))
        .await?;
        let trace_request = capability_trace_enabled().then(|| request.clone());
        let read_view = request.reads.primary.clone();
        let mut resolver = JazzSourceGraphPreparer {
            local_unavailable_scope: unavailable_inputs::local_unavailable_policy_binding(&request),
            node: self,
            read_view: &read_view,
            inline_sources,
            covered_input_sources,
            covered_input_descriptors,
            access_paths,
            bounded_deletion_register,
            count_access_path_metrics,
            current_projection_targets: BTreeMap::new(),
        };
        let node_uuid = resolver.node.node_uuid;
        let node_alias = resolver.node.self_node_alias;
        let mut result = match Box::pin(crate::node::query_engine::prepare_query_program_sources(
            &compilation,
            &mut resolver,
        ))
        .await
        {
            Ok((sources, explain)) => resolver.node.query.query_program_templates.lower(
                compilation,
                sources,
                explain,
                |graph| resolver.node.database.describe_template_input(graph),
            ),
            Err(error) => Err(error),
        };
        if let Ok(program) = result.as_mut() {
            program
                .lowered
                .targeted_refresh_tables
                .extend(policy_dependency_footprint.tables);
            program.lowered.targeted_refresh_uncertain |= policy_dependency_footprint.uncertain;
        }
        resolver
            .node
            .restore_scoped_policy_authorization_graphs(&policy_replacement_lease);
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
        source_requests: &[SourceRequest],
        outer_access_paths: &BTreeMap<SourceId, CurrentAccessPath>,
        bounded_deletion_register: Option<&(SourceId, GraphBuilder)>,
        lease: &std::rc::Rc<()>,
    ) -> Result<PolicyDependencyFootprint, Error> {
        // A deletion terminal carries the raw register but must be gated by
        // the same source occurrence resolved with its deleted preimage.
        // Preload that policy dependency before the source preparer reaches
        // the sibling; this is only dependency compilation, not another
        // physical source preparation for ordinary reads.
        let policy_source_requests = source_requests
            .iter()
            .cloned()
            .chain(
                source_requests
                    .iter()
                    .filter_map(authorized_deletion_preimage_source_request),
            )
            .collect::<Vec<_>>();
        let read_view = request.reads.primary.clone();
        let (dependencies, footprint) = {
            let mut preparer = JazzSourceGraphPreparer {
                local_unavailable_scope: None,
                node: self,
                read_view: &read_view,
                inline_sources: BTreeMap::new(),
                covered_input_sources: BTreeMap::new(),
                covered_input_descriptors: BTreeMap::new(),
                access_paths: BTreeMap::new(),
                bounded_deletion_register: None,
                count_access_path_metrics: true,
                current_projection_targets: BTreeMap::new(),
            };
            let mut dependencies = Vec::new();
            let mut footprint = PolicyDependencyFootprint::default();
            for source in &policy_source_requests {
                if !matches!(
                    &source.authorization,
                    SourceAuthorizationRequest::PolicyFiltered { .. }
                ) {
                    continue;
                }
                match preparer.policy_dependency_request(source)? {
                    Some(dependency) => {
                        footprint.include(&dependency);
                        // Match the outer occurrence before translating to
                        // the authorization program's protected root. An
                        // unrelated alias of the same table is not restricted
                        // by this query occurrence's equality.
                        let path = outer_access_paths.get(&source.source).cloned();
                        let register = bounded_deletion_register
                            .filter(|(source_id, _)| *source_id == source.source)
                            .map(|(_, graph)| graph.clone());
                        dependencies.push((dependency, path, register));
                    }
                    None => {
                        // Unsupported policy shapes must not silently become
                        // stale subscriptions. Keep the protected table and
                        // make local refresh conservative until a complete
                        // dependency graph is available.
                        footprint.uncertain = true;
                        footprint.tables.insert(source.source.table.clone());
                    }
                }
            }
            (dependencies, footprint)
        };
        let mut grouped = BTreeMap::new();
        for (dependency, path, register) in dependencies {
            let key = policy_authorization_graph_cache_key(&dependency);
            let entry = grouped
                .entry(key)
                .or_insert((dependency, path.clone(), register.clone()));
            // The same reusable proof may serve more than one occurrence.
            // Specialize only when every consumer has the same candidate
            // domain; an unrestricted consumer forces the ordinary proof.
            if entry.1 != path || entry.2 != register {
                entry.1 = None;
                entry.2 = None;
            }
        }
        for (cache_key, (dependency, path, register)) in grouped {
            let candidate_paths = match &dependency.policy {
                PolicyContext::AuthorizationSubplan {
                    protected_source, ..
                } => path.map(|path| BTreeMap::from([(protected_source.clone(), path)])),
                _ => None,
            };
            if let Some(access_paths) = candidate_paths {
                // This request-owned proof covers every OR arm over the
                // protected root, never just the arm providing a claim. The
                // scoped replacement is restored after compilation (including
                // cancellation), leaving reusable policy caches neutral.
                self.begin_scoped_policy_authorization_graph_replacement(&cache_key, lease);
                let bounded_register = match (&dependency.policy, register) {
                    (
                        PolicyContext::AuthorizationSubplan {
                            protected_source, ..
                        },
                        Some(graph),
                    ) => Some((protected_source.clone(), graph)),
                    _ => None,
                };
                match Box::pin(self.point_policy_authorization_row_id_graph(
                    dependency,
                    access_paths,
                    bounded_register,
                ))
                .await
                {
                    Ok(graph) => {
                        self.query
                            .policy_authorization_graph_cache
                            .insert(cache_key.clone(), graph);
                        #[cfg(test)]
                        wait_for_scoped_policy_graph_replacement_for_test().await;
                    }
                    Err(Error::QueryCapability(error)) if error.contains("PolicyProofCycle") => {
                        self.restore_scoped_policy_authorization_graphs(lease);
                        return Err(Error::QueryCapability(error));
                    }
                    Err(Error::QueryCapability(_)) => {
                        self.query.policy_authorization_graph_cache.insert(
                            cache_key.clone(),
                            PolicyAuthorizationGraph {
                                graph: empty_authorized_row_id_graph(),
                                route_fields: BTreeSet::new(),
                                access_paths: BTreeMap::new(),
                            },
                        );
                    }
                    Err(error) => {
                        self.restore_scoped_policy_authorization_graphs(lease);
                        return Err(error);
                    }
                }
            } else {
                match Box::pin(self.policy_authorization_row_id_graph(dependency)).await {
                    Ok(_) => {}
                    Err(Error::QueryCapability(error)) if error.contains("PolicyProofCycle") => {
                        self.restore_scoped_policy_authorization_graphs(lease);
                        return Err(Error::QueryCapability(error));
                    }
                    Err(Error::QueryCapability(_)) => {
                        self.begin_scoped_policy_authorization_graph_replacement(&cache_key, lease);
                        self.query.policy_authorization_graph_cache.insert(
                            cache_key,
                            PolicyAuthorizationGraph {
                                graph: empty_authorized_row_id_graph(),
                                route_fields: BTreeSet::new(),
                                access_paths: BTreeMap::new(),
                            },
                        );
                    }
                    Err(error) => {
                        self.restore_scoped_policy_authorization_graphs(lease);
                        return Err(error);
                    }
                }
            }
        }
        Ok(footprint)
    }

    pub(super) fn restore_expired_policy_compilation_state(&mut self) {
        self.query
            .policy_proof_stack
            .retain(|entry| entry.lease.strong_count() > 0);
        let expired = self
            .query
            .policy_authorization_graph_replacements
            .iter()
            .filter_map(|(key, replacements)| {
                replacements
                    .last()
                    .filter(|replacement| replacement.lease.strong_count() == 0)
                    .map(|_| key.clone())
            })
            .collect::<Vec<_>>();
        for key in expired {
            while self
                .query
                .policy_authorization_graph_replacements
                .get(&key)
                .and_then(|replacements| replacements.last())
                .is_some_and(|replacement| replacement.lease.strong_count() == 0)
            {
                self.restore_scoped_policy_authorization_graph(&key);
            }
        }
    }

    pub(super) fn policy_authorization_graph_cache_get(
        &mut self,
        cache_key: &str,
    ) -> Option<PolicyAuthorizationGraph> {
        self.restore_expired_policy_compilation_state();
        self.query
            .policy_authorization_graph_cache
            .get(cache_key)
            .cloned()
    }

    fn begin_scoped_policy_authorization_graph_replacement(
        &mut self,
        cache_key: &str,
        lease: &std::rc::Rc<()>,
    ) {
        self.restore_expired_policy_compilation_state();
        let previous = self
            .query
            .policy_authorization_graph_cache
            .remove(cache_key);
        self.query
            .policy_authorization_graph_replacements
            .entry(cache_key.to_owned())
            .or_default()
            .push(ScopedPolicyAuthorizationGraphReplacement {
                previous,
                lease: std::rc::Rc::downgrade(lease),
            });
    }

    fn restore_scoped_policy_authorization_graphs(&mut self, lease: &std::rc::Rc<()>) {
        let keys = self
            .query
            .policy_authorization_graph_replacements
            .iter()
            .filter_map(|(key, replacements)| {
                replacements
                    .last()
                    .filter(|replacement| {
                        std::rc::Weak::ptr_eq(&replacement.lease, &std::rc::Rc::downgrade(lease))
                    })
                    .map(|_| key.clone())
            })
            .collect::<Vec<_>>();
        for key in keys {
            self.restore_scoped_policy_authorization_graph(&key);
        }
    }

    fn restore_scoped_policy_authorization_graph(&mut self, cache_key: &str) {
        let (replacement, emptied) = {
            let Some(replacements) = self
                .query
                .policy_authorization_graph_replacements
                .get_mut(cache_key)
            else {
                return;
            };
            let replacement = replacements
                .pop()
                .expect("policy graph replacement stack is non-empty");
            (replacement, replacements.is_empty())
        };
        if emptied {
            self.query
                .policy_authorization_graph_replacements
                .remove(cache_key);
        }
        self.query
            .policy_authorization_graph_cache
            .remove(cache_key);
        match replacement.previous {
            Some(graph) => {
                self.query
                    .policy_authorization_graph_cache
                    .insert(cache_key.to_owned(), graph);
            }
            None => {
                self.query
                    .policy_authorization_graph_cache
                    .remove(cache_key);
            }
        }
    }

    pub(super) async fn prepared_query_plan_from_program(
        &mut self,
        program: &QueryProgram,
        _shape: &ValidatedQuery,
        _binding: &Binding,
    ) -> Result<PreparedQueryPlan, Error> {
        let output = app_row_terminal_schema(&program.lowered.output)?.clone();
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
            Ok(PreparedQueryPlan::Graph { graph, output })
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
                output,
            })
        }
    }

    pub(super) async fn subscribe_lowered_program(
        &mut self,
        program: QueryProgram,
        binding: &Binding,
        binding_source_shape: String,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<MultisinkSubscription, Error> {
        self.install_lowered_program_subscription(
            program,
            binding,
            binding_source_shape,
            prepared_claim_binding_mode,
            progress_waker,
            SubscriptionLifetime::Retained,
        )
        .await
        .map(|(subscription, _)| subscription)
    }

    /// The same installation and binding path serves one-result and retained
    /// consumers. Output terminals differ, but source hydration does not.
    async fn install_lowered_program_subscription(
        &mut self,
        program: QueryProgram,
        binding: &Binding,
        binding_source_shape: String,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
        progress_waker: Option<&std::task::Waker>,
        lifetime: SubscriptionLifetime,
    ) -> Result<(MultisinkSubscription, Option<PreparedShapeId>), Error> {
        // Subscription opening performs one bounded IVM poll.  When that poll
        // finds cold storage, retain the node owner's wake route so the
        // runtime can resume it without unrelated transport traffic.
        let params = prepared_params_from_domain(&program.lowered.parameters);
        let route_params = prepared_route_param_names(&program.lowered.parameters);
        if params.is_empty() {
            let sinks = lowered_program_sinks(&program);
            return self
                .database
                .subscribe_with_lifetime(sinks, lifetime, progress_waker)
                .map(|subscription| (subscription, None))
                .map_err(|error| {
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=subscribe_receiver_error error={error:?}"
                        );
                    }
                    Error::Groove(error)
                });
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
            .execution_terminals()
            .map(|terminal| {
                let public_fields = terminal_public_fields(&terminal.output)?;
                let route_fields = terminal_route_fields(
                    &route_params,
                    &terminal_route_eligible_fields(&terminal.output)?,
                );
                let route_value_indices = prepared_route_value_indices(&params, &route_fields);
                Ok(RoutedMultisinkTerminal::new(
                    terminal.sink.clone(),
                    terminal.graph.clone(),
                    route_fields,
                    public_fields,
                )
                .with_route_value_indices(route_value_indices))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let prepared = self
            .database
            .prepare(terminals, binding_source_shape, binding_descriptor)
            .await
            .map_err(|error| {
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    eprintln!(
                        "JAZZ_COVERED_INPUT_TRACE stage=prepare_receiver_error error={error:?}"
                    );
                }
                Error::Groove(error)
            })?;
        // prepare() allocates a caller-owned shape. Own it before the binding
        // await so cancellation during cold hydration also releases it.
        let mut owner = HydrationSubscription {
            database: &mut self.database,
            subscription: None,
            prepared_shape: Some(prepared.id()),
        };
        let subscription = owner
            .database
            .bind_shape_with_lifetime(prepared.id(), &values, lifetime, progress_waker)
            .await
            .map_err(|error| {
                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                    eprintln!("JAZZ_COVERED_INPUT_TRACE stage=bind_receiver_error error={error:?}");
                }
                Error::Groove(error)
            })?;
        owner.prepared_shape = None;
        Ok((subscription, Some(prepared.id())))
    }

    pub(super) async fn hydrate_lowered_program_once(
        &mut self,
        mut program: QueryProgram,
        binding: &Binding,
    ) -> Result<RecordDeltas, Error> {
        // Hydrate through the same live installation as a retained consumer.
        // The native CurrentRow boundary still consumes the compiler's
        // materialization layout (including physical provenance), whereas a
        // retained stream publishes its terminal layout directly.
        let graph = lowered_materialization_app_rows_graph(&program)?;
        program
            .lowered
            .terminals
            .retain(|terminal| terminal.sink == JAZZ_APP_ROWS_SINK);
        program.lowered.terminals[0].graph = graph;
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
        let (subscription, prepared_shape) = self
            .install_lowered_program_subscription(
                program,
                binding,
                binding_source_shape,
                PreparedClaimBindingMode::Strict,
                None,
                SubscriptionLifetime::FirstResult,
            )
            .await?;
        let mut owner = HydrationSubscription {
            database: &mut self.database,
            subscription: Some(subscription),
            prepared_shape,
        };
        let result = futures::future::poll_fn(|cx| {
            let subscription = owner
                .subscription
                .as_ref()
                .expect("hydration owns its stream");
            if let std::task::Poll::Ready(event) = subscription.poll_next_event(cx) {
                return std::task::Poll::Ready(Ok(event));
            }
            // Unlike a retained consumer, this call owns progress until its
            // complete initial multisink update, including an empty result.
            if let std::task::Poll::Ready(Err(error)) = owner.database.poll_progress(cx) {
                return std::task::Poll::Ready(Err(Error::Groove(error)));
            }
            subscription.poll_next_event(cx).map(Ok)
        })
        .await;
        let release = owner.release();
        let snapshot = match result? {
            GrooveSubscriptionEvent::Update(update) => update.deltas,
            GrooveSubscriptionEvent::Error(error) => return Err(Error::Groove(error.into())),
        };
        release?;
        take_required_sink_deltas(snapshot, JAZZ_APP_ROWS_SINK)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::query_engine::{
        ContentVersionFields, ProgramFactOutput, ProgramFactSchema, ProgramFactTerminal,
        ProgramSourceCoverageSchema, ResultMembershipSchema, VersionWitnessSchema,
        VersionWitnessSchemas,
    };

    #[test]
    fn result_membership_public_fields_retain_union_arm_identity() {
        let fields = fact_public_fields(&ProgramFactSchema::ResultMembership(
            ResultMembershipSchema {
                table_field: "table_name".to_owned(),
                row_field: "row_uuid".to_owned(),
                occurrence_id_fields: vec!["row_uuid".to_owned(), "__root_join_row_0".to_owned()],
                occurrence_union_arm_fields: BTreeMap::from([(0, "__root_join_arm_0".to_owned())]),
                payload_fields: Vec::new(),
                payload_publication_fields: BTreeMap::new(),
                branch_or_prefix_field: None,
                version: ResultMembershipVersionSchema::Content(ContentVersionFields {
                    tx_time_field: "content_tx_time".to_owned(),
                    tx_node_field: "content_tx_node_id".to_owned(),
                }),
                settle_position_field: Some("settle_position".to_owned()),
                routing_param_fields: BTreeSet::new(),
            },
        ))
        .expect("result membership fields");

        assert_eq!(
            fields,
            [
                "table_name",
                "row_uuid",
                "__root_join_row_0",
                "__root_join_arm_0",
                "content_tx_time",
                "content_tx_node_id",
                "settle_position",
            ]
        );
    }

    #[test]
    fn covered_input_facts_retain_policy_route_until_multisink_partitioning() {
        let route = "__jazz_claim_v1:6:claims3:sub".to_owned();
        let source = ProgramSourceId {
            table: "todos".to_owned().into(),
            path: vec![crate::protocol::ProgramSourceRole::Root],
        };
        let witness = VersionWitnessSchema {
            source: source.clone(),
            descriptor: RecordDescriptor::new(std::iter::empty::<(String, ValueType)>()),
            identity: VersionIdentityFields {
                table_field: "table_name".to_owned(),
                row_field: "row_uuid".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                batch_id_field: None,
                branch_or_prefix_field: None,
                row_digest_field: None,
                schema_field: "schema_version".to_owned(),
                layer_field: "layer".to_owned(),
            },
            created_by_field: "created_by".to_owned(),
            created_at_field: "created_at".to_owned(),
            updated_by_field: "updated_by".to_owned(),
            updated_at_field: "updated_at".to_owned(),
            parents_field: "parents".to_owned(),
            authored_columns_field: "authored_columns".to_owned(),
            deletion_field: "_deletion".to_owned(),
            user_fields: BTreeMap::new(),
        };
        let witness_schema = ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
            role_field: "event_kind".to_owned(),
            content: Some(witness),
            deletion: None,
            routing_param_fields: BTreeSet::from([route.clone()]),
        });
        let coverage_schema =
            ProgramFactSchema::ProgramSourceCoverage(ProgramSourceCoverageSchema {
                source,
                complete: true,
                routing_param_fields: BTreeSet::from([route.clone()]),
            });

        for schema in [witness_schema, coverage_schema] {
            assert!(
                fact_public_fields(&schema).unwrap().contains(&route),
                "a missing route field would make this fact terminal run for every policy binding"
            );
            assert_eq!(
                output_routing_fields_for_query_eval(&ProgramFactOutput {
                    key: ProgramFactKey::VersionWitnesses,
                    terminal: ProgramFactTerminal::VersionWitnessContent,
                    schema,
                }),
                BTreeSet::from([route.clone()]),
            );
        }
    }
}
