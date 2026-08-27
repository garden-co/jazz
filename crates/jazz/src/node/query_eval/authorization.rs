//! Query authorization semantics and policy-proof graph construction.
//!
//! This module selects operation-specific policies and policy-owning schemas,
//! constructs read/write authorization requests and support scopes, and caches
//! executable proof graphs. Parameter-slot and claim rewriting mechanics live
//! in the bindings module.

use super::*;
use crate::query::{col, eq, lit};

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
        access_paths: BTreeMap::new(),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn compile_permission_scope_policy(
    mut query: JazzQuery,
    claims: Option<&BTreeMap<String, Value>>,
    claim_values: &BTreeMap<String, Value>,
    schema: &RuntimeSchema,
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
    let shape = query.validate_runtime(schema)?;
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

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(crate) async fn read_policy_query_allows_open_tx_row(
        &mut self,
        tx_id: OpenTransactionId,
        policy: &crate::query::Query,
        policy_schema_version: SchemaVersionId,
        row_uuid: RowUuid,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        let policy_schema = if policy_schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&policy_schema_version)
                .ok_or(Error::InvalidStoredValue("policy schema payload missing"))?
                .schema
        }
        .clone();
        // Access-path pushdown is an optimization, not part of the decision
        // boundary. Keep the target coordinate in the policy AST as well so
        // inline transaction overlays cannot authorize one row from another
        // visible row returned by the same source graph.
        let policy = policy
            .clone()
            .filter(eq(col("id"), lit(Value::Uuid(row_uuid.0))));
        let policy_shape =
            policy.validate_with_schema_version(&policy_schema, policy_schema_version)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &policy_schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let binding = policy_shape.bind(BTreeMap::new())?;
        let input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
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
        let policy_context = match self.query_program_policy_context(identity) {
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
        let snapshot = self.open_tx(tx_id)?.base_snapshot.clone();
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: tx_query_read_set(&input.shape, policy_shape.schema_version(), tx_id, snapshot),
            policy: policy_context,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::PolicyPredicate,
                policy_shape.query(),
                &policy_schema,
            ),
        };
        let access_paths = BTreeMap::from([(
            root_source_id(policy_shape.query().table.as_str()),
            CurrentAccessPath::PrimaryKey(vec![Value::Uuid(row_uuid.0)]),
        )]);
        let program = self
            .compile_query_program_request_with_access_paths(request, access_paths)
            .await?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
            .await
    }

    pub(super) async fn policy_authorization_row_id_graph(
        &mut self,
        request: QueryProgramRequest,
    ) -> Result<PolicyAuthorizationGraph, Error> {
        self.query_engine_read_metrics.policy_authorization_graphs += 1;
        let cache_key = policy_authorization_graph_cache_key(&request);
        if let Some(graph) = self.query.policy_authorization_graph_cache.get(&cache_key) {
            return Ok(graph.clone());
        }
        self.policy_authorization_row_id_graph_inner(request, None, true)
            .await
    }

    pub(super) async fn point_policy_authorization_row_id_graph(
        &mut self,
        request: QueryProgramRequest,
        access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<PolicyAuthorizationGraph, Error> {
        self.query_engine_read_metrics.policy_authorization_graphs += 1;
        self.policy_authorization_row_id_graph_inner(request, Some(access_paths), false)
            .await
    }

    async fn policy_authorization_row_id_graph_inner(
        &mut self,
        request: QueryProgramRequest,
        forced_access_paths: Option<BTreeMap<SourceId, CurrentAccessPath>>,
        cache: bool,
    ) -> Result<PolicyAuthorizationGraph, Error> {
        let cache_key = policy_authorization_graph_cache_key(&request);
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

        let result = async {
            let access_paths = match forced_access_paths {
                Some(access_paths) => access_paths,
                None => self.query_program_access_paths(&request)?,
            };
            let program =
                if cache {
                    Box::pin(self.compile_query_program_request_with_access_paths(
                        request,
                        access_paths.clone(),
                    ))
                    .await?
                } else {
                    Box::pin(self.compile_query_program_request_with_shared_access_paths(
                        request,
                        access_paths.clone(),
                    ))
                    .await?
                };
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
                access_paths,
            };
            if cache {
                self.query
                    .policy_authorization_graph_cache
                    .insert(cache_key, graph.clone());
            }
            Ok(graph)
        }
        .await;

        if proof_table.is_some() {
            self.query
                .policy_proof_stack
                .pop()
                .expect("policy proof stack entry is balanced");
        }
        result
    }

    pub(super) fn query_program_policy_context(&self, identity: AuthorSubject) -> PolicyContext {
        if identity == AuthorSubject::SYSTEM {
            PolicyContext::System
        } else {
            let mut claims = default_policy_claim_values(identity);
            if let Some(session_claims) = self.session_claims.get(&identity) {
                claims.extend(session_claims.clone());
            }
            claims.insert(
                "user".to_owned(),
                Value::String(identity.canonical().to_owned()),
            );
            PolicyContext::Identity {
                mode: PolicyEnforcementMode::Enforcing,
                permission_subject: identity,
                claims,
                attribution: None,
            }
        }
    }

    pub(in crate::node) async fn write_policy_query_allows_current_row(
        &mut self,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        let policy_shape = policy.validate(&self.catalogue.schema)?;
        let policy_binding = policy_shape.bind(BTreeMap::new())?;
        let policy_shape = bind_query_params_with_mode(
            &policy_shape,
            &policy_binding,
            &self.catalogue.schema,
            ParamBindingMode::InlineAllReachableSeeds,
        )?;
        let binding = policy_shape.bind(BTreeMap::new())?;
        let input_shape = self.normalized_row_set_shape(&policy_shape, &binding)?;
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
            reads: current_query_read_set(
                &input.shape,
                policy_shape.schema_version(),
                policy_shape.schema_version(),
                DurabilityTier::Local,
                None,
                false,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::PolicyPredicate,
                policy_shape.query(),
                &self.catalogue.schema,
            ),
        };
        // A primary-key access path addresses the physical row UUID without
        // overloading public `id`, which may be a declared user column.
        let access_paths = BTreeMap::from([(
            root_source_id(policy_shape.query().table.as_str()),
            CurrentAccessPath::PrimaryKey(vec![Value::Uuid(row_uuid.0)]),
        )]);
        let program = self
            .compile_query_program_request_with_access_paths(request, access_paths)
            .await?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
            .await
    }

    /// Authorize an inline old/candidate row through the query program.
    ///
    /// Insert candidates reinterpret plain `inherits(parent)` as parent
    /// update-using authorization. Existing/update-check rows retain ordinary
    /// read inheritance unless the policy names an explicit write operation.
    #[allow(dead_code)]
    pub(in crate::node) async fn write_policy_query_allows_candidate(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorSubject,
        insert_candidate: bool,
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
        )
        .await
    }

    pub(in crate::node) async fn write_policy_query_allows_candidate_for_schema(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorSubject,
        insert_candidate: bool,
    ) -> Result<bool, Error> {
        self.write_policy_query_allows_candidate_with_provenance_for_schema(
            policy_schema_version,
            table,
            policy,
            row_uuid,
            cells,
            identity,
            insert_candidate,
            RowProvenance {
                created_by: identity,
                created_at: 0,
                updated_by: identity,
                updated_at: 0,
            },
        )
        .await
    }

    /// Authorize an inline candidate with the provenance the pending version
    /// will expose if accepted. Public provenance policies must evaluate this
    /// metadata exactly like an ordinary current-row source; synthesizing it
    /// from the permission subject would let an updater appear to be the
    /// original creator.
    pub(in crate::node) async fn write_policy_query_allows_candidate_with_provenance_for_schema(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        cells: &BTreeMap<String, Value>,
        identity: AuthorSubject,
        insert_candidate: bool,
        provenance: RowProvenance,
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
            reads: current_query_read_set(
                &input.shape,
                policy_shape.schema_version(),
                policy_shape.schema_version(),
                DurabilityTier::Local,
                None,
                false,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::PolicyPredicate,
                policy_shape.query(),
                &self.catalogue.schema,
            ),
        };
        let candidate = current_row_from_cells_with_explicit_provenance(
            table, row_uuid, cells, provenance, None,
        )?;
        let inline_sources = BTreeMap::from([(root_source, vec![candidate])]);
        let access_paths = self.current_query_primary_key_access_paths(&policy_shape, &binding)?;
        let program = Box::pin(
            self.compile_query_program_request_with_inline_sources_and_access_paths(
                request,
                inline_sources,
                access_paths,
            ),
        )
        .await?;
        self.write_policy_query_program_allows(&program, &policy_shape, &binding)
            .await
    }
    async fn write_policy_query_program_allows(
        &mut self,
        program: &QueryProgram,
        policy_shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<bool, Error> {
        let deltas = match self
            .prepared_query_plan_from_program(&program, &policy_shape, &binding)
            .await?
        {
            PreparedQueryPlan::Graph(graph) => self
                .database
                .query_graph(graph)
                .await
                .map_err(Error::Groove)?,
            PreparedQueryPlan::Prepared { shape, params } => {
                let values = binding_values_for_plan(
                    &binding,
                    &params,
                    &program.request.policy,
                    PreparedClaimBindingMode::Strict,
                )?;
                take_required_sink_deltas(
                    self.bind_shape_snapshot(shape, &values).await?,
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
    pub async fn query_rows(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.require_catalogue_ready()?;
        self.query_rows_with_prepared_plan(shape, binding, tier, None)
            .await
    }

    pub(super) fn compose_policy_filtered_current_source_graph(
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
        let cache_key = policy_authorization_graph_cache_key(&policy_request);
        let authorized = self
            .query
            .policy_authorization_graph_cache
            .get(&cache_key)
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "policy dependency was not prepared before source composition",
            ))?;
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
                access_paths: BTreeMap::new(),
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
            access_paths: BTreeMap::new(),
        })
    }

    pub(super) fn table_read_policy_authorization_request(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorSubject,
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

    pub(super) fn table_read_policy_authorization_request_at(
        &self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorSubject,
        param_binding_mode: ParamBindingMode,
        position: GlobalTime,
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
        // System authority bypasses the table policy.  Use the unfiltered
        // table query rather than merely dropping prepared claim descriptors:
        // retaining policy claim operands in the shape would still require an
        // identity context when the historical graph is lowered.
        let query = if identity == AuthorSubject::SYSTEM {
            JazzQuery::from(table.name.as_str())
        } else {
            authorization_query_from_read_policy(table)
        };
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
                policy_schema,
            ),
        })
    }

    pub(super) fn table_read_policy_authorization_request_for_include_deleted(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table_name: &str,
        identity: AuthorSubject,
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
        identity: AuthorSubject,
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
        // System authority bypasses the table policy.  Its authorization
        // subplan must therefore describe all rows, not the policy's claim
        // predicates: those operands are invalid without an identity context
        // even if the prepared binding descriptor itself has no claim slots.
        let mut query = if identity == AuthorSubject::SYSTEM {
            JazzQuery::from(table.name.as_str())
        } else {
            authorization_query_from_read_policy(table)
        };
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
                false,
            ),
            policy,
            input,
            output: current_query_output_request(
                CurrentQueryProgramOutput::AuthorizedRows,
                policy_shape.query(),
                policy_schema,
            ),
        };
        self.query
            .read_policy_authorization_request_cache
            .insert(cache_key, request.clone());
        Ok(request)
    }

    pub(super) fn maintained_view_content_current_with_version(
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
    pub(crate) async fn test_content_current_with_version(
        &mut self,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<groove::ivm::RecordDeltas, Error> {
        let graph = self.maintained_view_content_current_with_version(table, tier)?;
        self.database
            .query_graph(graph)
            .await
            .map_err(Error::Groove)
    }

    /// Pick the policy-owning schema for one authorization-support operation.
    ///
    /// A newer read-policy revision must not shadow an older write-policy
    /// revision (or vice versa): the terminal proof must hydrate precisely the
    /// policy clause that admission will evaluate.
    fn authorization_scope_policy_schema_for_action(
        &self,
        table: &str,
        _operation: AuthorizationScopeOperation,
    ) -> SchemaVersionId {
        let write_schema = self.catalogue.current_write_schema.schema;
        let has_operation_policy = self
            .table_in_schema(table, write_schema)
            .is_ok_and(|table| table.has_any_policy());
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
        writer: AuthorSubject,
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
        let claim_values = permission_scope_claim_values(writer, claims);
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

pub(super) fn permission_scope_claim_values(
    writer: AuthorSubject,
    claims: Option<&BTreeMap<String, Value>>,
) -> BTreeMap<String, Value> {
    let mut claim_values = claims.cloned().unwrap_or_default();
    // `user` is Jazz's reserved, issuer-scoped logical identity. Provider
    // claims such as `sub` and `user_id` retain their admitted values, while
    // other Jazz defaults remain fallbacks.
    for (name, value) in default_permission_scope_claim_values(writer) {
        if name == "user" {
            claim_values.insert(name, value);
        } else {
            claim_values.entry(name).or_insert(value);
        }
    }
    claim_values
}

#[cfg(test)]
mod authorization_scope_compiler_tests {
    use super::*;
    use crate::ids::NodeUuid;
    use crate::legacy_test_future::{ResultFutureExt as _, SettledNodeTestExt as _};
    use crate::node::NodeState;
    use crate::protocol::TableLens;
    use crate::schema::WritePolicies;
    use crate::tools::public_schema::OperationPolicy as PublicOperationPolicy;
    use crate::tools::{
        ColumnType as PublicColumnType, PolicyExpr as PublicPolicyExpr,
        SchemaBuilder as PublicSchemaBuilder, TablePolicies as PublicTablePolicies,
        TableSchemaBuilder as PublicTableSchemaBuilder, Value as PublicValue,
    };
    use jazz_storage_rocksdb::{Durability, RocksDbStorage};

    fn public_schema(builder: PublicSchemaBuilder) -> JazzSchema {
        crate::schema::JazzSchema::new(&builder.build())
            .expect("authorization-scope test public schema compiles")
    }

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
        let schema = public_schema(
            PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("resources")
                        .column("owner", PublicColumnType::Uuid)
                        .policies(PublicTablePolicies::new().with_select(
                            PublicPolicyExpr::eq_session(
                                "owner",
                                vec!["claims".to_owned(), "user_id".to_owned()],
                            ),
                        )),
                )
                .table(
                    PublicTableSchemaBuilder::new("document_access_edges")
                        .fk_column("resource_id", "resources")
                        .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
                ),
        );
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node = NodeState::new(NodeUuid::from_bytes([7; 16]), schema, storage).unwrap();
        let identity = AuthorSubject::for_test_bytes([8; 16]);
        node.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                crate::query::provider_claim_key("role"),
                Value::String("editor".to_owned()),
            )]),
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
        node.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                crate::query::provider_claim_key("role"),
                Value::String("viewer".to_owned()),
            )]),
        );
        let changed_claims = node
            .authorization_support_scope(identity, &first_action)
            .unwrap();
        assert_ne!(first.key.claims_digest, changed_claims.key.claims_digest);
    }

    #[test]
    fn actual_compiler_selects_write_clauses_and_skips_public_read_support() {
        let claim_policy = |column: &str| {
            PublicPolicyExpr::eq_session(column, vec!["claims".to_owned(), "user_id".to_owned()])
        };
        let schema = public_schema(
            PublicSchemaBuilder::new()
                .table(PublicTableSchemaBuilder::new("public"))
                .table(
                    PublicTableSchemaBuilder::new("protected")
                        .column("value", PublicColumnType::Text)
                        .column("insert_owner", PublicColumnType::Uuid)
                        .column("old_owner", PublicColumnType::Uuid)
                        .column("new_owner", PublicColumnType::Uuid)
                        .column("delete_owner", PublicColumnType::Uuid)
                        .policies(PublicTablePolicies {
                            select: PublicOperationPolicy::default(),
                            insert: PublicOperationPolicy::with_check(claim_policy("insert_owner")),
                            update: PublicOperationPolicy::using_and_check(
                                claim_policy("old_owner"),
                                claim_policy("new_owner"),
                            ),
                            delete: PublicOperationPolicy::using(claim_policy("delete_owner")),
                        }),
                ),
        );
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node = NodeState::new(NodeUuid::from_bytes([9; 16]), schema, storage).unwrap();
        let identity = AuthorSubject::for_test_bytes([3; 16]);
        node.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                crate::query::provider_claim_key("role"),
                Value::String("editor".to_owned()),
            )]),
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

    #[test]
    fn support_scope_preserves_admitted_claims_with_canonical_author() {
        let schema = public_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("resources")
                    .column("owner", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_select(
                        PublicPolicyExpr::eq_session(
                            "owner",
                            vec!["claims".to_owned(), "user_id".to_owned()],
                        ),
                    )),
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node = NodeState::new(NodeUuid::from_bytes([0x51; 16]), schema, storage).unwrap();
        let identity =
            AuthorSubject::authenticated("https://issuer.example", "opaque-subject").unwrap();
        let user_id = uuid::Uuid::from_bytes([0x52; 16]);
        node.set_test_provider_claims(
            identity,
            BTreeMap::from([
                (
                    crate::query::provider_claim_key("sub"),
                    Value::String("provider-subject".to_owned()),
                ),
                (
                    crate::query::provider_claim_key("user_id"),
                    Value::Uuid(user_id),
                ),
            ]),
        );

        let scope = node
            .authorization_support_scope(
                identity,
                &PermissionAdviceAction::Read {
                    table: "resources".to_owned(),
                    row: RowUuid::from_bytes([0x53; 16]),
                },
            )
            .expect("UUID session user_id must bind permission support");
        assert_eq!(scope.subscriptions.len(), 1);
        let binding = &scope.subscriptions[0].1;
        assert!(
            binding
                .values()
                .values()
                .any(|value| value == &Value::Uuid(user_id))
        );
        assert_eq!(
            permission_scope_claim_values(identity, node.session_claims.get(&identity))
                .get(&crate::query::provider_claim_key("sub")),
            Some(&Value::String("provider-subject".to_owned()))
        );
        assert_eq!(
            permission_scope_claim_values(identity, node.session_claims.get(&identity)).get("user"),
            Some(&Value::String(identity.canonical().to_owned()))
        );
    }

    /// A newer read-only policy closes inserts for both the current schema and
    /// projected versions authored under its predecessor.
    #[test]
    fn authorization_scope_uses_newer_read_only_policy_schema() {
        // A structural v2 schema gains a restrictive read policy without any
        // write policy. Read advice must compile v2's support query and an
        // older v1 insert must not bypass v2's now-closed policy set.
        let owner_policy =
            PublicPolicyExpr::eq_session("owner", vec!["claims".to_owned(), "user_id".to_owned()]);
        let base = public_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("notes")
                    .column("owner", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_insert(owner_policy)),
            ),
        );
        let evolved = public_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("notes")
                    .column("owner", PublicColumnType::Uuid)
                    .column("body", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(
                        PublicPolicyExpr::eq_literal(
                            "body",
                            PublicValue::Text("private".to_owned()),
                        ),
                    )),
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        let refs = base.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node =
            NodeState::new(NodeUuid::from_bytes([0x31; 16]), base.clone(), storage).unwrap();
        let evolved_id = evolved.version_id();
        node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
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
        node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved_id,
            },
        })
        .unwrap();

        let scope = node
            .authorization_support_scope(
                AuthorSubject::for_test_bytes([0x32; 16]),
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
                AuthorSubject::for_test_bytes([0x32; 16]),
                &PermissionAdviceAction::Insert {
                    table: "notes".to_owned(),
                    cells: BTreeMap::from([(
                        "owner".to_owned(),
                        Value::Uuid(uuid::Uuid::from_bytes([0x32; 16])),
                    )]),
                },
            )
            .unwrap();
        assert!(
            insert.subscriptions.is_empty(),
            "v2's read-only policy closes insert support instead of retaining v1's grant"
        );
        let writer = AuthorSubject::for_test_bytes([0x32; 16]);
        assert!(
            !node
                .dry_run_mergeable_write_allows_in_schema(
                    base.version_id(),
                    MergeableCommit::new("notes", RowUuid::from_bytes([0x34; 16]), 1)
                        .made_by(writer)
                        .permission_subject(writer)
                        .cells(BTreeMap::from([(
                            "owner".to_owned(),
                            Value::Uuid(uuid::Uuid::from_bytes([0x32; 16])),
                        )])),
                )
                .unwrap(),
            "a v1 version must be projected into the v2 closed policy set"
        );
    }

    /// Alice's v1 `users` version is projected through a rename lens to v2
    /// `people`, where Bob's terminal insert proof compiles the v2 policy.
    ///
    /// alice v1 write ──rename lens──► people action ──► bob's v2 support proof
    #[test]
    fn projected_rename_action_uses_terminal_policy_schema_for_support() {
        let base =
            public_schema(PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("users").column("owner", PublicColumnType::Uuid),
            ));
        let evolved = public_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("people")
                    .column("owner", PublicColumnType::Uuid)
                    .column("body", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_insert(
                        PublicPolicyExpr::eq_literal(
                            "body",
                            PublicValue::Text("migrated".to_owned()),
                        ),
                    )),
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        let refs = base.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
        let mut node =
            NodeState::new(NodeUuid::from_bytes([0x41; 16]), base.clone(), storage).unwrap();
        let evolved_id = evolved.version_id();
        node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
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
        node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
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
            .authorization_support_scope(AuthorSubject::for_test_bytes([0x44; 16]), &actions[0])
            .unwrap();
        assert_eq!(
            scope.subscriptions.len(),
            1,
            "v2 projected insert policy needs support"
        );
    }
}
