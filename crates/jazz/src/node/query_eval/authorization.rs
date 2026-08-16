//! Query authorization semantics and policy-proof graph construction.
//!
//! This module selects operation-specific policies and policy-owning schemas,
//! constructs read/write authorization requests and support scopes, and caches
//! executable proof graphs. Parameter-slot and claim rewriting mechanics live
//! in the bindings module.

use super::*;

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

pub(super) fn empty_policy_filtered_current_source_graph(
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

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn compile_permission_scope_policy(
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
pub(super) fn authorization_scope_action(
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
pub(super) fn authorization_policy_queries(
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
pub(super) fn authorization_operation_key(
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
    pub(super) fn policy_authorization_row_id_graph(
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

    pub(in crate::node) fn branch_read_policy_authorized_branch_ids(
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

    pub(super) fn query_program_policy_context(&self, identity: AuthorId) -> PolicyContext {
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

    pub(in crate::node) fn write_policy_query_allows_current_row(
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
    pub(in crate::node) fn write_policy_query_allows_candidate(
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

    pub(in crate::node) fn write_policy_query_allows_candidate_for_schema(
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

    pub(in crate::node) fn branch_write_policy_query_allows_candidate(
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

    pub(super) fn write_policy_query_program_allows(
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

    pub(super) fn policy_filtered_current_source_graph_via_query_engine(
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

    pub(super) fn table_read_policy_authorization_request(
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

    pub(super) fn table_read_policy_authorization_request_at(
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

    pub(super) fn table_read_policy_authorization_request_for_include_deleted(
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

    pub(super) fn table_read_policy_authorization_request_with_root_visibility(
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

    pub(super) fn branch_table_read_policy_authorization_request(
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

    pub(super) fn maintained_view_content_current_with_version(
        &self,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, Error> {
        let schema_version = self.catalogue.current_schema_version_id;
        self.maintained_view_content_current_with_version_in_schema(table, tier, schema_version)
    }

    pub(super) fn maintained_view_content_current_with_version_in_schema(
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
    pub(super) fn authorization_scope_policy_schema_for_action(
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
