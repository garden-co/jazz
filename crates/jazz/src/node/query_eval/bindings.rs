//! Stable query, policy-claim, and prepared-plan binding mechanics.
//!
//! This module discovers parameter requirements, assigns stable claim slots,
//! rewrites aliases, coerces values, and constructs binding/cache identities.
//! It does not decide whether a bound policy authorizes an operation.

use super::*;

/// The caller-owned meaning of an absent claim while binding a prepared plan.
/// Ordinary prepared queries require all declared bindings. Authorization
/// support, on the other hand, must represent an absent policy claim as an
/// empty proof for the commit's permission subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PreparedClaimBindingMode {
    Strict,
    FailClosedAuthorizationSupport,
}

pub(super) fn authorization_query_from_read_policy(table: &TableSchema) -> JazzQuery {
    let Some(policy) = &table.read_policy else {
        let mut query = crate::query::Query::from(table.name.as_str());
        // A table becomes closed as soon as it declares any policy.  An
        // omitted read clause is therefore an explicit empty read authority,
        // not the policy-free table default.  Express it as the smallest
        // ordinary query graph (a constant-false root predicate) so current,
        // historical, maintained, and advice paths all lower through the same
        // authorization machinery.
        if access_edge_parent_reference(table).is_none() && table.has_any_policy() {
            query.filters.push(Predicate::Any(Vec::new()));
        }
        return query;
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

pub(super) fn access_edge_parent_reference(table: &TableSchema) -> Option<String> {
    if !table.name.ends_with("_access_edges") && table.name != "team_access_edges" {
        return None;
    }
    table
        .references
        .contains_key("resource_id")
        .then(|| "resource_id".to_owned())
}

pub(super) fn rewrite_claim_join_for_binding(
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

pub(super) fn rewrite_claim_predicate_for_binding(
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

pub(super) fn default_permission_scope_claim_values(
    writer: AuthorSubject,
) -> BTreeMap<String, Value> {
    default_policy_claim_values(writer)
}

pub(super) fn default_policy_claim_values(writer: AuthorSubject) -> BTreeMap<String, Value> {
    // Alpha-compat built-ins live at the node admission/query boundary, not in
    // the compiler: lowering receives ordinary claim values plus spec `sub`.
    BUILTIN_POLICY_CLAIMS
        .iter()
        .map(|name| {
            let value = match *name {
                "user" => Value::String(writer.canonical().to_owned()),
                "isAdmin" => Value::Bool(false),
                _ => unreachable!("unknown built-in policy claim"),
            };
            ((*name).to_owned(), value)
        })
        .collect()
}

const BUILTIN_POLICY_CLAIMS: &[&str] = &["user", "isAdmin"];

fn is_builtin_policy_claim(name: &str) -> bool {
    BUILTIN_POLICY_CLAIMS.contains(&name)
}

pub(super) fn bind_scope_claim_operands(
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
    let storage_name = crate::query::operand_claim_storage_key(name);
    let Some(value) = claim_values.get(&storage_name).cloned() else {
        return;
    };
    let param = claim_param_field(&ClaimPath(crate::query::operand_claim_path(name)));
    binding_values.insert(param.clone(), value);
    *operand = Operand::Param(param);
}

pub(super) fn disambiguate_policy_claim_params(
    query: &mut JazzQuery,
    schema: &RuntimeSchema,
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
pub(super) fn disambiguate_policy_claim_params_with_outer_slots(
    query: &mut JazzQuery,
    schema: &RuntimeSchema,
    binding_values: &mut BTreeMap<String, Value>,
    outer_slots: &BTreeMap<String, ProgramClaimParam>,
) -> Result<BTreeMap<String, ProgramClaimParam>, Error> {
    let shape = query.validate_runtime(schema)?;
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

pub(super) fn typed_claim_param_alias(name: &str, ty: &ColumnType) -> String {
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
    matches!(operand, Operand::Claim(name) if !is_builtin_policy_claim(name) && !claims.is_some_and(|claims| {
        let storage = crate::query::operand_claim_storage_key(name);
        claims.contains_key(&storage)
    }))
}

#[derive(Clone, Copy)]
pub(crate) enum ParamBindingMode {
    InlineAllReachableSeeds,
    RetainAllParams,
}

impl ParamBindingMode {
    pub(super) fn cache_key(self) -> ParamBindingModeCacheKey {
        match self {
            Self::InlineAllReachableSeeds => ParamBindingModeCacheKey::InlineAllReachableSeeds,
            Self::RetainAllParams => ParamBindingModeCacheKey::RetainAllParams,
        }
    }
}

pub(super) fn binding_user_params_cache_key(params: &BTreeMap<String, ColumnType>) -> String {
    format!("{params:?}")
}

pub(super) fn binding_claim_params_cache_key(
    params: &BTreeMap<String, ProgramClaimParam>,
) -> String {
    format!("{params:?}")
}

pub(super) fn bind_query_params_with_mode(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &RuntimeSchema,
    mode: ParamBindingMode,
) -> Result<ValidatedQuery, Error> {
    let mut query = shape.query().clone();
    let root_source = root_source_id(&query.table);
    query.filters = if let Some(flat_join) = &query.flat_join {
        let root_scope = flat_join
            .root_alias
            .clone()
            .unwrap_or_else(|| query.table.clone());
        let mut source_tables = BTreeMap::from([(root_scope.clone(), query.table.clone())]);
        source_tables.extend(flat_join.sources.iter().map(|source| {
            (
                source.alias.clone().unwrap_or_else(|| source.table.clone()),
                source.table.clone(),
            )
        }));
        std::mem::take(&mut query.filters)
            .into_iter()
            .map(|predicate| {
                bind_flat_join_predicate(
                    predicate,
                    binding,
                    schema,
                    mode,
                    &root_scope,
                    &source_tables,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        std::mem::take(&mut query.filters)
            .into_iter()
            .map(|predicate| bind_query_predicate(predicate, binding, schema, &root_source, mode))
            .collect::<Result<Vec<_>, _>>()?
    };
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

fn bind_flat_join_predicate(
    predicate: Predicate,
    binding: &Binding,
    schema: &RuntimeSchema,
    mode: ParamBindingMode,
    root_scope: &str,
    source_tables: &BTreeMap<String, String>,
) -> Result<Predicate, Error> {
    if let Predicate::All(predicates) = predicate {
        return predicates
            .into_iter()
            .map(|predicate| {
                bind_flat_join_predicate(
                    predicate,
                    binding,
                    schema,
                    mode,
                    root_scope,
                    source_tables,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Predicate::All);
    }

    let sources = crate::query::flat_join_predicate_sources(&predicate)?;
    let scope = sources
        .iter()
        .next()
        .map(String::as_str)
        .unwrap_or(root_scope);
    let table = source_tables.get(scope).ok_or_else(|| {
        Error::QueryLowering(format!(
            "flat join filter references unknown source {scope}"
        ))
    })?;
    let predicate = crate::query::unqualify_flat_join_predicate(&predicate, scope)?;
    let predicate = bind_query_predicate(
        predicate,
        binding,
        schema,
        &bind_source_for_table(table),
        mode,
    )?;
    Ok(crate::query::qualify_flat_join_source_predicate(
        predicate, scope,
    )?)
}

fn bind_array_subquery_filter_literals(
    mut subquery: ArraySubquery,
    binding: &Binding,
    schema: &RuntimeSchema,
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

pub(super) fn inline_snapshot_bind_filter_literals(
    shape: &ValidatedQuery,
    binding: &Binding,
    schema: &RuntimeSchema,
) -> Result<ValidatedQuery, Error> {
    bind_query_params_with_mode(
        shape,
        binding,
        schema,
        ParamBindingMode::InlineAllReachableSeeds,
    )
}

pub(super) fn retarget_binding_value_sources(
    shape: &mut NormalizedRowSetShape,
    binding_source_shape: &str,
) {
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

pub(super) fn binding_claim_params_for_shape(
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

pub(super) fn normalized_source_tables(shape: &NormalizedRowSetShape) -> BTreeSet<String> {
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

pub(super) fn collect_reachable_seed_claim_params(
    schema: &RuntimeSchema,
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
        let path = ClaimPath(crate::query::operand_claim_path(user_claim));
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
    schema: &RuntimeSchema,
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
    schema: &RuntimeSchema,
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
    schema: &RuntimeSchema,
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
    schema: &RuntimeSchema,
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

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn program_binding_for_shape(
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

    pub(super) fn program_binding_for_shape_and_policy(
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

    pub(super) fn program_binding_for_shape_and_policy_with_prepared_claim_mode(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        source_shape: Option<String>,
        extra_user_params: BTreeMap<String, ColumnType>,
        claim_params: BTreeMap<String, ProgramClaimParam>,
        policy: &PolicyContext,
        prepared_claim_binding_mode: PreparedClaimBindingMode,
    ) -> Result<ProgramBinding, Error> {
        // System authority bypasses row policy entirely.  It consequently has
        // no identity context from which a policy claim could be bound. Some
        // authorization builders receive claim slots from an enclosing
        // prepared plan, so enforce the descriptor invariant at the shared
        // binding boundary as well as in the current-query path.
        //
        // The binding-source key includes every claim slot.  Retaining the
        // caller's key after dropping the slots would let a System program
        // reuse an identity-scoped descriptor, so derive a replacement key
        // from its ordinary query/user parameters only.
        let (source_shape, claim_params) = if matches!(policy, PolicyContext::System) {
            let mut param_types = shape.params().clone();
            param_types.extend(extra_user_params.clone());
            (
                source_shape.and_then(|_| {
                    query_binding_source_shape_for_parts_if_needed(&param_types, &BTreeMap::new())
                }),
                BTreeMap::new(),
            )
        } else {
            (source_shape, claim_params)
        };
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
}
