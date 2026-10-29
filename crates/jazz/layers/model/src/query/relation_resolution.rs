#[derive(Default)]
struct RelationFacadePlan {
    tables_by_scope: BTreeMap<String, String>,
    filters_by_scope: BTreeMap<String, Vec<Predicate>>,
    joins: Vec<RelationFacadeJoin>,
    output_scope: Option<String>,
    output_projection: Option<Vec<RelationProjectColumn>>,
    order_by: Vec<(String, OrderDirection)>,
    limit: Option<usize>,
    offset: usize,
}

#[derive(Clone, Debug)]
struct RelationFacadeJoin {
    left_scope: String,
    left_column: String,
    right_scope: String,
    right_column: String,
}
#[derive(Clone, Copy)]
enum RelationOutputStep {
    Offset(usize),
    Limit(usize),
}

/// Compose one output window around an already-composed inner window.
fn fold_relation_output_step(
    (offset, limit): (usize, Option<usize>),
    step: RelationOutputStep,
) -> (usize, Option<usize>) {
    match step {
        RelationOutputStep::Offset(value) => {
            let offset = offset.saturating_add(value);
            let limit = limit.map(|remaining| remaining.saturating_sub(value));
            (offset, limit)
        }
        RelationOutputStep::Limit(value) => {
            let limit = Some(limit.map_or(value, |remaining| remaining.min(value)));
            (offset, limit)
        }
    }
}

/// Normalize the currently-supported relation facade subset into the ordinary
/// query shape used by one-shot and maintained execution.
pub fn relation_query_to_query(query: &RelationQuery) -> Result<Query, QueryError> {
    if let Some(parts) = relation_union_parts(&query.rel) {
        let inputs = parts.inputs;
        if inputs.is_empty() {
            return Err(relation_unification_error(
                "union requires at least one labeled input",
            ));
        }
        let mut labels = BTreeSet::new();
        let mut output = None;
        for arm in &inputs {
            if arm.label.is_empty()
                || arm.label.len() > 4096
                || arm.label.contains('\0')
                || !labels.insert(&arm.label)
            {
                return Err(relation_unification_error(
                    "union arm labels must be 1..=4096 bytes, NUL-free, and unique",
                ));
            }
            let arm_query = relation_query_to_query(&RelationQuery {
                rel: arm.input.clone(),
            })?;
            match &output {
                Some(table) if table != &arm_query.table => {
                    return Err(relation_unification_error(
                        "union inputs must output the same table",
                    ));
                }
                None => output = Some(arm_query.table),
                _ => {}
            }
        }
        let mut output_query = Query::from(output.expect("non-empty union has an output table"));
        output_query.relation = Some(query.clone());
        return Ok(output_query);
    }
    if let Some(query) = relation_gather_to_query(&query.rel)? {
        return Ok(query);
    }

    let mut plan = RelationFacadePlan::default();
    collect_relation_facade(&query.rel, &mut plan)?;
    let output_scope = plan.output_scope.clone().ok_or_else(|| {
        relation_unification_error("relation query must end in a project over one output table")
    })?;
    let output_table = plan
        .tables_by_scope
        .get(&output_scope)
        .cloned()
        .ok_or_else(|| relation_unification_error("relation query output scope is unknown"))?;
    let relation = query.clone();
    let mut output_query = Query::from(output_table);
    output_query.relation = Some(relation);

    for filter in plan
        .filters_by_scope
        .remove(&output_scope)
        .unwrap_or_default()
    {
        output_query = output_query.filter(filter);
    }

    if !plan.joins.is_empty() {
        let join = relation_path_join_from_output(&mut plan, &output_scope)?;
        output_query.joins.push(join);
    }

    if !plan.filters_by_scope.is_empty() {
        return Err(relation_unification_error(
            "relation query filters on non-adjacent scopes are not unified yet",
        ));
    }

    for (column, direction) in plan.order_by {
        output_query = output_query.order_by(column, direction);
    }
    if let Some(limit) = plan.limit {
        output_query = output_query.limit(limit);
    }
    if plan.offset != 0 {
        output_query = output_query.offset(plan.offset);
    }
    Ok(output_query)
}

/// Return the terminal relation projection and its source scope.
///
/// Relation projections are deliberately kept separate from `Query::select`:
/// their aliases and source expressions are part of the relation row-set
pub fn relation_output_projection(
    relation: &RelationQuery,
) -> Result<(String, Vec<RelationProjectColumn>), QueryError> {
    if relation_union_parts(&relation.rel).is_some() {
        let (scope, columns) = relation_projection_from_expr(&relation.rel)?.ok_or_else(|| {
            relation_unification_error("relation query must define an output projection")
        })?;
        validate_relation_projection_aliases(&columns)?;
        // A UNION always retains its arm projections.
        reject_reserved_relation_aliases(&columns)?;
        return Ok((scope, columns));
    }
    let mut plan = RelationFacadePlan::default();
    collect_relation_facade(&relation.rel, &mut plan)?;
    let scope = plan.output_scope.ok_or_else(|| {
        relation_unification_error("relation query must end in a project over one output table")
    })?;
    let columns = plan.output_projection.ok_or_else(|| {
        relation_unification_error("relation query must define an output projection")
    })?;
    Ok((scope, columns))
}
/// Return explicit output aliases when the relation expression has them.
///
/// A supported `Gather` is normalized into the ordinary recursive query shape
/// and therefore keeps the source table's normal materialization contract.
pub fn relation_output_projection_if_present(
    relation: &RelationQuery,
) -> Result<Option<Vec<RelationProjectColumn>>, QueryError> {
    let (base, _, _, _, _) = peel_relation_output_steps(&relation.rel)?;
    if matches!(base, RelationExpr::Gather { .. }) {
        return Ok(None);
    }
    relation_output_projection(relation).map(|(_, columns)| Some(columns))
}
/// Return the projection for every leaf of a retained relation UNION.
///
/// Labels use the same length-prefixed path encoding as executable UNION
/// lowering, so a maintained member can select its exact arm projection.
pub fn relation_union_leaf_projections(
    relation: &RelationQuery,
) -> Result<BTreeMap<String, Vec<RelationProjectColumn>>, QueryError> {
    let mut projections = BTreeMap::new();
    collect_relation_union_leaf_projections(&relation.rel, None, None, &mut projections)?;
    Ok(projections)
}

fn collect_relation_union_leaf_projections(
    expr: &RelationExpr,
    label: Option<&str>,
    inherited: Option<Vec<RelationProjectColumn>>,
    projections: &mut BTreeMap<String, Vec<RelationProjectColumn>>,
) -> Result<(), QueryError> {
    match expr {
        RelationExpr::Union { inputs } => {
            for arm in inputs {
                let arm_label = label.map_or_else(
                    || arm.label.clone(),
                    |prefix| compose_union_arm_path(prefix, &arm.label),
                );
                collect_relation_union_leaf_projections(
                    &arm.input,
                    Some(&arm_label),
                    inherited.clone(),
                    projections,
                )?;
            }
        }
        RelationExpr::Project { input, columns } if relation_expr_contains_union(input) => {
            collect_relation_union_leaf_projections(
                input,
                label,
                inherited.or_else(|| Some(columns.clone())),
                projections,
            )?;
        }
        RelationExpr::Filter { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => {
            collect_relation_union_leaf_projections(input, label, inherited, projections)?;
        }
        RelationExpr::Project { columns, .. } => {
            let label = label.ok_or_else(|| {
                relation_unification_error("UNION leaf projection has no arm label")
            })?;
            if projections.insert(
                label.to_owned(),
                inherited.unwrap_or_else(|| columns.clone()),
            ).is_some() {
                return Err(relation_unification_error(
                    "UNION leaf projection labels must be unique",
                ));
            }
        }
        RelationExpr::TableScan { .. }
        | RelationExpr::Join { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => {
            let label = label.ok_or_else(|| {
                relation_unification_error("UNION leaf has no output projection")
            })?;
            let columns = if let Some(columns) = inherited {
                columns
            } else {
                relation_projection_from_expr(expr)?
                    .map(|(_, columns)| columns)
                    .ok_or_else(|| relation_unification_error("UNION leaf has no output projection"))?
            };
            if projections.insert(label.to_owned(), columns).is_some() {
                return Err(relation_unification_error(
                    "UNION leaf projection labels must be unique",
                ));
            }
        }
    }
    Ok(())
}

fn relation_expr_contains_union(expr: &RelationExpr) -> bool {
    match expr {
        RelationExpr::Union { .. } => true,
        RelationExpr::Project { input, .. }
        | RelationExpr::Filter { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => relation_expr_contains_union(input),
        RelationExpr::Join { left, right, .. } => {
            relation_expr_contains_union(left) || relation_expr_contains_union(right)
        }
        RelationExpr::TableScan { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => false,
    }
}

/// Encode a nested semantic UNION arm path without relying on a user label
/// separator.
pub fn compose_union_arm_path(prefix: &str, label: &str) -> String {
    format!("{}:{prefix}{}:{label}", prefix.len(), label.len())
}

fn relation_projection_from_expr(
    expr: &RelationExpr,
) -> Result<Option<(String, Vec<RelationProjectColumn>)>, QueryError> {
    match expr {
        RelationExpr::Project { input, columns } => {
            let scope = columns.iter().find_map(|column| match &column.expr {
                RelationProjectExpr::Column(column) => column.scope.clone(),
                RelationProjectExpr::RowId(_) => None,
            });
            if let Some(scope) = scope {
                return Ok(Some((scope, columns.clone())));
            }
            relation_projection_from_expr(input)
        }
        RelationExpr::Filter { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => relation_projection_from_expr(input),
        RelationExpr::Union { inputs } => inputs
            .first()
            .map(|arm| relation_projection_from_expr(&arm.input))
            .transpose()
            .map(|value| value.flatten()),
        RelationExpr::TableScan { .. }
        | RelationExpr::Join { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => Ok(None),
    }
}

/// Engine carrier names and namespaces that a *retained* relation projection
/// alias must not take. Retained aliases are lowered as graph fields beside
/// the source row's own carriers (`row_uuid`, `_app_<column>` cells, `$`
/// provenance, `tx_*` version fields, `__`-prefixed engine fields such as the
/// UNION arm and row carriers, and `left.`/`right.` join-side fields), so such
/// an alias would replace that carrier instead of adding a public output.
///
/// A full identity projection is not retained (it is the ordinary row shape)
/// and therefore keeps accepting every schema-permitted column name; see #2997
/// for the general logical-name vs carrier-name ambiguity.
const RESERVED_RELATION_ALIASES: &[&str] = &["row_uuid", "tx_time", "tx_node_id"];
const RESERVED_RELATION_ALIAS_PREFIXES: &[&str] = &[
    "$",
    crate::schema::APP_COLUMN_PREFIX,
    "__",
    "left.",
    "right.",
];

/// Structural alias rules shared by every relation projection.
pub fn validate_relation_projection_aliases(
    columns: &[RelationProjectColumn],
) -> Result<(), QueryError> {
    let mut aliases = BTreeSet::new();
    for column in columns {
        let alias = column.alias.as_str();
        if alias.is_empty() || alias.contains('\0') || !aliases.insert(alias) {
            return Err(relation_unification_error(
                "relation project aliases must be non-empty, NUL-free, and unique",
            ));
        }
    }
    Ok(())
}

/// Reject aliases of a retained projection that would replace an engine
/// carrier.
pub fn reject_reserved_relation_aliases(
    columns: &[RelationProjectColumn],
) -> Result<(), QueryError> {
    for column in columns {
        let alias = column.alias.as_str();
        if RESERVED_RELATION_ALIASES.contains(&alias)
            || RESERVED_RELATION_ALIAS_PREFIXES
                .iter()
                .any(|prefix| alias.starts_with(prefix))
        {
            return Err(QueryError::UnsupportedRelationQuery(format!(
                "relation project alias {alias:?} is a reserved internal name; aliases must not \
                 be row_uuid, tx_time or tx_node_id, or start with \"$\", \"{}\", \"__\", \"left.\" or \"right.\"",
                crate::schema::APP_COLUMN_PREFIX
            )));
        }
    }
    Ok(())
}

/// The order used when materializing a retained public UNION ALL result set.
/// The row-set compiler owns terminal pagination; callers use this only to
/// restore the ordered Vec after identity-keyed result storage.
pub fn relation_union_presentation_order(relation: &RelationQuery) -> Option<Vec<OrderBy>> {
    relation_union_parts(&relation.rel).and_then(|parts| {
        parts.order_by.map(|terms| {
            terms
                .into_iter()
                .map(|term| OrderBy {
                    column: term.column.column,
                    direction: term.direction,
                })
                .collect()
        })
    })
}

/// A retained `UNION ALL` and the terminal operators which must execute over
/// the complete union. Filter and project remain arm-local because they are
/// output-preserving, while order and window operators deliberately remain
/// above the union to retain UNION ALL multiplicity and global page semantics.
#[derive(Clone, Debug)]
pub struct RelationUnionParts {
    #[doc(hidden)]
    pub inputs: Vec<RelationUnionArm>,
    #[doc(hidden)]
    pub order_by: Option<Vec<RelationOrderBy>>,
    #[doc(hidden)]
    pub offset: Option<usize>,
    #[doc(hidden)]
    pub limit: Option<usize>,
}

impl RelationUnionParts {
    fn has_terminal_steps(&self) -> bool {
        self.order_by.is_some() || self.offset.is_some() || self.limit.is_some()
    }
}

/// Split a public union into independently normalizable arms and its global
/// terminal order/window operators. The accepted order is the public builder's
/// `OrderBy` then optional `Offset` then optional `Limit`; other wrapper
/// arrangements are rejected instead of changing their meaning by pushing
/// them into every arm.
pub fn relation_union_parts(expr: &RelationExpr) -> Option<RelationUnionParts> {
    match expr {
        RelationExpr::Union { inputs } => Some(RelationUnionParts {
            inputs: inputs.clone(),
            order_by: None,
            offset: None,
            limit: None,
        }),
        RelationExpr::Filter { input, predicate } => {
            let mut parts = relation_union_parts(input)?;
            if parts.has_terminal_steps() {
                return None;
            }
            parts.inputs = parts
                .inputs
                .into_iter()
                .map(|arm| RelationUnionArm {
                    label: arm.label,
                    input: RelationExpr::Filter {
                        input: Box::new(arm.input),
                        predicate: predicate.clone(),
                    },
                })
                .collect();
            Some(parts)
        }
        RelationExpr::Project { input, columns } => {
            let mut parts = relation_union_parts(input)?;
            if parts.has_terminal_steps() {
                return None;
            }
            parts.inputs = parts
                .inputs
                .into_iter()
                .map(|arm| RelationUnionArm {
                    label: arm.label,
                    input: RelationExpr::Project {
                        input: Box::new(arm.input),
                        columns: columns.clone(),
                    },
                })
                .collect();
            Some(parts)
        }
        RelationExpr::OrderBy { input, terms } => {
            let mut parts = relation_union_parts(input)?;
            if parts.has_terminal_steps() {
                return None;
            }
            parts.order_by = Some(terms.clone());
            Some(parts)
        }
        RelationExpr::Offset { input, offset } => {
            let mut parts = relation_union_parts(input)?;
            if parts.offset.is_some() || parts.limit.is_some() {
                return None;
            }
            parts.offset = Some(*offset);
            Some(parts)
        }
        RelationExpr::Limit { input, limit } => {
            let mut parts = relation_union_parts(input)?;
            if parts.limit.is_some() {
                return None;
            }
            parts.limit = Some(*limit);
            Some(parts)
        }
        RelationExpr::TableScan { .. }
        | RelationExpr::Join { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => None,
    }
}

/// Normalize the canonical public `gather` shape into the ordinary recursive
/// reachability query.  The TypeScript query adapter emits this shape for a
/// same-table forward hop: a seed relation supplies the initial rows and the
/// step table relates each frontier row to its parent through a scalar FK.
///
/// Keeping this conversion here is important: `ReachableVia` already has
/// maintained and one-shot lowering, so relation gathers do not need a second
/// evaluator or subscription implementation.
fn relation_gather_to_query(expr: &RelationExpr) -> Result<Option<Query>, QueryError> {
    let (expr, filters, order_by, offset, limit) = peel_relation_output_steps(expr)?;
    let RelationExpr::Gather {
        seed,
        step,
        frontier_key,
        bound,
        dedupe_key,
    } = expr
    else {
        return Ok(None);
    };

    let shape = recognize_relation_gather(seed, step, frontier_key, bound, dedupe_key)
        .map_err(|message| {
            relation_unification_error(
                message.replace("not supported yet", "not supported in queries yet"),
            )
        })?;
    // A query returns rows of the table the walk visits: the seed hop's
    // target, or else the seed table itself.
    let (seed_table, seed_team_column) = match shape.seed.hop_table {
        Some(hop_table) => (hop_table.to_owned(), shape.seed.team_column.clone()),
        None if shape.seed.team_column == "id" => (shape.seed.table.to_owned(), "id".to_owned()),
        None => {
            return Err(relation_unification_error(
                "Gather seeds projecting an FK without a hop are not supported in queries yet",
            ));
        }
    };
    if shape
        .step
        .output_table
        .is_some_and(|output_table| output_table != seed_table)
    {
        return Err(relation_unification_error(
            "Gather steps returning rows of another table are not supported in queries yet",
        ));
    }
    let seed_filters = relation_gather_predicates_to_query(&shape.seed.filters)?;
    let edge_filters = relation_gather_predicates_to_query(&shape.step.filters)?;
    let (edge_table, edge_member_column, edge_parent_column) = (
        shape.step.edge_table.to_owned(),
        shape.step.member_column.clone(),
        shape.step.parent_column.clone(),
    );

    let mut query = Query::from(seed_table.clone());
    for filter in filters {
        let Some((scope, filter)) = relation_predicate_to_query_predicate(filter)? else {
            continue;
        };
        if !scope.is_empty() && scope != seed_table {
            return Err(relation_unification_error(
                "gather output filters must be scoped to the gathered table",
            ));
        }
        query = query.filter(filter);
    }
    query.reachable.push(ReachableVia {
        // Treat each candidate output row as its own access row.  The
        // reachability closure then acts as a membership filter over that
        // same table, yielding the gather's seed rows and every reached row.
        access_table: seed_table.clone(),
        access_row_column: "id".to_owned(),
        access_team_column: "id".to_owned(),
        access_team_target: JoinTarget::RowId,
        from: Operand::Literal(Value::Uuid(uuid::Uuid::nil())),
        access_filters: Vec::new(),
        edge_table,
        edge_member_column,
        edge_parent_column,
        edge_filters,
        bound: shape.bound.clone(),
        seed: Some(ReachableSeed {
            table: shape.seed.table.to_owned(),
            user_column: None,
            user_claim: None,
            team_column: seed_team_column,
            filters: seed_filters,
        }),
    });

    for order in order_by {
        if order
            .column
            .scope
            .as_deref()
            .is_some_and(|scope| scope != seed_table)
        {
            return Err(relation_unification_error(
                "gather order_by must be scoped to the gathered table",
            ));
        }
        query = query.order_by(order.column.column, order.direction);
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    if offset != 0 {
        query = query.offset(offset);
    }
    Ok(Some(query))
}

fn peel_relation_output_steps(
    expr: &RelationExpr,
) -> Result<
    (
        &RelationExpr,
        Vec<&RelationPredicate>,
        Vec<RelationOrderBy>,
        usize,
        Option<usize>,
    ),
    QueryError,
> {
    let mut filters = Vec::new();
    let mut order_by = Vec::new();
    let mut output_steps = Vec::new();
    let mut current = expr;
    loop {
        if !filters.is_empty()
            && matches!(
                current,
                RelationExpr::Offset { .. } | RelationExpr::Limit { .. }
            )
        {
            return Err(relation_unification_error(
                "gather output filters cannot wrap limit or offset",
            ));
        }
        match current {
            RelationExpr::Filter { input, predicate } => {
                filters.push(predicate);
                current = input;
            }
            RelationExpr::OrderBy { input, terms } => {
                order_by.extend(terms.iter().cloned());
                current = input;
            }
            RelationExpr::Offset {
                input,
                offset: value,
            } => {
                output_steps.push(RelationOutputStep::Offset(*value));
                current = input;
            }
            RelationExpr::Limit {
                input,
                limit: value,
            } => {
                output_steps.push(RelationOutputStep::Limit(*value));
                current = input;
            }
            _ => break,
        }
    }

    let (offset, limit) = output_steps.into_iter().rev().fold(
        (0, None),
        fold_relation_output_step,
    );
    Ok((current, filters, order_by, offset, limit))
}

/// The gather recognizer already checked every column names its one table.
fn relation_gather_predicates_to_query(
    predicates: &[&RelationPredicate],
) -> Result<Vec<Predicate>, QueryError> {
    let mut lowered = Vec::new();
    for predicate in predicates {
        lowered.extend(
            relation_query_conjuncts(predicate)?
                .into_iter()
                .map(|conjunct| conjunct.predicate),
        );
    }
    Ok(lowered)
}

fn relation_unification_error(message: impl Into<String>) -> QueryError {
    QueryError::UnsupportedRelationQuery(message.into())
}

fn relation_path_join_from_output(
    plan: &mut RelationFacadePlan,
    output_scope: &str,
) -> Result<JoinVia, QueryError> {
    let mut path = Vec::<(String, RelationFacadeJoin)>::new();
    let mut current = output_scope.to_owned();
    let mut previous = None::<String>;

    loop {
        let incident = plan
            .joins
            .iter()
            .filter(|join| join.left_scope == current || join.right_scope == current)
            .filter(|join| {
                previous.as_ref().is_none_or(|previous| {
                    join.left_scope != *previous && join.right_scope != *previous
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        match incident.as_slice() {
            [] => break,
            [join] => {
                let next = if join.left_scope == current {
                    join.right_scope.clone()
                } else {
                    join.left_scope.clone()
                };
                path.push((next.clone(), join.clone()));
                previous = Some(current);
                current = next;
            }
            _ => {
                return Err(relation_unification_error(
                    "relation query joins must form one output-rooted path in this slice",
                ));
            }
        }
    }

    if path.len() != plan.joins.len() {
        return Err(relation_unification_error(
            "relation query joins must connect directly to the output scope in this slice",
        ));
    }
    if path.is_empty() {
        return Err(relation_unification_error(
            "relation query join path is empty",
        ));
    }

    build_relation_path_join(plan, output_scope, &path, 0)
}

fn build_relation_path_join(
    plan: &mut RelationFacadePlan,
    left_scope: &str,
    path: &[(String, RelationFacadeJoin)],
    index: usize,
) -> Result<JoinVia, QueryError> {
    let (right_scope, relation_join) = path
        .get(index)
        .ok_or_else(|| relation_unification_error("relation query join path ended unexpectedly"))?;
    let (left_column, right_column) =
        relation_join_columns(relation_join, left_scope, right_scope)?;
    let join_table = plan
        .tables_by_scope
        .get(right_scope)
        .cloned()
        .ok_or_else(|| relation_unification_error("relation query join scope is unknown"))?;
    let mut join = JoinVia {
        table: join_table,
        on_column: right_column,
        target: JoinTarget::Column,
        source_column: (left_column != "id").then_some(left_column),
        source_lookup: None,
        correlated_filters: Vec::new(),
        filters: plan
            .filters_by_scope
            .remove(right_scope)
            .unwrap_or_default(),
        nested_joins: Vec::new(),
    };
    if index + 1 < path.len() {
        join.nested_joins.push(build_relation_path_join(
            plan,
            right_scope,
            path,
            index + 1,
        )?);
    }
    Ok(join)
}

fn relation_join_columns(
    join: &RelationFacadeJoin,
    left_scope: &str,
    right_scope: &str,
) -> Result<(String, String), QueryError> {
    if join.left_scope == left_scope && join.right_scope == right_scope {
        Ok((join.left_column.clone(), join.right_column.clone()))
    } else if join.right_scope == left_scope && join.left_scope == right_scope {
        Ok((join.right_column.clone(), join.left_column.clone()))
    } else {
        Err(relation_unification_error(
            "relation query join path contains a non-adjacent edge",
        ))
    }
}

fn collect_relation_facade(
    expr: &RelationExpr,
    plan: &mut RelationFacadePlan,
) -> Result<(), QueryError> {
    match expr {
        RelationExpr::TableScan { table, alias } => {
            let scope = alias.clone().unwrap_or_else(|| table.clone());
            match plan.tables_by_scope.insert(scope, table.clone()) {
                Some(existing) if existing != *table => Err(relation_unification_error(
                    "relation query reuses an alias for different tables",
                )),
                _ => Ok(()),
            }
        }
        RelationExpr::Filter { input, predicate } => {
            collect_relation_facade(input, plan)?;
            if let Some((scope, predicate)) = relation_predicate_to_query_predicate(predicate)? {
                plan.filters_by_scope
                    .entry(scope)
                    .or_default()
                    .push(predicate);
            }
            Ok(())
        }
        RelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => {
            if *join_kind != RelationJoinKind::Inner {
                return Err(relation_unification_error(
                    "left relation joins are not unified yet",
                ));
            }
            collect_relation_facade(left, plan)?;
            collect_relation_facade(right, plan)?;
            for condition in on {
                let left_scope = relation_scope(&condition.left)?;
                let right_scope = relation_scope(&condition.right)?;
                plan.joins.push(RelationFacadeJoin {
                    left_scope,
                    left_column: condition.left.column.clone(),
                    right_scope,
                    right_column: condition.right.column.clone(),
                });
            }
            Ok(())
        }
        RelationExpr::Project { input, columns } => {
            collect_relation_facade(input, plan)?;
            let mut output_scope = None::<String>;
            validate_relation_projection_aliases(columns)?;
            for column in columns {
                let scope = match &column.expr {
                    RelationProjectExpr::Column(column) => relation_scope(column)?,
                    RelationProjectExpr::RowId(RelationRowIdRef::Current) => continue,
                    RelationProjectExpr::RowId(_) => {
                        return Err(relation_unification_error(
                            "outer/frontier row-id relation projections are not unified yet",
                        ));
                    }
                };
                match &output_scope {
                    Some(existing) if existing != &scope => {
                        return Err(relation_unification_error(
                            "relation query project must select one output scope",
                        ));
                    }
                    Some(_) => {}
                    None => output_scope = Some(scope),
                }
            }
            plan.output_scope = output_scope;
            plan.output_projection = Some(columns.clone());
            Ok(())
        }
        RelationExpr::OrderBy { input, terms } => {
            collect_relation_facade(input, plan)?;
            let output_scope = plan.output_scope.clone().ok_or_else(|| {
                relation_unification_error("relation order_by requires projected output scope")
            })?;
            for term in terms {
                let scope = relation_scope(&term.column)?;
                if scope != output_scope {
                    return Err(relation_unification_error(
                        "relation order_by on non-output scope is not unified yet",
                    ));
                }
                plan.order_by
                    .push((term.column.column.clone(), term.direction));
            }
            Ok(())
        }
        RelationExpr::Offset { input, offset } => {
            collect_relation_facade(input, plan)?;
            (plan.offset, plan.limit) = fold_relation_output_step(
                (plan.offset, plan.limit),
                RelationOutputStep::Offset(*offset),
            );
            Ok(())
        }
        RelationExpr::Limit { input, limit } => {
            collect_relation_facade(input, plan)?;
            (plan.offset, plan.limit) = fold_relation_output_step(
                (plan.offset, plan.limit),
                RelationOutputStep::Limit(*limit),
            );
            Ok(())
        }
        RelationExpr::Union { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => Err(relation_unification_error(
            "union/gather/distinct relation query lowering is not unified yet",
        )),
    }
}

#[doc(hidden)]
pub fn relation_scope(column: &RelationColumnRef) -> Result<String, QueryError> {
    column.scope.clone().ok_or_else(|| {
        relation_unification_error("relation column refs must be scoped for unified lowering")
    })
}

fn relation_predicate_to_query_predicate(
    predicate: &RelationPredicate,
) -> Result<Option<(String, Predicate)>, QueryError> {
    let conjuncts = relation_query_conjuncts(predicate)?;
    // A query filter lands on one source, so every column must name the same one.
    let mut scope = None::<String>;
    let mut predicates = Vec::new();
    for conjunct in conjuncts {
        for column in &conjunct.columns {
            let column_scope = relation_scope(column)?;
            match &scope {
                Some(existing) if *existing != column_scope => {
                    return Err(relation_unification_error(
                        "Relation predicates across scopes are not supported in queries yet",
                    ));
                }
                Some(_) => {}
                None => scope = Some(column_scope),
            }
        }
        predicates.push(conjunct.predicate);
    }
    Ok(relation_conjunction(predicates).map(|predicate| (scope.unwrap_or_default(), predicate)))
}

fn relation_query_conjuncts(
    predicate: &RelationPredicate,
) -> Result<Vec<RelationConjunct<'_>>, QueryError> {
    lower_relation_conjuncts(predicate, &mut |value| match relation_value_to_operand(value) {
        Ok(operand) => Ok(RelationOperand::Operand(operand)),
        Err(QueryError::UnsupportedRelationQuery(message)) => Err(message),
        Err(error) => Err(error.to_string()),
    })
    .map_err(|message| {
        relation_unification_error(message.replace("not supported yet", "not supported in queries yet"))
    })
}

fn relation_conjunction(mut predicates: Vec<Predicate>) -> Option<Predicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Predicate::All(predicates)),
    }
}

fn relation_value_to_operand(value: &RelationValueRef) -> Result<Operand, QueryError> {
    match value {
        RelationValueRef::Literal(value) => {
            Ok(Operand::Literal(
                relation_literal_to_record_value(value).map_err(relation_unification_error)?,
            ))
        }
        RelationValueRef::Param(name) => Ok(Operand::Param(name.clone())),
        RelationValueRef::SessionRef(path) => {
            if let Some(name) = crate::query::author_claim_path_key(path) {
                return Ok(Operand::Claim(name));
            }
            match path.as_slice() {
                [name] => Ok(Operand::Claim(name.clone())),
                [claims, name] if claims == "claims" => {
                    Ok(Operand::Claim(provider_claim_operand_key(name)))
                }
                _ => Err(relation_unification_error(
                    "session refs support session.user and session.claims[\"name\"]",
                )),
            }
        }
        RelationValueRef::OuterColumn(_)
        | RelationValueRef::FrontierColumn(_)
        | RelationValueRef::RowId(_) => Err(relation_unification_error(
            "outer/frontier/row-id relation predicate operands are not unified yet",
        )),
    }
}

