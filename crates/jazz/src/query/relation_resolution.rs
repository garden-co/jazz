#[derive(Default)]
struct RelationFacadePlan {
    tables_by_scope: BTreeMap<String, String>,
    filters_by_scope: BTreeMap<String, Vec<Predicate>>,
    joins: Vec<RelationFacadeJoin>,
    output_scope: Option<String>,
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

/// Normalize the currently-supported relation facade subset into the ordinary
/// query shape used by one-shot and maintained execution.
pub(crate) fn relation_query_to_query(query: &RelationQuery) -> Result<Query, QueryError> {
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
    let mut query = Query::from(output_table);

    for filter in plan
        .filters_by_scope
        .remove(&output_scope)
        .unwrap_or_default()
    {
        query = query.filter(filter);
    }

    if !plan.joins.is_empty() {
        let join = relation_path_join_from_output(&mut plan, &output_scope)?;
        query.joins.push(join);
    }

    if !plan.filters_by_scope.is_empty() {
        return Err(relation_unification_error(
            "relation query filters on non-adjacent scopes are not unified yet",
        ));
    }

    for (column, direction) in plan.order_by {
        query = query.order_by(column, direction);
    }
    if let Some(limit) = plan.limit {
        query = query.limit(limit);
    }
    if plan.offset != 0 {
        query = query.offset(plan.offset);
    }
    Ok(query)
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

    if !matches!(
        frontier_key,
        RelationKeyRef::RowId(RelationRowIdRef::Current)
    ) || !dedupe_key
        .as_slice()
        .eq(&[RelationKeyRef::RowId(RelationRowIdRef::Current)])
    {
        return Err(relation_unification_error(
            "gather requires current-row frontier and dedupe keys",
        ));
    }

    let (seed_table, seed_filters) = relation_gather_seed(seed)?;
    let (edge_table, edge_member_column, edge_parent_column, edge_filters) =
        relation_gather_step(step, &seed_table)?;

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
        bound: bound.clone(),
        seed: Some(ReachableSeed {
            table: seed_table.clone(),
            user_column: None,
            user_claim: None,
            team_column: "id".to_owned(),
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
    let mut offset = 0;
    let mut limit = None;
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
                offset = *value;
                current = input;
            }
            RelationExpr::Limit {
                input,
                limit: value,
            } => {
                limit = Some(*value);
                current = input;
            }
            _ => break,
        }
    }

    Ok((current, filters, order_by, offset, limit))
}

fn relation_gather_seed(seed: &RelationExpr) -> Result<(String, Vec<Predicate>), QueryError> {
    let mut filters = Vec::new();
    let mut current = seed;
    while let RelationExpr::Filter { input, predicate } = current {
        let Some((scope, predicate)) = relation_predicate_to_query_predicate(predicate)? else {
            current = input;
            continue;
        };
        let RelationExpr::TableScan { table, alias } = input.as_ref() else {
            return Err(relation_unification_error(
                "gather seed filters must be directly over one table scan",
            ));
        };
        let expected_scope = alias.as_deref().unwrap_or(table);
        if scope != expected_scope {
            return Err(relation_unification_error(
                "gather seed filters must be scoped to the seed table",
            ));
        }
        filters.push(predicate);
        current = input;
    }
    let RelationExpr::TableScan { table, alias } = current else {
        return Err(relation_unification_error(
            "gather seed must be a table scan with optional filters",
        ));
    };
    if alias.is_some() {
        return Err(relation_unification_error(
            "gather seed aliases are not unified yet",
        ));
    }
    Ok((table.clone(), filters))
}

fn relation_gather_step(
    step: &RelationExpr,
    seed_table: &str,
) -> Result<(String, String, String, Vec<Predicate>), QueryError> {
    let RelationExpr::Project { input, columns } = step else {
        return Err(relation_unification_error(
            "gather step must project its forward-hop target",
        ));
    };
    let RelationExpr::Join {
        left,
        right,
        on,
        join_kind: RelationJoinKind::Inner,
    } = input.as_ref()
    else {
        return Err(relation_unification_error(
            "gather step must be an inner forward-hop join",
        ));
    };
    let RelationExpr::TableScan {
        table: edge_table,
        alias: edge_alias,
    } = relation_gather_step_scan(left)?
    else {
        unreachable!("relation_gather_step_scan only returns table scans")
    };
    let edge_scope = edge_alias.as_deref().unwrap_or(edge_table);
    let RelationExpr::TableScan {
        table: target_table,
        alias: Some(target_alias),
    } = right.as_ref()
    else {
        return Err(relation_unification_error(
            "gather step target must use a scoped table scan",
        ));
    };
    if target_table != seed_table {
        return Err(relation_unification_error(
            "gather step must return rows from the seed table",
        ));
    }
    if on.len() != 1 {
        return Err(relation_unification_error(
            "gather step requires exactly one forward-hop join condition",
        ));
    }
    let condition = &on[0];
    if condition.left.scope.as_deref() != Some(edge_scope)
        || condition.right.scope.as_deref() != Some(target_alias)
        || condition.right.column != "id"
    {
        return Err(relation_unification_error(
            "gather step join must connect its table FK to the target row id",
        ));
    }
    if columns.iter().any(|column| match &column.expr {
        RelationProjectExpr::Column(column) => column.scope.as_deref() != Some(target_alias),
        RelationProjectExpr::RowId(RelationRowIdRef::Current) => true,
        RelationProjectExpr::RowId(_) => true,
    }) {
        return Err(relation_unification_error(
            "gather step must project only its forward-hop target",
        ));
    }

    let filters = relation_gather_step_filters(left)?;
    let frontier_filter = filters.iter().find_map(|predicate| match predicate {
        RelationPredicate::Cmp {
            left,
            op: RelationCmpOp::Eq,
            right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
        } if left.scope.as_deref() == Some(edge_scope) => Some(left.column.clone()),
        _ => None,
    });
    let Some(edge_member_column) = frontier_filter else {
        return Err(relation_unification_error(
            "gather step must compare one edge column to the frontier row id",
        ));
    };
    let edge_filters = filters
        .into_iter()
        .filter(|predicate| {
            !matches!(
                predicate,
                RelationPredicate::Cmp {
                    left: RelationColumnRef { scope: Some(scope), .. },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                } if scope == edge_scope
            )
        })
        .filter_map(|predicate| relation_predicate_to_query_predicate(&predicate).transpose())
        .map(|result| {
            result.and_then(|(scope, predicate)| {
                if scope != edge_scope {
                    return Err(relation_unification_error(
                        "gather step filters must be scoped to the edge table",
                    ));
                }
                Ok(predicate)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        edge_table.clone(),
        edge_member_column,
        condition.left.column.clone(),
        edge_filters,
    ))
}

fn relation_gather_step_scan(input: &RelationExpr) -> Result<&RelationExpr, QueryError> {
    let mut current = input;
    while let RelationExpr::Filter { input, .. } = current {
        current = input;
    }
    if matches!(current, RelationExpr::TableScan { .. }) {
        Ok(current)
    } else {
        Err(relation_unification_error(
            "gather step filters must be directly over one table scan",
        ))
    }
}

fn relation_gather_step_filters(
    input: &RelationExpr,
) -> Result<Vec<RelationPredicate>, QueryError> {
    let mut filters = Vec::new();
    let mut current = input;
    while let RelationExpr::Filter { input, predicate } = current {
        relation_gather_step_predicates(predicate, &mut filters)?;
        current = input;
    }
    Ok(filters)
}

fn relation_gather_step_predicates(
    predicate: &RelationPredicate,
    filters: &mut Vec<RelationPredicate>,
) -> Result<(), QueryError> {
    match predicate {
        RelationPredicate::And(predicates) => {
            for predicate in predicates {
                relation_gather_step_predicates(predicate, filters)?;
            }
            Ok(())
        }
        RelationPredicate::Cmp { .. }
        | RelationPredicate::IsNull { .. }
        | RelationPredicate::IsNotNull { .. }
        | RelationPredicate::In { .. }
        | RelationPredicate::Contains { .. }
        | RelationPredicate::EnumMatch { .. }
        | RelationPredicate::Or(_)
        | RelationPredicate::Not(_) => {
            filters.push(predicate.clone());
            Ok(())
        }
        RelationPredicate::True => Ok(()),
        RelationPredicate::False => {
            filters.push(predicate.clone());
            Ok(())
        }
    }
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
            plan.offset = *offset;
            Ok(())
        }
        RelationExpr::Limit { input, limit } => {
            collect_relation_facade(input, plan)?;
            plan.limit = Some(*limit);
            Ok(())
        }
        RelationExpr::Union { .. }
        | RelationExpr::Gather { .. }
        | RelationExpr::Distinct { .. } => Err(relation_unification_error(
            "union/gather/distinct relation query lowering is not unified yet",
        )),
    }
}

fn relation_scope(column: &RelationColumnRef) -> Result<String, QueryError> {
    column.scope.clone().ok_or_else(|| {
        relation_unification_error("relation column refs must be scoped for unified lowering")
    })
}

fn relation_predicate_to_query_predicate(
    predicate: &RelationPredicate,
) -> Result<Option<(String, Predicate)>, QueryError> {
    match predicate {
        RelationPredicate::Cmp { left, op, right } => {
            let scope = relation_scope(left)?;
            let predicate = match op {
                RelationCmpOp::Eq => Predicate::Eq(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
                RelationCmpOp::Ne => Predicate::Ne(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
                RelationCmpOp::Lt => Predicate::Lt(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
                RelationCmpOp::Le => Predicate::Lte(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
                RelationCmpOp::Gt => Predicate::Gt(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
                RelationCmpOp::Ge => Predicate::Gte(
                    Operand::Column(left.column.clone()),
                    relation_value_to_operand(right)?,
                ),
            };
            Ok(Some((scope, predicate)))
        }
        RelationPredicate::IsNull { column } => Ok(Some((
            relation_scope(column)?,
            Predicate::IsNull(Operand::Column(column.column.clone())),
        ))),
        RelationPredicate::IsNotNull { column } => Ok(Some((
            relation_scope(column)?,
            Predicate::Not(Box::new(Predicate::IsNull(Operand::Column(
                column.column.clone(),
            )))),
        ))),
        RelationPredicate::In { left, values } => Ok(Some((
            relation_scope(left)?,
            Predicate::In(
                Operand::Column(left.column.clone()),
                values
                    .iter()
                    .map(relation_value_to_operand)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        ))),
        RelationPredicate::Contains { left, right } => Ok(Some((
            relation_scope(left)?,
            Predicate::Contains(
                Operand::Column(left.column.clone()),
                relation_value_to_operand(right)?,
            ),
        ))),
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => Ok(Some((
            relation_scope(column)?,
            Predicate::EnumMatch {
                column: column.column.clone(),
                case: case.clone(),
                payload: Box::new(relation_payload_predicate_to_predicate(payload)?),
            },
        ))),
        RelationPredicate::And(predicates) => relation_predicate_list(predicates, true),
        RelationPredicate::Or(predicates) => relation_predicate_list(predicates, false),
        RelationPredicate::Not(predicate) => {
            let Some((scope, predicate)) = relation_predicate_to_query_predicate(predicate)? else {
                return Ok(Some((String::new(), Predicate::Any(Vec::new()))));
            };
            if is_always_false(&predicate) {
                return Ok(None);
            }
            Ok(Some((scope, Predicate::Not(Box::new(predicate)))))
        }
        RelationPredicate::True => Ok(None),
        RelationPredicate::False => Ok(Some((String::new(), Predicate::Any(Vec::new())))),
    }
}

fn relation_payload_predicate_to_predicate(
    predicate: &RelationPredicate,
) -> Result<Predicate, QueryError> {
    let unscoped_column = |column: &RelationColumnRef| {
        if column.scope.is_some() {
            return Err(relation_unification_error(
                "payload enum predicate fields must be unscoped",
            ));
        }
        Ok(Operand::Column(column.column.clone()))
    };
    match predicate {
        RelationPredicate::Cmp { left, op, right } => {
            let left = unscoped_column(left)?;
            let right = relation_value_to_operand(right)?;
            Ok(match op {
                RelationCmpOp::Eq => Predicate::Eq(left, right),
                RelationCmpOp::Ne => Predicate::Ne(left, right),
                RelationCmpOp::Lt => Predicate::Lt(left, right),
                RelationCmpOp::Le => Predicate::Lte(left, right),
                RelationCmpOp::Gt => Predicate::Gt(left, right),
                RelationCmpOp::Ge => Predicate::Gte(left, right),
            })
        }
        RelationPredicate::IsNull { column } => Ok(Predicate::IsNull(unscoped_column(column)?)),
        RelationPredicate::IsNotNull { column } => Ok(Predicate::Not(Box::new(Predicate::IsNull(
            unscoped_column(column)?,
        )))),
        RelationPredicate::In { left, values } => Ok(Predicate::In(
            unscoped_column(left)?,
            values
                .iter()
                .map(relation_value_to_operand)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        RelationPredicate::Contains { left, right } => Ok(Predicate::Contains(
            unscoped_column(left)?,
            relation_value_to_operand(right)?,
        )),
        RelationPredicate::And(children) => Ok(Predicate::All(
            children
                .iter()
                .map(relation_payload_predicate_to_predicate)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        RelationPredicate::Or(children) => Ok(Predicate::Any(
            children
                .iter()
                .map(relation_payload_predicate_to_predicate)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        RelationPredicate::Not(child) => Ok(Predicate::Not(Box::new(
            relation_payload_predicate_to_predicate(child)?,
        ))),
        RelationPredicate::True => Ok(Predicate::All(Vec::new())),
        RelationPredicate::False => Ok(Predicate::Any(Vec::new())),
        RelationPredicate::EnumMatch { .. } => Err(relation_unification_error(
            "nested payload enum matches are not supported",
        )),
    }
}

fn relation_predicate_list(
    predicates: &[RelationPredicate],
    is_and: bool,
) -> Result<Option<(String, Predicate)>, QueryError> {
    let mut scope = None::<String>;
    let mut items = Vec::new();
    for predicate in predicates {
        let Some((predicate_scope, predicate)) = relation_predicate_to_query_predicate(predicate)?
        else {
            if is_and {
                continue;
            }
            return Ok(None);
        };
        if is_always_false(&predicate) {
            if is_and {
                return Ok(Some((String::new(), Predicate::Any(Vec::new()))));
            }
            continue;
        }
        if predicate_scope.is_empty() {
            items.push(predicate);
            continue;
        }
        match &scope {
            Some(existing) if existing != &predicate_scope => {
                return Err(relation_unification_error(
                    "relation predicate composition across scopes is not unified yet",
                ));
            }
            Some(_) => {}
            None => scope = Some(predicate_scope),
        }
        items.push(predicate);
    }
    let Some(scope) = scope else {
        return if is_and {
            Ok(None)
        } else {
            Ok(Some((String::new(), Predicate::Any(Vec::new()))))
        };
    };
    let predicate = if is_and {
        Predicate::All(items)
    } else {
        Predicate::Any(items)
    };
    Ok(Some((scope, predicate)))
}

fn is_always_false(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::Any(predicates) if predicates.is_empty())
}

fn relation_value_to_operand(value: &RelationValueRef) -> Result<Operand, QueryError> {
    match value {
        RelationValueRef::Literal(value) => {
            Ok(Operand::Literal(json_value_to_record_value(value)?))
        }
        RelationValueRef::Param(name) => Ok(Operand::Param(name.clone())),
        RelationValueRef::SessionRef(path) => {
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

fn json_value_to_record_value(value: &serde_json::Value) -> Result<Value, QueryError> {
    match value {
        serde_json::Value::Null => Ok(Value::Nullable(None)),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Ok(Value::U64(value))
            } else if let Some(value) = value.as_i64() {
                Ok(Value::I64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::F64(value))
            } else {
                Err(relation_unification_error("relation literal is not finite"))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(json_value_to_record_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        serde_json::Value::Object(map) => runtime_value_object_to_record_value(map),
    }
}

fn runtime_value_object_to_record_value(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<Value, QueryError> {
    let Some(serde_json::Value::String(value_type)) = map.get("type") else {
        return Err(relation_unification_error(
            "object relation literals must use the runtime value envelope",
        ));
    };
    let value = map.get("value");
    match value_type.as_str() {
        "Null" => Ok(Value::Nullable(None)),
        "Boolean" => Ok(Value::Bool(runtime_bool_value(value)?)),
        "Text" => Ok(Value::String(runtime_string_value(value)?.to_owned())),
        "Uuid" => uuid::Uuid::parse_str(runtime_string_value(value)?)
            .map(Value::Uuid)
            .map_err(|_| relation_unification_error("invalid Uuid relation literal")),
        "Bytea" => Ok(Value::Bytes(runtime_bytea_value(value)?)),
        "Integer" => Ok(Value::I32(runtime_i32_value(value)?)),
        "Timestamp" => Ok(Value::U64(runtime_u64_value(value)?)),
        "BigInt" => Ok(Value::I64(runtime_i64_value(value)?)),
        "Double" => Ok(Value::F64(runtime_f64_value(value)?)),
        "Array" => {
            let Some(serde_json::Value::Array(values)) = value else {
                return Err(relation_unification_error(
                    "Array relation literal requires an array value",
                ));
            };
            Ok(Value::Array(
                values
                    .iter()
                    .map(json_value_to_record_value)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        _ => Err(relation_unification_error(format!(
            "unsupported runtime relation literal type {value_type}"
        ))),
    }
}

fn runtime_bool_value(value: Option<&serde_json::Value>) -> Result<bool, QueryError> {
    match value {
        Some(serde_json::Value::Bool(value)) => Ok(*value),
        _ => Err(relation_unification_error(
            "Boolean relation literal requires a boolean value",
        )),
    }
}

fn runtime_string_value(value: Option<&serde_json::Value>) -> Result<&str, QueryError> {
    match value {
        Some(serde_json::Value::String(value)) => Ok(value),
        _ => Err(relation_unification_error(
            "string relation literal requires a string value",
        )),
    }
}

fn runtime_bytea_value(value: Option<&serde_json::Value>) -> Result<Vec<u8>, QueryError> {
    let Some(serde_json::Value::Array(values)) = value else {
        return Err(relation_unification_error(
            "Bytea relation literal requires an array value",
        ));
    };
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| {
                    relation_unification_error("Bytea relation literal values must be bytes")
                })
        })
        .collect()
}

fn runtime_u64_value(value: Option<&serde_json::Value>) -> Result<u64, QueryError> {
    match value {
        Some(serde_json::Value::Number(value)) => value.as_u64().ok_or_else(|| {
            relation_unification_error("integer relation literal must be non-negative")
        }),
        _ => Err(relation_unification_error(
            "integer relation literal requires a numeric value",
        )),
    }
}

fn runtime_i32_value(value: Option<&serde_json::Value>) -> Result<i32, QueryError> {
    match value {
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| {
                relation_unification_error(
                    "Integer relation literal requires a signed 32-bit integer",
                )
            }),
        _ => Err(relation_unification_error(
            "Integer relation literal requires an integer value",
        )),
    }
}

fn runtime_i64_value(value: Option<&serde_json::Value>) -> Result<i64, QueryError> {
    match value {
        Some(serde_json::Value::Number(value)) => value.as_i64().ok_or_else(|| {
            relation_unification_error("BigInt relation literal requires a signed 64-bit integer")
        }),
        _ => Err(relation_unification_error(
            "BigInt relation literal requires an integer value",
        )),
    }
}

fn runtime_f64_value(value: Option<&serde_json::Value>) -> Result<f64, QueryError> {
    match value {
        Some(serde_json::Value::Number(value)) => value
            .as_f64()
            .ok_or_else(|| relation_unification_error("double relation literal must be finite")),
        _ => Err(relation_unification_error(
            "double relation literal requires a numeric value",
        )),
    }
}
