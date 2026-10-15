/// One relation `Gather`, recognized the same way for every front end.
///
/// Queries (`relation_query_to_query`) and relation-backed policies
/// (`PolicyExpr::ExistsRel`) both lower a gather to one seeded reachability
/// walk. They share this recognizer so a gather shape means the same thing in
/// both, or is rejected by both; each front end then only lowers the
/// predicates and decides where the walk's rows go.
pub(crate) struct RelationGatherShape<'a> {
    pub(crate) seed: RelationGatherSeed<'a>,
    pub(crate) step: RelationGatherStep<'a>,
    pub(crate) bound: &'a RecursionBound,
}

/// The rows a gather starts from.
pub(crate) struct RelationGatherSeed<'a> {
    /// Table whose filtered rows start the walk.
    pub(crate) table: &'a str,
    /// Filters over `table`; every column is the seed table's own.
    pub(crate) filters: Vec<&'a RelationPredicate>,
    /// Seed column holding each starting row id: `id`, or the FK that a seed
    /// hop or projection follows.
    pub(crate) team_column: String,
    /// The table a seed hop joins to, which is the table the walk visits.
    pub(crate) hop_table: Option<&'a str>,
}

/// One recursive hop: from a frontier row, along an edge row, to its parent.
pub(crate) struct RelationGatherStep<'a> {
    pub(crate) edge_table: &'a str,
    /// Edge column compared to the frontier row id.
    pub(crate) member_column: String,
    /// Edge column naming the next frontier row id.
    pub(crate) parent_column: String,
    /// Remaining edge filters, with top-level conjunctions flattened.
    pub(crate) filters: Vec<&'a RelationPredicate>,
    /// The table a joined step returns rows from. A scalar step carries only
    /// the parent key, so its table is whatever that FK references.
    pub(crate) output_table: Option<&'a str>,
}

pub(crate) fn recognize_relation_gather<'a>(
    seed: &'a RelationExpr,
    step: &'a RelationExpr,
    frontier_key: &RelationKeyRef,
    bound: &'a RecursionBound,
    dedupe_key: &[RelationKeyRef],
) -> Result<RelationGatherShape<'a>, String> {
    let current_row = RelationKeyRef::RowId(RelationRowIdRef::Current);
    if *frontier_key != current_row || dedupe_key != std::slice::from_ref(&current_row) {
        return Err("Gather keyed on anything but the current row id is not supported yet".into());
    }
    Ok(RelationGatherShape {
        seed: recognize_relation_gather_seed(seed)?,
        step: recognize_relation_gather_step(step)?,
        bound,
    })
}

fn recognize_relation_gather_seed(seed: &RelationExpr) -> Result<RelationGatherSeed<'_>, String> {
    let RelationExpr::Project { input, columns } = seed else {
        let (table, _, filters) = relation_gather_filtered_scan(seed, "Gather seeds")?;
        return Ok(RelationGatherSeed {
            table,
            filters,
            team_column: "id".to_owned(),
            hop_table: None,
        });
    };
    let Some(projected) = columns.iter().find(|column| column.alias == "id") else {
        return Err("Gather seed projections without an id column are not supported yet".into());
    };
    let RelationProjectExpr::Column(projected) = &projected.expr else {
        return Err("Gather seed ids projected from a row id are not supported yet".into());
    };

    let mut above_hop = Vec::new();
    let mut current = input.as_ref();
    while let RelationExpr::Filter { input, predicate } = current {
        relation_gather_conjuncts(predicate, &mut above_hop);
        current = input;
    }
    let RelationExpr::Join {
        left,
        right,
        on,
        join_kind,
    } = current
    else {
        // A projection straight off the seed table follows one of its FKs.
        let (table, scope, filters) = relation_gather_filtered_scan(input, "Gather seeds")?;
        if !relation_gather_in_scope(projected, scope) {
            return Err("Gather seed ids projected from another table are not supported yet".into());
        }
        return Ok(RelationGatherSeed {
            table,
            filters,
            team_column: projected.column.clone(),
            hop_table: None,
        });
    };

    // A seed hop names the visited row only when it is one inner equality
    // from a seed-table FK to that row's id, and the projected id is that row's.
    let (table, seed_scope, mut filters) = relation_gather_filtered_scan(left, "Gather seeds")?;
    let RelationExpr::TableScan {
        table: hop_table,
        alias: hop_alias,
    } = right.as_ref()
    else {
        return Err("Gather seed hops to anything but a table are not supported yet".into());
    };
    let hop_scope = hop_alias.as_deref().unwrap_or(hop_table);
    if hop_scope == seed_scope {
        return Err("Gather seed hops without distinct scopes are not supported yet".into());
    }
    let [join] = on.as_slice() else {
        return Err("Gather seed hops without exactly one join equality are not supported yet".into());
    };
    if *join_kind != RelationJoinKind::Inner
        || join.right.column != "id"
        || !relation_gather_in_scope(&join.right, hop_scope)
        || !relation_gather_in_scope(&join.left, seed_scope)
    {
        return Err(
            "Gather seed hops other than an inner join from a seed FK to the target id are not supported yet"
                .into(),
        );
    }
    // After the join an unscoped `id` could name either side's row.
    if projected.column != "id" || projected.scope.as_deref() != Some(hop_scope) {
        return Err("Gather seed ids other than the hop target row id are not supported yet".into());
    }
    // Filters above the hop are applied to the seed rows, so they must name them.
    if !relation_gather_predicates_in_scope(&above_hop, seed_scope) {
        return Err("Gather seed filters on the hop target are not supported yet".into());
    }
    filters.extend(above_hop);
    Ok(RelationGatherSeed {
        table,
        filters,
        team_column: join.left.column.clone(),
        hop_table: Some(hop_table),
    })
}

fn recognize_relation_gather_step(step: &RelationExpr) -> Result<RelationGatherStep<'_>, String> {
    let RelationExpr::Project { input, columns } = step else {
        return Err("Gather steps without a projection are not supported yet".into());
    };

    // The TypeScript query builder hops the edge FK back to the parent row and
    // returns that row; the policy builder carries only the parent key. Both
    // walk the same edge.
    let (edge_rel, output, parent_column) = if let RelationExpr::Join {
        left,
        right,
        on,
        join_kind,
    } = input.as_ref()
    {
        let RelationExpr::TableScan {
            table: output_table,
            alias: output_alias,
        } = right.as_ref()
        else {
            return Err("Gather steps joining anything but a table are not supported yet".into());
        };
        let output_scope = output_alias.as_deref().unwrap_or(output_table);
        let [join] = on.as_slice() else {
            return Err(
                "Gather steps without exactly one join equality are not supported yet".into(),
            );
        };
        // The step names the parent row only when it joins that row's id.
        if *join_kind != RelationJoinKind::Inner
            || join.right.column != "id"
            || !relation_gather_in_scope(&join.right, output_scope)
        {
            return Err(
                "Gather steps other than an inner join from an edge FK to the output id are not supported yet"
                    .into(),
            );
        }
        // The next frontier is the output row's id. A projection may carry
        // output columns, but an `id` alias must still be that row's id or the
        // step walks somewhere else.
        // Both sides of the join are in scope here, so an unscoped column
        // could name the edge row instead; only the output's own are allowed.
        for column in columns {
            let RelationProjectExpr::Column(projected) = &column.expr else {
                return Err("Gather steps projecting a row id are not supported yet".into());
            };
            if projected.scope.as_deref() != Some(output_scope)
                || (column.alias == "id" && projected.column != "id")
            {
                return Err(
                    "Gather steps projecting anything but the output row are not supported yet"
                        .into(),
                );
            }
        }
        (
            left.as_ref(),
            Some((output_table.as_str(), output_scope)),
            &join.left,
        )
    } else {
        let Some(projected) = columns.iter().find(|column| column.alias == "id") else {
            return Err("Gather steps without a joined output or an id key are not supported yet".into());
        };
        let RelationProjectExpr::Column(projected) = &projected.expr else {
            return Err("Gather steps projecting a row id are not supported yet".into());
        };
        (input.as_ref(), None, projected)
    };

    let (edge_table, edge_scope, filters) = relation_gather_filtered_scan(edge_rel, "Gather steps")?;
    if output.is_some_and(|(_, output_scope)| output_scope == edge_scope) {
        return Err(
            "Gather steps joining the edge table without distinct scopes are not supported yet"
                .into(),
        );
    }
    if !relation_gather_in_scope(parent_column, edge_scope) {
        return Err("Gather step keys from outside the edge table are not supported yet".into());
    }

    let is_frontier = |predicate: &RelationPredicate| {
        matches!(
            predicate,
            RelationPredicate::Cmp {
                left,
                op: RelationCmpOp::Eq,
                right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
            } if relation_gather_in_scope(left, edge_scope)
        )
    };
    let Some(frontier) = filters.iter().copied().find(|predicate| is_frontier(predicate)) else {
        return Err("Gather steps without one edge column equal to the frontier row id are not supported yet".into());
    };
    let RelationPredicate::Cmp {
        left: member_column,
        ..
    } = frontier
    else {
        unreachable!("is_frontier only matches comparisons");
    };
    let filters = filters
        .into_iter()
        .filter(|predicate| !std::ptr::eq(*predicate, frontier))
        .collect::<Vec<_>>();
    if filters.iter().any(|predicate| relation_predicate_reads_frontier(predicate)) {
        return Err("Gather steps reading the frontier more than once are not supported yet".into());
    }
    Ok(RelationGatherStep {
        edge_table,
        member_column: member_column.column.clone(),
        parent_column: parent_column.column.clone(),
        filters,
        output_table: output.map(|(table, _)| table),
    })
}

/// Filters directly over one table scan: the table, its scope, and the
/// filters' conjuncts, every one of which must name only that scope.
fn relation_gather_filtered_scan<'a>(
    rel: &'a RelationExpr,
    what: &str,
) -> Result<(&'a str, &'a str, Vec<&'a RelationPredicate>), String> {
    let mut filters = Vec::new();
    let mut current = rel;
    while let RelationExpr::Filter { input, predicate } = current {
        relation_gather_conjuncts(predicate, &mut filters);
        current = input;
    }
    let RelationExpr::TableScan { table, alias } = current else {
        return Err(format!(
            "{what} other than a filtered table scan are not supported yet"
        ));
    };
    let scope = alias.as_deref().unwrap_or(table);
    if !relation_gather_predicates_in_scope(&filters, scope) {
        return Err(format!(
            "{what} filtering on another table are not supported yet"
        ));
    }
    Ok((table, scope, filters))
}

fn relation_gather_conjuncts<'a>(
    predicate: &'a RelationPredicate,
    conjuncts: &mut Vec<&'a RelationPredicate>,
) {
    match predicate {
        RelationPredicate::And(children) => {
            for child in children {
                relation_gather_conjuncts(child, conjuncts);
            }
        }
        RelationPredicate::True => {}
        predicate => conjuncts.push(predicate),
    }
}

fn relation_gather_in_scope(column: &RelationColumnRef, scope: &str) -> bool {
    column.scope.as_deref().is_none_or(|column_scope| column_scope == scope)
}

fn relation_gather_predicates_in_scope(predicates: &[&RelationPredicate], scope: &str) -> bool {
    fn in_scope(predicate: &RelationPredicate, scope: &str) -> bool {
        match predicate {
            RelationPredicate::And(children) | RelationPredicate::Or(children) => {
                children.iter().all(|child| in_scope(child, scope))
            }
            RelationPredicate::Not(child) => in_scope(child, scope),
            RelationPredicate::Cmp { left, .. }
            | RelationPredicate::Contains { left, .. }
            | RelationPredicate::In { left, .. } => relation_gather_in_scope(left, scope),
            // Enum payload fields are relative to the matched record.
            RelationPredicate::IsNull { column }
            | RelationPredicate::IsNotNull { column }
            | RelationPredicate::EnumMatch { column, .. } => relation_gather_in_scope(column, scope),
            RelationPredicate::True | RelationPredicate::False => true,
        }
    }
    predicates.iter().all(|predicate| in_scope(predicate, scope))
}

fn relation_predicate_reads_frontier(predicate: &RelationPredicate) -> bool {
    let reads = |value: &RelationValueRef| {
        matches!(
            value,
            RelationValueRef::RowId(RelationRowIdRef::Frontier) | RelationValueRef::FrontierColumn(_)
        )
    };
    match predicate {
        RelationPredicate::And(children) | RelationPredicate::Or(children) => {
            children.iter().any(relation_predicate_reads_frontier)
        }
        RelationPredicate::Not(child) => relation_predicate_reads_frontier(child),
        RelationPredicate::EnumMatch { payload, .. } => relation_predicate_reads_frontier(payload),
        RelationPredicate::Cmp { right, .. } | RelationPredicate::Contains { right, .. } => {
            reads(right)
        }
        RelationPredicate::In { values, .. } => values.iter().any(reads),
        RelationPredicate::IsNull { .. }
        | RelationPredicate::IsNotNull { .. }
        | RelationPredicate::True
        | RelationPredicate::False => false,
    }
}
