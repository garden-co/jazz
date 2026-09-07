fn normalize_query(query: &Query) -> Query {
    let mut query = query.clone();
    query.filters.sort_by_key(canonical_predicate_key);
    for join in &mut query.joins {
        join.filters.sort_by_key(canonical_predicate_key);
        normalize_join(join);
    }
    query.joins.sort_by_key(canonical_join_key);
    for branch in &mut query.policy_branches {
        branch.filters.sort_by_key(canonical_predicate_key);
        for join in &mut branch.joins {
            join.filters.sort_by_key(canonical_predicate_key);
            normalize_join(join);
        }
        branch.joins.sort_by_key(canonical_join_key);
        for reachable in &mut branch.reachable {
            reachable
                .access_filters
                .sort_by_key(canonical_predicate_key);
            reachable.edge_filters.sort_by_key(canonical_predicate_key);
            if let Some(seed) = &mut reachable.seed {
                seed.filters.sort_by_key(canonical_predicate_key);
            }
        }
        branch.reachable.sort_by_key(canonical_reachable_key);
        branch.inherits.sort_by_key(canonical_inherits_key);
        branch.inherits.dedup();
    }
    query
        .policy_branches
        .sort_by_key(canonical_policy_branch_key);
    for reachable in &mut query.reachable {
        reachable
            .access_filters
            .sort_by_key(canonical_predicate_key);
        reachable.edge_filters.sort_by_key(canonical_predicate_key);
        if let Some(seed) = &mut reachable.seed {
            seed.filters.sort_by_key(canonical_predicate_key);
        }
    }
    query.reachable.sort_by_key(canonical_reachable_key);
    query.inherits.sort_by_key(canonical_inherits_key);
    query.inherits.dedup();
    query.includes.sort();
    query.includes.dedup();
    for subquery in &mut query.array_subqueries {
        normalize_array_subquery(subquery);
    }
    query
        .array_subqueries
        .sort_by_key(canonical_array_subquery_key);
    query.array_subqueries.dedup();
    if let Some(select) = &mut query.select {
        select.sort();
        select.dedup();
    }
    if let Some(aggregate) = &mut query.aggregate {
        aggregate.aggregates.sort_by_key(canonical_aggregate_key);
    }
    query
}

fn normalize_array_subquery(subquery: &mut ArraySubquery) {
    subquery.filters.sort_by_key(canonical_predicate_key);
    if let Some(select) = &mut subquery.select {
        select.sort();
        select.dedup();
    }
    for nested in &mut subquery.nested_arrays {
        normalize_array_subquery(nested);
    }
    subquery
        .nested_arrays
        .sort_by_key(canonical_array_subquery_key);
    subquery.nested_arrays.dedup();
}

fn canonical_flat_join_key(flat_join: &FlatJoin) -> Vec<u8> {
    let mut bytes = Vec::new();
    match flat_join.root_alias.as_deref() {
        Some(alias) => {
            bytes.push(1);
            put_str(&mut bytes, alias);
        }
        None => bytes.push(0),
    }
    put_len(&mut bytes, flat_join.sources.len());
    for source in &flat_join.sources {
        put_str(&mut bytes, &source.table);
        match source.alias.as_deref() {
            Some(alias) => {
                bytes.push(1);
                put_str(&mut bytes, alias);
            }
            None => bytes.push(0),
        }
        put_str(&mut bytes, &source.on.left);
        put_str(&mut bytes, &source.on.right);
    }
    bytes
}

fn normalize_join(join: &mut JoinVia) {
    join.correlated_filters
        .sort_by_key(canonical_join_correlation_key);
    for nested in &mut join.nested_joins {
        nested.filters.sort_by_key(canonical_predicate_key);
        normalize_join(nested);
    }
    join.nested_joins.sort_by_key(canonical_join_key);
}

fn canonical_policy_branch_key(branch: &PolicyBranch) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_len(&mut bytes, branch.filters.len());
    for filter in &branch.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_len(&mut bytes, branch.joins.len());
    for join in &branch.joins {
        put_bytes(&mut bytes, &canonical_join_key(join));
    }
    put_len(&mut bytes, branch.reachable.len());
    for reachable in &branch.reachable {
        put_bytes(&mut bytes, &canonical_reachable_key(reachable));
    }
    put_len(&mut bytes, branch.inherits.len());
    for inherits in &branch.inherits {
        put_bytes(&mut bytes, &canonical_inherits_key(inherits));
    }
    bytes
}

fn canonical_policy_branch_key_for_schema(
    branch: &PolicyBranch,
    schema: &RuntimeSchema,
) -> Result<Vec<u8>, QueryError> {
    let mut bytes = Vec::new();
    put_len(&mut bytes, branch.filters.len());
    for filter in &branch.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_len(&mut bytes, branch.joins.len());
    for join in &branch.joins {
        put_bytes(&mut bytes, &canonical_join_key(join));
    }
    put_len(&mut bytes, branch.reachable.len());
    for reachable in &branch.reachable {
        put_bytes(
            &mut bytes,
            &canonical_reachable_key_for_schema(reachable, schema)?,
        );
    }
    put_len(&mut bytes, branch.inherits.len());
    for inherits in &branch.inherits {
        put_bytes(&mut bytes, &canonical_inherits_key(inherits));
    }
    Ok(bytes)
}

pub(crate) fn canonical_aggregate_key(aggregate: &Aggregate) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(match aggregate.function {
        AggregateFunction::Count => b'c',
        AggregateFunction::Sum => b's',
        AggregateFunction::Avg => b'a',
        AggregateFunction::Min => b'n',
        AggregateFunction::Max => b'x',
    });
    if let Some(column) = &aggregate.column {
        put_str(&mut bytes, column);
    }
    put_str(&mut bytes, &aggregate.alias);
    bytes
}

fn canonical_array_subquery_key(subquery: &ArraySubquery) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &subquery.column_name);
    put_str(&mut bytes, &subquery.table);
    put_str(&mut bytes, &subquery.inner_column);
    put_str(&mut bytes, &subquery.outer_column);
    put_len(&mut bytes, subquery.filters.len());
    for filter in &subquery.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    if let Some(select) = &subquery.select {
        bytes.push(b's');
        put_len(&mut bytes, select.len());
        for column in select {
            put_str(&mut bytes, column);
        }
    }
    if !subquery.order_by.is_empty() {
        bytes.push(b'o');
        put_len(&mut bytes, subquery.order_by.len());
        for order in &subquery.order_by {
            put_str(&mut bytes, &order.column);
            bytes.push(match order.direction {
                OrderDirection::Asc => b'a',
                OrderDirection::Desc => b'd',
            });
        }
    }
    if let Some(limit) = subquery.limit {
        bytes.push(b'l');
        put_len(&mut bytes, limit);
    }
    if subquery.offset != 0 {
        bytes.push(b'f');
        put_len(&mut bytes, subquery.offset);
    }
    bytes.push(match subquery.requirement {
        ArraySubqueryRequirement::Optional => b'?',
        ArraySubqueryRequirement::AtLeastOne => b'+',
        ArraySubqueryRequirement::MatchCorrelationCardinality => b'=',
    });
    if !subquery.nested_arrays.is_empty() {
        bytes.push(b'n');
        put_len(&mut bytes, subquery.nested_arrays.len());
        for nested in &subquery.nested_arrays {
            put_bytes(&mut bytes, &canonical_array_subquery_key(nested));
        }
    }
    bytes
}

fn canonical_reachable_key(reachable: &ReachableVia) -> Vec<u8> {
    canonical_reachable_key_with_seed_type(reachable, None)
}

fn canonical_reachable_key_for_schema(
    reachable: &ReachableVia,
    schema: &RuntimeSchema,
) -> Result<Vec<u8>, QueryError> {
    let seed_value_type = reachable
        .seed
        .as_ref()
        .and_then(|seed| seed.user_column.as_ref().map(|column| (seed, column)))
        .map(|(seed, column)| {
            schema_table(schema, &seed.table)
                .and_then(|table| planner_column_type(&table, column).cloned())
        })
        .transpose()?;
    Ok(canonical_reachable_key_with_seed_type(
        reachable,
        seed_value_type.as_ref(),
    ))
}

fn canonical_reachable_key_with_seed_type(
    reachable: &ReachableVia,
    seed_value_type: Option<&ColumnType>,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &reachable.access_table);
    put_str(&mut bytes, &reachable.access_row_column);
    put_str(&mut bytes, &reachable.access_team_column);
    match reachable.access_team_target {
        JoinTarget::Column => {}
        JoinTarget::RowId => bytes.push(b'r'),
    }
    put_bytes(&mut bytes, &canonical_operand_key(&reachable.from));
    put_len(&mut bytes, reachable.access_filters.len());
    for filter in &reachable.access_filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_str(&mut bytes, &reachable.edge_table);
    put_str(&mut bytes, &reachable.edge_member_column);
    put_str(&mut bytes, &reachable.edge_parent_column);
    match reachable.bound {
        RecursionBound::Fixpoint => bytes.push(b'f'),
        RecursionBound::MaxDepth(max_depth) => {
            bytes.push(b'd');
            put_len(&mut bytes, max_depth);
        }
    }
    for filter in &reachable.edge_filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    if let Some(seed) = &reachable.seed {
        bytes.push(b's');
        put_str(&mut bytes, &seed.table);
        if let (Some(user_column), Some(user_claim)) = (&seed.user_column, &seed.user_claim) {
            bytes.push(b'u');
            put_str(&mut bytes, user_column);
            put_str(&mut bytes, user_claim);
            if let Some(seed_value_type) = seed_value_type {
                bytes.push(b't');
                put_column_type(&mut bytes, seed_value_type);
            }
        }
        put_str(&mut bytes, &seed.team_column);
        put_len(&mut bytes, seed.filters.len());
        for filter in &seed.filters {
            put_bytes(&mut bytes, &canonical_predicate_key(filter));
        }
    }
    bytes
}

fn canonical_inherits_key(inherits: &InheritsVia) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &inherits.parent_column);
    bytes.push(match inherits.operation {
        InheritsOperation::Select => b's',
        InheritsOperation::Insert => b'i',
        InheritsOperation::Update => b'u',
        InheritsOperation::Delete => b'd',
    });
    match inherits.max_depth {
        Some(max_depth) => {
            bytes.push(b'd');
            put_len(&mut bytes, max_depth);
        }
        None => bytes.push(b'u'),
    }
    bytes
}

fn canonical_join_key(join: &JoinVia) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &join.table);
    put_str(&mut bytes, &join.on_column);
    match join.target {
        JoinTarget::Column => {}
        JoinTarget::RowId => bytes.push(b'r'),
    }
    if let Some(column) = &join.source_column {
        bytes.push(b's');
        put_str(&mut bytes, column);
    }
    if let Some(lookup) = &join.source_lookup {
        bytes.push(b'l');
        put_str(&mut bytes, &lookup.table);
        put_str(&mut bytes, &lookup.row_id_source_column);
        put_str(&mut bytes, &lookup.value_column);
    }
    if !join.correlated_filters.is_empty() {
        bytes.push(b'c');
        put_len(&mut bytes, join.correlated_filters.len());
        for correlation in &join.correlated_filters {
            put_bytes(&mut bytes, &canonical_join_correlation_key(correlation));
        }
    }
    if !join.nested_joins.is_empty() {
        bytes.push(b'j');
        put_len(&mut bytes, join.nested_joins.len());
        for nested in &join.nested_joins {
            put_bytes(&mut bytes, &canonical_join_key(nested));
        }
    }
    for filter in &join.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    bytes
}

fn canonical_join_correlation_key(correlation: &JoinCorrelation) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &correlation.join_column);
    put_str(&mut bytes, &correlation.source_column);
    bytes
}

fn canonical_predicate_key(predicate: &Predicate) -> Vec<u8> {
    let mut bytes = Vec::new();
    match predicate {
        Predicate::All(predicates) => {
            bytes.push(b'A');
            let mut predicates = predicates
                .iter()
                .map(canonical_predicate_key)
                .collect::<Vec<_>>();
            predicates.sort();
            put_len(&mut bytes, predicates.len());
            for predicate in predicates {
                put_bytes(&mut bytes, &predicate);
            }
        }
        Predicate::Any(predicates) => {
            bytes.push(b'O');
            let mut predicates = predicates
                .iter()
                .map(canonical_predicate_key)
                .collect::<Vec<_>>();
            predicates.sort();
            put_len(&mut bytes, predicates.len());
            for predicate in predicates {
                put_bytes(&mut bytes, &predicate);
            }
        }
        Predicate::Not(predicate) => {
            bytes.push(b'!');
            put_bytes(&mut bytes, &canonical_predicate_key(predicate));
        }
        Predicate::Eq(left, right) => {
            bytes.push(b'e');
            let mut operands = [canonical_operand_key(left), canonical_operand_key(right)];
            operands.sort();
            put_bytes(&mut bytes, &operands[0]);
            put_bytes(&mut bytes, &operands[1]);
        }
        Predicate::Ne(left, right) => {
            bytes.push(b'n');
            let mut operands = [canonical_operand_key(left), canonical_operand_key(right)];
            operands.sort();
            put_bytes(&mut bytes, &operands[0]);
            put_bytes(&mut bytes, &operands[1]);
        }
        Predicate::In(left, values) => {
            bytes.push(b'i');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            let mut values = values.iter().map(canonical_operand_key).collect::<Vec<_>>();
            values.sort();
            put_len(&mut bytes, values.len());
            for value in values {
                put_bytes(&mut bytes, &value);
            }
        }
        Predicate::Gt(left, right) => {
            bytes.push(b'g');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Gte(left, right) => {
            bytes.push(b'G');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Lt(left, right) => {
            bytes.push(b't');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Lte(left, right) => {
            bytes.push(b'T');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Contains(left, right) => {
            bytes.push(b'c');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::IsNull(operand) => {
            bytes.push(b'0');
            put_bytes(&mut bytes, &canonical_operand_key(operand));
        }
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            bytes.push(b'm');
            put_str(&mut bytes, column);
            put_str(&mut bytes, case);
            put_bytes(&mut bytes, &canonical_predicate_key(payload));
        }
    }
    bytes
}

fn canonical_operand_key(operand: &Operand) -> Vec<u8> {
    let mut bytes = Vec::new();
    match operand {
        Operand::Column(name) => {
            bytes.push(b'c');
            put_str(&mut bytes, name);
        }
        Operand::Param(name) => {
            bytes.push(b'p');
            put_str(&mut bytes, name);
        }
        Operand::Claim(name) => {
            bytes.push(b'a');
            put_str(&mut bytes, name);
        }
        Operand::Literal(value) => {
            bytes.push(b'l');
            put_value(&mut bytes, value);
        }
    }
    bytes
}

fn canonical_query_bytes_for_schema(
    query: &Query,
    schema: &RuntimeSchema,
) -> Result<Vec<u8>, QueryError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-query-v0");
    put_str(&mut bytes, &query.table);
    put_len(&mut bytes, query.filters.len());
    for filter in &query.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_len(&mut bytes, query.joins.len());
    for join in &query.joins {
        put_bytes(&mut bytes, &canonical_join_key(join));
    }
    if let Some(flat_join) = &query.flat_join {
        bytes.push(b'j');
        put_bytes(&mut bytes, &canonical_flat_join_key(flat_join));
    }
    if !query.policy_branches.is_empty() {
        bytes.push(b'b');
        put_len(&mut bytes, query.policy_branches.len());
        for branch in &query.policy_branches {
            put_bytes(
                &mut bytes,
                &canonical_policy_branch_key_for_schema(branch, schema)?,
            );
        }
    }
    if !query.reachable.is_empty() {
        bytes.push(b'r');
        put_len(&mut bytes, query.reachable.len());
        for reachable in &query.reachable {
            put_bytes(
                &mut bytes,
                &canonical_reachable_key_for_schema(reachable, schema)?,
            );
        }
    }
    if !query.inherits.is_empty() {
        bytes.push(b'i');
        put_len(&mut bytes, query.inherits.len());
        for inherits in &query.inherits {
            put_bytes(&mut bytes, &canonical_inherits_key(inherits));
        }
    }
    put_len(&mut bytes, query.includes.len());
    for include in &query.includes {
        put_str(&mut bytes, &include.path);
        bytes.push(match include.join_mode {
            JoinMode::Inner => b'i',
            JoinMode::Holes => b'h',
        });
        bytes.push(u8::from(include.require));
    }
    if !query.array_subqueries.is_empty() {
        bytes.push(b'y');
        put_len(&mut bytes, query.array_subqueries.len());
        for subquery in &query.array_subqueries {
            put_bytes(&mut bytes, &canonical_array_subquery_key(subquery));
        }
    }
    if let Some(select) = &query.select {
        bytes.push(b's');
        put_len(&mut bytes, select.len());
        for column in select {
            put_str(&mut bytes, column);
        }
    }
    if !query.order_by.is_empty() {
        bytes.push(b'o');
        put_len(&mut bytes, query.order_by.len());
        for order in &query.order_by {
            put_str(&mut bytes, &order.column);
            bytes.push(match order.direction {
                OrderDirection::Asc => b'a',
                OrderDirection::Desc => b'd',
            });
        }
    }
    if let Some(aggregate) = &query.aggregate {
        bytes.push(b'a');
        put_len(&mut bytes, aggregate.aggregates.len());
        for aggregate in &aggregate.aggregates {
            put_bytes(&mut bytes, &canonical_aggregate_key(aggregate));
        }
        if let Some(group_by) = &aggregate.group_by {
            bytes.push(1);
            put_str(&mut bytes, group_by);
        } else {
            bytes.push(0);
        }
    }
    if query.limit.is_some() || query.offset != 0 {
        bytes.push(b'p');
        match query.limit {
            Some(limit) => {
                bytes.push(1);
                put_len(&mut bytes, limit);
            }
            None => bytes.push(0),
        }
        put_len(&mut bytes, query.offset);
    }
    if let Some(relation) = &query.relation {
        bytes.push(b'u');
        put_bytes(&mut bytes, &canonical_relation_query_key(relation)?);
    }
    Ok(bytes)
}

/// Relation query identity is an explicitly canonical JSON tree: object keys
/// sort lexicographically and scalar JSON values retain their exact content.
/// This prevents array-derived labels from accidentally collapsing distinct
/// literal values while keeping the public serde envelope out of shape ids.
fn canonical_relation_query_key(query: &crate::query::RelationQuery) -> Result<Vec<u8>, QueryError> {
    let value = serde_json::to_value(query)
        .map_err(|error| QueryError::UnsupportedRelationQuery(format!("encode relation query: {error}")))?;
    fn write(value: &serde_json::Value, out: &mut Vec<u8>) {
        match value {
            serde_json::Value::Null => out.push(b'n'),
            serde_json::Value::Bool(value) => out.push(if *value { b't' } else { b'f' }),
            serde_json::Value::Number(value) => { out.push(b'#'); put_str(out, &value.to_string()); }
            serde_json::Value::String(value) => { out.push(b's'); put_str(out, value); }
            serde_json::Value::Array(values) => { out.push(b'['); put_len(out, values.len()); for value in values { write(value, out); } }
            serde_json::Value::Object(values) => {
                out.push(b'{'); put_len(out, values.len());
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                for (key, value) in entries { put_str(out, key); write(value, out); }
            }
        }
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-relation-v1");
    write(&value, &mut bytes);
    Ok(bytes)
}

fn canonical_binding_bytes(values: &BTreeMap<String, Value>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-binding-v0");
    put_len(&mut bytes, values.len());
    for (name, value) in values {
        put_str(&mut bytes, name);
        put_value(&mut bytes, value);
    }
    bytes
}

fn value_type(value: &Value) -> ColumnType {
    match value {
        Value::U8(_) => ColumnType::U8,
        Value::U16(_) => ColumnType::U16,
        Value::U32(_) => ColumnType::U32,
        Value::U64(_) => ColumnType::U64,
        Value::I32(_) => ColumnType::I32,
        Value::I64(_) => ColumnType::I64,
        Value::F64(_) => ColumnType::F64,
        Value::Bool(_) => ColumnType::Bool,
        Value::String(_) => ColumnType::String,
        Value::Bytes(_) => ColumnType::Bytes,
        Value::Uuid(_) => ColumnType::Uuid,
        Value::EnumTag(_) => ColumnType::U8,
        Value::Tuple(values) => ColumnType::Tuple(values.iter().map(value_type).collect()),
        Value::Array(values) => values
            .first()
            .map(|value| ColumnType::Array(Box::new(value_type(value))))
            .unwrap_or_else(|| ColumnType::Array(Box::new(ColumnType::Bytes))),
        Value::Nullable(Some(value)) => ColumnType::Nullable(Box::new(value_type(value))),
        Value::Nullable(None) => ColumnType::Nullable(Box::new(ColumnType::Bytes)),
        Value::Record(_) => {
            panic!("record-valued query bindings are not part of the current Jazz query surface")
        }
        Value::Enum(_) => {
            panic!("union-valued query bindings are an internal Groove representation")
        }
        Value::Large(value) => match value.kind {
            groove::large_values::LargeValueKind::Bytes => ColumnType::Bytes,
            groove::large_values::LargeValueKind::String
            | groove::large_values::LargeValueKind::Json => ColumnType::String,
        },
    }
}

fn value_matches_type(value: &Value, column_type: &ColumnType) -> bool {
    match (value, column_type) {
        (Value::U8(_), ColumnType::U8)
        | (Value::U16(_), ColumnType::U16)
        | (Value::U32(_), ColumnType::U32)
        | (Value::U64(_), ColumnType::U64)
        | (Value::I32(_), ColumnType::I32)
        | (Value::I64(_), ColumnType::I64)
        | (Value::F64(_), ColumnType::F64)
        | (Value::Bool(_), ColumnType::Bool)
        | (Value::String(_), ColumnType::String)
        | (Value::Bytes(_), ColumnType::Bytes)
        | (Value::Uuid(_), ColumnType::Uuid) => true,
        (Value::EnumTag(_), ColumnType::EnumTag(_)) => true,
        (Value::Tuple(values), ColumnType::Tuple(types)) => {
            values.len() == types.len()
                && values
                    .iter()
                    .zip(types)
                    .all(|(value, column_type)| value_matches_type(value, column_type))
        }
        (Value::Array(values), ColumnType::Array(item_type)) => values
            .iter()
            .all(|value| value_matches_type(value, item_type)),
        (Value::Nullable(None), ColumnType::Nullable(_)) => true,
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => {
            value_matches_type(value, inner)
        }
        // Jazz has no public record column type in this step, so records are
        // never accepted as query-bound values.
        (Value::Record(_), _) => false,
        (Value::Enum(_), _) => false,
        // Indirect descriptors are engine-owned and cannot be supplied as a
        // public query binding; callers bind the ordinary logical primitive.
        (Value::Large(_), _) => false,
        _ => false,
    }
}

fn put_value(bytes: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => {
            bytes.push(1);
            bytes.push(*value);
        }
        Value::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::U32(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::U64(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::I32(value) => {
            bytes.push(15);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::I64(value) => {
            bytes.push(14);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::F64(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::Bool(value) => {
            bytes.push(6);
            bytes.push(u8::from(*value));
        }
        Value::String(value) => {
            bytes.push(7);
            put_str(bytes, value);
        }
        Value::Bytes(value) => {
            bytes.push(8);
            put_bytes(bytes, value);
        }
        Value::Uuid(value) => {
            bytes.push(9);
            bytes.extend_from_slice(value.as_bytes());
        }
        Value::EnumTag(value) => {
            bytes.push(10);
            bytes.push(*value);
        }
        Value::Tuple(values) => {
            bytes.push(11);
            put_len(bytes, values.len());
            for value in values {
                put_value(bytes, value);
            }
        }
        Value::Array(values) => {
            bytes.push(12);
            put_len(bytes, values.len());
            for value in values {
                put_value(bytes, value);
            }
        }
        Value::Nullable(None) => {
            bytes.push(13);
            bytes.push(0);
        }
        Value::Nullable(Some(value)) => {
            bytes.push(13);
            bytes.push(1);
            put_value(bytes, value);
        }
        Value::Record(_) => {
            panic!("record-valued query bindings have no current canonical encoding")
        }
        Value::Enum(_) => {
            panic!("union-valued query bindings are an internal Groove representation")
        }
        Value::Large(_) => {
            panic!("indirect descriptors are not public query binding values")
        }
    }
}

fn put_column_type(bytes: &mut Vec<u8>, ty: &ColumnType) {
    match ty {
        ColumnType::U8 => bytes.push(0),
        ColumnType::U16 => bytes.push(1),
        ColumnType::U32 => bytes.push(2),
        ColumnType::U64 => bytes.push(3),
        ColumnType::I32 => bytes.push(4),
        ColumnType::I64 => bytes.push(5),
        ColumnType::F64 => bytes.push(6),
        ColumnType::Bool => bytes.push(7),
        ColumnType::String => bytes.push(8),
        ColumnType::Bytes => bytes.push(9),
        ColumnType::Uuid => bytes.push(10),
        ColumnType::EnumTag(schema) => {
            bytes.push(11);
            put_str(bytes, &schema.name);
            put_len(bytes, schema.variants.len());
            for variant in &schema.variants {
                put_str(bytes, variant);
            }
        }
        ColumnType::Tuple(types) => {
            bytes.push(12);
            put_len(bytes, types.len());
            for ty in types {
                put_column_type(bytes, ty);
            }
        }
        ColumnType::Array(member) => {
            bytes.push(13);
            put_column_type(bytes, member);
        }
        ColumnType::Nullable(inner) => {
            bytes.push(14);
            put_column_type(bytes, inner);
        }
        ColumnType::Record(descriptor) => {
            bytes.push(15);
            put_len(bytes, descriptor.fields().len());
            for field in descriptor.fields() {
                match &field.name {
                    Some(name) => {
                        bytes.push(1);
                        put_str(bytes, name);
                    }
                    None => bytes.push(0),
                }
                put_column_type(bytes, &field.value_type);
            }
        }
        ColumnType::Enum(_) => {
            panic!(
                "union column types are internal to Groove and have no Jazz query binding encoding"
            )
        }
        _ => {
            panic!("raw stored-scalar backing types are not Jazz query column types")
        }
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_len(bytes, value.len());
    bytes.extend_from_slice(value);
}

fn put_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u64).to_be_bytes());
}
