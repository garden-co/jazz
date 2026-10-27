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
    put_opt_str(&mut bytes, flat_join.root_alias.as_deref());
    put_len(&mut bytes, flat_join.sources.len());
    for source in &flat_join.sources {
        put_str(&mut bytes, &source.table);
        put_opt_str(&mut bytes, source.alias.as_deref());
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
    put_keys(&mut bytes, &branch.filters, canonical_predicate_key);
    put_keys(&mut bytes, &branch.joins, canonical_join_key);
    put_keys(&mut bytes, &branch.reachable, canonical_reachable_key);
    put_keys(&mut bytes, &branch.inherits, canonical_inherits_key);
    bytes
}

fn canonical_policy_branch_key_for_schema(
    branch: &PolicyBranch,
    schema: &RuntimeSchema,
) -> Result<Vec<u8>, QueryError> {
    let mut bytes = Vec::new();
    put_keys(&mut bytes, &branch.filters, canonical_predicate_key);
    put_keys(&mut bytes, &branch.joins, canonical_join_key);
    try_put_keys(&mut bytes, &branch.reachable, |item| {
        canonical_reachable_key_for_schema(item, schema)
    })?;
    put_keys(&mut bytes, &branch.inherits, canonical_inherits_key);
    Ok(bytes)
}

#[doc(hidden)]
pub fn canonical_aggregate_key(aggregate: &Aggregate) -> Vec<u8> {
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
    put_keys(&mut bytes, &subquery.filters, canonical_predicate_key);
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
        put_keys(
            &mut bytes,
            &subquery.nested_arrays,
            canonical_array_subquery_key,
        );
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
        JoinTarget::Uncorrelated => bytes.push(b'e'),
    }
    put_bytes(&mut bytes, &canonical_operand_key(&reachable.from));
    put_keys(
        &mut bytes,
        &reachable.access_filters,
        canonical_predicate_key,
    );
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
        put_keys(&mut bytes, &seed.filters, canonical_predicate_key);
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
        JoinTarget::Uncorrelated => bytes.push(b'e'),
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
        put_keys(
            &mut bytes,
            &join.correlated_filters,
            canonical_join_correlation_key,
        );
    }
    if !join.nested_joins.is_empty() {
        bytes.push(b'j');
        put_keys(&mut bytes, &join.nested_joins, canonical_join_key);
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
            put_sorted_keys(&mut bytes, predicates.iter().map(canonical_predicate_key));
        }
        Predicate::Any(predicates) => {
            bytes.push(b'O');
            put_sorted_keys(&mut bytes, predicates.iter().map(canonical_predicate_key));
        }
        Predicate::Not(predicate) => {
            bytes.push(b'!');
            put_bytes(&mut bytes, &canonical_predicate_key(predicate));
        }
        Predicate::Eq(left, right) => put_unordered_operands(&mut bytes, b'e', left, right),
        Predicate::Ne(left, right) => put_unordered_operands(&mut bytes, b'n', left, right),
        Predicate::In(left, values) => {
            bytes.push(b'i');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_sorted_keys(&mut bytes, values.iter().map(canonical_operand_key));
        }
        Predicate::Gt(left, right) => put_operands(&mut bytes, b'g', left, right),
        Predicate::Gte(left, right) => put_operands(&mut bytes, b'G', left, right),
        Predicate::Lt(left, right) => put_operands(&mut bytes, b't', left, right),
        Predicate::Lte(left, right) => put_operands(&mut bytes, b'T', left, right),
        Predicate::Contains(left, right) => put_operands(&mut bytes, b'c', left, right),
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
    put_keys(&mut bytes, &query.filters, canonical_predicate_key);
    put_keys(&mut bytes, &query.joins, canonical_join_key);
    if let Some(flat_join) = &query.flat_join {
        bytes.push(b'j');
        put_bytes(&mut bytes, &canonical_flat_join_key(flat_join));
    }
    if !query.policy_branches.is_empty() {
        bytes.push(b'b');
        try_put_keys(&mut bytes, &query.policy_branches, |item| {
            canonical_policy_branch_key_for_schema(item, schema)
        })?;
    }
    if !query.reachable.is_empty() {
        bytes.push(b'r');
        try_put_keys(&mut bytes, &query.reachable, |item| {
            canonical_reachable_key_for_schema(item, schema)
        })?;
    }
    if !query.inherits.is_empty() {
        bytes.push(b'i');
        put_keys(&mut bytes, &query.inherits, canonical_inherits_key);
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
        put_keys(
            &mut bytes,
            &query.array_subqueries,
            canonical_array_subquery_key,
        );
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
        put_keys(&mut bytes, &aggregate.aggregates, canonical_aggregate_key);
        put_opt_str(&mut bytes, aggregate.group_by.as_deref());
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
fn canonical_relation_query_key(
    query: &crate::query::RelationQuery,
) -> Result<Vec<u8>, QueryError> {
    let value = serde_json::to_value(query).map_err(|error| {
        QueryError::UnsupportedRelationQuery(format!("encode relation query: {error}"))
    })?;
    fn write(value: &serde_json::Value, out: &mut Vec<u8>) {
        match value {
            serde_json::Value::Null => out.push(b'n'),
            serde_json::Value::Bool(value) => out.push(if *value { b't' } else { b'f' }),
            serde_json::Value::Number(value) => {
                out.push(b'#');
                put_str(out, &value.to_string());
            }
            serde_json::Value::String(value) => {
                out.push(b's');
                put_str(out, value);
            }
            serde_json::Value::Array(values) => {
                out.push(b'[');
                put_len(out, values.len());
                for value in values {
                    write(value, out);
                }
            }
            serde_json::Value::Object(values) => {
                out.push(b'{');
                put_len(out, values.len());
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                for (key, value) in entries {
                    put_str(out, key);
                    write(value, out);
                }
            }
        }
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-relation-v1");
    write(&value, &mut bytes);
    Ok(bytes)
}

fn canonical_binding_bytes(
    values: &BTreeMap<String, Value>,
) -> Result<Vec<u8>, QueryError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-binding-v0");
    put_len(&mut bytes, values.len());
    for (name, value) in values {
        put_str(&mut bytes, name);
        validate_binding_value(value)?;
        put_value(&mut bytes, value);
    }
    Ok(bytes)
}

/// Binding identities encode with the same tags as query literals, but only
/// accept values a public caller can supply.
fn validate_binding_value(value: &Value) -> Result<(), QueryError> {
    match value {
        Value::Tuple(values) | Value::Array(values) => {
            for value in values {
                validate_binding_value(value)?;
            }
        }
        Value::Nullable(Some(value)) => validate_binding_value(value)?,
        Value::Record(record) => validate_public_column_type(&ColumnType::Record(Box::new(
            record.descriptor().clone(),
        )))?,
        // U48 is an engine-owned stamp width, never a public query binding.
        Value::Enum(_) | Value::Large(_) | Value::U48(_) => {
            return Err(QueryError::OperandTypeMismatch);
        }
        _ => {}
    }
    Ok(())
}

fn validate_public_column_type(column_type: &ColumnType) -> Result<(), QueryError> {
    match column_type {
        ColumnType::Tuple(types) => {
            for column_type in types {
                validate_public_column_type(column_type)?;
            }
        }
        ColumnType::Array(inner) | ColumnType::Nullable(inner) => {
            validate_public_column_type(inner)?;
        }
        ColumnType::Record(descriptor) => {
            for field in descriptor.fields() {
                validate_public_column_type(&field.value_type)?;
            }
        }
        ColumnType::Enum(schema) => {
            for case in &schema.cases {
                for field in case.payload.fields() {
                    validate_public_column_type(&field.value_type)?;
                }
            }
        }
        ColumnType::Internal(_) | ColumnType::U48 => return Err(QueryError::OperandTypeMismatch),
        ColumnType::U8
        | ColumnType::U16
        | ColumnType::U32
        | ColumnType::U64
        | ColumnType::I32
        | ColumnType::I64
        | ColumnType::F64
        | ColumnType::Bool
        | ColumnType::String
        | ColumnType::Bytes
        | ColumnType::Uuid
        | ColumnType::EnumTag(_) => {}
    }
    Ok(())
}

fn value_type(value: &Value) -> Result<ColumnType, QueryError> {
    match value {
        Value::U8(_) => Ok(ColumnType::U8),
        Value::U16(_) => Ok(ColumnType::U16),
        Value::U32(_) => Ok(ColumnType::U32),
        Value::U64(_) => Ok(ColumnType::U64),
        Value::I32(_) => Ok(ColumnType::I32),
        Value::I64(_) => Ok(ColumnType::I64),
        Value::F64(_) => Ok(ColumnType::F64),
        Value::Bool(_) => Ok(ColumnType::Bool),
        Value::String(_) => Ok(ColumnType::String),
        Value::Bytes(_) => Ok(ColumnType::Bytes),
        Value::Uuid(_) => Ok(ColumnType::Uuid),
        Value::EnumTag(_) => Ok(ColumnType::U8),
        Value::Tuple(values) => values
            .iter()
            .map(value_type)
            .collect::<Result<Vec<_>, _>>()
            .map(ColumnType::Tuple),
        Value::Array(values) => {
            // Infer the array type from its first element as before, but walk
            // every element so an unsupported nested value cannot reach the
            // infallible canonical encoder.
            let mut types = values.iter().map(value_type);
            let first = types.next().transpose()?;
            for value_type in types {
                value_type?;
            }
            Ok(first
                .map(|value_type| ColumnType::Array(Box::new(value_type)))
                .unwrap_or_else(|| ColumnType::Array(Box::new(ColumnType::Bytes))))
        }
        Value::Nullable(Some(value)) => Ok(ColumnType::Nullable(Box::new(value_type(value)?))),
        Value::Nullable(None) => Ok(ColumnType::Nullable(Box::new(ColumnType::Bytes))),
        Value::Record(record) => {
            let column_type = ColumnType::Record(Box::new(record.descriptor().clone()));
            validate_public_column_type(&column_type)?;
            Ok(column_type)
        }
        Value::Enum(_) | Value::Large(_) | Value::U48(_) => Err(QueryError::OperandTypeMismatch),
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
        (Value::Record(record), ColumnType::Record(descriptor)) => {
            record.descriptor() == descriptor.as_ref()
        }
        (Value::Enum(_), _) => false,
        // Indirect descriptors are engine-owned and cannot be supplied as a
        // public query binding; callers bind the ordinary logical primitive.
        _ => false,
    }
}

/// Writes a one-byte tag followed by `payload`.
fn put_tagged(bytes: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    bytes.push(tag);
    bytes.extend_from_slice(payload);
}

fn put_value(bytes: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => put_tagged(bytes, 1, &[*value]),
        Value::U16(value) => put_tagged(bytes, 2, &value.to_be_bytes()),
        Value::U32(value) => put_tagged(bytes, 3, &value.to_be_bytes()),
        Value::U64(value) => put_tagged(bytes, 4, &value.to_be_bytes()),
        Value::I32(value) => put_tagged(bytes, 15, &value.to_be_bytes()),
        Value::I64(value) => put_tagged(bytes, 14, &value.to_be_bytes()),
        Value::F64(value) => put_tagged(bytes, 5, &value.to_bits().to_be_bytes()),
        Value::Bool(value) => put_tagged(bytes, 6, &[u8::from(*value)]),
        Value::String(value) => {
            bytes.push(7);
            put_str(bytes, value);
        }
        Value::Bytes(value) => {
            bytes.push(8);
            put_bytes(bytes, value);
        }
        Value::Uuid(value) => put_tagged(bytes, 9, value.as_bytes()),
        Value::EnumTag(value) => put_tagged(bytes, 10, &[*value]),
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
        Value::Nullable(None) => put_tagged(bytes, 13, &[0]),
        Value::Nullable(Some(value)) => {
            bytes.push(13);
            bytes.push(1);
            put_value(bytes, value);
        }
        Value::Record(record) => {
            bytes.push(16);
            put_column_type(
                bytes,
                &ColumnType::Record(Box::new(record.descriptor().clone())),
            );
            put_bytes(bytes, record.raw());
        }
        // These internal arms are rejected by value_type before a
        // validated query reaches canonicalization. Keep their fallback
        // encodings total for defensive callers such as binding identities.
        Value::Enum(value) => {
            bytes.push(17);
            bytes.extend_from_slice(&value.tag().to_be_bytes());
            put_column_type(
                bytes,
                &ColumnType::Record(Box::new(value.record().descriptor().clone())),
            );
            put_bytes(bytes, value.record().raw());
        }
        Value::U48(value) => put_tagged(bytes, 19, &value.to_be_bytes()),
        Value::Large(value) => {
            bytes.push(18);
            bytes.push(match value.kind {
                jazz_types::large_values::LargeValueKind::Bytes => 0,
                jazz_types::large_values::LargeValueKind::String => 1,
                jazz_types::large_values::LargeValueKind::Json => 2,
            });
            bytes.push(value.format_version);
            bytes.extend_from_slice(&value.logical_hash.0);
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
                // Physical slot identity is part of a record literal's type.
                // Equal display names must not alias distinct typed columns.
                match &field.identity {
                    None => bytes.push(0),
                    Some(jazz_types::records::FieldIdentity::Name(name)) => {
                        bytes.push(1);
                        put_str(bytes, name);
                    }
                    Some(jazz_types::records::FieldIdentity::Slot(slot)) => {
                        bytes.push(2);
                        bytes.extend_from_slice(&slot.to_be_bytes());
                    }
                    Some(jazz_types::records::FieldIdentity::NamedSlot { name, slot }) => {
                        bytes.push(3);
                        put_str(bytes, name);
                        bytes.extend_from_slice(&slot.to_be_bytes());
                    }
                }
                put_column_type(bytes, &field.value_type);
            }
        }
        ColumnType::Enum(schema) => {
            bytes.push(16);
            bytes.extend_from_slice(&schema.registry_id.to_be_bytes());
            put_str(bytes, &schema.name);
            put_len(bytes, schema.cases.len());
            for case in &schema.cases {
                put_str(bytes, &case.name);
                put_column_type(
                    bytes,
                    &ColumnType::Record(Box::new(case.payload.clone())),
                );
            }
        }
        // Internal physical types cannot be supplied through the public query
        // interface. Still keep this defensive encoder total if an internal
        // descriptor is carried by an otherwise supported record literal.
        ColumnType::Internal(_) => bytes.push(17),
        ColumnType::U48 => bytes.push(18),
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_len(bytes, value.len());
    bytes.extend_from_slice(value);
}

/// A length-prefixed list of each item's canonical key.
fn put_keys<T>(bytes: &mut Vec<u8>, items: &[T], key: impl Fn(&T) -> Vec<u8>) {
    put_len(bytes, items.len());
    for item in items {
        put_bytes(bytes, &key(item));
    }
}

fn try_put_keys<T>(
    bytes: &mut Vec<u8>,
    items: &[T],
    key: impl Fn(&T) -> Result<Vec<u8>, QueryError>,
) -> Result<(), QueryError> {
    put_len(bytes, items.len());
    for item in items {
        put_bytes(bytes, &key(item)?);
    }
    Ok(())
}

/// A length-prefixed list of keys in sorted order, for operand lists whose
/// order does not change meaning.
fn put_sorted_keys(bytes: &mut Vec<u8>, keys: impl IntoIterator<Item = Vec<u8>>) {
    let mut keys = keys.into_iter().collect::<Vec<_>>();
    keys.sort();
    put_len(bytes, keys.len());
    for key in keys {
        put_bytes(bytes, &key);
    }
}

fn put_operands(bytes: &mut Vec<u8>, tag: u8, left: &Operand, right: &Operand) {
    bytes.push(tag);
    put_bytes(bytes, &canonical_operand_key(left));
    put_bytes(bytes, &canonical_operand_key(right));
}

/// Like [`put_operands`], but for symmetric comparisons whose operand order
/// does not change meaning.
fn put_unordered_operands(bytes: &mut Vec<u8>, tag: u8, left: &Operand, right: &Operand) {
    bytes.push(tag);
    let mut operands = [canonical_operand_key(left), canonical_operand_key(right)];
    operands.sort();
    put_bytes(bytes, &operands[0]);
    put_bytes(bytes, &operands[1]);
}

fn put_opt_str(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            bytes.push(1);
            put_str(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn put_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u64).to_be_bytes());
}
