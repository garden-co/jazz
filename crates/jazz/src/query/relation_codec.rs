// JRQ v1 is a closed, explicit binary grammar for `RelationQuery`.
// The opcodes below are the wire ABI; relation AST field/variant names are
// deliberately never serialized. JSON is retained only as the public literal
// envelope, with explicit scalar tags to avoid decimal-format differences.
const MAGIC: &[u8] = b"JRQ\x01";
const MAX_BYTES: usize = 1 << 20;
const MAX_DEPTH: usize = 128;
const MAX_ITEMS: usize = 4096;
const MAX_STRING: usize = 1 << 16;
const MAX_UNION_LABEL: usize = 4096;

#[derive(Debug, thiserror::Error)]
/// Failure while encoding or decoding the bounded JRQ v1 carrier.
pub enum RelationCodecError {
    /// The payload violates the JRQ v1 grammar.
    #[error("malformed relation query binary: {0}")]
    Malformed(&'static str),
    /// A literal cannot be represented by the public JSON envelope.
    #[error("malformed relation query binary: {0}")]
    Detail(String),
}
type CodecResult<T> = std::result::Result<T, RelationCodecError>;
fn bad(message: &'static str) -> RelationCodecError { RelationCodecError::Malformed(message) }

#[derive(Default)]
struct JState { nodes: usize, string_bytes: usize }
impl JState {
    fn node(&mut self, depth: usize) -> CodecResult<()> {
        if depth >= MAX_DEPTH { return Err(bad("too deep")); }
        self.nodes += 1;
        if self.nodes > MAX_ITEMS { return Err(bad("too many nodes")); }
        Ok(())
    }
    fn string(&mut self, len: usize) -> CodecResult<()> {
        if len > MAX_STRING { return Err(bad("string too large")); }
        self.string_bytes = self.string_bytes.checked_add(len).ok_or_else(|| bad("too large"))?;
        if self.string_bytes > MAX_BYTES { return Err(bad("too large")); }
        Ok(())
    }
    fn collection(&self, len: usize) -> CodecResult<()> { if len > MAX_ITEMS { Err(bad("too many items")) } else { Ok(()) } }
}

fn jrq_put_len(out: &mut Vec<u8>, mut value: usize) { loop { let mut byte = (value & 0x7f) as u8; value >>= 7; if value != 0 { byte |= 0x80; } out.push(byte); if value == 0 { return; } } }
fn get_byte(input: &mut &[u8]) -> CodecResult<u8> { let Some((&value, rest)) = input.split_first() else { return Err(bad("truncated")); }; *input = rest; Ok(value) }
fn get_len(input: &mut &[u8]) -> CodecResult<usize> {
    let mut value = 0usize; let mut shift = 0u32;
    for index in 0..10 { let byte = get_byte(input)?; let part = (byte & 0x7f) as usize;
        if shift >= usize::BITS || part > (usize::MAX >> shift) { return Err(bad("length overflow")); }
        value |= part << shift;
        if byte & 0x80 == 0 { if index > 0 && part == 0 { return Err(bad("nonminimal length")); } return Ok(value); }
        shift += 7;
    }
    Err(bad("length overflow"))
}
fn put_string(out: &mut Vec<u8>, value: &str, state: &mut JState) -> CodecResult<()> { state.string(value.len())?; jrq_put_len(out, value.len()); out.extend_from_slice(value.as_bytes()); Ok(()) }
fn get_string(input: &mut &[u8], state: &mut JState) -> CodecResult<String> { let len = get_len(input)?; state.string(len)?; if input.len() < len { return Err(bad("truncated string")); } let (bytes, rest) = input.split_at(len); *input = rest; Ok(std::str::from_utf8(bytes).map_err(|_| bad("invalid utf8"))?.to_owned()) }
fn put_count(out: &mut Vec<u8>, len: usize, state: &JState) -> CodecResult<()> { state.collection(len)?; jrq_put_len(out, len); Ok(()) }
fn get_count(input: &mut &[u8], state: &JState) -> CodecResult<usize> { let len = get_len(input)?; state.collection(len)?; Ok(len) }
fn put_label(out: &mut Vec<u8>, label: &str, state: &mut JState) -> CodecResult<()> { if label.is_empty() || label.len() > MAX_UNION_LABEL || label.as_bytes().contains(&0) { return Err(bad("invalid union label")); } put_string(out, label, state) }
fn get_label(input: &mut &[u8], state: &mut JState) -> CodecResult<String> { let label = get_string(input, state)?; if label.is_empty() || label.len() > MAX_UNION_LABEL || label.as_bytes().contains(&0) { return Err(bad("invalid union label")); } Ok(label) }

fn put_column(out: &mut Vec<u8>, value: &RelationColumnRef, state: &mut JState) -> CodecResult<()> { match &value.scope { None => out.push(0), Some(scope) => { out.push(1); put_string(out, scope, state)?; } }; put_string(out, &value.column, state) }
fn get_column(input: &mut &[u8], state: &mut JState) -> CodecResult<RelationColumnRef> { let scope = match get_byte(input)? { 0 => None, 1 => Some(get_string(input, state)?), _ => return Err(bad("column scope tag")) }; Ok(RelationColumnRef { scope, column: get_string(input, state)? }) }
fn put_row_id(out: &mut Vec<u8>, value: RelationRowIdRef) { out.push(match value { RelationRowIdRef::Current => 0, RelationRowIdRef::Outer => 1, RelationRowIdRef::Frontier => 2 }); }
fn get_row_id(input: &mut &[u8]) -> CodecResult<RelationRowIdRef> { match get_byte(input)? { 0 => Ok(RelationRowIdRef::Current), 1 => Ok(RelationRowIdRef::Outer), 2 => Ok(RelationRowIdRef::Frontier), _ => Err(bad("row id tag")) } }

// Literal tags: null=0 false=1 true=2 i64=3 u64=4 f64-le=5 string=6 array=7 object=8.
fn put_json(out: &mut Vec<u8>, value: &serde_json::Value, depth: usize, state: &mut JState) -> CodecResult<()> {
    state.node(depth)?;
    match value {
        serde_json::Value::Null => out.push(0), serde_json::Value::Bool(false) => out.push(1), serde_json::Value::Bool(true) => out.push(2),
        serde_json::Value::Number(number) if number.is_i64() => { out.push(3); let n = number.as_i64().ok_or_else(|| bad("invalid integer"))?; jrq_put_len(out, (((n as u64) << 1) ^ ((n >> 63) as u64)) as usize); }
        serde_json::Value::Number(number) if number.is_u64() => { out.push(4); jrq_put_len(out, usize::try_from(number.as_u64().ok_or_else(|| bad("invalid integer"))?).map_err(|_| bad("integer overflow"))?); }
        serde_json::Value::Number(number) => { out.push(5); let n = number.as_f64().ok_or_else(|| bad("invalid float"))?; if !n.is_finite() { return Err(bad("invalid float")); } out.extend_from_slice(&n.to_le_bytes()); }
        serde_json::Value::String(value) => { out.push(6); put_string(out, value, state)?; }
        serde_json::Value::Array(values) => { out.push(7); put_count(out, values.len(), state)?; for value in values { put_json(out, value, depth + 1, state)?; } }
        serde_json::Value::Object(values) => { out.push(8); put_count(out, values.len(), state)?; let mut entries = values.iter().collect::<Vec<_>>(); entries.sort_unstable_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes())); for (key, value) in entries { put_string(out, key, state)?; put_json(out, value, depth + 1, state)?; } }
    }
    Ok(())
}
fn get_json(input: &mut &[u8], depth: usize, state: &mut JState) -> CodecResult<serde_json::Value> {
    state.node(depth)?;
    Ok(match get_byte(input)? {
        0 => serde_json::Value::Null, 1 => serde_json::Value::Bool(false), 2 => serde_json::Value::Bool(true),
        3 => { let n = get_len(input)? as u64; serde_json::Value::Number((((n >> 1) as i64) ^ -((n & 1) as i64)).into()) }
        4 => serde_json::Value::Number(serde_json::Number::from(get_len(input)? as u64)),
        5 => { if input.len() < 8 { return Err(bad("truncated float")); } let (bytes, rest) = input.split_at(8); *input = rest; let n = f64::from_le_bytes(bytes.try_into().expect("fixed float length")); serde_json::Value::Number(serde_json::Number::from_f64(n).ok_or_else(|| bad("invalid float"))?) }
        6 => serde_json::Value::String(get_string(input, state)?),
        7 => { let len = get_count(input, state)?; let mut values = Vec::with_capacity(len); for _ in 0..len { values.push(get_json(input, depth + 1, state)?); } serde_json::Value::Array(values) }
        8 => { let len = get_count(input, state)?; let mut values = serde_json::Map::new(); let mut previous: Option<Vec<u8>> = None; for _ in 0..len { let key = get_string(input, state)?; if previous.as_deref() >= Some(key.as_bytes()) { return Err(bad("noncanonical object key")); } previous = Some(key.as_bytes().to_vec()); let value = get_json(input, depth + 1, state)?; if values.insert(key, value).is_some() { return Err(bad("duplicate object key")); } } serde_json::Value::Object(values) }
        _ => return Err(bad("json tag")),
    })
}

fn put_key(out: &mut Vec<u8>, value: &RelationKeyRef, state: &mut JState) -> CodecResult<()> { match value { RelationKeyRef::Column(value) => { out.push(0); put_column(out, value, state) }, RelationKeyRef::RowId(value) => { out.push(1); put_row_id(out, *value); Ok(()) } } }
fn get_key(input: &mut &[u8], state: &mut JState) -> CodecResult<RelationKeyRef> { match get_byte(input)? { 0 => Ok(RelationKeyRef::Column(get_column(input, state)?)), 1 => Ok(RelationKeyRef::RowId(get_row_id(input)?)), _ => Err(bad("key tag")) } }
fn put_project_expr(out: &mut Vec<u8>, value: &RelationProjectExpr, state: &mut JState) -> CodecResult<()> { match value { RelationProjectExpr::Column(value) => { out.push(0); put_column(out, value, state) }, RelationProjectExpr::RowId(value) => { out.push(1); put_row_id(out, *value); Ok(()) } } }
fn get_project_expr(input: &mut &[u8], state: &mut JState) -> CodecResult<RelationProjectExpr> { match get_byte(input)? { 0 => Ok(RelationProjectExpr::Column(get_column(input, state)?)), 1 => Ok(RelationProjectExpr::RowId(get_row_id(input)?)), _ => Err(bad("project expression tag")) } }

fn jrq_put_value(out: &mut Vec<u8>, value: &RelationValueRef, depth: usize, state: &mut JState) -> CodecResult<()> { state.node(depth)?; match value { RelationValueRef::Literal(value) => { out.push(0); put_json(out, value, depth + 1, state) }, RelationValueRef::Param(value) => { out.push(1); put_string(out, value, state) }, RelationValueRef::SessionRef(values) => { out.push(2); put_count(out, values.len(), state)?; for value in values { put_string(out, value, state)?; } Ok(()) }, RelationValueRef::OuterColumn(value) => { out.push(3); put_column(out, value, state) }, RelationValueRef::FrontierColumn(value) => { out.push(4); put_column(out, value, state) }, RelationValueRef::RowId(value) => { out.push(5); put_row_id(out, *value); Ok(()) } } }
fn get_value(input: &mut &[u8], depth: usize, state: &mut JState) -> CodecResult<RelationValueRef> { state.node(depth)?; match get_byte(input)? { 0 => Ok(RelationValueRef::Literal(get_json(input, depth + 1, state)?)), 1 => Ok(RelationValueRef::Param(get_string(input, state)?)), 2 => { let len = get_count(input, state)?; let mut values = Vec::with_capacity(len); for _ in 0..len { values.push(get_string(input, state)?); } Ok(RelationValueRef::SessionRef(values)) }, 3 => Ok(RelationValueRef::OuterColumn(get_column(input, state)?)), 4 => Ok(RelationValueRef::FrontierColumn(get_column(input, state)?)), 5 => Ok(RelationValueRef::RowId(get_row_id(input)?)), _ => Err(bad("value tag")) } }

// Predicate tags: cmp=0 is-null=1 is-not-null=2 in=3 contains=4 enum-match=5 and=6 or=7 not=8 true=9 false=10.
fn put_predicate(out: &mut Vec<u8>, value: &RelationPredicate, depth: usize, state: &mut JState) -> CodecResult<()> { state.node(depth)?; match value {
    RelationPredicate::Cmp { left, op, right } => { out.push(0); put_column(out, left, state)?; out.push(match op { RelationCmpOp::Eq => 0, RelationCmpOp::Ne => 1, RelationCmpOp::Lt => 2, RelationCmpOp::Le => 3, RelationCmpOp::Gt => 4, RelationCmpOp::Ge => 5 }); jrq_put_value(out, right, depth + 1, state) }
    RelationPredicate::IsNull { column } => { out.push(1); put_column(out, column, state) }, RelationPredicate::IsNotNull { column } => { out.push(2); put_column(out, column, state) },
    RelationPredicate::In { left, values } => { out.push(3); put_column(out, left, state)?; put_count(out, values.len(), state)?; for value in values { jrq_put_value(out, value, depth + 1, state)?; } Ok(()) },
    RelationPredicate::Contains { left, right } => { out.push(4); put_column(out, left, state)?; jrq_put_value(out, right, depth + 1, state) },
    RelationPredicate::EnumMatch { column, case, payload } => { out.push(5); put_column(out, column, state)?; put_string(out, case, state)?; put_predicate(out, payload, depth + 1, state) },
    RelationPredicate::And(values) => { out.push(6); put_count(out, values.len(), state)?; for value in values { put_predicate(out, value, depth + 1, state)?; } Ok(()) }, RelationPredicate::Or(values) => { out.push(7); put_count(out, values.len(), state)?; for value in values { put_predicate(out, value, depth + 1, state)?; } Ok(()) },
    RelationPredicate::Not(value) => { out.push(8); put_predicate(out, value, depth + 1, state) }, RelationPredicate::True => { out.push(9); Ok(()) }, RelationPredicate::False => { out.push(10); Ok(()) },
} }
fn get_predicate(input: &mut &[u8], depth: usize, state: &mut JState) -> CodecResult<RelationPredicate> { state.node(depth)?; Ok(match get_byte(input)? {
    0 => { let left = get_column(input, state)?; let op = match get_byte(input)? { 0 => RelationCmpOp::Eq, 1 => RelationCmpOp::Ne, 2 => RelationCmpOp::Lt, 3 => RelationCmpOp::Le, 4 => RelationCmpOp::Gt, 5 => RelationCmpOp::Ge, _ => return Err(bad("comparison tag")) }; RelationPredicate::Cmp { left, op, right: get_value(input, depth + 1, state)? } },
    1 => RelationPredicate::IsNull { column: get_column(input, state)? }, 2 => RelationPredicate::IsNotNull { column: get_column(input, state)? },
    3 => { let left = get_column(input, state)?; let len = get_count(input, state)?; let mut values = Vec::with_capacity(len); for _ in 0..len { values.push(get_value(input, depth + 1, state)?); } RelationPredicate::In { left, values } },
    4 => RelationPredicate::Contains { left: get_column(input, state)?, right: get_value(input, depth + 1, state)? }, 5 => RelationPredicate::EnumMatch { column: get_column(input, state)?, case: get_string(input, state)?, payload: Box::new(get_predicate(input, depth + 1, state)?) },
    6 => { let len = get_count(input, state)?; let mut values = Vec::with_capacity(len); for _ in 0..len { values.push(get_predicate(input, depth + 1, state)?); } RelationPredicate::And(values) }, 7 => { let len = get_count(input, state)?; let mut values = Vec::with_capacity(len); for _ in 0..len { values.push(get_predicate(input, depth + 1, state)?); } RelationPredicate::Or(values) },
    8 => RelationPredicate::Not(Box::new(get_predicate(input, depth + 1, state)?)), 9 => RelationPredicate::True, 10 => RelationPredicate::False, _ => return Err(bad("predicate tag")),
}) }

// Relation tags: table=0 filter=1 union=2 join=3 project=4 gather=5 distinct=6 order=7 offset=8 limit=9.
fn put_expr(out: &mut Vec<u8>, value: &RelationExpr, depth: usize, state: &mut JState) -> CodecResult<()> { state.node(depth)?; match value {
    RelationExpr::TableScan { table, alias } => { out.push(0); put_string(out, table, state)?; match alias { None => out.push(0), Some(value) => { out.push(1); put_string(out, value, state)?; } }; Ok(()) },
    RelationExpr::Filter { input, predicate } => { out.push(1); put_expr(out, input, depth + 1, state)?; put_predicate(out, predicate, depth + 1, state) },
    RelationExpr::Union { inputs } => { out.push(2); put_count(out, inputs.len(), state)?; let mut labels = BTreeSet::new(); for arm in inputs { put_label(out, &arm.label, state)?; if !labels.insert(&arm.label) { return Err(bad("duplicate union label")); } put_expr(out, &arm.input, depth + 1, state)?; } Ok(()) },
    RelationExpr::Join { left, right, on, join_kind } => { out.push(3); put_expr(out, left, depth + 1, state)?; put_expr(out, right, depth + 1, state)?; out.push(match join_kind { RelationJoinKind::Inner => 0, RelationJoinKind::Left => 1 }); put_count(out, on.len(), state)?; for condition in on { put_column(out, &condition.left, state)?; put_column(out, &condition.right, state)?; } Ok(()) },
    RelationExpr::Project { input, columns } => { out.push(4); put_expr(out, input, depth + 1, state)?; put_count(out, columns.len(), state)?; for column in columns { put_string(out, &column.alias, state)?; put_project_expr(out, &column.expr, state)?; } Ok(()) },
    RelationExpr::Gather { seed, step, frontier_key, bound, dedupe_key } => { out.push(5); put_expr(out, seed, depth + 1, state)?; put_expr(out, step, depth + 1, state)?; put_key(out, frontier_key, state)?; match bound { RecursionBound::Fixpoint => out.push(0), RecursionBound::MaxDepth(value) => { out.push(1); jrq_put_len(out, *value); } }; put_count(out, dedupe_key.len(), state)?; for key in dedupe_key { put_key(out, key, state)?; } Ok(()) },
    RelationExpr::Distinct { input, key } => { out.push(6); put_expr(out, input, depth + 1, state)?; put_count(out, key.len(), state)?; for value in key { put_key(out, value, state)?; } Ok(()) },
    RelationExpr::OrderBy { input, terms } => { out.push(7); put_expr(out, input, depth + 1, state)?; put_count(out, terms.len(), state)?; for term in terms { put_column(out, &term.column, state)?; out.push(match term.direction { OrderDirection::Asc => 0, OrderDirection::Desc => 1 }); } Ok(()) },
    RelationExpr::Offset { input, offset } => { out.push(8); put_expr(out, input, depth + 1, state)?; jrq_put_len(out, *offset); Ok(()) }, RelationExpr::Limit { input, limit } => { out.push(9); put_expr(out, input, depth + 1, state)?; jrq_put_len(out, *limit); Ok(()) },
} }
fn get_expr(input: &mut &[u8], depth: usize, state: &mut JState) -> CodecResult<RelationExpr> { state.node(depth)?; Ok(match get_byte(input)? {
    0 => { let table = get_string(input, state)?; let alias = match get_byte(input)? { 0 => None, 1 => Some(get_string(input, state)?), _ => return Err(bad("alias tag")) }; RelationExpr::TableScan { table, alias } },
    1 => RelationExpr::Filter { input: Box::new(get_expr(input, depth + 1, state)?), predicate: get_predicate(input, depth + 1, state)? },
    2 => { let len = get_count(input, state)?; let mut labels = BTreeSet::new(); let mut inputs = Vec::with_capacity(len); for _ in 0..len { let label = get_label(input, state)?; if !labels.insert(label.clone()) { return Err(bad("duplicate union label")); } inputs.push(RelationUnionArm { label, input: get_expr(input, depth + 1, state)? }); } RelationExpr::Union { inputs } },
    3 => { let left = Box::new(get_expr(input, depth + 1, state)?); let right = Box::new(get_expr(input, depth + 1, state)?); let join_kind = match get_byte(input)? { 0 => RelationJoinKind::Inner, 1 => RelationJoinKind::Left, _ => return Err(bad("join tag")) }; let len = get_count(input, state)?; let mut on = Vec::with_capacity(len); for _ in 0..len { on.push(RelationJoinCondition { left: get_column(input, state)?, right: get_column(input, state)? }); } RelationExpr::Join { left, right, on, join_kind } },
    4 => { let relation_input = Box::new(get_expr(input, depth + 1, state)?); let len = get_count(input, state)?; let mut columns = Vec::with_capacity(len); for _ in 0..len { columns.push(RelationProjectColumn { alias: get_string(input, state)?, expr: get_project_expr(input, state)? }); } RelationExpr::Project { input: relation_input, columns } },
    5 => { let seed = Box::new(get_expr(input, depth + 1, state)?); let step = Box::new(get_expr(input, depth + 1, state)?); let frontier_key = get_key(input, state)?; let bound = match get_byte(input)? { 0 => RecursionBound::Fixpoint, 1 => RecursionBound::MaxDepth(get_len(input)?), _ => return Err(bad("recursion bound tag")) }; let len = get_count(input, state)?; let mut dedupe_key = Vec::with_capacity(len); for _ in 0..len { dedupe_key.push(get_key(input, state)?); } RelationExpr::Gather { seed, step, frontier_key, bound, dedupe_key } },
    6 => { let relation_input = Box::new(get_expr(input, depth + 1, state)?); let len = get_count(input, state)?; let mut key = Vec::with_capacity(len); for _ in 0..len { key.push(get_key(input, state)?); } RelationExpr::Distinct { input: relation_input, key } },
    7 => { let relation_input = Box::new(get_expr(input, depth + 1, state)?); let len = get_count(input, state)?; let mut terms = Vec::with_capacity(len); for _ in 0..len { let column = get_column(input, state)?; let direction = match get_byte(input)? { 0 => OrderDirection::Asc, 1 => OrderDirection::Desc, _ => return Err(bad("order direction tag")) }; terms.push(RelationOrderBy { column, direction }); } RelationExpr::OrderBy { input: relation_input, terms } },
    8 => RelationExpr::Offset { input: Box::new(get_expr(input, depth + 1, state)?), offset: get_len(input)? }, 9 => RelationExpr::Limit { input: Box::new(get_expr(input, depth + 1, state)?), limit: get_len(input)? }, _ => return Err(bad("expression tag")),
}) }

/// Encode a relation query into canonical JRQ v1 bytes.
pub fn encode_relation_query_v1(query: &RelationQuery) -> CodecResult<Vec<u8>> { let mut out = MAGIC.to_vec(); put_expr(&mut out, &query.rel, 0, &mut JState::default())?; if out.len() > MAX_BYTES { return Err(bad("too large")); } Ok(out) }
/// Decode exactly one JRQ v1 query, rejecting unknown and trailing bytes.
pub fn decode_relation_query_v1_exact(bytes: &[u8]) -> CodecResult<RelationQuery> {
    if bytes.len() > MAX_BYTES { return Err(bad("too large")); }
    else if !bytes.starts_with(MAGIC) { return Err(bad("version")); }
    let mut input = &bytes[MAGIC.len()..];
    let rel = get_expr(&mut input, 0, &mut JState::default())?;
    if !input.is_empty() { return Err(bad("trailing bytes")); }
    Ok(RelationQuery { rel })
}

#[cfg(test)]
mod relation_codec_tests {
    use super::*;

    fn col(name: &str) -> RelationColumnRef { RelationColumnRef { scope: Some("source".into()), column: name.into() } }
    fn scan(name: &str) -> RelationExpr { RelationExpr::TableScan { table: name.into(), alias: Some("source".into()) } }
    fn round_trip(rel: RelationExpr) {
        let query = RelationQuery { rel };
        let bytes = encode_relation_query_v1(&query).unwrap();
        assert_eq!(decode_relation_query_v1_exact(&bytes).unwrap(), query);
        assert_eq!(encode_relation_query_v1(&decode_relation_query_v1_exact(&bytes).unwrap()).unwrap(), bytes);
    }

    #[test]
    fn jrq_v1_round_trips_all_expression_predicate_and_value_variants() {
        let literal = RelationValueRef::Literal(serde_json::json!({"a": [null, true, -4, 1.5], "z": "text"}));
        let values = vec![literal.clone(), RelationValueRef::Param("p".into()), RelationValueRef::SessionRef(vec!["actor".into(), "role".into()]), RelationValueRef::OuterColumn(col("outer")), RelationValueRef::FrontierColumn(col("frontier")), RelationValueRef::RowId(RelationRowIdRef::Outer)];
        let predicates = vec![RelationPredicate::Cmp { left: col("cmp"), op: RelationCmpOp::Ge, right: literal.clone() }, RelationPredicate::IsNull { column: col("null") }, RelationPredicate::IsNotNull { column: col("not_null") }, RelationPredicate::In { left: col("in"), values }, RelationPredicate::Contains { left: col("contains"), right: RelationValueRef::RowId(RelationRowIdRef::Frontier) }, RelationPredicate::EnumMatch { column: col("kind"), case: "Case".into(), payload: Box::new(RelationPredicate::True) }, RelationPredicate::And(vec![RelationPredicate::True, RelationPredicate::False]), RelationPredicate::Or(vec![RelationPredicate::True, RelationPredicate::False]), RelationPredicate::Not(Box::new(RelationPredicate::True)), RelationPredicate::True, RelationPredicate::False];
        for predicate in predicates { round_trip(RelationExpr::Filter { input: Box::new(scan("rows")), predicate }); }
        round_trip(RelationExpr::Union { inputs: vec![RelationUnionArm { label: "one".into(), input: scan("one") }, RelationUnionArm { label: "two".into(), input: scan("two") }] });
        round_trip(RelationExpr::Join { left: Box::new(scan("left")), right: Box::new(scan("right")), on: vec![RelationJoinCondition { left: col("left"), right: col("right") }], join_kind: RelationJoinKind::Left });
        round_trip(RelationExpr::Project { input: Box::new(scan("rows")), columns: vec![RelationProjectColumn { alias: "column".into(), expr: RelationProjectExpr::Column(col("column")) }, RelationProjectColumn { alias: "row".into(), expr: RelationProjectExpr::RowId(RelationRowIdRef::Current) }] });
        round_trip(RelationExpr::Gather { seed: Box::new(scan("seed")), step: Box::new(scan("step")), frontier_key: RelationKeyRef::Column(col("frontier")), bound: RecursionBound::MaxDepth(3), dedupe_key: vec![RelationKeyRef::Column(col("dedupe")), RelationKeyRef::RowId(RelationRowIdRef::Frontier)] });
        round_trip(RelationExpr::Distinct { input: Box::new(scan("rows")), key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)] });
        round_trip(RelationExpr::OrderBy { input: Box::new(scan("rows")), terms: vec![RelationOrderBy { column: col("name"), direction: OrderDirection::Desc }] });
        round_trip(RelationExpr::Offset { input: Box::new(scan("rows")), offset: 17 });
        round_trip(RelationExpr::Limit { input: Box::new(scan("rows")), limit: 19 });
    }

    #[test]
    fn jrq_v1_rejects_noncanonical_and_invalid_wire_forms() {
        let valid = encode_relation_query_v1(&RelationQuery { rel: scan("rows") }).unwrap();
        assert!(decode_relation_query_v1_exact(&[valid, vec![0]].concat()).is_err());
        assert!(decode_relation_query_v1_exact(b"JRQ\x02").is_err());
        assert!(decode_relation_query_v1_exact(b"JRQ\x01\x00\x81\x00a\x00").is_err());
        let duplicate = RelationQuery { rel: RelationExpr::Union { inputs: vec![RelationUnionArm { label: "x".into(), input: scan("a") }, RelationUnionArm { label: "x".into(), input: scan("b") }] } };
        assert!(encode_relation_query_v1(&duplicate).is_err());
        let nul = RelationQuery { rel: RelationExpr::Union { inputs: vec![RelationUnionArm { label: "x\0".into(), input: scan("a") }] } };
        assert!(encode_relation_query_v1(&nul).is_err());
    }
}
