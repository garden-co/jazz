/// Decode a relation literal. Queries and relation-backed policies share this
/// so a literal means the same value in both.
///
/// A literal is either a bare JSON value from a query builder or the typed
/// `{"type", "value"}` envelope of a public
/// [`crate::model::public_schema::Value`].
pub(crate) fn relation_literal_to_record_value(value: &serde_json::Value) -> Result<Value, String> {
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
                Err("relation literal is not finite".to_owned())
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(relation_literal_to_record_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        serde_json::Value::Object(_) => {
            let value = serde_json::from_value(value.clone())
                .map_err(|error| format!("object relation literals must be typed values: {error}"))?;
            public_value_to_record_value(&value)
        }
    }
}

/// Lower a typed public value to the record value comparisons evaluate.
pub(crate) fn public_value_to_record_value(
    value: &crate::model::public_schema::Value,
) -> Result<Value, String> {
    use crate::model::public_schema::Value as PublicValue;
    match value {
        PublicValue::Null => Ok(Value::Nullable(None)),
        PublicValue::Boolean(value) => Ok(Value::Bool(*value)),
        PublicValue::Text(value) => Ok(Value::String(value.clone())),
        PublicValue::Integer(value) => Ok(Value::I32(*value)),
        PublicValue::BigInt(value) => Ok(Value::I64(*value)),
        PublicValue::Double(value) if value.is_finite() => Ok(Value::F64(*value)),
        PublicValue::Double(_) => Err("floating-point literals must be finite numbers".to_owned()),
        PublicValue::Timestamp(value) => Ok(Value::U64(*value)),
        PublicValue::Uuid(value) => Ok(Value::Uuid(*value.uuid())),
        PublicValue::Bytea(value) => Ok(Value::Bytes(value.clone())),
        PublicValue::Array(values) => values
            .iter()
            .map(public_value_to_record_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        other => Err(format!("{other:?} literals are not supported yet")),
    }
}

/// Lower one column comparison. A null right-hand side tests nullness, as the
/// TypeScript builders emit it; ordering against null has no meaning.
pub(crate) fn relation_comparison(
    column: &str,
    op: RelationCmpOp,
    right: Operand,
) -> Result<Predicate, String> {
    let left = Operand::Column(column.to_owned());
    if matches!(right, Operand::Literal(Value::Nullable(None))) {
        return match op {
            RelationCmpOp::Eq => Ok(Predicate::IsNull(left)),
            RelationCmpOp::Ne => Ok(Predicate::Not(Box::new(Predicate::IsNull(left)))),
            RelationCmpOp::Lt | RelationCmpOp::Le | RelationCmpOp::Gt | RelationCmpOp::Ge => {
                Err("Ordering comparisons against null are not supported yet".to_owned())
            }
        };
    }
    Ok(match op {
        RelationCmpOp::Eq => Predicate::Eq(left, right),
        RelationCmpOp::Ne => Predicate::Ne(left, right),
        RelationCmpOp::Lt => Predicate::Lt(left, right),
        RelationCmpOp::Le => Predicate::Lte(left, right),
        RelationCmpOp::Gt => Predicate::Gt(left, right),
        RelationCmpOp::Ge => Predicate::Gte(left, right),
    })
}
