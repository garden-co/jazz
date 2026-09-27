//! Canonical representation of values bound into prepared query sources.

use groove::records::Value;

/// Canonicalizes a bound value to the descriptor representation used by a
/// prepared Groove binding source. Lowering literal-only routed terminals
/// uses the same conversion so their route predicates compare like-for-like.
pub(crate) fn coerce_prepared_binding_value(
    value: Value,
    column_type: &groove::schema::ColumnType,
) -> Value {
    if let Some(value) = coerce_prepared_integer_value(&value, column_type) {
        return value;
    }
    match (value, column_type) {
        (Value::Uuid(value), groove::schema::ColumnType::String) => {
            Value::String(value.to_string())
        }
        (Value::String(value), groove::schema::ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(value), groove::schema::ColumnType::Nullable(inner)) => Value::Nullable(
            value.map(|value| Box::new(coerce_prepared_binding_value(*value, inner))),
        ),
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_prepared_binding_value(*value, column_type),
        ))),
        (value @ Value::Nullable(None), _) => value,
        (Value::Array(values), groove::schema::ColumnType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| coerce_prepared_binding_value(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), groove::schema::ColumnType::Tuple(types))
            if values.len() == types.len() =>
        {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_prepared_binding_value(value, column_type))
                    .collect(),
            )
        }
        (value, groove::schema::ColumnType::Nullable(inner))
            if !matches!(value, Value::Nullable(_)) =>
        {
            Value::Nullable(Some(Box::new(coerce_prepared_binding_value(value, inner))))
        }
        (value, _) => value,
    }
}

/// Normalizes prepared integer values. Failed conversions intentionally return
/// `None`, so the original typed value stays in the binding and cannot wrap
/// into an authorized value.
fn coerce_prepared_integer_value(
    value: &Value,
    column_type: &groove::schema::ColumnType,
) -> Option<Value> {
    let value = match value {
        Value::U8(value) => i128::from(*value),
        Value::U16(value) => i128::from(*value),
        Value::U32(value) => i128::from(*value),
        Value::U64(value) => i128::from(*value),
        Value::I32(value) => i128::from(*value),
        Value::I64(value) => i128::from(*value),
        _ => return None,
    };
    match column_type {
        groove::schema::ColumnType::U8 => u8::try_from(value).ok().map(Value::U8),
        groove::schema::ColumnType::U16 => u16::try_from(value).ok().map(Value::U16),
        groove::schema::ColumnType::U32 => u32::try_from(value).ok().map(Value::U32),
        groove::schema::ColumnType::U64 => u64::try_from(value).ok().map(Value::U64),
        groove::schema::ColumnType::I32 => i32::try_from(value).ok().map(Value::I32),
        groove::schema::ColumnType::I64 => i64::try_from(value).ok().map(Value::I64),
        _ => None,
    }
}
