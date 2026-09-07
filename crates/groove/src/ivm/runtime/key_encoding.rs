//! Index scan bounds, comparison semantics, and order-preserving key encoding.

use super::*;

fn resolved_record_value(
    record: BorrowedRecord<'_>,
    field: &str,
) -> Result<Value, IvmRuntimeError> {
    let index = super::record_projection::resolve_field_name(&record.descriptor(), field)
        .ok_or_else(|| records::Error::FieldNotFound(field.to_owned()))?;
    record.get_idx(index).map_err(Into::into)
}

pub(crate) fn durable_index_key_prefix(table: &str, index: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    // NUL separators keep table/index names prefix-decodable without escaping.
    prefix.extend(table.as_bytes());
    prefix.push(0);
    prefix.extend(index.as_bytes());
    prefix.push(0);
    prefix
}

pub(super) fn encode_ordered_bytes_without_terminal(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
}

pub(super) fn primary_key_value_bytes(
    descriptor: &RecordDescriptor,
    record: &[u8],
    primary_key_field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut bytes = Vec::new();
    for primary_key_field_idx in primary_key_field_indices {
        encode_record_field_key_part(&mut bytes, descriptor, record, *primary_key_field_idx)?;
    }
    Ok(bytes)
}

pub(super) fn primary_key_field_indices(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
) -> Result<Vec<usize>, IvmRuntimeError> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
    primary_key
        .columns
        .iter()
        .map(|column| {
            descriptor
                .field_index(&column.column)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
        })
        .collect()
}

pub(super) fn encode_record_field_key_part(
    key: &mut Vec<u8>,
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<(), IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    let borrowed = descriptor.bind(record);
    match &field.value_type {
        ValueType::U8 => {
            key.push(0);
            key.push(borrowed.get_u8(field_idx)?);
            Ok(())
        }
        ValueType::U32 => {
            key.push(2);
            key.extend(borrowed.get_u32(field_idx)?.to_be_bytes());
            Ok(())
        }
        ValueType::U64 => {
            key.push(3);
            key.extend(borrowed.get_u64(field_idx)?.to_be_bytes());
            Ok(())
        }
        ValueType::I32 => {
            key.push(14);
            key.extend(order_preserving_i32_bits(borrowed.get_i32(field_idx)?).to_be_bytes());
            Ok(())
        }
        ValueType::I64 => {
            key.push(13);
            key.extend(order_preserving_i64_bits(borrowed.get_i64(field_idx)?).to_be_bytes());
            Ok(())
        }
        ValueType::F64 => {
            let value = borrowed.get_f64(field_idx)?;
            if value.is_nan() {
                return Err(IvmRuntimeError::RecordEncoding(
                    records::Error::InvalidF64NaN,
                ));
            }
            key.push(4);
            key.extend(order_preserving_f64_bits(value).to_be_bytes());
            Ok(())
        }
        ValueType::Bool => {
            key.push(5);
            key.push(u8::from(borrowed.get_bool(field_idx)?));
            Ok(())
        }
        ValueType::String => {
            key.push(6);
            encode_ordered_bytes(key, borrowed.get_str(field_idx)?.as_bytes());
            Ok(())
        }
        ValueType::Bytes => {
            key.push(7);
            encode_ordered_bytes(key, borrowed.get_bytes(field_idx)?);
            Ok(())
        }
        ValueType::Uuid => {
            key.push(10);
            key.extend_from_slice(borrowed.get_uuid(field_idx)?.as_bytes());
            Ok(())
        }
        ValueType::EnumTag(_) => {
            let value = borrowed.get_enum(field_idx)?;
            encode_key_part(key, &Value::U8(value))
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::U64) => {
            match borrowed.get_nullable_u64(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(3);
                    key.extend(value.to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::I64) => {
            match borrowed.get_nullable_i64(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(13);
                    key.extend(order_preserving_i64_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::I32) => {
            match borrowed.get_nullable_i32(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(14);
                    key.extend(order_preserving_i32_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::F64) => {
            match borrowed.get_nullable_f64(field_idx)? {
                Some(value) => {
                    if value.is_nan() {
                        return Err(IvmRuntimeError::RecordEncoding(
                            records::Error::InvalidF64NaN,
                        ));
                    }
                    key.push(9);
                    key.push(4);
                    key.extend(order_preserving_f64_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::String) => {
            match borrowed.get_nullable_string(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(6);
                    encode_ordered_bytes(key, value.as_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Bytes) => {
            match borrowed.get_nullable_bytes(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(7);
                    encode_ordered_bytes(key, value);
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Uuid) => {
            match borrowed.get_nullable_uuid(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(10);
                    key.extend_from_slice(value.as_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::EnumTag(_)) => {
            match borrowed.get_nullable_enum(field_idx)? {
                Some(value) => {
                    encode_key_part(key, &Value::Nullable(Some(Box::new(Value::U8(value)))))
                }
                None => encode_key_part(key, &Value::Nullable(None)),
            }
        }
        _ => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let field_idx = super::record_projection::resolve_field_name(descriptor, field_name)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()))?;
            let value = descriptor.bind(record).get_idx(field_idx)?;
            encode_key_part(key, &value)
        }
    }
}

pub(super) fn record_field_key_parts(
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    match &field.value_type {
        ValueType::Array(_) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let field_idx = super::record_projection::resolve_field_name(descriptor, field_name)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()))?;
            let Value::Array(values) = descriptor.bind(record).get_idx(field_idx)? else {
                return Err(IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()));
            };
            values
                .into_iter()
                .map(|value| {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(key)
                })
                .collect()
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let field_idx = super::record_projection::resolve_field_name(descriptor, field_name)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()))?;
            match descriptor.bind(record).get_idx(field_idx)? {
                Value::Nullable(None) => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &Value::Nullable(None))?;
                    Ok(vec![key])
                }
                Value::Nullable(Some(value)) => match *value {
                    Value::Array(values) => values
                        .into_iter()
                        .map(|value| {
                            let mut key = Vec::new();
                            encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                            Ok(key)
                        })
                        .collect(),
                    value => {
                        let mut key = Vec::new();
                        encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                        Ok(vec![key])
                    }
                },
                value => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(vec![key])
                }
            }
        }
        _ => {
            let mut key = Vec::new();
            encode_record_field_key_part(&mut key, descriptor, record, field_idx)?;
            Ok(vec![key])
        }
    }
}

pub(super) fn compare_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let field_idx = super::record_projection::resolve_field_name(&record.descriptor(), field)
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
    match record_field_literal_ordering(record, field_idx, value)? {
        FieldLiteralOrdering::Compared(ordering) => return Ok(predicate(ordering)),
        FieldLiteralOrdering::SqlNull => return Ok(false),
        FieldLiteralOrdering::Unsupported => {}
    }
    let value = value.to_value();
    let actual = resolved_record_value(record, field)?;
    Ok(compare_values_sql(&actual, &value, comparison).is_some_and(predicate))
}

pub(super) enum FieldLiteralOrdering {
    Compared(std::cmp::Ordering),
    SqlNull,
    Unsupported,
}

fn record_field_literal_ordering(
    record: BorrowedRecord<'_>,
    field_idx: usize,
    value: &LiteralValue,
) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
    let field = record.field(field_idx)?;
    match (&field.value_type, value) {
        (ValueType::U8, LiteralValue::U8(expected)) => {
            Ok(ordering(&record.get_u8(field_idx)?, expected))
        }
        (ValueType::U32, LiteralValue::U32(expected)) => {
            Ok(ordering(&record.get_u32(field_idx)?, expected))
        }
        (ValueType::U64, LiteralValue::U64(expected)) => {
            Ok(ordering(&record.get_u64(field_idx)?, expected))
        }
        (ValueType::I32, LiteralValue::I32(expected)) => {
            Ok(ordering(&record.get_i32(field_idx)?, expected))
        }
        (ValueType::I64, LiteralValue::I64(expected)) => {
            Ok(ordering(&record.get_i64(field_idx)?, expected))
        }
        (ValueType::F64, LiteralValue::F64(expected)) => {
            let expected = f64::from_bits(*expected);
            Ok(record
                .get_f64(field_idx)?
                .partial_cmp(&expected)
                .map(FieldLiteralOrdering::Compared)
                .unwrap_or(FieldLiteralOrdering::SqlNull))
        }
        (ValueType::Bool, LiteralValue::Bool(expected)) => {
            Ok(ordering(&record.get_bool(field_idx)?, expected))
        }
        (ValueType::String, LiteralValue::String(expected)) => {
            Ok(ordering(record.get_str(field_idx)?, expected.as_str()))
        }
        (ValueType::Bytes, LiteralValue::Bytes(expected)) => {
            Ok(ordering(record.get_bytes(field_idx)?, expected.as_slice()))
        }
        (ValueType::Uuid, LiteralValue::Uuid(expected)) => Ok(record
            .get_uuid(field_idx)?
            .as_bytes()
            .partial_cmp(expected.as_bytes())
            .map(FieldLiteralOrdering::Compared)
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::EnumTag(_), LiteralValue::EnumTag(expected)) => {
            Ok(ordering(&record.get_enum(field_idx)?, expected))
        }
        (ValueType::Nullable(inner), LiteralValue::Nullable(Some(expected))) => {
            nullable_record_field_literal_ordering(record, field_idx, inner, expected)
        }
        (ValueType::Nullable(_), LiteralValue::Nullable(None)) => Ok(FieldLiteralOrdering::SqlNull),
        _ => Ok(FieldLiteralOrdering::Unsupported),
    }
}

fn nullable_record_field_literal_ordering(
    record: BorrowedRecord<'_>,
    field_idx: usize,
    inner: &ValueType,
    expected: &LiteralValue,
) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
    match (inner, expected) {
        (ValueType::U64, LiteralValue::U64(expected)) => Ok(record
            .get_nullable_u64(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::I64, LiteralValue::I64(expected)) => Ok(record
            .get_nullable_i64(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::I32, LiteralValue::I32(expected)) => Ok(record
            .get_nullable_i32(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::F64, LiteralValue::F64(expected)) => {
            let expected = f64::from_bits(*expected);
            Ok(record
                .get_nullable_f64(field_idx)?
                .and_then(|actual| actual.partial_cmp(&expected))
                .map(FieldLiteralOrdering::Compared)
                .unwrap_or(FieldLiteralOrdering::SqlNull))
        }
        (ValueType::String, LiteralValue::String(expected)) => Ok(record
            .get_nullable_string(field_idx)?
            .map(|actual| ordering(actual, expected.as_str()))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::Bytes, LiteralValue::Bytes(expected)) => Ok(record
            .get_nullable_bytes(field_idx)?
            .map(|actual| ordering(actual, expected.as_slice()))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::Uuid, LiteralValue::Uuid(expected)) => Ok(record
            .get_nullable_uuid(field_idx)?
            .and_then(|actual| actual.as_bytes().partial_cmp(expected.as_bytes()))
            .map(FieldLiteralOrdering::Compared)
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::EnumTag(_), LiteralValue::EnumTag(expected)) => Ok(record
            .get_nullable_enum(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        _ => Ok(FieldLiteralOrdering::Unsupported),
    }
}

pub(super) fn ordering<T: PartialOrd + ?Sized>(actual: &T, expected: &T) -> FieldLiteralOrdering {
    actual
        .partial_cmp(expected)
        .map(FieldLiteralOrdering::Compared)
        .unwrap_or(FieldLiteralOrdering::SqlNull)
}

pub(super) fn compare_record_fields(
    record: BorrowedRecord<'_>,
    field: &str,
    value_field: &str,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let left = resolved_record_value(record, field)?;
    let right = resolved_record_value(record, value_field)?;
    Ok(compare_values_sql(&left, &right, comparison).is_some_and(predicate))
}

pub(super) fn contains_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let needle = value.to_value();
    let haystack = resolved_record_value(record, field)?;
    Ok(value_contains_sql(&haystack, &needle, comparison))
}

pub(super) fn contains_record_field_value(
    record: BorrowedRecord<'_>,
    field: &str,
    needle_field: &str,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let haystack = resolved_record_value(record, field)?;
    let needle = resolved_record_value(record, needle_field)?;
    Ok(value_contains_sql(&haystack, &needle, comparison))
}

fn value_contains_sql(left: &Value, right: &Value, comparison: ValueComparison) -> bool {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => false,
        (Value::Nullable(Some(left)), right) => value_contains_sql(left, right, comparison),
        (left, Value::Nullable(Some(right))) => value_contains_sql(left, right, comparison),
        (Value::String(left), Value::String(right)) => left.contains(right),
        (Value::Array(values), right) => values.iter().any(|value| {
            compare_values_sql(value, right, comparison).is_some_and(std::cmp::Ordering::is_eq)
        }),
        _ => false,
    }
}

pub(super) fn compare_values_sql(
    left: &Value,
    right: &Value,
    comparison: ValueComparison,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => None,
        (Value::Nullable(Some(left)), right) => compare_values_sql(left, right, comparison),
        (left, Value::Nullable(Some(right))) => compare_values_sql(left, right, comparison),
        _ => compare_values(left, right, comparison),
    }
}

pub(super) fn is_sql_null_value(value: &Value) -> bool {
    match value {
        Value::Nullable(None) => true,
        Value::Nullable(Some(value)) => is_sql_null_value(value),
        _ => false,
    }
}

fn compare_values(
    left: &Value,
    right: &Value,
    comparison: ValueComparison,
) -> Option<std::cmp::Ordering> {
    if matches!(comparison, ValueComparison::Policy)
        && let (Some(left), Some(right)) = (integer_value(left), integer_value(right))
    {
        // i128 represents every supported integer exactly, including U64
        // values above i64::MAX. Floats deliberately do not participate: a
        // numeric-width match must never turn into lossy integer/float equality.
        return left.partial_cmp(&right);
    }
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.partial_cmp(right),
        (Value::U16(left), Value::U16(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::I32(left), Value::I32(right)) => left.partial_cmp(right),
        (Value::I64(left), Value::I64(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        (Value::EnumTag(left), Value::EnumTag(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.partial_cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().partial_cmp(right.as_bytes()),
        (Value::Tuple(left), Value::Tuple(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right, comparison))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right, comparison))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        _ => None,
    }
}

fn integer_value(value: &Value) -> Option<i128> {
    match value {
        Value::U8(value) => Some(i128::from(*value)),
        Value::U16(value) => Some(i128::from(*value)),
        Value::U32(value) => Some(i128::from(*value)),
        Value::U64(value) => Some(i128::from(*value)),
        Value::I32(value) => Some(i128::from(*value)),
        Value::I64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

pub(super) fn join_descriptor(
    left: &RecordDescriptor,
    right: &RecordDescriptor,
) -> RecordDescriptor {
    let fields = left
        .fields()
        .iter()
        .filter_map(|field| {
            Some((
                format!("left.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        })
        .chain(right.fields().iter().filter_map(|field| {
            Some((
                format!("right.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        }))
        .collect::<Vec<_>>();

    RecordDescriptor::new(fields)
}

pub(crate) fn encode_key_part(key: &mut Vec<u8>, value: &Value) -> Result<(), IvmRuntimeError> {
    // Type tags make composite keys unambiguous. Payload bytes are chosen to
    // preserve natural ordering in RocksDB's lexicographic iterator order.
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I32(value) => {
            key.push(14);
            key.extend(order_preserving_i32_bits(*value).to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::F64(value) => {
            if value.is_nan() {
                return Err(IvmRuntimeError::RecordEncoding(
                    records::Error::InvalidF64NaN,
                ));
            }
            key.push(4);
            key.extend(order_preserving_f64_bits(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_key_part(key, value)?;
            }
        }
        Value::EnumTag(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Nullable(None) => {
            key.push(8);
        }
        Value::Nullable(Some(value)) => {
            key.push(9);
            encode_key_part(key, value)?;
        }
        Value::Array(_) | Value::Record(_) | Value::Enum(_) | Value::Large(_) => {
            return Err(IvmRuntimeError::UnsupportedJoinKey);
        }
    }
    Ok(())
}

fn order_preserving_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    // Flip positive signs and invert negatives; the resulting unsigned integer
    // sorts like IEEE numeric order for non-NaN values.
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

pub(super) fn order_preserving_i64_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

pub(super) fn order_preserving_i32_bits(value: i32) -> u32 {
    (value as u32) ^ (1_u32 << 31)
}

pub(super) fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    // 0x00 terminates the byte string; embedded NULs are escaped as 00 ff.
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

pub(super) fn project_binding_source_deltas(
    input: &RecordDeltas,
    output_desc: &RecordDescriptor,
) -> Result<RecordDeltas, IvmRuntimeError> {
    if input.descriptor == *output_desc {
        return Ok(input.clone());
    }
    let mapping = output_desc
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_ref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            resolve_field_ref(&input.descriptor, &FieldRef::stored_name(name))
                .map(|index| (0, index))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deltas = input
        .deltas
        .iter()
        .map(|delta| {
            Ok(RecordDelta {
                record: output_desc
                    .project_record_raw(
                        std::slice::from_ref(&input.descriptor),
                        &[delta.raw()],
                        &mapping,
                    )?
                    .into(),
                weight: delta.weight,
            })
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    Ok(RecordDeltas {
        descriptor: *output_desc,
        deltas,
    })
}

pub(super) fn consolidate_deltas(deltas: Vec<RecordDelta>) -> Vec<RecordDelta> {
    if deltas.len() <= 1 {
        return deltas;
    }
    let mut deltas = deltas;
    deltas.sort_unstable_by(|left, right| left.record.cmp(&right.record));
    let mut consolidated = Vec::<RecordDelta>::with_capacity(deltas.len());
    for delta in deltas {
        if let Some(last) = consolidated.last_mut()
            && last.record == delta.record
        {
            last.weight += delta.weight;
            continue;
        }
        consolidated.push(delta);
    }
    consolidated
        .into_iter()
        .filter(|delta| delta.weight != 0)
        .collect()
}
