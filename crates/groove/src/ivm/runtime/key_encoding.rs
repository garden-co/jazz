//! Index scan bounds, comparison semantics, and order-preserving key encoding.

use super::*;

/// A predicate reads fields, not necessarily a materialized intermediate row.
/// Implementations retain the same typed comparison and SQL-null semantics.
pub(super) trait PredicateRecord: Copy {
    fn value(self, field: &str) -> Result<Value, IvmRuntimeError>;
    fn literal_ordering(
        self,
        field: &str,
        value: &LiteralValue,
    ) -> Result<FieldLiteralOrdering, IvmRuntimeError>;
}

impl PredicateRecord for BorrowedRecord<'_> {
    fn value(self, field: &str) -> Result<Value, IvmRuntimeError> {
        let index = super::record_projection::resolve_field_name(&self.descriptor(), field)
            .ok_or_else(|| records::Error::FieldNotFound(field.to_owned()))?;
        self.get_idx(index).map_err(Into::into)
    }

    fn literal_ordering(
        self,
        field: &str,
        value: &LiteralValue,
    ) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
        let index = super::record_projection::resolve_field_name(&self.descriptor(), field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
        record_field_literal_ordering(self, index, value)
    }
}

fn resolved_record_value(
    record: impl PredicateRecord,
    field: &str,
) -> Result<Value, IvmRuntimeError> {
    record.value(field)
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
    let i = field_idx;
    // Typed reads skip materializing a `Value`; the bytes are exactly those
    // `encode_key_part` writes for the same value.
    match &field.value_type {
        ValueType::U8 => put_u8(key, borrowed.get_u8(i)?),
        ValueType::U32 => put_u32(key, borrowed.get_u32(i)?),
        ValueType::U64 => put_u64(key, borrowed.get_u64(i)?),
        ValueType::U48 => put_u48(key, borrowed.get_u48(i)?),
        ValueType::I32 => put_i32(key, borrowed.get_i32(i)?),
        ValueType::I64 => put_i64(key, borrowed.get_i64(i)?),
        ValueType::F64 => put_f64(key, non_nan(borrowed.get_f64(i)?)?),
        ValueType::Bool => put_bool(key, borrowed.get_bool(i)?),
        ValueType::String => put_str(key, borrowed.get_str(i)?),
        ValueType::Bytes => put_bytes(key, borrowed.get_bytes(i)?),
        ValueType::Uuid => put_uuid(key, borrowed.get_uuid(i)?),
        ValueType::EnumTag(_) => put_u8(key, borrowed.get_enum(i)?),
        ValueType::Nullable(inner) => match inner.as_ref() {
            ValueType::U64 => put_nullable(key, borrowed.get_nullable_u64(i)?, put_u64),
            ValueType::I64 => put_nullable(key, borrowed.get_nullable_i64(i)?, put_i64),
            ValueType::I32 => put_nullable(key, borrowed.get_nullable_i32(i)?, put_i32),
            ValueType::F64 => {
                let value = borrowed.get_nullable_f64(i)?.map(non_nan).transpose()?;
                put_nullable(key, value, put_f64)
            }
            ValueType::String => put_nullable(key, borrowed.get_nullable_string(i)?, put_str),
            ValueType::Bytes => put_nullable(key, borrowed.get_nullable_bytes(i)?, put_bytes),
            ValueType::Uuid => put_nullable(key, borrowed.get_nullable_uuid(i)?, put_uuid),
            ValueType::EnumTag(_) => put_nullable(key, borrowed.get_nullable_enum(i)?, put_u8),
            _ => encode_key_part(key, &named_field_value(descriptor, record, field_idx)?),
        },
        _ => encode_key_part(key, &named_field_value(descriptor, record, field_idx)?),
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
    if !expands_to_key_parts(&field.value_type) {
        let mut key = Vec::new();
        encode_record_field_key_part(&mut key, descriptor, record, field_idx)?;
        return Ok(vec![key]);
    }
    key_parts(named_field_value(descriptor, record, field_idx)?)
        .iter()
        .map(|part| {
            let mut key = Vec::new();
            encode_key_part(&mut key, part)?;
            Ok(key)
        })
        .collect()
}

/// Whether a field of this type is indexed once per array element.
pub(super) fn expands_to_key_parts(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::Array(_) => true,
        ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Array(_)),
        _ => false,
    }
}

/// The values a record is keyed under for one field: each element of an
/// array (nullable arrays keep their elements nullable), else the value.
pub(super) fn key_parts(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        Value::Nullable(Some(value)) => match *value {
            Value::Array(values) => values
                .into_iter()
                .map(|value| Value::Nullable(Some(Box::new(value))))
                .collect(),
            value => vec![Value::Nullable(Some(Box::new(value)))],
        },
        value => vec![value],
    }
}

/// Read field `field_idx` through its name, so a renamed or aliased field
/// resolves exactly as predicates resolve it.
pub(super) fn named_field_value(
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<Value, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    let field_name = field
        .name
        .as_deref()
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
    let field_idx = super::record_projection::resolve_field_name(descriptor, field_name)
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()))?;
    Ok(descriptor.bind(record).get_idx(field_idx)?)
}

/// Every composite key of a record: `prefix` followed by one key part of
/// each field, in field order, without duplicates. A field with no parts (an
/// empty array) yields no keys at all.
pub(super) fn composite_keys(
    prefix: Vec<u8>,
    field_values: impl IntoIterator<Item = Result<Value, IvmRuntimeError>>,
    mut encode: impl FnMut(&mut Vec<u8>, &Value) -> Result<(), IvmRuntimeError>,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let mut keys = vec![prefix];
    let mut seen = rustc_hash::FxHashSet::default();
    for value in field_values {
        let parts = key_parts(value?);
        if let ([key], [part]) = (keys.as_mut_slice(), parts.as_slice()) {
            // One key and one part cannot produce a duplicate.
            encode(key, part)?;
            continue;
        }
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for part in &parts {
                let mut next = key.clone();
                encode(&mut next, part)?;
                if seen.insert(next.clone()) {
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }
    Ok(keys)
}

pub(super) fn compare_record_field(
    record: impl PredicateRecord,
    field: &str,
    value: &LiteralValue,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    match record.literal_ordering(field, value)? {
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

pub(super) fn record_field_literal_ordering(
    record: BorrowedRecord<'_>,
    field_idx: usize,
    value: &LiteralValue,
) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
    let field = record.field(field_idx)?;
    match (&field.value_type, value) {
        (
            ValueType::Record(descriptor),
            LiteralValue::NativeRecord {
                descriptor: expected,
                raw,
            },
        ) => {
            let expected: crate::records::RecordDescriptor =
                postcard::from_bytes(expected).map_err(|_| IvmRuntimeError::UnsupportedOperator)?;
            if descriptor.as_ref() != &expected {
                return Ok(FieldLiteralOrdering::Unsupported);
            }
            let Value::Record(actual) = record.get_idx(field_idx)? else {
                return Ok(FieldLiteralOrdering::Unsupported);
            };
            Ok(ordering(actual.raw(), raw.as_slice()))
        }
        (ValueType::U8, LiteralValue::U8(expected)) => {
            Ok(ordering(&record.get_u8(field_idx)?, expected))
        }
        (ValueType::U32, LiteralValue::U32(expected)) => {
            Ok(ordering(&record.get_u32(field_idx)?, expected))
        }
        (ValueType::U64, LiteralValue::U64(expected)) => {
            Ok(ordering(&record.get_u64(field_idx)?, expected))
        }
        (ValueType::U48, LiteralValue::U48(expected)) => {
            Ok(ordering(&record.get_u48(field_idx)?, expected))
        }
        (ValueType::I32, LiteralValue::I32(expected)) => {
            Ok(ordering(&record.get_i32(field_idx)?, expected))
        }
        (ValueType::I64, LiteralValue::I64(expected)) => {
            Ok(ordering(&record.get_i64(field_idx)?, expected))
        }
        (ValueType::F64, LiteralValue::F64(expected)) => Ok(ordering(
            &record.get_f64(field_idx)?,
            &f64::from_bits(*expected),
        )),
        (ValueType::Bool, LiteralValue::Bool(expected)) => {
            Ok(ordering(&record.get_bool(field_idx)?, expected))
        }
        (ValueType::String, LiteralValue::String(expected)) => {
            Ok(ordering(record.get_str(field_idx)?, expected.as_str()))
        }
        (ValueType::Bytes, LiteralValue::Bytes(expected)) => {
            Ok(ordering(record.get_bytes(field_idx)?, expected.as_slice()))
        }
        (ValueType::Uuid, LiteralValue::Uuid(expected)) => Ok(ordering(
            record.get_uuid(field_idx)?.as_bytes(),
            expected.as_bytes(),
        )),
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
    let r = record;
    let i = field_idx;
    Ok(match (inner, expected) {
        (ValueType::U64, LiteralValue::U64(e)) => nullable_ordering(r.get_nullable_u64(i)?, e),
        (ValueType::U48, LiteralValue::U48(e)) => nullable_ordering(r.get_nullable_u48(i)?, e),
        (ValueType::I64, LiteralValue::I64(e)) => nullable_ordering(r.get_nullable_i64(i)?, e),
        (ValueType::I32, LiteralValue::I32(e)) => nullable_ordering(r.get_nullable_i32(i)?, e),
        (ValueType::F64, LiteralValue::F64(e)) => {
            nullable_ordering(r.get_nullable_f64(i)?, &f64::from_bits(*e))
        }
        (ValueType::String, LiteralValue::String(e)) => {
            nullable_ordering(r.get_nullable_string(i)?, e.as_str())
        }
        (ValueType::Bytes, LiteralValue::Bytes(e)) => {
            nullable_ordering(r.get_nullable_bytes(i)?, e.as_slice())
        }
        (ValueType::Uuid, LiteralValue::Uuid(e)) => nullable_ordering(
            r.get_nullable_uuid(i)?.as_ref().map(uuid::Uuid::as_bytes),
            e.as_bytes(),
        ),
        (ValueType::EnumTag(_), LiteralValue::EnumTag(e)) => {
            nullable_ordering(r.get_nullable_enum(i)?, e)
        }
        _ => FieldLiteralOrdering::Unsupported,
    })
}

/// [`ordering`] of a nullable field; a null field compares as SQL null.
fn nullable_ordering<A, T>(actual: Option<A>, expected: &T) -> FieldLiteralOrdering
where
    A: std::borrow::Borrow<T>,
    T: PartialOrd + ?Sized,
{
    actual.map_or(FieldLiteralOrdering::SqlNull, |actual| {
        ordering(actual.borrow(), expected)
    })
}

pub(super) fn ordering<T: PartialOrd + ?Sized>(actual: &T, expected: &T) -> FieldLiteralOrdering {
    actual
        .partial_cmp(expected)
        .map(FieldLiteralOrdering::Compared)
        .unwrap_or(FieldLiteralOrdering::SqlNull)
}

pub(super) fn compare_record_fields(
    record: impl PredicateRecord,
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
    record: impl PredicateRecord,
    field: &str,
    value: &LiteralValue,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let needle = value.to_value();
    let haystack = resolved_record_value(record, field)?;
    Ok(value_contains_sql(&haystack, &needle, comparison))
}

pub(super) fn contains_record_field_value(
    record: impl PredicateRecord,
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
        (Value::U48(left), Value::U48(right)) => left.partial_cmp(right),
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
        Value::U48(value) => Some(i128::from(*value)),
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
        Value::F64(value) if value.is_nan() => {
            return Err(IvmRuntimeError::RecordEncoding(
                records::Error::InvalidF64NaN,
            ));
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_key_part(key, value)?;
            }
        }
        Value::Nullable(None) => {
            key.push(8);
        }
        Value::Nullable(Some(value)) => {
            key.push(9);
            encode_key_part(key, value)?;
        }
        value => encode_scalar_key_part(key, value)?,
    }
    Ok(())
}

/// Type-tagged, order-preserving bytes of one scalar key value. The arrangement
/// codec ([`encode_key_part`]) and the runtime window-group codec share these
/// tags and differ only in how they encode composite values; any non-scalar
/// value reaching here is not a supported key.
#[inline]
pub(super) fn encode_scalar_key_part(
    key: &mut Vec<u8>,
    value: &Value,
) -> Result<(), IvmRuntimeError> {
    match value {
        Value::U8(value) | Value::EnumTag(value) => put_u8(key, *value),
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
            Ok(())
        }
        Value::U32(value) => put_u32(key, *value),
        Value::U64(value) => put_u64(key, *value),
        Value::U48(value) => put_u48(key, *value),
        Value::I32(value) => put_i32(key, *value),
        Value::I64(value) => put_i64(key, *value),
        Value::F64(value) => put_f64(key, *value),
        Value::Bool(value) => put_bool(key, *value),
        Value::String(value) => put_str(key, value),
        Value::Bytes(value) => put_bytes(key, value),
        Value::Uuid(value) => put_uuid(key, *value),
        Value::Tuple(_)
        | Value::Nullable(_)
        | Value::Array(_)
        | Value::Record(_)
        | Value::Enum(_)
        | Value::Large(_) => Err(IvmRuntimeError::UnsupportedJoinKey),
    }
}

// One tagged writer per scalar key type, shared by the `Value` encoder and
// the typed record-field fast path so the tag table lives in one place.

type KeyResult = Result<(), IvmRuntimeError>;

#[inline]
fn put_u8(key: &mut Vec<u8>, value: u8) -> KeyResult {
    key.extend([0, value]);
    Ok(())
}

#[inline]
fn put_u32(key: &mut Vec<u8>, value: u32) -> KeyResult {
    key.push(2);
    key.extend(value.to_be_bytes());
    Ok(())
}

#[inline]
fn put_u64(key: &mut Vec<u8>, value: u64) -> KeyResult {
    key.push(3);
    key.extend(value.to_be_bytes());
    Ok(())
}

#[inline]
fn put_u48(key: &mut Vec<u8>, value: u64) -> KeyResult {
    key.push(16);
    key.extend(crate::records::u48_be_bytes(value)?);
    Ok(())
}

#[inline]
fn put_i32(key: &mut Vec<u8>, value: i32) -> KeyResult {
    key.push(14);
    key.extend(order_preserving_i32_bits(value).to_be_bytes());
    Ok(())
}

#[inline]
fn put_i64(key: &mut Vec<u8>, value: i64) -> KeyResult {
    key.push(13);
    key.extend(order_preserving_i64_bits(value).to_be_bytes());
    Ok(())
}

/// Callers that may see NaN reject it first with [`non_nan`].
#[inline]
fn put_f64(key: &mut Vec<u8>, value: f64) -> KeyResult {
    key.push(4);
    key.extend(order_preserving_f64_bits(value).to_be_bytes());
    Ok(())
}

#[inline]
fn put_bool(key: &mut Vec<u8>, value: bool) -> KeyResult {
    key.extend([5, u8::from(value)]);
    Ok(())
}

#[inline]
fn put_str(key: &mut Vec<u8>, value: &str) -> KeyResult {
    put_bytes_tagged(key, 6, value.as_bytes())
}

#[inline]
fn put_bytes(key: &mut Vec<u8>, value: &[u8]) -> KeyResult {
    put_bytes_tagged(key, 7, value)
}

#[inline]
fn put_bytes_tagged(key: &mut Vec<u8>, tag: u8, value: &[u8]) -> KeyResult {
    key.push(tag);
    encode_ordered_bytes(key, value);
    Ok(())
}

#[inline]
fn put_uuid(key: &mut Vec<u8>, value: uuid::Uuid) -> KeyResult {
    key.push(10);
    key.extend_from_slice(value.as_bytes());
    Ok(())
}

/// `Value::Nullable` framing: 8 for null, else 9 followed by the value.
#[inline]
fn put_nullable<T>(
    key: &mut Vec<u8>,
    value: Option<T>,
    put: impl FnOnce(&mut Vec<u8>, T) -> KeyResult,
) -> KeyResult {
    match value {
        Some(value) => {
            key.push(9);
            put(key, value)
        }
        None => {
            key.push(8);
            Ok(())
        }
    }
}

#[inline]
fn non_nan(value: f64) -> Result<f64, IvmRuntimeError> {
    if value.is_nan() {
        return Err(IvmRuntimeError::RecordEncoding(
            records::Error::InvalidF64NaN,
        ));
    }
    Ok(value)
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
