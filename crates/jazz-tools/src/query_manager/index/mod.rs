use std::ops::Bound;

use crate::query_manager::types::{
    ColumnName, CompositeIndex, CompositeIndexColumn, IndexDirection, Value,
};

/// Condition for index scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanCondition {
    /// No condition - scan all entries (uses "_id" index).
    All,
    /// Empty condition - scan no entries.
    Empty,
    /// Exact match on value.
    Eq(Value),
    /// Range scan with bounds (inclusive, exclusive, or unbounded).
    Range {
        min: Bound<Value>,
        max: Bound<Value>,
    },
}

pub(crate) fn composite_index_column_name(index: &CompositeIndex) -> String {
    index.name.as_str().to_string()
}

pub(crate) fn composite_index_prefix_range(
    parts: &[CompositeIndexColumn],
    values: &[Value],
) -> Option<(Value, Option<Value>)> {
    if values.len() > parts.len() {
        return None;
    }

    let prefix = composite_index_bytes(parts.iter().zip(values.iter()))?;
    let upper = increment_bytes(&prefix).map(Value::Bytea);
    Some((Value::Bytea(prefix), upper))
}

pub(crate) fn composite_index_value(
    parts: &[CompositeIndexColumn],
    values_by_column: &[(ColumnName, Value)],
) -> Option<Value> {
    let mut out = Vec::new();
    for part in parts {
        let value = values_by_column
            .iter()
            .find_map(|(column, value)| (*column == part.name).then_some(value))?;
        if matches!(value, Value::Null) {
            return None;
        }
        append_composite_index_part(&mut out, part, value)?;
    }
    Some(Value::Bytea(out))
}

fn composite_index_bytes<'a>(
    values: impl IntoIterator<Item = (&'a CompositeIndexColumn, &'a Value)>,
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for (part, value) in values {
        append_composite_index_part(&mut out, part, value)?;
    }
    Some(out)
}

fn append_composite_index_part(
    out: &mut Vec<u8>,
    part: &CompositeIndexColumn,
    value: &Value,
) -> Option<()> {
    let len_offset = out.len();
    out.extend_from_slice(&0_u32.to_be_bytes());
    let value_offset = out.len();
    crate::storage::append_encoded_value(out, value);
    let len = u32::try_from(out.len().checked_sub(value_offset)?).ok()?;
    out[len_offset..value_offset].copy_from_slice(&len.to_be_bytes());

    if matches!(part.direction, IndexDirection::Desc) {
        for byte in &mut out[value_offset..] {
            *byte = !*byte;
        }
    }
    Some(())
}

fn increment_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut next = bytes.to_vec();
    for idx in (0..next.len()).rev() {
        if next[idx] != u8::MAX {
            next[idx] += 1;
            next.truncate(idx + 1);
            return Some(next);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::object::ObjectId;
    use crate::query_manager::types::{ColumnName, CompositeIndexColumn, IndexDirection, Value};

    use super::{composite_index_prefix_range, composite_index_value};

    fn reference_composite_bytes<'a>(
        parts: impl IntoIterator<Item = (&'a CompositeIndexColumn, &'a Value)>,
    ) -> Vec<u8> {
        let mut out = Vec::new();

        for (part, value) in parts {
            let mut encoded = crate::storage::encode_value(value);
            if matches!(part.direction, IndexDirection::Desc) {
                for byte in &mut encoded {
                    *byte = !*byte;
                }
            }

            out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            out.extend_from_slice(&encoded);
        }

        out
    }

    #[test]
    fn composite_index_value_matches_storage_value_encoding() {
        let columns = vec![
            CompositeIndexColumn::asc("owner_id"),
            CompositeIndexColumn::desc("updated_at"),
            CompositeIndexColumn::asc("title"),
            CompositeIndexColumn::desc("score"),
            CompositeIndexColumn::asc("payload"),
            CompositeIndexColumn::asc("tags"),
            CompositeIndexColumn::asc("tuple"),
        ];
        let values = vec![
            ("owner_id", Value::Uuid(ObjectId::new())),
            ("updated_at", Value::Timestamp(1_700_000_123)),
            ("title", Value::Text("Quarterly plan".to_string())),
            ("score", Value::Double(-12.5)),
            ("payload", Value::Bytea(vec![0, 1, 2, 255])),
            (
                "tags",
                Value::Array(vec![
                    Value::Text("launch".to_string()),
                    Value::Integer(42),
                    Value::Boolean(true),
                ]),
            ),
            (
                "tuple",
                Value::Row {
                    id: None,
                    values: vec![Value::BigInt(-9), Value::BatchId([3; 16])],
                },
            ),
        ];
        let values_by_column = values
            .iter()
            .map(|(name, value)| (ColumnName::new(*name), value.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            composite_index_value(&columns, &values_by_column),
            Some(Value::Bytea(reference_composite_bytes(
                columns
                    .iter()
                    .zip(values_by_column.iter().map(|(_, value)| value))
            )))
        );
    }

    #[test]
    fn composite_index_prefix_range_matches_storage_value_encoding() {
        let columns = vec![
            CompositeIndexColumn::asc("owner_id"),
            CompositeIndexColumn::desc("updated_at"),
        ];
        let values = vec![
            Value::Text("alice".to_string()),
            Value::Timestamp(1_700_000_123),
        ];
        let expected_prefix = reference_composite_bytes(columns.iter().zip(values.iter()));

        let (prefix, upper) = composite_index_prefix_range(&columns, &values).unwrap();

        assert_eq!(prefix, Value::Bytea(expected_prefix.clone()));
        assert!(matches!(upper, Some(Value::Bytea(bytes)) if bytes > expected_prefix));
    }
}
