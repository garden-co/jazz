//! Merge columns as operations. A pending patch carries a counter column's
//! delta and a g-set column's added elements, relative to the row image the
//! write was made over. Core applies each op to the current image when it
//! accepts the patch, so concurrent patches compose in any order.

use super::*;
use crate::schema::MergeStrategy;

pub(in crate::node) fn counter_to_i128(value: &Value) -> Result<i128, Error> {
    match value {
        Value::U8(value) => Ok(i128::from(*value)),
        Value::U16(value) => Ok(i128::from(*value)),
        Value::U32(value) => Ok(i128::from(*value)),
        Value::U64(value) => Ok(i128::from(*value)),
        Value::I32(value) => Ok(i128::from(*value)),
        Value::I64(value) => Ok(i128::from(*value)),
        Value::Nullable(None) => Ok(0),
        Value::Nullable(Some(inner)) => counter_to_i128(inner),
        _ => Err(Error::InvalidStoredValue("counter value must be integer")),
    }
}

/// The bit width and value range of a counter column's integer type.
fn counter_range(column_type: &ValueType) -> Result<(u32, i128, i128), Error> {
    match column_type {
        ValueType::U8 => Ok((8, 0, i128::from(u8::MAX))),
        ValueType::U16 => Ok((16, 0, i128::from(u16::MAX))),
        ValueType::U32 => Ok((32, 0, i128::from(u32::MAX))),
        ValueType::U64 => Ok((64, 0, i128::from(u64::MAX))),
        ValueType::I32 => Ok((32, i128::from(i32::MIN), i128::from(i32::MAX))),
        ValueType::I64 => Ok((64, i128::from(i64::MIN), i128::from(i64::MAX))),
        ValueType::Nullable(inner) => counter_range(inner),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}

/// A counter op: a delta carried in the column's own integer type as its
/// two's-complement residue modulo 2^width. Deltas range over the signed
/// values of that width, so a decrement of an unsigned counter travels
/// exactly; a single write whose change does not fit is refused when it is
/// made (`split_merge_ops`).
fn counter_op(column_type: &ValueType, delta: i128) -> Result<Value, Error> {
    // `as` from i128 keeps the low bits: the two's-complement residue.
    match column_type {
        ValueType::U8 => Ok(Value::U8(delta as u8)),
        ValueType::U16 => Ok(Value::U16(delta as u16)),
        ValueType::U32 => Ok(Value::U32(delta as u32)),
        ValueType::U64 => Ok(Value::U64(delta as u64)),
        ValueType::I32 => Ok(Value::I32(delta as i32)),
        ValueType::I64 => Ok(Value::I64(delta as i64)),
        ValueType::Nullable(inner) => counter_op(inner, delta),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}

/// The signed delta a counter op carries: its residue read in the signed
/// range of the column's width.
pub(in crate::node) fn counter_op_delta(
    column_type: &ValueType,
    op: &Value,
) -> Result<i128, Error> {
    let (width, _, _) = counter_range(column_type)?;
    let residue = counter_to_i128(op)?;
    let half = 1_i128 << (width - 1);
    Ok(if residue >= half {
        residue - (1_i128 << width)
    } else {
        residue
    })
}

/// A counter's value after one op, or `None` when the sum leaves the
/// column's range. Core rejects such a write at settle rather than wrap.
pub(in crate::node) fn counter_after_op(
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
) -> Result<Option<Value>, Error> {
    let (_, min, max) = counter_range(column_type)?;
    let sum = counter_to_i128(previous)? + counter_op_delta(column_type, op)?;
    if sum < min || sum > max {
        return Ok(None);
    }
    counter_op(column_type, sum).map(Some)
}

fn gset_elements(
    column_type: &ValueType,
    values: impl IntoIterator<Item = Value>,
    elements: &mut BTreeMap<Vec<u8>, Value>,
) -> Result<(), Error> {
    let ValueType::Array(element_type) = column_type else {
        return Err(Error::InvalidStoredValue(
            "g-set merge strategy requires an array column",
        ));
    };
    // Elements are keyed and ordered by Groove's deterministic record
    // encoding; distinct float bit patterns stay distinct.
    let descriptor = records::RecordDescriptor::new([("element", element_type.as_ref().clone())]);
    for value in values {
        let key = descriptor.create(std::slice::from_ref(&value))?;
        elements.entry(key).or_insert(value);
    }
    Ok(())
}

fn gset_values(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(values)) => values.clone(),
        Some(Value::Nullable(Some(inner))) => gset_values(Some(inner)),
        _ => Vec::new(),
    }
}

/// The canonical union of two g-set values.
pub(in crate::node) fn gset_union(
    column_type: &ValueType,
    left: Option<&Value>,
    right: Option<&Value>,
) -> Result<Value, Error> {
    let mut elements = BTreeMap::new();
    gset_elements(column_type, gset_values(left), &mut elements)?;
    gset_elements(column_type, gset_values(right), &mut elements)?;
    Ok(Value::Array(elements.into_values().collect()))
}

/// The elements of `written` missing from `base`, canonically ordered: the
/// add op of a g-set write. Omitting an element never removes it.
pub(in crate::node) fn gset_added(
    column_type: &ValueType,
    written: Option<&Value>,
    base: Option<&Value>,
) -> Result<Value, Error> {
    let mut known = BTreeMap::new();
    gset_elements(column_type, gset_values(base), &mut known)?;
    let mut added = BTreeMap::new();
    gset_elements(column_type, gset_values(written), &mut added)?;
    Ok(Value::Array(
        added
            .into_iter()
            .filter(|(key, _)| !known.contains_key(key))
            .map(|(_, value)| value)
            .collect(),
    ))
}

/// Split an authored write into the patch's op cells and the local image.
/// `cells` holds the written absolute values; `base` is the image the write
/// was made over. Returns the local image's cells; `cells` becomes the ops.
pub(in crate::node) fn split_merge_ops(
    table_schema: &TableSchema,
    authored: &BTreeSet<String>,
    base: &BTreeMap<String, Value>,
    cells: &mut BTreeMap<String, Value>,
) -> Result<Option<BTreeMap<String, Value>>, Error> {
    let mut image = None;
    for column in &table_schema.columns {
        let strategy = table_schema.merge_strategy(&column.name);
        if strategy == MergeStrategy::Lww || !authored.contains(&column.name) {
            continue;
        }
        let Some(written) = cells.get(&column.name).cloned() else {
            continue;
        };
        let image = image.get_or_insert_with(|| cells.clone());
        let base_value = base.get(&column.name);
        match strategy {
            MergeStrategy::Counter => {
                let delta = counter_to_i128(&written)?
                    - base_value.map(counter_to_i128).transpose()?.unwrap_or(0);
                // Over an image, the change must fit the signed range of
                // the column's width so Core reads the op back exactly. A
                // write over no image carries its value itself.
                let (width, _, _) = counter_range(&column.column_type)?;
                let half = 1_i128 << (width - 1);
                if base_value.is_some() && (delta < -half || delta >= half) {
                    return Err(Error::InvalidMergeableCommit(
                        "counter change in one write must fit the signed range of the column's width",
                    ));
                }
                cells.insert(column.name.clone(), counter_op(&column.column_type, delta)?);
            }
            MergeStrategy::GSet => {
                image.insert(
                    column.name.clone(),
                    gset_union(&column.column_type, base_value, Some(&written))?,
                );
                cells.insert(
                    column.name.clone(),
                    gset_added(&column.column_type, Some(&written), base_value)?,
                );
            }
            MergeStrategy::Lww => {}
        }
    }
    Ok(image)
}

/// Apply an accepted patch's merge-column op onto the previous image's
/// value. Core rejects a counter op that would leave the column's range
/// before it accepts the write, so an accepted op that does is an error.
pub(in crate::node) fn apply_merge_op(
    strategy: MergeStrategy,
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
) -> Result<Value, Error> {
    match strategy {
        MergeStrategy::Counter => counter_after_op(column_type, previous, op)?.ok_or(
            Error::InvalidStoredValue("accepted counter op leaves the column's range"),
        ),
        MergeStrategy::GSet => gset_union(column_type, Some(previous), Some(op)),
        MergeStrategy::Lww => Ok(op.clone()),
    }
}

/// Apply a still-pending patch's merge-column op onto a local base. A
/// counter op that would leave the column's range there will be rejected by
/// Core, so the local view keeps the base value.
pub(in crate::node) fn apply_pending_merge_op(
    strategy: MergeStrategy,
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
) -> Result<Value, Error> {
    match strategy {
        MergeStrategy::Counter => Ok(counter_after_op(column_type, previous, op)?
            .unwrap_or_else(|| counter_value_without_null(previous))),
        _ => apply_merge_op(strategy, column_type, previous, op),
    }
}

fn counter_value_without_null(value: &Value) -> Value {
    match value {
        Value::Nullable(Some(inner)) => counter_value_without_null(inner),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counter op is exact for any single write whose change fits the
    /// signed range of the column's width: an unsigned decrement included.
    #[test]
    fn counter_op_restores_the_written_value_over_its_base() {
        for (column_type, base, written) in [
            (ValueType::U8, Value::U8(100), Value::U8(1)),
            (ValueType::U8, Value::U8(1), Value::U8(128)),
            (ValueType::U64, Value::U64(10), Value::U64(7)),
            (ValueType::I32, Value::I32(-5), Value::I32(i32::MAX - 5)),
        ] {
            let delta = counter_op(
                &column_type,
                counter_to_i128(&written).unwrap() - counter_to_i128(&base).unwrap(),
            )
            .unwrap();
            assert_eq!(
                apply_merge_op(MergeStrategy::Counter, &column_type, &base, &delta).unwrap(),
                written,
                "{column_type:?}"
            );
        }
    }

    /// A sum that leaves the column's range is not wrapped: settle sees it
    /// as out of range, below zero for an unsigned counter and past the
    /// maximum for a signed one.
    #[test]
    fn counter_op_out_of_the_column_range_does_not_wrap() {
        for (column_type, base, delta) in [
            (ValueType::U64, Value::U64(0), -1),
            (ValueType::U8, Value::U8(250), 10),
            (ValueType::I32, Value::I32(i32::MAX), 1),
            (ValueType::I64, Value::I64(i64::MIN), -1),
        ] {
            let op = counter_op(&column_type, delta).unwrap();
            assert_eq!(
                counter_after_op(&column_type, &base, &op).unwrap(),
                None,
                "{column_type:?}"
            );
            assert!(apply_merge_op(MergeStrategy::Counter, &column_type, &base, &op).is_err());
        }
    }

    /// A single write whose change does not fit the signed range of the
    /// column's width cannot be carried as an unambiguous op and is refused
    /// when it is made.
    #[test]
    fn counter_change_wider_than_half_the_type_is_refused_when_written() {
        let table = TableSchema::new(
            "counters",
            [groove::schema::ColumnSchema::new(
                "count",
                groove::schema::ColumnType::U8,
            )],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter);
        let authored = BTreeSet::from(["count".to_owned()]);
        let base = BTreeMap::from([("count".to_owned(), Value::U8(200))]);
        let mut cells = BTreeMap::from([("count".to_owned(), Value::U8(1))]);
        assert!(matches!(
            split_merge_ops(&table, &authored, &base, &mut cells),
            Err(Error::InvalidMergeableCommit(_))
        ));
    }
}
