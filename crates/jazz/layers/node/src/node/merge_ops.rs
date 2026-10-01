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

/// A counter value in the column's integer type, modulo 2^width.
///
/// Counter ops are deltas carried in the column's own type, so they are
/// taken modulo 2^width: a decrement of an unsigned counter, or a signed
/// jump wider than the type, is exact for the one write that makes it, and
/// adding it back to the base the write saw yields exactly the written
/// value. Core adds concurrent ops the same way, so they commute
/// (`INV-HIST-15`); a sum that leaves the column's range wraps.
fn counter_wrapping(column_type: &ValueType, value: i128) -> Result<Value, Error> {
    // `as` from i128 keeps the low bits: the two's-complement residue.
    match column_type {
        ValueType::U8 => Ok(Value::U8(value as u8)),
        ValueType::U16 => Ok(Value::U16(value as u16)),
        ValueType::U32 => Ok(Value::U32(value as u32)),
        ValueType::U64 => Ok(Value::U64(value as u64)),
        ValueType::I32 => Ok(Value::I32(value as i32)),
        ValueType::I64 => Ok(Value::I64(value as i64)),
        ValueType::Nullable(inner) => counter_wrapping(inner, value),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
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
                cells.insert(
                    column.name.clone(),
                    counter_wrapping(&column.column_type, delta)?,
                );
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

/// Apply a patch's merge-column op onto the previous image's value.
pub(in crate::node) fn apply_merge_op(
    strategy: MergeStrategy,
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
) -> Result<Value, Error> {
    match strategy {
        MergeStrategy::Counter => counter_wrapping(
            column_type,
            counter_to_i128(previous)? + counter_to_i128(op)?,
        ),
        MergeStrategy::GSet => gset_union(column_type, Some(previous), Some(op)),
        MergeStrategy::Lww => Ok(op.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counter op is exact for any single write, whatever its sign or size:
    /// an unsigned decrement, and a signed jump across the whole type.
    #[test]
    fn counter_op_restores_the_written_value_over_its_base() {
        for (column_type, base, written) in [
            (ValueType::U8, Value::U8(200), Value::U8(1)),
            (ValueType::U64, Value::U64(10), Value::U64(7)),
            (ValueType::I32, Value::I32(i32::MIN), Value::I32(i32::MAX)),
        ] {
            let delta = counter_wrapping(
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
}
