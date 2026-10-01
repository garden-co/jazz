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

/// A counter op: the exact delta of one write, as its two's-complement
/// value one bit wider than the column's type. The low `width` bits travel
/// in the op cell, in the column's own type; the sign bit travels beside the
/// cell in the patch's counter signs (`counter_signs`). The difference of any
/// two values of the type lies in `[-(2^width - 1), 2^width - 1]`, so every
/// single write of an in-range value is carried exactly.
pub(in crate::node) fn counter_op(
    column_type: &ValueType,
    delta: i128,
) -> Result<(Value, bool), Error> {
    let (width, _, _) = counter_range(column_type)?;
    let span = 1_i128 << width;
    if delta < -span || delta >= span {
        return Err(Error::InvalidMergeableCommit(
            "counter delta exceeds the span of the column's type",
        ));
    }
    Ok((counter_bits(column_type, delta)?, delta < 0))
}

/// `value`'s low bits in the column's integer type: `as` from i128 keeps
/// the two's-complement residue modulo 2^width.
fn counter_bits(column_type: &ValueType, value: i128) -> Result<Value, Error> {
    match column_type {
        ValueType::U8 => Ok(Value::U8(value as u8)),
        ValueType::U16 => Ok(Value::U16(value as u16)),
        ValueType::U32 => Ok(Value::U32(value as u32)),
        ValueType::U64 => Ok(Value::U64(value as u64)),
        ValueType::I32 => Ok(Value::I32(value as i32)),
        ValueType::I64 => Ok(Value::I64(value as i64)),
        ValueType::Nullable(inner) => counter_bits(inner, value),
        _ => Err(Error::InvalidStoredValue(
            "counter strategy requires integer column",
        )),
    }
}

/// The delta a counter op carries: its cell's low bits read unsigned, less
/// 2^width when its sign bit is set.
pub(in crate::node) fn counter_op_delta(
    column_type: &ValueType,
    op: &Value,
    negative: bool,
) -> Result<i128, Error> {
    let (width, _, _) = counter_range(column_type)?;
    let span = 1_i128 << width;
    let low = counter_to_i128(op)?.rem_euclid(span);
    Ok(if negative { low - span } else { low })
}

/// A counter's value after one op, or `None` when the sum leaves the
/// column's range. Core rejects such a write at settle rather than wrap.
pub(in crate::node) fn counter_after_op(
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
    negative: bool,
) -> Result<Option<Value>, Error> {
    let (_, min, max) = counter_range(column_type)?;
    let sum = counter_to_i128(previous)? + counter_op_delta(column_type, op, negative)?;
    if sum < min || sum > max {
        return Ok(None);
    }
    counter_bits(column_type, sum).map(Some)
}

/// Ordinal of each counter column among the table's counter columns, in
/// schema order: its bit in the counter signs. `None` for other columns.
fn counter_ordinals(table: &TableSchema) -> impl Iterator<Item = Option<usize>> + '_ {
    let mut next = 0;
    table.columns.iter().map(move |column| {
        (table.merge_strategy(&column.name) == MergeStrategy::Counter).then(|| {
            next += 1;
            next - 1
        })
    })
}

/// Whether the op of the column at schema position `index` is negative,
/// according to a patch's counter signs. Always false for a column that is
/// not a counter, and on a settled image (whose signs are empty).
pub(in crate::node) fn counter_sign(table: &TableSchema, signs: &[u8], index: usize) -> bool {
    counter_ordinals(table)
        .nth(index)
        .flatten()
        .is_some_and(|bit| {
            signs
                .get(bit / 8)
                .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
        })
}

/// Clear the counter signs of a row's history values: a settled image (or a
/// local image folded from a patch) carries values, not ops.
pub(in crate::node) fn clear_counter_signs(
    values: &mut [Value],
    descriptor: &records::RecordDescriptor,
) {
    if let Some(field) = descriptor.field_index(crate::schema::COUNTER_SIGNS_FIELD) {
        values[field] = Value::Bytes(Vec::new());
    }
}

/// Counter signs are canonical: no bit beyond the table's counter columns
/// and no trailing zero byte, so a patch has exactly one encoding and an
/// image with no negative op carries none.
pub(in crate::node) fn validate_counter_signs(
    table: &TableSchema,
    signs: &[u8],
) -> Result<(), Error> {
    let counters = counter_ordinals(table).flatten().count();
    let canonical = signs.last().is_none_or(|last| *last != 0)
        && signs.len() <= counters.div_ceil(8)
        && signs.iter().enumerate().all(|(byte_index, byte)| {
            (0..8).all(|bit| byte & (1 << bit) == 0 || byte_index * 8 + bit < counters)
        });
    if canonical {
        Ok(())
    } else {
        Err(Error::InvalidMergeableCommit(
            "counter signs must name only counter columns, without trailing zero bytes",
        ))
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
/// was made over. Returns the local image's cells and the patch's counter
/// signs; `cells` becomes the ops.
pub(in crate::node) fn split_merge_ops(
    table_schema: &TableSchema,
    authored: &BTreeSet<String>,
    base: &BTreeMap<String, Value>,
    cells: &mut BTreeMap<String, Value>,
) -> Result<(Option<BTreeMap<String, Value>>, Vec<u8>), Error> {
    let mut image = None;
    let mut signs = Vec::new();
    for (column, ordinal) in table_schema
        .columns
        .iter()
        .zip(counter_ordinals(table_schema))
    {
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
                // A write over no image carries its value itself, a delta
                // from zero. Either way the delta is the difference of two
                // values of the type, which an op carries exactly.
                let delta = counter_to_i128(&written)?
                    - base_value.map(counter_to_i128).transpose()?.unwrap_or(0);
                let (op, negative) = counter_op(&column.column_type, delta)?;
                if negative {
                    let bit = ordinal.ok_or(Error::InvalidStoredValue(
                        "counter column must have a sign ordinal",
                    ))?;
                    if signs.len() <= bit / 8 {
                        signs.resize(bit / 8 + 1, 0);
                    }
                    signs[bit / 8] |= 1 << (bit % 8);
                }
                cells.insert(column.name.clone(), op);
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
    Ok((image, signs))
}

/// Apply an accepted patch's merge-column op onto the previous image's
/// value. `negative` is the op's counter sign. Core rejects a counter op
/// that would leave the column's range before it accepts the write, so an
/// accepted op that does is an error.
pub(in crate::node) fn apply_merge_op(
    strategy: MergeStrategy,
    column_type: &ValueType,
    previous: &Value,
    op: &Value,
    negative: bool,
) -> Result<Value, Error> {
    match strategy {
        MergeStrategy::Counter => counter_after_op(column_type, previous, op, negative)?.ok_or(
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
    negative: bool,
) -> Result<Value, Error> {
    match strategy {
        MergeStrategy::Counter => Ok(counter_after_op(column_type, previous, op, negative)?
            .unwrap_or_else(|| counter_value_without_null(previous))),
        _ => apply_merge_op(strategy, column_type, previous, op, negative),
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

    /// A counter op is exact for any single write of an in-range value,
    /// whatever its sign or size: an unsigned decrement, a change wider than
    /// half the type, and a jump across the whole signed or unsigned type.
    #[test]
    fn counter_op_restores_the_written_value_over_its_base() {
        for (column_type, base, written) in [
            (ValueType::U8, Value::U8(200), Value::U8(1)),
            (ValueType::U8, Value::U8(0), Value::U8(u8::MAX)),
            (ValueType::U8, Value::U8(u8::MAX), Value::U8(0)),
            (ValueType::U64, Value::U64(10), Value::U64(7)),
            (ValueType::U64, Value::U64(u64::MAX), Value::U64(0)),
            (ValueType::U64, Value::U64(0), Value::U64(u64::MAX)),
            (ValueType::I32, Value::I32(i32::MIN), Value::I32(i32::MAX)),
            (ValueType::I32, Value::I32(i32::MAX), Value::I32(i32::MIN)),
            (ValueType::I64, Value::I64(i64::MIN), Value::I64(i64::MAX)),
            (ValueType::I64, Value::I64(i64::MAX), Value::I64(i64::MIN)),
        ] {
            let (op, negative) = counter_op(
                &column_type,
                counter_to_i128(&written).unwrap() - counter_to_i128(&base).unwrap(),
            )
            .unwrap();
            assert_eq!(
                apply_merge_op(MergeStrategy::Counter, &column_type, &base, &op, negative).unwrap(),
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
            (ValueType::U8, Value::U8(100), -199),
            (ValueType::I32, Value::I32(i32::MAX), 1),
            (ValueType::I64, Value::I64(i64::MIN), -1),
            (ValueType::I64, Value::I64(1), i128::from(i64::MAX)),
        ] {
            let (op, negative) = counter_op(&column_type, delta).unwrap();
            assert_eq!(
                counter_after_op(&column_type, &base, &op, negative).unwrap(),
                None,
                "{column_type:?}"
            );
            assert!(
                apply_merge_op(MergeStrategy::Counter, &column_type, &base, &op, negative).is_err()
            );
        }
    }

    /// A write wider than half the type splits into the op cell and a sign
    /// bit; the U8 200 -> 1 write (-199) is not confused with +57, which
    /// shares its low bits, and two such ops commute at settle.
    #[test]
    fn counter_change_wider_than_half_the_type_carries_its_sign() {
        let table = TableSchema::new(
            "counters",
            [
                groove::schema::ColumnSchema::new("label", groove::schema::ColumnType::String),
                groove::schema::ColumnSchema::new("count", groove::schema::ColumnType::U8),
            ],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter);
        let authored = BTreeSet::from(["count".to_owned()]);
        let base = BTreeMap::from([("count".to_owned(), Value::U8(200))]);
        let mut cells = BTreeMap::from([("count".to_owned(), Value::U8(1))]);
        let (_, signs) = split_merge_ops(&table, &authored, &base, &mut cells).unwrap();
        assert_eq!(cells["count"], Value::U8(57));
        assert_eq!(signs, vec![0b1]);
        validate_counter_signs(&table, &signs).unwrap();
        assert!(counter_sign(&table, &signs, 1));
        assert!(!counter_sign(&table, &signs, 0));
        // Over a concurrent +50 (200 -> 250), the op still subtracts 199.
        assert_eq!(
            apply_merge_op(
                MergeStrategy::Counter,
                &ValueType::U8,
                &Value::U8(250),
                &cells["count"],
                true
            )
            .unwrap(),
            Value::U8(51)
        );
        // An increment leaves the signs empty.
        let mut cells = BTreeMap::from([("count".to_owned(), Value::U8(255))]);
        let (_, signs) = split_merge_ops(&table, &authored, &base, &mut cells).unwrap();
        assert_eq!((cells["count"].clone(), signs), (Value::U8(55), Vec::new()));
    }

    #[test]
    fn counter_signs_are_canonical() {
        let table = TableSchema::new(
            "counters",
            [groove::schema::ColumnSchema::new(
                "count",
                groove::schema::ColumnType::I32,
            )],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter);
        validate_counter_signs(&table, &[]).unwrap();
        validate_counter_signs(&table, &[1]).unwrap();
        for signs in [&[0][..], &[2], &[1, 0], &[1, 1]] {
            assert!(validate_counter_signs(&table, signs).is_err(), "{signs:?}");
        }
    }
}
