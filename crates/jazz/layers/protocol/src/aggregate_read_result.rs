//! `jazz.aggregate-read-result.v1`: the result an exclusive aggregate read
//! observed, carried on its [`PredicateRead`] (garden-co/jazz#3768).
//!
//! An exclusive transaction that reads an aggregate observes only the
//! aggregate's output, so it records that output instead of the rows the
//! aggregate consumed. The authority re-runs the aggregate as the
//! transaction's permission subject and conflicts iff the canonical bytes
//! differ. Equality is byte equality, so this encoder is the only producer of
//! comparable results.
//!
//! The bytes are the Groove typed-record v1 encoding of one fixed descriptor:
//!
//! ```text
//! (format_v1 = 1, groups[(cells[(name, value?)])])
//! ```
//!
//! - one `groups` entry per output row of the aggregate (one for an ungrouped
//!   aggregate, one per group otherwise), sorted by its encoded group-record
//!   bytes, without duplicates;
//! - one `cells` entry per public output column (the `group_by` column and each
//!   aggregate alias), sorted by name, without duplicates;
//! - `value` is null for a null output (for example `sum` over no rows), and
//!   otherwise the typed value record `(descriptor, value)`: the canonical
//!   persisted Groove descriptor of `{value: T}` and the value's bytes under
//!   it, where `T` is the output type with any nullable wrapper removed.
//!
//! Decoders reject non-canonical bytes, an unknown `format_v1`, unsorted or
//! duplicate groups or cells, and typed values that do not round-trip.

use crate::exclusive_read_evidence::Error;
use crate::query::Query;
use crate::tx::{AggregateReadResult, PredicateRead, Transaction};
use groove::records::{
    OwnedRecord, Record, RecordDescriptor, Value, ValueType, decode_persisted_record_descriptor,
    encode_persisted_record_descriptor,
};
use std::collections::BTreeSet;

const FORMAT_V1: u8 = 1;

fn typed_value_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("descriptor", ValueType::Bytes),
        ("value", ValueType::Bytes),
    ])
}

fn cell_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("name", ValueType::String),
        ("value", ValueType::Nullable(Box::new(ValueType::Bytes))),
    ])
}

fn group_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([(
        "cells",
        ValueType::Record(Box::new(cell_descriptor())).array_of(),
    )])
}

/// `jazz_aggregate_read_result_v1`
pub fn aggregate_read_result_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v1", ValueType::U8),
        (
            "groups",
            ValueType::Record(Box::new(group_descriptor())).array_of(),
        ),
    ])
}

/// One output cell of an aggregate result: its public column name and its
/// value, `None` for a null output.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateResultCell {
    pub name: String,
    pub value: Option<(ValueType, Value)>,
}

fn invalid(what: &'static str) -> Error {
    Error::InvalidStoredValue(what)
}

fn encode_typed_value(value_type: &ValueType, value: &Value) -> Result<Vec<u8>, Error> {
    let value_descriptor = RecordDescriptor::new([("value", value_type.clone())]);
    let descriptor = encode_persisted_record_descriptor(&value_descriptor)?;
    let value = value_descriptor.create(std::slice::from_ref(value))?;
    Ok(typed_value_descriptor().create(&[Value::Bytes(descriptor), Value::Bytes(value)])?)
}

fn record_value(descriptor: &RecordDescriptor, values: &[Value]) -> Result<Value, Error> {
    Ok(Value::Record(OwnedRecord::new(
        descriptor.create(values)?,
        descriptor.clone(),
    )))
}

/// Encode `groups`, each a list of output cells, as canonical v1 bytes.
pub fn encode_aggregate_result_groups(
    groups: Vec<Vec<AggregateResultCell>>,
) -> Result<AggregateReadResult, Error> {
    let cell = cell_descriptor();
    let group = group_descriptor();
    let mut encoded_groups = Vec::with_capacity(groups.len());
    for mut cells in groups {
        cells.sort_by(|left, right| left.name.cmp(&right.name));
        if cells.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(invalid("aggregate result has a duplicate output column"));
        }
        let cells = cells
            .iter()
            .map(|entry| {
                let value = entry
                    .value
                    .as_ref()
                    .map(|(value_type, value)| encode_typed_value(value_type, value))
                    .transpose()?;
                record_value(
                    &cell,
                    &[
                        Value::String(entry.name.clone()),
                        Value::Nullable(value.map(|bytes| Box::new(Value::Bytes(bytes)))),
                    ],
                )
            })
            .collect::<Result<Vec<_>, Error>>()?;
        encoded_groups.push(group.create(&[Value::Array(cells)])?);
    }
    encoded_groups.sort();
    if encoded_groups.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("aggregate result has a duplicate group"));
    }
    let groups = encoded_groups
        .into_iter()
        .map(|bytes| Value::Record(OwnedRecord::new(bytes, group.clone())))
        .collect();
    Ok(AggregateReadResult(
        aggregate_read_result_descriptor().create(&[Value::U8(FORMAT_V1), Value::Array(groups)])?,
    ))
}

/// The public output cells of one materialized aggregate row: every
/// published application column with its value and unwrapped output type.
/// Hidden metadata (the synthetic row id) is not part of the result.
fn canonical_values(
    descriptor: &RecordDescriptor,
    bytes: &[u8],
    what: &'static str,
) -> Result<Vec<Value>, Error> {
    let values = Record::new(bytes.to_vec(), descriptor)
        .to_values()
        .map_err(|_| invalid(what))?;
    if descriptor.create(&values).map_err(|_| invalid(what))? != bytes {
        return Err(invalid(what));
    }
    Ok(values)
}

fn nested_values(value: &Value, what: &'static str) -> Result<Vec<Value>, Error> {
    match value {
        Value::Record(record) => record.to_values().map_err(|_| invalid(what)),
        _ => Err(invalid(what)),
    }
}

fn decode_typed_value(bytes: &[u8]) -> Result<(ValueType, Value), Error> {
    let what = "invalid aggregate result value";
    let outer = canonical_values(&typed_value_descriptor(), bytes, what)?;
    let [Value::Bytes(descriptor), Value::Bytes(value)] = outer.as_slice() else {
        return Err(invalid(what));
    };
    let value_descriptor =
        decode_persisted_record_descriptor(descriptor).map_err(|_| invalid(what))?;
    let [field] = value_descriptor.fields() else {
        return Err(invalid(what));
    };
    if field.name.as_deref() != Some("value") || matches!(field.value_type, ValueType::Nullable(_))
    {
        return Err(invalid(what));
    }
    let [decoded] = canonical_values(&value_descriptor, value, what)?
        .try_into()
        .map_err(|_| invalid(what))?;
    Ok((field.value_type.clone(), decoded))
}

/// Decode canonical v1 bytes into their groups, each a list of cells sorted
/// by name. Fails with [`Error::InvalidStoredValue`] on any byte string the
/// encoder would not produce.
pub fn decode_aggregate_read_result(
    result: &AggregateReadResult,
) -> Result<Vec<Vec<AggregateResultCell>>, Error> {
    let what = "invalid aggregate read result v1";
    let values = canonical_values(&aggregate_read_result_descriptor(), &result.0, what)?;
    let [Value::U8(FORMAT_V1), Value::Array(groups)] = values.as_slice() else {
        return Err(invalid(what));
    };
    let mut decoded = Vec::with_capacity(groups.len());
    let mut previous: Option<&[u8]> = None;
    for group in groups {
        let Value::Record(group_record) = group else {
            return Err(invalid(what));
        };
        if previous.is_some_and(|previous| previous >= group_record.raw()) {
            return Err(invalid("aggregate result groups are not canonical"));
        }
        previous = Some(group_record.raw());
        let group_values = nested_values(group, what)?;
        let [Value::Array(cells)] = group_values.as_slice() else {
            return Err(invalid(what));
        };
        let mut group_cells = Vec::with_capacity(cells.len());
        for cell in cells {
            let cell_values = nested_values(cell, what)?;
            let [Value::String(name), Value::Nullable(value)] = cell_values.as_slice() else {
                return Err(invalid(what));
            };
            let value = match value.as_deref() {
                None => None,
                Some(Value::Bytes(bytes)) => Some(decode_typed_value(bytes)?),
                Some(_) => return Err(invalid(what)),
            };
            group_cells.push(AggregateResultCell {
                name: name.clone(),
                value,
            });
        }
        if group_cells
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
        {
            return Err(invalid("aggregate result cells are not canonical"));
        }
        decoded.push(group_cells);
    }
    Ok(decoded)
}

/// The public output columns an aggregate shape produces, sorted.
fn aggregate_output_names(query: &Query) -> Option<BTreeSet<&str>> {
    let aggregate = query.aggregate.as_ref()?;
    Some(
        aggregate
            .group_by
            .iter()
            .map(String::as_str)
            .chain(aggregate.aggregates.iter().map(|item| item.alias.as_str()))
            .collect(),
    )
}

/// Why a predicate read's aggregate evidence is malformed, or `None` when it
/// is well formed: an aggregate shape carries a canonical v1 result whose
/// every group has exactly the shape's output columns (and an ungrouped
/// aggregate has exactly one group), and any other shape carries none.
///
/// This is structural and needs no schema or data: an authority rejects a
/// commit that fails it as `MalformedCommit` rather than as a conflict.
pub fn aggregate_read_malformation(read: &PredicateRead) -> Option<String> {
    let expected = aggregate_output_names(&read.shape);
    let result = match (&expected, &read.aggregate_result) {
        (None, None) => return None,
        (None, Some(_)) => {
            return Some(
                "exclusive predicate read carries an aggregate result but reads no aggregate"
                    .to_owned(),
            );
        }
        (Some(_), None) => {
            return Some("exclusive aggregate read carries no aggregate result".to_owned());
        }
        (Some(_), Some(result)) => result,
    };
    let expected = expected.expect("aggregate shape");
    let groups = match decode_aggregate_read_result(result) {
        Ok(groups) => groups,
        Err(error) => {
            return Some(format!(
                "exclusive aggregate read result is malformed: {error}"
            ));
        }
    };
    let grouped = read
        .shape
        .aggregate
        .as_ref()
        .is_some_and(|aggregate| aggregate.group_by.is_some());
    if !grouped && groups.len() != 1 {
        return Some(
            "exclusive ungrouped aggregate read result must have exactly one group".to_owned(),
        );
    }
    for group in &groups {
        let names = group
            .iter()
            .map(|cell| cell.name.as_str())
            .collect::<BTreeSet<_>>();
        if names != expected {
            return Some(
                "exclusive aggregate read result columns differ from its shape".to_owned(),
            );
        }
    }
    None
}

/// The first aggregate-evidence malformation in `tx`'s predicate reads.
pub fn transaction_aggregate_read_malformation(tx: &Transaction) -> Option<String> {
    tx.predicate_read_set
        .iter()
        .flatten()
        .find_map(aggregate_read_malformation)
}

/// Byte-level fixtures for `jazz.aggregate-read-result.v1`. These sit below
/// the public API on purpose: the contract is the durable bytes themselves,
/// which no public call exposes. Conflict behaviour is covered end to end in
/// the exclusive transaction tests.
#[cfg(test)]
mod tests {
    use super::*;

    fn cell(name: &str, value: Option<(ValueType, Value)>) -> AggregateResultCell {
        AggregateResultCell {
            name: name.to_owned(),
            value,
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn count_read(result: Option<AggregateReadResult>) -> PredicateRead {
        let shape = Query::from("todos").count();
        PredicateRead {
            table: "todos".to_owned(),
            shape_id: crate::query::ShapeId(uuid::Uuid::from_bytes([0x44; 16])),
            shape,
            binding_id: crate::query::binding_id_for_values(&Default::default()).unwrap(),
            binding_values: Default::default(),
            aggregate_result: result,
        }
    }

    fn count_result(count: u64) -> AggregateReadResult {
        encode_aggregate_result_groups(vec![vec![cell(
            "count",
            Some((ValueType::U64, Value::U64(count))),
        )]])
        .unwrap()
    }

    /// Pins the v1 bytes of `count = 3`. A change here is a wire and storage
    /// format change and needs a new format version.
    const COUNT_3_V1_HEX: &str = "0101000000010000000a00000000636f756e7401001e000000000300000011000000180000000001000000010176616c756505000300000000000000";

    /// `count = N` and a grouped `(bucket, count, sum)` result have pinned,
    /// order-independent bytes and round-trip through the canonical decoder.
    #[test]
    fn aggregate_read_result_v1_bytes_are_pinned_and_canonical() {
        let count = count_result(3);
        assert_eq!(hex(&count.0), COUNT_3_V1_HEX);
        assert_eq!(
            decode_aggregate_read_result(&count).unwrap(),
            vec![vec![cell("count", Some((ValueType::U64, Value::U64(3))))]]
        );
        assert_ne!(count, count_result(4));

        let group = |bucket: &str, count: u64, sum: Option<i64>| {
            vec![
                cell(
                    "sum_score",
                    sum.map(|sum| (ValueType::I64, Value::I64(sum))),
                ),
                cell("count", Some((ValueType::U64, Value::U64(count)))),
                cell(
                    "bucket",
                    Some((ValueType::String, Value::String(bucket.to_owned()))),
                ),
            ]
        };
        let forward =
            encode_aggregate_result_groups(vec![group("a", 2, Some(3)), group("b", 1, None)])
                .unwrap();
        let backward =
            encode_aggregate_result_groups(vec![group("b", 1, None), group("a", 2, Some(3))])
                .unwrap();
        assert_eq!(forward, backward);
        let decoded = decode_aggregate_read_result(&forward).unwrap();
        assert_eq!(decoded.len(), 2);
        for group in &decoded {
            let names = group
                .iter()
                .map(|cell| cell.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(names, ["bucket", "count", "sum_score"]);
        }
        assert!(decoded.iter().any(|group| group[2].value.is_none()));
        assert!(matches!(
            encode_aggregate_result_groups(vec![group("a", 1, None), group("a", 1, None)]),
            Err(Error::InvalidStoredValue(
                "aggregate result has a duplicate group"
            ))
        ));
    }

    /// Truncated, extended, unknown-version and non-canonical bytes are
    /// refused by the decoder instead of being compared as a weaker result.
    #[test]
    fn aggregate_read_result_v1_rejects_malformed_bytes() {
        let good = count_result(3);
        let rejects = |bytes: Vec<u8>| {
            assert!(
                matches!(
                    decode_aggregate_read_result(&AggregateReadResult(bytes.clone())),
                    Err(Error::InvalidStoredValue(_))
                ),
                "{}",
                hex(&bytes)
            );
        };
        let mut truncated = good.0.clone();
        truncated.truncate(truncated.len() - 6);
        rejects(truncated);
        let mut trailing = good.0.clone();
        trailing.push(0);
        rejects(trailing);
        let mut future = good.0.clone();
        future[0] = 2;
        rejects(future);
        rejects(Vec::new());

        // A nullable wrapper is never part of a value's encoded type.
        let wrapped = encode_aggregate_result_groups(vec![vec![cell(
            "count",
            Some((
                ValueType::Nullable(Box::new(ValueType::U64)),
                Value::Nullable(Some(Box::new(Value::U64(3)))),
            )),
        )]])
        .unwrap();
        rejects(wrapped.0);
    }

    /// An aggregate predicate read must carry a well-formed result with the
    /// shape's exact output columns; any other read must carry none.
    #[test]
    fn aggregate_read_malformation_is_structural() {
        assert_eq!(
            aggregate_read_malformation(&count_read(Some(count_result(1)))),
            None
        );
        assert!(aggregate_read_malformation(&count_read(None)).is_some());
        assert!(
            aggregate_read_malformation(&count_read(Some(AggregateReadResult(vec![1, 2, 3]))))
                .is_some()
        );
        let wrong_column = encode_aggregate_result_groups(vec![vec![cell(
            "total",
            Some((ValueType::U64, Value::U64(1))),
        )]])
        .unwrap();
        assert!(aggregate_read_malformation(&count_read(Some(wrong_column))).is_some());
        let no_groups = encode_aggregate_result_groups(Vec::new()).unwrap();
        assert!(aggregate_read_malformation(&count_read(Some(no_groups))).is_some());

        let mut plain = count_read(Some(count_result(1)));
        plain.shape = Query::from("todos");
        assert!(aggregate_read_malformation(&plain).is_some());
        plain.aggregate_result = None;
        assert_eq!(aggregate_read_malformation(&plain), None);
    }
}
