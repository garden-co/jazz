//! `jazz.exclusive-read-evidence.v2`: a serializable transaction's snapshot and
//! read sets, kept in `jazz_transactions` slots 5-8 while its fate is pending.
//!
//! Outbox and relay recovery rebuild commit units from storage. Without the
//! evidence, a unit that was in flight at a restart would reach the authority
//! with no read proof and be rejected as a conflict (garden-co/jazz#3663).
//! Each slot holds the Groove typed-record v1 bytes of one fixed descriptor
//! with a leading format byte; nested query and binding bytes reuse the
//! pinned native query Postcard codec and canonical `jazz-binding-v0` bytes.
//!
//! v2 changes only the predicate-read slot, whose format byte is now `2`:
//! each read carries the canonical `jazz.aggregate-read-result.v1` bytes of
//! the result an aggregate read observed, null for any other read
//! (garden-co/jazz#3768). The snapshot, row-read and absent-read slots keep
//! their v1 bytes and `format_v1 = 1`. A v1 predicate-read slot is refused:
//! it cannot say what an aggregate read returned.
//! The family id and record names keep their pre-rename spelling for format
//! compatibility. See SPEC 2 §2.8 and `dev/proofs/exclusive-read-evidence-storage-compatibility.md`.

use super::Error;
use crate::ids::{NodeUuid, RowUuid};
use crate::query::{
    BindingId, Query, ShapeId, binding_values_from_canonical_bytes,
    canonical_binding_bytes_for_values,
};
use crate::time::{GlobalTime, TxTime};
use crate::tx::{
    AbsentRead, AggregateReadResult, PredicateRead, RowRead, Snapshot, Transaction, TxId, TxKind,
};
use groove::records::{OwnedRecord, Record, RecordDescriptor, Value, ValueType};

const FORMAT_V1: u8 = 1;
/// Format byte of the predicate-read slot, which carries aggregate results.
const PREDICATE_READS_FORMAT_V2: u8 = 2;

fn dot_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("time", ValueType::U64), ("node", ValueType::Uuid)])
}

fn row_read_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("table", ValueType::String),
        ("row_uuid", ValueType::Uuid),
        ("version_time", ValueType::U64),
        ("version_node", ValueType::Uuid),
    ])
}

fn absent_read_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("table", ValueType::String), ("row_uuid", ValueType::Uuid)])
}

fn predicate_read_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("table", ValueType::String),
        ("shape_id", ValueType::Uuid),
        ("query", ValueType::Bytes),
        ("binding_id", ValueType::Uuid),
        ("bindings", ValueType::Bytes),
        (
            "aggregate_result",
            ValueType::Nullable(Box::new(ValueType::Bytes)),
        ),
    ])
}

/// `jazz_exclusive_base_snapshot_v1`
pub(super) fn base_snapshot_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v1", ValueType::U8),
        ("owner", ValueType::Uuid),
        ("global_base", ValueType::U64),
        ("local_base", ValueType::U64),
        (
            "dots",
            ValueType::Record(Box::new(dot_descriptor())).array_of(),
        ),
    ])
}

fn reads_descriptor(item: RecordDescriptor) -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v1", ValueType::U8),
        ("reads", ValueType::Record(Box::new(item)).array_of()),
    ])
}

/// `jazz_exclusive_row_reads_v1`
pub(super) fn row_reads_descriptor() -> RecordDescriptor {
    reads_descriptor(row_read_descriptor())
}

/// `jazz_exclusive_absent_reads_v1`
pub(super) fn absent_reads_descriptor() -> RecordDescriptor {
    reads_descriptor(absent_read_descriptor())
}

/// `jazz_exclusive_predicate_reads_v2`
pub(super) fn predicate_reads_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v2", ValueType::U8),
        (
            "reads",
            ValueType::Record(Box::new(predicate_read_descriptor())).array_of(),
        ),
    ])
}

fn nested(descriptor: &RecordDescriptor, values: &[Value]) -> Result<Value, Error> {
    Ok(Value::Record(OwnedRecord::new(
        descriptor.create(values)?,
        descriptor.clone(),
    )))
}

fn slot(bytes: Option<Vec<u8>>) -> Value {
    Value::Nullable(bytes.map(|bytes| Box::new(Value::Bytes(bytes))))
}

fn encode_base_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>, Error> {
    let dot = dot_descriptor();
    let dots = snapshot
        .dots
        .iter()
        .map(|dot_id| {
            nested(
                &dot,
                &[Value::U64(dot_id.time.0), Value::Uuid(dot_id.node.0)],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(base_snapshot_descriptor().create(&[
        Value::U8(FORMAT_V1),
        Value::Uuid(snapshot.owner.0),
        Value::U64(snapshot.global_base.0),
        Value::U64(snapshot.local_base.0),
        Value::Array(dots),
    ])?)
}

fn encode_reads(
    descriptor: RecordDescriptor,
    format: u8,
    item: &RecordDescriptor,
    reads: Vec<Vec<Value>>,
) -> Result<Vec<u8>, Error> {
    let reads = reads
        .iter()
        .map(|values| nested(item, values))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(descriptor.create(&[Value::U8(format), Value::Array(reads)])?)
}

fn encode_row_reads(reads: &[RowRead]) -> Result<Vec<u8>, Error> {
    encode_reads(
        row_reads_descriptor(),
        FORMAT_V1,
        &row_read_descriptor(),
        reads
            .iter()
            .map(|read| {
                vec![
                    Value::String(read.table.clone()),
                    Value::Uuid(read.row_uuid.0),
                    Value::U64(read.version.time.0),
                    Value::Uuid(read.version.node.0),
                ]
            })
            .collect(),
    )
}

fn encode_absent_reads(reads: &[AbsentRead]) -> Result<Vec<u8>, Error> {
    encode_reads(
        absent_reads_descriptor(),
        FORMAT_V1,
        &absent_read_descriptor(),
        reads
            .iter()
            .map(|read| {
                vec![
                    Value::String(read.table.clone()),
                    Value::Uuid(read.row_uuid.0),
                ]
            })
            .collect(),
    )
}

/// `None` when a predicate read cannot be represented in v2 (for example a
/// record-valued binding, which `jazz-binding-v0` cannot be decoded back
/// from). The caller then stores no evidence at all.
fn encode_predicate_reads(reads: &[PredicateRead]) -> Result<Option<Vec<u8>>, Error> {
    let mut items = Vec::with_capacity(reads.len());
    for read in reads {
        let Ok(query) = postcard::to_allocvec(&read.shape) else {
            return Ok(None);
        };
        let Ok(bindings) = canonical_binding_bytes_for_values(&read.binding_values) else {
            return Ok(None);
        };
        if binding_values_from_canonical_bytes(&bindings).is_err() || decode_query(&query).is_err()
        {
            return Ok(None);
        }
        items.push(vec![
            Value::String(read.table.clone()),
            Value::Uuid(read.shape_id.0),
            Value::Bytes(query),
            Value::Uuid(read.binding_id.0),
            Value::Bytes(bindings),
            aggregate_result_value(read.aggregate_result.as_ref()),
        ]);
    }
    encode_reads(
        predicate_reads_descriptor(),
        PREDICATE_READS_FORMAT_V2,
        &predicate_read_descriptor(),
        items,
    )
    .map(Some)
}

/// Storage values for slots 5-8 of a `jazz_transactions` row.
///
/// Evidence is kept only for a serializable transaction whose fate is still
/// pending: that is the only time a stored unit may be retransmitted for
/// validation. It is all-or-nothing; any component that v1 cannot represent
/// leaves every slot null, which fails closed exactly as before this format.
pub(super) fn evidence_slot_values(tx: &Transaction, pending: bool) -> Result<[Value; 4], Error> {
    let none = || [slot(None), slot(None), slot(None), slot(None)];
    if !pending || tx.kind != TxKind::Serializable {
        return Ok(none());
    }
    let Some(snapshot) = &tx.base_snapshot else {
        return Ok(none());
    };
    let predicate = match &tx.predicate_read_set {
        None => None,
        Some(reads) => match encode_predicate_reads(reads)? {
            Some(bytes) => Some(bytes),
            None => return Ok(none()),
        },
    };
    let base = encode_base_snapshot(snapshot)?;
    let rows = tx
        .row_read_set
        .as_deref()
        .map(encode_row_reads)
        .transpose()?;
    let absent = tx
        .absent_read_set
        .as_deref()
        .map(encode_absent_reads)
        .transpose()?;
    // Store only evidence the decoder reads back as exactly this transaction's
    // evidence. A relay persists downstream units it did not author, so a
    // malformed unit (for example a binding id that does not match its
    // bindings) must not become a row every later read rejects. Anything
    // that does not round-trip is stored as no evidence and fails closed.
    let round_trips = decode_evidence_slots(
        Some(&base),
        rows.as_deref(),
        absent.as_deref(),
        predicate.as_deref(),
    )
    .is_ok_and(|decoded| {
        decoded.base_snapshot.as_ref() == Some(snapshot)
            && decoded.row_read_set == tx.row_read_set
            && decoded.absent_read_set == tx.absent_read_set
            && decoded.predicate_read_set == tx.predicate_read_set
    });
    if !round_trips {
        return Ok(none());
    }
    Ok([slot(Some(base)), slot(rows), slot(absent), slot(predicate)])
}

fn invalid(message: &'static str) -> Error {
    Error::InvalidStoredValue(message)
}

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

fn aggregate_result_value(result: Option<&AggregateReadResult>) -> Value {
    Value::Nullable(result.map(|result| Box::new(Value::Bytes(result.0.clone()))))
}

fn versioned_reads(
    descriptor: &RecordDescriptor,
    format: u8,
    bytes: &[u8],
    what: &'static str,
) -> Result<Vec<Vec<Value>>, Error> {
    let values = canonical_values(descriptor, bytes, what)?;
    let [Value::U8(found), Value::Array(reads)] = values.as_slice() else {
        return Err(invalid(what));
    };
    if *found != format {
        return Err(invalid(what));
    }
    reads
        .iter()
        .map(|read| match read {
            Value::Record(record) => record.to_values().map_err(|_| invalid(what)),
            _ => Err(invalid(what)),
        })
        .collect()
}

fn decode_query(bytes: &[u8]) -> Result<Query, Error> {
    let what = "noncanonical serializable predicate query";
    let (query, rest): (Query, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| invalid(what))?;
    if !rest.is_empty() || postcard::to_allocvec(&query).map_err(|_| invalid(what))? != bytes {
        return Err(invalid(what));
    }
    Ok(query)
}

fn decode_base_snapshot(bytes: &[u8]) -> Result<Snapshot, Error> {
    let what = "invalid serializable base snapshot v1";
    let values = canonical_values(&base_snapshot_descriptor(), bytes, what)?;
    let [
        Value::U8(FORMAT_V1),
        Value::Uuid(owner),
        Value::U64(global_base),
        Value::U64(local_base),
        Value::Array(dots),
    ] = values.as_slice()
    else {
        return Err(invalid(what));
    };
    let dots = dots
        .iter()
        .map(|dot| {
            let Value::Record(dot) = dot else {
                return Err(invalid(what));
            };
            match dot.to_values().map_err(|_| invalid(what))?.as_slice() {
                [Value::U64(time), Value::Uuid(node)] => {
                    Ok(TxId::new(TxTime(*time), NodeUuid(*node)))
                }
                _ => Err(invalid(what)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Snapshot::serializable_base(
        NodeUuid(*owner),
        GlobalTime(*global_base),
        TxTime(*local_base),
        dots,
    )
    .map_err(|_| invalid(what))
}

fn decode_row_reads(bytes: &[u8]) -> Result<Vec<RowRead>, Error> {
    let what = "invalid serializable row reads v1";
    versioned_reads(&row_reads_descriptor(), FORMAT_V1, bytes, what)?
        .into_iter()
        .map(|read| match read.as_slice() {
            [
                Value::String(table),
                Value::Uuid(row),
                Value::U64(time),
                Value::Uuid(node),
            ] => Ok(RowRead {
                table: table.clone(),
                row_uuid: RowUuid(*row),
                version: TxId::new(TxTime(*time), NodeUuid(*node)),
            }),
            _ => Err(invalid(what)),
        })
        .collect()
}

fn decode_absent_reads(bytes: &[u8]) -> Result<Vec<AbsentRead>, Error> {
    let what = "invalid serializable absent reads v1";
    versioned_reads(&absent_reads_descriptor(), FORMAT_V1, bytes, what)?
        .into_iter()
        .map(|read| match read.as_slice() {
            [Value::String(table), Value::Uuid(row)] => Ok(AbsentRead {
                table: table.clone(),
                row_uuid: RowUuid(*row),
            }),
            _ => Err(invalid(what)),
        })
        .collect()
}

fn decode_predicate_reads(bytes: &[u8]) -> Result<Vec<PredicateRead>, Error> {
    let what = "invalid serializable predicate reads v2";
    versioned_reads(
        &predicate_reads_descriptor(),
        PREDICATE_READS_FORMAT_V2,
        bytes,
        what,
    )?
    .into_iter()
    .map(|read| match read.as_slice() {
        [
            Value::String(table),
            Value::Uuid(shape_id),
            Value::Bytes(query),
            Value::Uuid(binding_id),
            Value::Bytes(bindings),
            Value::Nullable(aggregate_result),
        ] => {
            let binding_values =
                binding_values_from_canonical_bytes(bindings).map_err(|_| invalid(what))?;
            if BindingId(uuid::Uuid::new_v5(&crate::query::QUERY_NAMESPACE, bindings))
                != BindingId(*binding_id)
            {
                return Err(invalid("serializable predicate binding id mismatch"));
            }
            let aggregate_result = match aggregate_result.as_deref() {
                None => None,
                Some(Value::Bytes(bytes)) => Some(AggregateReadResult(bytes.clone())),
                Some(_) => return Err(invalid(what)),
            };
            let read = PredicateRead {
                table: table.clone(),
                shape_id: ShapeId(*shape_id),
                shape: decode_query(query)?,
                binding_id: BindingId(*binding_id),
                binding_values,
                aggregate_result,
            };
            // The result is canonical and matches the shape, or the
            // stored evidence is corrupt.
            if super::aggregate_read_result::aggregate_read_malformation(&read).is_some() {
                return Err(invalid("invalid serializable aggregate read result"));
            }
            Ok(read)
        }
        _ => Err(invalid(what)),
    })
    .collect()
}

/// Decoded slots 5-8. A null slot is valid and means "no evidence".
pub(super) struct StoredEvidence {
    pub(super) base_snapshot: Option<Snapshot>,
    pub(super) row_read_set: Option<Vec<RowRead>>,
    pub(super) absent_read_set: Option<Vec<AbsentRead>>,
    pub(super) predicate_read_set: Option<Vec<PredicateRead>>,
}

pub(super) fn decode_evidence_slots(
    base_snapshot: Option<&[u8]>,
    row_read_set: Option<&[u8]>,
    absent_read_set: Option<&[u8]>,
    predicate_read_set: Option<&[u8]>,
) -> Result<StoredEvidence, Error> {
    if base_snapshot.is_none()
        && (row_read_set.is_some() || absent_read_set.is_some() || predicate_read_set.is_some())
    {
        return Err(invalid(
            "serializable read sets stored without a base snapshot",
        ));
    }
    Ok(StoredEvidence {
        base_snapshot: base_snapshot.map(decode_base_snapshot).transpose()?,
        row_read_set: row_read_set.map(decode_row_reads).transpose()?,
        absent_read_set: absent_read_set.map(decode_absent_reads).transpose()?,
        predicate_read_set: predicate_read_set.map(decode_predicate_reads).transpose()?,
    })
}

/// Byte-level fixtures for the stored format. These sit below the public API
/// on purpose: the contract is the durable bytes themselves, which no public
/// call exposes. The replay behaviour is covered end to end in the db tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::AuthorSubject;
    use crate::query::binding_id_for_values;
    use std::collections::BTreeMap;

    fn node(byte: u8) -> NodeUuid {
        NodeUuid::from_bytes([byte; 16])
    }

    fn row(byte: u8) -> RowUuid {
        RowUuid::from_bytes([byte; 16])
    }

    fn predicate_read() -> PredicateRead {
        let binding_values = BTreeMap::from([("done".to_owned(), Value::Bool(true))]);
        PredicateRead {
            table: "todos".to_owned(),
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x44; 16])),
            shape: Query::from("todos"),
            binding_id: binding_id_for_values(&binding_values).unwrap(),
            binding_values,
            aggregate_result: None,
        }
    }

    /// `count(*)` over `todos` that observed `count = 3`.
    fn aggregate_read() -> PredicateRead {
        let binding_values = BTreeMap::new();
        PredicateRead {
            table: "todos".to_owned(),
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x66; 16])),
            shape: Query::from("todos").count(),
            binding_id: binding_id_for_values(&binding_values).unwrap(),
            binding_values,
            aggregate_result: Some(
                crate::node::aggregate_read_result::encode_aggregate_result_groups(vec![vec![
                    crate::node::aggregate_read_result::AggregateResultCell {
                        name: "count".to_owned(),
                        value: Some((ValueType::U64, Value::U64(3))),
                    },
                ]])
                .unwrap(),
            ),
        }
    }

    fn fixture() -> Transaction {
        Transaction {
            tx_id: TxId::new(TxTime::from(20), node(1)),
            kind: TxKind::Serializable,
            n_total_writes: 1,
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            base_snapshot: Some(
                Snapshot::serializable_base(
                    node(1),
                    GlobalTime(3),
                    TxTime::from(12),
                    vec![
                        TxId::new(TxTime::from(10), node(1)),
                        TxId::new(TxTime::from(11), node(2)),
                    ],
                )
                .unwrap(),
            ),
            row_read_set: Some(vec![RowRead {
                table: "todos".to_owned(),
                row_uuid: row(0x22),
                version: TxId::new(TxTime::from(10), node(1)),
            }]),
            absent_read_set: Some(vec![AbsentRead {
                table: "todos".to_owned(),
                row_uuid: row(0x33),
            }]),
            predicate_read_set: Some(vec![predicate_read(), aggregate_read()]),
            user_metadata_json: None,
            contribution_merge: None,
        }
    }

    fn slot_bytes(value: &Value) -> Option<Vec<u8>> {
        match value {
            Value::Nullable(None) => None,
            Value::Nullable(Some(inner)) => match inner.as_ref() {
                Value::Bytes(bytes) => Some(bytes.clone()),
                other => panic!("slot is not bytes: {other:?}"),
            },
            other => panic!("slot is not nullable: {other:?}"),
        }
    }

    fn encoded(tx: &Transaction) -> [Option<Vec<u8>>; 4] {
        evidence_slot_values(tx, true)
            .unwrap()
            .each_ref()
            .map(slot_bytes)
    }

    fn decode(slots: &[Option<Vec<u8>>; 4]) -> Result<StoredEvidence, Error> {
        decode_evidence_slots(
            slots[0].as_deref(),
            slots[1].as_deref(),
            slots[2].as_deref(),
            slots[3].as_deref(),
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    const BASE_SNAPSHOT_V1_HEX: &str = "010101010101010101010101010101010103000000000000000000300000000000020000002000000000002800000000000101010101010101010101010101010100002c000000000002020202020202020202020202020202";
    const ROW_READS_V1_HEX: &str = "01010000002222222222222222222222222222222200002800000000000101010101010101010101010101010100746f646f73";
    const ABSENT_READS_V1_HEX: &str = "01010000003333333333333333333333333333333300746f646f73";
    const PREDICATE_READS_V2_HEX: &str = "02020000007600000044444444444444444444444444444444d327cd149e8f531dbb3fd829eba589f232000000470000006d00000000746f646f730005746f646f730000000000000000000000000000006a617a7a2d62696e64696e672d763000000000000000010000000000000004646f6e6506010066666666666666666666666666666666174cabfb8e53551b800edfb67ee4514f32000000510000006900000000746f646f730005746f646f73000000000000000000000101000005636f756e7400000000006a617a7a2d62696e64696e672d7630000000000000000001000101000000010000000a00000000636f756e7401001e000000000300000011000000180000000001000000010176616c756505000300000000000000";

    /// `jazz.exclusive-read-evidence.v2` bytes are pinned: a change here is a
    /// storage format change and needs a new format version. The snapshot,
    /// row-read and absent-read slots keep their v1 bytes; the predicate-read
    /// slot is v2 and carries the aggregate read's `count = 3` result.
    #[test]
    fn serializable_read_evidence_v2_bytes_are_pinned() {
        let tx = fixture();
        let slots = encoded(&tx);
        let actual = slots.each_ref().map(|slot| hex(slot.as_deref().unwrap()));
        assert_eq!(
            actual,
            [
                BASE_SNAPSHOT_V1_HEX,
                ROW_READS_V1_HEX,
                ABSENT_READS_V1_HEX,
                PREDICATE_READS_V2_HEX,
            ]
        );
        let decoded = decode(&slots).unwrap();
        assert_eq!(decoded.base_snapshot, tx.base_snapshot);
        assert_eq!(decoded.row_read_set, tx.row_read_set);
        assert_eq!(decoded.absent_read_set, tx.absent_read_set);
        assert_eq!(decoded.predicate_read_set, tx.predicate_read_set);
    }

    /// Evidence exists only for a pending serializable transaction, and is
    /// all-or-nothing: a predicate read v1 cannot represent stores nothing,
    /// which fails closed exactly like a row without evidence.
    #[test]
    fn serializable_read_evidence_is_written_only_when_replayable() {
        let tx = fixture();
        let none = [None, None, None, None];
        assert_eq!(
            evidence_slot_values(&tx, false)
                .unwrap()
                .each_ref()
                .map(slot_bytes),
            none
        );
        let mut mergeable = tx.clone();
        mergeable.kind = TxKind::Mergeable;
        assert_eq!(encoded(&mergeable), none);

        let record = RecordDescriptor::new([("x", ValueType::U8)]);
        let mut unrepresentable = tx.clone();
        unrepresentable.predicate_read_set.as_mut().unwrap()[0]
            .binding_values
            .insert("r".to_owned(), nested(&record, &[Value::U8(1)]).unwrap());
        assert_eq!(encoded(&unrepresentable), none);

        let mut mismatched = tx.clone();
        mismatched.predicate_read_set.as_mut().unwrap()[0].binding_id =
            BindingId(uuid::Uuid::from_bytes([0x55; 16]));
        assert_eq!(encoded(&mismatched), none);

        let mut empty_reads = tx.clone();
        empty_reads.row_read_set = None;
        empty_reads.absent_read_set = Some(Vec::new());
        empty_reads.predicate_read_set = None;
        let slots = encoded(&empty_reads);
        assert!(slots[0].is_some() && slots[1].is_none() && slots[3].is_none());
        let decoded = decode(&slots).unwrap();
        assert_eq!(decoded.row_read_set, None);
        assert_eq!(decoded.absent_read_set, Some(Vec::new()));
    }

    /// Malformed, non-canonical and unknown-version bytes are rejected as
    /// corrupt storage instead of being read as weaker evidence.
    #[test]
    fn serializable_read_evidence_rejects_malformed_noncanonical_and_unknown_versions() {
        let tx = fixture();
        let good = encoded(&tx);
        let rejects = |slots: [Option<Vec<u8>>; 4]| {
            let result = decode(&slots);
            assert!(
                matches!(result, Err(Error::InvalidStoredValue(_))),
                "{:?}: {:?}",
                slots.each_ref().map(|slot| slot.as_deref().map(hex)),
                result.map(|_| ())
            );
        };
        // Groove delimits a record's last variable field by the value's own
        // length, so a cut inside the trailing table name of a row or absent
        // read is just a different name; the store owns value integrity.
        // Cuts into fixed-width fields, and into the nested query and
        // binding bytes, are structural and must be refused.
        for index in [0, 3] {
            let mut truncated = good.clone();
            let bytes = truncated[index].as_mut().unwrap();
            bytes.truncate(bytes.len() - 1);
            rejects(truncated);

            let mut trailing = good.clone();
            trailing[index].as_mut().unwrap().push(0);
            rejects(trailing);
        }
        for index in 0..3 {
            // Every record leads with `format_v1`; an unknown version is refused.
            let mut future = good.clone();
            let bytes = future[index].as_mut().unwrap();
            let format = bytes.iter().position(|byte| *byte == FORMAT_V1).unwrap();
            bytes[format] = 2;
            rejects(future);
        }
        // The predicate-read slot leads with format 2. A v1 slot, which
        // cannot say what an aggregate read returned, and a later version are
        // both refused.
        assert_eq!(good[3].as_ref().unwrap()[0], PREDICATE_READS_FORMAT_V2);
        for format in [FORMAT_V1, PREDICATE_READS_FORMAT_V2 + 1] {
            let mut other = good.clone();
            other[3].as_mut().unwrap()[0] = format;
            rejects(other);
        }

        let mut orphaned = good.clone();
        orphaned[0] = None;
        rejects(orphaned);

        let predicate = |read: PredicateRead, query: Vec<u8>, bindings: Vec<u8>| {
            let mut slots = good.clone();
            slots[3] = Some(
                encode_reads(
                    predicate_reads_descriptor(),
                    PREDICATE_READS_FORMAT_V2,
                    &predicate_read_descriptor(),
                    vec![vec![
                        Value::String(read.table),
                        Value::Uuid(read.shape_id.0),
                        Value::Bytes(query),
                        Value::Uuid(read.binding_id.0),
                        Value::Bytes(bindings),
                        aggregate_result_value(read.aggregate_result.as_ref()),
                    ]],
                )
                .unwrap(),
            );
            slots
        };
        let read = predicate_read();
        let query = postcard::to_allocvec(&read.shape).unwrap();
        let bindings = canonical_binding_bytes_for_values(&read.binding_values).unwrap();
        decode(&predicate(read.clone(), query.clone(), bindings.clone())).unwrap();

        let mut long_query = query.clone();
        long_query.push(0);
        rejects(predicate(read.clone(), long_query, bindings.clone()));

        let other_values = BTreeMap::from([("done".to_owned(), Value::Bool(false))]);
        let other_bindings = canonical_binding_bytes_for_values(&other_values).unwrap();
        rejects(predicate(read.clone(), query.clone(), other_bindings));

        let mut long_bindings = bindings;
        long_bindings.push(0);
        rejects(predicate(read, query, long_bindings));

        // An aggregate read must carry a canonical result that matches its
        // shape, and no other read may carry one.
        let aggregate = aggregate_read();
        let aggregate_query = postcard::to_allocvec(&aggregate.shape).unwrap();
        let no_bindings = canonical_binding_bytes_for_values(&BTreeMap::new()).unwrap();
        decode(&predicate(
            aggregate.clone(),
            aggregate_query.clone(),
            no_bindings.clone(),
        ))
        .unwrap();
        let mut missing = aggregate.clone();
        missing.aggregate_result = None;
        rejects(predicate(
            missing,
            aggregate_query.clone(),
            no_bindings.clone(),
        ));
        let mut garbled = aggregate.clone();
        garbled.aggregate_result.as_mut().unwrap().0.push(0);
        rejects(predicate(garbled, aggregate_query, no_bindings));
        let mut stray = predicate_read();
        stray.aggregate_result = aggregate.aggregate_result;
        let stray_query = postcard::to_allocvec(&stray.shape).unwrap();
        let stray_bindings = canonical_binding_bytes_for_values(&stray.binding_values).unwrap();
        rejects(predicate(stray, stray_query, stray_bindings));
    }
}
