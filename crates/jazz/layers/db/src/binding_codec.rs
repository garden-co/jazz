//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary. Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.
//!
//! The bytes are the ones the Groove engine produced, so the TypeScript
//! decoders are unchanged: each row is a packed record described by its
//! batch's descriptor. SQLite has no physical column catalogue, so every
//! application cell is published as a `ResultField` under its column name;
//! `StoredColumn` stays in the enum only to keep the variant tags stable.

mod publication_type;

use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::Serialize;

use crate::db::RelationSnapshot;
use crate::db::{CurrentRow, RemovedRow, SubscriptionOutputRow};
use crate::ids::{PhysicalColumnId, RowUuid};
use crate::object::ResultKey;
use crate::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

/// Encode only the descriptor portion of the named-cell input role. This is
/// also the owner for cross-language descriptor corpus generation.
pub fn encode_named_cell_descriptor(descriptor: &RecordDescriptor) -> Result<Vec<u8>, String> {
    postcard::to_allocvec(&publication_type::NativeDescriptor(descriptor))
        .map_err(|error| error.to_string())
}

/// Encode the named-cell input ABI used by both native hosts. The descriptor
/// contains names and recursive value types, never compiler field identities.
pub fn encode_named_cells(record: &OwnedRecord) -> Result<Vec<u8>, String> {
    postcard::to_allocvec(&(
        publication_type::NativeDescriptor(record.descriptor()),
        record.raw(),
    ))
    .map_err(|error| error.to_string())
}

/// Decode one canonical named-cell input envelope into write cells.
/// Both NAPI and WASM use this owner; execution descriptor serde is not accepted.
pub fn decode_named_cells(bytes: &[u8]) -> Result<BTreeMap<String, Value>, String> {
    let mut remaining = bytes;
    let descriptor = publication_type::read_descriptor(&mut remaining)?;
    let (raw, trailing): (Vec<u8>, _) =
        postcard::take_from_bytes(remaining).map_err(|error| error.to_string())?;
    if !trailing.is_empty() {
        return Err("trailing bytes in named-cell envelope".to_owned());
    }
    let record = OwnedRecord::new(raw, descriptor);
    if encode_named_cells(&record)? != bytes {
        return Err("noncanonical named-cell envelope".to_owned());
    }
    let values = record.to_values().map_err(|error| error.to_string())?;
    if descriptor
        .create(&values)
        .map_err(|error| error.to_string())?
        != record.raw()
    {
        return Err("noncanonical named-cell record".to_owned());
    }
    let mut cells = BTreeMap::new();
    for (field, value) in descriptor.fields().iter().zip(values) {
        let name = field
            .name
            .as_ref()
            .ok_or("encoded cells must use named fields")?;
        if cells.insert(name.clone(), value).is_some() {
            return Err("encoded cells contain duplicate names".to_owned());
        }
    }
    Ok(cells)
}

/// Rust-owned frozen v1 binding corpus. Test harnesses may expose this through
/// a generated host artifact, but consumers must still decode its payloads
/// through their ordinary production readers.
pub const BINDING_CODEC_GOLDEN_FIXTURE: &str =
    include_str!("../../../fixtures/binding_codec_golden.json");

/// The explicit binding provenance of one record-descriptor field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum RowDescriptorFieldName<'a> {
    /// A stored application column with catalogue identity and exact output
    /// name. Not produced on SQLite, which has no physical column catalogue.
    StoredColumn {
        /// Authoritative physical catalogue identity.
        id: PhysicalColumnId,
        /// Exact application output name.
        output_name: &'a str,
    },
    /// A derived or application result field with its exact name.
    ResultField {
        /// Exact result name.
        name: &'a str,
    },
    /// Explicit engine metadata; consumers must never hide a result by its name.
    HiddenMetadata {
        /// Exact metadata field name.
        name: &'a str,
    },
}

/// One native binding descriptor entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RowDescriptorField<'a> {
    /// Explicit field provenance and exact descriptor name.
    pub name: RowDescriptorFieldName<'a>,
    /// Exact type used to decode this record slot.
    #[serde(serialize_with = "publication_type::serialize")]
    pub value_type: ValueType,
}

/// A contiguous run of rows with one table and record descriptor.
#[derive(Clone, Debug, Serialize)]
pub struct RowBatch<'a> {
    /// Logical table name.
    pub table: &'a str,
    /// Exact tagged descriptor required to decode `rows.raw`.
    pub descriptor: Vec<RowDescriptorField<'a>>,
    /// Rows in producer order.
    pub rows: Vec<Row<'a>>,
}

/// One packed row inside a [`RowBatch`].
#[derive(Clone, Debug, Serialize)]
pub struct Row<'a> {
    /// Stable source row identity.
    pub row_id: RowUuid,
    /// Whether the row is an opt-in deleted historical row.
    pub deleted: bool,
    /// Packed record bytes described by the enclosing batch.
    pub raw: Cow<'a, [u8]>,
}

/// Relation snapshot envelope used by both native hosts.
#[derive(Clone, Debug, Serialize)]
pub struct RelationSnapshotPayload<'a> {
    /// Number of root rows at the beginning of the flattened batch sequence.
    pub root_count: u64,
    /// Root and related rows in producer order.
    pub rows: Vec<RowBatch<'a>>,
}

/// Incremental subscription envelope used by both native hosts.
#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionDeltaPayload<'a> {
    /// Newly visible rows.
    pub added: Vec<RowBatch<'a>>,
    /// Still-visible rows with changed bytes.
    pub updated: Vec<RowBatch<'a>>,
    /// Rows no longer visible.
    pub removed: Vec<RemovedRowPayload>,
    /// One opaque occurrence key per added row.
    pub added_occurrence_keys: Vec<ResultKey>,
    /// One opaque occurrence key per updated row.
    pub updated_occurrence_keys: Vec<ResultKey>,
    /// One opaque occurrence key per removed row.
    pub removed_occurrence_keys: Vec<ResultKey>,
    /// Authoritative post-frame position for each added row.
    pub added_indices: Vec<u64>,
    /// Authoritative pre-frame position for each updated row.
    pub updated_previous_indices: Vec<u64>,
    /// Authoritative post-frame position for each updated row.
    pub updated_indices: Vec<u64>,
    /// Authoritative pre-frame position for each removed row.
    pub removed_indices: Vec<u64>,
    /// For each updated row, the opted-in columns' splices from its
    /// previous result ([`SubscriptionOutputRow::splices`]).
    pub updated_splices: Vec<Vec<(String, Vec<crate::db::LargeValueDeltaSplice>)>>,
}

/// Removed-row wire identity.
#[derive(Clone, Debug, Serialize)]
pub struct RemovedRowPayload {
    /// Logical table name.
    pub table: String,
    /// Stable source row identity.
    pub row_id: RowUuid,
}

/// Encode a flat row sequence, preserving contiguous batch boundaries.
pub fn encode_rows(rows: &[CurrentRow]) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&row_batches(rows)?)
}

/// Encode a relation snapshot.
pub fn encode_relation_snapshot(snapshot: &RelationSnapshot) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&RelationSnapshotPayload {
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows)?,
    })
}

/// A row's occurrence: the row itself, or a flat join's root followed by
/// its joined source rows. Relations arrive as nested rows, not joined
/// outputs.
fn occurrence(
    row: RowUuid,
    version: Option<crate::values::SourceVersion>,
    joined: &[crate::values::JoinedRow],
    union_arm: Option<&str>,
) -> ResultKey {
    crate::values::occurrence_key(row, version, joined, union_arm)
}

/// Encode an incremental subscription delta with aligned occurrence sidecars.
pub fn encode_subscription_delta(
    added: &[SubscriptionOutputRow],
    updated: &[SubscriptionOutputRow],
    removed: &[RemovedRow],
) -> Result<Vec<u8>, postcard::Error> {
    let added_rows = added.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
    let updated_rows = updated
        .iter()
        .map(|row| row.row.clone())
        .collect::<Vec<_>>();
    postcard::to_allocvec(&SubscriptionDeltaPayload {
        added: row_batches(&added_rows)?,
        updated: row_batches(&updated_rows)?,
        removed: removed
            .iter()
            .map(|row| RemovedRowPayload {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_occurrence_keys: added
            .iter()
            .map(|row| {
                occurrence(
                    row.row_uuid(),
                    row.root_version(),
                    row.joined_rows(),
                    row.union_arm(),
                )
            })
            .collect(),
        updated_occurrence_keys: updated
            .iter()
            .map(|row| {
                occurrence(
                    row.row_uuid(),
                    row.root_version(),
                    row.joined_rows(),
                    row.union_arm(),
                )
            })
            .collect(),
        removed_occurrence_keys: removed
            .iter()
            .map(|row| {
                occurrence(
                    row.row_uuid,
                    row.version,
                    &row.joined,
                    row.union_arm.as_deref(),
                )
            })
            .collect(),
        added_indices: added.iter().map(|row| row.index as u64).collect(),
        updated_previous_indices: updated
            .iter()
            .map(|row| row.previous_index.unwrap_or(row.index) as u64)
            .collect(),
        updated_indices: updated.iter().map(|row| row.index as u64).collect(),
        removed_indices: removed.iter().map(|row| row.index as u64).collect(),
        updated_splices: updated.iter().map(|row| row.splices.clone()).collect(),
    })
}

/// One row as a packed record: its projected cells under their declared
/// types. An unwritten cell (`None`) is published as null, widening a
/// non-nullable slot to nullable.
pub fn row_record(row: &CurrentRow) -> Result<OwnedRecord, postcard::Error> {
    let mut fields = Vec::with_capacity(row.cell_types().len());
    let mut values = Vec::with_capacity(row.cell_types().len());
    for ((name, value), ty) in row.cells().zip(row.cell_types()) {
        let (ty, value) = match (value, ty) {
            (Some(value), ty) => (ty.clone(), value.clone()),
            (None, ValueType::Nullable(_)) => (ty.clone(), Value::Nullable(None)),
            (None, ty) => (
                ValueType::Nullable(Box::new(ty.clone())),
                Value::Nullable(None),
            ),
        };
        fields.push((name.to_owned(), ty));
        values.push(value);
    }
    let descriptor = RecordDescriptor::new(fields);
    let raw = descriptor
        .create(&values)
        .map_err(|_| postcard::Error::SerdeSerCustom)?;
    Ok(OwnedRecord::new(raw, descriptor))
}

/// [`row_record`] as a host reads it. History metadata (reserved names,
/// read only from `history.<table>`) is converted only here, after the
/// read: `$transactionId` becomes the public transaction id a write handle
/// reports, and `$acceptedAt` its HLC's milliseconds. Ordering and
/// occurrence identity keep the full values.
fn published_record(row: &CurrentRow) -> Result<OwnedRecord, postcard::Error> {
    use crate::schema::history::HistoryMetadataColumn;
    use crate::schema::history::{ACCEPTED_AT_COLUMN, TRANSACTION_ID_COLUMN};
    let record = row_record(row)?;
    if !row
        .cells()
        .any(|(name, _)| HistoryMetadataColumn::contains(name))
    {
        return Ok(record);
    }
    let mut fields = Vec::with_capacity(record.descriptor().fields().len());
    let mut values = Vec::with_capacity(fields.capacity());
    for ((name, value), field) in row.cells().zip(record.descriptor().fields()) {
        let value = value.cloned().unwrap_or(Value::Nullable(None));
        let (ty, value) = match (name, value) {
            (TRANSACTION_ID_COLUMN, Value::Tuple(parts)) => {
                let [Value::U64(time), Value::Uuid(node)] = parts.as_slice() else {
                    return Err(postcard::Error::SerdeSerCustom);
                };
                let tx = jazz_model::tx::TxId::new(
                    jazz_types::time::TxTime(*time),
                    jazz_types::ids::NodeUuid(*node),
                );
                let id = jazz_model::model::transaction::TransactionId::from_committed_tx(tx);
                (ValueType::String, Value::String(id.to_string()))
            }
            (ACCEPTED_AT_COLUMN, Value::U64(hlc)) => (
                ValueType::U64,
                Value::U64(jazz_types::time::GlobalTime(hlc).physical_ms()),
            ),
            (_, value) => (field.value_type.clone(), value),
        };
        fields.push((name.to_owned(), ty));
        values.push(value);
    }
    let descriptor = RecordDescriptor::new(fields);
    let raw = descriptor
        .create(&values)
        .map_err(|_| postcard::Error::SerdeSerCustom)?;
    Ok(OwnedRecord::new(raw, descriptor))
}

/// Group only adjacent rows with equal table and tagged descriptor.
pub fn row_batches(rows: &[CurrentRow]) -> Result<Vec<RowBatch<'_>>, postcard::Error> {
    let mut batches: Vec<RowBatch<'_>> = Vec::new();
    for row in rows {
        let record = published_record(row)?;
        let descriptor = row
            .cells()
            .zip(record.descriptor().fields())
            .map(|((name, _), field)| RowDescriptorField {
                name: RowDescriptorFieldName::ResultField { name },
                value_type: field.value_type.clone(),
            })
            .collect::<Vec<_>>();
        let packed = Row {
            row_id: row.row_uuid(),
            deleted: row.is_deleted(),
            raw: Cow::Owned(record.raw().to_vec()),
        };
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == descriptor => {
                batch.rows.push(packed);
            }
            _ => batches.push(RowBatch {
                table: row.table(),
                descriptor,
                rows: vec![packed],
            }),
        }
    }
    Ok(batches)
}

/// One decoded [`RowBatch`]: what a host's reader sees.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRowBatch {
    /// Logical table name.
    pub table: String,
    /// Rows in producer order.
    pub rows: Vec<DecodedRow>,
}

/// One decoded [`Row`] with its cells by result name.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRow {
    /// Stable source row identity.
    pub row_id: RowUuid,
    /// Whether the row is an opt-in deleted historical row.
    pub deleted: bool,
    /// Cells by their descriptor name; metadata fields included.
    pub cells: BTreeMap<String, Value>,
}

/// Decode an [`encode_rows`] payload the way a host's reader does.
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<DecodedRowBatch>, String> {
    let mut input = bytes;
    let batches = read_row_batches(&mut input)?;
    if !input.is_empty() {
        return Err("trailing bytes after row batches".to_owned());
    }
    Ok(batches)
}

/// Decode an [`encode_relation_snapshot`] payload: the root-row count and
/// the row batches.
pub fn decode_relation_snapshot(bytes: &[u8]) -> Result<(usize, Vec<DecodedRowBatch>), String> {
    let mut input = bytes;
    let root_count: u64 = publication_type::take(&mut input)?;
    let batches = read_row_batches(&mut input)?;
    if !input.is_empty() {
        return Err("trailing bytes after relation snapshot".to_owned());
    }
    Ok((root_count as usize, batches))
}

/// Decode the added and updated rows of an [`encode_subscription_delta`]
/// payload.
pub fn decode_subscription_delta_rows(
    bytes: &[u8],
) -> Result<(Vec<DecodedRowBatch>, Vec<DecodedRowBatch>), String> {
    let mut input = bytes;
    let added = read_row_batches(&mut input)?;
    let updated = read_row_batches(&mut input)?;
    Ok((added, updated))
}

fn read_row_batches(input: &mut &[u8]) -> Result<Vec<DecodedRowBatch>, String> {
    use publication_type::{count, read_type, take};
    let mut budget = 1 << 20;
    let batches = count(input, budget)?;
    let mut decoded = Vec::with_capacity(batches);
    for _ in 0..batches {
        let table: String = take(input)?;
        let fields = count(input, budget)?;
        let mut descriptor = Vec::with_capacity(fields);
        for _ in 0..fields {
            let tag: u32 = take(input)?;
            let name: String = match tag {
                1 | 2 => take(input)?,
                _ => return Err("stored-column descriptor fields are not decoded".to_owned()),
            };
            descriptor.push((name, read_type(input, &mut budget, 0)?));
        }
        let names = descriptor
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let descriptor = RecordDescriptor::new(descriptor);
        let rows = count(input, budget)?;
        let mut decoded_rows = Vec::with_capacity(rows);
        for _ in 0..rows {
            let row_id: RowUuid = take(input)?;
            let deleted: bool = take(input)?;
            let raw: &[u8] = take(input)?;
            let values = crate::records::BorrowedRecord::new(raw, &descriptor)
                .to_values()
                .map_err(|error| error.to_string())?;
            decoded_rows.push(DecodedRow {
                row_id,
                deleted,
                cells: names.iter().cloned().zip(values).collect(),
            });
        }
        budget = budget.saturating_sub(rows + fields);
        decoded.push(DecodedRowBatch {
            table,
            rows: decoded_rows,
        });
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::records::{DescriptorField, EnumCase, EnumSchema, EnumValue, FieldIdentity};

    // Pins host input bytes independently of Rust execution serde; database
    // tests alone cannot identify that framing.
    #[test]
    fn named_cell_input_uses_explicit_canonical_recursive_binding_types() {
        let fixture = b"\x01\x01\x05score\x03\x08\x2a\x00\x00\x00\x00\x00\x00\x00";
        let cells = decode_named_cells(fixture).unwrap();
        assert_eq!(cells.get("score"), Some(&Value::U64(42)));
        let descriptor =
            RecordDescriptor::new_with_fields([DescriptorField::new("score", ValueType::U64)
                .with_identity(FieldIdentity::Slot(900))]);
        let record = OwnedRecord::new(descriptor.create(&[Value::U64(42)]).unwrap(), descriptor);
        assert_eq!(encode_named_cells(&record).unwrap(), fixture);
        assert!(
            decode_named_cells(&postcard::to_allocvec(&(descriptor, record.raw())).unwrap())
                .is_err()
        );
        let mut trailing = fixture.to_vec();
        trailing.push(0);
        assert!(decode_named_cells(&trailing).is_err());
        let mut overlong = vec![0x81, 0];
        overlong.extend_from_slice(&fixture[1..]);
        assert!(decode_named_cells(&overlong).is_err());
    }

    #[test]
    fn named_cell_input_rejects_ambiguous_and_unbounded_descriptors_and_decodes_payload_enums() {
        for descriptor in [
            RecordDescriptor::new([("same", ValueType::U64), ("same", ValueType::U64)]),
            RecordDescriptor::new_with_fields([DescriptorField {
                name: None,
                value_type: ValueType::U64,
                identity: None,
            }]),
        ] {
            let raw = descriptor
                .create(&vec![Value::U64(1); descriptor.fields().len()])
                .unwrap();
            assert!(
                decode_named_cells(
                    &encode_named_cells(&OwnedRecord::new(raw, descriptor)).unwrap()
                )
                .is_err()
            );
        }
        assert!(
            decode_named_cells(b"\x01\x01\x01x\x12").is_err(),
            "unknown type tag"
        );
        let mut deep = b"\x01\x01\x01x".to_vec();
        deep.extend(std::iter::repeat_n(14, 128));
        deep.push(0);
        assert!(decode_named_cells(&deep).is_err(), "recursive type bound");
        let payload = RecordDescriptor::new([("literal", ValueType::U64)]);
        let enum_schema = EnumSchema::new("choice", [EnumCase::new("selected", payload)])
            .unwrap()
            .with_registry_id(7);
        let descriptor =
            RecordDescriptor::new([("choice", ValueType::Enum(Box::new(enum_schema.clone())))]);
        let value = Value::Enum(EnumValue::create(0, payload, &[Value::U64(42)]).unwrap());
        let record = OwnedRecord::new(
            descriptor.create(std::slice::from_ref(&value)).unwrap(),
            descriptor,
        );
        assert_eq!(
            decode_named_cells(&encode_named_cells(&record).unwrap())
                .unwrap()
                .get("choice"),
            Some(&value)
        );
    }

    fn todo(id: u8, title: Option<&str>, done: Option<bool>) -> CurrentRow {
        CurrentRow {
            table: Arc::from("todos"),
            id: RowUuid::from_bytes([id; 16]),
            seq: id as u64,
            deleted: false,
            cells: Arc::new(vec![
                (
                    "title".to_owned(),
                    title.map(|t| Value::String(t.to_owned())),
                ),
                ("done".to_owned(), done.map(Value::Bool)),
            ]),
            types: Arc::from([ValueType::String, ValueType::Bool]),
            joined: Arc::from([]),
            version: None,
            union_arm: None,
        }
    }

    /// The TypeScript reader's view of a batch, declared independently so
    /// the variant tags and field order are pinned here.
    #[derive(serde::Deserialize)]
    enum FrozenName {
        StoredColumn { _id: u64, _output_name: String },
        ResultField { name: String },
        HiddenMetadata { _name: String },
    }
    #[derive(serde::Deserialize)]
    struct FrozenRow {
        row_id: RowUuid,
        deleted: bool,
        raw: Vec<u8>,
    }
    #[derive(serde::Deserialize)]
    struct FrozenBatch {
        table: String,
        descriptor: Vec<(FrozenName, ValueType)>,
        rows: Vec<FrozenRow>,
    }

    #[test]
    fn sqlite_rows_publish_as_named_result_fields_in_packed_records() {
        let rows = [
            todo(1, Some("first"), Some(true)),
            todo(2, Some("second"), Some(false)),
            // An unwritten cell widens its slot, so it starts a new batch.
            todo(3, Some("third"), None),
        ];
        let read: Vec<FrozenBatch> = postcard::from_bytes(&encode_rows(&rows).unwrap()).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].table, "todos");
        assert_eq!(read[0].rows.len(), 2);
        let names = read[0]
            .descriptor
            .iter()
            .map(|(name, _)| match name {
                FrozenName::ResultField { name } => name.as_str(),
                _ => panic!("SQLite publishes application cells as result fields"),
            })
            .collect::<Vec<_>>();
        assert_eq!(names, ["title", "done"]);
        assert_eq!(
            read[1].descriptor[1].1,
            ValueType::Nullable(Box::new(ValueType::Bool))
        );

        let descriptor = RecordDescriptor::new(
            read[0]
                .descriptor
                .iter()
                .zip(["title", "done"])
                .map(|((_, ty), name)| (name, ty.clone())),
        );
        let first = OwnedRecord::new(read[0].rows[0].raw.clone(), descriptor);
        assert_eq!(
            first.to_values().unwrap(),
            [Value::String("first".into()), Value::Bool(true)]
        );
        assert_eq!(read[0].rows[1].row_id, RowUuid::from_bytes([2; 16]));
        assert!(!read[0].rows[1].deleted);
    }

    #[test]
    fn subscription_delta_keys_are_the_rows_single_source_occurrences() {
        let row = todo(7, Some("t"), Some(false));
        let delta = encode_subscription_delta(
            &[SubscriptionOutputRow {
                row: row.clone(),
                previous_index: None,
                index: 0,
                splices: Vec::new(),
            }],
            &[],
            &[RemovedRow {
                table: "todos".to_owned(),
                row_uuid: RowUuid::from_bytes([8; 16]),
                joined: Vec::new(),
                version: None,
                union_arm: None,
                index: 1,
            }],
        )
        .unwrap();
        #[derive(serde::Deserialize)]
        struct FrozenDelta {
            added: Vec<FrozenBatch>,
            _updated: Vec<FrozenBatch>,
            removed: Vec<(String, RowUuid)>,
            added_occurrence_keys: Vec<serde_bytes::ByteBuf>,
            _updated_occurrence_keys: Vec<serde_bytes::ByteBuf>,
            removed_occurrence_keys: Vec<serde_bytes::ByteBuf>,
            added_indices: Vec<u64>,
            _updated_previous_indices: Vec<u64>,
            _updated_indices: Vec<u64>,
            removed_indices: Vec<u64>,
        }
        let read: FrozenDelta = postcard::from_bytes(&delta).unwrap();
        assert_eq!(read.added[0].rows[0].row_id, row.row_uuid());
        assert_eq!(
            read.removed,
            [("todos".to_owned(), RowUuid::from_bytes([8; 16]))]
        );
        assert_eq!(
            (read.added_indices, read.removed_indices),
            (vec![0], vec![1])
        );
        // Key version 2, the root uuid, no joined rows, no union arms, and a
        // current (not historical) root source.
        let key = |id: u8| [&[2][..], &[id; 16], &[0; 4], &[0; 4], &[0]].concat();
        assert_eq!(read.added_occurrence_keys[0].as_ref(), key(7));
        assert_eq!(read.removed_occurrence_keys[0].as_ref(), key(8));
    }
}
