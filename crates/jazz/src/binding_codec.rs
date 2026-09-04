//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary.  Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.

use serde::{Deserialize, Serialize};

use crate::db::{RemovedRow, SubscriptionOutputRow};
use crate::ids::RowUuid;
use crate::node::{CurrentRow, CurrentRowBindingField, RelationSnapshot};
use crate::tools::ResultKey;
use crate::wire::decode_postcard_exact;
use groove::ivm::TerminalOperation;
use groove::records::RecordDescriptor;

/// Rust-owned frozen v1 binding corpus. Test harnesses may expose this through
/// a generated host artifact, but consumers must still decode its payloads
/// through their ordinary production readers.
pub const BINDING_CODEC_GOLDEN_FIXTURE: &str =
    include_str!("../fixtures/binding_codec_golden.json");

/// The explicit binding provenance of one record-descriptor field.
///
/// This is deliberately part of the native-host descriptor, rather than
/// inferred from a field name. A collector descriptor may mix Jazz's physical
/// `user_{column}` fields with logical fields that legitimately use the same
/// spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowDescriptorFieldName<'a> {
    /// A persisted CurrentRow field using Jazz's private physical name.
    PhysicalColumn(#[serde(borrow)] &'a str),
    /// A query, relation, or collector field using its public logical name.
    LogicalField(#[serde(borrow)] &'a str),
}

/// One native binding descriptor entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDescriptorField<'a> {
    /// Explicit field provenance and exact descriptor name.
    #[serde(borrow)]
    pub name: RowDescriptorFieldName<'a>,
    /// Exact Groove type used to decode this record slot.
    pub value_type: groove::records::ValueType,
}

/// A contiguous run of rows with one table and record descriptor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowBatch<'a> {
    /// Logical table name.
    #[serde(borrow)]
    pub table: &'a str,
    /// Exact tagged descriptor required to decode `rows.raw`.
    #[serde(borrow)]
    pub descriptor: Vec<RowDescriptorField<'a>>,
    /// Rows in producer order.
    #[serde(borrow)]
    pub rows: Vec<Row<'a>>,
}

/// One packed row inside a [`RowBatch`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row<'a> {
    /// Stable source row identity.
    pub row_id: RowUuid,
    /// Whether the row is an opt-in deleted historical row.
    pub deleted: bool,
    /// Packed record bytes described by the enclosing batch.
    #[serde(borrow)]
    pub raw: &'a [u8],
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
    postcard::to_allocvec(&row_batches(rows))
}

/// Decode one canonical flat row sequence produced by [`encode_rows`].
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<RowBatch<'_>>, postcard::Error> {
    decode_postcard_exact(bytes)
}

/// Encode a relation snapshot.
pub fn encode_relation_snapshot(snapshot: &RelationSnapshot) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&RelationSnapshotPayload {
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows),
    })
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
        added: row_batches(&added_rows),
        updated: row_batches(&updated_rows),
        removed: removed
            .iter()
            .map(|row| RemovedRowPayload {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_occurrence_keys: added
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        updated_occurrence_keys: updated
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        removed_occurrence_keys: removed
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        added_indices: added.iter().map(|row| row.index as u64).collect(),
        updated_previous_indices: updated
            .iter()
            .map(|row| row.previous_index.unwrap_or(row.index) as u64)
            .collect(),
        updated_indices: updated.iter().map(|row| row.index as u64).collect(),
        removed_indices: removed.iter().map(|row| row.index as u64).collect(),
    })
}

/// Materialize a Groove descriptor from the shared binding descriptor.
///
/// The explicit physical/logical provenance tags remain in `descriptor`; this
/// helper exists only for consumers that need Groove's typed record reader for
/// the corresponding raw row bytes.
pub fn descriptor_record(descriptor: &[RowDescriptorField<'_>]) -> RecordDescriptor {
    RecordDescriptor::new(descriptor.iter().map(|field| {
        (
            match field.name {
                RowDescriptorFieldName::PhysicalColumn(name)
                | RowDescriptorFieldName::LogicalField(name) => name.to_owned(),
            },
            field.value_type.clone(),
        )
    }))
}

/// Group only adjacent rows with equal table and tagged descriptor.
pub fn row_batches(rows: &[CurrentRow]) -> Vec<RowBatch<'_>> {
    let mut batches: Vec<RowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        let binding_descriptor = descriptor
            .fields()
            .iter()
            .zip(row.binding_fields())
            .zip(row.binding_field_names())
            .map(|((field, binding), public_name)| {
                let name = field
                    .name
                    .as_deref()
                    .expect("native row descriptor fields must be named");
                let name = match binding {
                    CurrentRowBindingField::PhysicalColumn => {
                        assert!(
                            public_name.is_none(),
                            "physical native binding fields cannot override their private name"
                        );
                        RowDescriptorFieldName::PhysicalColumn(name)
                    }
                    CurrentRowBindingField::LogicalField => {
                        RowDescriptorFieldName::LogicalField(public_name.as_deref().unwrap_or(name))
                    }
                };
                RowDescriptorField {
                    name,
                    value_type: field.value_type.clone(),
                }
            })
            .collect::<Vec<_>>();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == binding_descriptor => {
                batch.rows.push(Row {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                });
            }
            _ => batches.push(RowBatch {
                table: row.table(),
                descriptor: binding_descriptor,
                rows: vec![Row {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                }],
            }),
        }
    }
    batches
}

/// Encode terminal operations in the JavaScript-native object shape.
pub fn terminal_operations_to_json(
    operations: &[TerminalOperation],
) -> Result<serde_json::Value, serde_json::Error> {
    let mut encoded = serde_json::to_value(operations)?;
    if operations.is_empty() {
        return Ok(encoded);
    }
    let encoded_operations = encoded
        .as_array_mut()
        .expect("terminal operations serialize as an array");
    for wire in encoded_operations {
        let serde_json::Value::Object(wire) = wire else {
            unreachable!("terminal operation serializes as an object");
        };
        wire.remove("root_descriptor");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::CurrentRowBindingField;
    use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

    #[test]
    fn tagged_descriptor_preserves_hybrid_physical_and_logical_user_check_fields() {
        // The field tags exist only at this internal host ABI boundary. Keep
        // this direct regression here so a hybrid collector cannot silently
        // collapse physical `check` and logical `user_check` into one plan.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("user_check".to_owned(), ValueType::String),
            ("user_check".to_owned(), ValueType::Bytes),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5c; 16])),
                Value::String("physical".to_owned()),
                Value::Bytes(b"logical".to_vec()),
            ])
            .expect("encode hybrid record");
        let row = CurrentRow::new_with_explicit_binding_fields(
            "notes",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingField::PhysicalColumn,
                CurrentRowBindingField::PhysicalColumn,
                CurrentRowBindingField::LogicalField,
            ],
        );

        let rows = [row];
        let batches = row_batches(&rows);
        assert_eq!(batches.len(), 1);
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::PhysicalColumn("user_check")
        ));
        assert!(matches!(
            batches[0].descriptor[2].name,
            RowDescriptorFieldName::LogicalField("user_check")
        ));
    }

    #[test]
    fn tagged_descriptor_uses_terminal_public_name_for_logical_source_carrier() {
        // A lowered projection may retain `user_title` as its source slot but
        // expose `title`. The producer supplies that mapping so hosts do not
        // accidentally apply a prefix heuristic to all logical fields.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("user_title".to_owned(), ValueType::String),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5d; 16])),
                Value::String("projected".to_owned()),
            ])
            .expect("encode projected record");
        let row = CurrentRow::new_with_explicit_binding_fields_and_names(
            "todos",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingField::LogicalField,
                CurrentRowBindingField::LogicalField,
            ],
            vec![None, Some("title".to_owned())],
        );

        let rows = [row];
        let batches = row_batches(&rows);
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::LogicalField("title")
        ));
    }

    #[test]
    fn decode_rows_round_trips_the_shared_tagged_descriptor_and_raw_record() {
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            (
                "user_title".to_owned(),
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ]);
        let row_id = uuid::Uuid::from_bytes([0x6a; 16]);
        let raw = descriptor
            .create(&[
                Value::Uuid(row_id),
                Value::Nullable(Some(Box::new(Value::String("roundtrip".to_owned())))),
            ])
            .expect("encode shared row");
        let row = CurrentRow::new_with_explicit_binding_fields(
            "todos",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingField::PhysicalColumn,
                CurrentRowBindingField::PhysicalColumn,
            ],
        );

        let encoded = encode_rows(&[row]).expect("encode rows");
        let decoded = decode_rows(&encoded).expect("decode rows");

        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0].descriptor[1].name,
            RowDescriptorFieldName::PhysicalColumn(name) if name == "user_title"
        ));
        let descriptor = descriptor_record(&decoded[0].descriptor);
        let values = groove::records::BorrowedRecord::new(&decoded[0].rows[0].raw, &descriptor)
            .to_values()
            .expect("decode raw record");
        assert_eq!(values[0], Value::Uuid(row_id));
        assert_eq!(
            values[1],
            Value::Nullable(Some(Box::new(Value::String("roundtrip".to_owned()))))
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(
            decode_rows(&trailing).is_err(),
            "the shared decoder must reject bytes after the one canonical row payload"
        );
    }
}
