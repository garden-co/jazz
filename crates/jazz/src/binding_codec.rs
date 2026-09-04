//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary.  Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.

use serde::Serialize;

use crate::db::{RemovedRow, SubscriptionOutputRow};
use crate::ids::{PhysicalColumnId, RowUuid};
use crate::node::{CurrentRow, CurrentRowBindingField, RelationSnapshot};
use crate::tools::ResultKey;
use groove::ivm::TerminalOperation;

/// Rust-owned frozen v1 binding corpus. Test harnesses may expose this through
/// a generated host artifact, but consumers must still decode its payloads
/// through their ordinary production readers.
pub const BINDING_CODEC_GOLDEN_FIXTURE: &str =
    include_str!("../fixtures/binding_codec_golden.json");

/// The explicit binding provenance of one record-descriptor field.
///
/// This is deliberately part of the native-host descriptor, rather than
/// inferred from a field name. A collector descriptor may mix stored
/// `_app_{column}` carriers with result fields that legitimately use the same
/// spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum RowDescriptorFieldName<'a> {
    /// A stored application column with its exact physical id and public name.
    StoredColumn {
        /// Node-local physical column id for this stored application column.
        id: PhysicalColumnId,
        /// Public output name that this stored column should appear under.
        output_name: &'a str,
    },
    /// A query, relation, collector, or synthetic result field.
    ResultField {
        /// Public result-field name exposed to the native host.
        name: &'a str,
    },
}

/// One native binding descriptor entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RowDescriptorField<'a> {
    /// Explicit field provenance and exact descriptor name.
    pub name: RowDescriptorFieldName<'a>,
    /// Exact Groove type used to decode this record slot.
    pub value_type: groove::records::ValueType,
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
            .zip(row.binding_field_column_ids())
            .map(|(((field, binding), public_name), column_id)| {
                let name = field
                    .name
                    .as_deref()
                    .expect("native row descriptor fields must be named");
                let name = match binding {
                    CurrentRowBindingField::StoredColumn => RowDescriptorFieldName::StoredColumn {
                        id: column_id
                            .expect("stored native binding fields must carry a physical column id"),
                        output_name: public_name.as_deref().unwrap_or(name),
                    },
                    CurrentRowBindingField::ResultField => RowDescriptorFieldName::ResultField {
                        name: public_name.as_deref().unwrap_or(name),
                    },
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
    use crate::ids::PhysicalColumnId;
    use crate::node::CurrentRowBindingField;
    use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

    #[test]
    fn tagged_descriptor_preserves_hybrid_stored_and_result_app_check_fields() {
        // The field tags exist only at this internal host ABI boundary. Keep
        // this direct regression here so a hybrid collector cannot silently
        // collapse stored `check` and result `_app_check` into one plan.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("_app_check".to_owned(), ValueType::String),
            ("_app_check".to_owned(), ValueType::Bytes),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5c; 16])),
                Value::String("physical".to_owned()),
                Value::Bytes(b"logical".to_vec()),
            ])
            .expect("encode hybrid record");
        let row = CurrentRow::new_with_explicit_binding_fields_and_names_and_ids(
            "notes",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingField::ResultField,
                CurrentRowBindingField::StoredColumn,
                CurrentRowBindingField::ResultField,
            ],
            vec![
                None,
                Some("check".to_owned()),
                Some("_app_check".to_owned()),
            ],
            vec![None, Some(PhysicalColumnId(7)), None],
        );

        let rows = [row];
        let batches = row_batches(&rows);
        assert_eq!(batches.len(), 1);
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::StoredColumn {
                id: PhysicalColumnId(7),
                output_name: "check",
            }
        ));
        assert!(matches!(
            batches[0].descriptor[2].name,
            RowDescriptorFieldName::ResultField { name: "_app_check" }
        ));
    }

    #[test]
    fn tagged_descriptor_uses_terminal_public_name_for_result_source_carrier() {
        // A lowered projection may retain `_app_title` as its source slot but
        // expose `title`. The producer supplies that mapping so hosts do not
        // accidentally apply a prefix heuristic to all result fields.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("_app_title".to_owned(), ValueType::String),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5d; 16])),
                Value::String("projected".to_owned()),
            ])
            .expect("encode projected record");
        let row = CurrentRow::new_with_explicit_binding_fields_and_names_and_ids(
            "todos",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingField::ResultField,
                CurrentRowBindingField::ResultField,
            ],
            vec![None, Some("title".to_owned())],
            vec![None, None],
        );

        let rows = [row];
        let batches = row_batches(&rows);
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::ResultField { name: "title" }
        ));
    }
}
