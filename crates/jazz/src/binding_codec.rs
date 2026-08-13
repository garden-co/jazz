//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary.  Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.

use serde::Serialize;

use crate::db::{RemovedRow, SubscriptionOutputRow, TerminalRootCarrier, TerminalRootLayout};
use crate::ids::RowUuid;
use crate::node::{CurrentRow, RelationSnapshot};
use crate::tools::ResultKey;
use groove::ivm::TerminalOperation;
use groove::records::RecordDescriptor;

/// A contiguous run of rows with one table and physical record descriptor.
#[derive(Clone, Debug, Serialize)]
pub struct RowBatch<'a> {
    /// Logical table name.
    pub table: &'a str,
    /// Exact descriptor required to decode `rows.raw`.
    pub descriptor: RecordDescriptor,
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
    })
}

/// Group only adjacent rows with equal table and descriptor.
pub fn row_batches(rows: &[CurrentRow]) -> Vec<RowBatch<'_>> {
    let mut batches: Vec<RowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(Row {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                });
            }
            _ => batches.push(RowBatch {
                table: row.table(),
                descriptor: descriptor.clone(),
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

/// Encode a terminal root layout in the JavaScript-native object shape.
pub fn terminal_layout_to_json(
    layout: &TerminalRootLayout,
) -> Result<serde_json::Value, postcard::Error> {
    let descriptor = postcard::to_allocvec(&layout.root_descriptor)?;
    Ok(serde_json::json!({
        "id": layout.id,
        "rootDescriptor": descriptor,
        "rootKeySlot": layout.root_key_slot,
        "rootKeyFieldName": layout.root_key_field_name,
        "publicFields": layout.public_fields.iter().map(|field| serde_json::json!({
            "name": field.name,
            "descriptorFieldName": field.descriptor_field_name,
            "slot": field.slot,
            "carrier": terminal_carrier_name(field.carrier),
        })).collect::<Vec<_>>(),
        "carrier": terminal_carrier_name(layout.carrier),
    }))
}

/// Encode terminal operations in the JavaScript-native object shape.
pub fn terminal_operations_to_json(
    operations: &[TerminalOperation],
    root_layout_id: &str,
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
        wire.insert(
            "rootLayoutId".to_owned(),
            serde_json::Value::String(root_layout_id.to_owned()),
        );
    }
    Ok(encoded)
}

fn terminal_carrier_name(carrier: TerminalRootCarrier) -> &'static str {
    match carrier {
        TerminalRootCarrier::CurrentRow => "CurrentRow",
        TerminalRootCarrier::Logical => "Logical",
    }
}
