//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary.  Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.

use serde::Serialize;

use crate::db::{RemovedRow, SubscriptionOutputRow};
use crate::ids::RowUuid;
use crate::node::{CurrentRow, RelationSnapshot};
use crate::tools::ResultKey;
use groove::ivm::TerminalOperation;
use groove::records::RecordDescriptor;

/// Rust-owned frozen v1 binding corpus. Test harnesses may expose this through
/// a generated host artifact, but consumers must still decode its payloads
/// through their ordinary production readers.
pub const BINDING_CODEC_GOLDEN_FIXTURE: &str =
    include_str!("../fixtures/binding_codec_golden.json");

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
