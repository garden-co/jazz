//! Binary encoding for WASM boundary.
//!
//! This module provides efficient binary encoding of rows for transfer to JavaScript.
//! The format is designed for fast JS decoding with DataView.
//!
//! ## Batch Format (for full query results)
//!
//! ```text
//! Header:
//!   u32: row_count
//!
//! For each row:
//!   [26 bytes]: ObjectId as Base32 UTF-8 string
//!   [column values in schema order...]
//! ```
//!
//! ## Single Row Format (for delta updates)
//!
//! ```text
//! [26 bytes]: ObjectId as Base32 UTF-8 string
//! [column values in schema order...]
//! ```
//!
//! ## Delta Format (for incremental updates)
//!
//! ```text
//! u8: delta_type (1=added, 2=updated, 3=removed)
//! For added/updated: [single row format]
//! For removed: [26 bytes ObjectId only]
//! ```
//!
//! ## Column value encoding:
//!   - bool:   1 byte (0 or 1)
//!   - i32:    4 bytes LE
//!   - u32:    4 bytes LE
//!   - i64:    8 bytes LE
//!   - f64:    8 bytes LE
//!   - Ref:    26 bytes (ObjectId as Base32 UTF-8)
//!   - String: u32 length + UTF-8 bytes
//!   - Bytes:  u32 length + raw bytes
//!   - Row:    [26 bytes id] + nested column values inline
//!   - Array:  u32 count + elements
//!
//! Nullable columns: 1-byte flag (0=null, 1=present) before value

use crate::object::ObjectId;
use crate::sql::query_graph::{DeltaBatch, RowDelta};
use crate::sql::row_buffer::{OwnedRow, RowRef, RowValue};

/// Delta type tags for binary encoding
pub const DELTA_ADDED: u8 = 1;
pub const DELTA_UPDATED: u8 = 2;
pub const DELTA_REMOVED: u8 = 3;

/// Encode multiple rows to binary format for WASM transfer.
pub fn encode_rows(rows: &[(ObjectId, OwnedRow)]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    // Encode each row
    for (id, row) in rows {
        encode_owned_row_to_buf(&mut buf, *id, row.as_ref());
    }

    buf
}

/// Encode a single row to binary format (no count header).
/// Used for delta updates where each row is passed individually.
pub fn encode_single_row(id: ObjectId, row: &OwnedRow) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_owned_row_to_buf(&mut buf, id, row.as_ref());
    buf
}

/// Encode a single delta to binary format.
/// Format: u8 type + row data (or just id for removes)
pub fn encode_delta(delta: &RowDelta) -> Vec<u8> {
    let mut buf = Vec::new();

    match delta {
        RowDelta::Added { id, row } => {
            buf.push(DELTA_ADDED);
            encode_owned_row_to_buf(&mut buf, *id, row.as_ref());
        }
        RowDelta::Updated { id, row, .. } => {
            buf.push(DELTA_UPDATED);
            encode_owned_row_to_buf(&mut buf, *id, row.as_ref());
        }
        RowDelta::Removed { id, .. } => {
            buf.push(DELTA_REMOVED);
            encode_object_id(&mut buf, *id);
        }
    }

    buf
}

/// Encode a batch of deltas, returning individual buffers for each.
/// This allows JS to process each delta independently.
pub fn encode_delta_batch(batch: &DeltaBatch) -> Vec<Vec<u8>> {
    batch.iter().map(encode_delta).collect()
}

/// Encode an ObjectId to the buffer (26 bytes Base32 UTF-8).
fn encode_object_id(buf: &mut Vec<u8>, id: ObjectId) {
    let id_str = id.to_string();
    debug_assert_eq!(id_str.len(), 26, "ObjectId string should be 26 chars");
    buf.extend_from_slice(id_str.as_bytes());
}

// ============================================================================
// Row Buffer Encoding Functions
// ============================================================================

/// Encode multiple OwnedRows to binary format for WASM transfer.
///
/// Note: Row IDs must be provided separately since they're out-of-band in row_buffer.
pub fn encode_owned_rows(rows: &[(ObjectId, &OwnedRow)]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    // Encode each row
    for (id, row) in rows {
        encode_owned_row_to_buf(&mut buf, *id, row.as_ref());
    }

    buf
}

/// Encode a single OwnedRow to binary format.
pub fn encode_single_owned_row(id: ObjectId, row: &OwnedRow) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_owned_row_to_buf(&mut buf, id, row.as_ref());
    buf
}

/// Encode a RowRef to the buffer.
fn encode_owned_row_to_buf(buf: &mut Vec<u8>, id: ObjectId, row: RowRef<'_>) {
    // ObjectId as 26-byte Base32 string
    encode_object_id(buf, id);

    // Encode each value using the descriptor
    for (col_idx, col) in row.descriptor.columns.iter().enumerate() {
        if let Some(value) = row.get(col_idx) {
            encode_row_value(buf, &value, col.nullable);
        }
    }
}

/// Encode a RowValue to the buffer.
fn encode_row_value(buf: &mut Vec<u8>, value: &RowValue<'_>, nullable: bool) {
    // Handle nullable wrapper
    if nullable {
        match value {
            RowValue::Null => {
                buf.push(0);
                return;
            }
            _ => {
                buf.push(1);
            }
        }
    }

    match value {
        RowValue::Null => {
            // Non-nullable null shouldn't happen, but handle gracefully
            buf.push(0);
        }
        RowValue::Bool(b) => {
            buf.push(if *b { 1 } else { 0 });
        }
        RowValue::I32(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        RowValue::U32(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        RowValue::I64(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        RowValue::F64(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        RowValue::String(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        RowValue::Bytes(b) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        RowValue::Ref(id) => {
            encode_object_id(buf, *id);
        }
        RowValue::Blob(content_ref) => {
            let blob_bytes = content_ref.to_row_bytes();
            buf.extend_from_slice(&(blob_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&blob_bytes);
        }
        RowValue::BlobArray(refs) => {
            buf.extend_from_slice(&(refs.len() as u32).to_le_bytes());
            for content_ref in refs {
                let blob_bytes = content_ref.to_row_bytes();
                buf.extend_from_slice(&(blob_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&blob_bytes);
            }
        }
        RowValue::Array(arr) => {
            // Encode as: count (u32) + each item's buffer with length prefix
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for item in arr.iter() {
                let item_data = item.buffer;
                buf.extend_from_slice(&(item_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(item_data);
            }
        }
    }
}

// ============================================================================
// Legacy Value Encoding Functions
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor};
    use crate::sql::schema::ColumnType;
    use std::sync::Arc;

    #[test]
    fn encode_owned_row_simple() {
        // Build a row descriptor with fixed-size columns
        let descriptor = Arc::new(RowDescriptor::new([
            ("age".to_string(), ColumnType::I32, false),
            ("score".to_string(), ColumnType::F64, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]));

        // Get column indices
        let age_idx = descriptor.column_index("age").unwrap();
        let score_idx = descriptor.column_index("score").unwrap();
        let active_idx = descriptor.column_index("active").unwrap();

        // Build a row
        let row = RowBuilder::new(descriptor.clone())
            .set_i32(age_idx, 42)
            .set_f64(score_idx, 95.5)
            .set_bool(active_idx, true)
            .build();

        let id = ObjectId::new(12345);
        let buf = encode_single_owned_row(id, &row);

        // Check ObjectId (26 bytes)
        let id_str = std::str::from_utf8(&buf[0..26]).unwrap();
        assert_eq!(id_str, id.to_string());

        // Check I32 (4 bytes) - columns are in descriptor order
        assert_eq!(i32::from_le_bytes(buf[26..30].try_into().unwrap()), 42);

        // Check F64 (8 bytes)
        assert_eq!(f64::from_le_bytes(buf[30..38].try_into().unwrap()), 95.5);

        // Check Bool (1 byte)
        assert_eq!(buf[38], 1);
    }

    #[test]
    fn encode_owned_rows_batch() {
        let descriptor = Arc::new(RowDescriptor::new([
            ("value".to_string(), ColumnType::I32, false),
        ]));

        let value_idx = descriptor.column_index("value").unwrap();

        let row1 = RowBuilder::new(descriptor.clone())
            .set_i32(value_idx, 100)
            .build();
        let row2 = RowBuilder::new(descriptor.clone())
            .set_i32(value_idx, 200)
            .build();

        let id1 = ObjectId::new(1);
        let id2 = ObjectId::new(2);

        let buf = encode_owned_rows(&[(id1, &row1), (id2, &row2)]);

        // Header: row count = 2
        assert_eq!(u32::from_le_bytes(buf[0..4].try_into().unwrap()), 2);

        // First row: id + i32
        let id1_str = std::str::from_utf8(&buf[4..30]).unwrap();
        assert_eq!(id1_str, id1.to_string());
        assert_eq!(i32::from_le_bytes(buf[30..34].try_into().unwrap()), 100);

        // Second row: id + i32
        let id2_str = std::str::from_utf8(&buf[34..60]).unwrap();
        assert_eq!(id2_str, id2.to_string());
        assert_eq!(i32::from_le_bytes(buf[60..64].try_into().unwrap()), 200);
    }

    #[test]
    fn encode_owned_row_with_string() {
        let descriptor = Arc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
        ]));

        let name_idx = descriptor.column_index("name").unwrap();

        let row = RowBuilder::new(descriptor.clone())
            .set_string(name_idx, "hello")
            .build();

        let id = ObjectId::new(999);
        let buf = encode_single_owned_row(id, &row);

        // ObjectId: 26 bytes
        assert_eq!(std::str::from_utf8(&buf[0..26]).unwrap(), id.to_string());

        // String: u32 length (5) + "hello"
        assert_eq!(u32::from_le_bytes(buf[26..30].try_into().unwrap()), 5);
        assert_eq!(&buf[30..35], b"hello");
    }

    #[test]
    fn encode_owned_row_with_nullable() {
        let descriptor = Arc::new(RowDescriptor::new([
            ("maybe_num".to_string(), ColumnType::I32, true),
        ]));

        let idx = descriptor.column_index("maybe_num").unwrap();

        // Row with present value
        let row_present = RowBuilder::new(descriptor.clone())
            .set_i32(idx, 42)
            .build();

        let id = ObjectId::new(111);
        let buf = encode_single_owned_row(id, &row_present);

        // ObjectId: 26 bytes, then presence byte (1) + i32
        assert_eq!(buf[26], 1); // present
        assert_eq!(i32::from_le_bytes(buf[27..31].try_into().unwrap()), 42);

        // Row with null value
        let row_null = RowBuilder::new(descriptor.clone())
            .set_null(idx)
            .build();

        let buf_null = encode_single_owned_row(id, &row_null);

        // ObjectId: 26 bytes, then presence byte (0) - no value follows
        assert_eq!(buf_null[26], 0); // null
        assert_eq!(buf_null.len(), 27); // just id + null flag
    }
}
