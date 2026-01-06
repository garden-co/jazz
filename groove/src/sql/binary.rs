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
use crate::sql::row::{Row, Value};

/// Delta type tags for binary encoding
pub const DELTA_ADDED: u8 = 1;
pub const DELTA_UPDATED: u8 = 2;
pub const DELTA_REMOVED: u8 = 3;

/// Encode multiple rows to binary format for WASM transfer.
pub fn encode_rows(rows: &[Row]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    // Encode each row
    for row in rows {
        encode_row_to_buf(&mut buf, row);
    }

    buf
}

/// Encode a single row to binary format (no count header).
/// Used for delta updates where each row is passed individually.
pub fn encode_single_row(row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_row_to_buf(&mut buf, row);
    buf
}

/// Encode a single delta to binary format.
/// Format: u8 type + row data (or just id for removes)
pub fn encode_delta(delta: &RowDelta) -> Vec<u8> {
    let mut buf = Vec::new();

    match delta {
        RowDelta::Added(row) => {
            buf.push(DELTA_ADDED);
            encode_row_to_buf(&mut buf, row);
        }
        RowDelta::Updated { new, .. } => {
            buf.push(DELTA_UPDATED);
            encode_row_to_buf(&mut buf, new);
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

/// Encode a single row (id + values) to the buffer.
fn encode_row_to_buf(buf: &mut Vec<u8>, row: &Row) {
    // ObjectId as 26-byte Base32 string
    let id_str = row.id.to_string();
    debug_assert_eq!(id_str.len(), 26, "ObjectId string should be 26 chars");
    buf.extend_from_slice(id_str.as_bytes());

    // Encode each value
    for value in &row.values {
        encode_value(buf, value);
    }
}

/// Encode a single value to the buffer.
///
/// Nullability is self-describing via Value::NullableSome and Value::NullableNone.
/// NullableSome writes presence byte 1 + inner value.
/// NullableNone writes presence byte 0.
fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::NullableNone => {
            // Null: write presence byte 0
            buf.push(0);
        }
        Value::NullableSome(inner) => {
            // Present: write presence byte 1 + inner value
            buf.push(1);
            encode_value(buf, inner);
        }
        Value::Bool(b) => {
            buf.push(if *b { 1 } else { 0 });
        }
        Value::I32(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::U32(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::I64(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::F64(n) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Bytes(b) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Ref(id) => {
            // ObjectId as 26-byte Base32 string
            let id_str = id.to_string();
            buf.extend_from_slice(id_str.as_bytes());
        }
        Value::Row(row) => {
            // Nested row: encode id + values inline
            encode_row_to_buf(buf, row);
        }
        Value::Array(arr) => {
            // Array: count + elements
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for elem in arr {
                encode_value(buf, elem);
            }
        }
        Value::Blob(content_ref) => {
            // Blob: serialize ContentRef bytes with length prefix
            // Format: u32 length + ContentRef bytes
            let blob_bytes = content_ref.to_row_bytes();
            buf.extend_from_slice(&(blob_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&blob_bytes);
        }
        Value::BlobArray(refs) => {
            // BlobArray: count + each blob's serialized ContentRef
            buf.extend_from_slice(&(refs.len() as u32).to_le_bytes());
            for content_ref in refs {
                let blob_bytes = content_ref.to_row_bytes();
                buf.extend_from_slice(&(blob_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&blob_bytes);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;

    #[test]
    fn encode_simple_row() {
        let id = ObjectId::new(12345);
        let row = Row::new(id, vec![
            Value::I32(42),
            Value::String("hello".into()),
            Value::Bool(true),
        ]);

        let buf = encode_rows(&[row]);

        // Header: 4 bytes (row count = 1)
        assert_eq!(u32::from_le_bytes(buf[0..4].try_into().unwrap()), 1);

        // ObjectId: 26 bytes
        let id_str = std::str::from_utf8(&buf[4..30]).unwrap();
        assert_eq!(id_str, id.to_string());

        // I32: 4 bytes
        assert_eq!(i32::from_le_bytes(buf[30..34].try_into().unwrap()), 42);

        // String: 4 bytes length + 5 bytes "hello"
        assert_eq!(u32::from_le_bytes(buf[34..38].try_into().unwrap()), 5);
        assert_eq!(&buf[38..43], b"hello");

        // Bool: 1 byte
        assert_eq!(buf[43], 1);
    }

    #[test]
    fn encode_nested_row() {
        let outer_id = ObjectId::new(100);
        let inner_id = ObjectId::new(200);

        let inner_row = Row::new(inner_id, vec![Value::I32(100)]);
        let outer_row = Row::new(outer_id, vec![
            Value::String("outer".into()),
            Value::Row(Box::new(inner_row)),
        ]);

        let buf = encode_rows(&[outer_row]);

        // Should contain both ObjectId strings
        let buf_str = String::from_utf8_lossy(&buf);
        assert!(buf_str.contains(&outer_id.to_string()));
        assert!(buf_str.contains(&inner_id.to_string()));
    }

    #[test]
    fn encode_array_of_rows() {
        let id = ObjectId::new(1);
        let inner_id1 = ObjectId::new(2);
        let inner_id2 = ObjectId::new(3);

        let row = Row::new(id, vec![
            Value::Array(vec![
                Value::Row(Box::new(Row::new(inner_id1, vec![Value::I32(1)]))),
                Value::Row(Box::new(Row::new(inner_id2, vec![Value::I32(2)]))),
            ]),
        ]);

        let buf = encode_rows(&[row]);

        // Expected size:
        // - Header: 4 bytes (row count)
        // - Outer row id: 26 bytes
        // - Array count: 4 bytes
        // - Inner row 1: 26 bytes (id) + 4 bytes (i32)
        // - Inner row 2: 26 bytes (id) + 4 bytes (i32)
        let expected_min = 4 + 26 + 4 + 2 * (26 + 4);
        assert_eq!(buf.len(), expected_min);

        // Check array count
        let array_count_offset = 4 + 26; // after header and outer id
        assert_eq!(u32::from_le_bytes(buf[array_count_offset..array_count_offset+4].try_into().unwrap()), 2);
    }
}
