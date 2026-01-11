//! Binary encoding for WASM boundary.
//!
//! This module provides efficient binary encoding of rows for transfer to JavaScript.
//! Rows are passed as raw row buffer bytes, avoiding re-encoding overhead.
//!
//! ## Batch Format (for full query results)
//!
//! ```text
//! Header:
//!   u32: row_count
//!
//! For each row:
//!   u32: row_size (bytes)
//!   [row buffer bytes] (id is first 16 bytes as ObjectId/u128 LE)
//! ```
//!
//! ## Single Row Format
//!
//! ```text
//! [row buffer bytes] (id is first 16 bytes as ObjectId/u128 LE)
//! ```
//!
//! ## Delta Format (for incremental updates)
//!
//! ```text
//! u8: delta_type (1=added, 2=updated, 3=removed)
//! For added/updated: [row buffer bytes]
//! For removed: [16 bytes ObjectId only]
//! ```
//!
//! ## Row Buffer Format
//!
//! Rows use the unified row buffer format from `row_buffer.rs`:
//!
//! ```text
//! [fixed-size columns][u32 offset₂][u32 offset₃]...[var_data₁][var_data₂]...
//! ```
//!
//! The first fixed-size column is always `id` (ObjectId, 16 bytes LE).
//!
//! Fixed-size column types and their byte sizes:
//!   - ObjectId: 16 bytes (u128 LE) - always first, the row's identity
//!   - bool:   1 byte (0 or 1)
//!   - i32:    4 bytes LE
//!   - u32:    4 bytes LE
//!   - i64:    8 bytes LE
//!   - f64:    8 bytes LE
//!   - Ref:    16 bytes (ObjectId as u128 LE)
//!
//! Nullable fixed columns have a 1-byte presence flag before the value.
//!
//! Variable-size columns come after the offset table. For N variable columns,
//! N-1 offsets are stored. The first variable column starts after the offset
//! table. The last variable column ends at the buffer end.

use crate::object::ObjectId;
use crate::sql::query_graph::{DeltaBatch, RowDelta};
use crate::sql::row_buffer::OwnedRow;

/// Delta type tags for binary encoding
pub const DELTA_ADDED: u8 = 1;
pub const DELTA_UPDATED: u8 = 2;
pub const DELTA_REMOVED: u8 = 3;

/// Encode multiple rows to binary format for WASM transfer.
///
/// Format: `[u32 count][u32 size₁][row₁][u32 size₂][row₂]...`
/// Each row buffer contains id as first 16 bytes.
pub fn encode_rows(rows: &[(ObjectId, OwnedRow)]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    // Encode each row with size prefix
    for (_id, row) in rows {
        // Row buffer already contains the id as first 16 bytes
        buf.extend_from_slice(&(row.buffer.len() as u32).to_le_bytes());
        buf.extend_from_slice(&row.buffer);
    }

    buf
}

/// Encode a single row to binary format (no count header).
///
/// Format: `[row buffer]` (id is first 16 bytes)
pub fn encode_single_row(_id: ObjectId, row: &OwnedRow) -> Vec<u8> {
    // Row buffer already contains the id as first 16 bytes
    row.buffer.clone()
}

/// Encode a single delta to binary format.
///
/// Format: `[u8 type][row buffer or ObjectId]`
pub fn encode_delta(delta: &RowDelta) -> Vec<u8> {
    match delta {
        RowDelta::Added { id: _, row } => {
            // Row buffer contains id as first 16 bytes
            let mut buf = Vec::with_capacity(1 + row.buffer.len());
            buf.push(DELTA_ADDED);
            buf.extend_from_slice(&row.buffer);
            buf
        }
        RowDelta::Updated { id: _, row, .. } => {
            // Row buffer contains id as first 16 bytes
            let mut buf = Vec::with_capacity(1 + row.buffer.len());
            buf.push(DELTA_UPDATED);
            buf.extend_from_slice(&row.buffer);
            buf
        }
        RowDelta::Removed { id, .. } => {
            // Only id for removed rows (no row data available)
            let mut buf = Vec::with_capacity(1 + 16);
            buf.push(DELTA_REMOVED);
            buf.extend_from_slice(&id.to_le_bytes());
            buf
        }
    }
}

/// Encode a batch of deltas, returning individual buffers for each.
pub fn encode_delta_batch(batch: &DeltaBatch) -> Vec<Vec<u8>> {
    batch.iter().map(encode_delta).collect()
}

/// Encode multiple OwnedRows to binary format for WASM transfer.
///
/// Same as `encode_rows` but takes references.
pub fn encode_owned_rows(rows: &[(ObjectId, &OwnedRow)]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    // Encode each row with size prefix
    for (_id, row) in rows {
        // Row buffer already contains the id as first 16 bytes
        buf.extend_from_slice(&(row.buffer.len() as u32).to_le_bytes());
        buf.extend_from_slice(&row.buffer);
    }

    buf
}

/// Encode a single OwnedRow to binary format.
pub fn encode_single_owned_row(id: ObjectId, row: &OwnedRow) -> Vec<u8> {
    encode_single_row(id, row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor};
    use crate::sql::schema::ColumnType;
    use std::sync::Arc;

    fn make_descriptor_with_id(
        columns: impl IntoIterator<Item = (String, ColumnType, bool)>,
    ) -> Arc<RowDescriptor> {
        // id column is auto-added by TableSchema, but for tests we need to add it manually
        let mut cols: Vec<_> = vec![("id".to_string(), ColumnType::ObjectId, false)];
        cols.extend(columns);
        Arc::new(RowDescriptor::new(cols))
    }

    #[test]
    fn encode_owned_row_simple() {
        // Build a row descriptor with id + fixed-size columns
        let descriptor = make_descriptor_with_id([
            ("age".to_string(), ColumnType::I32, false),
            ("score".to_string(), ColumnType::F64, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]);

        let id_idx = descriptor.column_index("id").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();
        let score_idx = descriptor.column_index("score").unwrap();
        let active_idx = descriptor.column_index("active").unwrap();

        let id = ObjectId::new(12345);
        let row = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id)
            .set_i32(age_idx, 42)
            .set_f64(score_idx, 95.5)
            .set_bool(active_idx, true)
            .build();

        let buf = encode_single_owned_row(id, &row);

        // Row buffer starts with ObjectId (16 bytes LE)
        let id_bytes: [u8; 16] = buf[0..16].try_into().unwrap();
        assert_eq!(ObjectId::from_le_bytes(id_bytes), id);

        // Fixed columns after id: i32 (4) + f64 (8) + bool (1) = 13 bytes
        assert_eq!(i32::from_le_bytes(buf[16..20].try_into().unwrap()), 42);
        assert_eq!(f64::from_le_bytes(buf[20..28].try_into().unwrap()), 95.5);
        assert_eq!(buf[28], 1);
    }

    #[test]
    fn encode_owned_rows_batch() {
        let descriptor = make_descriptor_with_id([
            ("value".to_string(), ColumnType::I32, false),
        ]);

        let id_idx = descriptor.column_index("id").unwrap();
        let value_idx = descriptor.column_index("value").unwrap();

        let id1 = ObjectId::new(1);
        let id2 = ObjectId::new(2);

        let row1 = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id1)
            .set_i32(value_idx, 100)
            .build();
        let row2 = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id2)
            .set_i32(value_idx, 200)
            .build();

        let buf = encode_owned_rows(&[(id1, &row1), (id2, &row2)]);

        // Header: row count = 2
        assert_eq!(u32::from_le_bytes(buf[0..4].try_into().unwrap()), 2);

        // First row: size = 20 (16 id + 4 i32)
        let row1_size = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        assert_eq!(row1_size, 20);

        let id1_bytes: [u8; 16] = buf[8..24].try_into().unwrap();
        assert_eq!(ObjectId::from_le_bytes(id1_bytes), id1);
        assert_eq!(i32::from_le_bytes(buf[24..28].try_into().unwrap()), 100);

        // Second row starts at offset 28 (4 header + 4 size + 20 row1)
        let row2_size = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        assert_eq!(row2_size, 20);

        let id2_bytes: [u8; 16] = buf[32..48].try_into().unwrap();
        assert_eq!(ObjectId::from_le_bytes(id2_bytes), id2);
        assert_eq!(i32::from_le_bytes(buf[48..52].try_into().unwrap()), 200);
    }

    #[test]
    fn encode_owned_row_with_string() {
        let descriptor = make_descriptor_with_id([
            ("name".to_string(), ColumnType::String, false),
        ]);

        let id_idx = descriptor.column_index("id").unwrap();
        let name_idx = descriptor.column_index("name").unwrap();

        let id = ObjectId::new(999);
        let row = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id)
            .set_string(name_idx, "hello")
            .build();

        let buf = encode_single_owned_row(id, &row);

        // ObjectId: 16 bytes at start of buffer
        let id_bytes: [u8; 16] = buf[0..16].try_into().unwrap();
        assert_eq!(ObjectId::from_le_bytes(id_bytes), id);

        // String "hello" = 5 bytes (no offset table for 1 var col)
        assert_eq!(&buf[16..21], b"hello");
    }

    #[test]
    fn encode_owned_row_with_nullable() {
        let descriptor = make_descriptor_with_id([
            ("maybe_num".to_string(), ColumnType::I32, true),
        ]);

        let id_idx = descriptor.column_index("id").unwrap();
        let idx = descriptor.column_index("maybe_num").unwrap();

        let id = ObjectId::new(111);

        // Row with present value
        let row_present = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id)
            .set_i32(idx, 42)
            .build();

        let buf = encode_single_owned_row(id, &row_present);

        // ObjectId: 16 bytes, then presence byte (1) + i32
        assert_eq!(buf[16], 1); // present
        assert_eq!(i32::from_le_bytes(buf[17..21].try_into().unwrap()), 42);

        // Row with null value
        let row_null = RowBuilder::new(descriptor.clone())
            .set_ref(id_idx, id)
            .set_null(idx)
            .build();

        let buf_null = encode_single_owned_row(id, &row_null);

        // ObjectId: 16 bytes, then presence byte (0) + zeroed i32
        assert_eq!(buf_null[16], 0); // null
        // Nullable fixed columns still have space reserved
        assert_eq!(buf_null.len(), 16 + 5); // id + presence + i32
    }
}
