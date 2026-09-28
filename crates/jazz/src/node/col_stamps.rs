//! Per-column last-writer-wins stamps stored in every settled row state.
//!
//! A row state records, for every plain (LWW) user column and for `_deletion`,
//! the stamp of the write that last set it. A stamp is Unix milliseconds: the
//! writer's transaction time, clamped by Core to the seq it assigned
//! (`min(tx physical ms, seq physical ms)`). Merge-strategy columns have no
//! stamp; their ops apply in seq order.
//!
//! The durable carrier is the `_col_stamps` bytes field of history, global
//! current and ahead current records and the `col_stamps` field of a wire
//! `VersionRecord`. It is either empty (an unstamped image: a pending local
//! patch, or a payload whose stamps are unknown) or exactly
//! `6 * (lww_columns + 1)` bytes: one unsigned 48-bit little-endian stamp per
//! slot, LWW columns in schema column order, then `_deletion`. The normative
//! description is `crates/jazz/SPEC/4_history_merging.md`, "Column stamps".

use super::Error;
use crate::schema::{MergeStrategy, TableSchema};

/// Bytes per stamp: an unsigned 48-bit little-endian integer.
pub(super) const STAMP_WIDTH: usize = 6;
/// Largest encodable stamp. Packed HLC physical milliseconds (46 bits) fit.
pub(super) const MAX_STAMP_MS: u64 = (1 << (8 * STAMP_WIDTH as u64)) - 1;

/// Stamp slot layout of one table schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StampSlots {
    /// Slot of each user column in schema order; `None` for merge columns.
    columns: Vec<Option<usize>>,
    deletion: usize,
}

impl StampSlots {
    pub(super) fn for_table(table: &TableSchema) -> Self {
        let mut next = 0;
        let columns = table
            .columns
            .iter()
            .map(|column| {
                (table.merge_strategy(&column.name) == MergeStrategy::Lww).then(|| {
                    next += 1;
                    next - 1
                })
            })
            .collect();
        Self {
            columns,
            deletion: next,
        }
    }

    /// Slot of the user column at schema position `index`, if it is stamped.
    pub(super) fn column(&self, index: usize) -> Option<usize> {
        self.columns.get(index).copied().flatten()
    }

    pub(super) fn deletion(&self) -> usize {
        self.deletion
    }

    pub(super) fn len(&self) -> usize {
        self.deletion + 1
    }

    pub(super) fn byte_len(&self) -> usize {
        self.len() * STAMP_WIDTH
    }
}

/// Decoded stamps of one row state, indexed by slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ColumnStamps(Vec<u64>);

impl ColumnStamps {
    pub(super) fn uniform(slots: &StampSlots, stamp: u64) -> Self {
        Self(vec![stamp; slots.len()])
    }

    /// Decode a stored carrier for `slots`. An empty carrier is an unstamped
    /// image and decodes to `None`; any other length must match the layout.
    pub(super) fn decode(bytes: &[u8], slots: &StampSlots) -> Result<Option<Self>, Error> {
        if bytes.is_empty() {
            return Ok(None);
        }
        if bytes.len() != slots.byte_len() {
            return Err(Error::InvalidStoredValue(
                "column stamps do not match the table's stamp slots",
            ));
        }
        Ok(Some(Self(decode_stamps(bytes)?)))
    }

    pub(super) fn get(&self, slot: usize) -> u64 {
        self.0[slot]
    }

    pub(super) fn set(&mut self, slot: usize, stamp: u64) {
        self.0[slot] = stamp;
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * STAMP_WIDTH);
        for stamp in &self.0 {
            debug_assert!(*stamp <= MAX_STAMP_MS);
            bytes.extend_from_slice(&stamp.to_le_bytes()[..STAMP_WIDTH]);
        }
        bytes
    }
}

/// The newest stamp in a carrier of any layout; 0 for an unstamped image.
pub(super) fn max_stamp(bytes: &[u8]) -> Result<u64, Error> {
    Ok(decode_stamps(bytes)?.into_iter().max().unwrap_or(0))
}

/// Reject a carrier that is neither empty nor exactly one stamp per slot.
pub(super) fn validate_stamps(bytes: &[u8], table: &TableSchema) -> Result<(), Error> {
    ColumnStamps::decode(bytes, &StampSlots::for_table(table)).map(|_| ())
}

fn decode_stamps(bytes: &[u8]) -> Result<Vec<u64>, Error> {
    if !bytes.len().is_multiple_of(STAMP_WIDTH) {
        return Err(Error::InvalidStoredValue(
            "column stamps are not a whole number of 6-byte stamps",
        ));
    }
    Ok(bytes
        .chunks_exact(STAMP_WIDTH)
        .map(|chunk| {
            let mut wide = [0u8; 8];
            wide[..STAMP_WIDTH].copy_from_slice(chunk);
            u64::from_le_bytes(wide)
        })
        .collect())
}

// Internal test: the durable `_col_stamps` byte layout is not observable
// through any public API (reads expose values, never stamps), so this pins the
// specified bytes directly until the deferred corpus fixtures cover it.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, MergeStrategy, TableSchema};
    use groove::schema::ColumnType;

    #[test]
    fn column_stamps_are_u48_little_endian_in_lww_schema_order_then_deletion() {
        let table = TableSchema::new(
            "todos",
            vec![
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("count", ColumnType::I64),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter);
        let slots = StampSlots::for_table(&table);
        assert_eq!(
            (slots.column(0), slots.column(1), slots.column(2)),
            (Some(0), None, Some(1))
        );
        assert_eq!(slots.deletion(), 2);

        let mut stamps = ColumnStamps::uniform(&slots, 0);
        stamps.set(0, 0x0102_0304_0506);
        stamps.set(1, 1);
        stamps.set(2, crate::time::HLC_MAX_PHYSICAL_MS);
        let bytes = stamps.encode();
        assert_eq!(
            bytes,
            [
                0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // title
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // done
                0xff, 0xff, 0xff, 0xff, 0xff, 0x3f, // _deletion
            ]
        );
        assert_eq!(ColumnStamps::decode(&bytes, &slots).unwrap(), Some(stamps));
        assert_eq!(ColumnStamps::decode(&[], &slots).unwrap(), None);
        assert!(ColumnStamps::decode(&bytes[..12], &slots).is_err());
        assert_eq!(max_stamp(&bytes).unwrap(), crate::time::HLC_MAX_PHYSICAL_MS);
    }
}
