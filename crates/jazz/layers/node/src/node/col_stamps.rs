//! Per-column last-writer-wins stamps stored in every settled row state.
//!
//! A row state records, for every plain (LWW) user column and for `_deletion`,
//! the stamp of the write that last set it. A stamp is Unix milliseconds: the
//! writer's transaction time, clamped by Core to the seq it assigned
//! (`min(tx physical ms, seq physical ms)`). Merge-strategy columns have no
//! stamp; their ops apply in seq order.
//!
//! Durable carrier: one hidden constant-width groove `U48` field per slot in
//! history, global-current, ahead-current and ahead-shadow records. The stamp
//! of the cell field `F` is named `_ts_F` (so `_ts__app_<column>` in a logical
//! layout, `_ts__app_<physical id>` in a physical one, and `_ts__deletion`).
//! Stamp fields follow `authored_columns` in slot order: LWW columns in schema
//! column order, then `_deletion`. An unstamped image (a pending local patch,
//! or a payload whose stamps are unknown) stores `0` in every slot, which is
//! exactly how a merge treats it.
//!
//! Wire carrier: the `col_stamps` bytes of a `VersionRecord`, either empty
//! (every slot `0`) or `6 * slots` bytes of U48 little-endian stamps in slot
//! order with at least one nonzero slot. The normative description is
//! `crates/jazz/SPEC/4_history_merging.md`, "Column stamps".

use super::Error;
use crate::schema::{MergeStrategy, STAMP_FIELD_PREFIX, TableSchema};
use groove::records::{BorrowedRecord, RecordDescriptor, Value, ValueType};

/// Bytes per wire stamp: an unsigned 48-bit little-endian integer.
pub(super) const STAMP_WIDTH: usize = 6;
/// Largest encodable stamp. Packed HLC physical milliseconds (46 bits) fit.
pub(super) const MAX_STAMP_MS: u64 = groove::records::U48_MAX;

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
}

/// Indices of the stamp fields of a row-state descriptor, in slot order.
pub(super) fn stamp_field_indices(descriptor: &RecordDescriptor) -> Vec<usize> {
    descriptor
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.value_type == ValueType::U48
                && field
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(STAMP_FIELD_PREFIX))
        })
        .map(|(index, _)| index)
        .collect()
}

/// Decoded stamps of one row state, indexed by slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ColumnStamps(Vec<u64>);

impl ColumnStamps {
    pub(super) fn uniform(slots: &StampSlots, stamp: u64) -> Self {
        Self(vec![stamp; slots.len()])
    }

    pub(super) fn get(&self, slot: usize) -> u64 {
        self.0[slot]
    }

    pub(super) fn set(&mut self, slot: usize, stamp: u64) {
        self.0[slot] = stamp;
    }

    /// Read the stamp fields of a stored row state laid out for `slots`.
    /// `None` when the layout carries no stamp fields at all.
    pub(super) fn read(
        record: BorrowedRecord<'_>,
        slots: &StampSlots,
    ) -> Result<Option<Self>, Error> {
        let indices = stamp_field_indices(&record.descriptor());
        if indices.is_empty() {
            return Ok(None);
        }
        if indices.len() != slots.len() {
            return Err(Error::InvalidStoredValue(
                "stamp fields do not match the table's stamp slots",
            ));
        }
        indices
            .into_iter()
            .map(|index| record.get_u48(index).map_err(Error::from))
            .collect::<Result<Vec<_>, _>>()
            .map(|stamps| Some(Self(stamps)))
    }

    /// Store these stamps into the stamp fields of a row-state value vector
    /// laid out by `descriptor`.
    pub(super) fn write_values(
        &self,
        values: &mut [Value],
        descriptor: &RecordDescriptor,
    ) -> Result<(), Error> {
        let indices = stamp_field_indices(descriptor);
        if indices.len() != self.0.len() {
            return Err(Error::InvalidStoredValue(
                "row image layout does not match the table's stamp slots",
            ));
        }
        for (index, stamp) in indices.into_iter().zip(&self.0) {
            values[index] = Value::U48(*stamp);
        }
        Ok(())
    }

    /// The stamp field values, in slot order.
    pub(super) fn values(&self) -> impl Iterator<Item = Value> + '_ {
        self.0.iter().map(|stamp| Value::U48(*stamp))
    }

    /// Decode a wire carrier for `slots`: empty is every slot `0`; otherwise
    /// exactly one 6-byte little-endian stamp per slot, not all zero.
    pub(super) fn decode_wire(bytes: &[u8], slots: &StampSlots) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Ok(Self::uniform(slots, 0));
        }
        if bytes.len() != slots.len() * STAMP_WIDTH {
            return Err(Error::InvalidStoredValue(
                "column stamps do not match the table's stamp slots",
            ));
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(Error::InvalidStoredValue(
                "all-zero column stamps must be encoded as the empty carrier",
            ));
        }
        Ok(Self(
            bytes
                .chunks_exact(STAMP_WIDTH)
                .map(|chunk| {
                    let mut wide = [0u8; 8];
                    wide[..STAMP_WIDTH].copy_from_slice(chunk);
                    u64::from_le_bytes(wide)
                })
                .collect(),
        ))
    }

    /// The canonical wire carrier of these stamps.
    #[cfg(test)]
    pub(super) fn encode_wire(&self) -> Vec<u8> {
        encode_wire_stamps(&self.0)
    }
}

fn encode_wire_stamps(stamps: &[u64]) -> Vec<u8> {
    if stamps.iter().all(|stamp| *stamp == 0) {
        return Vec::new();
    }
    let mut bytes = Vec::with_capacity(stamps.len() * STAMP_WIDTH);
    for stamp in stamps {
        debug_assert!(*stamp <= MAX_STAMP_MS);
        bytes.extend_from_slice(&stamp.to_le_bytes()[..STAMP_WIDTH]);
    }
    bytes
}

/// The canonical wire carrier of a stored row state of any layout.
pub(super) fn wire_stamps(record: BorrowedRecord<'_>) -> Result<Vec<u8>, Error> {
    let stamps = stamp_field_indices(&record.descriptor())
        .into_iter()
        .map(|index| record.get_u48(index).map_err(Error::from))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(encode_wire_stamps(&stamps))
}

/// The newest stamp of a stored row state of any layout; 0 when unstamped.
pub(super) fn max_stamp(record: BorrowedRecord<'_>) -> Result<u64, Error> {
    stamp_field_indices(&record.descriptor())
        .into_iter()
        .try_fold(0, |max, index| Ok(max.max(record.get_u48(index)?)))
}

/// Decode a wire carrier for `table` into its stamp field values.
pub(super) fn wire_stamp_values(bytes: &[u8], table: &TableSchema) -> Result<Vec<Value>, Error> {
    Ok(
        ColumnStamps::decode_wire(bytes, &StampSlots::for_table(table))?
            .values()
            .collect(),
    )
}

// Internal test: the wire `col_stamps` carrier and the hidden stamp field
// layout are not observable through any public API (reads expose values,
// never stamps), so this pins the specified bytes and names directly.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, MergeStrategy, TableSchema};
    use groove::schema::ColumnType;

    fn todos() -> TableSchema {
        TableSchema::new(
            "todos",
            vec![
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("count", ColumnType::I64),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)
    }

    #[test]
    fn column_stamps_are_u48_little_endian_in_lww_schema_order_then_deletion() {
        let table = todos();
        let slots = StampSlots::for_table(&table);
        assert_eq!(
            (slots.column(0), slots.column(1), slots.column(2)),
            (Some(0), None, Some(1))
        );
        assert_eq!(slots.deletion(), 2);

        let mut stamps = ColumnStamps::uniform(&slots, 0);
        assert_eq!(stamps.encode_wire(), Vec::<u8>::new());
        stamps.0[0] = 0x0102_0304_0506;
        stamps.0[1] = 1;
        stamps.0[2] = crate::time::HLC_MAX_PHYSICAL_MS;
        let bytes = stamps.encode_wire();
        assert_eq!(
            bytes,
            [
                0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // title
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // done
                0xff, 0xff, 0xff, 0xff, 0xff, 0x3f, // _deletion
            ]
        );
        assert_eq!(ColumnStamps::decode_wire(&bytes, &slots).unwrap(), stamps);
        assert_eq!(
            ColumnStamps::decode_wire(&[], &slots).unwrap(),
            ColumnStamps::uniform(&slots, 0)
        );
        assert!(ColumnStamps::decode_wire(&bytes[..12], &slots).is_err());
        assert!(ColumnStamps::decode_wire(&[0; 18], &slots).is_err());
    }

    #[test]
    fn stamp_fields_are_hidden_u48_fields_after_authored_columns() {
        let table = todos();
        let history = table.history_storage_table();
        let names = history
            .columns
            .iter()
            .rev()
            .take(4)
            .rev()
            .map(|column| (column.name.as_str(), column.column_type.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                ("authored_columns", ColumnType::U64.array_of().nullable()),
                ("_ts__app_title", ColumnType::U48),
                ("_ts__app_done", ColumnType::U48),
                ("_ts__deletion", ColumnType::U48),
            ]
        );
        // A stored row state holds the stamps as constant-width fixed fields:
        // six little-endian bytes each, ahead of the variable-width region.
        let descriptor = RecordDescriptor::new([
            ("note", ValueType::String),
            ("_ts__app_title", ValueType::U48),
            ("_ts__app_done", ValueType::U48),
            ("_ts__deletion", ValueType::U48),
        ]);
        let slots = StampSlots::for_table(&table);
        let mut values = vec![
            Value::String("x".to_owned()),
            Value::U48(0),
            Value::U48(0),
            Value::U48(0),
        ];
        let mut stamps = ColumnStamps::uniform(&slots, 7);
        stamps.0[2] = 0x0a0b_0c0d_0e0f;
        stamps.write_values(&mut values, &descriptor).unwrap();
        let raw = descriptor.create(&values).unwrap();
        assert_eq!(
            raw,
            [
                0x07, 0x00, 0x00, 0x00, 0x00, 0x00, // _ts__app_title
                0x07, 0x00, 0x00, 0x00, 0x00, 0x00, // _ts__app_done
                0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, // _ts__deletion
                0x02, b'x', // note
            ]
        );
        let record = descriptor.bind(&raw);
        assert_eq!(
            ColumnStamps::read(record, &slots).unwrap(),
            Some(stamps.clone())
        );
        assert_eq!(max_stamp(record).unwrap(), 0x0a0b_0c0d_0e0f);
        assert_eq!(wire_stamps(record).unwrap(), stamps.encode_wire());
    }
}
