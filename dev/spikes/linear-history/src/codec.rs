//! Spike-only byte layouts. Deliberately simple and explicit, but NOT a
//! durable contract: no version negotiation, no corpus fixtures.

use crate::model::*;

pub const CF_CURRENT: &str = "lh_current";
pub const CF_HISTORY: &str = "lh_history";
pub const CF_CHANGES: &str = "lh_changes";
pub const CF_INDEX: &str = "lh_index";
pub const CF_TX: &str = "lh_tx";
pub const CF_META: &str = "lh_meta";
pub const COLUMN_FAMILIES: [&str; 6] =
    [CF_CURRENT, CF_HISTORY, CF_CHANGES, CF_INDEX, CF_TX, CF_META];
pub const META_LAST_SEQ: &[u8] = b"last_seq";

const ROW_IMAGE_FORMAT: u8 = 1;

/// `table | row` — current row key and history prefix.
pub fn row_key(table: TableId, row: RowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&table.to_be_bytes());
    key.extend_from_slice(&row.to_be_bytes());
    key
}

/// `table | row | !seq` — newest first, so "newest at or before S" is a
/// forward seek to `!S` (RocksDB forward seeks are cheaper than reverse).
pub fn history_key(table: TableId, row: RowId, seq: Seq) -> Vec<u8> {
    let mut key = row_key(table, row);
    key.extend_from_slice(&(!seq).to_be_bytes());
    key
}

pub fn history_key_row(key: &[u8]) -> (TableId, RowId, Seq) {
    let table = TableId::from_be_bytes(key[0..4].try_into().unwrap());
    let row = RowId::from_be_bytes(key[4..20].try_into().unwrap());
    let seq = !Seq::from_be_bytes(key[20..28].try_into().unwrap());
    (table, row, seq)
}

/// `table | seq | row` — per-table change log.
pub fn change_key(table: TableId, seq: Seq, row: RowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(28);
    key.extend_from_slice(&table.to_be_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
    key.extend_from_slice(&row.to_be_bytes());
    key
}

pub fn change_key_parts(key: &[u8]) -> (Seq, RowId) {
    (
        Seq::from_be_bytes(key[4..12].try_into().unwrap()),
        RowId::from_be_bytes(key[12..28].try_into().unwrap()),
    )
}

/// `table | column | len | value` — index prefix for one value.
pub fn index_prefix(table: TableId, column: usize, value: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(10 + value.len() + 16);
    key.extend_from_slice(&table.to_be_bytes());
    key.extend_from_slice(&(column as u16).to_be_bytes());
    key.extend_from_slice(&(value.len() as u32).to_be_bytes());
    key.extend_from_slice(value);
    key
}

pub fn index_key(table: TableId, column: usize, value: &[u8], row: RowId) -> Vec<u8> {
    let mut key = index_prefix(table, column, value);
    key.extend_from_slice(&row.to_be_bytes());
    key
}

pub fn index_key_row(key: &[u8]) -> RowId {
    RowId::from_be_bytes(key[key.len() - 16..].try_into().unwrap())
}

pub fn tx_key(id: TxId) -> Vec<u8> {
    let mut key = Vec::with_capacity(12);
    key.extend_from_slice(&id.node.to_be_bytes());
    key.extend_from_slice(&id.counter.to_be_bytes());
    key
}

fn put_stamp(out: &mut Vec<u8>, stamp: Stamp) {
    out.extend_from_slice(&stamp.time.to_be_bytes());
    out.extend_from_slice(&stamp.node.to_be_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        head
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u16(&mut self) -> u16 {
        u16::from_be_bytes(self.take(2).try_into().unwrap())
    }
    fn u32(&mut self) -> u32 {
        u32::from_be_bytes(self.take(4).try_into().unwrap())
    }
    fn u64(&mut self) -> u64 {
        u64::from_be_bytes(self.take(8).try_into().unwrap())
    }
    fn stamp(&mut self) -> Stamp {
        Stamp {
            time: self.u64(),
            node: self.u32(),
        }
    }
}

/// `fmt u8 | seq u64 | deleted u8 | deleted_stamp | ncols u16 |
///  (stamp | tag u8 [| len u32 | bytes])*`
pub fn encode_row(image: &RowImage) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + image.cells.len() * 24);
    out.push(ROW_IMAGE_FORMAT);
    out.extend_from_slice(&image.seq.to_be_bytes());
    out.push(u8::from(image.deleted));
    put_stamp(&mut out, image.deleted_stamp);
    out.extend_from_slice(&(image.cells.len() as u16).to_be_bytes());
    for cell in &image.cells {
        put_stamp(&mut out, cell.stamp);
        match &cell.value {
            None => out.push(0),
            Some(bytes) => {
                out.push(1);
                out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    out
}

pub fn decode_row(bytes: &[u8]) -> RowImage {
    let mut r = Reader { bytes };
    assert_eq!(r.u8(), ROW_IMAGE_FORMAT, "unknown spike row image format");
    let seq = r.u64();
    let deleted = r.u8() != 0;
    let deleted_stamp = r.stamp();
    let n = r.u16() as usize;
    let cells = (0..n)
        .map(|_| {
            let stamp = r.stamp();
            let value = match r.u8() {
                0 => None,
                _ => {
                    let len = r.u32() as usize;
                    Some(r.take(len).to_vec())
                }
            };
            Cell { stamp, value }
        })
        .collect();
    RowImage {
        seq,
        deleted,
        deleted_stamp,
        cells,
    }
}

pub fn encode_outcome(outcome: Outcome) -> Vec<u8> {
    match outcome {
        Outcome::Accepted(seq) => {
            let mut out = vec![0];
            out.extend_from_slice(&seq.to_be_bytes());
            out
        }
        Outcome::Rejected(reason) => vec![
            1,
            match reason {
                RejectReason::UnknownTable => 0,
                RejectReason::UnknownColumn => 1,
                RejectReason::StrategyMismatch => 2,
                RejectReason::RowConflict => 3,
                RejectReason::PredicateConflict => 4,
            },
        ],
    }
}

pub fn decode_outcome(bytes: &[u8]) -> Outcome {
    let mut r = Reader { bytes };
    match r.u8() {
        0 => Outcome::Accepted(r.u64()),
        _ => Outcome::Rejected(match r.u8() {
            0 => RejectReason::UnknownTable,
            1 => RejectReason::UnknownColumn,
            2 => RejectReason::StrategyMismatch,
            3 => RejectReason::RowConflict,
            _ => RejectReason::PredicateConflict,
        }),
    }
}
