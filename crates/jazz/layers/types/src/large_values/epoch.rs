//! The encoding of an epoch's edits (SPEC §19.3).
//!
//! An epoch holds a tree and the edits applied on top of it, appended in
//! order. A row version names a prefix of them, so the encoding is a plain
//! concatenation: the first `n` encoded edits are the encoding of the first
//! `n` edits, and comparing two epochs' edits is a byte comparison.
//!
//! Each edit is a tag byte (`kind << 4 | small`) and a few fields. `small` is
//! the inserted length (append, insert, replace) or the deleted length
//! (delete) when it is 1 to 15; 0 means a varint length follows. Offsets are
//! zigzag varints relative to the end of the previous edit, so ordinary typing
//! spends one byte on them. String and JSON edits also carry UTF-16 offsets
//! and deletion lengths; an insertion's UTF-16 length comes from its bytes.

use super::{Error, LargeValueKind, ReplaceEdit};

/// The most encoded edit bytes an epoch holds, so its row fits one SQLite
/// page and an append dirties only that page.
pub const EPOCH_EDIT_BYTES: usize = 3584;
/// The most edits an epoch holds: a cell is `epoch * EPOCH_CELL_SPAN + n`.
pub const EPOCH_MAX_EDITS: usize = 4095;
/// Cells of one epoch: `n` runs from 0 to [`EPOCH_MAX_EDITS`].
pub const EPOCH_CELL_SPAN: i64 = 4096;
/// The most bytes one edit inserts into an epoch; a larger insertion
/// materializes the value into a new tree.
pub const EPOCH_INSERT_BYTES: usize = 256;

const APPEND: u8 = 0;
const INSERT: u8 = 1;
const DELETE: u8 = 2;
const REPLACE: u8 = 3;

/// Lengths as edits are replayed: the value's length and the end of the
/// previous edit, in bytes and UTF-16 units.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cursor {
    pub len: u64,
    pub utf16_len: u64,
    end: u64,
    utf16_end: u64,
}

impl Cursor {
    /// The cursor before an epoch's first edit, over a tree of these lengths.
    pub fn new(len: u64, utf16_len: Option<u64>) -> Self {
        Self {
            len,
            utf16_len: utf16_len.unwrap_or(0),
            end: 0,
            utf16_end: 0,
        }
    }

    fn step(&mut self, edit: &ReplaceEdit) -> Result<(), Error> {
        let inserted = edit.insert_bytes.len() as u64;
        self.len = self
            .len
            .checked_sub(edit.delete_length)
            .and_then(|len| len.checked_add(inserted))
            .ok_or(Error::MetricOverflow)?;
        self.utf16_len = self
            .utf16_len
            .checked_sub(edit.delete_utf16_length)
            .and_then(|len| len.checked_add(edit.insert_utf16_length))
            .ok_or(Error::MetricOverflow)?;
        self.end = edit
            .offset
            .checked_add(inserted)
            .ok_or(Error::MetricOverflow)?;
        self.utf16_end = edit
            .utf16_offset
            .checked_add(edit.insert_utf16_length)
            .ok_or(Error::MetricOverflow)?;
        Ok(())
    }
}

fn text(kind: LargeValueKind) -> bool {
    !matches!(kind, LargeValueKind::Bytes)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_relative(out: &mut Vec<u8>, at: u64, from: u64) {
    let delta = at.wrapping_sub(from) as i64;
    put_varint(out, ((delta << 1) ^ (delta >> 63)) as u64);
}

/// Append `edit`, applied after the edits `cursor` has seen, to `out`.
pub fn encode_edit(
    kind: LargeValueKind,
    cursor: &mut Cursor,
    edit: &ReplaceEdit,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    let inserted = edit.insert_bytes.len() as u64;
    let append = edit.delete_length == 0
        && inserted > 0
        && edit.offset == cursor.len
        && (!text(kind) || edit.utf16_offset == cursor.utf16_len);
    let (code, length) = if append {
        (APPEND, inserted)
    } else if edit.delete_length == 0 {
        (INSERT, inserted)
    } else if inserted == 0 {
        (DELETE, edit.delete_length)
    } else {
        (REPLACE, inserted)
    };
    let small = if (1..=15).contains(&length) {
        length as u8
    } else {
        0
    };
    out.push(code << 4 | small);
    if code != APPEND {
        put_relative(out, edit.offset, cursor.end);
        if text(kind) {
            put_relative(out, edit.utf16_offset, cursor.utf16_end);
        }
    }
    if code == REPLACE {
        put_varint(out, edit.delete_length);
        if text(kind) {
            put_varint(out, edit.delete_utf16_length);
        }
    }
    if small == 0 {
        put_varint(out, length);
    }
    if code == DELETE {
        if text(kind) {
            put_varint(out, edit.delete_utf16_length);
        }
    } else {
        out.extend_from_slice(&edit.insert_bytes);
    }
    cursor.step(edit)
}

/// The encoding of `edits`, applied in order over a tree of these lengths.
pub fn encode_edits(
    kind: LargeValueKind,
    len: u64,
    utf16_len: Option<u64>,
    edits: &[ReplaceEdit],
) -> Result<Vec<u8>, Error> {
    let mut cursor = Cursor::new(len, utf16_len);
    let mut out = Vec::new();
    for edit in edits {
        encode_edit(kind, &mut cursor, edit, &mut out)?;
    }
    Ok(out)
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, Error> {
        let byte = *self.bytes.get(self.at).ok_or(Error::MalformedScalar)?;
        self.at += 1;
        Ok(byte)
    }

    fn varint(&mut self) -> Result<u64, Error> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Error::MalformedScalar)
    }

    fn relative(&mut self, from: u64) -> Result<u64, Error> {
        let zigzag = self.varint()?;
        let delta = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
        Ok(from.wrapping_add(delta as u64))
    }

    fn take(&mut self, len: u64) -> Result<&[u8], Error> {
        let len = usize::try_from(len).map_err(|_| Error::MetricOverflow)?;
        let end = self.at.checked_add(len).ok_or(Error::MalformedScalar)?;
        let slice = self.bytes.get(self.at..end).ok_or(Error::MalformedScalar)?;
        self.at = end;
        Ok(slice)
    }
}

/// Decode up to `limit` edits (all with `None`) over a tree of these
/// lengths. Each decoded edit is checked against the value it applies to:
/// its range lies inside it and, for text, its insertion is UTF-8.
pub fn decode_edits(
    kind: LargeValueKind,
    len: u64,
    utf16_len: Option<u64>,
    bytes: &[u8],
    limit: Option<usize>,
) -> Result<(Vec<ReplaceEdit>, Cursor), Error> {
    let mut cursor = Cursor::new(len, utf16_len);
    let mut reader = Reader { bytes, at: 0 };
    let mut edits = Vec::new();
    while reader.at < bytes.len() && limit.is_none_or(|limit| edits.len() < limit) {
        let tag = reader.byte()?;
        let (code, small) = (tag >> 4, u64::from(tag & 0x0F));
        if code > REPLACE {
            return Err(Error::MalformedScalar);
        }
        let (offset, utf16_offset) = if code == APPEND {
            (cursor.len, if text(kind) { cursor.utf16_len } else { 0 })
        } else {
            let offset = reader.relative(cursor.end)?;
            let utf16_offset = if text(kind) {
                reader.relative(cursor.utf16_end)?
            } else {
                0
            };
            (offset, utf16_offset)
        };
        let (mut delete_length, mut delete_utf16_length) = (0, 0);
        if code == REPLACE {
            delete_length = reader.varint()?;
            if text(kind) {
                delete_utf16_length = reader.varint()?;
            }
        }
        let length = if small == 0 { reader.varint()? } else { small };
        let insert_bytes = match code {
            DELETE => {
                delete_length = length;
                if text(kind) {
                    delete_utf16_length = reader.varint()?;
                }
                Vec::new()
            }
            _ => reader.take(length)?.to_vec(),
        };
        let insert_utf16_length = if text(kind) {
            std::str::from_utf8(&insert_bytes)
                .map_err(|_| Error::InvalidUtf8)?
                .encode_utf16()
                .count() as u64
        } else {
            0
        };
        let end = offset
            .checked_add(delete_length)
            .ok_or(Error::MetricOverflow)?;
        if end > cursor.len
            || (text(kind)
                && utf16_offset
                    .checked_add(delete_utf16_length)
                    .is_none_or(|end| end > cursor.utf16_len))
        {
            return Err(Error::MalformedScalar);
        }
        let edit = ReplaceEdit {
            offset,
            delete_length,
            insert_bytes,
            utf16_offset,
            delete_utf16_length,
            insert_utf16_length,
        };
        cursor.step(&edit)?;
        edits.push(edit);
    }
    if limit.is_some_and(|limit| edits.len() < limit) {
        return Err(Error::MalformedScalar);
    }
    Ok((edits, cursor))
}

/// Whether `edits` over a tree of these lengths fit one epoch.
pub fn fits_epoch(
    kind: LargeValueKind,
    len: u64,
    utf16_len: Option<u64>,
    edits: &[ReplaceEdit],
) -> Result<bool, Error> {
    if edits.len() > EPOCH_MAX_EDITS
        || edits
            .iter()
            .any(|edit| edit.insert_bytes.len() > EPOCH_INSERT_BYTES)
    {
        return Ok(false);
    }
    Ok(encode_edits(kind, len, utf16_len, edits)?.len() <= EPOCH_EDIT_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(
        offset: u64,
        delete: u64,
        insert: &str,
        utf16_offset: u64,
        delete16: u64,
    ) -> ReplaceEdit {
        ReplaceEdit {
            offset,
            delete_length: delete,
            insert_bytes: insert.as_bytes().to_vec(),
            utf16_offset,
            delete_utf16_length: delete16,
            insert_utf16_length: insert.encode_utf16().count() as u64,
        }
    }

    #[test]
    fn edits_round_trip_and_typing_is_a_few_bytes() {
        let kind = LargeValueKind::String;
        let edits = vec![
            edit(100, 0, "x", 100, 0),
            edit(101, 0, "y", 101, 0),
            edit(100, 1, "", 100, 1),
            edit(101, 0, "z", 101, 0),
            edit(5, 2, "é", 5, 2),
            edit(202, 0, "tail", 201, 0),
        ];
        let encoded = encode_edits(kind, 200, Some(200), &edits).unwrap();
        // The first insertion pays for its absolute offsets; after that,
        // typing next to the previous edit costs four bytes, a nearby delete
        // four, a far replace nine and an append five.
        assert_eq!(encoded.len(), 6 + 4 + 4 + 4 + 9 + 5);
        let (decoded, cursor) = decode_edits(kind, 200, Some(200), &encoded, None).unwrap();
        assert_eq!(decoded, edits);
        assert_eq!((cursor.len, cursor.utf16_len), (206, 205));
        let (prefix, _) = decode_edits(kind, 200, Some(200), &encoded, Some(2)).unwrap();
        assert_eq!(prefix, edits[..2]);
        assert_eq!(
            encode_edits(kind, 200, Some(200), &edits[..2]).unwrap(),
            encoded[..10]
        );
    }

    #[test]
    fn a_one_byte_append_is_two_bytes() {
        let append = ReplaceEdit {
            offset: 70_000,
            delete_length: 0,
            insert_bytes: vec![7],
            utf16_offset: 0,
            delete_utf16_length: 0,
            insert_utf16_length: 0,
        };
        let encoded = encode_edits(LargeValueKind::Bytes, 70_000, None, &[append]).unwrap();
        assert_eq!(encoded, [0x01, 7]);
    }

    #[test]
    fn malformed_and_out_of_range_edits_fail() {
        let kind = LargeValueKind::Bytes;
        let edits = [ReplaceEdit {
            offset: 10,
            delete_length: 5,
            insert_bytes: Vec::new(),
            utf16_offset: 0,
            delete_utf16_length: 0,
            insert_utf16_length: 0,
        }];
        let encoded = encode_edits(kind, 100, None, &edits).unwrap();
        assert!(decode_edits(kind, 12, None, &encoded, None).is_err());
        assert!(decode_edits(kind, 100, None, &encoded[..1], None).is_err());
        assert!(decode_edits(kind, 100, None, &encoded, Some(2)).is_err());
        assert!(decode_edits(kind, 100, None, &[0x40], None).is_err());
    }
}
