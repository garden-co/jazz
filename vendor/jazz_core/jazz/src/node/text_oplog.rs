//! Pure operation-log algebra for large text/blob column values.

use crate::ids::{AuthorId, NodeUuid, RowUuid};
use crate::time::TxTime;

use super::Error;
use super::content_store::Extent;

const TAG_INSERT: u8 = 0;
const TAG_DELETE: u8 = 1;
const CONTENT_INLINE: u8 = 0;
const CONTENT_REF: u8 = 1;
const U32_LEN: usize = 4;
const U64_LEN: usize = 8;
const UUID_LEN: usize = 16;

/// One operation in a text/blob version payload.
///
/// Positions are byte offsets relative to the parent value as the author saw
/// it. Insert content can be inline while diffing/replaying or an extent
/// reference in stored version payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Insert bytes at `pos` in the parent value.
    Insert {
        /// Byte offset in the parent value.
        pos: usize,
        /// Bytes to insert, inline or content-store backed.
        content: Content,
    },
    /// Delete `len` bytes starting at `pos` in the parent value.
    Delete {
        /// Byte offset in the parent value.
        pos: usize,
        /// Number of parent bytes to delete.
        len: usize,
    },
}

/// Insert content for a text/blob op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Content {
    /// Inline bytes used by pure diff/replay.
    Inline(Vec<u8>),
    /// Content-store extent used by stored version payloads.
    Ref(Extent),
}

/// Causal identity used to order concurrent same-position insert runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MergeOrigin {
    /// Transaction HLC for the side being merged.
    pub tx_time: TxTime,
    /// Authenticated author for the side being merged.
    pub author: AuthorId,
    /// Node UUID as the final deterministic discriminator.
    pub node: NodeUuid,
}

impl Content {
    fn len(&self) -> usize {
        match self {
            Content::Inline(bytes) => bytes.len(),
            Content::Ref(extent) => {
                usize::try_from(extent.len).expect("content extent length exceeds usize")
            }
        }
    }

    fn inline_bytes(&self) -> &[u8] {
        match self {
            Content::Inline(bytes) => bytes,
            Content::Ref(_) => panic!("text op ref content must be resolved before replay"),
        }
    }
}

/// Return a minimal middle-replacement op batch from `old` to `new`.
pub fn diff(old: &[u8], new: &[u8]) -> Vec<Op> {
    if old == new {
        return Vec::new();
    }

    let prefix_len = old
        .iter()
        .zip(new.iter())
        .take_while(|(old_byte, new_byte)| old_byte == new_byte)
        .count();

    let max_suffix_len = old.len().min(new.len()) - prefix_len;
    let suffix_len = old[prefix_len..]
        .iter()
        .rev()
        .zip(new[prefix_len..].iter().rev())
        .take(max_suffix_len)
        .take_while(|(old_byte, new_byte)| old_byte == new_byte)
        .count();

    let mut ops = Vec::with_capacity(2);
    let delete_len = old.len() - prefix_len - suffix_len;
    if delete_len > 0 {
        ops.push(Op::Delete {
            pos: prefix_len,
            len: delete_len,
        });
    }

    let insert_end = new.len() - suffix_len;
    if prefix_len < insert_end {
        ops.push(Op::Insert {
            pos: prefix_len,
            content: Content::Inline(new[prefix_len..insert_end].to_vec()),
        });
    }

    ops
}

/// Apply an op batch to `parent`, interpreting op positions against `parent`.
pub fn replay(parent: &[u8], ops: &[Op]) -> Vec<u8> {
    let mut value = parent.to_vec();

    for (op_index, op) in ops.iter().enumerate() {
        match op {
            Op::Insert { pos, content } => {
                let adjusted_pos = adjusted_pos(*pos, &ops[..op_index], value.len());
                value.splice(
                    adjusted_pos..adjusted_pos,
                    content.inline_bytes().iter().copied(),
                );
            }
            Op::Delete { pos, len } => {
                let adjusted_pos = adjusted_pos(*pos, &ops[..op_index], value.len());
                let end = adjusted_pos
                    .checked_add(*len)
                    .expect("delete range end overflows usize");
                assert!(end <= value.len(), "delete range exceeds replay value");
                value.drain(adjusted_pos..end);
            }
        }
    }

    value
}

/// Merge two concurrent text/blob op streams produced since a shared LCA value.
///
/// This is replay-v1 for large-value concurrent text edits. Each side is first
/// replayed and diffed back to the ancestor, giving ancestor-relative
/// replacement ranges. Ancestor bytes deleted by either side are deleted once,
/// and insert runs from both sides are anchored at their ancestor byte offset.
/// When both sides insert at the same ancestor offset, runs are ordered by
/// causal origin so the result is independent of argument order and each run
/// remains contiguous.
pub fn merge_since_lca(
    ancestor: &[u8],
    left: (&[Op], MergeOrigin),
    right: (&[Op], MergeOrigin),
) -> Vec<u8> {
    let mut left_edits = edits_since_lca(ancestor, left.0, left.1);
    let mut right_edits = edits_since_lca(ancestor, right.0, right.1);

    let mut deleted = vec![false; ancestor.len()];
    mark_deleted(&mut deleted, &left_edits);
    mark_deleted(&mut deleted, &right_edits);

    let mut inserts = Vec::new();
    inserts.append(&mut left_edits.inserts);
    inserts.append(&mut right_edits.inserts);
    inserts.sort_by(|left, right| {
        left.pos
            .cmp(&right.pos)
            .then_with(|| left.origin.cmp(&right.origin))
    });

    let mut merged = Vec::new();
    let mut insert_index = 0;
    for (pos, byte) in ancestor.iter().copied().enumerate() {
        append_inserts_at(pos, &inserts, &mut insert_index, &mut merged);
        if !deleted[pos] {
            merged.push(byte);
        }
    }
    append_inserts_at(ancestor.len(), &inserts, &mut insert_index, &mut merged);

    merged
}

/// Encode an op batch into deterministic bytes for a version payload.
pub fn encode(ops: &[Op]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_usize(&mut bytes, ops.len());
    for op in ops {
        match op {
            Op::Insert { pos, content } => {
                bytes.push(TAG_INSERT);
                write_usize(&mut bytes, *pos);
                encode_content(&mut bytes, content);
            }
            Op::Delete { pos, len } => {
                bytes.push(TAG_DELETE);
                write_usize(&mut bytes, *pos);
                write_usize(&mut bytes, *len);
            }
        }
    }
    bytes
}

/// Decode an op batch previously produced by [`encode`].
pub fn decode(bytes: &[u8]) -> Result<Vec<Op>, Error> {
    let mut cursor = Cursor::new(bytes);
    let op_count = cursor.read_usize()?;
    let mut ops = Vec::with_capacity(op_count);

    for _ in 0..op_count {
        let tag = cursor.read_u8()?;
        let pos = cursor.read_usize()?;
        match tag {
            TAG_INSERT => {
                let content = decode_content(&mut cursor)?;
                ops.push(Op::Insert { pos, content });
            }
            TAG_DELETE => {
                let len = cursor.read_usize()?;
                ops.push(Op::Delete { pos, len });
            }
            _ => return Err(Error::InvalidStoredValue("unknown text op tag")),
        }
    }

    if cursor.remaining() != 0 {
        return Err(Error::InvalidStoredValue("trailing text op bytes"));
    }

    Ok(ops)
}

struct LcaEdits {
    deletes: Vec<(usize, usize)>,
    inserts: Vec<InsertRun>,
}

struct InsertRun {
    pos: usize,
    origin: MergeOrigin,
    bytes: Vec<u8>,
}

fn edits_since_lca(ancestor: &[u8], ops: &[Op], origin: MergeOrigin) -> LcaEdits {
    let value = replay(ancestor, ops);
    let canonical_ops = diff(ancestor, &value);
    let mut deletes = Vec::new();
    let mut inserts = Vec::new();

    for op in canonical_ops {
        match op {
            Op::Insert { pos, content } => inserts.push(InsertRun {
                pos,
                origin,
                bytes: content.inline_bytes().to_vec(),
            }),
            Op::Delete { pos, len } => deletes.push((pos, len)),
        }
    }

    LcaEdits { deletes, inserts }
}

fn mark_deleted(deleted: &mut [bool], edits: &LcaEdits) {
    for (pos, len) in &edits.deletes {
        let end = pos
            .checked_add(*len)
            .expect("delete range end overflows usize");
        assert!(end <= deleted.len(), "delete range exceeds ancestor value");
        deleted[*pos..end].fill(true);
    }
}

fn append_inserts_at(
    pos: usize,
    inserts: &[InsertRun],
    insert_index: &mut usize,
    merged: &mut Vec<u8>,
) {
    while let Some(insert) = inserts.get(*insert_index) {
        if insert.pos != pos {
            break;
        }
        merged.extend_from_slice(&insert.bytes);
        *insert_index += 1;
    }
}

fn encode_content(bytes: &mut Vec<u8>, content: &Content) {
    match content {
        Content::Inline(content) => {
            bytes.push(CONTENT_INLINE);
            write_usize(bytes, content.len());
            bytes.extend_from_slice(content);
        }
        Content::Ref(extent) => {
            bytes.push(CONTENT_REF);
            bytes.extend_from_slice(extent.writer.as_bytes());
            bytes.extend_from_slice(extent.row.as_bytes());
            let column = extent.column.as_bytes();
            let column_len = u32::try_from(column.len()).expect("column name length exceeds u32");
            bytes.extend_from_slice(&column_len.to_be_bytes());
            bytes.extend_from_slice(column);
            bytes.extend_from_slice(&extent.offset.to_be_bytes());
            bytes.extend_from_slice(&extent.len.to_be_bytes());
        }
    }
}

fn decode_content(cursor: &mut Cursor<'_>) -> Result<Content, Error> {
    match cursor.read_u8()? {
        CONTENT_INLINE => {
            let len = cursor.read_usize()?;
            Ok(Content::Inline(cursor.read_bytes(len)?.to_vec()))
        }
        CONTENT_REF => {
            let writer = AuthorId(uuid::Uuid::from_bytes(
                cursor
                    .read_bytes(UUID_LEN)?
                    .try_into()
                    .map_err(|_| Error::InvalidStoredValue("invalid content writer"))?,
            ));
            let row = RowUuid(uuid::Uuid::from_bytes(
                cursor
                    .read_bytes(UUID_LEN)?
                    .try_into()
                    .map_err(|_| Error::InvalidStoredValue("invalid content row"))?,
            ));
            let column_len = u32::from_be_bytes(
                cursor
                    .read_bytes(U32_LEN)?
                    .try_into()
                    .map_err(|_| Error::InvalidStoredValue("invalid content column length"))?,
            );
            let column = String::from_utf8(
                cursor
                    .read_bytes(usize::try_from(column_len).map_err(|_| {
                        Error::InvalidStoredValue("content column length too large")
                    })?)?
                    .to_vec(),
            )
            .map_err(|_| Error::InvalidStoredValue("content column is not utf-8"))?;
            let offset = cursor.read_u64()?;
            let len = cursor.read_u64()?;
            Ok(Content::Ref(Extent {
                writer,
                row,
                column,
                offset,
                len,
            }))
        }
        _ => Err(Error::InvalidStoredValue("unknown text op content tag")),
    }
}

fn adjusted_pos(pos: usize, prior_ops: &[Op], value_len: usize) -> usize {
    let mut adjusted = pos;
    for prior_op in prior_ops {
        match prior_op {
            Op::Insert {
                pos: prior_pos,
                content,
            } => {
                if *prior_pos <= pos {
                    adjusted = adjusted
                        .checked_add(content.len())
                        .expect("text op position overflows replay value");
                }
            }
            Op::Delete {
                pos: prior_pos,
                len,
            } => {
                if *prior_pos < pos {
                    let deleted_before_pos = (*len).min(pos - prior_pos);
                    adjusted = adjusted
                        .checked_sub(deleted_before_pos)
                        .expect("text op position underflows replay value");
                }
            }
        }
    }

    assert!(
        adjusted <= value_len,
        "text op position exceeds replay value"
    );
    adjusted
}

fn write_usize(bytes: &mut Vec<u8>, value: usize) {
    let value = u64::try_from(value).expect("text op field exceeds u64");
    bytes.extend_from_slice(&value.to_be_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        let byte = *self
            .bytes
            .get(self.pos)
            .ok_or(Error::InvalidStoredValue("truncated text op bytes"))?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_usize(&mut self) -> Result<usize, Error> {
        let value = self.read_u64()?;
        usize::try_from(value).map_err(|_| Error::InvalidStoredValue("text op integer too large"))
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        let bytes = self.read_bytes(U64_LEN)?;
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            Error::InvalidStoredValue("invalid text op integer")
        })?))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(Error::InvalidStoredValue("text op length overflow"))?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(Error::InvalidStoredValue("truncated text op bytes"))?;
        self.pos = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_replay_handles_core_cases() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"", b"hello"),
            (b"hello", b"hello world"),
            (b"world", b"hello world"),
            (b"hello world", b"hello brave world"),
            (b"hello brave world", b"hello world"),
            (b"abc", b"xyz"),
            (b"same", b"same"),
            ("aé日z".as_bytes(), "aé!日z".as_bytes()),
            (&[0, 1, 2, 3, 255], &[0, 9, 255]),
        ];

        for (old, new) in cases {
            assert_eq!(replay(old, &diff(old, new)), *new);
        }
    }

    #[test]
    fn diff_uses_common_prefix_suffix_middle_replacement() {
        assert_eq!(
            diff(b"abXYcd", b"abZcd"),
            vec![
                Op::Delete { pos: 2, len: 2 },
                Op::Insert {
                    pos: 2,
                    content: Content::Inline(b"Z".to_vec())
                }
            ]
        );
        assert_eq!(diff(b"abc", b"abc"), Vec::new());
        assert_eq!(diff(b"abc", b"ab"), vec![Op::Delete { pos: 2, len: 1 }]);
        assert_eq!(
            diff(b"ab", b"abc"),
            vec![Op::Insert {
                pos: 2,
                content: Content::Inline(b"c".to_vec())
            }]
        );
    }

    #[test]
    fn diff_replay_inverse_over_many_binary_inputs() {
        let values = sample_values();
        for old in &values {
            for new in &values {
                assert_eq!(replay(old, &diff(old, new)), *new);
            }
        }
    }

    #[test]
    fn encode_decode_round_trips() {
        let batches = vec![
            Vec::new(),
            vec![Op::Insert {
                pos: 0,
                content: Content::Inline(b"hello".to_vec()),
            }],
            vec![Op::Delete { pos: 3, len: 4 }],
            vec![
                Op::Delete { pos: 2, len: 2 },
                Op::Insert {
                    pos: 2,
                    content: Content::Inline(vec![0, 1, 255, b'x']),
                },
            ],
        ];

        for ops in batches {
            assert_eq!(decode(&encode(&ops)).unwrap(), ops);
        }
    }

    #[test]
    fn decode_rejects_malformed_bytes() {
        let mut unknown_tag = encode(&[Op::Delete { pos: 0, len: 1 }]);
        unknown_tag[U64_LEN] = 99;

        let mut trailing = encode(&[]);
        trailing.push(0);

        assert!(decode(&[0, 1]).is_err());
        assert!(decode(&unknown_tag).is_err());
        assert!(decode(&trailing).is_err());
    }

    #[test]
    fn successive_op_batches_materialize_version_chain() {
        let versions: &[&[u8]] = &[
            b"",
            b"hello",
            b"hello world",
            b"hello brave world",
            b"brave world",
            b"brave new world",
            &[0, b'b', b'r', b'a', b'v', b'e', 255],
        ];

        let mut materialized = versions[0].to_vec();
        for window in versions.windows(2) {
            let ops = diff(window[0], window[1]);
            let decoded_ops = decode(&encode(&ops)).unwrap();
            materialized = replay(&materialized, &decoded_ops);
            assert_eq!(materialized, window[1]);
        }
    }

    #[test]
    fn merge_since_lca_keeps_both_non_overlapping_insert_runs() {
        let ancestor = b"abcd";
        let left_ops = vec![Op::Insert {
            pos: 1,
            content: Content::Inline(b"LEFT".to_vec()),
        }];
        let right_ops = vec![Op::Insert {
            pos: 3,
            content: Content::Inline(b"right".to_vec()),
        }];

        let merged = merge_since_lca(
            ancestor,
            (&left_ops, merge_origin(10, 1, 1)),
            (&right_ops, merge_origin(11, 2, 2)),
        );
        assert!(contains_subslice(&merged, b"LEFT"));
        assert!(contains_subslice(&merged, b"right"));
    }

    #[test]
    fn merge_since_lca_is_commutative_under_causal_tie_break() {
        for ancestor in sample_values() {
            for left_pos in 0..=ancestor.len() {
                for right_pos in 0..=ancestor.len() {
                    let left_ops = vec![Op::Insert {
                        pos: left_pos,
                        content: Content::Inline(b"LEFT".to_vec()),
                    }];
                    let right_ops = vec![Op::Insert {
                        pos: right_pos,
                        content: Content::Inline(b"right".to_vec()),
                    }];
                    let left_origin = merge_origin(10, 1, 1);
                    let right_origin = merge_origin(11, 2, 2);

                    assert_eq!(
                        merge_since_lca(
                            &ancestor,
                            (&left_ops, left_origin),
                            (&right_ops, right_origin),
                        ),
                        merge_since_lca(
                            &ancestor,
                            (&right_ops, right_origin),
                            (&left_ops, left_origin),
                        )
                    );
                }
            }
        }
    }

    #[test]
    fn merge_since_lca_orders_same_position_inserts_by_earlier_tx_time() {
        let ancestor = b"ab";
        let later_lexicographically_early_ops = vec![Op::Insert {
            pos: 1,
            content: Content::Inline(b"a-later".to_vec()),
        }];
        let earlier_lexicographically_late_ops = vec![Op::Insert {
            pos: 1,
            content: Content::Inline(b"z-earlier".to_vec()),
        }];

        assert_eq!(
            merge_since_lca(
                ancestor,
                (&later_lexicographically_early_ops, merge_origin(20, 1, 1)),
                (&earlier_lexicographically_late_ops, merge_origin(10, 2, 2)),
            ),
            b"az-earliera-laterb"
        );
    }

    #[test]
    fn merge_since_lca_no_op_side_reduces_to_replay() {
        for ancestor in sample_values() {
            for value in sample_values() {
                let ops = diff(&ancestor, &value);
                assert_eq!(
                    merge_since_lca(
                        &ancestor,
                        (&ops, merge_origin(10, 1, 1)),
                        (&[], merge_origin(11, 2, 2)),
                    ),
                    replay(&ancestor, &ops)
                );
                assert_eq!(
                    merge_since_lca(
                        &ancestor,
                        (&[], merge_origin(10, 1, 1)),
                        (&ops, merge_origin(11, 2, 2)),
                    ),
                    replay(&ancestor, &ops)
                );
            }
        }
    }

    #[test]
    fn merge_since_lca_overlapping_deletes_delete_once() {
        let ancestor = b"abcdefgh";
        let left_ops = vec![Op::Delete { pos: 2, len: 4 }];
        let right_ops = vec![Op::Delete { pos: 2, len: 4 }];

        assert_eq!(
            merge_since_lca(
                ancestor,
                (&left_ops, merge_origin(10, 1, 1)),
                (&right_ops, merge_origin(11, 2, 2)),
            ),
            b"abgh"
        );
    }

    fn merge_origin(time: u64, author: u8, node: u8) -> MergeOrigin {
        MergeOrigin {
            tx_time: TxTime::from(time),
            author: AuthorId::from_bytes([author; 16]),
            node: NodeUuid::from_bytes([node; 16]),
        }
    }

    fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn sample_values() -> Vec<Vec<u8>> {
        vec![
            Vec::new(),
            b"a".to_vec(),
            b"ab".to_vec(),
            b"abc".to_vec(),
            b"xabc".to_vec(),
            b"abcx".to_vec(),
            b"axc".to_vec(),
            b"prefix-middle-suffix".to_vec(),
            b"prefix-NEW-suffix".to_vec(),
            "é".as_bytes().to_vec(),
            "aé日z".as_bytes().to_vec(),
            vec![0, 0, 1, 2, 3, 255],
            vec![0, 1, 9, 3, 255],
        ]
    }
}
