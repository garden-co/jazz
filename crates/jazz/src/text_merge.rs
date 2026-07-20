//! Pure plaintext operation algebra and in-memory text merge walk.
//!
//! This module is intentionally independent of row storage, protocol encoding,
//! schema lowering, and strategy escalation. Operations are byte-oriented
//! retain/insert/delete runs whose positions are interpreted by the operation
//! stream, not by stored row history.

use std::collections::{BTreeMap, BTreeSet};

const TAG_RETAIN: u8 = 0;
const TAG_INSERT: u8 = 1;
const TAG_DELETE: u8 = 2;
const U64_LEN: usize = 8;

/// Deterministic event identifier used by the pure merge walker.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EventId(pub u64);

/// Tie-break key for concurrent runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TieBreak(pub u64);

/// A normalized plaintext operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TextOp {
    runs: Vec<Run>,
}

/// One run in a plaintext operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Run {
    /// Copy `len` bytes from the input.
    Retain(usize),
    /// Insert these bytes at the current output position.
    Insert(Vec<u8>),
    /// Drop `len` bytes from the input.
    Delete(usize),
}

/// A transformed pair where `left_prime` applies after the original right op
/// and `right_prime` applies after the original left op.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transformed {
    /// Left operation transformed over the right operation.
    pub left_prime: TextOp,
    /// Right operation transformed over the left operation.
    pub right_prime: TextOp,
}

/// Errors returned by pure operation and graph APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TextMergeError {
    /// An operation tried to consume bytes beyond the input document.
    OperationConsumesPastEnd,
    /// A graph event id was inserted twice.
    DuplicateEvent(EventId),
    /// A referenced event was missing from the graph.
    MissingEvent(EventId),
    /// A graph walk found a cycle.
    Cycle(EventId),
    /// The requested heads do not share the supplied ancestor.
    AncestorNotOnHeadPath(EventId, EventId),
    /// The graph event has multiple parents; Step 1 walker expects raw edit
    /// paths between the chosen ancestor and heads.
    MergeEventOnRawPath(EventId),
    /// Encoded op bytes ended before the declared value was complete.
    TruncatedEncoding,
    /// Encoded op bytes carried an unknown run tag.
    UnknownRunTag,
    /// Encoded op bytes had trailing data.
    TrailingEncoding,
    /// Encoded op integer does not fit this platform.
    IntegerTooLarge,
    /// Strategy-specific input or configuration was invalid.
    StrategyInputInvalid,
}

/// In-memory edit event graph used by the pure merge walk.
#[derive(Clone, Debug, Default)]
pub struct TextEventGraph {
    events: BTreeMap<EventId, TextEvent>,
}

/// One event in a [`TextEventGraph`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextEvent {
    /// Event id.
    pub id: EventId,
    /// Parent events. The Step 1 walker accepts zero or one parent on each raw
    /// ancestor-to-head path.
    pub parents: Vec<EventId>,
    /// Operation relative to the materialized first-parent value.
    pub op: TextOp,
    /// Tie-break key used for deterministic concurrent run ordering.
    pub tie_break: TieBreak,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EditRun {
    pos: usize,
    delete_len: usize,
    insert: Vec<u8>,
    tie_break: TieBreak,
}

impl TextOp {
    /// Build and normalize a text operation from runs.
    pub fn new(runs: impl IntoIterator<Item = Run>) -> Self {
        let mut op = Self { runs: Vec::new() };
        for run in runs {
            op.push(run);
        }
        op
    }

    /// Return a no-op operation.
    pub fn identity() -> Self {
        Self::default()
    }

    /// Return the normalized runs.
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    /// Apply this operation to `input`.
    pub fn apply(&self, input: &[u8]) -> Result<Vec<u8>, TextMergeError> {
        let mut cursor = 0usize;
        let mut out = Vec::new();
        for run in &self.runs {
            match run {
                Run::Retain(len) => {
                    let end = cursor
                        .checked_add(*len)
                        .ok_or(TextMergeError::OperationConsumesPastEnd)?;
                    let retained = input
                        .get(cursor..end)
                        .ok_or(TextMergeError::OperationConsumesPastEnd)?;
                    out.extend_from_slice(retained);
                    cursor = end;
                }
                Run::Insert(bytes) => out.extend_from_slice(bytes),
                Run::Delete(len) => {
                    let end = cursor
                        .checked_add(*len)
                        .ok_or(TextMergeError::OperationConsumesPastEnd)?;
                    input
                        .get(cursor..end)
                        .ok_or(TextMergeError::OperationConsumesPastEnd)?;
                    cursor = end;
                }
            }
        }
        out.extend_from_slice(&input[cursor..]);
        Ok(out)
    }

    /// Compose two operations for a known base value.
    ///
    /// The returned operation has the same effect on `base` as applying `self`
    /// and then `next`. This pure Step 1 helper deliberately canonicalizes
    /// through a diff against `base`, avoiding storage-specific metadata.
    pub fn compose_on(&self, base: &[u8], next: &TextOp) -> Result<TextOp, TextMergeError> {
        let middle = self.apply(base)?;
        let composed = next.apply(&middle)?;
        Ok(diff(base, &composed))
    }

    /// Encode this operation into deterministic storage/wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_usize(&mut bytes, self.runs.len());
        for run in &self.runs {
            match run {
                Run::Retain(len) => {
                    bytes.push(TAG_RETAIN);
                    write_usize(&mut bytes, *len);
                }
                Run::Insert(insert) => {
                    bytes.push(TAG_INSERT);
                    write_usize(&mut bytes, insert.len());
                    bytes.extend_from_slice(insert);
                }
                Run::Delete(len) => {
                    bytes.push(TAG_DELETE);
                    write_usize(&mut bytes, *len);
                }
            }
        }
        bytes
    }

    fn push(&mut self, run: Run) {
        match run {
            Run::Retain(0) | Run::Delete(0) => {}
            Run::Insert(bytes) if bytes.is_empty() => {}
            Run::Retain(len) => match self.runs.last_mut() {
                Some(Run::Retain(prev)) => *prev += len,
                _ => self.runs.push(Run::Retain(len)),
            },
            Run::Delete(len) => match self.runs.last_mut() {
                Some(Run::Delete(prev)) => *prev += len,
                _ => self.runs.push(Run::Delete(len)),
            },
            Run::Insert(bytes) => match self.runs.last_mut() {
                Some(Run::Insert(prev)) => prev.extend(bytes),
                _ => self.runs.push(Run::Insert(bytes)),
            },
        }
    }
}

/// Decode bytes produced by [`TextOp::encode`].
pub fn decode(bytes: &[u8]) -> Result<TextOp, TextMergeError> {
    let mut cursor = Cursor { bytes, pos: 0 };
    let run_count = cursor.read_usize()?;
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        match cursor.read_u8()? {
            TAG_RETAIN => runs.push(Run::Retain(cursor.read_usize()?)),
            TAG_INSERT => {
                let len = cursor.read_usize()?;
                runs.push(Run::Insert(cursor.read_bytes(len)?.to_vec()));
            }
            TAG_DELETE => runs.push(Run::Delete(cursor.read_usize()?)),
            _ => return Err(TextMergeError::UnknownRunTag),
        }
    }
    if cursor.remaining() != 0 {
        return Err(TextMergeError::TrailingEncoding);
    }
    Ok(TextOp::new(runs))
}

/// Return a canonical middle-replacement op from `old` to `new`.
pub fn diff(old: &[u8], new: &[u8]) -> TextOp {
    if old == new {
        return TextOp::identity();
    }

    let prefix = old
        .iter()
        .zip(new.iter())
        .take_while(|(old_byte, new_byte)| old_byte == new_byte)
        .count();
    let suffix_limit = old.len().min(new.len()) - prefix;
    let suffix = old[prefix..]
        .iter()
        .rev()
        .zip(new[prefix..].iter().rev())
        .take(suffix_limit)
        .take_while(|(old_byte, new_byte)| old_byte == new_byte)
        .count();

    let mut runs = Vec::new();
    runs.push(Run::Retain(prefix));
    let delete_len = old.len() - prefix - suffix;
    runs.push(Run::Delete(delete_len));
    let insert_end = new.len() - suffix;
    runs.push(Run::Insert(new[prefix..insert_end].to_vec()));
    TextOp::new(runs)
}

/// Transform two concurrent operations for a known base value.
pub fn transform_on(
    base: &[u8],
    left: (&TextOp, TieBreak),
    right: (&TextOp, TieBreak),
) -> Result<Transformed, TextMergeError> {
    let merged = merge_concurrent_ops(base, [left, right])?;
    let left_value = left.0.apply(base)?;
    let right_value = right.0.apply(base)?;
    Ok(Transformed {
        left_prime: diff(&right_value, &merged),
        right_prime: diff(&left_value, &merged),
    })
}

/// Merge concurrent operations against `base` using deterministic tie-breaks.
pub fn merge_concurrent_ops<'a>(
    base: &[u8],
    ops: impl IntoIterator<Item = (&'a TextOp, TieBreak)>,
) -> Result<Vec<u8>, TextMergeError> {
    let edits = ops
        .into_iter()
        .map(|(op, tie_break)| edit_since_base(base, op, tie_break))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(apply_edit_runs(base, edits.into_iter().flatten().collect()))
}

impl TextEventGraph {
    /// Insert an event.
    pub fn insert(&mut self, event: TextEvent) -> Result<(), TextMergeError> {
        let id = event.id;
        if self.events.insert(id, event).is_some() {
            return Err(TextMergeError::DuplicateEvent(id));
        }
        Ok(())
    }

    /// Deterministically merge the raw edit paths from `ancestor` to `heads`.
    pub fn merge_heads(
        &self,
        ancestor: EventId,
        ancestor_value: &[u8],
        heads: &[EventId],
    ) -> Result<Vec<u8>, TextMergeError> {
        let mut head_ops = Vec::new();
        for head in heads {
            let (op, tie_break) =
                self.compose_path_from_ancestor(ancestor, *head, ancestor_value)?;
            head_ops.push((op, tie_break));
        }
        merge_concurrent_ops(
            ancestor_value,
            head_ops.iter().map(|(op, tie_break)| (op, *tie_break)),
        )
    }

    fn compose_path_from_ancestor(
        &self,
        ancestor: EventId,
        head: EventId,
        ancestor_value: &[u8],
    ) -> Result<(TextOp, TieBreak), TextMergeError> {
        if head == ancestor {
            return Ok((TextOp::identity(), TieBreak(0)));
        }

        let mut path = Vec::new();
        let mut seen = BTreeSet::new();
        let mut current = head;
        loop {
            if !seen.insert(current) {
                return Err(TextMergeError::Cycle(current));
            }
            if current == ancestor {
                break;
            }
            let event = self
                .events
                .get(&current)
                .ok_or(TextMergeError::MissingEvent(current))?;
            if event.parents.len() > 1 {
                return Err(TextMergeError::MergeEventOnRawPath(current));
            }
            path.push(event);
            current = *event
                .parents
                .first()
                .ok_or(TextMergeError::AncestorNotOnHeadPath(ancestor, head))?;
        }

        path.reverse();
        let mut value = ancestor_value.to_vec();
        let mut tie_break = TieBreak(0);
        for event in path {
            value = event.op.apply(&value)?;
            tie_break = event.tie_break;
        }
        Ok((diff(ancestor_value, &value), tie_break))
    }
}

fn edit_since_base(
    base: &[u8],
    op: &TextOp,
    tie_break: TieBreak,
) -> Result<Vec<EditRun>, TextMergeError> {
    let value = op.apply(base)?;
    let canonical = diff(base, &value);
    let mut cursor = 0usize;
    let mut edits = Vec::new();
    for run in canonical.runs() {
        match run {
            Run::Retain(len) => cursor += *len,
            Run::Delete(delete_len) => {
                edits.push(EditRun {
                    pos: cursor,
                    delete_len: *delete_len,
                    insert: Vec::new(),
                    tie_break,
                });
                cursor += *delete_len;
            }
            Run::Insert(insert) => edits.push(EditRun {
                pos: cursor,
                delete_len: 0,
                insert: insert.clone(),
                tie_break,
            }),
        }
    }
    Ok(edits)
}

fn apply_edit_runs(base: &[u8], mut edits: Vec<EditRun>) -> Vec<u8> {
    let mut deleted = vec![false; base.len()];
    for edit in &edits {
        let end = edit.pos + edit.delete_len;
        deleted[edit.pos..end].fill(true);
    }

    edits.sort_by(|left, right| {
        left.pos
            .cmp(&right.pos)
            .then_with(|| left.tie_break.cmp(&right.tie_break))
    });

    let mut out = Vec::new();
    let mut edit_index = 0usize;
    for (pos, byte) in base.iter().copied().enumerate() {
        append_inserts_at(pos, &edits, &mut edit_index, &mut out);
        if !deleted[pos] {
            out.push(byte);
        }
    }
    append_inserts_at(base.len(), &edits, &mut edit_index, &mut out);
    out
}

fn append_inserts_at(pos: usize, edits: &[EditRun], index: &mut usize, out: &mut Vec<u8>) {
    while let Some(edit) = edits.get(*index) {
        if edit.pos != pos {
            break;
        }
        if !edit.insert.is_empty() {
            out.extend_from_slice(&edit.insert);
        }
        *index += 1;
    }
}

fn write_usize(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(
        &u64::try_from(value)
            .expect("text op field exceeds u64")
            .to_be_bytes(),
    );
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn read_u8(&mut self) -> Result<u8, TextMergeError> {
        let byte = *self
            .bytes
            .get(self.pos)
            .ok_or(TextMergeError::TruncatedEncoding)?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_usize(&mut self) -> Result<usize, TextMergeError> {
        let bytes = self.read_bytes(U64_LEN)?;
        let value = u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| TextMergeError::TruncatedEncoding)?,
        );
        usize::try_from(value).map_err(|_| TextMergeError::IntegerTooLarge)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], TextMergeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(TextMergeError::TruncatedEncoding)?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(TextMergeError::TruncatedEncoding)?;
        self.pos = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_are_run_length_encoded() {
        let op = TextOp::new([
            Run::Retain(1),
            Run::Retain(2),
            Run::Insert(b"a".to_vec()),
            Run::Insert(b"bc".to_vec()),
            Run::Delete(1),
            Run::Delete(2),
            Run::Retain(0),
        ]);

        assert_eq!(
            op.runs(),
            &[Run::Retain(3), Run::Insert(b"abc".to_vec()), Run::Delete(3)]
        );
    }

    #[test]
    fn encode_decode_round_trips_retained_runs() {
        let op = TextOp::new([
            Run::Retain(3),
            Run::Insert(b"hello".to_vec()),
            Run::Delete(2),
        ]);

        assert_eq!(decode(&op.encode()).unwrap(), op);
        assert!(decode(&[0, 1]).is_err());
    }

    #[test]
    fn sequential_fast_path_matches_naive_apply() {
        let base = b"abcdef";
        let first = TextOp::new([Run::Retain(2), Run::Delete(2), Run::Insert(b"XY".to_vec())]);
        let second = TextOp::new([Run::Retain(4), Run::Insert(b"!".to_vec())]);

        let naive = second.apply(&first.apply(base).unwrap()).unwrap();
        let composed = first.compose_on(base, &second).unwrap();
        assert_eq!(composed.apply(base).unwrap(), naive);
    }

    #[test]
    fn disjoint_concurrent_edits_are_both_preserved() {
        let base = b"abcdef";
        let left = TextOp::new([Run::Retain(1), Run::Insert(b"L".to_vec())]);
        let right = TextOp::new([Run::Retain(5), Run::Insert(b"R".to_vec())]);

        let merged =
            merge_concurrent_ops(base, [(&left, TieBreak(1)), (&right, TieBreak(2))]).unwrap();
        assert_eq!(merged, b"aLbcdeRf");

        let transformed = transform_on(base, (&left, TieBreak(1)), (&right, TieBreak(2))).unwrap();
        assert_eq!(
            transformed
                .left_prime
                .apply(&right.apply(base).unwrap())
                .unwrap(),
            merged
        );
        assert_eq!(
            transformed
                .right_prime
                .apply(&left.apply(base).unwrap())
                .unwrap(),
            merged
        );
    }

    #[test]
    fn event_graph_walk_is_deterministic_for_same_tie_break_order() {
        let mut graph = TextEventGraph::default();
        let root = EventId(0);
        graph
            .insert(TextEvent {
                id: root,
                parents: Vec::new(),
                op: TextOp::identity(),
                tie_break: TieBreak(0),
            })
            .unwrap();
        graph
            .insert(TextEvent {
                id: EventId(1),
                parents: vec![root],
                op: TextOp::new([Run::Retain(1), Run::Insert(b"L".to_vec())]),
                tie_break: TieBreak(10),
            })
            .unwrap();
        graph
            .insert(TextEvent {
                id: EventId(2),
                parents: vec![root],
                op: TextOp::new([Run::Retain(1), Run::Insert(b"R".to_vec())]),
                tie_break: TieBreak(20),
            })
            .unwrap();

        let heads = [EventId(2), EventId(1)];
        let first = graph.merge_heads(root, b"ab", &heads).unwrap();
        for _ in 0..16 {
            assert_eq!(graph.merge_heads(root, b"ab", &heads).unwrap(), first);
        }
        assert_eq!(first, b"aLRb");
    }

    #[test]
    fn randomized_op_fuzz_matches_simple_reference() {
        let mut rng = Lcg::new(0x5eed_cafe);
        for _ in 0..2_000 {
            let base = random_bytes(&mut rng, 16);
            let left_value = random_bytes(&mut rng, 20);
            let right_value = random_bytes(&mut rng, 20);
            let left = diff(&base, &left_value);
            let right = diff(&base, &right_value);
            let left_tie = TieBreak(rng.next_u64() % 100);
            let right_tie = TieBreak(100 + rng.next_u64() % 100);

            let merged =
                merge_concurrent_ops(&base, [(&left, left_tie), (&right, right_tie)]).unwrap();
            let reference = reference_merge(&base, [(&left, left_tie), (&right, right_tie)]);
            assert_eq!(merged, reference);

            let transformed = transform_on(&base, (&left, left_tie), (&right, right_tie)).unwrap();
            assert_eq!(
                transformed
                    .left_prime
                    .apply(&right.apply(&base).unwrap())
                    .unwrap(),
                merged
            );
            assert_eq!(
                transformed
                    .right_prime
                    .apply(&left.apply(&base).unwrap())
                    .unwrap(),
                merged
            );
        }
    }

    #[test]
    fn randomized_graph_fuzz_matches_reference_merge() {
        let mut rng = Lcg::new(0x600d_f00d);
        for case in 0..512 {
            let base = random_bytes(&mut rng, 12);
            let root = EventId(0);
            let mut graph = TextEventGraph::default();
            graph
                .insert(TextEvent {
                    id: root,
                    parents: Vec::new(),
                    op: TextOp::identity(),
                    tie_break: TieBreak(0),
                })
                .unwrap();

            let mut heads = Vec::new();
            let mut expected_ops = Vec::new();
            for branch in 0..3 {
                let parent_value = random_bytes(&mut rng, 12);
                let child_value = random_bytes(&mut rng, 12);
                let first = diff(&base, &parent_value);
                let second = diff(&parent_value, &child_value);
                let first_id = EventId(1 + case * 10 + branch * 2);
                let second_id = EventId(first_id.0 + 1);
                graph
                    .insert(TextEvent {
                        id: first_id,
                        parents: vec![root],
                        op: first,
                        tie_break: TieBreak(first_id.0),
                    })
                    .unwrap();
                graph
                    .insert(TextEvent {
                        id: second_id,
                        parents: vec![first_id],
                        op: second,
                        tie_break: TieBreak(second_id.0),
                    })
                    .unwrap();
                heads.push(second_id);
                expected_ops.push((diff(&base, &child_value), TieBreak(second_id.0)));
            }

            let merged = graph.merge_heads(root, &base, &heads).unwrap();
            let reference = reference_merge(&base, expected_ops.iter().map(|(op, tie)| (op, *tie)));
            assert_eq!(merged, reference);
        }
    }

    fn reference_merge<'a>(
        base: &[u8],
        ops: impl IntoIterator<Item = (&'a TextOp, TieBreak)>,
    ) -> Vec<u8> {
        let mut deletes = BTreeSet::new();
        let mut inserts: BTreeMap<usize, Vec<(TieBreak, Vec<u8>)>> = BTreeMap::new();
        for (op, tie_break) in ops {
            let value = op.apply(base).unwrap();
            let canonical = diff(base, &value);
            let mut cursor = 0usize;
            for run in canonical.runs() {
                match run {
                    Run::Retain(len) => cursor += *len,
                    Run::Delete(len) => {
                        for pos in cursor..cursor + len {
                            deletes.insert(pos);
                        }
                        cursor += *len;
                    }
                    Run::Insert(bytes) => {
                        inserts
                            .entry(cursor)
                            .or_default()
                            .push((tie_break, bytes.clone()));
                    }
                }
            }
        }

        for runs in inserts.values_mut() {
            runs.sort_by_key(|(tie_break, _)| *tie_break);
        }

        let mut out = Vec::new();
        for pos in 0..=base.len() {
            if let Some(runs) = inserts.get(&pos) {
                for (_, bytes) in runs {
                    out.extend_from_slice(bytes);
                }
            }
            if pos < base.len() && !deletes.contains(&pos) {
                out.push(base[pos]);
            }
        }
        out
    }

    fn random_bytes(rng: &mut Lcg, max_len: usize) -> Vec<u8> {
        let len = (rng.next_u64() as usize) % (max_len + 1);
        (0..len)
            .map(|_| b'a' + ((rng.next_u64() as u8) % 6))
            .collect()
    }

    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            self.0
        }
    }
}
