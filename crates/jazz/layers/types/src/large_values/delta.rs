//! `delta(a, b)` (SPEC 19.6): edits that turn one chunked value of a column
//! into another, read only where the two differ.
//!
//! Edits are [`ReplaceEdit`]s applied in order, each in the coordinates of
//! the value the earlier ones made. Over one tree, the common edit prefix is
//! kept, a's remaining edits are undone (reading the bytes they deleted) and
//! b's are applied. Over two trees, a is undone to its tree, the trees are
//! walked together with subtrees of equal logical hash skipped, and b's edits
//! are applied. The result need not be minimal when content moves.

use std::collections::HashMap;

use super::{
    AttemptError, ChunkInputs, ChunkNode, ContentHash, Error, LargeValueKind, LargeValueRef,
    MAX_TREE_DEPTH, NodeRef, ReplaceEdit, byte_range_attempt, load_authenticated_node_attempt,
    tail_base_descriptor, utf16_length_of,
};

/// The edits that turn `a`'s value into `b`'s. A read blocked on absent
/// nodes reports [`AttemptError::Blocked`] once every node the attempt can
/// discover is requested.
pub fn delta_attempt(
    a: &LargeValueRef,
    b: &LargeValueRef,
    inputs: &mut ChunkInputs,
) -> Result<Vec<ReplaceEdit>, AttemptError> {
    if a.kind != b.kind {
        return Err(Error::DescriptorMismatch.into());
    }
    let same_tree = a.format_version == b.format_version
        && a.root == b.root
        && a.logical_hash == b.logical_hash;
    let common = if same_tree {
        a.edit_tail
            .iter()
            .zip(&b.edit_tail)
            .take_while(|(x, y)| x == y)
            .count()
    } else {
        0
    };
    let mut out = Vec::new();
    let mut blocked = false;
    // Undo a's own edits, newest first.
    for k in (common..a.edit_tail.len()).rev() {
        let edit = &a.edit_tail[k];
        let deleted = if edit.delete_length == 0 {
            Vec::new()
        } else {
            let before = with_tail(a, k)?;
            let end = edit
                .offset
                .checked_add(edit.delete_length)
                .ok_or(Error::MetricOverflow)?;
            match byte_range_attempt(&before, edit.offset..end, inputs) {
                Err(AttemptError::Blocked) => {
                    blocked = true;
                    Vec::new()
                }
                read => read?,
            }
        };
        out.push(ReplaceEdit {
            offset: edit.offset,
            delete_length: len(&edit.insert_bytes)?,
            insert_bytes: deleted,
            utf16_offset: edit.utf16_offset,
            delete_utf16_length: edit.insert_utf16_length,
            insert_utf16_length: edit.delete_utf16_length,
        });
    }
    if !same_tree {
        let (base_a, base_b) = (tail_base_descriptor(a)?, tail_base_descriptor(b)?);
        let mut walk = Walk {
            a: &base_a,
            b: &base_b,
            inputs,
            out: &mut out,
            blocked: false,
        };
        walk.segment(
            &[Item::root(&base_a)],
            &[Item::root(&base_b)],
            Offset::default(),
            0,
        )?;
        blocked |= walk.blocked;
    }
    if blocked {
        return Err(AttemptError::Blocked);
    }
    out.extend(b.edit_tail[common..].iter().cloned());
    Ok(out)
}

/// `value` with only its first `n` edits.
fn with_tail(value: &LargeValueRef, n: usize) -> Result<LargeValueRef, Error> {
    let mut out = tail_base_descriptor(value)?;
    for edit in &value.edit_tail[..n] {
        out.byte_length = out
            .byte_length
            .checked_sub(edit.delete_length)
            .and_then(|length| length.checked_add(len(&edit.insert_bytes).ok()?))
            .ok_or(Error::MetricOverflow)?;
        out.utf16_length = match out.utf16_length {
            None => None,
            Some(length) => Some(
                length
                    .checked_sub(edit.delete_utf16_length)
                    .and_then(|length| length.checked_add(edit.insert_utf16_length))
                    .ok_or(Error::MetricOverflow)?,
            ),
        };
        out.edit_tail.push(edit.clone());
    }
    Ok(out)
}

fn len(bytes: &[u8]) -> Result<u64, Error> {
    u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)
}

/// A position in both coordinates; UTF-16 stays zero for bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Offset {
    bytes: u64,
    utf16: u64,
}

impl Offset {
    fn plus(self, item: &Item) -> Result<Self, Error> {
        Ok(Offset {
            bytes: self
                .bytes
                .checked_add(item.bytes)
                .ok_or(Error::MetricOverflow)?,
            utf16: self
                .utf16
                .checked_add(item.utf16)
                .ok_or(Error::MetricOverflow)?,
        })
    }
}

/// A subtree in a walk: its node, authenticated logical hash and lengths.
#[derive(Clone, Debug)]
struct Item {
    node: NodeRef,
    hash: ContentHash,
    bytes: u64,
    utf16: u64,
}

impl Item {
    fn root(value: &LargeValueRef) -> Self {
        Item {
            node: value.root.clone(),
            hash: value.logical_hash,
            bytes: value.byte_length,
            utf16: value.utf16_length.unwrap_or(0),
        }
    }
}

/// A run of a frontier: its subtrees one level further down, or the leaf
/// bytes when every item is a leaf.
enum Expanded {
    Items(Vec<Item>),
    Leaves(Vec<u8>),
}

struct Walk<'a> {
    a: &'a LargeValueRef,
    b: &'a LargeValueRef,
    inputs: &'a mut ChunkInputs,
    out: &'a mut Vec<ReplaceEdit>,
    blocked: bool,
}

impl Walk<'_> {
    /// Emit the edits that turn the run `a` into the run `b`, where `a`
    /// starts at `at` (in b's coordinates: everything before is b's already).
    fn segment(
        &mut self,
        mut a: &[Item],
        mut b: &[Item],
        mut at: Offset,
        depth: usize,
    ) -> Result<(), AttemptError> {
        while let (Some(x), Some(y)) = (a.first(), b.first())
            && x.hash == y.hash
        {
            at = at.plus(y)?;
            a = &a[1..];
            b = &b[1..];
        }
        while let (Some(x), Some(y)) = (a.last(), b.last())
            && x.hash == y.hash
        {
            a = &a[..a.len() - 1];
            b = &b[..b.len() - 1];
        }
        if a.is_empty() && b.is_empty() {
            return Ok(());
        }
        if depth > 2 * MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        if b.is_empty() {
            // A deletion reads nothing: its lengths are the subtrees'.
            let gone = a
                .iter()
                .try_fold(Offset::default(), |sum, item| sum.plus(item))?;
            self.out.push(ReplaceEdit {
                offset: at.bytes,
                delete_length: gone.bytes,
                insert_bytes: Vec::new(),
                utf16_offset: at.utf16,
                delete_utf16_length: gone.utf16,
                insert_utf16_length: 0,
            });
            return Ok(());
        }
        let (Some(ea), Some(eb)) = (self.expand(self.a, a)?, self.expand(self.b, b)?) else {
            self.blocked = true;
            return Ok(());
        };
        match (ea, eb) {
            (Expanded::Leaves(a), Expanded::Leaves(b)) => self.replace(&a, &b, at),
            (ea, eb) => {
                let (a, b) = (items(ea, a), items(eb, b));
                self.aligned(&a, &b, at, depth + 1)
            }
        }
    }

    /// Recurse into the unmatched runs between subtrees the two sides share,
    /// matched greedily in order.
    fn aligned(
        &mut self,
        a: &[Item],
        b: &[Item],
        mut at: Offset,
        depth: usize,
    ) -> Result<(), AttemptError> {
        let mut by_hash: HashMap<[u8; 32], Vec<usize>> = HashMap::new();
        for (j, item) in b.iter().enumerate() {
            by_hash.entry(item.hash.0).or_default().push(j);
        }
        let (mut pa, mut pb) = (0, 0);
        for (i, item) in a.iter().enumerate() {
            let Some(js) = by_hash.get(&item.hash.0) else {
                continue;
            };
            let next = js.partition_point(|&j| j < pb);
            let Some(&j) = js.get(next) else {
                continue;
            };
            self.segment(&a[pa..i], &b[pb..j], at, depth)?;
            for item in &b[pb..=j] {
                at = at.plus(item)?;
            }
            (pa, pb) = (i + 1, j + 1);
        }
        self.segment(&a[pa..], &b[pb..], at, depth)
    }

    /// The run one level down: leaves stay, branches become their children.
    /// `None` when a node is absent (every node of the run is requested).
    fn expand(
        &mut self,
        value: &LargeValueRef,
        run: &[Item],
    ) -> Result<Option<Expanded>, AttemptError> {
        let mut nodes = Vec::with_capacity(run.len());
        let mut blocked = false;
        for item in run {
            match load_authenticated_node_attempt(
                value.format_version,
                value.kind,
                &item.node,
                item.hash,
                self.inputs,
            ) {
                Err(AttemptError::Blocked) => blocked = true,
                node => nodes.push(node?),
            }
        }
        if blocked {
            return Ok(None);
        }
        if nodes
            .iter()
            .all(|node| matches!(node, ChunkNode::Leaf { .. }))
        {
            let mut bytes = Vec::new();
            for node in nodes {
                if let ChunkNode::Leaf { bytes: leaf, .. } = node {
                    bytes.extend(leaf);
                }
            }
            return Ok(Some(Expanded::Leaves(bytes)));
        }
        let mut items = Vec::new();
        for (node, item) in nodes.into_iter().zip(run) {
            match node {
                ChunkNode::Leaf { .. } => items.push(item.clone()),
                ChunkNode::Branch { children, .. } => {
                    items.extend(children.into_iter().map(|child| Item {
                        node: child.node_ref,
                        hash: child.logical_hash,
                        bytes: child.metrics.byte_length,
                        utf16: child.metrics.utf16_length.unwrap_or(0),
                    }))
                }
            }
        }
        Ok(Some(Expanded::Items(items)))
    }

    /// One replace of `a`'s bytes by `b`'s, trimmed to where they differ
    /// (on code point boundaries for text).
    fn replace(&mut self, a: &[u8], b: &[u8], at: Offset) -> Result<(), AttemptError> {
        let text = self.a.kind != LargeValueKind::Bytes;
        let mut prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
        let mut suffix = a[prefix..]
            .iter()
            .rev()
            .zip(b[prefix..].iter().rev())
            .take_while(|(x, y)| x == y)
            .count();
        if text {
            while prefix > 0 && !boundary(a, prefix) {
                prefix -= 1;
            }
            while suffix > 0 && !boundary(a, a.len() - suffix) {
                suffix -= 1;
            }
        }
        let deleted = &a[prefix..a.len() - suffix];
        let inserted = &b[prefix..b.len() - suffix];
        if deleted.is_empty() && inserted.is_empty() {
            return Ok(());
        }
        let units = |bytes: &[u8]| -> Result<u64, Error> {
            if !text {
                return Ok(0);
            }
            utf16_length_of(std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?)
        };
        self.out.push(ReplaceEdit {
            offset: at
                .bytes
                .checked_add(len(&a[..prefix])?)
                .ok_or(Error::MetricOverflow)?,
            delete_length: len(deleted)?,
            insert_bytes: inserted.to_vec(),
            utf16_offset: at
                .utf16
                .checked_add(units(&a[..prefix])?)
                .ok_or(Error::MetricOverflow)?,
            delete_utf16_length: units(deleted)?,
            insert_utf16_length: units(inserted)?,
        });
        Ok(())
    }
}

/// A side's run as items: a run of leaves stays as it was.
fn items(expanded: Expanded, run: &[Item]) -> Vec<Item> {
    match expanded {
        Expanded::Items(items) => items,
        Expanded::Leaves(_) => run.to_vec(),
    }
}

/// Whether `at` is a UTF-8 code point boundary of `bytes`.
fn boundary(bytes: &[u8], at: usize) -> bool {
    at == bytes.len() || bytes[at] & 0b1100_0000 != 0b1000_0000
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::{TailOutcome, chunks::ChunkRequest, prepare, replace_tail_attempt};
    use super::*;

    /// The nodes of every value a test made, by object hash.
    #[derive(Default)]
    struct Store(HashMap<[u8; 32], Vec<u8>>);

    impl Store {
        fn value(&mut self, kind: LargeValueKind, bytes: &[u8]) -> LargeValueRef {
            let prepared = prepare(kind, bytes).unwrap();
            for chunk in prepared.staged_chunks {
                self.0.insert(chunk.node_ref.object_hash.0, chunk.encoded);
            }
            prepared.value_ref
        }

        /// Run `attempt` to completion, counting the nodes it read.
        fn run<T>(
            &self,
            mut attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
        ) -> (T, usize) {
            let mut inputs = ChunkInputs::default();
            let mut read = 0;
            loop {
                match attempt(&mut inputs) {
                    Ok(value) => return (value, read),
                    Err(AttemptError::Blocked) => {
                        let missing = inputs.take_missing_chunks();
                        assert!(!missing.is_empty());
                        for request in missing {
                            read += 1;
                            let bytes = self.0[&request.object_hash].clone();
                            inputs.install_chunk(
                                ChunkRequest {
                                    object_hash: request.object_hash,
                                },
                                bytes.into(),
                            );
                        }
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        }

        fn edit(
            &self,
            value: &LargeValueRef,
            at: u64,
            delete: u64,
            insert: &[u8],
        ) -> LargeValueRef {
            match self
                .run(|inputs| replace_tail_attempt(value, at, delete, insert.to_vec(), inputs))
                .0
            {
                TailOutcome::Updated(next) => next,
                TailOutcome::ConsolidationRequired(_) => panic!("the tail is full"),
            }
        }

        fn delta(&self, a: &LargeValueRef, b: &LargeValueRef) -> (Vec<ReplaceEdit>, usize) {
            self.run(|inputs| delta_attempt(a, b, inputs))
        }
    }

    /// `edits` applied in order to `bytes`, by byte coordinates.
    fn apply(bytes: &[u8], edits: &[ReplaceEdit]) -> Vec<u8> {
        let mut out = bytes.to_vec();
        for edit in edits {
            let at = edit.offset as usize;
            out.splice(
                at..at + edit.delete_length as usize,
                edit.insert_bytes.iter().copied(),
            );
        }
        out
    }

    /// `edits` applied in order to `text`, by UTF-16 coordinates.
    fn apply_utf16(text: &str, edits: &[ReplaceEdit]) -> String {
        let mut units: Vec<u16> = text.encode_utf16().collect();
        for edit in edits {
            let at = edit.utf16_offset as usize;
            let insert = std::str::from_utf8(&edit.insert_bytes).unwrap();
            assert_eq!(
                edit.insert_utf16_length as usize,
                insert.encode_utf16().count()
            );
            units.splice(
                at..at + edit.delete_utf16_length as usize,
                insert.encode_utf16(),
            );
        }
        String::from_utf16(&units).unwrap()
    }

    fn text(len: usize, seed: u64) -> String {
        let mut state = seed;
        (0..len)
            .map(|n| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                match (state >> 33) % 23 {
                    0 => 'é',
                    1 => '😀',
                    2 if n % 7 == 0 => '\n',
                    k => (b'a' + k as u8) as char,
                }
            })
            .collect()
    }

    #[test]
    fn a_later_version_of_one_epoch_is_its_new_edits() {
        let mut store = Store::default();
        let body = text(400_000, 1);
        let a = store.value(LargeValueKind::String, body.as_bytes());
        let a = store.edit(&a, 10, 2, "xyz".as_bytes());
        let b = store.edit(&a, 100, 0, "é!".as_bytes());
        let (edits, read) = store.delta(&a, &b);
        assert_eq!(edits, b.edit_tail[1..]);
        assert_eq!(read, 0, "nothing is read");
    }

    #[test]
    fn forked_tails_undo_one_side_and_apply_the_other() {
        let mut store = Store::default();
        let body = text(400_000, 2);
        let base = store.value(LargeValueKind::String, body.as_bytes());
        let base = store.edit(&base, 5, 0, b"common");
        let base_text = materialize(&store, &base);
        let char_at = |n: usize| base_text.char_indices().nth(n).unwrap().0 as u64;
        let a = store.edit(
            &base,
            char_at(1_000),
            char_at(1_012) - char_at(1_000),
            "ours".as_bytes(),
        );
        let a = store.edit(&a, 0, char_at(3), b"");
        let b = store.edit(
            &base,
            char_at(1_040),
            char_at(1_047) - char_at(1_040),
            "theirs😀".as_bytes(),
        );
        let a_text = materialize(&store, &a);
        let b_text = materialize(&store, &b);
        let (edits, _) = store.delta(&a, &b);
        assert_eq!(edits.len(), 3, "two undone, one applied: {edits:?}");
        assert_eq!(apply(a_text.as_bytes(), &edits), b_text.as_bytes());
        assert_eq!(apply_utf16(&a_text, &edits), b_text);
    }

    #[test]
    fn two_trees_differ_only_where_their_content_does() {
        let mut store = Store::default();
        let original = text(3_000_000, 3);
        let mut changed = original.clone();
        let at = changed.char_indices().nth(1_500_000).unwrap().0;
        changed.replace_range(at..at, "an inserted sentence 😀 ");
        let end = changed.char_indices().nth(2_600_000).unwrap().0;
        let end_to = changed.char_indices().nth(2_600_050).unwrap().0;
        changed.replace_range(end..end_to, "");
        let a = store.value(LargeValueKind::String, original.as_bytes());
        let b = store.value(LargeValueKind::String, changed.as_bytes());
        let (edits, read) = store.delta(&a, &b);
        assert_eq!(apply(original.as_bytes(), &edits), changed.as_bytes());
        assert_eq!(apply_utf16(&original, &edits), changed);
        let changed_bytes: u64 = edits
            .iter()
            .map(|edit| edit.delete_length + edit.insert_bytes.len() as u64)
            .sum();
        assert!(
            changed_bytes < 200_000,
            "the edits stay near the changes: {changed_bytes}"
        );
        let nodes = store.0.len();
        assert!(
            read * 4 < nodes,
            "only changed regions are read: {read} of {nodes}"
        );
    }

    #[test]
    fn trees_and_tails_compose_both_ways() {
        let mut store = Store::default();
        let original: Vec<u8> = (0..2_000_000u32).map(|n| (n * 31 % 251) as u8).collect();
        let mut other = original.clone();
        other.splice(700_000..700_010, [1, 2, 3]);
        let a = store.value(LargeValueKind::Bytes, &original);
        let a = store.edit(&a, 1_000_000, 100, b"tail of a");
        let b = store.value(LargeValueKind::Bytes, &other);
        let b = store.edit(&b, 3, 0, b"tail of b");
        let a_bytes = materialize_bytes(&store, &a);
        let b_bytes = materialize_bytes(&store, &b);
        let (edits, _) = store.delta(&a, &b);
        assert_eq!(apply(&a_bytes, &edits), b_bytes);
        let (back, _) = store.delta(&b, &a);
        assert_eq!(apply(&b_bytes, &back), a_bytes);
        assert!(edits.iter().all(|edit| edit.utf16_offset == 0));
    }

    #[test]
    fn deletion_and_insertion_at_the_ends() {
        let mut store = Store::default();
        let original = text(1_000_000, 4);
        let shorter: String = original.chars().take(600_000).collect();
        let longer = format!("{original}{}", text(300_000, 5));
        let a = store.value(LargeValueKind::String, original.as_bytes());
        for target in [&shorter, &longer] {
            let b = store.value(LargeValueKind::String, target.as_bytes());
            let (edits, _) = store.delta(&a, &b);
            assert_eq!(apply_utf16(&original, &edits), *target);
            let (back, _) = store.delta(&b, &a);
            assert_eq!(apply_utf16(target, &back), original);
        }
        let (same, read) = store.delta(&a, &a);
        assert!(same.is_empty());
        assert_eq!(read, 0);
    }

    fn materialize_bytes(store: &Store, value: &LargeValueRef) -> Vec<u8> {
        store
            .run(|inputs| super::super::materialize_attempt(value, inputs))
            .0
    }

    fn materialize(store: &Store, value: &LargeValueRef) -> String {
        String::from_utf8(materialize_bytes(store, value)).unwrap()
    }
}
