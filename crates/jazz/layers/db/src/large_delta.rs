//! Chunked values as edits extending a version the receiver holds (SPEC
//! 19.8). A delta names a version of the same row by its seq; the
//! receiver rebuilds the value from its own copy of that version's value
//! for the same column. Neither end keeps state for this beyond what sync
//! keeps already: the authority knows the version it last shipped each
//! row at, and a client bases its writes on the newest version it holds
//! settled, which came from the authority.

use jazz_types::large_values::{self as lv, LargeValueRef, epoch};
use serde::{Deserialize, Serialize};

use crate::records::Value;

/// A chunked value as the encoded edits extending the same column's value
/// in the row's version at `seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDelta {
    pub seq: u64,
    /// How many edits the base value has: a check of the base.
    pub base: u32,
    #[serde(with = "serde_bytes")]
    pub edits: Vec<u8>,
}

/// A commit's chunked cell that travels as a [`WireDelta`]: its op's
/// cell holds NULL in its place, under `depth` nullable wrappers. `key`
/// is the written row's branch key (empty unbranched).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellDelta {
    pub op: u32,
    pub column: String,
    pub depth: u8,
    pub key: Vec<u8>,
    pub delta: WireDelta,
}

/// A value's tree (its descriptor without edits, with the tree's lengths)
/// and its encoded edits.
fn split(value: &LargeValueRef) -> Option<(LargeValueRef, Vec<u8>)> {
    let (byte_length, utf16_length) = lv::tail_base_lengths(value).ok()?;
    let edits =
        epoch::encode_edits(value.kind, byte_length, utf16_length, &value.edit_tail).ok()?;
    let tree = LargeValueRef {
        kind: value.kind,
        format_version: value.format_version,
        logical_hash: value.logical_hash,
        root: value.root.clone(),
        byte_length,
        utf16_length,
        edit_tail: Vec::new(),
    };
    Some((tree, edits))
}

/// `next` as a delta over `base` (the value of the version at `seq`):
/// when `next` is `base`'s tree with edits extending `base`'s.
pub(crate) fn extend(seq: u64, base: &LargeValueRef, next: &LargeValueRef) -> Option<WireDelta> {
    let (base_tree, base_edits) = split(base)?;
    let (next_tree, next_edits) = split(next)?;
    (base_tree == next_tree && next_edits.starts_with(&base_edits)).then(|| WireDelta {
        seq,
        base: base.edit_tail.len() as u32,
        edits: next_edits[base_edits.len()..].to_vec(),
    })
}

/// The value `delta` makes of `base`, the value of the version it names.
/// `None` when `base` is not the value it was made over.
pub(crate) fn apply(base: &LargeValueRef, delta: &WireDelta) -> Option<LargeValueRef> {
    if base.edit_tail.len() != delta.base as usize {
        return None;
    }
    let (tree, mut edits) = split(base)?;
    edits.extend_from_slice(&delta.edits);
    let (edit_tail, cursor) =
        epoch::decode_edits(tree.kind, tree.byte_length, tree.utf16_length, &edits, None).ok()?;
    Some(LargeValueRef {
        byte_length: cursor.len,
        utf16_length: tree.utf16_length.map(|_| cursor.utf16_len),
        edit_tail,
        ..tree
    })
}

/// The innermost value of a cell, and how many nullable wrappers it is in.
pub(crate) fn unwrap(value: &mut Value) -> (&mut Value, u8) {
    let mut depth = 0;
    let mut value = value;
    while let Value::Nullable(Some(inner)) = value {
        value = inner;
        depth += 1;
    }
    (value, depth)
}

/// `value` under `depth` nullable wrappers.
pub(crate) fn wrap(mut value: Value, depth: u8) -> Value {
    for _ in 0..depth {
        value = Value::Nullable(Some(Box::new(value)));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(seed: usize) -> LargeValueRef {
        let text = format!("{seed}{}", "x".repeat(lv::INLINE_VALUE_MAX_BYTES));
        lv::prepare(lv::LargeValueKind::String, text.as_bytes())
            .unwrap()
            .value_ref
    }

    fn with(value: &LargeValueRef, appends: &[&str]) -> LargeValueRef {
        let mut value = value.clone();
        for suffix in appends {
            match lv::append_tail(&value, suffix.as_bytes().to_vec()).unwrap() {
                lv::TailOutcome::Updated(next) => value = next,
                lv::TailOutcome::ConsolidationRequired(_) => panic!("the tail is full"),
            }
        }
        value
    }

    #[test]
    fn extending_values_cross_as_their_new_edits() {
        let base = with(&tree(0), &["a"]);
        let next = with(&base, &["bc"]);
        let delta = extend(7, &base, &next).expect("a delta");
        assert_eq!((delta.seq, delta.base), (7, 1));
        assert_eq!(delta.edits, [0x02, b'b', b'c']);
        assert_eq!(apply(&base, &delta).unwrap(), next);
        // The same value is an empty delta.
        let same = extend(7, &next, &next).expect("a delta");
        assert!(same.edits.is_empty());
        assert_eq!(apply(&next, &same).unwrap(), next);
    }

    #[test]
    fn values_that_do_not_extend_the_base_cross_whole() {
        let base = with(&tree(0), &["a"]);
        assert_eq!(extend(1, &base, &with(&tree(0), &["z"])), None, "a fork");
        assert_eq!(
            extend(1, &base, &with(&tree(1), &["a"])),
            None,
            "another tree"
        );
        assert_eq!(extend(1, &with(&base, &["b"]), &base), None, "fewer edits");
    }

    #[test]
    fn a_delta_applies_only_to_the_value_it_was_made_over() {
        let base = with(&tree(0), &["a"]);
        let delta = extend(1, &base, &with(&base, &["b"])).unwrap();
        assert_eq!(apply(&tree(0), &delta), None, "the edit count differs");
        assert_eq!(apply(&with(&base, &["c"]), &delta), None);
    }
}
