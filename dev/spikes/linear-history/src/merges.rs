//! Example three-way merge functions. Each takes `(base, ours, theirs)`.

use crate::model::Value;

fn i64_of(value: Option<&[u8]>) -> i64 {
    value.map_or(0, |b| i64::from_le_bytes(b.try_into().expect("i64 cell")))
}

pub fn i64_value(v: i64) -> Value {
    Some(v.to_le_bytes().to_vec())
}

pub fn value_i64(value: Option<&[u8]>) -> i64 {
    i64_of(value)
}

/// Counter: `ours + (theirs - base)`.
pub fn counter_i64(base: Option<&[u8]>, ours: Option<&[u8]>, theirs: Option<&[u8]>) -> Value {
    i64_value(i64_of(ours).wrapping_add(i64_of(theirs).wrapping_sub(i64_of(base))))
}

fn set_of(value: Option<&[u8]>) -> std::collections::BTreeSet<u64> {
    value
        .unwrap_or_default()
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

pub fn u64_set_value(items: impl IntoIterator<Item = u64>) -> Value {
    let set: std::collections::BTreeSet<u64> = items.into_iter().collect();
    Some(set.into_iter().flat_map(u64::to_le_bytes).collect())
}

pub fn value_u64_set(value: Option<&[u8]>) -> Vec<u64> {
    set_of(value).into_iter().collect()
}

/// Set with removals: `(ours ∪ (theirs − base)) − (base − theirs)`.
pub fn u64_set(base: Option<&[u8]>, ours: Option<&[u8]>, theirs: Option<&[u8]>) -> Value {
    let (base, ours, theirs) = (set_of(base), set_of(ours), set_of(theirs));
    let added = theirs.difference(&base);
    let removed: std::collections::BTreeSet<_> = base.difference(&theirs).copied().collect();
    u64_set_value(
        ours.iter()
            .chain(added)
            .copied()
            .filter(|x| !removed.contains(x)),
    )
}
