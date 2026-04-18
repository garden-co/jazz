use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::query_manager::types::{Tuple, TupleDelta, TupleElement};

/// Borrowed tuple identity key with a precomputed hash.
///
/// We keep the "hash once, compare full IDs on collision" behavior from the old
/// implementation, but we borrow the tuple instead of allocating a temporary
/// `Vec<ObjectId>` just to key the maps in this function.
#[derive(Clone, Copy)]
struct HashedTupleRef<'a> {
    tuple: &'a Tuple,
    hash: u64,
}

impl<'a> HashedTupleRef<'a> {
    fn new(tuple: &'a Tuple) -> Self {
        let mut hasher = std::hash::DefaultHasher::new();
        tuple.hash(&mut hasher);
        Self {
            tuple,
            hash: hasher.finish(),
        }
    }
}

impl PartialEq for HashedTupleRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.tuple == other.tuple
    }
}

impl Eq for HashedTupleRef<'_> {}

impl Hash for HashedTupleRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

pub(crate) fn compute_tuple_delta(old_tuples: &[Tuple], new_tuples: &[Tuple]) -> TupleDelta {
    let mut delta = TupleDelta::new();
    let old_hashed: Vec<_> = old_tuples.iter().map(HashedTupleRef::new).collect();
    let new_hashed: Vec<_> = new_tuples.iter().map(HashedTupleRef::new).collect();
    let old_pos_by_ids: HashMap<&HashedTupleRef<'_>, usize> = old_hashed
        .iter()
        .enumerate()
        .map(|(idx, h)| (h, idx))
        .collect();
    let new_pos_by_ids: HashMap<&HashedTupleRef<'_>, usize> = new_hashed
        .iter()
        .enumerate()
        .map(|(idx, h)| (h, idx))
        .collect();

    for (old, h) in old_tuples.iter().zip(old_hashed.iter()) {
        if !new_pos_by_ids.contains_key(h) {
            delta.removed.push(old.clone());
        }
    }

    for (new, h) in new_tuples.iter().zip(new_hashed.iter()) {
        if !old_pos_by_ids.contains_key(h) {
            delta.added.push(new.clone());
        }
    }

    let new_to_old_idx_mapping: Vec<_> = new_hashed
        .iter()
        .enumerate()
        .filter_map(|(new_idx, h)| {
            old_pos_by_ids
                .get(h)
                .copied()
                .map(|old_idx| (new_idx, old_idx))
        })
        .collect();
    let old_idx_sequence: Vec<_> = new_to_old_idx_mapping
        .iter()
        .map(|(_, old_idx)| *old_idx)
        .collect();
    let lis_positions = lis_positions(&old_idx_sequence);
    let mut keep = vec![false; old_idx_sequence.len()];
    for pos in lis_positions {
        keep[pos] = true;
    }
    for (i, (new_idx, _)) in new_to_old_idx_mapping.iter().enumerate() {
        if !keep[i] {
            delta.moved.push(tuple_as_id_only(&new_tuples[*new_idx]));
        }
    }

    for (old, h) in old_tuples.iter().zip(old_hashed.iter()) {
        if let Some(new_idx) = new_pos_by_ids.get(h)
            && has_tuple_content_changed(old, &new_tuples[*new_idx])
        {
            delta
                .updated
                .push((old.clone(), new_tuples[*new_idx].clone()));
        }
    }

    delta
}

/// Returns the positions of the Longest Increasing Subsequence in the input sequence.
/// This is the fastest way to find the positions of the moved tuples, in O(n log n) time.
/// How it works:
/// 1. Keep only IDs present in both old and new lists.
/// 2. Replace each ID in new order with its position in the old order.
/// 3. You get a number sequence.
///   - If relative order is unchanged, sequence is increasing.
///   - Reorders create decreases.
/// 4. Find the Longest Increasing Subsequence (LIS) in that sequence.
///    - LIS elements are the largest set you can keep "as is" without moving.
/// 5. Mark IDs not in LIS as moved (in new order).
///
/// Example:
/// old: [A, B, C, D]
/// new: [B, C, A, D]
/// old positions: A=0,B=1,C=2,D=3
/// new mapped to old positions: [1,2,0,3]
/// LIS is [1,2,3] => IDs [B,C,D] stay
/// non-LIS is A => only A is moved
/// Result: [B, C, D] are not moved, A is moved.
fn lis_positions(sequence: &[usize]) -> Vec<usize> {
    if sequence.is_empty() {
        return Vec::new();
    }

    let mut tails_values: Vec<usize> = Vec::new();
    let mut tails_indices: Vec<usize> = Vec::new();
    let mut predecessors: Vec<Option<usize>> = vec![None; sequence.len()];

    for (idx, &value) in sequence.iter().enumerate() {
        let pos = match tails_values.binary_search(&value) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        if pos > 0 {
            predecessors[idx] = Some(tails_indices[pos - 1]);
        }

        if pos == tails_values.len() {
            tails_values.push(value);
            tails_indices.push(idx);
        } else {
            tails_values[pos] = value;
            tails_indices[pos] = idx;
        }
    }

    let mut lis = Vec::new();
    let mut current = tails_indices.last().copied();
    while let Some(idx) = current {
        lis.push(idx);
        current = predecessors[idx];
    }
    lis.reverse();
    lis
}

fn tuple_as_id_only(tuple: &Tuple) -> Tuple {
    Tuple::new(
        tuple
            .iter()
            .map(|elem| TupleElement::Id(elem.id()))
            .collect(),
    )
    .with_provenance(tuple.provenance().clone())
    .with_batch_provenance(tuple.batch_provenance().clone())
}

/// Check if tuple content or provenance changed (for tuples with same IDs).
fn has_tuple_content_changed(old: &Tuple, new: &Tuple) -> bool {
    if old.provenance() != new.provenance() || old.batch_provenance() != new.batch_provenance() {
        return true;
    }

    old.iter().zip(new.iter()).any(|(o, n)| match (o, n) {
        (
            TupleElement::Row {
                content: old_content,
                batch_id: old_commit,
                ..
            },
            TupleElement::Row {
                content: new_content,
                batch_id: new_commit,
                ..
            },
        ) => old_content != new_content || old_commit != new_commit,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{BranchName, ObjectId};

    fn id_tuple(ids: &[ObjectId]) -> Tuple {
        Tuple::new(ids.iter().copied().map(TupleElement::Id).collect())
    }

    fn row_tuple(id: ObjectId, content: &[u8], commit_byte: u8) -> Tuple {
        Tuple::new(vec![TupleElement::Row {
            id,
            content: content.to_vec().into(),
            batch_id: crate::row_histories::BatchId([commit_byte; 16]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    #[test]
    fn compute_tuple_delta_reports_moves_from_id_order_only() {
        let a = ObjectId::new();
        let b = ObjectId::new();
        let c = ObjectId::new();
        let d = ObjectId::new();

        let old = vec![
            id_tuple(&[a]),
            id_tuple(&[b]),
            id_tuple(&[c]),
            id_tuple(&[d]),
        ];
        let new = vec![
            id_tuple(&[b]),
            id_tuple(&[c]),
            id_tuple(&[a]),
            id_tuple(&[d]),
        ];

        let delta = compute_tuple_delta(&old, &new);

        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(delta.updated.is_empty());
        assert_eq!(delta.moved, vec![id_tuple(&[a])]);
    }

    #[test]
    fn compute_tuple_delta_reports_updates_for_same_ids() {
        let id = ObjectId::new();
        let old = vec![row_tuple(id, b"old", 1)];
        let new = vec![row_tuple(id, b"new", 2)];

        let delta = compute_tuple_delta(&old, &new);

        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(delta.moved.is_empty());
        assert_eq!(delta.updated, vec![(old[0].clone(), new[0].clone())]);
    }

    #[test]
    fn compute_tuple_delta_reports_provenance_only_updates() {
        let id = ObjectId::new();
        let branch = BranchName::new("main");
        let old = vec![id_tuple(&[id])];
        let new = vec![id_tuple(&[id]).with_provenance([(id, branch)].into_iter().collect())];

        let delta = compute_tuple_delta(&old, &new);

        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(delta.moved.is_empty());
        assert_eq!(delta.updated, vec![(old[0].clone(), new[0].clone())]);
    }
}
