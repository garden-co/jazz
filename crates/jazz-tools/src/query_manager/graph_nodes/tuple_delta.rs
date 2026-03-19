use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::object::ObjectId;
use crate::query_manager::types::{Tuple, TupleDelta, TupleElement};

/// Pre-hashed ID list: hash is computed once at construction, equality falls back to full comparison.
#[derive(Clone)]
struct HashedIds {
    ids: Vec<ObjectId>,
    hash: u64,
}

impl HashedIds {
    fn new(ids: Vec<ObjectId>) -> Self {
        let mut hasher = std::hash::DefaultHasher::new();
        ids.hash(&mut hasher);
        Self {
            ids,
            hash: hasher.finish(),
        }
    }
}

impl PartialEq for HashedIds {
    fn eq(&self, other: &Self) -> bool {
        self.ids == other.ids
    }
}

impl Eq for HashedIds {}

impl Hash for HashedIds {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

pub(crate) fn compute_tuple_delta(old_tuples: &[Tuple], new_tuples: &[Tuple]) -> TupleDelta {
    let mut delta = TupleDelta::new();
    let old_hashed: Vec<HashedIds> = old_tuples.iter().map(|t| HashedIds::new(t.ids())).collect();
    let new_hashed: Vec<HashedIds> = new_tuples.iter().map(|t| HashedIds::new(t.ids())).collect();
    let old_pos_by_ids: HashMap<&HashedIds, usize> = old_hashed
        .iter()
        .enumerate()
        .map(|(idx, h)| (h, idx))
        .collect();
    let new_pos_by_ids: HashMap<&HashedIds, usize> = new_hashed
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
}

/// Check if tuple content or provenance changed (for tuples with same IDs).
fn has_tuple_content_changed(old: &Tuple, new: &Tuple) -> bool {
    if old.provenance() != new.provenance() {
        return true;
    }

    old.iter().zip(new.iter()).any(|(o, n)| match (o, n) {
        (
            TupleElement::Row {
                content: old_content,
                commit_id: old_commit,
                ..
            },
            TupleElement::Row {
                content: new_content,
                commit_id: new_commit,
                ..
            },
        ) => old_content != new_content || old_commit != new_commit,
        _ => false,
    })
}
