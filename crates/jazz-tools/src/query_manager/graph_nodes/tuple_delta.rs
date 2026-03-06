use std::collections::HashMap;

use crate::query_manager::types::{Tuple, TupleDelta, TupleElement};

pub(crate) fn compute_tuple_delta(old_tuples: &[Tuple], new_tuples: &[Tuple]) -> TupleDelta {
    let mut delta = TupleDelta::new();
    let old_ids: Vec<_> = old_tuples.iter().map(|t| t.ids()).collect();
    let new_ids: Vec<_> = new_tuples.iter().map(|t| t.ids()).collect();
    let old_pos_by_ids: HashMap<_, _> = old_ids
        .iter()
        .enumerate()
        .map(|(idx, ids)| (ids.clone(), idx))
        .collect();
    let new_pos_by_ids: HashMap<_, _> = new_ids
        .iter()
        .enumerate()
        .map(|(idx, ids)| (ids.clone(), idx))
        .collect();

    for (old, ids) in old_tuples.iter().zip(old_ids.iter()) {
        if !new_pos_by_ids.contains_key(ids) {
            delta.removed.push(old.clone());
        }
    }

    for (new, ids) in new_tuples.iter().zip(new_ids.iter()) {
        if !old_pos_by_ids.contains_key(ids) {
            delta.added.push(new.clone());
        }
    }

    let new_to_old_idx_mapping: Vec<_> = new_ids
        .iter()
        .enumerate()
        .filter_map(|(new_idx, ids)| {
            old_pos_by_ids
                .get(ids)
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

    for (old, ids) in old_tuples.iter().zip(old_ids.iter()) {
        if let Some(new_idx) = new_pos_by_ids.get(ids)
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
