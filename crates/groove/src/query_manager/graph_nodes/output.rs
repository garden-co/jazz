use ahash::AHashSet;
use std::collections::{HashMap, HashSet};

use crate::query_manager::encoding::decode_row;
use crate::query_manager::types::{
    Row, RowDelta, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, Value,
};

use super::RowNode;

/// Output mode for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Emit deltas as they happen.
    Delta,
    /// Emit full result set on each change.
    Full,
}

/// Query subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuerySubscriptionId(pub u64);

/// Output node - terminal node that delivers results to subscribers.
///
/// Materializes any remaining unmaterialized elements and flattens
/// multi-element tuples to single Row outputs.
#[derive(Debug)]
pub struct OutputNode {
    descriptor: RowDescriptor,
    /// Output tuple descriptor (always fully materialized).
    output_tuple_descriptor: TupleDescriptor,
    mode: OutputMode,
    /// Current result tuples (for RowNode trait).
    current_tuples: AHashSet<Tuple>,
    /// Ordered tuples for deterministic output (preserves sort order).
    ordered_tuples: Vec<Tuple>,
    /// Pending tuple deltas to deliver.
    pending_tuple_deltas: Vec<TupleDelta>,
    /// True if subscriber has received initial snapshot.
    subscriber_initialized: bool,
    dirty: bool,
}

impl OutputNode {
    /// Create an OutputNode with TupleDescriptor.
    /// The output descriptor is always fully materialized.
    pub fn with_tuple_descriptor(tuple_descriptor: TupleDescriptor, mode: OutputMode) -> Self {
        let descriptor = tuple_descriptor.combined_descriptor();
        // Output is always fully materialized
        let output_tuple_descriptor = tuple_descriptor.clone().with_all_materialized();
        Self {
            descriptor,
            output_tuple_descriptor,
            mode,
            current_tuples: AHashSet::new(),
            ordered_tuples: Vec::new(),
            pending_tuple_deltas: Vec::new(),
            subscriber_initialized: false,
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Get the output mode.
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Take pending tuple deltas (for delta mode).
    pub fn take_tuple_deltas(&mut self) -> Vec<TupleDelta> {
        std::mem::take(&mut self.pending_tuple_deltas)
    }

    /// Get current result as rows (extracts from tuples).
    /// For single-table queries, converts length-1 tuples to rows.
    /// Returns rows in insertion order (preserves sort order from upstream nodes).
    pub fn current_rows(&self) -> Vec<Row> {
        self.ordered_tuples
            .iter()
            .filter_map(|t| t.to_single_row())
            .collect()
    }

    /// Decode current rows to Values (for output to user).
    pub fn decode_current(&self) -> Vec<Vec<Value>> {
        self.current_rows()
            .iter()
            .filter_map(|row| decode_row(&self.descriptor, &row.data).ok())
            .collect()
    }

    /// Decode a delta to Values.
    pub fn decode_delta(&self, delta: &RowDelta) -> DecodedDelta {
        DecodedDelta {
            added: delta
                .added
                .iter()
                .filter_map(|row| {
                    decode_row(&self.descriptor, &row.data)
                        .ok()
                        .map(|v| (row.id, v))
                })
                .collect(),
            removed: delta
                .removed
                .iter()
                .filter_map(|row| {
                    decode_row(&self.descriptor, &row.data)
                        .ok()
                        .map(|v| (row.id, v))
                })
                .collect(),
            updated: delta
                .updated
                .iter()
                .filter_map(|(old, new)| {
                    let old_v = decode_row(&self.descriptor, &old.data).ok()?;
                    let new_v = decode_row(&self.descriptor, &new.data).ok()?;
                    Some((old.id, old_v, new_v))
                })
                .collect(),
        }
    }
}

/// Decoded delta with Values instead of binary.
#[derive(Debug, Clone)]
pub struct DecodedDelta {
    pub added: Vec<(crate::object::ObjectId, Vec<Value>)>,
    pub removed: Vec<(crate::object::ObjectId, Vec<Value>)>,
    pub updated: Vec<(crate::object::ObjectId, Vec<Value>, Vec<Value>)>,
}

/// Indexed state derived from a row delta and previous output order.
///
/// This is used by adapters (e.g. wasm) to build stable index-based deltas
/// without re-implementing output ordering logic.
#[derive(Debug, Clone)]
pub struct IndexedRowState {
    pub pre_index_by_id: HashMap<crate::object::ObjectId, usize>,
    pub post_index_by_id: HashMap<crate::object::ObjectId, usize>,
    pub post_ids: Vec<crate::object::ObjectId>,
}

/// Compute pre/post index maps for a `RowDelta`, given the prior ordered ids.
///
/// Ordering rules:
/// - start from prior order and detach removed + updated-old ids
/// - append `added` ids (stream order)
/// - append `updated.new` ids (stream order, enables moves)
pub fn index_row_delta(
    current_ids: &[crate::object::ObjectId],
    delta: &RowDelta,
) -> IndexedRowState {
    // ASCII flow:
    // pre:    [A, B, C]
    // detach:    ^B
    // base:   [A, C]
    // +added: [A, C, N]
    // +upd:   [A, C, N, B]

    let pre_index_by_id: HashMap<_, _> = current_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();

    let mut ids_to_detach = HashSet::new();
    for row in &delta.removed {
        ids_to_detach.insert(row.id);
    }
    for (old, _) in &delta.updated {
        ids_to_detach.insert(old.id);
    }

    let mut post_ids = Vec::with_capacity(current_ids.len() + delta.added.len());
    let mut post_index_by_id = HashMap::new();

    for id in current_ids {
        if !ids_to_detach.contains(id) {
            post_index_by_id.insert(*id, post_ids.len());
            post_ids.push(*id);
        }
    }

    let mut append_if_missing = |id: crate::object::ObjectId| {
        if let std::collections::hash_map::Entry::Vacant(entry) = post_index_by_id.entry(id) {
            entry.insert(post_ids.len());
            post_ids.push(id);
        }
    };

    for row in &delta.added {
        append_if_missing(row.id);
    }

    for (_, new) in &delta.updated {
        append_if_missing(new.id);
    }

    IndexedRowState {
        pre_index_by_id,
        post_index_by_id,
        post_ids,
    }
}

impl RowNode for OutputNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Build next ordered state in three phases:
        // 1) detach removed + updated-old ids from pre-state,
        // 2) reinsert updated-new tuples in stream order (enables moves),
        // 3) append added tuples in stream order.
        let mut detached = AHashSet::new();
        for tuple in &input.removed {
            detached.insert(tuple.ids());
        }
        for (old_tuple, _) in &input.updated {
            detached.insert(old_tuple.ids());
        }

        let mut next_ordered: Vec<Tuple> = self
            .ordered_tuples
            .iter()
            .filter(|t| !detached.contains(&t.ids()))
            .cloned()
            .collect();
        let mut next_ids: AHashSet<_> = next_ordered.iter().map(|t| t.ids()).collect();

        for tuple in &input.added {
            let ids = tuple.ids();
            if !next_ids.contains(&ids) {
                next_ids.insert(ids);
                next_ordered.push(tuple.clone());
            }
        }

        for (_, new_tuple) in &input.updated {
            let ids = new_tuple.ids();
            if !next_ids.contains(&ids) {
                next_ids.insert(ids);
                next_ordered.push(new_tuple.clone());
            }
        }

        self.ordered_tuples = next_ordered;
        self.current_tuples = self.ordered_tuples.iter().cloned().collect();

        self.dirty = false;

        // Deliver immediately
        self.subscriber_initialized = true;
        if !input.is_empty() {
            self.pending_tuple_deltas.push(input.clone());
        }

        input
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_tuple(id: ObjectId, n: i32, name: &str) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, &[Value::Integer(n), Value::Text(name.into())]).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    fn make_output_node(mode: OutputMode) -> OutputNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor, true);
        OutputNode::with_tuple_descriptor(tuple_desc, mode)
    }

    fn make_row(id: ObjectId) -> Row {
        Row {
            id,
            data: vec![],
            commit_id: CommitId([0; 32]),
        }
    }

    fn ordered_ids(node: &OutputNode) -> Vec<ObjectId> {
        node.ordered_tuples.iter().map(|t| t.ids()[0]).collect()
    }

    fn assert_indexed_invariants(
        pre_ids: &[ObjectId],
        delta: &RowDelta,
        indexed: &IndexedRowState,
    ) {
        // pre_index_by_id must match pre_ids exactly
        for (idx, id) in pre_ids.iter().enumerate() {
            assert_eq!(indexed.pre_index_by_id.get(id), Some(&idx));
        }

        // post_index_by_id must be a perfect index map of post_ids
        for (idx, id) in indexed.post_ids.iter().enumerate() {
            assert_eq!(indexed.post_index_by_id.get(id), Some(&idx));
        }
        assert_eq!(indexed.post_index_by_id.len(), indexed.post_ids.len());

        // post_ids must be unique
        let unique: std::collections::HashSet<_> = indexed.post_ids.iter().copied().collect();
        assert_eq!(unique.len(), indexed.post_ids.len());

        // Length sanity: post = survivors + inserted_unique
        let mut detached = std::collections::HashSet::new();
        for row in &delta.removed {
            detached.insert(row.id);
        }
        for (old, _) in &delta.updated {
            detached.insert(old.id);
        }
        let survivors: Vec<_> = pre_ids
            .iter()
            .copied()
            .filter(|id| !detached.contains(id))
            .collect();

        let mut seen: std::collections::HashSet<_> = survivors.iter().copied().collect();
        let mut inserted_unique = 0usize;
        for id in delta
            .added
            .iter()
            .map(|r| r.id)
            .chain(delta.updated.iter().map(|(_, new)| new.id))
        {
            if seen.insert(id) {
                inserted_unique += 1;
            }
        }

        assert_eq!(indexed.post_ids.len(), survivors.len() + inserted_unique);
    }

    // Scenario: output node stores any non-empty delta for subscribers.
    //
    // ASCII:
    // input_delta:  +[Alice]
    // pending:      [delta1]
    #[test]
    fn output_stores_deltas() {
        let mut node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);
    }

    // Scenario: current rows decode to user values.
    //
    // ASCII:
    // tuples:   [id1 -> (1, "Alice")]
    // decoded:  [[1, "Alice"]]
    #[test]
    fn output_decodes_current() {
        let mut node = make_output_node(OutputMode::Full);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        node.process(TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        });

        let decoded = node.decode_current();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0][0], Value::Integer(1));
        assert_eq!(decoded[0][1], Value::Text("Alice".into()));
    }

    // Scenario: decode_delta maps row bytes to typed value vectors.
    //
    // ASCII:
    // row_delta.added: [id1]
    // decoded.added:   [(id1, [1, "Alice"])]
    #[test]
    fn output_decodes_delta() {
        let node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let row1 = Row::new(
            id1,
            encode_row(
                &test_descriptor(),
                &[Value::Integer(1), Value::Text("Alice".into())],
            )
            .unwrap(),
            CommitId([0; 32]),
        );

        let delta = RowDelta {
            added: vec![row1],
            removed: vec![],
            updated: vec![],
        };

        let decoded = node.decode_delta(&delta);
        assert_eq!(decoded.added.len(), 1);
        assert_eq!(decoded.added[0].0, id1);
        assert_eq!(decoded.added[0].1[0], Value::Integer(1));
        assert_eq!(decoded.added[0].1[1], Value::Text("Alice".into()));
    }

    // Scenario: empty deltas are not buffered for delivery.
    //
    // ASCII:
    // input_delta:   empty
    // pending_queue: []
    #[test]
    fn empty_delta_not_stored() {
        let mut node = make_output_node(OutputMode::Delta);

        let delta = TupleDelta::new();
        node.process(delta);

        let deltas = node.take_tuple_deltas();
        assert!(deltas.is_empty());
    }

    // Scenario: each non-empty process call is delivered immediately.
    //
    // ASCII:
    // tick1: +A -> deliver [deltaA]
    // tick2: +B -> deliver [deltaB]
    #[test]
    fn output_delivers_immediately() {
        let mut node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");
        let tuple2 = make_tuple(id2, 2, "Bob");

        // First delta
        let delta1 = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);

        // Should deliver immediately
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);

        // Second delta
        let delta2 = TupleDelta {
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);

        // Should also deliver immediately
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);
    }

    // Scenario: append add keeps existing order and places new row at tail.
    //
    // ASCII:
    // pre:   [A, B]
    // delta: +C
    // post:  [A, B, C]
    #[test]
    fn index_row_delta_append_add_uses_tail_index() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let delta = RowDelta {
            added: vec![make_row(id_c)],
            removed: vec![],
            updated: vec![],
        };

        let indexed = index_row_delta(&[id_a, id_b], &delta);
        assert_indexed_invariants(&[id_a, id_b], &delta, &indexed);
        assert_eq!(indexed.post_ids, vec![id_a, id_b, id_c]);
        assert_eq!(indexed.post_index_by_id.get(&id_c), Some(&2));
    }

    // Scenario: middle insert expressed as "add + move-updates".
    //
    // ASCII:
    // pre:   [A, B, C]
    // delta: +X, upd(B->B), upd(C->C)
    // post:  [A, X, B, C]
    #[test]
    fn index_row_delta_middle_insert_via_shift_updates() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let id_x = ObjectId::new();

        // pre [A, B, C], add X, move B/C => post [A, X, B, C]
        let delta = RowDelta {
            added: vec![make_row(id_x)],
            removed: vec![],
            updated: vec![
                (make_row(id_b), make_row(id_b)),
                (make_row(id_c), make_row(id_c)),
            ],
        };

        let indexed = index_row_delta(&[id_a, id_b, id_c], &delta);
        assert_indexed_invariants(&[id_a, id_b, id_c], &delta, &indexed);
        assert_eq!(indexed.post_ids, vec![id_a, id_x, id_b, id_c]);
        assert_eq!(indexed.pre_index_by_id.get(&id_b), Some(&1));
        assert_eq!(indexed.post_index_by_id.get(&id_b), Some(&2));
    }

    // Scenario: removing first row preserves pre-index semantics.
    //
    // ASCII:
    // pre:   [A, B, C]
    // delta: -A
    // post:  [B, C]
    #[test]
    fn index_row_delta_remove_first_preserves_pre_indices() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let delta = RowDelta {
            added: vec![],
            removed: vec![make_row(id_a)],
            updated: vec![],
        };

        let indexed = index_row_delta(&[id_a, id_b, id_c], &delta);
        assert_indexed_invariants(&[id_a, id_b, id_c], &delta, &indexed);
        assert_eq!(indexed.pre_index_by_id.get(&id_a), Some(&0));
        assert_eq!(indexed.post_ids, vec![id_b, id_c]);
    }

    // Scenario: identity-preserving update can still represent a move.
    //
    // ASCII:
    // pre:   [A, B, C]
    // delta: upd(B->B)
    // post:  [A, C, B]
    #[test]
    fn index_row_delta_identity_preserving_update_moves_row() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();

        // pre [A, B, C], update B->B => post [A, C, B]
        let delta = RowDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(make_row(id_b), make_row(id_b))],
        };

        let indexed = index_row_delta(&[id_a, id_b, id_c], &delta);
        assert_indexed_invariants(&[id_a, id_b, id_c], &delta, &indexed);
        assert_eq!(indexed.pre_index_by_id.get(&id_b), Some(&1));
        assert_eq!(indexed.post_index_by_id.get(&id_b), Some(&2));
        assert_eq!(indexed.post_ids, vec![id_a, id_c, id_b]);
    }

    // Scenario: identity change behaves like remove old + add new.
    //
    // ASCII:
    // pre:   [A, B]
    // delta: upd(B->N)
    // post:  [A, N]
    #[test]
    fn index_row_delta_identity_change_behaves_like_remove_add() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_new = ObjectId::new();

        let delta = RowDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(make_row(id_b), make_row(id_new))],
        };

        let indexed = index_row_delta(&[id_a, id_b], &delta);
        assert_indexed_invariants(&[id_a, id_b], &delta, &indexed);
        assert_eq!(indexed.pre_index_by_id.get(&id_b), Some(&1));
        assert_eq!(indexed.post_index_by_id.get(&id_new), Some(&1));
        assert_eq!(indexed.post_ids, vec![id_a, id_new]);
    }

    // Scenario: mixed batch keeps deterministic final order.
    //
    // ASCII:
    // pre:   [A, B, C]
    // delta: -B, +D, upd(C->C)
    // post:  [A, D, C]
    #[test]
    fn index_row_delta_mixed_batch_is_deterministic() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let id_d = ObjectId::new();

        // pre [A, B, C], remove B, add D, move C => post [A, D, C]
        let delta = RowDelta {
            added: vec![make_row(id_d)],
            removed: vec![make_row(id_b)],
            updated: vec![(make_row(id_c), make_row(id_c))],
        };

        let indexed = index_row_delta(&[id_a, id_b, id_c], &delta);
        assert_indexed_invariants(&[id_a, id_b, id_c], &delta, &indexed);
        assert_eq!(indexed.post_ids, vec![id_a, id_d, id_c]);
    }

    // Scenario: duplicate ids are deduped; first insertion point wins.
    //
    // ASCII:
    // pre:   [A, B]
    // delta: +X, +X, upd(B->X)
    // post:  [A, X]
    #[test]
    fn index_row_delta_dedupes_duplicate_ids_first_occurrence_wins() {
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_x = ObjectId::new();

        // Duplicated X across added and updated-new should appear once, positioned
        // at first insertion opportunity (added stream comes before updated stream).
        let delta = RowDelta {
            added: vec![make_row(id_x), make_row(id_x)],
            removed: vec![],
            updated: vec![(make_row(id_b), make_row(id_x))],
        };

        let indexed = index_row_delta(&[id_a, id_b], &delta);
        assert_indexed_invariants(&[id_a, id_b], &delta, &indexed);
        assert_eq!(indexed.post_ids, vec![id_a, id_x]);
        assert_eq!(indexed.post_index_by_id.get(&id_x), Some(&1));
    }

    // Scenario: output node applies move updates for stable identity rows.
    //
    // ASCII:
    // pre:   [A, B]
    // delta: +C, upd(A->A), upd(B->B)
    // post:  [C, A, B]
    #[test]
    fn output_applies_identity_updates_as_moves() {
        let mut node = make_output_node(OutputMode::Delta);

        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let a = make_tuple(id_a, 1, "A");
        let b = make_tuple(id_b, 2, "B");
        let c = make_tuple(id_c, 0, "C");

        // Seed [A, B]
        node.process(TupleDelta {
            added: vec![a.clone(), b.clone()],
            removed: vec![],
            updated: vec![],
        });
        assert_eq!(ordered_ids(&node), vec![id_a, id_b]);

        // Represent reorder to [C, A, B] via add C + move A/B.
        node.process(TupleDelta {
            added: vec![c],
            removed: vec![],
            updated: vec![(a.clone(), a), (b.clone(), b)],
        });

        assert_eq!(ordered_ids(&node), vec![id_c, id_a, id_b]);
    }

    // Scenario: remove-only keeps survivors in original relative order.
    //
    // ASCII:
    // pre:   [A, B, C]
    // delta: -B
    // post:  [A, C]
    #[test]
    fn output_remove_only_keeps_survivor_relative_order() {
        let mut node = make_output_node(OutputMode::Delta);
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let a = make_tuple(id_a, 1, "A");
        let b = make_tuple(id_b, 2, "B");
        let c = make_tuple(id_c, 3, "C");

        node.process(TupleDelta {
            added: vec![a, b.clone(), c],
            removed: vec![],
            updated: vec![],
        });
        node.process(TupleDelta {
            added: vec![],
            removed: vec![b],
            updated: vec![],
        });

        assert_eq!(ordered_ids(&node), vec![id_a, id_c]);
    }

    // Scenario: identity-changing update swaps id after detach/reinsert.
    //
    // ASCII:
    // pre:   [A, B]
    // delta: upd(B->C)
    // post:  [A, C]
    #[test]
    fn output_identity_change_update_replaces_id_in_place_after_detach() {
        let mut node = make_output_node(OutputMode::Delta);
        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let a = make_tuple(id_a, 1, "A");
        let b = make_tuple(id_b, 2, "B");
        let c = make_tuple(id_c, 2, "C");

        node.process(TupleDelta {
            added: vec![a.clone(), b.clone()],
            removed: vec![],
            updated: vec![],
        });
        node.process(TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(b, c)],
        });

        assert_eq!(ordered_ids(&node), vec![id_a, id_c]);
    }

    // Scenario: repeated updates for same id do not duplicate rows.
    //
    // ASCII:
    // pre:   [A]
    // delta: upd(A1->A2), upd(A3->A3)
    // post:  [A]
    #[test]
    fn output_repeated_updates_same_id_in_one_delta_do_not_duplicate() {
        let mut node = make_output_node(OutputMode::Delta);
        let id_a = ObjectId::new();
        let a_v1 = make_tuple(id_a, 1, "A1");
        let a_v2 = make_tuple(id_a, 2, "A2");
        let a_v3 = make_tuple(id_a, 3, "A3");

        node.process(TupleDelta {
            added: vec![a_v1.clone()],
            removed: vec![],
            updated: vec![],
        });
        node.process(TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(a_v1, a_v2), (a_v3.clone(), a_v3)],
        });

        assert_eq!(ordered_ids(&node), vec![id_a]);
    }

    // Scenario: sequential add then remove of same id returns to empty state.
    //
    // ASCII:
    // tick1: [] +A -> [A]
    // tick2: [A] -A -> []
    #[test]
    fn output_add_then_remove_same_id_across_ticks_is_stable() {
        let mut node = make_output_node(OutputMode::Delta);
        let id_a = ObjectId::new();
        let a = make_tuple(id_a, 1, "A");

        node.process(TupleDelta {
            added: vec![a.clone()],
            removed: vec![],
            updated: vec![],
        });
        assert_eq!(ordered_ids(&node), vec![id_a]);

        node.process(TupleDelta {
            added: vec![],
            removed: vec![a],
            updated: vec![],
        });
        assert!(ordered_ids(&node).is_empty());
    }
}
