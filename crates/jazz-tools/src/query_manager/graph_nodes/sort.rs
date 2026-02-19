use ahash::AHashSet;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::query_manager::encoding::compare_column;
use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta, TupleDescriptor};

use super::RowNode;

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Sort specification for a single column.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub col_index: usize,
    pub direction: SortDirection,
}

/// Sort node for ordering rows.
#[derive(Debug)]
pub struct SortNode {
    descriptor: RowDescriptor,
    /// Output tuple descriptor (same as input - pass-through).
    output_tuple_descriptor: TupleDescriptor,
    sort_keys: Vec<SortKey>,
    /// Current sorted tuples.
    sorted_tuples: Vec<Tuple>,
    /// HashSet view of current tuples (for trait requirement).
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl SortNode {
    /// Create a SortNode with TupleDescriptor.
    pub fn with_tuple_descriptor(
        tuple_descriptor: TupleDescriptor,
        sort_keys: Vec<SortKey>,
    ) -> Self {
        let descriptor = tuple_descriptor.combined_descriptor();
        Self {
            descriptor,
            output_tuple_descriptor: tuple_descriptor,
            sort_keys,
            sorted_tuples: Vec::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Compare two tuples by sort keys (assumes single-element tuples).
    fn compare_tuples(&self, a: &Tuple, b: &Tuple) -> Ordering {
        // For single-table queries, compare first element's content
        let a_content = a.get(0).and_then(|e| e.content());
        let b_content = b.get(0).and_then(|e| e.content());

        match (a_content, b_content) {
            (Some(a_data), Some(b_data)) => {
                for key in &self.sort_keys {
                    let ord = compare_column(
                        &self.descriptor,
                        a_data,
                        key.col_index,
                        b_data,
                        key.col_index,
                    )
                    .unwrap_or(Ordering::Equal);

                    let ord = match key.direction {
                        SortDirection::Ascending => ord,
                        SortDirection::Descending => ord.reverse(),
                    };

                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            }
            // Unmaterialized tuples sort to the end
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    /// Find the insertion position for a tuple (binary search).
    fn find_tuple_position(&self, tuple: &Tuple) -> usize {
        self.sorted_tuples
            .binary_search_by(|t| self.compare_tuples(t, tuple))
            .unwrap_or_else(|pos| pos)
    }

    /// Sync current_tuples HashSet from sorted_tuples Vec.
    fn sync_hashset(&mut self) {
        self.current_tuples = self.sorted_tuples.iter().cloned().collect();
    }
}

impl RowNode for SortNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let input_size = input.added.len() + input.removed.len() + input.updated.len();
        let old_sorted = self.sorted_tuples.clone();
        let old_positions: ahash::AHashMap<_, _> = old_sorted
            .iter()
            .enumerate()
            .map(|(idx, tuple)| (tuple.ids(), idx))
            .collect();

        // Track which tuple IDs are added/removed
        let mut added_ids: AHashSet<_> = input.added.iter().map(|t| t.ids()).collect();
        let added_ids_all = added_ids.clone();
        let mut removed_ids: AHashSet<_> = input.removed.iter().map(|t| t.ids()).collect();
        let removed_ids_all = removed_ids.clone();
        let updated_old_ids: AHashSet<_> = input.updated.iter().map(|(old, _)| old.ids()).collect();
        let mut emitted_updated_ids = updated_old_ids.clone();

        // Handle removals - find and remove
        for tuple in &input.removed {
            if let Some(pos) = self.sorted_tuples.iter().position(|t| t == tuple) {
                self.sorted_tuples.remove(pos);
            }
        }

        // Handle additions - insert in sorted position
        for tuple in &input.added {
            let pos = self.find_tuple_position(tuple);
            self.sorted_tuples.insert(pos, tuple.clone());
        }

        // Handle updates - remove old position, insert at new position
        for (old_tuple, new_tuple) in &input.updated {
            if let Some(pos) = self.sorted_tuples.iter().position(|t| t == old_tuple) {
                self.sorted_tuples.remove(pos);
            }
            let pos = self.find_tuple_position(new_tuple);
            self.sorted_tuples.insert(pos, new_tuple.clone());
        }

        // Sync the HashSet
        self.sync_hashset();

        // Build result with tuples in sorted order
        let mut result = TupleDelta::new();

        // Added tuples in sorted order
        for tuple in &self.sorted_tuples {
            if added_ids.remove(&tuple.ids()) {
                result.added.push(tuple.clone());
            }
        }

        // Removed tuples (order doesn't matter as much)
        for tuple in input.removed {
            if removed_ids.remove(&tuple.ids()) {
                result.removed.push(tuple);
            }
        }

        // Updated tuples (find in sorted)
        for (old_tuple, _) in &input.updated {
            if updated_old_ids.contains(&old_tuple.ids())
                && let Some(new_tuple) = self
                    .sorted_tuples
                    .iter()
                    .find(|t| t.ids() == old_tuple.ids())
            {
                result.updated.push((old_tuple.clone(), new_tuple.clone()));
            }
        }

        // Emit identity-preserving updates when tuples shift position because of
        // inserts/removals around them. This encodes move information for downstream
        // nodes that only receive deltas.
        for (new_idx, new_tuple) in self.sorted_tuples.iter().enumerate() {
            let ids = new_tuple.ids();
            if added_ids_all.contains(&ids) || removed_ids_all.contains(&ids) {
                continue;
            }
            if emitted_updated_ids.contains(&ids) {
                continue;
            }
            if let Some(old_idx) = old_positions.get(&ids)
                && *old_idx != new_idx
            {
                let old_tuple = old_sorted[*old_idx].clone();
                result.updated.push((old_tuple, new_tuple.clone()));
                emitted_updated_ids.insert(ids);
            }
        }

        let output_size = result.added.len() + result.removed.len() + result.updated.len();
        tracing::trace!(input_size, output_size, "sort node processed");

        self.dirty = false;
        result
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement, Value};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn make_tuple(id: ObjectId, values: &[Value]) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    fn get_sorted_ids(node: &SortNode) -> Vec<ObjectId> {
        node.sorted_tuples.iter().map(|t| t.ids()[0]).collect()
    }

    fn make_sort_node(sort_keys: Vec<SortKey>) -> SortNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor, true);
        SortNode::with_tuple_descriptor(tuple_desc, sort_keys)
    }

    // Scenario: ascending sort by score.
    //
    // ASCII:
    // input:   [A:100, B:50, C:75]
    // sorted:  [B:50, C:75, A:100]
    #[test]
    fn sort_ascending() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let tuple3 = make_tuple(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2, tuple3],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids.len(), 3);
        assert_eq!(sorted_ids[0], id2); // score 50
        assert_eq!(sorted_ids[1], id3); // score 75
        assert_eq!(sorted_ids[2], id1); // score 100
    }

    // Scenario: descending sort by score.
    //
    // ASCII:
    // input:   [A:100, B:50, C:75]
    // sorted:  [A:100, C:75, B:50]
    #[test]
    fn sort_descending() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score
            direction: SortDirection::Descending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let tuple3 = make_tuple(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2, tuple3],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids.len(), 3);
        assert_eq!(sorted_ids[0], id1); // score 100
        assert_eq!(sorted_ids[1], id3); // score 75
        assert_eq!(sorted_ids[2], id2); // score 50
    }

    // Scenario: multi-key sort (dept asc, score desc).
    //
    // ASCII:
    // dept1: [A:100, B:50]
    // dept2: [D:90,  C:75]
    // final: [A, B, D, C]
    #[test]
    fn sort_multiple_keys() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("dept", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ]);
        let sort_keys = vec![
            SortKey {
                col_index: 0, // dept ascending
                direction: SortDirection::Ascending,
            },
            SortKey {
                col_index: 2, // score descending
                direction: SortDirection::Descending,
            },
        ];
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor.clone(), true);
        let mut node = SortNode::with_tuple_descriptor(tuple_desc, sort_keys);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let id4 = ObjectId::new();

        let make_tuple_local = |id: ObjectId, values: &[Value]| -> Tuple {
            let data = encode_row(&descriptor, values).unwrap();
            Tuple::new(vec![TupleElement::Row {
                id,
                content: data,
                commit_id: CommitId([0; 32]),
            }])
        };

        let tuple1 = make_tuple_local(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let tuple2 = make_tuple_local(
            id2,
            &[
                Value::Integer(1),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let tuple3 = make_tuple_local(
            id3,
            &[
                Value::Integer(2),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );
        let tuple4 = make_tuple_local(
            id4,
            &[
                Value::Integer(2),
                Value::Text("D".into()),
                Value::Integer(90),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1, tuple2, tuple3, tuple4],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids.len(), 4);
        // Dept 1, score desc: 100, 50
        assert_eq!(sorted_ids[0], id1); // dept 1, score 100
        assert_eq!(sorted_ids[1], id2); // dept 1, score 50
        // Dept 2, score desc: 90, 75
        assert_eq!(sorted_ids[2], id4); // dept 2, score 90
        assert_eq!(sorted_ids[3], id3); // dept 2, score 75
    }

    // Scenario: insertion uses sorted position (not append order).
    //
    // ASCII:
    // tick1: [A:100]
    // tick2: +B:50
    // final: [B:50, A:100]
    #[test]
    fn sort_maintains_order_on_insert() {
        let sort_keys = vec![SortKey {
            col_index: 2,
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );

        node.process(TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        });

        // Insert tuple with lower score
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        node.process(TupleDelta {
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        });

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids[0], id2); // 50 first
        assert_eq!(sorted_ids[1], id1); // 100 second
    }

    // Scenario: insert at front shifts existing survivors.
    //
    // ASCII:
    // pre:    [A:10, B:20]
    // delta:  +C:0
    // post:   [C:0, A:10, B:20]
    // moves:  A, B => emitted as identity updates
    #[test]
    fn sort_emits_move_updates_when_insert_shifts_positions() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score asc
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let a = make_tuple(
            id_a,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(10),
            ],
        );
        let b = make_tuple(
            id_b,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(20),
            ],
        );
        let c = make_tuple(
            id_c,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(0),
            ],
        );

        // Seed: [A, B]
        node.process(TupleDelta {
            added: vec![a.clone(), b.clone()],
            removed: vec![],
            updated: vec![],
        });

        // Insert C at front => [C, A, B]. A and B should be emitted as move updates.
        let delta = node.process(TupleDelta {
            added: vec![c],
            removed: vec![],
            updated: vec![],
        });

        assert_eq!(delta.added.len(), 1);
        assert_eq!(delta.added[0].ids()[0], id_c);
        assert_eq!(delta.updated.len(), 2);
        assert_eq!(delta.updated[0].0.ids()[0], id_a);
        assert_eq!(delta.updated[0].1.ids()[0], id_a);
        assert_eq!(delta.updated[1].0.ids()[0], id_b);
        assert_eq!(delta.updated[1].1.ids()[0], id_b);
    }

    // Scenario: removing first row shifts remaining survivors left.
    //
    // ASCII:
    // pre:    [A:10, B:20, C:30]
    // delta:  -A
    // post:   [B:20, C:30]
    // moves:  B, C => emitted as identity updates
    #[test]
    fn sort_emits_move_updates_when_remove_shifts_positions() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score asc
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let a = make_tuple(
            id_a,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(10),
            ],
        );
        let b = make_tuple(
            id_b,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(20),
            ],
        );
        let c = make_tuple(
            id_c,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(30),
            ],
        );

        // Seed: [A, B, C]
        node.process(TupleDelta {
            added: vec![a.clone(), b.clone(), c.clone()],
            removed: vec![],
            updated: vec![],
        });

        // Remove A => [B, C]. B and C must be emitted as move updates.
        let delta = node.process(TupleDelta {
            added: vec![],
            removed: vec![a],
            updated: vec![],
        });

        assert_eq!(delta.removed.len(), 1);
        assert_eq!(delta.removed[0].ids()[0], id_a);
        assert_eq!(delta.updated.len(), 2);
        assert_eq!(delta.updated[0].0.ids()[0], id_b);
        assert_eq!(delta.updated[0].1.ids()[0], id_b);
        assert_eq!(delta.updated[1].0.ids()[0], id_c);
        assert_eq!(delta.updated[1].1.ids()[0], id_c);
    }

    // Scenario: explicit update should not be duplicated by move-emitter.
    //
    // ASCII:
    // pre:    [A:10, B:20]
    // delta:  upd(B:20 -> B:25)
    // post:   [A:10, B:25]
    // expect: one updated entry for B only
    #[test]
    fn sort_does_not_emit_extra_move_update_for_explicit_updated_row() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score asc
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let a = make_tuple(
            id_a,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(10),
            ],
        );
        let b_old = make_tuple(
            id_b,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(20),
            ],
        );
        let b_new = make_tuple(
            id_b,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(25),
            ],
        );

        // Seed: [A, B]
        node.process(TupleDelta {
            added: vec![a, b_old.clone()],
            removed: vec![],
            updated: vec![],
        });

        // Explicit update for B, no position shift.
        let delta = node.process(TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(b_old, b_new)],
        });

        // Should emit exactly the explicit update (no duplicate move update for same id).
        assert_eq!(delta.updated.len(), 1);
        assert_eq!(delta.updated[0].0.ids()[0], id_b);
        assert_eq!(delta.updated[0].1.ids()[0], id_b);
    }

    // Scenario: mixed add/remove emits moves only for shifted survivors.
    //
    // ASCII:
    // pre:    [A:10, B:20, C:30]
    // delta:  -B, +D:5
    // post:   [D:5, A:10, C:30]
    // moves:  A (shifted), not C (same index), not D/B (added/removed)
    #[test]
    fn sort_mixed_add_remove_emits_moves_for_shifted_survivors_only() {
        let sort_keys = vec![SortKey {
            col_index: 2, // score asc
            direction: SortDirection::Ascending,
        }];
        let mut node = make_sort_node(sort_keys);

        let id_a = ObjectId::new();
        let id_b = ObjectId::new();
        let id_c = ObjectId::new();
        let id_d = ObjectId::new();
        let a = make_tuple(
            id_a,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(10),
            ],
        );
        let b = make_tuple(
            id_b,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(20),
            ],
        );
        let c = make_tuple(
            id_c,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(30),
            ],
        );
        let d = make_tuple(
            id_d,
            &[
                Value::Integer(4),
                Value::Text("D".into()),
                Value::Integer(5),
            ],
        );

        // Seed: [A, B, C]
        node.process(TupleDelta {
            added: vec![a.clone(), b.clone(), c.clone()],
            removed: vec![],
            updated: vec![],
        });

        // Remove B, add D(5) => [D, A, C].
        // A shifts from index 0 -> 1 and should be emitted as move update.
        let delta = node.process(TupleDelta {
            added: vec![d],
            removed: vec![b],
            updated: vec![],
        });

        assert_eq!(delta.added.len(), 1);
        assert_eq!(delta.added[0].ids()[0], id_d);
        assert_eq!(delta.removed.len(), 1);
        assert_eq!(delta.removed[0].ids()[0], id_b);
        assert_eq!(delta.updated.len(), 1);
        assert_eq!(delta.updated[0].0.ids()[0], id_a);
        assert_eq!(delta.updated[0].1.ids()[0], id_a);
    }
}
