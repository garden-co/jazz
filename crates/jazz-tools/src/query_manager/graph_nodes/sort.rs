use ahash::AHashSet;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::object::ObjectId;
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
    pub target: SortTarget,
    pub direction: SortDirection,
}

/// Field used by a sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortTarget {
    Column(usize),
    /// Virtual sort key for object identity (`id`/`_id`).
    ///
    /// This is needed because object ID is not part of row payload columns,
    /// but query semantics allow `ORDER BY id|_id` (including desc and mixed keys).
    RowId,
}

/// Threshold: when adding more than this many tuples, use bulk append + sort
/// instead of individual binary-search inserts.
const BULK_ADD_THRESHOLD: usize = 16;

/// Compare two tuples by sort keys without borrowing self.
///
/// Extracted as a free function so it can be used inside `sort_unstable_by`
/// without conflicting borrows on `SortNode` fields.
fn compare_tuples_with(
    sort_keys: &[SortKey],
    descriptor: &RowDescriptor,
    a: &Tuple,
    b: &Tuple,
) -> Ordering {
    let a_content = a.get(0).and_then(|e| e.content());
    let b_content = b.get(0).and_then(|e| e.content());

    for key in sort_keys {
        let ord = match key.target {
            SortTarget::Column(col_index) => match (a_content, b_content) {
                (Some(a_data), Some(b_data)) => {
                    compare_column(descriptor, a_data, col_index, b_data, col_index)
                        .unwrap_or(Ordering::Equal)
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            SortTarget::RowId => compare_all_ids(a, b),
        };

        let ord = match key.direction {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        };

        if ord != Ordering::Equal {
            return ord;
        }
    }

    // Stable tie-breaker for deterministic ordering.
    compare_all_ids(a, b)
}

/// Compare tuples by all element IDs lexicographically, without allocating.
///
/// This is the zero-alloc equivalent of `a.ids().cmp(&b.ids())`. It must compare
/// *all* element IDs (not just the first) to produce a deterministic total ordering
/// for joined tuples where multiple rows share the same first_id.
#[inline]
fn compare_all_ids(a: &Tuple, b: &Tuple) -> Ordering {
    for (ea, eb) in a.iter().zip(b.iter()) {
        let ord = ea.id().cmp(&eb.id());
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
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

    /// Find the insertion position for a tuple (binary search).
    fn find_tuple_position(&self, tuple: &Tuple) -> usize {
        let sort_keys = &self.sort_keys;
        let descriptor = &self.descriptor;
        self.sorted_tuples
            .binary_search_by(|t| compare_tuples_with(sort_keys, descriptor, t, tuple))
            .unwrap_or_else(|pos| pos)
    }

    /// Full current ordering after sort has been applied.
    pub fn sorted_tuples(&self) -> &[Tuple] {
        &self.sorted_tuples
    }
}

impl RowNode for SortNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Use full tuple IDs (all elements) for identity tracking.
        // `ids()` allocates a Vec<ObjectId> per call, but this only happens once per
        // changed tuple — not in the sort comparison hot path — so the cost is O(k).
        // Using first_id() here would be incorrect for joined tuples where multiple
        // rows share the same first element ID.
        let removed_id_set: AHashSet<Vec<ObjectId>> = input
            .removed
            .iter()
            .chain(input.updated.iter().map(|(old, _)| old))
            .map(|t| t.ids())
            .collect();

        let added_id_set: AHashSet<Vec<ObjectId>> = input.added.iter().map(|t| t.ids()).collect();

        // --- Phase 1: Removals (single retain pass instead of k linear scans) ---
        if !removed_id_set.is_empty() {
            // Incremental hashset: remove entries before modifying the vec.
            for tuple in &input.removed {
                self.current_tuples.remove(tuple);
            }
            for (old, _) in &input.updated {
                self.current_tuples.remove(old);
            }
            // Tuple PartialEq is ID-based, so retain uses the same identity semantics.
            self.sorted_tuples
                .retain(|t| !removed_id_set.contains(&t.ids()));
        }

        // --- Phase 2: Additions ---
        let new_count = input.added.len() + input.updated.len();
        let use_bulk = self.sorted_tuples.is_empty() || new_count > BULK_ADD_THRESHOLD;

        if new_count > 0 {
            if use_bulk {
                // Bulk path: append all, then sort once — O(n log n) instead of O(n²) memmoves.
                for tuple in input
                    .added
                    .iter()
                    .chain(input.updated.iter().map(|(_, new)| new))
                {
                    self.current_tuples.insert(tuple.clone());
                    self.sorted_tuples.push(tuple.clone());
                }
                let sort_keys = &self.sort_keys;
                let descriptor = &self.descriptor;
                self.sorted_tuples
                    .sort_unstable_by(|a, b| compare_tuples_with(sort_keys, descriptor, a, b));
            } else {
                // Incremental path: binary search + insert for small batches.
                for tuple in input
                    .added
                    .iter()
                    .chain(input.updated.iter().map(|(_, new)| new))
                {
                    self.current_tuples.insert(tuple.clone());
                    let pos = self.find_tuple_position(tuple);
                    self.sorted_tuples.insert(pos, tuple.clone());
                }
            }
        }

        // --- Phase 3: Build result delta ---
        let mut result = TupleDelta::new();

        // Added tuples in sorted order (scan sorted_tuples, match against full IDs).
        if !added_id_set.is_empty() {
            let mut remaining = added_id_set;
            for tuple in &self.sorted_tuples {
                if remaining.is_empty() {
                    break;
                }
                if remaining.remove(&tuple.ids()) {
                    result.added.push(tuple.clone());
                }
            }
        }

        // Removed: move from input (no clone).
        result.removed = input.removed;

        // Updated: old from input (moved), new found by tuple equality in sorted_tuples.
        for (old_tuple, _) in input.updated {
            // Tuple equality is ID-based, so find() matches on all element IDs.
            if let Some(new_tuple) = self.sorted_tuples.iter().find(|t| *t == &old_tuple) {
                result.updated.push((old_tuple, new_tuple.clone()));
            }
        }

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
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
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
            target: SortTarget::Column(2), // score
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
            moved: vec![],
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
            target: SortTarget::Column(2), // score
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
            moved: vec![],
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
                target: SortTarget::Column(0), // dept ascending
                direction: SortDirection::Ascending,
            },
            SortKey {
                target: SortTarget::Column(2), // score descending
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
                row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
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
            moved: vec![],
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
            target: SortTarget::Column(2),
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
            moved: vec![],
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
            moved: vec![],
            updated: vec![],
        });

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids[0], id2); // 50 first
        assert_eq!(sorted_ids[1], id1); // 100 second
    }

    #[test]
    fn sort_by_row_id() {
        let sort_keys = vec![SortKey {
            target: SortTarget::RowId,
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
                Value::Integer(5),
            ],
        );
        let tuple2 = make_tuple(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(5),
            ],
        );
        let tuple3 = make_tuple(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(5),
            ],
        );

        node.process(TupleDelta {
            added: vec![tuple3, tuple1, tuple2],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        let sorted_ids = get_sorted_ids(&node);
        let mut expected = vec![id1, id2, id3];
        expected.sort();
        assert_eq!(sorted_ids, expected);
    }
}
