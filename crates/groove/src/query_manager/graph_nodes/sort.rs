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
    /// Create a SortNode with RowDescriptor (backward compatible).
    pub fn new(descriptor: RowDescriptor, sort_keys: Vec<SortKey>) -> Self {
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", descriptor.clone(), true);
        Self {
            descriptor,
            output_tuple_descriptor,
            sort_keys,
            sorted_tuples: Vec::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

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
        // Track which tuple IDs are added/removed
        let mut added_ids: AHashSet<_> = input.added.iter().map(|t| t.ids()).collect();
        let mut removed_ids: AHashSet<_> = input.removed.iter().map(|t| t.ids()).collect();
        let updated_old_ids: AHashSet<_> = input.updated.iter().map(|(old, _)| old.ids()).collect();

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
        result.pending = input.pending;

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

    #[test]
    fn sort_ascending() {
        let descriptor = test_descriptor();
        let sort_keys = vec![SortKey {
            col_index: 2, // score
            direction: SortDirection::Ascending,
        }];
        let mut node = SortNode::new(descriptor, sort_keys);

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
            pending: false,
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

    #[test]
    fn sort_descending() {
        let descriptor = test_descriptor();
        let sort_keys = vec![SortKey {
            col_index: 2, // score
            direction: SortDirection::Descending,
        }];
        let mut node = SortNode::new(descriptor, sort_keys);

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
            pending: false,
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
        let mut node = SortNode::new(descriptor.clone(), sort_keys);

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
            pending: false,
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

    #[test]
    fn sort_maintains_order_on_insert() {
        let descriptor = test_descriptor();
        let sort_keys = vec![SortKey {
            col_index: 2,
            direction: SortDirection::Ascending,
        }];
        let mut node = SortNode::new(descriptor, sort_keys);

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
            pending: false,
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
            pending: false,
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        });

        let sorted_ids = get_sorted_ids(&node);
        assert_eq!(sorted_ids[0], id2); // 50 first
        assert_eq!(sorted_ids[1], id1); // 100 second
    }
}
