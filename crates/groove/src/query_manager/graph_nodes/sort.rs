use std::cmp::Ordering;

use crate::query_manager::encoding::compare_column;
use crate::query_manager::types::{Row, RowDelta, RowDescriptor};

use super::RowNode;

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    sort_keys: Vec<SortKey>,
    /// Current sorted rows.
    current_rows: Vec<Row>,
    dirty: bool,
}

impl SortNode {
    pub fn new(descriptor: RowDescriptor, sort_keys: Vec<SortKey>) -> Self {
        Self {
            descriptor,
            sort_keys,
            current_rows: Vec::new(),
            dirty: true,
        }
    }

    /// Compare two rows by sort keys.
    fn compare_rows(&self, a: &Row, b: &Row) -> Ordering {
        for key in &self.sort_keys {
            let ord = compare_column(
                &self.descriptor,
                &a.data,
                key.col_index,
                &b.data,
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

    /// Find the insertion position for a row (binary search).
    fn find_position(&self, row: &Row) -> usize {
        self.current_rows
            .binary_search_by(|r| self.compare_rows(r, row))
            .unwrap_or_else(|pos| pos)
    }
}

impl RowNode for SortNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: RowDelta) -> RowDelta {
        // Track which IDs are added/removed for reconstructing a sorted delta
        let mut added_ids: std::collections::HashSet<_> =
            input.added.iter().map(|r| r.id).collect();
        let mut removed_ids: std::collections::HashSet<_> =
            input.removed.iter().map(|r| r.id).collect();
        let updated_old_ids: std::collections::HashSet<_> =
            input.updated.iter().map(|(old, _)| old.id).collect();

        // Handle removals - find and remove
        for row in &input.removed {
            if let Some(pos) = self.current_rows.iter().position(|r| r.id == row.id) {
                self.current_rows.remove(pos);
            }
        }

        // Handle additions - insert in sorted position
        for row in &input.added {
            let pos = self.find_position(row);
            self.current_rows.insert(pos, row.clone());
        }

        // Handle updates - remove old position, insert at new position
        for (old_row, new_row) in &input.updated {
            // Remove from old position
            if let Some(pos) = self.current_rows.iter().position(|r| r.id == old_row.id) {
                self.current_rows.remove(pos);
            }
            // Insert at new position
            let pos = self.find_position(new_row);
            self.current_rows.insert(pos, new_row.clone());
        }

        // Build result with rows in sorted order
        let mut result = RowDelta::new();

        // Propagate pending flag from input
        result.pending = input.pending;

        // Added rows in sorted order
        for row in &self.current_rows {
            if added_ids.remove(&row.id) {
                result.added.push(row.clone());
            }
        }

        // Removed rows (order doesn't matter as much, but use input order)
        for row in input.removed {
            if removed_ids.remove(&row.id) {
                result.removed.push(row);
            }
        }

        // Updated rows (find in sorted current_rows)
        for (old_row, _) in &input.updated {
            if updated_old_ids.contains(&old_row.id)
                && let Some(new_row) = self.current_rows.iter().find(|r| r.id == old_row.id)
            {
                result.updated.push((old_row.clone(), new_row.clone()));
            }
        }

        self.dirty = false;
        result
    }

    fn current_result(&self) -> &[Row] {
        &self.current_rows
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, Value};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn make_row(id: ObjectId, values: &[Value]) -> Row {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Row::new(id, data, CommitId([0; 32]))
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
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let row3 = make_row(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1, row2, row3],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let result = node.current_result();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, id2); // score 50
        assert_eq!(result[1].id, id3); // score 75
        assert_eq!(result[2].id, id1); // score 100
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
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let row3 = make_row(
            id3,
            &[
                Value::Integer(3),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1, row2, row3],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let result = node.current_result();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, id1); // score 100
        assert_eq!(result[1].id, id3); // score 75
        assert_eq!(result[2].id, id2); // score 50
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

        let make_row_local = |id: ObjectId, values: &[Value]| -> Row {
            let data = encode_row(&descriptor, values).unwrap();
            Row::new(id, data, CommitId([0; 32]))
        };

        let row1 = make_row_local(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        let row2 = make_row_local(
            id2,
            &[
                Value::Integer(1),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        let row3 = make_row_local(
            id3,
            &[
                Value::Integer(2),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );
        let row4 = make_row_local(
            id4,
            &[
                Value::Integer(2),
                Value::Text("D".into()),
                Value::Integer(90),
            ],
        );

        let delta = RowDelta {
            pending: false,
            added: vec![row1, row2, row3, row4],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let result = node.current_result();
        assert_eq!(result.len(), 4);
        // Dept 1, score desc: 100, 50
        assert_eq!(result[0].id, id1); // dept 1, score 100
        assert_eq!(result[1].id, id2); // dept 1, score 50
        // Dept 2, score desc: 90, 75
        assert_eq!(result[2].id, id4); // dept 2, score 90
        assert_eq!(result[3].id, id3); // dept 2, score 75
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
        let row1 = make_row(
            id1,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );

        node.process(RowDelta {
            pending: false,
            added: vec![row1],
            removed: vec![],
            updated: vec![],
        });

        // Insert row with lower score
        let row2 = make_row(
            id2,
            &[
                Value::Integer(2),
                Value::Text("B".into()),
                Value::Integer(50),
            ],
        );
        node.process(RowDelta {
            pending: false,
            added: vec![row2],
            removed: vec![],
            updated: vec![],
        });

        let result = node.current_result();
        assert_eq!(result[0].id, id2); // 50 first
        assert_eq!(result[1].id, id1); // 100 second
    }
}
