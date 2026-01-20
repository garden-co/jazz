use crate::query_manager::types::{Row, RowDelta, RowDescriptor};

use super::RowNode;

/// Limit and offset node for pagination.
#[derive(Debug)]
pub struct LimitOffsetNode {
    descriptor: RowDescriptor,
    limit: Option<usize>,
    offset: usize,
    /// All rows from input (before limit/offset applied).
    all_rows: Vec<Row>,
    /// Current rows after limit/offset.
    current_rows: Vec<Row>,
    dirty: bool,
}

impl LimitOffsetNode {
    pub fn new(descriptor: RowDescriptor, limit: Option<usize>, offset: usize) -> Self {
        Self {
            descriptor,
            limit,
            offset,
            all_rows: Vec::new(),
            current_rows: Vec::new(),
            dirty: true,
        }
    }

    /// Recompute current_rows from all_rows based on limit/offset.
    fn recompute_window(&mut self) {
        let start = self.offset.min(self.all_rows.len());
        let end = match self.limit {
            Some(limit) => (start + limit).min(self.all_rows.len()),
            None => self.all_rows.len(),
        };
        self.current_rows = self.all_rows[start..end].to_vec();
    }

    /// Compute the delta between old and new window.
    fn compute_delta(&self, old_rows: &[Row], new_rows: &[Row]) -> RowDelta {
        let mut delta = RowDelta::new();

        // Find removed rows (in old but not in new)
        for old in old_rows {
            if !new_rows.iter().any(|r| r.id == old.id) {
                delta.removed.push(old.clone());
            }
        }

        // Find added rows (in new but not in old)
        for new in new_rows {
            if !old_rows.iter().any(|r| r.id == new.id) {
                delta.added.push(new.clone());
            }
        }

        // Find updated rows (in both, but potentially different data)
        for new in new_rows {
            if let Some(old) = old_rows.iter().find(|r| r.id == new.id)
                && (old.data != new.data || old.commit_id != new.commit_id)
            {
                delta.updated.push((old.clone(), new.clone()));
            }
        }

        delta
    }
}

impl RowNode for LimitOffsetNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: RowDelta) -> RowDelta {
        let old_rows = self.current_rows.clone();

        // Apply changes to all_rows
        for row in input.removed {
            self.all_rows.retain(|r| r.id != row.id);
        }

        // For added rows, we maintain the order from input (assumed sorted)
        for row in input.added {
            self.all_rows.push(row);
        }

        // For updated rows, update in place
        for (old_row, new_row) in input.updated {
            if let Some(pos) = self.all_rows.iter().position(|r| r.id == old_row.id) {
                self.all_rows[pos] = new_row;
            }
        }

        // Recompute window
        self.recompute_window();
        self.dirty = false;

        // Return the delta for the window
        self.compute_delta(&old_rows, &self.current_rows)
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
        ])
    }

    fn make_row(id: ObjectId, n: i32, name: &str) -> Row {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, &[Value::Integer(n), Value::Text(name.into())]).unwrap();
        Row::new(id, data, CommitId([0; 32]))
    }

    #[test]
    fn limit_only() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 0);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let rows: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_row(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = RowDelta {
            added: rows,
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        assert_eq!(node.current_result().len(), 2);
        assert_eq!(node.current_result()[0].id, ids[0]);
        assert_eq!(node.current_result()[1].id, ids[1]);
    }

    #[test]
    fn offset_only() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, None, 2);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let rows: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_row(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = RowDelta {
            added: rows,
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 3);
        assert_eq!(node.current_result().len(), 3);
        assert_eq!(node.current_result()[0].id, ids[2]);
        assert_eq!(node.current_result()[1].id, ids[3]);
        assert_eq!(node.current_result()[2].id, ids[4]);
    }

    #[test]
    fn limit_and_offset() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 1);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let rows: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_row(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = RowDelta {
            added: rows,
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        assert_eq!(node.current_result().len(), 2);
        assert_eq!(node.current_result()[0].id, ids[1]);
        assert_eq!(node.current_result()[1].id, ids[2]);
    }

    #[test]
    fn removal_shifts_window() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 0);

        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let rows: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_row(*id, i as i32, &format!("Row{}", i)))
            .collect();

        // Initial: [0, 1, 2, 3] -> window [0, 1]
        node.process(RowDelta {
            added: rows.clone(),
            removed: vec![],
            updated: vec![],
        });
        assert_eq!(node.current_result()[0].id, ids[0]);
        assert_eq!(node.current_result()[1].id, ids[1]);

        // Remove first row: [1, 2, 3] -> window [1, 2]
        let result = node.process(RowDelta {
            added: vec![],
            removed: vec![rows[0].clone()],
            updated: vec![],
        });

        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].id, ids[0]);
        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].id, ids[2]); // New row slides in

        assert_eq!(node.current_result().len(), 2);
        assert_eq!(node.current_result()[0].id, ids[1]);
        assert_eq!(node.current_result()[1].id, ids[2]);
    }

    #[test]
    fn offset_beyond_data() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(10), 100);

        let id = ObjectId::new();
        let row = make_row(id, 1, "Row1");

        let delta = RowDelta {
            added: vec![row],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        assert!(node.current_result().is_empty());
    }
}
