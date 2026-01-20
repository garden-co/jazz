use std::collections::HashMap;

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::query_manager::types::{IdDelta, Row, RowDelta, RowDescriptor};

/// Materializes rows from IDs.
/// This is the boundary between Phase 1 (IdDelta) and Phase 2 (RowDelta).
#[derive(Debug)]
pub struct MaterializeNode {
    /// Descriptor for the row format.
    descriptor: RowDescriptor,
    /// Current materialized rows, keyed by ObjectId.
    rows: HashMap<ObjectId, Row>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

/// Function type for loading row data from storage.
pub type RowLoader = Box<dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>>;

impl MaterializeNode {
    pub fn new(descriptor: RowDescriptor) -> Self {
        Self {
            descriptor,
            rows: HashMap::new(),
            dirty: true,
        }
    }

    /// Process an IdDelta, loading row data for added IDs.
    /// Returns the RowDelta with full row data.
    pub fn materialize<F>(&mut self, delta: IdDelta, mut loader: F) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = RowDelta::new();

        // Handle removed IDs
        for id in delta.removed {
            if let Some(row) = self.rows.remove(&id) {
                result.removed.push(row);
            }
        }

        // Handle added IDs - load row data
        for id in delta.added {
            if let Some((data, commit_id)) = loader(id) {
                let row = Row::new(id, data, commit_id);
                self.rows.insert(id, row.clone());
                result.added.push(row);
            }
        }

        self.dirty = false;
        result
    }

    /// Check if a row has been updated (data changed).
    /// Call this when receiving an object update notification.
    pub fn check_update<F>(&mut self, id: ObjectId, mut loader: F) -> Option<RowDelta>
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        if let Some(old_row) = self.rows.get(&id)
            && let Some((new_data, new_commit_id)) = loader(id)
        {
            // Check if data actually changed
            if old_row.data != new_data || old_row.commit_id != new_commit_id {
                let old_row = old_row.clone();
                let new_row = Row::new(id, new_data, new_commit_id);
                self.rows.insert(id, new_row.clone());

                return Some(RowDelta {
                    added: vec![],
                    removed: vec![],
                    updated: vec![(old_row, new_row)],
                });
            }
        }
        None
    }

    /// Get the row descriptor.
    pub fn descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    /// Get current rows.
    pub fn current_rows(&self) -> &HashMap<ObjectId, Row> {
        &self.rows
    }

    /// Get a specific row by ID.
    pub fn get_row(&self, id: ObjectId) -> Option<&Row> {
        self.rows.get(&id)
    }

    /// Mark as dirty.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Check if dirty.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
    }

    #[test]
    fn materialize_added_ids() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];
        let commit1 = make_commit_id(1);
        let commit2 = make_commit_id(2);

        let delta = IdDelta {
            added: [id1, id2].into_iter().collect(),
            removed: Default::default(),
        };

        let loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else if id == id2 {
                Some((data2.clone(), commit2))
            } else {
                None
            }
        };

        let row_delta = node.materialize(delta, loader);

        assert_eq!(row_delta.added.len(), 2);
        assert!(row_delta.removed.is_empty());
        assert!(row_delta.updated.is_empty());
    }

    #[test]
    fn materialize_removed_ids() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // First add
        let add_delta = IdDelta {
            added: [id1].into_iter().collect(),
            removed: Default::default(),
        };

        let loader =
            |_: ObjectId| -> Option<(Vec<u8>, CommitId)> { Some((data1.clone(), commit1)) };
        node.materialize(add_delta, loader);

        // Then remove
        let remove_delta = IdDelta {
            added: Default::default(),
            removed: [id1].into_iter().collect(),
        };

        let row_delta = node.materialize(remove_delta, |_| None);

        assert!(row_delta.added.is_empty());
        assert_eq!(row_delta.removed.len(), 1);
        assert_eq!(row_delta.removed[0].id, id1);
    }

    #[test]
    fn check_update_detects_changes() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];
        let commit1 = make_commit_id(1);
        let commit2 = make_commit_id(2);

        // Add the row
        let add_delta = IdDelta {
            added: [id1].into_iter().collect(),
            removed: Default::default(),
        };
        node.materialize(add_delta, |_| Some((data1.clone(), commit1)));

        // Check for update with new data
        let update_delta = node.check_update(id1, |_| Some((data2.clone(), commit2)));

        assert!(update_delta.is_some());
        let update_delta = update_delta.unwrap();
        assert_eq!(update_delta.updated.len(), 1);
        assert_eq!(update_delta.updated[0].0.data, data1);
        assert_eq!(update_delta.updated[0].1.data, data2);
    }

    #[test]
    fn check_update_ignores_unchanged() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // Add the row
        let add_delta = IdDelta {
            added: [id1].into_iter().collect(),
            removed: Default::default(),
        };
        node.materialize(add_delta, |_| Some((data1.clone(), commit1)));

        // Check for update with same data
        let update_delta = node.check_update(id1, |_| Some((data1.clone(), commit1)));

        assert!(update_delta.is_none());
    }
}
