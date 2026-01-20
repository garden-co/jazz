use std::collections::{HashMap, HashSet};

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
    /// IDs that are pending (loader returned None, still loading).
    pending_ids: HashSet<ObjectId>,
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
            pending_ids: HashSet::new(),
            dirty: true,
        }
    }

    /// Process an IdDelta, loading row data for added IDs.
    /// Returns the RowDelta with full row data.
    /// If loader returns None for any ID, that ID is tracked as pending.
    pub fn materialize<F>(&mut self, delta: IdDelta, mut loader: F) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = RowDelta::new();

        // Handle removed IDs
        for id in delta.removed {
            // Remove from pending if it was there
            self.pending_ids.remove(&id);
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
            } else {
                // Row not yet available - track as pending
                self.pending_ids.insert(id);
            }
        }

        // Mark result as pending if we have any pending IDs
        result.pending = !self.pending_ids.is_empty();

        self.dirty = false;
        result
    }

    /// Re-check pending IDs and emit newly-loaded rows.
    /// Returns a RowDelta with rows that are now available.
    pub fn check_pending<F>(&mut self, mut loader: F) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = RowDelta::new();

        // Check each pending ID
        let pending_ids: Vec<ObjectId> = self.pending_ids.iter().copied().collect();
        for id in pending_ids {
            if let Some((data, commit_id)) = loader(id) {
                // Row is now available - remove from pending and add to result
                self.pending_ids.remove(&id);
                let row = Row::new(id, data, commit_id);
                self.rows.insert(id, row.clone());
                result.added.push(row);
            }
        }

        // Mark result as pending if we still have pending IDs
        result.pending = !self.pending_ids.is_empty();

        result
    }

    /// Check if there are any pending IDs.
    pub fn has_pending(&self) -> bool {
        !self.pending_ids.is_empty()
    }

    /// Get the set of pending IDs.
    pub fn pending_ids(&self) -> &HashSet<ObjectId> {
        &self.pending_ids
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
                    pending: !self.pending_ids.is_empty(),
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

    #[test]
    fn materialize_tracks_pending_when_loader_returns_none() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = IdDelta {
            added: [id1, id2].into_iter().collect(),
            removed: Default::default(),
        };

        // Loader only returns data for id1, not id2
        let loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else {
                None // id2 is pending
            }
        };

        let row_delta = node.materialize(delta, loader);

        // Only id1 should be materialized
        assert_eq!(row_delta.added.len(), 1);
        assert_eq!(row_delta.added[0].id, id1);

        // Delta should be marked as pending
        assert!(row_delta.pending);

        // Node should track id2 as pending
        assert!(node.has_pending());
        assert!(node.pending_ids().contains(&id2));
        assert!(!node.pending_ids().contains(&id1));
    }

    #[test]
    fn materialize_not_pending_when_all_loaded() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = IdDelta {
            added: [id1].into_iter().collect(),
            removed: Default::default(),
        };

        let loader =
            |_: ObjectId| -> Option<(Vec<u8>, CommitId)> { Some((data1.clone(), commit1)) };

        let row_delta = node.materialize(delta, loader);

        // Row should be materialized
        assert_eq!(row_delta.added.len(), 1);

        // Delta should NOT be pending
        assert!(!row_delta.pending);

        // Node should not have pending IDs
        assert!(!node.has_pending());
    }

    #[test]
    fn check_pending_emits_newly_loaded_rows() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];
        let commit1 = make_commit_id(1);
        let commit2 = make_commit_id(2);

        // First materialize - id2 is pending
        let delta = IdDelta {
            added: [id1, id2].into_iter().collect(),
            removed: Default::default(),
        };

        let loader1 = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else {
                None
            }
        };

        let row_delta = node.materialize(delta, loader1);
        assert_eq!(row_delta.added.len(), 1);
        assert!(row_delta.pending);
        assert!(node.pending_ids().contains(&id2));

        // Now id2 becomes available
        let loader2 = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id2 {
                Some((data2.clone(), commit2))
            } else {
                None
            }
        };

        let pending_delta = node.check_pending(loader2);

        // Should emit id2 as newly added
        assert_eq!(pending_delta.added.len(), 1);
        assert_eq!(pending_delta.added[0].id, id2);

        // Should no longer be pending
        assert!(!pending_delta.pending);
        assert!(!node.has_pending());
    }

    #[test]
    fn remove_clears_from_pending() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();

        // Add as pending
        let add_delta = IdDelta {
            added: [id1].into_iter().collect(),
            removed: Default::default(),
        };
        let row_delta = node.materialize(add_delta, |_| None);
        assert!(row_delta.pending);
        assert!(node.pending_ids().contains(&id1));

        // Remove while still pending
        let remove_delta = IdDelta {
            added: Default::default(),
            removed: [id1].into_iter().collect(),
        };
        let row_delta = node.materialize(remove_delta, |_| None);

        // Should no longer be pending
        assert!(!row_delta.pending);
        assert!(!node.pending_ids().contains(&id1));
    }
}
