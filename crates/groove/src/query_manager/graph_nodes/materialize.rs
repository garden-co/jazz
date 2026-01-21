use std::collections::{HashMap, HashSet};

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::query_manager::types::{
    Row, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, TupleElement,
};

/// Materializes rows from IDs/tuples.
/// Converts TupleElement::Id to TupleElement::Row by loading row data.
///
/// Supports selective per-element materialization: only specified elements
/// will be materialized, others pass through unchanged.
#[derive(Debug)]
pub struct MaterializeNode {
    /// Output tuple descriptor (with updated materialization state).
    output_descriptor: TupleDescriptor,
    /// Descriptor for the row format (for backward compatibility).
    descriptor: RowDescriptor,
    /// Which elements to materialize (indices).
    elements_to_materialize: HashSet<usize>,
    /// Current materialized rows, keyed by ObjectId.
    rows: HashMap<ObjectId, Row>,
    /// Current tuples (fully or partially materialized).
    current_tuples: HashSet<Tuple>,
    /// IDs that are pending (loader returned None, still loading).
    pending_ids: HashSet<ObjectId>,
    /// IDs to check for content updates (row data may have changed).
    updated_ids: HashSet<ObjectId>,
    /// IDs that were deleted (emit removal delta during settle).
    deleted_ids: HashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

/// Function type for loading row data from storage.
pub type RowLoader = Box<dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>>;

impl MaterializeNode {
    /// Create a new materialize node with TupleDescriptor and selective element materialization.
    pub fn with_elements(
        input_desc: TupleDescriptor,
        elements_to_materialize: HashSet<usize>,
    ) -> Self {
        let output_descriptor = input_desc
            .clone()
            .with_materialized(&elements_to_materialize);
        let descriptor = input_desc.combined_descriptor();
        Self {
            output_descriptor,
            descriptor,
            elements_to_materialize,
            rows: HashMap::new(),
            current_tuples: HashSet::new(),
            pending_ids: HashSet::new(),
            updated_ids: HashSet::new(),
            deleted_ids: HashSet::new(),
            dirty: true,
        }
    }

    /// Create a new materialize node that materializes ALL elements.
    pub fn new_all(input_desc: TupleDescriptor) -> Self {
        let element_count = input_desc.element_count();
        let elements: HashSet<usize> = (0..element_count).collect();
        Self::with_elements(input_desc, elements)
    }

    /// Create a new materialize node with RowDescriptor (backward compatible).
    /// Creates a single-element tuple descriptor and materializes it.
    pub fn new(descriptor: RowDescriptor) -> Self {
        let input_desc = TupleDescriptor::single("", descriptor.clone());
        let elements: HashSet<usize> = [0].into_iter().collect();
        let output_descriptor = input_desc.with_materialized(&elements);
        Self {
            output_descriptor,
            descriptor,
            elements_to_materialize: elements,
            rows: HashMap::new(),
            current_tuples: HashSet::new(),
            pending_ids: HashSet::new(),
            updated_ids: HashSet::new(),
            deleted_ids: HashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    /// Check if an element should be materialized.
    fn should_materialize(&self, element_index: usize) -> bool {
        self.elements_to_materialize.contains(&element_index)
    }

    /// Check if there are any pending IDs.
    pub fn has_pending(&self) -> bool {
        !self.pending_ids.is_empty()
    }

    /// Get the set of pending IDs.
    pub fn pending_ids(&self) -> &HashSet<ObjectId> {
        &self.pending_ids
    }

    /// Mark an ID for content update checking (only if we're tracking it).
    pub fn mark_updated(&mut self, id: ObjectId) {
        if self.rows.contains_key(&id) {
            self.updated_ids.insert(id);
        }
    }

    /// Mark an ID as deleted - emit removal delta during next settle.
    pub fn mark_deleted(&mut self, id: ObjectId) {
        if self.rows.contains_key(&id) {
            self.deleted_ids.insert(id);
        }
    }

    /// Get the row descriptor.
    pub fn descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    /// Mark as dirty.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Check if dirty.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    // ========================================================================
    // Tuple-based methods for unified tuple model
    // ========================================================================

    /// Process a TupleDelta, materializing any TupleElement::Id into TupleElement::Row.
    /// Returns a TupleDelta with fully materialized tuples.
    /// If loader returns None for any ID, that element remains as Id and pending is set.
    pub fn materialize_tuples<F>(&mut self, delta: TupleDelta, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = TupleDelta::new();

        // Handle removed tuples - find the materialized version from current_tuples
        for tuple in delta.removed {
            // Find the materialized tuple in current_tuples (uses ID-based equality)
            let materialized_tuple = self.current_tuples.get(&tuple).cloned();

            // If tuple not in current_tuples, it was already handled by check_deleted_tuples
            // Skip to avoid duplicate/unmaterialized removal
            if materialized_tuple.is_none() {
                continue;
            }

            // Remove from tracking
            for elem in tuple.iter() {
                let id = elem.id();
                self.pending_ids.remove(&id);
                self.updated_ids.remove(&id);
                self.rows.remove(&id);
            }
            self.current_tuples.remove(&tuple);

            // Emit the materialized version
            result.removed.push(materialized_tuple.unwrap());
        }

        // Handle added tuples - materialize each element
        for tuple in delta.added {
            let materialized = self.materialize_tuple(&tuple, &mut loader);
            self.current_tuples.insert(materialized.clone());
            result.added.push(materialized);
        }

        // Handle updated tuples
        for (old_tuple, new_tuple) in delta.updated {
            self.current_tuples.remove(&old_tuple);
            let materialized = self.materialize_tuple(&new_tuple, &mut loader);
            self.current_tuples.insert(materialized.clone());
            result.updated.push((old_tuple, materialized));
        }

        result.pending = !self.pending_ids.is_empty();
        self.dirty = false;
        result
    }

    /// Materialize a single tuple, converting Id elements to Row elements.
    /// Only materializes elements in `elements_to_materialize`.
    fn materialize_tuple<F>(&mut self, tuple: &Tuple, loader: &mut F) -> Tuple
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let materialized_elements: Vec<TupleElement> = tuple
            .iter()
            .enumerate()
            .map(|(elem_idx, elem)| {
                // Only materialize if this element is in our list
                if !self.should_materialize(elem_idx) {
                    return elem.clone();
                }

                match elem {
                    TupleElement::Id(id) => {
                        // Try to load the row data
                        if let Some((data, commit_id)) = loader(*id) {
                            let row = Row::new(*id, data.clone(), commit_id);
                            self.rows.insert(*id, row);
                            // Remove from pending since we successfully loaded
                            self.pending_ids.remove(id);
                            TupleElement::Row {
                                id: *id,
                                content: data,
                                commit_id,
                            }
                        } else {
                            // Still pending
                            self.pending_ids.insert(*id);
                            elem.clone()
                        }
                    }
                    TupleElement::Row {
                        id,
                        content,
                        commit_id,
                    } => {
                        // Already materialized - update our cache
                        let row = Row::new(*id, content.clone(), *commit_id);
                        self.rows.insert(*id, row);
                        // Ensure not in pending
                        self.pending_ids.remove(id);
                        elem.clone()
                    }
                }
            })
            .collect();

        Tuple::new(materialized_elements)
    }

    /// Re-check pending tuples and return newly-materialized ones.
    pub fn check_pending_tuples<F>(&mut self, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = TupleDelta::new();

        // Find tuples that have pending elements
        let pending_tuples: Vec<Tuple> = self
            .current_tuples
            .iter()
            .filter(|t| t.iter().any(|e| !e.is_materialized()))
            .cloned()
            .collect();

        for old_tuple in pending_tuples {
            // Try to materialize
            let new_tuple = self.materialize_tuple(&old_tuple, &mut loader);

            // If any element changed from Id to Row, emit update
            let changed = old_tuple
                .iter()
                .zip(new_tuple.iter())
                .any(|(old, new)| !old.is_materialized() && new.is_materialized());

            if changed {
                self.current_tuples.remove(&old_tuple);
                self.current_tuples.insert(new_tuple.clone());
                result.updated.push((old_tuple, new_tuple));
            }
        }

        result.pending = !self.pending_ids.is_empty();
        result
    }

    /// Check for deleted IDs and return removal deltas (tuple version).
    pub fn check_deleted_tuples(&mut self) -> TupleDelta {
        let mut result = TupleDelta::new();
        let ids_to_remove: Vec<_> = self.deleted_ids.drain().collect();

        for id in ids_to_remove {
            self.pending_ids.remove(&id);
            self.updated_ids.remove(&id);
            self.rows.remove(&id);

            // Find and remove tuples containing this ID
            let tuples_to_remove: Vec<Tuple> = self
                .current_tuples
                .iter()
                .filter(|t| t.ids().contains(&id))
                .cloned()
                .collect();

            for tuple in tuples_to_remove {
                self.current_tuples.remove(&tuple);
                result.removed.push(tuple);
            }
        }

        result.pending = !self.pending_ids.is_empty();
        result
    }

    /// Check for updated IDs and return update deltas (tuple version).
    pub fn check_updated_tuples<F>(&mut self, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let mut result = TupleDelta::new();
        let ids_to_check: Vec<_> = self.updated_ids.drain().collect();

        for id in ids_to_check {
            // Find tuples containing this ID
            let affected_tuples: Vec<Tuple> = self
                .current_tuples
                .iter()
                .filter(|t| t.ids().contains(&id))
                .cloned()
                .collect();

            for old_tuple in affected_tuples {
                // Re-materialize the tuple
                let new_tuple = self.rematerialize_tuple(&old_tuple, &mut loader);

                // Check if content actually changed
                if has_content_changed(&old_tuple, &new_tuple) {
                    self.current_tuples.remove(&old_tuple);
                    self.current_tuples.insert(new_tuple.clone());
                    result.updated.push((old_tuple, new_tuple));
                }
            }
        }

        result.pending = !self.pending_ids.is_empty();
        result
    }

    /// Re-materialize a tuple, reloading row data for materialized elements.
    fn rematerialize_tuple<F>(&mut self, tuple: &Tuple, loader: &mut F) -> Tuple
    where
        F: FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    {
        let elements: Vec<TupleElement> = tuple
            .iter()
            .enumerate()
            .map(|(elem_idx, elem)| {
                // Only rematerialize if this element should be materialized
                if !self.should_materialize(elem_idx) {
                    return elem.clone();
                }

                let id = elem.id();
                if let Some((data, commit_id)) = loader(id) {
                    let row = Row::new(id, data.clone(), commit_id);
                    self.rows.insert(id, row);
                    TupleElement::Row {
                        id,
                        content: data,
                        commit_id,
                    }
                } else {
                    // Couldn't load - keep as-is
                    elem.clone()
                }
            })
            .collect();

        Tuple::new(elements)
    }

    /// Get current tuples.
    pub fn current_tuples(&self) -> &HashSet<Tuple> {
        &self.current_tuples
    }
}

/// Check if any element's content changed between two tuples with same IDs.
fn has_content_changed(old: &Tuple, new: &Tuple) -> bool {
    old.iter().zip(new.iter()).any(|(o, n)| {
        match (o, n) {
            (
                TupleElement::Row {
                    content: c1,
                    commit_id: cid1,
                    ..
                },
                TupleElement::Row {
                    content: c2,
                    commit_id: cid2,
                    ..
                },
            ) => c1 != c2 || cid1 != cid2,
            _ => false, // If either is not materialized, can't compare content
        }
    })
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

    fn make_tuple_delta_add(ids: &[ObjectId]) -> TupleDelta {
        TupleDelta {
            added: ids.iter().map(|&id| Tuple::from_id(id)).collect(),
            removed: vec![],
            updated: vec![],
            pending: false,
        }
    }

    fn make_tuple_delta_remove(tuples: Vec<Tuple>) -> TupleDelta {
        TupleDelta {
            added: vec![],
            removed: tuples,
            updated: vec![],
            pending: false,
        }
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

        let delta = make_tuple_delta_add(&[id1, id2]);

        let loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else if id == id2 {
                Some((data2.clone(), commit2))
            } else {
                None
            }
        };

        let result = node.materialize_tuples(delta, loader);

        assert_eq!(result.added.len(), 2);
        assert!(result.removed.is_empty());
        assert!(result.updated.is_empty());
    }

    #[test]
    fn materialize_removed_ids() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // First add
        let add_delta = make_tuple_delta_add(&[id1]);
        let loader =
            |_: ObjectId| -> Option<(Vec<u8>, CommitId)> { Some((data1.clone(), commit1)) };
        let added = node.materialize_tuples(add_delta, loader);
        assert_eq!(added.added.len(), 1);

        // Then remove - use the materialized tuple
        let materialized_tuple = added.added[0].clone();
        let remove_delta = make_tuple_delta_remove(vec![materialized_tuple]);

        let result = node.materialize_tuples(remove_delta, |_| None);

        assert!(result.added.is_empty());
        assert_eq!(result.removed.len(), 1);
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
        let add_delta = make_tuple_delta_add(&[id1]);
        node.materialize_tuples(add_delta, |_| Some((data1.clone(), commit1)));

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with new data
        let update_delta = node.check_updated_tuples(|_| Some((data2.clone(), commit2)));

        assert_eq!(update_delta.updated.len(), 1);
    }

    #[test]
    fn check_update_ignores_unchanged() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // Add the row
        let add_delta = make_tuple_delta_add(&[id1]);
        node.materialize_tuples(add_delta, |_| Some((data1.clone(), commit1)));

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with same data - should not emit update
        let update_delta = node.check_updated_tuples(|_| Some((data1.clone(), commit1)));

        assert!(update_delta.updated.is_empty());
    }

    #[test]
    fn materialize_tracks_pending_when_loader_returns_none() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = make_tuple_delta_add(&[id1, id2]);

        // Loader only returns data for id1, not id2
        let loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else {
                None // id2 is pending
            }
        };

        let result = node.materialize_tuples(delta, loader);

        // Both tuples are added (tuple-based API adds all, pending or not)
        assert_eq!(result.added.len(), 2);

        // Delta should be marked as pending (id2 is still loading)
        assert!(result.pending);

        // Node should track id2 as pending
        assert!(node.has_pending());
        assert!(node.pending_ids().contains(&id2));
        assert!(!node.pending_ids().contains(&id1));

        // id1's tuple should be materialized, id2's should not
        let id1_tuple = result.added.iter().find(|t| t.ids()[0] == id1).unwrap();
        assert!(id1_tuple.is_fully_materialized());

        let id2_tuple = result.added.iter().find(|t| t.ids()[0] == id2).unwrap();
        assert!(!id2_tuple.is_fully_materialized());
    }

    #[test]
    fn materialize_not_pending_when_all_loaded() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = make_tuple_delta_add(&[id1]);
        let loader =
            |_: ObjectId| -> Option<(Vec<u8>, CommitId)> { Some((data1.clone(), commit1)) };

        let result = node.materialize_tuples(delta, loader);

        // Row should be materialized
        assert_eq!(result.added.len(), 1);

        // Delta should NOT be pending
        assert!(!result.pending);

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
        let delta = make_tuple_delta_add(&[id1, id2]);

        let loader1 = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id1 {
                Some((data1.clone(), commit1))
            } else {
                None
            }
        };

        let result = node.materialize_tuples(delta, loader1);
        // Both tuples are added (tuple-based API)
        assert_eq!(result.added.len(), 2);
        assert!(result.pending);
        assert!(node.pending_ids().contains(&id2));

        // Now id2 becomes available
        let loader2 = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
            if id == id2 {
                Some((data2.clone(), commit2))
            } else {
                None
            }
        };

        let pending_result = node.check_pending_tuples(loader2);

        // Should emit id2 as newly materialized (as an update from Id->Row)
        // The check_pending_tuples emits updates for tuples that become materialized
        assert_eq!(pending_result.updated.len(), 1);

        // Should no longer be pending
        assert!(!pending_result.pending);
        assert!(!node.has_pending());
    }

    #[test]
    fn remove_clears_from_pending() {
        let descriptor = test_descriptor();
        let mut node = MaterializeNode::new(descriptor);

        let id1 = ObjectId::new();

        // Add as pending
        let add_delta = make_tuple_delta_add(&[id1]);
        let result = node.materialize_tuples(add_delta, |_| None);
        assert!(result.pending);
        assert!(node.pending_ids().contains(&id1));

        // Remove while still pending - need to use a tuple that matches
        let tuple = Tuple::from_id(id1);
        let remove_delta = make_tuple_delta_remove(vec![tuple]);
        let result = node.materialize_tuples(remove_delta, |_| None);

        // Should no longer be pending
        assert!(!result.pending);
        assert!(!node.pending_ids().contains(&id1));
    }
}
