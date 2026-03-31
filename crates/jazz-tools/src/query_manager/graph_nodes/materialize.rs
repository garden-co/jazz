use ahash::{AHashMap, AHashSet};
use std::collections::HashSet;

use crate::object::ObjectId;
use crate::query_manager::types::{
    LoadedRow, Row, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, TupleElement,
};

/// Materializes rows from IDs/tuples.
/// Converts TupleElement::Id to TupleElement::Row by loading row data.
///
/// Supports selective per-element materialization: only specified elements
/// will be materialized, others pass through unchanged.
///
/// When the loader returns None for an element, the entire tuple is dropped
/// (the row is genuinely unavailable, e.g. hard-deleted).
#[derive(Debug)]
pub struct MaterializeNode {
    /// Output tuple descriptor (with updated materialization state).
    output_descriptor: TupleDescriptor,
    /// Descriptor for the row format (for backward compatibility).
    descriptor: RowDescriptor,
    /// Which elements to materialize (indices).
    elements_to_materialize: HashSet<usize>,
    /// Current materialized rows, keyed by ObjectId.
    rows: AHashMap<ObjectId, Row>,
    /// Current tuples (fully or partially materialized).
    current_tuples: AHashSet<Tuple>,
    /// IDs to check for content updates (row data may have changed).
    updated_ids: AHashSet<ObjectId>,
    /// IDs that were deleted (emit removal delta during settle).
    deleted_ids: AHashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

/// Function type for loading row data from storage.
pub type RowLoader = Box<dyn FnMut(ObjectId) -> Option<LoadedRow>>;

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
            rows: AHashMap::new(),
            current_tuples: AHashSet::new(),
            updated_ids: AHashSet::new(),
            deleted_ids: AHashSet::new(),
            dirty: true,
        }
    }

    /// Create a new materialize node that materializes ALL elements.
    pub fn new_all(input_desc: TupleDescriptor) -> Self {
        let element_count = input_desc.element_count();
        let elements: HashSet<usize> = (0..element_count).collect();
        Self::with_elements(input_desc, elements)
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    /// Check if an element should be materialized.
    fn should_materialize(&self, element_index: usize) -> bool {
        self.elements_to_materialize.contains(&element_index)
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
    /// If loader returns None for any element in a tuple, that tuple is silently dropped.
    pub fn materialize_tuples<F>(&mut self, delta: TupleDelta, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
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
                self.updated_ids.remove(&id);
                self.rows.remove(&id);
            }
            self.current_tuples.remove(&tuple);

            // Emit the materialized version
            result.removed.push(materialized_tuple.unwrap());
        }

        // Handle added tuples - materialize each element
        for tuple in delta.added {
            if let Some(materialized) = self.materialize_tuple(&tuple, &mut loader) {
                self.current_tuples.insert(materialized.clone());
                result.added.push(materialized);
            }
            // If materialize_tuple returns None, the tuple is dropped (unavailable row)
        }

        // Handle updated tuples
        for (old_tuple, new_tuple) in delta.updated {
            self.current_tuples.remove(&old_tuple);
            if let Some(materialized) = self.materialize_tuple(&new_tuple, &mut loader) {
                self.current_tuples.insert(materialized.clone());
                result.updated.push((old_tuple, materialized));
            } else {
                // New version unavailable - emit as removal
                result.removed.push(old_tuple);
            }
        }

        tracing::trace!(
            added = result.added.len(),
            removed = result.removed.len(),
            updated = result.updated.len(),
            total = self.current_tuples.len(),
            "materialize node processed"
        );

        self.dirty = false;
        result
    }

    /// Materialize a single tuple, converting Id elements to Row elements.
    /// Only materializes elements in `elements_to_materialize`.
    /// Returns None if any element that should be materialized can't be loaded.
    fn materialize_tuple<F>(&mut self, tuple: &Tuple, loader: &mut F) -> Option<Tuple>
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        let mut materialized_elements = Vec::with_capacity(tuple.len());
        let mut materialized_provenance = if tuple.len() == 1 {
            AHashSet::new()
        } else {
            tuple.provenance().clone()
        };

        for (elem_idx, elem) in tuple.iter().enumerate() {
            // Only materialize if this element is in our list
            if !self.should_materialize(elem_idx) {
                materialized_elements.push(elem.clone());
                continue;
            }

            match elem {
                TupleElement::Id(id) => {
                    // Try to load the row data
                    if let Some(loaded) = loader(*id) {
                        let row = Row::new(
                            *id,
                            loaded.data.clone(),
                            loaded.commit_id,
                            loaded.row_provenance.clone(),
                        );
                        self.rows.insert(*id, row);
                        if tuple.len() == 1 {
                            materialized_provenance = loaded.provenance.clone();
                        } else {
                            materialized_provenance.extend(loaded.provenance.iter().copied());
                        }
                        materialized_elements.push(TupleElement::Row {
                            id: *id,
                            content: loaded.data,
                            commit_id: loaded.commit_id,
                            row_provenance: loaded.row_provenance,
                        });
                    } else {
                        // Row unavailable - drop the entire tuple
                        return None;
                    }
                }
                TupleElement::Row {
                    id,
                    content,
                    commit_id,
                    row_provenance,
                } => {
                    // Already materialized - update our cache
                    let row = Row::new(*id, content.clone(), *commit_id, row_provenance.clone());
                    self.rows.insert(*id, row);
                    materialized_elements.push(elem.clone());
                }
            }
        }

        Some(Tuple::new_with_provenance(
            materialized_elements,
            materialized_provenance,
        ))
    }

    /// Check for deleted IDs and return removal deltas (tuple version).
    pub fn check_deleted_tuples(&mut self) -> TupleDelta {
        let mut result = TupleDelta::new();
        let ids_to_remove: Vec<_> = self.deleted_ids.drain().collect();

        for id in ids_to_remove {
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

        result
    }

    /// Check for updated IDs and return update deltas (tuple version).
    pub fn check_updated_tuples<F>(&mut self, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
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

        result
    }

    /// Re-materialize a tuple, reloading row data for materialized elements.
    fn rematerialize_tuple<F>(&mut self, tuple: &Tuple, loader: &mut F) -> Tuple
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        let mut rematerialized_provenance = if tuple.len() == 1 {
            AHashSet::new()
        } else {
            tuple.provenance().clone()
        };
        let elements: Vec<TupleElement> = tuple
            .iter()
            .enumerate()
            .map(|(elem_idx, elem)| {
                // Only rematerialize if this element should be materialized
                if !self.should_materialize(elem_idx) {
                    return elem.clone();
                }

                let id = elem.id();
                if let Some(loaded) = loader(id) {
                    let row = Row::new(
                        id,
                        loaded.data.clone(),
                        loaded.commit_id,
                        loaded.row_provenance.clone(),
                    );
                    self.rows.insert(id, row);
                    if tuple.len() == 1 {
                        rematerialized_provenance = loaded.provenance.clone();
                    } else {
                        rematerialized_provenance.extend(loaded.provenance.iter().copied());
                    }
                    TupleElement::Row {
                        id,
                        content: loaded.data,
                        commit_id: loaded.commit_id,
                        row_provenance: loaded.row_provenance,
                    }
                } else {
                    // Couldn't load - keep as-is
                    elem.clone()
                }
            })
            .collect();

        Tuple::new_with_provenance(elements, rematerialized_provenance)
    }

    /// Get current tuples.
    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
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
    use crate::commit::CommitId;
    use crate::metadata::RowProvenance;
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

    fn make_loaded_row(data: Vec<u8>, commit_id: CommitId) -> LoadedRow {
        LoadedRow::new(
            data,
            commit_id,
            RowProvenance::for_insert("jazz:test", 0),
            Default::default(),
        )
    }

    fn make_tuple_delta_add(ids: &[ObjectId]) -> TupleDelta {
        TupleDelta {
            added: ids.iter().map(|&id| Tuple::from_id(id)).collect(),
            removed: vec![],
            moved: vec![],
            updated: vec![],
        }
    }

    fn make_tuple_delta_remove(tuples: Vec<Tuple>) -> TupleDelta {
        TupleDelta {
            added: vec![],
            removed: tuples,
            moved: vec![],
            updated: vec![],
        }
    }

    fn make_materialize_node() -> MaterializeNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single("", descriptor);
        MaterializeNode::new_all(tuple_desc)
    }

    #[test]
    fn materialize_added_ids() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];
        let commit1 = make_commit_id(1);
        let commit2 = make_commit_id(2);

        let delta = make_tuple_delta_add(&[id1, id2]);

        let loader = |id: ObjectId| -> Option<LoadedRow> {
            if id == id1 {
                Some(make_loaded_row(data1.clone(), commit1))
            } else if id == id2 {
                Some(make_loaded_row(data2.clone(), commit2))
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
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // First add
        let add_delta = make_tuple_delta_add(&[id1]);
        let loader =
            |_: ObjectId| -> Option<LoadedRow> { Some(make_loaded_row(data1.clone(), commit1)) };
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
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];
        let commit1 = make_commit_id(1);
        let commit2 = make_commit_id(2);

        // Add the row
        let add_delta = make_tuple_delta_add(&[id1]);
        node.materialize_tuples(add_delta, |_| Some(make_loaded_row(data1.clone(), commit1)));

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with new data
        let update_delta =
            node.check_updated_tuples(|_| Some(make_loaded_row(data2.clone(), commit2)));

        assert_eq!(update_delta.updated.len(), 1);
    }

    #[test]
    fn check_update_ignores_unchanged() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        // Add the row
        let add_delta = make_tuple_delta_add(&[id1]);
        node.materialize_tuples(add_delta, |_| Some(make_loaded_row(data1.clone(), commit1)));

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with same data - should not emit update
        let update_delta =
            node.check_updated_tuples(|_| Some(make_loaded_row(data1.clone(), commit1)));

        assert!(update_delta.updated.is_empty());
    }

    #[test]
    fn materialize_drops_tuples_when_loader_returns_none() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = make_tuple_delta_add(&[id1, id2]);

        // Loader only returns data for id1, not id2
        let loader = |id: ObjectId| -> Option<LoadedRow> {
            if id == id1 {
                Some(make_loaded_row(data1.clone(), commit1))
            } else {
                None // id2 is unavailable (hard-deleted)
            }
        };

        let result = node.materialize_tuples(delta, loader);

        // Only id1's tuple should be added; id2 is silently dropped
        assert_eq!(result.added.len(), 1);
        assert!(result.added[0].is_fully_materialized());
        assert_eq!(result.added[0].ids()[0], id1);

        // Node should only track id1
        assert_eq!(node.current_tuples().len(), 1);
    }

    #[test]
    fn materialize_all_loaded() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let delta = make_tuple_delta_add(&[id1]);
        let loader =
            |_: ObjectId| -> Option<LoadedRow> { Some(make_loaded_row(data1.clone(), commit1)) };

        let result = node.materialize_tuples(delta, loader);

        // Row should be materialized
        assert_eq!(result.added.len(), 1);
        assert!(result.added[0].is_fully_materialized());
    }

    #[test]
    fn remove_unavailable_row_is_noop() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();

        // Add but loader returns None - tuple is dropped
        let add_delta = make_tuple_delta_add(&[id1]);
        let result = node.materialize_tuples(add_delta, |_| None);
        assert_eq!(result.added.len(), 0);

        // Node should have no tuples
        assert_eq!(node.current_tuples().len(), 0);
    }
}
