use ahash::{AHashMap, AHashSet};
use std::collections::HashSet;

use crate::object::ObjectId;
use crate::query_manager::types::{
    LoadedRow, Row, RowDescriptor, TableName, Tuple, TupleDelta, TupleDescriptor, TupleElement,
    TupleProvenance,
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
    /// Reverse index for materialized tuples, keyed by any object ID contained
    /// in the tuple.
    current_tuples_by_id: AHashMap<ObjectId, AHashSet<Tuple>>,
    /// Current upstream tuples, including rows that are in scope but not yet
    /// materializable at the requested durability.
    known_tuples: AHashSet<Tuple>,
    /// Reverse index for known tuples, keyed by any object ID contained in the
    /// tuple. This keeps row update handling proportional to the affected
    /// tuples instead of scanning every tuple in the materializer.
    known_tuples_by_id: AHashMap<ObjectId, AHashSet<Tuple>>,
    /// IDs to check for content updates (row data may have changed).
    updated_ids: AHashSet<ObjectId>,
    /// IDs that were deleted (emit removal delta during settle).
    deleted_ids: AHashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

/// Function type for loading row data from storage.
pub type RowLoader = Box<dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>>;

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
            current_tuples_by_id: AHashMap::new(),
            known_tuples: AHashSet::new(),
            known_tuples_by_id: AHashMap::new(),
            updated_ids: AHashSet::new(),
            deleted_ids: AHashSet::new(),
            dirty: true,
        }
    }

    fn insert_current_tuple(&mut self, tuple: Tuple) {
        if self.current_tuples.insert(tuple.clone()) {
            for id in tuple.id_iter() {
                self.current_tuples_by_id
                    .entry(id)
                    .or_default()
                    .insert(tuple.clone());
            }
        }
    }

    fn remove_current_tuple_from_index(&mut self, tuple: &Tuple) {
        for id in tuple.id_iter() {
            if let Some(tuples) = self.current_tuples_by_id.get_mut(&id) {
                tuples.remove(tuple);
                if tuples.is_empty() {
                    self.current_tuples_by_id.remove(&id);
                }
            }
        }
    }

    fn remove_current_tuple(&mut self, tuple: &Tuple) -> bool {
        if let Some(existing) = self.current_tuples.take(tuple) {
            self.remove_current_tuple_from_index(&existing);
            true
        } else {
            false
        }
    }

    fn take_current_tuple(&mut self, tuple: &Tuple) -> Option<Tuple> {
        let existing = self.current_tuples.take(tuple)?;
        self.remove_current_tuple_from_index(&existing);
        Some(existing)
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
        self.updated_ids.insert(id);
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

    fn insert_known_tuple(&mut self, tuple: Tuple) {
        if self.known_tuples.insert(tuple.clone()) {
            for id in tuple.id_iter() {
                self.known_tuples_by_id
                    .entry(id)
                    .or_default()
                    .insert(tuple.clone());
            }
        }
    }

    fn remove_known_tuple(&mut self, tuple: &Tuple) {
        if self.known_tuples.remove(tuple) {
            for id in tuple.id_iter() {
                if let Some(tuples) = self.known_tuples_by_id.get_mut(&id) {
                    tuples.remove(tuple);
                    if tuples.is_empty() {
                        self.known_tuples_by_id.remove(&id);
                    }
                }
            }
        }
    }

    // ========================================================================
    // Tuple-based methods for unified tuple model
    // ========================================================================

    /// Process a TupleDelta, materializing any TupleElement::Id into TupleElement::Row.
    /// Returns a TupleDelta with fully materialized tuples.
    /// If loader returns None for any element in a tuple, that tuple is silently dropped.
    pub fn materialize_tuples<F>(&mut self, delta: TupleDelta, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();

        // Handle removed tuples - find the materialized version from current_tuples
        for tuple in delta.removed {
            self.remove_known_tuple(&tuple);
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
            self.remove_current_tuple(&tuple);

            // Emit the materialized version
            result.removed.push(materialized_tuple.unwrap());
        }

        // Handle added tuples - materialize each element
        for tuple in delta.added {
            self.insert_known_tuple(tuple.clone());
            if let Some(materialized) = self.materialize_tuple(&tuple, &mut loader) {
                self.insert_current_tuple(materialized.clone());
                result.added.push(materialized);
            }
            // If materialize_tuple returns None, the tuple is dropped (unavailable row)
        }

        // Handle updated tuples
        for (old_tuple, new_tuple) in delta.updated {
            self.remove_known_tuple(&old_tuple);
            self.insert_known_tuple(new_tuple.clone());
            self.remove_current_tuple(&old_tuple);
            if let Some(materialized) = self.materialize_tuple(&new_tuple, &mut loader) {
                self.insert_current_tuple(materialized.clone());
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
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut materialized_elements = Vec::with_capacity(tuple.len());
        let mut materialized_provenance = if tuple.len() == 1 {
            TupleProvenance::new()
        } else {
            tuple.provenance().clone()
        };
        let mut materialized_batch_provenance = tuple.batch_provenance().clone();

        for (elem_idx, elem) in tuple.iter().enumerate() {
            // Only materialize if this element is in our list
            if !self.should_materialize(elem_idx) {
                materialized_elements.push(elem.clone());
                continue;
            }

            match elem {
                TupleElement::Id(id) => {
                    // Try to load the row data
                    let table_hint = self
                        .output_descriptor
                        .element(elem_idx)
                        .map(|element| element.table);
                    if let Some(loaded) = loader(*id, table_hint) {
                        let row = Row::new(
                            *id,
                            loaded.data.clone(),
                            loaded.batch_id,
                            loaded.row_provenance.clone(),
                        );
                        self.rows.insert(*id, row);
                        if tuple.len() == 1 {
                            materialized_provenance = loaded.provenance.clone();
                        } else {
                            for scoped_object in loaded.provenance.iter().copied() {
                                materialized_provenance.insert(scoped_object);
                            }
                        }
                        materialized_batch_provenance.insert(loaded.batch_id);
                        materialized_elements.push(TupleElement::Row {
                            id: *id,
                            content: loaded.data,
                            batch_id: loaded.batch_id,
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
                    batch_id,
                    row_provenance,
                } => {
                    // Already materialized - update our cache
                    let row = Row::new(*id, content.clone(), *batch_id, row_provenance.clone());
                    self.rows.insert(*id, row);
                    materialized_elements.push(elem.clone());
                }
            }
        }

        Some(Tuple::new_with_shadow_state(
            materialized_elements,
            materialized_provenance,
            materialized_batch_provenance,
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
                .current_tuples_by_id
                .get(&id)
                .map(|tuples| tuples.iter().cloned().collect())
                .unwrap_or_default();

            for tuple in tuples_to_remove {
                self.remove_current_tuple(&tuple);
                result.removed.push(tuple);
            }
        }

        result
    }

    /// Check for updated IDs and return update deltas (tuple version).
    pub fn check_updated_tuples<F>(&mut self, mut loader: F) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();
        let ids_to_check: Vec<_> = self.updated_ids.drain().collect();
        let mut affected_tuples = AHashSet::new();

        for id in ids_to_check {
            // Find tuples containing this ID, including rows that were previously
            // in scope but not materializable at the requested durability.
            if let Some(tuples) = self.known_tuples_by_id.get(&id) {
                affected_tuples.extend(tuples.iter().cloned());
            }
        }

        for tuple in affected_tuples {
            let previous_materialized = self.take_current_tuple(&tuple);

            if let Some(new_tuple) = self.materialize_tuple(&tuple, &mut loader) {
                self.insert_current_tuple(new_tuple.clone());
                match previous_materialized {
                    Some(old_tuple) => {
                        if has_content_changed(&old_tuple, &new_tuple) {
                            result.updated.push((old_tuple, new_tuple));
                        }
                    }
                    None => result.added.push(new_tuple),
                }
            } else if let Some(old_tuple) = previous_materialized {
                result.removed.push(old_tuple);
            }
        }

        result
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
                    batch_id: cid1,
                    ..
                },
                TupleElement::Row {
                    content: c2,
                    batch_id: cid2,
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
    use crate::metadata::RowProvenance;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};
    use crate::row_histories::BatchId;

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_commit_id(n: u8) -> BatchId {
        BatchId([n; 16])
    }

    fn make_loaded_row(data: Vec<u8>, batch_id: BatchId) -> LoadedRow {
        LoadedRow::new(
            data,
            RowProvenance::for_insert("jazz:test", 0),
            Default::default(),
            batch_id,
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

        let loader = |id: ObjectId, _table_hint: Option<TableName>| -> Option<LoadedRow> {
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
        let loader = |_: ObjectId, _table_hint: Option<TableName>| -> Option<LoadedRow> {
            Some(make_loaded_row(data1.clone(), commit1))
        };
        let added = node.materialize_tuples(add_delta, loader);
        assert_eq!(added.added.len(), 1);

        // Then remove - use the materialized tuple
        let materialized_tuple = added.added[0].clone();
        let remove_delta = make_tuple_delta_remove(vec![materialized_tuple]);

        let result = node.materialize_tuples(remove_delta, |_, _| None);

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
        node.materialize_tuples(add_delta, |_, _| {
            Some(make_loaded_row(data1.clone(), commit1))
        });

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with new data
        let update_delta =
            node.check_updated_tuples(|_, _| Some(make_loaded_row(data2.clone(), commit2)));

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
        node.materialize_tuples(add_delta, |_, _| {
            Some(make_loaded_row(data1.clone(), commit1))
        });

        // Mark for update check
        node.mark_updated(id1);

        // Check for update with same data - should not emit update
        let update_delta =
            node.check_updated_tuples(|_, _| Some(make_loaded_row(data1.clone(), commit1)));

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
        let loader = |id: ObjectId, _table_hint: Option<TableName>| -> Option<LoadedRow> {
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
        let loader = |_: ObjectId, _table_hint: Option<TableName>| -> Option<LoadedRow> {
            Some(make_loaded_row(data1.clone(), commit1))
        };

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
        let result = node.materialize_tuples(add_delta, |_, _| None);
        assert_eq!(result.added.len(), 0);

        // Node should have no tuples
        assert_eq!(node.current_tuples().len(), 0);
    }

    #[test]
    fn check_update_can_add_row_that_was_previously_unavailable() {
        let mut node = make_materialize_node();

        let id1 = ObjectId::new();
        let data1 = vec![1, 2, 3];
        let commit1 = make_commit_id(1);

        let add_delta = make_tuple_delta_add(&[id1]);
        let dropped = node.materialize_tuples(add_delta, |_, _| None);
        assert!(dropped.added.is_empty());
        assert_eq!(node.current_tuples().len(), 0);

        node.mark_updated(id1);

        let update_delta =
            node.check_updated_tuples(|_, _| Some(make_loaded_row(data1.clone(), commit1)));

        assert_eq!(update_delta.added.len(), 1);
        assert!(update_delta.updated.is_empty());
        assert_eq!(update_delta.added[0].ids(), &[id1]);
        assert_eq!(node.current_tuples().len(), 1);
    }
}
