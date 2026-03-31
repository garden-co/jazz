use ahash::AHashSet;

use crate::query_manager::{
    graph_nodes::tuple_delta::compute_tuple_delta,
    types::{RowDescriptor, Tuple, TupleDelta, TupleDescriptor},
};

use super::RowNode;

/// Limit and offset node for pagination.
#[derive(Debug)]
pub struct LimitOffsetNode {
    descriptor: RowDescriptor,
    /// Output tuple descriptor (same as input - pass-through).
    output_tuple_descriptor: TupleDescriptor,
    limit: Option<usize>,
    offset: usize,
    /// All tuples from input (before limit/offset applied).
    all_tuples: Vec<Tuple>,
    /// Current tuples after limit/offset.
    windowed_tuples: Vec<Tuple>,
    /// HashSet view for trait requirement.
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl LimitOffsetNode {
    /// Create a LimitOffsetNode with TupleDescriptor.
    pub fn with_tuple_descriptor(
        tuple_descriptor: TupleDescriptor,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        let descriptor = tuple_descriptor.combined_descriptor();
        Self {
            descriptor,
            output_tuple_descriptor: tuple_descriptor,
            limit,
            offset,
            all_tuples: Vec::new(),
            windowed_tuples: Vec::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Recompute windowed_tuples from all_tuples based on limit/offset.
    fn recompute_tuple_window(&mut self) {
        let start = self.offset.min(self.all_tuples.len());
        let end = match self.limit {
            Some(limit) => (start + limit).min(self.all_tuples.len()),
            None => self.all_tuples.len(),
        };
        let sync_scope = self.sync_scope_provenance();
        self.windowed_tuples.clear();
        self.windowed_tuples
            .extend(
                self.all_tuples[start..end]
                    .iter()
                    .cloned()
                    .map(|mut tuple| {
                        tuple.merge_provenance(&sync_scope);
                        tuple
                    }),
            );
        self.current_tuples = self.windowed_tuples.iter().cloned().collect();
    }

    /// Rebuild state from a full ordered input (e.g. upstream SortNode output).
    pub fn process_with_ordered_input(&mut self, ordered_tuples: &[Tuple]) -> TupleDelta {
        let old_tuples = std::mem::take(&mut self.windowed_tuples);
        self.all_tuples.clear();
        self.all_tuples.extend_from_slice(ordered_tuples);
        self.recompute_tuple_window();
        self.dirty = false;
        compute_tuple_delta(&old_tuples, &self.windowed_tuples)
    }

    /// Ordered tuples currently visible after applying offset/limit.
    pub fn windowed_tuples(&self) -> &[Tuple] {
        &self.windowed_tuples
    }

    /// Tuples that must be present locally to reproduce this paginated window.
    ///
    /// For offset-based pagination, the client must have the ordered prefix up to
    /// `offset + limit` so it can reapply the same windowing logic locally.
    /// When no limit is present, that means the full ordered input.
    pub fn sync_input_tuples(&self) -> &[Tuple] {
        let end = match self.limit {
            Some(limit) => self.offset.saturating_add(limit).min(self.all_tuples.len()),
            None => self.all_tuples.len(),
        };
        &self.all_tuples[..end]
    }

    fn sync_scope_provenance(&self) -> crate::query_manager::types::TupleProvenance {
        self.sync_input_tuples()
            .iter()
            .flat_map(|tuple| tuple.provenance().iter().copied())
            .collect()
    }
}

impl RowNode for LimitOffsetNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let old_tuples = std::mem::take(&mut self.windowed_tuples);

        // Apply changes to all_tuples
        for tuple in input.removed {
            self.all_tuples.retain(|t| t != &tuple);
        }

        // For added tuples, maintain order from input (assumed sorted)
        for tuple in input.added {
            self.all_tuples.push(tuple);
        }

        // For moved tuples, preserve tuple and update relative order by append semantics.
        for tuple in input.moved {
            if let Some(pos) = self.all_tuples.iter().position(|t| t == &tuple) {
                let existing = self.all_tuples.remove(pos);
                self.all_tuples.push(existing);
            }
        }

        // For updated tuples, update in place
        for (old_tuple, new_tuple) in input.updated {
            if let Some(pos) = self.all_tuples.iter().position(|t| t == &old_tuple) {
                self.all_tuples[pos] = new_tuple;
            }
        }

        // Recompute window
        self.recompute_tuple_window();
        self.dirty = false;

        // Return the delta for the window
        compute_tuple_delta(&old_tuples, &self.windowed_tuples)
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
        ])
    }

    fn make_tuple(id: ObjectId, n: i32, name: &str) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, &[Value::Integer(n), Value::Text(name.into())]).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    fn contains_id(tuples: &[Tuple], id: ObjectId) -> bool {
        tuples.iter().any(|t| t.ids().contains(&id))
    }

    fn get_windowed_ids(node: &LimitOffsetNode) -> Vec<ObjectId> {
        node.windowed_tuples.iter().map(|t| t.ids()[0]).collect()
    }

    fn make_limit_offset_node(limit: Option<usize>, offset: usize) -> LimitOffsetNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor, true);
        LimitOffsetNode::with_tuple_descriptor(tuple_desc, limit, offset)
    }

    #[test]
    fn limit_only() {
        let mut node = make_limit_offset_node(Some(2), 0);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            added: tuples,
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids.len(), 2);
        assert_eq!(windowed_ids[0], ids[0]);
        assert_eq!(windowed_ids[1], ids[1]);
    }

    #[test]
    fn offset_only() {
        let mut node = make_limit_offset_node(None, 2);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            added: tuples,
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 3);
        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids.len(), 3);
        assert_eq!(windowed_ids[0], ids[2]);
        assert_eq!(windowed_ids[1], ids[3]);
        assert_eq!(windowed_ids[2], ids[4]);
    }

    #[test]
    fn limit_and_offset() {
        let mut node = make_limit_offset_node(Some(2), 1);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            added: tuples,
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 2);
        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids.len(), 2);
        assert_eq!(windowed_ids[0], ids[1]);
        assert_eq!(windowed_ids[1], ids[2]);
    }

    #[test]
    fn removal_shifts_window() {
        let mut node = make_limit_offset_node(Some(2), 0);

        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        // Initial: [0, 1, 2, 3] -> window [0, 1]
        node.process(TupleDelta {
            added: tuples.clone(),
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids[0], ids[0]);
        assert_eq!(windowed_ids[1], ids[1]);

        // Remove first tuple: [1, 2, 3] -> window [1, 2]
        let result = node.process(TupleDelta {
            added: vec![],
            removed: vec![tuples[0].clone()],
            moved: vec![],
            updated: vec![],
        });

        assert_eq!(result.removed.len(), 1);
        assert!(contains_id(&result.removed, ids[0]));
        assert_eq!(result.added.len(), 1);
        assert!(contains_id(&result.added, ids[2])); // New tuple slides in
        assert!(result.moved.is_empty());

        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids.len(), 2);
        assert_eq!(windowed_ids[0], ids[1]);
        assert_eq!(windowed_ids[1], ids[2]);
    }

    #[test]
    fn offset_beyond_data() {
        let mut node = make_limit_offset_node(Some(10), 100);

        let id = ObjectId::new();
        let tuple = make_tuple(id, 1, "Row1");

        let delta = TupleDelta {
            added: vec![tuple],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        node.process(delta);

        assert!(node.windowed_tuples.is_empty());
    }

    #[test]
    fn insertion_before_window_does_not_mark_window_as_moved() {
        let mut node = make_limit_offset_node(Some(2), 1);
        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        node.process(TupleDelta {
            added: tuples[..3].to_vec(),
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        let delta = node.process(TupleDelta {
            added: vec![tuples[3].clone()],
            removed: vec![],
            moved: vec![tuples[0].clone()],
            updated: vec![],
        });

        assert!(delta.moved.is_empty());
    }

    #[test]
    fn ordered_input_insert_does_not_mark_existing_as_moved() {
        let mut node = make_limit_offset_node(None, 0);
        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let base: Vec<_> = ids
            .iter()
            .take(3)
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        node.process_with_ordered_input(&base);

        let inserted = make_tuple(ids[3], 99, "Inserted");
        let delta = node.process_with_ordered_input(&[
            inserted.clone(),
            base[0].clone(),
            base[1].clone(),
            base[2].clone(),
        ]);

        assert_eq!(delta.added.len(), 1);
        assert!(delta.removed.is_empty());
        assert!(delta.moved.is_empty());
    }

    #[test]
    fn ordered_input_remove_does_not_mark_following_as_moved() {
        let mut node = make_limit_offset_node(None, 0);
        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        node.process_with_ordered_input(&tuples);

        let delta = node.process_with_ordered_input(&[
            tuples[0].clone(),
            tuples[2].clone(),
            tuples[3].clone(),
        ]);

        assert_eq!(delta.removed.len(), 1);
        assert_eq!(delta.removed[0].first_id(), tuples[1].first_id());
        assert!(delta.added.is_empty());
        assert!(delta.moved.is_empty());
    }

    #[test]
    fn ordered_input_rotation_marks_only_reordered_tuple_as_moved() {
        let mut node = make_limit_offset_node(None, 0);
        let ids: Vec<_> = (0..3).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        node.process_with_ordered_input(&tuples);

        let delta = node.process_with_ordered_input(&[
            tuples[1].clone(),
            tuples[2].clone(),
            tuples[0].clone(),
        ]);

        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert_eq!(delta.moved.len(), 1);
        assert_eq!(delta.moved[0].first_id(), tuples[0].first_id());
    }
}
