use ahash::AHashSet;

use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta, TupleDescriptor};

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
        self.windowed_tuples = self.all_tuples[start..end].to_vec();
        self.current_tuples = self.windowed_tuples.iter().cloned().collect();
    }

    /// Compute the delta between old and new tuple window.
    fn compute_tuple_delta(&self, old_tuples: &[Tuple], new_tuples: &[Tuple]) -> TupleDelta {
        let mut delta = TupleDelta::new();
        let old_ids: std::collections::HashSet<Vec<crate::object::ObjectId>> =
            old_tuples.iter().map(|t| t.ids()).collect();
        let new_ids: std::collections::HashSet<Vec<crate::object::ObjectId>> =
            new_tuples.iter().map(|t| t.ids()).collect();

        // Find removed tuples (in old but not in new)
        for old in old_tuples {
            if !new_ids.contains(&old.ids()) {
                delta.removed.push(old.clone());
            }
        }

        // Find added tuples (in new but not in old)
        for new in new_tuples {
            if !old_ids.contains(&new.ids()) {
                delta.added.push(new.clone());
            }
        }

        // Find moved tuples (same IDs, different index)
        let old_pos: std::collections::HashMap<Vec<crate::object::ObjectId>, usize> = old_tuples
            .iter()
            .enumerate()
            .map(|(idx, t)| (t.ids(), idx))
            .collect();
        for (new_idx, new_tuple) in new_tuples.iter().enumerate() {
            let ids = new_tuple.ids();
            if let Some(old_idx) = old_pos.get(&ids)
                && old_idx != &new_idx
            {
                delta.moved.push(new_tuple.clone());
            }
        }

        // Find updated tuples (same IDs but different content)
        for new in new_tuples {
            if let Some(old) = old_tuples.iter().find(|t| *t == new) {
                // Check if content changed (since == only compares IDs)
                if has_tuple_content_changed(old, new) {
                    delta.updated.push((old.clone(), new.clone()));
                }
            }
        }

        delta
    }

    /// Rebuild state from a full ordered input (e.g. upstream SortNode output).
    pub fn process_with_ordered_input(&mut self, ordered_tuples: &[Tuple]) -> TupleDelta {
        let old_tuples = self.windowed_tuples.clone();
        self.all_tuples = ordered_tuples.to_vec();
        self.recompute_tuple_window();
        self.dirty = false;
        self.compute_tuple_delta(&old_tuples, &self.windowed_tuples)
    }

    /// Ordered tuples currently visible after applying offset/limit.
    pub fn windowed_tuples(&self) -> &[Tuple] {
        &self.windowed_tuples
    }
}

/// Check if tuple content changed (for tuples with same IDs).
fn has_tuple_content_changed(old: &Tuple, new: &Tuple) -> bool {
    old.iter()
        .zip(new.iter())
        .any(|(o, n)| match (o.content(), n.content()) {
            (Some(oc), Some(nc)) => oc != nc,
            _ => false,
        })
}

impl RowNode for LimitOffsetNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let old_tuples = self.windowed_tuples.clone();

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
        self.compute_tuple_delta(&old_tuples, &self.windowed_tuples)
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
}
