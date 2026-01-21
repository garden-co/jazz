use std::collections::HashSet;

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
    current_tuples: HashSet<Tuple>,
    dirty: bool,
}

impl LimitOffsetNode {
    /// Create a LimitOffsetNode with RowDescriptor (backward compatible).
    pub fn new(descriptor: RowDescriptor, limit: Option<usize>, offset: usize) -> Self {
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", descriptor.clone(), true);
        Self {
            descriptor,
            output_tuple_descriptor,
            limit,
            offset,
            all_tuples: Vec::new(),
            windowed_tuples: Vec::new(),
            current_tuples: HashSet::new(),
            dirty: true,
        }
    }

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
            current_tuples: HashSet::new(),
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
    fn compute_tuple_delta(
        &self,
        old_tuples: &[Tuple],
        new_tuples: &[Tuple],
        pending: bool,
    ) -> TupleDelta {
        let mut delta = TupleDelta::new();
        delta.pending = pending;

        // Find removed tuples (in old but not in new)
        for old in old_tuples {
            if !new_tuples.iter().any(|t| t == old) {
                delta.removed.push(old.clone());
            }
        }

        // Find added tuples (in new but not in old)
        for new in new_tuples {
            if !old_tuples.iter().any(|t| t == new) {
                delta.added.push(new.clone());
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
        let pending = input.pending;

        // Apply changes to all_tuples
        for tuple in input.removed {
            self.all_tuples.retain(|t| t != &tuple);
        }

        // For added tuples, maintain order from input (assumed sorted)
        for tuple in input.added {
            self.all_tuples.push(tuple);
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
        self.compute_tuple_delta(&old_tuples, &self.windowed_tuples, pending)
    }

    fn current_tuples(&self) -> &HashSet<Tuple> {
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

    #[test]
    fn limit_only() {
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 0);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            pending: false,
            added: tuples,
            removed: vec![],
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
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, None, 2);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            pending: false,
            added: tuples,
            removed: vec![],
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
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 1);

        let ids: Vec<_> = (0..5).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        let delta = TupleDelta {
            pending: false,
            added: tuples,
            removed: vec![],
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
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(2), 0);

        let ids: Vec<_> = (0..4).map(|_| ObjectId::new()).collect();
        let tuples: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| make_tuple(*id, i as i32, &format!("Row{}", i)))
            .collect();

        // Initial: [0, 1, 2, 3] -> window [0, 1]
        node.process(TupleDelta {
            pending: false,
            added: tuples.clone(),
            removed: vec![],
            updated: vec![],
        });
        let windowed_ids = get_windowed_ids(&node);
        assert_eq!(windowed_ids[0], ids[0]);
        assert_eq!(windowed_ids[1], ids[1]);

        // Remove first tuple: [1, 2, 3] -> window [1, 2]
        let result = node.process(TupleDelta {
            pending: false,
            added: vec![],
            removed: vec![tuples[0].clone()],
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
        let descriptor = test_descriptor();
        let mut node = LimitOffsetNode::new(descriptor, Some(10), 100);

        let id = ObjectId::new();
        let tuple = make_tuple(id, 1, "Row1");

        let delta = TupleDelta {
            pending: false,
            added: vec![tuple],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        assert!(node.windowed_tuples.is_empty());
    }
}
