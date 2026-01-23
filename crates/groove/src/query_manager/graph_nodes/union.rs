use std::collections::HashSet;

use crate::query_manager::types::{Tuple, TupleDelta, TupleDescriptor};

use super::TransformNode;

/// Union node for OR conditions.
/// Pure transform node that merges tuple sets from multiple inputs.
#[derive(Debug)]
pub struct UnionNode {
    /// Output tuple descriptor.
    output_tuple_descriptor: Option<TupleDescriptor>,
    /// Current union of all input tuples.
    current_tuples: HashSet<Tuple>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl UnionNode {
    pub fn new() -> Self {
        Self {
            output_tuple_descriptor: None,
            current_tuples: HashSet::new(),
            dirty: false,
        }
    }

    /// Create a UnionNode with TupleDescriptor.
    pub fn with_tuple_descriptor(tuple_descriptor: TupleDescriptor) -> Self {
        Self {
            output_tuple_descriptor: Some(tuple_descriptor),
            current_tuples: HashSet::new(),
            dirty: false,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> Option<&TupleDescriptor> {
        self.output_tuple_descriptor.as_ref()
    }
}

impl Default for UnionNode {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformNode for UnionNode {
    /// Compute the union of multiple input tuple sets.
    /// For union: a tuple is present if it's in ANY input.
    fn process(&mut self, inputs: &[&HashSet<Tuple>]) -> TupleDelta {
        // Compute new union of tuples
        let mut new_tuples = HashSet::new();
        for tuples in inputs {
            new_tuples.extend(tuples.iter().cloned());
        }

        // Compute delta
        let added: Vec<Tuple> = new_tuples
            .difference(&self.current_tuples)
            .cloned()
            .collect();
        let removed: Vec<Tuple> = self
            .current_tuples
            .difference(&new_tuples)
            .cloned()
            .collect();

        // Update state
        self.current_tuples = new_tuples;
        self.dirty = false;

        TupleDelta {
            added,
            removed,
            updated: vec![],
            pending: false,
        }
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
    use crate::object::ObjectId;
    use uuid::Uuid;

    fn contains_id(tuples: &[Tuple], id: ObjectId) -> bool {
        tuples.iter().any(|t| t.ids().contains(&id))
    }

    fn tuple_set_contains(set: &HashSet<Tuple>, id: ObjectId) -> bool {
        set.iter().any(|t| t.ids().contains(&id))
    }

    #[test]
    fn union_of_two_sets() {
        let id1 = ObjectId::from_uuid(Uuid::from_u128(1));
        let id2 = ObjectId::from_uuid(Uuid::from_u128(2));
        let id3 = ObjectId::from_uuid(Uuid::from_u128(3));

        let set1: HashSet<Tuple> = [Tuple::from_id(id1), Tuple::from_id(id2)]
            .into_iter()
            .collect();
        let set2: HashSet<Tuple> = [Tuple::from_id(id2), Tuple::from_id(id3)]
            .into_iter()
            .collect();

        let mut node = UnionNode::new();
        let delta = node.process(&[&set1, &set2]);

        assert_eq!(delta.added.len(), 3);
        assert!(contains_id(&delta.added, id1));
        assert!(contains_id(&delta.added, id2));
        assert!(contains_id(&delta.added, id3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn union_detects_additions() {
        let id1 = ObjectId::from_uuid(Uuid::from_u128(1));
        let id2 = ObjectId::from_uuid(Uuid::from_u128(2));
        let id3 = ObjectId::from_uuid(Uuid::from_u128(3));

        let set1: HashSet<Tuple> = [Tuple::from_id(id1)].into_iter().collect();
        let set2: HashSet<Tuple> = [Tuple::from_id(id2)].into_iter().collect();

        let mut node = UnionNode::new();
        node.process(&[&set1, &set2]);

        // Add id3 to set1
        let new_set1: HashSet<Tuple> = [Tuple::from_id(id1), Tuple::from_id(id3)]
            .into_iter()
            .collect();
        let delta = node.process(&[&new_set1, &set2]);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, id3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn union_detects_removals() {
        let id1 = ObjectId::from_uuid(Uuid::from_u128(1));
        let id2 = ObjectId::from_uuid(Uuid::from_u128(2));

        let set1: HashSet<Tuple> = [Tuple::from_id(id1), Tuple::from_id(id2)]
            .into_iter()
            .collect();
        let set2: HashSet<Tuple> = HashSet::new();

        let mut node = UnionNode::new();
        node.process(&[&set1, &set2]);

        // Remove id1 from set1
        let new_set1: HashSet<Tuple> = [Tuple::from_id(id2)].into_iter().collect();
        let delta = node.process(&[&new_set1, &set2]);

        assert!(delta.added.is_empty());
        assert_eq!(delta.removed.len(), 1);
        assert!(contains_id(&delta.removed, id1));
    }

    #[test]
    fn union_keeps_id_if_in_any_input() {
        let id1 = ObjectId::from_uuid(Uuid::from_u128(1));

        let set1: HashSet<Tuple> = [Tuple::from_id(id1)].into_iter().collect();
        let set2: HashSet<Tuple> = [Tuple::from_id(id1)].into_iter().collect();

        let mut node = UnionNode::new();
        node.process(&[&set1, &set2]);

        // Remove id1 from set1 but keep in set2
        let new_set1: HashSet<Tuple> = HashSet::new();
        let delta = node.process(&[&new_set1, &set2]);

        // id1 should still be present (in set2)
        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(tuple_set_contains(node.current_tuples(), id1));
    }
}
