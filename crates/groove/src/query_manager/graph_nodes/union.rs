use std::collections::HashSet;

use crate::object::ObjectId;
use crate::query_manager::types::IdDelta;

use super::IdNode;

/// Union node for OR conditions.
/// Merges IdDeltas from multiple sources at the ID level.
#[derive(Debug)]
pub struct UnionNode {
    /// Current union of all input IDs.
    current_ids: HashSet<ObjectId>,
    /// Input deltas waiting to be processed.
    pending_deltas: Vec<IdDelta>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl UnionNode {
    pub fn new() -> Self {
        Self {
            current_ids: HashSet::new(),
            pending_deltas: Vec::new(),
            dirty: false,
        }
    }

    /// Add an input delta to be processed.
    pub fn add_input(&mut self, delta: IdDelta) {
        if !delta.is_empty() {
            self.pending_deltas.push(delta);
            self.dirty = true;
        }
    }

    /// Compute the union of multiple input ID sets.
    /// For union: a row is present if it's in ANY input.
    pub fn process_inputs(&mut self, input_current_ids: &[&HashSet<ObjectId>]) -> IdDelta {
        // Compute new union
        let mut new_ids = HashSet::new();
        for ids in input_current_ids {
            new_ids.extend(ids.iter().copied());
        }

        let added: HashSet<ObjectId> = new_ids.difference(&self.current_ids).copied().collect();
        let removed: HashSet<ObjectId> = self.current_ids.difference(&new_ids).copied().collect();

        self.current_ids = new_ids;
        self.dirty = false;
        self.pending_deltas.clear();

        IdDelta { added, removed }
    }
}

impl Default for UnionNode {
    fn default() -> Self {
        Self::new()
    }
}

impl IdNode for UnionNode {
    fn process(&mut self) -> IdDelta {
        // Process pending deltas incrementally
        let mut result = IdDelta::new();

        for delta in self.pending_deltas.drain(..) {
            // For union: add if not already present, remove only if in removed set
            for id in delta.added {
                if self.current_ids.insert(id) {
                    result.added.insert(id);
                }
            }
            // Note: for union, we can't remove just because one input removed it
            // The row might still be present in another input
            // This is handled properly in process_inputs which takes all current sets
        }

        self.dirty = false;
        result
    }

    fn current_ids(&self) -> &HashSet<ObjectId> {
        &self.current_ids
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
    use uuid::Uuid;

    #[test]
    fn union_of_two_sets() {
        let id1 = ObjectId(Uuid::from_u128(1));
        let id2 = ObjectId(Uuid::from_u128(2));
        let id3 = ObjectId(Uuid::from_u128(3));

        let set1: HashSet<ObjectId> = [id1, id2].into_iter().collect();
        let set2: HashSet<ObjectId> = [id2, id3].into_iter().collect();

        let mut node = UnionNode::new();
        let delta = node.process_inputs(&[&set1, &set2]);

        assert_eq!(delta.added.len(), 3);
        assert!(delta.added.contains(&id1));
        assert!(delta.added.contains(&id2));
        assert!(delta.added.contains(&id3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn union_detects_additions() {
        let id1 = ObjectId(Uuid::from_u128(1));
        let id2 = ObjectId(Uuid::from_u128(2));
        let id3 = ObjectId(Uuid::from_u128(3));

        let set1: HashSet<ObjectId> = [id1].into_iter().collect();
        let set2: HashSet<ObjectId> = [id2].into_iter().collect();

        let mut node = UnionNode::new();
        node.process_inputs(&[&set1, &set2]);

        // Add id3 to set1
        let new_set1: HashSet<ObjectId> = [id1, id3].into_iter().collect();
        let delta = node.process_inputs(&[&new_set1, &set2]);

        assert_eq!(delta.added.len(), 1);
        assert!(delta.added.contains(&id3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn union_detects_removals() {
        let id1 = ObjectId(Uuid::from_u128(1));
        let id2 = ObjectId(Uuid::from_u128(2));

        let set1: HashSet<ObjectId> = [id1, id2].into_iter().collect();
        let set2: HashSet<ObjectId> = HashSet::new();

        let mut node = UnionNode::new();
        node.process_inputs(&[&set1, &set2]);

        // Remove id1 from set1
        let new_set1: HashSet<ObjectId> = [id2].into_iter().collect();
        let delta = node.process_inputs(&[&new_set1, &set2]);

        assert!(delta.added.is_empty());
        assert_eq!(delta.removed.len(), 1);
        assert!(delta.removed.contains(&id1));
    }

    #[test]
    fn union_keeps_id_if_in_any_input() {
        let id1 = ObjectId(Uuid::from_u128(1));

        let set1: HashSet<ObjectId> = [id1].into_iter().collect();
        let set2: HashSet<ObjectId> = [id1].into_iter().collect();

        let mut node = UnionNode::new();
        node.process_inputs(&[&set1, &set2]);

        // Remove id1 from set1 but keep in set2
        let new_set1: HashSet<ObjectId> = HashSet::new();
        let delta = node.process_inputs(&[&new_set1, &set2]);

        // id1 should still be present (in set2)
        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(node.current_ids.contains(&id1));
    }
}
