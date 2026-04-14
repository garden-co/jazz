//! ExistsOutputNode - terminal node for boolean EXISTS checks.
//!
//! Used by policy evaluation graphs to determine if any rows match a condition.
//! Returns true if at least one row has been added (and not all removed).

use ahash::AHashSet;

use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta};

use super::RowNode;

/// A terminal node that returns a boolean: does at least one row exist?
///
/// Used for policy evaluation graphs where we need to know if a condition
/// matches any rows, not the rows themselves.
#[derive(Debug)]
pub struct ExistsOutputNode {
    /// The input row descriptor (passed through unchanged).
    descriptor: RowDescriptor,
    /// Count of rows currently in the result set.
    count: usize,
    /// Current tuples (for RowNode trait compliance).
    current_tuples: AHashSet<Tuple>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl ExistsOutputNode {
    /// Create a new ExistsOutputNode.
    pub fn new(descriptor: RowDescriptor) -> Self {
        Self {
            descriptor,
            count: 0,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Returns true if at least one row exists.
    pub fn exists(&self) -> bool {
        self.count > 0
    }

    /// Get the current row count.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl RowNode for ExistsOutputNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Update count based on added/removed tuples
        self.count += input.added.len();
        self.count = self.count.saturating_sub(input.removed.len());

        // Update current tuples set
        for tuple in &input.added {
            self.current_tuples.insert(tuple.clone());
        }
        for tuple in &input.removed {
            self.current_tuples.remove(tuple);
        }
        // Handle updates: remove old, add new
        for (old, new) in &input.updated {
            self.current_tuples.remove(old);
            self.current_tuples.insert(new.clone());
        }

        self.dirty = false;

        // Pass through for potential chaining (though usually this is terminal)
        input
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_tuple(id: ObjectId) -> Tuple {
        Tuple::new(vec![TupleElement::Row {
            id,
            content: vec![].into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    #[test]
    fn test_empty_starts_not_exists() {
        let node = ExistsOutputNode::new(test_descriptor());
        assert!(!node.exists());
    }

    #[test]
    fn test_add_row_makes_exists_true() {
        let mut node = ExistsOutputNode::new(test_descriptor());

        let id = ObjectId::new();
        let delta = TupleDelta {
            added: vec![make_tuple(id)],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        node.process(delta);

        assert!(node.exists());
        assert_eq!(node.count(), 1);
    }

    #[test]
    fn test_remove_all_rows_makes_exists_false() {
        let mut node = ExistsOutputNode::new(test_descriptor());

        let id = ObjectId::new();
        let tuple = make_tuple(id);

        // Add one
        node.process(TupleDelta {
            added: vec![tuple.clone()],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        assert!(node.exists());

        // Remove it
        node.process(TupleDelta {
            added: vec![],
            removed: vec![tuple],
            moved: vec![],
            updated: vec![],
        });
        assert!(!node.exists());
        assert_eq!(node.count(), 0);
    }

    #[test]
    fn test_multiple_rows() {
        let mut node = ExistsOutputNode::new(test_descriptor());

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();

        node.process(TupleDelta {
            added: vec![make_tuple(id1), make_tuple(id2)],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        assert!(node.exists());
        assert_eq!(node.count(), 2);

        // Remove one - should still exist
        node.process(TupleDelta {
            added: vec![],
            removed: vec![make_tuple(id1)],
            moved: vec![],
            updated: vec![],
        });

        assert!(node.exists());
        assert_eq!(node.count(), 1);
    }

    #[test]
    fn test_count_cannot_go_negative() {
        let mut node = ExistsOutputNode::new(test_descriptor());

        // Try to remove from empty
        let id = ObjectId::new();
        node.process(TupleDelta {
            added: vec![],
            removed: vec![make_tuple(id)],
            moved: vec![],
            updated: vec![],
        });

        // Count should be 0, not negative
        assert_eq!(node.count(), 0);
        assert!(!node.exists());
    }
}
