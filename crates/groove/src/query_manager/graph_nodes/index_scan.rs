use std::collections::HashSet;
use std::ops::Bound;

use crate::object::ObjectId;
use crate::object_manager::ObjectManager;
use crate::query_manager::index::IndexState;
use crate::query_manager::types::IdDelta;

use super::IdNode;

/// Condition for index scan.
#[derive(Debug, Clone)]
pub enum ScanCondition {
    /// No condition - scan all entries (uses "_id" index).
    All,
    /// Exact match on key.
    Eq(Vec<u8>),
    /// Range scan with bounds (inclusive, exclusive, or unbounded).
    Range {
        min: Bound<Vec<u8>>,
        max: Bound<Vec<u8>>,
    },
}

/// Source node that scans an index.
/// Emits IdDelta based on the scan condition.
#[derive(Debug)]
pub struct IndexScanNode {
    /// Reference to the index state (borrowed from QueryManager).
    /// For now, we store the table/column info and access index externally.
    pub table: String,
    pub column: String,
    pub condition: ScanCondition,

    /// Current set of IDs matching the condition.
    current_ids: HashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl IndexScanNode {
    pub fn new(
        table: impl Into<String>,
        column: impl Into<String>,
        condition: ScanCondition,
    ) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            condition,
            current_ids: HashSet::new(),
            dirty: true,
        }
    }

    /// Scan the index and update current_ids.
    /// Returns the delta from previous state.
    pub fn scan(&mut self, index: &IndexState, om: &ObjectManager) -> IdDelta {
        let new_ids: HashSet<ObjectId> = match &self.condition {
            ScanCondition::All => index.scan_all(om).into_iter().collect(),
            ScanCondition::Eq(key) => index.lookup_exact(key, om).into_iter().collect(),
            ScanCondition::Range { min, max } => {
                index.range_scan(min, max, om).into_iter().collect()
            }
        };

        let added: HashSet<ObjectId> = new_ids.difference(&self.current_ids).copied().collect();
        let removed: HashSet<ObjectId> = self.current_ids.difference(&new_ids).copied().collect();

        self.current_ids = new_ids;
        self.dirty = false;

        IdDelta { added, removed }
    }
}

impl IdNode for IndexScanNode {
    fn process(&mut self) -> IdDelta {
        // Note: This requires external index access.
        // In practice, QueryManager will call scan() with the index.
        // This method returns empty delta as a fallback.
        self.dirty = false;
        IdDelta::new()
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
    use crate::query_manager::index::IndexState;

    #[test]
    fn scan_all_returns_all_rows() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "_id");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(row1.0.as_bytes(), row1, &mut om).unwrap();
        index.insert(row2.0.as_bytes(), row2, &mut om).unwrap();
        index.insert(row3.0.as_bytes(), row3, &mut om).unwrap();

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All);
        let delta = node.scan(&index, &om);

        assert_eq!(delta.added.len(), 3);
        assert!(delta.added.contains(&row1));
        assert!(delta.added.contains(&row2));
        assert!(delta.added.contains(&row3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn scan_eq_returns_matching_rows() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1, &mut om).unwrap();
        index.insert(b"bob@example.com", row2, &mut om).unwrap();

        let mut node = IndexScanNode::new(
            "users",
            "email",
            ScanCondition::Eq(b"alice@example.com".to_vec()),
        );
        let delta = node.scan(&index, &om);

        assert_eq!(delta.added.len(), 1);
        assert!(delta.added.contains(&row1));
    }

    #[test]
    fn scan_range_returns_rows_in_range() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "score");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(&10i32.to_le_bytes(), row1, &mut om).unwrap();
        index.insert(&20i32.to_le_bytes(), row2, &mut om).unwrap();
        index.insert(&30i32.to_le_bytes(), row3, &mut om).unwrap();

        let mut node = IndexScanNode::new(
            "users",
            "score",
            ScanCondition::Range {
                min: Bound::Included(15i32.to_le_bytes().to_vec()),
                max: Bound::Included(25i32.to_le_bytes().to_vec()),
            },
        );
        let delta = node.scan(&index, &om);

        assert_eq!(delta.added.len(), 1);
        assert!(delta.added.contains(&row2));
    }

    #[test]
    fn rescan_detects_changes() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "_id");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(row1.0.as_bytes(), row1, &mut om).unwrap();

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All);
        let delta1 = node.scan(&index, &om);
        assert_eq!(delta1.added.len(), 1);
        assert!(delta1.added.contains(&row1));

        // Add another row
        index.insert(row2.0.as_bytes(), row2, &mut om).unwrap();
        let delta2 = node.scan(&index, &om);
        assert_eq!(delta2.added.len(), 1);
        assert!(delta2.added.contains(&row2));
        assert!(delta2.removed.is_empty());

        // Remove first row
        index.remove(row1.0.as_bytes(), row1, &mut om).unwrap();
        let delta3 = node.scan(&index, &om);
        assert!(delta3.added.is_empty());
        assert_eq!(delta3.removed.len(), 1);
        assert!(delta3.removed.contains(&row1));
    }
}
