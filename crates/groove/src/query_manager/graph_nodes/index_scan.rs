use std::collections::HashSet;
use std::ops::Bound;

use crate::object::ObjectId;
use crate::query_manager::types::{RowDescriptor, Tuple, TupleDelta, TupleDescriptor};

use super::{SourceContext, SourceNode};

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
/// Emits TupleDelta with length-1 tuples based on the scan condition.
#[derive(Debug)]
pub struct IndexScanNode {
    /// Reference to the index state (borrowed from QueryManager).
    /// For now, we store the table/column info and access index externally.
    pub table: String,
    pub column: String,
    pub condition: ScanCondition,

    /// Output tuple descriptor (single element, unmaterialized).
    output_descriptor: TupleDescriptor,

    /// Current set of tuples (length-1) matching the condition.
    current_tuples: HashSet<Tuple>,
    /// Last scanned IDs (for computing deltas).
    last_scanned_ids: HashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl IndexScanNode {
    /// Create a new index scan node.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `column` - Column to scan on
    /// * `condition` - Scan condition
    /// * `row_descriptor` - Row descriptor for the table
    pub fn new(
        table: impl Into<String>,
        column: impl Into<String>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        let table = table.into();
        // Output is a single-element tuple, unmaterialized (ID-only)
        let output_descriptor = TupleDescriptor::single(&table, row_descriptor);
        Self {
            table,
            column: column.into(),
            condition,
            output_descriptor,
            current_tuples: HashSet::new(),
            last_scanned_ids: HashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }
}

impl SourceNode for IndexScanNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        let key = (self.table.clone(), self.column.clone());
        let new_ids: HashSet<ObjectId> = if let Some(index) = ctx.indices.get(&key) {
            match &self.condition {
                ScanCondition::All => index.scan_all(ctx.om).into_iter().collect(),
                ScanCondition::Eq(k) => index.lookup_exact(k, ctx.om).into_iter().collect(),
                ScanCondition::Range { min, max } => {
                    index.range_scan(min, max, ctx.om).into_iter().collect()
                }
            }
        } else {
            HashSet::new()
        };

        let added: HashSet<ObjectId> = new_ids
            .difference(&self.last_scanned_ids)
            .copied()
            .collect();
        let removed: HashSet<ObjectId> = self
            .last_scanned_ids
            .difference(&new_ids)
            .copied()
            .collect();

        // Update state
        self.last_scanned_ids = new_ids;
        self.current_tuples = self
            .last_scanned_ids
            .iter()
            .map(|&id| Tuple::from_id(id))
            .collect();
        self.dirty = false;

        // Return TupleDelta with length-1 tuples
        TupleDelta {
            added: added.into_iter().map(Tuple::from_id).collect(),
            removed: removed.into_iter().map(Tuple::from_id).collect(),
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
    use crate::object_manager::ObjectManager;
    use crate::query_manager::index::IndexState;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};
    use std::collections::HashMap;

    fn make_ctx<'a>(
        indices: &'a HashMap<(String, String), IndexState>,
        om: &'a ObjectManager,
    ) -> SourceContext<'a> {
        SourceContext { indices, om }
    }

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("_id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    /// Helper to check if delta contains a tuple with given ID.
    fn contains_id(tuples: &[Tuple], id: ObjectId) -> bool {
        tuples.iter().any(|t| t.ids().contains(&id))
    }

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

        let mut indices = HashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices, &om);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 3);
        assert!(contains_id(&delta.added, row1));
        assert!(contains_id(&delta.added, row2));
        assert!(contains_id(&delta.added, row3));
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

        let mut indices = HashMap::new();
        indices.insert(("users".to_string(), "email".to_string()), index);

        let mut node = IndexScanNode::new(
            "users",
            "email",
            ScanCondition::Eq(b"alice@example.com".to_vec()),
            test_descriptor(),
        );
        let ctx = make_ctx(&indices, &om);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row1));
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

        let mut indices = HashMap::new();
        indices.insert(("users".to_string(), "score".to_string()), index);

        let mut node = IndexScanNode::new(
            "users",
            "score",
            ScanCondition::Range {
                min: Bound::Included(15i32.to_le_bytes().to_vec()),
                max: Bound::Included(25i32.to_le_bytes().to_vec()),
            },
            test_descriptor(),
        );
        let ctx = make_ctx(&indices, &om);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row2));
    }

    #[test]
    fn rescan_detects_changes() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "_id");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(row1.0.as_bytes(), row1, &mut om).unwrap();

        let mut indices = HashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices, &om);
        let delta1 = node.scan(&ctx);
        assert_eq!(delta1.added.len(), 1);
        assert!(contains_id(&delta1.added, row1));

        // Add another row
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .insert(row2.0.as_bytes(), row2, &mut om)
            .unwrap();
        let ctx = make_ctx(&indices, &om);
        let delta2 = node.scan(&ctx);
        assert_eq!(delta2.added.len(), 1);
        assert!(contains_id(&delta2.added, row2));
        assert!(delta2.removed.is_empty());

        // Remove first row
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .remove(row1.0.as_bytes(), row1, &mut om)
            .unwrap();
        let ctx = make_ctx(&indices, &om);
        let delta3 = node.scan(&ctx);
        assert!(delta3.added.is_empty());
        assert_eq!(delta3.removed.len(), 1);
        assert!(contains_id(&delta3.removed, row1));
    }

    #[test]
    fn output_descriptor_has_unmaterialized_state() {
        let desc = test_descriptor();
        let node = IndexScanNode::new("users", "_id", ScanCondition::All, desc);
        let output = node.output_tuple_descriptor();

        // Should be single element, unmaterialized
        assert_eq!(output.element_count(), 1);
        assert!(!output.materialization().is_materialized(0));
    }
}
