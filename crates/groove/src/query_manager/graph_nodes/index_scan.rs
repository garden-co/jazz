use ahash::AHashSet;

use crate::object::ObjectId;
use crate::query_manager::index::ScanCondition;
use crate::query_manager::types::{
    ColumnName, RowDescriptor, TableName, Tuple, TupleDelta, TupleDescriptor,
};

use super::{SourceContext, SourceNode};

/// Source node that scans an index.
/// Emits TupleDelta with length-1 tuples based on the scan condition.
#[derive(Debug)]
pub struct IndexScanNode {
    /// Reference to the index state (borrowed from QueryManager).
    /// For now, we store the table/column info and access index externally.
    pub table: TableName,
    pub column: ColumnName,
    pub condition: ScanCondition,

    /// Output tuple descriptor (single element, unmaterialized).
    output_descriptor: TupleDescriptor,

    /// Current set of tuples (length-1) matching the condition.
    current_tuples: AHashSet<Tuple>,
    /// Last scanned IDs (for computing deltas).
    last_scanned_ids: AHashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,

    /// Last delta epoch processed (None = never scanned, uses slow path).
    last_delta_epoch: Option<u64>,
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
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        let table = table.into();
        // Output is a single-element tuple, unmaterialized (ID-only)
        let output_descriptor = TupleDescriptor::single(table.as_str(), row_descriptor);
        Self {
            table,
            column: column.into(),
            condition,
            output_descriptor,
            current_tuples: AHashSet::new(),
            last_scanned_ids: AHashSet::new(),
            dirty: true,
            last_delta_epoch: None,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }
}

impl SourceNode for IndexScanNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        let key = (
            self.table.as_str().to_string(),
            self.column.as_str().to_string(),
        );

        let Some(index) = ctx.indices.get(&key) else {
            self.dirty = false;
            return TupleDelta::new();
        };

        // Check if we can use the fast path (incremental deltas)
        // Requirements:
        // 1. We've done at least one scan (last_delta_epoch is Some)
        // 2. The epoch matches (no deltas were cleared since our last scan)
        let can_use_deltas =
            self.last_delta_epoch == Some(index.delta_epoch()) && self.last_delta_epoch.is_some();

        let (added, removed) = if can_use_deltas {
            // FAST PATH: Use deltas directly from the index
            let (delta_added, delta_removed) = index.get_deltas(&self.condition);

            // Filter to only IDs not already tracked (avoid double-counting)
            let added_ids: Vec<ObjectId> = delta_added
                .into_iter()
                .filter(|id| !self.last_scanned_ids.contains(id))
                .collect();
            let removed_ids: Vec<ObjectId> = delta_removed
                .into_iter()
                .filter(|id| self.last_scanned_ids.contains(id))
                .collect();

            // Update state incrementally
            for &id in &added_ids {
                self.last_scanned_ids.insert(id);
                self.current_tuples.insert(Tuple::from_id(id));
            }
            for &id in &removed_ids {
                self.last_scanned_ids.remove(&id);
                self.current_tuples.remove(&Tuple::from_id(id));
            }

            (added_ids, removed_ids)
        } else {
            // SLOW PATH: Full scan (first scan or epoch mismatch)
            let new_ids: AHashSet<ObjectId> = match &self.condition {
                ScanCondition::All => index.scan_all().into_iter().collect(),
                ScanCondition::Eq(k) => index.lookup_exact(k).into_iter().collect(),
                ScanCondition::Range { min, max } => {
                    index.range_scan(min, max).into_iter().collect()
                }
            };

            let added: Vec<ObjectId> = new_ids
                .difference(&self.last_scanned_ids)
                .copied()
                .collect();
            let removed: Vec<ObjectId> = self
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

            // Record the epoch so we can use fast path next time
            self.last_delta_epoch = Some(index.delta_epoch());

            (added, removed)
        };

        self.dirty = false;

        // Return TupleDelta with length-1 tuples
        TupleDelta {
            added: added.into_iter().map(Tuple::from_id).collect(),
            removed: removed.into_iter().map(Tuple::from_id).collect(),
            updated: vec![],
            pending: false,
        }
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
    use crate::query_manager::index::BTreeIndex;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};
    use ahash::AHashMap;
    use std::ops::Bound;

    fn make_ctx(indices: &AHashMap<(String, String), BTreeIndex>) -> SourceContext<'_> {
        SourceContext { indices }
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
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None); // Initialize empty index
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(row1.uuid().as_bytes(), row1).unwrap();
        index.insert(row2.uuid().as_bytes(), row2).unwrap();
        index.insert(row3.uuid().as_bytes(), row3).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 3);
        assert!(contains_id(&delta.added, row1));
        assert!(contains_id(&delta.added, row2));
        assert!(contains_id(&delta.added, row3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn scan_eq_returns_matching_rows() {
        let mut index = BTreeIndex::new("users", "email");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1).unwrap();
        index.insert(b"bob@example.com", row2).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "email".to_string()), index);

        let mut node = IndexScanNode::new(
            "users",
            "email",
            ScanCondition::Eq(b"alice@example.com".to_vec()),
            test_descriptor(),
        );
        let ctx = make_ctx(&indices);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row1));
    }

    #[test]
    fn scan_range_returns_rows_in_range() {
        let mut index = BTreeIndex::new("users", "score");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(&10i32.to_le_bytes(), row1).unwrap();
        index.insert(&20i32.to_le_bytes(), row2).unwrap();
        index.insert(&30i32.to_le_bytes(), row3).unwrap();

        let mut indices = AHashMap::new();
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
        let ctx = make_ctx(&indices);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row2));
    }

    #[test]
    fn rescan_detects_changes() {
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(row1.uuid().as_bytes(), row1).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices);
        let delta1 = node.scan(&ctx);
        assert_eq!(delta1.added.len(), 1);
        assert!(contains_id(&delta1.added, row1));

        // Simulate end of process cycle: clear deltas
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .clear_deltas();

        // Add another row
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .insert(row2.uuid().as_bytes(), row2)
            .unwrap();
        let ctx = make_ctx(&indices);
        let delta2 = node.scan(&ctx);
        assert_eq!(delta2.added.len(), 1);
        assert!(contains_id(&delta2.added, row2));
        assert!(delta2.removed.is_empty());

        // Simulate end of process cycle: clear deltas
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .clear_deltas();

        // Remove first row
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .remove(row1.uuid().as_bytes(), row1)
            .unwrap();
        let ctx = make_ctx(&indices);
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

    // ========================================================================
    // Delta/incremental scan tests
    // ========================================================================

    #[test]
    fn full_scan_on_first_call() {
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        index.insert(row1.uuid().as_bytes(), row1).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());

        // First scan should use slow path (last_delta_epoch is None)
        assert!(node.last_delta_epoch.is_none());

        let ctx = make_ctx(&indices);
        let delta = node.scan(&ctx);

        // After first scan, epoch should be recorded
        assert!(node.last_delta_epoch.is_some());
        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row1));
    }

    #[test]
    fn incremental_scan_uses_deltas() {
        // Test the fast path: multiple inserts within ONE process cycle (before clear_deltas)
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        // Insert first row
        index.insert(row1.uuid().as_bytes(), row1).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices);

        // First scan (slow path - no prior epoch recorded)
        let delta1 = node.scan(&ctx);
        assert_eq!(delta1.added.len(), 1);
        assert!(contains_id(&delta1.added, row1));

        // Insert more rows WITHOUT clearing deltas (same process cycle)
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .insert(row2.uuid().as_bytes(), row2)
            .unwrap();

        // Second scan uses fast path: epochs match, deltas available
        let ctx = make_ctx(&indices);
        let delta2 = node.scan(&ctx);

        // Fast path sees row1 (still in deltas) AND row2
        // But row1 is already in last_scanned_ids, so only row2 is "added"
        assert_eq!(delta2.added.len(), 1);
        assert!(contains_id(&delta2.added, row2));

        // Insert third row (still same epoch, no clear)
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .insert(row3.uuid().as_bytes(), row3)
            .unwrap();

        // Third scan also uses fast path
        let ctx = make_ctx(&indices);
        let delta3 = node.scan(&ctx);

        // Only row3 is new
        assert_eq!(delta3.added.len(), 1);
        assert!(contains_id(&delta3.added, row3));
    }

    #[test]
    fn full_scan_on_epoch_mismatch() {
        let mut index = BTreeIndex::new("users", "_id");
        index.process_meta_load(None);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        index.insert(row1.uuid().as_bytes(), row1).unwrap();

        let mut indices = AHashMap::new();
        indices.insert(("users".to_string(), "_id".to_string()), index);

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&indices);

        // First scan
        let delta1 = node.scan(&ctx);
        assert_eq!(delta1.added.len(), 1);
        let initial_epoch = node.last_delta_epoch;
        assert_eq!(initial_epoch, Some(0));

        // Clear deltas (simulating end of process cycle) - increments epoch
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .clear_deltas();

        // Insert new row at new epoch
        indices
            .get_mut(&("users".to_string(), "_id".to_string()))
            .unwrap()
            .insert(row2.uuid().as_bytes(), row2)
            .unwrap();

        // Scan with mismatched epoch - should fallback to slow path
        let ctx = make_ctx(&indices);
        let delta2 = node.scan(&ctx);

        // Node's epoch should be updated to new epoch
        assert_eq!(node.last_delta_epoch, Some(1));

        // Should detect the new row via full scan
        assert_eq!(delta2.added.len(), 1);
        assert!(contains_id(&delta2.added, row2));
    }
}
