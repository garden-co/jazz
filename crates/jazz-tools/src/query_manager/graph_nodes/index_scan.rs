use ahash::{AHashMap, AHashSet};

use crate::object::{BranchName, ObjectId};
use crate::query_manager::index::ScanCondition;
use crate::query_manager::types::{
    ColumnName, QueryBranchRef, RowDescriptor, ScopedObject, TableName, Tuple, TupleDelta,
    TupleDescriptor,
};

use super::{SourceContext, SourceNode, tuple_delta::compute_tuple_delta};

/// Source node that scans an index via Storage.
/// Emits TupleDelta with length-1 tuples based on the scan condition.
#[derive(Debug)]
pub struct IndexScanNode {
    pub table: TableName,
    pub column: ColumnName,
    pub branches: Vec<QueryBranchRef>,
    pub condition: ScanCondition,

    /// Output tuple descriptor (single element, unmaterialized).
    output_descriptor: TupleDescriptor,

    /// Current set of tuples (length-1) matching the condition.
    current_tuples: AHashSet<Tuple>,
    /// Stable ordered view of the current tuples for delta computation.
    current_tuple_order: Vec<Tuple>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl IndexScanNode {
    /// Create a new index scan node.
    pub fn new_with_branches(
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        branches: Vec<QueryBranchRef>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        let table = table.into();
        let output_descriptor = TupleDescriptor::single(table.as_str(), row_descriptor);
        Self {
            table,
            column: column.into(),
            branches,
            condition,
            output_descriptor,
            current_tuples: AHashSet::new(),
            current_tuple_order: Vec::new(),
            dirty: true,
        }
    }

    pub fn new_with_branch(
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        branch: QueryBranchRef,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        Self::new_with_branches(table, column, vec![branch], condition, row_descriptor)
    }

    /// Create a new index scan node on the default "main" branch.
    pub fn new(
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        Self::new_with_branches(
            table,
            column,
            vec![QueryBranchRef::raw(BranchName::new("main"))],
            condition,
            row_descriptor,
        )
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    fn merged_scoped_tuples(scoped_rows: Vec<ScopedObject>) -> Vec<Tuple> {
        let mut provenance_by_id = AHashMap::<ObjectId, AHashSet<(ObjectId, BranchName)>>::new();
        for (row_id, branch_name) in scoped_rows {
            provenance_by_id
                .entry(row_id)
                .or_default()
                .insert((row_id, branch_name));
        }

        let mut tuples: Vec<_> = provenance_by_id
            .into_iter()
            .map(|(row_id, provenance)| Tuple::from_id(row_id).with_provenance(provenance))
            .collect();
        tuples.sort_by_key(|tuple| tuple.first_id().map(|id| *id.uuid().as_bytes()));
        tuples
    }
}

impl SourceNode for IndexScanNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        let scoped_rows = match &self.condition {
            ScanCondition::All => ctx.storage.index_scan_all_scoped(
                self.table.as_str(),
                self.column.as_str(),
                &self.branches,
            ),
            ScanCondition::Eq(value) => ctx.storage.index_lookup_scoped(
                self.table.as_str(),
                self.column.as_str(),
                &self.branches,
                value,
            ),
            ScanCondition::Range { min, max } => {
                let start = min.as_ref();
                let end = max.as_ref();
                ctx.storage.index_range_scoped(
                    self.table.as_str(),
                    self.column.as_str(),
                    &self.branches,
                    start,
                    end,
                )
            }
        };
        let new_tuple_order = Self::merged_scoped_tuples(scoped_rows);
        let new_tuples: AHashSet<Tuple> = new_tuple_order.iter().cloned().collect();
        let delta = compute_tuple_delta(&self.current_tuple_order, &new_tuple_order);

        tracing::trace!(
            table = %self.table,
            branches = self.branches.len(),
            scanned = new_tuples.len(),
            added = delta.added.len(),
            removed = delta.removed.len(),
            updated = delta.updated.len(),
            "IndexScan results"
        );

        self.current_tuple_order = new_tuple_order;
        self.current_tuples = new_tuples;
        self.dirty = false;

        delta
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, Value};
    use crate::storage::MemoryStorage;
    use std::ops::Bound;

    fn make_ctx(storage: &dyn crate::storage::Storage) -> SourceContext<'_> {
        SourceContext { storage }
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
        let mut storage = MemoryStorage::new();
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row1), row1)
            .unwrap();
        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row2), row2)
            .unwrap();
        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row3), row3)
            .unwrap();

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&storage);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 3);
        assert!(contains_id(&delta.added, row1));
        assert!(contains_id(&delta.added, row2));
        assert!(contains_id(&delta.added, row3));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn scan_eq_returns_matching_rows() {
        let mut storage = MemoryStorage::new();
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert(
                "users",
                "email",
                "main",
                &Value::Text("alice@example.com".into()),
                row1,
            )
            .unwrap();
        storage
            .index_insert(
                "users",
                "email",
                "main",
                &Value::Text("bob@example.com".into()),
                row2,
            )
            .unwrap();

        let mut node = IndexScanNode::new(
            "users",
            "email",
            ScanCondition::Eq(Value::Text("alice@example.com".into())),
            test_descriptor(),
        );
        let ctx = make_ctx(&storage);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row1));
    }

    #[test]
    fn multi_branch_scan_merges_provenance_and_reports_updates() {
        let mut storage = MemoryStorage::new();
        let row = ObjectId::new();

        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row), row)
            .unwrap();
        storage
            .index_insert("users", "_id", "draft", &Value::Uuid(row), row)
            .unwrap();

        let branches = vec![
            QueryBranchRef::from_branch_name("main"),
            QueryBranchRef::from_branch_name("draft"),
        ];
        let mut node = IndexScanNode::new_with_branches(
            "users",
            "_id",
            branches,
            ScanCondition::All,
            test_descriptor(),
        );
        let first = {
            let ctx = make_ctx(&storage);
            node.scan(&ctx)
        };
        assert_eq!(first.added.len(), 1);
        assert_eq!(first.added[0].provenance().len(), 2);
        assert!(first.updated.is_empty());

        storage
            .index_remove("users", "_id", "main", &Value::Uuid(row), row)
            .unwrap();

        let second = {
            let ctx = make_ctx(&storage);
            node.scan(&ctx)
        };
        assert!(second.added.is_empty());
        assert!(second.removed.is_empty());
        assert_eq!(second.updated.len(), 1);
        assert_eq!(second.updated[0].1.provenance().len(), 1);
    }

    #[test]
    fn scan_range_returns_rows_in_range() {
        let mut storage = MemoryStorage::new();
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        storage
            .index_insert("users", "score", "main", &Value::Integer(10), row1)
            .unwrap();
        storage
            .index_insert("users", "score", "main", &Value::Integer(20), row2)
            .unwrap();
        storage
            .index_insert("users", "score", "main", &Value::Integer(30), row3)
            .unwrap();

        let mut node = IndexScanNode::new(
            "users",
            "score",
            ScanCondition::Range {
                min: Bound::Included(Value::Integer(15)),
                max: Bound::Included(Value::Integer(25)),
            },
            test_descriptor(),
        );
        let ctx = make_ctx(&storage);
        let delta = node.scan(&ctx);

        assert_eq!(delta.added.len(), 1);
        assert!(contains_id(&delta.added, row2));
    }

    #[test]
    fn rescan_detects_changes() {
        let mut storage = MemoryStorage::new();
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row1), row1)
            .unwrap();

        let mut node = IndexScanNode::new("users", "_id", ScanCondition::All, test_descriptor());
        let ctx = make_ctx(&storage);
        let delta1 = node.scan(&ctx);
        assert_eq!(delta1.added.len(), 1);
        assert!(contains_id(&delta1.added, row1));

        // Add another row
        storage
            .index_insert("users", "_id", "main", &Value::Uuid(row2), row2)
            .unwrap();

        let ctx = make_ctx(&storage);
        let delta2 = node.scan(&ctx);
        assert_eq!(delta2.added.len(), 1);
        assert!(contains_id(&delta2.added, row2));
        assert!(delta2.removed.is_empty());

        // Remove first row
        storage
            .index_remove("users", "_id", "main", &Value::Uuid(row1), row1)
            .unwrap();

        let ctx = make_ctx(&storage);
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

        assert_eq!(output.element_count(), 1);
        assert!(!output.materialization().is_materialized(0));
    }
}
