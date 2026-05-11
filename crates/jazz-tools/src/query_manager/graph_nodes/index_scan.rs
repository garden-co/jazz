use ahash::AHashSet;
use std::ops::Bound;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::index::ScanCondition;
use crate::query_manager::types::{
    ColumnName, RowDescriptor, TableName, Tuple, TupleDelta, TupleDescriptor, Value,
};
use crate::row_format::decode_row;

use super::{SourceContext, SourceNode};

/// Source node that scans an index via Storage.
/// Emits TupleDelta with length-1 tuples based on the scan condition.
#[derive(Debug)]
pub struct IndexScanNode {
    pub table: TableName,
    pub column: ColumnName,
    pub branch: String,
    pub condition: ScanCondition,

    /// Output tuple descriptor (single element, unmaterialized).
    output_descriptor: TupleDescriptor,
    row_descriptor: RowDescriptor,

    /// Current set of tuples (length-1) matching the condition.
    current_tuples: AHashSet<Tuple>,
    /// Last scanned IDs (for computing deltas).
    last_scanned_ids: AHashSet<ObjectId>,
    /// Whether this node needs reprocessing.
    dirty: bool,
}

impl IndexScanNode {
    /// Create a new index scan node.
    pub fn new_with_branch(
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        branch: impl Into<String>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        let table = table.into();
        let output_descriptor = TupleDescriptor::single(table.as_str(), row_descriptor.clone());
        Self {
            table,
            column: column.into(),
            branch: branch.into(),
            condition,
            output_descriptor,
            row_descriptor,
            current_tuples: AHashSet::new(),
            last_scanned_ids: AHashSet::new(),
            dirty: true,
        }
    }

    /// Create a new index scan node on the default "main" branch.
    pub fn new(
        table: impl Into<TableName>,
        column: impl Into<ColumnName>,
        condition: ScanCondition,
        row_descriptor: RowDescriptor,
    ) -> Self {
        Self::new_with_branch(table, column, "main", condition, row_descriptor)
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_descriptor
    }

    fn overlay_value_matches_condition(&self, row_id: ObjectId, data: &[u8]) -> bool {
        let Some(value) = self.overlay_index_value(row_id, data) else {
            return false;
        };
        match &self.condition {
            ScanCondition::All => true,
            ScanCondition::Eq(expected) => value == *expected || array_contains(&value, expected),
            ScanCondition::Range { min, max } => {
                bound_matches(min, &value, true) && bound_matches(max, &value, false)
            }
        }
    }

    fn overlay_index_value(&self, row_id: ObjectId, data: &[u8]) -> Option<Value> {
        if self.column.as_str() == "_id" {
            return Some(Value::Uuid(row_id));
        }
        if self.column.as_str() == "_id_deleted" {
            return None;
        }

        let column_index = self.row_descriptor.column_index(self.column.as_str())?;
        let values = decode_row(&self.row_descriptor, data).ok()?;
        values.get(column_index).cloned()
    }

    fn apply_local_overlay_rows(&self, ctx: &SourceContext, new_ids: &mut AHashSet<ObjectId>) {
        let Some(local_overlay_rows) = ctx.local_overlay_rows else {
            return;
        };

        for (&row_id, row_batch_key) in local_overlay_rows {
            if row_batch_key.branch_name.as_str() != self.branch {
                continue;
            }
            let Ok(Some(row)) = ctx.storage.load_history_query_row_batch(
                self.table.as_str(),
                self.branch.as_str(),
                row_id,
                row_batch_key.batch_id,
            ) else {
                continue;
            };
            if row.is_soft_deleted() || row.is_hard_deleted() {
                new_ids.remove(&row_id);
            } else if self.overlay_value_matches_condition(row_id, &row.data) {
                new_ids.insert(row_id);
            } else {
                new_ids.remove(&row_id);
            }
        }
    }
}

fn array_contains(value: &Value, expected: &Value) -> bool {
    matches!(value, Value::Array(values) if values.iter().any(|value| value == expected))
}

fn compare_values_for_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::BigInt(a), Value::BigInt(b)) => Some(a.cmp(b)),
        (Value::Double(a), Value::Double(b)) => Some(a.total_cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

fn bound_matches(bound: &Bound<Value>, value: &Value, is_lower: bool) -> bool {
    match bound {
        Bound::Unbounded => true,
        Bound::Included(bound) => compare_values_for_ordering(value, bound)
            .map(|ordering| {
                if is_lower {
                    matches!(
                        ordering,
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    )
                } else {
                    matches!(
                        ordering,
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    )
                }
            })
            .unwrap_or(false),
        Bound::Excluded(bound) => compare_values_for_ordering(value, bound)
            .map(|ordering| {
                if is_lower {
                    ordering == std::cmp::Ordering::Greater
                } else {
                    ordering == std::cmp::Ordering::Less
                }
            })
            .unwrap_or(false),
    }
}

impl SourceNode for IndexScanNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        let mut new_ids: AHashSet<ObjectId> = match &self.condition {
            ScanCondition::All => ctx
                .storage
                .index_scan_all(self.table.as_str(), self.column.as_str(), &self.branch)
                .into_iter()
                .collect(),
            ScanCondition::Eq(value) => ctx
                .storage
                .index_lookup(
                    self.table.as_str(),
                    self.column.as_str(),
                    &self.branch,
                    value,
                )
                .into_iter()
                .collect(),
            ScanCondition::Range { min, max } => {
                let start = min.as_ref();
                let end = max.as_ref();
                ctx.storage
                    .index_range(
                        self.table.as_str(),
                        self.column.as_str(),
                        &self.branch,
                        start,
                        end,
                    )
                    .into_iter()
                    .collect()
            }
        };
        self.apply_local_overlay_rows(ctx, &mut new_ids);

        // Diff against last scan
        let added: Vec<ObjectId> = new_ids
            .difference(&self.last_scanned_ids)
            .copied()
            .collect();
        let removed: Vec<ObjectId> = self
            .last_scanned_ids
            .difference(&new_ids)
            .copied()
            .collect();

        tracing::trace!(
            table = %self.table,
            branch = %self.branch,
            scanned = new_ids.len(),
            added = added.len(),
            removed = removed.len(),
            "IndexScan results"
        );

        self.last_scanned_ids = new_ids;
        let branch = BranchName::new(&self.branch);
        self.current_tuples = self
            .last_scanned_ids
            .iter()
            .map(|&id| Tuple::from_scoped_id(id, branch))
            .collect();
        self.dirty = false;

        TupleDelta {
            added: added
                .into_iter()
                .map(|id| Tuple::from_scoped_id(id, branch))
                .collect(),
            removed: removed
                .into_iter()
                .map(|id| Tuple::from_scoped_id(id, branch))
                .collect(),
            moved: vec![],
            updated: vec![],
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, Value};
    use crate::storage::{MemoryStorage, Storage};
    use std::ops::Bound;

    fn make_ctx(storage: &dyn crate::storage::Storage) -> SourceContext<'_> {
        SourceContext {
            storage,
            local_overlay_rows: None,
        }
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
