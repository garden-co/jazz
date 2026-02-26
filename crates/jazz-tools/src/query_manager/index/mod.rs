use std::ops::Bound;

use crate::query_manager::types::Value;

/// Condition for index scan.
#[derive(Debug, Clone)]
pub enum ScanCondition {
    /// No condition - scan all entries (uses "_id" index).
    All,
    /// No predicate, but bounded to a window in index key order.
    AllWindow {
        offset: usize,
        limit: usize,
        descending: bool,
    },
    /// Exact match on value.
    Eq(Value),
    /// Range scan with bounds (inclusive, exclusive, or unbounded).
    Range {
        min: Bound<Value>,
        max: Bound<Value>,
    },
}
