use std::ops::Bound;

use crate::query_manager::types::Value;

/// Condition for index scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanCondition {
    /// No condition - scan all entries (uses "_id" index).
    All,
    /// Empty condition - scan no entries.
    Empty,
    /// Exact match on value.
    Eq(Value),
    /// Range scan with bounds (inclusive, exclusive, or unbounded).
    Range {
        min: Bound<Value>,
        max: Bound<Value>,
    },
}
