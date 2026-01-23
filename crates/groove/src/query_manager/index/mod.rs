pub mod btree_index;
pub mod btree_page;

use std::ops::Bound;

pub use btree_index::{BTreeIndex, IndexError};
pub use btree_page::{BTreePage, IndexMeta, LeafEntry, PageId};

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
