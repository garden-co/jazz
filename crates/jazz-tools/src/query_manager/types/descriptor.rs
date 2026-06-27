use std::collections::HashMap;

use super::*;

/// Combined row descriptor for queries spanning multiple tables (joins).
/// Maps qualified column names (table.column) to table index and column index.
#[derive(Debug, Clone)]
pub struct CombinedRowDescriptor {
    /// Table names/aliases in order.
    pub tables: Vec<String>,
    /// Per-table descriptors.
    pub descriptors: Vec<RowDescriptor>,
    /// Map from (table, column) to (table_index, column_index).
    column_map: HashMap<(String, String), (usize, usize)>,
}

impl CombinedRowDescriptor {
    /// Create a new combined descriptor from table names and their descriptors.
    pub fn new(tables: Vec<String>, descriptors: Vec<RowDescriptor>) -> Self {
        let mut column_map = HashMap::new();

        for (table_idx, (table_name, descriptor)) in
            tables.iter().zip(descriptors.iter()).enumerate()
        {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                column_map.insert(
                    (table_name.clone(), col.name.to_string()),
                    (table_idx, col_idx),
                );
            }
        }

        Self {
            tables,
            descriptors,
            column_map,
        }
    }

    /// Create a single-table descriptor (for non-join queries).
    pub fn single(table: impl Into<String>, descriptor: RowDescriptor) -> Self {
        let table_name = table.into();
        Self::new(vec![table_name], vec![descriptor])
    }

    /// Resolve a qualified column reference to (table_index, column_index).
    pub fn resolve_column(&self, table: &str, column: &str) -> Option<(usize, usize)> {
        self.column_map
            .get(&(table.to_string(), column.to_string()))
            .copied()
    }

    /// Resolve an unqualified column reference (searches all tables, first match wins).
    pub fn resolve_unqualified(&self, column: &str) -> Option<(usize, usize)> {
        for (table_idx, descriptor) in self.descriptors.iter().enumerate() {
            if let Some(col_idx) = descriptor.column_index(column) {
                return Some((table_idx, col_idx));
            }
        }
        None
    }

    /// Get the descriptor for a specific table index.
    pub fn table_descriptor(&self, table_idx: usize) -> Option<&RowDescriptor> {
        self.descriptors.get(table_idx)
    }

    /// Get the table name for a specific index.
    pub fn table_name(&self, table_idx: usize) -> Option<&str> {
        self.tables.get(table_idx).map(|s| s.as_str())
    }

    /// Get total number of tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get total number of columns across all tables.
    pub fn total_column_count(&self) -> usize {
        self.descriptors.iter().map(|d| d.columns.len()).sum()
    }
}
