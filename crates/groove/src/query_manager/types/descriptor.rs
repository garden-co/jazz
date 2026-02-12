use std::collections::HashMap;

use super::*;

// ============================================================================
// TupleDescriptor - Describes structure of tuples in a node's output
// ============================================================================

/// Describes which element of a tuple contains a given set of columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElementDescriptor {
    /// Table name or alias for this element.
    pub table: String,
    /// Row descriptor for this element's columns.
    pub descriptor: RowDescriptor,
    /// Starting global column index for this element.
    pub column_offset: usize,
}

/// Describes the structure of tuples in a node's output.
///
/// Maps global column indices to (element_index, local_column_index) pairs,
/// enabling FilterNode to find data in multi-element tuples (e.g., after joins).
///
/// Also tracks per-element materialization state to enable lazy materialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TupleDescriptor {
    /// Descriptors for each element in the tuple.
    elements: Vec<ElementDescriptor>,
    /// Total columns across all elements.
    total_columns: usize,
    /// Per-element materialization state.
    materialization: MaterializationState,
}

impl TupleDescriptor {
    /// Create a single-element tuple descriptor (ID-only by default).
    pub fn single(table: &str, descriptor: RowDescriptor) -> Self {
        let total_columns = descriptor.columns.len();
        Self {
            elements: vec![ElementDescriptor {
                table: table.to_string(),
                descriptor,
                column_offset: 0,
            }],
            total_columns,
            materialization: MaterializationState::all_ids(1),
        }
    }

    /// Create a single-element tuple descriptor with explicit materialization state.
    pub fn single_with_materialization(
        table: &str,
        descriptor: RowDescriptor,
        materialized: bool,
    ) -> Self {
        let total_columns = descriptor.columns.len();
        Self {
            elements: vec![ElementDescriptor {
                table: table.to_string(),
                descriptor,
                column_offset: 0,
            }],
            total_columns,
            materialization: if materialized {
                MaterializationState::all_materialized(1)
            } else {
                MaterializationState::all_ids(1)
            },
        }
    }

    /// Create a tuple descriptor from multiple element descriptors (all ID-only).
    pub fn from_elements(elements: Vec<ElementDescriptor>) -> Self {
        let element_count = elements.len();
        let total_columns = elements
            .last()
            .map_or(0, |e| e.column_offset + e.descriptor.columns.len());
        Self {
            elements,
            total_columns,
            materialization: MaterializationState::all_ids(element_count),
        }
    }

    /// Create a tuple descriptor from table names and their descriptors (all ID-only).
    /// Computes column_offset for each element automatically.
    pub fn from_tables(tables: &[(String, RowDescriptor)]) -> Self {
        let mut elements = Vec::with_capacity(tables.len());
        let mut offset = 0;

        for (table, descriptor) in tables {
            elements.push(ElementDescriptor {
                table: table.clone(),
                descriptor: descriptor.clone(),
                column_offset: offset,
            });
            offset += descriptor.columns.len();
        }

        Self {
            total_columns: offset,
            materialization: MaterializationState::all_ids(elements.len()),
            elements,
        }
    }

    /// Concatenate two descriptors (for join output).
    /// Combines elements from both and concatenates materialization states.
    pub fn concat(left: &Self, right: &Self) -> Self {
        let mut elements = left.elements.clone();
        let left_cols = left.total_columns;
        for elem in &right.elements {
            elements.push(ElementDescriptor {
                table: elem.table.clone(),
                descriptor: elem.descriptor.clone(),
                column_offset: elem.column_offset + left_cols,
            });
        }
        Self {
            total_columns: left.total_columns + right.total_columns,
            materialization: left.materialization.concat(&right.materialization),
            elements,
        }
    }

    /// Get the materialization state.
    pub fn materialization(&self) -> &MaterializationState {
        &self.materialization
    }

    /// Return a new descriptor with specified elements marked as materialized.
    pub fn with_materialized(self, elements: &std::collections::HashSet<usize>) -> Self {
        Self {
            materialization: self.materialization.with_all_materialized(elements),
            ..self
        }
    }

    /// Return a new descriptor with all elements marked as materialized.
    pub fn with_all_materialized(self) -> Self {
        Self {
            materialization: self.materialization.materialize_all(),
            ..self
        }
    }

    /// Validate that all required elements are materialized.
    /// Returns Ok if all are materialized, Err with message otherwise.
    pub fn assert_materialized(
        &self,
        elements: &std::collections::HashSet<usize>,
    ) -> Result<(), String> {
        let unmaterialized: Vec<_> = elements
            .iter()
            .filter(|&&i| !self.materialization.is_materialized(i))
            .collect();
        if unmaterialized.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Elements {:?} are not materialized but required",
                unmaterialized
            ))
        }
    }

    /// Get column index by name, searching all elements.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let mut offset = 0;
        for elem in &self.elements {
            if let Some(local_idx) = elem.descriptor.column_index(name) {
                return Some(offset + local_idx);
            }
            offset += elem.descriptor.columns.len();
        }
        None
    }

    /// Get column index by qualified name (table.column).
    pub fn qualified_column_index(&self, table: &str, column: &str) -> Option<usize> {
        for elem in &self.elements {
            if elem.table == table
                && let Some(local_idx) = elem.descriptor.column_index(column)
            {
                return Some(elem.column_offset + local_idx);
            }
        }
        None
    }

    /// Map global column index to (element_index, local_column_index).
    ///
    /// Given a global column index from the combined descriptor, returns
    /// which tuple element contains that column and the local index within
    /// that element.
    pub fn resolve_column(&self, global_index: usize) -> Option<(usize, usize)> {
        for (elem_idx, elem) in self.elements.iter().enumerate() {
            let elem_end = elem.column_offset + elem.descriptor.columns.len();
            if global_index >= elem.column_offset && global_index < elem_end {
                let local_idx = global_index - elem.column_offset;
                return Some((elem_idx, local_idx));
            }
        }
        None
    }

    /// Get all element indices needed for a set of global column indices.
    pub fn elements_for_columns(
        &self,
        columns: &std::collections::HashSet<usize>,
    ) -> std::collections::HashSet<usize> {
        columns
            .iter()
            .filter_map(|&col| self.resolve_column(col).map(|(elem_idx, _)| elem_idx))
            .collect()
    }

    /// Get the number of elements in the tuple.
    pub fn element_count(&self) -> usize {
        self.elements.len()
    }

    /// Get an element descriptor by index.
    pub fn element(&self, index: usize) -> Option<&ElementDescriptor> {
        self.elements.get(index)
    }

    /// Get the total number of columns across all elements.
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }

    /// Create a combined RowDescriptor with all columns from all elements.
    pub fn combined_descriptor(&self) -> RowDescriptor {
        let columns: Vec<ColumnDescriptor> = self
            .elements
            .iter()
            .flat_map(|e| e.descriptor.columns.clone())
            .collect();
        RowDescriptor { columns }
    }

    /// Get iterator over elements.
    pub fn iter(&self) -> impl Iterator<Item = &ElementDescriptor> {
        self.elements.iter()
    }
}

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
