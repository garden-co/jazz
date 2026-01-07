//! Delta types for representing row changes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::sql::row::{Row, Value};
use crate::sql::row_buffer::{OwnedRow, RowDescriptor};
use crate::sql::schema::TableSchema;

/// Reference to prior row state via commit graph.
///
/// Contains the frontier commit IDs before a write operation.
/// Old row values can be looked up on-demand from these tips.
#[derive(Clone, Debug, Default)]
pub struct PriorState {
    /// Frontier commit IDs before the write.
    /// Empty if the row was just created.
    pub prior_tips: Vec<CommitId>,
}

impl PriorState {
    /// Create a new PriorState with the given commit tips.
    pub fn new(prior_tips: Vec<CommitId>) -> Self {
        Self { prior_tips }
    }

    /// Create an empty PriorState (for newly created rows).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if this row was just created (no prior state).
    pub fn is_new(&self) -> bool {
        self.prior_tips.is_empty()
    }
}

/// A change to a single row.
#[derive(Clone, Debug)]
pub enum RowDelta {
    /// Row was inserted (no prior state).
    Added(Row),

    /// Row was deleted.
    Removed {
        /// The ID of the deleted row.
        id: ObjectId,
        /// Prior tips for looking up deleted row data if needed.
        prior: PriorState,
    },

    /// Row was updated.
    Updated {
        /// The ID of the updated row.
        id: ObjectId,
        /// The new row values.
        new: Row,
        /// Prior tips for looking up old values on-demand.
        prior: PriorState,
    },
}

impl RowDelta {
    /// Get the row ID affected by this delta.
    pub fn row_id(&self) -> ObjectId {
        match self {
            RowDelta::Added(row) => row.id,
            RowDelta::Removed { id, .. } => *id,
            RowDelta::Updated { id, .. } => *id,
        }
    }

    /// Get the new row data if this delta has it.
    pub fn new_row(&self) -> Option<&Row> {
        match self {
            RowDelta::Added(row) => Some(row),
            RowDelta::Updated { new, .. } => Some(new),
            RowDelta::Removed { .. } => None,
        }
    }

    /// Returns true if this delta has prior state that can be looked up.
    pub fn has_prior(&self) -> bool {
        match self {
            RowDelta::Added(_) => false,
            RowDelta::Removed { prior, .. } => !prior.is_new(),
            RowDelta::Updated { prior, .. } => !prior.is_new(),
        }
    }
}

/// A batch of row changes.
#[derive(Clone, Debug, Default)]
pub struct DeltaBatch {
    deltas: Vec<RowDelta>,
}

impl FromIterator<RowDelta> for DeltaBatch {
    fn from_iter<I: IntoIterator<Item = RowDelta>>(iter: I) -> Self {
        Self {
            deltas: iter.into_iter().collect(),
        }
    }
}

impl DeltaBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a batch with a single Added delta.
    pub fn added(row: Row) -> Self {
        Self {
            deltas: vec![RowDelta::Added(row)],
        }
    }

    /// Create a batch with a single Updated delta.
    pub fn updated(id: ObjectId, new: Row, prior_tips: Vec<CommitId>) -> Self {
        Self {
            deltas: vec![RowDelta::Updated {
                id,
                new,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Create a batch with a single Removed delta.
    pub fn removed(id: ObjectId, prior_tips: Vec<CommitId>) -> Self {
        Self {
            deltas: vec![RowDelta::Removed {
                id,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Add a delta to the batch.
    pub fn push(&mut self, delta: RowDelta) {
        self.deltas.push(delta);
    }

    /// Extend this batch with deltas from another batch.
    pub fn extend(&mut self, other: DeltaBatch) {
        self.deltas.extend(other.deltas);
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Returns the number of deltas in the batch.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Iterate over deltas by reference.
    pub fn iter(&self) -> impl Iterator<Item = &RowDelta> {
        self.deltas.iter()
    }

    /// Consume the batch and iterate over deltas.
    pub fn into_iter(self) -> impl Iterator<Item = RowDelta> {
        self.deltas.into_iter()
    }

    /// Compact the batch by removing redundant changes.
    ///
    /// For example, if a row is Added and then Removed, both entries
    /// cancel out. If a row is Updated multiple times, only the final
    /// state matters.
    pub fn compact(&mut self) {
        if self.deltas.len() <= 1 {
            return;
        }

        // Track final state per row: None = removed/not present, Some = added/updated
        let mut final_state: HashMap<ObjectId, Option<(Row, PriorState)>> = HashMap::new();
        // Track which rows existed before this batch (had prior state on first delta)
        let mut existed_before: HashMap<ObjectId, bool> = HashMap::new();

        for delta in self.deltas.drain(..) {
            let id = delta.row_id();

            // Remember if row existed before (only on first encounter)
            existed_before.entry(id).or_insert_with(|| delta.has_prior());

            match delta {
                RowDelta::Added(row) => {
                    final_state.insert(id, Some((row, PriorState::empty())));
                }
                RowDelta::Removed {  .. } => {
                    final_state.insert(id, None);
                    // Keep the prior state if this is the first delta for this row
                    if !existed_before.get(&id).copied().unwrap_or(false) {
                        // Row was added then removed - just remove from final_state entirely
                        final_state.remove(&id);
                    }
                }
                RowDelta::Updated { new, prior, .. } => {
                    // Keep prior from first delta for this row
                    let prior_to_use = if let Some(Some((_, existing_prior))) = final_state.get(&id)
                    {
                        existing_prior.clone()
                    } else {
                        prior
                    };
                    final_state.insert(id, Some((new, prior_to_use)));
                }
            }
        }

        // Rebuild deltas from final state
        for (id, state) in final_state {
            let existed = existed_before.get(&id).copied().unwrap_or(false);
            match state {
                Some((row, prior)) => {
                    if existed {
                        self.deltas.push(RowDelta::Updated { id, new: row, prior });
                    } else {
                        self.deltas.push(RowDelta::Added(row));
                    }
                }
                None => {
                    if existed {
                        // Row existed and was removed
                        self.deltas.push(RowDelta::Removed {
                            id,
                            prior: PriorState::empty(), // Prior was already used
                        });
                    }
                    // If row didn't exist and was removed, it was added then removed - no delta
                }
            }
        }
    }
}

/// A row that has been joined from multiple tables.
///
/// This represents the result of a JOIN operation, containing the
/// row data from the primary table plus any joined tables.
#[derive(Clone, Debug)]
pub struct JoinedRow {
    /// The primary (left) table name.
    pub primary_table: String,
    /// Row ID from the primary table.
    pub primary_id: ObjectId,
    /// Column values from all tables in order: primary columns, then join1 columns, etc.
    pub values: Vec<Value>,
    /// Map from table name to (row_id, start_column_index).
    /// This allows looking up which columns belong to which table.
    pub table_offsets: HashMap<String, (ObjectId, usize)>,
}

impl JoinedRow {
    /// Create a JoinedRow from a single table's row.
    pub fn from_single(table: &str, row: Row) -> Self {
        let mut table_offsets = HashMap::new();
        table_offsets.insert(table.to_string(), (row.id, 0));

        Self {
            primary_table: table.to_string(),
            primary_id: row.id,
            values: row.values,
            table_offsets,
        }
    }

    /// Add columns from a joined table.
    pub fn add_joined(&mut self, table: &str, row: Row) {
        let start_idx = self.values.len();
        self.table_offsets.insert(table.to_string(), (row.id, start_idx));
        self.values.extend(row.values);
    }

    /// Get the row ID for a specific table.
    pub fn get_row_id(&self, table: &str) -> Option<ObjectId> {
        self.table_offsets.get(table).map(|(id, _)| *id)
    }

    /// Get a column value by table and column index within that table.
    pub fn get_value(&self, table: &str, col_idx: usize) -> Option<&Value> {
        let (_, start) = self.table_offsets.get(table)?;
        self.values.get(start + col_idx)
    }

    /// Get a column value by table name and column name.
    pub fn get_column(&self, table: &str, column: &str, schema: &TableSchema) -> Option<&Value> {
        let col_idx = schema.column_index(column)?;
        self.get_value(table, col_idx)
    }

    /// Convert to an output Row using the joined values.
    /// The output row ID is the primary table's row ID.
    pub fn to_output_row(&self) -> Row {
        Row::new(self.primary_id, self.values.clone())
    }

    /// Convert to an output Row containing only values from a specific table.
    ///
    /// Used for reverse JOINs where we want `SELECT Table.*` but the graph
    /// had to swap tables for the join logic.
    pub fn to_projected_row(&self, table: &str, column_count: usize) -> Option<Row> {
        let (row_id, start_idx) = self.table_offsets.get(table)?;
        let end_idx = start_idx + column_count;
        if end_idx > self.values.len() {
            return None;
        }
        let projected_values = self.values[*start_idx..end_idx].to_vec();
        Some(Row::new(*row_id, projected_values))
    }

    /// Check if this joined row contains a specific table.
    pub fn has_table(&self, table: &str) -> bool {
        self.table_offsets.contains_key(table)
    }

    /// Get all table names in this joined row.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.table_offsets.keys().map(|s| s.as_str())
    }
}

// ============================================================================
// Buffer-based Delta Types (new unified row format)
// ============================================================================

/// A change to a single row using the unified buffer format.
#[derive(Clone, Debug)]
pub enum BufferRowDelta {
    /// Row was inserted (no prior state).
    Added {
        /// The ID of the added row.
        id: ObjectId,
        /// The new row data.
        row: OwnedRow,
    },

    /// Row was deleted.
    Removed {
        /// The ID of the deleted row.
        id: ObjectId,
        /// Prior tips for looking up deleted row data if needed.
        prior: PriorState,
    },

    /// Row was updated.
    Updated {
        /// The ID of the updated row.
        id: ObjectId,
        /// The new row data.
        row: OwnedRow,
        /// Prior tips for looking up old values on-demand.
        prior: PriorState,
    },
}

impl BufferRowDelta {
    /// Get the row ID affected by this delta.
    pub fn row_id(&self) -> ObjectId {
        match self {
            BufferRowDelta::Added { id, .. } => *id,
            BufferRowDelta::Removed { id, .. } => *id,
            BufferRowDelta::Updated { id, .. } => *id,
        }
    }

    /// Get the new row data if this delta has it.
    pub fn new_row(&self) -> Option<&OwnedRow> {
        match self {
            BufferRowDelta::Added { row, .. } => Some(row),
            BufferRowDelta::Updated { row, .. } => Some(row),
            BufferRowDelta::Removed { .. } => None,
        }
    }

    /// Get the row descriptor if available.
    pub fn descriptor(&self) -> Option<&Arc<RowDescriptor>> {
        match self {
            BufferRowDelta::Added { row, .. } => Some(&row.descriptor),
            BufferRowDelta::Updated { row, .. } => Some(&row.descriptor),
            BufferRowDelta::Removed { .. } => None,
        }
    }

    /// Returns true if this delta has prior state that can be looked up.
    pub fn has_prior(&self) -> bool {
        match self {
            BufferRowDelta::Added { .. } => false,
            BufferRowDelta::Removed { prior, .. } => !prior.is_new(),
            BufferRowDelta::Updated { prior, .. } => !prior.is_new(),
        }
    }

    /// Convert from legacy RowDelta using a schema and descriptor.
    ///
    /// Note: This allocates a new buffer for the row data.
    pub fn from_legacy(
        delta: &RowDelta,
        schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
    ) -> Self {
        match delta {
            RowDelta::Added(row) => BufferRowDelta::Added {
                id: row.id,
                row: OwnedRow::from_legacy_row(row, schema, descriptor),
            },
            RowDelta::Removed { id, prior } => BufferRowDelta::Removed {
                id: *id,
                prior: prior.clone(),
            },
            RowDelta::Updated { id, new, prior } => BufferRowDelta::Updated {
                id: *id,
                row: OwnedRow::from_legacy_row(new, schema, descriptor),
                prior: prior.clone(),
            },
        }
    }

    /// Convert to legacy RowDelta.
    ///
    /// Note: This allocates for string/bytes values.
    pub fn to_legacy(&self, schema: &TableSchema) -> RowDelta {
        match self {
            BufferRowDelta::Added { id, row } => {
                RowDelta::Added(row.to_legacy_row_with_schema(*id, schema))
            }
            BufferRowDelta::Removed { id, prior } => RowDelta::Removed {
                id: *id,
                prior: prior.clone(),
            },
            BufferRowDelta::Updated { id, row, prior } => RowDelta::Updated {
                id: *id,
                new: row.to_legacy_row_with_schema(*id, schema),
                prior: prior.clone(),
            },
        }
    }
}

/// A batch of buffer-based row changes.
#[derive(Clone, Debug, Default)]
pub struct BufferDeltaBatch {
    /// The row descriptor for all rows in this batch.
    descriptor: Option<Arc<RowDescriptor>>,
    /// The deltas in this batch.
    deltas: Vec<BufferRowDelta>,
}

impl BufferDeltaBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a batch with an explicit descriptor.
    pub fn with_descriptor(descriptor: Arc<RowDescriptor>) -> Self {
        Self {
            descriptor: Some(descriptor),
            deltas: Vec::new(),
        }
    }

    /// Create a batch with a single Added delta.
    pub fn added(id: ObjectId, row: OwnedRow) -> Self {
        let descriptor = row.descriptor.clone();
        Self {
            descriptor: Some(descriptor),
            deltas: vec![BufferRowDelta::Added { id, row }],
        }
    }

    /// Create a batch with a single Updated delta.
    pub fn updated(id: ObjectId, row: OwnedRow, prior_tips: Vec<CommitId>) -> Self {
        let descriptor = row.descriptor.clone();
        Self {
            descriptor: Some(descriptor),
            deltas: vec![BufferRowDelta::Updated {
                id,
                row,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Create a batch with a single Removed delta.
    pub fn removed(id: ObjectId, prior_tips: Vec<CommitId>) -> Self {
        Self {
            descriptor: None,
            deltas: vec![BufferRowDelta::Removed {
                id,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Get the row descriptor for this batch (if available).
    pub fn descriptor(&self) -> Option<&Arc<RowDescriptor>> {
        self.descriptor.as_ref()
    }

    /// Add a delta to the batch.
    pub fn push(&mut self, delta: BufferRowDelta) {
        // Capture descriptor from first delta with a row
        if self.descriptor.is_none() {
            if let Some(desc) = delta.descriptor() {
                self.descriptor = Some(desc.clone());
            }
        }
        self.deltas.push(delta);
    }

    /// Extend this batch with deltas from another batch.
    pub fn extend(&mut self, other: BufferDeltaBatch) {
        if self.descriptor.is_none() {
            self.descriptor = other.descriptor;
        }
        self.deltas.extend(other.deltas);
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Returns the number of deltas in the batch.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Iterate over deltas by reference.
    pub fn iter(&self) -> impl Iterator<Item = &BufferRowDelta> {
        self.deltas.iter()
    }

    /// Consume the batch and iterate over deltas.
    pub fn into_iter(self) -> impl Iterator<Item = BufferRowDelta> {
        self.deltas.into_iter()
    }

    /// Convert from legacy DeltaBatch.
    pub fn from_legacy(
        batch: &DeltaBatch,
        schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
    ) -> Self {
        let deltas = batch
            .iter()
            .map(|d| BufferRowDelta::from_legacy(d, schema, descriptor.clone()))
            .collect();
        Self {
            descriptor: Some(descriptor),
            deltas,
        }
    }

    /// Convert to legacy DeltaBatch.
    pub fn to_legacy(&self, schema: &TableSchema) -> DeltaBatch {
        DeltaBatch {
            deltas: self.deltas.iter().map(|d| d.to_legacy(schema)).collect(),
        }
    }
}

// ============================================================================
// Buffer-based JoinedRow
// ============================================================================

/// A joined row using the unified buffer format.
///
/// Unlike the legacy `JoinedRow` which stores `Vec<Value>`, this stores
/// `OwnedRow` instances with their descriptors. Each table in the join
/// has its own row buffer with a shared descriptor.
#[derive(Clone, Debug)]
pub struct BufferJoinedRow {
    /// The primary (left) table name.
    pub primary_table: String,
    /// Row ID from the primary table.
    pub primary_id: ObjectId,
    /// Rows keyed by table name: (row_id, OwnedRow).
    /// Each table has its own descriptor via the OwnedRow.
    pub table_rows: HashMap<String, (ObjectId, OwnedRow)>,
}

impl BufferJoinedRow {
    /// Create a BufferJoinedRow from a single table's row.
    pub fn from_single(table: &str, row_id: ObjectId, row: OwnedRow) -> Self {
        let mut table_rows = HashMap::new();
        table_rows.insert(table.to_string(), (row_id, row));

        Self {
            primary_table: table.to_string(),
            primary_id: row_id,
            table_rows,
        }
    }

    /// Add a row from a joined table.
    pub fn add_joined(&mut self, table: &str, row_id: ObjectId, row: OwnedRow) {
        self.table_rows.insert(table.to_string(), (row_id, row));
    }

    /// Get the row ID for a specific table.
    pub fn get_row_id(&self, table: &str) -> Option<ObjectId> {
        self.table_rows.get(table).map(|(id, _)| *id)
    }

    /// Get a row reference for a specific table.
    pub fn get_row(&self, table: &str) -> Option<crate::sql::row_buffer::RowRef<'_>> {
        self.table_rows.get(table).map(|(_, row)| row.as_ref())
    }

    /// Get an owned row clone for a specific table.
    pub fn get_owned_row(&self, table: &str) -> Option<&OwnedRow> {
        self.table_rows.get(table).map(|(_, row)| row)
    }

    /// Get the descriptor for a specific table.
    pub fn get_descriptor(&self, table: &str) -> Option<&Arc<RowDescriptor>> {
        self.table_rows.get(table).map(|(_, row)| &row.descriptor)
    }

    /// Check if this joined row contains a specific table.
    pub fn has_table(&self, table: &str) -> bool {
        self.table_rows.contains_key(table)
    }

    /// Get all table names in this joined row.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.table_rows.keys().map(|s| s.as_str())
    }

    /// Convert to an output OwnedRow by joining all tables' rows.
    ///
    /// Creates a new row with columns from all tables concatenated.
    /// The output descriptor is the join of all table descriptors.
    pub fn to_output_row(&self) -> OwnedRow {
        use crate::sql::row_buffer::{join_rows, RowBuilder};

        // Start with the primary table's row
        let primary_row = match self.table_rows.get(&self.primary_table) {
            Some((_, row)) => row.clone(),
            None => {
                // No primary table row - return empty
                let empty_desc = Arc::new(RowDescriptor::new([]));
                return RowBuilder::new(empty_desc).build();
            }
        };

        // Join with all other tables
        let mut result = primary_row;
        for (table, (_, row)) in &self.table_rows {
            if table != &self.primary_table {
                let joined_desc = Arc::new(result.descriptor.join(&row.descriptor));
                result = join_rows(result.as_ref(), row.as_ref(), joined_desc);
            }
        }

        result
    }

    /// Convert to the legacy JoinedRow format.
    ///
    /// This is provided for compatibility during migration.
    pub fn to_legacy(&self, schemas: &HashMap<String, TableSchema>) -> JoinedRow {
        let mut values = Vec::new();
        let mut table_offsets = HashMap::new();

        // Add primary table first
        if let Some((row_id, row)) = self.table_rows.get(&self.primary_table) {
            let schema = schemas.get(&self.primary_table);
            let legacy_row = if let Some(s) = schema {
                row.to_legacy_row_with_schema(*row_id, s)
            } else {
                row.to_legacy_row(*row_id)
            };
            table_offsets.insert(self.primary_table.clone(), (*row_id, 0));
            values.extend(legacy_row.values);
        }

        // Add other tables
        for (table, (row_id, row)) in &self.table_rows {
            if table != &self.primary_table {
                let start_idx = values.len();
                let schema = schemas.get(table);
                let legacy_row = if let Some(s) = schema {
                    row.to_legacy_row_with_schema(*row_id, s)
                } else {
                    row.to_legacy_row(*row_id)
                };
                table_offsets.insert(table.clone(), (*row_id, start_idx));
                values.extend(legacy_row.values);
            }
        }

        JoinedRow {
            primary_table: self.primary_table.clone(),
            primary_id: self.primary_id,
            values,
            table_offsets,
        }
    }

    /// Create from a legacy JoinedRow.
    ///
    /// Requires descriptors for each table since JoinedRow doesn't store type info.
    pub fn from_legacy(
        legacy: &JoinedRow,
        schemas: &HashMap<String, TableSchema>,
        descriptors: &HashMap<String, Arc<RowDescriptor>>,
    ) -> Self {
        let mut table_rows = HashMap::new();

        for (table, (row_id, start_idx)) in &legacy.table_offsets {
            if let (Some(schema), Some(descriptor)) = (schemas.get(table), descriptors.get(table)) {
                // Extract values for this table
                let end_idx = start_idx + schema.columns.len();
                let values = if end_idx <= legacy.values.len() {
                    legacy.values[*start_idx..end_idx].to_vec()
                } else {
                    continue;
                };

                // Create a legacy Row and convert to OwnedRow
                let legacy_row = Row::new(*row_id, values);
                let owned_row = OwnedRow::from_legacy_row(&legacy_row, schema, descriptor.clone());
                table_rows.insert(table.clone(), (*row_id, owned_row));
            }
        }

        Self {
            primary_table: legacy.primary_table.clone(),
            primary_id: legacy.primary_id,
            table_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: u128, name: &str) -> Row {
        Row::new(ObjectId::new(id), vec![Value::String(name.to_string())])
    }

    #[test]
    fn delta_batch_empty() {
        let batch = DeltaBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn delta_batch_added() {
        let row = make_row(1, "Alice");
        let batch = DeltaBatch::added(row.clone());

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        let delta = batch.iter().next().unwrap();
        assert_eq!(delta.row_id(), ObjectId::new(1));
        assert!(matches!(delta, RowDelta::Added(_)));
    }

    #[test]
    fn delta_batch_compact_add_remove() {
        let row = make_row(1, "Alice");
        let mut batch = DeltaBatch::new();

        batch.push(RowDelta::Added(row));
        batch.push(RowDelta::Removed {
            id: ObjectId::new(1),
            prior: PriorState::empty(),
        });

        batch.compact();

        // Add followed by remove with no prior state = nothing
        assert!(batch.is_empty());
    }

    #[test]
    fn delta_batch_compact_multiple_updates() {
        let row1 = make_row(1, "Alice");
        let row2 = make_row(1, "Alicia");
        let row3 = make_row(1, "Alex");

        let mut batch = DeltaBatch::new();
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row1,
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]), // Existed before
        });
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row2,
            prior: PriorState::empty(),
        });
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row3.clone(),
            prior: PriorState::empty(),
        });

        batch.compact();

        // Should have single update to final state
        assert_eq!(batch.len(), 1);
        let delta = batch.iter().next().unwrap();
        assert!(matches!(delta, RowDelta::Updated { new, .. } if new.values[0] == Value::String("Alex".to_string())));
    }

    #[test]
    fn row_delta_accessors() {
        let row = make_row(1, "Alice");

        let added = RowDelta::Added(row.clone());
        assert_eq!(added.row_id(), ObjectId::new(1));
        assert!(added.new_row().is_some());
        assert!(!added.has_prior());

        let removed = RowDelta::Removed {
            id: ObjectId::new(2),
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]),
        };
        assert_eq!(removed.row_id(), ObjectId::new(2));
        assert!(removed.new_row().is_none());
        assert!(removed.has_prior());

        let updated = RowDelta::Updated {
            id: ObjectId::new(1),
            new: row,
            prior: PriorState::empty(),
        };
        assert_eq!(updated.row_id(), ObjectId::new(1));
        assert!(updated.new_row().is_some());
        assert!(!updated.has_prior());
    }

    // ========================================================================
    // BufferRowDelta tests
    // ========================================================================

    use crate::sql::row_buffer::{ColType, RowBuilder, RowDescriptor};
    use crate::sql::schema::{ColumnDef, ColumnType};

    fn make_test_schema() -> TableSchema {
        TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "name".to_string(),
                ty: ColumnType::String,
                nullable: false,
            }],
        }
    }

    fn make_test_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([("name".to_string(), ColType::String)]))
    }

    fn make_buffer_row(descriptor: &Arc<RowDescriptor>, name: &str) -> OwnedRow {
        let idx = descriptor.column_index("name").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(idx, name)
            .build()
    }

    #[test]
    fn buffer_delta_batch_empty() {
        let batch = BufferDeltaBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert!(batch.descriptor().is_none());
    }

    #[test]
    fn buffer_delta_batch_added() {
        let descriptor = make_test_descriptor();
        let row = make_buffer_row(&descriptor, "Alice");
        let id = ObjectId::new(1);
        let batch = BufferDeltaBatch::added(id, row);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        assert!(batch.descriptor().is_some());

        let delta = batch.iter().next().unwrap();
        assert_eq!(delta.row_id(), id);
        assert!(matches!(delta, BufferRowDelta::Added { .. }));
    }

    #[test]
    fn buffer_row_delta_accessors() {
        let descriptor = make_test_descriptor();
        let row = make_buffer_row(&descriptor, "Alice");
        let id = ObjectId::new(1);

        let added = BufferRowDelta::Added {
            id,
            row: row.clone(),
        };
        assert_eq!(added.row_id(), id);
        assert!(added.new_row().is_some());
        assert!(!added.has_prior());
        assert!(added.descriptor().is_some());

        let removed = BufferRowDelta::Removed {
            id: ObjectId::new(2),
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]),
        };
        assert_eq!(removed.row_id(), ObjectId::new(2));
        assert!(removed.new_row().is_none());
        assert!(removed.has_prior());
        assert!(removed.descriptor().is_none());

        let updated = BufferRowDelta::Updated {
            id,
            row,
            prior: PriorState::empty(),
        };
        assert_eq!(updated.row_id(), id);
        assert!(updated.new_row().is_some());
        assert!(!updated.has_prior());
    }

    #[test]
    fn buffer_delta_legacy_roundtrip() {
        let schema = make_test_schema();
        let descriptor = make_test_descriptor();

        // Create a legacy delta
        let legacy_row = make_row(1, "Alice");
        let legacy_delta = RowDelta::Added(legacy_row);

        // Convert to buffer delta
        let buffer_delta = BufferRowDelta::from_legacy(&legacy_delta, &schema, descriptor.clone());

        // Verify it's correct
        assert_eq!(buffer_delta.row_id(), ObjectId::new(1));
        assert!(matches!(buffer_delta, BufferRowDelta::Added { .. }));

        // Convert back to legacy
        let roundtrip = buffer_delta.to_legacy(&schema);
        assert_eq!(roundtrip.row_id(), ObjectId::new(1));

        // Verify the row value
        if let RowDelta::Added(row) = roundtrip {
            assert_eq!(row.values[0], Value::String("Alice".to_string()));
        } else {
            panic!("Expected Added delta");
        }
    }

    #[test]
    fn buffer_delta_batch_legacy_roundtrip() {
        let schema = make_test_schema();
        let descriptor = make_test_descriptor();

        // Create a legacy batch with multiple deltas
        let mut legacy_batch = DeltaBatch::new();
        legacy_batch.push(RowDelta::Added(make_row(1, "Alice")));
        legacy_batch.push(RowDelta::Updated {
            id: ObjectId::new(2),
            new: make_row(2, "Bob"),
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]),
        });

        // Convert to buffer batch
        let buffer_batch = BufferDeltaBatch::from_legacy(&legacy_batch, &schema, descriptor);

        assert_eq!(buffer_batch.len(), 2);
        assert!(buffer_batch.descriptor().is_some());

        // Convert back
        let roundtrip = buffer_batch.to_legacy(&schema);
        assert_eq!(roundtrip.len(), 2);
    }

    // ========================================================================
    // BufferJoinedRow tests
    // ========================================================================

    use crate::sql::row_buffer::RowValue;

    fn make_users_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([
            ("name".to_string(), ColType::String),
            ("age".to_string(), ColType::I32),
        ]))
    }

    fn make_posts_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([
            ("title".to_string(), ColType::String),
            ("author_id".to_string(), ColType::Ref),
        ]))
    }

    fn make_users_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("age", ColumnType::I32),
            ],
        )
    }

    fn make_posts_schema() -> TableSchema {
        TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("title", ColumnType::String),
                ColumnDef::required("author_id", ColumnType::Ref("users".to_string())),
            ],
        )
    }

    fn make_user_row(descriptor: &Arc<RowDescriptor>, name: &str, age: i32) -> OwnedRow {
        let name_idx = descriptor.column_index("name").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(name_idx, name)
            .set_i32(age_idx, age)
            .build()
    }

    fn make_post_row(descriptor: &Arc<RowDescriptor>, title: &str, author_id: ObjectId) -> OwnedRow {
        let title_idx = descriptor.column_index("title").unwrap();
        let author_idx = descriptor.column_index("author_id").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(title_idx, title)
            .set_ref(author_idx, author_id)
            .build()
    }

    #[test]
    fn buffer_joined_row_from_single() {
        let users_desc = make_users_descriptor();
        let user_row = make_user_row(&users_desc, "Alice", 30);
        let user_id = ObjectId::new(1);

        let joined = BufferJoinedRow::from_single("users", user_id, user_row);

        assert_eq!(joined.primary_table, "users");
        assert_eq!(joined.primary_id, user_id);
        assert!(joined.has_table("users"));
        assert!(!joined.has_table("posts"));
        assert_eq!(joined.get_row_id("users"), Some(user_id));
    }

    #[test]
    fn buffer_joined_row_add_joined() {
        let users_desc = make_users_descriptor();
        let posts_desc = make_posts_descriptor();

        let user_row = make_user_row(&users_desc, "Alice", 30);
        let user_id = ObjectId::new(1);
        let post_row = make_post_row(&posts_desc, "Hello World", user_id);
        let post_id = ObjectId::new(2);

        let mut joined = BufferJoinedRow::from_single("users", user_id, user_row);
        joined.add_joined("posts", post_id, post_row);

        assert!(joined.has_table("users"));
        assert!(joined.has_table("posts"));
        assert_eq!(joined.get_row_id("users"), Some(user_id));
        assert_eq!(joined.get_row_id("posts"), Some(post_id));

        // Check we can access the rows
        let user_ref = joined.get_row("users").unwrap();
        match user_ref.get_by_name("name") {
            Some(RowValue::String(s)) => assert_eq!(s, "Alice"),
            other => panic!("Expected String, got {:?}", other),
        }

        let post_ref = joined.get_row("posts").unwrap();
        match post_ref.get_by_name("title") {
            Some(RowValue::String(s)) => assert_eq!(s, "Hello World"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn buffer_joined_row_to_output() {
        let users_desc = make_users_descriptor();
        let posts_desc = make_posts_descriptor();

        let user_row = make_user_row(&users_desc, "Alice", 30);
        let user_id = ObjectId::new(1);
        let post_row = make_post_row(&posts_desc, "Hello World", user_id);
        let post_id = ObjectId::new(2);

        let mut joined = BufferJoinedRow::from_single("users", user_id, user_row);
        joined.add_joined("posts", post_id, post_row);

        let output = joined.to_output_row();

        // Output should have columns from both tables
        // The order depends on HashMap iteration, but we can check count
        assert!(output.descriptor.columns.len() >= 4); // 2 from users + 2 from posts
    }

    #[test]
    fn buffer_joined_row_legacy_roundtrip() {
        let users_desc = make_users_descriptor();
        let users_schema = make_users_schema();

        let user_row = make_user_row(&users_desc, "Alice", 30);
        let user_id = ObjectId::new(1);

        let joined = BufferJoinedRow::from_single("users", user_id, user_row);

        // Convert to legacy
        let mut schemas = HashMap::new();
        schemas.insert("users".to_string(), users_schema.clone());
        let legacy = joined.to_legacy(&schemas);

        assert_eq!(legacy.primary_table, "users");
        assert_eq!(legacy.primary_id, user_id);
        assert_eq!(legacy.values.len(), 2); // name + age

        // Convert back
        let mut descriptors = HashMap::new();
        descriptors.insert("users".to_string(), users_desc.clone());
        let back = BufferJoinedRow::from_legacy(&legacy, &schemas, &descriptors);

        assert_eq!(back.primary_table, "users");
        assert_eq!(back.primary_id, user_id);
        assert!(back.has_table("users"));

        // Check the row data
        let row_ref = back.get_row("users").unwrap();
        match row_ref.get_by_name("name") {
            Some(RowValue::String(s)) => assert_eq!(s, "Alice"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn buffer_joined_row_tables_iter() {
        let users_desc = make_users_descriptor();
        let posts_desc = make_posts_descriptor();

        let user_row = make_user_row(&users_desc, "Alice", 30);
        let user_id = ObjectId::new(1);
        let post_row = make_post_row(&posts_desc, "Hello World", user_id);
        let post_id = ObjectId::new(2);

        let mut joined = BufferJoinedRow::from_single("users", user_id, user_row);
        joined.add_joined("posts", post_id, post_row);

        let tables: Vec<&str> = joined.tables().collect();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"posts"));
    }
}
