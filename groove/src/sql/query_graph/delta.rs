//! Delta types for representing row changes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commit::CommitId;
use crate::object::ObjectId;
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

/// A change to a single row using the unified buffer format.
#[derive(Clone, Debug)]
pub enum RowDelta {
    /// Row was inserted (no prior state).
    Added {
        /// The ID of the added row.
        id: ObjectId,
        /// The new row data in buffer format.
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
        /// The new row data in buffer format.
        row: OwnedRow,
        /// Prior tips for looking up old values on-demand.
        prior: PriorState,
    },
}

impl RowDelta {
    /// Get the row ID affected by this delta.
    pub fn row_id(&self) -> ObjectId {
        match self {
            RowDelta::Added { id, .. } => *id,
            RowDelta::Removed { id, .. } => *id,
            RowDelta::Updated { id, .. } => *id,
        }
    }

    /// Get the new row data if this delta has it.
    pub fn new_row(&self) -> Option<&OwnedRow> {
        match self {
            RowDelta::Added { row, .. } => Some(row),
            RowDelta::Updated { row, .. } => Some(row),
            RowDelta::Removed { .. } => None,
        }
    }

    /// Get the row descriptor if available.
    pub fn descriptor(&self) -> Option<&Arc<RowDescriptor>> {
        match self {
            RowDelta::Added { row, .. } => Some(&row.descriptor),
            RowDelta::Updated { row, .. } => Some(&row.descriptor),
            RowDelta::Removed { .. } => None,
        }
    }

    /// Returns true if this delta has prior state that can be looked up.
    pub fn has_prior(&self) -> bool {
        match self {
            RowDelta::Added { .. } => false,
            RowDelta::Removed { prior, .. } => !prior.is_new(),
            RowDelta::Updated { prior, .. } => !prior.is_new(),
        }
    }

    /// Create a new delta with qualified column names.
    ///
    /// Converts column names from `column` to `table.column` format.
    /// This is needed for JOIN queries where predicates use qualified names.
    pub fn qualify_columns(self, table: &str, schema: &TableSchema) -> Self {
        match self {
            RowDelta::Added { id, row } => {
                let qualified_row = row.qualify_columns(table, schema);
                RowDelta::Added {
                    id,
                    row: qualified_row,
                }
            }
            RowDelta::Updated { id, row, prior } => {
                let qualified_row = row.qualify_columns(table, schema);
                RowDelta::Updated {
                    id,
                    row: qualified_row,
                    prior,
                }
            }
            RowDelta::Removed { id, prior } => {
                // Removed deltas don't have row data to qualify
                RowDelta::Removed { id, prior }
            }
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
    pub fn added(id: ObjectId, row: OwnedRow) -> Self {
        Self {
            deltas: vec![RowDelta::Added { id, row }],
        }
    }

    /// Create a batch with a single Updated delta.
    pub fn updated(id: ObjectId, row: OwnedRow, prior_tips: Vec<CommitId>) -> Self {
        Self {
            deltas: vec![RowDelta::Updated {
                id,
                row,
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
        let mut final_state: HashMap<ObjectId, Option<(OwnedRow, PriorState)>> = HashMap::new();
        // Track which rows existed before this batch (had prior state on first delta)
        let mut existed_before: HashMap<ObjectId, bool> = HashMap::new();

        for delta in self.deltas.drain(..) {
            let id = delta.row_id();

            // Remember if row existed before (only on first encounter)
            existed_before
                .entry(id)
                .or_insert_with(|| delta.has_prior());

            match delta {
                RowDelta::Added { row, .. } => {
                    final_state.insert(id, Some((row, PriorState::empty())));
                }
                RowDelta::Removed { .. } => {
                    final_state.insert(id, None);
                    // Keep the prior state if this is the first delta for this row
                    if !existed_before.get(&id).copied().unwrap_or(false) {
                        // Row was added then removed - just remove from final_state entirely
                        final_state.remove(&id);
                    }
                }
                RowDelta::Updated { row, prior, .. } => {
                    // Keep prior from first delta for this row
                    let prior_to_use = if let Some(Some((_, existing_prior))) = final_state.get(&id)
                    {
                        existing_prior.clone()
                    } else {
                        prior
                    };
                    final_state.insert(id, Some((row, prior_to_use)));
                }
            }
        }

        // Rebuild deltas from final state
        for (id, state) in final_state {
            let existed = existed_before.get(&id).copied().unwrap_or(false);
            match state {
                Some((row, prior)) => {
                    if existed {
                        self.deltas.push(RowDelta::Updated { id, row, prior });
                    } else {
                        self.deltas.push(RowDelta::Added { id, row });
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

// Legacy JoinedRow has been removed - use BufferJoinedRow instead.

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
    /// Create an empty BufferJoinedRow with just a primary table designation.
    pub fn new(primary_table: &str, primary_id: ObjectId) -> Self {
        Self {
            primary_table: primary_table.to_string(),
            primary_id,
            table_rows: HashMap::new(),
        }
    }

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

    /// Convert to an output OwnedRow by merging all tables' rows.
    ///
    /// Creates a new row with columns from all tables concatenated.
    /// Uses merge_rows to preserve buffer order and handle duplicate column names.
    pub fn to_output_row(&self) -> OwnedRow {
        use crate::sql::row_buffer::RowBuilder;

        // Start with the primary table's row
        let primary_row = match self.table_rows.get(&self.primary_table) {
            Some((_, row)) => row,
            None => {
                // No primary table row - return empty
                let empty_desc = Arc::new(RowDescriptor::new([]));
                return RowBuilder::new(empty_desc).build();
            }
        };

        // Collect all rows to merge: primary first, then others in stable order
        let mut rows_to_merge: Vec<&OwnedRow> = vec![primary_row];
        let mut other_tables: Vec<_> = self
            .table_rows
            .iter()
            .filter(|(table, _)| *table != &self.primary_table)
            .collect();
        // Sort by table name for stable ordering
        other_tables.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (_, (_, row)) in other_tables {
            rows_to_merge.push(row);
        }

        OwnedRow::merge_rows(&rows_to_merge)
    }

    /// Convert to an output OwnedRow with values in schema order.
    ///
    /// The combined schema specifies the expected output column order.
    /// Values are looked up by qualified column name to ensure correct ordering.
    pub fn to_output_row_with_schema(
        &self,
        combined_schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
    ) -> OwnedRow {
        use crate::sql::row_buffer::RowBuilder;

        let mut builder = RowBuilder::new(descriptor.clone());

        for (col_idx, col_def) in combined_schema.columns.iter().enumerate() {
            // col_def.name is qualified like "folders.owner_id"
            if let Some((table, col_name)) = col_def.name.split_once('.') {
                if let Some((_, owned_row)) = self.table_rows.get(table) {
                    // The individual owned_row has unqualified column names, so use col_name
                    if let Some(rv) = owned_row.get_by_name(col_name) {
                        // Get the buffer column index for this schema column index
                        if let Some(buf_col_idx) = descriptor
                            .columns
                            .iter()
                            .position(|c| c.schema_index == col_idx)
                        {
                            builder = builder.set_from_row_value(buf_col_idx, rv);
                        }
                    }
                }
            }
        }

        builder.build()
    }

    /// Convert to an output OwnedRow containing only values from a specific table.
    pub fn to_projected_row(
        &self,
        table: &str,
        schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
    ) -> Option<(ObjectId, OwnedRow)> {
        use crate::sql::row_buffer::RowBuilder;

        let (row_id, owned_row) = self.table_rows.get(table)?;

        let mut builder = RowBuilder::new(descriptor.clone());

        for (col_idx, col_def) in schema.columns.iter().enumerate() {
            let qualified_name = format!("{}.{}", table, col_def.name);
            if let Some(rv) = owned_row.get_by_name(&qualified_name) {
                // Get the buffer column index for this schema column index
                if let Some(buf_col_idx) = descriptor
                    .columns
                    .iter()
                    .position(|c| c.schema_index == col_idx)
                {
                    builder = builder.set_from_row_value(buf_col_idx, rv);
                }
            }
        }

        Some((*row_id, builder.build()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor, RowValue};
    use crate::sql::schema::{ColumnDef, ColumnType};

    fn make_test_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([(
            "name".to_string(),
            ColumnType::String,
            false,
        )]))
    }

    fn make_buffer_row(descriptor: &Arc<RowDescriptor>, name: &str) -> OwnedRow {
        let idx = descriptor.column_index("name").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(idx, name)
            .build()
    }

    #[test]
    fn delta_batch_empty() {
        let batch = DeltaBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn delta_batch_added() {
        let descriptor = make_test_descriptor();
        let row = make_buffer_row(&descriptor, "Alice");
        let id = ObjectId::new(1);
        let batch = DeltaBatch::added(id, row);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        let delta = batch.iter().next().unwrap();
        assert_eq!(delta.row_id(), id);
        assert!(matches!(delta, RowDelta::Added { .. }));
    }

    #[test]
    fn delta_batch_compact_add_remove() {
        let descriptor = make_test_descriptor();
        let row = make_buffer_row(&descriptor, "Alice");
        let id = ObjectId::new(1);

        let mut batch = DeltaBatch::new();
        batch.push(RowDelta::Added { id, row });
        batch.push(RowDelta::Removed {
            id,
            prior: PriorState::empty(),
        });

        batch.compact();

        // Add followed by remove with no prior state = nothing
        assert!(batch.is_empty());
    }

    #[test]
    fn delta_batch_compact_multiple_updates() {
        let descriptor = make_test_descriptor();
        let id = ObjectId::new(1);
        let row1 = make_buffer_row(&descriptor, "Alice");
        let row2 = make_buffer_row(&descriptor, "Alicia");
        let row3 = make_buffer_row(&descriptor, "Alex");

        let mut batch = DeltaBatch::new();
        batch.push(RowDelta::Updated {
            id,
            row: row1,
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]), // Existed before
        });
        batch.push(RowDelta::Updated {
            id,
            row: row2,
            prior: PriorState::empty(),
        });
        batch.push(RowDelta::Updated {
            id,
            row: row3,
            prior: PriorState::empty(),
        });

        batch.compact();

        // Should have single update to final state
        assert_eq!(batch.len(), 1);
        let delta = batch.iter().next().unwrap();
        if let RowDelta::Updated { row, .. } = delta {
            assert_eq!(row.get_by_name("name"), Some(RowValue::String("Alex")));
        } else {
            panic!("Expected Updated delta");
        }
    }

    #[test]
    fn row_delta_accessors() {
        let descriptor = make_test_descriptor();
        let row = make_buffer_row(&descriptor, "Alice");
        let id = ObjectId::new(1);

        let added = RowDelta::Added {
            id,
            row: row.clone(),
        };
        assert_eq!(added.row_id(), id);
        assert!(added.new_row().is_some());
        assert!(added.descriptor().is_some());
        assert!(!added.has_prior());

        let removed = RowDelta::Removed {
            id: ObjectId::new(2),
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]),
        };
        assert_eq!(removed.row_id(), ObjectId::new(2));
        assert!(removed.new_row().is_none());
        assert!(removed.descriptor().is_none());
        assert!(removed.has_prior());

        let updated = RowDelta::Updated {
            id,
            row,
            prior: PriorState::empty(),
        };
        assert_eq!(updated.row_id(), id);
        assert!(updated.new_row().is_some());
        assert!(updated.descriptor().is_some());
        assert!(!updated.has_prior());
    }

    // ========================================================================
    // BufferJoinedRow tests
    // ========================================================================

    fn make_users_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::I32, false),
        ]))
    }

    fn make_posts_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::new([
            ("title".to_string(), ColumnType::String, false),
            (
                "author_id".to_string(),
                ColumnType::Ref("users".to_string()),
                false,
            ),
        ]))
    }

    fn make_user_row(descriptor: &Arc<RowDescriptor>, name: &str, age: i32) -> OwnedRow {
        let name_idx = descriptor.column_index("name").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(name_idx, name)
            .set_i32(age_idx, age)
            .build()
    }

    fn make_post_row(
        descriptor: &Arc<RowDescriptor>,
        title: &str,
        author_id: ObjectId,
    ) -> OwnedRow {
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
