//! Copy-on-Write Writer - Handles updates across schema versions.
//!
//! When updating a row that exists in an old schema branch:
//! 1. Load the row from the old branch
//! 2. Transform it to current schema using lens
//! 3. Apply the update
//! 4. Write to current schema's branch (new version)
//!
//! Old data remains in old branch - no deletion or modification.

use std::collections::HashMap;

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::types::{RowDescriptor, SchemaHash, TableName, Value};

use super::context::SchemaContext;
use super::transformer::{LensTransformer, TransformError};

/// Result of a copy-on-write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The object ID (same as input for updates).
    pub object_id: ObjectId,
    /// The new row data (encoded with current schema).
    pub data: Vec<u8>,
    /// Whether the row was transformed from an old schema.
    pub was_transformed: bool,
    /// Source schema hash (where the original row was).
    pub source_schema: SchemaHash,
}

/// Error during write operations.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteError {
    /// Row not found in any live branch.
    RowNotFound(ObjectId),
    /// Transform failed.
    TransformError(TransformError),
    /// Decode error.
    DecodeError(String),
    /// Encode error.
    EncodeError(String),
    /// Table not found.
    TableNotFound(String),
    /// Column count mismatch in update.
    ColumnCountMismatch { expected: usize, actual: usize },
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::RowNotFound(id) => write!(f, "row not found: {:?}", id),
            WriteError::TransformError(e) => write!(f, "transform error: {}", e),
            WriteError::DecodeError(msg) => write!(f, "decode error: {}", msg),
            WriteError::EncodeError(msg) => write!(f, "encode error: {}", msg),
            WriteError::TableNotFound(name) => write!(f, "table not found: {}", name),
            WriteError::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {}, got {}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for WriteError {}

impl From<TransformError> for WriteError {
    fn from(e: TransformError) -> Self {
        WriteError::TransformError(e)
    }
}

/// Handles copy-on-write updates across schema versions.
pub struct CopyOnWriteWriter<'a> {
    context: &'a SchemaContext,
    table: String,
    /// Maps (object_id, branch) to (data, commit_id).
    /// Provided by the caller to look up row data.
    row_cache: HashMap<(ObjectId, String), (Vec<u8>, CommitId)>,
    /// Maps branch name to schema hash.
    branch_schema_map: HashMap<String, SchemaHash>,
}

impl<'a> CopyOnWriteWriter<'a> {
    /// Create a new writer for a specific table.
    pub fn new(
        context: &'a SchemaContext,
        table: &str,
        branch_schema_map: HashMap<String, SchemaHash>,
    ) -> Self {
        Self {
            context,
            table: table.to_string(),
            row_cache: HashMap::new(),
            branch_schema_map,
        }
    }

    /// Cache row data for later use.
    pub fn cache_row(&mut self, id: ObjectId, branch: &str, data: Vec<u8>, commit_id: CommitId) {
        self.row_cache
            .insert((id, branch.to_string()), (data, commit_id));
    }

    /// Find which branch contains a row.
    pub fn find_row_branch(&self, id: ObjectId) -> Option<&str> {
        for key in self.row_cache.keys() {
            if key.0 == id {
                return Some(&key.1);
            }
        }
        None
    }

    /// Get the descriptor for the current schema's table.
    fn current_descriptor(&self) -> Result<&RowDescriptor, WriteError> {
        let table_schema = self
            .context
            .current_schema
            .get(&TableName::new(&self.table))
            .ok_or_else(|| WriteError::TableNotFound(self.table.clone()))?;
        Ok(&table_schema.columns)
    }

    /// Prepare an update for a row, handling cross-schema transformation.
    ///
    /// # Arguments
    /// * `id` - Object ID of the row to update
    /// * `updater` - Function that takes current values and returns updated values
    ///
    /// # Returns
    /// WriteResult containing the new encoded row data for the current schema.
    pub fn prepare_update<F>(&self, id: ObjectId, updater: F) -> Result<WriteResult, WriteError>
    where
        F: FnOnce(&[Value]) -> Vec<Value>,
    {
        // Find the row in cache
        let (branch, data, _commit_id) =
            self.find_row_data(id).ok_or(WriteError::RowNotFound(id))?;

        // Get source schema hash
        let source_hash = self
            .branch_schema_map
            .get(&branch)
            .copied()
            .ok_or(WriteError::RowNotFound(id))?;

        let current_desc = self.current_descriptor()?;
        let was_transformed = source_hash != self.context.current_hash;

        // Transform if needed
        let current_values = if was_transformed {
            let transformer = LensTransformer::new(self.context, &self.table);
            let result = transformer.transform(&data, CommitId([0; 32]), source_hash)?;
            decode_row(current_desc, &result.data)
                .map_err(|e| WriteError::DecodeError(format!("{:?}", e)))?
        } else {
            decode_row(current_desc, &data)
                .map_err(|e| WriteError::DecodeError(format!("{:?}", e)))?
        };

        // Apply the update
        let updated_values = updater(&current_values);

        // Validate column count
        if updated_values.len() != current_desc.columns.len() {
            return Err(WriteError::ColumnCountMismatch {
                expected: current_desc.columns.len(),
                actual: updated_values.len(),
            });
        }

        // Encode with current schema
        let new_data = encode_row(current_desc, &updated_values)
            .map_err(|e| WriteError::EncodeError(format!("{:?}", e)))?;

        Ok(WriteResult {
            object_id: id,
            data: new_data,
            was_transformed,
            source_schema: source_hash,
        })
    }

    /// Prepare an insert (always goes to current schema).
    pub fn prepare_insert(&self, values: Vec<Value>) -> Result<WriteResult, WriteError> {
        let current_desc = self.current_descriptor()?;

        if values.len() != current_desc.columns.len() {
            return Err(WriteError::ColumnCountMismatch {
                expected: current_desc.columns.len(),
                actual: values.len(),
            });
        }

        let data = encode_row(current_desc, &values)
            .map_err(|e| WriteError::EncodeError(format!("{:?}", e)))?;

        Ok(WriteResult {
            object_id: ObjectId::new(),
            data,
            was_transformed: false,
            source_schema: self.context.current_hash,
        })
    }

    /// Helper to find row data in cache.
    fn find_row_data(&self, id: ObjectId) -> Option<(String, Vec<u8>, CommitId)> {
        for ((cached_id, branch), (data, commit_id)) in &self.row_cache {
            if *cached_id == id {
                return Some((branch.clone(), data.clone(), *commit_id));
            }
        }
        None
    }
}

/// Information about where a row was written.
#[derive(Debug, Clone)]
pub struct RowWriteInfo {
    /// Branch the row should be written to.
    pub target_branch: String,
    /// Whether this is a copy-on-write (row came from different schema).
    pub is_copy_on_write: bool,
    /// Original branch if copy-on-write.
    pub source_branch: Option<String>,
}

impl<'a> CopyOnWriteWriter<'a> {
    /// Determine where a write should go.
    pub fn get_write_info(&self, id: ObjectId) -> RowWriteInfo {
        let target_branch = crate::query_manager::types::ComposedBranchName::new(
            &self.context.env,
            self.context.current_hash,
            &self.context.user_branch,
        )
        .to_branch_name()
        .as_str()
        .to_string();

        if let Some(source_branch) = self.find_row_branch(id) {
            let is_copy_on_write = source_branch != target_branch;
            RowWriteInfo {
                target_branch,
                is_copy_on_write,
                source_branch: if is_copy_on_write {
                    Some(source_branch.to_string())
                } else {
                    None
                },
            }
        } else {
            // New row - write to current branch
            RowWriteInfo {
                target_branch,
                is_copy_on_write: false,
                source_branch: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use crate::schema_manager::auto_lens::generate_lens;

    fn make_schema_v1() -> crate::query_manager::types::Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn make_schema_v2() -> crate::query_manager::types::Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text),
            )
            .build()
    }

    fn make_commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
    }

    #[test]
    fn prepare_insert_current_schema() {
        let v2 = make_schema_v2();
        let ctx = SchemaContext::new(v2, "dev", "main");
        let branch_map = HashMap::new();

        let writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);

        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
            Value::Text("alice@example.com".to_string()),
        ];

        let result = writer.prepare_insert(values).unwrap();
        assert!(!result.was_transformed);
    }

    #[test]
    fn prepare_update_same_schema() {
        let v2 = make_schema_v2();
        let v2_hash = SchemaHash::compute(&v2);
        let ctx = SchemaContext::new(v2.clone(), "dev", "main");

        let mut branch_map = HashMap::new();
        let branch = "dev-12345678-main".to_string();
        branch_map.insert(branch.clone(), v2_hash);

        let mut writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);

        // Create a row
        let id = ObjectId::new();
        let table = v2.get(&TableName::new("users")).unwrap();
        let original = vec![
            Value::Uuid(id),
            Value::Text("Alice".to_string()),
            Value::Null,
        ];
        let data = encode_row(&table.columns, &original).unwrap();

        writer.cache_row(id, &branch, data, make_commit_id(1));

        // Update it
        let result = writer
            .prepare_update(id, |vals| {
                let mut new = vals.to_vec();
                new[2] = Value::Text("alice@example.com".to_string());
                new
            })
            .unwrap();

        assert!(!result.was_transformed);
        assert_eq!(result.object_id, id);
    }

    #[test]
    fn prepare_update_cross_schema() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        let mut branch_map = HashMap::new();
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        branch_map.insert(v1_branch.clone(), v1_hash);

        let mut writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);

        // Create a row in v1 schema (no email column)
        let id = ObjectId::new();
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let original = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
        let data = encode_row(&v1_table.columns, &original).unwrap();

        writer.cache_row(id, &v1_branch, data, make_commit_id(1));

        // Update it - should transform to v2 first
        let result = writer
            .prepare_update(id, |vals| {
                // vals should now have 3 columns (after transform)
                assert_eq!(vals.len(), 3);
                let mut new = vals.to_vec();
                new[1] = Value::Text("Alice Updated".to_string());
                new[2] = Value::Text("alice@example.com".to_string());
                new
            })
            .unwrap();

        assert!(result.was_transformed);
        assert_eq!(result.source_schema, v1_hash);

        // Verify the result can be decoded with v2 schema
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let decoded = decode_row(&v2_table.columns, &result.data).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[1], Value::Text("Alice Updated".to_string()));
        assert_eq!(decoded[2], Value::Text("alice@example.com".to_string()));
    }

    #[test]
    fn get_write_info_new_row() {
        let v2 = make_schema_v2();
        let ctx = SchemaContext::new(v2, "dev", "main");
        let branch_map = HashMap::new();

        let writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);
        let info = writer.get_write_info(ObjectId::new());

        assert!(!info.is_copy_on_write);
        assert!(info.source_branch.is_none());
    }

    #[test]
    fn get_write_info_copy_on_write() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        let mut branch_map = HashMap::new();
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        branch_map.insert(v1_branch.clone(), v1_hash);

        let mut writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);

        // Cache row from old branch
        let id = ObjectId::new();
        writer.cache_row(id, &v1_branch, vec![], make_commit_id(1));

        let info = writer.get_write_info(id);

        assert!(info.is_copy_on_write);
        assert_eq!(info.source_branch, Some(v1_branch));
    }
}
