//! Lens Transformer - Applies lens transforms to rows during materialization.
//!
//! This module provides utilities for transforming rows from old schema versions
//! to the current schema using lenses.

use crate::commit::CommitId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::types::{SchemaHash, TableName};

use super::context::SchemaContext;
use super::lens::Direction;

/// Result of a lens transform operation.
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// Transformed row data.
    pub data: Vec<u8>,
    /// Original commit ID (preserved).
    pub commit_id: CommitId,
    /// Whether the row was transformed (false if already in current schema).
    pub was_transformed: bool,
}

/// Error during lens transformation.
#[derive(Debug, Clone, PartialEq)]
pub enum TransformError {
    /// Failed to decode row with source schema.
    DecodeError(String),
    /// Failed to encode row with target schema.
    EncodeError(String),
    /// No lens path found between schemas.
    NoLensPath {
        source: SchemaHash,
        target: SchemaHash,
    },
    /// Table not found in schema.
    TableNotFound(String),
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformError::DecodeError(msg) => write!(f, "decode error: {}", msg),
            TransformError::EncodeError(msg) => write!(f, "encode error: {}", msg),
            TransformError::NoLensPath { source, target } => {
                write!(
                    f,
                    "no lens path from {} to {}",
                    source.short(),
                    target.short()
                )
            }
            TransformError::TableNotFound(name) => write!(f, "table not found: {}", name),
        }
    }
}

impl std::error::Error for TransformError {}

/// Transforms rows from old schema versions to the current schema.
///
/// Used during materialization to convert rows loaded from old schema branches
/// into the current schema format.
pub struct LensTransformer<'a> {
    context: &'a SchemaContext,
    table: String,
}

impl<'a> LensTransformer<'a> {
    /// Create a new transformer for a specific table.
    pub fn new(context: &'a SchemaContext, table: &str) -> Self {
        Self {
            context,
            table: table.to_string(),
        }
    }

    /// Transform a row from a source schema to the current schema.
    ///
    /// # Arguments
    /// * `data` - Raw row data encoded with source schema
    /// * `commit_id` - Commit ID of the row
    /// * `source_hash` - Schema hash of the source (where row was stored)
    ///
    /// # Returns
    /// Transformed row data and metadata, or error if transform fails.
    pub fn transform(
        &self,
        data: &[u8],
        commit_id: CommitId,
        source_hash: SchemaHash,
    ) -> Result<TransformResult, TransformError> {
        // If already in current schema, no transform needed
        if source_hash == self.context.current_hash {
            return Ok(TransformResult {
                data: data.to_vec(),
                commit_id,
                was_transformed: false,
            });
        }

        // Get source and target descriptors
        let source_schema =
            self.context
                .get_schema(&source_hash)
                .ok_or(TransformError::NoLensPath {
                    source: source_hash,
                    target: self.context.current_hash,
                })?;

        let source_table_name =
            translate_table_name_to_schema(self.context, &self.table, &source_hash)
                .ok_or_else(|| TransformError::TableNotFound(self.table.clone()))?;

        let source_table = source_schema
            .get(&crate::query_manager::types::TableName::new(
                &source_table_name,
            ))
            .ok_or_else(|| TransformError::TableNotFound(source_table_name.clone()))?;

        let target_table = self
            .context
            .current_schema
            .get(&crate::query_manager::types::TableName::new(&self.table))
            .ok_or_else(|| TransformError::TableNotFound(self.table.clone()))?;

        let source_desc = &source_table.columns;
        let target_desc = &target_table.columns;

        // Get lens path and apply transforms
        let lens_path =
            self.context
                .lens_path(&source_hash)
                .map_err(|_| TransformError::NoLensPath {
                    source: source_hash,
                    target: self.context.current_hash,
                })?;

        // Decode row with source schema
        let mut values = decode_row(source_desc, data)
            .map_err(|e| TransformError::DecodeError(format!("{:?}", e)))?;

        // Apply each lens in the path with the appropriate direction
        let mut current_desc = source_desc.clone();
        let mut current_table_name = source_table_name;
        for (lens, direction) in lens_path {
            // Get the next schema based on direction
            // Forward: source -> target, Backward: target -> source
            let next_hash = match direction {
                Direction::Forward => lens.target_hash,
                Direction::Backward => lens.source_hash,
            };

            let next_schema = self.context.get_schema(&next_hash).ok_or({
                TransformError::NoLensPath {
                    source: lens.source_hash,
                    target: lens.target_hash,
                }
            })?;
            let next_table_name = lens
                .translate_table(&current_table_name, direction)
                .ok_or_else(|| TransformError::TableNotFound(current_table_name.clone()))?;
            let next_table = next_schema
                .get(&crate::query_manager::types::TableName::new(
                    &next_table_name,
                ))
                .ok_or_else(|| TransformError::TableNotFound(next_table_name.clone()))?;
            let next_desc = &next_table.columns;

            // Apply lens with the appropriate direction
            values = lens.apply(&values, &current_desc, next_desc, direction);
            current_desc = next_desc.clone();
            current_table_name = next_table_name;
        }

        // Encode with target schema
        let transformed_data = encode_row(target_desc, &values)
            .map_err(|e| TransformError::EncodeError(format!("{:?}", e)))?;

        Ok(TransformResult {
            data: transformed_data,
            commit_id,
            was_transformed: true,
        })
    }
}

/// Translate a column name through the lens chain.
///
/// Used for index lookups: translates column names from current schema
/// to the equivalent column in an old schema (backward direction).
pub fn translate_table_name_to_schema(
    context: &SchemaContext,
    table: &str,
    target_hash: &SchemaHash,
) -> Option<String> {
    if target_hash == &context.current_hash {
        return Some(table.to_string());
    }

    let lens_path = context.lens_path(target_hash).ok()?;
    let mut current_table = table.to_string();
    for (lens, direction) in lens_path.iter().rev() {
        let translate_direction = direction.reverse();
        current_table = lens.translate_table(&current_table, translate_direction)?;

        let next_hash = match translate_direction {
            Direction::Forward => lens.target_hash,
            Direction::Backward => lens.source_hash,
        };
        let next_schema = context.get_schema(&next_hash)?;
        if !next_schema.contains_key(&TableName::new(&current_table)) {
            return None;
        }
    }

    Some(current_table)
}

/// Translate a table name from a source schema forward into the current schema.
pub fn translate_table_name_from_schema(
    context: &SchemaContext,
    table: &str,
    source_hash: &SchemaHash,
) -> Option<String> {
    if source_hash == &context.current_hash {
        return Some(table.to_string());
    }

    let lens_path = context.lens_path(source_hash).ok()?;
    let mut current_table = table.to_string();
    for (lens, direction) in lens_path {
        current_table = lens.translate_table(&current_table, direction)?;

        let next_hash = match direction {
            Direction::Forward => lens.target_hash,
            Direction::Backward => lens.source_hash,
        };
        let next_schema = context.get_schema(&next_hash)?;
        if !next_schema.contains_key(&TableName::new(&current_table)) {
            return None;
        }
    }

    Some(current_table)
}

/// Resolve the logical current-schema table name from an object-level table hint.
///
/// This treats metadata as provenance, not authority. If the hinted table no longer exists
/// in the current schema, we walk live schema versions and translate the old name forward.
pub fn resolve_current_table_name(context: &SchemaContext, hinted_table: &str) -> Option<String> {
    let hinted = TableName::new(hinted_table);
    if context.current_schema.contains_key(&hinted) {
        return Some(hinted_table.to_string());
    }

    let mut candidate: Option<String> = None;
    for (hash, schema) in &context.live_schemas {
        if !schema.contains_key(&hinted) {
            continue;
        }

        let translated = translate_table_name_from_schema(context, hinted_table, hash)?;
        if !context
            .current_schema
            .contains_key(&TableName::new(&translated))
        {
            continue;
        }

        match &candidate {
            Some(existing) if existing != &translated => return None,
            Some(_) => {}
            None => candidate = Some(translated),
        }
    }

    candidate
}

/// Translate a table/column pair through the lens chain for a target schema.
pub fn translate_table_and_column_for_schema(
    context: &SchemaContext,
    table: &str,
    column: &str,
    target_hash: &SchemaHash,
) -> Option<(String, String)> {
    if target_hash == &context.current_hash {
        return Some((table.to_string(), column.to_string()));
    }

    let lens_path = context.lens_path(target_hash).ok()?;
    let mut current_table = table.to_string();
    let mut current_column = column.to_string();
    for (lens, direction) in lens_path.iter().rev() {
        let translate_direction = direction.reverse();
        let (next_table, next_column) =
            lens.translate_table_and_column(&current_table, &current_column, translate_direction)?;
        current_table = next_table;
        current_column = next_column;
    }

    Some((current_table, current_column))
}

pub fn translate_column_for_index(
    context: &SchemaContext,
    table: &str,
    column: &str,
    target_hash: &SchemaHash,
) -> Option<String> {
    translate_table_and_column_for_schema(context, table, column, target_hash)
        .map(|(_, translated_column)| translated_column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema, Value};
    use crate::schema_manager::auto_lens::generate_lens;
    use crate::schema_manager::lens::{Lens, LensOp, LensTransform};

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
    fn transform_no_change_for_current_schema() {
        let v2 = make_schema_v2();
        let v2_hash = SchemaHash::compute(&v2);
        let ctx = SchemaContext::new(v2.clone(), "dev", "main");

        let transformer = LensTransformer::new(&ctx, "users");

        let table = v2
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let values = vec![
            Value::Uuid(ObjectId::new()),
            Value::Text("Alice".to_string()),
            Value::Null,
        ];
        let data = encode_row(&table.columns, &values).unwrap();

        let result = transformer
            .transform(&data, make_commit_id(1), v2_hash)
            .unwrap();

        assert!(!result.was_transformed);
        assert_eq!(result.data, data);
    }

    #[test]
    fn transform_adds_column() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        let transformer = LensTransformer::new(&ctx, "users");

        // Create row with v1 schema (no email column)
        let v1_table = v1
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let id = ObjectId::new();
        let v1_values = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
        let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

        // Transform to v2
        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Decode with v2 schema and verify email is Null
        let v2_table = v2
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let v2_values = decode_row(&v2_table.columns, &result.data).unwrap();

        assert_eq!(v2_values.len(), 3);
        assert_eq!(v2_values[0], Value::Uuid(id));
        assert_eq!(v2_values[1], Value::Text("Alice".to_string()));
        assert_eq!(v2_values[2], Value::Null); // Added column
    }

    #[test]
    fn transform_renamed_table() {
        use crate::schema_manager::lens::{Lens, LensOp, LensTransform};

        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("people")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        let transformer = LensTransformer::new(&ctx, "people");
        let v1_table = v1
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let id = ObjectId::new();
        let v1_values = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
        let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        let v2_table = v2
            .get(&crate::query_manager::types::TableName::new("people"))
            .unwrap();
        let v2_values = decode_row(&v2_table.columns, &result.data).unwrap();
        assert_eq!(v2_values, v1_values);
    }

    #[test]
    fn translate_column_no_change() {
        let v2 = make_schema_v2();
        let v2_hash = SchemaHash::compute(&v2);
        let ctx = SchemaContext::new(v2, "dev", "main");

        let result = translate_column_for_index(&ctx, "users", "name", &v2_hash);
        assert_eq!(result, Some("name".to_string()));
    }

    #[test]
    fn translate_column_through_lens() {
        use crate::schema_manager::lens::{Lens, LensOp, LensTransform};

        // Create schemas where a column was renamed
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        // Query uses "email_address" (current schema)
        // For v1 index, we need "email" (old schema)
        let result = translate_column_for_index(&ctx, "users", "email_address", &v1_hash);
        assert_eq!(result, Some("email".to_string()));
    }

    // ========================================================================
    // Multi-Hop Transform Tests
    // ========================================================================

    fn make_schema_v3() -> crate::query_manager::types::Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .nullable_column("role", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn transform_multi_hop() {
        // v1 row (2 cols) -> v2 (3 cols) -> v3 (4 cols)
        let v1 = make_schema_v1(); // id, name
        let v2 = make_schema_v2(); // id, name, email
        let v3 = make_schema_v3(); // id, name, email, role

        let v1_hash = SchemaHash::compute(&v1);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3.clone(), "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1.clone(), lens_v1_v2);

        let transformer = LensTransformer::new(&ctx, "users");

        // Create row with v1 schema (2 columns: id, name)
        let v1_table = v1
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let id = ObjectId::new();
        let v1_values = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
        let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

        // Transform from v1 to v3 (2 hops)
        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Decode with v3 schema and verify all 4 columns
        let v3_table = v3
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let v3_values = decode_row(&v3_table.columns, &result.data).unwrap();

        assert_eq!(v3_values.len(), 4);
        assert_eq!(v3_values[0], Value::Uuid(id));
        assert_eq!(v3_values[1], Value::Text("Alice".to_string()));
        assert_eq!(v3_values[2], Value::Null); // email added in v1->v2
        assert_eq!(v3_values[3], Value::Null); // role added in v2->v3
    }

    #[test]
    fn transform_multi_hop_from_middle() {
        // v2 row (3 cols) -> v3 (4 cols) - just 1 hop from middle
        let v1 = make_schema_v1();
        let v2 = make_schema_v2(); // id, name, email
        let v3 = make_schema_v3(); // id, name, email, role

        let v2_hash = SchemaHash::compute(&v2);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3.clone(), "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        let transformer = LensTransformer::new(&ctx, "users");

        // Create row with v2 schema (3 columns)
        let v2_table = v2
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let id = ObjectId::new();
        let v2_values = vec![
            Value::Uuid(id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ];
        let v2_data = encode_row(&v2_table.columns, &v2_values).unwrap();

        // Transform from v2 to v3 (1 hop)
        let result = transformer
            .transform(&v2_data, make_commit_id(1), v2_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Decode with v3 schema
        let v3_table = v3
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let v3_values = decode_row(&v3_table.columns, &result.data).unwrap();

        assert_eq!(v3_values.len(), 4);
        assert_eq!(v3_values[0], Value::Uuid(id));
        assert_eq!(v3_values[1], Value::Text("Bob".to_string()));
        assert_eq!(v3_values[2], Value::Text("bob@example.com".to_string())); // preserved
        assert_eq!(v3_values[3], Value::Null); // role added in v2->v3
    }

    #[test]
    fn translate_column_multi_hop_chained_renames() {
        use crate::schema_manager::lens::{Lens, LensOp, LensTransform};

        // Column renamed in v1->v2, then renamed again in v2->v3
        // email -> email_address -> contact_email
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("contact_email", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let v3_hash = SchemaHash::compute(&v3);

        // v1 -> v2: email -> email_address
        let mut transform_v1_v2 = LensTransform::new();
        transform_v1_v2.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens_v1_v2 = Lens::new(v1_hash, v2_hash, transform_v1_v2);

        // v2 -> v3: email_address -> contact_email
        let mut transform_v2_v3 = LensTransform::new();
        transform_v2_v3.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email_address".to_string(),
                new_name: "contact_email".to_string(),
            },
            false,
        );
        let lens_v2_v3 = Lens::new(v2_hash, v3_hash, transform_v2_v3);

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2, lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        // Query uses "contact_email" (current schema v3)
        // For v1 index lookup, we need to translate back through both renames
        let result = translate_column_for_index(&ctx, "users", "contact_email", &v1_hash);
        assert_eq!(result, Some("email".to_string()));

        // For v2 index lookup, we need just one rename back
        let result_v2 = translate_column_for_index(&ctx, "users", "contact_email", &v2_hash);
        assert_eq!(result_v2, Some("email_address".to_string()));
    }

    #[test]
    fn translate_column_added_in_middle() {
        // Column "email" added in v2, doesn't exist in v1
        // Query on "email" for v1 should return None
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .nullable_column("role", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2, lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        // Querying "email" for v1 index - column was added in v2,
        // so it shouldn't exist in v1 indices
        // The backward lens v2->v1 removes the email column
        let result = translate_column_for_index(&ctx, "users", "email", &v1_hash);
        // Backward through v3->v2 keeps email, backward through v2->v1 removes it
        assert_eq!(result, None);

        // Querying "email" for v2 index - column exists
        let result_v2 = translate_column_for_index(&ctx, "users", "email", &v2_hash);
        assert_eq!(result_v2, Some("email".to_string()));

        // Querying "role" for v1/v2 - column added in v3
        let result_role_v1 = translate_column_for_index(&ctx, "users", "role", &v1_hash);
        assert_eq!(result_role_v1, None);

        let result_role_v2 = translate_column_for_index(&ctx, "users", "role", &v2_hash);
        assert_eq!(result_role_v2, None);
    }

    #[test]
    fn transform_multi_hop_with_rename() {
        // v1: users(id, email)
        // v2: users(id, email_address) - renamed
        // v3: users(id, email_address, role) - added column
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text)
                    .nullable_column("role", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // v1 -> v2: rename email -> email_address
        let mut transform_v1_v2 = LensTransform::new();
        transform_v1_v2.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens_v1_v2 = Lens::new(v1_hash, v2_hash, transform_v1_v2);

        // v2 -> v3: add role column
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3.clone(), "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1.clone(), lens_v1_v2);

        let transformer = LensTransformer::new(&ctx, "users");

        // Create row with v1 schema
        let v1_table = v1
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let id = ObjectId::new();
        let v1_values = vec![
            Value::Uuid(id),
            Value::Text("alice@example.com".to_string()),
        ];
        let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

        // Transform v1 -> v3
        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Decode with v3 schema
        let v3_table = v3
            .get(&crate::query_manager::types::TableName::new("users"))
            .unwrap();
        let v3_values = decode_row(&v3_table.columns, &result.data).unwrap();

        assert_eq!(v3_values.len(), 3);
        assert_eq!(v3_values[0], Value::Uuid(id));
        // email value should be preserved under new name email_address
        assert_eq!(v3_values[1], Value::Text("alice@example.com".to_string()));
        assert_eq!(v3_values[2], Value::Null); // role added
    }
}
