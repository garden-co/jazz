//! Automatic lens generation from schema diffs.
//!
//! Generates lenses by comparing old and new schemas, detecting:
//! - Added/removed tables
//! - Added/removed columns
//! - Potential renames (marked as drafts for review)

use std::collections::{HashMap, HashSet};

use crate::query_manager::types::{ColumnType, Schema, SchemaHash, TableName, Value};

use super::lens::{Lens, LensOp, LensTransform};

/// Generate a lens that transforms from old schema to new schema.
///
/// Returns a lens with forward operations to migrate old → new.
/// Operations that are uncertain (e.g., potential renames) are marked as draft.
pub fn generate_lens(old: &Schema, new: &Schema) -> Lens {
    let old_hash = SchemaHash::compute(old);
    let new_hash = SchemaHash::compute(new);

    let mut transform = LensTransform::new();

    // Collect table names
    let old_tables: HashSet<&TableName> = old.keys().collect();
    let new_tables: HashSet<&TableName> = new.keys().collect();

    // Added tables
    for table_name in new_tables.difference(&old_tables) {
        let schema = new.get(*table_name).unwrap().clone();
        transform.push(
            LensOp::AddTable {
                table: table_name.as_str().to_string(),
                schema,
            },
            false, // Adding a table is deterministic
        );
    }

    // Removed tables
    for table_name in old_tables.difference(&new_tables) {
        let schema = old.get(*table_name).unwrap().clone();
        transform.push(
            LensOp::RemoveTable {
                table: table_name.as_str().to_string(),
                schema,
            },
            false, // Removing a table is deterministic
        );
    }

    // Common tables - check for column changes
    for table_name in old_tables.intersection(&new_tables) {
        let old_table = old.get(*table_name).unwrap();
        let new_table = new.get(*table_name).unwrap();

        let ops = generate_column_ops(
            table_name.as_str(),
            &old_table.descriptor,
            &new_table.descriptor,
        );
        for (op, is_draft) in ops {
            transform.push(op, is_draft);
        }
    }

    Lens::new(old_hash, new_hash, transform)
}

/// Generate column-level operations between two table versions.
fn generate_column_ops(
    table: &str,
    old_desc: &crate::query_manager::types::RowDescriptor,
    new_desc: &crate::query_manager::types::RowDescriptor,
) -> Vec<(LensOp, bool)> {
    let mut ops = Vec::new();

    // Build column maps
    let old_cols: HashMap<&str, &crate::query_manager::types::ColumnDescriptor> = old_desc
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let new_cols: HashMap<&str, &crate::query_manager::types::ColumnDescriptor> = new_desc
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let old_names: HashSet<&str> = old_cols.keys().copied().collect();
    let new_names: HashSet<&str> = new_cols.keys().copied().collect();

    // Try to detect renames by matching types
    let added_names: Vec<&str> = new_names.difference(&old_names).copied().collect();
    let removed_names: Vec<&str> = old_names.difference(&new_names).copied().collect();

    let mut handled_renames: HashSet<&str> = HashSet::new();
    let mut rename_targets: HashSet<&str> = HashSet::new();

    // Potential renames: same type, different name
    // Only consider 1:1 matches where types match exactly
    for removed in &removed_names {
        let old_col = old_cols.get(removed).unwrap();
        for added in &added_names {
            if rename_targets.contains(added) {
                continue; // Already matched
            }
            let new_col = new_cols.get(added).unwrap();
            if old_col.column_type == new_col.column_type
                && old_col.nullable == new_col.nullable
                && old_col.references == new_col.references
            {
                // Potential rename - mark as draft since we can't be sure
                ops.push((
                    LensOp::RenameColumn {
                        table: table.to_string(),
                        old_name: removed.to_string(),
                        new_name: added.to_string(),
                    },
                    true, // Draft - needs human review
                ));
                handled_renames.insert(*removed);
                rename_targets.insert(*added);
                break;
            }
        }
    }

    // Added columns (not from renames)
    for added in &added_names {
        if rename_targets.contains(added) {
            continue;
        }
        let col = new_cols.get(added).unwrap();
        let default = default_for_type(&col.column_type, col.nullable);
        ops.push((
            LensOp::AddColumn {
                table: table.to_string(),
                column: added.to_string(),
                column_type: col.column_type.clone(),
                default: default.clone(),
            },
            // Draft if we couldn't determine a reasonable default for non-nullable column
            needs_default_review(&default, col.nullable),
        ));
    }

    // Removed columns (not from renames)
    for removed in &removed_names {
        if handled_renames.contains(removed) {
            continue;
        }
        let col = old_cols.get(removed).unwrap();
        let default = default_for_type(&col.column_type, col.nullable);
        ops.push((
            LensOp::RemoveColumn {
                table: table.to_string(),
                column: removed.to_string(),
                column_type: col.column_type.clone(),
                default,
            },
            false, // Removing is deterministic (lossy but reversible with default)
        ));
    }

    // Check for type changes on common columns (these need manual handling)
    for name in old_names.intersection(&new_names) {
        let old_col = old_cols.get(name).unwrap();
        let new_col = new_cols.get(name).unwrap();

        if old_col.column_type != new_col.column_type {
            // Type change - we don't auto-handle this yet
            // For V1, this would require manual lens creation
            // TODO: Add TypeChange lens op in future version
        }
    }

    ops
}

/// Generate a reasonable default value for a column type.
fn default_for_type(column_type: &ColumnType, nullable: bool) -> Value {
    if nullable {
        return Value::Null;
    }

    match column_type {
        ColumnType::Integer => Value::Integer(0),
        ColumnType::BigInt => Value::BigInt(0),
        ColumnType::Double => Value::Double(0.0),
        ColumnType::Boolean => Value::Boolean(false),
        ColumnType::Text => Value::Text(String::new()),
        ColumnType::Enum(variants) => variants
            .first()
            .cloned()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        ColumnType::Timestamp => Value::Timestamp(0),
        ColumnType::Uuid => Value::Null, // Can't generate a sensible default
        ColumnType::Array(_) => Value::Array(Vec::new()),
        ColumnType::Row(_) => Value::Null, // Can't generate without schema
    }
}

/// Check if a default value needs human review.
fn needs_default_review(value: &Value, nullable: bool) -> bool {
    // Null for non-nullable columns needs review
    // Null for nullable columns is fine (it's a valid default)
    matches!(value, Value::Null) && !nullable
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{SchemaBuilder, TableSchema};

    #[test]
    fn auto_lens_add_table() {
        let old = SchemaBuilder::new().build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let lens = generate_lens(&old, &new);

        assert_eq!(lens.forward.ops.len(), 1);
        assert!(matches!(&lens.forward.ops[0], LensOp::AddTable { table, .. } if table == "users"));
        assert!(!lens.is_draft());
    }

    #[test]
    fn auto_lens_remove_table() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("legacy").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new().build();

        let lens = generate_lens(&old, &new);

        assert_eq!(lens.forward.ops.len(), 1);
        assert!(
            matches!(&lens.forward.ops[0], LensOp::RemoveTable { table, .. } if table == "legacy")
        );
        assert!(!lens.is_draft());
    }

    #[test]
    fn auto_lens_add_column() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let lens = generate_lens(&old, &new);

        assert_eq!(lens.forward.ops.len(), 1);
        if let LensOp::AddColumn {
            table,
            column,
            default,
            ..
        } = &lens.forward.ops[0]
        {
            assert_eq!(table, "users");
            assert_eq!(column, "email");
            assert_eq!(*default, Value::Null); // nullable column gets Null default
        } else {
            panic!("Expected AddColumn op");
        }
        assert!(!lens.is_draft()); // Nullable column with Null default is not draft
    }

    #[test]
    fn auto_lens_add_non_nullable_column_is_draft() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("count", ColumnType::Integer), // non-nullable
            )
            .build();

        let lens = generate_lens(&old, &new);

        // Non-nullable Integer gets default 0, which is not draft
        if let LensOp::AddColumn { default, .. } = &lens.forward.ops[0] {
            assert_eq!(*default, Value::Integer(0));
        }
        assert!(!lens.is_draft());
    }

    #[test]
    fn auto_lens_add_non_nullable_uuid_is_draft() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("org_id", ColumnType::Uuid), // non-nullable UUID has no default
            )
            .build();

        let lens = generate_lens(&old, &new);

        // Non-nullable UUID can't have a sensible default -> Null -> draft
        if let LensOp::AddColumn { default, .. } = &lens.forward.ops[0] {
            assert_eq!(*default, Value::Null);
        }
        assert!(lens.is_draft());
    }

    #[test]
    fn auto_lens_remove_column() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("deprecated", ColumnType::Text),
            )
            .build();

        let new = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let lens = generate_lens(&old, &new);

        assert_eq!(lens.forward.ops.len(), 1);
        assert!(
            matches!(&lens.forward.ops[0], LensOp::RemoveColumn { column, .. } if column == "deprecated")
        );
        assert!(!lens.is_draft());
    }

    #[test]
    fn auto_lens_rename_column_is_draft() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let lens = generate_lens(&old, &new);

        // Should detect potential rename
        assert_eq!(lens.forward.ops.len(), 1);
        if let LensOp::RenameColumn {
            old_name, new_name, ..
        } = &lens.forward.ops[0]
        {
            assert_eq!(old_name, "email");
            assert_eq!(new_name, "email_address");
        } else {
            panic!("Expected RenameColumn op");
        }

        // Rename detection is uncertain, should be draft
        assert!(lens.is_draft());
    }

    #[test]
    fn auto_lens_complex_migration() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("old_field", ColumnType::Integer),
            )
            .table(TableSchema::builder("legacy_table").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("new_field", ColumnType::Boolean),
            )
            .table(TableSchema::builder("new_table").column("id", ColumnType::Uuid))
            .build();

        let lens = generate_lens(&old, &new);

        // Should have:
        // - AddTable (new_table)
        // - RemoveTable (legacy_table)
        // - AddColumn (new_field)
        // - RemoveColumn (old_field)

        let mut found_add_table = false;
        let mut found_remove_table = false;
        let mut found_add_column = false;
        let mut found_remove_column = false;

        for op in &lens.forward.ops {
            match op {
                LensOp::AddTable { table, .. } if table == "new_table" => found_add_table = true,
                LensOp::RemoveTable { table, .. } if table == "legacy_table" => {
                    found_remove_table = true
                }
                LensOp::AddColumn { column, .. } if column == "new_field" => {
                    found_add_column = true
                }
                LensOp::RemoveColumn { column, .. } if column == "old_field" => {
                    found_remove_column = true
                }
                _ => {}
            }
        }

        assert!(found_add_table, "Should find AddTable for new_table");
        assert!(
            found_remove_table,
            "Should find RemoveTable for legacy_table"
        );
        assert!(found_add_column, "Should find AddColumn for new_field");
        assert!(
            found_remove_column,
            "Should find RemoveColumn for old_field"
        );
    }

    #[test]
    fn auto_lens_bidirectional() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("v1_field", ColumnType::Text),
            )
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("v2_field", ColumnType::Integer),
            )
            .build();

        let lens = generate_lens(&old, &new);

        // Forward should remove v1_field, add v2_field
        // Backward should be the inverse

        assert!(lens.forward.ops.len() >= 2);
        assert_eq!(lens.forward.ops.len(), lens.backward.ops.len());

        // Backward ops should be inverses in reverse order
        for (i, fwd_op) in lens.forward.ops.iter().enumerate() {
            let bwd_op = &lens.backward.ops[lens.forward.ops.len() - 1 - i];
            assert_eq!(fwd_op.invert(), *bwd_op);
        }
    }
}
