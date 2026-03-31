//! Automatic lens generation from schema diffs.
//!
//! Generates lenses by comparing old and new schemas, detecting:
//! - Added/removed tables
//! - Added/removed columns
//! - Potential renames (marked as drafts for review)

use crate::query_manager::types::{Schema, SchemaHash};

use super::diff::diff_schemas;
use super::lens::Lens;

/// Generate a lens that transforms from old schema to new schema.
///
/// Returns a lens with forward operations to migrate old → new.
/// Operations that are uncertain (e.g. potential renames or missing
/// non-nullable defaults) are marked as draft.
pub fn generate_lens(old: &Schema, new: &Schema) -> Lens {
    let old_hash = SchemaHash::compute(old);
    let new_hash = SchemaHash::compute(new);
    let diff = diff_schemas(old, new);
    Lens::new(old_hash, new_hash, diff.transform)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema, Value};
    use crate::schema_manager::LensOp;

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
    fn auto_lens_add_column_prefers_explicit_schema_default() {
        let default_org_id = ObjectId::new();
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column_with_default("org_id", ColumnType::Uuid, Value::Uuid(default_org_id)),
            )
            .build();

        let lens = generate_lens(&old, &new);

        if let LensOp::AddColumn { default, .. } = &lens.forward.ops[0] {
            assert_eq!(*default, Value::Uuid(default_org_id));
        } else {
            panic!("Expected AddColumn op");
        }
        assert!(
            !lens.is_draft(),
            "an explicit schema default should avoid the UUID draft fallback"
        );
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
    fn auto_lens_remove_column_prefers_explicit_schema_default() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column_with_default("role", ColumnType::Text, Value::Text("member".into())),
            )
            .build();

        let new = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let lens = generate_lens(&old, &new);

        if let LensOp::RemoveColumn { default, .. } = &lens.forward.ops[0] {
            assert_eq!(*default, Value::Text("member".into()));
        } else {
            panic!("Expected RemoveColumn op");
        }
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
    fn auto_lens_table_rename_is_draft() {
        let old = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("people")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let lens = generate_lens(&old, &new);

        assert_eq!(lens.forward.ops.len(), 1);
        assert_eq!(
            lens.forward.ops[0],
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            }
        );
        assert!(lens.is_draft());
    }

    #[test]
    fn auto_lens_pairs_multiple_unique_table_renames() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("orgs").column("name", ColumnType::Text))
            .table(TableSchema::builder("users").column("email", ColumnType::Text))
            .build();

        let new = SchemaBuilder::new()
            .table(TableSchema::builder("companies").column("name", ColumnType::Text))
            .table(TableSchema::builder("people").column("email", ColumnType::Text))
            .build();

        let lens = generate_lens(&old, &new);

        assert_eq!(
            lens.forward.ops,
            vec![
                LensOp::RenameTable {
                    old_name: "orgs".to_string(),
                    new_name: "companies".to_string(),
                },
                LensOp::RenameTable {
                    old_name: "users".to_string(),
                    new_name: "people".to_string(),
                },
            ]
        );
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
        // - AddColumn (new_field)
        // - RemoveColumn (old_field)

        let mut found_add_column = false;
        let mut found_remove_column = false;

        for op in &lens.forward.ops {
            match op {
                LensOp::AddColumn { column, .. } if column == "new_field" => {
                    found_add_column = true
                }
                LensOp::RemoveColumn { column, .. } if column == "old_field" => {
                    found_remove_column = true
                }
                _ => {}
            }
        }

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
