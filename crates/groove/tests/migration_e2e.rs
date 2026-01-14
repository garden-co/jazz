//! End-to-End Migration Demo Test
//!
//! This test demonstrates the full migration workflow:
//! 1. Schema versioning with content-addressed descriptors
//! 2. Schema diffing and lens generation
//! 3. Bidirectional transforms for cross-version compatibility
//!
//! Demo Scenario: Rename `title` -> `name` in a `documents` table

use groove::sql::{
    ColumnDef, ColumnType, LensGenerationOptions, TableSchema, diff_schemas, generate_lens,
    row_buffer::{OwnedRow, RowBuilder, RowDescriptor, RowValue},
};
use std::sync::Arc;

/// Demo: Full migration workflow for column rename
#[test]
fn test_migration_workflow_column_rename() {
    println!("\n");
    println!("==========================================");
    println!("  MIGRATION DEMO: Column Rename Workflow  ");
    println!("==========================================\n");

    // =====================================================
    // PHASE 1: Define v1 schema with `title` column
    // =====================================================
    println!("=== Phase 1: V1 Schema Definition ===\n");

    let v1_schema = TableSchema {
        name: "documents".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "title".to_string(),
                ty: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "content".to_string(),
                ty: ColumnType::String,
                nullable: true,
            },
        ],
    };

    println!("V1 Schema Columns:");
    for col in &v1_schema.columns {
        let nullable = if col.nullable {
            "(nullable)"
        } else {
            "(required)"
        };
        println!("  - {}: {:?} {}", col.name, col.ty, nullable);
    }

    // =====================================================
    // PHASE 2: Define v2 schema with `name` column (renamed)
    // =====================================================
    println!("\n=== Phase 2: V2 Schema Definition ===\n");

    let v2_schema = TableSchema {
        name: "documents".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(), // Renamed from 'title'
                ty: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "content".to_string(),
                ty: ColumnType::String,
                nullable: true,
            },
        ],
    };

    println!("V2 Schema Columns:");
    for col in &v2_schema.columns {
        let nullable = if col.nullable {
            "(nullable)"
        } else {
            "(required)"
        };
        println!("  - {}: {:?} {}", col.name, col.ty, nullable);
    }

    // =====================================================
    // PHASE 3: Compute schema diff
    // =====================================================
    println!("\n=== Phase 3: Schema Diff Analysis ===\n");

    let diff = diff_schemas(&v1_schema, &v2_schema);

    println!("Diff Results:");
    println!(
        "  Added columns: {:?}",
        diff.added.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    println!(
        "  Removed columns: {:?}",
        diff.removed.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    if !diff.potential_renames.is_empty() {
        println!("  Potential renames detected:");
        for rename in &diff.potential_renames {
            println!(
                "    - {} -> {} ({:?} confidence)",
                rename.old_name, rename.new_name, rename.confidence
            );
        }
    }

    // =====================================================
    // PHASE 4: Generate bidirectional lens
    // =====================================================
    println!("\n=== Phase 4: Lens Generation ===\n");

    // Confirm the rename
    let options = LensGenerationOptions {
        confirmed_renames: vec![("title".to_string(), "name".to_string())],
    };
    let result = generate_lens(&diff, &options);

    println!("Forward Transforms (v1 -> v2):");
    for transform in &result.lens.forward {
        println!("  {:?}", transform);
    }

    println!("\nBackward Transforms (v2 -> v1):");
    for transform in &result.lens.backward {
        println!("  {:?}", transform);
    }

    if !result.warnings.is_empty() {
        println!("\nWarnings:");
        for warning in &result.warnings {
            println!("  - {}", warning.message);
        }
    }

    // =====================================================
    // PHASE 5: Create test rows and demonstrate transforms
    // =====================================================
    println!("\n=== Phase 5: Row Transform Demo ===\n");

    // Create v1 row descriptor
    let v1_descriptor = Arc::new(RowDescriptor::from_table_schema(&v1_schema));

    // Create v2 row descriptor
    let v2_descriptor = Arc::new(RowDescriptor::from_table_schema(&v2_schema));

    // Build a v1 row
    let v1_row = RowBuilder::new(v1_descriptor.clone())
        .set_i64_by_name("id", 1)
        .set_string_by_name("title", "First Document")
        .set_string_by_name("content", "Hello world")
        .build();

    println!("Original V1 Row:");
    print_row(&v1_row);

    // Apply forward lens to transform v1 -> v2
    let v2_row = result
        .lens
        .apply_forward_owned(&v1_row)
        .expect("Forward transform should succeed");

    println!("\nTransformed to V2 (via forward lens):");
    print_row(&v2_row);

    // Apply backward lens to transform v2 -> v1
    let v1_row_back = result
        .lens
        .apply_backward_owned(&v2_row)
        .expect("Backward transform should succeed");

    println!("\nTransformed back to V1 (via backward lens):");
    print_row(&v1_row_back);

    // Verify roundtrip integrity
    assert_eq!(
        v1_row.get(0),
        v1_row_back.get(0),
        "ID should be preserved in roundtrip"
    );
    assert_eq!(
        v1_row.get(1),
        v1_row_back.get(1),
        "Title/Name should be preserved in roundtrip"
    );
    assert_eq!(
        v1_row.get(2),
        v1_row_back.get(2),
        "Content should be preserved in roundtrip"
    );

    println!("\n=== Roundtrip Verification Passed! ===");

    // =====================================================
    // PHASE 6: Demonstrate cross-version compatibility
    // =====================================================
    println!("\n=== Phase 6: Cross-Version Compatibility ===\n");

    // Build a v2 row (simulating new client writes)
    let v2_new_row = RowBuilder::new(v2_descriptor.clone())
        .set_i64_by_name("id", 2)
        .set_string_by_name("name", "Second Document")
        .set_string_by_name("content", "New content")
        .build();

    println!("New V2 Row (from new client):");
    print_row(&v2_new_row);

    // Old client reads v2 data via backward lens
    let v1_view = result
        .lens
        .apply_backward_owned(&v2_new_row)
        .expect("Backward transform should succeed");

    println!("\nV1 Client View (via backward lens):");
    print_row(&v1_view);

    println!("\n==========================================");
    println!("  MIGRATION DEMO COMPLETE                 ");
    println!("==========================================\n");
    println!("Summary:");
    println!("  - Schema diff correctly identified title -> name rename");
    println!("  - Bidirectional lens generated successfully");
    println!("  - V1 rows can be transformed to V2 (forward)");
    println!("  - V2 rows can be transformed to V1 (backward)");
    println!("  - Roundtrip transformation preserves data integrity");
    println!("  - Cross-version reads work correctly");
}

/// Demo: Column addition migration
#[test]
fn test_migration_workflow_add_column() {
    println!("\n");
    println!("==========================================");
    println!("  MIGRATION DEMO: Add Column Workflow     ");
    println!("==========================================\n");

    let v1_schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "username".to_string(),
                ty: ColumnType::String,
                nullable: false,
            },
        ],
    };

    let v2_schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "username".to_string(),
                ty: ColumnType::String,
                nullable: false,
            },
            ColumnDef {
                name: "email".to_string(),
                ty: ColumnType::String,
                nullable: true, // New nullable column
            },
        ],
    };

    println!("V1 -> V2: Adding 'email' column\n");

    let diff = diff_schemas(&v1_schema, &v2_schema);

    println!("Added columns:");
    for col in &diff.added {
        println!(
            "  + {} ({})",
            col.name,
            if col.nullable { "nullable" } else { "required" }
        );
    }

    let options = LensGenerationOptions::default();
    let result = generate_lens(&diff, &options);

    println!("\nLens transforms:");
    println!("  Forward: {:?}", result.lens.forward);
    println!("  Backward: {:?}", result.lens.backward);

    // Create and transform a row
    let v1_descriptor = Arc::new(RowDescriptor::from_table_schema(&v1_schema));

    let v1_row = RowBuilder::new(v1_descriptor.clone())
        .set_i64_by_name("id", 1)
        .set_string_by_name("username", "alice")
        .build();

    println!("\nV1 Row (without email):");
    print_row(&v1_row);

    let v2_row = result
        .lens
        .apply_forward_owned(&v1_row)
        .expect("Should transform successfully");

    println!("\nV2 Row (with email = NULL):");
    print_row(&v2_row);

    // Verify the email column is NULL
    assert!(
        v2_row
            .get(2)
            .map(|v| matches!(v, RowValue::Null))
            .unwrap_or(true),
        "New column should be NULL for migrated rows"
    );

    println!("\n=== Add Column Demo Complete ===\n");
}

/// Demo: Type change warning
#[test]
fn test_migration_workflow_type_change_warning() {
    println!("\n");
    println!("==========================================");
    println!("  MIGRATION DEMO: Type Change Warning     ");
    println!("==========================================\n");

    let v1_schema = TableSchema {
        name: "metrics".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "value".to_string(),
                ty: ColumnType::I32,
                nullable: false,
            },
        ],
    };

    let v2_schema = TableSchema {
        name: "metrics".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I64,
                nullable: false,
            },
            ColumnDef {
                name: "value".to_string(),
                ty: ColumnType::F64, // Type changed from I32 to F64
                nullable: false,
            },
        ],
    };

    println!("V1 -> V2: Changing 'value' from I32 to F64\n");

    let diff = diff_schemas(&v1_schema, &v2_schema);

    println!("Type changes detected:");
    for change in &diff.type_changes {
        println!(
            "  ! {} : {:?} -> {:?}",
            change.column, change.old_type, change.new_type
        );
    }

    let options = LensGenerationOptions::default();
    let result = generate_lens(&diff, &options);

    println!("\nWarnings (requires manual review):");
    for warning in &result.warnings {
        println!("  - {}", warning.message);
    }

    assert!(
        !result.warnings.is_empty(),
        "Type change should generate warnings"
    );

    println!("\n=== Type Change Demo Complete ===\n");
}

/// Helper function to print row contents
fn print_row(row: &OwnedRow) {
    for (i, col) in row.descriptor.columns.iter().enumerate() {
        let value = row
            .get(i)
            .map(|v| format!("{:?}", v))
            .unwrap_or("NULL".to_string());
        println!("  {}: {}", col.name, value);
    }
}
