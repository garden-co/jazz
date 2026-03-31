//! Schema diffing for automatic lens generation.
//!
//! This module computes the difference between two schemas and generates
//! a LensTransform that can transform data from the old schema to the new one.
//!
//! # Heuristics
//!
//! - New column in new schema → `AddColumn` with schema default when present (otherwise `NULL`)
//! - Missing column in new schema → `RemoveColumn` with schema default when present (otherwise `NULL`)
//! - Column type change → Marked as ambiguity (requires manual review)
//! - Possible rename (same type, one added + one removed) → `RenameColumn` marked as draft

use crate::query_manager::types::{ColumnType, Schema, TableSchema, Value};

use super::lens::{LensOp, LensTransform};

/// Result of a schema diff operation.
#[derive(Debug, Clone)]
pub struct DiffResult {
    /// The generated lens transform.
    pub transform: LensTransform,
    /// Ambiguities that require manual review.
    pub ambiguities: Vec<Ambiguity>,
    /// Table-level changes observed between the schemas.
    pub table_changes: Vec<TableChange>,
}

/// A table-level change detected during schema diffing.
#[derive(Debug, Clone, PartialEq)]
pub enum TableChange {
    Added {
        table: String,
    },
    Removed {
        table: String,
    },
    PossibleRename {
        old_table: String,
        new_table: String,
    },
}

/// An ambiguity detected during schema diffing.
#[derive(Debug, Clone, PartialEq)]
pub enum Ambiguity {
    /// A table might be a rename.
    PossibleTableRename {
        old_table: String,
        new_table: String,
    },
    /// A column might be a rename (same type, one added + one removed).
    PossibleRename {
        table: String,
        old_col: String,
        new_col: String,
    },
    /// A column's type changed (requires manual migration).
    TypeChange {
        table: String,
        column: String,
        old_type: ColumnType,
        new_type: ColumnType,
    },
}

impl std::fmt::Display for Ambiguity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ambiguity::PossibleTableRename {
                old_table,
                new_table,
            } => {
                write!(
                    f,
                    "Possible table rename: {} -> {} (same structure)",
                    old_table, new_table
                )
            }
            Ambiguity::PossibleRename {
                table,
                old_col,
                new_col,
            } => {
                write!(
                    f,
                    "Possible rename in {}: {} -> {} (same type)",
                    table, old_col, new_col
                )
            }
            Ambiguity::TypeChange {
                table,
                column,
                old_type,
                new_type,
            } => {
                write!(
                    f,
                    "Type change in {}.{}: {:?} -> {:?}",
                    table, column, old_type, new_type
                )
            }
        }
    }
}

/// Compute the difference between two schemas.
///
/// Returns a LensTransform that transforms `old` into `new`,
/// along with any ambiguities that require manual review.
pub fn diff_schemas(old: &Schema, new: &Schema) -> DiffResult {
    let mut transform = LensTransform::new();
    let mut ambiguities = Vec::new();
    let mut table_changes = Vec::new();

    // Collect all table names
    let old_tables: std::collections::HashSet<_> = old.keys().collect();
    let new_tables: std::collections::HashSet<_> = new.keys().collect();
    let mut removed_tables: Vec<_> = old_tables.difference(&new_tables).copied().collect();
    let mut added_tables: Vec<_> = new_tables.difference(&old_tables).copied().collect();

    removed_tables.sort_by_key(|table_name| table_name.as_str());
    added_tables.sort_by_key(|table_name| table_name.as_str());

    let rename_pairs = detect_possible_table_renames(old, new, &removed_tables, &added_tables);
    let matched_removed: std::collections::HashSet<_> = rename_pairs
        .iter()
        .map(|(removed_idx, _)| *removed_idx)
        .collect();
    let matched_added: std::collections::HashSet<_> = rename_pairs
        .iter()
        .map(|(_, added_idx)| *added_idx)
        .collect();

    for (removed_idx, added_idx) in rename_pairs {
        let old_table = removed_tables[removed_idx].as_str().to_string();
        let new_table = added_tables[added_idx].as_str().to_string();
        transform.push(
            LensOp::RenameTable {
                old_name: old_table.clone(),
                new_name: new_table.clone(),
            },
            true,
        );
        ambiguities.push(Ambiguity::PossibleTableRename {
            old_table: old_table.clone(),
            new_table: new_table.clone(),
        });
        table_changes.push(TableChange::PossibleRename {
            old_table,
            new_table,
        });
    }

    table_changes.extend(
        removed_tables
            .iter()
            .enumerate()
            .filter(|(idx, _)| !matched_removed.contains(idx))
            .map(|(_, table_name)| TableChange::Removed {
                table: table_name.as_str().to_string(),
            }),
    );
    table_changes.extend(
        added_tables
            .iter()
            .enumerate()
            .filter(|(idx, _)| !matched_added.contains(idx))
            .map(|(_, table_name)| TableChange::Added {
                table: table_name.as_str().to_string(),
            }),
    );

    // Tables in both (need to diff columns)
    for table_name in old_tables.intersection(&new_tables) {
        let old_table = &old[*table_name];
        let new_table = &new[*table_name];
        diff_table(
            table_name.as_str(),
            old_table,
            new_table,
            &mut transform,
            &mut ambiguities,
        );
    }

    DiffResult {
        transform,
        ambiguities,
        table_changes,
    }
}

fn detect_possible_table_renames(
    old: &Schema,
    new: &Schema,
    removed_tables: &[&crate::query_manager::types::TableName],
    added_tables: &[&crate::query_manager::types::TableName],
) -> Vec<(usize, usize)> {
    let removed_candidates: Vec<Vec<usize>> = removed_tables
        .iter()
        .map(|old_table_name| {
            added_tables
                .iter()
                .enumerate()
                .filter_map(|(added_idx, new_table_name)| {
                    (old[*old_table_name] == new[*new_table_name]).then_some(added_idx)
                })
                .collect()
        })
        .collect();
    let added_candidates: Vec<Vec<usize>> = added_tables
        .iter()
        .map(|new_table_name| {
            removed_tables
                .iter()
                .enumerate()
                .filter_map(|(removed_idx, old_table_name)| {
                    (new[*new_table_name] == old[*old_table_name]).then_some(removed_idx)
                })
                .collect()
        })
        .collect();

    removed_candidates
        .iter()
        .enumerate()
        .filter_map(|(removed_idx, candidate_added_tables)| {
            let &[added_idx] = candidate_added_tables.as_slice() else {
                return None;
            };
            let &[candidate_removed_idx] = added_candidates[added_idx].as_slice() else {
                return None;
            };
            (candidate_removed_idx == removed_idx).then_some((removed_idx, added_idx))
        })
        .collect()
}

/// Diff two table schemas and add operations to the transform.
fn diff_table(
    table_name: &str,
    old: &TableSchema,
    new: &TableSchema,
    transform: &mut LensTransform,
    ambiguities: &mut Vec<Ambiguity>,
) {
    let old_cols: std::collections::HashMap<_, _> = old
        .columns
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let new_cols: std::collections::HashMap<_, _> = new
        .columns
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let old_names: std::collections::HashSet<_> = old_cols.keys().copied().collect();
    let new_names: std::collections::HashSet<_> = new_cols.keys().copied().collect();

    // Columns only in old (removed or renamed)
    let removed: Vec<_> = old_names.difference(&new_names).copied().collect();

    // Columns only in new (added or renamed)
    let added: Vec<_> = new_names.difference(&old_names).copied().collect();

    // Columns in both (check for type changes)
    for col_name in old_names.intersection(&new_names) {
        let old_col = old_cols[*col_name];
        let new_col = new_cols[*col_name];

        if old_col.column_type != new_col.column_type {
            // Type changed - this is an ambiguity
            ambiguities.push(Ambiguity::TypeChange {
                table: table_name.to_string(),
                column: col_name.to_string(),
                old_type: old_col.column_type.clone(),
                new_type: new_col.column_type.clone(),
            });
        }
        // Note: nullable changes don't affect the lens transform
        // (they're constraints, not structural)
    }

    // Try to detect renames: same type, one added + one removed
    let mut handled_removed: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut handled_added: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for old_col_name in &removed {
        let old_col = old_cols[*old_col_name];

        // Find an added column with the same type
        for new_col_name in &added {
            if handled_added.contains(*new_col_name) {
                continue;
            }

            let new_col = new_cols[*new_col_name];
            if old_col.column_type == new_col.column_type
                && old_col.nullable == new_col.nullable
                && old_col.references == new_col.references
            {
                // Possible rename - emit as draft
                transform.push(
                    LensOp::RenameColumn {
                        table: table_name.to_string(),
                        old_name: old_col_name.to_string(),
                        new_name: new_col_name.to_string(),
                    },
                    true, // Draft - needs review
                );

                ambiguities.push(Ambiguity::PossibleRename {
                    table: table_name.to_string(),
                    old_col: old_col_name.to_string(),
                    new_col: new_col_name.to_string(),
                });

                handled_removed.insert(*old_col_name);
                handled_added.insert(*new_col_name);
                break;
            }
        }
    }

    // Remaining removed columns (not handled as renames)
    for old_col_name in &removed {
        if handled_removed.contains(*old_col_name) {
            continue;
        }
        let old_col = old_cols[*old_col_name];
        transform.push(
            LensOp::RemoveColumn {
                table: table_name.to_string(),
                column: old_col_name.to_string(),
                column_type: old_col.column_type.clone(),
                default: lens_default_for_column(old_col),
            },
            false,
        );
    }

    // Remaining added columns (not handled as renames)
    for new_col_name in &added {
        if handled_added.contains(*new_col_name) {
            continue;
        }
        let new_col = new_cols[*new_col_name];
        let default = lens_default_for_column(new_col);
        transform.push(
            LensOp::AddColumn {
                table: table_name.to_string(),
                column: new_col_name.to_string(),
                column_type: new_col.column_type.clone(),
                default: default.clone(),
            },
            needs_default_review(&default, new_col.nullable),
        );
    }
}

/// Prefer an explicit schema default, then fall back to a heuristic.
fn lens_default_for_column(col: &crate::query_manager::types::ColumnDescriptor) -> Value {
    col.default
        .clone()
        .unwrap_or_else(|| heuristic_default_for_type(&col.column_type, col.nullable))
}

/// Get a reasonable heuristic default value for a column type.
fn heuristic_default_for_type(ct: &ColumnType, nullable: bool) -> Value {
    if nullable {
        return Value::Null;
    }

    match ct {
        ColumnType::Integer => Value::Integer(0),
        ColumnType::BigInt => Value::BigInt(0),
        ColumnType::Double => Value::Double(0.0),
        ColumnType::Boolean => Value::Boolean(false),
        ColumnType::Text => Value::Text(String::new()),
        ColumnType::Enum { variants } => variants
            .first()
            .cloned()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        ColumnType::Timestamp => Value::Timestamp(0),
        ColumnType::Uuid => Value::Null, // Can't generate a sensible default
        ColumnType::Bytea => Value::Bytea(vec![]),
        ColumnType::Json { schema: _ } => Value::Null,
        ColumnType::Array { element: _ } => Value::Array(vec![]),
        ColumnType::Row { columns: _ } => Value::Null,
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
    use crate::object::ObjectId;
    use crate::query_manager::types::{
        ColumnDescriptor, RowDescriptor, SchemaBuilder, TableName, TableSchema,
    };

    fn make_schema(tables: Vec<(&str, Vec<(&str, ColumnType)>)>) -> Schema {
        tables
            .into_iter()
            .map(|(name, cols)| {
                let columns: Vec<ColumnDescriptor> = cols
                    .into_iter()
                    .map(|(n, t)| ColumnDescriptor::new(n, t))
                    .collect();
                (
                    TableName::new(name),
                    TableSchema::new(RowDescriptor::new(columns)),
                )
            })
            .collect()
    }

    #[test]
    fn diff_identical_schemas() {
        let schema = make_schema(vec![("users", vec![("id", ColumnType::Text)])]);

        let result = diff_schemas(&schema, &schema);

        assert!(result.transform.ops.is_empty());
        assert!(result.ambiguities.is_empty());
        assert!(result.table_changes.is_empty());
    }

    #[test]
    fn diff_add_column() {
        let old = make_schema(vec![("users", vec![("id", ColumnType::Text)])]);
        let new = make_schema(vec![(
            "users",
            vec![("id", ColumnType::Text), ("name", ColumnType::Text)],
        )]);

        let result = diff_schemas(&old, &new);

        assert_eq!(result.transform.ops.len(), 1);
        assert!(result.ambiguities.is_empty());
        assert!(result.table_changes.is_empty());

        match &result.transform.ops[0] {
            LensOp::AddColumn {
                table,
                column,
                column_type,
                ..
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
                assert_eq!(*column_type, ColumnType::Text);
            }
            _ => panic!("Expected AddColumn"),
        }
    }

    #[test]
    fn diff_remove_column() {
        let old = make_schema(vec![(
            "users",
            vec![("id", ColumnType::Text), ("deprecated", ColumnType::Text)],
        )]);
        let new = make_schema(vec![("users", vec![("id", ColumnType::Text)])]);

        let result = diff_schemas(&old, &new);

        assert_eq!(result.transform.ops.len(), 1);
        assert!(result.ambiguities.is_empty());
        assert!(result.table_changes.is_empty());

        match &result.transform.ops[0] {
            LensOp::RemoveColumn { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "deprecated");
            }
            _ => panic!("Expected RemoveColumn"),
        }
    }

    #[test]
    fn diff_type_change() {
        let old = make_schema(vec![("users", vec![("age", ColumnType::Text)])]);
        let new = make_schema(vec![("users", vec![("age", ColumnType::Integer)])]);

        let result = diff_schemas(&old, &new);

        // Type changes don't generate ops, just ambiguities
        assert!(result.transform.ops.is_empty());
        assert_eq!(result.ambiguities.len(), 1);
        assert!(result.table_changes.is_empty());

        match &result.ambiguities[0] {
            Ambiguity::TypeChange {
                table,
                column,
                old_type,
                new_type,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
                assert_eq!(*old_type, ColumnType::Text);
                assert_eq!(*new_type, ColumnType::Integer);
            }
            _ => panic!("Expected TypeChange"),
        }
    }

    #[test]
    fn diff_possible_rename() {
        let old = make_schema(vec![("users", vec![("email", ColumnType::Text)])]);
        let new = make_schema(vec![("users", vec![("email_address", ColumnType::Text)])]);

        let result = diff_schemas(&old, &new);

        // Should detect a possible rename
        assert_eq!(result.transform.ops.len(), 1);
        assert_eq!(result.ambiguities.len(), 1);
        assert!(result.transform.has_drafts());
        assert!(result.table_changes.is_empty());

        match &result.transform.ops[0] {
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => {
                assert_eq!(table, "users");
                assert_eq!(old_name, "email");
                assert_eq!(new_name, "email_address");
            }
            _ => panic!("Expected RenameColumn"),
        }

        match &result.ambiguities[0] {
            Ambiguity::PossibleRename {
                table,
                old_col,
                new_col,
            } => {
                assert_eq!(table, "users");
                assert_eq!(old_col, "email");
                assert_eq!(new_col, "email_address");
            }
            _ => panic!("Expected PossibleRename"),
        }
    }

    #[test]
    fn diff_possible_table_rename() {
        let old = make_schema(vec![("users", vec![("email", ColumnType::Text)])]);
        let new = make_schema(vec![("people", vec![("email", ColumnType::Text)])]);

        let result = diff_schemas(&old, &new);

        assert_eq!(
            result.transform.ops,
            vec![LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            }]
        );
        assert!(result.transform.has_drafts());
        assert_eq!(
            result.ambiguities,
            vec![Ambiguity::PossibleTableRename {
                old_table: "users".to_string(),
                new_table: "people".to_string(),
            }]
        );
        assert_eq!(
            result.table_changes,
            vec![TableChange::PossibleRename {
                old_table: "users".to_string(),
                new_table: "people".to_string(),
            }]
        );
    }

    #[test]
    fn diff_pairs_multiple_unique_table_renames() {
        let old = make_schema(vec![
            ("orgs", vec![("name", ColumnType::Text)]),
            ("users", vec![("email", ColumnType::Text)]),
        ]);
        let new = make_schema(vec![
            ("companies", vec![("name", ColumnType::Text)]),
            ("people", vec![("email", ColumnType::Text)]),
        ]);

        let result = diff_schemas(&old, &new);

        assert_eq!(
            result.transform.ops,
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
        assert!(result.transform.has_drafts());
        assert_eq!(
            result.ambiguities,
            vec![
                Ambiguity::PossibleTableRename {
                    old_table: "orgs".to_string(),
                    new_table: "companies".to_string(),
                },
                Ambiguity::PossibleTableRename {
                    old_table: "users".to_string(),
                    new_table: "people".to_string(),
                },
            ]
        );
        assert_eq!(
            result.table_changes,
            vec![
                TableChange::PossibleRename {
                    old_table: "orgs".to_string(),
                    new_table: "companies".to_string(),
                },
                TableChange::PossibleRename {
                    old_table: "users".to_string(),
                    new_table: "people".to_string(),
                },
            ]
        );
    }

    #[test]
    fn diff_leaves_duplicate_table_shapes_unpaired() {
        let old = make_schema(vec![
            ("admins", vec![("email", ColumnType::Text)]),
            ("users", vec![("email", ColumnType::Text)]),
        ]);
        let new = make_schema(vec![
            ("members", vec![("email", ColumnType::Text)]),
            ("people", vec![("email", ColumnType::Text)]),
        ]);

        let result = diff_schemas(&old, &new);

        assert!(
            result.transform.ops.is_empty(),
            "ambiguous same-shape tables should not be auto-paired"
        );
        assert!(result.ambiguities.is_empty());
        assert_eq!(
            result.table_changes,
            vec![
                TableChange::Removed {
                    table: "admins".to_string(),
                },
                TableChange::Removed {
                    table: "users".to_string(),
                },
                TableChange::Added {
                    table: "members".to_string(),
                },
                TableChange::Added {
                    table: "people".to_string(),
                },
            ]
        );
    }

    #[test]
    fn diff_complex_changes() {
        // Old: users(id, name, deprecated)
        // New: users(id, name, age), posts(id, title)
        let old = make_schema(vec![(
            "users",
            vec![
                ("id", ColumnType::Text),
                ("name", ColumnType::Text),
                ("deprecated", ColumnType::Text),
            ],
        )]);
        let new = make_schema(vec![
            (
                "users",
                vec![
                    ("id", ColumnType::Text),
                    ("name", ColumnType::Text),
                    ("age", ColumnType::Integer),
                ],
            ),
            (
                "posts",
                vec![("id", ColumnType::Text), ("title", ColumnType::Text)],
            ),
        ]);

        let result = diff_schemas(&old, &new);

        // Should have:
        // - RemoveColumn deprecated
        // - AddColumn age
        assert_eq!(result.transform.ops.len(), 2);

        // Count operation types
        let mut add_col = 0;
        let mut remove_col = 0;

        for op in &result.transform.ops {
            match op {
                LensOp::AddColumn { .. } => add_col += 1,
                LensOp::RemoveColumn { .. } => remove_col += 1,
                _ => {}
            }
        }

        assert_eq!(add_col, 1);
        assert_eq!(remove_col, 1);
        assert_eq!(
            result.table_changes,
            vec![TableChange::Added {
                table: "posts".to_string(),
            }]
        );
    }

    #[test]
    fn diff_add_column_with_correct_default() {
        let old = make_schema(vec![("t", vec![("a", ColumnType::Text)])]);
        let new = make_schema(vec![(
            "t",
            vec![
                ("a", ColumnType::Text),
                ("b", ColumnType::Integer),
                ("c", ColumnType::Boolean),
            ],
        )]);

        let result = diff_schemas(&old, &new);

        assert_eq!(result.transform.ops.len(), 2);

        for op in &result.transform.ops {
            match op {
                LensOp::AddColumn {
                    column,
                    column_type,
                    default,
                    ..
                } => match column.as_str() {
                    "b" => {
                        assert_eq!(*column_type, ColumnType::Integer);
                        assert_eq!(*default, Value::Integer(0));
                    }
                    "c" => {
                        assert_eq!(*column_type, ColumnType::Boolean);
                        assert_eq!(*default, Value::Boolean(false));
                    }
                    _ => panic!("Unexpected column"),
                },
                _ => panic!("Expected AddColumn"),
            }
        }
    }

    #[test]
    fn diff_add_column_prefers_explicit_schema_default() {
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

        let result = diff_schemas(&old, &new);

        match &result.transform.ops[0] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Uuid(default_org_id));
            }
            _ => panic!("Expected AddColumn"),
        }
        assert!(
            !result.transform.has_drafts(),
            "an explicit schema default should avoid the UUID draft fallback"
        );
    }

    #[test]
    fn diff_remove_column_prefers_explicit_schema_default() {
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

        let result = diff_schemas(&old, &new);

        match &result.transform.ops[0] {
            LensOp::RemoveColumn { default, .. } => {
                assert_eq!(*default, Value::Text("member".into()));
            }
            _ => panic!("Expected RemoveColumn"),
        }
    }

    #[test]
    fn diff_add_non_nullable_uuid_is_draft_without_explicit_default() {
        let old = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();
        let new = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("org_id", ColumnType::Uuid),
            )
            .build();

        let result = diff_schemas(&old, &new);

        match &result.transform.ops[0] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Null);
            }
            _ => panic!("Expected AddColumn"),
        }
        assert!(result.transform.has_drafts());
    }

    #[test]
    fn diff_result_display() {
        let ambiguity = Ambiguity::TypeChange {
            table: "users".to_string(),
            column: "age".to_string(),
            old_type: ColumnType::Text,
            new_type: ColumnType::Integer,
        };

        let display = format!("{}", ambiguity);
        assert!(display.contains("users"));
        assert!(display.contains("age"));
        assert!(display.contains("Text"));
        assert!(display.contains("Integer"));
    }
}
