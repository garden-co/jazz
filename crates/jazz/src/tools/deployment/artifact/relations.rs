//! An app's relations, resolved and checked once for every client: the
//! loader refuses a file whose relations don't resolve, the FFI follows
//! them for `include` and `hopTo`, and TS's `analyzeRelations`
//! (`packages/jazz-tools/src/codegen/relation-analyzer.ts`) is pinned to
//! the same results by `crates/jazz/tests/fixtures/relations_parity.json`.

use std::collections::BTreeMap;

use super::{Codegen, CodegenRelation};
use crate::tools::{ColumnType, Schema, TableName, TableSchema};

/// Whether a relation follows this table's reference or another table's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    /// This table's column references the target's `id`.
    Forward,
    /// The target table's column references this table's `id`.
    Reverse,
}

/// One relation of a table, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRelation {
    /// Its name, e.g. `owner` or `todosViaOwner`.
    pub name: String,
    /// Forward or reverse.
    pub kind: RelationKind,
    /// The table it's declared on.
    pub from_table: String,
    /// The table it leads to.
    pub to_table: String,
    /// The column matched on this table's side: the reference column for a
    /// forward relation, `id` for a reverse one.
    pub from_column: String,
    /// The column matched on the target's side: `id` for a forward
    /// relation, the target's reference column for a reverse one.
    pub to_column: String,
    /// Whether it leads to many rows: every reverse relation, and a forward
    /// one over a `uuid[]` column.
    pub is_array: bool,
    /// Whether a forward relation's column may be null (never for reverse).
    pub nullable: bool,
}

/// Every table's relations, in declared-name order.
pub type Relations = BTreeMap<String, Vec<ResolvedRelation>>;

/// Names a relation can't take: they collide with a row's own fields or
/// with JavaScript object internals in the generated clients.
fn reserved(name: &str) -> bool {
    name.is_empty()
        || name == "id"
        || name.starts_with('$')
        || matches!(name, "__proto__" | "constructor" | "prototype")
}

fn column<'a>(schema: &'a TableSchema, name: &str) -> Option<&'a crate::tools::ColumnDescriptor> {
    schema.columns.columns.iter().find(|c| c.name_str() == name)
}

/// Resolves and checks the relations `codegen` declares over `current`.
/// Errors are `(path, message)`.
pub fn resolve_relations(
    codegen: &Codegen,
    current: &Schema,
) -> Result<Relations, (String, String)> {
    let table = |path: &str, name: &str| {
        current.get(&TableName::from(name)).ok_or_else(|| {
            (
                path.to_owned(),
                format!(
                    "the current schema has no table {name}{}",
                    super::expr::did_you_mean(name, current.keys().map(|t| t.as_str()))
                ),
            )
        })
    };
    let mut resolved = Relations::new();
    for (name, entry) in &codegen.tables {
        let schema = table(&format!("tables.{name}"), name)?;
        let mut list = Vec::new();
        for (relation, declared) in &entry.relations {
            let path = format!("tables.{name}.relations.{relation}");
            if relation == "id" || column(schema, relation).is_some() {
                return Err((
                    path,
                    format!(
                        "table {name} already has a column {relation}; a relation needs its own name, e.g. `{relation}Row`"
                    ),
                ));
            }
            if reserved(relation) {
                return Err((
                    path,
                    format!("`{relation}` is reserved; a relation needs another name"),
                ));
            }
            // The forward relation this one follows, as (table holding the
            // column, column, target).
            let (holder, holder_name, column_name, target) = match declared {
                CodegenRelation::Forward {
                    table: other,
                    column,
                } => {
                    table(&path, other)?;
                    (schema, name.as_str(), column.as_str(), other.as_str())
                }
                CodegenRelation::Reverse {
                    table: other,
                    relation: forward,
                } => {
                    let other_schema = table(&path, other)?;
                    let Some(CodegenRelation::Forward { column, .. }) = codegen
                        .tables
                        .get(other)
                        .and_then(|other| other.relations.get(forward))
                        .filter(|forward| {
                            matches!(forward, CodegenRelation::Forward { table, .. } if table == name)
                        })
                    else {
                        return Err((
                            path,
                            format!("{other} has no forward relation {forward} to {name}"),
                        ));
                    };
                    (other_schema, other.as_str(), column.as_str(), name.as_str())
                }
            };
            let Some(stored) = column(holder, column_name) else {
                return Err((
                    path,
                    format!("table {holder_name} has no column {column_name}"),
                ));
            };
            let many = match &stored.column_type {
                ColumnType::Uuid => false,
                ColumnType::Array { element } if **element == ColumnType::Uuid => true,
                _ => {
                    return Err((
                        path,
                        format!(
                            "column {holder_name}.{column_name} must be a uuid or uuid[] to hold a relation"
                        ),
                    ));
                }
            };
            if let Some(references) = stored.references.as_ref().filter(|t| t.as_str() != target) {
                return Err((
                    path,
                    format!(
                        "column {column_name} references {references}, so a relation over it \
                         leads to {references}, not {target}"
                    ),
                ));
            }
            let forward = matches!(declared, CodegenRelation::Forward { .. });
            list.push(ResolvedRelation {
                name: relation.clone(),
                kind: if forward {
                    RelationKind::Forward
                } else {
                    RelationKind::Reverse
                },
                from_table: name.clone(),
                to_table: match declared {
                    CodegenRelation::Forward { table, .. }
                    | CodegenRelation::Reverse { table, .. } => table.clone(),
                },
                from_column: if forward {
                    column_name.to_owned()
                } else {
                    "id".to_owned()
                },
                to_column: if forward {
                    "id".to_owned()
                } else {
                    column_name.to_owned()
                },
                is_array: !forward || many,
                nullable: forward && stored.nullable,
            });
        }
        resolved.insert(name.clone(), list);
    }
    Ok(resolved)
}
