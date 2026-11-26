//! Read policies that keep rows to who wrote them (`$createdBy.account =
//! session.user`) without an index on that author: a deployment warns,
//! since every such read then visits every row of the table (#6378).

use std::collections::BTreeSet;

use crate::query::{RelationColumnRef, RelationExpr, RelationPredicate};
use crate::tools::public_schema::PolicyExpr;
use crate::tools::public_schema::{Schema, TableSchema};

use crate::catalogue::Permissions;

const AUTHORS: [&str; 2] = ["$createdBy", "$updatedBy"];

/// The stored author column (`$createdBy`, `$updatedBy`) `column` reads,
/// if any: the record itself or one of its parts.
fn author_of(column: &str) -> Option<&'static str> {
    AUTHORS.into_iter().find(|author| {
        column == *author
            || column
                .strip_prefix(author)
                .is_some_and(|rest| rest.starts_with('.'))
    })
}

/// Whether one of `table`'s declared indexes leads with `author`.
fn indexed(table: &TableSchema, author: &str) -> bool {
    table
        .indexed_columns
        .iter()
        .flatten()
        .any(|column| column.as_str() == author)
        || table.composite_indexes.iter().any(|index| {
            index
                .first()
                .is_some_and(|column| column.as_str() == author)
        })
}

/// `(table, author)` pairs a policy seeks by: its own table's author
/// compared outside a relation, and a relation's scanned table's author
/// compared in its filter (unless the filter also fixes the row's id).
fn sought(policy: &PolicyExpr, own: &str, found: &mut BTreeSet<(String, &'static str)>) {
    match policy {
        PolicyExpr::Cmp { column, .. }
        | PolicyExpr::In { column, .. }
        | PolicyExpr::InList { column, .. } => {
            if let Some(author) = author_of(column) {
                found.insert((own.to_owned(), author));
            }
        }
        PolicyExpr::And(parts) | PolicyExpr::Or(parts) => {
            for part in parts {
                sought(part, own, found);
            }
        }
        // A row of `table` the condition holds for: its author compared
        // there seeks `table`, unless the condition fixes the row's id.
        PolicyExpr::Exists { table, condition } => {
            let conjuncts = match condition.as_ref() {
                PolicyExpr::And(parts) => parts.iter().collect(),
                other => vec![other],
            };
            let by_id = conjuncts
                .iter()
                .any(|part| matches!(part, PolicyExpr::Cmp { column, .. } if column == "id"));
            if !by_id {
                sought(condition, table, found);
            }
        }
        PolicyExpr::ExistsRel { rel } => sought_in(rel, found),
        _ => {}
    }
}

fn sought_in(rel: &RelationExpr, found: &mut BTreeSet<(String, &'static str)>) {
    match rel {
        RelationExpr::Filter { input, predicate } => {
            if let RelationExpr::TableScan { table, alias } = input.as_ref() {
                let names = |column: &RelationColumnRef| {
                    column
                        .scope
                        .as_ref()
                        .is_none_or(|scope| Some(scope) == alias.as_ref() || *scope == *table)
                };
                let conjuncts = match predicate {
                    RelationPredicate::And(parts) => parts.iter().collect(),
                    other => vec![other],
                };
                let by_id = conjuncts.iter().any(|part| {
                    matches!(part, RelationPredicate::Cmp { left, .. }
                        if names(left) && left.column == "id")
                });
                if !by_id {
                    for part in conjuncts {
                        if let RelationPredicate::Cmp { left, .. }
                        | RelationPredicate::In { left, .. } = part
                            && names(left)
                            && let Some(author) = author_of(&left.column)
                        {
                            found.insert((table.as_str().to_owned(), author));
                        }
                    }
                }
            }
            sought_in(input, found);
        }
        RelationExpr::Union { inputs } => {
            for arm in inputs {
                sought_in(&arm.input, found);
            }
        }
        RelationExpr::Join { left, right, .. } => {
            sought_in(left, found);
            sought_in(right, found);
        }
        RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => sought_in(input, found),
        RelationExpr::Gather { seed, .. } => sought_in(seed, found),
        RelationExpr::TableScan { .. } => {}
    }
}

/// One warning per table and author a read policy seeks by with no index
/// leading with that author.
pub(super) fn warnings(current: &Schema, permissions: &Permissions) -> Vec<String> {
    let mut found = BTreeSet::new();
    for (table, policies) in permissions {
        for policy in [&policies.select.using, &policies.select.with_check]
            .into_iter()
            .flatten()
        {
            sought(policy, table.as_str(), &mut found);
        }
    }
    found
        .into_iter()
        .filter(|(table, author)| {
            current
                .iter()
                .find(|(name, _)| name.as_str() == table.as_str())
                .is_some_and(|(_, schema)| !indexed(schema, author))
        })
        .map(|(table, author)| {
            format!(
                "tables.{table}: a read policy keeps rows to `{author}` but no index leads \
                 with it, so every such read visits every row of `{table}`; add \"{author}\" \
                 to its `indexes` in jazz.app.json (`.indexOnly([..., \"{author}\"])` or \
                 `.compositeIndex([\"{author}\", ...])` in a TypeScript schema)"
            )
        })
        .collect()
}
