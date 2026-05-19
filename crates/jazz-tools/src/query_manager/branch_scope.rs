use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::object::{BranchName, ObjectId};
use crate::row_histories::BatchId;

use super::query::{Condition, Query};
use super::types::{Row, RowDescriptor, Tuple, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchScopeEntry {
    pub table: String,
    pub row_id: ObjectId,
    pub base_branch: BranchName,
    pub base_batch_id: BatchId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchScopeSnapshot {
    pub branch_id: ObjectId,
    pub scope_query: Query,
    pub scope_query_hash: String,
    pub entries: Vec<BranchScopeEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchDiffKind {
    Insert,
    Update,
    Delete,
    Unchanged,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchDiffRow {
    pub row_id: ObjectId,
    pub kind: BranchDiffKind,
    pub changed: Vec<String>,
    pub conflicts: Vec<String>,
    pub row: Option<Row>,
}

impl BranchScopeSnapshot {
    pub fn new(branch_id: ObjectId, scope_query: Query, entries: Vec<BranchScopeEntry>) -> Self {
        let mut deduped = BTreeMap::new();
        for entry in entries {
            deduped.insert((entry.table.clone(), entry.row_id), entry);
        }
        let scope_query_hash = stable_scope_query_hash(&scope_query);

        Self {
            branch_id,
            scope_query,
            scope_query_hash,
            entries: deduped.into_values().collect(),
        }
    }

    pub fn entry_for(&self, table: &str, row_id: ObjectId) -> Option<&BranchScopeEntry> {
        self.entries
            .iter()
            .find(|entry| entry.table == table && entry.row_id == row_id)
    }
}

pub fn stable_scope_query_hash(query: &Query) -> String {
    let json = serde_json::to_vec(query).expect("Query should serialize");
    hex::encode(Sha256::digest(json))
}

pub(crate) fn snapshot_from_branch_scope_query(query: &Query) -> Option<BranchScopeSnapshot> {
    let selector = query.branch_scope.as_ref()?;
    let entries = selector.entries.clone()?;
    let mut scope_query = query.clone();
    if let Some(selector) = scope_query.branch_scope.as_mut() {
        selector.entries = None;
    }
    Some(BranchScopeSnapshot::new(
        selector.branch_id,
        scope_query,
        entries,
    ))
}

pub fn values_match_scope_query(
    query: &Query,
    descriptor: &RowDescriptor,
    row_id: ObjectId,
    values: &[Value],
) -> bool {
    if query.disjuncts.is_empty() {
        return true;
    }

    query.disjuncts.iter().any(|disjunct| {
        disjunct
            .conditions
            .iter()
            .all(|condition| condition_matches(condition, descriptor, row_id, values))
    })
}

fn condition_matches(
    condition: &Condition,
    descriptor: &RowDescriptor,
    row_id: ObjectId,
    values: &[Value],
) -> bool {
    match condition {
        Condition::Eq { column, value } => row_value(column, descriptor, row_id, values)
            .is_some_and(|actual| actual == *value || array_contains(&actual, value)),
        Condition::Ne { column, value } => row_value(column, descriptor, row_id, values)
            .is_some_and(|actual| actual != *value && !array_contains(&actual, value)),
        Condition::Lt { column, value } => row_value(column, descriptor, row_id, values)
            .and_then(|actual| compare_values(&actual, value))
            .is_some_and(|ordering| ordering == std::cmp::Ordering::Less),
        Condition::Le { column, value } => row_value(column, descriptor, row_id, values)
            .and_then(|actual| compare_values(&actual, value))
            .is_some_and(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                )
            }),
        Condition::Gt { column, value } => row_value(column, descriptor, row_id, values)
            .and_then(|actual| compare_values(&actual, value))
            .is_some_and(|ordering| ordering == std::cmp::Ordering::Greater),
        Condition::Ge { column, value } => row_value(column, descriptor, row_id, values)
            .and_then(|actual| compare_values(&actual, value))
            .is_some_and(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                )
            }),
        Condition::Between { column, min, max } => {
            let Some(actual) = row_value(column, descriptor, row_id, values) else {
                return false;
            };
            let lower = compare_values(&actual, min).is_some_and(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                )
            });
            let upper = compare_values(&actual, max).is_some_and(|ordering| {
                matches!(
                    ordering,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                )
            });
            lower && upper
        }
        Condition::Contains { column, value } => {
            row_value(column, descriptor, row_id, values).is_some_and(|actual| {
                array_contains(&actual, value)
                    || matches!((&actual, value), (Value::Text(text), Value::Text(needle)) if text.contains(needle))
            })
        }
        Condition::IsNull { column } => row_value(column, descriptor, row_id, values)
            .is_some_and(|actual| actual.is_null()),
        Condition::IsNotNull { column } => row_value(column, descriptor, row_id, values)
            .is_some_and(|actual| !actual.is_null()),
    }
}

fn row_value(
    column: &str,
    descriptor: &RowDescriptor,
    row_id: ObjectId,
    values: &[Value],
) -> Option<Value> {
    let name = column
        .rsplit_once('.')
        .map_or(column, |(_, name)| name)
        .trim();
    if matches!(name, "id" | "_id") && descriptor.column_index(name).is_none() {
        return Some(Value::Uuid(row_id));
    }
    descriptor
        .column_index(name)
        .and_then(|index| values.get(index))
        .cloned()
}

fn array_contains(value: &Value, expected: &Value) -> bool {
    matches!(value, Value::Array(values) if values.iter().any(|value| value == expected))
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::BigInt(left), Value::BigInt(right)) => Some(left.cmp(right)),
        (Value::Double(left), Value::Double(right)) => Some(left.total_cmp(right)),
        (Value::Boolean(left), Value::Boolean(right)) => Some(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Some(left.cmp(right)),
        (Value::BatchId(left), Value::BatchId(right)) => Some(left.cmp(right)),
        (Value::Bytea(left), Value::Bytea(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

pub fn scope_entries_from_rows(table: &str, tuples: &[Tuple]) -> Vec<BranchScopeEntry> {
    scope_entries_from_rows_with_default_branch(table, "main", tuples)
}

pub fn scope_entries_from_rows_with_default_branch(
    table: &str,
    default_branch: &str,
    tuples: &[Tuple],
) -> Vec<BranchScopeEntry> {
    tuples
        .iter()
        .filter_map(|tuple| {
            let row = tuple.to_single_row()?;
            let base_branch = tuple
                .provenance()
                .iter()
                .find_map(|(id, branch)| (*id == row.id).then_some(*branch))
                .unwrap_or_else(|| BranchName::new(default_branch));

            Some(BranchScopeEntry {
                table: table.to_string(),
                row_id: row.id,
                base_branch,
                base_batch_id: row.batch_id,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{BranchName, ObjectId};
    use crate::row_histories::BatchId;

    #[test]
    fn snapshot_entry_key_is_row_and_table_stable() {
        let row_id = ObjectId::new();
        let batch_id = BatchId::new();
        let entry = BranchScopeEntry {
            table: "todos".into(),
            row_id,
            base_branch: BranchName::new("main"),
            base_batch_id: batch_id,
        };

        assert_eq!(entry.table.as_str(), "todos");
        assert_eq!(entry.row_id, row_id);
        assert_eq!(entry.base_batch_id, batch_id);
    }

    #[test]
    fn snapshot_replaces_duplicate_row_entries_with_latest_input() {
        let branch_id = ObjectId::new();
        let row_id = ObjectId::new();
        let first = BatchId::new();
        let second = BatchId::new();

        let snapshot = BranchScopeSnapshot::new(
            branch_id,
            Query::new("todos"),
            vec![
                BranchScopeEntry {
                    table: "todos".into(),
                    row_id,
                    base_branch: BranchName::new("main"),
                    base_batch_id: first,
                },
                BranchScopeEntry {
                    table: "todos".into(),
                    row_id,
                    base_branch: BranchName::new("main"),
                    base_batch_id: second,
                },
            ],
        );

        assert_eq!(
            snapshot.entry_for("todos", row_id).unwrap().base_batch_id,
            second
        );
        assert_eq!(snapshot.entries.len(), 1);
    }
}
