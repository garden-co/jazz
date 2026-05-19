use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::object::{BranchName, ObjectId};
use crate::row_histories::BatchId;

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
    pub scope_query_hash: String,
    pub entries: Vec<BranchScopeEntry>,
}

impl BranchScopeSnapshot {
    pub fn new(
        branch_id: ObjectId,
        scope_query_hash: String,
        entries: Vec<BranchScopeEntry>,
    ) -> Self {
        let mut deduped = BTreeMap::new();
        for entry in entries {
            deduped.insert((entry.table.clone(), entry.row_id), entry);
        }

        Self {
            branch_id,
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
            "todos-scope-v1".to_string(),
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
