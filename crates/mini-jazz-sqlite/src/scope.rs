use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub struct QueryResult {
    pub rows: Vec<RowView>,
    pub scope: QueryScope,
}

pub struct SubscriptionDiff {
    pub added: Vec<RowView>,
    pub updated: Vec<RowView>,
    pub removed: Vec<RowView>,
}

pub struct QueryScope {
    pub result_rows: Vec<ScopeRow>,
    pub dependency_rows: Vec<ScopeRow>,
    pub predicate_scopes: Vec<PredicateScope>,
}

pub struct QueryScopeBundle {
    pub txs: Vec<TxRecord>,
    pub branches: Vec<BranchRecord>,
    pub history_rows: Vec<HistoryRecord>,
}

pub struct BranchRecord {
    pub branch_id: String,
    pub base_global_epoch: i64,
}

pub struct TxRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub global_epoch: Option<i64>,
    pub kind: String,
    pub status: String,
    pub rejection_reason_json: Option<String>,
    pub created_at: i64,
    pub metadata_json: String,
}

pub struct HistoryRecord {
    pub table: String,
    pub row_id: String,
    pub branch_id: String,
    pub tx_id: String,
    pub op: String,
    pub values: BTreeMap<String, JsonValue>,
    pub conflicts_json: String,
    pub created_at: i64,
    pub updated_at: i64,
}

pub struct ScopeRow {
    pub table: String,
    pub row_id: String,
    pub tx_id: String,
    pub reason: ScopeReason,
}

pub enum ScopeReason {
    Result,
    Dependency,
}

pub struct PredicateScope {
    pub table: String,
    pub row_id: String,
    pub column: String,
    pub op: String,
    pub value: String,
    pub reason: PredicateReason,
}

pub enum PredicateReason {
    Filter,
    OptionalIncludeMissing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowView {
    pub(crate) values: BTreeMap<String, JsonValue>,
    pub(crate) includes: BTreeMap<String, RowView>,
}

impl RowView {
    pub fn get(&self, column: &str) -> Option<&str> {
        self.values.get(column)?.as_str()
    }

    pub fn include(&self, alias: &str) -> Option<&RowView> {
        self.includes.get(alias)
    }

    fn row_id(&self) -> &str {
        self.get("$rowId").unwrap_or("")
    }
}

pub(crate) fn diff_rows(previous: &[RowView], next: &[RowView]) -> SubscriptionDiff {
    let previous_by_id = previous
        .iter()
        .map(|row| (row.row_id().to_owned(), row))
        .collect::<BTreeMap<_, _>>();
    let next_by_id = next
        .iter()
        .map(|row| (row.row_id().to_owned(), row))
        .collect::<BTreeMap<_, _>>();

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();

    for (row_id, next_row) in &next_by_id {
        match previous_by_id.get(row_id) {
            Some(previous_row) if *previous_row != *next_row => updated.push((*next_row).clone()),
            Some(_) => {}
            None => added.push((*next_row).clone()),
        }
    }

    for (row_id, previous_row) in &previous_by_id {
        if !next_by_id.contains_key(row_id) {
            removed.push((*previous_row).clone());
        }
    }

    SubscriptionDiff {
        added,
        updated,
        removed,
    }
}
