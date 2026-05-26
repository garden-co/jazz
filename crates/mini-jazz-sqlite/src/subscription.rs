use crate::sync::QueryPredicateRecord;
use crate::types::{RowDiff, RowView};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct RowsSubscription {
    pub(crate) query: RowsSubscriptionQuery,
    pub(crate) last_rows: Vec<RowView>,
}

#[derive(Clone, Debug)]
pub(crate) enum RowsSubscriptionQuery {
    Table { table: String },
    Predicate(QueryPredicateRecord),
}

impl RowsSubscription {
    pub(crate) fn new(table: &str, rows: Vec<RowView>) -> Self {
        Self {
            query: RowsSubscriptionQuery::Table {
                table: table.to_owned(),
            },
            last_rows: rows,
        }
    }

    pub(crate) fn where_eq(table: &str, field: &str, value: JsonValue, rows: Vec<RowView>) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table, field, "eq", value,
            )),
            last_rows: rows,
        }
    }

    pub(crate) fn where_contains(
        table: &str,
        field: &str,
        needle: &str,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "contains",
                JsonValue::String(needle.to_owned()),
            )),
            last_rows: rows,
        }
    }

    pub(crate) fn where_in(
        table: &str,
        field: &str,
        values: Vec<JsonValue>,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "in",
                JsonValue::Array(values),
            )),
            last_rows: rows,
        }
    }

    pub(crate) fn where_eq_top_created_at_desc(
        table: &str,
        field: &str,
        value: JsonValue,
        limit: usize,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "eq_top_created_at_desc",
                serde_json::json!({
                    "eq": value,
                    "limit": limit,
                }),
            )),
            last_rows: rows,
        }
    }

    pub fn initial_rows(&self) -> &[RowView] {
        &self.last_rows
    }

    pub(crate) fn replace_with_diff(&mut self, next_rows: Vec<RowView>) -> Vec<RowDiff> {
        let before = by_id(&self.last_rows);
        let after = by_id(&next_rows);
        let mut diffs = Vec::new();

        for (id, before_row) in &before {
            match after.get(id) {
                Some(after_row) if after_row != before_row => diffs.push(RowDiff::Updated {
                    before: (*before_row).clone(),
                    after: (*after_row).clone(),
                }),
                Some(_) => {}
                None => diffs.push(RowDiff::Removed((*before_row).clone())),
            }
        }

        for (id, after_row) in &after {
            if !before.contains_key(id) {
                diffs.push(RowDiff::Added((*after_row).clone()));
            }
        }

        self.last_rows = next_rows;
        diffs
    }
}

fn by_id(rows: &[RowView]) -> BTreeMap<String, &RowView> {
    rows.iter().map(|row| (row.id.clone(), row)).collect()
}
