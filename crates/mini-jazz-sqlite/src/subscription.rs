use crate::sync::QueryPredicateRecord;
use crate::types::{RejectionInfo, RowDiff, RowView};
use crate::value::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct RowsSubscription {
    pub(crate) query: RowsSubscriptionQuery,
    pub(crate) last_rows: Vec<RowView>,
}

#[derive(Clone, Debug)]
pub struct RejectionSubscription {
    pub(crate) last_rejections: Vec<RejectionInfo>,
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

    pub(crate) fn where_ne(table: &str, field: &str, value: JsonValue, rows: Vec<RowView>) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table, field, "ne", value,
            )),
            last_rows: rows,
        }
    }

    pub(crate) fn where_recursive_refs(
        table: &str,
        root_id: &str,
        parent_field: &str,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                parent_field,
                "recursive_refs",
                JsonValue::String(root_id.to_owned()),
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
                })
                .into(),
            )),
            last_rows: rows,
        }
    }

    pub(crate) fn where_eq_top_field_desc(
        table: &str,
        field: &str,
        value: JsonValue,
        order_field: &str,
        limit: usize,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "eq_top_field_desc",
                serde_json::json!({
                    "eq": value,
                    "order_field": order_field,
                    "limit": limit,
                })
                .into(),
            )),
            last_rows: rows,
        }
    }

    pub fn initial_rows(&self) -> &[RowView] {
        &self.last_rows
    }

    pub(crate) fn replace_with_diff(&mut self, next_rows: Vec<RowView>) -> Vec<RowDiff> {
        if self.last_rows == next_rows {
            return Vec::new();
        }

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

        if diffs.is_empty() {
            let before_positions = positions_by_id(&self.last_rows);
            let after_positions = positions_by_id(&next_rows);
            for (id, before_index) in before_positions {
                let Some(after_index) = after_positions.get(&id) else {
                    continue;
                };
                if before_index == *after_index {
                    continue;
                }
                let Some(after_row) = after.get(&id) else {
                    continue;
                };
                diffs.push(RowDiff::Moved {
                    row: (*after_row).clone(),
                    before_index,
                    after_index: *after_index,
                });
            }
        }

        self.last_rows = next_rows;
        diffs
    }
}

impl RejectionSubscription {
    pub(crate) fn new(rejections: Vec<RejectionInfo>) -> Self {
        Self {
            last_rejections: rejections,
        }
    }

    pub fn initial_rejections(&self) -> &[RejectionInfo] {
        &self.last_rejections
    }

    pub(crate) fn replace_with_new(
        &mut self,
        next_rejections: Vec<RejectionInfo>,
    ) -> Vec<RejectionInfo> {
        let before = self
            .last_rejections
            .iter()
            .map(|rejection| (rejection.tx_id.as_str(), rejection))
            .collect::<BTreeMap<_, _>>();
        let events = next_rejections
            .iter()
            .filter(|rejection| {
                before
                    .get(rejection.tx_id.as_str())
                    .is_none_or(|previous| *previous != *rejection)
            })
            .cloned()
            .collect();
        self.last_rejections = next_rejections;
        events
    }
}

fn by_id(rows: &[RowView]) -> BTreeMap<String, &RowView> {
    rows.iter().map(|row| (row.id.clone(), row)).collect()
}

fn positions_by_id(rows: &[RowView]) -> BTreeMap<String, usize> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| (row.id.clone(), index))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn order_only_changes_emit_moved_diffs() {
        let first = row("first");
        let second = row("second");
        let mut subscription = RowsSubscription::new("items", vec![first.clone(), second.clone()]);

        let diffs = subscription.replace_with_diff(vec![second.clone(), first.clone()]);

        assert!(matches!(
            &diffs[..],
            [
                RowDiff::Moved {
                    row,
                    before_index: 0,
                    after_index: 1
                },
                RowDiff::Moved {
                    row: row2,
                    before_index: 1,
                    after_index: 0
                }
            ] if row.id == "first" && row2.id == "second"
        ));
        assert_eq!(
            subscription
                .initial_rows()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second", "first"]
        );
    }

    fn row(id: &str) -> RowView {
        RowView {
            table: "items".to_owned(),
            id: id.to_owned(),
            values: BTreeMap::from([("title".to_owned(), json!(id).into())]),
            created_by: "alice".to_owned(),
            tx_id: format!("tx-{id}"),
            conflict_count: 0,
        }
    }
}
