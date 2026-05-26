use crate::sync::QueryPredicateRecord;
use crate::types::{RejectionInfo, RowDiff, RowView};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionTier {
    Local,
    Edge,
    Global,
}

#[derive(Clone, Debug)]
pub struct RowsSubscription {
    pub(crate) query: RowsSubscriptionQuery,
    pub(crate) tier: SubscriptionTier,
    pub(crate) settled: bool,
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
        Self::new_query(
            RowsSubscriptionQuery::Table {
                table: table.to_owned(),
            },
            rows,
        )
    }

    pub(crate) fn new_at_tier(
        table: &str,
        tier: SubscriptionTier,
        settled: bool,
        rows: Vec<RowView>,
    ) -> Self {
        Self {
            query: RowsSubscriptionQuery::Table {
                table: table.to_owned(),
            },
            tier,
            settled,
            last_rows: rows,
        }
    }

    fn new_query(query: RowsSubscriptionQuery, rows: Vec<RowView>) -> Self {
        Self {
            query,
            tier: SubscriptionTier::Local,
            settled: true,
            last_rows: rows,
        }
    }

    pub(crate) fn where_eq(table: &str, field: &str, value: JsonValue, rows: Vec<RowView>) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(table, field, "eq", value)),
            rows,
        )
    }

    pub(crate) fn where_contains(
        table: &str,
        field: &str,
        needle: &str,
        rows: Vec<RowView>,
    ) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "contains",
                JsonValue::String(needle.to_owned()),
            )),
            rows,
        )
    }

    pub(crate) fn where_in(
        table: &str,
        field: &str,
        values: Vec<JsonValue>,
        rows: Vec<RowView>,
    ) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "in",
                JsonValue::Array(values),
            )),
            rows,
        )
    }

    pub(crate) fn where_ne(table: &str, field: &str, value: JsonValue, rows: Vec<RowView>) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(table, field, "ne", value)),
            rows,
        )
    }

    pub(crate) fn where_recursive_refs(
        table: &str,
        root_id: &str,
        parent_field: &str,
        rows: Vec<RowView>,
    ) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                parent_field,
                "recursive_refs",
                JsonValue::String(root_id.to_owned()),
            )),
            rows,
        )
    }

    pub(crate) fn where_eq_top_created_at_desc(
        table: &str,
        field: &str,
        value: JsonValue,
        limit: usize,
        rows: Vec<RowView>,
    ) -> Self {
        Self::new_query(
            RowsSubscriptionQuery::Predicate(QueryPredicateRecord::new(
                table,
                field,
                "eq_top_created_at_desc",
                serde_json::json!({
                    "eq": value,
                    "limit": limit,
                }),
            )),
            rows,
        )
    }

    pub fn initial_rows(&self) -> &[RowView] {
        &self.last_rows
    }

    pub fn tier(&self) -> SubscriptionTier {
        self.tier
    }

    pub fn is_settled(&self) -> bool {
        self.settled
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
