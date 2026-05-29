use crate::observed_query::{self, ObservedQuery};
use crate::sync::QueryReadRecord;
use crate::Result;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) type PredicateRefreshValue = (JsonValue, Vec<String>);
pub(crate) type TopFieldRefreshValue = (JsonValue, Vec<String>);
pub(crate) type TopCreatedAtRefreshValue = (JsonValue, Vec<String>);

type PredicateRefreshKey = (String, String, String, String);
type RecursiveRefreshKey = (String, String, String);
type TopFieldRefreshKey = (String, String, String, String, usize);
type TopCreatedAtRefreshKey = (String, String, String, usize);

pub(crate) enum QueryRefreshPlan {
    Predicate {
        table: String,
        field: String,
        op: String,
        values: Vec<PredicateRefreshValue>,
    },
    RecursiveRefs {
        table: String,
        field: String,
        root_ids: Vec<String>,
    },
    TopCreatedAt {
        table: String,
        field: String,
        values: Vec<TopCreatedAtRefreshValue>,
        limit: usize,
    },
    TopField {
        table: String,
        field: String,
        values: Vec<TopFieldRefreshValue>,
        order_field: String,
        limit: usize,
    },
    Single(QueryReadRecord),
}

pub(crate) fn plan_refreshes(
    current_branch_id: &str,
    reads: &[QueryReadRecord],
) -> Result<Vec<QueryRefreshPlan>> {
    let mut predicate_groups: BTreeMap<PredicateRefreshKey, Vec<PredicateRefreshValue>> =
        BTreeMap::new();
    let mut recursive_groups: BTreeMap<RecursiveRefreshKey, Vec<String>> = BTreeMap::new();
    let mut top_created_at_groups: BTreeMap<TopCreatedAtRefreshKey, Vec<TopCreatedAtRefreshValue>> =
        BTreeMap::new();
    let mut top_field_groups: BTreeMap<TopFieldRefreshKey, Vec<TopFieldRefreshValue>> =
        BTreeMap::new();
    let mut singles = Vec::new();

    for read in reads {
        if read.branch_id != current_branch_id {
            singles.push(QueryRefreshPlan::Single(read.clone()));
            continue;
        }
        match observed_query::decode(read)? {
            ObservedQuery::Predicate { op, value } => {
                predicate_groups
                    .entry((
                        read.table.clone(),
                        read.field.clone(),
                        read.branch_id.clone(),
                        op.as_str().to_owned(),
                    ))
                    .or_default()
                    .push((value, Vec::new()));
            }
            ObservedQuery::RecursiveRefs { root_id } => {
                recursive_groups
                    .entry((
                        read.table.clone(),
                        read.field.clone(),
                        read.branch_id.clone(),
                    ))
                    .or_default()
                    .push(root_id);
            }
            ObservedQuery::TopCreatedAt {
                value,
                limit,
                observed_ids,
            } => {
                top_created_at_groups
                    .entry((
                        read.table.clone(),
                        read.field.clone(),
                        read.branch_id.clone(),
                        limit,
                    ))
                    .or_default()
                    .push((value, observed_ids));
            }
            ObservedQuery::TopField {
                value,
                order_field,
                limit,
                observed_ids,
            } => {
                top_field_groups
                    .entry((
                        read.table.clone(),
                        read.field.clone(),
                        read.branch_id.clone(),
                        order_field,
                        limit,
                    ))
                    .or_default()
                    .push((value, observed_ids));
            }
            ObservedQuery::Built { .. } | ObservedQuery::Absent => {
                singles.push(QueryRefreshPlan::Single(read.clone()));
            }
        }
    }

    let mut plans = Vec::new();
    plans.extend(
        recursive_groups
            .into_iter()
            .map(
                |((table, field, _branch), root_ids)| QueryRefreshPlan::RecursiveRefs {
                    table,
                    field,
                    root_ids,
                },
            ),
    );
    plans.extend(top_created_at_groups.into_iter().map(
        |((table, field, _branch, limit), values)| QueryRefreshPlan::TopCreatedAt {
            table,
            field,
            values,
            limit,
        },
    ));
    plans.extend(top_field_groups.into_iter().map(
        |((table, field, _branch, order_field, limit), values)| QueryRefreshPlan::TopField {
            table,
            field,
            values,
            order_field,
            limit,
        },
    ));
    plans.extend(
        predicate_groups
            .into_iter()
            .map(
                |((table, field, _branch, op), values)| QueryRefreshPlan::Predicate {
                    table,
                    field,
                    op,
                    values,
                },
            ),
    );
    plans.extend(singles);
    Ok(plans)
}
