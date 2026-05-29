use crate::query_observation::observed_ids_from_query_value;
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
        if read.branch_id == current_branch_id
            && matches!(read.op.as_str(), "eq" | "ne" | "contains" | "in")
        {
            predicate_groups
                .entry((
                    read.table.clone(),
                    read.field.clone(),
                    read.branch_id.clone(),
                    read.op.clone(),
                ))
                .or_default()
                .push((read.value.clone(), Vec::new()));
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "recursive_refs" {
            let Some(root_id) = read.value.as_str() else {
                return Err(crate::Error::new("recursive refs expects root id string"));
            };
            recursive_groups
                .entry((
                    read.table.clone(),
                    read.field.clone(),
                    read.branch_id.clone(),
                ))
                .or_default()
                .push(root_id.to_owned());
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "eq_top_created_at_desc" {
            let value = read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            top_created_at_groups
                .entry((
                    read.table.clone(),
                    read.field.clone(),
                    read.branch_id.clone(),
                    limit as usize,
                ))
                .or_default()
                .push((value.clone(), observed_ids_from_query_value(&read.value)?));
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "eq_top_field_desc" {
            let value = read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
            let order_field = read
                .value
                .get("order_field")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
            let limit = read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
            top_field_groups
                .entry((
                    read.table.clone(),
                    read.field.clone(),
                    read.branch_id.clone(),
                    order_field.to_owned(),
                    limit as usize,
                ))
                .or_default()
                .push((value.clone(), observed_ids_from_query_value(&read.value)?));
            continue;
        }
        singles.push(QueryRefreshPlan::Single(read.clone()));
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
