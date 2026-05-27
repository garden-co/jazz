use crate::runtime::Runtime;
use crate::subscription::{RowsSubscription, RowsSubscriptionQuery};
use crate::sync::Bundle;
use crate::types::{RowView, SubscriptionDelta};
use crate::{Error, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltQuery {
    pub table: String,
    pub conditions: Vec<QueryCondition>,
    pub order_by: Vec<QueryOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryCondition {
    pub column: String,
    pub op: QueryConditionOp,
    pub value: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryConditionOp {
    Eq,
    Ne,
    In,
    Contains,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOrderBy {
    pub column: String,
    pub direction: QueryDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryDirection {
    Asc,
    Desc,
}

impl BuiltQuery {
    pub fn from_json_str(query: &str) -> Result<Self> {
        let value = serde_json::from_str(query)
            .map_err(|error| Error::new(format!("invalid query JSON: {error}")))?;
        Self::from_json_value(value)
    }

    pub fn from_json_value(value: JsonValue) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("query must be a JSON object"))?;
        reject_unsupported_fields(object)?;

        let table = object
            .get("table")
            .and_then(JsonValue::as_str)
            .filter(|table| !table.is_empty())
            .ok_or_else(|| Error::new("query table must be a non-empty string"))?
            .to_owned();

        Ok(Self {
            table,
            conditions: parse_conditions(object.get("conditions"))?,
            order_by: parse_order_by(object.get("orderBy"))?,
            limit: parse_usize_field(object, "limit")?,
            offset: parse_usize_field(object, "offset")?,
        })
    }

    pub(crate) fn ordered_page(&self) -> Option<(&QueryCondition, usize)> {
        if self.offset.unwrap_or(0) != 0 || self.conditions.len() != 1 || self.order_by.len() != 1 {
            return None;
        }
        let condition = self.conditions.first()?;
        if condition.op != QueryConditionOp::Eq {
            return None;
        }
        let order = self.order_by.first()?;
        if order.column == "$createdAt" && order.direction == QueryDirection::Desc {
            self.limit.map(|limit| (condition, limit))
        } else {
            None
        }
    }

    pub(crate) fn single_predicate_export_condition(&self) -> Result<Option<&QueryCondition>> {
        if self.conditions.len() != 1
            || !self.order_by.is_empty()
            || self.limit.is_some()
            || self.offset.unwrap_or(0) != 0
        {
            return Err(Error::new(
                "export_query supports one predicate, or one eq predicate ordered by $createdAt desc with a limit",
            ));
        }

        Ok(self.conditions.first())
    }

    pub(crate) fn to_json_value(&self) -> JsonValue {
        let mut object = JsonMap::new();
        object.insert("table".to_owned(), JsonValue::String(self.table.clone()));
        object.insert(
            "conditions".to_owned(),
            JsonValue::Array(
                self.conditions
                    .iter()
                    .map(|condition| {
                        serde_json::json!({
                            "column": condition.column.clone(),
                            "op": condition.op.as_str(),
                            "value": condition.value.clone(),
                        })
                    })
                    .collect(),
            ),
        );
        object.insert(
            "orderBy".to_owned(),
            JsonValue::Array(
                self.order_by
                    .iter()
                    .map(|order| {
                        JsonValue::Array(vec![
                            JsonValue::String(order.column.clone()),
                            JsonValue::String(order.direction.as_str().to_owned()),
                        ])
                    })
                    .collect(),
            ),
        );
        if let Some(limit) = self.limit {
            object.insert("limit".to_owned(), serde_json::json!(limit));
        }
        if let Some(offset) = self.offset {
            object.insert("offset".to_owned(), serde_json::json!(offset));
        }
        JsonValue::Object(object)
    }
}

impl QueryConditionOp {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            QueryConditionOp::Eq => "eq",
            QueryConditionOp::Ne => "ne",
            QueryConditionOp::In => "in",
            QueryConditionOp::Contains => "contains",
        }
    }
}

impl QueryDirection {
    fn as_str(&self) -> &'static str {
        match self {
            QueryDirection::Asc => "asc",
            QueryDirection::Desc => "desc",
        }
    }
}

impl Runtime {
    pub fn query(&self, query: BuiltQuery) -> Result<Vec<RowView>> {
        if let Some(rows) = self.read_rows_for_built_query(&query)? {
            return Ok(rows);
        }

        let mut rows = self.rows_for_conditions(&query.table, &query.conditions)?;
        apply_ordering(&mut rows, &query.order_by)?;
        Ok(apply_window(rows, query.offset, query.limit))
    }

    pub fn one(&self, query: BuiltQuery) -> Result<Option<RowView>> {
        Ok(self.query(query)?.into_iter().next())
    }

    pub fn subscribe_query(&self, query: BuiltQuery) -> Result<RowsSubscription> {
        Ok(RowsSubscription::query(query.clone(), self.query(query)?))
    }

    pub fn export_query(&self, query: BuiltQuery) -> Result<Bundle> {
        self.export_query_with_ref_includes(query, &[])
    }

    pub fn export_query_with_ref_includes(
        &self,
        query: BuiltQuery,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let rows = self.query(query.clone())?;
        self.export_built_query_scope(query, rows, ref_include_fields)
    }

    pub fn subscription_delta(
        &self,
        subscription: &mut RowsSubscription,
    ) -> Result<SubscriptionDelta> {
        let rows = match &subscription.query {
            RowsSubscriptionQuery::Table { table } => self.read_rows(table)?,
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq" => {
                self.read_rows_where_eq(&query.table, &query.field, query.value.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "ne" => {
                self.read_rows_where_ne(&query.table, &query.field, query.value.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "contains" => {
                let Some(needle) = query.value.as_str() else {
                    return Err(Error::new("contains expects a string value"));
                };
                self.read_rows_where_contains(&query.table, &query.field, needle)?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "in" => {
                let Some(values) = query.value.as_array() else {
                    return Err(Error::new("in predicate expects an array value"));
                };
                self.read_rows_where_in(&query.table, &query.field, values.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "recursive_refs" => {
                let Some(root_id) = query.value.as_str() else {
                    return Err(Error::new("recursive refs expects root id string"));
                };
                self.read_recursive_refs(&query.table, root_id, &query.field)?
            }
            RowsSubscriptionQuery::Predicate(query) => {
                return Err(Error::new(format!(
                    "unsupported subscription query {}",
                    query.op
                )));
            }
            RowsSubscriptionQuery::Built(query) => self.query(query.clone())?,
        };
        Ok(subscription.replace_with_subscription_delta(rows))
    }

    fn rows_for_conditions(
        &self,
        table: &str,
        conditions: &[QueryCondition],
    ) -> Result<Vec<RowView>> {
        let mut rows = match conditions.first() {
            Some(condition) => self.rows_for_condition(table, condition)?,
            None => self.read_rows(table)?,
        };
        if conditions.len() > 1 {
            let mut filtered = Vec::new();
            for row in rows {
                let mut matches = true;
                for condition in conditions {
                    if !condition_matches(&row, condition)? {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    filtered.push(row);
                }
            }
            rows = filtered;
        }
        Ok(rows)
    }

    fn rows_for_condition(&self, table: &str, condition: &QueryCondition) -> Result<Vec<RowView>> {
        match condition.op {
            QueryConditionOp::Eq => {
                self.read_rows_where_eq(table, &condition.column, condition.value.clone())
            }
            QueryConditionOp::Ne => {
                self.read_rows_where_ne(table, &condition.column, condition.value.clone())
            }
            QueryConditionOp::In => {
                let Some(values) = condition.value.as_array() else {
                    return Err(Error::new("in condition expects an array value"));
                };
                self.read_rows_where_in(table, &condition.column, values.clone())
            }
            QueryConditionOp::Contains => {
                let Some(needle) = condition.value.as_str() else {
                    return Err(Error::new("contains condition expects a string value"));
                };
                self.read_rows_where_contains(table, &condition.column, needle)
            }
        }
    }
}

fn reject_unsupported_fields(object: &JsonMap<String, JsonValue>) -> Result<()> {
    if object
        .get("includes")
        .is_some_and(|value| !is_empty_object(value))
    {
        return Err(Error::new("mini-sqlite query does not support includes"));
    }
    if object
        .get("__jazz_requireIncludes")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        return Err(Error::new(
            "mini-sqlite query does not support requireIncludes",
        ));
    }
    if object
        .get("select")
        .is_some_and(|value| !is_empty_array(value))
    {
        return Err(Error::new("mini-sqlite query does not support select"));
    }
    if object
        .get("hops")
        .is_some_and(|value| !is_empty_array(value))
    {
        return Err(Error::new("mini-sqlite query does not support hopTo"));
    }
    if object.get("gather").is_some_and(|value| !value.is_null()) {
        return Err(Error::new("mini-sqlite query does not support gather"));
    }
    if object.get("union").is_some_and(|value| !value.is_null()) {
        return Err(Error::new("mini-sqlite query does not support union"));
    }
    Ok(())
}

fn is_empty_object(value: &JsonValue) -> bool {
    value.as_object().is_some_and(JsonMap::is_empty)
}

fn is_empty_array(value: &JsonValue) -> bool {
    value.as_array().is_some_and(Vec::is_empty)
}

fn parse_conditions(value: Option<&JsonValue>) -> Result<Vec<QueryCondition>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(conditions) = value.as_array() else {
        return Err(Error::new("query conditions must be an array"));
    };
    conditions
        .iter()
        .map(|condition| {
            let object = condition
                .as_object()
                .ok_or_else(|| Error::new("query condition must be an object"))?;
            let column = object
                .get("column")
                .and_then(JsonValue::as_str)
                .filter(|column| !column.is_empty())
                .ok_or_else(|| Error::new("query condition column must be a non-empty string"))?
                .to_owned();
            let op = object
                .get("op")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| Error::new("query condition op must be a string"))
                .and_then(parse_condition_op)?;
            let value = object
                .get("value")
                .cloned()
                .ok_or_else(|| Error::new("query condition value is required"))?;
            Ok(QueryCondition { column, op, value })
        })
        .collect()
}

fn parse_condition_op(op: &str) -> Result<QueryConditionOp> {
    match op {
        "eq" => Ok(QueryConditionOp::Eq),
        "ne" => Ok(QueryConditionOp::Ne),
        "in" => Ok(QueryConditionOp::In),
        "contains" => Ok(QueryConditionOp::Contains),
        _ => Err(Error::new(format!("unsupported query condition op {op}"))),
    }
}

fn parse_order_by(value: Option<&JsonValue>) -> Result<Vec<QueryOrderBy>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(entries) = value.as_array() else {
        return Err(Error::new("query orderBy must be an array"));
    };
    entries
        .iter()
        .map(|entry| {
            let Some(pair) = entry.as_array() else {
                return Err(Error::new("query orderBy entry must be an array"));
            };
            if pair.len() != 2 {
                return Err(Error::new("query orderBy entry must have two items"));
            }
            let column = pair[0]
                .as_str()
                .filter(|column| !column.is_empty())
                .ok_or_else(|| Error::new("query orderBy column must be a non-empty string"))?
                .to_owned();
            let direction = match pair[1]
                .as_str()
                .ok_or_else(|| Error::new("query orderBy direction must be a string"))?
            {
                "asc" => QueryDirection::Asc,
                "desc" => QueryDirection::Desc,
                direction => {
                    return Err(Error::new(format!(
                        "unsupported query orderBy direction {direction}"
                    )));
                }
            };
            Ok(QueryOrderBy { column, direction })
        })
        .collect()
}

fn parse_usize_field(object: &JsonMap<String, JsonValue>, field: &str) -> Result<Option<usize>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(value) = value.as_u64() else {
        return Err(Error::new(format!(
            "query {field} must be a non-negative integer"
        )));
    };
    Ok(Some(value as usize))
}

fn condition_matches(row: &RowView, condition: &QueryCondition) -> Result<bool> {
    match condition.op {
        QueryConditionOp::Eq => {
            Ok(row_query_value(row, &condition.column).as_ref() == Some(&condition.value))
        }
        QueryConditionOp::Ne => {
            if condition.column == "id" || condition.column == "$createdBy" {
                return Ok(
                    row_query_value(row, &condition.column).as_ref() != Some(&condition.value)
                );
            }
            Ok(row
                .values
                .get(&condition.column)
                .is_some_and(|value| value != &condition.value))
        }
        QueryConditionOp::In => {
            let Some(values) = condition.value.as_array() else {
                return Err(Error::new("in condition expects an array value"));
            };
            Ok(row_query_value(row, &condition.column)
                .as_ref()
                .is_some_and(|value| values.contains(value)))
        }
        QueryConditionOp::Contains => {
            let Some(needle) = condition.value.as_str() else {
                return Err(Error::new("contains condition expects a string value"));
            };
            Ok(row_query_value(row, &condition.column)
                .and_then(|value| value.as_str().map(str::to_owned))
                .is_some_and(|value| value.contains(needle)))
        }
    }
}

fn row_query_value(row: &RowView, column: &str) -> Option<JsonValue> {
    match column {
        "id" => Some(JsonValue::String(row.id.clone())),
        "$createdBy" => Some(JsonValue::String(row.created_by.clone())),
        column => row.values.get(column).cloned(),
    }
}

fn apply_ordering(rows: &mut [RowView], order_by: &[QueryOrderBy]) -> Result<()> {
    if order_by.is_empty() {
        return Ok(());
    }
    if order_by.iter().any(|order| order.column == "$createdAt") {
        return Err(Error::new(
            "$createdAt orderBy requires one eq condition, desc direction, limit, and no offset",
        ));
    }
    rows.sort_by(|left, right| {
        for order in order_by {
            let ordering = compare_json_values(
                row_query_value(left, &order.column).as_ref(),
                row_query_value(right, &order.column).as_ref(),
            );
            let ordering = match order.direction {
                QueryDirection::Asc => ordering,
                QueryDirection::Desc => ordering.reverse(),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.id.cmp(&right.id)
    });
    Ok(())
}

fn compare_json_values(left: Option<&JsonValue>, right: Option<&JsonValue>) -> Ordering {
    let left_rank = json_value_rank(left);
    let right_rank = json_value_rank(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| match (left, right) {
            (Some(JsonValue::Bool(left)), Some(JsonValue::Bool(right))) => left.cmp(right),
            (Some(JsonValue::Number(left)), Some(JsonValue::Number(right))) => left
                .as_f64()
                .partial_cmp(&right.as_f64())
                .unwrap_or(Ordering::Equal),
            (Some(JsonValue::String(left)), Some(JsonValue::String(right))) => left.cmp(right),
            (Some(left), Some(right)) => left.to_string().cmp(&right.to_string()),
            _ => Ordering::Equal,
        })
}

fn json_value_rank(value: Option<&JsonValue>) -> u8 {
    match value {
        None | Some(JsonValue::Null) => 0,
        Some(JsonValue::Bool(_)) => 1,
        Some(JsonValue::Number(_)) => 2,
        Some(JsonValue::String(_)) => 3,
        Some(JsonValue::Array(_)) => 4,
        Some(JsonValue::Object(_)) => 5,
    }
}

fn apply_window(rows: Vec<RowView>, offset: Option<usize>, limit: Option<usize>) -> Vec<RowView> {
    let offset = offset.unwrap_or(0);
    match limit {
        Some(limit) => rows.into_iter().skip(offset).take(limit).collect(),
        None => rows.into_iter().skip(offset).collect(),
    }
}
