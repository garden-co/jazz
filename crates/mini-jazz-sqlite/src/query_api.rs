use crate::runtime::Runtime;
use crate::subscription::RowsSubscription;
use crate::sync::Bundle;
use crate::types::{RowView, SubscriptionDelta};
use crate::{Error, ReadTier, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltQuery {
    pub table: String,
    pub conditions: Vec<QueryCondition>,
    pub order_by: Vec<QueryOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCondition {
    pub column: String,
    pub op: QueryConditionOp,
    pub value: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryConditionOp {
    Eq,
    Ne,
    In,
    Contains,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryOrderBy {
    pub column: String,
    pub direction: QueryDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

pub(crate) fn predicate_query(
    table: &str,
    column: &str,
    op: QueryConditionOp,
    value: JsonValue,
) -> BuiltQuery {
    BuiltQuery {
        table: table.to_owned(),
        conditions: vec![QueryCondition {
            column: column.to_owned(),
            op,
            value,
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

impl Runtime {
    pub fn query(&self, query: BuiltQuery) -> Result<Vec<RowView>> {
        self.read_rows_for_built_query(&query)
    }

    pub fn query_at_tier(&self, query: BuiltQuery, tier: ReadTier) -> Result<Vec<RowView>> {
        self.query_context_at_tier(tier)
            .read_rows_for_built_query(&query)
    }

    pub fn query_branch(&mut self, branch_id: &str, query: BuiltQuery) -> Result<Vec<RowView>> {
        self.query_in_branch(branch_id, |runtime| runtime.query(query))
    }

    pub fn one(&self, query: BuiltQuery) -> Result<Option<RowView>> {
        Ok(self.query(query)?.into_iter().next())
    }

    pub fn subscribe_query(&self, query: BuiltQuery) -> Result<RowsSubscription> {
        Ok(RowsSubscription::query(query.clone(), self.query(query)?))
    }

    pub fn subscribe_query_at_tier(
        &self,
        query: BuiltQuery,
        tier: ReadTier,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::query_at_tier(
            query.clone(),
            tier,
            self.query_at_tier(query, tier)?,
        ))
    }

    pub fn export_query(&self, query: BuiltQuery) -> Result<Bundle> {
        self.export_query_with_ref_includes(query, &[])
    }

    pub fn export_query_with_ref_includes(
        &self,
        query: BuiltQuery,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let rows = self.query(export_support_query(&query)?)?;
        self.export_built_query_scope(query, rows, ref_include_fields)
    }

    pub fn subscription_delta(
        &self,
        subscription: &mut RowsSubscription,
    ) -> Result<SubscriptionDelta> {
        let rows = self.subscription_rows(subscription)?;
        Ok(subscription.replace_with_subscription_delta(rows))
    }
}

fn reject_unsupported_fields(object: &JsonMap<String, JsonValue>) -> Result<()> {
    if object.get("branch").is_some_and(|value| !value.is_null()) {
        return Err(Error::new("mini-sqlite query does not support branch"));
    }
    if object
        .get("branches")
        .is_some_and(|value| !is_empty_array(value))
    {
        return Err(Error::new("mini-sqlite query does not support branches"));
    }
    if object
        .get("include_deleted")
        .is_some_and(|value| value.as_bool().unwrap_or(true))
    {
        return Err(Error::new(
            "mini-sqlite query does not support include_deleted",
        ));
    }
    if object
        .get("includeDeleted")
        .is_some_and(|value| value.as_bool().unwrap_or(true))
    {
        return Err(Error::new(
            "mini-sqlite query does not support includeDeleted",
        ));
    }
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

fn export_support_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut support_query = query.clone();
    support_query.offset = None;
    if let Some(limit) = query.limit {
        support_query.limit = Some(
            offset
                .checked_add(limit)
                .ok_or_else(|| Error::new("query limit plus offset is too large"))?,
        );
    }
    Ok(support_query)
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
    const MAX_PORTABLE_USIZE: u64 = u32::MAX as u64;

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
    if value > MAX_PORTABLE_USIZE {
        return Err(Error::new(format!(
            "query {field} is too large; maximum is {MAX_PORTABLE_USIZE}"
        )));
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| Error::new(format!("query {field} is too large")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn built_query_rejects_window_values_too_large_for_wasm_usize() {
        for field in ["limit", "offset"] {
            let err = BuiltQuery::from_json_value(json!({
                "table": "todos",
                "conditions": [],
                field: u64::from(u32::MAX) + 1,
            }))
            .unwrap_err();

            assert!(
                err.to_string()
                    .contains(&format!("query {field} is too large")),
                "{err}"
            );
        }
    }

    #[test]
    fn built_query_rejects_branch_and_include_deleted_options_it_cannot_honor() {
        for (field, value) in [
            ("branch", json!("draft")),
            ("branches", json!(["draft"])),
            ("include_deleted", json!(true)),
            ("includeDeleted", json!(true)),
        ] {
            let err = BuiltQuery::from_json_value(json!({
                "table": "todos",
                "conditions": [],
                field: value,
            }))
            .unwrap_err();

            assert!(
                err.to_string()
                    .contains("mini-sqlite query does not support"),
                "{field}: {err}"
            );
        }
    }
}
