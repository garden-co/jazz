use crate::query_api::BuiltQuery;
use crate::query_observation::{built_query_from_read, observed_ids_from_query_value};
use crate::sync::QueryReadRecord;
use crate::Result;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug)]
pub(crate) enum ObservedQuery {
    Predicate {
        op: PredicateOp,
        value: JsonValue,
    },
    RecursiveRefs {
        root_id: String,
    },
    TopCreatedAt {
        value: JsonValue,
        limit: usize,
        observed_ids: Vec<String>,
    },
    TopField {
        value: JsonValue,
        order_field: String,
        limit: usize,
        observed_ids: Vec<String>,
    },
    Built {
        query: BuiltQuery,
        observed_ids: Vec<String>,
    },
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PredicateOp {
    Eq,
    Ne,
    Contains,
    In,
}

impl PredicateOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Contains => "contains",
            Self::In => "in",
        }
    }
}

pub(crate) fn decode(read: &QueryReadRecord) -> Result<ObservedQuery> {
    match read.op.as_str() {
        "eq" => Ok(ObservedQuery::Predicate {
            op: PredicateOp::Eq,
            value: read.value.clone(),
        }),
        "ne" => Ok(ObservedQuery::Predicate {
            op: PredicateOp::Ne,
            value: read.value.clone(),
        }),
        "contains" => Ok(ObservedQuery::Predicate {
            op: PredicateOp::Contains,
            value: read.value.clone(),
        }),
        "in" => Ok(ObservedQuery::Predicate {
            op: PredicateOp::In,
            value: read.value.clone(),
        }),
        "recursive_refs" => {
            let Some(root_id) = read.value.as_str() else {
                return Err(crate::Error::new("recursive refs expects root id string"));
            };
            Ok(ObservedQuery::RecursiveRefs {
                root_id: root_id.to_owned(),
            })
        }
        "eq_top_created_at_desc" => {
            let value = read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            Ok(ObservedQuery::TopCreatedAt {
                value: value.clone(),
                limit: limit as usize,
                observed_ids: observed_ids_from_query_value(&read.value)?,
            })
        }
        "eq_top_field_desc" => {
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
            Ok(ObservedQuery::TopField {
                value: value.clone(),
                order_field: order_field.to_owned(),
                limit: limit as usize,
                observed_ids: observed_ids_from_query_value(&read.value)?,
            })
        }
        "query" => Ok(ObservedQuery::Built {
            query: built_query_from_read(read)?,
            observed_ids: observed_ids_from_query_value(&read.value)?,
        }),
        "absent" => Ok(ObservedQuery::Absent),
        op => Err(crate::Error::new(format!(
            "unsupported observed query op {op}"
        ))),
    }
}

pub(crate) fn identity_value(read: &QueryReadRecord) -> Result<JsonValue> {
    match decode(read)? {
        ObservedQuery::TopCreatedAt { value, limit, .. } => Ok(serde_json::json!({
            "eq": value,
            "limit": limit,
        })),
        ObservedQuery::TopField {
            value,
            order_field,
            limit,
            ..
        } => Ok(serde_json::json!({
            "eq": value,
            "order_field": order_field,
            "limit": limit,
        })),
        ObservedQuery::Built { query, .. } => Ok(query.to_json_value()),
        _ => Ok(read.value.clone()),
    }
}
