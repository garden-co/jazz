use serde_json::Value as JsonValue;

use super::query::Query;

pub(crate) fn parse_query_json(json: &str) -> Result<Query, String> {
    serde_json::from_str(json).map_err(|e| format!("Parse error: {}", e))
}

fn parse_query_value(value: JsonValue) -> Result<Query, String> {
    serde_json::from_value(value).map_err(|e| format!("Parse error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::{parse_query_json, parse_query_value};
    use crate::query_api::query::{ArraySubqueryRequirement, Query};

    #[test]
    fn parses_public_query_payload() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "limit": 10
        });

        let query = parse_query_json(&raw.to_string()).expect("parse query");
        assert_eq!(query.table.as_str(), "todos");
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn parse_value_payload_round_trip() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "offset": 3
        });

        let query: Query = parse_query_value(raw).expect("parse query");
        assert_eq!(query.offset, 3);
    }

    #[test]
    fn parses_array_subquery_requirement_payload() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "array_subqueries": [{
                "column_name": "owner",
                "table": "users",
                "joins": [],
                "inner_column": "id",
                "outer_column": "todos.owner_id",
                "filters": [],
                "order_by": [],
                "limit": 1,
                "requirement": "AtLeastOne",
                "nested_arrays": []
            }],
            "limit": 1
        });

        let query = parse_query_json(&raw.to_string()).expect("parse query");
        assert_eq!(query.array_subqueries.len(), 1);
        assert_eq!(
            query.array_subqueries[0].requirement,
            ArraySubqueryRequirement::AtLeastOne
        );
    }
}
