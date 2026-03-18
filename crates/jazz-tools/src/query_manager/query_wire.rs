use serde_json::Value as JsonValue;

use super::query::Query;

pub fn parse_query_json(json: &str) -> Result<Query, String> {
    serde_json::from_str(json).map_err(|e| format!("Parse error: {}", e))
}

pub fn parse_query_value(value: JsonValue) -> Result<Query, String> {
    serde_json::from_value(value).map_err(|e| format!("Parse error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::{parse_query_json, parse_query_value};
    use crate::query_manager::query::{ArraySubqueryRequirement, Query};
    use crate::query_manager::relation_ir::{PredicateCmpOp, PredicateExpr, RelExpr, ValueRef};
    use crate::query_manager::types::Value;

    #[test]
    fn parses_canonical_relation_ir_payload() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "relation_ir": {
                "Filter": {
                    "input": { "TableScan": { "table": "todos" } },
                    "predicate": {
                        "Cmp": {
                            "left": { "column": "done" },
                            "op": "Eq",
                            "right": { "Literal": { "type": "Boolean", "value": true } }
                        }
                    }
                }
            }
        });

        let query = parse_query_json(&raw.to_string()).expect("parse query");
        assert_eq!(query.table.as_str(), "todos");
        match query.relation_ir {
            RelExpr::Filter { input, predicate } => {
                assert!(matches!(*input, RelExpr::TableScan { .. }));
                assert_eq!(
                    predicate,
                    PredicateExpr::Cmp {
                        left: crate::query_manager::relation_ir::ColumnRef::unscoped("done"),
                        op: PredicateCmpOp::Eq,
                        right: ValueRef::Literal(Value::Boolean(true)),
                    }
                );
            }
            other => panic!("unexpected relation_ir: {:?}", other),
        }
    }

    #[test]
    fn parse_value_payload_round_trip() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "relation_ir": {
                "Filter": {
                    "input": { "TableScan": { "table": "todos" } },
                    "predicate": {
                        "Cmp": {
                            "left": { "column": "title" },
                            "op": "Eq",
                            "right": { "Literal": { "type": "Text", "value": "hello" } }
                        }
                    }
                }
            }
        });

        let query: Query = parse_query_value(raw).expect("parse query");
        match query.relation_ir {
            RelExpr::Filter { predicate, .. } => match predicate {
                PredicateExpr::Cmp { right, .. } => {
                    assert_eq!(right, ValueRef::Literal(Value::Text("hello".to_string())));
                }
                other => panic!("unexpected predicate: {:?}", other),
            },
            other => panic!("unexpected relation_ir: {:?}", other),
        }
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
            "relation_ir": { "TableScan": { "table": "todos" } }
        });

        let query = parse_query_json(&raw.to_string()).expect("parse query");
        assert_eq!(query.array_subqueries.len(), 1);
        assert_eq!(
            query.array_subqueries[0].requirement,
            ArraySubqueryRequirement::AtLeastOne
        );
    }

    #[test]
    fn rejects_internally_tagged_relation_ir_payload() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "relation_ir": {
                "type": "TableScan",
                "table": "todos"
            }
        });

        let error =
            parse_query_json(&raw.to_string()).expect_err("internally-tagged payload should fail");
        assert!(error.contains("Parse error"), "{error}");
    }
}
