use serde_json::{Map, Value as JsonValue};

use super::query::Query;

const VALUE_VARIANTS: &[&str] = &[
    "Integer",
    "BigInt",
    "Boolean",
    "Text",
    "Timestamp",
    "Uuid",
    "Array",
    "Row",
    "Null",
];

pub fn parse_query_json_compat(json: &str) -> Result<Query, String> {
    let query_json: JsonValue =
        serde_json::from_str(json).map_err(|e| format!("Parse error: {}", e))?;
    parse_query_value_compat(query_json)
}

pub fn parse_query_value_compat(mut value: JsonValue) -> Result<Query, String> {
    let requires_relation_refresh = normalize_query_json_compat(&mut value)?;
    let mut query: Query =
        serde_json::from_value(value).map_err(|e| format!("Parse error: {}", e))?;
    if requires_relation_refresh {
        query
            .refresh_relation_ir()
            .map_err(|e| format!("Query build error: {}", e))?;
    }
    Ok(query)
}

fn normalize_query_json_compat(value: &mut JsonValue) -> Result<bool, String> {
    let object = expect_object_mut(value, "query payload")?;
    let table = object
        .get("table")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .unwrap_or_default();

    let relation_ir = object.remove("relation_ir");
    let mut requires_relation_refresh = false;

    let normalized_relation = match relation_ir {
        Some(rel) if rel.is_null() => {
            requires_relation_refresh = true;
            build_table_scan_relation(&table)?
        }
        Some(rel) if is_tagged_variant(&rel) => convert_relation_expr(&rel)?,
        Some(rel) => rel,
        None => {
            requires_relation_refresh = true;
            build_table_scan_relation(&table)?
        }
    };

    object.insert("relation_ir".to_string(), normalized_relation);
    Ok(requires_relation_refresh)
}

fn build_table_scan_relation(table: &str) -> Result<JsonValue, String> {
    if table.is_empty() {
        return Err("Parse error: query payload missing required field `table`".to_string());
    }
    Ok(wrap_variant(
        "TableScan",
        json_object(vec![("table", JsonValue::String(table.to_string()))]),
    ))
}

fn convert_relation_expr(value: &JsonValue) -> Result<JsonValue, String> {
    if !is_tagged_variant(value) {
        return Ok(value.clone());
    }

    let object = expect_object(value, "relation_ir")?;
    let variant = required_str_field(object, "type", "relation_ir")?;

    match variant {
        "TableScan" => Ok(wrap_variant(
            "TableScan",
            json_object(vec![(
                "table",
                required_field(object, "table", "TableScan")?.clone(),
            )]),
        )),
        "Filter" => Ok(wrap_variant(
            "Filter",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "Filter")?)?,
                ),
                (
                    "predicate",
                    convert_predicate_expr(required_field(object, "predicate", "Filter")?)?,
                ),
            ]),
        )),
        "Join" => Ok(wrap_variant(
            "Join",
            json_object(vec![
                (
                    "left",
                    convert_relation_expr(required_field(object, "left", "Join")?)?,
                ),
                (
                    "right",
                    convert_relation_expr(required_field(object, "right", "Join")?)?,
                ),
                (
                    "on",
                    convert_join_conditions(required_field(object, "on", "Join")?)?,
                ),
                (
                    "join_kind",
                    optional_field(object, &["join_kind", "joinKind"])
                        .ok_or_else(|| {
                            "Parse error: Join relation missing required field `joinKind`"
                                .to_string()
                        })?
                        .clone(),
                ),
            ]),
        )),
        "Project" => Ok(wrap_variant(
            "Project",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "Project")?)?,
                ),
                (
                    "columns",
                    convert_project_columns(required_field(object, "columns", "Project")?)?,
                ),
            ]),
        )),
        "Gather" => Ok(wrap_variant(
            "Gather",
            json_object(vec![
                (
                    "seed",
                    convert_relation_expr(required_field(object, "seed", "Gather")?)?,
                ),
                (
                    "step",
                    convert_relation_expr(required_field(object, "step", "Gather")?)?,
                ),
                (
                    "frontier_key",
                    convert_key_ref(
                        optional_field(object, &["frontier_key", "frontierKey"]).ok_or_else(
                            || {
                                "Parse error: Gather relation missing required field `frontierKey`"
                                    .to_string()
                            },
                        )?,
                    )?,
                ),
                (
                    "max_depth",
                    optional_field(object, &["max_depth", "maxDepth"])
                        .ok_or_else(|| {
                            "Parse error: Gather relation missing required field `maxDepth`"
                                .to_string()
                        })?
                        .clone(),
                ),
                (
                    "dedupe_key",
                    convert_key_refs(
                        optional_field(object, &["dedupe_key", "dedupeKey"]).ok_or_else(|| {
                            "Parse error: Gather relation missing required field `dedupeKey`"
                                .to_string()
                        })?,
                    )?,
                ),
            ]),
        )),
        "Distinct" => Ok(wrap_variant(
            "Distinct",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "Distinct")?)?,
                ),
                (
                    "key",
                    convert_key_refs(required_field(object, "key", "Distinct")?)?,
                ),
            ]),
        )),
        "OrderBy" => Ok(wrap_variant(
            "OrderBy",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "OrderBy")?)?,
                ),
                (
                    "terms",
                    convert_order_terms(required_field(object, "terms", "OrderBy")?)?,
                ),
            ]),
        )),
        "Offset" => Ok(wrap_variant(
            "Offset",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "Offset")?)?,
                ),
                (
                    "offset",
                    required_field(object, "offset", "Offset")?.clone(),
                ),
            ]),
        )),
        "Limit" => Ok(wrap_variant(
            "Limit",
            json_object(vec![
                (
                    "input",
                    convert_relation_expr(required_field(object, "input", "Limit")?)?,
                ),
                ("limit", required_field(object, "limit", "Limit")?.clone()),
            ]),
        )),
        other => Err(format!(
            "Parse error: unsupported tagged relation_ir variant `{}`",
            other
        )),
    }
}

fn convert_join_conditions(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "join conditions")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = expect_object(item, "join condition")?;
        out.push(json_object(vec![
            (
                "left",
                convert_column_ref(required_field(obj, "left", "join condition")?)?,
            ),
            (
                "right",
                convert_column_ref(required_field(obj, "right", "join condition")?)?,
            ),
        ]));
    }
    Ok(JsonValue::Array(out))
}

fn convert_project_columns(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "project columns")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = expect_object(item, "project column")?;
        out.push(json_object(vec![
            (
                "alias",
                required_field(obj, "alias", "project column")?.clone(),
            ),
            (
                "expr",
                convert_project_expr(required_field(obj, "expr", "project column")?)?,
            ),
        ]));
    }
    Ok(JsonValue::Array(out))
}

fn convert_project_expr(value: &JsonValue) -> Result<JsonValue, String> {
    if !is_tagged_variant(value) {
        return Ok(value.clone());
    }

    let object = expect_object(value, "project expression")?;
    let variant = required_str_field(object, "type", "project expression")?;

    match variant {
        "Column" => Ok(wrap_variant(
            "Column",
            convert_column_ref(required_field(object, "column", "project expression")?)?,
        )),
        "RowId" => Ok(wrap_variant(
            "RowId",
            optional_field(object, &["source"])
                .ok_or_else(|| {
                    "Parse error: RowId project expression missing `source` field".to_string()
                })?
                .clone(),
        )),
        other => Err(format!(
            "Parse error: unsupported tagged project expression variant `{}`",
            other
        )),
    }
}

fn convert_key_ref(value: &JsonValue) -> Result<JsonValue, String> {
    if !is_tagged_variant(value) {
        return Ok(value.clone());
    }

    let object = expect_object(value, "key reference")?;
    let variant = required_str_field(object, "type", "key reference")?;

    match variant {
        "Column" => Ok(wrap_variant(
            "Column",
            convert_column_ref(required_field(object, "column", "key reference")?)?,
        )),
        "RowId" => Ok(wrap_variant(
            "RowId",
            optional_field(object, &["source"])
                .ok_or_else(|| {
                    "Parse error: RowId key reference missing `source` field".to_string()
                })?
                .clone(),
        )),
        other => Err(format!(
            "Parse error: unsupported tagged key reference variant `{}`",
            other
        )),
    }
}

fn convert_key_refs(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "key references")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(convert_key_ref(item)?);
    }
    Ok(JsonValue::Array(out))
}

fn convert_order_terms(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "order by terms")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = expect_object(item, "order by term")?;
        out.push(json_object(vec![
            (
                "column",
                convert_column_ref(required_field(obj, "column", "order by term")?)?,
            ),
            (
                "direction",
                required_field(obj, "direction", "order by term")?.clone(),
            ),
        ]));
    }
    Ok(JsonValue::Array(out))
}

fn convert_column_ref(value: &JsonValue) -> Result<JsonValue, String> {
    let object = expect_object(value, "column reference")?;
    let mut out = Map::new();
    if let Some(scope) = object.get("scope") {
        out.insert("scope".to_string(), scope.clone());
    }
    out.insert(
        "column".to_string(),
        required_field(object, "column", "column reference")?.clone(),
    );
    Ok(JsonValue::Object(out))
}

fn convert_predicate_expr(value: &JsonValue) -> Result<JsonValue, String> {
    if let Some(unit_variant) = value.as_str() {
        return match unit_variant {
            "True" | "False" => Ok(JsonValue::String(unit_variant.to_string())),
            other => Err(format!(
                "Parse error: unsupported predicate literal variant `{}`",
                other
            )),
        };
    }
    if !is_tagged_variant(value) {
        return Ok(value.clone());
    }

    let object = expect_object(value, "predicate")?;
    let variant = required_str_field(object, "type", "predicate")?;

    match variant {
        "Cmp" => Ok(wrap_variant(
            "Cmp",
            json_object(vec![
                (
                    "left",
                    convert_column_ref(required_field(object, "left", "Cmp predicate")?)?,
                ),
                ("op", required_field(object, "op", "Cmp predicate")?.clone()),
                (
                    "right",
                    convert_value_ref(required_field(object, "right", "Cmp predicate")?)?,
                ),
            ]),
        )),
        "Contains" => Ok(wrap_variant(
            "Contains",
            json_object(vec![
                (
                    "left",
                    convert_column_ref(required_field(object, "left", "Contains predicate")?)?,
                ),
                (
                    "right",
                    convert_value_ref(optional_field(object, &["right", "value"]).ok_or_else(
                        || "Parse error: Contains predicate missing `value` field".to_string(),
                    )?)?,
                ),
            ]),
        )),
        "IsNull" => Ok(wrap_variant(
            "IsNull",
            json_object(vec![(
                "column",
                convert_column_ref(required_field(object, "column", "IsNull predicate")?)?,
            )]),
        )),
        "IsNotNull" => Ok(wrap_variant(
            "IsNotNull",
            json_object(vec![(
                "column",
                convert_column_ref(required_field(object, "column", "IsNotNull predicate")?)?,
            )]),
        )),
        "In" => Ok(wrap_variant(
            "In",
            json_object(vec![
                (
                    "left",
                    convert_column_ref(required_field(object, "left", "In predicate")?)?,
                ),
                (
                    "values",
                    convert_value_refs(required_field(object, "values", "In predicate")?)?,
                ),
            ]),
        )),
        "And" => {
            Ok(wrap_variant(
                "And",
                convert_predicate_array(optional_field(object, &["exprs"]).ok_or_else(|| {
                    "Parse error: And predicate missing `exprs` field".to_string()
                })?)?,
            ))
        }
        "Or" => Ok(wrap_variant(
            "Or",
            convert_predicate_array(
                optional_field(object, &["exprs"])
                    .ok_or_else(|| "Parse error: Or predicate missing `exprs` field".to_string())?,
            )?,
        )),
        "Not" => Ok(wrap_variant(
            "Not",
            convert_predicate_expr(
                optional_field(object, &["expr"])
                    .ok_or_else(|| "Parse error: Not predicate missing `expr` field".to_string())?,
            )?,
        )),
        "True" => Ok(JsonValue::String("True".to_string())),
        "False" => Ok(JsonValue::String("False".to_string())),
        other => Err(format!(
            "Parse error: unsupported tagged predicate variant `{}`",
            other
        )),
    }
}

fn convert_predicate_array(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "predicate expressions")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(convert_predicate_expr(item)?);
    }
    Ok(JsonValue::Array(out))
}

fn convert_value_ref(value: &JsonValue) -> Result<JsonValue, String> {
    if !is_tagged_variant(value) {
        return Ok(value.clone());
    }

    let object = expect_object(value, "value reference")?;
    let variant = required_str_field(object, "type", "value reference")?;

    match variant {
        "Literal" => Ok(wrap_variant(
            "Literal",
            convert_literal_value(required_field(object, "value", "Literal value reference")?)?,
        )),
        "SessionRef" => Ok(wrap_variant(
            "SessionRef",
            optional_field(object, &["path"])
                .ok_or_else(|| {
                    "Parse error: SessionRef value reference missing `path` field".to_string()
                })?
                .clone(),
        )),
        "OuterColumn" => Ok(wrap_variant(
            "OuterColumn",
            convert_column_ref(optional_field(object, &["column"]).ok_or_else(|| {
                "Parse error: OuterColumn value reference missing `column` field".to_string()
            })?)?,
        )),
        "FrontierColumn" => Ok(wrap_variant(
            "FrontierColumn",
            convert_column_ref(optional_field(object, &["column"]).ok_or_else(|| {
                "Parse error: FrontierColumn value reference missing `column` field".to_string()
            })?)?,
        )),
        "RowId" => Ok(wrap_variant(
            "RowId",
            optional_field(object, &["source"])
                .ok_or_else(|| {
                    "Parse error: RowId value reference missing `source` field".to_string()
                })?
                .clone(),
        )),
        other => Err(format!(
            "Parse error: unsupported tagged value reference variant `{}`",
            other
        )),
    }
}

fn convert_value_refs(value: &JsonValue) -> Result<JsonValue, String> {
    let items = expect_array(value, "value references")?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(convert_value_ref(item)?);
    }
    Ok(JsonValue::Array(out))
}

fn convert_literal_value(value: &JsonValue) -> Result<JsonValue, String> {
    if is_value_shape(value) {
        return Ok(value.clone());
    }

    match value {
        JsonValue::Null => Ok(JsonValue::String("Null".to_string())),
        JsonValue::Bool(value) => Ok(wrap_variant("Boolean", JsonValue::Bool(*value))),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
                    Ok(wrap_variant(
                        "Integer",
                        JsonValue::Number(serde_json::Number::from(value as i32)),
                    ))
                } else {
                    Ok(wrap_variant(
                        "BigInt",
                        JsonValue::Number(serde_json::Number::from(value)),
                    ))
                }
            } else if let Some(value) = number.as_u64() {
                if value <= i32::MAX as u64 {
                    Ok(wrap_variant(
                        "Integer",
                        JsonValue::Number(serde_json::Number::from(value as i32)),
                    ))
                } else if value <= i64::MAX as u64 {
                    Ok(wrap_variant(
                        "BigInt",
                        JsonValue::Number(serde_json::Number::from(value as i64)),
                    ))
                } else {
                    Err("Parse error: numeric literal exceeds i64 range".to_string())
                }
            } else {
                Err("Parse error: relation_ir numeric literals must be integers".to_string())
            }
        }
        JsonValue::String(value) => {
            if uuid::Uuid::parse_str(value).is_ok() {
                Ok(wrap_variant("Uuid", JsonValue::String(value.clone())))
            } else {
                Ok(wrap_variant("Text", JsonValue::String(value.clone())))
            }
        }
        JsonValue::Array(items) => {
            let mut converted = Vec::with_capacity(items.len());
            for item in items {
                converted.push(convert_literal_value(item)?);
            }
            Ok(wrap_variant("Array", JsonValue::Array(converted)))
        }
        JsonValue::Object(_) => Err(
            "Parse error: relation_ir literal object must use typed Value enum representation"
                .to_string(),
        ),
    }
}

fn wrap_variant(name: &str, value: JsonValue) -> JsonValue {
    JsonValue::Object(json_map(vec![(name, value)]))
}

fn json_object(entries: Vec<(&str, JsonValue)>) -> JsonValue {
    JsonValue::Object(json_map(entries))
}

fn json_map(entries: Vec<(&str, JsonValue)>) -> Map<String, JsonValue> {
    let mut object = Map::new();
    for (key, value) in entries {
        object.insert(key.to_string(), value);
    }
    object
}

fn is_tagged_variant(value: &JsonValue) -> bool {
    value
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(JsonValue::as_str)
        .is_some()
}

fn is_value_shape(value: &JsonValue) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.len() == 1
        && object
            .keys()
            .next()
            .map(|key| VALUE_VARIANTS.contains(&key.as_str()))
            .unwrap_or(false)
}

fn expect_object<'a>(
    value: &'a JsonValue,
    context: &str,
) -> Result<&'a Map<String, JsonValue>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("Parse error: {} must be a JSON object", context))
}

fn expect_object_mut<'a>(
    value: &'a mut JsonValue,
    context: &str,
) -> Result<&'a mut Map<String, JsonValue>, String> {
    value
        .as_object_mut()
        .ok_or_else(|| format!("Parse error: {} must be a JSON object", context))
}

fn expect_array<'a>(value: &'a JsonValue, context: &str) -> Result<&'a Vec<JsonValue>, String> {
    value
        .as_array()
        .ok_or_else(|| format!("Parse error: {} must be an array", context))
}

fn required_field<'a>(
    object: &'a Map<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<&'a JsonValue, String> {
    object
        .get(key)
        .ok_or_else(|| format!("Parse error: {} missing required field `{}`", context, key))
}

fn required_str_field<'a>(
    object: &'a Map<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<&'a str, String> {
    object
        .get(key)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| format!("Parse error: {} field `{}` must be a string", context, key))
}

fn optional_field<'a>(object: &'a Map<String, JsonValue>, keys: &[&str]) -> Option<&'a JsonValue> {
    keys.iter().find_map(|key| object.get(*key))
}

#[cfg(test)]
mod tests {
    use super::parse_query_json_compat;
    use crate::query_manager::query::Query;
    use crate::query_manager::relation_ir::{PredicateExpr, RelExpr, ValueRef};
    use crate::query_manager::types::Value;

    #[test]
    fn parses_tagged_relation_ir_payload() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "joins": [],
            "disjuncts": [{"conditions": []}],
            "order_by": [],
            "offset": 0,
            "limit": null,
            "include_deleted": false,
            "array_subqueries": [],
            "relation_ir": {
                "type": "Filter",
                "input": { "type": "TableScan", "table": "todos" },
                "predicate": {
                    "type": "Cmp",
                    "left": { "column": "done" },
                    "op": "Eq",
                    "right": {
                        "type": "Literal",
                        "value": { "Boolean": true }
                    }
                }
            }
        });

        let query = parse_query_json_compat(&raw.to_string()).expect("parse query");
        assert_eq!(query.table.as_str(), "todos");
        match query.relation_ir {
            RelExpr::Filter { input, predicate } => {
                assert!(matches!(*input, RelExpr::TableScan { .. }));
                match predicate {
                    PredicateExpr::Cmp { right, .. } => {
                        assert_eq!(right, ValueRef::Literal(Value::Boolean(true)));
                    }
                    other => panic!("unexpected predicate: {:?}", other),
                }
            }
            other => panic!("unexpected relation_ir: {:?}", other),
        }
    }

    #[test]
    fn parses_query_without_relation_ir_and_rebuilds_from_legacy_fields() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "joins": [],
            "disjuncts": [
                {
                    "conditions": [
                        { "Eq": { "column": "done", "value": { "Boolean": false } } }
                    ]
                }
            ],
            "order_by": [],
            "offset": 0,
            "limit": null,
            "include_deleted": false,
            "array_subqueries": []
        });

        let query = parse_query_json_compat(&raw.to_string()).expect("parse query");
        match query.relation_ir {
            RelExpr::Filter { .. } => {}
            other => panic!("expected filter relation, got {:?}", other),
        }
    }

    #[test]
    fn parse_value_payload_round_trip() {
        let raw = serde_json::json!({
            "table": "todos",
            "branches": ["main"],
            "joins": [],
            "disjuncts": [{"conditions": []}],
            "order_by": [],
            "offset": 0,
            "limit": null,
            "include_deleted": false,
            "array_subqueries": [],
            "relation_ir": {
                "type": "Filter",
                "input": { "type": "TableScan", "table": "todos" },
                "predicate": {
                    "type": "Cmp",
                    "left": { "column": "title" },
                    "op": "Eq",
                    "right": {
                        "type": "Literal",
                        "value": "hello"
                    }
                }
            }
        });

        let query: Query = parse_query_json_compat(&raw.to_string()).expect("parse query");
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
}
