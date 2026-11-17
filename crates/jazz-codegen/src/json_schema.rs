//! TypeScript for a `json` column's JSON Schema: the common subset (`type`,
//! `properties`/`required`, `additionalProperties`, `items`, `enum`,
//! `const`, `anyOf`/`oneOf`/`allOf`, `nullable`), read as
//! json-schema-to-ts reads it, so typed JSON columns keep the types the TS
//! DSL gave them. Anything beyond it is `unknown`.

use serde_json::Value;

use crate::ts::{prop, quote};

/// The TS type `schema` describes.
pub fn to_ts(schema: &Value) -> String {
    let ty = match schema {
        Value::Bool(true) => return "unknown".into(),
        Value::Bool(false) => return "never".into(),
        Value::Object(map) => map,
        _ => return "unknown".into(),
    };
    let mut parts: Vec<String> = Vec::new();
    if let Some(value) = ty.get("const") {
        parts.push(literal(value));
    } else if let Some(Value::Array(values)) = ty.get("enum") {
        parts.push(union(values.iter().map(literal).collect()));
    } else if let Some(Value::Array(options)) = ty.get("anyOf").or_else(|| ty.get("oneOf")) {
        parts.push(union(options.iter().map(|o| group(to_ts(o))).collect()));
    } else if let Some(kind) = ty.get("type") {
        let kinds: Vec<&str> = match kind {
            Value::String(k) => vec![k.as_str()],
            Value::Array(ks) => ks.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        if kinds.is_empty() {
            parts.push("unknown".into());
        } else {
            parts.push(union(kinds.iter().map(|k| of_kind(k, ty)).collect()));
        }
    } else if ty.contains_key("properties") {
        parts.push(object(ty));
    } else if ty.contains_key("items") {
        parts.push(of_kind("array", ty));
    } else if !ty.contains_key("allOf") {
        parts.push("unknown".into());
    }
    if let Some(Value::Array(all)) = ty.get("allOf") {
        parts.extend(all.iter().map(|s| group(to_ts(s))));
    }
    let mut out = if parts.len() == 1 {
        parts.pop().expect("one")
    } else {
        parts.join(" & ")
    };
    if ty.get("nullable") == Some(&Value::Bool(true)) && out != "unknown" {
        out = format!("{} | null", group(out));
    }
    out
}

fn of_kind(kind: &str, ty: &serde_json::Map<String, Value>) -> String {
    match kind {
        "string" => "string".into(),
        "number" | "integer" => "number".into(),
        "boolean" => "boolean".into(),
        "null" => "null".into(),
        "array" => match ty.get("items") {
            Some(Value::Array(tuple)) => format!(
                "[{}]",
                tuple.iter().map(to_ts).collect::<Vec<_>>().join(", ")
            ),
            Some(items) => format!("{}[]", array_element(to_ts(items))),
            None => "unknown[]".into(),
        },
        "object" => object(ty),
        _ => "unknown".into(),
    }
}

fn object(ty: &serde_json::Map<String, Value>) -> String {
    let required: Vec<&str> = match ty.get("required") {
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    let mut fields: Vec<String> = Vec::new();
    if let Some(Value::Object(properties)) = ty.get("properties") {
        for (name, schema) in properties {
            fields.push(format!(
                "{}{}: {}",
                prop(name),
                if required.contains(&name.as_str()) {
                    ""
                } else {
                    "?"
                },
                to_ts(schema)
            ));
        }
    }
    match ty.get("additionalProperties") {
        Some(Value::Bool(false)) => {}
        None | Some(Value::Bool(true)) => {
            if fields.is_empty() {
                return "{ [key: string]: unknown }".into();
            }
            fields.push("[key: string]: unknown".into());
        }
        Some(schema) => {
            if fields.is_empty() {
                return format!("{{ [key: string]: {} }}", to_ts(schema));
            }
            // Declared properties must fit an index signature's type.
            fields.push("[key: string]: unknown".into());
        }
    }
    if fields.is_empty() {
        "{}".into()
    } else {
        format!("{{ {} }}", fields.join("; "))
    }
}

fn literal(value: &Value) -> String {
    match value {
        Value::String(s) => quote(s),
        other => other.to_string(),
    }
}

fn union(items: Vec<String>) -> String {
    let mut unique: Vec<String> = Vec::new();
    for item in items {
        if !unique.contains(&item) {
            unique.push(item);
        }
    }
    match unique.len() {
        0 => "never".into(),
        _ => unique.join(" | "),
    }
}

/// Parenthesise a union or intersection where it's an operand.
pub(crate) fn group(ty: String) -> String {
    if top_level_operator(&ty) {
        format!("({ty})")
    } else {
        ty
    }
}

fn array_element(ty: String) -> String {
    group(ty)
}

/// Whether `ty` has a `|` or `&` outside brackets and strings.
fn top_level_operator(ty: &str) -> bool {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for c in ty.chars() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' | '[' | '(' | '<' => depth += 1,
            '}' | ']' | ')' | '>' => depth -= 1,
            '|' | '&' if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::to_ts;
    use serde_json::json;

    #[test]
    fn the_common_subset_becomes_typescript() {
        for (schema, ts) in [
            (json!({"type": "string"}), "string"),
            (json!({"type": ["string", "null"]}), "string | null"),
            (
                json!({"type": "integer", "nullable": true}),
                "number | null",
            ),
            (json!({"enum": ["a", "b", 1]}), "\"a\" | \"b\" | 1"),
            (json!({"const": "x"}), "\"x\""),
            (
                json!({"type": "array", "items": {"type": "number"}}),
                "number[]",
            ),
            (
                json!({"type": "array", "items": {"enum": ["a", "b"]}}),
                "(\"a\" | \"b\")[]",
            ),
            (
                json!({"type": "object", "properties": {"x": {"type": "number"}, "y": {"type": "string"}}, "required": ["x"], "additionalProperties": false}),
                "{ x: number; y?: string }",
            ),
            (
                json!({"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]}),
                "{ x: number; [key: string]: unknown }",
            ),
            (
                json!({"type": "object", "additionalProperties": {"type": "boolean"}}),
                "{ [key: string]: boolean }",
            ),
            (
                json!({"anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]}),
                "string | string[]",
            ),
            (json!({"oneOf": [{"const": 1}, {"const": 2}]}), "1 | 2"),
            (
                json!({"allOf": [{"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"], "additionalProperties": false}, {"type": "object", "properties": {"b": {"type": "string"}}, "additionalProperties": false}]}),
                "{ a: string } & { b?: string }",
            ),
            (json!({"$ref": "#/defs/x"}), "unknown"),
            (json!(true), "unknown"),
            (json!({}), "unknown"),
            (
                json!({"type": "object", "properties": {"odd key": {"type": "string"}}, "required": ["odd key"], "additionalProperties": false}),
                "{ \"odd key\": string }",
            ),
        ] {
            assert_eq!(to_ts(&schema), ts, "{schema}");
        }
    }
}
