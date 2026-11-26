//! TypeScript for a `json` column's JSON Schema: the common subset (`type`,
//! `properties`/`required`, `additionalProperties`, `items`, `enum`,
//! `const`, `anyOf`/`oneOf`/`allOf`, `nullable`), read as
//! json-schema-to-ts reads it, so typed JSON columns keep the types the TS
//! DSL gave them. Anything beyond it is `unknown`.
//!
//! [`to_effect`] reads the same subset into an Effect Schema whose `Type` is
//! [`to_ts`]'s, refusing what Effect can't express that way yet.

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

/// The Effect Schema of `schema`, whose `Type` is [`to_ts`]'s; `Err` names
/// what it can't express yet (`allOf`'s intersections, object and array
/// literals).
pub fn to_effect(schema: &Value) -> Result<String, String> {
    let ty = match schema {
        Value::Bool(false) => return Ok("Schema.Never".into()),
        Value::Object(map) => map,
        _ => return Ok(UNKNOWN.into()),
    };
    if ty.contains_key("allOf") {
        return Err("`allOf`".into());
    }
    let mut out = if let Some(value) = ty.get("const") {
        effect_literal(value)?
    } else if let Some(Value::Array(values)) = ty.get("enum") {
        let primitive = values.iter().all(|v| !v.is_null());
        let members = values
            .iter()
            .map(effect_literal)
            .collect::<Result<Vec<_>, _>>()?;
        if primitive && !members.is_empty() {
            // One `Schema.Literals` of the values, deduplicated as `to_ts`
            // deduplicates its union.
            let mut unique: Vec<String> = Vec::new();
            for value in values.iter().map(literal) {
                if !unique.contains(&value) {
                    unique.push(value);
                }
            }
            crate::effect::literals(unique)
        } else {
            effect_union(members)
        }
    } else if let Some(Value::Array(options)) = ty.get("anyOf").or_else(|| ty.get("oneOf")) {
        effect_union(options.iter().map(to_effect).collect::<Result<_, _>>()?)
    } else if let Some(kind) = ty.get("type") {
        let kinds: Vec<&str> = match kind {
            Value::String(k) => vec![k.as_str()],
            Value::Array(ks) => ks.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        if kinds.is_empty() {
            UNKNOWN.into()
        } else {
            effect_union(
                kinds
                    .iter()
                    .map(|k| effect_of_kind(k, ty))
                    .collect::<Result<_, _>>()?,
            )
        }
    } else if ty.contains_key("properties") {
        effect_object(ty)?
    } else if ty.contains_key("items") {
        effect_of_kind("array", ty)?
    } else {
        UNKNOWN.into()
    };
    if ty.get("nullable") == Some(&Value::Bool(true)) && out != UNKNOWN {
        out = format!("Schema.NullOr({out})");
    }
    Ok(out)
}

const UNKNOWN: &str = "Schema.Unknown";

fn effect_of_kind(kind: &str, ty: &serde_json::Map<String, Value>) -> Result<String, String> {
    Ok(match kind {
        "string" => "Schema.String".into(),
        "number" | "integer" => "Schema.Number".into(),
        "boolean" => "Schema.Boolean".into(),
        "null" => "Schema.Null".into(),
        // `to_ts`'s arrays and tuples are mutable.
        "array" => match ty.get("items") {
            Some(Value::Array(tuple)) => format!(
                "Schema.mutable(Schema.Tuple([{}]))",
                tuple
                    .iter()
                    .map(to_effect)
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ")
            ),
            Some(items) => crate::effect::mutable_array(to_effect(items)?),
            None => crate::effect::mutable_array(UNKNOWN.into()),
        },
        "object" => effect_object(ty)?,
        _ => UNKNOWN.into(),
    })
}

fn effect_object(ty: &serde_json::Map<String, Value>) -> Result<String, String> {
    let required: Vec<&str> = match ty.get("required") {
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    let mut fields: Vec<String> = Vec::new();
    if let Some(Value::Object(properties)) = ty.get("properties") {
        for (name, schema) in properties {
            let value = to_effect(schema)?;
            fields.push(format!(
                "{}: {}",
                prop(name),
                if required.contains(&name.as_str()) {
                    value
                } else {
                    format!("Schema.optionalKey({value})")
                }
            ));
        }
    }
    let record = |value: String| format!("Schema.Record(Schema.String, {value})");
    let rest = match ty.get("additionalProperties") {
        Some(Value::Bool(false)) => None,
        None | Some(Value::Bool(true)) => Some(UNKNOWN.to_string()),
        // Declared properties must fit an index signature's type, so
        // beside them the rest is `unknown`, as in `to_ts`.
        Some(schema) if fields.is_empty() => Some(to_effect(schema)?),
        Some(_) => Some(UNKNOWN.to_string()),
    };
    let fields = if fields.is_empty() {
        "Schema.Struct({})".to_string()
    } else {
        format!("Schema.Struct({{ {} }})", fields.join(", "))
    };
    Ok(match rest {
        None => fields,
        Some(value) if fields == "Schema.Struct({})" => record(value),
        Some(value) => format!("Schema.StructWithRest({fields}, [{}])", record(value)),
    })
}

/// A `const` or `enum` value's Schema: JSON literals beyond strings,
/// numbers and booleans have no Effect literal.
fn effect_literal(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Ok("Schema.Null".into()),
        Value::String(_) | Value::Number(_) | Value::Bool(_) => {
            Ok(format!("Schema.Literal({})", literal(value)))
        }
        Value::Array(_) | Value::Object(_) => Err(format!("the literal `{value}`")),
    }
}

fn effect_union(members: Vec<String>) -> String {
    let mut unique: Vec<String> = Vec::new();
    for member in members {
        if !unique.contains(&member) {
            unique.push(member);
        }
    }
    match unique.len() {
        0 => "Schema.Never".into(),
        1 => unique.pop().expect("one"),
        _ => format!("Schema.Union([{}])", unique.join(", ")),
    }
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

    /// The same subset as Effect Schemas, each with `to_ts`'s type.
    #[test]
    fn the_common_subset_becomes_effect_schemas() {
        for (schema, effect) in [
            (json!({"type": "string"}), "Schema.String"),
            (
                json!({"type": ["string", "null"]}),
                "Schema.Union([Schema.String, Schema.Null])",
            ),
            (
                json!({"type": "integer", "nullable": true}),
                "Schema.NullOr(Schema.Number)",
            ),
            (
                json!({"enum": ["a", "b", 1, "a"]}),
                "Schema.Literals([\"a\", \"b\", 1])",
            ),
            (
                json!({"enum": ["a", null]}),
                "Schema.Union([Schema.Literal(\"a\"), Schema.Null])",
            ),
            (json!({"const": "x"}), "Schema.Literal(\"x\")"),
            (json!({"const": null}), "Schema.Null"),
            (
                json!({"type": "array", "items": {"type": "number"}}),
                "Schema.mutable(Schema.Array(Schema.Number))",
            ),
            (
                json!({"type": "array", "items": [{"type": "number"}, {"type": "string"}]}),
                "Schema.mutable(Schema.Tuple([Schema.Number, Schema.String]))",
            ),
            (
                json!({"type": "array"}),
                "Schema.mutable(Schema.Array(Schema.Unknown))",
            ),
            (
                json!({"type": "object", "properties": {"x": {"type": "number"}, "y": {"type": "string"}}, "required": ["x"], "additionalProperties": false}),
                "Schema.Struct({ x: Schema.Number, y: Schema.optionalKey(Schema.String) })",
            ),
            (
                json!({"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]}),
                "Schema.StructWithRest(Schema.Struct({ x: Schema.Number }), [Schema.Record(Schema.String, Schema.Unknown)])",
            ),
            (
                json!({"type": "object", "additionalProperties": {"type": "boolean"}}),
                "Schema.Record(Schema.String, Schema.Boolean)",
            ),
            (
                json!({"type": "object", "additionalProperties": false}),
                "Schema.Struct({})",
            ),
            (
                json!({"anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]}),
                "Schema.Union([Schema.String, Schema.mutable(Schema.Array(Schema.String))])",
            ),
            (
                json!({"oneOf": [{"const": 1}, {"const": 2}]}),
                "Schema.Union([Schema.Literal(1), Schema.Literal(2)])",
            ),
            (json!({"$ref": "#/defs/x"}), "Schema.Unknown"),
            (json!(true), "Schema.Unknown"),
            (json!(false), "Schema.Never"),
            (json!({}), "Schema.Unknown"),
            (
                json!({"type": "object", "properties": {"odd key": {"type": "string"}}, "required": ["odd key"], "additionalProperties": false}),
                "Schema.Struct({ \"odd key\": Schema.String })",
            ),
        ] {
            assert_eq!(super::to_effect(&schema).unwrap(), effect, "{schema}");
        }
        for (schema, what) in [
            (json!({"allOf": [{"type": "string"}]}), "`allOf`"),
            (json!({"const": {"a": 1}}), "the literal `{\"a\":1}`"),
            (json!({"enum": [[1]]}), "the literal `[1]`"),
        ] {
            assert_eq!(super::to_effect(&schema).unwrap_err(), what, "{schema}");
        }
    }
}
