//! `jazz.permissions.json` (SPEC §10.10): each table's read / insert /
//! update / delete policies, as expression strings and anyOf / allOf
//! objects.

use std::collections::BTreeMap;

use crate::catalogue::Permissions;
use crate::tools::public_schema::{OperationPolicy, PolicyExpr, Schema, TableName, TablePolicies};

use super::CodegenRelation;
use super::expr::{self, Ast, ComputedNames, Context};
use super::json::Json;

/// A refusal at a JSON path of the permissions file.
pub type Refusal = (String, String);

fn refuse(path: impl Into<String>, message: impl Into<String>) -> Refusal {
    (path.into(), message.into())
}

/// The keys the file takes at the top (besides `$comment`), which the JSON
/// Schema in docs/public/schemas/ must list too.
pub const TOP_KEYS: &[&str] = &["format", "$schema", "$generated", "tables"];

pub fn read(
    root: &Json,
    schema: &Schema,
    relations: &BTreeMap<String, BTreeMap<String, CodegenRelation>>,
    computed: &ComputedNames,
) -> Result<Permissions, Refusal> {
    let top = root
        .as_object()
        .ok_or_else(|| refuse("(top)", "must be an object"))?;
    for (key, _) in top {
        if key == "defs" {
            return Err(refuse(
                "defs",
                "`defs` was removed in alpha.66: inline each definition where it is \
                 called, or author the app in schema.ts, where helpers are plain \
                 functions",
            ));
        }
        if key != "$comment" && !TOP_KEYS.contains(&key.as_str()) {
            return Err(refuse(
                "(top)",
                format!(
                    "unknown key `{key}`{}; this file takes `$schema` and `tables`",
                    expr::did_you_mean(key, TOP_KEYS.iter().copied())
                ),
            ));
        }
    }
    let context = Context {
        schema,
        relations,
        computed,
    };
    let mut permissions = Permissions::new();
    let Some(tables) = root.get("tables") else {
        return Ok(permissions);
    };
    let tables = tables
        .as_object()
        .ok_or_else(|| refuse("tables", "must be an object of table: policies"))?;
    for (table, ops) in tables {
        let path = format!("tables.{table}");
        // Jazz sets its own system tables' policies, whatever the schema
        // holds.
        if crate::schema::is_system_table(table) {
            return Err(refuse(&path, super::app_file::reserved_table_name(table)));
        }
        if schema.get(&TableName::from(table.as_str())).is_none() {
            let names: Vec<String> = schema.keys().map(|t| t.as_str().to_string()).collect();
            return Err(refuse(
                &path,
                format!(
                    "jazz.app.json has no table `{table}`{}",
                    expr::did_you_mean(table, names.iter().map(String::as_str))
                ),
            ));
        }
        let ops = ops
            .as_object()
            .ok_or_else(|| refuse(&path, "must be an object with read, insert, update, delete"))?;
        let mut policies = TablePolicies::default();
        for (op, policy) in ops {
            let op_path = format!("{path}.{op}");
            match op.as_str() {
                "$comment" => {}
                "read" | "insert" | "delete" => {
                    let side = |key: &str| -> Result<Option<PolicyExpr>, Refusal> {
                        policy
                            .get(key)
                            .map(|p| compile(p, &format!("{op_path}.{key}"), &context, table))
                            .transpose()
                    };
                    let operation = if is_two_sided(policy) {
                        OperationPolicy {
                            using: side("using")?,
                            with_check: side("withCheck")?,
                        }
                    } else {
                        let expr = Some(compile(policy, &op_path, &context, table)?);
                        if op == "insert" {
                            OperationPolicy {
                                using: None,
                                with_check: expr,
                            }
                        } else {
                            OperationPolicy {
                                using: expr,
                                with_check: None,
                            }
                        }
                    };
                    match op.as_str() {
                        "read" => policies.select = operation,
                        "insert" => policies.insert = operation,
                        _ => policies.delete = operation,
                    }
                }
                "update" => {
                    let old_new = policy
                        .as_object()
                        .filter(|entries| entries.iter().any(|(k, _)| k == "old" || k == "new"));
                    policies.update = match old_new {
                        Some(entries) => {
                            for (key, _) in entries {
                                if !matches!(key.as_str(), "old" | "new" | "$comment") {
                                    return Err(refuse(
                                        &op_path,
                                        format!(
                                            "unknown key `{key}`; update takes a policy or `old` and `new`"
                                        ),
                                    ));
                                }
                            }
                            let part = |key: &str| -> Result<Option<PolicyExpr>, Refusal> {
                                policy
                                    .get(key)
                                    .map(|p| {
                                        compile(p, &format!("{op_path}.{key}"), &context, table)
                                    })
                                    .transpose()
                            };
                            OperationPolicy {
                                using: part("old")?,
                                with_check: part("new")?,
                            }
                        }
                        None => {
                            let both = compile(policy, &op_path, &context, table)?;
                            OperationPolicy {
                                using: Some(both.clone()),
                                with_check: Some(both),
                            }
                        }
                    };
                }
                other => {
                    return Err(refuse(
                        &path,
                        format!(
                            "unknown operation `{other}`{}; a table takes read, insert, update and delete",
                            expr::did_you_mean(other, ["read", "insert", "update", "delete"])
                        ),
                    ));
                }
            }
        }
        let clauses = [
            ("read", &policies.select),
            ("insert", &policies.insert),
            ("update", &policies.update),
            ("delete", &policies.delete),
        ];
        for (op, policy) in clauses {
            if let Some(system) = [&policy.using, &policy.with_check]
                .into_iter()
                .flatten()
                .find_map(super::encryption::reaches_system_table)
            {
                return Err(refuse(
                    format!("{path}.{op}"),
                    format!(
                        "the policy reaches {system}, one of Jazz's system tables; an app policy \
                         may only test the members table with `exists __e2ee_members(…)`"
                    ),
                ));
            }
        }
        if policies != TablePolicies::default() {
            permissions.insert(TableName::from(table.as_str()), policies);
        }
    }
    Ok(permissions)
}

/// Whether an operation's value is `{"using": …, "withCheck": …}`: both
/// sides of a read, insert or delete policy, which the DSL never writes but
/// the engine can hold.
fn is_two_sided(value: &Json) -> bool {
    value.as_object().is_some_and(|entries| {
        !entries.is_empty()
            && entries
                .iter()
                .all(|(k, _)| matches!(k.as_str(), "using" | "withCheck" | "$comment"))
            && entries.iter().any(|(k, _)| k != "$comment")
    })
}

/// A policy read, before it is compiled: an expression (with its source,
/// for error reports) or an `anyOf` / `allOf` of policies.
enum Body {
    Expr(Ast, String),
    AnyOf(Vec<Body>),
    AllOf(Vec<Body>),
}

fn read_body(value: &Json, path: &str) -> Result<Body, Refusal> {
    match value {
        Json::String(source) => {
            let ast = expr::parse(source).map_err(|e| refuse(path, e.render(source)))?;
            Ok(Body::Expr(ast, source.clone()))
        }
        Json::Object(entries) => {
            let key = |k: &str| value.get(k);
            for (k, _) in entries {
                if !matches!(k.as_str(), "anyOf" | "allOf" | "where" | "$comment") {
                    return Err(refuse(
                        path,
                        format!(
                            "unknown key `{k}`; a policy is a string, {{ \"anyOf\": [...] }}, {{ \"allOf\": [...] }} or {{ \"$comment\": ..., \"where\": ... }}"
                        ),
                    ));
                }
            }
            let list = |items: &Json, kind: &str| -> Result<Vec<Body>, Refusal> {
                match items {
                    Json::Array(items) if !items.is_empty() => items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| read_body(item, &format!("{path}.{kind}[{i}]")))
                        .collect(),
                    _ => Err(refuse(
                        format!("{path}.{kind}"),
                        "must be a non-empty array of policies",
                    )),
                }
            };
            match (key("anyOf"), key("allOf"), key("where")) {
                (Some(items), None, None) => Ok(Body::AnyOf(list(items, "anyOf")?)),
                (None, Some(items), None) => Ok(Body::AllOf(list(items, "allOf")?)),
                (None, None, Some(inner)) => read_body(inner, &format!("{path}.where")),
                _ => Err(refuse(
                    path,
                    "a policy object takes exactly one of `anyOf`, `allOf`, `where`",
                )),
            }
        }
        other => Err(refuse(
            path,
            format!("a policy is a string or an object, not {}", other.kind()),
        )),
    }
}

/// Compile a policy, reporting an expression error with its path and caret.
fn compile(
    value: &Json,
    path: &str,
    context: &Context<'_>,
    table: &str,
) -> Result<PolicyExpr, Refusal> {
    match value {
        Json::String(source) => {
            let ast = expr::parse(source).map_err(|e| refuse(path, e.render(source)))?;
            expr::compile(&ast, context, table).map_err(|e| refuse(path, e.render(source)))
        }
        Json::Object(_) => {
            let body = read_body(value, path)?;
            compile_body(&body, path, context, table)
        }
        other => Err(refuse(
            path,
            format!("a policy is a string or an object, not {}", other.kind()),
        )),
    }
}

fn compile_body(
    body: &Body,
    path: &str,
    context: &Context<'_>,
    table: &str,
) -> Result<PolicyExpr, Refusal> {
    match body {
        Body::Expr(ast, source) => {
            expr::compile(ast, context, table).map_err(|e| refuse(path, e.render(source)))
        }
        Body::AnyOf(items) => Ok(PolicyExpr::Or(
            items
                .iter()
                .enumerate()
                .map(|(i, item)| compile_body(item, &format!("{path}.anyOf[{i}]"), context, table))
                .collect::<Result<_, _>>()?,
        )),
        Body::AllOf(items) => Ok(PolicyExpr::And(
            items
                .iter()
                .enumerate()
                .map(|(i, item)| compile_body(item, &format!("{path}.allOf[{i}]"), context, table))
                .collect::<Result<_, _>>()?,
        )),
    }
}

/// The file for `permissions`.
pub fn write(
    permissions: &Permissions,
    schema: &Schema,
    relations: &BTreeMap<String, BTreeMap<String, CodegenRelation>>,
) -> Result<Json, String> {
    let computed = ComputedNames::new();
    let context = Context {
        schema,
        relations,
        computed: &computed,
    };
    let mut tables = Vec::new();
    for (table, policies) in permissions {
        if *policies == TablePolicies::default() {
            continue;
        }
        let name = table.as_str();
        let at = |op: &str, e: String| format!("tables.{name}.{op}: {e}");
        let mut ops = Vec::new();
        // Nothing recurses through a policy deeper than the grammar reads.
        let print = |e: &PolicyExpr| {
            if expr::too_deep(e, 0) {
                return Err(expr::unsupported(&format!(
                    "a policy nested more than {} levels deep",
                    expr::MAX_DEPTH
                )));
            }
            policy_json(e, &context, name)
        };
        let one_sided = |op: &str,
                         policy: &crate::tools::public_schema::OperationPolicy,
                         usual_using: bool,
                         ops: &mut Vec<(String, Json)>|
         -> Result<(), String> {
            match (&policy.using, &policy.with_check) {
                (None, None) => {}
                (Some(e), None) if usual_using => {
                    ops.push((op.to_string(), print(e).map_err(|e| at(op, e))?))
                }
                (None, Some(e)) if !usual_using => {
                    ops.push((op.to_string(), print(e).map_err(|e| at(op, e))?))
                }
                (using, check) => {
                    let mut sides = Vec::new();
                    if let Some(e) = using {
                        sides.push(("using".to_string(), print(e).map_err(|e| at(op, e))?));
                    }
                    if let Some(e) = check {
                        sides.push(("withCheck".to_string(), print(e).map_err(|e| at(op, e))?));
                    }
                    ops.push((op.to_string(), Json::Object(sides)));
                }
            }
            Ok(())
        };
        one_sided("read", &policies.select, true, &mut ops)?;
        one_sided("insert", &policies.insert, false, &mut ops)?;
        match (&policies.update.using, &policies.update.with_check) {
            (None, None) => {}
            (Some(old), Some(new)) if old == new => {
                ops.push((
                    "update".to_string(),
                    print(old).map_err(|e| at("update", e))?,
                ));
            }
            (old, new) => {
                let mut parts = Vec::new();
                if let Some(old) = old {
                    parts.push((
                        "old".to_string(),
                        print(old).map_err(|e| at("update.old", e))?,
                    ));
                }
                if let Some(new) = new {
                    parts.push((
                        "new".to_string(),
                        print(new).map_err(|e| at("update.new", e))?,
                    ));
                }
                ops.push(("update".to_string(), Json::Object(parts)));
            }
        }
        one_sided("delete", &policies.delete, true, &mut ops)?;
        tables.push((name.to_string(), Json::Object(ops)));
    }
    Ok(Json::Object(vec![("tables".into(), Json::Object(tables))]))
}

/// A policy at the top: `or` and `and` as anyOf / allOf objects, the rest as
/// expression strings.
fn policy_json(expr: &PolicyExpr, context: &Context<'_>, table: &str) -> Result<Json, String> {
    match expr {
        PolicyExpr::Or(items) if items.len() >= 2 => Ok(Json::Object(vec![(
            "anyOf".into(),
            Json::Array(
                items
                    .iter()
                    .map(|i| policy_json(i, context, table))
                    .collect::<Result<_, _>>()?,
            ),
        )])),
        PolicyExpr::And(items) if items.len() >= 2 => Ok(Json::Object(vec![(
            "allOf".into(),
            Json::Array(
                items
                    .iter()
                    .map(|i| policy_json(i, context, table))
                    .collect::<Result<_, _>>()?,
            ),
        )])),
        other => leaf_json(other, context, table),
    }
}

/// A leaf as the expression string that compiles back to exactly `expr`.
fn leaf_json(expr: &PolicyExpr, context: &Context<'_>, table: &str) -> Result<Json, String> {
    expr::print(expr, context, table).map(Json::String)
}
