//! Queries that read other rows than their table's own, lowered through
//! jazz-app's untyped builder (`jazz_app::untyped`), the one the typed Rust
//! API wraps: `hopTo` (with `after`), `gather`, `union`, `history` and
//! `includeDeleted`.
//!
//! - `{ "table", "where"?, "orderBy"?, "limit"?, "hopTo": "<rel>" | [...],
//!   "after"?: { "where", "orderBy", "limit", "offset", "select" } }`: rows of
//!   the last hop's table; the top-level clauses filter the rows hopped from.
//! - `{ "table", "where"?, "gather": { "relation", "maxDepth"? } }`: the seed
//!   rows and every row reached along a self relation (default depth 10).
//! - `{ "union": [Q, ...], "orderBy"?, "limit"?, "offset"? }`: every arm's
//!   rows, a row once per arm that matches it, as TS's union (arms read one
//!   table, with `where` only).
//! - `{ "table", "history": true, "where"?, "orderBy"?, "limit"? }`: the
//!   table's versions, each row with `"$history": { "transactionId",
//!   "acceptedAt" }`.
//! - `"includeDeleted": true` on any of them reads deleted rows too. Rows
//!   carry no deleted marker yet, and in a subscription a row's deletion
//!   arrives as an `updated` row.

use jazz_app::jazz::query::{self as q, OrderDirection};
use jazz_app::jazz::tools::{QueryResult, Schema, TableSchema};
use jazz_app::untyped::{self, GatherStep, Hop};
use serde_json::{Map, Value as Json, json};

use crate::convert::{self, Relations, Shape};
use crate::error::{Result, refused, unsupported};

/// What a relational query reads, and how its rows come back.
pub(crate) struct Lowered {
    pub query: q::Query,
    pub reads_deleted: bool,
    pub shape: Shape,
    pub history: bool,
}

/// Whether `query_json` is one of these queries.
/// `query_json` reading at most `limit` rows: on a hop's target rows
/// (`after`), else on the query's own.
pub(crate) fn with_limit(query_json: &Json, limit: usize) -> Json {
    let mut query = query_json.clone();
    let narrow = |object: &mut Map<String, Json>| {
        let current = object
            .get("limit")
            .and_then(Json::as_u64)
            .map(|l| l as usize);
        object.insert(
            "limit".into(),
            json!(current.map_or(limit, |l| l.min(limit))),
        );
    };
    if let Json::Object(top) = &mut query {
        if top.get("hopTo").is_some_and(|h| !h.is_null()) {
            let after = top.entry("after").or_insert_with(|| json!({}));
            if let Json::Object(after) = after {
                narrow(after);
            }
        } else {
            narrow(top);
        }
    }
    query
}

pub(crate) fn is_relational(query_json: &Json) -> bool {
    // `history` and `includeDeleted` must be booleans; anything but `false`
    // goes here, where a non-boolean is refused by name.
    ["hopTo", "gather", "union", "history", "includeDeleted"]
        .iter()
        .any(|key| {
            query_json
                .get(key)
                .is_some_and(|v| !v.is_null() && *v != json!(false))
        })
}

fn table_of(schema: &Schema, name: &str) -> Result<(TableSchema, untyped::Table)> {
    let table = convert::table(schema, name)?.clone();
    let columns: Vec<String> = table
        .columns
        .columns
        .iter()
        .map(|c| c.name.as_str().to_owned())
        .collect();
    Ok((table, untyped::Table::new(name, columns)))
}

fn object<'a>(json: &'a Json, what: &str) -> Result<&'a Map<String, Json>> {
    json.as_object()
        .ok_or_else(|| refused(format!("{what} must be a JSON object")))
}

fn only(object: &Map<String, Json>, keys: &[&str], what: &str) -> Result<()> {
    match object.keys().find(|key| !keys.contains(&key.as_str())) {
        Some(key) => Err(refused(format!("{what} has an unknown key {key:?}"))),
        None => Ok(()),
    }
}

/// `where`, `orderBy`, `limit`, `offset` and `select` of `object`, on
/// `query`, as rows of `table`.
fn clauses(
    mut query: untyped::Query,
    table: &TableSchema,
    name: &str,
    object: &Map<String, Json>,
    path: &str,
) -> Result<untyped::Query> {
    if let Some(conditions) = object.get("where").filter(|w| !w.is_null()) {
        let conditions = conditions
            .as_array()
            .ok_or_else(|| refused(format!("{path}where must be an array")))?;
        for (at, condition) in conditions.iter().enumerate() {
            query = query.filter(convert::predicate(
                table,
                name,
                condition,
                &format!("{path}where[{at}]"),
            )?);
        }
    }
    if let Some(order) = object.get("orderBy").filter(|o| !o.is_null()) {
        for (at, entry) in order
            .as_array()
            .ok_or_else(|| refused(format!("{path}orderBy must be an array")))?
            .iter()
            .enumerate()
        {
            let column = entry
                .get("column")
                .and_then(Json::as_str)
                .ok_or_else(|| refused(format!("{path}orderBy[{at}] needs a \"column\"")))?;
            let direction = match entry.get("direction").and_then(Json::as_str) {
                None | Some("asc") => OrderDirection::Asc,
                Some("desc") => OrderDirection::Desc,
                Some(other) => {
                    return Err(refused(format!(
                        "{path}orderBy[{at}].direction must be \"asc\" or \"desc\", not {other:?}"
                    )));
                }
            };
            query = if column == "$acceptedAt" {
                query.order_by_accepted_at(direction)
            } else {
                convert::column(table, name, column)?;
                query.order_by(column, direction)
            };
        }
    }
    let count = |key: &str| -> Result<Option<usize>> {
        match object.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(n) => n
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .map(Some)
                .ok_or_else(|| refused(format!("{path}{key} must be a non-negative integer"))),
        }
    };
    if let Some(limit) = count("limit")? {
        query = query.limit(limit);
    }
    if let Some(offset) = count("offset")? {
        query = query.offset(offset);
    }
    if let Some(select) = object.get("select").filter(|s| !s.is_null()) {
        let names = select
            .as_array()
            .ok_or_else(|| refused(format!("{path}select must be an array of column names")))?
            .iter()
            .map(|n| {
                n.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| refused(format!("{path}select takes column names here")))
            })
            .collect::<Result<Vec<_>>>()?;
        if !names.iter().any(|n| n == "*") {
            for name_ in &names {
                if name_ != "id" {
                    convert::column(table, name, name_)?;
                }
            }
            query = query.select(names.into_iter().filter(|n| n != "id"));
        }
    }
    Ok(query)
}

pub(crate) fn lower(schema: &Schema, relations: &Relations, json: &Json) -> Result<Lowered> {
    let top = object(json, "the query")?;
    let include_deleted = match top.get("includeDeleted") {
        None | Some(Json::Null) | Some(Json::Bool(false)) => false,
        Some(Json::Bool(true)) => true,
        Some(_) => return Err(refused("\"includeDeleted\" must be true or false")),
    };

    // A union of arms of one table.
    if let Some(arms) = top.get("union").filter(|u| !u.is_null()) {
        only(
            top,
            &["union", "orderBy", "limit", "offset", "includeDeleted"],
            "a union",
        )?;
        let arms = arms
            .as_array()
            .ok_or_else(|| refused("\"union\" must be an array of queries"))?;
        let first = arms
            .first()
            .and_then(|arm| arm.get("table"))
            .and_then(Json::as_str)
            .ok_or_else(|| refused("a union needs arms, each with a \"table\""))?;
        let (table, untyped_table) = table_of(schema, first)?;
        let mut lowered = Vec::with_capacity(arms.len());
        for (at, arm) in arms.iter().enumerate() {
            let arm = object(arm, &format!("union[{at}]"))?;
            only(arm, &["table", "where"], &format!("union[{at}]"))?;
            if arm.get("table").and_then(Json::as_str) != Some(first) {
                return Err(refused(format!(
                    "union[{at}] must read table {first}, as every arm does"
                )));
            }
            let query = clauses(
                untyped::Query::table(untyped_table.clone()),
                &table,
                first,
                arm,
                &format!("union[{at}]."),
            )?;
            lowered.push((format!("arm{at}"), query));
        }
        let mut query = untyped::Query::union(untyped_table, lowered);
        query = clauses(query, &table, first, top, "")?;
        if include_deleted {
            query = query.include_deleted();
        }
        return finish(query, Shape::of(table), false);
    }

    let name = top
        .get("table")
        .and_then(Json::as_str)
        .ok_or_else(|| refused("the query needs a \"table\""))?;
    let (table, untyped_table) = table_of(schema, name)?;

    match top.get("history") {
        None | Some(Json::Null) | Some(Json::Bool(_)) => {}
        Some(_) => return Err(refused("\"history\" must be true or false")),
    }
    if top.get("history").is_some_and(|h| *h == json!(true)) {
        only(
            top,
            &[
                "table",
                "history",
                "where",
                "orderBy",
                "limit",
                "offset",
                "includeDeleted",
            ],
            "a history query",
        )?;
        let mut query = clauses(
            untyped::Query::history(untyped_table),
            &table,
            name,
            top,
            "",
        )?;
        if include_deleted {
            query = query.include_deleted();
        }
        return finish(query, Shape::of(table), true);
    }

    if let Some(gather) = top.get("gather").filter(|g| !g.is_null()) {
        only(
            top,
            &[
                "table",
                "gather",
                "where",
                "orderBy",
                "limit",
                "offset",
                "select",
                "includeDeleted",
            ],
            "a gather query",
        )?;
        let gather = object(gather, "\"gather\"")?;
        only(gather, &["relation", "maxDepth"], "\"gather\"")?;
        let relation_name = gather
            .get("relation")
            .and_then(Json::as_str)
            .ok_or_else(|| refused("\"gather\" needs a \"relation\""))?;
        let relation = relations
            .get(name, relation_name)
            .ok_or_else(|| refused(format!("table {name} has no relation {relation_name}")))?;
        if !relation.forward || relation.to_table != name {
            return Err(unsupported(format!(
                "gathering along {relation_name}: gather follows a forward relation of {name} to itself"
            )));
        }
        let depth = match gather.get("maxDepth") {
            None | Some(Json::Null) => 10,
            Some(n) => n
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| refused("\"gather.maxDepth\" must be a non-negative integer"))?,
        };
        // The seed rows' filters, then the gather; order, window and select
        // apply to what it reached.
        let mut seed = Map::new();
        if let Some(conditions) = top.get("where") {
            seed.insert("where".into(), conditions.clone());
        }
        let mut query = clauses(
            untyped::Query::table(untyped_table),
            &table,
            name,
            &seed,
            "",
        )?;
        query = query.gather(
            GatherStep::Along {
                column: relation.column.clone(),
                filters: Vec::new(),
            },
            depth,
        );
        let mut rest = top.clone();
        rest.remove("where");
        query = clauses(query, &table, name, &rest, "")?;
        if include_deleted {
            query = query.include_deleted();
        }
        return finish(query, Shape::of(table), false);
    }

    if let Some(hops) = top.get("hopTo").filter(|h| !h.is_null()) {
        only(
            top,
            &[
                "table",
                "hopTo",
                "after",
                "where",
                "orderBy",
                "limit",
                "offset",
                "includeDeleted",
            ],
            "a hopTo query",
        )?;
        let hops: Vec<&str> = match hops {
            Json::String(one) => vec![one.as_str()],
            Json::Array(many) => many
                .iter()
                .map(|h| {
                    h.as_str()
                        .ok_or_else(|| refused("\"hopTo\" takes relation names"))
                })
                .collect::<Result<_>>()?,
            _ => {
                return Err(refused(
                    "\"hopTo\" must be a relation name or an array of them",
                ));
            }
        };
        if hops.is_empty() {
            return Err(refused("\"hopTo\" needs a relation"));
        }
        let mut query = clauses(untyped::Query::table(untyped_table), &table, name, top, "")?;
        let mut at_table = name.to_owned();
        for hop in hops {
            let relation = relations
                .get(&at_table, hop)
                .ok_or_else(|| refused(format!("table {at_table} has no relation {hop}")))?;
            let (_, to) = table_of(schema, &relation.to_table)?;
            let (from_table, _) = table_of(schema, &at_table)?;
            let many = relation.forward
                && convert::column(&from_table, &at_table, &relation.column).is_ok_and(|c| {
                    matches!(
                        c.column_type.as_ref(),
                        jazz_app::jazz::tools::ColumnType::Array { .. }
                    )
                });
            query = query.hop_to(if many {
                // Over a `uuid[]` reference: to every row it names.
                Hop::ForwardArray {
                    column: relation.column.clone(),
                    to,
                }
            } else if relation.forward {
                Hop::Forward {
                    column: relation.column.clone(),
                    to,
                }
            } else {
                Hop::Reverse {
                    column: relation.column.clone(),
                    to,
                }
            });
            at_table = relation.to_table.clone();
        }
        let (target, _) = table_of(schema, &at_table)?;
        if let Some(after) = top.get("after").filter(|a| !a.is_null()) {
            let after = object(after, "\"after\"")?;
            only(
                after,
                &["where", "orderBy", "limit", "offset", "select"],
                "\"after\"",
            )?;
            query = clauses(query, &target, &at_table, after, "after.")?;
        }
        if include_deleted {
            query = query.include_deleted();
        }
        return finish(query, Shape::of(target), false);
    }

    // Only `includeDeleted` on an ordinary query.
    let mut plain = top.clone();
    plain.remove("includeDeleted");
    only(
        &plain,
        &["table", "where", "orderBy", "limit", "offset", "select"],
        "an includeDeleted query (includes and parts with includeDeleted are not supported yet)",
    )?;
    let mut query = clauses(
        untyped::Query::table(untyped_table),
        &table,
        name,
        &plain,
        "",
    )?;
    query = query.include_deleted();
    finish(query, Shape::of(table), false)
}

fn finish(query: untyped::Query, shape: Shape, history: bool) -> Result<Lowered> {
    let reads_deleted = query.reads_deleted();
    let query = query.into_core()?;
    Ok(Lowered {
        query,
        reads_deleted,
        shape,
        history,
    })
}

/// A result row as JSON: a history row also gets its `$history`.
pub(crate) fn row(lowered: &Lowered, result: QueryResult) -> Result<Json> {
    let id = result
        .key
        .row_id()
        .ok_or_else(|| crate::error::FfiError::Io("a joined result is not a row".into()))?;
    if !lowered.history {
        return Ok(convert::row_json(&lowered.shape, id, result.fields));
    }
    let mut history = Map::new();
    let mut fields = Vec::with_capacity(result.fields.len());
    for field in result.fields {
        match field.name.as_str() {
            "$transactionId" => {
                if let Some(tx) = untyped::transaction_id(&field.value) {
                    history.insert("transactionId".into(), json!(tx.to_string()));
                }
            }
            "$acceptedAt" => {
                if let Some(at) = untyped::accepted_at(&field.value) {
                    history.insert("acceptedAt".into(), json!(at.0));
                }
            }
            _ => fields.push(field),
        }
    }
    let mut row = convert::row_json(&lowered.shape, id, fields);
    if let Json::Object(object) = &mut row {
        object.insert("$history".into(), Json::Object(history));
    }
    Ok(row)
}
