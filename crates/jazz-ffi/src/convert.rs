//! Plain JSON to and from engine values, typed by the app's schema, and the
//! query, row and delta JSON of the FFI contract.
//!
//! Values: string/uuid/enum → JSON string; int/float → number; bigint →
//! decimal string; boolean; timestamp → number (ms since the epoch); json →
//! any JSON; bytes → base64 string; array → JSON array; optional → null.

use base64::Engine as _;
use jazz_app::jazz::query::{self as q, OrderDirection, Predicate};
use jazz_app::jazz::records::Value as CoreValue;
use jazz_app::jazz::tools::{
    ColumnType, ObjectId, OrderedRowDelta, QueryResultField, Schema, TableName, TableSchema, Value,
};
use serde_json::{Map, Number, Value as Json, json};

use crate::error::{FfiError, Result, refused, unsupported};

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// A column's type as a query or write sees it: user columns from the
/// schema, plus `id`, `$createdAt` and `$updatedAt` on every table.
pub(crate) struct ColumnInfo<'a> {
    pub name: &'a str,
    pub column_type: std::borrow::Cow<'a, ColumnType>,
    pub nullable: bool,
}

pub(crate) fn table<'a>(schema: &'a Schema, name: &str) -> Result<&'a TableSchema> {
    schema
        .get(&TableName::new(name))
        .ok_or_else(|| refused(format!("the app has no table {name}")))
}

pub(crate) fn column<'a>(
    table: &'a TableSchema,
    table_name: &str,
    name: &'a str,
) -> Result<ColumnInfo<'a>> {
    let builtin = |column_type| ColumnInfo {
        name,
        column_type: std::borrow::Cow::Owned(column_type),
        nullable: false,
    };
    match name {
        "id" => return Ok(builtin(ColumnType::Uuid)),
        "$createdAt" | "$updatedAt" => return Ok(builtin(ColumnType::Timestamp)),
        _ => {}
    }
    table
        .columns
        .columns
        .iter()
        .find(|column| column.name.as_str() == name)
        .map(|column| ColumnInfo {
            name,
            column_type: std::borrow::Cow::Borrowed(&column.column_type),
            nullable: column.nullable,
        })
        .ok_or_else(|| refused(format!("table {table_name} has no column {name}")))
}

/// A JSON value for a column of `column_type`.
pub(crate) fn to_value(
    json: &Json,
    column_type: &ColumnType,
    nullable: bool,
    path: &str,
) -> Result<Value> {
    if json.is_null() {
        return if nullable {
            Ok(Value::Null)
        } else {
            Err(refused(format!("{path} is required and cannot be null")))
        };
    }
    let wrong = |expected: &str| refused(format!("{path} must be {expected}, not {json}"));
    Ok(match column_type {
        ColumnType::Text => Value::Text(json.as_str().ok_or_else(|| wrong("a string"))?.to_owned()),
        ColumnType::Enum { variants } | ColumnType::ScalarEnum { variants, .. } => {
            let text = json.as_str().ok_or_else(|| wrong("a string"))?;
            if !variants.iter().any(|variant| variant == text) {
                return Err(refused(format!(
                    "{path} must be one of {}, not {json}",
                    variants.join(", ")
                )));
            }
            Value::Text(text.to_owned())
        }
        // The engine's client can't write them yet (its public values have
        // no payload enum).
        ColumnType::EnumPayload { .. } | ColumnType::CatalogueEnumPayload { .. } => {
            return Err(unsupported(format!(
                "writing an enum with payloads ({path})"
            )));
        }
        ColumnType::Integer => {
            let int = json.as_i64().ok_or_else(|| wrong("an integer"))?;
            Value::Integer(i32::try_from(int).map_err(|_| wrong("a 32-bit integer"))?)
        }
        ColumnType::BigInt => Value::BigInt(match json {
            Json::String(text) => text
                .parse()
                .map_err(|_| wrong("a decimal string of a 64-bit integer"))?,
            Json::Number(number) => number.as_i64().ok_or_else(|| wrong("a 64-bit integer"))?,
            _ => return Err(wrong("a decimal string")),
        }),
        ColumnType::Double => Value::Double(json.as_f64().ok_or_else(|| wrong("a number"))?),
        ColumnType::Boolean => Value::Boolean(json.as_bool().ok_or_else(|| wrong("a boolean"))?),
        ColumnType::Timestamp => Value::Timestamp(
            json.as_u64()
                .ok_or_else(|| wrong("milliseconds since the epoch, a non-negative integer"))?,
        ),
        ColumnType::Uuid => Value::Uuid(parse_id(
            json.as_str().ok_or_else(|| wrong("a UUID string"))?,
            path,
        )?),
        ColumnType::Bytea => Value::Bytea(
            BASE64
                .decode(json.as_str().ok_or_else(|| wrong("a base64 string"))?)
                .map_err(|_| wrong("a base64 string"))?,
        ),
        ColumnType::Json { .. } => Value::Text(json.to_string()),
        ColumnType::Array { element } => Value::Array(
            json.as_array()
                .ok_or_else(|| wrong("an array"))?
                .iter()
                .enumerate()
                .map(|(at, item)| to_value(item, element, false, &format!("{path}[{at}]")))
                .collect::<Result<_>>()?,
        ),
        ColumnType::TransactionId | ColumnType::Row { .. } => {
            return Err(unsupported(format!(
                "writing a column of type {column_type:?} ({path})"
            )));
        }
    })
}

pub(crate) fn parse_id(text: &str, path: &str) -> Result<ObjectId> {
    uuid::Uuid::parse_str(text)
        .map(ObjectId::from_uuid)
        .map_err(|_| refused(format!("{path} must be a UUID, not {text:?}")))
}

/// An engine value as plain JSON; `column_type` says how to read text
/// (json columns hold JSON text) and payload enums.
pub(crate) fn to_json(value: Value, column_type: Option<&ColumnType>) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Integer(int) => json!(int),
        Value::BigInt(int) => Json::String(int.to_string()),
        Value::Double(double) => Number::from_f64(double).map_or(Json::Null, Json::Number),
        Value::Boolean(bool) => Json::Bool(bool),
        Value::Text(text) => match column_type {
            Some(ColumnType::Json { .. }) => {
                serde_json::from_str(&text).unwrap_or(Json::String(text))
            }
            _ => Json::String(text),
        },
        Value::Timestamp(millis) => json!(millis),
        Value::Uuid(id) => Json::String(id.to_string()),
        Value::TransactionId(bytes) => {
            Json::String(bytes.iter().map(|b| format!("{b:02x}")).collect())
        }
        Value::Bytea(bytes) => Json::String(BASE64.encode(bytes)),
        Value::Array(values) => {
            let element = column_type.and_then(ColumnType::element_type);
            Json::Array(
                values
                    .into_iter()
                    .map(|value| to_json(value, element))
                    .collect(),
            )
        }
        // A payload enum reads as `{ "case": …, "fields": { … } }`.
        Value::Enum { case, values } => {
            let fields = match column_type {
                Some(
                    ColumnType::EnumPayload { cases }
                    | ColumnType::CatalogueEnumPayload { cases, .. },
                ) => cases
                    .iter()
                    .find(|c| c.name == case)
                    .map(|c| c.fields.as_slice()),
                _ => None,
            };
            let mut object = Map::new();
            for (at, value) in values.into_iter().enumerate() {
                let field = fields.and_then(|fields| fields.get(at));
                let name = field.map_or_else(|| at.to_string(), |f| f.name.as_str().to_owned());
                object.insert(name, to_json(value, field.map(|f| &f.column_type)));
            }
            json!({ "case": case, "fields": object })
        }
        Value::Row { values, .. } => Json::Array(
            values
                .into_iter()
                .map(|value| to_json(value, None))
                .collect(),
        ),
    }
}

/// What a query's rows hold: its table's columns and, nested under their
/// relation names, the rows of its includes.
#[derive(Debug, Clone)]
pub(crate) struct Shape {
    pub table: TableSchema,
    pub includes: Vec<IncludeShape>,
}

#[derive(Debug, Clone)]
pub(crate) struct IncludeShape {
    /// The relation's name: the key of its rows in the parent row.
    pub name: String,
    /// A forward relation holds one row or `null`; a reverse one an array.
    pub forward: bool,
    /// The included rows' values come in this order, then their includes'.
    pub columns: Vec<(String, ColumnType)>,
    pub shape: Shape,
}

impl Shape {
    pub(crate) fn of(table: TableSchema) -> Self {
        Self {
            table,
            includes: Vec::new(),
        }
    }
}

/// A row as `{ "id": …, "<column>": …, "<relation>": … }`.
pub(crate) fn row_json(shape: &Shape, id: ObjectId, fields: Vec<QueryResultField>) -> Json {
    let mut row = Map::new();
    row.insert("id".into(), Json::String(id.to_string()));
    for field in fields {
        if let Some(include) = shape.includes.iter().find(|i| i.name == field.name) {
            row.insert(field.name, included_json(include, field.value));
        } else if let Some(column) = shape
            .table
            .columns
            .columns
            .iter()
            .find(|column| column.name.as_str() == field.name)
        {
            row.insert(field.name, to_json(field.value, Some(&column.column_type)));
        }
    }
    Json::Object(row)
}

fn included_json(include: &IncludeShape, value: Value) -> Json {
    let rows: Vec<Json> = match value {
        Value::Array(rows) => rows
            .into_iter()
            .filter_map(|row| match row {
                Value::Row { id, values } => Some(included_row(include, id, values)),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    if include.forward {
        rows.into_iter().next().unwrap_or(Json::Null)
    } else {
        Json::Array(rows)
    }
}

fn included_row(include: &IncludeShape, id: Option<ObjectId>, values: Vec<Value>) -> Json {
    let mut row = Map::new();
    if let Some(id) = id {
        row.insert("id".into(), Json::String(id.to_string()));
    }
    let mut values = values.into_iter();
    for (name, column_type) in &include.columns {
        let value = values.next().unwrap_or(Value::Null);
        row.insert(name.clone(), to_json(value, Some(column_type)));
    }
    // The engine appends a row's nested relations in name order.
    let mut nested: Vec<&IncludeShape> = include.shape.includes.iter().collect();
    nested.sort_by(|a, b| a.name.cmp(&b.name));
    for nested in nested {
        let value = values.next().unwrap_or(Value::Null);
        row.insert(nested.name.clone(), included_json(nested, value));
    }
    Json::Object(row)
}

/// A table's relations as `jazz.app.json` declares them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Relations(std::collections::HashMap<String, Vec<Relation>>);

#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub name: String,
    pub to_table: String,
    /// Forward: this table's column referencing the target's id. Reverse:
    /// the target table's column referencing this table's id.
    pub column: String,
    pub forward: bool,
}

impl Relations {
    /// From the text of `jazz.app.json` (already checked by the loader).
    pub(crate) fn read(app: &str) -> Self {
        let Ok(app) = serde_json::from_str::<Json>(app) else {
            return Self::default();
        };
        let tables = app.get("tables").and_then(Json::as_object);
        let forward = |table: &str, name: &str| -> Option<(String, String)> {
            let relation = tables?.get(table)?.get("relations")?.get(name)?;
            Some((
                relation.get("rel")?.as_str()?.to_owned(),
                relation.get("column")?.as_str()?.to_owned(),
            ))
        };
        let mut relations = std::collections::HashMap::new();
        for (table, spec) in tables.into_iter().flatten() {
            let mut list = Vec::new();
            for (name, relation) in spec
                .get("relations")
                .and_then(Json::as_object)
                .into_iter()
                .flatten()
            {
                if let (Some(to_table), Some(column)) = (
                    relation.get("rel").and_then(Json::as_str),
                    relation.get("column").and_then(Json::as_str),
                ) {
                    list.push(Relation {
                        name: name.clone(),
                        to_table: to_table.to_owned(),
                        column: column.to_owned(),
                        forward: true,
                    });
                } else if let (Some(to_table), Some(via)) = (
                    relation.get("reverse").and_then(Json::as_str),
                    relation.get("relation").and_then(Json::as_str),
                ) && let Some((target, column)) = forward(to_table, via)
                    // A reverse relation reverses a forward one pointing here.
                    && target == *table
                {
                    list.push(Relation {
                        name: name.clone(),
                        to_table: to_table.to_owned(),
                        column,
                        forward: false,
                    });
                }
            }
            relations.insert(table.clone(), list);
        }
        Self(relations)
    }

    fn get(&self, table: &str, name: &str) -> Option<&Relation> {
        self.0
            .get(table)?
            .iter()
            .find(|relation| relation.name == name)
    }
}

/// A row's given id, if any, and its cells by column name.
pub(crate) type Cells = (Option<ObjectId>, Vec<(String, Value)>);

/// The engine values of a row JSON object, for an insert (`required`) or a
/// patch.
pub(crate) fn cells(table: &TableSchema, table_name: &str, row: &str, what: &str) -> Result<Cells> {
    let Json::Object(object) = parse(row, what)? else {
        return Err(refused(format!("{what} must be a JSON object")));
    };
    let mut id = None;
    let mut cells = Vec::with_capacity(object.len());
    for (name, json) in object {
        if name == "id" {
            id = Some(parse_id(
                json.as_str().unwrap_or_default(),
                &format!("{what}.id"),
            )?);
            continue;
        }
        let column = column(table, table_name, &name)?;
        if name.starts_with('$') {
            return Err(refused(format!("{name} is set by Jazz, not written")));
        }
        let value = to_value(
            &json,
            &column.column_type,
            column.nullable,
            &format!("{table_name}.{name}"),
        )?;
        cells.push((name, value));
    }
    Ok((id, cells))
}

pub(crate) fn parse(text: &str, what: &str) -> Result<Json> {
    serde_json::from_str(text)
        .map_err(|error| refused(format!("{what} is not valid JSON: {error}")))
}

/// The query JSON as an engine query, its table's name, and the shape of
/// its rows.
pub(crate) fn query(
    schema: &Schema,
    relations: &Relations,
    query_json: &Json,
) -> Result<(String, q::Query, Shape)> {
    let Json::Object(object) = query_json else {
        return Err(refused("the query must be a JSON object"));
    };
    let table_name = object
        .get("table")
        .and_then(Json::as_str)
        .ok_or_else(|| refused("the query needs a \"table\""))?;
    let mut query = q::Query::from(table_name);
    let mut parts = Parts::default();
    let shape = level(schema, relations, table_name, object, "", true, &mut parts)?;
    query.filters = parts.filters;
    query.order_by = parts.order_by;
    query.limit = parts.limit;
    query.offset = parts.offset;
    query.select = parts.select;
    query.array_subqueries = parts.includes;
    query.partial = parts.partial;
    Ok((table_name.to_owned(), query, shape))
}

/// One level of a query: the root, or an include's child query.
#[derive(Default)]
struct Parts {
    filters: Vec<Predicate>,
    order_by: Vec<q::OrderBy>,
    limit: Option<usize>,
    offset: usize,
    select: Option<Vec<String>>,
    includes: Vec<q::ArraySubquery>,
    partial: Vec<q::PartialSelect>,
}

fn level(
    schema: &Schema,
    relations: &Relations,
    table_name: &str,
    object: &Map<String, Json>,
    path: &str,
    root: bool,
    parts: &mut Parts,
) -> Result<Shape> {
    for key in object.keys() {
        match key.as_str() {
            "where" | "orderBy" | "limit" | "include" | "select" => {}
            "table" | "offset" if root => {}
            "offset"
                if matches!(object.get("offset"), Some(Json::Null))
                    || object.get("offset").and_then(Json::as_u64) == Some(0) => {}
            "offset" => {
                return Err(unsupported(format!(
                    "\"offset\" in an include ({path}); use \"limit\" or query the table"
                )));
            }
            "includes" | "relations" => {
                return Err(refused(format!(
                    "{path}{key}: unknown key; nested related rows are \"include\""
                )));
            }
            other => {
                return Err(refused(format!(
                    "the query has an unknown key {path}{other:?}"
                )));
            }
        }
    }
    let table = table(schema, table_name)?;

    if let Some(conditions) = object.get("where").filter(|w| !w.is_null()) {
        let conditions = conditions
            .as_array()
            .ok_or_else(|| refused(format!("{path}\"where\" must be an array of conditions")))?;
        for (at, condition) in conditions.iter().enumerate() {
            parts.filters.push(predicate(
                table,
                table_name,
                condition,
                &format!("{path}where[{at}]"),
            )?);
        }
    }
    if let Some(order) = object.get("orderBy").filter(|o| !o.is_null()) {
        let order = order
            .as_array()
            .ok_or_else(|| refused(format!("{path}\"orderBy\" must be an array")))?;
        for (at, entry) in order.iter().enumerate() {
            let at = format!("{path}orderBy[{at}]");
            let name = entry
                .get("column")
                .and_then(Json::as_str)
                .ok_or_else(|| refused(format!("{at} needs a \"column\"")))?;
            column(table, table_name, name)?;
            let direction = match entry.get("direction").and_then(Json::as_str) {
                None | Some("asc") => OrderDirection::Asc,
                Some("desc") => OrderDirection::Desc,
                Some(other) => {
                    return Err(refused(format!(
                        "{at}.direction must be \"asc\" or \"desc\", not {other:?}"
                    )));
                }
            };
            parts.order_by.push(q::OrderBy {
                column: name.to_owned(),
                direction,
            });
        }
    }
    let count = |key: &str| -> Result<Option<usize>> {
        match object.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(n) => n
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .map(Some)
                .ok_or_else(|| refused(format!("{path}\"{key}\" must be a non-negative integer"))),
        }
    };
    parts.limit = count("limit")?;
    if let Some(offset) = count("offset")? {
        parts.offset = offset;
    }

    // `select`: these columns (and `id`, always) instead of all of them.
    // `"*"` among them selects every column, as no `select` does. An object
    // reads part of a large value: `{ "column", "from", "to", "units":
    // "bytes"|"utf8"|"utf16" }` or `{ "column", "at": "<JSON pointer>" }`.
    if let Some(select) = object.get("select").filter(|s| !s.is_null()) {
        let select = select
            .as_array()
            .ok_or_else(|| refused(format!("{path}\"select\" must be an array of column names")))?;
        let every = select.iter().any(|n| n == "*");
        let mut chosen: Vec<String> = Vec::new();
        for (at, entry) in select.iter().enumerate() {
            let at = format!("{path}select[{at}]");
            let name = match entry {
                Json::String(name) if name == "*" => continue,
                Json::String(name) => name.as_str(),
                Json::Object(spec) => {
                    if !root {
                        return Err(unsupported(format!(
                            "reading part of a value in an include ({at})"
                        )));
                    }
                    let name = spec
                        .get("column")
                        .and_then(Json::as_str)
                        .ok_or_else(|| refused(format!("{at} needs a \"column\"")))?;
                    let info = column(table, table_name, name)?;
                    let range = |key: &str| -> Result<u64> {
                        spec.get(key).and_then(Json::as_u64).ok_or_else(|| {
                            refused(format!("{at}.{key} must be a non-negative integer"))
                        })
                    };
                    let part = match (spec.get("at"), spec.get("units").and_then(Json::as_str)) {
                        (Some(pointer), _) => {
                            if !matches!(info.column_type.as_ref(), ColumnType::Json { .. }) {
                                return Err(refused(format!(
                                    "{at}: \"at\" reads a json column; {name} isn't one"
                                )));
                            }
                            q::Part::JsonPointer(
                                pointer
                                    .as_str()
                                    .ok_or_else(|| {
                                        refused(format!("{at}.at must be a JSON pointer"))
                                    })?
                                    .to_owned(),
                            )
                        }
                        (None, Some("bytes")) => q::Part::Bytes {
                            from: range("from")?,
                            to: range("to")?,
                        },
                        (None, Some("utf16")) => q::Part::TextUtf16 {
                            from: range("from")?,
                            to: range("to")?,
                        },
                        (None, Some("utf8") | None) => q::Part::TextUtf8 {
                            from: range("from")?,
                            to: range("to")?,
                        },
                        (None, Some(other)) => {
                            return Err(refused(format!(
                                "{at}.units must be \"bytes\", \"utf8\" or \"utf16\", not {other:?}"
                            )));
                        }
                    };
                    parts.partial.push(q::PartialSelect {
                        column: name.to_owned(),
                        part,
                    });
                    name
                }
                _ => return Err(refused(format!("{at} must be a column name or a part"))),
            };
            if name == "id" {
                continue;
            }
            column(table, table_name, name)?;
            if name.starts_with('$') {
                return Err(unsupported(format!("selecting {name} ({at})")));
            }
            if !chosen.iter().any(|n| n == name) {
                chosen.push(name.to_owned());
            }
        }
        if !every {
            parts.select = Some(chosen);
        }
    }

    let mut shape = Shape::of(table.clone());
    if let Some(includes) = object.get("include").filter(|i| !i.is_null()) {
        let includes = includes
            .as_array()
            .ok_or_else(|| refused(format!("{path}\"include\" must be an array")))?;
        for (at, include) in includes.iter().enumerate() {
            let at = format!("{path}include[{at}]");
            let name = include
                .get("relation")
                .and_then(Json::as_str)
                .ok_or_else(|| refused(format!("{at} needs a \"relation\"")))?;
            for key in include.as_object().into_iter().flatten().map(|(k, _)| k) {
                if !matches!(key.as_str(), "relation" | "required" | "query") {
                    return Err(refused(format!("{at} has an unknown key {key:?}")));
                }
            }
            let relation = relations
                .get(table_name, name)
                .ok_or_else(|| refused(format!("table {table_name} has no relation {name}")))?;
            if table
                .columns
                .columns
                .iter()
                .any(|c| c.name.as_str() == name)
            {
                return Err(unsupported(format!(
                    "including relation {name}, which has the name of a column of {table_name}"
                )));
            }
            if shape.includes.iter().any(|i| i.name == name) {
                return Err(refused(format!("{at}: relation {name} is included twice")));
            }
            let required = match include.get("required") {
                None | Some(Json::Null) => false,
                Some(Json::Bool(required)) => *required,
                Some(_) => return Err(refused(format!("{at}.required must be true or false"))),
            };
            let child = match include.get("query") {
                None | Some(Json::Null) => Map::new(),
                Some(Json::Object(child)) => child.clone(),
                Some(_) => return Err(refused(format!("{at}.query must be an object"))),
            };
            let mut child_parts = Parts::default();
            let child_shape = level(
                schema,
                relations,
                &relation.to_table,
                &child,
                &format!("{at}.query."),
                false,
                &mut child_parts,
            )?;
            let mut subquery = if relation.forward {
                q::ArraySubquery::new(
                    name,
                    &relation.to_table,
                    "id",
                    format!("{table_name}.{}", relation.column),
                )
            } else {
                q::ArraySubquery::new(
                    name,
                    &relation.to_table,
                    &relation.column,
                    format!("{table_name}.id"),
                )
            };
            subquery.filters = child_parts.filters;
            subquery.order_by = child_parts.order_by;
            subquery.limit = child_parts.limit;
            subquery.select = child_parts.select.clone();
            subquery.nested_arrays = child_parts.includes;
            // As TS's requireIncludes: a required forward relation through
            // a non-null reference keeps only rows whose target is readable.
            // A forward relation through a `uuid[]` column references many
            // rows: it reads as an array, as in TS.
            let reference = column(table, table_name, &relation.column).ok();
            let many = relation.forward
                && reference
                    .as_ref()
                    .is_some_and(|c| matches!(c.column_type.as_ref(), ColumnType::Array { .. }));
            if required {
                let nullable = reference.as_ref().map(|c| c.nullable).unwrap_or(true);
                if relation.forward && !nullable {
                    subquery.requirement = if many {
                        q::ArraySubqueryRequirement::MatchCorrelationCardinality
                    } else {
                        q::ArraySubqueryRequirement::AtLeastOne
                    };
                } else {
                    return Err(unsupported(format!(
                        "\"required\" on {name}: only forward relations through a non-optional column can be required"
                    )));
                }
            }
            let child_table = convert_table(schema, &relation.to_table)?;
            let child_columns = match &child_parts.select {
                Some(_) => child_shape_columns(&child, child_table)?,
                None => child_table
                    .columns
                    .columns
                    .iter()
                    .map(|c| (c.name.as_str().to_owned(), c.column_type.clone()))
                    .collect(),
            };
            shape.includes.push(IncludeShape {
                name: name.to_owned(),
                forward: relation.forward && !many,
                columns: child_columns,
                shape: child_shape,
            });
            parts.includes.push(subquery);
        }
    }
    Ok(shape)
}

fn convert_table<'a>(schema: &'a Schema, name: &str) -> Result<&'a TableSchema> {
    table(schema, name)
}

/// An include's selected columns, in the order its query names them.
fn child_shape_columns(
    child: &Map<String, Json>,
    table: &TableSchema,
) -> Result<Vec<(String, ColumnType)>> {
    let mut columns: Vec<(String, ColumnType)> = Vec::new();
    for name in child
        .get("select")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(Json::as_str)
    {
        if name == "id" || columns.iter().any(|(n, _)| n == name) {
            continue;
        }
        if let Some(c) = table
            .columns
            .columns
            .iter()
            .find(|c| c.name.as_str() == name)
        {
            columns.push((name.to_owned(), c.column_type.clone()));
        }
    }
    Ok(columns)
}

fn predicate(
    table: &TableSchema,
    table_name: &str,
    condition: &Json,
    path: &str,
) -> Result<Predicate> {
    let name = condition
        .get("column")
        .and_then(Json::as_str)
        .ok_or_else(|| refused(format!("{path} needs a \"column\"")))?;
    let op = condition
        .get("op")
        .and_then(Json::as_str)
        .ok_or_else(|| refused(format!("{path} needs an \"op\"")))?;
    let column = column(table, table_name, name)?;
    if matches!(
        column.column_type.as_ref(),
        ColumnType::EnumPayload { .. } | ColumnType::CatalogueEnumPayload { .. }
    ) && !matches!(op, "isNull" | "isNotNull")
    {
        return Err(unsupported(format!(
            "comparing {name}, an enum with payloads ({path})"
        )));
    }
    let value = condition.get("value");
    let path = format!("{path}.value");
    let literal = || -> Result<q::Operand> {
        let value = value.ok_or_else(|| refused(format!("{path} is missing")))?;
        if value.is_null() {
            return Err(refused(format!(
                "{path} is null: use \"isNull\" or \"isNotNull\""
            )));
        }
        Ok(lit(to_value(value, &column.column_type, false, &path)?))
    };
    let compare = |compare: fn(q::Operand, q::Operand) -> Predicate| -> Result<Predicate> {
        Ok(compare(q::col(column.name), literal()?))
    };
    Ok(match op {
        "eq" => compare(q::eq)?,
        "ne" => compare(q::ne)?,
        "lt" => compare(q::lt)?,
        "lte" => compare(q::lte)?,
        "gt" => compare(q::gt)?,
        "gte" => compare(q::gte)?,
        "in" => {
            let values = value
                .and_then(Json::as_array)
                .ok_or_else(|| refused(format!("{path} must be an array for \"in\"")))?;
            let operands = values
                .iter()
                .enumerate()
                .map(|(at, value)| {
                    Ok(lit(to_value(
                        value,
                        &column.column_type,
                        false,
                        &format!("{path}[{at}]"),
                    )?))
                })
                .collect::<Result<Vec<_>>>()?;
            q::in_list(q::col(column.name), operands)
        }
        "contains" => {
            let value = value.ok_or_else(|| refused(format!("{path} is missing")))?;
            let part = match column.column_type.as_ref() {
                ColumnType::Text => to_value(value, &ColumnType::Text, false, &path)?,
                ColumnType::Array { element } => to_value(value, element, false, &path)?,
                _ => {
                    return Err(refused(format!(
                        "\"contains\" needs a string or array column; {name} is neither"
                    )));
                }
            };
            q::contains(q::col(column.name), lit(part))
        }
        "isNull" => q::is_null(q::col(column.name)),
        "isNotNull" => q::not(q::is_null(q::col(column.name))),
        other => return Err(refused(format!("{path}: unknown op {other:?}"))),
    })
}

fn lit(value: Value) -> q::Operand {
    q::lit(core(value))
}

/// As jazz-app turns column values into query literals.
fn core(value: Value) -> CoreValue {
    match value {
        Value::Boolean(value) => CoreValue::Bool(value),
        Value::Text(value) => CoreValue::String(value),
        Value::Integer(value) => CoreValue::I32(value),
        Value::BigInt(value) => CoreValue::I64(value),
        Value::Double(value) => CoreValue::F64(value),
        Value::Timestamp(value) => CoreValue::U64(value),
        Value::Uuid(value) => CoreValue::Uuid(*value.uuid()),
        Value::Bytea(value) => CoreValue::Bytes(value),
        Value::Array(values) => CoreValue::Array(values.into_iter().map(core).collect()),
        // `to_value` produces scalars and arrays of them only.
        _ => CoreValue::Nullable(None),
    }
}

/// An engine delta as delta JSON. `reset` marks the first delta (the full
/// result). Rows that only moved carry no new value and are left out: the
/// contract's deltas carry no positions.
pub(crate) fn delta_json(table: &Shape, delta: OrderedRowDelta, reset: bool) -> Result<String> {
    let id = |key: &jazz_app::jazz::tools::ResultKey| {
        key.row_id()
            .ok_or_else(|| FfiError::Io("a joined result is not a row".into()))
    };
    let mut added = Vec::with_capacity(delta.added.len());
    for entry in delta.added {
        added.push(row_json(table, id(&entry.id)?, entry.row.fields));
    }
    let mut updated = Vec::new();
    for entry in delta.updated {
        if let Some(row) = entry.row {
            updated.push(row_json(table, id(&entry.id)?, row.fields));
        }
    }
    let mut removed = Vec::with_capacity(delta.removed.len());
    for entry in delta.removed {
        removed.push(Json::String(id(&entry.id)?.to_string()));
    }
    Ok(json!({
        "added": added,
        "updated": updated,
        "removed": removed,
        "reset": reset,
        // Becomes true for a reset that replaces a backlog the reader fell
        // behind on, once the engine coalesces backlogs.
        "coalesced": false,
    })
    .to_string())
}

/// The `edits` of an `edit` call as the engine's partial updates:
/// `{ "kind": "text", "column", "text", "editedFrom" }` (a text-merge
/// column's new value, made from the one its writer last saw),
/// `{ "kind": "splice", "column", "within": { "units": "utf8"|"utf16"|"bytes",
/// "from", "to" }, "splices": [{ "at", "delete", "insert" }] }` (`insert` a
/// string, or base64 for bytes) and `{ "kind": "jsonSet", "column",
/// "edits": [{ "at": "<json pointer>", "value" }] }`.
/// A splice page's `to` that stands for the current value's end.
pub(crate) const WHOLE_VALUE: u64 = u64::MAX;

pub(crate) fn edits(
    table: &TableSchema,
    table_name: &str,
    edits: &Json,
) -> Result<Vec<jazz_app::jazz::db::LargeValueUpdate>> {
    use jazz_app::jazz::db::{
        JsonSetEdit, LargeValueUpdate, LargeValueUpdatePage, LargeValueUpdateSplice,
    };
    let edits = edits
        .as_array()
        .ok_or_else(|| refused("\"edits\" must be an array"))?;
    edits
        .iter()
        .enumerate()
        .map(|(at, edit)| {
            let path = format!("edits[{at}]");
            let text = |key: &str| -> Result<String> {
                edit.get(key)
                    .and_then(Json::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| refused(format!("{path} needs \"{key}\" (a string)")))
            };
            let number = |value: Option<&Json>, key: &str| -> Result<u64> {
                value.and_then(Json::as_u64).ok_or_else(|| {
                    refused(format!("{path}: \"{key}\" must be a non-negative integer"))
                })
            };
            let column_name = text("column")?;
            let info = column(table, table_name, &column_name)?;
            if column_name.starts_with('$') || column_name == "id" {
                return Err(refused(format!("{path}: {column_name} is set by Jazz")));
            }
            let kind = edit.get("kind").and_then(Json::as_str).unwrap_or_default();
            Ok(match kind {
                "text" => {
                    if !matches!(info.column_type.as_ref(), ColumnType::Text) {
                        return Err(refused(format!(
                            "{path}: {column_name} is not a string column"
                        )));
                    }
                    LargeValueUpdate::TextEdit {
                        column: column_name,
                        text: text("text")?,
                        edited_from: text("editedFrom")?,
                    }
                }
                "splice" => {
                    // Without `within`, the page is the whole current value
                    // (resolved when the edit runs), in `units`.
                    let whole =
                        json!({ "units": edit.get("units").cloned().unwrap_or(json!("utf8")) });
                    let within = edit.get("within").unwrap_or(&whole);
                    let (from, to) = if edit.get("within").is_some() {
                        (
                            number(within.get("from"), "within.from")?,
                            number(within.get("to"), "within.to")?,
                        )
                    } else {
                        (0, WHOLE_VALUE)
                    };
                    let bytes = match within.get("units").and_then(Json::as_str) {
                        Some("bytes") => true,
                        Some("utf8" | "utf16") => false,
                        _ => {
                            return Err(refused(format!(
                                "{path}.within.units must be \"utf8\", \"utf16\" or \"bytes\""
                            )));
                        }
                    };
                    let within = match within.get("units").and_then(Json::as_str) {
                        Some("bytes") => LargeValueUpdatePage::Bytes { from, to },
                        Some("utf16") => LargeValueUpdatePage::TextUtf16 { from, to },
                        _ => LargeValueUpdatePage::TextUtf8 { from, to },
                    };
                    let splices = edit
                        .get("splices")
                        .and_then(Json::as_array)
                        .ok_or_else(|| refused(format!("{path} needs \"splices\" (an array)")))?
                        .iter()
                        .enumerate()
                        .map(|(i, splice)| {
                            let insert = match splice.get("insert") {
                                None | Some(Json::Null) => Vec::new(),
                                Some(Json::String(insert)) if bytes => {
                                    BASE64.decode(insert).map_err(|_| {
                                        refused(format!(
                                            "{path}.splices[{i}].insert must be base64"
                                        ))
                                    })?
                                }
                                Some(Json::String(insert)) => insert.clone().into_bytes(),
                                Some(_) => {
                                    return Err(refused(format!(
                                        "{path}.splices[{i}].insert must be a string"
                                    )));
                                }
                            };
                            Ok(LargeValueUpdateSplice {
                                at: number(splice.get("at"), &format!("splices[{i}].at"))?,
                                delete: splice.get("delete").and_then(Json::as_u64).unwrap_or(0),
                                insert,
                            })
                        })
                        .collect::<Result<_>>()?;
                    LargeValueUpdate::Splice {
                        column: column_name,
                        within,
                        splices,
                    }
                }
                "jsonSet" => LargeValueUpdate::JsonSet {
                    column: column_name,
                    edits: edit
                        .get("edits")
                        .and_then(Json::as_array)
                        .ok_or_else(|| refused(format!("{path} needs \"edits\" (an array)")))?
                        .iter()
                        .enumerate()
                        .map(|(i, set)| {
                            Ok(JsonSetEdit {
                                at: set
                                    .get("at")
                                    .and_then(Json::as_str)
                                    .ok_or_else(|| {
                                        refused(format!("{path}.edits[{i}] needs \"at\""))
                                    })?
                                    .to_owned(),
                                value: set.get("value").cloned().unwrap_or(Json::Null),
                            })
                        })
                        .collect::<Result<_>>()?,
                },
                other => {
                    return Err(refused(format!(
                        "{path}.kind must be \"text\", \"splice\" or \"jsonSet\", not {other:?}"
                    )));
                }
            })
        })
        .collect()
}

/// White-box: no JSON input can produce a non-finite double (JSON has no
/// NaN or infinity), so the read side's mapping is checked directly.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_finite_doubles_read_as_null() {
        for double in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                to_json(Value::Double(double), Some(&ColumnType::Double)),
                Json::Null
            );
        }
        assert_eq!(to_json(Value::Double(0.5), None), json!(0.5));
    }
}
