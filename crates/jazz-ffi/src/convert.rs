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

/// A row as `{ "id": …, "<column>": … }`, with the table's columns only.
pub(crate) fn row_json(table: &TableSchema, id: ObjectId, fields: Vec<QueryResultField>) -> Json {
    let mut row = Map::new();
    row.insert("id".into(), Json::String(id.to_string()));
    for field in fields {
        if let Some(column) = table
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

/// The engine values of a row JSON object, for an insert (`required`) or a
/// patch.
pub(crate) fn cells(
    table: &TableSchema,
    table_name: &str,
    row: &str,
    what: &str,
) -> Result<(Option<ObjectId>, Vec<(String, Value)>)> {
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

/// The query JSON as an engine query, and its table's name.
pub(crate) fn query(schema: &Schema, query_json: &Json) -> Result<(String, q::Query)> {
    let Json::Object(object) = query_json else {
        return Err(refused("the query must be a JSON object"));
    };
    for key in object.keys() {
        match key.as_str() {
            "table" | "where" | "orderBy" | "limit" | "offset" => {}
            "include" | "includes" | "select" | "relations" => {
                return Err(unsupported(format!(
                    "{key} in a query (nested related rows); query the related table by its reference column"
                )));
            }
            other => return Err(refused(format!("the query has an unknown key {other:?}"))),
        }
    }
    let table_name = object
        .get("table")
        .and_then(Json::as_str)
        .ok_or_else(|| refused("the query needs a \"table\""))?;
    let table = table(schema, table_name)?;
    let mut query = q::Query::from(table_name);

    if let Some(conditions) = object.get("where").filter(|w| !w.is_null()) {
        let conditions = conditions
            .as_array()
            .ok_or_else(|| refused("\"where\" must be an array of conditions"))?;
        for (at, condition) in conditions.iter().enumerate() {
            query.filters.push(predicate(
                table,
                table_name,
                condition,
                &format!("where[{at}]"),
            )?);
        }
    }
    if let Some(order) = object.get("orderBy").filter(|o| !o.is_null()) {
        let order = order
            .as_array()
            .ok_or_else(|| refused("\"orderBy\" must be an array"))?;
        for (at, entry) in order.iter().enumerate() {
            let path = format!("orderBy[{at}]");
            let name = entry
                .get("column")
                .and_then(Json::as_str)
                .ok_or_else(|| refused(format!("{path} needs a \"column\"")))?;
            column(table, table_name, name)?;
            let direction = match entry.get("direction").and_then(Json::as_str) {
                None | Some("asc") => OrderDirection::Asc,
                Some("desc") => OrderDirection::Desc,
                Some(other) => {
                    return Err(refused(format!(
                        "{path}.direction must be \"asc\" or \"desc\", not {other:?}"
                    )));
                }
            };
            query.order_by.push(q::OrderBy {
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
                .ok_or_else(|| refused(format!("\"{key}\" must be a non-negative integer"))),
        }
    };
    query.limit = count("limit")?;
    if let Some(offset) = count("offset")? {
        query.offset = offset;
    }
    Ok((table_name.to_owned(), query))
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
pub(crate) fn delta_json(
    table: &TableSchema,
    delta: OrderedRowDelta,
    reset: bool,
) -> Result<String> {
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
