use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use jazz::groove::records::EnumSchema;
use jazz::groove::schema::ColumnType;
use jazz::schema::{ColumnSchema, JazzSchema, LargeValueKind, MergeStrategy, TableSchema};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdminSchemaConversionError {
    path: String,
    message: String,
}

impl AdminSchemaConversionError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }
}

impl fmt::Display for AdminSchemaConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for AdminSchemaConversionError {}

pub(crate) fn convert_admin_schema(
    schema: &Value,
) -> Result<JazzSchema, AdminSchemaConversionError> {
    let tables = table_entries(schema)?;
    let mut converted = Vec::with_capacity(tables.len());
    for (table_name, table_value) in tables {
        converted.push(convert_table(&table_name, table_value)?);
    }
    Ok(JazzSchema::new(converted))
}

fn table_entries(schema: &Value) -> Result<Vec<(String, &Value)>, AdminSchemaConversionError> {
    let object = schema
        .as_object()
        .ok_or_else(|| err("$", "schema must be a JSON object"))?;
    if let Some(schema) = object.get("schema") {
        return table_entries(schema);
    }
    if let Some(tables) = object.get("tables") {
        if let Some(tables) = tables.as_object() {
            return Ok(tables
                .iter()
                .map(|(name, table)| (name.clone(), table))
                .collect());
        }
        let tables = tables
            .as_array()
            .ok_or_else(|| err("$.tables", "tables must be an array or object"))?;
        return tables
            .iter()
            .enumerate()
            .map(|(index, table)| {
                let name = table.get("name").and_then(Value::as_str).ok_or_else(|| {
                    err(format!("$.tables[{index}].name"), "table name is required")
                })?;
                Ok((name.to_owned(), table))
            })
            .collect();
    }
    object
        .iter()
        .map(|(name, table)| Ok((name.clone(), table)))
        .collect()
}

fn convert_table(name: &str, value: &Value) -> Result<TableSchema, AdminSchemaConversionError> {
    let object = value
        .as_object()
        .ok_or_else(|| err(format!("$.{name}"), "table definition must be an object"))?;
    reject_present(
        object,
        &["readPolicy", "writePolicy", "policies"],
        format!("$.{name}"),
    )?;
    let columns_value = object
        .get("columns")
        .ok_or_else(|| err(format!("$.{name}.columns"), "columns are required"))?;
    let columns = columns_value
        .as_array()
        .ok_or_else(|| err(format!("$.{name}.columns"), "columns must be an array"))?;
    let mut converted_columns = Vec::with_capacity(columns.len());
    let mut references = BTreeMap::new();
    let mut column_indexed = BTreeSet::new();
    let mut merge_strategies = BTreeMap::new();
    for (index, column) in columns.iter().enumerate() {
        let path = format!("$.{name}.columns[{index}]");
        let (column, reference, indexed, merge_strategy) = convert_column(name, column, &path)?;
        if let Some(reference) = reference {
            references.insert(column.name.clone(), reference);
        }
        if let Some(merge_strategy) = merge_strategy {
            merge_strategies.insert(column.name.clone(), merge_strategy);
        }
        if indexed {
            column_indexed.insert(column.name.clone());
        }
        converted_columns.push(column);
    }
    let mut table = TableSchema::new(name, converted_columns);
    table.references = references;
    table.merge_strategies = merge_strategies;
    table.indexed_columns = indexed_columns(
        object.get("indexed_columns"),
        format!("$.{name}.indexed_columns"),
    )?;
    table.indexed_columns.extend(column_indexed);
    for column in &table.indexed_columns {
        if !table
            .columns
            .iter()
            .any(|candidate| candidate.name == *column)
        {
            return Err(err(
                format!("$.{name}.indexed_columns"),
                format!("indexed column {column:?} is not declared in table {name:?}"),
            ));
        }
    }
    Ok(table)
}

fn convert_column(
    table: &str,
    value: &Value,
    path: &str,
) -> Result<(ColumnSchema, Option<String>, bool, Option<MergeStrategy>), AdminSchemaConversionError>
{
    let object = value
        .as_object()
        .ok_or_else(|| err(path, "column definition must be an object"))?;
    reject_present(object, &["default", "policies"], path)?;
    reject_truthy(
        object,
        "json",
        path,
        "Json columns are not supported by this alpha slice",
    )?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| err(format!("{path}.name"), "column name is required"))?;
    let column_type_value = object
        .get("column_type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| err(format!("{path}.column_type"), "column_type is required"))?;
    let mut column = convert_column_type(
        table,
        name,
        column_type_value,
        &format!("{path}.column_type"),
    )?;
    if let Some(kind) = object.get("large_value") {
        let kind = kind.as_str().ok_or_else(|| {
            err(
                format!("{path}.large_value"),
                "large_value must be a string",
            )
        })?;
        if !matches!(column.column_type, ColumnType::Bytes) {
            return Err(err(
                format!("{path}.large_value"),
                "large_value is only supported on Bytea columns",
            ));
        }
        column.large_value = Some(match kind {
            "Blob" => LargeValueKind::Blob,
            "Text" => LargeValueKind::Text,
            _ => {
                return Err(err(
                    format!("{path}.large_value"),
                    "large_value must be Blob or Text",
                ));
            }
        });
    } else if object
        .get("large")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        match column.column_type {
            ColumnType::String => {
                column.column_type = ColumnType::Bytes;
                column.large_value = Some(LargeValueKind::Text);
            }
            ColumnType::Bytes => {
                column.large_value = Some(LargeValueKind::Blob);
            }
            _ => {
                return Err(err(
                    format!("{path}.large"),
                    "large columns must be Text/String or Bytea",
                ));
            }
        }
    }
    if object
        .get("timestamp")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && !matches!(column.column_type, ColumnType::String)
    {
        return Err(err(
            format!("{path}.timestamp"),
            "Timestamp columns must use Text/String storage in this alpha slice",
        ));
    }
    if object
        .get("nullable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        column.column_type = column.column_type.nullable();
    }
    if let Some(variants) = object.get("enum") {
        let variants = string_array(variants, format!("{path}.enum"))?;
        column.column_type = ColumnType::Enum(
            EnumSchema::new(format!("{table}_{name}"), variants)
                .map_err(|error| err(format!("{path}.enum"), error.to_string()))?,
        );
    }
    let reference = object
        .get("references")
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                err(
                    format!("{path}.references"),
                    "references must be a table name string",
                )
            })
        })
        .transpose()?;
    let indexed = object
        .get("indexOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let merge_strategy = object
        .get("merge_strategy")
        .map(|value| convert_merge_strategy(value, &format!("{path}.merge_strategy")))
        .transpose()?;
    Ok((column, reference, indexed, merge_strategy))
}

fn convert_merge_strategy(
    value: &Value,
    path: &str,
) -> Result<MergeStrategy, AdminSchemaConversionError> {
    match value.as_str() {
        Some("Counter") => Ok(MergeStrategy::Counter),
        Some("Lww") | Some("LWW") => Ok(MergeStrategy::Lww),
        Some("GSet") => Err(err(
            path,
            "GSet merge strategy is not supported by core schema conversion yet",
        )),
        Some(other) => Err(err(path, format!("unsupported merge strategy {other:?}"))),
        None => Err(err(path, "merge_strategy must be a string")),
    }
}

fn convert_column_type(
    table: &str,
    column: &str,
    value: &Value,
    path: &str,
) -> Result<ColumnSchema, AdminSchemaConversionError> {
    let kind = match value {
        Value::String(kind) => kind.as_str(),
        Value::Object(object) => {
            let kind = object.get("type").and_then(Value::as_str).ok_or_else(|| {
                err(
                    format!("{path}.type"),
                    "column type object requires a type string",
                )
            })?;
            if kind == "Array" {
                let element = object.get("element").ok_or_else(|| {
                    err(
                        format!("{path}.element"),
                        "array column requires an element type",
                    )
                })?;
                let element = convert_scalar_type(element, &format!("{path}.element"))?;
                return Ok(ColumnSchema::new(column, element.array_of()));
            }
            kind
        }
        _ => return Err(err(path, "column_type must be a string or object")),
    };
    let column_type = convert_scalar_kind(kind, path)?;
    let column_schema = ColumnSchema::new(column, column_type);
    let _ = table;
    Ok(column_schema)
}

fn convert_scalar_type(
    value: &Value,
    path: &str,
) -> Result<ColumnType, AdminSchemaConversionError> {
    match value {
        Value::String(kind) => convert_scalar_kind(kind, path),
        Value::Object(object) => {
            let kind = object.get("type").and_then(Value::as_str).ok_or_else(|| {
                err(
                    format!("{path}.type"),
                    "array element type object requires a type string",
                )
            })?;
            if kind == "Array" {
                return Err(err(
                    path,
                    "nested arrays are not supported by this alpha slice",
                ));
            }
            convert_scalar_kind(kind, path)
        }
        _ => Err(err(path, "array element type must be a string or object")),
    }
}

fn convert_scalar_kind(kind: &str, path: &str) -> Result<ColumnType, AdminSchemaConversionError> {
    match kind {
        "Text" | "String" | "string" => Ok(ColumnType::String),
        "Boolean" | "Bool" | "boolean" => Ok(ColumnType::Bool),
        "Uuid" | "UUID" | "uuid" => Ok(ColumnType::Uuid),
        "Bytea" | "Bytes" | "bytea" => Ok(ColumnType::Bytes),
        "Double" | "Float64" | "F64" | "double" => Ok(ColumnType::F64),
        // Jazz core has unsigned integer cells only. Alpha public INTEGER is
        // accepted as the non-negative i32 subset and represented as U32.
        "Integer" | "Int" | "I32" | "Number" => Ok(ColumnType::U32),
        "I64" => Err(err(
            path,
            "I64 columns are not supported by this alpha slice",
        )),
        "Json" | "JSON" => Err(err(
            path,
            "Json columns are not supported by this alpha slice",
        )),
        "Timestamp" | "timestamp" => Ok(ColumnType::U64),
        "Row" => Err(err(
            path,
            "Row columns are not supported by this alpha slice",
        )),
        other => Err(err(path, format!("unsupported column type {other:?}"))),
    }
}

fn indexed_columns(
    value: Option<&Value>,
    path: String,
) -> Result<BTreeSet<String>, AdminSchemaConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    Ok(string_array(value, path)?.into_iter().collect())
}

fn string_array(value: &Value, path: String) -> Result<Vec<String>, AdminSchemaConversionError> {
    let array = value
        .as_array()
        .ok_or_else(|| err(path.clone(), "must be an array of strings"))?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| err(format!("{path}[{index}]"), "must be a string"))
        })
        .collect()
}

fn reject_present(
    object: &serde_json::Map<String, Value>,
    keys: &[&str],
    path: impl Into<String>,
) -> Result<(), AdminSchemaConversionError> {
    let path = path.into();
    for key in keys {
        if object.contains_key(*key) {
            return Err(err(
                format!("{path}.{key}"),
                format!("{key} is not supported by this alpha slice"),
            ));
        }
    }
    Ok(())
}

fn reject_truthy(
    object: &serde_json::Map<String, Value>,
    key: &str,
    path: &str,
    message: &str,
) -> Result<(), AdminSchemaConversionError> {
    if object.get(key).and_then(Value::as_bool).unwrap_or(false) {
        return Err(err(format!("{path}.{key}"), message));
    }
    Ok(())
}

fn err(path: impl Into<String>, message: impl Into<String>) -> AdminSchemaConversionError {
    AdminSchemaConversionError::new(path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::groove::schema::ColumnType;
    use serde_json::json;

    #[test]
    fn converts_bare_upstream_table_map() {
        let schema = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "title", "column_type": "Text" },
                    { "name": "done", "column_type": "Boolean", "nullable": true },
                    { "name": "owner", "column_type": "Uuid", "references": "users" },
                    { "name": "tags", "column_type": { "type": "Array", "element": "Text" } },
                    { "name": "status", "column_type": "Text", "enum": ["open", "done"] }
                ],
                "indexed_columns": ["title"]
            }
        }))
        .expect("schema converts");

        let table = &schema.tables[0];
        assert_eq!(table.name, "todos");
        assert_eq!(
            table.references.get("owner").map(String::as_str),
            Some("users")
        );
        assert!(table.indexed_columns.contains("title"));
        assert!(table.global_current_indexed_columns().contains("owner"));
        assert_eq!(table.columns[1].column_type, ColumnType::Bool.nullable());
        assert_eq!(table.columns[3].column_type, ColumnType::String.array_of());
        assert!(matches!(table.columns[4].column_type, ColumnType::Enum(_)));
    }

    #[test]
    fn converts_public_large_value_marker() {
        let schema = convert_admin_schema(&json!({
            "files": {
                "columns": [
                    {
                        "name": "data",
                        "column_type": { "type": "Bytea" },
                        "large_value": "Blob"
                    }
                ]
            }
        }))
        .expect("schema converts");

        let column = &schema.tables[0].columns[0];
        assert_eq!(column.column_type, ColumnType::Bytes);
        assert_eq!(column.large_value, Some(LargeValueKind::Blob));
    }

    #[test]
    fn converts_public_text_large_value_marker() {
        let schema = convert_admin_schema(&json!({
            "docs": {
                "columns": [
                    {
                        "name": "body",
                        "column_type": { "type": "Bytea" },
                        "large_value": "Text"
                    }
                ]
            }
        }))
        .expect("schema converts");

        let column = &schema.tables[0].columns[0];
        assert_eq!(column.column_type, ColumnType::Bytes);
        assert_eq!(column.large_value, Some(LargeValueKind::Text));
    }

    #[test]
    fn converts_public_counter_merge_strategy() {
        let schema = convert_admin_schema(&json!({
            "counters": {
                "columns": [
                    {
                        "name": "count",
                        "column_type": { "type": "Integer" },
                        "merge_strategy": "Counter"
                    }
                ]
            }
        }))
        .expect("schema converts");

        let table = &schema.tables[0];
        assert_eq!(
            table.merge_strategies.get("count"),
            Some(&MergeStrategy::Counter)
        );
    }

    #[test]
    fn rejects_unsupported_merge_strategy() {
        let err = convert_admin_schema(&json!({
            "sets": {
                "columns": [
                    {
                        "name": "tags",
                        "column_type": { "type": "Array", "element": { "type": "Text" } },
                        "merge_strategy": "GSet"
                    }
                ]
            }
        }))
        .unwrap_err();

        assert!(err.to_string().contains("GSet merge strategy"));
    }

    #[test]
    fn converts_integer_as_u32_and_rejects_unsupported_types_and_defaults() {
        let schema = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "count", "column_type": "Integer" }
                ]
            }
        }))
        .expect("integer schema converts");
        assert_eq!(schema.tables[0].columns[0].column_type, ColumnType::U32);

        let err = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "count", "column_type": "I64" }
                ]
            }
        }))
        .unwrap_err();
        assert!(err.to_string().contains("I64"));

        let err = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "title", "column_type": "Text", "default": "" }
                ]
            }
        }))
        .unwrap_err();
        assert!(err.to_string().contains("default"));
    }
}
