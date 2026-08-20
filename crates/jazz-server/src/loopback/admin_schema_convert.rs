use std::collections::BTreeSet;
use std::fmt;

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    ColumnDescriptor as PublicColumnDescriptor, ColumnMergeStrategy as PublicMergeStrategy,
    ColumnName as PublicColumnName, ColumnType as PublicColumnType,
    RowDescriptor as PublicRowDescriptor, Schema as PublicSchema, TableName as PublicTableName,
    TableSchema as PublicTableSchema,
};
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
    let mut source = PublicSchema::new();
    for (table_name, table_value) in tables {
        source.insert(
            PublicTableName::new(&table_name),
            convert_table(&table_name, table_value)?,
        );
    }
    JazzSchema::new(&source)
        .map_err(|error| err("$", format!("public schema conversion failed: {error}")))
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

fn convert_table(
    name: &str,
    value: &Value,
) -> Result<PublicTableSchema, AdminSchemaConversionError> {
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
    let mut column_indexed = BTreeSet::new();
    for (index, column) in columns.iter().enumerate() {
        let path = format!("$.{name}.columns[{index}]");
        let (column, indexed) = convert_column(name, column, &path)?;
        if indexed {
            column_indexed.insert(column.name.as_str().to_owned());
        }
        converted_columns.push(column);
    }
    let mut selected_indexes = indexed_columns(
        object.get("indexed_columns"),
        format!("$.{name}.indexed_columns"),
    )?;
    selected_indexes.extend(column_indexed);
    for column in &selected_indexes {
        if !converted_columns
            .iter()
            .any(|candidate| candidate.name.as_str() == column)
        {
            return Err(err(
                format!("$.{name}.indexed_columns"),
                format!("indexed column {column:?} is not declared in table {name:?}"),
            ));
        }
    }
    let mut table = PublicTableSchema::new(PublicRowDescriptor::new(converted_columns));
    table.indexed_columns = Some(
        selected_indexes
            .into_iter()
            .map(PublicColumnName::new)
            .collect(),
    );
    Ok(table)
}

fn convert_column(
    table: &str,
    value: &Value,
    path: &str,
) -> Result<(PublicColumnDescriptor, bool), AdminSchemaConversionError> {
    let object = value
        .as_object()
        .ok_or_else(|| err(path, "column definition must be an object"))?;
    reject_present(object, &["policies"], path)?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| err(format!("{path}.name"), "column name is required"))?;
    let column_type_value = object
        .get("column_type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| err(format!("{path}.column_type"), "column_type is required"))?;
    let mut column = PublicColumnDescriptor::new(
        name,
        convert_column_type(column_type_value, &format!("{path}.column_type"))?,
    );
    if object.contains_key("large_value") {
        return Err(err(
            format!("{path}.large_value"),
            "this column extension has been removed from this core version",
        ));
    }
    if object
        .get("large")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(err(
            format!("{path}.large"),
            "this column extension has been removed from this core version",
        ));
    }
    if object
        .get("timestamp")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && !matches!(column.column_type, PublicColumnType::Text)
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
        column.nullable = true;
    }
    if let Some(variants) = object.get("enum") {
        let variants = string_array(variants, format!("{path}.enum"))?;
        column.column_type = PublicColumnType::ScalarEnum {
            name: format!("{table}_{name}"),
            variants,
        };
    }
    column.references = object
        .get("references")
        .map(|value| {
            value.as_str().map(PublicTableName::new).ok_or_else(|| {
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
        .transpose()?
        .flatten();
    if merge_strategy == Some(PublicMergeStrategy::GSet)
        && (column.nullable || !matches!(column.column_type, PublicColumnType::Array { .. }))
    {
        return Err(err(
            format!("{path}.merge_strategy"),
            "GSet merge strategy requires a non-nullable ARRAY column",
        ));
    }
    column.merge_strategy = merge_strategy;
    Ok((column, indexed))
}

fn convert_merge_strategy(
    value: &Value,
    path: &str,
) -> Result<Option<PublicMergeStrategy>, AdminSchemaConversionError> {
    match value.as_str() {
        Some("Counter") => Ok(Some(PublicMergeStrategy::Counter)),
        // LWW is the public schema's default and therefore has no explicit
        // source-level marker.
        Some("Lww") | Some("LWW") => Ok(None),
        Some("GSet") => Ok(Some(PublicMergeStrategy::GSet)),
        Some(other) => Err(err(path, format!("unsupported merge strategy {other:?}"))),
        None => Err(err(path, "merge_strategy must be a string")),
    }
}

fn convert_column_type(
    value: &Value,
    path: &str,
) -> Result<PublicColumnType, AdminSchemaConversionError> {
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
                return Ok(PublicColumnType::Array {
                    element: Box::new(element),
                });
            }
            kind
        }
        _ => return Err(err(path, "column_type must be a string or object")),
    };
    convert_scalar_kind(kind, path)
}

fn convert_scalar_type(
    value: &Value,
    path: &str,
) -> Result<PublicColumnType, AdminSchemaConversionError> {
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

fn convert_scalar_kind(
    kind: &str,
    path: &str,
) -> Result<PublicColumnType, AdminSchemaConversionError> {
    match kind {
        "Text" | "String" | "string" => Ok(PublicColumnType::Text),
        "Boolean" | "Bool" | "boolean" => Ok(PublicColumnType::Boolean),
        "Uuid" | "UUID" | "uuid" => Ok(PublicColumnType::Uuid),
        "Bytea" | "Bytes" | "bytea" => Ok(PublicColumnType::Bytea),
        "Double" | "Float64" | "F64" | "double" => Ok(PublicColumnType::Double),
        "Integer" | "Int" | "I32" | "Number" => Ok(PublicColumnType::Integer),
        "I64" => Err(err(
            path,
            "I64 columns are not supported by this alpha slice",
        )),
        "Json" | "JSON" => Ok(PublicColumnType::Text),
        "Timestamp" | "timestamp" => Ok(PublicColumnType::Timestamp),
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

fn err(path: impl Into<String>, message: impl Into<String>) -> AdminSchemaConversionError {
    AdminSchemaConversionError::new(path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::groove::schema::ColumnType;
    use jazz::schema::MergeStrategy;
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
        assert!(matches!(
            table.columns[4].column_type,
            ColumnType::EnumTag(_)
        ));
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
    fn converts_public_gset_merge_strategy() {
        let schema = convert_admin_schema(&json!({
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
        .expect("schema converts");

        assert_eq!(
            schema.tables[0].merge_strategies.get("tags"),
            Some(&MergeStrategy::GSet)
        );
    }

    #[test]
    fn converts_integer_as_i32_and_rejects_unsupported_types() {
        let schema = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "count", "column_type": "Integer", "default": 0 }
                ]
            }
        }))
        .expect("integer schema converts");
        assert_eq!(schema.tables[0].columns[0].column_type, ColumnType::I32);

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
                    { "name": "payload", "column_type": "Row" }
                ]
            }
        }))
        .unwrap_err();
        assert!(err.to_string().contains("Row columns"));

        let err = convert_admin_schema(&json!({
            "todos": {
                "columns": [
                    { "name": "count", "column_type": "BigInt" }
                ]
            }
        }))
        .unwrap_err();
        assert!(err.to_string().contains("unsupported column type"));
    }

    #[test]
    fn converts_json_as_string_storage() {
        let schema = convert_admin_schema(&json!({
            "events": {
                "columns": [
                    { "name": "payload", "column_type": "Json" },
                    { "name": "metadata", "column_type": { "type": "JSON" }, "nullable": true }
                ]
            }
        }))
        .expect("json schema converts");

        let table = &schema.tables[0];
        assert_eq!(table.columns[0].column_type, ColumnType::String);
        assert_eq!(table.columns[1].column_type, ColumnType::String.nullable());
    }

    #[test]
    fn rejects_removed_large_value_descriptor_field() {
        let err = convert_admin_schema(&json!({
            "files": {
                "columns": [{
                    "name": "data",
                    "column_type": "Bytea",
                    "large_value": "Blob"
                }]
            }
        }))
        .unwrap_err();

        assert_eq!(err.path, "$.files.columns[0].large_value");
    }

    #[test]
    fn rejects_removed_truthy_large_column_flag() {
        let err = convert_admin_schema(&json!({
            "files": {
                "columns": [{
                    "name": "data",
                    "column_type": "Bytea",
                    "large": true
                }]
            }
        }))
        .unwrap_err();

        assert_eq!(err.path, "$.files.columns[0].large");
    }
}
