use std::collections::BTreeMap;
use std::fmt;

use jazz::groove::records::EnumSchema;
use jazz::groove::schema::ColumnType as GrooveColumnType;
use jazz::schema::{
    ColumnSchema as CoreColumnSchema, JazzSchema, MergeStrategy, TableSchema as CoreTableSchema,
};

use crate::query_manager::types::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, Schema, TableName, TablePolicies,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectSchemaConversionError {
    path: String,
    message: String,
}

impl DirectSchemaConversionError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for DirectSchemaConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for DirectSchemaConversionError {}

pub(crate) fn convert_alpha_schema(
    schema: &Schema,
) -> Result<JazzSchema, DirectSchemaConversionError> {
    let mut tables = schema.iter().collect::<Vec<_>>();
    tables.sort_by_key(|(name, _)| name.as_str());
    tables
        .into_iter()
        .map(|(name, table)| convert_table(name, table))
        .collect::<Result<Vec<_>, _>>()
        .map(JazzSchema::new)
}

fn convert_table(
    name: &TableName,
    table: &crate::query_manager::types::TableSchema,
) -> Result<CoreTableSchema, DirectSchemaConversionError> {
    if table.policies != TablePolicies::default() {
        return Err(err(
            format!("$.{}", name.as_str()),
            "table policies are not supported by direct fixed-schema conversion yet",
        ));
    }

    let mut references = BTreeMap::new();
    let mut columns = Vec::with_capacity(table.columns.columns.len());
    let mut merge_strategies = BTreeMap::new();
    for column in &table.columns.columns {
        let converted = convert_column(name, column)?;
        if let Some(reference) = &column.references {
            references.insert(
                column.name.as_str().to_owned(),
                reference.as_str().to_owned(),
            );
        }
        if let Some(strategy) = column.merge_strategy {
            merge_strategies.insert(
                column.name.as_str().to_owned(),
                convert_merge_strategy(name, column, strategy)?,
            );
        }
        columns.push(converted);
    }

    let mut converted = CoreTableSchema::new(name.as_str(), columns);
    converted.references = references;
    converted.indexed_columns = table
        .indexed_columns
        .as_ref()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.as_str().to_owned())
                .collect()
        })
        .unwrap_or_default();
    converted.merge_strategies = merge_strategies;
    Ok(converted)
}

fn convert_column(
    table: &TableName,
    column: &ColumnDescriptor,
) -> Result<CoreColumnSchema, DirectSchemaConversionError> {
    if column.default.is_some() {
        return Err(err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            "column defaults are not supported by direct fixed-schema conversion yet",
        ));
    }
    let mut column_type = convert_column_type(table, column.name.as_str(), &column.column_type)?;
    if column.nullable {
        column_type = column_type.nullable();
    }
    Ok(CoreColumnSchema::new(column.name.as_str(), column_type))
}

fn convert_column_type(
    table: &TableName,
    column: &str,
    column_type: &ColumnType,
) -> Result<GrooveColumnType, DirectSchemaConversionError> {
    match column_type {
        ColumnType::Boolean => Ok(GrooveColumnType::Bool),
        ColumnType::Text => Ok(GrooveColumnType::String),
        ColumnType::Timestamp => Ok(GrooveColumnType::U64),
        ColumnType::Double => Ok(GrooveColumnType::F64),
        ColumnType::Uuid => Ok(GrooveColumnType::Uuid),
        ColumnType::Bytea => Ok(GrooveColumnType::Bytes),
        ColumnType::Enum { variants } => Ok(GrooveColumnType::Enum(
            EnumSchema::new(
                format!("{}_{}", table.as_str(), column),
                variants.iter().cloned(),
            )
            .map_err(|error| {
                err(
                    format!("$.{}.{}", table.as_str(), column),
                    format!("invalid enum: {error}"),
                )
            })?,
        )),
        ColumnType::Array { element } => {
            Ok(convert_column_type(table, column, element.as_ref())?.array_of())
        }
        ColumnType::Integer | ColumnType::BigInt => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "signed integer columns are not supported by direct fixed-schema conversion yet",
        )),
        ColumnType::BatchId => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "BatchId columns are not supported by direct fixed-schema conversion yet",
        )),
        ColumnType::Json { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "Json columns are not supported by direct fixed-schema conversion yet",
        )),
        ColumnType::Row { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "nested Row columns are not supported by direct fixed-schema conversion yet",
        )),
    }
}

fn convert_merge_strategy(
    table: &TableName,
    column: &ColumnDescriptor,
    strategy: ColumnMergeStrategy,
) -> Result<MergeStrategy, DirectSchemaConversionError> {
    match strategy {
        ColumnMergeStrategy::Counter => Ok(MergeStrategy::Counter),
        ColumnMergeStrategy::GSet => Err(err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            "GSet merge strategy is not supported by direct fixed-schema conversion yet",
        )),
    }
}

fn err(path: impl Into<String>, message: impl Into<String>) -> DirectSchemaConversionError {
    DirectSchemaConversionError::new(path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, SchemaBuilder, TableSchemaBuilder,
    };

    #[test]
    fn converts_supported_columns_references_and_indexes() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("created", ColumnType::Timestamp)
                    .column("score", ColumnType::Double)
                    .column("data", ColumnType::Bytea)
                    .fk_column("project_id", "projects")
                    .index_only(["project_id"]),
            )
            .build();

        let converted = convert_alpha_schema(&schema).unwrap();
        let todos = converted
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            todos.references.get("project_id").map(String::as_str),
            Some("projects")
        );
        assert!(todos.indexed_columns.contains("project_id"));
        assert_eq!(
            todos
                .columns
                .iter()
                .find(|column| column.name == "done")
                .unwrap()
                .column_type,
            GrooveColumnType::Bool
        );
    }

    #[test]
    fn rejects_unsupported_signed_integer_defaults_and_policies() {
        let integer_schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").column("count", ColumnType::Integer))
            .build();
        assert!(convert_alpha_schema(&integer_schema).is_err());

        let default_schema = [(
            TableName::new("todos"),
            crate::query_manager::types::TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text)
                    .default(crate::query_manager::types::Value::Text("x".to_owned())),
            ])),
        )]
        .into_iter()
        .collect();
        assert!(convert_alpha_schema(&default_schema).is_err());

        let policy_schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(crate::query_manager::policy::PolicyExpr::True),
                    ),
            )
            .build();
        assert!(convert_alpha_schema(&policy_schema).is_err());
    }
}
