use std::collections::BTreeMap;
use std::fmt;

use jazz::groove::records::{EnumSchema, Value as GrooveValue};
use jazz::groove::schema::ColumnType as GrooveColumnType;
use jazz::query::{Operand, Predicate, Query};
use jazz::schema::{
    ColumnSchema as CoreColumnSchema, JazzSchema, MergeStrategy, TableSchema as CoreTableSchema,
};

use crate::query_api::policy::{CmpOp, PolicyValue};
use crate::schema_api::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, Operation, PolicyExpr, Schema, TableName,
    TableSchema, Value,
};

const PUBLIC_USER_ID_SESSION_PATH: &str = "user_id";
const DIRECT_USER_ID_CLAIM: &str = "user_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaConversionError {
    path: String,
    message: String,
}

impl SchemaConversionError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SchemaConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for SchemaConversionError {}

pub(crate) fn convert_public_schema(schema: &Schema) -> Result<JazzSchema, SchemaConversionError> {
    let mut tables = schema.iter().collect::<Vec<_>>();
    tables.sort_by_key(|(name, _)| name.as_str());
    tables
        .into_iter()
        .map(|(name, table)| convert_table(schema, name, table))
        .collect::<Result<Vec<_>, _>>()
        .map(JazzSchema::new)
}

fn convert_table(
    schema: &Schema,
    name: &TableName,
    table: &TableSchema,
) -> Result<CoreTableSchema, SchemaConversionError> {
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
    converted.read_policy = convert_optional_policy(
        schema,
        table,
        name,
        "policies.select.using",
        table.policies.select.using.as_ref(),
    )?;
    converted.write_policy =
        convert_optional_policy(schema, table, name, "policies.write", write_policy(table))?;
    Ok(converted)
}

fn convert_column(
    table: &TableName,
    column: &ColumnDescriptor,
) -> Result<CoreColumnSchema, SchemaConversionError> {
    if column.default.is_some() {
        return Err(err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            "column defaults are not supported by core schema conversion yet",
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
) -> Result<GrooveColumnType, SchemaConversionError> {
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
        // Core does not currently have signed integer cells. Public
        // INTEGER columns are therefore represented as U32 and the
        // core write path rejects negative values.
        ColumnType::Integer => Ok(GrooveColumnType::U32),
        ColumnType::BigInt => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "BIGINT is signed, but local engine fixed schemas only support unsigned integer columns",
        )),
        ColumnType::BatchId => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "BatchId columns are not supported by core schema conversion yet",
        )),
        ColumnType::Json { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "Json columns are not supported by core schema conversion yet",
        )),
        ColumnType::Row { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "nested Row columns are not supported by core schema conversion yet",
        )),
    }
}

fn convert_merge_strategy(
    table: &TableName,
    column: &ColumnDescriptor,
    strategy: ColumnMergeStrategy,
) -> Result<MergeStrategy, SchemaConversionError> {
    match strategy {
        ColumnMergeStrategy::Counter => Ok(MergeStrategy::Counter),
        ColumnMergeStrategy::GSet => Err(err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            "GSet merge strategy is not supported by core schema conversion yet",
        )),
    }
}

fn write_policy(table: &TableSchema) -> Option<&PolicyExpr> {
    table
        .policies
        .insert
        .with_check
        .as_ref()
        .or(table.policies.update.with_check.as_ref())
        .or(table.policies.update.using.as_ref())
        .or(table.policies.delete.using.as_ref())
}

fn convert_optional_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    expr: Option<&PolicyExpr>,
) -> Result<Option<Query>, SchemaConversionError> {
    expr.map(|expr| convert_policy(schema, table_schema, table, path, expr))
        .transpose()
}

fn convert_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
    match expr {
        PolicyExpr::And(exprs) => {
            if !exprs.iter().any(is_core_inherited_select) {
                return Ok(Query::from(table.as_str())
                    .filter(convert_policy_predicate(table, path, expr)?));
            }
            let mut query = Query::from(table.as_str());
            for (index, expr) in exprs.iter().enumerate() {
                query = append_policy_clause(
                    schema,
                    table_schema,
                    table,
                    &format!("{path}.And[{index}]"),
                    query,
                    expr,
                )?;
            }
            Ok(query)
        }
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column,
            max_depth: None,
        } => append_inherited_select_policy(
            schema,
            table_schema,
            table,
            path,
            Query::from(table.as_str()),
            via_column,
        ),
        _ => Ok(Query::from(table.as_str()).filter(convert_policy_predicate(table, path, expr)?)),
    }
}

fn is_core_inherited_select(expr: &PolicyExpr) -> bool {
    matches!(
        expr,
        PolicyExpr::Inherits {
            operation: Operation::Select,
            max_depth: None,
            ..
        }
    )
}

fn append_policy_clause(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    expr: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
    match expr {
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column,
            max_depth: None,
        } => append_inherited_select_policy(schema, table_schema, table, path, query, via_column),
        _ => Ok(query.filter(convert_policy_predicate(table, path, expr)?)),
    }
}

fn append_inherited_select_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    via_column: &str,
) -> Result<Query, SchemaConversionError> {
    let column = table_schema
        .columns
        .columns
        .iter()
        .find(|column| column.name.as_str() == via_column)
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                format!("INHERITS via_column '{via_column}' was not found"),
            )
        })?;
    let parent_table = column.references.as_ref().ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' has no FK reference"),
        )
    })?;
    let parent_schema = schema.get(parent_table).ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' references unknown table '{parent_table}'"),
        )
    })?;
    let parent_policy = parent_schema.policies.select.using.as_ref().ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' references table '{parent_table}' without a SELECT policy"),
        )
    })?;
    let parent_filter = convert_policy_predicate(
        parent_table,
        &format!("{path}.Inherits[{parent_table}]"),
        parent_policy,
    )?;
    Ok(query.join_via_row_id(parent_table.as_str(), via_column, [parent_filter]))
}

fn convert_policy_predicate(
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<Predicate, SchemaConversionError> {
    match expr {
        PolicyExpr::True => Ok(Predicate::All(Vec::new())),
        PolicyExpr::False => Ok(Predicate::Any(Vec::new())),
        PolicyExpr::And(exprs) => exprs
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                convert_policy_predicate(table, &format!("{path}.And[{index}]"), expr)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Predicate::All),
        PolicyExpr::Or(exprs) => exprs
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                convert_policy_predicate(table, &format!("{path}.Or[{index}]"), expr)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Predicate::Any),
        PolicyExpr::Not(expr) => Ok(Predicate::Not(Box::new(convert_policy_predicate(
            table,
            &format!("{path}.Not"),
            expr,
        )?))),
        PolicyExpr::Cmp { column, op, value } => {
            let left = Operand::Column(column.clone());
            let right = convert_policy_operand(table, path, value)?;
            Ok(match op {
                CmpOp::Eq => Predicate::Eq(left, right),
                CmpOp::Ne => Predicate::Ne(left, right),
                CmpOp::Lt => Predicate::Lt(left, right),
                CmpOp::Le => Predicate::Lte(left, right),
                CmpOp::Gt => Predicate::Gt(left, right),
                CmpOp::Ge => Predicate::Gte(left, right),
            })
        }
        PolicyExpr::IsNull { column } => Ok(Predicate::IsNull(Operand::Column(column.clone()))),
        PolicyExpr::IsNotNull { column } => Ok(Predicate::Not(Box::new(Predicate::IsNull(
            Operand::Column(column.clone()),
        )))),
        PolicyExpr::Contains { column, value } => Ok(Predicate::Contains(
            Operand::Column(column.clone()),
            convert_policy_operand(table, path, value)?,
        )),
        other => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!("core schema policies do not support {other:?} yet"),
        )),
    }
}

fn convert_policy_operand(
    table: &TableName,
    path: &str,
    value: &PolicyValue,
) -> Result<Operand, SchemaConversionError> {
    match value {
        PolicyValue::SessionRef(path_segments)
            if path_segments.as_slice() == [String::from(PUBLIC_USER_ID_SESSION_PATH)] =>
        {
            Ok(Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()))
        }
        PolicyValue::SessionRef(path_segments) => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!(
                "core schema policies only support session.user_id references, got session.{}",
                path_segments.join(".")
            ),
        )),
        PolicyValue::Literal(value) => Ok(Operand::Literal(convert_policy_literal(
            table, path, value,
        )?)),
    }
}

fn convert_policy_literal(
    table: &TableName,
    path: &str,
    value: &Value,
) -> Result<GrooveValue, SchemaConversionError> {
    match value {
        Value::Null => Ok(GrooveValue::Nullable(None)),
        Value::Boolean(value) => Ok(GrooveValue::Bool(*value)),
        Value::Text(value) => Ok(GrooveValue::String(value.clone())),
        Value::Uuid(value) => Ok(GrooveValue::Uuid(*value.uuid())),
        other => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!("core schema policies do not support {other:?} literals yet"),
        )),
    }
}

fn err(path: impl Into<String>, message: impl Into<String>) -> SchemaConversionError {
    SchemaConversionError::new(path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::query_api::policy::{CmpOp, PolicyValue};
    use crate::query_api::types::TableSchemaBuilder;
    use crate::schema_api::{
        ColumnDescriptor, ColumnType, PolicyExpr, RowDescriptor, SchemaBuilder, TablePolicies,
        TableSchema,
    };
    use jazz::query::{JoinTarget, Operand, Predicate};
    use uuid::Uuid;

    #[test]
    fn converts_supported_columns_references_and_indexes() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("projects").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("created", ColumnType::Timestamp)
                    .column("score", ColumnType::Double)
                    .column("data", ColumnType::Bytea)
                    .fk_column("project_id", "projects")
                    .index_only(["project_id"]),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
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
    fn converts_public_integer_as_core_u32_and_rejects_defaults() {
        let integer_schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("count", ColumnType::Integer))
            .build();
        let integer_table = convert_public_schema(&integer_schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            integer_table
                .columns
                .iter()
                .find(|column| column.name == "count")
                .unwrap()
                .column_type,
            GrooveColumnType::U32
        );

        let integer_array_schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(
                "partSizes",
                ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                },
            ))
            .build();
        let integer_array_table = convert_public_schema(&integer_array_schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            integer_array_table
                .columns
                .iter()
                .find(|column| column.name == "partSizes")
                .unwrap()
                .column_type,
            GrooveColumnType::U32.array_of()
        );

        let default_schema = [(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text)
                    .default(Value::Text("x".to_owned())),
            ])),
        )]
        .into_iter()
        .collect();
        assert!(convert_public_schema(&default_schema).is_err());
    }

    #[test]
    fn converts_supported_table_policies_to_core_read_and_write_queries() {
        let owner_id = ObjectId::from_uuid(Uuid::nil());
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner_id", ColumnType::Text)
                    .column("token_id", ColumnType::Uuid)
                    .column("archived", ColumnType::Boolean)
                    .nullable_column("deleted_at", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::And(vec![
                                PolicyExpr::Cmp {
                                    column: "owner_id".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                                },
                                PolicyExpr::Not(Box::new(PolicyExpr::Cmp {
                                    column: "archived".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(false.into()),
                                })),
                                PolicyExpr::Or(vec![
                                    PolicyExpr::IsNull {
                                        column: "deleted_at".to_owned(),
                                    },
                                    PolicyExpr::IsNotNull {
                                        column: "deleted_at".to_owned(),
                                    },
                                ]),
                            ]))
                            .with_insert(PolicyExpr::Cmp {
                                column: "token_id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::Literal(Value::Uuid(owner_id)),
                            }),
                    ),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let todos = converted
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .unwrap();

        assert_eq!(todos.read_policy.as_ref().unwrap().table, "todos");
        assert_eq!(
            todos.read_policy.as_ref().unwrap().filters,
            vec![Predicate::All(vec![
                Predicate::Eq(
                    Operand::Column("owner_id".to_owned()),
                    Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
                ),
                Predicate::Not(Box::new(Predicate::Eq(
                    Operand::Column("archived".to_owned()),
                    Operand::Literal(GrooveValue::Bool(false)),
                ))),
                Predicate::Any(vec![
                    Predicate::IsNull(Operand::Column("deleted_at".to_owned())),
                    Predicate::Not(Box::new(Predicate::IsNull(Operand::Column(
                        "deleted_at".to_owned(),
                    )))),
                ]),
            ])]
        );
        assert_eq!(todos.write_policy.as_ref().unwrap().table, "todos");
        assert_eq!(
            todos.write_policy.as_ref().unwrap().filters,
            vec![Predicate::Eq(
                Operand::Column("token_id".to_owned()),
                Operand::Literal(GrooveValue::Uuid(Uuid::nil())),
            )]
        );
    }

    #[test]
    fn rejects_unsupported_policy_subset() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::SessionContains {
                            path: vec!["roles".to_owned()],
                            value: "admin".into(),
                        }),
                    ),
            )
            .build();

        let error = convert_public_schema(&schema).unwrap_err();
        assert!(error.to_string().starts_with(
            "$.todos.policies.select.using: core schema policies do not support SessionContains"
        ));
    }

    #[test]
    fn converts_unbounded_inherited_select_to_row_id_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column(
                        "owners",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Text),
                        },
                    )
                    .policies(TablePolicies::new().with_select(PolicyExpr::Contains {
                        column: "owners".to_owned(),
                        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                    })),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .nullable_fk_column("folder_id", "folders")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "folder_id".to_owned(),
                        max_depth: None,
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let documents = converted
            .tables
            .iter()
            .find(|table| table.name == "documents")
            .unwrap();
        let policy = documents.read_policy.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "folders");
        assert_eq!(join.on_column, "id");
        assert_eq!(join.target, JoinTarget::RowId);
        assert_eq!(join.source_column.as_deref(), Some("folder_id"));
        assert_eq!(
            join.filters,
            vec![Predicate::Contains(
                Operand::Column("owners".to_owned()),
                Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
            )]
        );
    }
}
