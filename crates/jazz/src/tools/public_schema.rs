//! Stable public schema, query, and session vocabulary.

pub use crate::tools::public_api::policy::{Operation, PolicyExpr};
pub use crate::tools::public_api::query::{AggregateFunction, Query, QueryBuilder};
pub use crate::tools::public_api::relation_ir::{
    ColumnRef as RelColumnRef, JoinCondition as RelJoinCondition, JoinKind as RelJoinKind,
    KeyRef as RelKeyRef, PredicateCmpOp as RelPredicateCmpOp, PredicateExpr as RelPredicateExpr,
    ProjectColumn as RelProjectColumn, ProjectExpr as RelProjectExpr,
    RecursionBound as RelRecursionBound, RelExpr, RowIdRef, ValueRef as RelValueRef,
};
pub use crate::tools::public_api::session::{AuthMode, Session, WriteContext};
pub use crate::tools::public_api::types::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, OrderedRowDelta, QueryResult,
    QueryResultField, Row, RowDelta, RowDescriptor, Schema, SchemaBuilder, SchemaHash, TableName,
    TablePolicies, TableSchema, Value, permissions, policy_expr,
};
pub use crate::tools::transaction::{BatchId, OpenBatchId};

/// Validate JSON-bearing public values without changing their source text.
///
/// This is shared by schema-default admission and every facade write path so a
/// JSON default cannot bypass the same syntax/schema contract as an explicit
/// value. Arrays recurse because `ColumnType::Array` permits JSON elements.
#[cfg(any(feature = "client", feature = "server", test))]
pub(crate) fn validate_json_value(
    value: &Value,
    column_type: &ColumnType,
    path: &str,
) -> Result<(), String> {
    match (value, column_type) {
        (Value::Null, _) => Ok(()),
        (Value::Text(source), ColumnType::Json { schema }) => {
            let instance: serde_json::Value = serde_json::from_str(source)
                .map_err(|error| format!("invalid JSON for column `{path}`: {error}"))?;
            let Some(schema) = schema else {
                return Ok(());
            };
            let validator = jsonschema::validator_for(schema).map_err(|error| {
                format!(
                    "JSON schema validation failed for column `{path}`: invalid declared schema: {error}"
                )
            })?;
            validator.validate(&instance).map_err(|error| {
                format!("JSON schema validation failed for column `{path}`: {error}")
            })
        }
        (Value::Array(values), ColumnType::Array { element }) => {
            values.iter().enumerate().try_for_each(|(index, value)| {
                validate_json_value(value, element, &format!("{path}[{index}]"))
            })
        }
        (Value::Enum { case, values }, ColumnType::EnumPayload { cases }) => {
            let Some(selected) = cases.iter().find(|entry| entry.name == *case) else {
                return Ok(());
            };
            selected
                .fields
                .iter()
                .zip(values)
                .try_for_each(|(field, value)| {
                    validate_json_value(
                        value,
                        &field.column_type,
                        &format!("{path}.{case}.{}", field.name_str()),
                    )
                })
        }
        _ => Ok(()),
    }
}

/// Compile every declared JSON Schema while the public schema is admitted.
///
/// Instance validation stays at writes, but a bad declaration is a schema
/// error even when a nullable column has no value or default yet.
#[cfg(any(feature = "client", feature = "server", test))]
pub(crate) fn validate_json_schemas(column_type: &ColumnType, path: &str) -> Result<(), String> {
    match column_type {
        ColumnType::Json {
            schema: Some(schema),
        } => jsonschema::validator_for(schema)
            .map(|_| ())
            .map_err(|error| format!("invalid JSON schema for column `{path}`: {error}")),
        ColumnType::Array { element } => validate_json_schemas(element, &format!("{path}[]")),
        ColumnType::Row { columns } => columns.columns.iter().try_for_each(|field| {
            validate_json_schemas(&field.column_type, &format!("{path}.{}", field.name_str()))
        }),
        ColumnType::EnumPayload { cases } => cases.iter().try_for_each(|case| {
            case.fields.iter().try_for_each(|field| {
                validate_json_schemas(
                    &field.column_type,
                    &format!("{path}.{}.{}", case.name, field.name_str()),
                )
            })
        }),
        _ => Ok(()),
    }
}
