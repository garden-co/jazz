//! SQL parsing and generation for schema definitions.
//!
//! This module provides bidirectional conversion between SQL DDL and our Schema/LensTransform types.
//!
//! # Supported SQL
//!
//! ## Schema DDL (CREATE TABLE)
//! ```sql
//! CREATE TABLE todos (
//!     title TEXT NOT NULL,
//!     completed BOOLEAN NOT NULL
//! );
//! ```
//!
//! ## Lens DDL (ALTER TABLE)
//! ```sql
//! ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
//! ALTER TABLE users DROP COLUMN deprecated_field;
//! ALTER TABLE users RENAME COLUMN email TO email_address;
//! CREATE TABLE new_table (id TEXT NOT NULL);
//! DROP TABLE old_table;
//! ```

use std::collections::HashMap;

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, DataType, Expr, ObjectName,
    Statement, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::query_manager::types::{
    ColumnDescriptor, ColumnName, ColumnType, RowDescriptor, Schema, TableName, TableSchema, Value,
};

use super::lens::{LensOp, LensTransform};

/// Errors that can occur during SQL parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParseError {
    /// SQL syntax error from the parser.
    SyntaxError(String),
    /// Unsupported SQL statement type.
    UnsupportedStatement(String),
    /// Unsupported column type.
    UnsupportedType(String),
    /// Unsupported constraint.
    UnsupportedConstraint(String),
    /// Missing required information.
    MissingInfo(String),
    /// Invalid value in DEFAULT clause.
    InvalidDefaultValue(String),
}

impl std::fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlParseError::SyntaxError(msg) => write!(f, "SQL syntax error: {}", msg),
            SqlParseError::UnsupportedStatement(msg) => write!(f, "Unsupported statement: {}", msg),
            SqlParseError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            SqlParseError::UnsupportedConstraint(msg) => {
                write!(f, "Unsupported constraint: {}", msg)
            }
            SqlParseError::MissingInfo(msg) => write!(f, "Missing required info: {}", msg),
            SqlParseError::InvalidDefaultValue(msg) => write!(f, "Invalid default value: {}", msg),
        }
    }
}

impl std::error::Error for SqlParseError {}

/// Parse a schema SQL file into a Schema.
///
/// The SQL should contain only CREATE TABLE statements.
///
/// # Example
/// ```ignore
/// let sql = r#"
///     CREATE TABLE todos (
///         title TEXT NOT NULL,
///         completed BOOLEAN NOT NULL
///     );
/// "#;
/// let schema = parse_schema(sql)?;
/// ```
pub fn parse_schema(sql: &str) -> Result<Schema, SqlParseError> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlParseError::SyntaxError(e.to_string()))?;

    let mut schema = HashMap::new();

    for stmt in statements {
        match stmt {
            Statement::CreateTable(create) => {
                let table_name = object_name_to_string(&create.name);
                let table_schema = parse_create_table(&create.columns)?;
                schema.insert(TableName::new(table_name), table_schema);
            }
            _ => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "Only CREATE TABLE statements are allowed in schema files, got: {:?}",
                    stmt
                )));
            }
        }
    }

    Ok(schema)
}

/// Parse a lens SQL file into a LensTransform.
///
/// The SQL should contain ALTER TABLE, CREATE TABLE, or DROP TABLE statements.
///
/// # Example
/// ```ignore
/// let sql = r#"
///     ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
///     ALTER TABLE users DROP COLUMN deprecated_field;
/// "#;
/// let transform = parse_lens(sql)?;
/// ```
pub fn parse_lens(sql: &str) -> Result<LensTransform, SqlParseError> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlParseError::SyntaxError(e.to_string()))?;

    let mut transform = LensTransform::new();

    for stmt in statements {
        match stmt {
            Statement::AlterTable {
                name, operations, ..
            } => {
                let table_name = object_name_to_string(&name);
                for op in operations {
                    let (lens_op, is_draft) = parse_alter_operation(&table_name, &op)?;
                    transform.push(lens_op, is_draft);
                }
            }
            Statement::CreateTable(create) => {
                let table_name = object_name_to_string(&create.name);
                let table_schema = parse_create_table(&create.columns)?;
                transform.push(
                    LensOp::AddTable {
                        table: table_name,
                        schema: table_schema,
                    },
                    false,
                );
            }
            Statement::Drop { names, .. } => {
                for name in names {
                    let table_name = object_name_to_string(&name);
                    // For DROP TABLE, we need the schema for reversibility
                    // This is marked as draft since we don't have the schema info
                    transform.push(
                        LensOp::RemoveTable {
                            table: table_name,
                            schema: TableSchema::new(RowDescriptor::new(vec![])),
                        },
                        true, // Draft - needs schema info filled in
                    );
                }
            }
            _ => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "Only ALTER TABLE, CREATE TABLE, and DROP TABLE are allowed in lens files, got: {:?}",
                    stmt
                )));
            }
        }
    }

    Ok(transform)
}

/// Generate SQL CREATE TABLE statements from a Schema.
pub fn schema_to_sql(schema: &Schema) -> String {
    let mut lines = Vec::new();

    // Sort tables for deterministic output
    let mut table_names: Vec<_> = schema.keys().collect();
    table_names.sort_by_key(|t| t.as_str());

    for table_name in table_names {
        let table_schema = &schema[table_name];
        lines.push(table_schema_to_sql(table_name.as_str(), table_schema));
    }

    lines.join("\n\n")
}

/// Generate SQL for a single table schema.
fn table_schema_to_sql(table_name: &str, schema: &TableSchema) -> String {
    let mut columns = Vec::new();

    for col in &schema.descriptor.columns {
        let col_sql = column_descriptor_to_sql(col);
        columns.push(format!("    {}", col_sql));
    }

    format!("CREATE TABLE {} (\n{}\n);", table_name, columns.join(",\n"))
}

/// Generate SQL for a column descriptor.
fn column_descriptor_to_sql(col: &ColumnDescriptor) -> String {
    let type_str = column_type_to_sql(&col.column_type);
    let nullable_str = if col.nullable { "" } else { " NOT NULL" };

    format!("{} {}{}", col.name.as_str(), type_str, nullable_str)
}

/// Generate SQL ALTER TABLE statements from a LensTransform.
pub fn lens_to_sql(transform: &LensTransform) -> String {
    let mut lines = Vec::new();

    for (idx, op) in transform.ops.iter().enumerate() {
        let is_draft = transform.draft_ops.contains(&idx);
        let sql = lens_op_to_sql(op);

        if is_draft {
            lines.push(format!("-- TODO: Review\n{}", sql));
        } else {
            lines.push(sql);
        }
    }

    lines.join("\n")
}

/// Generate SQL for a single lens operation.
fn lens_op_to_sql(op: &LensOp) -> String {
    match op {
        LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } => {
            let type_str = column_type_to_sql(column_type);
            let default_str = value_to_sql(default);
            format!(
                "ALTER TABLE {} ADD COLUMN {} {} DEFAULT {};",
                table, column, type_str, default_str
            )
        }
        LensOp::RemoveColumn { table, column, .. } => {
            format!("ALTER TABLE {} DROP COLUMN {};", table, column)
        }
        LensOp::RenameColumn {
            table,
            old_name,
            new_name,
        } => {
            format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {};",
                table, old_name, new_name
            )
        }
        LensOp::AddTable { table, schema } => table_schema_to_sql(table, schema),
        LensOp::RemoveTable { table, .. } => {
            format!("DROP TABLE {};", table)
        }
    }
}

// ============================================================================
// Internal Parsing Helpers
// ============================================================================

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_create_table(columns: &[ColumnDef]) -> Result<TableSchema, SqlParseError> {
    let mut col_descriptors = Vec::new();

    for col in columns {
        let col_desc = parse_column_def(col)?;
        col_descriptors.push(col_desc);
    }

    Ok(TableSchema::new(RowDescriptor::new(col_descriptors)))
}

fn parse_column_def(col: &ColumnDef) -> Result<ColumnDescriptor, SqlParseError> {
    let name = ColumnName::new(&col.name.value);
    let column_type = parse_data_type(&col.data_type)?;

    // Default to nullable unless NOT NULL is specified
    let mut nullable = true;
    let mut _references: Option<TableName> = None;

    for opt in &col.options {
        match &opt.option {
            ColumnOption::NotNull => {
                nullable = false;
            }
            ColumnOption::Null => {
                nullable = true;
            }
            ColumnOption::Default(_) => {
                // Default values are handled in lens operations, not schema
            }
            ColumnOption::Unique { .. } => {
                // Ignored for now - deferred feature
            }
            ColumnOption::ForeignKey { foreign_table, .. } => {
                _references = Some(TableName::new(object_name_to_string(foreign_table)));
            }
            _ => {
                // Ignore other options for now
            }
        }
    }

    let mut descriptor = ColumnDescriptor::new(name, column_type);
    if nullable {
        descriptor = descriptor.nullable();
    }
    if let Some(ref_table) = _references {
        descriptor = descriptor.references(ref_table);
    }

    Ok(descriptor)
}

fn parse_data_type(dt: &DataType) -> Result<ColumnType, SqlParseError> {
    match dt {
        DataType::Text | DataType::Varchar(_) | DataType::String(_) | DataType::Char(_) => {
            Ok(ColumnType::Text)
        }
        DataType::Integer(_) | DataType::Int(_) | DataType::SmallInt(_) | DataType::TinyInt(_) => {
            Ok(ColumnType::Integer)
        }
        DataType::BigInt(_) => Ok(ColumnType::BigInt),
        DataType::Boolean | DataType::Bool => Ok(ColumnType::Boolean),
        DataType::Real | DataType::Float(_) | DataType::Double(_) | DataType::DoublePrecision => {
            // Map floats to BigInt for now (we don't have REAL in ColumnType)
            // TODO: Add REAL to ColumnType
            Ok(ColumnType::BigInt)
        }
        DataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        DataType::Uuid => Ok(ColumnType::Uuid),
        DataType::Blob(_) | DataType::Binary(_) | DataType::Varbinary(_) | DataType::Bytea => {
            // Map blob types to Text for now
            Ok(ColumnType::Text)
        }
        _ => Err(SqlParseError::UnsupportedType(format!("{:?}", dt))),
    }
}

fn parse_alter_operation(
    table: &str,
    op: &AlterTableOperation,
) -> Result<(LensOp, bool), SqlParseError> {
    match op {
        AlterTableOperation::AddColumn {
            column_def,
            if_not_exists: _,
            ..
        } => {
            let col = parse_column_def(column_def)?;

            // Extract DEFAULT value if present
            let default = extract_default_value(column_def)?;

            Ok((
                LensOp::AddColumn {
                    table: table.to_string(),
                    column: col.name.as_str().to_string(),
                    column_type: col.column_type,
                    default,
                },
                false,
            ))
        }
        AlterTableOperation::DropColumn {
            column_name,
            if_exists: _,
            ..
        } => {
            // For DROP COLUMN, we mark as draft since we don't know the column type
            Ok((
                LensOp::RemoveColumn {
                    table: table.to_string(),
                    column: column_name.value.clone(),
                    column_type: ColumnType::Text, // Placeholder
                    default: Value::Null,
                },
                true, // Draft - needs type info filled in
            ))
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok((
            LensOp::RenameColumn {
                table: table.to_string(),
                old_name: old_column_name.value.clone(),
                new_name: new_column_name.value.clone(),
            },
            false,
        )),
        AlterTableOperation::AlterColumn { column_name: _, op } => {
            match op {
                AlterColumnOperation::SetDefault { value: _ } => {
                    // Can't really express this as a lens op directly
                    Err(SqlParseError::UnsupportedStatement(
                        "ALTER COLUMN SET DEFAULT is not supported in lenses".to_string(),
                    ))
                }
                AlterColumnOperation::DropDefault => Err(SqlParseError::UnsupportedStatement(
                    "ALTER COLUMN DROP DEFAULT is not supported in lenses".to_string(),
                )),
                _ => Err(SqlParseError::UnsupportedStatement(format!(
                    "Unsupported ALTER COLUMN operation: {:?}",
                    op
                ))),
            }
        }
        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "Unsupported ALTER TABLE operation: {:?}",
            op
        ))),
    }
}

fn extract_default_value(col: &ColumnDef) -> Result<Value, SqlParseError> {
    for opt in &col.options {
        if let ColumnOption::Default(expr) = &opt.option {
            return parse_default_expr(expr);
        }
    }
    // No DEFAULT specified - use NULL
    Ok(Value::Null)
}

fn parse_default_expr(expr: &Expr) -> Result<Value, SqlParseError> {
    match expr {
        Expr::Value(val) => parse_sql_value(val),
        Expr::UnaryOp { op, expr } => {
            // Handle negative numbers
            if matches!(op, sqlparser::ast::UnaryOperator::Minus)
                && let Expr::Value(SqlValue::Number(n, _)) = expr.as_ref()
            {
                let negated = format!("-{}", n);
                if let Ok(i) = negated.parse::<i32>() {
                    return Ok(Value::Integer(i));
                }
                if let Ok(i) = negated.parse::<i64>() {
                    return Ok(Value::BigInt(i));
                }
            }
            Err(SqlParseError::InvalidDefaultValue(format!(
                "Unsupported expression: {:?}",
                expr
            )))
        }
        Expr::Identifier(ident) if ident.value.to_uppercase() == "NULL" => Ok(Value::Null),
        _ => Err(SqlParseError::InvalidDefaultValue(format!(
            "Unsupported default expression: {:?}",
            expr
        ))),
    }
}

fn parse_sql_value(val: &SqlValue) -> Result<Value, SqlParseError> {
    match val {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i32>() {
                Ok(Value::Integer(i))
            } else if let Ok(i) = n.parse::<i64>() {
                Ok(Value::BigInt(i))
            } else {
                Err(SqlParseError::InvalidDefaultValue(format!(
                    "Cannot parse number: {}",
                    n
                )))
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        _ => Err(SqlParseError::InvalidDefaultValue(format!(
            "Unsupported value type: {:?}",
            val
        ))),
    }
}

// ============================================================================
// SQL Generation Helpers
// ============================================================================

fn column_type_to_sql(ct: &ColumnType) -> &'static str {
    match ct {
        ColumnType::Integer => "INTEGER",
        ColumnType::BigInt => "BIGINT",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Text => "TEXT",
        ColumnType::Timestamp => "TIMESTAMP",
        ColumnType::Uuid => "UUID",
        ColumnType::Array(_) => "TEXT", // Serialize as JSON text for now
        ColumnType::Row(_) => "TEXT",   // Serialize as JSON text for now
    }
}

fn value_to_sql(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Timestamp(t) => t.to_string(),
        Value::Uuid(id) => format!("'{:?}'", id),
        Value::Array(_) => "'[]'".to_string(),
        Value::Row(_) => "'{}'".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_create_table() {
        let sql = r#"
            CREATE TABLE todos (
                title TEXT NOT NULL,
                completed BOOLEAN NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.len(), 1);

        let todos = schema.get(&TableName::new("todos")).unwrap();
        assert_eq!(todos.descriptor.columns.len(), 2);

        let title = &todos.descriptor.columns[0];
        assert_eq!(title.name.as_str(), "title");
        assert_eq!(title.column_type, ColumnType::Text);
        assert!(!title.nullable);

        let completed = &todos.descriptor.columns[1];
        assert_eq!(completed.name.as_str(), "completed");
        assert_eq!(completed.column_type, ColumnType::Boolean);
        assert!(!completed.nullable);
    }

    #[test]
    fn parse_nullable_columns() {
        let sql = r#"
            CREATE TABLE users (
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let users = schema.get(&TableName::new("users")).unwrap();

        assert!(!users.descriptor.columns[0].nullable); // name
        assert!(users.descriptor.columns[1].nullable); // email
        assert!(users.descriptor.columns[2].nullable); // age
    }

    #[test]
    fn parse_multiple_tables() {
        let sql = r#"
            CREATE TABLE users (
                id UUID NOT NULL,
                name TEXT NOT NULL
            );

            CREATE TABLE posts (
                id UUID NOT NULL,
                title TEXT NOT NULL,
                author_id UUID NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.len(), 2);
        assert!(schema.contains_key(&TableName::new("users")));
        assert!(schema.contains_key(&TableName::new("posts")));
    }

    #[test]
    fn parse_add_column_lens() {
        let sql = "ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);

        match &transform.ops[0] {
            LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
                assert_eq!(*column_type, ColumnType::Integer);
                assert_eq!(*default, Value::Integer(0));
            }
            _ => panic!("Expected AddColumn"),
        }
    }

    #[test]
    fn parse_drop_column_lens() {
        let sql = "ALTER TABLE users DROP COLUMN deprecated_field;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(transform.has_drafts()); // DROP COLUMN is marked as draft

        match &transform.ops[0] {
            LensOp::RemoveColumn { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "deprecated_field");
            }
            _ => panic!("Expected RemoveColumn"),
        }
    }

    #[test]
    fn parse_rename_column_lens() {
        let sql = "ALTER TABLE users RENAME COLUMN email TO email_address;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(!transform.has_drafts());

        match &transform.ops[0] {
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => {
                assert_eq!(table, "users");
                assert_eq!(old_name, "email");
                assert_eq!(new_name, "email_address");
            }
            _ => panic!("Expected RenameColumn"),
        }
    }

    #[test]
    fn parse_create_table_lens() {
        let sql = r#"
            CREATE TABLE new_table (
                id TEXT NOT NULL,
                value INTEGER
            );
        "#;

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);

        match &transform.ops[0] {
            LensOp::AddTable { table, schema } => {
                assert_eq!(table, "new_table");
                assert_eq!(schema.descriptor.columns.len(), 2);
            }
            _ => panic!("Expected AddTable"),
        }
    }

    #[test]
    fn parse_drop_table_lens() {
        let sql = "DROP TABLE old_table;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(transform.has_drafts()); // DROP TABLE is marked as draft

        match &transform.ops[0] {
            LensOp::RemoveTable { table, .. } => {
                assert_eq!(table, "old_table");
            }
            _ => panic!("Expected RemoveTable"),
        }
    }

    #[test]
    fn parse_multiple_lens_ops() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
            ALTER TABLE users DROP COLUMN deprecated_field;
            ALTER TABLE users RENAME COLUMN email TO email_address;
        "#;

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 3);

        assert!(matches!(&transform.ops[0], LensOp::AddColumn { .. }));
        assert!(matches!(&transform.ops[1], LensOp::RemoveColumn { .. }));
        assert!(matches!(&transform.ops[2], LensOp::RenameColumn { .. }));
    }

    #[test]
    fn schema_to_sql_roundtrip() {
        let sql = r#"CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);"#;

        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);

        // Parse the regenerated SQL to verify it's valid
        let reparsed = parse_schema(&regenerated).unwrap();

        assert_eq!(schema.len(), reparsed.len());
        let todos = schema.get(&TableName::new("todos")).unwrap();
        let todos2 = reparsed.get(&TableName::new("todos")).unwrap();
        assert_eq!(
            todos.descriptor.columns.len(),
            todos2.descriptor.columns.len()
        );
    }

    #[test]
    fn lens_to_sql_add_column() {
        let transform = LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: Value::Integer(0),
        }]);

        let sql = lens_to_sql(&transform);
        assert!(sql.contains("ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;"));
    }

    #[test]
    fn lens_to_sql_with_draft() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RemoveColumn {
                table: "users".to_string(),
                column: "old".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            true,
        );

        let sql = lens_to_sql(&transform);
        assert!(sql.contains("-- TODO: Review"));
        assert!(sql.contains("ALTER TABLE users DROP COLUMN old;"));
    }

    #[test]
    fn parse_default_values() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN count INTEGER DEFAULT 42;
            ALTER TABLE users ADD COLUMN name TEXT DEFAULT 'unknown';
            ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;
        "#;

        let transform = parse_lens(sql).unwrap();

        match &transform.ops[0] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Integer(42));
            }
            _ => panic!(),
        }

        match &transform.ops[1] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Text("unknown".to_string()));
            }
            _ => panic!(),
        }

        match &transform.ops[2] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Boolean(true));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_various_column_types() {
        let sql = r#"
            CREATE TABLE test (
                a TEXT NOT NULL,
                b INTEGER NOT NULL,
                c BIGINT NOT NULL,
                d BOOLEAN NOT NULL,
                e TIMESTAMP NOT NULL,
                f UUID NOT NULL,
                g VARCHAR(255) NOT NULL,
                h CHAR(10) NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("test")).unwrap();

        assert_eq!(table.descriptor.columns[0].column_type, ColumnType::Text);
        assert_eq!(table.descriptor.columns[1].column_type, ColumnType::Integer);
        assert_eq!(table.descriptor.columns[2].column_type, ColumnType::BigInt);
        assert_eq!(table.descriptor.columns[3].column_type, ColumnType::Boolean);
        assert_eq!(
            table.descriptor.columns[4].column_type,
            ColumnType::Timestamp
        );
        assert_eq!(table.descriptor.columns[5].column_type, ColumnType::Uuid);
        assert_eq!(table.descriptor.columns[6].column_type, ColumnType::Text); // VARCHAR -> TEXT
        assert_eq!(table.descriptor.columns[7].column_type, ColumnType::Text); // CHAR -> TEXT
    }

    #[test]
    fn reject_non_create_in_schema() {
        let sql = "ALTER TABLE users ADD COLUMN age INTEGER;";

        let result = parse_schema(sql);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SqlParseError::UnsupportedStatement(_)
        ));
    }

    #[test]
    fn reject_select_in_lens() {
        let sql = "SELECT * FROM users;";

        let result = parse_lens(sql);
        assert!(result.is_err());
    }
}
