//! Binary encoding for schemas and lenses.
//!
//! This module provides deterministic binary serialization for Schema and LensTransform,
//! enabling content-addressed storage in the catalogue.
//!
//! Format uses a version byte prefix for future compatibility.

use std::collections::HashMap;

use crate::object::ObjectId;
use crate::query_manager::policy::{CmpOp, Operation, PolicyExpr, PolicyValue};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnName, ColumnType, RowDescriptor, Schema, TableName, TablePolicies,
    TableSchema, Value,
};

use super::lens::{LensOp, LensTransform};

/// Current encoding version.
const SCHEMA_VERSION: u8 = 2;
const LENS_VERSION: u8 = 1;

/// Encoding errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogueEncodingError {
    /// Data too short.
    TruncatedData { expected: usize, actual: usize },
    /// Unknown version byte.
    UnsupportedVersion { found: u8, expected: u8 },
    /// Invalid type tag.
    InvalidTypeTag { tag: u8, context: &'static str },
    /// Invalid UTF-8 string.
    InvalidUtf8 { context: &'static str },
    /// Generic decode error.
    DecodeError { message: String },
}

impl std::fmt::Display for CatalogueEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueEncodingError::TruncatedData { expected, actual } => {
                write!(f, "truncated data: expected {expected} bytes, got {actual}")
            }
            CatalogueEncodingError::UnsupportedVersion { found, expected } => {
                write!(f, "unsupported version: found {found}, expected {expected}")
            }
            CatalogueEncodingError::InvalidTypeTag { tag, context } => {
                write!(f, "invalid type tag {tag} in {context}")
            }
            CatalogueEncodingError::InvalidUtf8 { context } => {
                write!(f, "invalid UTF-8 in {context}")
            }
            CatalogueEncodingError::DecodeError { message } => {
                write!(f, "decode error: {message}")
            }
        }
    }
}

impl std::error::Error for CatalogueEncodingError {}

// ============================================================================
// Schema Encoding
// ============================================================================

/// Encode a Schema to binary format.
///
/// Format:
/// ```text
/// [version: u8][table_count: u32][table_1]...[table_n]
/// ```
///
/// Tables are sorted by name for deterministic encoding.
pub fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(SCHEMA_VERSION);

    // Sort tables by name for deterministic ordering
    let mut tables: Vec<_> = schema.iter().collect();
    tables.sort_by_key(|(name, _)| name.as_str());

    write_u32(&mut buf, tables.len() as u32);

    for (name, table_schema) in tables {
        encode_table_entry(&mut buf, name, table_schema);
    }

    buf
}

/// Decode a Schema from binary format.
pub fn decode_schema(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let version = data[0];
    match version {
        // v1 schemas did not encode policies.
        1 => decode_schema_v1(data),
        // v2 schemas include policies.
        SCHEMA_VERSION => decode_schema_v2(data),
        _ => Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: SCHEMA_VERSION,
        }),
    }
}

fn encode_table_entry(buf: &mut Vec<u8>, name: &TableName, schema: &TableSchema) {
    write_string(buf, name.as_str());
    encode_row_descriptor(buf, &schema.descriptor);
    encode_table_policies(buf, &schema.policies);
}

fn decode_table_entry(
    data: &[u8],
    offset: &mut usize,
) -> Result<(TableName, TableSchema), CatalogueEncodingError> {
    let name = read_string(data, offset, "table_name")?;
    let descriptor = decode_row_descriptor(data, offset)?;
    let policies = decode_table_policies(data, offset)?;

    Ok((
        TableName::new(name),
        TableSchema {
            descriptor,
            policies,
        },
    ))
}

fn decode_table_entry_v1(
    data: &[u8],
    offset: &mut usize,
) -> Result<(TableName, TableSchema), CatalogueEncodingError> {
    let name = read_string(data, offset, "table_name")?;
    let descriptor = decode_row_descriptor(data, offset)?;

    Ok((
        TableName::new(name),
        TableSchema {
            descriptor,
            policies: TablePolicies::default(),
        },
    ))
}

fn decode_schema_v1(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    let mut offset = 1;
    let table_count = read_u32(data, &mut offset)?;

    let mut schema = HashMap::new();
    for _ in 0..table_count {
        let (name, table_schema) = decode_table_entry_v1(data, &mut offset)?;
        schema.insert(name, table_schema);
    }

    Ok(schema)
}

fn decode_schema_v2(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    let mut offset = 1;
    let table_count = read_u32(data, &mut offset)?;

    let mut schema = HashMap::new();
    for _ in 0..table_count {
        let (name, table_schema) = decode_table_entry(data, &mut offset)?;
        schema.insert(name, table_schema);
    }

    Ok(schema)
}

fn encode_row_descriptor(buf: &mut Vec<u8>, desc: &RowDescriptor) {
    // Sort columns by name for deterministic encoding
    let mut columns: Vec<_> = desc.columns.iter().collect();
    columns.sort_by_key(|c| c.name.as_str());

    write_u32(buf, columns.len() as u32);
    for col in columns {
        encode_column_descriptor(buf, col);
    }
}

fn decode_row_descriptor(
    data: &[u8],
    offset: &mut usize,
) -> Result<RowDescriptor, CatalogueEncodingError> {
    let count = read_u32(data, offset)?;
    let mut columns = Vec::with_capacity(count as usize);

    for _ in 0..count {
        columns.push(decode_column_descriptor(data, offset)?);
    }

    Ok(RowDescriptor::new(columns))
}

fn encode_column_descriptor(buf: &mut Vec<u8>, col: &ColumnDescriptor) {
    write_string(buf, col.name.as_str());
    encode_column_type(buf, &col.column_type);
    buf.push(if col.nullable { 1 } else { 0 });

    // References (FK)
    match &col.references {
        Some(table) => {
            buf.push(1);
            write_string(buf, table.as_str());
        }
        None => {
            buf.push(0);
        }
    }
}

fn decode_column_descriptor(
    data: &[u8],
    offset: &mut usize,
) -> Result<ColumnDescriptor, CatalogueEncodingError> {
    let name = read_string(data, offset, "column_name")?;
    let column_type = decode_column_type(data, offset)?;
    let nullable = read_u8(data, offset)? != 0;
    let has_ref = read_u8(data, offset)? != 0;
    let references = if has_ref {
        Some(TableName::new(read_string(data, offset, "column_ref")?))
    } else {
        None
    };

    Ok(ColumnDescriptor {
        name: ColumnName::new(name),
        column_type,
        nullable,
        references,
    })
}

/// Column type tags.
const TYPE_INTEGER: u8 = 1;
const TYPE_BIGINT: u8 = 2;
const TYPE_BOOLEAN: u8 = 3;
const TYPE_TEXT: u8 = 4;
const TYPE_TIMESTAMP: u8 = 5;
const TYPE_UUID: u8 = 6;
const TYPE_ARRAY: u8 = 7;
const TYPE_ROW: u8 = 8;

fn encode_column_type(buf: &mut Vec<u8>, col_type: &ColumnType) {
    match col_type {
        ColumnType::Integer => buf.push(TYPE_INTEGER),
        ColumnType::BigInt => buf.push(TYPE_BIGINT),
        ColumnType::Boolean => buf.push(TYPE_BOOLEAN),
        ColumnType::Text => buf.push(TYPE_TEXT),
        ColumnType::Timestamp => buf.push(TYPE_TIMESTAMP),
        ColumnType::Uuid => buf.push(TYPE_UUID),
        ColumnType::Array(elem) => {
            buf.push(TYPE_ARRAY);
            encode_column_type(buf, elem);
        }
        ColumnType::Row(desc) => {
            buf.push(TYPE_ROW);
            encode_row_descriptor(buf, desc);
        }
    }
}

fn decode_column_type(
    data: &[u8],
    offset: &mut usize,
) -> Result<ColumnType, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        TYPE_INTEGER => Ok(ColumnType::Integer),
        TYPE_BIGINT => Ok(ColumnType::BigInt),
        TYPE_BOOLEAN => Ok(ColumnType::Boolean),
        TYPE_TEXT => Ok(ColumnType::Text),
        TYPE_TIMESTAMP => Ok(ColumnType::Timestamp),
        TYPE_UUID => Ok(ColumnType::Uuid),
        TYPE_ARRAY => {
            let elem = decode_column_type(data, offset)?;
            Ok(ColumnType::Array(Box::new(elem)))
        }
        TYPE_ROW => {
            let desc = decode_row_descriptor(data, offset)?;
            Ok(ColumnType::Row(Box::new(desc)))
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "column_type",
        }),
    }
}

// ============================================================================
// LensTransform Encoding
// ============================================================================

/// Encode a LensTransform to binary format.
///
/// Format:
/// ```text
/// [version: u8][op_count: u32][ops...][draft_count: u32][draft_indices...]
/// ```
pub fn encode_lens_transform(transform: &LensTransform) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(LENS_VERSION);

    // Ops
    write_u32(&mut buf, transform.ops.len() as u32);
    for op in &transform.ops {
        encode_lens_op(&mut buf, op);
    }

    // Draft indices
    write_u32(&mut buf, transform.draft_ops.len() as u32);
    for &idx in &transform.draft_ops {
        write_u32(&mut buf, idx as u32);
    }

    buf
}

/// Decode a LensTransform from binary format.
pub fn decode_lens_transform(data: &[u8]) -> Result<LensTransform, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let version = data[0];
    if version != LENS_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: LENS_VERSION,
        });
    }

    let mut offset = 1;

    // Ops
    let op_count = read_u32(data, &mut offset)?;
    let mut ops = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        ops.push(decode_lens_op(data, &mut offset)?);
    }

    // Draft indices
    let draft_count = read_u32(data, &mut offset)?;
    let mut draft_ops = Vec::with_capacity(draft_count as usize);
    for _ in 0..draft_count {
        draft_ops.push(read_u32(data, &mut offset)? as usize);
    }

    Ok(LensTransform { ops, draft_ops })
}

/// LensOp type tags.
const OP_ADD_COLUMN: u8 = 1;
const OP_REMOVE_COLUMN: u8 = 2;
const OP_RENAME_COLUMN: u8 = 3;
const OP_ADD_TABLE: u8 = 4;
const OP_REMOVE_TABLE: u8 = 5;

fn encode_lens_op(buf: &mut Vec<u8>, op: &LensOp) {
    match op {
        LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } => {
            buf.push(OP_ADD_COLUMN);
            write_string(buf, table);
            write_string(buf, column);
            encode_column_type(buf, column_type);
            encode_value(buf, default);
        }
        LensOp::RemoveColumn {
            table,
            column,
            column_type,
            default,
        } => {
            buf.push(OP_REMOVE_COLUMN);
            write_string(buf, table);
            write_string(buf, column);
            encode_column_type(buf, column_type);
            encode_value(buf, default);
        }
        LensOp::RenameColumn {
            table,
            old_name,
            new_name,
        } => {
            buf.push(OP_RENAME_COLUMN);
            write_string(buf, table);
            write_string(buf, old_name);
            write_string(buf, new_name);
        }
        LensOp::AddTable { table, schema } => {
            buf.push(OP_ADD_TABLE);
            write_string(buf, table);
            encode_table_schema(buf, schema);
        }
        LensOp::RemoveTable { table, schema } => {
            buf.push(OP_REMOVE_TABLE);
            write_string(buf, table);
            encode_table_schema(buf, schema);
        }
    }
}

fn decode_lens_op(data: &[u8], offset: &mut usize) -> Result<LensOp, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        OP_ADD_COLUMN => {
            let table = read_string(data, offset, "table")?;
            let column = read_string(data, offset, "column")?;
            let column_type = decode_column_type(data, offset)?;
            let default = decode_value(data, offset)?;
            Ok(LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            })
        }
        OP_REMOVE_COLUMN => {
            let table = read_string(data, offset, "table")?;
            let column = read_string(data, offset, "column")?;
            let column_type = decode_column_type(data, offset)?;
            let default = decode_value(data, offset)?;
            Ok(LensOp::RemoveColumn {
                table,
                column,
                column_type,
                default,
            })
        }
        OP_RENAME_COLUMN => {
            let table = read_string(data, offset, "table")?;
            let old_name = read_string(data, offset, "old_name")?;
            let new_name = read_string(data, offset, "new_name")?;
            Ok(LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            })
        }
        OP_ADD_TABLE => {
            let table = read_string(data, offset, "table")?;
            let schema = decode_table_schema(data, offset)?;
            Ok(LensOp::AddTable { table, schema })
        }
        OP_REMOVE_TABLE => {
            let table = read_string(data, offset, "table")?;
            let schema = decode_table_schema(data, offset)?;
            Ok(LensOp::RemoveTable { table, schema })
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "lens_op",
        }),
    }
}

fn encode_table_schema(buf: &mut Vec<u8>, schema: &TableSchema) {
    encode_row_descriptor(buf, &schema.descriptor);
    encode_table_policies(buf, &schema.policies);
}

fn decode_table_schema(
    data: &[u8],
    offset: &mut usize,
) -> Result<TableSchema, CatalogueEncodingError> {
    let descriptor = decode_row_descriptor(data, offset)?;
    let policies = decode_table_policies(data, offset)?;
    Ok(TableSchema {
        descriptor,
        policies,
    })
}

// ============================================================================
// Policy Encoding
// ============================================================================

const POLICY_EXPR_CMP: u8 = 1;
const POLICY_EXPR_IS_NULL: u8 = 2;
const POLICY_EXPR_IS_NOT_NULL: u8 = 3;
const POLICY_EXPR_IN: u8 = 4;
const POLICY_EXPR_EXISTS: u8 = 5;
const POLICY_EXPR_INHERITS: u8 = 6;
const POLICY_EXPR_AND: u8 = 7;
const POLICY_EXPR_OR: u8 = 8;
const POLICY_EXPR_NOT: u8 = 9;
const POLICY_EXPR_TRUE: u8 = 10;
const POLICY_EXPR_FALSE: u8 = 11;

const POLICY_VALUE_LITERAL: u8 = 1;
const POLICY_VALUE_SESSION_REF: u8 = 2;

fn encode_table_policies(buf: &mut Vec<u8>, policies: &TablePolicies) {
    encode_operation_policy(buf, &policies.select);
    encode_operation_policy(buf, &policies.insert);
    encode_operation_policy(buf, &policies.update);
    encode_operation_policy(buf, &policies.delete);
}

fn decode_table_policies(
    data: &[u8],
    offset: &mut usize,
) -> Result<TablePolicies, CatalogueEncodingError> {
    Ok(TablePolicies {
        select: decode_operation_policy(data, offset)?,
        insert: decode_operation_policy(data, offset)?,
        update: decode_operation_policy(data, offset)?,
        delete: decode_operation_policy(data, offset)?,
    })
}

fn encode_operation_policy(
    buf: &mut Vec<u8>,
    policy: &crate::query_manager::types::OperationPolicy,
) {
    encode_optional_policy_expr(buf, policy.using.as_ref());
    encode_optional_policy_expr(buf, policy.with_check.as_ref());
}

fn decode_operation_policy(
    data: &[u8],
    offset: &mut usize,
) -> Result<crate::query_manager::types::OperationPolicy, CatalogueEncodingError> {
    Ok(crate::query_manager::types::OperationPolicy {
        using: decode_optional_policy_expr(data, offset)?,
        with_check: decode_optional_policy_expr(data, offset)?,
    })
}

fn encode_optional_policy_expr(buf: &mut Vec<u8>, expr: Option<&PolicyExpr>) {
    match expr {
        Some(e) => {
            buf.push(1);
            encode_policy_expr(buf, e);
        }
        None => buf.push(0),
    }
}

fn decode_optional_policy_expr(
    data: &[u8],
    offset: &mut usize,
) -> Result<Option<PolicyExpr>, CatalogueEncodingError> {
    let has_expr = read_u8(data, offset)? != 0;
    if has_expr {
        Ok(Some(decode_policy_expr(data, offset)?))
    } else {
        Ok(None)
    }
}

fn encode_policy_expr(buf: &mut Vec<u8>, expr: &PolicyExpr) {
    match expr {
        PolicyExpr::Cmp { column, op, value } => {
            buf.push(POLICY_EXPR_CMP);
            write_string(buf, column);
            encode_cmp_op(buf, op);
            encode_policy_value(buf, value);
        }
        PolicyExpr::IsNull { column } => {
            buf.push(POLICY_EXPR_IS_NULL);
            write_string(buf, column);
        }
        PolicyExpr::IsNotNull { column } => {
            buf.push(POLICY_EXPR_IS_NOT_NULL);
            write_string(buf, column);
        }
        PolicyExpr::In {
            column,
            session_path,
        } => {
            buf.push(POLICY_EXPR_IN);
            write_string(buf, column);
            write_u32(buf, session_path.len() as u32);
            for part in session_path {
                write_string(buf, part);
            }
        }
        PolicyExpr::Exists { table, condition } => {
            buf.push(POLICY_EXPR_EXISTS);
            write_string(buf, table);
            encode_policy_expr(buf, condition);
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
        } => {
            buf.push(POLICY_EXPR_INHERITS);
            encode_policy_operation(buf, *operation);
            write_string(buf, via_column);
        }
        PolicyExpr::And(exprs) => {
            buf.push(POLICY_EXPR_AND);
            write_u32(buf, exprs.len() as u32);
            for expr in exprs {
                encode_policy_expr(buf, expr);
            }
        }
        PolicyExpr::Or(exprs) => {
            buf.push(POLICY_EXPR_OR);
            write_u32(buf, exprs.len() as u32);
            for expr in exprs {
                encode_policy_expr(buf, expr);
            }
        }
        PolicyExpr::Not(expr) => {
            buf.push(POLICY_EXPR_NOT);
            encode_policy_expr(buf, expr);
        }
        PolicyExpr::True => buf.push(POLICY_EXPR_TRUE),
        PolicyExpr::False => buf.push(POLICY_EXPR_FALSE),
    }
}

fn decode_policy_expr(
    data: &[u8],
    offset: &mut usize,
) -> Result<PolicyExpr, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        POLICY_EXPR_CMP => {
            let column = read_string(data, offset, "policy_cmp_column")?;
            let op = decode_cmp_op(data, offset)?;
            let value = decode_policy_value(data, offset)?;
            Ok(PolicyExpr::Cmp { column, op, value })
        }
        POLICY_EXPR_IS_NULL => {
            let column = read_string(data, offset, "policy_is_null_column")?;
            Ok(PolicyExpr::IsNull { column })
        }
        POLICY_EXPR_IS_NOT_NULL => {
            let column = read_string(data, offset, "policy_is_not_null_column")?;
            Ok(PolicyExpr::IsNotNull { column })
        }
        POLICY_EXPR_IN => {
            let column = read_string(data, offset, "policy_in_column")?;
            let count = read_u32(data, offset)? as usize;
            let mut session_path = Vec::with_capacity(count);
            for _ in 0..count {
                session_path.push(read_string(data, offset, "policy_in_session_path")?);
            }
            Ok(PolicyExpr::In {
                column,
                session_path,
            })
        }
        POLICY_EXPR_EXISTS => {
            let table = read_string(data, offset, "policy_exists_table")?;
            let condition = decode_policy_expr(data, offset)?;
            Ok(PolicyExpr::Exists {
                table,
                condition: Box::new(condition),
            })
        }
        POLICY_EXPR_INHERITS => {
            let operation = decode_policy_operation(data, offset)?;
            let via_column = read_string(data, offset, "policy_inherits_via_column")?;
            Ok(PolicyExpr::Inherits {
                operation,
                via_column,
            })
        }
        POLICY_EXPR_AND => {
            let count = read_u32(data, offset)? as usize;
            let mut exprs = Vec::with_capacity(count);
            for _ in 0..count {
                exprs.push(decode_policy_expr(data, offset)?);
            }
            Ok(PolicyExpr::And(exprs))
        }
        POLICY_EXPR_OR => {
            let count = read_u32(data, offset)? as usize;
            let mut exprs = Vec::with_capacity(count);
            for _ in 0..count {
                exprs.push(decode_policy_expr(data, offset)?);
            }
            Ok(PolicyExpr::Or(exprs))
        }
        POLICY_EXPR_NOT => {
            let inner = decode_policy_expr(data, offset)?;
            Ok(PolicyExpr::Not(Box::new(inner)))
        }
        POLICY_EXPR_TRUE => Ok(PolicyExpr::True),
        POLICY_EXPR_FALSE => Ok(PolicyExpr::False),
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "policy_expr",
        }),
    }
}

fn encode_policy_value(buf: &mut Vec<u8>, value: &PolicyValue) {
    match value {
        PolicyValue::Literal(v) => {
            buf.push(POLICY_VALUE_LITERAL);
            encode_value(buf, v);
        }
        PolicyValue::SessionRef(path) => {
            buf.push(POLICY_VALUE_SESSION_REF);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
        }
    }
}

fn decode_policy_value(
    data: &[u8],
    offset: &mut usize,
) -> Result<PolicyValue, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        POLICY_VALUE_LITERAL => Ok(PolicyValue::Literal(decode_value(data, offset)?)),
        POLICY_VALUE_SESSION_REF => {
            let count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(data, offset, "policy_session_ref_path")?);
            }
            Ok(PolicyValue::SessionRef(path))
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "policy_value",
        }),
    }
}

fn encode_cmp_op(buf: &mut Vec<u8>, op: &CmpOp) {
    let tag = match op {
        CmpOp::Eq => 1,
        CmpOp::Ne => 2,
        CmpOp::Lt => 3,
        CmpOp::Le => 4,
        CmpOp::Gt => 5,
        CmpOp::Ge => 6,
    };
    buf.push(tag);
}

fn decode_cmp_op(data: &[u8], offset: &mut usize) -> Result<CmpOp, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        1 => Ok(CmpOp::Eq),
        2 => Ok(CmpOp::Ne),
        3 => Ok(CmpOp::Lt),
        4 => Ok(CmpOp::Le),
        5 => Ok(CmpOp::Gt),
        6 => Ok(CmpOp::Ge),
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "policy_cmp_op",
        }),
    }
}

fn encode_policy_operation(buf: &mut Vec<u8>, operation: Operation) {
    let tag = match operation {
        Operation::Select => 1,
        Operation::Insert => 2,
        Operation::Update => 3,
        Operation::Delete => 4,
    };
    buf.push(tag);
}

fn decode_policy_operation(
    data: &[u8],
    offset: &mut usize,
) -> Result<Operation, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        1 => Ok(Operation::Select),
        2 => Ok(Operation::Insert),
        3 => Ok(Operation::Update),
        4 => Ok(Operation::Delete),
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "policy_operation",
        }),
    }
}

// ============================================================================
// Value Encoding
// ============================================================================

/// Value type tags.
const VALUE_NULL: u8 = 0;
const VALUE_INTEGER: u8 = 1;
const VALUE_BIGINT: u8 = 2;
const VALUE_BOOLEAN: u8 = 3;
const VALUE_TEXT: u8 = 4;
const VALUE_TIMESTAMP: u8 = 5;
const VALUE_UUID: u8 = 6;
const VALUE_ARRAY: u8 = 7;
const VALUE_ROW: u8 = 8;

fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.push(VALUE_NULL),
        Value::Integer(n) => {
            buf.push(VALUE_INTEGER);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::BigInt(n) => {
            buf.push(VALUE_BIGINT);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Boolean(b) => {
            buf.push(VALUE_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Text(s) => {
            buf.push(VALUE_TEXT);
            write_string(buf, s);
        }
        Value::Timestamp(t) => {
            buf.push(VALUE_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Uuid(id) => {
            buf.push(VALUE_UUID);
            buf.extend_from_slice(id.uuid().as_bytes());
        }
        Value::Array(elements) => {
            buf.push(VALUE_ARRAY);
            write_u32(buf, elements.len() as u32);
            for elem in elements {
                encode_value(buf, elem);
            }
        }
        Value::Row(values) => {
            buf.push(VALUE_ROW);
            write_u32(buf, values.len() as u32);
            for v in values {
                encode_value(buf, v);
            }
        }
    }
}

fn decode_value(data: &[u8], offset: &mut usize) -> Result<Value, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        VALUE_NULL => Ok(Value::Null),
        VALUE_INTEGER => {
            let bytes = read_bytes(data, offset, 4)?;
            Ok(Value::Integer(i32::from_le_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        VALUE_BIGINT => {
            let bytes = read_bytes(data, offset, 8)?;
            Ok(Value::BigInt(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        VALUE_BOOLEAN => {
            let b = read_u8(data, offset)?;
            Ok(Value::Boolean(b != 0))
        }
        VALUE_TEXT => {
            let s = read_string(data, offset, "value_text")?;
            Ok(Value::Text(s))
        }
        VALUE_TIMESTAMP => {
            let bytes = read_bytes(data, offset, 8)?;
            Ok(Value::Timestamp(u64::from_le_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        VALUE_UUID => {
            let bytes = read_bytes(data, offset, 16)?;
            let uuid =
                uuid::Uuid::from_slice(bytes).map_err(|e| CatalogueEncodingError::DecodeError {
                    message: format!("invalid uuid: {e}"),
                })?;
            Ok(Value::Uuid(ObjectId::from_uuid(uuid)))
        }
        VALUE_ARRAY => {
            let count = read_u32(data, offset)?;
            let mut elements = Vec::with_capacity(count as usize);
            for _ in 0..count {
                elements.push(decode_value(data, offset)?);
            }
            Ok(Value::Array(elements))
        }
        VALUE_ROW => {
            let count = read_u32(data, offset)?;
            let mut values = Vec::with_capacity(count as usize);
            for _ in 0..count {
                values.push(decode_value(data, offset)?);
            }
            Ok(Value::Row(values))
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "value",
        }),
    }
}

// ============================================================================
// Primitive Helpers
// ============================================================================

fn write_u32(buf: &mut Vec<u8>, n: u32) {
    buf.extend_from_slice(&n.to_le_bytes());
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, CatalogueEncodingError> {
    let bytes = read_bytes(data, offset, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, CatalogueEncodingError> {
    if *offset >= data.len() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: *offset + 1,
            actual: data.len(),
        });
    }
    let val = data[*offset];
    *offset += 1;
    Ok(val)
}

fn read_bytes<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], CatalogueEncodingError> {
    if *offset + len > data.len() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: *offset + len,
            actual: data.len(),
        });
    }
    let bytes = &data[*offset..*offset + len];
    *offset += len;
    Ok(bytes)
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn read_string(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<String, CatalogueEncodingError> {
    let len = read_u32(data, offset)? as usize;
    let bytes = read_bytes(data, offset, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| CatalogueEncodingError::InvalidUtf8 { context })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::SchemaBuilder;

    #[test]
    fn schema_roundtrip_simple() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        // Check table exists
        let users = decoded.get(&TableName::new("users")).unwrap();
        assert_eq!(users.descriptor.columns.len(), 2);
    }

    #[test]
    fn schema_roundtrip_complex() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text)
                    .column("score", ColumnType::Integer)
                    .fk_column("org_id", "orgs"),
            )
            .table(
                TableSchema::builder("orgs")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);

        let users = decoded.get(&TableName::new("users")).unwrap();
        assert_eq!(users.descriptor.columns.len(), 4);

        // Find nullable email column
        let email_col = users.descriptor.column("email").unwrap();
        assert!(email_col.nullable);
        assert_eq!(email_col.column_type, ColumnType::Text);

        // Find FK column
        let org_col = users.descriptor.column("org_id").unwrap();
        assert_eq!(org_col.references, Some(TableName::new("orgs")));
    }

    #[test]
    fn schema_roundtrip_with_arrays() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("posts")
                    .column("id", ColumnType::Uuid)
                    .column("tags", ColumnType::Array(Box::new(ColumnType::Text))),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        let posts = decoded.get(&TableName::new("posts")).unwrap();
        let tags_col = posts.descriptor.column("tags").unwrap();
        assert!(matches!(tags_col.column_type, ColumnType::Array(_)));
    }

    #[test]
    fn schema_roundtrip_with_policies_preserves_hash() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("id", ColumnType::Uuid)
                    .column("owner_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "owner_id",
                        vec!["user_id".to_string()],
                    ))),
            )
            .build();

        let original_hash = crate::query_manager::types::SchemaHash::compute(&schema);
        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let decoded_hash = crate::query_manager::types::SchemaHash::compute(&decoded);

        assert_eq!(
            original_hash, decoded_hash,
            "Schema hash must be stable across encode/decode when policies exist"
        );

        let decoded_todos = decoded.get(&TableName::new("todos")).unwrap();
        assert!(
            decoded_todos.policies.select.using.is_some(),
            "Policy should survive roundtrip"
        );
    }

    #[test]
    fn lens_transform_roundtrip_empty() {
        let transform = LensTransform::new();
        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert!(decoded.ops.is_empty());
        assert!(decoded.draft_ops.is_empty());
    }

    #[test]
    fn lens_transform_roundtrip_add_column() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "email".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 1);
        assert!(decoded.draft_ops.is_empty());

        if let LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } = &decoded.ops[0]
        {
            assert_eq!(table, "users");
            assert_eq!(column, "email");
            assert_eq!(*column_type, ColumnType::Text);
            assert_eq!(*default, Value::Null);
        } else {
            panic!("Expected AddColumn");
        }
    }

    #[test]
    fn lens_transform_roundtrip_with_drafts() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "a".to_string(),
                column_type: ColumnType::Integer,
                default: Value::Integer(0),
            },
            false,
        );
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "b".to_string(),
                column_type: ColumnType::Uuid,
                default: Value::Null,
            },
            true, // draft
        );
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "x".to_string(),
                new_name: "y".to_string(),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 3);
        assert_eq!(decoded.draft_ops, vec![1]); // Second op is draft
    }

    #[test]
    fn lens_transform_roundtrip_all_ops() {
        let mut transform = LensTransform::new();

        // AddColumn
        transform.push(
            LensOp::AddColumn {
                table: "t".to_string(),
                column: "c".to_string(),
                column_type: ColumnType::BigInt,
                default: Value::BigInt(42),
            },
            false,
        );

        // RemoveColumn
        transform.push(
            LensOp::RemoveColumn {
                table: "t".to_string(),
                column: "old".to_string(),
                column_type: ColumnType::Boolean,
                default: Value::Boolean(false),
            },
            false,
        );

        // RenameColumn
        transform.push(
            LensOp::RenameColumn {
                table: "t".to_string(),
                old_name: "a".to_string(),
                new_name: "b".to_string(),
            },
            false,
        );

        // AddTable
        transform.push(
            LensOp::AddTable {
                table: "new_table".to_string(),
                schema: TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                    "id",
                    ColumnType::Uuid,
                )])),
            },
            false,
        );

        // RemoveTable
        transform.push(
            LensOp::RemoveTable {
                table: "old_table".to_string(),
                schema: TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                    "x",
                    ColumnType::Text,
                )])),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 5);
    }

    #[test]
    fn value_roundtrip_all_types() {
        let values = vec![
            Value::Null,
            Value::Integer(42),
            Value::BigInt(-12345678901234i64),
            Value::Boolean(true),
            Value::Text("hello world".to_string()),
            Value::Timestamp(1234567890123456),
            Value::Uuid(ObjectId::from_uuid(uuid::Uuid::from_u128(0xDEADBEEF))),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Row(vec![Value::Text("a".to_string()), Value::Boolean(false)]),
        ];

        for original in values {
            let mut buf = Vec::new();
            encode_value(&mut buf, &original);

            let mut offset = 0;
            let decoded = decode_value(&buf, &mut offset).unwrap();

            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn schema_encoding_deterministic() {
        // Same schema encoded multiple times should produce identical bytes
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("b_table")
                    .column("z_col", ColumnType::Integer)
                    .column("a_col", ColumnType::Text),
            )
            .table(TableSchema::builder("a_table").column("id", ColumnType::Uuid))
            .build();

        let encoded1 = encode_schema(&schema);
        let encoded2 = encode_schema(&schema);

        assert_eq!(encoded1, encoded2);
    }

    #[test]
    fn decode_invalid_version() {
        let data = vec![99, 0, 0, 0, 0]; // Unknown version 99
        let result = decode_schema(&data);
        assert!(matches!(
            result,
            Err(CatalogueEncodingError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn decode_truncated_data() {
        let data = vec![1]; // Version only, no table count
        let result = decode_schema(&data);
        assert!(matches!(
            result,
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));
    }
}
