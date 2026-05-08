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
    ColumnDescriptor, ColumnMergeStrategy, ColumnName, ColumnType, RowDescriptor, Schema,
    SchemaHash, TableName, TablePolicies, TableSchema, Value,
};

use super::lens::{LensOp, LensTransform};

/// Current encoding version.
const SCHEMA_VERSION: u8 = SchemaEncodingVersion::V5 as u8;
const LENS_VERSION: u8 = 2;
const PERMISSIONS_VERSION: u8 = 1;
const PERMISSIONS_BUNDLE_VERSION: u8 = 2;
const PERMISSIONS_HEAD_VERSION: u8 = 2;

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
enum SchemaEncodingVersion {
    // v1 schemas did not encode policies.
    V1 = 1,
    // v2 schemas include policies, but no legacy inherit-policy byte.
    V2 = 2,
    // v3 schemas include policies and a legacy inherit-policy byte.
    V3 = 3,
    // v4 schemas include column defaults.
    V4 = 4,
    // v5 schemas include column merge strategies.
    V5 = 5,
}

impl SchemaEncodingVersion {
    fn from_byte(version: u8) -> Option<Self> {
        match version {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            3 => Some(Self::V3),
            4 => Some(Self::V4),
            5 => Some(Self::V5),
            _ => None,
        }
    }

    fn has_table_policies(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }

    fn has_legacy_inherit_policy_byte(self) -> bool {
        !matches!(self, Self::V2)
    }

    fn has_column_defaults(self) -> bool {
        matches!(self, Self::V4 | Self::V5)
    }

    fn has_column_merge_strategies(self) -> bool {
        matches!(self, Self::V5)
    }
}

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
/// Tables are sorted by name for deterministic encoding. Column order within a
/// table is preserved exactly as declared.
pub fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    let version = SchemaEncodingVersion::V5;
    buf.push(version as u8);

    // Sort tables by name for deterministic ordering
    let mut tables: Vec<_> = schema.iter().collect();
    tables.sort_by_key(|(name, _)| name.as_str());

    write_u32(&mut buf, tables.len() as u32);

    for (name, table_schema) in tables {
        encode_table_entry_with_version(&mut buf, name, table_schema, version);
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

    let Some(version) = SchemaEncodingVersion::from_byte(data[0]) else {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: data[0],
            expected: SCHEMA_VERSION,
        });
    };

    decode_schema_with_version(data, version)
}

/// Decode only one table descriptor from an encoded schema.
///
/// Structural schemas in large apps can be much larger than the row descriptor
/// needed for a single incoming history row. This keeps row replay from
/// materializing the entire schema map just to find one table.
pub fn decode_table_descriptor_from_schema(
    data: &[u8],
    table_name: &str,
) -> Result<Option<RowDescriptor>, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let Some(version) = SchemaEncodingVersion::from_byte(data[0]) else {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: data[0],
            expected: SCHEMA_VERSION,
        });
    };

    let mut offset = 1;
    let table_count = read_u32(data, &mut offset)?;
    for _ in 0..table_count {
        let name = read_string(data, &mut offset, "table_name")?;
        if name == table_name {
            return decode_row_descriptor_with_version(data, &mut offset, version).map(Some);
        }

        skip_row_descriptor_with_version(data, &mut offset, version)?;
        if version.has_table_policies() {
            decode_table_policies(data, &mut offset)?;
        }
    }

    Ok(None)
}

fn encode_table_entry_with_version(
    buf: &mut Vec<u8>,
    name: &TableName,
    schema: &TableSchema,
    version: SchemaEncodingVersion,
) {
    write_string(buf, name.as_str());
    encode_row_descriptor_with_version(buf, &schema.columns, version);
    if version.has_table_policies() {
        encode_table_policies(buf, &schema.policies);
    }
}

fn decode_table_entry_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<(TableName, TableSchema), CatalogueEncodingError> {
    let name = read_string(data, offset, "table_name")?;
    let descriptor = decode_row_descriptor_with_version(data, offset, version)?;
    if version.has_table_policies() {
        // Legacy schema versions encoded policies inline, but structural schema
        // decode intentionally drops them now that permissions are catalogued
        // separately.
        decode_table_policies(data, offset)?;
    }

    Ok((
        TableName::new(name),
        TableSchema {
            columns: descriptor,
            policies: TablePolicies::default(),
        },
    ))
}

fn decode_schema_with_version(
    data: &[u8],
    version: SchemaEncodingVersion,
) -> Result<Schema, CatalogueEncodingError> {
    let mut offset = 1;
    let table_count = read_u32(data, &mut offset)?;

    let mut schema = HashMap::new();
    for _ in 0..table_count {
        let (name, table_schema) = decode_table_entry_with_version(data, &mut offset, version)?;
        schema.insert(name, table_schema);
    }

    Ok(schema)
}

fn encode_row_descriptor_with_version(
    buf: &mut Vec<u8>,
    desc: &RowDescriptor,
    version: SchemaEncodingVersion,
) {
    write_u32(buf, desc.columns.len() as u32);
    for col in &desc.columns {
        encode_column_descriptor_with_version(buf, col, version);
    }
}

fn decode_row_descriptor_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<RowDescriptor, CatalogueEncodingError> {
    let count = read_u32(data, offset)?;
    let mut columns = Vec::with_capacity(count as usize);

    for _ in 0..count {
        columns.push(decode_column_descriptor_with_version(
            data, offset, version,
        )?);
    }

    Ok(RowDescriptor::new(columns))
}

fn skip_row_descriptor_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<(), CatalogueEncodingError> {
    let count = read_u32(data, offset)?;
    for _ in 0..count {
        skip_column_descriptor_with_version(data, offset, version)?;
    }
    Ok(())
}

fn encode_column_descriptor_with_version(
    buf: &mut Vec<u8>,
    col: &ColumnDescriptor,
    version: SchemaEncodingVersion,
) {
    write_string(buf, col.name.as_str());
    encode_column_type_with_version(buf, &col.column_type, version);
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
    if version.has_legacy_inherit_policy_byte() {
        // Legacy reserved byte kept for backward compatibility with v3 encoding.
        buf.push(0);
    }
    if version.has_column_defaults() {
        match &col.default {
            Some(default) => {
                buf.push(1);
                encode_value(buf, default);
            }
            None => buf.push(0),
        }
    }
    if version.has_column_merge_strategies() {
        match col.merge_strategy {
            Some(ColumnMergeStrategy::Counter) => {
                buf.push(1);
                buf.push(1);
            }
            None => buf.push(0),
        }
    }
}

fn decode_column_descriptor_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<ColumnDescriptor, CatalogueEncodingError> {
    let name = read_string(data, offset, "column_name")?;
    let column_type = decode_column_type_with_version(data, offset, version)?;
    let nullable = read_u8(data, offset)? != 0;
    let has_ref = read_u8(data, offset)? != 0;
    let references = if has_ref {
        Some(TableName::new(read_string(data, offset, "column_ref")?))
    } else {
        None
    };
    if version.has_legacy_inherit_policy_byte() {
        let _legacy_inherit_policy = read_u8(data, offset)? != 0;
    }
    let default = if version.has_column_defaults() {
        let has_default = read_u8(data, offset)? != 0;
        if has_default {
            Some(decode_value(data, offset)?)
        } else {
            None
        }
    } else {
        None
    };
    let merge_strategy = if version.has_column_merge_strategies() {
        let has_merge_strategy = read_u8(data, offset)? != 0;
        if has_merge_strategy {
            match read_u8(data, offset)? {
                1 => Some(ColumnMergeStrategy::Counter),
                tag => {
                    return Err(CatalogueEncodingError::InvalidTypeTag {
                        tag,
                        context: "column_merge_strategy",
                    });
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(ColumnDescriptor {
        name: ColumnName::new(name),
        column_type,
        nullable,
        references,
        default,
        merge_strategy,
    })
}

fn skip_column_descriptor_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<(), CatalogueEncodingError> {
    skip_string(data, offset)?;
    skip_column_type_with_version(data, offset, version)?;
    let _nullable = read_u8(data, offset)?;
    let has_ref = read_u8(data, offset)? != 0;
    if has_ref {
        skip_string(data, offset)?;
    }
    if version.has_legacy_inherit_policy_byte() {
        let _legacy_inherit_policy = read_u8(data, offset)?;
    }
    if version.has_column_defaults() {
        let has_default = read_u8(data, offset)? != 0;
        if has_default {
            skip_value(data, offset)?;
        }
    }
    if version.has_column_merge_strategies() {
        let has_merge_strategy = read_u8(data, offset)? != 0;
        if has_merge_strategy {
            let tag = read_u8(data, offset)?;
            if tag != 1 {
                return Err(CatalogueEncodingError::InvalidTypeTag {
                    tag,
                    context: "column_merge_strategy",
                });
            }
        }
    }
    Ok(())
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
const TYPE_ENUM: u8 = 9;
const TYPE_DOUBLE: u8 = 10;
const TYPE_BYTEA: u8 = 11;
const TYPE_JSON: u8 = 12;
const TYPE_BATCH_ID: u8 = 13;

fn encode_column_type_with_version(
    buf: &mut Vec<u8>,
    col_type: &ColumnType,
    version: SchemaEncodingVersion,
) {
    match col_type {
        ColumnType::Integer => buf.push(TYPE_INTEGER),
        ColumnType::BigInt => buf.push(TYPE_BIGINT),
        ColumnType::Double => buf.push(TYPE_DOUBLE),
        ColumnType::Boolean => buf.push(TYPE_BOOLEAN),
        ColumnType::Text => buf.push(TYPE_TEXT),
        ColumnType::Timestamp => buf.push(TYPE_TIMESTAMP),
        ColumnType::Uuid => buf.push(TYPE_UUID),
        ColumnType::BatchId => buf.push(TYPE_BATCH_ID),
        ColumnType::Bytea => buf.push(TYPE_BYTEA),
        ColumnType::Json { schema } => {
            buf.push(TYPE_JSON);
            match schema {
                Some(schema) => {
                    buf.push(1);
                    if let Ok(encoded) = serde_json::to_vec(schema) {
                        write_u32(buf, encoded.len() as u32);
                        buf.extend_from_slice(&encoded);
                    } else {
                        write_u32(buf, 0);
                    }
                }
                None => buf.push(0),
            }
        }
        ColumnType::Enum { variants } => {
            buf.push(TYPE_ENUM);
            write_u32(buf, variants.len() as u32);
            for variant in variants {
                write_string(buf, variant);
            }
        }
        ColumnType::Array { element: elem } => {
            buf.push(TYPE_ARRAY);
            encode_column_type_with_version(buf, elem, version);
        }
        ColumnType::Row { columns: desc } => {
            buf.push(TYPE_ROW);
            encode_row_descriptor_with_version(buf, desc, version);
        }
    }
}

fn decode_column_type_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<ColumnType, CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        TYPE_INTEGER => Ok(ColumnType::Integer),
        TYPE_BIGINT => Ok(ColumnType::BigInt),
        TYPE_DOUBLE => Ok(ColumnType::Double),
        TYPE_BOOLEAN => Ok(ColumnType::Boolean),
        TYPE_TEXT => Ok(ColumnType::Text),
        TYPE_TIMESTAMP => Ok(ColumnType::Timestamp),
        TYPE_UUID => Ok(ColumnType::Uuid),
        TYPE_BATCH_ID => Ok(ColumnType::BatchId),
        TYPE_BYTEA => Ok(ColumnType::Bytea),
        TYPE_JSON => {
            let has_schema = read_u8(data, offset)? != 0;
            if has_schema {
                let len = read_u32(data, offset)? as usize;
                let bytes = read_bytes(data, offset, len)?;
                let schema = serde_json::from_slice(bytes).map_err(|err| {
                    CatalogueEncodingError::DecodeError {
                        message: format!("invalid json schema payload: {err}"),
                    }
                })?;
                Ok(ColumnType::Json {
                    schema: Some(schema),
                })
            } else {
                Ok(ColumnType::Json { schema: None })
            }
        }
        TYPE_ENUM => {
            let variant_count = read_u32(data, offset)? as usize;
            let mut variants = Vec::with_capacity(variant_count);
            for _ in 0..variant_count {
                variants.push(read_string(data, offset, "enum_variant")?);
            }
            Ok(ColumnType::Enum { variants })
        }
        TYPE_ARRAY => {
            let elem = decode_column_type_with_version(data, offset, version)?;
            Ok(ColumnType::Array {
                element: Box::new(elem),
            })
        }
        TYPE_ROW => {
            let desc = decode_row_descriptor_with_version(data, offset, version)?;
            Ok(ColumnType::Row {
                columns: Box::new(desc),
            })
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "column_type",
        }),
    }
}

fn skip_column_type_with_version(
    data: &[u8],
    offset: &mut usize,
    version: SchemaEncodingVersion,
) -> Result<(), CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        TYPE_INTEGER | TYPE_BIGINT | TYPE_DOUBLE | TYPE_BOOLEAN | TYPE_TEXT | TYPE_TIMESTAMP
        | TYPE_UUID | TYPE_BATCH_ID | TYPE_BYTEA => Ok(()),
        TYPE_JSON => {
            let has_schema = read_u8(data, offset)? != 0;
            if has_schema {
                let len = read_u32(data, offset)? as usize;
                read_bytes(data, offset, len)?;
            }
            Ok(())
        }
        TYPE_ENUM => {
            let variant_count = read_u32(data, offset)? as usize;
            for _ in 0..variant_count {
                skip_string(data, offset)?;
            }
            Ok(())
        }
        TYPE_ARRAY => skip_column_type_with_version(data, offset, version),
        TYPE_ROW => skip_row_descriptor_with_version(data, offset, version),
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "column_type",
        }),
    }
}

fn encode_row_descriptor(buf: &mut Vec<u8>, desc: &RowDescriptor) {
    encode_row_descriptor_with_version(buf, desc, SchemaEncodingVersion::V3);
}

fn decode_row_descriptor(
    data: &[u8],
    offset: &mut usize,
) -> Result<RowDescriptor, CatalogueEncodingError> {
    decode_row_descriptor_with_version(data, offset, SchemaEncodingVersion::V3)
}

pub fn encode_row_descriptor_bytes(desc: &RowDescriptor) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_row_descriptor(&mut buf, desc);
    buf
}

pub fn decode_row_descriptor_bytes(data: &[u8]) -> Result<RowDescriptor, CatalogueEncodingError> {
    let mut offset = 0;
    let descriptor = decode_row_descriptor(data, &mut offset)?;
    if offset != data.len() {
        return Err(CatalogueEncodingError::DecodeError {
            message: format!(
                "row descriptor bytes had trailing data: decoded {offset} of {} bytes",
                data.len()
            ),
        });
    }
    Ok(descriptor)
}

fn encode_column_type(buf: &mut Vec<u8>, col_type: &ColumnType) {
    encode_column_type_with_version(buf, col_type, SchemaEncodingVersion::V3);
}

fn decode_column_type(
    data: &[u8],
    offset: &mut usize,
) -> Result<ColumnType, CatalogueEncodingError> {
    decode_column_type_with_version(data, offset, SchemaEncodingVersion::V3)
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
    match version {
        1 => decode_lens_transform_v1(data),
        LENS_VERSION => decode_lens_transform_v2(data),
        _ => Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: LENS_VERSION,
        }),
    }
}

/// LensOp type tags.
const OP_ADD_COLUMN: u8 = 1;
const OP_REMOVE_COLUMN: u8 = 2;
const OP_RENAME_COLUMN: u8 = 3;
const OP_ADD_TABLE: u8 = 4;
const OP_REMOVE_TABLE: u8 = 5;
const OP_RENAME_TABLE: u8 = 6;

fn encode_lens_op(buf: &mut Vec<u8>, op: &LensOp) {
    match op {
        LensOp::RenameTable { old_name, new_name } => {
            buf.push(OP_RENAME_TABLE);
            write_string(buf, old_name);
            write_string(buf, new_name);
        }
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
        OP_RENAME_TABLE => {
            let old_name = read_string(data, offset, "old_name")?;
            let new_name = read_string(data, offset, "new_name")?;
            Ok(LensOp::RenameTable { old_name, new_name })
        }
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

fn decode_lens_transform_v1(data: &[u8]) -> Result<LensTransform, CatalogueEncodingError> {
    let mut offset = 1;

    let op_count = read_u32(data, &mut offset)?;
    let mut ops = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        ops.push(decode_lens_op_v1(data, &mut offset)?);
    }

    let draft_count = read_u32(data, &mut offset)?;
    let mut draft_ops = Vec::with_capacity(draft_count as usize);
    for _ in 0..draft_count {
        draft_ops.push(read_u32(data, &mut offset)? as usize);
    }

    Ok(LensTransform { ops, draft_ops })
}

fn decode_lens_transform_v2(data: &[u8]) -> Result<LensTransform, CatalogueEncodingError> {
    let mut offset = 1;

    let op_count = read_u32(data, &mut offset)?;
    let mut ops = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        ops.push(decode_lens_op(data, &mut offset)?);
    }

    let draft_count = read_u32(data, &mut offset)?;
    let mut draft_ops = Vec::with_capacity(draft_count as usize);
    for _ in 0..draft_count {
        draft_ops.push(read_u32(data, &mut offset)? as usize);
    }

    Ok(LensTransform { ops, draft_ops })
}

fn decode_lens_op_v1(data: &[u8], offset: &mut usize) -> Result<LensOp, CatalogueEncodingError> {
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
            let schema = decode_table_schema_v1(data, offset)?;
            Ok(LensOp::AddTable { table, schema })
        }
        OP_REMOVE_TABLE => {
            let table = read_string(data, offset, "table")?;
            let schema = decode_table_schema_v1(data, offset)?;
            Ok(LensOp::RemoveTable { table, schema })
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "lens_op",
        }),
    }
}

fn encode_table_schema(buf: &mut Vec<u8>, schema: &TableSchema) {
    encode_row_descriptor(buf, &schema.columns);
}

fn decode_table_schema(
    data: &[u8],
    offset: &mut usize,
) -> Result<TableSchema, CatalogueEncodingError> {
    let descriptor = decode_row_descriptor(data, offset)?;
    Ok(TableSchema {
        columns: descriptor,
        policies: TablePolicies::default(),
    })
}

fn decode_table_schema_v1(
    data: &[u8],
    offset: &mut usize,
) -> Result<TableSchema, CatalogueEncodingError> {
    let descriptor = decode_row_descriptor(data, offset)?;
    decode_table_policies(data, offset)?;
    Ok(TableSchema {
        columns: descriptor,
        policies: TablePolicies::default(),
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
const POLICY_EXPR_INHERITS_WITH_DEPTH: u8 = 12;
const POLICY_EXPR_EXISTS_REL: u8 = 13;
const POLICY_EXPR_INHERITS_REFERENCING: u8 = 14;
const POLICY_EXPR_CONTAINS: u8 = 15;
const POLICY_EXPR_IN_LIST: u8 = 16;
const POLICY_EXPR_SESSION_CMP: u8 = 17;
const POLICY_EXPR_SESSION_IS_NULL: u8 = 18;
const POLICY_EXPR_SESSION_IS_NOT_NULL: u8 = 19;
const POLICY_EXPR_SESSION_CONTAINS: u8 = 20;
const POLICY_EXPR_SESSION_IN_LIST: u8 = 21;

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

pub fn encode_permissions(permissions: &HashMap<TableName, TablePolicies>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(PERMISSIONS_VERSION);

    let mut entries: Vec<_> = permissions.iter().collect();
    entries.sort_by_key(|(name, _)| name.as_str());
    write_u32(&mut buf, entries.len() as u32);

    for (table_name, policies) in entries {
        write_string(&mut buf, table_name.as_str());
        encode_table_policies(&mut buf, policies);
    }

    buf
}

pub fn decode_permissions(
    data: &[u8],
) -> Result<HashMap<TableName, TablePolicies>, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let version = data[0];
    if version != PERMISSIONS_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: PERMISSIONS_VERSION,
        });
    }

    let mut offset = 1;
    let table_count = read_u32(data, &mut offset)?;
    let mut permissions = HashMap::new();

    for _ in 0..table_count {
        let table_name = TableName::new(read_string(data, &mut offset, "table_name")?);
        let policies = decode_table_policies(data, &mut offset)?;
        permissions.insert(table_name, policies);
    }

    Ok(permissions)
}

pub fn encode_permissions_bundle(
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Vec<u8> {
    let encoded_permissions = encode_permissions(permissions);
    let mut buf = Vec::with_capacity(1 + 32 + 8 + 1 + 16 + 4 + encoded_permissions.len());
    buf.push(PERMISSIONS_BUNDLE_VERSION);
    buf.extend_from_slice(schema_hash.as_bytes());
    write_u64(&mut buf, version);
    match parent_bundle_object_id {
        Some(parent_bundle_object_id) => {
            buf.push(1);
            buf.extend_from_slice(parent_bundle_object_id.uuid().as_bytes());
        }
        None => buf.push(0),
    }
    write_u32(&mut buf, encoded_permissions.len() as u32);
    buf.extend_from_slice(&encoded_permissions);
    buf
}

type DecodedPermissionsBundle = (
    SchemaHash,
    u64,
    Option<ObjectId>,
    HashMap<TableName, TablePolicies>,
);

pub fn decode_permissions_bundle(
    data: &[u8],
) -> Result<DecodedPermissionsBundle, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let version = data[0];
    match version {
        1 => decode_permissions_bundle_v1(data),
        PERMISSIONS_BUNDLE_VERSION => decode_permissions_bundle_v2(data),
        _ => Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: PERMISSIONS_BUNDLE_VERSION,
        }),
    }
}

fn decode_permissions_bundle_v1(
    data: &[u8],
) -> Result<DecodedPermissionsBundle, CatalogueEncodingError> {
    let mut offset = 1;
    let schema_hash = SchemaHash::from_bytes(
        read_bytes(data, &mut offset, 32)?
            .try_into()
            .expect("schema hash length should be exact"),
    );
    let payload_len = read_u32(data, &mut offset)? as usize;
    let payload = read_bytes(data, &mut offset, payload_len)?;
    let permissions = decode_permissions(payload)?;
    Ok((schema_hash, 1, None, permissions))
}

fn decode_permissions_bundle_v2(
    data: &[u8],
) -> Result<DecodedPermissionsBundle, CatalogueEncodingError> {
    let mut offset = 1;
    let schema_hash = SchemaHash::from_bytes(
        read_bytes(data, &mut offset, 32)?
            .try_into()
            .expect("schema hash length should be exact"),
    );
    let version = read_u64(data, &mut offset)?;
    let has_parent = read_u8(data, &mut offset)? != 0;
    let parent_bundle_object_id = if has_parent {
        let parent_uuid =
            uuid::Uuid::from_slice(read_bytes(data, &mut offset, 16)?).map_err(|err| {
                CatalogueEncodingError::DecodeError {
                    message: format!("invalid parent permissions bundle object id: {err}"),
                }
            })?;
        Some(ObjectId::from_uuid(parent_uuid))
    } else {
        None
    };
    let payload_len = read_u32(data, &mut offset)? as usize;
    let payload = read_bytes(data, &mut offset, payload_len)?;
    let permissions = decode_permissions(payload)?;
    Ok((schema_hash, version, parent_bundle_object_id, permissions))
}

pub fn encode_permissions_head(
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    bundle_object_id: ObjectId,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 32 + 8 + 1 + 16 + 16);
    buf.push(PERMISSIONS_HEAD_VERSION);
    buf.extend_from_slice(schema_hash.as_bytes());
    write_u64(&mut buf, version);
    match parent_bundle_object_id {
        Some(parent_bundle_object_id) => {
            buf.push(1);
            buf.extend_from_slice(parent_bundle_object_id.uuid().as_bytes());
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(bundle_object_id.uuid().as_bytes());
    buf
}

type DecodedPermissionsHead = (SchemaHash, u64, Option<ObjectId>, ObjectId);

pub fn decode_permissions_head(
    data: &[u8],
) -> Result<DecodedPermissionsHead, CatalogueEncodingError> {
    if data.is_empty() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    }

    let version = data[0];
    match version {
        1 => decode_permissions_head_v1(data),
        PERMISSIONS_HEAD_VERSION => decode_permissions_head_v2(data),
        _ => Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: PERMISSIONS_HEAD_VERSION,
        }),
    }
}

fn decode_permissions_head_v1(
    data: &[u8],
) -> Result<DecodedPermissionsHead, CatalogueEncodingError> {
    let mut offset = 1;
    let schema_hash = SchemaHash::from_bytes(
        read_bytes(data, &mut offset, 32)?
            .try_into()
            .expect("schema hash length should be exact"),
    );
    let bundle_uuid =
        uuid::Uuid::from_slice(read_bytes(data, &mut offset, 16)?).map_err(|err| {
            CatalogueEncodingError::DecodeError {
                message: format!("invalid permissions bundle object id: {err}"),
            }
        })?;
    Ok((schema_hash, 1, None, ObjectId::from_uuid(bundle_uuid)))
}

fn decode_permissions_head_v2(
    data: &[u8],
) -> Result<DecodedPermissionsHead, CatalogueEncodingError> {
    let mut offset = 1;
    let schema_hash = SchemaHash::from_bytes(
        read_bytes(data, &mut offset, 32)?
            .try_into()
            .expect("schema hash length should be exact"),
    );
    let version = read_u64(data, &mut offset)?;
    let has_parent = read_u8(data, &mut offset)? != 0;
    let parent_bundle_object_id = if has_parent {
        let parent_uuid =
            uuid::Uuid::from_slice(read_bytes(data, &mut offset, 16)?).map_err(|err| {
                CatalogueEncodingError::DecodeError {
                    message: format!("invalid parent permissions bundle object id: {err}"),
                }
            })?;
        Some(ObjectId::from_uuid(parent_uuid))
    } else {
        None
    };
    let bundle_uuid =
        uuid::Uuid::from_slice(read_bytes(data, &mut offset, 16)?).map_err(|err| {
            CatalogueEncodingError::DecodeError {
                message: format!("invalid permissions bundle object id: {err}"),
            }
        })?;
    Ok((
        schema_hash,
        version,
        parent_bundle_object_id,
        ObjectId::from_uuid(bundle_uuid),
    ))
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
        PolicyExpr::SessionCmp { path, op, value } => {
            buf.push(POLICY_EXPR_SESSION_CMP);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
            encode_cmp_op(buf, op);
            encode_value(buf, value);
        }
        PolicyExpr::IsNull { column } => {
            buf.push(POLICY_EXPR_IS_NULL);
            write_string(buf, column);
        }
        PolicyExpr::SessionIsNull { path } => {
            buf.push(POLICY_EXPR_SESSION_IS_NULL);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
        }
        PolicyExpr::IsNotNull { column } => {
            buf.push(POLICY_EXPR_IS_NOT_NULL);
            write_string(buf, column);
        }
        PolicyExpr::SessionIsNotNull { path } => {
            buf.push(POLICY_EXPR_SESSION_IS_NOT_NULL);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
        }
        PolicyExpr::Contains { column, value } => {
            buf.push(POLICY_EXPR_CONTAINS);
            write_string(buf, column);
            encode_policy_value(buf, value);
        }
        PolicyExpr::SessionContains { path, value } => {
            buf.push(POLICY_EXPR_SESSION_CONTAINS);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
            encode_value(buf, value);
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
        PolicyExpr::InList { column, values } => {
            buf.push(POLICY_EXPR_IN_LIST);
            write_string(buf, column);
            write_u32(buf, values.len() as u32);
            for value in values {
                encode_policy_value(buf, value);
            }
        }
        PolicyExpr::SessionInList { path, values } => {
            buf.push(POLICY_EXPR_SESSION_IN_LIST);
            write_u32(buf, path.len() as u32);
            for part in path {
                write_string(buf, part);
            }
            write_u32(buf, values.len() as u32);
            for value in values {
                encode_value(buf, value);
            }
        }
        PolicyExpr::Exists { table, condition } => {
            buf.push(POLICY_EXPR_EXISTS);
            write_string(buf, table);
            encode_policy_expr(buf, condition);
        }
        PolicyExpr::ExistsRel { rel } => {
            buf.push(POLICY_EXPR_EXISTS_REL);
            if let Ok(encoded) = serde_json::to_vec(rel) {
                write_u32(buf, encoded.len() as u32);
                buf.extend_from_slice(&encoded);
            } else {
                write_u32(buf, 0);
            }
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => {
            buf.push(if max_depth.is_some() {
                POLICY_EXPR_INHERITS_WITH_DEPTH
            } else {
                POLICY_EXPR_INHERITS
            });
            encode_policy_operation(buf, *operation);
            write_string(buf, via_column);
            if let Some(depth) = max_depth {
                write_u32(buf, *depth as u32);
            }
        }
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => {
            buf.push(POLICY_EXPR_INHERITS_REFERENCING);
            encode_policy_operation(buf, *operation);
            write_string(buf, source_table);
            write_string(buf, via_column);
            buf.push(if max_depth.is_some() { 1 } else { 0 });
            if let Some(depth) = max_depth {
                write_u32(buf, *depth as u32);
            }
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
        POLICY_EXPR_SESSION_CMP => {
            let count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(data, offset, "policy_session_cmp_path")?);
            }
            let op = decode_cmp_op(data, offset)?;
            let value = decode_value(data, offset)?;
            Ok(PolicyExpr::SessionCmp { path, op, value })
        }
        POLICY_EXPR_IS_NULL => {
            let column = read_string(data, offset, "policy_is_null_column")?;
            Ok(PolicyExpr::IsNull { column })
        }
        POLICY_EXPR_SESSION_IS_NULL => {
            let count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(data, offset, "policy_session_is_null_path")?);
            }
            Ok(PolicyExpr::SessionIsNull { path })
        }
        POLICY_EXPR_IS_NOT_NULL => {
            let column = read_string(data, offset, "policy_is_not_null_column")?;
            Ok(PolicyExpr::IsNotNull { column })
        }
        POLICY_EXPR_SESSION_IS_NOT_NULL => {
            let count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(
                    data,
                    offset,
                    "policy_session_is_not_null_path",
                )?);
            }
            Ok(PolicyExpr::SessionIsNotNull { path })
        }
        POLICY_EXPR_CONTAINS => {
            let column = read_string(data, offset, "policy_contains_column")?;
            let value = decode_policy_value(data, offset)?;
            Ok(PolicyExpr::Contains { column, value })
        }
        POLICY_EXPR_SESSION_CONTAINS => {
            let count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(data, offset, "policy_session_contains_path")?);
            }
            let value = decode_value(data, offset)?;
            Ok(PolicyExpr::SessionContains { path, value })
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
        POLICY_EXPR_IN_LIST => {
            let column = read_string(data, offset, "policy_in_list_column")?;
            let count = read_u32(data, offset)? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_policy_value(data, offset)?);
            }
            Ok(PolicyExpr::InList { column, values })
        }
        POLICY_EXPR_SESSION_IN_LIST => {
            let path_count = read_u32(data, offset)? as usize;
            let mut path = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                path.push(read_string(data, offset, "policy_session_in_list_path")?);
            }
            let value_count = read_u32(data, offset)? as usize;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(decode_value(data, offset)?);
            }
            Ok(PolicyExpr::SessionInList { path, values })
        }
        POLICY_EXPR_EXISTS => {
            let table = read_string(data, offset, "policy_exists_table")?;
            let condition = decode_policy_expr(data, offset)?;
            Ok(PolicyExpr::Exists {
                table,
                condition: Box::new(condition),
            })
        }
        POLICY_EXPR_EXISTS_REL => {
            let len = read_u32(data, offset)? as usize;
            let bytes = read_bytes(data, offset, len)?;
            let rel = serde_json::from_slice(bytes).map_err(|err| {
                CatalogueEncodingError::DecodeError {
                    message: format!("invalid policy exists_rel relation: {err}"),
                }
            })?;
            Ok(PolicyExpr::ExistsRel { rel })
        }
        POLICY_EXPR_INHERITS => {
            let operation = decode_policy_operation(data, offset)?;
            let via_column = read_string(data, offset, "policy_inherits_via_column")?;
            Ok(PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth: None,
            })
        }
        POLICY_EXPR_INHERITS_WITH_DEPTH => {
            let operation = decode_policy_operation(data, offset)?;
            let via_column = read_string(data, offset, "policy_inherits_via_column")?;
            let max_depth = read_u32(data, offset)? as usize;
            Ok(PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth: Some(max_depth),
            })
        }
        POLICY_EXPR_INHERITS_REFERENCING => {
            let operation = decode_policy_operation(data, offset)?;
            let source_table = read_string(data, offset, "policy_inherits_referencing_source")?;
            let via_column = read_string(data, offset, "policy_inherits_referencing_via_column")?;
            let has_max_depth = read_u8(data, offset)? != 0;
            let max_depth = if has_max_depth {
                Some(read_u32(data, offset)? as usize)
            } else {
                None
            };
            Ok(PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
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
// 9 intentionally skipped: TYPE_ENUM is 9, and Values have no Enum tag
// (enum values are stored as Text). Keeping Double at 10 aligns with TYPE_DOUBLE.
const VALUE_DOUBLE: u8 = 10;
const VALUE_BYTEA: u8 = 11;
const VALUE_BATCH_ID: u8 = 12;

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
        Value::Double(f) => {
            buf.push(VALUE_DOUBLE);
            buf.extend_from_slice(&f.to_le_bytes());
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
        Value::BatchId(bytes) => {
            buf.push(VALUE_BATCH_ID);
            buf.extend_from_slice(bytes);
        }
        Value::Bytea(bytes) => {
            buf.push(VALUE_BYTEA);
            write_u32(buf, bytes.len() as u32);
            buf.extend_from_slice(bytes);
        }
        Value::Array(elements) => {
            buf.push(VALUE_ARRAY);
            write_u32(buf, elements.len() as u32);
            for elem in elements {
                encode_value(buf, elem);
            }
        }
        Value::Row { values, .. } => {
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
        VALUE_DOUBLE => {
            let bytes = read_bytes(data, offset, 8)?;
            Ok(Value::Double(f64::from_le_bytes(bytes.try_into().unwrap())))
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
        VALUE_BATCH_ID => {
            let bytes = read_bytes(data, offset, 16)?;
            Ok(Value::BatchId(bytes.try_into().expect("16-byte batch id")))
        }
        VALUE_BYTEA => {
            let len = read_u32(data, offset)? as usize;
            let bytes = read_bytes(data, offset, len)?;
            Ok(Value::Bytea(bytes.to_vec()))
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
            Ok(Value::Row { id: None, values })
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "value",
        }),
    }
}

fn skip_value(data: &[u8], offset: &mut usize) -> Result<(), CatalogueEncodingError> {
    let tag = read_u8(data, offset)?;
    match tag {
        VALUE_NULL => Ok(()),
        VALUE_INTEGER => read_bytes(data, offset, 4).map(|_| ()),
        VALUE_BIGINT | VALUE_DOUBLE | VALUE_TIMESTAMP => read_bytes(data, offset, 8).map(|_| ()),
        VALUE_BOOLEAN => read_u8(data, offset).map(|_| ()),
        VALUE_TEXT => skip_string(data, offset),
        VALUE_UUID | VALUE_BATCH_ID => read_bytes(data, offset, 16).map(|_| ()),
        VALUE_BYTEA => {
            let len = read_u32(data, offset)? as usize;
            read_bytes(data, offset, len).map(|_| ())
        }
        VALUE_ARRAY | VALUE_ROW => {
            let count = read_u32(data, offset)?;
            for _ in 0..count {
                skip_value(data, offset)?;
            }
            Ok(())
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

fn write_u64(buf: &mut Vec<u8>, n: u64) {
    buf.extend_from_slice(&n.to_le_bytes());
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, CatalogueEncodingError> {
    let bytes = read_bytes(data, offset, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, CatalogueEncodingError> {
    let bytes = read_bytes(data, offset, 8)?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
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

fn skip_string(data: &[u8], offset: &mut usize) -> Result<(), CatalogueEncodingError> {
    let len = read_u32(data, offset)? as usize;
    read_bytes(data, offset, len).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::SchemaBuilder;
    use serde_json::json;

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
        assert_eq!(users.columns.columns.len(), 2);
    }

    #[test]
    fn schema_roundtrip_preserves_declared_column_order() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let users = decoded.get(&TableName::new("users")).unwrap();
        let column_names = users
            .columns
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(column_names, vec!["name", "id", "email"]);
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
        assert_eq!(users.columns.columns.len(), 4);

        // Find nullable email column
        let email_col = users.columns.column("email").unwrap();
        assert!(email_col.nullable);
        assert_eq!(email_col.column_type, ColumnType::Text);

        // Find FK column
        let org_col = users.columns.column("org_id").unwrap();
        assert_eq!(org_col.references, Some(TableName::new("orgs")));
    }

    #[test]
    fn schema_roundtrip_with_arrays() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("posts")
                    .column("id", ColumnType::Uuid)
                    .column(
                        "tags",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Text),
                        },
                    ),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        let posts = decoded.get(&TableName::new("posts")).unwrap();
        let tags_col = posts.columns.column("tags").unwrap();
        assert!(matches!(
            tags_col.column_type,
            ColumnType::Array { element: _ }
        ));
    }

    #[test]
    fn schema_roundtrip_with_bytea() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("chunks")
                    .column("id", ColumnType::Uuid)
                    .column("payload", ColumnType::Bytea),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let chunks = decoded.get(&TableName::new("chunks")).unwrap();
        assert_eq!(
            chunks.columns.column("payload").unwrap().column_type,
            ColumnType::Bytea
        );
    }

    #[test]
    fn schema_roundtrip_with_json() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("documents")
                    .column(
                        "payload",
                        ColumnType::Json {
                            schema: Some(json!({
                                "type": "object",
                                "required": ["name"]
                            })),
                        },
                    )
                    .column("raw_payload", ColumnType::Json { schema: None }),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let docs = decoded.get(&TableName::new("documents")).unwrap();
        assert_eq!(
            docs.columns.column("payload").unwrap().column_type,
            ColumnType::Json {
                schema: Some(json!({
                    "type": "object",
                    "required": ["name"]
                }))
            }
        );
        assert_eq!(
            docs.columns.column("raw_payload").unwrap().column_type,
            ColumnType::Json { schema: None }
        );
    }

    #[test]
    fn schema_roundtrip_preserves_column_merge_strategy() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("counters"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("value", ColumnType::Integer)
                    .merge_strategy(ColumnMergeStrategy::Counter),
            ])),
        );

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let table = decoded
            .get(&TableName::new("counters"))
            .expect("decoded counters table");
        let column = table.columns.column("value").expect("counter column");

        assert_eq!(column.merge_strategy, Some(ColumnMergeStrategy::Counter));
    }

    #[test]
    fn schema_roundtrip_with_column_defaults() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column_with_default("done", ColumnType::Boolean, Value::Boolean(false))
                    .column_with_default("priority", ColumnType::Integer, Value::Integer(0))
                    .nullable_column("note", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let todos = decoded.get(&TableName::new("todos")).unwrap();

        assert_eq!(
            todos.columns.column("done").unwrap().default,
            Some(Value::Boolean(false))
        );
        assert_eq!(
            todos.columns.column("priority").unwrap().default,
            Some(Value::Integer(0))
        );
        assert_eq!(todos.columns.column("note").unwrap().default, None);
    }

    #[test]
    fn schema_roundtrip_with_fk_reference() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("image", ColumnType::Uuid).references("files"),
            ])),
        );
        schema.insert(
            TableName::new("files"),
            TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                "name",
                ColumnType::Text,
            )])),
        );

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let image_col = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image_col.references, Some(TableName::new("files")));
        assert_eq!(image_col.default, None);
    }

    #[test]
    fn decode_v2_schema_preserves_fk_references() {
        fn encode_schema_v2_for_test(schema: &Schema) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.push(2);

            let mut tables: Vec<_> = schema.iter().collect();
            tables.sort_by_key(|(name, _)| name.as_str());
            write_u32(&mut buf, tables.len() as u32);

            for (name, table_schema) in tables {
                write_string(&mut buf, name.as_str());

                let mut columns: Vec<_> = table_schema.columns.columns.iter().collect();
                columns.sort_by_key(|c| c.name.as_str());
                write_u32(&mut buf, columns.len() as u32);
                for col in columns {
                    write_string(&mut buf, col.name.as_str());
                    encode_column_type(&mut buf, &col.column_type);
                    buf.push(if col.nullable { 1 } else { 0 });
                    match &col.references {
                        Some(table) => {
                            buf.push(1);
                            write_string(&mut buf, table.as_str());
                        }
                        None => buf.push(0),
                    }
                }

                encode_table_policies(&mut buf, &table_schema.policies);
            }

            buf
        }

        let mut schema = Schema::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("image", ColumnType::Uuid).references("files"),
            ])),
        );
        schema.insert(
            TableName::new("files"),
            TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                "name",
                ColumnType::Text,
            )])),
        );

        let encoded_v2 = encode_schema_v2_for_test(&schema);
        let decoded = decode_schema(&encoded_v2).unwrap();
        let image_col = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image_col.references, Some(TableName::new("files")));
        assert_eq!(image_col.default, None);
    }

    #[test]
    fn decode_v3_schema_defaults_to_none() {
        fn encode_schema_v3_for_test(schema: &Schema) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.push(3);

            let mut tables: Vec<_> = schema.iter().collect();
            tables.sort_by_key(|(name, _)| name.as_str());
            write_u32(&mut buf, tables.len() as u32);

            for (name, table_schema) in tables {
                write_string(&mut buf, name.as_str());

                let mut columns: Vec<_> = table_schema.columns.columns.iter().collect();
                columns.sort_by_key(|c| c.name.as_str());
                write_u32(&mut buf, columns.len() as u32);
                for col in columns {
                    write_string(&mut buf, col.name.as_str());
                    encode_column_type(&mut buf, &col.column_type);
                    buf.push(if col.nullable { 1 } else { 0 });
                    match &col.references {
                        Some(table) => {
                            buf.push(1);
                            write_string(&mut buf, table.as_str());
                        }
                        None => buf.push(0),
                    }
                    buf.push(0);
                }

                encode_table_policies(&mut buf, &table_schema.policies);
            }

            buf
        }

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .fk_column("image", "files"),
            )
            .table(TableSchema::builder("files").column("name", ColumnType::Text))
            .build();

        let encoded_v3 = encode_schema_v3_for_test(&schema);
        let decoded = decode_schema(&encoded_v3).unwrap();
        let todos = decoded.get(&TableName::new("todos")).unwrap();

        assert_eq!(todos.columns.column("title").unwrap().default, None);
        assert_eq!(todos.columns.column("image").unwrap().default, None);
        assert_eq!(
            todos.columns.column("image").unwrap().references,
            Some(TableName::new("files"))
        );
    }

    #[test]
    fn schema_roundtrip_with_enum() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(
                "status",
                ColumnType::Enum {
                    variants: vec![
                        "done".to_string(),
                        "in_progress".to_string(),
                        "todo".to_string(),
                    ],
                },
            ))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        let todos = decoded.get(&TableName::new("todos")).unwrap();
        let status_col = todos.columns.column("status").unwrap();
        assert_eq!(
            status_col.column_type,
            ColumnType::Enum {
                variants: vec![
                    "done".to_string(),
                    "in_progress".to_string(),
                    "todo".to_string(),
                ]
            }
        );
    }

    #[test]
    fn schema_roundtrip_strips_policies_but_preserves_hash() {
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
            decoded_todos.policies == TablePolicies::default(),
            "Stored schema encoding should be structural-only"
        );
    }

    #[test]
    fn permissions_roundtrip_preserves_complex_policies() {
        let expected = PolicyExpr::And(vec![
            PolicyExpr::Contains {
                column: "owner_id".to_string(),
                value: PolicyValue::Literal(Value::Text("ali".to_string())),
            },
            PolicyExpr::InList {
                column: "status".to_string(),
                values: vec![
                    PolicyValue::Literal(Value::Text("active".to_string())),
                    PolicyValue::SessionRef(vec!["user_id".to_string()]),
                ],
            },
            PolicyExpr::SessionCmp {
                path: vec!["claims".to_string(), "role".to_string()],
                op: CmpOp::Eq,
                value: Value::Text("manager".to_string()),
            },
            PolicyExpr::SessionInList {
                path: vec!["claims".to_string(), "plan".to_string()],
                values: vec![
                    Value::Text("pro".to_string()),
                    Value::Text("enterprise".to_string()),
                ],
            },
            PolicyExpr::SessionContains {
                path: vec!["claims".to_string(), "teamIds".to_string()],
                value: Value::Text("team_a".to_string()),
            },
            PolicyExpr::SessionIsNull {
                path: vec!["claims".to_string(), "deleted_at".to_string()],
            },
            PolicyExpr::SessionIsNotNull {
                path: vec!["userId".to_string()],
            },
        ]);
        let permissions = HashMap::from([(
            TableName::new("todos"),
            TablePolicies::new().with_select(expected.clone()),
        )]);

        let encoded = encode_permissions(&permissions);
        let decoded = decode_permissions(&encoded).expect("permissions should decode");

        assert_eq!(
            decoded.get(&TableName::new("todos")),
            permissions.get(&TableName::new("todos"))
        );
    }

    #[test]
    fn permissions_bundle_roundtrip_preserves_target_schema() {
        let schema_hash = SchemaHash::compute(
            &SchemaBuilder::new()
                .table(TableSchema::builder("todos").column("title", ColumnType::Text))
                .build(),
        );
        let version = 7;
        let parent_bundle_object_id = Some(ObjectId::new());
        let permissions = HashMap::from([(
            TableName::new("todos"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        let encoded =
            encode_permissions_bundle(schema_hash, version, parent_bundle_object_id, &permissions);
        let (decoded_hash, decoded_version, decoded_parent_bundle_object_id, decoded_permissions) =
            decode_permissions_bundle(&encoded).expect("bundle should decode");

        assert_eq!(decoded_hash, schema_hash);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_parent_bundle_object_id, parent_bundle_object_id);
        assert_eq!(decoded_permissions, permissions);
    }

    #[test]
    fn permissions_head_roundtrip_preserves_bundle_pointer() {
        let schema_hash = SchemaHash::compute(
            &SchemaBuilder::new()
                .table(TableSchema::builder("todos").column("title", ColumnType::Text))
                .build(),
        );
        let version = 7;
        let parent_bundle_object_id = Some(ObjectId::new());
        let bundle_object_id = ObjectId::new();

        let encoded = encode_permissions_head(
            schema_hash,
            version,
            parent_bundle_object_id,
            bundle_object_id,
        );
        let (
            decoded_hash,
            decoded_version,
            decoded_parent_bundle_object_id,
            decoded_bundle_object_id,
        ) = decode_permissions_head(&encoded).expect("head should decode");

        assert_eq!(decoded_hash, schema_hash);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_parent_bundle_object_id, parent_bundle_object_id);
        assert_eq!(decoded_bundle_object_id, bundle_object_id);
    }

    #[test]
    fn decode_v2_schema_discards_policies() {
        fn encode_schema_v2_with_policies(schema: &Schema) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.push(2);

            let mut tables: Vec<_> = schema.iter().collect();
            tables.sort_by_key(|(name, _)| name.as_str());
            write_u32(&mut buf, tables.len() as u32);

            for (name, table_schema) in tables {
                write_string(&mut buf, name.as_str());

                let mut columns: Vec<_> = table_schema.columns.columns.iter().collect();
                columns.sort_by_key(|c| c.name.as_str());
                write_u32(&mut buf, columns.len() as u32);
                for col in columns {
                    write_string(&mut buf, col.name.as_str());
                    encode_column_type(&mut buf, &col.column_type);
                    buf.push(if col.nullable { 1 } else { 0 });
                    match &col.references {
                        Some(table) => {
                            buf.push(1);
                            write_string(&mut buf, table.as_str());
                        }
                        None => buf.push(0),
                    }
                }

                encode_table_policies(&mut buf, &table_schema.policies);
            }

            buf
        }

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("id", ColumnType::Uuid)
                    .column("owner_id", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "owner_id",
                        vec!["user_id".to_string()],
                    ))),
            )
            .build();

        let decoded = decode_schema(&encode_schema_v2_with_policies(&schema)).unwrap();
        assert_eq!(
            decoded.get(&TableName::new("todos")).unwrap().policies,
            TablePolicies::default()
        );
    }

    #[test]
    fn lens_roundtrip_strips_table_policies() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddTable {
                table: "todos".to_string(),
                schema: TableSchema::builder("todos")
                    .column("id", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True))
                    .build(),
            },
            false,
        );

        let decoded = decode_lens_transform(&encode_lens_transform(&transform)).unwrap();
        let LensOp::AddTable { schema, .. } = &decoded.ops[0] else {
            panic!("expected add-table op");
        };
        assert_eq!(schema.policies, TablePolicies::default());
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
    fn lens_transform_roundtrip_rename_table() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 1);
        assert!(matches!(
            &decoded.ops[0],
            LensOp::RenameTable { old_name, new_name }
            if old_name == "users" && new_name == "people"
        ));
    }

    #[test]
    fn lens_transform_roundtrip_all_ops() {
        let mut transform = LensTransform::new();

        // RenameTable
        transform.push(
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            false,
        );

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

        assert_eq!(decoded.ops.len(), 6);
        assert_eq!(decoded.ops, transform.ops);
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
            Value::Bytea(vec![0, 1, 2, 3, 0, 255]),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Row {
                id: None,
                values: vec![Value::Text("a".to_string()), Value::Boolean(false)],
            },
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
