//! Binary encoding for active catalogue payloads.
//!
//! This module provides deterministic binary serialization for Schema and LensTransform,
//! enabling content-addressed storage in the catalogue.
//!
//! Each storage-epoch-one payload family begins with its frozen `v1` outer
//! version byte. Decoders accept that single spelling only: no former outer
//! labels are aliases or compatibility inputs.

use std::collections::{BTreeSet, HashMap};

use jazz::tools::ObjectId;
use jazz::tools::public_schema::{CmpOp, Operation, PolicyExpr, PolicyValue};
use jazz::tools::public_schema::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnName, ColumnType, EnumCaseDescriptor,
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelProjectColumn, RelProjectExpr, RelRecursionBound, RelValueRef,
    RowDescriptor, RowIdRef, Schema, SchemaHash, TableName, TablePolicies, TableSchema, Value,
};

use jazz::tools::schema_lens::{LensOp, LensTransform};

/// Frozen storage-epoch-one outer envelope versions.
const SCHEMA_VERSION: u8 = 1;
const LENS_VERSION: u8 = 1;
const PERMISSIONS_VERSION: u8 = 1;
const PERMISSIONS_BUNDLE_VERSION: u8 = 1;
const PERMISSIONS_HEAD_VERSION: u8 = 1;

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

    if data[0] != SCHEMA_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: data[0],
            expected: SCHEMA_VERSION,
        });
    }

    let schema = decode_current_schema(data)?;
    ensure_canonical_payload(data, &encode_schema(&schema), "schema")?;
    Ok(schema)
}

fn encode_table_entry(buf: &mut Vec<u8>, name: &TableName, schema: &TableSchema) {
    write_string(buf, name.as_str());
    encode_row_descriptor(buf, &schema.columns);
    encode_indexed_columns(buf, schema.indexed_columns.as_deref());
    encode_branch_bindings(buf, &schema.branch_by);
}

fn decode_table_entry(
    data: &[u8],
    offset: &mut usize,
    schema_version: u8,
) -> Result<(TableName, TableSchema), CatalogueEncodingError> {
    let name = read_string(data, offset, "table_name")?;
    let descriptor = decode_row_descriptor(data, offset, schema_version)?;
    let indexed_columns = decode_indexed_columns(data, offset)?;
    let branch_by = decode_branch_bindings(data, offset)?;

    Ok((
        TableName::new(name),
        TableSchema {
            columns: descriptor,
            indexed_columns,
            policies: TablePolicies::default(),
            branch_by,
        },
    ))
}

fn encode_branch_bindings(buf: &mut Vec<u8>, bindings: &[ColumnName]) {
    write_u32(buf, bindings.len() as u32);
    for binding in bindings {
        write_string(buf, binding.as_str());
    }
}

fn decode_branch_bindings(
    data: &[u8],
    offset: &mut usize,
) -> Result<Vec<ColumnName>, CatalogueEncodingError> {
    let count = read_count(data, offset, "branch_bindings")?;
    let mut bindings = Vec::with_capacity(count);
    for _ in 0..count {
        bindings.push(ColumnName::new(read_string(data, offset, "branch_column")?));
    }
    ensure_unique_names(
        bindings.iter().map(|binding| binding.as_str()),
        "branch binding",
    )?;
    Ok(bindings)
}

fn encode_indexed_columns(buf: &mut Vec<u8>, indexed_columns: Option<&[ColumnName]>) {
    match indexed_columns {
        None => write_u32(buf, u32::MAX),
        Some(columns) => {
            write_u32(buf, columns.len() as u32);
            let mut columns: Vec<_> = columns.iter().map(|column| column.as_str()).collect();
            columns.sort_unstable();
            for column in columns {
                write_string(buf, column);
            }
        }
    }
}

fn decode_indexed_columns(
    data: &[u8],
    offset: &mut usize,
) -> Result<Option<Vec<ColumnName>>, CatalogueEncodingError> {
    let encoded_count = read_u32(data, offset)?;
    if encoded_count == u32::MAX {
        return Ok(None);
    }
    let count = bound_count(data, offset, encoded_count, "indexed_columns")?;

    let mut columns = Vec::with_capacity(count);
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let column = read_string(data, offset, "indexed_column")?;
        if previous
            .as_ref()
            .is_some_and(|previous| previous.as_bytes() >= column.as_bytes())
        {
            return Err(CatalogueEncodingError::DecodeError {
                message: "indexed columns must be strictly byte-ordered".to_owned(),
            });
        }
        previous = Some(column.clone());
        columns.push(ColumnName::new(column));
    }
    Ok(Some(columns))
}

fn decode_current_schema(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    let mut offset = 1;
    let schema_version = data[0];
    let table_count = read_count(data, &mut offset, "schema_tables")?;

    let mut schema = Schema::new();
    for _ in 0..table_count {
        let (name, table_schema) = decode_table_entry(data, &mut offset, schema_version)?;
        schema.insert(name, table_schema);
    }

    ensure_consumed(data, offset)?;
    Ok(schema)
}

fn encode_row_descriptor(buf: &mut Vec<u8>, desc: &RowDescriptor) {
    write_u32(buf, desc.columns.len() as u32);
    for col in &desc.columns {
        encode_column_descriptor(buf, col);
    }
}

fn decode_row_descriptor(
    data: &[u8],
    offset: &mut usize,
    schema_version: u8,
) -> Result<RowDescriptor, CatalogueEncodingError> {
    let count = read_count(data, offset, "row_descriptor_columns")?;
    let mut columns = Vec::with_capacity(count);

    for _ in 0..count {
        columns.push(decode_column_descriptor(data, offset, schema_version)?);
    }

    ensure_unique_names(
        columns.iter().map(|column| column.name.as_str()),
        "row descriptor column",
    )?;

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
    match &col.default {
        Some(default) => {
            buf.push(1);
            encode_value(buf, default);
        }
        None => buf.push(0),
    }
    match col.merge_strategy {
        Some(ColumnMergeStrategy::Counter) => {
            buf.push(1);
            buf.push(1);
        }
        Some(ColumnMergeStrategy::GSet) => {
            buf.push(1);
            buf.push(2);
        }
        None => buf.push(0),
    }
}

fn decode_column_descriptor(
    data: &[u8],
    offset: &mut usize,
    schema_version: u8,
) -> Result<ColumnDescriptor, CatalogueEncodingError> {
    let name = read_string(data, offset, "column_name")?;
    let column_type = decode_column_type(data, offset, schema_version)?;
    let nullable = read_u8(data, offset)? != 0;
    let has_ref = read_u8(data, offset)? != 0;
    let references = if has_ref {
        Some(TableName::new(read_string(data, offset, "column_ref")?))
    } else {
        None
    };
    let has_default = read_u8(data, offset)? != 0;
    let default = if has_default {
        Some(decode_value(data, offset)?)
    } else {
        None
    };
    let has_merge_strategy = read_u8(data, offset)? != 0;
    let merge_strategy = if has_merge_strategy {
        match read_u8(data, offset)? {
            1 => Some(ColumnMergeStrategy::Counter),
            2 => Some(ColumnMergeStrategy::GSet),
            tag => {
                return Err(CatalogueEncodingError::InvalidTypeTag {
                    tag,
                    context: "column_merge_strategy",
                });
            }
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
const TYPE_TRANSACTION_ID: u8 = 13;
const TYPE_ENUM_PAYLOAD: u8 = 14;
const TYPE_SCALAR_ENUM: u8 = 15;
const TYPE_CATALOGUE_ENUM_PAYLOAD: u8 = 16;

fn encode_column_type(buf: &mut Vec<u8>, col_type: &ColumnType) {
    match col_type {
        ColumnType::Integer => buf.push(TYPE_INTEGER),
        ColumnType::BigInt => buf.push(TYPE_BIGINT),
        ColumnType::Double => buf.push(TYPE_DOUBLE),
        ColumnType::Boolean => buf.push(TYPE_BOOLEAN),
        ColumnType::Text => buf.push(TYPE_TEXT),
        ColumnType::Timestamp => buf.push(TYPE_TIMESTAMP),
        ColumnType::Uuid => buf.push(TYPE_UUID),
        ColumnType::TransactionId => buf.push(TYPE_TRANSACTION_ID),
        ColumnType::Bytea => buf.push(TYPE_BYTEA),
        ColumnType::Json { schema } => {
            buf.push(TYPE_JSON);
            match schema {
                Some(schema) => {
                    buf.push(1);
                    encode_canonical_json_value(buf, schema);
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
        ColumnType::ScalarEnum { name, variants } => {
            buf.push(TYPE_SCALAR_ENUM);
            write_string(buf, name);
            write_u32(buf, variants.len() as u32);
            for variant in variants {
                write_string(buf, variant);
            }
        }
        ColumnType::EnumPayload { cases } => {
            buf.push(TYPE_ENUM_PAYLOAD);
            write_u32(buf, cases.len() as u32);
            for case in cases {
                write_string(buf, &case.name);
                encode_row_descriptor(buf, &RowDescriptor::new(case.fields.clone()));
            }
        }
        ColumnType::CatalogueEnumPayload { name, cases } => {
            buf.push(TYPE_CATALOGUE_ENUM_PAYLOAD);
            write_string(buf, name);
            write_u32(buf, cases.len() as u32);
            for case in cases {
                write_string(buf, &case.name);
                encode_row_descriptor(buf, &RowDescriptor::new(case.fields.clone()));
            }
        }
        ColumnType::Array { element: elem } => {
            buf.push(TYPE_ARRAY);
            encode_column_type(buf, elem);
        }
        ColumnType::Row { columns: desc } => {
            buf.push(TYPE_ROW);
            encode_row_descriptor(buf, desc);
        }
    }
}

fn decode_column_type(
    data: &[u8],
    offset: &mut usize,
    schema_version: u8,
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
        TYPE_TRANSACTION_ID => Ok(ColumnType::TransactionId),
        TYPE_BYTEA => Ok(ColumnType::Bytea),
        TYPE_JSON => {
            let has_schema = read_flag(data, offset, "json_schema_presence")?;
            if has_schema {
                let schema = decode_canonical_json_value(data, offset)?;
                Ok(ColumnType::Json {
                    schema: Some(schema),
                })
            } else {
                Ok(ColumnType::Json { schema: None })
            }
        }
        TYPE_ENUM => {
            let variant_count = read_count(data, offset, "enum_variants")?;
            let mut variants = Vec::with_capacity(variant_count);
            for _ in 0..variant_count {
                variants.push(read_string(data, offset, "enum_variant")?);
            }
            ensure_unique_names(variants.iter().map(String::as_str), "enum variant")?;
            Ok(ColumnType::Enum { variants })
        }
        TYPE_SCALAR_ENUM => {
            let name = read_string(data, offset, "scalar_enum_name")?;
            let variant_count = read_count(data, offset, "scalar_enum_variants")?;
            let mut variants = Vec::with_capacity(variant_count);
            for _ in 0..variant_count {
                variants.push(read_string(data, offset, "scalar_enum_variant")?);
            }
            ensure_unique_names(variants.iter().map(String::as_str), "scalar enum variant")?;
            Ok(ColumnType::ScalarEnum { name, variants })
        }
        TYPE_ENUM_PAYLOAD => {
            let count = read_count(data, offset, "enum_payload_cases")?;
            let mut cases = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_string(data, offset, "enum_case")?;
                let fields = decode_row_descriptor(data, offset, schema_version)?.columns;
                cases.push(EnumCaseDescriptor { name, fields });
            }
            ensure_unique_names(
                cases.iter().map(|case| case.name.as_str()),
                "enum payload case",
            )?;
            Ok(ColumnType::EnumPayload { cases })
        }
        TYPE_CATALOGUE_ENUM_PAYLOAD => {
            let name = read_string(data, offset, "catalogue_enum_payload_name")?;
            let count = read_count(data, offset, "catalogue_enum_payload_cases")?;
            let mut cases = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_string(data, offset, "catalogue_enum_payload_case")?;
                let fields = decode_row_descriptor(data, offset, schema_version)?.columns;
                cases.push(EnumCaseDescriptor { name, fields });
            }
            ensure_unique_names(
                cases.iter().map(|case| case.name.as_str()),
                "catalogue enum payload case",
            )?;
            Ok(ColumnType::CatalogueEnumPayload { name, cases })
        }
        TYPE_ARRAY => {
            let elem = decode_column_type(data, offset, schema_version)?;
            Ok(ColumnType::Array {
                element: Box::new(elem),
            })
        }
        TYPE_ROW => {
            let desc = decode_row_descriptor(data, offset, schema_version)?;
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

    let transform = decode_current_lens_transform(data, version)?;
    ensure_canonical_payload(data, &encode_lens_transform(&transform), "lens")?;
    Ok(transform)
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

fn decode_lens_op(
    data: &[u8],
    offset: &mut usize,
    lens_version: u8,
) -> Result<LensOp, CatalogueEncodingError> {
    let schema_version = schema_version_for_lens_payload(lens_version);
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
            let column_type = decode_column_type(data, offset, schema_version)?;
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
            let column_type = decode_column_type(data, offset, schema_version)?;
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
            let schema = decode_table_schema(data, offset, lens_version)?;
            Ok(LensOp::AddTable { table, schema })
        }
        OP_REMOVE_TABLE => {
            let table = read_string(data, offset, "table")?;
            let schema = decode_table_schema(data, offset, lens_version)?;
            Ok(LensOp::RemoveTable { table, schema })
        }
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "lens_op",
        }),
    }
}

fn decode_current_lens_transform(
    data: &[u8],
    lens_version: u8,
) -> Result<LensTransform, CatalogueEncodingError> {
    let mut offset = 1;

    let op_count = read_count(data, &mut offset, "lens_ops")?;
    let mut ops = Vec::with_capacity(op_count);
    for _ in 0..op_count {
        ops.push(decode_lens_op(data, &mut offset, lens_version)?);
    }

    let draft_count = read_count(data, &mut offset, "lens_draft_ops")?;
    let mut draft_ops = Vec::with_capacity(draft_count);
    for _ in 0..draft_count {
        draft_ops.push(read_u32(data, &mut offset)? as usize);
    }

    ensure_consumed(data, offset)?;
    Ok(LensTransform { ops, draft_ops })
}

fn encode_table_schema(buf: &mut Vec<u8>, schema: &TableSchema) {
    encode_row_descriptor(buf, &schema.columns);
    encode_branch_bindings(buf, &schema.branch_by);
}

fn decode_table_schema(
    data: &[u8],
    offset: &mut usize,
    lens_version: u8,
) -> Result<TableSchema, CatalogueEncodingError> {
    let schema_version = schema_version_for_lens_payload(lens_version);
    let descriptor = decode_row_descriptor(data, offset, schema_version)?;
    let branch_by = decode_branch_bindings(data, offset)?;
    Ok(TableSchema {
        columns: descriptor,
        indexed_columns: None,
        policies: TablePolicies::default(),
        branch_by,
    })
}

fn schema_version_for_lens_payload(_lens_version: u8) -> u8 {
    SCHEMA_VERSION
}

// ============================================================================
// Canonical nested payload codec
// ============================================================================

// JSON Schema declarations and relation-backed policies are restart-authoritative
// nested values.  They deliberately share this small, explicit v1 algebra instead
// of delegating their bytes to serde.  The outer schema/lens/permissions envelope
// supplies the storage family; this marker freezes the nested record/enum grammar.
const NESTED_CODEC_VERSION: u8 = 1;

const JSON_NULL: u8 = 1;
const JSON_FALSE: u8 = 2;
const JSON_TRUE: u8 = 3;
const JSON_NUMBER: u8 = 4;
const JSON_STRING: u8 = 5;
const JSON_ARRAY: u8 = 6;
const JSON_OBJECT: u8 = 7;

fn encode_canonical_json_value(buf: &mut Vec<u8>, value: &serde_json::Value) {
    buf.push(NESTED_CODEC_VERSION);
    encode_canonical_json_value_body(buf, value);
}

fn encode_canonical_json_value_body(buf: &mut Vec<u8>, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => buf.push(JSON_NULL),
        serde_json::Value::Bool(false) => buf.push(JSON_FALSE),
        serde_json::Value::Bool(true) => buf.push(JSON_TRUE),
        serde_json::Value::Number(number) => {
            buf.push(JSON_NUMBER);
            // serde_json::Number has one normalized textual representation after
            // parsing.  Store that exact primitive rather than JSON object bytes.
            write_string(buf, &number.to_string());
        }
        serde_json::Value::String(value) => {
            buf.push(JSON_STRING);
            write_string(buf, value);
        }
        serde_json::Value::Array(values) => {
            buf.push(JSON_ARRAY);
            write_u32(buf, values.len() as u32);
            for value in values {
                encode_canonical_json_value_body(buf, value);
            }
        }
        serde_json::Value::Object(values) => {
            buf.push(JSON_OBJECT);
            write_u32(buf, values.len() as u32);
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, value) in entries {
                write_string(buf, key);
                encode_canonical_json_value_body(buf, value);
            }
        }
    }
}

fn decode_canonical_json_value(
    data: &[u8],
    offset: &mut usize,
) -> Result<serde_json::Value, CatalogueEncodingError> {
    let start = *offset;
    let version = read_u8(data, offset)?;
    if version != NESTED_CODEC_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: NESTED_CODEC_VERSION,
        });
    }
    let value = decode_canonical_json_value_body(data, offset)?;
    let mut canonical = Vec::new();
    encode_canonical_json_value(&mut canonical, &value);
    ensure_canonical_segment(data, start, *offset, &canonical, "json schema")?;
    Ok(value)
}

fn decode_canonical_json_value_body(
    data: &[u8],
    offset: &mut usize,
) -> Result<serde_json::Value, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        JSON_NULL => Ok(serde_json::Value::Null),
        JSON_FALSE => Ok(serde_json::Value::Bool(false)),
        JSON_TRUE => Ok(serde_json::Value::Bool(true)),
        JSON_NUMBER => {
            let source = read_string(data, offset, "json_number")?;
            let number = serde_json::from_str::<serde_json::Number>(&source).map_err(|error| {
                CatalogueEncodingError::DecodeError {
                    message: format!("invalid canonical JSON number: {error}"),
                }
            })?;
            if number.to_string() != source {
                return Err(CatalogueEncodingError::DecodeError {
                    message: "non-canonical JSON number spelling".to_owned(),
                });
            }
            Ok(serde_json::Value::Number(number))
        }
        JSON_STRING => Ok(serde_json::Value::String(read_string(
            data,
            offset,
            "json_string",
        )?)),
        JSON_ARRAY => {
            let count = read_count(data, offset, "json_array")?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_canonical_json_value_body(data, offset)?);
            }
            Ok(serde_json::Value::Array(values))
        }
        JSON_OBJECT => {
            let count = read_count(data, offset, "json_object")?;
            let mut values = serde_json::Map::new();
            let mut previous_key: Option<String> = None;
            for _ in 0..count {
                let key = read_string(data, offset, "json_object_key")?;
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous.as_bytes() >= key.as_bytes())
                {
                    return Err(CatalogueEncodingError::DecodeError {
                        message: "JSON object keys must be strictly byte-ordered".to_owned(),
                    });
                }
                let value = decode_canonical_json_value_body(data, offset)?;
                previous_key = Some(key.clone());
                values.insert(key, value);
            }
            Ok(serde_json::Value::Object(values))
        }
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "canonical_json_value",
        }),
    }
}

const REL_TABLE_SCAN: u8 = 1;
const REL_FILTER: u8 = 2;
const REL_UNION: u8 = 3;
const REL_JOIN: u8 = 4;
const REL_PROJECT: u8 = 5;
const REL_GATHER: u8 = 6;

const REL_PREDICATE_CMP: u8 = 1;
const REL_PREDICATE_IS_NULL: u8 = 2;
const REL_PREDICATE_IS_NOT_NULL: u8 = 3;
const REL_PREDICATE_IN: u8 = 4;
const REL_PREDICATE_CONTAINS: u8 = 5;
const REL_PREDICATE_ENUM_MATCH: u8 = 6;
const REL_PREDICATE_AND: u8 = 7;
const REL_PREDICATE_OR: u8 = 8;
const REL_PREDICATE_NOT: u8 = 9;
const REL_PREDICATE_TRUE: u8 = 10;
const REL_PREDICATE_FALSE: u8 = 11;

const REL_VALUE_LITERAL: u8 = 1;
const REL_VALUE_SESSION_REF: u8 = 2;
const REL_VALUE_OUTER_COLUMN: u8 = 3;
const REL_VALUE_ROW_ID: u8 = 4;

const REL_ROW_ID_CURRENT: u8 = 1;
const REL_ROW_ID_OUTER: u8 = 2;
const REL_ROW_ID_FRONTIER: u8 = 3;

const REL_KEY_COLUMN: u8 = 1;
const REL_KEY_ROW_ID: u8 = 2;
const REL_PROJECT_COLUMN: u8 = 1;
const REL_PROJECT_ROW_ID: u8 = 2;
const REL_JOIN_INNER: u8 = 1;
const REL_JOIN_LEFT: u8 = 2;
const REL_BOUND_FIXPOINT: u8 = 1;
const REL_BOUND_MAX_DEPTH: u8 = 2;

fn encode_canonical_relation_expr(buf: &mut Vec<u8>, rel: &RelExpr) {
    buf.push(NESTED_CODEC_VERSION);
    encode_relation_expr_body(buf, rel);
}

fn encode_relation_expr_body(buf: &mut Vec<u8>, rel: &RelExpr) {
    match rel {
        RelExpr::TableScan { table, alias } => {
            buf.push(REL_TABLE_SCAN);
            write_string(buf, table.as_str());
            encode_optional_string(buf, alias.as_deref());
        }
        RelExpr::Filter { input, predicate } => {
            buf.push(REL_FILTER);
            encode_relation_expr_body(buf, input);
            encode_relation_predicate(buf, predicate);
        }
        RelExpr::Union { inputs } => {
            buf.push(REL_UNION);
            write_u32(buf, inputs.len() as u32);
            for input in inputs {
                encode_relation_expr_body(buf, input);
            }
        }
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => {
            buf.push(REL_JOIN);
            encode_relation_expr_body(buf, left);
            encode_relation_expr_body(buf, right);
            write_u32(buf, on.len() as u32);
            for condition in on {
                encode_relation_column_ref(buf, &condition.left);
                encode_relation_column_ref(buf, &condition.right);
            }
            buf.push(match join_kind {
                RelJoinKind::Inner => REL_JOIN_INNER,
                RelJoinKind::Left => REL_JOIN_LEFT,
            });
        }
        RelExpr::Project { input, columns } => {
            buf.push(REL_PROJECT);
            encode_relation_expr_body(buf, input);
            write_u32(buf, columns.len() as u32);
            for column in columns {
                write_string(buf, &column.alias);
                encode_relation_project_expr(buf, &column.expr);
            }
        }
        RelExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => {
            buf.push(REL_GATHER);
            encode_relation_expr_body(buf, seed);
            encode_relation_expr_body(buf, step);
            encode_relation_key_ref(buf, frontier_key);
            encode_relation_bound(buf, bound);
            write_u32(buf, dedupe_key.len() as u32);
            for key in dedupe_key {
                encode_relation_key_ref(buf, key);
            }
        }
    }
}

fn decode_canonical_relation_expr(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelExpr, CatalogueEncodingError> {
    let start = *offset;
    let version = read_u8(data, offset)?;
    if version != NESTED_CODEC_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: NESTED_CODEC_VERSION,
        });
    }
    let rel = decode_relation_expr_body(data, offset)?;
    let mut canonical = Vec::new();
    encode_canonical_relation_expr(&mut canonical, &rel);
    ensure_canonical_segment(data, start, *offset, &canonical, "relation policy")?;
    Ok(rel)
}

fn decode_relation_expr_body(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelExpr, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_TABLE_SCAN => Ok(RelExpr::TableScan {
            table: TableName::new(read_string(data, offset, "relation_table")?),
            alias: decode_optional_string(data, offset, "relation_alias")?,
        }),
        REL_FILTER => Ok(RelExpr::Filter {
            input: Box::new(decode_relation_expr_body(data, offset)?),
            predicate: decode_relation_predicate(data, offset)?,
        }),
        REL_UNION => {
            let count = read_count(data, offset, "relation_union")?;
            let mut inputs = Vec::with_capacity(count);
            for _ in 0..count {
                inputs.push(decode_relation_expr_body(data, offset)?);
            }
            Ok(RelExpr::Union { inputs })
        }
        REL_JOIN => {
            let left = Box::new(decode_relation_expr_body(data, offset)?);
            let right = Box::new(decode_relation_expr_body(data, offset)?);
            let count = read_count(data, offset, "relation_join_conditions")?;
            let mut on = Vec::with_capacity(count);
            for _ in 0..count {
                on.push(RelJoinCondition {
                    left: decode_relation_column_ref(data, offset)?,
                    right: decode_relation_column_ref(data, offset)?,
                });
            }
            let join_kind = match read_u8(data, offset)? {
                REL_JOIN_INNER => RelJoinKind::Inner,
                REL_JOIN_LEFT => RelJoinKind::Left,
                tag => {
                    return Err(CatalogueEncodingError::InvalidTypeTag {
                        tag,
                        context: "relation_join_kind",
                    });
                }
            };
            Ok(RelExpr::Join {
                left,
                right,
                on,
                join_kind,
            })
        }
        REL_PROJECT => {
            let input = Box::new(decode_relation_expr_body(data, offset)?);
            let count = read_count(data, offset, "relation_project_columns")?;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(RelProjectColumn {
                    alias: read_string(data, offset, "relation_project_alias")?,
                    expr: decode_relation_project_expr(data, offset)?,
                });
            }
            Ok(RelExpr::Project { input, columns })
        }
        REL_GATHER => {
            let seed = Box::new(decode_relation_expr_body(data, offset)?);
            let step = Box::new(decode_relation_expr_body(data, offset)?);
            let frontier_key = decode_relation_key_ref(data, offset)?;
            let bound = decode_relation_bound(data, offset)?;
            let count = read_count(data, offset, "relation_dedupe_keys")?;
            let mut dedupe_key = Vec::with_capacity(count);
            for _ in 0..count {
                dedupe_key.push(decode_relation_key_ref(data, offset)?);
            }
            Ok(RelExpr::Gather {
                seed,
                step,
                frontier_key,
                bound,
                dedupe_key,
            })
        }
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_expr",
        }),
    }
}

fn encode_relation_predicate(buf: &mut Vec<u8>, predicate: &RelPredicateExpr) {
    match predicate {
        RelPredicateExpr::Cmp { left, op, right } => {
            buf.push(REL_PREDICATE_CMP);
            encode_relation_column_ref(buf, left);
            encode_relation_cmp_op(buf, *op);
            encode_relation_value_ref(buf, right);
        }
        RelPredicateExpr::IsNull { column } => {
            buf.push(REL_PREDICATE_IS_NULL);
            encode_relation_column_ref(buf, column);
        }
        RelPredicateExpr::IsNotNull { column } => {
            buf.push(REL_PREDICATE_IS_NOT_NULL);
            encode_relation_column_ref(buf, column);
        }
        RelPredicateExpr::In { left, values } => {
            buf.push(REL_PREDICATE_IN);
            encode_relation_column_ref(buf, left);
            write_u32(buf, values.len() as u32);
            for value in values {
                encode_relation_value_ref(buf, value);
            }
        }
        RelPredicateExpr::Contains { left, right } => {
            buf.push(REL_PREDICATE_CONTAINS);
            encode_relation_column_ref(buf, left);
            encode_relation_value_ref(buf, right);
        }
        RelPredicateExpr::EnumMatch {
            column,
            case,
            payload,
        } => {
            buf.push(REL_PREDICATE_ENUM_MATCH);
            encode_relation_column_ref(buf, column);
            write_string(buf, case);
            encode_relation_predicate(buf, payload);
        }
        RelPredicateExpr::And(expressions) => {
            buf.push(REL_PREDICATE_AND);
            write_u32(buf, expressions.len() as u32);
            for expression in expressions {
                encode_relation_predicate(buf, expression);
            }
        }
        RelPredicateExpr::Or(expressions) => {
            buf.push(REL_PREDICATE_OR);
            write_u32(buf, expressions.len() as u32);
            for expression in expressions {
                encode_relation_predicate(buf, expression);
            }
        }
        RelPredicateExpr::Not(expression) => {
            buf.push(REL_PREDICATE_NOT);
            encode_relation_predicate(buf, expression);
        }
        RelPredicateExpr::True => buf.push(REL_PREDICATE_TRUE),
        RelPredicateExpr::False => buf.push(REL_PREDICATE_FALSE),
    }
}

fn decode_relation_predicate(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelPredicateExpr, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_PREDICATE_CMP => Ok(RelPredicateExpr::Cmp {
            left: decode_relation_column_ref(data, offset)?,
            op: decode_relation_cmp_op(data, offset)?,
            right: decode_relation_value_ref(data, offset)?,
        }),
        REL_PREDICATE_IS_NULL => Ok(RelPredicateExpr::IsNull {
            column: decode_relation_column_ref(data, offset)?,
        }),
        REL_PREDICATE_IS_NOT_NULL => Ok(RelPredicateExpr::IsNotNull {
            column: decode_relation_column_ref(data, offset)?,
        }),
        REL_PREDICATE_IN => {
            let left = decode_relation_column_ref(data, offset)?;
            let count = read_count(data, offset, "relation_predicate_in")?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_relation_value_ref(data, offset)?);
            }
            Ok(RelPredicateExpr::In { left, values })
        }
        REL_PREDICATE_CONTAINS => Ok(RelPredicateExpr::Contains {
            left: decode_relation_column_ref(data, offset)?,
            right: decode_relation_value_ref(data, offset)?,
        }),
        REL_PREDICATE_ENUM_MATCH => Ok(RelPredicateExpr::EnumMatch {
            column: decode_relation_column_ref(data, offset)?,
            case: read_string(data, offset, "relation_enum_case")?,
            payload: Box::new(decode_relation_predicate(data, offset)?),
        }),
        REL_PREDICATE_AND => {
            let count = read_count(data, offset, "relation_predicate_and")?;
            let mut expressions = Vec::with_capacity(count);
            for _ in 0..count {
                expressions.push(decode_relation_predicate(data, offset)?);
            }
            Ok(RelPredicateExpr::And(expressions))
        }
        REL_PREDICATE_OR => {
            let count = read_count(data, offset, "relation_predicate_or")?;
            let mut expressions = Vec::with_capacity(count);
            for _ in 0..count {
                expressions.push(decode_relation_predicate(data, offset)?);
            }
            Ok(RelPredicateExpr::Or(expressions))
        }
        REL_PREDICATE_NOT => Ok(RelPredicateExpr::Not(Box::new(decode_relation_predicate(
            data, offset,
        )?))),
        REL_PREDICATE_TRUE => Ok(RelPredicateExpr::True),
        REL_PREDICATE_FALSE => Ok(RelPredicateExpr::False),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_predicate",
        }),
    }
}

fn encode_relation_column_ref(buf: &mut Vec<u8>, column: &RelColumnRef) {
    encode_optional_string(buf, column.scope.as_deref());
    write_string(buf, &column.column);
}

fn decode_relation_column_ref(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelColumnRef, CatalogueEncodingError> {
    Ok(RelColumnRef {
        scope: decode_optional_string(data, offset, "relation_column_scope")?,
        column: read_string(data, offset, "relation_column")?,
    })
}

fn encode_relation_value_ref(buf: &mut Vec<u8>, value: &RelValueRef) {
    match value {
        RelValueRef::Literal(value) => {
            buf.push(REL_VALUE_LITERAL);
            encode_value(buf, value);
        }
        RelValueRef::SessionRef(path) => {
            buf.push(REL_VALUE_SESSION_REF);
            encode_string_list(buf, path);
        }
        RelValueRef::OuterColumn(column) => {
            buf.push(REL_VALUE_OUTER_COLUMN);
            encode_relation_column_ref(buf, column);
        }
        RelValueRef::RowId(row_id) => {
            buf.push(REL_VALUE_ROW_ID);
            encode_relation_row_id(buf, row_id);
        }
    }
}

fn decode_relation_value_ref(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelValueRef, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_VALUE_LITERAL => Ok(RelValueRef::Literal(decode_value(data, offset)?)),
        REL_VALUE_SESSION_REF => Ok(RelValueRef::SessionRef(decode_string_list(
            data,
            offset,
            "relation_session_path",
        )?)),
        REL_VALUE_OUTER_COLUMN => Ok(RelValueRef::OuterColumn(decode_relation_column_ref(
            data, offset,
        )?)),
        REL_VALUE_ROW_ID => Ok(RelValueRef::RowId(decode_relation_row_id(data, offset)?)),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_value_ref",
        }),
    }
}

fn encode_relation_cmp_op(buf: &mut Vec<u8>, op: RelPredicateCmpOp) {
    buf.push(match op {
        RelPredicateCmpOp::Eq => 1,
        RelPredicateCmpOp::Ne => 2,
        RelPredicateCmpOp::Lt => 3,
        RelPredicateCmpOp::Le => 4,
        RelPredicateCmpOp::Gt => 5,
        RelPredicateCmpOp::Ge => 6,
    });
}

fn decode_relation_cmp_op(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelPredicateCmpOp, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        1 => Ok(RelPredicateCmpOp::Eq),
        2 => Ok(RelPredicateCmpOp::Ne),
        3 => Ok(RelPredicateCmpOp::Lt),
        4 => Ok(RelPredicateCmpOp::Le),
        5 => Ok(RelPredicateCmpOp::Gt),
        6 => Ok(RelPredicateCmpOp::Ge),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_cmp_op",
        }),
    }
}

fn encode_relation_row_id(buf: &mut Vec<u8>, row_id: &RowIdRef) {
    buf.push(match row_id {
        RowIdRef::Current => REL_ROW_ID_CURRENT,
        RowIdRef::Outer => REL_ROW_ID_OUTER,
        RowIdRef::Frontier => REL_ROW_ID_FRONTIER,
    });
}

fn decode_relation_row_id(
    data: &[u8],
    offset: &mut usize,
) -> Result<RowIdRef, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_ROW_ID_CURRENT => Ok(RowIdRef::Current),
        REL_ROW_ID_OUTER => Ok(RowIdRef::Outer),
        REL_ROW_ID_FRONTIER => Ok(RowIdRef::Frontier),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_row_id",
        }),
    }
}

fn encode_relation_key_ref(buf: &mut Vec<u8>, key: &RelKeyRef) {
    match key {
        RelKeyRef::Column(column) => {
            buf.push(REL_KEY_COLUMN);
            encode_relation_column_ref(buf, column);
        }
        RelKeyRef::RowId(row_id) => {
            buf.push(REL_KEY_ROW_ID);
            encode_relation_row_id(buf, row_id);
        }
    }
}

fn decode_relation_key_ref(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelKeyRef, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_KEY_COLUMN => Ok(RelKeyRef::Column(decode_relation_column_ref(data, offset)?)),
        REL_KEY_ROW_ID => Ok(RelKeyRef::RowId(decode_relation_row_id(data, offset)?)),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_key_ref",
        }),
    }
}

fn encode_relation_project_expr(buf: &mut Vec<u8>, expression: &RelProjectExpr) {
    match expression {
        RelProjectExpr::Column(column) => {
            buf.push(REL_PROJECT_COLUMN);
            encode_relation_column_ref(buf, column);
        }
        RelProjectExpr::RowId(row_id) => {
            buf.push(REL_PROJECT_ROW_ID);
            encode_relation_row_id(buf, row_id);
        }
    }
}

fn decode_relation_project_expr(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelProjectExpr, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_PROJECT_COLUMN => Ok(RelProjectExpr::Column(decode_relation_column_ref(
            data, offset,
        )?)),
        REL_PROJECT_ROW_ID => Ok(RelProjectExpr::RowId(decode_relation_row_id(data, offset)?)),
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_project_expr",
        }),
    }
}

fn encode_relation_bound(buf: &mut Vec<u8>, bound: &RelRecursionBound) {
    match bound {
        RelRecursionBound::Fixpoint => buf.push(REL_BOUND_FIXPOINT),
        RelRecursionBound::MaxDepth(depth) => {
            buf.push(REL_BOUND_MAX_DEPTH);
            write_u64(buf, *depth as u64);
        }
    }
}

fn decode_relation_bound(
    data: &[u8],
    offset: &mut usize,
) -> Result<RelRecursionBound, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        REL_BOUND_FIXPOINT => Ok(RelRecursionBound::Fixpoint),
        REL_BOUND_MAX_DEPTH => {
            let depth = read_u64(data, offset)?;
            let depth =
                usize::try_from(depth).map_err(|_| CatalogueEncodingError::DecodeError {
                    message: "relation recursion depth exceeds platform usize".to_owned(),
                })?;
            Ok(RelRecursionBound::MaxDepth(depth))
        }
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "relation_recursion_bound",
        }),
    }
}

fn encode_optional_string(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            buf.push(1);
            write_string(buf, value);
        }
        None => buf.push(0),
    }
}

fn decode_optional_string(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<Option<String>, CatalogueEncodingError> {
    if read_flag(data, offset, context)? {
        Ok(Some(read_string(data, offset, context)?))
    } else {
        Ok(None)
    }
}

fn encode_string_list(buf: &mut Vec<u8>, values: &[String]) {
    write_u32(buf, values.len() as u32);
    for value in values {
        write_string(buf, value);
    }
}

fn decode_string_list(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<Vec<String>, CatalogueEncodingError> {
    let count = read_count(data, offset, context)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_string(data, offset, context)?);
    }
    Ok(values)
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
    let table_count = read_count(data, &mut offset, "permissions_tables")?;
    let mut permissions = HashMap::new();

    for _ in 0..table_count {
        let table_name = TableName::new(read_string(data, &mut offset, "table_name")?);
        let policies = decode_table_policies(data, &mut offset)?;
        permissions.insert(table_name, policies);
    }

    ensure_consumed(data, offset)?;
    ensure_canonical_payload(data, &encode_permissions(&permissions), "permissions")?;
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
    if version != PERMISSIONS_BUNDLE_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: PERMISSIONS_BUNDLE_VERSION,
        });
    }

    let decoded = decode_current_permissions_bundle(data)?;
    ensure_canonical_payload(
        data,
        &encode_permissions_bundle(decoded.0, decoded.1, decoded.2, &decoded.3),
        "permissions bundle",
    )?;
    Ok(decoded)
}

fn decode_current_permissions_bundle(
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
    ensure_consumed(data, offset)?;
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
    if version != PERMISSIONS_HEAD_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: PERMISSIONS_HEAD_VERSION,
        });
    }

    let decoded = decode_current_permissions_head(data)?;
    ensure_canonical_payload(
        data,
        &encode_permissions_head(decoded.0, decoded.1, decoded.2, decoded.3),
        "permissions head",
    )?;
    Ok(decoded)
}

fn decode_current_permissions_head(
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
    ensure_consumed(data, offset)?;
    Ok((
        schema_hash,
        version,
        parent_bundle_object_id,
        ObjectId::from_uuid(bundle_uuid),
    ))
}

fn encode_operation_policy(
    buf: &mut Vec<u8>,
    policy: &jazz::tools::public_schema::OperationPolicy,
) {
    encode_optional_policy_expr(buf, policy.using.as_ref());
    encode_optional_policy_expr(buf, policy.with_check.as_ref());
}

fn decode_operation_policy(
    data: &[u8],
    offset: &mut usize,
) -> Result<jazz::tools::public_schema::OperationPolicy, CatalogueEncodingError> {
    Ok(jazz::tools::public_schema::OperationPolicy {
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
            encode_canonical_relation_expr(buf, rel);
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
            let count = read_count(data, offset, "policy_session_cmp_path")?;
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
            let count = read_count(data, offset, "policy_session_is_null_path")?;
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
            let count = read_count(data, offset, "policy_session_is_not_null_path")?;
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
            let count = read_count(data, offset, "policy_session_contains_path")?;
            let mut path = Vec::with_capacity(count);
            for _ in 0..count {
                path.push(read_string(data, offset, "policy_session_contains_path")?);
            }
            let value = decode_value(data, offset)?;
            Ok(PolicyExpr::SessionContains { path, value })
        }
        POLICY_EXPR_IN => {
            let column = read_string(data, offset, "policy_in_column")?;
            let count = read_count(data, offset, "policy_in_session_path")?;
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
            let count = read_count(data, offset, "policy_in_list_values")?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_policy_value(data, offset)?);
            }
            Ok(PolicyExpr::InList { column, values })
        }
        POLICY_EXPR_SESSION_IN_LIST => {
            let path_count = read_count(data, offset, "policy_session_in_list_path")?;
            let mut path = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                path.push(read_string(data, offset, "policy_session_in_list_path")?);
            }
            let value_count = read_count(data, offset, "policy_session_in_list_values")?;
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
            let rel = decode_canonical_relation_expr(data, offset)?;
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
            let count = read_count(data, offset, "policy_and")?;
            let mut exprs = Vec::with_capacity(count);
            for _ in 0..count {
                exprs.push(decode_policy_expr(data, offset)?);
            }
            Ok(PolicyExpr::And(exprs))
        }
        POLICY_EXPR_OR => {
            let count = read_count(data, offset, "policy_or")?;
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
            let count = read_count(data, offset, "policy_session_ref_path")?;
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
const VALUE_ENUM: u8 = 13;

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
        Value::TransactionId(bytes) => {
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
        Value::Enum { case, values } => {
            buf.push(VALUE_ENUM);
            write_string(buf, case);
            write_u32(buf, values.len() as u32);
            for value in values {
                encode_value(buf, value);
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
            Ok(Value::TransactionId(
                bytes.try_into().expect("16-byte batch id"),
            ))
        }
        VALUE_BYTEA => {
            let len = read_u32(data, offset)? as usize;
            let bytes = read_bytes(data, offset, len)?;
            Ok(Value::Bytea(bytes.to_vec()))
        }
        VALUE_ARRAY => {
            let count = read_count(data, offset, "value_array")?;
            let mut elements = Vec::with_capacity(count);
            for _ in 0..count {
                elements.push(decode_value(data, offset)?);
            }
            Ok(Value::Array(elements))
        }
        VALUE_ROW => {
            let count = read_count(data, offset, "value_row")?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_value(data, offset)?);
            }
            Ok(Value::Row { id: None, values })
        }
        VALUE_ENUM => {
            let case = read_string(data, offset, "enum_value_case")?;
            let count = read_count(data, offset, "value_enum")?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_value(data, offset)?);
            }
            Ok(Value::Enum { case, values })
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

fn ensure_consumed(data: &[u8], offset: usize) -> Result<(), CatalogueEncodingError> {
    if offset == data.len() {
        return Ok(());
    }
    Err(CatalogueEncodingError::DecodeError {
        message: format!(
            "trailing data after decoded payload: {} bytes remain",
            data.len() - offset
        ),
    })
}

fn ensure_canonical_payload(
    actual: &[u8],
    canonical: &[u8],
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    if actual == canonical {
        return Ok(());
    }
    Err(CatalogueEncodingError::DecodeError {
        message: format!("non-canonical {context} payload"),
    })
}

fn ensure_canonical_segment(
    data: &[u8],
    start: usize,
    end: usize,
    canonical: &[u8],
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    ensure_canonical_payload(&data[start..end], canonical, context)
}

fn read_flag(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<bool, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        tag => Err(CatalogueEncodingError::InvalidTypeTag { tag, context }),
    }
}

fn read_count(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<usize, CatalogueEncodingError> {
    let count = read_u32(data, offset)?;
    bound_count(data, offset, count, context)
}

fn bound_count(
    data: &[u8],
    offset: &usize,
    count: u32,
    context: &'static str,
) -> Result<usize, CatalogueEncodingError> {
    let count = count as usize;
    // Every nested record/list item has at least one tag byte.  Reject absurd
    // declared cardinalities before Vec::with_capacity can turn corruption into
    // allocation pressure during restart.
    if count > data.len().saturating_sub(*offset) {
        return Err(CatalogueEncodingError::DecodeError {
            message: format!("{context} count exceeds remaining canonical payload"),
        });
    }
    Ok(count)
}

fn ensure_unique_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(CatalogueEncodingError::DecodeError {
                message: format!("duplicate {context}: {name}"),
            });
        }
    }
    Ok(())
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
    use jazz::tools::public_schema::SchemaBuilder;
    use jazz::tools::public_schema::{
        PolicyExpr, RelColumnRef, RelExpr, RelPredicateCmpOp, RelPredicateExpr, RelValueRef,
    };
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

    // This stays internal because it verifies the exact restart-authoritative
    // byte contract below the public catalogue API.
    #[test]
    fn nested_catalogue_payload_v1_goldens_are_exact() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("docs").column(
                "payload",
                ColumnType::Json {
                    schema: Some(json!({"z": null, "a": [true, 1]})),
                },
            ))
            .build();
        let schema_bytes = encode_schema(&schema);
        assert_eq!(
            hex(&schema_bytes),
            "010100000004000000646f637301000000070000007061796c6f61640c010107020000000100000061060200000003040100000031010000007a0100000000ffffffff00000000"
        );
        assert_eq!(decode_schema(&schema_bytes).unwrap(), schema);

        let lens = LensTransform::new();
        assert_eq!(hex(&encode_lens_transform(&lens)), "010000000000000000");
        let decoded_lens = decode_lens_transform(&encode_lens_transform(&lens)).unwrap();
        assert!(decoded_lens.ops.is_empty());
        assert!(decoded_lens.draft_ops.is_empty());

        let rel = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("members"),
                alias: Some("m".to_owned()),
            }),
            predicate: RelPredicateExpr::Cmp {
                left: RelColumnRef::unscoped("owner_id"),
                op: RelPredicateCmpOp::Eq,
                right: RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
            },
        };
        let permissions = HashMap::from([(
            TableName::new("docs"),
            TablePolicies::new().with_select(PolicyExpr::ExistsRel { rel: rel.clone() }),
        )]);
        let permission_bytes = encode_permissions(&permissions);
        assert_eq!(
            hex(&permission_bytes),
            "010100000004000000646f6373010d010201070000006d656d6265727301010000006d0100080000006f776e65725f696401020200000006000000636c61696d730300000073756200000000000000"
        );
        assert_eq!(decode_permissions(&permission_bytes).unwrap(), permissions);
    }

    #[test]
    fn nested_catalogue_payload_rejects_noncanonical_order_versions_and_suffixes() {
        // v1 JSON object with the semantic keys `b` then `a`: valid-looking
        // data, but not the required ascending UTF-8 byte order.
        let mut offset = 0;
        let unordered_json = [
            NESTED_CODEC_VERSION,
            JSON_OBJECT,
            2,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            b'b',
            JSON_NULL,
            1,
            0,
            0,
            0,
            b'a',
            JSON_NULL,
        ];
        assert!(decode_canonical_json_value(&unordered_json, &mut offset).is_err());

        let rel = RelExpr::TableScan {
            table: TableName::new("docs"),
            alias: None,
        };
        let permissions = HashMap::from([(
            TableName::new("docs"),
            TablePolicies::new().with_select(PolicyExpr::ExistsRel { rel }),
        )]);
        let mut bytes = encode_permissions(&permissions);
        bytes.push(0);
        assert!(decode_permissions(&bytes).is_err());

        // `HashMap` reconstruction would otherwise overwrite the first entry;
        // exact re-encoding makes this alternate duplicate-map spelling corrupt.
        let one_entry = encode_permissions(&permissions);
        let mut duplicate_entry = one_entry.clone();
        duplicate_entry[1..5].copy_from_slice(&2_u32.to_le_bytes());
        duplicate_entry.extend_from_slice(&one_entry[5..]);
        assert!(decode_permissions(&duplicate_entry).is_err());

        let mut unknown_nested_version = encode_permissions(&permissions);
        let nested_version = unknown_nested_version
            .windows(2)
            .position(|window| window == [POLICY_EXPR_EXISTS_REL, NESTED_CODEC_VERSION])
            .expect("ExistsRel nested version");
        unknown_nested_version[nested_version + 1] = NESTED_CODEC_VERSION + 1;
        assert!(decode_permissions(&unknown_nested_version).is_err());
    }

    // These are internal tests because the public failure boundary is the
    // catalogue scan; here we prove every binary collection decoder rejects a
    // hostile count before it can reserve memory during that scan.
    #[test]
    fn catalogue_payload_counts_are_bounded_before_collection_allocation() {
        let huge = u32::MAX.to_le_bytes();
        let with_huge = |tag: u8| [vec![tag], huge.to_vec()].concat();
        let with_empty_name_and_huge = |tag: u8| [vec![tag, 0, 0, 0, 0], huge.to_vec()].concat();

        let mut offset = 0;
        assert_count_bound(
            decode_policy_expr(&with_huge(POLICY_EXPR_SESSION_CMP), &mut offset),
            "policy_session_cmp_path",
        );
        for (tag, context) in [
            (POLICY_EXPR_SESSION_IS_NULL, "policy_session_is_null_path"),
            (
                POLICY_EXPR_SESSION_IS_NOT_NULL,
                "policy_session_is_not_null_path",
            ),
            (POLICY_EXPR_SESSION_CONTAINS, "policy_session_contains_path"),
            (POLICY_EXPR_AND, "policy_and"),
            (POLICY_EXPR_OR, "policy_or"),
        ] {
            let mut offset = 0;
            assert_count_bound(decode_policy_expr(&with_huge(tag), &mut offset), context);
        }
        let session_in_list_value_count = [
            vec![POLICY_EXPR_SESSION_IN_LIST],
            0_u32.to_le_bytes().to_vec(),
            huge.to_vec(),
        ]
        .concat();
        let mut offset = 0;
        assert_count_bound(
            decode_policy_expr(&session_in_list_value_count, &mut offset),
            "policy_session_in_list_values",
        );
        for (tag, context) in [
            (POLICY_EXPR_IN, "policy_in_session_path"),
            (POLICY_EXPR_IN_LIST, "policy_in_list_values"),
        ] {
            let mut offset = 0;
            assert_count_bound(
                decode_policy_expr(&with_empty_name_and_huge(tag), &mut offset),
                context,
            );
        }

        assert_count_bound(
            decode_permissions(&[PERMISSIONS_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "permissions_tables",
        );
        assert_count_bound(
            decode_schema(&[SCHEMA_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "schema_tables",
        );
        assert_count_bound(
            decode_lens_transform(&[LENS_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "lens_ops",
        );

        let mut offset = 0;
        assert_count_bound(
            decode_column_type(&with_huge(TYPE_ENUM), &mut offset, SCHEMA_VERSION),
            "enum_variants",
        );
        let mut offset = 0;
        assert_count_bound(
            decode_value(&with_huge(VALUE_ARRAY), &mut offset),
            "value_array",
        );
        let mut offset = 0;
        assert_count_bound(
            decode_policy_value(&with_huge(POLICY_VALUE_SESSION_REF), &mut offset),
            "policy_session_ref_path",
        );

        let mut offset = 0;
        assert!(matches!(
            decode_policy_expr(&[POLICY_EXPR_SESSION_CMP], &mut offset),
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));
        assert!(matches!(
            decode_permissions(&[PERMISSIONS_VERSION]),
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));

        // A one-item count with no item bytes is the planted-sensitivity case:
        // it must fail in `read_count`, not after a collection is allocated.
        assert_count_bound(
            decode_permissions(&[PERMISSIONS_VERSION, 1, 0, 0, 0]),
            "permissions_tables",
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
    fn schema_roundtrip_preserves_indexed_columns() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .index_only(["done"]),
            )
            .build();

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let todos = decoded
            .get(&TableName::new("todos"))
            .expect("decoded todos table");
        assert_eq!(todos.indexed_columns, Some(vec![ColumnName::new("done")]));
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
    fn schema_roundtrip_with_catalogue_native_enums() {
        let scalar_enum = ColumnType::ScalarEnum {
            name: "status".to_owned(),
            variants: vec!["todo".to_owned(), "done".to_owned()],
        };
        let payload_enum = ColumnType::CatalogueEnumPayload {
            name: "event".to_owned(),
            cases: vec![EnumCaseDescriptor {
                name: "renamed".to_owned(),
                fields: vec![ColumnDescriptor::new("title", ColumnType::Text)],
            }],
        };
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("events")
                    .column("status", scalar_enum.clone())
                    .column("event", payload_enum.clone()),
            )
            .build();

        let decoded = decode_schema(&encode_schema(&schema)).unwrap();
        let events = decoded.get(&TableName::new("events")).unwrap();
        assert_eq!(
            events.columns.column("status").unwrap().column_type,
            scalar_enum
        );
        assert_eq!(
            events.columns.column("event").unwrap().column_type,
            payload_enum
        );
    }

    #[test]
    fn schema_roundtrip_strips_policies_but_preserves_hash() {
        let schema =
            SchemaBuilder::new()
                .table(
                    TableSchema::builder("todos")
                        .column("id", ColumnType::Uuid)
                        .column("owner_id", ColumnType::Uuid)
                        .column("title", ColumnType::Text)
                        .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                            "owner_id",
                            vec!["user".to_owned()],
                        ))),
                )
                .build();

        let original_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let decoded_hash = jazz::tools::public_schema::SchemaHash::compute(&decoded);

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
    fn schema_roundtrip_preserves_branch_columns_and_hash() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("id", ColumnType::Uuid)
                    .column("workspace_id", ColumnType::Uuid)
                    .branch_by("workspace_id"),
            )
            .build();

        let decoded = decode_schema(&encode_schema(&schema)).expect("schema decodes");
        let todos = decoded.get(&TableName::new("todos")).expect("todos table");

        assert_eq!(todos.branch_by, vec![ColumnName::new("workspace_id")]);
        assert_eq!(SchemaHash::compute(&decoded), SchemaHash::compute(&schema));
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
                    PolicyValue::SessionRef(vec!["user".to_owned()]),
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
                path: vec!["user".to_owned()],
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
    fn storage_epoch_one_catalogue_envelopes_reject_pre_freeze_outer_labels() {
        assert!(matches!(
            decode_schema(&[12]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 12,
                expected: 1
            })
        ));
        assert!(matches!(
            decode_lens_transform(&[5]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 5,
                expected: 1
            })
        ));

        assert!(matches!(
            decode_permissions(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
        assert!(matches!(
            decode_permissions_bundle(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
        assert!(matches!(
            decode_permissions_head(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
    }

    #[test]
    fn decode_truncated_data() {
        let data = vec![SCHEMA_VERSION]; // Version only, no table count
        let result = decode_schema(&data);
        assert!(matches!(
            result,
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn assert_count_bound<T>(result: Result<T, CatalogueEncodingError>, context: &'static str) {
        match result {
            Err(CatalogueEncodingError::DecodeError { message }) => assert!(
                message.contains(context) && message.contains("count exceeds"),
                "expected bounded count error for {context}, got {message}"
            ),
            Err(error) => panic!("expected bounded count error for {context}, got {error}"),
            Ok(_) => panic!("huge untrusted count unexpectedly decoded for {context}"),
        }
    }
}
