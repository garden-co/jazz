use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::object::ObjectId;
use crate::query_api::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};
use uuid::Uuid;

/// Maximum payload size allowed for a single BYTEA value (1 MiB).
pub const BYTEA_MAX_BYTES: usize = 1_048_576;
const INVALID_UUID_TEXT_SENTINEL: [u8; 16] = [0xff; 16];

/// Encoding error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingError {
    /// Value count doesn't match column count.
    ColumnCountMismatch { expected: usize, actual: usize },
    /// Value type doesn't match column type.
    TypeMismatch {
        column: String,
        expected: ColumnType,
        actual: Option<ColumnType>,
    },
    /// Null value for non-nullable column.
    NullNotAllowed { column: String },
    /// Binary data is malformed or too short.
    MalformedData { message: String },
    /// BYTEA payload exceeds the configured per-cell limit.
    ByteaTooLarge {
        column: String,
        actual: usize,
        max: usize,
    },
    /// Column index out of bounds.
    ColumnIndexOutOfBounds { index: usize, max: usize },
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {expected}, got {actual}"
                )
            }
            EncodingError::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "type mismatch for column '{column}': expected {expected:?}, got {actual:?}"
                )
            }
            EncodingError::NullNotAllowed { column } => {
                write!(f, "null not allowed for column '{column}'")
            }
            EncodingError::MalformedData { message } => {
                write!(f, "malformed data: {message}")
            }
            EncodingError::ByteaTooLarge {
                column,
                actual,
                max,
            } => {
                write!(
                    f,
                    "bytea payload too large for column '{column}': {actual} bytes exceeds limit {max}"
                )
            }
            EncodingError::ColumnIndexOutOfBounds { index, max } => {
                write!(f, "column index {index} out of bounds (max {max})")
            }
        }
    }
}

impl std::error::Error for EncodingError {}

#[derive(Debug, Clone)]
struct CompiledColumnLayout {
    fixed_offset: Option<usize>,
    fixed_total_size: Option<usize>,
    fixed_value_size: Option<usize>,
    variable_index: Option<usize>,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct CompiledRowLayout {
    columns: Vec<CompiledColumnLayout>,
    fixed_section_size: usize,
    variable_column_count: usize,
}

fn compiled_row_layout_cache() -> &'static Mutex<HashMap<[u8; 32], Arc<CompiledRowLayout>>> {
    static CACHE: OnceLock<Mutex<HashMap<[u8; 32], Arc<CompiledRowLayout>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn compile_row_layout(descriptor: &RowDescriptor) -> CompiledRowLayout {
    let mut columns = Vec::with_capacity(descriptor.columns.len());
    let mut fixed_offset = 0usize;
    let mut variable_index = 0usize;

    for column in &descriptor.columns {
        if let Some(fixed_value_size) = column.column_type.fixed_size() {
            let fixed_total_size = fixed_value_size + usize::from(column.nullable);
            columns.push(CompiledColumnLayout {
                fixed_offset: Some(fixed_offset),
                fixed_total_size: Some(fixed_total_size),
                fixed_value_size: Some(fixed_value_size),
                variable_index: None,
                nullable: column.nullable,
            });
            fixed_offset += fixed_total_size;
        } else {
            columns.push(CompiledColumnLayout {
                fixed_offset: None,
                fixed_total_size: None,
                fixed_value_size: None,
                variable_index: Some(variable_index),
                nullable: column.nullable,
            });
            variable_index += 1;
        }
    }

    CompiledRowLayout {
        columns,
        fixed_section_size: fixed_offset,
        variable_column_count: variable_index,
    }
}

fn compiled_row_layout(descriptor: &RowDescriptor) -> Arc<CompiledRowLayout> {
    let key = descriptor.content_hash();
    let cache = compiled_row_layout_cache();
    {
        let guard = cache.lock().expect("compiled row layout cache poisoned");
        if let Some(compiled) = guard.get(&key) {
            return compiled.clone();
        }
    }

    let compiled = Arc::new(compile_row_layout(descriptor));
    cache
        .lock()
        .expect("compiled row layout cache poisoned")
        .insert(key, compiled.clone());
    compiled
}

/// Binary row format:
///
/// ```text
/// [fixed fields...][var offsets (u32 each, skip first)...][var data...]
/// ```
///
/// - Fixed-size columns are laid out first in column order
/// - For nullable columns: 1-byte prefix (0=null, 1=present) before value
/// - Variable-length columns have their offsets stored after fixed data
///   - First variable column's offset is implicit (starts right after offsets)
///   - Subsequent offsets are u32 values
/// - Variable data follows offset table
pub fn encode_row(descriptor: &RowDescriptor, values: &[Value]) -> Result<Vec<u8>, EncodingError> {
    let layout = compiled_row_layout(descriptor);
    encode_row_with_layout(descriptor, layout.as_ref(), values)
}

fn encode_row_with_layout(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    values: &[Value],
) -> Result<Vec<u8>, EncodingError> {
    if values.len() != descriptor.columns.len() {
        return Err(EncodingError::ColumnCountMismatch {
            expected: descriptor.columns.len(),
            actual: values.len(),
        });
    }

    let offset_table_size = layout.variable_column_count.saturating_sub(1) * size_of::<u32>();
    let estimated_var_data_len = descriptor
        .columns
        .iter()
        .zip(values.iter())
        .filter(|(col, _)| col.column_type.is_variable())
        .map(|(col, val)| estimated_variable_value_len(col, val))
        .sum::<usize>();

    let mut fixed_data = Vec::with_capacity(layout.fixed_section_size);
    let mut var_data = Vec::with_capacity(estimated_var_data_len);
    let mut var_offsets: Vec<u32> = Vec::with_capacity(layout.variable_column_count);

    // Separate fixed and variable columns while maintaining order
    let mut var_columns: Vec<(usize, &ColumnDescriptor, &Value)> = Vec::new();

    for (i, (col, val)) in descriptor.columns.iter().zip(values.iter()).enumerate() {
        // Validate type match
        if !val.is_null() && !value_matches_column_type(val, &col.column_type) {
            return Err(EncodingError::TypeMismatch {
                column: col.name.to_string(),
                expected: col.column_type.clone(),
                actual: val.column_type(),
            });
        }

        // Check null allowed
        if val.is_null() && !col.nullable {
            return Err(EncodingError::NullNotAllowed {
                column: col.name.to_string(),
            });
        }

        // Enforce BYTEA payload limits (including nested bytea in arrays/rows).
        if !val.is_null() {
            validate_value_size(val, &col.column_type, col.name_str())?;
        }

        if col.column_type.is_variable() {
            var_columns.push((i, col, val));
        } else {
            // Encode fixed-size value
            encode_fixed_value(&mut fixed_data, col, val);
        }
    }

    // Encode variable-length values and build offset table
    for (_i, col, val) in &var_columns {
        var_offsets.push(var_data.len() as u32);
        encode_variable_value(&mut var_data, col, val);
    }

    // Build final binary: fixed_data + offset_table (skip first) + var_data
    let mut result =
        Vec::with_capacity(layout.fixed_section_size + offset_table_size + var_data.len());
    result.extend(fixed_data);

    // Write offsets (skip first, as it's implicitly 0)
    for offset in var_offsets.iter().skip(1) {
        result.extend_from_slice(&offset.to_le_bytes());
    }

    result.extend(var_data);

    Ok(result)
}

fn estimated_variable_value_len(col: &ColumnDescriptor, val: &Value) -> usize {
    let nullable_prefix = usize::from(col.nullable);
    if val.is_null() {
        return nullable_prefix;
    }

    nullable_prefix
        + match val {
            Value::Text(s) => s.len(),
            Value::Bytea(bytes) => bytes.len(),
            Value::Array(elements) => estimated_array_len(elements, &col.column_type),
            Value::Row { id, values } => {
                let id_len = 1 + id.map(|_| 16).unwrap_or(0);
                if let ColumnType::Row { columns } = &col.column_type {
                    id_len + estimated_row_len(columns, values)
                } else {
                    id_len
                }
            }
            _ => 0,
        }
}

fn estimated_row_len(descriptor: &RowDescriptor, values: &[Value]) -> usize {
    let layout = compiled_row_layout(descriptor);
    layout.fixed_section_size
        + layout.variable_column_count.saturating_sub(1) * size_of::<u32>()
        + descriptor
            .columns
            .iter()
            .zip(values.iter())
            .filter(|(col, _)| col.column_type.is_variable())
            .map(|(col, val)| estimated_variable_value_len(col, val))
            .sum::<usize>()
}

fn estimated_array_len(elements: &[Value], column_type: &ColumnType) -> usize {
    let element_type = match column_type {
        ColumnType::Array { element } => element.as_ref(),
        _ => return 0,
    };

    let offsets_len = elements.len().saturating_sub(1) * size_of::<u32>();
    let data_len = elements
        .iter()
        .map(|element| match (element, element_type) {
            (Value::Text(value), ColumnType::Text)
            | (Value::Text(value), ColumnType::Json { .. })
            | (Value::Text(value), ColumnType::Enum { .. }) => value.len(),
            (Value::Bytea(bytes), ColumnType::Bytea) => bytes.len(),
            (Value::Array(_), ColumnType::Array { .. }) => {
                let nested_col = ColumnDescriptor::new("", column_type.clone());
                estimated_variable_value_len(&nested_col, element)
            }
            (
                Value::Row { id, values },
                ColumnType::Row {
                    columns: descriptor,
                },
            ) => 1 + id.map(|_| 16).unwrap_or(0) + estimated_row_len(descriptor, values),
            _ => element_type.fixed_size().unwrap_or(0),
        })
        .sum::<usize>();

    size_of::<u32>() + offsets_len + data_len
}

fn value_matches_column_type(value: &Value, column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Integer => matches!(value, Value::Integer(_)),
        ColumnType::BigInt => matches!(value, Value::BigInt(_)),
        ColumnType::Double => matches!(value, Value::Double(_)),
        ColumnType::Boolean => matches!(value, Value::Boolean(_)),
        ColumnType::Timestamp => matches!(value, Value::Timestamp(_)),
        ColumnType::Uuid => matches!(value, Value::Uuid(_)),
        ColumnType::BatchId => {
            matches!(value, Value::BatchId(_))
                || matches!(value, Value::Bytea(bytes) if bytes.len() == 16)
        }
        ColumnType::Text => matches!(value, Value::Text(_)),
        ColumnType::Bytea => matches!(value, Value::Bytea(_)),
        ColumnType::Json { schema: _ } => matches!(value, Value::Text(_)),
        ColumnType::Enum { variants } => match value {
            Value::Text(s) => variants.contains(s),
            _ => false,
        },
        ColumnType::Array {
            element: element_type,
        } => match value {
            Value::Array(elements) => elements.iter().all(|element| {
                !element.is_null() && value_matches_column_type(element, element_type)
            }),
            _ => false,
        },
        ColumnType::Row {
            columns: row_descriptor,
        } => match value {
            Value::Row { values, .. } if values.len() == row_descriptor.columns.len() => values
                .iter()
                .zip(row_descriptor.columns.iter())
                .all(|(inner_value, inner_column)| {
                    if inner_value.is_null() {
                        inner_column.nullable
                    } else {
                        value_matches_column_type(inner_value, &inner_column.column_type)
                    }
                }),
            _ => false,
        },
    }
}

fn validate_bytea_size(column: &str, bytes: &[u8]) -> Result<(), EncodingError> {
    if bytes.len() > BYTEA_MAX_BYTES {
        return Err(EncodingError::ByteaTooLarge {
            column: column.to_string(),
            actual: bytes.len(),
            max: BYTEA_MAX_BYTES,
        });
    }
    Ok(())
}

fn validate_value_size(
    value: &Value,
    column_type: &ColumnType,
    column: &str,
) -> Result<(), EncodingError> {
    match (value, column_type) {
        (Value::BatchId(_), ColumnType::BatchId) => Ok(()),
        (Value::Bytea(bytes), ColumnType::Bytea) => validate_bytea_size(column, bytes),
        (
            Value::Array(values),
            ColumnType::Array {
                element: element_type,
            },
        ) => {
            for element in values {
                validate_value_size(element, element_type, column)?;
            }
            Ok(())
        }
        (
            Value::Row { values, .. },
            ColumnType::Row {
                columns: row_descriptor,
            },
        ) => {
            for (inner_value, inner_column) in values.iter().zip(row_descriptor.columns.iter()) {
                validate_value_size(
                    inner_value,
                    &inner_column.column_type,
                    inner_column.name_str(),
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn encode_enum_variant_index(variants: &[String], value: &str) -> Result<u8, EncodingError> {
    variants
        .iter()
        .position(|variant| variant == value)
        .and_then(|index| u8::try_from(index).ok())
        .ok_or_else(|| EncodingError::TypeMismatch {
            column: "enum".to_string(),
            expected: ColumnType::Enum {
                variants: variants.to_vec(),
            },
            actual: Some(ColumnType::Text),
        })
}

/// Encode a fixed-size value to the buffer.
fn encode_fixed_value(buf: &mut Vec<u8>, col: &ColumnDescriptor, val: &Value) {
    if col.nullable {
        if val.is_null() {
            buf.push(0); // null marker
            // Still need to write placeholder bytes for fixed size
            let size = col.column_type.fixed_size().unwrap();
            buf.extend(std::iter::repeat_n(0, size));
            return;
        } else {
            buf.push(1); // present marker
        }
    }

    if let ColumnType::Enum { variants } = &col.column_type {
        let index = match val {
            Value::Text(value) => {
                encode_enum_variant_index(variants, value).unwrap_or_else(|_| unreachable!())
            }
            Value::Null => 0,
            other => unreachable!("Enum is fixed-size only for text values, got {other:?}"),
        };
        buf.push(index);
        return;
    }

    match val {
        Value::Integer(n) => buf.extend_from_slice(&n.to_le_bytes()),
        Value::BigInt(n) => buf.extend_from_slice(&n.to_le_bytes()),
        Value::Double(f) => buf.extend_from_slice(&f.to_le_bytes()),
        Value::Boolean(b) => buf.push(if *b { 1 } else { 0 }),
        Value::Timestamp(t) => buf.extend_from_slice(&t.to_le_bytes()),
        Value::Uuid(id) => buf.extend_from_slice(id.uuid().as_bytes()),
        Value::BatchId(bytes) => buf.extend_from_slice(bytes),
        Value::Bytea(bytes) if matches!(col.column_type, ColumnType::BatchId) => {
            debug_assert_eq!(bytes.len(), 16, "validated batch ids must be 16 bytes");
            buf.extend_from_slice(bytes);
        }
        Value::Null => {
            // Should not reach here for non-nullable (validated above)
            let size = col.column_type.fixed_size().unwrap();
            buf.extend(std::iter::repeat_n(0, size));
        }
        Value::Text(_) => unreachable!("Text is not fixed-size"),
        Value::Bytea(_) => unreachable!("Bytea is not fixed-size"),
        Value::Array(_) => unreachable!("Array is not fixed-size"),
        Value::Row { .. } => unreachable!("Row is not fixed-size"),
    }
}

/// Encode a variable-length value to the buffer.
fn encode_variable_value(buf: &mut Vec<u8>, col: &ColumnDescriptor, val: &Value) {
    if col.nullable {
        if val.is_null() {
            buf.push(0); // null marker
            return;
        } else {
            buf.push(1); // present marker
        }
    }

    match val {
        Value::Text(s) => buf.extend_from_slice(s.as_bytes()),
        Value::Bytea(bytes) => buf.extend_from_slice(bytes),
        Value::Array(elements) => encode_array_into(buf, elements, &col.column_type),
        Value::Row { id, values } => {
            // Encode row using its descriptor from the column type
            if let ColumnType::Row { columns: desc } = &col.column_type {
                // Encode optional row id: 1-byte flag + 16-byte UUID if present
                match id {
                    Some(obj_id) => {
                        buf.push(1);
                        buf.extend_from_slice(obj_id.uuid().as_bytes());
                    }
                    None => buf.push(0),
                }
                let row_bytes = encode_row(desc, values).unwrap_or_default();
                buf.extend(row_bytes);
            }
        }
        Value::Null => {} // Already handled above for nullable
        _ => unreachable!("Non-text/bytea/array/row types are fixed-size"),
    }
}

/// Decode a binary row to Value slice.
pub fn decode_row(descriptor: &RowDescriptor, data: &[u8]) -> Result<Vec<Value>, EncodingError> {
    let layout = compiled_row_layout(descriptor);
    let mut values = Vec::with_capacity(descriptor.columns.len());

    for i in 0..descriptor.columns.len() {
        values.push(decode_column_with_layout(
            descriptor,
            layout.as_ref(),
            data,
            i,
        )?);
    }

    Ok(values)
}

#[derive(Copy, Clone)]
enum DecodeValueContext {
    Column,
    ArrayElement,
}

impl DecodeValueContext {
    fn too_short_message(self, value_type: &str) -> String {
        match self {
            DecodeValueContext::Column => format!("{value_type} too short"),
            DecodeValueContext::ArrayElement => format!("{value_type} element too short"),
        }
    }
}

fn decode_text_value(data: &[u8], variants: Option<&[String]>) -> Result<Value, EncodingError> {
    let s = std::str::from_utf8(data).map_err(|e| EncodingError::MalformedData {
        message: format!("invalid utf8: {e}"),
    })?;

    if let Some(variants) = variants
        && !variants.iter().any(|variant| variant == s)
    {
        return Err(EncodingError::MalformedData {
            message: format!("invalid enum variant: {s}"),
        });
    }

    Ok(Value::Text(s.to_string()))
}

fn decode_enum_value(data: &[u8], variants: &[String]) -> Result<Value, EncodingError> {
    if variants.len() <= u8::MAX as usize + 1 && data.len() == 1 {
        let index = data[0] as usize;
        let value = variants
            .get(index)
            .ok_or_else(|| EncodingError::MalformedData {
                message: format!("invalid enum variant index: {index}"),
            })?;
        return Ok(Value::Text(value.clone()));
    }

    decode_text_value(data, Some(variants))
}

fn decode_non_null_value(
    data: &[u8],
    column_type: &ColumnType,
    context: DecodeValueContext,
) -> Result<Value, EncodingError> {
    match column_type {
        ColumnType::Integer => {
            if data.len() < 4 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("integer"),
                });
            }
            Ok(Value::Integer(i32::from_le_bytes(
                data[..4].try_into().unwrap(),
            )))
        }
        ColumnType::BigInt => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("bigint"),
                });
            }
            Ok(Value::BigInt(i64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Double => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("double"),
                });
            }
            Ok(Value::Double(f64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Boolean => {
            if data.is_empty() {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("boolean"),
                });
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        ColumnType::Timestamp => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("timestamp"),
                });
            }
            Ok(Value::Timestamp(u64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Uuid => {
            if data.len() < 16 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("uuid"),
                });
            }
            let uuid =
                uuid::Uuid::from_slice(&data[..16]).map_err(|e| EncodingError::MalformedData {
                    message: format!("invalid uuid: {e}"),
                })?;
            Ok(Value::Uuid(ObjectId::from_uuid(uuid)))
        }
        ColumnType::BatchId => {
            if data.len() < 16 {
                return Err(EncodingError::MalformedData {
                    message: context.too_short_message("batch_id"),
                });
            }
            Ok(Value::BatchId(data[..16].try_into().unwrap()))
        }
        ColumnType::Bytea => Ok(Value::Bytea(data.to_vec())),
        ColumnType::Text | ColumnType::Json { schema: _ } => decode_text_value(data, None),
        ColumnType::Enum { variants } => decode_enum_value(data, variants),
        ColumnType::Array {
            element: element_type,
        } => {
            let elements = decode_array(data, element_type)?;
            Ok(Value::Array(elements))
        }
        ColumnType::Row { columns: row_desc } => {
            // Decode optional row id: 1-byte flag + 16-byte UUID if present
            if data.is_empty() {
                return Err(EncodingError::MalformedData {
                    message: "row id flag missing".to_string(),
                });
            }
            let (id, row_data) = if data[0] == 1 {
                if data.len() < 17 {
                    return Err(EncodingError::MalformedData {
                        message: "row id too short".to_string(),
                    });
                }
                let uuid = uuid::Uuid::from_slice(&data[1..17]).map_err(|e| {
                    EncodingError::MalformedData {
                        message: format!("invalid row id uuid: {e}"),
                    }
                })?;
                (Some(ObjectId::from_uuid(uuid)), &data[17..])
            } else {
                (None, &data[1..])
            };
            let values = decode_row(row_desc, row_data)?;
            Ok(Value::Row { id, values })
        }
    }
}

/// Decode a single column from binary data to Value.
pub fn decode_column(
    descriptor: &RowDescriptor,
    data: &[u8],
    col_index: usize,
) -> Result<Value, EncodingError> {
    let layout = compiled_row_layout(descriptor);
    decode_column_with_layout(descriptor, layout.as_ref(), data, col_index)
}

fn decode_column_with_layout(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &[u8],
    col_index: usize,
) -> Result<Value, EncodingError> {
    if col_index >= descriptor.columns.len() {
        return Err(EncodingError::ColumnIndexOutOfBounds {
            index: col_index,
            max: descriptor.columns.len().saturating_sub(1),
        });
    }

    let col = &descriptor.columns[col_index];

    // Get the byte slice for this column
    let (bytes, is_null) = column_bytes_internal_with_layout(descriptor, layout, data, col_index)?;

    if is_null {
        return Ok(Value::Null);
    }

    // Decode based on type
    decode_non_null_value(bytes, &col.column_type, DecodeValueContext::Column)
}

/// Get byte slice for a column (O(1) for fixed, O(1) for variable with offset table).
/// Returns (bytes, is_null).
fn column_bytes_internal<'a>(
    descriptor: &RowDescriptor,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    if col_index >= descriptor.columns.len() {
        return Err(EncodingError::ColumnIndexOutOfBounds {
            index: col_index,
            max: descriptor.columns.len().saturating_sub(1),
        });
    }
    let layout = compiled_row_layout(descriptor);
    column_bytes_internal_with_layout(descriptor, layout.as_ref(), data, col_index)
}

fn column_bytes_internal_with_layout<'a>(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    let column_layout = &layout.columns[col_index];
    if column_layout.variable_index.is_some() {
        variable_column_bytes(descriptor, layout, data, col_index)
    } else {
        fixed_column_bytes(descriptor, layout, data, col_index)
    }
}

/// Get byte slice for a fixed-size column.
fn fixed_column_bytes<'a>(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    let col = &descriptor.columns[col_index];
    let column_layout = &layout.columns[col_index];
    let offset = column_layout
        .fixed_offset
        .ok_or(EncodingError::ColumnIndexOutOfBounds {
            index: col_index,
            max: descriptor.columns.len().saturating_sub(1),
        })?;
    let total_size =
        column_layout
            .fixed_total_size
            .ok_or(EncodingError::ColumnIndexOutOfBounds {
                index: col_index,
                max: descriptor.columns.len().saturating_sub(1),
            })?;
    let value_size =
        column_layout
            .fixed_value_size
            .ok_or(EncodingError::ColumnIndexOutOfBounds {
                index: col_index,
                max: descriptor.columns.len().saturating_sub(1),
            })?;

    if offset + total_size > data.len() {
        return Err(EncodingError::MalformedData {
            message: format!("data too short for column {}", col.name),
        });
    }

    if column_layout.nullable {
        let is_null = data[offset] == 0;
        Ok((&data[offset + 1..offset + total_size], is_null))
    } else {
        Ok((&data[offset..offset + value_size], false))
    }
}

/// Get byte slice for a variable-length column.
fn variable_column_bytes<'a>(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    let fixed_size = layout.fixed_section_size;
    let var_count = layout.variable_column_count;
    let offset_table_size = if var_count > 1 {
        (var_count - 1) * 4
    } else {
        0
    };

    let var_data_start =
        fixed_size
            .checked_add(offset_table_size)
            .ok_or(EncodingError::MalformedData {
                message: "variable data start offset overflowed".into(),
            })?;

    if var_data_start > data.len() {
        return Err(EncodingError::MalformedData {
            message: "data too short for variable section".into(),
        });
    }

    // Find which variable column index this is
    let var_index =
        layout.columns[col_index]
            .variable_index
            .ok_or(EncodingError::ColumnIndexOutOfBounds {
                index: col_index,
                max: descriptor.columns.len().saturating_sub(1),
            })?;

    // Get start offset for this variable column
    let start_offset = if var_index == 0 {
        0
    } else {
        let offset_pos = fixed_size + (var_index - 1) * 4;
        if offset_pos + 4 > data.len() {
            return Err(EncodingError::MalformedData {
                message: "offset table truncated".into(),
            });
        }
        u32::from_le_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
    };

    // Get end offset (from next offset or end of data)
    let end_offset = if var_index + 1 < var_count {
        let offset_pos = fixed_size + var_index * 4;
        if offset_pos + 4 > data.len() {
            return Err(EncodingError::MalformedData {
                message: "offset table truncated".into(),
            });
        }
        u32::from_le_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
    } else {
        data.len() - var_data_start
    };

    if start_offset > end_offset {
        return Err(EncodingError::MalformedData {
            message: "variable column offsets out of order".into(),
        });
    }

    let var_section_len = data.len() - var_data_start;
    if end_offset > var_section_len {
        return Err(EncodingError::MalformedData {
            message: "variable column offset out of bounds".into(),
        });
    }

    let start = var_data_start
        .checked_add(start_offset)
        .ok_or(EncodingError::MalformedData {
            message: "variable column start overflowed".into(),
        })?;
    let end = var_data_start
        .checked_add(end_offset)
        .ok_or(EncodingError::MalformedData {
            message: "variable column end overflowed".into(),
        })?;

    let bytes = &data[start..end];

    if layout.columns[col_index].nullable {
        if bytes.is_empty() {
            return Err(EncodingError::MalformedData {
                message: "nullable variable column has no null marker".into(),
            });
        }
        let is_null = bytes[0] == 0;
        Ok((&bytes[1..], is_null))
    } else {
        Ok((bytes, false))
    }
}

/// Check if column is null.
pub fn column_is_null(
    descriptor: &RowDescriptor,
    data: &[u8],
    col_index: usize,
) -> Result<bool, EncodingError> {
    let (_, is_null) = column_bytes_internal(descriptor, data, col_index)?;
    Ok(is_null)
}

/// Encode a Value to binary bytes (for filter comparisons).
/// Note: Row values cannot be encoded without their descriptor - use encode_value_with_type instead.
fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Integer(n) => n.to_le_bytes().to_vec(),
        Value::BigInt(n) => n.to_le_bytes().to_vec(),
        Value::Double(f) => f.to_le_bytes().to_vec(),
        Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
        Value::Timestamp(t) => t.to_le_bytes().to_vec(),
        Value::Uuid(id) => id.uuid().as_bytes().to_vec(),
        Value::BatchId(bytes) => bytes.to_vec(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Bytea(bytes) => bytes.clone(),
        Value::Array(elements) => encode_array_simple(elements),
        Value::Row { .. } => panic!("Row values require a descriptor - use encode_value_with_type"),
        Value::Null => vec![],
    }
}

/// Encode a Value to binary bytes with type information (needed for Row values).
pub fn encode_value_with_type(value: &Value, col_type: &ColumnType) -> Vec<u8> {
    match (value, col_type) {
        (Value::Text(raw), ColumnType::Uuid) => Uuid::parse_str(raw)
            .map(|uuid| ObjectId::from_uuid(uuid).uuid().as_bytes().to_vec())
            .unwrap_or_else(|_| INVALID_UUID_TEXT_SENTINEL.to_vec()),
        (Value::Text(raw), ColumnType::Enum { variants }) if col_type.fixed_size().is_some() => {
            vec![encode_enum_variant_index(variants, raw).unwrap_or_else(|_| unreachable!())]
        }
        (Value::Row { id, values }, ColumnType::Row { columns: desc }) => {
            let mut buf = Vec::new();
            // Encode optional row id: 1-byte flag + 16-byte UUID if present
            match id {
                Some(obj_id) => {
                    buf.push(1);
                    buf.extend_from_slice(obj_id.uuid().as_bytes());
                }
                None => buf.push(0),
            }
            buf.extend(encode_row(desc, values).unwrap_or_default());
            buf
        }
        (Value::Array(elements), ColumnType::Array { element: _ }) => {
            encode_array(elements, col_type)
        }
        // For non-Row/Array types, fall back to simple encoding
        _ => encode_value(value),
    }
}

/// Simple array encoding for homogeneous arrays (no Row elements).
fn encode_array_simple(elements: &[Value]) -> Vec<u8> {
    let count = elements.len() as u32;
    let mut result = count.to_le_bytes().to_vec();

    if elements.is_empty() {
        return result;
    }

    // Determine if ALL elements are fixed-size
    let is_fixed = elements
        .iter()
        .all(|v| v.column_type().and_then(|t| t.fixed_size()).is_some());

    if is_fixed {
        for elem in elements {
            result.extend(encode_value(elem));
        }
    } else {
        let mut element_data: Vec<Vec<u8>> = Vec::with_capacity(elements.len());
        for elem in elements {
            element_data.push(encode_value(elem));
        }

        let mut offset: u32 = 0;
        for data in &element_data[..element_data.len().saturating_sub(1)] {
            offset += data.len() as u32;
            result.extend(offset.to_le_bytes());
        }

        for data in element_data {
            result.extend(data);
        }
    }

    result
}

/// Encode an array of Values to binary format.
///
/// Format mirrors row encoding:
/// - `[4-byte count][offset_2][offset_3]...[offset_N][elem_1]...[elem_N]`
/// - First offset is implicit (0), end of last element is implicit (end of data)
/// - For fixed-size elements: no offset table needed
/// - Array elements cannot be null (use empty array or nullable array column instead)
///
/// The `array_type` parameter is needed to properly encode Row elements,
/// which require their descriptor for encoding.
fn encode_array(elements: &[Value], array_type: &ColumnType) -> Vec<u8> {
    let mut result = Vec::new();
    encode_array_into(&mut result, elements, array_type);
    result
}

fn encode_array_into(buf: &mut Vec<u8>, elements: &[Value], array_type: &ColumnType) {
    let count = elements.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());

    if elements.is_empty() {
        return;
    }

    // Get the element type from the array type
    let element_type = match array_type {
        ColumnType::Array { element: elem_type } => elem_type.as_ref(),
        _ => return, // Not an array type
    };

    // Check if element type is fixed-size
    let is_fixed = element_type.fixed_size().is_some();

    if is_fixed {
        // Fixed-size elements: just concatenate encoded values (no offset table)
        for elem in elements {
            encode_value_with_type_into(buf, elem, element_type);
        }
    } else {
        // Variable-length elements: build offset table (skip first) + data
        let mut element_data: Vec<Vec<u8>> = Vec::with_capacity(elements.len());
        for elem in elements {
            element_data.push(encode_value_with_type(elem, element_type));
        }

        // Build offset table (skip first offset, which is implicit 0)
        let mut offset: u32 = 0;
        for data in &element_data[..element_data.len().saturating_sub(1)] {
            offset += data.len() as u32;
            buf.extend(offset.to_le_bytes());
        }

        // Append element data
        for data in element_data {
            buf.extend(data);
        }
    }
}

fn encode_value_with_type_into(buf: &mut Vec<u8>, value: &Value, col_type: &ColumnType) {
    match (value, col_type) {
        (Value::Text(raw), ColumnType::Enum { variants }) if col_type.fixed_size().is_some() => {
            buf.push(encode_enum_variant_index(variants, raw).unwrap_or_else(|_| unreachable!()));
        }
        (Value::Integer(n), _) => buf.extend_from_slice(&n.to_le_bytes()),
        (Value::BigInt(n), _) => buf.extend_from_slice(&n.to_le_bytes()),
        (Value::Double(f), _) => buf.extend_from_slice(&f.to_le_bytes()),
        (Value::Boolean(b), _) => buf.push(if *b { 1 } else { 0 }),
        (Value::Timestamp(t), _) => buf.extend_from_slice(&t.to_le_bytes()),
        (Value::Uuid(id), _) => buf.extend_from_slice(id.uuid().as_bytes()),
        (Value::BatchId(bytes), _) => buf.extend_from_slice(bytes),
        _ => buf.extend(encode_value_with_type(value, col_type)),
    }
}

/// Decode an array from binary format.
fn decode_array(data: &[u8], element_type: &ColumnType) -> Result<Vec<Value>, EncodingError> {
    if data.len() < 4 {
        return Err(EncodingError::MalformedData {
            message: "array too short for count".into(),
        });
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let is_fixed = element_type.fixed_size().is_some();
    let mut values = Vec::with_capacity(count);

    if is_fixed {
        // Fixed-size elements: no offset table
        let elem_size = element_type.fixed_size().unwrap();
        let mut offset = 4;

        for _ in 0..count {
            if offset + elem_size > data.len() {
                return Err(EncodingError::MalformedData {
                    message: "array data truncated".into(),
                });
            }
            let elem_data = &data[offset..offset + elem_size];
            values.push(decode_array_element(elem_data, element_type)?);
            offset += elem_size;
        }
    } else {
        // Variable-length elements: offset table has (count - 1) entries
        let offset_table_start = 4;
        let offset_table_size = (count - 1) * 4;
        let data_start = offset_table_start + offset_table_size;

        if data_start > data.len() {
            return Err(EncodingError::MalformedData {
                message: "array offset table truncated".into(),
            });
        }

        for i in 0..count {
            // Start offset: first is 0, rest come from offset table
            let start = if i == 0 {
                data_start
            } else {
                let offset_pos = offset_table_start + (i - 1) * 4;
                u32::from_le_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                    + data_start
            };

            // End offset: from next offset, or end of data for last element
            let end = if i + 1 < count {
                let offset_pos = offset_table_start + i * 4;
                u32::from_le_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                    + data_start
            } else {
                data.len()
            };

            if end > data.len() || start > end {
                return Err(EncodingError::MalformedData {
                    message: "array element bounds invalid".into(),
                });
            }

            let elem_data = &data[start..end];
            values.push(decode_array_element(elem_data, element_type)?);
        }
    }

    Ok(values)
}

/// Decode a single array element from bytes (no null marker - arrays don't contain nulls).
fn decode_array_element(data: &[u8], element_type: &ColumnType) -> Result<Value, EncodingError> {
    decode_non_null_value(data, element_type, DecodeValueContext::ArrayElement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
            ColumnDescriptor::new("active", ColumnType::Boolean),
        ])
    }

    #[test]
    fn encode_decode_roundtrip() {
        let descriptor = test_descriptor();
        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(12345))),
            Value::Text("Alice".into()),
            Value::Integer(30),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn text_uuid_encoding_is_fixed_width_for_valid_and_invalid_text() {
        let encoded_valid = encode_value_with_type(
            &Value::Text(Uuid::from_u128(12345).to_string()),
            &ColumnType::Uuid,
        );
        let encoded_invalid =
            encode_value_with_type(&Value::Text("not-a-uuid".into()), &ColumnType::Uuid);

        assert_eq!(encoded_valid.len(), 16);
        assert_eq!(encoded_invalid.len(), 16);
    }

    #[test]
    fn encode_decode_with_nullable() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text).nullable(),
            ColumnDescriptor::new("score", ColumnType::Integer).nullable(),
        ]);

        // With values
        let values1 = vec![
            Value::Integer(1),
            Value::Text("Bob".into()),
            Value::Integer(100),
        ];
        let encoded1 = encode_row(&descriptor, &values1).unwrap();
        let decoded1 = decode_row(&descriptor, &encoded1).unwrap();
        assert_eq!(values1, decoded1);

        // With nulls
        let values2 = vec![Value::Integer(2), Value::Null, Value::Null];
        let encoded2 = encode_row(&descriptor, &values2).unwrap();
        let decoded2 = decode_row(&descriptor, &encoded2).unwrap();
        assert_eq!(values2, decoded2);
    }

    #[test]
    fn encode_decode_fixed_batch_id_column() {
        let descriptor =
            RowDescriptor::new(vec![ColumnDescriptor::new("batch_id", ColumnType::BatchId)]);
        let values = vec![Value::BatchId([0xAB; 16])];

        let encoded = encode_row(&descriptor, &values).unwrap();
        assert_eq!(encoded.len(), 16);

        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn encode_fixed_batch_id_column_accepts_legacy_bytea_value() {
        let descriptor =
            RowDescriptor::new(vec![ColumnDescriptor::new("batch_id", ColumnType::BatchId)]);
        let values = vec![Value::Bytea(vec![0xCD; 16])];

        let encoded = encode_row(&descriptor, &values).unwrap();
        assert_eq!(encoded.len(), 16);
        assert_eq!(encoded, vec![0xCD; 16]);
    }

    #[test]
    fn null_not_allowed_for_non_nullable() {
        let descriptor = test_descriptor();
        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(1))),
            Value::Null, // name is not nullable
            Value::Integer(30),
            Value::Boolean(true),
        ];

        let result = encode_row(&descriptor, &values);
        assert!(matches!(result, Err(EncodingError::NullNotAllowed { .. })));
    }

    #[test]
    fn type_mismatch_error() {
        let descriptor = test_descriptor();
        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(1))),
            Value::Integer(42), // Should be Text
            Value::Integer(30),
            Value::Boolean(true),
        ];

        let result = encode_row(&descriptor, &values);
        assert!(matches!(result, Err(EncodingError::TypeMismatch { .. })));
    }

    #[test]
    fn column_count_mismatch_error() {
        let descriptor = test_descriptor();
        let values = vec![Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(1)))];

        let result = encode_row(&descriptor, &values);
        assert!(matches!(
            result,
            Err(EncodingError::ColumnCountMismatch { .. })
        ));
    }

    #[test]
    fn encode_value_test() {
        assert_eq!(
            encode_value(&Value::Integer(42)),
            42i32.to_le_bytes().to_vec()
        );
        assert_eq!(
            encode_value(&Value::BigInt(42)),
            42i64.to_le_bytes().to_vec()
        );
        assert_eq!(encode_value(&Value::Boolean(true)), vec![1]);
        assert_eq!(encode_value(&Value::Boolean(false)), vec![0]);
        assert_eq!(
            encode_value(&Value::Timestamp(12345)),
            12345u64.to_le_bytes().to_vec()
        );
        assert_eq!(
            encode_value(&Value::Text("hello".into())),
            b"hello".to_vec()
        );
    }

    #[test]
    fn multiple_variable_columns() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("first_name", ColumnType::Text),
            ColumnDescriptor::new("last_name", ColumnType::Text),
            ColumnDescriptor::new("email", ColumnType::Text),
        ]);

        let values = vec![
            Value::Integer(1),
            Value::Text("John".into()),
            Value::Text("Doe".into()),
            Value::Text("john.doe@example.com".into()),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn column_is_null_test() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text).nullable(),
        ]);

        let with_value = vec![Value::Integer(1), Value::Text("Alice".into())];
        let with_null = vec![Value::Integer(2), Value::Null];

        let encoded_value = encode_row(&descriptor, &with_value).unwrap();
        let encoded_null = encode_row(&descriptor, &with_null).unwrap();

        assert!(!column_is_null(&descriptor, &encoded_value, 0).unwrap());
        assert!(!column_is_null(&descriptor, &encoded_value, 1).unwrap());
        assert!(!column_is_null(&descriptor, &encoded_null, 0).unwrap());
        assert!(column_is_null(&descriptor, &encoded_null, 1).unwrap());
    }

    // ========================================================================
    // Array encoding tests
    // ========================================================================

    #[test]
    fn array_encode_decode_empty() {
        let elements: Vec<Value> = vec![];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Integer),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Integer).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_integers() {
        let elements = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Integer),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Integer).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_single_integer() {
        let elements = vec![Value::Integer(42)];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Integer),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Integer).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_texts() {
        let elements = vec![
            Value::Text("hello".into()),
            Value::Text("world".into()),
            Value::Text("!".into()),
        ];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Text),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Text).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_single_text() {
        let elements = vec![Value::Text("hello".into())];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Text),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Text).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_booleans() {
        let elements = vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
        ];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Boolean),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Boolean).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_uuids() {
        let elements = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(1))),
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(2))),
        ];
        let array_type = ColumnType::Array {
            element: Box::new(ColumnType::Uuid),
        };
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Uuid).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_in_row_roundtrip() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ),
        ]);

        let values = vec![
            Value::Integer(1),
            Value::Array(vec![
                Value::Text("rust".into()),
                Value::Text("database".into()),
            ]),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn array_of_integers_in_row() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "scores",
                ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                },
            ),
        ]);

        let values = vec![
            Value::Text("Alice".into()),
            Value::Array(vec![
                Value::Integer(100),
                Value::Integer(95),
                Value::Integer(87),
            ]),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn empty_array_in_row() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ),
        ]);

        let values = vec![Value::Integer(1), Value::Array(vec![])];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn nested_array() {
        // Array of arrays of integers
        let inner_type = ColumnType::Array {
            element: Box::new(ColumnType::Integer),
        };
        let array_type = ColumnType::Array {
            element: Box::new(inner_type.clone()),
        };
        let elements = vec![
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Array(vec![
                Value::Integer(3),
                Value::Integer(4),
                Value::Integer(5),
            ]),
        ];

        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &inner_type).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_of_rows() {
        // Array of rows (heterogeneous tuples)
        let row_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);
        let row_type = ColumnType::Row {
            columns: Box::new(row_desc.clone()),
        };
        let array_type = ColumnType::Array {
            element: Box::new(row_type.clone()),
        };

        let elements = vec![
            Value::Row {
                id: None,
                values: vec![Value::Integer(1), Value::Text("Alice".into())],
            },
            Value::Row {
                id: None,
                values: vec![Value::Integer(2), Value::Text("Bob".into())],
            },
        ];

        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &row_type).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn encode_decode_bytea_roundtrip_with_nul_bytes() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("payload", ColumnType::Bytea),
        ]);
        let values = vec![
            Value::Integer(1),
            Value::Bytea(vec![0x00, 0x11, 0x00, 0x22, 0xFF]),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn encode_row_rejects_oversized_bytea() {
        let descriptor =
            RowDescriptor::new(vec![ColumnDescriptor::new("payload", ColumnType::Bytea)]);
        let over_limit = vec![7u8; BYTEA_MAX_BYTES + 1];

        let err = encode_row(&descriptor, &[Value::Bytea(over_limit)]).unwrap_err();
        assert!(matches!(err, EncodingError::ByteaTooLarge { .. }));
    }

    #[test]
    fn encode_row_rejects_invalid_enum_variant() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "status",
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()],
            },
        )]);

        let err = encode_row(&descriptor, &[Value::Text("invalid".to_string())]).unwrap_err();
        assert!(matches!(err, EncodingError::TypeMismatch { .. }));
    }

    #[test]
    fn decode_row_rejects_invalid_enum_variant() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "status",
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()],
            },
        )]);

        let mut encoded = encode_row(&descriptor, &[Value::Text("todo".to_string())]).unwrap();
        encoded[0] = 9;

        let err = decode_row(&descriptor, &encoded).unwrap_err();
        assert!(matches!(err, EncodingError::MalformedData { .. }));
    }

    #[test]
    fn enum_columns_encode_as_single_byte_indices() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "status",
            ColumnType::Enum {
                variants: vec!["todo".to_string(), "doing".to_string(), "done".to_string()],
            },
        )]);

        let encoded = encode_row(&descriptor, &[Value::Text("done".to_string())]).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();

        assert_eq!(encoded, vec![2]);
        assert_eq!(decoded, vec![Value::Text("done".to_string())]);
    }

    #[test]
    fn real_encode_decode_roundtrip() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("temperature", ColumnType::Double),
            ColumnDescriptor::new("pressure", ColumnType::Double),
        ]);

        let values = vec![
            Value::Text("sensor-1".into()),
            Value::Double(23.456),
            Value::Double(-101.325),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();

        assert_eq!(values, decoded);
    }

    #[test]
    fn real_encode_decode_special_values() {
        // Exercise: zero, negative zero, infinity, negative infinity, NaN, subnormal
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new("val", ColumnType::Double)]);

        let cases: Vec<f64> = vec![
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            f64::MIN_POSITIVE, // smallest normal
            5e-324,            // smallest subnormal
            -5e-324,           // negative subnormal
            f64::MAX,
            f64::MIN,
        ];

        for val in cases {
            let values = vec![Value::Double(val)];
            let encoded = encode_row(&descriptor, &values).unwrap();
            let decoded = decode_row(&descriptor, &encoded).unwrap();

            match &decoded[0] {
                Value::Double(decoded_val) => {
                    // Bitwise comparison handles NaN and negative zero
                    assert_eq!(
                        val.to_bits(),
                        decoded_val.to_bits(),
                        "round-trip failed for {val}"
                    );
                }
                other => panic!("expected Value::Double, got {other:?}"),
            }
        }
    }

    #[test]
    fn real_nullable_encode_decode() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("score", ColumnType::Double).nullable(),
        ]);

        // With value
        let values = vec![Value::Double(99.5)];
        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(values, decoded);

        // With null
        let nulls = vec![Value::Null];
        let encoded = encode_row(&descriptor, &nulls).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(nulls, decoded);
    }

    #[test]
    fn real_type_mismatch_rejected() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "temperature",
            ColumnType::Double,
        )]);

        let err = encode_row(&descriptor, &[Value::Integer(42)]).unwrap_err();
        assert!(matches!(err, EncodingError::TypeMismatch { .. }));
    }
}
