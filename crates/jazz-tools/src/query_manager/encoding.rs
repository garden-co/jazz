use std::cmp::Ordering;

use crate::object::ObjectId;

use super::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

/// Maximum payload size allowed for a single BYTEA value (1 MiB).
pub const BYTEA_MAX_BYTES: usize = 1_048_576;

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
    /// Requested comparison is unsupported for this column type.
    UnsupportedComparison {
        column: String,
        column_type: ColumnType,
        operation: String,
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
            EncodingError::UnsupportedComparison {
                column,
                column_type,
                operation,
            } => {
                write!(
                    f,
                    "unsupported {operation} comparison for column '{column}' with type {column_type:?}"
                )
            }
            EncodingError::ColumnIndexOutOfBounds { index, max } => {
                write!(f, "column index {index} out of bounds (max {max})")
            }
        }
    }
}

impl std::error::Error for EncodingError {}

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
    if values.len() != descriptor.columns.len() {
        return Err(EncodingError::ColumnCountMismatch {
            expected: descriptor.columns.len(),
            actual: values.len(),
        });
    }

    let mut fixed_data = Vec::new();
    let mut var_data = Vec::new();
    let mut var_offsets: Vec<u32> = Vec::new();

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
    let mut result = fixed_data;

    // Write offsets (skip first, as it's implicitly 0)
    for offset in var_offsets.iter().skip(1) {
        result.extend_from_slice(&offset.to_le_bytes());
    }

    result.extend(var_data);

    Ok(result)
}

fn value_matches_column_type(value: &Value, column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Integer => matches!(value, Value::Integer(_)),
        ColumnType::BigInt => matches!(value, Value::BigInt(_)),
        ColumnType::Double => matches!(value, Value::Double(_)),
        ColumnType::Boolean => matches!(value, Value::Boolean(_)),
        ColumnType::Timestamp => matches!(value, Value::Timestamp(_)),
        ColumnType::Uuid => matches!(value, Value::Uuid(_)),
        ColumnType::Text => matches!(value, Value::Text(_)),
        ColumnType::Bytea => matches!(value, Value::Bytea(_)),
        ColumnType::Enum(variants) => match value {
            Value::Text(s) => variants.contains(s),
            _ => false,
        },
        ColumnType::Array(element_type) => match value {
            Value::Array(elements) => elements.iter().all(|element| {
                !element.is_null() && value_matches_column_type(element, element_type)
            }),
            _ => false,
        },
        ColumnType::Row(row_descriptor) => match value {
            Value::Row(values) if values.len() == row_descriptor.columns.len() => values
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
        (Value::Bytea(bytes), ColumnType::Bytea) => validate_bytea_size(column, bytes),
        (Value::Array(values), ColumnType::Array(element_type)) => {
            for element in values {
                validate_value_size(element, element_type, column)?;
            }
            Ok(())
        }
        (Value::Row(values), ColumnType::Row(row_descriptor)) => {
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

    match val {
        Value::Integer(n) => buf.extend_from_slice(&n.to_le_bytes()),
        Value::BigInt(n) => buf.extend_from_slice(&n.to_le_bytes()),
        Value::Double(f) => buf.extend_from_slice(&f.to_le_bytes()),
        Value::Boolean(b) => buf.push(if *b { 1 } else { 0 }),
        Value::Timestamp(t) => buf.extend_from_slice(&t.to_le_bytes()),
        Value::Uuid(id) => buf.extend_from_slice(id.uuid().as_bytes()),
        Value::Null => {
            // Should not reach here for non-nullable (validated above)
            let size = col.column_type.fixed_size().unwrap();
            buf.extend(std::iter::repeat_n(0, size));
        }
        Value::Text(_) => unreachable!("Text is not fixed-size"),
        Value::Bytea(_) => unreachable!("Bytea is not fixed-size"),
        Value::Array(_) => unreachable!("Array is not fixed-size"),
        Value::Row(_) => unreachable!("Row is not fixed-size"),
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
        Value::Array(elements) => buf.extend(encode_array(elements, &col.column_type)),
        Value::Row(values) => {
            // Encode row using its descriptor from the column type
            if let ColumnType::Row(desc) = &col.column_type {
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
    let mut values = Vec::with_capacity(descriptor.columns.len());

    for i in 0..descriptor.columns.len() {
        values.push(decode_column(descriptor, data, i)?);
    }

    Ok(values)
}

/// Decode a single column from binary data to Value.
pub fn decode_column(
    descriptor: &RowDescriptor,
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
    let (bytes, is_null) = column_bytes_internal(descriptor, data, col_index)?;

    if is_null {
        return Ok(Value::Null);
    }

    // Decode based on type
    match &col.column_type {
        ColumnType::Integer => {
            if bytes.len() < 4 {
                return Err(EncodingError::MalformedData {
                    message: "integer too short".into(),
                });
            }
            let n = i32::from_le_bytes(bytes[..4].try_into().unwrap());
            Ok(Value::Integer(n))
        }
        ColumnType::BigInt => {
            if bytes.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "bigint too short".into(),
                });
            }
            let n = i64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok(Value::BigInt(n))
        }
        ColumnType::Double => {
            if bytes.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "double too short".into(),
                });
            }
            let f = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok(Value::Double(f))
        }
        ColumnType::Boolean => {
            if bytes.is_empty() {
                return Err(EncodingError::MalformedData {
                    message: "boolean too short".into(),
                });
            }
            Ok(Value::Boolean(bytes[0] != 0))
        }
        ColumnType::Timestamp => {
            if bytes.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "timestamp too short".into(),
                });
            }
            let t = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok(Value::Timestamp(t))
        }
        ColumnType::Uuid => {
            if bytes.len() < 16 {
                return Err(EncodingError::MalformedData {
                    message: "uuid too short".into(),
                });
            }
            let uuid =
                uuid::Uuid::from_slice(&bytes[..16]).map_err(|e| EncodingError::MalformedData {
                    message: format!("invalid uuid: {e}"),
                })?;
            Ok(Value::Uuid(ObjectId::from_uuid(uuid)))
        }
        ColumnType::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        ColumnType::Text => {
            let s = std::str::from_utf8(bytes).map_err(|e| EncodingError::MalformedData {
                message: format!("invalid utf8: {e}"),
            })?;
            Ok(Value::Text(s.to_string()))
        }
        ColumnType::Enum(variants) => {
            let s = std::str::from_utf8(bytes).map_err(|e| EncodingError::MalformedData {
                message: format!("invalid utf8: {e}"),
            })?;
            if !variants.iter().any(|variant| variant == s) {
                return Err(EncodingError::MalformedData {
                    message: format!("invalid enum variant: {s}"),
                });
            }
            Ok(Value::Text(s.to_string()))
        }
        ColumnType::Array(element_type) => {
            let elements = decode_array(bytes, element_type)?;
            Ok(Value::Array(elements))
        }
        ColumnType::Row(row_desc) => {
            let values = decode_row(row_desc, bytes)?;
            Ok(Value::Row(values))
        }
    }
}

/// Get byte slice for a column (O(1) for fixed, O(1) for variable with offset table).
/// Returns (bytes, is_null).
fn column_bytes_internal<'a>(
    descriptor: &RowDescriptor,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    let col = &descriptor.columns[col_index];

    if col.column_type.is_variable() {
        // Variable-length column
        variable_column_bytes(descriptor, data, col_index)
    } else {
        // Fixed-size column
        fixed_column_bytes(descriptor, data, col_index)
    }
}

/// Get byte slice for a fixed-size column.
fn fixed_column_bytes<'a>(
    descriptor: &RowDescriptor,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    let mut offset = 0;

    for (i, col) in descriptor.columns.iter().enumerate() {
        if col.column_type.is_variable() {
            continue; // Skip variable columns in fixed section
        }

        let nullable_prefix = if col.nullable { 1 } else { 0 };
        let value_size = col.column_type.fixed_size().unwrap();
        let total_size = nullable_prefix + value_size;

        if i == col_index {
            if offset + total_size > data.len() {
                return Err(EncodingError::MalformedData {
                    message: format!("data too short for column {}", col.name),
                });
            }

            if col.nullable {
                let is_null = data[offset] == 0;
                return Ok((&data[offset + 1..offset + total_size], is_null));
            } else {
                return Ok((&data[offset..offset + value_size], false));
            }
        }

        offset += total_size;
    }

    Err(EncodingError::ColumnIndexOutOfBounds {
        index: col_index,
        max: descriptor.columns.len().saturating_sub(1),
    })
}

/// Get byte slice for a variable-length column.
fn variable_column_bytes<'a>(
    descriptor: &RowDescriptor,
    data: &'a [u8],
    col_index: usize,
) -> Result<(&'a [u8], bool), EncodingError> {
    // Calculate fixed section size
    let fixed_size = calculate_fixed_section_size(descriptor);

    // Calculate offset table size (var_count - 1 entries, each 4 bytes)
    let var_count = descriptor.variable_column_count();
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
    let mut var_index = 0;
    for (i, col) in descriptor.columns.iter().enumerate() {
        if col.column_type.is_variable() {
            if i == col_index {
                break;
            }
            var_index += 1;
        }
    }

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

    let col = &descriptor.columns[col_index];
    let bytes = &data[start..end];

    if col.nullable {
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

/// Calculate the size of the fixed section in bytes.
fn calculate_fixed_section_size(descriptor: &RowDescriptor) -> usize {
    let mut size = 0;
    for col in &descriptor.columns {
        if let Some(fixed_size) = col.column_type.fixed_size() {
            size += fixed_size;
            if col.nullable {
                size += 1; // null marker
            }
        }
    }
    size
}

/// Get byte slice for a column (public API).
/// Returns None if the column is null.
pub fn column_bytes<'a>(
    descriptor: &RowDescriptor,
    data: &'a [u8],
    col_index: usize,
) -> Result<Option<&'a [u8]>, EncodingError> {
    let (bytes, is_null) = column_bytes_internal(descriptor, data, col_index)?;
    if is_null { Ok(None) } else { Ok(Some(bytes)) }
}

/// Compare column values in binary form (for filtering, sorting).
/// Nulls sort first (less than any non-null value).
pub fn compare_column(
    descriptor: &RowDescriptor,
    data: &[u8],
    col_index: usize,
    other_data: &[u8],
    other_col_index: usize,
) -> Result<Ordering, EncodingError> {
    let (bytes1, is_null1) = column_bytes_internal(descriptor, data, col_index)?;
    let (bytes2, is_null2) = column_bytes_internal(descriptor, other_data, other_col_index)?;

    // Handle nulls: null < non-null
    match (is_null1, is_null2) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }

    let col = &descriptor.columns[col_index];

    match &col.column_type {
        ColumnType::Integer => {
            let n1 = i32::from_le_bytes(bytes1[..4].try_into().unwrap());
            let n2 = i32::from_le_bytes(bytes2[..4].try_into().unwrap());
            Ok(n1.cmp(&n2))
        }
        ColumnType::BigInt => {
            let n1 = i64::from_le_bytes(bytes1[..8].try_into().unwrap());
            let n2 = i64::from_le_bytes(bytes2[..8].try_into().unwrap());
            Ok(n1.cmp(&n2))
        }
        ColumnType::Double => {
            let f1 = f64::from_le_bytes(bytes1[..8].try_into().unwrap());
            let f2 = f64::from_le_bytes(bytes2[..8].try_into().unwrap());
            Ok(f1.total_cmp(&f2))
        }
        ColumnType::Boolean => {
            let b1 = bytes1[0] != 0;
            let b2 = bytes2[0] != 0;
            Ok(b1.cmp(&b2))
        }
        ColumnType::Timestamp => {
            let t1 = u64::from_le_bytes(bytes1[..8].try_into().unwrap());
            let t2 = u64::from_le_bytes(bytes2[..8].try_into().unwrap());
            Ok(t1.cmp(&t2))
        }
        ColumnType::Uuid => {
            // Compare as bytes (UUIDs have natural byte ordering)
            Ok(bytes1.cmp(bytes2))
        }
        ColumnType::Bytea => Err(EncodingError::UnsupportedComparison {
            column: col.name_str().to_string(),
            column_type: col.column_type.clone(),
            operation: "ordering".to_string(),
        }),
        ColumnType::Text | ColumnType::Enum(_) | ColumnType::Array(_) | ColumnType::Row(_) => {
            // Lexicographic comparison of bytes
            Ok(bytes1.cmp(bytes2))
        }
    }
}

/// Compare a column value against a binary value (for filtering).
pub fn compare_column_to_value(
    descriptor: &RowDescriptor,
    data: &[u8],
    col_index: usize,
    value: &[u8],
) -> Result<Ordering, EncodingError> {
    let (bytes, is_null) = column_bytes_internal(descriptor, data, col_index)?;

    // If column is null, it's less than any concrete value
    if is_null {
        return Ok(Ordering::Less);
    }

    let col = &descriptor.columns[col_index];

    match &col.column_type {
        ColumnType::Integer => {
            let n1 = i32::from_le_bytes(bytes[..4].try_into().unwrap());
            let n2 = i32::from_le_bytes(value[..4].try_into().unwrap());
            Ok(n1.cmp(&n2))
        }
        ColumnType::BigInt => {
            let n1 = i64::from_le_bytes(bytes[..8].try_into().unwrap());
            let n2 = i64::from_le_bytes(value[..8].try_into().unwrap());
            Ok(n1.cmp(&n2))
        }
        ColumnType::Double => {
            let f1 = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            let f2 = f64::from_le_bytes(value[..8].try_into().unwrap());
            Ok(f1.total_cmp(&f2))
        }
        ColumnType::Boolean => {
            let b1 = bytes[0] != 0;
            let b2 = value[0] != 0;
            Ok(b1.cmp(&b2))
        }
        ColumnType::Timestamp => {
            let t1 = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let t2 = u64::from_le_bytes(value[..8].try_into().unwrap());
            Ok(t1.cmp(&t2))
        }
        ColumnType::Bytea => Err(EncodingError::UnsupportedComparison {
            column: col.name_str().to_string(),
            column_type: col.column_type.clone(),
            operation: "ordering".to_string(),
        }),
        ColumnType::Uuid
        | ColumnType::Text
        | ColumnType::Enum(_)
        | ColumnType::Array(_)
        | ColumnType::Row(_) => Ok(bytes.cmp(value)),
    }
}

/// Check if column matches a binary value.
pub fn column_eq(
    descriptor: &RowDescriptor,
    data: &[u8],
    col_index: usize,
    value: &[u8],
) -> Result<bool, EncodingError> {
    let (bytes, is_null) = column_bytes_internal(descriptor, data, col_index)?;

    if is_null {
        return Ok(false); // Null never equals a value
    }

    Ok(bytes == value)
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
pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Integer(n) => n.to_le_bytes().to_vec(),
        Value::BigInt(n) => n.to_le_bytes().to_vec(),
        Value::Double(f) => f.to_le_bytes().to_vec(),
        Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
        Value::Timestamp(t) => t.to_le_bytes().to_vec(),
        Value::Uuid(id) => id.uuid().as_bytes().to_vec(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Bytea(bytes) => bytes.clone(),
        Value::Array(elements) => encode_array_simple(elements),
        Value::Row(_) => panic!("Row values require a descriptor - use encode_value_with_type"),
        Value::Null => vec![],
    }
}

/// Encode a Value to binary bytes with type information (needed for Row values).
pub fn encode_value_with_type(value: &Value, col_type: &ColumnType) -> Vec<u8> {
    match (value, col_type) {
        (Value::Row(values), ColumnType::Row(desc)) => encode_row(desc, values).unwrap_or_default(),
        (Value::Array(elements), ColumnType::Array(_)) => encode_array(elements, col_type),
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
pub fn encode_array(elements: &[Value], array_type: &ColumnType) -> Vec<u8> {
    let count = elements.len() as u32;
    let mut result = count.to_le_bytes().to_vec();

    if elements.is_empty() {
        return result;
    }

    // Get the element type from the array type
    let element_type = match array_type {
        ColumnType::Array(elem_type) => elem_type.as_ref(),
        _ => return result, // Not an array type
    };

    // Check if element type is fixed-size
    let is_fixed = element_type.fixed_size().is_some();

    if is_fixed {
        // Fixed-size elements: just concatenate encoded values (no offset table)
        for elem in elements {
            result.extend(encode_value_with_type(elem, element_type));
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
            result.extend(offset.to_le_bytes());
        }

        // Append element data
        for data in element_data {
            result.extend(data);
        }
    }

    result
}

/// Decode an array from binary format.
pub fn decode_array(data: &[u8], element_type: &ColumnType) -> Result<Vec<Value>, EncodingError> {
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
    match element_type {
        ColumnType::Integer => {
            if data.len() < 4 {
                return Err(EncodingError::MalformedData {
                    message: "integer element too short".into(),
                });
            }
            Ok(Value::Integer(i32::from_le_bytes(
                data[..4].try_into().unwrap(),
            )))
        }
        ColumnType::BigInt => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "bigint element too short".into(),
                });
            }
            Ok(Value::BigInt(i64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Double => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "double element too short".into(),
                });
            }
            Ok(Value::Double(f64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Boolean => {
            if data.is_empty() {
                return Err(EncodingError::MalformedData {
                    message: "boolean element too short".into(),
                });
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        ColumnType::Timestamp => {
            if data.len() < 8 {
                return Err(EncodingError::MalformedData {
                    message: "timestamp element too short".into(),
                });
            }
            Ok(Value::Timestamp(u64::from_le_bytes(
                data[..8].try_into().unwrap(),
            )))
        }
        ColumnType::Uuid => {
            if data.len() < 16 {
                return Err(EncodingError::MalformedData {
                    message: "uuid element too short".into(),
                });
            }
            let uuid =
                uuid::Uuid::from_slice(&data[..16]).map_err(|e| EncodingError::MalformedData {
                    message: format!("invalid uuid: {e}"),
                })?;
            Ok(Value::Uuid(ObjectId::from_uuid(uuid)))
        }
        ColumnType::Bytea => Ok(Value::Bytea(data.to_vec())),
        ColumnType::Text => {
            let s = std::str::from_utf8(data).map_err(|e| EncodingError::MalformedData {
                message: format!("invalid utf8: {e}"),
            })?;
            Ok(Value::Text(s.to_string()))
        }
        ColumnType::Enum(variants) => {
            let s = std::str::from_utf8(data).map_err(|e| EncodingError::MalformedData {
                message: format!("invalid utf8: {e}"),
            })?;
            if !variants.iter().any(|variant| variant == s) {
                return Err(EncodingError::MalformedData {
                    message: format!("invalid enum variant: {s}"),
                });
            }
            Ok(Value::Text(s.to_string()))
        }
        ColumnType::Array(inner_type) => {
            let inner_values = decode_array(data, inner_type)?;
            Ok(Value::Array(inner_values))
        }
        ColumnType::Row(row_desc) => {
            let values = decode_row(row_desc, data)?;
            Ok(Value::Row(values))
        }
    }
}

/// Project columns from a source row to create a new row (for projections).
/// column_mapping: (src_col_index, dst_col_index)
///
/// Uses direct byte copying (memcpy) instead of decode/encode for efficiency.
pub fn project_row(
    src_descriptor: &RowDescriptor,
    src_data: &[u8],
    dst_descriptor: &RowDescriptor,
    column_mapping: &[(usize, usize)],
) -> Result<Vec<u8>, EncodingError> {
    // Build reverse lookup: dst_col -> src_col
    let mut dst_to_src: Vec<Option<usize>> = vec![None; dst_descriptor.columns.len()];
    for &(src_col, dst_col) in column_mapping {
        dst_to_src[dst_col] = Some(src_col);
    }

    let mut fixed_data = Vec::new();
    let mut var_data = Vec::new();
    let mut var_offsets: Vec<u32> = Vec::new();

    // 1. Copy fixed columns (in destination column order)
    for (dst_col, dst_col_desc) in dst_descriptor.columns.iter().enumerate() {
        if dst_col_desc.column_type.is_variable() {
            continue; // Handle variable columns separately
        }

        let value_size = dst_col_desc.column_type.fixed_size().unwrap();

        if let Some(src_col) = dst_to_src[dst_col] {
            let (bytes, is_null) = column_bytes_internal(src_descriptor, src_data, src_col)?;

            if dst_col_desc.nullable {
                if is_null {
                    fixed_data.push(0); // null marker
                    fixed_data.extend(std::iter::repeat_n(0, value_size));
                } else {
                    fixed_data.push(1); // present marker
                    fixed_data.extend_from_slice(bytes);
                }
            } else {
                // Destination is non-nullable, source must have a value
                fixed_data.extend_from_slice(bytes);
            }
        } else {
            // No mapping for this column - write null/zeros
            if dst_col_desc.nullable {
                fixed_data.push(0); // null marker
                fixed_data.extend(std::iter::repeat_n(0, value_size));
            } else {
                // Non-nullable with no source - this should be an error in practice
                // but we'll write zeros for compatibility
                fixed_data.extend(std::iter::repeat_n(0, value_size));
            }
        }
    }

    // 2. Copy variable columns (in destination column order)
    for (dst_col, dst_col_desc) in dst_descriptor.columns.iter().enumerate() {
        if !dst_col_desc.column_type.is_variable() {
            continue;
        }

        var_offsets.push(var_data.len() as u32);

        if let Some(src_col) = dst_to_src[dst_col] {
            let (bytes, is_null) = column_bytes_internal(src_descriptor, src_data, src_col)?;

            if dst_col_desc.nullable {
                if is_null {
                    var_data.push(0); // null marker only
                } else {
                    var_data.push(1); // present marker
                    var_data.extend_from_slice(bytes);
                }
            } else {
                var_data.extend_from_slice(bytes);
            }
        } else {
            // No mapping - write null marker if nullable, empty if not
            if dst_col_desc.nullable {
                var_data.push(0); // null marker
            }
            // Non-nullable variable with no source: empty (0 length)
        }
    }

    // 3. Build result: fixed_data + offset_table (skip first) + var_data
    let mut result = fixed_data;

    // Write offsets (skip first, as it's implicitly 0)
    for offset in var_offsets.iter().skip(1) {
        result.extend_from_slice(&offset.to_le_bytes());
    }

    result.extend(var_data);

    Ok(result)
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
    fn column_bytes_access() {
        let descriptor = test_descriptor();
        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(12345))),
            Value::Text("Alice".into()),
            Value::Integer(30),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();

        // Access integer column directly
        let age_bytes = column_bytes(&descriptor, &encoded, 2).unwrap().unwrap();
        assert_eq!(age_bytes.len(), 4);
        assert_eq!(i32::from_le_bytes(age_bytes.try_into().unwrap()), 30);

        // Access boolean column
        let active_bytes = column_bytes(&descriptor, &encoded, 3).unwrap().unwrap();
        assert_eq!(active_bytes, &[1]);

        // Access text column
        let name_bytes = column_bytes(&descriptor, &encoded, 1).unwrap().unwrap();
        assert_eq!(name_bytes, b"Alice");
    }

    #[test]
    fn column_eq_test() {
        let descriptor = test_descriptor();
        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(12345))),
            Value::Text("Alice".into()),
            Value::Integer(30),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();

        // Test equality
        assert!(column_eq(&descriptor, &encoded, 2, &30i32.to_le_bytes()).unwrap());
        assert!(!column_eq(&descriptor, &encoded, 2, &31i32.to_le_bytes()).unwrap());

        assert!(column_eq(&descriptor, &encoded, 1, b"Alice").unwrap());
        assert!(!column_eq(&descriptor, &encoded, 1, b"Bob").unwrap());
    }

    #[test]
    fn compare_column_test() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("score", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);

        let values1 = vec![Value::Integer(10), Value::Text("Alice".into())];
        let values2 = vec![Value::Integer(20), Value::Text("Bob".into())];

        let encoded1 = encode_row(&descriptor, &values1).unwrap();
        let encoded2 = encode_row(&descriptor, &values2).unwrap();

        // Integer comparison
        assert_eq!(
            compare_column(&descriptor, &encoded1, 0, &encoded2, 0).unwrap(),
            Ordering::Less
        );

        // Text comparison
        assert_eq!(
            compare_column(&descriptor, &encoded1, 1, &encoded2, 1).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn compare_nullable_columns() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("score", ColumnType::Integer).nullable(),
        ]);

        let with_value = vec![Value::Integer(10)];
        let with_null = vec![Value::Null];

        let encoded_value = encode_row(&descriptor, &with_value).unwrap();
        let encoded_null = encode_row(&descriptor, &with_null).unwrap();

        // Null < value
        assert_eq!(
            compare_column(&descriptor, &encoded_null, 0, &encoded_value, 0).unwrap(),
            Ordering::Less
        );

        // Value > null
        assert_eq!(
            compare_column(&descriptor, &encoded_value, 0, &encoded_null, 0).unwrap(),
            Ordering::Greater
        );

        // Null == null
        assert_eq!(
            compare_column(&descriptor, &encoded_null, 0, &encoded_null, 0).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn project_row_test() {
        let src_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("email", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ]);

        let dst_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ]);

        let src_values = vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Text("alice@example.com".into()),
            Value::Integer(30),
        ];

        let src_encoded = encode_row(&src_descriptor, &src_values).unwrap();

        // Map: src_name(1) -> dst_name(0), src_age(3) -> dst_age(1)
        let mapping = [(1, 0), (3, 1)];
        let dst_encoded =
            project_row(&src_descriptor, &src_encoded, &dst_descriptor, &mapping).unwrap();

        let dst_decoded = decode_row(&dst_descriptor, &dst_encoded).unwrap();
        assert_eq!(
            dst_decoded,
            vec![Value::Text("Alice".into()), Value::Integer(30)]
        );
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

        // Access each text column
        assert_eq!(
            column_bytes(&descriptor, &encoded, 1).unwrap().unwrap(),
            b"John"
        );
        assert_eq!(
            column_bytes(&descriptor, &encoded, 2).unwrap().unwrap(),
            b"Doe"
        );
        assert_eq!(
            column_bytes(&descriptor, &encoded, 3).unwrap().unwrap(),
            b"john.doe@example.com"
        );
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
        let array_type = ColumnType::Array(Box::new(ColumnType::Integer));
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Integer).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_integers() {
        let elements = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let array_type = ColumnType::Array(Box::new(ColumnType::Integer));
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Integer).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_single_integer() {
        let elements = vec![Value::Integer(42)];
        let array_type = ColumnType::Array(Box::new(ColumnType::Integer));
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
        let array_type = ColumnType::Array(Box::new(ColumnType::Text));
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Text).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_encode_decode_single_text() {
        let elements = vec![Value::Text("hello".into())];
        let array_type = ColumnType::Array(Box::new(ColumnType::Text));
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
        let array_type = ColumnType::Array(Box::new(ColumnType::Boolean));
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
        let array_type = ColumnType::Array(Box::new(ColumnType::Uuid));
        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &ColumnType::Uuid).unwrap();
        assert_eq!(decoded, elements);
    }

    #[test]
    fn array_in_row_roundtrip() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("tags", ColumnType::Array(Box::new(ColumnType::Text))),
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
            ColumnDescriptor::new("scores", ColumnType::Array(Box::new(ColumnType::Integer))),
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
            ColumnDescriptor::new("tags", ColumnType::Array(Box::new(ColumnType::Text))),
        ]);

        let values = vec![Value::Integer(1), Value::Array(vec![])];

        let encoded = encode_row(&descriptor, &values).unwrap();
        let decoded = decode_row(&descriptor, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn nested_array() {
        // Array of arrays of integers
        let inner_type = ColumnType::Array(Box::new(ColumnType::Integer));
        let array_type = ColumnType::Array(Box::new(inner_type.clone()));
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
        let row_type = ColumnType::Row(Box::new(row_desc.clone()));
        let array_type = ColumnType::Array(Box::new(row_type.clone()));

        let elements = vec![
            Value::Row(vec![Value::Integer(1), Value::Text("Alice".into())]),
            Value::Row(vec![Value::Integer(2), Value::Text("Bob".into())]),
        ];

        let encoded = encode_array(&elements, &array_type);
        let decoded = decode_array(&encoded, &row_type).unwrap();
        assert_eq!(decoded, elements);
    }

    // ========================================================================
    // project_row tests (for memcpy optimization validation)
    // ========================================================================

    #[test]
    fn project_all_fixed_columns() {
        // Integer, BigInt, Boolean, Timestamp, Uuid - all fixed-size types
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::BigInt),
            ColumnDescriptor::new("c", ColumnType::Boolean),
            ColumnDescriptor::new("d", ColumnType::Timestamp),
            ColumnDescriptor::new("e", ColumnType::Uuid),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("x", ColumnType::Integer),
            ColumnDescriptor::new("y", ColumnType::BigInt),
            ColumnDescriptor::new("z", ColumnType::Boolean),
            ColumnDescriptor::new("w", ColumnType::Timestamp),
            ColumnDescriptor::new("v", ColumnType::Uuid),
        ]);

        let src_values = vec![
            Value::Integer(42),
            Value::BigInt(1234567890123),
            Value::Boolean(true),
            Value::Timestamp(9999999999),
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(0xDEADBEEF))),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..5).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_all_variable_columns() {
        // Multiple Text columns
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Text),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Text),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("x", ColumnType::Text),
            ColumnDescriptor::new("y", ColumnType::Text),
            ColumnDescriptor::new("z", ColumnType::Text),
        ]);

        let src_values = vec![
            Value::Text("hello".into()),
            Value::Text("world".into()),
            Value::Text("rust".into()),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..3).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_mixed_columns() {
        // Fixed + variable interleaved
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("active", ColumnType::Boolean),
            ColumnDescriptor::new("bio", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::BigInt),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id2", ColumnType::Integer),
            ColumnDescriptor::new("name2", ColumnType::Text),
            ColumnDescriptor::new("active2", ColumnType::Boolean),
            ColumnDescriptor::new("bio2", ColumnType::Text),
            ColumnDescriptor::new("score2", ColumnType::BigInt),
        ]);

        let src_values = vec![
            Value::Integer(100),
            Value::Text("Alice".into()),
            Value::Boolean(false),
            Value::Text("Developer from NYC".into()),
            Value::BigInt(9999),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..5).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_with_nullable_present() {
        // Nullable columns that have values
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text).nullable(),
            ColumnDescriptor::new("score", ColumnType::Integer).nullable(),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("x", ColumnType::Integer),
            ColumnDescriptor::new("y", ColumnType::Text).nullable(),
            ColumnDescriptor::new("z", ColumnType::Integer).nullable(),
        ]);

        let src_values = vec![
            Value::Integer(1),
            Value::Text("Bob".into()),
            Value::Integer(95),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..3).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_with_nullable_null() {
        // Nullable columns that are null
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text).nullable(),
            ColumnDescriptor::new("score", ColumnType::Integer).nullable(),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("x", ColumnType::Integer),
            ColumnDescriptor::new("y", ColumnType::Text).nullable(),
            ColumnDescriptor::new("z", ColumnType::Integer).nullable(),
        ]);

        let src_values = vec![Value::Integer(1), Value::Null, Value::Null];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..3).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_single_column_fixed() {
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::BigInt),
        ]);

        let dst_desc =
            RowDescriptor::new(vec![ColumnDescriptor::new("only_c", ColumnType::BigInt)]);

        let src_values = vec![
            Value::Integer(1),
            Value::Text("ignored".into()),
            Value::BigInt(12345),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping = [(2, 0)]; // src col 2 -> dst col 0
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, vec![Value::BigInt(12345)]);
    }

    #[test]
    fn project_single_column_variable() {
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Text),
        ]);

        let dst_desc = RowDescriptor::new(vec![ColumnDescriptor::new("only_b", ColumnType::Text)]);

        let src_values = vec![
            Value::Integer(1),
            Value::Text("selected".into()),
            Value::Text("ignored".into()),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping = [(1, 0)]; // src col 1 -> dst col 0
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, vec![Value::Text("selected".into())]);
    }

    #[test]
    fn project_reorder_columns() {
        // Destination order different from source
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Boolean),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("c_first", ColumnType::Boolean),
            ColumnDescriptor::new("a_second", ColumnType::Integer),
            ColumnDescriptor::new("b_third", ColumnType::Text),
        ]);

        let src_values = vec![
            Value::Integer(42),
            Value::Text("hello".into()),
            Value::Boolean(true),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        // Reorder: src c(2) -> dst 0, src a(0) -> dst 1, src b(1) -> dst 2
        let mapping = [(2, 0), (0, 1), (1, 2)];
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(
            decoded,
            vec![
                Value::Boolean(true),
                Value::Integer(42),
                Value::Text("hello".into())
            ]
        );
    }

    #[test]
    fn project_subset_of_columns() {
        // Skip some columns
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Boolean),
            ColumnDescriptor::new("d", ColumnType::Text),
            ColumnDescriptor::new("e", ColumnType::BigInt),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("b_only", ColumnType::Text),
            ColumnDescriptor::new("e_only", ColumnType::BigInt),
        ]);

        let src_values = vec![
            Value::Integer(1),
            Value::Text("pick_me".into()),
            Value::Boolean(false),
            Value::Text("skip_me".into()),
            Value::BigInt(999),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        // Pick b(1) and e(4) only
        let mapping = [(1, 0), (4, 1)];
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(
            decoded,
            vec![Value::Text("pick_me".into()), Value::BigInt(999)]
        );
    }

    #[test]
    fn project_with_empty_text() {
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer),
            ColumnDescriptor::new("b", ColumnType::Text),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("x", ColumnType::Integer),
            ColumnDescriptor::new("y", ColumnType::Text),
        ]);

        let src_values = vec![Value::Integer(1), Value::Text("".into())];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping = [(0, 0), (1, 1)];
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_with_large_text() {
        // 10KB+ text
        let large_text = "x".repeat(15_000);

        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("content", ColumnType::Text),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id2", ColumnType::Integer),
            ColumnDescriptor::new("content2", ColumnType::Text),
        ]);

        let src_values = vec![Value::Integer(1), Value::Text(large_text.clone())];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping = [(0, 0), (1, 1)];
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
    }

    #[test]
    fn project_identity_all_columns() {
        // Full row copy (all columns in order)
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Uuid),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Integer),
            ColumnDescriptor::new("d", ColumnType::Boolean),
            ColumnDescriptor::new("e", ColumnType::Text),
        ]);

        let values = vec![
            Value::Uuid(ObjectId::from_uuid(Uuid::from_u128(12345))),
            Value::Text("Alice".into()),
            Value::Integer(30),
            Value::Boolean(true),
            Value::Text("extra".into()),
        ];

        let encoded = encode_row(&descriptor, &values).unwrap();

        // Identity projection
        let mapping: Vec<(usize, usize)> = (0..5).map(|i| (i, i)).collect();
        let projected = project_row(&descriptor, &encoded, &descriptor, &mapping).unwrap();

        let decoded = decode_row(&descriptor, &projected).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn project_only_variable_columns_reordered() {
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("first", ColumnType::Text),
            ColumnDescriptor::new("second", ColumnType::Text),
            ColumnDescriptor::new("third", ColumnType::Text),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("was_third", ColumnType::Text),
            ColumnDescriptor::new("was_first", ColumnType::Text),
        ]);

        let src_values = vec![
            Value::Text("AAA".into()),
            Value::Text("BBB".into()),
            Value::Text("CCC".into()),
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        // Pick third(2) -> 0, first(0) -> 1
        let mapping = [(2, 0), (0, 1)];
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(
            decoded,
            vec![Value::Text("CCC".into()), Value::Text("AAA".into())]
        );
    }

    #[test]
    fn project_mixed_nullable() {
        // Mix of nullable and non-nullable, some null some not
        let src_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("a", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("b", ColumnType::Text),
            ColumnDescriptor::new("c", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("d", ColumnType::Text).nullable(),
        ]);

        let dst_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("w", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("x", ColumnType::Text),
            ColumnDescriptor::new("y", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("z", ColumnType::Text).nullable(),
        ]);

        let src_values = vec![
            Value::Null,                    // nullable, is null
            Value::Text("required".into()), // non-nullable
            Value::Integer(42),             // nullable, has value
            Value::Null,                    // nullable, is null
        ];

        let src_encoded = encode_row(&src_desc, &src_values).unwrap();
        let mapping: Vec<(usize, usize)> = (0..4).map(|i| (i, i)).collect();
        let dst_encoded = project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap();

        let decoded = decode_row(&dst_desc, &dst_encoded).unwrap();
        assert_eq!(decoded, src_values);
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
    fn compare_column_to_value_rejects_ordering_on_bytea() {
        let descriptor =
            RowDescriptor::new(vec![ColumnDescriptor::new("payload", ColumnType::Bytea)]);
        let encoded = encode_row(&descriptor, &[Value::Bytea(vec![1, 2, 3])]).unwrap();

        let err = compare_column_to_value(&descriptor, &encoded, 0, &[1, 2, 4]).unwrap_err();
        assert!(matches!(err, EncodingError::UnsupportedComparison { .. }));
    }

    #[test]
    fn encode_row_rejects_invalid_enum_variant() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "status",
            ColumnType::Enum(vec!["done".to_string(), "todo".to_string()]),
        )]);

        let err = encode_row(&descriptor, &[Value::Text("invalid".to_string())]).unwrap_err();
        assert!(matches!(err, EncodingError::TypeMismatch { .. }));
    }

    #[test]
    fn decode_row_rejects_invalid_enum_variant() {
        let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new(
            "status",
            ColumnType::Enum(vec!["done".to_string(), "todo".to_string()]),
        )]);

        let mut encoded = encode_row(&descriptor, &[Value::Text("todo".to_string())]).unwrap();
        encoded.as_mut_slice().clone_from_slice(b"nope");

        let err = decode_row(&descriptor, &encoded).unwrap_err();
        assert!(matches!(err, EncodingError::MalformedData { .. }));
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
