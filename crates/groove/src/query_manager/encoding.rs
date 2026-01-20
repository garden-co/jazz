use std::cmp::Ordering;

use crate::object::ObjectId;

use super::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

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
        if !val.is_null() && val.column_type().is_some_and(|t| t != col.column_type) {
            return Err(EncodingError::TypeMismatch {
                column: col.name.clone(),
                expected: col.column_type,
                actual: val.column_type(),
            });
        }

        // Check null allowed
        if val.is_null() && !col.nullable {
            return Err(EncodingError::NullNotAllowed {
                column: col.name.clone(),
            });
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
        Value::Boolean(b) => buf.push(if *b { 1 } else { 0 }),
        Value::Timestamp(t) => buf.extend_from_slice(&t.to_le_bytes()),
        Value::Uuid(id) => buf.extend_from_slice(id.0.as_bytes()),
        Value::Null => {
            // Should not reach here for non-nullable (validated above)
            let size = col.column_type.fixed_size().unwrap();
            buf.extend(std::iter::repeat_n(0, size));
        }
        Value::Text(_) => unreachable!("Text is not fixed-size"),
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
        Value::Null => {} // Already handled above for nullable
        _ => unreachable!("Non-text types are fixed-size"),
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
    match col.column_type {
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
            Ok(Value::Uuid(ObjectId(uuid)))
        }
        ColumnType::Text => {
            let s = std::str::from_utf8(bytes).map_err(|e| EncodingError::MalformedData {
                message: format!("invalid utf8: {e}"),
            })?;
            Ok(Value::Text(s.to_string()))
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

    let var_data_start = fixed_size + offset_table_size;

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

    let col = &descriptor.columns[col_index];
    let bytes = &data[var_data_start + start_offset..var_data_start + end_offset];

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

    match col.column_type {
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
        ColumnType::Text => {
            // Lexicographic comparison of UTF-8 bytes
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

    match col.column_type {
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
        ColumnType::Uuid => Ok(bytes.cmp(value)),
        ColumnType::Text => Ok(bytes.cmp(value)),
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
pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Integer(n) => n.to_le_bytes().to_vec(),
        Value::BigInt(n) => n.to_le_bytes().to_vec(),
        Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
        Value::Timestamp(t) => t.to_le_bytes().to_vec(),
        Value::Uuid(id) => id.0.as_bytes().to_vec(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Null => vec![],
    }
}

/// Project columns from a source row to create a new row (for projections).
/// column_mapping: (src_col_index, dst_col_index)
pub fn project_row(
    src_descriptor: &RowDescriptor,
    src_data: &[u8],
    dst_descriptor: &RowDescriptor,
    column_mapping: &[(usize, usize)],
) -> Result<Vec<u8>, EncodingError> {
    // Decode source values for mapped columns
    let mut dst_values = vec![Value::Null; dst_descriptor.columns.len()];

    for &(src_col, dst_col) in column_mapping {
        let value = decode_column(src_descriptor, src_data, src_col)?;
        dst_values[dst_col] = value;
    }

    // Encode to destination format
    encode_row(dst_descriptor, &dst_values)
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
            Value::Uuid(ObjectId(Uuid::from_u128(12345))),
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
            Value::Uuid(ObjectId(Uuid::from_u128(1))),
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
            Value::Uuid(ObjectId(Uuid::from_u128(1))),
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
        let values = vec![Value::Uuid(ObjectId(Uuid::from_u128(1)))];

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
            Value::Uuid(ObjectId(Uuid::from_u128(12345))),
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
            Value::Uuid(ObjectId(Uuid::from_u128(12345))),
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
}
