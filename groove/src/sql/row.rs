use crate::sql::schema::{ColumnType, TableSchema};
use crate::object::ObjectId;

/// Runtime value representation.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Ref(ObjectId),
}

impl Value {
    /// Check if value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(n) => Some(*n),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(n) => Some(*n),
            _ => None,
        }
    }

    /// Try to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get as ref (object ID).
    pub fn as_ref(&self) -> Option<ObjectId> {
        match self {
            Value::Ref(id) => Some(*id),
            _ => None,
        }
    }
}

/// A row with its object ID and values.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub id: ObjectId,
    pub values: Vec<Value>,
}

impl Row {
    /// Create a new row with generated ID.
    pub fn new(id: ObjectId, values: Vec<Value>) -> Self {
        Row { id, values }
    }

    /// Get value by column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// Encode a varint (LEB128 unsigned).
fn encode_varint(mut value: usize, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint (LEB128 unsigned). Returns (value, bytes_consumed).
fn decode_varint(data: &[u8]) -> Result<(usize, usize), RowError> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(RowError::VarintOverflow);
        }
    }

    Err(RowError::UnexpectedEof)
}

/// Encode row values to binary format.
///
/// Format:
/// - Length-prefix header: one varint per variable-size column
/// - Column values in schema order
///
/// For nullable columns, each value is prefixed with a 1-byte presence flag.
pub fn encode_row(values: &[Value], schema: &TableSchema) -> Result<Vec<u8>, RowError> {
    if values.len() != schema.columns.len() {
        return Err(RowError::ColumnCountMismatch {
            expected: schema.columns.len(),
            got: values.len(),
        });
    }

    // First pass: compute variable column lengths and encode values
    let mut encoded_values: Vec<Vec<u8>> = Vec::with_capacity(values.len());
    let mut variable_lengths: Vec<usize> = Vec::new();

    for (value, col) in values.iter().zip(&schema.columns) {
        // Check null validity
        if value.is_null() && !col.nullable {
            return Err(RowError::NullInNonNullable {
                column: col.name.clone(),
            });
        }

        // Encode this column's value
        let encoded = encode_column_value(value, &col.ty, col.nullable)?;

        // Track length for variable columns
        if !col.ty.is_fixed_size() {
            variable_lengths.push(encoded.len());
        }

        encoded_values.push(encoded);
    }

    // Build the output: header + values
    let mut buf = Vec::new();

    // Header: varints for variable column lengths
    for len in &variable_lengths {
        encode_varint(*len, &mut buf);
    }

    // Values
    for encoded in encoded_values {
        buf.extend(encoded);
    }

    Ok(buf)
}

/// Encode a single column value.
fn encode_column_value(value: &Value, ty: &ColumnType, nullable: bool) -> Result<Vec<u8>, RowError> {
    let mut buf = Vec::new();

    // Nullable prefix
    if nullable {
        if value.is_null() {
            buf.push(0x00);
            // For fixed-size types, still write placeholder bytes
            if let Some(size) = ty.fixed_size() {
                buf.extend(std::iter::repeat(0u8).take(size));
            }
            return Ok(buf);
        } else {
            buf.push(0x01);
        }
    } else if value.is_null() {
        return Err(RowError::NullInNonNullable {
            column: "unknown".into(),
        });
    }

    // Encode the actual value
    match (value, ty) {
        (Value::Bool(b), ColumnType::Bool) => {
            buf.push(if *b { 0x01 } else { 0x00 });
        }
        (Value::I64(n), ColumnType::I64) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        (Value::F64(n), ColumnType::F64) => {
            buf.extend_from_slice(&n.to_le_bytes());
        }
        (Value::String(s), ColumnType::String) => {
            buf.extend_from_slice(s.as_bytes());
        }
        (Value::Bytes(b), ColumnType::Bytes) => {
            buf.extend_from_slice(b);
        }
        (Value::Ref(id), ColumnType::Ref(_)) => {
            buf.extend_from_slice(&id.0.to_le_bytes());
        }
        _ => {
            return Err(RowError::TypeMismatch {
                expected: format!("{:?}", ty),
                got: format!("{:?}", value),
            });
        }
    }

    Ok(buf)
}

/// Decode row values from binary format.
pub fn decode_row(data: &[u8], schema: &TableSchema) -> Result<Vec<Value>, RowError> {
    let mut pos = 0;

    // Read header: lengths of variable columns
    let mut variable_lengths: Vec<usize> = Vec::new();
    for col in &schema.columns {
        if !col.ty.is_fixed_size() {
            let (len, consumed) = decode_varint(&data[pos..])?;
            variable_lengths.push(len);
            pos += consumed;
        }
    }

    // Read column values
    let mut values = Vec::with_capacity(schema.columns.len());
    let mut var_idx = 0;

    for col in &schema.columns {
        let value = if col.ty.is_fixed_size() {
            decode_fixed_column(&data[pos..], &col.ty, col.nullable)?
        } else {
            let len = variable_lengths[var_idx];
            var_idx += 1;
            decode_variable_column(&data[pos..pos + len], &col.ty, col.nullable)?
        };

        // Advance position
        if col.ty.is_fixed_size() {
            let base_size = col.ty.fixed_size().unwrap();
            pos += if col.nullable { 1 + base_size } else { base_size };
        } else {
            pos += variable_lengths[var_idx - 1];
        }

        values.push(value);
    }

    Ok(values)
}

/// Decode a fixed-size column value.
fn decode_fixed_column(data: &[u8], ty: &ColumnType, nullable: bool) -> Result<Value, RowError> {
    let mut pos = 0;

    // Check null flag
    if nullable {
        if data.is_empty() {
            return Err(RowError::UnexpectedEof);
        }
        if data[0] == 0x00 {
            return Ok(Value::Null);
        }
        pos = 1;
    }

    match ty {
        ColumnType::Bool => {
            if data.len() < pos + 1 {
                return Err(RowError::UnexpectedEof);
            }
            Ok(Value::Bool(data[pos] != 0))
        }
        ColumnType::I64 => {
            if data.len() < pos + 8 {
                return Err(RowError::UnexpectedEof);
            }
            let bytes: [u8; 8] = data[pos..pos + 8].try_into().unwrap();
            Ok(Value::I64(i64::from_le_bytes(bytes)))
        }
        ColumnType::F64 => {
            if data.len() < pos + 8 {
                return Err(RowError::UnexpectedEof);
            }
            let bytes: [u8; 8] = data[pos..pos + 8].try_into().unwrap();
            Ok(Value::F64(f64::from_le_bytes(bytes)))
        }
        ColumnType::Ref(_) => {
            if data.len() < pos + 16 {
                return Err(RowError::UnexpectedEof);
            }
            let bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            Ok(Value::Ref(ObjectId::from_le_bytes(bytes)))
        }
        _ => Err(RowError::TypeMismatch {
            expected: "fixed-size type".into(),
            got: format!("{:?}", ty),
        }),
    }
}

/// Decode a variable-size column value.
fn decode_variable_column(data: &[u8], ty: &ColumnType, nullable: bool) -> Result<Value, RowError> {
    let mut pos = 0;

    // Check null flag
    if nullable {
        if data.is_empty() {
            return Err(RowError::UnexpectedEof);
        }
        if data[0] == 0x00 {
            return Ok(Value::Null);
        }
        pos = 1;
    }

    match ty {
        ColumnType::String => {
            let s = std::str::from_utf8(&data[pos..]).map_err(|_| RowError::InvalidUtf8)?;
            Ok(Value::String(s.to_string()))
        }
        ColumnType::Bytes => {
            Ok(Value::Bytes(data[pos..].to_vec()))
        }
        _ => Err(RowError::TypeMismatch {
            expected: "variable-size type".into(),
            got: format!("{:?}", ty),
        }),
    }
}

/// Errors during row encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowError {
    UnexpectedEof,
    VarintOverflow,
    InvalidUtf8,
    ColumnCountMismatch { expected: usize, got: usize },
    NullInNonNullable { column: String },
    TypeMismatch { expected: String, got: String },
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowError::UnexpectedEof => write!(f, "unexpected end of row data"),
            RowError::VarintOverflow => write!(f, "varint overflow"),
            RowError::InvalidUtf8 => write!(f, "invalid UTF-8 in row data"),
            RowError::ColumnCountMismatch { expected, got } => {
                write!(f, "column count mismatch: expected {}, got {}", expected, got)
            }
            RowError::NullInNonNullable { column } => {
                write!(f, "null value in non-nullable column: {}", column)
            }
            RowError::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {}, got {}", expected, got)
            }
        }
    }
}

impl std::error::Error for RowError {}

/// Unit tests for private APIs only.
/// Most tests have been moved to tests/sql_row.rs.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 1_000_000];

        for &value in &test_values {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let (decoded, _) = decode_varint(&buf).unwrap();
            assert_eq!(value, decoded, "varint roundtrip failed for {}", value);
        }
    }
}
