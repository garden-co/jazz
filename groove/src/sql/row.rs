/// Errors during row encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowError {
    UnexpectedEof,
    VarintOverflow,
    InvalidUtf8,
    ColumnCountMismatch { expected: usize, got: usize },
    NullInNonNullable { column: String },
    TypeMismatch { expected: String, got: String },
    BlobDecodeError(String),
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowError::UnexpectedEof => write!(f, "unexpected end of row data"),
            RowError::VarintOverflow => write!(f, "varint overflow"),
            RowError::InvalidUtf8 => write!(f, "invalid UTF-8 in row data"),
            RowError::ColumnCountMismatch { expected, got } => {
                write!(
                    f,
                    "column count mismatch: expected {}, got {}",
                    expected, got
                )
            }
            RowError::NullInNonNullable { column } => {
                write!(f, "null value in non-nullable column: {}", column)
            }
            RowError::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {}, got {}", expected, got)
            }
            RowError::BlobDecodeError(msg) => {
                write!(f, "blob decode error: {}", msg)
            }
        }
    }
}

impl std::error::Error for RowError {}
