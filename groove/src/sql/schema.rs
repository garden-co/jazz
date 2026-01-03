/// Column type definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// Boolean: 1 byte (0x00 = false, 0x01 = true)
    Bool,
    /// Signed 64-bit integer: 8 bytes, little-endian
    I64,
    /// 64-bit float: 8 bytes, IEEE 754 little-endian
    F64,
    /// UTF-8 string: varint length in header, data in body
    String,
    /// Raw bytes: varint length in header, data in body
    Bytes,
    /// Reference to another table: 16 bytes (u128 object ID)
    Ref(String),
}

impl ColumnType {
    /// Returns true if this is a fixed-size type.
    pub fn is_fixed_size(&self) -> bool {
        matches!(self, ColumnType::Bool | ColumnType::I64 | ColumnType::F64 | ColumnType::Ref(_))
    }

    /// Returns the fixed size in bytes, or None for variable-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Bool => Some(1),
            ColumnType::I64 => Some(8),
            ColumnType::F64 => Some(8),
            ColumnType::Ref(_) => Some(16),
            ColumnType::String | ColumnType::Bytes => None,
        }
    }
}

/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool) -> Self {
        ColumnDef {
            name: name.into(),
            ty,
            nullable,
        }
    }

    /// Create a non-nullable column.
    pub fn required(name: impl Into<String>, ty: ColumnType) -> Self {
        Self::new(name, ty, false)
    }

    /// Create a nullable column.
    pub fn optional(name: impl Into<String>, ty: ColumnType) -> Self {
        Self::new(name, ty, true)
    }
}

/// Table schema definition.
/// Each table implicitly has an `id` column (Object ID / UUIDv7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        TableSchema {
            name: name.into(),
            columns,
        }
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Count of variable-size columns (for header).
    pub fn variable_column_count(&self) -> usize {
        self.columns.iter().filter(|c| !c.ty.is_fixed_size()).count()
    }

    /// Serialize schema to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Table name: length (u16) + bytes
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        // Column count (u16)
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        // Each column
        for col in &self.columns {
            // Column name: length (u16) + bytes
            let col_name = col.name.as_bytes();
            buf.extend_from_slice(&(col_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(col_name);

            // Column type tag (u8)
            let type_tag = match &col.ty {
                ColumnType::Bool => 0u8,
                ColumnType::I64 => 1,
                ColumnType::F64 => 2,
                ColumnType::String => 3,
                ColumnType::Bytes => 4,
                ColumnType::Ref(_) => 5,
            };
            buf.push(type_tag);

            // If Ref, add target table name
            if let ColumnType::Ref(target) = &col.ty {
                let target_bytes = target.as_bytes();
                buf.extend_from_slice(&(target_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(target_bytes);
            }

            // Nullable flag (u8)
            buf.push(if col.nullable { 1 } else { 0 });
        }

        buf
    }

    /// Deserialize schema from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SchemaError> {
        let mut pos = 0;

        // Table name
        if data.len() < pos + 2 {
            return Err(SchemaError::UnexpectedEof);
        }
        let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if data.len() < pos + name_len {
            return Err(SchemaError::UnexpectedEof);
        }
        let name = std::str::from_utf8(&data[pos..pos + name_len])
            .map_err(|_| SchemaError::InvalidUtf8)?
            .to_string();
        pos += name_len;

        // Column count
        if data.len() < pos + 2 {
            return Err(SchemaError::UnexpectedEof);
        }
        let col_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Parse columns
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            // Column name
            if data.len() < pos + 2 {
                return Err(SchemaError::UnexpectedEof);
            }
            let col_name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if data.len() < pos + col_name_len {
                return Err(SchemaError::UnexpectedEof);
            }
            let col_name = std::str::from_utf8(&data[pos..pos + col_name_len])
                .map_err(|_| SchemaError::InvalidUtf8)?
                .to_string();
            pos += col_name_len;

            // Type tag
            if data.len() < pos + 1 {
                return Err(SchemaError::UnexpectedEof);
            }
            let type_tag = data[pos];
            pos += 1;

            let ty = match type_tag {
                0 => ColumnType::Bool,
                1 => ColumnType::I64,
                2 => ColumnType::F64,
                3 => ColumnType::String,
                4 => ColumnType::Bytes,
                5 => {
                    // Ref: read target table name
                    if data.len() < pos + 2 {
                        return Err(SchemaError::UnexpectedEof);
                    }
                    let target_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;

                    if data.len() < pos + target_len {
                        return Err(SchemaError::UnexpectedEof);
                    }
                    let target = std::str::from_utf8(&data[pos..pos + target_len])
                        .map_err(|_| SchemaError::InvalidUtf8)?
                        .to_string();
                    pos += target_len;

                    ColumnType::Ref(target)
                }
                _ => return Err(SchemaError::InvalidTypeTag(type_tag)),
            };

            // Nullable flag
            if data.len() < pos + 1 {
                return Err(SchemaError::UnexpectedEof);
            }
            let nullable = data[pos] != 0;
            pos += 1;

            columns.push(ColumnDef {
                name: col_name,
                ty,
                nullable,
            });
        }

        Ok(TableSchema { name, columns })
    }
}

/// Errors during schema parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    UnexpectedEof,
    InvalidUtf8,
    InvalidTypeTag(u8),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::UnexpectedEof => write!(f, "unexpected end of schema data"),
            SchemaError::InvalidUtf8 => write!(f, "invalid UTF-8 in schema"),
            SchemaError::InvalidTypeTag(tag) => write!(f, "invalid type tag: {}", tag),
        }
    }
}

impl std::error::Error for SchemaError {}

// Tests have been moved to tests/sql_schema.rs
