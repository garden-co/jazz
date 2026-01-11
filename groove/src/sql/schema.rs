use std::sync::Arc;

use super::row_buffer::RowDescriptor;

/// Column type definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// Boolean: 1 byte (0x00 = false, 0x01 = true)
    Bool,
    /// Signed 32-bit integer: 4 bytes, little-endian (fits in JS number)
    I32,
    /// Unsigned 32-bit integer: 4 bytes, little-endian (fits in JS number)
    U32,
    /// Signed 64-bit integer: 8 bytes, little-endian (requires JS BigInt)
    I64,
    /// 64-bit float: 8 bytes, IEEE 754 little-endian
    F64,
    /// UTF-8 string: varint length in header, data in body
    String,
    /// Raw bytes: varint length in header, data in body
    Bytes,
    /// Primary key ObjectId: 16 bytes (u128 object ID).
    /// Unlike Ref, this doesn't reference another table - it's the row's own identity.
    ObjectId,
    /// Reference to another table: 16 bytes (u128 object ID)
    Ref(String),
    /// Large binary data, potentially chunked via ContentRef.
    /// Unlike Bytes (always inline), Blob can be large and is stored as
    /// either inline bytes or a list of chunk hashes.
    Blob,
    /// Array of blobs.
    BlobArray,
    /// Array of rows (used for ARRAY_AGG and array subqueries).
    /// Contains the descriptor for each item in the array.
    Array(Arc<RowDescriptor>),
}

impl ColumnType {
    /// Returns true if this is a fixed-size type.
    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            ColumnType::Bool
                | ColumnType::I32
                | ColumnType::U32
                | ColumnType::I64
                | ColumnType::F64
                | ColumnType::ObjectId
                | ColumnType::Ref(_)
        )
    }

    /// Returns the fixed size in bytes, or None for variable-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Bool => Some(1),
            ColumnType::I32 | ColumnType::U32 => Some(4),
            ColumnType::I64 => Some(8),
            ColumnType::F64 => Some(8),
            ColumnType::ObjectId => Some(16),
            ColumnType::Ref(_) => Some(16),
            ColumnType::String
            | ColumnType::Bytes
            | ColumnType::Blob
            | ColumnType::BlobArray
            | ColumnType::Array(_) => None,
        }
    }

    /// Returns the fixed size in bytes accounting for nullability.
    /// Nullable fixed-size types have an extra presence byte.
    pub fn fixed_size_nullable(&self, nullable: bool) -> Option<usize> {
        self.fixed_size().map(|size| if nullable { size + 1 } else { size })
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

    /// Create the standard `id` column (ObjectId primary key).
    /// This is a non-nullable ObjectId column.
    pub fn id() -> Self {
        Self::new("id", ColumnType::ObjectId, false)
    }
}

/// Table schema definition.
/// Every table has an explicit `id` column (ObjectId / UUIDv7) as the first column.
/// This is automatically prepended by `TableSchema::new()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    /// Create a new table schema.
    /// Automatically prepends the `id` column as the first column.
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        let mut all_columns = Vec::with_capacity(columns.len() + 1);
        all_columns.push(ColumnDef::id());
        all_columns.extend(columns);
        TableSchema {
            name: name.into(),
            columns: all_columns,
        }
    }

    /// Create a table schema without auto-prepending the `id` column.
    /// Used internally for combined/qualified schemas from JOINs.
    pub fn new_raw(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
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
    ///
    /// Supports both unqualified names ("title") and qualified names ("documents.title").
    /// For qualified names in a combined schema, finds the column with that exact name.
    /// For qualified names in a single-table schema, the table prefix must match.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        // First try exact match (works for combined schemas with qualified column names)
        if let Some(idx) = self.columns.iter().position(|c| c.name == name) {
            return Some(idx);
        }

        // For qualified names, try matching table.column against this schema
        if let Some((table, col)) = name.split_once('.') {
            if table == self.name {
                return self.columns.iter().position(|c| c.name == col);
            }
        }

        None
    }

    /// Create a combined schema by joining this schema with another.
    ///
    /// The resulting schema has columns from both tables, prefixed with their
    /// table names (e.g., "documents.id", "documents.title", "folders.id", "folders.name").
    pub fn combine(&self, other: &TableSchema) -> TableSchema {
        let mut combined_columns = Vec::new();

        // Add columns from this table with qualified names (including id)
        for col in &self.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", self.name, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }

        // Add columns from other table with qualified names (including id)
        for col in &other.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", other.name, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }

        TableSchema::new_raw(format!("{}+{}", self.name, other.name), combined_columns)
    }

    /// Extend this schema with columns from another table.
    ///
    /// Unlike `combine()`, this preserves existing column names unchanged and
    /// only qualifies the new table's columns. This is used for chain joins
    /// where the input already has qualified column names.
    pub fn extend_with(&self, other: &TableSchema) -> TableSchema {
        let mut combined_columns = self.columns.clone();

        // Add columns from other table with qualified names (including id)
        for col in &other.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", other.name, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }

        TableSchema::new_raw(format!("{}+{}", self.name, other.name), combined_columns)
    }

    /// Create a schema with qualified column names (table.column format).
    ///
    /// This is used when projecting a single table from a JOIN result.
    pub fn qualify(&self, table_name: &str) -> TableSchema {
        let qualified_columns = self.columns.iter().map(|col| {
            ColumnDef {
                name: format!("{}.{}", table_name, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            }
        }).collect();

        TableSchema::new_raw(table_name.to_string(), qualified_columns)
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
                ColumnType::I32 => 6,
                ColumnType::U32 => 7,
                ColumnType::Blob => 8,
                ColumnType::BlobArray => 9,
                ColumnType::ObjectId => 10,
                ColumnType::Array(_) => panic!("Array columns cannot be serialized in table schemas"),
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
                6 => ColumnType::I32,
                7 => ColumnType::U32,
                8 => ColumnType::Blob,
                9 => ColumnType::BlobArray,
                10 => ColumnType::ObjectId,
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

        Ok(TableSchema::new_raw(name, columns))
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
