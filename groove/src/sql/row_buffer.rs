//! Unified row representation with zero-copy buffer format.
//!
//! This module provides a standardized row representation that uses a single
//! buffer format across storage, memory, and WASM boundaries. The design
//! prioritizes zero-copy reads and efficient operations like projection and JOIN.
//!
//! # Buffer Layout
//!
//! ```text
//! [fixed-size columns in descriptor order]
//! [variable-size columns: varint length prefix + data, in descriptor order]
//! ```
//!
//! Fixed-size columns come first to enable O(1) random access. Variable-size
//! columns follow with varint length prefixes.
//!
//! # Nullable Columns
//!
//! Nullable columns have a 1-byte presence flag (0x00 = null, 0x01 = present).
//! For fixed-size types, the flag is followed by the value bytes (or zeros if null).
//! For variable-size types, if null the length is 0.

use std::sync::Arc;

use crate::object::ObjectId;
use crate::storage::ContentRef;

use super::row::{Row, Value};
use super::schema::{ColumnType, TableSchema};

/// Column type for the unified row format.
///
/// This enum determines how column values are encoded in the buffer.
/// Fixed-size types enable O(1) access, variable-size types require
/// scanning the variable section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColType {
    // Fixed-size types (come first in buffer)
    Bool,
    I32,
    U32,
    I64,
    F64,
    /// ObjectId reference - 16 bytes
    Ref,

    // Nullable fixed-size types (1 byte presence + value)
    NullableBool,
    NullableI32,
    NullableU32,
    NullableI64,
    NullableF64,
    NullableRef,

    // Variable-size types (come after fixed in buffer)
    String,
    Bytes,
    Blob,
    BlobArray,

    // Nullable variable-size types
    NullableString,
    NullableBytes,
    NullableBlob,
    NullableBlobArray,
}

impl ColType {
    /// Convert from the existing ColumnType.
    pub fn from_column_type(ct: &ColumnType, nullable: bool) -> Self {
        let base = match ct {
            ColumnType::Bool => ColType::Bool,
            ColumnType::I32 => ColType::I32,
            ColumnType::U32 => ColType::U32,
            ColumnType::I64 => ColType::I64,
            ColumnType::F64 => ColType::F64,
            ColumnType::String => ColType::String,
            ColumnType::Bytes => ColType::Bytes,
            ColumnType::Ref(_) => ColType::Ref,
            ColumnType::Blob => ColType::Blob,
            ColumnType::BlobArray => ColType::BlobArray,
        };
        if nullable {
            base.to_nullable()
        } else {
            base
        }
    }

    /// Returns true if this is a fixed-size type.
    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            ColType::Bool
                | ColType::I32
                | ColType::U32
                | ColType::I64
                | ColType::F64
                | ColType::Ref
                | ColType::NullableBool
                | ColType::NullableI32
                | ColType::NullableU32
                | ColType::NullableI64
                | ColType::NullableF64
                | ColType::NullableRef
        )
    }

    /// Returns the fixed size in bytes, or None for variable-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColType::Bool => Some(1),
            ColType::I32 | ColType::U32 => Some(4),
            ColType::I64 | ColType::F64 => Some(8),
            ColType::Ref => Some(16),
            // Nullable: 1 byte presence + value
            ColType::NullableBool => Some(2),
            ColType::NullableI32 | ColType::NullableU32 => Some(5),
            ColType::NullableI64 | ColType::NullableF64 => Some(9),
            ColType::NullableRef => Some(17),
            // Variable-size
            ColType::String
            | ColType::Bytes
            | ColType::Blob
            | ColType::BlobArray
            | ColType::NullableString
            | ColType::NullableBytes
            | ColType::NullableBlob
            | ColType::NullableBlobArray => None,
        }
    }

    /// Returns true if this is a nullable type.
    pub fn is_nullable(&self) -> bool {
        matches!(
            self,
            ColType::NullableBool
                | ColType::NullableI32
                | ColType::NullableU32
                | ColType::NullableI64
                | ColType::NullableF64
                | ColType::NullableRef
                | ColType::NullableString
                | ColType::NullableBytes
                | ColType::NullableBlob
                | ColType::NullableBlobArray
        )
    }

    /// Returns the non-nullable version of this type.
    pub fn to_non_nullable(&self) -> ColType {
        match self {
            ColType::NullableBool => ColType::Bool,
            ColType::NullableI32 => ColType::I32,
            ColType::NullableU32 => ColType::U32,
            ColType::NullableI64 => ColType::I64,
            ColType::NullableF64 => ColType::F64,
            ColType::NullableRef => ColType::Ref,
            ColType::NullableString => ColType::String,
            ColType::NullableBytes => ColType::Bytes,
            ColType::NullableBlob => ColType::Blob,
            ColType::NullableBlobArray => ColType::BlobArray,
            other => other.clone(),
        }
    }

    /// Returns the nullable version of this type.
    pub fn to_nullable(&self) -> ColType {
        match self {
            ColType::Bool => ColType::NullableBool,
            ColType::I32 => ColType::NullableI32,
            ColType::U32 => ColType::NullableU32,
            ColType::I64 => ColType::NullableI64,
            ColType::F64 => ColType::NullableF64,
            ColType::Ref => ColType::NullableRef,
            ColType::String => ColType::NullableString,
            ColType::Bytes => ColType::NullableBytes,
            ColType::Blob => ColType::NullableBlob,
            ColType::BlobArray => ColType::NullableBlobArray,
            other => other.clone(),
        }
    }
}

/// Descriptor for a single column in the row buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColDescriptor {
    /// Column name.
    pub name: String,
    /// Column type determining encoding.
    pub col_type: ColType,
    /// Byte offset within the fixed-size section (for fixed-size columns),
    /// or index into the variable-size section (for variable-size columns).
    pub offset: usize,
}

/// Descriptor for a row's structure.
///
/// A RowDescriptor defines the schema of rows in a buffer format. It contains
/// column definitions with pre-computed offsets for efficient access.
///
/// RowDescriptors are typically created once per table or query output format
/// and shared across many rows via Arc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescriptor {
    /// All columns in this row.
    pub columns: Vec<ColDescriptor>,
    /// Total bytes for the fixed-size section.
    pub fixed_size: usize,
    /// Number of variable-size columns.
    pub variable_count: usize,
}

impl RowDescriptor {
    /// Create a RowDescriptor from an existing TableSchema.
    pub fn from_table_schema(schema: &TableSchema) -> Self {
        let columns = schema.columns.iter().map(|col| {
            let col_type = ColType::from_column_type(&col.ty, col.nullable);
            (col.name.clone(), col_type)
        });
        Self::new(columns)
    }

    /// Create a RowDescriptor with qualified column names (table.column).
    ///
    /// This is used for JOIN operations where predicates use qualified names.
    pub fn from_table_schema_qualified(schema: &TableSchema, table_name: &str) -> Self {
        let columns = schema.columns.iter().map(|col| {
            let col_type = ColType::from_column_type(&col.ty, col.nullable);
            (format!("{}.{}", table_name, col.name), col_type)
        });
        Self::new(columns)
    }

    /// Create a new RowDescriptor from column definitions.
    ///
    /// Columns are reordered: fixed-size columns first, then variable-size.
    /// The `offset` field is computed for each column.
    pub fn new(columns: impl IntoIterator<Item = (String, ColType)>) -> Self {
        let mut fixed_cols: Vec<(String, ColType)> = Vec::new();
        let mut var_cols: Vec<(String, ColType)> = Vec::new();

        for (name, col_type) in columns {
            if col_type.is_fixed_size() {
                fixed_cols.push((name, col_type));
            } else {
                var_cols.push((name, col_type));
            }
        }

        let mut descriptors = Vec::with_capacity(fixed_cols.len() + var_cols.len());
        let mut fixed_offset = 0;

        // Add fixed-size columns with byte offsets
        for (name, col_type) in fixed_cols {
            let size = col_type.fixed_size().unwrap();
            descriptors.push(ColDescriptor {
                name,
                col_type,
                offset: fixed_offset,
            });
            fixed_offset += size;
        }

        let fixed_size = fixed_offset;

        // Add variable-size columns with indices
        for (var_idx, (name, col_type)) in var_cols.into_iter().enumerate() {
            descriptors.push(ColDescriptor {
                name,
                col_type,
                offset: var_idx,
            });
        }

        let variable_count = descriptors.len() - descriptors.iter().filter(|c| c.col_type.is_fixed_size()).count();

        RowDescriptor {
            columns: descriptors,
            fixed_size,
            variable_count,
        }
    }

    /// Create a RowDescriptor preserving column order (no reordering).
    ///
    /// Use this when you need columns in a specific order (e.g., for JOIN output).
    /// The buffer layout still has fixed columns first, but the descriptor
    /// remembers the original order for iteration.
    pub fn new_ordered(columns: impl IntoIterator<Item = (String, ColType)>) -> Self {
        let columns: Vec<_> = columns.into_iter().collect();

        // Compute fixed-size total
        let mut fixed_offset = 0;
        let mut var_idx = 0;

        let mut descriptors = Vec::with_capacity(columns.len());

        // First pass: compute fixed-size offsets
        for (name, col_type) in &columns {
            if col_type.is_fixed_size() {
                let size = col_type.fixed_size().unwrap();
                descriptors.push(ColDescriptor {
                    name: name.clone(),
                    col_type: col_type.clone(),
                    offset: fixed_offset,
                });
                fixed_offset += size;
            }
        }

        let fixed_size = fixed_offset;

        // Second pass: add variable-size columns
        for (name, col_type) in &columns {
            if !col_type.is_fixed_size() {
                descriptors.push(ColDescriptor {
                    name: name.clone(),
                    col_type: col_type.clone(),
                    offset: var_idx,
                });
                var_idx += 1;
            }
        }

        RowDescriptor {
            columns: descriptors,
            fixed_size,
            variable_count: var_idx,
        }
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get column descriptor by name.
    pub fn column(&self, name: &str) -> Option<&ColDescriptor> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Create a projection descriptor with only the specified columns.
    pub fn project(&self, column_names: &[&str]) -> RowDescriptor {
        let cols: Vec<_> = column_names
            .iter()
            .filter_map(|name| {
                self.columns
                    .iter()
                    .find(|c| c.name == *name)
                    .map(|c| (c.name.clone(), c.col_type.clone()))
            })
            .collect();
        RowDescriptor::new_ordered(cols)
    }

    /// Create a combined descriptor for JOIN (self columns + other columns).
    pub fn join(&self, other: &RowDescriptor) -> RowDescriptor {
        let cols: Vec<_> = self
            .columns
            .iter()
            .chain(other.columns.iter())
            .map(|c| (c.name.clone(), c.col_type.clone()))
            .collect();
        RowDescriptor::new_ordered(cols)
    }
}

/// A borrowed view into a row buffer. Zero-copy reads.
#[derive(Debug, Clone, Copy)]
pub struct RowRef<'a> {
    /// Descriptor defining the row structure.
    pub descriptor: &'a RowDescriptor,
    /// Raw buffer containing row data.
    pub buffer: &'a [u8],
}

/// Value read from a row buffer.
///
/// Unlike the old `Value` enum, this is designed for efficient reads from
/// the buffer format. Strings and bytes are borrowed, not owned.
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue<'a> {
    Bool(bool),
    I32(i32),
    U32(u32),
    I64(i64),
    F64(f64),
    Ref(ObjectId),
    String(&'a str),
    Bytes(&'a [u8]),
    Blob(ContentRef),
    BlobArray(Vec<ContentRef>),
    Null,
}

impl<'a> RowValue<'a> {
    /// Convert to the legacy Value type (allocates for strings/bytes).
    pub fn to_value(&self) -> Value {
        match self {
            RowValue::Bool(v) => Value::Bool(*v),
            RowValue::I32(v) => Value::I32(*v),
            RowValue::U32(v) => Value::U32(*v),
            RowValue::I64(v) => Value::I64(*v),
            RowValue::F64(v) => Value::F64(*v),
            RowValue::Ref(v) => Value::Ref(*v),
            RowValue::String(v) => Value::String((*v).to_string()),
            RowValue::Bytes(v) => Value::Bytes((*v).to_vec()),
            RowValue::Blob(v) => Value::Blob(v.clone()),
            RowValue::BlobArray(v) => Value::BlobArray(v.clone()),
            RowValue::Null => Value::NullableNone,
        }
    }

    /// Convert to the legacy Value type, wrapping in NullableSome for non-null values.
    pub fn to_nullable_value(&self) -> Value {
        match self {
            RowValue::Null => Value::NullableNone,
            other => Value::NullableSome(Box::new(other.to_value())),
        }
    }
}

impl<'a> RowRef<'a> {
    /// Create a new RowRef from a descriptor and buffer.
    pub fn new(descriptor: &'a RowDescriptor, buffer: &'a [u8]) -> Self {
        RowRef { descriptor, buffer }
    }

    /// Get the value at the given column index.
    pub fn get(&self, col_idx: usize) -> Option<RowValue<'a>> {
        let col = self.descriptor.columns.get(col_idx)?;
        self.get_column(col)
    }

    /// Get the value for a column by name.
    pub fn get_by_name(&self, name: &str) -> Option<RowValue<'a>> {
        let col = self.descriptor.column(name)?;
        self.get_column(col)
    }

    /// Get value from a column descriptor.
    fn get_column(&self, col: &ColDescriptor) -> Option<RowValue<'a>> {
        if col.col_type.is_fixed_size() {
            self.get_fixed(col)
        } else {
            self.get_variable(col)
        }
    }

    /// Get a fixed-size column value.
    fn get_fixed(&self, col: &ColDescriptor) -> Option<RowValue<'a>> {
        let offset = col.offset;
        let data = &self.buffer[offset..];

        match &col.col_type {
            ColType::Bool => Some(RowValue::Bool(data.first()? != &0)),
            ColType::I32 => {
                let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                Some(RowValue::I32(i32::from_le_bytes(bytes)))
            }
            ColType::U32 => {
                let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                Some(RowValue::U32(u32::from_le_bytes(bytes)))
            }
            ColType::I64 => {
                let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                Some(RowValue::I64(i64::from_le_bytes(bytes)))
            }
            ColType::F64 => {
                let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                Some(RowValue::F64(f64::from_le_bytes(bytes)))
            }
            ColType::Ref => {
                let bytes: [u8; 16] = data.get(..16)?.try_into().ok()?;
                Some(RowValue::Ref(ObjectId::from_le_bytes(bytes)))
            }
            // Nullable fixed-size
            ColType::NullableBool => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    Some(RowValue::Bool(data.get(1)? != &0))
                }
            }
            ColType::NullableI32 => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    let bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
                    Some(RowValue::I32(i32::from_le_bytes(bytes)))
                }
            }
            ColType::NullableU32 => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    let bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
                    Some(RowValue::U32(u32::from_le_bytes(bytes)))
                }
            }
            ColType::NullableI64 => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    let bytes: [u8; 8] = data.get(1..9)?.try_into().ok()?;
                    Some(RowValue::I64(i64::from_le_bytes(bytes)))
                }
            }
            ColType::NullableF64 => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    let bytes: [u8; 8] = data.get(1..9)?.try_into().ok()?;
                    Some(RowValue::F64(f64::from_le_bytes(bytes)))
                }
            }
            ColType::NullableRef => {
                if data.first()? == &0 {
                    Some(RowValue::Null)
                } else {
                    let bytes: [u8; 16] = data.get(1..17)?.try_into().ok()?;
                    Some(RowValue::Ref(ObjectId::from_le_bytes(bytes)))
                }
            }
            _ => None, // Not a fixed-size type
        }
    }

    /// Get a variable-size column value.
    fn get_variable(&self, col: &ColDescriptor) -> Option<RowValue<'a>> {
        let var_idx = col.offset;

        // Parse varint header to find the offset and length
        let (offset, len) = self.find_variable_column(var_idx)?;
        let data = self.buffer.get(offset..offset + len)?;

        // Handle nullable types
        let (_is_null, value_data) = if col.col_type.is_nullable() {
            if data.is_empty() || data[0] == 0 {
                return Some(RowValue::Null);
            }
            (false, &data[1..])
        } else {
            (false, data)
        };

        match &col.col_type {
            ColType::String | ColType::NullableString => {
                let s = std::str::from_utf8(value_data).ok()?;
                Some(RowValue::String(s))
            }
            ColType::Bytes | ColType::NullableBytes => Some(RowValue::Bytes(value_data)),
            ColType::Blob | ColType::NullableBlob => {
                let (content_ref, _) = ContentRef::from_row_bytes(value_data).ok()?;
                Some(RowValue::Blob(content_ref))
            }
            ColType::BlobArray | ColType::NullableBlobArray => {
                let mut pos = 0;
                let (count, consumed) = decode_varint(&value_data[pos..])?;
                pos += consumed;

                let mut refs = Vec::with_capacity(count);
                for _ in 0..count {
                    let (content_ref, consumed) =
                        ContentRef::from_row_bytes(&value_data[pos..]).ok()?;
                    refs.push(content_ref);
                    pos += consumed;
                }
                Some(RowValue::BlobArray(refs))
            }
            _ => None, // Not a variable-size type
        }
    }

    /// Find the offset and length of a variable-size column.
    fn find_variable_column(&self, var_idx: usize) -> Option<(usize, usize)> {
        // Variable column data starts after fixed-size section
        // First we read the varint headers for all variable columns
        let mut pos = 0;
        let header_data = &self.buffer[self.descriptor.fixed_size..];

        let mut lengths = Vec::with_capacity(self.descriptor.variable_count);
        for _ in 0..self.descriptor.variable_count {
            let (len, consumed) = decode_varint(&header_data[pos..])?;
            lengths.push(len);
            pos += consumed;
        }

        // Now calculate the offset for the requested column
        let data_start = self.descriptor.fixed_size + pos;
        let mut offset = data_start;
        for i in 0..var_idx {
            offset += lengths.get(i)?;
        }

        Some((offset, *lengths.get(var_idx)?))
    }
}

/// An owned row with its own buffer. For caching and WASM transfer.
#[derive(Debug, Clone)]
pub struct OwnedRow {
    /// Descriptor defining the row structure.
    pub descriptor: Arc<RowDescriptor>,
    /// Owned buffer containing row data.
    pub buffer: Vec<u8>,
}

impl OwnedRow {
    /// Create a new OwnedRow.
    pub fn new(descriptor: Arc<RowDescriptor>, buffer: Vec<u8>) -> Self {
        OwnedRow { descriptor, buffer }
    }

    /// Get a borrowed view of this row.
    pub fn as_ref(&self) -> RowRef<'_> {
        RowRef {
            descriptor: &self.descriptor,
            buffer: &self.buffer,
        }
    }

    /// Get the value at the given column index.
    pub fn get(&self, col_idx: usize) -> Option<RowValue<'_>> {
        self.as_ref().get(col_idx)
    }

    /// Get the value for a column by name.
    pub fn get_by_name(&self, name: &str) -> Option<RowValue<'_>> {
        self.as_ref().get_by_name(name)
    }

    /// Convert to the legacy Row type (allocates).
    ///
    /// This is provided for compatibility during migration. The `id` must be
    /// provided since it's stored out-of-band in the new format.
    /// Values are returned in descriptor column order.
    pub fn to_legacy_row(&self, id: ObjectId) -> Row {
        let values: Vec<Value> = self
            .descriptor
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let value = self.get(idx).unwrap_or(RowValue::Null);
                if col.col_type.is_nullable() {
                    value.to_nullable_value()
                } else {
                    value.to_value()
                }
            })
            .collect();
        Row::new(id, values)
    }

    /// Convert to the legacy Row type with values in schema order.
    ///
    /// This is provided for compatibility during migration. Values are
    /// returned in the order specified by the schema, not descriptor order.
    pub fn to_legacy_row_with_schema(&self, id: ObjectId, schema: &TableSchema) -> Row {
        let values: Vec<Value> = schema
            .columns
            .iter()
            .map(|col_def| {
                // Find the value by column name
                let value = self.get_by_name(&col_def.name).unwrap_or(RowValue::Null);
                if col_def.nullable {
                    value.to_nullable_value()
                } else {
                    value.to_value()
                }
            })
            .collect();
        Row::new(id, values)
    }

    /// Create an OwnedRow from a legacy Row.
    ///
    /// This is provided for compatibility during migration. The schema is
    /// needed to map value indices to column names.
    pub fn from_legacy_row(row: &Row, schema: &TableSchema, descriptor: Arc<RowDescriptor>) -> Self {
        Self::from_legacy_row_qualified(row, schema, descriptor, None)
    }

    /// Create an OwnedRow from a legacy Row with optional table name qualification.
    ///
    /// When `table_name` is provided, column names are qualified as `table.column`
    /// when looking up the descriptor index. This is needed for JOIN operations
    /// where descriptors use qualified names.
    pub fn from_legacy_row_qualified(
        row: &Row,
        schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
        table_name: Option<&str>,
    ) -> Self {
        let mut builder = RowBuilder::new(descriptor.clone());

        // Legacy rows have values in schema column order
        for (schema_idx, value) in row.values.iter().enumerate() {
            // Find the column name from the schema
            if let Some(col_def) = schema.columns.get(schema_idx) {
                // Build the lookup name (optionally qualified)
                let lookup_name = if let Some(table) = table_name {
                    format!("{}.{}", table, col_def.name)
                } else {
                    col_def.name.clone()
                };
                // Find the corresponding descriptor column index
                if let Some(desc_idx) = descriptor.column_index(&lookup_name) {
                    builder = Self::set_from_value(builder, desc_idx, value);
                }
            }
        }

        builder.build()
    }

    /// Create an OwnedRow for a single table from a combined legacy Row.
    ///
    /// Used for chain joins where the input row contains data from multiple tables.
    /// The combined_schema has qualified column names like "table.column".
    /// We extract only the columns belonging to `table_name`.
    pub fn from_legacy_row_combined(
        row: &Row,
        combined_schema: &TableSchema,
        descriptor: Arc<RowDescriptor>,
        table_name: &str,
    ) -> Self {
        let mut builder = RowBuilder::new(descriptor.clone());
        let prefix = format!("{}.", table_name);

        // Iterate through combined schema and extract values for this table
        for (schema_idx, col_def) in combined_schema.columns.iter().enumerate() {
            if col_def.name.starts_with(&prefix) {
                // This column belongs to our table
                if let Some(value) = row.values.get(schema_idx) {
                    // Find the descriptor column by qualified name
                    if let Some(desc_idx) = descriptor.column_index(&col_def.name) {
                        builder = Self::set_from_value(builder, desc_idx, value);
                    }
                }
            }
        }

        builder.build()
    }

    /// Helper to set a builder value from a legacy Value.
    fn set_from_value(builder: RowBuilder, idx: usize, value: &Value) -> RowBuilder {
        match value {
            Value::Bool(v) => builder.set_bool(idx, *v),
            Value::I32(v) => builder.set_i32(idx, *v),
            Value::U32(v) => builder.set_u32(idx, *v),
            Value::I64(v) => builder.set_i64(idx, *v),
            Value::F64(v) => builder.set_f64(idx, *v),
            Value::String(v) => builder.set_string(idx, v),
            Value::Bytes(v) => builder.set_bytes(idx, v),
            Value::Ref(v) => builder.set_ref(idx, *v),
            Value::NullableNone => builder.set_null(idx),
            Value::NullableSome(inner) => Self::set_from_value(builder, idx, inner),
            // TODO: Handle Row, Array, Blob, BlobArray
            _ => builder,
        }
    }

    /// Create a new OwnedRow with qualified column names.
    ///
    /// Converts column names from `column` to `table.column` format.
    /// This is needed for JOIN queries where predicates use qualified names.
    pub fn qualify_columns(&self, table: &str, schema: &TableSchema) -> Self {
        // Create a new descriptor with qualified column names
        let qualified_descriptor = Arc::new(RowDescriptor::from_table_schema_qualified(schema, table));

        // Build the new row with qualified column names
        let mut builder = RowBuilder::new(qualified_descriptor.clone());

        // Copy values from current row to qualified row
        for col_def in schema.columns.iter() {
            let unqualified_name = &col_def.name;
            let qualified_name = format!("{}.{}", table, unqualified_name);

            // Try to get value by unqualified name from current row
            if let Some(value) = self.get_by_name(unqualified_name) {
                // Find the index in the qualified descriptor
                if let Some(qualified_idx) = qualified_descriptor.column_index(&qualified_name) {
                    builder = Self::set_from_row_value(builder, qualified_idx, value);
                }
            }
        }

        builder.build()
    }

    /// Helper to set a builder value from a RowValue.
    fn set_from_row_value(builder: RowBuilder, idx: usize, value: RowValue<'_>) -> RowBuilder {
        match value {
            RowValue::Bool(v) => builder.set_bool(idx, v),
            RowValue::I32(v) => builder.set_i32(idx, v),
            RowValue::U32(v) => builder.set_u32(idx, v),
            RowValue::I64(v) => builder.set_i64(idx, v),
            RowValue::F64(v) => builder.set_f64(idx, v),
            RowValue::String(v) => builder.set_string(idx, v),
            RowValue::Bytes(v) => builder.set_bytes(idx, v),
            RowValue::Ref(v) => builder.set_ref(idx, v),
            RowValue::Null => builder.set_null(idx),
            // TODO: Handle Blob, BlobArray
            _ => builder,
        }
    }
}

/// Builder for constructing row buffers.
pub struct RowBuilder {
    descriptor: Arc<RowDescriptor>,
    fixed_section: Vec<u8>,
    variable_sections: Vec<Vec<u8>>,
}

impl RowBuilder {
    /// Create a new builder for the given descriptor.
    pub fn new(descriptor: Arc<RowDescriptor>) -> Self {
        let fixed_size = descriptor.fixed_size;
        let var_count = descriptor.variable_count;

        RowBuilder {
            descriptor,
            fixed_section: vec![0u8; fixed_size],
            variable_sections: vec![Vec::new(); var_count],
        }
    }

    /// Set a boolean column value.
    pub fn set_bool(mut self, col_idx: usize, value: bool) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::Bool => {
                        self.fixed_section[offset] = if value { 1 } else { 0 };
                    }
                    ColType::NullableBool => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1] = if value { 1 } else { 0 };
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set an i32 column value.
    pub fn set_i32(mut self, col_idx: usize, value: i32) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::I32 => {
                        self.fixed_section[offset..offset + 4]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    ColType::NullableI32 => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1..offset + 5]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set a u32 column value.
    pub fn set_u32(mut self, col_idx: usize, value: u32) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::U32 => {
                        self.fixed_section[offset..offset + 4]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    ColType::NullableU32 => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1..offset + 5]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set an i64 column value.
    pub fn set_i64(mut self, col_idx: usize, value: i64) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::I64 => {
                        self.fixed_section[offset..offset + 8]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    ColType::NullableI64 => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1..offset + 9]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set an f64 column value.
    pub fn set_f64(mut self, col_idx: usize, value: f64) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::F64 => {
                        self.fixed_section[offset..offset + 8]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    ColType::NullableF64 => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1..offset + 9]
                            .copy_from_slice(&value.to_le_bytes());
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set a Ref (ObjectId) column value.
    pub fn set_ref(mut self, col_idx: usize, value: ObjectId) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_fixed_size() {
                let offset = col.offset;
                match &col.col_type {
                    ColType::Ref => {
                        self.fixed_section[offset..offset + 16]
                            .copy_from_slice(&value.0.to_le_bytes());
                    }
                    ColType::NullableRef => {
                        self.fixed_section[offset] = 1; // present
                        self.fixed_section[offset + 1..offset + 17]
                            .copy_from_slice(&value.0.to_le_bytes());
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set a string column value.
    pub fn set_string(mut self, col_idx: usize, value: &str) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if !col.col_type.is_fixed_size() {
                let var_idx = col.offset;
                match &col.col_type {
                    ColType::String => {
                        self.variable_sections[var_idx] = value.as_bytes().to_vec();
                    }
                    ColType::NullableString => {
                        let mut data = vec![1u8]; // present
                        data.extend_from_slice(value.as_bytes());
                        self.variable_sections[var_idx] = data;
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set a bytes column value.
    pub fn set_bytes(mut self, col_idx: usize, value: &[u8]) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if !col.col_type.is_fixed_size() {
                let var_idx = col.offset;
                match &col.col_type {
                    ColType::Bytes => {
                        self.variable_sections[var_idx] = value.to_vec();
                    }
                    ColType::NullableBytes => {
                        let mut data = vec![1u8]; // present
                        data.extend_from_slice(value);
                        self.variable_sections[var_idx] = data;
                    }
                    _ => {}
                }
            }
        }
        self
    }

    /// Set a nullable column to null.
    pub fn set_null(mut self, col_idx: usize) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx) {
            if col.col_type.is_nullable() {
                if col.col_type.is_fixed_size() {
                    let offset = col.offset;
                    self.fixed_section[offset] = 0; // null flag
                } else {
                    let var_idx = col.offset;
                    self.variable_sections[var_idx] = vec![0u8]; // null flag
                }
            }
        }
        self
    }

    // --- By-name variants for ergonomic usage ---

    /// Set a boolean column by name.
    pub fn set_bool_by_name(self, name: &str, value: bool) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_bool(idx, value)
        } else {
            self
        }
    }

    /// Set an i32 column by name.
    pub fn set_i32_by_name(self, name: &str, value: i32) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_i32(idx, value)
        } else {
            self
        }
    }

    /// Set a u32 column by name.
    pub fn set_u32_by_name(self, name: &str, value: u32) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_u32(idx, value)
        } else {
            self
        }
    }

    /// Set an i64 column by name.
    pub fn set_i64_by_name(self, name: &str, value: i64) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_i64(idx, value)
        } else {
            self
        }
    }

    /// Set an f64 column by name.
    pub fn set_f64_by_name(self, name: &str, value: f64) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_f64(idx, value)
        } else {
            self
        }
    }

    /// Set a Ref (ObjectId) column by name.
    pub fn set_ref_by_name(self, name: &str, value: ObjectId) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_ref(idx, value)
        } else {
            self
        }
    }

    /// Set a string column by name.
    pub fn set_string_by_name(self, name: &str, value: &str) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_string(idx, value)
        } else {
            self
        }
    }

    /// Set a bytes column by name.
    pub fn set_bytes_by_name(self, name: &str, value: &[u8]) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_bytes(idx, value)
        } else {
            self
        }
    }

    /// Set a nullable column to null by name.
    pub fn set_null_by_name(self, name: &str) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_null(idx)
        } else {
            self
        }
    }

    /// Build the final row buffer.
    pub fn build(self) -> OwnedRow {
        let mut buffer = self.fixed_section;

        // Add varint headers for variable columns
        for section in &self.variable_sections {
            encode_varint(section.len(), &mut buffer);
        }

        // Add variable column data
        for section in self.variable_sections {
            buffer.extend(section);
        }

        OwnedRow {
            descriptor: self.descriptor,
            buffer,
        }
    }
}

/// Project a row to a subset of columns.
///
/// This creates a new buffer containing only the specified columns.
pub fn project_row(
    row: RowRef<'_>,
    source_cols: &[usize],
    target_descriptor: Arc<RowDescriptor>,
) -> OwnedRow {
    let mut builder = RowBuilder::new(target_descriptor);

    for (target_idx, &source_idx) in source_cols.iter().enumerate() {
        if let Some(value) = row.get(source_idx) {
            builder = match value {
                RowValue::Bool(v) => builder.set_bool(target_idx, v),
                RowValue::I32(v) => builder.set_i32(target_idx, v),
                RowValue::U32(v) => builder.set_u32(target_idx, v),
                RowValue::I64(v) => builder.set_i64(target_idx, v),
                RowValue::F64(v) => builder.set_f64(target_idx, v),
                RowValue::Ref(v) => builder.set_ref(target_idx, v),
                RowValue::String(v) => builder.set_string(target_idx, v),
                RowValue::Bytes(v) => builder.set_bytes(target_idx, v),
                RowValue::Null => builder.set_null(target_idx),
                // TODO: Handle Blob and BlobArray
                _ => builder,
            };
        }
    }

    builder.build()
}

/// Join two rows by concatenating their buffers.
///
/// This is a more efficient operation than project_row for JOINs,
/// as it can memcpy entire sections.
pub fn join_rows(
    left: RowRef<'_>,
    right: RowRef<'_>,
    target_descriptor: Arc<RowDescriptor>,
) -> OwnedRow {
    // For now, use the simple approach via builder
    // TODO: Optimize with direct memcpy of fixed sections
    let left_col_count = left.descriptor.columns.len();
    let right_col_count = right.descriptor.columns.len();

    let source_cols: Vec<usize> = (0..left_col_count)
        .chain((0..right_col_count).map(|i| left_col_count + i))
        .collect();

    // Create a temporary combined row view
    // For now, just build using the builder
    let mut builder = RowBuilder::new(target_descriptor);

    for (target_idx, source_idx) in source_cols.iter().enumerate() {
        let value = if *source_idx < left_col_count {
            left.get(*source_idx)
        } else {
            right.get(*source_idx - left_col_count)
        };

        if let Some(value) = value {
            builder = match value {
                RowValue::Bool(v) => builder.set_bool(target_idx, v),
                RowValue::I32(v) => builder.set_i32(target_idx, v),
                RowValue::U32(v) => builder.set_u32(target_idx, v),
                RowValue::I64(v) => builder.set_i64(target_idx, v),
                RowValue::F64(v) => builder.set_f64(target_idx, v),
                RowValue::Ref(v) => builder.set_ref(target_idx, v),
                RowValue::String(v) => builder.set_string(target_idx, v),
                RowValue::Bytes(v) => builder.set_bytes(target_idx, v),
                RowValue::Null => builder.set_null(target_idx),
                _ => builder,
            };
        }
    }

    builder.build()
}

// Varint encoding/decoding helpers

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

fn decode_varint(data: &[u8]) -> Option<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_descriptor_new() {
        let desc = RowDescriptor::new([
            ("name".to_string(), ColType::String),
            ("age".to_string(), ColType::I32),
            ("active".to_string(), ColType::Bool),
        ]);

        // Fixed columns should come first
        assert_eq!(desc.columns.len(), 3);
        assert_eq!(desc.fixed_size, 5); // i32 (4) + bool (1)
        assert_eq!(desc.variable_count, 1);

        // Check that fixed columns have correct offsets
        let age_col = desc.column("age").unwrap();
        assert!(age_col.col_type.is_fixed_size());

        let active_col = desc.column("active").unwrap();
        assert!(active_col.col_type.is_fixed_size());

        let name_col = desc.column("name").unwrap();
        assert!(!name_col.col_type.is_fixed_size());
    }

    #[test]
    fn test_row_builder_and_reader() {
        let desc = Arc::new(RowDescriptor::new([
            ("name".to_string(), ColType::String),
            ("age".to_string(), ColType::I32),
            ("score".to_string(), ColType::F64),
        ]));

        // Find column indices
        let name_idx = desc.column_index("name").unwrap();
        let age_idx = desc.column_index("age").unwrap();
        let score_idx = desc.column_index("score").unwrap();

        let row = RowBuilder::new(desc.clone())
            .set_string(name_idx, "Alice")
            .set_i32(age_idx, 30)
            .set_f64(score_idx, 95.5)
            .build();

        // Read back values
        assert_eq!(row.get_by_name("name"), Some(RowValue::String("Alice")));
        assert_eq!(row.get_by_name("age"), Some(RowValue::I32(30)));
        assert_eq!(row.get_by_name("score"), Some(RowValue::F64(95.5)));
    }

    #[test]
    fn test_nullable_columns() {
        let desc = Arc::new(RowDescriptor::new([
            ("name".to_string(), ColType::NullableString),
            ("age".to_string(), ColType::NullableI32),
        ]));

        let name_idx = desc.column_index("name").unwrap();
        let age_idx = desc.column_index("age").unwrap();

        // Test with values
        let row = RowBuilder::new(desc.clone())
            .set_string(name_idx, "Bob")
            .set_i32(age_idx, 25)
            .build();

        assert_eq!(row.get_by_name("name"), Some(RowValue::String("Bob")));
        assert_eq!(row.get_by_name("age"), Some(RowValue::I32(25)));

        // Test with nulls
        let row_null = RowBuilder::new(desc.clone())
            .set_null(name_idx)
            .set_null(age_idx)
            .build();

        assert_eq!(row_null.get_by_name("name"), Some(RowValue::Null));
        assert_eq!(row_null.get_by_name("age"), Some(RowValue::Null));
    }

    #[test]
    fn test_projection() {
        let source_desc = Arc::new(RowDescriptor::new([
            ("a".to_string(), ColType::I32),
            ("b".to_string(), ColType::String),
            ("c".to_string(), ColType::I64),
        ]));

        let a_idx = source_desc.column_index("a").unwrap();
        let b_idx = source_desc.column_index("b").unwrap();
        let c_idx = source_desc.column_index("c").unwrap();

        let row = RowBuilder::new(source_desc.clone())
            .set_i32(a_idx, 1)
            .set_string(b_idx, "hello")
            .set_i64(c_idx, 100)
            .build();

        // Project to just columns a and c
        let target_desc = Arc::new(RowDescriptor::new([
            ("a".to_string(), ColType::I32),
            ("c".to_string(), ColType::I64),
        ]));

        let projected = project_row(row.as_ref(), &[a_idx, c_idx], target_desc);

        assert_eq!(projected.get_by_name("a"), Some(RowValue::I32(1)));
        assert_eq!(projected.get_by_name("c"), Some(RowValue::I64(100)));
    }

    #[test]
    fn test_join_descriptor() {
        let left_desc = RowDescriptor::new([
            ("a".to_string(), ColType::I32),
            ("b".to_string(), ColType::String),
        ]);

        let right_desc = RowDescriptor::new([
            ("c".to_string(), ColType::I64),
            ("d".to_string(), ColType::Bool),
        ]);

        let joined = left_desc.join(&right_desc);

        assert_eq!(joined.columns.len(), 4);
        assert!(joined.column("a").is_some());
        assert!(joined.column("b").is_some());
        assert!(joined.column("c").is_some());
        assert!(joined.column("d").is_some());
    }

    #[test]
    fn test_varint_roundtrip() {
        let test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 1_000_000];

        for &value in &test_values {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let (decoded, _) = decode_varint(&buf).unwrap();
            assert_eq!(value, decoded, "varint roundtrip failed for {}", value);
        }
    }

    #[test]
    fn test_legacy_conversion_roundtrip() {
        use super::super::schema::{ColumnDef, ColumnType, TableSchema};

        // Create a table schema
        let schema = TableSchema::new(
            "test",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("age", ColumnType::I32),
                ColumnDef::optional("score", ColumnType::F64),
            ],
        );

        // Create a RowDescriptor from the schema
        let desc = Arc::new(RowDescriptor::from_table_schema(&schema));

        // Create a legacy Row
        let id = ObjectId::new(12345);
        let legacy_row = Row::new(
            id,
            vec![
                Value::String("Alice".to_string()),
                Value::I32(30),
                Value::NullableSome(Box::new(Value::F64(95.5))),
            ],
        );

        // Convert to OwnedRow
        let owned_row = OwnedRow::from_legacy_row(&legacy_row, &schema, desc.clone());

        // Verify values
        assert_eq!(owned_row.get_by_name("name"), Some(RowValue::String("Alice")));
        assert_eq!(owned_row.get_by_name("age"), Some(RowValue::I32(30)));
        assert_eq!(owned_row.get_by_name("score"), Some(RowValue::F64(95.5)));

        // Convert back to legacy Row (in schema order)
        let converted_back = owned_row.to_legacy_row_with_schema(id, &schema);
        assert_eq!(converted_back.id, legacy_row.id);
        assert_eq!(converted_back.values.len(), legacy_row.values.len());

        // Check individual values
        assert_eq!(converted_back.values[0].as_str(), Some("Alice"));
        assert_eq!(converted_back.values[1].as_i32(), Some(30));
        // The score comes back as NullableSome(F64(95.5))
        match &converted_back.values[2] {
            Value::NullableSome(inner) => assert_eq!(inner.as_f64(), Some(95.5)),
            _ => panic!("Expected NullableSome for nullable column"),
        }
    }

    #[test]
    fn test_from_table_schema() {
        use super::super::schema::{ColumnDef, ColumnType, TableSchema};

        let schema = TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("age", ColumnType::I32),
                ColumnDef::optional("email", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        );

        let desc = RowDescriptor::from_table_schema(&schema);

        // Should have 4 columns
        assert_eq!(desc.columns.len(), 4);

        // Check that nullable columns have nullable types
        let name_col = desc.column("name").unwrap();
        assert!(!name_col.col_type.is_nullable());
        assert_eq!(name_col.col_type, ColType::String);

        let email_col = desc.column("email").unwrap();
        assert!(email_col.col_type.is_nullable());
        assert_eq!(email_col.col_type, ColType::NullableString);

        // Fixed-size columns should have computed offsets
        let age_col = desc.column("age").unwrap();
        assert_eq!(age_col.col_type, ColType::I32);
        assert!(age_col.col_type.is_fixed_size());

        let active_col = desc.column("active").unwrap();
        assert_eq!(active_col.col_type, ColType::Bool);
        assert!(active_col.col_type.is_fixed_size());

        // Fixed size should be: i32 (4) + bool (1) = 5
        assert_eq!(desc.fixed_size, 5);

        // Variable count should be: name + email = 2
        assert_eq!(desc.variable_count, 2);
    }
}
