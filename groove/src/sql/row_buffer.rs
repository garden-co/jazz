//! Unified row representation with zero-copy buffer format.
//!
//! This module provides a standardized row representation that uses a single
//! buffer format across storage, memory, and WASM boundaries. The design
//! prioritizes zero-copy reads and efficient operations like projection and JOIN.
//!
//! # Buffer Layout
//!
//! ```text
//! [fixed-size columns][u32 offset₂][u32 offset₃]...[var_data₁][var_data₂][var_data₃]...
//! ```
//!
//! Fixed-size columns come first, followed by an offset table for variable-size
//! columns, then the variable column data. For N variable columns, N-1 offsets
//! are stored (the first variable column starts immediately after the offset table,
//! the last ends at buffer end). All columns are O(1) accessible.
//!
//! # Nullable Columns
//!
//! Nullable columns have a 1-byte presence flag (0x00 = null, 0x01 = present).
//! For fixed-size types, the flag is followed by the value bytes (or zeros if null).
//! For variable-size types, if null the length is 0.

use std::sync::Arc;

use crate::object::ObjectId;
use crate::storage::ContentRef;

use super::query_graph::PredicateValue;
use super::schema::{ColumnType, TableSchema};

/// Descriptor for a single column in the row buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColDescriptor {
    /// Column name.
    pub name: String,
    /// Column type (base type from schema, includes Array descriptor if applicable).
    pub ty: ColumnType,
    /// Whether this column is nullable.
    pub nullable: bool,
    /// Byte offset within the fixed-size section (for fixed-size columns),
    /// or index into the variable-size section (for variable-size columns).
    pub offset: usize,
    /// Original index in the schema (before reordering for buffer layout).
    pub schema_index: usize,
}

impl ColDescriptor {
    /// Returns true if this column is fixed-size in the buffer.
    pub fn is_fixed_size(&self) -> bool {
        self.ty.is_fixed_size()
    }

    /// Returns the fixed size in bytes, accounting for nullability.
    /// Nullable fixed-size types have an extra presence byte.
    pub fn fixed_size(&self) -> Option<usize> {
        self.ty.fixed_size_nullable(self.nullable)
    }

    /// Get the item descriptor for Array types.
    pub fn item_descriptor(&self) -> Option<&Arc<RowDescriptor>> {
        match &self.ty {
            ColumnType::Array(desc) => Some(desc),
            _ => None,
        }
    }
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
        let columns = schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), col.ty.clone(), col.nullable));
        Self::new(columns)
    }

    /// Create a RowDescriptor with qualified column names (table.column).
    ///
    /// This is used for JOIN operations where predicates use qualified names.
    pub fn from_table_schema_qualified(schema: &TableSchema, table_name: &str) -> Self {
        let columns = schema.columns.iter().map(|col| {
            (
                format!("{}.{}", table_name, col.name),
                col.ty.clone(),
                col.nullable,
            )
        });
        Self::new(columns)
    }

    /// Create a new RowDescriptor from column definitions.
    ///
    /// Columns are reordered: fixed-size columns first, then variable-size.
    /// The `offset` field is computed for each column.
    pub fn new(columns: impl IntoIterator<Item = (String, ColumnType, bool)>) -> Self {
        let mut fixed_cols: Vec<(usize, String, ColumnType, bool)> = Vec::new();
        let mut var_cols: Vec<(usize, String, ColumnType, bool)> = Vec::new();

        for (schema_idx, (name, ty, nullable)) in columns.into_iter().enumerate() {
            if ty.is_fixed_size() {
                fixed_cols.push((schema_idx, name, ty, nullable));
            } else {
                var_cols.push((schema_idx, name, ty, nullable));
            }
        }

        let mut descriptors = Vec::with_capacity(fixed_cols.len() + var_cols.len());
        let mut fixed_offset = 0;

        // Add fixed-size columns with byte offsets
        for (schema_idx, name, ty, nullable) in fixed_cols {
            let size = ty.fixed_size_nullable(nullable).unwrap();
            descriptors.push(ColDescriptor {
                name,
                ty,
                nullable,
                offset: fixed_offset,
                schema_index: schema_idx,
            });
            fixed_offset += size;
        }

        let fixed_size = fixed_offset;

        // Add variable-size columns with indices
        for (var_idx, (schema_idx, name, ty, nullable)) in var_cols.into_iter().enumerate() {
            descriptors.push(ColDescriptor {
                name,
                ty,
                nullable,
                offset: var_idx,
                schema_index: schema_idx,
            });
        }

        let variable_count =
            descriptors.len() - descriptors.iter().filter(|c| c.is_fixed_size()).count();

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
    pub fn new_ordered(columns: impl IntoIterator<Item = (String, ColumnType, bool)>) -> Self {
        let columns: Vec<_> = columns.into_iter().enumerate().collect();

        // Compute fixed-size total
        let mut fixed_offset = 0;
        let mut var_idx = 0;

        let mut descriptors = Vec::with_capacity(columns.len());

        // First pass: compute fixed-size offsets
        for (schema_idx, (name, ty, nullable)) in &columns {
            if ty.is_fixed_size() {
                let size = ty.fixed_size_nullable(*nullable).unwrap();
                descriptors.push(ColDescriptor {
                    name: name.clone(),
                    ty: ty.clone(),
                    nullable: *nullable,
                    offset: fixed_offset,
                    schema_index: *schema_idx,
                });
                fixed_offset += size;
            }
        }

        let fixed_size = fixed_offset;

        // Second pass: add variable-size columns
        for (schema_idx, (name, ty, nullable)) in &columns {
            if !ty.is_fixed_size() {
                descriptors.push(ColDescriptor {
                    name: name.clone(),
                    ty: ty.clone(),
                    nullable: *nullable,
                    offset: var_idx,
                    schema_index: *schema_idx,
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
    ///
    /// Supports both qualified (table.column) and unqualified (column) names.
    /// First tries exact match, then:
    /// - If searching for "column", tries to find "*.column" (any table prefix)
    /// - If searching for "table.column", tries to find "column" (unqualified)
    pub fn column_index(&self, name: &str) -> Option<usize> {
        // First try exact match
        if let Some(idx) = self.columns.iter().position(|c| c.name == name) {
            return Some(idx);
        }

        // If the search name is unqualified, try to find a qualified match
        if !name.contains('.') {
            // Search for any column ending with ".{name}"
            let suffix = format!(".{}", name);
            if let Some(idx) = self.columns.iter().position(|c| c.name.ends_with(&suffix)) {
                return Some(idx);
            }
        } else {
            // If the search name is qualified, try to find an unqualified match
            if let Some(col_name) = name.split('.').next_back()
                && let Some(idx) = self.columns.iter().position(|c| c.name == col_name)
            {
                return Some(idx);
            }
        }

        None
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
                    .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
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
            .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
            .collect();
        RowDescriptor::new_ordered(cols)
    }

    /// Create a combined descriptor for multiple rows (for multi-table JOIN).
    /// Uses new_ordered which groups fixed-size columns first.
    pub fn join_all(descriptors: &[&RowDescriptor]) -> RowDescriptor {
        let cols: Vec<_> = descriptors
            .iter()
            .flat_map(|d| d.columns.iter())
            .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
            .collect();
        RowDescriptor::new_ordered(cols)
    }

    /// Create a combined descriptor preserving exact buffer order from sources.
    ///
    /// This is used when concatenating row buffers - the order must match exactly.
    /// Each source row's columns appear in their buffer order (fixed-size section
    /// first, then variable-size section).
    ///
    /// The schema_index is computed to reflect logical column order (the order
    /// columns would appear in schema order across all tables). This allows
    /// `get_column(schema_idx)` to return columns in the expected logical order.
    pub fn concat_preserving_buffer_order(descriptors: &[&RowDescriptor]) -> RowDescriptor {
        // First, compute the logical schema order for each source descriptor.
        // For each descriptor, we need to know the schema indices in order.
        let mut logical_order: Vec<(usize, usize, &ColDescriptor)> = Vec::new(); // (desc_idx, new_logical_idx, col)

        let mut logical_idx = 0;
        for (desc_idx, desc) in descriptors.iter().enumerate() {
            // Get columns sorted by their original schema_index
            let mut cols_by_schema: Vec<_> = desc.columns.iter().enumerate().collect();
            cols_by_schema.sort_by_key(|(_, c)| c.schema_index);

            for (_, col) in cols_by_schema {
                logical_order.push((desc_idx, logical_idx, col));
                logical_idx += 1;
            }
        }

        // Now build the merged descriptor in buffer order but with correct schema_indices
        let mut columns = Vec::new();
        let mut fixed_offset = 0;
        let mut var_idx = 0;

        for (desc_idx, desc) in descriptors.iter().enumerate() {
            for col in &desc.columns {
                let mut new_col = col.clone();

                // Find this column's logical index by matching on original schema_index
                // (schema_index is unique within a descriptor)
                let logical_schema_idx = logical_order
                    .iter()
                    .find(|(d, _, c)| *d == desc_idx && c.schema_index == col.schema_index)
                    .map(|(_, idx, _)| *idx)
                    .unwrap_or(columns.len());

                new_col.schema_index = logical_schema_idx;

                if col.is_fixed_size() {
                    new_col.offset = fixed_offset;
                    fixed_offset += col.fixed_size().unwrap();
                } else {
                    new_col.offset = var_idx;
                    var_idx += 1;
                }

                columns.push(new_col);
            }
        }

        RowDescriptor {
            columns,
            fixed_size: fixed_offset,
            variable_count: var_idx,
        }
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
    /// Array of rows. Items can be iterated without allocation.
    Array(ArrayValue<'a>),
    Null,
}

/// An array value that provides zero-copy iteration over items.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArrayValue<'a> {
    /// Descriptor for each item in the array.
    pub item_descriptor: &'a RowDescriptor,
    /// Raw buffer containing the array data.
    /// Format: `[u32 count][u32 offset₂][u32 offset₃]...[item₁][item₂]...`
    /// For N items, N-1 offsets are stored. Item 0 starts after the offset table.
    pub data: &'a [u8],
}

impl<'a> ArrayValue<'a> {
    /// Get the number of items in the array.
    ///
    /// Array format: `[u32 count][u32 offset₂][u32 offset₃]...[item₁][item₂]...`
    pub fn len(&self) -> usize {
        if self.data.len() < 4 {
            return 0;
        }
        let bytes: [u8; 4] = self.data[0..4].try_into().unwrap();
        u32::from_le_bytes(bytes) as usize
    }

    /// Returns true if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get an item at a specific index (O(1) access).
    pub fn get(&self, index: usize) -> Option<RowRef<'a>> {
        let count = self.len();
        if index >= count {
            return None;
        }

        // Header size: 4 bytes count + (count-1) * 4 bytes offsets
        let header_size = 4 + if count > 1 { (count - 1) * 4 } else { 0 };

        // Get start offset
        let start = if index == 0 {
            header_size
        } else {
            // Offset for item i is at position 4 + (i-1) * 4
            let offset_pos = 4 + (index - 1) * 4;
            let bytes: [u8; 4] = self.data.get(offset_pos..offset_pos + 4)?.try_into().ok()?;
            u32::from_le_bytes(bytes) as usize
        };

        // Get end offset
        let end = if index == count - 1 {
            self.data.len()
        } else {
            let offset_pos = 4 + index * 4;
            let bytes: [u8; 4] = self.data.get(offset_pos..offset_pos + 4)?.try_into().ok()?;
            u32::from_le_bytes(bytes) as usize
        };

        let item_data = self.data.get(start..end)?;
        Some(RowRef {
            descriptor: self.item_descriptor,
            buffer: item_data,
        })
    }

    /// Iterate over items in the array.
    pub fn iter(&self) -> ArrayValueIter<'a> {
        let count = self.len();
        ArrayValueIter {
            array: *self,
            current_index: 0,
            count,
        }
    }
}

/// Iterator over array items.
pub struct ArrayValueIter<'a> {
    array: ArrayValue<'a>,
    current_index: usize,
    count: usize,
}

impl<'a> Iterator for ArrayValueIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.count {
            return None;
        }
        let item = self.array.get(self.current_index)?;
        self.current_index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.current_index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ArrayValueIter<'a> {}

impl<'a> RowValue<'a> {
    /// Convert to PredicateValue for use in policy/predicate comparisons.
    ///
    /// This is the preferred conversion method - it produces a minimal value
    /// type suitable for comparisons without complex nested types.
    pub fn to_predicate_value(&self) -> PredicateValue {
        match self {
            RowValue::Bool(v) => PredicateValue::Bool(*v),
            RowValue::I32(v) => PredicateValue::I32(*v),
            RowValue::U32(v) => PredicateValue::U32(*v),
            RowValue::I64(v) => PredicateValue::I64(*v),
            RowValue::F64(v) => PredicateValue::F64(*v),
            RowValue::Ref(v) => PredicateValue::Ref(*v),
            RowValue::String(v) => PredicateValue::String((*v).to_string()),
            RowValue::Bytes(v) => PredicateValue::Bytes((*v).to_vec()),
            RowValue::Null => PredicateValue::Null,
            // Complex types not directly representable in PredicateValue
            RowValue::Blob(_) | RowValue::BlobArray(_) | RowValue::Array(_) => PredicateValue::Null,
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
    fn get_column(&self, col: &'a ColDescriptor) -> Option<RowValue<'a>> {
        if col.is_fixed_size() {
            self.get_fixed(col)
        } else {
            self.get_variable(col)
        }
    }

    /// Get a fixed-size column value.
    fn get_fixed(&self, col: &ColDescriptor) -> Option<RowValue<'a>> {
        let offset = col.offset;
        let data = &self.buffer[offset..];

        if col.nullable {
            // First byte is presence flag
            if data.first()? == &0 {
                return Some(RowValue::Null);
            }
            // Data starts after presence byte
            let data = &data[1..];
            match &col.ty {
                ColumnType::Bool => Some(RowValue::Bool(data.first()? != &0)),
                ColumnType::I32 => {
                    let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                    Some(RowValue::I32(i32::from_le_bytes(bytes)))
                }
                ColumnType::U32 => {
                    let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                    Some(RowValue::U32(u32::from_le_bytes(bytes)))
                }
                ColumnType::I64 => {
                    let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                    Some(RowValue::I64(i64::from_le_bytes(bytes)))
                }
                ColumnType::F64 => {
                    let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                    Some(RowValue::F64(f64::from_le_bytes(bytes)))
                }
                ColumnType::ObjectId | ColumnType::Ref(_) => {
                    let bytes: [u8; 16] = data.get(..16)?.try_into().ok()?;
                    Some(RowValue::Ref(ObjectId::from_le_bytes(bytes)))
                }
                _ => None, // Not a fixed-size type
            }
        } else {
            match &col.ty {
                ColumnType::Bool => Some(RowValue::Bool(data.first()? != &0)),
                ColumnType::I32 => {
                    let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                    Some(RowValue::I32(i32::from_le_bytes(bytes)))
                }
                ColumnType::U32 => {
                    let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
                    Some(RowValue::U32(u32::from_le_bytes(bytes)))
                }
                ColumnType::I64 => {
                    let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                    Some(RowValue::I64(i64::from_le_bytes(bytes)))
                }
                ColumnType::F64 => {
                    let bytes: [u8; 8] = data.get(..8)?.try_into().ok()?;
                    Some(RowValue::F64(f64::from_le_bytes(bytes)))
                }
                ColumnType::ObjectId | ColumnType::Ref(_) => {
                    let bytes: [u8; 16] = data.get(..16)?.try_into().ok()?;
                    Some(RowValue::Ref(ObjectId::from_le_bytes(bytes)))
                }
                _ => None, // Not a fixed-size type
            }
        }
    }

    /// Get a variable-size column value.
    fn get_variable(&self, col: &'a ColDescriptor) -> Option<RowValue<'a>> {
        let var_idx = col.offset;

        // Parse varint header to find the offset and length
        let (offset, len) = self.find_variable_column(var_idx)?;
        let data = self.buffer.get(offset..offset + len)?;

        // Handle nullable types
        let (_is_null, value_data) = if col.nullable {
            if data.is_empty() || data[0] == 0 {
                return Some(RowValue::Null);
            }
            (false, &data[1..])
        } else {
            (false, data)
        };

        match &col.ty {
            ColumnType::String => {
                let s = std::str::from_utf8(value_data).ok()?;
                Some(RowValue::String(s))
            }
            ColumnType::Bytes => Some(RowValue::Bytes(value_data)),
            ColumnType::Blob => {
                let (content_ref, _) = ContentRef::from_row_bytes(value_data).ok()?;
                Some(RowValue::Blob(content_ref))
            }
            ColumnType::BlobArray => {
                if value_data.len() < 4 {
                    return None;
                }
                let count_bytes: [u8; 4] = value_data[0..4].try_into().ok()?;
                let count = u32::from_le_bytes(count_bytes) as usize;
                let mut pos = 4;

                let mut refs = Vec::with_capacity(count);
                for _ in 0..count {
                    let (content_ref, consumed) =
                        ContentRef::from_row_bytes(&value_data[pos..]).ok()?;
                    refs.push(content_ref);
                    pos += consumed;
                }
                Some(RowValue::BlobArray(refs))
            }
            ColumnType::Array(item_descriptor) => Some(RowValue::Array(ArrayValue {
                item_descriptor: item_descriptor.as_ref(),
                data: value_data,
            })),
            _ => None, // Not a variable-size type
        }
    }

    /// Find the offset and length of a variable-size column.
    ///
    /// Buffer layout for variable columns:
    /// ```text
    /// [fixed section][u32 offset₂][u32 offset₃]...[var_data₁][var_data₂]...
    /// ```
    ///
    /// For N variable columns, N-1 offsets are stored. Column 0 starts after
    /// the offset table. Column N-1 ends at buffer end. O(1) access.
    fn find_variable_column(&self, var_idx: usize) -> Option<(usize, usize)> {
        let var_count = self.descriptor.variable_count;
        if var_idx >= var_count {
            return None;
        }

        let fixed_size = self.descriptor.fixed_size;

        // Special case: single variable column (no offset table)
        if var_count == 1 {
            let start = fixed_size;
            let len = self.buffer.len() - start;
            return Some((start, len));
        }

        // Offset table has N-1 entries for N variable columns
        let offset_table_size = (var_count - 1) * 4;
        let var_data_start = fixed_size + offset_table_size;

        // Get start offset for this column
        let start = if var_idx == 0 {
            // First column starts right after offset table
            var_data_start
        } else {
            // Read offset from table (0-indexed: offset for column i is at position i-1)
            let offset_pos = fixed_size + (var_idx - 1) * 4;
            let bytes: [u8; 4] = self
                .buffer
                .get(offset_pos..offset_pos + 4)?
                .try_into()
                .ok()?;
            u32::from_le_bytes(bytes) as usize
        };

        // Get end offset for this column
        let end = if var_idx == var_count - 1 {
            // Last column ends at buffer end
            self.buffer.len()
        } else {
            // Read next column's offset from table
            let offset_pos = fixed_size + var_idx * 4;
            let bytes: [u8; 4] = self
                .buffer
                .get(offset_pos..offset_pos + 4)?
                .try_into()
                .ok()?;
            u32::from_le_bytes(bytes) as usize
        };

        Some((start, end - start))
    }
}

/// An owned row with its own buffer. For caching and WASM transfer.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedRow {
    /// Descriptor defining the row structure.
    pub descriptor: Arc<RowDescriptor>,
    /// Owned buffer containing row data.
    pub buffer: Vec<u8>,
}

/// An owned row with its ObjectId. Combines identity and data.
///
/// This type is used throughout the codebase where we need both the row ID
/// and the row data together. The `id` is stored as the first column in the
/// row buffer (16 bytes, ObjectId as u128 LE).
///
/// The binary format for WASM transfer is simply the row buffer:
/// ```text
/// [row buffer bytes] (id is first 16 bytes as ObjectId/u128 LE)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct IdentifiedRow {
    /// The row data (id is first 16 bytes of buffer).
    pub row: OwnedRow,
}

impl IdentifiedRow {
    /// Create a new IdentifiedRow.
    /// The id should already be in the row buffer as the first column.
    pub fn new(_id: ObjectId, row: OwnedRow) -> Self {
        // id is already in the row buffer, we just store the row
        Self { row }
    }

    /// Get the row ID (extracted from the row buffer).
    pub fn id(&self) -> ObjectId {
        // id is the first 16 bytes of the buffer (ObjectId column at offset 0)
        if self.row.buffer.len() >= 16 {
            let id_bytes: [u8; 16] = self.row.buffer[0..16].try_into().unwrap();
            ObjectId::from_le_bytes(id_bytes)
        } else {
            ObjectId::new(0)
        }
    }

    /// Get a reference to the row.
    pub fn row(&self) -> &OwnedRow {
        &self.row
    }

    /// Get a borrowed view of the row.
    pub fn as_row_ref(&self) -> RowRef<'_> {
        self.row.as_ref()
    }

    /// Destructure into (ObjectId, OwnedRow) tuple for compatibility.
    pub fn into_tuple(self) -> (ObjectId, OwnedRow) {
        let id = self.id();
        (id, self.row)
    }

    /// Create from (ObjectId, OwnedRow) tuple for compatibility.
    /// The id should already be in the row buffer.
    pub fn from_tuple((_id, row): (ObjectId, OwnedRow)) -> Self {
        Self { row }
    }

    /// Encode to binary format for WASM transfer.
    ///
    /// Format: `[row buffer]` (id is first 16 bytes)
    pub fn to_bytes(&self) -> Vec<u8> {
        // Row buffer already contains id as first 16 bytes
        self.row.buffer.clone()
    }

    /// Decode from binary format.
    ///
    /// Returns None if buffer is too short.
    pub fn from_bytes(data: &[u8], descriptor: Arc<RowDescriptor>) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        // The entire data is the row buffer (id is first 16 bytes)
        let row = OwnedRow::new(descriptor, data.to_vec());
        Some(Self { row })
    }
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

    /// Create a new OwnedRow with the `id` column set to the given value.
    ///
    /// This is used when inserting rows - the id is generated by the database
    /// and needs to be injected into the row buffer.
    pub fn with_id(&self, id: ObjectId) -> Self {
        // Create a new row from this one with the id column set
        RowBuilder::from_owned_row(self)
            .set_ref(0, id) // id is always column 0
            .build()
    }

    /// Create a new OwnedRow with qualified column names.
    ///
    /// Converts column names from `column` to `table.column` format.
    /// This is needed for JOIN queries where predicates use qualified names.
    pub fn qualify_columns(&self, table: &str, schema: &TableSchema) -> Self {
        // Create a new descriptor with qualified column names
        let qualified_descriptor =
            Arc::new(RowDescriptor::from_table_schema_qualified(schema, table));

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
            RowValue::Array(arr) => {
                // Collect items into OwnedRows
                let items: Vec<OwnedRow> = arr
                    .iter()
                    .map(|row_ref| {
                        OwnedRow::new(
                            Arc::new(row_ref.descriptor.clone()),
                            row_ref.buffer.to_vec(),
                        )
                    })
                    .collect();
                builder.set_array(idx, &items)
            }
            RowValue::Blob(content_ref) => builder.set_blob(idx, content_ref),
            RowValue::BlobArray(refs) => builder.set_blob_array(idx, &refs),
        }
    }

    /// Create a new OwnedRow by projecting and renaming columns.
    ///
    /// Used by Projection nodes to convert qualified column names back to
    /// unqualified names (e.g., "documents.title" → "title").
    ///
    /// Columns not in the map are excluded from the output.
    pub fn project_rename(
        &self,
        column_map: &std::collections::HashMap<String, String>,
        output_descriptor: Arc<RowDescriptor>,
    ) -> Self {
        let mut builder = RowBuilder::new(output_descriptor.clone());

        for (old_name, new_name) in column_map {
            if let Some(value) = self.get_by_name(old_name)
                && let Some(new_idx) = output_descriptor.column_index(new_name)
            {
                builder = Self::set_from_row_value(builder, new_idx, value);
            }
        }

        builder.build()
    }

    /// Merge multiple rows into a single combined row.
    ///
    /// This is used for JOIN operations. The rows are merged in order,
    /// with columns from each row appearing in sequence. Values are copied
    /// in buffer order (not schema order) for efficiency.
    ///
    /// The output descriptor preserves the exact buffer order from the source rows.
    pub fn merge_rows(rows: &[&OwnedRow]) -> OwnedRow {
        if rows.is_empty() {
            // Return an empty row with empty descriptor
            return OwnedRow::new(Arc::new(RowDescriptor::new(std::iter::empty())), vec![]);
        }

        if rows.len() == 1 {
            return rows[0].clone();
        }

        // Build combined descriptor preserving buffer order from sources
        let descriptors: Vec<&RowDescriptor> = rows.iter().map(|r| r.descriptor.as_ref()).collect();
        let combined_descriptor =
            Arc::new(RowDescriptor::concat_preserving_buffer_order(&descriptors));

        // Build the combined row using RowBuilder
        let mut builder = RowBuilder::new(combined_descriptor.clone());
        let mut output_col_idx = 0;

        // Copy values from each source row in buffer order
        for row in rows {
            for buf_idx in 0..row.descriptor.columns.len() {
                if let Some(value) = row.get(buf_idx) {
                    builder = Self::set_from_row_value(builder, output_col_idx, value);
                }
                output_col_idx += 1;
            }
        }

        builder.build()
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

    /// Create a builder initialized from an existing row.
    ///
    /// This is useful for updates where you want to modify only some columns
    /// while preserving others.
    pub fn from_owned_row(row: &OwnedRow) -> Self {
        let mut builder = Self::new(row.descriptor.clone());
        for idx in 0..row.descriptor.columns.len() {
            if let Some(value) = row.get(idx) {
                builder = builder.set_from_row_value(idx, value);
            }
        }
        builder
    }

    /// Set a boolean column value.
    pub fn set_bool(mut self, col_idx: usize, value: bool) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::Bool)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1] = if value { 1 } else { 0 };
            } else {
                self.fixed_section[offset] = if value { 1 } else { 0 };
            }
        }
        self
    }

    /// Set an i32 column value.
    pub fn set_i32(mut self, col_idx: usize, value: i32) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::I32)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1..offset + 5].copy_from_slice(&value.to_le_bytes());
            } else {
                self.fixed_section[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            }
        }
        self
    }

    /// Set a u32 column value.
    pub fn set_u32(mut self, col_idx: usize, value: u32) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::U32)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1..offset + 5].copy_from_slice(&value.to_le_bytes());
            } else {
                self.fixed_section[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            }
        }
        self
    }

    /// Set an i64 column value.
    pub fn set_i64(mut self, col_idx: usize, value: i64) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::I64)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1..offset + 9].copy_from_slice(&value.to_le_bytes());
            } else {
                self.fixed_section[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
        }
        self
    }

    /// Set an f64 column value.
    pub fn set_f64(mut self, col_idx: usize, value: f64) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::F64)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1..offset + 9].copy_from_slice(&value.to_le_bytes());
            } else {
                self.fixed_section[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
        }
        self
    }

    /// Set a Ref (ObjectId) or ObjectId column value.
    pub fn set_ref(mut self, col_idx: usize, value: ObjectId) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.is_fixed_size()
            && matches!(&col.ty, ColumnType::Ref(_) | ColumnType::ObjectId)
        {
            let offset = col.offset;
            if col.nullable {
                self.fixed_section[offset] = 1; // present
                self.fixed_section[offset + 1..offset + 17].copy_from_slice(&value.0.to_le_bytes());
            } else {
                self.fixed_section[offset..offset + 16].copy_from_slice(&value.0.to_le_bytes());
            }
        }
        self
    }

    /// Set a string column value.
    pub fn set_string(mut self, col_idx: usize, value: &str) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && !col.is_fixed_size()
            && matches!(&col.ty, ColumnType::String)
        {
            let var_idx = col.offset;
            if col.nullable {
                let mut data = vec![1u8]; // present
                data.extend_from_slice(value.as_bytes());
                self.variable_sections[var_idx] = data;
            } else {
                self.variable_sections[var_idx] = value.as_bytes().to_vec();
            }
        }
        self
    }

    /// Set a bytes column value.
    pub fn set_bytes(mut self, col_idx: usize, value: &[u8]) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && !col.is_fixed_size()
            && matches!(&col.ty, ColumnType::Bytes)
        {
            let var_idx = col.offset;
            if col.nullable {
                let mut data = vec![1u8]; // present
                data.extend_from_slice(value);
                self.variable_sections[var_idx] = data;
            } else {
                self.variable_sections[var_idx] = value.to_vec();
            }
        }
        self
    }

    /// Set a blob column value.
    pub fn set_blob(mut self, col_idx: usize, value: ContentRef) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && !col.is_fixed_size()
            && matches!(&col.ty, ColumnType::Blob)
        {
            let var_idx = col.offset;
            if col.nullable {
                let mut data = vec![1u8]; // present
                data.extend_from_slice(&value.to_row_bytes());
                self.variable_sections[var_idx] = data;
            } else {
                self.variable_sections[var_idx] = value.to_row_bytes();
            }
        }
        self
    }

    /// Set a blob array column value.
    ///
    /// Format: `[u32 count][content_ref₁][content_ref₂]...`
    pub fn set_blob_array(mut self, col_idx: usize, values: &[ContentRef]) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && !col.is_fixed_size()
            && matches!(&col.ty, ColumnType::BlobArray)
        {
            let var_idx = col.offset;
            let mut data = if col.nullable { vec![1u8] } else { Vec::new() };
            data.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for v in values {
                data.extend_from_slice(&v.to_row_bytes());
            }
            self.variable_sections[var_idx] = data;
        }
        self
    }

    /// Set a nullable column to null.
    pub fn set_null(mut self, col_idx: usize) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && col.nullable
        {
            if col.is_fixed_size() {
                let offset = col.offset;
                self.fixed_section[offset] = 0; // null flag
            } else {
                let var_idx = col.offset;
                self.variable_sections[var_idx] = vec![0u8]; // null flag
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

    /// Set an array column value.
    ///
    /// Format: `[u32 count][u32 offset₂][u32 offset₃]...[item₁][item₂]...`
    /// For N items, N-1 offsets are stored. Item 0 starts after the offset table.
    pub fn set_array(mut self, col_idx: usize, items: &[OwnedRow]) -> Self {
        if let Some(col) = self.descriptor.columns.get(col_idx)
            && !col.is_fixed_size()
            && matches!(&col.ty, ColumnType::Array(_))
        {
            let var_idx = col.offset;
            let mut data = if col.nullable { vec![1u8] } else { Vec::new() };
            let count = items.len();

            // Write item count as u32
            data.extend_from_slice(&(count as u32).to_le_bytes());

            if count == 0 {
                // Empty array: just the count
                self.variable_sections[var_idx] = data;
                return self;
            }

            // Calculate header size: count (4) + (N-1) offsets * 4
            let array_header_start = data.len(); // Account for nullable byte if present
            let offset_table_size = if count > 1 { (count - 1) * 4 } else { 0 };
            let items_start = array_header_start - 4 + 4 + offset_table_size; // relative to array data start

            // Calculate absolute offsets for items 1 through N-1
            let mut current_offset = items_start;
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    // Write offset for item i
                    data.extend_from_slice(&(current_offset as u32).to_le_bytes());
                }
                current_offset += item.buffer.len();
            }

            // Write item data
            for item in items {
                data.extend_from_slice(&item.buffer);
            }

            self.variable_sections[var_idx] = data;
        }
        self
    }

    /// Set an array column value by name.
    pub fn set_array_by_name(self, name: &str, items: &[OwnedRow]) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_array(idx, items)
        } else {
            self
        }
    }

    /// Set a blob column value by name.
    pub fn set_blob_by_name(self, name: &str, value: ContentRef) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_blob(idx, value)
        } else {
            self
        }
    }

    /// Set a column value from a RowValue.
    ///
    /// This is useful when copying values between rows or when
    /// working with dynamically typed row data.
    pub fn set_from_row_value(self, idx: usize, value: RowValue<'_>) -> Self {
        match value {
            RowValue::Bool(v) => self.set_bool(idx, v),
            RowValue::I32(v) => self.set_i32(idx, v),
            RowValue::U32(v) => self.set_u32(idx, v),
            RowValue::I64(v) => self.set_i64(idx, v),
            RowValue::F64(v) => self.set_f64(idx, v),
            RowValue::String(v) => self.set_string(idx, v),
            RowValue::Bytes(v) => self.set_bytes(idx, v),
            RowValue::Ref(v) => self.set_ref(idx, v),
            RowValue::Null => self.set_null(idx),
            RowValue::Array(arr) => {
                // Collect items into OwnedRows
                let items: Vec<OwnedRow> = arr
                    .iter()
                    .map(|row_ref| {
                        OwnedRow::new(
                            Arc::new(row_ref.descriptor.clone()),
                            row_ref.buffer.to_vec(),
                        )
                    })
                    .collect();
                self.set_array(idx, &items)
            }
            RowValue::Blob(content_ref) => self.set_blob(idx, content_ref),
            RowValue::BlobArray(refs) => self.set_blob_array(idx, &refs),
        }
    }

    /// Set a column value from a PredicateValue.
    ///
    /// This is useful when applying SQL UPDATE assignments or setting
    /// values from parsed SQL literals.
    pub fn set_from_predicate_value(self, idx: usize, value: &PredicateValue) -> Self {
        match value {
            PredicateValue::Bool(v) => self.set_bool(idx, *v),
            PredicateValue::I32(v) => self.set_i32(idx, *v),
            PredicateValue::U32(v) => self.set_u32(idx, *v),
            PredicateValue::I64(v) => self.set_i64(idx, *v),
            PredicateValue::F64(v) => self.set_f64(idx, *v),
            PredicateValue::String(v) => self.set_string(idx, v),
            PredicateValue::Bytes(v) => self.set_bytes(idx, v),
            PredicateValue::Ref(v) => self.set_ref(idx, *v),
            PredicateValue::Null => self.set_null(idx),
        }
    }

    /// Set a column value from a PredicateValue, looking up by column name.
    pub fn set_from_predicate_value_by_name(self, name: &str, value: &PredicateValue) -> Self {
        if let Some(idx) = self.descriptor.column_index(name) {
            self.set_from_predicate_value(idx, value)
        } else {
            self
        }
    }

    /// Build the final row buffer.
    ///
    /// Buffer layout:
    /// ```text
    /// [fixed-size columns][u32 offset₂][u32 offset₃]...[var_data₁][var_data₂][var_data₃]...
    /// ```
    ///
    /// For N variable columns, we store N-1 offsets. The first variable column
    /// starts immediately after the offset table. The last variable column ends
    /// at the buffer end. Offsets are absolute from buffer start.
    pub fn build(self) -> OwnedRow {
        let mut buffer = self.fixed_section;
        let var_count = self.variable_sections.len();

        if var_count == 0 {
            // No variable columns, no offset table needed
            return OwnedRow {
                descriptor: self.descriptor,
                buffer,
            };
        }

        // Calculate where variable data starts (after fixed section + offset table)
        // We store N-1 offsets for N variable columns
        let offset_table_size = if var_count > 1 {
            (var_count - 1) * 4
        } else {
            0
        };
        let var_data_start = buffer.len() + offset_table_size;

        // Calculate absolute offsets for each variable column (except the first)
        let mut current_offset = var_data_start;
        for i in 0..var_count {
            if i > 0 {
                // Write offset for column i (columns 1 through N-1)
                buffer.extend_from_slice(&(current_offset as u32).to_le_bytes());
            }
            current_offset += self.variable_sections[i].len();
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
/// Matches columns by name from left and right rows to the target descriptor.
/// This handles the case where the target descriptor may have a different
/// column order due to fixed-first reordering.
pub fn join_rows(
    left: RowRef<'_>,
    right: RowRef<'_>,
    target_descriptor: Arc<RowDescriptor>,
) -> OwnedRow {
    let mut builder = RowBuilder::new(target_descriptor.clone());

    // For each target column, find the value from left or right row by name
    for (target_idx, target_col) in target_descriptor.columns.iter().enumerate() {
        // Try left row first
        let value = left
            .get_by_name(&target_col.name)
            .or_else(|| right.get_by_name(&target_col.name));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::schema::ColumnType;

    #[test]
    fn test_row_descriptor_new() {
        let desc = RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::I32, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]);

        // Fixed columns should come first
        assert_eq!(desc.columns.len(), 3);
        assert_eq!(desc.fixed_size, 5); // i32 (4) + bool (1)
        assert_eq!(desc.variable_count, 1);

        // Check that fixed columns have correct offsets
        let age_col = desc.column("age").unwrap();
        assert!(age_col.is_fixed_size());

        let active_col = desc.column("active").unwrap();
        assert!(active_col.is_fixed_size());

        let name_col = desc.column("name").unwrap();
        assert!(!name_col.is_fixed_size());
    }

    #[test]
    fn test_row_builder_and_reader() {
        let desc = Arc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::I32, false),
            ("score".to_string(), ColumnType::F64, false),
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
            ("name".to_string(), ColumnType::String, true),
            ("age".to_string(), ColumnType::I32, true),
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
            ("a".to_string(), ColumnType::I32, false),
            ("b".to_string(), ColumnType::String, false),
            ("c".to_string(), ColumnType::I64, false),
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
            ("a".to_string(), ColumnType::I32, false),
            ("c".to_string(), ColumnType::I64, false),
        ]));

        let projected = project_row(row.as_ref(), &[a_idx, c_idx], target_desc);

        assert_eq!(projected.get_by_name("a"), Some(RowValue::I32(1)));
        assert_eq!(projected.get_by_name("c"), Some(RowValue::I64(100)));
    }

    #[test]
    fn test_join_descriptor() {
        let left_desc = RowDescriptor::new([
            ("a".to_string(), ColumnType::I32, false),
            ("b".to_string(), ColumnType::String, false),
        ]);

        let right_desc = RowDescriptor::new([
            ("c".to_string(), ColumnType::I64, false),
            ("d".to_string(), ColumnType::Bool, false),
        ]);

        let joined = left_desc.join(&right_desc);

        assert_eq!(joined.columns.len(), 4);
        assert!(joined.column("a").is_some());
        assert!(joined.column("b").is_some());
        assert!(joined.column("c").is_some());
        assert!(joined.column("d").is_some());
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

        // Should have 5 columns (id + 4 user-defined)
        assert_eq!(desc.columns.len(), 5);

        // Check that id column is first
        let id_col = desc.column("id").unwrap();
        assert!(!id_col.nullable);
        assert_eq!(id_col.ty, ColumnType::ObjectId);

        // Check that nullable columns have nullable flag set
        let name_col = desc.column("name").unwrap();
        assert!(!name_col.nullable);
        assert_eq!(name_col.ty, ColumnType::String);

        let email_col = desc.column("email").unwrap();
        assert!(email_col.nullable);
        assert_eq!(email_col.ty, ColumnType::String);

        // Fixed-size columns should have computed offsets
        let age_col = desc.column("age").unwrap();
        assert_eq!(age_col.ty, ColumnType::I32);
        assert!(age_col.is_fixed_size());

        let active_col = desc.column("active").unwrap();
        assert_eq!(active_col.ty, ColumnType::Bool);
        assert!(active_col.is_fixed_size());

        // Fixed size should be: ObjectId (16) + i32 (4) + bool (1) = 21
        assert_eq!(desc.fixed_size, 21);

        // Variable count should be: name + email = 2
        assert_eq!(desc.variable_count, 2);
    }

    #[test]
    fn test_project_rename() {
        use std::collections::HashMap;

        // Create a row with qualified column names (like after a JOIN)
        let source_desc = Arc::new(RowDescriptor::new([
            ("documents.id".to_string(), ColumnType::I32, false),
            ("documents.title".to_string(), ColumnType::String, false),
            ("documents.folder_id".to_string(), ColumnType::I64, false),
        ]));

        let id_idx = source_desc.column_index("documents.id").unwrap();
        let title_idx = source_desc.column_index("documents.title").unwrap();
        let folder_idx = source_desc.column_index("documents.folder_id").unwrap();

        let row = RowBuilder::new(source_desc.clone())
            .set_i32(id_idx, 42)
            .set_string(title_idx, "Test Document")
            .set_i64(folder_idx, 123)
            .build();

        // Verify source row has qualified names
        assert_eq!(
            row.get_by_name("documents.title"),
            Some(RowValue::String("Test Document"))
        );
        assert_eq!(row.get_by_name("title"), None);

        // Create column map: qualified -> unqualified
        let mut column_map = HashMap::new();
        column_map.insert("documents.id".to_string(), "id".to_string());
        column_map.insert("documents.title".to_string(), "title".to_string());
        column_map.insert("documents.folder_id".to_string(), "folder_id".to_string());

        // Create output descriptor with unqualified names
        let output_desc = Arc::new(RowDescriptor::new([
            ("id".to_string(), ColumnType::I32, false),
            ("title".to_string(), ColumnType::String, false),
            ("folder_id".to_string(), ColumnType::I64, false),
        ]));

        // Project and rename
        let projected = row.project_rename(&column_map, output_desc);

        // Verify projected row has unqualified names
        assert_eq!(projected.get_by_name("id"), Some(RowValue::I32(42)));
        assert_eq!(
            projected.get_by_name("title"),
            Some(RowValue::String("Test Document"))
        );
        assert_eq!(projected.get_by_name("folder_id"), Some(RowValue::I64(123)));
        assert_eq!(projected.get_by_name("documents.title"), None);
    }
}
