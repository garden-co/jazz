//! Schema lenses for bidirectional data transformation.
//!
//! Lenses define how to transform row data between different schema versions.
//! They are bidirectional: each lens can transform data both forward (from parent
//! to child schema) and backward (from child to parent schema).
//!
//! ## Design
//!
//! Lenses are auto-generated from schema diffs where possible, with SQL expressions
//! for custom transformations. If a transformation cannot be applied (e.g., missing
//! required data), the row becomes invisible to clients using that schema version.
//!
//! ## Example
//!
//! ```text
//! Schema v1: { id: ObjectId, title: String }
//! Schema v2: { id: ObjectId, name: String }  // title renamed to name
//!
//! Lens (v1 → v2):
//!   forward: [Rename { from: "title", to: "name" }]
//!   backward: [Rename { from: "name", to: "title" }]
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::row_buffer::{OwnedRow, RowBuilder, RowDescriptor, RowRef, RowValue};
use crate::sql::schema::ColumnType;

/// A lens defines bidirectional transformations between two schema versions.
///
/// Each lens is associated with one parent in the descriptor's `parent_descriptors`
/// list, at the same index in the `lenses` list.
#[derive(Debug, Clone, PartialEq)]
pub struct Lens {
    /// Transforms to apply when converting from parent schema to this schema.
    pub forward: Vec<ColumnTransform>,
    /// Transforms to apply when converting from this schema to parent schema.
    /// For simple transforms like Rename, this is automatically computed.
    pub backward: Vec<ColumnTransform>,
}

impl Lens {
    /// Create a new lens with the given transforms.
    pub fn new(forward: Vec<ColumnTransform>, backward: Vec<ColumnTransform>) -> Self {
        Lens { forward, backward }
    }

    /// Create an empty lens (identity transformation).
    pub fn identity() -> Self {
        Lens {
            forward: vec![],
            backward: vec![],
        }
    }

    /// Create a lens from forward transforms, auto-computing backward transforms
    /// where possible.
    pub fn from_forward(forward: Vec<ColumnTransform>) -> Self {
        let backward = forward.iter().filter_map(|t| t.invert()).collect();
        Lens { forward, backward }
    }

    /// Check if this lens is an identity (no transformations needed).
    pub fn is_identity(&self) -> bool {
        self.forward.is_empty() && self.backward.is_empty()
    }

    /// Compose this lens with another lens.
    /// The result applies `self` first, then `other`.
    pub fn compose(&self, other: &Lens) -> Lens {
        let mut forward = self.forward.clone();
        forward.extend(other.forward.iter().cloned());

        let mut backward = other.backward.clone();
        backward.extend(self.backward.iter().cloned());

        Lens { forward, backward }
    }

    /// Apply the forward transformation to a row.
    ///
    /// Transforms a row from the parent schema to this schema.
    /// Returns `LensError::Incompatible` if the row cannot be transformed
    /// (e.g., required column missing or SQL expression fails).
    pub fn apply_forward(&self, row: RowRef<'_>) -> Result<OwnedRow, LensError> {
        apply_transforms(row, &self.forward)
    }

    /// Apply the backward transformation to a row.
    ///
    /// Transforms a row from this schema to the parent schema.
    /// Returns `LensError::Incompatible` if the row cannot be transformed.
    pub fn apply_backward(&self, row: RowRef<'_>) -> Result<OwnedRow, LensError> {
        apply_transforms(row, &self.backward)
    }

    /// Apply forward transformation to an owned row.
    pub fn apply_forward_owned(&self, row: &OwnedRow) -> Result<OwnedRow, LensError> {
        self.apply_forward(row.as_ref())
    }

    /// Apply backward transformation to an owned row.
    pub fn apply_backward_owned(&self, row: &OwnedRow) -> Result<OwnedRow, LensError> {
        self.apply_backward(row.as_ref())
    }

    /// Compute the target descriptor after applying forward transforms.
    ///
    /// Given a source descriptor, returns what the descriptor would look like
    /// after applying the forward transforms.
    pub fn target_descriptor(&self, source: &RowDescriptor) -> RowDescriptor {
        compute_target_descriptor(source, &self.forward)
    }

    /// Compute the source descriptor after applying backward transforms.
    ///
    /// Given a target descriptor, returns what the descriptor would look like
    /// after applying the backward transforms (i.e., the original source).
    pub fn source_descriptor(&self, target: &RowDescriptor) -> RowDescriptor {
        compute_target_descriptor(target, &self.backward)
    }

    /// Serialize the lens to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Forward transforms
        buf.extend_from_slice(&(self.forward.len() as u32).to_le_bytes());
        for transform in &self.forward {
            transform.serialize(&mut buf);
        }

        // Backward transforms
        buf.extend_from_slice(&(self.backward.len() as u32).to_le_bytes());
        for transform in &self.backward {
            transform.serialize(&mut buf);
        }

        buf
    }

    /// Deserialize a lens from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), LensError> {
        let mut pos = 0;

        // Forward transforms
        if data.len() < pos + 4 {
            return Err(LensError::DeserializationError(
                "unexpected end of data".into(),
            ));
        }
        let forward_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut forward = Vec::with_capacity(forward_count);
        for _ in 0..forward_count {
            let (transform, new_pos) = ColumnTransform::deserialize(&data[pos..])?;
            forward.push(transform);
            pos += new_pos;
        }

        // Backward transforms
        if data.len() < pos + 4 {
            return Err(LensError::DeserializationError(
                "unexpected end of data".into(),
            ));
        }
        let backward_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut backward = Vec::with_capacity(backward_count);
        for _ in 0..backward_count {
            let (transform, new_pos) = ColumnTransform::deserialize(&data[pos..])?;
            backward.push(transform);
            pos += new_pos;
        }

        Ok((Lens { forward, backward }, pos))
    }
}

/// A single column transformation.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnTransform {
    /// Rename a column.
    Rename { from: String, to: String },

    /// Add a new column with a default value.
    /// If `default` is None and the column is required, rows without this
    /// column become invisible.
    Add {
        name: String,
        default: Option<DefaultValue>,
    },

    /// Remove a column.
    /// The column data is preserved in the source schema version.
    Remove { name: String },

    /// Transform a column value using a SQL expression.
    /// The expression can reference the original column value as `$value`.
    Transform {
        column: String,
        expr: SqlExpr,
        /// Expression for the reverse transformation (if invertible).
        reverse_expr: Option<SqlExpr>,
    },
}

impl ColumnTransform {
    /// Create a rename transform.
    pub fn rename(from: impl Into<String>, to: impl Into<String>) -> Self {
        ColumnTransform::Rename {
            from: from.into(),
            to: to.into(),
        }
    }

    /// Create an add column transform with a default value.
    pub fn add_with_default(name: impl Into<String>, default: DefaultValue) -> Self {
        ColumnTransform::Add {
            name: name.into(),
            default: Some(default),
        }
    }

    /// Create an add column transform without a default (nullable column).
    pub fn add_nullable(name: impl Into<String>) -> Self {
        ColumnTransform::Add {
            name: name.into(),
            default: None,
        }
    }

    /// Create a remove column transform.
    pub fn remove(name: impl Into<String>) -> Self {
        ColumnTransform::Remove { name: name.into() }
    }

    /// Create a transform using a SQL expression.
    pub fn transform(column: impl Into<String>, expr: SqlExpr) -> Self {
        ColumnTransform::Transform {
            column: column.into(),
            expr,
            reverse_expr: None,
        }
    }

    /// Create a transform with both forward and reverse expressions.
    pub fn transform_bidirectional(
        column: impl Into<String>,
        expr: SqlExpr,
        reverse_expr: SqlExpr,
    ) -> Self {
        ColumnTransform::Transform {
            column: column.into(),
            expr,
            reverse_expr: Some(reverse_expr),
        }
    }

    /// Compute the inverse of this transform, if possible.
    pub fn invert(&self) -> Option<ColumnTransform> {
        match self {
            ColumnTransform::Rename { from, to } => Some(ColumnTransform::Rename {
                from: to.clone(),
                to: from.clone(),
            }),
            ColumnTransform::Add { name, .. } => {
                Some(ColumnTransform::Remove { name: name.clone() })
            }
            ColumnTransform::Remove { name } => {
                // Can't automatically invert a remove - we don't know the type/default
                // This will be handled during lens generation with warnings
                Some(ColumnTransform::Add {
                    name: name.clone(),
                    default: None, // Will cause row to be invisible if column is required
                })
            }
            ColumnTransform::Transform {
                column,
                reverse_expr,
                expr,
                ..
            } => reverse_expr.as_ref().map(|rev| ColumnTransform::Transform {
                column: column.clone(),
                expr: rev.clone(),
                reverse_expr: Some(expr.clone()),
            }),
        }
    }

    /// Serialize to bytes.
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            ColumnTransform::Rename { from, to } => {
                buf.push(0); // tag
                serialize_string(buf, from);
                serialize_string(buf, to);
            }
            ColumnTransform::Add { name, default } => {
                buf.push(1); // tag
                serialize_string(buf, name);
                match default {
                    None => buf.push(0),
                    Some(d) => {
                        buf.push(1);
                        d.serialize(buf);
                    }
                }
            }
            ColumnTransform::Remove { name } => {
                buf.push(2); // tag
                serialize_string(buf, name);
            }
            ColumnTransform::Transform {
                column,
                expr,
                reverse_expr,
            } => {
                buf.push(3); // tag
                serialize_string(buf, column);
                expr.serialize(buf);
                match reverse_expr {
                    None => buf.push(0),
                    Some(e) => {
                        buf.push(1);
                        e.serialize(buf);
                    }
                }
            }
        }
    }

    /// Deserialize from bytes. Returns (transform, bytes_consumed).
    fn deserialize(data: &[u8]) -> Result<(Self, usize), LensError> {
        if data.is_empty() {
            return Err(LensError::DeserializationError(
                "unexpected end of data".into(),
            ));
        }

        let tag = data[0];
        let mut pos = 1;

        match tag {
            0 => {
                // Rename
                let (from, len) = deserialize_string(&data[pos..])?;
                pos += len;
                let (to, len) = deserialize_string(&data[pos..])?;
                pos += len;
                Ok((ColumnTransform::Rename { from, to }, pos))
            }
            1 => {
                // Add
                let (name, len) = deserialize_string(&data[pos..])?;
                pos += len;
                if pos >= data.len() {
                    return Err(LensError::DeserializationError(
                        "unexpected end of data".into(),
                    ));
                }
                let default = if data[pos] == 0 {
                    pos += 1;
                    None
                } else {
                    pos += 1;
                    let (d, len) = DefaultValue::deserialize(&data[pos..])?;
                    pos += len;
                    Some(d)
                };
                Ok((ColumnTransform::Add { name, default }, pos))
            }
            2 => {
                // Remove
                let (name, len) = deserialize_string(&data[pos..])?;
                pos += len;
                Ok((ColumnTransform::Remove { name }, pos))
            }
            3 => {
                // Transform
                let (column, len) = deserialize_string(&data[pos..])?;
                pos += len;
                let (expr, len) = SqlExpr::deserialize(&data[pos..])?;
                pos += len;
                if pos >= data.len() {
                    return Err(LensError::DeserializationError(
                        "unexpected end of data".into(),
                    ));
                }
                let reverse_expr = if data[pos] == 0 {
                    pos += 1;
                    None
                } else {
                    pos += 1;
                    let (e, len) = SqlExpr::deserialize(&data[pos..])?;
                    pos += len;
                    Some(e)
                };
                Ok((
                    ColumnTransform::Transform {
                        column,
                        expr,
                        reverse_expr,
                    },
                    pos,
                ))
            }
            _ => Err(LensError::DeserializationError(format!(
                "invalid transform tag: {}",
                tag
            ))),
        }
    }
}

/// Default value for an added column.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    /// NULL value.
    Null,
    /// Boolean literal.
    Bool(bool),
    /// Integer literal (i64).
    Int(i64),
    /// Float literal (f64).
    Float(f64),
    /// String literal.
    String(String),
    /// SQL expression to compute the default.
    Expr(SqlExpr),
}

impl DefaultValue {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            DefaultValue::Null => buf.push(0),
            DefaultValue::Bool(b) => {
                buf.push(1);
                buf.push(if *b { 1 } else { 0 });
            }
            DefaultValue::Int(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            DefaultValue::Float(n) => {
                buf.push(3);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            DefaultValue::String(s) => {
                buf.push(4);
                serialize_string(buf, s);
            }
            DefaultValue::Expr(e) => {
                buf.push(5);
                e.serialize(buf);
            }
        }
    }

    fn deserialize(data: &[u8]) -> Result<(Self, usize), LensError> {
        if data.is_empty() {
            return Err(LensError::DeserializationError(
                "unexpected end of data".into(),
            ));
        }

        let tag = data[0];
        match tag {
            0 => Ok((DefaultValue::Null, 1)),
            1 => {
                if data.len() < 2 {
                    return Err(LensError::DeserializationError(
                        "unexpected end of data".into(),
                    ));
                }
                Ok((DefaultValue::Bool(data[1] != 0), 2))
            }
            2 => {
                if data.len() < 9 {
                    return Err(LensError::DeserializationError(
                        "unexpected end of data".into(),
                    ));
                }
                let n = i64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok((DefaultValue::Int(n), 9))
            }
            3 => {
                if data.len() < 9 {
                    return Err(LensError::DeserializationError(
                        "unexpected end of data".into(),
                    ));
                }
                let n = f64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok((DefaultValue::Float(n), 9))
            }
            4 => {
                let (s, len) = deserialize_string(&data[1..])?;
                Ok((DefaultValue::String(s), 1 + len))
            }
            5 => {
                let (e, len) = SqlExpr::deserialize(&data[1..])?;
                Ok((DefaultValue::Expr(e), 1 + len))
            }
            _ => Err(LensError::DeserializationError(format!(
                "invalid default value tag: {}",
                tag
            ))),
        }
    }
}

/// A SQL expression for transforms and defaults.
///
/// This is a simplified representation that can be serialized.
/// For now, we store expressions as strings that will be parsed and evaluated.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    /// The SQL expression text (e.g., "COALESCE($value, 'unknown')")
    pub text: String,
}

impl SqlExpr {
    /// Create a new SQL expression.
    pub fn new(text: impl Into<String>) -> Self {
        SqlExpr { text: text.into() }
    }

    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_string(buf, &self.text);
    }

    fn deserialize(data: &[u8]) -> Result<(Self, usize), LensError> {
        let (text, len) = deserialize_string(data)?;
        Ok((SqlExpr { text }, len))
    }
}

/// Errors during lens operations.
#[derive(Debug, Clone, PartialEq)]
pub enum LensError {
    /// Row cannot be transformed (e.g., missing required data).
    Incompatible { reason: String },
    /// Error during serialization/deserialization.
    DeserializationError(String),
    /// Transform expression evaluation error.
    EvaluationError { column: String, reason: String },
}

impl std::fmt::Display for LensError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LensError::Incompatible { reason } => {
                write!(f, "row incompatible with schema: {}", reason)
            }
            LensError::DeserializationError(msg) => {
                write!(f, "lens deserialization error: {}", msg)
            }
            LensError::EvaluationError { column, reason } => {
                write!(f, "transform error on column '{}': {}", column, reason)
            }
        }
    }
}

impl std::error::Error for LensError {}

// Serialization helpers

fn serialize_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn deserialize_string(data: &[u8]) -> Result<(String, usize), LensError> {
    if data.len() < 4 {
        return Err(LensError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }
    let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return Err(LensError::DeserializationError(
            "unexpected end of data".into(),
        ));
    }
    let s = std::str::from_utf8(&data[4..4 + len])
        .map_err(|_| LensError::DeserializationError("invalid utf8".into()))?
        .to_string();
    Ok((s, 4 + len))
}

/// Mapping for tracking column renames during lens application.
/// Used in Phase 5 (Lens Application on Rows) for actual row transformations.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ColumnMapping {
    /// Maps source column name → target column name
    pub forward: HashMap<String, String>,
    /// Maps target column name → source column name
    pub backward: HashMap<String, String>,
}

#[allow(dead_code)]
impl ColumnMapping {
    /// Create an empty column mapping.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rename mapping.
    pub fn add_rename(&mut self, from: &str, to: &str) {
        self.forward.insert(from.to_string(), to.to_string());
        self.backward.insert(to.to_string(), from.to_string());
    }

    /// Get the target name for a source column.
    pub fn map_forward<'a>(&'a self, source: &'a str) -> &'a str {
        self.forward
            .get(source)
            .map(|s| s.as_str())
            .unwrap_or(source)
    }

    /// Get the source name for a target column.
    pub fn map_backward<'a>(&'a self, target: &'a str) -> &'a str {
        self.backward
            .get(target)
            .map(|s| s.as_str())
            .unwrap_or(target)
    }

    /// Build a mapping from a list of transforms.
    ///
    /// This properly chains renames: if we have a→b followed by b→c,
    /// the final mapping will be a→c.
    pub fn from_transforms(transforms: &[ColumnTransform]) -> Self {
        let mut mapping = Self::new();
        for transform in transforms {
            if let ColumnTransform::Rename { from, to } = transform {
                // Check if 'from' is already the target of a previous rename
                // If so, update that previous mapping to point to the new 'to'
                let original_from = mapping.backward.get(from).cloned();
                if let Some(original) = original_from {
                    // Chain: original → from → to becomes original → to
                    mapping.forward.insert(original.clone(), to.to_string());
                    mapping.backward.remove(from);
                    mapping.backward.insert(to.to_string(), original);
                } else {
                    // Direct mapping
                    mapping.forward.insert(from.to_string(), to.to_string());
                    mapping.backward.insert(to.to_string(), from.to_string());
                }
            }
        }
        mapping
    }
}

// =============================================================================
// Row Transformation Functions
// =============================================================================

/// Apply a list of transforms to a row, producing a new row.
///
/// This is the core transformation logic used by both forward and backward
/// lens application. The transforms are applied in order.
fn apply_transforms(
    row: RowRef<'_>,
    transforms: &[ColumnTransform],
) -> Result<OwnedRow, LensError> {
    // Build the target descriptor from source + transforms
    let target_descriptor = compute_target_descriptor(row.descriptor, transforms);
    let target_descriptor = Arc::new(target_descriptor);

    // Build column mapping for renames
    let mapping = ColumnMapping::from_transforms(transforms);

    // Track which columns should be removed
    let removed_columns: std::collections::HashSet<&str> = transforms
        .iter()
        .filter_map(|t| match t {
            ColumnTransform::Remove { name } => Some(name.as_str()),
            _ => None,
        })
        .collect();

    // Track added columns with their defaults
    let added_columns: HashMap<&str, Option<&DefaultValue>> = transforms
        .iter()
        .filter_map(|t| match t {
            ColumnTransform::Add { name, default } => Some((name.as_str(), default.as_ref())),
            _ => None,
        })
        .collect();

    // Track transform expressions (not yet implemented - will error)
    let transform_exprs: HashMap<&str, &SqlExpr> = transforms
        .iter()
        .filter_map(|t| match t {
            ColumnTransform::Transform { column, expr, .. } => Some((column.as_str(), expr)),
            _ => None,
        })
        .collect();

    // Build the target row
    let mut builder = RowBuilder::new(target_descriptor.clone());

    // Process each column in the source row
    for (col_idx, col) in row.descriptor.columns.iter().enumerate() {
        // Skip removed columns
        if removed_columns.contains(col.name.as_str()) {
            continue;
        }

        // Check if this column has a transform expression
        if let Some(expr) = transform_exprs.get(col.name.as_str()) {
            // SQL expression transforms not yet implemented
            // For now, return an error if the expression looks like a TODO
            if expr.text.contains("TODO") {
                return Err(LensError::EvaluationError {
                    column: col.name.clone(),
                    reason: format!("Transform expression not implemented: {}", expr.text),
                });
            }
            // For other expressions, we'd need to evaluate them
            // For now, just copy the original value (placeholder behavior)
        }

        // Get the value from source row
        if let Some(value) = row.get(col_idx) {
            // Determine target column name (may be renamed)
            let target_name = mapping.map_forward(&col.name);

            // Find target column index
            if let Some(target_idx) = target_descriptor.column_index(target_name) {
                builder = set_builder_value(builder, target_idx, value);
            }
        }
    }

    // Process added columns with their defaults
    for (col_name, default) in added_columns {
        if let Some(target_idx) = target_descriptor.column_index(col_name) {
            let col = &target_descriptor.columns[target_idx];

            match default {
                Some(DefaultValue::Null) => {
                    if col.nullable {
                        builder = builder.set_null(target_idx);
                    } else {
                        return Err(LensError::Incompatible {
                            reason: format!(
                                "Column '{}' is non-nullable but default is NULL",
                                col_name
                            ),
                        });
                    }
                }
                Some(DefaultValue::Bool(v)) => {
                    builder = builder.set_bool(target_idx, *v);
                }
                Some(DefaultValue::Int(v)) => {
                    // Try to set as appropriate integer type
                    match &col.ty {
                        ColumnType::I32 => builder = builder.set_i32(target_idx, *v as i32),
                        ColumnType::I64 => builder = builder.set_i64(target_idx, *v),
                        _ => {}
                    }
                }
                Some(DefaultValue::Float(v)) => {
                    builder = builder.set_f64(target_idx, *v);
                }
                Some(DefaultValue::String(v)) => {
                    builder = builder.set_string(target_idx, v);
                }
                Some(DefaultValue::Expr(expr)) => {
                    // SQL expression defaults not yet implemented
                    return Err(LensError::EvaluationError {
                        column: col_name.to_string(),
                        reason: format!("Default expression not implemented: {}", expr.text),
                    });
                }
                None => {
                    // No default - column must be nullable
                    if col.nullable {
                        builder = builder.set_null(target_idx);
                    } else {
                        return Err(LensError::Incompatible {
                            reason: format!(
                                "Column '{}' is non-nullable but has no default value",
                                col_name
                            ),
                        });
                    }
                }
            }
        }
    }

    Ok(builder.build())
}

/// Helper to set a builder value from a RowValue.
fn set_builder_value(builder: RowBuilder, idx: usize, value: RowValue<'_>) -> RowBuilder {
    match value {
        RowValue::Bool(v) => builder.set_bool(idx, v),
        RowValue::I32(v) => builder.set_i32(idx, v),
        RowValue::U32(v) => builder.set_u32(idx, v),
        RowValue::I64(v) => builder.set_i64(idx, v),
        RowValue::F64(v) => builder.set_f64(idx, v),
        RowValue::Ref(v) => builder.set_ref(idx, v),
        RowValue::String(v) => builder.set_string(idx, v),
        RowValue::Bytes(v) => builder.set_bytes(idx, v),
        RowValue::Null => builder.set_null(idx),
        RowValue::Blob(content_ref) => builder.set_blob(idx, content_ref),
        RowValue::BlobArray(refs) => builder.set_blob_array(idx, &refs),
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
    }
}

/// Compute the target descriptor after applying transforms to a source descriptor.
fn compute_target_descriptor(
    source: &RowDescriptor,
    transforms: &[ColumnTransform],
) -> RowDescriptor {
    let mapping = ColumnMapping::from_transforms(transforms);

    // Track which columns should be removed
    let removed_columns: std::collections::HashSet<&str> = transforms
        .iter()
        .filter_map(|t| match t {
            ColumnTransform::Remove { name } => Some(name.as_str()),
            _ => None,
        })
        .collect();

    // Start with source columns (renamed and excluding removed)
    let mut columns: Vec<(String, ColumnType, bool)> = source
        .columns
        .iter()
        .filter(|c| !removed_columns.contains(c.name.as_str()))
        .map(|c| {
            let name = mapping.map_forward(&c.name).to_string();
            (name, c.ty.clone(), c.nullable)
        })
        .collect();

    // Add new columns from Add transforms
    for transform in transforms {
        if let ColumnTransform::Add { name, default } = transform {
            // Determine type and nullability from default
            // For now, assume String type and nullable based on default presence
            let (ty, nullable) = match default {
                Some(DefaultValue::Null) => (ColumnType::String, true),
                Some(DefaultValue::Bool(_)) => (ColumnType::Bool, false),
                Some(DefaultValue::Int(_)) => (ColumnType::I64, false),
                Some(DefaultValue::Float(_)) => (ColumnType::F64, false),
                Some(DefaultValue::String(_)) => (ColumnType::String, false),
                Some(DefaultValue::Expr(_)) => (ColumnType::String, false), // Assume string for expressions
                None => (ColumnType::String, true), // No default means nullable
            };
            columns.push((name.clone(), ty, nullable));
        }
    }

    RowDescriptor::new(columns)
}

// =============================================================================
// Schema Diff and Lens Generation
// =============================================================================

use crate::sql::schema::{ColumnDef, TableSchema};

/// The result of diffing two schemas.
#[derive(Debug, Clone)]
pub struct SchemaDiff {
    /// Columns that exist in old but not in new (by name).
    pub removed: Vec<ColumnDef>,
    /// Columns that exist in new but not in old (by name).
    pub added: Vec<ColumnDef>,
    /// Columns that exist in both but have type changes.
    pub type_changes: Vec<TypeChange>,
    /// Potential renames detected (heuristic: same type, one removed + one added).
    pub potential_renames: Vec<PotentialRename>,
}

/// A detected type change for a column.
#[derive(Debug, Clone)]
pub struct TypeChange {
    pub column: String,
    pub old_type: ColumnType,
    pub new_type: ColumnType,
    pub old_nullable: bool,
    pub new_nullable: bool,
}

/// A potential rename detected by heuristics.
#[derive(Debug, Clone)]
pub struct PotentialRename {
    pub old_name: String,
    pub new_name: String,
    pub column_type: ColumnType,
    pub confidence: RenameConfidence,
}

/// Confidence level for rename detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameConfidence {
    /// Exact type match with only one candidate.
    High,
    /// Multiple candidates or other ambiguity.
    Medium,
}

impl SchemaDiff {
    /// Check if the schemas are identical.
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty()
            && self.added.is_empty()
            && self.type_changes.is_empty()
            && self.potential_renames.is_empty()
    }
}

/// Compute the diff between two table schemas.
///
/// This function compares columns by name and detects:
/// - Removed columns (in old, not in new)
/// - Added columns (in new, not in old)
/// - Type changes (same name, different type)
/// - Potential renames (heuristic: same type removed + added)
pub fn diff_schemas(old: &TableSchema, new: &TableSchema) -> SchemaDiff {
    let mut removed: Vec<ColumnDef> = Vec::new();
    let mut added: Vec<ColumnDef> = Vec::new();
    let mut type_changes: Vec<TypeChange> = Vec::new();

    // Build a map of new columns by name for quick lookup
    let new_columns: HashMap<&str, &ColumnDef> =
        new.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let old_columns: HashMap<&str, &ColumnDef> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    // Find removed columns and type changes
    for old_col in &old.columns {
        match new_columns.get(old_col.name.as_str()) {
            Some(new_col) => {
                // Column exists in both - check for type change
                if old_col.ty != new_col.ty || old_col.nullable != new_col.nullable {
                    type_changes.push(TypeChange {
                        column: old_col.name.clone(),
                        old_type: old_col.ty.clone(),
                        new_type: new_col.ty.clone(),
                        old_nullable: old_col.nullable,
                        new_nullable: new_col.nullable,
                    });
                }
            }
            None => {
                // Column removed
                removed.push(old_col.clone());
            }
        }
    }

    // Find added columns
    for new_col in &new.columns {
        if !old_columns.contains_key(new_col.name.as_str()) {
            added.push(new_col.clone());
        }
    }

    // Detect potential renames: match removed + added columns by type
    let potential_renames = detect_potential_renames(&removed, &added);

    SchemaDiff {
        removed,
        added,
        type_changes,
        potential_renames,
    }
}

/// Detect potential renames by matching removed and added columns by type.
///
/// TODO(GCO-1087): This heuristic can give false positives when columns of the same type
/// are removed and added (e.g., removing `created_at` and adding `updated_at`, both I64).
/// Consider adding name similarity heuristics or requiring explicit confirmation.
fn detect_potential_renames(removed: &[ColumnDef], added: &[ColumnDef]) -> Vec<PotentialRename> {
    let mut potential_renames = Vec::new();

    // Group added columns by type
    let mut added_by_type: HashMap<String, Vec<&ColumnDef>> = HashMap::new();
    for col in added {
        let type_key = format!("{:?}_{}", col.ty, col.nullable);
        added_by_type.entry(type_key).or_default().push(col);
    }

    // For each removed column, look for matching added columns
    for removed_col in removed {
        let type_key = format!("{:?}_{}", removed_col.ty, removed_col.nullable);
        if let Some(candidates) = added_by_type.get(&type_key) {
            let confidence = if candidates.len() == 1 {
                RenameConfidence::High
            } else {
                RenameConfidence::Medium
            };

            for added_col in candidates {
                potential_renames.push(PotentialRename {
                    old_name: removed_col.name.clone(),
                    new_name: added_col.name.clone(),
                    column_type: removed_col.ty.clone(),
                    confidence,
                });
            }
        }
    }

    potential_renames
}

/// Warnings generated during lens creation.
#[derive(Debug, Clone)]
pub struct LensWarning {
    pub kind: LensWarningKind,
    pub message: String,
}

/// Types of warnings that can occur during lens generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LensWarningKind {
    /// Adding a non-nullable column requires a default value.
    NonNullableAddRequiresDefault,
    /// Type change requires custom transform expression.
    TypeChangeRequiresTransform,
    /// Potential rename detected - user should confirm.
    PotentialRename,
    /// Removing column may cause data loss.
    ColumnRemovalWarning,
}

/// Result of lens generation.
#[derive(Debug, Clone)]
pub struct LensGenerationResult {
    /// The generated lens.
    pub lens: Lens,
    /// Warnings that may require user attention.
    pub warnings: Vec<LensWarning>,
}

/// Options for lens generation.
#[derive(Debug, Clone, Default)]
pub struct LensGenerationOptions {
    /// Confirmed renames: (old_name, new_name) pairs.
    /// Use this to confirm potential renames detected by `diff_schemas`.
    pub confirmed_renames: Vec<(String, String)>,
}

/// Generate a lens from a schema diff.
///
/// This creates a lens that can transform data from the old schema to the new schema
/// (forward) and from new to old (backward).
///
/// # Behavior
///
/// - **Renames**: If `confirmed_renames` is provided, those columns will use `Rename`
///   transforms. Otherwise, they appear as separate Add/Remove.
/// - **Nullable additions**: Automatically handled with NULL default.
/// - **Non-nullable additions**: Warning generated, forward uses NULL (row will be
///   incompatible), user should provide a default expression.
/// - **Removals**: Backward transform adds removed columns back with NULL.
/// - **Type changes**: Warning generated, placeholder transform created.
pub fn generate_lens(diff: &SchemaDiff, options: &LensGenerationOptions) -> LensGenerationResult {
    let mut forward: Vec<ColumnTransform> = Vec::new();
    let mut backward: Vec<ColumnTransform> = Vec::new();
    let mut warnings: Vec<LensWarning> = Vec::new();

    // Build set of confirmed renames for quick lookup
    let confirmed_renames: HashMap<&str, &str> = options
        .confirmed_renames
        .iter()
        .map(|(old, new)| (old.as_str(), new.as_str()))
        .collect();

    // Track which removed/added columns are part of renames
    let renamed_old: std::collections::HashSet<&str> = confirmed_renames.keys().copied().collect();
    let renamed_new: std::collections::HashSet<&str> =
        confirmed_renames.values().copied().collect();

    // Process confirmed renames
    for (old_name, new_name) in &options.confirmed_renames {
        forward.push(ColumnTransform::Rename {
            from: old_name.clone(),
            to: new_name.clone(),
        });
        backward.push(ColumnTransform::Rename {
            from: new_name.clone(),
            to: old_name.clone(),
        });
    }

    // Process removed columns (not part of renames)
    for col in &diff.removed {
        if renamed_old.contains(col.name.as_str()) {
            continue;
        }

        // Forward: Remove the column
        forward.push(ColumnTransform::Remove {
            name: col.name.clone(),
        });

        // Backward: Add the column back (with NULL if was nullable)
        backward.push(ColumnTransform::Add {
            name: col.name.clone(),
            default: if col.nullable {
                Some(DefaultValue::Null)
            } else {
                // Non-nullable column being added back - needs TODO
                warnings.push(LensWarning {
                    kind: LensWarningKind::ColumnRemovalWarning,
                    message: format!(
                        "Column '{}' was removed. Backward transform adds it with NULL, \
                         but it was non-nullable. Data from new schema will be incompatible \
                         with old clients unless a proper default is provided.",
                        col.name
                    ),
                });
                Some(DefaultValue::Null)
            },
        });
    }

    // Process added columns (not part of renames)
    for col in &diff.added {
        if renamed_new.contains(col.name.as_str()) {
            continue;
        }

        // Forward: Add the column
        if col.nullable {
            forward.push(ColumnTransform::Add {
                name: col.name.clone(),
                default: Some(DefaultValue::Null),
            });
        } else {
            // Non-nullable addition - requires default
            warnings.push(LensWarning {
                kind: LensWarningKind::NonNullableAddRequiresDefault,
                message: format!(
                    "Column '{}' is non-nullable but has no default value. \
                     Please provide a default expression. Using NULL for now (will fail validation).",
                    col.name
                ),
            });
            forward.push(ColumnTransform::Add {
                name: col.name.clone(),
                default: Some(DefaultValue::Null), // TODO marker
            });
        }

        // Backward: Remove the column
        backward.push(ColumnTransform::Remove {
            name: col.name.clone(),
        });
    }

    // Process type changes
    // TODO(GCO-1092): Currently just emits warnings and placeholder transforms.
    // Should auto-generate transforms for safe coercions (e.g., I64 -> String).
    for change in &diff.type_changes {
        warnings.push(LensWarning {
            kind: LensWarningKind::TypeChangeRequiresTransform,
            message: format!(
                "Column '{}' changed type from {:?} to {:?}. \
                 Please provide custom transform expressions.",
                change.column, change.old_type, change.new_type
            ),
        });

        // Placeholder transform that will need user editing
        forward.push(ColumnTransform::Transform {
            column: change.column.clone(),
            expr: SqlExpr {
                text: format!(
                    "/* TODO: convert {:?} to {:?} */",
                    change.old_type, change.new_type
                ),
            },
            reverse_expr: Some(SqlExpr {
                text: format!(
                    "/* TODO: convert {:?} to {:?} */",
                    change.new_type, change.old_type
                ),
            }),
        });

        backward.push(ColumnTransform::Transform {
            column: change.column.clone(),
            expr: SqlExpr {
                text: format!(
                    "/* TODO: convert {:?} to {:?} */",
                    change.new_type, change.old_type
                ),
            },
            reverse_expr: Some(SqlExpr {
                text: format!(
                    "/* TODO: convert {:?} to {:?} */",
                    change.old_type, change.new_type
                ),
            }),
        });
    }

    // Warn about potential renames that weren't confirmed
    for potential in &diff.potential_renames {
        if !confirmed_renames.contains_key(potential.old_name.as_str()) {
            warnings.push(LensWarning {
                kind: LensWarningKind::PotentialRename,
                message: format!(
                    "Detected potential rename: '{}' → '{}' (confidence: {:?}). \
                     Add to confirmed_renames if this is correct.",
                    potential.old_name, potential.new_name, potential.confidence
                ),
            });
        }
    }

    LensGenerationResult {
        lens: Lens { forward, backward },
        warnings,
    }
}

// =============================================================================
// Lens Context for Query Evaluation (Phase 7)
// =============================================================================

use crate::sql::catalog::DescriptorId;

/// A lens context holds lenses for transforming rows between schema versions
/// during query evaluation.
///
/// During sync, the server may need to evaluate queries against rows that are
/// stored in different schema versions. The LensContext provides a registry
/// of lenses that can be looked up by (source, target) descriptor pair.
///
/// # Example
///
/// ```ignore
/// // Create context with lenses from descriptor chain
/// let mut ctx = LensContext::new();
/// ctx.register_lens(old_descriptor_id, new_descriptor_id, lens);
///
/// // Transform a row for query evaluation
/// if let Some(transformed) = ctx.transform_row(&row, row_descriptor_id, target_descriptor_id) {
///     // Use transformed row for predicate evaluation
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct LensContext {
    /// Registry of lenses: (source_descriptor_id, target_descriptor_id) → Lens
    lenses: HashMap<(DescriptorId, DescriptorId), Lens>,
    /// Cached composed lenses for multi-hop transformations
    composed_cache: HashMap<(DescriptorId, DescriptorId), Lens>,
}

impl LensContext {
    /// Create an empty lens context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a lens for transforming from source to target descriptor.
    pub fn register_lens(&mut self, source: DescriptorId, target: DescriptorId, lens: Lens) {
        // Clear composed cache when adding new lenses
        self.composed_cache.clear();
        self.lenses.insert((source, target), lens);
    }

    /// Get the direct lens from source to target (if registered).
    pub fn get_lens(&self, source: &DescriptorId, target: &DescriptorId) -> Option<&Lens> {
        self.lenses.get(&(*source, *target))
    }

    /// Get the reverse lens from target to source.
    ///
    /// Uses the backward transforms of the forward lens.
    pub fn get_reverse_lens(&self, target: &DescriptorId, source: &DescriptorId) -> Option<Lens> {
        self.lenses.get(&(*source, *target)).map(|lens| {
            // Create reverse lens by swapping forward/backward
            Lens::new(lens.backward.clone(), lens.forward.clone())
        })
    }

    /// Transform a row from source schema to target schema.
    ///
    /// Returns None if the row is incompatible (lens cannot be applied).
    pub fn transform_row(
        &self,
        row: &OwnedRow,
        source: &DescriptorId,
        target: &DescriptorId,
    ) -> Result<OwnedRow, LensError> {
        if source == target {
            // Same schema version - no transformation needed
            return Ok(row.clone());
        }

        // Look up direct lens
        if let Some(lens) = self.get_lens(source, target) {
            return lens.apply_forward_owned(row);
        }

        // Try reverse lens
        if let Some(lens) = self.get_reverse_lens(source, target) {
            return lens.apply_forward_owned(row);
        }

        // TODO: Build composed lens for multi-hop transformations
        // For now, return error if no direct lens exists
        Err(LensError::Incompatible {
            reason: format!(
                "No lens found for transformation from {} to {}",
                source, target
            ),
        })
    }

    /// Check if a lens exists for the given source/target pair.
    pub fn has_lens(&self, source: &DescriptorId, target: &DescriptorId) -> bool {
        source == target
            || self.lenses.contains_key(&(*source, *target))
            || self.lenses.contains_key(&(*target, *source))
    }

    /// Get all registered lenses.
    pub fn all_lenses(&self) -> impl Iterator<Item = (&(DescriptorId, DescriptorId), &Lens)> {
        self.lenses.iter()
    }

    /// Number of registered lenses.
    pub fn len(&self) -> usize {
        self.lenses.len()
    }

    /// Whether the context is empty.
    pub fn is_empty(&self) -> bool {
        self.lenses.is_empty()
    }
}

/// A query context that includes lens information for cross-schema queries.
///
/// Used during query evaluation to know the target schema version and
/// available lenses for row transformation.
///
/// This is integrated into the query graph system:
/// - `QueryGraph` stores optional `target_descriptor` and `lens_context`
/// - `QueryNode::eval_filter_with_lens` transforms rows before predicate evaluation
/// - `QueryGraphBuilder::output_with_lens` creates graphs with lens context
/// - `GraphRegistry::set_lens_context` sets lens context on registered graphs
///
/// Note: Row-level descriptor tracking is not yet implemented. Currently, the
/// lens context is used when set, but rows are assumed to be at the target
/// descriptor version unless explicit tracking is added.
#[derive(Debug, Clone)]
pub struct QueryLensContext {
    /// The target schema version for this query.
    pub target_descriptor: DescriptorId,
    /// Available lenses for transformation.
    pub lenses: LensContext,
}

impl QueryLensContext {
    /// Create a new query lens context.
    pub fn new(target_descriptor: DescriptorId) -> Self {
        QueryLensContext {
            target_descriptor,
            lenses: LensContext::new(),
        }
    }

    /// Create from a target descriptor with the given lenses.
    pub fn with_lenses(target_descriptor: DescriptorId, lenses: LensContext) -> Self {
        QueryLensContext {
            target_descriptor,
            lenses,
        }
    }

    /// Transform a row to the target schema version.
    ///
    /// Returns None if the row is incompatible.
    pub fn transform_to_target(
        &self,
        row: &OwnedRow,
        source_descriptor: &DescriptorId,
    ) -> Result<OwnedRow, LensError> {
        self.lenses
            .transform_row(row, source_descriptor, &self.target_descriptor)
    }

    /// Check if a row at the given schema version can be transformed to target.
    pub fn can_transform(&self, source_descriptor: &DescriptorId) -> bool {
        self.lenses
            .has_lens(source_descriptor, &self.target_descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;

    #[test]
    fn test_rename_roundtrip() {
        let transform = ColumnTransform::rename("title", "name");
        let mut buf = Vec::new();
        transform.serialize(&mut buf);

        let (restored, _) = ColumnTransform::deserialize(&buf).unwrap();
        assert_eq!(transform, restored);
    }

    #[test]
    fn test_add_with_default_roundtrip() {
        let transform =
            ColumnTransform::add_with_default("status", DefaultValue::String("active".into()));
        let mut buf = Vec::new();
        transform.serialize(&mut buf);

        let (restored, _) = ColumnTransform::deserialize(&buf).unwrap();
        assert_eq!(transform, restored);
    }

    #[test]
    fn test_add_nullable_roundtrip() {
        let transform = ColumnTransform::add_nullable("optional_field");
        let mut buf = Vec::new();
        transform.serialize(&mut buf);

        let (restored, _) = ColumnTransform::deserialize(&buf).unwrap();
        assert_eq!(transform, restored);
    }

    #[test]
    fn test_remove_roundtrip() {
        let transform = ColumnTransform::remove("deprecated");
        let mut buf = Vec::new();
        transform.serialize(&mut buf);

        let (restored, _) = ColumnTransform::deserialize(&buf).unwrap();
        assert_eq!(transform, restored);
    }

    #[test]
    fn test_transform_roundtrip() {
        let transform = ColumnTransform::transform_bidirectional(
            "count",
            SqlExpr::new("$value * 100"),
            SqlExpr::new("$value / 100"),
        );
        let mut buf = Vec::new();
        transform.serialize(&mut buf);

        let (restored, _) = ColumnTransform::deserialize(&buf).unwrap();
        assert_eq!(transform, restored);
    }

    #[test]
    fn test_lens_roundtrip() {
        let lens = Lens::new(
            vec![
                ColumnTransform::rename("title", "name"),
                ColumnTransform::add_with_default("version", DefaultValue::Int(1)),
            ],
            vec![
                ColumnTransform::rename("name", "title"),
                ColumnTransform::remove("version"),
            ],
        );

        let bytes = lens.to_bytes();
        let (restored, _) = Lens::from_bytes(&bytes).unwrap();

        assert_eq!(lens, restored);
    }

    #[test]
    fn test_lens_from_forward() {
        let lens = Lens::from_forward(vec![ColumnTransform::rename("title", "name")]);

        assert_eq!(lens.forward.len(), 1);
        assert_eq!(lens.backward.len(), 1);

        // Check that backward is the inverse
        assert_eq!(lens.backward[0], ColumnTransform::rename("name", "title"));
    }

    #[test]
    fn test_rename_inversion() {
        let transform = ColumnTransform::rename("a", "b");
        let inverted = transform.invert().unwrap();
        assert_eq!(inverted, ColumnTransform::rename("b", "a"));
    }

    #[test]
    fn test_add_remove_inversion() {
        let add = ColumnTransform::add_nullable("field");
        let inverted = add.invert().unwrap();
        assert_eq!(inverted, ColumnTransform::remove("field"));

        let remove = ColumnTransform::remove("field");
        let inverted = remove.invert().unwrap();
        if let ColumnTransform::Add { name, default } = inverted {
            assert_eq!(name, "field");
            assert!(default.is_none());
        } else {
            panic!("expected Add transform");
        }
    }

    #[test]
    fn test_lens_compose() {
        let lens1 = Lens::from_forward(vec![ColumnTransform::rename("a", "b")]);
        let lens2 = Lens::from_forward(vec![ColumnTransform::rename("b", "c")]);

        let composed = lens1.compose(&lens2);

        assert_eq!(composed.forward.len(), 2);
        assert_eq!(composed.forward[0], ColumnTransform::rename("a", "b"));
        assert_eq!(composed.forward[1], ColumnTransform::rename("b", "c"));

        assert_eq!(composed.backward.len(), 2);
        assert_eq!(composed.backward[0], ColumnTransform::rename("c", "b"));
        assert_eq!(composed.backward[1], ColumnTransform::rename("b", "a"));
    }

    #[test]
    fn test_column_mapping() {
        let transforms = vec![
            ColumnTransform::rename("title", "name"),
            ColumnTransform::rename("desc", "description"),
        ];

        let mapping = ColumnMapping::from_transforms(&transforms);

        assert_eq!(mapping.map_forward("title"), "name");
        assert_eq!(mapping.map_forward("desc"), "description");
        assert_eq!(mapping.map_forward("other"), "other"); // unchanged

        assert_eq!(mapping.map_backward("name"), "title");
        assert_eq!(mapping.map_backward("description"), "desc");
        assert_eq!(mapping.map_backward("other"), "other"); // unchanged
    }

    #[test]
    fn test_default_values_roundtrip() {
        let defaults = vec![
            DefaultValue::Null,
            DefaultValue::Bool(true),
            DefaultValue::Bool(false),
            DefaultValue::Int(42),
            DefaultValue::Int(-100),
            DefaultValue::Float(1.5),
            DefaultValue::String("hello".into()),
            DefaultValue::Expr(SqlExpr::new("NOW()")),
        ];

        for default in defaults {
            let mut buf = Vec::new();
            default.serialize(&mut buf);
            let (restored, _) = DefaultValue::deserialize(&buf).unwrap();
            assert_eq!(default, restored);
        }
    }

    #[test]
    fn test_identity_lens() {
        let lens = Lens::identity();
        assert!(lens.is_identity());

        let non_identity = Lens::from_forward(vec![ColumnTransform::rename("a", "b")]);
        assert!(!non_identity.is_identity());
    }

    // =========================================================================
    // Schema Diff Tests
    // =========================================================================

    #[test]
    fn test_diff_identical_schemas() {
        let schema = TableSchema::new(
            "test",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&schema, &schema);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_column_added() {
        let old = TableSchema::new("test", vec![]);
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::optional("description", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].name, "description");
        assert!(diff.removed.is_empty());
        assert!(diff.type_changes.is_empty());
    }

    #[test]
    fn test_diff_column_removed() {
        let old = TableSchema::new(
            "test",
            vec![ColumnDef::optional("description", ColumnType::String)],
        );
        let new = TableSchema::new("test", vec![]);

        let diff = diff_schemas(&old, &new);
        assert!(diff.added.is_empty());
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].name, "description");
    }

    #[test]
    fn test_diff_column_renamed() {
        let old = TableSchema::new(
            "test",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        // Without confirmation, rename appears as remove + add
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].name, "title");
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].name, "name");
        // Potential rename should be detected
        assert!(!diff.potential_renames.is_empty());
        assert_eq!(diff.potential_renames[0].old_name, "title");
        assert_eq!(diff.potential_renames[0].new_name, "name");
        assert_eq!(diff.potential_renames[0].confidence, RenameConfidence::High);
    }

    #[test]
    fn test_diff_type_change() {
        let old = TableSchema::new("test", vec![ColumnDef::required("count", ColumnType::I32)]);
        let new = TableSchema::new("test", vec![ColumnDef::required("count", ColumnType::I64)]);

        let diff = diff_schemas(&old, &new);
        assert_eq!(diff.type_changes.len(), 1);
        assert_eq!(diff.type_changes[0].column, "count");
        assert_eq!(diff.type_changes[0].old_type, ColumnType::I32);
        assert_eq!(diff.type_changes[0].new_type, ColumnType::I64);
    }

    #[test]
    fn test_diff_nullable_change() {
        let old = TableSchema::new(
            "test",
            vec![ColumnDef::required("name", ColumnType::String)],
        );
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::optional("name", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        assert_eq!(diff.type_changes.len(), 1);
        assert_eq!(diff.type_changes[0].column, "name");
        assert!(!diff.type_changes[0].old_nullable);
        assert!(diff.type_changes[0].new_nullable);
    }

    // =========================================================================
    // Lens Generation Tests
    // =========================================================================

    #[test]
    fn test_generate_lens_rename() {
        let old = TableSchema::new(
            "test",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        let options = LensGenerationOptions {
            confirmed_renames: vec![("title".into(), "name".into())],
        };
        let result = generate_lens(&diff, &options);

        // Should have rename transform
        assert_eq!(result.lens.forward.len(), 1);
        assert!(matches!(
            &result.lens.forward[0],
            ColumnTransform::Rename { from, to } if from == "title" && to == "name"
        ));
        assert_eq!(result.lens.backward.len(), 1);
        assert!(matches!(
            &result.lens.backward[0],
            ColumnTransform::Rename { from, to } if from == "name" && to == "title"
        ));

        // No warnings for confirmed rename
        let rename_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.kind == LensWarningKind::PotentialRename)
            .collect();
        assert!(rename_warnings.is_empty());
    }

    #[test]
    fn test_generate_lens_unconfirmed_rename_warning() {
        let old = TableSchema::new(
            "test",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        let result = generate_lens(&diff, &LensGenerationOptions::default());

        // Without confirmation, should be add + remove
        let has_remove = result
            .lens
            .forward
            .iter()
            .any(|t| matches!(t, ColumnTransform::Remove { name } if name == "title"));
        let has_add = result
            .lens
            .forward
            .iter()
            .any(|t| matches!(t, ColumnTransform::Add { name, .. } if name == "name"));
        assert!(has_remove);
        assert!(has_add);

        // Should have warning about potential rename
        let rename_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.kind == LensWarningKind::PotentialRename)
            .collect();
        assert!(!rename_warnings.is_empty());
    }

    #[test]
    fn test_generate_lens_nullable_add() {
        let old = TableSchema::new("test", vec![]);
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::optional("description", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        let result = generate_lens(&diff, &LensGenerationOptions::default());

        // Forward should add with NULL
        assert!(result.lens.forward.iter().any(|t| {
            matches!(
                t,
                ColumnTransform::Add { name, default: Some(DefaultValue::Null) }
                if name == "description"
            )
        }));

        // Backward should remove
        assert!(
            result.lens.backward.iter().any(|t| {
                matches!(t, ColumnTransform::Remove { name } if name == "description")
            })
        );

        // No warnings for nullable column
        assert!(
            result
                .warnings
                .iter()
                .all(|w| w.kind != LensWarningKind::NonNullableAddRequiresDefault)
        );
    }

    #[test]
    fn test_generate_lens_non_nullable_add_warning() {
        let old = TableSchema::new("test", vec![]);
        let new = TableSchema::new(
            "test",
            vec![ColumnDef::required("required_field", ColumnType::String)],
        );

        let diff = diff_schemas(&old, &new);
        let result = generate_lens(&diff, &LensGenerationOptions::default());

        // Should have warning about non-nullable add
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.kind == LensWarningKind::NonNullableAddRequiresDefault)
        );
    }

    #[test]
    fn test_generate_lens_type_change_warning() {
        let old = TableSchema::new("test", vec![ColumnDef::required("count", ColumnType::I32)]);
        let new = TableSchema::new("test", vec![ColumnDef::required("count", ColumnType::I64)]);

        let diff = diff_schemas(&old, &new);
        let result = generate_lens(&diff, &LensGenerationOptions::default());

        // Should have warning about type change
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.kind == LensWarningKind::TypeChangeRequiresTransform)
        );

        // Should have Transform placeholders
        assert!(result.lens.forward.iter().any(|t| {
            matches!(t, ColumnTransform::Transform { column, .. } if column == "count")
        }));
    }

    #[test]
    fn test_generate_lens_demo_scenario() {
        // Demo scenario: rename column from `title` to `name`
        let v1 = TableSchema::new(
            "documents",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let v2 = TableSchema::new(
            "documents",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&v1, &v2);
        let options = LensGenerationOptions {
            confirmed_renames: vec![("title".into(), "name".into())],
        };
        let result = generate_lens(&diff, &options);

        // Forward: title → name
        assert_eq!(result.lens.forward.len(), 1);
        match &result.lens.forward[0] {
            ColumnTransform::Rename { from, to } => {
                assert_eq!(from, "title");
                assert_eq!(to, "name");
            }
            _ => panic!("Expected Rename transform"),
        }

        // Backward: name → title
        assert_eq!(result.lens.backward.len(), 1);
        match &result.lens.backward[0] {
            ColumnTransform::Rename { from, to } => {
                assert_eq!(from, "name");
                assert_eq!(to, "title");
            }
            _ => panic!("Expected Rename transform"),
        }

        // No warnings
        assert!(result.warnings.is_empty());
    }

    // =========================================================================
    // Lens Application Tests (Phase 5)
    // =========================================================================

    #[test]
    fn test_apply_rename_forward() {
        // Create a row with 'title' column
        let source_desc = Arc::new(RowDescriptor::new([(
            "title".to_string(),
            ColumnType::String,
            false,
        )]));

        let title_idx = source_desc.column_index("title").unwrap();
        let row = RowBuilder::new(source_desc.clone())
            .set_string(title_idx, "Hello World")
            .build();

        // Create lens that renames title → name
        let lens = Lens::from_forward(vec![ColumnTransform::rename("title", "name")]);

        // Apply forward transformation
        let result = lens.apply_forward_owned(&row).unwrap();

        // Should have 'name' column with the value
        assert_eq!(
            result.get_by_name("name"),
            Some(RowValue::String("Hello World"))
        );
        // 'title' should not exist in target
        assert_eq!(result.get_by_name("title"), None);
    }

    #[test]
    fn test_apply_rename_backward() {
        // Create a row with 'name' column
        let source_desc = Arc::new(RowDescriptor::new([(
            "name".to_string(),
            ColumnType::String,
            false,
        )]));

        let name_idx = source_desc.column_index("name").unwrap();
        let row = RowBuilder::new(source_desc.clone())
            .set_string(name_idx, "Hello World")
            .build();

        // Create lens that renames title → name (backward: name → title)
        let lens = Lens::from_forward(vec![ColumnTransform::rename("title", "name")]);

        // Apply backward transformation
        let result = lens.apply_backward_owned(&row).unwrap();

        // Should have 'title' column with the value
        assert_eq!(
            result.get_by_name("title"),
            Some(RowValue::String("Hello World"))
        );
    }

    #[test]
    fn test_apply_add_column_with_default() {
        // Create a row with just 'id' column
        let source_desc = Arc::new(RowDescriptor::new([(
            "id".to_string(),
            ColumnType::I32,
            false,
        )]));

        let id_idx = source_desc.column_index("id").unwrap();
        let row = RowBuilder::new(source_desc.clone())
            .set_i32(id_idx, 42)
            .build();

        // Create lens that adds 'status' with default value
        let lens = Lens::new(
            vec![ColumnTransform::add_with_default(
                "status",
                DefaultValue::String("active".into()),
            )],
            vec![ColumnTransform::remove("status")],
        );

        // Apply forward transformation
        let result = lens.apply_forward_owned(&row).unwrap();

        // Should have both 'id' and 'status'
        assert_eq!(result.get_by_name("id"), Some(RowValue::I32(42)));
        assert_eq!(
            result.get_by_name("status"),
            Some(RowValue::String("active"))
        );
    }

    #[test]
    fn test_apply_remove_column() {
        // Create a row with 'id' and 'deprecated' columns
        let source_desc = Arc::new(RowDescriptor::new([
            ("id".to_string(), ColumnType::I32, false),
            ("deprecated".to_string(), ColumnType::String, false),
        ]));

        let id_idx = source_desc.column_index("id").unwrap();
        let deprecated_idx = source_desc.column_index("deprecated").unwrap();
        let row = RowBuilder::new(source_desc.clone())
            .set_i32(id_idx, 42)
            .set_string(deprecated_idx, "old_value")
            .build();

        // Create lens that removes 'deprecated'
        let lens = Lens::new(
            vec![ColumnTransform::remove("deprecated")],
            vec![ColumnTransform::add_nullable("deprecated")],
        );

        // Apply forward transformation
        let result = lens.apply_forward_owned(&row).unwrap();

        // Should have only 'id'
        assert_eq!(result.get_by_name("id"), Some(RowValue::I32(42)));
        assert_eq!(result.get_by_name("deprecated"), None);
    }

    #[test]
    fn test_apply_nullable_add_without_default() {
        // Create a row with just 'id' column
        let source_desc = Arc::new(RowDescriptor::new([(
            "id".to_string(),
            ColumnType::I32,
            false,
        )]));

        let id_idx = source_desc.column_index("id").unwrap();
        let row = RowBuilder::new(source_desc.clone())
            .set_i32(id_idx, 42)
            .build();

        // Create lens that adds nullable 'optional' without default
        let lens = Lens::new(
            vec![ColumnTransform::add_nullable("optional")],
            vec![ColumnTransform::remove("optional")],
        );

        // Apply forward transformation - should succeed (nullable gets NULL)
        let result = lens.apply_forward_owned(&row).unwrap();

        // Should have 'id' and 'optional' (NULL)
        assert_eq!(result.get_by_name("id"), Some(RowValue::I32(42)));
        assert_eq!(result.get_by_name("optional"), Some(RowValue::Null));
    }

    #[test]
    fn test_target_descriptor() {
        let source_desc = RowDescriptor::new([
            ("title".to_string(), ColumnType::String, false),
            ("count".to_string(), ColumnType::I32, false),
        ]);

        let lens = Lens::from_forward(vec![
            ColumnTransform::rename("title", "name"),
            ColumnTransform::remove("count"),
            ColumnTransform::add_with_default("status", DefaultValue::String("active".into())),
        ]);

        let target_desc = lens.target_descriptor(&source_desc);

        // Should have 'name' (renamed from title) and 'status' (added)
        assert!(target_desc.column("name").is_some());
        assert!(target_desc.column("status").is_some());
        // Should not have 'title' (renamed) or 'count' (removed)
        assert!(target_desc.column("title").is_none());
        assert!(target_desc.column("count").is_none());
    }

    #[test]
    fn test_demo_scenario_bidirectional() {
        // Demo scenario: rename `title` → `name` with bidirectional transforms

        // V1 row with title
        let v1_desc = Arc::new(RowDescriptor::new([
            ("id".to_string(), ColumnType::I32, false),
            ("title".to_string(), ColumnType::String, false),
        ]));

        let id_idx = v1_desc.column_index("id").unwrap();
        let title_idx = v1_desc.column_index("title").unwrap();
        let v1_row = RowBuilder::new(v1_desc.clone())
            .set_i32(id_idx, 1)
            .set_string(title_idx, "My Document")
            .build();

        // Generate lens from schema diff
        let v1_schema = TableSchema::new(
            "documents",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let v2_schema = TableSchema::new(
            "documents",
            vec![ColumnDef::required("name", ColumnType::String)],
        );

        let diff = diff_schemas(&v1_schema, &v2_schema);
        let options = LensGenerationOptions {
            confirmed_renames: vec![("title".into(), "name".into())],
        };
        let result = generate_lens(&diff, &options);
        let lens = result.lens;

        // Forward: v1 → v2 (title → name)
        let v2_row = lens.apply_forward_owned(&v1_row).unwrap();
        assert_eq!(v2_row.get_by_name("id"), Some(RowValue::I32(1)));
        assert_eq!(
            v2_row.get_by_name("name"),
            Some(RowValue::String("My Document"))
        );
        assert_eq!(v2_row.get_by_name("title"), None);

        // Backward: v2 → v1 (name → title)
        let v1_restored = lens.apply_backward_owned(&v2_row).unwrap();
        assert_eq!(v1_restored.get_by_name("id"), Some(RowValue::I32(1)));
        assert_eq!(
            v1_restored.get_by_name("title"),
            Some(RowValue::String("My Document"))
        );
        assert_eq!(v1_restored.get_by_name("name"), None);
    }

    #[test]
    fn test_compose_and_apply() {
        // Test composing lenses: a → b → c
        let lens1 = Lens::from_forward(vec![ColumnTransform::rename("a", "b")]);
        let lens2 = Lens::from_forward(vec![ColumnTransform::rename("b", "c")]);
        let composed = lens1.compose(&lens2);

        // Create row with 'a'
        let source_desc = Arc::new(RowDescriptor::new([(
            "a".to_string(),
            ColumnType::String,
            false,
        )]));
        let a_idx = source_desc.column_index("a").unwrap();
        let row = RowBuilder::new(source_desc)
            .set_string(a_idx, "value")
            .build();

        // Apply composed lens
        let result = composed.apply_forward_owned(&row).unwrap();

        // Should have 'c' (not 'a' or 'b')
        assert_eq!(result.get_by_name("c"), Some(RowValue::String("value")));
        assert_eq!(result.get_by_name("a"), None);
        assert_eq!(result.get_by_name("b"), None);
    }

    // =========================================================================
    // Lens Context Tests (Phase 7)
    // =========================================================================

    #[test]
    fn test_lens_context_register_and_get() {
        let mut ctx = LensContext::new();

        // Create descriptor IDs
        let id1 = DescriptorId::from_object_id(ObjectId::new(1));
        let id2 = DescriptorId::from_object_id(ObjectId::new(2));

        // Register a lens
        let lens = Lens::from_forward(vec![ColumnTransform::rename("a", "b")]);
        ctx.register_lens(id1, id2, lens.clone());

        // Should be able to get it
        assert!(ctx.get_lens(&id1, &id2).is_some());
        assert!(ctx.has_lens(&id1, &id2));

        // Reverse should also work
        assert!(ctx.has_lens(&id2, &id1));
    }

    #[test]
    fn test_lens_context_transform_row() {
        let mut ctx = LensContext::new();

        // Create descriptor IDs
        let id_v1 = DescriptorId::from_object_id(ObjectId::new(1));
        let id_v2 = DescriptorId::from_object_id(ObjectId::new(2));

        // Register rename lens (title → name)
        let lens = Lens::from_forward(vec![ColumnTransform::rename("title", "name")]);
        ctx.register_lens(id_v1, id_v2, lens);

        // Create a v1 row with 'title'
        let v1_desc = Arc::new(RowDescriptor::new([(
            "title".to_string(),
            ColumnType::String,
            false,
        )]));
        let row = RowBuilder::new(v1_desc)
            .set_string_by_name("title", "Hello")
            .build();

        // Transform to v2
        let result = ctx.transform_row(&row, &id_v1, &id_v2).unwrap();
        assert_eq!(result.get_by_name("name"), Some(RowValue::String("Hello")));
        assert_eq!(result.get_by_name("title"), None);
    }

    #[test]
    fn test_lens_context_transform_same_version() {
        let ctx = LensContext::new();

        let id = DescriptorId::from_object_id(ObjectId::new(1));

        // Create a row
        let desc = Arc::new(RowDescriptor::new([(
            "name".to_string(),
            ColumnType::String,
            false,
        )]));
        let row = RowBuilder::new(desc)
            .set_string_by_name("name", "Test")
            .build();

        // Transform to same version - should return clone
        let result = ctx.transform_row(&row, &id, &id).unwrap();
        assert_eq!(result.get_by_name("name"), Some(RowValue::String("Test")));
    }

    #[test]
    fn test_lens_context_reverse_transform() {
        let mut ctx = LensContext::new();

        // Create descriptor IDs
        let id_v1 = DescriptorId::from_object_id(ObjectId::new(1));
        let id_v2 = DescriptorId::from_object_id(ObjectId::new(2));

        // Register rename lens (title → name), only v1→v2
        let lens = Lens::from_forward(vec![ColumnTransform::rename("title", "name")]);
        ctx.register_lens(id_v1, id_v2, lens);

        // Create a v2 row with 'name'
        let v2_desc = Arc::new(RowDescriptor::new([(
            "name".to_string(),
            ColumnType::String,
            false,
        )]));
        let row = RowBuilder::new(v2_desc)
            .set_string_by_name("name", "Hello")
            .build();

        // Transform back to v1 (using reverse)
        let result = ctx.transform_row(&row, &id_v2, &id_v1).unwrap();
        assert_eq!(result.get_by_name("title"), Some(RowValue::String("Hello")));
        assert_eq!(result.get_by_name("name"), None);
    }

    #[test]
    fn test_query_lens_context() {
        let mut lenses = LensContext::new();

        // Create descriptor IDs
        let id_v1 = DescriptorId::from_object_id(ObjectId::new(1));
        let id_v2 = DescriptorId::from_object_id(ObjectId::new(2));

        // Register lens
        let lens = Lens::from_forward(vec![ColumnTransform::rename("old", "new")]);
        lenses.register_lens(id_v1, id_v2, lens);

        // Create query context targeting v2
        let query_ctx = QueryLensContext::with_lenses(id_v2, lenses);

        // Should be able to transform v1 rows to v2
        assert!(query_ctx.can_transform(&id_v1));
        assert!(query_ctx.can_transform(&id_v2)); // Same version

        // Create a v1 row
        let v1_desc = Arc::new(RowDescriptor::new([(
            "old".to_string(),
            ColumnType::String,
            false,
        )]));
        let row = RowBuilder::new(v1_desc)
            .set_string_by_name("old", "value")
            .build();

        // Transform to target
        let result = query_ctx.transform_to_target(&row, &id_v1).unwrap();
        assert_eq!(result.get_by_name("new"), Some(RowValue::String("value")));
    }
}
