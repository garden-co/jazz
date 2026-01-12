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
    pub fn from_transforms(transforms: &[ColumnTransform]) -> Self {
        let mut mapping = Self::new();
        for transform in transforms {
            if let ColumnTransform::Rename { from, to } = transform {
                mapping.add_rename(from, to);
            }
        }
        mapping
    }
}

// =============================================================================
// Schema Diff and Lens Generation
// =============================================================================

use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
