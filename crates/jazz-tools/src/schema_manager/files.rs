//! File convention API for schema and migration files.
//!
//! This module provides a high-level API for working with schema directories,
//! following the convention:
//!
//! ```text
//! schema/
//! ├── current.sql                                          # Editable source of truth
//! ├── schema_v1_455a1f10a158.sql                           # v1 with hash
//! ├── schema_v2_add_description_357c464c4c43.sql           # Optional description before hash
//! ├── schema_v3_abc123def456.sql                           # v3
//! ├── migration_v1_v2_455a1f10a158_357c464c4c43_fwd.sql    # v1 → v2 forward
//! └── migration_v1_v2_455a1f10a158_357c464c4c43_bwd.sql    # v1 → v2 backward
//! ```
//!
//! **Naming rules:**
//! - Schema: `schema_vN_{description}_{hash}.sql` where description is optional
//! - Migration: `migration_vA_vB_{hashA}_{hashB}_{fwd|bwd}.sql`
//! - Hash is always the last component before `.sql` or `_fwd`/`_bwd`

use std::fs;
use std::path::{Path, PathBuf};

use crate::query_manager::types::{ColumnType, Schema, Value};

use super::lens::{Direction, LensOp, LensTransform};
use super::sql::{
    SqlParseError, column_type_to_sql, lens_to_sql, parse_lens, parse_schema, schema_to_sql,
};

/// Errors that can occur during file operations.
#[derive(Debug)]
pub enum FileError {
    /// I/O error reading/writing files.
    Io(std::io::Error),
    /// SQL parsing error.
    Parse(SqlParseError),
    /// File not found.
    NotFound(String),
    /// Invalid filename format.
    InvalidFilename(String),
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileError::Io(e) => write!(f, "I/O error: {}", e),
            FileError::Parse(e) => write!(f, "Parse error: {}", e),
            FileError::NotFound(path) => write!(f, "File not found: {}", path),
            FileError::InvalidFilename(name) => write!(f, "Invalid filename: {}", name),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileError::Io(e) => Some(e),
            FileError::Parse(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FileError {
    fn from(e: std::io::Error) -> Self {
        FileError::Io(e)
    }
}

impl From<SqlParseError> for FileError {
    fn from(e: SqlParseError) -> Self {
        FileError::Parse(e)
    }
}

/// Information parsed from a versioned schema filename.
///
/// Valid format: `schema_vN_{description}_{hash}.sql` where description is optional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaFileInfo {
    /// Version number (1, 2, 3, ...)
    pub version: u32,
    /// Optional description (e.g., "add_description")
    pub description: Option<String>,
    /// 12-char hex hash from filename
    pub hash: String,
}

/// Information parsed from a migration filename.
///
/// Valid format: `migration_vA_vB_{hashA}_{hashB}_{fwd|bwd}.sql`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationFileInfo {
    /// Source version number
    pub from_version: u32,
    /// Target version number
    pub to_version: u32,
    /// 12-char hex hash of source schema
    pub from_hash: String,
    /// 12-char hex hash of target schema
    pub to_hash: String,
    /// Direction (Some for SQL files, None for .ts stubs)
    pub direction: Option<Direction>,
}

/// A schema directory following the file convention.
#[derive(Debug, Clone)]
pub struct SchemaDirectory {
    path: PathBuf,
}

impl SchemaDirectory {
    /// Create a new SchemaDirectory for the given path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Get the directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read and parse current.sql.
    pub fn current_schema(&self) -> Result<Schema, FileError> {
        let path = self.path.join("current.sql");
        if !path.exists() {
            return Err(FileError::NotFound(path.display().to_string()));
        }
        let content = fs::read_to_string(&path)?;
        let schema = parse_schema(&content)?;
        Ok(schema)
    }

    /// List all frozen schema versions in order (by version number).
    ///
    /// Scans for files matching `schema_v*_*.sql` pattern.
    /// Returns schema info sorted by version number.
    pub fn schema_versions(&self) -> Result<Vec<SchemaFileInfo>, FileError> {
        let mut versions: Vec<SchemaFileInfo> = Vec::new();

        if !self.path.exists() {
            return Ok(Vec::new());
        }

        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();

            if let Some(info) = parse_versioned_schema_filename(&name) {
                versions.push(info);
            }
        }

        // Sort by version number
        versions.sort_by_key(|v| v.version);

        Ok(versions)
    }

    /// Get the latest schema version info, if any.
    pub fn latest_version(&self) -> Result<Option<SchemaFileInfo>, FileError> {
        let versions = self.schema_versions()?;
        Ok(versions.into_iter().last())
    }

    /// Read a specific frozen schema by version number.
    pub fn schema_by_version(&self, version: u32) -> Result<Schema, FileError> {
        let versions = self.schema_versions()?;
        let info = versions
            .into_iter()
            .find(|v| v.version == version)
            .ok_or_else(|| FileError::NotFound(format!("schema version {}", version)))?;

        self.schema_by_info(&info)
    }

    /// Read a specific frozen schema by its file info.
    pub fn schema_by_info(&self, info: &SchemaFileInfo) -> Result<Schema, FileError> {
        let filename = schema_filename(info);
        let path = self.path.join(&filename);

        if !path.exists() {
            return Err(FileError::NotFound(path.display().to_string()));
        }

        let content = fs::read_to_string(&path)?;
        let schema = parse_schema(&content)?;
        Ok(schema)
    }

    /// Read schema SQL content by version (for hash validation).
    pub fn schema_sql_by_version(&self, version: u32) -> Result<String, FileError> {
        let versions = self.schema_versions()?;
        let info = versions
            .into_iter()
            .find(|v| v.version == version)
            .ok_or_else(|| FileError::NotFound(format!("schema version {}", version)))?;

        let filename = schema_filename(&info);
        let path = self.path.join(&filename);
        Ok(fs::read_to_string(&path)?)
    }

    /// Read a migration between two versions.
    pub fn migration(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
        direction: Direction,
    ) -> Result<LensTransform, FileError> {
        let filename =
            migration_sql_filename(from_version, to_version, from_hash, to_hash, direction);
        let path = self.path.join(&filename);

        if !path.exists() {
            return Err(FileError::NotFound(path.display().to_string()));
        }

        let content = fs::read_to_string(&path)?;
        let transform = parse_lens(&content)?;
        Ok(transform)
    }

    /// Check if a migration SQL file exists between two versions.
    pub fn has_migration_sql(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
        direction: Direction,
    ) -> bool {
        let filename =
            migration_sql_filename(from_version, to_version, from_hash, to_hash, direction);
        self.path.join(&filename).exists()
    }

    /// Check if a migration TypeScript stub exists between two versions.
    pub fn has_migration_ts_stub(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
    ) -> bool {
        let filename = migration_ts_filename(from_version, to_version, from_hash, to_hash);
        self.path.join(&filename).exists()
    }

    /// Write a frozen schema file with version number.
    pub fn write_schema(
        &self,
        schema: &Schema,
        version: u32,
        description: Option<&str>,
        hash: &str,
    ) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let info = SchemaFileInfo {
            version,
            description: description.map(String::from),
            hash: hash.to_string(),
        };
        let filename = schema_filename(&info);
        let path = self.path.join(&filename);
        let content = schema_to_sql(schema);

        fs::write(&path, content)?;
        Ok(path)
    }

    /// Write a migration SQL file.
    pub fn write_migration_sql(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
        direction: Direction,
        transform: &LensTransform,
    ) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let filename =
            migration_sql_filename(from_version, to_version, from_hash, to_hash, direction);
        let path = self.path.join(&filename);
        let content = lens_to_sql(transform);

        fs::write(&path, content)?;
        Ok(path)
    }

    /// Write both forward and backward migration SQL files.
    pub fn write_migration_sql_pair(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
        forward: &LensTransform,
    ) -> Result<(PathBuf, PathBuf), FileError> {
        let backward = forward.invert();

        let fwd_path = self.write_migration_sql(
            from_version,
            to_version,
            from_hash,
            to_hash,
            Direction::Forward,
            forward,
        )?;
        let bwd_path = self.write_migration_sql(
            from_version,
            to_version,
            from_hash,
            to_hash,
            Direction::Backward,
            &backward,
        )?;

        Ok((fwd_path, bwd_path))
    }

    /// Write current.sql from a schema.
    pub fn write_current(&self, schema: &Schema) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let path = self.path.join("current.sql");
        let content = schema_to_sql(schema);

        fs::write(&path, content)?;
        Ok(path)
    }

    /// Check if current.sql exists.
    pub fn has_current(&self) -> bool {
        self.path.join("current.sql").exists()
    }

    /// Check if a schema version exists by hash.
    pub fn has_schema_with_hash(&self, hash: &str) -> bool {
        self.schema_versions()
            .map(|versions| versions.iter().any(|v| v.hash == hash))
            .unwrap_or(false)
    }

    /// Write a TypeScript migration stub file.
    ///
    /// The stub contains the inferred migration operations that the user should review/modify.
    pub fn write_migration_ts_stub(
        &self,
        from_version: u32,
        to_version: u32,
        from_hash: &str,
        to_hash: &str,
        transform: &LensTransform,
    ) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let filename = migration_ts_filename(from_version, to_version, from_hash, to_hash);
        let path = self.path.join(&filename);
        let content = lens_transform_to_ts(transform);

        fs::write(&path, content)?;
        Ok(path)
    }
}

/// Parse a versioned schema filename and extract info.
///
/// Valid formats:
/// - `schema_v1_455a1f10a158.sql` → version=1, description=None, hash="455a1f10a158"
/// - `schema_v2_add_description_357c464c4c43.sql` → version=2, description=Some("add_description"), hash="357c464c4c43"
pub fn parse_versioned_schema_filename(name: &str) -> Option<SchemaFileInfo> {
    if !name.starts_with("schema_v") || !name.ends_with(".sql") {
        return None;
    }

    // Remove "schema_v" prefix and ".sql" suffix
    let inner = &name[8..name.len() - 4];

    // Split by underscore
    let parts: Vec<&str> = inner.split('_').collect();
    if parts.is_empty() {
        return None;
    }

    // First part is the version number
    let version: u32 = parts[0].parse().ok()?;

    // Last part is the hash (12 hex chars)
    let hash = parts.last()?;
    if hash.len() != 12 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    // Middle parts (if any) are the description
    let description = if parts.len() > 2 {
        Some(parts[1..parts.len() - 1].join("_"))
    } else {
        None
    };

    Some(SchemaFileInfo {
        version,
        description,
        hash: hash.to_string(),
    })
}

/// Parse a migration filename and extract info.
///
/// Valid formats:
/// - `migration_v1_v2_455a1f10a158_357c464c4c43.ts` → TypeScript stub (direction=None)
/// - `migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql` → Forward migration
/// - `migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql` → Backward migration
pub fn parse_migration_filename(name: &str) -> Option<MigrationFileInfo> {
    if !name.starts_with("migration_") {
        return None;
    }

    let is_ts = name.ends_with(".ts");
    let is_sql = name.ends_with(".sql");

    if !is_ts && !is_sql {
        return None;
    }

    // Remove prefix and suffix
    let inner = if is_ts {
        &name[10..name.len() - 3] // Remove "migration_" and ".ts"
    } else {
        &name[10..name.len() - 4] // Remove "migration_" and ".sql"
    };

    let parts: Vec<&str> = inner.split('_').collect();

    // For .ts: v1_v2_hash1_hash2 (4 parts)
    // For .sql: v1_v2_fwd/bwd_hash1_hash2 (5 parts)
    let (expected_parts, direction, hash_start_idx) = if is_ts {
        (4, None, 2)
    } else {
        if parts.len() < 3 {
            return None;
        }
        let dir = match parts[2] {
            "fwd" => Direction::Forward,
            "bwd" => Direction::Backward,
            _ => return None,
        };
        (5, Some(dir), 3)
    };

    if parts.len() != expected_parts {
        return None;
    }

    // Parse version numbers (v1, v2 format)
    if !parts[0].starts_with('v') {
        return None;
    }
    let from_version: u32 = parts[0][1..].parse().ok()?;

    if !parts[1].starts_with('v') {
        return None;
    }
    let to_version: u32 = parts[1][1..].parse().ok()?;

    // Validate hashes (12 hex chars each)
    let from_hash = parts[hash_start_idx];
    let to_hash = parts[hash_start_idx + 1];

    if from_hash.len() != 12 || !from_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if to_hash.len() != 12 || !to_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Some(MigrationFileInfo {
        from_version,
        to_version,
        from_hash: from_hash.to_string(),
        to_hash: to_hash.to_string(),
        direction,
    })
}

/// Generate a schema filename from info.
///
/// Examples:
/// - `schema_v1_455a1f10a158.sql`
/// - `schema_v2_add_description_357c464c4c43.sql`
pub fn schema_filename(info: &SchemaFileInfo) -> String {
    match &info.description {
        Some(desc) => format!("schema_v{}_{}_{}.sql", info.version, desc, info.hash),
        None => format!("schema_v{}_{}.sql", info.version, info.hash),
    }
}

/// Generate a migration TypeScript stub filename.
///
/// Example: `migration_v1_v2_455a1f10a158_357c464c4c43.ts`
pub fn migration_ts_filename(
    from_version: u32,
    to_version: u32,
    from_hash: &str,
    to_hash: &str,
) -> String {
    format!(
        "migration_v{}_v{}_{}_{}.ts",
        from_version, to_version, from_hash, to_hash
    )
}

/// Generate a migration SQL filename.
///
/// Examples:
/// - `migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql`
/// - `migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql`
///
/// Direction comes before hashes so truncated filenames still show useful info.
pub fn migration_sql_filename(
    from_version: u32,
    to_version: u32,
    from_hash: &str,
    to_hash: &str,
    direction: Direction,
) -> String {
    let dir = match direction {
        Direction::Forward => "fwd",
        Direction::Backward => "bwd",
    };
    format!(
        "migration_v{}_v{}_{}_{}_{}.sql",
        from_version, to_version, dir, from_hash, to_hash
    )
}

/// Convert a Value to its TypeScript literal representation.
fn value_to_ts_literal(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Real(f) => {
            assert!(
                f.is_finite(),
                "non-finite float in value_to_ts_literal: {f}"
            );
            format!("{f:?}")
        }
        Value::Text(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Timestamp(t) => t.to_string(),
        Value::Uuid(u) => format!("\"{}\"", u.uuid()),
        Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(value_to_ts_literal).collect();
            format!("[{}]", elements.join(", "))
        }
        Value::Row(row) => {
            let elements: Vec<String> = row.iter().map(value_to_ts_literal).collect();
            format!("[{}]", elements.join(", "))
        }
    }
}

fn string_to_ts_literal(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn enum_variants_to_ts_args(variants: &[String]) -> String {
    variants
        .iter()
        .map(|variant| string_to_ts_literal(variant))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Map ColumnType to col builder method name.
fn sql_type_to_col_method(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text => "string",
        ColumnType::Enum(_) => "enum",
        ColumnType::Boolean => "boolean",
        ColumnType::Integer | ColumnType::BigInt => "int",
        ColumnType::Timestamp => "int", // Timestamps are stored as integers
        _ => "string",                  // Fallback for unknown types
    }
}

/// Convert a LensTransform to TypeScript code.
///
/// Generates a TypeScript file with the migration definition using the jazz-tools DSL.
/// Uses side-effect collection - no export needed.
fn lens_transform_to_ts(transform: &LensTransform) -> String {
    let mut lines = Vec::new();
    lines.push("import { migrate, col } from \"jazz-tools\"".to_string());
    lines.push(String::new());

    // Group operations by table
    let mut tables: std::collections::HashMap<&str, Vec<(usize, &LensOp)>> =
        std::collections::HashMap::new();
    for (idx, op) in transform.ops.iter().enumerate() {
        tables.entry(op.table()).or_default().push((idx, op));
    }

    let table_names: Vec<_> = tables.keys().copied().collect();

    for (table_idx, table) in table_names.iter().enumerate() {
        let ops = tables.get(table).unwrap();
        let is_draft = |idx: usize| transform.draft_ops.contains(&idx);

        lines.push(format!("migrate(\"{}\", {{", table));

        for (idx, op) in ops {
            let draft_comment = if is_draft(*idx) {
                " // TODO: Review this auto-generated operation"
            } else {
                ""
            };

            match op {
                LensOp::AddColumn {
                    column,
                    column_type,
                    default,
                    ..
                } => {
                    let default_ts = value_to_ts_literal(default);
                    let optional = if matches!(default, Value::Null) {
                        ".optional()"
                    } else {
                        ""
                    };
                    match column_type {
                        ColumnType::Array(element_type) => {
                            let element_literal = column_type_to_sql(element_type);
                            lines.push(format!(
                                "  {}: col.add(){}.array({{ of: \"{}\", default: {} }}),{}",
                                column, optional, element_literal, default_ts, draft_comment
                            ));
                        }
                        ColumnType::Enum(variants) => {
                            let variant_args = enum_variants_to_ts_args(variants);
                            lines.push(format!(
                                "  {}: col.add(){}.enum({}, {{ default: {} }}),{}",
                                column, optional, variant_args, default_ts, draft_comment
                            ));
                        }
                        _ => {
                            let method = sql_type_to_col_method(column_type);
                            lines.push(format!(
                                "  {}: col.add(){}.{}({{ default: {} }}),{}",
                                column, optional, method, default_ts, draft_comment
                            ));
                        }
                    }
                }
                LensOp::RemoveColumn {
                    column,
                    column_type,
                    default,
                    ..
                } => {
                    let default_ts = value_to_ts_literal(default);
                    match column_type {
                        ColumnType::Array(element_type) => {
                            let element_literal = column_type_to_sql(element_type);
                            lines.push(format!(
                                "  {}: col.drop().array({{ of: \"{}\", backwardsDefault: {} }}),{}",
                                column, element_literal, default_ts, draft_comment
                            ));
                        }
                        ColumnType::Enum(variants) => {
                            let variant_args = enum_variants_to_ts_args(variants);
                            lines.push(format!(
                                "  {}: col.drop().enum({}, {{ backwardsDefault: {} }}),{}",
                                column, variant_args, default_ts, draft_comment
                            ));
                        }
                        _ => {
                            let method = sql_type_to_col_method(column_type);
                            lines.push(format!(
                                "  {}: col.drop().{}({{ backwardsDefault: {} }}),{}",
                                column, method, default_ts, draft_comment
                            ));
                        }
                    }
                }
                LensOp::RenameColumn {
                    old_name, new_name, ..
                } => {
                    lines.push(format!(
                        "  {}: col.rename(\"{}\"),{}",
                        new_name, old_name, draft_comment
                    ));
                }
                LensOp::AddTable { .. } | LensOp::RemoveTable { .. } => {
                    lines.push(
                        "  // TODO: Table-level operation not yet supported in TypeScript DSL"
                            .to_string(),
                    );
                }
            }
        }

        lines.push("})".to_string());
        if table_idx < table_names.len() - 1 {
            lines.push(String::new());
        }
    }

    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema};
    use tempfile::TempDir;

    fn create_test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    #[test]
    fn schema_directory_new() {
        let dir = SchemaDirectory::new("./schema");
        assert_eq!(dir.path(), Path::new("./schema"));
    }

    #[test]
    fn write_and_read_current() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let schema = create_test_schema();
        dir.write_current(&schema).unwrap();

        assert!(dir.has_current());

        let read_schema = dir.current_schema().unwrap();
        assert_eq!(read_schema.len(), 1);
    }

    #[test]
    fn write_and_read_frozen_schema() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let schema = create_test_schema();
        let hash = SchemaHash::compute(&schema);

        dir.write_schema(&schema, 1, None, &hash.short()).unwrap();

        assert!(dir.has_schema_with_hash(&hash.short()));

        let read_schema = dir.schema_by_version(1).unwrap();
        assert_eq!(read_schema.len(), 1);
    }

    #[test]
    fn list_schema_versions() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        // Create two schemas
        let schema1 = SchemaBuilder::new()
            .table(TableSchema::builder("a").column("x", ColumnType::Text))
            .build();
        let hash1 = SchemaHash::compute(&schema1);

        let schema2 = SchemaBuilder::new()
            .table(TableSchema::builder("b").column("y", ColumnType::Integer))
            .build();
        let hash2 = SchemaHash::compute(&schema2);

        dir.write_schema(&schema1, 1, None, &hash1.short()).unwrap();
        dir.write_schema(&schema2, 2, Some("add_table_b"), &hash2.short())
            .unwrap();

        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 2);

        // Should be in version order
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[0].hash, hash1.short());
        assert_eq!(versions[0].description, None);

        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[1].hash, hash2.short());
        assert_eq!(versions[1].description, Some("add_table_b".to_string()));
    }

    #[test]
    fn write_and_read_migration() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let hash1 = "a1b2c3d4e5f6";
        let hash2 = "f7e8d9c0b1a2";

        let transform = LensTransform::with_ops(vec![super::super::lens::LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: crate::query_manager::types::Value::Integer(0),
        }]);

        dir.write_migration_sql(1, 2, hash1, hash2, Direction::Forward, &transform)
            .unwrap();

        assert!(dir.has_migration_sql(1, 2, hash1, hash2, Direction::Forward));
        assert!(!dir.has_migration_sql(1, 2, hash1, hash2, Direction::Backward));

        let read_transform = dir
            .migration(1, 2, hash1, hash2, Direction::Forward)
            .unwrap();
        assert_eq!(read_transform.ops.len(), 1);
    }

    #[test]
    fn write_migration_pair() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let hash1 = "a1b2c3d4e5f6";
        let hash2 = "f7e8d9c0b1a2";

        let forward = LensTransform::with_ops(vec![super::super::lens::LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: crate::query_manager::types::Value::Integer(0),
        }]);

        dir.write_migration_sql_pair(1, 2, hash1, hash2, &forward)
            .unwrap();

        assert!(dir.has_migration_sql(1, 2, hash1, hash2, Direction::Forward));
        assert!(dir.has_migration_sql(1, 2, hash1, hash2, Direction::Backward));

        // Verify backward is the inverse
        let bwd = dir
            .migration(1, 2, hash1, hash2, Direction::Backward)
            .unwrap();
        assert!(matches!(
            &bwd.ops[0],
            super::super::lens::LensOp::RemoveColumn { .. }
        ));
    }

    #[test]
    fn parse_versioned_schema_filename_valid() {
        // Simple version with hash
        let info = parse_versioned_schema_filename("schema_v1_455a1f10a158.sql");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.version, 1);
        assert_eq!(info.description, None);
        assert_eq!(info.hash, "455a1f10a158");

        // Version with description
        let info = parse_versioned_schema_filename("schema_v2_add_description_357c464c4c43.sql");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.version, 2);
        assert_eq!(info.description, Some("add_description".to_string()));
        assert_eq!(info.hash, "357c464c4c43");

        // Multi-word description
        let info = parse_versioned_schema_filename("schema_v3_add_user_table_abc123def456.sql");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.version, 3);
        assert_eq!(info.description, Some("add_user_table".to_string()));
        assert_eq!(info.hash, "abc123def456");
    }

    #[test]
    fn parse_versioned_schema_filename_invalid() {
        assert!(parse_versioned_schema_filename("current.sql").is_none());
        assert!(parse_versioned_schema_filename("schema_abc.sql").is_none()); // No version
        assert!(parse_versioned_schema_filename("schema_v1.sql").is_none()); // No hash
        assert!(parse_versioned_schema_filename("schema_v1_abc.sql").is_none()); // Hash too short
        assert!(parse_versioned_schema_filename("schema_455a1f10a158.sql").is_none());
        // Old format
    }

    #[test]
    fn parse_migration_filename_valid() {
        // TypeScript stub
        let info = parse_migration_filename("migration_v1_v2_455a1f10a158_357c464c4c43.ts");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.from_version, 1);
        assert_eq!(info.to_version, 2);
        assert_eq!(info.from_hash, "455a1f10a158");
        assert_eq!(info.to_hash, "357c464c4c43");
        assert_eq!(info.direction, None);

        // Forward SQL (direction before hashes)
        let info = parse_migration_filename("migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.from_version, 1);
        assert_eq!(info.to_version, 2);
        assert_eq!(info.from_hash, "455a1f10a158");
        assert_eq!(info.to_hash, "357c464c4c43");
        assert_eq!(info.direction, Some(Direction::Forward));

        // Backward SQL
        let info = parse_migration_filename("migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.direction, Some(Direction::Backward));
    }

    #[test]
    fn parse_migration_filename_invalid() {
        assert!(parse_migration_filename("schema_v1_455a1f10a158.sql").is_none());
        assert!(parse_migration_filename("migration_v1_v2_fwd_abc_def.sql").is_none()); // Hashes too short
        assert!(
            parse_migration_filename("migration_v1_v2_invalid_455a1f10a158_357c464c4c43.sql")
                .is_none()
        );
        assert!(parse_migration_filename("lens_455a1f10a158_357c464c4c43_fwd.sql").is_none());
        // Old format
    }

    #[test]
    fn current_schema_not_found() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let result = dir.current_schema();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FileError::NotFound(_)));
    }

    #[test]
    fn latest_version() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        // No versions yet
        assert!(dir.latest_version().unwrap().is_none());

        // Create a schema
        let schema = create_test_schema();
        let hash = SchemaHash::compute(&schema);
        dir.write_schema(&schema, 1, None, &hash.short()).unwrap();

        // Now we have a latest version
        let latest = dir.latest_version().unwrap();
        assert!(latest.is_some());
        let latest = latest.unwrap();
        assert_eq!(latest.version, 1);
        assert_eq!(latest.hash, hash.short());
    }

    #[test]
    fn schema_filename_generation() {
        let info = SchemaFileInfo {
            version: 1,
            description: None,
            hash: "455a1f10a158".to_string(),
        };
        assert_eq!(schema_filename(&info), "schema_v1_455a1f10a158.sql");

        let info = SchemaFileInfo {
            version: 2,
            description: Some("add_description".to_string()),
            hash: "357c464c4c43".to_string(),
        };
        assert_eq!(
            schema_filename(&info),
            "schema_v2_add_description_357c464c4c43.sql"
        );
    }

    #[test]
    fn migration_filename_generation() {
        assert_eq!(
            migration_ts_filename(1, 2, "455a1f10a158", "357c464c4c43"),
            "migration_v1_v2_455a1f10a158_357c464c4c43.ts"
        );

        // SQL filenames have direction before hashes for better truncation display
        assert_eq!(
            migration_sql_filename(1, 2, "455a1f10a158", "357c464c4c43", Direction::Forward),
            "migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql"
        );

        assert_eq!(
            migration_sql_filename(1, 2, "455a1f10a158", "357c464c4c43", Direction::Backward),
            "migration_v1_v2_bwd_455a1f10a158_357c464c4c43.sql"
        );
    }

    #[test]
    fn lens_transform_to_ts_marks_nullable_adds_as_optional() {
        let transform = LensTransform::with_ops(vec![
            super::super::lens::LensOp::AddColumn {
                table: "todos".to_string(),
                column: "description".to_string(),
                column_type: ColumnType::Text,
                default: crate::query_manager::types::Value::Null,
            },
            super::super::lens::LensOp::AddColumn {
                table: "todos".to_string(),
                column: "title".to_string(),
                column_type: ColumnType::Text,
                default: crate::query_manager::types::Value::Text("".to_string()),
            },
        ]);

        let ts = lens_transform_to_ts(&transform);
        assert!(ts.contains("description: col.add().optional().string({ default: null }),"));
        assert!(ts.contains("title: col.add().string({ default: \"\" }),"));
    }

    #[test]
    fn lens_transform_to_ts_uses_array_builder_for_array_columns() {
        let transform = LensTransform::with_ops(vec![
            super::super::lens::LensOp::AddColumn {
                table: "projects".to_string(),
                column: "todos".to_string(),
                column_type: ColumnType::Array(Box::new(ColumnType::Uuid)),
                default: crate::query_manager::types::Value::Array(vec![]),
            },
            super::super::lens::LensOp::RemoveColumn {
                table: "projects".to_string(),
                column: "todos".to_string(),
                column_type: ColumnType::Array(Box::new(ColumnType::Uuid)),
                default: crate::query_manager::types::Value::Array(vec![]),
            },
        ]);

        let ts = lens_transform_to_ts(&transform);
        assert!(ts.contains("todos: col.add().array({ of: \"UUID\", default: [] }),"));
        assert!(ts.contains("todos: col.drop().array({ of: \"UUID\", backwardsDefault: [] }),"));
    }

    #[test]
    fn lens_transform_to_ts_uses_enum_builder_for_enum_columns() {
        let transform = LensTransform::with_ops(vec![
            super::super::lens::LensOp::AddColumn {
                table: "todos".to_string(),
                column: "status".to_string(),
                column_type: ColumnType::Enum(vec!["done".to_string(), "todo".to_string()]),
                default: crate::query_manager::types::Value::Text("todo".to_string()),
            },
            super::super::lens::LensOp::RemoveColumn {
                table: "todos".to_string(),
                column: "status".to_string(),
                column_type: ColumnType::Enum(vec!["done".to_string(), "todo".to_string()]),
                default: crate::query_manager::types::Value::Text("todo".to_string()),
            },
        ]);

        let ts = lens_transform_to_ts(&transform);
        assert!(ts.contains("status: col.add().enum(\"done\", \"todo\", { default: \"todo\" }),"));
        assert!(ts.contains(
            "status: col.drop().enum(\"done\", \"todo\", { backwardsDefault: \"todo\" }),"
        ));
    }

    #[test]
    #[should_panic(expected = "non-finite float")]
    fn value_to_ts_literal_rejects_infinity() {
        value_to_ts_literal(&crate::query_manager::types::Value::Real(f64::INFINITY));
    }

    #[test]
    #[should_panic(expected = "non-finite float")]
    fn value_to_ts_literal_rejects_nan() {
        value_to_ts_literal(&crate::query_manager::types::Value::Real(f64::NAN));
    }
}
