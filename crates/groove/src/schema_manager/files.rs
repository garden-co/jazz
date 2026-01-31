//! File convention API for schema and lens files.
//!
//! This module provides a high-level API for working with schema directories,
//! following the convention:
//!
//! ```text
//! schema/
//! ├── current.sql              # Editable source of truth
//! ├── schema_a1b2c3d4e5f6.sql   # Frozen v1 (12-char hex hash)
//! ├── schema_f7e8d9c0b1a2.sql   # Frozen v2
//! ├── lens_a1b2c3d4e5f6_f7e8d9c0b1a2_fwd.sql  # v1 → v2
//! └── lens_a1b2c3d4e5f6_f7e8d9c0b1a2_bwd.sql  # v2 → v1
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use crate::query_manager::types::{Schema, SchemaHash};

use super::lens::{Direction, LensTransform};
use super::sql::{SqlParseError, lens_to_sql, parse_lens, parse_schema, schema_to_sql};

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

    /// List all frozen schema hashes in order (oldest to newest).
    ///
    /// Scans for files matching `schema_*.sql` pattern and extracts hashes.
    /// Returns hashes sorted by file modification time.
    pub fn schema_versions(&self) -> Result<Vec<SchemaHash>, FileError> {
        use std::time::SystemTime;

        let mut versions: Vec<(SchemaHash, Option<SystemTime>)> = Vec::new();

        if !self.path.exists() {
            return Ok(Vec::new());
        }

        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();

            if let Some(hash) = parse_schema_filename(&name) {
                let modified = entry.metadata()?.modified().ok();
                versions.push((hash, modified));
            }
        }

        // Sort by modification time (oldest first)
        versions.sort_by(|a, b| a.1.cmp(&b.1));

        Ok(versions.into_iter().map(|(hash, _)| hash).collect())
    }

    /// Get the latest schema version hash, if any.
    pub fn latest_version(&self) -> Result<Option<SchemaHash>, FileError> {
        let versions = self.schema_versions()?;
        Ok(versions.last().copied())
    }

    /// Read a specific frozen schema by hash.
    pub fn schema(&self, hash: SchemaHash) -> Result<Schema, FileError> {
        let filename = format!("schema_{}.sql", hash.short());
        let path = self.path.join(&filename);

        if !path.exists() {
            return Err(FileError::NotFound(path.display().to_string()));
        }

        let content = fs::read_to_string(&path)?;
        let schema = parse_schema(&content)?;
        Ok(schema)
    }

    /// Read a lens between two versions.
    pub fn lens(
        &self,
        from: SchemaHash,
        to: SchemaHash,
        direction: Direction,
    ) -> Result<LensTransform, FileError> {
        let dir_suffix = match direction {
            Direction::Forward => "fwd",
            Direction::Backward => "bwd",
        };
        let filename = format!("lens_{}_{}_{}.sql", from.short(), to.short(), dir_suffix);
        let path = self.path.join(&filename);

        if !path.exists() {
            return Err(FileError::NotFound(path.display().to_string()));
        }

        let content = fs::read_to_string(&path)?;
        let transform = parse_lens(&content)?;
        Ok(transform)
    }

    /// Check if a lens exists between two versions.
    pub fn has_lens(&self, from: SchemaHash, to: SchemaHash, direction: Direction) -> bool {
        let dir_suffix = match direction {
            Direction::Forward => "fwd",
            Direction::Backward => "bwd",
        };
        let filename = format!("lens_{}_{}_{}.sql", from.short(), to.short(), dir_suffix);
        self.path.join(&filename).exists()
    }

    /// Write a frozen schema file.
    pub fn write_schema(&self, schema: &Schema, hash: SchemaHash) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let filename = format!("schema_{}.sql", hash.short());
        let path = self.path.join(&filename);
        let content = schema_to_sql(schema);

        fs::write(&path, content)?;
        Ok(path)
    }

    /// Write a lens file.
    pub fn write_lens(
        &self,
        from: SchemaHash,
        to: SchemaHash,
        direction: Direction,
        transform: &LensTransform,
    ) -> Result<PathBuf, FileError> {
        fs::create_dir_all(&self.path)?;

        let dir_suffix = match direction {
            Direction::Forward => "fwd",
            Direction::Backward => "bwd",
        };
        let filename = format!("lens_{}_{}_{}.sql", from.short(), to.short(), dir_suffix);
        let path = self.path.join(&filename);
        let content = lens_to_sql(transform);

        fs::write(&path, content)?;
        Ok(path)
    }

    /// Write both forward and backward lens files.
    pub fn write_lens_pair(
        &self,
        from: SchemaHash,
        to: SchemaHash,
        forward: &LensTransform,
    ) -> Result<(PathBuf, PathBuf), FileError> {
        let backward = forward.invert();

        let fwd_path = self.write_lens(from, to, Direction::Forward, forward)?;
        let bwd_path = self.write_lens(from, to, Direction::Backward, &backward)?;

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

    /// Check if a schema version exists.
    pub fn has_schema(&self, hash: SchemaHash) -> bool {
        let filename = format!("schema_{}.sql", hash.short());
        self.path.join(&filename).exists()
    }
}

/// Parse a schema filename and extract the hash.
///
/// Valid format: `schema_{12-char-hex}.sql`
fn parse_schema_filename(name: &str) -> Option<SchemaHash> {
    if !name.starts_with("schema_") || !name.ends_with(".sql") {
        return None;
    }

    let hash_part = &name[7..name.len() - 4]; // Remove "schema_" prefix and ".sql" suffix

    // Must be 12 hex chars
    if hash_part.len() != 12 || !hash_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    // Decode hex to bytes
    let bytes = hex_decode(hash_part)?;
    if bytes.len() != 6 {
        return None;
    }

    // Create a SchemaHash with just the first 6 bytes filled in
    let mut hash_bytes = [0u8; 32];
    hash_bytes[..6].copy_from_slice(&bytes);
    Some(SchemaHash::from_bytes(hash_bytes))
}

/// Parse a lens filename and extract the hashes and direction.
///
/// Valid format: `lens_{12-char-hex}_{12-char-hex}_{fwd|bwd}.sql`
#[allow(dead_code)]
fn parse_lens_filename(name: &str) -> Option<(SchemaHash, SchemaHash, Direction)> {
    if !name.starts_with("lens_") || !name.ends_with(".sql") {
        return None;
    }

    let inner = &name[5..name.len() - 4]; // Remove "lens_" prefix and ".sql" suffix
    let parts: Vec<&str> = inner.split('_').collect();

    if parts.len() != 3 {
        return None;
    }

    let from_hex = parts[0];
    let to_hex = parts[1];
    let dir_str = parts[2];

    // Validate hex parts
    if from_hex.len() != 12
        || !from_hex.chars().all(|c| c.is_ascii_hexdigit())
        || to_hex.len() != 12
        || !to_hex.chars().all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }

    let direction = match dir_str {
        "fwd" => Direction::Forward,
        "bwd" => Direction::Backward,
        _ => return None,
    };

    let from_bytes = hex_decode(from_hex)?;
    let to_bytes = hex_decode(to_hex)?;

    let mut from_hash = [0u8; 32];
    let mut to_hash = [0u8; 32];
    from_hash[..6].copy_from_slice(&from_bytes);
    to_hash[..6].copy_from_slice(&to_bytes);

    Some((
        SchemaHash::from_bytes(from_hash),
        SchemaHash::from_bytes(to_hash),
        direction,
    ))
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
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

        dir.write_schema(&schema, hash).unwrap();

        assert!(dir.has_schema(hash));

        let read_schema = dir.schema(hash).unwrap();
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

        dir.write_schema(&schema1, hash1).unwrap();
        // Small delay to ensure different modification times
        std::thread::sleep(std::time::Duration::from_millis(10));
        dir.write_schema(&schema2, hash2).unwrap();

        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 2);

        // Should be in order (oldest first)
        assert_eq!(versions[0].short(), hash1.short());
        assert_eq!(versions[1].short(), hash2.short());
    }

    #[test]
    fn write_and_read_lens() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let hash1 = SchemaHash::from_bytes([1; 32]);
        let hash2 = SchemaHash::from_bytes([2; 32]);

        let transform = LensTransform::with_ops(vec![super::super::lens::LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: crate::query_manager::types::Value::Integer(0),
        }]);

        dir.write_lens(hash1, hash2, Direction::Forward, &transform)
            .unwrap();

        assert!(dir.has_lens(hash1, hash2, Direction::Forward));
        assert!(!dir.has_lens(hash1, hash2, Direction::Backward));

        let read_transform = dir.lens(hash1, hash2, Direction::Forward).unwrap();
        assert_eq!(read_transform.ops.len(), 1);
    }

    #[test]
    fn write_lens_pair() {
        let temp = TempDir::new().unwrap();
        let dir = SchemaDirectory::new(temp.path().join("schema"));

        let hash1 = SchemaHash::from_bytes([1; 32]);
        let hash2 = SchemaHash::from_bytes([2; 32]);

        let forward = LensTransform::with_ops(vec![super::super::lens::LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: crate::query_manager::types::Value::Integer(0),
        }]);

        dir.write_lens_pair(hash1, hash2, &forward).unwrap();

        assert!(dir.has_lens(hash1, hash2, Direction::Forward));
        assert!(dir.has_lens(hash1, hash2, Direction::Backward));

        // Verify backward is the inverse
        let bwd = dir.lens(hash1, hash2, Direction::Backward).unwrap();
        assert!(matches!(
            &bwd.ops[0],
            super::super::lens::LensOp::RemoveColumn { .. }
        ));
    }

    #[test]
    fn parse_schema_filename_valid() {
        let hash = parse_schema_filename("schema_a1b2c3d4e5f6.sql");
        assert!(hash.is_some());
        assert_eq!(hash.unwrap().short(), "a1b2c3d4e5f6");
    }

    #[test]
    fn parse_schema_filename_invalid() {
        assert!(parse_schema_filename("current.sql").is_none());
        assert!(parse_schema_filename("schema_abc.sql").is_none()); // Too short
        assert!(parse_schema_filename("schema_a1b2c3d4e5f6.txt").is_none());
        assert!(parse_schema_filename("lens_a1b2c3d4e5f6_f7e8d9c0b1a2_fwd.sql").is_none());
    }

    #[test]
    fn parse_lens_filename_valid() {
        let result = parse_lens_filename("lens_a1b2c3d4e5f6_f7e8d9c0b1a2_fwd.sql");
        assert!(result.is_some());
        let (from, to, dir) = result.unwrap();
        assert_eq!(from.short(), "a1b2c3d4e5f6");
        assert_eq!(to.short(), "f7e8d9c0b1a2");
        assert_eq!(dir, Direction::Forward);

        let result = parse_lens_filename("lens_a1b2c3d4e5f6_f7e8d9c0b1a2_bwd.sql");
        assert!(result.is_some());
        let (_, _, dir) = result.unwrap();
        assert_eq!(dir, Direction::Backward);
    }

    #[test]
    fn parse_lens_filename_invalid() {
        assert!(parse_lens_filename("schema_a1b2c3d4e5f6.sql").is_none());
        assert!(parse_lens_filename("lens_abc_def_fwd.sql").is_none()); // Too short
        assert!(parse_lens_filename("lens_a1b2c3d4e5f6_f7e8d9c0b1a2_invalid.sql").is_none());
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
        dir.write_schema(&schema, hash).unwrap();

        // Now we have a latest version
        let latest = dir.latest_version().unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().short(), hash.short());
    }
}
