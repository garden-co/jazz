//! Well-known metadata keys and values used across object and commit metadata.

use std::collections::BTreeMap;
use std::fmt;

/// Keys used in object metadata (`HashMap<String, String>`) and commit
/// metadata (`BTreeMap<String, String>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataKey {
    /// Table name — set on row objects.
    Table,
    /// Object type — catalogue_schema, catalogue_lens, index, etc.
    Type,
    /// Commit delete marker — soft or hard.
    Delete,
    /// Application identifier on catalogue objects.
    AppId,
    /// Schema content hash on catalogue schema objects.
    SchemaHash,
    /// Source schema hash on catalogue lens objects.
    SourceHash,
    /// Target schema hash on catalogue lens objects.
    TargetHash,
    /// Flag to suppress sync for an object.
    NoSync,
}

impl MetadataKey {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Type => "type",
            Self::Delete => "delete",
            Self::AppId => "app_id",
            Self::SchemaHash => "schema_hash",
            Self::SourceHash => "source_hash",
            Self::TargetHash => "target_hash",
            Self::NoSync => "nosync",
        }
    }
}

impl fmt::Display for MetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Values for the `Type` metadata key — what kind of object this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    CatalogueSchema,
    CatalogueLens,
    Index,
}

impl ObjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatalogueSchema => "catalogue_schema",
            Self::CatalogueLens => "catalogue_lens",
            Self::Index => "index",
        }
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Values for the `Delete` commit metadata key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteKind {
    Soft,
    Hard,
}

impl DeleteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Soft => "soft",
            Self::Hard => "hard",
        }
    }
}

impl fmt::Display for DeleteKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Commit metadata marking a soft delete (content preserved).
pub fn soft_delete_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([(
        MetadataKey::Delete.to_string(),
        DeleteKind::Soft.to_string(),
    )])
}

/// Commit metadata marking a hard delete (content cleared).
pub fn hard_delete_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([(
        MetadataKey::Delete.to_string(),
        DeleteKind::Hard.to_string(),
    )])
}
