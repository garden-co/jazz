//! Well-known metadata keys and values used across object and commit metadata.

use std::collections::BTreeMap;
use std::fmt;

/// Keys used in object metadata (`HashMap<String, String>`) and commit
/// metadata (`BTreeMap<String, String>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataKey {
    /// Table name — set on row objects.
    Table,
    /// Origin schema hash - set on row objects.
    OriginSchemaHash,
    /// Object type — catalogue_schema, catalogue_lens, index, etc.
    Type,
    /// Commit delete marker — soft or hard.
    Delete,
    /// Original creating principal for a row commit history.
    CreatedBy,
    /// Original creation timestamp for a row commit history.
    CreatedAt,
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
            Self::OriginSchemaHash => "origin_schema_hash",
            Self::Type => "type",
            Self::Delete => "delete",
            Self::CreatedBy => "created_by",
            Self::CreatedAt => "created_at",
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
    CataloguePermissionsBundle,
    CataloguePermissionsHead,
    /// Legacy single-object permissions catalogue entry.
    CataloguePermissions,
    Index,
}

impl ObjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatalogueSchema => "catalogue_schema",
            Self::CatalogueLens => "catalogue_lens",
            Self::CataloguePermissionsBundle => "catalogue_permissions_bundle",
            Self::CataloguePermissionsHead => "catalogue_permissions_head",
            Self::CataloguePermissions => "catalogue_permissions",
            Self::Index => "index",
        }
    }

    pub fn is_catalogue_type_str(kind: &str) -> bool {
        matches!(
            kind,
            "catalogue_schema"
                | "catalogue_lens"
                | "catalogue_permissions_bundle"
                | "catalogue_permissions_head"
                | "catalogue_permissions"
        )
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

pub const SYSTEM_PRINCIPAL_ID: &str = "jazz:system";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowProvenance {
    pub created_by: String,
    pub created_at: u64,
    pub updated_by: String,
    pub updated_at: u64,
}

impl RowProvenance {
    pub fn for_insert(author: impl Into<String>, timestamp: u64) -> Self {
        let author = author.into();
        Self {
            created_by: author.clone(),
            created_at: timestamp,
            updated_by: author,
            updated_at: timestamp,
        }
    }

    pub fn for_update(existing: &Self, author: impl Into<String>, timestamp: u64) -> Self {
        Self {
            created_by: existing.created_by.clone(),
            created_at: existing.created_at,
            updated_by: author.into(),
            updated_at: timestamp,
        }
    }
}

pub fn row_provenance_metadata(
    provenance: &RowProvenance,
    delete_kind: Option<DeleteKind>,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::from([
        (
            MetadataKey::CreatedBy.to_string(),
            provenance.created_by.clone(),
        ),
        (
            MetadataKey::CreatedAt.to_string(),
            provenance.created_at.to_string(),
        ),
    ]);

    if let Some(delete_kind) = delete_kind {
        metadata.insert(MetadataKey::Delete.to_string(), delete_kind.to_string());
    }

    metadata
}

pub fn row_provenance_from_metadata(
    metadata: Option<&BTreeMap<String, String>>,
    updated_by: &str,
    updated_at: u64,
) -> Option<RowProvenance> {
    let metadata = metadata?;
    let created_by = metadata.get(MetadataKey::CreatedBy.as_str())?.clone();
    let created_at = metadata
        .get(MetadataKey::CreatedAt.as_str())?
        .parse::<u64>()
        .ok()?;
    Some(RowProvenance {
        created_by,
        created_at,
        updated_by: updated_by.to_string(),
        updated_at,
    })
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
