//! Catalogue snapshots shared by native and WASM server hosts.
use jazz::catalogue::CatalogueRows;
use jazz::tools::{
    ObjectId,
    public_schema::{Schema, SchemaHash},
    schema_lens::Lens,
};
use std::collections::{HashMap, HashSet};
/// Errors from server-local catalogue operations.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum CatalogueError {
    QueryError(String),
    WriteError(String),
    StorageError(String),
    DecodeError {
        object_id: ObjectId,
        object_type: String,
        error: String,
    },
    NotFound,
    LockError,
}

impl std::fmt::Display for CatalogueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueError::QueryError(message) => write!(f, "Query error: {message}"),
            CatalogueError::WriteError(message) => write!(f, "Write error: {message}"),
            CatalogueError::StorageError(message) => write!(f, "Storage error: {message}"),
            CatalogueError::DecodeError {
                object_id,
                object_type,
                error,
            } => write!(
                f,
                "Decode error in durable catalogue entry {object_id} ({object_type}): {error}"
            ),
            CatalogueError::NotFound => write!(f, "Not found"),
            CatalogueError::LockError => write!(f, "Lock error"),
        }
    }
}

impl std::error::Error for CatalogueError {}

/// The catalogue as the HTTP routes read it, rebuilt from the store's
/// rows after every change.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct CatalogueSnapshot {
    pub(crate) schemas: HashMap<SchemaHash, Schema>,
    pub(crate) schema_published_at: HashMap<SchemaHash, u64>,
    pub(crate) lens_edges: HashSet<(SchemaHash, SchemaHash)>,
    pub(crate) lenses: HashMap<(SchemaHash, SchemaHash), Lens>,
    /// Every published permissions bundle (see [`jazz::catalogue`]),
    /// oldest first.
    pub(crate) bundles: Vec<jazz::catalogue::CatalogueBundle>,
    /// The admitted schemas, oldest first.
    pub(crate) admitted: Vec<SchemaHash>,
}

impl CatalogueSnapshot {
    pub(crate) fn from_rows(
        rows: &CatalogueRows,
        initial_schema: Option<&Schema>,
    ) -> Result<Self, CatalogueError> {
        let mut index = Self::default();
        for schema in &rows.schemas {
            index.schemas.insert(schema.hash, schema.schema.clone());
            if let Some(published_at) = schema.published_at {
                index.schema_published_at.insert(schema.hash, published_at);
            }
        }
        for lens in &rows.lenses {
            let lens = Lens::new(lens.source, lens.target, lens.forward.clone());
            if !lens.is_draft() {
                index
                    .lens_edges
                    .insert((lens.source_hash, lens.target_hash));
                index
                    .lenses
                    .insert((lens.source_hash, lens.target_hash), lens);
            }
        }
        index.bundles = rows.bundles.clone();
        index
            .bundles
            .sort_by_key(|bundle| (bundle.published_at, bundle.id.0));
        let mut admitted: Vec<_> = rows
            .schemas
            .iter()
            .filter_map(|schema| Some((schema.admitted?, schema.hash)))
            .collect();
        admitted.sort_by_key(|(n, _)| *n);
        index.admitted = admitted.into_iter().map(|(_, hash)| hash).collect();
        if let Some(schema) = initial_schema {
            // The store's data was written under it too: the deploy guard
            // counts it as admitted (SPEC 4 §4.9), oldest.
            let hash = index.add_schema(schema.clone());
            if !index.admitted.contains(&hash) {
                index.admitted.insert(0, hash);
            }
        }
        Ok(index)
    }

    pub(crate) fn add_schema(&mut self, schema: Schema) -> SchemaHash {
        let hash = SchemaHash::compute(&schema);
        self.schemas.entry(hash).or_insert(schema);
        hash
    }

    #[cfg(feature = "native")]
    pub(crate) fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        let mut hashes = self.schemas.keys().copied().collect::<Vec<_>>();
        hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes
    }

    #[cfg(feature = "native")]
    pub(crate) fn latest_published_schema(&self) -> Option<Schema> {
        let mut candidates = self
            .schema_published_at
            .iter()
            .map(|(hash, published_at)| (*published_at, *hash))
            .collect::<Vec<_>>();
        candidates.sort_by(|(left_time, left_hash), (right_time, right_hash)| {
            left_time
                .cmp(right_time)
                .then_with(|| left_hash.as_bytes().cmp(right_hash.as_bytes()))
        });
        let (_, hash) = candidates.pop()?;
        self.schemas.get(&hash).cloned()
    }
}

use super::permissions_validation::validate_permissions_relations;
use jazz::catalogue::{CatalogueLens, CataloguePut};
use jazz::tools::{
    deployment::{DeploymentCatalogue, PreparedDeployment},
    public_schema::TablePolicies,
};
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentResponse {
    pub changed: bool,
    pub published: PublishedArtifacts,
    /// The published permissions bundle (see [`jazz::catalogue`]): what
    /// the backend pins its sessions to.
    pub bundle: String,
}
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PublishedArtifacts {
    pub schemas: Vec<String>,
    pub migrations: Vec<PublishedMigration>,
}
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedMigration {
    pub from_hash: String,
    pub to_hash: String,
}

impl CatalogueSnapshot {
    pub(crate) fn deployment_catalogue(&self) -> DeploymentCatalogue {
        DeploymentCatalogue {
            schemas: self.schemas.values().cloned().collect(),
            migrations: self.lenses.values().cloned().collect(),
            admitted_schema_hashes: self.admitted.clone(),
        }
    }
    pub(crate) fn stage_deployment(
        &self,
        prepared: &PreparedDeployment,
        now: u64,
    ) -> Result<(CataloguePut, DeploymentResponse), CatalogueError> {
        let mut put = CataloguePut::default();
        let mut published = PublishedArtifacts::default();
        for schema in prepared.schemas() {
            let hash = SchemaHash::compute(schema);
            let unchanged = self.schemas.get(&hash).is_some_and(|stored| {
                let mut structural = stored.clone();
                for table in structural.values_mut() {
                    table.policies = TablePolicies::default();
                }
                structural == *schema
            });
            // A configured initial schema, or one the store admitted, is
            // published only once a deployment publishes it.
            if unchanged && self.schema_published_at.contains_key(&hash) {
                continue;
            }
            published.schemas.push(hash.to_string());
            put.schemas.push((schema.clone(), now));
        }
        for lens in prepared.migrations() {
            // Preparation already rejects conflicting definitions for this pair.
            if self
                .lenses
                .contains_key(&(lens.source_hash, lens.target_hash))
            {
                continue;
            }
            published.migrations.push(PublishedMigration {
                from_hash: lens.source_hash.to_string(),
                to_hash: lens.target_hash.to_string(),
            });
            put.lenses.push(CatalogueLens {
                source: lens.source_hash,
                target: lens.target_hash,
                published_at: now,
                forward: lens.forward.clone(),
            });
        }
        let source = prepared.active_schema().public_schema();
        let schema_hash = SchemaHash::compute(source);
        let permissions = source
            .iter()
            .filter(|(_, table)| table.policies != TablePolicies::default())
            .map(|(name, table)| (*name, table.policies.clone()))
            .collect::<HashMap<_, _>>();
        validate_permissions_relations(&permissions).map_err(CatalogueError::WriteError)?;
        let permissions: jazz::catalogue::Permissions = permissions.into_iter().collect();
        let bundle = jazz::catalogue::bundle_id(&schema_hash, &permissions)
            .map_err(|error| CatalogueError::WriteError(error.to_string()))?;
        let new = !self.bundles.iter().any(|known| known.id == bundle);
        if new {
            put.bundles.push((schema_hash, permissions, now));
        }
        let changed = new || !published.schemas.is_empty() || !published.migrations.is_empty();
        published.schemas.sort();
        published
            .migrations
            .sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        Ok((
            put,
            DeploymentResponse {
                changed,
                published,
                bundle: bundle.to_hex(),
            },
        ))
    }
}

/// HTTP graph inventory, captured under one catalogue index lock.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationGraph {
    /// The schema of the bundle published last, which new sessions pin to.
    latest_bundle_schema_hash: Option<String>,
    schemas: Vec<String>,
    migrations: Vec<MigrationGraphEdge>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MigrationGraphEdge {
    from_hash: String,
    to_hash: String,
    /// Derived capability, not persisted provenance: no migration file is needed.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    automatic: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaSummary {
    hash: String,
    published_at: Option<u64>,
}

#[derive(Debug, serde::Serialize)]
pub struct SchemaSummaryResponse {
    #[deprecated(note = "Use `schemas` instead")]
    hashes: Vec<String>,
    schemas: Vec<SchemaSummary>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredSchemaResponse {
    schema: Schema,
    published_at: Option<u64>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedBundleView {
    bundle: String,
    schema_hash: String,
    published_at: u64,
    permissions: std::collections::HashMap<String, TablePolicies>,
}

#[derive(Debug, serde::Serialize)]
pub struct PublishedBundlesResponse {
    /// Oldest first; the last is what a backend booting now pins to.
    bundles: Vec<PublishedBundleView>,
}

impl CatalogueSnapshot {
    pub fn migration_graph(&self) -> MigrationGraph {
        let mut schemas: Vec<_> = self.schemas.keys().map(ToString::to_string).collect();
        schemas.sort();
        let mut migrations: Vec<_> = self
            .lenses
            .values()
            .map(|lens| MigrationGraphEdge {
                from_hash: lens.source_hash.to_string(),
                to_hash: lens.target_hash.to_string(),
                automatic: lens.forward.ops.is_empty()
                    && lens.backward.ops.is_empty()
                    && !lens.is_draft()
                    && self
                        .schemas
                        .get(&lens.source_hash)
                        .zip(self.schemas.get(&lens.target_hash))
                        .is_some_and(|(from, to)| {
                            jazz::tools::deployment::schemas_are_compatible(from, to)
                        }),
            })
            .collect();
        migrations.sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        MigrationGraph {
            latest_bundle_schema_hash: self.bundles.last().map(|bundle| bundle.schema.to_string()),
            schemas,
            migrations,
        }
    }
    pub fn schema(&self, hash: &SchemaHash) -> Option<StoredSchemaResponse> {
        Some(StoredSchemaResponse {
            schema: self.schemas.get(hash)?.clone(),
            published_at: self.schema_published_at.get(hash).copied(),
        })
    }
    pub fn schemas(&self) -> SchemaSummaryResponse {
        let mut hashes = self.schemas.keys().copied().collect::<Vec<_>>();
        hashes.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        #[allow(deprecated)]
        SchemaSummaryResponse {
            hashes: hashes.iter().map(ToString::to_string).collect(),
            schemas: hashes
                .iter()
                .map(|hash| SchemaSummary {
                    hash: hash.to_string(),
                    published_at: self.schema_published_at.get(hash).copied(),
                })
                .collect(),
        }
    }
    pub fn bundles(&self) -> PublishedBundlesResponse {
        PublishedBundlesResponse {
            bundles: self
                .bundles
                .iter()
                .map(|bundle| PublishedBundleView {
                    bundle: bundle.id.to_string(),
                    schema_hash: bundle.schema.to_string(),
                    published_at: bundle.published_at,
                    permissions: bundle
                        .permissions
                        .iter()
                        .map(|(table, policies)| (table.to_string(), policies.clone()))
                        .collect(),
                })
                .collect(),
        }
    }
}
impl super::ServerCore {
    /// Read catalogue metadata through this authority's owning connection.
    pub fn catalogue_snapshot(&self) -> Result<CatalogueSnapshot, String> {
        let rows = self.db.catalogue().map_err(|error| error.to_string())?;
        CatalogueSnapshot::from_rows(&rows, None).map_err(|error| error.to_string())
    }
}
