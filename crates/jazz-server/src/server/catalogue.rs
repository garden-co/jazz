mod deployment;
pub(crate) use deployment::DeploymentResponse;

#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::server::catalogue_storage::{CatalogueStorage, CatalogueStorageError};
use jazz::catalogue::CatalogueRows;
#[cfg(any(test, feature = "embedded-server"))]
use jazz::catalogue::{CatalogueLens, CataloguePut};
use jazz::tools::ObjectId;
use jazz::tools::public_schema::{Schema, SchemaHash, TablePolicies};
use jazz::tools::schema_lens::Lens;
#[cfg(test)]
use jazz::tx::DurabilityTier;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionSchemaDiagnostics {
    pub client_schema_hash: SchemaHash,
    pub disconnected_permissions_schema_hash: Option<SchemaHash>,
    pub unreachable_schema_hashes: Vec<SchemaHash>,
}

#[cfg(test)]
impl ConnectionSchemaDiagnostics {
    pub(crate) fn has_issues(&self) -> bool {
        self.disconnected_permissions_schema_hash.is_some()
            || !self.unreachable_schema_hashes.is_empty()
    }
}

/// HTTP graph inventory, captured under one catalogue index lock.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MigrationGraph {
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

/// Server-local catalogue facade.
///
/// This is intentionally a thin wrapper over the direct catalogue store.
/// It may read and write admin catalogue metadata only: schemas, permissions,
/// and lenses. Production websocket sync, row storage, query execution, and
/// client lifecycle semantics stay on the local server path.
#[derive(Debug, Default)]
pub struct ServerCatalogue;

pub(crate) trait CatalogueStore {
    fn migration_graph(&self) -> Result<MigrationGraph, CatalogueError>;
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, CatalogueError>;
    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, CatalogueError>;
    #[cfg(feature = "embedded-server")]
    fn known_lenses(&self) -> Result<Vec<Lens>, CatalogueError>;
    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError>;
    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, CatalogueError>;
    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, CatalogueError>;
}

pub(crate) struct StoredCatalogue {
    /// A fixed-schema server's schema, known before any is published.
    initial_schema: Option<Schema>,
    index: Mutex<CatalogueIndex>,
    #[cfg(test)]
    test_schema_branches: Mutex<Vec<String>>,
    #[cfg(test)]
    test_local_durability_tiers: Mutex<HashSet<DurabilityTier>>,
    storage: Mutex<CatalogueStorage>,
}

impl StoredCatalogue {
    /// The permissions bundle published last, if any.
    #[cfg(feature = "embedded-server")]
    pub(crate) fn latest_bundle(&self) -> Option<jazz::catalogue::BundleId> {
        Some(self.index.lock().ok()?.bundles.last()?.id)
    }

    /// Every published permissions bundle, oldest first.
    pub(crate) fn published_bundles(&self) -> Vec<jazz::catalogue::CatalogueBundle> {
        self.index
            .lock()
            .map(|index| index.bundles.clone())
            .unwrap_or_default()
    }

    pub(crate) fn new(
        initial_schema: Option<Schema>,
        storage: CatalogueStorage,
    ) -> Result<Self, CatalogueError> {
        let index = CatalogueIndex::from_rows(&storage.read()?, initial_schema.as_ref())?;
        Ok(Self {
            initial_schema,
            index: Mutex::new(index),
            #[cfg(test)]
            test_schema_branches: Mutex::new(Vec::new()),
            #[cfg(test)]
            test_local_durability_tiers: Mutex::new(HashSet::new()),
            storage: Mutex::new(storage),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_observability(
        initial_schema: Option<Schema>,
        storage: CatalogueStorage,
        schema_branches: Vec<String>,
        local_durability_tiers: HashSet<DurabilityTier>,
    ) -> Result<Self, CatalogueError> {
        let store = Self::new(initial_schema, storage)?;
        *store
            .test_schema_branches
            .lock()
            .expect("schema branches lock") = schema_branches;
        *store
            .test_local_durability_tiers
            .lock()
            .expect("durability tiers lock") = local_durability_tiers;
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn local_durability_tiers_for_test(
        &self,
    ) -> Result<HashSet<DurabilityTier>, CatalogueError> {
        let tiers = self
            .test_local_durability_tiers
            .lock()
            .map_err(|_| CatalogueError::LockError)?;
        Ok(tiers.clone())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn persist_schema(&self) -> Result<ObjectId, CatalogueError> {
        let hash = {
            let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
            index
                .known_schema_hashes()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    CatalogueError::WriteError("no known schema to persist".to_string())
                })?
        };
        let schema = self
            .known_schema(&hash)?
            .ok_or_else(|| CatalogueError::WriteError("known schema disappeared".to_string()))?;
        <Self as CatalogueStore>::publish_schema(self, schema)
    }

    #[cfg(test)]
    pub(crate) fn stored_lens_for_test(
        &self,
        source_hash: SchemaHash,
        target_hash: SchemaHash,
    ) -> Result<Option<Lens>, CatalogueError> {
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(storage
            .read()?
            .lenses
            .into_iter()
            .find(|lens| lens.source == source_hash && lens.target == target_hash)
            .map(|lens| Lens::new(lens.source, lens.target, lens.forward)))
    }

    pub(crate) fn latest_published_schema(&self) -> Result<Option<Schema>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.latest_published_schema())
    }

    #[cfg(test)]
    pub(crate) fn connection_schema_diagnostics(
        &self,
        client_schema_hash: SchemaHash,
    ) -> Result<ConnectionSchemaDiagnostics, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.connection_schema_diagnostics(client_schema_hash))
    }

    /// Read the catalogue again, after the runtime changed it.
    pub(crate) fn refresh(&self) -> Result<(), CatalogueError> {
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        self.reindex(&storage)
    }

    fn reindex(&self, storage: &CatalogueStorage) -> Result<(), CatalogueError> {
        let index = CatalogueIndex::from_rows(&storage.read()?, self.initial_schema.as_ref())?;
        *self.index.lock().map_err(|_| CatalogueError::LockError)? = index;
        Ok(())
    }

    /// Publish `put` and read the catalogue it leaves.
    #[cfg(any(test, feature = "embedded-server"))]
    fn write(&self, storage: &CatalogueStorage, put: CataloguePut) -> Result<(), CatalogueError> {
        let written = storage.put(put);
        // Whatever the outcome, serve what the store holds.
        let reindexed = self.reindex(storage);
        written?;
        reindexed
    }
}

impl From<CatalogueStorageError> for CatalogueError {
    fn from(error: CatalogueStorageError) -> Self {
        CatalogueError::StorageError(error.to_string())
    }
}

pub(crate) fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// The catalogue as the HTTP routes read it, rebuilt from the store's
/// rows after every change.
#[derive(Debug, Default, Clone, PartialEq)]
struct CatalogueIndex {
    schemas: HashMap<SchemaHash, Schema>,
    schema_published_at: HashMap<SchemaHash, u64>,
    lens_edges: HashSet<(SchemaHash, SchemaHash)>,
    lenses: HashMap<(SchemaHash, SchemaHash), Lens>,
    /// Every published permissions bundle (see [`jazz::catalogue`]),
    /// oldest first.
    bundles: Vec<jazz::catalogue::CatalogueBundle>,
    /// The admitted schemas, oldest first.
    admitted: Vec<SchemaHash>,
}

impl CatalogueIndex {
    fn from_rows(
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

    fn add_schema(&mut self, schema: Schema) -> SchemaHash {
        let hash = SchemaHash::compute(&schema);
        self.schemas.entry(hash).or_insert(schema);
        hash
    }

    fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        let mut hashes = self.schemas.keys().copied().collect::<Vec<_>>();
        hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes
    }

    fn latest_published_schema(&self) -> Option<Schema> {
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

    #[cfg(test)]
    fn connection_schema_diagnostics(
        &self,
        client_schema_hash: SchemaHash,
    ) -> ConnectionSchemaDiagnostics {
        let active_permissions_hash = self.known_schema_hashes().into_iter().next();
        let reachable_hashes = self.non_draft_reachable_hashes(client_schema_hash);
        let disconnected_permissions_schema_hash = active_permissions_hash
            .filter(|permissions_hash| !reachable_hashes.contains(permissions_hash));

        let mut unreachable_schema_hashes: Vec<_> = self
            .known_schema_hashes()
            .into_iter()
            .filter(|hash| *hash != client_schema_hash)
            .filter(|hash| !reachable_hashes.contains(hash))
            .filter(|hash| Some(*hash) != disconnected_permissions_schema_hash)
            .collect();
        unreachable_schema_hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        ConnectionSchemaDiagnostics {
            client_schema_hash,
            disconnected_permissions_schema_hash,
            unreachable_schema_hashes,
        }
    }

    #[cfg(test)]
    fn non_draft_reachable_hashes(&self, schema_hash: SchemaHash) -> HashSet<SchemaHash> {
        if !self.schemas.contains_key(&schema_hash) {
            return HashSet::new();
        }

        let mut seen = HashSet::from([schema_hash]);
        let mut queue = VecDeque::from([schema_hash]);
        while let Some(current) = queue.pop_front() {
            for &(source, target) in &self.lens_edges {
                let next = if source == current {
                    Some(target)
                } else if target == current {
                    Some(source)
                } else {
                    None
                };
                if let Some(next) = next
                    && seen.insert(next)
                {
                    queue.push_back(next);
                }
            }
        }
        seen
    }
}

impl CatalogueStore for StoredCatalogue {
    fn migration_graph(&self) -> Result<MigrationGraph, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        let mut schemas: Vec<_> = index.schemas.keys().map(ToString::to_string).collect();
        schemas.sort();
        let mut migrations: Vec<_> = index
            .lenses
            .values()
            .map(|lens| MigrationGraphEdge {
                from_hash: lens.source_hash.to_string(),
                to_hash: lens.target_hash.to_string(),
                automatic: lens.forward.ops.is_empty()
                    && lens.backward.ops.is_empty()
                    && !lens.is_draft()
                    && index
                        .schemas
                        .get(&lens.source_hash)
                        .zip(index.schemas.get(&lens.target_hash))
                        .is_some_and(|(from, to)| {
                            jazz::tools::deployment::schemas_are_compatible(from, to)
                        }),
            })
            .collect();
        migrations.sort_by(|a, b| (&a.from_hash, &a.to_hash).cmp(&(&b.from_hash, &b.to_hash)));
        Ok(MigrationGraph {
            latest_bundle_schema_hash: index.bundles.last().map(|bundle| bundle.schema.to_string()),
            schemas,
            migrations,
        })
    }

    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.known_schema_hashes())
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.schemas.get(schema_hash).cloned())
    }

    #[cfg(feature = "embedded-server")]
    fn known_lenses(&self) -> Result<Vec<Lens>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.lenses.values().cloned().collect())
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.schema_published_at.get(schema_hash).copied())
    }

    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, CatalogueError> {
        let hash = SchemaHash::compute(&schema);
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        self.write(
            &storage,
            CataloguePut {
                schemas: vec![(schema, unix_timestamp_millis())],
                ..CataloguePut::default()
            },
        )?;
        Ok(hash.to_object_id())
    }

    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, CatalogueError> {
        if lens.is_draft() {
            return Err(CatalogueError::WriteError(
                "cannot publish draft lens".to_string(),
            ));
        }
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        self.write(
            &storage,
            CataloguePut {
                lenses: vec![CatalogueLens {
                    source: lens.source_hash,
                    target: lens.target_hash,
                    published_at: unix_timestamp_millis(),
                    forward: lens.forward.clone(),
                }],
                ..CataloguePut::default()
            },
        )?;
        Ok(lens.object_id())
    }
}

impl ServerCatalogue {
    pub(crate) fn migration_graph(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<MigrationGraph, CatalogueError> {
        store.migration_graph()
    }

    pub(crate) fn known_schema_hashes(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Vec<SchemaHash>, CatalogueError> {
        store.known_schema_hashes()
    }

    pub(crate) fn known_schema(
        &self,
        store: &impl CatalogueStore,
        schema_hash: &SchemaHash,
    ) -> Result<Option<Schema>, CatalogueError> {
        store.known_schema(schema_hash)
    }

    #[cfg(feature = "embedded-server")]
    pub(crate) fn known_lenses(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Vec<Lens>, CatalogueError> {
        store.known_lenses()
    }

    pub(crate) fn schema_published_at(
        &self,
        store: &impl CatalogueStore,
        schema_hash: &SchemaHash,
    ) -> Result<Option<u64>, CatalogueError> {
        store.schema_published_at(schema_hash)
    }

    #[cfg(any(test, feature = "embedded-server"))]
    pub(crate) fn publish_schema(
        &self,
        store: &impl CatalogueStore,
        schema: Schema,
    ) -> Result<ObjectId, CatalogueError> {
        store.publish_schema(schema)
    }

    #[cfg(any(test, feature = "embedded-server"))]
    pub(crate) fn publish_lens(
        &self,
        store: &impl CatalogueStore,
        lens: &Lens,
    ) -> Result<ObjectId, CatalogueError> {
        store.publish_lens(lens)
    }
}

#[cfg(test)]
mod tests {
    use jazz::tools::public_schema::SchemaBuilder;

    use super::{CatalogueStore, StoredCatalogue};

    fn storage() -> crate::server::CatalogueStorage {
        crate::runtime::AppStore::memory()
            .unwrap()
            .catalogue()
            .unwrap()
    }

    /// Alice's unreferenced empty schema survives catalogue loading.
    #[test]
    fn unreferenced_empty_schema_is_loaded() {
        let schema = SchemaBuilder::new().build();
        let store = StoredCatalogue::new(None, storage()).unwrap();
        store.publish_schema(schema.clone()).unwrap();
        let hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let reopened = StoredCatalogue::new(None, store.storage.into_inner().unwrap()).unwrap();
        assert_eq!(reopened.known_schema(&hash).unwrap(), Some(schema));
        assert!(reopened.schema_published_at(&hash).unwrap().is_some());
    }
}
