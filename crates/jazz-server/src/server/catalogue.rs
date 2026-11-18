mod deployment;

#[cfg(test)]
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
pub(crate) use crate::portable::catalogue::ConnectionSchemaDiagnostics;
pub(crate) use crate::portable::catalogue::{CatalogueIndex, MigrationGraph};
use crate::server::catalogue_storage::{CatalogueStorage, CatalogueStorageError};
#[cfg(any(test, feature = "embedded-server"))]
use jazz::catalogue::{CatalogueLens, CataloguePut};
use jazz::tools::ObjectId;
use jazz::tools::public_schema::{Schema, SchemaHash};
#[cfg(any(test, feature = "embedded-server"))]
use jazz::tools::schema_lens::Lens;
#[cfg(test)]
use jazz::tx::DurabilityTier;

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
        self.index.lock().ok()?.latest_bundle()
    }

    /// Every published permissions bundle, oldest first.
    pub(crate) fn published_bundles(&self) -> Vec<jazz::catalogue::CatalogueBundle> {
        self.index
            .lock()
            .map(|index| index.published_bundles().to_vec())
            .unwrap_or_default()
    }

    pub(crate) fn new(
        initial_schema: Option<Schema>,
        storage: CatalogueStorage,
    ) -> Result<Self, CatalogueError> {
        let index = CatalogueIndex::from_rows(&storage.read()?, initial_schema.as_ref());
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

    /// The catalogue as it reads now.
    pub(crate) fn index(&self) -> Result<CatalogueIndex, CatalogueError> {
        Ok(self
            .index
            .lock()
            .map_err(|_| CatalogueError::LockError)?
            .clone())
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
        let index = CatalogueIndex::from_rows(&storage.read()?, self.initial_schema.as_ref());
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

impl CatalogueStore for StoredCatalogue {
    fn migration_graph(&self) -> Result<MigrationGraph, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.migration_graph())
    }

    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.known_schema_hashes())
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.known_schema(schema_hash).cloned())
    }

    #[cfg(feature = "embedded-server")]
    fn known_lenses(&self) -> Result<Vec<Lens>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.known_lenses().cloned().collect())
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.schema_published_at(schema_hash))
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
