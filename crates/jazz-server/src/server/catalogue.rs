mod deployment;
pub(crate) use deployment::DeploymentResponse;

#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::server::catalogue_storage::{CatalogueStorage, CatalogueStorageError};
#[cfg(test)]
use crate::server::permissions_validation::validate_permissions_relations;
use jazz::catalogue::CatalogueRows;
#[cfg(any(test, feature = "embedded-server"))]
use jazz::catalogue::{CatalogueLens, CataloguePut};
use jazz::tools::AppId;
use jazz::tools::ObjectId;
use jazz::tools::public_schema::{Schema, SchemaHash, TableName, TablePolicies};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ActiveSchemaSummary {
    pub schema_hash: SchemaHash,
    pub version: u64,
    pub parent_bundle_object_id: Option<ObjectId>,
    pub bundle_object_id: ObjectId,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ActiveSchema {
    pub summary: ActiveSchemaSummary,
    pub permissions: HashMap<TableName, TablePolicies>,
}

/// HTTP graph inventory, captured under one catalogue index lock.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MigrationGraph {
    active_schema_hash: Option<String>,
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
    fn known_lenses(&self) -> Result<Vec<Lens>, CatalogueError>;
    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError>;
    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, CatalogueError>;
    fn active_schema_summary(&self) -> Result<Option<ActiveSchemaSummary>, CatalogueError>;
    fn active_schema(&self) -> Result<Option<ActiveSchema>, CatalogueError>;
    #[cfg(test)]
    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError>;
    #[cfg(any(test, feature = "embedded-server"))]
    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, CatalogueError>;
}

pub(crate) struct StoredCatalogue {
    app_id: AppId,
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
    pub(crate) fn new(
        app_id: AppId,
        initial_schema: Option<Schema>,
        storage: CatalogueStorage,
    ) -> Result<Self, CatalogueError> {
        let index = CatalogueIndex::from_rows(app_id, &storage.read()?, initial_schema.as_ref())?;
        Ok(Self {
            app_id,
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
        app_id: AppId,
        initial_schema: Option<Schema>,
        storage: CatalogueStorage,
        schema_branches: Vec<String>,
        local_durability_tiers: HashSet<DurabilityTier>,
    ) -> Result<Self, CatalogueError> {
        let store = Self::new(app_id, initial_schema, storage)?;
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
        let index =
            CatalogueIndex::from_rows(self.app_id, &storage.read()?, self.initial_schema.as_ref())?;
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
    /// Every permissions revision, oldest first; the last is current.
    revisions: Vec<ActiveSchema>,
}

impl CatalogueIndex {
    fn from_rows(
        app_id: AppId,
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
        let mut parent = None;
        for revision in &rows.permissions {
            let permissions = revision
                .permissions
                .iter()
                .map(|(name, policies)| (*name, policies.clone()))
                .collect::<HashMap<_, _>>();
            let bundle_object_id = permissions_bundle_object_id(
                app_id,
                revision.schema,
                revision.revision,
                parent,
                &permissions,
            )?;
            index.revisions.push(ActiveSchema {
                summary: ActiveSchemaSummary {
                    schema_hash: revision.schema,
                    version: revision.revision,
                    parent_bundle_object_id: parent,
                    bundle_object_id,
                },
                permissions,
            });
            parent = Some(bundle_object_id);
        }
        if let Some(schema) = initial_schema {
            index.add_schema(schema.clone());
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

    fn active_schema(&self) -> Option<ActiveSchema> {
        self.revisions.last().cloned()
    }

    fn active_schema_summary(&self) -> Option<ActiveSchemaSummary> {
        self.revisions.last().map(|active| active.summary)
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
        let active_permissions_hash = self
            .active_schema_summary()
            .map(|head| head.schema_hash)
            .or_else(|| self.known_schema_hashes().into_iter().next());
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

/// A permissions revision's identity, derived from its content and its
/// parent's: clients name the head they expect by it.
fn permissions_bundle_object_id(
    app_id: AppId,
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Result<ObjectId, CatalogueError> {
    let permissions = permissions
        .iter()
        .map(|(name, policies)| (*name, policies.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let content = jazz::catalogue::canonical_json(&(
        schema_hash.to_string(),
        version,
        parent_bundle_object_id.map(|parent| parent.to_string()),
        permissions,
    ))
    .map_err(|error| CatalogueError::WriteError(error.to_string()))?;
    let identity = format!(
        "jazz-catalogue-permissions-bundle:{}:{content}",
        app_id.uuid()
    );
    Ok(ObjectId::from_uuid(Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        identity.as_bytes(),
    )))
}

#[cfg(test)]
fn permissions_head_object_id(app_id: AppId) -> ObjectId {
    ObjectId::from_uuid(Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        format!("jazz-catalogue-permissions-head:{}", app_id.uuid()).as_bytes(),
    ))
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
            active_schema_hash: index
                .active_schema_summary()
                .map(|active| active.schema_hash.to_string()),
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

    fn active_schema_summary(&self) -> Result<Option<ActiveSchemaSummary>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.active_schema_summary())
    }

    fn active_schema(&self) -> Result<Option<ActiveSchema>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.active_schema())
    }

    #[cfg(test)]
    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError> {
        validate_permissions_relations(&permissions)
            .map_err(|error| CatalogueError::WriteError(error.to_string()))?;
        // Catalogue mutations take storage before the in-memory index, so
        // parent validation and the write are linearizable for callers
        // sharing this StoredCatalogue.
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        let current = self
            .index
            .lock()
            .map_err(|_| CatalogueError::LockError)?
            .active_schema();
        let current_parent_bundle_object_id =
            current.as_ref().map(|head| head.summary.bundle_object_id);
        if current_parent_bundle_object_id != expected_parent_bundle_object_id {
            return Err(CatalogueError::WriteError(format!(
                "stale permissions parent: expected {:?}, current {:?}",
                expected_parent_bundle_object_id, current_parent_bundle_object_id
            )));
        }
        if let Some(current) = &current
            && current.summary.schema_hash == schema_hash
            && current.permissions == permissions
        {
            return Ok(Some(permissions_head_object_id(self.app_id)));
        }
        let revision = current.map(|head| head.summary.version + 1).unwrap_or(1);
        self.write(
            &storage,
            CataloguePut {
                permissions: vec![(
                    revision,
                    schema_hash,
                    permissions.into_iter().collect(),
                    unix_timestamp_millis(),
                )],
                ..CataloguePut::default()
            },
        )?;
        Ok(Some(permissions_head_object_id(self.app_id)))
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

    pub(crate) fn active_schema_summary(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<ActiveSchemaSummary>, CatalogueError> {
        store.active_schema_summary()
    }

    pub(crate) fn active_schema(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<ActiveSchema>, CatalogueError> {
        store.active_schema()
    }

    #[cfg(test)]
    pub(crate) fn publish_permissions_bundle(
        &self,
        store: &impl CatalogueStore,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError> {
        store.publish_permissions_bundle(schema_hash, permissions, expected_parent_bundle_object_id)
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
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use jazz::tools::public_schema::{
        PolicyExpr, RelationExpr, RelationUnionArm, SchemaBuilder, TableName, TablePolicies,
    };

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
        let app_id = jazz::tools::AppId::from_name("empty-schema-catalogue");
        let schema = SchemaBuilder::new().build();
        let store = StoredCatalogue::new(app_id, None, storage()).unwrap();
        store.publish_schema(schema.clone()).unwrap();
        let hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let reopened =
            StoredCatalogue::new(app_id, None, store.storage.into_inner().unwrap()).unwrap();
        assert_eq!(reopened.known_schema(&hash).unwrap(), Some(schema));
        assert!(reopened.schema_published_at(&hash).unwrap().is_some());
    }

    #[test]
    fn concurrent_permissions_publications_compare_and_swap_the_parent() {
        const PUBLISHERS: usize = 8;
        let schema = SchemaBuilder::new().build();
        let schema_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let store = Arc::new(
            StoredCatalogue::new(
                jazz::tools::AppId::from_name("permissions-publication-race"),
                Some(schema),
                storage(),
            )
            .expect("catalogue"),
        );
        let storage_guard = store.storage.lock().expect("storage lock");
        let start = Arc::new(Barrier::new(PUBLISHERS + 1));
        let mut publishers = Vec::new();
        for _ in 0..PUBLISHERS {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            publishers.push(std::thread::spawn(move || {
                start.wait();
                store.publish_permissions_bundle(
                    schema_hash,
                    std::collections::HashMap::from([(
                        jazz::tools::public_schema::TableName::new("todos"),
                        TablePolicies::default(),
                    )]),
                    None,
                )
            }));
        }

        start.wait();
        std::thread::sleep(Duration::from_millis(50));
        drop(storage_guard);

        let results = publishers
            .into_iter()
            .map(|publisher| publisher.join().expect("publisher thread"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(super::CatalogueError::WriteError(message))
                            if message.starts_with("stale permissions parent")
                    )
                })
                .count(),
            PUBLISHERS - 1
        );
    }

    #[test]
    fn malformed_relation_union_labels_fail_before_catalogue_publication() {
        let schema = SchemaBuilder::new().build();
        let schema_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let store = StoredCatalogue::new(
            jazz::tools::AppId::from_name("invalid-union-labels"),
            Some(schema),
            storage(),
        )
        .expect("catalogue");
        let leaf = || RelationExpr::TableScan {
            table: String::from("todos"),
            alias: None,
        };
        for labels in [
            vec!["".to_owned()],
            vec!["duplicate".to_owned(), "duplicate".to_owned()],
            vec!["contains\0nul".to_owned()],
            vec!["x".repeat(4097)],
        ] {
            let permissions = std::collections::HashMap::from([(
                TableName::new("todos"),
                TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                    rel: RelationExpr::Union {
                        inputs: labels
                            .into_iter()
                            .map(|label| RelationUnionArm {
                                label,
                                input: leaf(),
                            })
                            .collect(),
                    },
                }),
            )]);
            assert!(matches!(
                store.publish_permissions_bundle(schema_hash, permissions, None),
                Err(super::CatalogueError::WriteError(_))
            ));
            assert!(store.active_schema_summary().expect("read head").is_none());
            assert!(store.active_schema().expect("read permissions").is_none());
        }
    }
}
