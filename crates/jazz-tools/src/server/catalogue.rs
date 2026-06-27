use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::AppId;
use crate::admin_catalogue_payload_codec::{
    decode_lens_transform, decode_permissions_bundle, decode_permissions_head, decode_schema,
    encode_lens_transform, encode_permissions_bundle, encode_permissions_head, encode_schema,
};
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::public_api::types::{Schema, SchemaHash, TableName, TablePolicies};
use crate::schema_lens::Lens;
use crate::server::catalogue_entry::CatalogueEntry;
use crate::server::catalogue_storage::{
    CatalogueStorage, CatalogueStorageError, DynCatalogueStorage,
};
#[cfg(test)]
use crate::sync::{ClientId, DurabilityTier};

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
pub(crate) struct PermissionsHeadSummary {
    pub schema_hash: SchemaHash,
    pub version: u64,
    pub parent_bundle_object_id: Option<ObjectId>,
    pub bundle_object_id: ObjectId,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CurrentPermissionsSummary {
    pub head: PermissionsHeadSummary,
    pub permissions: HashMap<TableName, TablePolicies>,
}

/// Errors from server-local catalogue operations.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum CatalogueError {
    QueryError(String),
    WriteError(String),
    NotFound,
    LockError,
}

impl std::fmt::Display for CatalogueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueError::QueryError(message) => write!(f, "Query error: {message}"),
            CatalogueError::WriteError(message) => write!(f, "Write error: {message}"),
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
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, CatalogueError>;
    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, CatalogueError>;
    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError>;
    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, CatalogueError>;
    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, CatalogueError>;
    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, CatalogueError>;
    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, CatalogueError>;
    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError>;
    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, CatalogueError>;
    fn flush(&self) -> Result<(), CatalogueError>;
    fn close(&self) -> Result<(), CatalogueError>;
}

pub(crate) struct StoredCatalogue {
    app_id: AppId,
    index: Mutex<CatalogueIndex>,
    #[cfg(test)]
    test_schema_branches: Mutex<Vec<String>>,
    #[cfg(test)]
    test_clients: Mutex<HashSet<ClientId>>,
    #[cfg(test)]
    test_local_durability_tiers: Mutex<HashSet<DurabilityTier>>,
    storage: Mutex<DynCatalogueStorage>,
}

impl StoredCatalogue {
    pub(crate) fn new(
        app_id: AppId,
        initial_schema: Option<Schema>,
        storage: DynCatalogueStorage,
    ) -> Self {
        let mut index = CatalogueIndex::from_storage(storage.as_ref(), app_id).unwrap_or_default();
        if let Some(schema) = initial_schema {
            index.add_schema(schema);
        }
        Self {
            app_id,
            index: Mutex::new(index),
            #[cfg(test)]
            test_schema_branches: Mutex::new(Vec::new()),
            #[cfg(test)]
            test_clients: Mutex::new(HashSet::new()),
            #[cfg(test)]
            test_local_durability_tiers: Mutex::new(HashSet::new()),
            storage: Mutex::new(storage),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_observability(
        app_id: AppId,
        initial_schema: Option<Schema>,
        storage: DynCatalogueStorage,
        schema_branches: Vec<String>,
        local_durability_tiers: HashSet<DurabilityTier>,
    ) -> Self {
        let store = Self::new(app_id, initial_schema, storage);
        *store
            .test_schema_branches
            .lock()
            .expect("schema branches lock") = schema_branches;
        *store
            .test_local_durability_tiers
            .lock()
            .expect("durability tiers lock") = local_durability_tiers;
        store
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn add_known_schema(&self, schema: Schema) -> Result<(), CatalogueError> {
        let mut index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        index.add_schema(schema);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn client_registered_for_test(
        &self,
        client_id: ClientId,
    ) -> Result<bool, CatalogueError> {
        let clients = self
            .test_clients
            .lock()
            .map_err(|_| CatalogueError::LockError)?;
        Ok(clients.contains(&client_id))
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
    pub(crate) fn add_client(
        &self,
        client_id: ClientId,
        _session: Option<crate::public_api::session::Session>,
    ) -> Result<(), CatalogueError> {
        let mut clients = self
            .test_clients
            .lock()
            .map_err(|_| CatalogueError::LockError)?;
        clients.insert(client_id);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ensure_client_with_session(
        &self,
        client_id: ClientId,
        _session: crate::public_api::session::Session,
    ) -> Result<(), CatalogueError> {
        self.add_client(client_id, None)
    }

    #[cfg(test)]
    pub(crate) fn ensure_client_as_backend(
        &self,
        client_id: ClientId,
    ) -> Result<(), CatalogueError> {
        self.add_client(client_id, None)
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
        let entries = storage.scan_catalogue_entries()?;
        Ok(entries.into_iter().find_map(|entry| {
            if entry.object_type() != Some(ObjectType::CatalogueLens.as_str()) {
                return None;
            }
            let source = entry
                .metadata
                .get(MetadataKey::SourceHash.as_str())
                .and_then(|raw| SchemaHash::from_hex(raw))?;
            let target = entry
                .metadata
                .get(MetadataKey::TargetHash.as_str())
                .and_then(|raw| SchemaHash::from_hex(raw))?;
            if source != source_hash || target != target_hash {
                return None;
            }
            let transform = decode_lens_transform(&entry.content).ok()?;
            Some(Lens::new(source, target, transform))
        }))
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
}

impl From<CatalogueStorageError> for CatalogueError {
    fn from(error: CatalogueStorageError) -> Self {
        CatalogueError::WriteError(error.to_string())
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Default)]
struct CatalogueIndex {
    schemas: HashMap<SchemaHash, Schema>,
    schema_published_at: HashMap<SchemaHash, u64>,
    lens_edges: HashSet<(SchemaHash, SchemaHash)>,
    permissions_head: Option<PermissionsHeadSummary>,
    permissions_bundles: HashMap<ObjectId, CurrentPermissionsSummary>,
}

impl CatalogueIndex {
    fn from_storage(storage: &dyn CatalogueStorage, app_id: AppId) -> Result<Self, CatalogueError> {
        let mut index = Self::default();
        for entry in storage.scan_catalogue_entries()? {
            if entry.metadata.get(MetadataKey::AppId.as_str()) != Some(&app_id.uuid().to_string()) {
                continue;
            }
            index.apply_entry(&entry);
        }
        Ok(index)
    }

    fn add_schema(&mut self, schema: Schema) -> SchemaHash {
        let hash = SchemaHash::compute(&schema);
        self.schemas.insert(hash, schema);
        hash
    }

    fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        let mut hashes = self.schemas.keys().copied().collect::<Vec<_>>();
        hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes
    }

    fn are_schema_hashes_connected(&self, from_hash: SchemaHash, to_hash: SchemaHash) -> bool {
        if !self.schemas.contains_key(&from_hash) || !self.schemas.contains_key(&to_hash) {
            return false;
        }
        let mut seen = HashSet::from([from_hash]);
        let mut queue = VecDeque::from([from_hash]);
        while let Some(current) = queue.pop_front() {
            if current == to_hash {
                return true;
            }
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
        false
    }

    fn current_permissions(&self) -> Option<CurrentPermissionsSummary> {
        let head = self.permissions_head?;
        self.permissions_bundles
            .get(&head.bundle_object_id)
            .cloned()
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
            .permissions_head
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

    fn apply_entry(&mut self, entry: &CatalogueEntry) {
        match entry.object_type() {
            Some(kind) if kind == ObjectType::CatalogueSchema.as_str() => {
                let Ok(schema) = decode_schema(&entry.content) else {
                    return;
                };
                if schema.is_empty() {
                    return;
                }
                let hash = entry
                    .metadata
                    .get(MetadataKey::SchemaHash.as_str())
                    .and_then(|raw| SchemaHash::from_hex(raw))
                    .unwrap_or_else(|| SchemaHash::compute(&schema));
                self.schemas.entry(hash).or_insert(schema);
                if let Some(published_at) = entry
                    .metadata
                    .get(MetadataKey::PublishedAt.as_str())
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    self.schema_published_at
                        .entry(hash)
                        .and_modify(|existing| *existing = (*existing).max(published_at))
                        .or_insert(published_at);
                }
            }
            Some(kind) if kind == ObjectType::CatalogueLens.as_str() => {
                let Some(source) = entry
                    .metadata
                    .get(MetadataKey::SourceHash.as_str())
                    .and_then(|raw| SchemaHash::from_hex(raw))
                else {
                    return;
                };
                let Some(target) = entry
                    .metadata
                    .get(MetadataKey::TargetHash.as_str())
                    .and_then(|raw| SchemaHash::from_hex(raw))
                else {
                    return;
                };
                let Ok(transform) = decode_lens_transform(&entry.content) else {
                    return;
                };
                let lens = Lens::new(source, target, transform);
                if !lens.is_draft() {
                    self.lens_edges.insert((source, target));
                }
            }
            Some(kind) if kind == ObjectType::CataloguePermissionsBundle.as_str() => {
                let Ok((schema_hash, version, parent_bundle_object_id, permissions)) =
                    decode_permissions_bundle(&entry.content)
                else {
                    return;
                };
                let head = PermissionsHeadSummary {
                    schema_hash,
                    version,
                    parent_bundle_object_id,
                    bundle_object_id: entry.object_id,
                };
                self.permissions_bundles.insert(
                    entry.object_id,
                    CurrentPermissionsSummary { head, permissions },
                );
            }
            Some(kind) if kind == ObjectType::CataloguePermissionsHead.as_str() => {
                let Ok((schema_hash, version, parent_bundle_object_id, bundle_object_id)) =
                    decode_permissions_head(&entry.content)
                else {
                    return;
                };
                let head = PermissionsHeadSummary {
                    schema_hash,
                    version,
                    parent_bundle_object_id,
                    bundle_object_id,
                };
                if self
                    .permissions_head
                    .is_none_or(|current| current.version <= head.version)
                {
                    self.permissions_head = Some(head);
                }
            }
            _ => {}
        }
    }
}

fn schema_entry(app_id: AppId, schema: Schema, published_at: u64) -> (SchemaHash, CatalogueEntry) {
    let hash = SchemaHash::compute(&schema);
    let mut metadata = catalogue_metadata(app_id, ObjectType::CatalogueSchema);
    metadata.insert(MetadataKey::SchemaHash.to_string(), hash.to_string());
    metadata.insert(
        MetadataKey::PublishedAt.to_string(),
        published_at.to_string(),
    );
    (
        hash,
        CatalogueEntry {
            object_id: hash.to_object_id(),
            metadata,
            content: encode_schema(&schema),
        },
    )
}

fn lens_entry(app_id: AppId, lens: &Lens) -> CatalogueEntry {
    let mut metadata = catalogue_metadata(app_id, ObjectType::CatalogueLens);
    metadata.insert(
        MetadataKey::SourceHash.to_string(),
        lens.source_hash.to_string(),
    );
    metadata.insert(
        MetadataKey::TargetHash.to_string(),
        lens.target_hash.to_string(),
    );
    CatalogueEntry {
        object_id: lens.object_id(),
        metadata,
        content: encode_lens_transform(&lens.forward),
    }
}

fn permissions_bundle_object_id(
    app_id: AppId,
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: &HashMap<TableName, TablePolicies>,
) -> ObjectId {
    let mut identity = format!("jazz-catalogue-permissions-bundle:{}:", app_id.uuid()).into_bytes();
    identity.extend_from_slice(&encode_permissions_bundle(
        schema_hash,
        version,
        parent_bundle_object_id,
        permissions,
    ));
    ObjectId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_DNS, &identity))
}

fn permissions_head_object_id(app_id: AppId) -> ObjectId {
    ObjectId::from_uuid(Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        format!("jazz-catalogue-permissions-head:{}", app_id.uuid()).as_bytes(),
    ))
}

fn catalogue_metadata(app_id: AppId, object_type: ObjectType) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Type.to_string(), object_type.to_string());
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata
}

impl CatalogueStore for StoredCatalogue {
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.known_schema_hashes())
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.schemas.get(schema_hash).cloned())
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.schema_published_at.get(schema_hash).copied())
    }

    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.are_schema_hashes_connected(from_hash, to_hash))
    }

    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, CatalogueError> {
        let published_at = unix_timestamp_millis();
        let (schema_hash, entry) = schema_entry(self.app_id, schema, published_at);
        let mut storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        storage.upsert_catalogue_entry(&entry)?;
        let object_id = entry.object_id;
        let mut index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        index.apply_entry(&entry);
        index.schema_published_at.insert(schema_hash, published_at);
        Ok(object_id)
    }

    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.permissions_head)
    }

    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, CatalogueError> {
        let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        Ok(index.current_permissions())
    }

    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError> {
        let (head, bundle_entry, head_entry) = {
            let index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
            let current_parent_bundle_object_id =
                index.permissions_head.map(|head| head.bundle_object_id);
            if current_parent_bundle_object_id != expected_parent_bundle_object_id {
                return Err(CatalogueError::WriteError(format!(
                    "stale permissions parent: expected {:?}, current {:?}",
                    expected_parent_bundle_object_id, current_parent_bundle_object_id
                )));
            }

            if let Some(current) = index.current_permissions()
                && current.head.schema_hash == schema_hash
                && current.permissions == permissions
            {
                return Ok(Some(permissions_head_object_id(self.app_id)));
            }

            let version = index
                .permissions_head
                .map(|head| head.version + 1)
                .unwrap_or(1);
            let bundle_object_id = permissions_bundle_object_id(
                self.app_id,
                schema_hash,
                version,
                current_parent_bundle_object_id,
                &permissions,
            );
            let head = PermissionsHeadSummary {
                schema_hash,
                version,
                parent_bundle_object_id: current_parent_bundle_object_id,
                bundle_object_id,
            };
            let bundle_entry = CatalogueEntry {
                object_id: bundle_object_id,
                metadata: catalogue_metadata(self.app_id, ObjectType::CataloguePermissionsBundle),
                content: encode_permissions_bundle(
                    schema_hash,
                    version,
                    current_parent_bundle_object_id,
                    &permissions,
                ),
            };
            let head_entry = CatalogueEntry {
                object_id: permissions_head_object_id(self.app_id),
                metadata: catalogue_metadata(self.app_id, ObjectType::CataloguePermissionsHead),
                content: encode_permissions_head(
                    schema_hash,
                    version,
                    current_parent_bundle_object_id,
                    bundle_object_id,
                ),
            };
            (head, bundle_entry, head_entry)
        };

        let mut storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        storage.upsert_catalogue_entry(&bundle_entry)?;
        storage.upsert_catalogue_entry(&head_entry)?;
        let mut index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        index.apply_entry(&bundle_entry);
        index.permissions_head = Some(head);
        Ok(Some(head_entry.object_id))
    }

    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, CatalogueError> {
        if lens.is_draft() {
            return Err(CatalogueError::WriteError(
                "cannot publish draft lens".to_string(),
            ));
        }
        let entry = lens_entry(self.app_id, lens);
        let mut storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        storage.upsert_catalogue_entry(&entry)?;
        let mut index = self.index.lock().map_err(|_| CatalogueError::LockError)?;
        index.apply_entry(&entry);
        Ok(entry.object_id)
    }

    fn flush(&self) -> Result<(), CatalogueError> {
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        storage.flush()?;
        storage.flush_wal()?;
        Ok(())
    }

    fn close(&self) -> Result<(), CatalogueError> {
        let storage = self.storage.lock().map_err(|_| CatalogueError::LockError)?;
        storage.flush()?;
        storage.flush_wal()?;
        storage.close()?;
        Ok(())
    }
}

impl ServerCatalogue {
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

    pub(crate) fn schema_published_at(
        &self,
        store: &impl CatalogueStore,
        schema_hash: &SchemaHash,
    ) -> Result<Option<u64>, CatalogueError> {
        store.schema_published_at(schema_hash)
    }

    pub(crate) fn are_schema_hashes_connected(
        &self,
        store: &impl CatalogueStore,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, CatalogueError> {
        store.are_schema_hashes_connected(from_hash, to_hash)
    }

    pub(crate) fn publish_schema(
        &self,
        store: &impl CatalogueStore,
        schema: Schema,
    ) -> Result<ObjectId, CatalogueError> {
        store.publish_schema(schema)
    }

    pub(crate) fn current_permissions_head(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<PermissionsHeadSummary>, CatalogueError> {
        store.current_permissions_head()
    }

    pub(crate) fn current_permissions(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<CurrentPermissionsSummary>, CatalogueError> {
        store.current_permissions()
    }

    pub(crate) fn publish_permissions_bundle(
        &self,
        store: &impl CatalogueStore,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, CatalogueError> {
        store.publish_permissions_bundle(schema_hash, permissions, expected_parent_bundle_object_id)
    }

    pub(crate) fn publish_lens(
        &self,
        store: &impl CatalogueStore,
        lens: &Lens,
    ) -> Result<ObjectId, CatalogueError> {
        store.publish_lens(lens)
    }

    pub(crate) fn flush(&self, store: &impl CatalogueStore) -> Result<(), CatalogueError> {
        store.flush()
    }

    pub(crate) fn close(&self, store: &impl CatalogueStore) -> Result<(), CatalogueError> {
        store.close()
    }
}
