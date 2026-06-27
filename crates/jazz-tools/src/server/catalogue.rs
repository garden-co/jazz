use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::catalogue::CatalogueEntry;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::query_manager::types::{Schema, SchemaHash, TableName, TablePolicies};
use crate::runtime_tokio::RuntimeError;
use crate::schema_manager::encoding::{
    decode_lens_transform, decode_permissions, decode_permissions_bundle, decode_permissions_head,
    decode_schema, encode_lens_transform, encode_permissions_bundle, encode_permissions_head,
    encode_schema,
};
use crate::schema_manager::manager::{CurrentPermissionsSummary, PermissionsHeadSummary};
use crate::schema_manager::{AppId, Lens, SchemaManager};
use crate::server::DynStorage;
use crate::storage::StorageError;
#[cfg(test)]
use crate::sync_manager::{ClientId, InboxEntry, QueryPropagation, SyncPayload};

/// Server-local catalogue facade.
///
/// This is intentionally a thin wrapper over the direct catalogue store.
/// It may read and write admin catalogue metadata only: schemas, permissions,
/// and lenses. Production websocket sync, row storage, query execution, and
/// client lifecycle semantics must stay on the direct `CoreServer` path.
#[derive(Debug, Default)]
pub struct ServerCatalogue;

pub(crate) trait CatalogueStore {
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, RuntimeError>;
    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, RuntimeError>;
    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, RuntimeError>;
    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, RuntimeError>;
    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, RuntimeError>;
    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, RuntimeError>;
    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, RuntimeError>;
    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError>;
    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, RuntimeError>;
    fn flush(&self) -> Result<(), RuntimeError>;
    fn close(&self) -> Result<(), RuntimeError>;
}

pub(crate) struct DirectCatalogueStore {
    app_id: AppId,
    index: Mutex<CatalogueIndex>,
    #[cfg(test)]
    schema_manager: Mutex<SchemaManager>,
    storage: Mutex<DynStorage>,
    #[cfg(test)]
    test_query_subscriptions: Mutex<
        Vec<(
            String,
            Vec<String>,
            QueryPropagation,
            crate::query_manager::query::Query,
        )>,
    >,
}

impl DirectCatalogueStore {
    pub(crate) fn new(schema_manager: SchemaManager, storage: DynStorage) -> Self {
        let app_id = schema_manager.app_id();
        let mut index = CatalogueIndex::from_storage(storage.as_ref(), app_id).unwrap_or_default();
        for hash in schema_manager.known_schema_hashes() {
            if let Some(schema) = schema_manager.get_known_schema(&hash) {
                index.schemas.entry(hash).or_insert_with(|| schema.clone());
            }
            if let Some(published_at) = schema_manager.schema_published_at(&hash) {
                index.schema_published_at.insert(hash, published_at);
            }
        }
        Self {
            app_id,
            index: Mutex::new(index),
            #[cfg(test)]
            schema_manager: Mutex::new(schema_manager),
            storage: Mutex::new(storage),
            #[cfg(test)]
            test_query_subscriptions: Mutex::new(Vec::new()),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn add_known_schema(&self, schema: Schema) -> Result<(), RuntimeError> {
        let mut index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        index.add_schema(schema);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn with_schema_manager<R>(
        &self,
        f: impl FnOnce(&SchemaManager) -> R,
    ) -> Result<R, RuntimeError> {
        #[cfg(test)]
        {
            let schema_manager = self
                .schema_manager
                .lock()
                .map_err(|_| RuntimeError::LockError)?;
            Ok(f(&schema_manager))
        }
        #[cfg(not(test))]
        {
            let _ = f;
            Err(RuntimeError::WriteError(
                "schema manager is not available in direct catalogue store".to_string(),
            ))
        }
    }

    #[cfg(test)]
    pub(crate) fn with_sync_manager<R>(
        &self,
        f: impl FnOnce(&crate::sync_manager::SyncManager) -> R,
    ) -> Result<R, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(f(schema_manager.query_manager().sync_manager()))
    }

    #[cfg(test)]
    pub(crate) fn add_client(
        &self,
        client_id: ClientId,
        _session: Option<crate::query_manager::session::Session>,
    ) -> Result<(), RuntimeError> {
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_id);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ensure_client_with_session(
        &self,
        client_id: ClientId,
        _session: crate::query_manager::session::Session,
    ) -> Result<(), RuntimeError> {
        self.add_client(client_id, None)
    }

    #[cfg(test)]
    pub(crate) fn ensure_client_as_backend(&self, client_id: ClientId) -> Result<(), RuntimeError> {
        self.add_client(client_id, None)
    }

    #[cfg(test)]
    pub(crate) fn push_sync_inbox(&self, entry: InboxEntry) -> Result<(), RuntimeError> {
        if let SyncPayload::QuerySubscription {
            query, propagation, ..
        } = entry.payload
        {
            let schema_manager = self
                .schema_manager
                .lock()
                .map_err(|_| RuntimeError::LockError)?;
            let branches = schema_manager
                .all_branches()
                .into_iter()
                .map(|branch| branch.as_str().to_string())
                .collect();
            drop(schema_manager);
            let query_json = serde_json::to_string(&query)
                .unwrap_or_else(|_| "{\"error\":\"query serialization failed\"}".to_string());
            self.test_query_subscriptions
                .lock()
                .map_err(|_| RuntimeError::LockError)?
                .push((query_json, branches, propagation, *query));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn flush(&self) -> Result<(), RuntimeError> {
        <Self as CatalogueStore>::flush(self)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn persist_schema(&self) -> Result<ObjectId, RuntimeError> {
        let hash = {
            let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
            index
                .known_schema_hashes()
                .into_iter()
                .next()
                .ok_or_else(|| RuntimeError::WriteError("no known schema to persist".to_string()))?
        };
        let schema = self
            .known_schema(&hash)?
            .ok_or_else(|| RuntimeError::WriteError("known schema disappeared".to_string()))?;
        <Self as CatalogueStore>::publish_schema(self, schema)
    }

    #[cfg(test)]
    pub(crate) fn stored_lens_for_test(
        &self,
        source_hash: SchemaHash,
        target_hash: SchemaHash,
    ) -> Result<Option<Lens>, RuntimeError> {
        let storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        let entries = storage.scan_catalogue_entries().map_err(storage_error)?;
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

    pub(crate) fn server_subscription_telemetry(
        &self,
    ) -> Vec<crate::query_manager::manager::ServerSubscriptionTelemetryGroup> {
        #[cfg(test)]
        {
            use std::collections::HashMap;

            let mut groups = HashMap::<
                (String, Vec<String>, String),
                crate::query_manager::manager::ServerSubscriptionTelemetryGroup,
            >::new();
            let subscriptions = self.test_query_subscriptions.lock().unwrap();
            for (query_json, branches, propagation, query) in subscriptions.iter() {
                let propagation_label = match propagation {
                    QueryPropagation::Full => "full",
                    QueryPropagation::LocalOnly => "local-only",
                };
                let key = (
                    query_json.clone(),
                    branches.clone(),
                    propagation_label.to_string(),
                );
                let group_index = groups.len();
                groups
                    .entry(key)
                    .and_modify(|group| group.count += 1)
                    .or_insert_with(|| {
                        let group_key =
                            format!("{propagation_label}:{}:{group_index}", query.table.as_str());
                        crate::query_manager::manager::ServerSubscriptionTelemetryGroup {
                            group_key,
                            count: 1,
                            table: query.table.as_str().to_string(),
                            query: query_json.clone(),
                            branches: branches.clone(),
                            propagation: *propagation,
                        }
                    });
            }
            return groups.into_values().collect();
        }

        #[cfg(not(test))]
        Vec::new()
    }
}

fn storage_error(error: StorageError) -> RuntimeError {
    RuntimeError::WriteError(error.to_string())
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
    fn from_storage(
        storage: &dyn crate::storage::Storage,
        app_id: AppId,
    ) -> Result<Self, RuntimeError> {
        let mut index = Self::default();
        for entry in storage.scan_catalogue_entries().map_err(storage_error)? {
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
            Some(kind) if kind == ObjectType::CataloguePermissions.as_str() => {
                let Some(schema_hash) = entry
                    .metadata
                    .get(MetadataKey::SchemaHash.as_str())
                    .and_then(|raw| SchemaHash::from_hex(raw))
                else {
                    return;
                };
                let Ok(permissions) = decode_permissions(&entry.content) else {
                    return;
                };
                let head = PermissionsHeadSummary {
                    schema_hash,
                    version: 1,
                    parent_bundle_object_id: None,
                    bundle_object_id: entry.object_id,
                };
                self.permissions_head = Some(head);
                self.permissions_bundles.insert(
                    entry.object_id,
                    CurrentPermissionsSummary { head, permissions },
                );
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
    SchemaManager::permissions_head_object_id_for(app_id)
}

fn catalogue_metadata(app_id: AppId, object_type: ObjectType) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Type.to_string(), object_type.to_string());
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata
}

impl CatalogueStore for DirectCatalogueStore {
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.known_schema_hashes())
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.schemas.get(schema_hash).cloned())
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.schema_published_at.get(schema_hash).copied())
    }

    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.are_schema_hashes_connected(from_hash, to_hash))
    }

    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, RuntimeError> {
        let published_at = unix_timestamp_millis();
        let (schema_hash, entry) = schema_entry(self.app_id, schema, published_at);
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        storage
            .upsert_catalogue_entry(&entry)
            .map_err(storage_error)?;
        let object_id = entry.object_id;
        let mut index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        index.apply_entry(&entry);
        index.schema_published_at.insert(schema_hash, published_at);
        Ok(object_id)
    }

    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.permissions_head)
    }

    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, RuntimeError> {
        let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(index.current_permissions())
    }

    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError> {
        let (head, bundle_entry, head_entry) = {
            let index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
            let current_parent_bundle_object_id =
                index.permissions_head.map(|head| head.bundle_object_id);
            if current_parent_bundle_object_id != expected_parent_bundle_object_id {
                return Err(RuntimeError::WriteError(format!(
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

        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        storage
            .upsert_catalogue_entry(&bundle_entry)
            .map_err(storage_error)?;
        storage
            .upsert_catalogue_entry(&head_entry)
            .map_err(storage_error)?;
        let mut index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        index.apply_entry(&bundle_entry);
        index.permissions_head = Some(head);
        Ok(Some(head_entry.object_id))
    }

    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        if lens.is_draft() {
            return Err(RuntimeError::WriteError(
                "cannot publish draft lens".to_string(),
            ));
        }
        let entry = lens_entry(self.app_id, lens);
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        storage
            .upsert_catalogue_entry(&entry)
            .map_err(storage_error)?;
        let mut index = self.index.lock().map_err(|_| RuntimeError::LockError)?;
        index.apply_entry(&entry);
        Ok(entry.object_id)
    }

    fn flush(&self) -> Result<(), RuntimeError> {
        let storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        storage.flush().map_err(storage_error)?;
        storage.flush_wal().map_err(storage_error)
    }

    fn close(&self) -> Result<(), RuntimeError> {
        let storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        storage.flush().map_err(storage_error)?;
        storage.flush_wal().map_err(storage_error)?;
        storage.close().map_err(storage_error)
    }
}

impl ServerCatalogue {
    pub(crate) fn known_schema_hashes(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Vec<SchemaHash>, RuntimeError> {
        store.known_schema_hashes()
    }

    pub(crate) fn known_schema(
        &self,
        store: &impl CatalogueStore,
        schema_hash: &SchemaHash,
    ) -> Result<Option<Schema>, RuntimeError> {
        store.known_schema(schema_hash)
    }

    pub(crate) fn schema_published_at(
        &self,
        store: &impl CatalogueStore,
        schema_hash: &SchemaHash,
    ) -> Result<Option<u64>, RuntimeError> {
        store.schema_published_at(schema_hash)
    }

    pub(crate) fn are_schema_hashes_connected(
        &self,
        store: &impl CatalogueStore,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, RuntimeError> {
        store.are_schema_hashes_connected(from_hash, to_hash)
    }

    pub(crate) fn publish_schema(
        &self,
        store: &impl CatalogueStore,
        schema: Schema,
    ) -> Result<ObjectId, RuntimeError> {
        store.publish_schema(schema)
    }

    pub(crate) fn current_permissions_head(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<PermissionsHeadSummary>, RuntimeError> {
        store.current_permissions_head()
    }

    pub(crate) fn current_permissions(
        &self,
        store: &impl CatalogueStore,
    ) -> Result<Option<CurrentPermissionsSummary>, RuntimeError> {
        store.current_permissions()
    }

    pub(crate) fn publish_permissions_bundle(
        &self,
        store: &impl CatalogueStore,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError> {
        store.publish_permissions_bundle(schema_hash, permissions, expected_parent_bundle_object_id)
    }

    pub(crate) fn publish_lens(
        &self,
        store: &impl CatalogueStore,
        lens: &Lens,
    ) -> Result<ObjectId, RuntimeError> {
        store.publish_lens(lens)
    }

    pub(crate) fn flush(&self, store: &impl CatalogueStore) -> Result<(), RuntimeError> {
        store.flush()
    }

    pub(crate) fn close(&self, store: &impl CatalogueStore) -> Result<(), RuntimeError> {
        store.close()
    }
}
