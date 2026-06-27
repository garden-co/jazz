use std::collections::HashMap;
use std::sync::Mutex;

use crate::object::ObjectId;
use crate::query_manager::types::{Schema, SchemaHash, TableName, TablePolicies};
use crate::runtime_tokio::RuntimeError;
use crate::schema_manager::manager::{CurrentPermissionsSummary, PermissionsHeadSummary};
use crate::schema_manager::{Lens, SchemaManager};
use crate::server::DynStorage;
use crate::storage::StorageError;
#[cfg(test)]
use crate::sync_manager::{ClientId, InboxEntry, QueryPropagation, SyncPayload};

/// Server-local catalogue facade.
///
/// This is intentionally a thin wrapper over the direct catalogue store and
/// schema manager while catalogue indexing is being simplified.
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
        Self {
            schema_manager: Mutex::new(schema_manager),
            storage: Mutex::new(storage),
            #[cfg(test)]
            test_query_subscriptions: Mutex::new(Vec::new()),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn add_known_schema(&self, schema: Schema) -> Result<(), RuntimeError> {
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        schema_manager.add_known_schema(schema);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub(crate) fn with_schema_manager<R>(
        &self,
        f: impl FnOnce(&SchemaManager) -> R,
    ) -> Result<R, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(f(&schema_manager))
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
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.persist_schema(&mut *storage))
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

impl CatalogueStore for DirectCatalogueStore {
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.known_schema_hashes())
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.get_known_schema(schema_hash).cloned())
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.schema_published_at(schema_hash))
    }

    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.are_schema_hashes_connected(from_hash, to_hash))
    }

    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, RuntimeError> {
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        schema_manager.add_known_schema(schema.clone());
        Ok(schema_manager.persist_schema_object(&mut *storage, &schema))
    }

    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.current_permissions_head())
    }

    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, RuntimeError> {
        let schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        Ok(schema_manager.current_permissions())
    }

    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError> {
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        schema_manager
            .publish_permissions_bundle(
                &mut *storage,
                schema_hash,
                permissions,
                expected_parent_bundle_object_id,
            )
            .map_err(|error| RuntimeError::WriteError(error.to_string()))
    }

    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        let mut schema_manager = self
            .schema_manager
            .lock()
            .map_err(|_| RuntimeError::LockError)?;
        let mut storage = self.storage.lock().map_err(|_| RuntimeError::LockError)?;
        schema_manager
            .publish_lens(&mut *storage, lens)
            .map_err(|error| RuntimeError::WriteError(error.to_string()))
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
