use std::collections::HashMap;

use crate::object::ObjectId;
use crate::query_manager::types::{Schema, SchemaHash, TableName, TablePolicies};
use crate::runtime_tokio::{RuntimeError, TokioRuntime};
use crate::schema_manager::Lens;
use crate::schema_manager::manager::{CurrentPermissionsSummary, PermissionsHeadSummary};
use crate::server::DynStorage;

/// Server-local catalogue facade.
///
/// This is intentionally a thin wrapper over the existing Tokio runtime and
/// schema manager. Storage authority still lives in the runtime; this facade
/// only gives server/admin routes one place to call for catalogue operations.
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
}

impl CatalogueStore for TokioRuntime<DynStorage> {
    fn known_schema_hashes(&self) -> Result<Vec<SchemaHash>, RuntimeError> {
        self.known_schema_hashes()
    }

    fn known_schema(&self, schema_hash: &SchemaHash) -> Result<Option<Schema>, RuntimeError> {
        self.known_schema(schema_hash)
    }

    fn schema_published_at(&self, schema_hash: &SchemaHash) -> Result<Option<u64>, RuntimeError> {
        self.schema_published_at(schema_hash)
    }

    fn are_schema_hashes_connected(
        &self,
        from_hash: SchemaHash,
        to_hash: SchemaHash,
    ) -> Result<bool, RuntimeError> {
        self.with_schema_manager(|schema_manager| {
            schema_manager.are_schema_hashes_connected(from_hash, to_hash)
        })
    }

    fn publish_schema(&self, schema: Schema) -> Result<ObjectId, RuntimeError> {
        self.publish_schema(schema)
    }

    fn current_permissions_head(&self) -> Result<Option<PermissionsHeadSummary>, RuntimeError> {
        self.current_permissions_head()
    }

    fn current_permissions(&self) -> Result<Option<CurrentPermissionsSummary>, RuntimeError> {
        self.current_permissions()
    }

    fn publish_permissions_bundle(
        &self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, RuntimeError> {
        self.publish_permissions_bundle(schema_hash, permissions, expected_parent_bundle_object_id)
    }

    fn publish_lens(&self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        self.publish_lens(lens)
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
}
