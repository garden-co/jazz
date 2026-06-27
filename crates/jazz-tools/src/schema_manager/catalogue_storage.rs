use std::{error::Error, fmt};

use crate::catalogue::CatalogueEntry;
use crate::object::ObjectId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogueStorageError {
    message: String,
}

impl CatalogueStorageError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CatalogueStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for CatalogueStorageError {}

/// Narrow storage surface used by schema-manager catalogue persistence.
pub trait SchemaManagerCatalogueStorage {
    fn storage_cache_namespace(&self) -> usize;

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, CatalogueStorageError>;

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, CatalogueStorageError>;

    fn upsert_catalogue_entry(
        &mut self,
        entry: &CatalogueEntry,
    ) -> Result<(), CatalogueStorageError>;
}

impl<T: crate::storage::Storage + ?Sized> SchemaManagerCatalogueStorage for T {
    fn storage_cache_namespace(&self) -> usize {
        crate::storage::Storage::storage_cache_namespace(self)
    }

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, CatalogueStorageError> {
        crate::storage::Storage::load_catalogue_entry(self, object_id)
            .map_err(|error| CatalogueStorageError::new(error.to_string()))
    }

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, CatalogueStorageError> {
        crate::storage::Storage::scan_catalogue_entries(self)
            .map_err(|error| CatalogueStorageError::new(error.to_string()))
    }

    fn upsert_catalogue_entry(
        &mut self,
        entry: &CatalogueEntry,
    ) -> Result<(), CatalogueStorageError> {
        crate::storage::Storage::upsert_catalogue_entry(self, entry)
            .map_err(|error| CatalogueStorageError::new(error.to_string()))
    }
}
