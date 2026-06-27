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

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use crate::catalogue::CatalogueEntry;
    use crate::object::ObjectId;

    use super::{CatalogueStorageError, SchemaManagerCatalogueStorage};

    #[derive(Debug, Default)]
    pub(crate) struct CatalogueMemoryStorage {
        entries: HashMap<ObjectId, CatalogueEntry>,
        cache_namespace: usize,
    }

    impl CatalogueMemoryStorage {
        pub(crate) fn new() -> Self {
            Self {
                entries: HashMap::new(),
                cache_namespace: 1,
            }
        }
    }

    impl SchemaManagerCatalogueStorage for CatalogueMemoryStorage {
        fn storage_cache_namespace(&self) -> usize {
            self.cache_namespace
        }

        fn load_catalogue_entry(
            &self,
            object_id: ObjectId,
        ) -> Result<Option<CatalogueEntry>, CatalogueStorageError> {
            Ok(self.entries.get(&object_id).cloned())
        }

        fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, CatalogueStorageError> {
            Ok(self.entries.values().cloned().collect())
        }

        fn upsert_catalogue_entry(
            &mut self,
            entry: &CatalogueEntry,
        ) -> Result<(), CatalogueStorageError> {
            self.entries.insert(entry.object_id, entry.clone());
            Ok(())
        }
    }
}
