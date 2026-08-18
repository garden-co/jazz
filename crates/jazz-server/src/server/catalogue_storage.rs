use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::server::catalogue_entry::CatalogueEntry;
use jazz::groove::storage::{BoxedStorage, OrderedKvStorage};
use jazz::tools::ObjectId;

pub(crate) type DynCatalogueStorage = Box<dyn CatalogueStorage + Send>;
pub(crate) type CatalogueStorageResult<T> = Result<T, CatalogueStorageError>;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum CatalogueStorageError {
    IoError(String),
}

impl std::fmt::Display for CatalogueStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueStorageError::IoError(message) => write!(f, "IO error: {message}"),
        }
    }
}

impl std::error::Error for CatalogueStorageError {}

pub(crate) trait CatalogueStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>>;
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()>;
    fn flush(&self) -> CatalogueStorageResult<()>;
    fn flush_wal(&self) -> CatalogueStorageResult<()>;
    fn close(&self) -> CatalogueStorageResult<()>;
}

#[derive(Default)]
pub(crate) struct CatalogueMemoryStorage {
    entries: BTreeMap<ObjectId, CatalogueEntry>,
}

impl CatalogueMemoryStorage {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl CatalogueStorage for CatalogueMemoryStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>> {
        Ok(self.entries.values().cloned().collect())
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()> {
        self.entries.insert(entry.object_id, entry.clone());
        Ok(())
    }

    fn flush(&self) -> CatalogueStorageResult<()> {
        Ok(())
    }

    fn flush_wal(&self) -> CatalogueStorageResult<()> {
        Ok(())
    }

    fn close(&self) -> CatalogueStorageResult<()> {
        Ok(())
    }
}

pub(crate) struct CatalogueKvStorage {
    storage: Mutex<Option<BoxedStorage>>,
}

impl CatalogueKvStorage {
    // Keep the original catalogue RocksDB layout: entries lived in the default
    // column family under `cat:` keys before storage became adapter-driven.
    pub(crate) const COLUMN_FAMILY: &'static str = "default";
    const ENTRY_PREFIX: &'static [u8] = b"cat:";

    pub(crate) fn new(storage: BoxedStorage) -> Self {
        Self {
            storage: Mutex::new(Some(storage)),
        }
    }

    fn with_storage<T>(
        &self,
        operation: impl FnOnce(&BoxedStorage) -> CatalogueStorageResult<T>,
    ) -> CatalogueStorageResult<T> {
        let storage = self.storage.lock().map_err(|_| {
            CatalogueStorageError::IoError("catalogue storage mutex poisoned".to_owned())
        })?;
        let storage = storage.as_ref().ok_or_else(|| {
            CatalogueStorageError::IoError("catalogue storage already closed".to_owned())
        })?;
        operation(storage)
    }

    fn entry_key(object_id: ObjectId) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::ENTRY_PREFIX.len() + 32);
        key.extend_from_slice(Self::ENTRY_PREFIX);
        key.extend_from_slice(object_id.uuid().simple().to_string().as_bytes());
        key
    }
}

impl CatalogueStorage for CatalogueKvStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>> {
        let mut entries = Vec::new();
        self.with_storage(|storage| {
            storage
                .scan_prefix(
                    Self::COLUMN_FAMILY,
                    Self::ENTRY_PREFIX,
                    &mut |key, value| {
                        let Some(hex_id) = key.strip_prefix(Self::ENTRY_PREFIX) else {
                            return Ok(());
                        };
                        let uuid = uuid::Uuid::parse_str(std::str::from_utf8(hex_id).map_err(
                            |error| jazz::groove::storage::Error::Backend {
                                backend: "catalogue",
                                message: format!("catalogue key utf8: {error}"),
                            },
                        )?)
                        .map_err(|error| {
                            jazz::groove::storage::Error::Backend {
                                backend: "catalogue",
                                message: format!("catalogue key uuid: {error}"),
                            }
                        })?;
                        let object_id = ObjectId::from_uuid(uuid);
                        let entry = CatalogueEntry::decode_storage_row(object_id, value).map_err(
                            |error| jazz::groove::storage::Error::Backend {
                                backend: "catalogue",
                                message: format!("decode catalogue entry: {error}"),
                            },
                        )?;
                        entries.push(entry);
                        Ok(())
                    },
                )
                .map_err(storage_error)
        })?;
        entries.sort_by_key(|entry| entry.object_id);
        Ok(entries)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()> {
        let bytes = entry.encode_storage_row().map_err(|error| {
            CatalogueStorageError::IoError(format!("encode catalogue entry: {error}"))
        })?;
        self.with_storage(|storage| {
            storage
                .set(
                    Self::COLUMN_FAMILY,
                    &Self::entry_key(entry.object_id),
                    &bytes,
                )
                .map_err(storage_error)
        })
    }

    fn flush(&self) -> CatalogueStorageResult<()> {
        self.with_storage(|storage| storage.flush_write_boundary().map_err(storage_error))
    }

    fn flush_wal(&self) -> CatalogueStorageResult<()> {
        self.with_storage(|storage| storage.flush_write_boundary().map_err(storage_error))
    }

    fn close(&self) -> CatalogueStorageResult<()> {
        let storage = self
            .storage
            .lock()
            .map_err(|_| {
                CatalogueStorageError::IoError("catalogue storage mutex poisoned".to_owned())
            })?
            .take();
        if let Some(storage) = storage {
            storage.flush_write_boundary().map_err(storage_error)?;
            drop(storage);
        }
        Ok(())
    }
}

fn storage_error(error: jazz::groove::storage::Error) -> CatalogueStorageError {
    CatalogueStorageError::IoError(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::groove::storage::{OrderedKvStorage, StorageFactory};

    #[test]
    fn adapter_catalogue_reads_the_pre_extraction_default_cf_layout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalogue.rocksdb");
        let object_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16]));
        let entry = CatalogueEntry {
            object_id,
            metadata: std::collections::HashMap::from([("type".to_owned(), "table".to_owned())]),
            content: b"legacy-catalogue-row".to_vec(),
        };

        {
            let legacy = jazz_storage_rocksdb::RocksDbStorage::open(&path, &["default"]).unwrap();
            legacy
                .set(
                    "default",
                    &CatalogueKvStorage::entry_key(object_id),
                    &entry.encode_storage_row().unwrap(),
                )
                .unwrap();
        }

        let storage = jazz_storage_rocksdb::RocksDbStorageFactory
            .open(&path, &[CatalogueKvStorage::COLUMN_FAMILY])
            .unwrap();
        let catalogue = CatalogueKvStorage::new(storage);
        assert_eq!(catalogue.scan_catalogue_entries().unwrap(), vec![entry]);
    }
}
