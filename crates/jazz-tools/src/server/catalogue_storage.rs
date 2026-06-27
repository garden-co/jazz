use std::collections::BTreeMap;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
use std::path::Path;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
use std::sync::Mutex;

use crate::catalogue::CatalogueEntry;
use crate::object::ObjectId;
use crate::storage::StorageError;

pub(crate) type DynCatalogueStorage = Box<dyn CatalogueStorage + Send>;

pub(crate) trait CatalogueStorage {
    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError>;
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError>;
    fn flush(&self) -> Result<(), StorageError>;
    fn flush_wal(&self) -> Result<(), StorageError>;
    fn close(&self) -> Result<(), StorageError>;
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
    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        Ok(self.entries.values().cloned().collect())
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        self.entries.insert(entry.object_id, entry.clone());
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub(crate) struct CatalogueRocksDbStorage {
    db: Mutex<Option<rocksdb::DB>>,
}

#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
impl CatalogueRocksDbStorage {
    const ENTRY_PREFIX: &'static [u8] = b"cat:";

    pub(crate) fn open(
        path: impl AsRef<Path>,
        cache_size_bytes: usize,
    ) -> Result<Self, StorageError> {
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        let cache = rocksdb::Cache::new_lru_cache(cache_size_bytes);
        block_opts.set_block_cache(&cache);

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        let db = rocksdb::DB::open(&opts, path.as_ref())
            .map_err(|error| StorageError::IoError(format!("catalogue rocksdb open: {error}")))?;
        Ok(Self {
            db: Mutex::new(Some(db)),
        })
    }

    fn with_db<T>(
        &self,
        f: impl FnOnce(&rocksdb::DB) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let db = self
            .db
            .lock()
            .map_err(|_| StorageError::IoError("catalogue rocksdb mutex poisoned".to_string()))?;
        let db = db.as_ref().ok_or_else(|| {
            StorageError::IoError("catalogue rocksdb storage already closed".to_string())
        })?;
        f(db)
    }

    fn entry_key(object_id: ObjectId) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::ENTRY_PREFIX.len() + 32);
        key.extend_from_slice(Self::ENTRY_PREFIX);
        key.extend_from_slice(object_id.uuid().simple().to_string().as_bytes());
        key
    }
}

#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
impl CatalogueStorage for CatalogueRocksDbStorage {
    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        self.with_db(|db| {
            let mut read_opts = rocksdb::ReadOptions::default();
            read_opts.set_iterate_upper_bound(b"cat;".to_vec());
            let iter = db.iterator_opt(
                rocksdb::IteratorMode::From(Self::ENTRY_PREFIX, rocksdb::Direction::Forward),
                read_opts,
            );
            let mut entries = Vec::new();
            for item in iter {
                let (key, value) = item.map_err(|error| {
                    StorageError::IoError(format!("catalogue rocksdb iter: {error}"))
                })?;
                let Some(hex_id) = key.strip_prefix(Self::ENTRY_PREFIX) else {
                    continue;
                };
                let uuid = uuid::Uuid::parse_str(std::str::from_utf8(hex_id).map_err(|error| {
                    StorageError::IoError(format!("catalogue rocksdb key utf8: {error}"))
                })?)
                .map_err(|error| {
                    StorageError::IoError(format!("catalogue rocksdb key uuid: {error}"))
                })?;
                let object_id = ObjectId::from_uuid(uuid);
                let entry =
                    CatalogueEntry::decode_storage_row(object_id, &value).map_err(|error| {
                        StorageError::IoError(format!("decode catalogue entry: {error}"))
                    })?;
                entries.push(entry);
            }
            entries.sort_by_key(|entry| entry.object_id);
            Ok(entries)
        })
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        self.with_db(|db| {
            let bytes = entry.encode_storage_row().map_err(|error| {
                StorageError::IoError(format!("encode catalogue entry: {error}"))
            })?;
            db.put(Self::entry_key(entry.object_id), bytes)
                .map_err(|error| StorageError::IoError(format!("catalogue rocksdb put: {error}")))
        })
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.with_db(|db| {
            db.flush()
                .map_err(|error| StorageError::IoError(format!("catalogue rocksdb flush: {error}")))
        })
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        self.with_db(|db| {
            db.flush_wal(true).map_err(|error| {
                StorageError::IoError(format!("catalogue rocksdb flush_wal: {error}"))
            })
        })
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(db) = self
            .db
            .lock()
            .map_err(|_| StorageError::IoError("catalogue rocksdb mutex poisoned".to_string()))?
            .take()
        else {
            return Ok(());
        };
        drop(db);
        Ok(())
    }
}
