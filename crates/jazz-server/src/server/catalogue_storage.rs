use std::collections::BTreeMap;

use crate::kv::HostKv;
use crate::server::catalogue_entry::CatalogueEntry;
use jazz::tools::ObjectId;

pub(crate) type DynCatalogueStorage = Box<dyn CatalogueStorage + Send>;
pub(crate) type CatalogueStorageResult<T> = Result<T, CatalogueStorageError>;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum CatalogueStorageError {
    IoError(String),
    /// The backend could not prove the batch remained uncommitted. Reopen
    /// or recover before serving an in-memory catalogue based on the old state.
    PossiblyCommitted(String),
}

impl std::fmt::Display for CatalogueStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueStorageError::IoError(message) => write!(f, "IO error: {message}"),
            CatalogueStorageError::PossiblyCommitted(message) => {
                write!(f, "catalogue batch may have committed: {message}")
            }
        }
    }
}

impl std::error::Error for CatalogueStorageError {}

pub(crate) trait CatalogueStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>>;
    #[cfg(any(test, feature = "embedded-server"))]
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()>;
    /// Validate all encodings before atomically writing any of the entries.
    /// Durability is established separately by `flush` / `flush_wal`.
    fn upsert_catalogue_entries(
        &mut self,
        entries: &[CatalogueEntry],
    ) -> CatalogueStorageResult<()>;
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

    #[cfg(any(test, feature = "embedded-server"))]
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()> {
        self.entries.insert(entry.object_id, entry.clone());
        Ok(())
    }

    fn upsert_catalogue_entries(
        &mut self,
        entries: &[CatalogueEntry],
    ) -> CatalogueStorageResult<()> {
        encode_batch(entries)?;
        for entry in entries {
            self.entries.insert(entry.object_id, entry.clone());
        }
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

/// The durable catalogue: a host table in the app's store file.
pub(crate) struct CatalogueKvStorage {
    kv: HostKv,
}

impl CatalogueKvStorage {
    // `cat:` is the reserved server-catalogue namespace. Every epoch-1 entry
    // uses the versioned raw-UUID subnamespace below; scans deliberately cover
    // the whole parent namespace so an old or alternate spelling fails closed
    // instead of becoming invisible restart state.
    const SCAN_PREFIX: &'static [u8] = b"cat:";
    const ENTRY_PREFIX: &'static [u8] = b"cat:v1:";

    pub(crate) fn new(kv: HostKv) -> Self {
        Self { kv }
    }

    pub(crate) fn entry_key(object_id: ObjectId) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::ENTRY_PREFIX.len() + 16);
        key.extend_from_slice(Self::ENTRY_PREFIX);
        key.extend_from_slice(object_id.uuid().as_bytes());
        key
    }

    fn decode_entry_key(key: &[u8]) -> CatalogueStorageResult<ObjectId> {
        let raw = key.strip_prefix(Self::ENTRY_PREFIX).ok_or_else(|| {
            CatalogueStorageError::IoError("catalogue key uses an unsupported namespace".to_owned())
        })?;
        let uuid: [u8; 16] = raw.try_into().map_err(|_| {
            CatalogueStorageError::IoError("catalogue key is not one raw UUID".to_owned())
        })?;
        Ok(ObjectId::from_uuid(uuid::Uuid::from_bytes(uuid)))
    }

    /// Write one raw row, bypassing entry encoding (tests of malformed rows).
    #[cfg(test)]
    fn put_raw(&self, key: Vec<u8>, value: Vec<u8>) {
        self.kv.write_many(&[(key, value)]).unwrap();
    }
}

impl CatalogueStorage for CatalogueKvStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>> {
        let rows = self
            .kv
            .prefix(Self::SCAN_PREFIX)
            .map_err(CatalogueStorageError::IoError)?;
        let mut entries = rows
            .into_iter()
            .map(|(key, value)| {
                let object_id = Self::decode_entry_key(&key)?;
                CatalogueEntry::decode_storage_row(object_id, &value).map_err(|error| {
                    CatalogueStorageError::IoError(format!("decode catalogue entry: {error}"))
                })
            })
            .collect::<CatalogueStorageResult<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.object_id);
        Ok(entries)
    }

    #[cfg(any(test, feature = "embedded-server"))]
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()> {
        self.upsert_catalogue_entries(std::slice::from_ref(entry))
    }

    fn upsert_catalogue_entries(
        &mut self,
        entries: &[CatalogueEntry],
    ) -> CatalogueStorageResult<()> {
        let pairs = encode_batch(entries)?;
        self.kv
            .write_many(&pairs)
            .map_err(CatalogueStorageError::IoError)
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

fn encode_batch(entries: &[CatalogueEntry]) -> CatalogueStorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    entries
        .iter()
        .map(|entry| {
            let value = entry.encode_storage_row().map_err(|error| {
                CatalogueStorageError::IoError(format!("encode catalogue entry: {error}"))
            })?;
            Ok((CatalogueKvStorage::entry_key(entry.object_id), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal adapter tests: the deploy endpoint is not connected yet, and
    // entry-encoding failure must be checked before any native batch is submitted.
    fn check_batch_rejection_is_atomic(storage: &mut dyn CatalogueStorage) -> Vec<CatalogueEntry> {
        let entry = |byte, content: &[u8]| CatalogueEntry {
            object_id: ObjectId::from_uuid(uuid::Uuid::from_bytes([byte; 16])),
            metadata: std::collections::HashMap::new(),
            content: content.to_vec(),
        };
        let original = entry(1, b"old");
        storage.upsert_catalogue_entry(&original).unwrap();
        let replacement = entry(1, b"new");
        let second = entry(2, b"second");
        let mut invalid = second.clone();
        invalid
            .metadata
            .insert("oversized".into(), "x".repeat(usize::from(u16::MAX) + 1));
        assert!(matches!(
            storage.upsert_catalogue_entries(&[replacement.clone(), invalid]),
            Err(CatalogueStorageError::IoError(_))
        ));
        assert_eq!(storage.scan_catalogue_entries().unwrap(), vec![original]);
        let expected = vec![replacement, second];
        storage.upsert_catalogue_entries(&expected).unwrap();
        assert_eq!(storage.scan_catalogue_entries().unwrap(), expected);
        storage.upsert_catalogue_entries(&[]).unwrap();
        assert_eq!(storage.scan_catalogue_entries().unwrap(), expected);
        expected
    }

    #[test]
    fn memory_catalogue_batch_rejects_invalid_entries_without_a_prefix_write() {
        check_batch_rejection_is_atomic(&mut CatalogueMemoryStorage::new());
    }

    #[test]
    fn persistent_catalogue_batch_rejects_invalid_entries_and_reopens_complete() {
        let directory = tempfile::tempdir().unwrap();
        let storage_config = crate::runtime::StorageConfig::data_dir(directory.path());
        let open = || {
            let store = crate::runtime::AppStore::open(&storage_config).unwrap();
            (
                CatalogueKvStorage::new(store.kv(crate::kv::CATALOGUE).unwrap()),
                store,
            )
        };
        let (mut storage, store) = open();
        let expected = check_batch_rejection_is_atomic(&mut storage);
        storage.flush().unwrap();
        storage.close().unwrap();
        drop(storage);
        store.close().unwrap();
        let (reopened, _store) = open();
        assert_eq!(reopened.scan_catalogue_entries().unwrap(), expected);
        reopened.close().unwrap();
    }

    #[test]
    fn catalogue_key_v1_is_exact_and_rejects_alternate_spellings() {
        let object_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16]));
        let golden = b"cat:v1:\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a";
        assert_eq!(CatalogueKvStorage::entry_key(object_id), golden);
        assert_eq!(
            CatalogueKvStorage::decode_entry_key(golden).unwrap(),
            object_id
        );
        for malformed in [
            b"cat:4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a".as_slice(),
            b"cat:v1:\x4a".as_slice(),
            b"cat:v1:\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x4a\x00"
                .as_slice(),
        ] {
            assert!(CatalogueKvStorage::decode_entry_key(malformed).is_err());
        }
    }

    #[test]
    fn scan_rejects_one_malformed_entry_before_returning_partial_catalogue() {
        let directory = tempfile::tempdir().unwrap();
        let valid_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16]));
        let malformed_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4b; 16]));
        let valid = CatalogueEntry {
            object_id: valid_id,
            metadata: std::collections::HashMap::from([("type".to_owned(), "table".to_owned())]),
            content: b"catalogue-row".to_vec(),
        };
        let store = crate::runtime::AppStore::open(&crate::runtime::StorageConfig::data_dir(
            directory.path(),
        ))
        .unwrap();
        let mut catalogue = CatalogueKvStorage::new(store.kv(crate::kv::CATALOGUE).unwrap());
        catalogue.upsert_catalogue_entry(&valid).unwrap();
        catalogue.put_raw(
            CatalogueKvStorage::entry_key(malformed_id),
            b"JCAT\x01".to_vec(),
        );
        assert!(catalogue.scan_catalogue_entries().is_err());
    }
}
