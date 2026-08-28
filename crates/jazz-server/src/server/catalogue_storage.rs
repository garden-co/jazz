use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

use crate::server::catalogue_entry::CatalogueEntry;
use jazz::groove::storage::{BoxedStorage, OrderedKvStorage, StorageCodecProfile, StorageFactory};
use jazz::storage_codec_profile::epoch_1_storage_codec_profile;
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
    commands: mpsc::Sender<CatalogueStorageCommand>,
    closed: AtomicBool,
}

enum CatalogueStorageCommand {
    Scan(mpsc::Sender<CatalogueStorageResult<Vec<CatalogueEntry>>>),
    Upsert(CatalogueEntry, mpsc::Sender<CatalogueStorageResult<()>>),
    Flush(mpsc::Sender<CatalogueStorageResult<()>>),
    Close(mpsc::Sender<CatalogueStorageResult<()>>),
}

impl CatalogueKvStorage {
    // `cat:` is the reserved server-catalogue namespace. Every epoch-1 entry
    // uses the versioned raw-UUID subnamespace below; scans deliberately cover
    // the whole parent namespace so an old or alternate spelling fails closed
    // instead of becoming invisible restart state.
    pub(crate) const COLUMN_FAMILY: &'static str = "default";
    const SCAN_PREFIX: &'static [u8] = b"cat:";
    const ENTRY_PREFIX: &'static [u8] = b"cat:v1:";

    pub(crate) fn open(
        factory: Arc<dyn StorageFactory>,
        path: PathBuf,
    ) -> CatalogueStorageResult<Self> {
        let codec_profile = catalogue_storage_codec_profile()?;
        let (commands, receiver) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("jazz-catalogue-storage".to_owned())
            .spawn(move || {
                let storage = match jazz::db::block_on(factory.open(
                    path,
                    vec![Self::COLUMN_FAMILY.to_owned()],
                    codec_profile,
                )) {
                    Ok(storage) => storage,
                    Err(error) => {
                        let _ = opened_tx.send(Err(storage_error(error)));
                        return;
                    }
                };
                if opened_tx.send(Ok(())).is_err() {
                    return;
                }
                run_catalogue_storage(storage, receiver);
            })
            .map_err(|error| {
                CatalogueStorageError::IoError(format!("spawn catalogue storage owner: {error}"))
            })?;
        opened_rx.recv().map_err(|_| {
            CatalogueStorageError::IoError("catalogue storage owner exited during open".to_owned())
        })??;
        Ok(Self {
            commands,
            closed: AtomicBool::new(false),
        })
    }

    fn request<T>(
        &self,
        command: impl FnOnce(mpsc::Sender<CatalogueStorageResult<T>>) -> CatalogueStorageCommand,
    ) -> CatalogueStorageResult<T> {
        let (reply, response) = mpsc::channel();
        self.commands.send(command(reply)).map_err(|_| {
            CatalogueStorageError::IoError("catalogue storage owner is closed".to_owned())
        })?;
        response.recv().map_err(|_| {
            CatalogueStorageError::IoError("catalogue storage owner exited".to_owned())
        })?
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
}

impl CatalogueStorage for CatalogueKvStorage {
    fn scan_catalogue_entries(&self) -> CatalogueStorageResult<Vec<CatalogueEntry>> {
        self.request(CatalogueStorageCommand::Scan)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> CatalogueStorageResult<()> {
        self.request(|reply| CatalogueStorageCommand::Upsert(entry.clone(), reply))
    }

    fn flush(&self) -> CatalogueStorageResult<()> {
        self.request(CatalogueStorageCommand::Flush)
    }

    fn flush_wal(&self) -> CatalogueStorageResult<()> {
        self.request(CatalogueStorageCommand::Flush)
    }

    fn close(&self) -> CatalogueStorageResult<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.request(CatalogueStorageCommand::Close)
    }
}

impl Drop for CatalogueKvStorage {
    fn drop(&mut self) {
        // The owner thread holds the boxed storage and therefore any backend
        // lock. Join its close boundary before a direct builder drop returns.
        let _ = self.close();
    }
}

fn run_catalogue_storage(storage: BoxedStorage, commands: mpsc::Receiver<CatalogueStorageCommand>) {
    let mut storage = Some(storage);
    while let Ok(command) = commands.recv() {
        let active_storage = storage
            .as_ref()
            .expect("catalogue storage is present until close");
        match command {
            CatalogueStorageCommand::Scan(reply) => {
                let _ = reply.send(scan_entries(active_storage));
            }
            CatalogueStorageCommand::Upsert(entry, reply) => {
                let result = entry
                    .encode_storage_row()
                    .map_err(|error| {
                        CatalogueStorageError::IoError(format!("encode catalogue entry: {error}"))
                    })
                    .and_then(|bytes| {
                        jazz::db::block_on(active_storage.set(
                            CatalogueKvStorage::COLUMN_FAMILY.to_owned(),
                            CatalogueKvStorage::entry_key(entry.object_id),
                            bytes,
                        ))
                        .map_err(storage_error)
                    });
                let _ = reply.send(result);
            }
            CatalogueStorageCommand::Flush(reply) => {
                let result = jazz::db::block_on(active_storage.flush_write_boundary())
                    .map_err(storage_error);
                let _ = reply.send(result);
            }
            CatalogueStorageCommand::Close(reply) => {
                let owned_storage = storage.take().expect("catalogue storage closes once");
                let result = jazz::db::block_on(owned_storage.close()).map_err(storage_error);
                // `close()` flushes the backend, but the boxed owner may retain
                // process-level resources (notably RocksDB's lock) until drop.
                // Release those before acknowledging the synchronous boundary.
                drop(owned_storage);
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn scan_entries(storage: &BoxedStorage) -> CatalogueStorageResult<Vec<CatalogueEntry>> {
    let rows = jazz::db::block_on(storage.prefix(
        CatalogueKvStorage::COLUMN_FAMILY.to_owned(),
        CatalogueKvStorage::SCAN_PREFIX.to_vec(),
    ))
    .map_err(storage_error)?;
    let mut entries = rows
        .into_iter()
        .map(|(key, value)| {
            let object_id = CatalogueKvStorage::decode_entry_key(&key)?;
            CatalogueEntry::decode_storage_row(object_id, &value).map_err(|error| {
                CatalogueStorageError::IoError(format!("decode catalogue entry: {error}"))
            })
        })
        .collect::<CatalogueStorageResult<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.object_id);
    Ok(entries)
}

fn storage_error(error: jazz::groove::storage::Error) -> CatalogueStorageError {
    CatalogueStorageError::IoError(error.to_string())
}

/// Closed manifest profile for the server's independent catalogue root.
///
/// The catalogue has the same Jazz-owned durable families as an app runtime,
/// plus its own opaque entry payload codec. Keeping this constructor shared by
/// production and physical-layout fixtures prevents a stale Groove-only
/// manifest from masquerading as an accepted Jazz store.
pub(crate) fn catalogue_storage_codec_profile() -> CatalogueStorageResult<StorageCodecProfile> {
    epoch_1_storage_codec_profile()
        .and_then(|profile| profile.with_additional_codecs(["jazz.server-catalogue-entry.v1"]))
        .map_err(storage_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::groove::storage::OrderedKvStorage;

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
    fn adapter_catalogue_reopens_only_the_v1_default_cf_layout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalogue.rocksdb");
        let object_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16]));
        let entry = CatalogueEntry {
            object_id,
            metadata: std::collections::HashMap::from([("type".to_owned(), "table".to_owned())]),
            content: b"catalogue-row".to_vec(),
        };

        {
            // This lower-level fixture intentionally writes a physically
            // pre-existing catalogue row. It must use the same settled
            // manifest as the production opener; Jazz no longer admits a
            // Groove-only root as a compatibility layout.
            let storage =
                jazz_storage_rocksdb::RocksDbStorage::open_with_durability_and_codec_profile(
                    &path,
                    &["default"],
                    jazz_storage_rocksdb::Durability::WalNoSync,
                    &catalogue_storage_codec_profile().unwrap(),
                )
                .unwrap();
            jazz::db::block_on(storage.set(
                "default".to_owned(),
                CatalogueKvStorage::entry_key(object_id),
                entry.encode_storage_row().unwrap(),
            ))
            .unwrap();
        }

        let catalogue = CatalogueKvStorage::open(
            Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory),
            path.clone(),
        )
        .unwrap();
        assert_eq!(
            catalogue.scan_catalogue_entries().unwrap(),
            vec![entry.clone()]
        );
        catalogue.close().unwrap();
        let reopened =
            CatalogueKvStorage::open(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory), path)
                .unwrap();
        assert_eq!(reopened.scan_catalogue_entries().unwrap(), vec![entry]);
        reopened.close().unwrap();
    }

    #[test]
    fn scan_rejects_one_malformed_entry_before_returning_partial_catalogue() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalogue.rocksdb");
        let valid_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16]));
        let malformed_id = ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4b; 16]));
        let valid = CatalogueEntry {
            object_id: valid_id,
            metadata: std::collections::HashMap::from([("type".to_owned(), "table".to_owned())]),
            content: b"catalogue-row".to_vec(),
        };
        {
            let storage = jazz_storage_rocksdb::RocksDbStorage::open(&path, &["default"]).unwrap();
            jazz::db::block_on(storage.set(
                "default".to_owned(),
                CatalogueKvStorage::entry_key(valid_id),
                valid.encode_storage_row().unwrap(),
            ))
            .unwrap();
            jazz::db::block_on(storage.set(
                "default".to_owned(),
                CatalogueKvStorage::entry_key(malformed_id),
                b"JCAT\x01".to_vec(),
            ))
            .unwrap();
        }
        let catalogue =
            CatalogueKvStorage::open(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory), path)
                .unwrap();
        assert!(catalogue.scan_catalogue_entries().is_err());
        catalogue.close().unwrap();
    }
}
