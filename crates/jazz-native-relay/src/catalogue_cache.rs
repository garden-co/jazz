use super::*;
use jazz::db::Db;
use rusqlite::{Connection, OptionalExtension};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

const MAGIC: &[u8] = b"JNC\x02";

type CatalogueObservation =
    Pin<Box<dyn Future<Output = Result<jazz::db::AuthenticatedCatalogueState, jazz::db::Error>>>>;

/// The host owns this auxiliary root independently of every account root.
/// The admitted scope supplies registry and canonical application/environment;
/// neither account nor session identity participates in the cache key.
pub(super) struct CatalogueCache {
    path: PathBuf,
    scope: Vec<u8>,
    pub(super) capture: Option<Vec<u8>>,
    pending_capture: Option<Vec<u8>>,
    pending_ready: bool,
    observation: Option<CatalogueObservation>,
}

impl CatalogueCache {
    pub(super) fn load(config: &RelayOpenConfig) -> Result<Self, RelayError> {
        let mut scope = Vec::new();
        for part in [&config.scope.app_namespace, &config.scope.storage_namespace] {
            let length =
                u32::try_from(part.len()).map_err(|_| invalid("catalogue scope too large"))?;
            scope.extend_from_slice(&length.to_be_bytes());
            scope.extend_from_slice(part.as_bytes());
        }
        let mut key = b"jazz-native-catalogue-root-v1\0".to_vec();
        key.extend_from_slice(&scope);
        let parent = config
            .sqlite_path
            .parent()
            .ok_or_else(|| invalid("catalogue cache requires admitted storage root"))?;
        let mut cache = Self {
            path: parent.join(format!("catalogue-{}.sqlite", blake3::hash(&key).to_hex())),
            scope,
            capture: None,
            pending_capture: None,
            pending_ready: false,
            observation: None,
        };
        let _lock = cache.lock()?;
        let storage = cache.open()?;
        let bytes = storage
            .query_row(
                "SELECT capture FROM _jazz_native_catalogue_v2 WHERE slot=1",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(sql_error)?;
        drop(storage);
        if let Some(bytes) = bytes {
            cache.capture = Some(cache.decode(&bytes)?.to_vec());
        }
        Ok(cache)
    }

    fn open(&self) -> Result<Connection, RelayError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| RelayError::Storage(error.to_string()))?;
        }
        if let Ok(meta) = std::fs::symlink_metadata(&self.path)
            && !meta.file_type().is_file()
        {
            return Err(invalid("catalogue cache must be a regular file"));
        }
        let mut conn = Connection::open(&self.path).map_err(sql_error)?;
        conn.execute_batch("PRAGMA synchronous=FULL")
            .map_err(sql_error)?;
        let tx = conn.transaction().map_err(sql_error)?;
        let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='_jazz_native_catalogue_v2')",[],|r|r.get(0)).map_err(sql_error)?;
        if !exists {
            let occupied: bool = tx
                .query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master)", [], |r| {
                    r.get(0)
                })
                .map_err(sql_error)?;
            if occupied {
                return Err(RelayError::Db(jazz::db::Error::new(
                    jazz::db::ErrorCode::UnsupportedStorageCodecs,
                    "native catalogue cache requires jazz.sqlite.native-catalogue-cache.v2",
                )));
            }
            tx.execute_batch("CREATE TABLE _jazz_native_catalogue_v2(slot INTEGER PRIMARY KEY CHECK(slot=1),capture BLOB NOT NULL)").map_err(sql_error)?;
        }
        tx.commit().map_err(sql_error)?;
        storage::sync_parent(&self.path)?;
        Ok(conn)
    }

    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], RelayError> {
        let prefix_len = MAGIC.len() + self.scope.len();
        if !bytes.starts_with(MAGIC)
            || bytes.get(MAGIC.len()..prefix_len) != Some(self.scope.as_slice())
        {
            return Err(invalid("unknown or wrong-scope catalogue cache envelope"));
        }
        let length_bytes = bytes
            .get(prefix_len..prefix_len + 4)
            .ok_or_else(|| invalid("truncated catalogue cache"))?;
        let length =
            u32::from_be_bytes(length_bytes.try_into().expect("four-byte length")) as usize;
        let capture = &bytes[prefix_len + 4..];
        if capture.len() != length || capture.is_empty() {
            return Err(invalid("malformed catalogue cache payload length"));
        }
        Ok(capture)
    }

    /// Pump turns and foreground readiness operations share this retained
    /// observation. Polling never lends the cache into the asynchronous owner
    /// lock, so cancelling a waiter cannot cancel or duplicate the drain.
    pub(super) fn poll_refresh(
        &mut self,
        owner: &Rc<Db>,
        context: &mut Context<'_>,
    ) -> Poll<Result<bool, RelayError>> {
        if self.pending_capture.is_none() {
            let observation = self.observation.get_or_insert_with(|| {
                let owner = Rc::clone(owner);
                Box::pin(async move { owner.take_authenticated_catalogue_state() })
            });
            let state = match observation.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.observation = None;
                    match result {
                        Ok(state) => state,
                        Err(error) => return Poll::Ready(Err(RelayError::Db(error))),
                    }
                }
            };
            self.pending_capture = state.capture;
            self.pending_ready = state.ready;
        }
        if let Some(capture) = self.pending_capture.as_deref() {
            // On error the exact drained capture stays owned here for retry;
            // neither the pump nor a waiter can report durable readiness.
            let merged = match self.persist(capture) {
                Ok(merged) => merged,
                Err(error) => return Poll::Ready(Err(error)),
            };
            self.pending_capture = None;
            self.capture = Some(merged);
        }
        Poll::Ready(Ok(self.pending_ready))
    }

    fn lock(&self) -> Result<std::fs::File, RelayError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| RelayError::Storage(error.to_string()))?;
        }
        let path = self.path.with_extension("lock");
        if let Ok(meta) = std::fs::symlink_metadata(&path)
            && !meta.file_type().is_file()
        {
            return Err(invalid("catalogue lock must be a regular file"));
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| RelayError::Storage(error.to_string()))?;
        lock.lock()
            .map_err(|error| RelayError::Storage(error.to_string()))?;
        Ok(lock)
    }

    fn persist(&self, capture: &[u8]) -> Result<Vec<u8>, RelayError> {
        // A stable inode coordinates account owner threads and host processes.
        // Never unlink it: replacing a lockfile would split ownership.
        let _lock = self.lock()?;
        let storage = self.open()?;
        let previous = storage
            .query_row(
                "SELECT capture FROM _jazz_native_catalogue_v2 WHERE slot=1",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(sql_error)?;
        let capture = if let Some(previous) = previous {
            jazz::db::merge_catalogue_captures(self.decode(&previous)?, capture)
                .map_err(RelayError::Db)?
        } else {
            capture.to_vec()
        };
        let length =
            u32::try_from(capture.len()).map_err(|_| invalid("catalogue capture too large"))?;
        let mut envelope = Vec::with_capacity(MAGIC.len() + self.scope.len() + 4 + capture.len());
        envelope.extend_from_slice(MAGIC);
        envelope.extend_from_slice(&self.scope);
        envelope.extend_from_slice(&length.to_be_bytes());
        envelope.extend_from_slice(&capture);
        storage.execute("INSERT INTO _jazz_native_catalogue_v2 VALUES(1,?1) ON CONFLICT(slot) DO UPDATE SET capture=excluded.capture",[envelope]).map_err(sql_error)?;
        storage.close().map_err(|(_, e)| sql_error(e))?;
        storage::sync_parent(&self.path)?;
        Ok(capture)
    }
}

fn invalid(message: &str) -> RelayError {
    RelayError::ForegroundCommand(message.to_owned())
}

fn sql_error(error: rusqlite::Error) -> RelayError {
    RelayError::Db(jazz::db::Error::new(
        jazz::db::ErrorCode::Storage,
        error.to_string(),
    ))
}
