use super::*;
use jazz::groove::storage::{OrderedKvStorage, StorageCodecProfile, StorageFactory};

const FAMILY: &str = "catalogue";
const KEY: &[u8] = b"authenticated-capture-v1";
const MAGIC: &[u8] = b"JNC\x01";

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
        let storage = cache.open()?;
        let bytes =
            block_on(storage.get(FAMILY.into(), KEY.to_vec())).map_err(RelayError::Storage)?;
        block_on(storage.close()).map_err(RelayError::Storage)?;
        if let Some(bytes) = bytes {
            cache.capture = Some(cache.decode(&bytes)?.to_vec());
        }
        Ok(cache)
    }

    fn open(&self) -> Result<BoxedStorage, RelayError> {
        let profile = StorageCodecProfile::groove_epoch_1()
            .with_additional_codecs(["jazz.native-authenticated-catalogue-cache.v1"])
            .map_err(RelayError::Storage)?;
        block_on(SqliteStorageFactory::default().open(
            self.path.clone(),
            vec![FAMILY.into()],
            profile,
        ))
        .map_err(RelayError::Storage)
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
        owner: &Rc<Db<BoxedStorage>>,
        context: &mut Context<'_>,
    ) -> Poll<Result<bool, RelayError>> {
        if self.pending_capture.is_none() {
            let observation = self.observation.get_or_insert_with(|| {
                let owner = Rc::clone(owner);
                Box::pin(async move { owner.take_authenticated_catalogue_state().await })
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
            if let Err(error) = self.persist(capture) {
                return Poll::Ready(Err(error));
            }
            self.capture = self.pending_capture.take();
        }
        Poll::Ready(Ok(self.pending_ready))
    }

    fn persist(&self, capture: &[u8]) -> Result<(), RelayError> {
        if self.capture.as_deref() == Some(capture) {
            return Ok(());
        }
        let length =
            u32::try_from(capture.len()).map_err(|_| invalid("catalogue capture too large"))?;
        // A stable inode coordinates account owner threads and host processes.
        // Never unlink it: replacing a lockfile would split ownership.
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path.with_extension("lock"))
            .map_err(|error| invalid(&format!("open catalogue lock: {error}")))?;
        lock.lock()
            .map_err(|error| invalid(&format!("lock catalogue cache: {error}")))?;
        let storage = self.open()?;
        let previous =
            block_on(storage.get(FAMILY.into(), KEY.to_vec())).map_err(RelayError::Storage)?;
        if let Some(previous) = previous {
            let previous = self.decode(&previous)?;
            jazz::db::validate_catalogue_capture_replacement(previous, capture)
                .map_err(RelayError::Db)?;
        }
        let mut envelope = Vec::with_capacity(MAGIC.len() + self.scope.len() + 4 + capture.len());
        envelope.extend_from_slice(MAGIC);
        envelope.extend_from_slice(&self.scope);
        envelope.extend_from_slice(&length.to_be_bytes());
        envelope.extend_from_slice(capture);
        block_on(storage.set(FAMILY.into(), KEY.to_vec(), envelope))
            .map_err(RelayError::Storage)?;
        block_on(storage.flush_write_boundary()).map_err(RelayError::Storage)?;
        block_on(storage.close()).map_err(RelayError::Storage)?;
        Ok(())
    }
}

fn invalid(message: &str) -> RelayError {
    RelayError::ForegroundCommand(message.to_owned())
}
