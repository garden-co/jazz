//! The runtime's native host: the store thread that owns an app's SQLite
//! connection (an [`AppStore`], its [`StoreLoop`] and chunk I/O), and the
//! [`ServerRuntimeHandle`] request and WebSocket tasks drive it through.
//! Moved unchanged from `runtime/mod.rs`; the runtime's engine side
//! ([`Owner`]) stays there.

use super::*;

pub(super) type Job = Box<dyn FnOnce(&mut Owner) + Send>;
/// Work on the store's host tables, before or after the runtime opened.
pub(crate) type HostJob = Box<dyn FnOnce(&dyn HostTables) + Send>;

/// After a panic the runtime restarts on the store at most this often; a
/// second panic sooner leaves it stopped and the store reports the fault.
pub(super) const RESTART_INTERVAL: Duration = Duration::from_secs(60);

/// What the store thread is asked to do. Host work has its own queue, which
/// the thread runs before every runtime job and tick, up to a tick slice of
/// work at a time: a host write waits for at most the one job or tick slice
/// in progress, and runtime ticks still run while host work keeps coming.
pub(super) enum Work {
    /// Host work is queued.
    Host,
    Owner(Job),
    Start(Start),
    /// Stop the runtime; the store keeps serving its host tables.
    Stop(std_mpsc::Sender<()>),
    Close,
}

/// What the runtime is opened with.
#[derive(Clone)]
pub(super) struct Runtime {
    pub(super) schema: JazzSchema,
    pub(super) any_stored_catalogue: bool,
}

/// Open the runtime on the store.
pub(super) struct Start {
    pub(super) runtime: Runtime,
    pub(super) initial: Option<InitialCatalogue>,
    pub(super) reply: std_mpsc::Sender<Result<(), CatalogueApplyError>>,
}

/// Sendable handle for the thread that owns one app's store file: its host
/// tables from the start, and the runtime once [`AppStore::start_runtime`]
/// opened it.
#[derive(Clone)]
pub struct AppStore {
    pub(super) inner: Arc<StoreThread>,
}

pub(super) struct StoreThread {
    pub(super) work: Mutex<Option<std_mpsc::Sender<Work>>>,
    pub(super) host: std_mpsc::Sender<HostJob>,
    /// Committed host tables, read without waiting for the store's thread.
    /// An in-memory store lives as long as this connection to it.
    pub(super) reader: Arc<HostReader>,
    pub(super) join: Mutex<Option<thread::JoinHandle<()>>>,
    pub(super) tick_queued: Arc<AtomicBool>,
    pub(super) fault: Arc<Mutex<Option<String>>>,
    /// The app the store serves (see [`AppStore::set_app`]).
    pub(super) app: Arc<Mutex<Option<String>>>,
    /// Tests: runtime ticks so far.
    #[cfg(test)]
    pub(super) ticks: Arc<AtomicU64>,
}

/// The file a store lives in.
#[derive(Clone)]
pub(super) enum Source {
    /// The database file, and where its large-value bytes live.
    File(PathBuf, Blobs),
    /// An in-memory store shared by name within this process.
    Memory(String),
}

impl Source {
    pub(super) fn new(storage: &StorageConfig) -> Self {
        static NEXT_MEMORY_STORE: AtomicU64 = AtomicU64::new(0);
        match storage {
            StorageConfig::InMemory => Self::Memory(format!(
                "jazz-server-{}-{}",
                std::process::id(),
                NEXT_MEMORY_STORE.fetch_add(1, Ordering::Relaxed)
            )),
            StorageConfig::SQLite { path, blobs } => Self::File(path.clone(), blobs.clone()),
        }
    }

    pub(super) fn open(&self) -> Result<StoreFile, String> {
        match self {
            Self::Memory(name) => jazz::db::block_on(StoreFile::shared_memory(name)),
            Self::File(path, _) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|error| format!("create {}: {error}", parent.display()))?;
                }
                // Every commit (rows, catalogue, account registry) is durable
                // when it returns.
                jazz::db::block_on(StoreFile::open(
                    path,
                    FileOptions {
                        synchronous_full: true,
                        ..FileOptions::default()
                    },
                ))
            }
        }
        .map_err(|error| format!("open server store: {error}"))
    }

    /// The chunk store the runtime's large-value bytes go to, if not the
    /// database file.
    pub(super) fn chunk_store(&self) -> Result<Option<ChunkStoreChoice>, String> {
        match self {
            Self::File(_, Blobs::Files(dir)) => jazz::chunk_store::FileChunkStore::new(dir)
                .map(|store| {
                    Some(ChunkStoreChoice {
                        store: Arc::new(store),
                        network: false,
                    })
                })
                .map_err(|error| format!("open large-value directory {}: {error}", dir.display())),
            Self::File(_, Blobs::Network(network)) => Ok(Some(ChunkStoreChoice {
                store: Arc::clone(&network.store),
                network: true,
            })),
            _ => Ok(None),
        }
    }

    pub(super) fn reader(&self) -> Result<HostReader, String> {
        match self {
            Self::Memory(name) => HostReader::shared_memory(name),
            Self::File(path, _) => HostReader::open(path, None),
        }
        .map_err(|error| format!("open server store reader: {error}"))
    }
}

/// The chunk store a runtime opens with, and whether its I/O runs beside
/// the store thread ([`ChunkIoRunner`]).
#[derive(Clone)]
pub(super) struct ChunkStoreChoice {
    pub(super) store: Arc<dyn jazz::chunk_store::ChunkStore>,
    pub(super) network: bool,
}

/// Runs a network chunk store's batches ([`jazz::db::Db::take_chunk_io`])
/// on a thread of its own and hands each outcome back to the store thread
/// as runtime work, which applies it and ticks. One batch is out at a time.
/// After a batch fails, the next waits (1 s, doubling up to a minute), so
/// a bucket that refuses every request is not asked again on every tick.
///
/// While a batch is out or backing off, the nodes reads wait on are
/// fetched beside it on a second thread
/// ([`jazz::db::Db::take_chunk_fetches`]), so a read never waits for a
/// batch's uploads, nor for the backoff of puts that failed. Fetches that
/// fail back off the same way, in both lanes: until the wait is over,
/// batches go out without fetches
/// ([`jazz::db::Db::take_chunk_io_holding_fetches`]), so uploads that
/// succeed do not ask the store again for what it refused.
pub(super) struct ChunkIoRunner {
    pub(super) batches: Lane,
    pub(super) out: std::cell::Cell<bool>,
    /// How long until the next batch, after batches failed.
    pub(super) backoff: Backoff,
    pub(super) fetches: Lane,
    /// Whether fetches are out on [`Self::fetches`].
    pub(super) fetching: std::cell::Cell<bool>,
    /// How long until the next fetches, after fetches failed.
    pub(super) fetch_backoff: Backoff,
}

/// What a [`ChunkIoRunner`] may hand over: a batch (`fetches`: with what
/// reads wait on in it), and fetches on their own lane.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Plan {
    pub(super) batch: bool,
    pub(super) fetches: bool,
    pub(super) fetch_lane: bool,
}

/// Consecutive failures of one kind of chunk I/O, and when it may go
/// again: 1 s after the first, doubling up to a minute.
#[derive(Default)]
pub(super) struct Backoff {
    pub(super) failures: std::cell::Cell<u32>,
    pub(super) not_before: std::cell::Cell<Option<std::time::Instant>>,
    /// The workload token generation the last failure saw: a fresh token
    /// (often what the failure lacked) ends the wait, once per run of
    /// failures, so a store that is down for another reason is not asked
    /// again on every token rotation.
    pub(super) failed_on_token: std::cell::Cell<u64>,
    pub(super) token_ended_wait: std::cell::Cell<bool>,
}

impl Backoff {
    /// Whether the last failure's wait is still on, and no fresh workload
    /// token ended it.
    pub(super) fn waiting(&self) -> bool {
        self.waiting_at(crate::middleware::workload::workload_token_generation())
    }

    /// [`Self::waiting`] with the workload token generation now.
    pub(super) fn waiting_at(&self, generation: u64) -> bool {
        if self
            .not_before
            .get()
            .is_none_or(|at| std::time::Instant::now() >= at)
        {
            return false;
        }
        if self.token_ended_wait.get() || generation == self.failed_on_token.get() {
            return true;
        }
        self.token_ended_wait.set(true);
        self.not_before.set(None);
        false
    }

    /// An attempt ended; `failed` delays the next. Returns whether to log
    /// the failure: the first of a run, then once the wait is at its
    /// longest.
    pub(super) fn ended(&self, failed: bool) -> bool {
        self.ended_at(
            failed,
            crate::middleware::workload::workload_token_generation(),
        )
    }

    /// [`Self::ended`] with the workload token generation now.
    pub(super) fn ended_at(&self, failed: bool, generation: u64) -> bool {
        if !failed {
            self.failures.set(0);
            self.not_before.set(None);
            self.token_ended_wait.set(false);
            return false;
        }
        self.failed_on_token.set(generation);
        let failures = self.failures.get().saturating_add(1);
        self.failures.set(failures);
        let (first, longest) = CHUNK_IO_BACKOFF;
        let wait = first
            .checked_mul(1 << (failures - 1).min(16))
            .unwrap_or(longest)
            .min(longest);
        self.not_before.set(Some(std::time::Instant::now() + wait));
        failures == 1 || wait == longest
    }
}

/// A thread that runs chunk I/O and hands each outcome to `finish` on the
/// store thread.
pub(super) struct Lane {
    pub(super) sender: std_mpsc::Sender<jazz::chunk_store::ChunkIo>,
    /// Dropped to cancel the work in flight.
    pub(super) cancel: Option<futures::channel::oneshot::Sender<()>>,
    /// Disconnects once the thread has ended.
    pub(super) ended: std_mpsc::Receiver<()>,
}

/// The first wait after a failed batch, and the longest.
pub(super) const CHUNK_IO_BACKOFF: (Duration, Duration) =
    (Duration::from_secs(1), Duration::from_secs(60));
/// How long dropping a runner waits for its cancelled batch to stop.
pub(super) const CHUNK_IO_STOP: Duration = Duration::from_secs(5);

impl Lane {
    pub(super) fn start(
        name: &str,
        store: Arc<dyn jazz::chunk_store::ChunkStore>,
        work: std_mpsc::Sender<Work>,
        finish: fn(&mut Owner, jazz::chunk_store::ChunkIoDone),
    ) -> Result<Self, String> {
        let (sender, receiver) = std_mpsc::channel::<jazz::chunk_store::ChunkIo>();
        let (cancel, mut cancelled) = futures::channel::oneshot::channel::<()>();
        let (ended_tx, ended) = std_mpsc::channel::<()>();
        thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let _ended = ended_tx;
                for io in receiver {
                    // Cancelled work is dropped mid-flight: its requests are
                    // aborted and it goes to the store's next batch.
                    let run = io.run(&*store);
                    let done =
                        match block_on(futures::future::select(Box::pin(run), &mut cancelled)) {
                            futures::future::Either::Left((done, _)) => done,
                            futures::future::Either::Right(_) => return,
                        };
                    let finish: Job = Box::new(move |owner: &mut Owner| finish(owner, done));
                    if work.send(Work::Owner(finish)).is_err() {
                        return;
                    }
                }
            })
            .map_err(|error| format!("start {name} thread: {error}"))?;
        Ok(Self {
            sender,
            cancel: Some(cancel),
            ended,
        })
    }

    pub(super) fn send(&self, io: jazz::chunk_store::ChunkIo) -> bool {
        self.sender.send(io).is_ok()
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        // Cancel the work in flight and close the channel; wait a little
        // for the thread to stop, and never for the bucket.
        drop(self.cancel.take());
        let (closed, _) = std_mpsc::channel();
        drop(std::mem::replace(&mut self.sender, closed));
        if let Err(std_mpsc::RecvTimeoutError::Timeout) = self.ended.recv_timeout(CHUNK_IO_STOP) {
            tracing::warn!("the large-value store I/O thread did not stop in time; leaving it");
        }
    }
}

impl ChunkIoRunner {
    pub(super) fn start(
        store: Arc<dyn jazz::chunk_store::ChunkStore>,
        work: std_mpsc::Sender<Work>,
    ) -> Result<Self, String> {
        Ok(Self {
            batches: Lane::start(
                "jazz-chunk-io",
                Arc::clone(&store),
                work.clone(),
                Owner::finish_chunk_io,
            )?,
            out: std::cell::Cell::new(false),
            backoff: Backoff::default(),
            fetches: Lane::start("jazz-chunk-fetch", store, work, Owner::finish_chunk_fetches)?,
            fetching: std::cell::Cell::new(false),
            fetch_backoff: Backoff::default(),
        })
    }

    /// Hand the runtime's next batch over, unless one is out or the last
    /// failed too recently; then hand over what reads wait on beside it,
    /// unless fetches are out or failed too recently.
    pub(super) fn pump(&self, owner: &Owner) {
        let Plan {
            batch,
            fetches,
            fetch_lane,
        } = self.plan();
        if batch {
            let taken = if fetches {
                block_on(owner.db.take_chunk_io())
            } else {
                block_on(owner.db.take_chunk_io_holding_fetches())
            };
            match taken {
                // Nothing to do: dropping an empty batch ends it.
                Ok(Some(io)) if io.is_empty() => {}
                Ok(Some(io)) => {
                    self.out.set(true);
                    if !self.batches.send(io) {
                        tracing::error!("the chunk I/O thread is gone");
                    }
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "take large-value store I/O"),
            }
        }
        if !fetch_lane {
            return;
        }
        match block_on(owner.db.take_chunk_fetches()) {
            Ok(Some(io)) => {
                self.fetching.set(true);
                if !self.fetches.send(io) {
                    tracing::error!("the chunk fetch thread is gone");
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "take large-value store fetches"),
        }
    }

    /// What [`Self::pump`] may hand over now.
    pub(super) fn plan(&self) -> Plan {
        let fetches = !self.fetch_backoff.waiting();
        Plan {
            batch: !self.out.get() && !self.backoff.waiting(),
            fetches,
            fetch_lane: fetches && !self.fetching.get(),
        }
    }

    /// A batch came back; `failed` delays the next. Returns whether to log
    /// the failure: the first of a run, then once the wait is at its longest.
    pub(super) fn returned(&self, failed: bool) -> bool {
        self.out.set(false);
        self.backoff.ended(failed)
    }

    /// [`Self::returned`] for a batch whose fetches (`None`: it had none)
    /// failed or not: they delay fetches in both lanes, as the fetch
    /// lane's do.
    pub(super) fn returned_with(&self, failed: bool, fetches_failed: Option<bool>) -> bool {
        if let Some(fetches_failed) = fetches_failed {
            self.fetch_backoff.ended(fetches_failed);
        }
        self.returned(failed)
    }

    /// Fetches came back; `failed` delays the next, in both lanes. Returns
    /// whether to log the failure, as [`Self::returned`] does.
    pub(super) fn fetched(&self, failed: bool) -> bool {
        self.fetching.set(false);
        self.fetch_backoff.ended(failed)
    }
}

impl AppStore {
    /// Open the store `storage` names on its own thread.
    pub fn open(storage: &StorageConfig) -> Result<Self, String> {
        let source = Source::new(storage);
        let (work, receiver) = std_mpsc::channel::<Work>();
        let thread_wake = work.clone();
        let (host, host_jobs) = std_mpsc::channel::<HostJob>();
        let (opened_tx, opened_rx) = std_mpsc::channel();
        let tick_queued = Arc::new(AtomicBool::new(false));
        let fault = Arc::new(Mutex::new(None));
        let app = Arc::new(Mutex::new(None));
        let (thread_source, thread_ticks, thread_fault, thread_app) = (
            source.clone(),
            Arc::clone(&tick_queued),
            Arc::clone(&fault),
            Arc::clone(&app),
        );
        #[cfg(test)]
        let ticks = Arc::new(AtomicU64::new(0));
        #[cfg(test)]
        let thread_tick_count = Arc::clone(&ticks);
        let join = thread::Builder::new()
            .name("jazz-server-store".to_owned())
            .spawn(move || {
                let file = match thread_source.open() {
                    Ok(file) => {
                        let _ = opened_tx.send(Ok(()));
                        file
                    }
                    Err(error) => {
                        let _ = opened_tx.send(Err(error));
                        return;
                    }
                };
                StoreLoop {
                    source: thread_source,
                    work: receiver,
                    wake: thread_wake,
                    host: host_jobs,
                    backlog: VecDeque::new(),
                    tick_queued: thread_ticks,
                    fault: thread_fault,
                    app: thread_app,
                    state: State::Idle(Ok(file)),
                    started: None,
                    restarted: None,
                    #[cfg(test)]
                    ticks: thread_tick_count,
                }
                .run();
            })
            .map_err(|error| format!("failed to spawn server store thread: {error}"))?;
        let opened = opened_rx
            .recv()
            .map_err(|_| "server store thread exited before opening".to_owned())
            .and_then(|opened| opened)
            .and_then(|()| source.reader());
        let reader = match opened {
            Ok(reader) => Arc::new(reader),
            Err(error) => {
                drop(work);
                let _ = join.join();
                return Err(error);
            }
        };
        Ok(Self {
            inner: Arc::new(StoreThread {
                work: Mutex::new(Some(work)),
                host,
                reader,
                join: Mutex::new(Some(join)),
                tick_queued,
                fault,
                app,
                #[cfg(test)]
                ticks,
            }),
        })
    }

    /// Name the app this store serves in its reports (slow queries).
    /// Takes effect when the runtime next opens.
    pub fn set_app(&self, app: impl Into<String>) {
        if let Ok(mut slot) = self.inner.app.lock() {
            *slot = Some(app.into());
        }
    }

    /// A store that lives as long as its handles.
    pub fn memory() -> Result<Self, String> {
        Self::open(&StorageConfig::InMemory)
    }

    /// Key-value access to one host table.
    pub(crate) fn kv(&self, table: HostTable) -> Result<HostKv, String> {
        Ok(HostKv::new(
            table,
            Arc::clone(&self.inner.reader),
            HostSender {
                host: self.inner.host.clone(),
                wake: self.sender()?,
            },
        ))
    }

    /// The store's catalogue tables.
    pub(crate) fn catalogue(&self) -> Result<crate::server::CatalogueStorage, String> {
        Ok(crate::server::CatalogueStorage::new(
            Arc::clone(&self.inner.reader),
            HostSender {
                host: self.inner.host.clone(),
                wake: self.sender()?,
            },
        ))
    }

    /// SQL access to the host tables a server shapes itself.
    pub(crate) fn sql(&self) -> Result<HostSql, String> {
        Ok(HostSql::new(
            Arc::clone(&self.inner.reader),
            HostSender {
                host: self.inner.host.clone(),
                wake: self.sender()?,
            },
        ))
    }

    /// Why the runtime stopped serving, if it failed (panicked) and could
    /// not be restarted. The host tables stay available; the server
    /// reports itself unhealthy so it is restarted.
    pub fn fault(&self) -> Option<String> {
        self.inner.fault.lock().ok().and_then(|fault| fault.clone())
    }

    pub(super) fn sender(&self) -> Result<std_mpsc::Sender<Work>, String> {
        self.inner
            .work
            .lock()
            .map_err(|_| "server store lock poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "server store is closed".to_owned())
    }

    pub(super) fn send(&self, work: Work) -> Result<(), String> {
        self.sender()?
            .send(work)
            .map_err(|_| "server store thread is not running".to_owned())
    }

    /// Open the runtime on this store. `any_stored_catalogue`: reopen with
    /// whatever catalogue the store holds (a catalogue-driven server), not
    /// only one that admits `schema`.
    pub fn start_runtime(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
    ) -> Result<ServerRuntimeHandle, String> {
        self.start(schema, role, any_stored_catalogue, false)
    }

    pub(super) fn start(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
        owns_store: bool,
    ) -> Result<ServerRuntimeHandle, String> {
        self.start_with_catalogue(schema, role, any_stored_catalogue, owns_store, None)
            .map_err(String::from)
    }

    pub(crate) fn start_deployment(
        &self,
        schema: JazzSchema,
        change: CatalogueChange,
        kept: crate::portable::deployment::KeptValues,
    ) -> Result<ServerRuntimeHandle, CatalogueApplyError> {
        self.start_with_catalogue(schema, NodeRole::Core, true, false, Some((change, kept)))
    }

    fn start_with_catalogue(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
        owns_store: bool,
        initial: Option<InitialCatalogue>,
    ) -> Result<ServerRuntimeHandle, CatalogueApplyError> {
        if role == NodeRole::Relay {
            return Err("relay nodes are not supported yet on SQLite"
                .to_owned()
                .into());
        }
        let (reply, started) = std_mpsc::channel();
        self.send(Work::Start(Start {
            runtime: Runtime {
                schema,
                any_stored_catalogue,
            },
            initial,
            reply,
        }))?;
        started
            .recv()
            .map_err(|_| "server store thread exited before the runtime started".to_owned())??;
        let (activity, _) = watch::channel(0_u64);
        Ok(ServerRuntimeHandle {
            inner: Arc::new(Inner {
                store: self.clone(),
                owns_store,
                activity,
                next_session: AtomicU64::new(1),
                edges: Mutex::new(HashMap::new()),
                thin_ingress: Arc::new(tokio::sync::Semaphore::new(THIN_INGRESS_SERVER_BUDGET)),
            }),
        })
    }

    /// Stop the runtime and wait for it; the host tables stay available.
    pub fn stop_runtime(&self) -> Result<(), String> {
        let (reply, stopped) = std_mpsc::channel();
        self.send(Work::Stop(reply))?;
        stopped
            .recv()
            .map_err(|_| "server store thread exited while stopping the runtime".to_owned())
    }

    /// Stop and join the store thread (and the runtime on it); the file is
    /// closed, its WAL checkpointed, when this returns. Host tables are
    /// unavailable afterwards.
    /// Safe to call more than once.
    pub fn close(&self) -> Result<(), String> {
        // Before the store's connection, so that one closes last and
        // checkpoints the WAL into the file. (An in-memory store is gone
        // once both are.)
        self.inner.reader.close();
        let work = self
            .inner
            .work
            .lock()
            .map_err(|_| "server store lock poisoned".to_owned())?
            .take();
        if let Some(work) = work {
            let _ = work.send(Work::Close);
        }
        let join = self
            .inner
            .join
            .lock()
            .map_err(|_| "server store join lock poisoned".to_owned())?
            .take();
        if let Some(join) = join {
            join.join()
                .map_err(|_| "server store thread panicked".to_owned())?;
        }
        Ok(())
    }
}

/// Sends host-table work to a store's thread; it keeps the thread running.
pub(crate) struct HostSender {
    pub(super) host: std_mpsc::Sender<HostJob>,
    pub(super) wake: std_mpsc::Sender<Work>,
}

impl HostSender {
    pub(crate) fn send(&self, job: HostJob) -> Result<(), String> {
        let closed = "server store is closed";
        self.host.send(job).map_err(|_| closed.to_owned())?;
        self.wake.send(Work::Host).map_err(|_| closed.to_owned())
    }
}

/// Host tables of a store that could not be opened: each use reports why.
pub(super) struct Unavailable(pub(super) String);

#[async_trait::async_trait(?Send)]
impl HostTables for Unavailable {
    async fn catalogue(&self) -> Result<CatalogueRows, jazz::db::Error> {
        Err(self.error())
    }

    async fn catalogue_put(
        &self,
        _: &jazz::catalogue::CataloguePut,
    ) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }

    async fn host_get(&self, _: HostTable, _: &[u8]) -> Result<Option<Vec<u8>>, jazz::db::Error> {
        Err(self.error())
    }
    async fn host_prefix(
        &self,
        _: HostTable,
        _: &[u8],
    ) -> Result<Vec<jazz::host::HostPair>, jazz::db::Error> {
        Err(self.error())
    }
    async fn host_put_many(
        &self,
        _: HostTable,
        _: &[jazz::host::HostPair],
    ) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
    async fn host_put_if_absent(
        &self,
        _: HostTable,
        _: &[u8],
        _: &[u8],
    ) -> Result<Option<Vec<u8>>, jazz::db::Error> {
        Err(self.error())
    }
    async fn host_delete(&self, _: HostTable, _: &[u8]) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
    async fn host_sql(&self, _: &mut dyn jazz::host::HostSqlBody) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
}

impl Unavailable {
    pub(super) fn error(&self) -> jazz::db::Error {
        jazz::db::Error::new(jazz::db::ErrorCode::Storage, self.0.clone())
    }
}

/// The store thread.
pub(super) struct StoreLoop {
    pub(super) source: Source,
    pub(super) work: std_mpsc::Receiver<Work>,
    /// Hands runtime work back to this loop (a network chunk store's
    /// finished batches).
    pub(super) wake: std_mpsc::Sender<Work>,
    pub(super) host: std_mpsc::Receiver<HostJob>,
    /// Host work taken off the queue but not run yet (a tick slice ended).
    pub(super) backlog: VecDeque<HostJob>,
    pub(super) tick_queued: Arc<AtomicBool>,
    pub(super) fault: Arc<Mutex<Option<String>>>,
    pub(super) app: Arc<Mutex<Option<String>>>,
    pub(super) state: State,
    /// What the running runtime was opened with, to restart it.
    pub(super) started: Option<Runtime>,
    pub(super) restarted: Option<Instant>,
    #[cfg(test)]
    pub(super) ticks: Arc<AtomicU64>,
}

// One per store: its size does not matter.
#[allow(clippy::large_enum_variant)]
pub(super) enum State {
    /// No runtime: the host tables on the file, or why it could not be
    /// opened (tried again on the next host work).
    Idle(Result<StoreFile, String>),
    Running(Owner),
}

impl StoreLoop {
    pub(super) fn run(mut self) {
        loop {
            let inbound = self.owner().is_some_and(|owner| owner.inbound_pending());
            let busy = inbound || !self.backlog.is_empty();
            let first = if busy {
                match self.work.try_recv() {
                    Ok(next) => Some(next),
                    Err(std_mpsc::TryRecvError::Empty) => None,
                    Err(std_mpsc::TryRecvError::Disconnected) => break,
                }
            } else if self.owner().is_some() {
                match self.work.recv_timeout(IDLE_TICK) {
                    Ok(next) => Some(next),
                    Err(std_mpsc::RecvTimeoutError::Timeout) => None,
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                }
            } else {
                match self.work.recv() {
                    Ok(next) => Some(next),
                    Err(_) => break,
                }
            };
            let deadline = Instant::now() + TICK_SLICE;
            // Host work alone does not tick the runtime: a login must not
            // change when sessions' messages are taken.
            let mut tick = inbound || (first.is_none() && !busy);
            let mut next = first;
            while let Some(item) = next.take() {
                self.host_work(deadline);
                tick |= !matches!(item, Work::Host);
                if !self.handle(item) {
                    self.stop();
                    return;
                }
                if Instant::now() >= deadline {
                    break;
                }
                next = self.work.try_recv().ok();
            }
            self.host_work(deadline);
            if tick && self.owner().is_some() {
                self.tick_queued.store(false, Ordering::Release);
                self.on_owner(Owner::tick);
                #[cfg(test)]
                self.ticks.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stop();
    }

    pub(super) fn owner(&self) -> Option<&Owner> {
        match &self.state {
            State::Running(owner) => Some(owner),
            State::Idle(_) => None,
        }
    }

    /// Run queued host work, ahead of runtime work, until `deadline`.
    pub(super) fn host_work(&mut self, deadline: Instant) {
        self.backlog.extend(self.host.try_iter());
        while Instant::now() < deadline
            && let Some(job) = self.backlog.pop_front()
        {
            self.host_job(job);
        }
    }

    pub(super) fn host_job(&mut self, job: HostJob) {
        if let State::Idle(Err(_)) = self.state {
            self.state = State::Idle(self.source.open());
        }
        let ran = match &self.state {
            State::Idle(Ok(file)) => catch_unwind(AssertUnwindSafe(|| job(file))),
            State::Idle(Err(error)) => {
                catch_unwind(AssertUnwindSafe(|| job(&Unavailable(error.clone()))))
            }
            State::Running(owner) => catch_unwind(AssertUnwindSafe(|| {
                job(&owner.db);
                #[cfg(test)]
                owner.assert_holds_nothing("host work");
            })),
        };
        if let Err(panic) = ran {
            let panic = panic_message(&*panic);
            match self.state {
                State::Running(_) => self.fault(format!("host work panicked: {panic}")),
                // Nothing but the connection holds state: open it afresh.
                State::Idle(_) => {
                    tracing::error!(%panic, "server store host work panicked");
                    self.state = State::Idle(Err(String::new()));
                    self.state = State::Idle(self.source.open());
                }
            }
        }
    }

    /// Do `work`; false once the store is to close.
    pub(super) fn handle(&mut self, work: Work) -> bool {
        match work {
            // Run before every item.
            Work::Host => {}
            // Without a runtime the job is dropped, and its caller told so.
            Work::Owner(job) => self.on_owner(job),
            Work::Start(start) => {
                let started = match &self.state {
                    State::Running(_) => {
                        Err("the server runtime is already running".to_owned().into())
                    }
                    State::Idle(_) => self.open_runtime(start.runtime, start.initial),
                };
                let _ = start.reply.send(started);
            }
            Work::Stop(reply) => {
                self.stop();
                let _ = reply.send(());
            }
            Work::Close => return false,
        }
        true
    }

    /// Open the runtime on the file. If it fails, the file is opened afresh
    /// for the host tables: the failed open consumed the connection, and the
    /// store (even an in-memory one) outlives it.
    pub(super) fn open_runtime(
        &mut self,
        runtime: Runtime,
        initial: Option<InitialCatalogue>,
    ) -> Result<(), CatalogueApplyError> {
        let file = match std::mem::replace(&mut self.state, State::Idle(Err(String::new()))) {
            State::Idle(file) => file,
            State::Running(owner) => {
                self.state = State::Running(owner);
                return Err("the server runtime is already running".to_owned().into());
            }
        };
        let opened = file.map_err(CatalogueApplyError::from).and_then(|file| {
            catch_unwind(AssertUnwindSafe(|| {
                Owner::open(
                    runtime.schema.clone(),
                    file,
                    self.source.chunk_store()?,
                    self.wake.clone(),
                    runtime.any_stored_catalogue,
                    self.app.lock().ok().and_then(|app| app.clone()),
                    self.source.reader().is_ok(),
                    initial,
                )
            }))
            .unwrap_or_else(|panic| {
                Err(format!("opening the runtime panicked: {}", panic_message(&*panic)).into())
            })
        });
        match opened {
            Ok(owner) => {
                self.state = State::Running(owner);
                self.started = Some(runtime);
                // A runtime is serving again: the server is healthy.
                if let Ok(mut fault) = self.fault.lock() {
                    *fault = None;
                }
                Ok(())
            }
            Err(error) => {
                self.state = State::Idle(self.source.open());
                Err(error)
            }
        }
    }

    /// Run `f` on the runtime, if one is running.
    pub(super) fn on_owner(&mut self, f: impl FnOnce(&mut Owner)) {
        let State::Running(owner) = &mut self.state else {
            return;
        };
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
            f(owner);
            #[cfg(test)]
            owner.assert_holds_nothing("runtime work");
        })) {
            self.fault(format!(
                "server runtime panicked: {}",
                panic_message(&*panic)
            ));
        }
    }

    /// The runtime panicked: its state can no longer be trusted. Close its
    /// sessions (clients reconnect), reopen the file (rolling back whatever
    /// the panic left uncommitted) and restart the runtime on it, as a
    /// process restart would. A second panic within [`RESTART_INTERVAL`]
    /// leaves it stopped and recorded in [`AppStore::fault`]; the host
    /// tables stay available either way.
    pub(super) fn fault(&mut self, panic: String) {
        let started = self.started.take();
        self.stop();
        tracing::error!(%panic, "server runtime failed");
        let restart = match started {
            Some(runtime)
                if self
                    .restarted
                    .is_none_or(|restarted| restarted.elapsed() >= RESTART_INTERVAL) =>
            {
                self.restarted = Some(Instant::now());
                self.open_runtime(runtime, None).map_err(String::from)
            }
            _ => Err("it failed again within a minute of restarting".to_owned()),
        };
        match restart {
            Ok(()) => tracing::warn!("server runtime restarted after a failure"),
            Err(error) => {
                let fault = format!("{panic}; not restarted: {error}");
                tracing::error!(%fault, "server runtime stopped");
                if let Ok(mut slot) = self.fault.lock() {
                    *slot = Some(fault);
                }
            }
        }
    }

    /// Stop the runtime, if one is running: close its sessions and reopen
    /// the file for the host tables.
    pub(super) fn stop(&mut self) {
        let State::Running(owner) =
            std::mem::replace(&mut self.state, State::Idle(Err(String::new())))
        else {
            return;
        };
        self.started = None;
        // A runtime that panicked may panic again on its way down; the file
        // is reopened either way.
        let _ = catch_unwind(AssertUnwindSafe(move || owner.close()));
        self.state = State::Idle(self.source.open());
    }
}

pub(super) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    }
}

/// Sendable handle for the thread that owns the runtime's store.
#[derive(Clone)]
pub struct ServerRuntimeHandle {
    pub(super) inner: Arc<Inner>,
}

pub(super) struct Inner {
    pub(super) store: AppStore,
    /// The store was opened for this runtime alone: shutdown closes it.
    pub(super) owns_store: bool,
    pub(super) activity: watch::Sender<u64>,
    pub(super) next_session: AtomicU64,
    pub(super) edges: Mutex<HashMap<ServerSession, Edge>>,
    /// [`THIN_INGRESS_SERVER_BUDGET`], as permits of one byte.
    pub(super) thin_ingress: Arc<tokio::sync::Semaphore>,
}

/// The socket side of one session.
pub(super) struct Edge {
    /// Taken by [`ServerRuntimeHandle::open_wire_stream`].
    pub(super) output: Option<tokio_mpsc::UnboundedReceiver<Outbound>>,
    /// A sync session's room for frames read and not yet taken by the owner:
    /// [`SESSION_QUEUE_BUDGET`], as permits of one byte.
    pub(super) ingress: Arc<tokio::sync::Semaphore>,
    /// A thin session's room for requests being joined or queued: the most
    /// one request takes, as permits of one byte.
    pub(super) thin_ingress: Arc<tokio::sync::Semaphore>,
    pub(super) egress: Arc<Egress>,
}

/// Activity subscription for a server runtime.
pub struct ServerRuntimeActivity {
    pub(super) receiver: watch::Receiver<u64>,
}

impl ServerRuntimeActivity {
    /// Wait until something asked sessions to look for new output.
    pub async fn changed(&mut self) -> Result<(), String> {
        self.receiver
            .changed()
            .await
            .map_err(|_| "server runtime activity channel closed".to_owned())
    }
}

/// Take `bytes` the client read off `egress`. Crossing back under the
/// budget queues a tick: output the owner held back can move again.
pub(super) fn release_egress(egress: &AtomicUsize, bytes: usize, runtime: &Weak<Inner>) {
    let before = egress.fetch_sub(bytes, Ordering::AcqRel);
    if before >= SESSION_QUEUE_BUDGET
        && before - bytes < SESSION_QUEUE_BUDGET
        && let Some(inner) = runtime.upgrade()
    {
        let _ = ServerRuntimeHandle { inner }.queue_tick();
    }
}

/// Counts the raw bytes a session's socket writes (see [`Egress`]).
#[derive(Clone)]
pub struct WrittenBytes(Arc<Egress>);

impl WrittenBytes {
    /// The socket wrote `bytes`.
    pub fn add(&self, bytes: usize) {
        self.0.written.fetch_add(bytes as u64, Ordering::AcqRel);
    }
}

/// One session's ordered outbound frame batches.
pub struct ServerRuntimeFrameStream {
    pub(super) receiver: tokio_mpsc::UnboundedReceiver<Outbound>,
    pub(super) egress: Arc<Egress>,
    pub(super) runtime: Weak<Inner>,
}

impl ServerRuntimeFrameStream {
    /// Receive the next batch of outbound frames, its bytes no longer held.
    pub async fn recv(&mut self) -> Option<Result<Vec<AbiBytes>, jazz::wire::WireError>> {
        let (result, bytes) = self.receiver.recv().await?;
        release_egress(&self.egress, bytes, &self.runtime);
        Some(result)
    }

    /// Receive the next batch with its size, its bytes still held: the
    /// caller releases them through [`Self::egress_release`] as it writes
    /// them, so the session's egress counts them until they are written
    /// and a slow reader is seen reading as each part goes.
    pub async fn recv_held(
        &mut self,
    ) -> Option<(Result<Vec<AbiBytes>, jazz::wire::WireError>, usize)> {
        self.receiver.recv().await
    }

    /// Counts the raw bytes the session's socket writes, so a client that
    /// reads slowly is seen reading while a large message is still going.
    pub fn written_bytes(&self) -> WrittenBytes {
        WrittenBytes(Arc::clone(&self.egress))
    }

    /// Releases bytes of [`Self::recv_held`] batches as they are written.
    pub fn egress_release(&self) -> EgressRelease {
        EgressRelease {
            egress: Arc::clone(&self.egress),
            runtime: self.runtime.clone(),
        }
    }
}

/// Releases a session's held output as it is written (see
/// [`ServerRuntimeFrameStream::recv_held`]).
#[derive(Clone)]
pub struct EgressRelease {
    pub(super) egress: Arc<Egress>,
    pub(super) runtime: Weak<Inner>,
}

impl EgressRelease {
    /// `bytes` of the session's output were written.
    pub fn release(&self, bytes: usize) {
        if bytes > 0 {
            release_egress(&self.egress, bytes, &self.runtime);
        }
    }
}

impl ServerRuntimeHandle {
    /// Start a core runtime over `storage`.
    pub fn start_with_storage(schema: JazzSchema, storage: StorageConfig) -> Result<Self, String> {
        Self::start_with_storage_config(schema, storage, NodeRole::Core)
    }

    /// Start a runtime with an explicit role.
    pub fn start_with_storage_config(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
    ) -> Result<Self, String> {
        Self::start(schema, storage, role, false)
    }

    pub(super) fn start(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
        any_stored_catalogue: bool,
    ) -> Result<Self, String> {
        if role == NodeRole::Relay {
            return Err("relay nodes are not supported yet on SQLite".to_owned());
        }
        AppStore::open(&storage)?.start(schema, role, any_stored_catalogue, true)
    }

    /// Start a runtime whose schema comes from the administrative catalogue.
    /// A reopened store keeps everything earlier schemas added; the caller
    /// then re-applies its durable selection.
    pub fn start_with_catalogue_schema(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
    ) -> Result<Self, String> {
        Self::start(schema, storage, role, true)
    }

    /// Subscribe to activity notices.
    pub fn subscribe_activity(&self) -> ServerRuntimeActivity {
        ServerRuntimeActivity {
            receiver: self.inner.activity.subscribe(),
        }
    }

    /// Wake every session to look for new output.
    pub fn notify_activity(&self) {
        self.inner.activity.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// Admit an authenticated session, pinned to `bundle` when its backend
    /// pinned it (see [`jazz::catalogue`]).
    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_session_context(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        trust: CommitUnitTrust,
        _negotiated_features: jazz::wire::WireFeatures,
        _session_context: Option<ConnectionSessionContext>,
        link_admission: ServerLinkAdmission,
    ) -> Result<ServerSession, SessionOpenError> {
        // The SQLite sync protocol has no per-query delegated scope yet, so
        // an authority link is served as the trusted SYSTEM link it is. A
        // client relay (a browser worker relaying for its tabs) is served as
        // the one session it authenticated as: everything it relays is read
        // and written under that session's policies, so it can never reach
        // past that scope, and its tabs share it.
        match link_admission {
            ServerLinkAdmission::OrdinarySession
            | ServerLinkAdmission::AuthorityQueryDelegate
            | ServerLinkAdmission::ScopeIsolatedClientRelay { .. } => {}
        }
        // Trusted links are served with full access; a session reads and
        // writes under its identity's policies.
        let served_as = trust.served_as(identity);
        let session = ServerSession(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(Egress::default());
        self.insert_edge(session, receiver, Arc::clone(&egress))?;
        let opened = self
            .run(move |owner| {
                Ok(owner.open_session(session, served_as, claims, bundle, output, egress))
            })
            .await;
        self.settle_open(session, opened)
    }

    pub(super) fn insert_edge(
        &self,
        session: ServerSession,
        receiver: tokio_mpsc::UnboundedReceiver<Outbound>,
        egress: Arc<Egress>,
    ) -> Result<(), SessionOpenError> {
        self.edges().map_err(SessionOpenError::Runtime)?.insert(
            session,
            Edge {
                output: Some(receiver),
                ingress: Arc::new(tokio::sync::Semaphore::new(SESSION_QUEUE_BUDGET)),
                thin_ingress: Arc::new(tokio::sync::Semaphore::new(
                    jazz::thin::MAX_THIN_MESSAGE_BYTES,
                )),
                egress,
            },
        );
        Ok(())
    }

    /// The opened `session`, or why not (its edge then goes).
    pub(super) fn settle_open(
        &self,
        session: ServerSession,
        opened: Result<Result<(), jazz::db::Error>, String>,
    ) -> Result<ServerSession, SessionOpenError> {
        let opened = match opened {
            Ok(Ok(())) => return Ok(session),
            Ok(Err(error)) => SessionOpenError::Refused(error),
            Err(message) => SessionOpenError::Runtime(message),
        };
        self.edges()
            .map_err(SessionOpenError::Runtime)?
            .remove(&session);
        Err(opened)
    }

    /// Take the session's ordered output stream. Output produced before
    /// this call is kept.
    pub fn open_wire_stream(
        &self,
        session: ServerSession,
    ) -> Result<ServerRuntimeFrameStream, String> {
        let mut edges = self.edges()?;
        let edge = edges
            .get_mut(&session)
            .ok_or_else(|| "unknown server session".to_owned())?;
        let receiver = edge
            .output
            .take()
            .ok_or_else(|| "session already has a wire stream".to_owned())?;
        Ok(ServerRuntimeFrameStream {
            receiver,
            egress: Arc::clone(&edge.egress),
            runtime: Arc::downgrade(&self.inner),
        })
    }

    /// The room a batch of `bytes` takes: all of the budget for a batch
    /// larger than it, so one never waits for room that cannot come.
    pub(super) fn wire_room(
        &self,
        session: ServerSession,
        bytes: usize,
    ) -> Result<(Arc<tokio::sync::Semaphore>, u32), String> {
        let room = self
            .edges()?
            .get(&session)
            .map(|edge| Arc::clone(&edge.ingress))
            .ok_or_else(|| "unknown server session".to_owned())?;
        let permits =
            u32::try_from(bytes.clamp(1, SESSION_QUEUE_BUDGET)).expect("the budget fits u32");
        Ok((room, permits))
    }

    /// Room for a batch of `bytes` read off `session`'s socket, if its
    /// ingress budget has it now.
    pub fn try_admit_wire_frames(
        &self,
        session: ServerSession,
        bytes: usize,
    ) -> Result<Option<WireIngress>, String> {
        let (room, permits) = self.wire_room(session, bytes)?;
        match room.try_acquire_many_owned(permits) {
            Ok(room) => Ok(Some(WireIngress { _room: room })),
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => Err("server runtime stopped".to_owned()),
        }
    }

    /// Wait for room for a batch of `bytes` in `session`'s ingress budget:
    /// until the owner has taken enough of the frames before it.
    pub async fn admit_wire_frames(
        &self,
        session: ServerSession,
        bytes: usize,
    ) -> Result<WireIngress, String> {
        let (room, permits) = self.wire_room(session, bytes)?;
        room.acquire_many_owned(permits)
            .await
            .map(|room| WireIngress { _room: room })
            .map_err(|_| "server runtime stopped".to_owned())
    }

    /// Queue inbound frames (each one `postcard(SyncMessage)`) for the
    /// engine, in the room [`Self::admit_wire_frames`] made for them, which
    /// frees once the owner has taken them.
    pub fn receive_wire_frames(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
        ingress: WireIngress,
    ) -> Result<(), String> {
        if frames.is_empty() {
            return Ok(());
        }
        if !self.edges()?.contains_key(&session) {
            return Err("unknown server session".to_owned());
        }
        self.send(Box::new(move |owner| {
            owner.receive(session, frames, ingress)
        }))
    }

    /// Admit an authenticated thin client connection (SPEC 8.14). It is
    /// served as its session, or as SYSTEM for a trusted link, like a sync
    /// session, pinned to `bundle` when its backend pinned it, and takes
    /// its output from [`Self::open_wire_stream`].
    pub async fn open_thin_session(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, SessionOpenError> {
        let served_as = trust.served_as(identity);
        let session = ServerSession(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(Egress::default());
        self.insert_edge(session, receiver, Arc::clone(&egress))?;
        let opened = self
            .run(move |owner| {
                Ok(owner.open_thin_session(session, served_as, claims, bundle, output, egress))
            })
            .await;
        self.settle_open(session, opened)
    }

    /// Wait for room for a thin request `bytes` long on the wire, before
    /// reading or joining it: in `session`'s budget (the most one request
    /// takes, [`jazz::thin::MAX_THIN_MESSAGE_BYTES`]) and, for a request
    /// `split` into parts, the server's (`THIN_INGRESS_SERVER_BUDGET`).
    /// While it waits, its reader reads nothing more, so the client's
    /// socket backs up instead of the session ending; the room frees as the
    /// owner runs queued requests.
    pub async fn admit_thin_request(
        &self,
        session: ServerSession,
        bytes: usize,
        split: bool,
    ) -> Result<ThinIngress, String> {
        let permits = u32::try_from(bytes)
            .ok()
            .filter(|_| bytes <= jazz::thin::MAX_THIN_MESSAGE_BYTES)
            .ok_or_else(|| format!("a thin request of {bytes} bytes is over the most one takes"))?;
        let room = self
            .edges()?
            .get(&session)
            .map(|edge| Arc::clone(&edge.thin_ingress))
            .ok_or_else(|| "unknown server session".to_owned())?;
        let closed = |_| "server runtime stopped".to_owned();
        // Session first: a session waiting on itself holds none of the
        // server's room.
        let session_room = room.acquire_many_owned(permits).await.map_err(closed)?;
        let server_room = if split {
            Some(
                Arc::clone(&self.inner.thin_ingress)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(closed)?,
            )
        } else {
            None
        };
        Ok(ThinIngress {
            _session: session_room,
            _server: server_room,
        })
    }

    /// Queue a thin client's request for the owner, in the room
    /// [`Self::admit_thin_request`] made for it, which frees once it ran.
    pub fn receive_thin_request(
        &self,
        session: ServerSession,
        request: jazz::thin::ThinRequest,
        ingress: ThinIngress,
    ) -> Result<(), String> {
        if !self.edges()?.contains_key(&session) {
            return Err("unknown server session".to_owned());
        }
        self.send(Box::new(move |owner| {
            owner.thin_request(session, request, ingress);
        }))
    }

    /// Count `bytes` against `session`'s egress budget as if they were
    /// queued and never drained, to test output near the budget.
    #[cfg(test)]
    pub(crate) fn hold_egress_for_test(&self, session: ServerSession, bytes: usize) {
        let edges = self.edges().expect("edges");
        edges[&session].egress.fetch_add(bytes, Ordering::AcqRel);
    }

    /// Release `bytes` held by [`Self::hold_egress_for_test`], as the
    /// client reading them would.
    #[cfg(test)]
    pub(crate) fn release_egress_for_test(&self, session: ServerSession, bytes: usize) {
        let egress = Arc::clone(&self.edges().expect("edges")[&session].egress);
        release_egress(&egress, bytes, &Arc::downgrade(&self.inner));
    }

    /// Apply `limits` from now on (sessions opened after this call take
    /// its per-session bounds).
    #[cfg(test)]
    pub(crate) async fn set_egress_limits_for_test(&self, limits: EgressLimits) {
        self.run(move |owner| {
            owner.limits = limits;
            Ok(())
        })
        .await
        .expect("owner");
    }

    /// Sessions this runtime closed because their client fell behind.
    pub async fn egress_sheds(&self) -> Result<EgressSheds, String> {
        self.run(|owner| Ok(owner.sheds)).await
    }

    /// Whether `session` is an open thin session.
    #[cfg(test)]
    pub(crate) async fn is_thin_session_for_test(&self, session: ServerSession) -> bool {
        self.run(move |owner| Ok(owner.thin.contains_key(&session)))
            .await
            .expect("owner")
    }

    /// Wait until the owner has run everything queued before this call.
    #[cfg(test)]
    pub(crate) async fn settle_for_test(&self) {
        self.run(|_| Ok(())).await.expect("owner");
    }

    /// Ask the owner for a tick; requests coalesce. Output reaches every
    /// session's stream after each tick.
    pub fn request_wire_tick(&self, _session: ServerSession) -> Result<(), String> {
        self.queue_tick()
    }

    pub(super) fn queue_tick(&self) -> Result<(), String> {
        if self
            .inner
            .store
            .inner
            .tick_queued
            .swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }
        self.send(Box::new(|_| {}))
    }

    /// Close a session.
    pub fn close(&self, session: ServerSession) {
        if let Ok(mut edges) = self.edges() {
            edges.remove(&session);
        }
        let _ = self.send(Box::new(move |owner| owner.close_session(session)));
    }

    /// Stop the runtime and wait for it. A runtime started on a store of its
    /// own also closes the store (it is closed when this returns); one
    /// started on an [`AppStore`] leaves the store's host tables to it.
    /// Safe to call more than once.
    pub async fn shutdown(&self) -> Result<(), String> {
        let store = self.inner.store.clone();
        let owns_store = self.inner.owns_store;
        tokio::task::spawn_blocking(move || {
            if owns_store {
                store.close()
            } else {
                // A closed store has no runtime left to stop.
                store.stop_runtime().or_else(|error| match store.sender() {
                    Err(_) => Ok(()),
                    Ok(_) => Err(error),
                })
            }
        })
        .await
        .map_err(|error| format!("server runtime shutdown task failed: {error}"))?
    }

    // ---- catalogue ---------------------------------------------------------

    /// Admit `schema` with its lens from an already admitted schema.
    pub async fn publish_schema_with_lens(
        &self,
        schema: JazzSchema,
        lens: MigrationLens,
    ) -> Result<SchemaVersionId, String> {
        self.run(move |owner| {
            LensEffects::of(&lens).map_err(|error| error.to_string())?;
            if !owner.catalogue.schemas.contains_key(&lens.source) {
                return Err("lens source schema is not admitted".to_owned());
            }
            let id = schema.version_id();
            let mut lenses = owner.catalogue.lenses_for(id);
            if !lenses.iter().any(|admitted| admitted.id == lens.id) {
                lenses.push(lens.clone());
            }
            // Clients on the published version read and write by it before
            // (and without) it becoming current.
            block_on(owner.db.admit_schema_with_lenses(schema, &lenses))
                .map_err(|error| error.to_string())?;
            owner.reload_catalogue()?;
            owner.catalogue.admit_lens(id, lens);
            Ok(id)
        })
        .await
    }

    /// Whether `schema` is admitted.
    pub async fn runtime_catalogue_contains_schema(
        &self,
        schema: SchemaVersionId,
    ) -> Result<bool, String> {
        self.run(move |owner| Ok(owner.catalogue.schemas.contains_key(&schema)))
            .await
    }

    /// Whether `schema` and `lens` are admitted.
    #[doc(hidden)]
    pub async fn runtime_catalogue_contains(
        &self,
        schema: SchemaVersionId,
        lens: MigrationLensId,
    ) -> Result<(bool, bool), String> {
        self.run(move |owner| {
            Ok((
                owner.catalogue.schemas.contains_key(&schema),
                owner
                    .catalogue
                    .incoming
                    .values()
                    .flatten()
                    .any(|admitted| admitted.id == lens),
            ))
        })
        .await
    }

    /// The store's catalogue: every schema, lens and permissions revision.
    pub async fn catalogue(&self) -> Result<CatalogueRows, String> {
        self.run(|owner| block_on(owner.db.catalogue()).map_err(|error| error.to_string()))
            .await
    }

    /// The current schema, with its policies.
    #[doc(hidden)]
    pub async fn current_schema(&self) -> Result<JazzSchema, String> {
        self.run(|owner| Ok(owner.db.current_schema())).await
    }

    /// Apply `change` to the store's catalogue in one transaction (see
    /// [`Db::apply_catalogue`]): a change that fails leaves nothing behind.
    ///
    /// With `supersedes`, the change publishes that bundle new to this
    /// store, and every session pinned to another bundle ends, told to
    /// reconnect later, in the same job as the commit:
    /// a publish that commits always ends them, so a deployment retried
    /// after a later failure (which then changes nothing) leaves no session
    /// on the permissions it replaced.
    pub async fn apply_catalogue(
        &self,
        change: CatalogueChange,
        supersedes: Option<jazz::catalogue::BundleId>,
    ) -> Result<(), CatalogueApplyError> {
        self.apply_catalogue_keeping(change, supersedes, Vec::new())
            .await
    }

    /// [`Self::apply_catalogue`] that also keeps `kept` host-table values
    /// (table, key, value) in the same transaction: a key holding a different
    /// value fails the whole change ([`Db::apply_catalogue_keeping`]).
    pub async fn apply_catalogue_keeping(
        &self,
        change: CatalogueChange,
        supersedes: Option<jazz::catalogue::BundleId>,
        kept: crate::portable::deployment::KeptValues,
    ) -> Result<(), CatalogueApplyError> {
        let (ended, result) = self
            .run(move |owner| Ok(owner.apply_catalogue_keeping(change, supersedes, &kept)))
            .await
            .unwrap_or_else(|error| (0, Err(CatalogueApplyError::Failed(error))));
        // Sessions that ended are gone even when a later step failed: the
        // runtime loop still has to see that.
        if result.is_ok() || ended > 0 {
            self.notify_activity();
        }
        result
    }

    /// Read the store's catalogue again into the runtime's view of it,
    /// which a change that committed and then failed may have left behind.
    pub async fn reload_catalogue(&self) -> Result<(), String> {
        self.run(|owner| owner.reload_catalogue()).await
    }

    /// Whether the app the runtime serves has Jazz's E2EE system tables.
    pub async fn encrypts(&self) -> Result<bool, String> {
        self.run(|owner| Ok(owner.encrypts())).await
    }

    /// Whether the runtime has the account registry its checks read, which
    /// an app that encrypts columns needs.
    pub async fn has_account_registry(&self) -> Result<bool, String> {
        self.run(|owner| Ok(owner.registry_installed)).await
    }

    /// Tests: how many sessions (sync and thin) the runtime serves now.
    #[cfg(test)]
    pub(crate) async fn open_sessions_for_test(&self) -> usize {
        self.run(|owner| Ok(owner.sessions.len() + owner.thin.len()))
            .await
            .unwrap()
    }

    /// Tests: the next [`Self::apply_catalogue`] fails after its commit
    /// (and after ending superseded sessions), as a failed catalogue reload.
    #[cfg(test)]
    pub(crate) async fn fail_next_catalogue_reload_for_test(&self) {
        self.run(|owner| {
            owner.fail_next_reload = true;
            Ok(())
        })
        .await
        .unwrap();
    }

    /// Tests: run `f` on the runtime's database.
    #[cfg(test)]
    pub(crate) async fn with_db_for_test<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Db) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        self.run(move |owner| f(&owner.db)).await
    }

    pub(super) fn edges(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<ServerSession, Edge>>, String> {
        self.inner
            .edges
            .lock()
            .map_err(|_| "server runtime session lock poisoned".to_owned())
    }

    pub(super) fn send(&self, job: Job) -> Result<(), String> {
        self.inner.store.send(Work::Owner(job))
    }

    pub(super) async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Owner) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let (reply, response) = oneshot::channel();
        self.send(Box::new(move |owner| {
            let _ = reply.send(job(owner));
        }))?;
        response
            .await
            .map_err(|_| "server runtime thread dropped the response".to_owned())?
    }
}
