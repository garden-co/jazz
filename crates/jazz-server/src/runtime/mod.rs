//! The server's semantic runtime: one SQLite [`Db`] owned by a dedicated
//! thread, driven by commands from request and WebSocket tasks.
//!
//! The engine is single-threaded (`Rc`/`RefCell`), so every engine call runs
//! on the owner thread. Handles are `Send` and cheap to clone. Each
//! WebSocket session is one subscriber of the engine; its inbound frames are
//! queued for the owner and its outbound messages stream back in order.
//! Shutdown joins the owner, so the store is closed when it returns.
//!
//! One app's state is one SQLite file: the owner thread is also the only
//! connection to it. It starts before the runtime does (an [`AppStore`]),
//! serving the server's host tables (catalogue, account registry), and the
//! runtime's [`Db`] opens on the same connection once a schema is known.

pub mod alpha59_alpha60;
pub mod auth_admission;
mod thin;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc as std_mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use crate::kv::HostKv;
use crate::kv::HostSql;
use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, LensEffects, SubscriberId, SyncMessage, Transport,
    TransportError, block_on,
};
use jazz::host::{HostReader, HostTable, HostTables, StoreFile};
use jazz::ids::{AuthorSubject, MigrationLensId, NodeUuid, SchemaVersionId};
use jazz::protocol::MigrationLens;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{TableName, TablePolicies};
use jazz::wire::{WireAuthorityEndpoint, WireErrorCode, WireFeatures, WireRetry};
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};

/// Encoded frames exchanged with a host transport.
pub type AbiBytes = Vec<u8>;

/// Where the runtime keeps its store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageConfig {
    /// A store that lives as long as the runtime (tests, dry runs, probes).
    InMemory,
    /// One SQLite file (WAL), holding the server's host tables too.
    SQLite {
        /// The database file.
        path: PathBuf,
        /// Where settled large-value bytes live.
        blobs: Blobs,
    },
}

/// Where a SQLite store keeps settled large-value bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Blobs {
    /// In the database file itself.
    #[default]
    InFile,
    /// As files in this directory ([`jazz::chunk_store::FileChunkStore`]).
    Files(PathBuf),
    /// In a store reached over the network, such as an S3 bucket
    /// ([`crate::s3::S3ChunkStore`]). Its I/O runs beside the store thread,
    /// so a slow bucket never stalls sync.
    Network(NetworkChunkStore),
}

/// A chunk store reached over the network, for [`Blobs::Network`].
#[derive(Clone)]
pub struct NetworkChunkStore {
    pub store: Arc<dyn jazz::chunk_store::ChunkStore>,
    /// What it is, for logs.
    pub name: String,
}

impl std::fmt::Debug for NetworkChunkStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetworkChunkStore({})", self.name)
    }
}

impl PartialEq for NetworkChunkStore {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.store, &other.store)
    }
}

impl Eq for NetworkChunkStore {}

impl StorageConfig {
    /// The durable store inside a server data directory.
    pub fn data_dir(path: impl Into<PathBuf>) -> Self {
        Self::SQLite {
            path: path.into().join("jazz.sqlite"),
            blobs: Blobs::InFile,
        }
    }

    /// The same store, with settled large-value bytes kept as files in
    /// `dir` instead of in the database file. In memory it changes nothing.
    pub fn with_blob_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.with_blobs(Blobs::Files(dir.into()))
    }

    /// The same store, with settled large-value bytes in `blobs`. In memory
    /// it changes nothing.
    pub fn with_blobs(self, blobs: Blobs) -> Self {
        match self {
            Self::SQLite { path, .. } => Self::SQLite { path, blobs },
            Self::InMemory => Self::InMemory,
        }
    }
}

/// Deployment role of a server node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeRole {
    /// Stateless relay in front of a core. Not supported yet on SQLite.
    Relay,
    /// Durable core node that owns authoritative storage.
    Core,
}

/// How much the server trusts a link's commits, chosen at admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// An end-user session: every read and write is checked against the
    /// table policies for its identity.
    Session,
    /// An authenticated backend. It is served with full access.
    TrustedBackend,
    /// An authenticated administrative or authority link, served with full
    /// access.
    TrustedAuthority,
}

/// Authority endpoints and features negotiated for one connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSessionContext {
    /// This server's endpoint for the connection.
    pub local: WireAuthorityEndpoint,
    /// The peer's endpoint, when it advertised one.
    pub remote: Option<WireAuthorityEndpoint>,
    /// The admitted identity of the link.
    pub link_identity: AuthorSubject,
    /// The negotiated wire features.
    pub negotiated_features: WireFeatures,
}

/// Capability selected by the server after authenticating a WebSocket and
/// negotiating its features.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ServerLinkAdmission {
    /// A normal session or backend link.
    #[default]
    OrdinarySession,
    /// Host-verified SYSTEM authority credential permitting scoped query
    /// delegation. On SQLite it is served as a trusted SYSTEM link; the sync
    /// protocol cannot express a delegated scope yet.
    AuthorityQueryDelegate,
    /// A durable relay for exactly the one session authenticated by this
    /// connection. On SQLite it is served as that session.
    ScopeIsolatedClientRelay {
        /// Fresh server-issued capability epoch for this accepted connection.
        admission_epoch: u64,
    },
}

/// Opaque handle for one admitted session.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServerSession(u64);

/// Bytes a session may have queued in each direction. Inbound frames past
/// it wait, unread, until the owner has taken earlier ones, so a fast peer
/// is held back by its transport; outbound messages past it wait in the
/// engine until the socket catches up.
pub(crate) const SESSION_QUEUE_BUDGET: usize = 8 * 1024 * 1024;
/// Bytes of split thin requests (each over a wire frame) the whole server
/// may hold, being joined from parts or queued for the owner. A split past
/// it waits for room before its parts are read, so many large writes at
/// once slow down instead of growing memory without bound. Requests that
/// fit one frame are bounded by their session's room alone, so they never
/// queue behind large writes of other sessions.
pub(crate) const THIN_INGRESS_SERVER_BUDGET: usize = 256 * 1024 * 1024;
/// The slow-query threshold: `JAZZ_SLOW_QUERY_MS` (0 turns the log off),
/// else jazz-db's default.
fn slow_query_threshold() -> Option<Duration> {
    match std::env::var("JAZZ_SLOW_QUERY_MS")
        .ok()
        .and_then(|ms| ms.trim().parse::<u64>().ok())
    {
        Some(0) => None,
        Some(ms) => Some(Duration::from_millis(ms)),
        None => Some(jazz::slow::DEFAULT_SLOW_QUERY_THRESHOLD),
    }
}

/// How often the owner ticks with no traffic, for time-based engine work
/// such as expiring staged large-value chunks.
pub(crate) const IDLE_TICK: Duration = Duration::from_secs(1);

/// Inbound messages one session may have applied per round of ticks.
const INBOUND_MESSAGES_PER_TICK: usize = 16;

/// About how long one tick may take. Each tick serves as many sessions as
/// the recent per-session cost fits in it (at least one). A round serves
/// every session with inbound messages once, over as many ticks as that
/// takes, before any is served again: a session waits at most a round, and
/// host work queued meanwhile (catalogue writes, registrations) runs
/// between the ticks. A served session's queued messages (up to
/// [`INBOUND_MESSAGES_PER_TICK`]) are taken in one tick: clients rely on a
/// read sent just before their writes being answered after them. Smaller
/// ticks cost no throughput: each message commits on its own either way.
const TICK_SLICE: Duration = Duration::from_millis(4);

type Job = Box<dyn FnOnce(&mut Owner) + Send>;
/// Work on the store's host tables, before or after the runtime opened.
pub(crate) type HostJob = Box<dyn FnOnce(&dyn HostTables) + Send>;

/// After a panic the runtime restarts on the store at most this often; a
/// second panic sooner leaves it stopped and the store reports the fault.
const RESTART_INTERVAL: Duration = Duration::from_secs(60);

/// What the store thread is asked to do. Host work has its own queue, which
/// the thread runs before every runtime job and tick, up to a tick slice of
/// work at a time: a host write waits for at most the one job or tick slice
/// in progress, and runtime ticks still run while host work keeps coming.
enum Work {
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
struct Runtime {
    schema: JazzSchema,
    any_stored_catalogue: bool,
}

/// Open the runtime on the store.
struct Start {
    runtime: Runtime,
    reply: std_mpsc::Sender<Result<(), String>>,
}

/// Sendable handle for the thread that owns one app's store file: its host
/// tables from the start, and the runtime once [`AppStore::start_runtime`]
/// opened it.
#[derive(Clone)]
pub struct AppStore {
    inner: Arc<StoreThread>,
}

struct StoreThread {
    work: Mutex<Option<std_mpsc::Sender<Work>>>,
    host: std_mpsc::Sender<HostJob>,
    /// Committed host tables, read without waiting for the store's thread.
    /// An in-memory store lives as long as this connection to it.
    reader: Arc<HostReader>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    tick_queued: Arc<AtomicBool>,
    fault: Arc<Mutex<Option<String>>>,
    /// The app the store serves (see [`AppStore::set_app`]).
    app: Arc<Mutex<Option<String>>>,
    /// Tests: runtime ticks so far.
    #[cfg(test)]
    ticks: Arc<AtomicU64>,
}

/// The file a store lives in.
#[derive(Clone)]
enum Source {
    /// The database file, and where its large-value bytes live.
    File(PathBuf, Blobs),
    /// An in-memory store shared by name within this process.
    Memory(String),
}

impl Source {
    fn new(storage: &StorageConfig) -> Self {
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

    fn open(&self) -> Result<StoreFile, String> {
        match self {
            Self::Memory(name) => StoreFile::shared_memory(name),
            Self::File(path, _) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|error| format!("create {}: {error}", parent.display()))?;
                }
                // Every commit (rows, catalogue, account registry) is durable
                // when it returns.
                StoreFile::open(
                    path,
                    FileOptions {
                        synchronous_full: true,
                        ..FileOptions::default()
                    },
                )
            }
        }
        .map_err(|error| format!("open server store: {error}"))
    }

    /// The chunk store the runtime's large-value bytes go to, if not the
    /// database file.
    fn chunk_store(&self) -> Result<Option<ChunkStoreChoice>, String> {
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

    fn reader(&self) -> Result<HostReader, String> {
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
struct ChunkStoreChoice {
    store: Arc<dyn jazz::chunk_store::ChunkStore>,
    network: bool,
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
struct ChunkIoRunner {
    batches: Lane,
    out: std::cell::Cell<bool>,
    /// How long until the next batch, after batches failed.
    backoff: Backoff,
    fetches: Lane,
    /// Whether fetches are out on [`Self::fetches`].
    fetching: std::cell::Cell<bool>,
    /// How long until the next fetches, after fetches failed.
    fetch_backoff: Backoff,
}

/// What a [`ChunkIoRunner`] may hand over: a batch (`fetches`: with what
/// reads wait on in it), and fetches on their own lane.
#[derive(Debug, PartialEq, Eq)]
struct Plan {
    batch: bool,
    fetches: bool,
    fetch_lane: bool,
}

/// Consecutive failures of one kind of chunk I/O, and when it may go
/// again: 1 s after the first, doubling up to a minute.
#[derive(Default)]
struct Backoff {
    failures: std::cell::Cell<u32>,
    not_before: std::cell::Cell<Option<std::time::Instant>>,
    /// The workload token generation the last failure saw: a fresh token
    /// (often what the failure lacked) ends the wait, once per run of
    /// failures, so a store that is down for another reason is not asked
    /// again on every token rotation.
    failed_on_token: std::cell::Cell<u64>,
    token_ended_wait: std::cell::Cell<bool>,
}

impl Backoff {
    /// Whether the last failure's wait is still on, and no fresh workload
    /// token ended it.
    fn waiting(&self) -> bool {
        self.waiting_at(crate::middleware::workload::workload_token_generation())
    }

    /// [`Self::waiting`] with the workload token generation now.
    fn waiting_at(&self, generation: u64) -> bool {
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
    fn ended(&self, failed: bool) -> bool {
        self.ended_at(
            failed,
            crate::middleware::workload::workload_token_generation(),
        )
    }

    /// [`Self::ended`] with the workload token generation now.
    fn ended_at(&self, failed: bool, generation: u64) -> bool {
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
struct Lane {
    sender: std_mpsc::Sender<jazz::chunk_store::ChunkIo>,
    /// Dropped to cancel the work in flight.
    cancel: Option<futures::channel::oneshot::Sender<()>>,
    /// Disconnects once the thread has ended.
    ended: std_mpsc::Receiver<()>,
}

/// The first wait after a failed batch, and the longest.
const CHUNK_IO_BACKOFF: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(60));
/// How long dropping a runner waits for its cancelled batch to stop.
const CHUNK_IO_STOP: Duration = Duration::from_secs(5);

impl Lane {
    fn start(
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

    fn send(&self, io: jazz::chunk_store::ChunkIo) -> bool {
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
    fn start(
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
    fn pump(&self, owner: &Owner) {
        let Plan {
            batch,
            fetches,
            fetch_lane,
        } = self.plan();
        if batch {
            let taken = if fetches {
                owner.db.take_chunk_io()
            } else {
                owner.db.take_chunk_io_holding_fetches()
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
        match owner.db.take_chunk_fetches() {
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
    fn plan(&self) -> Plan {
        let fetches = !self.fetch_backoff.waiting();
        Plan {
            batch: !self.out.get() && !self.backoff.waiting(),
            fetches,
            fetch_lane: fetches && !self.fetching.get(),
        }
    }

    /// A batch came back; `failed` delays the next. Returns whether to log
    /// the failure: the first of a run, then once the wait is at its longest.
    fn returned(&self, failed: bool) -> bool {
        self.out.set(false);
        self.backoff.ended(failed)
    }

    /// [`Self::returned`] for a batch whose fetches (`None`: it had none)
    /// failed or not: they delay fetches in both lanes, as the fetch
    /// lane's do.
    fn returned_with(&self, failed: bool, fetches_failed: Option<bool>) -> bool {
        if let Some(fetches_failed) = fetches_failed {
            self.fetch_backoff.ended(fetches_failed);
        }
        self.returned(failed)
    }

    /// Fetches came back; `failed` delays the next, in both lanes. Returns
    /// whether to log the failure, as [`Self::returned`] does.
    fn fetched(&self, failed: bool) -> bool {
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
    #[cfg(test)]
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

    fn sender(&self) -> Result<std_mpsc::Sender<Work>, String> {
        self.inner
            .work
            .lock()
            .map_err(|_| "server store lock poisoned".to_owned())?
            .clone()
            .ok_or_else(|| "server store is closed".to_owned())
    }

    fn send(&self, work: Work) -> Result<(), String> {
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

    fn start(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
        owns_store: bool,
    ) -> Result<ServerRuntimeHandle, String> {
        if role == NodeRole::Relay {
            return Err("relay nodes are not supported yet on SQLite".to_owned());
        }
        let (reply, started) = std_mpsc::channel();
        self.send(Work::Start(Start {
            runtime: Runtime {
                schema,
                any_stored_catalogue,
            },
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
    host: std_mpsc::Sender<HostJob>,
    wake: std_mpsc::Sender<Work>,
}

impl HostSender {
    pub(crate) fn send(&self, job: HostJob) -> Result<(), String> {
        let closed = "server store is closed";
        self.host.send(job).map_err(|_| closed.to_owned())?;
        self.wake.send(Work::Host).map_err(|_| closed.to_owned())
    }
}

/// Host tables of a store that could not be opened: each use reports why.
struct Unavailable(String);

impl HostTables for Unavailable {
    fn catalogue(&self) -> Result<CatalogueRows, jazz::db::Error> {
        Err(self.error())
    }

    fn catalogue_put(&self, _: &jazz::catalogue::CataloguePut) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }

    fn host_get(&self, _: HostTable, _: &[u8]) -> Result<Option<Vec<u8>>, jazz::db::Error> {
        Err(self.error())
    }
    fn host_prefix(
        &self,
        _: HostTable,
        _: &[u8],
    ) -> Result<Vec<jazz::host::HostPair>, jazz::db::Error> {
        Err(self.error())
    }
    fn host_put_many(
        &self,
        _: HostTable,
        _: &[jazz::host::HostPair],
    ) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
    fn host_put_if_absent(
        &self,
        _: HostTable,
        _: &[u8],
        _: &[u8],
    ) -> Result<Option<Vec<u8>>, jazz::db::Error> {
        Err(self.error())
    }
    fn host_delete(&self, _: HostTable, _: &[u8]) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
    fn host_sql(
        &self,
        _: &mut dyn FnMut(&rusqlite::Connection) -> Result<(), jazz::db::Error>,
    ) -> Result<(), jazz::db::Error> {
        Err(self.error())
    }
}

impl Unavailable {
    fn error(&self) -> jazz::db::Error {
        jazz::db::Error::new(jazz::db::ErrorCode::Storage, self.0.clone())
    }
}

/// The store thread.
struct StoreLoop {
    source: Source,
    work: std_mpsc::Receiver<Work>,
    /// Hands runtime work back to this loop (a network chunk store's
    /// finished batches).
    wake: std_mpsc::Sender<Work>,
    host: std_mpsc::Receiver<HostJob>,
    /// Host work taken off the queue but not run yet (a tick slice ended).
    backlog: VecDeque<HostJob>,
    tick_queued: Arc<AtomicBool>,
    fault: Arc<Mutex<Option<String>>>,
    app: Arc<Mutex<Option<String>>>,
    state: State,
    /// What the running runtime was opened with, to restart it.
    started: Option<Runtime>,
    restarted: Option<Instant>,
    #[cfg(test)]
    ticks: Arc<AtomicU64>,
}

// One per store: its size does not matter.
#[allow(clippy::large_enum_variant)]
enum State {
    /// No runtime: the host tables on the file, or why it could not be
    /// opened (tried again on the next host work).
    Idle(Result<StoreFile, String>),
    Running(Owner),
}

impl StoreLoop {
    fn run(mut self) {
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

    fn owner(&self) -> Option<&Owner> {
        match &self.state {
            State::Running(owner) => Some(owner),
            State::Idle(_) => None,
        }
    }

    /// Run queued host work, ahead of runtime work, until `deadline`.
    fn host_work(&mut self, deadline: Instant) {
        self.backlog.extend(self.host.try_iter());
        while Instant::now() < deadline
            && let Some(job) = self.backlog.pop_front()
        {
            self.host_job(job);
        }
    }

    fn host_job(&mut self, job: HostJob) {
        if let State::Idle(Err(_)) = self.state {
            self.state = State::Idle(self.source.open());
        }
        let ran = match &self.state {
            State::Idle(Ok(file)) => catch_unwind(AssertUnwindSafe(|| job(file))),
            State::Idle(Err(error)) => {
                catch_unwind(AssertUnwindSafe(|| job(&Unavailable(error.clone()))))
            }
            State::Running(owner) => catch_unwind(AssertUnwindSafe(|| job(&owner.db))),
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
    fn handle(&mut self, work: Work) -> bool {
        match work {
            // Run before every item.
            Work::Host => {}
            // Without a runtime the job is dropped, and its caller told so.
            Work::Owner(job) => self.on_owner(job),
            Work::Start(start) => {
                let started = match &self.state {
                    State::Running(_) => Err("the server runtime is already running".to_owned()),
                    State::Idle(_) => self.open_runtime(start.runtime),
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
    fn open_runtime(&mut self, runtime: Runtime) -> Result<(), String> {
        let file = match std::mem::replace(&mut self.state, State::Idle(Err(String::new()))) {
            State::Idle(file) => file,
            State::Running(owner) => {
                self.state = State::Running(owner);
                return Err("the server runtime is already running".to_owned());
            }
        };
        let opened = file.and_then(|file| {
            catch_unwind(AssertUnwindSafe(|| {
                Owner::open(
                    runtime.schema.clone(),
                    file,
                    self.source.chunk_store()?,
                    self.wake.clone(),
                    runtime.any_stored_catalogue,
                    self.app.lock().ok().and_then(|app| app.clone()),
                )
            }))
            .unwrap_or_else(|panic| {
                Err(format!(
                    "opening the runtime panicked: {}",
                    panic_message(&*panic)
                ))
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
    fn on_owner(&mut self, f: impl FnOnce(&mut Owner)) {
        let State::Running(owner) = &mut self.state else {
            return;
        };
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| f(owner))) {
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
    fn fault(&mut self, panic: String) {
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
                self.open_runtime(runtime)
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
    fn stop(&mut self) {
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

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
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
    inner: Arc<Inner>,
}

struct Inner {
    store: AppStore,
    /// The store was opened for this runtime alone: shutdown closes it.
    owns_store: bool,
    activity: watch::Sender<u64>,
    next_session: AtomicU64,
    edges: Mutex<HashMap<ServerSession, Edge>>,
    /// [`THIN_INGRESS_SERVER_BUDGET`], as permits of one byte.
    thin_ingress: Arc<tokio::sync::Semaphore>,
}

/// The socket side of one session.
struct Edge {
    /// Taken by [`ServerRuntimeHandle::open_wire_stream`].
    output: Option<tokio_mpsc::UnboundedReceiver<Outbound>>,
    /// A sync session's room for frames read and not yet taken by the owner:
    /// [`SESSION_QUEUE_BUDGET`], as permits of one byte.
    ingress: Arc<tokio::sync::Semaphore>,
    /// A thin session's room for requests being joined or queued: the most
    /// one request takes, as permits of one byte.
    thin_ingress: Arc<tokio::sync::Semaphore>,
    egress: Arc<AtomicUsize>,
}

/// Room for one thin request, in its session's and the server's ingress
/// budgets, held from before its bytes are buffered until the owner has
/// run it.
pub struct ThinIngress {
    _session: tokio::sync::OwnedSemaphorePermit,
    _server: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Room for one batch of sync frames in its session's ingress budget, held
/// from when it was read until the owner has taken it.
pub struct WireIngress {
    _room: tokio::sync::OwnedSemaphorePermit,
}

/// A session's next frame batch, or the error frame that ends it.
type Outbound = (Result<Vec<AbiBytes>, jazz::wire::WireError>, usize);

/// Activity subscription for a server runtime.
pub struct ServerRuntimeActivity {
    receiver: watch::Receiver<u64>,
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

/// The error frame for a session the engine closed over a message it sent.
/// Overload and storage failures are the server's state and can pass, so the
/// client reconnects with backoff. Anything else fails the same way when the
/// client reconnects and resends, so asking for that would loop.
fn session_closed_error(error: &jazz::db::Error) -> jazz::wire::WireError {
    let (code, retry) = match error.code {
        jazz::db::ErrorCode::Backpressure | jazz::db::ErrorCode::Storage => {
            (WireErrorCode::Backpressure, WireRetry::Later)
        }
        _ => (WireErrorCode::Internal, WireRetry::Never),
    };
    jazz::wire::WireError::new(code, retry, format!("sync session closed: {error}"))
}

/// Take `bytes` the client read off `egress`. Crossing back under the
/// budget queues a tick: output the owner held back can move again.
fn release_egress(egress: &AtomicUsize, bytes: usize, runtime: &Weak<Inner>) {
    let before = egress.fetch_sub(bytes, Ordering::AcqRel);
    if before >= SESSION_QUEUE_BUDGET
        && before - bytes < SESSION_QUEUE_BUDGET
        && let Some(inner) = runtime.upgrade()
    {
        let _ = ServerRuntimeHandle { inner }.queue_tick();
    }
}

/// One session's ordered outbound frame batches.
pub struct ServerRuntimeFrameStream {
    receiver: tokio_mpsc::UnboundedReceiver<Outbound>,
    egress: Arc<AtomicUsize>,
    runtime: Weak<Inner>,
}

impl ServerRuntimeFrameStream {
    /// Receive the next batch of outbound frames.
    pub async fn recv(&mut self) -> Option<Result<Vec<AbiBytes>, jazz::wire::WireError>> {
        let (result, bytes) = self.receiver.recv().await?;
        release_egress(&self.egress, bytes, &self.runtime);
        Some(result)
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

    fn start(
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
        _negotiated_features: WireFeatures,
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
        let served_as = match trust {
            CommitUnitTrust::Session => identity,
            CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority => {
                AuthorSubject::SYSTEM
            }
        };
        let session = ServerSession(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(AtomicUsize::new(0));
        self.insert_edge(session, receiver, Arc::clone(&egress))?;
        let opened = self
            .run(move |owner| {
                Ok(owner.open_session(session, served_as, claims, bundle, output, egress))
            })
            .await;
        self.settle_open(session, opened)
    }

    fn insert_edge(
        &self,
        session: ServerSession,
        receiver: tokio_mpsc::UnboundedReceiver<Outbound>,
        egress: Arc<AtomicUsize>,
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
    fn settle_open(
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
    fn wire_room(
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
        let served_as = match trust {
            CommitUnitTrust::Session => identity,
            CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority => {
                AuthorSubject::SYSTEM
            }
        };
        let session = ServerSession(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(AtomicUsize::new(0));
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
    /// `split` into parts, the server's ([`THIN_INGRESS_SERVER_BUDGET`]).
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

    fn queue_tick(&self) -> Result<(), String> {
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
        self.run(|owner| owner.db.catalogue().map_err(|error| error.to_string()))
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
        let (ended, result) = self
            .run(move |owner| {
                let mut ended = 0;
                let result = (|| {
                    if change.activate.is_some() {
                        return Err(CatalogueApplyError::Rejected(
                            "a server never makes a schema current: sessions pin bundles"
                                .to_owned(),
                        ));
                    }
                    block_on(owner.db.apply_catalogue(change)).map_err(|error| {
                        if error.code == jazz::db::ErrorCode::Storage {
                            CatalogueApplyError::Failed(error.to_string())
                        } else {
                            CatalogueApplyError::Rejected(error.to_string())
                        }
                    })?;
                    if let Some(published) = supersedes {
                        ended = owner.end_pinned_sessions(
                            &|bundle| *bundle != published,
                            &superseded_error(&published),
                        );
                        if ended > 0 {
                            tracing::info!(
                                bundle = %published,
                                ended,
                                "new permissions bundle: sessions pinned to others reconnect"
                            );
                        }
                    }
                    #[cfg(test)]
                    if std::mem::take(&mut owner.fail_next_reload) {
                        // As a reload that failed partway: the runtime's
                        // view of the catalogue is no longer the store's.
                        owner.catalogue = Catalogue::default();
                        return Err(CatalogueApplyError::Failed(
                            "injected catalogue reload failure".to_owned(),
                        ));
                    }
                    owner
                        .reload_catalogue()
                        .map_err(CatalogueApplyError::Failed)?;
                    Ok(())
                })();
                Ok((ended, result))
            })
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

    fn edges(&self) -> Result<std::sync::MutexGuard<'_, HashMap<ServerSession, Edge>>, String> {
        self.inner
            .edges
            .lock()
            .map_err(|_| "server runtime session lock poisoned".to_owned())
    }

    fn send(&self, job: Job) -> Result<(), String> {
        self.inner.store.send(Work::Owner(job))
    }

    async fn run<T: Send + 'static>(
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

/// Why a session was not opened.
#[derive(Debug, Clone)]
pub enum SessionOpenError {
    /// The store refused to serve the session (see
    /// [`jazz::db::Db::check_session_pin`]), with its typed error.
    Refused(jazz::db::Error),
    /// The runtime could not open it (its thread is gone, its state is
    /// poisoned).
    Runtime(String),
}

impl std::fmt::Display for SessionOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "{error}"),
            Self::Runtime(message) => f.write_str(message),
        }
    }
}

impl SessionOpenError {
    /// Whether `self` contains `needle` (as its message shows it).
    pub fn contains(&self, needle: &str) -> bool {
        self.to_string().contains(needle)
    }
}

/// Why a catalogue change was not applied. Either way nothing of it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogueApplyError {
    /// The change does not fit this catalogue: an activation, or a
    /// schema or lens the store refuses to admit.
    Rejected(String),
    /// Storage or the runtime failed.
    Failed(String),
}

impl std::fmt::Display for CatalogueApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) | Self::Failed(message) => f.write_str(message),
        }
    }
}

impl From<CatalogueApplyError> for String {
    fn from(error: CatalogueApplyError) -> Self {
        error.to_string()
    }
}

// ---- owner thread -------------------------------------------------------------

struct Owner {
    db: Db,
    sessions: HashMap<ServerSession, OwnerSession>,
    /// Thin client connections (SPEC 8.14), which are not sync sessions.
    thin: HashMap<ServerSession, thin::ThinSession>,
    /// The tick in progress's time slice, shared with every session's queues.
    slice: Rc<TickSlice>,
    catalogue: Catalogue,
    /// The app this store serves, named in slow-query reports.
    app: Option<String>,
    /// Runs a network chunk store's I/O beside this thread.
    chunk_io: Option<ChunkIoRunner>,
    /// Tests: the next catalogue reload after a commit fails.
    #[cfg(test)]
    fail_next_reload: bool,
}

struct OwnerSession {
    subscriber: SubscriberId,
    /// The published bundle its backend pinned it to, if any.
    bundle: Option<jazz::catalogue::BundleId>,
    /// The parts of a message split across frames, until its last part.
    joiner: jazz::db::FrameJoiner,
    queues: Rc<SessionQueues>,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

struct SessionQueues {
    inbound: RefCell<VecDeque<SyncMessage>>,
    /// The ingress room each received batch holds until the engine has
    /// taken its last message: (messages pushed through that batch, room).
    held_ingress: RefCell<VecDeque<(u64, WireIngress)>>,
    /// Messages pushed onto `inbound`, and taken from it, ever.
    pushed: std::cell::Cell<u64>,
    taken: std::cell::Cell<u64>,
    /// Output the socket has not taken yet, encoded as it is queued so its
    /// size is known, with its bytes.
    outbound: RefCell<VecDeque<(Vec<AbiBytes>, usize)>>,
    /// What the session's output holds in memory (see [`Backlog`]).
    backlog: Backlog,
    /// Inbound messages the engine may still take in the tick serving this
    /// session.
    budget: std::cell::Cell<usize>,
    /// The tick in progress serves this session.
    served: std::cell::Cell<bool>,
    /// A tick of this round served this session already.
    done: std::cell::Cell<bool>,
    slice: Rc<TickSlice>,
}

/// How many sessions the tick in progress takes inbound messages from,
/// sized to [`TICK_SLICE`].
struct TickSlice {
    /// Sessions this tick may serve: at least one, so every tick makes
    /// progress.
    cap: std::cell::Cell<usize>,
    taken: std::cell::Cell<usize>,
    /// Recent cost of a tick per session it served (a moving average).
    per_session: std::cell::Cell<Option<Duration>>,
}

impl Default for TickSlice {
    fn default() -> Self {
        Self {
            cap: std::cell::Cell::new(usize::MAX),
            taken: std::cell::Cell::new(0),
            per_session: std::cell::Cell::new(None),
        }
    }
}

impl TickSlice {
    fn take(&self) -> bool {
        let open = self.taken.get() < self.cap.get();
        if open {
            self.taken.set(self.taken.get() + 1);
        }
        open
    }

    /// Learn from a tick that took `elapsed`, and size the next one.
    fn ticked(&self, elapsed: Duration) {
        let taken = self.taken.replace(0);
        if taken == 0 {
            return;
        }
        let sample = elapsed / taken as u32;
        let average = match self.per_session.get() {
            Some(average) => (average * 3 + sample) / 4,
            None => sample,
        };
        self.per_session.set(Some(average));
        let fits = TICK_SLICE.as_nanos() / average.as_nanos().max(1);
        self.cap
            .set(usize::try_from(fits).unwrap_or(usize::MAX).max(1));
    }
}

/// Before a tick: a round ends once every session with inbound messages
/// was served in it (a time slice can end a tick mid-round); until then,
/// only sessions it did not serve yet are served.
fn start_tick<'a>(sessions: impl Iterator<Item = &'a SessionQueues> + Clone) {
    let round_over = sessions
        .clone()
        .all(|queues| queues.done.get() || queues.inbound.borrow().is_empty());
    for queues in sessions {
        if round_over {
            queues.done.set(false);
        }
        queues.served.set(false);
        queues.backlog.start_tick();
    }
}

/// The engine's side of one session.
struct SessionTransport(Rc<SessionQueues>);

impl SessionQueues {
    /// Free the room of every batch whose messages the engine has all taken.
    fn release_taken_ingress(&self) {
        let taken = self.taken.get();
        let mut held = self.held_ingress.borrow_mut();
        while held.front().is_some_and(|(through, _)| *through <= taken) {
            held.pop_front();
        }
    }

    fn new(slice: Rc<TickSlice>, egress: Arc<AtomicUsize>) -> Self {
        Self {
            inbound: RefCell::default(),
            held_ingress: RefCell::default(),
            pushed: std::cell::Cell::new(0),
            taken: std::cell::Cell::new(0),
            outbound: RefCell::default(),
            backlog: Backlog::new(egress),
            budget: std::cell::Cell::new(0),
            served: std::cell::Cell::new(false),
            done: std::cell::Cell::new(false),
            slice,
        }
    }

    /// Queue `message` for the socket, encoded, counting it in the backlog.
    fn queue(&self, message: SyncMessage) -> Result<(), TransportError> {
        let (frames, bytes) = encode_outbound(message)?;
        self.backlog.queued(bytes);
        self.outbound.borrow_mut().push_back((frames, bytes));
        Ok(())
    }
}

fn encode_outbound(message: SyncMessage) -> Result<(Vec<AbiBytes>, usize), TransportError> {
    let frames = jazz::db::encode_frames(message).map_err(|error| {
        tracing::error!(%error, "encode outbound sync message");
        TransportError::Failed(format!("encode outbound sync message: {error}"))
    })?;
    let bytes = frames.iter().map(Vec::len).sum();
    Ok((frames, bytes))
}

/// Undrained output a session may hold beyond its biggest tick's: a reader
/// this far behind is closed with `Backpressure` (the client reconnects
/// later) instead of holding server memory without bound.
pub(crate) const SESSION_BACKLOG_FLOOR: usize = 64 * 1024 * 1024;

/// How much of a session's output sits in server memory: queued in the
/// engine and handed to the socket but not yet read off it.
///
/// One tick's output for a session is always taken whole, however large
/// (a first result is as big as what the client asked for). Output of a
/// later tick is refused, and the session closed, once what earlier ticks
/// left undrained exceeds twice the session's biggest tick, and at least
/// [`SESSION_BACKLOG_FLOOR`]: a reader that keeps up drains a burst before
/// the next one doubles it, and one that does not is bounded by about
/// that much.
struct Backlog {
    /// Bytes the socket took and the client has not read yet.
    egress: Arc<AtomicUsize>,
    /// Bytes queued in the engine, not yet handed to the socket.
    queued: std::cell::Cell<usize>,
    /// Undrained bytes (queued and egress) when this tick started.
    carried: std::cell::Cell<usize>,
    /// Bytes queued in this tick.
    burst: std::cell::Cell<usize>,
    /// The most bytes one earlier tick queued.
    peak: std::cell::Cell<usize>,
}

impl Backlog {
    fn new(egress: Arc<AtomicUsize>) -> Self {
        Self {
            egress,
            queued: std::cell::Cell::new(0),
            carried: std::cell::Cell::new(0),
            burst: std::cell::Cell::new(0),
            peak: std::cell::Cell::new(0),
        }
    }

    fn undrained(&self) -> usize {
        self.queued
            .get()
            .saturating_add(self.egress.load(Ordering::Acquire))
    }

    /// A tick starts: what is undrained now was left by earlier ones.
    fn start_tick(&self) {
        self.peak.set(self.peak.get().max(self.burst.replace(0)));
        self.carried.set(self.undrained());
    }

    /// Whether this tick may queue more for the session.
    fn admits(&self) -> bool {
        let allowed = SESSION_BACKLOG_FLOOR.max(self.peak.get().saturating_mul(2));
        self.carried.get() <= allowed
    }

    fn queued(&self, bytes: usize) {
        self.queued.set(self.queued.get() + bytes);
        self.burst.set(self.burst.get() + bytes);
    }

    /// `bytes` left the engine for the socket.
    fn handed_on(&self, bytes: usize) {
        self.queued.set(self.queued.get().saturating_sub(bytes));
    }
}

impl Transport for SessionTransport {
    /// `Backpressure` closes the session (the engine drops a backed-up
    /// subscriber, and the host tells the client to come back later).
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if !self.0.backlog.admits() {
            return Err(TransportError::Backpressure);
        }
        self.0.queue(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let queues = &self.0;
        if queues.inbound.borrow().is_empty() {
            return None;
        }
        if !queues.served.get() {
            if queues.done.get() || !queues.slice.take() {
                return None;
            }
            queues.served.set(true);
            queues.done.set(true);
            queues.budget.set(INBOUND_MESSAGES_PER_TICK);
        }
        let budget = queues.budget.get();
        if budget == 0 {
            return None;
        }
        let message = queues.inbound.borrow_mut().pop_front()?;
        queues.budget.set(budget - 1);
        queues.taken.set(queues.taken.get() + 1);
        queues.release_taken_ingress();
        Some(message)
    }
}

/// What the store's catalogue admitted, read from its rows (see
/// [`jazz::catalogue`]) after every change.
#[derive(Default)]
struct Catalogue {
    schemas: BTreeMap<SchemaVersionId, JazzSchema>,
    /// Every admitted lens, keyed by target schema.
    incoming: BTreeMap<SchemaVersionId, Vec<MigrationLens>>,
}

impl Catalogue {
    fn load(rows: &CatalogueRows) -> Result<Self, String> {
        let compile = |schema: &jazz::tools::public_schema::Schema| {
            JazzSchema::new(schema).map_err(|error| format!("catalogue schema: {error}"))
        };
        let mut catalogue = Self::default();
        for schema in &rows.schemas {
            if let Some(version) = schema.version {
                catalogue.schemas.insert(version, compile(&schema.schema)?);
            }
        }
        for lens in &rows.lenses {
            let (Some(source), Some(target)) =
                (rows.schema(&lens.source), rows.schema(&lens.target))
            else {
                continue;
            };
            let target_version = compile(&target.schema)?.version_id();
            if compile(&source.schema)?.version_id() == target_version
                || !catalogue.schemas.contains_key(&target_version)
            {
                continue;
            }
            let lens = jazz::tools::schema_lens::runtime::compile_lens(
                &jazz::tools::schema_lens::Lens::new(
                    lens.source,
                    lens.target,
                    lens.forward.clone(),
                ),
                &source.schema,
                &target.schema,
            )
            .map_err(|error| format!("catalogue lens: {error}"))?;
            catalogue.admit_lens(target_version, lens);
        }
        Ok(catalogue)
    }

    fn admit_lens(&mut self, target: SchemaVersionId, lens: MigrationLens) {
        let incoming = self.incoming.entry(target).or_default();
        if !incoming.iter().any(|admitted| admitted.id == lens.id) {
            incoming.push(lens);
        }
    }

    fn lenses_for(&self, schema: SchemaVersionId) -> Vec<MigrationLens> {
        self.incoming.get(&schema).cloned().unwrap_or_default()
    }
}

fn config(schema: JazzSchema) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// `schema` with each table's policies replaced by `permissions` (a table
/// without an entry denies everything).
fn with_permissions(
    schema: &JazzSchema,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Result<JazzSchema, String> {
    let mut source = schema.public_schema().clone();
    if permissions.keys().any(|name| !source.contains_key(name)) {
        return Err("schema permissions reference an unknown table".to_owned());
    }
    for (name, table) in source.iter_mut() {
        table.policies = permissions.get(name).cloned().unwrap_or_default();
    }
    JazzSchema::new(&source).map_err(|error| format!("invalid schema permissions: {error}"))
}

impl Owner {
    /// Open the runtime on `file`. A reopened store admits every version
    /// it admitted before (the engine reads them from its catalogue) and
    /// makes its current permissions revision current again. `schema` is
    /// current instead when the store has no revision yet, or (unless
    /// `any_stored_catalogue`) none whose catalogue admits `schema`.
    fn open(
        schema: JazzSchema,
        file: StoreFile,
        chunk_store: Option<ChunkStoreChoice>,
        wake: std_mpsc::Sender<Work>,
        any_stored_catalogue: bool,
        app: Option<String>,
    ) -> Result<Self, String> {
        let rows = file
            .catalogue()
            .map_err(|error| format!("read catalogue: {error}"))?;
        let stored = any_stored_catalogue
            || rows
                .schemas
                .iter()
                .any(|stored| stored.version == Some(schema.version_id()));
        let head = rows.head().filter(|_| stored);
        let compile = |hash: &jazz::tools::public_schema::SchemaHash| {
            let structural = rows
                .schema(hash)
                .ok_or("the current permissions revision's schema is missing")?;
            JazzSchema::new(&structural.schema)
                .map_err(|error| format!("catalogue schema: {error}"))
        };
        // Every activation records a revision: without one, the store's
        // genesis is current.
        let current = match (head, rows.genesis().filter(|_| any_stored_catalogue)) {
            (Some(head), _) => with_permissions(
                &compile(&head.schema)?,
                &head.permissions.clone().into_iter().collect(),
            )?,
            (None, Some(genesis)) => compile(&genesis.hash)?,
            (None, None) => schema,
        };
        let config = match &chunk_store {
            Some(choice) if choice.network => config(current)
                .with_chunk_store(Arc::clone(&choice.store))
                .with_host_chunk_io(),
            Some(choice) => config(current).with_chunk_store(Arc::clone(&choice.store)),
            None => config(current),
        };
        let chunk_io = match chunk_store {
            Some(choice) if choice.network => Some(ChunkIoRunner::start(choice.store, wake)?),
            _ => None,
        };
        let db = block_on(Db::open_file(config, file))
            .map_err(|error| format!("open server store: {error}"))?;
        db.set_authority();
        db.require_session_pins();
        db.track_closed_subscribers();
        db.set_slow_query_threshold(slow_query_threshold());
        let catalogue = Catalogue::load(&db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(Self {
            db,
            sessions: HashMap::new(),
            thin: HashMap::new(),
            slice: Rc::default(),
            catalogue,
            app,
            chunk_io,
            #[cfg(test)]
            fail_next_reload: false,
        })
    }

    fn reload_catalogue(&mut self) -> Result<(), String> {
        self.catalogue = Catalogue::load(&self.db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(())
    }

    /// Log the query shapes that ran slow since the last report, when one
    /// is due. Each one held the store's only writer for its whole run,
    /// delaying every session, so the line says how long, what SQLite did,
    /// and how to make it fast.
    /// Text merges that ran past their work budget: each write stood as
    /// sent, so a concurrent edit left the current text (history keeps it).
    fn report_merge_fallbacks(&self) {
        let (fallbacks, dropped) = self.db.take_merge_fallbacks();
        let app = self.app.as_deref().unwrap_or_default();
        for fallback in &fallbacks {
            tracing::warn!(
                app,
                table = %fallback.table,
                row = %fallback.row.0,
                column = %fallback.column,
                bytes = fallback.bytes,
                "text merge ran past its work budget in app {app}: the write stood as sent, \
                 and a concurrent edit to {}.{} left the current text (history keeps it)",
                fallback.table,
                fallback.column,
            );
        }
        if dropped > 0 {
            tracing::warn!(
                app,
                dropped,
                "{dropped} more text merges in app {app} ran past their work budget"
            );
        }
    }

    fn report_slow_queries(&self) {
        let Some((shapes, dropped)) = self.db.take_due_slow_queries() else {
            return;
        };
        let app = self.app.as_deref().unwrap_or_default();
        for shape in &shapes {
            tracing::warn!(
                app,
                table = %shape.table,
                runs = shape.runs,
                total_ms = shape.total.as_millis() as u64,
                slowest_ms = shape.slowest.as_millis() as u64,
                rows = shape.rows,
                scanned = shape.scanned,
                sorted = shape.sorted,
                plan = %shape.plan,
                sql = %shape.sql,
                hint = %shape.hint().unwrap_or_default(),
                "{}",
                shape.line(self.app.as_deref()),
            );
        }
        if dropped > 0 {
            tracing::warn!(app, dropped, "more slow query shapes than one report holds");
        }
    }

    /// Close every session (their streams end) and the store's runtime.
    fn close(mut self) {
        for (_, session) in self.sessions.drain() {
            self.db.disconnect_subscriber(session.subscriber);
        }
        self.thin.clear();
    }

    fn open_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Result<(), jazz::db::Error> {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let queues = Rc::new(SessionQueues::new(
            Rc::clone(&self.slice),
            Arc::clone(&egress),
        ));
        let subscriber = self.db.accept_pinned_subscriber(
            Box::new(SessionTransport(Rc::clone(&queues))),
            identity,
            claims,
            bundle,
        )?;
        self.sessions.insert(
            session,
            OwnerSession {
                subscriber,
                bundle,
                // An untrusted peer's split message must fit the budget
                // its frames are admitted against (SPEC 8.1).
                joiner: if identity != AuthorSubject::SYSTEM {
                    jazz::db::FrameJoiner::with_ceiling(
                        jazz::protocol_limits::MAX_UNTRUSTED_MESSAGE_BYTES,
                    )
                } else {
                    jazz::db::FrameJoiner::default()
                },
                queues,
                output,
                egress,
            },
        );
        Ok(())
    }

    /// Queue `frames`' messages for the engine. `ingress` stays held until
    /// the engine has taken them, so the budget bounds what waits for it.
    fn receive(&mut self, session: ServerSession, frames: Vec<AbiBytes>, ingress: WireIngress) {
        let Some(owner_session) = self.sessions.get_mut(&session) else {
            return;
        };
        let queues = Rc::clone(&owner_session.queues);
        let mut inbound = queues.inbound.borrow_mut();
        for frame in frames {
            match owner_session.joiner.push(&frame) {
                Ok(None) => {}
                Ok(Some(message)) => {
                    inbound.push_back(message);
                    queues.pushed.set(queues.pushed.get() + 1);
                }
                Err(error) => {
                    drop(inbound);
                    let _ = owner_session.output.send((
                        Err(jazz::wire::WireError::new(
                            WireErrorCode::MalformedFrame,
                            WireRetry::Never,
                            error.to_string(),
                        )),
                        0,
                    ));
                    self.close_session(session);
                    return;
                }
            }
        }
        drop(inbound);
        queues
            .held_ingress
            .borrow_mut()
            .push_back((queues.pushed.get(), ingress));
        queues.release_taken_ingress();
    }

    /// End every session (sync or thin) pinned to a bundle `ends` names,
    /// telling each peer `error` first. Unpinned sessions (the system's,
    /// an admin's) stay. How many ended.
    fn end_pinned_sessions(
        &mut self,
        ends: &dyn Fn(&jazz::catalogue::BundleId) -> bool,
        error: &jazz::wire::WireError,
    ) -> usize {
        let ending: Vec<ServerSession> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.bundle.as_ref().is_some_and(ends))
            .map(|(id, _)| *id)
            .collect();
        let mut ended = ending.len();
        for id in ending {
            let _ = self.sessions[&id].output.send((Err(error.clone()), 0));
            self.close_session(id);
        }
        ended += self.end_pinned_thin_sessions(ends, error);
        ended
    }

    fn close_session(&mut self, session: ServerSession) {
        if let Some(session) = self.sessions.remove(&session) {
            self.db.disconnect_subscriber(session.subscriber);
        }
        self.close_thin_session(session);
    }

    fn inbound_pending(&self) -> bool {
        self.sessions
            .values()
            .any(|session| !session.queues.inbound.borrow().is_empty())
    }

    /// One engine tick, then every session's output up to its budget. Each
    /// session's inbound messages are taken a bounded number per tick, so a
    /// large batch gets its first answers out early and other sessions are
    /// served between its parts.
    /// A network chunk store's batch came back: apply it (the tick that
    /// follows this job hands the next one over).
    /// Fetches run beside a batch came back: install them. A failure
    /// refuses the reads waiting on them, and holds fetches back (batches
    /// go on without them) for the backoff.
    fn finish_chunk_fetches(&mut self, done: jazz::chunk_store::ChunkIoDone) {
        let finished = self.db.finish_chunk_fetches(done);
        let log = match &self.chunk_io {
            Some(runner) => runner.fetched(finished.is_err()),
            None => finished.is_err(),
        };
        if let Err(error) = finished
            && log
        {
            tracing::warn!(%error, app = self.app.as_deref().unwrap_or("?"), "large-value store fetch failed; the reads waiting on it are refused, and fetches retry with backoff");
        }
    }

    fn finish_chunk_io(&mut self, done: jazz::chunk_store::ChunkIoDone) {
        // Fetches that failed in a batch hold fetches back as the fetch
        // lane's do, so the next batch does not ask again at once.
        let fetches_failed = done.fetches_failed();
        let finished = self.db.finish_chunk_io(done);
        let log = match &self.chunk_io {
            Some(runner) => runner.returned_with(finished.is_err(), fetches_failed),
            None => finished.is_err(),
        };
        if let Err(error) = finished
            && log
        {
            tracing::warn!(%error, app = self.app.as_deref().unwrap_or("?"), "large-value store I/O failed; retrying with backoff");
        }
    }

    fn tick(&mut self) {
        start_tick(self.sessions.values().map(|session| &*session.queues));
        let started = std::time::Instant::now();
        let ticked = block_on(self.db.tick());
        self.slice.ticked(started.elapsed());
        if let Err(error) = ticked {
            tracing::error!(%error, "server runtime tick failed");
        }
        self.retry_parked_thin();
        if let Some(runner) = &self.chunk_io {
            runner.pump(self);
        }
        self.report_slow_queries();
        self.report_merge_fallbacks();
        for (subscriber, error) in self.db.take_closed_subscribers() {
            let Some(id) = self
                .sessions
                .iter()
                .find_map(|(id, session)| (session.subscriber == subscriber).then_some(*id))
            else {
                continue;
            };
            tracing::warn!(%error, app = self.app.as_deref().unwrap_or("?"), "closing sync session");
            let _ = self.sessions[&id]
                .output
                .send((Err(session_closed_error(&error)), 0));
            self.close_session(id);
        }
        let mut gone = Vec::new();
        for (id, session) in &self.sessions {
            let mut outbound = session.queues.outbound.borrow_mut();
            let mut frames = Vec::new();
            let mut bytes = 0;
            while session.egress.load(Ordering::Acquire) + bytes < SESSION_QUEUE_BUDGET
                && let Some((encoded, size)) = outbound.pop_front()
            {
                bytes += size;
                frames.extend(encoded);
            }
            drop(outbound);
            session.queues.backlog.handed_on(bytes);
            if frames.is_empty() {
                continue;
            }
            session.egress.fetch_add(bytes, Ordering::AcqRel);
            if session.output.send((Ok(frames), bytes)).is_err() {
                gone.push(*id);
            }
        }
        for id in gone {
            self.close_session(id);
        }
        self.pump_thin();
    }
}

/// What a session pinned to a bundle other than `published` is told when it
/// ends: reconnect later. `NotReady` with `Later` is the close every client
/// already answers by reconnecting with backoff, so new permissions need no
/// new wire code; the message says why.
fn superseded_error(published: &jazz::catalogue::BundleId) -> jazz::wire::WireError {
    jazz::wire::WireError::new(
        WireErrorCode::NotReady,
        WireRetry::Later,
        format!(
            "permissions changed: bundle {published} was published; reconnect through the \
             app's backend"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};

    #[test]
    fn only_server_side_failures_ask_the_client_to_reconnect() {
        let closed = |code| session_closed_error(&jazz::db::Error::new(code, "closed"));
        assert!(closed(jazz::db::ErrorCode::Backpressure).asks_reconnect_later());
        assert!(closed(jazz::db::ErrorCode::Storage).asks_reconnect_later());
        for code in [
            jazz::db::ErrorCode::Protocol,
            jazz::db::ErrorCode::Schema,
            jazz::db::ErrorCode::Query,
            jazz::db::ErrorCode::WriteRejected,
        ] {
            let error = closed(code);
            assert_eq!(error.retry, WireRetry::Never, "{code:?}");
            assert!(!error.asks_reconnect_later(), "{code:?}");
        }
    }

    fn schema() -> JazzSchema {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchema::builder("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap()
    }

    const PROBE: HostTable = HostTable::new("probe");

    fn host_sender(store: &AppStore) -> HostSender {
        HostSender {
            host: store.inner.host.clone(),
            wake: store.sender().unwrap(),
        }
    }

    /// Hold the store thread in a runtime job until the returned sender
    /// sends (or drops).
    fn hold(runtime: &ServerRuntimeHandle) -> std_mpsc::Sender<()> {
        let (entered, running) = std_mpsc::channel();
        let (release, released) = std_mpsc::channel::<()>();
        runtime
            .send(Box::new(move |_| {
                entered.send(()).unwrap();
                let _ = released.recv();
            }))
            .unwrap();
        running.recv().unwrap();
        release
    }

    /// A host write arriving during a long runtime job (or tick) runs as
    /// soon as it ends, ahead of runtime work queued before it, and
    /// finishes. Reads (a login's) do not wait for it at all: they see what
    /// was committed.
    #[test]
    fn host_work_runs_ahead_of_queued_runtime_work() {
        let store = AppStore::memory().unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        let kv = store.kv(PROBE).unwrap();
        kv.write_many(&[(b"before".to_vec(), b"1".to_vec())])
            .unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = hold(&runtime);
        for _ in 0..3 {
            let order = Arc::clone(&order);
            runtime
                .send(Box::new(move |_| order.lock().unwrap().push("runtime")))
                .unwrap();
        }
        let order_host = Arc::clone(&order);
        host_sender(&store)
            .send(Box::new(move |tables: &dyn HostTables| {
                tables
                    .host_put_many(PROBE, &[(b"k".to_vec(), b"v".to_vec())])
                    .unwrap();
                order_host.lock().unwrap().push("host");
            }))
            .unwrap();
        // A blocking writer (a registration) waits for the long job, then
        // finishes.
        let writer = store.kv(PROBE).unwrap();
        let write =
            std::thread::spawn(move || writer.write_many(&[(b"w".to_vec(), b"w".to_vec())]));
        // Meanwhile reads answer at once, from committed state.
        assert_eq!(kv.get(b"before").unwrap(), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"k").unwrap(), None);
        release.send(()).unwrap();
        write.join().unwrap().unwrap();
        assert_eq!(kv.get(b"k").unwrap(), Some(b"v".to_vec()));
        assert_eq!(kv.get(b"w").unwrap(), Some(b"w".to_vec()));
        block_on(runtime.run(|_| Ok(()))).unwrap();
        assert_eq!(
            *order.lock().unwrap(),
            vec!["host", "runtime", "runtime", "runtime"]
        );
        store.close().unwrap();
    }

    /// Host work that never stops coming does not stop the runtime: each
    /// round of host work is cut at a tick slice and a tick runs. Counted in
    /// ticks between host jobs, not in wall time, so a slow machine only
    /// ticks more often.
    #[test]
    fn ticks_keep_running_under_constant_host_work() {
        const JOBS: usize = 200;
        let store = AppStore::memory().unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        // A session with more inbound messages than the window's ticks take.
        block_on(runtime.run(|owner| {
            let (output, _) = tokio_mpsc::unbounded_channel();
            owner
                .open_session(
                    ServerSession(1),
                    AuthorSubject::SYSTEM,
                    BTreeMap::new(),
                    None,
                    output,
                    Arc::default(),
                )
                .map_err(|error| error.to_string())?;
            let mut inbound = owner.sessions[&ServerSession(1)]
                .queues
                .inbound
                .borrow_mut();
            inbound.extend((0..100_000).map(|id| SyncMessage::Unsubscribe { id }));
            Ok(())
        }))
        .unwrap();
        // Queue all the host work at once, each job at least 1ms: a tick
        // slice fits at most a handful.
        let release = hold(&runtime);
        let host = host_sender(&store);
        let (seen, ticks_seen) = std_mpsc::channel();
        for _ in 0..JOBS {
            let ticks = Arc::clone(&store.inner.ticks);
            let seen = seen.clone();
            host.send(Box::new(move |_: &dyn HostTables| {
                std::thread::sleep(Duration::from_millis(1));
                let _ = seen.send(ticks.load(Ordering::Relaxed));
            }))
            .unwrap();
        }
        release.send(()).unwrap();
        let ticks = (0..JOBS)
            .map(|_| ticks_seen.recv().unwrap())
            .collect::<Vec<_>>();
        let during = ticks[JOBS - 1] - ticks[0];
        // 200ms of host work in 4ms slices: about 40 ticks.
        assert!(during >= 20, "{during} ticks during {JOBS} host jobs");
        store.close().unwrap();
    }

    /// A session's ingress budget holds back frames until the engine has
    /// taken their messages, not only until the owner queued them: what
    /// waits for the engine stays within the budget.
    #[test]
    fn received_frames_hold_their_room_until_the_engine_takes_them() {
        let store = AppStore::memory().unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        let handle_room = Arc::new(tokio::sync::Semaphore::new(SESSION_QUEUE_BUDGET));
        let available = {
            let room = Arc::clone(&handle_room);
            move || room.available_permits()
        };
        let room = Arc::clone(&handle_room);
        let held = block_on(runtime.run(move |owner| {
            let (output, _) = tokio_mpsc::unbounded_channel();
            owner
                .open_session(
                    ServerSession(1),
                    AuthorSubject::SYSTEM,
                    BTreeMap::new(),
                    None,
                    output,
                    Arc::default(),
                )
                .map_err(|error| error.to_string())?;
            let frames = |ids: std::ops::Range<u64>| {
                ids.flat_map(|id| jazz::db::encode_frames(SyncMessage::Unsubscribe { id }).unwrap())
                    .collect::<Vec<_>>()
            };
            let take = |bytes: u32| WireIngress {
                _room: Arc::clone(&room).try_acquire_many_owned(bytes).unwrap(),
            };
            // Two batches: two messages in 1000 bytes, then one in 500.
            owner.receive(ServerSession(1), frames(0..2), take(1000));
            owner.receive(ServerSession(1), frames(2..3), take(500));
            // Queued for the engine, not taken: the room stays held.
            Ok(room.available_permits())
        }))
        .unwrap();
        assert_eq!(held, SESSION_QUEUE_BUDGET - 1500, "queued, not yet taken");
        // The engine takes them on its next ticks, and the room comes back.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while available() < SESSION_QUEUE_BUDGET {
            assert!(
                std::time::Instant::now() < deadline,
                "the engine never took them"
            );
            block_on(runtime.run(|_| Ok(()))).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        let left = block_on(runtime.run(|owner| {
            Ok(owner.sessions[&ServerSession(1)]
                .queues
                .inbound
                .borrow()
                .len())
        }))
        .unwrap();
        assert_eq!(left, 0, "room comes back only once every message is taken");
        store.close().unwrap();
    }

    /// A host write that panics half done, on the runtime's connection,
    /// leaves nothing behind: the restart reopens the file, rolling back
    /// the open write, and the next writes commit.
    #[test]
    fn a_panic_mid_host_write_rolls_the_write_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = AppStore::open(&StorageConfig::data_dir(dir.path())).unwrap();
        let kv = store.kv(PROBE).unwrap();
        kv.write_many(&[(b"a".to_vec(), b"1".to_vec())]).unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        let panicked = block_on(runtime.with_db_for_test(|db| {
            db.host_put_many_then_panic_for_test(
                PROBE,
                &[
                    (b"a".to_vec(), b"overwritten".to_vec()),
                    (b"half".to_vec(), b"done".to_vec()),
                ],
            )
            .map_err(|error| error.to_string())
        }));
        assert!(panicked.is_err());
        assert_eq!(store.fault(), None, "restarted");
        let on_runtime = block_on(runtime.with_db_for_test(|db| {
            Ok((
                db.host_get(PROBE, b"a")
                    .map_err(|error| error.to_string())?,
                db.host_get(PROBE, b"half")
                    .map_err(|error| error.to_string())?,
            ))
        }))
        .unwrap();
        assert_eq!(on_runtime, (Some(b"1".to_vec()), None));
        kv.write_many(&[(b"b".to_vec(), b"2".to_vec())]).unwrap();
        assert_eq!(
            kv.prefix(b"").unwrap(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        store.close().unwrap();
        let reopened = AppStore::open(&StorageConfig::data_dir(dir.path())).unwrap();
        assert_eq!(
            reopened.kv(PROBE).unwrap().prefix(b"").unwrap(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        reopened.close().unwrap();
    }

    /// A runtime job that panics restarts the runtime on the store, as a
    /// process restart would; the host tables stay available throughout. A
    /// second panic soon after leaves the runtime stopped, the fault
    /// recorded, and the host tables still available.
    #[test]
    fn a_panicking_runtime_restarts_and_host_tables_stay_available() {
        let dir = tempfile::tempdir().unwrap();
        let store = AppStore::open(&StorageConfig::data_dir(dir.path())).unwrap();
        let kv = store.kv(PROBE).unwrap();
        kv.write_many(&[(b"a".to_vec(), b"1".to_vec())]).unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        let panicked = block_on(runtime.run::<()>(|_| panic!("injected runtime failure")));
        assert!(panicked.is_err(), "the job's caller hears it failed");
        // Restarted: runtime work and host work both go on.
        block_on(runtime.run(|owner| block_on(owner.db.tick()).map_err(|error| error.to_string())))
            .unwrap();
        kv.write_many(&[(b"b".to_vec(), b"2".to_vec())]).unwrap();
        assert_eq!(store.fault(), None);

        let _ = block_on(runtime.run::<()>(|_| panic!("injected runtime failure")));
        assert!(block_on(runtime.run(|_| Ok(()))).is_err(), "no runtime");
        let fault = store.fault().expect("a second failure is recorded");
        assert!(fault.contains("injected runtime failure"), "{fault}");
        kv.write_many(&[(b"c".to_vec(), b"3".to_vec())]).unwrap();
        assert_eq!(
            kv.prefix(b"").unwrap(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec())
            ]
        );
        // Starting a runtime again (a redeploy) clears the fault.
        let restarted = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        assert_eq!(store.fault(), None);
        block_on(restarted.run(|_| Ok(()))).unwrap();
        store.close().unwrap();
    }

    /// A runtime that fails to open leaves the store as it was, even an
    /// in-memory one: the host tables keep what they held.
    #[test]
    fn a_failed_runtime_start_keeps_the_host_tables() {
        let store = AppStore::memory().unwrap();
        let kv = store.kv(PROBE).unwrap();
        kv.write_many(&[(b"a".to_vec(), b"1".to_vec())]).unwrap();
        let error = store
            .start_runtime(schema(), NodeRole::Relay, false)
            .err()
            .expect("a relay cannot start");
        assert!(error.contains("relay"), "{error}");
        let corrupt = SchemaBuilder::new().build();
        store
            .catalogue()
            .unwrap()
            .put(jazz::catalogue::CataloguePut {
                schemas: vec![(corrupt, 1)],
                permissions: vec![(
                    1,
                    jazz::tools::public_schema::SchemaHash::from_bytes([7; 32]),
                    Default::default(),
                    1,
                )],
                ..Default::default()
            })
            .unwrap();
        let error = store
            .start_runtime(schema(), NodeRole::Core, true)
            .err()
            .expect("a revision of a missing schema fails the start");
        assert!(error.contains("schema is missing"), "{error}");
        assert_eq!(kv.get(b"a").unwrap(), Some(b"1".to_vec()));
        kv.write_many(&[(b"b".to_vec(), b"2".to_vec())]).unwrap();
        assert_eq!(kv.get(b"b").unwrap(), Some(b"2".to_vec()));
        store.close().unwrap();
    }

    /// With room for one session per tick, every session with messages is
    /// served once per round: one that keeps sending does not keep the
    /// others waiting.
    #[test]
    fn every_waiting_session_is_served_once_per_round() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions: Vec<Rc<SessionQueues>> = (0..3)
            .map(|_| {
                Rc::new(SessionQueues::new(
                    Rc::clone(&slice),
                    Arc::new(AtomicUsize::new(0)),
                ))
            })
            .collect();
        let push = |session: usize, count: u64| {
            sessions[session]
                .inbound
                .borrow_mut()
                .extend((0..count).map(|id| SyncMessage::Unsubscribe { id }));
        };
        push(1, 1);
        push(2, 20);
        let mut served = Vec::new();
        for _ in 0..4 {
            // Session 0 sends a little before every tick.
            push(0, 2);
            start_tick(sessions.iter().map(|queues| &**queues));
            // The engine asks every session, in a fixed order.
            let mut tick = Vec::new();
            for (index, queues) in sessions.iter().enumerate() {
                let mut transport = SessionTransport(Rc::clone(queues));
                let taken = std::iter::from_fn(|| transport.try_recv()).count();
                if taken > 0 {
                    tick.push((index, taken));
                }
            }
            slice.taken.set(0);
            served.push(tick);
        }
        assert_eq!(
            served,
            vec![
                vec![(0, 2)],
                vec![(1, 1)],
                // At most a tick's budget at once.
                vec![(2, INBOUND_MESSAGES_PER_TICK)],
                // A new round: session 0's messages waited only one round.
                vec![(0, 6)],
            ]
        );
    }

    /// A reader that stops draining is closed once what earlier ticks left
    /// undrained passes the backlog bound; a tick's own output is taken
    /// whole however large, and a reader that drains is admitted again.
    #[test]
    fn a_session_that_stops_draining_is_refused_past_its_backlog() {
        let egress = Arc::new(AtomicUsize::new(0));
        let queues = Rc::new(SessionQueues::new(
            Rc::new(TickSlice::default()),
            Arc::clone(&egress),
        ));
        let mut transport = SessionTransport(Rc::clone(&queues));
        let message = || SyncMessage::Unsubscribe { id: 1 };
        let tick = || start_tick(std::iter::once(&*queues));

        tick();
        transport
            .send(message())
            .expect("an idle session takes output");
        // The socket took everything and the client read none of it:
        // a tick's worth past the floor sits undrained.
        egress.store(SESSION_BACKLOG_FLOOR + 1, Ordering::Release);
        queues.backlog.handed_on(queues.backlog.queued.get());
        queues.outbound.borrow_mut().clear();
        // The tick that produced it is never refused...
        transport
            .send(message())
            .expect("a tick's own output is whole");
        // ...but the next tick's output is, as the reader fell behind.
        tick();
        assert!(matches!(
            transport.send(message()),
            Err(TransportError::Backpressure)
        ));

        // Once the client reads, the session takes output again.
        egress.store(0, Ordering::Release);
        tick();
        transport
            .send(message())
            .expect("a drained session takes output");
    }

    /// The bound grows with the session's own biggest tick: a reader that
    /// drains a large first result at a steady pace is not cut off while
    /// it is still within twice that burst.
    #[test]
    fn the_backlog_bound_follows_the_biggest_tick() {
        let egress = Arc::new(AtomicUsize::new(0));
        let backlog = Backlog::new(Arc::clone(&egress));
        backlog.start_tick();
        let burst = 3 * SESSION_BACKLOG_FLOOR;
        backlog.queued(burst);
        backlog.handed_on(burst);
        egress.store(burst, Ordering::Release);
        backlog.start_tick();
        assert!(backlog.admits(), "a burst being drained stays admitted");
        egress.store(2 * burst + 1, Ordering::Release);
        backlog.start_tick();
        assert!(
            !backlog.admits(),
            "past twice the biggest tick it is refused"
        );
    }

    #[test]
    fn ticks_serve_as_many_sessions_as_fit_in_the_slice() {
        let slice = TickSlice::default();
        // Unsized at first: the per-session budget is the only bound.
        assert!((0..100).all(|_| slice.take()));
        slice.ticked(TICK_SLICE * 25);
        assert_eq!(slice.cap.get(), 4, "a session costs a quarter slice");
        assert!((0..4).all(|_| slice.take()));
        assert!(!slice.take());
        // The moving average follows the cost down, and up again.
        for _ in 0..20 {
            slice.ticked(TICK_SLICE / 40);
            assert!(slice.take());
        }
        assert!(slice.cap.get() >= 30, "cheap sessions: {}", slice.cap.get());
        for _ in 0..20 {
            slice.taken.set(1);
            slice.ticked(TICK_SLICE * 3);
        }
        assert_eq!(slice.cap.get(), 1, "a tick always takes at least one");
    }

    /// A network blob store on the mock bucket, under `prefix`.
    fn bucket_blobs(bucket: &crate::s3::mock::MockS3, prefix: &str, owner: &str) -> Blobs {
        let store = crate::s3::S3ChunkStore::new(bucket.options(prefix), owner.to_owned()).unwrap();
        Blobs::Network(NetworkChunkStore {
            store: Arc::new(store),
            name: "mock bucket".to_owned(),
        })
    }

    fn docs_source() -> jazz::tools::public_schema::Schema {
        SchemaBuilder::new()
            .table(TableSchema::builder("docs").column("body", ColumnType::Bytea))
            .build()
    }

    fn docs() -> JazzSchema {
        JazzSchema::new(&docs_source()).unwrap()
    }

    fn read_body(db: &Db) -> Option<Vec<u8>> {
        use jazz::query::Query;
        let rows = block_on(db.all(
            &db.prepare_query(&Query::from("docs")).unwrap(),
            jazz::db::ReadOpts {
                tier: jazz::db::ReadTier::LocalOnly,
                ..Default::default()
            },
        ))
        .unwrap();
        rows.first().and_then(|row| {
            row.cells()
                .find(|(name, _)| *name == "body")
                .and_then(|(_, value)| match value {
                    Some(jazz::records::Value::Bytes(bytes)) => Some(bytes.clone()),
                    _ => None,
                })
        })
    }

    /// A store whose large values live in a bucket: settled leaves move
    /// there off the store thread (the bucket checks each one's checksum),
    /// and a reopened store reads them back from it.
    #[test]
    fn large_values_move_to_an_s3_bucket_and_read_back() {
        let bucket = crate::s3::mock::MockS3::start();
        let dir = tempfile::tempdir().unwrap();
        let storage =
            || StorageConfig::data_dir(dir.path()).with_blobs(bucket_blobs(&bucket, "app/", "me"));
        let value: Vec<u8> = (0..400 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        {
            let store = AppStore::open(&storage()).unwrap();
            let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
            let written = value.clone();
            block_on(runtime.run(move |owner| {
                block_on(owner.db.insert(
                    "docs",
                    BTreeMap::from([("body".to_owned(), jazz::records::Value::Bytes(written))]),
                    Default::default(),
                ))
                .map(|_| ())
                .map_err(|error| error.to_string())
            }))
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            // 400 KiB of bytes is at least two leaves.
            while bucket.names("app/").len() < 2 {
                assert!(Instant::now() < deadline, "leaves never reached the bucket");
                // Each job is followed by a tick, which hands the next batch over.
                block_on(runtime.run(|_| Ok(()))).unwrap();
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(
                bucket
                    .state
                    .objects
                    .lock()
                    .unwrap()
                    .keys()
                    .all(|key| key.starts_with("bucket/app/")),
                "objects live under the bucket and prefix"
            );
            store.close().unwrap();
        }
        let store = AppStore::open(&storage()).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let read = block_on(runtime.run(|owner| Ok(read_body(&owner.db)))).unwrap();
        assert_eq!(read.as_deref(), Some(value.as_slice()));
        assert!(
            bucket.state.calls.gets.load(Ordering::Relaxed) > 0,
            "read from the bucket"
        );
        store.close().unwrap();
    }

    /// A 400 KiB value written to a store over `bucket`, whose leaves the
    /// store moved to the bucket (some of them, at least) before closing.
    fn offloaded_value(bucket: &crate::s3::mock::MockS3, storage: &StorageConfig) -> Vec<u8> {
        let value: Vec<u8> = (0..400 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let store = AppStore::open(storage).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let written = value.clone();
        block_on(runtime.run(move |owner| {
            block_on(owner.db.insert(
                "docs",
                BTreeMap::from([("body".to_owned(), jazz::records::Value::Bytes(written))]),
                Default::default(),
            ))
            .map(|_| ())
            .map_err(|error| error.to_string())
        }))
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        // Marked offloaded once a batch's hand-back ran, not when its puts
        // reached the bucket.
        while block_on(runtime.run(|owner| {
            owner
                .db
                .offloaded_large_value_nodes_for_test()
                .map_err(|error| error.to_string())
        }))
        .unwrap()
            < 2
        {
            assert!(
                Instant::now() < deadline,
                "leaves never moved to the bucket"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(bucket.names("app/").len() >= 2);
        store.close().unwrap();
        value
    }

    /// A read (`all_serialized_query`) on the store thread, by a caller
    /// that fetches in place, of a value whose leaves live only in the
    /// bucket, after a restart: it fetches them and answers, instead of
    /// waiting for a tick the blocked store thread can never run.
    #[test]
    fn a_serialized_read_of_offloaded_leaves_fetches_them_after_a_restart() {
        let bucket = crate::s3::mock::MockS3::start();
        let dir = tempfile::tempdir().unwrap();
        let storage =
            StorageConfig::data_dir(dir.path()).with_blobs(bucket_blobs(&bucket, "app/", "me"));
        let value = offloaded_value(&bucket, &storage);
        let store = AppStore::open(&storage).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let query = postcard::to_allocvec(&jazz::query::Query::from("docs")).unwrap();
        let read = |runtime: &ServerRuntimeHandle| {
            let (done, answer) = std_mpsc::channel();
            let (runtime, query) = (runtime.clone(), query.clone());
            std::thread::spawn(move || {
                let read = block_on(runtime.run(move |owner| {
                    match block_on(owner.db.all_serialized_query(
                        &query,
                        jazz::db::ReadOpts::default(),
                        None,
                        None,
                        None,
                    )) {
                        Ok(jazz::db::SerializedReadResult::Rows(rows)) => Ok(rows
                            .first()
                            .and_then(|row| row.cell("body"))
                            .and_then(|value| match value {
                                jazz::records::Value::Bytes(bytes) => Some(bytes),
                                _ => None,
                            })),
                        Ok(_) => Err("expected rows".to_owned()),
                        Err(error) => Err(error.to_string()),
                    }
                }));
                let _ = done.send(read);
            });
            answer
                .recv_timeout(Duration::from_secs(20))
                .expect("the read answers instead of blocking the store thread")
                .unwrap()
        };
        assert_eq!(read(&runtime).as_deref(), Some(value.as_slice()));
        assert!(
            bucket.state.calls.gets.load(Ordering::Relaxed) > 0,
            "read from the bucket"
        );
        // The store thread is free: the next read answers too.
        assert_eq!(read(&runtime).as_deref(), Some(value.as_slice()));
        store.close().unwrap();
    }

    /// A trusted thin session on `runtime`, with the docs schema declared.
    async fn thin_docs_session(
        runtime: &ServerRuntimeHandle,
    ) -> (ServerSession, ServerRuntimeFrameStream) {
        let session = runtime
            .open_thin_session(
                AuthorSubject::SYSTEM,
                BTreeMap::new(),
                None,
                CommitUnitTrust::TrustedBackend,
            )
            .await
            .unwrap();
        let mut stream = runtime.open_wire_stream(session).unwrap();
        send_thin(
            runtime,
            session,
            0,
            jazz::thin::ThinOp::Schema {
                schema: serde_json::to_vec(&docs_source()).unwrap(),
            },
        )
        .await;
        assert_eq!(
            next_thin(&mut stream, Duration::from_secs(5)).await,
            Some((0, jazz::thin::ThinBody::Ok))
        );
        (session, stream)
    }

    async fn send_thin(
        runtime: &ServerRuntimeHandle,
        session: ServerSession,
        id: u64,
        op: jazz::thin::ThinOp,
    ) {
        let room = runtime
            .admit_thin_request(session, 1024, false)
            .await
            .unwrap();
        runtime
            .receive_thin_request(session, jazz::thin::ThinRequest { id, op }, room)
            .unwrap();
        runtime.request_wire_tick(session).unwrap();
    }

    /// The next thin answer within `wait`, or `None`.
    async fn next_thin(
        stream: &mut ServerRuntimeFrameStream,
        wait: Duration,
    ) -> Option<(u64, jazz::thin::ThinBody)> {
        let frames = tokio::time::timeout(wait, stream.recv())
            .await
            .ok()?
            .expect("open")
            .expect("frames");
        assert_eq!(frames.len(), 1, "one answer at a time");
        match jazz::wire::decode_frame(&frames[0]).unwrap() {
            jazz::wire::WireFrame::ThinResponse(response) => Some((response.id, response.body)),
            other => panic!("expected a thin response, got {other:?}"),
        }
    }

    fn thin_query(transaction: Option<&str>) -> jazz::thin::ThinOp {
        jazz::thin::ThinOp::Query {
            query: postcard::to_allocvec(&jazz::query::Query::from("docs")).unwrap(),
            options: Vec::new(),
            transaction: transaction.map(str::to_owned),
        }
    }

    /// The body a thin query's rows hold.
    fn thin_body(body: jazz::thin::ThinBody) -> Vec<u8> {
        let jazz::thin::ThinBody::Rows { rows, .. } = body else {
            panic!("expected rows, got {body:?}");
        };
        let rows = jazz::binding_codec::decode_rows(&rows).unwrap();
        match rows[0].rows[0].cells.get("body") {
            Some(jazz::records::Value::Bytes(bytes)) => bytes.clone(),
            other => panic!("expected the body, got {other:?}"),
        }
    }

    /// The store thread answers runtime work within a second.
    async fn store_thread_is_free(runtime: &ServerRuntimeHandle) {
        tokio::time::timeout(Duration::from_secs(1), runtime.run(|_| Ok(())))
            .await
            .expect("the store thread is not blocked")
            .unwrap();
    }

    /// A thin session's read of offloaded leaves after a restart, inside
    /// and outside a transaction, waits for the fetch lane instead of
    /// fetching on the store thread, and answers once they landed. While
    /// the bucket does not answer, the store thread serves everyone else,
    /// and the session's next request waits behind the parked one.
    #[test]
    fn a_thin_read_of_offloaded_leaves_waits_for_the_fetch_lane() {
        // The S3 store runs and drops a runtime of its own: only the thin
        // client's side runs on this one.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bucket = crate::s3::mock::MockS3::start();
        let dir = tempfile::tempdir().unwrap();
        let storage =
            StorageConfig::data_dir(dir.path()).with_blobs(bucket_blobs(&bucket, "app/", "me"));
        let value = offloaded_value(&bucket, &storage);
        let store = AppStore::open(&storage).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        rt.block_on(async {
            let (session, mut stream) = thin_docs_session(&runtime).await;
            bucket.state.stall_gets.store(true, Ordering::Relaxed);
            send_thin(&runtime, session, 1, thin_query(None)).await;
            send_thin(&runtime, session, 2, thin_query(None)).await;
            assert_eq!(
                next_thin(&mut stream, Duration::from_millis(500)).await,
                None,
                "nothing answers while the bucket does not"
            );
            store_thread_is_free(&runtime).await;
            bucket.state.stall_gets.store(false, Ordering::Relaxed);
            for id in [1, 2] {
                let (answered, body) = next_thin(&mut stream, Duration::from_secs(20))
                    .await
                    .expect("answered once the leaves landed");
                assert_eq!(answered, id, "answered in order");
                assert_eq!(thin_body(body), value);
            }
        });
        assert!(bucket.state.calls.gets.load(Ordering::Relaxed) > 0);
        store.close().unwrap();

        // A transaction's read, after another restart.
        let store = AppStore::open(&storage).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        rt.block_on(async {
            let (session, mut stream) = thin_docs_session(&runtime).await;
            let transaction = "0190f3e4000070008000000000000007";
            send_thin(
                &runtime,
                session,
                1,
                jazz::thin::ThinOp::Begin {
                    transaction: transaction.to_owned(),
                    kind: "serializable".to_owned(),
                },
            )
            .await;
            assert_eq!(
                next_thin(&mut stream, Duration::from_secs(5)).await,
                Some((1, jazz::thin::ThinBody::Ok))
            );
            send_thin(&runtime, session, 2, thin_query(Some(transaction))).await;
            let (answered, body) = next_thin(&mut stream, Duration::from_secs(20))
                .await
                .expect("the transaction's read answers");
            assert_eq!(answered, 2);
            assert_eq!(thin_body(body), value);
        });
        store.close().unwrap();
    }

    /// A bucket that fails a thin read's fetch answers the read with why,
    /// and later reads of those leaves are refused at once while fetches
    /// back off, without asking the bucket again.
    #[test]
    fn a_thin_read_of_leaves_the_bucket_fails_is_refused_with_backoff() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bucket = crate::s3::mock::MockS3::start();
        let dir = tempfile::tempdir().unwrap();
        let storage =
            StorageConfig::data_dir(dir.path()).with_blobs(bucket_blobs(&bucket, "app/", "me"));
        offloaded_value(&bucket, &storage);
        let store = AppStore::open(&storage).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        rt.block_on(async {
            let (session, mut stream) = thin_docs_session(&runtime).await;
            bucket.state.failing.store(true, Ordering::Relaxed);
            send_thin(&runtime, session, 1, thin_query(None)).await;
            let (answered, body) = next_thin(&mut stream, Duration::from_secs(20))
                .await
                .expect("refused, not left waiting");
            assert_eq!(answered, 1);
            assert!(
                matches!(&body, jazz::thin::ThinBody::Error { code, .. } if code == "Storage"),
                "{body:?}"
            );
            let asked = bucket.state.calls.requests.load(Ordering::Relaxed);
            send_thin(&runtime, session, 2, thin_query(None)).await;
            let (answered, body) = next_thin(&mut stream, Duration::from_secs(1))
                .await
                .expect("refused at once while fetches back off");
            assert_eq!(answered, 2);
            assert!(
                matches!(body, jazz::thin::ThinBody::Error { .. }),
                "{body:?}"
            );
            assert_eq!(
                bucket.state.calls.requests.load(Ordering::Relaxed),
                asked,
                "the bucket is not asked again before the backoff ends"
            );
            store_thread_is_free(&runtime).await;
        });
        store.close().unwrap();
    }

    /// A bucket that refuses everything is asked again with backoff, not
    /// on every tick; once it answers, the leaves move.
    #[test]
    fn a_failing_bucket_is_retried_with_backoff() {
        let bucket = crate::s3::mock::MockS3::start();
        bucket.state.failing.store(true, Ordering::Relaxed);
        let dir = tempfile::tempdir().unwrap();
        let store = AppStore::open(
            &StorageConfig::data_dir(dir.path()).with_blobs(bucket_blobs(&bucket, "app/", "me")),
        )
        .unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let value: Vec<u8> = (0..400 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        block_on(runtime.run(move |owner| {
            block_on(owner.db.insert(
                "docs",
                BTreeMap::from([("body".to_owned(), jazz::records::Value::Bytes(value))]),
                Default::default(),
            ))
            .map(|_| ())
            .map_err(|error| error.to_string())
        }))
        .unwrap();
        let tick_for = |how_long: Duration| {
            let until = Instant::now() + how_long;
            while Instant::now() < until {
                block_on(runtime.run(|_| Ok(()))).unwrap();
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        tick_for(Duration::from_millis(2_500));
        // Batches at 0 s, 1 s and 3 s at most, each failing at its claim.
        let asked = bucket.state.calls.requests.load(Ordering::Relaxed);
        assert!((1..=3).contains(&asked), "{asked} requests in 2.5 s");
        bucket.state.failing.store(false, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(20);
        while bucket.names("app/").len() < 2 {
            assert!(Instant::now() < deadline, "leaves never reached the bucket");
            tick_for(Duration::from_millis(50));
        }
        store.close().unwrap();
    }

    /// The same through a Vercel Blob store: the core writes every leaf
    /// itself, and a reopened store reads them back (each checked against
    /// its hash, since Blob keeps no checksum).
    #[test]
    fn large_values_move_to_a_vercel_blob_store_and_read_back() {
        let blob = crate::vercel_blob::mock::MockBlob::start();
        let dir = tempfile::tempdir().unwrap();
        let storage = || {
            let store = crate::vercel_blob::VercelBlobChunkStore::new(
                blob.options("jazz/app/"),
                "me".to_owned(),
            )
            .unwrap();
            StorageConfig::data_dir(dir.path()).with_blobs(Blobs::Network(NetworkChunkStore {
                store: Arc::new(store),
                name: "mock blob".to_owned(),
            }))
        };
        let value: Vec<u8> = (0..400 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        {
            let store = AppStore::open(&storage()).unwrap();
            let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
            let written = value.clone();
            block_on(runtime.run(move |owner| {
                block_on(owner.db.insert(
                    "docs",
                    BTreeMap::from([("body".to_owned(), jazz::records::Value::Bytes(written))]),
                    Default::default(),
                ))
                .map(|_| ())
                .map_err(|error| error.to_string())
            }))
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            while blob.names("jazz/app/").len() < 2 {
                assert!(Instant::now() < deadline, "leaves never reached the store");
                block_on(runtime.run(|_| Ok(()))).unwrap();
                std::thread::sleep(Duration::from_millis(20));
            }
            store.close().unwrap();
        }
        let store = AppStore::open(&storage()).unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let read = block_on(runtime.run(|owner| Ok(read_body(&owner.db)))).unwrap();
        assert_eq!(read.as_deref(), Some(value.as_slice()));
        assert!(
            blob.state.calls.gets.load(Ordering::Relaxed) > 0,
            "read from the store"
        );
        store.close().unwrap();
    }

    /// A server's sessions are governed by the bundle they pin: a catalogue
    /// change that would make a schema current is refused, and the store
    /// keeps no permissions revision.
    #[test]
    fn a_server_refuses_to_make_a_schema_current() {
        let store = AppStore::memory().unwrap();
        let runtime = store
            .start_runtime(schema(), NodeRole::Core, false)
            .unwrap();
        let refused = block_on(runtime.apply_catalogue(
            CatalogueChange {
                activate: Some((1, schema(), Vec::new())),
                ..CatalogueChange::default()
            },
            None,
        ))
        .unwrap_err();
        assert!(
            matches!(&refused, CatalogueApplyError::Rejected(message) if message.contains("never makes a schema current")),
            "{refused:?}"
        );
        let rows = block_on(runtime.catalogue()).unwrap();
        assert!(rows.head().is_none(), "no permissions revision was written");
        store.close().unwrap();
    }

    #[test]
    fn chunk_io_backoff_doubles_to_a_minute_and_resets() {
        let (work, _work) = std_mpsc::channel();
        let store: Arc<dyn jazz::chunk_store::ChunkStore> = Arc::new(
            jazz::chunk_store::FileChunkStore::new(tempfile::tempdir().unwrap().keep()).unwrap(),
        );
        let runner = ChunkIoRunner::start(store, work).unwrap();
        let wait = |runner: &ChunkIoRunner| {
            runner.backoff.not_before.get().map(|at| {
                at.saturating_duration_since(Instant::now())
                    .as_secs_f64()
                    .round() as u64
            })
        };
        let mut logged = Vec::new();
        let mut waits = Vec::new();
        for _ in 0..9 {
            logged.push(runner.returned(true));
            waits.push(wait(&runner).unwrap());
        }
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(
            logged,
            [true, false, false, false, false, false, true, true, true]
        );
        assert!(!runner.returned(false));
        assert_eq!(wait(&runner), None);
    }

    /// What the runner hands over as lanes go out, come back and fail:
    /// failed fetches hold fetches back in both lanes for their backoff,
    /// while batches (of uploads) still go; a failed batch does not hold
    /// fetches back.
    #[test]
    fn failed_fetches_back_off_in_both_lanes() {
        let (work, _work) = std_mpsc::channel();
        let store: Arc<dyn jazz::chunk_store::ChunkStore> = Arc::new(
            jazz::chunk_store::FileChunkStore::new(tempfile::tempdir().unwrap().keep()).unwrap(),
        );
        let runner = ChunkIoRunner::start(store, work).unwrap();
        let plan = |batch, fetches, fetch_lane| Plan {
            batch,
            fetches,
            fetch_lane,
        };
        assert_eq!(runner.plan(), plan(true, true, true));
        runner.out.set(true);
        runner.fetching.set(true);
        assert_eq!(runner.plan(), plan(false, true, false), "both out");

        // Fetches fail: batches go on without fetches, the lane waits.
        assert!(runner.fetched(true), "the first failure is logged");
        runner.out.set(false);
        assert_eq!(runner.plan(), plan(true, false, false));
        assert!(
            !runner.fetched(true),
            "nor the next ones, until the longest wait"
        );
        assert_eq!(runner.plan(), plan(true, false, false));

        // Once fetches succeed, both lanes fetch again.
        runner.fetch_backoff.not_before.set(None);
        assert_eq!(runner.plan(), plan(true, true, true));
        runner.fetching.set(true);
        assert!(!runner.fetched(false));
        assert_eq!(runner.fetch_backoff.failures.get(), 0);

        // A failed batch whose fetches succeeded (its puts failed) waits,
        // and fetches go on beside it.
        runner.out.set(true);
        assert!(runner.returned_with(true, Some(false)));
        assert_eq!(runner.plan(), plan(false, true, true));

        // A batch whose fetches failed holds fetches back in both lanes,
        // as failed fetches on their own lane do.
        runner.backoff.not_before.set(None);
        runner.out.set(true);
        runner.returned_with(true, Some(true));
        runner.backoff.not_before.set(None);
        assert_eq!(runner.plan(), plan(true, false, false));
        // One with no fetches says nothing of them.
        runner.fetch_backoff.not_before.set(None);
        runner.out.set(true);
        runner.returned_with(true, None);
        runner.backoff.not_before.set(None);
        assert_eq!(runner.plan(), plan(true, true, true));
    }

    /// A backoff after failures ends once a fresh workload token arrives,
    /// rather than waiting it out on a stale one; but only once until an
    /// attempt succeeds, so a store down for another reason is not asked
    /// again on every token rotation. The generation is passed in: other
    /// tests keep tokens in this process at the same time.
    #[test]
    fn a_fresh_workload_token_ends_the_chunk_io_backoff_once() {
        let backoff = Backoff::default();
        for _ in 0..7 {
            backoff.ended_at(true, 5);
        }
        assert!(backoff.waiting_at(5), "backed off");
        assert!(!backoff.waiting_at(6), "a fresh token ends the wait");
        assert_eq!(backoff.not_before.get(), None);

        // The early attempt fails too: the next token waits out the backoff.
        backoff.ended_at(true, 6);
        assert!(backoff.waiting_at(7), "once per run of failures");

        // A success ends the run; the next run may be cut short again.
        backoff.ended_at(false, 7);
        backoff.ended_at(true, 7);
        assert!(backoff.waiting_at(7));
        assert!(!backoff.waiting_at(8));
    }
}
