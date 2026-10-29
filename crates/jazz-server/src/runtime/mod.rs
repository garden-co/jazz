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

pub mod auth_admission;

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
    Db, DbConfig, DbIdentity, FileOptions, LensEffects, Reason, SubscriberId, SyncMessage,
    Transport, TransportError, WireError, block_on,
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
    },
}

impl StorageConfig {
    /// The durable store inside a server data directory.
    pub fn data_dir(path: impl Into<PathBuf>) -> Self {
        Self::SQLite {
            path: path.into().join("jazz.sqlite"),
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
/// it are refused (the peer ignored flow control); outbound messages past it
/// wait in the engine until the socket catches up.
const SESSION_QUEUE_BUDGET: usize = 8 * 1024 * 1024;
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
const IDLE_TICK: Duration = Duration::from_secs(1);

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
    awaiting_permissions: bool,
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
    File(PathBuf),
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
            StorageConfig::SQLite { path } => Self::File(path.clone()),
        }
    }

    fn open(&self) -> Result<StoreFile, String> {
        match self {
            Self::Memory(name) => StoreFile::shared_memory(name),
            Self::File(path) => {
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

    fn reader(&self) -> Result<HostReader, String> {
        match self {
            Self::Memory(name) => HostReader::shared_memory(name),
            Self::File(path) => HostReader::open(path, None),
        }
        .map_err(|error| format!("open server store reader: {error}"))
    }
}

impl AppStore {
    /// Open the store `storage` names on its own thread.
    pub fn open(storage: &StorageConfig) -> Result<Self, String> {
        let source = Source::new(storage);
        let (work, receiver) = std_mpsc::channel::<Work>();
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
    /// only one that admits `schema`. `awaiting_permissions`: until the
    /// first schema activation, session reads wait and session writes are
    /// rejected.
    pub fn start_runtime(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
    ) -> Result<ServerRuntimeHandle, String> {
        self.start(
            schema,
            role,
            any_stored_catalogue,
            awaiting_permissions,
            false,
        )
    }

    fn start(
        &self,
        schema: JazzSchema,
        role: NodeRole,
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
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
                awaiting_permissions,
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
                    runtime.any_stored_catalogue,
                    runtime.awaiting_permissions,
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
        let awaiting_permissions = self.owner().map(|owner| owner.awaiting_permissions);
        let started = self.started.take();
        self.stop();
        tracing::error!(%panic, "server runtime failed");
        let restart = match (started, awaiting_permissions) {
            (Some(mut runtime), Some(awaiting_permissions))
                if self
                    .restarted
                    .is_none_or(|restarted| restarted.elapsed() >= RESTART_INTERVAL) =>
            {
                runtime.awaiting_permissions = awaiting_permissions;
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
}

/// The socket side of one session.
struct Edge {
    /// Taken by [`ServerRuntimeHandle::open_wire_stream`].
    output: Option<tokio_mpsc::UnboundedReceiver<Outbound>>,
    ingress: Arc<AtomicUsize>,
    egress: Arc<AtomicUsize>,
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
        let before = self.egress.fetch_sub(bytes, Ordering::AcqRel);
        if before >= SESSION_QUEUE_BUDGET
            && before - bytes < SESSION_QUEUE_BUDGET
            && let Some(inner) = self.runtime.upgrade()
        {
            // Output the owner held back for this session can move again.
            let _ = ServerRuntimeHandle { inner }.queue_tick();
        }
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
        Self::start(schema, storage, role, false, false)
    }

    /// Start a core runtime for a catalogue-driven server whose permissions
    /// head is not published yet: until the first schema activation,
    /// session reads wait and session writes are rejected.
    pub fn start_awaiting_permissions(
        schema: JazzSchema,
        storage: StorageConfig,
    ) -> Result<Self, String> {
        Self::start(schema, storage, NodeRole::Core, false, true)
    }

    fn start(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
    ) -> Result<Self, String> {
        if role == NodeRole::Relay {
            return Err("relay nodes are not supported yet on SQLite".to_owned());
        }
        AppStore::open(&storage)?.start(
            schema,
            role,
            any_stored_catalogue,
            awaiting_permissions,
            true,
        )
    }

    /// Start a runtime whose schema comes from the administrative catalogue.
    /// A reopened store keeps everything earlier schemas added; the caller
    /// then re-applies its durable selection.
    pub fn start_with_catalogue_schema(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
    ) -> Result<Self, String> {
        Self::start(schema, storage, role, true, true)
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

    /// Admit an authenticated session.
    pub async fn open_with_session_context(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
        _negotiated_features: WireFeatures,
        _session_context: Option<ConnectionSessionContext>,
        link_admission: ServerLinkAdmission,
    ) -> Result<ServerSession, String> {
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
        self.edges()?.insert(
            session,
            Edge {
                output: Some(receiver),
                ingress: Arc::new(AtomicUsize::new(0)),
                egress: Arc::clone(&egress),
            },
        );
        let opened = self
            .run(move |owner| {
                owner.open_session(session, served_as, claims, output, egress);
                Ok(())
            })
            .await;
        if opened.is_err() {
            self.edges()?.remove(&session);
        }
        opened.map(|()| session)
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

    /// Queue inbound frames (each one `postcard(SyncMessage)`) for the
    /// engine. Refused when the session already has its budget queued.
    pub fn receive_wire_frames(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<(), String> {
        if frames.is_empty() {
            return Ok(());
        }
        let bytes = frames.iter().map(Vec::len).sum::<usize>();
        let ingress = self
            .edges()?
            .get(&session)
            .map(|edge| Arc::clone(&edge.ingress))
            .ok_or_else(|| "unknown server session".to_owned())?;
        if ingress.fetch_add(bytes, Ordering::AcqRel) + bytes > SESSION_QUEUE_BUDGET {
            ingress.fetch_sub(bytes, Ordering::AcqRel);
            return Err("session ingress budget exceeded".to_owned());
        }
        self.send(Box::new(move |owner| {
            owner.receive(session, frames);
            ingress.fetch_sub(bytes, Ordering::AcqRel);
        }))
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

    /// Make `schema` current with `permissions` at catalogue `revision`.
    pub async fn activate_schema(
        &self,
        revision: u64,
        schema: JazzSchema,
        permissions: HashMap<TableName, TablePolicies>,
    ) -> Result<SchemaVersionId, String> {
        let result = self
            .run(move |owner| owner.activate(revision, schema, &permissions))
            .await;
        if result.is_ok() {
            self.notify_activity();
        }
        result
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
    pub async fn apply_catalogue(
        &self,
        change: CatalogueChange,
    ) -> Result<(), CatalogueApplyError> {
        let result = self
            .run(move |owner| {
                Ok((|| {
                    if let Some((revision, ..)) = &change.activate {
                        owner
                            .check_revision(*revision)
                            .map_err(CatalogueApplyError::Rejected)?;
                    }
                    block_on(owner.db.apply_catalogue(change)).map_err(|error| {
                        if error.code == jazz::db::ErrorCode::Storage {
                            CatalogueApplyError::Failed(error.to_string())
                        } else {
                            CatalogueApplyError::Rejected(error.to_string())
                        }
                    })?;
                    owner
                        .reload_catalogue()
                        .map_err(CatalogueApplyError::Failed)?;
                    owner.release_held_reads();
                    Ok(())
                })())
            })
            .await
            .unwrap_or_else(|error| Err(CatalogueApplyError::Failed(error)));
        if result.is_ok() {
            self.notify_activity();
        }
        result
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

/// Why a catalogue change was not applied. Either way nothing of it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogueApplyError {
    /// The change does not fit this catalogue: a stale revision, or a
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
    /// The tick in progress's time slice, shared with every session's queues.
    slice: Rc<TickSlice>,
    catalogue: Catalogue,
    /// No permissions head is published yet: session reads wait in
    /// `OwnerSession::held` and session commits are rejected.
    awaiting_permissions: bool,
    /// The app this store serves, named in slow-query reports.
    app: Option<String>,
}

struct OwnerSession {
    subscriber: SubscriberId,
    /// A session under its own identity (not a trusted backend or
    /// authority link).
    untrusted: bool,
    /// Reads held until the permissions head is published, in order.
    held: VecDeque<SyncMessage>,
    /// The parts of a message split across frames, until its last part.
    joiner: jazz::db::FrameJoiner,
    queues: Rc<SessionQueues>,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

struct SessionQueues {
    inbound: RefCell<VecDeque<SyncMessage>>,
    outbound: RefCell<VecDeque<SyncMessage>>,
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
    }
}

/// The engine's side of one session.
struct SessionTransport(Rc<SessionQueues>);

impl Transport for SessionTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.0.outbound.borrow_mut().push_back(message);
        Ok(())
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
    /// The current permissions revision (0 before the first).
    revision: u64,
}

impl Catalogue {
    fn load(rows: &CatalogueRows) -> Result<Self, String> {
        let compile = |schema: &jazz::tools::public_schema::Schema| {
            JazzSchema::new(schema).map_err(|error| format!("catalogue schema: {error}"))
        };
        let mut catalogue = Self {
            revision: rows.head().map_or(0, |head| head.revision),
            ..Self::default()
        };
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
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
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
        let db = block_on(Db::open_file(config(current), file))
            .map_err(|error| format!("open server store: {error}"))?;
        db.set_authority();
        db.set_slow_query_threshold(slow_query_threshold());
        let catalogue = Catalogue::load(&db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(Self {
            db,
            sessions: HashMap::new(),
            slice: Rc::default(),
            catalogue,
            awaiting_permissions: awaiting_permissions && head.is_none(),
            app,
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
    }

    fn open_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let queues = Rc::new(SessionQueues {
            inbound: RefCell::default(),
            outbound: RefCell::default(),
            budget: std::cell::Cell::new(0),
            served: std::cell::Cell::new(false),
            done: std::cell::Cell::new(false),
            slice: Rc::clone(&self.slice),
        });
        let subscriber = self.db.accept_subscriber_with_claims(
            Box::new(SessionTransport(Rc::clone(&queues))),
            identity,
            claims,
        );
        self.sessions.insert(
            session,
            OwnerSession {
                subscriber,
                untrusted: identity != AuthorSubject::SYSTEM,
                held: VecDeque::new(),
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
    }

    fn receive(&mut self, session: ServerSession, frames: Vec<AbiBytes>) {
        let Some(owner_session) = self.sessions.get(&session) else {
            return;
        };
        let gated = self.awaiting_permissions && owner_session.untrusted;
        let Some(owner_session) = self.sessions.get_mut(&session) else {
            return;
        };
        let mut inbound = owner_session.queues.inbound.borrow_mut();
        for frame in frames {
            match owner_session.joiner.push(&frame) {
                Ok(None) => {}
                Ok(Some(SyncMessage::Commit { tx, .. })) if gated => {
                    owner_session
                        .queues
                        .outbound
                        .borrow_mut()
                        .push_back(SyncMessage::Fate {
                            tx,
                            through: self.db.seq_high_water(),
                            result: Err(WireError::Rejected {
                                reason: Some(Reason::PermissionsHeadMissing.as_str().to_owned()),
                                message: "permissions_head_missing: no published permissions head"
                                    .to_owned(),
                            }),
                            held: Vec::new(),
                            global: true,
                        });
                }
                Ok(Some(
                    message @ (SyncMessage::Read { .. }
                    | SyncMessage::Unsubscribe { .. }
                    | SyncMessage::ResendWhole { .. }),
                )) if gated => owner_session.held.push_back(message),
                Ok(Some(message)) => inbound.push_back(message),
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
    }

    /// The permissions head is published: reads held meanwhile are served,
    /// ahead of anything that arrived after them.
    fn release_held_reads(&mut self) {
        self.awaiting_permissions = false;
        for session in self.sessions.values_mut() {
            let mut inbound = session.queues.inbound.borrow_mut();
            for message in std::mem::take(&mut session.held).into_iter().rev() {
                inbound.push_front(message);
            }
        }
    }

    fn close_session(&mut self, session: ServerSession) {
        if let Some(session) = self.sessions.remove(&session) {
            self.db.disconnect_subscriber(session.subscriber);
        }
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
    fn tick(&mut self) {
        start_tick(self.sessions.values().map(|session| &*session.queues));
        let started = std::time::Instant::now();
        let ticked = block_on(self.db.tick());
        self.slice.ticked(started.elapsed());
        if let Err(error) = ticked {
            tracing::error!(%error, "server runtime tick failed");
        }
        self.report_slow_queries();
        for (subscriber, error) in self.db.take_closed_subscribers() {
            let Some(id) = self
                .sessions
                .iter()
                .find_map(|(id, session)| (session.subscriber == subscriber).then_some(*id))
            else {
                continue;
            };
            tracing::warn!(%error, "closing sync session");
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
            let mut failed = None;
            while session.egress.load(Ordering::Acquire) + bytes < SESSION_QUEUE_BUDGET
                && let Some(message) = outbound.pop_front()
            {
                match jazz::db::encode_frames(message) {
                    Ok(encoded) => {
                        bytes += encoded.iter().map(Vec::len).sum::<usize>();
                        frames.extend(encoded);
                    }
                    // Dropped, the message would leave the client waiting
                    // on it: the session ends with why instead.
                    Err(error) => {
                        tracing::error!(%error, "encode outbound sync message");
                        failed = Some(error.to_string());
                        break;
                    }
                }
            }
            if let Some(error) = failed {
                drop(outbound);
                let _ = session.output.send((
                    Err(jazz::wire::WireError::new(
                        WireErrorCode::Internal,
                        WireRetry::Never,
                        format!("sync session closed: {error}"),
                    )),
                    0,
                ));
                gone.push(*id);
                continue;
            }
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
    }

    /// An activation at a revision older than the current one is stale: the
    /// runtime is ahead of whoever proposed it.
    fn check_revision(&self, revision: u64) -> Result<(), String> {
        let current = self.catalogue.revision;
        if revision < current {
            return Err(format!(
                "stale active schema revision {revision}: the runtime is at revision {current}"
            ));
        }
        Ok(())
    }

    fn activate(
        &mut self,
        revision: u64,
        schema: JazzSchema,
        permissions: &HashMap<TableName, TablePolicies>,
    ) -> Result<SchemaVersionId, String> {
        self.check_revision(revision)?;
        let id = schema.version_id();
        let lenses = self.catalogue.lenses_for(id);
        let selected = with_permissions(&schema, permissions)?;
        block_on(self.db.apply_catalogue(CatalogueChange {
            activate: Some((revision, selected, lenses)),
            ..CatalogueChange::default()
        }))
        .map_err(|error| error.to_string())?;
        self.reload_catalogue()?;
        self.release_held_reads();
        Ok(id)
    }
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
            .start_runtime(schema(), NodeRole::Core, false, false)
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
            .start_runtime(schema(), NodeRole::Core, false, false)
            .unwrap();
        // A session with more inbound messages than the window's ticks take.
        block_on(runtime.run(|owner| {
            let (output, _) = tokio_mpsc::unbounded_channel();
            owner.open_session(
                ServerSession(1),
                AuthorSubject::SYSTEM,
                BTreeMap::new(),
                output,
                Arc::default(),
            );
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
            .start_runtime(schema(), NodeRole::Core, false, false)
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
            .start_runtime(schema(), NodeRole::Core, false, false)
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
            .start_runtime(schema(), NodeRole::Core, false, false)
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
            .start_runtime(schema(), NodeRole::Relay, false, false)
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
            .start_runtime(schema(), NodeRole::Core, true, false)
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
                Rc::new(SessionQueues {
                    inbound: RefCell::default(),
                    outbound: RefCell::default(),
                    budget: std::cell::Cell::new(0),
                    served: std::cell::Cell::new(false),
                    done: std::cell::Cell::new(false),
                    slice: Rc::clone(&slice),
                })
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
}
