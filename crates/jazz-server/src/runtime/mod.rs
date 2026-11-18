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

#[cfg(feature = "native")]
pub mod auth_admission;
mod core;
#[cfg(feature = "native")]
mod host;
mod thin;

pub use core::ServerCore;
#[cfg(feature = "native")]
pub use host::*;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use web_time::Instant;

use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::db::{
    Db, DbConfig, DbIdentity, SubscriberId, SyncMessage, Transport, TransportError, block_on,
};
use jazz::host::{HostTables, StoreFile};
use jazz::ids::{AuthorSubject, NodeUuid, SchemaVersionId};
use jazz::protocol::MigrationLens;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{TableName, TablePolicies};
use jazz::wire::{WireErrorCode, WireRetry};
use tokio::sync::mpsc as tokio_mpsc;

// What only the native host uses (`host`, through `use super::*`).
#[cfg(feature = "native")]
use crate::kv::{HostKv, HostSql};
#[cfg(feature = "native")]
use jazz::db::{FileOptions, LensEffects};
#[cfg(feature = "native")]
use jazz::host::{HostReader, HostTable};
#[cfg(feature = "native")]
use jazz::ids::MigrationLensId;
#[cfg(feature = "native")]
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "native")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "native")]
use std::sync::{Weak, mpsc as std_mpsc};
#[cfg(feature = "native")]
use std::thread;
#[cfg(feature = "native")]
use tokio::sync::{oneshot, watch};

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

pub use crate::portable::admission::{CommitUnitTrust, ServerLinkAdmission};

#[doc(inline)]
pub use crate::portable::handshake::ConnectionSessionContext;

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
#[cfg(feature = "native")]
pub(crate) const IDLE_TICK: Duration = Duration::from_secs(1);

/// Inbound messages one session may have applied per round of ticks.
const INBOUND_MESSAGES_PER_TICK: usize = 16;

/// Reads one tick may take out of turn, across all sessions (see
/// [`start_tick`]). Bounded so sessions that flood reads add at most this
/// many reads to a tick, and the sessions whose turn it is (and their
/// writes) are still served in it.
const READS_OUT_OF_TURN_PER_TICK: usize = 8;

/// About how long one tick may take. Each tick serves as many sessions as
/// the recent per-session cost fits in it (at least one). A round serves
/// every session with inbound messages once, over as many ticks as that
/// takes, before any is served again: a session waits at most a round, and
/// host work queued meanwhile (catalogue writes, registrations) runs
/// between the ticks. A served session's queued messages (up to
/// [`INBOUND_MESSAGES_PER_TICK`]) are taken in one tick: clients rely on a
/// read sent just before their writes being answered after them. Smaller
/// ticks cost no throughput: each message commits on its own either way.
/// A session with nothing but reads queued does not wait for its turn: its
/// reads are taken out of turn, up to [`READS_OUT_OF_TURN_PER_TICK`] a tick.
const TICK_SLICE: Duration = Duration::from_millis(4);

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

/// A session's output in server memory: the bytes held (handed to its
/// socket side and not yet written), which it derefs to, and the raw bytes
/// its socket has written, which tell a client that is reading slowly from
/// one that stopped.
#[derive(Default)]
pub struct Egress {
    held: AtomicUsize,
    written: AtomicU64,
}

impl std::ops::Deref for Egress {
    type Target = AtomicUsize;

    fn deref(&self) -> &AtomicUsize {
        &self.held
    }
}

impl Egress {
    /// Raw bytes the session's socket has written.
    fn written(&self) -> u64 {
        self.written.load(Ordering::Acquire)
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
    /// The bounds on output held for clients (see [`EgressLimits`]).
    limits: EgressLimits,
    /// Sessions closed for falling behind, by reason.
    sheds: EgressSheds,
    /// Thin client connections (SPEC 8.14), which are not sync sessions.
    thin: HashMap<ServerSession, thin::ThinSession>,
    /// The tick in progress's time slice, shared with every session's queues.
    slice: Rc<TickSlice>,
    catalogue: Catalogue,
    /// The app this store serves, named in slow-query reports.
    app: Option<String>,
    /// Whether the account registry the checks beside the account tables
    /// read is installed.
    registry_installed: bool,
    /// Runs a network chunk store's I/O beside this thread.
    #[cfg(feature = "native")]
    chunk_io: Option<ChunkIoRunner>,
    /// Tests: the next catalogue reload after a commit fails.
    #[cfg(all(test, feature = "native"))]
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
    egress: Arc<Egress>,
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
    /// Whether its client is reading.
    clock: ReadClock,
    /// Inbound messages the engine may still take in the tick serving this
    /// session.
    budget: std::cell::Cell<usize>,
    /// The tick in progress serves this session.
    served: std::cell::Cell<bool>,
    /// A tick of this round served this session already.
    done: std::cell::Cell<bool>,
    /// Reads this tick may take from the session out of turn (see
    /// [`start_tick`]).
    early: std::cell::Cell<usize>,
    /// This tick took some of them.
    early_served: std::cell::Cell<bool>,
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
    /// Sessions this tick served out of turn: each counts as a session
    /// served in the cost average, so out-of-turn reads are charged to
    /// their own session instead of inflating the cost of the others.
    early: std::cell::Cell<usize>,
    /// Where the next tick's out-of-turn grants start among the sessions
    /// that may have them, so the first sessions do not always get them.
    cursor: std::cell::Cell<usize>,
}

impl Default for TickSlice {
    fn default() -> Self {
        Self {
            cap: std::cell::Cell::new(usize::MAX),
            taken: std::cell::Cell::new(0),
            per_session: std::cell::Cell::new(None),
            early: std::cell::Cell::new(0),
            cursor: std::cell::Cell::new(0),
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
        let taken = self.taken.replace(0) + self.early.replace(0);
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
///
/// Then grant the tick's [`READS_OUT_OF_TURN_PER_TICK`] to sessions whose
/// queues hold nothing but reads, one read at a time round-robin, starting
/// where the last tick's grants left off. A session whose turn comes in
/// this tick is served in turn and leaves its grant unused. Nothing is
/// queued during a tick, so a granted queue still holds only reads when
/// the engine asks for it.
fn start_tick<'a>(sessions: impl Iterator<Item = &'a SessionQueues> + Clone) {
    let round_over = sessions
        .clone()
        .all(|queues| queues.done.get() || queues.inbound.borrow().is_empty());
    for queues in sessions.clone() {
        if round_over {
            queues.done.set(false);
        }
        queues.served.set(false);
        queues.early.set(0);
        queues.early_served.set(false);
        queues.backlog.start_tick();
    }
    let readers: Vec<&SessionQueues> = sessions
        .filter(|queues| !queues.inbound.borrow().is_empty() && queues.only_reads_queued())
        .collect();
    let Some(slice) = readers.first().map(|queues| Rc::clone(&queues.slice)) else {
        return;
    };
    let start = slice.cursor.get() % readers.len();
    let mut left = READS_OUT_OF_TURN_PER_TICK;
    let mut granted = 0;
    while left > 0 {
        let before = left;
        for queues in readers[start..].iter().chain(&readers[..start]) {
            if left == 0 {
                break;
            }
            if queues.early.get() < queues.inbound.borrow().len() {
                if queues.early.get() == 0 {
                    granted += 1;
                }
                queues.early.set(queues.early.get() + 1);
                left -= 1;
            }
        }
        if left == before {
            break;
        }
    }
    slice.cursor.set(start.wrapping_add(granted));
}

/// The engine's side of one session.
struct SessionTransport(Rc<SessionQueues>);

impl SessionQueues {
    /// Whether every queued inbound message only reads: nothing a read
    /// taken now could overtake.
    fn only_reads_queued(&self) -> bool {
        self.inbound.borrow().iter().all(|message| {
            matches!(
                message,
                SyncMessage::Read { .. } | SyncMessage::Unsubscribe { .. }
            )
        })
    }

    fn take(&self) -> Option<SyncMessage> {
        let message = self.inbound.borrow_mut().pop_front()?;
        self.taken.set(self.taken.get() + 1);
        self.release_taken_ingress();
        Some(message)
    }

    /// Free the room of every batch whose messages the engine has all taken.
    fn release_taken_ingress(&self) {
        let taken = self.taken.get();
        let mut held = self.held_ingress.borrow_mut();
        while held.front().is_some_and(|(through, _)| *through <= taken) {
            held.pop_front();
        }
    }

    fn new(slice: Rc<TickSlice>, egress: Arc<Egress>, limits: &EgressLimits) -> Self {
        Self {
            inbound: RefCell::default(),
            held_ingress: RefCell::default(),
            pushed: std::cell::Cell::new(0),
            taken: std::cell::Cell::new(0),
            outbound: RefCell::default(),
            backlog: Backlog::new(egress, limits),
            clock: ReadClock::default(),
            budget: std::cell::Cell::new(0),
            served: std::cell::Cell::new(false),
            done: std::cell::Cell::new(false),
            early: std::cell::Cell::new(0),
            early_served: std::cell::Cell::new(false),
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
/// later) instead of holding server memory without bound. The default of
/// [`EgressLimits::session_backlog_bytes`] (`JAZZ_SESSION_BACKLOG_BYTES`).
pub(crate) const SESSION_BACKLOG_FLOOR: usize = 64 * 1024 * 1024;

/// The most undrained output one session may hold however big its ticks
/// are: the default of [`EgressLimits::session_backlog_max_bytes`]
/// (`JAZZ_SESSION_BACKLOG_MAX_BYTES`).
pub(crate) const SESSION_BACKLOG_CEILING: usize = 256 * 1024 * 1024;

/// The undrained output every session together may hold when nothing sets
/// [`EgressLimits::egress_budget_bytes`] and no memory limit is known.
pub(crate) const EGRESS_BUDGET_DEFAULT: usize = 2 * 1024 * 1024 * 1024;

/// How far behind the minimum read rate a session may fall, while its
/// output waits at the budget, before it is closed: the default of
/// [`EgressLimits::slow_reader_timeout`] (`JAZZ_SLOW_READER_TIMEOUT_SECS`).
/// Generous, so a slow mobile link that pauses is not cut off ([`ReadClock`]).
pub(crate) const SLOW_READER_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounds on the output the server holds for its clients. Every bound
/// closes the session it picks with a retryable `Backpressure` error: the
/// client reconnects with backoff and resumes from its last settled seq.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EgressLimits {
    /// Undrained output one session may hold beyond twice its biggest
    /// tick (default 64 MiB).
    pub session_backlog_bytes: usize,
    /// Undrained output one session may hold at most, however big its
    /// ticks (default 256 MiB). At least `session_backlog_bytes`.
    pub session_backlog_max_bytes: usize,
    /// Undrained output all sessions together may hold, best effort: past
    /// it, the sessions behind the minimum read rate by
    /// [`Self::stall_grace`] are closed,
    /// biggest first, until the rest fit or none is left stalled. Sessions
    /// still reading are never closed for it, so the total may exceed it by
    /// what they hold (each bounded by its own backlog bound).
    pub egress_budget_bytes: usize,
    /// How far behind [`Self::min_read_rate`] a session may fall, while its
    /// output waits at the budget, before it is closed.
    pub slow_reader_timeout: Duration,
    /// How long a session must have been behind [`Self::min_read_rate`]
    /// before the egress budget may close it (default 10 s).
    pub stall_grace: Duration,
    /// Bytes a second a client must read, on average, while its output
    /// waits, to count as reading (default 1024 bytes a second).
    pub min_read_rate: u64,
}

impl Default for EgressLimits {
    fn default() -> Self {
        Self {
            session_backlog_bytes: SESSION_BACKLOG_FLOOR,
            session_backlog_max_bytes: SESSION_BACKLOG_CEILING,
            egress_budget_bytes: EGRESS_BUDGET_DEFAULT,
            slow_reader_timeout: SLOW_READER_TIMEOUT,
            stall_grace: STALL_GRACE,
            min_read_rate: MIN_READ_RATE,
        }
    }
}

static EGRESS_LIMITS: Mutex<Option<EgressLimits>> = Mutex::new(None);

/// Set the egress limits runtimes started after this call apply (the
/// process's, from its launch options).
pub fn set_egress_limits(limits: EgressLimits) {
    *EGRESS_LIMITS.lock().unwrap_or_else(|e| e.into_inner()) = Some(limits);
}

fn egress_limits() -> EgressLimits {
    EGRESS_LIMITS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .unwrap_or_default()
}

/// The stalled sessions to close so what every session holds fits
/// `budget`, biggest first. `held` lists every session's undrained bytes
/// and whether it is stalled ([`ReadClock`]): a session that is reading is
/// never picked, however much it holds, as what it holds is shrinking (and
/// bounded by its own backlog bound); a stalled one far behind goes before
/// stalled ones that hold little.
fn sessions_to_shed(held: Vec<(ServerSession, usize, bool)>, budget: usize) -> Vec<ServerSession> {
    let mut total: usize = held.iter().map(|(_, bytes, _)| *bytes).sum();
    if total <= budget {
        return Vec::new();
    }
    let mut held: Vec<(ServerSession, usize)> = held
        .into_iter()
        .filter(|(_, _, stalled)| *stalled)
        .map(|(session, bytes, _)| (session, bytes))
        .collect();
    held.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.0.cmp(&b.0.0)));
    let mut shed = Vec::new();
    for (session, bytes) in held {
        if total <= budget {
            break;
        }
        total -= bytes;
        shed.push(session);
    }
    shed
}

/// How far a session's client is behind a minimum read rate, while its
/// output waits at the budget.
///
/// A client's progress is the bytes it took (handed to the socket side
/// minus those still held) or the raw bytes its socket wrote, whichever
/// moved more since the last check: either is the client making room. Each
/// check while the egress is at the budget, the clock runs, and progress
/// winds it back at [`EgressLimits::min_read_rate`]: a client reading at
/// least that fast stays at zero, one that stopped, or trickles a few
/// bytes a second to dodge the bound, falls behind as fast as time passes.
/// Under the budget nothing waits on the client, and the clock resets.
/// Past [`EgressLimits::slow_reader_timeout`] behind, the session is
/// closed; past [`EgressLimits::stall_grace`], the egress budget may close
/// it.
#[derive(Default)]
struct ReadClock {
    /// Bytes ever handed to the socket side.
    handed: std::cell::Cell<u64>,
    /// Bytes the client had taken at the last check (`handed - egress`).
    taken: std::cell::Cell<i128>,
    /// Raw bytes the socket had written at the last check.
    written: std::cell::Cell<u64>,
    /// The moment the client's reading has caught up to, at the minimum
    /// rate, while the egress is at the budget.
    caught_up: std::cell::Cell<Option<Instant>>,
}

impl ReadClock {
    /// `bytes` were handed to the socket side.
    fn handed(&self, bytes: usize) {
        self.handed.set(self.handed.get() + bytes as u64);
    }

    /// Check at `now`: how far the client is behind `min_rate` bytes a
    /// second while its egress is at the budget, if it is.
    fn check(&self, egress: &Egress, now: Instant, min_rate: u64) -> Option<Duration> {
        let (held, written) = (egress.load(Ordering::Acquire), egress.written());
        let taken = i128::from(self.handed.get()) - held as i128;
        let took = u64::try_from(taken - self.taken.replace(taken)).unwrap_or(0);
        let wrote = written.saturating_sub(self.written.replace(written));
        let progress = took.max(wrote);
        if held < SESSION_QUEUE_BUDGET {
            self.caught_up.set(None);
            return None;
        }
        let credit = Duration::try_from_secs_f64(progress as f64 / min_rate.max(1) as f64)
            .unwrap_or(Duration::MAX);
        let caught_up = self
            .caught_up
            .get()
            .unwrap_or(now)
            .checked_add(credit)
            .unwrap_or(now)
            .min(now);
        self.caught_up.set(Some(caught_up));
        Some(now.duration_since(caught_up))
    }
}

/// The default of [`EgressLimits::min_read_rate`]: 1 KiB/s. Slower than
/// any link a person uses; a client that reads slower than this for the
/// slow-reader timeout while its output waits is closed.
pub(crate) const MIN_READ_RATE: u64 = 1024;

/// How long a session must have been behind the minimum read rate before
/// the egress budget may close it: the default of
/// [`EgressLimits::stall_grace`] (`JAZZ_EGRESS_STALL_GRACE_SECS`). Output
/// handed over in this tick has not had a chance to be read, so a session
/// that just got a big result is not closed at once; a client reading at
/// the minimum rate never falls behind at all. Shorter reclaims memory
/// sooner in a spike, longer spares clients whose link pauses.
pub(crate) const STALL_GRACE: Duration = Duration::from_secs(10);

/// Sessions a runtime closed because their client fell behind, by reason.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EgressSheds {
    /// Its client wrote nothing for longer than the slow-reader timeout.
    pub slow_reader: u64,
    /// The server's egress budget was spent while it was stalled.
    pub egress_budget: u64,
}

/// Why a session was closed for falling behind.
#[derive(Clone, Copy, Debug)]
enum ShedReason {
    SlowReader,
    EgressBudget,
}

impl ShedReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::SlowReader => "slow_reader",
            Self::EgressBudget => "egress_budget",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::SlowReader => "it read nothing for longer than the slow-reader timeout",
            Self::EgressBudget => {
                "the server's egress budget is spent, and it held the most output of the \
                 sessions that read nothing"
            }
        }
    }
}

/// How much of a session's output sits in server memory: queued in the
/// engine, and handed to the socket side but not yet written to the socket
/// (a WebSocket connection releases it message by message as it writes,
/// so its writer's queue counts; kernel buffers do not).
///
/// One tick's output for a session is always taken whole, however large
/// (a first result is as big as what the client asked for). Output of a
/// later tick is refused, and the session closed, once what earlier ticks
/// left undrained exceeds twice the session's biggest tick, and at least
/// [`SESSION_BACKLOG_FLOOR`] but at most [`SESSION_BACKLOG_CEILING`]: a
/// reader that keeps up drains a burst before the next one doubles it,
/// and one that does not is bounded by about that much.
struct Backlog {
    /// Bytes handed to the socket side and not yet written.
    egress: Arc<Egress>,
    /// [`EgressLimits::session_backlog_bytes`] and
    /// [`EgressLimits::session_backlog_max_bytes`].
    floor: usize,
    ceiling: usize,
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
    fn new(egress: Arc<Egress>, limits: &EgressLimits) -> Self {
        Self {
            egress,
            floor: limits.session_backlog_bytes,
            ceiling: limits
                .session_backlog_max_bytes
                .max(limits.session_backlog_bytes),
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
        // Once the burst it allowed for has drained, a session is bounded
        // by the floor again: one huge result must not raise its bound for
        // the rest of its life.
        if self.carried.get() < self.floor {
            self.peak.set(0);
        }
    }

    /// Whether this tick may queue more for the session.
    fn admits(&self) -> bool {
        // Twice the biggest tick, so a burst being drained is not cut off,
        // but never past the ceiling: one huge first result must not let
        // the session hold several times it for good. A burst bigger than
        // the ceiling still drains, with a floor's worth of room for what
        // follows it.
        let peak = self.peak.get();
        let allowed = self
            .floor
            .max(peak.saturating_mul(2))
            .min(self.ceiling.max(peak.saturating_add(self.floor)));
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

    /// A session is served in its turn: once a round, as the tick's slice
    /// fits it, up to [`INBOUND_MESSAGES_PER_TICK`] messages. Out of turn,
    /// a session whose queue holds nothing but reads (and unsubscribes) has
    /// as many taken as [`start_tick`] granted it from the tick's
    /// [`READS_OUT_OF_TURN_PER_TICK`]: a point read from an idle backend
    /// does not wait for every other session's commits. A read queued behind a commit of its session, or
    /// ahead of one, waits for the session's turn as before, so it is never
    /// answered ahead of its session's earlier commits (INV-SYNC-49), and a
    /// read sent just before writes is still answered after them.
    fn try_recv(&mut self) -> Option<SyncMessage> {
        let queues = &self.0;
        if queues.inbound.borrow().is_empty() {
            return None;
        }
        if !queues.served.get() {
            if queues.done.get() || !queues.slice.take() {
                let early = queues.early.get();
                if early == 0 {
                    return None;
                }
                queues.early.set(early - 1);
                if !queues.early_served.replace(true) {
                    queues.slice.early.set(queues.slice.early.get() + 1);
                }
                return queues.take();
            }
            queues.served.set(true);
            queues.done.set(true);
            queues.budget.set(INBOUND_MESSAGES_PER_TICK);
        }
        let budget = queues.budget.get();
        if budget == 0 {
            return None;
        }
        queues.budget.set(budget - 1);
        queues.take()
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

    #[cfg(feature = "native")]
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
    /// Tests: the core holds nothing open between the pieces of work it
    /// serves, so that it also runs where storage cannot keep a
    /// transaction open across calls (Durable Objects' `transactionSync`).
    /// Thin-client transactions are the one exception while the core still
    /// runs them: their handles, and the overlay of the last one staged.
    /// A violation aborts the test process: the store loop catches panics
    /// and restarts the runtime, which a test could miss.
    #[cfg(all(test, feature = "native"))]
    fn assert_holds_nothing(&self, after: &str) {
        let held = self.db.held_for_test();
        let thin = self.open_thin_transactions();
        let allowed = held.open_batches == thin
            && !held.callback_transaction
            && (thin > 0 || (!held.overlay && !held.sqlite_transaction));
        if !allowed {
            // Straight to stderr: the test harness captures `eprintln!`,
            // and an abort loses what it captured.
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr(),
                "the core holds {held:?} after {after}, with {thin} thin-client \
                 transactions open: it must hold nothing between calls"
            );
            std::process::abort();
        }
    }
}

impl Owner {
    /// Open the runtime on `file` with the native host's chunk store (see
    /// [`Self::open_on`]): a network one gets its I/O runner, which wakes
    /// the store thread through `wake`.
    #[cfg(feature = "native")]
    #[allow(clippy::too_many_arguments)]
    fn open(
        schema: JazzSchema,
        file: StoreFile,
        chunk_store: Option<ChunkStoreChoice>,
        wake: std_mpsc::Sender<Work>,
        any_stored_catalogue: bool,
        app: Option<String>,
        registry: Option<HostReader>,
        registry_required: bool,
    ) -> Result<Self, String> {
        let chunk_io = match &chunk_store {
            Some(choice) if choice.network => {
                Some(ChunkIoRunner::start(Arc::clone(&choice.store), wake)?)
            }
            _ => None,
        };
        let mut owner =
            Self::open_on(
                schema,
                file,
                any_stored_catalogue,
                app,
                |config| match &chunk_store {
                    Some(choice) if choice.network => config
                        .with_chunk_store(Arc::clone(&choice.store))
                        .with_host_chunk_io(),
                    Some(choice) => config.with_chunk_store(Arc::clone(&choice.store)),
                    None => config,
                },
            )?;
        owner.chunk_io = chunk_io;
        // The checks beside the E2EE account tables' policies read the
        // account registry the store keeps beside the rows.
        // The registry names the app's founding namespace, parsed as device
        // accounts' ids derive from it (`local_first_founding_app`).
        let installed = match (registry, owner.app.as_deref()) {
            (Some(reader), Some(app)) => {
                let app = crate::portable::accounts::local_first_founding_app(app);
                owner.db.set_account_registry(Rc::new(
                    crate::server::account_store::RegistryFounders::new(reader, *app.uuid()),
                ));
                true
            }
            _ => false,
        };
        // Without a registry an account founded locally that later signs in
        // through an external login would count as founded by the app, so
        // an encrypted app is not served without one.
        owner.registry_installed = installed;
        if registry_required && !installed && owner.encrypts()? {
            return Err(
                "this app encrypts columns, which needs the account registry \
                 of an app's store: open the store for an app (`set_app`) with its \
                 host tables"
                    .to_owned(),
            );
        }
        Ok(owner)
    }

    /// Open the runtime on `file`, whatever hosts it. A reopened store
    /// admits every version it admitted before (the engine reads them from
    /// its catalogue) and makes its current permissions revision current
    /// again. `schema` is current instead when the store has no revision
    /// yet, or (unless `any_stored_catalogue`) none whose catalogue admits
    /// `schema`. `configure` adds the host's chunk store, if any, to the
    /// store's configuration.
    fn open_on(
        schema: JazzSchema,
        file: StoreFile,
        any_stored_catalogue: bool,
        app: Option<String>,
        configure: impl FnOnce(jazz::db::DbConfig) -> jazz::db::DbConfig,
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
        let config = configure(config(current));
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
            limits: egress_limits(),
            sheds: EgressSheds::default(),
            slice: Rc::default(),
            catalogue,
            app,
            registry_installed: false,
            #[cfg(feature = "native")]
            chunk_io: None,
            #[cfg(all(test, feature = "native"))]
            fail_next_reload: false,
        })
    }

    /// Whether the app's schema has Jazz's E2EE system tables.
    fn encrypts(&self) -> Result<bool, String> {
        self.db
            .table_identity(jazz::tools::deployment::artifact::system_tables::MEMBERS)
            .map(|identity| identity.is_some())
            .map_err(|error| format!("read the schema: {error}"))
    }

    fn reload_catalogue(&mut self) -> Result<(), String> {
        self.catalogue = Catalogue::load(&self.db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(())
    }

    /// Apply `change` to the store's catalogue in one transaction, and with
    /// `supersedes` (a bundle the change publishes new to this store) end
    /// every session pinned to another bundle (see
    /// `ServerRuntimeHandle::apply_catalogue`). How many sessions ended,
    /// which they did even when a later step failed, and the outcome.
    fn apply_catalogue(
        &mut self,
        change: CatalogueChange,
        supersedes: Option<jazz::catalogue::BundleId>,
    ) -> (usize, Result<(), CatalogueApplyError>) {
        let mut ended = 0;
        let result = (|| {
            block_on(self.db.apply_catalogue(change)).map_err(|error| {
                if error.code == jazz::db::ErrorCode::Storage {
                    CatalogueApplyError::Failed(error.to_string())
                } else {
                    CatalogueApplyError::Rejected(error.to_string())
                }
            })?;
            if let Some(published) = supersedes {
                ended = self.end_pinned_sessions(
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
            #[cfg(all(test, feature = "native"))]
            if std::mem::take(&mut self.fail_next_reload) {
                // As a reload that failed partway: the runtime's
                // view of the catalogue is no longer the store's.
                self.catalogue = Catalogue::default();
                return Err(CatalogueApplyError::Failed(
                    "injected catalogue reload failure".to_owned(),
                ));
            }
            self.reload_catalogue()
                .map_err(CatalogueApplyError::Failed)?;
            Ok(())
        })();
        (ended, result)
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
        egress: Arc<Egress>,
    ) -> Result<(), jazz::db::Error> {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let queues = Rc::new(SessionQueues::new(
            Rc::clone(&self.slice),
            Arc::clone(&egress),
            &self.limits,
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
    #[cfg(feature = "native")]
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

    #[cfg(feature = "native")]
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
        let started = Instant::now();
        let ticked = block_on(self.db.tick());
        self.slice.ticked(started.elapsed());
        if let Err(error) = ticked {
            tracing::error!(%error, "server runtime tick failed");
        }
        self.retry_parked_thin();
        #[cfg(feature = "native")]
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
            session.queues.clock.handed(bytes);
            if session.output.send((Ok(frames), bytes)).is_err() {
                gone.push(*id);
            }
        }
        for id in gone {
            self.close_session(id);
        }
        self.pump_thin();
        self.shed_egress();
    }

    /// Close, with `Backpressure`, the sessions whose client read nothing
    /// for longer than the slow-reader timeout, then, while all sessions
    /// together hold more than the server's egress budget, the stalled
    /// sessions holding the most.
    fn shed_egress(&mut self) {
        let now = Instant::now();
        let timeout = self.limits.slow_reader_timeout;
        let mut held: Vec<(ServerSession, usize, bool)> = Vec::new();
        let mut too_slow = Vec::new();
        for (id, session) in &self.sessions {
            let stalled =
                session
                    .queues
                    .clock
                    .check(&session.egress, now, self.limits.min_read_rate);
            if stalled.is_some_and(|stalled| stalled > timeout) {
                too_slow.push(*id);
            }
            let stalled_long = stalled.is_some_and(|stalled| stalled >= self.limits.stall_grace);
            held.push((*id, session.queues.backlog.undrained(), stalled_long));
        }
        for (id, thin) in &self.thin {
            let stalled = thin.stalled_for(now, self.limits.min_read_rate);
            if stalled.is_some_and(|stalled| stalled > timeout) {
                too_slow.push(*id);
            }
            let stalled_long = stalled.is_some_and(|stalled| stalled >= self.limits.stall_grace);
            held.push((*id, thin.undrained(), stalled_long));
        }
        held.retain(|(id, ..)| !too_slow.contains(id));
        for session in too_slow {
            self.shed(session, ShedReason::SlowReader);
        }
        for session in sessions_to_shed(held, self.limits.egress_budget_bytes) {
            self.shed(session, ShedReason::EgressBudget);
        }
    }

    /// Tell `session` it is closed for backpressure, and close it.
    fn shed(&mut self, session: ServerSession, reason: ShedReason) {
        match reason {
            ShedReason::SlowReader => self.sheds.slow_reader += 1,
            ShedReason::EgressBudget => self.sheds.egress_budget += 1,
        }
        let error = jazz::db::Error::new(
            jazz::db::ErrorCode::Backpressure,
            format!("the client fell behind: {}", reason.message()),
        );
        tracing::warn!(
            %error,
            reason = reason.as_str(),
            app = self.app.as_deref().unwrap_or("?"),
            "closing a session that fell behind"
        );
        let wire = session_closed_error(&error);
        if let Some(owned) = self.sessions.get(&session) {
            let _ = owned.output.send((Err(wire), 0));
        } else if let Some(thin) = self.thin.get(&session) {
            thin.fail(wire);
        }
        self.close_session(session);
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

#[cfg(all(test, feature = "native"))]
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
                    Arc::new(Egress::default()),
                    &EgressLimits::default(),
                ))
            })
            .collect();
        let push = |session: usize, count: u64| {
            sessions[session]
                .inbound
                .borrow_mut()
                .extend((0..count).map(|_| write()));
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

    /// A message the out-of-turn rule treats as a write: anything but a
    /// read or an unsubscribe.
    fn write() -> SyncMessage {
        SyncMessage::ResendWhole { rows: Vec::new() }
    }

    fn read(id: u64) -> SyncMessage {
        SyncMessage::Read {
            id,
            query: Box::new(jazz::query::Query::from("todos")),
            binding: BTreeMap::new(),
            include_deleted: false,
            tier: jazz::db::ReadTier::Remote,
            wait: None,
            view: Box::new(jazz::protocol::ReadViewSourceSpec::Current),
            since: None,
            span: jazz::db::ReadSpan::Once { at: None },
        }
    }

    /// Run one tick over `sessions` as the engine does (it asks every
    /// session, in a fixed order) and list what each gave: (session, how
    /// many messages, how many of them reads).
    fn tick_sessions(
        slice: &TickSlice,
        sessions: &[Rc<SessionQueues>],
    ) -> Vec<(usize, usize, usize)> {
        start_tick(sessions.iter().map(|queues| &**queues));
        let mut tick = Vec::new();
        for (index, queues) in sessions.iter().enumerate() {
            let mut transport = SessionTransport(Rc::clone(queues));
            let taken: Vec<SyncMessage> = std::iter::from_fn(|| transport.try_recv()).collect();
            if !taken.is_empty() {
                let reads = taken
                    .iter()
                    .filter(|message| matches!(message, SyncMessage::Read { .. }))
                    .count();
                tick.push((index, taken.len(), reads));
            }
        }
        // A tick far over the slice: the slice stays at one session.
        slice.ticked(TICK_SLICE * 100);
        assert_eq!(slice.cap.get(), 1);
        tick
    }

    fn queues_on(slice: &Rc<TickSlice>, count: usize) -> Vec<Rc<SessionQueues>> {
        (0..count)
            .map(|_| {
                Rc::new(SessionQueues::new(
                    Rc::clone(slice),
                    Arc::new(Egress::default()),
                    &EgressLimits::default(),
                ))
            })
            .collect()
    }

    /// Bob's backend sends a point read while alice and carol have commits
    /// queued and the slice fits one session a tick. Bob's read is taken in
    /// the next tick, beside whichever writer's turn it is, instead of after
    /// every writer's turn.
    ///
    /// ```text
    /// tick 1: alice (her turn)            bob idle
    /// tick 2: carol (her turn)  ◄── bob's read arrives, taken out of turn
    /// ```
    ///
    /// White-box: which tick takes a message is not observable through
    /// public clients without timing, so this drives the session queues
    /// the engine reads from, as `every_waiting_session_is_served_once_per_round`.
    #[test]
    fn an_idle_sessions_read_is_not_held_behind_other_sessions_commits() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions = queues_on(&slice, 3);
        let (alice, carol, bob) = (&sessions[0], &sessions[1], &sessions[2]);
        alice.inbound.borrow_mut().extend((0..40).map(|_| write()));
        carol.inbound.borrow_mut().extend((0..40).map(|_| write()));

        assert_eq!(
            tick_sessions(&slice, &sessions),
            vec![(0, INBOUND_MESSAGES_PER_TICK, 0)]
        );
        bob.inbound.borrow_mut().push_back(read(1));
        assert_eq!(
            tick_sessions(&slice, &sessions),
            vec![(1, INBOUND_MESSAGES_PER_TICK, 0), (2, 1, 1)],
            "carol's turn, and bob's read beside it"
        );
        // Bob kept his own turn: the round still ends once he has had it.
        assert!(!bob.done.get());
    }

    /// A read queued behind a commit of its own session, or ahead of one,
    /// waits for the session's turn: it is never answered ahead of the
    /// session's earlier commits (INV-SYNC-49), and a read sent just before
    /// writes is still answered after them.
    ///
    /// White-box for the same reason as
    /// `an_idle_sessions_read_is_not_held_behind_other_sessions_commits`.
    #[test]
    fn a_read_queued_with_its_sessions_writes_waits_for_its_turn() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions = queues_on(&slice, 3);
        sessions[0]
            .inbound
            .borrow_mut()
            .extend((0..40).map(|_| write()));
        // Alice wrote, then read; bob read, then wrote.
        sessions[1].inbound.borrow_mut().extend([write(), read(1)]);
        sessions[2].inbound.borrow_mut().extend([read(1), write()]);

        assert_eq!(
            tick_sessions(&slice, &sessions),
            vec![(0, INBOUND_MESSAGES_PER_TICK, 0)],
            "neither read is taken out of turn"
        );
        assert_eq!(tick_sessions(&slice, &sessions), vec![(1, 2, 1)]);
        assert_eq!(tick_sessions(&slice, &sessions), vec![(2, 2, 1)]);
    }

    /// Mallory floods reads. Each tick takes at most
    /// `READS_OUT_OF_TURN_PER_TICK` of them out of turn, and alice, whose
    /// commits are queued, is served every round as she would be without
    /// the flood: one tick her turn, the next mallory's.
    ///
    /// White-box for the same reason as
    /// `an_idle_sessions_read_is_not_held_behind_other_sessions_commits`.
    #[test]
    fn reads_out_of_turn_are_bounded_per_tick_and_writers_keep_their_turns() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions = queues_on(&slice, 2);
        sessions[0]
            .inbound
            .borrow_mut()
            .extend((0..48).map(|_| write()));
        sessions[1].inbound.borrow_mut().extend((0..1000).map(read));

        let turn = INBOUND_MESSAGES_PER_TICK;
        let flood = READS_OUT_OF_TURN_PER_TICK;
        let ticks: Vec<_> = (0..6).map(|_| tick_sessions(&slice, &sessions)).collect();
        assert_eq!(
            ticks,
            vec![
                vec![(0, turn, 0), (1, flood, flood)],
                vec![(1, turn, turn)],
                vec![(0, turn, 0), (1, flood, flood)],
                vec![(1, turn, turn)],
                vec![(0, turn, 0), (1, flood, flood)],
                vec![(1, turn, turn)],
            ]
        );
        assert!(
            sessions[0].inbound.borrow().is_empty(),
            "alice's writes all ran"
        );
    }

    /// Twelve backends each have reads queued while alice's commits fill
    /// the slice. Each tick grants its `READS_OUT_OF_TURN_PER_TICK` reads
    /// one per backend, starting where the last tick's grants ended, so no
    /// backend waits more than two ticks for a read out of turn, whatever
    /// its place in the order the engine asks sessions in.
    ///
    /// White-box for the same reason as
    /// `an_idle_sessions_read_is_not_held_behind_other_sessions_commits`.
    #[test]
    fn out_of_turn_reads_rotate_across_waiting_sessions() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions = queues_on(&slice, 13);
        sessions[0]
            .inbound
            .borrow_mut()
            .extend((0..1000).map(|_| write()));
        for backend in &sessions[1..] {
            backend.inbound.borrow_mut().extend((0..1000).map(read));
            // Each had its turn this round already.
            backend.done.set(true);
        }
        let early = |tick: &[(usize, usize, usize)]| -> Vec<usize> {
            tick.iter()
                .filter(|(index, _, _)| *index != 0)
                .map(|(index, taken, reads)| {
                    assert_eq!((*taken, *reads), (1, 1), "one read each");
                    *index
                })
                .collect()
        };
        let first = tick_sessions(&slice, &sessions);
        assert_eq!(first[0].0, 0, "alice's turn");
        assert_eq!(early(&first), (1..=8).collect::<Vec<_>>());
        let second = tick_sessions(&slice, &sessions);
        assert_eq!(early(&second), [1, 2, 3, 4, 9, 10, 11, 12]);
    }

    /// A tick that served alice in her turn and bob's read out of turn
    /// costs two sessions' worth: the next slice is sized as if each cost
    /// half the tick, not as if alice alone cost all of it.
    ///
    /// White-box: the slice's cost model has no public effect to observe.
    #[test]
    fn out_of_turn_reads_are_charged_to_their_own_session() {
        let slice = Rc::new(TickSlice::default());
        slice.cap.set(1);
        let sessions = queues_on(&slice, 2);
        sessions[0]
            .inbound
            .borrow_mut()
            .extend((0..40).map(|_| write()));
        sessions[1].inbound.borrow_mut().push_back(read(1));
        start_tick(sessions.iter().map(|queues| &**queues));
        for queues in &sessions {
            let mut transport = SessionTransport(Rc::clone(queues));
            while transport.try_recv().is_some() {}
        }
        assert!(
            sessions[1].inbound.borrow().is_empty(),
            "bob's read was taken"
        );
        slice.ticked(TICK_SLICE);
        assert_eq!(slice.cap.get(), 2, "two sessions in one slice");
        assert_eq!((slice.taken.get(), slice.early.get()), (0, 0));
    }

    /// A client `Db`'s end of a session the test drives by hand.
    struct Pipe {
        out: Rc<RefCell<VecDeque<SyncMessage>>>,
        inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    }

    impl Transport for Pipe {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.out.borrow_mut().push_back(message);
            Ok(())
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.inbound.borrow_mut().pop_front()
        }
    }

    /// Alice uploads two large values past the server's ingress limit: the
    /// engine applies the first and holds the second commit back. Then,
    /// while bob's queued messages fill the tick's slice, she reads `docs`.
    /// Her queue holds nothing but that read, so it is taken out of turn,
    /// and the engine still keeps it behind her held commit: it is answered
    /// only once the limit lifts, with both rows (INV-SYNC-49).
    ///
    /// ```text
    /// alice ──commit 1, commit 2(held)──► engine
    /// bob   ──…queued…──► his turn      alice ──read(docs)──► taken early
    ///                                                         waits for commit 2
    /// (limit lifted) ──► commit 2 ──► read answered: [1, 2]
    /// ```
    ///
    /// White-box: the ingress limit and the tick's slice are not reachable
    /// through a public server, so this drives a real client `Db` against
    /// the owner by hand.
    #[test]
    fn an_early_read_still_waits_for_its_sessions_held_commits() {
        const LARGE: usize = 300 * 1024;
        let store = AppStore::memory().unwrap();
        let runtime = store.start_runtime(docs(), NodeRole::Core, false).unwrap();
        let outcome = block_on(runtime.run(move |owner| {
            owner
                .db
                .set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
                    incoming_bytes_per_window: LARGE as u64,
                    window_ms: 60_000,
                    max_age_ms: 10 * 60 * 1_000,
                });
            let room = Arc::new(tokio::sync::Semaphore::new(usize::MAX >> 4));
            let (alice_out, mut alice_rx) = tokio_mpsc::unbounded_channel();
            let (bob_out, _bob_rx) = tokio_mpsc::unbounded_channel();
            let egress = Arc::new(Egress::default());
            for (session, output, egress) in [
                (ServerSession(1), alice_out, Arc::clone(&egress)),
                (ServerSession(2), bob_out, Arc::default()),
            ] {
                owner
                    .open_session(
                        session,
                        AuthorSubject::SYSTEM,
                        BTreeMap::new(),
                        None,
                        output,
                        egress,
                    )
                    .map_err(|error| error.to_string())?;
            }
            let alice = block_on(Db::open(DbConfig::new(
                docs(),
                DbIdentity {
                    node: NodeUuid::from_bytes([7; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )))
            .map_err(|error| error.to_string())?;
            let up = Rc::new(RefCell::new(VecDeque::new()));
            let down = Rc::new(RefCell::new(VecDeque::new()));
            let _ = block_on(alice.connect_upstream(Box::new(Pipe {
                out: Rc::clone(&up),
                inbound: Rc::clone(&down),
            })));
            let mut joiner = jazz::db::FrameJoiner::default();
            // One round trip: alice's tick, her messages to the owner, the
            // owner's tick, its answers back to her.
            let mut exchange = |owner: &mut Owner| {
                block_on(alice.tick()).unwrap();
                let frames: Vec<AbiBytes> = up
                    .borrow_mut()
                    .drain(..)
                    .flat_map(|message| jazz::db::encode_frames(message).unwrap())
                    .collect();
                if !frames.is_empty() {
                    let ingress = WireIngress {
                        _room: Arc::clone(&room).try_acquire_many_owned(1).unwrap(),
                    };
                    owner.receive(ServerSession(1), frames, ingress);
                }
                owner.tick();
                while let Ok((frames, bytes)) = alice_rx.try_recv() {
                    egress.fetch_sub(bytes, Ordering::AcqRel);
                    for frame in frames.unwrap() {
                        if let Some(message) = joiner.push(&frame).unwrap() {
                            down.borrow_mut().push_back(message);
                        }
                    }
                }
                block_on(alice.tick()).unwrap();
            };
            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            for n in [1u8, 2] {
                let body: Vec<u8> = (0..LARGE)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect();
                block_on(alice.insert(
                    "docs",
                    BTreeMap::from([("body".to_owned(), Value::Bytes(body))]),
                    jazz::db::InsertOptions {
                        row_id: Some(jazz::ids::RowUuid::from_bytes([n; 16])),
                        ..Default::default()
                    },
                ))
                .map_err(|error| error.to_string())?;
            }
            for _ in 0..12 {
                exchange(owner);
            }
            let on_server = |owner: &Owner| {
                owner
                    .db
                    .read(
                        &owner
                            .db
                            .prepare_query(&jazz::query::Query::from("docs"))
                            .unwrap(),
                    )
                    .unwrap()
                    .len()
            };
            let held = on_server(owner) == 1;

            // Bob's turn fills the next tick, and alice had hers this round.
            let bob = Rc::clone(&owner.sessions[&ServerSession(2)].queues);
            bob.inbound
                .borrow_mut()
                .extend((0..40).map(|_| SyncMessage::ResendWhole { rows: Vec::new() }));
            let alice_queues = Rc::clone(&owner.sessions[&ServerSession(1)].queues);
            alice_queues.done.set(true);
            owner.slice.cap.set(1);
            let query = alice
                .prepare_query(&jazz::query::Query::from("docs"))
                .map_err(|error| error.to_string())?;
            let mut stream = block_on(alice.subscribe(
                &query,
                jazz::db::ReadOpts {
                    tier: jazz::db::ReadTier::Remote,
                    ..Default::default()
                },
            ))
            .map_err(|error| error.to_string())?;
            let mut shown = std::collections::BTreeSet::new();
            let mut drain = |stream: &mut jazz::db::SubscriptionStream| {
                let mut answered = false;
                while let Some(event) = stream.try_next_event() {
                    if let jazz::db::SubscriptionEvent::Delta {
                        added,
                        removed,
                        settled,
                        ..
                    } = event
                    {
                        for gone in removed {
                            shown.remove(&gone.row_uuid);
                        }
                        shown.extend(added.iter().map(|row| row.row.row_uuid()));
                        answered |= settled;
                    }
                }
                answered
            };
            exchange(owner);
            let taken_early = alice_queues.inbound.borrow().is_empty()
                && bob.inbound.borrow().len() == 40 - INBOUND_MESSAGES_PER_TICK;
            let answered_while_held = drain(&mut stream);

            owner
                .db
                .set_large_value_staging_policy(jazz::db::LargeValueStagingPolicy {
                    incoming_bytes_per_window: u64::MAX,
                    window_ms: 60_000,
                    max_age_ms: 10 * 60 * 1_000,
                });
            let mut answered = false;
            for _ in 0..12 {
                exchange(owner);
                answered |= drain(&mut stream);
            }
            Ok((held, taken_early, answered_while_held, answered, shown))
        }))
        .unwrap();
        let (held, taken_early, answered_while_held, answered, shown) = outcome;
        assert!(held, "the ingress limit holds the second upload back");
        assert!(
            taken_early,
            "alice's read was taken out of turn, beside bob's turn"
        );
        assert!(
            !answered_while_held,
            "the read waits behind the held commit"
        );
        assert!(answered, "the read is answered once the upload lands");
        assert_eq!(
            shown.into_iter().collect::<Vec<_>>(),
            [
                jazz::ids::RowUuid::from_bytes([1; 16]),
                jazz::ids::RowUuid::from_bytes([2; 16])
            ],
            "with the writer's own held upload"
        );
        store.close().unwrap();
    }

    /// A reader that stops draining is closed once what earlier ticks left
    /// undrained passes the backlog bound; a tick's own output is taken
    /// whole however large, and a reader that drains is admitted again.
    #[test]
    fn a_session_that_stops_draining_is_refused_past_its_backlog() {
        let egress = Arc::new(Egress::default());
        let queues = Rc::new(SessionQueues::new(
            Rc::new(TickSlice::default()),
            Arc::clone(&egress),
            &EgressLimits::default(),
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
        let egress = Arc::new(Egress::default());
        let backlog = Backlog::new(Arc::clone(&egress), &EgressLimits::default());
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

    /// The bound follows the biggest tick only up to the ceiling, or the
    /// burst plus a floor's worth when that is more: after a first result
    /// of 200 MiB, a reader that stops draining is refused past 264 MiB,
    /// not at twice its first result.
    #[test]
    fn the_backlog_bound_stops_at_the_ceiling() {
        let egress = Arc::new(Egress::default());
        let backlog = Backlog::new(Arc::clone(&egress), &EgressLimits::default());
        backlog.start_tick();
        let burst = 200 * 1024 * 1024;
        backlog.queued(burst);
        backlog.handed_on(burst);
        egress.store(burst, Ordering::Release);
        backlog.start_tick();
        assert!(backlog.admits(), "the first result is being drained");
        egress.store(burst + SESSION_BACKLOG_FLOOR + 1, Ordering::Release);
        backlog.start_tick();
        assert!(
            !backlog.admits(),
            "past the bound it is refused, though under twice its biggest tick"
        );
    }

    /// A burst bigger than the ceiling still drains: the session may hold
    /// it plus a floor's worth of what follows, but no more.
    #[test]
    fn a_burst_over_the_ceiling_drains_with_a_floor_of_room() {
        let egress = Arc::new(Egress::default());
        let backlog = Backlog::new(Arc::clone(&egress), &EgressLimits::default());
        backlog.start_tick();
        let burst = 300 * 1024 * 1024;
        backlog.queued(burst);
        backlog.handed_on(burst);
        egress.store(burst + SESSION_BACKLOG_FLOOR, Ordering::Release);
        backlog.start_tick();
        assert!(backlog.admits(), "the burst and a floor's worth after it");
        egress.store(burst + SESSION_BACKLOG_FLOOR + 1, Ordering::Release);
        backlog.start_tick();
        assert!(!backlog.admits());
    }

    /// Once the burst that raised it has drained, a session's bound is the
    /// floor again: one huge result does not let it hold more for good.
    #[test]
    fn the_bound_drops_back_once_the_burst_has_drained() {
        let egress = Arc::new(Egress::default());
        let backlog = Backlog::new(Arc::clone(&egress), &EgressLimits::default());
        backlog.start_tick();
        let burst = 200 * 1024 * 1024;
        backlog.queued(burst);
        backlog.handed_on(burst);
        egress.store(burst, Ordering::Release);
        backlog.start_tick();
        egress.store(SESSION_BACKLOG_FLOOR + 1, Ordering::Release);
        backlog.start_tick();
        assert!(backlog.admits(), "still draining the burst");
        // Drained under the floor: the burst no longer counts.
        egress.store(SESSION_BACKLOG_FLOOR - 1, Ordering::Release);
        backlog.start_tick();
        egress.store(SESSION_BACKLOG_FLOOR + 1, Ordering::Release);
        backlog.start_tick();
        assert!(
            !backlog.admits(),
            "past the floor again, with no new burst, it is refused"
        );
    }

    /// Over the server's egress budget, the stalled sessions holding the
    /// most are closed first, only as many as the rest need to fit. A
    /// session that is reading is never picked, however much it holds.
    #[test]
    fn the_egress_budget_sheds_the_biggest_stalled_backlogs_first() {
        let held = vec![
            (ServerSession(1), 10, true),
            (ServerSession(2), 500, true),
            (ServerSession(3), 40, true),
            (ServerSession(4), 300, true),
            (ServerSession(5), 900, false),
        ];
        assert_eq!(sessions_to_shed(held.clone(), 2000), Vec::new());
        assert_eq!(sessions_to_shed(held.clone(), 1300), vec![ServerSession(2)]);
        assert_eq!(
            sessions_to_shed(held.clone(), 950),
            vec![ServerSession(2), ServerSession(4)]
        );
        assert_eq!(
            sessions_to_shed(held, 100),
            vec![
                ServerSession(2),
                ServerSession(4),
                ServerSession(3),
                ServerSession(1)
            ],
            "the reading session stays, even over budget"
        );
    }

    /// While its output waits at the budget, a client is behind by the time
    /// it read slower than the minimum rate: one reading faster stays at
    /// zero however much it has left, one that stops or only trickles falls
    /// behind as time passes, and raw bytes its socket wrote count as
    /// reading as much as messages taken.
    #[test]
    fn the_read_clock_measures_how_far_behind_the_minimum_rate_a_client_is() {
        let clock = ReadClock::default();
        let egress = Egress::default();
        let start = std::time::Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        let full = SESSION_QUEUE_BUDGET;
        let rate = 1024;
        let check = |held: usize, secs| {
            egress.store(held, Ordering::Release);
            clock.check(&egress, at(secs), rate)
        };
        // A slow reader taking 2 KiB a second, refilled: never behind.
        clock.handed(full);
        assert_eq!(check(full, 0), Some(Duration::ZERO));
        for secs in 1..600 {
            clock.handed(2048);
            assert_eq!(check(full, secs), Some(Duration::ZERO), "at {secs}s");
        }
        // It stops: behind by the time passed.
        assert_eq!(check(full, 900), Some(Duration::from_secs(301)));
        // A trickle of 10 bytes a second winds it back by 10 ms a second.
        for secs in 901..=1000 {
            clock.handed(10);
            check(full, secs);
        }
        let behind = check(full, 1000).unwrap();
        assert!(
            behind > Duration::from_secs(400),
            "a trickle stays behind: {behind:?}"
        );
        // Raw bytes its socket wrote, a large message still going out,
        // count as reading: 1 MiB is 1024 s at 1 KiB/s.
        egress.written.fetch_add(1024 * 1024, Ordering::AcqRel);
        assert_eq!(check(full, 1001), Some(Duration::ZERO));
        // Under the budget nothing waits on it, and the clock resets: back
        // at the budget after a long stop, it starts from zero.
        assert_eq!(check(full, 1300), Some(Duration::from_secs(299)));
        assert_eq!(check(full - 1, 1301), None);
        assert_eq!(check(full, 1302), Some(Duration::ZERO));
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
                matches!(&body, jazz::thin::ThinBody::Error { code, .. } if code == "storage"),
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
    /// An opened server checks an account root's `kind` against how the
    /// account was founded, as the account registry in its own store says,
    /// not against the session's issuer alone. The registry holds alice's
    /// account as founded by an external login; a session of a local-first
    /// identity writes as that account.
    ///
    /// ```text
    /// registry: alice's account founded by an external login (app)
    /// alice's account, from a local-first session:
    ///   device request ──► ✓
    ///   identity + root "local-first" ──► ✗ (founded by the app)
    ///   identity + root "app"         ──► ✓
    /// ```
    #[test]
    fn an_account_root_must_name_how_the_registry_says_its_account_was_founded() {
        use crate::server::account_store::StoredAccountRegistry;
        use jazz::db::{InsertOptions, WriteIdentity};
        use jazz::ids::RowUuid;
        use jazz::tools::OpenTransactionId;
        use jazz::tools::deployment::artifact::system_tables::{
            self, derive_e2ee_policies, with_e2ee_fragment,
        };
        use jazz::tools::public_schema::{EncryptedColumn, EncryptionEntry, PlainType, SweepBy};

        let app = "founders-view-test";
        let dir = tempfile::tempdir().unwrap();
        let store = AppStore::open(&StorageConfig::data_dir(dir.path())).unwrap();
        store.set_app(app);
        let mut registry = StoredAccountRegistry::open(store.sql().unwrap()).unwrap();
        let alice = registry
            .login_or_register(&jazz::account_registry::Principal {
                issuer: "https://issuer.example".into(),
                subject: "alice".into(),
            })
            .unwrap()
            .assignment
            .account;

        // Tasks encrypt their title in their project's space.
        let encrypted = BTreeMap::from([(
            "tasks".to_owned(),
            EncryptionEntry {
                space: "projectId".to_owned(),
                scope: "projects".to_owned(),
                columns: BTreeMap::from([(
                    "title".to_owned(),
                    EncryptedColumn {
                        plain: PlainType {
                            column_type: ColumnType::Text,
                            nullable: false,
                        },
                        sealed_type: None,
                        from: None,
                        to: None,
                        convert: None,
                        reverse: None,
                    },
                )]),
                equality: Vec::new(),
                backfill: Vec::new(),
                rehome: None,
                sweep: SweepBy::Updaters,
            },
        )]);
        let mut tables = with_e2ee_fragment(
            &SchemaBuilder::new()
                .table(TableSchema::builder("projects").column("ownerId", ColumnType::Uuid))
                .table(
                    TableSchema::builder("tasks")
                        .fk_column("projectId", "projects")
                        .column("title", ColumnType::Bytea),
                )
                .build(),
        );
        for (table, policies) in
            derive_e2ee_policies(&tables, &encrypted, &Default::default()).unwrap()
        {
            tables.get_mut(&table).unwrap().policies = policies;
        }
        let runtime = store
            .start_runtime(JazzSchema::new(&tables).unwrap(), NodeRole::Core, false)
            .unwrap();

        let subject = uuid::Uuid::from_bytes([0x1f; 16]).to_string();
        let session = AuthorSubject::reserved(jazz::identity::DEVICE_ACCOUNT_ISSUER, &subject)
            .unwrap()
            .with_account(alice);
        let bytes = move |text: &str| Value::Bytes(text.as_bytes().to_vec());
        let device = RowUuid(uuid::Uuid::new_v4());
        let epoch = uuid::Uuid::new_v4();
        let commit =
            move |db: &Db, writes: Vec<(&'static str, RowUuid, BTreeMap<String, Value>)>| {
                let id = OpenTransactionId::new();
                block_on(db.begin_mergeable(id)).map_err(|error| error.message)?;
                let tx = db.tx_ref(id);
                for (table, row, cells) in writes {
                    let staged = block_on(tx.insert(
                        table,
                        cells,
                        InsertOptions {
                            row_id: Some(row),
                            identity: WriteIdentity::Session(session),
                            ..Default::default()
                        },
                    ));
                    if let Err(error) = staged {
                        let _ = db.abandon_transaction_handle(id);
                        return Err(error.message);
                    }
                }
                block_on(db.commit_mergeable_handle(id))
                    .map(drop)
                    .map_err(|error| error.message)
            };
        let request = BTreeMap::from([
            ("publicKey".to_owned(), bytes("pk")),
            ("mechanism".to_owned(), Value::String("m".to_owned())),
            ("version".to_owned(), Value::I64(1)),
            ("signingPublicKey".to_owned(), bytes("spk")),
            ("signingMechanism".to_owned(), Value::String("s".to_owned())),
            ("signingVersion".to_owned(), Value::I64(1)),
            ("nonceCommitment".to_owned(), bytes("n")),
        ]);
        let identity_and_root = move |kind: &'static str| {
            vec![
                (
                    system_tables::ACCOUNT_IDENTITIES,
                    RowUuid(alice.0),
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(0)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), Value::Uuid(epoch)),
                        ("envelope".to_owned(), bytes("e")),
                        ("verification".to_owned(), bytes("v")),
                    ]),
                ),
                (
                    system_tables::ACCOUNT_ROOTS,
                    RowUuid(uuid::Uuid::new_v4()),
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(0)),
                        ("accountId".to_owned(), Value::Uuid(alice.0)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), Value::Uuid(epoch)),
                        ("kind".to_owned(), Value::String(kind.to_owned())),
                    ]),
                ),
            ]
        };

        block_on(runtime.with_db_for_test(move |db| {
            commit(db, vec![(system_tables::DEVICE_REQUESTS, device, request)])
                .map_err(|error| format!("the device request: {error}"))?;
            // Without the registry the session's issuer would decide, and
            // a local-first session's root would have to say "local-first".
            commit(db, identity_and_root("local-first"))
                .expect_err("the registry says the app founded the account");
            commit(db, identity_and_root("app"))
                .map_err(|error| format!("the root the registry names: {error}"))
        }))
        .unwrap();
        store.close().unwrap();
    }

    /// A store opened for no app has no account registry, so an encrypted
    /// app is not served from it: an account founded locally that later
    /// signs in through an external login would count as app-founded.
    #[test]
    fn an_encrypted_app_is_not_served_without_the_account_registry() {
        use jazz::tools::deployment::artifact::system_tables::with_e2ee_fragment;
        let tables = with_e2ee_fragment(
            &SchemaBuilder::new()
                .table(TableSchema::builder("projects").column("ownerId", ColumnType::Uuid))
                .build(),
        );
        let schema = || JazzSchema::new(&tables).unwrap();
        let store = AppStore::memory().unwrap();
        let Err(error) = store.start_runtime(schema(), NodeRole::Core, false) else {
            panic!("no app named, so no registry");
        };
        assert!(error.contains("account registry"), "{error}");
        store.close().unwrap();

        let store = AppStore::memory().unwrap();
        store.set_app("the-encrypted-app");
        store
            .start_runtime(schema(), NodeRole::Core, false)
            .expect("an app's store serves it");
        store.close().unwrap();
    }
}
