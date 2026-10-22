//! Client↔server sync over a message transport.
//!
//! Model: the server (the authority) is a `Db` holding complete history; a
//! client is a `Db` whose store is a partial replica of server versions
//! plus its own optimistic writes.
//!
//! * Replica rows carry the server's commit seq in `_seq`, so history and
//!   as-of cuts on the client are over server versions. Optimistic local
//!   writes take seqs from `LOCAL_BASE` up, above any server seq, and are
//!   therefore invisible to every cut at a server seq.
//! * A row touched by a pending (unsettled) local transaction keeps its
//!   settled image in the shadow. Server images for it update the shadow;
//!   a rebase restores every shadowed row and replays the pending
//!   transactions in order. Settling or rejecting a transaction is a rebase
//!   without it. Linear history: the authority orders commits by arrival.
//! * The server keeps one fan-out subscription per client query (under the
//!   client's identity, so policies apply there) and sends row images for
//!   rows entering or changing in it. A row leaving it is sent again if the
//!   client may still read it, and evicted otherwise. Answers carry the
//!   server seq they are complete through, and windows carry their row ids
//!   so the client can drop stale cached rows.
//! * Serializable transactions: reads run locally at the snapshot cut
//!   (hydrated from the authority at that cut when online); the commit
//!   ships the read set with server versions, and the authority re-runs
//!   each read under the client's identity and applies the writes only if
//!   every result and every written row is unchanged since the snapshot.
//!   A stale or partial replica can only cause a conflict, never a
//!   non-serializable commit.

use crate::schema::history::{history_source_table, is_history_table};
use crate::values::query_error;
use rusqlite::OptionalExtension;
use std::cell::Cell as StdCell;
use std::cell::RefCell;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::Poll;
use web_time::Instant;

use crate::protocol::SnapshotRef;
use crate::tx::TxId;
use jazz_protocol::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz_types::large_values::chunks::ChunkRequest;
use jazz_types::time::{GlobalTime, TxTime};
use serde::{Deserialize, Serialize};

use super::*;
pub use crate::options::{FirstLoad, RemoteLinkHint};
pub use crate::wire::TransportError;

/// Wall-clock Unix ms.
fn now_ms() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Optimistic local seqs start here; every server seq is below it. Below
/// 2^53, so seqs stay exact where SQLite integers surface as JS numbers
/// (Durable Objects).
pub const LOCAL_BASE: u64 = 1 << 52;

/// A message transport between a client and its server.
pub trait Transport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError>;
    fn try_recv(&mut self) -> Option<SyncMessage>;
}

/// An in-process link (a worker and its tab, or tests): each side's sends
/// land in the other side's receive queue.
struct InProcess {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for InProcess {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Two connected in-process transports: (client side, server side).
pub fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(InProcess {
            outbound: up.clone(),
            inbound: down.clone(),
        }),
        Box::new(InProcess {
            outbound: down,
            inbound: up,
        }),
    )
}

/// A byte-frame transport (a socket).
pub use crate::wire::WireTransport;

/// Frames messages with postcard over a byte transport.
pub struct WireTransportAdapter<W>(W);

impl<W> WireTransportAdapter<W> {
    pub fn current(wire: W) -> Self {
        Self(wire)
    }
}

impl<W: WireTransport> Transport for WireTransportAdapter<W> {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let frame =
            postcard::to_allocvec(&message).map_err(|e| TransportError::Failed(e.to_string()))?;
        self.0.send_frame(frame)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let frame = self.0.try_recv_frame()?;
        Some(postcard::from_bytes(&frame).expect("decode sync frame"))
    }
}

/// One stored SQL cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Cell {
    Null,
    Int(i64),
    Real(f64),
    /// Raw TEXT bytes: a large-capable cell may be tagged bytes or a chunked
    /// descriptor, which travel as stored (never as chunk bytes).
    Text(#[serde(with = "serde_bytes")] Vec<u8>),
    Blob(Vec<u8>),
}

impl From<Sql> for Cell {
    fn from(value: Sql) -> Self {
        match value {
            Sql::Null => Cell::Null,
            Sql::Integer(v) => Cell::Int(v),
            Sql::Real(v) => Cell::Real(v),
            Sql::Text(v) => Cell::Text(v),
            Sql::Blob(v) => Cell::Blob(v),
        }
    }
}

impl From<&Cell> for Sql {
    fn from(value: &Cell) -> Self {
        match value {
            Cell::Null => Sql::Null,
            Cell::Int(v) => Sql::Integer(*v),
            Cell::Real(v) => Sql::Real(*v),
            Cell::Text(v) => Sql::Text(v.clone()),
            Cell::Blob(v) => Sql::Blob(v.clone()),
        }
    }
}

/// One stored row version: the unit of replication.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Image {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub seq: u64,
    pub deleted: bool,
    /// Whether the version's commit wrote content (see `_c`): history
    /// reads list only content versions.
    pub content: bool,
    pub cells: Vec<(String, Cell)>,
}

/// Which branch a replicated write addresses.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireTarget {
    Root,
    Exact(BranchSelector),
    View {
        head: BranchSelector,
        base: Option<BranchSelector>,
    },
    /// A view over a base frozen at a snapshot (resolved by the receiver).
    FrozenView {
        head: BranchSelector,
        base: BranchSelector,
        snapshot: SnapshotRef,
    },
}

/// A replicated write.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireOp {
    Insert {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
    },
    Update {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
    },
    Upsert {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
    },
    Delete {
        table: String,
        row: [u8; 16],
        target: WireTarget,
    },
    Restore {
        table: String,
        row: [u8; 16],
        cells: Option<BTreeMap<String, Value>>,
        target: WireTarget,
    },
    Move {
        table: String,
        row: [u8; 16],
        from: BranchSelector,
        to: BranchSelector,
    },
}

/// One read of a serializable transaction: the rows it returned at the
/// snapshot, with their server versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireRead {
    pub query: Query,
    pub binding: BTreeMap<String, Value>,
    pub rows: Vec<([u8; 16], u64)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireKind {
    Mergeable,
    /// `own`: the sender's own commits (by local seq) that were unsettled
    /// when the transaction began; its snapshot includes them, and the
    /// authority resolves each to where it settled.
    Serializable {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<u64>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireError {
    Conflict(String),
    Rejected(String),
    Other(String),
}

/// A large-value node named by its locator and the object hash it must
/// match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireChunkRef {
    pub locator: jazz_types::large_values::Locator,
    pub hash: [u8; 32],
}

/// A large-value node's encoded bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireChunk {
    pub locator: jazz_types::large_values::Locator,
    pub hash: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

impl WireChunkRef {
    fn of(request: &ChunkRequest) -> Self {
        Self {
            locator: request.locator,
            hash: request.object_hash,
        }
    }

    fn request(&self) -> ChunkRequest {
        ChunkRequest {
            object_hash: self.hash,
            locator: self.locator,
        }
    }
}

impl WireChunk {
    fn request(&self) -> ChunkRequest {
        ChunkRequest {
            object_hash: self.hash,
            locator: self.locator,
        }
    }
}

/// Encoded node bytes per `PushChunks` or `Chunks` message.
const CHUNK_BATCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    // client → server
    /// `since`: a resumed subscription the client held settled through
    /// that seq; its opening ships only rows changed after it.
    /// `include_deleted`: the result also lists deleted rows (the root's),
    /// which ship as their deleted current versions.
    Subscribe {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        since: Option<u64>,
        include_deleted: bool,
    },
    Unsubscribe {
        id: u64,
    },
    /// Sent first on a reconnect: every settled row version the replica
    /// holds, as (table, id, branch key, seq). The server watches them again
    /// and ships what changed (or an eviction for what the client may no
    /// longer read), so the replica is coherent as a whole, not per query.
    Hold {
        rows: Vec<(String, [u8; 16], Vec<u8>, u64)>,
    },
    /// A one-shot read, at the current state or as of a server seq.
    Fetch {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        at: Option<u64>,
        include_deleted: bool,
    },
    /// `author`: the provenance the writes carry when it is not the
    /// sender's own (a trusted session or an attribution), as a canonical
    /// author subject. Only a trusted link may set one that differs from
    /// its own identity.
    Commit {
        node: [u8; 16],
        tx: u64,
        kind: WireKind,
        ops: Vec<WireOp>,
        author: Option<String>,
    },
    /// Large-value nodes the client's descriptors name and it lacks
    /// (pull by locator; holding the locator is the read capability).
    ChunkRequests {
        requests: Vec<WireChunkRef>,
    },
    /// Nodes of the client's new large values, sent ahead of the commit
    /// that references them. The server holds them under a receipt until
    /// the commit arrives or the receipt expires.
    PushChunks {
        nodes: Vec<WireChunk>,
    },
    // server → client
    /// Requested nodes, and the requests the server cannot answer.
    Chunks {
        nodes: Vec<WireChunk>,
        missing: Vec<WireChunkRef>,
    },
    Rows {
        images: Vec<Image>,
    },
    Evict {
        table: String,
        id: [u8; 16],
        key: Vec<u8>,
    },
    /// A subscription's result is complete through server seq `through`;
    /// `window` lists its rows when it is limited.
    Settled {
        id: u64,
        through: u64,
        window: Option<Vec<[u8; 16]>>,
    },
    Answer {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
    },
    Fate {
        tx: u64,
        through: u64,
        result: Result<(), WireError>,
    },
    // client → server
    /// A permission preflight: would the session's identity be allowed
    /// `action` right now? Never writes.
    Advise {
        id: u64,
        action: PermissionAdviceAction,
    },
    // server → client
    Advice {
        id: u64,
        advice: PermissionAdvice,
    },
    // client → server
    /// The schema version the client reads and writes by, sent first on
    /// every connection. Its queries, writes, row images, evictions and held
    /// versions use that version's table and column names; the server maps
    /// them onto its stored rows. A version the server has not admitted is
    /// named like the server's current schema.
    Schema {
        version: [u8; 16],
    },
    // server → client
    /// The transactions behind the versions a history read ships, as
    /// (seq, writer node, writer tx), sent ahead of their `Rows`: a
    /// replica lists a history version only once it knows its
    /// `$transactionId`.
    Origins {
        commits: Vec<(u64, [u8; 16], u64)>,
    },
}

// ---- client state ---------------------------------------------------------

pub(crate) enum Kind {
    Mergeable,
    Serializable {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<u64>,
    },
}

struct Pending {
    tx: u64,
    ops: Vec<StagedOp>,
    rows: Vec<(String, RowUuid, Vec<u8>)>,
}

/// One remote attachment: a client query the server maintains.
pub(crate) struct Remote {
    id: u64,
    refs: usize,
    query: Query,
    binding: BTreeMap<String, Value>,
    include_deleted: bool,
    pub(crate) answered: Rc<StdCell<bool>>,
    /// The server seq this query's result was last settled through.
    through: Option<u64>,
    /// Resubscribed with `since`: on its opening, fetch result rows the
    /// replica lacks (rows that became visible without changing).
    resumed: bool,
}

/// What a client knows about its path to the server.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Link {
    pub(crate) hint: RemoteLinkHint,
    /// When the hint was last reported (an attempt's start).
    pub(crate) since: Instant,
    /// Whether the host reported a hint at all (else the own upstream decides).
    pub(crate) reported: bool,
    /// The own upstream was detached: a live report cannot hold reads.
    pub(crate) detached: bool,
}

/// Handle for a connected upstream.
#[derive(Debug)]
pub struct Upstream(u64);

pub(crate) struct Client {
    transport: Option<Box<dyn Transport>>,
    pub(crate) hint: Rc<StdCell<Link>>,
    next_id: u64,
    remote: HashMap<String, Remote>,
    remote_ids: HashMap<u64, String>,
    outbox: Vec<SyncMessage>,
    pending: Vec<Pending>,
    unsent: Vec<SyncMessage>,
    /// Commits sent but not settled, for resending after a reconnect.
    sent: Vec<SyncMessage>,
    node: [u8; 16],
    connection: u64,
    fates: HashMap<u64, Result<(), Error>>,
    shadow: HashMap<(String, RowUuid, Vec<u8>), Option<Image>>,
    /// Highest server seq any answer was complete through.
    pub(crate) known: u64,
    /// The `known` cut last written to `_jazz_meta`.
    persisted_known: u64,
    answers: HashMap<u64, Option<(u64, Vec<[u8; 16]>)>>,
    /// Permission preflights asked for; `None` until answered.
    advice: HashMap<u64, Option<PermissionAdvice>>,
    /// Large-value nodes asked for and not yet answered.
    requested: BTreeSet<ChunkRequest>,
    /// Nodes to ask for on the next send.
    wanted: BTreeSet<ChunkRequest>,
    /// Nodes pushed for each unsettled commit.
    pushed: HashMap<u64, Vec<jazz_types::large_values::Locator>>,
    /// Requests the server could not answer; never asked again.
    unavailable: BTreeSet<ChunkRequest>,
    /// Chunks arrived this tick: results waiting for them run again.
    arrived: bool,
}

fn remote_key(query: &Query, binding: &BTreeMap<String, Value>, include_deleted: bool) -> String {
    format!("{query:?}{binding:?}{include_deleted}")
}

/// Whether a read may wait for the server: a live link needs the own
/// upstream connected; an attempt holds reads only for its window.
pub(crate) fn may_wait(link: Link) -> bool {
    match link.hint {
        RemoteLinkHint::Live => !link.detached,
        RemoteLinkHint::Attempting => {
            link.since.elapsed() < crate::options::REMOTE_LINK_ATTEMPT_WINDOW
        }
        RemoteLinkHint::Failed | RemoteLinkHint::NoServer => false,
    }
}

/// When a gated opening is released.
pub(crate) struct Gate {
    pub(crate) answered: Rc<StdCell<bool>>,
    pub(crate) hint: Rc<StdCell<Link>>,
    /// Held on a live link: leaving it (even for a new attempt) releases.
    pub(crate) live: bool,
    /// `None`: wait for the server however long (the Remote tier).
    pub(crate) deadline: Option<Instant>,
}

impl Gate {
    pub(crate) fn open(&self) -> bool {
        if self.answered.get() {
            return true;
        }
        let link = self.hint.get();
        match self.deadline {
            None => false,
            Some(deadline) => {
                Instant::now() >= deadline
                    || !may_wait(link)
                    || (self.live && link.hint != RemoteLinkHint::Live)
            }
        }
    }
}

/// How a subscription's deltas report `settled`.
#[derive(Clone)]
pub(crate) enum SettledBy {
    Always,
    Never,
    Remote(Rc<StdCell<bool>>),
}

impl SettledBy {
    pub(crate) fn get(&self) -> bool {
        match self {
            SettledBy::Always => true,
            SettledBy::Never => false,
            SettledBy::Remote(answered) => answered.get(),
        }
    }
}

// ---- server state ---------------------------------------------------------

pub(crate) struct Session {
    transport: Box<dyn Transport>,
    identity: AuthorSubject,
    /// Per client subscription: its queue, its group, whether it is
    /// windowed, and whether its opening's row ids were sent.
    subs: BTreeMap<u64, (fanout::Queue, Weak<RefCell<fanout::Group>>, bool, bool)>,
    outbox: Vec<SyncMessage>,
    /// Evictions answering a `Hold`: they describe the state the client
    /// declared, so they go out before any row this tick ships (a resumed
    /// subscription may ship a row's newer version).
    stale: Vec<SyncMessage>,
    /// Images to send this tick: (table, id, branch key).
    touched: BTreeSet<(String, RowUuid, Vec<u8>)>,
    /// The versions this tick's accepted commits from the client wrote:
    /// (table, id, branch key, seq). Each ships even when a later version
    /// supersedes it, so the replica holds every state its own commits
    /// produced (a snapshot naming one reads it there).
    committed: Vec<(String, RowUuid, Vec<u8>, u64)>,
    /// Supporting rows each subscription needs besides its own result.
    support: BTreeMap<u64, Rc<Vec<Support>>>,
    /// The replica's coherence watches: `(table, column, value)` for every
    /// row shipped (`id`), include target and array correlation. A commit
    /// that writes a matching row ships its image (or an eviction).
    watch: HashSet<u64>,
    /// Resumed subscriptions whose opening is still to send: `since` seq.
    resume: BTreeMap<u64, u64>,
    /// Supporting rows shipped to the replica, per table, with whether the
    /// client could read each when last checked. A commit to a table their
    /// read policy depends on re-checks them: lost access ships an
    /// eviction, regained access ships the image.
    held: BTreeMap<String, BTreeMap<(RowUuid, Vec<u8>), bool>>,
    /// False once the client disconnected or its transport failed; the
    /// slot holds nothing and is reused by the next subscriber.
    open: bool,
    /// Pushed chunk bytes admitted in the current staging-policy window,
    /// and when it started (the ingress rate limit).
    ingress: (Instant, usize),
    /// Inbound messages held back by the rate limit, in order.
    backlog: VecDeque<SyncMessage>,
    /// The schema version the client declared (`None`: the current one).
    version: Option<crate::ids::SchemaVersionId>,
    /// That version's schema, governed by the current schema's policies,
    /// as built for the current schema it holds.
    schema: Option<(Rc<Schema>, Rc<Schema>)>,
}

impl Session {
    fn new(transport: Box<dyn Transport>, identity: AuthorSubject) -> Self {
        Self {
            transport,
            identity,
            subs: BTreeMap::new(),
            outbox: Vec::new(),
            stale: Vec::new(),
            touched: BTreeSet::new(),
            committed: Vec::new(),
            support: BTreeMap::new(),
            watch: HashSet::new(),
            resume: BTreeMap::new(),
            held: BTreeMap::new(),
            open: true,
            ingress: (Instant::now(), 0),
            backlog: VecDeque::new(),
            version: None,
            schema: None,
        }
    }

    /// Release everything the session holds; its shared results are swept
    /// once no other member reads them.
    fn close(&mut self, dropped: &std::cell::Cell<usize>) {
        dropped.set(dropped.get() + self.subs.len());
        *self = Session {
            open: false,
            ..Session::new(Box::new(ClosedTransport), self.identity)
        };
    }

    /// Nothing to send this tick: no queued result events, no rows its
    /// commits touched, no replies waiting.
    fn idle(&self) -> bool {
        self.touched.is_empty()
            && self.outbox.is_empty()
            && self.stale.is_empty()
            && self.resume.is_empty()
            && self
                .subs
                .values()
                .all(|(queue, ..)| queue.borrow().is_empty())
    }
}

struct ClosedTransport;

impl Transport for ClosedTransport {
    fn send(&mut self, _: SyncMessage) -> Result<(), TransportError> {
        Err(TransportError::Failed("session closed".into()))
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

/// A table a remote query reads besides its own: the client evaluates the
/// query locally, so the server ships these rows too.
#[derive(Debug)]
pub(crate) enum Support {
    /// Forward reference `column` → `target.id`.
    Include { column: String, target: String },
    /// Children `table.inner = parent.outer` (all readable ones; the client
    /// applies the array's own filters and window), and the support of each
    /// child (nested arrays).
    Array {
        table: String,
        inner: String,
        outer: String,
        nested: Vec<Support>,
    },
    /// The edges of a gather from the row's `frontier` value: edge rows
    /// whose `member` holds it, then transitively from each edge's
    /// `parent`. The depth bound is the client's to apply.
    Reach {
        frontier: String,
        edges: String,
        member: String,
        parent: String,
    },
}

/// Whether a reachability traversal is a gather the replica can evaluate
/// from the result and its edges ([`Support::Reach`]): seeded by rows of
/// the root table that are themselves in the result (no filter after the
/// gather), reaching root rows by id.
fn gather_shaped(query: &Query, via: &crate::query::ReachableVia) -> bool {
    via.seed.as_ref().is_some_and(|seed| {
        seed.table == query.table && seed.user_column.is_none() && seed.team_column == "id"
    }) && via.access_table == query.table
        && via.access_row_column == "id"
        && via.access_team_column == "id"
        && via.access_filters.is_empty()
        && query.filters.is_empty()
}

/// Whether a junction traversal's witnesses can ship as [`Support::Array`]
/// children of the row it starts from: it correlates with a column of that
/// row (not a referenced row's), at every level.
fn join_followable(join: &crate::query::JoinVia) -> bool {
    join.target != crate::query::JoinTarget::Uncorrelated
        && join.source_lookup.is_none()
        && join.nested_joins.iter().all(join_followable)
}

/// The edge table of a [`Support::Reach`], by stored names.
struct Reach<'a> {
    edges: &'a str,
    member: &'a str,
    parent: &'a str,
}

/// A watch as a 64-bit hash: per-session checks stay a few integer probes
/// however many sessions there are. A collision only ships an extra image.
fn wk(key: (String, String, String)) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn watch_key(cell: &Sql) -> String {
    format!("{cell:?}")
}

fn image_cell(image: &Image, column: &str) -> Option<Sql> {
    if column == "id" {
        return Some(Sql::Blob(image.id.to_vec()));
    }
    image
        .cells
        .iter()
        .find(|(name, _)| name == column)
        .map(|(_, cell)| Sql::from(cell))
}

/// Handle for an accepted subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriberId(pub usize);

fn wire_error(error: &Error) -> WireError {
    match error.code {
        ErrorCode::TransactionConflict => WireError::Conflict(error.message.clone()),
        ErrorCode::WriteRejected => WireError::Rejected(error.message.clone()),
        _ => WireError::Other(error.to_string()),
    }
}

fn from_wire_error(error: WireError) -> Error {
    match error {
        WireError::Conflict(m) => Error::new(ErrorCode::TransactionConflict, m),
        WireError::Rejected(m) => Error::new(ErrorCode::WriteRejected, m),
        WireError::Other(m) => Error::new(ErrorCode::Protocol, m),
    }
}

fn wire_target(target: &WriteTarget) -> Result<WireTarget, Error> {
    Ok(match target {
        WriteTarget::Root => WireTarget::Root,
        WriteTarget::BranchView { head, base } => WireTarget::View {
            head: head.clone(),
            base: match base {
                None => None,
                Some(BranchViewBase::Current(base)) => Some(base.clone()),
                Some(BranchViewBase::Snapshot { branch, snapshot }) => {
                    return Ok(WireTarget::FrozenView {
                        head: head.clone(),
                        base: branch.clone(),
                        snapshot: snapshot.clone(),
                    });
                }
            },
        },
    })
}

fn exact_target(target: &ExactWriteTarget) -> WireTarget {
    match target {
        ExactWriteTarget::Root => WireTarget::Root,
        ExactWriteTarget::Branch(selector) => WireTarget::Exact(selector.clone()),
    }
}

fn to_wire(op: &StagedOp) -> Result<WireOp, Error> {
    let bytes = |row: &RowUuid| *row.0.as_bytes();
    Ok(match op {
        StagedOp::Insert(table, cells, opts) => WireOp::Insert {
            table: table.clone(),
            row: bytes(&opts.row_id.expect("inserts carry their row id")),
            cells: cells.clone(),
            target: exact_target(&opts.target),
        },
        StagedOp::Update(table, row, cells, opts) => WireOp::Update {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: wire_target(&opts.target)?,
        },
        StagedOp::Upsert(table, row, cells, opts) => WireOp::Upsert {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: wire_target(&opts.target)?,
        },
        StagedOp::Delete(table, row, opts) => WireOp::Delete {
            table: table.clone(),
            row: bytes(row),
            target: wire_target(&opts.target)?,
        },
        StagedOp::Restore(table, row, cells, opts) => WireOp::Restore {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: exact_target(&opts.target),
        },
        StagedOp::Move(table, from, to, row) => WireOp::Move {
            table: table.clone(),
            row: bytes(row),
            from: from.clone(),
            to: to.clone(),
        },
    })
}

fn view_target(target: WireTarget) -> WriteTarget {
    match target {
        WireTarget::Root | WireTarget::Exact(_) => WriteTarget::Root,
        WireTarget::View { head, base } => WriteTarget::BranchView {
            head,
            base: base.map(BranchViewBase::Current),
        },
        WireTarget::FrozenView {
            head,
            base,
            snapshot,
        } => WriteTarget::BranchView {
            head,
            base: Some(BranchViewBase::Snapshot {
                branch: base,
                snapshot,
            }),
        },
    }
}

fn exact(target: WireTarget) -> ExactWriteTarget {
    match target {
        WireTarget::Exact(selector) => ExactWriteTarget::Branch(selector),
        _ => ExactWriteTarget::Root,
    }
}

fn from_wire(op: WireOp) -> Result<StagedOp, Error> {
    let id = |row: [u8; 16]| RowUuid::from_bytes(row);
    Ok(match op {
        WireOp::Insert {
            table,
            row,
            cells,
            target,
        } => StagedOp::Insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(id(row)),
                target: exact(target),
                ..Default::default()
            },
        ),
        WireOp::Update {
            table,
            row,
            cells,
            target,
        } => StagedOp::Update(
            table,
            id(row),
            cells,
            UpdateOptions {
                target: view_target(target),
                ..Default::default()
            },
        ),
        WireOp::Upsert {
            table,
            row,
            cells,
            target,
        } => StagedOp::Upsert(
            table,
            id(row),
            cells,
            UpsertOptions {
                target: view_target(target),
                ..Default::default()
            },
        ),
        WireOp::Delete { table, row, target } => StagedOp::Delete(
            table,
            id(row),
            DeleteOptions {
                target: view_target(target),
                ..Default::default()
            },
        ),
        WireOp::Restore {
            table,
            row,
            cells,
            target,
        } => StagedOp::Restore(
            table,
            id(row),
            cells,
            RestoreOptions {
                target: exact(target),
                ..Default::default()
            },
        ),
        // A move checks no policy locally; the authority would need the
        // source DELETE and destination INSERT policies under the session.
        WireOp::Move { .. } => return Err(unsupported("branch moves over sync")),
    })
}

/// The image filter for one row version. `+_seq` keeps the planner on
/// the row-id index: after a bulk commit every row shares one `_seq`, and
/// the `_seq` index would scan them all for each lookup.
const ID_AT_SEQ: &str = "id = ?1 AND +_seq = ?2";

// ---- stored images ----------------------------------------------------------

impl Inner {
    /// The supporting tables a remote query reads (see [`Support`]), by
    /// stored table and column names.
    pub(crate) fn support_of(&self, query: &Query) -> Result<Vec<Support>, Error> {
        // History reads take no relationships.
        if is_history_table(&query.table) {
            return Ok(Vec::new());
        }
        let table = self.schema.table(&query.table)?;
        let mut support = Vec::new();
        for include in &query.includes {
            let target = table
                .references
                .get(&include.path)
                .ok_or_else(|| query_error(format!("`{}` is not a reference", include.path)))?;
            support.push(Support::Include {
                column: table.stored_name(&include.path).to_owned(),
                target: self.schema.stored_table(target)?.to_owned(),
            });
        }
        support.extend(self.array_support(table, &query.array_subqueries)?);
        support.extend(self.join_support(table, &query.joins)?);
        for via in &query.reachable {
            let edges = self.schema.table(&via.edge_table)?;
            support.push(Support::Reach {
                frontier: table.stored_name(&via.access_team_column).to_owned(),
                edges: edges.stored.clone(),
                member: edges.stored_name(&via.edge_member_column).to_owned(),
                parent: edges.stored_name(&via.edge_parent_column).to_owned(),
            });
        }
        Ok(support)
    }

    /// The support of `arrays` read from rows of `parent`, nested arrays
    /// included.
    fn array_support(
        &self,
        parent: &crate::values::Table,
        arrays: &[crate::query::ArraySubquery],
    ) -> Result<Vec<Support>, Error> {
        let mut support = Vec::with_capacity(arrays.len());
        for array in arrays {
            let child = self.schema.table(&array.table)?;
            support.push(Support::Array {
                table: child.stored.clone(),
                inner: child.stored_name(&array.inner_column).to_owned(),
                outer: parent.stored_name(&array.outer_column).to_owned(),
                nested: self.array_support(child, &array.nested_arrays)?,
            });
        }
        Ok(support)
    }

    /// The support of junction traversals from rows of `parent`: every
    /// readable row of the joined table correlated with the parent row
    /// (the client applies the join's filters), and its nested joins'.
    fn join_support(
        &self,
        parent: &crate::values::Table,
        joins: &[crate::query::JoinVia],
    ) -> Result<Vec<Support>, Error> {
        let mut support = Vec::with_capacity(joins.len());
        for join in joins {
            let target = self.schema.table(&join.table)?;
            support.push(Support::Array {
                table: target.stored.clone(),
                inner: target.stored_name(&join.on_column).to_owned(),
                outer: parent
                    .stored_name(join.source_column.as_deref().unwrap_or("id"))
                    .to_owned(),
                nested: self.join_support(target, &join.nested_joins)?,
            });
        }
        Ok(support)
    }

    /// The images a history read ships for its result `versions` (row and
    /// seq, of `source`): each version, then the current image of its row
    /// in its branch, so a replica never takes an old version for its
    /// current one. The versions' seqs go to `seqs`, for their origins.
    fn history_versions(
        &self,
        source: &str,
        versions: impl IntoIterator<Item = (RowUuid, u64)>,
        images: &mut Vec<Image>,
        seqs: &mut BTreeSet<u64>,
    ) -> Result<(), Error> {
        let mut current = BTreeSet::new();
        for (row, seq) in versions {
            let rid = row.0.as_bytes().to_vec();
            let seq = seq as i64;
            let mut found = self.images_where(source, false, ID_AT_SEQ, &[&rid, &seq])?;
            if found.is_empty() {
                found = self.images_where(source, true, ID_AT_SEQ, &[&rid, &seq])?;
            }
            for image in &found {
                seqs.insert(image.seq);
                current.insert((row, image.key.clone()));
            }
            images.extend(found);
        }
        for (row, key) in current {
            images.extend(self.current_image(source, row, &key)?);
        }
        Ok(())
    }

    /// The images a flat-join occurrence ships for one joined row of stored
    /// table `table`: its current images, or for a history source the
    /// version it names and the row's current image.
    fn joined_images(
        &self,
        table: &str,
        joined: &crate::values::JoinedRow,
        seqs: &mut BTreeSet<u64>,
    ) -> Result<Vec<Image>, Error> {
        let id = joined.row.0.as_bytes().to_vec();
        let Some((tx, node)) = joined.version else {
            return self.images_where(table, false, "id = ?1", &[&id]);
        };
        // A version's seq: as `_jazz_settled` recorded its upload, else
        // this authority's own commit (whose tx is its seq).
        let recorded = self
            .conn
            .prepare_cached("SELECT seq FROM _jazz_settled WHERE tx = ?1 AND node = ?2")
            .and_then(|mut statement| {
                statement
                    .query_row(
                        rusqlite::params![tx as i64, node.as_bytes().to_vec()],
                        |r| r.get::<_, i64>(0),
                    )
                    .optional()
            })
            .map_err(storage)?;
        let seq = match recorded {
            Some(seq) => seq as u64,
            None if node == self.identity.node => tx,
            None => return Ok(Vec::new()),
        };
        let mut images = Vec::new();
        self.history_versions(table, [(joined.row, seq)], &mut images, seqs)?;
        Ok(images)
    }

    /// Who committed each of `seqs`, as (seq, node, tx): `_jazz_settled`
    /// for uploads, this authority's node for its own commits.
    fn origins(&self, seqs: &BTreeSet<u64>) -> Result<Vec<(u64, [u8; 16], u64)>, Error> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT node, tx FROM _jazz_settled WHERE seq = ?1")
            .map_err(storage)?;
        let mut out = Vec::with_capacity(seqs.len());
        for &seq in seqs {
            let recorded = statement
                .query_row([seq as i64], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)? as u64))
                })
                .optional()
                .map_err(storage)?;
            out.push(match recorded {
                Some((node, tx)) => (seq, node.try_into().unwrap_or([0; 16]), tx),
                None => (seq, *self.identity.node.as_bytes(), seq),
            });
        }
        Ok(out)
    }

    /// Ship the readable supporting rows of one shipped image, and watch
    /// their keys so later writes reach the replica.
    fn follow_support(
        &self,
        support: &[Support],
        image: &Image,
        identity: Option<AuthorSubject>,
        out: &mut Vec<Image>,
        watch: &mut HashSet<u64>,
        held: &mut BTreeMap<String, BTreeMap<(RowUuid, Vec<u8>), bool>>,
    ) -> Result<(), Error> {
        for item in support {
            let (table, column, value, nested) = match item {
                Support::Reach {
                    frontier,
                    edges,
                    member,
                    parent,
                } => {
                    if let Some(value) = image_cell(image, frontier) {
                        let reach = Reach {
                            edges,
                            member,
                            parent,
                        };
                        self.follow_edges(&reach, value, identity, out, watch, held)?;
                    }
                    continue;
                }
                Support::Include { column, target } => match image_cell(image, column) {
                    Some(value @ Sql::Blob(_)) => (target, "id", value, &[][..]),
                    _ => continue,
                },
                Support::Array {
                    table,
                    inner,
                    outer,
                    nested,
                } => match image_cell(image, outer) {
                    Some(Sql::Null) | None => continue,
                    Some(value) => (table, inner.as_str(), value, &nested[..]),
                },
            };
            // A watched key's rows were shipped with it, and later writes
            // to them ship as they happen. Nested support is followed
            // again: a child that joined since may have unwatched children.
            let fresh = watch.insert(wk((table.clone(), column.to_owned(), watch_key(&value))));
            if !fresh && nested.is_empty() {
                continue;
            }
            let filter = if column == "id" {
                "id = ?1".to_owned()
            } else {
                format!("{} = ?1", quote(column))
            };
            for found in self.images_where(table, false, &filter, &[&value])? {
                let row = RowUuid::from_bytes(found.id);
                let can = readable(self, table, row, &found.key, identity)?;
                if identity.is_some() {
                    held.entry(table.clone())
                        .or_default()
                        .insert((row, found.key.clone()), can);
                }
                if !can {
                    continue;
                }
                let unwatched = watch.insert(wk((
                    table.clone(),
                    "id".to_owned(),
                    watch_key(&Sql::Blob(found.id.to_vec())),
                )));
                // Not watched yet, so the replica does not hold it (rows it
                // declared on resume are watched already).
                let ship = fresh || unwatched;
                if nested.is_empty() {
                    if ship {
                        out.push(found);
                    }
                    continue;
                }
                if ship {
                    out.push(found.clone());
                }
                self.follow_support(nested, &found, identity, out, watch, held)?;
            }
        }
        Ok(())
    }

    /// Ship the readable edges of a gather from `value` and, transitively,
    /// from their parents, watching each frontier value so later edges
    /// reach the replica. A watched value was followed already: cycles end.
    fn follow_edges(
        &self,
        reach: &Reach<'_>,
        value: Sql,
        identity: Option<AuthorSubject>,
        out: &mut Vec<Image>,
        watch: &mut HashSet<u64>,
        held: &mut BTreeMap<String, BTreeMap<(RowUuid, Vec<u8>), bool>>,
    ) -> Result<(), Error> {
        let mut frontier = vec![value];
        while let Some(value) = frontier.pop() {
            if matches!(value, Sql::Null) {
                continue;
            }
            let key = (
                reach.edges.to_owned(),
                reach.member.to_owned(),
                watch_key(&value),
            );
            if !watch.insert(wk(key)) {
                continue;
            }
            let filter = if reach.member == "id" {
                "id = ?1".to_owned()
            } else {
                format!("{} = ?1", quote(reach.member))
            };
            for found in self.images_where(reach.edges, false, &filter, &[&value])? {
                let row = RowUuid::from_bytes(found.id);
                let can = readable(self, reach.edges, row, &found.key, identity)?;
                if identity.is_some() {
                    held.entry(reach.edges.to_owned())
                        .or_default()
                        .insert((row, found.key.clone()), can);
                }
                if !can {
                    continue;
                }
                if let Some(parent) = image_cell(&found, reach.parent) {
                    frontier.push(parent);
                }
                let unwatched = watch.insert(wk((
                    reach.edges.to_owned(),
                    "id".to_owned(),
                    watch_key(&Sql::Blob(found.id.to_vec())),
                )));
                if unwatched {
                    out.push(found);
                }
            }
        }
        Ok(())
    }

    /// Stored tables stored table `table`'s read policy reads, as `author`
    /// (claims do not change which tables a policy names).
    fn read_policy_tables(
        &self,
        table: &str,
        author: AuthorSubject,
    ) -> Result<BTreeSet<String>, Error> {
        let schema = self.owner.clone();
        let table = owned_table(&schema, table)?;
        let session = self.session(author);
        let binding = BTreeMap::new();
        let claims = crate::compile::Session::default();
        let mut ctx = crate::compile::Ctx::new(&schema, None, &claims, &binding);
        ctx.operation_policy(table, Operation::Select, "a", &session)?;
        let mut tables = ctx.tables;
        tables.remove(&table.stored);
        Ok(tables)
    }

    /// Every settled row version in the replica (pending local versions sit
    /// above `LOCAL_BASE`; their settled images are in the shadow), by
    /// stored table.
    fn held_versions(&self) -> Result<Vec<(String, [u8; 16], Vec<u8>, u64)>, Error> {
        let mut out = Vec::new();
        for table in self.owner.tables.values().map(|table| &table.stored) {
            let sql = format!(
                "SELECT id, _b, _seq FROM {} WHERE _seq < ?1 AND _del = 0",
                quote(table)
            );
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let rows = statement
                .query_map([LOCAL_BASE as i64], |r| {
                    let id: Vec<u8> = r.get(0)?;
                    Ok((
                        table.clone(),
                        id.try_into().unwrap_or([0; 16]),
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, i64>(2)? as u64,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                out.push(row.map_err(storage)?);
            }
        }
        Ok(out)
    }

    /// Whether written rows of `deps` may change what `author` can read in
    /// `table`. A dependency the read policy pins to a session value (`EXISTS
    /// memberships WHERE member = session.user ...`) matters only when a
    /// written row's old or new image holds that value; one it does not pin
    /// always matters.
    fn policy_touched(
        &self,
        table: &str,
        author: AuthorSubject,
        deps: &[&String],
        written: &[((String, RowUuid, Vec<u8>), Vec<u64>)],
        values: &mut HashMap<(String, String), BTreeSet<String>>,
    ) -> Result<bool, Error> {
        let schema = self.owner.clone();
        let Some(policy) = owned_table(&schema, table)?.policy(Operation::Select) else {
            return Ok(true);
        };
        let session = self.session(author);
        let mut pins = HashMap::new();
        if !super::fanout::policy_pins(policy, &session, &mut pins) {
            return Ok(true);
        }
        let pins = super::fanout::stored_pins(&schema, pins);
        for dep in deps {
            let Some(Some((column, value))) = pins.get(dep.as_str()) else {
                return Ok(true);
            };
            let key = ((*dep).clone(), column.clone());
            if !values.contains_key(&key) {
                let columns = BTreeSet::from([column.clone()]);
                let mut seen = BTreeSet::new();
                for ((t, row, branch), _) in written.iter().filter(|((t, ..), _)| t == *dep) {
                    for image in self.images(t, *row, branch, &columns) {
                        seen.extend(
                            image
                                .into_iter()
                                .filter(|(c, _)| c == column)
                                .map(|(_, v)| v),
                        );
                    }
                }
                values.insert(key.clone(), seen);
            }
            if values[&key].contains(value) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Row images selected from `table` (or its history) by `filter`.
    fn images_where(
        &self,
        table: &str,
        history: bool,
        filter: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<Image>, Error> {
        let name = if history {
            format!("{table}__h")
        } else {
            table.to_owned()
        };
        let sql = format!("SELECT * FROM {} WHERE {filter}", quote(&name));
        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
        let names: Vec<String> = statement
            .column_names()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let rows = statement
            .query_map(params, |r| {
                let mut image = Image {
                    table: table.to_owned(),
                    id: [0; 16],
                    key: Vec::new(),
                    seq: 0,
                    deleted: false,
                    content: true,
                    cells: Vec::new(),
                };
                for (i, name) in names.iter().enumerate() {
                    match name.as_str() {
                        "rid" => {}
                        "id" => image.id = r.get::<_, Vec<u8>>(i)?.try_into().unwrap_or([0; 16]),
                        "_b" => image.key = r.get(i)?,
                        "_seq" => image.seq = r.get::<_, i64>(i)? as u64,
                        "_del" => image.deleted = r.get::<_, i64>(i)? != 0,
                        "_c" => image.content = r.get::<_, i64>(i)? != 0,
                        _ => image
                            .cells
                            .push((name.clone(), Cell::from(r.get::<_, Sql>(i)?))),
                    }
                }
                Ok(image)
            })
            .map_err(storage)?;
        let mut images = rows.collect::<Result<Vec<_>, _>>().map_err(storage)?;
        for image in &mut images {
            self.resolve_authors(image)?;
        }
        Ok(images)
    }

    /// Images carry authors by their identity parts: codes are local to
    /// one database, so they never leave it.
    /// An update stamp stored as NULL is sent as the create stamp it
    /// equals.
    fn resolve_authors(&self, image: &mut Image) -> Result<(), Error> {
        for i in 0..image.cells.len() {
            let Some(created) = crate::values::elided_as(&image.cells[i].0) else {
                continue;
            };
            if !matches!(image.cells[i].1, Cell::Null) {
                continue;
            }
            let stamp = image
                .cells
                .iter()
                .find(|(name, _)| name == created)
                .map(|(_, cell)| cell.clone());
            if let Some(stamp) = stamp {
                image.cells[i].1 = stamp;
            }
        }
        let mut cells = Vec::with_capacity(image.cells.len() + 2);
        for (name, cell) in std::mem::take(&mut image.cells) {
            if name != "$createdBy" && name != "$updatedBy" {
                cells.push((name, cell));
                continue;
            }
            let (account, issuer, subject) = match cell {
                Cell::Int(code) => {
                    let (account, issuer, subject) = self.author_parts(code)?;
                    (
                        Cell::Blob(account.as_bytes().to_vec()),
                        Cell::Text(issuer.into_bytes()),
                        Cell::Text(subject.into_bytes()),
                    )
                }
                _ => (Cell::Null, Cell::Null, Cell::Null),
            };
            cells.push((format!("{name}.account"), account));
            cells.push((format!("{name}.identity.issuer"), issuer));
            cells.push((format!("{name}.identity.subject"), subject));
        }
        image.cells = cells;
        Ok(())
    }

    /// The stored cells of an image: author parts back to local codes.
    fn stored_cells(&self, image: &Image) -> Result<Vec<(String, Sql)>, Error> {
        let mut cells = Vec::with_capacity(image.cells.len());
        type Parts = (Option<uuid::Uuid>, Option<String>, Option<String>);
        let mut parts: BTreeMap<&'static str, Parts> = BTreeMap::new();
        for (name, cell) in &image.cells {
            let Some((stored, part)) = crate::values::author_part(name) else {
                cells.push((name.clone(), Sql::from(cell)));
                continue;
            };
            let entry = parts.entry(stored).or_default();
            match (part, cell) {
                ("account", Cell::Blob(bytes)) => entry.0 = uuid::Uuid::from_slice(bytes).ok(),
                ("issuer", Cell::Text(text)) => entry.1 = String::from_utf8(text.clone()).ok(),
                ("subject", Cell::Text(text)) => entry.2 = String::from_utf8(text.clone()).ok(),
                _ => {}
            }
        }
        for (stored, parts) in parts {
            let code = match parts {
                (Some(account), Some(issuer), Some(subject)) => {
                    Sql::Integer(self.author_code(&(account, issuer, subject))?)
                }
                _ => Sql::Null,
            };
            cells.push((stored.to_owned(), code));
        }
        // An update stamp equal to the create stamp is stored as NULL.
        let stamps: BTreeMap<String, Sql> = cells
            .iter()
            .filter(|(name, _)| name == "$createdAt" || name == "$createdBy")
            .cloned()
            .collect();
        for (name, value) in &mut cells {
            if let Some(created) = crate::values::elided_as(name)
                && stamps.get(created) == Some(value)
            {
                *value = Sql::Null;
            }
        }
        Ok(cells)
    }

    fn current_image(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<Option<Image>, Error> {
        let id = row.0.as_bytes().to_vec();
        Ok(self
            .images_where(table, false, "id = ?1 AND _b = ?2", &[&id, &key])?
            .pop())
    }

    fn put_image(&self, image: &Image, history: bool) -> Result<(), Error> {
        let name = if history {
            format!("{}__h", image.table)
        } else {
            image.table.clone()
        };
        let mut names = vec![
            "id".to_owned(),
            "_b".to_owned(),
            "_seq".to_owned(),
            "_del".to_owned(),
            "_c".to_owned(),
        ];
        let mut values = vec![
            Sql::Blob(image.id.to_vec()),
            Sql::Blob(image.key.clone()),
            Sql::Integer(image.seq as i64),
            Sql::Integer(image.deleted as i64),
            Sql::Integer(image.content as i64),
        ];
        for (column, value) in self.stored_cells(image)? {
            names.push(quote(&column));
            values.push(value);
        }
        let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
        let verb = if history {
            "INSERT OR IGNORE"
        } else {
            "INSERT"
        };
        let sql = format!(
            "{verb} INTO {} ({}) VALUES ({})",
            quote(&name),
            names.join(", "),
            placeholders.join(", ")
        );
        self.exec(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        Ok(())
    }

    /// Drop a row's current version and its history from `from_seq` up.
    /// The dropped image is kept for dispatch: keyed invalidation reads old
    /// images from history, which this removes.
    fn drop_row(&self, table: &str, row: RowUuid, key: &[u8], from_seq: u64) -> Result<(), Error> {
        if let Some(image) = self.current_image(table, row, key)? {
            self.gone.borrow_mut().push(image);
        }
        let id = row.0.as_bytes().to_vec();
        self.exec(
            &format!("DELETE FROM {} WHERE id = ?1 AND _b = ?2", quote(table)),
            rusqlite::params![id, key],
        )
        .map_err(storage)?;
        self.exec(
            &format!(
                "DELETE FROM {} WHERE id = ?1 AND _b = ?2 AND _seq >= ?3",
                quote(&format!("{table}__h"))
            ),
            rusqlite::params![id, key, from_seq as i64],
        )
        .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.to_owned(), row, key.to_vec()));
        Ok(())
    }

    /// Merge one server version into the replica: newer replaces current
    /// (which becomes history), older lands in history (an as-of answer).
    fn merge_image(&self, image: &Image) -> Result<(), Error> {
        let row = RowUuid::from_bytes(image.id);
        match self.current_image(&image.table, row, &image.key)? {
            None => {
                self.put_image(image, false)?;
                self.changed
                    .borrow_mut()
                    .push((image.table.clone(), row, image.key.clone()));
            }
            Some(current) if current.seq == image.seq => {}
            Some(current) if current.seq < image.seq || current.seq >= LOCAL_BASE => {
                if current.seq < LOCAL_BASE {
                    self.put_image(&current, true)?;
                }
                self.drop_row(&image.table, row, &image.key, LOCAL_BASE)?;
                self.put_image(image, false)?;
            }
            // An older version: history reads of the row change.
            Some(_) => {
                self.put_image(image, true)?;
                self.changed
                    .borrow_mut()
                    .push((image.table.clone(), row, image.key.clone()));
            }
        }
        Ok(())
    }

    fn evict(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        self.drop_row(table, row, key, 0)
    }

    /// The (stored table, id, key) of the rows a commit at `seq` wrote.
    fn written_rows(
        &self,
        ops: &[StagedOp],
        seq: u64,
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, Error> {
        let mut out = BTreeSet::new();
        for op in ops {
            let Some(row) = op.row() else { continue };
            let table = self.schema.stored_table(op.table())?;
            let sql = format!("SELECT _b FROM {} WHERE {ID_AT_SEQ}", quote(table));
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let keys: Vec<Vec<u8>> = statement
                .query_map(
                    rusqlite::params![row.0.as_bytes().to_vec(), seq as i64],
                    |r| r.get(0),
                )
                .map_err(storage)?
                .collect::<Result<_, _>>()
                .map_err(storage)?;
            for key in keys {
                out.insert((table.to_owned(), row, key));
            }
        }
        Ok(out.into_iter().collect())
    }

    /// The settled image a local write at `seq` superseded: the newest
    /// server version in history (none if the row is new).
    fn settled_before(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<Image>, Error> {
        let id = row.0.as_bytes().to_vec();
        let base = LOCAL_BASE as i64;
        Ok(self
            .images_where(
                table,
                true,
                "id = ?1 AND _b = ?2 AND _seq < ?3 ORDER BY _seq DESC LIMIT 1",
                &[&id, &key, &base],
            )?
            .pop())
    }
}

// ---- persisted sync state --------------------------------------------------

/// Sync state lives in the store itself, written in the same SQLite
/// transaction as the rows it describes, so a crash never separates a local
/// write from its upload record, or a server commit from its dedupe entry.
pub(crate) const SYNC_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_pending (tx INTEGER PRIMARY KEY, commit_msg BLOB NOT NULL, rows BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_shadow (t TEXT NOT NULL, id BLOB NOT NULL, k BLOB NOT NULL, image BLOB, PRIMARY KEY (t, id, k));\
    CREATE TABLE IF NOT EXISTS _jazz_meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_authors (n INTEGER PRIMARY KEY, account BLOB NOT NULL, issuer TEXT NOT NULL, subject TEXT NOT NULL, UNIQUE (account, issuer, subject));\
    CREATE TABLE IF NOT EXISTS _jazz_applied (node BLOB NOT NULL, tx INTEGER NOT NULL, result BLOB NOT NULL, PRIMARY KEY (node, tx));\
    CREATE TABLE IF NOT EXISTS _jazz_settled (node BLOB NOT NULL, tx INTEGER NOT NULL, seq INTEGER, PRIMARY KEY (node, tx));\
    CREATE INDEX IF NOT EXISTS _jazz_settled_by_seq ON _jazz_settled (seq);";

type RowKey = (String, RowUuid, Vec<u8>);

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("encode sync state")
}

impl Inner {
    /// Store requested nodes the server sent.
    fn receive_chunks(
        &mut self,
        nodes: Vec<WireChunk>,
        missing: Vec<WireChunkRef>,
    ) -> Result<(), Error> {
        let client = self.client.as_mut().expect("client");
        let mut accepted = Vec::new();
        for node in nodes {
            let request = node.request();
            // Only nodes this replica asked for, verified against the hash
            // it learned from the parent or descriptor.
            if client.requested.remove(&request) {
                accepted.push((request, node.bytes));
            }
        }
        for missing in missing {
            let request = missing.request();
            client.requested.remove(&request);
            client.unavailable.insert(request);
        }
        client.arrived = true;
        for (request, bytes) in accepted {
            crate::large::install_fetched(&self.conn, &self.chunks, &request, bytes)?;
        }
        Ok(())
    }

    /// The nodes a commit's large values need pushed first: (their
    /// locators, the batches carrying them).
    fn pushes_for(
        &self,
        commit: &SyncMessage,
    ) -> Result<(Vec<jazz_types::large_values::Locator>, Vec<SyncMessage>), Error> {
        let SyncMessage::Commit { ops, .. } = commit else {
            return Ok((Vec::new(), Vec::new()));
        };
        fn large(value: &Value, out: &mut Vec<jazz_types::large_values::LargeValueRef>) {
            match value {
                Value::Large(value) => out.push((**value).clone()),
                Value::Nullable(Some(inner)) => large(inner, out),
                _ => {}
            }
        }
        let mut values = Vec::new();
        for op in ops {
            let cells = match op {
                WireOp::Insert { cells, .. }
                | WireOp::Update { cells, .. }
                | WireOp::Upsert { cells, .. } => Some(cells),
                WireOp::Restore { cells, .. } => cells.as_ref(),
                WireOp::Delete { .. } | WireOp::Move { .. } => None,
            };
            for value in cells.into_iter().flat_map(|cells| cells.values()) {
                large(value, &mut values);
            }
        }
        let mut locators = Vec::new();
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0;
        for value in &values {
            for (request, bytes) in crate::large::unpushed(&self.conn, value)? {
                locators.push(request.locator);
                size += bytes.len();
                batch.push(WireChunk {
                    locator: request.locator,
                    hash: request.object_hash,
                    bytes,
                });
                if size >= CHUNK_BATCH_BYTES {
                    batches.push(SyncMessage::PushChunks {
                        nodes: std::mem::take(&mut batch),
                    });
                    size = 0;
                }
            }
        }
        if !batch.is_empty() {
            batches.push(SyncMessage::PushChunks { nodes: batch });
        }
        Ok((locators, batches))
    }

    fn persist_pending(&self, tx: u64, commit: &SyncMessage, rows: &[RowKey]) -> Result<(), Error> {
        let rows: Vec<(String, [u8; 16], Vec<u8>)> = rows
            .iter()
            .map(|(t, r, k)| (t.clone(), *r.0.as_bytes(), k.clone()))
            .collect();
        self.exec(
            "INSERT OR REPLACE INTO _jazz_pending (tx, commit_msg, rows) VALUES (?1, ?2, ?3)",
            rusqlite::params![tx as i64, encode(commit), encode(&rows)],
        )
        .map_err(storage)?;
        Ok(())
    }

    fn forget_pending(&self, tx: u64) -> Result<(), Error> {
        self.exec("DELETE FROM _jazz_pending WHERE tx = ?1", [tx as i64])
            .map_err(storage)?;
        Ok(())
    }

    fn persist_shadow(&self) -> Result<(), Error> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        self.exec("DELETE FROM _jazz_shadow", []).map_err(storage)?;
        for ((table, row, key), image) in &client.shadow {
            self.exec(
                "INSERT INTO _jazz_shadow (t, id, k, image) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    table,
                    row.0.as_bytes().to_vec(),
                    key,
                    image.as_ref().map(encode)
                ],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    fn persist_meta(&self, key: &str, value: u64) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_meta (k, v) VALUES (?1, ?2)",
            rusqlite::params![key, encode(&value)],
        )
        .map_err(storage)?;
        Ok(())
    }

    fn meta(&self, key: &str) -> Result<Option<u64>, Error> {
        match self
            .conn
            .query_row("SELECT v FROM _jazz_meta WHERE k = ?1", [key], |r| {
                r.get::<_, Vec<u8>>(0)
            }) {
            Ok(bytes) => Ok(postcard::from_bytes(&bytes).ok()),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    fn applied_fate(
        &self,
        node: [u8; 16],
        tx: u64,
    ) -> Result<Option<Result<(), WireError>>, Error> {
        match self.conn.query_row(
            "SELECT result FROM _jazz_applied WHERE node = ?1 AND tx = ?2",
            rusqlite::params![node.to_vec(), tx as i64],
            |r| r.get::<_, Vec<u8>>(0),
        ) {
            Ok(bytes) => Ok(postcard::from_bytes(&bytes).ok()),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    fn record_fate(
        &self,
        node: [u8; 16],
        tx: u64,
        result: &Result<(), WireError>,
    ) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_applied (node, tx, result) VALUES (?1, ?2, ?3)",
            rusqlite::params![node.to_vec(), tx as i64, encode(result)],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Where a commit landed in the authority's order: `Some(seq)` once
    /// accepted, `None` once rejected. Kept by the authority for every
    /// upload and by a replica for its own, so snapshots that name
    /// individual commits stay readable after they settle.
    fn record_settled(&self, node: [u8; 16], tx: u64, seq: Option<u64>) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_settled (node, tx, seq) VALUES (?1, ?2, ?3)",
            rusqlite::params![node.to_vec(), tx as i64, seq.map(|s| s as i64)],
        )
        .map_err(storage)?;
        Ok(())
    }

    fn settled(&self, node: [u8; 16], tx: u64) -> Result<Option<Option<u64>>, Error> {
        match self.conn.query_row(
            "SELECT seq FROM _jazz_settled WHERE node = ?1 AND tx = ?2",
            rusqlite::params![node.to_vec(), tx as i64],
            |r| r.get::<_, Option<i64>>(0),
        ) {
            Ok(seq) => Ok(Some(seq.map(|s| s as u64))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// A serializable transaction's snapshot on this store: the authority's
    /// state through `snapshot` plus this replica's own commits that were
    /// unsettled at begin (`own`), wherever each is now: settled at a seq,
    /// still pending at its local seq, or rejected (then it never happened).
    pub(crate) fn serializable_cut(&self, snapshot: u64, own: &[u64]) -> Result<Cut, Error> {
        let mut cut = Cut::at(snapshot);
        let me = *self.identity.node.as_bytes();
        for &tx in own {
            match self.settled(me, tx)? {
                Some(Some(seq)) => cut.extra.push(seq),
                Some(None) => {}
                None => cut.extra.push(tx),
            }
        }
        Ok(cut)
    }

    /// The own commits a replica's serializable transaction began over:
    /// those still unsettled (none on the authority).
    pub(crate) fn unsettled_own(&self) -> Vec<u64> {
        self.client.as_ref().map_or_else(Vec::new, |client| {
            client.pending.iter().map(|p| p.tx).collect()
        })
    }

    /// The cut a snapshot reference reads: the authority's state through
    /// its global base, plus each commit it names individually (a
    /// replica's writes not yet settled when it was captured) wherever
    /// that commit is now: still pending here, or settled at a seq.
    pub(crate) fn resolve_snapshot(&self, snapshot: &SnapshotRef) -> Result<Cut, Error> {
        if snapshot.local_base.0 != 0 {
            return Err(unsupported(
                "snapshot references with a local base (captured before SQLite)",
            ));
        }
        // A replica reads snapshots from its own store: it must already hold
        // the authority state the reference was captured at.
        let horizon = self.client.as_ref().map_or(self.seq, |client| client.known);
        if snapshot.global_base.0 > horizon {
            return Err(unsupported(
                "reading a snapshot newer than this replica's synced state",
            ));
        }
        let mut cut = Cut::at(snapshot.global_base.0);
        let me = *self.identity.node.as_bytes();
        for dot in &snapshot.dots {
            let node = *dot.node.as_bytes();
            let tx = dot.time.0;
            match self.settled(node, tx)? {
                Some(Some(seq)) => {
                    cut.extra.push(seq);
                    continue;
                }
                // Rejected: it never happened.
                Some(None) => continue,
                None => {}
            }
            if node == me
                && let Some(client) = &self.client
            {
                if client.pending.iter().any(|p| p.tx == tx) {
                    cut.extra.push(tx);
                    continue;
                }
                if client.fates.get(&tx).is_some_and(Result::is_err) {
                    continue;
                }
            }
            return Err(unsupported(
                "reading a snapshot that names another replica's commits not yet settled here",
            ));
        }
        Ok(cut)
    }

    /// Reload a replica's sync state after a restart: pending commits are
    /// resent on the next connection (the server dedupes them).
    pub(crate) fn load_client(&mut self) -> Result<(), Error> {
        if self.meta("client")?.is_none() {
            return Ok(());
        }
        let mut client = Client::new(*self.identity.node.as_bytes());
        client.known = self.meta("known")?.unwrap_or(0);
        client.persisted_known = client.known;
        let mut statement = self
            .conn
            .prepare("SELECT tx, commit_msg, rows FROM _jazz_pending ORDER BY tx")
            .map_err(storage)?;
        let pending = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        for (tx, commit, rows) in pending {
            let commit: SyncMessage = postcard::from_bytes(&commit)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            let rows: Vec<(String, [u8; 16], Vec<u8>)> = postcard::from_bytes(&rows)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            let SyncMessage::Commit { ops, author, .. } = &commit else {
                continue;
            };
            let mut ops = ops
                .iter()
                .cloned()
                .map(from_wire)
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(author) = author.as_deref().map(AuthorSubject::from_canonical) {
                let author = author.map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
                ops.iter_mut().for_each(|op| op.attribute(author));
            }
            client.pending.push(Pending {
                tx,
                ops,
                rows: rows
                    .into_iter()
                    .map(|(t, r, k)| (t, RowUuid::from_bytes(r), k))
                    .collect(),
            });
            client.unsent.push(commit);
        }
        let mut statement = self
            .conn
            .prepare("SELECT t, id, k, image FROM _jazz_shadow")
            .map_err(storage)?;
        let shadow = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        for (table, id, key, image) in shadow {
            let image = image
                .map(|bytes| postcard::from_bytes(&bytes))
                .transpose()
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            client.shadow.insert(
                (
                    table,
                    RowUuid::from_bytes(id.try_into().unwrap_or([0; 16])),
                    key,
                ),
                image,
            );
        }
        self.client = Some(client);
        Ok(())
    }
}

impl Client {
    fn new(node: [u8; 16]) -> Self {
        Client {
            node,
            connection: 0,
            sent: Vec::new(),
            transport: None,
            hint: Rc::new(StdCell::new(Link {
                hint: RemoteLinkHint::NoServer,
                since: Instant::now(),
                reported: false,
                detached: false,
            })),
            next_id: 1,
            remote: HashMap::new(),
            remote_ids: HashMap::new(),
            outbox: Vec::new(),
            pending: Vec::new(),
            unsent: Vec::new(),
            fates: HashMap::new(),
            shadow: HashMap::new(),
            known: 0,
            persisted_known: 0,
            answers: HashMap::new(),
            advice: HashMap::new(),
            requested: BTreeSet::new(),
            wanted: BTreeSet::new(),
            pushed: HashMap::new(),
            unavailable: BTreeSet::new(),
            arrived: false,
        }
    }
}

// ---- client ---------------------------------------------------------------

impl Db {
    /// Connect this database to its server. Writes from now on upload, and
    /// subscriptions attach remotely.
    pub async fn connect_upstream(&self, transport: Box<dyn Transport>) -> Upstream {
        self.ensure_client();
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut().expect("client");
        client.transport = Some(transport);
        let mut link = client.hint.get();
        link.detached = false;
        if !link.reported {
            link.hint = RemoteLinkHint::Live;
        }
        client.hint.set(link);
        // A new server session knows nothing: attach every remote query
        // again and resend unsettled commits (the server dedupes by node
        // and transaction).
        let resubscribe: Vec<SyncMessage> = client
            .remote
            .values_mut()
            .map(|r| {
                r.resumed = r.through.is_some();
                SyncMessage::Subscribe {
                    id: r.id,
                    query: r.query.clone(),
                    binding: r.binding.clone(),
                    since: r.through,
                    include_deleted: r.include_deleted,
                }
            })
            .collect();
        let shadowed: Vec<(String, [u8; 16], Vec<u8>, u64)> = client
            .shadow
            .iter()
            .filter_map(|((table, row, key), image)| {
                image
                    .as_ref()
                    .map(|image| (table.clone(), *row.0.as_bytes(), key.clone(), image.seq))
            })
            .collect();
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::Subscribe { .. }
                    | SyncMessage::Hold { .. }
                    | SyncMessage::Schema { .. }
            )
        });
        let version = *inner.owner.jazz.version_id().as_bytes();
        let client = inner.client.as_mut().expect("client");
        client.outbox.insert(0, SyncMessage::Schema { version });
        // Every settled row the replica holds, whether or not a live query
        // still reads it: the new session watches them, so a later change
        // (a deletion, lost access) still reaches the cached copy.
        let mut rows = shadowed;
        match inner.held_versions() {
            Ok(held) => rows.extend(held),
            Err(_) => rows.clear(),
        }
        if !rows.is_empty() {
            // Held rows name tables as this store's schema does.
            let names = inner.owner.clone();
            let rows = rows
                .into_iter()
                .filter_map(|(table, id, key, seq)| {
                    Some((names.by_stored(&table)?.name.clone(), id, key, seq))
                })
                .collect();
            let client = inner.client.as_mut().expect("client");
            client.outbox.push(SyncMessage::Hold { rows });
        }
        let client = inner.client.as_mut().expect("client");
        client.outbox.extend(resubscribe);
        // Sent-but-unsettled commits go first, in their original order.
        let mut resend = client.sent.clone();
        resend.append(&mut client.unsent);
        client.unsent = resend;
        // A new session answers nothing the old one was asked.
        for slot in client.advice.values_mut() {
            slot.get_or_insert(PermissionAdvice::Unknown);
        }
        let asked = std::mem::take(&mut client.requested);
        client.wanted.extend(asked);
        client.connection += 1;
        Upstream(client.connection)
    }

    /// Drop the upstream: reads stop waiting, writes queue.
    pub fn detach_connection(&self, upstream: &Upstream) -> bool {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return false;
        };
        if client.connection != upstream.0 || client.transport.is_none() {
            return false;
        }
        client.transport = None;
        let mut link = client.hint.get();
        link.detached = true;
        client.hint.set(link);
        // Nothing settles at the server's tier while detached: a
        // subscription (or one opened meanwhile) is held until the next
        // upstream answers it again.
        for remote in client.remote.values() {
            remote.answered.set(false);
        }
        true
    }

    /// Become a replica: optimistic seqs from here on sit above every
    /// server seq.
    fn ensure_client(&self) {
        let mut inner = self.inner.borrow_mut();
        if inner.client.is_some() {
            return;
        }
        inner.seq = inner.seq.max(LOCAL_BASE);
        let node = *inner.identity.node.as_bytes();
        inner.client = Some(Client::new(node));
        inner
            .persist_meta("client", 1)
            .expect("persist replica flag");
    }

    pub fn set_remote_link_hint(&self, hint: RemoteLinkHint) {
        self.ensure_client();
        let inner = self.inner.borrow();
        let client = inner.client.as_ref().expect("client");
        let link = client.hint.get();
        client.hint.set(Link {
            hint,
            since: Instant::now(),
            reported: true,
            ..link
        });
    }

    pub(crate) fn is_client(&self) -> bool {
        self.inner.borrow().client.is_some()
    }

    /// Record a local commit for upload (client only). Inside an open
    /// transaction the ops join it.
    pub(crate) fn record_upload(&self, ops: Vec<StagedOp>, seq: u64, _mark: usize, kind: Kind) {
        let mut inner = self.inner.borrow_mut();
        if inner.client.is_none() {
            return;
        }
        if inner.tx.is_some() {
            inner.capture.extend(ops);
            return;
        }
        if ops.is_empty() || seq != inner.seq {
            return;
        }
        // The writer the authority stamps: an explicit identity, or the
        // system at this node (the authority stamps its own writes as its
        // node, so a trusted link names its own).
        let author = ops.iter().find_map(StagedOp::author).or_else(|| {
            (inner.identity.author == AuthorSubject::SYSTEM).then(|| inner.stamp_author())
        });
        let rows = inner.written_rows(&ops, seq).unwrap_or_default();
        let mut shadows = Vec::new();
        for (table, row, key) in &rows {
            let shadowed = inner
                .client
                .as_ref()
                .is_some_and(|c| c.shadow.contains_key(&(table.clone(), *row, key.clone())));
            if !shadowed {
                shadows.push((
                    (table.clone(), *row, key.clone()),
                    inner.settled_before(table, *row, key).ok().flatten(),
                ));
            }
        }
        let wire: Result<Vec<WireOp>, Error> = ops.iter().map(to_wire).collect();
        let client = inner.client.as_mut().expect("client");
        client.shadow.extend(shadows);
        match wire {
            Ok(wire) => {
                let kind = match kind {
                    Kind::Mergeable => WireKind::Mergeable,
                    Kind::Serializable {
                        snapshot,
                        reads,
                        own,
                    } => WireKind::Serializable {
                        snapshot,
                        reads,
                        own,
                    },
                };
                let commit = SyncMessage::Commit {
                    node: client.node,
                    tx: seq,
                    kind,
                    ops: wire,
                    author: author.map(|author| author.canonical().to_owned()),
                };
                client.unsent.push(commit.clone());
                client.pending.push(Pending {
                    tx: seq,
                    ops,
                    rows: rows.clone(),
                });
                inner
                    .persist_pending(seq, &commit, &rows)
                    .expect("persist pending commit");
                inner.persist_shadow().expect("persist shadow");
            }
            Err(error) => {
                client.fates.insert(seq, Err(error));
                client.pending.push(Pending {
                    tx: seq,
                    ops: Vec::new(),
                    rows,
                });
                drop(inner);
                let _ = self.settle_locally(seq);
            }
        }
    }

    /// A transaction that cannot upload is rejected at once.
    fn settle_locally(&self, tx: u64) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if let Some(client) = inner.client.as_mut() {
            client.pending.retain(|p| p.tx != tx);
        }
        drop(inner);
        self.rebase()
    }

    /// Remote reads ship the query's rows plus its supporting rows (single
    /// hop includes, array subqueries, junction traversals, gathers); the
    /// client evaluates locally. Other shapes that read further tables
    /// (uncorrelated traversals or ones through a referenced row, gathers
    /// filtered after the gather, multi-hop includes, aggregates) would see
    /// a partial replica, so they are refused explicitly.
    pub(crate) fn remote_shape(query: &Query) -> Result<(), Error> {
        let other_tables = !query.joins.iter().all(join_followable)
            || !query.reachable.iter().all(|via| gather_shaped(query, via))
            || !query.inherits.is_empty()
            || query
                .includes
                .iter()
                .any(|include| include.path.contains('.'))
            || query.aggregate.is_some();
        if other_tables {
            return Err(crate::values::unsupported(format!(
                "a synced read of `{}` that reads further tables (joins, multi-hop includes or aggregates)",
                query.table
            )));
        }
        Ok(())
    }

    /// Attach a remote query (refcounted per query and binding).
    pub(crate) fn attach_remote(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
    ) -> Option<Rc<StdCell<bool>>> {
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut()?;
        let key = remote_key(&prepared.query, &prepared.binding, include_deleted);
        if let Some(remote) = client.remote.get_mut(&key) {
            remote.refs += 1;
            return Some(remote.answered.clone());
        }
        let id = client.next_id;
        client.next_id += 1;
        let answered = Rc::new(StdCell::new(false));
        client.outbox.push(SyncMessage::Subscribe {
            id,
            query: prepared.query.clone(),
            binding: prepared.binding.clone(),
            since: None,
            include_deleted,
        });
        client.remote.insert(
            key.clone(),
            Remote {
                id,
                refs: 1,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                include_deleted,
                answered: answered.clone(),
                through: None,
                resumed: false,
            },
        );
        client.remote_ids.insert(id, key);
        Some(answered)
    }

    pub(crate) fn detach_remote(
        &self,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
    ) {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return;
        };
        let key = remote_key(query, binding, include_deleted);
        let Some(remote) = client.remote.get_mut(&key) else {
            return;
        };
        remote.refs -= 1;
        if remote.refs == 0 {
            let id = remote.id;
            client.remote.remove(&key);
            client.remote_ids.remove(&id);
            client.outbox.push(SyncMessage::Unsubscribe { id });
        }
    }

    pub(crate) fn link_hint(&self) -> Option<Rc<StdCell<Link>>> {
        self.inner.borrow().client.as_ref().map(|c| c.hint.clone())
    }

    /// Ask the server for one read and wait for its answer, unless the
    /// link cannot answer or `deadline` passes. Returns the server seq the
    /// answer is complete through, if it answered.
    pub(crate) async fn fetch(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        deadline: Option<Instant>,
    ) -> Option<u64> {
        let id = {
            let mut inner = self.inner.borrow_mut();
            let client = inner.client.as_mut()?;
            if !may_wait(client.hint.get()) {
                return None;
            }
            let id = client.next_id;
            client.next_id += 1;
            client.answers.insert(id, None);
            client.outbox.push(SyncMessage::Fetch {
                id,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                at,
                include_deleted,
            });
            id
        };
        poll_fn(|_| {
            let mut inner = self.inner.borrow_mut();
            let client = inner.client.as_mut().expect("client");
            if let Some(Some((through, _))) = client.answers.get(&id) {
                let through = *through;
                client.answers.remove(&id);
                return Poll::Ready(Some(through));
            }
            if !may_wait(client.hint.get()) || deadline.is_some_and(|d| Instant::now() >= d) {
                client.answers.remove(&id);
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await
    }

    /// Freeze everything this database has committed, its own unsettled
    /// writes included, as a persistable reference for snapshot reads and
    /// frozen branch bases. A replica whose link can answer first lets its
    /// pending writes settle, so the reference is one authority cut any
    /// replica can read; writes still pending after that (offline) are
    /// named individually and stay readable here and at the authority.
    pub async fn capture_snapshot(&self) -> Result<SnapshotRef, Error> {
        let node = self.inner.borrow().identity.node;
        if !self.is_client() {
            return Ok(SnapshotRef {
                owner: node,
                global_base: GlobalTime(self.inner.borrow().seq),
                local_base: TxTime(0),
                dots: Vec::new(),
            });
        }
        let captured: Vec<u64> = {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            client.pending.iter().map(|p| p.tx).collect()
        };
        poll_fn(|_| {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            let settling = client.transport.is_some()
                && may_wait(client.hint.get())
                && client.pending.iter().any(|p| captured.contains(&p.tx));
            if settling {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        let inner = self.inner.borrow();
        let client = inner.client.as_ref().expect("client");
        Ok(SnapshotRef {
            owner: node,
            global_base: GlobalTime(client.known),
            local_base: TxTime(0),
            dots: client
                .pending
                .iter()
                .filter(|p| captured.contains(&p.tx))
                .map(|p| TxId {
                    time: TxTime(p.tx),
                    node,
                })
                .collect(),
        })
    }

    /// Wait for a local transaction's fate at `tier`.
    pub async fn wait_for_transaction(&self, tx: u64, tier: DurabilityTier) -> Result<u64, Error> {
        if tier < DurabilityTier::Global || !self.is_client() {
            return Ok(tx);
        }
        poll_fn(|_| {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            match client.fates.get(&tx) {
                Some(Ok(())) => Poll::Ready(Ok(tx)),
                Some(Err(error)) => Poll::Ready(Err(error.clone())),
                None if client.pending.iter().any(|p| p.tx == tx) => Poll::Pending,
                // Not pending and no fate: a local no-op or pre-connection commit.
                None => Poll::Ready(Ok(tx)),
            }
        })
        .await
    }

    /// Wait until every transaction pending upload when called (recovered
    /// ones included) reaches `tier`, failing with the first the server
    /// rejected. Writes made after the call are not waited for.
    pub async fn wait_for_pending_writes(&self, tier: DurabilityTier) -> Result<(), Error> {
        if tier < DurabilityTier::Global || !self.is_client() {
            return Ok(());
        }
        let pending: Vec<u64> = {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            client.pending.iter().map(|p| p.tx).collect()
        };
        for tx in pending {
            self.wait_for_transaction(tx, tier).await?;
        }
        Ok(())
    }

    /// Ask the upstream authority whether `action` would be allowed for
    /// this replica's session; wait with [`Db::permission_advice`]. Without
    /// a connected upstream the answer is `Unknown`: a replica's own policy
    /// evidence may be incomplete, so it never decides locally.
    pub fn request_permission_advice(&self, action: PermissionAdviceAction) -> u64 {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return 0;
        };
        let id = client.next_id;
        client.next_id += 1;
        if client.transport.is_some() && may_wait(client.hint.get()) {
            client.advice.insert(id, None);
            client.outbox.push(SyncMessage::Advise { id, action });
        } else {
            client.advice.insert(id, Some(PermissionAdvice::Unknown));
        }
        id
    }

    /// The answer to a [`Db::request_permission_advice`]; `Unknown` if the
    /// link fails or the request was cancelled first.
    pub async fn permission_advice(&self, id: u64) -> PermissionAdvice {
        poll_fn(|_| {
            let mut inner = self.inner.borrow_mut();
            let Some(client) = inner.client.as_mut() else {
                return Poll::Ready(PermissionAdvice::Unknown);
            };
            match client.advice.get(&id) {
                None => Poll::Ready(PermissionAdvice::Unknown),
                Some(Some(advice)) => {
                    let advice = *advice;
                    client.advice.remove(&id);
                    Poll::Ready(advice)
                }
                Some(None) if client.transport.is_none() || !may_wait(client.hint.get()) => {
                    client.advice.remove(&id);
                    Poll::Ready(PermissionAdvice::Unknown)
                }
                Some(None) => Poll::Pending,
            }
        })
        .await
    }

    /// Resolve an outstanding request as `Unknown`; its answer is ignored.
    pub fn cancel_permission_advice_request(&self, id: u64) {
        let mut inner = self.inner.borrow_mut();
        if let Some(slot) = inner.client.as_mut().and_then(|c| c.advice.get_mut(&id)) {
            slot.get_or_insert(PermissionAdvice::Unknown);
        }
    }

    /// Dry-run `action` as `identity` (`None`: a trusted link) and roll it
    /// back. A server that is itself a replica answers `Unknown`.
    fn advise(
        &self,
        identity: Option<AuthorSubject>,
        action: PermissionAdviceAction,
    ) -> PermissionAdvice {
        if self.is_client() {
            return PermissionAdvice::Unknown;
        }
        let who = match identity {
            Some(author) => Who::Session(author),
            None => Who::Local,
        };
        let op = match action {
            PermissionAdviceAction::Read { table, row } => {
                let inner = self.inner.borrow();
                let id = row.0.as_bytes().to_vec();
                let images = match inner.images_where(&table, false, "id = ?1", &[&id]) {
                    Ok(images) => images,
                    Err(_) => return PermissionAdvice::Unknown,
                };
                let Some(image) = images.into_iter().find(|image| !image.deleted) else {
                    return PermissionAdvice::Denied;
                };
                return match readable(&inner, &table, row, &image.key, identity) {
                    Ok(true) => PermissionAdvice::Allowed,
                    Ok(false) => PermissionAdvice::Denied,
                    Err(_) => PermissionAdvice::Unknown,
                };
            }
            PermissionAdviceAction::Insert { table, cells } => {
                let owner = self.inner.borrow().owner.clone();
                // The candidate has no id yet: a policy that looks at it
                // cannot be decided.
                match owner.table(&table) {
                    Ok(schema) => {
                        if schema
                            .policy(crate::tools::Operation::Insert)
                            .is_some_and(depends_on_row_id)
                        {
                            return PermissionAdvice::Unknown;
                        }
                    }
                    Err(_) => return PermissionAdvice::Unknown,
                }
                StagedOp::Insert(
                    table,
                    cells,
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes(*uuid::Uuid::new_v4().as_bytes())),
                        ..Default::default()
                    },
                )
            }
            PermissionAdviceAction::Update { table, row, patch } => {
                StagedOp::Update(table, row, patch, Default::default())
            }
            PermissionAdviceAction::Delete { table, row } => {
                StagedOp::Delete(table, row, Default::default())
            }
        };
        let mut inner = self.inner();
        let seq = inner.seq + 1;
        if inner.conn.execute_batch("SAVEPOINT advise").is_err() {
            return PermissionAdvice::Unknown;
        }
        let mark = inner.changed.borrow().len();
        let applied = inner.apply_as(&op, who, seq).map(drop);
        let undone = inner
            .conn
            .execute_batch("ROLLBACK TO advise; RELEASE advise");
        inner.changed.borrow_mut().truncate(mark);
        match (applied, undone) {
            (_, Err(_)) => PermissionAdvice::Unknown,
            (Ok(()), Ok(())) => PermissionAdvice::Allowed,
            (Err(error), Ok(())) if error.code == ErrorCode::WriteRejected => {
                PermissionAdvice::Denied
            }
            (Err(_), Ok(())) => PermissionAdvice::Unknown,
        }
    }

    /// Client half of a tick: apply what the server sent, rebase if a
    /// pending transaction settled or a shadowed row changed, then send.
    fn client_tick(&self) -> Result<(), Error> {
        if self.inner.borrow().client.is_none() {
            return Ok(());
        }
        // Everything one tick applies commits together.
        self.inner
            .borrow()
            .conn
            .execute_batch("SAVEPOINT sync_tick")
            .map_err(storage)?;
        let applied = self.client_apply();
        let inner = self.inner.borrow();
        if applied.is_ok() {
            inner
                .conn
                .execute_batch("RELEASE sync_tick")
                .map_err(storage)?;
        } else {
            inner
                .conn
                .execute_batch("ROLLBACK TO sync_tick; RELEASE sync_tick")
                .map_err(storage)?;
        }
        drop(inner);
        applied?;
        self.notify();
        let arrived = {
            let mut inner = self.inner.borrow_mut();
            let client = inner.client.as_mut().expect("client");
            std::mem::take(&mut client.arrived)
        };
        if arrived {
            self.inner.borrow().retry_awaiting();
        }
        self.flush_settled();
        self.client_send()
    }

    fn client_apply(&self) -> Result<(), Error> {
        let mut rebase = false;
        let mut answered_remotes = Vec::new();
        let mut shadow_changed = false;
        {
            let mut inner = self.inner.borrow_mut();
            let Some(client) = inner.client.as_mut() else {
                return Ok(());
            };
            let mut inbound = Vec::new();
            if let Some(transport) = client.transport.as_mut() {
                while let Some(message) = transport.try_recv() {
                    inbound.push(message);
                }
            }
            // Chunks are not ordered with rows; everything else is applied
            // in arrival order.
            let mut queue = VecDeque::new();
            for message in inbound {
                match message {
                    SyncMessage::Chunks { nodes, missing } => {
                        inner.receive_chunks(nodes, missing)?;
                    }
                    other => queue.push_back(other),
                }
            }
            // Rows install without their chunks: a chunked cell carries the
            // sort key its indexes need.
            while let Some(message) = queue.pop_front() {
                match message {
                    SyncMessage::Rows { images } => {
                        let names = inner.owner.clone();
                        for image in images {
                            let Some(image) = image_in(&names, image) else {
                                continue;
                            };
                            let shadowed = inner.receive_image(image)?;
                            rebase |= shadowed;
                            shadow_changed |= shadowed;
                        }
                    }
                    SyncMessage::Evict { table, id, key } => {
                        let Ok(table) = inner.owner.stored_table(&table).map(str::to_owned) else {
                            continue;
                        };
                        let row = RowUuid::from_bytes(id);
                        let client = inner.client.as_mut().expect("client");
                        if let Some(shadow) =
                            client.shadow.get_mut(&(table.clone(), row, key.clone()))
                        {
                            *shadow = None;
                            rebase = true;
                        } else {
                            inner.evict(&table, row, &key)?;
                        }
                    }
                    SyncMessage::Settled {
                        id,
                        through,
                        window,
                    } => {
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        let Some(key) = client.remote_ids.get(&id).cloned() else {
                            continue;
                        };
                        let remote = client.remote.get_mut(&key).expect("remote");
                        remote.answered.set(true);
                        remote.through = Some(through);
                        let resumed = std::mem::take(&mut remote.resumed);
                        // Cached rows the server's result lacks are stale;
                        // after a resume, result rows the replica lacks are fetched.
                        if let Some(ids) = window {
                            answered_remotes.push((
                                remote.query.clone(),
                                remote.binding.clone(),
                                Some(ids),
                                resumed,
                            ));
                        }
                    }
                    SyncMessage::Origins { commits } => {
                        for (seq, node, tx) in commits {
                            inner.record_settled(node, tx, Some(seq))?;
                        }
                    }
                    SyncMessage::Advice { id, advice } => {
                        let client = inner.client.as_mut().expect("client");
                        // Late answers to cancelled requests are dropped.
                        if let Some(slot @ None) = client.advice.get_mut(&id) {
                            *slot = Some(advice);
                        }
                    }
                    SyncMessage::Answer { id, through, rows } => {
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        if let Some(slot) = client.answers.get_mut(&id) {
                            *slot = Some((through, rows));
                        }
                    }
                    SyncMessage::Fate {
                        tx,
                        through,
                        result,
                    } => {
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        client.pending.retain(|p| p.tx != tx);
                        client.sent.retain(
                            |m| !matches!(m, SyncMessage::Commit { tx: sent, .. } if *sent == tx),
                        );
                        let accepted = result.is_ok();
                        client.fates.insert(tx, result.map_err(from_wire_error));
                        let node = client.node;
                        if let Some(locators) = client.pushed.remove(&tx) {
                            crate::large::mark_pushed(&inner.conn, &locators)?;
                        }
                        inner.record_settled(node, tx, accepted.then_some(through))?;
                        inner.forget_pending(tx)?;
                        rebase = true;
                    }
                    other => return Err(unsupported(format!("client received {other:?}"))),
                }
            }
        }
        if rebase {
            self.rebase()?;
        } else if shadow_changed {
            self.inner.borrow().persist_shadow()?;
        }
        // Nothing to reconcile after a resume: the client declared what it
        // holds (`Hold`), so the server ships what is missing or stale.
        drop(answered_remotes);
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut().expect("client");
        let known = client.known;
        if known == client.persisted_known {
            return Ok(());
        }
        client.persisted_known = known;
        inner.persist_meta("known", known)
    }

    fn client_send(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let demand = inner.chunks.take_demand();
        let client = inner.client.as_mut().expect("client");
        client.wanted.extend(demand);
        if client.transport.is_none() {
            return Ok(());
        }
        let mut out = std::mem::take(&mut client.outbox);
        let wanted: Vec<ChunkRequest> = std::mem::take(&mut client.wanted)
            .into_iter()
            .filter(|r| !client.requested.contains(r) && !client.unavailable.contains(r))
            .collect();
        for batch in wanted.chunks(256) {
            client.requested.extend(batch.iter().cloned());
            out.push(SyncMessage::ChunkRequests {
                requests: batch.iter().map(WireChunkRef::of).collect(),
            });
        }
        let commits = std::mem::take(&mut client.unsent);
        // Each commit's new large-value nodes go ahead of it.
        let mut pushes = Vec::with_capacity(commits.len());
        for commit in &commits {
            pushes.push(inner.pushes_for(commit)?);
        }
        let client = inner.client.as_mut().expect("client");
        let transport = client.transport.as_mut().expect("transport");
        for commit in &commits {
            if !client.sent.iter().any(|m| matches!((m, commit), (SyncMessage::Commit { tx: a, .. }, SyncMessage::Commit { tx: b, .. }) if a == b)) {
                client.sent.push(commit.clone());
            }
        }
        for (commit, (locators, batches)) in commits.into_iter().zip(pushes) {
            if let SyncMessage::Commit { tx, .. } = &commit
                && !locators.is_empty()
            {
                client.pushed.insert(*tx, locators);
            }
            out.extend(batches);
            out.push(commit);
        }
        for message in out {
            transport
                .send(message)
                .map_err(|e| Error::new(ErrorCode::Storage, format!("{e:?}")))?;
        }
        Ok(())
    }

    /// Push a settled marker to subscriptions whose remote answered without
    /// changing their rows.
    fn flush_settled(&self) {
        self.inner.borrow_mut().fan.flush_settled();
    }

    /// Restore every shadowed row to its settled image and replay the
    /// pending transactions, in order, over them.
    fn rebase(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return Ok(());
        };
        let shadow = std::mem::take(&mut client.shadow);
        let pending: Vec<(u64, Vec<StagedOp>)> = client
            .pending
            .iter()
            .map(|p| (p.tx, p.ops.clone()))
            .collect();
        let keep: BTreeSet<(String, RowUuid, Vec<u8>)> = client
            .pending
            .iter()
            .flat_map(|p| p.rows.iter().cloned())
            .collect();
        for ((table, row, key), image) in &shadow {
            match image {
                Some(image) => {
                    inner.drop_row(table, *row, key, image.seq)?;
                    inner.put_image(image, false)?;
                }
                None => inner.drop_row(table, *row, key, 0)?,
            }
        }
        for (tx, ops) in pending {
            inner
                .conn
                .execute_batch("SAVEPOINT replay")
                .map_err(storage)?;
            let replayed = ops.iter().try_for_each(|op| inner.apply(op, tx).map(drop));
            let end = if replayed.is_ok() {
                "RELEASE replay"
            } else {
                "ROLLBACK TO replay; RELEASE replay"
            };
            inner.conn.execute_batch(end).map_err(storage)?;
        }
        let client = inner.client.as_mut().expect("client");
        client.shadow = shadow
            .into_iter()
            .filter(|(row, _)| keep.contains(row))
            .collect();
        inner.persist_shadow()
    }
}

impl Inner {
    /// The schema session `index` reads and writes by, when it declared an
    /// admitted version other than the current one: that version's names,
    /// governed by the current schema's policies. `None` is the current
    /// schema (also for a version this store has not admitted).
    pub(crate) fn session_schema(&mut self, index: usize) -> Option<Rc<Schema>> {
        let version = self.sessions[index].version?;
        if version == self.owner.jazz.version_id() {
            return None;
        }
        if let Some((owner, schema)) = &self.sessions[index].schema
            && Rc::ptr_eq(owner, &self.owner)
        {
            return Some(schema.clone());
        }
        let jazz = self
            .admitted
            .iter()
            .find(|admitted| admitted.version_id() == version)?;
        let schema = Rc::new(self.named(jazz).with_policies_of(self.owner.clone()));
        self.sessions[index].schema = Some((self.owner.clone(), schema.clone()));
        Some(schema)
    }

    /// Apply one server image; returns whether a rebase is needed (the row
    /// is shadowed by a pending write).
    fn receive_image(&mut self, image: Image) -> Result<bool, Error> {
        let row = RowUuid::from_bytes(image.id);
        let client = self.client.as_mut().expect("client");
        if let Some(shadow) = client
            .shadow
            .get_mut(&(image.table.clone(), row, image.key.clone()))
        {
            match shadow {
                Some(old) if old.seq >= image.seq => {
                    self.put_image(&image, true)?;
                    return Ok(false);
                }
                // The replaced server version stays as history.
                Some(old) => {
                    let older = std::mem::replace(old, image);
                    self.put_image(&older, true)?;
                }
                None => *shadow = Some(image),
            }
            return Ok(true);
        }
        self.merge_image(&image)?;
        Ok(false)
    }
}

// ---- server ---------------------------------------------------------------

impl Db {
    /// Serve a client over `transport` as `identity`.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
    ) -> SubscriberId {
        let mut inner = self.inner.borrow_mut();
        let session = Session::new(transport, identity);
        if let Some(index) = inner.sessions.iter().position(|session| !session.open) {
            inner.sessions[index] = session;
            return SubscriberId(index);
        }
        inner.sessions.push(session);
        SubscriberId(inner.sessions.len() - 1)
    }

    /// The client went away: drop its subscriptions, watches and pending
    /// replies now. A reconnect is a new subscriber that resumes from its
    /// last settled seq.
    pub fn disconnect_subscriber(&self, subscriber: SubscriberId) {
        let mut inner = self.inner.borrow_mut();
        let dropped = inner.fan.dropped.clone();
        if let Some(session) = inner.sessions.get_mut(subscriber.0) {
            session.close(&dropped);
        }
    }

    /// Sessions currently served (for tests and metrics).
    pub fn open_subscribers(&self) -> usize {
        self.inner
            .borrow()
            .sessions
            .iter()
            .filter(|session| session.open)
            .count()
    }

    /// Seed a row as settled server state.
    pub fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: &str,
        row: RowUuid,
        _made_by: AuthorSubject,
        cells: BTreeMap<String, Value>,
    ) -> Result<u64, Error> {
        let handle = block_on(self.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))?;
        Ok(handle.seq)
    }

    /// Server half of a tick: take every session's messages (commits apply
    /// in arrival order), let fan-out run, then send each session the
    /// images its subscriptions and commits touched, as of now.
    fn server_tick(&self) -> Result<(), Error> {
        let sessions = self.inner.borrow().sessions.len();
        if sessions == 0 {
            return Ok(());
        }
        crate::large::expire_receipts(&self.inner.borrow().conn, now_ms())?;
        for index in 0..sessions {
            let inbound: Vec<SyncMessage> = {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                if !session.open {
                    continue;
                }
                let mut inbound = std::mem::take(&mut session.backlog);
                inbound.extend(std::iter::from_fn(|| session.transport.try_recv()));
                inbound.into()
            };
            let mut inbound: VecDeque<SyncMessage> = inbound.into();
            while let Some(message) = inbound.pop_front() {
                if let SyncMessage::PushChunks { nodes } = &message {
                    let bytes: usize = nodes.iter().map(|node| node.bytes.len()).sum();
                    let mut inner = self.inner.borrow_mut();
                    let policy = inner.staging;
                    let session = &mut inner.sessions[index];
                    if session.ingress.0.elapsed()
                        >= std::time::Duration::from_millis(policy.window_ms)
                    {
                        session.ingress = (Instant::now(), 0);
                    }
                    // Over the staging policy's budget: this push and
                    // everything after it wait for a later window, so its
                    // commit still follows it. A first push larger than the
                    // whole budget is let through rather than held forever.
                    if session.ingress.1 > 0
                        && (session.ingress.1 + bytes) as u64 > policy.incoming_bytes_per_window
                    {
                        inbound.push_front(message);
                        session.backlog = inbound;
                        break;
                    }
                    session.ingress.1 += bytes;
                }
                self.serve(index, message)?;
            }
        }
        self.notify();
        let mut inner = self.inner.borrow_mut();
        let through = inner.seq;
        let mut cache: HashMap<(String, RowUuid, u64), Vec<Image>> = HashMap::new();
        // Rows written since the last tick, with the watch keys they match.
        let written = std::mem::take(&mut inner.since_tick);
        let mut written_keys: Vec<((String, RowUuid, Vec<u8>), Vec<u64>)> = Vec::new();
        for (table, row, key) in written.into_iter().collect::<BTreeSet<_>>() {
            let mut keys = vec![wk((
                table.clone(),
                "id".to_owned(),
                watch_key(&Sql::Blob(row.0.as_bytes().to_vec())),
            ))];
            if let Some(image) = inner.current_image(&table, row, &key)? {
                for (column, cell) in &image.cells {
                    keys.push(wk((
                        table.clone(),
                        column.clone(),
                        watch_key(&Sql::from(cell)),
                    )));
                }
            }
            written_keys.push(((table, row, key), keys));
        }
        let written_tables: BTreeSet<&str> = written_keys
            .iter()
            .map(|((table, ..), _)| table.as_str())
            .collect();
        let mut policy_tables: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut written_values: HashMap<(String, String), BTreeSet<String>> = HashMap::new();
        let dropped = inner.fan.dropped.clone();
        for index in 0..sessions {
            if !inner.sessions[index].open {
                continue;
            }
            // Held supporting rows whose readability may have changed: their
            // table's read policy reads a written table, or the row itself
            // was written.
            let mut recheck: Vec<(String, RowUuid, Vec<u8>)> = Vec::new();
            if !written_tables.is_empty() {
                let identity = inner.sessions[index].identity;
                let tables: Vec<String> = inner.sessions[index].held.keys().cloned().collect();
                for table in tables {
                    if !policy_tables.contains_key(&table) {
                        let deps = inner.read_policy_tables(&table, identity)?;
                        policy_tables.insert(table.clone(), deps);
                    }
                    let held = &inner.sessions[index].held[&table];
                    let deps: Vec<&String> = policy_tables[&table]
                        .iter()
                        .filter(|dep| written_tables.contains(dep.as_str()))
                        .collect();
                    if !deps.is_empty()
                        && inner.policy_touched(
                            &table,
                            identity,
                            &deps,
                            &written_keys,
                            &mut written_values,
                        )?
                    {
                        recheck.extend(
                            held.keys()
                                .map(|(row, key)| (table.clone(), *row, key.clone())),
                        );
                    } else if written_tables.contains(table.as_str()) {
                        for ((t, row, key), _) in &written_keys {
                            if *t == table && held.contains_key(&(*row, key.clone())) {
                                recheck.push((table.clone(), *row, key.clone()));
                            }
                        }
                    }
                }
            }
            if recheck.is_empty()
                && inner.sessions[index].idle()
                && !written_keys
                    .iter()
                    .any(|(_, keys)| keys.iter().any(|k| inner.sessions[index].watch.contains(k)))
            {
                continue;
            }
            let identity = inner.sessions[index].identity;
            let policy = (identity != AuthorSubject::SYSTEM).then_some(identity);
            let names = inner
                .session_schema(index)
                .unwrap_or_else(|| inner.owner.clone());
            let mut touched = std::mem::take(&mut inner.sessions[index].touched);
            let committed = std::mem::take(&mut inner.sessions[index].committed);
            let mut watch = std::mem::take(&mut inner.sessions[index].watch);
            let mut held_rows = std::mem::take(&mut inner.sessions[index].held);
            for (table, row, key) in recheck {
                let can = inner.current_image(&table, row, &key)?.is_some()
                    && readable(&inner, &table, row, &key, policy)?;
                let was = held_rows
                    .get_mut(&table)
                    .and_then(|rows| rows.get_mut(&(row, key.clone())));
                if let Some(was) = was
                    && *was != can
                {
                    *was = can;
                    touched.insert((table, row, key));
                }
            }
            for (row, keys) in &written_keys {
                if keys.iter().any(|k| watch.contains(k)) {
                    touched.insert(row.clone());
                }
            }
            let mut evictions = Vec::new();
            let mut settled = Vec::new();
            let subs: Vec<(u64, fanout::Queue, Weak<RefCell<fanout::Group>>, bool)> = inner
                .sessions[index]
                .subs
                .iter_mut()
                .map(|(id, (q, g, windowed, opened))| {
                    let list = *windowed || !*opened;
                    *opened = true;
                    (*id, q.clone(), g.clone(), list)
                })
                .collect();
            let mut images = Vec::new();
            // Seqs of shipped history versions: their origins go first.
            let mut history_seqs = BTreeSet::new();
            for (id, queue, group, list) in subs {
                let events: Vec<SubscriptionEvent> = queue.borrow_mut().drain(..).collect();
                if events.is_empty() {
                    continue;
                }
                let first = images.len();
                let since = inner.sessions[index].resume.remove(&id);
                let support = inner.sessions[index]
                    .support
                    .get(&id)
                    .cloned()
                    .filter(|support| !support.is_empty());
                // A flat join's joined sources, by stored table: their rows
                // ship with each occurrence.
                let joined_tables = match group.upgrade() {
                    Some(group) => group
                        .borrow()
                        .query()
                        .flat_join
                        .iter()
                        .flat_map(|flat| &flat.sources)
                        .map(|source| {
                            let table = &source.table;
                            names
                                .stored_table(history_source_table(table).unwrap_or(table))
                                .map(str::to_owned)
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                    None => Vec::new(),
                };
                // Rows a resumed client already holds: not shipped, but
                // their supporting rows are re-watched (and shipped if newer).
                let mut held = Vec::new();
                for event in events {
                    let SubscriptionEvent::Delta {
                        added,
                        updated,
                        removed,
                        ..
                    } = event
                    else {
                        continue;
                    };
                    for row in added.iter().chain(updated.iter()) {
                        for (table, joined) in joined_tables.iter().zip(row.joined_rows()) {
                            images.extend(inner.joined_images(table, joined, &mut history_seqs)?);
                        }
                        // A history row is a version of its source table.
                        let history = history_source_table(&row.table);
                        let table = names
                            .stored_table(history.unwrap_or(&row.table))?
                            .to_owned();
                        // Unchanged for a resumed client: a version it declared
                        // holding (`Hold` watched it and re-ships it if stale).
                        // A row's seq alone cannot say so: it may have entered
                        // the result while away, through a policy or filter.
                        let unchanged = since.is_some()
                            && history.is_none()
                            && watch.contains(&wk((
                                table.clone(),
                                "id".to_owned(),
                                watch_key(&Sql::Blob(row.id.0.as_bytes().to_vec())),
                            )));
                        if unchanged && support.is_none() {
                            continue;
                        }
                        let key = (row.table.to_string(), row.id, row.seq);
                        if history.is_some() {
                            if !cache.contains_key(&key) {
                                let mut found = Vec::new();
                                inner.history_versions(
                                    &table,
                                    [(row.id, row.seq)],
                                    &mut found,
                                    &mut history_seqs,
                                )?;
                                cache.insert(key.clone(), found);
                            } else {
                                history_seqs.insert(row.seq);
                            }
                        } else if !cache.contains_key(&key) {
                            let id = row.id.0.as_bytes().to_vec();
                            let seq = row.seq as i64;
                            let found =
                                inner.images_where(&table, false, ID_AT_SEQ, &[&id, &seq])?;
                            cache.insert(key.clone(), found);
                        }
                        if unchanged {
                            held.extend(cache[&key].iter().cloned());
                        } else {
                            images.extend(cache[&key].iter().cloned());
                        }
                    }
                    for gone in removed {
                        let id = gone.row_uuid.0.as_bytes().to_vec();
                        let table = names.stored_table(
                            history_source_table(&gone.table).unwrap_or(&gone.table),
                        )?;
                        for image in inner.images_where(table, false, "id = ?1", &[&id])? {
                            touched.insert((image.table.clone(), gone.row_uuid, image.key.clone()));
                        }
                        // A joined row may have left for its permissions.
                        for (table, joined) in joined_tables.iter().zip(&gone.joined) {
                            let id = joined.row.0.as_bytes().to_vec();
                            for image in inner.images_where(table, false, "id = ?1", &[&id])? {
                                touched.insert((
                                    image.table.clone(),
                                    joined.row,
                                    image.key.clone(),
                                ));
                            }
                        }
                    }
                }
                if let Some(support) = support {
                    let mut basis: Vec<Image> = images[first..].to_vec();
                    for image in &held {
                        watch.insert(wk((
                            image.table.clone(),
                            "id".to_owned(),
                            watch_key(&Sql::Blob(image.id.to_vec())),
                        )));
                    }
                    basis.extend(held);
                    let mut extra = Vec::new();
                    for image in &basis {
                        inner.follow_support(
                            &support,
                            image,
                            policy,
                            &mut extra,
                            &mut watch,
                            &mut held_rows,
                        )?;
                    }
                    images.extend(extra);
                }
                // The opening and every window delta list the result's
                // rows, so the client can drop stale cached ones.
                let window = list.then(|| group.upgrade()).flatten().map(|group| {
                    group
                        .borrow()
                        .rows
                        .iter()
                        .map(|row| *row.id.0.as_bytes())
                        .collect()
                });
                settled.push(SyncMessage::Settled {
                    id,
                    through,
                    window,
                });
            }
            // Rows that left a result, and rows a commit wrote: the current
            // image if the client may read it, else an eviction.
            for (table, row, key) in touched {
                match inner.current_image(&table, row, &key)? {
                    Some(image) if readable(&inner, &table, row, &key, policy)? => {
                        images.push(image)
                    }
                    _ => evictions.push(SyncMessage::Evict {
                        table,
                        id: *row.0.as_bytes(),
                        key,
                    }),
                }
            }
            // Superseded versions of the client's own commits land in its
            // history, while the row stays readable.
            for (table, row, key, seq) in committed {
                let superseded = inner
                    .current_image(&table, row, &key)?
                    .is_some_and(|current| current.seq != seq);
                if superseded && readable(&inner, &table, row, &key, policy)? {
                    let id = row.0.as_bytes().to_vec();
                    images.extend(inner.images_where(
                        &table,
                        true,
                        "id = ?1 AND _b = ?2 AND _seq = ?3",
                        &[&id, &key, &(seq as i64)],
                    )?);
                }
            }
            for image in &images {
                watch.insert(wk((
                    image.table.clone(),
                    "id".to_owned(),
                    watch_key(&Sql::Blob(image.id.to_vec())),
                )));
            }
            for eviction in &evictions {
                if let SyncMessage::Evict { table, id, .. } = eviction {
                    watch.remove(&wk((
                        table.clone(),
                        "id".to_owned(),
                        watch_key(&Sql::Blob(id.to_vec())),
                    )));
                }
            }
            let mut out = std::mem::take(&mut inner.sessions[index].stale);
            if !history_seqs.is_empty() {
                out.push(SyncMessage::Origins {
                    commits: inner.origins(&history_seqs)?,
                });
            }
            let session = &mut inner.sessions[index];
            session.watch = watch;
            session.held = held_rows;
            let mut seen = HashSet::new();
            images.retain(|image| {
                seen.insert((image.table.clone(), image.id, image.key.clone(), image.seq))
            });
            if !images.is_empty() {
                out.push(SyncMessage::Rows { images });
            }
            out.extend(evictions);
            out.extend(std::mem::take(&mut session.outbox));
            out.extend(settled);
            for message in out {
                let Some(message) = message_out(&names, message) else {
                    continue;
                };
                // A failed or backed-up client is dropped, never waited
                // on: one slow client must not stall the others, and its
                // replica is coherent only with every message. It
                // reconnects and resumes from its last settled seq.
                if session.transport.send(message).is_err() {
                    session.close(&dropped);
                    break;
                }
            }
        }
        Ok(())
    }

    /// A handle that reads and writes through session `index`'s schema.
    fn session_db(&self, index: usize) -> Db {
        let view = self.inner.borrow_mut().session_schema(index);
        Db {
            inner: self.inner.clone(),
            view,
        }
    }

    fn serve(&self, index: usize, message: SyncMessage) -> Result<(), Error> {
        let identity = self.inner.borrow().sessions[index].identity;
        let policy = (identity != AuthorSubject::SYSTEM).then_some(identity);
        // Queries, writes and held rows name tables and columns as the
        // client's schema version does.
        let db = self.session_db(index);
        match message {
            SyncMessage::Schema { version } => {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                session.version = Some(crate::ids::SchemaVersionId::from_bytes(version));
                session.schema = None;
            }
            SyncMessage::Subscribe {
                id,
                query,
                binding,
                since,
                include_deleted,
            } => {
                let prepared = db.prepare_query_bound(&query, binding)?;
                let windowed = query.limit.is_some() || query.offset > 0;
                let mut inner = db.inner();
                let support = Rc::new(inner.support_of(&query)?);
                let (queue, group) = inner.subscribe_member(
                    &prepared,
                    policy,
                    ReadSpec {
                        include_deleted,
                        ..ReadSpec::default()
                    },
                    SettledBy::Always,
                )?;
                inner.sessions[index]
                    .subs
                    .insert(id, (queue, group, windowed, false));
                inner.sessions[index].support.insert(id, support);
                if let Some(since) = since {
                    inner.sessions[index].resume.insert(id, since);
                }
            }
            SyncMessage::Hold { rows } => {
                let mut inner = db.inner();
                let names = inner.schema.clone();
                let owner = inner.owner.clone();
                let mut watch = std::mem::take(&mut inner.sessions[index].watch);
                let mut touched = Vec::new();
                let mut evict = Vec::new();
                let mut by_table: BTreeMap<String, HashMap<([u8; 16], Vec<u8>), u64>> =
                    BTreeMap::new();
                for (table, id, key, seq) in rows {
                    by_table.entry(table).or_default().insert((id, key), seq);
                }
                for (name, declared) in by_table {
                    let Some(table) = names
                        .table(&name)
                        .ok()
                        .and_then(|table| owner.by_stored(&table.stored))
                    else {
                        continue;
                    };
                    let stored = table.stored.clone();
                    let ids: Vec<[u8; 16]> = declared
                        .keys()
                        .map(|(id, _)| *id)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    let mut current = HashMap::new();
                    for (id, key, seq, deleted, can) in
                        inner.versions_readable(table, policy, &ids)?
                    {
                        current.insert((id, key), (seq, deleted, can));
                    }
                    for ((id, key), seq) in declared {
                        // A deleted version the client already holds stays
                        // (includeDeleted reads list it); a deletion it
                        // missed is an eviction.
                        match current.get(&(id, key.clone())) {
                            Some(&(now, deleted, can)) if !deleted || (now == seq && can) => {
                                watch.insert(wk((
                                    stored.clone(),
                                    "id".to_owned(),
                                    watch_key(&Sql::Blob(id.to_vec())),
                                )));
                                if now != seq || !can {
                                    touched.push((stored.clone(), RowUuid::from_bytes(id), key));
                                }
                            }
                            _ => evict.push(SyncMessage::Evict {
                                table: stored.clone(),
                                id,
                                key,
                            }),
                        }
                    }
                }
                let session = &mut inner.sessions[index];
                session.watch = watch;
                session.touched.extend(touched);
                session.stale.extend(evict);
            }
            SyncMessage::Unsubscribe { id } => {
                let mut inner = self.inner.borrow_mut();
                inner.sessions[index].support.remove(&id);
                if inner.sessions[index].subs.remove(&id).is_some() {
                    let dropped = inner.fan.dropped.clone();
                    dropped.set(dropped.get() + 1);
                }
            }
            SyncMessage::Advise { id, action } => {
                let advice = self.advise(policy, action);
                self.inner.borrow_mut().sessions[index]
                    .outbox
                    .push(SyncMessage::Advice { id, advice });
            }
            SyncMessage::Fetch {
                id,
                query,
                binding,
                at,
                include_deleted,
            } => {
                let prepared = db.prepare_query_bound(&query, binding)?;
                let mut inner = db.inner();
                // A history read ships versions of its source table.
                let history = history_source_table(&query.table);
                let table = inner
                    .schema
                    .stored_table(history.unwrap_or(&query.table))?
                    .to_owned();
                let read = ReadSpec {
                    cut: at.map(Cut::at),
                    include_deleted,
                    ..ReadSpec::default()
                };
                let rows = inner.run(&prepared, policy, &read)?;
                let mut images = Vec::new();
                // Seqs of shipped history versions, for their origins.
                let mut seqs = BTreeSet::new();
                // A flat join ships each occurrence's joined rows.
                let joined_tables = query
                    .flat_join
                    .iter()
                    .flat_map(|flat| &flat.sources)
                    .map(|source| {
                        let table = &source.table;
                        inner
                            .schema
                            .stored_table(history_source_table(table).unwrap_or(table))
                            .map(str::to_owned)
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                for row in &rows {
                    for (table, joined) in joined_tables.iter().zip(row.joined_rows()) {
                        images.extend(inner.joined_images(table, joined, &mut seqs)?);
                    }
                }
                if history.is_some() {
                    let versions = rows.iter().map(|row| (row.id, row.seq));
                    inner.history_versions(&table, versions, &mut images, &mut seqs)?;
                } else {
                    for row in &rows {
                        let rid = row.id.0.as_bytes().to_vec();
                        let seq = row.seq as i64;
                        let mut found =
                            inner.images_where(&table, false, ID_AT_SEQ, &[&rid, &seq])?;
                        if found.is_empty() {
                            found = inner.images_where(&table, true, ID_AT_SEQ, &[&rid, &seq])?;
                        }
                        images.extend(found);
                    }
                }
                let support = inner.support_of(&query)?;
                let mut extra = Vec::new();
                let mut watch = std::mem::take(&mut inner.sessions[index].watch);
                let mut held = std::mem::take(&mut inner.sessions[index].held);
                for image in &images {
                    inner.follow_support(
                        &support, image, policy, &mut extra, &mut watch, &mut held,
                    )?;
                }
                images.extend(extra);
                for image in &images {
                    watch.insert(wk((
                        image.table.clone(),
                        "id".to_owned(),
                        watch_key(&Sql::Blob(image.id.to_vec())),
                    )));
                }
                let origins = inner.origins(&seqs)?;
                let through = at.unwrap_or(inner.seq);
                let session = &mut inner.sessions[index];
                session.watch = watch;
                session.held = held;
                if !origins.is_empty() {
                    session
                        .outbox
                        .push(SyncMessage::Origins { commits: origins });
                }
                if !images.is_empty() {
                    session.outbox.push(SyncMessage::Rows { images });
                }
                session.outbox.push(SyncMessage::Answer {
                    id,
                    through,
                    rows: rows.iter().map(|r| *r.id.0.as_bytes()).collect(),
                });
            }
            SyncMessage::Commit {
                node,
                tx,
                kind,
                ops,
                author,
            } => {
                // A resend after a reconnect gets the recorded fate.
                let known = self.inner.borrow().applied_fate(node, tx)?;
                let (result, rows) = match known {
                    Some(result) => (result, Vec::new()),
                    None => match commit_who(policy, author.as_deref())
                        .map_err(|error| (error, Vec::new()))
                        .and_then(|who| db.authorize_commit(who, policy, kind, ops, (node, tx)))
                    {
                        // The accepted commit recorded its fate atomically.
                        Ok(rows) => (Ok(()), rows),
                        Err((error, rows)) => {
                            let result = Err(wire_error(&error));
                            let inner = self.inner.borrow();
                            inner.record_fate(node, tx, &result)?;
                            inner.record_settled(node, tx, None)?;
                            (result, rows)
                        }
                    },
                };
                let mut inner = self.inner.borrow_mut();
                // An accepted commit's fate carries the seq it landed at (a
                // resend gets the recorded one), so the replica learns it.
                let through = match (&result, inner.settled(node, tx)?) {
                    (Ok(()), Some(Some(seq))) => seq,
                    _ => inner.seq,
                };
                let session = &mut inner.sessions[index];
                if result.is_ok() {
                    session.committed.extend(
                        rows.iter()
                            .map(|(table, row, key)| (table.clone(), *row, key.clone(), through)),
                    );
                }
                session.touched.extend(rows);
                session.outbox.push(SyncMessage::Fate {
                    tx,
                    through,
                    result,
                });
            }
            SyncMessage::ChunkRequests { requests } => {
                let mut inner = self.inner.borrow_mut();
                let mut replies = Vec::new();
                let mut nodes = Vec::new();
                let mut missing = Vec::new();
                let mut size = 0;
                for request in requests {
                    match crate::large::node(&inner.conn, &request.locator)? {
                        Some((hash, bytes)) if hash.as_slice() == request.hash => {
                            size += bytes.len();
                            nodes.push(WireChunk {
                                locator: request.locator,
                                hash: request.hash,
                                bytes,
                            });
                            if size >= CHUNK_BATCH_BYTES {
                                replies.push(SyncMessage::Chunks {
                                    nodes: std::mem::take(&mut nodes),
                                    missing: Vec::new(),
                                });
                                size = 0;
                            }
                        }
                        _ => missing.push(request),
                    }
                }
                replies.push(SyncMessage::Chunks { nodes, missing });
                inner.sessions[index].outbox.extend(replies);
            }
            SyncMessage::PushChunks { nodes } => {
                let inner = self.inner.borrow();
                let max_age = i64::try_from(inner.staging.max_age_ms).unwrap_or(i64::MAX);
                let expires = now_ms().saturating_add(max_age);
                inner
                    .conn
                    .execute_batch("SAVEPOINT jazz_lv_push")
                    .map_err(storage)?;
                for node in nodes {
                    // A node that fails authentication is dropped; a commit
                    // that needs it is then rejected for missing chunks.
                    let _ = crate::large::receive_pushed(
                        &inner.conn,
                        &inner.chunks,
                        &node.request(),
                        node.bytes,
                        expires,
                    );
                }
                inner
                    .conn
                    .execute_batch("RELEASE jazz_lv_push")
                    .map_err(storage)?;
            }
            other => return Err(unsupported(format!("server received {other:?}"))),
        }
        Ok(())
    }

    /// Validate (serializable) and apply one uploaded commit as `identity`.
    /// Returns the rows the client must hear about either way: the rows it
    /// wrote, so a rejected client rebases onto the authority's versions.
    #[allow(clippy::type_complexity)]
    fn authorize_commit(
        &self,
        who: Who,
        identity: Option<AuthorSubject>,
        kind: WireKind,
        ops: Vec<WireOp>,
        origin: ([u8; 16], u64),
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, (Error, Vec<(String, RowUuid, Vec<u8>)>)> {
        let ops: Vec<StagedOp> = match ops.into_iter().map(from_wire).collect::<Result<_, _>>() {
            Ok(ops) => ops,
            Err(error) => return Err((error, Vec::new())),
        };
        let mentioned = |inner: &Inner| -> Vec<(String, RowUuid, Vec<u8>)> {
            let mut out = Vec::new();
            for op in &ops {
                let (Some(row), Ok(table)) = (op.row(), inner.schema.stored_table(op.table()))
                else {
                    continue;
                };
                let id = row.0.as_bytes().to_vec();
                for image in inner
                    .images_where(table, false, "id = ?1", &[&id])
                    .unwrap_or_default()
                {
                    out.push((table.to_owned(), row, image.key));
                }
            }
            out
        };
        if let WireKind::Serializable {
            snapshot,
            reads,
            own,
        } = &kind
        {
            let inner = self.inner();
            // The sender's own commits its snapshot included, by local seq
            // and where each settled here (earlier on this connection, so
            // before this one).
            let mut settled = BTreeMap::new();
            for &tx in own {
                if let Some(seq) = inner
                    .settled(origin.0, tx)
                    .map_err(|e| (e, Vec::new()))?
                    .flatten()
                {
                    settled.insert(tx, seq);
                }
            }
            let in_snapshot = |seq: u64| seq <= *snapshot || settled.values().any(|s| *s == seq);
            for op in &ops {
                if let Some(row) = op.row()
                    && inner
                        .schema
                        .stored_table(op.table())
                        .and_then(|table| inner.newest_seq(table, row))
                        .map_err(|e| (e, Vec::new()))?
                        .is_some_and(|seq| !in_snapshot(seq))
                {
                    let rows = mentioned(&inner);
                    return Err((
                        conflict("row visible parent changed since transaction write was staged"),
                        rows,
                    ));
                }
            }
            for read in reads {
                let prepared = PreparedQuery {
                    query: read.query.clone(),
                    binding: read.binding.clone(),
                    shape: Shape([0; 16]),
                    binding_id: Binding([0; 16]),
                    request_claims: None,
                    schema: self.view.clone(),
                };
                let now = inner
                    .run(&prepared, identity, &ReadSpec::default())
                    .map_err(|e| (e, Vec::new()))?;
                let now: Vec<([u8; 16], u64)> =
                    now.iter().map(|r| (*r.id.0.as_bytes(), r.seq)).collect();
                // A row the sender read at an own commit's local seq is
                // compared where that commit settled.
                let then: Vec<([u8; 16], u64)> = read
                    .rows
                    .iter()
                    .map(|(row, seq)| (*row, settled.get(seq).copied().unwrap_or(*seq)))
                    .collect();
                if now != then {
                    let rows = mentioned(&inner);
                    return Err((
                        conflict("a row read by the transaction changed since its snapshot"),
                        rows,
                    ));
                }
            }
        }
        let seq = {
            let mut inner = self.inner();
            let seq = inner.seq + 1;
            inner
                .conn
                .execute_batch("SAVEPOINT upload")
                .map_err(|e| (storage(e), Vec::new()))?;
            let mark = inner.changed.borrow().len();
            let applied = ops
                .iter()
                .try_for_each(|op| inner.apply_as(op, who, seq).map(drop));
            if let Err(error) = applied {
                inner
                    .conn
                    .execute_batch("ROLLBACK TO upload; RELEASE upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.changed.borrow_mut().truncate(mark);
                let rows = mentioned(&inner);
                return Err((error, rows));
            }
            inner
                .record_fate(origin.0, origin.1, &Ok(()))
                .and_then(|()| inner.record_settled(origin.0, origin.1, Some(seq)))
                .map_err(|e| (e, Vec::new()))?;
            inner
                .conn
                .execute_batch("RELEASE upload")
                .map_err(|e| (storage(e), Vec::new()))?;
            inner.seq = seq;
            seq
        };
        self.notify();
        let inner = self.inner.borrow();
        Ok(inner.written_rows(&ops, seq).unwrap_or_default())
    }

    /// One tick for both roles.
    pub(crate) fn sync_tick(&self) -> Result<(), Error> {
        self.server_tick()?;
        self.client_tick()
    }
}

/// Who an uploaded commit is applied as. A session link writes under its
/// own identity and may not name another author; a trusted link (no policy
/// identity) writes unchecked, attributed to the author it names.
fn commit_who(policy: Option<AuthorSubject>, author: Option<&str>) -> Result<Who, Error> {
    let author = author
        .map(AuthorSubject::from_canonical)
        .transpose()
        .map_err(|error| Error::new(ErrorCode::WriteRejected, error.to_string()))?;
    match (policy, author) {
        // A node's own writes (it names itself as the system at that node)
        // over a session link are that session's: the link decides.
        (Some(session), Some(AuthorSubject::SystemAt(_))) => Ok(Who::Session(session)),
        (Some(session), Some(author)) if author != session => Err(Error::new(
            ErrorCode::WriteRejected,
            "attribution requires a trusted serving node",
        )),
        (Some(session), _) => Ok(Who::Session(session)),
        (None, Some(author)) if author != AuthorSubject::SYSTEM => Ok(Who::Attributed(author)),
        (None, _) => Ok(Who::Local),
    }
}

/// Whether a policy can look at the row's own id (subqueries count: they
/// may correlate on it).
fn depends_on_row_id(policy: &PolicyExpr) -> bool {
    match policy {
        PolicyExpr::Cmp { column, .. }
        | PolicyExpr::IsNull { column }
        | PolicyExpr::IsNotNull { column }
        | PolicyExpr::Contains { column, .. }
        | PolicyExpr::In { column, .. }
        | PolicyExpr::InList { column, .. } => column == "id",
        PolicyExpr::Inherits { via_column, .. } => via_column == "id",
        PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::Exists { .. }
        | PolicyExpr::ExistsRel { .. } => true,
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().any(depends_on_row_id),
        PolicyExpr::Not(item) => depends_on_row_id(item),
        PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::True
        | PolicyExpr::False => false,
    }
}

/// Whether `identity` may read a row of stored table `table`.
fn readable(
    inner: &Inner,
    table: &str,
    row: RowUuid,
    key: &[u8],
    identity: Option<AuthorSubject>,
) -> Result<bool, Error> {
    let Some(author) = identity else {
        return Ok(true);
    };
    let schema = inner.owner.clone();
    let table = owned_table(&schema, table)?;
    inner.check(table, row, key, author, Operation::Select)
}

/// The current schema's table stored as `stored`.
fn owned_table<'a>(schema: &'a Schema, stored: &str) -> Result<&'a Table, Error> {
    schema
        .by_stored(stored)
        .ok_or_else(|| query_error(format!("unknown table {stored}")))
}

// ---- names on the wire ------------------------------------------------------
//
// Stored images name stored tables and columns, which differ between nodes
// (each names a renamed table or column after where it first stored it). On
// the wire a row names its table and columns as the client's schema version
// does: the server maps its stored names to the session's, the client maps
// them back to its own. A column the receiving side's schema lacks is left
// out; provenance (`$...`) is never renamed.

/// An image named by `schema` instead of by stored names; `None` when the
/// schema has no such table.
fn image_out(schema: &Schema, mut image: Image) -> Option<Image> {
    let table = schema.by_stored(&image.table)?;
    image.table = table.name.clone();
    image.cells = std::mem::take(&mut image.cells)
        .into_iter()
        .filter_map(
            |(name, cell)| match table.columns.iter().find(|c| c.stored == name) {
                Some(column) => Some((column.name.clone(), cell)),
                None => name.starts_with('$').then_some((name, cell)),
            },
        )
        .collect();
    Some(image)
}

/// An image named by `schema`, by this store's stored names instead.
fn image_in(schema: &Schema, mut image: Image) -> Option<Image> {
    let table = schema.tables.get(&image.table)?;
    image.table = table.stored.clone();
    image.cells = std::mem::take(&mut image.cells)
        .into_iter()
        .filter_map(
            |(name, cell)| match table.columns.iter().find(|c| c.name == name) {
                Some(column) => Some((column.stored.clone(), cell)),
                None => name.starts_with('$').then_some((name, cell)),
            },
        )
        .collect();
    Some(image)
}

/// A server message as the session's schema names rows.
fn message_out(schema: &Schema, message: SyncMessage) -> Option<SyncMessage> {
    Some(match message {
        SyncMessage::Rows { images } => SyncMessage::Rows {
            images: images
                .into_iter()
                .filter_map(|image| image_out(schema, image))
                .collect(),
        },
        SyncMessage::Evict { table, id, key } => SyncMessage::Evict {
            table: schema.by_stored(&table)?.name.clone(),
            id,
            key,
        },
        other => other,
    })
}

// ---- reads through the binding entry point -------------------------------

/// What a serialized one-shot read returns.
#[derive(Debug)]
pub enum SerializedReadResult {
    Rows(Vec<CurrentRow>),
    /// Never produced here: relation reads return rows.
    Relation(RelationSnapshot),
}

/// A relation read's rows; roots first.
#[derive(Debug)]
pub struct RelationSnapshot {
    pub rows: Vec<CurrentRow>,
    pub root_count: usize,
}

/// Coverage held by a one-shot read; nothing to release here.
#[derive(Debug)]
pub struct QueryAttachment;

impl Db {
    pub fn detach_query(&self, _attachment: QueryAttachment) {}

    /// The bindings' one-shot read: a postcard query, optionally inside an
    /// open transaction. On a client a Remote read, or a local-first read
    /// that waits for the server, fetches first and then reads locally.
    #[allow(clippy::too_many_arguments)]
    pub async fn all_serialized_query<F, E>(
        &self,
        query: &[u8],
        opts: ReadOpts,
        open_tx: Option<OpenTransactionId>,
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        _author: Option<AuthorSubject>,
        _require_coverage: bool,
        _coverage_expired: E,
        _release_coverage: F,
    ) -> Result<SerializedReadResult, Error>
    where
        F: Fn(QueryAttachment),
        E: Fn() -> bool,
    {
        let query: Query = postcard::from_bytes(query)
            .map_err(|e| Error::new(ErrorCode::Query, format!("undecodable query: {e}")))?;
        let mut prepared = self.prepare_query(&query)?;
        if let Some((author, claims)) = request_scope {
            prepared = prepared.with_identity_claims(author, claims);
        }
        // Relations arrive as nested rows in their parent's cells, so a
        // relation read's snapshot is its root rows alone.
        let relation = !prepared.query.array_subqueries.is_empty();
        let result = |rows: Vec<CurrentRow>| {
            if relation {
                SerializedReadResult::Relation(RelationSnapshot {
                    root_count: rows.len(),
                    rows,
                })
            } else {
                SerializedReadResult::Rows(rows)
            }
        };
        if let Some(tx) = open_tx {
            let roots = self.serializable_tx_ref(tx).all_prepared(&prepared).await?;
            return Ok(result(self.nest_relations(&prepared, roots)?));
        }
        let frozen = ReadSpec::of(&opts, &self.inner())?.frozen();
        if self.is_client() && opts.tier != ReadTier::LocalOnly && !frozen {
            let wait = match (opts.tier, &opts.first_load) {
                (ReadTier::Remote, _) => Some(None),
                (_, FirstLoad::WaitForRemote { timeout_ms }) if *timeout_ms > 0 => Some(Some(
                    Instant::now() + std::time::Duration::from_millis(*timeout_ms),
                )),
                _ => None,
            };
            if let Some(deadline) = wait {
                Self::remote_shape(&prepared.query)?;
                self.fetch(&prepared, None, opts.read_view.include_deleted, deadline)
                    .await;
            }
        }
        let roots = self
            .all(
                &prepared,
                ReadOpts {
                    first_load: FirstLoad::Deliver,
                    ..opts
                },
            )
            .await?;
        Ok(result(self.nest_relations(&prepared, roots)?))
    }
}

impl Db {
    /// A non-durable foreground relaying through a durable owner is not
    /// built: the relay tier (a Db that is both client and server for the
    /// same rows, gating on the authority's answer) is designed only.
    pub fn set_non_durable_client(&self) {
        unimplemented!("non-durable foregrounds over a relay are not supported on SQLite yet")
    }

    pub fn query_coverage_attachment_counts_for_test(&self) -> (usize, usize) {
        unimplemented!("coverage attachments are not modelled on SQLite")
    }

    pub fn set_tick_scheduler(&self, _scheduler: Option<Rc<dyn TickScheduler>>) {
        unimplemented!("host tick schedulers are not supported on SQLite yet")
    }
}

impl Db {
    /// Test diagnostics: pending txs, shadowed rows, fates.
    pub fn sync_debug(&self) -> String {
        let inner = self.inner.borrow();
        let Some(client) = &inner.client else {
            return "not a client".to_owned();
        };
        format!(
            "pending {:?} sent {} unsent {} shadow {:?} fates {:?} connected {}",
            client
                .pending
                .iter()
                .map(|p| p.tx - LOCAL_BASE)
                .collect::<Vec<_>>(),
            client.sent.len(),
            client.unsent.len(),
            client
                .shadow
                .keys()
                .map(|(t, r, _)| (t.clone(), *r))
                .collect::<Vec<_>>(),
            client
                .fates
                .iter()
                .map(|(tx, r)| (tx - LOCAL_BASE, r.is_ok()))
                .collect::<BTreeMap<_, _>>(),
            client.transport.is_some(),
        )
    }
}
