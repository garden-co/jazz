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
//!   client may still read it. Otherwise a write that took the client's
//!   access unlists it: synced subscriptions and `ReadTier::Remote` reads
//!   leave it out, but the copy already delivered stays readable locally
//!   (INV-SYNC-14) until the server ships the row again. A read-policy
//!   change, or a held row the server checks on reconnect, evicts it: the
//!   row is currently unavailable to the reader. Answers carry the server
//!   seq they are complete through, and windows carry their row ids so the
//!   client can drop stale cached rows.
//! * Serializable transactions: reads run locally at the snapshot cut
//!   (hydrated from the authority at that cut when online); the commit
//!   ships the read set with server versions, and the authority re-runs
//!   each read under the client's identity and applies the writes only if
//!   every result and every written row is unchanged since the snapshot.
//!   A stale or partial replica can only cause a conflict, never a
//!   non-serializable commit.

#[path = "sync/accepted_overlay.rs"]
mod accepted_overlay;
#[path = "sync/inheritance.rs"]
mod inheritance;
#[path = "sync/reachability.rs"]
mod reachability;
#[path = "sync/readable_policy.rs"]
mod readable_policy;
#[path = "sync/scoped_live.rs"]
mod scoped_live;
#[path = "sync/scoped_queries.rs"]
mod scoped_queries;
#[path = "sync/scoped_reads.rs"]
mod scoped_reads;
use scoped_reads::ScopedEvidence;
pub use scoped_reads::ScopedReadRowV1;
#[path = "sync/unions.rs"]
mod unions;
pub use unions::UnionOccurrenceV1;
pub(super) use unions::{UnionLive, waiting as union_waiting};
type FetchAnswer = (
    u64,
    Vec<[u8; 16]>,
    Option<Vec<ScopedReadRowV1>>,
    Option<Vec<UnionOccurrenceV1>>,
);

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

use crate::ids::NodeUuid;
use crate::protocol::SnapshotRef;
use crate::tx::TxId;
use jazz_protocol::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz_types::large_values::chunks::ChunkRequest;
use jazz_types::time::{GlobalTime, TxTime};
use serde::{Deserialize, Serialize};

use super::*;
#[path = "sync/owner.rs"]
mod owner;
#[path = "sync/owner_init.rs"]
pub(super) mod owner_init;
#[path = "sync/relay.rs"]
mod relay;
pub use relay::OwnerDurabilityBarrier;
use relay::{RelayUpload, UploadId};
pub(crate) const RELAY_DDL: &str = relay::DDL;
pub(crate) const OWNER_DDL: &str = owner::DDL;
pub use crate::options::{FirstLoad, RemoteLinkHint};
pub use crate::wire::TransportError;
pub(crate) use owner::{
    check_open_schema as check_owner_open_schema, handoff_account_owner,
    stored_subject as stored_owner_subject,
};

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
/// Ephemeral own publications follow the durable owner's ordered prefix. Both
/// ranges remain exactly representable by JavaScript's number type.
pub(crate) const FOREGROUND_BASE: u64 = LOCAL_BASE + (1 << 51);

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

/// `jazz.sqlite.content-witness.v1`: the exact content version retained by a
/// later deletion or content-free restore. Row state and content have distinct
/// sequences. A partial replica must retain this link, never infer it from the
/// newest history version it happens to have cached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentWitnessRef {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub row_seq: u64,
    pub content_seq: u64,
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
    /// The read listed deleted rows too (`includeDeleted`).
    pub include_deleted: bool,
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
    /// `jazz.sqlite.serializable-origins.v1`: each own dependency is
    /// (local publication position, original writer node, original writer tx).
    /// Read versions name positions; authority validation resolves the origins.
    SerializableOriginsV1 {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<(u64, [u8; 16], u64)>,
    },
    /// jazz.sqlite.serializable-observed.v1: original dependencies plus one
    /// serializable-read-content.v1 digest for each row of each read. The
    /// authority validates values that pending replay may have changed without
    /// changing the final row's original transaction identity.
    SerializableObservedV1 {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<(u64, [u8; 16], u64)>,
        content: Vec<Vec<[u8; 32]>>,
    },
    /// jazz.sqlite.serializable-union-observed.v1: the same origin and read
    /// layout, with semantic union occurrences included in content evidence.
    /// Union rows use serializable-union-read-content.v1; ordinary rows keep
    /// serializable-read-content.v1. Required even for an empty union read.
    /// Retained non-UNION aliases also use this kind and the plain-row content
    /// role; their existing Query AST and named cells retain the projection.
    SerializableUnionObservedV1 {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<(u64, [u8; 16], u64)>,
        content: Vec<Vec<[u8; 32]>>,
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

/// Encoded node bytes per `PushChunks` or `Chunks` message: a batch is
/// flushed once it reaches this, so with one more node at most and the
/// message's framing it still fits one wire frame.
const CHUNK_BATCH_BYTES: usize = jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES / 2;
const _: () = assert!(
    CHUNK_BATCH_BYTES + jazz_types::large_values::MAX_ENCODED_NODE_BYTES + 64 * 1024
        <= jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES
);

/// An exact accepted base needed by a private owner input. `None` means the
/// coordinate has no accepted image, not that its image may be omitted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerInputBaseV1 {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub seq: Option<u64>,
}

/// The exact accepted image (or absence) at one frozen inherited base cut.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerFrozenBaseV1 {
    pub base: OwnerInputBaseV1,
    pub at: u64,
}

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
        /// The writes are `author`'s as a session, with these claims (a
        /// trusted link acting for it): the authority checks their
        /// policies as that session. Otherwise `author` is provenance only.
        session: Option<BTreeMap<String, Value>>,
        /// The writes' explicit provenance timestamp (Unix ms), if any.
        updated_at: Option<u64>,
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
    /// The row is currently unavailable to this reader (a policy change,
    /// or a held row the server checked): the replica drops its copy.
    Evict {
        table: String,
        id: [u8; 16],
        key: Vec<u8>,
    },
    /// The row left the server's results for this reader (a write took
    /// away its read access): later settled results leave it out, but the
    /// copy already delivered stays readable locally (INV-SYNC-14).
    Unlist {
        table: String,
        id: [u8; 16],
        key: Vec<u8>,
    },
    /// A subscription's result is complete through server seq `through`;
    /// `window` lists its rows when it is limited. `global`: the result is
    /// the global authority's (directly, or through relays that settle by
    /// their upstream), not a relay's own store.
    Settled {
        id: u64,
        through: u64,
        window: Option<Vec<[u8; 16]>>,
        global: bool,
    },
    Answer {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
    },
    /// A `Subscribe` or `Fetch` the server cannot serve, with why: the
    /// read ends with this error instead of waiting for an answer.
    Refused {
        id: u64,
        code: ErrorCode,
        message: String,
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
    /// `jazz.sqlite.sync.content-origins.v1`: the transactions behind
    /// shipped content, as (seq, writer node, writer tx), ahead of `Rows`.
    /// Ordinary reads carry these too. This identifies a version; only
    /// a Global settlement/receipt establishes authority coverage.
    Origins {
        commits: Vec<(u64, [u8; 16], u64)>,
    },
    /// `jazz.sqlite.sync.scoped-fetch.v1`: a trusted backend's one-shot
    /// read as a served identity, with immutable request claims. Append-only
    /// tag; ordinary session connections cannot submit this request.
    /// Uses the existing Rows/Answer/Refused responses and never persists.
    ScopedFetchV1 {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        author: String,
        claims: BTreeMap<String, Value>,
        include_deleted: bool,
    },
    /// Append-only tag 20, `jazz.sqlite.sync.content-witness.v1`. Sent with
    /// ordinary Rows/Origins; the receiver persists the exact retained-content
    /// link. This is provenance, not a Global coverage or acceptance receipt.
    ContentWitnessesV1 {
        witnesses: Vec<ContentWitnessRef>,
    },
    /// Append-only tag 21: authority-qualified row evidence, separate from
    /// ordinary Origins and from query-completeness answers.
    AcceptedRowsV1 {
        rows: Vec<AcceptedRow>,
    },
    /// Tag 22, `jazz.sqlite.sync.accepted-fetch.v1`: require a complete
    /// authority answer at one cut. Scope must match the authenticated session,
    /// unless a trusted backend link vouches for a delegated request.
    AcceptedFetchV1 {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        at: Option<u64>,
        include_deleted: bool,
        scope: Option<(String, BTreeMap<String, Value>)>,
    },
    /// Tag 23: exact root ids at the authority cut requested above. Ordinary
    /// cache answers cannot satisfy this request, even on an otherwise live link.
    AcceptedAnswerV1 {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
    },
    /// Tag 24, jazz.sqlite.sync.catalogue-metadata.v1. Cached authority
    /// metadata is useful offline but never asserts fresh query coverage.
    CatalogueMetadataV1 {
        head: CatalogueHeadV1,
        schemas: Vec<CatalogueSchemaV1>,
    },
    /// Tag 25, jazz.sqlite.sync.catalogue-fetch.v1. Evaluate the opened
    /// schema's zero-row query under this authenticated or trusted scope.
    CatalogueFetchV1 {
        id: u64,
        table: String,
        opened: crate::ids::SchemaVersionId,
        scope: Option<(String, BTreeMap<String, Value>)>,
    },
    /// Tag 26, jazz.sqlite.sync.catalogue-covered.v1. The metadata must be
    /// admitted before this fresh authority answer can complete its request.
    CatalogueCoveredV1 {
        id: u64,
        head: CatalogueHeadV1,
        opened: crate::ids::SchemaVersionId,
    },
    /// Tag 27, jazz.sqlite.sync.origin-write-fate.v1. A relay multiplexes writers,
    /// so the complete original transaction identity is mandatory.
    FateV1 {
        node: [u8; 16],
        tx: u64,
        through: u64,
        result: Result<(), WireError>,
    },
    /// Tag 28, jazz.sqlite.sync.owner-stored.v1. The immediate durable owner
    /// has retained this complete original upload; this is not a terminal fate.
    OwnerStoredV1 {
        node: [u8; 16],
        tx: u64,
    },
    /// Tag 29, jazz.sqlite.sync.owner-projection.v1. A foreground advertises
    /// qualified-input support and only the pending identities it already holds.
    /// This is a format declaration, never an identity or trust grant.
    OwnerProjectionV1 {
        known: Vec<([u8; 16], u64)>,
    },
    /// Tag 30, jazz.sqlite.sync.owner-inputs.v1. Complete original Commit bytes
    /// paired with stable owner publication positions, in publication order.
    /// Accepted bases precede this envelope through the ordinary row protocol.
    OwnerInputsV1 {
        schema: [u8; 16],
        through: u64,
        bases: Vec<OwnerInputBaseV1>,
        frozen: Vec<OwnerFrozenBaseV1>,
        units: Vec<(u64, Vec<u8>)>,
    },
    /// Tag 31, jazz.sqlite.sync.owner-ready.v1. The immediate private owner
    /// supports recovery for this subject and schema. This is neither a flush
    /// receipt nor authority acceptance; the host authenticates the transport.
    OwnerReadyV1 {
        schema: [u8; 16],
        subject: String,
        node: [u8; 16],
    },
    /// Tag 32, jazz.sqlite.sync.policy-descriptor.v1. Authority-issued policies
    /// for the exact accepted catalogue head; this never grants session trust.
    PolicyDescriptorV1(PolicyDescriptorV1),
    /// Tag 33, jazz.sqlite.sync.owner-initialization-reserve.v1.
    OwnerInitializationReserveV1 {
        id: u64,
        nonce: [u8; 16],
        node: [u8; 16],
        minimum: u64,
    },
    /// Tag 34, jazz.sqlite.sync.owner-initialization-reserved.v1. Success is
    /// emitted only after the private owner's real backing-store flush.
    OwnerInitializationReservedV1 {
        id: u64,
        nonce: [u8; 16],
        result: Result<([u8; 16], u64), WireError>,
    },
    /// Tag 35, jazz.sqlite.sync.owner-initialization-publish.v1. The private
    /// owner validates the capability and forwards the unchanged normal Commit.
    OwnerInitializationPublishV1 {
        nonce: [u8; 16],
        commit: Vec<u8>,
    },
    /// Tag 36, jazz.sqlite.sync.scoped-evidence-answer.v1. Exact versions of
    /// the bounded permission closure, only for trusted backend scoped reads.
    /// Row contents still arrive through ordinary Rows/chunk synchronization.
    ScopedEvidenceAnswerV1 {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
        evidence: Vec<ScopedReadRowV1>,
    },
    /// Tag 37, jazz.sqlite.sync.accepted-scoped-evidence-answer.v1.
    /// Accepted query coverage plus exact permission inputs at the requested cut.
    AcceptedScopedEvidenceAnswerV1 {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
        evidence: Vec<ScopedReadRowV1>,
    },
    /// Tag 38, jazz.sqlite.sync.accepted-overlay-check.v1. A trusted backend
    /// asks whether its staged writes preserve an accepted query at one cut.
    AcceptedOverlayCheckV1 {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        through: u64,
        include_deleted: bool,
        author: String,
        claims: BTreeMap<String, Value>,
        ops: Vec<WireOp>,
    },
    /// Tag 39, jazz.sqlite.sync.accepted-overlay-checked.v1. No rows or writes
    /// are published; the matching request's staged projection was unchanged.
    AcceptedOverlayCheckedV1 {
        id: u64,
        through: u64,
    },
    /// Tag 40, jazz.sqlite.sync.readable-policy-subscribe.v1. An ordinary
    /// session requests the readable permission inputs of its result as normal
    /// row images. Unreadable or unbounded inputs refuse this subscription.
    SubscribeReadablePolicyV1 {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        since: Option<u64>,
        include_deleted: bool,
    },
    /// Tag 41, jazz.sqlite.sync.union-answer.v1. An ordinary fetch's exact
    /// ordered semantic occurrences, after every UNION and arm window.
    UnionAnswerV1 {
        id: u64,
        through: u64,
        occurrences: Vec<UnionOccurrenceV1>,
    },
    /// Tag 42, jazz.sqlite.sync.scoped-union-subscribe.v1. Only a trusted
    /// backend can maintain this immutable request scope. Every update carries
    /// occurrence coverage, permission evidence and matching Global settlement.
    ScopedUnionSubscribeV1 {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        author: String,
        claims: BTreeMap<String, Value>,
        include_deleted: bool,
    },
}

fn subscription_message(
    readable_policy: bool,
    id: u64,
    query: Query,
    binding: BTreeMap<String, Value>,
    since: Option<u64>,
    include_deleted: bool,
    scope: RemoteScope,
) -> SyncMessage {
    if let Some((author, claims)) = scope {
        SyncMessage::ScopedUnionSubscribeV1 {
            id,
            query,
            binding,
            author: author.canonical().to_owned(),
            claims,
            include_deleted,
        }
    } else if readable_policy {
        SyncMessage::SubscribeReadablePolicyV1 {
            id,
            query,
            binding,
            since,
            include_deleted,
        }
    } else {
        SyncMessage::Subscribe {
            id,
            query,
            binding,
            since,
            include_deleted,
        }
    }
}

fn evidence_answer(
    accepted: bool,
    id: u64,
    through: u64,
    rows: Vec<[u8; 16]>,
    evidence: Vec<ScopedReadRowV1>,
) -> SyncMessage {
    if accepted {
        SyncMessage::AcceptedScopedEvidenceAnswerV1 {
            id,
            through,
            rows,
            evidence,
        }
    } else {
        SyncMessage::ScopedEvidenceAnswerV1 {
            id,
            through,
            rows,
            evidence,
        }
    }
}

// ---- client state ---------------------------------------------------------

pub(crate) enum Kind {
    Mergeable,
    Serializable {
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<u64>,
        content: Vec<Vec<[u8; 32]>>,
    },
}

struct Pending {
    /// The relay journal owns retry and persistence for this private projection.
    owner_managed: bool,
    /// This owner completed its storage barrier for the immutable own upload.
    locally_durable: bool,
    tx: u64,
    seq: u64,
    node: [u8; 16],
    ops: Vec<StagedOp>,
    rows: Vec<(String, RowUuid, Vec<u8>)>,
}

/// One remote attachment: a client query the server maintains.
pub(crate) struct Remote {
    id: u64,
    scope: RemoteScope,
    refs: usize,
    query: Query,
    binding: BTreeMap<String, Value>,
    include_deleted: bool,
    pub(crate) answered: Rc<StdCell<bool>>,
    /// Whether the last answer was the global authority's.
    pub(crate) global: Rc<StdCell<bool>>,
    /// Why the server refused this query, once it has.
    pub(crate) refused: Rc<RefCell<Option<Error>>>,
    /// The server seq this query's result was last settled through.
    through: Option<u64>,
    /// Resubscribed with `since`: on its opening, fetch result rows the
    /// replica lacks (rows that became visible without changing).
    resumed: bool,
    union: Option<Rc<UnionLive>>,
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
    /// Checked once on owner recovery, before any outbox prefix can escape.
    initialization_recovery_error: Option<Error>,
    relay_uploads: BTreeMap<UploadId, RelayUpload>,
    relay_finished: BTreeSet<UploadId>,
    owner_stored: BTreeSet<UploadId>,
    initialization_reservations: BTreeMap<u64, owner_init::ReservationAnswer>,
    pub(super) initialization_uploads: BTreeMap<UploadId, [u8; 16]>,
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
    /// Schema declared when this upstream connection opened.
    upstream_schema: Option<crate::ids::SchemaVersionId>,
    fates: HashMap<UploadId, Result<(), Error>>,
    /// Exact immutable bytes of the private inputs received from this owner.
    owner_inputs: BTreeMap<UploadId, Vec<u8>>,
    owner_projection_bound: bool,
    /// Pinned for this foreground's lifetime, including reconnects.
    owner_node: Option<[u8; 16]>,
    /// An acknowledgement applies only to the current connection and schema.
    owner_ready: Option<crate::ids::SchemaVersionId>,
    /// Ordered semantic frames held while an owner input fetches its chunks.
    owner_inbound: VecDeque<SyncMessage>,
    shadow: HashMap<(String, RowUuid, Vec<u8>), Option<Image>>,
    /// Highest server seq any answer was complete through.
    pub(crate) known: u64,
    /// The `known` cut last written to `_jazz_meta`.
    persisted_known: u64,
    answers: HashMap<u64, Option<Result<FetchAnswer, Error>>>,
    scoped_answers: HashSet<u64>,
    overlay_answers: HashSet<u64>,
    accepted_answers: HashSet<u64>,
    union_answers: HashSet<u64>,
    qualified_union_receipts: HashMap<u64, (u64, Vec<UnionOccurrenceV1>)>,
    catalogue_answers:
        HashMap<u64, Option<Result<(CatalogueHeadV1, crate::ids::SchemaVersionId), Error>>>,
    /// Permission preflights asked for; `None` until answered.
    advice: HashMap<u64, Option<PermissionAdvice>>,
    /// Large-value nodes asked for and not yet answered.
    requested: BTreeSet<ChunkRequest>,
    /// Nodes to ask for on the next send.
    wanted: BTreeSet<ChunkRequest>,
    /// Nodes pushed for each unsettled commit.
    pushed: HashMap<UploadId, Vec<jazz_types::large_values::Locator>>,
    /// Requests the server could not answer; never asked again.
    unavailable: BTreeSet<ChunkRequest>,
    /// Chunks arrived this tick: results waiting for them run again.
    arrived: bool,
}

pub(crate) type RemoteScope = Option<(AuthorSubject, BTreeMap<String, Value>)>;

// Runtime-only attachment identity. Unscoped keys retain their previous form.
fn scoped_remote_key(
    query: &Query,
    binding: &BTreeMap<String, Value>,
    include_deleted: bool,
    scope: &RemoteScope,
) -> String {
    let key = remote_key(query, binding, include_deleted);
    match scope {
        Some(scope) => format!("scoped:{key}{scope:?}"),
        None => key,
    }
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
    /// Whether the answer is the global authority's.
    pub(crate) global: Rc<StdCell<bool>>,
    pub(crate) hint: Rc<StdCell<Link>>,
    /// Held on a live link: leaving it (even for a new attempt) releases.
    pub(crate) live: bool,
    /// `None`: wait for the server however long (the Remote tier).
    pub(crate) deadline: Option<Instant>,
    /// A fresh foreground must receive its owner's cache before any fallback.
    pub(crate) wait_for_owner: bool,
    pub(crate) require_global: bool,
    pub(crate) _wake: Option<Rc<Instant>>,
    pub(crate) _attempt_wake: Option<Rc<Instant>>,
}

impl Gate {
    /// Whether the read may keep waiting: its deadline has not passed and
    /// the link may still answer.
    pub(crate) fn may_still_wait(&self) -> bool {
        self.deadline
            .is_none_or(|deadline| Instant::now() < deadline)
            && may_wait(self.hint.get())
    }

    pub(crate) fn open(&self) -> bool {
        if self.wait_for_owner && !self.answered.get() {
            return false;
        }
        if self.answered.get() && (!self.require_global || self.global.get()) {
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
    /// By this store: `true` when it is the global authority.
    Always(bool),
    Never,
    /// By the upstream's answer, and whether that answer is the global
    /// authority's.
    Remote(Rc<StdCell<bool>>, Rc<StdCell<bool>>),
    /// The relay can answer from its cache immediately, but global settlement
    /// still requires the upstream authority's answer.
    Relay(Rc<StdCell<bool>>, Rc<StdCell<bool>>),
    Union {
        answered: Rc<StdCell<bool>>,
        global: Rc<StdCell<bool>>,
        live: Rc<UnionLive>,
        relay: bool,
    },
}

impl SettledBy {
    pub(crate) fn get(&self) -> bool {
        match self {
            SettledBy::Always(_) | SettledBy::Relay(..) => true,
            SettledBy::Never => false,
            SettledBy::Remote(answered, _) => answered.get(),
            SettledBy::Union {
                answered,
                live,
                relay,
                ..
            } => *relay || (answered.get() && !live.optimistic.get()),
        }
    }

    /// The durability a settled result has.
    pub(crate) fn tier(&self) -> DurabilityTier {
        let global = match self {
            SettledBy::Always(authority) => *authority,
            SettledBy::Never => false,
            SettledBy::Remote(answered, global) | SettledBy::Relay(answered, global) => {
                answered.get() && global.get()
            }
            SettledBy::Union {
                answered,
                global,
                live,
                ..
            } => answered.get() && global.get() && !live.optimistic.get(),
        };
        if global {
            DurabilityTier::Global
        } else {
            DurabilityTier::Local
        }
    }
}

// ---- server state ---------------------------------------------------------

/// An upstream subscription's identity: query, binding, include_deleted.
type RemoteRead = (Query, BTreeMap<String, Value>, bool, RemoteScope);

/// A downstream request keeps its own upstream ID and immutable prepared scope.
struct ForwardedFetch {
    upstream: u64,
    connection: u64,
    owner: Rc<Schema>,
    downstream_version: Option<crate::ids::SchemaVersionId>,
    prepared: PreparedQuery,
    identity: AuthorSubject,
    include_deleted: bool,
    accepted: bool,
    scoped_evidence: bool,
    overlay: bool,
    /// Uses the upstream connection's admitted identity, with no delegation.
    ordinary: bool,
    // Retain the timer so missing content cannot leave a relay request open forever.
    deadline: Option<Rc<Instant>>,
}

struct ForwardedCatalogue {
    upstream: u64,
    connection: u64,
    opened: crate::ids::SchemaVersionId,
    owner: Rc<Schema>,
    deadline: Rc<Instant>,
}

struct ReleasedReads {
    subscriptions: Vec<RemoteRead>,
    fetches: Vec<u64>,
}

pub(crate) struct Session {
    transport: Box<dyn Transport>,
    identity: AuthorSubject,
    /// This connection's own claims. Two sessions of one identity may hold
    /// different claims (two tokens), so its reads and writes evaluate
    /// policies with these, not the identity's latest.
    claims: Option<BTreeMap<String, Value>>,
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
    /// Touched rows a `Hold` declared: the client's copy is checked, so
    /// one it can no longer read is evicted rather than unlisted.
    probed: BTreeSet<(String, RowUuid, Vec<u8>)>,
    /// The read policies changed since the last tick: a row the client can
    /// no longer read is evicted rather than unlisted.
    revalidate: bool,
    /// The versions this tick's accepted commits from the client wrote:
    /// (table, id, branch key, seq). Each ships even when a later version
    /// supersedes it, so the replica holds every state its own commits
    /// produced (a snapshot naming one reads it there).
    committed: Vec<(String, RowUuid, Vec<u8>, u64)>,
    /// Supporting rows each subscription needs besides its own result.
    support: BTreeMap<u64, Rc<Vec<Support>>>,
    readable_policy: BTreeMap<u64, PreparedQuery>,
    query_support: BTreeMap<u64, PreparedQuery>,
    scoped_unions: BTreeMap<u64, PreparedQuery>,
    /// The replica's coherence watches: `(table, column, value)` for every
    /// row shipped (`id`), include target and array correlation. A commit
    /// that writes a matching row ships its image (or an eviction).
    watch: HashSet<u64>,
    /// Resumed subscriptions whose opening is still to send: `since` seq.
    resume: BTreeMap<u64, u64>,
    /// Supporting rows shipped to the replica, and rows of fetches and
    /// ended subscriptions it keeps, per table, with whether the client
    /// could read each when last checked. A commit to a table their
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
    /// On a relay, the upstream subscription each of the client's
    /// subscriptions holds (query, binding, include_deleted): its results
    /// settle once the upstream answers, never from the relay's store.
    relayed: BTreeMap<u64, RemoteRead>,
    forwarded: BTreeMap<u64, ForwardedFetch>,
    forwarded_catalogue: BTreeMap<u64, ForwardedCatalogue>,
    relay_uploads: BTreeSet<UploadId>,
    owner_acked: BTreeSet<UploadId>,
    initialization_reservations: BTreeMap<u64, owner_init::OwnerReservation>,
    owner_projection: bool,
    owner_observed: BTreeSet<UploadId>,
    catalogue_pending: bool,
    /// That version's schema, governed by the current schema's policies,
    /// as built for the current schema it holds.
    schema: Option<(Rc<Schema>, Rc<Schema>)>,
}

impl Session {
    /// Read policies changed: rows the client loses read access to are
    /// evicted on its next tick.
    pub(crate) fn revalidate(&mut self) {
        self.revalidate = true;
    }

    fn new(transport: Box<dyn Transport>, identity: AuthorSubject) -> Self {
        Self {
            transport,
            identity,
            claims: None,
            subs: BTreeMap::new(),
            outbox: Vec::new(),
            stale: Vec::new(),
            touched: BTreeSet::new(),
            probed: BTreeSet::new(),
            revalidate: false,
            committed: Vec::new(),
            support: BTreeMap::new(),
            readable_policy: BTreeMap::new(),
            query_support: BTreeMap::new(),
            scoped_unions: BTreeMap::new(),
            watch: HashSet::new(),
            resume: BTreeMap::new(),
            held: BTreeMap::new(),
            open: true,
            ingress: (Instant::now(), 0),
            backlog: VecDeque::new(),
            version: None,
            schema: None,
            relayed: BTreeMap::new(),
            forwarded: BTreeMap::new(),
            forwarded_catalogue: BTreeMap::new(),
            relay_uploads: BTreeSet::new(),
            owner_acked: BTreeSet::new(),
            initialization_reservations: BTreeMap::new(),
            owner_projection: false,
            owner_observed: BTreeSet::new(),
            catalogue_pending: false,
        }
    }

    /// Release everything the session holds; its shared results are swept
    /// once no other member reads them.
    /// Returns the upstream subscriptions and one-shot reads to release.
    #[must_use]
    fn close(&mut self, dropped: &std::cell::Cell<usize>) -> ReleasedReads {
        dropped.set(dropped.get() + self.subs.len());
        let relayed = std::mem::take(&mut self.relayed);
        let fetches = self
            .forwarded
            .values()
            .map(|read| read.upstream)
            .chain(self.forwarded_catalogue.values().map(|read| read.upstream))
            .collect();
        *self = Session {
            open: false,
            ..Session::new(Box::new(ClosedTransport), self.identity)
        };
        ReleasedReads {
            subscriptions: relayed.into_values().collect(),
            fetches,
        }
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
    /// A forward array relation (`outer` holds child ids) correlates each
    /// element; a reverse one (`inner` holds parent ids) each child whose
    /// array contains the parent's value.
    Array {
        table: String,
        inner: String,
        outer: String,
        outer_array: bool,
        inner_array: bool,
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

/// A snapshot whose history this store has not observed (yet): it may be
/// readable once the store syncs further.
fn snapshot_unavailable(why: &str) -> Error {
    Error::new(
        crate::options::ErrorCode::NotObserved,
        format!("Snapshot history is unavailable in this store: {why}"),
    )
}

/// Whether a reachability traversal is a gather the replica can evaluate
/// from the result and its edges ([`Support::Reach`]): seeded by rows of
/// the root table that are themselves in the result (no filter after the
/// gather), reaching root rows by id.
fn gather_from_root(query: &Query, via: &crate::query::ReachableVia) -> bool {
    via.seed.as_ref().is_some_and(|seed| {
        seed.table == query.table && seed.user_column.is_none() && seed.team_column == "id"
    }) && via.access_table == query.table
        && via.access_row_column == "id"
        && via.access_team_column == "id"
}

fn gather_shaped(query: &Query, via: &crate::query::ReachableVia) -> bool {
    gather_from_root(query, via) && via.access_filters.is_empty() && query.filters.is_empty()
}

/// What a remote read of `query` subscribes to: the query itself, or for
/// an aggregate its input rows (its filters and traversals, every column,
/// no window), which the client aggregates as it would local rows.
pub(crate) fn remote_query(query: &Query) -> std::borrow::Cow<'_, Query> {
    if crate::db::relation::is_union(query) {
        return std::borrow::Cow::Borrowed(query);
    }
    let mut input = crate::db::relation::input(query);
    // A window past an offset is only right over the rows before it too:
    // the replica holds the whole prefix and applies the offset itself.
    if query.aggregate.is_none() && query.offset > 0 {
        let prefix = input.to_mut();
        prefix.limit = query.limit.map(|limit| limit + query.offset);
        prefix.offset = 0;
        return input;
    }
    if query.aggregate.is_none() {
        return input;
    }
    let aggregate = input.to_mut();
    aggregate.aggregate = None;
    aggregate.select = None;
    aggregate.order_by.clear();
    aggregate.limit = None;
    aggregate.offset = 0;
    input
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

/// The elements of an array cell, a UUID as its stored blob: the values
/// an array relation correlates (and its watches key) one by one.
fn array_elements(cell: &Sql) -> Option<Vec<Sql>> {
    let text = match cell {
        Sql::Text(text) if text.starts_with(b"[") => text,
        _ => return None,
    };
    let elements: Vec<serde_json::Value> = serde_json::from_slice(text).ok()?;
    Some(
        elements
            .into_iter()
            .map(|element| match element {
                serde_json::Value::String(s) => match uuid::Uuid::parse_str(&s) {
                    Ok(id) => Sql::Blob(id.as_bytes().to_vec()),
                    Err(_) => Sql::Text(s.into_bytes()),
                },
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .map(Sql::Integer)
                    .unwrap_or_else(|| Sql::Real(n.as_f64().unwrap_or_default())),
                other => Sql::Text(other.to_string().into_bytes()),
            })
            .collect(),
    )
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
        for via in query
            .reachable
            .iter()
            .filter(|via| gather_shaped(query, via))
        {
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
                outer_array: crate::compile::is_array(parent, &array.outer_column),
                inner_array: crate::compile::is_array(child, &array.inner_column),
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
                outer_array: false,
                inner_array: false,
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
            None => match self.local_position(TxId::new(TxTime(tx), node))? {
                Some(seq) => seq,
                None if node == self.identity.node => tx,
                None => return Ok(Vec::new()),
            },
        };
        let mut images = Vec::new();
        self.history_versions(table, [(joined.row, seq)], &mut images, seqs)?;
        Ok(images)
    }

    /// Add the exact content witness behind each already-authorized image.
    /// Local complete stores resolve it by (id, branch, seq); replicas may
    /// forward only an explicit upstream link, because their history has gaps.
    /// Missing links/content stay missing rather than becoming guessed evidence.
    fn content_witnesses(
        &self,
        images: &mut Vec<Image>,
        seqs: &mut BTreeSet<u64>,
    ) -> Result<Vec<ContentWitnessRef>, Error> {
        let mut seen = HashSet::new();
        images.retain(|image| {
            seen.insert((image.table.clone(), image.id, image.key.clone(), image.seq))
        });
        let mut witnesses = Vec::new();
        let mut refs = Vec::new();
        for image in images.iter().filter(|image| image.seq < LOCAL_BASE) {
            if image.content {
                seqs.insert(image.seq);
                continue;
            }
            let id = image.id.to_vec();
            // A non-content image's content is earlier, hence in history.
            let (filter, seq) = if self.client.is_none() {
                (
                    "id = ?1 AND _b = ?2 AND _seq < ?3 AND _c = 1 \
                     ORDER BY _seq DESC LIMIT 1",
                    image.seq,
                )
            } else if let Some(seq) =
                self.content_witness_seq(&image.table, &image.id, &image.key, image.seq)?
            {
                ("id = ?1 AND _b = ?2 AND _seq = ?3 AND _c = 1", seq)
            } else {
                continue;
            };
            let witness = self
                .images_where(
                    &image.table,
                    true,
                    filter,
                    &[&id, &image.key, &(seq as i64)],
                )?
                .pop();
            if let Some(witness) = witness {
                seqs.insert(witness.seq);
                refs.push(ContentWitnessRef {
                    table: image.table.clone(),
                    id: image.id,
                    key: image.key.clone(),
                    row_seq: image.seq,
                    content_seq: witness.seq,
                });
                if seen.insert((
                    witness.table.clone(),
                    witness.id,
                    witness.key.clone(),
                    witness.seq,
                )) {
                    witnesses.push(witness);
                }
            }
        }
        images.extend(witnesses);
        Ok(refs)
    }

    /// Durable `jazz.sqlite.content-witness.v1`: postcard u64, with no trailing
    /// bytes. The SQL primary key names the state whose content it identifies.
    fn content_witness_seq(
        &self,
        table: &str,
        id: &[u8; 16],
        key: &[u8],
        row_seq: u64,
    ) -> Result<Option<u64>, Error> {
        let bytes = self
            .conn
            .query_row(
                "SELECT content FROM _jazz_content_witness_v1 \
             WHERE t = ?1 AND id = ?2 AND k = ?3 AND seq = ?4",
                rusqlite::params![table, id.as_slice(), key, row_seq as i64],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(storage)?;
        bytes
            .map(|bytes| {
                let (seq, remaining) =
                    postcard::take_from_bytes::<u64>(&bytes).map_err(|error| {
                        Error::new(
                            ErrorCode::Storage,
                            format!("invalid content-witness-v1: {error}"),
                        )
                    })?;
                if !remaining.is_empty() || seq == 0 || seq >= row_seq || row_seq >= LOCAL_BASE {
                    return Err(Error::new(
                        ErrorCode::Storage,
                        "invalid content-witness-v1 sequence",
                    ));
                }
                Ok(seq)
            })
            .transpose()
    }

    fn record_content_witness(&self, witness: &ContentWitnessRef) -> Result<(), Error> {
        if witness.content_seq == 0
            || witness.content_seq >= witness.row_seq
            || witness.row_seq >= LOCAL_BASE
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "invalid content-witness-v1 sequence",
            ));
        }
        if let Some(previous) =
            self.content_witness_seq(&witness.table, &witness.id, &witness.key, witness.row_seq)?
        {
            if previous != witness.content_seq {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "conflicting content-witness-v1 reference",
                ));
            }
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO _jazz_content_witness_v1 (t, id, k, seq, content) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![&witness.table, witness.id.as_slice(), &witness.key, witness.row_seq as i64, encode(&witness.content_seq)],
        ).map_err(storage)?;
        Ok(())
    }

    /// Who wrote each accepted content sequence. A local store identifies its
    /// own commits; a replica forwards only recorded upstream origins. Neither
    /// an optimistic sequence nor a missing origin becomes relay provenance.
    pub(super) fn origins(&self, seqs: &BTreeSet<u64>) -> Result<Vec<(u64, [u8; 16], u64)>, Error> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT node, tx FROM _jazz_settled WHERE seq = ?1")
            .map_err(storage)?;
        let mut out = Vec::with_capacity(seqs.len());
        for &seq in seqs.iter().filter(|&&seq| seq < LOCAL_BASE) {
            let recorded = statement
                .query_row([seq as i64], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)? as u64))
                })
                .optional()
                .map_err(storage)?;
            match recorded {
                Some((node, tx)) => {
                    let node = node.try_into().map_err(|_| {
                        Error::new(ErrorCode::Storage, "invalid stored content origin node")
                    })?;
                    out.push((seq, node, tx));
                }
                None if self.client.is_none() => {
                    let origin = self.local_origin(seq)?;
                    out.push((seq, *origin.node.as_bytes(), origin.time.0));
                }
                None => {}
            }
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
            let (table, column, values, nested, contains) = match item {
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
                    Some(value @ Sql::Blob(_)) => (target, "id", vec![value], &[][..], false),
                    _ => continue,
                },
                Support::Array {
                    table,
                    inner,
                    outer,
                    outer_array,
                    inner_array,
                    nested,
                } => match image_cell(image, outer) {
                    Some(Sql::Null) | None => continue,
                    Some(value) if *outer_array => {
                        let elements = array_elements(&value).unwrap_or_default();
                        (table, inner.as_str(), elements, &nested[..], *inner_array)
                    }
                    Some(value) => (
                        table,
                        inner.as_str(),
                        vec![value],
                        &nested[..],
                        *inner_array,
                    ),
                },
            };
            for value in values {
                self.follow_value(
                    table, column, value, contains, nested, identity, out, watch, held,
                )?;
            }
        }
        Ok(())
    }

    /// Ship the readable rows of `table` whose `column` is `value` (or,
    /// `contains`, whose array `column` holds it) and their nested
    /// support, watching the correlation so later writes reach the replica.
    #[allow(clippy::too_many_arguments)]
    fn follow_value(
        &self,
        table: &str,
        column: &str,
        value: Sql,
        contains: bool,
        nested: &[Support],
        identity: Option<AuthorSubject>,
        out: &mut Vec<Image>,
        watch: &mut HashSet<u64>,
        held: &mut BTreeMap<String, BTreeMap<(RowUuid, Vec<u8>), bool>>,
    ) -> Result<(), Error> {
        // A watched key's rows were shipped with it, and later writes
        // to them ship as they happen. Nested support is followed
        // again: a child that joined since may have unwatched children.
        let fresh = watch.insert(wk((table.to_owned(), column.to_owned(), watch_key(&value))));
        if !fresh && nested.is_empty() {
            return Ok(());
        }
        let filter = if column == "id" {
            "id = ?1".to_owned()
        } else if contains {
            format!(
                "EXISTS (SELECT 1 FROM json_each({}) AS r WHERE unhex(replace(r.value, '-', '')) = ?1)",
                quote(column)
            )
        } else {
            format!("{} = ?1", quote(column))
        };
        for found in self.images_where(table, false, &filter, &[&value])? {
            let row = RowUuid::from_bytes(found.id);
            let can = readable(self, table, row, &found.key, identity)?;
            if identity.is_some() {
                held.entry(table.to_owned())
                    .or_default()
                    .insert((row, found.key.clone()), can);
            }
            if !can {
                continue;
            }
            let unwatched = watch.insert(wk((
                table.to_owned(),
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
        let mut ctx =
            crate::compile::Ctx::new(&schema, None, &claims, &binding).with_catalogue(self);
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
            // Rows the server stopped listing are not declared: it ships
            // them again if this reader regains access.
            let sql = format!(
                "SELECT id, _b, _seq FROM {} AS t WHERE _seq < ?1 AND _del = 0 AND NOT EXISTS \
                 (SELECT 1 FROM _jazz_revoked AS r WHERE r.t = ?2 AND r.id = t.id AND r.k = t._b)",
                quote(table)
            );
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let rows = statement
                .query_map(rusqlite::params![LOCAL_BASE as i64, table], |r| {
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
    pub(super) fn images_where(
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

    /// The server accepted this replica's commit `tx` at `through` but will
    /// not ship rows it wrote that the reader cannot read (a write-only
    /// insert). Those versions are the replica's own writes: they stay, as
    /// settled at `through`, rather than vanishing on the rebase.
    fn keep_unlisted_own(&mut self, id: UploadId, through: u64) -> Result<(), Error> {
        let client = self.client.as_ref().expect("client");
        let Some(pending) = client.pending.iter().find(|p| (p.node, p.tx) == id) else {
            return Ok(());
        };
        let position = pending.seq;
        let rows = pending.rows.clone();
        for (table, row, key) in rows {
            let unlisted = self
                .conn
                .query_row(
                    "SELECT 1 FROM _jazz_revoked WHERE t = ?1 AND id = ?2 AND k = ?3",
                    rusqlite::params![table, row.0.as_bytes().to_vec(), key],
                    |_| Ok(()),
                )
                .optional()
                .map_err(storage)?
                .is_some();
            let unsettled = matches!(
                self.client.as_ref().expect("client").shadow.get(&(
                    table.clone(),
                    row,
                    key.clone()
                )),
                Some(None)
            );
            if !(unlisted && unsettled) {
                continue;
            }
            let current = self
                .current_image(&table, row, &key)?
                .filter(|image| image.seq == position);
            let image = match current {
                Some(image) => Some(image),
                None => self
                    .images_where(
                        &table,
                        true,
                        "id=?1 AND _b=?2 AND _seq=?3",
                        &[&row.as_bytes().to_vec(), &key, &(position as i64)],
                    )?
                    .pop(),
            };
            if let Some(mut image) = image {
                image.seq = through;
                let client = self.client.as_mut().expect("client");
                client.shadow.insert((table, row, key), Some(image));
            }
        }
        Ok(())
    }

    /// Forget that the server stopped listing a row ([`Inner::unlist`]).
    fn readmit(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        let readmitted = self
            .conn
            .execute(
                "DELETE FROM _jazz_revoked WHERE t = ?1 AND id = ?2 AND k = ?3",
                rusqlite::params![table, row.0.as_bytes().to_vec(), key],
            )
            .map_err(storage)?;
        if readmitted > 0 {
            self.changed
                .borrow_mut()
                .push((table.to_owned(), row, key.to_vec()));
        }
        Ok(())
    }

    /// The server no longer lists a row for this replica (it lost read
    /// access). The cached copy stays readable locally, as local reads are
    /// not a redaction boundary, but reads of the server's view
    /// (`ReadTier::Remote`) leave it out until the server ships it again.
    fn unlist(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO _jazz_revoked (t, id, k) VALUES (?1, ?2, ?3)",
                rusqlite::params![table, row.0.as_bytes().to_vec(), key],
            )
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.to_owned(), row, key.to_vec()));
        Ok(())
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
    CREATE INDEX IF NOT EXISTS _jazz_settled_by_seq ON _jazz_settled (seq);\
    CREATE TABLE IF NOT EXISTS _jazz_content_witness_v1 (t TEXT NOT NULL, id BLOB NOT NULL, k BLOB NOT NULL, seq INTEGER NOT NULL, content BLOB NOT NULL, PRIMARY KEY (t, id, k, seq)) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_revoked (t TEXT NOT NULL, id BLOB NOT NULL, k BLOB NOT NULL, PRIMARY KEY (t, id, k)) WITHOUT ROWID;";

type RowKey = (String, RowUuid, Vec<u8>);

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("encode sync state")
}

impl Inner {
    fn write_rejection(&self, position: u64) -> Result<Option<Error>, Error> {
        let origin = self.local_origin(position)?;
        let tx = origin.time.0;
        if let Some(Err(error)) = self
            .client
            .as_ref()
            .and_then(|client| client.fates.get(&(*origin.node.as_bytes(), tx)))
        {
            return Ok(Some(error.clone()));
        }
        let node = *origin.node.as_bytes();
        if let Some(Err(error)) = self.applied_fate(node, tx)? {
            return Ok(Some(from_wire_error(error)));
        }
        if self.settled(node, tx)? == Some(None) {
            // Earlier SQLite replicas retained the rejection but not its reason.
            // Never interpret that absence of detail as successful acceptance.
            return Err(Error::new(
                ErrorCode::NotObserved,
                "write was rejected; stored rejection detail is unavailable",
            ));
        }
        Ok(None)
    }

    fn write_state(&self, tx: u64) -> Result<WriteState, Error> {
        self.require_owner_subject(self.identity.author)?;
        if let Some(error) = self.write_rejection(tx)? {
            return Ok(WriteState::rejected(&error, DurabilityTier::None));
        }
        let origin = self.local_origin(tx)?;
        if let Some(Some(seq)) = self.settled(*origin.node.as_bytes(), origin.time.0)? {
            if seq >= LOCAL_BASE {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "upstream write receipt is not an authority sequence",
                ));
            }
            return Ok(WriteState {
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(seq)),
                durability: DurabilityTier::Global,
            });
        }
        if tx > self.seq {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "write has not been observed by this database",
            ));
        }
        if self.authority && self.client.is_none() && tx < LOCAL_BASE {
            return Ok(WriteState {
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(tx)),
                durability: DurabilityTier::Global,
            });
        }
        Ok(WriteState {
            fate: Fate::Pending,
            global_time: None,
            durability: if !self.non_durable
                || self.client.as_ref().is_some_and(|client| {
                    client
                        .owner_stored
                        .contains(&(*origin.node.as_bytes(), origin.time.0))
                }) {
                DurabilityTier::Local
            } else {
                DurabilityTier::None
            },
        })
    }

    pub(super) fn initialization_publication_status(
        &self,
        position: u64,
    ) -> Result<InitializationTransactionStatus, Error> {
        let origin = self.local_origin(position)?;
        if self
            .settled(*origin.node.as_bytes(), origin.time.0)?
            .is_some()
            || self.client.is_none()
        {
            return self
                .write_state(position)
                .map(InitializationTransactionStatus::Complete);
        }
        let unit: Option<(Vec<u8>, Vec<u8>)> = self
            .conn
            .query_row(
                "SELECT commit_msg,rows FROM _jazz_pending WHERE tx=?1",
                [origin.time.0 as i64],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        let Some((message, rows)) = unit else {
            return Ok(InitializationTransactionStatus::Incomplete);
        };
        let (message, trailing): (SyncMessage, _) = postcard::take_from_bytes(&message)
            .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
        if !trailing.is_empty()
            || !matches!(message,SyncMessage::Commit{node,tx,..} if node==*origin.node.as_bytes() && tx==origin.time.0)
        {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization upload identity is corrupt",
            ));
        }
        let (rows, trailing): (Vec<(String, [u8; 16], Vec<u8>)>, _) =
            postcard::take_from_bytes(&rows)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
        if !trailing.is_empty() {
            return Err(Error::new(
                ErrorCode::Storage,
                "trailing initialization upload rows",
            ));
        }
        let SyncMessage::Commit { kind, ops, .. } = message else {
            unreachable!("checked commit")
        };
        if matches!(kind, WireKind::Mergeable) {
            return Ok(InitializationTransactionStatus::Incomplete);
        }
        // Do not accept an empty/truncated row manifest as proof of a complete
        // upload. Resolve only each operation's exact stored row coordinates.
        let mut expected = BTreeSet::new();
        for op in ops {
            let op = from_wire(op)?;
            let Some(row) = op.row() else { continue };
            let table = self.schema.stored_table(op.table())?;
            let mut images = self.images_where(
                table,
                false,
                "id=?1 AND _seq=?2",
                &[&row.as_bytes().to_vec(), &(position as i64)],
            )?;
            images.extend(self.images_where(
                table,
                true,
                "id=?1 AND _seq=?2",
                &[&row.as_bytes().to_vec(), &(position as i64)],
            )?);
            if images.is_empty() {
                return Ok(InitializationTransactionStatus::Incomplete);
            }
            for image in images {
                expected.insert((table.to_owned(), *row.as_bytes(), image.key));
            }
        }
        if expected != rows.iter().cloned().collect() || expected.len() != rows.len() {
            return Ok(InitializationTransactionStatus::Incomplete);
        }
        for (table, row, key) in rows {
            // Use the immutable stored coordinates recorded by the normal
            // upload path; a missing/malformed physical source is Storage.
            let row = RowUuid::from_bytes(row);
            let current = self.current_image(&table, row, &key)?;
            if current.is_none_or(|image| image.seq != position)
                && self
                    .images_where(
                        &table,
                        true,
                        "id=?1 AND _b=?2 AND _seq=?3",
                        &[&row.as_bytes().to_vec(), &key, &(position as i64)],
                    )?
                    .is_empty()
            {
                return Ok(InitializationTransactionStatus::Incomplete);
            }
        }
        self.write_state(position)
            .map(InitializationTransactionStatus::Complete)
    }

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

    fn forget_pending(&self, id: relay::UploadId) -> Result<(), Error> {
        self.exec("DELETE FROM _jazz_pending WHERE tx = ?1", [id.1 as i64])
            .map_err(storage)?;
        self.release_upload_roots(id)
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

    /// Durable allocator floor, including transactions that publish no rows.
    /// Named encoding jazz.sqlite.issued-sequence.v1: one exact postcard u64.
    pub(crate) fn record_issued_seq(&self, seq: u64) -> Result<(), Error> {
        if seq >= i64::MAX as u64 {
            return Err(Error::new(
                ErrorCode::Storage,
                "issued-sequence-v1 range exhausted",
            ));
        }
        self.persist_meta("issued-seq-v1", seq)
    }

    /// Rows alone cannot recover ids consumed by empty, rejected, or rebased
    /// transactions. The floor joins each commit's existing SQLite savepoint.
    pub(crate) fn restore_issued_seq(&mut self) -> Result<(), Error> {
        let stored: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT v FROM _jazz_meta WHERE k = 'issued-seq-v1'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        if let Some(bytes) = stored {
            let (seq, rest): (u64, _) = postcard::take_from_bytes(&bytes).map_err(|e| {
                Error::new(
                    ErrorCode::Storage,
                    format!("invalid issued-sequence-v1: {e}"),
                )
            })?;
            if !rest.is_empty() || seq >= i64::MAX as u64 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "invalid issued-sequence-v1 range or trailing bytes",
                ));
            }
            self.seq = self.seq.max(seq);
        }
        // Recover transaction evidence already present in an older SQLite
        // file too; never mistake another replica's local ids for ours.
        let known: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(v) FROM (SELECT MAX(tx) AS v FROM _jazz_pending \
             UNION ALL SELECT MAX(seq) FROM _jazz_owner_positions_v1 \
             UNION ALL SELECT MAX(tx) FROM _jazz_applied WHERE node = ?1 \
             UNION ALL SELECT MAX(tx) FROM _jazz_settled WHERE node = ?1 \
             UNION ALL SELECT MAX(seq) FROM _jazz_settled WHERE seq < ?2)",
                rusqlite::params![self.identity.node.as_bytes().to_vec(), LOCAL_BASE as i64],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if let Some(known) = known {
            if known < 0 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "negative persisted transaction sequence",
                ));
            }
            self.seq = self.seq.max(known as u64);
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
            Ok(bytes) => postcard::from_bytes(&bytes).map(Some).map_err(|error| {
                Error::new(
                    ErrorCode::Storage,
                    format!("invalid stored write fate: {error}"),
                )
            }),
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
        self.exec(
            "UPDATE _jazz_local_commits_v1 SET pending=0 WHERE node=?1 AND tx=?2",
            rusqlite::params![node.to_vec(), tx as i64],
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
        for &tx in own {
            let origin = self.local_origin(tx)?;
            match self.settled(*origin.node.as_bytes(), origin.time.0)? {
                Some(Some(seq)) => cut.extra.push(seq),
                Some(None) => {}
                None => {
                    let current = self
                        .client
                        .as_ref()
                        .and_then(|client| {
                            client
                                .pending
                                .iter()
                                .find(|p| {
                                    p.node == *origin.node.as_bytes() && p.tx == origin.time.0
                                })
                                .map(|p| p.seq)
                        })
                        .unwrap_or(tx);
                    cut.extra.push(current);
                }
            }
        }
        Ok(cut)
    }

    /// The own commits a replica's serializable transaction began over:
    /// those still unsettled (none on the authority).
    pub(crate) fn unsettled_own(&self) -> Vec<u64> {
        self.client.as_ref().map_or_else(Vec::new, |client| {
            client.pending.iter().map(|p| p.seq).collect()
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
            return Err(snapshot_unavailable(
                "it was captured past the authority state this store has synced",
            ));
        }
        let mut cut = Cut::at(snapshot.global_base.0);
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
            if let Some(client) = &self.client {
                if let Some(pending) = client.pending.iter().find(|p| p.tx == tx && p.node == node)
                {
                    cut.extra.push(pending.seq);
                    continue;
                }
                if client.fates.get(&(node, tx)).is_some_and(Result::is_err) {
                    continue;
                }
            }
            return Err(snapshot_unavailable(
                "it names another replica's commits not yet settled here",
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
            .prepare("SELECT p.tx, p.commit_msg, p.rows FROM _jazz_pending p LEFT JOIN _jazz_local_commits_v1 l ON l.tx=p.tx ORDER BY COALESCE(l.seq,p.tx)")
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
            let SyncMessage::Commit {
                node,
                ops,
                author,
                session,
                updated_at,
                ..
            } = &commit
            else {
                continue;
            };
            let mut ops = ops
                .iter()
                .cloned()
                .map(from_wire)
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(author) = author.as_deref().map(AuthorSubject::from_canonical) {
                let author = author.map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
                ops.iter_mut()
                    .for_each(|op| op.attribute(author, session.is_some()));
            }
            if let Some(at) = *updated_at {
                ops.iter_mut().for_each(|op| op.stamp_at(at));
            }
            let seq = self
                .local_position(TxId::new(TxTime(tx), NodeUuid::from_bytes(*node)))?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::Storage,
                        "pending upload lacks its local-commit-v1 identity record",
                    )
                })?;
            client.pending.push(Pending {
                owner_managed: false,
                locally_durable: false,
                tx,
                seq,
                node: *node,
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
        client.relay_uploads = self.load_relay_uploads()?;
        self.client = Some(client);
        let recovery = self
            .check_initialization_recovery()
            .and_then(|()| self.check_owner_initialization_recovery());
        self.client
            .as_mut()
            .expect("client")
            .initialization_recovery_error = recovery.err();
        Ok(())
    }

    fn check_initialization_recovery(&self) -> Result<(), Error> {
        let mut statement=self.conn.prepare("SELECT seq,author FROM _jazz_local_commits_v1 WHERE initialization=1 AND pending=1 ORDER BY seq").map_err(storage)?;
        let pending = statement
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
            .map_err(storage)?;
        for entry in pending {
            let (seq, author) = entry.map_err(storage)?;
            if author != self.identity.author.canonical() {
                return Err(rejected(
                    "pending initialization belongs to another admitted owner",
                ));
            }
            if !matches!(
                self.initialization_publication_status(seq as u64)?,
                InitializationTransactionStatus::Complete(_)
            ) {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "pending initialization recovery unit is incomplete",
                ));
            }
        }
        Ok(())
    }
}

impl Client {
    fn cancel_fetch(&mut self, id: u64) {
        self.answers.remove(&id);
        self.scoped_answers.remove(&id);
        self.overlay_answers.remove(&id);
        self.accepted_answers.remove(&id);
        self.union_answers.remove(&id);
        self.qualified_union_receipts.remove(&id);
        self.catalogue_answers.remove(&id);
        self.outbox.retain(|message| {
            !matches!(message,
                SyncMessage::Fetch { id: request, .. }
                    | SyncMessage::ScopedFetchV1 { id: request, .. }
                    | SyncMessage::AcceptedFetchV1 { id: request, .. }
                    | SyncMessage::AcceptedOverlayCheckV1 { id: request, .. }
                    | SyncMessage::CatalogueFetchV1 { id: request, .. }
                    if *request == id)
        });
    }

    fn new(node: [u8; 16]) -> Self {
        Client {
            initialization_recovery_error: None,
            relay_uploads: BTreeMap::new(),
            relay_finished: BTreeSet::new(),
            owner_stored: BTreeSet::new(),
            initialization_reservations: BTreeMap::new(),
            initialization_uploads: BTreeMap::new(),
            node,
            connection: 0,
            upstream_schema: None,
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
            owner_inputs: BTreeMap::new(),
            owner_projection_bound: false,
            owner_node: None,
            owner_ready: None,
            owner_inbound: VecDeque::new(),
            shadow: HashMap::new(),
            known: 0,
            persisted_known: 0,
            answers: HashMap::new(),
            scoped_answers: HashSet::new(),
            overlay_answers: HashSet::new(),
            accepted_answers: HashSet::new(),
            union_answers: HashSet::new(),
            qualified_union_receipts: HashMap::new(),
            catalogue_answers: HashMap::new(),
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
        let readable_policy = inner.scope_isolated_owner;
        let client = inner.client.as_mut().expect("client");
        client.transport = Some(transport);
        client.owner_ready = None;
        client.owner_inbound.clear();
        client.qualified_union_receipts.clear();
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
                if let Some(live) = &r.union {
                    live.retire_pending();
                }
                r.resumed = r.through.is_some();
                subscription_message(
                    readable_policy,
                    r.id,
                    r.query.clone(),
                    r.binding.clone(),
                    r.through,
                    r.include_deleted,
                    r.scope.clone(),
                )
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
        // Commits, their chunk pushes and chunk requests a full queue held
        // back are sent again below from what is still owed.
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::Subscribe { .. }
                    | SyncMessage::SubscribeReadablePolicyV1 { .. }
                    | SyncMessage::ScopedUnionSubscribeV1 { .. }
                    | SyncMessage::Hold { .. }
                    | SyncMessage::Schema { .. }
                    | SyncMessage::OwnerProjectionV1 { .. }
                    | SyncMessage::Commit { .. }
                    | SyncMessage::PushChunks { .. }
                    | SyncMessage::ChunkRequests { .. }
                    | SyncMessage::Fetch { .. }
                    | SyncMessage::ScopedFetchV1 { .. }
                    | SyncMessage::AcceptedFetchV1 { .. }
                    | SyncMessage::AcceptedOverlayCheckV1 { .. }
                    | SyncMessage::CatalogueFetchV1 { .. }
            )
        });
        inner.resend_initialization_reservations();
        let version = inner.owner.jazz.version_id();
        let client = inner.client.as_mut().expect("client");
        client.upstream_schema = Some(version);
        client.outbox.insert(
            0,
            SyncMessage::Schema {
                version: *version.as_bytes(),
            },
        );
        if inner.non_durable {
            let client = inner.client.as_mut().expect("client");
            let known = client.owner_inputs.keys().copied().collect();
            client
                .outbox
                .insert(1, SyncMessage::OwnerProjectionV1 { known });
        }
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
        for upload in client.relay_uploads.values_mut() {
            upload.sent = false;
        }
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
        client.owner_ready = None;
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

    /// Become a replica through the ordinary role/admission path.
    fn ensure_client(&self) {
        self.ensure_client_checked().expect("persist replica flag");
    }
    fn ensure_client_checked(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if inner.client.is_some() {
            return Ok(());
        }
        inner.persist_meta("client", 1)?;
        inner.seq = inner.seq.max(LOCAL_BASE);
        let node = *inner.identity.node.as_bytes();
        inner.client = Some(Client::new(node));
        Ok(())
    }
    /// A fresh offline owner joins the same replica path before reserving any
    /// identity. Pre-existing standalone history has no authority snapshot or
    /// upload units and must not be reinterpreted as accepted remote history.
    pub(super) fn admit_initialization_replica(&self) -> Result<(), Error> {
        let inner = self.inner.borrow();
        if let Some(client) = &inner.client {
            if let Some(error) = &client.initialization_recovery_error {
                return Err(error.clone());
            }
            return Ok(());
        }
        if inner.authority {
            return Ok(());
        }
        if inner.seq != 0 {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "initialization requires replica admission before standalone writes",
            ));
        }
        drop(inner);
        self.ensure_client_checked()
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
        drop(inner);
        self.schedule_read_deadlines();
        self.schedule_pending_tick();
    }

    pub(crate) fn is_client(&self) -> bool {
        self.inner.borrow().client.is_some()
    }

    /// Record a local commit for upload (client only). Inside an open
    /// transaction the ops join it.
    pub(crate) fn record_upload(
        &self,
        ops: Vec<StagedOp>,
        seq: u64,
        identity: Option<WriteIdentity>,
        kind: Kind,
    ) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if inner.client.is_none() {
            return Ok(());
        }
        if inner.tx.is_some() {
            inner.capture.extend(ops);
            return Ok(());
        }
        if seq != inner.seq {
            return Ok(());
        }
        let origin = inner.local_origin(seq)?;
        let author = match identity {
            Some(WriteIdentity::Session(author) | WriteIdentity::Attribution(author)) => {
                Some(author)
            }
            _ => ops
                .iter()
                .find_map(StagedOp::author)
                .or_else(|| Some(inner.stamp_author())),
        };
        let explicit_session = matches!(identity, Some(WriteIdentity::Session(_)))
            || ops.iter().any(StagedOp::is_session);
        let session_claims = if explicit_session {
            author.map(|author| inner.claims.get(&author).cloned().unwrap_or_default())
        } else if matches!(identity, None | Some(WriteIdentity::Database))
            && !ops
                .iter()
                .any(|op| matches!(op.identity(), Some(WriteIdentity::Attribution(_))))
            && inner.identity.author != AuthorSubject::SYSTEM
        {
            // An ordinary node's own write must remain a session write even
            // over a backend-authenticated socket. Local session metadata is
            // not an upstream credential. Normal session links use their
            // admitted claims; trusted links receive no vouched claims here.
            Some(BTreeMap::new())
        } else {
            None
        };
        let rows = if inner.scope_isolated_owner || inner.non_durable {
            inner.owner_input_coordinates(&ops)?
        } else {
            inner.written_rows(&ops, seq)?
        };
        let mut shadows = Vec::new();
        for (table, row, key) in &rows {
            if !inner
                .client
                .as_ref()
                .expect("client")
                .shadow
                .contains_key(&(table.clone(), *row, key.clone()))
            {
                shadows.push((
                    (table.clone(), *row, key.clone()),
                    match inner.current_image(table, *row, key)? {
                        Some(image) if image.seq < LOCAL_BASE => Some(image),
                        _ => inner.settled_before(table, *row, key)?,
                    },
                ));
            }
        }
        let wire = ops
            .iter()
            .map(to_wire)
            .collect::<Result<Vec<WireOp>, Error>>()?;
        let kind = match kind {
            Kind::Mergeable => WireKind::Mergeable,
            Kind::Serializable {
                snapshot,
                reads,
                own,
                content,
            } => {
                let origins = own
                    .iter()
                    .map(|&position| {
                        inner
                            .local_origin(position)
                            .map(|id| (position, *id.node.as_bytes(), id.time.0))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // Every read carries content evidence. A write-only batch has
                // no observed content and keeps the existing origin encoding.
                if reads.iter().any(|read| read.query.relation.is_some()) {
                    WireKind::SerializableUnionObservedV1 {
                        snapshot,
                        reads,
                        own: origins,
                        content,
                    }
                } else if !reads.is_empty() {
                    WireKind::SerializableObservedV1 {
                        snapshot,
                        reads,
                        own: origins,
                        content,
                    }
                } else if origins
                    .iter()
                    .all(|(position, node, tx)| position == tx && node == origin.node.as_bytes())
                {
                    WireKind::Serializable {
                        snapshot,
                        reads,
                        own,
                    }
                } else {
                    WireKind::SerializableOriginsV1 {
                        snapshot,
                        reads,
                        own: origins,
                    }
                }
            }
        };
        let commit = SyncMessage::Commit {
            node: *origin.node.as_bytes(),
            tx: origin.time.0,
            kind,
            ops: wire,
            author: author.map(|a| a.canonical().to_owned()),
            session: session_claims,
            updated_at: ops.iter().find_map(StagedOp::updated_at_ms),
        };
        inner.persist_pending(origin.time.0, &commit, &rows)?;
        inner.retain_upload_roots(&commit)?;
        inner.record_owner_input_rows((*origin.node.as_bytes(), origin.time.0), &rows, &ops)?;
        for ((table, row, key), image) in &shadows {
            inner
                .exec(
                    "INSERT INTO _jazz_shadow(t,id,k,image) VALUES(?1,?2,?3,?4)",
                    rusqlite::params![
                        table,
                        row.as_bytes().as_slice(),
                        key,
                        image.as_ref().map(encode)
                    ],
                )
                .map_err(storage)?;
        }
        let pending_tables = rows.iter().map(|(table, ..)| table.clone()).collect();
        let client = inner.client.as_mut().expect("client");
        client.shadow.extend(shadows);
        client.unsent.push(commit);
        client.pending.push(Pending {
            owner_managed: false,
            locally_durable: false,
            tx: origin.time.0,
            seq,
            node: *origin.node.as_bytes(),
            ops,
            rows,
        });
        // Standalone writes notify before their upload is recorded. Refresh
        // only affected UNION groups once the pending status is available.
        inner.refresh_union_pending(&pending_tables);
        Ok(())
    }

    /// Remote reads ship the query's rows plus its supporting rows (single
    /// hop includes, array subqueries, junction traversals, gathers), and
    /// an aggregate's input rows ([`remote_query`]); the client evaluates
    /// locally. Other shapes that read further tables
    /// (uncorrelated traversals or ones through a referenced row, gathers
    /// filtered after the gather, multi-hop includes) would see a partial
    /// replica, so they are refused explicitly.
    pub(crate) fn remote_shape(query: &Query) -> Result<(), Error> {
        if crate::db::relation::is_union(query) {
            return Err(crate::values::unsupported(
                "remote union reads and subscriptions (union-arm coverage)",
            ));
        }
        let query = &*remote_query(query);
        if query.relation.is_some() {
            return Err(crate::values::query_error(
                "invalid retained relation projection",
            ));
        }
        let other_tables = !query.joins.iter().all(join_followable)
            || query
                .reachable
                .iter()
                .any(|via| gather_from_root(query, via) && !gather_shaped(query, via))
            || query
                .includes
                .iter()
                .any(|include| include.path.contains('.'));
        if other_tables {
            return Err(crate::values::unsupported(format!(
                "a synced read of `{}` that reads further tables (uncorrelated joins, joins through a referenced row, filtered gathers or multi-hop includes)",
                query.table
            )));
        }
        Ok(())
    }

    /// Attach a remote query (refcounted per query and binding): whether
    /// the server answered it, and why it refused it.
    pub(crate) fn attach_remote(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        scope: RemoteScope,
    ) -> Option<(
        Rc<StdCell<bool>>,
        Rc<RefCell<Option<Error>>>,
        Rc<StdCell<bool>>,
    )> {
        let query = remote_query(&prepared.query);
        let mut inner = self.inner.borrow_mut();
        let readable_policy = inner.scope_isolated_owner;
        let client = inner.client.as_mut()?;
        let key = scoped_remote_key(&query, &prepared.binding, include_deleted, &scope);
        if let Some(remote) = client.remote.get_mut(&key) {
            remote.refs += 1;
            // A new attachment is a new read. Its authority wait cannot reuse
            // a receipt issued before this request, even if another observer
            // keeps the same query alive. A fresh ID also excludes old replies
            // already in flight. Relays take this path too, so the round trip
            // reaches the authority rather than ending at a warm owner.
            let id = client.next_id;
            client.next_id += 1;
            let previous = std::mem::replace(&mut remote.id, id);
            remote.answered.set(false);
            remote.global.set(false);
            *remote.refused.borrow_mut() = None;
            remote.through = None;
            remote.resumed = false;
            if let Some(live) = &remote.union {
                live.retire_pending();
            }
            client.remote_ids.remove(&previous);
            client.remote_ids.insert(id, key);
            client
                .outbox
                .push(SyncMessage::Unsubscribe { id: previous });
            client.outbox.push(subscription_message(
                readable_policy,
                id,
                query.clone().into_owned(),
                prepared.binding.clone(),
                None,
                include_deleted,
                scope.clone(),
            ));
            return Some((
                remote.answered.clone(),
                remote.refused.clone(),
                remote.global.clone(),
            ));
        }
        let id = client.next_id;
        client.next_id += 1;
        let answered = Rc::new(StdCell::new(false));
        let refused = Rc::new(RefCell::new(None));
        let global = Rc::new(StdCell::new(false));
        client.outbox.push(subscription_message(
            readable_policy,
            id,
            query.clone().into_owned(),
            prepared.binding.clone(),
            None,
            include_deleted,
            scope.clone(),
        ));
        client.remote.insert(
            key.clone(),
            Remote {
                id,
                scope: scope.clone(),
                refs: 1,
                query: query.into_owned(),
                binding: prepared.binding.clone(),
                include_deleted,
                answered: answered.clone(),
                refused: refused.clone(),
                global: global.clone(),
                through: None,
                resumed: false,
                union: crate::db::relation::is_union(&prepared.query)
                    .then(|| Rc::new(UnionLive::new(scope.is_some()))),
            },
        );
        client.remote_ids.insert(id, key);
        Some((answered, refused, global))
    }

    pub(crate) fn detach_remote(
        &self,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        scope: &RemoteScope,
    ) {
        self.inner
            .borrow_mut()
            .release_remote(query, binding, include_deleted, scope);
    }

    pub(crate) fn link_hint(&self) -> Option<Rc<StdCell<Link>>> {
        self.inner.borrow().client.as_ref().map(|c| c.hint.clone())
    }
}

impl Inner {
    /// Drop one reference to an upstream subscription; the last one
    /// unsubscribes.
    pub(crate) fn release_remote(
        &mut self,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        scope: &RemoteScope,
    ) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let key = scoped_remote_key(&remote_query(query), binding, include_deleted, scope);
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
}

struct FetchRequest {
    inner: Rc<RefCell<Inner>>,
    id: u64,
}

impl Drop for FetchRequest {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        if let Some(client) = inner.client.as_mut() {
            client.cancel_fetch(self.id);
        }
    }
}

impl Db {
    /// Ask the server for one read and wait for its answer, unless the
    /// link cannot answer or `deadline` passes. Returns the server seq the
    /// answer is complete through, if it answered, or why it refused.
    pub(crate) async fn fetch(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        deadline: Option<Instant>,
    ) -> Result<Option<u64>, Error> {
        self.fetch_answer(prepared, at, include_deleted, deadline, None, false)
            .await
            .map(|answer| answer.map(|(through, _, _, _)| through))
    }

    /// Strict transaction reads wait while offline before issuing a request.
    /// The caller retains its read guard and coverage deadline during this wait.
    pub(super) async fn wait_for_read_transport(&self) {
        poll_fn(|_| {
            if self
                .inner
                .borrow()
                .client
                .as_ref()
                .is_some_and(|client| client.transport.is_some() && may_wait(client.hint.get()))
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    pub(super) async fn fetch_answer(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        deadline: Option<Instant>,
        scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        accepted: bool,
    ) -> Result<Option<FetchAnswer>, Error> {
        let _deadline = self.read_deadline(deadline);
        let _attempt_deadline = self.read_attempt_deadline();
        // An in-flight host handshake is not an admitted transport. Wait before
        // assigning this request to a connection: connect_upstream retires old
        // requests, including any incorrectly queued before the first connect.
        let available = poll_fn(|_| {
            let inner = self.inner.borrow();
            let Some(client) = &inner.client else {
                return Poll::Ready(false);
            };
            if !may_wait(client.hint.get()) || deadline.is_some_and(|d| Instant::now() >= d) {
                Poll::Ready(false)
            } else if client.transport.is_some() {
                Poll::Ready(true)
            } else {
                Poll::Pending
            }
        })
        .await;
        if !available {
            return Ok(None);
        }
        let (id, connection) = {
            let mut inner = self.inner.borrow_mut();
            let Some(client) = inner.client.as_mut() else {
                return Ok(None);
            };
            if !may_wait(client.hint.get()) {
                return Ok(None);
            }
            let id = client.next_id;
            client.next_id += 1;
            client.answers.insert(id, None);
            if crate::db::relation::is_union(&prepared.query) {
                client.union_answers.insert(id);
            }
            if scope.is_some() {
                client.scoped_answers.insert(id);
            }
            if accepted {
                client.accepted_answers.insert(id);
            }
            // A paired read hydrates the prefix so local re-evaluation of its
            // original offset/window has the same predecessors as the authority.
            // Scoped ordinary answers retain their exact-window contract.
            let query = if !accepted && scope.is_some() {
                prepared.query.clone()
            } else {
                remote_query(&prepared.query).into_owned()
            };
            let binding = prepared.binding.clone();
            let message = if accepted {
                SyncMessage::AcceptedFetchV1 {
                    id,
                    query,
                    binding,
                    at,
                    include_deleted,
                    scope: scope.map(|(author, claims)| (author.canonical().to_owned(), claims)),
                }
            } else {
                match scope {
                    Some((author, claims)) => SyncMessage::ScopedFetchV1 {
                        id,
                        query,
                        binding,
                        author: author.canonical().to_owned(),
                        claims,
                        include_deleted,
                    },
                    None => SyncMessage::Fetch {
                        id,
                        query,
                        binding,
                        at,
                        include_deleted,
                    },
                }
            };
            client.outbox.push(message);
            (id, client.connection)
        };
        if accepted {
            self.schedule_pending_tick();
        }
        let _request = FetchRequest {
            inner: self.inner.clone(),
            id,
        };
        poll_fn(|_| {
            let mut inner = self.inner.borrow_mut();
            let client = inner.client.as_mut().expect("client");
            if client.connection != connection {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "one-shot read interrupted by an upstream reconnect",
                )));
            }
            if let Some(Some(_)) = client.answers.get(&id) {
                let answer = client.answers.remove(&id).flatten().expect("answered");
                return Poll::Ready(answer.map(Some));
            }
            if !may_wait(client.hint.get()) || deadline.is_some_and(|d| Instant::now() >= d) {
                client.answers.remove(&id);
                return Poll::Ready(Ok(None));
            }
            Poll::Pending
        })
        .await
    }

    /// A remote one-shot must not overtake its preceding uploads, including
    /// commits held behind streamed chunks. Capture only this opening's writes;
    /// later writes cannot extend its wait. Either terminal fate releases the
    /// barrier: rejected writes are absent from the authority's ensuing answer.
    async fn wait_for_preceding_read_writes(&self, reader: AuthorSubject) {
        let mut pending: Vec<UploadId> = {
            let inner = self.inner.borrow();
            let Some(client) = &inner.client else {
                return;
            };
            client
                .pending
                .iter()
                .filter(|pending| {
                    // A trusted backend can serve independent sessions. Only
                    // writes in this read's identity belong to its ordering
                    // barrier; provenance-only attribution remains a backend
                    // write, just as in the ordinary transaction read path.
                    pending.ops.iter().any(|op| {
                        let writer = match op.identity() {
                            Some(WriteIdentity::Session(author)) => author,
                            _ => inner.identity.author,
                        };
                        writer == reader
                    })
                })
                .map(|p| (p.node, p.tx))
                .collect()
        };
        if pending.is_empty() {
            return;
        }
        let _attempt = self.read_attempt_deadline();
        self.schedule_pending_tick();
        poll_fn(|_| {
            let inner = self.inner.borrow();
            let Some(client) = &inner.client else {
                return Poll::Ready(());
            };
            pending.retain(|id| !client.fates.contains_key(id));
            if pending.is_empty() || !may_wait(client.hint.get()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
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
        let captured: Vec<UploadId> = {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            client.pending.iter().map(|p| (p.node, p.tx)).collect()
        };
        poll_fn(|_| {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            let settling = client.transport.is_some()
                && may_wait(client.hint.get())
                && client
                    .pending
                    .iter()
                    .any(|p| captured.contains(&(p.node, p.tx)));
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
                .filter(|p| captured.contains(&(p.node, p.tx)))
                .map(|p| TxId {
                    time: TxTime(p.tx),
                    node: NodeUuid::from_bytes(p.node),
                })
                .collect(),
        })
    }

    /// Wait for a local transaction's fate at `tier`.
    pub async fn wait_for_transaction(&self, tx: u64, tier: DurabilityTier) -> Result<u64, Error> {
        poll_fn(|_| {
            let inner = self.inner.borrow();
            inner.require_owner_subject(inner.identity.author)?;
            if let Some(error) = inner.write_rejection(tx)? {
                return Poll::Ready(Err(error));
            }
            if tier == DurabilityTier::None {
                return Poll::Ready(Ok(tx));
            }
            let state = inner.write_state(tx)?;
            if state.durability >= tier {
                return Poll::Ready(Ok(tx));
            }
            let origin = inner.local_origin(tx)?;
            if inner.client.as_ref().is_some_and(|client| {
                client
                    .pending
                    .iter()
                    .any(|p| p.node == *origin.node.as_bytes() && p.tx == origin.time.0)
            }) {
                return Poll::Pending;
            }
            Poll::Ready(Err(Error::new(
                ErrorCode::NotObserved,
                "write has no observed authority acceptance",
            )))
        })
        .await
    }

    /// Snapshot this node's write without consuming rejection reporting or
    /// waiting. Hosts with asynchronous local persistence qualify durability.
    pub fn write_state(&self, tx: u64) -> Result<WriteState, Error> {
        let inner = self
            .inner
            .try_borrow()
            .map_err(|_| Error::new(ErrorCode::NotObserved, "write state is temporarily busy"))?;
        inner.write_state(tx)
    }

    /// Wait until every transaction pending upload when called (recovered
    /// ones included) reaches `tier`, failing with the first the server
    /// rejected. Writes made after the call are not waited for.
    pub async fn wait_for_pending_writes(&self, tier: DurabilityTier) -> Result<(), Error> {
        if tier == DurabilityTier::None || !self.is_client() {
            return Ok(());
        }
        let pending: Vec<u64> = {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            client.pending.iter().map(|p| p.seq).collect()
        };
        for tx in pending {
            self.wait_for_transaction(tx, tier).await?;
        }
        Ok(())
    }

    /// Whether chunks this replica requested may still arrive: it is
    /// connected to an upstream that may answer, with requests outstanding.
    pub fn awaiting_chunks(&self) -> bool {
        let inner = self.inner.borrow();
        inner.client.as_ref().is_some_and(|client| {
            client.transport.is_some()
                && may_wait(client.hint.get())
                && !(client.requested.is_empty() && client.wanted.is_empty())
        })
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
        let unit = [(inner.schema.clone(), op.clone())];
        let applied = inner.candidate_unit(&unit, seq, false, false, |inner| {
            inner.apply_as(&op, who, seq).map(drop)
        });
        let undone = inner.rollback_savepoint("advise");
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
        let (capture_pending, known, persisted_known) = {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            (
                inner.catalogue_capture_pending.get(),
                client.known,
                client.persisted_known,
            )
        };
        let applied = self.client_apply().and_then(|answers| {
            self.inner
                .borrow()
                .conn
                .execute_batch("RELEASE sync_tick")
                .map_err(storage)?;
            Ok(answers)
        });
        let ready = {
            let mut inner = self.inner.borrow_mut();
            if let Err(error) = &applied {
                if inner.scope_isolated_owner || inner.non_durable {
                    inner.owner_projection_error = Some(Error::new(
                        ErrorCode::Storage,
                        format!(
                            "scope-isolated owner recovery requires reopen after a failed sync tick: {error}"
                        ),
                    ));
                }
                inner.rollback_savepoint("sync_tick").map_err(storage)?;
                inner.catalogue_capture_pending.set(capture_pending);
            }
            let client = inner.client.as_mut().expect("client");
            let finished = std::mem::take(&mut client.relay_finished);
            let ready = !finished.is_empty();
            for id in finished {
                if applied.is_ok() {
                    client.relay_uploads.remove(&id);
                } else if let Some(upload) = client.relay_uploads.get_mut(&id) {
                    upload.sent = false;
                }
            }
            if applied.is_err() {
                client.known = known;
                client.persisted_known = persisted_known;
            }
            ready
        };
        // A receipt cannot outlive a rolled-back batch of its evidence rows.
        // Stage only this tick's answers, without copying unrelated requests.
        let (answers, union_updates) = applied?;
        let union_changed = self.inner.borrow_mut().apply_union_coverage(union_updates);
        self.inner
            .borrow_mut()
            .client
            .as_mut()
            .expect("client")
            .publish_fetch_answers(answers);
        self.inner().refresh_authenticated_views()?;
        self.notify()?;
        self.inner().refresh_union_coverage(&union_changed);
        let arrived = {
            let mut inner = self.inner.borrow_mut();
            let client = inner.client.as_mut().expect("client");
            std::mem::take(&mut client.arrived)
        };
        if arrived {
            self.inner.borrow().retry_awaiting();
        }
        self.flush_settled();
        if ready {
            self.server_tick()?;
        }
        self.client_send()
    }

    fn client_apply(
        &self,
    ) -> Result<
        (
            Vec<(u64, Result<FetchAnswer, Error>)>,
            Vec<unions::LiveUpdate>,
        ),
        Error,
    > {
        let mut answers = Vec::new();
        let mut union_updates = Vec::new();
        let mut rebase = false;
        let mut answered_remotes = Vec::new();
        let mut shadow_changed = false;
        {
            let mut inner = self.inner.borrow_mut();
            let Some(client) = inner.client.as_mut() else {
                return Ok((answers, union_updates));
            };
            let mut inbound = Vec::new();
            if let Some(transport) = client.transport.as_mut() {
                while let Some(message) = transport.try_recv() {
                    inbound.push(message);
                }
            }
            // Chunks are not ordered with rows; everything else is applied
            // in arrival order.
            let mut queue =
                std::mem::take(&mut inner.client.as_mut().expect("client").owner_inbound);
            for message in inbound {
                match message {
                    SyncMessage::Chunks { nodes, missing } => {
                        inner.receive_chunks(nodes, missing)?;
                    }
                    other => queue.push_back(other),
                }
            }
            // Rows install without their chunks: a chunked cell carries the
            // sort key its indexes need. Pending operations must first fetch the
            // verified prefixes needed to reconstruct those index keys.
            while let Some(message) = queue.pop_front() {
                let accepted_evidence =
                    matches!(&message, SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. });
                if let SyncMessage::OwnerInputsV1 { schema, units, .. } = &message
                    && !inner.owner_input_chunks_ready(*schema, units)?
                {
                    queue.push_front(message);
                    let bytes = queue
                        .iter()
                        .map(|message| encode(message).len())
                        .sum::<usize>();
                    if bytes > 4 * jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES {
                        return Err(unsupported(
                            "owner input hydration backlog exceeding eight MiB",
                        ));
                    }
                    inner.client.as_mut().expect("client").owner_inbound = queue;
                    break;
                }
                let message = match message {
                    SyncMessage::Fate {
                        tx,
                        through,
                        result,
                    } => {
                        let client = inner.client.as_ref().expect("client");
                        if client.relay_uploads.keys().any(|(_, time)| *time == tx) {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "unqualified write fate is ambiguous on a relay",
                            ));
                        }
                        let mut origins = client.pending.iter().filter(|pending| pending.tx == tx);
                        let node = origins.next().map_or(client.node, |pending| pending.node);
                        if origins.any(|pending| pending.node != node) {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "unqualified write fate is ambiguous across original writers",
                            ));
                        }
                        SyncMessage::FateV1 {
                            node,
                            tx,
                            through,
                            result,
                        }
                    }
                    other => other,
                };
                match message {
                    SyncMessage::OwnerReadyV1 {
                        schema,
                        subject,
                        node,
                    } => {
                        inner.receive_owner_ready(schema, subject, node)?;
                    }
                    SyncMessage::OwnerInputsV1 {
                        schema,
                        through,
                        bases,
                        frozen,
                        units,
                    } => {
                        let required =
                            inner.receive_owner_inputs(schema, through, bases, frozen, units)?;
                        inner.replay_pending(&required)?;
                        // The exact bases and complete units are reconciled now,
                        // before any following answer or authority fate.
                        rebase = false;
                    }
                    SyncMessage::OwnerInitializationReservedV1 { id, nonce, result } => {
                        inner.receive_initialization_reservation(id, nonce, result)?;
                    }
                    SyncMessage::OwnerStoredV1 { node, tx } => {
                        let client = inner.client.as_mut().expect("client");
                        if client
                            .pending
                            .iter()
                            .any(|pending| pending.node == node && pending.tx == tx)
                        {
                            client.owner_stored.insert((node, tx));
                        }
                    }
                    SyncMessage::Rows { images } => {
                        if inner.non_durable && images.iter().any(|image| image.seq >= LOCAL_BASE) {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "foreground received an unqualified pending row",
                            ));
                        }
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
                        inner.readmit(&table, row, &key)?;
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
                    SyncMessage::Unlist { table, id, key } => {
                        let Ok(table) = inner.owner.stored_table(&table).map(str::to_owned) else {
                            continue;
                        };
                        inner.unlist(&table, RowUuid::from_bytes(id), &key)?;
                    }
                    SyncMessage::Settled {
                        id,
                        through,
                        window,
                        global,
                    } => {
                        let client = inner.client.as_mut().expect("client");
                        let union = client
                            .remote_ids
                            .get(&id)
                            .and_then(|key| client.remote.get(key))
                            .is_some_and(|remote| remote.union.is_some());
                        if (global || union) && through >= LOCAL_BASE {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "read settlement carries an optimistic sequence",
                            ));
                        }
                        client.known = client.known.max(through);
                        let Some(key) = client.remote_ids.get(&id).cloned() else {
                            continue;
                        };
                        let remote = client.remote.get_mut(&key).expect("remote");
                        if remote.union.is_some() {
                            union_updates.push(unions::LiveUpdate::Settled {
                                id,
                                through,
                                global,
                            });
                            continue;
                        }
                        remote.answered.set(true);
                        remote.global.set(global);
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
                    SyncMessage::CatalogueMetadataV1 { head, schemas } => {
                        let first_owner_catalogue =
                            inner.scope_isolated_owner && inner.catalogue_head()?.is_none();
                        inner.record_catalogue(&head, &schemas)?;
                        if first_owner_catalogue {
                            inner.reset_everything();
                            for session in &mut inner.sessions {
                                session.revalidate();
                            }
                        }
                        inner.restore_owner_policies()?;
                        for session in &mut inner.sessions {
                            session.catalogue_pending = true;
                        }
                    }
                    SyncMessage::PolicyDescriptorV1(descriptor) => {
                        inner.record_policy_descriptor(&descriptor)?;
                        inner.restore_owner_policies()?;
                        for session in &mut inner.sessions {
                            session.catalogue_pending = true;
                        }
                    }
                    SyncMessage::CatalogueCoveredV1 { id, head, opened } => {
                        let result = if inner.catalogue_head()?.as_ref() != Some(&head)
                            || inner.catalogue_schema(opened)?.is_none()
                        {
                            Err(Error::new(
                                ErrorCode::NotObserved,
                                "catalogue coverage lacks its exact accepted metadata",
                            ))
                        } else {
                            Ok((head, opened))
                        };
                        if let Some(slot) = inner
                            .client
                            .as_mut()
                            .expect("client")
                            .catalogue_answers
                            .get_mut(&id)
                        {
                            *slot = Some(result);
                        }
                    }
                    SyncMessage::AcceptedRowsV1 { rows } => {
                        for mut row in rows {
                            let Ok(table) = inner.owner.stored_table(&row.table).map(str::to_owned)
                            else {
                                continue;
                            };
                            row.table = table;
                            inner.record_accepted_row(&row)?;
                        }
                    }
                    SyncMessage::ContentWitnessesV1 { witnesses } => {
                        for mut witness in witnesses {
                            let Ok(table) =
                                inner.owner.stored_table(&witness.table).map(str::to_owned)
                            else {
                                continue;
                            };
                            witness.table = table;
                            inner.record_content_witness(&witness)?;
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
                    SyncMessage::ScopedEvidenceAnswerV1 {
                        id,
                        through,
                        rows,
                        mut evidence,
                    }
                    | SyncMessage::AcceptedScopedEvidenceAnswerV1 {
                        id,
                        through,
                        rows,
                        mut evidence,
                    } => {
                        let live_scope = inner
                            .client
                            .as_ref()
                            .and_then(|client| {
                                client
                                    .remote_ids
                                    .get(&id)
                                    .and_then(|key| client.remote.get(key))
                            })
                            .map(|remote| remote.scope.is_some());
                        if let Some(scoped) = live_scope {
                            if !scoped || accepted_evidence {
                                return Err(Error::new(
                                    ErrorCode::Protocol,
                                    "scoped evidence answered a different live request kind",
                                ));
                            }
                            scoped_reads::validate_evidence(through, &evidence)?;
                            for row in &mut evidence {
                                row.table = inner.owner.stored_table(&row.table)?.to_owned();
                            }
                            union_updates.push(unions::LiveUpdate::Evidence {
                                id,
                                through,
                                rows,
                                evidence,
                            });
                            continue;
                        }
                        if !inner
                            .client
                            .as_ref()
                            .expect("client")
                            .answers
                            .contains_key(&id)
                        {
                            continue;
                        }
                        let client = inner.client.as_ref().expect("client");
                        if client.overlay_answers.contains(&id)
                            || !client.scoped_answers.contains(&id)
                            || client.accepted_answers.contains(&id) != accepted_evidence
                        {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "scoped evidence answered a different request kind",
                            ));
                        }
                        scoped_reads::validate_evidence(through, &evidence)?;
                        for row in &mut evidence {
                            row.table = inner.owner.stored_table(&row.table)?.to_owned();
                        }
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        answers.push((id, Ok((through, rows, Some(evidence), None))));
                    }
                    SyncMessage::AcceptedOverlayCheckedV1 { id, through } => {
                        let client = inner.client.as_mut().expect("client");
                        if !client.answers.contains_key(&id) {
                            continue;
                        }
                        if !client.overlay_answers.contains(&id) || through >= LOCAL_BASE {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "accepted overlay response answered a different request kind or invalid cut",
                            ));
                        }
                        answers.push((id, Ok((through, Vec::new(), None, None))));
                    }
                    SyncMessage::AcceptedAnswerV1 { id, through, rows } => {
                        let client = inner.client.as_mut().expect("client");
                        if through >= LOCAL_BASE {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "accepted answer has an optimistic cut",
                            ));
                        }
                        if client.accepted_answers.contains(&id)
                            && !client.overlay_answers.contains(&id)
                        {
                            client.known = client.known.max(through);
                            if client.answers.contains_key(&id) {
                                answers.push((id, Ok((through, rows, None, None))));
                            }
                        }
                    }
                    SyncMessage::Answer { id, through, rows } => {
                        let client = inner.client.as_mut().expect("client");
                        if client.union_answers.contains(&id) {
                            answers.push((
                                id,
                                Err(Error::new(
                                    ErrorCode::NotObserved,
                                    "ordinary row IDs cannot cover a UNION read",
                                )),
                            ));
                            continue;
                        }
                        if client.accepted_answers.contains(&id)
                            || client.overlay_answers.contains(&id)
                        {
                            if client.answers.contains_key(&id) {
                                answers.push((
                                    id,
                                    Err(Error::new(
                                        ErrorCode::NotObserved,
                                        "ordinary cache answer cannot cover an accepted read",
                                    )),
                                ));
                            }
                            continue;
                        }
                        client.known = client.known.max(through);
                        if client.answers.contains_key(&id) {
                            answers.push((id, Ok((through, rows, None, None))));
                        }
                    }
                    SyncMessage::UnionAnswerV1 {
                        id,
                        through,
                        occurrences,
                    } => {
                        let client = inner.client.as_mut().expect("client");
                        if client
                            .remote_ids
                            .get(&id)
                            .and_then(|key| client.remote.get(key))
                            .is_some_and(|remote| remote.union.is_some())
                        {
                            union_updates.push(unions::LiveUpdate::Receipt {
                                id,
                                through,
                                occurrences,
                            });
                            continue;
                        }
                        if !client.answers.contains_key(&id) {
                            continue;
                        }
                        if !client.union_answers.contains(&id)
                            || client.overlay_answers.contains(&id)
                            || through >= LOCAL_BASE
                        {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "UNION answer has a different request kind or invalid cut",
                            ));
                        }
                        if !client.scoped_answers.contains(&id)
                            && !client.accepted_answers.contains(&id)
                        {
                            client.known = client.known.max(through);
                        }
                        let rows = occurrences
                            .iter()
                            .map(|occurrence| occurrence.row)
                            .collect();
                        answers.push((id, Ok((through, rows, None, Some(occurrences)))));
                    }
                    SyncMessage::Refused { id, code, message } => {
                        let client = inner.client.as_mut().expect("client");
                        let error = Error::new(code, message);
                        if let Some(slot) = client.catalogue_answers.get_mut(&id) {
                            *slot = Some(Err(error));
                        } else if client.answers.contains_key(&id) {
                            answers.push((id, Err(error)));
                        } else if let Some(key) = client.remote_ids.get(&id)
                            && let Some(remote) = client.remote.get(key)
                        {
                            *remote.refused.borrow_mut() = Some(error);
                        }
                    }
                    SyncMessage::FateV1 {
                        node,
                        tx,
                        through,
                        result,
                    } => {
                        if through >= LOCAL_BASE {
                            return Err(Error::new(
                                ErrorCode::Protocol,
                                "write fate has an optimistic authority sequence",
                            ));
                        }
                        let id = (node, tx);
                        let client = inner.client.as_ref().expect("client");
                        if client.relay_finished.contains(&id) {
                            continue;
                        }
                        let relayed = client.relay_uploads.contains_key(&id);
                        let pending = client
                            .pending
                            .iter()
                            .find(|pending| pending.node == node && pending.tx == tx);
                        let own = pending.is_some();
                        let owner_managed = pending.is_some_and(|pending| pending.owner_managed);
                        // Unknown or repeated receipts cannot manufacture a write.
                        if !relayed && !own {
                            continue;
                        }
                        let accepted = result.is_ok();
                        if own && accepted {
                            inner.keep_unlisted_own(id, through)?;
                        }
                        if relayed {
                            // Track attempted receipts too: a storage failure before
                            // fate insertion must leave this unit ready to resend.
                            inner
                                .client
                                .as_mut()
                                .expect("client")
                                .relay_finished
                                .insert(id);
                        }
                        inner
                            .conn
                            .execute_batch("SAVEPOINT jazz_write_fate")
                            .map_err(storage)?;
                        let persisted = inner
                            .record_fate(node, tx, &result)
                            .and_then(|()| {
                                inner.record_settled(node, tx, accepted.then_some(through))
                            })
                            .and_then(|()| {
                                if own && (inner.scope_isolated_owner || inner.non_durable) {
                                    inner.finish_owner_input_rows(id)?;
                                }
                                if relayed {
                                    inner.finish_relay_upload(id)
                                } else if !owner_managed {
                                    inner.forget_pending(id)
                                } else {
                                    Ok(())
                                }
                            });
                        if let Err(error) = persisted {
                            inner
                                .rollback_savepoint("jazz_write_fate")
                                .map_err(storage)?;
                            return Err(error);
                        }
                        inner
                            .conn
                            .execute_batch("RELEASE jazz_write_fate")
                            .map_err(storage)?;
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        if own {
                            client.owner_stored.remove(&id);
                            client.initialization_uploads.remove(&id);
                            client.owner_inputs.remove(&id);
                            client
                                .pending
                                .retain(|pending| pending.node != node || pending.tx != tx);
                            client
                                .sent
                                .retain(|message| relay::upload_id(message) != Some(id));
                            client.fates.insert(id, result.map_err(from_wire_error));
                            if let Some(locators) = client.pushed.remove(&id) {
                                crate::large::mark_pushed(&inner.conn, &locators)?;
                            }
                            rebase = true;
                        }
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
            return Ok((answers, union_updates));
        }
        client.persisted_known = known;
        inner.persist_meta("known", known)?;
        Ok((answers, union_updates))
    }

    fn client_send(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if let Some(error) = &inner
            .client
            .as_ref()
            .expect("client")
            .initialization_recovery_error
        {
            return Err(error.clone());
        }
        let demand = inner.chunks.take_demand();
        let client = inner.client.as_mut().expect("client");
        client.wanted.extend(demand);
        if client.transport.is_none() {
            return Ok(());
        }
        let client = inner.client.as_ref().expect("client");
        // Once this foreground has admitted a private owner, a reconnect must
        // acknowledge that owner before any retry payload leaves the tab.
        let uploads_ready = !inner.non_durable
            || client.owner_node.is_none()
            || (client.owner_ready == Some(inner.owner.jazz.version_id())
                && client.upstream_schema == client.owner_ready);
        let mut relayed = client
            .relay_uploads
            .iter()
            .filter(|(_, upload)| uploads_ready && !upload.sent)
            .map(|(id, upload)| (*id, upload.order, upload.schema, upload.message.clone()))
            .collect::<Vec<_>>();
        relayed.sort_by_key(|(_, order, _, _)| *order);
        let upstream = client.upstream_schema;
        let scoped = inner.scope_isolated_owner;
        if scoped
            && upstream != Some(inner.owner.jazz.version_id())
            && (!client.unsent.is_empty() || !client.relay_uploads.is_empty())
        {
            return Err(unsupported(
                "owner uploads after a schema change without upstream reconnection",
            ));
        }
        let position = |message: &SyncMessage| -> Result<u64, Error> {
            if !scoped {
                return Ok(0);
            }
            let (node, tx) = relay::upload_id(message).ok_or_else(|| {
                Error::new(
                    ErrorCode::Storage,
                    "owner outbox contains a non-commit unit",
                )
            })?;
            inner
                .local_position(TxId::new(TxTime(tx), NodeUuid::from_bytes(node)))?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::Storage,
                        "owner upload has no publication position",
                    )
                })
        };
        let mut forwarded = Vec::new();
        let mut ordered = Vec::new();
        for (_, _, schema, message) in &relayed {
            if Some(*schema) != upstream {
                return Err(unsupported(
                    "recovering relay uploads under a different upstream schema",
                ));
            }
            let mut unit = inner.relay_pushes(message)?;
            unit.push(message.clone());
            if scoped {
                ordered.push((position(message)?, unit));
            } else {
                forwarded.extend(unit);
            }
        }
        let pushes = inner
            .client
            .as_ref()
            .expect("client")
            .unsent
            .iter()
            .filter(|_| uploads_ready)
            .map(|commit| Ok((position(commit)?, inner.pushes_for(commit)?)))
            .collect::<Result<Vec<_>, Error>>()?;
        let client = inner.client.as_mut().expect("client");
        let mut out = std::mem::take(&mut client.outbox);
        if !uploads_ready {
            out.retain(|message| {
                if matches!(
                    message,
                    SyncMessage::Commit { .. }
                        | SyncMessage::PushChunks { .. }
                        | SyncMessage::OwnerInitializationPublishV1 { .. }
                ) {
                    client.outbox.push(message.clone());
                    false
                } else {
                    true
                }
            });
        }
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
        let commits = if uploads_ready {
            std::mem::take(&mut client.unsent)
        } else {
            Vec::new()
        };
        // Each commit's new large-value nodes go ahead of it.
        let client = inner.client.as_mut().expect("client");
        for (id, _, _, _) in relayed {
            client.relay_uploads.get_mut(&id).expect("queued").sent = true;
        }
        let transport = client.transport.as_mut().expect("transport");
        for commit in &commits {
            if !client
                .sent
                .iter()
                .any(|m| relay::upload_id(m) == relay::upload_id(commit))
            {
                client.sent.push(commit.clone());
            }
        }
        for (commit, (position, (locators, mut batches))) in commits.into_iter().zip(pushes) {
            if let Some(id) = relay::upload_id(&commit)
                && !locators.is_empty()
            {
                client.pushed.insert(id, locators);
            }
            batches.push(commit);
            if scoped {
                ordered.push((position, batches));
            } else {
                out.extend(batches);
            }
        }
        if scoped {
            ordered.sort_by_key(|(position, _)| *position);
            out.extend(ordered.into_iter().flat_map(|(_, unit)| unit));
        } else {
            out.extend(forwarded);
        }
        // A full outbound queue keeps the rest, in order, for the flush its
        // drain wakes.
        let mut out = out.into_iter();
        while let Some(message) = out.next() {
            let message = match relay::upload_id(&message)
                .and_then(|id| client.initialization_uploads.get(&id))
            {
                Some(nonce) => SyncMessage::OwnerInitializationPublishV1 {
                    nonce: *nonce,
                    commit: encode(&message),
                },
                None => message,
            };
            match transport.send(message.clone()) {
                Ok(()) => {}
                Err(TransportError::Backpressure) => {
                    let mut held: Vec<SyncMessage> = std::iter::once(message).chain(out).collect();
                    held.append(&mut client.outbox);
                    client.outbox = held;
                    break;
                }
                Err(e) => return Err(Error::new(ErrorCode::Storage, format!("{e:?}"))),
            }
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
        self.inner.borrow_mut().replay_pending(&BTreeSet::new())
    }
}

impl Inner {
    /// Validate an owner envelope before its answer, while later authority
    /// fates may still invalidate ordinary speculative descendants normally.
    fn replay_pending(&mut self, required: &BTreeSet<UploadId>) -> Result<(), Error> {
        let Some(client) = self.client.as_mut() else {
            return Ok(());
        };
        let shadow = std::mem::take(&mut client.shadow);
        let pending: Vec<(UploadId, u64, Vec<StagedOp>)> = client
            .pending
            .iter()
            .map(|p| ((p.node, p.tx), p.seq, p.ops.clone()))
            .collect();
        let keep: BTreeSet<(String, RowUuid, Vec<u8>)> = client
            .pending
            .iter()
            .flat_map(|p| p.rows.iter().cloned())
            .collect();
        for ((table, row, key), image) in &shadow {
            match image {
                Some(image) => {
                    self.drop_row(table, *row, key, image.seq)?;
                    self.put_image(image, false)?;
                }
                None => self.drop_row(table, *row, key, 0)?,
            }
        }
        for (id, tx, ops) in pending {
            self.conn
                .execute_batch("SAVEPOINT replay")
                .map_err(storage)?;
            let replayed = ops.iter().try_for_each(|op| {
                if self.non_durable {
                    let who = op.author().map_or(Who::Local, Who::Attributed);
                    self.apply_replayed_as(op, who, tx).map(drop)
                } else {
                    self.apply(op, tx).map(drop)
                }
            });
            match replayed {
                Ok(()) => self.conn.execute_batch("RELEASE replay").map_err(storage)?,
                Err(error) => {
                    self.rollback_savepoint("replay").map_err(storage)?;
                    if required.contains(&id) {
                        self.client.as_mut().expect("client").shadow = shadow;
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            format!("owner input cannot be replayed as a complete unit: {error}"),
                        ));
                    }
                }
            }
        }
        let client = self.client.as_mut().expect("client");
        client.shadow = shadow
            .into_iter()
            .filter(|(row, _)| keep.contains(row))
            .collect();
        self.persist_shadow()
    }

    fn release_session_reads(&mut self, released: ReleasedReads) {
        for (query, binding, include_deleted, scope) in released.subscriptions {
            self.release_remote(&query, &binding, include_deleted, &scope);
        }
        if let Some(client) = self.client.as_mut() {
            for id in released.fetches {
                client.cancel_fetch(id);
            }
        }
    }

    /// The schema session `index` reads and writes by, when it declared an
    /// admitted version other than the current one: that version's names,
    /// governed by the current schema's policies. `None` is the current
    /// schema (also for a version this store has not admitted).
    /// Whether session `index` declared a schema version this store has
    /// not admitted.
    fn session_unadmitted(&self, index: usize) -> bool {
        self.sessions[index].version.is_some_and(|version| {
            version != self.owner.jazz.version_id()
                && !self
                    .admitted
                    .iter()
                    .any(|admitted| admitted.version_id() == version)
        })
    }

    /// The schema session `index` declared, governed by the current
    /// schema's policies. A declared version pins its subscriptions even
    /// while it is the current one, so they keep reading by its names after
    /// a newer version becomes current.
    pub(crate) fn session_schema(&mut self, index: usize) -> Option<Rc<Schema>> {
        let version = self.sessions[index].version?;
        if let Some((owner, schema)) = &self.sessions[index].schema
            && Rc::ptr_eq(owner, &self.owner)
        {
            return Some(schema.clone());
        }
        let schema = self.governed(version)?;
        self.sessions[index].schema = Some((self.owner.clone(), schema.clone()));
        Some(schema)
    }

    /// Admitted `version` under its stored names, governed by the current
    /// schema's policies.
    pub(crate) fn governed(&self, version: crate::ids::SchemaVersionId) -> Option<Rc<Schema>> {
        let jazz = if version == self.owner.jazz.version_id() {
            &self.owner.jazz
        } else {
            self.admitted
                .iter()
                .find(|admitted| admitted.version_id() == version)?
        };
        Some(Rc::new(
            self.named(jazz).with_policies_of(self.owner.clone()),
        ))
    }

    /// Apply one server image; returns whether a rebase is needed (the row
    /// is shadowed by a pending write).
    fn receive_image(&mut self, image: Image) -> Result<bool, Error> {
        let row = RowUuid::from_bytes(image.id);
        // A version the server ships is one it lists for this replica again.
        self.readmit(&image.table, row, &image.key)?;
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
        self.accept_subscriber_with_claims(transport, identity, None)
    }

    /// [`Self::accept_subscriber`] for a connection that authenticated
    /// with its own claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: Option<BTreeMap<String, Value>>,
    ) -> SubscriberId {
        let mut inner = self.inner.borrow_mut();
        let mut session = Session::new(transport, identity);
        session.claims = claims;
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
            let released = session.close(&dropped);
            inner.release_session_reads(released);
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
            if self.inner.borrow().sessions[index].catalogue_pending {
                self.queue_catalogue_metadata(index)?;
            }
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
            // Writes held back by the ingress limit, in order. Reads and
            // other messages that change no rows overtake them, so a client
            // uploading a large value can still read.
            let mut held: VecDeque<SyncMessage> = VecDeque::new();
            while let Some(message) = inbound.pop_front() {
                if !held.is_empty() && !overtakes_writes(&message) {
                    held.push_back(message);
                    continue;
                }
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
                        held.push_back(message);
                        continue;
                    }
                    session.ingress.1 += bytes;
                }
                // A session on a schema version this server has not
                // admitted (a draft awaiting its lens) waits: serving it
                // under another version would expose rows it may not read
                // that way.
                if !matches!(message, SyncMessage::Schema { .. }) {
                    let inner = self.inner.borrow_mut();
                    if inner.session_unadmitted(index) {
                        inbound.push_front(message);
                        held.extend(inbound);
                        inbound = VecDeque::new();
                        break;
                    }
                }
                // A read the server cannot serve is refused to the client,
                // which would otherwise wait on it.
                let read = match &message {
                    SyncMessage::Subscribe { id, .. }
                    | SyncMessage::SubscribeReadablePolicyV1 { id, .. }
                    | SyncMessage::ScopedUnionSubscribeV1 { id, .. }
                    | SyncMessage::Fetch { id, .. }
                    | SyncMessage::ScopedFetchV1 { id, .. }
                    | SyncMessage::AcceptedFetchV1 { id, .. }
                    | SyncMessage::AcceptedOverlayCheckV1 { id, .. }
                    | SyncMessage::CatalogueFetchV1 { id, .. } => Some(*id),
                    _ => None,
                };
                match (self.serve(index, message), read) {
                    (Ok(()), _) => {}
                    (Err(error), Some(id)) => {
                        self.inner.borrow_mut().sessions[index]
                            .outbox
                            .push(SyncMessage::Refused {
                                id,
                                code: error.code,
                                message: error.message,
                            });
                    }
                    (Err(error), None) => return Err(error),
                }
            }
            if !held.is_empty() {
                self.inner.borrow_mut().sessions[index].backlog = held;
            }
        }
        self.progress_scoped_fetches();
        self.progress_catalogue_fetches();
        self.progress_relay_uploads()?;
        self.progress_owner_input_fates()?;
        self.progress_owner_durability();
        self.notify()?;
        let mut inner = self.inner.borrow_mut();
        // A relay's optimistic sequence is not an authority cut. Even its
        // owner-local answer carries at most the accepted prefix it has seen.
        let through = inner
            .client
            .as_ref()
            .map_or(inner.seq, |client| client.known);
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
                    let cell = Sql::from(cell);
                    // An array cell matches a reverse array relation's
                    // watch on each element it holds.
                    for element in array_elements(&cell).unwrap_or_default() {
                        keys.push(wk((table.clone(), column.clone(), watch_key(&element))));
                    }
                    keys.push(wk((table.clone(), column.clone(), watch_key(&cell))));
                }
            }
            written_keys.push(((table, row, key), keys));
        }
        let written_tables: BTreeSet<&str> = written_keys
            .iter()
            .map(|((table, ..), _)| table.as_str())
            .collect();
        let mut written_values: HashMap<(String, String), BTreeSet<String>> = HashMap::new();
        let dropped = inner.fan.dropped.clone();
        let mut scoped_updates = Vec::new();
        for index in 0..sessions {
            if !inner.sessions[index].open {
                continue;
            }
            let session_identity = inner.sessions[index].identity;
            let previous_claims = inner.sessions[index]
                .claims
                .clone()
                .map(|claims| inner.claims.insert(session_identity, claims));
            let send_result = (|| -> Result<(), Error> {
                let mut policy_tables: HashMap<String, BTreeSet<String>> = HashMap::new();
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
                let refused = inner.sessions[index]
                    .relayed
                    .iter()
                    .filter_map(|(id, (query, binding, deleted, scope))| {
                        let key = scoped_remote_key(query, binding, *deleted, scope);
                        let error = inner
                            .client
                            .as_ref()?
                            .remote
                            .get(&key)?
                            .refused
                            .borrow()
                            .clone()?;
                        Some((*id, error))
                    })
                    .collect::<Vec<_>>();
                for (id, error) in refused {
                    inner.refuse_subscription(index, id, error);
                }
                if recheck.is_empty()
                    && inner.sessions[index].idle()
                    && !written_keys.iter().any(|(_, keys)| {
                        keys.iter().any(|k| inner.sessions[index].watch.contains(k))
                    })
                {
                    return Ok(());
                }
                let identity = inner.sessions[index].identity;
                let policy = (identity != AuthorSubject::SYSTEM).then_some(identity);
                let names = inner
                    .session_schema(index)
                    .unwrap_or_else(|| inner.owner.clone());
                let mut touched = std::mem::take(&mut inner.sessions[index].touched);
                let probed = std::mem::take(&mut inner.sessions[index].probed);
                let revalidate = std::mem::take(&mut inner.sessions[index].revalidate);
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
                        (*id, q.clone(), g.clone(), *windowed || !*opened)
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
                    // A relay's result settles once its upstream answered.
                    let answered = events.iter().any(|event| {
                        matches!(event, SubscriptionEvent::Delta { settled: true, .. })
                    });
                    let global = events.iter().any(|event| {
                    matches!(event, SubscriptionEvent::Delta { settled: true, tier, .. } if *tier == DurabilityTier::Global)
                });
                    let union = group
                        .upgrade()
                        .is_some_and(|group| crate::db::relation::is_union(group.borrow().query()));
                    // A UNION receipt describes the group's current rows. A
                    // later local preview must not inherit global settlement
                    // from an earlier event drained in the same batch.
                    let (answered, global) = if union {
                        events
                            .iter()
                            .rev()
                            .find_map(|event| match event {
                                SubscriptionEvent::Delta { settled, tier, .. } => {
                                    Some((*settled, *settled && *tier == DurabilityTier::Global))
                                }
                                _ => None,
                            })
                            .unwrap_or((false, false))
                    } else {
                        (answered, global)
                    };
                    let coverage_through = if union && global {
                        group
                            .upgrade()
                            .and_then(|group| group.borrow().union_cut())
                            .unwrap_or(through)
                    } else {
                        through
                    };
                    if let Some(prepared) = inner.sessions[index].scoped_unions.get(&id).cloned() {
                        if !answered || !global {
                            continue;
                        }
                        let Some(group) = group.upgrade() else {
                            continue;
                        };
                        let group = group.borrow();
                        let roots = group.rows.iter().map(|row| *row.id.as_bytes()).collect();
                        let scope = prepared.request_claims.clone().expect("scoped live query");
                        let (prepared, evidence) = if inner.client.is_some() {
                            let Some((cut, occurrences, evidence)) = inner
                                .live_union(
                                    &prepared,
                                    group.includes_deleted(),
                                    &Some(scope.clone()),
                                )
                                .and_then(|live| live.scoped_coverage())
                            else {
                                continue;
                            };
                            if cut != coverage_through {
                                continue;
                            }
                            (
                                unions::exact(&prepared, &occurrences)?,
                                ScopedEvidence::Supplied(evidence),
                            )
                        } else {
                            (prepared, ScopedEvidence::Collect)
                        };
                        scoped_updates.push(scoped_live::Update {
                            index,
                            id,
                            prepared,
                            identity: scope.0,
                            through: coverage_through,
                            include_deleted: group.includes_deleted(),
                            roots,
                            evidence,
                        });
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
                                images.extend(inner.joined_images(
                                    table,
                                    joined,
                                    &mut history_seqs,
                                )?);
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
                            let unchanged = !union
                                && since.is_some()
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
                                let mut found =
                                    inner.images_where(&table, false, ID_AT_SEQ, &[&id, &seq])?;
                                if union && found.is_empty() {
                                    found = inner.images_where(
                                        &table,
                                        true,
                                        ID_AT_SEQ,
                                        &[&id, &seq],
                                    )?;
                                }
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
                                touched.insert((
                                    image.table.clone(),
                                    gone.row_uuid,
                                    image.key.clone(),
                                ));
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
                    if let Some(prepared) = inner.sessions[index].query_support.get(&id).cloned() {
                        let roots = group
                            .upgrade()
                            .map(|group| group.borrow().rows.clone())
                            .unwrap_or_default();
                        let inputs = if union {
                            inner.union_inputs(
                                &prepared,
                                policy,
                                &roots,
                                if global { coverage_through } else { inner.seq },
                            )
                        } else {
                            inner
                                .inheritance_inputs(&prepared, policy, &roots, &ReadSpec::default())
                                .and_then(|mut inputs| {
                                    inputs.extend(inner.reachability_inputs(
                                        &prepared,
                                        policy,
                                        &roots.iter().map(|row| row.id).collect::<Vec<_>>(),
                                        &ReadSpec::default(),
                                    )?);
                                    Ok(inputs)
                                })
                        };
                        match inputs {
                            Ok(inputs) => {
                                for image in inputs {
                                    watch.insert(wk((
                                        image.table.clone(),
                                        "id".to_owned(),
                                        watch_key(&Sql::Blob(image.id.to_vec())),
                                    )));
                                    held_rows.entry(image.table.clone()).or_default().insert(
                                        (RowUuid::from_bytes(image.id), image.key.clone()),
                                        true,
                                    );
                                    images.push(image);
                                }
                            }
                            Err(error) => {
                                images.truncate(first);
                                inner.refuse_subscription(index, id, error);
                                continue;
                            }
                        }
                    }
                    if let Some(prepared) = inner.sessions[index].readable_policy.get(&id).cloned()
                    {
                        let inputs = inner.subscription_policy_inputs(&prepared, policy, &group);
                        match inputs {
                            Ok(inputs) => {
                                for image in inputs {
                                    let row = RowUuid::from_bytes(image.id);
                                    watch.insert(wk((
                                        image.table.clone(),
                                        "id".to_owned(),
                                        watch_key(&Sql::Blob(image.id.to_vec())),
                                    )));
                                    held_rows
                                        .entry(image.table.clone())
                                        .or_default()
                                        .insert((row, image.key.clone()), true);
                                    images.push(image);
                                }
                            }
                            Err(error) => {
                                images.truncate(first);
                                inner.refuse_subscription(index, id, error);
                                continue;
                            }
                        }
                    }
                    // The opening and every window delta list the result's
                    // rows, so the client can drop stale cached ones.
                    let window = (list && !union)
                        .then(|| group.upgrade())
                        .flatten()
                        .map(|group| {
                            group
                                .borrow()
                                .rows
                                .iter()
                                .map(|row| *row.id.0.as_bytes())
                                .collect()
                        });
                    if answered {
                        if union {
                            let occurrences = group
                                .upgrade()
                                .map(|group| unions::occurrences(&group.borrow().rows))
                                .transpose()?
                                .unwrap_or_default();
                            settled.push(SyncMessage::UnionAnswerV1 {
                                id,
                                through: coverage_through,
                                occurrences,
                            });
                        }
                        settled.push(SyncMessage::Settled {
                            id,
                            through: coverage_through,
                            window,
                            global,
                        });
                        // The settled opening listed the result.
                        if let Some((.., opened)) = inner.sessions[index].subs.get_mut(&id) {
                            *opened = true;
                        }
                    }
                }
                // Rows that left a result, and rows a commit wrote: the current
                // image if the client may read it, else an eviction.
                for (table, row, key) in touched {
                    match inner.current_image(&table, row, &key)? {
                        Some(image) if readable(&inner, &table, row, &key, policy)? => {
                            images.push(image)
                        }
                        Some(_)
                            if !revalidate
                                && !probed.contains(&(table.clone(), row, key.clone())) =>
                        {
                            evictions.push(SyncMessage::Unlist {
                                table,
                                id: *row.0.as_bytes(),
                                key,
                            })
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
                    let current = inner.current_image(&table, row, &key)?;
                    if policy.is_some() && current.is_some() {
                        // A writer retains accepted rows even without ever reading
                        // them. Watch their permission dependencies just like a
                        // fetched row, so a later cross-row revocation reaches the
                        // cache before a fresh subscription can settle over it.
                        let can = readable(&inner, &table, row, &key, policy)?;
                        held_rows
                            .entry(table.clone())
                            .or_default()
                            .insert((row, key.clone()), can);
                    }
                    let superseded = current.is_some_and(|current| current.seq != seq);
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
                    if let SyncMessage::Evict { table, id, .. }
                    | SyncMessage::Unlist { table, id, .. } = eviction
                    {
                        watch.remove(&wk((
                            table.clone(),
                            "id".to_owned(),
                            watch_key(&Sql::Blob(id.to_vec())),
                        )));
                    }
                }
                let (owner_inputs, input_rows) =
                    match inner.owner_query_inputs(index, &mut images, &mut evictions) {
                        Ok(frame) => frame,
                        Err(error) => {
                            // This peer cannot hold a coherent projection. Refuse its
                            // reads and release its interests; do not poison the owner
                            // or publish partial input frames to another reader.
                            let session = &mut inner.sessions[index];
                            let mut ids = session.subs.keys().copied().collect::<BTreeSet<_>>();
                            ids.extend(session.outbox.iter().filter_map(|message| match message {
                                SyncMessage::Answer { id, .. }
                                | SyncMessage::AcceptedAnswerV1 { id, .. }
                                | SyncMessage::ScopedEvidenceAnswerV1 { id, .. }
                                | SyncMessage::AcceptedScopedEvidenceAnswerV1 { id, .. } => {
                                    Some(*id)
                                }
                                _ => None,
                            }));
                            for id in ids {
                                let _ = session.transport.send(SyncMessage::Refused {
                                    id,
                                    code: error.code,
                                    message: error.message.clone(),
                                });
                            }
                            let released = session.close(&dropped);
                            inner.release_session_reads(released);
                            return Ok(());
                        }
                    };
                for (table, row, key) in input_rows {
                    watch.insert(wk((
                        table.clone(),
                        "id".to_owned(),
                        watch_key(&Sql::Blob(row.as_bytes().to_vec())),
                    )));
                    held_rows.entry(table).or_default().insert((row, key), true);
                }
                let witnesses = inner.content_witnesses(&mut images, &mut history_seqs)?;
                let accepted_rows = inner.accepted_images(&images)?;
                let mut out = std::mem::take(&mut inner.sessions[index].stale);
                let origins = inner.origins(&history_seqs)?;
                if !origins.is_empty() {
                    out.push(SyncMessage::Origins { commits: origins });
                }
                if !witnesses.is_empty() {
                    out.push(SyncMessage::ContentWitnessesV1 { witnesses });
                }
                if !accepted_rows.is_empty() {
                    out.push(SyncMessage::AcceptedRowsV1 {
                        rows: accepted_rows,
                    });
                }
                let session = &mut inner.sessions[index];
                session.watch = watch;
                session.held = held_rows;
                if !images.is_empty() {
                    out.push(SyncMessage::Rows { images });
                }
                out.extend(evictions);
                out.extend(owner_inputs);
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
                        let released = session.close(&dropped);
                        inner.release_session_reads(released);
                        break;
                    }
                }
                Ok(())
            })();
            if let Some(previous) = previous_claims {
                match previous {
                    Some(claims) => {
                        inner.claims.insert(session_identity, claims);
                    }
                    None => {
                        inner.claims.remove(&session_identity);
                    }
                }
            }
            send_result?;
        }
        drop(inner);
        self.send_scoped_union_updates(scoped_updates)
    }

    #[allow(clippy::too_many_arguments)]
    fn serve_fetch(
        &self,
        index: usize,
        id: u64,
        query: &Query,
        prepared: &PreparedQuery,
        policy: Option<AuthorSubject>,
        at: Option<u64>,
        include_deleted: bool,
        accepted: bool,
        expected: Option<&[[u8; 16]]>,
        evidence: Option<ScopedEvidence>,
    ) -> Result<(), Error> {
        let mut inner = self.inner();
        // A history read ships versions of its source table.
        let union = crate::db::relation::is_union(&prepared.query);
        if union {
            Self::remote_fetch_shape(&prepared.query)?;
            inner.check_union_shape(prepared)?;
        }
        let history = history_source_table(&query.table);
        let table = inner
            .schema
            .stored_table(history.unwrap_or(&query.table))?
            .to_owned();
        let read = ReadSpec {
            cut: at.map(Cut::at),
            permission_cut: matches!(&evidence, Some(ScopedEvidence::Collect))
                .then_some(at.unwrap_or(inner.seq)),
            include_deleted,
            ..ReadSpec::default()
        };
        inner.check_reachability_shape(prepared)?;
        inner.check_inheritance_shape(prepared)?;
        let publish_evidence = inner.sessions[index].identity == AuthorSubject::SYSTEM;
        if matches!(&evidence, Some(ScopedEvidence::Collect)) && !publish_evidence {
            return Err(rejected(
                "scoped permission evidence requires a trusted backend connection",
            ));
        }
        let rows = match &evidence {
            Some(ScopedEvidence::Supplied(evidence)) => {
                inner.run_scoped_evidence(prepared, policy, &read, evidence)?
            }
            Some(ScopedEvidence::Collect) => {
                let mut budget = scoped_reads::MAX_EVIDENCE;
                inner
                    .with_candidate_sql_budget(&mut budget, || inner.run(prepared, policy, &read))?
            }
            None => inner.run(prepared, policy, &read)?,
        };
        if let Some(expected) = expected {
            let actual: Vec<_> = rows.iter().map(|row| *row.id.0.as_bytes()).collect();
            if actual != expected {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "relay lacks the exact accepted query result",
                ));
            }
        }
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
                let mut found = inner.images_where(&table, false, ID_AT_SEQ, &[&rid, &seq])?;
                if found.is_empty() {
                    found = inner.images_where(&table, true, ID_AT_SEQ, &[&rid, &seq])?;
                }
                images.extend(found);
            }
        }
        let scoped_evidence = match evidence {
            Some(ScopedEvidence::Collect) => {
                if inner.client.is_some() {
                    return Err(unsupported(
                        "collecting scoped permission evidence away from the current authority",
                    ));
                }
                let through = at.unwrap_or(inner.seq);
                let mut budget = scoped_reads::MAX_EVIDENCE;
                images.extend(if union {
                    inner.union_inputs_with_budget(prepared, policy, &rows, through, &mut budget)?
                } else {
                    inner.scoped_query_images(prepared, policy, &images, through, &mut budget)?
                });
                let evidence = inner.permission_evidence_with_budget(
                    prepared,
                    policy,
                    &images,
                    through,
                    &mut budget,
                )?;
                for row in &evidence {
                    images.extend(inner.scoped_evidence_image(row)?);
                }
                Some(evidence)
            }
            Some(ScopedEvidence::Supplied(evidence)) if publish_evidence => {
                for row in &evidence {
                    images.extend(inner.scoped_evidence_image(row)?);
                }
                Some(evidence)
            }
            None | Some(ScopedEvidence::Supplied(_)) => None,
        };
        let roots = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        let mut reachability_inputs = inner.reachability_inputs(prepared, policy, &roots, &read)?;
        reachability_inputs.extend(inner.inheritance_inputs(prepared, policy, &rows, &read)?);
        if union && scoped_evidence.is_none() {
            reachability_inputs.extend(inner.union_inputs(
                prepared,
                policy,
                &rows,
                at.unwrap_or(inner.seq),
            )?);
        }
        // Scoped evidence already contains the exact query and policy inputs.
        // Re-following them against the current cache would mix cuts and claims.
        let support = if scoped_evidence.is_some() || union {
            Vec::new()
        } else {
            inner.support_of(query)?
        };
        let mut extra = Vec::new();
        let mut watch = std::mem::take(&mut inner.sessions[index].watch);
        let mut held = std::mem::take(&mut inner.sessions[index].held);
        // The replica keeps current rows it fetched: like supporting
        // rows, a policy dependency's commit re-checks them.
        if policy.is_some() && at.is_none() && history.is_none() {
            for image in images.iter().filter(|image| image.table == table) {
                held.entry(table.clone())
                    .or_default()
                    .insert((RowUuid::from_bytes(image.id), image.key.clone()), true);
            }
        }
        for image in &images {
            inner.follow_support(&support, image, policy, &mut extra, &mut watch, &mut held)?;
        }
        images.extend(extra);
        for image in &reachability_inputs {
            if at.is_none() {
                held.entry(image.table.clone())
                    .or_default()
                    .insert((RowUuid::from_bytes(image.id), image.key.clone()), true);
            }
        }
        images.extend(reachability_inputs);
        for image in &images {
            watch.insert(wk((
                image.table.clone(),
                "id".to_owned(),
                watch_key(&Sql::Blob(image.id.to_vec())),
            )));
        }
        let (owner_inputs, input_rows) = if at.is_none() && history.is_none() && !accepted {
            match inner.owner_query_inputs(index, &mut images, &mut Vec::new()) {
                Ok(frame) => frame,
                Err(error) => {
                    inner.sessions[index].watch = watch;
                    inner.sessions[index].held = held;
                    return Err(error);
                }
            }
        } else {
            if inner.scope_isolated_owner && images.iter().any(|image| image.seq >= LOCAL_BASE) {
                return Err(unsupported("historical pending owner query inputs"));
            }
            (Vec::new(), Vec::new())
        };
        for (table, row, key) in input_rows {
            watch.insert(wk((
                table.clone(),
                "id".to_owned(),
                watch_key(&Sql::Blob(row.as_bytes().to_vec())),
            )));
            held.entry(table).or_default().insert((row, key), true);
        }
        let witnesses = inner.content_witnesses(&mut images, &mut seqs)?;
        let accepted_rows = inner.accepted_images(&images)?;
        let origins = inner.origins(&seqs)?;
        // An owner's local publication aliases never qualify an authority
        // snapshot, including for a current (non-historical) fetch response.
        let through = at.unwrap_or_else(|| {
            inner
                .client
                .as_ref()
                .map_or(inner.seq, |client| client.known)
        });
        let session = &mut inner.sessions[index];
        session.watch = watch;
        session.held = held;
        if !origins.is_empty() {
            session
                .outbox
                .push(SyncMessage::Origins { commits: origins });
        }
        if !witnesses.is_empty() {
            session
                .outbox
                .push(SyncMessage::ContentWitnessesV1 { witnesses });
        }
        if !accepted_rows.is_empty() {
            session.outbox.push(SyncMessage::AcceptedRowsV1 {
                rows: accepted_rows,
            });
        }
        if !images.is_empty() {
            session.outbox.push(SyncMessage::Rows { images });
        }
        session.outbox.extend(owner_inputs);
        let occurrences = union.then(|| unions::occurrences(&rows)).transpose()?;
        let rows = rows.iter().map(|r| *r.id.0.as_bytes()).collect();
        if let Some(occurrences) = occurrences {
            session.outbox.push(SyncMessage::UnionAnswerV1 {
                id,
                through,
                occurrences,
            });
            // Scoped coverage needs both semantic occurrences and the exact
            // permission closure at the same cut. Neither frame alone answers it.
            if let Some(evidence) = scoped_evidence {
                session
                    .outbox
                    .push(evidence_answer(accepted, id, through, rows, evidence));
            } else if accepted {
                session
                    .outbox
                    .push(SyncMessage::AcceptedAnswerV1 { id, through, rows });
            }
        } else {
            session
                .outbox
                .push(if let Some(evidence) = scoped_evidence {
                    evidence_answer(accepted, id, through, rows, evidence)
                } else if accepted {
                    SyncMessage::AcceptedAnswerV1 { id, through, rows }
                } else {
                    SyncMessage::Answer { id, through, rows }
                });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn forward_fetch(
        &self,
        index: usize,
        id: u64,
        prepared: PreparedQuery,
        identity: AuthorSubject,
        include_deleted: bool,
        at: Option<u64>,
        accepted: bool,
        scoped_evidence: bool,
        overlay_ops: Option<Vec<WireOp>>,
        ordinary: bool,
    ) -> Result<(), Error> {
        let deadline = self.read_deadline(
            (accepted || ordinary).then(|| Instant::now() + std::time::Duration::from_secs(15)),
        );
        let mut inner = self.inner();
        let owner = inner.owner.clone();
        let downstream_version = inner.sessions[index].version;
        if downstream_version.is_some_and(|version| version != owner.jazz.version_id()) {
            return Err(unsupported("relay reads across different schema versions"));
        }
        if inner.sessions[index].forwarded.contains_key(&id)
            || inner.sessions[index].forwarded_catalogue.contains_key(&id)
        {
            return Err(query_error("duplicate pending relay request ID"));
        }
        let client = inner.client.as_mut().expect("relay");
        if client.transport.is_none() || !may_wait(client.hint.get()) {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "relay read has no available authority",
            ));
        }
        if client.upstream_schema != Some(owner.jazz.version_id()) {
            return Err(unsupported(
                "relay reads after a schema change without upstream reconnect",
            ));
        }
        let upstream = client.next_id;
        client.next_id += 1;
        let connection = client.connection;
        let claims = prepared
            .request_claims
            .as_ref()
            .expect("forwarded immutable claims")
            .1
            .clone();
        client.answers.insert(upstream, None);
        if overlay_ops.is_none() && crate::db::relation::is_union(&prepared.query) {
            client.union_answers.insert(upstream);
        }
        if !ordinary {
            client.scoped_answers.insert(upstream);
        }
        if accepted {
            client.accepted_answers.insert(upstream);
        }
        let overlay = overlay_ops.is_some();
        let message = if ordinary {
            SyncMessage::Fetch {
                id: upstream,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                at,
                include_deleted,
            }
        } else if let Some(ops) = overlay_ops {
            client.overlay_answers.insert(upstream);
            SyncMessage::AcceptedOverlayCheckV1 {
                id: upstream,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                through: at.expect("overlay cut"),
                include_deleted,
                author: identity.canonical().to_owned(),
                claims,
                ops,
            }
        } else if accepted {
            SyncMessage::AcceptedFetchV1 {
                id: upstream,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                at,
                include_deleted,
                scope: Some((identity.canonical().to_owned(), claims)),
            }
        } else {
            SyncMessage::ScopedFetchV1 {
                id: upstream,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                author: identity.canonical().to_owned(),
                claims,
                include_deleted,
            }
        };
        client.outbox.push(message);
        inner.sessions[index].forwarded.insert(
            id,
            ForwardedFetch {
                upstream,
                connection,
                owner,
                downstream_version,
                prepared,
                identity,
                include_deleted,
                accepted,
                scoped_evidence,
                overlay,
                ordinary,
                deadline,
            },
        );
        Ok(())
    }

    /// A relay answers only after its upstream does. Each hop uses normal row
    /// sync, then the same fetch response path constrained to that exact cut.
    fn progress_scoped_fetches(&self) {
        let ready = {
            let mut inner = self.inner.borrow_mut();
            let mut ready = Vec::new();
            for index in 0..inner.sessions.len() {
                let mut completed = Vec::new();
                for (&id, request) in &inner.sessions[index].forwarded {
                    // A private owner's first catalogue can replace deny-by-
                    // default policies while this ordinary read waits. Its
                    // physical schema must stay identical; the reply still
                    // rechecks the authenticated governor and exact roots.
                    let same_owner = Rc::ptr_eq(&request.owner, &inner.owner)
                        || (request.ordinary
                            && inner.scope_isolated_owner
                            && request.owner.jazz.without_permissions()
                                == inner.owner.jazz.without_permissions()
                            && request.owner.names().tables == inner.owner.names().tables);
                    let error = if request
                        .deadline
                        .as_ref()
                        .is_some_and(|at| Instant::now() >= **at)
                    {
                        Some(Error::new(ErrorCode::NotObserved, "relay read timed out"))
                    } else if !same_owner
                        || request.downstream_version != inner.sessions[index].version
                    {
                        Some(Error::new(
                            ErrorCode::NotObserved,
                            "relay read interrupted by a schema change",
                        ))
                    } else if inner
                        .client
                        .as_ref()
                        .is_none_or(|client| client.connection != request.connection)
                    {
                        Some(Error::new(
                            ErrorCode::NotObserved,
                            "relay read interrupted by an upstream reconnect",
                        ))
                    } else {
                        None
                    };
                    let answer = error.map(Err).or_else(|| {
                        let client = inner.client.as_ref().expect("same connection");
                        client
                            .answers
                            .get(&request.upstream)
                            .cloned()
                            .flatten()
                            .or_else(|| {
                                (client.transport.is_none() || !may_wait(client.hint.get())).then(
                                    || {
                                        Err(Error::new(
                                            ErrorCode::NotObserved,
                                            "relay read has no available authority",
                                        ))
                                    },
                                )
                            })
                    });
                    if let Some(answer) = answer {
                        if !request.overlay
                            && (request.ordinary
                                || request.accepted
                                || request.scoped_evidence
                                || matches!(&answer, Ok((_, _, Some(_), _))))
                            && let Ok((through, rows, evidence, union)) = &answer
                        {
                            // Rows can precede their chunks. Keep the request alive while
                            // normal chunk demand hydrates this exact authority result.
                            let mut exact = match exact_fetch_query(
                                &request.prepared,
                                rows.clone(),
                                union.as_deref(),
                            ) {
                                Ok(exact) => exact,
                                Err(error) => {
                                    completed.push((id, Err(error)));
                                    continue;
                                }
                            };
                            exact.authority_policies = match inner
                                .needs_authority_policies(&exact, Some(request.identity))
                            {
                                Ok(needed) => needed,
                                Err(error) => {
                                    completed.push((id, Err(error)));
                                    continue;
                                }
                            };
                            let read = ReadSpec {
                                cut: Some(Cut::at(*through)),
                                include_deleted: request.include_deleted,
                                ..ReadSpec::default()
                            };
                            let evaluated = if request.scoped_evidence || evidence.is_some() {
                                match evidence {
                                    Some(evidence) => inner.run_scoped_evidence(
                                        &exact,
                                        Some(request.identity),
                                        &read,
                                        evidence,
                                    ),
                                    None => Err(Error::new(
                                        ErrorCode::NotObserved,
                                        "authority omitted scoped permission evidence",
                                    )),
                                }
                            } else {
                                inner.run(&exact, Some(request.identity), &read)
                            };
                            let evaluated = evaluated.and_then(|rows| {
                                if let Some(occurrences) = union {
                                    unions::check(&rows, occurrences)?;
                                }
                                Ok(rows)
                            });
                            if let Err(error) = evaluated {
                                if crate::large::is_missing(&error) {
                                    continue;
                                }
                                completed.push((id, Err(error)));
                                continue;
                            }
                        }
                        completed.push((id, answer));
                    }
                }
                for (id, answer) in completed {
                    let request = inner.sessions[index]
                        .forwarded
                        .remove(&id)
                        .expect("pending");
                    if let Some(client) = inner.client.as_mut() {
                        client.cancel_fetch(request.upstream);
                    }
                    ready.push((index, id, request, answer));
                }
            }
            ready
        };
        for (index, id, request, answer) in ready {
            let result = answer.and_then(|(through, rows, evidence, union)| {
                if request.overlay {
                    self.inner().sessions[index]
                        .outbox
                        .push(SyncMessage::AcceptedOverlayCheckedV1 { id, through });
                    return Ok(());
                }
                let expected = (request.ordinary
                    || request.accepted
                    || request.scoped_evidence
                    || evidence.is_some())
                .then_some(rows.clone());
                let mut exact = exact_fetch_query(&request.prepared, rows, union.as_deref())?;
                exact.authority_policies = self
                    .inner()
                    .needs_authority_policies(&exact, Some(request.identity))?;
                // Forwarding may request scoped proof upstream even when the
                // downstream SYSTEM read was unscoped. Keep its original reply
                // contract; it does not need permission witnesses to materialize.
                let evidence = if request.accepted
                    && !request.scoped_evidence
                    && request.identity == AuthorSubject::SYSTEM
                {
                    None
                } else {
                    evidence
                };
                self.session_db(index).serve_fetch(
                    index,
                    id,
                    &exact.query,
                    &exact,
                    Some(request.identity),
                    Some(through),
                    request.include_deleted,
                    request.accepted,
                    expected.as_deref(),
                    evidence.map(ScopedEvidence::Supplied),
                )
            });
            if let Err(error) = result {
                self.inner.borrow_mut().sessions[index]
                    .outbox
                    .push(SyncMessage::Refused {
                        id,
                        code: error.code,
                        message: error.message,
                    });
            }
        }
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
        let (identity, claims, upstream_claims) = {
            let inner = self.inner.borrow();
            let session = &inner.sessions[index];
            // Capture the node's own scope before installing this downstream
            // session's claims. Same identity does not imply the same token.
            let upstream_claims = (inner.client.is_some()
                && matches!(
                    &message,
                    SyncMessage::Fetch { .. }
                        | SyncMessage::Subscribe { .. }
                        | SyncMessage::SubscribeReadablePolicyV1 { .. }
                        | SyncMessage::ScopedUnionSubscribeV1 { .. }
                ))
            .then(|| {
                inner
                    .claims
                    .get(&inner.identity.author)
                    .cloned()
                    .unwrap_or_default()
            });
            (session.identity, session.claims.clone(), upstream_claims)
        };
        let Some(claims) = claims.filter(|_| identity != AuthorSubject::SYSTEM) else {
            return self.serve_as(index, message, None, upstream_claims);
        };
        // The session's claims stand for its identity's while it is served.
        let previous = self
            .inner
            .borrow_mut()
            .claims
            .insert(identity, claims.clone());
        let result = self.serve_as(index, message, Some(claims), upstream_claims);
        let mut inner = self.inner.borrow_mut();
        match previous {
            Some(previous) => inner.claims.insert(identity, previous),
            None => inner.claims.remove(&identity),
        };
        result
    }

    fn serve_as(
        &self,
        index: usize,
        message: SyncMessage,
        claims: Option<BTreeMap<String, Value>>,
        upstream_claims: Option<BTreeMap<String, Value>>,
    ) -> Result<(), Error> {
        let connection_identity = self.inner.borrow().sessions[index].identity;
        if let SyncMessage::AcceptedOverlayCheckV1 {
            id,
            query,
            binding,
            through,
            include_deleted,
            author,
            claims,
            ops,
        } = &message
        {
            if connection_identity != AuthorSubject::SYSTEM {
                return Err(rejected(
                    "accepted overlay checks require a trusted backend connection",
                ));
            }
            let author =
                AuthorSubject::from_canonical(author).map_err(|e| query_error(e.to_string()))?;
            if author == AuthorSubject::SYSTEM {
                return Err(rejected(
                    "accepted overlay checks require a session identity",
                ));
            }
            let db = self.session_db(index);
            let prepared = db
                .prepare_query_bound(query, binding.clone())?
                .with_identity_claims(author, claims.clone());
            db.check_accepted_query(&prepared)?;
            accepted_overlay::validate_ops(ops)?;
            if self.is_client() {
                return db.forward_fetch(
                    index,
                    *id,
                    prepared,
                    author,
                    *include_deleted,
                    Some(*through),
                    true,
                    false,
                    Some(ops.clone()),
                    false,
                );
            }
            db.validate_accepted_overlay(&prepared, author, *through, *include_deleted, ops)?;
            self.inner().sessions[index]
                .outbox
                .push(SyncMessage::AcceptedOverlayCheckedV1 {
                    id: *id,
                    through: *through,
                });
            return Ok(());
        }
        // Delegation changes only this request, never the connection identity
        // or the authority's cached claims for another request.
        let accepted = matches!(&message, SyncMessage::AcceptedFetchV1 { .. });
        let (message, identity, claims, scoped) = match message {
            SyncMessage::AcceptedFetchV1 {
                id,
                query,
                binding,
                at,
                include_deleted,
                scope,
            } => {
                let scoped = scope.is_some();
                let (identity, claims) = match scope {
                    Some((author, requested_claims)) => {
                        let author = AuthorSubject::from_canonical(&author)
                            .map_err(|e| Error::new(ErrorCode::Query, e.to_string()))?;
                        if connection_identity != AuthorSubject::SYSTEM {
                            let authenticated = claims.clone().unwrap_or_else(|| {
                                self.inner()
                                    .claims
                                    .get(&connection_identity)
                                    .cloned()
                                    .unwrap_or_default()
                            });
                            if author != connection_identity || requested_claims != authenticated {
                                return Err(Error::new(
                                    ErrorCode::WriteRejected,
                                    "accepted read scope differs from its authenticated connection",
                                ));
                            }
                        }
                        (author, Some(requested_claims))
                    }
                    None => (connection_identity, claims),
                };
                (
                    SyncMessage::Fetch {
                        id,
                        query,
                        binding,
                        at,
                        include_deleted,
                    },
                    identity,
                    claims,
                    scoped,
                )
            }
            SyncMessage::ScopedUnionSubscribeV1 {
                id,
                query,
                binding,
                author,
                claims,
                include_deleted,
            } => {
                if connection_identity != AuthorSubject::SYSTEM {
                    return Err(rejected(
                        "scoped live UNION subscriptions require a trusted backend connection",
                    ));
                }
                let author = AuthorSubject::from_canonical(&author)
                    .map_err(|e| query_error(e.to_string()))?;
                (
                    SyncMessage::Subscribe {
                        id,
                        query,
                        binding,
                        since: None,
                        include_deleted,
                    },
                    author,
                    Some(claims),
                    true,
                )
            }
            SyncMessage::ScopedFetchV1 {
                id,
                query,
                binding,
                author,
                claims,
                include_deleted,
            } => {
                if connection_identity != AuthorSubject::SYSTEM {
                    return Err(Error::new(
                        ErrorCode::WriteRejected,
                        "scoped reads require a trusted backend connection",
                    ));
                }
                let author = AuthorSubject::from_canonical(&author)
                    .map_err(|error| Error::new(ErrorCode::Query, error.to_string()))?;
                (
                    SyncMessage::Fetch {
                        id,
                        query,
                        binding,
                        at: None,
                        include_deleted,
                    },
                    author,
                    Some(claims),
                    true,
                )
            }
            message => (message, connection_identity, claims, false),
        };
        // A subscription's group keeps the session's claims for later runs.
        let with_claims = |prepared: PreparedQuery| match &claims {
            Some(claims) => prepared.with_identity_claims(identity, claims.clone()),
            None => prepared,
        };
        self.inner().require_owner_subject(identity)?;
        let policy = (identity != AuthorSubject::SYSTEM).then_some(identity);
        // Queries, writes and held rows name tables and columns as the
        // client's schema version does.
        let db = self.session_db(index);
        let readable_policy = matches!(&message, SyncMessage::SubscribeReadablePolicyV1 { .. });
        match message {
            SyncMessage::OwnerInitializationReserveV1 {
                id,
                nonce,
                node,
                minimum,
            } => {
                let result = db.reserve_owner_initialization(index, id, nonce, node, minimum);
                if let Err(error) = result {
                    self.inner().sessions[index].outbox.push(
                        SyncMessage::OwnerInitializationReservedV1 {
                            id,
                            nonce,
                            result: Err(wire_error(&error)),
                        },
                    );
                }
            }
            SyncMessage::OwnerInitializationPublishV1 { nonce, commit } => {
                db.publish_owner_initialization(index, policy, nonce, &commit)?;
            }
            SyncMessage::OwnerProjectionV1 { known } => {
                db.admit_owner_observer(index, known)?;
            }
            SyncMessage::Schema { version } => {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                session.version = Some(crate::ids::SchemaVersionId::from_bytes(version));
                session.schema = None;
                drop(inner);
                self.queue_catalogue_metadata(index)?;
            }
            SyncMessage::CatalogueFetchV1 {
                id,
                table,
                opened,
                scope,
            } => {
                let (author, scope_claims) = match scope {
                    Some((author, requested)) => {
                        let author = AuthorSubject::from_canonical(&author)
                            .map_err(|e| query_error(e.to_string()))?;
                        let authenticated = claims.clone().unwrap_or_else(|| {
                            self.inner()
                                .claims
                                .get(&connection_identity)
                                .cloned()
                                .unwrap_or_default()
                        });
                        if connection_identity != AuthorSubject::SYSTEM
                            && (author != connection_identity || requested != authenticated)
                        {
                            return Err(Error::new(
                                ErrorCode::WriteRejected,
                                "catalogue request scope differs from its authenticated connection",
                            ));
                        }
                        (author, requested)
                    }
                    None => (
                        connection_identity,
                        claims.unwrap_or_else(|| {
                            self.inner()
                                .claims
                                .get(&connection_identity)
                                .cloned()
                                .unwrap_or_default()
                        }),
                    ),
                };
                if self.is_client() {
                    self.forward_catalogue_fetch(index, id, table, opened, (author, scope_claims))?;
                } else {
                    let (head, schemas) = self.authority_catalogue_coverage(
                        &table,
                        opened,
                        Some((author, scope_claims)),
                    )?;
                    let mut inner = self.inner();
                    inner.sessions[index]
                        .outbox
                        .push(SyncMessage::CatalogueMetadataV1 {
                            head: head.clone(),
                            schemas,
                        });
                    if let Some(descriptor) = inner.policy_descriptor()? {
                        inner.sessions[index]
                            .outbox
                            .push(SyncMessage::PolicyDescriptorV1(descriptor));
                    }
                    inner.sessions[index]
                        .outbox
                        .push(SyncMessage::CatalogueCoveredV1 { id, head, opened });
                }
            }
            SyncMessage::Subscribe {
                id,
                query,
                binding,
                since,
                include_deleted,
            }
            | SyncMessage::SubscribeReadablePolicyV1 {
                id,
                query,
                binding,
                since,
                include_deleted,
            } => {
                if readable_policy {
                    let inner = db.inner();
                    if !inner.authority || inner.client.is_some() {
                        return Err(unsupported(
                            "readable permission subscriptions through a non-authority relay",
                        ));
                    }
                    if inner.sessions[index]
                        .version
                        .is_some_and(|version| version != inner.owner.jazz.version_id())
                    {
                        return Err(unsupported(
                            "readable permission subscriptions across schema versions",
                        ));
                    }
                }
                let prepared = with_claims(db.prepare_query_bound(&query, binding)?);
                let union = crate::db::relation::is_union(&prepared.query);
                if scoped {
                    if !union {
                        return Err(unsupported(
                            "scoped live subscription requests without a UNION",
                        ));
                    }
                    db.check_scoped_fetch(&prepared)?;
                    if self.is_client() && db.inner().identity.author != AuthorSubject::SYSTEM {
                        return Err(rejected(
                            "scoped live UNION relays require a trusted SYSTEM upstream",
                        ));
                    }
                }
                let scope =
                    scoped.then(|| prepared.request_claims.clone().expect("bound live scope"));
                if union {
                    Self::remote_fetch_shape(&prepared.query)?;
                    db.inner().check_union_shape(&prepared)?;
                    if self.is_client() && !scoped {
                        let inner = db.inner();
                        let upstream_identity = inner.identity.author;
                        let request_claims = prepared
                            .request_claims
                            .as_ref()
                            .map(|(_, claims)| claims.clone())
                            .unwrap_or_else(|| {
                                inner.claims.get(&identity).cloned().unwrap_or_default()
                            });
                        if identity != upstream_identity
                            || (identity != AuthorSubject::SYSTEM
                                && upstream_claims.as_ref() != Some(&request_claims))
                        {
                            return Err(rejected(
                                "live UNION relay scope differs from its upstream admission",
                            ));
                        }
                    }
                }
                db.inner().check_reachability_shape(&prepared)?;
                db.inner().check_inheritance_shape(&prepared)?;
                if readable_policy {
                    db.inner().check_readable_policy_shape(&prepared, policy)?;
                }
                let windowed = query.limit.is_some() || query.offset > 0;
                // A relay serves what its upstream lists, settled once the
                // upstream answers: its own store may be cold.
                let relayed = db.attach_remote(&prepared, include_deleted, scope.clone());
                let mut inner = db.inner();
                let support = Rc::new(inner.support_of(&query)?);
                let authority = inner.authority;
                let union_live = inner.live_union(&prepared, include_deleted, &scope);
                let subscribed = inner.subscribe_member(
                    &prepared,
                    policy,
                    ReadSpec {
                        union_live: union_live.clone(),
                        union_preview: !scoped,
                        scoped_live: scoped,
                        include_deleted,
                        members_only: relayed.is_some(),
                        ..ReadSpec::default()
                    },
                    match &relayed {
                        Some((answered, _, global)) => match &union_live {
                            Some(live) => SettledBy::Union {
                                answered: answered.clone(),
                                global: global.clone(),
                                live: live.clone(),
                                relay: true,
                            },
                            None => SettledBy::Relay(answered.clone(), global.clone()),
                        },
                        None => SettledBy::Always(authority),
                    },
                );
                if relayed.is_some() {
                    match &subscribed {
                        Ok(_) => {
                            let read = (
                                prepared.query.clone(),
                                prepared.binding.clone(),
                                include_deleted,
                                scope.clone(),
                            );
                            inner.sessions[index].relayed.insert(id, read);
                        }
                        Err(_) => inner.release_remote(
                            &prepared.query,
                            &prepared.binding,
                            include_deleted,
                            &scope,
                        ),
                    }
                }
                let (queue, group) = subscribed?;
                if scoped {
                    inner.sessions[index]
                        .scoped_unions
                        .insert(id, prepared.clone());
                }
                if union || reachability::needed(&query) || !query.inherits.is_empty() {
                    if let Some(group) = group.upgrade() {
                        group.borrow_mut().observe_support_inputs(&queue);
                    }
                    inner.sessions[index]
                        .query_support
                        .insert(id, prepared.clone());
                }
                if readable_policy {
                    if let Some(group) = group.upgrade() {
                        group.borrow_mut().observe_support_inputs(&queue);
                    }
                    inner.sessions[index]
                        .readable_policy
                        .insert(id, prepared.clone());
                }
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
                session.probed.extend(touched.iter().cloned());
                session.touched.extend(touched);
                session.stale.extend(evict);
            }
            SyncMessage::Unsubscribe { id } => {
                let mut inner = self.inner.borrow_mut();
                inner.sessions[index].support.remove(&id);
                inner.sessions[index].readable_policy.remove(&id);
                inner.sessions[index].query_support.remove(&id);
                inner.sessions[index].scoped_unions.remove(&id);
                if let Some((query, binding, include_deleted, scope)) =
                    inner.sessions[index].relayed.remove(&id)
                {
                    inner.release_remote(&query, &binding, include_deleted, &scope);
                }
                if let Some((queue, group, ..)) = inner.sessions[index].subs.remove(&id) {
                    let dropped = inner.fan.dropped.clone();
                    dropped.set(dropped.get() + 1);
                    // Rows the result lost since the last tick were not
                    // re-checked yet: the client may still hold them.
                    let events: Vec<SubscriptionEvent> = queue.borrow_mut().drain(..).collect();
                    if let Some(group) = group.upgrade() {
                        let table = group.borrow().root().to_owned();
                        for event in events {
                            let SubscriptionEvent::Delta { removed, .. } = event else {
                                continue;
                            };
                            for gone in removed {
                                let id = gone.row_uuid.0.as_bytes().to_vec();
                                for image in inner.images_where(&table, false, "id = ?1", &[&id])? {
                                    inner.sessions[index].touched.insert((
                                        table.clone(),
                                        gone.row_uuid,
                                        image.key,
                                    ));
                                }
                            }
                        }
                    }
                    // The replica keeps the rows it was shipped: held, a
                    // policy dependency's commit still re-checks them, so a
                    // retained copy is unlisted once the client loses access.
                    if policy.is_some()
                        && let Some(group) = group.upgrade()
                    {
                        let group = group.borrow();
                        let table = group.root().to_owned();
                        let mut held = std::mem::take(&mut inner.sessions[index].held);
                        for row in group.rows.iter().filter(|row| *row.table == *table) {
                            let rid = row.id.0.as_bytes().to_vec();
                            let seq = row.seq as i64;
                            for image in
                                inner.images_where(&table, false, ID_AT_SEQ, &[&rid, &seq])?
                            {
                                held.entry(table.clone())
                                    .or_default()
                                    .entry((row.id, image.key))
                                    .or_insert(true);
                            }
                        }
                        inner.sessions[index].held = held;
                    }
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
                let prepared = with_claims(db.prepare_query_bound(&query, binding)?);
                let upstream_identity = db.inner().identity.author;
                let ordinary = !scoped
                    && !accepted
                    && (upstream_identity != AuthorSubject::SYSTEM
                        || identity == AuthorSubject::SYSTEM);
                let forward = self.is_client()
                    && (scoped
                        || accepted
                        || db.inner().client.as_ref().is_some_and(|client| {
                            client.transport.is_some() && may_wait(client.hint.get())
                        }));
                let delegated = scoped || (forward && !ordinary && !accepted);
                let scoped_evidence = if accepted {
                    db.check_accepted_query(&prepared)?;
                    scoped
                        && connection_identity == AuthorSubject::SYSTEM
                        && db.check_scoped_fetch(&prepared)?
                } else if delegated {
                    db.check_scoped_fetch(&prepared)?
                } else {
                    false
                };
                // Ordinary fetches also recover offline owner inputs. They
                // retain their cache contract without a live upstream; a
                // request already forwarded never falls back after disconnect.
                if forward {
                    let claims = prepared
                        .request_claims
                        .as_ref()
                        .map(|(_, claims)| claims.clone())
                        .unwrap_or_else(|| {
                            db.inner()
                                .claims
                                .get(&identity)
                                .cloned()
                                .unwrap_or_default()
                        });
                    let prepared = prepared.with_identity_claims(identity, claims);
                    if ordinary {
                        if identity != upstream_identity {
                            return Err(rejected(
                                "ordinary relay read identity differs from its upstream identity",
                            ));
                        }
                        if identity != AuthorSubject::SYSTEM
                            && prepared.request_claims.as_ref().map(|(_, claims)| claims)
                                != upstream_claims.as_ref()
                        {
                            return Err(rejected(
                                "ordinary relay read claims differ from its upstream claims",
                            ));
                        }
                        Self::remote_fetch_shape(&prepared.query)?;
                    }
                    db.forward_fetch(
                        index,
                        id,
                        prepared,
                        identity,
                        include_deleted,
                        at,
                        accepted,
                        scoped_evidence,
                        None,
                        ordinary,
                    )?;
                } else {
                    if accepted {
                        let inner = db.inner();
                        if !inner.authority
                            || at.is_some_and(|cut| cut > inner.seq || cut >= LOCAL_BASE)
                        {
                            return Err(Error::new(
                                ErrorCode::NotObserved,
                                "accepted read requires an available authority snapshot",
                            ));
                        }
                    }
                    db.serve_fetch(
                        index,
                        id,
                        &query,
                        &prepared,
                        policy,
                        at,
                        include_deleted,
                        accepted,
                        None,
                        scoped_evidence.then_some(ScopedEvidence::Collect),
                    )?;
                }
            }
            SyncMessage::Commit {
                node,
                tx,
                kind,
                ops,
                author,
                session,
                updated_at,
            } => {
                if self.is_client() {
                    let request = SyncMessage::Commit {
                        node,
                        tx,
                        kind,
                        ops,
                        author,
                        session,
                        updated_at,
                    };
                    if let Err(error) = db.forward_upload(index, policy, request, None) {
                        let mut inner = self.inner.borrow_mut();
                        // This unit is already journalled. A private projection
                        // fault cannot reject a unit that recovery will forward.
                        if inner.owner_projection_error.is_some() {
                            return Err(error);
                        }
                        let through = inner.client.as_ref().expect("relay").known;
                        inner.sessions[index].outbox.push(SyncMessage::FateV1 {
                            node,
                            tx,
                            through,
                            result: Err(wire_error(&error)),
                        });
                    }
                    return Ok(());
                }
                // A resend after a reconnect gets the recorded fate.
                let known = self.inner.borrow().applied_fate(node, tx)?;
                let (result, rows) = match known {
                    Some(result) => (result, Vec::new()),
                    None => match commit_who(policy, author.as_deref(), session.is_some())
                        .map_err(|error| (error, Vec::new()))
                        .and_then(|who| {
                            // A trusted link's session write is checked
                            // with the claims the link vouches for.
                            let vouched = match (who, session) {
                                (Who::Session(author), Some(claims)) if policy.is_none() => {
                                    Some((author, claims))
                                }
                                _ => None,
                            };
                            let previous = vouched.as_ref().map(|(author, claims)| {
                                (
                                    *author,
                                    self.inner
                                        .borrow_mut()
                                        .claims
                                        .insert(*author, claims.clone()),
                                )
                            });
                            let result =
                                db.authorize_commit(who, kind, ops, updated_at, (node, tx));
                            if let Some((author, previous)) = previous {
                                let mut inner = self.inner.borrow_mut();
                                match previous {
                                    Some(previous) => inner.claims.insert(author, previous),
                                    None => inner.claims.remove(&author),
                                };
                            }
                            result
                        }) {
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
                session.outbox.push(SyncMessage::FateV1 {
                    node,
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

    /// Validate (serializable) and apply one uploaded commit as `who`.
    /// Returns the rows the client must hear about either way: the rows it
    /// wrote, so a rejected client rebases onto the authority's versions.
    #[allow(clippy::type_complexity)]
    fn authorize_commit(
        &self,
        who: Who,
        kind: WireKind,
        ops: Vec<WireOp>,
        updated_at: Option<u64>,
        origin: ([u8; 16], u64),
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, (Error, Vec<(String, RowUuid, Vec<u8>)>)> {
        if self.is_client() {
            return Err((
                unsupported("write uploads through a SQLite relay"),
                Vec::new(),
            ));
        }
        // A trusted backend's session batch validates reads under the same
        // session as its writes, not under the transport's SYSTEM privilege.
        let identity = match who {
            Who::Session(author) => Some(author),
            Who::Local | Who::Attributed(_) => None,
        };
        let mut ops: Vec<StagedOp> = match ops.into_iter().map(from_wire).collect::<Result<_, _>>()
        {
            Ok(ops) => ops,
            Err(error) => return Err((error, Vec::new())),
        };
        if let Some(at) = updated_at {
            ops.iter_mut().for_each(|op| op.stamp_at(at));
        }
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
        let serializable = match &kind {
            WireKind::Mergeable => None,
            WireKind::Serializable {
                snapshot,
                reads,
                own,
            } => Some((
                *snapshot,
                reads,
                own.iter().map(|&tx| (tx, origin.0, tx)).collect::<Vec<_>>(),
            )),
            WireKind::SerializableOriginsV1 {
                snapshot,
                reads,
                own,
            }
            | WireKind::SerializableObservedV1 {
                snapshot,
                reads,
                own,
                ..
            }
            | WireKind::SerializableUnionObservedV1 {
                snapshot,
                reads,
                own,
                ..
            } => {
                let mut positions = BTreeSet::new();
                let mut origins = BTreeSet::new();
                if *snapshot >= LOCAL_BASE
                    || own.iter().any(|(position, node, tx)| {
                        *position < LOCAL_BASE
                            || *position >= i64::MAX as u64
                            || *tx == 0
                            || *tx >= i64::MAX as u64
                            || !positions.insert(*position)
                            || !origins.insert((*node, *tx))
                    })
                {
                    return Err((
                        Error::new(
                            ErrorCode::Protocol,
                            "invalid serializable-origins-v1 dependency",
                        ),
                        Vec::new(),
                    ));
                }
                Some((*snapshot, reads, own.clone()))
            }
        };
        if let Some((snapshot, reads, own)) = serializable {
            if reads.iter().any(|read| read.query.relation.is_some())
                && !matches!(&kind, WireKind::SerializableUnionObservedV1 { .. })
            {
                return Err((
                    Error::new(
                        ErrorCode::Protocol,
                        "retained relation read requires serializable-union-observed-v1 evidence",
                    ),
                    Vec::new(),
                ));
            }
            let content = match &kind {
                WireKind::SerializableObservedV1 { content, .. }
                | WireKind::SerializableUnionObservedV1 { content, .. } => {
                    if content.len() != reads.len()
                        || content
                            .iter()
                            .zip(reads)
                            .any(|(content, read)| content.len() != read.rows.len())
                    {
                        return Err((
                            Error::new(
                                ErrorCode::Protocol,
                                "invalid serializable-observed-v1 read content",
                            ),
                            Vec::new(),
                        ));
                    }
                    Some(content)
                }
                _ => None,
            };
            let inner = self.inner();
            // The sender's own commits its snapshot included, by local seq
            // and where each settled here (earlier on this connection, so
            // before this one).
            let mut settled = BTreeMap::new();
            for (position, node, tx) in own {
                if let Some(seq) = inner
                    .settled(node, tx)
                    .map_err(|e| (e, Vec::new()))?
                    .flatten()
                {
                    settled.insert(position, seq);
                }
            }
            let in_snapshot = |seq: u64| seq <= snapshot || settled.values().any(|s| *s == seq);
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
            for (index, read) in reads.iter().enumerate() {
                let normalized = normalize_relation(&read.query, &inner.schema)
                    .map_err(|error| (error, Vec::new()))?;
                let prepared = PreparedQuery {
                    query: normalized.unwrap_or_else(|| read.query.clone()),
                    binding: read.binding.clone(),
                    shape: Shape([0; 16]),
                    binding_id: Binding([0; 16]),
                    request_claims: None,
                    authority_policies: false,
                    schema: self.view.clone(),
                };
                let spec = ReadSpec {
                    include_deleted: read.include_deleted,
                    ..ReadSpec::default()
                };
                let observed = inner
                    .read_rows(&prepared, identity, &spec)
                    .map_err(|e| (e, Vec::new()))?;
                let content_changed = if let Some(content) = content {
                    read_contents(&observed).map_err(|error| (error, Vec::new()))? != content[index]
                } else {
                    false
                };
                let now: Vec<([u8; 16], u64)> = versions(&observed)
                    .iter()
                    .map(|(row, seq)| (*row.as_bytes(), *seq))
                    .collect();
                // A row the sender read at an own commit's local seq is
                // compared where that commit settled.
                let then: Vec<([u8; 16], u64)> = read
                    .rows
                    .iter()
                    .map(|(row, seq)| (*row, settled.get(seq).copied().unwrap_or(*seq)))
                    .collect();
                if now != then || content_changed {
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
            let unit = ops
                .iter()
                .cloned()
                .map(|op| (inner.schema.clone(), op))
                .collect::<Vec<_>>();
            let serializable = matches!(
                kind,
                WireKind::Serializable { .. }
                    | WireKind::SerializableOriginsV1 { .. }
                    | WireKind::SerializableObservedV1 { .. }
                    | WireKind::SerializableUnionObservedV1 { .. }
            );
            let applied = inner
                .candidate_unit(&unit, seq, serializable, false, |inner| {
                    ops.iter()
                        .try_for_each(|op| inner.apply_as(op, who, seq).map(drop))
                })
                .and_then(|()| inner.record_writer_order(origin.0, origin.1, mark));
            if let Err(error) = applied {
                inner
                    .rollback_savepoint("upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.changed.borrow_mut().truncate(mark);
                let rows = mentioned(&inner);
                return Err((error, rows));
            }
            if let Err(error) = inner
                .record_fate(origin.0, origin.1, &Ok(()))
                .and_then(|()| inner.record_settled(origin.0, origin.1, Some(seq)))
                .and_then(|()| inner.record_issued_seq(seq))
            {
                inner
                    .rollback_savepoint("upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.changed.borrow_mut().truncate(mark);
                return Err((error, Vec::new()));
            }
            inner
                .conn
                .execute_batch("RELEASE upload")
                .map_err(|e| (storage(e), Vec::new()))?;
            inner.seq = seq;
            seq
        };
        self.notify().map_err(|error| (error, Vec::new()))?;
        let inner = self.inner.borrow();
        Ok(inner.written_rows(&ops, seq).unwrap_or_default())
    }

    /// One tick for both roles.
    pub(crate) fn sync_tick(&self) -> Result<(), Error> {
        {
            let inner = self.inner();
            inner.require_owner_subject(inner.identity.author)?;
        }
        self.server_tick()?;
        self.client_tick()
    }
}

/// Who an uploaded commit is applied as. A session link writes under its
/// own identity and may not name another author; a trusted link (no policy
/// identity) writes as the session it names (checked as it), or unchecked,
/// attributed to the author it names.
fn commit_who(
    policy: Option<AuthorSubject>,
    author: Option<&str>,
    session: bool,
) -> Result<Who, Error> {
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
        (None, Some(author)) if session && author != AuthorSubject::SYSTEM => {
            Ok(Who::Session(author))
        }
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
        | PolicyExpr::ExistsIncludingCreated { .. }
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
    // A relay cannot re-admit a row its authority unlisted just because its
    // retained local image still satisfies the old policy. Keep revocation
    // effective at every hop, including SYSTEM downstream sessions.
    if inner.client.is_some()
        && inner
            .conn
            .query_row(
                "SELECT 1 FROM _jazz_revoked WHERE t = ?1 AND id = ?2 AND k = ?3",
                rusqlite::params![table, row.as_bytes().to_vec(), key],
                |_| Ok(()),
            )
            .optional()
            .map_err(storage)?
            .is_some()
    {
        return Ok(false);
    }
    let Some(author) = identity else {
        return Ok(true);
    };
    if inner.owner_pending_preview(table, row, key, identity)? {
        return Ok(true);
    }
    let schema = inner.owner.clone();
    let table = owned_table(&schema, table)?;
    inner.check(
        table,
        row,
        key,
        author,
        Operation::Select,
        "read policy denied the row",
    )
}

/// The current schema's table stored as `stored`.
fn owned_table<'a>(schema: &'a Schema, stored: &str) -> Result<&'a Table, Error> {
    schema
        .by_stored(stored)
        .ok_or_else(|| query_error(format!("unknown table {stored}")))
}

/// Whether a client message may be served ahead of the writes the ingress
/// limit holds back: it reads or declares interest, and writes no rows.
fn overtakes_writes(message: &SyncMessage) -> bool {
    matches!(
        message,
        SyncMessage::Subscribe { .. }
            | SyncMessage::SubscribeReadablePolicyV1 { .. }
            | SyncMessage::ScopedUnionSubscribeV1 { .. }
            | SyncMessage::Unsubscribe { .. }
            | SyncMessage::Fetch { .. }
            | SyncMessage::ScopedFetchV1 { .. }
            | SyncMessage::AcceptedFetchV1 { .. }
            | SyncMessage::AcceptedOverlayCheckV1 { .. }
            | SyncMessage::CatalogueFetchV1 { .. }
            | SyncMessage::Advise { .. }
    )
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
    let accepted_evidence = matches!(&message, SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. });
    Some(match message {
        SyncMessage::ScopedEvidenceAnswerV1 {
            id,
            through,
            rows,
            mut evidence,
        }
        | SyncMessage::AcceptedScopedEvidenceAnswerV1 {
            id,
            through,
            rows,
            mut evidence,
        } => {
            for row in &mut evidence {
                let Some(table) = schema.by_stored(&row.table) else {
                    return Some(SyncMessage::Refused {
                        id, code: ErrorCode::Query,
                        message: "scoped permission evidence across a schema view missing a dependency table is unsupported".into(),
                    });
                };
                row.table = table.name.clone();
            }
            evidence_answer(accepted_evidence, id, through, rows, evidence)
        }
        SyncMessage::Rows { images } => SyncMessage::Rows {
            images: images
                .into_iter()
                .filter_map(|image| image_out(schema, image))
                .collect(),
        },
        SyncMessage::AcceptedRowsV1 { rows } => SyncMessage::AcceptedRowsV1 {
            rows: rows
                .into_iter()
                .filter_map(|mut row| {
                    row.table = schema.by_stored(&row.table)?.name.clone();
                    Some(row)
                })
                .collect(),
        },
        SyncMessage::ContentWitnessesV1 { witnesses } => SyncMessage::ContentWitnessesV1 {
            witnesses: witnesses
                .into_iter()
                .filter_map(|mut witness| {
                    witness.table = schema.by_stored(&witness.table)?.name.clone();
                    Some(witness)
                })
                .collect(),
        },
        SyncMessage::Evict { table, id, key } => SyncMessage::Evict {
            table: schema.by_stored(&table)?.name.clone(),
            id,
            key,
        },
        SyncMessage::Unlist { table, id, key } => SyncMessage::Unlist {
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

fn scoped_result_query(prepared: &PreparedQuery, rows: Vec<[u8; 16]>) -> PreparedQuery {
    let mut exact = prepared.clone();
    exact.query.filters.push(crate::query::in_list(
        crate::query::col("id"),
        rows.into_iter()
            .map(|row| crate::query::lit(Value::Uuid(RowUuid::from_bytes(row).0))),
    ));
    // The authority has already applied this window.
    exact.query.limit = None;
    exact.query.offset = 0;
    exact
}

fn exact_fetch_query(
    prepared: &PreparedQuery,
    rows: Vec<[u8; 16]>,
    union: Option<&[UnionOccurrenceV1]>,
) -> Result<PreparedQuery, Error> {
    match (crate::db::relation::is_union(&prepared.query), union) {
        (true, Some(occurrences)) => unions::exact(prepared, occurrences),
        (false, None) => Ok(scoped_result_query(prepared, rows)),
        _ => Err(Error::new(
            ErrorCode::Protocol,
            "fetch coverage does not match its query kind",
        )),
    }
}

fn row_local_policy(policy: &PolicyExpr) -> bool {
    match policy {
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().all(row_local_policy),
        PolicyExpr::Not(item) => row_local_policy(item),
        PolicyExpr::Cmp { .. }
        | PolicyExpr::IsNull { .. }
        | PolicyExpr::IsNotNull { .. }
        | PolicyExpr::Contains { .. }
        | PolicyExpr::In { .. }
        | PolicyExpr::InList { .. }
        | PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::True
        | PolicyExpr::False => true,
        _ => false,
    }
}

impl Db {
    pub(super) fn check_scoped_fetch(&self, prepared: &PreparedQuery) -> Result<bool, Error> {
        let query = &prepared.query;
        if crate::db::relation::is_union(query) {
            Self::remote_fetch_shape(query)?;
            for (_, query) in unions::leaves(query)? {
                let mut leaf = prepared.clone();
                leaf.query = self.prepare_query(&query)?.query;
                self.check_scoped_fetch(&leaf)?;
            }
            // Even row-local arms carry one qualified reply: this prevents
            // ordinary UNION coverage being substituted for a scoped response.
            return Ok(true);
        }
        if history_source_table(&query.table).is_some()
            || query.flat_join.is_some()
            || !query.policy_branches.is_empty()
            || !query.reachable.is_empty()
            || !query.inherits.is_empty()
            || !query.includes.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
        {
            return Err(unsupported(
                "scoped remote reads of relations, aggregates or history",
            ));
        }
        let inner = self.inner();
        let schema = inner.effective_schema(prepared.schema.as_ref().unwrap_or(&inner.owner));
        let table = schema.table(&query.table)?;
        if !table.branch_by.is_empty() {
            return Err(unsupported("scoped remote reads of branched tables"));
        }
        let tables = scoped_queries::tables(&schema, table, &query.joins)?;
        let mut needs_evidence = !query.joins.is_empty();
        for table in tables {
            let policy = schema
                .policy_schema()
                .by_stored(&table.stored)
                .and_then(|table| table.policy(crate::tools::Operation::Select));
            let nonlocal_policy = policy.is_some_and(|policy| !row_local_policy(policy));
            needs_evidence |= nonlocal_policy;
            if !nonlocal_policy {
                continue;
            }
            // Reject unbounded dependency shapes even for an empty result,
            // before the authority executes its ordinary permission query.
            let identity = prepared
                .request_claims
                .as_ref()
                .map(|(author, _)| *author)
                .unwrap_or(inner.identity.author);
            let (_, session) = inner.read_sessions(prepared, Some(identity));
            Ctx::new(&schema, None, &session, &prepared.binding)
                .with_catalogue(&*inner)
                .with_candidate_evidence(Some(crate::compile::CandidateEvidence {
                    accepted: inner.seq,
                    publication: inner.seq + 1,
                    post: false,
                }))
                .operation_policy(table, crate::tools::Operation::Select, "t", &session)?;
        }
        Ok(needs_evidence)
    }

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
        author: Option<AuthorSubject>,
        require_coverage: bool,
        coverage_expired: E,
        _release_coverage: F,
    ) -> Result<SerializedReadResult, Error>
    where
        F: Fn(QueryAttachment),
        E: Fn() -> bool,
    {
        if open_tx.is_none()
            && opts.tier == ReadTier::Remote
            && !ReadSpec::of(&opts, &self.inner())?.frozen()
        {
            // This is upload ordering, not query coverage. In particular, a
            // large upload cannot consume the binding's coverage deadline.
            let reader = request_scope
                .as_ref()
                .map(|(identity, _)| *identity)
                .or(author)
                .unwrap_or_else(|| self.inner().identity.author);
            self.wait_for_preceding_read_writes(reader).await;
        }
        let read = self.read_serialized_query(query, opts, open_tx, request_scope, author);
        let mut read = std::pin::pin!(read);
        let mut deadline = None;
        poll_fn(|cx| match read.as_mut().poll(cx) {
            Poll::Pending if require_coverage => {
                // Start the binding's coverage budget only once the normal
                // read actually waits. Dropping this future on expiry also
                // releases its fetch or subscription through the usual guards.
                if coverage_expired() {
                    return Poll::Ready(Err(Error::new(
                        ErrorCode::NotObserved,
                        "Timed out waiting for query coverage",
                    )));
                }
                if deadline.is_none() {
                    deadline = self
                        .read_deadline(Some(Instant::now() + std::time::Duration::from_secs(15)));
                }
                Poll::Pending
            }
            ready => ready,
        })
        .await
    }

    async fn read_serialized_query(
        &self,
        query: &[u8],
        opts: ReadOpts,
        open_tx: Option<OpenTransactionId>,
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        author: Option<AuthorSubject>,
    ) -> Result<SerializedReadResult, Error> {
        let query: Query = postcard::from_bytes(query)
            .map_err(|e| Error::new(ErrorCode::Query, format!("undecodable query: {e}")))?;
        let wait = match (opts.tier, opts.first_load) {
            (ReadTier::Remote, _) => Some(None),
            (ReadTier::LocalFirst, FirstLoad::WaitForRemote { timeout_ms }) if timeout_ms > 0 => {
                Some(Some(
                    Instant::now() + std::time::Duration::from_millis(timeout_ms),
                ))
            }
            _ => None,
        };
        let frozen = open_tx.is_none() && ReadSpec::of(&opts, &self.inner())?.frozen();
        if open_tx.is_none()
            && !frozen
            && self.is_client()
            && let Some(deadline) = wait
        {
            let (needed, opened, scope) = {
                let inner = self.inner();
                let scope = request_scope.clone().or_else(|| {
                    author
                        .filter(|author| {
                            *author != AuthorSubject::SYSTEM && *author != inner.identity.author
                        })
                        .map(|author| {
                            (
                                author,
                                inner.claims.get(&author).cloned().unwrap_or_default(),
                            )
                        })
                });
                (
                    inner.needs_catalogue_schema(&inner.schema)?,
                    inner.schema.jazz.version_id(),
                    scope,
                )
            };
            if needed {
                // A stale host cache is not a refusal from the authority. Obtain
                // its exact schema receipt before preparing the ordinary query;
                // local-only/frozen/transaction reads never fetch admission here.
                let limit = Instant::now() + std::time::Duration::from_secs(15);
                self.fetch_catalogue_until(
                    &query.table,
                    opened,
                    scope,
                    deadline.map_or(limit, |d| d.min(limit)),
                )
                .await?;
            }
        }
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
            let rows = self
                .serializable_tx_ref(tx)
                .all_prepared_at_tier(&prepared, opts.read_view.include_deleted, opts.tier)
                .await?;
            return Ok(result(rows));
        }
        if (self.inner().non_durable
            || (crate::db::relation::is_union(&prepared.query)
                && opts.tier == ReadTier::LocalFirst
                && wait.is_some()
                && prepared.request_claims.is_none()
                && author.is_none_or(|id| id == self.inner().identity.author)))
            && self.is_client()
            && opts.tier != ReadTier::LocalOnly
            && !frozen
        {
            if prepared.request_claims.is_some()
                || author.is_some_and(|id| {
                    id != self.inner().identity.author && id != AuthorSubject::SYSTEM
                })
            {
                return Err(unsupported(
                    "request-scoped foreground reads through a storage owner",
                ));
            }
            // A temporary normal subscription provides both owner-local and
            // authority settlement, including an empty answer. Its drop releases
            // the upstream attachment on success, refusal or cancellation.
            let mut opts = opts;
            if let Some(Some(deadline)) = wait {
                // Catalogue admission has already consumed part of this wait.
                opts.first_load = FirstLoad::WaitForRemote {
                    timeout_ms: deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                };
            }
            let mut stream = self.open_subscription(&prepared, author, opts)?;
            let rows = poll_fn(|_| {
                if let Some(error) = stream
                    .refused
                    .as_ref()
                    .and_then(|slot| slot.borrow().clone())
                {
                    return Poll::Ready(Err(error));
                }
                match stream.try_next_event() {
                    Some(SubscriptionEvent::Delta { added, .. }) => {
                        Poll::Ready(Ok(added.into_iter().map(|row| row.row).collect()))
                    }
                    Some(SubscriptionEvent::Rejected { reason }) => {
                        Poll::Ready(Err(Error::new(ErrorCode::Query, reason)))
                    }
                    Some(SubscriptionEvent::Closed) => Poll::Ready(Err(Error::new(
                        ErrorCode::NotObserved,
                        "foreground owner read closed before answering",
                    ))),
                    None => Poll::Pending,
                }
            })
            .await?;
            return Ok(result(rows));
        }
        if self.is_client()
            && opts.tier != ReadTier::LocalOnly
            && !frozen
            && let Some(deadline) = wait
        {
            if author.is_some_and(|author| {
                author != AuthorSubject::SYSTEM && author != self.inner().identity.author
            }) || prepared.request_claims.is_some()
            {
                let scoped_evidence = self.check_scoped_fetch(&prepared)?;
                let identity = author.unwrap_or(self.inner().identity.author);
                let claims = match &prepared.request_claims {
                    Some((requested, claims)) if *requested == identity => claims.clone(),
                    Some(_) => {
                        return Err(Error::new(
                            ErrorCode::Query,
                            "request claims differ from the read identity",
                        ));
                    }
                    None => self
                        .inner()
                        .claims
                        .get(&identity)
                        .cloned()
                        .unwrap_or_default(),
                };
                let answer = self
                    .fetch_answer(
                        &prepared,
                        None,
                        opts.read_view.include_deleted,
                        deadline,
                        Some((identity, claims)),
                        false,
                    )
                    .await?;
                if let Some((through, rows, evidence, union)) = answer {
                    let scoped_evidence = scoped_evidence
                        || evidence.is_some()
                        || self.check_scoped_fetch(&prepared)?;
                    let expected = rows.clone();
                    // Only these authority-approved roots, at this cut, belong
                    // to this request. Other cached or pending rows do not.
                    let mut exact = exact_fetch_query(&prepared, rows, union.as_deref())?;
                    // This query may have been prepared before the authority's
                    // policy descriptor arrived with its answer. Recheck simple
                    // row-local policies under that authenticated governor too.
                    exact.authority_policies = self
                        .inner()
                        .needs_authority_policies(&exact, Some(identity))?;
                    let read = ReadSpec {
                        cut: Some(Cut::at(through)),
                        include_deleted: opts.read_view.include_deleted,
                        ..ReadSpec::default()
                    };
                    let rows = self
                        .hydrate_one_shot(wait, || {
                            if scoped_evidence {
                                let evidence = evidence.as_ref().ok_or_else(|| {
                                    Error::new(
                                        ErrorCode::NotObserved,
                                        "authority omitted scoped permission evidence",
                                    )
                                })?;
                                self.inner().run_scoped_evidence(
                                    &exact,
                                    Some(identity),
                                    &read,
                                    evidence,
                                )
                            } else {
                                self.inner().run(&exact, Some(identity), &read)
                            }
                        })
                        .await?;
                    if rows
                        .iter()
                        .map(|row| *row.id.as_bytes())
                        .collect::<Vec<_>>()
                        != expected
                    {
                        return Err(Error::new(
                            ErrorCode::NotObserved,
                            "scoped permission evidence does not reproduce the authority result",
                        ));
                    }
                    if let Some(occurrences) = union {
                        unions::check(&rows, &occurrences)?;
                    }
                    return Ok(result(rows));
                }
                if opts.tier == ReadTier::Remote {
                    return Err(Error::new(
                        ErrorCode::NotObserved,
                        "scoped remote read has no available authority",
                    ));
                }
                // A local-first deadline may return the locally permitted
                // cache; it never promotes it to an authoritative answer.
            } else {
                Self::remote_fetch_shape(&prepared.query)?;
                let answer = self
                    .fetch_answer(
                        &prepared,
                        None,
                        opts.read_view.include_deleted,
                        deadline,
                        None,
                        false,
                    )
                    .await?;
                if opts.tier == ReadTier::Remote
                    && let Some((through, rows, _, union)) = answer
                {
                    if let Some(occurrences) = union {
                        let mut exact = unions::exact(&prepared, &occurrences)?;
                        let identity = author.unwrap_or(self.inner().identity.author);
                        exact.authority_policies = self
                            .inner()
                            .needs_authority_policies(&exact, Some(identity))?;
                        let read = ReadSpec {
                            cut: Some(Cut::at(through)),
                            include_deleted: opts.read_view.include_deleted,
                            ..ReadSpec::default()
                        };
                        let rows = self
                            .hydrate_one_shot(wait, || {
                                self.inner().run(&exact, Some(identity), &read)
                            })
                            .await?;
                        unions::check(&rows, &occurrences)?;
                        return Ok(result(rows));
                    }
                    // The answer names this read's roots, including an empty
                    // result. Own cached writes need not have been subscribed
                    // or fetched before, so asynchronous revocations alone do
                    // not establish coverage. Keep the original window: the
                    // remote query hydrated its prefix (or aggregate inputs).
                    let root_id = prepared.query.flat_join.as_ref().map_or_else(
                        || "id".to_owned(),
                        |flat| {
                            format!(
                                "{}.id",
                                flat.root_alias.as_ref().unwrap_or(&prepared.query.table)
                            )
                        },
                    );
                    prepared.query.filters.push(crate::query::in_list(
                        crate::query::col(root_id),
                        rows.into_iter()
                            .map(|row| crate::query::lit(Value::Uuid(RowUuid::from_bytes(row).0))),
                    ));
                }
            }
        }
        let rows = self
            .hydrate_one_shot(wait.filter(|_| !frozen), || {
                let inner = self.inner();
                let roots = inner.run(&prepared, author, &ReadSpec::of(&opts, &inner)?)?;
                inner.nest_relations(&prepared, author, roots)
            })
            .await?;
        Ok(result(rows))
    }

    /// A row answer can precede its chunk tree. Retry only that read's normal
    /// evaluation while sync supplies its missing nodes, preserving its scope,
    /// root restriction and snapshot. Immediate/local/frozen reads never wait.
    async fn hydrate_one_shot<T>(
        &self,
        wait: Option<Option<Instant>>,
        mut read: impl FnMut() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let deadline = wait.map(|deadline| {
            deadline.unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(15))
        });
        let connection = self.inner().client.as_ref().map(|client| client.connection);
        let _deadline = self.read_deadline(deadline);
        let _attempt_deadline = deadline.and_then(|_| self.read_attempt_deadline());
        poll_fn(|_| {
            if self.inner().client.as_ref().map(|client| client.connection) != connection {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "one-shot hydration interrupted by an upstream reconnect",
                )));
            }
            match read() {
                Err(error) if crate::large::is_missing(&error) && deadline.is_some() => {
                    if deadline.is_some_and(|at| Instant::now() >= at)
                        || self.link_hint().is_none_or(|link| !may_wait(link.get()))
                    {
                        return Poll::Ready(Err(Error::new(
                            ErrorCode::NotObserved,
                            "one-shot content hydration unavailable",
                        )));
                    }
                    self.schedule_pending_tick();
                    Poll::Pending
                }
                result => Poll::Ready(result),
            }
        })
        .await
    }
}

impl Db {
    /// Whether this runtime relies on an admitted owner for durable storage.
    pub fn is_non_durable_client(&self) -> bool {
        self.inner().non_durable
    }

    /// Mark this store as a non-durable foreground over a durable relay:
    /// its own store starts empty each time, so a subscription's opening,
    /// unless explicitly LocalOnly, waits for its storage owner's answer.
    /// Authority waits separately require global settlement. New writes require
    /// the current connection's subject- and schema-bound private-owner reply.
    pub fn set_non_durable_client(&self) {
        self.ensure_client();
        let mut inner = self.inner.borrow_mut();
        if inner.scope_isolated_owner {
            inner.owner_projection_error = Some(unsupported(
                "turning a scope-isolated storage owner into a foreground",
            ));
            return;
        }
        inner.non_durable = true;
        inner.seq = inner.seq.max(FOREGROUND_BASE);
    }

    /// Distinct maintained upstream queries and their attachment references.
    /// These are the actual sync registrations, including a foreground's
    /// temporary one-shot stream, rather than the local fanout group count.
    #[cfg(any(test, feature = "testing"))]
    pub fn query_coverage_attachment_counts_for_test(&self) -> (usize, usize) {
        let inner = self.inner.borrow();
        inner.client.as_ref().map_or((0, 0), |client| {
            (
                client.remote.len(),
                client.remote.values().map(|remote| remote.refs).sum(),
            )
        })
    }

    /// Request the actual installed scheduler from a host lifecycle test.
    #[cfg(any(test, feature = "testing"))]
    pub fn schedule_tick_for_test(&self, urgency: TickUrgency) {
        let scheduler = self.inner.borrow().scheduler.clone();
        if let Some(scheduler) = scheduler {
            scheduler.schedule_tick(urgency);
        }
    }

    /// Inspect the actual authority availability used by remote read admission.
    #[cfg(any(test, feature = "testing"))]
    pub fn remote_link_hint_for_test(&self) -> Option<RemoteLinkHint> {
        self.inner
            .borrow()
            .client
            .as_ref()
            .map(|client| client.hint.get().hint)
    }

    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.inner.borrow_mut().scheduler = scheduler;
        self.schedule_read_deadlines();
    }

    pub(crate) fn read_deadline(&self, at: Option<Instant>) -> Option<Rc<Instant>> {
        let deadline = at.map(Rc::new)?;
        self.inner
            .borrow_mut()
            .read_deadlines
            .push(Rc::downgrade(&deadline));
        self.schedule_read_deadlines();
        Some(deadline)
    }

    pub(crate) fn read_attempt_deadline(&self) -> Option<Rc<Instant>> {
        let link = self.link_hint()?.get();
        self.read_deadline(
            (link.hint == RemoteLinkHint::Attempting)
                .then_some(link.since + crate::options::REMOTE_LINK_ATTEMPT_WINDOW),
        )
    }

    pub(crate) fn schedule_pending_tick(&self) {
        let scheduler = self.inner.borrow().scheduler.clone();
        if let Some(scheduler) = scheduler {
            scheduler.schedule_tick(TickUrgency::Immediate);
        }
    }

    /// Hosts may keep only their earliest timer. Every tick re-arms the next
    /// outstanding deadline, even if the read whose timer fired was cancelled.
    pub(crate) fn schedule_read_deadlines(&self) {
        let (scheduler, next) = {
            let mut inner = self.inner.borrow_mut();
            let now = Instant::now();
            let mut next = None;
            inner.read_deadlines.retain(|weak| {
                let Some(deadline) = weak.upgrade() else {
                    return false;
                };
                if *deadline > now {
                    next = Some(next.map_or(*deadline, |at: Instant| at.min(*deadline)));
                }
                // Keep live guards even after their original wake expires:
                // the host may restart a connection attempt while they wait.
                true
            });
            if !inner.read_deadlines.is_empty()
                && let Some(client) = &inner.client
            {
                let link = client.hint.get();
                let at = link.since + crate::options::REMOTE_LINK_ATTEMPT_WINDOW;
                if link.hint == RemoteLinkHint::Attempting && at > now {
                    next = Some(next.map_or(at, |pending| pending.min(at)));
                }
            }
            (inner.scheduler.clone(), next)
        };
        if let (Some(scheduler), Some(next)) = (scheduler, next) {
            // Round up: an early timer must not consume the final sub-ms wait.
            let nanos = next.saturating_duration_since(Instant::now()).as_nanos();
            let millis = nanos.div_ceil(1_000_000).max(1).min(u128::from(u64::MAX)) as u64;
            scheduler.schedule_tick_after(millis);
        }
    }
}

impl Db {
    /// Test diagnostics: pending txs, shadowed rows, fates.
    pub fn sync_debug(&self) -> String {
        let inner = self.inner.borrow();
        let Some(client) = &inner.client else {
            return "not a client".to_owned();
        };
        let revoked: Vec<(String, String)> = inner
            .conn
            .prepare("SELECT t, hex(id) FROM _jazz_revoked")
            .and_then(|mut s| {
                s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()
            })
            .unwrap_or_default();
        format!(
            "pending {:?} sent {} unsent {} shadow {:?} fates {:?} connected {} unlisted {revoked:?}",
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
                .map(|((node, tx), r)| ((node, tx.saturating_sub(LOCAL_BASE)), r.is_ok()))
                .collect::<BTreeMap<_, _>>(),
            client.transport.is_some(),
        )
    }
}

impl Db {
    pub(super) async fn fetch_catalogue(
        &self,
        table: &str,
        opened: crate::ids::SchemaVersionId,
        scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<(), Error> {
        let deadline = Instant::now() + std::time::Duration::from_secs(15);
        self.fetch_catalogue_until(table, opened, scope, deadline)
            .await
    }

    async fn fetch_catalogue_until(
        &self,
        table: &str,
        opened: crate::ids::SchemaVersionId,
        scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        deadline: Instant,
    ) -> Result<(), Error> {
        let _wake = self.read_deadline(Some(deadline));
        let _attempt = self.read_attempt_deadline();
        let (id, connection) = {
            let mut inner = self.inner();
            let client = inner.client.as_mut().expect("client");
            if client.transport.is_none() || !may_wait(client.hint.get()) {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "catalogue coverage has no available authority",
                ));
            }
            let id = client.next_id;
            client.next_id += 1;
            client.catalogue_answers.insert(id, None);
            client.outbox.push(SyncMessage::CatalogueFetchV1 {
                id,
                table: table.into(),
                opened,
                scope: scope.map(|(author, claims)| (author.canonical().to_owned(), claims)),
            });
            (id, client.connection)
        };
        let _request = FetchRequest {
            inner: self.inner.clone(),
            id,
        };
        self.schedule_pending_tick();
        poll_fn(|_| {
            let mut inner = self.inner();
            let client = inner.client.as_mut().expect("client");
            if client.connection != connection {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "catalogue coverage interrupted by reconnect",
                )));
            }
            if let Some(Some(answer)) = client.catalogue_answers.remove(&id) {
                return Poll::Ready(answer.and_then(|(head, version)| {
                    if inner.catalogue_head()?.as_ref() != Some(&head)
                        || inner.catalogue_schema(opened)?.is_none()
                    {
                        Err(Error::new(
                            ErrorCode::NotObserved,
                            "catalogue coverage metadata changed before completion",
                        ))
                    } else if version == opened {
                        Ok(())
                    } else {
                        Err(Error::new(
                            ErrorCode::Protocol,
                            "catalogue coverage answered another schema",
                        ))
                    }
                }));
            }
            // Keep an unanswered slot; removing it would lose a later answer.
            client.catalogue_answers.insert(id, None);
            if client.transport.is_none()
                || !may_wait(client.hint.get())
                || Instant::now() >= deadline
            {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "fresh catalogue coverage unavailable",
                )));
            }
            Poll::Pending
        })
        .await
    }

    pub(super) fn broadcast_catalogue_metadata(&self) -> Result<(), Error> {
        let count = self.inner.borrow().sessions.len();
        for index in 0..count {
            self.queue_catalogue_metadata(index)?;
        }
        Ok(())
    }
    fn queue_catalogue_metadata(&self, index: usize) -> Result<(), Error> {
        let mut inner = self.inner();
        if !inner.sessions[index].open {
            return Ok(());
        }
        inner.sessions[index].catalogue_pending = false;
        if inner.catalogue_head()?.is_none() {
            return Ok(());
        }
        let opened = inner.sessions[index]
            .version
            .unwrap_or(inner.owner.jazz.version_id());
        let (head, schemas) = inner.catalogue_records(opened)?;
        inner.sessions[index]
            .outbox
            .push(SyncMessage::CatalogueMetadataV1 { head, schemas });
        if let Some(descriptor) = inner.policy_descriptor()? {
            inner.sessions[index]
                .outbox
                .push(SyncMessage::PolicyDescriptorV1(descriptor));
        }
        Ok(())
    }
    fn forward_catalogue_fetch(
        &self,
        index: usize,
        id: u64,
        table: String,
        opened: crate::ids::SchemaVersionId,
        scope: (AuthorSubject, BTreeMap<String, Value>),
    ) -> Result<(), Error> {
        let deadline = self
            .read_deadline(Some(Instant::now() + std::time::Duration::from_secs(15)))
            .expect("deadline");
        let mut inner = self.inner();
        if inner.sessions[index].forwarded.contains_key(&id)
            || inner.sessions[index].forwarded_catalogue.contains_key(&id)
        {
            return Err(query_error("duplicate forwarded request ID"));
        }
        let owner = inner.owner.clone();
        let client = inner.client.as_mut().expect("relay");
        if client.transport.is_none() || !may_wait(client.hint.get()) {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "catalogue relay has no available authority",
            ));
        }
        let upstream = client.next_id;
        client.next_id += 1;
        let connection = client.connection;
        client.catalogue_answers.insert(upstream, None);
        client.outbox.push(SyncMessage::CatalogueFetchV1 {
            id: upstream,
            table,
            opened,
            scope: Some((scope.0.canonical().to_owned(), scope.1)),
        });
        inner.sessions[index].forwarded_catalogue.insert(
            id,
            ForwardedCatalogue {
                upstream,
                connection,
                opened,
                owner,
                deadline,
            },
        );
        Ok(())
    }
    fn progress_catalogue_fetches(&self) {
        let mut inner = self.inner();
        for index in 0..inner.sessions.len() {
            let mut ready = Vec::new();
            for (&id, request) in &inner.sessions[index].forwarded_catalogue {
                let answer = if !Rc::ptr_eq(&request.owner, &inner.owner)
                    || Instant::now() >= *request.deadline
                    || inner.client.as_ref().is_none_or(|c| {
                        c.connection != request.connection
                            || c.transport.is_none()
                            || !may_wait(c.hint.get())
                    }) {
                    Some(Err(Error::new(
                        ErrorCode::NotObserved,
                        "catalogue relay coverage interrupted or unavailable",
                    )))
                } else {
                    inner
                        .client
                        .as_ref()
                        .expect("relay")
                        .catalogue_answers
                        .get(&request.upstream)
                        .cloned()
                        .flatten()
                };
                if let Some(answer) = answer {
                    ready.push((id, answer));
                }
            }
            for (id, answer) in ready {
                let request = inner.sessions[index]
                    .forwarded_catalogue
                    .remove(&id)
                    .expect("pending");
                if let Some(client) = inner.client.as_mut() {
                    client.cancel_fetch(request.upstream);
                }
                let result = answer.and_then(|(head, opened)| {
                    let (cached, schemas) = inner.catalogue_records(opened)?;
                    if cached != head || opened != request.opened {
                        return Err(Error::new(
                            ErrorCode::NotObserved,
                            "catalogue relay lacks the exact covered metadata",
                        ));
                    }
                    Ok((head, opened, schemas, inner.policy_descriptor()?))
                });
                match result {
                    Ok((head, opened, schemas, descriptor)) => {
                        inner.sessions[index]
                            .outbox
                            .push(SyncMessage::CatalogueMetadataV1 {
                                head: head.clone(),
                                schemas,
                            });
                        if let Some(descriptor) = descriptor {
                            inner.sessions[index]
                                .outbox
                                .push(SyncMessage::PolicyDescriptorV1(descriptor));
                        }
                        inner.sessions[index]
                            .outbox
                            .push(SyncMessage::CatalogueCoveredV1 { id, head, opened });
                    }
                    Err(error) => inner.sessions[index].outbox.push(SyncMessage::Refused {
                        id,
                        code: error.code,
                        message: error.message,
                    }),
                }
            }
        }
    }
}
