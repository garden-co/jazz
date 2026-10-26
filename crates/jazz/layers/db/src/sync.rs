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

use crate::schema::history::{history_source_table, is_history_table};
use crate::values::CachedRow;
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
    /// Why this transport stopped yielding messages, once: a peer's frame
    /// it cannot decode ends the link rather than the process. The
    /// database then drops the link (a client detaches, a server closes
    /// the session).
    fn take_failure(&mut self) -> Option<TransportError> {
        None
    }
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
pub struct WireTransportAdapter<W> {
    wire: W,
    joiner: FrameJoiner,
    /// A frame failed to decode: nothing more is read, and
    /// [`Transport::take_failure`] reports it once.
    failed: Option<Option<TransportError>>,
}

/// The largest encoded message one wire frame carries, leaving room for
/// the carrier's count and length prefixes.
const MAX_MESSAGE_FRAME_BYTES: usize = jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES - 64;

/// `message` as wire frames. A `Rows` message can outgrow one frame (a
/// first result, or one commit's fan-out): its images travel in as many
/// frames as it takes (a row fits one, as a large value travels as
/// chunks), each but the last a `RowsPart`, and [`FrameJoiner`] makes
/// them one message again.
pub fn encode_frames(message: SyncMessage) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let SyncMessage::Rows { images } = message else {
        return Ok(vec![postcard::to_allocvec(&message)?]);
    };
    let mut parts = Vec::new();
    split_rows(images, &mut parts)?;
    let last = parts.len() - 1;
    for (i, (images, frame)) in parts.iter_mut().enumerate() {
        if i < last {
            *frame = postcard::to_allocvec(&SyncMessage::RowsPart {
                images: std::mem::take(images),
            })?;
        }
    }
    Ok(parts.into_iter().map(|(_, frame)| frame).collect())
}

/// `images` halved until each half encodes as a `Rows` within one frame,
/// in order, each with that encoding.
fn split_rows(
    images: Vec<Image>,
    parts: &mut Vec<(Vec<Image>, Vec<u8>)>,
) -> Result<(), postcard::Error> {
    let message = SyncMessage::Rows { images };
    let frame = postcard::to_allocvec(&message)?;
    let SyncMessage::Rows { mut images } = message else {
        unreachable!()
    };
    if frame.len() <= MAX_MESSAGE_FRAME_BYTES || images.len() <= 1 {
        parts.push((images, frame));
        return Ok(());
    }
    let rest = images.split_off(images.len() / 2);
    split_rows(images, parts)?;
    split_rows(rest, parts)
}

/// One frame's [`SyncMessage`]: exactly one postcard value, so a frame
/// with bytes after its message is malformed rather than truncated.
pub fn decode_message(frame: &[u8]) -> Result<SyncMessage, postcard::Error> {
    match postcard::take_from_bytes(frame)? {
        (message, []) => Ok(message),
        _ => Err(postcard::Error::DeserializeBadEncoding),
    }
}

/// Joins a `Rows` message's frames (see [`encode_frames`]) back into the
/// one message they encode.
#[derive(Default)]
pub struct FrameJoiner {
    /// The images of the parts received since the last whole message.
    parts: Option<Vec<Image>>,
}

impl FrameJoiner {
    /// The message `frame` completes, if any. A frame that does not decode,
    /// or a message other than `Rows` after a `RowsPart`, is the peer's
    /// error.
    pub fn push(&mut self, frame: &[u8]) -> Result<Option<SyncMessage>, TransportError> {
        let message = decode_message(frame)
            .map_err(|error| TransportError::Failed(format!("malformed sync frame: {error}")))?;
        match (self.parts.take(), message) {
            (parts, SyncMessage::RowsPart { images }) => {
                self.parts = Some(parts.unwrap_or_default());
                self.parts.as_mut().expect("parts").extend(images);
                Ok(None)
            }
            (Some(mut joined), SyncMessage::Rows { images }) => {
                joined.extend(images);
                Ok(Some(SyncMessage::Rows { images: joined }))
            }
            (Some(_), message) => Err(TransportError::Failed(format!(
                "a Rows message's parts end with its Rows frame, not {}",
                message_kind(&message)
            ))),
            (None, message) => Ok(Some(message)),
        }
    }
}

/// A message's variant name, for errors that must not dump its contents.
fn message_kind(message: &SyncMessage) -> String {
    let debug = format!("{message:?}");
    debug
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or_default()
        .to_owned()
}

impl<W> WireTransportAdapter<W> {
    pub fn current(wire: W) -> Self {
        Self {
            wire,
            joiner: FrameJoiner::default(),
            failed: None,
        }
    }
}

impl<W: WireTransport> Transport for WireTransportAdapter<W> {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        for frame in encode_frames(message).map_err(|e| TransportError::Failed(e.to_string()))? {
            self.wire.send_frame(frame)?;
        }
        Ok(())
    }

    /// The next whole message: a `Rows` whose frames have not all arrived
    /// waits for the rest.
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.failed.is_some() {
            return None;
        }
        loop {
            let frame = self.wire.try_recv_frame()?;
            match self.joiner.push(&frame) {
                Ok(Some(message)) => return Some(message),
                Ok(None) => {}
                Err(error) => {
                    self.failed = Some(Some(error));
                    return None;
                }
            }
        }
    }

    fn take_failure(&mut self) -> Option<TransportError> {
        self.failed.as_mut().and_then(Option::take)
    }
}

/// Which end of a connection reads a frame batch, at which point, for
/// [`validate_frame_batch_for_corpus`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorpusReader {
    /// The server's first batch: exactly one `Hello`.
    ServerOpening,
    /// The server after the `Hello`: every frame is a sync message.
    Server,
    /// A client: each frame is a `Hello` (tag 0), an `Error` (tag 1) or a
    /// sync message, as its websocket reads them. No server-sent message
    /// has tag 0 or 1.
    Client,
}

/// Test-only: decode one websocket frame batch the way `reader` does, with
/// the production decoders, negotiating a `Hello` against
/// `negotiated_features`. NAPI and WASM expose it so the pinned wire corpus
/// (`crates/jazz/fixtures/sqlite_sync_wire_v1.json`) runs through each
/// shipped artifact. Not a host API.
#[doc(hidden)]
pub fn validate_frame_batch_for_corpus(
    batch: &[u8],
    reader: CorpusReader,
    negotiated_features: crate::wire::WireFeatures,
) -> Result<(), String> {
    use crate::wire::{
        WireFrame, decode_frame, decode_hello_frame, decode_websocket_frame_batch, negotiate_wire,
    };
    let hello = |frame: &[u8]| {
        decode_hello_frame(frame)
            .and_then(|hello| negotiate_wire(&hello, negotiated_features))
            .map(drop)
            .map_err(|error| format!("hello rejected: {}", error.message))
    };
    let message = |frame: &[u8]| {
        decode_message(frame)
            .map(drop)
            .map_err(|error| format!("malformed sync message: {error}"))
    };
    let frames = decode_websocket_frame_batch(batch)
        .map_err(|error| format!("malformed websocket frame batch: {error}"))?;
    match reader {
        CorpusReader::ServerOpening => match frames.as_slice() {
            [frame] => hello(frame),
            _ => Err("the opening batch must be exactly one Hello".to_owned()),
        },
        CorpusReader::Server => frames.iter().try_for_each(|frame| message(frame)),
        CorpusReader::Client => frames.iter().try_for_each(|frame| match frame.first() {
            Some(0) => hello(frame),
            Some(1) => match decode_frame(frame) {
                Ok(WireFrame::Error(_)) => Ok(()),
                _ => Err("malformed WireFrame::Error".to_owned()),
            },
            _ => message(frame),
        }),
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

/// Whether `image` is an older unsettled version of a relay's than the
/// `held` one: a relay numbers its unsettled versions from `LOCAL_BASE` up,
/// in its own order, and ships older ones in as-of answers. Cuts never read
/// them (a snapshot naming another replica's unsettled commit is
/// unavailable), and their seqs may collide with this replica's own, so the
/// replica keeps neither as history.
fn relay_older(held: u64, image: u64) -> bool {
    image >= LOCAL_BASE && held > image
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

/// A row a commit writes, as the writer's replica holds it afterwards:
/// [`echo_digest`] of its image (in the session schema's names).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Expect {
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub digest: [u8; 16],
}

/// The provenance cells the applying clock stamps: a writer's replica
/// holds its own clock's, so the authority's travel in the Fate ([`Held`]).
const STAMPS: [&str; 2] = ["$createdAt", "$updatedAt"];

/// What a writer and the authority compare to agree on a row version
/// without shipping it: everything in an image but its seq and
/// [`STAMPS`].
fn echo_digest(image: &Image) -> [u8; 16] {
    let mut cells: Vec<&(String, Cell)> = image
        .cells
        .iter()
        .filter(|(name, _)| !STAMPS.contains(&name.as_str()))
        .collect();
    cells.sort_by(|a, b| a.0.cmp(&b.0));
    let bytes = postcard::to_allocvec(&(
        &image.table,
        &image.id,
        &image.key,
        image.deleted,
        image.content,
        cells,
    ))
    .expect("postcard encodes images");
    let hash = blake3::hash(&bytes);
    hash.as_bytes()[..16].try_into().expect("16 bytes")
}

/// A row of a commit's `expect` the server did not ship (see
/// `SyncMessage::Fate`), with the authority's [`STAMPS`] for it: `None`
/// where its version has no such cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Held {
    pub index: u32,
    pub created_at: Option<Cell>,
    pub updated_at: Option<Cell>,
}

impl Held {
    fn of(index: u32, image: &Image) -> Self {
        let stamp = |name: &str| {
            image
                .cells
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, cell)| cell.clone())
        };
        Held {
            index,
            created_at: stamp(STAMPS[0]),
            updated_at: stamp(STAMPS[1]),
        }
    }

    /// `image` with the authority's stamps.
    fn stamp(&self, image: &mut Image) {
        for (name, cell) in STAMPS.iter().zip([&self.created_at, &self.updated_at]) {
            image.cells.retain(|(n, _)| n != name);
            if let Some(cell) = cell {
                image.cells.push(((*name).to_owned(), cell.clone()));
            }
        }
    }
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

/// A replicated write. `at`: its explicit provenance timestamp (Unix ms),
/// if the writer gave one; each write in a commit carries its own.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireOp {
    Insert {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
        at: Option<u64>,
    },
    Update {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
        at: Option<u64>,
    },
    Upsert {
        table: String,
        row: [u8; 16],
        cells: BTreeMap<String, Value>,
        target: WireTarget,
        at: Option<u64>,
    },
    Delete {
        table: String,
        row: [u8; 16],
        target: WireTarget,
        at: Option<u64>,
    },
    Restore {
        table: String,
        row: [u8; 16],
        cells: Option<BTreeMap<String, Value>>,
        target: WireTarget,
        at: Option<u64>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    // client → server
    /// `since`: a resumed subscription the client held settled through
    /// that seq; its opening ships only rows changed after it.
    /// `include_deleted`: the result also lists deleted rows (the root's),
    /// which ship as their deleted current versions.
    /// `tier`: how far the subscription reaches, as for `Fetch`: a relay
    /// serves a LocalOnly one from its store alone, without its upstream
    /// (a non-durable foreground's LocalOnly read). An authority's store
    /// is the answer either way.
    /// `wait`: how long (ms) a relay waits for its upstream's answer
    /// before it serves its own store: a LocalFirst one's first load (the
    /// reader's `firstLoadRemoteWaitMs`). `None`: a LocalFirst one waits
    /// while the upstream may answer, a Remote one however long it takes
    /// (only the authority confirms a remote result).
    Subscribe {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        since: Option<u64>,
        include_deleted: bool,
        tier: ReadTier,
        wait: Option<u64>,
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
    /// `tier` is how far it reaches past a relay: `LocalOnly` is served
    /// from the relay's store, `LocalFirst` too while the relay asks its
    /// upstream to refresh it, `Remote` once its upstream answered. An
    /// authority's store answers any tier. `wait`: how long (ms) a relay
    /// waits for its upstream's answer while it may give one, then serves
    /// its store: a LocalFirst read's first load, or a Remote read that
    /// takes the relay's view when the authority cannot answer (a
    /// serializable transaction's snapshot). `None`: a LocalFirst read is
    /// served now, a Remote one once the upstream answered. `view`: the
    /// branch view the read observes, so its window and versions are the
    /// view's. A view frozen at a snapshot reference is resolved by the
    /// receiver (a reopened tab's capture, which its worker settled), which
    /// also sends the [`SyncMessage::Origins`] of the commits it names.
    Fetch {
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        at: Option<u64>,
        include_deleted: bool,
        tier: ReadTier,
        wait: Option<u64>,
        view: ReadViewSourceSpec,
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
        /// The rows as the writer's replica holds them after these writes
        /// (see [`Expect`]): the authority skips echoing a row whose new
        /// version is exactly that. Empty when the writer does not know.
        expect: Vec<Expect>,
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
    /// `held`: the commit's `expect` entries whose row the server did not
    /// ship because its version at `through` is what the writer expected:
    /// the writer installs its own, with the authority's stamps, as that
    /// version. `global`: the fate is the global authority's (directly, or
    /// relayed from it), not the last word of a replica with no server,
    /// which orders its sessions' commits but cannot make them `Global`.
    Fate {
        tx: u64,
        through: u64,
        result: Result<(), WireError>,
        held: Vec<Held>,
        global: bool,
    },
    // client → server
    /// A permission preflight: would the session's identity be allowed
    /// `action` right now? Never writes. `author` and `session` name the
    /// session a trusted link asks for, as on a [`SyncMessage::Commit`].
    Advise {
        id: u64,
        action: PermissionAdviceAction,
        author: Option<String>,
        session: Option<BTreeMap<String, Value>>,
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
    /// The transactions behind shipped row versions, as (seq, writer
    /// node, writer tx), sent ahead of their `Rows`. A replica uses these
    /// for history's `$transactionId` and to avoid replaying an accepted
    /// write over its own result. Only `Fate` settles a pending write.
    Origins {
        commits: Vec<(u64, [u8; 16], u64)>,
    },
    /// Whether the server has admitted the schema version the session
    /// declared, sent when that changes. Until it is admitted the server
    /// holds the session's messages (a draft awaiting its lens), so a
    /// read that must be covered by the server fails with the reason
    /// rather than waiting out its budget.
    SchemaAdmission {
        admitted: bool,
    },
    /// The leading images of a `Rows` message too large for one wire
    /// frame; the message's final frame is the `Rows` itself. A receiver
    /// joins the parts and applies them as that one message, so a
    /// commit's rows reach readers whole (a wire framing only: never sent
    /// as a message of its own).
    RowsPart {
        images: Vec<Image>,
    },
    /// A relay holds the session's commit `tx` in its store and uploads
    /// it: the commit is `Local` for a non-durable session (a tab over its
    /// worker), whose own store is memory. Its fate follows once the
    /// relay's upstream answers.
    Stored {
        tx: u64,
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

impl Kind {
    /// Whether the authority must see the commit even with no writes.
    pub(crate) fn validates_reads(&self) -> bool {
        matches!(self, Kind::Serializable { reads, .. } if !reads.is_empty())
    }
}

struct Pending {
    tx: u64,
    ops: Vec<StagedOp>,
    rows: Vec<(String, RowUuid, Vec<u8>)>,
    /// The commit's `expect` rows as stored here, in its order: the server
    /// may name them `held` instead of shipping them.
    expected: Vec<Image>,
}

/// A row an accepted commit wrote that the writer said it holds.
struct Echo {
    tx: u64,
    index: u32,
    table: String,
    row: RowUuid,
    key: Vec<u8>,
    seq: u64,
    digest: [u8; 16],
}

/// One remote attachment: a client query the server maintains.
pub(crate) struct Remote {
    id: u64,
    refs: usize,
    query: Query,
    binding: BTreeMap<String, Value>,
    include_deleted: bool,
    /// How far it reaches (see `SyncMessage::Subscribe`).
    reach: RemoteReach,
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
}

/// The snapshot reference a read view is frozen at, if any.
fn frozen_at(source: &ReadViewSourceSpec) -> Option<&SnapshotRef> {
    match source {
        ReadViewSourceSpec::BranchView {
            base: Some(BranchViewBase::Snapshot { snapshot, .. }),
            ..
        }
        | ReadViewSourceSpec::Snapshot { snapshot } => Some(snapshot),
        _ => None,
    }
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
    /// Accepted fates from a replica with no server: they order the
    /// commit, but it never becomes `Global`.
    local_fates: HashSet<u64>,
    /// Commits the relay this non-durable replica writes through holds
    /// in its store, their fate still to come ([`SyncMessage::Stored`]).
    stored: HashSet<u64>,
    shadow: HashMap<(String, RowUuid, Vec<u8>), Option<Image>>,
    /// Highest server seq any answer was complete through.
    pub(crate) known: u64,
    /// The `known` cut last written to `_jazz_meta`.
    persisted_known: u64,
    answers: HashMap<u64, Option<Result<(u64, Vec<[u8; 16]>), Error>>>,
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
    /// As a relay: sessions' commits this store accepted and uploads as
    /// its own, by upload tx. Each session gets its fate once the
    /// upstream's arrives (`(session, session tx, rows it wrote)`); a
    /// refused one also gets the rows as this store reverted them.
    relayed_fates: HashMap<u64, Vec<RelayedFate>>,
    /// As a relay: sessions' one-shot reads asked of the upstream, served
    /// from this store once it answers.
    relayed_fetches: Vec<RelayedFetch>,
    /// A relayed fate or read became ready: the serving half runs again.
    relay_ready: bool,
    /// The server said it has not admitted this client's schema version.
    unadmitted: bool,
    /// A new session has not yet been told which rows this replica holds:
    /// the next send reads them and puts the `Hold` right after `Schema`.
    hold_owed: bool,
}

/// A session's commit a relay uploads as its own: (session, the session's
/// tx, the rows it wrote).
type RelayedFate = (usize, u64, Vec<(String, RowUuid, Vec<u8>)>);

/// A session's `Fetch` a relay asked its upstream for.
/// A relayed read's upstream answer: its query, binding, whether it read
/// deleted rows, and the rows it listed.
type RelayedAnswer = (Query, BTreeMap<String, Value>, bool, Vec<[u8; 16]>, u64);

struct RelayedFetch {
    /// The relay's own fetch id upstream.
    upstream: u64,
    /// What it asked its upstream: asked again on a new connection, since
    /// an answer the last one owed is lost with it.
    ask: SyncMessage,
    /// A Remote read with no wait: served only once the upstream answered.
    strict: bool,
    /// Otherwise it is served from this store once the upstream can no
    /// longer answer, or its wait runs out (`None`: never).
    until: Option<Instant>,
    session: usize,
    /// The session's `Fetch`, served locally once the upstream answered.
    message: SyncMessage,
}

fn remote_key(
    query: &Query,
    binding: &BTreeMap<String, Value>,
    include_deleted: bool,
    reach: RemoteReach,
) -> String {
    format!("{query:?}{binding:?}{include_deleted}{reach:?}")
}

/// How far a read reaches past a relay: its tier, and how long the relay
/// waits for its upstream (see `SyncMessage::Subscribe` and `Fetch`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RemoteReach {
    pub(crate) tier: ReadTier,
    pub(crate) wait: Option<u64>,
}

impl RemoteReach {
    /// What the authority lists, once it answers.
    pub(crate) const REMOTE: RemoteReach = RemoteReach {
        tier: ReadTier::Remote,
        wait: None,
    };

    /// What the authority lists while it can answer, else the relay's
    /// view: a serializable transaction's snapshot read, which pins
    /// offline at the cut the replica knows.
    pub(crate) const BEST_EFFORT: RemoteReach = RemoteReach {
        tier: ReadTier::Remote,
        wait: Some(u64::MAX),
    };
}

/// When a wait of `ms` from now runs out; `None` if never.
fn wait_until(ms: u64) -> Option<Instant> {
    Instant::now().checked_add(std::time::Duration::from_millis(ms))
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
    /// A non-durable foreground's local read: its relay answers from its
    /// store whatever the relay's own path to the server, so it waits
    /// while the link to the relay is attached.
    pub(crate) relay_view: bool,
}

impl Gate {
    /// Whether the read may keep waiting: its deadline has not passed and
    /// the link may still answer.
    pub(crate) fn may_still_wait(&self) -> bool {
        if self.relay_view {
            return !self.hint.get().detached;
        }
        self.deadline
            .is_none_or(|deadline| Instant::now() < deadline)
            && may_wait(self.hint.get())
    }

    pub(crate) fn open(&self) -> bool {
        if self.answered.get() {
            return true;
        }
        let link = self.hint.get();
        if self.relay_view {
            return link.detached;
        }
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
}

impl SettledBy {
    pub(crate) fn get(&self) -> bool {
        match self {
            SettledBy::Always(_) => true,
            SettledBy::Never => false,
            SettledBy::Remote(answered, _) => answered.get(),
        }
    }

    /// The durability a settled result has.
    pub(crate) fn tier(&self) -> DurabilityTier {
        let global = match self {
            SettledBy::Always(authority) => *authority,
            SettledBy::Never => false,
            SettledBy::Remote(answered, global) => answered.get() && global.get(),
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
type RemoteRead = (Query, BTreeMap<String, Value>, bool);

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
    /// This tick's accepted commits' `expect` rows: the image of each is
    /// not shipped while it is the row's current version and matches.
    echoes: Vec<Echo>,
    /// Supporting rows each subscription needs besides its own result.
    support: BTreeMap<u64, Rc<Vec<Support>>>,
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
    /// Inbound messages held back by the rate limit, or reads waiting for
    /// large-value chunks from this relay's upstream, in order.
    backlog: VecDeque<SyncMessage>,
    /// The schema version the client declared (`None`: the current one).
    version: Option<crate::ids::SchemaVersionId>,
    /// Fetches whose upstream answer arrived, or that this relay already
    /// asked its upstream to refresh: served from this store.
    fetched_upstream: BTreeSet<u64>,
    /// The client was told its declared version is not admitted.
    told_unadmitted: bool,
    /// On a relay, the upstream subscription each of the client's
    /// subscriptions holds (query, binding, include_deleted): its results
    /// settle once the upstream answers, never from the relay's store.
    relayed: BTreeMap<u64, RemoteRead>,
    /// That version's schema, governed by the current schema's policies,
    /// as built for the current schema it holds.
    schema: Option<(Rc<Schema>, Rc<Schema>)>,
    /// On a relay, LocalFirst subscriptions waiting for the upstream's
    /// first answer for at most their first-load wait.
    first_load: BTreeMap<u64, FirstLoadWait>,
}

/// A relayed LocalFirst subscription's first-load wait: once `until`
/// passes unanswered, its result is this store's (see
/// [`Db::serve_due_first_loads`]).
struct FirstLoadWait {
    until: Instant,
    prepared: PreparedQuery,
    include_deleted: bool,
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
            echoes: Vec::new(),
            support: BTreeMap::new(),
            watch: HashSet::new(),
            resume: BTreeMap::new(),
            held: BTreeMap::new(),
            open: true,
            ingress: (Instant::now(), 0),
            backlog: VecDeque::new(),
            version: None,
            fetched_upstream: BTreeSet::new(),
            told_unadmitted: false,
            schema: None,
            relayed: BTreeMap::new(),
            first_load: BTreeMap::new(),
        }
    }

    /// Release everything the session holds; its shared results are swept
    /// once no other member reads them.
    /// Returns the upstream subscriptions it relayed, for the caller to
    /// release.
    #[must_use]
    fn close(&mut self, dropped: &std::cell::Cell<usize>) -> Vec<RemoteRead> {
        dropped.set(dropped.get() + self.subs.len());
        let relayed = std::mem::take(&mut self.relayed);
        *self = Session {
            open: false,
            ..Session::new(Box::new(ClosedTransport), self.identity)
        };
        relayed.into_values().collect()
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
/// from the result and its edges ([`Support::Reach`]), reaching root rows
/// by id: seeded by root rows themselves, or by rows that reference root
/// rows (a hop), which ship as [`Support::Array`] children of what they
/// reference. Filters after the gather apply on the replica
/// ([`remote_query`]).
fn gather_shaped(query: &Query, via: &crate::query::ReachableVia) -> bool {
    via.seed.as_ref().is_some_and(|seed| {
        seed.user_column.is_none() && (seed.team_column != "id" || seed.table == query.table)
    }) && via.access_table == query.table
        && via.access_row_column == "id"
        && via.access_team_column == "id"
        && via.access_filters.is_empty()
}

/// What a remote read of `query` subscribes to: the query itself, or for
/// an aggregate its input rows (its filters and traversals, every column,
/// no window), which the client aggregates as it would local rows.
pub(crate) fn remote_query(query: &Query) -> std::borrow::Cow<'_, Query> {
    if let Some(prefix) = crate::db::union_prefix(query) {
        return std::borrow::Cow::Owned(prefix);
    }
    // A gather's closure runs through rows its later filters (or window)
    // leave out: the replica holds the whole closure and narrows it itself.
    if !query.reachable.is_empty()
        && (!query.filters.is_empty() || query.limit.is_some() || query.offset > 0)
    {
        let mut closure = query.clone();
        closure.filters.clear();
        closure.aggregate = None;
        closure.select = None;
        closure.order_by.clear();
        closure.limit = None;
        closure.offset = 0;
        return std::borrow::Cow::Owned(closure);
    }
    // A window past an offset is only right over the rows before it too:
    // the replica holds the whole prefix and applies the offset itself.
    if query.aggregate.is_none() && query.offset > 0 {
        let mut prefix = query.clone();
        prefix.limit = query.limit.map(|limit| limit + query.offset);
        prefix.offset = 0;
        return std::borrow::Cow::Owned(prefix);
    }
    if query.aggregate.is_none() {
        return std::borrow::Cow::Borrowed(query);
    }
    let mut input = query.clone();
    input.aggregate = None;
    input.select = None;
    input.order_by.clear();
    input.limit = None;
    input.offset = 0;
    std::borrow::Cow::Owned(input)
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
            at: opts.updated_at_ms,
        },
        StagedOp::Update(table, row, cells, opts) => WireOp::Update {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Upsert(table, row, cells, opts) => WireOp::Upsert {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Delete(table, row, opts) => WireOp::Delete {
            table: table.clone(),
            row: bytes(row),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Restore(table, row, cells, opts) => WireOp::Restore {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: exact_target(&opts.target),
            at: opts.updated_at_ms,
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
            at,
        } => StagedOp::Insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(id(row)),
                target: exact(target),
                updated_at_ms: at,
                ..Default::default()
            },
        ),
        WireOp::Update {
            table,
            row,
            cells,
            target,
            at,
        } => StagedOp::Update(
            table,
            id(row),
            cells,
            UpdateOptions {
                target: view_target(target),
                updated_at_ms: at,
                ..Default::default()
            },
        ),
        WireOp::Upsert {
            table,
            row,
            cells,
            target,
            at,
        } => StagedOp::Upsert(
            table,
            id(row),
            cells,
            UpsertOptions {
                target: view_target(target),
                updated_at_ms: at,
                ..Default::default()
            },
        ),
        WireOp::Delete {
            table,
            row,
            target,
            at,
        } => StagedOp::Delete(
            table,
            id(row),
            DeleteOptions {
                target: view_target(target),
                updated_at_ms: at,
                ..Default::default()
            },
        ),
        WireOp::Restore {
            table,
            row,
            cells,
            target,
            at,
        } => StagedOp::Restore(
            table,
            id(row),
            cells,
            RestoreOptions {
                target: exact(target),
                updated_at_ms: at,
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
        // A union's rows are read through every arm: each arm's support.
        if let Some(plan) = crate::db::union_plan(query, &self.schema)? {
            let mut support = Vec::new();
            for (_, arm) in &plan.arms {
                support.extend(self.support_of(arm)?);
            }
            return Ok(support);
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
        support.extend(self.inherited_support(table, crate::compile::DEFAULT_INHERITS_DEPTH)?);
        for via in &query.reachable {
            // Seed rows that reference the rows the walk starts from.
            if let Some(seed) = via.seed.as_ref().filter(|seed| seed.team_column != "id") {
                let seeds = self.schema.table(&seed.table)?;
                support.push(Support::Array {
                    table: seeds.stored.clone(),
                    inner: seeds.stored_name(&seed.team_column).to_owned(),
                    outer: "id".to_owned(),
                    outer_array: false,
                    inner_array: crate::compile::is_array(seeds, &seed.team_column),
                    nested: Vec::new(),
                });
            }
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

    /// The parents whose read permission rows of `table` inherit, `depth`
    /// hops up: a replica holds the row its read rests on, as it would
    /// had it read that row itself.
    fn inherited_support(
        &self,
        table: &crate::values::Table,
        depth: usize,
    ) -> Result<Vec<Support>, Error> {
        use crate::tools::{Operation, PolicyExpr};
        fn hops<'p>(policy: &'p PolicyExpr, out: &mut Vec<(&'p str, Option<usize>)>) {
            match policy {
                PolicyExpr::Inherits {
                    operation: Operation::Select,
                    via_column,
                    max_depth,
                } => out.push((via_column, *max_depth)),
                PolicyExpr::And(parts) | PolicyExpr::Or(parts) => {
                    parts.iter().for_each(|part| hops(part, out))
                }
                _ => {}
            }
        }
        let mut support = Vec::new();
        if depth == 0 {
            return Ok(support);
        }
        let mut via = Vec::new();
        if let Some(policy) = table.policy(Operation::Select) {
            hops(policy, &mut via);
        }
        for (column, max_depth) in via {
            let Some(target) = table.references.get(column) else {
                continue;
            };
            let parent = self.schema.table(target)?;
            let depth = max_depth.map_or(depth, |bound| bound.min(depth));
            if depth == 0 {
                continue;
            }
            support.push(Support::Array {
                table: parent.stored.clone(),
                inner: "id".to_owned(),
                outer: table.stored_name(column).to_owned(),
                outer_array: crate::compile::is_array(table, column),
                inner_array: false,
                nested: self.inherited_support(parent, depth - 1)?,
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
            let source = join.source_column.as_deref().unwrap_or("id");
            support.push(Support::Array {
                table: target.stored.clone(),
                inner: target.stored_name(&join.on_column).to_owned(),
                outer: parent.stored_name(source).to_owned(),
                outer_array: crate::compile::is_array(parent, source),
                inner_array: crate::compile::is_array(target, &join.on_column),
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
        let mut ctx = crate::compile::Ctx::new(&schema, None, &claims, &binding);
        ctx.operation_policy(table, Operation::Select, "a", &session)?;
        let mut tables = ctx.tables;
        tables.remove(&table.stored);
        Ok(tables)
    }

    /// Every settled row version in the replica (pending local versions sit
    /// above `LOCAL_BASE`; their settled images are in the shadow), by
    /// stored table.
    /// The `Hold` a new session starts with: every settled row the replica
    /// holds, whether or not a live query still reads it. The session
    /// watches them, so a later change (a deletion, lost access) still
    /// reaches the cached copy.
    fn hold(&self) -> Result<Option<SyncMessage>, Error> {
        let client = self.client.as_ref().expect("client");
        let mut rows: Vec<(String, [u8; 16], Vec<u8>, u64)> = client
            .shadow
            .iter()
            .filter_map(|((table, row, key), image)| {
                image
                    .as_ref()
                    .map(|image| (table.clone(), *row.0.as_bytes(), key.clone(), image.seq))
            })
            .collect();
        rows.extend(self.held_versions()?);
        if rows.is_empty() {
            return Ok(None);
        }
        // Held rows name tables as this store's schema does.
        let rows = rows
            .into_iter()
            .filter_map(|(table, id, key, seq)| {
                Some((self.owner.by_stored(&table)?.name.clone(), id, key, seq))
            })
            .collect();
        Ok(Some(SyncMessage::Hold { rows }))
    }

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
            Some(current) if relay_older(current.seq, image.seq) => {
                if self.non_durable {
                    self.put_image(image, true)?;
                    self.changed
                        .borrow_mut()
                        .push((image.table.clone(), row, image.key.clone()));
                }
            }
            Some(current) if current.seq < image.seq || current.seq >= LOCAL_BASE => {
                // A relay's newer version: its older one stays as history.
                let relay_newer = self.non_durable && current.seq >= LOCAL_BASE;
                let relay_newer = relay_newer && current.seq < image.seq;
                if current.seq < LOCAL_BASE || relay_newer {
                    self.put_image(&current, true)?;
                }
                let floor = if relay_newer { image.seq } else { LOCAL_BASE };
                self.drop_row(&image.table, row, &image.key, floor)?;
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
    fn keep_unlisted_own(&mut self, tx: u64, through: u64) -> Result<(), Error> {
        let client = self.client.as_ref().expect("client");
        let Some(pending) = client.pending.iter().find(|p| p.tx == tx) else {
            return Ok(());
        };
        let rows = pending.rows.clone();
        for (table, row, key) in rows {
            let unlisted = self
                .conn
                .cached_row(
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
            if let Some(mut image) = self.current_image(&table, row, &key)? {
                image.seq = through;
                let client = self.client.as_mut().expect("client");
                client.shadow.insert((table, row, key), Some(image));
            }
        }
        Ok(())
    }

    /// The rows of own commit `tx` the server did not ship (`held`): its
    /// version at `through` is the one this replica wrote, so that becomes
    /// the settled image. A newer image already received stays.
    fn install_held(&mut self, tx: u64, through: u64, held: &[Held]) -> Result<(), Error> {
        if held.is_empty() {
            return Ok(());
        }
        let client = self.client.as_mut().expect("client");
        let Some(pending) = client.pending.iter_mut().find(|p| p.tx == tx) else {
            return Ok(());
        };
        let expected = std::mem::take(&mut pending.expected);
        for held in held {
            let Some(mut image) = expected.get(held.index as usize).cloned() else {
                return Err(unsupported(format!(
                    "fate of commit {tx} holds row {} it did not expect",
                    held.index
                )));
            };
            held.stamp(&mut image);
            image.seq = through;
            self.receive_image(image)?;
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
    CREATE TABLE IF NOT EXISTS _jazz_revoked (t TEXT NOT NULL, id BLOB NOT NULL, k BLOB NOT NULL, PRIMARY KEY (t, id, k)) WITHOUT ROWID;";

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
            .cached_row("SELECT v FROM _jazz_meta WHERE k = ?1", [key], |r| {
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
        match self.conn.cached_row(
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

    /// The sessions' commits this store's own commit `seq` carries, as it
    /// accepted them through a relay.
    fn relayed_origins(&self, seq: u64) -> Result<Vec<([u8; 16], u64)>, Error> {
        let me = self.identity.node.as_bytes().to_vec();
        let mut statement = self
            .conn
            .prepare_cached("SELECT node, tx FROM _jazz_settled WHERE seq = ?1 AND node != ?2")
            .map_err(storage)?;
        let rows = statement
            .query_map(rusqlite::params![seq as i64, me], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(storage)?;
        let mut origins = Vec::new();
        for row in rows {
            let (node, tx) = row.map_err(storage)?;
            let node: [u8; 16] = node.try_into().map_err(|_| {
                Error::new(
                    ErrorCode::Storage,
                    "a settled commit's node is not 16 bytes",
                )
            })?;
            origins.push((node, tx as u64));
        }
        Ok(origins)
    }

    fn settled(&self, node: [u8; 16], tx: u64) -> Result<Option<Option<u64>>, Error> {
        match self.conn.cached_row(
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

    /// Whether this store answers to an upstream it relays for: a replica,
    /// unless its host says it has no server (then it is the last word on
    /// what its sessions commit).
    fn relays_upstream(&self) -> bool {
        self.client.as_ref().is_some_and(|client| {
            let link = client.hint.get();
            !(link.reported && link.hint == RemoteLinkHint::NoServer)
        })
    }

    /// The seq this store tells its sessions it is complete through. A
    /// relay tells them the authority's seq it knows (its own local seqs
    /// number nothing the authority can check), so a session's snapshot
    /// is one the authority validates against as it is.
    fn served_through(&self) -> u64 {
        match &self.client {
            Some(client) if self.relays_upstream() => client.known,
            _ => self.seq,
        }
    }

    /// A session's serializable commit as this relay uploads it: the
    /// session's own commits it names, and its reads of their versions,
    /// become this store's commits that carry them (the authority places
    /// each where it settled). Its snapshot and every other read version
    /// are already the authority's seqs ([`Self::served_through`]).
    fn relayed_serializable(
        &self,
        origin: [u8; 16],
        snapshot: u64,
        reads: Vec<WireRead>,
        own: Vec<u64>,
    ) -> Result<Kind, Error> {
        let mut carried = BTreeMap::new();
        for tx in own {
            if let Some(Some(seq)) = self.settled(origin, tx)? {
                carried.insert(tx, seq);
            }
        }
        let reads = reads
            .into_iter()
            .map(|read| WireRead {
                rows: read
                    .rows
                    .into_iter()
                    .map(|(row, seq)| (row, carried.get(&seq).copied().unwrap_or(seq)))
                    .collect(),
                ..read
            })
            .collect();
        Ok(Kind::Serializable {
            snapshot,
            reads,
            own: carried.into_values().collect(),
        })
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
        // One with no server is the last word on its store's seqs.
        let horizon = match &self.client {
            Some(client) if !client.no_server() => client.known,
            _ => self.seq,
        };
        if snapshot.global_base.0 > horizon {
            return Err(snapshot_unavailable(
                "it was captured past the authority state this store has synced",
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
            let mut commit: SyncMessage = postcard::from_bytes(&commit)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            // The rows it expected are not kept across a reopen: the
            // server ships them.
            if let SyncMessage::Commit { expect, .. } = &mut commit {
                expect.clear();
            }
            let rows: Vec<(String, [u8; 16], Vec<u8>)> = postcard::from_bytes(&rows)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            let SyncMessage::Commit {
                ops,
                author,
                session,
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
            client.pending.push(Pending {
                tx,
                ops,
                rows: rows
                    .into_iter()
                    .map(|(t, r, k)| (t, RowUuid::from_bytes(r), k))
                    .collect(),
                expected: Vec::new(),
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
    /// Its host says there is no server: it is the last word on its store.
    fn no_server(&self) -> bool {
        let link = self.hint.get();
        link.reported && link.hint == RemoteLinkHint::NoServer
    }

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
            local_fates: HashSet::new(),
            stored: HashSet::new(),
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
            relayed_fates: HashMap::new(),
            relayed_fetches: Vec::new(),
            relay_ready: false,
            unadmitted: false,
            hold_owed: false,
        }
    }
}

// ---- client ---------------------------------------------------------------

impl Db {
    /// Connect this database to its server. Writes from now on upload, and
    /// subscriptions attach remotely.
    pub async fn connect_upstream(&self, transport: Box<dyn Transport>) -> Upstream {
        self.become_replica();
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
                    tier: r.reach.tier,
                    wait: r.reach.wait,
                }
            })
            .collect();
        // Commits, their chunk pushes and chunk requests a full queue held
        // back are sent again below from what is still owed.
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::Subscribe { .. }
                    | SyncMessage::Hold { .. }
                    | SyncMessage::Schema { .. }
                    | SyncMessage::Commit { .. }
                    | SyncMessage::PushChunks { .. }
                    | SyncMessage::ChunkRequests { .. }
            )
        });
        let version = *inner.owner_version.as_bytes();
        let client = inner.client.as_mut().expect("client");
        client.outbox.insert(0, SyncMessage::Schema { version });
        // Reading the held rows can fail, and connecting cannot: the send
        // that reads them reports the error, and the next one tries again.
        client.hold_owed = true;
        client.outbox.extend(resubscribe);
        // Relayed reads still waiting are asked again: the old session's
        // answers are lost with it.
        let reask: Vec<SyncMessage> = client
            .relayed_fetches
            .iter()
            .filter(|fetch| matches!(client.answers.get(&fetch.upstream), Some(None)))
            .map(|fetch| fetch.ask.clone())
            .collect();
        let relayed: HashSet<u64> = client.relayed_fetches.iter().map(|f| f.upstream).collect();
        client
            .outbox
            .retain(|m| !matches!(m, SyncMessage::Fetch { id, .. } if relayed.contains(id)));
        client.outbox.extend(reask);
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
        client.unadmitted = false;
        client.connection += 1;
        Upstream(client.connection)
    }

    /// Unlist the cached rows of a remote query's settled view that its
    /// answer, listing `listed`, left out. Each pass reads the view again,
    /// so rows that move up into a window are checked too.
    fn unlist_unanswered(
        &self,
        query: Query,
        binding: BTreeMap<String, Value>,
        include_deleted: bool,
        listed: &[[u8; 16]],
        through: u64,
    ) -> Result<(), Error> {
        // A history read lists versions and a flat join tuples, not the
        // rows a replica caches.
        if history_source_table(&query.table).is_some() || query.flat_join.is_some() {
            return Ok(());
        }
        let listed: HashSet<[u8; 16]> = listed.iter().copied().collect();
        // Only which rows the view holds matters: ids, not values (whose
        // large chunks may not be here yet), nor included rows.
        let mut query = remote_query(&query).into_owned();
        query.select = Some(Vec::new());
        query.includes.clear();
        query.array_subqueries.clear();
        let prepared = PreparedQuery {
            query,
            binding,
            shape: Shape([0; 16]),
            binding_id: Binding([0; 16]),
            request_claims: None,
            schema: self.view.clone(),
        };
        // The answer lists the result as of `through`: rows that arrived
        // after it (later in the same batch) are not its to judge.
        let read = ReadSpec {
            members_only: true,
            settled_only: true,
            include_deleted,
            cut: Some(crate::compile::Cut::at(through)),
            ..ReadSpec::default()
        };
        let inner = self.inner.borrow();
        let table = inner.schema.stored_table(&prepared.query.table)?.to_owned();
        let mut previous = Vec::new();
        loop {
            // Ordering by a value whose chunks are not here yet: the rows
            // are reconciled by a later answer.
            let rows = match inner.run(&prepared, None, &read) {
                Err(error) if crate::large::is_missing(&error) => return Ok(()),
                rows => rows?,
            };
            let stale: Vec<RowUuid> = rows
                .into_iter()
                .filter(|row| row.union_arm.is_none() && !listed.contains(row.id.0.as_bytes()))
                .map(|row| row.id)
                .collect();
            if stale.is_empty() || stale == previous {
                return Ok(());
            }
            for &row in &stale {
                let id = row.0.as_bytes().to_vec();
                for image in inner.images_where(&table, false, "id = ?1", &[&id])? {
                    if image.seq <= through {
                        inner.unlist(&table, row, &image.key)?;
                    }
                }
            }
            previous = stale;
        }
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
        inner.detach_upstream();
        true
    }

    /// Become a replica: optimistic seqs from here on sit above every
    /// server seq, and commits are owed to an upstream (uploaded once one
    /// connects). A store that will connect later becomes one before it
    /// writes, or its earlier commits stay local.
    pub fn become_replica(&self) {
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
        self.become_replica();
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
    /// Record a local commit's upload. It reads and writes the store, so
    /// it runs inside the commit's SQLite transaction: an error leaves the
    /// replica's sync state as it was, and the caller rolls the commit back.
    pub(crate) fn record_upload(
        &self,
        ops: Vec<StagedOp>,
        seq: u64,
        _mark: usize,
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
        // A read-only serializable commit still goes up: the authority
        // validates its reads.
        if (ops.is_empty() && !kind.validates_reads()) || seq != inner.seq {
            return Ok(());
        }
        // The writer the authority stamps: an explicit identity, or the
        // system at this node (the authority stamps its own writes as its
        // node, so a trusted link names its own).
        let session = ops.iter().any(StagedOp::is_session);
        let author = ops.iter().find_map(StagedOp::author).or_else(|| {
            (inner.identity.author == AuthorSubject::SYSTEM).then(|| inner.stamp_author())
        });
        // A session's writes carry its claims, for the authority's check.
        let session_claims = author
            .filter(|_| session)
            .map(|author| inner.claims.get(&author).cloned().unwrap_or_default());
        let rows = inner.written_rows(&ops, seq)?;
        let mut shadows = Vec::new();
        for (table, row, key) in &rows {
            let shadowed = inner
                .client
                .as_ref()
                .is_some_and(|c| c.shadow.contains_key(&(table.clone(), *row, key.clone())));
            if !shadowed {
                shadows.push((
                    (table.clone(), *row, key.clone()),
                    inner.settled_before(table, *row, key)?,
                ));
            }
        }
        let wire: Result<Vec<WireOp>, Error> = ops.iter().map(to_wire).collect();
        // Each written row as this replica now holds it, so the authority
        // can skip echoing a version that is exactly this.
        let mut expected = Vec::new();
        let mut expect = Vec::new();
        for (table, row, key) in &rows {
            let Some(image) = inner.current_image(table, *row, key)? else {
                continue;
            };
            let Some(named) = image_out(&inner.owner, image.clone()) else {
                continue;
            };
            expect.push(Expect {
                id: image.id,
                key: image.key.clone(),
                digest: echo_digest(&named),
            });
            expected.push(image);
        }
        let client = inner.client.as_mut().expect("client");
        let added: Vec<RowKey> = shadows.iter().map(|(key, _)| key.clone()).collect();
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
                    session: session_claims,
                    expect,
                };
                client.unsent.push(commit.clone());
                client.pending.push(Pending {
                    tx: seq,
                    ops,
                    rows: rows.clone(),
                    expected,
                });
                let persisted = inner
                    .persist_pending(seq, &commit, &rows)
                    .and_then(|()| inner.persist_shadow());
                if let Err(error) = persisted {
                    let client = inner.client.as_mut().expect("client");
                    client.unsent.pop();
                    client.pending.pop();
                    for key in &added {
                        client.shadow.remove(key);
                    }
                    return Err(error);
                }
            }
            Err(error) => {
                client.fates.insert(seq, Err(error));
                client.pending.push(Pending {
                    tx: seq,
                    ops: Vec::new(),
                    rows,
                    expected: Vec::new(),
                });
                drop(inner);
                let _ = self.settle_locally(seq);
            }
        }
        Ok(())
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
    /// hop includes, array subqueries, junction traversals, gathers), and
    /// an aggregate's input rows or a gather's whole closure
    /// ([`remote_query`]); the client evaluates locally. Other shapes that
    /// read further tables (uncorrelated traversals or ones through a
    /// referenced row, gathers seeded by a claim, multi-hop includes) would
    /// see a partial replica, so they are refused explicitly.
    pub(crate) fn remote_shape(&self, query: &Query) -> Result<(), Error> {
        let schema = self.inner().schema.clone();
        if let Some(plan) = crate::db::union_plan(query, &schema)? {
            return plan
                .arms
                .iter()
                .try_for_each(|(_, arm)| self.remote_shape(arm));
        }
        let query = &*remote_query(query);
        let other_tables = !query.joins.iter().all(join_followable)
            || !query.reachable.iter().all(|via| gather_shaped(query, via))
            || !query.inherits.is_empty()
            || query
                .includes
                .iter()
                .any(|include| include.path.contains('.'));
        if other_tables {
            return Err(crate::values::unsupported(format!(
                "a synced read of `{}` that reads further tables (uncorrelated joins, joins through a referenced row, claim-seeded gathers or multi-hop includes)",
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
        reach: RemoteReach,
    ) -> Option<(
        Rc<StdCell<bool>>,
        Rc<RefCell<Option<Error>>>,
        Rc<StdCell<bool>>,
    )> {
        let query = remote_query(&prepared.query);
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut()?;
        let key = remote_key(&query, &prepared.binding, include_deleted, reach);
        if let Some(remote) = client.remote.get_mut(&key) {
            remote.refs += 1;
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
        client.outbox.push(SyncMessage::Subscribe {
            id,
            query: query.clone().into_owned(),
            binding: prepared.binding.clone(),
            since: None,
            include_deleted,
            tier: reach.tier,
            wait: reach.wait,
        });
        client.remote.insert(
            key.clone(),
            Remote {
                id,
                refs: 1,
                query: query.into_owned(),
                binding: prepared.binding.clone(),
                include_deleted,
                reach,
                answered: answered.clone(),
                refused: refused.clone(),
                global: global.clone(),
                through: None,
                resumed: false,
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
        reach: RemoteReach,
    ) {
        self.inner
            .borrow_mut()
            .release_remote(query, binding, include_deleted, reach);
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
        reach: RemoteReach,
    ) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let key = remote_key(&remote_query(query), binding, include_deleted, reach);
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

impl Db {
    /// Ask the server for one read and wait for its answer, unless the
    /// link cannot answer or `deadline` passes. Returns the server seq the
    /// answer is complete through, if it answered, or why it refused.
    /// `strict` (a Remote read): the read waits for an answer while the
    /// link is down or there is no server, asking again on each new
    /// connection, since an answer the last one owed is lost with it.
    /// `coverage`: the read must be covered by the server. It fails once
    /// the check says the caller's budget is spent (`NotObserved`), or as
    /// soon as the server says it has not admitted this client's schema
    /// version.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fetch(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        reach: RemoteReach,
        deadline: Option<Instant>,
        coverage: Option<&dyn Fn() -> bool>,
        strict: bool,
        view: &ReadViewSourceSpec,
    ) -> Result<Option<u64>, Error> {
        let tier = reach.tier;
        // A non-durable foreground's relay answers a local read from its
        // store whatever its own path to the server: the read waits for it
        // while connected to it.
        let answerable = |inner: &Inner| {
            let client = inner.client.as_ref().expect("client");
            may_wait(client.hint.get())
                || (inner.non_durable && tier != ReadTier::Remote && client.transport.is_some())
        };
        {
            let inner = self.inner.borrow();
            if inner.client.is_none() || !strict && !answerable(&inner) {
                return Ok(None);
            }
        }
        let message = |id: u64| SyncMessage::Fetch {
            id,
            query: remote_query(&prepared.query).into_owned(),
            binding: prepared.binding.clone(),
            at,
            include_deleted,
            tier,
            wait: reach.wait,
            view: view.clone(),
        };
        // The request in flight, and the connection it went out on.
        let mut asked: Option<(u64, u64)> = None;
        let answer = poll_fn(|_| {
            let mut inner = self.inner.borrow_mut();
            let answerable = answerable(&inner);
            let client = inner.client.as_mut().expect("client");
            if let Some((id, _)) = asked
                && let Some(Some(_)) = client.answers.get(&id)
            {
                let answer = client.answers.remove(&id).flatten().expect("answered");
                return Poll::Ready(answer.map(Some));
            }
            let ask = if strict {
                client.transport.is_some()
                    && asked.is_none_or(|(_, connection)| connection != client.connection)
            } else {
                asked.is_none()
            };
            if ask {
                if let Some((id, _)) = asked {
                    client.answers.remove(&id);
                }
                let id = client.next_id;
                client.next_id += 1;
                client.answers.insert(id, None);
                client.outbox.push(message(id));
                asked = Some((id, client.connection));
            }
            let forget = |client: &mut Client| {
                if let Some((id, _)) = asked {
                    client.answers.remove(&id);
                }
            };
            if (!strict && !answerable) || deadline.is_some_and(|d| Instant::now() >= d) {
                forget(client);
                return Poll::Ready(Ok(None));
            }
            if let Some(expired) = coverage {
                if client.unadmitted {
                    forget(client);
                    return Poll::Ready(Err(Error::new(
                        ErrorCode::Schema,
                        "schema version is awaiting published catalogue admission; \
                         deploy it to the server",
                    )));
                }
                if expired() {
                    forget(client);
                    return Poll::Ready(Err(Error::new(
                        ErrorCode::NotObserved,
                        "Timed out waiting for query coverage",
                    )));
                }
            }
            Poll::Pending
        })
        .await?;
        // A current answer lists its result's rows: cached rows the
        // replica's settled view of it holds and the answer lacks lost their
        // listing (INV-SYNC-14), though no subscription saw them leave.
        let Some((through, listed)) = answer else {
            return Ok(None);
        };
        if at.is_none() {
            self.unlist_unanswered(
                prepared.query.clone(),
                prepared.binding.clone(),
                include_deleted,
                &listed,
                through,
            )?;
        }
        Ok(Some(through))
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

    /// Whether a commit reaching `tier` takes more than this store: `Global`
    /// takes the authority, and `Local` on a non-durable replica its relay.
    fn waits_upstream(&self, tier: DurabilityTier) -> bool {
        self.is_client()
            && match tier {
                DurabilityTier::None => false,
                DurabilityTier::Local => self.inner.borrow().non_durable,
                DurabilityTier::Global => true,
            }
    }

    /// Wait for a local transaction's fate at `tier`.
    pub async fn wait_for_transaction(&self, tx: u64, tier: DurabilityTier) -> Result<u64, Error> {
        if !self.waits_upstream(tier) {
            return Ok(tx);
        }
        poll_fn(|_| {
            let inner = self.inner.borrow();
            let client = inner.client.as_ref().expect("client");
            match client.fates.get(&tx) {
                // Ordered by a replica with no server: no authority makes
                // it `Global`.
                Some(Ok(()))
                    if tier == DurabilityTier::Global && client.local_fates.contains(&tx) =>
                {
                    Poll::Pending
                }
                Some(Ok(())) => Poll::Ready(Ok(tx)),
                Some(Err(error)) => Poll::Ready(Err(error.clone())),
                None if tier == DurabilityTier::Local && client.stored.contains(&tx) => {
                    Poll::Ready(Ok(tx))
                }
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
        if !self.waits_upstream(tier) {
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

    /// Whether chunks this replica requested may still arrive: it is
    /// connected to an upstream that may answer, with requests outstanding
    /// (sent, or recorded by a read for the next send). A non-durable
    /// foreground's relay answers from its store whatever its own path to
    /// the server. Chunks the upstream answered missing are not waited for
    /// again.
    pub fn awaiting_chunks(&self) -> bool {
        let inner = self.inner.borrow();
        inner.client.as_ref().is_some_and(|client| {
            let unavailable = |request: &ChunkRequest| client.unavailable.contains(request);
            client.transport.is_some()
                && (may_wait(client.hint.get()) || inner.non_durable)
                && (!client.requested.is_empty()
                    || client.wanted.iter().any(|request| !unavailable(request))
                    || inner.chunks.has_demand_beyond(unavailable))
        })
    }

    /// Whether a read recorded chunk requests that only the next send
    /// carries upstream: a host that ticks when asked should tick.
    pub fn owes_chunk_requests(&self) -> bool {
        let inner = self.inner.borrow();
        inner.client.as_ref().is_some_and(|client| {
            client.transport.is_some()
                && inner.chunks.has_demand_beyond(|request| {
                    client.requested.contains(request) || client.unavailable.contains(request)
                })
        })
    }

    /// Ask the upstream authority whether `action` would be allowed for
    /// this replica's session, or for `session` (an identity a trusted
    /// backend acts for, with its claims); wait with
    /// [`Db::permission_advice`]. Without a connected upstream the answer
    /// is `Unknown`: a replica's own policy evidence may be incomplete, so
    /// it never decides locally.
    pub fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
        session: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> u64 {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return 0;
        };
        let id = client.next_id;
        client.next_id += 1;
        if client.transport.is_some() && may_wait(client.hint.get()) {
            client.advice.insert(id, None);
            let (author, session) = match session {
                Some((author, claims)) => (Some(author.canonical().to_owned()), Some(claims)),
                None => (None, None),
            };
            client.outbox.push(SyncMessage::Advise {
                id,
                action,
                author,
                session,
            });
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
            // The candidate gets a fresh id, as the insert it previews will:
            // no other row can reference it yet, so the dry run decides what
            // the authority would.
            PermissionAdviceAction::Insert { table, cells } => StagedOp::Insert(
                table,
                cells,
                InsertOptions {
                    row_id: Some(RowUuid::from_bytes(*uuid::Uuid::new_v4().as_bytes())),
                    ..Default::default()
                },
            ),
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
            let mut failed = false;
            if let Some(transport) = client.transport.as_mut() {
                while let Some(message) = transport.try_recv() {
                    inbound.push(message);
                }
                failed = transport.take_failure().is_some();
            }
            // An upstream that sent what this client cannot decode is
            // dropped like a lost link; what arrived before it applies.
            if failed {
                inner.detach_upstream();
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
                        client.known = client.known.max(through);
                        let Some(key) = client.remote_ids.get(&id).cloned() else {
                            continue;
                        };
                        let remote = client.remote.get_mut(&key).expect("remote");
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
                                remote.include_deleted,
                                Some(ids),
                                resumed,
                                through,
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
                            *slot = Some(Ok((through, rows)));
                        }
                        client.relay_ready |=
                            client.relayed_fetches.iter().any(|f| f.upstream == id);
                    }
                    SyncMessage::SchemaAdmission { admitted } => {
                        inner.client.as_mut().expect("client").unadmitted = !admitted;
                    }
                    SyncMessage::Refused { id, code, message } => {
                        let client = inner.client.as_mut().expect("client");
                        let error = Error::new(code, message);
                        client.relay_ready |=
                            client.relayed_fetches.iter().any(|f| f.upstream == id);
                        if let Some(slot) = client.answers.get_mut(&id) {
                            *slot = Some(Err(error));
                        } else if let Some(key) = client.remote_ids.get(&id)
                            && let Some(remote) = client.remote.get(key)
                        {
                            *remote.refused.borrow_mut() = Some(error);
                        }
                    }
                    SyncMessage::Stored { tx } => {
                        let client = inner.client.as_mut().expect("client");
                        if client.pending.iter().any(|p| p.tx == tx) {
                            client.stored.insert(tx);
                        }
                    }
                    SyncMessage::Fate {
                        tx,
                        through,
                        result,
                        held,
                        global,
                    } => {
                        let accepted = result.is_ok();
                        if accepted {
                            inner.keep_unlisted_own(tx, through)?;
                            inner.install_held(tx, through, &held)?;
                        }
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        let serializable = client.sent.iter().any(|m| {
                            matches!(m, SyncMessage::Commit { tx: sent, kind: WireKind::Serializable { .. }, .. } if *sent == tx)
                        });
                        client.pending.retain(|p| p.tx != tx);
                        client.stored.remove(&tx);
                        client.sent.retain(
                            |m| !matches!(m, SyncMessage::Commit { tx: sent, .. } if *sent == tx),
                        );
                        let result = result.map_err(from_wire_error);
                        // A relayed session's refusal is that session's to
                        // report, not this store's. One whose session is gone
                        // is reported as the commit the session made.
                        let relayed = client.relayed_fates.contains_key(&tx);
                        if let Err(error) = &result {
                            let origins = inner.relayed_origins(tx)?;
                            for &(node, session_tx) in &origins {
                                let refused = Err(wire_error(error));
                                inner.record_fate(node, session_tx, &refused)?;
                                inner.record_settled(node, session_tx, None)?;
                            }
                            let me = *inner.identity.node.as_bytes();
                            if let (Some(rejections), false) =
                                (inner.upstream_rejections.as_mut(), relayed)
                            {
                                let rejection = |writer, seq| crate::db::UpstreamRejection {
                                    writer,
                                    seq,
                                    error: error.clone(),
                                    serializable,
                                };
                                if origins.is_empty() {
                                    rejections.push(rejection(me, tx));
                                }
                                rejections.extend(
                                    origins.into_iter().map(|(node, t)| rejection(node, t)),
                                );
                            }
                        }
                        let client = inner.client.as_mut().expect("client");
                        if accepted && !global {
                            client.local_fates.insert(tx);
                        }
                        client.fates.insert(tx, result);
                        let node = client.node;
                        if let Some(locators) = client.pushed.remove(&tx) {
                            crate::large::mark_pushed(&inner.conn, &locators)?;
                        }
                        inner.record_settled(node, tx, accepted.then_some(through))?;
                        inner.forget_pending(tx)?;
                        rebase = true;
                        // Sessions whose commits this upload carried learn
                        // their fate from the upstream's.
                        let client = inner.client.as_mut().expect("client");
                        if let Some(relayed) = client.relayed_fates.remove(&tx) {
                            client.relay_ready = true;
                            let fate = client.fates.get(&tx).cloned().unwrap_or(Ok(()));
                            for (index, session_tx, rows) in relayed {
                                let Some(session) =
                                    inner.sessions.get_mut(index).filter(|s| s.open)
                                else {
                                    continue;
                                };
                                // A refused commit's rows go back to the
                                // session as this store reverts them.
                                if fate.is_err() {
                                    session.touched.extend(rows);
                                }
                                session.outbox.push(SyncMessage::Fate {
                                    tx: session_tx,
                                    through,
                                    result: fate.clone().map_err(|error| wire_error(&error)),
                                    held: Vec::new(),
                                    global,
                                });
                            }
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
        // A settled answer lists its result's rows: cached rows the
        // replica's view of that result holds and the server's lacks lost
        // their listing (INV-SYNC-14), as rows a commit took away do.
        // Nothing to reconcile after a resume: the client declared what it
        // holds (`Hold`), so the server ships what is missing or stale.
        for (query, binding, include_deleted, listed, resumed, through) in answered_remotes {
            if let (Some(listed), false) = (listed, resumed) {
                self.unlist_unanswered(query, binding, include_deleted, &listed, through)?;
            }
        }
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
        if client.hold_owed {
            let hold = inner.hold()?;
            let client = inner.client.as_mut().expect("client");
            if let Some(hold) = hold {
                let at = client
                    .outbox
                    .iter()
                    .position(|m| !matches!(m, SyncMessage::Schema { .. }))
                    .unwrap_or(client.outbox.len());
                client.outbox.insert(at, hold);
            }
            client.hold_owed = false;
        }
        let client = inner.client.as_mut().expect("client");
        let mut out = std::mem::take(&mut client.outbox);
        // Commits staged since the last send go ahead of the one-shot reads
        // asked since, so a read sees the writer's own earlier writes.
        let first_fetch = out
            .iter()
            .position(|m| matches!(m, SyncMessage::Fetch { .. }))
            .unwrap_or(out.len());
        let fetches = out.split_off(first_fetch);
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
        out.extend(fetches);
        // A full outbound queue keeps the rest, in order, for the flush its
        // drain wakes.
        let mut out = out.into_iter();
        while let Some(message) = out.next() {
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
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return Ok(());
        };
        let shadow = std::mem::take(&mut client.shadow);
        let node = client.node;
        let pending: Vec<(u64, Vec<StagedOp>, Vec<RowKey>)> = client
            .pending
            .iter()
            .map(|p| (p.tx, p.ops.clone(), p.rows.clone()))
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
        for (tx, ops, rows) in pending {
            // Origins identifies accepted row versions, not the write's
            // receipt. Once every written coordinate has an accepted image
            // at or after this commit, replay would apply it twice. Keep
            // the pending upload and waiters until its actual Fate arrives.
            if let Some(Some(accepted)) = inner.settled(node, tx)?
                && !rows.is_empty()
                && rows.iter().all(|row| {
                    shadow
                        .get(row)
                        .and_then(Option::as_ref)
                        .is_some_and(|image| image.seq >= accepted && image.seq < LOCAL_BASE)
                })
            {
                continue;
            }
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
    /// Drop the upstream link: reads stop waiting, writes queue.
    fn detach_upstream(&mut self) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
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
    }

    /// Session `index` closed: what this relay owed it is dropped, so a
    /// later session in its slot is not handed it.
    fn forget_relayed(&mut self, index: usize) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let answers = &mut client.answers;
        client.relayed_fetches.retain(|fetch| {
            let keep = fetch.session != index;
            if !keep {
                answers.remove(&fetch.upstream);
            }
            keep
        });
        for sessions in client.relayed_fates.values_mut() {
            sessions.retain(|(session, _, _)| *session != index);
        }
        client
            .relayed_fates
            .retain(|_, sessions| !sessions.is_empty());
    }

    /// Session `index`'s relayed reads that are ready to serve from this
    /// store: the upstream answered, or a first-load wait ran out or can no
    /// longer be answered. A refusal goes to the session as its own.
    fn relayed_fetches_ready(&mut self, index: usize) -> (Vec<SyncMessage>, Vec<RelayedAnswer>) {
        let Some(client) = self.client.as_mut() else {
            return (Vec::new(), Vec::new());
        };
        let mut answers = Vec::new();
        let can_wait = may_wait(client.hint.get());
        let now = Instant::now();
        let mut ready = Vec::new();
        let mut refused = Vec::new();
        let mut kept = Vec::new();
        for fetch in std::mem::take(&mut client.relayed_fetches) {
            let answered = matches!(client.answers.get(&fetch.upstream), Some(Some(_)));
            let given_up =
                !fetch.strict && (!can_wait || fetch.until.is_some_and(|until| now >= until));
            if fetch.session != index || !answered && !given_up {
                kept.push(fetch);
                continue;
            }
            let answer = client.answers.remove(&fetch.upstream).flatten();
            let SyncMessage::Fetch { id, .. } = &fetch.message else {
                continue;
            };
            match answer {
                Some(Err(error)) => refused.push(SyncMessage::Refused {
                    id: *id,
                    code: error.code,
                    message: error.message,
                }),
                Some(Ok((through, listed))) => {
                    if let SyncMessage::Fetch {
                        query,
                        binding,
                        at: None,
                        include_deleted,
                        ..
                    } = &fetch.ask
                    {
                        answers.push((
                            query.clone(),
                            binding.clone(),
                            *include_deleted,
                            listed,
                            through,
                        ));
                    }
                    ready.push((*id, fetch.message));
                }
                None => ready.push((*id, fetch.message)),
            }
        }
        client.relayed_fetches = kept;
        let session = &mut self.sessions[index];
        session.outbox.extend(refused);
        let ready = ready
            .into_iter()
            .map(|(id, message)| {
                session.fetched_upstream.insert(id);
                message
            })
            .collect();
        (ready, answers)
    }

    /// The identity whose policies govern session `index`'s reads and
    /// writes. `None` for a trusted link (the system), and for a replica's
    /// own identity: its foregrounds (a browser worker's tabs) read and
    /// write its store as it does itself, and the authority checks what it
    /// uploads.
    pub(crate) fn session_policy(&self, index: usize) -> Option<AuthorSubject> {
        let identity = self.sessions[index].identity;
        let own = self.client.is_some() && identity == self.identity.author;
        (identity != AuthorSubject::SYSTEM && !own).then_some(identity)
    }

    /// Whether session `index` declared a schema version this store has
    /// not admitted.
    fn session_unadmitted(&self, index: usize) -> bool {
        self.sessions[index].version.is_some_and(|version| {
            version != self.owner_version && !self.admitted.iter().any(|(id, _)| *id == version)
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
        let jazz = if version == self.owner_version {
            &self.owner.jazz
        } else {
            &self.admitted.iter().find(|(id, _)| *id == version)?.1
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
                Some(old) if old.seq >= image.seq && old.seq < LOCAL_BASE => {
                    self.put_image(&image, true)?;
                    return Ok(false);
                }
                // A relay's older unsettled version (an as-of answer).
                Some(old) if relay_older(old.seq, image.seq) => {
                    if self.non_durable {
                        self.put_image(&image, true)?;
                    }
                    return Ok(false);
                }
                // The replaced server version stays as history. A relay's
                // unsettled version gives way to a settled one, as the
                // relay's store moved on from it.
                Some(old) => {
                    let relay_newer = old.seq >= LOCAL_BASE && old.seq < image.seq;
                    let older = std::mem::replace(old, image);
                    if older.seq < LOCAL_BASE || self.non_durable && relay_newer {
                        self.put_image(&older, true)?;
                    }
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
            for (query, binding, include_deleted) in session.close(&dropped) {
                inner.release_remote(&query, &binding, include_deleted, RemoteReach::REMOTE);
            }
            inner.forget_relayed(subscriber.0);
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
            let (inbound, failed): (Vec<SyncMessage>, bool) = {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                if !session.open {
                    continue;
                }
                let mut inbound = std::mem::take(&mut session.backlog);
                inbound.extend(std::iter::from_fn(|| session.transport.try_recv()));
                let failed = session.transport.take_failure().is_some();
                (inbound.into(), failed)
            };
            // A client that sent what this server cannot decode is
            // disconnected; nothing it sent is served.
            if failed {
                self.disconnect_subscriber(SubscriberId(index));
                continue;
            }
            let mut inbound: VecDeque<SyncMessage> = inbound.into();
            self.serve_due_first_loads(index)?;
            // Relayed reads the upstream answered (or can no longer answer)
            // are served from this store first, in the order they came.
            let (relayed, answers) = self.inner.borrow_mut().relayed_fetches_ready(index);
            // The upstream's answers list their results' rows: cached rows
            // they left out are not served as the reader's (INV-SYNC-14).
            for (query, binding, include_deleted, listed, through) in answers {
                self.unlist_unanswered(query, binding, include_deleted, &listed, through)?;
            }
            for message in relayed.into_iter().rev() {
                inbound.push_front(message);
            }
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
                    SyncMessage::Subscribe { id, .. } | SyncMessage::Fetch { id, .. } => Some(*id),
                    _ => None,
                };
                let fetch = matches!(message, SyncMessage::Fetch { .. }).then(|| message.clone());
                match (self.serve(index, message), read) {
                    (Ok(()), _) => {}
                    // A relay's read whose large values it is still fetching
                    // from its upstream is served again once they arrive,
                    // from its store: the upstream is not asked again.
                    (Err(error), Some(id))
                        if crate::large::is_missing(&error)
                            && fetch.is_some()
                            && self.awaiting_chunks() =>
                    {
                        self.inner.borrow_mut().sessions[index]
                            .fetched_upstream
                            .insert(id);
                        held.push_back(fetch.expect("a fetch"));
                    }
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
            let mut inner = self.inner.borrow_mut();
            if !held.is_empty() {
                inner.sessions[index].backlog = held;
            }
            // Tell the client when its declared version's admission changes.
            let unadmitted = inner.session_unadmitted(index);
            let session = &mut inner.sessions[index];
            if session.open && session.told_unadmitted != unadmitted {
                session.told_unadmitted = unadmitted;
                session.outbox.push(SyncMessage::SchemaAdmission {
                    admitted: !unadmitted,
                });
            }
        }
        self.notify();
        let mut inner = self.inner.borrow_mut();
        let through = inner.served_through();
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
            let policy = inner.session_policy(index);
            let names = inner
                .session_schema(index)
                .unwrap_or_else(|| inner.owner.clone());
            let mut touched = std::mem::take(&mut inner.sessions[index].touched);
            let probed = std::mem::take(&mut inner.sessions[index].probed);
            let revalidate = std::mem::take(&mut inner.sessions[index].revalidate);
            let committed = std::mem::take(&mut inner.sessions[index].committed);
            let mut echoes = std::mem::take(&mut inner.sessions[index].echoes);
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
                let answered = events
                    .iter()
                    .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. }));
                let global = events.iter().any(|event| {
                    matches!(event, SubscriptionEvent::Delta { settled: true, tier, .. } if *tier == DurabilityTier::Global)
                });
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
                if answered {
                    settled.push(SyncMessage::Settled {
                        id,
                        through,
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
                        if !revalidate && !probed.contains(&(table.clone(), row, key.clone())) =>
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
                let superseded = inner
                    .current_image(&table, row, &key)?
                    .is_some_and(|current| current.seq != seq);
                if superseded {
                    echoes.retain(|e| !(e.table == table && e.row == row && e.key == key));
                }
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
            let mut out = std::mem::take(&mut inner.sessions[index].stale);
            let session = &mut inner.sessions[index];
            session.watch = watch;
            session.held = held_rows;
            let mut seen = HashSet::new();
            images.retain(|image| {
                seen.insert((image.table.clone(), image.id, image.key.clone(), image.seq))
            });
            // The writer's own rows, still at the version its commit
            // produced and exactly as it holds them, settle by its Fate.
            let mut held: BTreeMap<u64, Vec<Held>> = BTreeMap::new();
            let fated: HashSet<u64> = session
                .outbox
                .iter()
                .filter_map(|message| match message {
                    SyncMessage::Fate {
                        tx, result: Ok(()), ..
                    } => Some(*tx),
                    _ => None,
                })
                .collect();
            echoes.retain(|echo| fated.contains(&echo.tx));
            if !echoes.is_empty() {
                images.retain(|image| {
                    let Some(at) = echoes.iter().position(|e| {
                        e.table == image.table
                            && *e.row.0.as_bytes() == image.id
                            && e.key == image.key
                            && e.seq == image.seq
                    }) else {
                        return true;
                    };
                    let echo = echoes.swap_remove(at);
                    let same = image_out(&names, image.clone())
                        .is_some_and(|named| echo_digest(&named) == echo.digest);
                    if same {
                        held.entry(echo.tx)
                            .or_default()
                            .push(Held::of(echo.index, image));
                    }
                    !same
                });
                for message in &mut session.outbox {
                    if let SyncMessage::Fate { tx, held: h, .. } = message
                        && let Some(mut rows) = held.remove(tx)
                    {
                        rows.sort_unstable_by_key(|held| held.index);
                        *h = rows;
                    }
                }
            }
            // A row can arrive in an earlier tick than its Fate. Supply
            // its origin so the writer can avoid replaying it twice. Do
            // this after echo suppression: held rows need only their Fate.
            history_seqs.extend(images.iter().map(|image| image.seq));
            if !history_seqs.is_empty() {
                out.push(SyncMessage::Origins {
                    commits: inner.origins(&history_seqs)?,
                });
            }
            let session = &mut inner.sessions[index];
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
                    let released = session.close(&dropped);
                    for (query, binding, include_deleted) in released {
                        inner.release_remote(
                            &query,
                            &binding,
                            include_deleted,
                            RemoteReach::REMOTE,
                        );
                    }
                    inner.forget_relayed(index);
                    break;
                }
            }
        }
        Ok(())
    }

    /// A handle that reads and writes through session `index`'s schema.
    /// Session `index`'s relayed LocalFirst subscriptions whose first-load
    /// wait ran out before the upstream answered: from now on each is this
    /// store's result, settled at once, as when the upstream cannot answer.
    /// The upstream subscription it keeps fills the store when it answers.
    fn serve_due_first_loads(&self, index: usize) -> Result<(), Error> {
        let now = Instant::now();
        let due: Vec<(u64, FirstLoadWait)> = {
            let mut inner = self.inner.borrow_mut();
            let session = &mut inner.sessions[index];
            let ids: Vec<u64> = session
                .first_load
                .iter()
                .filter(|(_, wait)| wait.until <= now)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| session.first_load.remove(&id).map(|wait| (id, wait)))
                .collect()
        };
        for (id, wait) in due {
            let policy = {
                let inner = self.inner.borrow();
                // Answered and sent meanwhile: it already lists the
                // upstream's result.
                if !matches!(inner.sessions[index].subs.get(&id), Some((.., false))) {
                    continue;
                }
                inner.session_policy(index)
            };
            let db = self.session_db(index);
            let mut inner = db.inner();
            let authority = inner.authority;
            let (queue, group) = inner.subscribe_member(
                &wait.prepared,
                policy,
                ReadSpec {
                    include_deleted: wait.include_deleted,
                    ..ReadSpec::default()
                },
                SettledBy::Always(authority),
            )?;
            let session = &mut inner.sessions[index];
            if let Some((_, _, windowed, _)) = session.subs.remove(&id) {
                session.subs.insert(id, (queue, group, windowed, false));
                let dropped = inner.fan.dropped.clone();
                dropped.set(dropped.get() + 1);
            }
        }
        Ok(())
    }

    fn session_db(&self, index: usize) -> Db {
        let view = self.inner.borrow_mut().session_schema(index);
        Db {
            inner: self.inner.clone(),
            view,
        }
    }

    fn serve(&self, index: usize, message: SyncMessage) -> Result<(), Error> {
        let (identity, claims) = {
            let inner = self.inner.borrow();
            let session = &inner.sessions[index];
            (session.identity, session.claims.clone())
        };
        let Some(claims) = claims.filter(|_| identity != AuthorSubject::SYSTEM) else {
            return self.serve_as(index, message, None);
        };
        // The session's claims stand for its identity's while it is served.
        let previous = self
            .inner
            .borrow_mut()
            .claims
            .insert(identity, claims.clone());
        let result = self.serve_as(index, message, Some(claims));
        let mut inner = self.inner.borrow_mut();
        match previous {
            Some(previous) => inner.claims.insert(identity, previous),
            None => inner.claims.remove(&identity),
        };
        result
    }

    /// Run `f` with a vouched session's claims standing for its identity's.
    fn with_claims<R>(
        &self,
        vouched: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        f: impl FnOnce() -> R,
    ) -> R {
        let Some((author, claims)) = vouched else {
            return f();
        };
        let previous = self.inner.borrow_mut().claims.insert(author, claims);
        let result = f();
        let mut inner = self.inner.borrow_mut();
        match previous {
            Some(previous) => inner.claims.insert(author, previous),
            None => inner.claims.remove(&author),
        };
        result
    }

    fn serve_as(
        &self,
        index: usize,
        message: SyncMessage,
        claims: Option<BTreeMap<String, Value>>,
    ) -> Result<(), Error> {
        let identity = self.inner.borrow().sessions[index].identity;
        // A subscription's group keeps the session's claims for later runs.
        let with_claims = |prepared: PreparedQuery| match &claims {
            Some(claims) => prepared.with_identity_claims(identity, claims.clone()),
            None => prepared,
        };
        let policy = self.inner.borrow().session_policy(index);
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
                tier,
                wait,
            } => {
                let local = tier == ReadTier::LocalOnly;
                let prepared = with_claims(db.prepare_query_bound(&query, binding)?);
                let windowed = crate::db::is_windowed(&query);
                // A relay serves what its upstream lists, settled once the
                // upstream answers: its own store may be cold. A Remote
                // read waits for that answer however long it takes, its
                // link down or no server at all: only the authority
                // confirms a remote result. A LocalFirst one whose upstream
                // cannot answer yet (its link is down, or still to come)
                // serves this store, settled at once, while the upstream
                // subscription it keeps fills the store once the link is
                // up; one with a first-load wait is served so once the wait
                // runs out.
                let upstream = db
                    .inner()
                    .client
                    .as_ref()
                    .map(|client| may_wait(client.hint.get()));
                let attached = if upstream.is_some() && !local {
                    db.attach_remote(&prepared, include_deleted, RemoteReach::REMOTE)
                } else {
                    None
                };
                let strict = tier == ReadTier::Remote && wait.is_none();
                let relayed = attached
                    .as_ref()
                    .filter(|_| upstream == Some(true) || strict);
                if relayed.is_some()
                    && let Some(until) = wait.and_then(wait_until)
                {
                    self.inner.borrow_mut().sessions[index].first_load.insert(
                        id,
                        FirstLoadWait {
                            until,
                            prepared: prepared.clone(),
                            include_deleted,
                        },
                    );
                }
                let mut inner = db.inner();
                let support = Rc::new(inner.support_of(&query)?);
                let authority = inner.authority;
                let subscribed = inner.subscribe_member(
                    &prepared,
                    policy,
                    ReadSpec {
                        include_deleted,
                        members_only: relayed.is_some(),
                        // A Remote one leaves out this replica's pending
                        // versions: only the authority confirms them.
                        settled_only: relayed.is_some() && tier == ReadTier::Remote,
                        ..ReadSpec::default()
                    },
                    match relayed {
                        Some((answered, _, global)) => {
                            SettledBy::Remote(answered.clone(), global.clone())
                        }
                        None => SettledBy::Always(authority),
                    },
                );
                if attached.is_some() {
                    match &subscribed {
                        Ok(_) => {
                            let read = (
                                prepared.query.clone(),
                                prepared.binding.clone(),
                                include_deleted,
                            );
                            inner.sessions[index].relayed.insert(id, read);
                        }
                        Err(_) => inner.release_remote(
                            &prepared.query,
                            &prepared.binding,
                            include_deleted,
                            RemoteReach::REMOTE,
                        ),
                    }
                }
                let (queue, group) = subscribed?;
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
                inner.sessions[index].first_load.remove(&id);
                if let Some((query, binding, include_deleted)) =
                    inner.sessions[index].relayed.remove(&id)
                {
                    inner.release_remote(&query, &binding, include_deleted, RemoteReach::REMOTE);
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
            SyncMessage::Advise {
                id,
                action,
                author,
                session,
            } => {
                // As for a commit: a trusted link may ask for a session it
                // vouches for; a session link only for its own.
                let advice = match commit_who(policy, author.as_deref(), session.is_some()) {
                    Ok(who) => {
                        let identity = match who {
                            Who::Session(author) => Some(author),
                            _ => policy,
                        };
                        let vouched = vouched(policy, who, session);
                        self.with_claims(vouched, || self.advise(identity, action))
                    }
                    Err(_) => PermissionAdvice::Denied,
                };
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
                tier,
                wait,
                view,
            } => {
                // A relay asks its upstream first for a Remote read, then
                // serves it from its store once the answer's rows are in
                // it; the read waits for that answer while the link is down
                // or there is no server, since only the authority confirms
                // a remote result. A read with a wait is asked the same way
                // while the upstream may answer, and served once it does or
                // the wait runs out. Any other LocalFirst read is served
                // now, and its upstream's answer refreshes the store for
                // the next. A read as of a snapshot reference is this
                // store's: the reference names commits it settled.
                let snapshot = frozen_at(&view).cloned();
                let (relay, refresh) = if snapshot.is_some() {
                    (None, false)
                } else {
                    let mut inner = self.inner.borrow_mut();
                    let answered = inner.sessions[index].fetched_upstream.remove(&id);
                    let client = inner.client.as_ref();
                    let upstream = client.is_some_and(|client| may_wait(client.hint.get()));
                    let waits = upstream && tier != ReadTier::LocalOnly && wait.is_some();
                    let strict = client.is_some() && tier == ReadTier::Remote && wait.is_none();
                    (
                        (!answered && (strict || waits)).then_some(strict),
                        upstream && !answered && tier == ReadTier::LocalFirst && !waits,
                    )
                };
                if refresh {
                    let prepared = db.prepare_query_bound(&query, binding.clone())?;
                    let mut inner = self.inner.borrow_mut();
                    let client = inner.client.as_mut().expect("client");
                    let upstream = client.next_id;
                    client.next_id += 1;
                    client.outbox.push(SyncMessage::Fetch {
                        id: upstream,
                        query: remote_query(&prepared.query).into_owned(),
                        binding: prepared.binding.clone(),
                        at,
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        view: view.clone(),
                    });
                }
                if let Some(strict) = relay {
                    let prepared = db.prepare_query_bound(&query, binding.clone())?;
                    let mut inner = self.inner.borrow_mut();
                    let client = inner.client.as_mut().expect("client");
                    let upstream = client.next_id;
                    client.next_id += 1;
                    client.answers.insert(upstream, None);
                    let ask = SyncMessage::Fetch {
                        id: upstream,
                        query: remote_query(&prepared.query).into_owned(),
                        binding: prepared.binding.clone(),
                        at,
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        view: view.clone(),
                    };
                    client.outbox.push(ask.clone());
                    client.relayed_fetches.push(RelayedFetch {
                        upstream,
                        session: index,
                        ask,
                        strict,
                        until: wait.and_then(wait_until),
                        message: SyncMessage::Fetch {
                            id,
                            query,
                            binding,
                            at,
                            include_deleted,
                            tier,
                            wait,
                            view,
                        },
                    });
                    return Ok(());
                }
                let prepared = with_claims(db.prepare_query_bound(&query, binding)?);
                let mut inner = db.inner();
                // A history read ships versions of its source table.
                let history = history_source_table(&query.table);
                let table = inner
                    .schema
                    .stored_table(history.unwrap_or(&query.table))?
                    .to_owned();
                // A strict Remote read leaves out this replica's pending
                // versions, and the cached rows its upstream no longer lists
                // for it: only the authority confirms them. One with a wait
                // (a serializable snapshot) takes this replica's view.
                let strict = tier == ReadTier::Remote && wait.is_none() && inner.client.is_some();
                // The commits a snapshot reference names, as this store
                // settled them: the reader resolves the reference with them.
                let mut named = Vec::new();
                for dot in snapshot.iter().flat_map(|snapshot| &snapshot.dots) {
                    let (node, tx) = (*dot.node.as_bytes(), dot.time.0);
                    if let Some(Some(seq)) = inner.settled(node, tx)? {
                        named.push((seq, node, tx));
                    }
                }
                let opts = ReadOpts {
                    read_view: crate::protocol::ReadViewSpec {
                        source: view,
                        include_deleted,
                    },
                    ..ReadOpts::default()
                };
                let viewed = match ReadSpec::of(&opts, &inner) {
                    Ok(viewed) => viewed,
                    Err(error) => {
                        inner.sessions[index].outbox.push(SyncMessage::Refused {
                            id,
                            code: error.code,
                            message: error.message,
                        });
                        return Ok(());
                    }
                };
                let read = ReadSpec {
                    cut: viewed.cut.or(at.map(Cut::at)),
                    include_deleted,
                    settled_only: strict,
                    members_only: strict,
                    view: viewed.view,
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
                // Fetches can also deliver an accepted version before the
                // writer's receipt, including a current tombstone shipped
                // alongside its earlier history.
                seqs.extend(images.iter().map(|image| image.seq));
                let mut origins = inner.origins(&seqs)?;
                origins.extend(named);
                let through = at.unwrap_or(inner.served_through());
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
                session,
                expect,
            } => {
                // A resend after a reconnect gets the recorded fate.
                let known = self.inner.borrow().applied_fate(node, tx)?;
                let (result, rows) = match known {
                    Some(result) => (result, Vec::new()),
                    None => match commit_who(policy, author.as_deref(), session.is_some())
                        .map_err(|error| (error, Vec::new()))
                        .and_then(|who| {
                            let vouched = vouched(policy, who, session);
                            self.with_claims(vouched, || {
                                db.authorize_commit(who, policy, kind, ops, (node, tx))
                            })
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
                    _ => inner.served_through(),
                };
                let session = &mut inner.sessions[index];
                if result.is_ok() {
                    session.committed.extend(
                        rows.iter()
                            .map(|(table, row, key)| (table.clone(), *row, key.clone(), through)),
                    );
                }
                session.touched.extend(rows.iter().cloned());
                // A relay's accepted commit is final once its upstream
                // accepts the upload carrying it: the session waits for that
                // fate, not this store's. A relay its host says has no
                // server is the last word on it.
                let upload = result.is_ok()
                    && inner.client.as_ref().is_some_and(|client| {
                        let link = client.hint.get();
                        !(link.reported && link.hint == RemoteLinkHint::NoServer)
                            && client.pending.iter().any(|p| p.tx == through)
                    });
                if upload {
                    let client = inner.client.as_mut().expect("client");
                    client
                        .relayed_fates
                        .entry(through)
                        .or_default()
                        .push((index, tx, rows));
                    inner.sessions[index]
                        .outbox
                        .push(SyncMessage::Stored { tx });
                } else {
                    // The rows the writer holds as they now are need no
                    // echo: fan-out drops their images if still current.
                    if result.is_ok() {
                        let echoes = expect.into_iter().enumerate().filter_map(|(i, expect)| {
                            let (table, row, key) = rows.iter().find(|(_, row, key)| {
                                *row.0.as_bytes() == expect.id && *key == expect.key
                            })?;
                            Some(Echo {
                                tx,
                                index: i as u32,
                                table: table.clone(),
                                row: *row,
                                key: key.clone(),
                                seq: through,
                                digest: expect.digest,
                            })
                        });
                        inner.sessions[index].echoes.extend(echoes);
                    }
                    // A replica's own last word (it has no server) orders
                    // the commit at a local seq, but is not the authority's.
                    let global = inner.client.is_none() || through < LOCAL_BASE;
                    inner.sessions[index].outbox.push(SyncMessage::Fate {
                        tx,
                        through,
                        result,
                        held: Vec::new(),
                        global,
                    });
                }
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
                // A relay pushes what its sessions push on to its upstream,
                // ahead of the commits that name it.
                let relay = inner.client.is_some();
                for node in nodes {
                    // A node that fails authentication is dropped; a commit
                    // that needs it is then rejected for missing chunks.
                    let request = node.request();
                    let received = crate::large::receive_pushed(
                        &inner.conn,
                        &inner.chunks,
                        &request,
                        node.bytes,
                        expires,
                    );
                    if relay && received.is_ok() {
                        crate::large::mark_unpushed(&inner.conn, &request.locator)?;
                    }
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
        mut kind: WireKind,
        ops: Vec<WireOp>,
        origin: ([u8; 16], u64),
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, (Error, Vec<(String, RowUuid, Vec<u8>)>)> {
        let ops: Vec<StagedOp> = match ops.into_iter().map(from_wire).collect::<Result<_, _>>() {
            Ok(ops) => ops,
            Err(error) => return Err((error, Vec::new())),
        };
        // A relay uploads what its sessions commit as its own. A
        // serializable one goes up as serializable, and its upstream
        // validates it; this store applies it optimistically meanwhile, as
        // any replica applies its own.
        let relay = self.is_client();
        let forward =
            matches!(kind, WireKind::Serializable { .. }) && self.inner.borrow().relays_upstream();
        let upload = match std::mem::replace(&mut kind, WireKind::Mergeable) {
            WireKind::Serializable {
                snapshot,
                reads,
                own,
            } if forward => self
                .inner
                .borrow()
                .relayed_serializable(origin.0, snapshot, reads, own)
                .map_err(|e| (e, Vec::new()))?,
            unchanged => {
                kind = unchanged;
                Kind::Mergeable
            }
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
                    return Err((Error::serialization_conflict(), rows));
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
                let spec = ReadSpec {
                    include_deleted: read.include_deleted,
                    ..ReadSpec::default()
                };
                let now: Vec<([u8; 16], u64)> = inner
                    .read_versions(&prepared, identity, &spec)
                    .map_err(|e| (e, Vec::new()))?
                    .iter()
                    .map(|(row, seq)| (*row.0.as_bytes(), *seq))
                    .collect();
                // A row the sender read at an own commit's local seq is
                // compared where that commit settled.
                let then: Vec<([u8; 16], u64)> = read
                    .rows
                    .iter()
                    .map(|(row, seq)| (*row, settled.get(seq).copied().unwrap_or(*seq)))
                    .collect();
                if now != then {
                    let rows = mentioned(&inner);
                    return Err((Error::serialization_conflict(), rows));
                }
            }
        }
        let (seq, mark) = {
            let mut inner = self.inner();
            let seq = inner.seq + 1;
            inner
                .conn
                .execute_batch("SAVEPOINT upload")
                .map_err(|e| (storage(e), Vec::new()))?;
            let mark = inner.changed.borrow().len();
            // The unit's write checks run at its end, grounded (SPEC 7).
            inner.defer_checks();
            let applied = ops
                .iter()
                .try_for_each(|op| inner.apply_as(op, who, seq).map(drop))
                .and_then(|()| inner.finish_checks(seq));
            if let Err(error) = applied {
                inner.abandon_checks();
                inner
                    .conn
                    .execute_batch("ROLLBACK TO upload; RELEASE upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.changed.borrow_mut().truncate(mark);
                let rows = mentioned(&inner);
                return Err((error, rows));
            }
            let recorded = inner
                .record_fate(origin.0, origin.1, &Ok(()))
                .and_then(|()| inner.record_settled(origin.0, origin.1, Some(seq)));
            if let Err(error) = recorded {
                inner
                    .conn
                    .execute_batch("ROLLBACK TO upload; RELEASE upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.changed.borrow_mut().truncate(mark);
                return Err((error, Vec::new()));
            }
            inner.seq = seq;
            (seq, mark)
        };
        // The relay's own upload record and the rows the sender hears about
        // are read before the commit is released, so a storage error
        // rejects it whole instead of leaving it applied but not relayed.
        let finished = if relay {
            self.record_upload(ops.clone(), seq, 0, upload)
        } else {
            Ok(())
        }
        .and_then(|()| self.inner.borrow().written_rows(&ops, seq));
        let mut inner = self.inner();
        match finished {
            Ok(rows) => {
                inner
                    .conn
                    .execute_batch("RELEASE upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                drop(inner);
                self.notify();
                Ok(rows)
            }
            Err(error) => {
                inner
                    .conn
                    .execute_batch("ROLLBACK TO upload; RELEASE upload")
                    .map_err(|e| (storage(e), Vec::new()))?;
                inner.seq = seq - 1;
                inner.changed.borrow_mut().truncate(mark);
                Err((error, Vec::new()))
            }
        }
    }

    /// One tick for both roles.
    pub(crate) fn sync_tick(&self) -> Result<(), Error> {
        self.server_tick()?;
        self.client_tick()?;
        // Relayed fates and reads the upstream just answered go out now.
        let ready = self
            .inner
            .borrow_mut()
            .client
            .as_mut()
            .is_some_and(|client| std::mem::take(&mut client.relay_ready));
        if ready {
            self.server_tick()?;
            // Chunks a relayed read found missing go upstream now.
            if self.owes_chunk_requests() {
                self.client_send()?;
            }
        }
        Ok(())
    }
}

/// Who an uploaded commit is applied as. A session link writes under its
/// own identity (a session it names is checked as the link) and may not
/// attribute to another author; a trusted link (no policy identity) writes
/// as the session it names (impersonation: checked as it), or unchecked,
/// attributed to the author it names or as itself.
/// The session and claims a trusted link (no `policy` of its own) vouches
/// for: its session writes and preflights are checked with them.
fn vouched(
    policy: Option<AuthorSubject>,
    who: Who,
    session: Option<BTreeMap<String, Value>>,
) -> Option<(AuthorSubject, BTreeMap<String, Value>)> {
    match (who, session) {
        (Who::Session(author), Some(claims)) if policy.is_none() => Some((author, claims)),
        _ => None,
    }
}

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
        // A session link cannot vouch for the sessions it writes as: their
        // writes are checked as the link's own.
        (Some(link), Some(_)) if session => Ok(Who::Session(link)),
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

/// Whether a client message may be served ahead of the writes the ingress
/// limit holds back: it reads or declares interest, and writes no rows.
fn overtakes_writes(message: &SyncMessage) -> bool {
    matches!(
        message,
        SyncMessage::Subscribe { .. }
            | SyncMessage::Unsubscribe { .. }
            | SyncMessage::Fetch { .. }
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
        require_coverage: bool,
        coverage_expired: E,
        _release_coverage: F,
    ) -> Result<SerializedReadResult, Error>
    where
        F: Fn(QueryAttachment),
        E: Fn() -> bool,
    {
        let query: Query = postcard::from_bytes(query)
            .map_err(|e| Error::new(ErrorCode::Query, format!("undecodable query: {e}")))?;
        let mut prepared = self.prepare_query(&query)?;
        // An explicit identity reads what its policies let it see.
        let identity = request_scope.as_ref().map(|(author, _)| *author);
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
                .all_prepared_at(
                    &prepared,
                    opts.read_view.include_deleted,
                    identity,
                    opts.tier,
                )
                .await?;
            return Ok(result(rows));
        }
        let frozen = frozen_at(&opts.read_view.source).is_some();
        // A non-durable foreground's store starts empty: its local view is
        // its relay's, which it asks for at every tier, a frozen one too
        // (a reopened tab's capture names commits only its relay settled).
        let non_durable = self.inner().non_durable;
        if self.is_client()
            && (opts.tier != ReadTier::LocalOnly || non_durable)
            && (!frozen || non_durable)
        {
            let wait = match (opts.tier, &opts.first_load) {
                (ReadTier::Remote, _) => Some(None),
                _ if non_durable => Some(None),
                (_, FirstLoad::WaitForRemote { timeout_ms }) if *timeout_ms > 0 => Some(Some(
                    Instant::now() + std::time::Duration::from_millis(*timeout_ms),
                )),
                _ => None,
            };
            if let Some(deadline) = wait {
                self.remote_shape(&prepared.query)?;
                // A read that must be covered by the server fails once
                // the caller's budget is spent or the server cannot serve
                // its schema version; others fall back to local.
                let coverage: Option<&dyn Fn() -> bool> =
                    require_coverage.then_some(&coverage_expired);
                // A non-durable foreground reads its relay's view at the
                // tier it was asked for, with its first load's wait.
                let reach = if non_durable {
                    RemoteReach {
                        tier: opts.tier,
                        wait: crate::db::first_load_wait(opts.tier, &opts.first_load),
                    }
                } else {
                    RemoteReach::REMOTE
                };
                // A Remote read waits for the server while offline.
                let strict = opts.tier == ReadTier::Remote;
                self.fetch(
                    &prepared,
                    None,
                    opts.read_view.include_deleted,
                    reach,
                    deadline,
                    coverage,
                    strict,
                    &opts.read_view.source,
                )
                .await?;
            }
        }
        let opts = ReadOpts {
            first_load: FirstLoad::Deliver,
            ..opts
        };
        // A row whose large values are still being fetched answers once
        // their chunks arrive, while the upstream that will send them is
        // connected (a reloaded tab's rows come from its relay first).
        poll_fn(|_| {
            let read = {
                let inner = self.inner();
                ReadSpec::of(&opts, &inner).and_then(|spec| {
                    let roots = inner.run(&prepared, identity, &spec)?;
                    inner.nest_relations(&prepared, identity, &spec, roots)
                })
            };
            match read {
                Err(error) if crate::large::is_missing(&error) && self.awaiting_chunks() => {
                    Poll::Pending
                }
                read => Poll::Ready(read.map(result)),
            }
        })
        .await
    }
}

impl Db {
    /// Mark this store as a non-durable foreground over a durable relay:
    /// its own store starts empty each time, so a subscription's opening,
    /// whatever its tier, waits for the upstream's answer (the relay's
    /// stored view, or its upstream's once that answers).
    pub fn set_non_durable_client(&self) {
        self.inner.borrow_mut().non_durable = true;
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
                .map(|(tx, r)| (tx - LOCAL_BASE, r.is_ok()))
                .collect::<BTreeMap<_, _>>(),
            client.transport.is_some(),
        )
    }
}
