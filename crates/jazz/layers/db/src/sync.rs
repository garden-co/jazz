//! Client↔server sync over a message transport.
//!
//! Model: the server (the authority) is a `Db` holding complete history; a
//! client is a `Db` whose store is a partial replica of server versions
//! plus its own optimistic writes.
//!
//! * Replica rows carry the server's commit seq in `_seq`, so history and
//!   as-of cuts on the client are over server versions. Optimistic local
//!   writes take seqs from `LOCAL_BASE` up, above any server seq, and are
//!   therefore invisible to every cut at a server seq. A non-durable client
//!   (a worker's tab) numbers its own from `NON_DURABLE_BASE` up: its
//!   replica also holds its worker's unsettled versions at the worker's
//!   local seqs, and the two numberings must never meet.
//! * A row touched by a pending (unsettled) local transaction keeps its
//!   upstream baseline in history, below this replica's local sequence range.
//!   A transient cache selects that image during a tick; a rebase restores it
//!   and replays pending transactions in order. Settling or rejecting one
//!   is a rebase without it. Linear history: the authority orders commits by arrival.
//! * The server keeps one fan-out subscription per client query (under the
//!   client's identity, so policies apply there) and sends row images for
//!   rows entering or changing in it. A row leaving it is sent again if the
//!   client may still read it, else evicted: the replica drops its copy
//!   (INV-SYNC-14), whether a write or a read-policy change took the
//!   access. Answers carry the server seq they are complete through, and
//!   openings and windows carry their row ids so the client can drop stale
//!   cached rows.
//! * Coherence is per subscription: the server keeps a row current for a
//!   session only while one of its live subscriptions covers it. A cached
//!   row no live subscription covers is a cold copy, current again once a
//!   query covers it. A subscription resumes from the seq its result was
//!   last settled through (`since`, persisted per subscription), and its
//!   opening ships only what changed since.
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

#[path = "large_delta.rs"]
mod large_delta;
pub use crate::options::{FirstLoad, RemoteLinkHint};

#[path = "sync_conflicts.rs"]
mod conflicts;
pub use crate::wire::TransportError;
pub(crate) use conflicts::Built;
pub use conflicts::WriteConflict;
pub use large_delta::{CellDelta, WireDelta};

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

/// A non-durable client's optimistic local seqs start here, so they never
/// collide with its upstream relay's (from `LOCAL_BASE` up, which a relay
/// ships to it for versions it has not settled). A relay would need 2^51
/// local commits to reach it. Still below 2^53.
///
/// One relay tier only (SPEC 9 §9.5): a durable replica behind a relay would
/// number from `LOCAL_BASE` like the relay, so that is not supported.
pub const NON_DURABLE_BASE: u64 = LOCAL_BASE + (1 << 51);

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
    /// Send what an earlier `send` accepted but could not yet hand on
    /// whole (the rest of a split message). `Backpressure` means some is
    /// still waiting for the queue to drain. A transport whose `send`
    /// hands on every message whole needs nothing here; one wrapping
    /// another must forward it, or the inner one's parts stay unsent.
    fn flush(&mut self) -> Result<(), TransportError> {
        Ok(())
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
    /// The parts of a split message a full queue held back after its
    /// first went out. They go ahead of anything sent later: a reader
    /// joins parts in order, so the message is never sent again from its
    /// start.
    unsent: VecDeque<Vec<u8>>,
}

/// The largest encoded message one wire frame carries, leaving room for
/// the carrier's count and length prefixes.
const MAX_MESSAGE_FRAME_BYTES: usize = jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES - 64;

/// The first byte of a frame carrying a leading part of a message too
/// large for one frame: more parts follow. Postcard writes a
/// [`SyncMessage`]'s variant tag as one byte below 0x80, so neither part
/// marker begins a whole message, a `Hello` or a `WireFrame`.
const PART: u8 = 0xFF;
/// The first byte of the frame carrying a split message's last part.
const LAST_PART: u8 = 0xFE;

/// `message` as wire frames: one frame, unless it outgrows one (a first
/// result, one commit's fan-out, a large commit). Then its encoding
/// travels in as many parts as it takes, each behind a part marker, and
/// [`FrameJoiner`] makes them one message again. Splitting is a transport
/// matter: any message may be split, and no message knows it was. A
/// message beyond the logical message ceiling no reader joins is refused
/// here, not sent.
pub fn encode_frames(message: SyncMessage) -> Result<Vec<Vec<u8>>, TransportError> {
    let bytes =
        postcard::to_allocvec(&message).map_err(|e| TransportError::Failed(e.to_string()))?;
    if bytes.len() > jazz_protocol::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES {
        return Err(TransportError::Failed(format!(
            "a {} message of {} bytes is over the {}-byte message ceiling: not supported yet",
            message.kind(),
            bytes.len(),
            jazz_protocol::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES,
        )));
    }
    if bytes.len() <= MAX_MESSAGE_FRAME_BYTES {
        return Ok(vec![bytes]);
    }
    let parts = bytes.chunks(MAX_MESSAGE_FRAME_BYTES - 1);
    let last = parts.len() - 1;
    Ok(parts
        .enumerate()
        .map(|(i, part)| {
            let mut frame = Vec::with_capacity(part.len() + 1);
            frame.push(if i == last { LAST_PART } else { PART });
            frame.extend_from_slice(part);
            frame
        })
        .collect())
}

/// One whole message's bytes as a [`SyncMessage`]: exactly one postcard
/// value, so bytes after the message are malformed rather than truncated.
pub fn decode_message(frame: &[u8]) -> Result<SyncMessage, postcard::Error> {
    match postcard::take_from_bytes(frame)? {
        (message, []) => Ok(message),
        _ => Err(postcard::Error::DeserializeBadEncoding),
    }
}

/// Joins a split message's frames (see [`encode_frames`]) back into the
/// one message they encode.
pub struct FrameJoiner {
    /// The bytes of the parts received since the last whole message.
    parts: Option<Vec<u8>>,
    /// How many parts those are.
    frames: usize,
    /// The most bytes a joined message may have.
    ceiling: usize,
}

impl Default for FrameJoiner {
    fn default() -> Self {
        Self::with_ceiling(jazz_protocol::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES)
    }
}

impl FrameJoiner {
    /// A joiner refusing split messages over `ceiling` bytes: a server
    /// bounds what an untrusted peer makes it buffer by the connection's
    /// budget.
    pub fn with_ceiling(ceiling: usize) -> Self {
        Self {
            parts: None,
            frames: 0,
            ceiling,
        }
    }

    /// The message `frame` completes, if any. A frame over the frame
    /// limit or that does not decode, a whole message between a split
    /// message's parts, a last part with no leading ones, and parts beyond
    /// the joined-message ceiling or the batch frame count are the peer's
    /// error. A refused message's parts are dropped at once: the link
    /// closes, and none of it reaches the database.
    pub fn push(&mut self, frame: &[u8]) -> Result<Option<SyncMessage>, TransportError> {
        let result = self.push_checked(frame);
        if result.is_err() {
            *self = Self::with_ceiling(self.ceiling);
        }
        result
    }

    fn push_checked(&mut self, frame: &[u8]) -> Result<Option<SyncMessage>, TransportError> {
        let failed = |why: String| TransportError::Failed(format!("malformed sync frame: {why}"));
        if frame.len() > MAX_MESSAGE_FRAME_BYTES {
            return Err(failed("a frame over the frame limit".to_owned()));
        }
        let joined;
        let bytes = match (frame.split_first(), self.parts.take()) {
            (Some((&marker @ (PART | LAST_PART), part)), parts) => {
                let mut parts = match (marker, parts) {
                    (LAST_PART, None) => {
                        return Err(failed("a last part with no parts before it".to_owned()));
                    }
                    (_, parts) => parts.unwrap_or_default(),
                };
                if parts.len() + part.len() > self.ceiling
                    || self.frames >= jazz_protocol::protocol_limits::MAX_WIRE_BATCH_FRAMES
                {
                    return Err(failed(
                        "a split message beyond the message ceiling".to_owned(),
                    ));
                }
                parts.extend_from_slice(part);
                if marker == PART {
                    self.parts = Some(parts);
                    self.frames += 1;
                    return Ok(None);
                }
                self.frames = 0;
                joined = parts;
                joined.as_slice()
            }
            (_, Some(_)) => {
                return Err(failed(
                    "a split message's parts end with its last part, not a whole message"
                        .to_owned(),
                ));
            }
            (_, None) => frame,
        };
        decode_message(bytes)
            .map(Some)
            .map_err(|error| failed(error.to_string()))
    }
}

impl<W> WireTransportAdapter<W> {
    pub fn current(wire: W) -> Self {
        Self {
            wire,
            joiner: FrameJoiner::default(),
            failed: None,
            unsent: VecDeque::new(),
        }
    }
}

impl<W: WireTransport> Transport for WireTransportAdapter<W> {
    /// Accepts `message` once its first frame is queued: `Backpressure`
    /// means none of it was, so the caller keeps it. Parts a full queue
    /// refuses after that wait in `unsent` for [`Transport::flush`].
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.flush()?;
        let mut frames = encode_frames(message)?.into_iter();
        let first = frames.next().expect("a message encodes to a frame");
        self.wire.send_frame(first)?;
        self.unsent.extend(frames);
        match self.flush() {
            Err(TransportError::Backpressure) => Ok(()),
            result => result,
        }
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        while let Some(frame) = self.unsent.front() {
            self.wire.send_frame(frame.clone())?;
            self.unsent.pop_front();
        }
        Ok(())
    }

    /// The next whole message: a split message whose frames have not all
    /// arrived waits for the rest.
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
    // Frames of a split message join across the batch, as a reader's do.
    let mut joiner = FrameJoiner::default();
    let mut message = |frame: &[u8]| joiner.push(frame).map(drop).map_err(|e| e.to_string());
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
    /// Raw TEXT bytes.
    Text(#[serde(with = "serde_bytes")] Vec<u8>),
    Blob(Vec<u8>),
    /// A chunked large value, as its encoded descriptor: a stored chunked
    /// cell names an epoch local to its store, so it travels as the
    /// descriptor and is interned again where it lands (never as chunk
    /// bytes).
    Large(#[serde(with = "serde_bytes")] Vec<u8>),
    /// A chunked large value as the edits extending the same column's
    /// value in a version of the row the receiver holds: only on the
    /// wire, where the receiver makes it [`Cell::Large`] again (SPEC 19.8).
    Delta(WireDelta),
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
            // Not a stored cell until interned ([`Inner::stored_cells`]).
            Cell::Large(_) | Cell::Delta(_) => Sql::Null,
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

/// The most image bytes one `Rows` carries: a first result of any size
/// travels as several, each well under the logical message ceiling a
/// reader joins split frames up to.
const ROWS_MESSAGE_BYTES: usize = jazz_protocol::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES / 4;

/// `images` (and the `origins` they come with) as `Rows` messages of at
/// most `limit` ([`ROWS_MESSAGE_BYTES`]) of images each. A reader installs each
/// message whole, so a commit's versions (one seq) never straddle two: a
/// result that needs more than one goes in seq order, and a single
/// commit larger than the bound still travels as one.
fn rows_messages(
    mut images: Vec<Image>,
    origins: Vec<(u64, [u8; 16], u64)>,
    limit: usize,
) -> Vec<SyncMessage> {
    let size = |image: &Image| {
        postcard::serialize_with_flavor(image, postcard::ser_flavors::Size::default())
            .unwrap_or(usize::MAX)
    };
    let sizes: Vec<usize> = images.iter().map(size).collect();
    if sizes.iter().fold(0usize, |sum, n| sum.saturating_add(*n)) <= limit {
        if images.is_empty() && origins.is_empty() {
            return Vec::new();
        }
        return vec![SyncMessage::Rows { images, origins }];
    }
    let mut sized: Vec<(usize, Image)> = sizes.into_iter().zip(images.drain(..)).collect();
    sized.sort_by_key(|(_, image)| image.seq);
    let mut out = Vec::new();
    let mut origins = Some(origins);
    let mut batch: Vec<Image> = Vec::new();
    let mut bytes = 0usize;
    for (n, image) in sized {
        let same_commit = batch.last().is_some_and(|last| last.seq == image.seq);
        if !batch.is_empty() && !same_commit && bytes.saturating_add(n) > limit {
            out.push(SyncMessage::Rows {
                images: std::mem::take(&mut batch),
                origins: origins.take().unwrap_or_default(),
            });
            bytes = 0;
        }
        bytes = bytes.saturating_add(n);
        batch.push(image);
    }
    out.push(SyncMessage::Rows {
        images: batch,
        origins: origins.take().unwrap_or_default(),
    });
    out
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

/// What the writer of a row version built it on (SPEC 4 §4.6, "Base of a
/// write"): the version of the row its image was, in names the receiver
/// knows. A write over no held image (an insert, a blind write) sends none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireBase {
    /// The commit's first op that writes the row.
    pub op: u32,
    /// The row's branch key (empty unbranched).
    pub key: Vec<u8>,
    pub on: BuiltOn,
}

/// The settled version of a row a commit writes that its writer holds, so
/// that the writer did not push the nodes of its chunked values' trees
/// (SPEC 19.4): the authority counts that version's trees as the writer's
/// when it may read the version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireSource {
    /// The op that writes the row.
    pub op: u32,
    /// The row's branch key (empty unbranched).
    pub key: Vec<u8>,
    pub seq: u64,
}

/// The version a write was built on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltOn {
    /// An accepted version, by its upstream seq. From a relay's session it
    /// may be the relay's own unsettled version (a seq from `LOCAL_BASE`
    /// up), which the relay renames to the commit that wrote it.
    Seq(u64),
    /// The sender's own unsettled commit `back` txs before this one: the
    /// writer saw that commit as it sent it, and what that was built on.
    Own { back: u64 },
    /// An unsettled commit by its sender's (node, tx), as a relay names one
    /// of its own commits.
    Commit { node: [u8; 16], tx: u64 },
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

impl WireOp {
    /// The table the op writes, as the sender's schema names it.
    pub fn table(&self) -> &str {
        match self {
            WireOp::Insert { table, .. }
            | WireOp::Update { table, .. }
            | WireOp::Upsert { table, .. }
            | WireOp::Delete { table, .. }
            | WireOp::Restore { table, .. }
            | WireOp::Move { table, .. } => table,
        }
    }
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
    /// `reason`: the [`Reason`] by its stable name. A name this build does
    /// not know arrives as a rejection without a reason, its message intact.
    Rejected {
        reason: Option<String>,
        message: String,
    },
    Other(String),
}

/// A large-value node named by its object hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireChunkRef {
    pub hash: [u8; 32],
}

/// A large-value node's encoded bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireChunk {
    pub hash: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

/// A pull of a large value's nodes, citing the version of a row that
/// holds it (SPEC 19.8): the authority answers only for a version the
/// reader may read, and only with nodes of that version's tree `root`.
/// `nodes` are the ones the reader lacks; `span`, base-tree bytes
/// `[start, end)` a read needs, asks for every node beneath them that
/// covers those bytes in the same answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WirePull {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub seq: u64,
    pub root: [u8; 32],
    pub nodes: Vec<[u8; 32]>,
    pub span: Option<(u64, u64)>,
}

impl WireChunkRef {
    fn request(&self) -> ChunkRequest {
        ChunkRequest {
            object_hash: self.hash,
        }
    }
}

impl WireChunk {
    fn request(&self) -> ChunkRequest {
        ChunkRequest {
            object_hash: self.hash,
        }
    }
}

/// Encoded node bytes per `PushChunks` or `Chunks` message: a batch is
/// flushed once it reaches this, so with one more node at most and the
/// message's framing it still fits one wire frame.
const CHUNK_BATCH_BYTES: usize = jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES / 2;
/// The most node bytes one range pull is answered with: a read past them
/// pulls again for the rest.
const PULL_BYTES: usize = 4 << 20;
/// The most node bytes one `ChunkRequests` is answered with, across all
/// its pulls: the rest is answered once that has been sent.
const ANSWER_PULL_BYTES: usize = 4 * PULL_BYTES;
const _: () = assert!(
    CHUNK_BATCH_BYTES + jazz_types::large_values::MAX_ENCODED_NODE_BYTES + 64 * 1024
        <= jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES
);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    // client → server
    // Variants before `Commit` keep their wire tags: a replica stores its
    // pending uploads as `Commit` frames.
    /// A read of `query`, kept current until `Unsubscribe` or answered
    /// once (see [`ReadSpan`]). Either way the server ships the result's
    /// rows and supporting rows, then `Settled` (or `Refused`).
    /// `include_deleted`: the result also lists deleted rows (the root's),
    /// which ship as their deleted current versions.
    /// `tier`: how far the read reaches past a relay: `LocalOnly` is served
    /// from the relay's store alone (a non-durable foreground's LocalOnly
    /// read), `LocalFirst` from it too while the relay asks its upstream,
    /// `Remote` once its upstream answered. An authority's store is the
    /// answer either way.
    /// `wait`: how long (ms) a relay waits for its upstream's answer while
    /// it may give one, then serves its store: a LocalFirst read's first
    /// load (the reader's `firstLoadRemoteWaitMs`), or a one-shot Remote
    /// read that takes the relay's view when the authority cannot answer (a
    /// serializable transaction's snapshot). `None`: a live LocalFirst read
    /// waits while the upstream may answer, a one-shot one is served now;
    /// a Remote one waits however long it takes (only the authority
    /// confirms a remote result).
    Read {
        id: u64,
        query: Box<Query>,
        binding: BTreeMap<String, Value>,
        include_deleted: bool,
        tier: ReadTier,
        wait: Option<u64>,
        span: ReadSpan,
    },
    Unsubscribe {
        id: u64,
    },
    /// Rows, as (stored table, id, branch key, seq), whose shipped deltas
    /// the replica could not apply: it lacks the version a delta was made
    /// over (SPEC 19.8). The server ships each whole, or evicts it. Sent
    /// when a delta fails, and again on each connection until the row
    /// arrives whole. The seq is not read.
    Hold {
        rows: Vec<(String, [u8; 16], Vec<u8>, u64)>,
    },
    /// A read's result is complete through server seq `through`.
    /// `listing`: the result's rows, in order: always for a one-shot
    /// read, and for a live one's opening and window changes when it is
    /// limited or its opening could not tell what left it; the replica
    /// drops cached rows a listing leaves out. `global`: the result is the
    /// global authority's (directly, or through relays that settle by
    /// their upstream), not a relay's own store.
    Settled {
        id: u64,
        through: u64,
        listing: Option<Vec<[u8; 16]>>,
        global: bool,
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
        /// The ops' chunked cells that travel as edits extending the
        /// writer's newest settled version of their row (SPEC 19.8).
        deltas: Vec<CellDelta>,
        /// For each op whose chunked values travel as trees, the settled
        /// version of its row their unpushed nodes sit under.
        sources: Vec<WireSource>,
        /// The base of each row the commit writes over an image its
        /// writer held (SPEC 4 §4.6); a row it inserted, or wrote without
        /// holding, has none. Only the authority's conflict derivation
        /// reads it (INV-HIST-21).
        bases: Vec<WireBase>,
        /// The session commit a relay uploads this one for, as (node, tx),
        /// when it is not the sender's own: the writer its bases' unnamed
        /// `pending` nodes stand for.
        origin: Option<([u8; 16], u64)>,
    },
    /// Large-value nodes the client lacks, pulled through the row
    /// versions that hold them.
    ChunkRequests {
        pulls: Vec<WirePull>,
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
    /// Row images. `origins` names the transactions behind the history
    /// versions among them, as (seq, writer node, writer tx): a replica
    /// lists a history version only once it knows its `$transactionId`,
    /// and a one-shot read frozen at a snapshot names the commits that
    /// snapshot holds. Empty for current rows.
    Rows {
        images: Vec<Image>,
        origins: Vec<(u64, [u8; 16], u64)>,
    },
    /// The row is unavailable to this reader (a write or a policy change
    /// took its read access): the replica drops its copy (INV-SYNC-14).
    Evict {
        table: String,
        id: [u8; 16],
        key: Vec<u8>,
    },
    /// A `Read` the server cannot serve, with why: the
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
    /// every connection and again ahead of commits staged under another
    /// version. Its queries and writes use that version's table and column
    /// names, which the server resolves to its stored names; row images
    /// and evictions travel by stored names either way. A version the
    /// server has not admitted is named like the server's current schema.
    Schema {
        version: [u8; 16],
    },
    // server → client
    /// Whether the server has admitted the schema version the session
    /// declared, sent when that changes. Until it is admitted the server
    /// holds the session's messages (a draft awaiting its lens), so a
    /// read that must be covered by the server fails with the reason
    /// rather than waiting out its budget.
    SchemaAdmission {
        admitted: bool,
    },
    /// A relay holds the session's commit `tx` in its store and uploads
    /// it: the commit is `Local` for a non-durable session (a tab over its
    /// worker), whose own store is memory. Its fate follows once the
    /// relay's upstream answers.
    ///
    /// This is durability progress, never a decision: a `Stored` commit
    /// can still be rejected, and a [`SyncMessage::Fate`] always follows
    /// it. It is not a non-global `Fate` either: that one is the final
    /// word of a replica with no upstream, which never comes back with a
    /// global verdict, while a relay's `Stored` promises one.
    Stored {
        tx: u64,
    },
    /// Client → server, after its first `Schema`: the schema versions
    /// whose stored names this replica keeps, each with
    /// [`names_digest`] of them. The server answers with the
    /// [`SyncMessage::Catalogue`] entries it lacks or keeps otherwise.
    Holds {
        versions: Vec<([u8; 16], [u8; 16])>,
    },
    /// Server → client: schema versions this store admitted, in admission
    /// order, with where it stores their tables and columns, and the lens
    /// cells its lenses made. A replica stores rows as its upstream does
    /// (rows travel by stored names, untranslated), so it admits each
    /// version under these names, renaming what it named otherwise while
    /// it did not know them. Sent ahead of any row after a `Holds`, and
    /// again when the store admits a version.
    Catalogue {
        versions: Vec<CatalogueVersion>,
        cells: Vec<CatalogueCell>,
    },
}

/// How long a [`SyncMessage::Read`] lasts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReadSpan {
    /// Kept current until `Unsubscribe`: what changes in the result
    /// ships as it changes, and each settled state is a `Settled`.
    /// `since`: resumed, the replica holding the result as it settled
    /// through that seq; the opening ships only what changed after it.
    Live { since: Option<u64> },
    /// Answered once, at the current state or as of server seq `at`, in
    /// branch view `view`, whose window and versions the read observes. A
    /// view frozen at a snapshot reference is resolved by the receiver (a
    /// reopened tab's capture, which its worker settled), which also sends
    /// the origins of the commits it names (see [`SyncMessage::Rows`]).
    Once {
        at: Option<u64>,
        view: Box<ReadViewSourceSpec>,
    },
}

/// One admitted schema version in a [`SyncMessage::Catalogue`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogueVersion {
    /// Its structural schema, without permissions.
    pub schema: crate::protocol::SchemaVersion,
    /// Per table: (name, stored name, per column (name, stored name)).
    pub names: Vec<(String, String, Vec<(String, String)>)>,
}

/// A stored column a lens fills (see `crate::names::LensCell`): a copy
/// of stored column `source`, or a dropped column's backwards default.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogueCell {
    pub table: String,
    pub column: String,
    pub since: [u8; 16],
    pub source: Option<String>,
    pub default: Cell,
}

impl SyncMessage {
    /// The variant's name, for errors and diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Read { .. } => "Read",
            Self::Unsubscribe { .. } => "Unsubscribe",
            Self::Hold { .. } => "Hold",
            Self::Commit { .. } => "Commit",
            Self::ChunkRequests { .. } => "ChunkRequests",
            Self::PushChunks { .. } => "PushChunks",
            Self::Chunks { .. } => "Chunks",
            Self::Rows { .. } => "Rows",
            Self::Evict { .. } => "Evict",
            Self::Settled { .. } => "Settled",
            Self::Refused { .. } => "Refused",
            Self::Fate { .. } => "Fate",
            Self::Advise { .. } => "Advise",
            Self::Advice { .. } => "Advice",
            Self::Schema { .. } => "Schema",
            Self::SchemaAdmission { .. } => "SchemaAdmission",
            Self::Stored { .. } => "Stored",
            Self::Holds { .. } => "Holds",
            Self::Catalogue { .. } => "Catalogue",
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

/// A replica's unsettled commits, in commit order, with the pending txs of
/// each row they write (so an upload finds a row's newest pending commit
/// without scanning the queue).
#[derive(Default)]
struct PendingQueue {
    commits: Vec<Pending>,
    by_row: HashMap<RowKey, BTreeSet<u64>>,
}

impl std::ops::Deref for PendingQueue {
    type Target = [Pending];

    fn deref(&self) -> &[Pending] {
        &self.commits
    }
}

impl PendingQueue {
    /// Stored table `from` is stored as `to` now.
    fn moved_table(&mut self, from: &str, to: &str) {
        let commits = std::mem::take(&mut self.commits);
        self.by_row.clear();
        for mut pending in commits {
            for row in &mut pending.rows {
                if row.0 == from {
                    row.0 = to.to_owned();
                }
            }
            self.push(pending);
        }
    }

    fn push(&mut self, pending: Pending) {
        for row in &pending.rows {
            self.by_row
                .entry(row.clone())
                .or_default()
                .insert(pending.tx);
        }
        self.commits.push(pending);
    }

    fn pop(&mut self) {
        if let Some(pending) = self.commits.pop() {
            self.unindex(&pending);
        }
    }

    fn remove(&mut self, tx: u64) {
        if let Some(at) = self.commits.iter().position(|p| p.tx == tx) {
            let pending = self.commits.remove(at);
            self.unindex(&pending);
        }
    }

    fn unindex(&mut self, pending: &Pending) {
        for row in &pending.rows {
            if let Some(txs) = self.by_row.get_mut(row) {
                txs.remove(&pending.tx);
                if txs.is_empty() {
                    self.by_row.remove(row);
                }
            }
        }
    }

    fn get_mut(&mut self, tx: u64) -> Option<&mut Pending> {
        self.commits.iter_mut().find(|p| p.tx == tx)
    }

    /// The newest pending commit that writes `row`.
    fn newest(&self, row: &RowKey) -> Option<u64> {
        self.by_row.get(row).and_then(|txs| txs.last().copied())
    }
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
    /// How far it reaches (see `SyncMessage::Read`).
    reach: RemoteReach,
    pub(crate) answered: Rc<StdCell<bool>>,
    /// Whether the last answer was the global authority's.
    pub(crate) global: Rc<StdCell<bool>>,
    /// Why the server refused this query, once it has.
    pub(crate) refused: Rc<RefCell<Option<Error>>>,
    /// The server seq this query's result was last settled through, kept
    /// in `_jazz_watermarks` under `watermark`, so a reopened replica
    /// resumes from it too.
    through: Option<u64>,
    watermark: [u8; 32],
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

/// A commit of this store's that the authority refused, kept with its
/// payload so it can be retried (INV-TX-9).
#[derive(Clone, Debug)]
pub struct RejectedTransaction {
    /// The commit's local seq (its transaction time).
    pub seq: u64,
    /// Why the authority refused it.
    pub error: Error,
    pub serializable: bool,
    /// The writes it made, as it uploaded them.
    pub ops: Vec<WireOp>,
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
    pending: PendingQueue,
    unsent: Vec<SyncMessage>,
    /// Commits sent but not settled, for resending after a reconnect.
    sent: Vec<SyncMessage>,
    /// Pending commits written under a schema version other than the
    /// current one (before an upgrade), by tx: their ops name tables and
    /// columns as it does, so they upload under it.
    versions: HashMap<u64, [u8; 16]>,
    node: [u8; 16],
    connection: u64,
    fates: HashMap<u64, Result<(), Error>>,
    /// Accepted fates from a replica with no server: they order the
    /// commit, but it never becomes `Global`.
    local_fates: HashSet<u64>,
    /// Fates this replica gives its own commits without sending them (one
    /// too large to send), applied with the next messages that arrive.
    refused: Vec<SyncMessage>,
    /// Commits the relay this non-durable replica writes through holds
    /// in its store, their fate still to come ([`SyncMessage::Stored`]).
    stored: HashSet<u64>,
    /// Transient cache reconstructed from pending row keys and history.
    baselines: HashMap<(String, RowUuid, Vec<u8>), Option<Image>>,
    /// Highest server seq any answer was complete through.
    pub(crate) known: u64,
    /// The `known` cut last written to `_jazz_meta`.
    persisted_known: u64,
    answers: HashMap<u64, Option<Result<(u64, Vec<[u8; 16]>), Error>>>,
    /// Permission preflights asked for; `None` until answered.
    advice: HashMap<u64, Option<PermissionAdvice>>,
    /// The next permission preflight's id. Each client starts at a random
    /// point, so an id held over from another instance (a reopened
    /// database) or a late answer to one never names a request of this
    /// one (INV-API-28).
    next_advice: u64,
    /// Large-value nodes asked for and not yet answered.
    requested: BTreeSet<ChunkRequest>,
    /// Nodes to ask for on the next send.
    wanted: BTreeSet<ChunkRequest>,
    /// Nodes pushed for each unsettled commit.
    pushed: HashMap<u64, Vec<jazz_types::large_values::ContentHash>>,
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
    /// A relayed fate or read became ready, or the upstream sent rows or a
    /// settlement: the serving half runs again in the same tick, so a
    /// relay's subscribers see it without waiting for another tick.
    relay_ready: bool,
    /// The server said it has not admitted this client's schema version.
    unadmitted: bool,
    /// Rows asked for whole (their shipped deltas did not apply), until a
    /// whole image of each arrives. This store holds a stale version of
    /// each, so no result it settles is the server's: no watermark is
    /// stored while any is owed, and each is asked again on reconnect.
    whole: HashSet<(String, [u8; 16], Vec<u8>)>,
    /// No resume point was stored since they were last all forgotten, so
    /// forgetting them again (another eviction this tick) costs nothing.
    forgotten: bool,
}

/// A session's commit a relay uploads as its own, for its upload: the
/// session commit's (node, tx) and its bases in this store's terms.
pub(crate) struct Relayed {
    origin: ([u8; 16], u64),
    bases: Vec<WireBase>,
}

/// A session's commit a relay uploads as its own: (session, the session's
/// tx, the rows it wrote).
type RelayedFate = (usize, u64, Vec<(String, RowUuid, Vec<u8>)>);

/// A relayed read's upstream answer: its query, binding, whether it read
/// deleted rows, its read view, the rows it listed and the seq it is
/// complete through.
type RelayedAnswer = (
    Query,
    BTreeMap<String, Value>,
    bool,
    ReadViewSourceSpec,
    Vec<[u8; 16]>,
    u64,
);

/// A session's one-shot `Read` a relay asked its upstream for.
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
    /// The session's `Read`, served locally once the upstream answered.
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
/// waits for its upstream (see `SyncMessage::Read`).
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

/// How far a client's read reaches past its store, or `None` when its
/// store answers alone: a `LocalOnly` or frozen read. A non-durable
/// foreground's store starts empty, so its local view is its relay's: it
/// asks at every tier, a frozen read too (a reopened tab's capture names
/// commits only its relay settled), and tells the relay its tier and first
/// load's wait. To an authority a read is always remote.
pub(crate) fn remote_reach(
    opts: &ReadOpts,
    non_durable: bool,
    frozen: bool,
) -> Option<RemoteReach> {
    if non_durable {
        return Some(RemoteReach {
            tier: opts.tier,
            wait: crate::db::first_load_wait(opts.tier, &opts.first_load),
        });
    }
    (opts.tier != ReadTier::LocalOnly && !frozen).then_some(RemoteReach::REMOTE)
}

/// How long a remote read's first answer waits for the server: `Some(None)`
/// until it answers, `Some(Some(t))` until `t`, `None` not at all. A
/// `Remote` read and a non-durable foreground's always wait; a first load
/// waits for its timeout while the link `may_wait`.
pub(crate) fn remote_deadline(
    opts: &ReadOpts,
    non_durable: bool,
    may_wait: bool,
) -> Option<Option<Instant>> {
    match (opts.tier, &opts.first_load) {
        (ReadTier::Remote, _) => Some(None),
        _ if non_durable => Some(None),
        (_, FirstLoad::WaitForRemote { timeout_ms }) if *timeout_ms > 0 && may_wait => Some(Some(
            Instant::now() + std::time::Duration::from_millis(*timeout_ms),
        )),
        _ => None,
    }
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
    /// while the link to the relay is attached. A first load it holds
    /// for its upstream it serves from its store once that upstream
    /// cannot answer (see [`Db::serve_due_first_loads`]).
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
    /// Images to send this tick: (table, id, branch key).
    touched: BTreeSet<(String, RowUuid, Vec<u8>)>,
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
    /// row shipped (`id`), include target and array correlation, each held
    /// by the subscriptions that cover it. A commit that writes a matching
    /// row ships its image (or an eviction).
    watch: Watches,
    /// The newest version of each row with chunked cells the replica was
    /// shipped this session, by (table, id, branch key): later versions'
    /// chunked cells go as deltas over it (SPEC 19.8). The replica keeps a
    /// row it was shipped after the subscriptions that covered it end, so
    /// only an eviction drops it.
    shipped: HashMap<(String, [u8; 16], Vec<u8>), u64>,
    /// Resumed subscriptions whose opening is still to send.
    resume: BTreeMap<u64, Resume>,
    /// Supporting rows live subscriptions cover, per table, with whether
    /// the client could read each when last checked. A commit to a table
    /// their read policy depends on re-checks them: lost access ships an
    /// eviction, regained access ships the image.
    held: Kept,
    /// False once the client disconnected or its transport failed; the
    /// slot holds nothing and is reused by the next subscriber.
    open: bool,
    /// Which acceptance of this slot the session is ([`SubscriberId`]).
    generation: u64,
    /// Pushed chunk bytes admitted in the current staging-policy window,
    /// and when it started (the ingress rate limit).
    ingress: (Instant, usize),
    /// Inbound messages held back by the rate limit, or reads waiting for
    /// large-value chunks from this relay's upstream, in order.
    backlog: VecDeque<SyncMessage>,
    /// The tables each read in `backlog` depends on (`None`: unknown), so a
    /// read held across ticks is not compiled again on each; valid while
    /// the schema (and its policies) it was worked out under is current.
    backlog_reads: (Option<Rc<Schema>>, HashMap<u64, Option<BTreeSet<String>>>),
    /// The schema version the client declared (`None`: the current one).
    version: Option<crate::ids::SchemaVersionId>,
    /// Fetches whose upstream answer arrived, or that this relay already
    /// asked its upstream to refresh: served from this store.
    fetched_upstream: BTreeSet<u64>,
    /// A relay's pulls of nodes it is still fetching from its upstream:
    /// answered from its store once they arrive (or are unavailable).
    parked_pulls: Vec<WirePull>,
    /// Pulls past an answer's byte budget: answered once the session's
    /// outbox has drained (SPEC 19.8).
    deferred_pulls: Vec<WirePull>,
    /// The client was told its declared version is not admitted.
    told_unadmitted: bool,
    /// The versions the client keeps names for, with their digests
    /// (`None` until its `Holds`), and whether it is owed a `Catalogue`.
    holds: Option<HashMap<[u8; 16], [u8; 16]>>,
    catalogue_owed: bool,
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
    fn new(transport: Box<dyn Transport>, identity: AuthorSubject) -> Self {
        Self {
            transport,
            identity,
            claims: None,
            subs: BTreeMap::new(),
            outbox: Vec::new(),
            touched: BTreeSet::new(),
            committed: Vec::new(),
            echoes: Vec::new(),
            support: BTreeMap::new(),
            watch: Watches::default(),
            shipped: HashMap::new(),
            resume: BTreeMap::new(),
            held: Kept::default(),
            open: true,
            generation: 0,
            ingress: (Instant::now(), 0),
            backlog: VecDeque::new(),
            backlog_reads: (None, HashMap::new()),
            version: None,
            fetched_upstream: BTreeSet::new(),
            parked_pulls: Vec::new(),
            deferred_pulls: Vec::new(),
            told_unadmitted: false,
            holds: None,
            catalogue_owed: false,
            schema: None,
            relayed: BTreeMap::new(),
            first_load: BTreeMap::new(),
        }
    }

    /// Release everything the session holds (its shared results are swept
    /// once no other member reads them) and return the upstream
    /// subscriptions it relayed, for the caller to release.
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
    /// A version was admitted: a client that declared its holdings is
    /// told of it.
    pub(super) fn owe_catalogue(&mut self) {
        if self.holds.is_some() {
            self.catalogue_owed = true;
        }
    }

    fn idle(&self) -> bool {
        self.touched.is_empty()
            && self.outbox.is_empty()
            && self.resume.is_empty()
            && self
                .subs
                .values()
                .all(|(queue, ..)| queue.borrow().is_empty())
    }
}

/// A session's coherence watches, each held by the subscriptions that
/// rely on it (or by the fetch being served, for its duration): a key stays
/// watched while one holds it, so an ended subscription leaves nothing
/// watched that no other one covers.
#[derive(Default)]
struct Watches {
    holders: HashMap<u64, u32>,
    by_owner: HashMap<u64, HashSet<u64>>,
}

impl Watches {
    fn contains(&self, key: &u64) -> bool {
        self.holders.contains_key(key)
    }

    /// Hold `key` for `owner`: (whether the session did not watch it yet,
    /// whether `owner` did not hold it yet).
    fn hold(&mut self, owner: u64, key: u64) -> (bool, bool) {
        let fresh = !self.holders.contains_key(&key);
        let new = self.by_owner.entry(owner).or_default().insert(key);
        if new {
            *self.holders.entry(key).or_default() += 1;
        }
        (fresh, new)
    }

    /// Hold `key` for every owner that holds one of `matched`.
    fn share(&mut self, matched: &[u64], key: u64) {
        let owners: Vec<u64> = self
            .by_owner
            .iter()
            .filter(|(_, keys)| matched.iter().any(|k| keys.contains(k)))
            .map(|(owner, _)| *owner)
            .collect();
        for owner in owners {
            self.hold(owner, key);
        }
    }

    /// `owner` no longer relies on `key`.
    fn release_key(&mut self, owner: u64, key: u64) {
        if self
            .by_owner
            .get_mut(&owner)
            .is_some_and(|keys| keys.remove(&key))
        {
            self.drop_hold(key);
        }
    }

    /// Everything `owner` held.
    fn release(&mut self, owner: u64) {
        for key in self.by_owner.remove(&owner).unwrap_or_default() {
            self.drop_hold(key);
        }
    }

    /// An evicted row: the replica holds no copy, so nobody watches it.
    fn forget(&mut self, key: u64) {
        if self.holders.remove(&key).is_some() {
            for keys in self.by_owner.values_mut() {
                keys.remove(&key);
            }
        }
    }

    fn drop_hold(&mut self, key: u64) {
        if let Some(count) = self.holders.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.holders.remove(&key);
            }
        }
    }
}

/// A session's supporting rows, per table, with whether the client could
/// read each when last checked, each held by the subscriptions that ship
/// it (see [`Watches`]).
#[derive(Default)]
struct Kept {
    rows: BTreeMap<String, BTreeMap<(RowUuid, Vec<u8>), (bool, u32)>>,
    by_owner: HashMap<u64, HashSet<RowKey>>,
}

impl Kept {
    /// Keep `row` for `owner`, readable or not as checked now.
    fn keep(&mut self, owner: u64, table: &str, row: RowUuid, key: &[u8], can: bool) {
        let rows = self.rows.entry(table.to_owned()).or_default();
        let entry = rows.entry((row, key.to_vec())).or_insert((can, 0));
        entry.0 = can;
        if self
            .by_owner
            .entry(owner)
            .or_default()
            .insert((table.to_owned(), row, key.to_vec()))
        {
            entry.1 += 1;
        }
    }

    fn release(&mut self, owner: u64) {
        for (table, row, key) in self.by_owner.remove(&owner).unwrap_or_default() {
            let Some(rows) = self.rows.get_mut(&table) else {
                continue;
            };
            if let Some(entry) = rows.get_mut(&(row, key.clone())) {
                entry.1 -= 1;
                if entry.1 == 0 {
                    rows.remove(&(row, key));
                }
            }
            if rows.is_empty() {
                self.rows.remove(&table);
            }
        }
    }

    fn tables(&self) -> impl Iterator<Item = &String> {
        self.rows.keys()
    }

    fn of(&self, table: &str) -> Option<&BTreeMap<(RowUuid, Vec<u8>), (bool, u32)>> {
        self.rows.get(table)
    }

    /// Record a re-check: whether the client's access to `row` changed.
    fn recheck(&mut self, table: &str, row: RowUuid, key: &[u8], can: bool) -> bool {
        match self
            .rows
            .get_mut(table)
            .and_then(|rows| rows.get_mut(&(row, key.to_vec())))
        {
            Some(entry) if entry.0 != can => {
                entry.0 = can;
                true
            }
            _ => false,
        }
    }
}

/// How a resumed subscription's opening ships: the client held its result
/// and supporting rows as of server seq `since`.
#[derive(Clone, Debug)]
struct Resume {
    since: u64,
    /// A table that decides membership (a policy input, a joined or filtered
    /// table) was written after `since`: rows may have entered the result
    /// without changing. `prior` is the result as of `since`; a row not in
    /// it ships, and supporting rows whose access changed ship or evict.
    prior: Option<HashSet<RowUuid>>,
}

/// What a resumed opening may leave out while following supporting rows:
/// versions the client holds since `since`.
#[derive(Clone, Copy)]
struct Floor {
    since: u64,
    /// Membership inputs changed: a held version still ships when the
    /// client could not read it as of `since`.
    diff: bool,
}

/// One owner's walk over supporting rows: what it ships, watches and keeps.
struct Follow<'w> {
    owner: u64,
    identity: Option<AuthorSubject>,
    out: &'w mut Vec<Image>,
    watch: &'w mut Watches,
    held: &'w mut Kept,
    /// Supporting rows found unreadable on a resume whose membership
    /// inputs changed: the client may hold them, so each ships its
    /// eviction.
    gone: &'w mut Vec<RowKey>,
    followed: &'w mut HashSet<(usize, u64)>,
}

/// The owner of a session's watches on rows outside its subscriptions:
/// what its one-shot reads returned and its commits touched. They stay
/// current until the session ends.
const SESSION_OWNER: u64 = u64::MAX;

/// The watch on row `id` of stored table `table`.
fn id_watch(table: &str, id: [u8; 16]) -> u64 {
    wk((
        table.to_owned(),
        "id".to_owned(),
        watch_key(&Sql::Blob(id.to_vec())),
    ))
}

/// The `_jazz_meta` key of the seq the current schema was activated at.
pub(super) const ACTIVATED_META: &str = "activated";
/// The `_jazz_meta` key of a digest of the current schema's read policies.
pub(super) const POLICIES_META: &str = "policies";

/// The stored tables `support` reads rows of.
fn support_tables(support: &[Support], out: &mut Vec<String>) {
    for support in support {
        match support {
            Support::Include { target, .. } => out.push(target.clone()),
            Support::Array { table, nested, .. } => {
                out.push(table.clone());
                support_tables(nested, out);
            }
            Support::Reach { edges, .. } => out.push(edges.clone()),
        }
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

/// Handle for an accepted subscriber. Session slots are reused, so the
/// handle also names which acceptance it is: a stale one names nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberId {
    slot: usize,
    generation: u64,
}

impl SubscriberId {
    /// The session slot it names (for tests and metrics).
    pub fn slot(&self) -> usize {
        self.slot
    }
}

fn wire_error(error: &Error) -> WireError {
    match error.code {
        ErrorCode::TransactionConflict => WireError::Conflict(error.message.clone()),
        ErrorCode::WriteRejected => WireError::Rejected {
            reason: error.reason.map(|reason| reason.as_str().to_owned()),
            message: error.message.clone(),
        },
        _ => WireError::Other(error.to_string()),
    }
}

fn from_wire_error(error: WireError) -> Error {
    match error {
        WireError::Conflict(m) => Error::new(ErrorCode::TransactionConflict, m),
        WireError::Rejected { reason, message } => Error {
            code: ErrorCode::WriteRejected,
            reason: reason.as_deref().and_then(Reason::from_name),
            message,
        },
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
        support.extend(self.policy_support(table, crate::compile::DEFAULT_INHERITS_DEPTH)?);
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

    /// The rows a read of `table` rests on under its read policy: the
    /// parents it inherits from and the rows its correlated `EXISTS`
    /// clauses consult. A replica (or a backend reading for a request's
    /// session) evaluates the policy locally over them.
    fn policy_support(
        &self,
        table: &crate::values::Table,
        depth: usize,
    ) -> Result<Vec<Support>, Error> {
        let mut support = self.inherited_support(table, depth)?;
        support.extend(self.exists_support(table, depth)?);
        Ok(support)
    }

    /// The rows of each table a correlated `EXISTS` in `table`'s read
    /// policy consults, `depth` levels down: those whose correlated column
    /// equals the read row's, as if the reader had read them itself. An
    /// uncorrelated `EXISTS` has no per-row support.
    fn exists_support(
        &self,
        table: &crate::values::Table,
        depth: usize,
    ) -> Result<Vec<Support>, Error> {
        use crate::tools::{CmpOp, Operation, PolicyExpr, PolicyValue};
        /// The `(inner, outer)` columns an `EXISTS` condition correlates.
        fn correlation(condition: &PolicyExpr) -> Option<(&str, &str)> {
            match condition {
                PolicyExpr::Cmp {
                    column,
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(path),
                } => match path.as_slice() {
                    [outer_row, outer] if outer_row == "__jazz_outer_row" => Some((column, outer)),
                    _ => None,
                },
                PolicyExpr::And(parts) => parts.iter().find_map(correlation),
                _ => None,
            }
        }
        fn clauses<'p>(policy: &'p PolicyExpr, out: &mut Vec<(&'p str, &'p PolicyExpr)>) {
            match policy {
                PolicyExpr::Exists { table, condition } => out.push((table, condition)),
                PolicyExpr::And(parts) | PolicyExpr::Or(parts) => {
                    parts.iter().for_each(|part| clauses(part, out))
                }
                PolicyExpr::Not(part) => clauses(part, out),
                _ => {}
            }
        }
        let mut support = Vec::new();
        if depth == 0 {
            return Ok(support);
        }
        let mut exists = Vec::new();
        if let Some(policy) = table.policy(Operation::Select) {
            clauses(policy, &mut exists);
        }
        for (consulted, condition) in exists {
            let Some((inner, outer)) = correlation(condition) else {
                continue;
            };
            let consulted = self.schema.table(consulted)?;
            support.push(Support::Array {
                table: consulted.stored.clone(),
                inner: consulted.stored_name(inner).to_owned(),
                outer: table.stored_name(outer).to_owned(),
                outer_array: crate::compile::is_array(table, outer),
                inner_array: crate::compile::is_array(consulted, inner),
                nested: self.policy_support(consulted, depth - 1)?,
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
    /// their keys for `follow`'s owner so later writes reach the replica.
    /// With a `floor`, versions the client holds since then are followed
    /// and watched but not shipped again.
    fn follow_support(
        &self,
        support: &[Support],
        image: &Image,
        floor: Option<Floor>,
        follow: &mut Follow<'_>,
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
                        self.follow_edges(&reach, value, floor, follow)?;
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
                self.follow_value(table, column, value, contains, nested, floor, follow)?;
            }
        }
        Ok(())
    }

    /// Whether a found supporting row ships: not watched yet (the replica
    /// does not hold it current), unless the client holds this version
    /// since the floor and could read it then. A row it could not read
    /// then entered through a membership input that changed.
    fn ships(
        &self,
        table: &str,
        found: &Image,
        unwatched: bool,
        floor: Option<Floor>,
        identity: Option<AuthorSubject>,
    ) -> Result<bool, Error> {
        let Some(floor) = floor.filter(|floor| found.seq <= floor.since) else {
            return Ok(unwatched);
        };
        if !floor.diff {
            return Ok(false);
        }
        let row = RowUuid::from_bytes(found.id);
        Ok(!readable_version(
            self,
            table,
            row,
            &found.key,
            floor.since,
            identity,
        )?)
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
        floor: Option<Floor>,
        follow: &mut Follow<'_>,
    ) -> Result<(), Error> {
        // A key the owner holds was followed for it already, and later
        // writes to its rows ship as they happen. Nested support is
        // followed again: a child that joined since may have unwatched
        // children. A key another subscription holds is followed for this
        // one too, so its rows stay watched when that one ends.
        let key = wk((table.to_owned(), column.to_owned(), watch_key(&value)));
        let (fresh, new) = follow.watch.hold(follow.owner, key);
        if !new && nested.is_empty() {
            return Ok(());
        }
        // Once per traversal, though: a policy whose EXISTS consults its
        // own table (a member reads the band's members) nests the same
        // support at every depth, and each row reaches the same key
        // again, so following it anew costs (rows per key)^depth. The
        // nested support node tells depths apart, so a key reached with
        // deeper support than before is still followed.
        if !follow.followed.insert((nested.as_ptr() as usize, key)) {
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
            let can = readable(self, table, row, &found.key, follow.identity)?;
            if follow.identity.is_some() {
                follow.held.keep(follow.owner, table, row, &found.key, can);
            }
            if !can {
                // Access may have ended while the client was away.
                if floor.is_some_and(|floor| floor.diff) {
                    follow.gone.push((table.to_owned(), row, found.key.clone()));
                }
                continue;
            }
            let (unwatched, _) = follow.watch.hold(
                follow.owner,
                wk((
                    table.to_owned(),
                    "id".to_owned(),
                    watch_key(&Sql::Blob(found.id.to_vec())),
                )),
            );
            // Not watched yet, so the replica does not hold it current; or
            // a new correlation's row.
            let ship = self.ships(table, &found, unwatched || fresh, floor, follow.identity)?;
            // A row that ships may point at rows the client never had:
            // its own support is followed whole.
            let below = if ship { None } else { floor };
            if ship {
                follow.out.push(found.clone());
            }
            if !nested.is_empty() {
                self.follow_support(nested, &found, below, follow)?;
            }
        }
        Ok(())
    }

    /// Ship the readable edges of a gather from `value` and, transitively,
    /// from their parents, watching each frontier value so later edges
    /// reach the replica. A value the owner holds was followed already:
    /// cycles end.
    fn follow_edges(
        &self,
        reach: &Reach<'_>,
        value: Sql,
        floor: Option<Floor>,
        follow: &mut Follow<'_>,
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
            let (_, new) = follow.watch.hold(follow.owner, wk(key));
            if !new {
                continue;
            }
            let filter = if reach.member == "id" {
                "id = ?1".to_owned()
            } else {
                format!("{} = ?1", quote(reach.member))
            };
            for found in self.images_where(reach.edges, false, &filter, &[&value])? {
                let row = RowUuid::from_bytes(found.id);
                let can = readable(self, reach.edges, row, &found.key, follow.identity)?;
                if follow.identity.is_some() {
                    follow
                        .held
                        .keep(follow.owner, reach.edges, row, &found.key, can);
                }
                if !can {
                    if floor.is_some_and(|floor| floor.diff) {
                        follow
                            .gone
                            .push((reach.edges.to_owned(), row, found.key.clone()));
                    }
                    continue;
                }
                if let Some(parent) = image_cell(&found, reach.parent) {
                    frontier.push(parent);
                }
                let (unwatched, _) = follow.watch.hold(
                    follow.owner,
                    wk((
                        reach.edges.to_owned(),
                        "id".to_owned(),
                        watch_key(&Sql::Blob(found.id.to_vec())),
                    )),
                );
                if self.ships(reach.edges, &found, unwatched, floor, follow.identity)? {
                    follow.out.push(found);
                }
            }
        }
        Ok(())
    }

    /// Stored tables stored table `table`'s read policy reads, as `author`
    /// (claims do not change which tables a policy names).
    pub(super) fn read_policy_tables(
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
                        // The authority's record of what the version was
                        // built on; never replicated.
                        "_base" | "_cols" => {}
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
            self.portable_large(image)?;
        }
        Ok(images)
    }

    /// Images carry chunked values by their descriptors: epochs are local
    /// to one database.
    fn portable_large(&self, image: &mut Image) -> Result<(), Error> {
        let Some(table) = self.physical.tables.get(&image.table) else {
            return Ok(());
        };
        for (name, cell) in &mut image.cells {
            let Cell::Int(chunked) = cell else {
                continue;
            };
            if !table
                .by_stored_column(name)
                .is_some_and(crate::large::large_capable)
            {
                continue;
            }
            let value = self.chunks.descriptor(*chunked)?;
            *cell = Cell::Large(
                jazz_types::large_values::encode_large_value_ref(&value)
                    .map_err(|e| Error::new(ErrorCode::Storage, format!("large value: {e}")))?,
            );
        }
        Ok(())
    }

    /// `image`'s chunked cells as deltas over the version of the row
    /// `shipped` says the replica holds, and `shipped` updated for it
    /// (SPEC 19.8).
    fn ship_deltas(
        &self,
        shipped: &mut HashMap<(String, [u8; 16], Vec<u8>), u64>,
        image: &mut Image,
    ) -> Result<(), Error> {
        let held = shipped
            .get(&(image.table.clone(), image.id, image.key.clone()))
            .copied();
        hold_shipped(shipped, image);
        let Some(seq) = held.filter(|_| !image.deleted) else {
            return Ok(());
        };
        let row = RowUuid::from_bytes(image.id);
        let Some(base) = self.version_image(&image.table, row, &image.key, seq)? else {
            return Ok(());
        };
        for (name, cell) in &mut image.cells {
            let Cell::Large(descriptor) = cell else {
                continue;
            };
            let Some((_, old)) = base.cells.iter().find(|(n, _)| n == name) else {
                continue;
            };
            let (Some(old), Ok(new)) = (
                self.large_of(&image.table, name, old)?,
                jazz_types::large_values::decode_large_value_ref(descriptor),
            ) else {
                continue;
            };
            if let Some(delta) = large_delta::extend(seq, &old, &new) {
                *cell = Cell::Delta(delta);
            }
        }
        Ok(())
    }

    /// The version of a row at `seq`, current or in history.
    fn version_image(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<Option<Image>, Error> {
        let id = row.0.as_bytes().to_vec();
        let seq = seq as i64;
        for history in [false, true] {
            let found = self
                .images_where(
                    table,
                    history,
                    "id = ?1 AND _b = ?2 AND _seq = ?3",
                    &[&id, &key, &seq],
                )?
                .pop();
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// The chunked value a cell of stored `table`'s `column` holds, as an
    /// image carries it (a descriptor) or as stored (an epoch cell).
    fn large_of(
        &self,
        table: &str,
        column: &str,
        cell: &Cell,
    ) -> Result<Option<jazz_types::large_values::LargeValueRef>, Error> {
        match cell {
            Cell::Large(descriptor) => {
                Ok(jazz_types::large_values::decode_large_value_ref(descriptor).ok())
            }
            Cell::Int(chunked)
                if self
                    .physical
                    .tables
                    .get(table)
                    .and_then(|table| table.by_stored_column(column))
                    .is_some_and(crate::large::large_capable) =>
            {
                self.chunks.descriptor(*chunked).map(Some)
            }
            _ => Ok(None),
        }
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
            if let Cell::Large(descriptor) = cell {
                let value =
                    jazz_types::large_values::decode_large_value_ref(descriptor).map_err(|e| {
                        Error::new(
                            ErrorCode::Storage,
                            format!("large value from upstream: {e}"),
                        )
                    })?;
                cells.push((name.clone(), self.large_cell(&value)?));
                continue;
            }
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
        let physical = self.physical.tables.get(&image.table);
        let mut cells = self.stored_cells(image)?;
        // Key columns are this store's: an upstream's keys for columns
        // indexed here are kept, others dropped, and missing ones derived.
        if let Some(physical) = physical {
            cells.retain(|(name, _)| {
                crate::large::keyed_by(name).is_none() || physical.key_of_key(name).is_some()
            });
            for key in &physical.keys {
                if cells.iter().any(|(name, _)| *name == key.stored) {
                    continue;
                }
                let keyed = crate::large::keyed_by(&key.stored).expect("a key column");
                let Some((_, cell)) = cells.iter().find(|(name, _)| name == keyed) else {
                    continue;
                };
                let cell = match self.key_cell(cell) {
                    Ok(cell) => cell,
                    // A replica may not hold the value's first chunks.
                    Err(error) if crate::large::is_missing(&error) => Sql::Null,
                    Err(error) => return Err(error),
                };
                cells.push((key.stored.clone(), cell));
            }
        }
        for (column, value) in cells {
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

    /// The server evicted a row: this reader cannot read it. A row pending
    /// commits touch settles as absent and they replay over that; returns
    /// whether to rebase.
    fn receive_eviction(&mut self, table: &str, row: RowUuid, key: &[u8]) -> Result<bool, Error> {
        let client = self.client.as_mut().expect("client");
        let overlaid = (table.to_owned(), row, key.to_vec());
        match client.baselines.get_mut(&overlaid) {
            Some(baseline) => {
                *baseline = None;
                self.evict(table, row, key)?;
                Ok(true)
            }
            None => {
                self.evict(table, row, key)?;
                Ok(false)
            }
        }
    }

    /// The rows of own commit `tx` the server did not ship (`held`): its
    /// version at `through` is the one this replica wrote, so that becomes
    /// the settled image. A newer image already received stays.
    fn install_held(&mut self, tx: u64, through: u64, held: &[Held]) -> Result<(), Error> {
        if held.is_empty() {
            return Ok(());
        }
        let client = self.client.as_mut().expect("client");
        let Some(pending) = client.pending.get_mut(tx) else {
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

    /// The upstream image beneath pending local writes. A replay that
    /// leaves the row untouched keeps it current; otherwise snapshot()
    /// preserved it in history. The row/branch/sequence primary key bounds
    /// the lookup independently of unrelated rows and older versions.
    fn upstream_baseline(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<Image>, Error> {
        let id = row.0.as_bytes().to_vec();
        let base = self.local_base();
        if let Some(image) = self.current_image(table, row, key)?
            && image.seq < base
        {
            return Ok(Some(image));
        }
        let base = base as i64;
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
    CREATE TABLE IF NOT EXISTS _jazz_meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_authors (n INTEGER PRIMARY KEY, account BLOB NOT NULL, issuer TEXT NOT NULL, subject TEXT NOT NULL, UNIQUE (account, issuer, subject));\
    CREATE TABLE IF NOT EXISTS _jazz_applied (node BLOB NOT NULL, tx INTEGER NOT NULL, result BLOB NOT NULL, PRIMARY KEY (node, tx));\
    CREATE TABLE IF NOT EXISTS _jazz_settled (node BLOB NOT NULL, tx INTEGER NOT NULL, seq INTEGER, PRIMARY KEY (node, tx));\
    CREATE INDEX IF NOT EXISTS _jazz_settled_by_seq ON _jazz_settled (seq);\
    CREATE TABLE IF NOT EXISTS _jazz_nodes (node BLOB PRIMARY KEY, author INTEGER NOT NULL) WITHOUT ROWID;";

/// The retry store for refused commits. It is created by the first refusal
/// it keeps, not on open, so that opening an existing store without writing
/// leaves the file byte-identical.
const REJECTED_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_rejected \
    (tx INTEGER PRIMARY KEY, commit_msg BLOB NOT NULL, error BLOB NOT NULL, serializable INTEGER NOT NULL)";

/// The schema version each pending commit was written under (see
/// `persist_pending`). Created by the first pending commit, so that
/// opening an existing store without writing leaves the file
/// byte-identical.
const PENDING_VERSIONS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_pending_versions \
    (tx INTEGER PRIMARY KEY, version BLOB NOT NULL)";

/// Per remote query, the server seq its result was last settled
/// through (see `Remote::through`). Created by the first settlement, so
/// that opening an existing store without one leaves the file
/// byte-identical.
const WATERMARKS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_watermarks \
    (key BLOB PRIMARY KEY, through INTEGER NOT NULL) WITHOUT ROWID";

/// A remote query's `_jazz_watermarks` key: what it reads, and as whom.
/// How far it reaches (tier and wait) is not part of it: every tier's
/// result settles from the same server state.
fn watermark_key(
    query: &Query,
    binding: &BTreeMap<String, Value>,
    include_deleted: bool,
    author: AuthorSubject,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&encode(&(query, binding, include_deleted)));
    hasher.update(format!("{author:?}").as_bytes());
    *hasher.finalize().as_bytes()
}

type RowKey = (String, RowUuid, Vec<u8>);

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("encode sync state")
}

impl Inner {
    /// Pulls of `wanted` nodes, each through the received row version
    /// whose tree holds it, with the ranges reads of each tree wait on
    /// (SPEC 19.8). A node no received version holds is unavailable.
    fn pulls(
        &mut self,
        wanted: Vec<ChunkRequest>,
        spans: &HashMap<[u8; 32], std::ops::Range<u64>>,
    ) -> Result<Vec<WirePull>, Error> {
        let mut by_root: BTreeMap<[u8; 32], WirePull> = BTreeMap::new();
        for request in wanted {
            let Some((root, via)) = crate::large::via_of(&self.conn, &request.object_hash)? else {
                let client = self.client.as_mut().expect("client");
                client.requested.remove(&request);
                client.unavailable.insert(request);
                continue;
            };
            by_root
                .entry(root)
                .or_insert_with(|| WirePull {
                    table: via.table,
                    id: via.id,
                    key: via.key,
                    seq: via.seq,
                    root,
                    nodes: Vec::new(),
                    span: spans.get(&root).map(|span| (span.start, span.end)),
                })
                .nodes
                .push(request.object_hash);
        }
        Ok(by_root.into_values().collect())
    }

    /// Store requested nodes the server sent.
    fn receive_chunks(
        &mut self,
        nodes: Vec<WireChunk>,
        missing: Vec<WireChunkRef>,
    ) -> Result<(), Error> {
        for node in nodes {
            let request = node.request();
            // Only nodes this replica asked for, or that a parent it holds
            // names (a range pull sends them parents first), verified
            // against the hash it learned from the parent or descriptor.
            let client = self.client.as_mut().expect("client");
            if client.requested.remove(&request)
                || crate::large::has_parent(&self.conn, &request.object_hash)?
            {
                crate::large::install_fetched(&self.conn, &self.chunks, &request, node.bytes)?;
            }
        }
        let parked = self
            .sessions
            .iter()
            .any(|session| !session.parked_pulls.is_empty());
        let client = self.client.as_mut().expect("client");
        for missing in missing {
            let request = missing.request();
            client.requested.remove(&request);
            client.unavailable.insert(request);
        }
        client.arrived = true;
        // Pulls this relay parked for its sessions are answered now.
        client.relay_ready |= parked;
        Ok(())
    }

    /// The nodes a commit's large values need pushed first: (their
    /// hashes, the batches carrying them). Every node goes but those under
    /// the trees of the settled version each op names as its source
    /// (SPEC 19.4). `None` when this replica must first fetch nodes it
    /// lacks (a value copied from a row it holds only partly): they are
    /// asked for, and the commit waits.
    fn pushes_for(
        &mut self,
        commit: &SyncMessage,
    ) -> Result<Option<(Vec<jazz_types::large_values::ContentHash>, Vec<SyncMessage>)>, Error> {
        let SyncMessage::Commit { ops, sources, .. } = commit else {
            return Ok(Some((Vec::new(), Vec::new())));
        };
        fn large(value: &Value, out: &mut Vec<jazz_types::large_values::LargeValueRef>) {
            match value {
                Value::Large(value) => out.push((**value).clone()),
                Value::Nullable(Some(inner)) => large(inner, out),
                _ => {}
            }
        }
        let mut hashes = Vec::new();
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0;
        let mut absent = Vec::new();
        for (index, op) in ops.iter().enumerate() {
            let (row, cells) = match op {
                WireOp::Insert { row, cells, .. }
                | WireOp::Update { row, cells, .. }
                | WireOp::Upsert { row, cells, .. }
                | WireOp::Restore {
                    row,
                    cells: Some(cells),
                    ..
                } => (RowUuid::from_bytes(*row), cells),
                _ => continue,
            };
            let mut values = Vec::new();
            for value in cells.values() {
                large(value, &mut values);
            }
            if values.is_empty() {
                continue;
            }
            let mut named = BTreeSet::new();
            if let Some(source) = sources.iter().find(|source| source.op as usize == index)
                && let Ok(table) = self.schema.stored_table(op.table())
                && let Some(image) = self.held_version(table, row, &source.key, source.seq)?
            {
                named = self.large_roots(table, &image)?;
            }
            for value in &values {
                let nodes = match crate::large::to_push(&self.conn, &self.chunks, value, &named)? {
                    Ok(nodes) => nodes,
                    Err(missing) => {
                        absent.extend(missing);
                        continue;
                    }
                };
                for (request, bytes) in nodes {
                    hashes.push(jazz_types::large_values::ContentHash(request.object_hash));
                    size += bytes.len();
                    batch.push(WireChunk {
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
        }
        if !absent.is_empty() {
            let client = self.client.as_mut().expect("client");
            client.wanted.extend(absent);
            return Ok(None);
        }
        if !batch.is_empty() {
            batches.push(SyncMessage::PushChunks { nodes: batch });
        }
        Ok(Some((hashes, batches)))
    }

    /// Record the version each chunked value of a received image
    /// arrived in (`table` as the wire names it): pulls of its nodes cite
    /// it.
    /// Whether the image had any.
    fn record_vias(&self, table: &str, image: &Image) -> Result<bool, Error> {
        let via = crate::large::Via {
            table: table.to_owned(),
            id: image.id,
            key: image.key.clone(),
            seq: image.seq,
        };
        let roots = self.large_roots(&image.table, image)?;
        for root in &roots {
            crate::large::record_via(&self.conn, root, &via)?;
        }
        Ok(!roots.is_empty())
    }

    /// The roots of an image's chunked values.
    fn large_roots(&self, table: &str, image: &Image) -> Result<BTreeSet<[u8; 32]>, Error> {
        let mut roots = BTreeSet::new();
        for (name, cell) in &image.cells {
            if let Some(value) = self.large_of(table, name, cell)? {
                roots.insert(value.root.object_hash.0);
            }
        }
        Ok(roots)
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
        // Which version's names its ops use, should the store reopen under
        // another one before it settles.
        self.exec(PENDING_VERSIONS_DDL, []).map_err(storage)?;
        self.exec(
            "INSERT OR REPLACE INTO _jazz_pending_versions (tx, version) VALUES (?1, ?2)",
            rusqlite::params![tx as i64, self.owner_version.as_bytes().to_vec()],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// The seq a remote query's result was last settled through.
    fn watermark(&self, key: &[u8; 32]) -> Result<Option<u64>, Error> {
        let exists: bool = self
            .conn
            .cached_row(
                "SELECT count(*) FROM sqlite_master WHERE name = '_jazz_watermarks'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(storage)?
            > 0;
        if !exists {
            return Ok(None);
        }
        self.conn
            .cached_row(
                "SELECT through FROM _jazz_watermarks WHERE key = ?1",
                [key.as_slice()],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map(|through| through.map(|through| through as u64))
            .map_err(storage)
    }

    /// Forget every remote query's resume point: this store may no longer
    /// hold a result as it settled (a row it held was evicted, or is owed
    /// whole), so each opens fresh until it settles again. An eviction is
    /// rare enough that a fresh opening costs less than knowing which
    /// results held the row.
    fn forget_watermarks(&mut self) -> Result<(), Error> {
        if self.client.as_ref().is_some_and(|client| client.forgotten) {
            return Ok(());
        }
        if self.has_table("_jazz_watermarks")? {
            self.exec("DELETE FROM _jazz_watermarks", [])
                .map_err(storage)?;
        }
        if let Some(client) = self.client.as_mut() {
            for remote in client.remote.values_mut() {
                remote.through = None;
            }
            client.forgotten = true;
        }
        Ok(())
    }

    fn store_watermark(&self, key: &[u8; 32], through: u64) -> Result<(), Error> {
        self.exec(WATERMARKS_DDL, []).map_err(storage)?;
        self.exec(
            "INSERT OR REPLACE INTO _jazz_watermarks (key, through) VALUES (?1, ?2)",
            rusqlite::params![key.as_slice(), through as i64],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Move refused commit `tx`'s payload from the upload queue into the
    /// retry store.
    fn retain_rejected(&self, tx: u64, error: &Error, serializable: bool) -> Result<(), Error> {
        self.exec(REJECTED_DDL, []).map_err(storage)?;
        self.exec(
            "INSERT OR REPLACE INTO _jazz_rejected (tx, commit_msg, error, serializable) \
             SELECT tx, commit_msg, ?2, ?3 FROM _jazz_pending WHERE tx = ?1",
            rusqlite::params![tx as i64, encode(&wire_error(error)), serializable],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// The refused commits in the retry store, oldest first.
    pub(crate) fn rejected_transactions(&self) -> Result<Vec<RejectedTransaction>, Error> {
        if !self.has_rejected_store()? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT tx, commit_msg, error, serializable FROM _jazz_rejected ORDER BY tx",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, bool>(3)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        let decode = |e: postcard::Error| Error::new(ErrorCode::Storage, e.to_string());
        rows.into_iter()
            .map(|(seq, commit, error, serializable)| {
                let ops = match postcard::from_bytes::<SyncMessage>(&commit).map_err(decode)? {
                    SyncMessage::Commit { ops, .. } => ops,
                    _ => Vec::new(),
                };
                Ok(RejectedTransaction {
                    seq,
                    error: from_wire_error(postcard::from_bytes(&error).map_err(decode)?),
                    serializable,
                    ops,
                })
            })
            .collect()
    }

    /// Drop refused commit `tx` from the retry store.
    pub(crate) fn discard_rejected(&self, tx: u64) -> Result<(), Error> {
        if !self.has_rejected_store()? {
            return Ok(());
        }
        self.exec("DELETE FROM _jazz_rejected WHERE tx = ?1", [tx as i64])
            .map_err(storage)?;
        Ok(())
    }

    fn has_rejected_store(&self) -> Result<bool, Error> {
        self.conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_rejected'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)
    }

    fn forget_pending(&self, tx: u64) -> Result<(), Error> {
        self.exec("DELETE FROM _jazz_pending WHERE tx = ?1", [tx as i64])
            .map_err(storage)?;
        if self.has_table("_jazz_pending_versions")? {
            self.exec(
                "DELETE FROM _jazz_pending_versions WHERE tx = ?1",
                [tx as i64],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    /// The rows pending commits recorded in stored table `from`, which is
    /// stored as `to` now.
    pub(crate) fn rename_pending_rows(&self, from: &str, to: &str) -> Result<(), Error> {
        let stored: Vec<(i64, Vec<u8>)> = self
            .conn
            .prepare("SELECT tx, rows FROM _jazz_pending")
            .and_then(|mut statement| {
                statement
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()
            })
            .map_err(storage)?;
        for (tx, rows) in stored {
            let mut rows: Vec<(String, [u8; 16], Vec<u8>)> = postcard::from_bytes(&rows)
                .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
            if !rows.iter().any(|(table, ..)| table == from) {
                continue;
            }
            for row in &mut rows {
                if row.0 == from {
                    row.0 = to.to_owned();
                }
            }
            self.exec(
                "UPDATE _jazz_pending SET rows = ?2 WHERE tx = ?1",
                rusqlite::params![tx, encode(&rows)],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    fn has_table(&self, name: &str) -> Result<bool, Error> {
        let count: i64 = self
            .conn
            .cached_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [name],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(count > 0)
    }

    /// The schema version each pending commit was written under, where
    /// one was recorded (stores from before have none: the current one).
    fn pending_versions(&self) -> Result<HashMap<u64, crate::ids::SchemaVersionId>, Error> {
        if !self.has_table("_jazz_pending_versions")? {
            return Ok(HashMap::new());
        }
        let mut statement = self
            .conn
            .prepare("SELECT tx, version FROM _jazz_pending_versions")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))
            .map_err(storage)?;
        let mut versions = HashMap::new();
        for row in rows {
            let (tx, version) = row.map_err(storage)?;
            let version: [u8; 16] = version.try_into().map_err(|_| {
                Error::new(
                    ErrorCode::Storage,
                    "bad schema version in _jazz_pending_versions",
                )
            })?;
            versions.insert(tx as u64, crate::ids::SchemaVersionId::from_bytes(version));
        }
        Ok(versions)
    }

    pub(super) fn persist_meta(&self, key: &str, value: u64) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_meta (k, v) VALUES (?1, ?2)",
            rusqlite::params![key, encode(&value)],
        )
        .map_err(storage)?;
        Ok(())
    }

    pub(super) fn meta(&self, key: &str) -> Result<Option<u64>, Error> {
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
            // An unreadable fate is a damaged store, not an unseen commit:
            // treating it as unseen would authorize a resend afresh.
            Ok(bytes) => postcard::from_bytes(&bytes).map(Some).map_err(|error| {
                Error::new(
                    ErrorCode::Storage,
                    format!("undecodable recorded fate of commit {tx}: {error}"),
                )
            }),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// Whether `identity` may commit as `node`. A node belongs to the
    /// identity that first commits as it: commits are deduplicated by
    /// (node, tx), so another identity writing as that node could claim its
    /// transactions before it sends them.
    fn claim_node(&self, node: [u8; 16], identity: &AuthorSubject) -> Result<bool, Error> {
        let author = self.author_code(&author_key(identity))?;
        self.exec(
            "INSERT OR IGNORE INTO _jazz_nodes (node, author) VALUES (?1, ?2)",
            rusqlite::params![node.to_vec(), author],
        )
        .map_err(storage)?;
        let owner: i64 = self
            .conn
            .cached_row(
                "SELECT author FROM _jazz_nodes WHERE node = ?1",
                [node.to_vec()],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(owner == author)
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

    /// Where each of `own` (this replica's commits unsettled at a
    /// serializable snapshot) has since landed at a seq: a relay with no
    /// server settles its sessions' commits at its own local seqs.
    pub(crate) fn own_landings(&self, own: &[u64]) -> Result<HashMap<u64, u64>, Error> {
        let me = *self.identity.node.as_bytes();
        let mut landed = HashMap::new();
        for &tx in own {
            if let Some(Some(seq)) = self.settled(me, tx)? {
                landed.insert(tx, seq);
            }
        }
        Ok(landed)
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

    /// The `Catalogue` session `index` is owed: the admitted versions it
    /// keeps no names for, or other names for.
    fn catalogue_for(&mut self, index: usize) -> Option<SyncMessage> {
        if !std::mem::take(&mut self.sessions[index].catalogue_owed) {
            return None;
        }
        let mut versions = Vec::new();
        let mut sent = Vec::new();
        for (version, names) in self.names.in_order() {
            let digest = crate::names::names_digest(names);
            let bytes = *version.as_bytes();
            let holds = self.sessions[index].holds.as_ref()?;
            if holds.get(&bytes) == Some(&digest) {
                continue;
            }
            let jazz = if version == self.owner_version {
                &self.owner.jazz
            } else {
                match self.admitted.iter().find(|(id, _)| *id == version) {
                    Some((_, jazz)) => jazz,
                    // Named by an earlier process, not admitted by this
                    // one: its columns still travel in rows.
                    None => continue,
                }
            };
            versions.push(CatalogueVersion {
                schema: crate::protocol::SchemaVersion {
                    id: version,
                    schema: jazz.without_permissions(),
                },
                names: names
                    .tables
                    .iter()
                    .map(|(table, (stored, columns))| {
                        let columns = columns
                            .iter()
                            .map(|(column, stored)| (column.clone(), stored.clone()))
                            .collect();
                        (table.clone(), stored.clone(), columns)
                    })
                    .collect(),
            });
            sent.push((bytes, digest));
        }
        if versions.is_empty() {
            return None;
        }
        let cells = self
            .names
            .cells()
            .iter()
            .map(|cell| {
                let (source, default) = match &cell.rule {
                    crate::names::LensCellRule::Copy(source) => (Some(source.clone()), Cell::Null),
                    crate::names::LensCellRule::Dropped(default) => {
                        (None, Cell::from(default.clone()))
                    }
                };
                CatalogueCell {
                    table: cell.table.clone(),
                    column: cell.column.clone(),
                    since: *cell.since.as_bytes(),
                    source,
                    default,
                }
            })
            .collect();
        let holds = self.sessions[index].holds.get_or_insert_with(HashMap::new);
        holds.extend(sent);
        Some(SyncMessage::Catalogue { versions, cells })
    }

    /// How a subscription resuming from `since` opens (see [`Resume`]):
    /// `None` opens it fresh, shipping the whole result.
    ///
    /// Only an authority resumes: a relay's store may be cold, and its seqs
    /// are not the ones its sessions resume from. A windowed, flat-join,
    /// aggregate, relation or history read opens fresh, as does a session
    /// with its own claims (its policies read them, and they are not part
    /// of what the client resumes from), a `since` past what this store
    /// serves (a reset store), or one at or before the current schema's
    /// activation (its policies may read differently).
    fn resume_from(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        include_deleted: bool,
        support: &[Support],
        since: u64,
        claims: bool,
    ) -> Result<Option<Resume>, Error> {
        let query = &prepared.query;
        if self.client.is_some()
            || claims
            || since > self.served_through()
            || crate::db::is_windowed(query)
            || query.flat_join.is_some()
            || query.aggregate.is_some()
            || query.relation.is_some()
            || is_history_table(&query.table)
            || self.meta(ACTIVATED_META)?.is_some_and(|at| at >= since)
        {
            return Ok(None);
        }
        // Membership inputs: every table the read and its policies read
        // (its own included: a filter or policy may read other rows of
        // it), and the supporting tables with their policies' inputs.
        let mut inputs = self.read_dependencies(prepared, identity)?;
        let mut tables = Vec::new();
        support_tables(support, &mut tables);
        for table in tables {
            if let Some(author) = identity
                && self.owner.by_stored(&table).is_some()
            {
                inputs.extend(self.read_policy_tables(&table, author)?);
            }
            inputs.insert(table);
        }
        let mut moved = false;
        for table in &inputs {
            if self.owner.by_stored(table).is_none() {
                moved = true;
                break;
            }
            let max: Option<i64> = self
                .conn
                .cached_row(
                    &format!("SELECT MAX(_seq) FROM {}", quote(table)),
                    [],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            if max.is_some_and(|max| max as u64 > since) {
                moved = true;
                break;
            }
        }
        let prior = if moved {
            let read = ReadSpec {
                cut: Some(Cut::at(since)),
                include_deleted,
                ..ReadSpec::default()
            };
            Some(
                self.run(prepared, identity, &read)?
                    .iter()
                    .map(|row| row.id)
                    .collect(),
            )
        } else {
            None
        };
        Ok(Some(Resume { since, prior }))
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

    /// Reload pending commits and their history baselines after a restart.
    pub(crate) fn load_client(&mut self) -> Result<(), Error> {
        if self.meta("client")?.is_none() {
            return Ok(());
        }
        // A settled upload's rows now carry server seqs, so the stored
        // versions no longer show the local seqs handed out. The fates
        // recorded for this node do: the next commit continues after them,
        // or it would reuse a transaction id the authority dedupes away.
        let me = *self.identity.node.as_bytes();
        let settled: Option<i64> = self
            .conn
            .cached_row(
                "SELECT MAX(tx) FROM _jazz_settled WHERE node = ?1",
                [me.to_vec()],
                |r| r.get(0),
            )
            .map_err(storage)?;
        self.seq = self.seq.max(settled.unwrap_or(0) as u64);
        // So do pending commits, including ones that left no local rows (a
        // delete of a row this replica does not hold).
        let pending: Option<i64> = self
            .conn
            .cached_row("SELECT MAX(tx) FROM _jazz_pending", [], |r| r.get(0))
            .map_err(storage)?;
        self.seq = self.seq.max(pending.unwrap_or(0) as u64);
        let mut client = Client::new(me);
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
        let versions = self.pending_versions()?;
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
            // Written under an earlier version: it uploads under that one,
            // and replays here by the current names (see `rebase`).
            if let Some(version) = versions.get(&tx).filter(|v| **v != self.owner_version) {
                client.versions.insert(tx, *version.as_bytes());
            }
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
        let keys: BTreeSet<RowKey> = client
            .pending
            .iter()
            .flat_map(|pending| pending.rows.iter().cloned())
            .collect();
        for (table, row, key) in keys {
            let image = self.upstream_baseline(&table, row, &key)?;
            client.baselines.insert((table, row, key), image);
        }
        self.client = Some(client);
        Ok(())
    }
}

impl Client {
    /// Stored table `from` is stored as `to` now (a replica took its
    /// upstream's name for it).
    pub(crate) fn moved_table(&mut self, from: &str, to: &str) {
        self.pending.moved_table(from, to);
        self.baselines = std::mem::take(&mut self.baselines)
            .into_iter()
            .map(|((table, row, key), mut image)| {
                if let Some(image) = image.as_mut().filter(|image| image.table == from) {
                    image.table = to.to_owned();
                }
                let table = if table == from { to.to_owned() } else { table };
                ((table, row, key), image)
            })
            .collect();
        self.whole = std::mem::take(&mut self.whole)
            .into_iter()
            .map(|(table, id, key)| {
                let table = if table == from { to.to_owned() } else { table };
                (table, id, key)
            })
            .collect();
    }

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
            versions: HashMap::new(),
            transport: None,
            hint: Rc::new(StdCell::new(Link {
                hint: RemoteLinkHint::NoServer,
                since: Instant::now(),
                reported: false,
                detached: false,
            })),
            next_id: 1,
            next_advice: fresh_advice_base(),
            remote: HashMap::new(),
            remote_ids: HashMap::new(),
            outbox: Vec::new(),
            pending: PendingQueue::default(),
            unsent: Vec::new(),
            fates: HashMap::new(),
            local_fates: HashSet::new(),
            refused: Vec::new(),
            stored: HashSet::new(),
            baselines: HashMap::new(),
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
            whole: HashSet::new(),
            forgotten: false,
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
        // What the old session refused, the new one answers afresh.
        for remote in client.remote.values() {
            remote.refused.borrow_mut().take();
        }
        let resubscribe: Vec<SyncMessage> = client
            .remote
            .values_mut()
            .map(|r| SyncMessage::Read {
                id: r.id,
                query: Box::new(r.query.clone()),
                binding: r.binding.clone(),
                include_deleted: r.include_deleted,
                tier: r.reach.tier,
                wait: r.reach.wait,
                span: ReadSpan::Live { since: r.through },
            })
            .collect();
        // Commits, their chunk pushes and chunk requests a full queue held
        // back are sent again below from what is still owed.
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::Read {
                    span: ReadSpan::Live { .. },
                    ..
                } | SyncMessage::Schema { .. }
                    | SyncMessage::Holds { .. }
                    | SyncMessage::Hold { .. }
                    | SyncMessage::Commit { .. }
                    | SyncMessage::PushChunks { .. }
                    | SyncMessage::ChunkRequests { .. }
            )
        });
        let version = *inner.owner_version.as_bytes();
        // The versions whose names this store keeps: the server sends
        // those it stores otherwise, or it has and this store lacks.
        let versions = inner
            .names
            .in_order()
            .into_iter()
            .map(|(version, names)| (*version.as_bytes(), crate::names::names_digest(names)))
            .collect();
        let client = inner.client.as_mut().expect("client");
        client.outbox.insert(0, SyncMessage::Schema { version });
        client.outbox.insert(1, SyncMessage::Holds { versions });
        // What the old session could not answer, the new one may.
        client.unavailable.clear();
        client.outbox.extend(resubscribe);
        // Rows still owed whole were asked of the old session.
        if !client.whole.is_empty() {
            let rows = client
                .whole
                .iter()
                .map(|(table, id, key)| (table.clone(), *id, key.clone(), 0))
                .collect();
            client.outbox.push(SyncMessage::Hold { rows });
        }
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
            .retain(|m| !matches!(m, SyncMessage::Read { id, .. } if relayed.contains(id)));
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

    /// Evict the cached rows of a remote query's settled view that its
    /// answer, listing `listed`, left out: the server's result no longer
    /// holds them for this reader. The answer judges only the rows its read
    /// view holds: a branch view's answer, the rows on its head branch. A
    /// row a pending write touches waits for the server's own word on it.
    /// Each pass reads the view again, so rows that move up into a window
    /// are checked too.
    fn evict_unanswered(
        &self,
        query: Query,
        binding: BTreeMap<String, Value>,
        include_deleted: bool,
        view: &ReadViewSourceSpec,
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
        let opts = ReadOpts {
            read_view: crate::protocol::ReadViewSpec {
                source: view.clone(),
                include_deleted,
            },
            ..ReadOpts::default()
        };
        let mut read = ReadSpec::of(&opts, &self.inner.borrow())?;
        // A frozen view is this store's own state, not the server's to judge.
        if read.frozen() {
            return Ok(());
        }
        read.settled_only = true;
        read.cut = Some(crate::compile::Cut::at(through));
        let (table, head) = {
            let inner = self.inner.borrow();
            let schema = inner.schema.clone();
            let table = schema.table(&prepared.query.table)?;
            let head = match &read.view {
                Some(view) if !table.branch_by.is_empty() => Some(table.selector_key(&view.head)?),
                _ => None,
            };
            (table.stored.clone(), head)
        };
        let mut previous = Vec::new();
        loop {
            let inner = self.inner.borrow();
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
            let mut evict = Vec::new();
            for &row in &stale {
                let id = row.0.as_bytes().to_vec();
                for image in inner.images_where(&table, false, "id = ?1", &[&id])? {
                    let overlaid = inner.client.as_ref().is_some_and(|client| {
                        client
                            .baselines
                            .contains_key(&(table.clone(), row, image.key.clone()))
                    });
                    let in_view = head.as_ref().is_none_or(|head| *head == image.key);
                    if in_view && image.seq <= through && !overlaid {
                        evict.push((row, image.key));
                    }
                }
            }
            drop(inner);
            let mut inner = self.inner.borrow_mut();
            if !evict.is_empty() {
                inner.forget_watermarks()?;
            }
            for (row, key) in evict {
                inner.evict(&table, row, &key)?;
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
        inner.raise_to_local_base();
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

    /// Record a local commit's upload (client only); inside an open
    /// transaction the ops join it. It reads and writes the store, so
    /// it runs inside the commit's SQLite transaction: an error leaves the
    /// replica's sync state as it was, and the caller rolls the commit back.
    pub(crate) fn record_upload(
        &self,
        ops: Vec<StagedOp>,
        seq: u64,
        kind: Kind,
        relayed: Option<Relayed>,
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
        let mut baselines = Vec::new();
        // What this replica built each row it wrote on (SPEC 4 §4.6): its
        // newest pending commit to the row, else the settled image under
        // its pending writes.
        let mut bases = Vec::new();
        // The settled image of each written row, which its chunked values
        // go as deltas over (SPEC 19.8).
        let mut settled_images = HashMap::new();
        let by_row = inner.row_ops(&ops);
        for (table, row, key) in &rows {
            let row_key = (table.clone(), *row, key.clone());
            let client = inner.client.as_ref().expect("client");
            let image = match client.baselines.get(&row_key) {
                Some(image) => image.clone(),
                None => {
                    let image = inner.upstream_baseline(table, *row, key)?;
                    baselines.push((row_key.clone(), image.clone()));
                    image
                }
            };
            let settled = image.as_ref().map(|image| image.seq);
            if let Some(image) = image.filter(|image| !image.deleted) {
                settled_images.insert(row_key.clone(), image);
            }
            // This replica sends the commit: its predecessor goes by how
            // far back it is.
            let on = match client.pending.newest(&row_key) {
                Some(tx) => Some(BuiltOn::Own { back: seq - tx }),
                None => settled.map(BuiltOn::Seq),
            };
            if relayed.is_none()
                && let Some(on) = on
                && let Some(&op) = by_row
                    .get(&(table.clone(), *row))
                    .and_then(|ops| ops.first())
            {
                bases.push(WireBase {
                    op: op as u32,
                    key: key.clone(),
                    on,
                });
            }
        }
        let (bases, origin) = match relayed {
            Some(relayed) => (relayed.bases, Some(relayed.origin)),
            None => (bases, None),
        };
        let mut wire: Result<Vec<WireOp>, Error> = ops.iter().map(to_wire).collect();
        let (mut deltas, mut sources) = (Vec::new(), Vec::new());
        if let Ok(wire) = &mut wire {
            (deltas, sources) = inner.commit_deltas(wire, &rows, &settled_images)?;
        }
        // Each written row as this replica now holds it, so the authority
        // can skip echoing a version that is exactly this.
        let mut expected = Vec::new();
        let mut expect = Vec::new();
        for (table, row, key) in &rows {
            let Some(image) = inner.current_image(table, *row, key)? else {
                continue;
            };
            expect.push(Expect {
                id: image.id,
                key: image.key.clone(),
                digest: echo_digest(&image),
            });
            expected.push(image);
        }
        let client = inner.client.as_mut().expect("client");
        let added: Vec<RowKey> = baselines.iter().map(|(key, _)| key.clone()).collect();
        client.baselines.extend(baselines);
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
                    deltas,
                    sources,
                    bases,
                    origin,
                };
                client.unsent.push(commit.clone());
                client.pending.push(Pending {
                    tx: seq,
                    ops,
                    rows: rows.clone(),
                    expected,
                });
                let persisted = inner.persist_pending(seq, &commit, &rows);
                if let Err(error) = persisted {
                    let client = inner.client.as_mut().expect("client");
                    client.unsent.pop();
                    client.pending.pop();
                    for key in &added {
                        client.baselines.remove(key);
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
            client.pending.remove(tx);
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
        let author = inner.identity.author;
        let watermark = watermark_key(&query, &prepared.binding, include_deleted, author);
        // A replica that cannot read its watermark opens the query fresh.
        let through = inner.watermark(&watermark).ok().flatten();
        let client = inner.client.as_mut()?;
        client.outbox.push(SyncMessage::Read {
            id,
            query: Box::new(query.clone().into_owned()),
            binding: prepared.binding.clone(),
            include_deleted,
            tier: reach.tier,
            wait: reach.wait,
            span: ReadSpan::Live { since: through },
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
                through,
                watermark,
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
        // Held on a live link: leaving it (even for a new attempt) releases
        // the read, as it releases a gated opening.
        let live = self.inner.borrow().client.as_ref().is_some_and(|client| {
            let link = client.hint.get();
            link.hint == RemoteLinkHint::Live && may_wait(link)
        });
        // A non-durable foreground's relay answers a local read from its
        // store whatever its own path to the server: the read waits for it
        // while connected to it.
        let answerable = |inner: &Inner| {
            let client = inner.client.as_ref().expect("client");
            let link = client.hint.get();
            if inner.non_durable && tier != ReadTier::Remote {
                return client.transport.is_some();
            }
            may_wait(link) && !(live && link.hint != RemoteLinkHint::Live)
        };
        {
            let inner = self.inner.borrow();
            if inner.client.is_none() || !strict && !answerable(&inner) {
                return Ok(None);
            }
        }
        let message = |id: u64| SyncMessage::Read {
            id,
            query: Box::new(remote_query(&prepared.query).into_owned()),
            binding: prepared.binding.clone(),
            include_deleted,
            tier,
            wait: reach.wait,
            span: ReadSpan::Once {
                at,
                view: Box::new(view.clone()),
            },
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
            self.evict_unanswered(
                prepared.query.clone(),
                prepared.binding.clone(),
                include_deleted,
                view,
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

    /// How many local commits await the upstream's verdict: committed here
    /// (recovered ones included) and not yet accepted or refused by the
    /// server. Zero on a database that has never had an upstream.
    pub fn pending_upload_count(&self) -> usize {
        self.inner
            .borrow()
            .client
            .as_ref()
            .map_or(0, |client| client.pending.len())
    }

    /// Whether chunks this replica requested may still arrive: it is
    /// connected to an upstream that may answer, with requests outstanding
    /// (sent, or recorded by a read for the next send). A non-durable
    /// foreground's relay answers from its store whatever its own path to
    /// the server. Chunks the upstream answered missing are not waited for
    /// again.
    pub fn awaiting_chunks(&self) -> bool {
        let inner = self.inner.borrow();
        // Offloaded nodes the next tick fetches from the chunk store.
        if inner.chunks.has_fetch() {
            return true;
        }
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
        let id = client.next_advice;
        client.next_advice += 1;
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

    /// Resolve an outstanding request as `Unknown`: its waiter is removed,
    /// a request not yet sent is never sent, and a late answer is ignored.
    pub fn cancel_permission_advice_request(&self, id: u64) {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return;
        };
        if client.advice.remove(&id).is_some() {
            client
                .outbox
                .retain(|m| !matches!(m, SyncMessage::Advise { id: asked, .. } if *asked == id));
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
        if begin_savepoint(&inner.conn, "advise").is_err() {
            return PermissionAdvice::Unknown;
        }
        let mark = inner.changed.borrow().len();
        let applied = inner.apply_as(&op, who, seq).map(drop);
        inner.changed.borrow_mut().truncate(mark);
        let undone = end_savepoint(&inner.conn, "advise", false);
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
    /// pending transaction settled or an overlaid row changed, then send.
    fn client_tick(&self) -> Result<(), Error> {
        if self.inner.borrow().client.is_none() {
            return Ok(());
        }
        // Everything one tick applies commits together.
        begin_savepoint(&self.inner.borrow().conn, "sync_tick")?;
        let applied = self.client_apply();
        end_savepoint(&self.inner.borrow().conn, "sync_tick", applied.is_ok())?;
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
            inbound.append(&mut client.refused);
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
            // What the upstream sent about rows reaches this store's own
            // subscribers on the serving pass that follows.
            if queue.iter().any(|message| {
                matches!(
                    message,
                    SyncMessage::Rows { .. }
                        | SyncMessage::Evict { .. }
                        | SyncMessage::Settled { .. }
                )
            }) && !inner.sessions.is_empty()
            {
                inner.client.as_mut().expect("client").relay_ready = true;
            }
            // Rows install without their chunks: a chunked cell carries the
            // sort key its indexes need.
            while let Some(message) = queue.pop_front() {
                match message {
                    // Rows and evictions name stored tables and columns, as
                    // this store mirrors its upstream's.
                    SyncMessage::Rows { images, origins } => {
                        // A history version is listed once its transaction
                        // is known.
                        for (seq, node, tx) in origins {
                            inner.record_settled(node, tx, Some(seq))?;
                        }
                        for image in images {
                            let Some(mut image) = inner.stored_image(image) else {
                                continue;
                            };
                            let named = (image.table.clone(), image.id, image.key.clone());
                            if !inner.receive_deltas(&mut image)? {
                                inner.ask_whole(named)?;
                                continue;
                            }
                            inner.client.as_mut().expect("client").whole.remove(&named);
                            if inner.record_vias(&named.0, &image)? {
                                // A node no version held may be held by
                                // this one: missing answers are transient.
                                inner.client.as_mut().expect("client").unavailable.clear();
                            }
                            let overlaid = inner.receive_image(image)?;
                            rebase |= overlaid;
                        }
                    }
                    SyncMessage::Evict { table, id, key } => {
                        if !inner.physical.tables.contains_key(&table) {
                            continue;
                        }
                        rebase |= inner.receive_eviction(&table, RowUuid::from_bytes(id), &key)?;
                        // Results settled before this one may have held
                        // the row: a resume from them would count it as
                        // held, though this store no longer has it.
                        inner.forget_watermarks()?;
                        let client = inner.client.as_mut().expect("client");
                        client.whole.remove(&(table, id, key));
                    }
                    SyncMessage::Catalogue { versions, cells } => {
                        match inner.mirror_catalogue(versions, cells) {
                            Ok(()) => rebase = true,
                            // This store cannot store rows as its upstream
                            // does: it stops syncing with it, nothing that
                            // follows applies, and its remote reads fail
                            // with why. Local reads are served as before.
                            Err(error) => {
                                inner.refuse_upstream(error);
                                queue.clear();
                            }
                        }
                    }
                    SyncMessage::Settled {
                        id,
                        through,
                        listing,
                        global,
                    } => {
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        // A one-shot read's answer.
                        if let Some(slot) = client.answers.get_mut(&id) {
                            *slot = Some(Ok((through, listing.unwrap_or_default())));
                            client.relay_ready |=
                                client.relayed_fetches.iter().any(|f| f.upstream == id);
                            continue;
                        }
                        let Some(key) = client.remote_ids.get(&id).cloned() else {
                            continue;
                        };
                        let remote = client.remote.get_mut(&key).expect("remote");
                        remote.answered.set(true);
                        remote.global.set(global);
                        // A row owed whole is stale here: this store does
                        // not hold the result as of `through`.
                        let resumable = client.whole.is_empty();
                        if resumable {
                            remote.through = Some(through);
                            client.forgotten = false;
                        }
                        let watermark = remote.watermark;
                        // Cached rows the server's result lacks are stale.
                        if let Some(ids) = listing {
                            answered_remotes.push((
                                remote.query.clone(),
                                remote.binding.clone(),
                                remote.include_deleted,
                                ids,
                                through,
                            ));
                        }
                        // In the tick's savepoint, with the rows it settles.
                        if resumable {
                            inner.store_watermark(&watermark, through)?;
                        }
                    }
                    SyncMessage::Advice { id, advice } => {
                        let client = inner.client.as_mut().expect("client");
                        // Late answers to cancelled requests are dropped.
                        if let Some(slot @ None) = client.advice.get_mut(&id) {
                            *slot = Some(advice);
                        }
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
                            inner.install_held(tx, through, &held)?;
                        }
                        let client = inner.client.as_mut().expect("client");
                        client.known = client.known.max(through);
                        let serializable = client.sent.iter().any(|m| {
                            matches!(m, SyncMessage::Commit { tx: sent, kind: WireKind::Serializable { .. }, .. } if *sent == tx)
                        });
                        client.pending.remove(tx);
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
                            // The originating store keeps its own refused
                            // payload, for a host that reports refusals,
                            // until the host has (INV-TX-9). A relay keeps
                            // none of its sessions'.
                            if inner.upstream_rejections.is_some() && !relayed && origins.is_empty()
                            {
                                inner.retain_rejected(tx, error, serializable)?;
                            }
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
                        if let Some(hashes) = client.pushed.remove(&tx) {
                            crate::large::mark_pushed(&inner.conn, &hashes)?;
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
        }
        // A settled answer lists its result's rows: cached rows the
        // replica's view of that result holds and the server's lacks lost
        // their listing (INV-SYNC-14), as rows a commit took away do. A
        // resumed opening lists its result too.
        for (query, binding, include_deleted, listed, through) in answered_remotes {
            self.evict_unanswered(
                query,
                binding,
                include_deleted,
                &ReadViewSourceSpec::Current,
                &listed,
                through,
            )?;
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
        let mut out = std::mem::take(&mut client.outbox);
        // Commits staged since the last send go ahead of the reads asked
        // since (and what was queued after them, in order). The server
        // applies a session's messages in arrival order, so a read sees the
        // writer's own earlier writes however its ticks are sliced.
        let first_read = out
            .iter()
            .position(|m| matches!(m, SyncMessage::Read { .. }))
            .unwrap_or(out.len());
        let reads = out.split_off(first_read);
        let wanted: Vec<ChunkRequest> = std::mem::take(&mut client.wanted)
            .into_iter()
            .filter(|r| !client.requested.contains(r) && !client.unavailable.contains(r))
            .collect();
        client.requested.extend(wanted.iter().cloned());
        let spans = inner.chunks.take_spans();
        let pulls = inner.pulls(wanted, &spans)?;
        for batch in pulls.chunks(64) {
            out.push(SyncMessage::ChunkRequests {
                pulls: batch.to_vec(),
            });
        }
        let client = inner.client.as_mut().expect("client");
        let mut commits = std::mem::take(&mut client.unsent);
        // Each commit's large-value nodes go ahead of it. A commit whose
        // nodes must be fetched first waits, and so do those after it.
        let mut pushes = Vec::with_capacity(commits.len());
        for commit in &commits {
            match inner.pushes_for(commit)? {
                Some(push) => pushes.push(push),
                None => break,
            }
        }
        let current = *inner.owner_version.as_bytes();
        let waiting = commits.split_off(pushes.len());
        let trusted = inner.identity.author == AuthorSubject::SYSTEM;
        let client = inner.client.as_mut().expect("client");
        client.unsent = waiting;
        // An untrusted session's server joins no message over its budget
        // and would end the session on every resend: a commit that large is
        // refused here, for good, as a rejected fate.
        if !trusted {
            let limit = jazz_protocol::protocol_limits::MAX_UNTRUSTED_MESSAGE_BYTES;
            let mut kept = Vec::with_capacity(commits.len());
            for (commit, push) in commits.into_iter().zip(pushes) {
                let size = postcard::serialize_with_flavor(
                    &commit,
                    postcard::ser_flavors::Size::default(),
                )
                .unwrap_or(usize::MAX);
                match &commit {
                    SyncMessage::Commit { tx, .. } if size > limit => {
                        client.refused.push(SyncMessage::Fate {
                            tx: *tx,
                            through: client.known,
                            result: Err(WireError::Rejected {
                                reason: None,
                                message: format!(
                                    "a commit of {size} bytes is over the {limit} bytes one \
                                     message may carry: not supported yet"
                                ),
                            }),
                            held: Vec::new(),
                            global: false,
                        });
                    }
                    _ => kept.push((commit, push)),
                }
            }
            (commits, pushes) = kept.into_iter().unzip();
        }
        let transport = client.transport.as_mut().expect("transport");
        for commit in &commits {
            if !client.sent.iter().any(|m| matches!((m, commit), (SyncMessage::Commit { tx: a, .. }, SyncMessage::Commit { tx: b, .. }) if a == b)) {
                client.sent.push(commit.clone());
            }
        }
        // A commit written under an earlier version is sent under it: the
        // server reads its ops by that version's names. An authority keeps
        // admitting every version it published (a server replays its
        // catalogue on start), so the server holds the session's later
        // messages behind such a switch only for a version never published
        // (a draft), as it does for a session declaring one.
        let mut declared = current;
        for (commit, (hashes, batches)) in commits.into_iter().zip(pushes) {
            if let SyncMessage::Commit { tx, .. } = &commit {
                if !hashes.is_empty() {
                    client.pushed.insert(*tx, hashes);
                }
                let version = client.versions.get(tx).copied().unwrap_or(current);
                if version != declared {
                    out.push(SyncMessage::Schema { version });
                    declared = version;
                }
            }
            out.extend(batches);
            out.push(commit);
        }
        if declared != current {
            out.push(SyncMessage::Schema { version: current });
        }
        out.extend(reads);
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
        // With nothing new to send, the rest of a split message still goes
        // out as the queue drains.
        match transport.flush() {
            Ok(()) | Err(TransportError::Backpressure) => Ok(()),
            Err(e) => Err(Error::new(ErrorCode::Storage, format!("{e:?}"))),
        }
    }

    /// Push a settled marker to subscriptions whose remote answered without
    /// changing their rows.
    fn flush_settled(&self) {
        self.inner.borrow_mut().fan.flush_settled();
    }

    /// Restore every overlaid row to its upstream baseline and replay the
    /// pending transactions, in order, over them.
    fn rebase(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let Some(client) = inner.client.as_mut() else {
            return Ok(());
        };
        let baseline = std::mem::take(&mut client.baselines);
        let versions = client.versions.clone();
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
        // A commit written under an earlier version replays by the names
        // the current one gives what it wrote, as they stand now (an
        // upstream's catalogue may have renamed them since).
        let pending: Vec<(u64, Vec<StagedOp>)> = pending
            .into_iter()
            .map(|(tx, ops)| match versions.get(&tx) {
                Some(version) => {
                    let version = crate::ids::SchemaVersionId::from_bytes(*version);
                    (tx, inner.by_current_names(version, ops))
                }
                None => (tx, ops),
            })
            .collect();
        for ((table, row, key), image) in &baseline {
            match image {
                Some(image) => {
                    inner.drop_row(table, *row, key, image.seq)?;
                    inner.put_image(image, false)?;
                }
                None => inner.drop_row(table, *row, key, 0)?,
            }
        }
        for (tx, ops) in pending {
            begin_savepoint(&inner.conn, "replay")?;
            inner.merging = true;
            let replayed = ops.iter().try_for_each(|op| inner.apply(op, tx).map(drop));
            inner.merging = false;
            end_savepoint(&inner.conn, "replay", replayed.is_ok())?;
        }
        let client = inner.client.as_mut().expect("client");
        client.baselines = baseline
            .into_iter()
            .filter(|(row, _)| keep.contains(row))
            .collect();
        Ok(())
    }
}

impl Inner {
    /// Where this replica's optimistic local seqs start.
    fn local_base(&self) -> u64 {
        if self.non_durable {
            NON_DURABLE_BASE
        } else {
            LOCAL_BASE
        }
    }

    /// Hand out local seqs from this replica's base on. The jump hands out
    /// no seq, so the high water stays where it was until a commit.
    pub(crate) fn raise_to_local_base(&mut self) {
        let base = self.local_base();
        if self.seq < base {
            let from = match self.base_jump {
                Some((from, to)) if self.seq == to => from,
                _ => self.seq,
            };
            self.base_jump = Some((from, base));
            self.seq = base;
        }
    }

    /// The upstream sent what this store cannot follow (a catalogue it
    /// cannot mirror): drop the link, and fail every remote read with
    /// `error` until the next connection answers it afresh.
    fn refuse_upstream(&mut self, error: Error) {
        self.detach_upstream();
        if let Some(client) = self.client.as_mut() {
            for remote in client.remote.values() {
                *remote.refused.borrow_mut() = Some(error.clone());
            }
        }
    }

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
    /// The pulls a relay parked for session `index` whose nodes it now
    /// holds, or that its upstream cannot answer: they are answered again.
    fn parked_pulls_ready(&mut self, index: usize) -> Result<Option<Vec<WirePull>>, Error> {
        let parked = std::mem::take(&mut self.sessions[index].parked_pulls);
        if parked.is_empty() {
            return Ok(None);
        }
        let connected = self
            .client
            .as_ref()
            .is_some_and(|client| client.transport.is_some());
        let mut ready = Vec::new();
        let mut kept = Vec::new();
        for pull in parked {
            let mut settled = true;
            for hash in &pull.nodes {
                let unavailable = self.client.as_ref().is_some_and(|client| {
                    client
                        .unavailable
                        .contains(&ChunkRequest { object_hash: *hash })
                });
                if connected && !unavailable && crate::large::node(&self.conn, hash)?.is_none() {
                    settled = false;
                    break;
                }
            }
            if settled {
                ready.push(pull)
            } else {
                kept.push(pull)
            }
        }
        self.sessions[index].parked_pulls = kept;
        Ok((!ready.is_empty()).then_some(ready))
    }

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
            let SyncMessage::Read { id, .. } = &fetch.message else {
                continue;
            };
            match answer {
                Some(Err(error)) => refused.push(SyncMessage::Refused {
                    id: *id,
                    code: error.code,
                    message: error.message,
                }),
                Some(Ok((through, listed))) => {
                    if let SyncMessage::Read {
                        query,
                        binding,
                        include_deleted,
                        span: ReadSpan::Once { at: None, view },
                        ..
                    } = &fetch.ask
                    {
                        answers.push((
                            (**query).clone(),
                            binding.clone(),
                            *include_deleted,
                            (**view).clone(),
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

    /// `image`'s deltas made whole over the versions they name (SPEC
    /// 19.8). False when this replica holds no such version (it evicted
    /// the row, INV-SYNC-14) or not the value a delta was made over.
    fn receive_deltas(&self, image: &mut Image) -> Result<bool, Error> {
        let row = RowUuid::from_bytes(image.id);
        for (name, cell) in &mut image.cells {
            let Cell::Delta(delta) = cell else {
                continue;
            };
            let Some(base) = self.held_version(&image.table, row, &image.key, delta.seq)? else {
                return Ok(false);
            };
            let Some((_, old)) = base.cells.iter().find(|(n, _)| n == name) else {
                return Ok(false);
            };
            let Some(old) = self.large_of(&image.table, name, old)? else {
                return Ok(false);
            };
            let Some(value) = large_delta::apply(&old, delta) else {
                return Ok(false);
            };
            *cell = Cell::Large(
                jazz_types::large_values::encode_large_value_ref(&value)
                    .map_err(|e| Error::new(ErrorCode::Storage, format!("large value: {e}")))?,
            );
        }
        Ok(true)
    }

    /// The version of a row at `seq` this replica holds settled: the
    /// upstream baseline under its pending writes, or a stored one.
    fn held_version(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<Option<Image>, Error> {
        let client = self.client.as_ref().expect("client");
        if let Some(Some(image)) = client.baselines.get(&(table.to_owned(), row, key.to_vec()))
            && image.seq == seq
        {
            return Ok(Some(image.clone()));
        }
        self.version_image(table, row, key, seq)
    }

    /// A commit's chunked values that extend the same column's value in
    /// their row's settled image, as deltas over it: each op's cell holds
    /// NULL in its place (SPEC 19.8). The upstream holds every settled
    /// version, as it shipped them. The rest travel as trees, named with
    /// that settled image as their source (SPEC 19.4).
    fn commit_deltas(
        &self,
        ops: &mut [WireOp],
        rows: &[RowKey],
        settled: &HashMap<RowKey, Image>,
    ) -> Result<(Vec<CellDelta>, Vec<WireSource>), Error> {
        let mut deltas = Vec::new();
        let mut sources = Vec::new();
        for (index, op) in ops.iter_mut().enumerate() {
            let Ok(table) = self.schema.table(op.table()) else {
                continue;
            };
            let (row, cells) = match op {
                WireOp::Insert { row, cells, .. }
                | WireOp::Update { row, cells, .. }
                | WireOp::Upsert { row, cells, .. }
                | WireOp::Restore {
                    row,
                    cells: Some(cells),
                    ..
                } => (RowUuid::from_bytes(*row), cells),
                _ => continue,
            };
            // An op over several branches (a view's) has no one base.
            let mut keys = rows
                .iter()
                .filter(|(stored, written, _)| *stored == table.stored && *written == row);
            let (Some(row_key), None) = (keys.next(), keys.next()) else {
                continue;
            };
            let Some(base) = settled.get(row_key) else {
                continue;
            };
            let mut trees = false;
            for (column, cell) in cells.iter_mut() {
                let (value, depth) = large_delta::unwrap(cell);
                let Value::Large(large) = value else {
                    continue;
                };
                let stored = table.stored_name(column);
                let old = match base.cells.iter().find(|(name, _)| name == stored) {
                    Some((_, old)) => self.large_of(&table.stored, stored, old)?,
                    None => None,
                };
                let delta = old.and_then(|old| large_delta::extend(base.seq, &old, large));
                if let Some(delta) = delta {
                    *value = Value::Nullable(None);
                    deltas.push(CellDelta {
                        op: index as u32,
                        column: column.clone(),
                        depth,
                        key: row_key.2.clone(),
                        delta,
                    });
                } else {
                    trees = true;
                }
            }
            if trees {
                sources.push(WireSource {
                    op: index as u32,
                    key: row_key.2.clone(),
                    seq: base.seq,
                });
            }
        }
        Ok((deltas, sources))
    }

    /// Ask the upstream for a row whole: a `Hold` of it, which it answers
    /// with the row whole or its eviction (SPEC 19.8). Until it arrives
    /// this store holds a stale version, so every watermark is forgotten
    /// and none is stored (see `Client::whole`).
    fn ask_whole(&mut self, (table, id, key): (String, [u8; 16], Vec<u8>)) -> Result<(), Error> {
        self.forget_watermarks()?;
        let client = self.client.as_mut().expect("client");
        client.whole.insert((table.clone(), id, key.clone()));
        let asked = client.outbox.iter().any(|message| {
            matches!(message, SyncMessage::Hold { rows } if rows.iter().any(|held| {
                held.0 == table && held.1 == id && held.2 == key && held.3 == 0
            }))
        });
        if !asked {
            client.outbox.push(SyncMessage::Hold {
                rows: vec![(table, id, key, 0)],
            });
        }
        Ok(())
    }

    /// Apply one server image; returns whether a rebase is needed (the row
    /// is overlaid by a pending write).
    fn receive_image(&mut self, image: Image) -> Result<bool, Error> {
        // Two tiers number local seqs: relays from `LOCAL_BASE`, the
        // non-durable foregrounds behind them from `NON_DURABLE_BASE`. An
        // upstream version in a foreground's own range would come from a
        // third tier (a foreground serving sessions), whose seqs could meet
        // this replica's own.
        if self.non_durable && image.seq >= NON_DURABLE_BASE {
            return Err(unsupported(
                "a non-durable client's upstream relaying versions numbered in that \
                 client's own local range (only a durable store relays to it)",
            ));
        }
        let row = RowUuid::from_bytes(image.id);
        let client = self.client.as_mut().expect("client");
        let overlaid = (image.table.clone(), row, image.key.clone());
        if let Some(baseline) = client.baselines.get_mut(&overlaid) {
            match baseline {
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
                None => *baseline = Some(image),
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
        // A third tier of local seqs would meet a foreground's own (see
        // `NON_DURABLE_BASE`); its sessions refuse what it relays.
        debug_assert!(
            !inner.non_durable,
            "a non-durable client serves no sessions"
        );
        inner.accepted += 1;
        let generation = inner.accepted;
        let mut session = Session::new(transport, identity);
        session.claims = claims;
        session.generation = generation;
        let slot = match inner.sessions.iter().position(|session| !session.open) {
            Some(slot) => {
                inner.sessions[slot] = session;
                slot
            }
            None => {
                inner.sessions.push(session);
                inner.sessions.len() - 1
            }
        };
        SubscriberId { slot, generation }
    }

    /// The handle of the session in `slot`.
    fn subscriber_at(&self, slot: usize) -> SubscriberId {
        SubscriberId {
            slot,
            generation: self.inner.borrow().sessions[slot].generation,
        }
    }

    /// The client went away: drop its subscriptions, watches and pending
    /// replies now. A reconnect is a new subscriber that resumes from its
    /// last settled seq.
    pub fn disconnect_subscriber(&self, subscriber: SubscriberId) {
        let mut inner = self.inner.borrow_mut();
        let dropped = inner.fan.dropped.clone();
        if let Some(session) = inner
            .sessions
            .get_mut(subscriber.slot)
            .filter(|session| session.open && session.generation == subscriber.generation)
        {
            for (query, binding, include_deleted) in session.close(&dropped) {
                inner.release_remote(&query, &binding, include_deleted, RemoteReach::REMOTE);
            }
            inner.forget_relayed(subscriber.slot);
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

    /// Seed a row as settled server state, written by `made_by`.
    pub fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: &str,
        row: RowUuid,
        made_by: AuthorSubject,
        cells: BTreeMap<String, Value>,
    ) -> Result<u64, Error> {
        let handle = block_on(self.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row),
                identity: WriteIdentity::Attribution(made_by),
                ..Default::default()
            },
        ))?;
        Ok(handle.seq)
    }

    /// Sessions the server ended since the last call because of what they
    /// sent (a message it cannot decode or serve), with why. The host
    /// closes their links; each is already disconnected.
    pub fn take_closed_subscribers(&self) -> Vec<(SubscriberId, Error)> {
        std::mem::take(&mut self.inner.borrow_mut().closed)
    }

    /// Serve one session's inbound messages for this tick.
    fn serve_session(&self, index: usize) -> Result<(), Error> {
        let (inbound, failed): (Vec<SyncMessage>, bool) = {
            let mut inner = self.inner.borrow_mut();
            let session = &mut inner.sessions[index];
            if !session.open {
                return Ok(());
            }
            // The rest of a split message a full queue held back goes out
            // as it drains, ahead of anything this tick sends.
            if let Err(TransportError::Failed(error)) = session.transport.flush() {
                return Err(Error::new(ErrorCode::Protocol, error));
            }
            let mut inbound = std::mem::take(&mut session.backlog);
            inbound.extend(std::iter::from_fn(|| session.transport.try_recv()));
            let failed = session.transport.take_failure().is_some();
            (inbound.into(), failed)
        };
        // A client that sent what this server cannot decode is
        // disconnected; nothing it sent is served.
        if failed {
            return Err(Error::new(
                ErrorCode::Protocol,
                "sync message the server cannot decode",
            ));
        }
        let mut inbound: VecDeque<SyncMessage> = inbound.into();
        self.serve_due_first_loads(index)?;
        // Relayed reads the upstream answered (or can no longer answer)
        // are served from this store first, in the order they came.
        let (relayed, answers) = self.inner.borrow_mut().relayed_fetches_ready(index);
        // The upstream's answers list their results' rows: cached rows
        // they left out are not served as the reader's (INV-SYNC-14).
        for (query, binding, include_deleted, view, listed, through) in answers {
            self.evict_unanswered(query, binding, include_deleted, &view, &listed, through)?;
        }
        for message in relayed.into_iter().rev() {
            inbound.push_front(message);
        }
        if let Some(pulls) = self.inner.borrow_mut().parked_pulls_ready(index)? {
            inbound.push_front(SyncMessage::ChunkRequests { pulls });
        }
        // Pulls an earlier answer deferred, once what it sent has drained.
        {
            let mut inner = self.inner.borrow_mut();
            let session = &mut inner.sessions[index];
            if session.outbox.is_empty() && !session.deferred_pulls.is_empty() {
                let pulls = std::mem::take(&mut session.deferred_pulls);
                inbound.push_front(SyncMessage::ChunkRequests { pulls });
            }
        }
        // Writes held back by the ingress limit, and what the session sent
        // after them, in order. A read is never answered ahead of a commit
        // sent before it that it could observe (INV-SYNC-49); a read that
        // depends on no table the held commits write, and an unsubscribe
        // or advice request, overtakes them, so a client uploading a large
        // value can still read everything else.
        let mut held: VecDeque<SyncMessage> = VecDeque::new();
        let mut barrier = HeldWrites::default();
        while let Some(message) = inbound.pop_front() {
            if !held.is_empty() && !self.overtakes(index, &barrier, &message) {
                self.note_held(index, &mut barrier, &message);
                held.push_back(message);
                continue;
            }
            if let SyncMessage::Read { id, .. } = &message {
                self.inner.borrow_mut().sessions[index]
                    .backlog_reads
                    .1
                    .remove(id);
            }
            if let SyncMessage::PushChunks { nodes } = &message {
                let bytes: usize = nodes.iter().map(|node| node.bytes.len()).sum();
                let mut inner = self.inner.borrow_mut();
                let policy = inner.staging;
                let session = &mut inner.sessions[index];
                if session.ingress.0.elapsed() >= std::time::Duration::from_millis(policy.window_ms)
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
            if !matches!(
                message,
                SyncMessage::Schema { .. } | SyncMessage::Holds { .. }
            ) {
                let inner = self.inner.borrow_mut();
                if inner.session_unadmitted(index) {
                    inbound.push_front(message);
                    held.extend(inbound);
                    break;
                }
            }
            // A read the server cannot serve is refused to the client,
            // which would otherwise wait on it.
            let read = match &message {
                SyncMessage::Read { id, .. } => Some(*id),
                _ => None,
            };
            let fetch = matches!(
                message,
                SyncMessage::Read {
                    span: ReadSpan::Once { .. },
                    ..
                }
            )
            .then(|| message.clone());
            // A message served while the chunk store fetches the nodes it
            // needs is served again once they arrive.
            let again = (fetch.is_none() && self.inner.borrow().chunks.store().is_some())
                .then(|| message.clone());
            match (self.serve(index, message), read) {
                (Ok(()), _) => {}
                (Err(error), _)
                    if crate::large::is_missing(&error)
                        && again.is_some()
                        && self.inner.borrow().chunks.has_fetch() =>
                {
                    held.push_back(again.expect("a copy"));
                }
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
                    let fetch = fetch.expect("a fetch");
                    self.note_held(index, &mut barrier, &fetch);
                    held.push_back(fetch);
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
        Ok(())
    }

    /// Whether `message` may be served ahead of what session `index` has
    /// held: an unsubscribe of a read not itself held, an advice request
    /// (advisory, it changes nothing), or a read that depends on no table
    /// the held commits write.
    fn overtakes(&self, index: usize, barrier: &HeldWrites, message: &SyncMessage) -> bool {
        match message {
            SyncMessage::Unsubscribe { id } => !barrier.reads.contains(id),
            SyncMessage::Advise { .. } => true,
            // A read frozen at a snapshot may name the held commits
            // themselves, which the server cannot resolve until they apply.
            SyncMessage::Read {
                span: ReadSpan::Once { view, .. },
                ..
            } if frozen_at(view).is_some() => barrier.tables.is_some() && !barrier.commits,
            SyncMessage::Read { .. } => {
                let Some(written) = &barrier.tables else {
                    return false;
                };
                self.read_dependencies(index, message)
                    .is_some_and(|reads| reads.is_disjoint(written))
            }
            _ => false,
        }
    }

    /// Record what a message held for session `index` may change.
    fn note_held(&self, index: usize, barrier: &mut HeldWrites, message: &SyncMessage) {
        match message {
            SyncMessage::PushChunks { .. } | SyncMessage::ChunkRequests { .. } => {}
            SyncMessage::Read { id, .. } => {
                barrier.reads.insert(*id);
            }
            SyncMessage::Unsubscribe { .. } | SyncMessage::Advise { .. } => {}
            SyncMessage::Commit { ops, .. } => {
                barrier.commits = true;
                let schema = {
                    let mut inner = self.inner.borrow_mut();
                    inner
                        .session_schema(index)
                        .unwrap_or_else(|| inner.owner.clone())
                };
                for op in ops {
                    let stored = schema.stored_table(op.table()).map(str::to_owned);
                    match (&mut barrier.tables, stored) {
                        (Some(tables), Ok(table)) => {
                            tables.insert(table);
                        }
                        (tables, _) => *tables = None,
                    }
                }
            }
            // Anything else (a schema change, say) may change what every
            // later read sees: nothing overtakes it.
            _ => barrier.tables = None,
        }
    }

    /// The stored tables a read session `index` sent depends on, read as
    /// that session (its policies and claims); `None` when they cannot be
    /// told, so the read is not let ahead of anything.
    fn read_dependencies(&self, index: usize, message: &SyncMessage) -> Option<BTreeSet<String>> {
        let SyncMessage::Read {
            id, query, binding, ..
        } = message
        else {
            return None;
        };
        {
            let mut inner = self.inner.borrow_mut();
            let owner = inner.owner.clone();
            let cache = &mut inner.sessions[index].backlog_reads;
            if !cache.0.as_ref().is_some_and(|at| Rc::ptr_eq(at, &owner)) {
                *cache = (Some(owner), HashMap::new());
            }
            if let Some(known) = cache.1.get(id) {
                return known.clone();
            }
        }
        let (identity, policy, claims) = {
            let inner = self.inner.borrow();
            let session = &inner.sessions[index];
            (
                session.identity,
                inner.session_policy(index),
                session.claims.clone(),
            )
        };
        let vouched = claims
            .filter(|_| identity != AuthorSubject::SYSTEM)
            .map(|claims| (identity, claims));
        let tables = self.with_claims(vouched, || {
            let prepared = self
                .session_db(index)
                .prepare_query_bound(query, binding.clone())
                .ok()?;
            self.inner
                .borrow()
                .read_dependencies(&prepared, policy)
                .ok()
        });
        // An unadmitted version may be admitted without the owner schema
        // changing: what it resolved to then is not kept.
        let mut inner = self.inner.borrow_mut();
        if !inner.session_unadmitted(index) {
            inner.sessions[index]
                .backlog_reads
                .1
                .insert(*id, tables.clone());
        }
        tables
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
            // What one session sent that the server cannot serve (or a
            // storage failure serving it) ends that session alone: the
            // others are still served and fanned out this tick.
            if let Err(error) = self.serve_session(index) {
                let subscriber = self.subscriber_at(index);
                self.disconnect_subscriber(subscriber);
                self.inner.borrow_mut().closed.push((subscriber, error));
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
                let tables: Vec<String> = inner.sessions[index].held.tables().cloned().collect();
                for table in tables {
                    if !policy_tables.contains_key(&table) {
                        let deps = inner.read_policy_tables(&table, identity)?;
                        policy_tables.insert(table.clone(), deps);
                    }
                    let Some(held) = inner.sessions[index].held.of(&table) else {
                        continue;
                    };
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
            let committed = std::mem::take(&mut inner.sessions[index].committed);
            let mut echoes = std::mem::take(&mut inner.sessions[index].echoes);
            let mut watch = std::mem::take(&mut inner.sessions[index].watch);
            let mut held_rows = std::mem::take(&mut inner.sessions[index].held);
            for (table, row, key) in recheck {
                let can = inner.current_image(&table, row, &key)?.is_some()
                    && readable(&inner, &table, row, &key, policy)?;
                if held_rows.recheck(&table, row, &key, can) {
                    touched.insert((table, row, key));
                }
            }
            for ((table, row, key), keys) in &written_keys {
                if keys.iter().any(|k| watch.contains(k)) {
                    touched.insert((table.clone(), *row, key.clone()));
                    // A row that joins a watched correlation (a new child)
                    // is watched for whoever watches the correlation.
                    watch.share(
                        &keys[1..],
                        wk((
                            table.clone(),
                            "id".to_owned(),
                            watch_key(&Sql::Blob(row.0.as_bytes().to_vec())),
                        )),
                    );
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
                let resume = inner.sessions[index].resume.remove(&id);
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
                // Rows a resumed client already holds, as of the seq it
                // resumes from: not shipped, but watched again, and their
                // supporting rows followed (and shipped where they changed).
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
                        // Unchanged for a resumed client: a version it held as
                        // of `since` that was in the result then. A row whose
                        // membership inputs changed may have entered the
                        // result without changing: its seq alone cannot say,
                        // so the result as of `since` does.
                        let unchanged = resume.as_ref().is_some_and(|resume| {
                            history.is_none()
                                && row.seq <= resume.since
                                && resume
                                    .prior
                                    .as_ref()
                                    .is_none_or(|prior| prior.contains(&row.id))
                        });
                        let id_key = wk((
                            table.clone(),
                            "id".to_owned(),
                            watch_key(&Sql::Blob(row.id.0.as_bytes().to_vec())),
                        ));
                        watch.hold(id, id_key);
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
                        let rid = gone.row_uuid.0.as_bytes().to_vec();
                        let table = names.stored_table(
                            history_source_table(&gone.table).unwrap_or(&gone.table),
                        )?;
                        // It ships once more as it is now (or its eviction),
                        // and this subscription no longer covers it.
                        watch.release_key(
                            id,
                            wk((
                                table.to_owned(),
                                "id".to_owned(),
                                watch_key(&Sql::Blob(rid.clone())),
                            )),
                        );
                        for image in inner.images_where(table, false, "id = ?1", &[&rid])? {
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
                for image in &images[first..] {
                    watch.hold(
                        id,
                        wk((
                            image.table.clone(),
                            "id".to_owned(),
                            watch_key(&Sql::Blob(image.id.to_vec())),
                        )),
                    );
                }
                // Rows the client held in the result as of `since` that are
                // in it no more, though they need not have changed (their
                // membership inputs did): each ships as it is now, or its
                // eviction.
                if let (Some(prior), Some(group)) = (
                    resume.as_ref().and_then(|resume| resume.prior.as_ref()),
                    group.upgrade(),
                ) {
                    let group = group.borrow();
                    let table = group.root().to_owned();
                    let now: HashSet<RowUuid> = group.rows.iter().map(|row| row.id).collect();
                    for row in prior.iter().filter(|row| !now.contains(row)) {
                        let rid = row.0.as_bytes().to_vec();
                        for image in inner.images_where(&table, false, "id = ?1", &[&rid])? {
                            touched.insert((table.clone(), *row, image.key));
                        }
                    }
                }
                if let Some(support) = support {
                    let shipped: Vec<Image> = images[first..].to_vec();
                    let floor = resume.as_ref().map(|resume| Floor {
                        since: resume.since,
                        diff: resume.prior.is_some(),
                    });
                    let mut extra = Vec::new();
                    let mut gone = Vec::new();
                    let mut followed = HashSet::new();
                    let mut follow = Follow {
                        owner: id,
                        identity: policy,
                        out: &mut extra,
                        watch: &mut watch,
                        held: &mut held_rows,
                        gone: &mut gone,
                        followed: &mut followed,
                    };
                    // A shipped row may point at rows the client never had:
                    // its support is followed whole. A held one's changed
                    // since the floor.
                    for image in &shipped {
                        inner.follow_support(&support, image, None, &mut follow)?;
                    }
                    for image in &held {
                        inner.follow_support(&support, image, floor, &mut follow)?;
                    }
                    images.extend(extra);
                    touched.extend(gone);
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
                        listing: window,
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
                    _ => evictions.push(SyncMessage::Evict {
                        table,
                        id: *row.0.as_bytes(),
                        key,
                    }),
                }
            }
            // Rows the session's own commits wrote stay current for the
            // session, in a subscription or not: its pending writes sit on
            // them (see the fetch above).
            for (table, row, ..) in &committed {
                watch.hold(SESSION_OWNER, id_watch(table, *row.0.as_bytes()));
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
            // An eviction is the row's state as of now, after every event
            // drained above: a version of it a result shipped earlier in the
            // tick is stale. Shipped, it would also reach the writer as a
            // held row of its commit's Fate, which goes after the evictions
            // and would reinstate the row (INV-SYNC-14).
            let evicted: HashSet<(&str, [u8; 16], &[u8])> = evictions
                .iter()
                .filter_map(|eviction| match eviction {
                    SyncMessage::Evict { table, id, key } => {
                        Some((table.as_str(), *id, key.as_slice()))
                    }
                    _ => None,
                })
                .collect();
            if !evicted.is_empty() {
                images.retain(|image| {
                    !evicted.contains(&(image.table.as_str(), image.id, image.key.as_slice()))
                });
            }
            for eviction in &evictions {
                if let SyncMessage::Evict { table, id, .. } = eviction {
                    watch.forget(wk((
                        table.clone(),
                        "id".to_owned(),
                        watch_key(&Sql::Blob(id.to_vec())),
                    )));
                }
            }
            let mut out = Vec::new();
            // Rows travel by stored names: the versions they are stored by
            // reach the client first.
            if let Some(catalogue) = inner.catalogue_for(index) {
                out.push(catalogue);
            }
            let origins = if history_seqs.is_empty() {
                Vec::new()
            } else {
                inner.origins(&history_seqs)?
            };
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
                    let same = echo_digest(image) == echo.digest;
                    if same {
                        held.entry(echo.tx)
                            .or_default()
                            .push(Held::of(echo.index, image));
                        hold_shipped(&mut session.shipped, image);
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
            out.extend(rows_messages(images, origins, ROWS_MESSAGE_BYTES));
            out.extend(evictions);
            out.extend(std::mem::take(&mut session.outbox));
            out.extend(settled);
            // A relay's versions of the session's own commits it still
            // uploads are the session's pending writes, which it holds as
            // it wrote them: shipped at the relay's seqs, they would stand
            // in for them as another writer's. The session gets the version
            // they sit on instead, the base its pending writes replay over.
            let own: HashSet<u64> = inner.client.as_ref().map_or_else(HashSet::new, |client| {
                client
                    .relayed_fates
                    .iter()
                    .filter(|(_, sessions)| sessions.iter().any(|(s, _, _)| *s == index))
                    .map(|(seq, _)| *seq)
                    .collect()
            });
            if !own.is_empty() {
                let mut shipped = HashSet::new();
                for message in &mut out {
                    let SyncMessage::Rows { images, .. } = message else {
                        continue;
                    };
                    let mut bases = Vec::new();
                    for image in images.iter().filter(|image| own.contains(&image.seq)) {
                        if !shipped.insert((image.table.clone(), image.id, image.key.clone())) {
                            continue;
                        }
                        let id = image.id.to_vec();
                        let base = inner
                            .images_where(
                                &image.table,
                                true,
                                "id = ?1 AND _b = ?2 AND _seq < ?3 ORDER BY _seq DESC",
                                &[&id, &image.key, &(image.seq as i64)],
                            )?
                            .into_iter()
                            .find(|base| !own.contains(&base.seq));
                        // Read access is the base version's own: the
                        // session's pending write may be what lets it read
                        // the row now.
                        if let Some(base) = base {
                            let row = RowUuid::from_bytes(base.id);
                            if readable_version(
                                &inner,
                                &base.table,
                                row,
                                &base.key,
                                base.seq,
                                policy,
                            )? {
                                bases.push(base);
                            }
                        }
                    }
                    images.retain(|image| !own.contains(&image.seq));
                    for base in bases {
                        if !images.iter().any(|image| {
                            image.table == base.table
                                && image.id == base.id
                                && image.key == base.key
                                && image.seq == base.seq
                        }) {
                            images.push(base);
                        }
                    }
                }
                out.retain(|message| {
                    !matches!(message, SyncMessage::Rows { images, origins }
                            if images.is_empty() && origins.is_empty())
                });
            }
            // Chunked cells go as deltas over the version of their row the
            // replica holds (SPEC 19.8).
            let mut shipped = std::mem::take(&mut inner.sessions[index].shipped);
            for message in &mut out {
                match message {
                    SyncMessage::Rows { images, .. } => {
                        for image in images.iter_mut() {
                            inner.ship_deltas(&mut shipped, image)?;
                        }
                    }
                    SyncMessage::Evict { table, id, key } => {
                        shipped.remove(&(table.clone(), *id, key.clone()));
                    }
                    _ => {}
                }
            }
            let session = &mut inner.sessions[index];
            session.shipped = shipped;
            for message in out {
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
    /// wait ran out, or whose upstream can no longer answer (its link
    /// failed), before the upstream answered: from now on each is this
    /// store's result, settled at once, as when the upstream cannot answer.
    /// The upstream subscription it keeps fills the store when it answers.
    fn serve_due_first_loads(&self, index: usize) -> Result<(), Error> {
        let now = Instant::now();
        let due: Vec<(u64, FirstLoadWait)> = {
            let mut inner = self.inner.borrow_mut();
            let unanswerable = inner
                .client
                .as_ref()
                .is_none_or(|client| !may_wait(client.hint.get()));
            let session = &mut inner.sessions[index];
            let ids: Vec<u64> = session
                .first_load
                .iter()
                .filter(|(_, wait)| unanswerable || wait.until <= now)
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
            SyncMessage::Read {
                id,
                query,
                binding,
                include_deleted,
                tier,
                wait,
                span: ReadSpan::Live { since },
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
                if let Some(since) = since {
                    let claims = inner.sessions[index].claims.is_some();
                    let resume = inner.resume_from(
                        &prepared,
                        policy,
                        include_deleted,
                        &support,
                        since,
                        claims,
                    )?;
                    if let Some(resume) = resume {
                        inner.sessions[index].resume.insert(id, resume);
                    }
                }
                inner.sessions[index].support.insert(id, support);
            }
            // Rows the replica could not apply a delta to: each ships
            // whole (or as an eviction), as the replica lacks the version
            // the delta was made over.
            SyncMessage::Hold { rows } => {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                for (table, id, key, _) in rows {
                    session.shipped.remove(&(table.clone(), id, key.clone()));
                    session
                        .touched
                        .insert((table, RowUuid::from_bytes(id), key));
                }
            }
            SyncMessage::Holds { versions } => {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                session.holds = Some(versions.into_iter().collect());
                session.catalogue_owed = true;
            }
            SyncMessage::Unsubscribe { id } => {
                let mut inner = self.inner.borrow_mut();
                inner.sessions[index].support.remove(&id);
                inner.sessions[index].first_load.remove(&id);
                inner.sessions[index].resume.remove(&id);
                // Rows only this subscription covered go cold: the client
                // keeps them, unwatched, until a subscription covers them
                // again and its opening brings them up to date.
                inner.sessions[index].watch.release(id);
                inner.sessions[index].held.release(id);
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
            SyncMessage::Read {
                id,
                query,
                binding,
                include_deleted,
                tier,
                wait,
                span: ReadSpan::Once { at, view },
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
                    client.outbox.push(SyncMessage::Read {
                        id: upstream,
                        query: Box::new(remote_query(&prepared.query).into_owned()),
                        binding: prepared.binding.clone(),
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        span: ReadSpan::Once {
                            at,
                            view: view.clone(),
                        },
                    });
                }
                if let Some(strict) = relay {
                    let prepared = db.prepare_query_bound(&query, binding.clone())?;
                    let mut inner = self.inner.borrow_mut();
                    let client = inner.client.as_mut().expect("client");
                    let upstream = client.next_id;
                    client.next_id += 1;
                    client.answers.insert(upstream, None);
                    let ask = SyncMessage::Read {
                        id: upstream,
                        query: Box::new(remote_query(&prepared.query).into_owned()),
                        binding: prepared.binding.clone(),
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        span: ReadSpan::Once {
                            at,
                            view: view.clone(),
                        },
                    };
                    client.outbox.push(ask.clone());
                    client.relayed_fetches.push(RelayedFetch {
                        upstream,
                        session: index,
                        ask,
                        strict,
                        until: wait.and_then(wait_until),
                        message: SyncMessage::Read {
                            id,
                            query,
                            binding,
                            include_deleted,
                            tier,
                            wait,
                            span: ReadSpan::Once { at, view },
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
                        source: *view,
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
                    view: viewed.view,
                    splices: Vec::new(),
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
                // A fetched row stays current for the rest of the session,
                // as rows the session's own commits touch do: they back its
                // open transactions and pending writes. Unlike a
                // subscription's, they are not resumed after a reconnect.
                let support = inner.support_of(&query)?;
                let mut extra = Vec::new();
                let mut watch = std::mem::take(&mut inner.sessions[index].watch);
                let mut held = std::mem::take(&mut inner.sessions[index].held);
                if policy.is_some() && at.is_none() && history.is_none() {
                    for image in images.iter().filter(|image| image.table == table) {
                        let row = RowUuid::from_bytes(image.id);
                        held.keep(SESSION_OWNER, &table, row, &image.key, true);
                    }
                }
                let (mut gone, mut followed) = (Vec::new(), HashSet::new());
                let mut follow = Follow {
                    owner: SESSION_OWNER,
                    identity: policy,
                    out: &mut extra,
                    watch: &mut watch,
                    held: &mut held,
                    gone: &mut gone,
                    followed: &mut followed,
                };
                for image in &images {
                    inner.follow_support(&support, image, None, &mut follow)?;
                }
                images.extend(extra);
                for image in &images {
                    watch.hold(SESSION_OWNER, id_watch(&image.table, image.id));
                }
                let session = &mut inner.sessions[index];
                session.watch = watch;
                session.held = held;
                let mut origins = inner.origins(&seqs)?;
                origins.extend(named);
                let through = at.unwrap_or(inner.served_through());
                let authority = inner.client.is_none();
                let session = &mut inner.sessions[index];
                session
                    .outbox
                    .extend(rows_messages(images, origins, ROWS_MESSAGE_BYTES));
                session.outbox.push(SyncMessage::Settled {
                    id,
                    through,
                    listing: Some(rows.iter().map(|r| *r.id.0.as_bytes()).collect()),
                    // A strict read at a relay is served once its upstream
                    // answered, from the settled rows alone.
                    global: authority || strict,
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
                deltas,
                sources,
                bases,
                origin,
            } => {
                let identity = self.inner.borrow().sessions[index].identity;
                if !self.inner.borrow().claim_node(node, &identity)? {
                    let mut inner = self.inner.borrow_mut();
                    let through = inner.served_through();
                    inner.sessions[index].outbox.push(SyncMessage::Fate {
                        tx,
                        through,
                        result: Err(WireError::Rejected {
                            reason: None,
                            message: "commit names a node another identity writes as".to_owned(),
                        }),
                        held: Vec::new(),
                        global: true,
                    });
                    return Ok(());
                }
                // The session commit a relay's carries, when the link may
                // write as its node (a trusted link, or one whose identity
                // writes as that node too); otherwise the sender's own. The
                // bases are in the sender's terms either way.
                let writer = match origin {
                    Some(origin)
                        if policy.is_none()
                            || self.inner.borrow().claim_node(origin.0, &identity)? =>
                    {
                        origin
                    }
                    _ => (node, tx),
                };
                // A resend after a reconnect gets the recorded fate.
                let known = self.inner.borrow().applied_fate(node, tx)?;
                let (result, rows) = match known {
                    Some(result) => (result, Vec::new()),
                    None => match commit_who(policy, author.as_deref(), session.is_some())
                        .map_err(|error| (error, Vec::new()))
                        .and_then(|who| {
                            let (ops, mut roots) = db
                                .resolve_deltas(policy, ops, deltas)
                                .map_err(|error| (error, Vec::new()))?;
                            roots.extend(
                                db.source_roots(policy, &ops, &sources)
                                    .map_err(|error| (error, Vec::new()))?,
                            );
                            let possession = crate::large::Possession {
                                author: receipt_author(&identity),
                                checked: policy.is_some(),
                                roots,
                                settle: Default::default(),
                            };
                            let vouched = vouched(policy, who, session);
                            self.with_claims(vouched, || {
                                db.authorize_commit(
                                    who,
                                    policy,
                                    kind,
                                    ops,
                                    (node, tx),
                                    (bases, writer),
                                    possession,
                                )
                            })
                        }) {
                        // The accepted commit recorded its fate atomically.
                        Ok(rows) => (Ok(()), rows),
                        // Its large values need nodes the chunk store is
                        // still fetching: it is applied again once they
                        // arrive, not refused.
                        Err((error, _))
                            if crate::large::is_missing(&error)
                                && self.inner.borrow().chunks.has_fetch() =>
                        {
                            return Err(error);
                        }
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
            SyncMessage::ChunkRequests { pulls } => {
                let (replies, parked, deferred) = db.answer_pulls(policy, pulls)?;
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                session.outbox.extend(replies);
                session.parked_pulls.extend(parked);
                session.deferred_pulls.extend(deferred);
            }
            SyncMessage::PushChunks { nodes } => {
                let inner = self.inner.borrow();
                let max_age = i64::try_from(inner.staging.max_age_ms).unwrap_or(i64::MAX);
                let expires = now_ms().saturating_add(max_age);
                // A relay pushes what its sessions push on to its upstream,
                // ahead of the commits that name it.
                let relay = inner.client.is_some();
                let author = receipt_author(&inner.sessions[index].identity);
                in_savepoint(&inner.conn, "jazz_lv_push", || {
                    for node in nodes {
                        // A node that fails authentication is dropped; a
                        // commit that needs it is then rejected for missing
                        // chunks.
                        let request = node.request();
                        let received = crate::large::receive_pushed(
                            &inner.conn,
                            &inner.chunks,
                            &request,
                            node.bytes,
                            expires,
                            &author,
                        );
                        if relay && received.is_ok() {
                            crate::large::mark_unpushed(
                                &inner.conn,
                                &jazz_types::large_values::ContentHash(request.object_hash),
                            )?;
                        }
                    }
                    Ok(())
                })?;
            }
            other => return Err(unsupported(format!("server received {other:?}"))),
        }
        Ok(())
    }

    /// Validate (serializable) and apply one uploaded commit as `identity`.
    /// Returns the rows the client must hear about either way: the rows it
    /// wrote, so a rejected client rebases onto the authority's versions.
    #[allow(clippy::type_complexity)]
    /// A commit's deltas made whole over the versions they name (SPEC
    /// 19.8), each only where `identity` may read that version: naming a
    /// version must not copy what the writer cannot read into its write.
    fn resolve_deltas(
        &self,
        identity: Option<AuthorSubject>,
        mut ops: Vec<WireOp>,
        deltas: Vec<CellDelta>,
    ) -> Result<(Vec<WireOp>, BTreeSet<[u8; 32]>), Error> {
        let mut roots = BTreeSet::new();
        let unresolved = || {
            Error::new(
                ErrorCode::WriteRejected,
                "a large-value delta names no version of its row the writer may read",
            )
        };
        let inner = self.inner();
        for delta in deltas {
            let op = ops.get_mut(delta.op as usize).ok_or_else(unresolved)?;
            let table = inner.schema.table(op.table())?;
            let stored = table.stored_name(&delta.column).to_owned();
            let (row, cells) = match op {
                WireOp::Insert { row, cells, .. }
                | WireOp::Update { row, cells, .. }
                | WireOp::Upsert { row, cells, .. }
                | WireOp::Restore {
                    row,
                    cells: Some(cells),
                    ..
                } => (RowUuid::from_bytes(*row), cells),
                _ => return Err(unresolved()),
            };
            let seq = delta.delta.seq;
            if !still_readable_version(&inner, &table.stored, row, &delta.key, seq, identity)? {
                return Err(unresolved());
            }
            let base = inner
                .version_image(&table.stored, row, &delta.key, seq)?
                .ok_or_else(unresolved)?;
            let (_, old) = base
                .cells
                .iter()
                .find(|(name, _)| *name == stored)
                .ok_or_else(unresolved)?;
            let old = inner
                .large_of(&table.stored, &stored, old)?
                .ok_or_else(unresolved)?;
            let value = large_delta::apply(&old, &delta.delta).ok_or_else(unresolved)?;
            roots.insert(value.root.object_hash.0);
            let cell = cells.get_mut(&delta.column).ok_or_else(unresolved)?;
            *cell = large_delta::wrap(Value::Large(Box::new(value)), delta.depth);
        }
        Ok((ops, roots))
    }

    /// Answer pulls (SPEC 19.8): each only where `identity` may read the
    /// version it cites and that version holds its root, and only with
    /// nodes of that tree. Everything else comes back missing, so naming
    /// a hash never reads another row's content. A node only in an
    /// asynchronous chunk store fails the whole batch as missing before
    /// anything is sent: it is answered again once its nodes are fetched.
    ///
    /// One answer carries at most about [`ANSWER_PULL_BYTES`], each node
    /// once: the pulls (and wanted nodes) past it come back deferred, to
    /// be answered once the session's outbox has drained.
    fn answer_pulls(
        &self,
        identity: Option<AuthorSubject>,
        pulls: Vec<WirePull>,
    ) -> Result<(Vec<SyncMessage>, Vec<WirePull>, Vec<WirePull>), Error> {
        let inner = self.inner();
        let mut replies = Vec::new();
        let mut parked = Vec::new();
        let mut deferred = Vec::new();
        let mut nodes = Vec::new();
        let mut missing = Vec::new();
        let mut size = 0;
        let mut answered = 0;
        let mut sent = std::collections::HashSet::new();
        let mut fetching = false;
        for pull in pulls {
            if answered >= ANSWER_PULL_BYTES {
                deferred.push(pull);
                continue;
            }
            let row = RowUuid::from_bytes(pull.id);
            // Pulls name the version's table as it is stored, as rows do.
            let image = match inner
                .owner
                .by_stored(&pull.table)
                .map(|t| t.stored.as_str())
            {
                Some(table)
                    if still_readable_version(
                        &inner, table, row, &pull.key, pull.seq, identity,
                    )? =>
                {
                    inner
                        .version_image(table, row, &pull.key, pull.seq)?
                        .map(|image| (table, image))
                }
                _ => None,
            };
            let holds = match &image {
                Some((table, image)) => inner.large_roots(table, image)?.contains(&pull.root),
                None => false,
            };
            if !holds {
                missing.extend(pull.nodes.into_iter().map(|hash| WireChunkRef { hash }));
                continue;
            }
            let span = pull.span.map(|(start, end)| start..end);
            let found = crate::large::pulled(
                &inner.conn,
                &inner.chunks,
                &pull.root,
                &pull.nodes,
                span.clone(),
                PULL_BYTES.min(ANSWER_PULL_BYTES - answered),
                &mut sent,
            );
            let (found, foreign, absent, rest) = match found {
                Err(error) if crate::large::is_missing(&error) => {
                    fetching = true;
                    continue;
                }
                found => found?,
            };
            missing.extend(foreign.into_iter().map(|hash| WireChunkRef { hash }));
            if !rest.is_empty() {
                deferred.push(WirePull {
                    nodes: rest,
                    span: None,
                    ..pull.clone()
                });
            }
            // A relay fetches what it lacks from its upstream, through the
            // version it received, and answers once it arrives; what its
            // upstream cannot answer is missing here too.
            let (fetch, absent): (Vec<_>, Vec<_>) = match &inner.client {
                Some(client) if client.transport.is_some() => {
                    absent.into_iter().partition(|hash| {
                        !client
                            .unavailable
                            .contains(&ChunkRequest { object_hash: *hash })
                    })
                }
                _ => (Vec::new(), absent),
            };
            missing.extend(absent.into_iter().map(|hash| WireChunkRef { hash }));
            if !fetch.is_empty() {
                inner.chunks.add_demand(
                    fetch.iter().map(|hash| ChunkRequest { object_hash: *hash }),
                    span.clone().map(|span| (pull.root, span)),
                );
                parked.push(WirePull {
                    nodes: fetch,
                    ..pull.clone()
                });
            }
            for (hash, bytes) in found {
                size += bytes.len();
                answered += bytes.len();
                nodes.push(WireChunk { hash, bytes });
                if size >= CHUNK_BATCH_BYTES {
                    replies.push(SyncMessage::Chunks {
                        nodes: std::mem::take(&mut nodes),
                        missing: Vec::new(),
                    });
                    size = 0;
                }
            }
        }
        if fetching {
            return Err(crate::large::missing());
        }
        replies.push(SyncMessage::Chunks { nodes, missing });
        Ok((replies, parked, deferred))
    }

    /// The roots of the chunked values of the settled versions a commit
    /// names as its sources, each where `identity` may read it: the trees
    /// the writer holds without pushing them (SPEC 19.4). A source it may
    /// not read, or this store lacks, counts for nothing.
    fn source_roots(
        &self,
        identity: Option<AuthorSubject>,
        ops: &[WireOp],
        sources: &[WireSource],
    ) -> Result<BTreeSet<[u8; 32]>, Error> {
        let inner = self.inner();
        let mut roots = BTreeSet::new();
        for source in sources {
            let Some(op) = ops.get(source.op as usize) else {
                continue;
            };
            let (Ok(table), Some(row)) = (inner.schema.stored_table(op.table()), wire_row(op))
            else {
                continue;
            };
            if !still_readable_version(&inner, table, row, &source.key, source.seq, identity)? {
                continue;
            }
            if let Some(image) = inner.version_image(table, row, &source.key, source.seq)? {
                roots.extend(inner.large_roots(table, &image)?);
            }
        }
        Ok(roots)
    }

    #[allow(clippy::too_many_arguments)]
    fn authorize_commit(
        &self,
        who: Who,
        identity: Option<AuthorSubject>,
        mut kind: WireKind,
        ops: Vec<WireOp>,
        origin: ([u8; 16], u64),
        (bases, writer): (Vec<WireBase>, ([u8; 16], u64)),
        possession: crate::large::Possession,
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
        // What each row was built on (SPEC 4 §4.6), before anything is
        // applied: a base on a rejected commit rejects the upload. A relay
        // renames the bases for its upstream; the authority resolves them.
        let by_row = self.inner().row_ops(&ops);
        let (bases, mut built) = {
            let inner = self.inner();
            let resolved = if relay {
                inner
                    .relay_bases(origin, &ops, bases)
                    .map(|bases| (bases, HashMap::new()))
            } else {
                inner
                    .resolve_bases(origin, &ops, &bases)
                    .map(|built| (Vec::new(), built))
            };
            match resolved {
                Ok(resolved) => resolved,
                Err(error) => return Err((error, mentioned(&inner))),
            }
        };
        let (seq, mark) = {
            let mut inner = self.inner();
            let seq = inner.seq + 1;
            begin_savepoint(&inner.conn, "upload").map_err(|e| (e, Vec::new()))?;
            let mark = inner.changed.borrow().len();
            // The unit's write checks run at its end, grounded (SPEC 7).
            inner.defer_checks();
            // The unit was made over the sender's image (SPEC 4, INV-HIST-8);
            // text merges read what each row was built on.
            inner.merging = true;
            inner.possession = Some(possession);
            inner.merge_bases = std::mem::take(&mut built);
            inner.text_in.borrow_mut().clear();
            let applied = ops
                .iter()
                .try_for_each(|op| inner.apply_as(op, who, seq).map(drop))
                .and_then(|()| inner.finish_checks(seq))
                .and_then(|()| inner.settle_possession());
            inner.possession = None;
            inner.merging = false;
            built = std::mem::take(&mut inner.merge_bases);
            if let Err(error) = applied {
                inner.abandon_checks();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                let rows = mentioned(&inner);
                return Err((error, rows));
            }
            let recorded = inner
                .record_fate(origin.0, origin.1, &Ok(()))
                .and_then(|()| inner.record_settled(origin.0, origin.1, Some(seq)));
            if let Err(error) = recorded {
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                return Err((error, Vec::new()));
            }
            inner.seq = seq;
            (seq, mark)
        };
        // The relay's own upload record and the rows the sender hears about
        // are read before the commit is released, so a storage error
        // rejects it whole instead of leaving it applied but not relayed.
        // The authority keeps what each version was built on (INV-HIST-21);
        // a relay passes the bases on instead.
        let finished = if relay {
            self.record_upload(
                ops.clone(),
                seq,
                upload,
                Some(Relayed {
                    origin: writer,
                    bases,
                }),
            )
        } else {
            Ok(())
        }
        .and_then(|()| self.inner.borrow().written_rows(&ops, seq))
        .and_then(|rows| {
            if !relay {
                let inner = self.inner.borrow();
                inner.record_bases(&ops, &by_row, &built, seq, &rows)?;
                inner.record_text_in(seq, origin.0, &built, &rows)?;
            }
            Ok(rows)
        });
        let mut inner = self.inner();
        match finished {
            Ok(rows) => {
                end_savepoint(&inner.conn, "upload", true).map_err(|e| (e, Vec::new()))?;
                drop(inner);
                self.notify();
                Ok(rows)
            }
            Err(error) => {
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                inner.seq = seq - 1;
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

/// Where a client's permission advice ids start: random, below 2^51 so the
/// ids stay exact as JavaScript numbers, and never 0 (the id of advice
/// asked without a client).
fn fresh_advice_base() -> u64 {
    let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
    (high >> 13) | 1
}

/// [`readable`] for the version of the row at `seq`.
fn readable_version(
    inner: &Inner,
    table: &str,
    row: RowUuid,
    key: &[u8],
    seq: u64,
    identity: Option<AuthorSubject>,
) -> Result<bool, Error> {
    let Some(author) = identity else {
        return Ok(true);
    };
    let schema = inner.owner.clone();
    let table = owned_table(&schema, table)?;
    inner.check_version(table, row, key, seq, author, Operation::Select)
}

/// Whether `identity` may read the version of `row` at `seq` and, still,
/// the row as it stands: losing read access takes effect at once for the
/// content of every earlier version too (SPEC 19.9, INV-RLS-13).
fn still_readable_version(
    inner: &Inner,
    table: &str,
    row: RowUuid,
    key: &[u8],
    seq: u64,
    identity: Option<AuthorSubject>,
) -> Result<bool, Error> {
    Ok(readable_version(inner, table, row, key, seq, identity)?
        && readable(inner, table, row, key, identity)?)
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

/// What a session's held messages may change, so a later read that cannot
/// observe it is served ahead of them.
struct HeldWrites {
    /// Stored tables the held commits write; `None` once something is held
    /// whose effect on reads is not known.
    tables: Option<BTreeSet<String>>,
    /// A commit is held: a snapshot read may name it.
    commits: bool,
    /// Held reads, which their unsubscribes must not overtake.
    reads: HashSet<u64>,
}

impl Default for HeldWrites {
    fn default() -> Self {
        Self {
            tables: Some(BTreeSet::new()),
            commits: false,
            reads: HashSet::new(),
        }
    }
}

/// The author a session's receipts are kept under: its identity.
fn receipt_author(identity: &AuthorSubject) -> Vec<u8> {
    identity.canonical().as_bytes().to_vec()
}

/// The row a write names.
fn wire_row(op: &WireOp) -> Option<RowUuid> {
    match op {
        WireOp::Insert { row, .. }
        | WireOp::Update { row, .. }
        | WireOp::Upsert { row, .. }
        | WireOp::Delete { row, .. }
        | WireOp::Restore { row, .. }
        | WireOp::Move { row, .. } => Some(RowUuid::from_bytes(*row)),
    }
}

/// Record that the replica holds `image`, when it has chunked cells: its
/// later versions' go as deltas over it (SPEC 19.8).
fn hold_shipped(shipped: &mut HashMap<(String, [u8; 16], Vec<u8>), u64>, image: &Image) {
    if image.deleted
        || !image
            .cells
            .iter()
            .any(|(_, cell)| matches!(cell, Cell::Large(_)))
    {
        return;
    }
    let seq = shipped
        .entry((image.table.clone(), image.id, image.key.clone()))
        .or_insert(image.seq);
    *seq = (*seq).max(image.seq);
}

// ---- reads through the binding entry point -------------------------------

/// What a serialized one-shot read returns.
#[derive(Debug)]
pub enum SerializedReadResult {
    Rows(Vec<CurrentRow>),
    /// A read with array relations: its root rows, each carrying its
    /// related rows nested in its cells.
    Relation(RelationSnapshot),
}

/// A relation read's rows; roots first.
#[derive(Debug)]
pub struct RelationSnapshot {
    pub rows: Vec<CurrentRow>,
    pub root_count: usize,
}

impl Db {
    /// The bindings' one-shot read: a postcard query, optionally inside an
    /// open transaction. On a client a Remote read, or a local-first read
    /// that waits for the server, fetches first and then reads locally.
    /// `request_scope` reads as that identity, with those claims.
    /// `coverage`: the read must be covered by the server, and fails once
    /// the check says the caller's budget is spent (see [`Db::fetch`]).
    pub async fn all_serialized_query(
        &self,
        query: &[u8],
        opts: ReadOpts,
        open_tx: Option<OpenTransactionId>,
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        coverage: Option<&dyn Fn() -> bool>,
    ) -> Result<SerializedReadResult, Error> {
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
                .tx_ref(tx)
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
        let non_durable = self.inner().non_durable;
        if self.is_client()
            && let Some(reach) = remote_reach(&opts, non_durable, frozen)
            && let Some(deadline) = remote_deadline(&opts, non_durable, true)
        {
            self.remote_shape(&prepared.query)?;
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
    /// stored view, or its upstream's once that answers). Its own writes
    /// take seqs from [`NON_DURABLE_BASE`] up.
    pub fn set_non_durable_client(&self) {
        let mut inner = self.inner.borrow_mut();
        debug_assert!(
            inner.sessions.is_empty(),
            "a non-durable client serves no sessions"
        );
        inner.non_durable = true;
        if inner.client.is_some() {
            inner.raise_to_local_base();
        }
    }
}

impl Db {
    /// Test diagnostics: pending txs, overlaid rows, fates.
    pub fn sync_debug(&self) -> String {
        let inner = self.inner.borrow();
        let Some(client) = &inner.client else {
            return "not a client".to_owned();
        };
        let base = inner.local_base();
        format!(
            "pending {:?} sent {} unsent {} baseline {:?} fates {:?} connected {}",
            client
                .pending
                .iter()
                .map(|p| p.tx - base)
                .collect::<Vec<_>>(),
            client.sent.len(),
            client.unsent.len(),
            client
                .baselines
                .keys()
                .map(|(t, r, _)| (t.clone(), *r))
                .collect::<Vec<_>>(),
            client
                .fates
                .iter()
                .map(|(tx, r)| (tx.saturating_sub(base), r.is_ok()))
                .collect::<BTreeMap<_, _>>(),
            client.transport.is_some(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(id: u8, seq: u64, body: usize) -> Image {
        Image {
            table: "notes".to_owned(),
            id: [id; 16],
            key: Vec::new(),
            seq,
            deleted: false,
            content: true,
            cells: vec![("body".to_owned(), Cell::Blob(vec![0; body]))],
        }
    }

    fn shipped(messages: &[SyncMessage]) -> Vec<Vec<(u8, u64)>> {
        messages
            .iter()
            .map(|message| match message {
                SyncMessage::Rows { images, .. } => images
                    .iter()
                    .map(|image| (image.id[0], image.seq))
                    .collect(),
                other => panic!("not Rows: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn rows_over_the_bound_split_between_commits_in_seq_order() {
        let images = vec![
            image(1, 3, 100),
            image(2, 1, 100),
            image(3, 3, 100),
            image(4, 2, 100),
        ];
        let origins = vec![(1, [9; 16], 1)];
        let messages = rows_messages(images, origins.clone(), 300);
        // seq 1 and 2 fit together; seq 3's two versions stay together.
        assert_eq!(
            shipped(&messages),
            [vec![(2, 1), (4, 2)], vec![(1, 3), (3, 3)]]
        );
        let SyncMessage::Rows { origins: first, .. } = &messages[0] else {
            unreachable!()
        };
        assert_eq!(first, &origins, "the origins go with the first part");
    }

    #[test]
    fn one_commit_over_the_bound_still_travels_whole() {
        let images = vec![image(1, 5, 300), image(2, 5, 300)];
        assert_eq!(
            shipped(&rows_messages(images, Vec::new(), 250)),
            [vec![(1, 5), (2, 5)]]
        );
    }

    #[test]
    fn rows_under_the_bound_keep_their_order_in_one_message() {
        let images = vec![image(1, 3, 10), image(2, 1, 10)];
        assert_eq!(
            shipped(&rows_messages(images, Vec::new(), 250)),
            [vec![(1, 3), (2, 1)]]
        );
        assert!(rows_messages(Vec::new(), Vec::new(), 250).is_empty());
    }

    #[test]
    fn a_joiner_refuses_a_split_message_over_its_ceiling() {
        let bytes = postcard::to_allocvec(&SyncMessage::Unsubscribe { id: 7 }).unwrap();
        let parts = [
            [&[PART], &bytes[..1]].concat(),
            [&[LAST_PART], &bytes[1..]].concat(),
        ];
        let mut joiner = FrameJoiner::with_ceiling(bytes.len());
        assert!(joiner.push(&parts[0]).unwrap().is_none());
        assert!(matches!(
            joiner.push(&parts[1]).unwrap(),
            Some(SyncMessage::Unsubscribe { id: 7 })
        ));
        let mut joiner = FrameJoiner::with_ceiling(bytes.len() - 1);
        assert!(joiner.push(&parts[0]).unwrap().is_none());
        assert!(joiner.push(&parts[1]).is_err());
    }
}
