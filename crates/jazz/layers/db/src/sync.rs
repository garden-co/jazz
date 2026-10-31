//! Client↔server sync over a message transport.
//!
//! Model: the server (the authority) is a `Db` holding complete history; a
//! client is a `Db` whose store is a partial replica of server versions
//! plus its own optimistic writes.
//!
//! * A row version is at a [`Pos`]: the sequencer that numbered it (`_n`)
//!   and its seq there (`_seq`). The authority's versions are at `_n` = 0,
//!   so history and as-of cuts on the client are over server versions. A
//!   replica's optimistic writes are at its own node's code, numbered by
//!   its own counter from 1, and order after every server version, so every
//!   cut at a server seq leaves them out. A non-durable client (a worker's
//!   tab) also holds its worker's unsettled versions, at the worker's code:
//!   a node code's role bits ([`RELAY`], [`LEAF`]) order the worker's
//!   before the tab's own.
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
use crate::values::{STATE_AS_SENT, STATE_DELETED, STATE_NO_CONTENT};
use rusqlite::OptionalExtension;
use rusqlite::types::Value as SqlValue;
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

/// The role bits of a node code (`_n`, SPEC 4): a code is its role ORed
/// with the node's row in `_jazz_nodes`. Code 0 is the authority, the
/// sequencer of every accepted version. A durable replica (a relay, such
/// as a browser's worker) numbers its own versions at a `RELAY` code; a
/// non-durable one (a worker's tab) at a `LEAF` code, so on a tab its
/// relay's unsettled versions order before its own.
pub const RELAY: i64 = 1 << 48;
pub const LEAF: i64 = 2 << 48;

/// The node row a code names, without its role bits.
pub(crate) fn node_index(code: i64) -> i64 {
    code & (RELAY - 1)
}

/// A row version's position: the sequencer that numbered it (`_n`, 0 for
/// the authority) and its seq there. Positions order by sequencer first,
/// so every accepted version precedes every unsettled one, and a relay's
/// unsettled versions precede its tab's own.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos {
    pub n: i64,
    pub seq: u64,
}

impl Pos {
    /// Every accepted version: the cut of a read that leaves out unsettled
    /// ones.
    pub const SETTLED: Self = Self {
        n: 0,
        seq: i64::MAX as u64,
    };

    /// An accepted version, by its authority seq.
    pub const fn at(seq: u64) -> Self {
        Self { n: 0, seq }
    }

    /// Before every unsettled version, after every accepted one.
    pub const UNSETTLED: Self = Self { n: 1, seq: 0 };

    /// Whether the authority numbered the version.
    pub fn accepted(self) -> bool {
        self.n == 0
    }
}

/// Transaction `seq@node`: the node that wrote it and that node's
/// counter. A commit keeps it at every hop: a relay forwards a session's
/// commit under it, and its fate comes back under it.
pub(crate) fn tx_id(node: [u8; 16], seq: u64) -> TxId {
    TxId::new(TxTime(seq), crate::ids::NodeUuid::from_bytes(node))
}

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
/// `held` one: a relay ships older ones in as-of answers. Cuts never read
/// them (a snapshot naming another replica's unsettled commit is
/// unavailable), so the replica keeps neither as history.
fn relay_older(held: Pos, image: Pos) -> bool {
    !image.accepted() && held > image
}

/// The node whose counter a position counts: `None` for the authority
/// (later, a shard), else the node that numbered it (a relay's own order).
pub type Sequencer = Option<[u8; 16]>;

/// A position per sequencer. A watermark, cut or snapshot names one seq for
/// each sequencer it covers (SPEC 8 §8.2), so it extends to several
/// sequencers when sharding lands. Every one names a single sequencer today.
pub type Positions = Vec<(Sequencer, u64)>;

/// `seq` counted at `by`, as [`Positions`].
pub fn positions(by: Sequencer, seq: u64) -> Positions {
    vec![(by, seq)]
}

/// The one position `positions` names (`None` for none). Positions across
/// more than one sequencer are refused: nothing reads them yet.
pub fn single_position(positions: &[(Sequencer, u64)]) -> Result<Option<(Sequencer, u64)>, Error> {
    match positions {
        [] => Ok(None),
        [one] => Ok(Some(*one)),
        _ => Err(Error::new(
            ErrorCode::Protocol,
            "positions across more than one sequencer are not supported yet",
        )),
    }
}

/// The one position a message must name.
pub fn required_position(
    positions: &[(Sequencer, u64)],
    what: &str,
) -> Result<(Sequencer, u64), Error> {
    single_position(positions)?
        .ok_or_else(|| Error::new(ErrorCode::Protocol, format!("{what} names no position")))
}

/// One stored row version: the unit of replication.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Image {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub seq: u64,
    /// The node whose unsettled version this is, numbered by its own
    /// counter (a relay's, shipped to its tab); `None` for an accepted
    /// version, numbered by the authority.
    pub by: Option<[u8; 16]>,
    pub deleted: bool,
    /// Whether the version's commit wrote content (stored in `_st`): history
    /// reads list only content versions.
    pub content: bool,
    pub cells: Vec<(String, Cell)>,
}

/// The most image bytes one `Rows` carries: a first result of any size
/// travels as several, each well under the logical message ceiling a
/// reader joins split frames up to.
const ROWS_MESSAGE_BYTES: usize = jazz_protocol::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES / 4;

/// Where a commit landed and who wrote it: (seq, the node whose position
/// it is when not the authority's (as [`Image::by`]), writer node, writer
/// tx).
pub type TxName = (u64, Option<[u8; 16]>, [u8; 16], u64);

/// `images` (and the `tx_ids` they come with) as `Rows` messages of at
/// most `limit` ([`ROWS_MESSAGE_BYTES`]) of images each. A reader installs each
/// message whole, so a commit's versions (one seq) never straddle two: a
/// result that needs more than one goes in seq order, and a single
/// commit larger than the bound still travels as one.
fn rows_messages(mut images: Vec<Image>, tx_ids: Vec<TxName>, limit: usize) -> Vec<SyncMessage> {
    let size = |image: &Image| {
        postcard::serialize_with_flavor(image, postcard::ser_flavors::Size::default())
            .unwrap_or(usize::MAX)
    };
    let sizes: Vec<usize> = images.iter().map(size).collect();
    if sizes.iter().fold(0usize, |sum, n| sum.saturating_add(*n)) <= limit {
        if images.is_empty() && tx_ids.is_empty() {
            return Vec::new();
        }
        return vec![SyncMessage::Rows { images, tx_ids }];
    }
    let mut sized: Vec<(usize, Image)> = sizes.into_iter().zip(images.drain(..)).collect();
    sized.sort_by_key(|(_, image)| (image.by, image.seq));
    let mut out = Vec::new();
    let mut tx_ids = Some(tx_ids);
    let mut batch: Vec<Image> = Vec::new();
    let mut bytes = 0usize;
    for (n, image) in sized {
        let same_commit = batch
            .last()
            .is_some_and(|last| (last.by, last.seq) == (image.by, image.seq));
        if !batch.is_empty() && !same_commit && bytes.saturating_add(n) > limit {
            out.push(SyncMessage::Rows {
                images: std::mem::take(&mut batch),
                tx_ids: tx_ids.take().unwrap_or_default(),
            });
            bytes = 0;
        }
        bytes = bytes.saturating_add(n);
        batch.push(image);
    }
    out.push(SyncMessage::Rows {
        images: batch,
        tx_ids: tx_ids.take().unwrap_or_default(),
    });
    out
}

/// The provenance cells the applying clock stamps: a writer's replica
/// holds its own clock's, so the authority's time travels in the Fate
/// ([`Outcome::Accepted`]).
const STAMPS: [&str; 2] = ["$createdAt", "$updatedAt"];

/// `message` with each occurrence of the name `from` that is a whole name
/// (not part of a longer one) spelled `to`.
fn replace_name(message: &str, from: &str, to: &str) -> String {
    let part = |c: char| c.is_alphanumeric() || c == '_' || c == '~';
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(at) = rest.find(from) {
        let before = rest[..at].chars().next_back();
        let after = rest[at + from.len()..].chars().next();
        out.push_str(&rest[..at]);
        if before.is_some_and(part) || after.is_some_and(part) {
            out.push_str(from);
        } else {
            out.push_str(to);
        }
        rest = &rest[at + from.len()..];
    }
    out.push_str(rest);
    out
}

/// What a writer and the authority must agree on for a row version the
/// authority does not ship back (see [`AsWritten`]): everything in an image but
/// its seq and [`STAMPS`].
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
/// It travels on the commit's first op that writes the row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireBase {
    /// The row's branch key (empty unbranched).
    pub key: Vec<u8>,
    pub on: BuiltOn,
}

/// A version of the row an op writes that its writer may read, whose
/// chunked values' trees vouch for the nodes the writer did not push
/// (SPEC 19.4): the authority counts that version's trees as possessed by
/// the writer, for every op of the commit, only while the writer may still
/// read the version, so naming a root never gets the writer a tree it could
/// not read anyway.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessProof {
    /// The row's branch key (empty unbranched).
    pub key: Vec<u8>,
    pub seq: u64,
}

/// The version a write was built on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltOn {
    /// An accepted version, by its upstream seq.
    Seq(u64),
    /// The sender's own unsettled commit `back` txs before this one: the
    /// writer saw that commit as it sent it, and what that was built on.
    Own { back: u64 },
    /// An unsettled commit by its sender's (node, tx), as a relay names one
    /// of its own commits.
    Commit { node: [u8; 16], tx: u64 },
}

/// A row of a commit the server did not ship back (see
/// [`Outcome::Accepted`]), named as a [`WireBase`] names it: by the
/// commit's first op that writes it and its branch key. It carries the
/// authority's [`echo_digest`] of its version, which the writer checks its
/// own image against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsWritten {
    pub op: u32,
    pub key: Vec<u8>,
    pub digest: [u8; 16],
}

impl AsWritten {
    fn of(op: u32, image: &Image) -> Self {
        AsWritten {
            op,
            key: image.key.clone(),
            digest: echo_digest(image),
        }
    }
}

/// How the authority decided a commit (see [`SyncMessage::Fate`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// The commit landed at `at`, per sequencer. `accepted_at` is the time
    /// (Unix ms) the authority's clock stamped its writes with: the
    /// `$createdAt`/`$updatedAt` of every write without an explicit time.
    /// `None` when the authority no longer knows it (a resend's recorded
    /// fate), which `as_written` is then empty for.
    ///
    /// `as_written`: the rows of the commit the authority did not ship
    /// because their version at `at` is likely the writer's own image of
    /// them: the commit applied unmerged over the very version its writer
    /// built on (a [`WireBase`], or none for an insert of a new row), and
    /// that version is still current (SPEC 8 §8.3). The writer stamps its
    /// image at `accepted_at` where its own clock stamped it and installs
    /// it if its digest matches; otherwise it asks for the row whole.
    Accepted {
        at: Positions,
        accepted_at: Option<u64>,
        as_written: Vec<AsWritten>,
    },
    /// The commit was refused, for `error`, and landed nowhere. The rows
    /// it wrote shipped back ahead of the fate as they are, and the session
    /// is complete through `through` with them: a retry's snapshot reads
    /// at it, so it sees what the commit conflicted with.
    Refused {
        through: Positions,
        error: WireError,
    },
}

impl Outcome {
    /// An accepted commit that holds no rows back.
    fn accepted(by: Sequencer, seq: u64, accepted_at: Option<u64>) -> Self {
        Outcome::Accepted {
            at: positions(by, seq),
            accepted_at,
            as_written: Vec::new(),
        }
    }

    /// Where the commit landed once accepted, or how far the session was
    /// complete when refused: the outcome's one position.
    pub fn position(&self) -> Result<(Sequencer, u64), Error> {
        let (Outcome::Accepted { at: positions, .. }
        | Outcome::Refused {
            through: positions, ..
        }) = self;
        required_position(positions, "a fate")
    }

    /// The seq the commit landed at, once accepted.
    pub fn seq(&self) -> Result<Option<u64>, Error> {
        match self {
            Outcome::Accepted { .. } => Ok(Some(self.position()?.1)),
            Outcome::Refused { .. } => Ok(None),
        }
    }

    /// The commit's result as this store records and reports it.
    pub fn result(&self) -> Result<(), WireError> {
        match self {
            Outcome::Accepted { .. } => Ok(()),
            Outcome::Refused { error, .. } => Err(error.clone()),
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

/// A replicated write with what its writer knew of the row it writes:
/// what it was built on, the settled version its chunked values' trees sit
/// under, and the chunked cells that travel as edits.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireOp {
    pub write: WireWrite,
    /// The base of each row this op is the commit's first to write, where
    /// its writer held an image (SPEC 4 §4.6): one per branch key it
    /// writes the row under (a move writes two). Only the authority's
    /// conflict derivation reads it (INV-HIST-21).
    pub bases: Vec<WireBase>,
    /// Set when the op's chunked values travel as trees: the settled
    /// version of its row their unpushed nodes sit under.
    pub access_proof: Option<AccessProof>,
    /// The op's chunked cells that travel as edits extending the writer's
    /// newest settled version of its row (SPEC 19.8).
    pub deltas: Vec<CellDelta>,
}

impl From<WireWrite> for WireOp {
    fn from(write: WireWrite) -> Self {
        WireOp {
            write,
            bases: Vec::new(),
            access_proof: None,
            deltas: Vec::new(),
        }
    }
}

impl WireOp {
    /// The stored table the op writes.
    pub fn table(&self) -> &str {
        self.write.table()
    }
}

/// A replicated write, by stored table and column names (branch selectors
/// too), whichever schema version wrote it; an insert names every column of
/// that version. `at`: its explicit provenance timestamp (Unix ms), if the
/// writer gave one; each write in a commit carries its own.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireWrite {
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
        /// What the columns `cells` leaves out take if the upsert creates
        /// the row: its writer's version's defaults and the backwards
        /// defaults of columns its lineage dropped (SPEC 8.9).
        created: BTreeMap<String, Value>,
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

impl WireWrite {
    /// The row the write names.
    pub fn row(&self) -> [u8; 16] {
        match self {
            WireWrite::Insert { row, .. }
            | WireWrite::Update { row, .. }
            | WireWrite::Upsert { row, .. }
            | WireWrite::Delete { row, .. }
            | WireWrite::Restore { row, .. }
            | WireWrite::Move { row, .. } => *row,
        }
    }

    /// The stored table the write names.
    pub fn table(&self) -> &str {
        match self {
            WireWrite::Insert { table, .. }
            | WireWrite::Update { table, .. }
            | WireWrite::Upsert { table, .. }
            | WireWrite::Delete { table, .. }
            | WireWrite::Restore { table, .. }
            | WireWrite::Move { table, .. } => table,
        }
    }
}

/// One read of a serializable transaction: the rows it returned at the
/// snapshot, with their server versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireRead {
    /// The schema version whose names `query` uses (the one the reading
    /// view was in): reads stay in a version, which orders their results
    /// and decides their lens, while writes travel by stored names.
    pub version: [u8; 16],
    pub query: Query,
    pub binding: BTreeMap<String, Value>,
    /// The read listed deleted rows too (`includeDeleted`).
    pub include_deleted: bool,
    /// Each row the read listed, at the version it read: an authority seq
    /// (no node), or an unsettled commit by its (node, tx): the sender's
    /// own, or its relay's that the relay shipped it.
    pub rows: Vec<([u8; 16], Option<[u8; 16]>, u64)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireKind {
    Mergeable,
    /// `own`: the sender's own commits (by local seq) that were unsettled
    /// when the transaction began; its snapshot includes them, and the
    /// authority resolves each to where it settled.
    /// `snapshot`: where it read, per sequencer, as the sender's server
    /// served it when it pinned it.
    Serializable {
        snapshot: Positions,
        reads: Vec<WireRead>,
        own: Vec<u64>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The node whose unsettled version `seq` is (as [`Image::by`]).
    pub by: Option<[u8; 16]>,
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

/// Who a commit's writes, or a preflight, act as. Authors are canonical
/// author subjects. Only a trusted link may name an author other than its
/// own identity; the server decides from the link how far it trusts one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Acting {
    /// The sender's own: checked and attributed as the link.
    Own,
    /// Attributed to `author` (provenance only), checked as the sender.
    Attributed { author: String },
    /// `author`'s as a session with these claims: over a trusted link,
    /// checked and attributed as that session; over a session link,
    /// checked as the link's own identity.
    Session {
        author: String,
        claims: BTreeMap<String, Value>,
    },
}

impl Acting {
    /// The author it names, if any.
    fn author(&self) -> Option<&str> {
        match self {
            Acting::Own => None,
            Acting::Attributed { author } | Acting::Session { author, .. } => Some(author),
        }
    }
}

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
        /// The read view the result is of: the current state, a branch
        /// view (head over an optional live or frozen base), or a
        /// snapshot. Part of the read's identity (`INV-BVIEW-12`).
        view: Box<ReadViewSourceSpec>,
        /// Last complete checkpoint for this read, independent of its lifetime.
        since: Positions,
        span: ReadSpan,
    },
    Unsubscribe {
        id: u64,
    },
    /// Rows, as (stored table, id, branch key, seq), whose shipped deltas
    /// the replica could not apply: it lacks the version a delta was made
    /// over (SPEC 19.8). The server ships each whole, or evicts it. Sent
    /// when a delta fails, and again on each connection until the row
    /// arrives whole. A nonzero seq names a version of the row the replica's
    /// own commit produced and could not install: the server ships it too,
    /// if superseded and still readable.
    ResendWhole {
        rows: Vec<(String, [u8; 16], Vec<u8>, u64)>,
    },
    /// A read's result is complete through `through`, per sequencer (a
    /// replica with no server numbers its versions at its own node). It
    /// speaks for this read alone: the connection's position is
    /// [`SyncMessage::ConnectionSettled`]'s.
    /// `members`: the result's rows, in order: always for a one-shot
    /// read, and for a live one's opening and window changes when it is
    /// limited or its opening could not tell what left it; the replica
    /// drops cached rows `members` leaves out. `global`: the result is the
    /// global authority's (directly, or through relays that settle by
    /// their upstream), not a relay's own store.
    Settled {
        id: u64,
        through: Positions,
        members: Option<Vec<[u8; 16]>>,
        global: bool,
    },
    /// `id`: the writer's, at every hop (a relay forwards its sessions'
    /// commits under theirs). `acting`: who the writes are (see
    /// [`Acting`]).
    Commit {
        id: TxId,
        kind: WireKind,
        ops: Vec<WireOp>,
        acting: Acting,
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
    /// Row images. `tx_ids` names the transactions behind the history
    /// versions among them, as [`TxName`]s: a replica lists a history
    /// version only once it knows its `$transactionId`, and a one-shot
    /// read frozen at a snapshot names the commits that snapshot holds.
    /// Empty for current rows.
    Rows {
        images: Vec<Image>,
        tx_ids: Vec<TxName>,
    },
    /// The row is unavailable to this reader (a write or a policy change
    /// took its read access): the replica drops its copy (INV-SYNC-14).
    /// `hidden`: the row still exists, as a version this reader may not
    /// read, so in a branch view over a base it still overrides the base's
    /// row of the id; the replica keeps a marker of it that masks that row
    /// until an image of it arrives (ch. 8 §8.4). Otherwise it is gone.
    AccessLost {
        table: String,
        id: [u8; 16],
        key: Vec<u8>,
        hidden: bool,
    },
    /// A `Read` the server cannot serve, with why: the
    /// read ends with this error instead of waiting for an answer.
    Refused {
        id: u64,
        code: ErrorCode,
        message: String,
    },
    /// How the authority decided commit `id` ([`Outcome`]). `global`:
    /// the fate is the global authority's (directly, or relayed from it),
    /// not the last word of a replica with no server, which orders its
    /// sessions' commits but cannot make them `Global`.
    Fate {
        id: TxId,
        outcome: Outcome,
        global: bool,
    },
    // client → server
    /// A permission preflight: would the session's identity be allowed
    /// `action` right now? Never writes. `acting` names who asks, as on
    /// a [`SyncMessage::Commit`].
    CheckAllowed {
        id: u64,
        action: PermissionAdviceAction,
        acting: Acting,
    },
    // server → client
    /// The answer to a [`SyncMessage::CheckAllowed`]: `Allowed`, `Denied`
    /// or `Unknown`.
    CheckAllowedResult {
        id: u64,
        result: PermissionAdvice,
    },
    // client → server
    // server → client
    /// Whether the server has admitted the schema version the session
    /// declared, sent when that changes after the [`SyncMessage::Welcome`]
    /// told it first. Until it is admitted the server
    /// holds the session's messages (a draft awaiting its lens), so a
    /// read that must be covered by the server fails with the reason
    /// rather than waiting out its budget.
    SchemaAdmission {
        admitted: bool,
    },
    /// A relay holds the session's commit `id` in its store and uploads
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
        id: TxId,
    },
    /// Client → server, first on every connection: the schema version
    /// the client reads by (its queries use that version's names), and
    /// the versions whose stored names this replica keeps, each
    /// with [`names_digest`] of them. Never held behind an unadmitted
    /// version. The server answers with one [`SyncMessage::Welcome`].
    Join {
        version: [u8; 16],
        known: Vec<([u8; 16], [u8; 16])>,
    },
    /// Server → client: schema versions this store admitted, in admission
    /// order, with where it stores their tables and columns, and the lens
    /// cells its lenses made. A replica stores rows as its upstream does
    /// (rows travel by stored names, untranslated), so it admits each
    /// version under these names, renaming what it named otherwise while
    /// it did not know them. The first rides in the
    /// [`SyncMessage::Welcome`]; this one is sent when the store admits a
    /// version or learns more of its authority's catalogue.
    Catalogue(CatalogueUpdate),
    /// Server → client, the answer to a [`SyncMessage::Join`], ahead of
    /// any row: whether the declared version is admitted (later changes
    /// follow as [`SyncMessage::SchemaAdmission`]) and the catalogue
    /// entries the client lacks or keeps otherwise.
    Welcome {
        admitted: bool,
        catalogue: CatalogueUpdate,
    },
    /// Server → client: the position the whole connection is settled
    /// through. Every read it settled is complete through `through`, and
    /// nothing later sent for those reads lands at or below it. A read's
    /// own [`SyncMessage::Settled`] may name a later position (a sharded
    /// gateway settles each read as far as the shards it reads), so this,
    /// not a read's settle or a commit's fate, moves what the replica
    /// knows. Sent after a tick's rows, settles and fates whenever it moved.
    ConnectionSettled {
        through: Positions,
    },
}

/// The catalogue entries a [`SyncMessage::Catalogue`] or
/// [`SyncMessage::Welcome`] carries.
///
/// `namespace`: the authority's (see `Inner::namespace`), once the sender
/// knows it; a replica that holds another one is another app's and refuses
/// this upstream. `confirmed`: the versions whose names are the
/// authority's, each with its names' digest (every version the authority
/// admitted; on a relay, those its upstream confirmed). Only a confirmed
/// version's tables and columns have an identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogueUpdate {
    pub namespace: Option<[u8; 16]>,
    pub versions: Vec<CatalogueVersion>,
    pub cells: Vec<CatalogueCell>,
    pub confirmed: Vec<([u8; 16], [u8; 16])>,
}

/// How long a [`SyncMessage::Read`] lasts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReadSpan {
    /// Kept current until `Unsubscribe`: what changes in the result
    /// ships as it changes, and each settled state is a `Settled`.
    Live,
    /// Answered once, at the current state (`at` empty) or as of `at`, in
    /// the read's view, whose window and versions the read observes. A
    /// view frozen at a snapshot reference is resolved by the receiver (a
    /// reopened tab's capture, which its worker settled), which also sends
    /// the tx ids of the commits it names (see [`SyncMessage::Rows`]).
    Once { at: Positions },
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
            Self::ResendWhole { .. } => "ResendWhole",
            Self::Commit { .. } => "Commit",
            Self::ChunkRequests { .. } => "ChunkRequests",
            Self::PushChunks { .. } => "PushChunks",
            Self::Chunks { .. } => "Chunks",
            Self::Rows { .. } => "Rows",
            Self::AccessLost { .. } => "AccessLost",
            Self::Settled { .. } => "Settled",
            Self::Refused { .. } => "Refused",
            Self::Fate { .. } => "Fate",
            Self::CheckAllowed { .. } => "CheckAllowed",
            Self::CheckAllowedResult { .. } => "CheckAllowedResult",
            Self::SchemaAdmission { .. } => "SchemaAdmission",
            Self::Stored { .. } => "Stored",
            Self::Join { .. } => "Join",
            Self::Catalogue(_) => "Catalogue",
            Self::Welcome { .. } => "Welcome",
            Self::ConnectionSettled { .. } => "ConnectionSettled",
        }
    }
}

// ---- client state ---------------------------------------------------------

pub(crate) enum Kind {
    Mergeable,
    Serializable {
        snapshot: u64,
        by: Option<[u8; 16]>,
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
    /// Its local seq: where this store holds its versions.
    tx: u64,
    /// Its id upstream: this store's own, or the session's it carries.
    id: TxId,
    /// A serializable commit: its fate is reported as one (INV-TX-9),
    /// whether or not it was ever sent.
    serializable: bool,
    ops: Vec<StagedOp>,
    rows: Vec<(String, RowUuid, Vec<u8>)>,
    /// The rows the commit wrote as stored here: the server may name them
    /// `as_written` instead of shipping them. `None` once this store cannot
    /// name them as the upstream does: the commit was written under an
    /// older schema version, or the upstream's catalogue changed since.
    expected: Option<Vec<Expected>>,
}

/// A row a pending commit wrote, as this replica holds it.
struct Expected {
    /// The commit's first op that writes the row, and the row's branch key.
    op: u32,
    key: Vec<u8>,
    image: Image,
    /// Where the row's `$createdAt` comes from.
    created: Created,
    /// The row's last write has no explicit time: this replica's clock
    /// stamped its `$updatedAt`, which the authority's stamped at its
    /// `accepted_at`.
    updated_clocked: bool,
}

/// Where an as-written row's `$createdAt` comes from: only a write that creates
/// the row stamps it, and it never changes after.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Created {
    /// The commit created the row by a write without an explicit time:
    /// the authority's clock stamped it at its `accepted_at`.
    Clocked,
    /// The commit created the row at an explicit time, the one the image
    /// holds.
    Explicit,
    /// The row was there before: its settled versions hold the stamp.
    /// The image may not (an own earlier commit stamped it here, by this
    /// replica's clock).
    Beneath,
}

impl Expected {
    /// The image as the authority holds it: accepted at `at`, over a row
    /// created at `created` if the commit did not create it.
    fn accepted(&self, at: u64, created: Option<Cell>) -> Image {
        let mut image = self.image.clone();
        for (name, cell) in &mut image.cells {
            if name == STAMPS[0] {
                match (self.created, &created) {
                    (Created::Clocked, _) => *cell = Cell::Int(at as i64),
                    (Created::Beneath, Some(created)) => *cell = created.clone(),
                    _ => {}
                }
            } else if name == STAMPS[1] && self.updated_clocked {
                *cell = Cell::Int(at as i64);
            }
        }
        image
    }
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

    /// No commit's images can be compared with the upstream's any more.
    fn forget_expected(&mut self) {
        for pending in &mut self.commits {
            pending.expected = None;
        }
    }

    /// The id pending commit `tx` uploads under: this store's own, or that
    /// of the session commit it carries.
    pub(super) fn upload_id(&self, tx: u64) -> Option<TxId> {
        self.commits.iter().find(|p| p.tx == tx).map(|p| p.id)
    }

    /// The newest pending commit that writes `row`.
    fn newest(&self, row: &RowKey) -> Option<u64> {
        self.by_row.get(row).and_then(|txs| txs.last().copied())
    }
}

/// A row asked for whole: its stored table, id and branch key.
type WholeRow = (String, [u8; 16], Vec<u8>);

/// A fate held back until rows it names arrive whole (see
/// `Client::deferred`).
struct DeferredFate {
    fate: SyncMessage,
    /// The rows it waits for; `None` for a fate that only follows one.
    awaited: Option<HashSet<WholeRow>>,
}

/// A row an accepted commit wrote that its writer holds as the commit
/// left it (see `SyncMessage::Fate`): fan-out names it `as_written` rather than
/// ship it, if that version is still current.
struct Echo {
    id: TxId,
    op: u32,
    table: String,
    row: RowUuid,
    key: Vec<u8>,
    at: Pos,
}

/// A row an applied commit wrote over the very version its writer built
/// it on, unmerged: its writer likely holds the new version as it is (see
/// [`Outcome::Accepted`]).
pub(crate) struct Echoable {
    pub(crate) row: RowKey,
    /// The commit's first op that writes the row.
    pub(crate) op: u32,
}

/// Subscription-only state attached to an active read.
struct LiveRead {
    refs: usize,
    reach: RemoteReach,
    answered: Rc<StdCell<bool>>,
    global: Rc<StdCell<bool>>,
    refused: Rc<RefCell<Option<Error>>>,
    /// The highest position the upstream settled this read through: its
    /// results read as of it (or the connection's, when that is further),
    /// see `Inner::visible_read`. Kept across reconnects.
    visible: Option<Pos>,
}

enum ReadLifetime {
    Once { historical: bool },
    Live(LiveRead),
}

/// Query identity and settlement metadata shared by every active upstream read.
struct ActiveRead {
    query: Query,
    binding: BTreeMap<String, Value>,
    include_deleted: bool,
    view: ReadViewSourceSpec,
    watermark: Option<[u8; 32]>,
    through: Option<(Sequencer, u64)>,
    lifetime: ReadLifetime,
}

impl ActiveRead {
    fn live(&self) -> Option<&LiveRead> {
        match &self.lifetime {
            ReadLifetime::Live(live) => Some(live),
            ReadLifetime::Once { .. } => None,
        }
    }

    fn live_mut(&mut self) -> Option<&mut LiveRead> {
        match &mut self.lifetime {
            ReadLifetime::Live(live) => Some(live),
            ReadLifetime::Once { .. } => None,
        }
    }

    fn since(&self) -> Positions {
        self.through
            .map_or_else(Vec::new, |(by, seq)| positions(by, seq))
    }
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
    /// All active reads, keyed by their wire request id.
    reads: HashMap<u64, ActiveRead>,
    /// Deduplication index for live reads; owns no query or checkpoint state.
    live_reads: HashMap<String, u64>,
    outbox: Vec<SyncMessage>,
    pending: PendingQueue,
    unsent: Vec<SyncMessage>,
    /// Commits sent but not settled, for resending after a reconnect.
    sent: Vec<SyncMessage>,
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
    /// The position the upstream last said the connection is settled
    /// through ([`SyncMessage::ConnectionSettled`]).
    pub(crate) known: u64,
    /// The node code whose positions `known` counts: 0, the authority's,
    /// unless the upstream is a replica with no server (its own node's).
    pub(crate) known_n: i64,
    /// The `known` cut last written to `_jazz_meta`.
    persisted_known: u64,
    /// This replica's own commits accepted past `known`, as (seq, tx):
    /// what it knows does not hold them yet, so a snapshot or a
    /// serializable transaction names them by tx. Two sequencers may number
    /// different commits alike, so each is its own entry.
    pub(crate) own_accepted: BTreeSet<(u64, u64)>,
    /// Per stored table, the newest accepted position of a version the
    /// upstream sent: a result reading no table past its position needs
    /// no cut.
    pub(crate) newest_foreign: HashMap<String, u64>,
    /// A live read's position, the connection's, or an own commit past
    /// them moved since results last ran.
    pub(crate) visibility_moved: bool,
    answers: HashMap<u64, Option<Result<(Pos, Vec<[u8; 16]>), Error>>>,
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
    /// This store committed writes before it became a replica (see
    /// [`stranded`]): it refuses every upstream.
    stranded: bool,
    /// Why this store refused its upstream's catalogue, until one is
    /// mirrored: remote reads opened meanwhile fail with it at once.
    uncatalogued: Option<Error>,
    /// The upstream sent its catalogue on this connection: this store
    /// knows every version the authority deployed, so its own sessions'
    /// commits under a version it still lacks are refused (INV-SYNC-52).
    catalogued: bool,
    /// Rows asked for whole (their shipped deltas did not apply), until a
    /// whole image of each arrives. This store holds a stale version of
    /// each, so no result it settles is the server's: no watermark is
    /// stored while any is owed, and each is asked again on reconnect.
    /// Each with the version of it this store must also hold, if any: an
    /// own commit's, which it could not install (0 for none).
    whole: HashMap<WholeRow, u64>,
    /// Fates of own commits applied only once rows they name as written,
    /// which this store could not install, arrived whole (the commit stays
    /// pending meanwhile, so its writes stay visible), and the fates that
    /// arrived after them, in order.
    deferred: VecDeque<DeferredFate>,
    /// The deferred fate being applied now: its tx, and whether the rows it
    /// waited for arrived (so it installs the rest without asking again).
    releasing: Option<(u64, bool)>,
    /// No resume point was stored since they were last all forgotten, so
    /// forgetting them again (another eviction this tick) costs nothing.
    forgotten: bool,
}

/// A session's commit a relay uploads as its own, for its upload: the
/// session commit's (node, tx) and its bases in this store's terms, by
/// the op each travels on.
pub(crate) struct Relayed {
    origin: ([u8; 16], u64),
    bases: Vec<(usize, WireBase)>,
}

/// A session's commit a relay uploads: (session, the commit's id, the rows
/// it wrote).
type RelayedFate = (usize, TxId, Vec<(String, RowUuid, Vec<u8>)>);

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
    view: &ReadViewSourceSpec,
    reach: RemoteReach,
) -> String {
    format!("{query:?}{binding:?}{include_deleted}{view:?}{reach:?}")
}

/// What a replica asks its upstream to keep current for a live read of
/// `query` in `source`: the query, its deletion flag and its view. A
/// branch view is maintained upstream as it is, over a frozen base too, so
/// the upstream also ships the head images that mask base rows this
/// replica holds (ch. 8 §8.4). A snapshot read takes the current rows and
/// applies the cut itself.
pub(crate) fn upstream_read<'q>(
    query: &'q Query,
    include_deleted: bool,
    source: &ReadViewSourceSpec,
) -> (std::borrow::Cow<'q, Query>, bool, ReadViewSourceSpec) {
    let query = remote_query(query);
    match source {
        ReadViewSourceSpec::Snapshot { .. } => {
            (query, include_deleted, ReadViewSourceSpec::Current)
        }
        view => (query, include_deleted, view.clone()),
    }
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
type RemoteRead = (Query, BTreeMap<String, Value>, bool, ReadViewSourceSpec);

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
    /// On a relay, rows a one-shot answer shipped at one of this store's
    /// unsettled versions (by its node and seq), which no watch keeps
    /// current: each ships again once uploads settle and that version is no
    /// longer the row's, since a refused write leaves the replica holding a
    /// version no later answer replaces (SPEC 8 §8.5).
    unsettled: BTreeMap<(String, RowUuid, Vec<u8>), ([u8; 16], u64)>,
    /// Versions evicted as unreadable while the session still watches
    /// their id (it holds another branch's version of the row): a write
    /// that leaves one unreadable ships nothing more, so the replica learns
    /// once that it may not read it, not each time it is edited.
    unreadable: HashSet<(String, RowUuid, Vec<u8>)>,
    /// The versions this tick's accepted commits from the client wrote:
    /// (table, id, branch key, position). Each ships even when a later
    /// version supersedes it, so the replica holds every state its own
    /// commits produced (a snapshot naming one reads it there).
    committed: Vec<(String, RowUuid, Vec<u8>, Pos)>,
    /// This tick's accepted commits' rows their writer holds as they left
    /// them: the image of each is not shipped while it is the row's current
    /// version.
    echoes: Vec<Echo>,
    /// Rows the replica asked for whole this tick: they ship even where an
    /// echo would name them as written, since the replica could not install
    /// its own image of them.
    asked_whole: HashSet<RowKey>,
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
    shipped: HashMap<(String, [u8; 16], Vec<u8>), Pos>,
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
    /// (`None` until its `Join`), and whether it is owed a `Catalogue`.
    known_catalogue: Option<HashMap<[u8; 16], [u8; 16]>>,
    catalogue_owed: bool,
    /// The client's `Join` is not answered yet: its catalogue and
    /// admission go out together as a `Welcome`.
    welcome_owed: bool,
    /// The namespace and confirmations the client was last sent.
    told: Option<(Option<[u8; 16]>, Vec<([u8; 16], [u8; 16])>)>,
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
    /// The last [`SyncMessage::ConnectionSettled`] position it was sent.
    connection_settled: Option<(Option<[u8; 16]>, u64)>,
    /// On a relay, how far each relayed subscription was told it is
    /// settled: as far as its upstream read's own settle, which may pass
    /// what the relay knows of the connection.
    settles: BTreeMap<u64, RelayedSettle>,
}

/// A relayed subscription's settle, forwarded from its upstream read.
struct RelayedSettle {
    /// The upstream live read's key.
    key: String,
    /// The position the session was last told, once the result settled.
    sent: Option<u64>,
    global: bool,
}

/// A relayed LocalFirst subscription's first-load wait: once `until`
/// passes unanswered, its result is this store's (see
/// [`Db::serve_due_first_loads`]).
struct FirstLoadWait {
    until: Instant,
    prepared: PreparedQuery,
    /// The read's view and deletion flag, served without the settled-only
    /// narrowing once the wait runs out.
    read: ReadSpec,
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
            unsettled: BTreeMap::new(),
            unreadable: HashSet::new(),
            committed: Vec::new(),
            echoes: Vec::new(),
            asked_whole: HashSet::new(),
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
            known_catalogue: None,
            catalogue_owed: false,
            welcome_owed: false,
            told: None,
            schema: None,
            relayed: BTreeMap::new(),
            first_load: BTreeMap::new(),
            connection_settled: None,
            settles: BTreeMap::new(),
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
        if self.known_catalogue.is_some() {
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

/// The watch on row `id` of stored table `table`.
fn id_watch(table: &str, id: [u8; 16]) -> u64 {
    wk((
        table.to_owned(),
        "id".to_owned(),
        watch_key(&Sql::Blob(id.to_vec())),
    ))
}

/// The `_jazz_meta` key of the authority namespace (see `Inner::namespace`).
pub(super) const NAMESPACE_META: &str = "namespace";

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
    let write = match op {
        StagedOp::Insert(table, cells, opts) => WireWrite::Insert {
            table: table.clone(),
            row: bytes(&opts.row_id.expect("inserts carry their row id")),
            cells: cells.clone(),
            target: exact_target(&opts.target),
            at: opts.updated_at_ms,
        },
        StagedOp::Update(table, row, cells, opts) => WireWrite::Update {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Upsert(table, row, cells, created, opts) => WireWrite::Upsert {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            created: created.clone(),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Delete(table, row, opts) => WireWrite::Delete {
            table: table.clone(),
            row: bytes(row),
            target: wire_target(&opts.target)?,
            at: opts.updated_at_ms,
        },
        StagedOp::Restore(table, row, cells, opts) => WireWrite::Restore {
            table: table.clone(),
            row: bytes(row),
            cells: cells.clone(),
            target: exact_target(&opts.target),
            at: opts.updated_at_ms,
        },
        StagedOp::Move(table, from, to, row) => WireWrite::Move {
            table: table.clone(),
            row: bytes(row),
            from: from.clone(),
            to: to.clone(),
        },
    };
    Ok(write.into())
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
    Ok(match op.write {
        WireWrite::Insert {
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
        WireWrite::Update {
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
        WireWrite::Upsert {
            table,
            row,
            cells,
            created,
            target,
            at,
        } => StagedOp::Upsert(
            table,
            id(row),
            cells,
            created,
            UpsertOptions {
                target: view_target(target),
                updated_at_ms: at,
                ..Default::default()
            },
        ),
        WireWrite::Delete {
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
        WireWrite::Restore {
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
        WireWrite::Move { .. } => return Err(unsupported("branch moves over sync")),
    })
}

/// The image filter for one row version. `+_seq` keeps the planner on
/// the row-id index: after a bulk commit every row shares one `_seq`, and
/// the `_seq` index would scan them all for each lookup.
const ID_AT: &str = "id = ?1 AND +_n = ?2 AND +_seq = ?3";

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
    /// current one. The versions' seqs go to `seqs`, for their tx ids.
    fn history_versions(
        &self,
        source: &str,
        versions: impl IntoIterator<Item = (RowUuid, Pos)>,
        images: &mut Vec<Image>,
        seqs: &mut BTreeSet<u64>,
    ) -> Result<(), Error> {
        let versions: Vec<(RowUuid, Pos)> = versions.into_iter().collect();
        let found = self.version_images(source, &versions)?;
        let mut current = BTreeSet::new();
        for version in &versions {
            for image in &found[version] {
                seqs.insert(image.seq);
                current.insert((version.0, image.key.clone()));
            }
            images.extend(found[version].iter().cloned());
        }
        let currents = self.current_images(source, &current)?;
        images.extend(current.iter().filter_map(|key| currents.get(key).cloned()));
        Ok(())
    }

    /// [`Self::history_versions`] for each of `versions` on its own: its
    /// images, then the current images of its row in their branches.
    fn history_versions_each(
        &self,
        source: &str,
        versions: &[(RowUuid, Pos)],
        seqs: &mut BTreeSet<u64>,
    ) -> Result<HashMap<(RowUuid, Pos), Vec<Image>>, Error> {
        let found = self.version_images(source, versions)?;
        let keys: BTreeSet<(RowUuid, Vec<u8>)> = found
            .iter()
            .flat_map(|((row, _), images)| images.iter().map(|image| (*row, image.key.clone())))
            .collect();
        let currents = self.current_images(source, &keys)?;
        Ok(found
            .into_iter()
            .map(|((row, at), mut images)| {
                let keys: BTreeSet<Vec<u8>> = images
                    .iter()
                    .inspect(|image| {
                        seqs.insert(image.seq);
                    })
                    .map(|image| image.key.clone())
                    .collect();
                images.extend(
                    keys.into_iter()
                        .filter_map(|key| currents.get(&(row, key)).cloned()),
                );
                ((row, at), images)
            })
            .collect())
    }

    /// The images flat-join occurrences ship for their joined rows, one
    /// list per `(stored table, joined row)` of `joined`, in order: a
    /// joined row's current images, or for a history source the version it
    /// names and the row's current image.
    fn joined_images(
        &self,
        joined: &[(&str, &crate::values::JoinedRow)],
        seqs: &mut BTreeSet<u64>,
    ) -> Result<Vec<Vec<Image>>, Error> {
        // Each pair's version: `None` for its current images, or the
        // version's position (`None` inside: a version with no images).
        let mut at = Vec::with_capacity(joined.len());
        let mut current: BTreeMap<&str, Vec<RowUuid>> = BTreeMap::new();
        let mut versions: BTreeMap<&str, Vec<(RowUuid, Pos)>> = BTreeMap::new();
        for (table, joined) in joined {
            let Some((tx, node)) = joined.version else {
                current.entry(table).or_default().push(joined.row);
                at.push(None);
                continue;
            };
            // A version's seq: as `_jazz_transactions` recorded its upload,
            // else this authority's own commit (whose tx is its seq).
            let recorded = self
                .conn
                .prepare_cached(
                    "SELECT t.seq FROM _jazz_transactions AS t JOIN _jazz_nodes AS n \
                     ON n.n = t.n WHERE t.tx = ?1 AND n.node = ?2 AND t.sn = 0 AND t.seq IS NOT NULL",
                )
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
                Some(seq) => Some(seq as u64),
                None if node == self.identity.node => Some(tx),
                None => None,
            };
            if let Some(seq) = seq {
                versions
                    .entry(table)
                    .or_default()
                    .push((joined.row, Pos::at(seq)));
            }
            at.push(Some(seq.map(Pos::at)));
        }
        let mut current_of = HashMap::new();
        for (table, rows) in current {
            current_of.insert(table, self.rows_images(table, rows)?);
        }
        let mut versions_of = HashMap::new();
        for (table, versions) in versions {
            versions_of.insert(table, self.history_versions_each(table, &versions, seqs)?);
        }
        Ok(joined
            .iter()
            .zip(at)
            .map(|((table, joined), at)| match at {
                None => current_of[table]
                    .get(&joined.row)
                    .cloned()
                    .unwrap_or_default(),
                Some(None) => Vec::new(),
                Some(Some(at)) => versions_of[table][&(joined.row, at)].clone(),
            })
            .collect())
    }

    /// Who committed each of `seqs`, as (seq, node, tx): `_jazz_transactions`
    /// for uploads, this authority's node for its own commits.
    fn tx_ids(&self, seqs: &BTreeSet<u64>) -> Result<Vec<TxName>, Error> {
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT n.node, t.tx FROM _jazz_transactions AS t JOIN _jazz_nodes AS n \
                 ON n.n = t.n WHERE t.sn = 0 AND t.seq = ?1",
            )
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
                Some((node, tx)) => (seq, None, node.try_into().unwrap_or([0; 16]), tx),
                None => (seq, None, *self.identity.node.as_bytes(), seq),
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
            Pos::at(floor.since),
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
        // An as-sent version is no image: it never syncs.
        let sql = if history {
            format!(
                "SELECT * FROM (SELECT * FROM {} WHERE (_st & {STATE_AS_SENT}) = 0) WHERE {filter}",
                quote(&name)
            )
        } else {
            format!("SELECT * FROM {} WHERE {filter}", quote(&name))
        };
        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
        // A cached `SELECT *` prepared before its table gained a column
        // names the old columns until a step re-prepares it: the names are
        // read from the first row.
        let mut names: Vec<String> = Vec::new();
        let rows = statement
            .query_map(params, |r| {
                if names.is_empty() {
                    let statement = r.as_ref();
                    names = (0..statement.column_count())
                        .map(|i| statement.column_name(i).map(str::to_owned))
                        .collect::<Result<_, _>>()?;
                }
                let mut image = Image {
                    table: table.to_owned(),
                    id: [0; 16],
                    key: Vec::new(),
                    seq: 0,
                    by: None,
                    deleted: false,
                    content: true,
                    cells: Vec::new(),
                };
                let mut code = 0;
                for (i, name) in names.iter().enumerate() {
                    match name.as_str() {
                        "rid" => {}
                        "id" => image.id = r.get::<_, Vec<u8>>(i)?.try_into().unwrap_or([0; 16]),
                        "_b" => image.key = r.get(i)?,
                        "_n" => code = r.get::<_, i64>(i)?,
                        "_seq" => image.seq = r.get::<_, i64>(i)? as u64,
                        "_st" => {
                            let state = r.get::<_, i64>(i)?;
                            image.deleted = state & STATE_DELETED != 0;
                            image.content = state & STATE_NO_CONTENT == 0;
                        }
                        // The authority's record of what the version was
                        // built on; never replicated.
                        "_base" | "_cols" => {}
                        _ => image
                            .cells
                            .push((name.clone(), Cell::from(r.get::<_, Sql>(i)?))),
                    }
                }
                Ok((code, image))
            })
            .map_err(storage)?;
        let images = rows.collect::<Result<Vec<_>, _>>().map_err(storage)?;
        let mut images = images
            .into_iter()
            .map(|(code, mut image)| {
                image.by = self.node_of(code)?;
                Ok(image)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for image in &mut images {
            self.resolve_authors(image)?;
            self.portable_large(image)?;
            self.portable_enums(image)?;
        }
        Ok(images)
    }

    /// Images carry enum cells by their variant names: a store's tags are
    /// its own (SPEC 10.8).
    fn portable_enums(&self, image: &mut Image) -> Result<(), Error> {
        for (name, cell) in &mut image.cells {
            let Some(ty) = self
                .names
                .stored_type(&image.table, name)
                .filter(|ty| crate::values::is_enum_type(ty))
            else {
                continue;
            };
            let tags = self.names.tags().column(&image.table, name);
            let tags = tags.borrow();
            let named = |tag: i64| {
                tags.name(tag).map(str::to_owned).ok_or_else(|| {
                    Error::new(
                        ErrorCode::Storage,
                        format!("enum tag {tag} names no variant this store knows"),
                    )
                })
            };
            *cell = match cell {
                Cell::Int(tag) => Cell::Text(named(*tag)?.into_bytes()),
                Cell::Text(json) if crate::values::is_array_type(ty) => {
                    let items: Vec<serde_json::Value> = serde_json::from_slice(json)
                        .map_err(|e| Error::new(ErrorCode::Storage, format!("array cell: {e}")))?;
                    let items = items
                        .into_iter()
                        .map(|item| match item.as_i64() {
                            Some(tag) => named(tag).map(serde_json::Value::String),
                            None => Ok(item),
                        })
                        .collect::<Result<Vec<_>, Error>>()?;
                    Cell::Text(serde_json::Value::Array(items).to_string().into_bytes())
                }
                _ => continue,
            };
        }
        Ok(())
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
        shipped: &mut HashMap<(String, [u8; 16], Vec<u8>), Pos>,
        image: &mut Image,
    ) -> Result<(), Error> {
        let held = shipped
            .get(&(image.table.clone(), image.id, image.key.clone()))
            .copied();
        hold_shipped(shipped, image, self.pos(image)?);
        // A delta names its base by an authority seq: a relay's unsettled
        // version travels whole to its tab.
        let Some(at) = held.filter(|held| !image.deleted && held.accepted()) else {
            return Ok(());
        };
        let seq = at.seq;
        let row = RowUuid::from_bytes(image.id);
        let Some(base) = self.version_image(&image.table, row, &image.key, at)? else {
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

    /// The version of a row at `at`, current or in history.
    fn version_image(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        at: Pos,
    ) -> Result<Option<Image>, Error> {
        let id = row.0.as_bytes().to_vec();
        let seq = at.seq as i64;
        for history in [false, true] {
            let found = self
                .images_where(
                    table,
                    history,
                    "id = ?1 AND _b = ?2 AND _n = ?3 AND _seq = ?4",
                    &[&id, &key, &at.n, &seq],
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
            // An enum cell travels by variant name (see `portable_enums`).
            if self
                .names
                .stored_type(&image.table, name)
                .is_some_and(crate::values::is_enum_type)
            {
                cells.push((
                    name.clone(),
                    self.enum_cell(&image.table, name, Sql::from(cell))?,
                ));
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

    /// Of the ids `ids`, those whose base row the reader may see through
    /// `mask`'s view: of the root table, rows the query matches on the
    /// base; of another table, rows the reader may read there.
    fn masked(
        &self,
        policy: Option<AuthorSubject>,
        mask: &fanout::ViewMask,
        ids: &[RowUuid],
    ) -> Result<Vec<RowUuid>, Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        match &mask.base_query {
            Some(query) => {
                let mut out = Vec::new();
                for chunk in ids.chunks(500) {
                    out.extend(
                        self.run(&query.narrowed(chunk), policy, &mask.base_read)?
                            .iter()
                            .map(|row| row.id),
                    );
                }
                Ok(out)
            }
            None => {
                let mut out = Vec::new();
                for &row in ids {
                    let shown = self
                        .current_image(&mask.table, row, &mask.base)?
                        .is_some_and(|image| !image.deleted || mask.base_read.include_deleted);
                    if shown && readable(self, &mask.table, row, &mask.base, policy)? {
                        out.push(row);
                    }
                }
                Ok(out)
            }
        }
    }

    /// The head rows of `mask`'s view that mask base rows the reader may
    /// see through it. A resumed read takes them all too: whether the
    /// reader may read one can have changed since without it changing.
    fn masking_rows(
        &self,
        policy: Option<AuthorSubject>,
        mask: &fanout::ViewMask,
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, Error> {
        let mut heads: Vec<RowUuid> = self
            .images_where(&mask.table, false, "_b = ?1", &[&mask.head])?
            .into_iter()
            .map(|image| RowUuid::from_bytes(image.id))
            .collect();
        // A relay's markers of head rows it may not read mask for its
        // sessions too.
        heads.extend(
            self.withheld_ids(&mask.table, &mask.head)?
                .into_iter()
                .map(RowUuid::from_bytes),
        );
        Ok(self
            .masked(policy, mask, &heads)?
            .into_iter()
            .map(|row| (mask.table.clone(), row, mask.head.clone()))
            .collect())
    }

    /// Mark a row this replica may not read the current version of (see
    /// [`WITHHELD_DDL`]).
    fn withhold(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        if !self.withheld.get() {
            self.exec(WITHHELD_DDL, []).map_err(storage)?;
            self.withheld.set(true);
        }
        let id = row.0.as_bytes().to_vec();
        self.exec(
            "INSERT OR IGNORE INTO _jazz_withheld (tbl, id, _b) VALUES (?1, ?2, ?3)",
            rusqlite::params![table, id, key],
        )
        .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.to_owned(), row, key.to_vec()));
        Ok(())
    }

    /// Lift a row's withheld marker, if it has one.
    fn unwithhold(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        if !self.withheld.get() {
            return Ok(());
        }
        let id = row.0.as_bytes().to_vec();
        let lifted = self
            .exec(
                "DELETE FROM _jazz_withheld WHERE tbl = ?1 AND id = ?2 AND _b = ?3",
                rusqlite::params![table, id, key],
            )
            .map_err(storage)?;
        if lifted > 0 {
            self.changed
                .borrow_mut()
                .push((table.to_owned(), row, key.to_vec()));
        }
        Ok(())
    }

    /// Whether stored table `table` holds a version of `row` on any branch.
    fn holds_id(&self, table: &str, row: RowUuid) -> Result<bool, Error> {
        self.conn
            .cached_row(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE id = ?1)",
                    quote(table)
                ),
                [row.0.as_bytes().to_vec()],
                |r| r.get::<_, bool>(0),
            )
            .map_err(storage)
    }

    pub(super) fn is_withheld(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<bool, Error> {
        if !self.withheld.get() {
            return Ok(false);
        }
        let id = row.0.as_bytes().to_vec();
        self.conn
            .cached_row(
                "SELECT count(*) FROM _jazz_withheld WHERE tbl = ?1 AND id = ?2 AND _b = ?3",
                rusqlite::params![table, id, key],
                |r| r.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(storage)
    }

    /// The ids of `table` withheld on branch key `key`.
    fn withheld_ids(&self, table: &str, key: &[u8]) -> Result<Vec<[u8; 16]>, Error> {
        if !self.withheld.get() {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare_cached("SELECT id FROM _jazz_withheld WHERE tbl = ?1 AND _b = ?2")
            .map_err(storage)?;
        let ids = statement
            .query_map(rusqlite::params![table, key], |r| r.get::<_, Vec<u8>>(0))
            .map_err(storage)?
            .filter_map(|id| id.ok().and_then(|id| id.try_into().ok()))
            .collect();
        Ok(ids)
    }

    /// Every image of each of `rows` in `table` (or its history).
    fn images_of(
        &self,
        table: &str,
        history: bool,
        rows: impl IntoIterator<Item = RowUuid>,
    ) -> Result<Vec<Image>, Error> {
        self.images_matching(table, history, rows, |row| {
            vec![SqlValue::Blob(row.0.as_bytes().to_vec())]
        })
    }

    /// The images of `table` (or its history) that match one of `keys`, read
    /// a chunk of keys per statement. Reads of many images all go through
    /// here: an opening snapshot ships thousands, a later tick a few. A key
    /// binds to a row id, or to a row id, node code and seq (an [`ID_AT`]
    /// version, so a row with a long history reads only the versions asked
    /// for). A chunk binds its keys to the smallest of a few statement sizes
    /// that fits, the rest repeating its last key, so a lone key is one seek
    /// and every size stays prepared. The largest size fits the
    /// connection's bound-variable limit (100 on a Durable Object).
    fn images_matching<K: Ord>(
        &self,
        table: &str,
        history: bool,
        keys: impl IntoIterator<Item = K>,
        bind: impl Fn(&K) -> Vec<SqlValue>,
    ) -> Result<Vec<Image>, Error> {
        const SIZES: [usize; 4] = [1, 8, 64, 512];
        // Each key once: a key in two chunks would match twice.
        let keys: Vec<Vec<SqlValue>> = keys
            .into_iter()
            .collect::<BTreeSet<_>>()
            .iter()
            .map(bind)
            .collect();
        let Some(arity) = keys.first().map(Vec::len) else {
            return Ok(Vec::new());
        };
        let variables = self
            .conn
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
            .map_err(storage)?;
        let largest = (usize::try_from(variables).unwrap_or(0) / arity).clamp(1, SIZES[3]);
        let sizes: Vec<usize> = SIZES
            .into_iter()
            .filter(|size| *size < largest)
            .chain([largest])
            .collect();
        let mut images = Vec::new();
        for chunk in keys.chunks(largest) {
            let size = *sizes
                .iter()
                .find(|size| **size >= chunk.len())
                .expect("a chunk fits the largest size");
            let filter = match (arity, size) {
                (1, 1) => "id = ?1".to_owned(),
                (1, _) => {
                    let placeholders: Vec<String> = (1..=size).map(|i| format!("?{i}")).collect();
                    format!("id IN ({})", placeholders.join(", "))
                }
                (3, _) => {
                    let terms: Vec<String> = (0..size)
                        .map(|i| {
                            let at = 3 * i;
                            format!(
                                "(id = ?{} AND +_n = ?{} AND +_seq = ?{})",
                                at + 1,
                                at + 2,
                                at + 3
                            )
                        })
                        .collect();
                    terms.join(" OR ")
                }
                _ => unreachable!("a key binds a row id or a version"),
            };
            let last = chunk.last().expect("a chunk is never empty");
            let params: Vec<&dyn rusqlite::ToSql> = chunk
                .iter()
                .chain(std::iter::repeat(last))
                .take(size)
                .flatten()
                .map(|value| value as &dyn rusqlite::ToSql)
                .collect();
            images.extend(self.images_where(table, history, &filter, &params)?);
        }
        Ok(images)
    }

    /// The current images of each of `rows` in `table`, by row.
    fn rows_images(
        &self,
        table: &str,
        rows: impl IntoIterator<Item = RowUuid>,
    ) -> Result<HashMap<RowUuid, Vec<Image>>, Error> {
        let mut by_row: HashMap<RowUuid, Vec<Image>> = HashMap::new();
        for image in self.images_of(table, false, rows)? {
            by_row
                .entry(RowUuid::from_bytes(image.id))
                .or_default()
                .push(image);
        }
        Ok(by_row)
    }

    /// The images of `table` (or its history) at each of `versions`.
    fn images_at(
        &self,
        table: &str,
        history: bool,
        versions: &[(RowUuid, Pos)],
    ) -> Result<HashMap<(RowUuid, Pos), Vec<Image>>, Error> {
        let mut found: HashMap<(RowUuid, Pos), Vec<Image>> = versions
            .iter()
            .map(|version| (*version, Vec::new()))
            .collect();
        // A current table holds a version or two of a row, read whole by
        // id; a history may hold thousands, so it reads only these.
        let images = if history {
            self.images_matching(table, history, versions.iter().copied(), |(row, at)| {
                vec![
                    SqlValue::Blob(row.0.as_bytes().to_vec()),
                    SqlValue::Integer(at.n),
                    SqlValue::Integer(at.seq as i64),
                ]
            })?
        } else {
            self.images_of(table, history, versions.iter().map(|(row, _)| *row))?
        };
        for image in images {
            // A read: a node no version names yet is not interned.
            let Some(n) = self.known_code(image.by)? else {
                continue;
            };
            let at = Pos { n, seq: image.seq };
            if let Some(images) = found.get_mut(&(RowUuid::from_bytes(image.id), at)) {
                images.push(image);
            }
        }
        Ok(found)
    }

    /// The images of each of `versions` of `source`: in the current table,
    /// else in its history.
    fn version_images(
        &self,
        source: &str,
        versions: &[(RowUuid, Pos)],
    ) -> Result<HashMap<(RowUuid, Pos), Vec<Image>>, Error> {
        let mut found = self.images_at(source, false, versions)?;
        let missing: Vec<(RowUuid, Pos)> = versions
            .iter()
            .filter(|version| found[*version].is_empty())
            .copied()
            .collect();
        if !missing.is_empty() {
            found.extend(self.images_at(source, true, &missing)?);
        }
        Ok(found)
    }

    /// The current image of each `(row, branch key)` of `keys` that has one.
    fn current_images(
        &self,
        table: &str,
        keys: &BTreeSet<(RowUuid, Vec<u8>)>,
    ) -> Result<HashMap<(RowUuid, Vec<u8>), Image>, Error> {
        let mut found = HashMap::new();
        for image in self.images_of(table, false, keys.iter().map(|(row, _)| *row))? {
            let key = (RowUuid::from_bytes(image.id), image.key.clone());
            if keys.contains(&key) {
                found.insert(key, image);
            }
        }
        Ok(found)
    }

    /// Read the current images of `keys` (stored table, row, branch key)
    /// at once, for the stores that follow: a replica stores a message's
    /// images one by one, each after reading what it holds. Until
    /// [`Self::end_current_batch`], [`Self::current_image`] answers from
    /// these, and a store or drop of one of them forgets it.
    fn begin_current_batch(
        &self,
        keys: impl IntoIterator<Item = (String, RowUuid, Vec<u8>)>,
    ) -> Result<(), Error> {
        let mut by_table: BTreeMap<String, BTreeSet<(RowUuid, Vec<u8>)>> = BTreeMap::new();
        let baselines = self.client.as_ref().map(|client| &client.baselines);
        for key in keys {
            // An image over a pending write's baseline replaces the
            // baseline, not the stored row.
            if baselines.is_some_and(|baselines| baselines.contains_key(&key)) {
                continue;
            }
            let (table, row, key) = key;
            by_table.entry(table).or_default().insert((row, key));
        }
        let mut batch = HashMap::new();
        for (table, keys) in by_table {
            let mut found = self.current_images(&table, &keys)?;
            for (row, key) in keys {
                let image = found.remove(&(row, key.clone()));
                batch.insert((table.clone(), row, key), image);
            }
        }
        *self.current_batch.borrow_mut() = Some(batch);
        Ok(())
    }

    fn end_current_batch(&self) {
        self.current_batch.borrow_mut().take();
    }

    /// A store or drop of `(table, row, key)`: a batch read of it is stale.
    fn forget_current(&self, table: &str, row: RowUuid, key: &[u8]) {
        if let Some(batch) = self.current_batch.borrow_mut().as_mut() {
            batch.remove(&(table.to_owned(), row, key.to_vec()));
        }
    }

    fn current_image(&self, table: &str, row: RowUuid, key: &[u8]) -> Result<Option<Image>, Error> {
        if let Some(batch) = self.current_batch.borrow().as_ref()
            && let Some(image) = batch.get(&(table.to_owned(), row, key.to_vec()))
        {
            return Ok(image.clone());
        }
        let id = row.0.as_bytes().to_vec();
        Ok(self
            .images_where(table, false, "id = ?1 AND _b = ?2", &[&id, &key])?
            .pop())
    }

    fn put_image(&self, image: &Image, history: bool) -> Result<(), Error> {
        if !history {
            self.forget_current(&image.table, RowUuid::from_bytes(image.id), &image.key);
        }
        let mut values = vec![
            Sql::Blob(image.id.to_vec()),
            Sql::Blob(image.key.clone()),
            Sql::Integer(self.code_of(image.by)?),
            Sql::Integer(image.seq as i64),
            Sql::Integer(
                if image.deleted { STATE_DELETED } else { 0 }
                    | if image.content { 0 } else { STATE_NO_CONTENT },
            ),
        ];
        let mut columns = Vec::new();
        for (column, value) in self.stored_cells(image)? {
            columns.push(column);
            values.push(value);
        }
        let mut inserts = self.image_inserts.borrow_mut();
        let (known, sql) = inserts
            .entry((image.table.clone(), history))
            .or_insert_with(|| (Vec::new(), String::new()));
        if sql.is_empty() || *known != columns {
            let name = if history {
                format!("{}__h", image.table)
            } else {
                image.table.clone()
            };
            let names: Vec<String> = ["id", "_b", "_n", "_seq", "_st"]
                .into_iter()
                .map(str::to_owned)
                .chain(columns.iter().map(|column| quote(column)))
                .collect();
            let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
            let verb = if history {
                "INSERT OR IGNORE"
            } else {
                "INSERT"
            };
            *sql = format!(
                "{verb} INTO {} ({}) VALUES ({})",
                quote(&name),
                names.join(", "),
                placeholders.join(", ")
            );
            *known = columns;
        }
        let sql = sql.clone();
        drop(inserts);
        self.exec(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        Ok(())
    }

    /// Drop a row's current version and its history from `from` up.
    /// The dropped image is kept for dispatch: keyed invalidation reads old
    /// images from history, which this removes.
    fn drop_row(&self, table: &str, row: RowUuid, key: &[u8], from: Pos) -> Result<(), Error> {
        if let Some(image) = self.current_image(table, row, key)? {
            self.gone.borrow_mut().push(image);
        }
        self.forget_current(table, row, key);
        let id = row.0.as_bytes().to_vec();
        self.exec(
            &format!("DELETE FROM {} WHERE id = ?1 AND _b = ?2", quote(table)),
            rusqlite::params![id, key],
        )
        .map_err(storage)?;
        self.exec(
            &format!(
                "DELETE FROM {} WHERE id = ?1 AND _b = ?2 AND (_n, _seq) >= (?3, ?4)",
                quote(&format!("{table}__h"))
            ),
            rusqlite::params![id, key, from.n, from.seq as i64],
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
        let at = self.pos(image)?;
        let current = match self.current_image(&image.table, row, &image.key)? {
            Some(current) => Some((self.pos(&current)?, current)),
            None => None,
        };
        match current {
            None => {
                self.put_image(image, false)?;
                self.changed
                    .borrow_mut()
                    .push((image.table.clone(), row, image.key.clone()));
            }
            Some((held, _)) if held == at => {}
            Some((held, _)) if relay_older(held, at) => {
                if self.non_durable {
                    self.put_image(image, true)?;
                    self.changed
                        .borrow_mut()
                        .push((image.table.clone(), row, image.key.clone()));
                }
            }
            Some((held, current)) if held < at || !held.accepted() => {
                // A relay's newer version: its older one stays as history.
                let relay_newer = self.non_durable && !held.accepted() && held < at;
                if held.accepted() || relay_newer {
                    self.put_image(&current, true)?;
                }
                let floor = if relay_newer { at } else { Pos::UNSETTLED };
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
        self.drop_row(table, row, key, Pos::default())
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

    /// The rows own commit `tx` wrote, each as this store holds it at
    /// `tx` and by the commit's first op that writes it, for the authority
    /// to hold (see [`Expected`]). `over`: the rows this replica held a
    /// version of before (settled, or an own earlier pending commit's),
    /// which the commit did not create.
    fn expected_rows(
        &self,
        tx: u64,
        ops: &[StagedOp],
        rows: &[RowKey],
        over: &HashSet<RowKey>,
    ) -> Result<Vec<Expected>, Error> {
        let by_row = self.row_ops(ops);
        let mut expected = Vec::new();
        for row_key @ (table, row, key) in rows {
            let Some(row_ops) = by_row.get(&(table.clone(), *row)) else {
                continue;
            };
            let (Some(&first), Some(&last)) = (row_ops.first(), row_ops.last()) else {
                continue;
            };
            let Some(image) = self.version_image(table, *row, key, self.own_pos(tx))? else {
                continue;
            };
            let clocked = |op: usize| ops[op].updated_at_ms().is_none();
            let created = if over.contains(row_key) {
                Created::Beneath
            } else if clocked(first) {
                Created::Clocked
            } else {
                Created::Explicit
            };
            expected.push(Expected {
                op: first as u32,
                key: key.clone(),
                image,
                created,
                updated_clocked: clocked(last),
            });
        }
        Ok(expected)
    }

    /// The rows of own commit `tx` the server did not ship (`as_written`): its
    /// version at `seq` (sequenced by `by`) is the one this replica wrote, stamped at
    /// `accepted_at`, so that becomes the settled image. Returns those
    /// images, the rows it cannot install so (to ask for whole), and how
    /// many of those are digest mismatches. A row is not installed if its
    /// image is not the authority's version (its digest differs: the
    /// authority's inference missed a difference, such as a stamp by
    /// another author), this store has no image of it as the upstream
    /// names it (written under another schema version, or its catalogue
    /// changed since), or it lacks the row's create stamp. Changes nothing.
    fn as_written_images(
        &self,
        tx: u64,
        seq: u64,
        by: Option<[u8; 16]>,
        accepted_at: Option<u64>,
        as_written: &[AsWritten],
    ) -> Result<(Vec<Image>, Vec<WholeRow>, u64), Error> {
        let client = self.client.as_ref().expect("client");
        let mut images = Vec::new();
        let mut whole = Vec::new();
        let mut mismatched = 0;
        let Some(pending) = client.pending.iter().find(|p| p.tx == tx) else {
            return Ok((images, whole, mismatched));
        };
        for written in as_written {
            let row = pending
                .ops
                .get(written.op as usize)
                .and_then(StagedOp::row)
                .and_then(|row| {
                    pending
                        .rows
                        .iter()
                        .find(|(_, r, key)| *r == row && *key == written.key)
                });
            let Some((table, row, key)) = row else {
                return Err(unsupported(format!(
                    "fate of commit {tx} holds a row of op {} it did not write",
                    written.op
                )));
            };
            let entry = match &pending.expected {
                Some(expected) => {
                    let Some(entry) = expected
                        .iter()
                        .find(|e| e.op == written.op && e.key == written.key)
                    else {
                        return Err(unsupported(format!(
                            "fate of commit {tx} holds a row of op {} it did not expect",
                            written.op
                        )));
                    };
                    let same = echo_digest(&entry.image) == written.digest;
                    mismatched += u64::from(!same);
                    same.then_some(entry)
                }
                None => None,
            };
            let image = match (entry, accepted_at) {
                (Some(entry), Some(at)) if entry.created == Created::Beneath => self
                    .settled_created(table, *row, key)?
                    .map(|created| entry.accepted(at, Some(created))),
                (Some(entry), Some(at)) => Some(entry.accepted(at, None)),
                _ => None,
            };
            match image {
                Some(mut image) => {
                    image.seq = seq;
                    image.by = by;
                    images.push(image);
                }
                None => whole.push((table.clone(), *row.0.as_bytes(), key.clone())),
            }
        }
        Ok((images, whole, mismatched))
    }

    /// The `$createdAt` of a row's settled version beneath pending writes:
    /// the cached baseline (which an earlier fate in the same batch may
    /// just have installed), or the stored one.
    fn settled_created(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<Cell>, Error> {
        let client = self.client.as_ref().expect("client");
        let beneath = match client.baselines.get(&(table.to_owned(), row, key.to_vec())) {
            Some(Some(image)) => Some(image.clone()),
            _ => self.upstream_baseline(table, row, key)?,
        };
        Ok(beneath.and_then(|beneath| {
            beneath
                .cells
                .into_iter()
                .find(|(name, _)| name == STAMPS[0])
                .map(|(_, created)| created)
        }))
    }

    /// The rows a resent commit that landed at `at` wrote: each branch key
    /// its row has now, or had at `at` (since superseded). They ship to
    /// the writer as they are now, with the version at `at` if superseded.
    fn resent_rows(&self, ops: &[StagedOp], at: Pos) -> Result<Vec<RowKey>, Error> {
        let mut out = BTreeSet::new();
        for op in ops {
            let Some(row) = op.row() else { continue };
            let table = op.table();
            let sql = format!(
                "SELECT _b FROM {} WHERE id = ?1 UNION SELECT _b FROM {} WHERE {ID_AT}",
                quote(table),
                quote(&format!("{table}__h"))
            );
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let keys: Vec<Vec<u8>> = statement
                .query_map(
                    rusqlite::params![row.0.as_bytes().to_vec(), at.n, at.seq as i64],
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

    /// The (stored table, id, key) of the rows a commit at `seq` wrote.
    fn written_rows(
        &self,
        ops: &[StagedOp],
        seq: u64,
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, Error> {
        let mut out = BTreeSet::new();
        for op in ops {
            let Some(row) = op.row() else { continue };
            let table = op.table();
            let sql = format!("SELECT _b FROM {} WHERE {ID_AT}", quote(table));
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let keys: Vec<Vec<u8>> = statement
                .query_map(
                    rusqlite::params![row.0.as_bytes().to_vec(), self.me, seq as i64],
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
        if let Some(image) = self.current_image(table, row, key)?
            && self.pos(&image)?.n < self.me
        {
            return Ok(Some(image));
        }
        Ok(self
            .images_where(
                table,
                true,
                "id = ?1 AND _b = ?2 AND _n < ?3 ORDER BY _n DESC, _seq DESC LIMIT 1",
                &[&id, &key, &self.me],
            )?
            .pop())
    }
}

// ---- persisted sync state --------------------------------------------------

/// Sync state lives in the store itself, written in the same SQLite
/// transaction as the rows it describes, so a crash never separates a local
/// write from its upload record, or a server commit from its dedupe entry.
///
/// A pending commit is stored a row per op, not as one encoded message:
/// `_jazz_pending` keeps its `Commit` with its ops and reads left out,
/// `_jazz_pending_items` each op (with its bases, access proof and deltas) and
/// serializable read (by [`PendingPart`]), and `_jazz_pending_rows` the
/// local rows it wrote. Ops name tables and columns by stored names, so a
/// commit is the same whichever schema version wrote it or the store
/// reopens under. Deleting a commit's `_jazz_pending`
/// row once it settles deletes the rest with it.
/// A row's values over the inline limit live in large-value storage, so a
/// stored op is about the size of the row it writes, however large the
/// commit: a store with a small value limit (a Durable Object's 2 MiB)
/// keeps any commit whose rows it can hold, even one that travels in
/// several frames.
///
/// A commit is named by its writer's (node, tx). `_jazz_nodes` interns each
/// node as a small code `n` (and records which identity owns it, see
/// `claim_node`), and `_jazz_transactions` keeps one row per commit this
/// store knows of under that code: (`sn`, `seq`) the position it landed
/// at (`seq` NULL once rejected): an authority seq (`sn` 0), or a relay's
/// own commit carrying it (`sn` the relay's own code); and `result` the
/// fate this store recorded for it as a server (NULL when it only learned
/// where the commit landed).
pub(crate) const SYNC_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_pending (tx INTEGER PRIMARY KEY, header BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_pending_items (tx INTEGER NOT NULL, part INTEGER NOT NULL, n INTEGER NOT NULL, item BLOB NOT NULL, PRIMARY KEY (tx, part, n));\
    CREATE TABLE IF NOT EXISTS _jazz_pending_rows (tx INTEGER NOT NULL, n INTEGER NOT NULL, tbl TEXT NOT NULL, id BLOB NOT NULL, key BLOB NOT NULL, PRIMARY KEY (tx, n));\
    CREATE TRIGGER IF NOT EXISTS _jazz_pending_forget AFTER DELETE ON _jazz_pending BEGIN \
     DELETE FROM _jazz_pending_items WHERE tx = OLD.tx; \
     DELETE FROM _jazz_pending_rows WHERE tx = OLD.tx; END;\
    CREATE TABLE IF NOT EXISTS _jazz_meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_authors (n INTEGER PRIMARY KEY, account BLOB NOT NULL, issuer TEXT NOT NULL, subject TEXT NOT NULL, UNIQUE (account, issuer, subject));\
    CREATE TABLE IF NOT EXISTS _jazz_nodes (n INTEGER PRIMARY KEY, node BLOB NOT NULL UNIQUE, author INTEGER);\
    CREATE TABLE IF NOT EXISTS _jazz_transactions (n INTEGER NOT NULL, tx INTEGER NOT NULL, sn INTEGER NOT NULL DEFAULT 0, seq INTEGER, result BLOB, PRIMARY KEY (n, tx)) WITHOUT ROWID;\
    CREATE INDEX IF NOT EXISTS _jazz_transactions_by_seq ON _jazz_transactions (sn, seq);";

/// The retry store for refused commits, with each one's ops in
/// `_jazz_rejected_ops`. It is created by the first refusal it keeps, not
/// on open, so that opening an existing store without writing leaves the
/// file byte-identical.
const REJECTED_DDL: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS _jazz_rejected \
     (tx INTEGER PRIMARY KEY, error BLOB NOT NULL, serializable INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS _jazz_rejected_ops \
     (tx INTEGER NOT NULL, n INTEGER NOT NULL, op BLOB NOT NULL, PRIMARY KEY (tx, n))",
];

/// Which list of a pending `Commit` a `_jazz_pending_items` row belongs to.
#[derive(Clone, Copy)]
enum PendingPart {
    Op = 0,
    Read = 1,
}

impl PendingPart {
    fn from_stored(part: i64) -> Result<Self, Error> {
        Ok(match part {
            0 => Self::Op,
            1 => Self::Read,
            _ => {
                return Err(crate::format::unsupported(format_args!(
                    "unknown part {part} in _jazz_pending_items"
                )));
            }
        })
    }
}

/// Rows a replica holds no image of because it may not read their current
/// version, by (stored table, id, branch key): in a branch view over a
/// base, each still masks the base's row of the id (ch. 8 §8.4). Created
/// by the first such eviction, so that opening an existing store without
/// one leaves the file byte-identical.
pub(crate) const WITHHELD_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_withheld \
    (tbl TEXT NOT NULL, id BLOB NOT NULL, _b BLOB NOT NULL, PRIMARY KEY (tbl, id, _b)) \
    WITHOUT ROWID";

/// Per remote query, the positions its result was last settled through
/// (see `ActiveRead::through`): one row per sequencer, `n` its code as in
/// `_n` (0 for the authority). Created by the first settlement, so that
/// opening an existing store without one leaves the file byte-identical.
const WATERMARKS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_watermarks \
    (key BLOB NOT NULL, n INTEGER NOT NULL, through INTEGER NOT NULL, \
    PRIMARY KEY (key, n)) WITHOUT ROWID";

/// A remote query's `_jazz_watermarks` key: what it reads, and as whom.
/// How far it reaches (tier and wait) is not part of it: every tier's
/// result settles from the same server state.
fn watermark_key(
    query: &Query,
    binding: &BTreeMap<String, Value>,
    include_deleted: bool,
    view: &ReadViewSourceSpec,
    author: AuthorSubject,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    // A current-state read keeps the key it had before views were part of
    // it, so its resume point survives the upgrade.
    match view {
        ReadViewSourceSpec::Current => {
            hasher.update(&encode(&(query, binding, include_deleted)));
        }
        view => {
            hasher.update(&encode(&(query, binding, include_deleted, view)));
        }
    }
    hasher.update(format!("{author:?}").as_bytes());
    *hasher.finalize().as_bytes()
}

/// Owns a fetch's transient request state, including when its future is cancelled.
struct PendingFetch<'a> {
    db: &'a Db,
    asked: Option<(u64, u64)>,
}

impl Drop for PendingFetch<'_> {
    fn drop(&mut self) {
        if let Some((id, _)) = self.asked
            && let Some(client) = self.db.inner.borrow_mut().client.as_mut()
        {
            client.forget_once(id);
        }
    }
}

type RowKey = (String, RowUuid, Vec<u8>);

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("encode sync state")
}

/// A stored sync record. One written in another layout is refused as an
/// unsupported storage format rather than misread: it must decode exactly,
/// with no bytes left over.
fn decode_stored<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    match postcard::take_from_bytes(bytes) {
        Ok((value, [])) => Ok(value),
        Ok((_, rest)) => Err(crate::format::unsupported(format_args!(
            "a stored sync record has {} trailing bytes",
            rest.len()
        ))),
        Err(error) => Err(crate::format::unsupported(format_args!(
            "a stored sync record does not decode ({error})"
        ))),
    }
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
                    by: via.by,
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
    /// the trees of the settled version each op names as its access proof
    /// (SPEC 19.4). `None` when this replica must first fetch nodes it
    /// lacks (a value copied from a row it holds only partly): they are
    /// asked for, and the commit waits.
    fn pushes_for(
        &mut self,
        commit: &SyncMessage,
    ) -> Result<Option<(Vec<jazz_types::large_values::ContentHash>, Vec<SyncMessage>)>, Error> {
        let SyncMessage::Commit { ops, .. } = commit else {
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
        for op in ops {
            let (row, cells) = match &op.write {
                WireWrite::Insert { row, cells, .. }
                | WireWrite::Update { row, cells, .. }
                | WireWrite::Upsert { row, cells, .. }
                | WireWrite::Restore {
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
            let table = op.table();
            if let Some(proof) = &op.access_proof
                && let Some(image) = self.held_version(table, row, &proof.key, proof.seq)?
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
            by: image.by,
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
        let SyncMessage::Commit {
            id,
            kind,
            ops,
            acting,
        } = commit
        else {
            return Err(Error::new(ErrorCode::Storage, "only a Commit is pending"));
        };
        let (kind, reads) = match kind {
            WireKind::Mergeable => (WireKind::Mergeable, &[][..]),
            WireKind::Serializable {
                snapshot,
                reads,
                own,
            } => (
                WireKind::Serializable {
                    snapshot: snapshot.clone(),
                    reads: Vec::new(),
                    own: own.clone(),
                },
                reads.as_slice(),
            ),
        };
        let header = SyncMessage::Commit {
            id: *id,
            kind,
            ops: Vec::new(),
            acting: acting.clone(),
        };
        self.exec(
            "INSERT INTO _jazz_pending (tx, header) VALUES (?1, ?2)",
            rusqlite::params![tx as i64, encode(&header)],
        )
        .map_err(storage)?;
        let items = (ops.iter().map(encode).map(|item| (PendingPart::Op, item)))
            .chain(reads.iter().map(|item| (PendingPart::Read, encode(item))));
        let mut n = 0i64;
        let mut last = None;
        for (part, item) in items {
            if last != Some(part as i64) {
                last = Some(part as i64);
                n = 0;
            }
            self.exec(
                "INSERT INTO _jazz_pending_items (tx, part, n, item) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![tx as i64, part as i64, n, item],
            )
            .map_err(storage)?;
            n += 1;
        }
        for (n, (table, row, key)) in rows.iter().enumerate() {
            self.exec(
                "INSERT INTO _jazz_pending_rows (tx, n, tbl, id, key) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![tx as i64, n as i64, table, row.0.as_bytes().as_slice(), key],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    /// Where a remote query's result was last settled through.
    fn watermark(&self, key: &[u8; 32]) -> Result<Option<(Sequencer, u64)>, Error> {
        if !self.has_table("_jazz_watermarks")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare_cached("SELECT n, through FROM _jazz_watermarks WHERE key = ?1")
            .map_err(storage)?;
        let rows = stmt
            .query_map([key.as_slice()], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        let mut positions = Vec::with_capacity(rows.len());
        for (n, through) in rows {
            positions.push((self.node_of(n)?, through as u64));
        }
        single_position(&positions)
    }

    fn active_read(
        &self,
        query: Query,
        binding: BTreeMap<String, Value>,
        include_deleted: bool,
        view: ReadViewSourceSpec,
        lifetime: ReadLifetime,
    ) -> ActiveRead {
        let resumable = match &lifetime {
            ReadLifetime::Live(_) => true,
            ReadLifetime::Once { historical } => {
                !historical && matches!(view, ReadViewSourceSpec::Current)
            }
        };
        let watermark = resumable.then(|| {
            watermark_key(
                &query,
                &binding,
                include_deleted,
                &view,
                self.identity.author,
            )
        });
        // A replica that cannot read its watermark opens fresh.
        let through = watermark
            .as_ref()
            .and_then(|key| self.watermark(key).ok().flatten());
        ActiveRead {
            query,
            binding,
            include_deleted,
            view,
            watermark,
            through,
            lifetime,
        }
    }

    /// Register a one-shot request in the same table as live reads.
    fn register_once_read(
        &mut self,
        id: u64,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        view: &ReadViewSourceSpec,
        at: &Positions,
    ) -> Positions {
        let read = self.active_read(
            query.clone(),
            binding.clone(),
            include_deleted,
            view.clone(),
            ReadLifetime::Once {
                historical: !at.is_empty(),
            },
        );
        let since = read.since();
        self.client.as_mut().expect("client").reads.insert(id, read);
        since
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
            for read in client.reads.values_mut() {
                read.through = None;
            }
            client.forgotten = true;
        }
        Ok(())
    }

    /// Record that a remote query's result settled through `through` at
    /// sequencer `by`, replacing where it had settled before.
    fn store_watermark(&self, key: &[u8; 32], by: Sequencer, through: u64) -> Result<(), Error> {
        let n = self.code_of(by)?;
        self.exec(WATERMARKS_DDL, []).map_err(storage)?;
        self.exec(
            "DELETE FROM _jazz_watermarks WHERE key = ?1",
            [key.as_slice()],
        )
        .map_err(storage)?;
        self.exec(
            "INSERT INTO _jazz_watermarks (key, n, through) VALUES (?1, ?2, ?3)",
            rusqlite::params![key.as_slice(), n, through as i64],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Move refused commit `tx`'s payload from the upload queue into the
    /// retry store.
    fn retain_rejected(&self, tx: u64, error: &Error, serializable: bool) -> Result<(), Error> {
        for ddl in REJECTED_DDL {
            self.exec(ddl, []).map_err(storage)?;
        }
        self.exec(
            "INSERT OR REPLACE INTO _jazz_rejected (tx, error, serializable) VALUES (?1, ?2, ?3)",
            rusqlite::params![tx as i64, encode(&wire_error(error)), serializable],
        )
        .map_err(storage)?;
        // A commit refused again replaces its ops whole.
        self.exec("DELETE FROM _jazz_rejected_ops WHERE tx = ?1", [tx as i64])
            .map_err(storage)?;
        self.exec(
            "INSERT INTO _jazz_rejected_ops (tx, n, op) \
             SELECT tx, n, item FROM _jazz_pending_items WHERE tx = ?1 AND part = ?2",
            rusqlite::params![tx as i64, PendingPart::Op as i64],
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
            .prepare_cached("SELECT tx, error, serializable FROM _jazz_rejected ORDER BY tx")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        rows.into_iter()
            .map(|(seq, error, serializable)| {
                let mut statement = self
                    .conn
                    .prepare_cached("SELECT op FROM _jazz_rejected_ops WHERE tx = ?1 ORDER BY n")
                    .map_err(storage)?;
                let ops = statement
                    .query_map([seq as i64], |r| r.get::<_, Vec<u8>>(0))
                    .map_err(storage)?
                    .map(|op| decode_stored(&op.map_err(storage)?))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RejectedTransaction {
                    seq,
                    error: from_wire_error(decode_stored(&error)?),
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
        for table in ["_jazz_rejected", "_jazz_rejected_ops"] {
            self.exec(&format!("DELETE FROM {table} WHERE tx = ?1"), [tx as i64])
                .map_err(storage)?;
        }
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

    /// Forget settled commit `tx`: its items, rows and version go with it
    /// (`_jazz_pending_forget`).
    fn forget_pending(&self, tx: u64) -> Result<(), Error> {
        self.exec("DELETE FROM _jazz_pending WHERE tx = ?1", [tx as i64])
            .map_err(storage)?;
        Ok(())
    }

    /// Pending commit `tx` as `persist_pending` stored it: its header
    /// with its lists filled from `_jazz_pending_items`.
    fn pending_commit(&self, tx: u64, header: &[u8]) -> Result<SyncMessage, Error> {
        let mut commit: SyncMessage = decode_stored(header)?;
        let SyncMessage::Commit { kind, ops, .. } = &mut commit else {
            return Err(crate::format::unsupported(
                "a pending upload is not a Commit",
            ));
        };
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT part, item FROM _jazz_pending_items WHERE tx = ?1 ORDER BY part, n",
            )
            .map_err(storage)?;
        let items = statement
            .query_map([tx as i64], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(storage)?;
        for item in items {
            let (part, item) = item.map_err(storage)?;
            match PendingPart::from_stored(part)? {
                PendingPart::Op => ops.push(decode_stored(&item)?),
                PendingPart::Read => match kind {
                    WireKind::Serializable { reads, .. } => reads.push(decode_stored(&item)?),
                    WireKind::Mergeable => {
                        return Err(crate::format::unsupported(
                            "a mergeable pending commit has reads",
                        ));
                    }
                },
            }
        }
        Ok(commit)
    }

    /// The local rows pending commit `tx` wrote, in order.
    fn pending_rows(&self, tx: u64) -> Result<Vec<RowKey>, Error> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT tbl, id, key FROM _jazz_pending_rows WHERE tx = ?1 ORDER BY n")
            .map_err(storage)?;
        let rows = statement
            .query_map([tx as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(storage)?;
        rows.map(|row| {
            let (table, id, key) = row.map_err(storage)?;
            let id: [u8; 16] = id
                .try_into()
                .map_err(|_| crate::format::unsupported("bad row id in _jazz_pending_rows"))?;
            Ok((table, RowUuid::from_bytes(id), key))
        })
        .collect()
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

    pub(super) fn persist_meta(&self, key: &str, value: u64) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_meta (k, v) VALUES (?1, ?2)",
            rusqlite::params![key, encode(&value)],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// The namespace of the authority whose catalogue this store keeps:
    /// minted once by an authority with no upstream, mirrored by every
    /// replica of it (see [`SyncMessage::Catalogue`]); `None` on a store
    /// that has received none yet. Stored identities are derived from it
    /// (see [`crate::names::identity`]), so two apps' tables never share one.
    pub(super) fn namespace(&self) -> Result<Option<[u8; 16]>, Error> {
        let stored = match self.conn.cached_row(
            "SELECT v FROM _jazz_meta WHERE k = ?1",
            [NAMESPACE_META],
            |r| r.get::<_, Vec<u8>>(0),
        ) {
            Ok(bytes) => Some(
                bytes
                    .try_into()
                    .map_err(|_| Error::new(ErrorCode::Storage, "bad namespace in _jazz_meta"))?,
            ),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(error) => return Err(storage(error)),
        };
        if stored.is_some() || !self.publishes_catalogue() {
            return Ok(stored);
        }
        let minted = *uuid::Uuid::new_v4().as_bytes();
        self.persist_namespace(minted)?;
        Ok(Some(minted))
    }

    pub(super) fn persist_namespace(&self, namespace: [u8; 16]) -> Result<(), Error> {
        self.exec(
            "INSERT OR REPLACE INTO _jazz_meta (k, v) VALUES (?1, ?2)",
            rusqlite::params![NAMESPACE_META, namespace.to_vec()],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Whether this store's catalogue is its own: an authority with no
    /// upstream. Every version it admits is confirmed; a replica's are
    /// confirmed by the `Catalogue` it mirrors.
    pub(super) fn publishes_catalogue(&self) -> bool {
        self.authority && self.client.is_none()
    }

    /// Whether `version`'s names are the authority's.
    pub(super) fn names_confirmed(&self, version: &crate::ids::SchemaVersionId) -> bool {
        if self.publishes_catalogue() {
            self.names.get(version).is_some()
        } else {
            self.names.confirmed(version)
        }
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

    pub(crate) fn applied_fate(
        &self,
        node: [u8; 16],
        tx: u64,
    ) -> Result<Option<Result<(), WireError>>, Error> {
        match self.conn.cached_row(
            "SELECT t.result FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
             WHERE n.node = ?1 AND t.tx = ?2 AND t.result IS NOT NULL",
            rusqlite::params![node.to_vec(), tx as i64],
            |r| r.get::<_, Vec<u8>>(0),
        ) {
            // An unreadable fate is a store this build cannot read, not an
            // unseen commit: treating it as unseen would authorize a resend
            // afresh.
            Ok(bytes) => decode_stored(&bytes).map(Some),
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
        let code = self.node_code(node)?;
        self.exec(
            "UPDATE _jazz_nodes SET author = ?2 WHERE n = ?1 AND author IS NULL",
            rusqlite::params![code, author],
        )
        .map_err(storage)?;
        let owner: i64 = self
            .conn
            .cached_row("SELECT author FROM _jazz_nodes WHERE n = ?1", [code], |r| {
                r.get(0)
            })
            .map_err(storage)?;
        Ok(owner == author)
    }

    /// Whether `node` is one `identity` writes as (see [`Inner::claim_node`]),
    /// without claiming it.
    pub(super) fn writes_as(
        &self,
        node: [u8; 16],
        identity: &AuthorSubject,
    ) -> Result<bool, Error> {
        let author = self.author_code(&author_key(identity))?;
        match self.conn.cached_row(
            "SELECT author FROM _jazz_nodes WHERE node = ?1",
            [node.to_vec()],
            |r| r.get::<_, Option<i64>>(0),
        ) {
            Ok(owner) => Ok(owner == Some(author)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }

    /// Whether the commit that landed at `at` was written as a node that
    /// belongs to `identity` (see [`Inner::claim_node`]).
    fn committed_by(&self, at: Pos, identity: &AuthorSubject) -> Result<bool, Error> {
        let author = self.author_code(&author_key(identity))?;
        match self.conn.cached_row(
            "SELECT n.author FROM _jazz_transactions AS t JOIN _jazz_nodes AS n \
             ON n.n = t.n WHERE t.sn = ?1 AND t.seq = ?2",
            [at.n, at.seq as i64],
            |r| r.get::<_, Option<i64>>(0),
        ) {
            Ok(owner) => Ok(owner == Some(author)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }

    /// `node`'s code in `_jazz_nodes`, interning it on first use.
    /// This store's own node code as a replica: a relay's, or a
    /// non-durable client's (see [`RELAY`]).
    pub(crate) fn own_code(&self) -> Result<i64, Error> {
        let role = if self.non_durable { LEAF } else { RELAY };
        Ok(role | self.node_code(*self.identity.node.as_bytes())?)
    }

    /// The newest seq this replica's node numbered a version of its own at.
    fn own_high_water(&self) -> Result<u64, Error> {
        let mut seq = 0u64;
        for table in self.physical.tables.keys() {
            for name in [table.clone(), format!("{table}__h")] {
                let max: Option<i64> = self
                    .conn
                    .cached_row(
                        &format!("SELECT MAX(_seq) FROM {} WHERE _n = ?1", quote(&name)),
                        [self.me],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                seq = seq.max(max.unwrap_or(0) as u64);
            }
        }
        Ok(seq)
    }

    /// The code an image's version is at (`_n`): 0 for an accepted
    /// version, else the code of the node that numbered it.
    pub(crate) fn code_of(&self, by: Option<[u8; 16]>) -> Result<i64, Error> {
        match by {
            None => Ok(0),
            Some(node) if node == *self.identity.node.as_bytes() && self.me != 0 => Ok(self.me),
            Some(node) => Ok(RELAY | self.node_code(node)?),
        }
    }

    /// Where an image's version is.
    pub(crate) fn pos(&self, image: &Image) -> Result<Pos, Error> {
        Ok(Pos {
            n: self.code_of(image.by)?,
            seq: image.seq,
        })
    }

    /// The node a stored version's code names (`None` for the authority).
    pub(crate) fn node_of(&self, code: i64) -> Result<Option<[u8; 16]>, Error> {
        if code == 0 {
            return Ok(None);
        }
        let node: Vec<u8> = self
            .conn
            .cached_row(
                "SELECT node FROM _jazz_nodes WHERE n = ?1",
                [node_index(code)],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(Some(node.try_into().map_err(|_| {
            Error::new(ErrorCode::Storage, "a stored node id is not 16 bytes")
        })?))
    }

    /// [`Self::code_of`] without interning: `None` for a node this store
    /// does not know.
    pub(crate) fn known_code(&self, by: Option<[u8; 16]>) -> Result<Option<i64>, Error> {
        let node = match by {
            None => return Ok(Some(0)),
            Some(node) if node == *self.identity.node.as_bytes() && self.me != 0 => {
                return Ok(Some(self.me));
            }
            Some(node) => node,
        };
        let code: Option<i64> = self
            .conn
            .cached_row(
                "SELECT n FROM _jazz_nodes WHERE node = ?1",
                [node.to_vec()],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(code.map(|code| RELAY | code))
    }

    fn node_code(&self, node: [u8; 16]) -> Result<i64, Error> {
        self.exec(
            "INSERT OR IGNORE INTO _jazz_nodes (node) VALUES (?1)",
            [node.to_vec()],
        )
        .map_err(storage)?;
        self.conn
            .cached_row(
                "SELECT n FROM _jazz_nodes WHERE node = ?1",
                [node.to_vec()],
                |r| r.get(0),
            )
            .map_err(storage)
    }

    /// Record the cross-shard part `commit` refused, as its decider's
    /// abort decides it: a resend of it gets this fate.
    pub(crate) fn record_aborted(&self, commit: crate::db::shard::CommitRef) -> Result<(), Error> {
        self.record_fate(
            commit.node,
            commit.tx,
            &Err(WireError::Rejected {
                reason: None,
                message: "its cross-shard commit was decided aborted: retry it as a new commit"
                    .into(),
            }),
        )?;
        self.record_settled(commit.node, commit.tx, None)
    }

    fn record_fate(
        &self,
        node: [u8; 16],
        tx: u64,
        result: &Result<(), WireError>,
    ) -> Result<(), Error> {
        let code = self.node_code(node)?;
        self.exec(
            "INSERT INTO _jazz_transactions (n, tx, result) VALUES (?1, ?2, ?3) \
             ON CONFLICT (n, tx) DO UPDATE SET result = excluded.result",
            rusqlite::params![code, tx as i64, encode(result)],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Where a commit landed: `Some(at)` once accepted (in the authority's
    /// order, or a relay's own commit carrying it), `None` once rejected.
    /// Kept by the authority for every upload and by a replica for its
    /// own, so snapshots that name individual commits stay readable after
    /// they settle.
    fn record_settled(&self, node: [u8; 16], tx: u64, at: Option<Pos>) -> Result<(), Error> {
        let code = self.node_code(node)?;
        self.exec(
            "INSERT INTO _jazz_transactions (n, tx, sn, seq) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (n, tx) DO UPDATE SET sn = excluded.sn, seq = excluded.seq",
            rusqlite::params![
                code,
                tx as i64,
                at.map_or(0, |at| at.n),
                at.map(|at| at.seq as i64)
            ],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// The sessions' commits this store's own commit `seq` carries, as it
    /// accepted them through a relay.
    fn relayed_origins(&self, seq: u64) -> Result<Vec<([u8; 16], u64)>, Error> {
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT n.node, t.tx FROM _jazz_transactions AS t JOIN _jazz_nodes AS n \
                 ON n.n = t.n WHERE t.sn = ?1 AND t.seq = ?2",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map(rusqlite::params![self.me, seq as i64], |r| {
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

    /// Where `node`'s commit `tx` landed: `Some(Some(position))`, rejected
    /// (`Some(None)`), or unknown here (`None`). A row with no `seq` reads as
    /// rejected: every `record_fate` is paired with a `record_settled` in the
    /// same transaction, so an accepted commit's row always has one.
    fn settled(&self, node: [u8; 16], tx: u64) -> Result<Option<Option<Pos>>, Error> {
        match self.conn.cached_row(
            "SELECT t.sn, t.seq FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
             WHERE n.node = ?1 AND t.tx = ?2",
            rusqlite::params![node.to_vec(), tx as i64],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?)),
        ) {
            Ok((n, seq)) => Ok(Some(seq.map(|seq| Pos { n, seq: seq as u64 }))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// Where each of `own` (this replica's commits unsettled at a
    /// serializable snapshot) has since landed.
    pub(crate) fn own_landings(&self, own: &[u64]) -> Result<HashMap<u64, Pos>, Error> {
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
    pub(crate) fn serializable_cut(&self, snapshot: Pos, own: &[u64]) -> Result<Cut, Error> {
        let mut cut = Cut::at(snapshot);
        let me = *self.identity.node.as_bytes();
        for &tx in own {
            match self.settled(me, tx)? {
                Some(Some(at)) => cut.extra.push(at),
                Some(None) => {}
                None => cut.extra.push(Pos {
                    n: self.me,
                    seq: tx,
                }),
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

    /// Whether a relay owes session `index` a position its upstream
    /// moved: the connection's (`at`), once the session was told one, or
    /// a settled result's upstream read's.
    fn owes_positions(&self, index: usize, at: (Option<[u8; 16]>, u64)) -> bool {
        let session = &self.sessions[index];
        let Some(client) = &self.client else {
            return false;
        };
        session.connection_settled.is_some_and(|sent| sent != at)
            || session.settles.values().any(|settle| {
                settle
                    .sent
                    .is_some_and(|sent| sent < client.settled_through(&settle.key))
            })
    }

    /// The seq this store tells its sessions it is complete through. A
    /// relay tells them the authority's seq it knows (its own local seqs
    /// number nothing the authority can check), so a session's snapshot
    /// is one the authority validates against as it is.
    pub(crate) fn served_through(&self) -> u64 {
        match &self.client {
            Some(client) if self.relays_upstream() => client.known,
            // A shard's held position may still take a cross-shard part.
            _ => match self.shard.lowest_hold() {
                Some(held) => self.seq.min(held - 1),
                None => self.seq,
            },
        }
    }

    /// The node whose positions [`Self::served_through`] counts, when not
    /// the authority's: a replica's with no server.
    pub(crate) fn served_by(&self) -> Option<[u8; 16]> {
        (self.me != 0 && !self.relays_upstream()).then(|| *self.identity.node.as_bytes())
    }

    /// Position `seq` as [`Self::served_through`] counts it: the authority's,
    /// or this replica's own when it has no server (see [`Self::served_by`]).
    pub(crate) fn served_at(&self, seq: u64) -> Pos {
        Pos {
            n: if self.served_by().is_some() {
                self.me
            } else {
                0
            },
            seq,
        }
    }

    /// The read a replica runs for `read` over stored `tables`. Its
    /// upstream settles each read apart (SPEC 8 §8.4): a live result is
    /// settled as far as its own read's settle, and the connection as far
    /// as `known`. While the store holds another writer's accepted versions
    /// of those tables past that position, the read is a cut at it: its
    /// rows are as of the position, never part of a commit the upstream has
    /// not settled for it, plus this replica's own writes (unsettled, or
    /// accepted past the position). Otherwise it reads the current state.
    pub(crate) fn visible_read<'r>(
        &self,
        read: &'r ReadSpec,
        tables: &BTreeSet<String>,
    ) -> std::borrow::Cow<'r, ReadSpec> {
        use std::borrow::Cow;
        let Some(client) = &self.client else {
            return Cow::Borrowed(read);
        };
        // A frozen read is its own cut; a branch view reads its branches
        // current (known limit).
        if read.position == Position::Current
            || client.no_server()
            || read.cut.is_some()
            || read.view.is_some()
        {
            return Cow::Borrowed(read);
        }
        let known = Pos {
            n: client.known_n,
            seq: client.known,
        };
        let through = match &read.position {
            Position::Remote(key) => Pos {
                n: known.n,
                seq: client.settled_through(key),
            },
            _ => known,
        };
        let past = tables.iter().any(|table| {
            client
                .newest_foreign
                .get(table)
                .is_some_and(|&newest| newest > through.seq)
        });
        if !past {
            return Cow::Borrowed(read);
        }
        let mut cut = Cut::at(through);
        cut.extra = client
            .own_accepted
            .range((through.seq + 1, 0)..)
            .map(|&(seq, _)| Pos { n: through.n, seq })
            .collect();
        cut.unsettled = !read.settled_only;
        Cow::Owned(ReadSpec {
            cut: Some(cut),
            ..read.clone()
        })
    }

    /// The position this store reads as complete through: the authority's
    /// seq it synced, or for a replica with no server its own newest.
    pub(crate) fn frontier(&self) -> Pos {
        match &self.client {
            Some(client) if client.no_server() => Pos {
                n: self.me,
                seq: self.seq,
            },
            Some(client) => Pos {
                n: client.known_n,
                seq: client.known,
            },
            None => Pos::at(self.seq),
        }
    }

    /// Advance what this replica knows it is complete through to
    /// `through`, counted at `by`'s node (the authority's when `None`).
    fn learn_known(&mut self, through: u64, by: Option<[u8; 16]>) -> Result<Pos, Error> {
        let at = Pos {
            n: self.code_of(by)?,
            seq: through,
        };
        let client = self.client.as_mut().expect("client");
        // Another sequencer (the upstream gained or lost a server) restarts
        // the count; the same one only moves forward.
        if at.n != client.known_n || at.seq > client.known {
            client.known_n = at.n;
            client.known = at.seq;
        }
        Ok(at)
    }

    /// The catalogue session `index` is owed: the admitted versions it
    /// keeps no names for, or other names for. Its first, after a `Join`,
    /// is always owed: it rides in the `Welcome`.
    fn catalogue_for(&mut self, index: usize) -> Result<Option<CatalogueUpdate>, Error> {
        if !std::mem::take(&mut self.sessions[index].catalogue_owed) {
            return Ok(None);
        }
        if self.sessions[index].known_catalogue.is_none() {
            return Ok(None);
        }
        let namespace = self.namespace()?;
        let confirmed: Vec<([u8; 16], [u8; 16])> = if self.publishes_catalogue() {
            self.names
                .in_order()
                .into_iter()
                .map(|(version, names)| (*version.as_bytes(), crate::names::names_digest(names)))
                .collect()
        } else {
            self.names.confirmations()
        };
        let mut versions = Vec::new();
        let mut sent = Vec::new();
        for (version, names) in self.names.in_order() {
            let digest = crate::names::names_digest(names);
            let bytes = *version.as_bytes();
            let known_catalogue = self.sessions[index]
                .known_catalogue
                .as_ref()
                .expect("known catalogue");
            if known_catalogue.get(&bytes) == Some(&digest) {
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
        let told = (namespace, confirmed);
        if versions.is_empty() && self.sessions[index].told.as_ref() == Some(&told) {
            return Ok(None);
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
        let known_catalogue = self.sessions[index]
            .known_catalogue
            .get_or_insert_with(HashMap::new);
        known_catalogue.extend(sent);
        let (namespace, confirmed) = told.clone();
        self.sessions[index].told = Some(told);
        Ok(Some(CatalogueUpdate {
            namespace,
            versions,
            cells,
            confirmed,
        }))
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
        read: &ReadSpec,
        support: &[Support],
        since: u64,
        claims: bool,
    ) -> Result<Option<Resume>, Error> {
        let query = &prepared.query;
        if self.client.is_some()
            || claims
            || read.cut.is_some()
            || since > self.served_through()
            || crate::db::is_windowed(query)
            || query.flat_join.is_some()
            || query.aggregate.is_some()
            || query.relation.is_some()
            || is_history_table(&query.table)
            || crate::catalogue::activated_seq(&self.conn)?.is_some_and(|at| at >= since)
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
            // Any version after the authority's `since`: a newer accepted
            // one, or an unsettled one.
            let newer: Option<i64> = self
                .conn
                .cached_row(
                    &format!(
                        "SELECT 1 FROM {} WHERE (_n, _seq) > (0, ?1) LIMIT 1",
                        quote(table)
                    ),
                    [since as i64],
                    |r| r.get(0),
                )
                .optional()
                .map_err(storage)?;
            if newer.is_some() {
                moved = true;
                break;
            }
        }
        // A branch view's prior result reads its head (and live base) as
        // of `since`; a frozen base keeps its own cut.
        let prior = if moved {
            let read = ReadSpec {
                cut: Some(Cut::at(Pos::at(since))),
                ..read.clone()
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

    /// A session's serializable commit as this relay uploads it, under
    /// the session's id: its own commits stay the session's, and a read of
    /// this relay's unsettled version names the commit that wrote it (the
    /// authority places each where it settled). Its snapshot and every
    /// other read version are already the authority's seqs
    /// ([`Self::served_through`]).
    fn relayed_serializable(
        &self,
        snapshot: u64,
        by: Option<[u8; 16]>,
        reads: Vec<WireRead>,
        own: Vec<u64>,
    ) -> Result<Kind, Error> {
        let me = *self.identity.node.as_bytes();
        let pending = &self.client.as_ref().expect("a relay is a client").pending;
        let named = reads
            .into_iter()
            .map(|read| {
                let rows = read
                    .rows
                    .into_iter()
                    .map(|(row, by, seq)| match (by, pending.upload_id(seq)) {
                        (Some(node), Some(id)) if node == me => {
                            (row, Some(*id.node.as_bytes()), id.time.0)
                        }
                        _ => (row, by, seq),
                    })
                    .collect();
                WireRead { rows, ..read }
            })
            .collect();
        Ok(Kind::Serializable {
            snapshot,
            by,
            reads: named,
            own,
        })
    }

    /// The own commits a replica's serializable transaction began over:
    /// those still unsettled (none on the authority), and those accepted
    /// past the position it knows.
    pub(crate) fn unsettled_own(&self) -> Vec<u64> {
        self.client.as_ref().map_or_else(Vec::new, |client| {
            client
                .pending
                .iter()
                .map(|p| p.tx)
                .chain(client.own_accepted.iter().map(|&(_, tx)| tx))
                .collect()
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
        let mut cut = Cut::at(Pos::at(snapshot.global_base.0));
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
                    cut.extra.push(Pos {
                        n: self.me,
                        seq: tx,
                    });
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
        // This replica's counter numbers its own commits: it resumes after
        // its newest unsettled version. A settled upload's rows now carry
        // server seqs, so the stored versions no longer show the seqs
        // handed out. The fates recorded for this node do: the next commit
        // continues after them, or it would reuse a transaction id the
        // authority dedupes away.
        let me = *self.identity.node.as_bytes();
        self.me = self.own_code()?;
        self.seq = self.own_high_water()?;
        let settled: Option<i64> = self
            .conn
            .cached_row(
                "SELECT MAX(t.tx) FROM _jazz_transactions AS t JOIN _jazz_nodes AS n \
                 ON n.n = t.n WHERE n.node = ?1",
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
        client.stranded = self.meta("stranded")?.is_some();
        client.known = self.meta("known")?.unwrap_or(0);
        client.persisted_known = client.known;
        let mut statement = self
            .conn
            .prepare("SELECT tx, header FROM _jazz_pending ORDER BY tx")
            .map_err(storage)?;
        let pending = statement
            .query_map([], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        drop(statement);
        for (tx, header) in pending {
            let commit = self.pending_commit(tx, &header)?;
            let rows = self.pending_rows(tx)?;
            let SyncMessage::Commit {
                id,
                ops,
                acting,
                kind,
            } = &commit
            else {
                continue;
            };
            let id = *id;
            let serializable = matches!(kind, WireKind::Serializable { .. });
            let mut ops = ops
                .iter()
                .cloned()
                .map(from_wire)
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(author) = acting.author().map(AuthorSubject::from_canonical) {
                let author = author.map_err(|e| {
                    crate::format::unsupported(format_args!("a pending commit's author: {e}"))
                })?;
                let session = matches!(acting, Acting::Session { .. });
                ops.iter_mut().for_each(|op| op.attribute(author, session));
            }
            // Its rows as this store holds them, each at the commit's local
            // seq (the current version, or history under a later pending
            // write), as it held them when it sent it.
            let mut over = HashSet::new();
            for row_key @ (table, row, key) in &rows {
                if client.pending.newest(row_key).is_some()
                    || self.upstream_baseline(table, *row, key)?.is_some()
                {
                    over.insert(row_key.clone());
                }
            }
            let expected = Some(self.expected_rows(tx, &ops, &rows, &over)?);
            client.pending.push(Pending {
                tx,
                id,
                serializable,
                ops,
                rows,
                expected,
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
        self.restore_positions(&mut client)?;
        self.client = Some(client);
        Ok(())
    }

    /// What reads by position need of a reopened store (SPEC 8 §8.4): the
    /// newest accepted version each table holds, and this replica's own
    /// commits accepted past what it knows of its upstream, from its
    /// settled-transaction records. Rows the store kept from before are
    /// then read as of `known` until the upstream settles them again.
    fn restore_positions(&self, client: &mut Client) -> Result<(), Error> {
        // A store that wrote before it became a replica holds its own
        // versions at the authority's numbering, and never syncs them.
        let tables = if client.stranded {
            Vec::new()
        } else {
            self.physical.tables.keys().collect()
        };
        for table in tables {
            let newest: Option<i64> = self
                .conn
                .cached_row(
                    &format!("SELECT MAX(_seq) FROM {} WHERE _n = 0", quote(table)),
                    [],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            if let Some(newest) = newest {
                client.newest_foreign.insert(table.clone(), newest as u64);
            }
        }
        let node = self.node_code(*self.identity.node.as_bytes())?;
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT seq, tx FROM _jazz_transactions \
                 WHERE n = ?1 AND sn = 0 AND seq > ?2",
            )
            .map_err(storage)?;
        let own = statement
            .query_map([node, client.known as i64], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        client.own_accepted.extend(own);
        Ok(())
    }
}

impl Client {
    fn forget_once(&mut self, id: u64) {
        self.answers.remove(&id);
        self.reads.remove(&id);
        self.outbox.retain(|message| {
            !matches!(message, SyncMessage::Read { id: read, span: ReadSpan::Once { .. }, .. } if *read == id)
        });
    }

    /// These requests belonged to the old link. Live fetch futures and
    /// relayed reads re-register when retried; fire-and-forget refreshes end.
    /// Persisted watermarks describe the cache, so they remain reusable.
    fn clear_once_requests(&mut self) {
        self.reads.retain(|_, read| read.live().is_some());
        self.outbox.retain(|message| {
            !matches!(
                message,
                SyncMessage::Read {
                    span: ReadSpan::Once { .. },
                    ..
                }
            )
        });
    }

    /// The local seq of commit `id` this store uploads: its own (its
    /// node's counter is its local seq), or a session's it carries.
    fn local_tx(&self, id: &TxId) -> Option<u64> {
        if *id.node.as_bytes() == self.node {
            return Some(id.time.0);
        }
        self.pending.iter().find(|p| p.id == *id).map(|p| p.tx)
    }

    /// How far the live read `key` is settled: its own settle, or the
    /// connection's position when that is further.
    fn settled_through(&self, key: &str) -> u64 {
        let visible = self
            .live_reads
            .get(key)
            .and_then(|id| self.reads.get(id))
            .and_then(|read| match &read.lifetime {
                ReadLifetime::Live(live) => live.visible,
                ReadLifetime::Once { .. } => None,
            });
        match visible {
            Some(at) if at.n == self.known_n => at.seq.max(self.known),
            _ => self.known,
        }
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
            transport: None,
            hint: Rc::new(StdCell::new(Link {
                hint: RemoteLinkHint::NoServer,
                since: Instant::now(),
                reported: false,
                detached: false,
            })),
            next_id: 1,
            next_advice: fresh_advice_base(),
            reads: HashMap::new(),
            live_reads: HashMap::new(),
            outbox: Vec::new(),
            pending: PendingQueue::default(),
            unsent: Vec::new(),
            fates: HashMap::new(),
            local_fates: HashSet::new(),
            refused: Vec::new(),
            stored: HashSet::new(),
            baselines: HashMap::new(),
            known: 0,
            known_n: 0,
            persisted_known: 0,
            own_accepted: BTreeSet::new(),
            newest_foreign: HashMap::new(),
            visibility_moved: false,
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
            stranded: false,
            uncatalogued: None,
            catalogued: false,
            whole: HashMap::new(),
            deferred: VecDeque::new(),
            releasing: None,
            forgotten: false,
        }
    }
}

// ---- client ---------------------------------------------------------------

/// The refusal of a commit written under a schema version the server has
/// not deployed (INV-SYNC-52).
fn schema_version_not_deployed() -> Error {
    Error {
        code: ErrorCode::WriteRejected,
        reason: Some(Reason::SchemaVersionNotDeployed),
        message: "this write was made through a schema version the server has not deployed: \
                  deploy the schema before releasing the app that writes through it"
            .to_owned(),
    }
}

/// Why a store that committed writes before it became a replica refuses
/// every upstream: those commits sit at seqs its upstream numbers its own
/// versions by, and were never recorded for upload, so syncing would lose
/// them to the upstream's rows (#4443). They stay local, and so does what
/// it writes after.
fn stranded() -> Error {
    Error::new(
        ErrorCode::Protocol,
        "this store committed writes before it became a replica, and uploading them is \
         not supported yet: a store that will connect upstream becomes a replica \
         (`become_replica`, or a remote link hint) before it writes",
    )
}

impl Db {
    /// Connect this database to its server. Writes from now on upload, and
    /// subscriptions attach remotely.
    pub async fn connect_upstream(&self, transport: Box<dyn Transport>) -> Upstream {
        self.become_replica();
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut().expect("client");
        // Detaching already ended the one-shot requests sent on the last
        // link; those queued since were never sent and go out on this one.
        if client.transport.is_some() {
            client.clear_once_requests();
        }
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
        for remote in client.reads.values().filter_map(ActiveRead::live) {
            remote.refused.borrow_mut().take();
        }
        let resubscribe: Vec<SyncMessage> = client
            .reads
            .iter()
            .filter_map(|(&id, read)| {
                let live = read.live()?;
                Some(SyncMessage::Read {
                    id,
                    query: Box::new(read.query.clone()),
                    binding: read.binding.clone(),
                    include_deleted: read.include_deleted,
                    tier: live.reach.tier,
                    wait: live.reach.wait,
                    view: Box::new(read.view.clone()),
                    since: read.since(),
                    span: ReadSpan::Live,
                })
            })
            .collect();
        // Commits, their chunk pushes and chunk requests a full queue held
        // back are sent again below from what is still owed.
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::Read {
                    span: ReadSpan::Live,
                    ..
                } | SyncMessage::Join { .. }
                    | SyncMessage::ResendWhole { .. }
                    | SyncMessage::Commit { .. }
                    | SyncMessage::PushChunks { .. }
                    | SyncMessage::ChunkRequests { .. }
            )
        });
        let version = *inner.owner_version.as_bytes();
        // The versions whose names this store keeps and whose schema it
        // holds: the server sends those it stores otherwise, or it has and
        // this store lacks. One only an earlier process admitted is sent
        // again, so this one holds it and can serve its own sessions by it.
        let known = inner
            .names
            .in_order()
            .into_iter()
            .filter(|(version, _)| inner.deployed(*version))
            .map(|(version, names)| (*version.as_bytes(), crate::names::names_digest(names)))
            .collect();
        let client = inner.client.as_mut().expect("client");
        client
            .outbox
            .insert(0, SyncMessage::Join { version, known });
        // What the old session could not answer, the new one may.
        client.unavailable.clear();
        client.outbox.extend(resubscribe);
        // Rows still owed whole were asked of the old session.
        if !client.whole.is_empty() {
            let rows = client
                .whole
                .iter()
                .map(|((table, id, key), seq)| (table.clone(), *id, key.clone(), *seq))
                .collect();
            client.outbox.push(SyncMessage::ResendWhole { rows });
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
        // Re-read checkpoints: cache eviction since the original request
        // can invalidate a relayed request's previously chosen floor.
        for mut ask in reask {
            if let SyncMessage::Read {
                id,
                query,
                binding,
                include_deleted,
                view,
                since,
                span: ReadSpan::Once { at },
                ..
            } = &mut ask
            {
                *since = inner.register_once_read(*id, query, binding, *include_deleted, view, at);
            }
            inner.client.as_mut().expect("client").outbox.push(ask);
        }
        let client = inner.client.as_mut().expect("client");
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
        client.catalogued = false;
        client.connection += 1;
        let connection = client.connection;
        if client.stranded {
            inner.refuse_upstream(stranded());
        }
        Upstream(connection)
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
        read.position = Position::Current;
        // A frozen view is this store's own state, not the server's to judge.
        if read.frozen() {
            return Ok(());
        }
        read.settled_only = true;
        read.cut = Some(crate::compile::Cut::at(Pos::at(through)));
        let (table, head, base) = {
            let inner = self.inner.borrow();
            let schema = inner.schema.clone();
            let table = schema.table(&prepared.query.table)?;
            let (head, base) = match &read.view {
                Some(view) if !table.branch_by.is_empty() => (
                    Some(table.selector_key(&view.head)?),
                    view.base
                        .as_ref()
                        .map(|base| table.selector_key(base))
                        .transpose()?,
                ),
                _ => (None, None),
            };
            (table.stored.clone(), head, base)
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
            let mut ask = Vec::new();
            for &row in &stale {
                let id = row.0.as_bytes().to_vec();
                for image in inner.images_where(&table, false, "id = ?1", &[&id])? {
                    let overlaid = inner.client.as_ref().is_some_and(|client| {
                        client
                            .baselines
                            .contains_key(&(table.clone(), row, image.key.clone()))
                    });
                    if image.by.is_some() || image.seq > through || overlaid {
                        continue;
                    }
                    if head.as_ref().is_none_or(|head| *head == image.key) {
                        evict.push((row, image.key));
                    } else if let (Some(head), Some(base)) = (&head, &base)
                        && *base == image.key
                    {
                        // An inherited row the answer leaves out: the head
                        // masks it (a tombstone, or a version the read
                        // leaves out) or the base row changed. The base row
                        // may be another read's, so it is not evicted: the
                        // upstream sends the head's and the base's row as
                        // they are, or their evictions.
                        ask.push((table.clone(), *row.0.as_bytes(), head.clone()));
                        ask.push((table.clone(), *row.0.as_bytes(), image.key));
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
            for named in ask {
                inner.ask_whole(named, 0)?;
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

    /// Become a replica: its versions from here on are its own node's
    /// (ordered after every server version), and commits are owed to an
    /// upstream (uploaded once one connects). A store that will connect
    /// later becomes one before it writes: what it committed before sits at
    /// the authority's positions, which its upstream numbers its own versions
    /// by, and was never recorded for upload, so such a store keeps it and
    /// refuses every upstream (see [`stranded`]).
    pub fn become_replica(&self) {
        let mut inner = self.inner.borrow_mut();
        if inner.client.is_some() {
            return;
        }
        let stranded = !inner.authority && inner.holds_rows().expect("read the store's tables");
        inner.me = inner.own_code().expect("intern this replica's node");
        let node = *inner.identity.node.as_bytes();
        let mut client = Client::new(node);
        client.stranded = stranded;
        inner.client = Some(client);
        if stranded {
            inner
                .persist_meta("stranded", 1)
                .expect("persist stranded flag");
        }
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
    /// `ops` name tables and columns as the schema the write ran under
    /// (`Inner::schema`: the owner's, or the view's that wrote them); they
    /// upload by stored names (see [`Inner::stored_op`]), so no schema
    /// version travels with the commit.
    pub(crate) fn record_upload(
        &self,
        ops: Vec<StagedOp>,
        seq: u64,
        kind: Kind,
        relayed: Option<Relayed>,
    ) -> Result<(), Error> {
        let ops = {
            let mut inner = self.inner.borrow_mut();
            if inner.client.is_none() {
                return Ok(());
            }
            let schema = inner.schema.clone();
            let ops = ops
                .into_iter()
                .map(|op| inner.stored_op(op, &schema))
                .collect::<Result<Vec<_>, _>>()?;
            if inner.tx.is_some() {
                inner.capture.extend(ops);
                return Ok(());
            }
            ops
        };
        self.record_stored_upload(ops, seq, kind, relayed)
    }

    /// [`Db::record_upload`] of ops already named by stored names.
    pub(crate) fn record_stored_upload(
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
        // Its ops are looked up by the names they travel by.
        let view = inner.stored_view();
        let active = std::mem::replace(&mut inner.schema, view);
        drop(inner);
        let recorded = self.record_upload_under_stored_view(ops, seq, kind, relayed);
        self.inner.borrow_mut().schema = active;
        recorded
    }

    fn record_upload_under_stored_view(
        &self,
        ops: Vec<StagedOp>,
        seq: u64,
        kind: Kind,
        relayed: Option<Relayed>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
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
        let acting = match author {
            None => Acting::Own,
            Some(author) if session => Acting::Session {
                author: author.canonical().to_owned(),
                claims: inner.claims.get(&author).cloned().unwrap_or_default(),
            },
            Some(author) => Acting::Attributed {
                author: author.canonical().to_owned(),
            },
        };
        let rows = inner.written_rows(&ops, seq)?;
        let mut baselines = Vec::new();
        // What this replica built each row it wrote on (SPEC 4 §4.6): its
        // newest pending commit to the row, else the settled image under
        // its pending writes.
        let mut bases = Vec::new();
        // The settled image of each written row, which its chunked values
        // go as deltas over (SPEC 19.8).
        let mut settled_images = HashMap::new();
        // The written rows this replica held a version of (settled, or an
        // own pending commit's): the commit did not create those.
        let mut over = HashSet::new();
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
            // Deltas and access proofs name their image by an authority
            // seq: over a relay's unsettled version (on a tab) values go
            // whole.
            if let Some(image) = image
                .as_ref()
                .filter(|image| !image.deleted && image.by.is_none())
            {
                settled_images.insert(row_key.clone(), image.clone());
            }
            // This replica sends the commit: its predecessor goes by how
            // far back it is.
            // A relay's unsettled version (on a tab) is the relay's commit.
            let on = match client.pending.newest(&row_key) {
                Some(tx) => Some(BuiltOn::Own { back: seq - tx }),
                None => image.as_ref().map(|image| match image.by {
                    Some(node) => BuiltOn::Commit {
                        node,
                        tx: image.seq,
                    },
                    None => BuiltOn::Seq(image.seq),
                }),
            };
            if on.is_some() {
                over.insert(row_key.clone());
            }
            if relayed.is_none()
                && let Some(on) = on
                && let Some(&op) = by_row
                    .get(&(table.clone(), *row))
                    .and_then(|ops| ops.first())
            {
                bases.push((
                    op,
                    WireBase {
                        key: key.clone(),
                        on,
                    },
                ));
            }
        }
        // A session's commit goes up under its own id.
        let (bases, id) = match relayed {
            Some(relayed) => (relayed.bases, tx_id(relayed.origin.0, relayed.origin.1)),
            None => (
                bases,
                tx_id(inner.client.as_ref().expect("client").node, seq),
            ),
        };
        let mut wire: Result<Vec<WireOp>, Error> = ops.iter().map(to_wire).collect();
        if let Ok(wire) = &mut wire {
            for (op, base) in bases {
                // A relay's bases come from `relay_bases`, which refused any
                // naming no op of the commit.
                let op = wire.get_mut(op).ok_or_else(|| {
                    Error::new(
                        ErrorCode::Protocol,
                        "a relayed base names no op of its commit",
                    )
                })?;
                op.bases.push(base);
            }
            inner.commit_deltas(wire, &rows, &settled_images)?;
        }
        // Each written row as this replica now holds it: the authority
        // does not echo a row it infers is exactly this, and names it
        // `as_written` instead.
        let expected = inner.expected_rows(seq, &ops, &rows, &over)?;
        let client = inner.client.as_mut().expect("client");
        let added: Vec<RowKey> = baselines.iter().map(|(key, _)| key.clone()).collect();
        client.baselines.extend(baselines);
        let serializable = matches!(kind, Kind::Serializable { .. });
        match wire {
            Ok(wire) => {
                let kind = match kind {
                    Kind::Mergeable => WireKind::Mergeable,
                    Kind::Serializable {
                        snapshot,
                        by,
                        reads,
                        own,
                    } => WireKind::Serializable {
                        snapshot: positions(by, snapshot),
                        reads,
                        own,
                    },
                };
                let commit = SyncMessage::Commit {
                    id,
                    kind,
                    ops: wire,
                    acting,
                };
                client.unsent.push(commit.clone());
                client.pending.push(Pending {
                    tx: seq,
                    id,
                    serializable,
                    ops,
                    rows: rows.clone(),
                    expected: Some(expected),
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
                    id,
                    serializable,
                    ops: Vec::new(),
                    rows,
                    expected: None,
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
        view: &ReadViewSourceSpec,
        reach: RemoteReach,
    ) -> Option<(
        String,
        (
            Rc<StdCell<bool>>,
            Rc<RefCell<Option<Error>>>,
            Rc<StdCell<bool>>,
        ),
    )> {
        let mut inner = self.inner.borrow_mut();
        inner.client.as_ref()?;
        let (query, include_deleted, view) = upstream_read(&prepared.query, include_deleted, view);
        let attached = inner.attach_upstream_read(
            query.into_owned(),
            &prepared.binding,
            include_deleted,
            view,
            reach,
        );
        Some(attached)
    }

    pub(crate) fn detach_remote(
        &self,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        view: &ReadViewSourceSpec,
        reach: RemoteReach,
    ) {
        self.inner
            .borrow_mut()
            .release_remote(query, binding, include_deleted, view, reach);
    }

    pub(crate) fn link_hint(&self) -> Option<Rc<StdCell<Link>>> {
        self.inner.borrow().client.as_ref().map(|c| c.hint.clone())
    }
}

impl Inner {
    /// Attach one upstream live read (refcounted by its key): its key, and
    /// whether the server answered it, why it refused it, and whether the
    /// answer was the global authority's.
    fn attach_upstream_read(
        &mut self,
        query: Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        view: ReadViewSourceSpec,
        reach: RemoteReach,
    ) -> (
        String,
        (
            Rc<StdCell<bool>>,
            Rc<RefCell<Option<Error>>>,
            Rc<StdCell<bool>>,
        ),
    ) {
        let key = remote_key(&query, binding, include_deleted, &view, reach);
        let client = self.client.as_mut().expect("client");
        if let Some(id) = client.live_reads.get(&key) {
            let remote = client
                .reads
                .get_mut(id)
                .and_then(ActiveRead::live_mut)
                .expect("live read");
            remote.refs += 1;
            let flags = (
                remote.answered.clone(),
                remote.refused.clone(),
                remote.global.clone(),
            );
            return (key, flags);
        }
        let id = client.next_id;
        client.next_id += 1;
        let answered = Rc::new(StdCell::new(false));
        let refused = Rc::new(RefCell::new(
            client
                .stranded
                .then(stranded)
                .or_else(|| client.uncatalogued.clone()),
        ));
        let global = Rc::new(StdCell::new(false));
        let read = self.active_read(
            query,
            binding.clone(),
            include_deleted,
            view,
            ReadLifetime::Live(LiveRead {
                refs: 1,
                reach,
                answered: answered.clone(),
                refused: refused.clone(),
                global: global.clone(),
                visible: None,
            }),
        );
        let client = self.client.as_mut().expect("client");
        client.outbox.push(SyncMessage::Read {
            id,
            query: Box::new(read.query.clone()),
            binding: read.binding.clone(),
            include_deleted,
            tier: reach.tier,
            wait: reach.wait,
            view: Box::new(read.view.clone()),
            since: read.since(),
            span: ReadSpan::Live,
        });
        client.reads.insert(id, read);
        client.live_reads.insert(key.clone(), id);
        (key, (answered, refused, global))
    }

    /// Drop one reference to an upstream read by its key; the last one
    /// unsubscribes.
    fn release_upstream_read(&mut self, key: &str) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        let Some(&id) = client.live_reads.get(key) else {
            return;
        };
        let live = client
            .reads
            .get_mut(&id)
            .and_then(ActiveRead::live_mut)
            .expect("live read");
        live.refs -= 1;
        if live.refs == 0 {
            client.live_reads.remove(key);
            client.reads.remove(&id);
            client.outbox.push(SyncMessage::Unsubscribe { id });
        }
    }

    /// Drop one reference to an upstream subscription; the last one
    /// unsubscribes.
    pub(crate) fn release_remote(
        &mut self,
        query: &Query,
        binding: &BTreeMap<String, Value>,
        include_deleted: bool,
        view: &ReadViewSourceSpec,
        reach: RemoteReach,
    ) {
        let (query, include_deleted, view) = upstream_read(query, include_deleted, view);
        let key = remote_key(&query, binding, include_deleted, &view, reach);
        self.release_upstream_read(&key);
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
        at: Option<Pos>,
        include_deleted: bool,
        reach: RemoteReach,
        deadline: Option<Instant>,
        coverage: Option<&dyn Fn() -> bool>,
        strict: bool,
        view: &ReadViewSourceSpec,
    ) -> Result<Option<Pos>, Error> {
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
            if tier == ReadTier::Remote {
                inner.check_not_stranded()?;
            }
            if inner.client.is_none() || !strict && !answerable(&inner) {
                return Ok(None);
            }
        }
        let at = match at {
            Some(at) => positions(self.inner.borrow().node_of(at.n)?, at.seq),
            None => Vec::new(),
        };
        let message = |id: u64, since: Positions| SyncMessage::Read {
            id,
            query: Box::new(remote_query(&prepared.query).into_owned()),
            binding: prepared.binding.clone(),
            include_deleted,
            tier,
            wait: reach.wait,
            view: Box::new(view.clone()),
            since,
            span: ReadSpan::Once { at: at.clone() },
        };
        // The request in flight, and the connection it went out on.
        let mut request = PendingFetch {
            db: self,
            asked: None,
        };
        let answer = poll_fn(|_| {
            let mut inner = self.inner.borrow_mut();
            let answerable = answerable(&inner);
            let client = inner.client.as_mut().expect("client");
            if let Some((id, _)) = request.asked
                && let Some(Some(_)) = client.answers.get(&id)
            {
                let answer = client.answers.remove(&id).flatten().expect("answered");
                return Poll::Ready(answer.map(Some));
            }
            // A non-strict read falls back to local data if its request
            // belonged to a replaced link, even if it missed the disconnect.
            if !strict
                && request
                    .asked
                    .is_some_and(|(_, connection)| connection < client.connection)
            {
                return Poll::Ready(Ok(None));
            }
            let ask = if strict {
                client.transport.is_some()
                    && request
                        .asked
                        .is_none_or(|(_, connection)| connection != client.connection)
            } else {
                request.asked.is_none()
            };
            if ask {
                if let Some((id, _)) = request.asked {
                    client.forget_once(id);
                }
                let id = client.next_id;
                client.next_id += 1;
                client.answers.insert(id, None);
                // Asked while detached, it goes out on the next connection.
                let connection = client.connection + u64::from(client.transport.is_none());
                let query = remote_query(&prepared.query).into_owned();
                let since = inner.register_once_read(
                    id,
                    &query,
                    &prepared.binding,
                    include_deleted,
                    view,
                    &at,
                );
                inner
                    .client
                    .as_mut()
                    .expect("client")
                    .outbox
                    .push(message(id, since));
                request.asked = Some((id, connection));
            }
            let client = inner.client.as_mut().expect("client");
            if (!strict && !answerable) || deadline.is_some_and(|d| Instant::now() >= d) {
                return Poll::Ready(Ok(None));
            }
            if let Some(expired) = coverage {
                if client.unadmitted {
                    return Poll::Ready(Err(Error::new(
                        ErrorCode::Schema,
                        "schema version is awaiting published catalogue admission; \
                         deploy it to the server",
                    )));
                }
                if expired() {
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
        let Some((through, _listed)) = answer else {
            return Ok(None);
        };
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
            // Own commits accepted past the known position are named too.
            dots: client
                .pending
                .iter()
                .map(|p| p.tx)
                .filter(|tx| captured.contains(tx))
                .chain(client.own_accepted.iter().map(|&(_, tx)| tx))
                .map(|tx| TxId {
                    time: TxTime(tx),
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
                // A stranded store uploads nothing (INV-SYNC-51).
                None if client.stranded && client.pending.iter().any(|p| p.tx == tx) => {
                    Poll::Ready(Err(stranded()))
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

    /// One-shot read records, answer slots, and queued one-shot requests.
    #[cfg(any(test, feature = "testing"))]
    pub fn pending_once_state_for_test(&self) -> (usize, usize, usize) {
        let inner = self.inner.borrow();
        let Some(client) = inner.client.as_ref() else {
            return (0, 0, 0);
        };
        (
            client
                .reads
                .values()
                .filter(|read| read.live().is_none())
                .count(),
            client.answers.len(),
            client
                .outbox
                .iter()
                .filter(|message| {
                    matches!(
                        message,
                        SyncMessage::Read {
                            span: ReadSpan::Once { .. },
                            ..
                        }
                    )
                })
                .count(),
        )
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
            let acting = match session {
                Some((author, claims)) => Acting::Session {
                    author: author.canonical().to_owned(),
                    claims,
                },
                None => Acting::Own,
            };
            client
                .outbox
                .push(SyncMessage::CheckAllowed { id, action, acting });
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
            client.outbox.retain(
                |m| !matches!(m, SyncMessage::CheckAllowed { id: asked, .. } if *asked == id),
            );
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
                        | SyncMessage::AccessLost { .. }
                        | SyncMessage::Settled { .. }
                        | SyncMessage::ConnectionSettled { .. }
                )
            }) && !inner.sessions.is_empty()
            {
                inner.client.as_mut().expect("client").relay_ready = true;
            }
            // Rows install without their chunks: a chunked cell carries the
            // sort key its indexes need.
            loop {
                // A deferred fate whose rows arrived whole applies next, in
                // the order its fates came.
                let client = inner.client.as_mut().expect("client");
                if client.releasing.is_none()
                    && let Some(head) = client.deferred.front()
                    && head
                        .awaited
                        .as_ref()
                        .is_none_or(|rows| rows.iter().all(|row| !client.whole.contains_key(row)))
                {
                    let DeferredFate { fate, awaited } = client.deferred.pop_front().expect("head");
                    if let SyncMessage::Fate { id, .. } = &fate
                        && let Some(tx) = client.local_tx(id)
                    {
                        client.releasing = Some((tx, awaited.is_some()));
                    }
                    queue.push_front(fate);
                }
                let Some(message) = queue.pop_front() else {
                    break;
                };
                match message {
                    // Rows and evictions name stored tables and columns, as
                    // this store mirrors its upstream's.
                    SyncMessage::Rows { images, tx_ids } => {
                        // A history version is listed once its transaction
                        // is known.
                        for (seq, by, node, tx) in tx_ids {
                            let at = Pos {
                                n: inner.code_of(by)?,
                                seq,
                            };
                            inner.record_settled(node, tx, Some(at))?;
                        }
                        let images: Vec<Image> = images
                            .into_iter()
                            .filter_map(|image| inner.stored_image(image))
                            .collect();
                        inner.begin_current_batch(images.iter().map(|image| {
                            (
                                image.table.clone(),
                                RowUuid::from_bytes(image.id),
                                image.key.clone(),
                            )
                        }))?;
                        let received = (|| {
                            for mut image in images {
                                let at = inner.pos(&image)?;
                                if at.accepted() {
                                    let client = inner.client.as_mut().expect("client");
                                    let newest = client
                                        .newest_foreign
                                        .entry(image.table.clone())
                                        .or_default();
                                    *newest = (*newest).max(at.seq);
                                }
                                let named = (image.table.clone(), image.id, image.key.clone());
                                if !inner.receive_deltas(&mut image)? {
                                    inner.ask_whole(named, 0)?;
                                    continue;
                                }
                                inner.client.as_mut().expect("client").whole.remove(&named);
                                if inner.record_vias(&named.0, &image)? {
                                    // A node no version held may be held by
                                    // this one: missing answers are transient.
                                    inner.client.as_mut().expect("client").unavailable.clear();
                                }
                                inner.unwithhold(
                                    &image.table,
                                    RowUuid::from_bytes(image.id),
                                    &image.key,
                                )?;
                                let overlaid = inner.receive_image(image)?;
                                rebase |= overlaid;
                            }
                            Ok::<_, Error>(())
                        })();
                        inner.end_current_batch();
                        received?;
                    }
                    SyncMessage::AccessLost {
                        table,
                        id,
                        key,
                        hidden,
                    } => {
                        if !inner.physical.tables.contains_key(&table) {
                            continue;
                        }
                        let row = RowUuid::from_bytes(id);
                        let held = inner.current_image(&table, row, &key)?.is_some();
                        rebase |= inner.receive_eviction(&table, row, &key)?;
                        // A version still exists: it masks the base's row in
                        // branch views until an image of it arrives.
                        if hidden {
                            inner.withhold(&table, row, &key)?;
                        } else {
                            inner.unwithhold(&table, row, &key)?;
                        }
                        // Results settled before this one may have held
                        // the row: a resume from them would count it as
                        // held, though this store no longer has it.
                        if held {
                            inner.forget_watermarks()?;
                        }
                        let client = inner.client.as_mut().expect("client");
                        client.whole.remove(&(table, id, key));
                    }
                    // The answer to this client's `Join`: the catalogue
                    // applies first, as rows that follow are stored by it.
                    SyncMessage::Welcome {
                        admitted,
                        catalogue,
                    } => {
                        queue.push_front(SyncMessage::SchemaAdmission { admitted });
                        queue.push_front(SyncMessage::Catalogue(catalogue));
                    }
                    SyncMessage::Catalogue(CatalogueUpdate {
                        namespace,
                        versions,
                        cells,
                        confirmed,
                    }) => {
                        match inner.mirror_catalogue(namespace, versions, cells, &confirmed) {
                            // Versions it took rebuilt the store's
                            // schema: its pending commits replay on top.
                            Ok(mirrored) => {
                                rebase |= mirrored;
                                // The authority may apply commits already
                                // sent under a version this store just
                                // took: what it made of them is no image
                                // of the result, so their rows come whole.
                                if mirrored {
                                    let client = inner.client.as_mut().expect("client");
                                    client.pending.forget_expected();
                                }
                                let client = inner.client.as_mut().expect("client");
                                client.catalogued = true;
                                client.uncatalogued = None;
                            }
                            // This store cannot store rows as its upstream
                            // does: it stops syncing with it, nothing that
                            // follows applies, and its remote reads fail
                            // with why. Local reads are served as before.
                            Err(error) => {
                                inner.client.as_mut().expect("client").uncatalogued =
                                    Some(error.clone());
                                inner.refuse_upstream(error);
                                queue.clear();
                            }
                        }
                    }
                    SyncMessage::Settled {
                        id,
                        through,
                        members,
                        global,
                    } => {
                        let (by, through) = required_position(&through, "a settlement")?;
                        // A read's settle speaks for that read alone.
                        let at = Pos {
                            n: inner.code_of(by)?,
                            seq: through,
                        };
                        let client = inner.client.as_mut().expect("client");
                        let resumable = client.whole.is_empty();
                        let mut live_watermark = None;
                        let mut once = false;
                        if let Some(read) = client.reads.get_mut(&id) {
                            let (listed, save_after) = match &mut read.lifetime {
                                ReadLifetime::Once { historical } => {
                                    once = true;
                                    (
                                        (!*historical).then(|| members.clone().unwrap_or_default()),
                                        global && by.is_none(),
                                    )
                                }
                                ReadLifetime::Live(live) => {
                                    live.answered.set(true);
                                    live.global.set(global);
                                    if live.visible.is_none_or(|v| v.n != at.n || v.seq < at.seq) {
                                        live.visible = Some(at);
                                        client.visibility_moved = true;
                                    }
                                    if resumable {
                                        read.through = Some((by, through));
                                        client.forgotten = false;
                                        live_watermark = read.watermark;
                                    }
                                    (members.clone(), false)
                                }
                            };
                            if let Some(listed) = listed {
                                answered_remotes.push((
                                    read.query.clone(),
                                    read.binding.clone(),
                                    read.include_deleted,
                                    read.view.clone(),
                                    listed,
                                    through,
                                    if save_after {
                                        read.watermark.map(|key| (key, by))
                                    } else {
                                        None
                                    },
                                ));
                            }
                        }
                        if once {
                            client.reads.remove(&id);
                        }
                        if let Some(slot) = client.answers.get_mut(&id) {
                            *slot = Some(Ok((at, members.unwrap_or_default())));
                            client.relay_ready |=
                                client.relayed_fetches.iter().any(|f| f.upstream == id);
                        }
                        // Live checkpoints retain their existing savepoint ordering:
                        // a subsequent membership eviction can invalidate them.
                        if let Some(watermark) = live_watermark {
                            inner.store_watermark(&watermark, by, through)?;
                        }
                    }
                    SyncMessage::CheckAllowedResult { id, result } => {
                        let client = inner.client.as_mut().expect("client");
                        // Late answers to cancelled requests are dropped.
                        if let Some(slot @ None) = client.advice.get_mut(&id) {
                            *slot = Some(result);
                        }
                    }
                    SyncMessage::SchemaAdmission { admitted } => {
                        inner.client.as_mut().expect("client").unadmitted = !admitted;
                    }
                    SyncMessage::Refused { id, code, message } => {
                        let client = inner.client.as_mut().expect("client");
                        if client
                            .reads
                            .get(&id)
                            .is_some_and(|read| read.live().is_none())
                        {
                            client.reads.remove(&id);
                        }
                        let error = Error::new(code, message);
                        client.relay_ready |=
                            client.relayed_fetches.iter().any(|f| f.upstream == id);
                        if let Some(slot) = client.answers.get_mut(&id) {
                            *slot = Some(Err(error));
                        } else if let Some(remote) =
                            client.reads.get(&id).and_then(ActiveRead::live)
                        {
                            *remote.refused.borrow_mut() = Some(error);
                        }
                    }
                    SyncMessage::Stored { id } => {
                        let client = inner.client.as_mut().expect("client");
                        if let Some(tx) = client.local_tx(&id)
                            && client.pending.iter().any(|p| p.tx == tx)
                        {
                            client.stored.insert(tx);
                        }
                    }
                    SyncMessage::Fate {
                        id,
                        outcome,
                        global,
                    } => {
                        let (by, through) = outcome.position()?;
                        let client = inner.client.as_mut().expect("client");
                        // Not a commit this store uploads: nothing to settle.
                        let Some(tx) = client.local_tx(&id) else {
                            continue;
                        };
                        let releasing = client.releasing.take_if(|(at, _)| *at == tx);
                        // Fates apply in the order they came: one after a
                        // deferred fate waits behind it.
                        if releasing.is_none() && !client.deferred.is_empty() {
                            client.deferred.push_back(DeferredFate {
                                fate: SyncMessage::Fate {
                                    id,
                                    outcome,
                                    global,
                                },
                                awaited: None,
                            });
                            continue;
                        }
                        let accepted_at = match &outcome {
                            Outcome::Accepted {
                                accepted_at,
                                as_written,
                                ..
                            } => {
                                let (images, whole, mismatched) = inner.as_written_images(
                                    tx,
                                    through,
                                    by,
                                    *accepted_at,
                                    as_written,
                                )?;
                                let arrived = matches!(releasing, Some((_, true)));
                                // Rows it cannot install are asked for whole,
                                // and the commit stays pending until they
                                // arrive: its writes never drop out here.
                                if !whole.is_empty() && !arrived {
                                    inner.as_written_mismatches += mismatched;
                                    for row in &whole {
                                        inner.ask_whole(row.clone(), through)?;
                                    }
                                    let client = inner.client.as_mut().expect("client");
                                    client.deferred.push_front(DeferredFate {
                                        fate: SyncMessage::Fate {
                                            id,
                                            outcome,
                                            global,
                                        },
                                        awaited: Some(whole.into_iter().collect()),
                                    });
                                    continue;
                                }
                                let client = inner.client.as_mut().expect("client");
                                if let Some(pending) = client.pending.get_mut(tx) {
                                    pending.expected = None;
                                }
                                inner.begin_current_batch(images.iter().map(|image| {
                                    let row = RowUuid::from_bytes(image.id);
                                    (image.table.clone(), row, image.key.clone())
                                }))?;
                                let received = images
                                    .into_iter()
                                    .try_for_each(|image| inner.receive_image(image).map(drop));
                                inner.end_current_batch();
                                received?;
                                *accepted_at
                            }
                            _ => None,
                        };
                        let result = outcome.result();
                        let accepted = matches!(outcome, Outcome::Accepted { .. });
                        // A relay's own last word is at its own position.
                        let landed = if global || by.is_some() {
                            Pos {
                                n: inner.code_of(by)?,
                                seq: through,
                            }
                        } else {
                            Pos::at(through)
                        };
                        // Accepted past what this replica knows, its own
                        // commit is named by tx until the connection's
                        // position covers it.
                        {
                            let client = inner.client.as_mut().expect("client");
                            if accepted
                                && (global || by.is_some())
                                && (landed.n != client.known_n || through > client.known)
                            {
                                client.own_accepted.insert((through, tx));
                                client.visibility_moved = true;
                            }
                        }
                        let client = inner.client.as_mut().expect("client");
                        // From the pending commit, not what was sent: a
                        // commit refused before sending was never sent.
                        let serializable =
                            client.pending.iter().any(|p| p.tx == tx && p.serializable);
                        client.pending.remove(tx);
                        client.stored.remove(&tx);
                        client.sent.retain(
                            |m| !matches!(m, SyncMessage::Commit { id: sent, .. } if *sent == id),
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
                        inner.record_settled(node, tx, accepted.then_some(landed))?;
                        // The sessions' commits it carried settle where it
                        // did: a resend of one gets the upstream's fate.
                        if accepted && global {
                            for (origin, session_tx) in inner.relayed_origins(tx)? {
                                inner.record_settled(origin, session_tx, Some(landed))?;
                            }
                        }
                        inner.forget_pending(tx)?;
                        rebase = true;
                        // Sessions whose commits this upload carried learn
                        // their fate from the upstream's.
                        let client = inner.client.as_mut().expect("client");
                        if let Some(relayed) = client.relayed_fates.remove(&tx) {
                            client.relay_ready = true;
                            let fate = client.fates.get(&tx).cloned().unwrap_or(Ok(()));
                            let known = client.known;
                            for (index, session_id, rows) in relayed {
                                let Some(session) =
                                    inner.sessions.get_mut(index).filter(|s| s.open)
                                else {
                                    continue;
                                };
                                // Reconcile the writer at settlement even
                                // without a live subscription. Acceptance may
                                // have merged concurrent upstream changes;
                                // refusal restores the upstream versions.
                                session.touched.extend(rows);
                                let outcome = match &fate {
                                    Ok(()) if accepted => {
                                        Outcome::accepted(by, landed.seq, accepted_at)
                                    }
                                    Ok(()) => {
                                        return Err(unsupported(format!(
                                            "commit {tx} accepted at no seq"
                                        )));
                                    }
                                    Err(error) => Outcome::Refused {
                                        through: positions(by, known),
                                        error: wire_error(error),
                                    },
                                };
                                session.outbox.push(SyncMessage::Fate {
                                    id: session_id,
                                    outcome,
                                    global,
                                });
                            }
                        }
                    }
                    SyncMessage::ConnectionSettled { through } => {
                        let (by, through) = required_position(&through, "a connection settlement")?;
                        inner.learn_known(through, by)?;
                        let client = inner.client.as_mut().expect("client");
                        let known = client.known;
                        client.own_accepted = client.own_accepted.split_off(&(known + 1, 0));
                        client.visibility_moved = true;
                    }
                    other => return Err(unsupported(format!("client received {other:?}"))),
                }
            }
        }
        if rebase {
            self.rebase()?;
            self.inner.borrow_mut().reconcile_unsettled()?;
        }
        // Results whose position moved read as of the new one.
        {
            let mut inner = self.inner.borrow_mut();
            if std::mem::take(&mut inner.client.as_mut().expect("client").visibility_moved) {
                inner.refresh_visibility()?;
            }
        }
        // A settled answer lists its result's rows: cached rows the
        // replica's view of that result holds and the server's lacks lost
        // their listing (INV-SYNC-14), as rows a commit took away do. A
        // resumed opening lists its result too.
        for (query, binding, include_deleted, view, listed, through, watermark) in answered_remotes
        {
            self.evict_unanswered(query, binding, include_deleted, &view, &listed, through)?;
            if let Some((watermark, by)) = watermark {
                let mut inner = self.inner.borrow_mut();
                if inner.client.as_ref().expect("client").whole.is_empty() {
                    inner.store_watermark(&watermark, by, through)?;
                    inner.client.as_mut().expect("client").forgotten = false;
                }
            }
        }
        let mut inner = self.inner.borrow_mut();
        let client = inner.client.as_mut().expect("client");
        let known = client.known;
        if known == client.persisted_known || client.known_n != 0 {
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
                    SyncMessage::Commit { id, .. } if size > limit => {
                        client.refused.push(SyncMessage::Fate {
                            id: *id,
                            outcome: Outcome::Refused {
                                through: positions(None, client.known),
                                error: WireError::Rejected {
                                    reason: None,
                                    message: format!(
                                        "a commit of {size} bytes is over the {limit} bytes one \
                                         message may carry: not supported yet"
                                    ),
                                },
                            },
                            global: false,
                        });
                    }
                    _ => kept.push((commit, push)),
                }
            }
            (commits, pushes) = kept.into_iter().unzip();
        }
        for commit in &commits {
            if !client.sent.iter().any(|m| matches!((m, commit), (SyncMessage::Commit { id: a, .. }, SyncMessage::Commit { id: b, .. }) if a == b)) {
                client.sent.push(commit.clone());
            }
        }
        // A commit names tables and columns by stored names, whichever
        // version wrote it, so no version travels with it. One naming
        // what the server does not hold was written under a version it
        // never deployed, and is refused (INV-SYNC-52).
        for (commit, (hashes, batches)) in commits.into_iter().zip(pushes) {
            if let SyncMessage::Commit { id, .. } = &commit
                && let Some(tx) = client.local_tx(id)
                && !hashes.is_empty()
            {
                client.pushed.insert(tx, hashes);
            }
            out.extend(batches);
            out.push(commit);
        }
        out.extend(reads);
        // A full outbound queue keeps the rest, in order, for the flush its
        // drain wakes.
        let transport = client.transport.as_mut().expect("transport");
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
        for ((table, row, key), image) in &baseline {
            match image {
                Some(image) => {
                    let at = inner.pos(image)?;
                    inner.drop_row(table, *row, key, at)?;
                    inner.put_image(image, false)?;
                }
                None => inner.drop_row(table, *row, key, Pos::default())?,
            }
        }
        // Pending ops name tables and columns by stored names, whichever
        // version wrote them: they replay under the stored view, so what a
        // view wrote that the current version lacks stays visible through
        // that view, and nothing is renamed across an upgrade.
        let view = inner.stored_view();
        let active = std::mem::replace(&mut inner.schema, view);
        let replayed = pending.into_iter().try_for_each(|(tx, ops)| {
            begin_savepoint(&inner.conn, "replay")?;
            inner.merging = true;
            let replayed = ops.iter().try_for_each(|op| match inner.apply(op, tx) {
                // A part of a cross-shard commit that landed before its fate
                // (another shard answered first) already holds its row. The
                // writer's version overlays that baseline as it overlays any
                // other, until the fate: the whole commit still shows as its
                // writer's. A conflicting insert by someone else is refused
                // upstream, and the fate drops the whole commit.
                Err(error) if error.reason == Some(Reason::RowAlreadyExists) => {
                    let StagedOp::Insert(table, _, opts) = op else {
                        return Err(error);
                    };
                    let landed = baseline.iter().find_map(|((t, row, key), image)| {
                        let image = image.as_ref()?;
                        (t == table && Some(*row) == opts.row_id).then_some((row, key, image))
                    });
                    let Some((row, key, image)) = landed else {
                        return Err(error);
                    };
                    let at = inner.pos(image)?;
                    inner.drop_row(table, *row, key, at)?;
                    inner.apply(op, tx).map(drop)
                }
                other => other.map(drop),
            });
            inner.merging = false;
            end_savepoint(&inner.conn, "replay", replayed.is_ok())
        });
        inner.schema = active;
        replayed?;
        let client = inner.client.as_mut().expect("client");
        client.baselines = baseline
            .into_iter()
            .filter(|(row, _)| keep.contains(row))
            .collect();
        Ok(())
    }
}

impl Inner {
    /// A stranded replica (see [`stranded`]) has no upstream to answer a
    /// `Remote` read: its rows at the authority's positions are its own,
    /// never confirmed (INV-SYNC-51). A local-first read serves its store.
    pub(crate) fn check_not_stranded(&self) -> Result<(), Error> {
        match &self.client {
            Some(client) if client.stranded => Err(stranded()),
            _ => Ok(()),
        }
    }

    /// Whether any stored table holds a row or a version of one: on a
    /// store that is not a replica, only its own commits put them there.
    fn holds_rows(&self) -> Result<bool, Error> {
        for stored in self.physical.tables.keys() {
            for table in [stored.clone(), format!("{stored}__h")] {
                let sql = format!("SELECT EXISTS (SELECT 1 FROM {})", quote(&table));
                let any: bool = self
                    .conn
                    .query_row(&sql, [], |r| r.get(0))
                    .map_err(storage)?;
                if any {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// The upstream sent what this store cannot follow (a catalogue it
    /// cannot mirror): drop the link, and fail every remote read with
    /// `error` until the next connection answers it afresh.
    fn refuse_upstream(&mut self, error: Error) {
        self.detach_upstream();
        if let Some(client) = self.client.as_mut() {
            for remote in client.reads.values().filter_map(ActiveRead::live) {
                *remote.refused.borrow_mut() = Some(error.clone());
            }
        }
    }

    /// After uploads settled: rows sessions were shipped at an unsettled
    /// version that is no longer current here ship again as they are now,
    /// accepted or evicted (see [`Session::unsettled`]).
    fn reconcile_unsettled(&mut self) -> Result<(), Error> {
        for index in 0..self.sessions.len() {
            if !self.sessions[index].open || self.sessions[index].unsettled.is_empty() {
                continue;
            }
            let rows = std::mem::take(&mut self.sessions[index].unsettled);
            let mut still = BTreeMap::new();
            let mut moved = Vec::new();
            for ((table, row, key), shipped) in rows {
                let current = self
                    .current_image(&table, row, &key)?
                    .and_then(|image| Some((image.by?, image.seq)));
                if current != Some(shipped) {
                    moved.push((table.clone(), row, key.clone()));
                }
                // Shipped again below while still unsettled: tracked anew.
                if let Some(current) = current {
                    still.insert((table, row, key), current);
                }
            }
            let session = &mut self.sessions[index];
            session.unsettled = still;
            session.touched.extend(moved);
        }
        Ok(())
    }

    /// Drop the upstream link: reads stop waiting, writes queue.
    fn detach_upstream(&mut self) {
        let Some(client) = self.client.as_mut() else {
            return;
        };
        client.transport = None;
        client.clear_once_requests();
        // What the authority deployed is known again once the next
        // upstream sends its catalogue.
        client.catalogued = false;
        let mut link = client.hint.get();
        link.detached = true;
        client.hint.set(link);
        // Nothing settles at the server's tier while detached: a
        // subscription (or one opened meanwhile) is held until the next
        // upstream answers it again.
        for remote in client.reads.values().filter_map(ActiveRead::live) {
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
        let reads = &mut client.reads;
        client.relayed_fetches.retain(|fetch| {
            let keep = fetch.session != index;
            if !keep {
                answers.remove(&fetch.upstream);
                reads.remove(&fetch.upstream);
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
                        view,
                        span: ReadSpan::Once { at },
                        ..
                    } = &fetch.ask
                        && at.is_empty()
                    {
                        answers.push((
                            (**query).clone(),
                            binding.clone(),
                            *include_deleted,
                            (**view).clone(),
                            listed,
                            through.seq,
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
        self.sessions[index]
            .version
            .is_some_and(|version| !self.deployed(version))
    }

    /// Whether this store holds every stored table and column `ops`
    /// name: otherwise they were written under a version it has not
    /// deployed.
    fn holds_names(&self, ops: &[WireOp]) -> bool {
        let view = self.stored_view();
        ops.iter().all(|op| {
            let Ok(table) = view.table(op.table()) else {
                return false;
            };
            let cells = match &op.write {
                WireWrite::Insert { cells, .. }
                | WireWrite::Update { cells, .. }
                | WireWrite::Upsert { cells, .. }
                | WireWrite::Restore {
                    cells: Some(cells), ..
                } => Some(cells),
                _ => None,
            };
            let created = match &op.write {
                WireWrite::Upsert { created, .. } => Some(created),
                _ => None,
            };
            cells
                .into_iter()
                .chain(created)
                .flat_map(|cells| cells.keys())
                .all(|name| name.starts_with('$') || table.has_column(name))
        })
    }

    /// `message` (about a replicated write, which names tables and
    /// columns by stored names) with each stored name the current schema
    /// gives another name spelled by that name, so it reads as the app's.
    pub(crate) fn by_current_names(&self, message: &str) -> String {
        let mut message = message.to_owned();
        for table in self.owner.tables.values() {
            let names = std::iter::once((&table.stored, &table.name))
                .chain(table.columns.iter().map(|c| (&c.stored, &c.name)));
            for (stored, name) in names {
                if stored != name && message.contains(stored.as_str()) {
                    message = replace_name(&message, stored, name);
                }
            }
        }
        message
    }

    /// Whether this store admitted schema `version`.
    fn deployed(&self, version: crate::ids::SchemaVersionId) -> bool {
        version == self.owner_version || self.admitted.iter().any(|(id, _)| *id == version)
    }

    /// Whether this store knows every version the authority deployed: it
    /// is the authority (it has no upstream), or its upstream sent its
    /// catalogue on this connection.
    /// A store with no upstream link is the top of its tree: a worker
    /// that will sync becomes a replica (or is given a link hint) before
    /// its tabs write.
    fn knows_deployed(&self) -> bool {
        self.client
            .as_ref()
            .is_none_or(|client| client.catalogued && client.transport.is_some())
    }

    /// Refuse each commit in `held`, messages of session `index` in the
    /// order it sent them, that it wrote under a version this store has
    /// not admitted, once it knows every version the authority deployed
    /// (INV-SYNC-52).
    fn refuse_undeployed(&mut self, index: usize, held: &mut VecDeque<SyncMessage>) {
        if !self.knows_deployed() {
            return;
        }
        let version = self.sessions[index].version;
        let mut refused = Vec::new();
        held.retain(|message| match message {
            SyncMessage::Commit { id, .. } if version.is_some_and(|v| !self.deployed(v)) => {
                refused.push(*id);
                false
            }
            _ => true,
        });
        for id in refused {
            self.refuse_undeployed_commit(index, id);
        }
    }

    /// Answer session `index`'s commit `id`, written under a version this
    /// store has not admitted, with its refusal.
    fn refuse_undeployed_commit(&mut self, index: usize, id: TxId) {
        let through = self.served_through();
        let by = self.served_by();
        self.sessions[index].outbox.push(SyncMessage::Fate {
            id,
            outcome: Outcome::Refused {
                through: positions(by, through),
                error: wire_error(&schema_version_not_deployed()),
            },
            global: true,
        });
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
            && image.by.is_none()
        {
            return Ok(Some(image.clone()));
        }
        self.version_image(table, row, key, Pos::at(seq))
    }

    /// A commit's chunked values that extend the same column's value in
    /// their row's settled image, as deltas over it: each op's cell holds
    /// NULL in its place (SPEC 19.8). The upstream holds every settled
    /// version, as it shipped them. The rest travel as trees, named with
    /// that settled image as their access proof (SPEC 19.4).
    fn commit_deltas(
        &self,
        ops: &mut [WireOp],
        rows: &[RowKey],
        settled: &HashMap<RowKey, Image>,
    ) -> Result<(), Error> {
        for op in ops.iter_mut() {
            let Ok(table) = self.schema.table(op.table()) else {
                continue;
            };
            let (row, cells) = match &mut op.write {
                WireWrite::Insert { row, cells, .. }
                | WireWrite::Update { row, cells, .. }
                | WireWrite::Upsert { row, cells, .. }
                | WireWrite::Restore {
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
                    op.deltas.push(CellDelta {
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
                op.access_proof = Some(AccessProof {
                    key: row_key.2.clone(),
                    seq: base.seq,
                });
            }
        }
        Ok(())
    }

    /// Ask the upstream for a row whole: a `ResendWhole` of it, which it
    /// answers with the row whole or its eviction (SPEC 19.8). Until it arrives
    /// this store holds a stale version, so every watermark is forgotten
    /// and none is stored (see `Client::whole`). `seq`: a version of the row
    /// an own commit produced, which this store must hold too (0 for none).
    fn ask_whole(&mut self, (table, id, key): WholeRow, seq: u64) -> Result<(), Error> {
        self.forget_watermarks()?;
        let client = self.client.as_mut().expect("client");
        let owed = client
            .whole
            .entry((table.clone(), id, key.clone()))
            .or_default();
        *owed = (*owed).max(seq);
        let asked = client.outbox.iter().any(|message| {
            matches!(message, SyncMessage::ResendWhole { rows } if rows.iter().any(|held| {
                held.0 == table && held.1 == id && held.2 == key && held.3 == seq
            }))
        });
        if !asked {
            client.outbox.push(SyncMessage::ResendWhole {
                rows: vec![(table, id, key, seq)],
            });
        }
        Ok(())
    }

    /// Apply one server image; returns whether a rebase is needed (the row
    /// is overlaid by a pending write).
    fn receive_image(&mut self, image: Image) -> Result<bool, Error> {
        // An upstream ships accepted versions and its own unsettled ones,
        // never this replica's own: those it holds as it wrote them.
        if image.by == Some(*self.identity.node.as_bytes()) {
            return Err(unsupported(
                "an upstream shipping a version numbered by this replica's own node",
            ));
        }
        let at = self.pos(&image)?;
        let row = RowUuid::from_bytes(image.id);
        let overlaid = (image.table.clone(), row, image.key.clone());
        let old = match self
            .client
            .as_ref()
            .expect("client")
            .baselines
            .get(&overlaid)
        {
            None => None,
            Some(None) => Some(None),
            Some(Some(old)) => Some(Some(self.pos(old)?)),
        };
        if let Some(old) = old {
            let client = self.client.as_mut().expect("client");
            let baseline = client.baselines.get_mut(&overlaid).expect("baseline");
            match old {
                Some(held) if held >= at && held.accepted() => {
                    self.put_image(&image, true)?;
                    return Ok(false);
                }
                // A relay's older unsettled version (an as-of answer).
                Some(held) if relay_older(held, at) => {
                    if self.non_durable {
                        self.put_image(&image, true)?;
                    }
                    return Ok(false);
                }
                // The replaced server version stays as history. A relay's
                // unsettled version gives way to a settled one, as the
                // relay's store moved on from it.
                Some(held) => {
                    let relay_newer = !held.accepted() && held < at;
                    let older = baseline.replace(image).expect("held baseline");
                    if held.accepted() || self.non_durable && relay_newer {
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
    /// Whether this store may serve a session of `identity`. A store
    /// configured as a user serves only that user: it relays what its
    /// sessions write under its own identity, so another user's writes
    /// would be attributed to this one. Another identity is refused with
    /// [`Reason::IdentityMismatch`]. A system store (a server) serves any
    /// identity.
    pub fn check_serves(&self, identity: AuthorSubject) -> Result<(), Error> {
        let own = self.inner.borrow().identity.author;
        if matches!(own, AuthorSubject::Authenticated(_)) && identity != own {
            return Err(Error::with_reason(
                ErrorCode::Protocol,
                Reason::IdentityMismatch,
                format!(
                    "a store configured as {} serves only its own identity, not {}",
                    own.canonical(),
                    identity.canonical()
                ),
            ));
        }
        Ok(())
    }

    /// Serve a client over `transport` as `identity`, once
    /// [`Self::check_serves`] allows it.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
    ) -> Result<SubscriberId, Error> {
        self.accept_subscriber_with_claims(transport, identity, None)
    }

    /// [`Self::accept_subscriber`] for a connection that authenticated
    /// with its own claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriberId, Error> {
        self.check_serves(identity)?;
        let mut inner = self.inner.borrow_mut();
        // One relay tier only (SPEC 9 §9.5).
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
        Ok(SubscriberId { slot, generation })
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
            for (query, binding, include_deleted, view) in session.close(&dropped) {
                inner.release_remote(
                    &query,
                    &binding,
                    include_deleted,
                    &view,
                    RemoteReach::REMOTE,
                );
            }
            inner.forget_relayed(subscriber.slot);
        }
    }

    /// Rows the authority named as written for this replica's own commits
    /// that were not this replica's image of them, and were asked for
    /// whole instead (for tests and metrics): each is a case the
    /// authority's inference that its writer holds the row missed, at the
    /// cost of one round trip (SPEC 8 §8.3).
    pub fn as_written_mismatches(&self) -> u64 {
        self.inner.borrow().as_written_mismatches
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
        Ok(handle.seq.seq)
    }

    /// Sessions the server ended since the last call, with why: for what
    /// they sent (a message it cannot decode or serve), or because what it
    /// had for them could not be sent (a full queue, or a message it cannot
    /// encode). The host closes their links and tells each peer why; each
    /// is already disconnected. Empty unless the host tracks them
    /// ([`Db::track_closed_subscribers`]).
    pub fn take_closed_subscribers(&self) -> Vec<(SubscriberId, Error)> {
        self.inner
            .borrow_mut()
            .closed
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Keep the sessions the server ends, with why, for
    /// [`Db::take_closed_subscribers`]: for a host that takes them, as
    /// often as it ticks.
    pub fn track_closed_subscribers(&self) {
        self.inner.borrow_mut().closed.get_or_insert_with(Vec::new);
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
                self.note_held(&mut barrier, &message);
                held.push_back(message);
                continue;
            }
            if let SyncMessage::Read { id, .. } = &message {
                self.inner.borrow_mut().sessions[index]
                    .backlog_reads
                    .1
                    .remove(id);
            }
            // A commit writing a row a cross-shard part holds waits until
            // the part lands (or its hold is released), as does every
            // commit but a fixed part's while a reopened shard waits out
            // its offset bound; what the session sent after it waits
            // behind it (SPEC 15).
            if let SyncMessage::Commit { id, ops, .. } = &message
                && self.inner.borrow().commit_waits(
                    crate::db::shard::CommitRef::from(id),
                    &crate::db::shard::HeldRow::of(ops),
                )?
            {
                self.note_held(&mut barrier, &message);
                held.push_back(message);
                continue;
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
            // that way. Once it knows every version the authority
            // deployed, its commits do not wait: they are refused
            // (INV-SYNC-52).
            // So does a commit naming stored tables or columns it does not
            // hold yet: a version it has not admitted wrote it.
            let refuses = matches!(message, SyncMessage::Commit { .. })
                && self.inner.borrow().knows_deployed();
            let unheld = matches!(&message, SyncMessage::Commit { ops, .. } if !self.inner.borrow().holds_names(ops));
            if !refuses && !matches!(message, SyncMessage::Join { .. }) {
                let mut inner = self.inner.borrow_mut();
                if inner.session_unadmitted(index) || unheld {
                    inbound.push_front(message);
                    held.extend(inbound);
                    // The commits it holds that way are refused now: a
                    // refused commit changes nothing a read could observe.
                    inner.refuse_undeployed(index, &mut held);
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
                    self.note_held(&mut barrier, &fetch);
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
        // Tell the client when its declared version's admission changes:
        // the `Welcome` it is still owed tells it first.
        let unadmitted = inner.session_unadmitted(index);
        let session = &mut inner.sessions[index];
        if session.open && !session.welcome_owed && session.told_unadmitted != unadmitted {
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
            SyncMessage::CheckAllowed { .. } => true,
            // A read frozen at a snapshot may name the held commits
            // themselves, which the server cannot resolve until they apply.
            SyncMessage::Read { view, .. } if frozen_at(view).is_some() => {
                barrier.tables.is_some() && !barrier.commits
            }
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
    fn note_held(&self, barrier: &mut HeldWrites, message: &SyncMessage) {
        match message {
            SyncMessage::PushChunks { .. } | SyncMessage::ChunkRequests { .. } => {}
            SyncMessage::Read { id, .. } => {
                barrier.reads.insert(*id);
            }
            SyncMessage::Unsubscribe { .. } | SyncMessage::CheckAllowed { .. } => {}
            SyncMessage::Commit { ops, .. } => {
                barrier.commits = true;
                let schema = self.inner.borrow().stored_view();
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
                if let Some(closed) = self.inner.borrow_mut().closed.as_mut() {
                    closed.push((subscriber, error));
                }
            }
        }
        self.notify();
        let mut inner = self.inner.borrow_mut();
        let through = inner.served_through();
        let by = inner.served_by();
        let relaying = inner.relays_upstream();
        let mut cache: HashMap<(String, RowUuid, Pos), Vec<Image>> = HashMap::new();
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
            // The session's branch views over a base: a head row written
            // over a base row, or a base row written under a head row, masks
            // (or stops masking) that base row, if the reader may see it
            // through the view; no other reader learns of it.
            // Each held by the subscription whose view it masks for.
            let masks: Vec<(u64, fanout::ViewMask)> = inner.sessions[index]
                .subs
                .iter()
                .filter_map(|(id, (_, group, ..))| Some((*id, group.upgrade()?)))
                .flat_map(|(id, group)| {
                    let masks = group.borrow().masks().to_vec();
                    masks.into_iter().map(move |mask| (id, mask))
                })
                .collect();
            let mut masking: Vec<(u64, String, RowUuid, Vec<u8>)> = Vec::new();
            for ((table, row, key), _) in &written_keys {
                for (owner, mask) in masks.iter().filter(|(_, mask)| mask.table == *table) {
                    let has_head = if *key == mask.head {
                        true
                    } else if *key == mask.base {
                        inner.current_image(table, *row, &mask.head)?.is_some()
                            || inner.is_withheld(table, *row, &mask.head)?
                    } else {
                        continue;
                    };
                    if has_head
                        && !inner
                            .masked(inner.session_policy(index), mask, &[*row])?
                            .is_empty()
                    {
                        masking.push((*owner, table.clone(), *row, mask.head.clone()));
                    }
                }
            }
            if recheck.is_empty()
                && masking.is_empty()
                && inner.sessions[index].idle()
                && !inner.sessions[index].catalogue_owed
                && !(relaying && inner.owes_positions(index, (by, through)))
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
            let asked_whole = std::mem::take(&mut inner.sessions[index].asked_whole);
            echoes.retain(|echo| {
                !asked_whole.contains(&(echo.table.clone(), echo.row, echo.key.clone()))
            });
            let mut watch = std::mem::take(&mut inner.sessions[index].watch);
            let mut held_rows = std::mem::take(&mut inner.sessions[index].held);
            let mut unreadable = std::mem::take(&mut inner.sessions[index].unreadable);
            for (table, row, key) in recheck {
                let can = inner.current_image(&table, row, &key)?.is_some()
                    && readable(&inner, &table, row, &key, policy)?;
                if held_rows.recheck(&table, row, &key, can) {
                    touched.insert((table, row, key));
                }
            }
            for ((table, row, key), keys) in &written_keys {
                if keys.iter().any(|k| watch.contains(k)) {
                    // A version the replica already knows it may not read
                    // (evicted, or hidden in a view) ships again only once
                    // it may.
                    let known = (table.clone(), *row, key.clone());
                    let hidden = held_rows
                        .of(table)
                        .and_then(|rows| rows.get(&(*row, key.clone())))
                        .is_some_and(|(can, _)| !can);
                    if hidden || unreadable.contains(&known) {
                        let can = inner.current_image(table, *row, key)?.is_some()
                            && readable(&inner, table, *row, key, policy)?;
                        if can {
                            unreadable.remove(&known);
                            touched.insert(known);
                        }
                    } else {
                        touched.insert(known);
                    }
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
            // Seqs of shipped history versions: their tx ids go first.
            let mut history_seqs = BTreeSet::new();
            // Branch views opening (or reset) this tick: the head rows that
            // mask base rows they match ship with them.
            let mut opening_masks = Vec::new();
            for (id, queue, group, list) in subs {
                let events: Vec<SubscriptionEvent> = queue.borrow_mut().drain(..).collect();
                if events.is_empty() {
                    continue;
                }
                if events
                    .iter()
                    .any(|event| matches!(event, SubscriptionEvent::Delta { reset: true, .. }))
                    && let Some(group) = group.upgrade()
                {
                    opening_masks
                        .extend(group.borrow().masks().iter().map(|mask| (id, mask.clone())));
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
                // Unchanged for a resumed client: a version it held as of
                // `since` that was in the result then. A row whose
                // membership inputs changed may have entered the result
                // without changing: its seq alone cannot say, so the result
                // as of `since` does. Only the opening snapshot is judged
                // so: a row a later commit in this tick added joined after
                // that judgement.
                let held_as_of = |row: &SubscriptionOutputRow, reset: bool| {
                    resume.as_ref().is_some_and(|resume| {
                        reset
                            && row.seq <= Pos::at(resume.since)
                            && resume
                                .prior
                                .as_ref()
                                .is_none_or(|prior| prior.contains(&row.id))
                    })
                };
                // Every image this subscription's events ship or touch, read
                // a chunk of rows per statement: an opening snapshot ships
                // every row of its result.
                let mut current: BTreeMap<String, Vec<(RowUuid, Pos)>> = BTreeMap::new();
                let mut versions: BTreeMap<String, Vec<(RowUuid, Pos)>> = BTreeMap::new();
                let mut joined_rows = Vec::new();
                let mut leaving: BTreeMap<&str, Vec<RowUuid>> = BTreeMap::new();
                for event in &events {
                    let SubscriptionEvent::Delta {
                        added,
                        updated,
                        removed,
                        reset,
                        ..
                    } = event
                    else {
                        continue;
                    };
                    for row in added.iter().chain(updated.iter()) {
                        joined_rows.extend(
                            joined_tables
                                .iter()
                                .map(String::as_str)
                                .zip(row.joined_rows()),
                        );
                        let history = history_source_table(&row.table);
                        if (history.is_none() && held_as_of(row, *reset) && support.is_none())
                            || cache.contains_key(&(row.table.to_string(), row.id, row.seq))
                        {
                            continue;
                        }
                        let wanted = if history.is_some() {
                            &mut versions
                        } else {
                            &mut current
                        };
                        wanted
                            .entry(row.table.to_string())
                            .or_default()
                            .push((row.id, row.seq));
                    }
                    for gone in removed {
                        let table = names.stored_table(
                            history_source_table(&gone.table).unwrap_or(&gone.table),
                        )?;
                        leaving.entry(table).or_default().push(gone.row_uuid);
                        for (table, joined) in joined_tables.iter().zip(&gone.joined) {
                            leaving.entry(table).or_default().push(joined.row);
                        }
                    }
                }
                for (table, versions) in current {
                    let stored = names.stored_table(&table)?;
                    for ((row, at), found) in inner.images_at(stored, false, &versions)? {
                        cache.insert((table.clone(), row, at), found);
                    }
                }
                // A history row's version and its row's current images; its
                // seq goes to `history_seqs` once found, and again each time
                // the row ships from the cache.
                let mut fresh = HashSet::new();
                for (table, versions) in versions {
                    let stored =
                        names.stored_table(history_source_table(&table).unwrap_or(&table))?;
                    for ((row, at), found) in
                        inner.history_versions_each(stored, &versions, &mut history_seqs)?
                    {
                        fresh.insert((table.clone(), row, at));
                        cache.insert((table.clone(), row, at), found);
                    }
                }
                let mut joined_images = inner
                    .joined_images(&joined_rows, &mut history_seqs)?
                    .into_iter();
                let mut leaving_images = HashMap::new();
                for (table, rows) in leaving {
                    leaving_images.insert(table, inner.rows_images(table, rows)?);
                }
                for event in events {
                    let SubscriptionEvent::Delta {
                        added,
                        updated,
                        removed,
                        reset,
                        ..
                    } = event
                    else {
                        continue;
                    };
                    for row in added.iter().chain(updated.iter()) {
                        for _ in row.joined_rows().iter().take(joined_tables.len()) {
                            images.extend(joined_images.next().unwrap_or_default());
                        }
                        // A history row is a version of its source table.
                        let history = history_source_table(&row.table);
                        let table = names
                            .stored_table(history.unwrap_or(&row.table))?
                            .to_owned();
                        let unchanged = history.is_none() && held_as_of(row, reset);
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
                        if history.is_some() && !fresh.remove(&key) {
                            history_seqs.insert(row.seq.seq);
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
                                watch_key(&Sql::Blob(rid)),
                            )),
                        );
                        let mut touch = |table: &str, row: RowUuid| {
                            for image in leaving_images[table].get(&row).into_iter().flatten() {
                                touched.insert((image.table.clone(), row, image.key.clone()));
                            }
                        };
                        touch(table, gone.row_uuid);
                        // A joined row may have left for its permissions.
                        for (table, joined) in joined_tables.iter().zip(&gone.joined) {
                            touch(table, joined.row);
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
                    let gone = inner.rows_images(
                        &table,
                        prior.iter().filter(|row| !now.contains(row)).copied(),
                    )?;
                    for (row, images) in gone {
                        for image in images {
                            touched.insert((table.clone(), row, image.key));
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
                    // A relayed result is settled as far as its upstream
                    // read is, which may pass the connection's position.
                    let through = match inner.sessions[index].settles.get(&id) {
                        Some(settle) => {
                            let at = inner
                                .client
                                .as_ref()
                                .map_or(through, |client| client.settled_through(&settle.key));
                            let settle = inner.sessions[index]
                                .settles
                                .get_mut(&id)
                                .expect("just read");
                            settle.sent = Some(at);
                            settle.global = global;
                            at
                        }
                        None => through,
                    };
                    settled.push(SyncMessage::Settled {
                        id,
                        through: positions(by, through),
                        members: window,
                        global,
                    });
                    // The settled opening listed the result.
                    if let Some((.., opened)) = inner.sessions[index].subs.get_mut(&id) {
                        *opened = true;
                    }
                }
            }
            // A masking head row's readability is rechecked as policies'
            // inputs change: access regained ships its image, which lifts
            // the replica's marker of it. A write to a masking row the
            // client already knows is hidden ships nothing: each edit of a
            // version it may not read would otherwise reach it (SPEC 8.4).
            for (owner, table, row, key) in masking {
                let can = inner.current_image(&table, row, &key)?.is_some()
                    && readable(&inner, &table, row, &key, policy)?;
                let hidden_before = held_rows
                    .of(&table)
                    .and_then(|rows| rows.get(&(row, key.clone())))
                    .is_some_and(|(could, _)| !could);
                held_rows.keep(owner, &table, row, &key, can);
                if can || !hidden_before {
                    touched.insert((table, row, key));
                }
            }
            // An opening ships every masking row as of now: the client's
            // copy may predate a change of access (a resume included).
            for (owner, mask) in &opening_masks {
                for (table, row, key) in inner.masking_rows(policy, mask)? {
                    let can = inner.current_image(&table, row, &key)?.is_some()
                        && readable(&inner, &table, row, &key, policy)?;
                    held_rows.keep(*owner, &table, row, &key, can);
                    touched.insert((table, row, key));
                }
            }
            // Rows that left a result, and rows a commit wrote: the current
            // image if the client may read it, else an eviction (hidden
            // while a version of it still exists).
            // Ids the replica holds no version of any more: nobody watches
            // them.
            let mut gone = Vec::new();
            for (table, row, key) in touched {
                let hidden = match inner.current_image(&table, row, &key)? {
                    Some(image) if readable(&inner, &table, row, &key, policy)? => {
                        unreadable.remove(&(table.clone(), row, key.clone()));
                        images.push(image);
                        continue;
                    }
                    // Hidden only where it masks: the head of one of the
                    // session's branch views over a base. Elsewhere the
                    // replica just drops the row and keeps no marker.
                    Some(_) => masks
                        .iter()
                        .chain(&opening_masks)
                        .any(|(_, mask)| mask.table == table && mask.head == key),
                    None => inner.is_withheld(&table, row, &key)?,
                };
                // A version that exists but is unreadable leaves the id
                // watched: the replica may hold the row on another branch
                // (a branch view's base, the main row under a draft). A
                // relay's marker stands for such a version too. A relay may
                // not hold the version it evicts at all, while its session
                // holds the row on another branch: any version of the id
                // here keeps it watched (#4484).
                if inner.is_withheld(&table, row, &key)? || inner.holds_id(&table, row)? {
                    unreadable.insert((table.clone(), row, key.clone()));
                } else {
                    gone.push((table.clone(), *row.0.as_bytes()));
                }
                evictions.push(SyncMessage::AccessLost {
                    table,
                    id: *row.0.as_bytes(),
                    key,
                    hidden,
                });
            }
            // Superseded versions of the client's own commits land in its
            // history, while the row stays readable.
            for (table, row, key, at) in committed {
                let superseded = match inner.current_image(&table, row, &key)? {
                    Some(current) => inner.pos(&current)? != at,
                    None => false,
                };
                if superseded {
                    echoes.retain(|e| !(e.table == table && e.row == row && e.key == key));
                }
                if superseded && readable(&inner, &table, row, &key, policy)? {
                    let id = row.0.as_bytes().to_vec();
                    images.extend(inner.images_where(
                        &table,
                        true,
                        "id = ?1 AND _b = ?2 AND _n = ?3 AND _seq = ?4",
                        &[&id, &key, &at.n, &(at.seq as i64)],
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
                    SyncMessage::AccessLost { table, id, key, .. } => {
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
            for (table, id) in &gone {
                watch.forget(id_watch(table, *id));
            }
            unreadable
                .retain(|(table, row, _)| watch.contains(&id_watch(table, *row.0.as_bytes())));
            // A relayed result whose upstream read settled further, with no
            // change to its rows here, is told so: its rows may have arrived
            // through another read, held back until this one settled them.
            let state = &mut *inner;
            if let Some(client) = state.client.as_ref() {
                for (&id, settle) in state.sessions[index].settles.iter_mut() {
                    let at = client.settled_through(&settle.key);
                    if settle.sent.is_some_and(|sent| sent < at) {
                        settle.sent = Some(at);
                        settled.push(SyncMessage::Settled {
                            id,
                            through: positions(by, at),
                            members: None,
                            global: settle.global,
                        });
                    }
                }
            }
            let mut out = Vec::new();
            // Rows travel by stored names: the versions they are stored by
            // reach the client first, in the `Welcome` it is owed.
            if let Some(catalogue) = inner.catalogue_for(index)? {
                let unadmitted = inner.session_unadmitted(index);
                let session = &mut inner.sessions[index];
                out.push(if std::mem::take(&mut session.welcome_owed) {
                    session.told_unadmitted = unadmitted;
                    SyncMessage::Welcome {
                        admitted: !unadmitted,
                        catalogue,
                    }
                } else {
                    SyncMessage::Catalogue(catalogue)
                });
            }
            let tx_ids = if history_seqs.is_empty() {
                Vec::new()
            } else {
                inner.tx_ids(&history_seqs)?
            };
            let session = &mut inner.sessions[index];
            session.watch = watch;
            session.held = held_rows;
            session.unreadable = unreadable;
            let mut seen = HashSet::new();
            images.retain(|image| {
                seen.insert((
                    image.table.clone(),
                    image.id,
                    image.key.clone(),
                    image.by,
                    image.seq,
                ))
            });
            // The writer's own rows, still at the version its commit
            // produced and exactly as it holds them, settle by its Fate.
            let mut as_written: BTreeMap<TxId, Vec<AsWritten>> = BTreeMap::new();
            let fated: HashSet<TxId> = session
                .outbox
                .iter()
                .filter_map(|message| match message {
                    SyncMessage::Fate {
                        id,
                        outcome: Outcome::Accepted { .. },
                        ..
                    } => Some(*id),
                    _ => None,
                })
                .collect();
            echoes.retain(|echo| fated.contains(&echo.id));
            if !echoes.is_empty() {
                images.retain(|image| {
                    let Some(at) = echoes.iter().position(|e| {
                        e.table == image.table
                            && *e.row.0.as_bytes() == image.id
                            && e.key == image.key
                            && e.at == Pos::at(image.seq)
                            && image.by.is_none()
                    }) else {
                        return true;
                    };
                    let echo = echoes.swap_remove(at);
                    as_written
                        .entry(echo.id)
                        .or_default()
                        .push(AsWritten::of(echo.op, image));
                    hold_shipped(&mut session.shipped, image, echo.at);
                    false
                });
                for message in &mut session.outbox {
                    if let SyncMessage::Fate {
                        id,
                        outcome: Outcome::Accepted { as_written: h, .. },
                        ..
                    } = message
                        && let Some(mut rows) = as_written.remove(id)
                    {
                        rows.sort_unstable_by(|a, b| (a.op, &a.key).cmp(&(b.op, &b.key)));
                        *h = rows;
                    }
                }
            }
            out.extend(rows_messages(images, tx_ids, ROWS_MESSAGE_BYTES));
            out.extend(evictions);
            out.extend(std::mem::take(&mut session.outbox));
            out.extend(settled);
            // The connection's position follows the tick's rows, settles
            // and fates, when it moved: a read's settle no longer moves what
            // the replica knows, and rows shipped past that position would
            // otherwise wait for a later tick's to show.
            let delivers = out.iter().any(|message| {
                matches!(
                    message,
                    SyncMessage::Rows { .. }
                        | SyncMessage::AccessLost { .. }
                        | SyncMessage::Settled { .. }
                        | SyncMessage::Fate { .. }
                )
            });
            // A relay also passes on each move of its upstream's position
            // to a session it has delivered to: that position arrives on
            // its own, after rows the relay may already have served.
            let forwards = relaying && session.connection_settled.is_some();
            if (delivers || forwards) && session.connection_settled != Some((by, through)) {
                session.connection_settled = Some((by, through));
                out.push(SyncMessage::ConnectionSettled {
                    through: positions(by, through),
                });
            }
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
                // This relay's versions of those commits, at its own node.
                let me = *inner.identity.node.as_bytes();
                let is_own = |image: &Image| image.by == Some(me) && own.contains(&image.seq);
                let mut shipped = HashSet::new();
                for message in &mut out {
                    let SyncMessage::Rows { images, .. } = message else {
                        continue;
                    };
                    let mut bases = Vec::new();
                    for image in images.iter().filter(|image| is_own(image)) {
                        if !shipped.insert((image.table.clone(), image.id, image.key.clone())) {
                            continue;
                        }
                        let id = image.id.to_vec();
                        let base = inner
                            .images_where(
                                &image.table,
                                true,
                                "id = ?1 AND _b = ?2 AND (_n, _seq) < (?3, ?4) \
                                 ORDER BY _n DESC, _seq DESC",
                                &[&id, &image.key, &inner.me, &(image.seq as i64)],
                            )?
                            .into_iter()
                            .find(|base| !is_own(base));
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
                                inner.pos(&base)?,
                                policy,
                            )? {
                                bases.push(base);
                            }
                        }
                    }
                    images.retain(|image| !is_own(image));
                    for base in bases {
                        if !images.iter().any(|image| {
                            image.table == base.table
                                && image.id == base.id
                                && image.key == base.key
                                && image.by == base.by
                                && image.seq == base.seq
                        }) {
                            images.push(base);
                        }
                    }
                }
                out.retain(|message| {
                    !matches!(message, SyncMessage::Rows { images, tx_ids }
                            if images.is_empty() && tx_ids.is_empty())
                });
            }
            // Chunked cells go as deltas over the version of their row the
            // replica holds (SPEC 19.8). Whatever ships last of a version,
            // its image or its eviction, is what the replica knows of it.
            let mut shipped = std::mem::take(&mut inner.sessions[index].shipped);
            let mut unreadable = std::mem::take(&mut inner.sessions[index].unreadable);
            for message in &mut out {
                match message {
                    SyncMessage::Rows { images, .. } => {
                        for image in images.iter_mut() {
                            unreadable.remove(&(
                                image.table.clone(),
                                RowUuid::from_bytes(image.id),
                                image.key.clone(),
                            ));
                            inner.ship_deltas(&mut shipped, image)?;
                        }
                    }
                    SyncMessage::AccessLost { table, id, key, .. } => {
                        shipped.remove(&(table.clone(), *id, key.clone()));
                        let row = RowUuid::from_bytes(*id);
                        if inner.current_image(table, row, key)?.is_some()
                            || inner.is_withheld(table, row, key)?
                        {
                            unreadable.insert((table.clone(), row, key.clone()));
                        }
                    }
                    _ => {}
                }
            }
            let session = &mut inner.sessions[index];
            session.shipped = shipped;
            session.unreadable = unreadable;
            let subscriber = SubscriberId {
                slot: index,
                generation: session.generation,
            };
            for message in out {
                // A failed or backed-up client is dropped, never waited
                // on: one slow client must not stall the others, and its
                // replica is coherent only with every message. It
                // reconnects and resumes from its last settled seq. The
                // host tells it why (see `take_closed_subscribers`): a
                // backed-up one may reconnect, one sent a message that
                // cannot be encoded would fail the same way again.
                if let Err(error) = session.transport.send(message) {
                    let code = match error {
                        TransportError::Backpressure => ErrorCode::Backpressure,
                        TransportError::Failed(_) => ErrorCode::Protocol,
                    };
                    if let Some(closed) = inner.closed.as_mut() {
                        closed.push((
                            subscriber,
                            Error::new(code, format!("could not send to this session: {error}")),
                        ));
                    }
                    let session = &mut inner.sessions[index];
                    let released = session.close(&dropped);
                    for (query, binding, include_deleted, view) in released {
                        inner.release_remote(
                            &query,
                            &binding,
                            include_deleted,
                            &view,
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
                wait.read,
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

    /// A handle on this store under its stored view, which replicated
    /// writes apply under.
    fn stored_db(&self) -> Db {
        let view = self.inner.borrow().stored_view();
        Db {
            inner: self.inner.clone(),
            view: Some(view),
        }
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
        // Queries and held rows name tables and columns as the client's
        // schema version does; commits by stored names (see below).
        let db = self.session_db(index);
        match message {
            SyncMessage::Read {
                id,
                query,
                binding,
                include_deleted,
                tier,
                wait,
                view,
                since,
                span: ReadSpan::Live,
            } => {
                // Refused before the read is set up, so a refused read
                // holds nothing open.
                let since = single_position(&since)?;
                let local = tier == ReadTier::LocalOnly;
                let prepared = with_claims(db.prepare_query_bound(&query, binding)?);
                let windowed = crate::db::is_windowed(&query);
                // The result is kept in the read's view: a branch view's
                // head over its live or frozen base (SPEC 11.5).
                let viewed = {
                    let opts = ReadOpts {
                        read_view: crate::protocol::ReadViewSpec {
                            source: (*view).clone(),
                            include_deleted,
                        },
                        ..ReadOpts::default()
                    };
                    // A session's read is served as this store holds it.
                    ReadSpec {
                        position: Position::Current,
                        ..ReadSpec::of(&opts, &db.inner())?
                    }
                };
                let resume_read = viewed.clone();
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
                    db.attach_remote(&prepared, include_deleted, &view, RemoteReach::REMOTE)
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
                            read: viewed.clone(),
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
                        // A Remote one leaves out this replica's pending
                        // versions: only the authority confirms them.
                        settled_only: relayed.is_some() && tier == ReadTier::Remote,
                        ..viewed
                    },
                    match relayed {
                        Some((_, (answered, _, global))) => {
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
                                (*view).clone(),
                            );
                            inner.sessions[index].relayed.insert(id, read);
                            if let Some((key, _)) = &attached {
                                inner.sessions[index].settles.insert(
                                    id,
                                    RelayedSettle {
                                        key: key.clone(),
                                        sent: None,
                                        global: false,
                                    },
                                );
                            }
                        }
                        Err(_) => inner.release_remote(
                            &prepared.query,
                            &prepared.binding,
                            include_deleted,
                            &view,
                            RemoteReach::REMOTE,
                        ),
                    }
                }
                let (queue, group) = subscribed?;
                inner.sessions[index]
                    .subs
                    .insert(id, (queue, group, windowed, false));
                // A resume point at another sequencer than the one this
                // store serves at (it gained or lost a server since) opens
                // fresh.
                let since = match since {
                    Some((by, seq)) if by == inner.served_by() => Some(seq),
                    _ => None,
                };
                if let Some(since) = since {
                    let claims = inner.sessions[index].claims.is_some();
                    let resume = inner.resume_from(
                        &prepared,
                        policy,
                        &resume_read,
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
            SyncMessage::ResendWhole { rows } => {
                let mut inner = self.inner.borrow_mut();
                let identity = inner.sessions[index].identity;
                // A seq names a version this session's own commit produced:
                // any other is not the session's to ask for (it may be one
                // the session could not read when it was current).
                let mut own = Vec::with_capacity(rows.len());
                for (.., seq) in &rows {
                    let at = inner.served_at(*seq);
                    own.push((*seq > 0 && inner.committed_by(at, &identity)?).then_some(at));
                }
                let session = &mut inner.sessions[index];
                for ((table, id, key, _), own) in rows.into_iter().zip(own) {
                    session.shipped.remove(&(table.clone(), id, key.clone()));
                    let row = (table, RowUuid::from_bytes(id), key);
                    if let Some(at) = own {
                        session
                            .committed
                            .push((row.0.clone(), row.1, row.2.clone(), at));
                    }
                    session.asked_whole.insert(row.clone());
                    session.touched.insert(row);
                }
            }
            // Declares the version the client reads by, and owes the
            // `Welcome` with the catalogue the client lacks.
            SyncMessage::Join { version, known } => {
                let mut inner = self.inner.borrow_mut();
                let session = &mut inner.sessions[index];
                session.version = Some(crate::ids::SchemaVersionId::from_bytes(version));
                session.schema = None;
                session.known_catalogue = Some(known.into_iter().collect());
                session.catalogue_owed = true;
                session.told = None;
                session.welcome_owed = true;
            }
            SyncMessage::Unsubscribe { id } => {
                let mut inner = self.inner.borrow_mut();
                inner.sessions[index].support.remove(&id);
                inner.sessions[index].first_load.remove(&id);
                inner.sessions[index].resume.remove(&id);
                inner.sessions[index].settles.remove(&id);
                // Rows only this subscription covered go cold: the client
                // keeps them, unwatched, until a subscription covers them
                // again and its opening brings them up to date.
                inner.sessions[index].watch.release(id);
                inner.sessions[index].held.release(id);
                if let Some((query, binding, include_deleted, view)) =
                    inner.sessions[index].relayed.remove(&id)
                {
                    inner.release_remote(
                        &query,
                        &binding,
                        include_deleted,
                        &view,
                        RemoteReach::REMOTE,
                    );
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
            SyncMessage::CheckAllowed { id, action, acting } => {
                // As for a commit: a trusted link may ask for a session it
                // vouches for; a session link only for its own.
                let result = match commit_who(policy, &acting) {
                    Ok(who) => {
                        let identity = match who {
                            Who::Session(author) => Some(author),
                            _ => policy,
                        };
                        let vouched = vouched(policy, who, acting);
                        self.with_claims(vouched, || self.advise(identity, action))
                    }
                    Err(_) => PermissionAdvice::Denied,
                };
                self.inner.borrow_mut().sessions[index]
                    .outbox
                    .push(SyncMessage::CheckAllowedResult { id, result });
            }
            SyncMessage::Read {
                id,
                query,
                binding,
                include_deleted,
                tier,
                wait,
                view,
                since,
                span: ReadSpan::Once { at },
            } => {
                // The position the read is as of, counted at this store:
                // refused before anything is asked or served.
                let pinned = match single_position(&at)? {
                    Some((by, seq)) => Some(Pos {
                        n: self.inner.borrow().known_code(by)?.ok_or_else(|| {
                            Error::new(
                                ErrorCode::Protocol,
                                "a read as of a sequencer this store does not know",
                            )
                        })?,
                        seq,
                    }),
                    None => None,
                };
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
                let (relay, refresh) = if matches!(*view, ReadViewSourceSpec::Snapshot { .. }) {
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
                    let upstream_since = inner.register_once_read(
                        upstream,
                        &remote_query(&prepared.query),
                        &prepared.binding,
                        include_deleted,
                        &view,
                        &at,
                    );
                    let client = inner.client.as_mut().expect("client");
                    client.outbox.push(SyncMessage::Read {
                        id: upstream,
                        query: Box::new(remote_query(&prepared.query).into_owned()),
                        binding: prepared.binding.clone(),
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        view: view.clone(),
                        since: upstream_since,
                        span: ReadSpan::Once { at: at.clone() },
                    });
                }
                if let Some(strict) = relay {
                    let prepared = db.prepare_query_bound(&query, binding.clone())?;
                    let mut inner = self.inner.borrow_mut();
                    let client = inner.client.as_mut().expect("client");
                    let upstream = client.next_id;
                    client.next_id += 1;
                    let upstream_since = inner.register_once_read(
                        upstream,
                        &remote_query(&prepared.query),
                        &prepared.binding,
                        include_deleted,
                        &view,
                        &at,
                    );
                    let client = inner.client.as_mut().expect("client");
                    client.answers.insert(upstream, None);
                    let ask = SyncMessage::Read {
                        id: upstream,
                        query: Box::new(remote_query(&prepared.query).into_owned()),
                        binding: prepared.binding.clone(),
                        include_deleted,
                        tier: ReadTier::Remote,
                        wait: None,
                        view: view.clone(),
                        since: upstream_since,
                        span: ReadSpan::Once { at: at.clone() },
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
                            view,
                            since,
                            span: ReadSpan::Once { at },
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
                    // At the authority's position or this store's own.
                    if let Some(Some(at)) = inner.settled(node, tx)? {
                        named.push((at.seq, inner.node_of(at.n)?, node, tx));
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
                    // A session's read is served as this store holds it.
                    Ok(viewed) => ReadSpec {
                        position: Position::Current,
                        ..viewed
                    },
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
                    position: Position::Current,
                    cut: viewed.cut.or(pinned.map(Cut::at)),
                    include_deleted,
                    settled_only: strict,
                    view: viewed.view,
                    splices: Vec::new(),
                };
                let support = inner.support_of(&query)?;
                let since = match single_position(&since)? {
                    Some((by, seq)) if by == inner.served_by() => Some(seq),
                    _ => None,
                };
                let resume = if at.is_empty()
                    && matches!(opts.read_view.source, ReadViewSourceSpec::Current)
                {
                    since
                        .map(|since| {
                            inner.resume_from(
                                &prepared,
                                policy,
                                &read,
                                &support,
                                since,
                                prepared.request_claims.is_some(),
                            )
                        })
                        .transpose()?
                        .flatten()
                } else {
                    None
                };
                let rows = inner.run(&prepared, policy, &read)?;
                let unchanged: HashSet<RowUuid> = rows
                    .iter()
                    .filter(|row| {
                        resume.as_ref().is_some_and(|resume| {
                            row.seq <= Pos::at(resume.since)
                                && resume
                                    .prior
                                    .as_ref()
                                    .is_none_or(|prior| prior.contains(&row.id))
                        })
                    })
                    .map(|row| row.id)
                    .collect();
                let mut images = Vec::new();
                // Seqs of shipped history versions, for their tx ids.
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
                let joined: Vec<_> = rows
                    .iter()
                    .flat_map(|row| {
                        joined_tables
                            .iter()
                            .map(String::as_str)
                            .zip(row.joined_rows())
                    })
                    .collect();
                images.extend(
                    inner
                        .joined_images(&joined, &mut seqs)?
                        .into_iter()
                        .flatten(),
                );
                if history.is_some() {
                    let versions = rows.iter().map(|row| (row.id, row.seq));
                    inner.history_versions(&table, versions, &mut images, &mut seqs)?;
                } else {
                    let versions: Vec<(RowUuid, Pos)> =
                        rows.iter().map(|row| (row.id, row.seq)).collect();
                    let found = inner.version_images(&table, &versions)?;
                    for version in &versions {
                        images.extend(found[version].iter().cloned());
                    }
                }
                // Deduplicate the supporting-row walk within this answer.
                // These temporary watches are discarded when it settles;
                // only live reads keep rows current beyond their answer.
                let mut extra = Vec::new();
                let mut watch = Watches::default();
                let mut held = Kept::default();
                let (mut gone, mut followed) = (Vec::new(), HashSet::new());
                let mut follow = Follow {
                    owner: id,
                    identity: policy,
                    out: &mut extra,
                    watch: &mut watch,
                    held: &mut held,
                    gone: &mut gone,
                    followed: &mut followed,
                };
                for image in &images {
                    let floor = unchanged
                        .contains(&RowUuid::from_bytes(image.id))
                        .then(|| {
                            resume.as_ref().map(|resume| Floor {
                                since: resume.since,
                                diff: resume.prior.is_some(),
                            })
                        })
                        .flatten();
                    inner.follow_support(&support, image, floor, &mut follow)?;
                }
                images.retain(|image| !unchanged.contains(&RowUuid::from_bytes(image.id)));
                images.extend(extra);
                // A dependency may have lost access since the checkpoint.
                // Send that correction before acknowledging completeness.
                let mut lost = Vec::new();
                for (table, row, key) in gone {
                    match inner.current_image(&table, row, &key)? {
                        Some(image) if readable(&inner, &table, row, &key, policy)? => {
                            images.push(image)
                        }
                        _ => lost.push(SyncMessage::AccessLost {
                            table,
                            id: *row.0.as_bytes(),
                            key,
                            hidden: false,
                        }),
                    }
                }
                // Under a branch view over a base, the head rows that mask
                // base rows its query matches.
                let schema = prepared
                    .schema
                    .clone()
                    .unwrap_or_else(|| inner.owner.clone());
                let mut hidden = Vec::new();
                let masks = if read.view.as_ref().is_some_and(|view| view.base.is_some()) {
                    let tables = inner.read_dependencies_in(&prepared, policy, &read)?;
                    fanout::view_masks(&schema, &prepared, &read, &tables)
                } else {
                    Vec::new()
                };
                for mask in &masks {
                    for (table, row, key) in inner.masking_rows(policy, mask)? {
                        match inner.current_image(&table, row, &key)? {
                            Some(image) if readable(&inner, &table, row, &key, policy)? => {
                                images.push(image)
                            }
                            _ => hidden.push(SyncMessage::AccessLost {
                                table,
                                id: *row.0.as_bytes(),
                                key,
                                hidden: true,
                            }),
                        }
                    }
                }
                let mut tx_ids = inner.tx_ids(&seqs)?;
                tx_ids.extend(named);
                let through = if at.is_empty() {
                    positions(inner.served_by(), inner.served_through())
                } else {
                    at
                };
                let authority = inner.client.is_none();
                let session = &mut inner.sessions[index];
                session.unsettled.extend(images.iter().filter_map(|image| {
                    let row = RowUuid::from_bytes(image.id);
                    let key = (image.table.clone(), row, image.key.clone());
                    Some((key, (image.by?, image.seq)))
                }));
                session.outbox.extend(lost);
                session.outbox.extend(hidden);
                session
                    .outbox
                    .extend(rows_messages(images, tx_ids, ROWS_MESSAGE_BYTES));
                session.outbox.push(SyncMessage::Settled {
                    id,
                    through,
                    members: Some(rows.iter().map(|r| *r.id.0.as_bytes()).collect()),
                    // A strict read at a relay is served once its upstream
                    // answered, from the settled rows alone.
                    global: authority || strict,
                });
            }
            SyncMessage::Commit {
                id,
                kind,
                ops,
                acting,
            } => {
                let (node, tx) = (*id.node.as_bytes(), id.time.0);
                // A commit keeps its writer's id at every hop: a link may
                // send one only as a node its identity writes as. A
                // trusted link (a relay forwarding its sessions' commits)
                // may send any.
                let identity = self.inner.borrow().sessions[index].identity;
                if policy.is_some() && !self.inner.borrow().claim_node(node, &identity)? {
                    let mut inner = self.inner.borrow_mut();
                    let through = inner.served_through();
                    let by = inner.served_by();
                    inner.sessions[index].outbox.push(SyncMessage::Fate {
                        id,
                        outcome: Outcome::Refused {
                            through: positions(by, through),
                            error: WireError::Rejected {
                                reason: None,
                                message: "commit names a node another identity writes as"
                                    .to_owned(),
                            },
                        },
                        global: true,
                    });
                    return Ok(());
                }
                // Its ops name tables and columns by stored names,
                // whichever version wrote them: they apply under the stored
                // view, and one naming what this store does not hold was
                // written under a version it has not deployed.
                let db = self.stored_db();
                // A resend after a reconnect gets the recorded fate.
                let known = self.inner.borrow().applied_fate(node, tx)?;
                let resent = known.is_some();
                // A cross-shard part that already landed holds nothing (its
                // gateway may hold it again for the resend).
                if resent {
                    let part = crate::db::shard::CommitRef { node, tx };
                    let mut inner = self.inner.borrow_mut();
                    inner.forget_hold(part)?;
                    inner.shard.landed(part);
                }
                // A commit written under a version this server has not
                // deployed is refused at once, loudly: the app that wrote
                // it was released ahead of its schema (INV-SYNC-52).
                if known.is_none()
                    && (self.inner.borrow().session_unadmitted(index)
                        || !self.inner.borrow().holds_names(&ops))
                {
                    self.inner.borrow_mut().refuse_undeployed_commit(index, id);
                    return Ok(());
                }
                let (result, rows, echoable) = match known {
                    // A resend's writer may not hold what the commit made of
                    // its rows (its fate was lost, or it reopened while
                    // waiting for rows it could not install): they ship as
                    // they are, unless newer versions do.
                    Some(result) => {
                        let landed = match (&result, self.inner.borrow().settled(node, tx)?) {
                            (Ok(()), Some(Some(at))) => Some(at),
                            _ => None,
                        };
                        let rows = match landed {
                            Some(at) => ops
                                .into_iter()
                                .map(from_wire)
                                .collect::<Result<Vec<_>, _>>()
                                .and_then(|ops| self.inner.borrow().resent_rows(&ops, at))
                                .unwrap_or_default(),
                            None => Vec::new(),
                        };
                        (result, rows, Vec::new())
                    }
                    None => match commit_who(policy, &acting)
                        .map_err(|error| (error, Vec::new()))
                        .and_then(|who| {
                            let (ops, mut roots) = db
                                .resolve_deltas(policy, ops)
                                .map_err(|error| (error, Vec::new()))?;
                            roots.extend(
                                db.proof_roots(policy, &ops)
                                    .map_err(|error| (error, Vec::new()))?,
                            );
                            let possession = crate::large::Possession {
                                author: receipt_author(&identity),
                                checked: policy.is_some(),
                                roots,
                                settle: Default::default(),
                            };
                            let vouched = vouched(policy, who, acting);
                            self.with_claims(vouched, || {
                                db.authorize_commit(who, policy, kind, ops, (node, tx), possession)
                            })
                        }) {
                        // The accepted commit recorded its fate atomically.
                        Ok((rows, echoable)) => (Ok(()), rows, echoable),
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
                            let inner = self.inner.borrow();
                            // Read back by the names the current schema
                            // gives what the commit named by stored ones.
                            let error = Error {
                                message: inner.by_current_names(&error.message),
                                ..error
                            };
                            let result = Err(wire_error(&error));
                            inner.record_fate(node, tx, &result)?;
                            inner.record_settled(node, tx, None)?;
                            (result, rows, Vec::new())
                        }
                    },
                };
                let mut inner = self.inner.borrow_mut();
                // An accepted commit's fate carries the seq it landed at (a
                // resend gets the recorded one), so the replica learns it.
                // Where it landed: this store's own position (the
                // authority's, or a replica's own node's), or for a resend
                // of one a relay already settled upstream, the authority's.
                let (landed, by) = match (&result, inner.settled(node, tx)?) {
                    (Ok(()), Some(Some(at))) => (at, inner.node_of(at.n)?),
                    _ => (inner.served_at(inner.served_through()), inner.served_by()),
                };
                let through = landed.seq;
                // The time this store's clock stamped the commit's writes
                // with, when it just applied it.
                let accepted_at = match inner.last_stamp {
                    (seq, at) if seq == through && !resent && result.is_ok() => Some(at as u64),
                    _ => None,
                };
                let session = &mut inner.sessions[index];
                if result.is_ok() {
                    session.committed.extend(
                        rows.iter()
                            .map(|(table, row, key)| (table.clone(), *row, key.clone(), landed)),
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
                            && landed.n == inner.me
                            && client.pending.iter().any(|p| p.tx == through)
                    });
                if upload {
                    let client = inner.client.as_mut().expect("client");
                    client
                        .relayed_fates
                        .entry(through)
                        .or_default()
                        .push((index, id, rows));
                    inner.sessions[index]
                        .outbox
                        .push(SyncMessage::Stored { id });
                } else {
                    // A replica's own last word (it has no server) orders
                    // the commit at its own position, but is not the
                    // authority's.
                    let global = landed.accepted();
                    // The rows the writer likely holds as they now are go
                    // as written: fan-out drops their images if still
                    // current, and the writer checks each by its digest.
                    // A replica's last word ships its versions instead.
                    let session = &mut inner.sessions[index];
                    if result.is_ok() && global {
                        for Echoable { row: row_key, op } in echoable {
                            let (table, row, key) = row_key;
                            session.echoes.push(Echo {
                                id,
                                op,
                                table,
                                row,
                                key,
                                at: landed,
                            });
                        }
                    }
                    let outcome = match result {
                        Ok(()) => Outcome::accepted(by, through, accepted_at),
                        Err(error) => Outcome::Refused {
                            through: positions(by, through),
                            error,
                        },
                    };
                    inner.sessions[index].outbox.push(SyncMessage::Fate {
                        id,
                        outcome,
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
    ) -> Result<(Vec<WireOp>, BTreeSet<[u8; 32]>), Error> {
        let mut roots = BTreeSet::new();
        let unresolved = || {
            Error::new(
                ErrorCode::WriteRejected,
                "a large-value delta names no version of its row the writer may read",
            )
        };
        // A delta stands in for its op's cell, which holds the NULL it
        // replaces, once per column. Checked on the commit alone, before
        // anything stored is read.
        let malformed = |what: &str| {
            Error::new(
                ErrorCode::WriteRejected,
                format!("a large-value delta {what}"),
            )
        };
        for op in &mut ops {
            let mut columns = BTreeSet::new();
            for delta in &op.deltas {
                if !columns.insert(delta.column.as_str()) {
                    return Err(malformed("names a column another delta of its op names"));
                }
                let cell = match &mut op.write {
                    WireWrite::Insert { cells, .. }
                    | WireWrite::Update { cells, .. }
                    | WireWrite::Upsert { cells, .. }
                    | WireWrite::Restore {
                        cells: Some(cells), ..
                    } => cells.get_mut(&delta.column),
                    _ => None,
                };
                let placeholder = cell.is_some_and(|cell| {
                    let (inner, depth) = large_delta::unwrap(cell);
                    matches!(inner, Value::Nullable(None)) && depth == delta.depth
                });
                if !placeholder {
                    return Err(malformed("stands in for no NULL cell of its op"));
                }
            }
        }
        let inner = self.inner();
        for op in &mut ops {
            let deltas = std::mem::take(&mut op.deltas);
            for delta in deltas {
                let table = inner.schema.table(op.table())?;
                let stored = table.stored_name(&delta.column).to_owned();
                let (row, cells) = match &mut op.write {
                    WireWrite::Insert { row, cells, .. }
                    | WireWrite::Update { row, cells, .. }
                    | WireWrite::Upsert { row, cells, .. }
                    | WireWrite::Restore {
                        row,
                        cells: Some(cells),
                        ..
                    } => (RowUuid::from_bytes(*row), cells),
                    _ => return Err(unresolved()),
                };
                let seq = delta.delta.seq;
                if !still_readable_version(
                    &inner,
                    &table.stored,
                    row,
                    &delta.key,
                    Pos::at(seq),
                    identity,
                )? {
                    return Err(unresolved());
                }
                let base = inner
                    .version_image(&table.stored, row, &delta.key, Pos::at(seq))?
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
            // A node this store never interned holds no version here; a
            // pull never interns one.
            let at = inner.known_code(pull.by)?.map(|n| Pos { n, seq: pull.seq });
            // Pulls name the version's table as it is stored, as rows do.
            let image = match (
                inner
                    .owner
                    .by_stored(&pull.table)
                    .map(|t| t.stored.as_str()),
                at,
            ) {
                (Some(table), Some(at))
                    if still_readable_version(&inner, table, row, &pull.key, at, identity)? =>
                {
                    inner
                        .version_image(table, row, &pull.key, at)?
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

    /// The roots of the chunked values of the versions a commit's ops name
    /// as their access proofs, each where `identity` may read it: the trees
    /// the writer holds without pushing them (SPEC 19.4). A proof naming a
    /// version it may not read, or this store lacks, counts for nothing.
    fn proof_roots(
        &self,
        identity: Option<AuthorSubject>,
        ops: &[WireOp],
    ) -> Result<BTreeSet<[u8; 32]>, Error> {
        let inner = self.inner();
        let mut roots = BTreeSet::new();
        for op in ops {
            let Some(proof) = &op.access_proof else {
                continue;
            };
            let (Ok(table), Some(row)) = (inner.schema.stored_table(op.table()), wire_row(op))
            else {
                continue;
            };
            if !still_readable_version(
                &inner,
                table,
                row,
                &proof.key,
                Pos::at(proof.seq),
                identity,
            )? {
                continue;
            }
            if let Some(image) = inner.version_image(table, row, &proof.key, Pos::at(proof.seq))? {
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
        id: ([u8; 16], u64),
        possession: crate::large::Possession,
    ) -> Result<(Vec<RowKey>, Vec<Echoable>), (Error, Vec<RowKey>)> {
        // Each base by the op it travels on; the ops apply without them.
        let bases: Vec<(usize, WireBase)> = ops
            .iter()
            .enumerate()
            .flat_map(|(index, op)| op.bases.iter().map(move |base| (index, base.clone())))
            .collect();
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
            } if forward => {
                let (by, snapshot) = required_position(&snapshot, "a serializable commit")
                    .map_err(|e| (e, Vec::new()))?;
                self.inner
                    .borrow()
                    .relayed_serializable(snapshot, by, reads, own)
                    .map_err(|e| (e, Vec::new()))?
            }
            unchanged => {
                kind = unchanged;
                Kind::Mergeable
            }
        };
        if let WireKind::Serializable {
            snapshot,
            reads,
            own,
        } = &kind
        {
            let (by, snapshot) = required_position(snapshot, "a serializable commit")
                .map_err(|e| (e, Vec::new()))?;
            let inner = self.inner();
            // The sender's own commits its snapshot included, by local seq
            // and where each settled here (earlier on this connection, so
            // before this one).
            let mut settled = BTreeMap::new();
            for &tx in own {
                if let Some(seq) = inner
                    .settled(id.0, tx)
                    .map_err(|e| (e, Vec::new()))?
                    .flatten()
                {
                    settled.insert(tx, seq);
                }
            }
            // The session's snapshot is the position its server served it
            // at. Positions order as main's single seq space did: the
            // authority's before a replica's own, so a snapshot a replica
            // numbered holds every authority version (one this store cannot
            // name holds them all).
            let base = Pos {
                n: inner
                    .known_code(by)
                    .map_err(|e| (e, Vec::new()))?
                    .unwrap_or(i64::MAX),
                seq: snapshot,
            };
            let in_snapshot = |at: Pos| at <= base || settled.values().any(|s| *s == at);
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
                // A read is evaluated in the version it was made in.
                let Some(schema) =
                    inner.governed(crate::ids::SchemaVersionId::from_bytes(read.version))
                else {
                    return Err((schema_version_not_deployed(), mentioned(&inner)));
                };
                let prepared = PreparedQuery {
                    query: read.query.clone(),
                    binding: read.binding.clone(),
                    shape: Shape([0; 16]),
                    binding_id: Binding([0; 16]),
                    request_claims: None,
                    schema: Some(schema),
                };
                let spec = ReadSpec {
                    include_deleted: read.include_deleted,
                    ..ReadSpec::default()
                };
                let now: Vec<([u8; 16], Option<Pos>)> = inner
                    .read_versions(&prepared, identity, &spec)
                    .map_err(|e| (e, Vec::new()))?
                    .iter()
                    .map(|(row, at)| (*row.0.as_bytes(), Some(*at)))
                    .collect();
                // A row the sender read at an own commit is compared where
                // that commit settled; one at this store's own position (a
                // relay's version it shipped the sender) where it is; one a
                // relay names by the commit that wrote it, where that
                // settled.
                let me = *inner.identity.node.as_bytes();
                let mut then: Vec<([u8; 16], Option<Pos>)> = Vec::new();
                for &(row, by, seq) in &read.rows {
                    let at = match by {
                        None => Some(Pos::at(seq)),
                        Some(node) if node == id.0 => settled.get(&seq).copied(),
                        Some(node) if node == me && inner.me != 0 => Some(Pos { n: inner.me, seq }),
                        Some(node) => inner
                            .settled(node, seq)
                            .map_err(|e| (e, Vec::new()))?
                            .flatten(),
                    };
                    then.push((row, at));
                }
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
        let (bases, mut built, landed) = {
            let inner = self.inner();
            let resolved = if let Err(error) = inner.check_bases(&ops, &by_row, &bases) {
                Err(error)
            } else if relay {
                // Where the bases landed in this relay's own history, for
                // the echo inference alone (see `Echoable`).
                inner.relay_landed(id, &ops, &bases).and_then(|landed| {
                    inner
                        .relay_bases(id, &ops, bases)
                        .map(|bases| (bases, HashMap::new(), landed))
                })
            } else {
                inner
                    .resolve_bases(id, identity.as_ref(), &ops, &bases)
                    .map(|built| (Vec::new(), built, HashMap::new()))
            };
            match resolved {
                Ok(resolved) => resolved,
                Err(error) => return Err((error, mentioned(&inner))),
            }
        };
        // Each written row's version before the commit, for the echo
        // inference: a write over the version its writer built on, or an
        // insert of a row there was none of, left it as its writer holds it.
        let previous = {
            let inner = self.inner();
            let mut previous = HashMap::new();
            for (table, row) in by_row.keys() {
                let versions = inner
                    .current_versions(table, *row)
                    .map_err(|e| (e, Vec::new()))?;
                for (key, seq) in versions {
                    previous.insert((table.clone(), *row, key), seq);
                }
            }
            previous
        };
        let merged: HashSet<RowKey>;
        let (seq, mark, prior) = {
            let mut inner = self.inner();
            // A cross-shard part lands where all its parts do: at the
            // position its gateway fixed, under commits this shard accepted
            // since its hold. None of those wrote its rows: every other
            // writer of them waited while the hold stood (sessions in
            // `serve_session`, local writes in `Db::unheld`), so it always
            // lands (INV-SHARD-19).
            let part = crate::db::shard::CommitRef {
                node: id.0,
                tx: id.1,
            };
            let fixed = inner.shard.fixed(part);
            let seq = fixed.unwrap_or_else(|| inner.next_seq());
            let prior = inner.seq;
            begin_savepoint(&inner.conn, "upload").map_err(|e| (e, Vec::new()))?;
            let mark = inner.changed.borrow().len();
            // The unit's write checks run at its end, grounded (SPEC 7).
            inner.defer_checks();
            // The unit was made over the sender's image (SPEC 4, INV-HIST-8);
            // text merges read what each row was built on.
            inner.merging = true;
            inner.possession = Some(possession);
            inner.merge_bases = std::mem::take(&mut built);
            inner.as_sent.borrow_mut().clear();
            let applied = ops
                .iter()
                .try_for_each(|op| inner.apply_as(op, who, seq).map(drop))
                .and_then(|()| inner.finish_checks(seq))
                .and_then(|()| inner.settle_possession());
            inner.possession = None;
            inner.merging = false;
            built = std::mem::take(&mut inner.merge_bases);
            // Rows a text merge changed are not as their writer sent them
            // (an unmerged text write notes no value as sent).
            merged = inner
                .as_sent
                .borrow()
                .iter()
                .filter(|(_, columns)| columns.values().any(Option::is_some))
                .map(|(row, _)| row.clone())
                .collect();
            if let Err(error) = applied {
                inner.as_sent.borrow_mut().clear();
                inner.abandon_checks();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                let rows = mentioned(&inner);
                return Err((error, rows));
            }
            let recorded = inner
                .record_fate(id.0, id.1, &Ok(()))
                .and_then(|()| inner.record_settled(id.0, id.1, Some(inner.own_pos(seq))));
            if let Err(error) = recorded {
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                return Err((error, Vec::new()));
            }
            // Its hold goes with it: the record of both commits together.
            if let Err(error) = inner.forget_hold(part) {
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                return Err((error, Vec::new()));
            }
            inner.seq = inner.seq.max(seq);
            (seq, mark, prior)
        };
        // The relay's own upload record and the rows the sender hears about
        // are read before the commit is released, so a storage error
        // rejects it whole instead of leaving it applied but not relayed.
        // The authority keeps what each version was built on (INV-HIST-21);
        // a relay passes the bases on instead.
        let finished = if relay {
            self.record_stored_upload(
                ops.clone(),
                seq,
                upload,
                Some(Relayed { origin: id, bases }),
            )
        } else {
            Ok(())
        }
        .and_then(|()| self.inner.borrow().written_rows(&ops, seq))
        .and_then(|rows| {
            if !relay {
                let inner = self.inner.borrow();
                inner.record_bases(&ops, &by_row, &built, seq, &rows)?;
                inner.record_as_sent(seq, id.0, &built, &rows)?;
            }
            Ok(rows)
        });
        let mut inner = self.inner();
        match finished {
            Ok(rows) => {
                end_savepoint(&inner.conn, "upload", true).map_err(|e| (e, Vec::new()))?;
                inner.shard.landed(crate::db::shard::CommitRef {
                    node: id.0,
                    tx: id.1,
                });
                let on = if relay {
                    landed
                } else {
                    built
                        .iter()
                        .map(|(row_key, built)| {
                            let (Built::Seq(seq) | Built::On(seq)) = *built;
                            (row_key.clone(), inner.served_at(seq))
                        })
                        .collect()
                };
                let echoable = echoable(&ops, &by_row, &rows, &on, &previous, &merged);
                drop(inner);
                self.notify();
                Ok((rows, echoable))
            }
            Err(error) => {
                inner.as_sent.borrow_mut().clear();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "upload", false).map_err(|e| (e, Vec::new()))?;
                inner.seq = prior;
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
    acting: Acting,
) -> Option<(AuthorSubject, BTreeMap<String, Value>)> {
    match (who, acting) {
        (Who::Session(author), Acting::Session { claims, .. }) if policy.is_none() => {
            Some((author, claims))
        }
        _ => None,
    }
}

/// The rows of an applied commit its writer likely holds as they now are:
/// rows written over the version they were built on (`on`, by where it
/// landed here) or, built on nothing, created by its ops alone where there
/// was no version, with no text merge changing what was sent. The writer checks each by its
/// digest, so a row it holds otherwise costs a round trip, never a wrong
/// cache. A row a `Move` writes is not inferred (it spans two branches).
fn echoable(
    ops: &[StagedOp],
    by_row: &conflicts::RowOps,
    rows: &[RowKey],
    on: &HashMap<RowKey, Pos>,
    previous: &HashMap<RowKey, Pos>,
    merged: &HashSet<RowKey>,
) -> Vec<Echoable> {
    rows.iter()
        .filter(|row| !merged.contains(*row))
        .filter_map(|row_key @ (table, row, _)| {
            let indexes = by_row.get(&(table.clone(), *row))?;
            if indexes
                .iter()
                .any(|&i| matches!(ops[i], StagedOp::Move(..)))
            {
                return None;
            }
            let built = on.get(row_key).copied();
            // With no base, the writer held no image: inserts, or upserts
            // of an unbranched row, made a new row from the ops alone. An
            // upsert into a branch may copy a parent branch's version (and
            // its create stamp), which the writer may hold otherwise. (Ops
            // are by row, not branch key: every op on the row must qualify.)
            let new_row = indexes.iter().all(|&i| match ops[i] {
                StagedOp::Insert(..) => true,
                StagedOp::Upsert(..) => row_key.2.is_empty(),
                _ => false,
            });
            if built.is_none() && !new_row {
                return None;
            }
            (previous.get(row_key).copied() == built).then(|| Echoable {
                row: row_key.clone(),
                op: indexes[0] as u32,
            })
        })
        .collect()
}

fn commit_who(policy: Option<AuthorSubject>, acting: &Acting) -> Result<Who, Error> {
    let session = matches!(acting, Acting::Session { .. });
    let author = acting
        .author()
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

/// [`readable`] for the version of the row at `at`.
fn readable_version(
    inner: &Inner,
    table: &str,
    row: RowUuid,
    key: &[u8],
    at: Pos,
    identity: Option<AuthorSubject>,
) -> Result<bool, Error> {
    let Some(author) = identity else {
        return Ok(true);
    };
    let schema = inner.owner.clone();
    let table = owned_table(&schema, table)?;
    inner.check_version(table, row, key, at, author, Operation::Select)
}

/// Whether `identity` may read the version of `row` at `seq` and, still,
/// the row as it stands: losing read access takes effect at once for the
/// content of every earlier version too (SPEC 19.9, INV-RLS-13).
fn still_readable_version(
    inner: &Inner,
    table: &str,
    row: RowUuid,
    key: &[u8],
    at: Pos,
    identity: Option<AuthorSubject>,
) -> Result<bool, Error> {
    Ok(readable_version(inner, table, row, key, at, identity)?
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
    match &op.write {
        WireWrite::Insert { row, .. }
        | WireWrite::Update { row, .. }
        | WireWrite::Upsert { row, .. }
        | WireWrite::Delete { row, .. }
        | WireWrite::Restore { row, .. }
        | WireWrite::Move { row, .. } => Some(RowUuid::from_bytes(*row)),
    }
}

/// Record that the replica holds `image`, when it has chunked cells: its
/// later versions' go as deltas over it (SPEC 19.8).
fn hold_shipped(shipped: &mut HashMap<(String, [u8; 16], Vec<u8>), Pos>, image: &Image, at: Pos) {
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
        .or_insert(at);
    *seq = (*seq).max(at);
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
        // A branch view's head is current over a frozen base too: only a
        // snapshot read is wholly this store's.
        let frozen = matches!(opts.read_view.source, ReadViewSourceSpec::Snapshot { .. });
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
    /// stored view, or its upstream's once that answers). Its own versions
    /// are at a [`LEAF`] code, after its relay's.
    pub fn set_non_durable_client(&self) {
        let mut inner = self.inner.borrow_mut();
        debug_assert!(
            inner.sessions.is_empty(),
            "a non-durable client serves no sessions"
        );
        inner.non_durable = true;
        if inner.client.is_some() {
            inner.me = inner.own_code().expect("intern this replica's node");
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
        format!(
            "pending {:?} sent {} unsent {} baseline {:?} fates {:?} connected {}",
            client.pending.iter().map(|p| p.tx).collect::<Vec<_>>(),
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
                .map(|(tx, r)| (*tx, r.is_ok()))
                .collect::<BTreeMap<_, _>>(),
            client.transport.is_some(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeUuid;

    #[test]
    fn replace_name_rewrites_only_whole_names() {
        assert_eq!(
            replace_name("todos, todos_old and (todos)", "todos", "tasks"),
            "tasks, todos_old and (tasks)"
        );
        assert_eq!(
            replace_name("title~ab12cd`", "title~ab12cd", "name"),
            "name`"
        );
        assert_eq!(
            replace_name("xtitle~ab12cd", "title~ab12cd", "name"),
            "xtitle~ab12cd"
        );
    }

    /// A pending `Commit` header in the layout before `acting` (separate
    /// `author` and `session`), with its lists empty as `persist_pending`
    /// stores them.
    fn pre_acting_header(
        author: Option<&str>,
        session: Option<BTreeMap<String, Value>>,
        origin: Option<([u8; 16], u64)>,
    ) -> Vec<u8> {
        const COMMIT: u32 = 4;
        postcard::to_allocvec(&(
            COMMIT,
            [7u8; 16],
            1u64,
            WireKind::Mergeable,
            Vec::<WireOp>::new(),
            author.map(str::to_owned),
            session,
            Vec::<()>::new(),
            origin,
        ))
        .unwrap()
    }

    #[test]
    fn a_pending_header_from_before_acting_is_refused_not_misread() {
        let author = AuthorSubject::system_at(NodeUuid::from_bytes([9; 16]));
        let headers = [
            pre_acting_header(None, None, None),
            pre_acting_header(None, None, Some(([8; 16], 3))),
            pre_acting_header(Some(author.canonical()), None, Some(([8; 16], 3))),
            pre_acting_header(Some(author.canonical()), Some(BTreeMap::new()), None),
        ];
        for header in headers {
            let error = decode_stored::<SyncMessage>(&header)
                .expect_err("an old pending header must not decode");
            assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs, "{error:?}");
            assert!(error.message.contains("resync"), "{error:?}");
        }
    }

    #[test]
    fn a_current_pending_header_round_trips() {
        let header = SyncMessage::Commit {
            id: tx_id([7; 16], 1),
            kind: WireKind::Mergeable,
            ops: Vec::new(),
            acting: Acting::Session {
                author: AuthorSubject::system_at(NodeUuid::from_bytes([9; 16]))
                    .canonical()
                    .to_owned(),
                claims: BTreeMap::from([("role".to_owned(), Value::String("admin".to_owned()))]),
            },
        };
        let bytes = encode(&header);
        let decoded: SyncMessage = decode_stored(&bytes).unwrap();
        assert_eq!(encode(&decoded), bytes);
    }

    fn image(id: u8, seq: u64, body: usize) -> Image {
        Image {
            table: "notes".to_owned(),
            id: [id; 16],
            key: Vec::new(),
            seq,
            by: None,
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
        let tx_ids = vec![(1, None, [9; 16], 1)];
        let messages = rows_messages(images, tx_ids.clone(), 300);
        // seq 1 and 2 fit together; seq 3's two versions stay together.
        assert_eq!(
            shipped(&messages),
            [vec![(2, 1), (4, 2)], vec![(1, 3), (3, 3)]]
        );
        let SyncMessage::Rows { tx_ids: first, .. } = &messages[0] else {
            unreachable!()
        };
        assert_eq!(first, &tx_ids, "the tx ids go with the first part");
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
